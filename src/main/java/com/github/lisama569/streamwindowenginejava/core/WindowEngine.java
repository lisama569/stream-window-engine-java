package com.github.lisama569.streamwindowenginejava.core;

import com.github.lisama569.streamwindowenginejava.checkpoint.CheckpointData;
import com.github.lisama569.streamwindowenginejava.model.Event;
import com.github.lisama569.streamwindowenginejava.model.IngestResult;
import com.github.lisama569.streamwindowenginejava.model.IngestStatus;
import com.github.lisama569.streamwindowenginejava.model.MarkerResult;
import com.github.lisama569.streamwindowenginejava.model.MarkerStatus;
import com.github.lisama569.streamwindowenginejava.model.SourceMarker;
import com.github.lisama569.streamwindowenginejava.model.WindowResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Consumer;

/**
 * 事件时间窗口聚合引擎（支持多来源接入与来源空闲/进度标记）。
 *
 * 语义：
 * - 窗口为固定长度的滚动窗口，按事件时间归属；
 * - 每个来源维护单调不减的来源水位（已见最大事件时间 - 允许迟到时长，空闲标记可抬升），
 *   全局水位 = 各来源水位的最小值，单调不减；
 * - 不带来源标识的事件归入默认来源，单路用法下语义与多来源能力引入前完全一致；
 * - 事件时间 <= 全局水位的事件判为 LATE_REJECTED，不计入也不静默丢弃（结论可区分）；
 * - eventId 重复的事件判为 DUPLICATE，不重复计数；
 * - 空闲/进度标记把对应来源水位抬到声明的进度前沿：长时间无数据的来源不再拖住
 *   其它来源已满足条件的窗口结算；重复标记判 DUPLICATE，过期/矛盾标记判 STALE，
 *   均不生效且互相可区分；
 * - 窗口结束时间 <= 全局水位时窗口确定，结果只取决于被接受事件的集合，
 *   与到达顺序、标记交错顺序及并发提交方式无关（前提是来源不自相矛盾：
 *   不发出比已发标记进度更旧的事件）；
 * - 打开窗口数、来源数、事件/标记去重表均有上限，触限时返回 BACKPRESSURE，
 *   不丢数据、不崩溃；
 * - 已关闭窗口状态与过期去重条目随水位推进回收，内存不随事件总数无界增长。
 *
 * 线程安全：所有公开方法由同一把锁保护，可被并发摄取线程安全调用。
 */
public class WindowEngine {

    private static final Logger log = LoggerFactory.getLogger(WindowEngine.class);

    /** 兼容构造（旧单路用法）的默认来源数上限。 */
    public static final int DEFAULT_MAX_SOURCES = 4096;

    private final long windowSizeMillis;
    private final int maxOpenWindows;
    private final int maxRetainedResults;
    private final SourceProgressTracker progressTracker;
    private final DedupStore dedupStore;
    private final DedupStore markerDedupStore;
    private final Consumer<WindowResult> resultListener;

    /** windowStart -> (key -> [count, valueSum]) */
    private final Map<Long, Map<String, long[]>> openWindows = new HashMap<>();
    private final Deque<WindowResult> finalizedResults = new ArrayDeque<>();
    private final ReentrantLock lock = new ReentrantLock();

    /** 兼容旧单路用法的构造：来源数与标记去重表上限取默认值。 */
    public WindowEngine(long windowSizeMillis,
                        long allowedLatenessMillis,
                        int maxOpenWindows,
                        int maxDedupEntries,
                        int maxRetainedResults,
                        Consumer<WindowResult> resultListener) {
        this(windowSizeMillis, allowedLatenessMillis, maxOpenWindows, maxDedupEntries,
                maxRetainedResults, DEFAULT_MAX_SOURCES, maxDedupEntries, resultListener);
    }

    public WindowEngine(long windowSizeMillis,
                        long allowedLatenessMillis,
                        int maxOpenWindows,
                        int maxDedupEntries,
                        int maxRetainedResults,
                        int maxSources,
                        int maxMarkerEntries,
                        Consumer<WindowResult> resultListener) {
        if (windowSizeMillis <= 0) {
            throw new IllegalArgumentException("windowSizeMillis must be > 0");
        }
        this.windowSizeMillis = windowSizeMillis;
        this.maxOpenWindows = maxOpenWindows;
        this.maxRetainedResults = maxRetainedResults;
        this.progressTracker = new SourceProgressTracker(allowedLatenessMillis, maxSources);
        this.dedupStore = new DedupStore(maxDedupEntries);
        this.markerDedupStore = new DedupStore(maxMarkerEntries);
        this.resultListener = resultListener != null ? resultListener : r -> { };
    }

    public IngestResult ingest(Event event) {
        if (event == null || event.eventId() == null || event.eventId().isBlank()
                || event.key() == null) {
            return IngestResult.of(IngestStatus.LATE_REJECTED, "invalid event: null/blank eventId or key");
        }
        lock.lock();
        try {
            String src = SourceProgressTracker.normalize(event.sourceId());
            long watermark = progressTracker.globalWatermark();
            if (event.eventTimeMillis() <= watermark) {
                log.info("ingest LATE_REJECTED source={} eventId={} eventTime={} watermark={}",
                        src, event.eventId(), event.eventTimeMillis(), watermark);
                return IngestResult.of(IngestStatus.LATE_REJECTED,
                        "eventTime " + event.eventTimeMillis() + " <= watermark " + watermark);
            }
            if (dedupStore.isDuplicate(event.eventId(), event.eventTimeMillis())) {
                log.info("ingest DUPLICATE source={} eventId={} (dedup by eventId)", src, event.eventId());
                return IngestResult.of(IngestStatus.DUPLICATE,
                        "duplicate eventId " + event.eventId());
            }
            if (!progressTracker.canAccommodate(event.sourceId())) {
                log.info("ingest BACKPRESSURE source={} eventId={} source count limit reached",
                        src, event.eventId());
                return IngestResult.of(IngestStatus.BACKPRESSURE,
                        "source count limit reached");
            }
            // 先推进水位并回收已确定窗口/过期去重条目，再做容量检查，
            // 避免“负责推进水位的事件”因尚未回收的旧窗口而被误判背压。
            advanceWatermarkAndFinalize(progressTracker.observeEvent(event.sourceId(), event.eventTimeMillis()));

            long windowStart = Math.floorDiv(event.eventTimeMillis(), windowSizeMillis) * windowSizeMillis;
            Map<String, long[]> window = openWindows.get(windowStart);
            if (window == null && openWindows.size() >= maxOpenWindows) {
                log.info("ingest BACKPRESSURE source={} eventId={} openWindows={} >= maxOpenWindows={}",
                        src, event.eventId(), openWindows.size(), maxOpenWindows);
                return IngestResult.of(IngestStatus.BACKPRESSURE,
                        "open windows limit reached: " + maxOpenWindows);
            }
            if (!dedupStore.register(event.eventId(), event.eventTimeMillis())) {
                log.info("ingest BACKPRESSURE source={} eventId={} dedup store full", src, event.eventId());
                return IngestResult.of(IngestStatus.BACKPRESSURE,
                        "dedup store limit reached");
            }
            if (window == null) {
                window = new HashMap<>();
                openWindows.put(windowStart, window);
            }
            long[] stats = window.computeIfAbsent(event.key(), k -> new long[2]);
            stats[0] += 1;
            stats[1] += event.value();
            return IngestResult.accepted();
        } finally {
            lock.unlock();
        }
    }

    /**
     * 处理来源空闲/进度标记。标记是权威声明：生效后该来源水位抬到声明的进度前沿，
     * 可能推动全局水位并结算窗口；重复、过期或矛盾的标记不生效且结论可区分。
     */
    public MarkerResult ingestMarker(SourceMarker marker) {
        if (marker == null || marker.markerId() == null || marker.markerId().isBlank()
                || marker.sourceId() == null || marker.sourceId().isBlank()) {
            return MarkerResult.of(MarkerStatus.STALE, "invalid marker: null/blank markerId or sourceId");
        }
        lock.lock();
        try {
            String src = SourceProgressTracker.normalize(marker.sourceId());
            if (markerDedupStore.isDuplicate(marker.markerId(), marker.progressTimeMillis())) {
                log.info("marker DUPLICATE source={} markerId={} (dedup by markerId)", src, marker.markerId());
                return MarkerResult.of(MarkerStatus.DUPLICATE,
                        "duplicate markerId " + marker.markerId());
            }
            if (!progressTracker.canAccommodate(marker.sourceId())) {
                log.info("marker BACKPRESSURE source={} markerId={} source count limit reached",
                        src, marker.markerId());
                return MarkerResult.of(MarkerStatus.BACKPRESSURE,
                        "source count limit reached");
            }
            long sourceWatermark = progressTracker.sourceWatermark(marker.sourceId());
            long globalWatermark = progressTracker.globalWatermark();
            // 过期/矛盾：进度前沿不超过该来源已知进度；对新来源，不超过全局水位
            // （其进度已被全局水位蕴含，生效也无意义）。均不生效，结论可区分。
            if (marker.progressTimeMillis() <= sourceWatermark
                    || (!progressTracker.hasSource(marker.sourceId())
                        && marker.progressTimeMillis() <= globalWatermark)) {
                log.info("marker STALE source={} markerId={} progress={} sourceWatermark={} globalWatermark={}",
                        src, marker.markerId(), marker.progressTimeMillis(), sourceWatermark, globalWatermark);
                return MarkerResult.of(MarkerStatus.STALE,
                        "progress " + marker.progressTimeMillis() + " not beyond known progress "
                                + Math.max(sourceWatermark, globalWatermark));
            }
            if (!markerDedupStore.register(marker.markerId(), marker.progressTimeMillis())) {
                log.info("marker BACKPRESSURE source={} markerId={} marker dedup store full",
                        src, marker.markerId());
                return MarkerResult.of(MarkerStatus.BACKPRESSURE,
                        "marker dedup store limit reached");
            }
            long watermark = progressTracker.applyMarker(marker.sourceId(), marker.progressTimeMillis());
            log.info("marker ACCEPTED source={} markerId={} progress={} sourceWatermark={} globalWatermark={}",
                    src, marker.markerId(), marker.progressTimeMillis(),
                    progressTracker.sourceWatermark(marker.sourceId()), watermark);
            advanceWatermarkAndFinalize(watermark);
            return MarkerResult.of(MarkerStatus.ACCEPTED, "accepted");
        } finally {
            lock.unlock();
        }
    }

    /** 结算结束时间 <= 全局水位的窗口，并回收过期去重条目。 */
    private void advanceWatermarkAndFinalize(long watermark) {
        List<Long> toFinalize = new ArrayList<>();
        for (Map.Entry<Long, Map<String, long[]>> e : openWindows.entrySet()) {
            if (e.getKey() + windowSizeMillis <= watermark) {
                toFinalize.add(e.getKey());
            }
        }
        for (Long windowStart : toFinalize) {
            Map<String, long[]> window = openWindows.remove(windowStart);
            Map<String, Long> countByKey = new TreeMap<>();
            long totalCount = 0;
            long totalValue = 0;
            for (Map.Entry<String, long[]> e : window.entrySet()) {
                countByKey.put(e.getKey(), e.getValue()[0]);
                totalCount += e.getValue()[0];
                totalValue += e.getValue()[1];
            }
            WindowResult result = new WindowResult(
                    windowStart, windowStart + windowSizeMillis, totalCount, totalValue, countByKey);
            finalizedResults.addLast(result);
            while (finalizedResults.size() > maxRetainedResults) {
                finalizedResults.pollFirst();
            }
            log.info("window finalized start={} end={} totalCount={} totalValue={} countByKey={} watermark={}",
                    result.windowStart(), result.windowEnd(), totalCount, totalValue, countByKey, watermark);
            resultListener.accept(result);
        }
        int pruned = dedupStore.pruneUpTo(watermark);
        int prunedMarkers = markerDedupStore.pruneBelow(watermark);
        if (pruned > 0 || prunedMarkers > 0) {
            log.info("dedup pruned {} event entries and {} marker entries up to watermark={}, remaining={}/{}",
                    pruned, prunedMarkers, watermark, dedupStore.size(), markerDedupStore.size());
        }
    }

    public long currentWatermark() {
        lock.lock();
        try {
            return progressTracker.globalWatermark();
        } finally {
            lock.unlock();
        }
    }

    /** 各来源当前水位（观测与调试用）。 */
    public Map<String, Long> sourceWatermarks() {
        lock.lock();
        try {
            return progressTracker.snapshot();
        } finally {
            lock.unlock();
        }
    }

    public List<WindowResult> finalizedResults() {
        lock.lock();
        try {
            return List.copyOf(finalizedResults);
        } finally {
            lock.unlock();
        }
    }

    public int openWindowCount() {
        lock.lock();
        try {
            return openWindows.size();
        } finally {
            lock.unlock();
        }
    }

    public int dedupSize() {
        lock.lock();
        try {
            return dedupStore.size();
        } finally {
            lock.unlock();
        }
    }

    public CheckpointData snapshot() {
        lock.lock();
        try {
            Map<Long, Map<String, long[]>> windowsCopy = new HashMap<>();
            for (Map.Entry<Long, Map<String, long[]>> e : openWindows.entrySet()) {
                Map<String, long[]> inner = new HashMap<>();
                for (Map.Entry<String, long[]> kv : e.getValue().entrySet()) {
                    inner.put(kv.getKey(), kv.getValue().clone());
                }
                windowsCopy.put(e.getKey(), inner);
            }
            return new CheckpointData(
                    CheckpointData.CURRENT_FORMAT_VERSION,
                    progressTracker.globalWatermark(),
                    dedupStore.snapshot(),
                    windowsCopy,
                    List.copyOf(finalizedResults),
                    progressTracker.snapshot(),
                    markerDedupStore.snapshot());
        } finally {
            lock.unlock();
        }
    }

    public void restore(CheckpointData data) {
        lock.lock();
        try {
            progressTracker.restore(data.sourceWatermarks());
            dedupStore.restore(data.dedupEntries());
            markerDedupStore.restore(data.markerDedupEntries());
            openWindows.clear();
            for (Map.Entry<Long, Map<String, long[]>> e : data.openWindows().entrySet()) {
                Map<String, long[]> inner = new HashMap<>();
                for (Map.Entry<String, long[]> kv : e.getValue().entrySet()) {
                    inner.put(kv.getKey(), kv.getValue().clone());
                }
                openWindows.put(e.getKey(), inner);
            }
            finalizedResults.clear();
            finalizedResults.addAll(data.finalizedResults());
            log.info("engine restored from checkpoint: watermark={} sources={} dedupEntries={} markerDedupEntries={} openWindows={} finalizedResults={}",
                    progressTracker.globalWatermark(), data.sourceWatermarks().size(),
                    data.dedupEntries().size(), data.markerDedupEntries().size(),
                    openWindows.size(), finalizedResults.size());
        } finally {
            lock.unlock();
        }
    }
}
