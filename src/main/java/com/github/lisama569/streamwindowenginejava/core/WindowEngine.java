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
 * 事件时间窗口聚合引擎。
 *
 * 语义：
 * - 窗口为固定长度的滚动窗口，按事件时间归属；
 * - 支持多来源接入：事件可携带 sourceId；每个已注册来源维护单调进度，
 *   全局水位 = min(各来源进度) - 允许迟到时长，单调不减；
 * - 来源可提交进度/空闲标记（见 submitMarker），标记生效后该来源不再拖住
 *   其它来源已满足条件的窗口结论；不带来源信息的事件归入默认来源，
 *   单来源时语义与旧版完全一致；
 * - 事件时间 <= 水位的事件判为 LATE_REJECTED，不计入也不静默丢弃（结论可区分）；
 * - eventId 重复的事件判为 DUPLICATE，不重复计数；
 * - 窗口结束时间 <= 水位时窗口确定，结果只取决于被接受事件的集合，与到达顺序无关；
 * - 打开窗口数与去重表均有上限，触限时返回 BACKPRESSURE，不丢数据、不崩溃；
 * - 已关闭窗口状态与过期去重条目随水位推进回收，内存不随事件总数无界增长。
 *
 * 线程安全：所有公开方法由同一把锁保护，可被并发摄取线程安全调用。
 */
public class WindowEngine {

    private static final Logger log = LoggerFactory.getLogger(WindowEngine.class);

    private final long windowSizeMillis;
    private final int maxOpenWindows;
    private final int maxRetainedResults;
    private final SourceProgressTracker progressTracker;
    private final DedupStore dedupStore;
    private final Consumer<WindowResult> resultListener;

    /** windowStart -> (key -> [count, valueSum]) */
    private final Map<Long, Map<String, long[]>> openWindows = new HashMap<>();
    private final Deque<WindowResult> finalizedResults = new ArrayDeque<>();
    private final ReentrantLock lock = new ReentrantLock();

    /** 兼容旧调用方：等价于不限制来源数量的单路用法。 */
    public WindowEngine(long windowSizeMillis,
                        long allowedLatenessMillis,
                        int maxOpenWindows,
                        int maxDedupEntries,
                        int maxRetainedResults,
                        Consumer<WindowResult> resultListener) {
        this(windowSizeMillis, allowedLatenessMillis, maxOpenWindows, maxDedupEntries,
                maxRetainedResults, Integer.MAX_VALUE, resultListener);
    }

    public WindowEngine(long windowSizeMillis,
                        long allowedLatenessMillis,
                        int maxOpenWindows,
                        int maxDedupEntries,
                        int maxRetainedResults,
                        int maxSources,
                        Consumer<WindowResult> resultListener) {
        if (windowSizeMillis <= 0) {
            throw new IllegalArgumentException("windowSizeMillis must be > 0");
        }
        this.windowSizeMillis = windowSizeMillis;
        this.maxOpenWindows = maxOpenWindows;
        this.maxRetainedResults = maxRetainedResults;
        this.progressTracker = new SourceProgressTracker(allowedLatenessMillis, maxSources);
        this.dedupStore = new DedupStore(maxDedupEntries);
        this.resultListener = resultListener != null ? resultListener : r -> { };
    }

    public IngestResult ingest(Event event) {
        if (event == null || event.eventId() == null || event.eventId().isBlank()
                || event.key() == null) {
            return IngestResult.of(IngestStatus.LATE_REJECTED, "invalid event: null/blank eventId or key");
        }
        lock.lock();
        try {
            long watermark = progressTracker.currentWatermark();
            if (event.eventTimeMillis() <= watermark) {
                log.info("ingest LATE_REJECTED eventId={} source={} eventTime={} watermark={}",
                        event.eventId(), event.sourceId(), event.eventTimeMillis(), watermark);
                return IngestResult.of(IngestStatus.LATE_REJECTED,
                        "eventTime " + event.eventTimeMillis() + " <= watermark " + watermark);
            }
            if (dedupStore.isDuplicate(event.eventId(), event.eventTimeMillis())) {
                log.info("ingest DUPLICATE eventId={} source={} (dedup by eventId)",
                        event.eventId(), event.sourceId());
                return IngestResult.of(IngestStatus.DUPLICATE,
                        "duplicate eventId " + event.eventId());
            }
            // 事件确定会被接受后才登记来源，避免被拒绝的事件留下空进度来源拖住水位
            if (!progressTracker.registerSource(event.sourceId())) {
                log.info("ingest BACKPRESSURE eventId={} source={} sourceCount={} >= maxSources",
                        event.eventId(), event.sourceId(), progressTracker.sourceCount());
                return IngestResult.of(IngestStatus.BACKPRESSURE,
                        "source limit reached: " + progressTracker.sourceCount());
            }
            // 先推进水位并回收已确定窗口/过期去重条目，再做容量检查，
            // 避免“负责推进水位的事件”因尚未回收的旧窗口而被误判背压。
            advanceWatermarkAndFinalize(event.sourceId(), event.eventTimeMillis());

            long windowStart = Math.floorDiv(event.eventTimeMillis(), windowSizeMillis) * windowSizeMillis;
            Map<String, long[]> window = openWindows.get(windowStart);
            if (window == null && openWindows.size() >= maxOpenWindows) {
                log.info("ingest BACKPRESSURE eventId={} openWindows={} >= maxOpenWindows={}",
                        event.eventId(), openWindows.size(), maxOpenWindows);
                return IngestResult.of(IngestStatus.BACKPRESSURE,
                        "open windows limit reached: " + maxOpenWindows);
            }
            if (!dedupStore.register(event.eventId(), event.eventTimeMillis())) {
                log.info("ingest BACKPRESSURE eventId={} dedup store full", event.eventId());
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

    private void advanceWatermarkAndFinalize(String sourceId, long eventTimeMillis) {
        long watermark = progressTracker.observeEvent(sourceId, eventTimeMillis);
        finalizeAndPrune(watermark);
    }

    /** 确定所有结束时间 <= watermark 的窗口，并回收过期去重条目。 */
    private void finalizeAndPrune(long watermark) {
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
        if (pruned > 0) {
            log.info("dedup pruned {} entries up to watermark={}, remaining={}",
                    pruned, watermark, dedupStore.size());
        }
    }

    public long currentWatermark() {
        lock.lock();
        try {
            return progressTracker.currentWatermark();
        } finally {
            lock.unlock();
        }
    }

    /**
     * 提交来源进度/空闲标记：声明该来源在 progressTimeMillis 前的数据已到齐、当前空闲。
     *
     * 结论互相可区分且幂等：
     * - 重复 markerId -> DUPLICATE，不重复推进；
     * - 进度不领先于该来源已知进度（过期/互相矛盾）-> STALE，不生效；
     * - 来源数或去重表达上限 -> BACKPRESSURE，不生效，可重试；
     * - 生效 -> APPLIED，来源进度推进，可能触发窗口确定。
     * 标记去重条目以 "M:" 前缀与事件去重条目共存于同一去重表，
     * 随水位一同回收、一同进入检查点，重启后重复标记仍判 DUPLICATE。
     */
    public MarkerResult submitMarker(SourceMarker marker) {
        if (marker == null || marker.markerId() == null || marker.markerId().isBlank()) {
            return MarkerResult.of(MarkerStatus.STALE, "invalid marker: null/blank markerId");
        }
        lock.lock();
        try {
            String dedupKey = markerDedupKey(marker.markerId());
            if (dedupStore.isDuplicate(dedupKey, marker.progressTimeMillis())) {
                log.info("marker DUPLICATE markerId={} source={} (dedup by markerId)",
                        marker.markerId(), marker.sourceId());
                return MarkerResult.of(MarkerStatus.DUPLICATE,
                        "duplicate markerId " + marker.markerId());
            }
            // 过期/矛盾判定先于来源登记：未生效的标记不得留下空进度来源拖住水位
            long knownProgress = progressTracker.sourceProgress(marker.sourceId());
            if (marker.progressTimeMillis() <= knownProgress) {
                log.info("marker STALE markerId={} source={} progressTime={} <= knownProgress={}",
                        marker.markerId(), marker.sourceId(),
                        marker.progressTimeMillis(), knownProgress);
                return MarkerResult.of(MarkerStatus.STALE,
                        "progressTime " + marker.progressTimeMillis()
                                + " <= known source progress " + knownProgress);
            }
            // 背压预检也都先于来源登记，保证未生效的标记不改变任何状态
            if (dedupStore.isFull()) {
                log.info("marker BACKPRESSURE markerId={} dedup store full", marker.markerId());
                return MarkerResult.of(MarkerStatus.BACKPRESSURE,
                        "dedup store limit reached");
            }
            if (!progressTracker.registerSource(marker.sourceId())) {
                log.info("marker BACKPRESSURE markerId={} source={} sourceCount={} >= maxSources",
                        marker.markerId(), marker.sourceId(), progressTracker.sourceCount());
                return MarkerResult.of(MarkerStatus.BACKPRESSURE,
                        "source limit reached: " + progressTracker.sourceCount());
            }
            // 预检均已通过且持同一把锁，以下两步必然成功
            dedupStore.register(dedupKey, marker.progressTimeMillis());
            progressTracker.applyMarker(marker.sourceId(), marker.progressTimeMillis());
            long watermark = progressTracker.currentWatermark();
            log.info("marker APPLIED markerId={} source={} progressTime={} watermark={}",
                    marker.markerId(), marker.sourceId(),
                    marker.progressTimeMillis(), watermark);
            finalizeAndPrune(watermark);
            return MarkerResult.of(MarkerStatus.APPLIED,
                    "source progress advanced to " + marker.progressTimeMillis());
        } finally {
            lock.unlock();
        }
    }

    static String markerDedupKey(String markerId) {
        return "M:" + markerId;
    }

    /** 指定来源当前已知进度（事件时间语义）；未知来源返回 Long.MIN_VALUE。 */
    public long sourceProgress(String sourceId) {
        lock.lock();
        try {
            return progressTracker.sourceProgress(sourceId);
        } finally {
            lock.unlock();
        }
    }

    public int sourceCount() {
        lock.lock();
        try {
            return progressTracker.sourceCount();
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
                    progressTracker.currentWatermark(),
                    dedupStore.snapshot(),
                    windowsCopy,
                    List.copyOf(finalizedResults),
                    progressTracker.snapshot());
        } finally {
            lock.unlock();
        }
    }

    public void restore(CheckpointData data) {
        lock.lock();
        try {
            progressTracker.restore(data.watermark(), data.sourceProgress());
            dedupStore.restore(data.dedupEntries());
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
            log.info("engine restored from checkpoint: watermark={} dedupEntries={} openWindows={} finalizedResults={}",
                    data.watermark(), data.dedupEntries().size(), openWindows.size(), finalizedResults.size());
        } finally {
            lock.unlock();
        }
    }
}
