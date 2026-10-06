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
 * - eventId 重复的事件判为 DUPLICATE，不重复计数；事件与标记使用各自独立的
 *   去重表，eventId 与 markerId 即使相同也互不影响；
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
    /** 事件去重表：只按 eventId 去重 */
    private final DedupStore eventDedupStore;
    /** 标记去重表：只按 markerId 去重，与事件去重完全独立，编号互撞互不影响 */
    private final DedupStore markerDedupStore;
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
        this.eventDedupStore = new DedupStore(maxDedupEntries);
        this.markerDedupStore = new DedupStore(maxDedupEntries);
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
            if (eventDedupStore.isDuplicate(event.eventId(), event.eventTimeMillis())) {
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
            if (!eventDedupStore.register(event.eventId(), event.eventTimeMillis())) {
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
        int pruned = eventDedupStore.pruneUpTo(watermark) + markerDedupStore.pruneUpTo(watermark);
        if (pruned > 0) {
            log.info("dedup pruned {} entries up to watermark={}, remaining={}",
                    pruned, watermark, eventDedupStore.size() + markerDedupStore.size());
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
     * - 进度不领先于该来源已知进度，或不领先于当前全局水位
     *   （过期/互相矛盾，如未知来源发来的过期空闲标记）-> STALE，
     *   不生效也不登记来源，不会拖住其它来源的水位；
     * - 来源数或去重表达上限 -> BACKPRESSURE，不生效，可重试；
     * - 生效 -> APPLIED，来源进度推进，可能触发窗口确定。
     * 标记去重与事件去重使用各自独立的去重表：markerId 与 eventId 即使相同
     * 也互不影响，各自按自身语义去重；两张表都随水位回收、都进入检查点，
     * 重启后重复标记仍判 DUPLICATE。
     */
    public MarkerResult submitMarker(SourceMarker marker) {
        if (marker == null || marker.markerId() == null || marker.markerId().isBlank()) {
            return MarkerResult.of(MarkerStatus.STALE, "invalid marker: null/blank markerId");
        }
        lock.lock();
        try {
            // 标记去重表与事件去重表各自独立：markerId 与 eventId 即使相同也互不影响
            if (markerDedupStore.isDuplicate(marker.markerId(), marker.progressTimeMillis())) {
                log.info("marker DUPLICATE markerId={} source={} (dedup by markerId)",
                        marker.markerId(), marker.sourceId());
                return MarkerResult.of(MarkerStatus.DUPLICATE,
                        "duplicate markerId " + marker.markerId());
            }
            // 过期/矛盾判定先于来源登记：未生效的标记不得留下空进度来源拖住水位。
            // 两类过期都要拦截：
            // 1) 不领先于该来源已知进度（重复推进或矛盾）；
            // 2) 不领先于当前全局水位——典型如一个从未出现过的来源发来明显落后的
            //    空闲标记：若生效，该来源会以过期进度参与 min 运算，把水位永远
            //    钉死在低位，拖住其它所有来源。水位单调不减，这种标记不可能再
            //    推进任何东西，判 STALE 且不登记来源。
            long knownProgress = progressTracker.sourceProgress(marker.sourceId());
            long watermark = progressTracker.currentWatermark();
            if (marker.progressTimeMillis() <= knownProgress
                    || marker.progressTimeMillis() <= watermark) {
                log.info("marker STALE markerId={} source={} progressTime={} knownProgress={} watermark={}",
                        marker.markerId(), marker.sourceId(), marker.progressTimeMillis(),
                        knownProgress, watermark);
                return MarkerResult.of(MarkerStatus.STALE,
                        "progressTime " + marker.progressTimeMillis()
                                + " <= known source progress " + knownProgress
                                + " or current watermark " + watermark);
            }
            // 背压预检也都先于来源登记，保证未生效的标记不改变任何状态
            if (markerDedupStore.isFull()) {
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
            markerDedupStore.register(marker.markerId(), marker.progressTimeMillis());
            progressTracker.applyMarker(marker.sourceId(), marker.progressTimeMillis());
            watermark = progressTracker.currentWatermark();
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

    /** v1/v2 旧格式检查点中标记去重条目混入事件去重表时使用的前缀，仅用于恢复旧数据。 */
    static final String LEGACY_MARKER_DEDUP_PREFIX = "M:";

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
            return eventDedupStore.size() + markerDedupStore.size();
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
                    eventDedupStore.snapshot(),
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
            progressTracker.restore(data.watermark(), data.sourceProgress());
            restoreDedup(data);
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
            log.info("engine restored from checkpoint: formatVersion={} watermark={} "
                            + "eventDedupEntries={} markerDedupEntries={} openWindows={} finalizedResults={}",
                    data.formatVersion(), data.watermark(), eventDedupStore.size(),
                    markerDedupStore.size(), openWindows.size(), finalizedResults.size());
        } finally {
            lock.unlock();
        }
    }

    /**
     * 恢复去重表。v3 起事件与标记去重分表存储，直接恢复；
     * v1/v2 旧格式只有一张混合表（标记条目以 "M:" 前缀混入），
     * 按前缀拆分到两张独立的表，拆分后两类编号互不影响。
     */
    private void restoreDedup(CheckpointData data) {
        if (data.formatVersion() >= 3) {
            eventDedupStore.restore(data.dedupEntries());
            markerDedupStore.restore(data.markerDedupEntries());
            return;
        }
        Map<String, Long> eventEntries = new HashMap<>();
        Map<String, Long> markerEntries = new HashMap<>();
        for (Map.Entry<String, Long> e : data.dedupEntries().entrySet()) {
            if (e.getKey().startsWith(LEGACY_MARKER_DEDUP_PREFIX)) {
                markerEntries.put(e.getKey().substring(LEGACY_MARKER_DEDUP_PREFIX.length()),
                        e.getValue());
            } else {
                eventEntries.put(e.getKey(), e.getValue());
            }
        }
        eventDedupStore.restore(eventEntries);
        markerDedupStore.restore(markerEntries);
        if (!markerEntries.isEmpty()) {
            log.info("legacy checkpoint (formatVersion={}) dedup entries split: {} event entries, "
                            + "{} marker entries (\"{}\" prefix)",
                    data.formatVersion(), eventEntries.size(), markerEntries.size(),
                    LEGACY_MARKER_DEDUP_PREFIX);
        }
    }
}