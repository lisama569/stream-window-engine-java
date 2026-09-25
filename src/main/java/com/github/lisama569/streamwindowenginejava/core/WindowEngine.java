package com.github.lisama569.streamwindowenginejava.core;

import com.github.lisama569.streamwindowenginejava.checkpoint.CheckpointData;
import com.github.lisama569.streamwindowenginejava.model.Event;
import com.github.lisama569.streamwindowenginejava.model.IngestResult;
import com.github.lisama569.streamwindowenginejava.model.IngestStatus;
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
 * - 水位 = 已见最大事件时间 - 允许迟到时长，单调不减；
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
    private final WatermarkTracker watermarkTracker;
    private final DedupStore dedupStore;
    private final Consumer<WindowResult> resultListener;

    /** windowStart -> (key -> [count, valueSum]) */
    private final Map<Long, Map<String, long[]>> openWindows = new HashMap<>();
    private final Deque<WindowResult> finalizedResults = new ArrayDeque<>();
    private final ReentrantLock lock = new ReentrantLock();

    public WindowEngine(long windowSizeMillis,
                        long allowedLatenessMillis,
                        int maxOpenWindows,
                        int maxDedupEntries,
                        int maxRetainedResults,
                        Consumer<WindowResult> resultListener) {
        if (windowSizeMillis <= 0) {
            throw new IllegalArgumentException("windowSizeMillis must be > 0");
        }
        this.windowSizeMillis = windowSizeMillis;
        this.maxOpenWindows = maxOpenWindows;
        this.maxRetainedResults = maxRetainedResults;
        this.watermarkTracker = new WatermarkTracker(allowedLatenessMillis);
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
            long watermark = watermarkTracker.currentWatermark();
            if (event.eventTimeMillis() <= watermark) {
                log.info("ingest LATE_REJECTED eventId={} eventTime={} watermark={}",
                        event.eventId(), event.eventTimeMillis(), watermark);
                return IngestResult.of(IngestStatus.LATE_REJECTED,
                        "eventTime " + event.eventTimeMillis() + " <= watermark " + watermark);
            }
            if (dedupStore.isDuplicate(event.eventId(), event.eventTimeMillis())) {
                log.info("ingest DUPLICATE eventId={} (dedup by eventId)", event.eventId());
                return IngestResult.of(IngestStatus.DUPLICATE,
                        "duplicate eventId " + event.eventId());
            }
            // 先推进水位并回收已确定窗口/过期去重条目，再做容量检查，
            // 避免“负责推进水位的事件”因尚未回收的旧窗口而被误判背压。
            advanceWatermarkAndFinalize(event.eventTimeMillis());

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

    private void advanceWatermarkAndFinalize(long eventTimeMillis) {
        long watermark = watermarkTracker.observe(eventTimeMillis);
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
            return watermarkTracker.currentWatermark();
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
                    watermarkTracker.currentWatermark(),
                    dedupStore.snapshot(),
                    windowsCopy,
                    List.copyOf(finalizedResults));
        } finally {
            lock.unlock();
        }
    }

    public void restore(CheckpointData data) {
        lock.lock();
        try {
            watermarkTracker.restore(data.watermark());
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
