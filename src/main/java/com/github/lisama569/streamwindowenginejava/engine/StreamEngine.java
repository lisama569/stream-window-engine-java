package com.github.lisama569.streamwindowenginejava.engine;

import com.github.lisama569.streamwindowenginejava.checkpoint.CheckpointData;
import com.github.lisama569.streamwindowenginejava.checkpoint.CheckpointManager;
import com.github.lisama569.streamwindowenginejava.checkpoint.CheckpointMissingException;
import com.github.lisama569.streamwindowenginejava.config.EngineProperties;
import com.github.lisama569.streamwindowenginejava.model.*;
import com.github.lisama569.streamwindowenginejava.window.AggregateAccumulator;
import com.github.lisama569.streamwindowenginejava.window.WindowAssigner;
import jakarta.annotation.PostConstruct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.util.*;

/**
 * 流式窗口聚合引擎：事件时间滚动窗口 + 显式水位推进 + 有界状态 + 检查点恢复。
 *
 * <p>语义约定：
 * <ul>
 *   <li>窗口为事件时间滚动窗口 [start, end)，按 key 分组聚合 count/sum。</li>
 *   <li>水位由外部显式推进且必须单调递增；水位越过 windowEnd 时窗口定版，
 *       越过 windowEnd + allowedLateness 时窗口状态被回收。</li>
 *   <li>eventId 全局去重：同一 eventId 重复提交（含重启恢复后）不重复计数。</li>
 *   <li>所有公共方法 synchronized，并发摄取下结果与串行执行一致。</li>
 * </ul>
 */
@Component
public class StreamEngine {

    private static final Logger log = LoggerFactory.getLogger(StreamEngine.class);

    /** 已定版结果的键 */
    private record WindowKey(String key, long windowStart) {
    }

    private final EngineProperties properties;
    private final CheckpointManager checkpointManager;
    private final WindowAssigner assigner;

    /** 当前水位（事件时间，毫秒） */
    private long watermark = Long.MIN_VALUE;

    /** 活跃/宽限窗口状态：key -> (windowStart -> accumulator) */
    private final Map<String, Map<Long, AggregateAccumulator>> activeWindows = new LinkedHashMap<>();

    /** 已定版窗口结果（插入序，用于有界淘汰最老结果） */
    private final LinkedHashMap<WindowKey, WindowResult> finalizedResults = new LinkedHashMap<>();

    /** 每个窗口已用最大结果版本号：key -> (windowStart -> version) */
    private final Map<String, Map<Long, Long>> windowVersions = new LinkedHashMap<>();

    /** 已见 eventId 去重索引（有界） */
    private final LinkedHashSet<String> seenEventIds = new LinkedHashSet<>();

    /** 有界死信日志：超出允许迟到范围的事件 */
    private final Deque<DeadLetter> deadLetters = new ArrayDeque<>();
    private long deadLettersDropped;

    public StreamEngine(EngineProperties properties, CheckpointManager checkpointManager) {
        this.properties = properties;
        this.checkpointManager = checkpointManager;
        this.assigner = new WindowAssigner(properties.getWindowSizeMs());
    }

    /** 启动时从最近一次有效检查点恢复；缺失/损坏/版本不兼容按配置明确拒绝 */
    @PostConstruct
    public synchronized void restore() {
        Optional<CheckpointData> loaded = checkpointManager.load();
        if (loaded.isEmpty()) {
            if (properties.isCheckpointRequiredOnStartup()) {
                throw new CheckpointMissingException(
                        "checkpoint required on startup but none found in " + properties.getCheckpointDir());
            }
            log.info("no checkpoint found, starting fresh");
            return;
        }
        CheckpointData data = loaded.get();
        this.watermark = data.getWatermark();
        data.getActiveWindows().forEach((key, windows) ->
                activeWindows.put(key, new LinkedHashMap<>(windows)));
        data.getFinalizedResults().forEach(r ->
                finalizedResults.put(new WindowKey(r.key(), r.windowStart()), r));
        data.getWindowVersions().forEach((key, versions) ->
                windowVersions.put(key, new LinkedHashMap<>(versions)));
        seenEventIds.addAll(data.getSeenEventIds());
        log.info("restored from checkpoint: watermark={}, activeWindows={}, finalizedResults={}, seenEventIds={}",
                watermark, activeWindowCount(), finalizedResults.size(), seenEventIds.size());
    }

    /**
     * 摄取单条事件，返回可区分的处理结论（见 {@link IngestStatus}）。
     * 任何路径都不会静默计入或丢弃事件。
     */
    public synchronized IngestResult ingest(Event event) {
        validate(event);
        long windowStart = assigner.windowStart(event.eventTime());
        long windowEnd = windowStart + assigner.windowSizeMs();

        // 1. 去重：eventId 已见过（含重启恢复后），不重复计数
        if (seenEventIds.contains(event.eventId())) {
            log.info("ingest DUPLICATE eventId={} key={} eventTime={} watermark={} (dedup by eventId)",
                    event.eventId(), event.key(), event.eventTime(), watermark);
            return IngestResult.of(IngestStatus.DUPLICATE, windowStart,
                    "eventId already seen, not counted again");
        }

        // 2. 背压：去重索引达到上限
        if (seenEventIds.size() >= properties.getMaxSeenEventIds()) {
            log.warn("ingest BACKPRESSURE eventId={} reason=seen-event-ids index full ({}/{})",
                    event.eventId(), seenEventIds.size(), properties.getMaxSeenEventIds());
            return IngestResult.of(IngestStatus.BACKPRESSURE, windowStart,
                    "seen-event-ids index at capacity " + properties.getMaxSeenEventIds() + ", retry later");
        }

        // 3. 迟到分类
        if (event.eventTime() <= watermark) {
            if (windowEnd + properties.getAllowedLatenessMs() <= watermark) {
                // 3a. 超出允许迟到范围：拒绝并进入有界死信日志，不影响任何窗口结果
                addDeadLetter(event, "event time beyond allowed lateness");
                log.warn("ingest REJECTED_TOO_LATE eventId={} key={} eventTime={} windowEnd={} watermark={} allowedLatenessMs={}",
                        event.eventId(), event.key(), event.eventTime(), windowEnd, watermark,
                        properties.getAllowedLatenessMs());
                return IngestResult.of(IngestStatus.REJECTED_TOO_LATE, windowStart,
                        "event time beyond allowed lateness (watermark=" + watermark + ")");
            }
            // 3b. 允许迟到范围内：计入宽限窗口并触发结果订正
            AggregateAccumulator acc = activeWindow(event.key(), windowStart, event);
            if (acc == null) {
                return backpressure(windowStart);
            }
            acc.add(event.eventId(), event.value());
            seenEventIds.add(event.eventId());
            WindowResult corrected = finalizeWindow(event.key(), windowStart, acc);
            log.info("ingest ACCEPTED_LATE eventId={} key={} eventTime={} watermark={} -> corrected window=[{},{}) count={} sum={} version={}",
                    event.eventId(), event.key(), event.eventTime(), watermark,
                    windowStart, windowEnd, corrected.count(), corrected.sum(), corrected.version());
            persistIfNeeded();
            return IngestResult.of(IngestStatus.ACCEPTED_LATE, windowStart,
                    "late within allowed lateness, window result corrected to version " + corrected.version());
        }

        // 4. 正常路径
        AggregateAccumulator acc = activeWindow(event.key(), windowStart, event);
        if (acc == null) {
            return backpressure(windowStart);
        }
        acc.add(event.eventId(), event.value());
        seenEventIds.add(event.eventId());
        log.debug("ingest ACCEPTED eventId={} key={} eventTime={} window=[{},{})",
                event.eventId(), event.key(), event.eventTime(), windowStart, windowEnd);
        persistIfNeeded();
        return IngestResult.of(IngestStatus.ACCEPTED, windowStart, "accepted into active window");
    }

    /**
     * 推进水位（必须严格单调递增）。
     * 水位越过 windowEnd 的窗口定版；越过 windowEnd + allowedLateness 的窗口状态被回收。
     *
     * @return 推进后的水位
     */
    public synchronized long advanceWatermark(long newWatermark) {
        if (newWatermark <= watermark) {
            throw new IllegalArgumentException(
                    "watermark must be strictly increasing: current=" + watermark + ", requested=" + newWatermark);
        }
        long previous = this.watermark;
        this.watermark = newWatermark;

        // 定版：windowEnd <= watermark 且尚未定版。定版后状态保留（宽限期），
        // 供允许迟到范围内的事件订正；是否已定版由 windowVersions 判定（已持久化）。
        int finalized = 0;
        for (var keyEntry : activeWindows.entrySet()) {
            String key = keyEntry.getKey();
            Map<Long, Long> versions = windowVersions.getOrDefault(key, Map.of());
            for (var winEntry : keyEntry.getValue().entrySet()) {
                long windowStart = winEntry.getKey();
                long windowEnd = windowStart + assigner.windowSizeMs();
                if (windowEnd <= watermark && !versions.containsKey(windowStart)) {
                    WindowResult result = finalizeWindow(key, windowStart, winEntry.getValue());
                    finalized++;
                    log.info("window finalized: key={} window=[{},{}) count={} sum={} version={} watermark={}",
                            key, windowStart, windowEnd, result.count(), result.sum(), result.version(), watermark);
                }
            }
        }

        // 回收：windowEnd + allowedLateness <= watermark，状态与版本计数器一并清除
        int purged = 0;
        var keyIt = activeWindows.entrySet().iterator();
        while (keyIt.hasNext()) {
            var keyEntry = keyIt.next();
            var winIt = keyEntry.getValue().entrySet().iterator();
            while (winIt.hasNext()) {
                var winEntry = winIt.next();
                long windowEnd = winEntry.getKey() + assigner.windowSizeMs();
                if (windowEnd + properties.getAllowedLatenessMs() <= watermark) {
                    // 该窗口的去重依据随状态一并回收：此后同 eventId 再来只会被判为超迟到拒绝
                    seenEventIds.removeAll(winEntry.getValue().getEventIds());
                    Map<Long, Long> versions = windowVersions.get(keyEntry.getKey());
                    if (versions != null) {
                        versions.remove(winEntry.getKey());
                    }
                    winIt.remove();
                    purged++;
                }
            }
            if (keyEntry.getValue().isEmpty()) {
                keyIt.remove();
            }
        }

        evictFinalizedIfNeeded();
        checkpointManager.save(snapshot());
        log.info("watermark advanced: {} -> {}, finalizedWindows={}, purgedWindows={}, activeWindows={}, seenEventIds={}",
                previous, watermark, finalized, purged, activeWindowCount(), seenEventIds.size());
        return watermark;
    }

    public synchronized long currentWatermark() {
        return watermark;
    }

    /** 查询某 key 的全部已定版窗口结果（按窗口起点升序） */
    public synchronized List<WindowResult> results(String key) {
        return finalizedResults.values().stream()
                .filter(r -> r.key().equals(key))
                .sorted(Comparator.comparingLong(WindowResult::windowStart))
                .toList();
    }

    /** 死信日志快照（观测用） */
    public synchronized List<DeadLetter> deadLetters() {
        return List.copyOf(deadLetters);
    }

    /** 当前状态快照（用于观测与测试） */
    public synchronized Map<String, Object> stats() {
        Map<String, Object> stats = new LinkedHashMap<>();
        stats.put("watermark", watermark);
        stats.put("activeWindows", activeWindowCount());
        stats.put("finalizedResults", finalizedResults.size());
        stats.put("seenEventIds", seenEventIds.size());
        stats.put("deadLetters", deadLetters.size());
        stats.put("deadLettersDropped", deadLettersDropped);
        return stats;
    }

    // ------------------------------------------------------------------

    private void validate(Event event) {
        if (event == null || event.eventId() == null || event.eventId().isBlank()) {
            throw new IllegalArgumentException("eventId must be non-blank");
        }
        if (event.key() == null || event.key().isBlank()) {
            throw new IllegalArgumentException("key must be non-blank");
        }
    }

    /** 取窗口状态；不存在时按容量约束创建，容量不足返回 null（背压） */
    private AggregateAccumulator activeWindow(String key, long windowStart, Event event) {
        Map<Long, AggregateAccumulator> windows = activeWindows.computeIfAbsent(key, k -> new LinkedHashMap<>());
        AggregateAccumulator acc = windows.get(windowStart);
        if (acc == null) {
            if (activeWindowCount() >= properties.getMaxBufferedWindows()) {
                log.warn("ingest BACKPRESSURE eventId={} reason=buffered windows full ({}/{})",
                        event.eventId(), activeWindowCount(), properties.getMaxBufferedWindows());
                return null;
            }
            acc = new AggregateAccumulator();
            windows.put(windowStart, acc);
        }
        return acc;
    }

    private IngestResult backpressure(Long windowStart) {
        return IngestResult.of(IngestStatus.BACKPRESSURE, windowStart,
                "buffered windows at capacity " + properties.getMaxBufferedWindows() + ", retry later");
    }

    /** 生成/更新窗口的定版结果，版本号单调递增；eventIds 排序以保证结果与到达顺序无关 */
    private WindowResult finalizeWindow(String key, long windowStart, AggregateAccumulator acc) {
        long windowEnd = windowStart + assigner.windowSizeMs();
        long version = windowVersions
                .computeIfAbsent(key, k -> new LinkedHashMap<>())
                .merge(windowStart, 1L, Long::sum);
        List<String> sortedIds = acc.getEventIds().stream().sorted().toList();
        WindowResult result = new WindowResult(key, windowStart, windowEnd,
                acc.getCount(), acc.getSum(), sortedIds, version);
        finalizedResults.put(new WindowKey(key, windowStart), result);
        return result;
    }

    private void evictFinalizedIfNeeded() {
        var it = finalizedResults.entrySet().iterator();
        while (finalizedResults.size() > properties.getMaxStoredResults() && it.hasNext()) {
            WindowKey evicted = it.next().getKey();
            it.remove();
            log.info("finalized result evicted (bounded store): key={} windowStart={}", evicted.key(), evicted.windowStart());
        }
    }

    private void addDeadLetter(Event event, String reason) {
        if (deadLetters.size() >= properties.getMaxDeadLetters()) {
            deadLetters.pollFirst();
            deadLettersDropped++;
        }
        deadLetters.addLast(new DeadLetter(event, reason));
    }

    private int activeWindowCount() {
        return activeWindows.values().stream().mapToInt(Map::size).sum();
    }

    private void persistIfNeeded() {
        if (properties.isCheckpointOnIngest()) {
            checkpointManager.save(snapshot());
        }
    }

    private CheckpointData snapshot() {
        CheckpointData data = new CheckpointData();
        data.setWatermark(watermark);
        Map<String, Map<Long, AggregateAccumulator>> active = new LinkedHashMap<>();
        activeWindows.forEach((key, windows) -> active.put(key, new LinkedHashMap<>(windows)));
        data.setActiveWindows(active);
        data.setFinalizedResults(new ArrayList<>(finalizedResults.values()));
        data.setSeenEventIds(new ArrayList<>(seenEventIds));
        Map<String, Map<Long, Long>> versions = new LinkedHashMap<>();
        windowVersions.forEach((key, v) -> versions.put(key, new LinkedHashMap<>(v)));
        data.setWindowVersions(versions);
        return data;
    }
}
