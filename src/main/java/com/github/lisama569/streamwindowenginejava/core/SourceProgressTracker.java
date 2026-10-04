package com.github.lisama569.streamwindowenginejava.core;

import java.util.HashMap;
import java.util.Map;

/**
 * 多来源进度跟踪器。
 *
 * 每个来源维护一个单调不减的来源水位：
 *   来源水位 = max(空闲标记声明的进度前沿, 该来源已见最大事件时间 - 允许迟到时长)
 * 全局水位 = 所有来源水位的最小值，单调不减。
 * 空闲标记把来源水位抬到标记声明的进度前沿，因此长时间无数据的来源只要发了标记，
 * 就不会拖住其它来源的窗口结算。
 *
 * 新来源注册时其初始水位以当前全局水位为下界：保证全局水位单调不减，
 * 且比全局水位更旧的数据本就会被判迟到，下界收紧不改变任何结论。
 */
public class SourceProgressTracker {

    /** 不带来源标识的旧调用方统一归入的默认来源。 */
    public static final String DEFAULT_SOURCE_ID = "__default__";

    private final long allowedLatenessMillis;
    private final int maxSources;
    /** sourceId -> 来源水位 */
    private final Map<String, Long> sourceWatermarks = new HashMap<>();
    private long globalWatermark = Long.MIN_VALUE;

    public SourceProgressTracker(long allowedLatenessMillis, int maxSources) {
        if (allowedLatenessMillis < 0) {
            throw new IllegalArgumentException("allowedLatenessMillis must be >= 0");
        }
        if (maxSources <= 0) {
            throw new IllegalArgumentException("maxSources must be > 0");
        }
        this.allowedLatenessMillis = allowedLatenessMillis;
        this.maxSources = maxSources;
    }

    public static String normalize(String sourceId) {
        return (sourceId == null || sourceId.isBlank()) ? DEFAULT_SOURCE_ID : sourceId;
    }

    public boolean hasSource(String sourceId) {
        return sourceWatermarks.containsKey(normalize(sourceId));
    }

    /** @return true 表示该来源已存在或尚未达到来源数上限，可以注册 */
    public boolean canAccommodate(String sourceId) {
        return hasSource(sourceId) || sourceWatermarks.size() < maxSources;
    }

    public long sourceWatermark(String sourceId) {
        Long wm = sourceWatermarks.get(normalize(sourceId));
        return wm == null ? Long.MIN_VALUE : wm;
    }

    /** 观察一个事件时间：推进对应来源水位，返回推进后的全局水位。 */
    public long observeEvent(String sourceId, long eventTimeMillis) {
        String src = normalize(sourceId);
        long candidate = eventTimeMillis - allowedLatenessMillis;
        Long old = sourceWatermarks.get(src);
        if (old == null) {
            // 新来源：已有其它来源时初始水位以当前全局水位为下界，保证全局水位单调
            long initial = sourceWatermarks.isEmpty()
                    ? candidate
                    : Math.max(candidate, globalWatermark);
            registerSource(src, initial);
        } else if (candidate > old) {
            sourceWatermarks.put(src, candidate);
            raiseGlobal(old);
        }
        return globalWatermark;
    }

    /**
     * 应用一个空闲/进度标记：把来源水位抬到 progressTimeMillis。
     * 调用方需先完成 STALE/容量校验。返回推进后的全局水位。
     */
    public long applyMarker(String sourceId, long progressTimeMillis) {
        String src = normalize(sourceId);
        Long old = sourceWatermarks.get(src);
        if (old == null) {
            registerSource(src, progressTimeMillis);
        } else if (progressTimeMillis > old) {
            sourceWatermarks.put(src, progressTimeMillis);
            raiseGlobal(old);
        }
        return globalWatermark;
    }

    /** 注册新来源：首个来源直接决定全局水位；其余来源初始水位不低于当前全局水位。 */
    private void registerSource(String src, long initialWatermark) {
        sourceWatermarks.put(src, initialWatermark);
        if (sourceWatermarks.size() == 1 || initialWatermark < globalWatermark) {
            globalWatermark = initialWatermark;
        }
    }

    /** 被抬升的来源若原本是全局水位的最小者，重算全局水位（其余情况全局水位不变）。 */
    private void raiseGlobal(long oldSourceWatermark) {
        if (oldSourceWatermark == globalWatermark) {
            long min = Long.MAX_VALUE;
            for (long wm : sourceWatermarks.values()) {
                min = Math.min(min, wm);
            }
            globalWatermark = min;
        }
    }

    public long globalWatermark() {
        return globalWatermark;
    }

    public int sourceCount() {
        return sourceWatermarks.size();
    }

    public Map<String, Long> snapshot() {
        return Map.copyOf(sourceWatermarks);
    }

    /** 从检查点恢复各来源水位；全局水位取各来源最小值。 */
    public void restore(Map<String, Long> restored) {
        sourceWatermarks.clear();
        sourceWatermarks.putAll(restored);
        long min = Long.MAX_VALUE;
        for (long wm : sourceWatermarks.values()) {
            min = Math.min(min, wm);
        }
        globalWatermark = sourceWatermarks.isEmpty() ? Long.MIN_VALUE : min;
    }
}
