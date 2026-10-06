package com.github.lisama569.streamwindowenginejava.core;

import java.util.HashMap;
import java.util.Map;

/**
 * 多来源进度跟踪器。
 *
 * 语义：
 * - 每个已注册来源维护单调不减的进度：已见最大事件时间，或已生效标记声明的进度，
 *   两者取大；
 * - 全局水位 = min(各来源进度) - allowedLateness，单调不减；
 * - 来源只有被事件或标记注册后才参与水位计算，因此不带来源信息的旧用法
 *   （只有默认来源）与旧版单路 WatermarkTracker 语义完全一致；
 * - 空闲来源通过标记推进进度后，不再拖住其它来源已满足条件的窗口结论；
 * - 来源数量达到上限时 registerSource 返回 false，由引擎施加背压。
 *
 * 线程安全由调用方（WindowEngine 的统一锁）保证。
 */
public class SourceProgressTracker {

    /** 不带来源信息的事件/标记归入的默认来源。 */
    public static final String DEFAULT_SOURCE = "";

    private final long allowedLatenessMillis;
    private final int maxSources;
    /** sourceId -> 该来源单调不减的进度（事件时间语义） */
    private final Map<String, Long> progressBySource = new HashMap<>();
    private long watermark = Long.MIN_VALUE;

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

    /** null 来源归入默认来源。 */
    public static String normalize(String sourceId) {
        return sourceId == null ? DEFAULT_SOURCE : sourceId;
    }

    /**
     * 登记来源（幂等）。来源数超上限且来源未知时返回 false（背压），
     * 已注册来源重复登记返回 true。
     */
    public boolean registerSource(String sourceId) {
        String id = normalize(sourceId);
        if (progressBySource.containsKey(id)) {
            return true;
        }
        if (progressBySource.size() >= maxSources) {
            return false;
        }
        progressBySource.put(id, Long.MIN_VALUE);
        return true;
    }

    /** 观察某来源的事件时间，推进该来源进度，返回推进后的全局水位。 */
    public long observeEvent(String sourceId, long eventTimeMillis) {
        String id = normalize(sourceId);
        long current = progressBySource.getOrDefault(id, Long.MIN_VALUE);
        if (eventTimeMillis > current) {
            progressBySource.put(id, eventTimeMillis);
            recomputeWatermark();
        }
        return watermark;
    }

    /**
     * 应用标记进度。
     *
     * @return true 表示生效（进度推进）；false 表示过期/互相矛盾
     *         （不领先于该来源已知进度），未生效
     */
    public boolean applyMarker(String sourceId, long progressTimeMillis) {
        String id = normalize(sourceId);
        long current = progressBySource.getOrDefault(id, Long.MIN_VALUE);
        if (progressTimeMillis <= current) {
            return false;
        }
        progressBySource.put(id, progressTimeMillis);
        recomputeWatermark();
        return true;
    }

    private void recomputeWatermark() {
        long minProgress = Long.MAX_VALUE;
        for (long p : progressBySource.values()) {
            minProgress = Math.min(minProgress, p);
        }
        long candidate = minProgress == Long.MIN_VALUE || minProgress == Long.MAX_VALUE
                ? Long.MIN_VALUE
                : minProgress - allowedLatenessMillis;
        if (candidate > watermark) {
            watermark = candidate;
        }
    }

    public long currentWatermark() {
        return watermark;
    }

    /** 该来源当前已知进度；未知来源返回 Long.MIN_VALUE。 */
    public long sourceProgress(String sourceId) {
        return progressBySource.getOrDefault(normalize(sourceId), Long.MIN_VALUE);
    }

    public int sourceCount() {
        return progressBySource.size();
    }

    public Map<String, Long> snapshot() {
        return Map.copyOf(progressBySource);
    }

    /**
     * 从检查点恢复。
     *
     * 兼容规则（严格按检查点格式版本区分，不越界套用）：
     * - legacySingleSource=true（v1 旧格式，无来源进度字段）：sourceProgress 为空时
     *   以 watermark + allowedLateness 重建默认来源进度，与旧版
     *   WatermarkTracker.restore 语义一致，恢复后仍按单路语义工作；
     * - legacySingleSource=false（v2+ 新格式）：sourceProgress 如实恢复；
     *   空表表示空跑检查点（从未注册过任何来源），恢复后同样没有任何来源，
     *   绝不虚构默认来源——否则该来源会以固定进度参与 min() 计算，
     *   把水位永远钉死，导致重启后带来源的事件被接受但窗口永远不定版。
     */
    public void restore(long watermark, Map<String, Long> sourceProgress, boolean legacySingleSource) {
        progressBySource.clear();
        if (sourceProgress == null || sourceProgress.isEmpty()) {
            if (legacySingleSource) {
                progressBySource.put(DEFAULT_SOURCE, safeAdd(watermark));
            }
            // 新格式的空表：保持无任何来源，等真实事件/标记注册
        } else {
            progressBySource.putAll(sourceProgress);
        }
        this.watermark = watermark;
    }

    private long safeAdd(long watermark) {
        long v = watermark + allowedLatenessMillis;
        return v < watermark ? Long.MAX_VALUE : v; // 溢出保护
    }
}
