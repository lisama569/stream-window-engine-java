package com.github.lisama569.streamwindowenginejava.checkpoint;

import com.github.lisama569.streamwindowenginejava.model.WindowResult;
import com.github.lisama569.streamwindowenginejava.window.AggregateAccumulator;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * 检查点持久化数据：水位、活跃/宽限窗口状态、已归档结果、去重索引。
 * 整体可 JSON 序列化，写入时附带 magic、版本与 CRC32 校验和。
 */
public class CheckpointData {

    /** 当前水位 */
    private long watermark = Long.MIN_VALUE;

    /** 活跃/宽限窗口状态：key -> (windowStart -> accumulator) */
    private Map<String, Map<Long, AggregateAccumulator>> activeWindows = new LinkedHashMap<>();

    /** 已定版窗口结果（含迟到订正历史中的最新版） */
    private List<WindowResult> finalizedResults = new ArrayList<>();

    /** 已见 eventId 去重索引（有界） */
    private List<String> seenEventIds = new ArrayList<>();

    /** 每个窗口的结果版本计数器：key -> (windowStart -> 已用最大版本号) */
    private Map<String, Map<Long, Long>> windowVersions = new LinkedHashMap<>();

    public long getWatermark() {
        return watermark;
    }

    public void setWatermark(long watermark) {
        this.watermark = watermark;
    }

    public Map<String, Map<Long, AggregateAccumulator>> getActiveWindows() {
        return activeWindows;
    }

    public void setActiveWindows(Map<String, Map<Long, AggregateAccumulator>> activeWindows) {
        this.activeWindows = activeWindows;
    }

    public List<WindowResult> getFinalizedResults() {
        return finalizedResults;
    }

    public void setFinalizedResults(List<WindowResult> finalizedResults) {
        this.finalizedResults = finalizedResults;
    }

    public List<String> getSeenEventIds() {
        return seenEventIds;
    }

    public void setSeenEventIds(List<String> seenEventIds) {
        this.seenEventIds = seenEventIds;
    }

    public Map<String, Map<Long, Long>> getWindowVersions() {
        return windowVersions;
    }

    public void setWindowVersions(Map<String, Map<Long, Long>> windowVersions) {
        this.windowVersions = windowVersions;
    }
}
