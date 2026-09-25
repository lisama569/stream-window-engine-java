package com.github.lisama569.streamwindowenginejava.model;

/**
 * 摄取结果状态：每种结论可区分，绝不静默计入或丢弃。
 */
public enum IngestStatus {
    /** 正常计入活跃窗口 */
    ACCEPTED,
    /** 事件时间早于当前水位，但仍在允许迟到范围内，计入宽限窗口并触发结果订正 */
    ACCEPTED_LATE,
    /** 超出允许迟到范围，被拒绝并进入有界死信日志，不影响任何窗口结果 */
    REJECTED_TOO_LATE,
    /** eventId 已见过（含重启恢复后），不重复计数 */
    DUPLICATE,
    /** 有界缓冲/状态达到上限，施加背压；事件未被计入，调用方应重试 */
    BACKPRESSURE
}
