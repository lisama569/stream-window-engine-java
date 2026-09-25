package com.github.lisama569.streamwindowenginejava.model;

/**
 * 摄取结论，互相可区分：
 * ACCEPTED       - 已计入对应窗口
 * DUPLICATE      - 与已处理事件 eventId 重复，未重复计数
 * LATE_REJECTED  - 事件时间已落后于水位（超出允许迟到范围），拒绝计入
 * BACKPRESSURE   - 内部缓冲/状态达到上限，未计入，调用方应稍后重试
 */
public enum IngestStatus {
    ACCEPTED,
    DUPLICATE,
    LATE_REJECTED,
    BACKPRESSURE
}
