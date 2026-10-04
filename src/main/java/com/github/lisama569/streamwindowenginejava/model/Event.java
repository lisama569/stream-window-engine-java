package com.github.lisama569.streamwindowenginejava.model;

/**
 * 输入事件。eventId 全局唯一，作为去重依据；eventTimeMillis 为事件时间（epoch 毫秒）。
 * sourceId 可选：标识事件来自哪一路上游来源；为 null 时按默认来源（单路语义）处理，
 * 旧调用方不带来源信息时行为与多来源能力引入前完全一致。
 */
public record Event(String eventId, String key, long eventTimeMillis, long value, String sourceId) {

    /** 兼容旧单路用法：不带来源标识，归入默认来源。 */
    public Event(String eventId, String key, long eventTimeMillis, long value) {
        this(eventId, key, eventTimeMillis, value, null);
    }
}
