package com.github.lisama569.streamwindowenginejava.model;

/**
 * 输入事件。eventId 全局唯一，作为去重依据；eventTimeMillis 为事件时间（epoch 毫秒）。
 * sourceId 标识事件来源（多路接入场景）；为 null 时按默认单路来源处理，
 * 与旧版单来源语义完全一致。
 */
public record Event(String eventId, String key, long eventTimeMillis, long value, String sourceId) {

    /** 兼容旧调用方：不带来源信息，归入默认来源。 */
    public Event(String eventId, String key, long eventTimeMillis, long value) {
        this(eventId, key, eventTimeMillis, value, null);
    }
}
