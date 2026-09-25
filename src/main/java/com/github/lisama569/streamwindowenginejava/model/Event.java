package com.github.lisama569.streamwindowenginejava.model;

/**
 * 输入事件。eventId 全局唯一，作为去重依据；eventTimeMillis 为事件时间（epoch 毫秒）。
 */
public record Event(String eventId, String key, long eventTimeMillis, long value) {
}
