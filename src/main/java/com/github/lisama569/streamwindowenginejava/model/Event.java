package com.github.lisama569.streamwindowenginejava.model;

/**
 * 输入事件。eventId 全局唯一，用于幂等去重；eventTime 为事件时间（毫秒）。
 */
public record Event(String eventId, String key, long eventTime, long value) {
}
