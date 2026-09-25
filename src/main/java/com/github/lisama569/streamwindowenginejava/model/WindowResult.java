package com.github.lisama569.streamwindowenginejava.model;

import java.util.List;

/**
 * 窗口聚合结果。version 单调递增：水位首次越过 windowEnd 后定版，
 * 允许迟到范围内的事件会触发订正并递增版本；超过允许迟到后结果被清理。
 */
public record WindowResult(
        String key,
        long windowStart,
        long windowEnd,
        long count,
        long sum,
        List<String> eventIds,
        long version) {
}
