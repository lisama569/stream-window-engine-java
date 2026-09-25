package com.github.lisama569.streamwindowenginejava.model;

import java.util.Map;

/**
 * 窗口在水位满足条件后确定下来的最终聚合结果。
 */
public record WindowResult(long windowStart,
                           long windowEnd,
                           long totalCount,
                           long totalValue,
                           Map<String, Long> countByKey) {
}
