package com.github.lisama569.streamwindowenginejava.model;

/**
 * 来源进度/空闲标记：声明 sourceId 在 progressTimeMillis（事件时间）之前的数据已经到齐、
 * 当前处于空闲。标记生效后该来源以 progressTimeMillis 参与全局水位计算，
 * 不再因长时间无数据而拖住其它来源已满足条件的窗口结论。
 * markerId 全局唯一，作为标记去重依据（重复标记幂等，不重复推进进度）。
 */
public record SourceMarker(String markerId, String sourceId, long progressTimeMillis) {
}
