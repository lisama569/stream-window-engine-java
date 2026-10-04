package com.github.lisama569.streamwindowenginejava.model;

/**
 * 来源进度/空闲标记：声明“来源 sourceId 在事件时间 progressTimeMillis 之前（含）的数据已经到齐，
 * 当前空闲”。标记之后该来源以其进度前沿参与全局水位计算，不再拖住其它来源的窗口结算。
 *
 * markerId 全局唯一，作为标记去重依据；重复提交同一 markerId 判为 DUPLICATE，不重复生效。
 * 标记是权威声明：来源恢复发数据时，事件时间 <= 全局水位的事件会被可区分地判为
 * LATE_REJECTED（不静默丢弃，也不静默计入）；来源不应发出与自己已发标记矛盾的事件。
 */
public record SourceMarker(String markerId, String sourceId, long progressTimeMillis) {
}
