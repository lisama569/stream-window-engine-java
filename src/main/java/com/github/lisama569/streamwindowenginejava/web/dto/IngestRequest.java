package com.github.lisama569.streamwindowenginejava.web.dto;

/** 摄取请求体 */
public record IngestRequest(String eventId, String key, long eventTime, long value) {
}
