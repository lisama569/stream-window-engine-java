package com.github.lisama569.streamwindowenginejava.model;

/**
 * 摄取结论：状态 + 所属窗口起点（若可判定）+ 人类可读原因。
 */
public record IngestResult(IngestStatus status, Long windowStart, String reason) {

    public static IngestResult of(IngestStatus status, Long windowStart, String reason) {
        return new IngestResult(status, windowStart, reason);
    }
}
