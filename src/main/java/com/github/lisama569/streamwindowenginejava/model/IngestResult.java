package com.github.lisama569.streamwindowenginejava.model;

/**
 * 单次摄取的可区分结论，附带人可读的原因说明。
 */
public record IngestResult(IngestStatus status, String reason) {

    public static IngestResult accepted() {
        return new IngestResult(IngestStatus.ACCEPTED, "accepted");
    }

    public static IngestResult of(IngestStatus status, String reason) {
        return new IngestResult(status, reason);
    }
}
