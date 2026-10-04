package com.github.lisama569.streamwindowenginejava.model;

/**
 * 单次标记提交的可区分结论，附带人可读的原因说明。
 */
public record MarkerResult(MarkerStatus status, String reason) {

    public static MarkerResult of(MarkerStatus status, String reason) {
        return new MarkerResult(status, reason);
    }
}
