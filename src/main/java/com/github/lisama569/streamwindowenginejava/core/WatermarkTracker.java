package com.github.lisama569.streamwindowenginejava.core;

/**
 * 水位跟踪器：watermark = 已见最大事件时间 - 允许迟到时长。
 * 水位单调不减；事件时间早于或等于水位的事件视为超出允许迟到范围。
 */
public class WatermarkTracker {

    private final long allowedLatenessMillis;
    private long maxEventTime = Long.MIN_VALUE;
    private long watermark = Long.MIN_VALUE;

    public WatermarkTracker(long allowedLatenessMillis) {
        if (allowedLatenessMillis < 0) {
            throw new IllegalArgumentException("allowedLatenessMillis must be >= 0");
        }
        this.allowedLatenessMillis = allowedLatenessMillis;
    }

    /** 观察一个事件时间，返回推进后的水位（单调不减）。 */
    public long observe(long eventTimeMillis) {
        if (eventTimeMillis > maxEventTime) {
            maxEventTime = eventTimeMillis;
            long candidate = maxEventTime - allowedLatenessMillis;
            if (candidate > watermark) {
                watermark = candidate;
            }
        }
        return watermark;
    }

    public long currentWatermark() {
        return watermark;
    }

    /** 从检查点恢复水位；maxEventTime 以 watermark + allowedLateness 为下界恢复。 */
    public void restore(long watermark) {
        this.watermark = watermark;
        this.maxEventTime = Math.max(maxEventTime, watermark + allowedLatenessMillis);
    }
}
