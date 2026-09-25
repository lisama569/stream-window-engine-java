package com.github.lisama569.streamwindowenginejava.window;

/**
 * 滚动窗口分配器：把事件时间映射到 [start, end) 窗口。
 */
public class WindowAssigner {

    private final long windowSizeMs;

    public WindowAssigner(long windowSizeMs) {
        if (windowSizeMs <= 0) {
            throw new IllegalArgumentException("windowSizeMs must be positive");
        }
        this.windowSizeMs = windowSizeMs;
    }

    /** 事件时间所属窗口起点（含） */
    public long windowStart(long eventTime) {
        return Math.floorDiv(eventTime, windowSizeMs) * windowSizeMs;
    }

    /** 事件时间所属窗口终点（不含） */
    public long windowEnd(long eventTime) {
        return windowStart(eventTime) + windowSizeMs;
    }

    public long windowSizeMs() {
        return windowSizeMs;
    }
}
