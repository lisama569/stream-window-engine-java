package com.github.lisama569.streamwindowenginejava.tests;

import com.github.lisama569.streamwindowenginejava.window.WindowAssigner;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class WindowAssignerTest {

    private final WindowAssigner assigner = new WindowAssigner(60_000);

    @Test
    void assignsTumblingWindows() {
        assertThat(assigner.windowStart(0)).isEqualTo(0);
        assertThat(assigner.windowStart(59_999)).isEqualTo(0);
        assertThat(assigner.windowStart(60_000)).isEqualTo(60_000);
        assertThat(assigner.windowEnd(60_000)).isEqualTo(120_000);
    }

    @Test
    void handlesNegativeEventTime() {
        // 负事件时间也应落在对齐的窗口内
        assertThat(assigner.windowStart(-1)).isEqualTo(-60_000);
        assertThat(assigner.windowEnd(-1)).isEqualTo(0);
    }

    @Test
    void rejectsNonPositiveWindowSize() {
        assertThatThrownBy(() -> new WindowAssigner(0)).isInstanceOf(IllegalArgumentException.class);
    }
}
