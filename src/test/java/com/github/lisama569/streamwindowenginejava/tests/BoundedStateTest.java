package com.github.lisama569.streamwindowenginejava.tests;

import com.github.lisama569.streamwindowenginejava.checkpoint.CheckpointManager;
import com.github.lisama569.streamwindowenginejava.config.EngineProperties;
import com.github.lisama569.streamwindowenginejava.engine.StreamEngine;
import com.github.lisama569.streamwindowenginejava.model.Event;
import com.github.lisama569.streamwindowenginejava.model.IngestResult;
import com.github.lisama569.streamwindowenginejava.model.IngestStatus;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * 有界状态与背压：缓冲达到上限时以可区分方式施加背压（不丢数据、不崩溃）；
 * 状态随水位推进回收，内存不随事件总数无界增长。
 */
class BoundedStateTest {

    private static final Logger log = LoggerFactory.getLogger(BoundedStateTest.class);
    private static final long WINDOW = 60_000;
    private static final long LATENESS = 30_000;

    @TempDir
    Path checkpointDir;

    private StreamEngine newEngine(int maxBufferedWindows, int maxSeenEventIds) {
        EngineProperties props = new EngineProperties();
        props.setWindowSizeMs(WINDOW);
        props.setAllowedLatenessMs(LATENESS);
        props.setMaxBufferedWindows(maxBufferedWindows);
        props.setMaxSeenEventIds(maxSeenEventIds);
        props.setCheckpointDir(checkpointDir.toString());
        StreamEngine e = new StreamEngine(props, new CheckpointManager(props));
        e.restore();
        return e;
    }

    @Test
    void backpressureWhenWindowBufferIsFullAndRecoversAfterReclaim() {
        StreamEngine engine = newEngine(2, 10_000);

        // 填满 2 个窗口缓冲（窗口 [0,60s) 与 [60s,120s)）
        assertThat(engine.ingest(new Event("w0", "k1", 1_000, 1)).status()).isEqualTo(IngestStatus.ACCEPTED);
        assertThat(engine.ingest(new Event("w1", "k1", 61_000, 1)).status()).isEqualTo(IngestStatus.ACCEPTED);

        // 第三个窗口触发背压：可区分、未计入、不崩溃
        IngestResult bp = engine.ingest(new Event("w2", "k1", 121_000, 1));
        log.info("backpressure: status={} reason={} stats={}", bp.status(), bp.reason(), engine.stats());
        assertThat(bp.status()).isEqualTo(IngestStatus.BACKPRESSURE);

        // 推进水位回收旧窗口后，重试同一事件成功：背压期间没有丢数据，只是延迟
        engine.advanceWatermark(WINDOW + LATENESS); // 回收窗口 [0,60s)
        IngestResult retry = engine.ingest(new Event("w2", "k1", 121_000, 1));
        log.info("retry after reclaim: status={} watermark={}", retry.status(), engine.currentWatermark());
        assertThat(retry.status()).isEqualTo(IngestStatus.ACCEPTED);
    }

    @Test
    void backpressureWhenSeenIndexIsFull() {
        StreamEngine engine = newEngine(10_000, 3);
        assertThat(engine.ingest(new Event("s0", "k1", 1_000, 1)).status()).isEqualTo(IngestStatus.ACCEPTED);
        assertThat(engine.ingest(new Event("s1", "k1", 2_000, 1)).status()).isEqualTo(IngestStatus.ACCEPTED);
        assertThat(engine.ingest(new Event("s2", "k1", 3_000, 1)).status()).isEqualTo(IngestStatus.ACCEPTED);
        IngestResult bp = engine.ingest(new Event("s3", "k1", 4_000, 1));
        log.info("seen-index backpressure: status={} reason={}", bp.status(), bp.reason());
        assertThat(bp.status()).isEqualTo(IngestStatus.BACKPRESSURE);
    }

    @Test
    void stateIsReclaimedAsWatermarkAdvances() {
        StreamEngine engine = newEngine(10_000, 10_000);

        // 写入 100 个窗口、共 1000 条事件
        for (int w = 0; w < 100; w++) {
            for (int i = 0; i < 10; i++) {
                engine.ingest(new Event("e-" + w + "-" + i, "k1", w * WINDOW + i * 1_000L, 1));
            }
        }
        assertThat(engine.stats().get("activeWindows")).isEqualTo(100);
        assertThat(engine.stats().get("seenEventIds")).isEqualTo(1000);

        // 水位推进到第 50 个窗口终点 + 允许迟到：前 50 个窗口状态被回收
        engine.advanceWatermark(50 * WINDOW + LATENESS);
        log.info("after reclaim: stats={}", engine.stats());
        assertThat(engine.stats().get("activeWindows")).isEqualTo(50);
        assertThat(engine.stats().get("seenEventIds")).isEqualTo(500);
        assertThat(engine.results("k1")).hasSize(50);

        // 水位推过全部窗口：活跃状态清零，仅保留有界的已定版结果
        engine.advanceWatermark(100 * WINDOW + LATENESS);
        log.info("after full reclaim: stats={}", engine.stats());
        assertThat(engine.stats().get("activeWindows")).isEqualTo(0);
        assertThat(engine.stats().get("seenEventIds")).isEqualTo(0);
        assertThat(engine.results("k1")).hasSize(100);
    }

    @Test
    void watermarkMustBeStrictlyIncreasing() {
        StreamEngine engine = newEngine(10_000, 10_000);
        engine.advanceWatermark(WINDOW);
        assertThatThrownBy(() -> engine.advanceWatermark(WINDOW))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> engine.advanceWatermark(WINDOW - 1))
                .isInstanceOf(IllegalArgumentException.class);
    }
}
