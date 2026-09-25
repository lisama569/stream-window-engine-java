package com.github.lisama569.streamwindowenginejava.checkpoint;

import com.github.lisama569.streamwindowenginejava.core.WindowEngine;
import com.github.lisama569.streamwindowenginejava.model.Event;
import com.github.lisama569.streamwindowenginejava.model.IngestResult;
import com.github.lisama569.streamwindowenginejava.model.IngestStatus;
import com.github.lisama569.streamwindowenginejava.model.WindowResult;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Optional;
import java.util.stream.IntStream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 检查点与恢复测试：重启后续跑、重复提交不重复计数、损坏/版本不兼容明确拒绝。
 */
class CheckpointRecoveryTest {

    private static final Logger log = LoggerFactory.getLogger(CheckpointRecoveryTest.class);

    private static final long WINDOW = 1000;
    private static final long LATENESS = 2000;

    @TempDir
    Path dir;

    private WindowEngine newEngine() {
        return new WindowEngine(WINDOW, LATENESS, 1024, 100_000, 10_000,
                r -> log.info("listener observed window result: {}", r));
    }

    private List<Event> batch(String prefix, int from, int to, long baseTime) {
        // 事件时间落在 [baseTime, baseTime+1000) 的同一窗口
        return IntStream.range(from, to)
                .mapToObj(i -> new Event(prefix + "-" + i, "key-" + (i % 2), baseTime + (i % 1000), i))
                .toList();
    }

    @Test
    void restartRecoversAndDuplicateResubmissionDoesNotDoubleCount() {
        Path file = dir.resolve("checkpoint.json");
        CheckpointStore store = new CheckpointStore(file);

        // 第一次运行：摄取两批事件后落盘检查点
        WindowEngine first = newEngine();
        List<Event> batchA = batch("a", 0, 50, 0);
        List<Event> batchB = batch("b", 0, 50, 1000);
        batchA.forEach(e -> assertEquals(IngestStatus.ACCEPTED, first.ingest(e).status()));
        batchB.forEach(e -> assertEquals(IngestStatus.ACCEPTED, first.ingest(e).status()));
        store.save(first.snapshot());
        log.info("checkpoint saved at watermark={}", first.currentWatermark());

        // 模拟重启：新引擎从检查点恢复
        WindowEngine recovered = newEngine();
        Optional<CheckpointData> data = store.load();
        assertTrue(data.isPresent());
        recovered.restore(data.get());
        log.info("recovered watermark={}", recovered.currentWatermark());
        assertEquals(first.currentWatermark(), recovered.currentWatermark());

        // 重启后重复提交同一批事件：不得重复计数
        int duplicates = 0;
        for (Event e : batchA) {
            IngestResult r = recovered.ingest(e);
            log.info("resubmit {} -> {} ({})", e.eventId(), r.status(), r.reason());
            assertTrue(r.status() == IngestStatus.DUPLICATE || r.status() == IngestStatus.LATE_REJECTED,
                    "resubmitted event must be distinguishable, got " + r.status());
            if (r.status() == IngestStatus.DUPLICATE) {
                duplicates++;
            }
        }
        log.info("resubmission summary: duplicates={} (dedup basis: eventId persisted in checkpoint)",
                duplicates);

        // 推进水位完成两个窗口
        assertEquals(IngestStatus.ACCEPTED,
                recovered.ingest(new Event("pusher", "k", 10_000, 0)).status());
        List<WindowResult> afterRestart = recovered.finalizedResults();
        afterRestart.forEach(r -> log.info("finalized window after restart: {}", r));

        // 对照组：无重启一次性跑完
        WindowEngine reference = newEngine();
        batchA.forEach(reference::ingest);
        batchB.forEach(reference::ingest);
        reference.ingest(new Event("pusher", "k", 10_000, 0));
        List<WindowResult> expected = reference.finalizedResults();

        assertEquals(expected, afterRestart, "restart + duplicate resubmission must not change results");
        assertEquals(50, afterRestart.get(0).totalCount(), "no double count after recovery");
        assertEquals(50, afterRestart.get(1).totalCount(), "no double count after recovery");
    }

    @Test
    void corruptedCheckpointIsRejected() throws Exception {
        Path file = dir.resolve("checkpoint.json");
        CheckpointStore store = new CheckpointStore(file);
        store.save(newEngine().snapshot());

        // 篡改负载字节，破坏 CRC 校验
        byte[] bytes = Files.readAllBytes(file);
        bytes[bytes.length / 2] ^= 0xFF;
        Files.write(file, bytes);

        CheckpointException ex = assertThrows(CheckpointException.class, store::load);
        log.info("corrupted checkpoint rejected: {}", ex.getMessage());
    }

    @Test
    void garbageCheckpointIsRejected() throws Exception {
        Path file = dir.resolve("checkpoint.json");
        Files.writeString(file, "this is not a checkpoint");
        CheckpointException ex = assertThrows(CheckpointException.class,
                () -> new CheckpointStore(file).load());
        log.info("garbage checkpoint rejected: {}", ex.getMessage());
    }

    @Test
    void incompatibleVersionIsRejected() throws Exception {
        Path file = dir.resolve("checkpoint.json");
        CheckpointStore store = new CheckpointStore(file);
        store.save(newEngine().snapshot());

        // 保留合法结构但改写格式版本
        String json = Files.readString(file);
        Files.writeString(file, json.replace(
                "\"formatVersion\":" + CheckpointData.CURRENT_FORMAT_VERSION,
                "\"formatVersion\":999"));

        CheckpointException ex = assertThrows(CheckpointException.class, store::load);
        log.info("incompatible checkpoint rejected: {}", ex.getMessage());
        assertTrue(ex.getMessage().contains("incompatible"));
    }

    @Test
    void missingCheckpointYieldsEmpty() {
        CheckpointStore store = new CheckpointStore(dir.resolve("absent.json"));
        assertTrue(store.load().isEmpty(), "missing checkpoint must yield empty, not an error");
    }
}
