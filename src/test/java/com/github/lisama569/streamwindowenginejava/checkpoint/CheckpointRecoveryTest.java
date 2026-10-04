package com.github.lisama569.streamwindowenginejava.checkpoint;

import com.github.lisama569.streamwindowenginejava.core.WindowEngine;
import com.github.lisama569.streamwindowenginejava.model.Event;
import com.github.lisama569.streamwindowenginejava.model.IngestResult;
import com.github.lisama569.streamwindowenginejava.model.IngestStatus;
import com.github.lisama569.streamwindowenginejava.model.MarkerResult;
import com.github.lisama569.streamwindowenginejava.model.MarkerStatus;
import com.github.lisama569.streamwindowenginejava.model.SourceMarker;
import com.github.lisama569.streamwindowenginejava.model.WindowResult;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.zip.CRC32;
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

    @Test
    void restartRecoversSourceProgressAndMarkerDedup() {
        Path file = dir.resolve("checkpoint.json");
        CheckpointStore store = new CheckpointStore(file);

        // 第一次运行：两个来源的事件 + 一个空闲标记，然后落盘
        WindowEngine first = newEngine();
        first.ingest(new Event("a1", "k", 100, 1, "fast"));
        first.ingest(new Event("b1", "k", 200, 1, "slow"));
        first.ingest(new Event("a2", "k", 5000, 1, "fast"));
        assertEquals(MarkerStatus.ACCEPTED,
                first.ingestMarker(new SourceMarker("m1", "slow", 5000)).status());
        store.save(first.snapshot());
        log.info("checkpoint saved: global={} sources={}", first.currentWatermark(), first.sourceWatermarks());

        // 模拟重启：恢复来源水位与标记去重表
        WindowEngine recovered = newEngine();
        Optional<CheckpointData> data = store.load();
        assertTrue(data.isPresent());
        recovered.restore(data.get());
        log.info("recovered: global={} sources={}", recovered.currentWatermark(), recovered.sourceWatermarks());
        assertEquals(first.currentWatermark(), recovered.currentWatermark());
        assertEquals(first.sourceWatermarks(), recovered.sourceWatermarks());

        // 重启后重复提交事件与标记：可区分且不重复生效
        IngestResult dupEvent = recovered.ingest(new Event("a1", "k", 100, 1, "fast"));
        log.info("resubmitted event after restart: status={} reason={}", dupEvent.status(), dupEvent.reason());
        assertTrue(dupEvent.status() == IngestStatus.DUPLICATE
                || dupEvent.status() == IngestStatus.LATE_REJECTED);
        MarkerResult dupMarker = recovered.ingestMarker(new SourceMarker("m1", "slow", 5000));
        log.info("resubmitted marker after restart: status={} reason={}", dupMarker.status(), dupMarker.reason());
        assertTrue(dupMarker.status() == MarkerStatus.DUPLICATE
                || dupMarker.status() == MarkerStatus.STALE,
                "resubmitted marker must be distinguishable, got " + dupMarker.status());

        // 重启后续跑：与无重启对照组结论一致
        recovered.ingest(new Event("a3", "k", 20_000, 1, "fast"));
        recovered.ingestMarker(new SourceMarker("m2", "slow", 30_000));
        recovered.ingest(new Event("a4", "k", 30_000, 1, "fast"));
        List<WindowResult> afterRestart = recovered.finalizedResults();
        afterRestart.forEach(r -> log.info("finalized window after restart: {}", r));

        WindowEngine reference = newEngine();
        reference.ingest(new Event("a1", "k", 100, 1, "fast"));
        reference.ingest(new Event("b1", "k", 200, 1, "slow"));
        reference.ingest(new Event("a2", "k", 5000, 1, "fast"));
        reference.ingestMarker(new SourceMarker("m1", "slow", 5000));
        reference.ingest(new Event("a3", "k", 20_000, 1, "fast"));
        reference.ingestMarker(new SourceMarker("m2", "slow", 30_000));
        reference.ingest(new Event("a4", "k", 30_000, 1, "fast"));
        assertEquals(reference.finalizedResults(), afterRestart,
                "restart + duplicate resubmission must not change multi-source results");
    }

    @Test
    void legacyV1CheckpointIsMigratedWithClearRules() throws Exception {
        // 手工构造 v1 格式检查点（无来源状态）：全局水位 3000、一条去重条目、一个未关闭窗口
        Path file = dir.resolve("checkpoint.json");
        var mapper = new com.fasterxml.jackson.databind.ObjectMapper();
        Map<String, Object> payload = Map.of(
                "formatVersion", 1,
                "watermark", 3000,
                "dedupEntries", Map.of("old-1", 5000),
                "openWindows", Map.of("0", Map.of("k", new long[]{2, 11})),
                "finalizedResults", List.of());
        byte[] payloadBytes = mapper.writeValueAsBytes(payload);
        CRC32 crc = new CRC32();
        crc.update(payloadBytes);
        var envelope = mapper.createObjectNode();
        envelope.put("formatVersion", 1);
        envelope.put("checksum", crc.getValue());
        envelope.put("payload", Base64.getEncoder().encodeToString(payloadBytes));
        Files.write(file, mapper.writeValueAsBytes(envelope));

        // 兼容规则：整体迁移——全局水位归入默认来源，标记去重表为空
        Optional<CheckpointData> loaded = new CheckpointStore(file).load();
        assertTrue(loaded.isPresent(), "v1 checkpoint must be accepted via migration rule");
        WindowEngine engine = newEngine();
        engine.restore(loaded.get());
        log.info("migrated v1 checkpoint: global={} sources={}",
                engine.currentWatermark(), engine.sourceWatermarks());
        assertEquals(3000, engine.currentWatermark());
        assertEquals(Map.of("__default__", 3000L), engine.sourceWatermarks());

        // 恢复后语义完整：去重、迟到、窗口结算都按单路规则工作，不是只恢复一半
        assertEquals(IngestStatus.DUPLICATE,
                engine.ingest(new Event("old-1", "k", 5000, 1)).status());
        assertEquals(IngestStatus.LATE_REJECTED,
                engine.ingest(new Event("late-1", "k", 2500, 1)).status());
        assertEquals(IngestStatus.ACCEPTED,
                engine.ingest(new Event("pusher", "k", 10_000, 0)).status());
        List<WindowResult> results = engine.finalizedResults();
        results.forEach(r -> log.info("finalized window from migrated checkpoint: {}", r));
        assertEquals(1, results.size());
        assertEquals(2, results.get(0).totalCount(), "open window state from v1 must be fully restored");
        assertEquals(11, results.get(0).totalValue());
    }
}
