package com.github.lisama569.streamwindowenginejava.checkpoint;

import com.fasterxml.jackson.databind.ObjectMapper;
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
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.zip.CRC32;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 多来源场景的检查点恢复：来源进度与标记去重随检查点持久化，
 * 重启后重复提交与重复标记不改变结果；旧版（v1，无来源进度表）检查点
 * 按清晰规则兼容恢复为单路语义。
 */
class MultiSourceCheckpointRecoveryTest {

    private static final Logger log = LoggerFactory.getLogger(MultiSourceCheckpointRecoveryTest.class);

    private static final long WINDOW = 1000;
    private static final long LATENESS = 2000;

    @TempDir
    Path dir;

    private WindowEngine newEngine() {
        return new WindowEngine(WINDOW, LATENESS, 1024, 100_000, 10_000, 100,
                r -> log.info("listener observed window result: {}", r));
    }

    private static List<WindowResult> sorted(List<WindowResult> results) {
        return results.stream()
                .sorted(Comparator.comparingLong(WindowResult::windowStart))
                .toList();
    }

    /** 两个来源的基础数据：窗口 [2000,3000) 由标记定版，之后两来源继续到 [9000,10000)。 */
    private void ingestPhaseOne(WindowEngine engine) {
        assertEquals(IngestStatus.ACCEPTED,
                engine.ingest(new Event("a1", "k", 2100, 1, "A")).status());
        assertEquals(IngestStatus.ACCEPTED,
                engine.ingest(new Event("a2", "k", 2500, 2, "A")).status());
        assertEquals(IngestStatus.ACCEPTED,
                engine.ingest(new Event("b1", "k", 2200, 4, "B")).status());
        assertEquals(MarkerStatus.APPLIED,
                engine.submitMarker(new SourceMarker("mA-1", "A", 10_000)).status());
        assertEquals(MarkerStatus.APPLIED,
                engine.submitMarker(new SourceMarker("mB-1", "B", 10_000)).status());
        assertEquals(IngestStatus.ACCEPTED,
                engine.ingest(new Event("a3", "k", 9000, 8, "A")).status());
        assertEquals(IngestStatus.ACCEPTED,
                engine.ingest(new Event("b2", "k", 9500, 16, "B")).status());
    }

    private void ingestPhaseTwo(WindowEngine engine) {
        assertEquals(IngestStatus.ACCEPTED,
                engine.ingest(new Event("a4", "k", 11_000, 32, "A")).status());
        assertEquals(IngestStatus.ACCEPTED,
                engine.ingest(new Event("b3", "k", 11_500, 64, "B")).status());
        assertEquals(MarkerStatus.APPLIED,
                engine.submitMarker(new SourceMarker("mA-2", "A", 20_000)).status());
        assertEquals(MarkerStatus.APPLIED,
                engine.submitMarker(new SourceMarker("mB-2", "B", 20_000)).status());
    }

    @Test
    void restartRecoversSourcesAndMarkersWithoutDoubleCounting() {
        Path file = dir.resolve("checkpoint.json");
        CheckpointStore store = new CheckpointStore(file);

        WindowEngine first = newEngine();
        ingestPhaseOne(first);
        store.save(first.snapshot());
        log.info("checkpoint saved: watermark={} sourceProgressA={} sourceProgressB={}",
                first.currentWatermark(), first.sourceProgress("A"), first.sourceProgress("B"));

        // 模拟重启
        WindowEngine recovered = newEngine();
        Optional<CheckpointData> data = store.load();
        assertTrue(data.isPresent());
        recovered.restore(data.get());
        log.info("recovered: watermark={} sourceProgressA={} sourceProgressB={} sources={}",
                recovered.currentWatermark(), recovered.sourceProgress("A"),
                recovered.sourceProgress("B"), recovered.sourceCount());
        assertEquals(first.currentWatermark(), recovered.currentWatermark());
        assertEquals(first.sourceProgress("A"), recovered.sourceProgress("A"));
        assertEquals(first.sourceProgress("B"), recovered.sourceProgress("B"));

        // 重启后重复提交事件：不重复计数（结论可区分：DUPLICATE 或 LATE_REJECTED）
        for (Event e : List.of(new Event("a1", "k", 2100, 1, "A"),
                new Event("a3", "k", 9000, 8, "A"))) {
            IngestResult r = recovered.ingest(e);
            log.info("resubmit event {} -> {} ({}) (dedup basis: eventId)",
                    e.eventId(), r.status(), r.reason());
            assertTrue(r.status() == IngestStatus.DUPLICATE
                    || r.status() == IngestStatus.LATE_REJECTED);
        }
        // 重启后重复标记：幂等，判 DUPLICATE（去重依据 markerId 随检查点持久化）
        MarkerResult dupMarker = recovered.submitMarker(new SourceMarker("mB-1", "B", 10_000));
        log.info("resubmit marker mB-1 -> {} ({}) (dedup basis: markerId)",
                dupMarker.status(), dupMarker.reason());
        assertEquals(MarkerStatus.DUPLICATE, dupMarker.status());
        // 过期标记：可区分且不生效
        assertEquals(MarkerStatus.STALE,
                recovered.submitMarker(new SourceMarker("mB-x", "B", 5000)).status());

        // 继续正常摄取并完成全部窗口
        ingestPhaseTwo(recovered);
        List<WindowResult> afterRestart = sorted(recovered.finalizedResults());
        afterRestart.forEach(r -> log.info("finalized after restart: {}", r));

        // 对照组：无重启一次性跑完
        WindowEngine reference = newEngine();
        ingestPhaseOne(reference);
        ingestPhaseTwo(reference);
        List<WindowResult> expected = sorted(reference.finalizedResults());

        assertEquals(expected, afterRestart,
                "restart + duplicate resubmission must not change window results");
    }

    @Test
    void emptyCheckpointRestartDoesNotPinWatermark() {
        Path file = dir.resolve("checkpoint.json");
        CheckpointStore store = new CheckpointStore(file);

        // 空跑进程：一条事件、一个标记都没有，正常关停落盘
        WindowEngine emptyRun = newEngine();
        store.save(emptyRun.snapshot());
        CheckpointData saved = store.load().orElseThrow();
        log.info("empty checkpoint saved: formatVersion={} watermark={} sources={}",
                saved.formatVersion(), saved.watermark(), saved.sourceProgress());

        // 重启后上游开始带来源标识发事件：水位必须正常推进、窗口按时定版
        WindowEngine restarted = newEngine();
        restarted.restore(saved);
        assertEquals(0, restarted.sourceCount(),
                "empty checkpoint must not resurrect a phantom source");
        assertEquals(IngestStatus.ACCEPTED,
                restarted.ingest(new Event("a1", "k", 5000, 1, "A")).status());
        assertEquals(IngestStatus.ACCEPTED,
                restarted.ingest(new Event("a2", "k", 10_000, 2, "A")).status());
        log.info("after restart with sourced events: watermark={} sourceProgressA={}",
                restarted.currentWatermark(), restarted.sourceProgress("A"));
        assertEquals(10_000 - LATENESS, restarted.currentWatermark(),
                "watermark must advance after empty-checkpoint restart");
        assertEquals(MarkerStatus.APPLIED,
                restarted.submitMarker(new SourceMarker("mA-1", "A", 20_000)).status());
        List<WindowResult> results = restarted.finalizedResults();
        results.forEach(r -> log.info("finalized after empty-checkpoint restart: {}", r));
        assertTrue(results.stream().anyMatch(r -> r.windowStart() == 5000 && r.totalCount() == 1)
                        && results.stream().anyMatch(r -> r.windowStart() == 10_000 && r.totalCount() == 1),
                "both windows must finalize on time after empty-checkpoint restart");

        // 同样的空检查点，换成不带来源的老用法也必须正常
        WindowEngine legacy = newEngine();
        legacy.restore(saved);
        assertEquals(IngestStatus.ACCEPTED,
                legacy.ingest(new Event("e1", "k", 7000, 1)).status());
        assertEquals(7000 - LATENESS, legacy.currentWatermark(),
                "legacy source-less usage must advance watermark after empty-checkpoint restart");
        log.info("legacy usage after empty-checkpoint restart: watermark={}",
                legacy.currentWatermark());
    }

    @Test
    void eventAndMarkerDedupNamespacesStaySeparateAcrossRestart() {
        Path file = dir.resolve("checkpoint.json");
        CheckpointStore store = new CheckpointStore(file);

        // 事件 "42"、事件 "M:42"（ID 携带标记前缀）与标记 "42" 共存后落盘
        WindowEngine first = newEngine();
        assertEquals(IngestStatus.ACCEPTED,
                first.ingest(new Event("42", "k", 5000, 1, "A")).status());
        assertEquals(MarkerStatus.APPLIED,
                first.submitMarker(new SourceMarker("42", "B", 10_000)).status());
        assertEquals(IngestStatus.ACCEPTED,
                first.ingest(new Event("M:42", "k", 9500, 2, "A")).status());
        store.save(first.snapshot());
        log.info("checkpoint saved with colliding ids: watermark={} dedup={}",
                first.currentWatermark(), first.snapshot().dedupEntries());

        // 重启：三类条目各自判重，互不误判
        WindowEngine recovered = newEngine();
        recovered.restore(store.load().orElseThrow());
        IngestResult dupEvent = recovered.ingest(new Event("M:42", "k", 9500, 2, "A"));
        MarkerResult dupMarker = recovered.submitMarker(new SourceMarker("42", "B", 10_000));
        log.info("after restart: resubmit event M:42 -> {} (dedup basis: eventId); "
                        + "resubmit marker 42 -> {} (dedup basis: markerId)",
                dupEvent.status(), dupMarker.status());
        assertEquals(IngestStatus.DUPLICATE, dupEvent.status());
        assertEquals(MarkerStatus.DUPLICATE, dupMarker.status());

        // 新标记/新事件不受对方命名空间影响，正常生效
        assertEquals(MarkerStatus.APPLIED,
                recovered.submitMarker(new SourceMarker("M:42", "B", 20_000)).status());
        assertEquals(IngestStatus.ACCEPTED,
                recovered.ingest(new Event("43", "k", 15_000, 4, "A")).status());
        log.info("after restart: watermark={} progressB={}",
                recovered.currentWatermark(), recovered.sourceProgress("B"));
    }

    @Test
    void legacyV1CheckpointWithoutSourceProgressRestoresAsSingleSource() throws Exception {
        Path file = dir.resolve("checkpoint.json");
        // 手工构造 v1 格式检查点：没有 sourceProgress 字段
        ObjectMapper mapper = new ObjectMapper();
        byte[] payload = mapper.writeValueAsBytes(mapper.createObjectNode()
                .put("formatVersion", 1)
                .put("watermark", 3000)
                .putPOJO("dedupEntries", java.util.Map.of("e1", 100))
                .putPOJO("openWindows", java.util.Map.of())
                .putPOJO("finalizedResults", java.util.List.of()));
        CRC32 crc = new CRC32();
        crc.update(payload);
        var envelope = mapper.createObjectNode()
                .put("formatVersion", 1)
                .put("checksum", crc.getValue())
                .put("payload", java.util.Base64.getEncoder().encodeToString(payload));
        Files.write(file, mapper.writeValueAsBytes(envelope));

        Optional<CheckpointData> data = new CheckpointStore(file).load();
        assertTrue(data.isPresent(), "v1 checkpoint must be accepted by the compat rule");
        assertTrue(data.get().sourceProgress().isEmpty());

        WindowEngine engine = newEngine();
        engine.restore(data.get());
        log.info("v1 restored: watermark={} sources={}", engine.currentWatermark(), engine.sourceCount());
        assertEquals(3000, engine.currentWatermark());

        // 恢复后仍按单路语义工作：迟到拒绝、新事件推进水位
        IngestResult late = engine.ingest(new Event("e2", "k", 2500, 1));
        assertEquals(IngestStatus.LATE_REJECTED, late.status());
        assertEquals(IngestStatus.ACCEPTED,
                engine.ingest(new Event("e3", "k", 10_000, 1)).status());
        assertEquals(10_000 - LATENESS, engine.currentWatermark(),
                "single-source semantics must be preserved after v1 restore");
        log.info("v1 compat verified: watermark advanced to {}", engine.currentWatermark());
    }
}
