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
import static org.junit.jupiter.api.Assertions.assertNull;
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
    void emptyCheckpointRestartDoesNotBlockSourcedIngestion() {
        // 进程空跑（无事件、无标记）后正常关停，落盘的是没有任何来源的空检查点
        Path file = dir.resolve("checkpoint.json");
        CheckpointStore store = new CheckpointStore(file);
        WindowEngine first = newEngine();
        store.save(first.snapshot());
        log.info("empty checkpoint saved: watermark={} sources={}",
                first.currentWatermark(), first.sourceCount());

        // 重启后上游带来源标识发事件：水位必须正常推进、窗口按时定版
        WindowEngine recovered = newEngine();
        CheckpointData data = store.load().orElseThrow();
        recovered.restore(data);
        log.info("recovered from empty checkpoint: formatVersion={} watermark={} sources={}",
                data.formatVersion(), recovered.currentWatermark(), recovered.sourceCount());
        assertEquals(0, recovered.sourceCount(),
                "empty checkpoint must not register any phantom source");

        assertEquals(IngestStatus.ACCEPTED,
                recovered.ingest(new Event("a1", "k", 2100, 1, "A")).status());
        assertEquals(IngestStatus.ACCEPTED,
                recovered.ingest(new Event("b1", "k", 2200, 2, "B")).status());
        assertEquals(MarkerStatus.APPLIED,
                recovered.submitMarker(new SourceMarker("mA-1", "A", 10_000)).status());
        assertEquals(MarkerStatus.APPLIED,
                recovered.submitMarker(new SourceMarker("mB-1", "B", 10_000)).status());
        log.info("sourced ingestion after empty restart: watermark={} sourceProgressA={} sourceProgressB={}",
                recovered.currentWatermark(), recovered.sourceProgress("A"),
                recovered.sourceProgress("B"));
        assertEquals(10_000 - LATENESS, recovered.currentWatermark(),
                "watermark must advance after restart from an empty checkpoint");

        List<WindowResult> results = recovered.finalizedResults();
        results.forEach(r -> log.info("finalized after empty-checkpoint restart: {}", r));
        assertEquals(1, results.size());
        assertEquals(2000, results.get(0).windowStart());
        assertEquals(2, results.get(0).totalCount());
        assertEquals(3, results.get(0).totalValue());
    }

    @Test
    void emptyCheckpointRestartKeepsLegacySingleSourceSemantics() {
        // 同一份空检查点，换成不带来源的老用法也必须正常推进水位、按时定版
        Path file = dir.resolve("checkpoint.json");
        CheckpointStore store = new CheckpointStore(file);
        store.save(newEngine().snapshot());

        WindowEngine recovered = newEngine();
        recovered.restore(store.load().orElseThrow());
        assertEquals(IngestStatus.ACCEPTED,
                recovered.ingest(new Event("e1", "k", 2100, 1)).status());
        assertEquals(IngestStatus.ACCEPTED,
                recovered.ingest(new Event("e2", "k", 10_000, 2)).status());
        log.info("legacy single-source after empty restart: watermark={} sources={}",
                recovered.currentWatermark(), recovered.sourceCount());
        assertEquals(10_000 - LATENESS, recovered.currentWatermark());

        List<WindowResult> results = recovered.finalizedResults();
        results.forEach(r -> log.info("finalized (legacy usage) after empty restart: {}", r));
        assertEquals(1, results.size());
        assertEquals(2000, results.get(0).windowStart());
        assertEquals(1, results.get(0).totalCount());
    }

    @Test
    void legacyV2CheckpointWithMixedDedupEntriesIsSplitOnRestore() throws Exception {
        // 手工构造 v2 格式检查点：标记去重条目以 "M:" 前缀混在 dedupEntries 里
        Path file = dir.resolve("checkpoint.json");
        ObjectMapper mapper = new ObjectMapper();
        byte[] payload = mapper.writeValueAsBytes(mapper.createObjectNode()
                .put("formatVersion", 2)
                .put("watermark", 1000)
                .putPOJO("dedupEntries", java.util.Map.of("e1", 5000L, "M:m1", 6000L))
                .putPOJO("openWindows", java.util.Map.of())
                .putPOJO("finalizedResults", java.util.List.of())
                .putPOJO("sourceProgress", java.util.Map.of("A", 3000L)));
        CRC32 crc = new CRC32();
        crc.update(payload);
        var envelope = mapper.createObjectNode()
                .put("formatVersion", 2)
                .put("checksum", crc.getValue())
                .put("payload", java.util.Base64.getEncoder().encodeToString(payload));
        Files.write(file, mapper.writeValueAsBytes(envelope));

        WindowEngine engine = newEngine();
        CheckpointData data = new CheckpointStore(file).load().orElseThrow();
        engine.restore(data);
        log.info("v2 restored: formatVersion={} watermark={} sourceProgressA={}",
                data.formatVersion(), engine.currentWatermark(), engine.sourceProgress("A"));

        // 拆分后各自命中各自的去重表
        IngestResult dupEvent = engine.ingest(new Event("e1", "k", 5000, 1, "A"));
        MarkerResult dupMarker = engine.submitMarker(new SourceMarker("m1", "A", 7000));
        log.info("after v2 restore: resubmit event e1 -> {} (dedup basis: eventId), "
                        + "resubmit marker m1 -> {} (dedup basis: markerId)",
                dupEvent.status(), dupMarker.status());
        assertEquals(IngestStatus.DUPLICATE, dupEvent.status());
        assertEquals(MarkerStatus.DUPLICATE, dupMarker.status());

        // 拆分后互不干扰：与标记同号的事件、与事件同号的标记都按各自语义处理
        assertEquals(IngestStatus.ACCEPTED,
                engine.ingest(new Event("m1", "k", 6000, 1, "A")).status(),
                "event id equal to a legacy markerId must not be dropped");
        assertEquals(MarkerStatus.APPLIED,
                engine.submitMarker(new SourceMarker("e1", "A", 8000)).status(),
                "marker id equal to a legacy eventId must still apply");
        assertEquals(8000, engine.sourceProgress("A"));
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
        assertNull(data.get().sourceProgress(),
                "v1 payload has no sourceProgress field; null marks the legacy format");

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
