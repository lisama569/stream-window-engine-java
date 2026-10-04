package com.github.lisama569.streamwindowenginejava.core;

import com.github.lisama569.streamwindowenginejava.model.Event;
import com.github.lisama569.streamwindowenginejava.model.IngestStatus;
import com.github.lisama569.streamwindowenginejava.model.MarkerResult;
import com.github.lisama569.streamwindowenginejava.model.MarkerStatus;
import com.github.lisama569.streamwindowenginejava.model.SourceMarker;
import com.github.lisama569.streamwindowenginejava.model.WindowResult;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 多来源接入与来源空闲/进度标记测试。
 * 日志打印来源、各来源水位、全局水位、窗口结果与去重依据，结论可复现、失败原因可区分。
 */
class MultiSourceWindowTest {

    private static final Logger log = LoggerFactory.getLogger(MultiSourceWindowTest.class);

    private static final long WINDOW = 1000;
    private static final long LATENESS = 2000;

    private WindowEngine newEngine() {
        return new WindowEngine(WINDOW, LATENESS, 1024, 100_000, 10_000,
                r -> log.info("listener observed window result: {}", r));
    }

    private static Event ev(String id, long t, String source) {
        return new Event(id, "k", t, 1, source);
    }

    @Test
    void idleSourceDoesNotBlockWindowFinalization() {
        WindowEngine engine = newEngine();
        // 两个来源共同落入窗口 [0,1000)
        assertEquals(IngestStatus.ACCEPTED, engine.ingest(ev("a1", 100, "fast")).status());
        assertEquals(IngestStatus.ACCEPTED, engine.ingest(ev("a2", 500, "fast")).status());
        assertEquals(IngestStatus.ACCEPTED, engine.ingest(ev("b1", 200, "slow")).status());

        // fast 推进到 5000，但 slow 停更：slow 水位 = 200-2000 拖住全局水位
        assertEquals(IngestStatus.ACCEPTED, engine.ingest(ev("a3", 5000, "fast")).status());
        log.info("slow idle without marker: sourceWatermarks={} global={}",
                engine.sourceWatermarks(), engine.currentWatermark());
        assertTrue(engine.finalizedResults().isEmpty(),
                "idle source without marker must hold back finalization");

        // slow 发出空闲标记：声明 5000 前已到齐 -> 全局水位推进到 fast 的水位 3000
        MarkerResult m = engine.ingestMarker(new SourceMarker("m1", "slow", 5000));
        log.info("idle marker result: status={} reason={} sourceWatermarks={} global={}",
                m.status(), m.reason(), engine.sourceWatermarks(), engine.currentWatermark());
        assertEquals(MarkerStatus.ACCEPTED, m.status());
        assertEquals(3000, engine.currentWatermark());

        List<WindowResult> results = engine.finalizedResults();
        results.forEach(r -> log.info("finalized window after idle marker: {}", r));
        assertEquals(1, results.size(), "window [0,1000) must finalize once slow is marked idle");
        assertEquals(0, results.get(0).windowStart());
        assertEquals(3, results.get(0).totalCount(), "events from both sources must be counted");
    }

    @Test
    void resumedSourceCountsNormallyAndExpiredEventsAreDistinguishable() {
        WindowEngine engine = newEngine();
        engine.ingest(ev("a1", 100, "fast"));
        engine.ingest(ev("b1", 200, "slow"));
        engine.ingest(ev("a2", 5000, "fast"));
        engine.ingestMarker(new SourceMarker("m1", "slow", 5000));
        long globalAfterIdle = engine.currentWatermark();
        log.info("global watermark after idle marker={}", globalAfterIdle);

        // slow 恢复发数据：晚于全局水位的事件正常计入
        assertEquals(IngestStatus.ACCEPTED, engine.ingest(ev("b2", 6000, "slow")).status());
        // 已确认过期（<= 全局水位）的事件：可区分结论，不静默丢弃也不静默计入
        var expired = engine.ingest(ev("b3", 2500, "slow"));
        log.info("expired event after resume: status={} reason={}", expired.status(), expired.reason());
        assertEquals(IngestStatus.LATE_REJECTED, expired.status());

        // 推进全局水位，结算所有窗口（a4 留在未结算窗口中，不计入已结算总数）
        engine.ingestMarker(new SourceMarker("m2", "slow", 30_000));
        engine.ingest(ev("a3", 20_000, "fast"));
        engine.ingest(ev("a4", 30_000, "fast"));
        log.info("final sourceWatermarks={} global={}", engine.sourceWatermarks(), engine.currentWatermark());
        List<WindowResult> results = engine.finalizedResults();
        results.forEach(r -> log.info("finalized window: {}", r));

        WindowResult w6000 = results.stream().filter(r -> r.windowStart() == 6000).findFirst().orElseThrow();
        assertEquals(1, w6000.totalCount(), "only the in-time resumed event b2 counts; expired b3 must not");
        long total = results.stream().mapToLong(WindowResult::totalCount).sum();
        assertEquals(5, total, "a1,b1,a2,b2,a3 counted exactly once; b3 rejected; a4 not yet finalized");
    }

    /** 每个来源各自的消息序列（事件按时间递增，标记在对应阶段事件之后），保证来源不自相矛盾。 */
    private static List<Object> sourceScript(String source, String idPrefix) {
        List<Object> ops = new ArrayList<>();
        for (int i = 0; i < 100; i++) {
            ops.add(ev(idPrefix + "-p1-" + i, 100 + i * 8L, source));      // 窗口 [0,1000)
        }
        ops.add(new SourceMarker(idPrefix + "-mk1", source, 2000));         // 空闲到 2000
        for (int i = 0; i < 100; i++) {
            ops.add(ev(idPrefix + "-p2-" + i, 2100 + i * 8L, source));      // 窗口 [2000,3000)
        }
        ops.add(new SourceMarker(idPrefix + "-mk2", source, 100_000));      // 收尾
        return ops;
    }

    private static Object apply(WindowEngine engine, Object op) {
        if (op instanceof Event e) {
            var r = engine.ingest(e);
            assertEquals(IngestStatus.ACCEPTED, r.status(),
                    "event " + e.eventId() + " rejected: " + r.reason());
            return r;
        }
        SourceMarker m = (SourceMarker) op;
        var r = engine.ingestMarker(m);
        assertEquals(MarkerStatus.ACCEPTED, r.status(),
                "marker " + m.markerId() + " rejected: " + r.reason());
        return r;
    }

    /** 归并多个来源脚本：保持每个来源内部顺序，来源间交错由 seed 决定。 */
    private static List<Object> interleave(List<List<Object>> scripts, long seed) {
        Random rnd = new Random(seed);
        List<List<Object>> remaining = new ArrayList<>();
        for (List<Object> s : scripts) {
            remaining.add(new ArrayList<>(s));
        }
        List<Object> merged = new ArrayList<>();
        while (true) {
            remaining.removeIf(List::isEmpty);
            if (remaining.isEmpty()) {
                break;
            }
            List<Object> picked = remaining.get(rnd.nextInt(remaining.size()));
            merged.add(picked.remove(0));
        }
        return merged;
    }

    @Test
    void interleavingsAndConcurrencyYieldIdenticalResults() throws Exception {
        List<List<Object>> scripts = List.of(
                sourceScript("s1", "s1"), sourceScript("s2", "s2"), sourceScript("s3", "s3"));

        // 顺序对照组
        WindowEngine reference = newEngine();
        interleave(scripts, 0L).forEach(op -> apply(reference, op));
        List<WindowResult> expected = reference.finalizedResults();
        expected.forEach(r -> log.info("reference finalized window: {}", r));
        assertEquals(2, expected.size());
        assertEquals(300, expected.get(0).totalCount());
        assertEquals(300, expected.get(1).totalCount());

        // 不同交错顺序
        for (long seed : new long[]{1L, 7L, 99L}) {
            WindowEngine engine = newEngine();
            interleave(scripts, seed).forEach(op -> apply(engine, op));
            List<WindowResult> results = engine.finalizedResults();
            log.info("interleave seed={} global={} sources={}",
                    seed, engine.currentWatermark(), engine.sourceWatermarks());
            assertEquals(expected, results, "results differ for interleave seed " + seed);
        }

        // 并发提交：每个来源一个线程，来源间任意交错
        WindowEngine concurrent = newEngine();
        ConcurrentLinkedQueue<String> anomalies = new ConcurrentLinkedQueue<>();
        ExecutorService pool = Executors.newFixedThreadPool(scripts.size());
        CountDownLatch ready = new CountDownLatch(scripts.size());
        CountDownLatch go = new CountDownLatch(1);
        List<Future<?>> futures = new ArrayList<>();
        for (List<Object> script : scripts) {
            futures.add(pool.submit(() -> {
                ready.countDown();
                go.await(5, TimeUnit.SECONDS);
                for (Object op : script) {
                    try {
                        apply(concurrent, op);
                    } catch (AssertionError e) {
                        anomalies.add(e.getMessage());
                    }
                }
                return null;
            }));
        }
        ready.await(5, TimeUnit.SECONDS);
        go.countDown();
        for (Future<?> f : futures) {
            f.get(30, TimeUnit.SECONDS);
        }
        pool.shutdown();
        assertTrue(anomalies.isEmpty(), "unexpected rejections under concurrency: " + anomalies);
        List<WindowResult> concurrentResults = concurrent.finalizedResults();
        concurrentResults.forEach(r -> log.info("concurrent finalized window: {}", r));
        assertEquals(expected, concurrentResults,
                "concurrent multi-source ingestion must produce identical window results");
    }

    @Test
    void duplicateAndStaleMarkersAreIdempotentAndDistinguishable() {
        WindowEngine engine = newEngine();
        engine.ingest(ev("e1", 5000, "s1")); // s1 水位 3000，全局 3000

        assertEquals(MarkerStatus.ACCEPTED,
                engine.ingestMarker(new SourceMarker("m1", "s1", 8000)).status());
        assertEquals(8000, engine.currentWatermark());
        var stateAfterAccept = engine.sourceWatermarks();
        var resultsAfterAccept = engine.finalizedResults();

        // 重复标记：同一 markerId -> DUPLICATE，不重复生效
        MarkerResult dup = engine.ingestMarker(new SourceMarker("m1", "s1", 8000));
        log.info("duplicate marker: status={} reason={} (dedup basis: markerId)", dup.status(), dup.reason());
        assertEquals(MarkerStatus.DUPLICATE, dup.status());

        // 过期标记：进度不超过该来源已知进度 -> STALE，不生效
        MarkerResult stale = engine.ingestMarker(new SourceMarker("m2", "s1", 7000));
        log.info("stale marker: status={} reason={}", stale.status(), stale.reason());
        assertEquals(MarkerStatus.STALE, stale.status());

        // 新来源但进度已被全局水位蕴含 -> STALE
        MarkerResult staleNew = engine.ingestMarker(new SourceMarker("m3", "s2", 5000));
        log.info("stale marker for new source: status={} reason={}", staleNew.status(), staleNew.reason());
        assertEquals(MarkerStatus.STALE, staleNew.status());

        // 新来源且进度超出全局水位 -> ACCEPTED；相同进度再次提交（不同 markerId）-> STALE
        assertEquals(MarkerStatus.ACCEPTED,
                engine.ingestMarker(new SourceMarker("m3", "s2", 9000)).status());
        MarkerResult equal = engine.ingestMarker(new SourceMarker("m4", "s2", 9000));
        log.info("equal-progress marker: status={} reason={}", equal.status(), equal.reason());
        assertEquals(MarkerStatus.STALE, equal.status());

        // 任何重复/过期标记都没有改脏已有状态
        assertEquals(8000, engine.currentWatermark(), "global watermark must not move backwards");
        assertEquals(9000, engine.sourceWatermarks().get("s2"));
        assertEquals(stateAfterAccept.get("s1"), engine.sourceWatermarks().get("s1"));
        assertEquals(resultsAfterAccept, engine.finalizedResults(),
                "duplicate/stale markers must not corrupt finalized results");
    }

    @Test
    void sourceCountLimitAppliesDistinguishableBackpressure() {
        WindowEngine engine = new WindowEngine(WINDOW, LATENESS, 1024, 100_000, 10_000,
                2, 100, r -> { });
        assertEquals(IngestStatus.ACCEPTED, engine.ingest(ev("e1", 100, "s1")).status());
        assertEquals(IngestStatus.ACCEPTED, engine.ingest(ev("e2", 200, "s2")).status());

        // 第三个来源的事件与标记都被可区分地背压，不丢数据、不崩溃
        var bpEvent = engine.ingest(ev("e3", 300, "s3"));
        log.info("new-source event under limit: status={} reason={}", bpEvent.status(), bpEvent.reason());
        assertEquals(IngestStatus.BACKPRESSURE, bpEvent.status());
        var bpMarker = engine.ingestMarker(new SourceMarker("m1", "s3", 10_000));
        log.info("new-source marker under limit: status={} reason={}", bpMarker.status(), bpMarker.reason());
        assertEquals(MarkerStatus.BACKPRESSURE, bpMarker.status());

        // 已有来源不受影响
        assertEquals(IngestStatus.ACCEPTED, engine.ingest(ev("e4", 400, "s1")).status());
        assertEquals(MarkerStatus.ACCEPTED,
                engine.ingestMarker(new SourceMarker("m2", "s1", 10_000)).status());
        assertEquals(2, engine.sourceWatermarks().size());
    }

    @Test
    void markerDedupLimitAppliesDistinguishableBackpressure() {
        WindowEngine engine = new WindowEngine(WINDOW, LATENESS, 1024, 100_000, 10_000,
                16, 1, r -> { });
        engine.ingest(ev("e1", 100, "s1"));
        assertEquals(MarkerStatus.ACCEPTED,
                engine.ingestMarker(new SourceMarker("m1", "s1", 5000)).status());

        // 标记去重表已满（m1 进度 5000 等于水位，严格回收保留它）：新标记被可区分地背压
        MarkerResult bp = engine.ingestMarker(new SourceMarker("m2", "s1", 9000));
        log.info("marker under dedup limit: status={} reason={}", bp.status(), bp.reason());
        assertEquals(MarkerStatus.BACKPRESSURE, bp.status());

        // 水位推进回收 m1 的条目后，m2 重试成功：背压不是丢数据
        engine.ingest(ev("e2", 20_000, "s1"));
        MarkerResult retry = engine.ingestMarker(new SourceMarker("m2", "s1", 9000));
        log.info("retry of m2 after prune: status={} reason={}", retry.status(), retry.reason());
        assertEquals(MarkerStatus.STALE, retry.status(),
                "watermark already beyond 9000, retried marker is distinguishably stale");
    }
}
