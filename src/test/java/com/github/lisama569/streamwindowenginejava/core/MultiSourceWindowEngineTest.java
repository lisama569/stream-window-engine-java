package com.github.lisama569.streamwindowenginejava.core;

import com.github.lisama569.streamwindowenginejava.model.Event;
import com.github.lisama569.streamwindowenginejava.model.IngestResult;
import com.github.lisama569.streamwindowenginejava.model.IngestStatus;
import com.github.lisama569.streamwindowenginejava.model.MarkerResult;
import com.github.lisama569.streamwindowenginejava.model.MarkerStatus;
import com.github.lisama569.streamwindowenginejava.model.SourceMarker;
import com.github.lisama569.streamwindowenginejava.model.WindowResult;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
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
 * 多来源与空闲/进度标记语义测试。
 * 日志打印来源、当前进度、水位、窗口结果与去重依据，保证结论可复现、失败原因可区分。
 */
class MultiSourceWindowEngineTest {

    private static final Logger log = LoggerFactory.getLogger(MultiSourceWindowEngineTest.class);

    private static final long WINDOW = 1000;
    private static final long LATENESS = 2000;

    private WindowEngine newEngine() {
        return new WindowEngine(WINDOW, LATENESS, 1024, 100_000, 10_000, 100,
                r -> log.info("listener observed window result: {}", r));
    }

    private static List<WindowResult> sorted(List<WindowResult> results) {
        return results.stream()
                .sorted(Comparator.comparingLong(WindowResult::windowStart))
                .toList();
    }

    @Test
    void idleSourceDoesNotBlockFinalizationAndResumesCorrectly() {
        WindowEngine engine = newEngine();
        // 两个来源各自有数据落在窗口 [2000,3000)
        assertEquals(IngestStatus.ACCEPTED,
                engine.ingest(new Event("a1", "k", 2100, 1, "A")).status());
        assertEquals(IngestStatus.ACCEPTED,
                engine.ingest(new Event("a2", "k", 2500, 2, "A")).status());
        assertEquals(IngestStatus.ACCEPTED,
                engine.ingest(new Event("b1", "k", 2200, 4, "B")).status());

        // A 继续前进到 10000，但 B 停更：水位被 B 拖住（min(10000,2200)-2000=200，
        // 单调不减故停在 A 独自推进到的 500），窗口不能定版
        assertEquals(IngestStatus.ACCEPTED,
                engine.ingest(new Event("a3", "k", 10_000, 8, "A")).status());
        log.info("B idle: watermark={} sourceProgressA={} sourceProgressB={}",
                engine.currentWatermark(),
                engine.sourceProgress("A"), engine.sourceProgress("B"));
        assertEquals(2500 - LATENESS, engine.currentWatermark(),
                "watermark is monotonic: B joining later cannot pull it back, but blocks further advance");
        assertTrue(engine.finalizedResults().isEmpty(),
                "window [2000,3000) must not finalize while B is idle");

        // B 发出空闲标记：10000 前数据已到齐 -> 不再拖住窗口结论
        MarkerResult marker = engine.submitMarker(new SourceMarker("mB-1", "B", 10_000));
        log.info("idle marker result: status={} reason={} watermark={}",
                marker.status(), marker.reason(), engine.currentWatermark());
        assertEquals(MarkerStatus.APPLIED, marker.status());
        assertEquals(10_000 - LATENESS, engine.currentWatermark());

        List<WindowResult> results = engine.finalizedResults();
        results.forEach(r -> log.info("finalized after idle marker: {}", r));
        assertEquals(1, results.size());
        assertEquals(2000, results.get(0).windowStart());
        assertEquals(3, results.get(0).totalCount(), "a1, a2, b1 must all be counted");
        assertEquals(7, results.get(0).totalValue());

        // B 恢复发数据：仍在迟到视野内的正常计入
        assertEquals(IngestStatus.ACCEPTED,
                engine.ingest(new Event("b2", "k", 9000, 16, "B")).status());
        // 已被水位越过的：可区分的迟到结论，不悄悄丢掉也不改坏已定版结果
        IngestResult late = engine.ingest(new Event("b3", "k", 7000, 32, "B"));
        log.info("late event after resume: status={} reason={} (dedup basis: eventId=b3)",
                late.status(), late.reason());
        assertEquals(IngestStatus.LATE_REJECTED, late.status());
        assertEquals(1, engine.finalizedResults().size(),
                "finalized window [2000,3000) must remain untouched");
    }

    /** 构造事件与标记集合：所有事件时间 > 任何交错下可达的水位，保证任意交错结论一致。 */
    private List<Object> interleavableSet() {
        List<Object> items = new ArrayList<>();
        for (int i = 0; i < 50; i++) {
            long t = 4001 + (i * 37L) % 1999; // [4001, 6000)
            items.add(new Event("A-e" + i, "key-" + (i % 3), t, i, "A"));
            items.add(new Event("B-e" + i, "key-" + (i % 3), t, i * 2L, "B"));
        }
        items.add(new SourceMarker("mA-1", "A", 6000));
        items.add(new SourceMarker("mB-1", "B", 6000));
        return items;
    }

    private void apply(WindowEngine engine, Object item, List<Object> anomalies) {
        if (item instanceof Event e) {
            IngestResult r = engine.ingest(e);
            if (r.status() != IngestStatus.ACCEPTED) {
                anomalies.add(r);
            }
        } else {
            MarkerResult r = engine.submitMarker((SourceMarker) item);
            if (r.status() != MarkerStatus.APPLIED) {
                anomalies.add(r);
            }
        }
    }

    private List<WindowResult> runAndFinalize(WindowEngine engine, List<Object> items) {
        List<Object> anomalies = new ArrayList<>();
        for (Object item : items) {
            apply(engine, item, anomalies);
        }
        assertTrue(anomalies.isEmpty(), "unexpected rejections: " + anomalies);
        // 收尾：两个来源都推进到 20000，水位 18000，所有窗口定版
        assertEquals(MarkerStatus.APPLIED,
                engine.submitMarker(new SourceMarker("mA-2", "A", 20_000)).status());
        assertEquals(MarkerStatus.APPLIED,
                engine.submitMarker(new SourceMarker("mB-2", "B", 20_000)).status());
        log.info("run finished: watermark={} windows={}", engine.currentWatermark(),
                engine.finalizedResults().size());
        return sorted(engine.finalizedResults());
    }

    @Test
    void arbitraryInterleavingYieldsIdenticalResults() {
        List<Object> items = interleavableSet();
        List<WindowResult> reference = runAndFinalize(newEngine(), items);
        reference.forEach(r -> log.info("reference finalized window: {}", r));

        for (long seed : new long[]{1L, 7L, 99L}) {
            List<Object> shuffled = new ArrayList<>(items);
            Collections.shuffle(shuffled, new Random(seed));
            List<WindowResult> results = runAndFinalize(newEngine(), shuffled);
            log.info("shuffle seed={} watermark consistent, windows={}", seed, results.size());
            assertEquals(reference, results, "results differ for shuffle seed " + seed);
        }
        assertEquals(2, reference.size(), "expected windows [4000,5000) and [5000,6000)");
    }

    @Test
    void concurrentEventsAndMarkersMatchSequentialReference() throws Exception {
        List<Object> items = interleavableSet();
        List<WindowResult> reference = runAndFinalize(newEngine(), items);

        WindowEngine concurrent = newEngine();
        ConcurrentLinkedQueue<Object> anomalies = new ConcurrentLinkedQueue<>();
        int threads = 8;
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        CountDownLatch ready = new CountDownLatch(threads);
        CountDownLatch go = new CountDownLatch(1);
        List<Future<?>> futures = new ArrayList<>();
        for (int t = 0; t < threads; t++) {
            final int thread = t;
            futures.add(pool.submit(() -> {
                ready.countDown();
                go.await(5, TimeUnit.SECONDS);
                for (int i = thread; i < items.size(); i += threads) {
                    apply(concurrent, items.get(i), new ArrayList<>() {
                        @Override
                        public boolean add(Object o) {
                            return anomalies.add(o);
                        }
                    });
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
        assertEquals(MarkerStatus.APPLIED,
                concurrent.submitMarker(new SourceMarker("mA-2", "A", 20_000)).status());
        assertEquals(MarkerStatus.APPLIED,
                concurrent.submitMarker(new SourceMarker("mB-2", "B", 20_000)).status());
        List<WindowResult> results = sorted(concurrent.finalizedResults());
        results.forEach(r -> log.info("concurrent finalized window: {}", r));
        assertEquals(reference, results,
                "concurrent interleaving of events and markers must produce identical results");
    }

    @Test
    void duplicateAndStaleMarkersAreIdempotentAndDistinguishable() {
        WindowEngine engine = newEngine();
        assertEquals(IngestStatus.ACCEPTED,
                engine.ingest(new Event("b1", "k", 3000, 1, "B")).status());

        MarkerResult applied = engine.submitMarker(new SourceMarker("m1", "B", 5000));
        assertEquals(MarkerStatus.APPLIED, applied.status());
        long watermarkAfterM1 = engine.currentWatermark();
        log.info("after m1: watermark={} sourceProgressB={}",
                watermarkAfterM1, engine.sourceProgress("B"));

        // 重复标记：幂等，不重复推进、不重复触发结论
        MarkerResult dup = engine.submitMarker(new SourceMarker("m1", "B", 5000));
        log.info("duplicate marker: status={} reason={} (dedup basis: markerId=m1)",
                dup.status(), dup.reason());
        assertEquals(MarkerStatus.DUPLICATE, dup.status());

        // 过期/矛盾标记：进度不领先于已知进度，不生效
        MarkerResult staleLower = engine.submitMarker(new SourceMarker("m2", "B", 4000));
        MarkerResult staleEqual = engine.submitMarker(new SourceMarker("m3", "B", 5000));
        log.info("stale markers: m2={} m3={}", staleLower.status(), staleEqual.status());
        assertEquals(MarkerStatus.STALE, staleLower.status());
        assertEquals(MarkerStatus.STALE, staleEqual.status());
        assertEquals(watermarkAfterM1, engine.currentWatermark(),
                "duplicate/stale markers must not move the watermark");

        // 更高进度的新标记正常生效
        assertEquals(MarkerStatus.APPLIED,
                engine.submitMarker(new SourceMarker("m4", "B", 8000)).status());
        assertEquals(8000 - LATENESS, engine.currentWatermark());

        // 非法标记：可区分结论，不改变状态
        MarkerResult invalid = engine.submitMarker(new SourceMarker(" ", "B", 99999));
        assertEquals(MarkerStatus.STALE, invalid.status());
        assertEquals(8000 - LATENESS, engine.currentWatermark());
    }

    @Test
    void concurrentDuplicateMarkerAppliedExactlyOnce() throws Exception {
        WindowEngine engine = newEngine();
        SourceMarker shared = new SourceMarker("shared-m", "B", 5000);
        ExecutorService pool = Executors.newFixedThreadPool(2);
        CountDownLatch go = new CountDownLatch(1);
        List<Future<MarkerStatus>> futures = new ArrayList<>();
        for (int i = 0; i < 2; i++) {
            futures.add(pool.submit(() -> {
                go.await(5, TimeUnit.SECONDS);
                return engine.submitMarker(shared).status();
            }));
        }
        go.countDown();
        List<MarkerStatus> statuses = new ArrayList<>();
        for (Future<MarkerStatus> f : futures) {
            statuses.add(f.get(10, TimeUnit.SECONDS));
        }
        pool.shutdown();
        statuses.sort(Enum::compareTo);
        log.info("concurrent duplicate marker statuses={} (dedup basis: markerId={})",
                statuses, shared.markerId());
        assertEquals(List.of(MarkerStatus.APPLIED, MarkerStatus.DUPLICATE), statuses);
        assertEquals(5000, engine.sourceProgress("B"),
                "shared markerId must advance source progress exactly once");
    }

    @Test
    void eventAndMarkerDedupAreIndependentEvenWithCollidingIds() {
        WindowEngine engine = newEngine();
        // 一路事件的编号与另一路标记的编号完全相同：各自按自身语义处理
        assertEquals(IngestStatus.ACCEPTED,
                engine.ingest(new Event("42", "k", 3000, 1, "A")).status());
        MarkerResult marker = engine.submitMarker(new SourceMarker("42", "B", 5000));
        log.info("marker id collides with eventId: status={} reason={} (dedup basis: markerId=42)",
                marker.status(), marker.reason());
        assertEquals(MarkerStatus.APPLIED, marker.status(),
                "marker id colliding with an eventId must still apply");
        assertEquals(5000, engine.sourceProgress("B"));

        // 反向：标记先登记，同编号事件也不能被误判成重复
        assertEquals(MarkerStatus.APPLIED,
                engine.submitMarker(new SourceMarker("77", "A", 6000)).status());
        IngestResult event = engine.ingest(new Event("77", "k", 4500, 4, "A"));
        log.info("event id collides with markerId: status={} reason={} (dedup basis: eventId=77)",
                event.status(), event.reason());
        assertEquals(IngestStatus.ACCEPTED, event.status(),
                "event id colliding with a markerId must not be dropped as duplicate");

        // 极端碰撞：eventId 恰好等于旧版标记去重键（"M:" 前缀），同样互不影响
        assertEquals(IngestStatus.ACCEPTED,
                engine.ingest(new Event("M:77", "k", 4600, 8, "A")).status());

        // 各自的去重仍然生效：重复事件（水位之上，命中事件去重表）、
        // 重复标记都可区分；已被水位越过的重投则判 LATE_REJECTED，同样不重复计数
        IngestResult dupEvent = engine.ingest(new Event("77", "k", 4500, 4, "A"));
        log.info("resubmit event 77 -> {} ({}) (dedup basis: eventId)",
                dupEvent.status(), dupEvent.reason());
        assertEquals(IngestStatus.DUPLICATE, dupEvent.status());
        assertEquals(IngestStatus.LATE_REJECTED,
                engine.ingest(new Event("42", "k", 3000, 1, "A")).status());
        assertEquals(MarkerStatus.DUPLICATE,
                engine.submitMarker(new SourceMarker("42", "B", 5000)).status());

        // 收尾定版，核对窗口结果只包含被接受的三条事件
        assertEquals(MarkerStatus.APPLIED,
                engine.submitMarker(new SourceMarker("mA-final", "A", 20_000)).status());
        assertEquals(MarkerStatus.APPLIED,
                engine.submitMarker(new SourceMarker("mB-final", "B", 20_000)).status());
        List<WindowResult> results = sorted(engine.finalizedResults());
        results.forEach(r -> log.info("finalized with colliding ids: {}", r));
        assertEquals(2, results.size());
        assertEquals(3000, results.get(0).windowStart());
        assertEquals(1, results.get(0).totalCount(), "only event 42@3000");
        assertEquals(4000, results.get(1).windowStart());
        assertEquals(2, results.get(1).totalCount(), "events 77@4500 and M:77@4600");
        assertEquals(12, results.get(1).totalValue());
    }

    @Test
    void staleMarkerFromUnknownSourceIsRejectedAndDoesNotBlockOthers() {
        WindowEngine engine = newEngine();
        assertEquals(IngestStatus.ACCEPTED,
                engine.ingest(new Event("a1", "k", 2100, 1, "A")).status());
        assertEquals(IngestStatus.ACCEPTED,
                engine.ingest(new Event("b1", "k", 2200, 2, "B")).status());
        assertEquals(MarkerStatus.APPLIED,
                engine.submitMarker(new SourceMarker("mA-1", "A", 10_000)).status());
        assertEquals(MarkerStatus.APPLIED,
                engine.submitMarker(new SourceMarker("mB-1", "B", 10_000)).status());
        long watermark = engine.currentWatermark();
        log.info("before stale marker: watermark={} sources={} finalized={}",
                watermark, engine.sourceCount(), engine.finalizedResults().size());
        assertEquals(10_000 - LATENESS, watermark);
        assertEquals(1, engine.finalizedResults().size(),
                "window [2000,3000) must be finalized before the stale marker arrives");

        // 从未出现过的来源发来明显落后于水位的空闲标记：
        // 不生效、结论可区分（STALE）、不登记来源、不拖住水位
        MarkerResult stale = engine.submitMarker(new SourceMarker("mX-1", "X", 1000));
        log.info("stale marker from unknown source: status={} reason={} watermark={}",
                stale.status(), stale.reason(), engine.currentWatermark());
        assertEquals(MarkerStatus.STALE, stale.status());
        assertEquals(watermark, engine.currentWatermark(),
                "stale marker must not move the watermark");
        assertEquals(2, engine.sourceCount(), "stale marker must not register its source");
        assertEquals(Long.MIN_VALUE, engine.sourceProgress("X"));

        // 边界：恰好等于当前水位同样判 STALE
        assertEquals(MarkerStatus.STALE,
                engine.submitMarker(new SourceMarker("mX-2", "X", watermark)).status());
        assertEquals(2, engine.sourceCount());

        // 其它来源继续推进不受拖累，后续窗口按时定版
        assertEquals(IngestStatus.ACCEPTED,
                engine.ingest(new Event("a2", "k", 11_000, 4, "A")).status());
        assertEquals(IngestStatus.ACCEPTED,
                engine.ingest(new Event("b2", "k", 11_500, 8, "B")).status());
        assertEquals(MarkerStatus.APPLIED,
                engine.submitMarker(new SourceMarker("mA-2", "A", 20_000)).status());
        assertEquals(MarkerStatus.APPLIED,
                engine.submitMarker(new SourceMarker("mB-2", "B", 20_000)).status());
        log.info("after others advance: watermark={} sources={}",
                engine.currentWatermark(), engine.sourceCount());
        assertEquals(20_000 - LATENESS, engine.currentWatermark());

        List<WindowResult> results = sorted(engine.finalizedResults());
        results.forEach(r -> log.info("finalized after stale marker rejected: {}", r));
        assertEquals(2, results.size());
        assertEquals(2000, results.get(0).windowStart());
        assertEquals(11_000, results.get(1).windowStart());
        assertEquals(2, results.get(1).totalCount(), "a2 and b2 must both be counted");
    }

    @Test
    void sourceLimitAppliesDistinguishableBackpressure() {
        WindowEngine engine = new WindowEngine(WINDOW, LATENESS, 1024, 100_000, 10_000, 2,
                r -> log.info("listener observed window result: {}", r));
        assertEquals(IngestStatus.ACCEPTED,
                engine.ingest(new Event("e1", "k", 3000, 1, "s1")).status());
        assertEquals(IngestStatus.ACCEPTED,
                engine.ingest(new Event("e2", "k", 3000, 1, "s2")).status());

        // 第三个来源：事件与标记都被可区分地背压，不丢数据、不崩溃
        IngestResult bpEvent = engine.ingest(new Event("e3", "k", 3000, 1, "s3"));
        MarkerResult bpMarker = engine.submitMarker(new SourceMarker("m1", "s3", 3000));
        log.info("source limit: event={} marker={} sourceCount={}",
                bpEvent.status(), bpMarker.status(), engine.sourceCount());
        assertEquals(IngestStatus.BACKPRESSURE, bpEvent.status());
        assertEquals(MarkerStatus.BACKPRESSURE, bpMarker.status());
        assertEquals(2, engine.sourceCount());

        // 已有来源不受影响
        assertEquals(IngestStatus.ACCEPTED,
                engine.ingest(new Event("e4", "k", 4000, 1, "s1")).status());
        assertEquals(MarkerStatus.APPLIED,
                engine.submitMarker(new SourceMarker("m2", "s2", 4000)).status());
    }
}
