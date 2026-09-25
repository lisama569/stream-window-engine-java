package com.github.lisama569.streamwindowenginejava.core;

import com.github.lisama569.streamwindowenginejava.model.Event;
import com.github.lisama569.streamwindowenginejava.model.IngestResult;
import com.github.lisama569.streamwindowenginejava.model.IngestStatus;
import com.github.lisama569.streamwindowenginejava.model.WindowResult;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 乱序 / 迟到 / 重复 / 背压语义测试。
 * 日志打印水位、窗口结果与去重依据，保证结论可复现、失败原因可区分。
 */
class WindowEngineTest {

    private static final Logger log = LoggerFactory.getLogger(WindowEngineTest.class);

    private static final long WINDOW = 1000;
    private static final long LATENESS = 2000;

    private WindowEngine newEngine() {
        return new WindowEngine(WINDOW, LATENESS, 1024, 100_000, 10_000,
                r -> log.info("listener observed window result: {}", r));
    }

    /** 生成一批两两事件时间差不超过允许迟到范围的事件（满足有界乱序前提）。 */
    private List<Event> sampleEvents(int count) {
        List<Event> events = new ArrayList<>();
        Random rnd = new Random(42);
        for (int i = 0; i < count; i++) {
            long t = rnd.nextInt((int) LATENESS); // [0, 2000)，任意子集内部乱序都不会触发迟到
            events.add(new Event("e-" + i, "key-" + (i % 3), t, i));
        }
        return events;
    }

    private List<WindowResult> runToCompletion(WindowEngine engine, List<Event> events) {
        for (Event e : events) {
            IngestResult r = engine.ingest(e);
            assertEquals(IngestStatus.ACCEPTED, r.status(), "event " + e.eventId() + " rejected: " + r.reason());
        }
        // 推进水位越过所有窗口
        IngestResult push = engine.ingest(new Event("pusher", "k", 10_000, 0));
        assertEquals(IngestStatus.ACCEPTED, push.status());
        log.info("final watermark={}", engine.currentWatermark());
        List<WindowResult> results = engine.finalizedResults();
        results.forEach(r -> log.info("finalized window: {}", r));
        return results;
    }

    @Test
    void outOfOrderArrivalYieldsSameWindowResults() {
        List<Event> events = sampleEvents(200);

        WindowEngine ordered = newEngine();
        List<WindowResult> orderedResults = runToCompletion(ordered, events);

        for (long seed : new long[]{1L, 7L, 99L}) {
            List<Event> shuffled = new ArrayList<>(events);
            Collections.shuffle(shuffled, new Random(seed));
            WindowEngine engine = newEngine();
            List<WindowResult> results = runToCompletion(engine, shuffled);
            log.info("shuffle seed={} produced {} windows, watermark={}",
                    seed, results.size(), engine.currentWatermark());
            assertEquals(orderedResults, results, "results differ for shuffle seed " + seed);
        }
        assertEquals(2, orderedResults.size(), "expected windows [0,1000) and [1000,2000)");
    }

    @Test
    void lateEventIsRejectedWithDistinguishableResult() {
        WindowEngine engine = newEngine();
        assertEquals(IngestStatus.ACCEPTED,
                engine.ingest(new Event("a", "k", 5_000, 1)).status());
        long watermark = engine.currentWatermark();
        log.info("watermark after first event={}", watermark);
        assertEquals(5_000 - LATENESS, watermark);

        // 超出允许迟到范围：eventTime <= watermark
        IngestResult late = engine.ingest(new Event("late-1", "k", watermark, 1));
        log.info("late event result: status={} reason={}", late.status(), late.reason());
        assertEquals(IngestStatus.LATE_REJECTED, late.status());
        assertTrue(late.reason().contains("watermark"));

        // 允许范围内的乱序：watermark < eventTime < maxEventTime
        IngestResult withinLateness = engine.ingest(new Event("ok-1", "k", watermark + 1, 1));
        assertEquals(IngestStatus.ACCEPTED, withinLateness.status());
    }

    @Test
    void duplicateEventIdIsNotCountedTwice() {
        WindowEngine engine = newEngine();
        Event e = new Event("dup-1", "k", 100, 5);
        assertEquals(IngestStatus.ACCEPTED, engine.ingest(e).status());
        IngestResult dup = engine.ingest(e);
        log.info("duplicate resubmission result: status={} reason={} (dedup basis: eventId={})",
                dup.status(), dup.reason(), e.eventId());
        assertEquals(IngestStatus.DUPLICATE, dup.status());

        engine.ingest(new Event("pusher", "k", 10_000, 0));
        List<WindowResult> results = engine.finalizedResults();
        assertEquals(1, results.size());
        assertEquals(1, results.get(0).totalCount(), "duplicate must not be counted twice");
        assertEquals(5, results.get(0).totalValue());
    }

    @Test
    void backpressureWhenOpenWindowsExceedLimit() {
        // window=1000, lateness=2500, maxOpenWindows=2：水位视野内最多 2 个窗口
        WindowEngine engine = new WindowEngine(WINDOW, 2500, 2, 100_000, 10_000, r -> { });
        assertEquals(IngestStatus.ACCEPTED, engine.ingest(new Event("e1", "k", 0, 1)).status());
        assertEquals(IngestStatus.ACCEPTED, engine.ingest(new Event("e2", "k", 2000, 1)).status());

        // 打开第 3 个窗口 -> 可区分的背压，不丢数据、不崩溃
        IngestResult bp1 = engine.ingest(new Event("e3", "k", 3000, 1));
        log.info("backpressure result: status={} reason={}", bp1.status(), bp1.reason());
        assertEquals(IngestStatus.BACKPRESSURE, bp1.status());

        // 水位推进到 1500，窗口 [0,1000) 回收，e4 落入新窗口
        assertEquals(IngestStatus.ACCEPTED, engine.ingest(new Event("e4", "k", 4000, 1)).status());
        // 打开窗口仍满（[2000,3000) 与 [4000,5000)），e5 被背压
        IngestResult bp2 = engine.ingest(new Event("e5", "k", 5000, 1));
        assertEquals(IngestStatus.BACKPRESSURE, bp2.status());
        log.info("backpressure result: status={} reason={}", bp2.status(), bp2.reason());

        // e6 把水位推进到 3000，回收窗口 [2000,3000)，并打开窗口 [5000,6000)
        assertEquals(IngestStatus.ACCEPTED, engine.ingest(new Event("e6", "k", 5500, 1)).status());
        log.info("watermark={} openWindows={}", engine.currentWatermark(), engine.openWindowCount());

        // 背压不是丢数据：e5 仍在迟到视野内且其窗口已打开，重试成功
        IngestResult retry = engine.ingest(new Event("e5", "k", 5000, 1));
        log.info("retry of e5: status={} reason={}", retry.status(), retry.reason());
        assertEquals(IngestStatus.ACCEPTED, retry.status());

        // 而 e3 的事件时间已被水位越过，重试得到可区分的迟到结论
        IngestResult lateRetry = engine.ingest(new Event("e3", "k", 3000, 1));
        log.info("retry of e3: status={} reason={}", lateRetry.status(), lateRetry.reason());
        assertEquals(IngestStatus.LATE_REJECTED, lateRetry.status());

        // 收尾：推进水位，验证最终窗口内容完整（背压未丢数据）
        assertEquals(IngestStatus.ACCEPTED, engine.ingest(new Event("pusher", "k", 100_000, 0)).status());
        List<WindowResult> results = engine.finalizedResults();
        results.forEach(r -> log.info("finalized window: {}", r));
        assertEquals(4, results.size());
        WindowResult w5000 = results.get(3);
        assertEquals(5000, w5000.windowStart());
        assertEquals(2, w5000.totalCount(), "e5 retry and e6 must both be counted");
    }

    @Test
    void stateIsReclaimedAsWatermarkAdvances() {
        WindowEngine engine = newEngine();
        for (int i = 0; i < 100; i++) {
            engine.ingest(new Event("ev-" + i, "k", i * 1000L, 1));
        }
        log.info("after 100 windows: openWindows={} dedupSize={} watermark={}",
                engine.openWindowCount(), engine.dedupSize(), engine.currentWatermark());
        assertTrue(engine.openWindowCount() <= 3,
                "open windows must be reclaimed, got " + engine.openWindowCount());
        assertTrue(engine.dedupSize() <= 3,
                "dedup entries must be pruned, got " + engine.dedupSize());
        assertEquals(97, engine.finalizedResults().size());
    }
}
