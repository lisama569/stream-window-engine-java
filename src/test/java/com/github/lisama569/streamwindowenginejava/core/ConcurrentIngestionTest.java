package com.github.lisama569.streamwindowenginejava.core;

import com.github.lisama569.streamwindowenginejava.model.Event;
import com.github.lisama569.streamwindowenginejava.model.IngestResult;
import com.github.lisama569.streamwindowenginejava.model.IngestStatus;
import com.github.lisama569.streamwindowenginejava.model.WindowResult;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 并发摄取一致性：多线程同时摄取，最终窗口结果必须与单线程顺序执行完全一致。
 */
class ConcurrentIngestionTest {

    private static final Logger log = LoggerFactory.getLogger(ConcurrentIngestionTest.class);

    private static final long WINDOW = 1000;
    private static final long LATENESS = 2000;
    private static final int THREADS = 8;
    private static final int EVENTS_PER_THREAD = 500;

    private WindowEngine newEngine() {
        return new WindowEngine(WINDOW, LATENESS, 1024, 1_000_000, 10_000,
                r -> log.debug("listener observed window result: {}", r));
    }

    /** 每个线程的事件时间都在 [0, LATENESS) 内，满足有界乱序前提。 */
    private Event eventFor(int thread, int seq) {
        long t = (thread * 31L + seq * 7L) % LATENESS;
        return new Event("t" + thread + "-e" + seq, "key-" + (seq % 4), t, seq);
    }

    @Test
    void concurrentIngestionMatchesSequentialReference() throws Exception {
        WindowEngine concurrent = newEngine();
        ConcurrentLinkedQueue<IngestResult> anomalies = new ConcurrentLinkedQueue<>();
        CountDownLatch ready = new CountDownLatch(THREADS);
        CountDownLatch go = new CountDownLatch(1);

        ExecutorService pool = Executors.newFixedThreadPool(THREADS);
        List<Future<?>> futures = new ArrayList<>();
        for (int t = 0; t < THREADS; t++) {
            final int thread = t;
            futures.add(pool.submit(() -> {
                ready.countDown();
                go.await(5, TimeUnit.SECONDS);
                for (int i = 0; i < EVENTS_PER_THREAD; i++) {
                    IngestResult r = concurrent.ingest(eventFor(thread, i));
                    if (r.status() != IngestStatus.ACCEPTED) {
                        anomalies.add(r);
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

        assertTrue(anomalies.isEmpty(), "unexpected ingest results under concurrency: " + anomalies);
        concurrent.ingest(new Event("pusher", "k", 10_000, 0));
        List<WindowResult> concurrentResults = concurrent.finalizedResults();
        log.info("concurrent run: watermark={} windows={}", concurrent.currentWatermark(), concurrentResults.size());
        concurrentResults.forEach(r -> log.info("concurrent finalized window: {}", r));

        // 单线程顺序对照组，同一事件集合
        WindowEngine sequential = newEngine();
        for (int t = 0; t < THREADS; t++) {
            for (int i = 0; i < EVENTS_PER_THREAD; i++) {
                assertEquals(IngestStatus.ACCEPTED, sequential.ingest(eventFor(t, i)).status());
            }
        }
        sequential.ingest(new Event("pusher", "k", 10_000, 0));
        List<WindowResult> sequentialResults = sequential.finalizedResults();
        sequentialResults.forEach(r -> log.info("sequential finalized window: {}", r));

        assertEquals(sequentialResults, concurrentResults,
                "concurrent ingestion must produce identical window results");

        long totalCount = concurrentResults.stream().mapToLong(WindowResult::totalCount).sum();
        assertEquals((long) THREADS * EVENTS_PER_THREAD, totalCount,
                "every concurrently ingested event must be counted exactly once");
        log.info("total counted events={} (expected {})", totalCount, THREADS * EVENTS_PER_THREAD);
    }

    @Test
    void concurrentDuplicateSubmissionCountedOnce() throws Exception {
        // 两个线程同时提交同一 eventId：恰好一个 ACCEPTED，一个 DUPLICATE
        WindowEngine engine = newEngine();
        Event shared = new Event("shared-1", "k", 100, 7);
        ExecutorService pool = Executors.newFixedThreadPool(2);
        CountDownLatch go = new CountDownLatch(1);
        List<Future<IngestStatus>> futures = new ArrayList<>();
        for (int i = 0; i < 2; i++) {
            futures.add(pool.submit(() -> {
                go.await(5, TimeUnit.SECONDS);
                return engine.ingest(shared).status();
            }));
        }
        go.countDown();
        List<IngestStatus> statuses = new ArrayList<>();
        for (Future<IngestStatus> f : futures) {
            statuses.add(f.get(10, TimeUnit.SECONDS));
        }
        pool.shutdown();
        statuses.sort(Enum::compareTo);
        log.info("concurrent duplicate submission statuses={} (dedup basis: eventId={})", statuses, shared.eventId());
        assertEquals(List.of(IngestStatus.ACCEPTED, IngestStatus.DUPLICATE), statuses);

        engine.ingest(new Event("pusher", "k", 10_000, 0));
        assertEquals(1, engine.finalizedResults().get(0).totalCount(),
                "shared eventId must be counted exactly once");
    }
}
