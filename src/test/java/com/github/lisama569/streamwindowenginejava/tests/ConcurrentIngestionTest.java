package com.github.lisama569.streamwindowenginejava.tests;

import com.github.lisama569.streamwindowenginejava.checkpoint.CheckpointManager;
import com.github.lisama569.streamwindowenginejava.config.EngineProperties;
import com.github.lisama569.streamwindowenginejava.engine.StreamEngine;
import com.github.lisama569.streamwindowenginejava.model.Event;
import com.github.lisama569.streamwindowenginejava.model.IngestResult;
import com.github.lisama569.streamwindowenginejava.model.IngestStatus;
import com.github.lisama569.streamwindowenginejava.model.WindowResult;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * 并发摄取：多线程同时提交事件，结果必须与串行执行一致；
 * 并发重复提交同一 eventId 时恰好计入一次。
 */
class ConcurrentIngestionTest {

    private static final Logger log = LoggerFactory.getLogger(ConcurrentIngestionTest.class);
    private static final long WINDOW = 60_000;

    @TempDir
    Path checkpointDir;

    private StreamEngine newEngine() {
        EngineProperties props = new EngineProperties();
        props.setWindowSizeMs(WINDOW);
        props.setAllowedLatenessMs(30_000);
        props.setCheckpointDir(checkpointDir.toString());
        StreamEngine e = new StreamEngine(props, new CheckpointManager(props));
        e.restore();
        return e;
    }

    @Test
    void concurrentIngestionOfDistinctEventsMatchesSerialResult() throws Exception {
        int total = 2_000;
        int threads = 16;

        // 并发摄取
        StreamEngine concurrent = newEngine();
        runConcurrent(concurrent, threads, total, i -> new Event("c" + i, "k1", (i % 60) * 1_000L, 1));
        concurrent.advanceWatermark(WINDOW);
        List<WindowResult> concurrentResults = concurrent.results("k1");

        // 串行摄取（独立引擎、独立检查点目录）
        EngineProperties serialProps = new EngineProperties();
        serialProps.setWindowSizeMs(WINDOW);
        serialProps.setAllowedLatenessMs(30_000);
        serialProps.setCheckpointDir(checkpointDir.resolve("serial").toString());
        StreamEngine serial = new StreamEngine(serialProps, new CheckpointManager(serialProps));
        serial.restore();
        IntStream.range(0, total).forEach(i -> serial.ingest(new Event("c" + i, "k1", (i % 60) * 1_000L, 1)));
        serial.advanceWatermark(WINDOW);
        List<WindowResult> serialResults = serial.results("k1");

        log.info("concurrency check: watermark={} concurrentResults={} serialResults={}",
                WINDOW, concurrentResults, serialResults);
        assertThat(concurrentResults).isEqualTo(serialResults);
        assertThat(concurrentResults.get(0).count()).isEqualTo(total);
        assertThat(concurrentResults.get(0).sum()).isEqualTo(total);
    }

    @Test
    void concurrentDuplicateSubmissionsAreCountedExactlyOnce() throws Exception {
        int distinct = 200;
        int duplicatesPerEvent = 8;
        StreamEngine engine = newEngine();

        // 每个 eventId 被 8 个线程同时提交
        List<IngestResult> results = new CopyOnWriteArrayList<>();
        try (ExecutorService pool = Executors.newFixedThreadPool(16)) {
            List<Future<?>> futures = new ArrayList<>();
            for (int i = 0; i < distinct; i++) {
                Event event = new Event("d" + i, "k1", (i % 60) * 1_000L, 5);
                for (int d = 0; d < duplicatesPerEvent; d++) {
                    futures.add(pool.submit(() -> results.add(engine.ingest(event))));
                }
            }
            for (Future<?> f : futures) {
                f.get(30, TimeUnit.SECONDS);
            }
        }

        long accepted = results.stream().filter(r -> r.status() == IngestStatus.ACCEPTED).count();
        long duplicates = results.stream().filter(r -> r.status() == IngestStatus.DUPLICATE).count();
        log.info("concurrent duplicates: accepted={} duplicates={} total={} dedupBasis=eventId",
                accepted, duplicates, results.size());
        assertThat(accepted).isEqualTo(distinct);
        assertThat(duplicates).isEqualTo((long) distinct * (duplicatesPerEvent - 1));

        engine.advanceWatermark(WINDOW);
        WindowResult r = engine.results("k1").get(0);
        log.info("window result after concurrent duplicates: {}", r);
        assertThat(r.count()).isEqualTo(distinct);
        assertThat(r.sum()).isEqualTo(distinct * 5L);
    }

    private void runConcurrent(StreamEngine engine, int threads, int total,
                               java.util.function.IntFunction<Event> factory) throws Exception {
        try (ExecutorService pool = Executors.newFixedThreadPool(threads)) {
            List<Future<IngestResult>> futures = new ArrayList<>();
            for (int i = 0; i < total; i++) {
                int idx = i;
                futures.add(pool.submit(() -> engine.ingest(factory.apply(idx))));
            }
            for (Future<IngestResult> f : futures) {
                assertThat(f.get(30, TimeUnit.SECONDS).status()).isEqualTo(IngestStatus.ACCEPTED);
            }
        }
    }
}
