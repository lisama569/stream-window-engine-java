package com.github.lisama569.streamwindowenginejava.tests;

import com.github.lisama569.streamwindowenginejava.checkpoint.CheckpointManager;
import com.github.lisama569.streamwindowenginejava.config.EngineProperties;
import com.github.lisama569.streamwindowenginejava.engine.StreamEngine;
import com.github.lisama569.streamwindowenginejava.model.Event;
import com.github.lisama569.streamwindowenginejava.model.IngestResult;
import com.github.lisama569.streamwindowenginejava.model.IngestStatus;
import com.github.lisama569.streamwindowenginejava.model.WindowResult;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * 乱序与迟到事件：同一事件集合在任意到达顺序下窗口结果一致；
 * 迟到事件按允许迟到范围给出可区分结论。
 */
class OutOfOrderAndLateEventTest {

    private static final Logger log = LoggerFactory.getLogger(OutOfOrderAndLateEventTest.class);
    private static final long WINDOW = 60_000;
    private static final long LATENESS = 30_000;

    @TempDir
    Path checkpointDir;

    private StreamEngine engine;

    @BeforeEach
    void setUp() {
        engine = newEngine();
    }

    private StreamEngine newEngine() {
        EngineProperties props = new EngineProperties();
        props.setWindowSizeMs(WINDOW);
        props.setAllowedLatenessMs(LATENESS);
        props.setCheckpointDir(checkpointDir.toString());
        StreamEngine e = new StreamEngine(props, new CheckpointManager(props));
        e.restore();
        return e;
    }

    private List<Event> sampleEvents() {
        // 全部属于窗口 [0, 60s)，key=k1
        List<Event> events = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            events.add(new Event("e" + i, "k1", i * 5_000L, i + 1));
        }
        return events;
    }

    @Test
    void sameEventSetInAnyArrivalOrderYieldsIdenticalWindowResults() {
        List<Event> events = sampleEvents();

        // 顺序到达
        StreamEngine engineA = newEngine();
        events.forEach(engineA::ingest);
        engineA.advanceWatermark(WINDOW);
        List<WindowResult> resultsA = engineA.results("k1");

        // 逆序到达（全新引擎、全新检查点目录）
        EngineProperties propsB = new EngineProperties();
        propsB.setWindowSizeMs(WINDOW);
        propsB.setAllowedLatenessMs(LATENESS);
        propsB.setCheckpointDir(checkpointDir.resolve("b").toString());
        StreamEngine engineB = new StreamEngine(propsB, new CheckpointManager(propsB));
        engineB.restore();
        List<Event> reversed = new ArrayList<>(events);
        Collections.reverse(reversed);
        reversed.forEach(engineB::ingest);
        engineB.advanceWatermark(WINDOW);
        List<WindowResult> resultsB = engineB.results("k1");

        // 随机乱序到达
        EngineProperties propsC = new EngineProperties();
        propsC.setWindowSizeMs(WINDOW);
        propsC.setAllowedLatenessMs(LATENESS);
        propsC.setCheckpointDir(checkpointDir.resolve("c").toString());
        StreamEngine engineC = new StreamEngine(propsC, new CheckpointManager(propsC));
        engineC.restore();
        List<Event> shuffled = new ArrayList<>(events);
        Collections.shuffle(shuffled, new java.util.Random(42));
        shuffled.forEach(engineC::ingest);
        engineC.advanceWatermark(WINDOW);
        List<WindowResult> resultsC = engineC.results("k1");

        log.info("order-independence check: watermark={} resultsA={} resultsB={} resultsC={}",
                WINDOW, resultsA, resultsB, resultsC);
        assertThat(resultsA).hasSize(1);
        assertThat(resultsB).isEqualTo(resultsA);
        assertThat(resultsC).isEqualTo(resultsA);
        WindowResult r = resultsA.get(0);
        assertThat(r.count()).isEqualTo(10);
        assertThat(r.sum()).isEqualTo(55);
        assertThat(r.eventIds()).containsExactlyInAnyOrderElementsOf(
                events.stream().map(Event::eventId).toList());
    }

    @Test
    void lateEventWithinAllowedLatenessCorrectsFinalizedResult() {
        sampleEvents().forEach(engine::ingest);
        engine.advanceWatermark(WINDOW); // 窗口 [0,60s) 定版，version=1
        WindowResult v1 = engine.results("k1").get(0);
        assertThat(v1.version()).isEqualTo(1);
        assertThat(v1.count()).isEqualTo(10);

        // 水位=60s，窗口终点+允许迟到=90s > 60s：迟到但在允许范围内
        IngestResult late = engine.ingest(new Event("late-1", "k1", 10_000, 100));
        log.info("late ingest: status={} watermark={} reason={}", late.status(), engine.currentWatermark(), late.reason());
        assertThat(late.status()).isEqualTo(IngestStatus.ACCEPTED_LATE);

        WindowResult v2 = engine.results("k1").get(0);
        log.info("corrected window result: {}", v2);
        assertThat(v2.version()).isEqualTo(2);
        assertThat(v2.count()).isEqualTo(11);
        assertThat(v2.sum()).isEqualTo(155);
        assertThat(v2.eventIds()).contains("late-1");
    }

    @Test
    void eventBeyondAllowedLatenessIsRejectedAndVisible() {
        sampleEvents().forEach(engine::ingest);
        engine.advanceWatermark(WINDOW + LATENESS); // 水位=90s，窗口 [0,60s) 状态被回收
        assertThat(engine.stats().get("activeWindows")).isEqualTo(0);

        WindowResult before = engine.results("k1").get(0);
        IngestResult tooLate = engine.ingest(new Event("too-late-1", "k1", 10_000, 100));
        log.info("too-late ingest: status={} watermark={} reason={}",
                tooLate.status(), engine.currentWatermark(), tooLate.reason());
        assertThat(tooLate.status()).isEqualTo(IngestStatus.REJECTED_TOO_LATE);

        // 不被静默计入：窗口结果保持不变；也不被静默丢弃：死信日志可见
        assertThat(engine.results("k1").get(0)).isEqualTo(before);
        assertThat(engine.deadLetters()).hasSize(1);
        assertThat(engine.deadLetters().get(0).event().eventId()).isEqualTo("too-late-1");
        log.info("dead letters: {}", engine.deadLetters());
    }

    @Test
    void duplicateEventIdIsNotCountedTwice() {
        Event event = new Event("dup-1", "k1", 5_000, 7);
        IngestResult first = engine.ingest(event);
        IngestResult second = engine.ingest(event);
        log.info("dedup check: first={} second={} dedupBasis=eventId '{}'", first.status(), second.status(), event.eventId());
        assertThat(first.status()).isEqualTo(IngestStatus.ACCEPTED);
        assertThat(second.status()).isEqualTo(IngestStatus.DUPLICATE);

        engine.advanceWatermark(WINDOW);
        WindowResult r = engine.results("k1").get(0);
        log.info("window result after dedup: {}", r);
        assertThat(r.count()).isEqualTo(1);
        assertThat(r.sum()).isEqualTo(7);
    }
}
