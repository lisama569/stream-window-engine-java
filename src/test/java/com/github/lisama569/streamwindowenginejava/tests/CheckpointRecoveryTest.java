package com.github.lisama569.streamwindowenginejava.tests;

import com.github.lisama569.streamwindowenginejava.checkpoint.*;
import com.github.lisama569.streamwindowenginejava.config.EngineProperties;
import com.github.lisama569.streamwindowenginejava.engine.StreamEngine;
import com.github.lisama569.streamwindowenginejava.model.Event;
import com.github.lisama569.streamwindowenginejava.model.IngestStatus;
import com.github.lisama569.streamwindowenginejava.model.WindowResult;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * 检查点与恢复：重启后从最近一次有效检查点继续；
 * 重复提交不重复计数；缺失/损坏/版本不兼容明确拒绝。
 */
class CheckpointRecoveryTest {

    private static final Logger log = LoggerFactory.getLogger(CheckpointRecoveryTest.class);
    private static final long WINDOW = 60_000;

    @TempDir
    Path checkpointDir;

    private EngineProperties props() {
        EngineProperties props = new EngineProperties();
        props.setWindowSizeMs(WINDOW);
        props.setAllowedLatenessMs(30_000);
        props.setCheckpointDir(checkpointDir.toString());
        return props;
    }

    private StreamEngine newEngine(EngineProperties props) {
        StreamEngine e = new StreamEngine(props, new CheckpointManager(props));
        e.restore();
        return e;
    }

    @Test
    void restartContinuesFromCheckpointAndDuplicateResubmissionIsNotCounted() {
        EngineProperties props = props();

        // 第一次运行：摄取 3 条事件并推进水位定版
        StreamEngine first = newEngine(props);
        first.ingest(new Event("r1", "k1", 1_000, 10));
        first.ingest(new Event("r2", "k1", 2_000, 20));
        first.ingest(new Event("r3", "k1", 3_000, 30));
        first.advanceWatermark(WINDOW);
        List<WindowResult> beforeRestart = first.results("k1");
        log.info("before restart: watermark={} results={}", first.currentWatermark(), beforeRestart);

        // 模拟进程重启：同一检查点目录新建引擎
        StreamEngine second = newEngine(props);
        assertThat(second.currentWatermark()).isEqualTo(WINDOW);
        assertThat(second.results("k1")).isEqualTo(beforeRestart);
        log.info("after restart: watermark={} results={}", second.currentWatermark(), second.results("k1"));

        // 重启后重复提交同样的事件：必须全部判为 DUPLICATE，不重复计数
        // 注意：r1/r2/r3 所在窗口已定版但仍在宽限期内，去重依据（eventId 索引）随检查点恢复
        var dup1 = second.ingest(new Event("r1", "k1", 1_000, 10));
        var dup2 = second.ingest(new Event("r2", "k1", 2_000, 20));
        var dup3 = second.ingest(new Event("r3", "k1", 3_000, 30));
        log.info("duplicate resubmission after restart: {} {} {} (dedupBasis=eventId, restored from checkpoint)",
                dup1.status(), dup2.status(), dup3.status());
        assertThat(dup1.status()).isEqualTo(IngestStatus.DUPLICATE);
        assertThat(dup2.status()).isEqualTo(IngestStatus.DUPLICATE);
        assertThat(dup3.status()).isEqualTo(IngestStatus.DUPLICATE);

        WindowResult r = second.results("k1").get(0);
        log.info("window result after duplicate resubmission: {}", r);
        assertThat(r.count()).isEqualTo(3);
        assertThat(r.sum()).isEqualTo(60);
    }

    @Test
    void missingCheckpointIsRejectedInStrictMode() {
        EngineProperties props = props();
        props.setCheckpointRequiredOnStartup(true);
        assertThatThrownBy(() -> newEngine(props))
                .isInstanceOf(CheckpointMissingException.class);
    }

    @Test
    void missingCheckpointIsToleratedInDefaultMode() {
        StreamEngine engine = newEngine(props());
        assertThat(engine.currentWatermark()).isEqualTo(Long.MIN_VALUE);
        assertThat(engine.results("k1")).isEmpty();
    }

    @Test
    void corruptedCheckpointIsRejectedNotPartiallyApplied() throws IOException {
        EngineProperties props = props();
        StreamEngine first = newEngine(props);
        first.ingest(new Event("c1", "k1", 1_000, 1));
        first.advanceWatermark(WINDOW);

        // 破坏检查点文件内容
        Path file = checkpointDir.resolve("checkpoint.json");
        byte[] bytes = Files.readAllBytes(file);
        bytes[bytes.length / 2] ^= 0x7F;
        Files.write(file, bytes);

        assertThatThrownBy(() -> newEngine(props))
                .isInstanceOf(CheckpointCorruptedException.class);
    }

    @Test
    void incompatibleVersionIsRejected() throws IOException {
        EngineProperties props = props();
        StreamEngine first = newEngine(props);
        first.ingest(new Event("v1", "k1", 1_000, 1));
        first.advanceWatermark(WINDOW);

        // 篡改格式版本号
        Path file = checkpointDir.resolve("checkpoint.json");
        String json = Files.readString(file);
        Files.writeString(file, json.replace("\"version\":1", "\"version\":999"));

        assertThatThrownBy(() -> newEngine(props))
                .isInstanceOf(CheckpointVersionException.class);
    }

    @Test
    void garbageFileIsRejectedAsCorrupted() throws IOException {
        Files.createDirectories(checkpointDir);
        Files.writeString(checkpointDir.resolve("checkpoint.json"), "this is not a checkpoint");
        assertThatThrownBy(() -> newEngine(props()))
                .isInstanceOf(CheckpointCorruptedException.class);
    }
}
