package com.github.lisama569.streamwindowenginejava.service;

import com.github.lisama569.streamwindowenginejava.checkpoint.CheckpointException;
import com.github.lisama569.streamwindowenginejava.config.EngineProperties;
import com.github.lisama569.streamwindowenginejava.model.Event;
import com.github.lisama569.streamwindowenginejava.model.IngestStatus;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * 服务级测试：检查点缺失时按配置明确拒绝；关停落盘后新实例可恢复。
 */
class EventAggregationServiceTest {

    private static final Logger log = LoggerFactory.getLogger(EventAggregationServiceTest.class);

    @TempDir
    Path dir;

    private EngineProperties props(boolean required) {
        EngineProperties p = new EngineProperties();
        p.setWindowSizeMillis(1000);
        p.setAllowedLatenessMillis(2000);
        p.setCheckpointPath(dir.resolve("checkpoint.json").toString());
        p.setCheckpointRequired(required);
        return p;
    }

    @Test
    void missingRequiredCheckpointIsRejected() {
        EventAggregationService service = new EventAggregationService(props(true));
        CheckpointException ex = assertThrows(CheckpointException.class, service::restore);
        log.info("missing required checkpoint rejected: {}", ex.getMessage());
    }

    @Test
    void missingOptionalCheckpointStartsFresh() {
        EventAggregationService service = new EventAggregationService(props(false));
        service.restore(); // 不抛异常，全新启动
        assertEquals(IngestStatus.ACCEPTED,
                service.ingest(new Event("e1", "k", 100, 1)).status());
    }

    @Test
    void shutdownCheckpointAllowsRestartRecovery() {
        EventAggregationService first = new EventAggregationService(props(false));
        first.restore();
        first.ingest(new Event("e1", "k", 100, 5));
        first.ingest(new Event("e2", "k", 200, 6));
        first.shutdown(); // 落盘检查点
        log.info("first instance checkpointed at watermark={}", first.currentWatermark());

        // 模拟重启
        EventAggregationService second = new EventAggregationService(props(true));
        second.restore();
        log.info("second instance recovered watermark={}", second.currentWatermark());
        assertEquals(first.currentWatermark(), second.currentWatermark());

        // 重复提交不重复计数
        assertEquals(IngestStatus.DUPLICATE, second.ingest(new Event("e1", "k", 100, 5)).status());
        // 新事件正常推进
        assertEquals(IngestStatus.ACCEPTED,
                second.ingest(new Event("pusher", "k", 10_000, 0)).status());
        assertEquals(1, second.finalizedResults().size());
        assertEquals(2, second.finalizedResults().get(0).totalCount());
        assertEquals(11, second.finalizedResults().get(0).totalValue());
        log.info("recovered window result: {}", second.finalizedResults().get(0));
    }
}
