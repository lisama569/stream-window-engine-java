package com.github.lisama569.streamwindowenginejava.service;

import com.github.lisama569.streamwindowenginejava.checkpoint.CheckpointData;
import com.github.lisama569.streamwindowenginejava.checkpoint.CheckpointException;
import com.github.lisama569.streamwindowenginejava.checkpoint.CheckpointStore;
import com.github.lisama569.streamwindowenginejava.config.EngineProperties;
import com.github.lisama569.streamwindowenginejava.core.WindowEngine;
import com.github.lisama569.streamwindowenginejava.model.Event;
import com.github.lisama569.streamwindowenginejava.model.IngestResult;
import com.github.lisama569.streamwindowenginejava.model.MarkerResult;
import com.github.lisama569.streamwindowenginejava.model.SourceMarker;
import com.github.lisama569.streamwindowenginejava.model.WindowResult;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.nio.file.Path;
import java.util.List;
import java.util.Optional;

/**
 * 聚合服务门面：组合窗口引擎与本地文件检查点存储。
 * 启动时从最近有效检查点恢复；检查点缺失（且配置要求必须存在）、损坏或版本不兼容时
 * 抛出 {@link CheckpointException} 明确拒绝启动，不做部分应用。
 * 正常关停时落盘一次检查点。
 */
@Service
public class EventAggregationService {

    private static final Logger log = LoggerFactory.getLogger(EventAggregationService.class);

    private final EngineProperties properties;
    private final CheckpointStore checkpointStore;
    private final WindowEngine engine;

    public EventAggregationService(EngineProperties properties) {
        this.properties = properties;
        this.checkpointStore = new CheckpointStore(Path.of(properties.getCheckpointPath()));
        this.engine = new WindowEngine(
                properties.getWindowSizeMillis(),
                properties.getAllowedLatenessMillis(),
                properties.getMaxOpenWindows(),
                properties.getMaxDedupEntries(),
                properties.getMaxRetainedResults(),
                properties.getMaxSources(),
                r -> log.info("window result emitted: {}", r));
    }

    @PostConstruct
    void restore() {
        Optional<CheckpointData> data = checkpointStore.load();
        if (data.isEmpty()) {
            if (properties.isCheckpointRequired()) {
                throw new CheckpointException(
                        "checkpoint is required but missing at " + properties.getCheckpointPath());
            }
            log.info("no checkpoint found at {}, starting fresh", properties.getCheckpointPath());
            return;
        }
        engine.restore(data.get());
    }

    @PreDestroy
    void shutdown() {
        checkpointNow();
    }

    public IngestResult ingest(Event event) {
        return engine.ingest(event);
    }

    /** 提交来源进度/空闲标记，结论可区分（APPLIED/DUPLICATE/STALE/BACKPRESSURE）。 */
    public MarkerResult submitMarker(SourceMarker marker) {
        return engine.submitMarker(marker);
    }

    /** 指定来源当前已知进度；未知来源返回 Long.MIN_VALUE。 */
    public long sourceProgress(String sourceId) {
        return engine.sourceProgress(sourceId);
    }

    public long currentWatermark() {
        return engine.currentWatermark();
    }

    public List<WindowResult> finalizedResults() {
        return engine.finalizedResults();
    }

    public void checkpointNow() {
        CheckpointData data = engine.snapshot();
        checkpointStore.save(data);
        log.info("checkpoint saved: watermark={} dedupEntries={} openWindows={} finalizedResults={} sources={}",
                data.watermark(), data.dedupEntries().size(),
                data.openWindows().size(), data.finalizedResults().size(),
                data.sourceProgress().size());
    }
}
