package com.github.lisama569.streamwindowenginejava.api;

import com.github.lisama569.streamwindowenginejava.model.Event;
import com.github.lisama569.streamwindowenginejava.model.IngestResult;
import com.github.lisama569.streamwindowenginejava.model.MarkerResult;
import com.github.lisama569.streamwindowenginejava.model.SourceMarker;
import com.github.lisama569.streamwindowenginejava.model.WindowResult;
import com.github.lisama569.streamwindowenginejava.service.EventAggregationService;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;
import java.util.Map;

/**
 * REST 入口：事件摄取、水位与窗口结果查询、手动触发检查点。
 */
@RestController
@RequestMapping("/api")
public class EventController {

    private final EventAggregationService service;

    public EventController(EventAggregationService service) {
        this.service = service;
    }

    @PostMapping("/events")
    public IngestResult ingest(@RequestBody Event event) {
        return service.ingest(event);
    }

    @PostMapping("/markers")
    public MarkerResult ingestMarker(@RequestBody SourceMarker marker) {
        return service.ingestMarker(marker);
    }

    @GetMapping("/watermark")
    public long watermark() {
        return service.currentWatermark();
    }

    @GetMapping("/sources")
    public Map<String, Long> sources() {
        return service.sourceWatermarks();
    }

    @GetMapping("/windows")
    public List<WindowResult> windows() {
        return service.finalizedResults();
    }

    @PostMapping("/checkpoint")
    public String checkpoint() {
        service.checkpointNow();
        return "checkpoint saved";
    }
}
