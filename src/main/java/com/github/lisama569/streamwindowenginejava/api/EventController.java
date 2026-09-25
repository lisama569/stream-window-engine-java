package com.github.lisama569.streamwindowenginejava.api;

import com.github.lisama569.streamwindowenginejava.model.Event;
import com.github.lisama569.streamwindowenginejava.model.IngestResult;
import com.github.lisama569.streamwindowenginejava.model.WindowResult;
import com.github.lisama569.streamwindowenginejava.service.EventAggregationService;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

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

    @GetMapping("/watermark")
    public long watermark() {
        return service.currentWatermark();
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
