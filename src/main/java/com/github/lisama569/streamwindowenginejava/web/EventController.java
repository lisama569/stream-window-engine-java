package com.github.lisama569.streamwindowenginejava.web;

import com.github.lisama569.streamwindowenginejava.engine.StreamEngine;
import com.github.lisama569.streamwindowenginejava.model.Event;
import com.github.lisama569.streamwindowenginejava.model.IngestResult;
import com.github.lisama569.streamwindowenginejava.model.WindowResult;
import com.github.lisama569.streamwindowenginejava.web.dto.IngestRequest;
import com.github.lisama569.streamwindowenginejava.web.dto.WatermarkRequest;
import org.springframework.web.bind.annotation.*;

import java.util.List;
import java.util.Map;

/**
 * REST 接口：摄取、水位推进、结果查询与状态观测。
 */
@RestController
@RequestMapping("/api")
public class EventController {

    private final StreamEngine engine;

    public EventController(StreamEngine engine) {
        this.engine = engine;
    }

    @PostMapping("/events")
    public IngestResult ingest(@RequestBody IngestRequest request) {
        return engine.ingest(new Event(request.eventId(), request.key(), request.eventTime(), request.value()));
    }

    @PostMapping("/watermark")
    public Map<String, Long> advanceWatermark(@RequestBody WatermarkRequest request) {
        long wm = engine.advanceWatermark(request.watermark());
        return Map.of("watermark", wm);
    }

    @GetMapping("/windows")
    public List<WindowResult> windows(@RequestParam String key) {
        return engine.results(key);
    }

    @GetMapping("/stats")
    public Map<String, Object> stats() {
        return engine.stats();
    }
}
