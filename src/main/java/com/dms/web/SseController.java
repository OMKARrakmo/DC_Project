package com.dms.web;

import com.dms.core.EventLogger;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter;

@RestController
@RequestMapping("/api")
public class SseController {

    private final EventLogger eventLogger;

    public SseController(EventLogger eventLogger) {
        this.eventLogger = eventLogger;
    }

    @GetMapping(value = "/events", produces = MediaType.TEXT_EVENT_STREAM_VALUE)
    public SseEmitter streamEvents() {
        // Set timeout to 30 minutes
        SseEmitter emitter = new SseEmitter(1800_000L);
        eventLogger.addEmitter(emitter);
        return emitter;
    }
}
