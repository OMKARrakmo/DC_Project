package com.dms.core;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.atomic.AtomicLong;

@Component
public class EventLogger {

    private static final Logger log = LoggerFactory.getLogger(EventLogger.class);

    private final NodeConfig nodeConfig;
    private final LamportClock lamportClock;
    private final ConcurrentLinkedDeque<EventMessage> history = new ConcurrentLinkedDeque<>();
    private final Set<SseEmitter> emitters = ConcurrentHashMap.newKeySet();
    private final AtomicLong eventSequence = new AtomicLong(0);
    private static final int MAX_HISTORY = 1000;

    public EventLogger(NodeConfig nodeConfig, LamportClock lamportClock) {
        this.nodeConfig = nodeConfig;
        this.lamportClock = lamportClock;
    }

    public EventMessage log(String tag, String message) {
        long currentLamport = lamportClock.get();
        return logWithLamport(tag, currentLamport, message);
    }

    public EventMessage logWithLamport(String tag, long lamportTs, String message) {
        String eventId = String.valueOf(eventSequence.incrementAndGet());
        EventMessage event = new EventMessage(eventId, tag, nodeConfig.getId(), lamportTs, message);

        history.addLast(event);
        while (history.size() > MAX_HISTORY) {
            history.pollFirst();
        }

        // Print directly to console/SLF4J
        System.out.println(event.getFormatted());
        log.info(event.getFormatted());

        // Broadcast to SSE clients
        List<SseEmitter> deadEmitters = new ArrayList<>();
        for (SseEmitter emitter : emitters) {
            try {
                emitter.send(SseEmitter.event()
                        .id(eventId)
                        .name("log")
                        .data(event));
            } catch (IOException | IllegalStateException e) {
                deadEmitters.add(emitter);
            }
        }
        emitters.removeAll(deadEmitters);

        return event;
    }

    public void addEmitter(SseEmitter emitter) {
        emitters.add(emitter);
        emitter.onCompletion(() -> emitters.remove(emitter));
        emitter.onTimeout(() -> emitters.remove(emitter));
        emitter.onError((e) -> emitters.remove(emitter));

        // Send recent events to catch up
        for (EventMessage msg : history) {
            try {
                emitter.send(SseEmitter.event()
                        .id(msg.getId())
                        .name("log")
                        .data(msg));
            } catch (IOException | IllegalStateException e) {
                emitters.remove(emitter);
                break;
            }
        }
    }

    public List<EventMessage> getHistory() {
        return new ArrayList<>(history);
    }
}
