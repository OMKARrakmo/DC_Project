package com.dms.core;

import org.springframework.stereotype.Component;

import java.util.concurrent.atomic.AtomicLong;

@Component
public class LamportClock {

    private final AtomicLong clock = new AtomicLong(0);

    public long get() {
        return clock.get();
    }

    public long tick() {
        return clock.incrementAndGet();
    }

    public long update(long receivedTs) {
        return clock.updateAndGet(current -> Math.max(current, receivedTs) + 1);
    }

    public void set(long val) {
        clock.set(val);
    }
}
