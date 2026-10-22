package com.dms.core;

import org.springframework.stereotype.Service;

import java.io.Serializable;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

@Service
public class ThreadPoolService {

    public record TaskResult(int taskIndex, String threadName, long latencyMs, long timestamp) implements Serializable {}

    private final NodeConfig nodeConfig;
    private final EventLogger eventLogger;
    private final LamportClock lamportClock;

    private final AtomicInteger threadCounter = new AtomicInteger(1);
    private final ThreadPoolExecutor executor;

    public ThreadPoolService(NodeConfig nodeConfig,
                             EventLogger eventLogger,
                             LamportClock lamportClock) {
        this.nodeConfig = nodeConfig;
        this.eventLogger = eventLogger;
        this.lamportClock = lamportClock;

        int initialCore = 4;
        int maxPool = 16;
        long keepAliveTime = 60L;

        this.executor = new ThreadPoolExecutor(
                initialCore,
                maxPool,
                keepAliveTime,
                TimeUnit.SECONDS,
                new LinkedBlockingQueue<>(100),
                r -> {
                    Thread t = new Thread(r, "dms-worker-" + threadCounter.getAndIncrement());
                    t.setDaemon(true);
                    return t;
                }
        );
    }

    public void setPoolSize(int coreSize) {
        setCorePoolSize(coreSize);
    }

    public void setCorePoolSize(int coreSize) {
        int bounded = Math.max(1, Math.min(coreSize, 32));
        if (bounded > executor.getMaximumPoolSize()) {
            executor.setMaximumPoolSize(bounded);
            executor.setCorePoolSize(bounded);
        } else {
            executor.setCorePoolSize(bounded);
            executor.setMaximumPoolSize(Math.max(bounded, executor.getMaximumPoolSize()));
        }
        lamportClock.tick();
        eventLogger.log("EXP2-THREAD", String.format(
                "Updated ThreadPoolExecutor core size to %d (max: %d)",
                executor.getCorePoolSize(), executor.getMaximumPoolSize()));
    }

    public int getCorePoolSize() {
        return executor.getCorePoolSize();
    }

    public Map<String, Object> getStatus() {
        return getPoolStatus();
    }

    public Map<String, Object> getPoolStatus() {
        Map<String, Object> map = new HashMap<>();
        map.put("corePoolSize", executor.getCorePoolSize());
        map.put("maximumPoolSize", executor.getMaximumPoolSize());
        map.put("activeCount", executor.getActiveCount());
        map.put("poolSize", executor.getPoolSize());
        map.put("queueSize", executor.getQueue().size());
        map.put("completedTaskCount", executor.getCompletedTaskCount());
        map.put("taskCount", executor.getTaskCount());
        return map;
    }

    public <T> Future<T> submit(Callable<T> task) {
        return executor.submit(task);
    }

    public Future<?> submit(Runnable task) {
        return executor.submit(task);
    }

    /**
     * Executes a burst of tasks on the thread pool with tracked latency and worker thread names.
     */
    public List<TaskResult> runBurst(int count, int delayMs) {
        int bounded = Math.max(1, Math.min(count, 50));
        long ts = lamportClock.tick();
        eventLogger.logWithLamport("EXP2-THREAD", ts, String.format(
                "Executing burst of %d tasks on ThreadPoolExecutor (artificial delay=%dms)",
                bounded, delayMs));

        List<Future<TaskResult>> futures = new ArrayList<>();
        for (int i = 1; i <= bounded; i++) {
            final int index = i;
            futures.add(executor.submit(() -> {
                long taskStart = System.currentTimeMillis();
                String threadName = Thread.currentThread().getName();
                if (delayMs > 0) {
                    try {
                        Thread.sleep(delayMs);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                }
                long elapsed = System.currentTimeMillis() - taskStart;
                return new TaskResult(index, threadName, elapsed, System.currentTimeMillis());
            }));
        }

        List<TaskResult> results = new ArrayList<>();
        Map<String, Integer> distribution = new TreeMap<>();
        for (Future<TaskResult> f : futures) {
            try {
                TaskResult r = f.get();
                results.add(r);
                distribution.put(r.threadName(), distribution.getOrDefault(r.threadName(), 0) + 1);
            } catch (Exception e) {
                eventLogger.log("EXP2-THREAD", "Error in thread burst task: " + e.getMessage());
            }
        }

        results.sort(Comparator.comparingInt(TaskResult::taskIndex));
        eventLogger.log("EXP2-THREAD", String.format(
                "Burst finished: %d tasks executed across workers %s",
                results.size(), distribution));
        return results;
    }

    /**
     * Executes the Exp 2 Race Condition Demonstration:
     * N threads increment a shared counter M times.
     * Unsafe: threads synchronize at CyclicBarrier, read, sleep, then write (causing lost updates).
     * Safe: increments atomically using AtomicInteger (expected == actual).
     */
    public Map<String, Object> runRaceDemo(int numThreads, int incrementsPerThread) {
        int threads = Math.max(2, Math.min(numThreads, 20));
        int increments = Math.max(5, Math.min(incrementsPerThread, 50));
        int expected = threads * increments;

        long ts = lamportClock.tick();
        eventLogger.logWithLamport("EXP2-THREAD", ts, String.format(
                "Starting Race Condition Demo: %d threads, %d increments each (Expected total: %d)",
                threads, increments, expected));

        // 1. Unsafe counter with CyclicBarrier inducing race condition
        int[] unsafeWrapper = new int[1];
        CyclicBarrier barrier = new CyclicBarrier(threads);
        CountDownLatch latchUnsafe = new CountDownLatch(threads);
        List<Thread> unsafeThreads = new ArrayList<>();

        for (int i = 0; i < threads; i++) {
            Thread t = new Thread(() -> {
                for (int j = 0; j < increments; j++) {
                    try {
                        barrier.await(2, TimeUnit.SECONDS);
                        // Race window: read value into local variable
                        int current = unsafeWrapper[0];
                        // Brief sleep forces context switch
                        Thread.sleep(2);
                        // Write back calculated value (overwrites other threads' concurrent updates)
                        unsafeWrapper[0] = current + 1;
                    } catch (Exception ignored) {
                    }
                }
                latchUnsafe.countDown();
            }, "unsafe-worker-" + i);
            unsafeThreads.add(t);
            t.start();
        }

        try {
            latchUnsafe.await(10, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }

        int unsafeActual = unsafeWrapper[0];
        int lostUpdates = expected - unsafeActual;

        // 2. Safe counter with AtomicInteger
        AtomicInteger safeCounter = new AtomicInteger(0);
        CountDownLatch latchSafe = new CountDownLatch(threads);
        List<Thread> safeThreads = new ArrayList<>();

        for (int i = 0; i < threads; i++) {
            Thread t = new Thread(() -> {
                for (int j = 0; j < increments; j++) {
                    safeCounter.incrementAndGet();
                }
                latchSafe.countDown();
            }, "safe-worker-" + i);
            safeThreads.add(t);
            t.start();
        }

        try {
            latchSafe.await(10, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }

        int safeActual = safeCounter.get();

        long endTs = lamportClock.tick();
        eventLogger.logWithLamport("EXP2-THREAD", endTs, String.format(
                "Race Demo Results: Expected=%d | Unsafe Actual=%d (Lost Updates=%d) | Safe Actual=%d (Match=%s)",
                expected, unsafeActual, lostUpdates, safeActual, safeActual == expected));

        Map<String, Object> result = new HashMap<>();
        result.put("threads", threads);
        result.put("incrementsPerThread", increments);
        result.put("expected", expected);
        result.put("unsafeActual", unsafeActual);
        result.put("unsafeLostUpdates", lostUpdates);
        result.put("lostUpdates", lostUpdates);
        result.put("safeActual", safeActual);
        result.put("safeLostUpdates", 0);
        result.put("safeMatchesExpected", safeActual == expected);
        result.put("threadPoolType", "ThreadPoolExecutor (dms-worker-%d)");
        result.put("explanation", String.format(
                "Unsafe counter experienced %d lost updates due to simultaneous unsynchronized read-modify-write operations across %d threads at CyclicBarrier. AtomicInteger guarantees atomicity with 0 lost updates.",
                lostUpdates, threads));

        return result;
    }
}
