package com.example.datapipeline.demo;

import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.LockSupport;
import java.util.function.Consumer;

/**
 * Fake high-frequency source: one daemon thread emitting {@link Tick}s at a
 * live-adjustable rate. The consumer is the pipeline's submit() — non-blocking
 * by contract, so this thread can never be stalled by backpressure.
 */
final class TickSource {
    private final Consumer<Tick> sink;
    private final AtomicLong submitted;
    private volatile int ratePerSecond;
    private volatile boolean running;
    private Thread thread;
    private long nextId;

    TickSource(Consumer<Tick> sink, AtomicLong submitted, int initialRate) {
        this.sink = sink;
        this.submitted = submitted;
        this.ratePerSecond = initialRate;
    }

    void setRate(int perSecond) {
        this.ratePerSecond = Math.max(1, perSecond);
    }

    synchronized void start() {
        if (running) return;
        running = true;
        thread = new Thread(this::run, "demo-tick-source");
        thread.setDaemon(true);
        thread.start();
    }

    synchronized void stop() {
        running = false;
        if (thread != null) {
            thread.interrupt();
            thread = null;
        }
    }

    private void run() {
        while (running) {
            long intervalNanos = 1_000_000_000L / ratePerSecond;
            sink.accept(new Tick(nextId++, 1));
            submitted.incrementAndGet();
            LockSupport.parkNanos(intervalNanos);
            if (Thread.interrupted()) return;
        }
    }
}
