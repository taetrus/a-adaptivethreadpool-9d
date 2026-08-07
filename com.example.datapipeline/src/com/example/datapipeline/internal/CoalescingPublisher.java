package com.example.datapipeline.internal;

import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

import com.example.datapipeline.api.ErrorHandler;

/**
 * Publishes results to the UI executor with at most one pending runnable:
 * if the EDT hasn't run the previous one yet, the pending value is swapped instead.
 *
 * Note: null is used as the internal empty sentinel; callers must not publish null results.
 * Null results are filtered upstream in the pipeline.
 */
final class CoalescingPublisher<R> {
    private final Executor uiExecutor;
    private final Consumer<R> consumer;
    private final ErrorHandler errorHandler;
    private final AtomicReference<R> pending = new AtomicReference<R>();

    CoalescingPublisher(Executor uiExecutor, Consumer<R> consumer, ErrorHandler errorHandler) {
        this.uiExecutor = uiExecutor;
        this.consumer = consumer;
        this.errorHandler = errorHandler;
    }

    void publish(R result) {
        java.util.Objects.requireNonNull(result, "result must not be null");
        if (pending.getAndSet(result) == null) {
            uiExecutor.execute(this::drain);
        }
    }

    private void drain() {
        R r = pending.getAndSet(null);
        if (r == null) return;
        try {
            consumer.accept(r);
        } catch (Throwable t) {
            try { errorHandler.onError(t, r); } catch (Throwable ignored) {}
        }
    }
}
