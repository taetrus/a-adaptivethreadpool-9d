package com.example.datapipeline.internal;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.function.Consumer;

final class ProcessAllIntake<T> implements Intake<T> {
    private final ArrayBlockingQueue<T> queue;
    private final Consumer<T> onOverflow;

    ProcessAllIntake(int capacity, Consumer<T> onOverflow) {
        this.queue = new ArrayBlockingQueue<T>(capacity);
        this.onOverflow = onOverflow;
    }

    @Override public boolean offer(T item) {
        while (!queue.offer(item)) {
            T dropped = queue.poll();
            if (dropped != null) {
                try { onOverflow.accept(dropped); } catch (Throwable ignored) {}
            }
        }
        return true;
    }

    @Override public T take() throws InterruptedException { return queue.take(); }
    @Override public T poll() { return queue.poll(); }
}
