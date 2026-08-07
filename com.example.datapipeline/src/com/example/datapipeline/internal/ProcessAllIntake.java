package com.example.datapipeline.internal;

import java.util.ArrayDeque;
import java.util.function.Consumer;

final class ProcessAllIntake<T> implements Intake<T> {
    private final Object lock = new Object();
    private final ArrayDeque<T> queue = new ArrayDeque<T>();
    private final int capacity;
    private final Consumer<T> onOverflow;

    ProcessAllIntake(int capacity, Consumer<T> onOverflow) {
        this.capacity = capacity;
        this.onOverflow = onOverflow;
    }

    @Override public boolean offer(T item) {
        T dropped = null;
        synchronized (lock) {
            if (queue.size() == capacity) dropped = queue.poll();
            queue.add(item);
            lock.notifyAll();
        }
        if (dropped != null) {
            try { onOverflow.accept(dropped); } catch (Throwable ignored) {}
        }
        return true;
    }

    @Override public T take() throws InterruptedException {
        synchronized (lock) {
            while (queue.isEmpty()) lock.wait();
            return queue.poll();
        }
    }

    @Override public T poll() {
        synchronized (lock) { return queue.poll(); }
    }
}
