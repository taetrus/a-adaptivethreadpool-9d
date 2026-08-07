package com.example.datapipeline.internal;

import java.util.function.BinaryOperator;

final class ConflatingIntake<T> implements Intake<T> {
    private final Object lock = new Object();
    private final BinaryOperator<T> conflator;
    private T slot;

    ConflatingIntake(BinaryOperator<T> conflator) { this.conflator = conflator; }

    @Override public boolean offer(T item) {
        synchronized (lock) {
            if (slot == null) {
                slot = item;
            } else {
                try { slot = conflator.apply(slot, item); }
                catch (Throwable t) { slot = item; } // keep newest on conflator failure
            }
            lock.notifyAll();
        }
        return true;
    }

    @Override public T take() throws InterruptedException {
        synchronized (lock) {
            while (slot == null) lock.wait();
            T t = slot;
            slot = null;
            return t;
        }
    }

    @Override public T poll() {
        synchronized (lock) {
            T t = slot;
            slot = null;
            return t;
        }
    }
}
