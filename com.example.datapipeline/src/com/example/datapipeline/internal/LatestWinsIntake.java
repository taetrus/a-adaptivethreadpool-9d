package com.example.datapipeline.internal;

final class LatestWinsIntake<T> implements Intake<T> {
    private final Object lock = new Object();
    private T slot;

    @Override public boolean offer(T item) {
        synchronized (lock) {
            slot = item;
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
