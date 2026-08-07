package com.example.datapipeline.internal;

/** Stage 1: policy-bearing buffer between the data source and the processing stage. */
interface Intake<T> {
    /** Non-blocking; called from the source thread. Always accepts (policy decides what gives way). */
    boolean offer(T item);
    /** Blocks until an item is available; called by the worker/dispatcher. */
    T take() throws InterruptedException;
    /** Non-blocking; null if empty. Used by tick-pull mode. */
    T poll();
}
