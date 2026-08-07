package com.example.datapipeline.api;

/** How items are processed: one worker in order, or a pool with re-sequenced output. */
public final class ExecutionMode {
    public static final ExecutionMode SEQUENTIAL = new ExecutionMode(1);

    private final int threads;

    private ExecutionMode(int threads) { this.threads = threads; }

    public static ExecutionMode parallelOrdered(int nThreads) {
        if (nThreads < 2) throw new IllegalArgumentException("nThreads must be >= 2, got " + nThreads);
        return new ExecutionMode(nThreads);
    }

    public boolean isParallel() { return threads > 1; }
    public int threadCount() { return threads; }
}
