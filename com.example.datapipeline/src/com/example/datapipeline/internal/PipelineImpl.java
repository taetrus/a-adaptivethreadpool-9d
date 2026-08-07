package com.example.datapipeline.internal;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.example.datapipeline.api.DataPipeline;
import com.example.datapipeline.api.ErrorHandler;
import com.example.datapipeline.api.ExecutionMode;
import com.example.datapipeline.api.UiUpdateMode;

public final class PipelineImpl<T, R> implements DataPipeline<T, R> {
    private static final Logger LOG = Logger.getLogger(PipelineImpl.class.getName());
    private static final long CLOSE_WAIT_MS = 2000;

    private final Function<T, R> processor;
    private final ErrorHandler errorHandler;
    private final Intake<T> intake;
    private final CoalescingPublisher<R> publisher;
    private final UiUpdateMode uiMode;
    private final boolean processOnlyOnTick;
    private final ExecutionMode execMode;

    private final AtomicReference<R> latestResult = new AtomicReference<R>(); // periodic mode
    private volatile boolean closed;

    private Thread worker;                    // SEQUENTIAL
    private Thread dispatcher;                // PARALLEL_ORDERED (Task 7)
    private ExecutorService pool;             // PARALLEL_ORDERED (Task 7)
    private ScheduledExecutorService scheduler; // periodic modes (Task 8)

    public PipelineImpl(DataPipeline.Builder<T, R> b) {
        this.processor = b.getProcessor();
        this.errorHandler = b.getErrorHandler();
        this.uiMode = b.getUiUpdateMode();
        this.processOnlyOnTick = b.isProcessOnlyOnTick();
        this.execMode = b.getExecutionMode();
        this.publisher = new CoalescingPublisher<R>(
                b.getUiThreadExecutor(), b.getUiConsumer(), b.getErrorHandler());
        this.intake = createIntake(b);
        start();
    }

    private Intake<T> createIntake(DataPipeline.Builder<T, R> b) {
        switch (b.getOverflowPolicy()) {
            case PROCESS_ALL:
                Consumer<T> onOverflow = b.getOnOverflow() != null ? b.getOnOverflow()
                        : item -> LOG.warning("Buffer full, dropped oldest item: " + item);
                return new ProcessAllIntake<T>(b.getBufferCapacity(), onOverflow);
            case CONFLATE:
                return new ConflatingIntake<T>(b.getConflator());
            case LATEST_WINS:
            default:
                return new LatestWinsIntake<T>();
        }
    }

    private void start() {
        if (processOnlyOnTick) {
            startTickPull();      // Task 8
            return;
        }
        if (execMode.isParallel()) {
            startParallel();      // Task 7
        } else {
            worker = newDaemon("datapipeline-worker-0", this::sequentialLoop);
            worker.start();
        }
        if (uiMode.isPeriodic()) {
            startPeriodicUi();    // Task 8
        }
    }

    private void sequentialLoop() {
        while (!closed) {
            T item;
            try { item = intake.take(); } catch (InterruptedException e) { return; }
            processAndEmit(item);
        }
    }

    /** Runs the processor and hands the result to the UI stage. Never throws. */
    private void processAndEmit(T item) {
        R result;
        try {
            result = processor.apply(item);
        } catch (Throwable t) {
            safeError(t, item);
            return;
        }
        if (result == null) {
            safeError(new NullPointerException("processor returned null"), item);
            return;
        }
        emit(result);
    }

    void emit(R result) {
        if (uiMode.isPeriodic()) {
            latestResult.set(result);   // periodic tick picks it up (Task 8)
        } else {
            publisher.publish(result);
        }
    }

    void safeError(Throwable t, Object item) {
        try { errorHandler.onError(t, item); }
        catch (Throwable h) { LOG.log(Level.WARNING, "onError handler threw", h); }
    }

    private void startParallel() {
        final Resequencer<R> resequencer = new Resequencer<R>();
        pool = Executors.newFixedThreadPool(execMode.threadCount(),
                daemonFactory("datapipeline-worker-", 0));
        dispatcher = newDaemon("datapipeline-dispatcher", () -> {
            long seq = 0;
            while (!closed) {
                final T item;
                try { item = intake.take(); } catch (InterruptedException e) { return; }
                final long mySeq = seq++;
                try {
                    pool.execute(() -> {
                        R result = null;
                        Throwable failure = null;
                        try {
                            result = processor.apply(item);
                            if (result == null) failure = new NullPointerException("processor returned null");
                        } catch (Throwable t) {
                            failure = t;
                        }
                        if (failure != null) safeError(failure, item);
                        // Emission must stay inside the resequencer monitor so results reach the
                        // UI stage serialized in sequence order — CoalescingPublisher assumes a
                        // single producer at a time; concurrent emit() calls from multiple pool
                        // threads can silently drop results. accept/skip are themselves
                        // synchronized on `resequencer`, so this outer lock is reentrant.
                        synchronized (resequencer) {
                            java.util.List<R> releasable = (failure != null)
                                    ? resequencer.skip(mySeq)
                                    : resequencer.accept(mySeq, result);
                            for (R r : releasable) emit(r);
                        }
                    });
                } catch (java.util.concurrent.RejectedExecutionException e) {
                    return; // pool shut down during close()
                }
            }
        });
        dispatcher.start();
    }
    // ----- Task 8 fills these -----
    private void startTickPull() { throw new UnsupportedOperationException("Task 8"); }
    private void startPeriodicUi() { throw new UnsupportedOperationException("Task 8"); }

    @Override public boolean submit(T item) {
        if (closed) return false;
        intake.offer(item);
        return true;
    }

    @Override public void close() {
        if (closed) return;
        closed = true;
        if (scheduler != null) shutdown(scheduler);
        if (pool != null) shutdown(pool);
        if (dispatcher != null) dispatcher.interrupt();
        if (worker != null) worker.interrupt();
        join(dispatcher);
        join(worker);
    }

    private static void shutdown(ExecutorService es) {
        es.shutdown();
        try {
            if (!es.awaitTermination(CLOSE_WAIT_MS, TimeUnit.MILLISECONDS)) es.shutdownNow();
        } catch (InterruptedException e) {
            es.shutdownNow();
            Thread.currentThread().interrupt();
        }
    }

    private static void join(Thread t) {
        if (t == null) return;
        try { t.join(CLOSE_WAIT_MS); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }
    }

    static Thread newDaemon(String name, Runnable r) {
        Thread t = new Thread(r, name);
        t.setDaemon(true);
        return t;
    }

    /** ThreadFactory for pools; names threads datapipeline-worker-1..n. */
    static ThreadFactory daemonFactory(String prefix, int startIndex) {
        final AtomicInteger idx = new AtomicInteger(startIndex);
        return r -> {
            Thread t = new Thread(r, prefix + idx.getAndIncrement());
            t.setDaemon(true);
            return t;
        };
    }
}
