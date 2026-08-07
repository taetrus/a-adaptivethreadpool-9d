package com.example.datapipeline.api;

import java.util.concurrent.Executor;
import java.util.function.BinaryOperator;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.swing.SwingUtilities;

import com.example.datapipeline.internal.PipelineImpl;

/**
 * Bounded processing pipeline: submit data from any thread, process on pipeline
 * threads, deliver results to the Swing EDT. Never blocks the submitting thread.
 */
public interface DataPipeline<T, R> extends AutoCloseable {

    /** Non-blocking. Returns false if the pipeline is closed. */
    boolean submit(T item);

    /** Idempotent orderly shutdown; waits up to 2s for in-flight work. */
    @Override void close();

    static <T, R> Builder<T, R> builder() { return new Builder<T, R>(); }

    final class Builder<T, R> {
        private static final Logger LOG = Logger.getLogger(Builder.class.getName());

        private Function<T, R> processor;
        private Consumer<R> uiConsumer;
        private OverflowPolicy overflowPolicy;
        private BinaryOperator<T> conflator;
        private int bufferCapacity = 1024;
        private boolean bufferCapacitySet;
        private ExecutionMode executionMode = ExecutionMode.SEQUENTIAL;
        private UiUpdateMode uiUpdateMode = UiUpdateMode.immediate();
        private boolean processOnlyOnTick;
        private ErrorHandler errorHandler =
                (t, item) -> LOG.log(Level.WARNING, "Pipeline error on item: " + item, t);
        private Consumer<T> onOverflow;
        private Executor uiThreadExecutor = SwingUtilities::invokeLater;
        private ExecutionMode effectiveExecutionMode;

        Builder() {}

        public Builder<T, R> processor(Function<T, R> p) { this.processor = p; return this; }
        public Builder<T, R> uiConsumer(Consumer<R> c) { this.uiConsumer = c; return this; }
        public Builder<T, R> overflowPolicy(OverflowPolicy p) { this.overflowPolicy = p; return this; }
        public Builder<T, R> conflator(BinaryOperator<T> c) { this.conflator = c; return this; }
        public Builder<T, R> bufferCapacity(int n) { this.bufferCapacity = n; this.bufferCapacitySet = true; return this; }
        public Builder<T, R> executionMode(ExecutionMode m) { this.executionMode = m; return this; }
        public Builder<T, R> uiUpdateMode(UiUpdateMode m) { this.uiUpdateMode = m; return this; }
        public Builder<T, R> processOnlyOnTick(boolean b) { this.processOnlyOnTick = b; return this; }
        public Builder<T, R> onError(ErrorHandler h) { this.errorHandler = h; return this; }
        public Builder<T, R> onOverflow(Consumer<T> c) { this.onOverflow = c; return this; }
        /** Replace the EDT with another executor (tests, JavaFX). Default: SwingUtilities::invokeLater. */
        public Builder<T, R> uiThreadExecutor(Executor e) { this.uiThreadExecutor = e; return this; }

        public DataPipeline<T, R> build() {
            if (processor == null) throw new IllegalStateException("processor is required");
            if (uiConsumer == null) throw new IllegalStateException("uiConsumer is required");
            if (overflowPolicy == null) throw new IllegalStateException("overflowPolicy is required");
            if (overflowPolicy == OverflowPolicy.CONFLATE && conflator == null)
                throw new IllegalStateException("CONFLATE requires a conflator");
            if (overflowPolicy != OverflowPolicy.CONFLATE && conflator != null)
                throw new IllegalStateException("conflator is only valid with CONFLATE");
            if (overflowPolicy != OverflowPolicy.PROCESS_ALL && bufferCapacitySet)
                throw new IllegalStateException("bufferCapacity is only valid with PROCESS_ALL");
            if (overflowPolicy != OverflowPolicy.PROCESS_ALL && onOverflow != null)
                throw new IllegalStateException("onOverflow is only valid with PROCESS_ALL");
            if (processOnlyOnTick && !uiUpdateMode.isPeriodic())
                throw new IllegalStateException("processOnlyOnTick requires UiUpdateMode.periodic(...)");
            if (bufferCapacitySet && bufferCapacity < 1)
                throw new IllegalStateException("bufferCapacity must be >= 1");

            ExecutionMode effective = executionMode;
            if (effective.isParallel() && overflowPolicy != OverflowPolicy.PROCESS_ALL) {
                LOG.warning("PARALLEL_ORDERED with " + overflowPolicy
                        + " cannot parallelize (at most one pending item); running sequentially");
                effective = ExecutionMode.SEQUENTIAL;
            }
            if (effective.isParallel() && processOnlyOnTick) {
                LOG.warning("PARALLEL_ORDERED with processOnlyOnTick cannot parallelize; running sequentially");
                effective = ExecutionMode.SEQUENTIAL;
            }
            this.effectiveExecutionMode = effective;
            return new PipelineImpl<T, R>(this);
        }

        // internal — not API; read by PipelineImpl
        public Function<T, R> getProcessor() { return processor; }
        public Consumer<R> getUiConsumer() { return uiConsumer; }
        public OverflowPolicy getOverflowPolicy() { return overflowPolicy; }
        public BinaryOperator<T> getConflator() { return conflator; }
        public int getBufferCapacity() { return bufferCapacity; }
        public ExecutionMode getExecutionMode() { return executionMode; }
        /** The mode actually used to build the pipeline (after degrading incompatible combinations). */
        public ExecutionMode getEffectiveExecutionMode() { return effectiveExecutionMode; }
        public UiUpdateMode getUiUpdateMode() { return uiUpdateMode; }
        public boolean isProcessOnlyOnTick() { return processOnlyOnTick; }
        public ErrorHandler getErrorHandler() { return errorHandler; }
        public Consumer<T> getOnOverflow() { return onOverflow; }
        public Executor getUiThreadExecutor() { return uiThreadExecutor; }
    }
}
