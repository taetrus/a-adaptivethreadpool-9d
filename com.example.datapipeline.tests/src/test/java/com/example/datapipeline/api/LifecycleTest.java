package com.example.datapipeline.api;

import static org.junit.Assert.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.junit.Test;

public class LifecycleTest {

    private static final java.util.concurrent.Executor DIRECT = Runnable::run;

    private DataPipeline<Integer, Integer> build(ExecutionMode mode, UiUpdateMode ui) {
        return DataPipeline.<Integer, Integer>builder()
                .processor(i -> i)
                .uiConsumer(i -> {})
                .overflowPolicy(OverflowPolicy.PROCESS_ALL)
                .executionMode(mode)
                .uiUpdateMode(ui)
                .uiThreadExecutor(DIRECT)
                .build();
    }

    @Test public void submitAfterCloseReturnsFalseWithoutThrowing() {
        DataPipeline<Integer, Integer> p = build(ExecutionMode.SEQUENTIAL, UiUpdateMode.immediate());
        p.close();
        assertFalse(p.submit(1));
    }

    @Test public void closeIsIdempotent() {
        DataPipeline<Integer, Integer> p = build(ExecutionMode.SEQUENTIAL, UiUpdateMode.immediate());
        p.close();
        p.close(); // must not throw
    }

    @Test public void closeTerminatesAllPipelineThreads() throws Exception {
        DataPipeline<Integer, Integer> p = build(
                ExecutionMode.parallelOrdered(3), UiUpdateMode.periodic(50));
        // let it spin up fully
        p.submit(1);
        Thread.sleep(150);
        p.close();
        Thread.sleep(200); // give threads time to die
        for (Thread t : allThreads()) {
            assertFalse("thread still alive after close: " + t.getName(),
                    t.getName().startsWith("datapipeline-") && t.isAlive());
        }
    }

    @Test public void closeLetsInFlightWorkFinish() throws Exception {
        CountDownLatch started = new CountDownLatch(1);
        CountDownLatch delivered = new CountDownLatch(1);
        DataPipeline<Integer, Integer> p = DataPipeline.<Integer, Integer>builder()
                .processor(i -> {
                    started.countDown();
                    try { Thread.sleep(200); } catch (InterruptedException ignored) {}
                    return i;
                })
                .uiConsumer(i -> delivered.countDown())
                .overflowPolicy(OverflowPolicy.PROCESS_ALL)
                .executionMode(ExecutionMode.parallelOrdered(2))
                .uiThreadExecutor(DIRECT)
                .build();
        p.submit(1);
        assertTrue(started.await(2, TimeUnit.SECONDS));
        p.close(); // pool shutdown() lets the in-flight task finish within the 2s budget
        assertTrue("in-flight item should complete during close",
                delivered.await(1, TimeUnit.SECONDS));
    }

    private static Thread[] allThreads() {
        Thread[] threads = new Thread[Thread.activeCount() * 2];
        int n = Thread.enumerate(threads);
        return java.util.Arrays.copyOf(threads, n);
    }
}
