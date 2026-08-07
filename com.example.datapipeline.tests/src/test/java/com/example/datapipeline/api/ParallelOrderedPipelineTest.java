package com.example.datapipeline.api;

import static org.junit.Assert.*;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.junit.Test;

public class ParallelOrderedPipelineTest {

    private static final java.util.concurrent.Executor DIRECT = Runnable::run;

    @Test public void resultsArriveInSubmissionOrderDespiteUnevenProcessingTimes() throws Exception {
        final int N = 20;
        List<Integer> seen = Collections.synchronizedList(new ArrayList<Integer>());
        CountDownLatch done = new CountDownLatch(N);
        DataPipeline<Integer, Integer> p = DataPipeline.<Integer, Integer>builder()
                .processor(i -> {
                    // early items sleep longer → finish out of order
                    try { Thread.sleep((N - i) % 7 * 10); } catch (InterruptedException ignored) {}
                    return i;
                })
                .uiConsumer(i -> { seen.add(i); done.countDown(); })
                .overflowPolicy(OverflowPolicy.PROCESS_ALL)
                .executionMode(ExecutionMode.parallelOrdered(4))
                .uiThreadExecutor(DIRECT)
                .build();
        try {
            for (int i = 0; i < N; i++) p.submit(i);
            assertTrue(done.await(10, TimeUnit.SECONDS));
            List<Integer> expected = new ArrayList<Integer>();
            for (int i = 0; i < N; i++) expected.add(i);
            assertEquals(expected, seen);
        } finally { p.close(); }
    }

    @Test public void dispatcherToPoolHandoffIsBoundedSoOverflowEngages() throws Exception {
        final int N = 20;
        CountDownLatch workersBlocked = new CountDownLatch(2);
        CountDownLatch releaseWorkers = new CountDownLatch(1);
        CountDownLatch overflowSeen = new CountDownLatch(1);
        DataPipeline<Integer, Integer> p = DataPipeline.<Integer, Integer>builder()
                .processor(i -> {
                    workersBlocked.countDown();
                    try { releaseWorkers.await(10, TimeUnit.SECONDS); } catch (InterruptedException ignored) {}
                    return i;
                })
                .uiConsumer(i -> {})
                .overflowPolicy(OverflowPolicy.PROCESS_ALL)
                .bufferCapacity(2)
                .onOverflow(dropped -> overflowSeen.countDown())
                .executionMode(ExecutionMode.parallelOrdered(2))
                .uiThreadExecutor(DIRECT)
                .build();
        try {
            for (int i = 0; i < N; i++) p.submit(i);
            // wait for both workers to be saturated on the latch
            assertTrue(workersBlocked.await(5, TimeUnit.SECONDS));
            // with the handoff bounded, the intake must fill and the drop-oldest policy must fire
            assertTrue("expected onOverflow to fire once the bounded handoff saturates the pool",
                    overflowSeen.await(5, TimeUnit.SECONDS));
        } finally {
            releaseWorkers.countDown();
            p.close();
        }
    }

    @Test public void failedItemDoesNotStallSubsequentResults() throws Exception {
        List<Integer> seen = Collections.synchronizedList(new ArrayList<Integer>());
        CountDownLatch done = new CountDownLatch(2);
        DataPipeline<Integer, Integer> p = DataPipeline.<Integer, Integer>builder()
                .processor(i -> { if (i == 1) throw new RuntimeException("boom"); return i; })
                .uiConsumer(i -> { seen.add(i); done.countDown(); })
                .overflowPolicy(OverflowPolicy.PROCESS_ALL)
                .executionMode(ExecutionMode.parallelOrdered(2))
                .uiThreadExecutor(DIRECT)
                .build();
        try {
            p.submit(0); p.submit(1); p.submit(2);
            assertTrue(done.await(5, TimeUnit.SECONDS));
            assertEquals(java.util.Arrays.asList(0, 2), seen);
        } finally { p.close(); }
    }
}
