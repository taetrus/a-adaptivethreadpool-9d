package com.example.datapipeline.api;

import static org.junit.Assert.*;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.Test;

public class PeriodicUiTest {

    private static final java.util.concurrent.Executor DIRECT = Runnable::run;

    @Test public void periodicDeliversNewestResultPerTick() throws Exception {
        List<Integer> seen = Collections.synchronizedList(new ArrayList<Integer>());
        CountDownLatch first = new CountDownLatch(1);
        DataPipeline<Integer, Integer> p = DataPipeline.<Integer, Integer>builder()
                .processor(i -> i)
                .uiConsumer(i -> { seen.add(i); first.countDown(); })
                .overflowPolicy(OverflowPolicy.PROCESS_ALL)
                .uiUpdateMode(UiUpdateMode.periodic(100))
                .uiThreadExecutor(DIRECT)
                .build();
        try {
            for (int i = 1; i <= 50; i++) p.submit(i); // all processed fast, before first tick
            assertTrue(first.await(2, TimeUnit.SECONDS));
            Thread.sleep(250); // a couple more ticks with no new data
            int updates = seen.size();
            assertTrue("expected few coalesced updates, got " + updates, updates <= 3);
            assertEquals(Integer.valueOf(50), seen.get(seen.size() - 1)); // newest won
        } finally { p.close(); }
    }

    @Test public void periodicTickWithNoNewDataPushesNothing() throws Exception {
        AtomicInteger updates = new AtomicInteger();
        CountDownLatch first = new CountDownLatch(1);
        DataPipeline<Integer, Integer> p = DataPipeline.<Integer, Integer>builder()
                .processor(i -> i)
                .uiConsumer(i -> { updates.incrementAndGet(); first.countDown(); })
                .overflowPolicy(OverflowPolicy.LATEST_WINS)
                .uiUpdateMode(UiUpdateMode.periodic(50))
                .uiThreadExecutor(DIRECT)
                .build();
        try {
            p.submit(1);
            assertTrue(first.await(2, TimeUnit.SECONDS));
            Thread.sleep(300); // ~6 empty ticks
            assertEquals(1, updates.get());
        } finally { p.close(); }
    }

    @Test public void tickPullProcessesOnlyOncePerTickOnFreshestData() throws Exception {
        AtomicInteger processed = new AtomicInteger();
        List<Integer> seen = Collections.synchronizedList(new ArrayList<Integer>());
        CountDownLatch first = new CountDownLatch(1);
        DataPipeline<Integer, Integer> p = DataPipeline.<Integer, Integer>builder()
                .processor(i -> { processed.incrementAndGet(); return i; })
                .uiConsumer(i -> { seen.add(i); first.countDown(); })
                .overflowPolicy(OverflowPolicy.LATEST_WINS)
                .uiUpdateMode(UiUpdateMode.periodic(100))
                .processOnlyOnTick(true)
                .uiThreadExecutor(DIRECT)
                .build();
        try {
            for (int i = 1; i <= 100; i++) p.submit(i); // burst before first tick
            assertTrue(first.await(2, TimeUnit.SECONDS));
            assertTrue("processed " + processed.get() + " times, expected 1 or 2", processed.get() <= 2);
            assertEquals(Integer.valueOf(100), seen.get(0)); // freshest data was processed
        } finally { p.close(); }
    }

    @Test public void tickPullProcessorErrorDoesNotKillScheduler() throws Exception {
        CountDownLatch errored = new CountDownLatch(1);
        CountDownLatch recovered = new CountDownLatch(1);
        DataPipeline<Integer, Integer> p = DataPipeline.<Integer, Integer>builder()
                .processor(i -> { if (i == 1) throw new RuntimeException("boom"); return i; })
                .uiConsumer(i -> recovered.countDown())
                .overflowPolicy(OverflowPolicy.LATEST_WINS)
                .uiUpdateMode(UiUpdateMode.periodic(50))
                .processOnlyOnTick(true)
                .onError((t, item) -> errored.countDown())
                .uiThreadExecutor(DIRECT)
                .build();
        try {
            p.submit(1);
            assertTrue(errored.await(2, TimeUnit.SECONDS));
            p.submit(2);
            assertTrue("scheduler must survive processor errors", recovered.await(2, TimeUnit.SECONDS));
        } finally { p.close(); }
    }
}
