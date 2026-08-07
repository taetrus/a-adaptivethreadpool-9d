package com.example.datapipeline.api;

import static org.junit.Assert.*;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.junit.Test;

public class SequentialPipelineTest {

    /** Runs UI runnables synchronously on the calling (worker) thread — fine for assertions. */
    private static final java.util.concurrent.Executor DIRECT = Runnable::run;

    @Test public void processesAndDeliversInOrder() throws Exception {
        List<String> seen = Collections.synchronizedList(new ArrayList<String>());
        CountDownLatch done = new CountDownLatch(3);
        DataPipeline<Integer, String> p = DataPipeline.<Integer, String>builder()
                .processor(i -> "r" + i)
                .uiConsumer(s -> { seen.add(s); done.countDown(); })
                .overflowPolicy(OverflowPolicy.PROCESS_ALL)
                .uiThreadExecutor(DIRECT)
                .build();
        try {
            p.submit(1); p.submit(2); p.submit(3);
            assertTrue(done.await(2, TimeUnit.SECONDS));
            assertEquals(java.util.Arrays.asList("r1", "r2", "r3"), seen);
        } finally { p.close(); }
    }

    @Test public void latestWinsSkipsStaleItemsUnderLoad() throws Exception {
        CountDownLatch firstStarted = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        List<Integer> processed = Collections.synchronizedList(new ArrayList<Integer>());
        CountDownLatch done = new CountDownLatch(2);
        DataPipeline<Integer, Integer> p = DataPipeline.<Integer, Integer>builder()
                .processor(i -> {
                    processed.add(i);
                    if (i == 1) {
                        firstStarted.countDown();
                        try { release.await(2, TimeUnit.SECONDS); } catch (InterruptedException ignored) {}
                    }
                    return i;
                })
                .uiConsumer(i -> done.countDown())
                .overflowPolicy(OverflowPolicy.LATEST_WINS)
                .uiThreadExecutor(DIRECT)
                .build();
        try {
            p.submit(1);
            assertTrue(firstStarted.await(2, TimeUnit.SECONDS));
            p.submit(2); p.submit(3); p.submit(4); // arrive while 1 is processing
            release.countDown();
            assertTrue(done.await(2, TimeUnit.SECONDS));
            assertEquals(java.util.Arrays.asList(1, 4), processed); // 2 and 3 were overwritten
        } finally { p.close(); }
    }

    @Test public void processorExceptionRoutedToOnErrorAndPipelineSurvives() throws Exception {
        List<Object> failedItems = Collections.synchronizedList(new ArrayList<Object>());
        CountDownLatch ok = new CountDownLatch(1);
        DataPipeline<Integer, Integer> p = DataPipeline.<Integer, Integer>builder()
                .processor(i -> { if (i == 13) throw new IllegalStateException("boom"); return i; })
                .uiConsumer(i -> ok.countDown())
                .overflowPolicy(OverflowPolicy.PROCESS_ALL)
                .onError((t, item) -> failedItems.add(item))
                .uiThreadExecutor(DIRECT)
                .build();
        try {
            p.submit(13);
            p.submit(2);
            assertTrue(ok.await(2, TimeUnit.SECONDS)); // pipeline still alive after error
            assertEquals(java.util.Arrays.asList((Object) 13), failedItems);
        } finally { p.close(); }
    }

    @Test public void nullProcessorResultRoutedToOnError() throws Exception {
        CountDownLatch errored = new CountDownLatch(1);
        DataPipeline<Integer, Integer> p = DataPipeline.<Integer, Integer>builder()
                .processor(i -> null)
                .uiConsumer(i -> fail("null result must not reach UI"))
                .overflowPolicy(OverflowPolicy.LATEST_WINS)
                .onError((t, item) -> errored.countDown())
                .uiThreadExecutor(DIRECT)
                .build();
        try {
            p.submit(1);
            assertTrue(errored.await(2, TimeUnit.SECONDS));
        } finally { p.close(); }
    }
}
