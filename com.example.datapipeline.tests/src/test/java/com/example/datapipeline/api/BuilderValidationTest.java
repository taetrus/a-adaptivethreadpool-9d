package com.example.datapipeline.api;

import static org.junit.Assert.*;
import org.junit.Test;

public class BuilderValidationTest {

    private DataPipeline.Builder<String, String> valid() {
        return DataPipeline.<String, String>builder()
                .processor(s -> s)
                .uiConsumer(s -> {})
                .overflowPolicy(OverflowPolicy.LATEST_WINS);
    }

    @Test public void buildsWithMinimalValidConfig() {
        DataPipeline<String, String> p = valid().build();
        assertNotNull(p);
        p.close();
    }

    @Test(expected = IllegalStateException.class)
    public void processorIsRequired() {
        DataPipeline.<String, String>builder()
                .uiConsumer(s -> {}).overflowPolicy(OverflowPolicy.LATEST_WINS).build();
    }

    @Test(expected = IllegalStateException.class)
    public void uiConsumerIsRequired() {
        DataPipeline.<String, String>builder()
                .processor(s -> s).overflowPolicy(OverflowPolicy.LATEST_WINS).build();
    }

    @Test(expected = IllegalStateException.class)
    public void overflowPolicyIsRequired() {
        DataPipeline.<String, String>builder()
                .processor(s -> s).uiConsumer(s -> {}).build();
    }

    @Test(expected = IllegalStateException.class)
    public void conflateRequiresConflator() {
        valid().overflowPolicy(OverflowPolicy.CONFLATE).build();
    }

    @Test(expected = IllegalStateException.class)
    public void conflatorForbiddenWithoutConflate() {
        valid().conflator((a, b) -> b).build();
    }

    @Test(expected = IllegalStateException.class)
    public void bufferCapacityOnlyWithProcessAll() {
        valid().bufferCapacity(64).build();
    }

    @Test(expected = IllegalStateException.class)
    public void onOverflowOnlyWithProcessAll() {
        valid().onOverflow(t -> {}).build();
    }

    @Test(expected = IllegalStateException.class)
    public void processOnlyOnTickRequiresPeriodic() {
        valid().processOnlyOnTick(true).build();
    }

    @Test public void parallelWithLatestWinsDegradesToSequentialWithoutError() {
        DataPipeline.Builder<String, String> b =
                valid().executionMode(ExecutionMode.parallelOrdered(4));
        DataPipeline<String, String> p = b.build();
        try {
            assertNotNull(p);
            // The builder must not corrupt the (reusable) Builder's own executionMode field.
            assertTrue("Builder.executionMode must remain unmutated after build()",
                    b.getExecutionMode().isParallel());
            // The pipeline itself must actually degrade to sequential: no dispatcher thread.
            for (Thread t : allThreads()) {
                assertFalse("no dispatcher thread should exist for a degraded-to-sequential pipeline",
                        t.getName().equals("datapipeline-dispatcher") && t.isAlive());
            }
        } finally {
            p.close();
        }
    }

    private static Thread[] allThreads() {
        Thread[] threads = new Thread[Thread.activeCount() * 2];
        int n = Thread.enumerate(threads);
        return java.util.Arrays.copyOf(threads, n);
    }

    @Test(expected = IllegalArgumentException.class)
    public void parallelOrderedRejectsSingleThread() {
        ExecutionMode.parallelOrdered(1);
    }

    @Test(expected = IllegalArgumentException.class)
    public void periodicRejectsZeroPeriod() {
        UiUpdateMode.periodic(0);
    }
}
