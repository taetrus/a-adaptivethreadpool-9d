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
        DataPipeline<String, String> p =
                valid().executionMode(ExecutionMode.parallelOrdered(4)).build();
        assertNotNull(p);
        p.close();
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
