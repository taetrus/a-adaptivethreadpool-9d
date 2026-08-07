package com.example.datapipeline.internal;

import static org.junit.Assert.*;
import org.junit.Test;

import com.example.datapipeline.api.DataPipeline;
import com.example.datapipeline.api.OverflowPolicy;

public class PipelineFactoryComponentTest {

    private static DataPipeline.Builder<Integer, Integer> validBuilder() {
        return DataPipeline.<Integer, Integer>builder()
                .processor(i -> i)
                .uiConsumer(i -> {})
                .overflowPolicy(OverflowPolicy.LATEST_WINS)
                .uiThreadExecutor(Runnable::run);
    }

    @Test public void buildReturnsWorkingPipeline() {
        PipelineFactoryComponent factory = new PipelineFactoryComponent();
        DataPipeline<Integer, Integer> p = factory.build(validBuilder());
        assertTrue(p.submit(1));
        factory.deactivate();
    }

    @Test public void deactivateClosesAllCreatedPipelines() {
        PipelineFactoryComponent factory = new PipelineFactoryComponent();
        DataPipeline<Integer, Integer> p1 = factory.build(validBuilder());
        DataPipeline<Integer, Integer> p2 = factory.build(validBuilder());
        factory.deactivate();
        assertFalse(p1.submit(1));
        assertFalse(p2.submit(1));
    }

    @Test public void deactivateIsIdempotent() {
        PipelineFactoryComponent factory = new PipelineFactoryComponent();
        factory.build(validBuilder());
        factory.deactivate();
        factory.deactivate(); // must not throw
    }
}
