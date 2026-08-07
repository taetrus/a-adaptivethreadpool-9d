package com.example.datapipeline.internal;

import com.example.datapipeline.api.DataPipeline;

public final class PipelineImpl<T, R> implements DataPipeline<T, R> {

    public PipelineImpl(DataPipeline.Builder<T, R> b) {
        // wiring added in Task 6
    }

    @Override public boolean submit(T item) { return false; }
    @Override public void close() {}
}
