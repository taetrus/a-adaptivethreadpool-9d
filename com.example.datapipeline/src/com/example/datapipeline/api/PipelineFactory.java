package com.example.datapipeline.api;

/**
 * OSGi service for creating pipelines. Pipelines built through this factory are
 * closed automatically when the datapipeline bundle deactivates — no leaked
 * threads across bundle restarts.
 */
public interface PipelineFactory {
    <T, R> DataPipeline<T, R> build(DataPipeline.Builder<T, R> builder);
}
