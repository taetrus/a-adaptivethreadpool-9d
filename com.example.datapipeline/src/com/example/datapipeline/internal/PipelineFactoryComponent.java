package com.example.datapipeline.internal;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import com.example.datapipeline.api.DataPipeline;
import com.example.datapipeline.api.PipelineFactory;

/** DS component (declared in OSGI-INF/component.xml). */
public final class PipelineFactoryComponent implements PipelineFactory {

    private final List<DataPipeline<?, ?>> pipelines = new CopyOnWriteArrayList<DataPipeline<?, ?>>();

    @Override public <T, R> DataPipeline<T, R> build(DataPipeline.Builder<T, R> builder) {
        DataPipeline<T, R> p = builder.build();
        pipelines.add(p);
        return p;
    }

    /** DS deactivate method — closes every pipeline this factory created. */
    public void deactivate() {
        for (DataPipeline<?, ?> p : pipelines) {
            try { p.close(); } catch (Throwable ignored) {}
        }
        pipelines.clear();
    }
}
