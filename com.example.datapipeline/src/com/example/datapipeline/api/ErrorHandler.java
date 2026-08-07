package com.example.datapipeline.api;

/**
 * Called when the processor, the UI consumer, or the pipeline itself fails on an item.
 * {@code item} is the input for processing errors and the result for UI-delivery errors.
 */
@FunctionalInterface
public interface ErrorHandler {
    void onError(Throwable error, Object item);
}
