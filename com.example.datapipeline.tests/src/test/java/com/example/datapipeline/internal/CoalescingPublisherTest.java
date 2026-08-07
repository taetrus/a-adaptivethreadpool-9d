package com.example.datapipeline.internal;

import static org.junit.Assert.*;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executor;
import org.junit.Test;

public class CoalescingPublisherTest {

    /** Queues runnables; runs them only when told — simulates a busy EDT. */
    private static final class ManualExecutor implements Executor {
        final List<Runnable> queued = new ArrayList<Runnable>();
        @Override public void execute(Runnable r) { queued.add(r); }
        void runAll() { List<Runnable> copy = new ArrayList<Runnable>(queued); queued.clear();
                        for (Runnable r : copy) r.run(); }
    }

    @Test public void deliversResultToConsumer() {
        ManualExecutor edt = new ManualExecutor();
        List<String> seen = new ArrayList<String>();
        CoalescingPublisher<String> p = new CoalescingPublisher<String>(edt, seen::add, (t, i) -> {});
        p.publish("a");
        edt.runAll();
        assertEquals(java.util.Arrays.asList("a"), seen);
    }

    @Test public void coalescesWhenEdtIsBehind() {
        ManualExecutor edt = new ManualExecutor();
        List<String> seen = new ArrayList<String>();
        CoalescingPublisher<String> p = new CoalescingPublisher<String>(edt, seen::add, (t, i) -> {});
        p.publish("a"); p.publish("b"); p.publish("c");
        assertEquals(1, edt.queued.size()); // never more than one pending runnable
        edt.runAll();
        assertEquals(java.util.Arrays.asList("c"), seen); // newest wins
    }

    @Test public void consumerExceptionRoutedToErrorHandlerAndSwallowed() {
        ManualExecutor edt = new ManualExecutor();
        List<Object> failed = new ArrayList<Object>();
        CoalescingPublisher<String> p = new CoalescingPublisher<String>(
                edt, s -> { throw new RuntimeException("boom"); }, (t, item) -> failed.add(item));
        p.publish("a");
        edt.runAll(); // must not throw
        assertEquals(java.util.Arrays.asList("a"), failed);
    }

    @Test public void errorHandlerExceptionSwallowed() {
        ManualExecutor edt = new ManualExecutor();
        CoalescingPublisher<String> p = new CoalescingPublisher<String>(
                edt, s -> { throw new RuntimeException("boom"); },
                (t, item) -> { throw new RuntimeException("handler boom"); });
        p.publish("a");
        edt.runAll(); // must not throw
    }
}
