package com.example.datapipeline.internal;

import static org.junit.Assert.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.Test;

public class LatestWinsIntakeTest {

    @Test public void newerOverwritesUnread() {
        LatestWinsIntake<Integer> in = new LatestWinsIntake<Integer>();
        in.offer(1); in.offer(2); in.offer(3);
        assertEquals(Integer.valueOf(3), in.poll());
        assertNull(in.poll());
    }

    @Test public void takeBlocksUntilOffer() throws Exception {
        final LatestWinsIntake<Integer> in = new LatestWinsIntake<Integer>();
        final AtomicReference<Integer> got = new AtomicReference<Integer>();
        final CountDownLatch done = new CountDownLatch(1);
        Thread t = new Thread(() -> {
            try { got.set(in.take()); done.countDown(); } catch (InterruptedException ignored) {}
        });
        t.start();
        Thread.sleep(50); // let taker block
        in.offer(42);
        assertTrue(done.await(2, TimeUnit.SECONDS));
        assertEquals(Integer.valueOf(42), got.get());
    }
}
