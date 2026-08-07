package com.example.datapipeline.internal;

import static org.junit.Assert.*;
import java.util.ArrayList;
import java.util.List;
import org.junit.Test;

public class ProcessAllIntakeTest {

    @Test public void fifoOrder() throws Exception {
        ProcessAllIntake<Integer> in = new ProcessAllIntake<Integer>(10, d -> {});
        in.offer(1); in.offer(2); in.offer(3);
        assertEquals(Integer.valueOf(1), in.take());
        assertEquals(Integer.valueOf(2), in.take());
        assertEquals(Integer.valueOf(3), in.take());
    }

    @Test public void overflowDropsOldestAndReportsIt() throws Exception {
        List<Integer> dropped = new ArrayList<Integer>();
        ProcessAllIntake<Integer> in = new ProcessAllIntake<Integer>(2, dropped::add);
        in.offer(1); in.offer(2); in.offer(3);
        assertEquals(java.util.Arrays.asList(1), dropped);
        assertEquals(Integer.valueOf(2), in.take());
        assertEquals(Integer.valueOf(3), in.take());
    }

    @Test public void pollReturnsNullWhenEmpty() {
        assertNull(new ProcessAllIntake<Integer>(2, d -> {}).poll());
    }

    @Test public void overflowCallbackThrowingDoesNotPropagate() {
        ProcessAllIntake<Integer> in =
                new ProcessAllIntake<Integer>(1, d -> { throw new RuntimeException("boom"); });
        in.offer(1);
        in.offer(2); // triggers overflow; must not throw
        assertEquals(Integer.valueOf(2), in.poll());
    }
}
