package com.example.datapipeline.internal;

import static org.junit.Assert.*;
import org.junit.Test;

public class ConflatingIntakeTest {

    @Test public void mergesPendingWithConflator() {
        ConflatingIntake<Integer> in = new ConflatingIntake<Integer>((a, b) -> a + b);
        in.offer(1); in.offer(2); in.offer(3);
        assertEquals(Integer.valueOf(6), in.poll());
        assertNull(in.poll());
    }

    @Test public void firstItemStoredUnmerged() {
        ConflatingIntake<Integer> in = new ConflatingIntake<Integer>((a, b) -> { throw new AssertionError(); });
        in.offer(7);
        assertEquals(Integer.valueOf(7), in.poll());
    }

    @Test public void conflatorThrowingKeepsNewestAndDoesNotPropagate() {
        ConflatingIntake<Integer> in = new ConflatingIntake<Integer>((a, b) -> { throw new RuntimeException("boom"); });
        in.offer(1);
        in.offer(2); // conflator fails; keep newest, swallow
        assertEquals(Integer.valueOf(2), in.poll());
    }
}
