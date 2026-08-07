package com.example.datapipeline.internal;

import static org.junit.Assert.*;
import java.util.Arrays;
import org.junit.Test;

public class ResequencerTest {

    @Test public void inOrderResultsReleaseImmediately() {
        Resequencer<String> rs = new Resequencer<String>();
        assertEquals(Arrays.asList("a"), rs.accept(0, "a"));
        assertEquals(Arrays.asList("b"), rs.accept(1, "b"));
    }

    @Test public void outOfOrderResultsAreHeldThenReleasedTogether() {
        Resequencer<String> rs = new Resequencer<String>();
        assertTrue(rs.accept(1, "b").isEmpty());
        assertTrue(rs.accept(2, "c").isEmpty());
        assertEquals(Arrays.asList("a", "b", "c"), rs.accept(0, "a"));
    }

    @Test public void skippedSequenceDoesNotStallOrEmit() {
        Resequencer<String> rs = new Resequencer<String>();
        assertTrue(rs.accept(1, "b").isEmpty());
        assertEquals(Arrays.asList("b"), rs.skip(0));
    }

    @Test public void skipInMiddleReleasesSurroundingResults() {
        Resequencer<String> rs = new Resequencer<String>();
        assertEquals(Arrays.asList("a"), rs.accept(0, "a"));
        assertTrue(rs.accept(2, "c").isEmpty());
        assertEquals(Arrays.asList("c"), rs.skip(1));
    }
}
