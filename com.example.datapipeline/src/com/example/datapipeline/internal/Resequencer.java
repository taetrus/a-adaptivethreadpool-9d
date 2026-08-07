package com.example.datapipeline.internal;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.PriorityQueue;

/** Releases parallel results in submission order; failed items release their slot via skip(). */
final class Resequencer<R> {

    private static final class Entry<R> {
        final long seq; final R result; final boolean skipped;
        Entry(long seq, R result, boolean skipped) {
            this.seq = seq; this.result = result; this.skipped = skipped;
        }
    }

    private final PriorityQueue<Entry<R>> pending =
            new PriorityQueue<Entry<R>>(11, Comparator.comparingLong(e -> e.seq));
    private long nextExpected;

    synchronized List<R> accept(long seq, R result) { return add(new Entry<R>(seq, result, false)); }
    synchronized List<R> skip(long seq) { return add(new Entry<R>(seq, null, true)); }

    private List<R> add(Entry<R> e) {
        pending.add(e);
        List<R> out = new ArrayList<R>();
        while (!pending.isEmpty() && pending.peek().seq == nextExpected) {
            Entry<R> head = pending.poll();
            nextExpected++;
            if (!head.skipped) out.add(head.result);
        }
        return out;
    }
}
