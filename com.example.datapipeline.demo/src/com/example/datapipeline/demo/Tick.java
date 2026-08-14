package com.example.datapipeline.demo;

/** One emission from the fake source. {@code merged} counts how many ticks were conflated into this one. */
final class Tick {
    final long id;
    final int merged;

    Tick(long id, int merged) {
        this.id = id;
        this.merged = merged;
    }

    static Tick conflate(Tick older, Tick newer) {
        return new Tick(newer.id, older.merged + newer.merged);
    }
}
