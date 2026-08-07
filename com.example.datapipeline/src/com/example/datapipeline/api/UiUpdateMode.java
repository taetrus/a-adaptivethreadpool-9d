package com.example.datapipeline.api;

/** When results reach the UI: as they complete, or on a fixed period using the newest result. */
public final class UiUpdateMode {
    private static final UiUpdateMode IMMEDIATE = new UiUpdateMode(0);

    private final long periodMillis;

    private UiUpdateMode(long periodMillis) { this.periodMillis = periodMillis; }

    public static UiUpdateMode immediate() { return IMMEDIATE; }

    public static UiUpdateMode periodic(long millis) {
        if (millis <= 0) throw new IllegalArgumentException("period must be > 0, got " + millis);
        return new UiUpdateMode(millis);
    }

    public boolean isPeriodic() { return periodMillis > 0; }
    public long periodMillis() { return periodMillis; }
}
