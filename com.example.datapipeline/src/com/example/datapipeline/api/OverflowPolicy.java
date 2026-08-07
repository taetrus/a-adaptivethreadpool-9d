package com.example.datapipeline.api;

/** What happens to backlog when data arrives faster than it is processed. */
public enum OverflowPolicy {
    /** Every item is processed; a bounded buffer holds the backlog (drop-oldest on overflow). */
    PROCESS_ALL,
    /** Only the newest unprocessed item is kept; older unread items are discarded. */
    LATEST_WINS,
    /** Pending items are merged with a user-supplied conflator function. */
    CONFLATE
}
