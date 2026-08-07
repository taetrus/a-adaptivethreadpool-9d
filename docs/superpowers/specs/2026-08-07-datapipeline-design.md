# DataPipeline — Bounded processing pipeline for high-frequency data in Swing/OSGi apps

**Date:** 2026-08-07
**Status:** Approved design

## Problem

An external source emits data at high frequency. Each item must be processed (expensive computation) and the result shown in a Swing UI. Naively firing a thread per item causes unbounded thread creation and performance collapse. The consumer must be able to choose what happens to the backlog, how processing executes, and how often the UI updates.

## Goals

- Reusable OSGi DS bundle, **Java 8**, Maven **Tycho manifest-first** build, zero third-party runtime dependencies.
- Single ordered data source per pipeline instance.
- Non-blocking ingestion: `submit()` never blocks the data-source thread and never spawns threads.
- Thread count bounded by construction.
- Selectable overflow policy, execution mode, and UI delivery mode (all combinations meaningful per the compatibility rules below).

## Non-goals

- Multiple independent sources per pipeline (create one pipeline per source instead).
- Ultra-low-latency (sub-millisecond) delivery; Disruptor-class throughput.
- Java 9+ APIs, RxJava, or any reactive-streams dependency.

## Public API

Bundle `com.example.datapipeline`, exported package `com.example.datapipeline.api`, implementation in private package `com.example.datapipeline.internal`.

```java
DataPipeline<T,R> pipeline = DataPipeline.<T,R>builder()
    .processor(t -> expensiveComputation(t))        // T -> R, worker thread(s)
    .uiConsumer(r -> label.setText(r.toString()))   // runs on EDT
    .overflowPolicy(OverflowPolicy.LATEST_WINS)     // PROCESS_ALL | LATEST_WINS | CONFLATE
    .conflator((older, newer) -> merge(older, newer)) // required iff CONFLATE
    .bufferCapacity(1024)                           // PROCESS_ALL only
    .executionMode(ExecutionMode.SEQUENTIAL)        // or ExecutionMode.parallelOrdered(nThreads)
    .uiUpdateMode(UiUpdateMode.periodic(100))       // or UiUpdateMode.immediate()
    .processOnlyOnTick(false)                       // true = no processing between UI ticks
    .onError((throwable, item) -> ...)              // optional
    .onOverflow(dropped -> ...)                     // optional, PROCESS_ALL only
    .build();

pipeline.submit(data);   // non-blocking; returns false after close()
pipeline.close();        // AutoCloseable, idempotent
```

A DS component `PipelineFactory` (service interface in the api package) wraps the builder so consumer bundles obtain pipelines via service injection. The core is plain Java 8, unit-testable without an OSGi runtime.

### Configuration compatibility rules

- `CONFLATE` requires a `conflator`; builder throws `IllegalStateException` otherwise.
- `bufferCapacity`/`onOverflow` are only meaningful with `PROCESS_ALL`; setting them otherwise throws at build time.
- `PARALLEL_ORDERED` + (`LATEST_WINS` or `CONFLATE`): at most one item is ever pending, so parallelism cannot help. The builder logs a warning and runs effectively sequential. Not an error — configs may be user-supplied at runtime.
- `processOnlyOnTick(true)` requires `UiUpdateMode.periodic(...)`; builder throws otherwise.
- `processOnlyOnTick(true)` + `PARALLEL_ORDERED`: the tick processes one item at a time, so parallelism cannot help; logged warning, effectively sequential (same treatment as the rule above).

## Architecture — three stages

```
source thread          worker thread(s)              EDT
    │  submit()             │                          │
    ▼                       ▼                          ▼
 [Intake] ──take──▶ [Processing (+Resequencer)] ──▶ [UI delivery]
 (policy)            (SEQUENTIAL | PARALLEL_ORDERED)  (immediate | periodic)
```

### Stage 1 — Intake (overflow policy)

Interface `Intake<T>` with `offer(T)` (non-blocking, called by source) and `take()` (blocking, called by worker/dispatcher). Three implementations:

- **ProcessAllIntake** — `ArrayBlockingQueue<T>(bufferCapacity)`. On full queue: drop **oldest**, invoke `onOverflow(dropped)` (default: log). Rationale: favor fresh data, consistent with the library's purpose.
- **LatestWinsIntake** — single slot (`AtomicReference<T>` + signalling). New data overwrites an unread slot.
- **ConflatingIntake** — single slot under a lock: `slot = (slot == null) ? newer : conflator.apply(slot, newer)`.

### Stage 2 — Processing

- **SEQUENTIAL** — one dedicated worker thread: `take → process → hand off` loop.
- **PARALLEL_ORDERED(n)** — a dispatcher thread stamps each item with a monotonically increasing sequence number and submits it to a fixed pool of `n` threads. A **Resequencer** (`PriorityQueue` keyed by sequence + `nextExpected` counter, under a lock) buffers completed results and releases them strictly in arrival order. A failed item releases its sequence slot (no result emitted) so the resequencer never stalls.

### Stage 3 — UI delivery

- **immediate()** — each released result is coalesced through a single pending slot + `SwingUtilities.invokeLater`: if a previously scheduled runnable has not run yet, the pending value is swapped instead of queuing another runnable. The EDT can never accumulate a backlog of pipeline runnables.
- **periodic(ms)** — a shared single-thread `ScheduledExecutorService` tick reads the latest released result (one slot, overwritten between ticks) and pushes it to the EDT via the same coalescing mechanism. Nothing is pushed if no new result arrived since the last tick.
- **processOnlyOnTick(true)** — inverts stages 2/3: the periodic tick *pulls* from intake, runs the processor on the worker (never the EDT), then paints. No processing occurs between ticks. With `LATEST_WINS` this yields exactly one computation per UI frame, always on the freshest data.

### Thread inventory (worst case)

1 scheduler + N workers (N = 1 for SEQUENTIAL) + 1 dispatcher (PARALLEL_ORDERED only). All daemon, named `datapipeline-worker-<i>`, `datapipeline-dispatcher`, `datapipeline-scheduler`.

## Error handling

- Processor exception → caught on worker, routed to `onError(throwable, item)` (default: log). Pipeline continues.
- UI-consumer exception on EDT → same `onError` route. Pipeline continues.
- `PROCESS_ALL` overflow → `onOverflow(droppedOldest)`, default log-and-drop.
- `submit()` after `close()` → returns `false`, no exception (sources race with shutdown).
- Callbacks (`onError`, `onOverflow`) throwing → swallowed with a log line; user callbacks must not be able to kill pipeline threads.

## Lifecycle

- `close()`: stop accepting new data, allow in-flight work to finish with a bounded wait (2 s), then interrupt remaining threads. Idempotent.
- `PipelineFactory` DS component tracks every pipeline it created and closes them all on component deactivation — no leaked threads across bundle restarts.

## Testing strategy

Plain JUnit 4, headless, no OSGi runtime:

- Unit tests per `Intake` implementation (offer/take semantics, overwrite, conflation math, overflow drop-oldest).
- `Resequencer` unit tests with out-of-order and failed completions.
- Pipeline integration tests using `CountDownLatch`es and an injectable "EDT" executor (package-private seam) so tests run without a real Swing `EventQueue`.
- One timing-tolerant test per UI mode (`periodic`, `processOnlyOnTick`) with generous margins for CI.

## Bundle/build layout

```
pom.xml                                (Tycho parent)
com.example.datapipeline/              (library bundle: META-INF/MANIFEST.MF, OSGI-INF DS xml generated from annotations)
com.example.datapipeline.demo/         (optional demo Swing app — may be deferred)
```

Manifest-first: `Export-Package: com.example.datapipeline.api`; DS via `org.osgi.service.component.annotations` (compile-time only).
