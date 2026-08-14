# DataPipeline

A bounded processing pipeline for high-frequency data in Swing/OSGi applications. DataPipeline accepts data from an external source at high frequency, processes it on bounded worker threads, and delivers results to the Swing EDT with selectable overflow, concurrency, and update policies. Thread count is fixed at construction, guaranteeing no unbounded thread creation. The library is Java 8, pure OSGi DS, and has zero runtime dependencies.

## Quick Start

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

## Overflow Policies

Choose how the pipeline handles a backlog when processing cannot keep up with ingestion.

### PROCESS_ALL

Bounded buffered queue with configurable capacity (default 1024). When full, the oldest item is dropped and `onOverflow(droppedItem)` is called (default: logged). Favors fresh data; good for scenarios where you prefer to lose old samples and always process the newest ones.

- Optional: `bufferCapacity(capacity)` to set the queue size (default 1024).
- Optional: `onOverflow(dropped -> ...)` callback for dropped items.

### LATEST_WINS

Single-slot overflow policy. New data overwrites any unread slot. At most one item waits for processing. When processing completes, the freshest item is queued next. Good for real-time data where you only care about the current state.

- Requires: nothing additional.
- Optional: useful with `ExecutionMode.SEQUENTIAL` for minimal latency.

### CONFLATE

Single-slot overflow policy with custom merge logic. If a new item arrives before the current slot is read, both are passed to a `conflator` function that returns a combined result. Deterministic merge of concurrent updates. Good for scenarios where partial updates can be combined (e.g., accumulated scores, merged geo bounds).

- Requires: `conflator((older, newer) -> mergedResult)` must be supplied.
- IllegalStateException thrown at build time if omitted.

## Execution Modes

Choose how many threads process items, and in what order results are released.

### SEQUENTIAL

One dedicated worker thread reads from intake, processes each item, and passes the result to UI delivery. Results are released in submission order. Simplest mode, minimal thread overhead, but throughput limited to one item at a time.

```java
.executionMode(ExecutionMode.SEQUENTIAL)
```

### PARALLEL_ORDERED

A dispatcher thread assigns each item a monotonically increasing sequence number and submits it to a fixed pool of N worker threads. A **Resequencer** buffers completed results and releases them strictly in arrival order, even if a later item finishes before an earlier one. All N worker threads race to process items concurrently; order is guaranteed on output.

```java
.executionMode(ExecutionMode.parallelOrdered(nThreads))
```

Good for CPU-bound processing where you need throughput and can spare N threads but must maintain input order for the UI.

## UI Delivery Modes

Choose when and how results are pushed to the Swing EDT.

### Immediate

Each released result triggers an immediate push to the EDT via `SwingUtilities.invokeLater()`. A single pending slot coalesces updates: if a runnable is already scheduled but not yet running, the pending value is overwritten instead of queuing a second runnable. The EDT never accumulates a backlog of pipeline runnables.

```java
.uiUpdateMode(UiUpdateMode.immediate())
```

Good for low-latency, latency-sensitive UIs where you want to see results as soon as they're ready.

### Periodic

A shared single-thread scheduler ticks at a fixed interval (e.g., every 100 ms). Each tick reads the latest released result and pushes it to the EDT via the same coalescing mechanism. If no new result arrived since the last tick, nothing is pushed. This decouples processing rate from UI update rate.

```java
.uiUpdateMode(UiUpdateMode.periodic(100))  // milliseconds
```

Good for high-frequency data where you want to throttle UI updates to a fixed frame rate. Note: `close()` stops the scheduler first, so a result that completes processing during close() may never reach a periodic tick and go undelivered.

### Process-On-Tick

Only meaningful with `UiUpdateMode.periodic(...)`. When enabled, the periodic tick *pulls* from intake, processes the item on the worker thread (never the EDT), and pushes the result. No processing occurs between ticks. Effectively inverts stages 2 and 3: UI updates synchronously drive processing.

```java
.uiUpdateMode(UiUpdateMode.periodic(100))
.processOnlyOnTick(true)
```

Good for UI-driven processing where you want exactly one computation per frame, always on the freshest data (especially with `LATEST_WINS`).

## Degenerate Configurations and Warnings

The builder logs a warning and adjusts behavior automatically in the following cases:

### PARALLEL_ORDERED + LATEST_WINS or CONFLATE

When you combine parallel execution with an overflow policy that holds at most one pending item, parallelism cannot help. At most one item is ever awaiting processing, so multiple worker threads are idle. The builder logs a warning and runs effectively as SEQUENTIAL (one item, one thread, parallelism unused):

    WARNING: PARALLEL_ORDERED with LATEST_WINS/CONFLATE cannot parallelize (at most one pending item); running sequentially

### processOnlyOnTick(true) with PERIODIC mode and PARALLEL_ORDERED

When `processOnlyOnTick(true)` is enabled, the periodic tick processes one item per cycle. Combined with `PARALLEL_ORDERED`, parallelism cannot help. The builder logs a warning and runs effectively sequential:

    WARNING: PARALLEL_ORDERED with processOnlyOnTick cannot parallelize; running sequentially

### processOnlyOnTick(true) without PERIODIC mode

If `processOnlyOnTick(true)` is set without `UiUpdateMode.periodic(...)`, the builder throws `IllegalStateException` at build time. There is no "tick" to drive processing:

    java.lang.IllegalStateException: processOnlyOnTick requires UiUpdateMode.periodic(...)

## Thread Inventory

The pipeline spawns a fixed, bounded set of daemon threads, all named with the `datapipeline-` prefix. The exact thread count depends on the configuration:

**Worker and Dispatcher Threads:**
- **SEQUENTIAL mode**: 1 worker thread (`datapipeline-worker-0`)
- **PARALLEL_ORDERED(N) mode**: N worker threads (`datapipeline-worker-0` through `datapipeline-worker-{N-1}`) plus 1 dispatcher thread (`datapipeline-dispatcher`)
- **processOnlyOnTick(true) mode**: No separate worker or dispatcher; processing runs on the scheduler thread

**Scheduler Thread:**
- Created only when `UiUpdateMode.periodic(...)` or `processOnlyOnTick(true)` is used
- `datapipeline-scheduler`: runs periodic ticks and/or drives processing in tick-pull mode
- Not created when using `UiUpdateMode.immediate()`

**Total Thread Count Examples:**
- SEQUENTIAL + immediate: 1 worker
- SEQUENTIAL + periodic: 1 worker + 1 scheduler = 2
- PARALLEL_ORDERED(4) + immediate: 4 workers + 1 dispatcher = 5
- PARALLEL_ORDERED(4) + periodic: 4 workers + 1 dispatcher + 1 scheduler = 6
- processOnlyOnTick(true): 1 scheduler only

All threads are daemon threads and terminate when the pipeline is closed. `close()` allows in-flight work a bounded ~2s window to finish, then interrupts remaining threads. On component deactivation, `PipelineFactory` closes all pipelines it created, guaranteeing no thread leaks across bundle restarts.

## Building

### Requirements

- **Maven 3.6+** with **JDK 11–17** (JDK 21 is not supported; Tycho 2.7.5 fails on JavaSE-21)
- The library bundle targets **Java 8** (OSGI `Bundle-RequiredExecutionEnvironment: JavaSE-1.8`); you can run the compiled bundle on Java 8+, but Maven must run on JDK 11–17

### Setup

Set `JAVA_HOME` to JDK 11–17:

```bash
export JAVA_HOME=$(/usr/libexec/java_home -v 17)
```

(Adjust the version number if you prefer JDK 11–16. On Linux, use your system's Java path.)

### Build

```bash
mvn verify
```

This compiles the bundle, runs unit tests, and generates the OSGi manifest and DS descriptor. Output: `com.example.datapipeline/target/com.example.datapipeline-1.0.0.jar`.

### Running Tests

Tests run automatically during `mvn verify`. To run only tests without building:

```bash
mvn test
```

## Demo

An interactive OSGi-launched Swing demo lives in `com.example.datapipeline.demo` (a DS bundle that receives `PipelineFactory` and opens a dashboard) plus `com.example.datapipeline.demo.launcher` (embeds Apache Felix + SCR and installs the bundles). A fake source emits ticks at an adjustable rate (100–10,000/s) through an adjustable-delay processor; the window shows submitted/processed/dropped/UI-updates per second while you switch overflow policy, execution mode, and UI delivery mode live.

```bash
mvn verify
java -jar com.example.datapipeline.demo.launcher/target/com.example.datapipeline.demo.launcher-1.0.0-SNAPSHOT-jar-with-dependencies.jar
```

Pass `-Ddemo.autostart=true` to start the tick source immediately. Closing the window stops the OSGi framework, which deactivates the demo component and closes its pipeline. The Felix/SCR jars are demo-only dependencies — the library itself still has none.

## License

Refer to the license file in this repository.
