# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Build & Test

Tycho 2.7.5 requires the Maven JVM to be JDK 11–17 (it fails on JDK 21+), while the bundle itself targets Java 8 (`Bundle-RequiredExecutionEnvironment: JavaSE-1.8`). On this machine:

```bash
export JAVA_HOME=$(/usr/libexec/java_home -v 17)
mvn -q verify                                        # full build: bundle jar + all tests
mvn -q -pl com.example.datapipeline.tests -am test   # tests only
mvn -q -pl com.example.datapipeline.tests -am test -Dtest=ResequencerTest -DfailIfNoTests=false   # single test class
```

`-DfailIfNoTests=false` is required for single-test runs because the reactor includes the bundle module, which has no surefire tests.

No Java 9+ APIs anywhere in the bundle. Zero third-party runtime dependencies — JUnit 4.13.2 is test-scope only. Some tests are timing-based (PeriodicUiTest, LifecycleTest); a lone failure there is worth one re-run before investigating.

## Architecture

`DataPipeline<T,R>` is a bounded, policy-driven pipeline: high-frequency data in, expensive processing on pipeline-owned threads, results delivered to the Swing EDT. Design spec: `docs/superpowers/specs/2026-08-07-datapipeline-design.md` (authoritative for semantics).

Three stages, wired by `internal/PipelineImpl` from the `DataPipeline.Builder` config:

1. **Intake** (`internal/Intake` + three impls) — the overflow policy lives here as a queue strategy: bounded FIFO with atomic drop-oldest + `onOverflow` (`ProcessAllIntake`), single overwriting slot (`LatestWinsIntake`), or slot merged via user conflator (`ConflatingIntake`). `submit()` only calls `intake.offer()` — it never blocks and never creates threads.
2. **Processing** — one worker thread (SEQUENTIAL), or dispatcher + fixed pool + `Resequencer` (PARALLEL_ORDERED). Degenerate combos (parallel + LATEST_WINS/CONFLATE, parallel + processOnlyOnTick) degrade to sequential with a logged warning at build time — never an error.
3. **UI delivery** — `CoalescingPublisher` guarantees at most one pending runnable on the EDT (newest result wins if the EDT is behind); periodic mode ticks a scheduler that drains a `latestResult` slot; tick-pull mode (`processOnlyOnTick`) inverts the flow — the scheduler thread itself pulls from intake and processes, so no worker/dispatcher exists at all.

### Invariants that are easy to break

- **`CoalescingPublisher` assumes one producer at a time.** In parallel mode this holds only because release+emit happens inside `synchronized (resequencer)` in `startParallel()` — processing stays outside the lock, emission stays inside. Moving `emit()` out of that block reintroduces a proven data-loss race.
- **The dispatcher→pool handoff is bounded by a `Semaphore(threadCount*2)`.** Without it the pool's unbounded queue absorbs all backlog and PROCESS_ALL's drop-oldest policy never engages. Acquire is after `intake.take()`, release in the pool task's `finally`.
- **`Resequencer` expects each sequence number exactly once**, contiguous from 0; failed items must call `skip(seq)` or the stream stalls forever.
- **`close()` uses one shared 2s deadline** across scheduler/pool/dispatcher/worker waits — don't reintroduce per-step budgets. The sequential worker's `workerBusy` flag makes close join-before-interrupt only when an item is mid-flight, keeping idle close fast.
- **Processor results must be non-null** (null is the internal empty sentinel); `PipelineImpl` routes null results to `onError`, and `publish(null)` throws. Callback exceptions (`onError`, `onOverflow`, conflator, UI consumer) are always swallowed/routed — a user callback must never kill a pipeline thread.
- **Thread names are API-ish**: `datapipeline-worker-<i>` (from 0), `datapipeline-dispatcher`, `datapipeline-scheduler` (exact, no suffix). LifecycleTest sweeps for the `datapipeline-` prefix; README documents the inventory per mode.

### OSGi packaging

Manifest-first (checked-in `META-INF/MANIFEST.MF`): only `com.example.datapipeline.api` is exported; `internal` is bundle-private. DS is a hand-written `OSGI-INF/component.xml` (deliberately no annotation processing) publishing `PipelineFactory`; its `deactivate()` closes every pipeline it built. If you add API packages or rename the component class, both the manifest and component.xml must be updated by hand — nothing generates them.

### Demo modules

`com.example.datapipeline.demo` (manifest-first bundle, DS component consuming `PipelineFactory`) + `com.example.datapipeline.demo.launcher` (plain jar embedding Felix + SCR; run the `*-jar-with-dependencies.jar`, `-Ddemo.autostart=true` to start emitting immediately). Felix/SCR are demo-only deps. OSGi gotcha encoded in the demo manifest: `javax.*` packages get no boot delegation — every used `javax.swing.*` subpackage must be in `Import-Package` explicitly.

### Test module layout

`com.example.datapipeline.tests` is a plain-jar Maven module (not an OSGi fragment, no OSGi runtime needed). It can test package-private internals because test classes sit in the same package names (`com.example.datapipeline.internal`) on the plain classpath. Tests inject a synchronous executor via `Builder.uiThreadExecutor(Runnable::run)` instead of a real EDT — keep new tests headless the same way.
