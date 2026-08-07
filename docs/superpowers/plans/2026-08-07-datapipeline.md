# DataPipeline Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** A reusable OSGi bundle providing a bounded, policy-driven processing pipeline (`DataPipeline<T,R>`) that ingests high-frequency data, processes it on a bounded set of threads, and delivers results to the Swing EDT.

**Architecture:** Three stages behind one builder API: an `Intake` (overflow policy: process-all / latest-wins / conflate), a processing stage (sequential worker or fixed pool + resequencer), and a UI delivery stage (coalesced immediate, periodic, or tick-pull). A DS component publishes a `PipelineFactory` service and closes all created pipelines on deactivation.

**Tech Stack:** Java 8, OSGi DS (hand-written component XML), Maven Tycho 2.7.5 manifest-first, JUnit 4.13.2 (plain jar test module, no OSGi runtime).

**Spec:** `docs/superpowers/specs/2026-08-07-datapipeline-design.md`

## Global Constraints

- Java 8 source/target; `Bundle-RequiredExecutionEnvironment: JavaSE-1.8`. No Java 9+ APIs.
- Zero third-party runtime dependencies. JUnit 4.13.2 test-scope only.
- Bundle symbolic name `com.example.datapipeline`; only `com.example.datapipeline.api` is exported; implementation lives in `com.example.datapipeline.internal`.
- `submit()` must never block and never create threads.
- All pipeline threads are daemon threads named `datapipeline-worker-<i>`, `datapipeline-dispatcher`, `datapipeline-scheduler`.
- Processor results must be non-null (`null` is the internal "empty" sentinel); a null result is routed to `onError`.
- Exceptions from user callbacks (`onError`, `onOverflow`) must never kill pipeline threads.
- Tests must run headless without a real Swing `EventQueue` (inject a test `Executor` as the UI executor).
- Tycho itself needs JDK 11+ to *run*; it still compiles the bundle to Java 8 per the BREE. If `mvn` uses JDK 8, upgrade the JDK running Maven, not the BREE.
- Commit after every task with the message given in the task.

## File Structure

```
pom.xml                                          Tycho parent (modules, p2 repo, plugin config)
com.example.datapipeline/
  pom.xml                                        eclipse-plugin packaging
  META-INF/MANIFEST.MF                           manifest-first metadata
  build.properties                               PDE build config
  OSGI-INF/component.xml                         DS declaration for PipelineFactory
  src/com/example/datapipeline/api/
    OverflowPolicy.java                          enum PROCESS_ALL | LATEST_WINS | CONFLATE
    ExecutionMode.java                           SEQUENTIAL / parallelOrdered(n)
    UiUpdateMode.java                            immediate() / periodic(ms)
    ErrorHandler.java                            (Throwable, Object item) callback
    DataPipeline.java                            interface + Builder (validation lives here)
    PipelineFactory.java                         DS service interface
  src/com/example/datapipeline/internal/
    Intake.java                                  offer/take/poll interface
    ProcessAllIntake.java                        bounded queue, drop-oldest + onOverflow
    LatestWinsIntake.java                        one slot, overwrite
    ConflatingIntake.java                        one slot + merge function
    Resequencer.java                             in-order release for parallel mode
    CoalescingPublisher.java                     EDT publisher, never queues twice
    PipelineImpl.java                            wires stages, owns threads, close()
    PipelineFactoryComponent.java                DS component, closes pipelines on deactivate
com.example.datapipeline.tests/
  pom.xml                                        plain jar, junit, depends on bundle
  src/test/java/com/example/datapipeline/internal/   (same package → can see package-private classes)
    ProcessAllIntakeTest.java
    LatestWinsIntakeTest.java
    ConflatingIntakeTest.java
    ResequencerTest.java
    CoalescingPublisherTest.java
    PipelineFactoryComponentTest.java
  src/test/java/com/example/datapipeline/api/
    BuilderValidationTest.java
    SequentialPipelineTest.java
    ParallelOrderedPipelineTest.java
    PeriodicUiTest.java
    LifecycleTest.java
```

---

### Task 1: Build skeleton

**Files:**
- Create: `pom.xml`, `com.example.datapipeline/pom.xml`, `com.example.datapipeline/META-INF/MANIFEST.MF`, `com.example.datapipeline/build.properties`, `com.example.datapipeline.tests/pom.xml`
- Create: `com.example.datapipeline/src/com/example/datapipeline/api/OverflowPolicy.java` (one real class so the bundle compiles)

**Interfaces:**
- Produces: a building Tycho reactor; `OverflowPolicy` enum with constants `PROCESS_ALL`, `LATEST_WINS`, `CONFLATE`.

- [ ] **Step 1: Parent pom**

```xml
<?xml version="1.0" encoding="UTF-8"?>
<project xmlns="http://maven.apache.org/POM/4.0.0">
  <modelVersion>4.0.0</modelVersion>
  <groupId>com.example</groupId>
  <artifactId>datapipeline-parent</artifactId>
  <version>1.0.0-SNAPSHOT</version>
  <packaging>pom</packaging>
  <properties>
    <tycho.version>2.7.5</tycho.version>
    <project.build.sourceEncoding>UTF-8</project.build.sourceEncoding>
  </properties>
  <modules>
    <module>com.example.datapipeline</module>
    <module>com.example.datapipeline.tests</module>
  </modules>
  <repositories>
    <repository>
      <id>photon</id>
      <layout>p2</layout>
      <url>https://download.eclipse.org/releases/photon</url>
    </repository>
  </repositories>
  <build>
    <plugins>
      <plugin>
        <groupId>org.eclipse.tycho</groupId>
        <artifactId>tycho-maven-plugin</artifactId>
        <version>${tycho.version}</version>
        <extensions>true</extensions>
      </plugin>
      <plugin>
        <groupId>org.eclipse.tycho</groupId>
        <artifactId>target-platform-configuration</artifactId>
        <version>${tycho.version}</version>
        <configuration>
          <environments>
            <environment><os>macosx</os><ws>cocoa</ws><arch>x86_64</arch></environment>
          </environments>
        </configuration>
      </plugin>
    </plugins>
  </build>
</project>
```

- [ ] **Step 2: Bundle project**

`com.example.datapipeline/pom.xml`:

```xml
<?xml version="1.0" encoding="UTF-8"?>
<project xmlns="http://maven.apache.org/POM/4.0.0">
  <modelVersion>4.0.0</modelVersion>
  <parent>
    <groupId>com.example</groupId>
    <artifactId>datapipeline-parent</artifactId>
    <version>1.0.0-SNAPSHOT</version>
  </parent>
  <artifactId>com.example.datapipeline</artifactId>
  <packaging>eclipse-plugin</packaging>
</project>
```

`com.example.datapipeline/META-INF/MANIFEST.MF` (note: file must end with a newline; header lines must not exceed 72 bytes):

```
Manifest-Version: 1.0
Bundle-ManifestVersion: 2
Bundle-Name: DataPipeline
Bundle-SymbolicName: com.example.datapipeline
Bundle-Version: 1.0.0.qualifier
Bundle-RequiredExecutionEnvironment: JavaSE-1.8
Export-Package: com.example.datapipeline.api
Import-Package: javax.swing
Service-Component: OSGI-INF/component.xml
```

`com.example.datapipeline/build.properties`:

```
source.. = src/
output.. = bin/
bin.includes = META-INF/,\
               OSGI-INF/,\
               .
```

Create a placeholder `com.example.datapipeline/OSGI-INF/component.xml` now so `bin.includes` resolves (real content in Task 10):

```xml
<?xml version="1.0" encoding="UTF-8"?>
<!-- populated in Task 10 -->
```

Note: an XML file whose root is a comment is invalid XML but harmless here — nothing parses it until `Service-Component` is honored at OSGi runtime, and Task 10 replaces it. If Tycho's manifest validation complains, temporarily remove the `Service-Component` line from the manifest and re-add it in Task 10.

`OverflowPolicy.java`:

```java
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
```

- [ ] **Step 3: Test module**

`com.example.datapipeline.tests/pom.xml`:

```xml
<?xml version="1.0" encoding="UTF-8"?>
<project xmlns="http://maven.apache.org/POM/4.0.0">
  <modelVersion>4.0.0</modelVersion>
  <parent>
    <groupId>com.example</groupId>
    <artifactId>datapipeline-parent</artifactId>
    <version>1.0.0-SNAPSHOT</version>
  </parent>
  <artifactId>com.example.datapipeline.tests</artifactId>
  <packaging>jar</packaging>
  <dependencies>
    <dependency>
      <groupId>com.example</groupId>
      <artifactId>com.example.datapipeline</artifactId>
      <version>1.0.0-SNAPSHOT</version>
    </dependency>
    <dependency>
      <groupId>junit</groupId>
      <artifactId>junit</artifactId>
      <version>4.13.2</version>
      <scope>test</scope>
    </dependency>
  </dependencies>
  <build>
    <plugins>
      <plugin>
        <groupId>org.apache.maven.plugins</groupId>
        <artifactId>maven-compiler-plugin</artifactId>
        <version>3.8.1</version>
        <configuration><source>1.8</source><target>1.8</target></configuration>
      </plugin>
    </plugins>
  </build>
</project>
```

If the reactor complains about the `eclipse-plugin` dependency type, add `<type>eclipse-plugin</type>` to that dependency.

- [ ] **Step 4: Verify the build**

Run: `mvn -q verify`
Expected: BUILD SUCCESS (bundle jar produced, empty test module passes).

- [ ] **Step 5: Commit**

```bash
git add -A && git commit -m "build: Tycho manifest-first skeleton with bundle and test modules"
```

---

### Task 2: API types and builder validation

**Files:**
- Create: `com.example.datapipeline/src/com/example/datapipeline/api/ExecutionMode.java`, `UiUpdateMode.java`, `ErrorHandler.java`, `DataPipeline.java`
- Create: `com.example.datapipeline/src/com/example/datapipeline/internal/PipelineImpl.java` (skeleton only)
- Test: `com.example.datapipeline.tests/src/test/java/com/example/datapipeline/api/BuilderValidationTest.java`

**Interfaces:**
- Produces:
  - `ExecutionMode.SEQUENTIAL`, `ExecutionMode.parallelOrdered(int nThreads)` (throws `IllegalArgumentException` if `nThreads < 2`), `boolean isParallel()`, `int threadCount()`
  - `UiUpdateMode.immediate()`, `UiUpdateMode.periodic(long millis)` (throws if `millis <= 0`), `boolean isPeriodic()`, `long periodMillis()`
  - `ErrorHandler { void onError(Throwable error, Object item); }` — non-generic; `item` is the input `T` for processing errors and the result `R` for UI-consumer errors
  - `DataPipeline<T,R> extends AutoCloseable { boolean submit(T item); void close(); static <T,R> Builder<T,R> builder(); }`
  - `DataPipeline.Builder<T,R>` methods: `processor(Function<T,R>)`, `uiConsumer(Consumer<R>)`, `overflowPolicy(OverflowPolicy)`, `conflator(BinaryOperator<T>)`, `bufferCapacity(int)`, `executionMode(ExecutionMode)`, `uiUpdateMode(UiUpdateMode)`, `processOnlyOnTick(boolean)`, `onError(ErrorHandler)`, `onOverflow(Consumer<T>)`, `uiThreadExecutor(Executor)`, `build()`
  - `PipelineImpl` skeleton constructor: `PipelineImpl(Builder<T,R> b)` (reads builder fields, which are package-visible… see note below)

**Note on cross-package access:** `Builder` (api) exposes its fields to `PipelineImpl` (internal) via public getters that are documented as internal (`/** internal — not API */`). Both packages ship in one bundle and only `api` is exported, so `internal` is invisible to consumers; the getters on Builder are technically visible but harmless (read-only).

- [ ] **Step 1: Write the failing validation tests**

```java
package com.example.datapipeline.api;

import static org.junit.Assert.*;
import org.junit.Test;

public class BuilderValidationTest {

    private DataPipeline.Builder<String, String> valid() {
        return DataPipeline.<String, String>builder()
                .processor(s -> s)
                .uiConsumer(s -> {})
                .overflowPolicy(OverflowPolicy.LATEST_WINS);
    }

    @Test public void buildsWithMinimalValidConfig() {
        DataPipeline<String, String> p = valid().build();
        assertNotNull(p);
        p.close();
    }

    @Test(expected = IllegalStateException.class)
    public void processorIsRequired() {
        DataPipeline.<String, String>builder()
                .uiConsumer(s -> {}).overflowPolicy(OverflowPolicy.LATEST_WINS).build();
    }

    @Test(expected = IllegalStateException.class)
    public void uiConsumerIsRequired() {
        DataPipeline.<String, String>builder()
                .processor(s -> s).overflowPolicy(OverflowPolicy.LATEST_WINS).build();
    }

    @Test(expected = IllegalStateException.class)
    public void overflowPolicyIsRequired() {
        DataPipeline.<String, String>builder()
                .processor(s -> s).uiConsumer(s -> {}).build();
    }

    @Test(expected = IllegalStateException.class)
    public void conflateRequiresConflator() {
        valid().overflowPolicy(OverflowPolicy.CONFLATE).build();
    }

    @Test(expected = IllegalStateException.class)
    public void conflatorForbiddenWithoutConflate() {
        valid().conflator((a, b) -> b).build();
    }

    @Test(expected = IllegalStateException.class)
    public void bufferCapacityOnlyWithProcessAll() {
        valid().bufferCapacity(64).build();
    }

    @Test(expected = IllegalStateException.class)
    public void onOverflowOnlyWithProcessAll() {
        valid().onOverflow(t -> {}).build();
    }

    @Test(expected = IllegalStateException.class)
    public void processOnlyOnTickRequiresPeriodic() {
        valid().processOnlyOnTick(true).build();
    }

    @Test public void parallelWithLatestWinsDegradesToSequentialWithoutError() {
        DataPipeline<String, String> p =
                valid().executionMode(ExecutionMode.parallelOrdered(4)).build();
        assertNotNull(p);
        p.close();
    }

    @Test(expected = IllegalArgumentException.class)
    public void parallelOrderedRejectsSingleThread() {
        ExecutionMode.parallelOrdered(1);
    }

    @Test(expected = IllegalArgumentException.class)
    public void periodicRejectsZeroPeriod() {
        UiUpdateMode.periodic(0);
    }
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `mvn -q -pl com.example.datapipeline.tests -am test`
Expected: COMPILATION ERROR (classes don't exist yet).

- [ ] **Step 3: Implement the API types**

`ExecutionMode.java`:

```java
package com.example.datapipeline.api;

/** How items are processed: one worker in order, or a pool with re-sequenced output. */
public final class ExecutionMode {
    public static final ExecutionMode SEQUENTIAL = new ExecutionMode(1);

    private final int threads;

    private ExecutionMode(int threads) { this.threads = threads; }

    public static ExecutionMode parallelOrdered(int nThreads) {
        if (nThreads < 2) throw new IllegalArgumentException("nThreads must be >= 2, got " + nThreads);
        return new ExecutionMode(nThreads);
    }

    public boolean isParallel() { return threads > 1; }
    public int threadCount() { return threads; }
}
```

`UiUpdateMode.java`:

```java
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
```

`ErrorHandler.java`:

```java
package com.example.datapipeline.api;

/**
 * Called when the processor, the UI consumer, or the pipeline itself fails on an item.
 * {@code item} is the input for processing errors and the result for UI-delivery errors.
 */
@FunctionalInterface
public interface ErrorHandler {
    void onError(Throwable error, Object item);
}
```

`DataPipeline.java`:

```java
package com.example.datapipeline.api;

import java.util.concurrent.Executor;
import java.util.function.BinaryOperator;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.swing.SwingUtilities;

import com.example.datapipeline.internal.PipelineImpl;

/**
 * Bounded processing pipeline: submit data from any thread, process on pipeline
 * threads, deliver results to the Swing EDT. Never blocks the submitting thread.
 */
public interface DataPipeline<T, R> extends AutoCloseable {

    /** Non-blocking. Returns false if the pipeline is closed. */
    boolean submit(T item);

    /** Idempotent orderly shutdown; waits up to 2s for in-flight work. */
    @Override void close();

    static <T, R> Builder<T, R> builder() { return new Builder<T, R>(); }

    final class Builder<T, R> {
        private static final Logger LOG = Logger.getLogger(Builder.class.getName());

        private Function<T, R> processor;
        private Consumer<R> uiConsumer;
        private OverflowPolicy overflowPolicy;
        private BinaryOperator<T> conflator;
        private int bufferCapacity = 1024;
        private boolean bufferCapacitySet;
        private ExecutionMode executionMode = ExecutionMode.SEQUENTIAL;
        private UiUpdateMode uiUpdateMode = UiUpdateMode.immediate();
        private boolean processOnlyOnTick;
        private ErrorHandler errorHandler =
                (t, item) -> LOG.log(Level.WARNING, "Pipeline error on item: " + item, t);
        private Consumer<T> onOverflow;
        private Executor uiThreadExecutor = SwingUtilities::invokeLater;

        Builder() {}

        public Builder<T, R> processor(Function<T, R> p) { this.processor = p; return this; }
        public Builder<T, R> uiConsumer(Consumer<R> c) { this.uiConsumer = c; return this; }
        public Builder<T, R> overflowPolicy(OverflowPolicy p) { this.overflowPolicy = p; return this; }
        public Builder<T, R> conflator(BinaryOperator<T> c) { this.conflator = c; return this; }
        public Builder<T, R> bufferCapacity(int n) { this.bufferCapacity = n; this.bufferCapacitySet = true; return this; }
        public Builder<T, R> executionMode(ExecutionMode m) { this.executionMode = m; return this; }
        public Builder<T, R> uiUpdateMode(UiUpdateMode m) { this.uiUpdateMode = m; return this; }
        public Builder<T, R> processOnlyOnTick(boolean b) { this.processOnlyOnTick = b; return this; }
        public Builder<T, R> onError(ErrorHandler h) { this.errorHandler = h; return this; }
        public Builder<T, R> onOverflow(Consumer<T> c) { this.onOverflow = c; return this; }
        /** Replace the EDT with another executor (tests, JavaFX). Default: SwingUtilities::invokeLater. */
        public Builder<T, R> uiThreadExecutor(Executor e) { this.uiThreadExecutor = e; return this; }

        public DataPipeline<T, R> build() {
            if (processor == null) throw new IllegalStateException("processor is required");
            if (uiConsumer == null) throw new IllegalStateException("uiConsumer is required");
            if (overflowPolicy == null) throw new IllegalStateException("overflowPolicy is required");
            if (overflowPolicy == OverflowPolicy.CONFLATE && conflator == null)
                throw new IllegalStateException("CONFLATE requires a conflator");
            if (overflowPolicy != OverflowPolicy.CONFLATE && conflator != null)
                throw new IllegalStateException("conflator is only valid with CONFLATE");
            if (overflowPolicy != OverflowPolicy.PROCESS_ALL && bufferCapacitySet)
                throw new IllegalStateException("bufferCapacity is only valid with PROCESS_ALL");
            if (overflowPolicy != OverflowPolicy.PROCESS_ALL && onOverflow != null)
                throw new IllegalStateException("onOverflow is only valid with PROCESS_ALL");
            if (processOnlyOnTick && !uiUpdateMode.isPeriodic())
                throw new IllegalStateException("processOnlyOnTick requires UiUpdateMode.periodic(...)");
            if (bufferCapacitySet && bufferCapacity < 1)
                throw new IllegalStateException("bufferCapacity must be >= 1");

            if (executionMode.isParallel() && overflowPolicy != OverflowPolicy.PROCESS_ALL) {
                LOG.warning("PARALLEL_ORDERED with " + overflowPolicy
                        + " cannot parallelize (at most one pending item); running sequentially");
                executionMode = ExecutionMode.SEQUENTIAL;
            }
            if (executionMode.isParallel() && processOnlyOnTick) {
                LOG.warning("PARALLEL_ORDERED with processOnlyOnTick cannot parallelize; running sequentially");
                executionMode = ExecutionMode.SEQUENTIAL;
            }
            return new PipelineImpl<T, R>(this);
        }

        // internal — not API; read by PipelineImpl
        public Function<T, R> getProcessor() { return processor; }
        public Consumer<R> getUiConsumer() { return uiConsumer; }
        public OverflowPolicy getOverflowPolicy() { return overflowPolicy; }
        public BinaryOperator<T> getConflator() { return conflator; }
        public int getBufferCapacity() { return bufferCapacity; }
        public ExecutionMode getExecutionMode() { return executionMode; }
        public UiUpdateMode getUiUpdateMode() { return uiUpdateMode; }
        public boolean isProcessOnlyOnTick() { return processOnlyOnTick; }
        public ErrorHandler getErrorHandler() { return errorHandler; }
        public Consumer<T> getOnOverflow() { return onOverflow; }
        public Executor getUiThreadExecutor() { return uiThreadExecutor; }
    }
}
```

`PipelineImpl.java` skeleton (fleshed out in Tasks 6–9):

```java
package com.example.datapipeline.internal;

import com.example.datapipeline.api.DataPipeline;

public final class PipelineImpl<T, R> implements DataPipeline<T, R> {

    public PipelineImpl(DataPipeline.Builder<T, R> b) {
        // wiring added in Task 6
    }

    @Override public boolean submit(T item) { return false; }
    @Override public void close() {}
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `mvn -q -pl com.example.datapipeline.tests -am test`
Expected: all `BuilderValidationTest` tests PASS.

- [ ] **Step 5: Commit**

```bash
git add -A && git commit -m "feat: API types and builder with configuration validation"
```

---

### Task 3: Intake implementations

**Files:**
- Create: `com.example.datapipeline/src/com/example/datapipeline/internal/Intake.java`, `ProcessAllIntake.java`, `LatestWinsIntake.java`, `ConflatingIntake.java`
- Test: `com.example.datapipeline.tests/src/test/java/com/example/datapipeline/internal/ProcessAllIntakeTest.java`, `LatestWinsIntakeTest.java`, `ConflatingIntakeTest.java`

**Interfaces:**
- Produces:
  - `interface Intake<T> { boolean offer(T item); T take() throws InterruptedException; T poll(); }` — `offer` non-blocking (source thread); `take` blocks until data (worker); `poll` returns null if empty (tick-pull mode)
  - `ProcessAllIntake(int capacity, Consumer<T> onOverflow)` — `onOverflow` never null (caller passes a no-op/logging default)
  - `LatestWinsIntake()` / `ConflatingIntake(BinaryOperator<T> conflator)`

- [ ] **Step 1: Write the failing tests**

`ProcessAllIntakeTest.java`:

```java
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
```

`LatestWinsIntakeTest.java`:

```java
package com.example.datapipeline.internal;

import static org.junit.Assert.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.Test;

public class LatestWinsIntakeTest {

    @Test public void newerOverwritesUnread() {
        LatestWinsIntake<Integer> in = new LatestWinsIntake<Integer>();
        in.offer(1); in.offer(2); in.offer(3);
        assertEquals(Integer.valueOf(3), in.poll());
        assertNull(in.poll());
    }

    @Test public void takeBlocksUntilOffer() throws Exception {
        final LatestWinsIntake<Integer> in = new LatestWinsIntake<Integer>();
        final AtomicReference<Integer> got = new AtomicReference<Integer>();
        final CountDownLatch done = new CountDownLatch(1);
        Thread t = new Thread(() -> {
            try { got.set(in.take()); done.countDown(); } catch (InterruptedException ignored) {}
        });
        t.start();
        Thread.sleep(50); // let taker block
        in.offer(42);
        assertTrue(done.await(2, TimeUnit.SECONDS));
        assertEquals(Integer.valueOf(42), got.get());
    }
}
```

`ConflatingIntakeTest.java`:

```java
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
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `mvn -q -pl com.example.datapipeline.tests -am test`
Expected: COMPILATION ERROR (`Intake` classes missing).

- [ ] **Step 3: Implement**

`Intake.java`:

```java
package com.example.datapipeline.internal;

/** Stage 1: policy-bearing buffer between the data source and the processing stage. */
interface Intake<T> {
    /** Non-blocking; called from the source thread. Always accepts (policy decides what gives way). */
    boolean offer(T item);
    /** Blocks until an item is available; called by the worker/dispatcher. */
    T take() throws InterruptedException;
    /** Non-blocking; null if empty. Used by tick-pull mode. */
    T poll();
}
```

`ProcessAllIntake.java`:

```java
package com.example.datapipeline.internal;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.function.Consumer;

final class ProcessAllIntake<T> implements Intake<T> {
    private final ArrayBlockingQueue<T> queue;
    private final Consumer<T> onOverflow;

    ProcessAllIntake(int capacity, Consumer<T> onOverflow) {
        this.queue = new ArrayBlockingQueue<T>(capacity);
        this.onOverflow = onOverflow;
    }

    @Override public boolean offer(T item) {
        while (!queue.offer(item)) {
            T dropped = queue.poll();
            if (dropped != null) {
                try { onOverflow.accept(dropped); } catch (Throwable ignored) {}
            }
        }
        return true;
    }

    @Override public T take() throws InterruptedException { return queue.take(); }
    @Override public T poll() { return queue.poll(); }
}
```

`LatestWinsIntake.java`:

```java
package com.example.datapipeline.internal;

final class LatestWinsIntake<T> implements Intake<T> {
    private final Object lock = new Object();
    private T slot;

    @Override public boolean offer(T item) {
        synchronized (lock) {
            slot = item;
            lock.notifyAll();
        }
        return true;
    }

    @Override public T take() throws InterruptedException {
        synchronized (lock) {
            while (slot == null) lock.wait();
            T t = slot;
            slot = null;
            return t;
        }
    }

    @Override public T poll() {
        synchronized (lock) {
            T t = slot;
            slot = null;
            return t;
        }
    }
}
```

`ConflatingIntake.java`:

```java
package com.example.datapipeline.internal;

import java.util.function.BinaryOperator;

final class ConflatingIntake<T> implements Intake<T> {
    private final Object lock = new Object();
    private final BinaryOperator<T> conflator;
    private T slot;

    ConflatingIntake(BinaryOperator<T> conflator) { this.conflator = conflator; }

    @Override public boolean offer(T item) {
        synchronized (lock) {
            if (slot == null) {
                slot = item;
            } else {
                try { slot = conflator.apply(slot, item); }
                catch (Throwable t) { slot = item; } // keep newest on conflator failure
            }
            lock.notifyAll();
        }
        return true;
    }

    @Override public T take() throws InterruptedException {
        synchronized (lock) {
            while (slot == null) lock.wait();
            T t = slot;
            slot = null;
            return t;
        }
    }

    @Override public T poll() {
        synchronized (lock) {
            T t = slot;
            slot = null;
            return t;
        }
    }
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `mvn -q -pl com.example.datapipeline.tests -am test`
Expected: all intake tests PASS.

- [ ] **Step 5: Commit**

```bash
git add -A && git commit -m "feat: intake implementations for all three overflow policies"
```

---

### Task 4: Resequencer

**Files:**
- Create: `com.example.datapipeline/src/com/example/datapipeline/internal/Resequencer.java`
- Test: `com.example.datapipeline.tests/src/test/java/com/example/datapipeline/internal/ResequencerTest.java`

**Interfaces:**
- Produces: `Resequencer<R>` with `List<R> accept(long seq, R result)` and `List<R> skip(long seq)`. Sequences start at 0 and are contiguous. Both methods return every result now releasable in order (possibly empty). `skip` marks a failed item so its slot doesn't stall the stream.

- [ ] **Step 1: Write the failing tests**

```java
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
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `mvn -q -pl com.example.datapipeline.tests -am test`
Expected: COMPILATION ERROR (`Resequencer` missing).

- [ ] **Step 3: Implement**

```java
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
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `mvn -q -pl com.example.datapipeline.tests -am test`
Expected: all `ResequencerTest` tests PASS.

- [ ] **Step 5: Commit**

```bash
git add -A && git commit -m "feat: resequencer for in-order release of parallel results"
```

---

### Task 5: CoalescingPublisher

**Files:**
- Create: `com.example.datapipeline/src/com/example/datapipeline/internal/CoalescingPublisher.java`
- Test: `com.example.datapipeline.tests/src/test/java/com/example/datapipeline/internal/CoalescingPublisherTest.java`

**Interfaces:**
- Produces: `CoalescingPublisher<R>` with constructor `(Executor uiExecutor, Consumer<R> consumer, ErrorHandler errorHandler)` and `void publish(R result)`. Guarantee: at most one runnable pending on the UI executor at any time; a newer result replaces an undelivered older one.

- [ ] **Step 1: Write the failing tests**

```java
package com.example.datapipeline.internal;

import static org.junit.Assert.*;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executor;
import org.junit.Test;

public class CoalescingPublisherTest {

    /** Queues runnables; runs them only when told — simulates a busy EDT. */
    private static final class ManualExecutor implements Executor {
        final List<Runnable> queued = new ArrayList<Runnable>();
        @Override public void execute(Runnable r) { queued.add(r); }
        void runAll() { List<Runnable> copy = new ArrayList<Runnable>(queued); queued.clear();
                        for (Runnable r : copy) r.run(); }
    }

    @Test public void deliversResultToConsumer() {
        ManualExecutor edt = new ManualExecutor();
        List<String> seen = new ArrayList<String>();
        CoalescingPublisher<String> p = new CoalescingPublisher<String>(edt, seen::add, (t, i) -> {});
        p.publish("a");
        edt.runAll();
        assertEquals(java.util.Arrays.asList("a"), seen);
    }

    @Test public void coalescesWhenEdtIsBehind() {
        ManualExecutor edt = new ManualExecutor();
        List<String> seen = new ArrayList<String>();
        CoalescingPublisher<String> p = new CoalescingPublisher<String>(edt, seen::add, (t, i) -> {});
        p.publish("a"); p.publish("b"); p.publish("c");
        assertEquals(1, edt.queued.size()); // never more than one pending runnable
        edt.runAll();
        assertEquals(java.util.Arrays.asList("c"), seen); // newest wins
    }

    @Test public void consumerExceptionRoutedToErrorHandlerAndSwallowed() {
        ManualExecutor edt = new ManualExecutor();
        List<Object> failed = new ArrayList<Object>();
        CoalescingPublisher<String> p = new CoalescingPublisher<String>(
                edt, s -> { throw new RuntimeException("boom"); }, (t, item) -> failed.add(item));
        p.publish("a");
        edt.runAll(); // must not throw
        assertEquals(java.util.Arrays.asList("a"), failed);
    }

    @Test public void errorHandlerExceptionSwallowed() {
        ManualExecutor edt = new ManualExecutor();
        CoalescingPublisher<String> p = new CoalescingPublisher<String>(
                edt, s -> { throw new RuntimeException("boom"); },
                (t, item) -> { throw new RuntimeException("handler boom"); });
        p.publish("a");
        edt.runAll(); // must not throw
    }
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `mvn -q -pl com.example.datapipeline.tests -am test`
Expected: COMPILATION ERROR (`CoalescingPublisher` missing).

- [ ] **Step 3: Implement**

```java
package com.example.datapipeline.internal;

import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

import com.example.datapipeline.api.ErrorHandler;

/**
 * Publishes results to the UI executor with at most one pending runnable:
 * if the EDT hasn't run the previous one yet, the pending value is swapped instead.
 */
final class CoalescingPublisher<R> {
    private final Executor uiExecutor;
    private final Consumer<R> consumer;
    private final ErrorHandler errorHandler;
    private final AtomicReference<R> pending = new AtomicReference<R>();

    CoalescingPublisher(Executor uiExecutor, Consumer<R> consumer, ErrorHandler errorHandler) {
        this.uiExecutor = uiExecutor;
        this.consumer = consumer;
        this.errorHandler = errorHandler;
    }

    void publish(R result) {
        if (pending.getAndSet(result) == null) {
            uiExecutor.execute(this::drain);
        }
    }

    private void drain() {
        R r = pending.getAndSet(null);
        if (r == null) return;
        try {
            consumer.accept(r);
        } catch (Throwable t) {
            try { errorHandler.onError(t, r); } catch (Throwable ignored) {}
        }
    }
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `mvn -q -pl com.example.datapipeline.tests -am test`
Expected: all `CoalescingPublisherTest` tests PASS.

- [ ] **Step 5: Commit**

```bash
git add -A && git commit -m "feat: coalescing UI publisher — EDT can never build a backlog"
```

---

### Task 6: PipelineImpl — sequential execution, immediate UI delivery

**Files:**
- Modify: `com.example.datapipeline/src/com/example/datapipeline/internal/PipelineImpl.java` (replace the Task 2 skeleton entirely)
- Test: `com.example.datapipeline.tests/src/test/java/com/example/datapipeline/api/SequentialPipelineTest.java`

**Interfaces:**
- Consumes: `Intake` implementations (Task 3), `CoalescingPublisher` (Task 5), builder getters (Task 2).
- Produces: a working `SEQUENTIAL` + `immediate()` pipeline. Full `PipelineImpl` including `close()` and the periodic/parallel fields as stubs wired in Tasks 7–8 (the code below already contains the structure; Tasks 7–8 fill the marked methods).

- [ ] **Step 1: Write the failing tests**

```java
package com.example.datapipeline.api;

import static org.junit.Assert.*;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.junit.Test;

public class SequentialPipelineTest {

    /** Runs UI runnables synchronously on the calling (worker) thread — fine for assertions. */
    private static final java.util.concurrent.Executor DIRECT = Runnable::run;

    @Test public void processesAndDeliversInOrder() throws Exception {
        List<String> seen = Collections.synchronizedList(new ArrayList<String>());
        CountDownLatch done = new CountDownLatch(3);
        DataPipeline<Integer, String> p = DataPipeline.<Integer, String>builder()
                .processor(i -> "r" + i)
                .uiConsumer(s -> { seen.add(s); done.countDown(); })
                .overflowPolicy(OverflowPolicy.PROCESS_ALL)
                .uiThreadExecutor(DIRECT)
                .build();
        try {
            p.submit(1); p.submit(2); p.submit(3);
            assertTrue(done.await(2, TimeUnit.SECONDS));
            assertEquals(java.util.Arrays.asList("r1", "r2", "r3"), seen);
        } finally { p.close(); }
    }

    @Test public void latestWinsSkipsStaleItemsUnderLoad() throws Exception {
        CountDownLatch firstStarted = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        List<Integer> processed = Collections.synchronizedList(new ArrayList<Integer>());
        CountDownLatch done = new CountDownLatch(2);
        DataPipeline<Integer, Integer> p = DataPipeline.<Integer, Integer>builder()
                .processor(i -> {
                    processed.add(i);
                    if (i == 1) {
                        firstStarted.countDown();
                        try { release.await(2, TimeUnit.SECONDS); } catch (InterruptedException ignored) {}
                    }
                    return i;
                })
                .uiConsumer(i -> done.countDown())
                .overflowPolicy(OverflowPolicy.LATEST_WINS)
                .uiThreadExecutor(DIRECT)
                .build();
        try {
            p.submit(1);
            assertTrue(firstStarted.await(2, TimeUnit.SECONDS));
            p.submit(2); p.submit(3); p.submit(4); // arrive while 1 is processing
            release.countDown();
            assertTrue(done.await(2, TimeUnit.SECONDS));
            assertEquals(java.util.Arrays.asList(1, 4), processed); // 2 and 3 were overwritten
        } finally { p.close(); }
    }

    @Test public void processorExceptionRoutedToOnErrorAndPipelineSurvives() throws Exception {
        List<Object> failedItems = Collections.synchronizedList(new ArrayList<Object>());
        CountDownLatch ok = new CountDownLatch(1);
        DataPipeline<Integer, Integer> p = DataPipeline.<Integer, Integer>builder()
                .processor(i -> { if (i == 13) throw new IllegalStateException("boom"); return i; })
                .uiConsumer(i -> ok.countDown())
                .overflowPolicy(OverflowPolicy.PROCESS_ALL)
                .onError((t, item) -> failedItems.add(item))
                .uiThreadExecutor(DIRECT)
                .build();
        try {
            p.submit(13);
            p.submit(2);
            assertTrue(ok.await(2, TimeUnit.SECONDS)); // pipeline still alive after error
            assertEquals(java.util.Arrays.asList((Object) 13), failedItems);
        } finally { p.close(); }
    }

    @Test public void nullProcessorResultRoutedToOnError() throws Exception {
        CountDownLatch errored = new CountDownLatch(1);
        DataPipeline<Integer, Integer> p = DataPipeline.<Integer, Integer>builder()
                .processor(i -> null)
                .uiConsumer(i -> fail("null result must not reach UI"))
                .overflowPolicy(OverflowPolicy.LATEST_WINS)
                .onError((t, item) -> errored.countDown())
                .uiThreadExecutor(DIRECT)
                .build();
        try {
            p.submit(1);
            assertTrue(errored.await(2, TimeUnit.SECONDS));
        } finally { p.close(); }
    }
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `mvn -q -pl com.example.datapipeline.tests -am test`
Expected: tests FAIL/time out (skeleton `submit()` returns false and does nothing).

- [ ] **Step 3: Implement PipelineImpl (full structure; parallel/periodic bodies filled in Tasks 7–8)**

```java
package com.example.datapipeline.internal;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.example.datapipeline.api.DataPipeline;
import com.example.datapipeline.api.ErrorHandler;
import com.example.datapipeline.api.ExecutionMode;
import com.example.datapipeline.api.UiUpdateMode;

public final class PipelineImpl<T, R> implements DataPipeline<T, R> {
    private static final Logger LOG = Logger.getLogger(PipelineImpl.class.getName());
    private static final long CLOSE_WAIT_MS = 2000;

    private final Function<T, R> processor;
    private final ErrorHandler errorHandler;
    private final Intake<T> intake;
    private final CoalescingPublisher<R> publisher;
    private final UiUpdateMode uiMode;
    private final boolean processOnlyOnTick;
    private final ExecutionMode execMode;

    private final AtomicReference<R> latestResult = new AtomicReference<R>(); // periodic mode
    private volatile boolean closed;

    private Thread worker;                    // SEQUENTIAL
    private Thread dispatcher;                // PARALLEL_ORDERED (Task 7)
    private ExecutorService pool;             // PARALLEL_ORDERED (Task 7)
    private ScheduledExecutorService scheduler; // periodic modes (Task 8)

    public PipelineImpl(DataPipeline.Builder<T, R> b) {
        this.processor = b.getProcessor();
        this.errorHandler = b.getErrorHandler();
        this.uiMode = b.getUiUpdateMode();
        this.processOnlyOnTick = b.isProcessOnlyOnTick();
        this.execMode = b.getExecutionMode();
        this.publisher = new CoalescingPublisher<R>(
                b.getUiThreadExecutor(), b.getUiConsumer(), b.getErrorHandler());
        this.intake = createIntake(b);
        start();
    }

    private Intake<T> createIntake(DataPipeline.Builder<T, R> b) {
        switch (b.getOverflowPolicy()) {
            case PROCESS_ALL:
                Consumer<T> onOverflow = b.getOnOverflow() != null ? b.getOnOverflow()
                        : item -> LOG.warning("Buffer full, dropped oldest item: " + item);
                return new ProcessAllIntake<T>(b.getBufferCapacity(), onOverflow);
            case CONFLATE:
                return new ConflatingIntake<T>(b.getConflator());
            case LATEST_WINS:
            default:
                return new LatestWinsIntake<T>();
        }
    }

    private void start() {
        if (processOnlyOnTick) {
            startTickPull();      // Task 8
            return;
        }
        if (execMode.isParallel()) {
            startParallel();      // Task 7
        } else {
            worker = newDaemon("datapipeline-worker-0", this::sequentialLoop);
            worker.start();
        }
        if (uiMode.isPeriodic()) {
            startPeriodicUi();    // Task 8
        }
    }

    private void sequentialLoop() {
        while (!closed) {
            T item;
            try { item = intake.take(); } catch (InterruptedException e) { return; }
            processAndEmit(item);
        }
    }

    /** Runs the processor and hands the result to the UI stage. Never throws. */
    private void processAndEmit(T item) {
        R result;
        try {
            result = processor.apply(item);
        } catch (Throwable t) {
            safeError(t, item);
            return;
        }
        if (result == null) {
            safeError(new NullPointerException("processor returned null"), item);
            return;
        }
        emit(result);
    }

    void emit(R result) {
        if (uiMode.isPeriodic()) {
            latestResult.set(result);   // periodic tick picks it up (Task 8)
        } else {
            publisher.publish(result);
        }
    }

    void safeError(Throwable t, Object item) {
        try { errorHandler.onError(t, item); }
        catch (Throwable h) { LOG.log(Level.WARNING, "onError handler threw", h); }
    }

    // ----- Task 7 fills these -----
    private void startParallel() { throw new UnsupportedOperationException("Task 7"); }
    // ----- Task 8 fills these -----
    private void startTickPull() { throw new UnsupportedOperationException("Task 8"); }
    private void startPeriodicUi() { throw new UnsupportedOperationException("Task 8"); }

    @Override public boolean submit(T item) {
        if (closed) return false;
        intake.offer(item);
        return true;
    }

    @Override public void close() {
        if (closed) return;
        closed = true;
        if (scheduler != null) shutdown(scheduler);
        if (pool != null) shutdown(pool);
        if (dispatcher != null) dispatcher.interrupt();
        if (worker != null) worker.interrupt();
        join(dispatcher);
        join(worker);
    }

    private static void shutdown(ExecutorService es) {
        es.shutdown();
        try {
            if (!es.awaitTermination(CLOSE_WAIT_MS, TimeUnit.MILLISECONDS)) es.shutdownNow();
        } catch (InterruptedException e) {
            es.shutdownNow();
            Thread.currentThread().interrupt();
        }
    }

    private static void join(Thread t) {
        if (t == null) return;
        try { t.join(CLOSE_WAIT_MS); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }
    }

    static Thread newDaemon(String name, Runnable r) {
        Thread t = new Thread(r, name);
        t.setDaemon(true);
        return t;
    }

    /** ThreadFactory for pools; names threads datapipeline-worker-1..n. */
    static ThreadFactory daemonFactory(String prefix, int startIndex) {
        final AtomicInteger idx = new AtomicInteger(startIndex);
        return r -> {
            Thread t = new Thread(r, prefix + idx.getAndIncrement());
            t.setDaemon(true);
            return t;
        };
    }
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `mvn -q -pl com.example.datapipeline.tests -am test`
Expected: all `SequentialPipelineTest` tests PASS (and all earlier tests still pass — `BuilderValidationTest.buildsWithMinimalValidConfig` now exercises the real pipeline).

- [ ] **Step 5: Commit**

```bash
git add -A && git commit -m "feat: sequential pipeline with immediate coalesced UI delivery"
```

---

### Task 7: Parallel-ordered execution

**Files:**
- Modify: `com.example.datapipeline/src/com/example/datapipeline/internal/PipelineImpl.java` (replace `startParallel()` stub)
- Test: `com.example.datapipeline.tests/src/test/java/com/example/datapipeline/api/ParallelOrderedPipelineTest.java`

**Interfaces:**
- Consumes: `Resequencer` (Task 4), `PipelineImpl` structure (Task 6: `processAndEmit` is *not* reused here — the parallel path inlines processing so failures can call `Resequencer.skip`).

- [ ] **Step 1: Write the failing tests**

```java
package com.example.datapipeline.api;

import static org.junit.Assert.*;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.junit.Test;

public class ParallelOrderedPipelineTest {

    private static final java.util.concurrent.Executor DIRECT = Runnable::run;

    @Test public void resultsArriveInSubmissionOrderDespiteUnevenProcessingTimes() throws Exception {
        final int N = 20;
        List<Integer> seen = Collections.synchronizedList(new ArrayList<Integer>());
        CountDownLatch done = new CountDownLatch(N);
        DataPipeline<Integer, Integer> p = DataPipeline.<Integer, Integer>builder()
                .processor(i -> {
                    // early items sleep longer → finish out of order
                    try { Thread.sleep((N - i) % 7 * 10); } catch (InterruptedException ignored) {}
                    return i;
                })
                .uiConsumer(i -> { seen.add(i); done.countDown(); })
                .overflowPolicy(OverflowPolicy.PROCESS_ALL)
                .executionMode(ExecutionMode.parallelOrdered(4))
                .uiThreadExecutor(DIRECT)
                .build();
        try {
            for (int i = 0; i < N; i++) p.submit(i);
            assertTrue(done.await(10, TimeUnit.SECONDS));
            List<Integer> expected = new ArrayList<Integer>();
            for (int i = 0; i < N; i++) expected.add(i);
            assertEquals(expected, seen);
        } finally { p.close(); }
    }

    @Test public void failedItemDoesNotStallSubsequentResults() throws Exception {
        List<Integer> seen = Collections.synchronizedList(new ArrayList<Integer>());
        CountDownLatch done = new CountDownLatch(2);
        DataPipeline<Integer, Integer> p = DataPipeline.<Integer, Integer>builder()
                .processor(i -> { if (i == 1) throw new RuntimeException("boom"); return i; })
                .uiConsumer(i -> { seen.add(i); done.countDown(); })
                .overflowPolicy(OverflowPolicy.PROCESS_ALL)
                .executionMode(ExecutionMode.parallelOrdered(2))
                .uiThreadExecutor(DIRECT)
                .build();
        try {
            p.submit(0); p.submit(1); p.submit(2);
            assertTrue(done.await(5, TimeUnit.SECONDS));
            assertEquals(java.util.Arrays.asList(0, 2), seen);
        } finally { p.close(); }
    }
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `mvn -q -pl com.example.datapipeline.tests -am test`
Expected: `ParallelOrderedPipelineTest` FAILS with `UnsupportedOperationException: Task 7`.

- [ ] **Step 3: Implement `startParallel()`** (replace the stub; add fields as shown)

```java
    private void startParallel() {
        final Resequencer<R> resequencer = new Resequencer<R>();
        pool = Executors.newFixedThreadPool(execMode.threadCount(),
                daemonFactory("datapipeline-worker-", 0));
        dispatcher = newDaemon("datapipeline-dispatcher", () -> {
            long seq = 0;
            while (!closed) {
                final T item;
                try { item = intake.take(); } catch (InterruptedException e) { return; }
                final long mySeq = seq++;
                try {
                    pool.execute(() -> {
                        R result = null;
                        Throwable failure = null;
                        try {
                            result = processor.apply(item);
                            if (result == null) failure = new NullPointerException("processor returned null");
                        } catch (Throwable t) {
                            failure = t;
                        }
                        java.util.List<R> releasable;
                        if (failure != null) {
                            safeError(failure, item);
                            releasable = resequencer.skip(mySeq);
                        } else {
                            releasable = resequencer.accept(mySeq, result);
                        }
                        for (R r : releasable) emit(r);
                    });
                } catch (java.util.concurrent.RejectedExecutionException e) {
                    return; // pool shut down during close()
                }
            }
        });
        dispatcher.start();
    }
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `mvn -q -pl com.example.datapipeline.tests -am test`
Expected: all tests PASS.

- [ ] **Step 5: Commit**

```bash
git add -A && git commit -m "feat: parallel-ordered execution with resequenced delivery"
```

---

### Task 8: Periodic UI delivery and tick-pull mode

**Files:**
- Modify: `com.example.datapipeline/src/com/example/datapipeline/internal/PipelineImpl.java` (replace `startPeriodicUi()` and `startTickPull()` stubs)
- Test: `com.example.datapipeline.tests/src/test/java/com/example/datapipeline/api/PeriodicUiTest.java`

**Interfaces:**
- Consumes: `latestResult` slot and `emit()` from Task 6 (periodic branch already stores into `latestResult`).

- [ ] **Step 1: Write the failing tests** (timing-tolerant: generous periods, count ranges not exact counts)

```java
package com.example.datapipeline.api;

import static org.junit.Assert.*;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.Test;

public class PeriodicUiTest {

    private static final java.util.concurrent.Executor DIRECT = Runnable::run;

    @Test public void periodicDeliversNewestResultPerTick() throws Exception {
        List<Integer> seen = Collections.synchronizedList(new ArrayList<Integer>());
        CountDownLatch first = new CountDownLatch(1);
        DataPipeline<Integer, Integer> p = DataPipeline.<Integer, Integer>builder()
                .processor(i -> i)
                .uiConsumer(i -> { seen.add(i); first.countDown(); })
                .overflowPolicy(OverflowPolicy.PROCESS_ALL)
                .uiUpdateMode(UiUpdateMode.periodic(100))
                .uiThreadExecutor(DIRECT)
                .build();
        try {
            for (int i = 1; i <= 50; i++) p.submit(i); // all processed fast, before first tick
            assertTrue(first.await(2, TimeUnit.SECONDS));
            Thread.sleep(250); // a couple more ticks with no new data
            int updates = seen.size();
            assertTrue("expected few coalesced updates, got " + updates, updates <= 3);
            assertEquals(Integer.valueOf(50), seen.get(seen.size() - 1)); // newest won
        } finally { p.close(); }
    }

    @Test public void periodicTickWithNoNewDataPushesNothing() throws Exception {
        AtomicInteger updates = new AtomicInteger();
        CountDownLatch first = new CountDownLatch(1);
        DataPipeline<Integer, Integer> p = DataPipeline.<Integer, Integer>builder()
                .processor(i -> i)
                .uiConsumer(i -> { updates.incrementAndGet(); first.countDown(); })
                .overflowPolicy(OverflowPolicy.LATEST_WINS)
                .uiUpdateMode(UiUpdateMode.periodic(50))
                .uiThreadExecutor(DIRECT)
                .build();
        try {
            p.submit(1);
            assertTrue(first.await(2, TimeUnit.SECONDS));
            Thread.sleep(300); // ~6 empty ticks
            assertEquals(1, updates.get());
        } finally { p.close(); }
    }

    @Test public void tickPullProcessesOnlyOncePerTickOnFreshestData() throws Exception {
        AtomicInteger processed = new AtomicInteger();
        List<Integer> seen = Collections.synchronizedList(new ArrayList<Integer>());
        CountDownLatch first = new CountDownLatch(1);
        DataPipeline<Integer, Integer> p = DataPipeline.<Integer, Integer>builder()
                .processor(i -> { processed.incrementAndGet(); return i; })
                .uiConsumer(i -> { seen.add(i); first.countDown(); })
                .overflowPolicy(OverflowPolicy.LATEST_WINS)
                .uiUpdateMode(UiUpdateMode.periodic(100))
                .processOnlyOnTick(true)
                .uiThreadExecutor(DIRECT)
                .build();
        try {
            for (int i = 1; i <= 100; i++) p.submit(i); // burst before first tick
            assertTrue(first.await(2, TimeUnit.SECONDS));
            assertTrue("processed " + processed.get() + " times, expected 1 or 2", processed.get() <= 2);
            assertEquals(Integer.valueOf(100), seen.get(0)); // freshest data was processed
        } finally { p.close(); }
    }

    @Test public void tickPullProcessorErrorDoesNotKillScheduler() throws Exception {
        CountDownLatch errored = new CountDownLatch(1);
        CountDownLatch recovered = new CountDownLatch(1);
        DataPipeline<Integer, Integer> p = DataPipeline.<Integer, Integer>builder()
                .processor(i -> { if (i == 1) throw new RuntimeException("boom"); return i; })
                .uiConsumer(i -> recovered.countDown())
                .overflowPolicy(OverflowPolicy.LATEST_WINS)
                .uiUpdateMode(UiUpdateMode.periodic(50))
                .processOnlyOnTick(true)
                .onError((t, item) -> errored.countDown())
                .uiThreadExecutor(DIRECT)
                .build();
        try {
            p.submit(1);
            assertTrue(errored.await(2, TimeUnit.SECONDS));
            p.submit(2);
            assertTrue("scheduler must survive processor errors", recovered.await(2, TimeUnit.SECONDS));
        } finally { p.close(); }
    }
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `mvn -q -pl com.example.datapipeline.tests -am test`
Expected: `PeriodicUiTest` FAILS with `UnsupportedOperationException: Task 8`.

- [ ] **Step 3: Implement the two stubs** (replace both; a `Runnable` given to `scheduleAtFixedRate` must never throw, or the scheduler silently cancels it — both bodies are fully guarded)

```java
    private void startPeriodicUi() {
        scheduler = Executors.newSingleThreadScheduledExecutor(
                daemonFactory("datapipeline-scheduler", 0));
        scheduler.scheduleAtFixedRate(() -> {
            try {
                R r = latestResult.getAndSet(null);
                if (r != null) publisher.publish(r);
            } catch (Throwable t) {
                safeError(t, null);
            }
        }, uiMode.periodMillis(), uiMode.periodMillis(), TimeUnit.MILLISECONDS);
    }

    private void startTickPull() {
        scheduler = Executors.newSingleThreadScheduledExecutor(
                daemonFactory("datapipeline-scheduler", 0));
        scheduler.scheduleAtFixedRate(() -> {
            try {
                T item = intake.poll();
                if (item == null) return;
                R result;
                try {
                    result = processor.apply(item);
                } catch (Throwable t) {
                    safeError(t, item);
                    return;
                }
                if (result == null) {
                    safeError(new NullPointerException("processor returned null"), item);
                    return;
                }
                publisher.publish(result);
            } catch (Throwable t) {
                safeError(t, null);
            }
        }, uiMode.periodMillis(), uiMode.periodMillis(), TimeUnit.MILLISECONDS);
    }
```

Note: in tick-pull mode `emit()` is bypassed on purpose — the tick itself is the pacing, so the result goes straight to the publisher instead of the `latestResult` slot.

- [ ] **Step 4: Run tests to verify they pass**

Run: `mvn -q -pl com.example.datapipeline.tests -am test`
Expected: all tests PASS. Rerun once more to shake out timing flakiness: `mvn -q -pl com.example.datapipeline.tests -am test`.

- [ ] **Step 5: Commit**

```bash
git add -A && git commit -m "feat: periodic UI delivery and process-only-on-tick mode"
```

---

### Task 9: Lifecycle — close semantics

**Files:**
- Test: `com.example.datapipeline.tests/src/test/java/com/example/datapipeline/api/LifecycleTest.java`
- Modify: `com.example.datapipeline/src/com/example/datapipeline/internal/PipelineImpl.java` (only if a test exposes a gap; `close()` was written in Task 6)

**Interfaces:**
- Consumes: full `PipelineImpl` from Tasks 6–8.

- [ ] **Step 1: Write the tests**

```java
package com.example.datapipeline.api;

import static org.junit.Assert.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.junit.Test;

public class LifecycleTest {

    private static final java.util.concurrent.Executor DIRECT = Runnable::run;

    private DataPipeline<Integer, Integer> build(ExecutionMode mode, UiUpdateMode ui) {
        return DataPipeline.<Integer, Integer>builder()
                .processor(i -> i)
                .uiConsumer(i -> {})
                .overflowPolicy(OverflowPolicy.PROCESS_ALL)
                .executionMode(mode)
                .uiUpdateMode(ui)
                .uiThreadExecutor(DIRECT)
                .build();
    }

    @Test public void submitAfterCloseReturnsFalseWithoutThrowing() {
        DataPipeline<Integer, Integer> p = build(ExecutionMode.SEQUENTIAL, UiUpdateMode.immediate());
        p.close();
        assertFalse(p.submit(1));
    }

    @Test public void closeIsIdempotent() {
        DataPipeline<Integer, Integer> p = build(ExecutionMode.SEQUENTIAL, UiUpdateMode.immediate());
        p.close();
        p.close(); // must not throw
    }

    @Test public void closeTerminatesAllPipelineThreads() throws Exception {
        DataPipeline<Integer, Integer> p = build(
                ExecutionMode.parallelOrdered(3), UiUpdateMode.periodic(50));
        // let it spin up fully
        p.submit(1);
        Thread.sleep(150);
        p.close();
        Thread.sleep(200); // give threads time to die
        for (Thread t : allThreads()) {
            assertFalse("thread still alive after close: " + t.getName(),
                    t.getName().startsWith("datapipeline-") && t.isAlive());
        }
    }

    @Test public void closeLetsInFlightWorkFinish() throws Exception {
        CountDownLatch started = new CountDownLatch(1);
        CountDownLatch delivered = new CountDownLatch(1);
        DataPipeline<Integer, Integer> p = DataPipeline.<Integer, Integer>builder()
                .processor(i -> {
                    started.countDown();
                    try { Thread.sleep(200); } catch (InterruptedException ignored) {}
                    return i;
                })
                .uiConsumer(i -> delivered.countDown())
                .overflowPolicy(OverflowPolicy.PROCESS_ALL)
                .executionMode(ExecutionMode.parallelOrdered(2))
                .uiThreadExecutor(DIRECT)
                .build();
        p.submit(1);
        assertTrue(started.await(2, TimeUnit.SECONDS));
        p.close(); // pool shutdown() lets the in-flight task finish within the 2s budget
        assertTrue("in-flight item should complete during close",
                delivered.await(1, TimeUnit.SECONDS));
    }

    private static Thread[] allThreads() {
        Thread[] threads = new Thread[Thread.activeCount() * 2];
        int n = Thread.enumerate(threads);
        return java.util.Arrays.copyOf(threads, n);
    }
}
```

- [ ] **Step 2: Run tests**

Run: `mvn -q -pl com.example.datapipeline.tests -am test`
Expected: likely all PASS (close() written in Task 6). If `closeTerminatesAllPipelineThreads` fails: the sequential worker blocked in `intake.take()` needs the interrupt — verify `close()` interrupts `worker` *after* setting `closed = true`, and that `sequentialLoop` returns on `InterruptedException`. Fix whatever gap the failure shows; do not weaken the test.

- [ ] **Step 3: Commit**

```bash
git add -A && git commit -m "test: lifecycle guarantees — idempotent close, no leaked threads"
```

---

### Task 10: DS component and factory service

**Files:**
- Create: `com.example.datapipeline/src/com/example/datapipeline/api/PipelineFactory.java`
- Create: `com.example.datapipeline/src/com/example/datapipeline/internal/PipelineFactoryComponent.java`
- Modify: `com.example.datapipeline/OSGI-INF/component.xml` (replace placeholder)
- Test: `com.example.datapipeline.tests/src/test/java/com/example/datapipeline/internal/PipelineFactoryComponentTest.java`

**Interfaces:**
- Produces: `PipelineFactory { <T,R> DataPipeline<T,R> build(DataPipeline.Builder<T,R> builder); }` — an OSGi service; pipelines built through it are closed automatically when the bundle deactivates.

- [ ] **Step 1: Write the failing tests**

```java
package com.example.datapipeline.internal;

import static org.junit.Assert.*;
import org.junit.Test;

import com.example.datapipeline.api.DataPipeline;
import com.example.datapipeline.api.OverflowPolicy;

public class PipelineFactoryComponentTest {

    private static DataPipeline.Builder<Integer, Integer> validBuilder() {
        return DataPipeline.<Integer, Integer>builder()
                .processor(i -> i)
                .uiConsumer(i -> {})
                .overflowPolicy(OverflowPolicy.LATEST_WINS)
                .uiThreadExecutor(Runnable::run);
    }

    @Test public void buildReturnsWorkingPipeline() {
        PipelineFactoryComponent factory = new PipelineFactoryComponent();
        DataPipeline<Integer, Integer> p = factory.build(validBuilder());
        assertTrue(p.submit(1));
        factory.deactivate();
    }

    @Test public void deactivateClosesAllCreatedPipelines() {
        PipelineFactoryComponent factory = new PipelineFactoryComponent();
        DataPipeline<Integer, Integer> p1 = factory.build(validBuilder());
        DataPipeline<Integer, Integer> p2 = factory.build(validBuilder());
        factory.deactivate();
        assertFalse(p1.submit(1));
        assertFalse(p2.submit(1));
    }

    @Test public void deactivateIsIdempotent() {
        PipelineFactoryComponent factory = new PipelineFactoryComponent();
        factory.build(validBuilder());
        factory.deactivate();
        factory.deactivate(); // must not throw
    }
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `mvn -q -pl com.example.datapipeline.tests -am test`
Expected: COMPILATION ERROR (`PipelineFactory`/`PipelineFactoryComponent` missing).

- [ ] **Step 3: Implement**

`PipelineFactory.java`:

```java
package com.example.datapipeline.api;

/**
 * OSGi service for creating pipelines. Pipelines built through this factory are
 * closed automatically when the datapipeline bundle deactivates — no leaked
 * threads across bundle restarts.
 */
public interface PipelineFactory {
    <T, R> DataPipeline<T, R> build(DataPipeline.Builder<T, R> builder);
}
```

`PipelineFactoryComponent.java`:

```java
package com.example.datapipeline.internal;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import com.example.datapipeline.api.DataPipeline;
import com.example.datapipeline.api.PipelineFactory;

/** DS component (declared in OSGI-INF/component.xml). */
public final class PipelineFactoryComponent implements PipelineFactory {

    private final List<DataPipeline<?, ?>> pipelines = new CopyOnWriteArrayList<DataPipeline<?, ?>>();

    @Override public <T, R> DataPipeline<T, R> build(DataPipeline.Builder<T, R> builder) {
        DataPipeline<T, R> p = builder.build();
        pipelines.add(p);
        return p;
    }

    /** DS deactivate method — closes every pipeline this factory created. */
    public void deactivate() {
        for (DataPipeline<?, ?> p : pipelines) {
            try { p.close(); } catch (Throwable ignored) {}
        }
        pipelines.clear();
    }
}
```

`OSGI-INF/component.xml` (replace placeholder):

```xml
<?xml version="1.0" encoding="UTF-8"?>
<scr:component xmlns:scr="http://www.osgi.org/xmlns/scr/v1.3.0"
               name="com.example.datapipeline.factory"
               deactivate="deactivate">
  <implementation class="com.example.datapipeline.internal.PipelineFactoryComponent"/>
  <service>
    <provide interface="com.example.datapipeline.api.PipelineFactory"/>
  </service>
</scr:component>
```

If the `Service-Component` manifest header was removed in Task 1, re-add it now.

- [ ] **Step 4: Run tests and full build**

Run: `mvn -q verify`
Expected: BUILD SUCCESS, all tests pass, bundle jar contains `OSGI-INF/component.xml` (verify: `unzip -l com.example.datapipeline/target/*.jar | grep component.xml`).

- [ ] **Step 5: Commit**

```bash
git add -A && git commit -m "feat: PipelineFactory DS service with automatic pipeline cleanup"
```

---

### Task 11: README

**Files:**
- Create: `README.md`

- [ ] **Step 1: Write README** covering: what the library does (one paragraph), the builder example from the spec's "Public API" section verbatim, the three overflow policies, the two execution modes, the three UI delivery behaviors, the degenerate-combination warnings, thread inventory, and how to build (`mvn verify`, JDK 11+ to run Maven, bundle targets Java 8).

- [ ] **Step 2: Verify the build one final time**

Run: `mvn -q verify`
Expected: BUILD SUCCESS.

- [ ] **Step 3: Commit and push**

```bash
git add -A && git commit -m "docs: README with usage and build instructions"
git push
```
