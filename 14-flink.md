# Topic 14 — Apache Flink

> Standalone deep-dive · The production streaming engine for exactly-once state-rich processing

[← Back to main](./README.md)

---

## How to use this document

This is the architect-level reference for Apache Flink: how it achieves exactly-once semantics via checkpoints, how its event-time processing works, how to operate it in production, and when to pick it over Spark Streaming or Kafka Streams. Every chapter follows the four lenses: **Concept · Mechanics · Trade-offs · Interview**.

> **🧠 Principal Mindset:** Flink is the production answer for serious streaming. Spark Streaming is fine for micro-batch workloads where seconds-of-latency is OK. Kafka Streams is great when both ends are Kafka. For everything else — exactly-once with state, event-time correctness, sub-second latency at scale — Flink is the staff-level choice. If your streaming architecture needs to survive scrutiny from a principal interviewer, know Flink deeply.

## Table of Contents

**PART I — FOUNDATIONS**
- 1. What Flink Is and Why It Matters
- 2. The History: From Stratosphere to Industry Standard
- 3. Core Design Principles
- 4. Flink vs Spark Streaming vs Kafka Streams

**PART II — THE EXECUTION MODEL**
- 5. The Dataflow Graph
- 6. Operators, Tasks, Subtasks, and Slots
- 7. Parallelism and Scaling
- 8. Backpressure

**PART III — STATE**
- 9. The State Abstraction
- 10. State Backends (Heap, RocksDB)
- 11. Keyed vs Operator State
- 12. State TTL and Cleanup

**PART IV — TIME AND WATERMARKS**
- 13. Processing Time vs Event Time vs Ingestion Time
- 14. Watermarks: The Core Concept
- 15. Windows: Tumbling, Sliding, Session
- 16. Allowed Lateness and Late Data

**PART V — FAULT TOLERANCE**
- 17. Checkpoints: The Chandy-Lamport Snapshot
- 18. Savepoints
- 19. Exactly-Once End-to-End
- 20. State Recovery

**PART VI — APIS**
- 21. DataStream API
- 22. Table API and Flink SQL
- 23. ProcessFunction (The Escape Hatch)
- 24. The Python API (PyFlink)

**PART VII — PRODUCTION PATTERNS**
- 25. Kafka → Flink → Iceberg/Lakehouse
- 26. CDC with Flink CDC
- 27. Stream-Table Joins and Enrichment
- 28. Operational Playbook

**PART VIII — INTERVIEW PRACTICE**
- 29. The Staff-Level Flink Interview
- 30. War Stories
- 31. Cheat Sheet & Mental Models

---

## PART I — FOUNDATIONS

### 1. What Flink Is and Why It Matters

#### 1.1 The One-Sentence Definition

Apache Flink is a **distributed stream processing framework** that provides exactly-once state consistency, event-time processing with watermarks, millisecond-latency processing, and the ability to treat batch as a special case of streaming — all with a unified API.

#### 1.2 The Shape of a Flink Application

```
Kafka → [FLINK PIPELINE] → Sink
            │
            ├── Source operators (parallel, read from Kafka partitions)
            ├── Transformations (map, filter, keyBy, window, aggregate)
            ├── State (per-key, maintained across events)
            └── Sink operators (parallel, write to Iceberg/Kafka/DB)
```

A Flink job runs continuously (or until stopped). It processes events as they arrive, maintains state, and emits results.

#### 1.3 Key Properties

```
   • EXACTLY-ONCE STATE SEMANTICS
     Via asynchronous checkpoints (Chandy-Lamport algorithm)
   
   • EVENT-TIME PROCESSING
     Watermarks propagate through the pipeline
     Correct results regardless of event arrival order
   
   • LOW LATENCY
     Single-event streaming (not micro-batch)
     Sub-second end-to-end possible
   
   • STATEFUL
     Operators can maintain state keyed by partition
     State scales with data; persisted to RocksDB
   
   • UNIFIED BATCH + STREAM
     Same API for both (DataStream, Table, SQL)
     Batch is "streaming over a bounded source"
```

#### 1.4 Who Uses Flink

```
Reference deployments:
   • Alibaba (production since 2016; billions of events/day)
   • Netflix (real-time personalization)
   • Uber (fraud detection, ETA)
   • Lyft (surge pricing)
   • LinkedIn (streaming analytics)
   • Pinterest (ads)
   • Stripe (fraud, risk)
   • Thousands of mid-size companies

Common thread: real-time correctness matters.
Fraud, personalization, pricing, risk — can't tolerate approximate.
```

> **🎯 Interview Angle:** Q: Why Flink over Spark Streaming?
> **Staff:** "Four specific reasons. (1) Latency: Flink is event-at-a-time; Spark is micro-batch (minimum ~hundreds of ms). For sub-second end-to-end, Flink. (2) Exactly-once with state: both achieve it, but Flink's checkpoint mechanism is lighter-weight and more mature. (3) Event-time semantics: Flink's watermark model is more flexible for out-of-order events. (4) Memory/state scalability: Flink's RocksDB state backend handles TB+ of state per operator; Spark's state primitives are less scalable. The counter: Spark wins if the team is Spark-fluent, the latency bar is seconds-not-milliseconds, or micro-batch simplicity is a value. For a pure streaming interview answer at staff level, know when to pick each — not blanket preferences."

### 2. The History: From Stratosphere to Industry Standard

#### 2.1 Origins (2010-2015)

```
2010: Stratosphere project begins at TU Berlin.
      Research on distributed data processing with Nephele + PACTs.
      
2014: Stratosphere forks into Flink (Apache incubator).
      Reorganized around dataflow + streaming primitives.

2015: Flink 0.9 — first production releases.
      Google's Dataflow paper (2015) validates Flink's approach:
      event-time semantics, watermarks, exactly-once.
```

#### 2.2 Growth (2016-2019)

```
2016: Alibaba adopts Flink for Singles Day (billions of events/day).
      Flink Forward conference starts.

2017: Flink 1.3 — exactly-once sink support matures.
      Stateful stream processing established as a category.

2018: Alibaba acquires Flink's creators (data Artisans → Ververica).
      Kubernetes support added.

2019: Flink 1.9 — unified batch/streaming announced.
      Blink SQL (Alibaba's branch) merged.
```

#### 2.3 Maturity (2020-2024)

```
2020: Flink 1.11+ — unified batch/stream execution.
      PyFlink matures.

2021: Flink 1.13-1.14 — Flink SQL for stream + batch.
      Table Store (later Paimon) introduced.

2022: Flink 1.15-1.16 — SQL improvements, better K8s operators.
      Confluent acquires Immerok (Flink specialist).

2023: Confluent Cloud offers managed Flink.
      Flink 1.17-1.18 — major SQL enhancements.

2024: Flink 1.19-1.20 — further unification + optimizations.
      Flink is clearly the de facto streaming engine.
```

#### 2.4 Current State (2025-2026)

```
2025: Flink 2.0 released with major changes:
   • Disaggregated state management (storage separate from compute)
   • Streaming lakehouse integration (Iceberg, Paimon, Hudi)
   • Improved SQL semantics
   • Better Kubernetes operations

2026: Flink is the staff-level default for streaming.
   Confluent Cloud, Ververica, AWS KDA, Google Dataflow, Alibaba RT Compute
   all offer managed Flink.
```

### 3. Core Design Principles

#### 3.1 Streaming First

```
Flink is designed as a STREAMING engine with batch as a special case.

Spark took the opposite path: batch engine with streaming via micro-batch.

Consequences:
   • Flink: event-at-a-time; sub-millisecond-per-event overhead
   • Spark Streaming: batch-at-a-time; overhead ~hundreds of ms per batch
   
   Flink wins at low latency; Spark wins at throughput for very large batches.
```

#### 3.2 State Is a First-Class Citizen

```
Operators maintain state keyed by key.
State is persisted (RocksDB) + checkpointed to durable storage.
State can scale with data: TB+ per operator.

Compare:
   Kafka Streams: state via RocksDB per task; good up to GB
   Spark Streaming: state via mapGroupsWithState; less scalable
   Flink: state as a core abstraction; TB+ at scale
```

#### 3.3 Event Time via Watermarks

```
Flink's watermark system (based on Google Dataflow research):
   • Each operator has a watermark W
   • W asserts "no events with timestamp < W will arrive after this point"
   • Windows close when watermark passes their end
   • Allowed-lateness extends windows to accept late events
   • Late events beyond allowed-lateness go to side-output

This handles out-of-order delivery correctly.
```

#### 3.4 Exactly-Once Semantics

```
Flink achieves exactly-once via:
   1. Checkpoints: periodic distributed snapshots of all operator state
   2. Source offsets committed with checkpoint (replayable sources)
   3. Transactional sinks (2PC) OR idempotent sinks + replayed sources

End-to-end exactly-once requires BOTH source and sink to participate.
```

#### 3.5 Operator Chaining and Fusion

```
Flink's optimizer chains compatible operators into one task:
   source → map → filter → keyBy → window → sink
   
   Chain forms: [source → map → filter] + [keyBy ... window] + [sink]
   Each chain runs on one thread, avoiding serialization between operators.
   
   Massive performance gain for CPU-bound pipelines.
```

### 4. Flink vs Spark Streaming vs Kafka Streams

#### 4.1 The Comparison Matrix

| Dimension | Flink | Spark Streaming | Kafka Streams |
|-----------|-------|-----------------|---------------|
| **Processing model** | True streaming (event-at-a-time) | Micro-batch | Streaming (library) |
| **Deployment** | Separate cluster (JobManager + TaskManagers) | Spark cluster | JVM library embedded in app |
| **Latency** | <100ms achievable | 100ms - 1s (micro-batch) | <100ms |
| **Throughput** | Very high | Very high | Moderate |
| **State scale** | TB+ (RocksDB disaggregated) | GB | GB |
| **Event-time** | First-class, mature | Supported | Supported |
| **Exactly-once** | Yes (checkpoints + 2PC sinks) | Yes (checkpointing) | Yes (Kafka transactions) |
| **SQL support** | Flink SQL (mature) | Structured Streaming SQL | kSQL (separate project) |
| **Language** | Java, Scala, Python (PyFlink), SQL | Scala, Java, Python, R | Java, Scala (JVM-only) |
| **Operational burden** | Medium-high | Medium (reuses Spark infra) | Low (library in your app) |
| **Multi-source** | Many connectors | Many connectors | Kafka primarily |
| **Best for** | Low-latency, state-heavy, complex streaming | Spark shops; micro-batch OK | Kafka-in-Kafka-out, lightweight |

#### 4.2 Decision Framework

```
PICK FLINK WHEN:
   ✓ Low-latency (sub-second) critical
   ✓ Large state (GB-TB per key space)
   ✓ Complex event-time semantics (watermarks, windowing)
   ✓ Serious exactly-once with multiple sinks
   ✓ Team can manage separate cluster
   ✓ Multi-source / multi-sink topology

PICK SPARK STREAMING WHEN:
   ✓ Already Spark shop (shared skill, cluster)
   ✓ Micro-batch latency acceptable (10s-1 min)
   ✓ Workload includes batch + stream together
   ✓ Easier operations (fewer moving parts)

PICK KAFKA STREAMS WHEN:
   ✓ Kafka-only pipeline
   ✓ Lightweight (no separate cluster)
   ✓ Embedded in app
   ✓ Small-to-medium state
   ✓ JVM team
```

#### 4.3 The Managed Options

```
MANAGED FLINK (reduces ops burden):
   • Confluent Cloud Flink (SQL-first, managed)
   • Ververica Cloud (full Flink)
   • AWS Kinesis Data Analytics (fully managed)
   • Google Dataflow (uses Apache Beam, Flink underneath for some)
   • Alibaba Realtime Compute

For teams without SRE bandwidth, managed Flink closes the ops gap vs Kafka Streams.
```

---

## PART II — THE EXECUTION MODEL

### 5. The Dataflow Graph

#### 5.1 The DAG

```
A Flink job is a Directed Acyclic Graph (DAG) of operators.

   SOURCE → MAP → FILTER → KEYBY → WINDOW → AGG → SINK

Each operator is deployed to one or more parallel instances (subtasks).
Data flows from upstream to downstream subtasks via network channels.
```

#### 5.2 Logical Plan vs Physical Plan

```
LOGICAL PLAN:
   What the user wrote (DataStream API, Table API, SQL).

PHYSICAL PLAN:
   How Flink actually executes:
      • Operator chaining (fuse compatible operators)
      • Parallelism per operator
      • Partition strategy between operators (hash, broadcast, forward, rebalance)
```

#### 5.3 Streaming vs Batch Execution

```
STREAMING MODE (default):
   • Operators run continuously
   • Backpressure propagates upstream
   • Watermarks drive window closures

BATCH MODE (bounded source):
   • Operators process stage-by-stage (like Spark)
   • Can shuffle through disk
   • Different optimizations (hash joins, bulk aggregation)

Flink's unified API means the SAME CODE runs in either mode.
```

### 6. Operators, Tasks, Subtasks, and Slots

#### 6.1 The Hierarchy

```
JOB
├── OPERATOR (logical, e.g. "filter")
│   ├── SUBTASK 1 (parallel instance on one slot)
│   ├── SUBTASK 2 (parallel instance on another slot)
│   ├── SUBTASK 3
│   └── ...
├── OPERATOR ("window aggregation")
│   ├── SUBTASK 1
│   └── ...
└── OPERATOR ("sink")
```

#### 6.2 Task Managers and Slots

```
TASK MANAGER = a JVM process running on a worker machine.
   Has a configured number of SLOTS (default 1).
   Each slot = resources for one parallel operator subtask (or chain).

CLUSTER = JobManager + N TaskManagers.
   Total parallelism = sum of slots across TaskManagers.

Example:
   JobManager + 10 TaskManagers × 4 slots each = 40 slots
   A job with parallelism=40 fully occupies the cluster.
```

#### 6.3 Operator Chaining

```
Compatible operators can be chained into one slot:
   source → map → filter   (if same parallelism, forward partition)
   
Entire chain runs on one thread; no network between them.
Benefits: no serialization overhead; better cache locality.
```

#### 6.4 Slot Sharing

```
By default, operators from different stages share slots:
   Slot 1: subtask-1-of-source, subtask-1-of-filter, subtask-1-of-sink
   
This maximizes resource use; one slot runs end-to-end pipeline.
```

### 7. Parallelism and Scaling

#### 7.1 Setting Parallelism

```java
// Default parallelism for all operators
env.setParallelism(10);

// Per-operator parallelism
dataStream.map(...).setParallelism(20);
```

#### 7.2 Scaling Up

```
Flink doesn't auto-scale by default (yet).
Scaling requires:
   1. Stop job (take savepoint)
   2. Change parallelism
   3. Restart from savepoint

Scaling triggers KEY REDISTRIBUTION:
   Keys hash to different subtasks when parallelism changes.
   Keyed state must redistribute accordingly.
   Flink handles this via Key Groups (fixed buckets of keys).
```

#### 7.3 Key Groups

```
Keys are hashed into KEY_GROUPS (default 128).
Each key group's state is assigned to one subtask.

When parallelism changes:
   Reassign key groups across subtasks.
   State for each key group is moved as a unit.
```

#### 7.4 Reactive Mode (Experimental)

```
Flink 1.13+ offers reactive mode for auto-scaling in Kubernetes:
   Adds/removes TaskManagers; Flink adjusts parallelism accordingly.
   Still maturing in 2026.
```

### 8. Backpressure

#### 8.1 What Backpressure Is

```
BACKPRESSURE = downstream slowness propagates upstream.

Mechanism:
   • Downstream subtask's input buffer fills
   • It stops reading from network
   • Upstream subtask can't write; its output buffer fills
   • Eventually: upstream stops processing (effectively blocks source)
```

#### 8.2 Why It's Good

```
Backpressure protects from catastrophe:
   • Without it: slow sink causes memory explosion upstream
   • With it: slow sink causes upstream to slow down naturally
   
Flink's credit-based flow control makes backpressure efficient:
   Receivers announce available buffer space; senders don't overflow.
```

#### 8.3 Diagnosing Backpressure

```
Flink UI:
   Each subtask shows a backpressure indicator (OK / LOW / HIGH).
   HIGH on a subtask means it's being throttled by downstream.

Common causes:
   • Slow sink (DB write, Kafka produce)
   • Expensive operation (large join, slow UDF)
   • Insufficient parallelism
   • Resource contention (CPU, memory, GC)
```

> **🎯 Interview Angle:** Q: Your Flink job has consistent backpressure in the middle of the pipeline. What do you check?
> **Staff:** "First, identify which operator is BACKPRESSURE_SOURCE (the actual bottleneck) vs BACKPRESSURE_PROPAGATED (just a victim). The backpressured operator just upstream of a slow operator is the signal. Then: (1) is the slow operator CPU-bound? Check task manager CPU; may need higher parallelism or a more efficient operation. (2) Is it I/O-bound? Likely a sink or external call. Add async I/O or increase sink parallelism. (3) Is it memory-bound? GC pressure can throttle; tune state backend or heap. (4) Is there skew? Some keys taking disproportionate work; may need key salting or custom partitioning. The signal to management: never ignore backpressure. If it's chronic, the pipeline is operating at the margin — any blip tips it over."

---

## PART III — STATE

### 9. The State Abstraction

#### 9.1 Why State Matters

```
Streaming without state = just filters and maps.
Streaming WITH state = running counts, deduplication, joins, windows, patterns.

Example state uses:
   • Running counter per user (click count)
   • Last-seen-at per user (session timeout)
   • Join buffers (keep records from stream A waiting for matches in B)
   • Window buckets (collect events in a time window)
   • Deduplication sets (seen-before IDs)
```

#### 9.2 State Access

```java
public class CountingFunction extends KeyedProcessFunction<String, Event, Count> {
    private ValueState<Long> countState;
    
    @Override
    public void open(Configuration config) {
        countState = getRuntimeContext().getState(
            new ValueStateDescriptor<>("count", Long.class));
    }
    
    @Override
    public void processElement(Event event, Context ctx, Collector<Count> out) {
        Long count = countState.value();
        if (count == null) count = 0L;
        count++;
        countState.update(count);
        out.collect(new Count(event.user, count));
    }
}
```

The state is implicitly keyed by the current key (ctx.getCurrentKey()).

#### 9.3 State Types

```
ValueState<T>           — single value per key
ListState<T>            — list per key
MapState<K, V>          — map per key
ReducingState<T>        — single value, reduced via function
AggregatingState<I, O>  — like reducing, with input/output type separation
```

### 10. State Backends (Heap, RocksDB)

#### 10.1 HashMapStateBackend (In-Memory)

```
State stored in JVM heap as a HashMap.
Fast access; limited by heap size.

GOOD FOR:
   • Small state (sub-GB)
   • Low-latency reads
   • Simple jobs

BAD FOR:
   • Large state (OOM risk)
   • GC pressure
   • Jobs with unpredictable state growth
```

#### 10.2 RocksDBStateBackend

```
State stored in embedded RocksDB (LSM tree).
Lives on local disk; spills from heap.

GOOD FOR:
   • Large state (GB-TB)
   • Predictable memory usage (RocksDB manages memory)
   • Production streaming

BAD FOR:
   • Low-latency reads (disk IO vs heap)
   • Jobs with tiny state (overhead not worth it)
```

#### 10.3 Configuring

```yaml
# flink-conf.yaml
state.backend: rocksdb
state.backend.rocksdb.localdir: /mnt/local-ssd/rocksdb
state.checkpoints.dir: s3://bucket/checkpoints/
state.savepoints.dir: s3://bucket/savepoints/
```

#### 10.4 Flink 2.0 Disaggregated State

```
Flink 2.0 (2025) introduced disaggregated state:
   State on remote storage (S3) with aggressive local caching.
   
Benefits:
   • Scale state without scaling compute
   • Fast rescaling (no state transfer needed)
   • Better cost efficiency

Trade-off: higher read latency for cold state (vs local RocksDB).
```

### 11. Keyed vs Operator State

#### 11.1 Keyed State

```
Associated with a specific KEY in a keyed stream.
   dataStream.keyBy(event -> event.user_id)
             .process(new MyKeyedProcessFunction());

Each key sees its own state.
State is partitioned: subtask handling user_42 has only user_42's state.
```

#### 11.2 Operator State

```
Associated with a specific OPERATOR SUBTASK.
Not keyed; used for things like:
   • Source offsets (Kafka consumer offsets per partition)
   • Sink buffers (Kafka producer in-flight messages)

Less commonly used in application code.
```

#### 11.3 Broadcast State

```
A special operator state: broadcast to all subtasks.
Used for dynamic rules / configs.

Example: fraud detection rules updated from a config stream.
   The rules are broadcast; each subtask has its own copy.
```

### 12. State TTL and Cleanup

#### 12.1 Why TTL

```
State can grow indefinitely without pruning:
   • Unique users ever seen
   • Session states never cleaned up
   • Join buffer growing with every miss

TTL (Time-to-Live) auto-expires state after N time.
```

#### 12.2 Configuring TTL

```java
StateTtlConfig ttlConfig = StateTtlConfig
    .newBuilder(Time.hours(24))
    .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
    .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
    .cleanupInRocksdbCompactFilter(1000)
    .build();

ValueStateDescriptor<Long> stateDesc = new ValueStateDescriptor<>("count", Long.class);
stateDesc.enableTimeToLive(ttlConfig);
```

#### 12.3 TTL Cleanup Strategies

```
cleanupInBackground():
   RocksDB compaction filter prunes expired state during compaction.
   Default for RocksDB backend.

cleanupIncrementally():
   Periodic small cleanup during state access.
   Default for heap backend.

cleanupFullSnapshot():
   Cleanup during savepoint/checkpoint.
```

> **🔥 Production Scenario:** A fraud detection pipeline tracked unique devices seen in the last 24 hours. Without TTL, state grew unbounded; after 2 months the job used 40 GB of state and failed on checkpoints. Added TTL of 24h: state stabilized at 500 MB. Lesson: any keyed state must have an expiry story — explicitly or via natural turnover.

---

## PART IV — TIME AND WATERMARKS

### 13. Processing Time vs Event Time vs Ingestion Time

#### 13.1 The Three Times

```
PROCESSING TIME:
   The system clock on the Flink TaskManager when the event is processed.
   
   Pros: simple, no watermarks needed.
   Cons: non-deterministic; replays produce different results.

EVENT TIME:
   A timestamp carried by the event itself (from source).
   
   Pros: deterministic; replays produce same results.
   Cons: requires watermarks; out-of-order events are a concern.

INGESTION TIME:
   The timestamp assigned by Flink when the event enters the source operator.
   
   Pros: deterministic within a Flink job.
   Cons: doesn't survive source replays (new ingestion time each time).
```

#### 13.2 Choosing

```
USE EVENT TIME WHEN:
   ✓ You care about correctness regardless of event arrival order
   ✓ Replays must produce consistent results
   ✓ The source carries meaningful timestamps

USE PROCESSING TIME WHEN:
   ✓ You care about "wall clock" grouping (e.g., every 1 minute of real time)
   ✓ The business doesn't care about event order
   ✓ Simplicity is paramount

MOST PRODUCTION USE CASES → event time.
```

### 14. Watermarks: The Core Concept

#### 14.1 What a Watermark Is

```
WATERMARK = a timestamp W asserting:
   "No events with timestamp < W will arrive after this point."

Flink inserts watermarks into the stream.
Operators use watermarks to decide when to emit results for a window.

Watermarks flow through the pipeline alongside events.
```

#### 14.2 Generating Watermarks

```java
WatermarkStrategy<Event> strategy = WatermarkStrategy
    .<Event>forBoundedOutOfOrderness(Duration.ofSeconds(5))
    .withTimestampAssigner((event, timestamp) -> event.timestamp);

dataStream.assignTimestampsAndWatermarks(strategy);
```

The "bounded out-of-orderness" strategy assumes events can arrive up to 5 seconds late. Watermark = max observed timestamp - 5 seconds.

#### 14.3 Watermark Propagation

```
Watermarks flow through the pipeline:
   Source operator generates watermarks based on its events.
   Downstream operators take the MIN of watermarks from all inputs.
      (A keyBy'd operator takes min across upstream subtasks.)

Why min: a window can't close until ALL upstream subtasks confirm
         no earlier events are coming.
```

#### 14.4 Watermark Semantics

```
Watermark W → "No events with timestamp < W will arrive."
   
If an event arrives with timestamp < W: it's LATE.
Flink handles late events via:
   • Dropping (default for windows)
   • Allowed-lateness (extend window)
   • Side-output (route to a separate stream for processing)
```

### 15. Windows: Tumbling, Sliding, Session

#### 15.1 Tumbling Windows

```
Fixed-size, non-overlapping.
   TumblingEventTimeWindows.of(Time.minutes(10))
   
   10:00-10:10, 10:10-10:20, 10:20-10:30, ...
```

#### 15.2 Sliding Windows

```
Fixed-size, overlapping.
   SlidingEventTimeWindows.of(Time.minutes(10), Time.minutes(5))
   
   10:00-10:10, 10:05-10:15, 10:10-10:20, ...
   
   Each event belongs to multiple windows.
```

#### 15.3 Session Windows

```
Dynamic-sized windows based on gaps.
   EventTimeSessionWindows.withGap(Time.minutes(5))
   
   Session = sequence of events with gap < 5 min.
   
   Different sessions can have different durations.
   Classic use: user web sessions.
```

#### 15.4 Window Operations

```java
dataStream
    .keyBy(e -> e.user_id)
    .window(TumblingEventTimeWindows.of(Time.minutes(10)))
    .aggregate(new MyAggregator());

// With a trigger (fire early for intermediate results)
dataStream
    .keyBy(e -> e.user_id)
    .window(TumblingEventTimeWindows.of(Time.minutes(10)))
    .trigger(EarlyFireTrigger.create(Time.minutes(2)))
    .aggregate(new MyAggregator());
```

### 16. Allowed Lateness and Late Data

#### 16.1 Allowed Lateness

```
By default, when a window closes (watermark passes end), late events are dropped.

Allowed lateness extends the window:
   .allowedLateness(Time.minutes(5))
   
   After the window would normally close, late events (within 5 min) still
   update the window. Each late event may trigger a re-emission.
```

#### 16.2 Side Output for Late Data

```java
OutputTag<Event> lateTag = new OutputTag<>("late"){};

SingleOutputStreamOperator<Result> result = dataStream
    .keyBy(e -> e.user_id)
    .window(TumblingEventTimeWindows.of(Time.minutes(10)))
    .allowedLateness(Time.minutes(5))
    .sideOutputLateData(lateTag)
    .aggregate(new MyAggregator());

DataStream<Event> veryLate = result.getSideOutput(lateTag);
// Route very late events to a different pipeline / dead letter queue
```

#### 16.3 Trade-offs

```
Shorter allowed-lateness:
   Pros: lower memory (windows close sooner).
   Cons: more late data dropped or sidelined.

Longer allowed-lateness:
   Pros: more complete windows.
   Cons: more memory, more re-emissions, downstream must handle updates.

Rule: measure actual event lateness distribution.
      Set allowed-lateness to cover 99-99.9% of lateness.
```

---

## PART V — FAULT TOLERANCE

### 17. Checkpoints: The Chandy-Lamport Snapshot

#### 17.1 The Problem

```
A streaming job runs continuously. It can fail (process crash, machine failure).
On recovery, it must resume WITHOUT losing or duplicating data.

Challenge: a global snapshot of a distributed system is non-trivial.
   Operators are at different progress points.
   Can't just "pause everything and take a snapshot."
```

#### 17.2 The Chandy-Lamport Algorithm

```
Flink implements Chandy-Lamport distributed snapshots.

Core idea: inject SPECIAL MARKERS into the stream alongside data.
   When an operator sees a marker:
      1. Snapshot its own state.
      2. Forward the marker to downstream operators.
   
When all operators have snapshotted after the same marker,
the collective state is a consistent snapshot.
```

#### 17.3 Flink's Implementation

```
Checkpoint coordinator (JobManager) initiates checkpoints:
   1. Insert checkpoint barriers at sources (barrier N)
   2. Barriers flow downstream alongside data
   3. When an operator sees barrier N on ALL input channels:
      - Snapshot its state (async, write to RocksDB + remote storage)
      - Forward barrier N downstream
   4. Snapshot complete when all sinks acknowledge barrier N
   5. Coordinator commits checkpoint N

Properties:
   • Async: snapshots don't block processing
   • Consistent: all operators snapshotted "after" the same barrier
   • Scalable: 100 GB+ state per operator; TB+ cluster-wide
```

#### 17.4 Checkpoint Timing

```yaml
# flink-conf.yaml
execution.checkpointing.interval: 60s       # take a checkpoint every 60s
execution.checkpointing.min-pause: 10s      # wait at least 10s between
execution.checkpointing.timeout: 10min      # fail if checkpoint > 10 min
execution.checkpointing.max-concurrent-checkpoints: 1
```

#### 17.5 Checkpoint Storage

```
Checkpoints written to durable storage:
   • S3 (common)
   • HDFS
   • Azure Blob
   • Local filesystem (dev)

Format: files per operator, named by checkpoint ID + subtask ID.
```

### 18. Savepoints

#### 18.1 What a Savepoint Is

```
SAVEPOINT = a user-triggered checkpoint, intended for RESTARTING jobs.

Differences from automatic checkpoints:
   • Triggered manually (CLI command)
   • Not deleted automatically (persist indefinitely)
   • Portable across Flink versions (with some caveats)
   • Used for: planned upgrades, parallelism changes, state migration
```

#### 18.2 Taking a Savepoint

```bash
# Stop job with savepoint
flink stop --savepointPath s3://bucket/savepoints/ <jobId>

# Trigger savepoint while job runs
flink savepoint <jobId> s3://bucket/savepoints/
```

#### 18.3 Restoring from Savepoint

```bash
flink run -s s3://bucket/savepoints/savepoint-xxxxx \
          -p 10 \
          my-job.jar
```

Restores state from the savepoint; starts fresh execution.

#### 18.4 Savepoint vs Checkpoint

| Aspect | Savepoint | Checkpoint |
|--------|-----------|------------|
| **Trigger** | Manual | Automatic |
| **Retention** | Explicit | Managed (configurable retention) |
| **Format** | Stable, version-portable | Internal, may change |
| **Use case** | Planned ops | Fault tolerance |

### 19. Exactly-Once End-to-End

#### 19.1 The Challenge

```
Flink's checkpoints give exactly-once within Flink.
But end-to-end exactly-once requires the SOURCE and SINK to cooperate.

   Source: must be REPLAYABLE from committed offsets.
   Sink: must be TRANSACTIONAL (2PC) or IDEMPOTENT.
```

#### 19.2 Source Requirements

```
Replayable sources (support exactly-once):
   • Kafka (offsets committed with checkpoint)
   • Kinesis (sequence numbers)
   • Pulsar (message IDs)
   • File sources (with proper split tracking)

Non-replayable sources (at-most-once):
   • TCP socket
   • Non-committing message queues

For exactly-once end-to-end, use a replayable source.
```

#### 19.3 Transactional Sinks

```
Transactional sinks implement two-phase commit (2PC):
   
   Phase 1 (pre-commit): on checkpoint, buffer writes but don't commit.
   Phase 2 (commit): after checkpoint acknowledges, commit the transaction.

If Flink crashes between phases:
   Recover from last checkpoint; previous pre-commits are rolled back.
   Replay from the recovered offsets.
```

#### 19.4 Example: Kafka Sink with 2PC

```java
KafkaSink<String> sink = KafkaSink.<String>builder()
    .setBootstrapServers("...")
    .setRecordSerializer(...)
    .setDeliveryGuarantee(DeliveryGuarantee.EXACTLY_ONCE)
    .setTransactionalIdPrefix("my-tx-")
    .build();
```

Kafka transactions coordinate with Flink checkpoints.

#### 19.5 Idempotent Sinks (Alternative)

```
If the sink is idempotent (same input → same output regardless of retries),
you don't need 2PC. The sink can be at-least-once.

Example: UPSERT by key to a database.
   Replaying produces same final state; no 2PC needed.

Many real sinks (Iceberg via files, databases via UPSERT) are idempotent.
```

### 20. State Recovery

#### 20.1 Automatic Recovery

```
On task failure:
   1. Flink marks the task as failed.
   2. Restart strategy decides (restart all vs restart affected region).
   3. New tasks start on available slots.
   4. State is loaded from the last successful checkpoint.
   5. Source offsets are restored.
   6. Processing resumes.
```

#### 20.2 Restart Strategies

```yaml
# fixed-delay restart
restart-strategy: fixed-delay
restart-strategy.fixed-delay.attempts: 3
restart-strategy.fixed-delay.delay: 10s

# failure-rate restart
restart-strategy: failure-rate
restart-strategy.failure-rate.max-failures-per-interval: 3
restart-strategy.failure-rate.failure-rate-interval: 5min
restart-strategy.failure-rate.delay: 10s

# exponential-delay (Flink 1.13+)
restart-strategy: exponential-delay
```

#### 20.3 Region-Based Recovery

```
By default (and in modern Flink), only the FAILED REGION restarts.
A region = subgraph connected by pipelined (streaming) edges.

Minimizes recovery scope.
Jobs with embarrassing-parallel topology recover independently per region.
```

---

## PART VI — APIS

### 21. DataStream API

#### 21.1 The Workhorse API

```java
StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

DataStream<Event> events = env
    .fromSource(KafkaSource.<Event>builder()...build(),
                WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                    .withTimestampAssigner((e, ts) -> e.timestamp),
                "kafka-source");

DataStream<Alert> alerts = events
    .filter(e -> e.amount > 10000)
    .keyBy(e -> e.user_id)
    .window(TumblingEventTimeWindows.of(Time.minutes(5)))
    .reduce((a, b) -> new Event(a.user, a.amount + b.amount))
    .filter(e -> e.amount > 50000)
    .map(e -> new Alert(e.user, "high_volume"));

alerts.sinkTo(KafkaSink.<Alert>builder()...build());

env.execute("fraud-detection");
```

Low-level, maximum control, verbose but explicit.

### 22. Table API and Flink SQL

#### 22.1 Table API

```java
TableEnvironment tableEnv = TableEnvironment.create(settings);

tableEnv.executeSql("""
    CREATE TABLE orders (
        order_id BIGINT,
        user_id BIGINT,
        amount DECIMAL(18,2),
        order_time TIMESTAMP(3),
        WATERMARK FOR order_time AS order_time - INTERVAL '5' SECOND
    ) WITH (
        'connector' = 'kafka',
        'topic' = 'orders',
        'format' = 'json',
        ...
    )
""");

Table t = tableEnv.sqlQuery("""
    SELECT user_id, 
           TUMBLE_START(order_time, INTERVAL '5' MINUTE) AS window_start,
           SUM(amount) AS total
    FROM orders
    GROUP BY user_id, TUMBLE(order_time, INTERVAL '5' MINUTE)
""");

t.executeInsert("sink_table");
```

#### 22.2 Flink SQL

```sql
-- Define source
CREATE TABLE orders (
    order_id BIGINT,
    user_id BIGINT,
    amount DECIMAL(18,2),
    order_time TIMESTAMP(3),
    WATERMARK FOR order_time AS order_time - INTERVAL '5' SECOND
) WITH ('connector' = 'kafka', ...);

-- Define sink
CREATE TABLE user_totals (
    user_id BIGINT,
    window_start TIMESTAMP(3),
    total DECIMAL(18,2),
    PRIMARY KEY(user_id, window_start) NOT ENFORCED
) WITH ('connector' = 'jdbc', ...);

-- Streaming aggregation
INSERT INTO user_totals
SELECT 
    user_id,
    TUMBLE_START(order_time, INTERVAL '5' MINUTE),
    SUM(amount)
FROM orders
GROUP BY user_id, TUMBLE(order_time, INTERVAL '5' MINUTE);
```

Flink SQL is powerful: supports streaming semantics (windows, event-time), CDC, joins across streams and dimension tables.

### 23. ProcessFunction (The Escape Hatch)

#### 23.1 When Other APIs Fall Short

```
For complex per-event logic:
   • Custom state management (multiple states, complex interactions)
   • Timers (fire callbacks at specific wall-clock or event times)
   • Side outputs (emit to multiple streams)
   • Dynamic logic not expressible in SQL

KeyedProcessFunction gives you all of this.
```

#### 23.2 Example: Session Timeout Detection

```java
public class SessionTimeout extends KeyedProcessFunction<String, Event, Timeout> {
    private ValueState<Long> lastSeen;
    private final long TIMEOUT_MS = 30_000;
    
    @Override
    public void open(Configuration config) {
        lastSeen = getRuntimeContext().getState(
            new ValueStateDescriptor<>("lastSeen", Long.class));
    }
    
    @Override
    public void processElement(Event event, Context ctx, Collector<Timeout> out) {
        Long prev = lastSeen.value();
        if (prev != null) {
            // Delete the old timer
            ctx.timerService().deleteEventTimeTimer(prev + TIMEOUT_MS);
        }
        lastSeen.update(event.timestamp);
        ctx.timerService().registerEventTimeTimer(event.timestamp + TIMEOUT_MS);
    }
    
    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<Timeout> out) {
        out.collect(new Timeout(ctx.getCurrentKey(), timestamp));
        lastSeen.clear();
    }
}
```

### 24. The Python API (PyFlink)

#### 24.1 PyFlink Capabilities

```python
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import TableEnvironment

# Table API
t_env = TableEnvironment.create(...)

t_env.execute_sql("""
    CREATE TABLE orders (...) WITH ('connector' = 'kafka', ...)
""")

result = t_env.sql_query("SELECT ... FROM orders GROUP BY ...")
result.execute_insert("sink")

# DataStream API (newer)
env = StreamExecutionEnvironment.get_execution_environment()
stream = env.add_source(...)
stream.map(lambda x: ...).sink_to(...)
env.execute()
```

#### 24.2 Caveats

```
PyFlink wraps the Java Flink runtime.
Python UDFs: run via Py4J bridge → slower than Java/Scala UDFs.

For performance-critical operators: use Java/Scala.
For prototyping / glue code: PyFlink is fine.
Flink SQL in PyFlink: full performance (SQL executes in JVM).
```

---

## PART VII — PRODUCTION PATTERNS

### 25. Kafka → Flink → Iceberg/Lakehouse

#### 25.1 The Streaming Lakehouse Pattern

```
Kafka (events) 
   → Flink (processing, enrichment, aggregation)
   → Iceberg (ACID lakehouse layer on S3)
   → Downstream: Spark batch, Trino ad-hoc, Snowflake BI
```

#### 25.2 Setup

```java
// Flink reads Kafka
KafkaSource<Event> source = KafkaSource.<Event>builder()
    .setBootstrapServers("...")
    .setTopics("events")
    .setGroupId("flink-ingestion")
    .setStartingOffsets(OffsetsInitializer.earliest())
    .setValueOnlyDeserializer(new JsonDeserializer<>(Event.class))
    .build();

DataStream<Event> stream = env.fromSource(source, 
    WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ofSeconds(10))
        .withTimestampAssigner((e, ts) -> e.timestamp),
    "kafka");

// Flink writes to Iceberg
IcebergSink.builderForRow(stream.map(e -> ...))
    .table(myIcebergTable)
    .writeFormat(FileFormat.PARQUET)
    .partitionSpec(partitionSpec)
    .set("write-format", "parquet")
    .build();
```

#### 25.3 Checkpoint Interval Tuning

```
Checkpoint interval = commit frequency to Iceberg.

   Short (10 sec): low latency; many small files; aggressive compaction needed
   Medium (60 sec): balanced
   Long (5 min): fewer files; higher freshness latency

Recommended: 60-120 seconds for most production streaming lakehouse pipelines.
Pair with hourly data file compaction + nightly snapshot expiry.
```

### 26. CDC with Flink CDC

#### 26.1 What Flink CDC Is

```
Flink CDC = Flink connectors for Change Data Capture.
   • MySQL CDC
   • Postgres CDC
   • Oracle CDC
   • SQL Server CDC
   • MongoDB CDC

Uses database replication protocols directly (no Debezium needed).
```

#### 26.2 Example

```sql
CREATE TABLE orders_cdc (
    id BIGINT PRIMARY KEY NOT ENFORCED,
    user_id BIGINT,
    amount DECIMAL(18,2),
    updated_at TIMESTAMP(3)
) WITH (
    'connector' = 'postgres-cdc',
    'hostname' = 'db.example.com',
    'port' = '5432',
    'database-name' = 'prod',
    'schema-name' = 'public',
    'table-name' = 'orders',
    'slot.name' = 'flink_slot',
    'decoding.plugin.name' = 'pgoutput'
);

-- Downstream: write CDC changes to Iceberg
INSERT INTO orders_iceberg
SELECT * FROM orders_cdc;
```

### 27. Stream-Table Joins and Enrichment

#### 27.1 Lookup Joins

```sql
-- Join stream against a lookup table (dimension)
CREATE TABLE products (
    product_id BIGINT PRIMARY KEY NOT ENFORCED,
    product_name STRING,
    price DECIMAL(10,2)
) WITH ('connector' = 'jdbc', ...);

SELECT o.*, p.product_name, p.price
FROM orders o
JOIN products FOR SYSTEM_TIME AS OF o.proctime AS p
    ON o.product_id = p.product_id;
```

Lookup joins fetch dimension data at event time; the dimension table is NOT streamed.

#### 27.2 Temporal Joins

```sql
-- Join two streams with temporal semantics
CREATE TABLE prices (
    symbol STRING,
    price DECIMAL,
    quote_time TIMESTAMP(3),
    WATERMARK FOR quote_time AS ...,
    PRIMARY KEY(symbol) NOT ENFORCED
) WITH (...);

SELECT t.trade_id, t.trade_time, p.price AS price_at_trade
FROM trades t
JOIN prices FOR SYSTEM_TIME AS OF t.trade_time AS p
    ON t.symbol = p.symbol;
```

The temporal join finds the most recent price AS OF the trade's time — perfect for ASOF-style joins in streaming.

### 28. Operational Playbook

#### 28.1 Monitoring

```
KEY METRICS:
   • Checkpoint duration (should be < interval; alert if > 50% of interval)
   • Checkpoint size (growth indicates state growth)
   • Backpressure (any sustained HIGH = problem)
   • Records-in-rate vs records-out-rate (imbalance = backpressure)
   • Lag on source (Kafka consumer lag)
   • JVM heap, GC time (should be < 10% of time)
   • Watermark lag (if event time lags wall clock heavily)

TOOLS:
   • Flink Web UI (default)
   • Prometheus + Grafana (via metrics reporter)
   • Datadog, Dynatrace, etc.
```

#### 28.2 Common Failures

```
OOM:
   • Keyed state too large; switch to RocksDB backend
   • GC pressure; tune heap sizes
   • Memory leak in UDF (states not cleared)

CHECKPOINT TIMEOUTS:
   • State too large to snapshot in interval
   • Slow S3 writes (checkpoint storage saturated)
   • Backpressure (barriers can't flow)
   
   Mitigations: increase timeout, use incremental checkpoints,
                check backpressure source

RESTART LOOPS:
   • Deterministic bug (same input crashes repeatedly)
   • Dependency issue (unreachable sink)
   • OOM

STATE GROWTH:
   • Missing TTL
   • Keys with unbounded cardinality (UUIDs, device IDs forever)
```

#### 28.3 Deploy Patterns

```
STANDALONE CLUSTER:
   JobManager + TaskManagers run as separate processes.
   Jobs submitted via REST or CLI.

YARN:
   Flink deploys as YARN application.
   Legacy pattern for Hadoop shops.

KUBERNETES:
   Native Kubernetes operator (Flink Kubernetes Operator).
   Each job = a Kubernetes custom resource.
   Managed scaling, upgrades, HA.
   
   Modern default.

MANAGED:
   • AWS KDA (Kinesis Data Analytics)
   • Confluent Cloud Flink
   • Ververica Cloud
   • Google Dataflow (Flink or Beam)
   • Alibaba RT Compute
```

---

## PART VIII — INTERVIEW PRACTICE

### 29. The Staff-Level Flink Interview

#### 29.1 Common Questions

```
1. "How does Flink achieve exactly-once semantics?"
2. "Explain Chandy-Lamport snapshots in Flink's context."
3. "Processing time vs event time — when do you pick each?"
4. "How do watermarks work? What happens to late events?"
5. "Design a fraud detection pipeline with Flink."
6. "Your Flink job has backpressure — what do you investigate?"
7. "Flink vs Spark Streaming vs Kafka Streams: when do you pick each?"
8. "State backends: Heap vs RocksDB — trade-offs."
9. "How does end-to-end exactly-once require source + sink cooperation?"
10. "What happens on a Flink task failure?"
```

#### 29.2 Worked Example

```
Q: Design a real-time fraud detection pipeline. Expect 50K events/sec,
   p95 latency < 200ms, exactly-once end-to-end.

STAFF ANSWER:

ARCHITECTURE:
   Transactions (Kafka) 
     → Flink (fraud scoring + enrichment)
     → Decisions (Kafka output)
     → Also: audit log in Iceberg

TOPOLOGY:
   Source (Kafka)
     → Watermark assignment (event-time with 2s out-of-order tolerance)
     → KeyBy user_id
     → ProcessFunction (fraud rules + state)
          - State: ValueState<UserProfile> (user features)
          - State: ListState<RecentTxn> (last 10 txns per user)
          - RocksDB backend (user profiles too large for heap)
          - Timers to clean stale state (TTL 90 days)
     → Enrichment (lookup join to accounts dimension)
     → Scoring (ML model via async I/O to feature store)
     → Decision operator (threshold logic)
     → KafkaSink (EXACTLY_ONCE delivery guarantee, transactional)
     → Also: side-output to Iceberg sink for audit

STATE SIZING:
   50M users × ~2KB profile = 100 GB state in RocksDB
   Checkpoint every 60s; incremental checkpoints enabled

PARALLELISM:
   Source: 20 (Kafka partition count)
   Processing: 40 (CPU-bound; higher parallelism)
   Sinks: 20 (balance writer count vs Kafka transaction overhead)

LATENCY TARGET:
   p95 < 200ms is tight; Flink can hit this but requires:
   - Short checkpoint interval (shouldn't dominate processing)
   - Fast sink (Kafka with EXACTLY_ONCE commits every checkpoint)
   - Minimal GC pauses (RocksDB backend; well-tuned JVM)
   - Async I/O for feature store lookup (don't block processing)

CHECKPOINT STRATEGY:
   Interval: 60s
   Timeout: 5 min
   Incremental: enabled (only state deltas since last checkpoint)
   Backing storage: S3 with Transfer Manager for parallelism

RESCALING:
   Expected: 2x throughput growth over 12 months
   Mitigations:
      - Over-provision parallelism to 50 (buffer for growth)
      - Use savepoints for planned rescales
      - Monitor key-group hotspots

MONITORING:
   - End-to-end latency (p50/p95/p99)
   - Checkpoint duration + size
   - Backpressure per operator
   - Kafka source lag
   - Error rates per sink

TRADE-OFFS vs ALTERNATIVES:
   - Spark Streaming: micro-batch overhead too high for 200ms p95
   - Kafka Streams: 100 GB state in one app would be painful; cluster deploy makes HA easier
   - Flink SQL: expressible but would need UDFs for ML scoring; stick with DataStream
```

#### 29.3 Senior vs Staff

| Senior Answer | Staff Answer |
|---------------|--------------|
| "Flink has exactly-once semantics." | "Flink gives exactly-once FOR INTERNAL STATE via Chandy-Lamport checkpoints. End-to-end exactly-once requires: (1) a replayable source committing offsets with checkpoints (Kafka, Kinesis); (2) a transactional sink doing 2PC (Kafka, iceberg with careful handling) OR an idempotent sink (UPSERT by key). Checkpoints are the mechanism; the whole pipeline must cooperate. For teams that confuse 'exactly-once at Flink' with 'exactly-once end-to-end', the distinction matters for correctness." |
| "Use RocksDB for large state." | "RocksDB state backend writes to local disk via LSM-tree; incremental checkpoints only transfer deltas. For 100 GB+ keyed state, RocksDB is almost mandatory — heap-based would require too much memory and OOM easily under load. Trade-offs: RocksDB reads hit disk; heap is faster for sub-ms access. Tune: write buffer size, compaction thresholds, bloom filter settings. Flink 2.0's disaggregated state is the next step — state on S3 with aggressive local caching, decoupling state size from compute size." |
| "Watermarks handle late data." | "Watermarks ASSERT 'no events with timestamp < W will arrive.' Flink inserts watermarks at sources; downstream operators take MIN across upstream subtasks (strongest guarantee). Windows close when watermark passes end + allowed-lateness. Events arriving after allowed-lateness go to side-output (don't drop silently). Setting allowed-lateness is a product decision: measure actual lateness distribution, set to cover p99.9. Too strict: lose data; too loose: memory bloat, delayed results. The staff-level signal is treating watermark tuning as a quantified ops decision, not a default." |

### 30. War Stories

> **🔥 Production Scenario — The Checkpoint Catastrophe.** A team ran Flink with 10-second checkpoints and 50 GB keyed state. Checkpoints routinely took 30+ seconds, causing backpressure. Fix: (1) enabled incremental checkpoints — now only state deltas transfer; (2) increased interval to 60s; (3) tuned S3 upload parallelism. Checkpoint duration dropped to 8s. Lesson: checkpoints are not free; monitor duration vs interval ratio.

> **🔥 Production Scenario — The State Explosion.** A deduplication job used ValueState<Boolean> per unique event_id, no TTL. After 6 months: 2 TB of state; checkpoint failures; eventual job crash. Fix: added TTL of 24h (dedup window was never longer anyway) + used MapState for better memory. State dropped to 50 GB; stable. Lesson: every piece of keyed state needs an eviction story.

> **🔥 Production Scenario — The Watermark Stall.** One Kafka partition had no data for an hour (producer bug). Watermark advanced normally in other partitions but overall watermark = MIN across partitions → stuck. Downstream windows never closed. Fix: added `withIdleness(Duration.ofMinutes(5))` to the watermark strategy — partitions idle > 5 min stop contributing to watermark MIN. Lesson: idle partitions stall watermarks; always configure idleness.

> **🔥 Production Scenario — The Savepoint Migration.** A team upgraded Flink 1.14 → 1.17 and planned to use savepoints for migration. The savepoint restored but one operator's state deserialization failed due to a serializer change. 2-hour outage. Fix: migrated to Flink's new "unified savepoint" format first (which is forward-compatible), then upgraded. Lesson: Flink upgrades + savepoint compatibility deserve a test environment and runbook; don't skip rehearsal.

> **🔥 Production Scenario — The Backpressure Mystery.** A pipeline had intermittent backpressure despite low CPU and memory. Investigation: the sink (JDBC to Postgres) was slow during Postgres's VACUUM window. Flink's backpressure was correctly protecting from DB overload, but downstream SLA was breached. Fix: scheduled VACUUM outside the sink's peak hours; added async I/O to the sink with controlled concurrency. Lesson: backpressure is a signal, not just a performance issue — understand what's actually slow.

### 31. Cheat Sheet & Mental Models

#### 31.1 The One-Minute Pitch

```
Apache Flink = production-grade streaming engine with:
   • Exactly-once state via Chandy-Lamport checkpoints
   • Event-time processing with watermarks
   • Scalable state (RocksDB, TB per operator)
   • Low latency (sub-100ms achievable)
   • Unified batch + stream in one API

Pick for: real streaming (not micro-batch), large state, event-time correctness.
Avoid when: batch-only; Spark team's comfort zone; sub-GB state in simple apps.

Managed options: Confluent Cloud Flink, AWS KDA, Ververica, Dataflow, Alibaba RT.
```

#### 31.2 The Four Lenses — Compressed

```
CONCEPT
   Distributed streaming engine. State + time + exactly-once as first-class.

MECHANICS
   DataFlow graph of operators → subtasks on slots on TaskManagers.
   State in RocksDB; checkpoints via Chandy-Lamport barriers.
   Watermarks propagate; windows close when watermark passes end + lateness.

TRADE-OFFS
   Pros: sub-second latency, large state, exactly-once, mature.
   Cons: operational cluster (vs Kafka Streams library), steep learning curve,
         JVM-first (Python is wrapper).

INTERVIEW
   Know: checkpoint mechanism, watermarks, state backends, exactly-once E2E.
   Defend: Flink vs Spark Streaming vs Kafka Streams for a given workload.
```

#### 31.3 Essential Commands

```bash
# Run a job
flink run my-job.jar

# Submit with parallelism
flink run -p 20 my-job.jar

# Stop with savepoint
flink stop --savepointPath s3://.../savepoints/ <jobId>

# Restore from savepoint
flink run -s s3://.../savepoints/sp-xxxx -p 20 my-job.jar

# List jobs
flink list

# Cancel a job
flink cancel <jobId>

# Trigger savepoint (while running)
flink savepoint <jobId> s3://.../savepoints/
```

#### 31.4 Mental Model

```
Think of Flink as:
   • The streaming database engine
   • State + time + exactly-once as first-class citizens
   • Running continuously, not in batches
   • The production answer when "streaming" is a serious requirement

When to reach for it:
   • Sub-second latency with correctness
   • Large keyed state (GB-TB)
   • Event-time windowing with watermarks
   • Exactly-once end-to-end

When to reach past it:
   • Micro-batch is fine → Spark Streaming
   • Kafka-only, lightweight → Kafka Streams
   • Simple ETL, no state → Spark batch or DuckDB

Modern lakehouse: Flink → Iceberg is becoming the production streaming default.
```

> **🧠 Principal Mindset:** Flink's position in 2026 is dominant in serious streaming, but the operational complexity remains a real cost. Many teams start with Spark Streaming because the team already knows Spark. That's fine until latency or state scale forces the move. The staff-level signal: know when Flink is the right call AND know its ops burden. Managed Flink (Confluent, AWS KDA) is closing that burden gap — don't rule it out because of "we don't want to operate a cluster." If the workload demands Flink's capabilities, use it; just use a managed version if ops bandwidth is limited.

---

[← Back to main](./README.md)
