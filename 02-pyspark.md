# Topic 2 — PySpark

> Section 2 of 10 · 58 pages in source PDF · pages 92–149

[← Back to main](./README.md)

---

PySpark Architect-Level Reference Principal / Staff Engineer Interview Notes Catalyst · Tungsten · Shuffle · Partitioning · AQE · Memory · Skew · Joins · Streaming · Delta · Cost The distributed-compute companion to SQL: how Spark actually runs your job.

How to use this document PySpark hides an enormous amount of complexity behind a DataFrame API that looks like Pandas. This is a feature for productivity and a trap for staff-level interviews. What's tested is not 'can you call df.groupBy' — it's whether you can reason about partitioning, shuffles, memory, skew, spill, and the Catalyst plan when something is slow at TB scale. ■ Principal mindset: Interviewers test three things: (1) can you read a Spark plan and predict its behavior, (2) can you debug a job that runs in dev but OOMs in prod, (3) do you know when NOT to use Spark. Get those three right and the rest is detail.

Table of Contents

## PART I — FOUNDATIONS

#### 1. Why Spark Exists & When to Use It

#### 2. The Spark Architecture: Driver, Executors, Cluster Managers

#### 4. The DataFrame API: Transformations & Actions

## PART II — EXECUTION INTERNALS

#### 5. Lazy Evaluation, DAG & Stages

#### 6. Catalyst Optimizer Deep Dive

#### 7. Tungsten: Memory & Code Generation

#### 8. The Shuffle: Spark's Most Important Operation

#### 9. Reading the Spark UI & Plans

## PART III — PARTITIONING & DATA LAYOUT

#### 10. Partitions, Files & Parallelism

#### 11. Repartition vs Coalesce

#### 12. Bucketing & Sort Columns

#### 13. File Formats: Parquet, ORC, Avro, Delta

## PART IV — JOINS, AGGREGATIONS & SKEW

#### 14. Join Strategies: Broadcast, SMJ, Shuffle Hash

#### 15. Aggregations & Window Functions

#### 16. Data Skew: Detection & Mitigation

#### 17. Adaptive Query Execution (AQE)

## PART V — PERFORMANCE & MEMORY

#### 18. Memory Management & OOM Patterns

#### 19. Caching, Persistence & Checkpointing

#### 21. Tuning Playbook

## PART VI — STREAMING & LAKEHOUSE

#### 22. Structured Streaming Fundamentals

#### 23. State, Watermarks & Exactly-Once

#### 24. Delta Lake & Lakehouse Patterns

## PART VII — PRODUCTION & SCALE

#### 25. Cluster Sizing & Cost Modeling

#### 26. Common Failure Modes & Recovery

#### 27. Anti-Patterns at Scale

## PART VIII — INTERVIEW PRACTICE

#### 28. The Staff-Level Spark Interview

#### 29. War Stories

#### 30. Cheat Sheet & Mental Models

## PART I — FOUNDATIONS

#### 1. Why Spark Exists & When to Use It

1.1 The Problem Spark Solved Hadoop MapReduce (2006) gave us distributed batch computation, but every job wrote intermediate results to HDFS. A pipeline of 5 MapReduce jobs meant 5 disk round-trips — fine for nightly batch, useless for iterative algorithms (ML, graph) or interactive analytics. Spark (Berkeley AMP Lab, 2009; Apache top-level 2014) keeps intermediates in memory across stages, often 10–100× faster than MR for iterative workloads. KEY INNOVATIONS: 1. RDD (Resilient Distributed Dataset) — in-memory, fault-tolerant collections 2. Lazy DAG execution — optimizer can see the whole job

#### 3. Unified API — batch, SQL, streaming, ML, graph all on one engine

```
1.2 When NOT to Use Spark
DATA FITS ON ONE MACHINE (<100 GB)
  → Use Pandas, Polars, DuckDB. Spark overhead alone eats your speed budget.
  → Spark on a single node: JVM startup, driver overhead, no parallelism gain.
LOW-LATENCY POINT QUERIES
  → Use Postgres / DynamoDB / Redis. Spark is throughput-optimized, not latency.
  → "Get user by ID" in 50ms is not a Spark workload.
HIGHLY TRANSACTIONAL WORKLOAD
  → Spark has no native transactions. Delta Lake adds ACID on top, but it's
    optimized for append/merge, not row-level OLTP.
SIMPLE SQL ON A WAREHOUSE
  → Snowflake / BigQuery / Redshift outperform Spark for ad-hoc SQL.
  → Spark wins when you need code (UDFs, ML, complex transforms).
REAL-TIME (<100ms latency)
  → Use Flink. Spark Structured Streaming is micro-batch
    (typical latency: 100ms – seconds).
```

1.3 When Spark IS the Right Tool ✓ ETL over TB+ of data with complex transforms in Python/Scala ✓ Iterative ML on large datasets (MLlib, or distributed training prep) ✓ Reading from many disparate sources (S3, Kafka, JDBC, Cassandra, Iceberg) ✓ Mixing SQL + code (window functions + custom UDF + ML scoring) ✓ Streaming with sub-minute latency tolerance ✓ Lakehouse patterns: Delta/Iceberg/Hudi for ACID on object storage ✓ Multi-source joins where data lives in places SQL warehouses can't reach ■ Principal mindset: Most companies use Spark for the wrong thing. If your jobs are 'SELECT a, SUM(b) FROM big_parquet GROUP BY a', the warehouse does that better and cheaper. Spark earns its keep when you need code in your pipeline — custom transformations, ML, multi-source joins, or lakehouse table operations. ■ Interview angle: Q: When would you choose Spark over Snowflake/BigQuery? Staff: When the work requires code, not just SQL. Custom UDFs (especially Pandas UDFs), ML feature engineering, reading from sources the warehouse can't (Kafka, Cassandra, complex APIs), or lakehouse operations

(Delta MERGE, schema evolution, time travel) where compute and storage are separate. For pure SQL workloads on tabular data, warehouses win on cost and ergonomics. Many teams should NOT be using Spark.

#### 2. The Spark Architecture

```
2.1 The Components
■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■
■                      DRIVER (your code)                      ■
■  ■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■   ■
■  ■  SparkContext / SparkSession                          ■   ■
■  ■    Builds the DAG                                    ■   ■
■  ■    Catalyst optimizes                                ■   ■
■  ■    Schedules tasks to executors                      ■   ■
■  ■    Tracks task progress, retries                     ■   ■
■  ■    Collects results from actions                     ■   ■
■  ■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■   ■
■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■
                         ■ talks to
                         ▼
              ■■■■■■■■■■■■■■■■■■■■■■■■
              ■  CLUSTER MANAGER     ■
              ■  (YARN / K8s /       ■
              ■   Standalone /       ■
              ■   Mesos / Databricks)■
              ■    Allocates cores  ■
              ■    Manages executor ■
              ■     lifecycle        ■
              ■■■■■■■■■■■■■■■■■■■■■■■■
                         ■
        ■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■
        ▼                ▼                ▼
   ■■■■■■■■■■■      ■■■■■■■■■■■      ■■■■■■■■■■■
   ■EXECUTOR ■      ■EXECUTOR ■      ■EXECUTOR ■
   ■  JVM    ■      ■  JVM    ■      ■  JVM    ■
   ■  runs  ■      ■  runs  ■      ■  runs  ■
   ■   tasks ■      ■   tasks ■      ■   tasks ■
   ■  holds ■      ■  holds ■      ■  holds ■
   ■   cache ■      ■   cache ■      ■   cache ■
   ■■■■■■■■■■■      ■■■■■■■■■■■      ■■■■■■■■■■■
```

```sql
2.2 Driver Responsibilities
 Maintain SparkContext / SparkSession
 Build the logical plan from your DataFrame code
 Run Catalyst to produce the physical plan
 Split into JOBS → STAGES → TASKS
 Send serialized tasks to executors
 Collect results back (this is where collect() can OOM the driver)
 Web UI (default port 4040)
DRIVER FAILURE = JOB FAILURE
  Driver is a single point of failure. If it dies, the whole job dies.
  In Databricks/EMR, driver lives on a dedicated node — don't run heavy
  Python code there.
```

2.3 Executor Responsibilities  JVM process started by the cluster manager  Multiple TASK SLOTS per executor (one per core)  Holds: - Cached/persisted RDDs/DataFrames in its block manager - Shuffle output (until consumed) - Broadcast variables  Runs Python workers (forked subprocesses) for Python UDFs

2.4 Cluster Managers Compared Manager Strength Weakness YARN Mature, Hadoop ecosystem integration Heavyweight; declining Kubernetes Native containers, cloud-portable Operationally complex Standalone Simple, no extra infra Limited scheduling Databricks Managed, autoscaling, optimized Vendor lock-in, $$ EMR / Glue AWS-managed Spark Less flexible than DB

```
2.5 Deploy Modes
CLIENT MODE
  Driver runs ON THE MACHINE THAT SUBMITTED the job (e.g., your laptop).
   Good for: interactive (notebooks, spark-shell)
   Bad for: production — your laptop closing kills the job
CLUSTER MODE
  Driver runs INSIDE the cluster (on a worker node).
   Good for: production batch
   Job survives client disconnect
LOCAL MODE  (master="local[*]")
  Single JVM, no cluster — for development/tests.
  No real parallelism beyond cores; no shuffle network.
■ Interview angle: Q: Your Spark job's driver runs out of memory. What's happening?
Staff: A few possibilities. (1) Someone called .collect() or .toPandas() on a large DataFrame — pulls all rows
to driver. (2) Broadcast variable too large — by default, broadcast joins use 10MB threshold; if AQE auto-broadcasts
a 2GB table the driver materializes it. (3) Driver-side Python code holding state across iterations. (4) Too many tasks
(10K+ partitions) — task metadata alone consumes driver memory. Diagnosis: check Spark UI 'Storage' and
'Executors' tabs, then look at the driver heap with jmap or jstat.
```

3. RDDs, DataFrames, and Datasets

3.1 The Three APIs API Year Strengths When to use RDD 2009 Full control, type-safe (Scala/Java) Custom partitioner, low-level DataFrame 2015 Catalyst optimization, columnar Default for everything Dataset 2016 DF + compile-time types (Scala only) Scala typed pipelines In PySpark, the practical choice is DataFrame (= Dataset[Row]). RDDs are still there but you almost never need them in 2024+. Datasets are Scala-only.

3.2 Why DataFrame Wins Over RDD RDD DataFrame ■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■ Opaque to Spark — black-box lambdas Catalyst sees the whole expression tree No schema Explicit schema, columnar storage Row-by-row processing Vectorized via Tungsten No partition awareness for joins Optimizer rewrites joins, pushes predicates Serializes Python objects Stays in JVM (no Python serialization unless you call a Python UDF) Manual partitioning Automatic, AQE-tuned Same code on all languages, slow Same code on all languages, equally fast RULE: only drop to RDD when you genuinely need custom partitioning, low-level control, or you're working with non-tabular data (e.g., text streams before tokenization).

3.3 The PySpark Speed Trap DataFrame operations stay in the JVM and Catalyst optimizes them. The moment you call .rdd, use a regular Python UDF, or call .foreach with Python code, Spark must:

#### 1. Serialize each row to Python (pickle)

#### 2. Fork a Python worker process

#### 3. Send rows over a socket to Python

#### 4. Receive results

5. Deserialize back to JVM Cost: 10-100× slowdown vs native DataFrame operations. This is why PySpark and Scala Spark perform IDENTICALLY when you use DataFrame ops only. They diverge only when Python code runs. ■■ Warning: The #1 PySpark performance mistake: writing a Python UDF for something that exists as a built-in pyspark.sql.functions. Always check the F-namespace first.

```python
3.4 The Modern PySpark Cheatsheet
from pyspark.sql import SparkSession, functions as F, Window as W
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
spark = (SparkSession.builder
    .appName("my_job")
    .config("spark.sql.adaptive.enabled", "true")
    .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
    .config("spark.sql.shuffle.partitions", "200")
    .getOrCreate())
```

```python
df = spark.read.parquet("s3://bucket/path/")
df2 = (df
    .filter(F.col("status") == "active")
    .withColumn("year", F.year("created_at"))
    .groupBy("year", "country")
    .agg(F.sum("amount").alias("total"),
         F.countDistinct("user_id").alias("uniq_users"))
    .orderBy(F.desc("total")))
df2.write.mode("overwrite").parquet("s3://bucket/out/")
```

#### 4. The DataFrame API: Transformations & Actions

```
4.1 The Fundamental Distinction
TRANSFORMATIONS  — lazy. Build the DAG. Return a new DataFrame.
                  No work done until an action runs.
  Examples: select, filter, withColumn, groupBy, join, repartition,
            distinct, dropDuplicates, union
ACTIONS          — eager. Trigger execution. Return values or write data.
  Examples: count, collect, show, take, first, write, foreach,
            toPandas, saveAsTable
EVERY ACTION = A NEW JOB
  count() on a DataFrame doesn't reuse work from a previous count() unless
  you've cached/persisted it explicitly.
```

```
4.2 Narrow vs Wide Transformations
NARROW (1-to-1 partition mapping, no shuffle):
  filter, select, withColumn, map, flatMap, union, drop
WIDE (require shuffle — moving data across executors):
  groupBy, orderBy, distinct, dropDuplicates, repartition,
  join (unless broadcast), window functions
THE STAGE BOUNDARY IS THE SHUFFLE
  Spark splits the DAG into stages at every wide transformation.
  Within a stage, all transformations pipeline through one task per partition.
```

```
4.3 Common Transformations
# COLUMN OPERATIONS
df.select("a", "b", F.col("c").alias("cc"))
df.withColumn("new", F.col("a") + F.col("b"))
df.withColumnRenamed("old_name", "new_name")
df.drop("col1", "col2")
# FILTERING
df.filter(F.col("x") > 10)              # equivalent to .where()
df.filter("x > 10 AND status = 'active'")  # SQL string
# DEDUP
df.distinct()                            # all columns
df.dropDuplicates(["user_id"])           # subset
df.dropDuplicates(["user_id"])           # nondeterministic which row kept!
# DETERMINISTIC DEDUP — use windowing
w = W.partitionBy("user_id").orderBy(F.desc("updated_at"))
df.withColumn("rn", F.row_number().over(w)).filter("rn = 1").drop("rn")
# GROUPING
df.groupBy("country").agg(
    F.sum("amount").alias("total"),
    F.avg("amount").alias("avg"),
    F.countDistinct("user_id").alias("uniq")
)
# SORTING (wide!)
df.orderBy(F.desc("amount"))            # global sort - expensive at scale
df.sortWithinPartitions("amount")        # narrow - sort within each partition
```

#### 4.4 Common Actions & Their Costs

# CHEAP ACTIONS df.show(5) # samples one partition, may not be deterministic df.take(10) # returns 10 rows to driver df.first() # returns one row # EXPENSIVE — full scan df.count() # scans all partitions (no aggregation pushdown) df.collect() # ■■ pulls ALL rows to driver. OOM trap. df.toPandas() # ■■ same, but converts to Pandas. Use sparingly. # I/O ACTIONS df.write.mode("overwrite").parquet("path") df.write.partitionBy("date").mode("append").parquet("path") df.write.bucketBy(64, "user_id").saveAsTable("orders") ■■ Warning: .collect() and .toPandas() are the most common cause of driver OOM. Always filter and limit first. For moving DF to Pandas, prefer spark.conf.set('spark.sql.execution.arrow.pyspark.enabled', 'true') first — Arrow-based conversion is 10–100× faster. ■ Interview angle: Q: What's the difference between narrow and wide transformations, and why does it matter? Staff: Narrow means each input partition maps to one output partition, so no data movement across executors — Spark can pipeline them within a single stage. Wide means data must be shuffled (sorted/hashed across executors), which creates a stage boundary. Shuffles are the dominant cost in Spark jobs. Knowing which ops are wide is how you predict performance — and how you design pipelines to minimize them: bucketing pre-shuffles data; broadcast joins avoid shuffle entirely; repartition trades one shuffle now for cheaper ops later.

## PART II — EXECUTION INTERNALS

#### 5. Lazy Evaluation, DAG & Stages

```
5.1 The Job → Stage → Task Hierarchy
JOB    = one action (.count(), .write, .collect())
STAGE  = set of transformations between shuffles
TASK   = unit of work = one stage × one partition
EXAMPLE:
  df.filter(...).groupBy("a").sum("b").write.parquet(...)
  → 1 JOB (the write)
  → 2 STAGES:
      Stage 0: read + filter (narrow) → produces shuffle output
      Stage 1: read shuffle + sum + write
  → N TASKS per stage (N = number of partitions)
If Stage 0 has 200 partitions and you have 50 cores, Spark runs
50 tasks in parallel, four waves of work.
```

#### 5.2 The DAG Scheduler

#### 1. Driver builds the LOGICAL PLAN from your DataFrame transformations

#### 2. Catalyst optimizes → PHYSICAL PLAN

```
  4. For each stage:
     a. Compute task locations (data locality)
     b. Submit task set to Task Scheduler
     c. Task Scheduler dispatches to executors
```

#### 5. Executors run tasks, write shuffle output if not last stage

#### 6. Next stage reads previous stage's shuffle output

#### 7. Final stage returns results / writes output

5.3 Pipelining Within a Stage All narrow transformations in a stage are pipelined — fused into one loop per partition. Reading a row, filtering it, computing a column, and emitting to the next stage happens row-by-row in memory, no materialization between steps. This is why narrow ops are essentially free compared to shuffles.

5.4 What Triggers a New Stage  Any wide transformation: groupBy, join (non-broadcast), distinct, orderBy, window, repartition, partitionBy on write  Reading from a source that requires a separate stage (some file formats with schema inference)  Calling .cache() or .persist() creates a stage boundary for caching

5.5 Examining the DAG # Logical and physical plans df.explain() # physical plan only (default) df.explain(True) # parsed, analyzed, optimized, physical df.explain("formatted") # tree view with operator details (best for reading)

df.explain("cost") # with cost-based stats (when CBO enabled) # Programmatically df._jdf.queryExecution().toString() # raw plan string # Spark UI > SQL tab — visual DAG with stage timings ■ Principal mindset: Reading a physical plan fluently is a staff-level skill. Look for: Exchange (= shuffle), SortMergeJoin vs BroadcastHashJoin, Filter pushed below Project (predicate pushdown), and number of partitions at each Exchange. The plan tells you where the cost is BEFORE you run the job.

#### 6. Catalyst Optimizer Deep Dive

```
6.1 The Catalyst Pipeline
Your DataFrame code
    ■
    ▼
PARSED LOGICAL PLAN     (syntax tree, unresolved references)
    ■
    ▼  Analyzer (resolves columns, types, functions via catalog)
ANALYZED LOGICAL PLAN
    ■
    ▼  Optimizer (rule-based rewrites)
OPTIMIZED LOGICAL PLAN
    ■
    ▼  Strategy (cost-based selection of physical operators)
PHYSICAL PLAN
    ■
    ▼  Code generation (Whole-Stage CodeGen)
JAVA BYTECODE → executors
```

```sql
6.2 Key Rule-Based Optimizations
 PREDICATE PUSHDOWN
   df.join(...).filter(x > 10)  →  push filter below join when safe
   For Parquet/Delta: filter pushed all the way to the file reader
 PROJECTION PRUNING
   df.select("a", "b").filter(...)
   → only column a and b are read from disk
 CONSTANT FOLDING
   F.lit(2) + F.lit(3)  →  F.lit(5) at compile time
 COMBINE FILTERS / PROJECTS
   .filter(a > 1).filter(b < 5)  →  .filter(a > 1 AND b < 5)
 ELIMINATE SUBQUERIES
   Inlined when safe (similar to SQL CTE inlining)
 JOIN REORDER (CBO enabled)
   Pick join order that minimizes intermediate sizes
 PUSHDOWN TO DATA SOURCES
   Parquet: column pruning, row group filtering by min/max stats
   JDBC: WHERE pushed to remote SQL
   Delta/Iceberg: full predicate + partition pushdown
```

```
6.3 Cost-Based Optimizer (CBO)
CBO uses table & column STATISTICS to:
   Pick join order
   Pick join algorithm (broadcast vs SMJ vs shuffle hash)
   Pick aggregation strategy
ENABLE:
  spark.conf.set("spark.sql.cbo.enabled", "true")
  spark.conf.set("spark.sql.cbo.joinReorder.enabled", "true")
COLLECT STATISTICS:
  ANALYZE TABLE my_table COMPUTE STATISTICS;                  -- table-level
  ANALYZE TABLE my_table COMPUTE STATISTICS FOR COLUMNS a, b; -- column-level
WITHOUT STATS, Spark uses heuristics — often picks SMJ when broadcast
```

would have been cheaper. CBO matters most for large DW-style queries.

6.4 Whole-Stage Code Generation Catalyst doesn't just plan — it generates Java bytecode at runtime for each stage. Instead of interpreting a tree of operators, it compiles filter(x > 10).select(a, b) into a single Java method with a tight loop. This is Whole-Stage CodeGen (introduced in Spark 2.0). Result: often 2–10× faster than the previous interpreter model. # See the generated code df.filter("x > 10").select("a", "b").queryExecution.debug.codegen # Disable for debugging spark.conf.set("spark.sql.codegen.wholeStage", "false") # Generated code is JIT-compiled by the JVM after sufficient invocations, # so the first batch of a partition is slower than subsequent batches. ■ Interview angle: Q: A Spark job is slow. EXPLAIN shows SortMergeJoin where you expected BroadcastHashJoin. Why didn't the optimizer pick broadcast? Staff: Several possibilities. (1) The smaller side is above spark.sql.autoBroadcastJoinThreshold (default 10MB) — either raise it or use broadcast(df) hint. (2) Stats are missing — without ANALYZE, Spark estimates size from input file sizes, which over-estimates after filters. Fix: enable CBO + collect stats. (3) AQE wasn't enabled, so Spark couldn't see runtime sizes after filters. (4) The small side wasn't actually small (DataFrame after wide transformations has unknown size). Hint or cache+count to force materialization.

#### 7. Tungsten: Memory & Code Generation

```
7.1 Why Tungsten Exists
Spark 1.x stored rows as JVM objects. Java objects have ~12-byte headers per object plus pointer chasing
— at scale, garbage collection dominates CPU. Project Tungsten (Spark 1.5+) rewrote the execution engine
to use off-heap, binary-encoded memory and CPU-cache-friendly layouts.
TUNGSTEN PILLARS:
  1. OFF-HEAP MEMORY MANAGEMENT
     Spark manages its own memory outside the JVM heap
     → no GC overhead, predictable behavior
  2. BINARY ROW FORMAT (UnsafeRow)
     Rows stored as packed bytes, not Java objects
     → 5-10× less memory per row, cache-friendly
  3. CACHE-AWARE COMPUTATION
     Sort, hash table, aggregation use cache-line-aligned layouts
  4. WHOLE-STAGE CODEGEN  (see Ch 6)
     Compiled inner loops, no virtual call overhead
```

```
7.2 The Memory Regions
EXECUTOR MEMORY (set by spark.executor.memory, default 1G)
■■■ Reserved Memory  (300 MB, system reserve)
■■■ User Memory      (40% of (executor - reserved))
■   ■■■ Your Python UDF state, broadcast vars stored on executor
■■■ Spark Memory     (60% of (executor - reserved), unified pool)
    ■■■ EXECUTION    — joins, sorts, aggregations (shuffles)
    ■■■ STORAGE      — cached DataFrames
UNIFIED MEMORY (Spark 1.6+)
  Execution and Storage borrow from each other dynamically.
  Cached data can be evicted to make room for shuffle/sort.
  Spark 3+ also supports OFF-HEAP execution memory.
OFF-HEAP MEMORY  (optional, spark.memory.offHeap.enabled)
  Allocated via sun.misc.Unsafe, outside JVM heap.
  Reduces GC pressure for shuffle-heavy jobs.
```

```sql
7.3 Spill
When execution memory is exhausted, Spark spills intermediate state to disk. This is graceful degradation
— the job completes — but performance collapses. Spill is visible in the Spark UI as 'Shuffle Spill (Memory)'
and 'Shuffle Spill (Disk)'.
COMMON CAUSES OF SPILL:
   Aggregation with too many distinct groups
   Sort with insufficient memory per task
   Hash join with build side too large
   Skewed partitions (one task has 10x more data)
FIXES:
   Increase spark.executor.memory
   Increase number of partitions (smaller per-task data)
   Switch from groupBy to reduceByKey (RDD) or use AQE skew handling
   Use disk-based shuffle (default) with fast local SSDs
■ Production scenario: A job processing 2TB of clickstream data ran for 6 hours and OOM'd. The Spark UI
showed 95% of tasks finishing in 30 seconds and 5% running for 90+ minutes with multi-GB disk spill. Diagnosis:
```

skew on user_id — bot accounts had millions of events. Fix: salted the join key (Ch 16) — added a random number 0-9 to the bot accounts on both sides. Job dropped to 25 minutes.

#### 8. The Shuffle: Spark's Most Important Operation

8.1 What is a Shuffle? A shuffle redistributes data across executors so that all rows with the same key end up in the same partition. It's the only way to do groupBy, joins, or sorts on distributed data. Shuffles are expensive — they involve disk I/O, network I/O, serialization, and stage synchronization. Optimizing Spark = minimizing shuffles.

```
8.2 Anatomy of a Shuffle
STAGE BOUNDARY
    ■
    ▼
MAP SIDE  (in the upstream stage)
  Each task:
```

#### 1. Compute output rows

#### 2. Compute target partition (hash(key) % numPartitions)

#### 3. Write rows to LOCAL DISK in N files (one per target partition)

#### 4. Sort within each file (for sort-based shuffle, default)

```
    5. Build an INDEX file mapping partition → file offset
    ■
    ▼  Stage finishes; shuffle output files are persisted on each executor's disk
REDUCE SIDE  (in the downstream stage)
  Each task:
```

#### 1. Fetch its partition's data from EVERY upstream executor over network

#### 2. Merge sorted streams

3. Process (aggregate, join, sort) KEY INSIGHT: Map output is written to disk, not memory. This is for fault tolerance — if a reducer fails, it can re-fetch without re-running the map. But it means shuffle is bounded by disk throughput, not RAM.

```
8.3 Shuffle Configuration
spark.sql.shuffle.partitions = 200       # default; controls reduce-side count
spark.default.parallelism = N            # RDD-only; not for DataFrame
# Compression
spark.shuffle.compress = true
spark.shuffle.spill.compress = true
spark.io.compression.codec = "lz4"       # lz4 > snappy for shuffle
# Network
spark.reducer.maxSizeInFlight = 48m      # per-reducer fetch buffer
spark.reducer.maxReqsInFlight = Integer.MAX_VALUE
# Manager
spark.shuffle.manager = "sort"           # default; "tungsten-sort" was removed
# Service
spark.shuffle.service.enabled = true     # external shuffle service (YARN)
                                          # lets executors die without losing shuffle
```

```
8.4 Why 200 Partitions Is Almost Always Wrong
DEFAULT: spark.sql.shuffle.partitions = 200
  → After ANY shuffle, output has 200 partitions.
WHY THIS IS USUALLY WRONG:
   Small job (10 GB total): 200 partitions = 50 MB each. OK.
   Large job (1 TB total):  200 partitions = 5 GB each. Skew, spill, OOM.
```

```
   Tiny job (100 MB total): 200 partitions = 500 KB each.
                             Most tasks idle, schedule overhead dominates.
RULE OF THUMB:
  Target partition size: 100-200 MB after shuffle.
  Total partitions = total shuffle size / 128 MB
AQE FIXES THIS DYNAMICALLY (Ch 17)
  spark.sql.adaptive.coalescePartitions.enabled = true
  Spark right-sizes partition count after seeing actual shuffle output.
```

```
8.5 The External Shuffle Service
WITHOUT external shuffle service:
  Executor holds shuffle output.
  Executor dies → all its shuffle data lost → upstream stages must rerun.
WITH external shuffle service (recommended):
  A separate process on each worker holds shuffle output.
  Executor dies → shuffle survives → only failed tasks rerun.
```

Required for: dynamic allocation, spot instances, executor preemption. ENABLE: spark.shuffle.service.enabled = true spark.dynamicAllocation.enabled = true # requires it ■ Interview angle: Q: A 1TB job has 200 shuffle partitions and OOMs. What do you do? Staff: Each partition is ~5GB on average, more with skew. Three approaches. (1) Increase shuffle partitions to 5000-8000 so each is ~128-200MB. (2) Enable AQE with coalesce — let Spark right-size dynamically. (3) Check for skew with a salt-aggregate; if one partition is 10x larger, no number of partitions fixes it. Long-term: tune for ~150MB per shuffle partition as a target; that's the sweet spot for most workloads.

#### 9. Reading the Spark UI & Plans

```
9.1 The Spark UI Tabs
JOBS TAB
   List of jobs with duration, stages, tasks
   Click into job → DAG visualization
   Look for: failed jobs, jobs taking longer than expected
STAGES TAB
   Per-stage detail
   DURATION breakdown (compute, shuffle read/write, serialization)
   Per-task timeline — see straggler tasks
   "Shuffle Read Size" / "Shuffle Write Size"
   "Shuffle Spill (Memory)" / "Shuffle Spill (Disk)" — RED FLAGS
   Skew shown as: max task duration >> median
SQL / DATAFRAME TAB
   Visual query plan with metrics overlaid
   Best place to spot: Exchange (shuffle), SortMergeJoin vs BHJ,
    BroadcastExchange (broadcast cost)
   Per-operator: rows in/out, time taken, memory used
EXECUTORS TAB
   Memory used per executor
   GC time (high = problem)
   Task counts: succeeded, failed, killed
   Storage memory used (cached data)
STORAGE TAB
   Cached DataFrames
   Fraction cached in memory vs spilled to disk
ENVIRONMENT TAB
   All Spark configs (verify what's actually set)
```

  - 9.2 The Six Numbers That Matter
- For each STAGE, look at:
- 1. INPUT SIZE          — data read from source
- 2. SHUFFLE READ SIZE   — data fetched over network
- 3. SHUFFLE WRITE SIZE  — data written for next stage
- 4. SHUFFLE SPILL (DISK) — > 0 means out-of-memory degradation
- 5. TASK DURATION       — median vs max (skew indicator)
- 6. GC TIME             — > 10% of task time means memory pressure

```
9.3 Diagnosing Common Problems
SYMPTOM                        DIAGNOSIS
■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■
Few tasks, mostly idle         → too few partitions
                                  Fix: repartition, raise shuffle.partitions
Most tasks 30s, one task 30m   → skew
                                  Fix: salt key, AQE skew handling, broadcast
Large Shuffle Spill (Disk)     → execution memory too small per task
                                  Fix: more partitions, more executor memory
High GC time                   → too much memory pressure / object churn
                                  Fix: tune memory, use off-heap, avoid Python UDFs
SortMergeJoin where you wanted → small side over broadcast threshold
```

```
broadcast                        Fix: increase threshold or use broadcast() hint
Stage retries multiple times   → executor lost (preemption, OOM, network)
                                  Fix: enable shuffle service, lower task memory
OutOfMemoryError on driver     → collect(), toPandas(), or auto-broadcast
                                  Fix: filter before collect; raise driver memory
```

```
9.4 Reading the Plan: Annotated Example
== Physical Plan ==
*(3) HashAggregate(keys=[country], functions=[sum(amount)])
+- Exchange hashpartitioning(country, 200), ENSURE_REQUIREMENTS
   +- *(2) HashAggregate(keys=[country], functions=[partial_sum(amount)])
      +- *(2) Project [country, amount]
         +- *(2) BroadcastHashJoin [user_id], [user_id], Inner
            :- *(2) Filter (status = 'active')
            :  +- *(2) ColumnarToRow
            :     +- FileScan parquet orders[user_id,amount,status]
            :        Batched: true
            :        PushedFilters: [IsNotNull(status), EqualTo(status,active)]
            +- BroadcastExchange HashedRelationBroadcastMode(...)
               +- *(1) Filter isnotnull(user_id)
                  +- FileScan parquet users[user_id,country]
READING TOP-DOWN:
   Final HashAggregate is in stage 3 (the *(3))
   Exchange = the shuffle by country, 200 partitions
   Partial HashAggregate (stage 2) — pre-aggregates before shuffle (great!)
   BroadcastHashJoin — small side (users) broadcast to all executors
   PushedFilters — predicate pushed to Parquet reader (great!)
   ColumnarToRow — columnar Parquet → row format for processing
■ Principal mindset: Reading the Spark UI fluently is the single most powerful skill at staff level. When asked to
debug a slow job, walk through the Stages tab systematically: which stage took longest? What was its shuffle
read/write? Was there spill? What does the SQL tab show as the plan? You build credibility by being methodical, not
by guessing.
```

## PART III — PARTITIONING & DATA LAYOUT

#### 10. Partitions, Files & Parallelism

```
10.1 The Two Kinds of Partitioning
1. IN-MEMORY PARTITIONS (RDD/DataFrame partitions)
    How data is split across tasks at runtime
    Number = parallelism level
    Changed via repartition(), coalesce(), shuffle config
2. STORAGE PARTITIONING (files on disk)
    Directory layout: /year=2024/month=03/
    Enables PARTITION PRUNING at read time
    Created via .write.partitionBy("year", "month")
THESE ARE DIFFERENT THINGS. CONFUSING THEM CAUSES MOST PARTITIONING BUGS.
```

```
10.2 How Many Partitions?
GUIDELINE:
  In-memory partition target: 100-200 MB per partition
CALCULATION:
  num_partitions = max(
      total_input_size / 128 MB,
      total_cores * 2-4,                  # at least 2-4× cores for parallelism
      200                                 # minimum default
  )
READING PARQUET:
  Spark splits files by maxPartitionBytes (default 128MB)
  spark.conf.set("spark.sql.files.maxPartitionBytes", "256MB")  # tune
  Small files trap: 100k files of 1MB each → 100k tiny partitions → overhead kills you
  Fix: COALESCE or COMPACT before processing (or use Delta OPTIMIZE)
AFTER SHUFFLE:
  spark.sql.shuffle.partitions (default 200) — almost always wrong
  Use AQE to auto-tune.
```

```
10.3 The Small Files Problem
SYMPTOM:
  Reading a directory with thousands of small files.
  Spark UI: thousands of tasks, each finishing in <100ms.
  Total time dominated by task scheduling overhead.
CAUSES:
   Streaming jobs writing per-trigger files
   Many partitions × append-only writes
   Hive-style partitioning with high cardinality
FIXES:
   Compact at write time:
      df.coalesce(N).write.parquet(...)   # before write
   Periodic compaction job (read + rewrite)
   Delta Lake OPTIMIZE command
```

```
   Iceberg rewrite_data_files action
   spark.sql.files.maxPartitionBytes (groups files into larger partitions)
```

```
10.4 Storage Partitioning Patterns
df.write.partitionBy("date").parquet("s3://path/")
# Creates:
#   s3://path/date=2024
01/part-00000.parquet
#   s3://path/date=2024
02/part-00000.parquet
# QUERIES THAT PRUNE:
spark.read.parquet("s3://path/").filter("date = '2024
01'")
# → only reads one partition's files
# RULES OF THUMB:
#  Partition by columns frequently in WHERE
#  Aim for partitions of 1+ GB (not too small)
#  Avoid high-cardinality (don't partition by user_id with millions of users)
#  1-3 partition columns max (multi-level dirs get unwieldy)
# DELTA / ICEBERG offer better alternatives:
#    Z-ORDER (Delta) — multi-dim clustering within files
#    Hidden partitioning (Iceberg) — partition by date(ts) without dir
#    Data skipping via min/max stats in metadata
■ Production scenario: A team partitioned a streaming table by user_id (50M users). Each write created millions of
tiny files across millions of partitions. Reads hit S3 throughput limits. Storage cost ballooned (each file has
overhead). Fix: repartitioned by event_date with z-order on user_id. Read latency dropped 10×, storage costs by
80%.
```

#### 11. Repartition vs Coalesce

```
11.1 The Difference
repartition(N)
   Full shuffle to N partitions, evenly distributed
   Can INCREASE or decrease partition count
   Expensive (shuffle)
   Optionally repartition(N, col1, col2) — hash partition by columns
coalesce(N)
   No shuffle — collapses N partitions to M < N by merging adjacent ones
   Can only DECREASE partition count
   Cheap (no network)
   Result may be uneven (some partitions much larger than others)
USE CASES:
  Reducing for output:           coalesce(10)
  Rebalancing skewed partitions: repartition(200)
  Pre-shuffling for join:        repartition("user_id")
```

11.2 The Coalesce Trap # This looks reasonable but kills parallelism: df.coalesce(10).filter(complex_condition).map(expensive_udf).write.parquet(...) # Why? coalesce(10) propagates UPSTREAM — the filter and UDF # also run with 10 partitions. If df had 1000 partitions and you have # 100 cores, you're using 10% of available parallelism for the EXPENSIVE work. # Fix: do the expensive work first, coalesce only for output df.filter(complex_condition).map(expensive_udf).coalesce(10).write.parquet(...) # Or use repartition (forces shuffle to right-size): df.filter(complex_condition).map(expensive_udf).repartition(10).write.parquet(...) 11.3 repartitionByRange df.repartitionByRange(N, "col")  Partitions by RANGE of col values, not hash  Each partition gets a contiguous range  Useful for: subsequent sortWithinPartitions + write (sorted output files)  Skew-prone if col distribution is uneven (uses sampling to set ranges) ■ Interview angle: Q: What's the difference between repartition and coalesce? Senior: 'Repartition shuffles, coalesce doesn't.' Staff: 'Right, but the operational impact differs more than that. Coalesce propagates upstream — every stage feeding into coalesced output runs with the lower partition count, losing parallelism. So for filter().coalesce(10).write, the filter runs in 10 tasks even if you had 1000 partitions of input. Use repartition when you want a clean stage boundary with even distribution; coalesce only for right-sizing the final write where upstream parallelism doesn't matter.'

#### 12. Bucketing & Sort Columns

```sql
12.1 What is Bucketing?
Bucketing PRE-SHUFFLES data on disk by hash(col) % num_buckets.
df.write.bucketBy(64, "user_id").sortBy("event_time").saveAsTable("events")
Stored as:
  events/part
bucket=0.parquet
  events/part
bucket=1.parquet
  ...
  events/part
bucket=63.parquet
PROMISE:
  When two bucketed tables are joined on the bucket key with same N buckets,
  Spark can SKIP the shuffle on both sides — each bucket joins locally.
PRACTICAL VALUE:
   Big eliminate-shuffle wins when you join the same big tables repeatedly
   Common in DW workloads where dim/fact tables are written once, read many times
```

```
12.2 The Catch
 Only works in Spark-managed tables (saveAsTable, not raw paths)
 Requires reading WITH bucket awareness — must use catalog
 Number of buckets fixed forever (can't repartition without rewrite)
 Both sides of join need SAME bucket count and key
 Incompatible with most non-Spark tools (Trino, Snowflake, Athena)
→ Delta / Iceberg offer similar (Z-order, hidden partitioning) with more flexibility.
```

```
12.3 When to Use Bucketing
GOOD FIT:
  ✓ Same big tables joined repeatedly on same key
  ✓ Spark-only environment (Databricks, EMR with Spark catalog)
  ✓ Write once, read many
POOR FIT:
  ✗ Multi-tool environment (mix of Spark, Trino, Snowflake)
  ✗ Frequently-rewritten tables
  ✗ When Delta/Iceberg with Z-ORDER would do the job
■ Principal mindset: Bucketing is an old optimization. Modern lakehouse formats (Delta, Iceberg) provide similar
shuffle-elimination via Z-ORDER and partition transforms, with the advantage of being tool-neutral and rewritable.
Outside of large Databricks-only enterprises, you should default to Delta + Z-ORDER over bucketing.
```

#### 13. File Formats: Parquet, ORC, Avro, Delta

13.1 The Format Matrix Format Type Best for Pushdown Schema evo Parquet Columnar Analytics, general Excellent Append cols ORC Columnar Hive ecosystem Excellent Append cols Avro Row Streaming, schema reg None Excellent JSON Row text Debugging, semi-struct None Implicit CSV Row text Interop only None None Delta Parquet+log Lakehouse: ACID + Parquet Excellent Full DDL Iceberg Parquet+meta Lakehouse: open Excellent Full DDL Hudi Parquet+log Lakehouse: upserts Excellent Full DDL

```
13.2 Why Parquet Dominates
1. COLUMNAR
   Each column stored separately → only read columns you SELECT
   100-column table, querying 3 cols → 97% I/O reduction
2. COMPRESSION
   Same-column values are similar → high compression ratios
   Default: snappy (fast). Better: zstd (smaller).
3. STATS
   Each row group (~128MB) stores min/max per column
   Predicate WHERE x > 100 skips row groups where max ≤ 100
4. NESTED TYPES
   Native support for structs, arrays, maps (Dremel encoding)
5. UNIVERSAL
   Read by every major data tool
CONFIG:
  spark.sql.parquet.compression.codec = "snappy"  # or "zstd"
  spark.sql.parquet.filterPushdown = true          # default
  spark.sql.parquet.mergeSchema = false            # set true only if needed
```

```
13.3 Parquet Internals
FILE STRUCTURE:
  ■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■
  ■ Magic number "PAR1"                          ■
  ■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■
  ■ ROW GROUP 1  (~128 MB)                       ■
  ■   ■■■ Column chunk: col_a (pages)            ■
  ■   ■■■ Column chunk: col_b (pages)            ■
  ■   ■■■ Column chunk: col_c (pages)            ■
  ■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■
  ■ ROW GROUP 2                                  ■
  ■ ...                                           ■
  ■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■
  ■ Footer (schema, row group metadata, stats)   ■
  ■ Magic number "PAR1"                          ■
  ■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■
```

Within a column chunk: PAGE 1 (compressed) | PAGE 2 | ... | dictionary page (optional) ENCODINGS: PLAIN, RLE (run-length), DICTIONARY, DELTA, BYTE_STREAM_SPLIT Spark picks based on data; dictionary often gives 5-10× compression

```sql
13.4 Delta Lake = Parquet + Transaction Log
DELTA LAKE adds to Parquet:
   _delta_log/ directory with JSON files (commit log)
   Each commit = JSON describing files added/removed
   ACID transactions (single-writer, optimistic concurrency)
   Time travel: SELECT * FROM delta.`path` VERSION AS OF 5
   MERGE INTO (upserts), DELETE, UPDATE
   Schema enforcement and evolution
   OPTIMIZE: compact small files, Z-ORDER
   VACUUM: remove old versions
OPERATIONALLY:
   A Delta table IS a Parquet directory + log
   Tools without Delta support see only Parquet files (current snapshot)
   Delta Sharing protocol for cross-org access
ALTERNATIVES:
  ICEBERG (Apache, vendor-neutral, growing fast)
  HUDI (LinkedIn/Uber, optimized for streaming upserts)
■ Interview angle: Q: Delta vs Iceberg — when to pick which?
Staff: Both are great. Delta is mature, has the deepest Spark/Databricks integration, and Z-ORDER works well.
Iceberg has truly open governance and broader engine support (Trino, Flink, Snowflake, BigQuery all read Iceberg
natively). For Databricks-centric shops, Delta. For multi-engine lakehouses or where vendor neutrality matters,
Iceberg. Both have parity on the important features: ACID, time travel, schema evolution, partition evolution. Hudi is
best when streaming upserts at high volume are the primary workload.
```

## PART IV — JOINS, AGGREGATIONS & SKEW

#### 14. Join Strategies: Broadcast, SMJ, Shuffle Hash

#### 14.1 The Three Join Algorithms

##### 14.1.1 Broadcast Hash Join (BHJ)

#### 1. Driver collects small side to memory

#### 2. Broadcasts a hashmap to every executor

```sql
  3. Each executor probes its local hashmap with rows from the big side
  4. NO SHUFFLE — joins happen locally on each partition
COST: O(big_size) — only the big side is scanned, no network shuffle
LIMIT: small side must fit in driver + executor memory
       (each executor gets the full small side!)
THRESHOLD: spark.sql.autoBroadcastJoinThreshold (default 10 MB)
  Auto-broadcast triggers when estimated small side < threshold
HINT: F.broadcast(small_df) — force broadcast
14.1.2 Sort-Merge Join (SMJ) — Spark default
ALGORITHM:
  1. SHUFFLE both sides by join key → same key in same partition
  2. SORT each partition by join key
  3. MERGE: walk both sorted streams in lockstep
COST: O(N log N + M log M) sort + O(N+M) merge + 2 shuffles
NO MEMORY LIMIT — sorted data streams from disk
BEST FOR: two large tables, neither small enough to broadcast
```

14.1.3 Shuffle Hash Join (SHJ) ALGORITHM: 1. SHUFFLE both sides by join key

#### 2. Within each partition, build a hashmap on smaller side

3. Probe with the larger side COST: O(N+M) shuffle + O(M) hash build + O(N) probe LIMIT: smaller partition must fit in memory DISABLED BY DEFAULT in Spark — SMJ chosen instead ENABLE: spark.sql.join.preferSortMergeJoin = false

14.2 Picking a Join Strategy Scenario Best strategy Small dim table (< 100 MB) joined to fact Broadcast Two large fact tables SMJ (or AQE-tuned) Both pre-bucketed on join key Bucket join (no shuffle) Skewed join key AQE skew handling or salting Non-equi join (range) BroadcastNestedLoop or rewrite Cross join (cartesian) Avoid; rewrite if possible

```python
14.3 Broadcast Join Patterns
# Auto-broadcast (when Catalyst knows size)
small = spark.read.parquet("countries").filter(...)   # ~5MB
big = spark.read.parquet("orders")                     # ~500GB
joined = big.join(small, "country_code")               # auto-broadcasts small
# FORCE broadcast (when Catalyst doesn't know size — e.g., after transforms)
from pyspark.sql.functions import broadcast
joined = big.join(broadcast(small), "country_code")
# Anti-pattern: broadcasting too-large side
# Driver OOM if small side > driver memory
# Executor OOM if small side × num_executors > cluster memory
# Tune threshold:
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "100MB")
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")  # disable
```

```sql
14.4 Common Join Anti-Patterns
1. JOIN ON COMPUTED EXPRESSION
   df1.join(df2, F.col("a") + 1 == F.col("b"))  # disables broadcast estimate
   → precompute the column instead
2. STRING vs INT JOIN KEYS
   df1.join(df2, "id") where id is string in one, int in other
   → implicit cast; may bypass certain optimizations
3. JOINING WITH OR CONDITION
   df1.join(df2, (df1.a == df2.x) | (df1.b == df2.y))
   → can't use hash join, falls back to nested loop. Rewrite as UNION of two joins.
4. EXPLODING ROWS BEFORE JOIN
   df.withColumn("x", F.explode("array_col")).join(...)
   → join AFTER explode multiplies cost. Join then explode if possible.
5. CARTESIAN JOIN BY ACCIDENT
   forgotten ON clause → cross join.
   Catalyst will refuse unless spark.sql.crossJoin.enabled = true.
■ Interview angle: Q: How does Spark decide between broadcast and sort-merge join?
Staff: Three factors. (1) Catalyst's size estimate of the smaller side — if below autoBroadcastJoinThreshold
(default 10MB), it broadcasts. (2) Whether stats are available — without ANALYZE, the estimate comes from file
sizes, which overestimates after filters. (3) AQE — at runtime, Spark can re-decide based on actual post-shuffle
sizes. If broadcast is inappropriate (small side too large for driver/executor memory), force SMJ by disabling
auto-broadcast or use explicit hints. The right hint says, 'Spark, I know more about my data than your stats do'.
```

#### 15. Aggregations & Window Functions

```sql
15.1 Two-Phase Aggregation
Spark aggregations are two-phase by default: a partial aggregation before the shuffle (reduces data sent)
and a final aggregation after. This is similar to MapReduce's combiner.
SELECT country, SUM(amount) FROM orders GROUP BY country;
PHYSICAL PLAN:
  HashAggregate(final)               ← combines partial sums per country
  +- Exchange hashpartitioning(country, 200)   ← shuffle
     +- HashAggregate(partial)       ← pre-aggregates per partition
        +- FileScan orders
KEY POINT:
  Pre-aggregation reduces shuffle volume by N (N = avg group size).
  This is why SUM is cheap and COUNT(DISTINCT x) is expensive —
  DISTINCT can't pre-aggregate the way SUM can.
15.2 COUNT(DISTINCT) — The Slow One
COUNT(DISTINCT x) requires the FULL set of distinct values to be present
in one place per group. Spark cannot pre-aggregate without losing distinctness.
OPTIMIZATIONS:
1. APPROX_COUNT_DISTINCT (HyperLogLog)
   F.approx_count_distinct("user_id", rsd=0.05)   # ~5% error, much faster
2. MULTIPLE DISTINCTS — REWRITE
   Bad:
     SELECT COUNT(DISTINCT a), COUNT(DISTINCT b), COUNT(DISTINCT c) FROM t;
     -- Spark does this in one big agg, sorts everything, slow
   Better:
     spark.conf.set("spark.sql.optimizer.distinctAggregateRewrite.enabled", "true")
     -- rewrites to a UNION of separate distinct counts
3. PRE-AGGREGATE BY DAY (if cardinality reduction helps)
```

```python
15.3 Window Functions
from pyspark.sql import Window as W
# Top N per group
w = W.partitionBy("user_id").orderBy(F.desc("created_at"))
df.withColumn("rn", F.row_number().over(w)).filter("rn <= 3")
# Running total
w = W.partitionBy("user_id").orderBy("date") \
     .rowsBetween(W.unboundedPreceding, W.currentRow)
df.withColumn("cumsum", F.sum("amount").over(w))
# Moving average (7-day)
w = W.partitionBy("user_id").orderBy("date").rowsBetween(-6, 0)
df.withColumn("ma7", F.avg("amount").over(w))
# Lead / Lag
w = W.partitionBy("user_id").orderBy("date")
df.withColumn("prev_amount", F.lag("amount", 1).over(w))
# Multiple windows — Spark dedupes the partition+order if same
df.select(
```

```
    F.sum("a").over(w),
    F.avg("a").over(w),    # same w → one sort
    F.max("a").over(w),
).distinct()
```

```
15.4 Window Function Performance
 Every window with ORDER BY requires a SORT within partition
 Partitioning by high-cardinality column → many small windows (OK)
 Partitioning by low-cardinality column → few huge windows (bad)
 UNBOUNDED PRECEDING + UNBOUNDED FOLLOWING — entire partition in memory
  Spark can buffer to disk; still slow at scale
PARTITION BY () with no key — single partition for the entire dataset
→ NO PARALLELISM. Don't do this for global running totals on large data.
■ Production scenario: A pipeline computed global rank across 5B rows with
F.row_number().over(W.orderBy('score')) — no partitionBy. Spark sorted the entire dataset in a single
task. 14 hours, OOM. Fix: bucketed approximation — partition by hash bucket, compute rank within bucket, then
post-process. Or use a different approach entirely (percentile approximation via t-digest).
```

#### 16. Data Skew: Detection & Mitigation

16.1 What is Skew? Skew = some partitions have vastly more data than others. The slowest task becomes the stage's wall-clock time. In a 1000-task stage, one task with 10× the data of others extends the whole stage by 10×, while 999 cores idle.

```
16.2 Detecting Skew
# In Spark UI > Stages > tasks table:
#   Look at Duration column.
#   Healthy:  P50=20s, P99=25s  (tight)
#   Skewed:   P50=20s, P99=600s (one outlier)
# Programmatically — count rows per partition:
df.groupBy(F.spark_partition_id()).count().orderBy(F.desc("count")).show()
# For join skew — count rows per join key:
df.groupBy("join_key").count().orderBy(F.desc("count")).show(20)
# RULE OF THUMB:
#   If max partition > 10× median → likely skewed
#   If max partition > 100× median → definitely skewed, must fix
```

```sql
16.3 Common Skew Sources
1. NULL JOIN KEYS
   All rows with NULL hash to same partition. Often huge.
   Fix: filter NULLs before join, OR transform NULLs to random:
   df.withColumn("key", F.when(F.col("key").isNull(),
                                F.concat(F.lit("null_"), F.rand()))
                          .otherwise(F.col("key")))
2. POWER-LAW DISTRIBUTIONS
   user_id with bots having 1000× more events than normal users
   country_code where one country has 80% of users
3. DEFAULT VALUES
   user_id = -1 for unknown, ends up containing millions of rows
4. RECENT DATE PARTITIONS
   Streaming append always to today's partition → hotspot
```

```python
16.4 Mitigation: Salting
SALTING: add randomness to a skewed key to spread it.
# Original join (skewed on user_id where bots cause hotspots)
big.join(small, "user_id")
# Salted version:
# 1. Add a salt to the big side (random 0-9)
salt_n = 10
big_salted = big.withColumn("salt", (F.rand() * salt_n).cast("int"))
# 2. Replicate the small side N times with all salt values
import pyspark.sql.functions as F
small_replicated = (small
    .withColumn("salt_array", F.array([F.lit(i) for i in range(salt_n)]))
    .withColumn("salt", F.explode("salt_array"))
    .drop("salt_array"))
```

# 3. Join on (user_id, salt) result = big_salted.join(small_replicated, ["user_id", "salt"]) # COST: small side grows 10×, but skewed partition splits 10 ways. # Use only on the skewed keys (selective salting) for efficiency: # salt = (hash(key) % salt_n) WHEN key IN (hot_keys) ELSE 0

#### 16.5 AQE Skew Handling (the modern way)

#### 1. Detects skewed partitions at runtime (post-shuffle sizes)

#### 2. Splits them into smaller sub-partitions

```sql
  3. For joins, replicates the matching range from the other side
ENABLE:
  spark.sql.adaptive.enabled = true
  spark.sql.adaptive.skewJoin.enabled = true
  spark.sql.adaptive.skewJoin.skewedPartitionFactor = 5       # >5× median = skewed
  spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes = 256MB
This handles many cases automatically. Salting is still needed when:
   Spark misses the skew (small dataset, weird distribution)
   Skew is on the build side of broadcast (broadcast still happens)
   Custom shuffle semantics required
■ Interview angle: Q: A join stage in your job has 999 tasks finishing in 30 seconds and one task running for an
hour. Walk me through your debugging.
Staff: Skew on the join key. (1) Verify in Spark UI — look at max/p99/p50 task duration. (2) Find the culprit key:
df.groupBy(join_key).count().orderBy(desc('count')) shows the top N. Often it's NULL, a default
sentinel, or a power-user. (3) Pick a fix based on the cause: NULL or sentinels → filter them out or randomize.
Power-law → AQE skew handling first, then selective salting on top keys if AQE misses it. Bot accounts → maybe
separate pipeline for them. (4) Verify the fix in UI: tight task duration distribution after.
```

#### 17. Adaptive Query Execution (AQE)

17.1 The Pre-AQE World Catalyst optimizes the plan ONCE, before execution, based on table statistics. If stats are missing or stale, decisions are bad. For example, the optimizer picks SMJ because it estimated the small side as 50MB; the actual side after filters is 5MB and would have been a perfect broadcast — but the plan is locked in. AQE (Spark 3.0+) fixes this by re-optimizing at runtime.

```
17.2 The Three AQE Features
1. DYNAMICALLY COALESCE SHUFFLE PARTITIONS
   After shuffle, look at actual partition sizes. Merge small partitions
   together to hit a target (default 64MB).
   → No more "200 partitions for a 10GB job"
   → spark.sql.adaptive.coalescePartitions.enabled = true
2. DYNAMICALLY SWITCH JOIN STRATEGIES
   Catalyst said SMJ. Mid-job, sees actual size of small side after
   shuffle/filter — under broadcast threshold? Switch to BHJ.
   → spark.sql.adaptive.localShuffleReader.enabled = true (for converted joins)
3. DYNAMICALLY OPTIMIZE SKEWED JOINS
   Detects skewed partitions post-shuffle, splits them.
   → spark.sql.adaptive.skewJoin.enabled = true
```

```
17.3 Enabling AQE
# The "always enable" block
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")
spark.conf.set("spark.sql.adaptive.localShuffleReader.enabled", "true")
# Tuning
spark.conf.set("spark.sql.adaptive.advisoryPartitionSizeInBytes", "128MB")
spark.conf.set("spark.sql.adaptive.coalescePartitions.minPartitionSize", "1MB")
spark.conf.set("spark.sql.adaptive.coalescePartitions.initialPartitionNum", "1000")
# Databricks: enabled by default in DBR 7.3+
# Open-source Spark 3.2+: enabled by default
# CHECK IT'S WORKING — Spark UI > SQL plan shows "AdaptiveSparkPlan"
# and you'll see plan changes between initial and final
```

17.4 When AQE Helps Most ✓ Long pipelines with many shuffles ✓ Workloads where input size varies significantly (good day vs bad day) ✓ Jobs without pre-collected statistics ✓ Skewed joins ✓ Spark.sql.shuffle.partitions far from optimal LESS HELPFUL: ✗ Single-shuffle jobs where the right partition count is well-known ✗ Latency-sensitive jobs (AQE adds planning overhead per stage) ■ Principal mindset: AQE has changed Spark tuning fundamentally. Pre-AQE, you spent hours tuning spark.sql.shuffle.partitions per job. With AQE on, you set sensible defaults and let Spark adapt. Most performance recommendations written before 2021 are obsolete. If you're still hand-tuning partition counts and not seeing AQE-related improvements, verify AQE is actually enabled — and look for skew it might be missing.

## PART V — PERFORMANCE & MEMORY

#### 18. Memory Management & OOM Patterns

```
18.1 The Executor Memory Hierarchy (Revisited)
spark.executor.memory = 8g          # heap (JVM)
spark.executor.memoryOverhead = 1g  # off-heap (Python workers, native libs)
spark.memory.offHeap.enabled = true
spark.memory.offHeap.size = 4g      # off-heap for Spark execution
TOTAL EXECUTOR FOOTPRINT:
  heap + memoryOverhead + offHeap.size = 8 + 1 + 4 = 13 GB
YARN / K8s allocates AT LEAST the total. Container OOMKilled if exceeded.
THE CLASSIC MISTAKE:
  spark.executor.memory = 32g  but memoryOverhead not bumped (default ~10%)
  Python workers eat overhead, container killed.
```

```
18.2 The OOM Decision Tree
DRIVER OOM
  ■ collect / toPandas: filter first, use Arrow conversion
  ■ broadcast too large: raise driver memory or disable auto-broadcast
  ■ too many tasks: reduce partitions
EXECUTOR HEAP OOM
  ■ large aggregation state: more shuffle partitions, less per-task
  ■ skewed task: AQE or salting
  ■ large broadcast var: reduce or stream
EXECUTOR overhead OOM (container killed by YARN/K8s)
  ■ Python UDFs allocating heavily: switch to Pandas UDF or built-ins
  ■ Native libraries (Arrow, BLAS): raise memoryOverhead
SHUFFLE OOM
  ■ giant shuffle blocks: more partitions
  ■ shuffle service down: enable external shuffle service
GC SPIRAL
  ■ heap nearly full but not over → constant Full GC → no work done
  ■ Look for GC time > 10% in UI
  ■ Fix: raise memory, use off-heap, switch to G1GC
```

```
18.3 Right-Sizing Executors
GUIDELINES (Databricks-style):
OPTION A: FEW FAT EXECUTORS
  spark.executor.memory = 32g
  spark.executor.cores  = 8
   Pro: less shuffle (fewer executors to fetch from), big broadcasts fit
   Con: more chance of GC issues, single executor failure costs more
OPTION B: MANY THIN EXECUTORS
  spark.executor.memory = 8g
  spark.executor.cores  = 2
```

```
   Pro: less GC, faster failover, better packing
   Con: more network shuffle, broadcast harder
PRAGMATIC DEFAULT (cloud, mixed workload):
  cores = 4-5 per executor
  memory = 16-24 GB
  overhead = 10-15% (more if Python-heavy)
  enable AQE + dynamic allocation
YARN MAX EXECUTORS = (cluster_cores - 1) / executor_cores
DRIVER usually 1 core / 8-16 GB unless heavy collect
■ Production scenario: A team set executor.memory=64g with 1 core per executor 'for safety'. Their cluster had 32
cores × 256GB nodes. Each node could run 4 executors → 4 cores used, 28 cores idle, 4× 64GB allocated = 256GB
consumed. They were paying for 32 cores and using 4. Switched to 16g × 4 cores per executor — 8 executors per
node, all cores used, cluster cost dropped 8×.
```

#### 19. Caching, Persistence & Checkpointing

```
19.1 When to Cache
CACHE IF:
  ✓ A DataFrame is used MORE THAN ONCE downstream
  ✓ The DataFrame is expensive to recompute (heavy transforms upstream)
  ✓ It fits in memory (or memory + disk)
DON'T CACHE IF:
  ✗ Used only once — cache adds overhead, no payoff
  ✗ Source is already fast (cheap Parquet scan)
  ✗ Cluster is memory-constrained — caching evicts shuffle state, hurts
  ✗ The DF is huge and you only need a small slice — filter first
THE CACHING ANTI-PATTERN:
  cache_df = df.cache()  # ■■ doesn't actually cache until an action runs
  cache_df.show()        # NOW it caches
```

```python
19.2 Storage Levels
Level
Memory
Disk
Serialized
Replicated
MEMORY_ONLY (default)
✓
✗
✗ (objects)
✗
MEMORY_AND_DISK
✓
✓
✗
✗
MEMORY_ONLY_SER
✓
✗
✓ (compact)
✗
MEMORY_AND_DISK_SER
✓
✓
✓
✗
DISK_ONLY
✗
✓
✓
✗
OFF_HEAP
✓ off
✗
✓
✗
from pyspark import StorageLevel
df.persist(StorageLevel.MEMORY_AND_DISK_SER)
df.cache()                     # = MEMORY_AND_DISK for DataFrames in Spark 2.0+
# Always trigger materialization explicitly:
df.cache(); df.count()         # forces all partitions into cache
# Release:
df.unpersist()                  # important to free memory in long jobs
```

```
19.3 Checkpointing
CHECKPOINT — write DF to reliable storage, truncate the lineage.
WHY:
  Long lineages (100+ transformations) → slow recompute on failure,
  big plan object on driver. Checkpoint cuts the chain.
  Common in iterative ML / graph algorithms.
USAGE:
  spark.sparkContext.setCheckpointDir("s3://bucket/checkpoints/")
  df_checkpointed = df.checkpoint()  # blocks until written
  # Or "localCheckpoint" — faster, no fault tolerance:
  df_lc = df.localCheckpoint()
CACHE vs CHECKPOINT:
  Cache:      in-memory/disk, lineage preserved (recomputable on failure)
```

Checkpoint: durable storage, lineage truncated (no recompute needed) ■ Interview angle: Q: When would you cache vs checkpoint? Staff: Cache for reuse — same DataFrame referenced 2+ times downstream in the same job. Checkpoint for lineage truncation — long iterative pipelines (graph, ML) where each iteration depends on the previous, and recomputing from source on failure would be catastrophic. Cache is cheaper but lineage stays; if a cached partition is lost, Spark recomputes from source. Checkpoint pays the cost of writing to reliable storage but breaks the chain — failure recovery becomes O(steps since last checkpoint) instead of O(all steps).

20. UDFs, Pandas UDFs & the Python Boundary

```
20.1 The Python ↔ JVM Cost
For every Python UDF row:
  JVM: serialize row to pickle
       ↓ TCP socket
  Python: deserialize, run UDF, serialize result
       ↑ TCP socket
  JVM: deserialize, emit row
OVERHEAD:
  ~30-100 µs per row of serialization + IPC
  At 1M rows = 30-100 seconds of pure overhead, before any work.
```

```
20.2 The Hierarchy of UDF Performance
TIER 1 (fastest, native, optimized) — ALWAYS PREFER
  Built-in functions in pyspark.sql.functions (F.col, F.sum, F.when, ...)
  Native Spark, JVM-only, Catalyst-optimized, codegen'd.
TIER 2 (slower, but vectorized
100× faster than tier 4)
  Pandas UDF (vectorized UDF)
  Receives a batch of rows as Pandas Series; returns Pandas Series.
  Arrow-based zero-copy serialization.
TIER 3 (Spark SQL via expr)
  expr("CASE WHEN x > 0 THEN 'pos' ELSE 'neg' END")
  Same as Tier 1 internally, sometimes more readable.
TIER 4 (slow — row-at-a-time Python UDF)
  @udf decorator: receives one value at a time, returns one value.
  Last resort. Almost always replaceable with Tier 1 or 2.
```

```python
20.3 Pandas UDF Examples
from pyspark.sql.functions import pandas_udf
from pyspark.sql.types import DoubleType
import pandas as pd
# Series → Series (scalar)
@pandas_udf(DoubleType())
def log_amount(s: pd.Series) -> pd.Series:
    import numpy as np
    return np.log1p(s)
df.withColumn("log_amt", log_amount("amount"))
# Multi-arg, returns Series
@pandas_udf(DoubleType())
def weighted_score(score: pd.Series, weight: pd.Series) -> pd.Series:
    return score * weight
# Grouped Map (whole group as DataFrame in, DataFrame out)
def normalize(pdf: pd.DataFrame) -> pd.DataFrame:
    pdf['z'] = (pdf['x'] - pdf['x'].mean()) / pdf['x'].std()
    return pdf
df.groupby("group").applyInPandas(normalize, schema="group string, x double, z double")
# Iterator of Series — for amortizing setup cost (model loading)
@pandas_udf(DoubleType())
def predict(it: Iterator[pd.Series]) -> Iterator[pd.Series]:
    model = load_model()    # loaded ONCE per executor
```

```
    for s in it:
        yield pd.Series(model.predict(s.values))
```

```
20.4 When You Truly Need a UDF
LEGITIMATE UDF use cases:
   Calling an external library not available as F.* function
   Per-row Python logic that's genuinely complex
   Model inference (use Pandas UDF iterator for setup amortization)
   Custom string processing not expressible in SQL
INSPECT BEFORE WRITING A UDF:
   F.regexp_extract, F.regexp_replace
   F.from_json, F.to_json
   F.aggregate, F.transform, F.filter (higher-order on arrays)
   F.expr(...) — full SQL expression access
■ Production scenario: A team wrote a Python UDF for date parsing on a 1B-row table. Job took 14 hours.
Replaced with F.to_timestamp(col, fmt) — 12 minutes. UDF wasn't even logically necessary; the engineer
didn't know the built-in existed. Lesson: always grep pyspark.sql.functions before writing a UDF.
```

#### 21. Tuning Playbook

  - 21.1 The Systematic Approach
- 1. PROFILE
- Run with conservative config. Capture Spark UI.
- 2. IDENTIFY THE BOTTLENECK STAGE
- Which stage took longest? Why? (compute? shuffle? skew? GC?)
- 3. ATTACK THE BOTTLENECK
- Don't tune everything at once. Change one thing, measure.
- 4. ITERATE
- Stop when good enough. Diminishing returns are real.

21.2 The Standard Config Block # Enable AQE (huge wins for most workloads) spark.sql.adaptive.enabled = true spark.sql.adaptive.coalescePartitions.enabled = true spark.sql.adaptive.skewJoin.enabled = true spark.sql.adaptive.advisoryPartitionSizeInBytes = 128m # Shuffle spark.sql.shuffle.partitions = 200 # AQE adjusts spark.shuffle.service.enabled = true spark.shuffle.compress = true spark.io.compression.codec = lz4 # I/O spark.sql.files.maxPartitionBytes = 128m spark.sql.parquet.compression.codec = snappy spark.sql.parquet.filterPushdown = true # Joins spark.sql.autoBroadcastJoinThreshold = 100m # Python spark.sql.execution.arrow.pyspark.enabled = true spark.sql.execution.arrow.maxRecordsPerBatch = 10000 # Dynamic allocation (cluster scaling) spark.dynamicAllocation.enabled = true spark.dynamicAllocation.minExecutors = 2 spark.dynamicAllocation.maxExecutors = 100 spark.dynamicAllocation.executorIdleTimeout = 60s

  - 21.3 The Tuning Order (most impact first)
- 1. ENABLE AQE                       — fixes 80% of common issues
- 2. FIX SKEW                          — single biggest perf win when present
- 3. CHOOSE THE RIGHT JOIN STRATEGY    — broadcast where possible
- 4. RIGHT-SIZE PARTITIONS
- 200MB target after shuffle
- 5. ELIMINATE PYTHON UDFs              — Pandas UDF or built-ins
- 6. AVOID UNNECESSARY SHUFFLES         — bucketing, pre-partitioning
- 7. CACHE WHAT YOU REUSE               — but only what you reuse
- 8. TUNE EXECUTOR SIZING               — last resort, smallest gains usually
- 9. FILE FORMAT / LAYOUT               — Parquet+zstd, Delta, Z-ORDER

#### 21.4 The Quick Diagnosis Table

Symptom Most likely cause One task takes 100× longer Skew Job slow despite low CPU Shuffle / disk / network bound Frequent executor loss Memory or spot preemption Driver OOM collect/toPandas, or huge broadcast High GC time Heap pressure, raise mem or off-heap Many small files in output Coalesce before write Slow reads, many tasks <1 sec Small input files SMJ where BHJ expected Stats missing, threshold too low Job slower with more cores Driver bottleneck or stage starvation ■ Principal mindset: The best Spark engineers don't tune; they design pipelines that don't need tuning. Default AQE on, write Parquet/Delta with sane partitioning, use built-in functions, avoid unnecessary shuffles. Tuning is what you do when design constraints force trade-offs — not the everyday workflow.

## PART VI — STREAMING & LAKEHOUSE

#### 22. Structured Streaming Fundamentals

```
22.1 The Model
Structured Streaming treats a stream as an unbounded table that grows over time. You write the same
DataFrame code as for batch, and Spark runs it incrementally — re-executing on new data.
# Source (unbounded table)
events = (spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "...")
    .option("subscribe", "events")
    .load())
# Transform (same API as batch)
agg = (events
    .selectExpr("CAST(value AS STRING) as json")
    .select(F.from_json("json", schema).alias("e"))
    .select("e.*")
    .filter("status = 'active'")
    .groupBy(F.window("event_time", "1 hour"), "country")
    .agg(F.count("*").alias("n")))
# Sink
query = (agg.writeStream
    .format("delta")
    .outputMode("update")
    .option("checkpointLocation", "s3://bucket/checkpoint/")
    .trigger(processingTime="1 minute")
    .start("s3://bucket/output/"))
```

22.2 Triggers .trigger(processingTime="30 seconds") # micro-batch every 30s (default) .trigger(once=True) # run once and exit (replaces batch) .trigger(availableNow=True) # process all available, then exit .trigger(continuous="1 second") # experimental low-latency mode PRACTICAL:  availableNow is the modern replacement for "once" (Spark 3.3+)  Use it for "incremental batch": run hourly, process everything new  Continuous mode is experimental, rarely production-ready

```
22.3 Output Modes
APPEND  (default)
  Only NEW rows since last trigger are written. No updates to past results.
  Required for: file sinks, most use cases.
  Watermarking required if using aggregations.
UPDATE
  Only CHANGED rows are written. Need a sink that supports updates.
  Good for: Delta MERGE, JDBC upsert.
COMPLETE
  Entire result table written every trigger.
```

Only practical for small aggregations. Don't use on TB-scale.

```
22.4 Sources & Sinks
SOURCES:
  Kafka       — production-grade
  File        — directory of new files (CSV, JSON, Parquet)
  Delta/Iceberg/Hudi — CDC-style change feed
  Kinesis     — via connector
  Socket      — testing only
  Rate        — synthetic, testing
SINKS:
  Kafka, Delta, Parquet, JDBC, Cassandra, ForEachBatch (custom),
  Memory (testing), Console (testing)
FOREACHBATCH: write to any sink
  def write_to_sink(batch_df, batch_id):
      batch_df.write.format("jdbc").options(...).save()
  query = stream.writeStream.foreachBatch(write_to_sink).start()
```

#### 23. State, Watermarks & Exactly-Once

23.1 The State Problem Stateless transforms (filter, map) are easy. Stateful ops (aggregations, joins, deduplication) require Spark to remember things across micro-batches. State is stored in RocksDB or HDFS-style key-value store, checkpointed to durable storage.

```
23.2 Watermarks
Problem: late-arriving events. If we aggregate per hour and the 3 PM bucket
gets an event at 3:45 PM, do we re-open the bucket? Forever?
WATERMARK = a threshold beyond which we accept events as too late.
agg = events.withWatermark("event_time", "10 minutes") \
            .groupBy(F.window("event_time", "1 hour")) \
            .count()
Semantics:
   Watermark = max(event_time seen) - 10 minutes
   A window can be finalized once watermark > window end
   Events with event_time < watermark are DROPPED
   State for finalized windows can be cleaned up
WITHOUT WATERMARK:
  Append mode aggregations: FORBIDDEN (state grows unbounded)
  Update mode: allowed but state grows forever
```

```
23.3 Stateful Operations
 AGGREGATIONS (groupBy + agg): keep running totals per group
 STREAM-STREAM JOINS: buffer one side until match arrives
 DEDUPLICATION (dropDuplicates): keep seen IDs in state
 MAP/FLATMAP GROUPS WITH STATE: arbitrary state per key
STATE STORE TYPES:
  HDFSBackedStateStore (default) — async checkpoints
  RocksDBStateStore — better for large state, less GC pressure
  spark.sql.streaming.stateStore.providerClass
STATE TUNING:
  spark.sql.streaming.minBatchesToRetain = 100   # for retry
  spark.sql.streaming.maxOffsetsPerTrigger       # rate limit
```

```
23.4 Exactly-Once Semantics
STREAMING EXACTLY-ONCE requires THREE things:
1. REPLAYABLE SOURCE
   Kafka offsets, Kinesis sequence numbers, file modification times
   Spark records WHICH offsets each batch processed
2. CHECKPOINTING
   Spark writes "I processed offsets X-Y for batch N" to checkpoint dir
   On restart, resumes from last committed offset
3. IDEMPOTENT OR TRANSACTIONAL SINK
   File/Delta: atomic write of batch_id-suffixed files (or txn)
   Kafka: txn producer (requires Spark 3.0+, kafka.transactional.id)
   JDBC: foreachBatch with MERGE / upsert by batch_id
```

WITHOUT ALL THREE: at-least-once only (duplicates possible) WITH ALL THREE: end-to-end exactly-once DELTA + Structured Streaming = exactly-once out of the box (sink does idempotent writes) ■ Interview angle: Q: How does Spark Structured Streaming achieve exactly-once? Staff: The trick is that Spark only commits the OUTPUT after recording the processed offsets in the checkpoint. If the job crashes between processing and committing, on restart it sees uncommitted offsets and re-processes — but the sink must accept duplicate writes (idempotency) or refuse them (txns). Three pieces: (1) replayable source — Kafka offsets stored in checkpoint, (2) deterministic processing — same input always produces same output for a batch, (3) idempotent/transactional sink — Delta writes by batch_id, Kafka uses transactional producer. Miss any one and you get at-least-once.

#### 24. Delta Lake & Lakehouse Patterns

24.1 The Medallion Architecture ■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■ ■ BRONZE — raw, as-received from source ■ ■  Schema-on-read, append-only ■ ■  One row per source event ■ ■  Source of truth; never reprocessed externally ■ ■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■ ▼ ■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■ ■ SILVER — cleaned, validated, deduplicated ■ ■  Conformed schema, type-checked ■ ■  Joined with reference dims (slowly-changing) ■ ■  Late-arriving data reconciled ■ ■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■ ▼ ■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■ ■ GOLD — business-aggregated, denormalized ■ ■  Star schema / wide tables ■ ■  Optimized for analytical queries (BI, ML features) ■ ■  Pre-aggregated metrics ■ ■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■

```python
24.2 Delta Operations
# CREATE
df.write.format("delta").save("s3://bucket/table/")
# or in catalog:
df.write.format("delta").saveAsTable("db.table")
# READ
delta_df = spark.read.format("delta").load("s3://bucket/table/")
# UPDATE / DELETE / MERGE
from delta.tables import DeltaTable
dt = DeltaTable.forPath(spark, "s3://bucket/table/")
# UPSERT (the killer feature)
dt.alias("t").merge(
    source=updates.alias("s"),
    condition="t.id = s.id"
).whenMatchedUpdateAll() \
 .whenNotMatchedInsertAll() \
 .execute()
# TIME TRAVEL
spark.read.format("delta").option("versionAsOf", 5).load("path")
spark.read.format("delta").option("timestampAsOf", "2024
01").load("path")
# OPTIMIZE — compact small files, optionally Z-ORDER
spark.sql("OPTIMIZE delta.`s3://bucket/table/` ZORDER BY (user_id, ts)")
# VACUUM — remove old version files (default 7 days retention)
spark.sql("VACUUM delta.`s3://bucket/table/` RETAIN 168 HOURS")
# SCHEMA EVOLUTION
df.write.format("delta") \
    .option("mergeSchema", "true") \
    .mode("append").save("path")
```

#### 24.3 Streaming + Delta Patterns

```python
# Streaming write
(stream_df.writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", "ckpt/")
    .start("s3://bucket/silver/"))
# Streaming MERGE (idempotent upserts)
def upsert(batch_df, batch_id):
    dt.alias("t").merge(
        source=batch_df.alias("s"),
        condition="t.id = s.id"
    ).whenMatchedUpdateAll() \
     .whenNotMatchedInsertAll() \
     .execute()
(stream_df.writeStream
    .foreachBatch(upsert)
    .option("checkpointLocation", "ckpt/")
    .start())
# Delta change feed (CDC) — Bronze → Silver
spark.readStream.format("delta") \
    .option("readChangeFeed", "true") \
    .option("startingVersion", 10) \
    .load("s3://bucket/bronze/")
24.4 Lakehouse vs Warehouse — When to Pick What
Aspect
Lakehouse (Delta/Iceberg)
Warehouse (Snowflake/BQ)
Compute coupling
Decoupled (BYOC: Spark, Trino,...)
Tightly coupled, managed
Cost model
Storage + compute separately
Storage + compute, per-query
Open format
✓ Parquet, public spec
✗ proprietary internal
SQL ergonomics
Improving (Photon, DBSQL)
Excellent
ML / Python
First-class (Spark, Pandas API)
Improving
Multi-tool read
✓ Trino, Snowflake, Spark, Flink
✗ vendor only
Cold start
Slower (cluster up)
Instant (managed)
Best for
Engineer-heavy, ML, code pipelines
Analyst-heavy, ad-hoc SQL
■ Interview angle: Q: When would you use a lakehouse vs a data warehouse?
Staff: Workload, team, and ecosystem. Lakehouse wins when your pipelines have heavy code (UDFs, ML, custom
transforms), when you want multi-engine access (same data read by Spark, Trino, BigQuery), or when
storage/compute decoupling is important for cost (cheap S3 storage, scale compute up and down). Warehouse wins
for ad-hoc SQL by analysts, instant auto-scaling, mature governance/sharing features, and operational simplicity.
Most large orgs end up with both: lakehouse for engineering pipelines, warehouse exposed to BI tools, with
replicated gold tables in both.
```

## PART VII — PRODUCTION & SCALE

#### 25. Cluster Sizing & Cost Modeling

```
25.1 The Sizing Mental Model
TOTAL_COMPUTE = TOTAL_DATA_SIZE / TARGET_TIME × OVERHEAD_FACTOR
Example:
  Process 5 TB in 1 hour with 1.5× safety factor
  TOTAL_COMPUTE = 5 TB / 3600 s × 1.5 = ~2 GB/s sustained throughput
  Single executor reads ~50-200 MB/s from S3 (network bound)
  → need 10-40 executors just for I/O
  Add compute overhead: 2-4× more cores for transformations
  GUESSTIMATE: 30 executors × 4 cores × 16 GB = 120 cores, 480 GB RAM
```

```
25.2 Compute Tiers
SMALL  (< 100 GB) — single big node or 5-10 small executors
                  20 cores, 128 GB RAM, ~$5-10/hr
                  Spark may be overkill; consider DuckDB or Polars
MEDIUM (100 GB - 5 TB) — typical Spark sweet spot
                  50-150 cores, 200-600 GB RAM, ~$20-100/hr
                  Use AQE, default configs work
LARGE  (5 TB - 100 TB) — purposeful tuning required
                  200-1000 cores, 1-5 TB RAM, $100-1000/hr
                  Must tune: partition counts, broadcast thresholds, skew
XL     (> 100 TB) — architect-level concerns
                  1000+ cores, multi-pool, $1000+/hr
                  Question: should this be one job? Or many smaller?
```

```
25.3 The Cost Calculus
ON-DEMAND vs SPOT/PREEMPTIBLE
  Spot is 60-90% cheaper but can be reclaimed mid-job
  Mitigations:
     External shuffle service (preserves shuffle data on executor loss)
     Long checkpoints (re-run cost bounded)
     Mix: driver on-demand, executors on spot
AUTOSCALING
  Dynamic allocation scales executors up/down as needed
  Saves $$ when stages have different parallelism needs
INSTANCE FAMILY CHOICE
  Compute-optimized (c-series): CPU-bound jobs
  Memory-optimized (r-series):   shuffle-heavy, big aggs
  Storage-optimized (i-series):  shuffle to local SSD, low S3 cost
  General (m-series):            mixed; safe default
NETWORK
  Reading S3 across regions = 10× cost + 100× latency
  Keep compute in same region as storage
```

```
25.4 Cost Levers (in order of impact)
1. PROCESS LESS DATA
    Push filters to source (predicate pushdown)
    Read only required columns (projection pushdown)
    Pre-aggregate / pre-filter upstream
2. RIGHT-SIZE CLUSTER
    Don't run on a 100-executor cluster for a 10-minute job
3. SPOT / PREEMPTIBLE
    60-80% savings for fault-tolerant batch
4. OPTIMIZE FILE LAYOUT
    Compact small files
    Parquet + zstd compression
    Z-ORDER hot columns
5. CACHE WISELY
    Cache reduces I/O for reused DFs
6. ELIMINATE WASTE
    collect/toPandas on full DF
    Unneeded shuffles
    Inefficient UDFs
■ Principal mindset: Cost engineering is staff-level work. Most teams accept the cluster bill without questioning. A
staff engineer audits: which jobs cost most? Why? Can the same output be produced 10× cheaper? Often the
answer is yes — by rewriting one bad UDF, fixing one skewed join, or moving an analytics workload to a warehouse
where it belongs.
```

#### 26. Common Failure Modes & Recovery

26.1 Failure Taxonomy Failure Cause Recovery Task fails Bad data / OOM Auto-retry (4×); task moves to another executor Executor lost OOM / preemption Tasks re-run; shuffle may need re-compute Stage fails Repeated task failures Job fails; need to investigate Driver fails OOM / network Whole job dies; restart with checkpoint if streaming Shuffle file lost Executor crashed before fetch Re-run upstream stage Source unreachable S3 throttling, Kafka down Retry with backoff Schema mismatch Source changed types Job dies; fix schema or use mergeSchema

```
26.2 Retry Strategy
# Task retries
spark.task.maxFailures = 4              # default; task retried 4× before stage fails
# Stage retries
spark.stage.maxConsecutiveAttempts = 4
# Speculative execution — re-launch slow tasks on another executor
spark.speculation = true                # off by default; helpful for stragglers
spark.speculation.multiplier = 1.5      # task is "slow" if 1.5× median
# RDD blacklisting (executors with repeated failures avoided)
spark.blacklist.enabled = true
```

```
26.3 Idempotency in Spark Jobs
WRITE IDEMPOTENCY:
  Spark write modes:
     overwrite      — atomic at table level for Delta; partial for Parquet
     append         — duplicates on retry unless dedupe at read
     errorIfExists  — safe but not idempotent
  USE DELTA / ICEBERG for atomic, idempotent writes.
  For raw Parquet: write to TEMP path, then atomic rename.
JOB-LEVEL IDEMPOTENCY:
   Compute should be a function of (input version, code version)
   Output partition path includes run_id or processing_date
   MERGE INTO uses business key, not append
   Streaming: checkpoint + idempotent sink = exactly-once
■ Production scenario: A pipeline appended daily aggregates to a Parquet table. A retry after a partial write
created duplicate rows for that day. Downstream consumers got doubled metrics. Fix: switched to Delta with
MERGE — same logic, idempotent. When the same retry happened weeks later, the duplicate rows were silently
deduplicated by the merge.
```

#### 27. Anti-Patterns at Scale

```python
27.1 The Catalog
CODE ANTI-PATTERNS
  ■ Python UDF for something built-in (toLowerCase, regex, date parsing)
  ■ .collect() / .toPandas() on big data (driver OOM)
  ■ Iteration over a DataFrame (for row in df.collect()) — anti-Spark
  ■ Many .withColumn calls instead of one .select with all columns
  ■ DataFrame mutation in a loop (Spark doesn't mutate; creates lineage)
  ■ Caching without an action to materialize
  ■ Not unpersisting cached DataFrames in long jobs
DESIGN ANTI-PATTERNS
  ■ Running OLAP-style SQL on Spark when a warehouse would do it cheaper
  ■ Spark for sub-second latency (use Flink or a serving DB)
  ■ Spark on a single small node (use DuckDB/Polars)
  ■ "Spark for everything" architecture
  ■ One giant job doing 10 unrelated things — split it
LAYOUT ANTI-PATTERNS
  ■ Many tiny files (compact before processing)
  ■ Partitioning by high-cardinality column (user_id with 50M users)
  ■ No partitioning on a TB+ table (every query is a full scan)
  ■ CSV/JSON storage in prod (use Parquet/Delta)
  ■ Schema-on-read for hot tables (define and enforce)
OPERATIONAL ANTI-PATTERNS
  ■ No monitoring on job duration / data volume
  ■ Manual restart on failure (need automated retry + alerting)
  ■ No data quality checks (Great Expectations, Deequ, dbt tests)
  ■ Mutating production tables in-place (use atomic replace, Delta)
  ■ Not using AQE in 2024
  ■ Hand-tuning shuffle.partitions when AQE would do it
■ Production scenario: An analytics team's flagship dashboard ran 300+ Spark queries per day on a 5-node
cluster. Spark spinning up for each query took 60-90 seconds, queries themselves ran in 10-30 seconds. Cost:
$40K/month. Migration to BigQuery: queries ran in 5-15 seconds (no warm-up), cost $3K/month. The right tool for
ad-hoc SQL is a warehouse, not Spark.
■ Interview angle: Q: Your team uses Spark for everything. Which workloads should NOT be on Spark?
Staff: Three categories. (1) Ad-hoc SQL on tabular data with no custom code — warehouses do this 5-10× cheaper
with better ergonomics. (2) Real-time (<100ms) — Flink or a serving DB. (3) Small data (<100GB) — DuckDB or
Polars on a single beefy node. The 'Spark for everything' architecture started in 2015 when Spark was the only good
option; today it's specifically for code-heavy pipelines, ML feature engineering, and multi-source joins. Auditing which
workloads can be moved off Spark is one of the highest-impact exercises a data platform team can do.
```

## PART VIII — INTERVIEW PRACTICE

#### 28. The Staff-Level Spark Interview

  - 28.1 Question Categories
- 1. INTERNALS  — "Walk through what happens when you call .count() on a DF"
- 2. DEBUGGING  — "This job is slow. Spark UI shows X. What do you do?"
- 3. DESIGN     — "Design a pipeline to process 10TB/day with these requirements"
- 4. TRADE-OFFS — "Spark vs Flink vs Snowflake for this workload"
- 5. SCALE      — "What breaks first as you scale to 100TB? 1PB?"
- 6. CODE       — Write a window function, salting, broadcast hint, MERGE

#### 28.2 The Frameworks

#### 1. Read the Spark UI (which stage? what numbers?)

#### 2. Read the plan (where's the shuffle? what join?)

#### 3. Hypothesize (skew? small files? bad config?)

#### 4. Verify (run a count by key, check stats)

#### 5. Fix one thing, measure

```
  1. Clarify requirements
      Data volume? Velocity? Variety?
      Latency: sub-second? minutes? hourly?
      Consistency requirements?
      Cost constraints?
```

#### 2. Choose storage layer (S3 + Delta? warehouse? message bus?)

#### 3. Choose compute (Spark? Flink? warehouse?)

#### 4. Define partitioning strategy

#### 5. Discuss failure modes & recovery

```
  6. Cost model
FOR TRADE-OFF QUESTIONS:
  Always start: "Depends on... here are the dimensions I'd consider"
  Avoid: "Spark is always better than X" / "Use Y because it's modern"
```

```
28.3 Worked Example: Debug a Slow Job
INTERVIEWER: "A nightly Spark job that used to take 45 min now takes 6 hours.
              Yesterday's data volume was the same as a week ago. Debug."
STAFF RESPONSE:
"First, I'd open the Spark UI for the slow run and compare stage durations
 to a known-good run. Most likely culprits given 'same volume but slower':
 1. Skew that developed gradually — a key went viral / a bot account.
    Check: max vs median task duration per stage. Verify with
    df.groupBy(key).count().orderBy(desc).show(20).
 2. Small-file accumulation — upstream system started writing 10× more files.
    Check: number of input tasks vs input bytes. If many tasks <100ms,
    we're scheduling-bound. Run OPTIMIZE / compaction.
 3. Cluster sharing — another job started competing for resources.
    Check: executor allocation timeline.
```

4. AWS / network throttling — S3 SlowDown errors in driver log. Check: driver log for 503s; spread reads across prefixes. 5. Recent code change — git blame the pipeline. Check: when did the regression start? Bisect. I'd start with #1 and #2 — they're easiest to verify from the UI alone." The signal: methodical, mentions specific UI tabs, named diagnostics, prioritized hypotheses.

28.4 Red Flags vs Green Flags GREEN FLAGS RED FLAGS ■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■ "Depends on..." "Always do X" References Spark UI / plan Talks only at API level Discusses cost & trade-offs Optimizes for purity, not pragmatism Mentions AQE, Delta, modern tools Talks like it's 2017 Asks clarifying questions Jumps to implementation "I'd measure first" "This is definitely faster" Knows when NOT to use Spark "Spark for everything" Mentions data quality, monitoring Talks only about happy path

29. War Stories ■ Production scenario: The Cartesian Catastrophe. A junior engineer wrote df1.join(df2) without an ON clause. Catalyst refused (cross join not enabled). They set spark.sql.crossJoin.enabled = true to 'fix it'. Job kicked off, scaled cluster to 200 nodes, produced 10^11 rows, filled S3 bucket, billing alert triggered at $50K. Lesson: cross-join guardrails exist for a reason. Add cost-control alerts at the platform level. ■ Production scenario: The Phantom Performance Gain. A team added .cache() everywhere 'for performance'. Jobs got slower. Investigation: caching DataFrames that were only used once added overhead. Worse, cached data evicted shuffle state, causing recomputes. Removing all caches cut job times by 30%. Lesson: cache is not 'free speed' — it's a memory trade. Measure before adding cache calls. ■ Production scenario: The String UUID Disaster. A team stored UUIDs as 36-char strings in Parquet. Joins on UUID columns were slow. Switched to fixed-byte binary representation (16 bytes): file size dropped 60%, joins ran 5× faster (better vectorization, less data to hash). For any large-table join key, the type choice has compounding consequences. ■ Production scenario: The Streaming Checkpoint That Wouldn't Stop Growing. A streaming job had a stateful aggregation without a watermark. State accumulated for months. Eventually the state store on each executor was ~50 GB. Failover from spot interruption took 20 minutes (state reload). Adding a 24-hour watermark released old state; checkpoint dropped to ~500 MB; failovers became 30-second operations. ■ Production scenario: The Schema Drift Outage. Upstream Kafka producer added a new field. Spark streaming job had a fixed schema, dropped messages with _corrupt_record. No alert because the job 'succeeded'. Two weeks of metric loss before BI team noticed. Lesson: validate row counts in every batch, alert on _corrupt_record > threshold, treat schema evolution as a first-class concern. ■ Production scenario: The Driver OOM Mystery. A job's driver kept OOMing 'randomly'. Heap dump showed millions of org.apache.spark.scheduler.Task objects. Cause: the pipeline had 50K shuffle partitions × 5 stages = 250K task objects on the driver. Reducing partitions to 2K fixed it. Driver memory scales with task count, not just data volume.

#### 30. Cheat Sheet & Mental Models

  - 30.1 The Six Mental Models
- 1. THE DAG IS LAZY — nothing runs until an action
- 2. SHUFFLES ARE THE COST — narrow ops are essentially free
- 3. PARTITIONS ARE THE PARALLELISM UNIT — target 100-200MB each
- 4. CATALYST OPTIMIZES, AQE ADAPTS — give it stats, let it work
- 5. PYTHON CROSSING IS EXPENSIVE — built-ins > Pandas UDF > Python UDF
- 6. SKEW IS THE #1 SCALING PROBLEM — design joins to avoid hot keys

```python
30.2 The One-Page Reference
SESSION & CONFIG
  spark = SparkSession.builder.appName("x") \
      .config("spark.sql.adaptive.enabled", "true") \
      .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
      .config("spark.sql.adaptive.skewJoin.enabled", "true") \
      .config("spark.sql.execution.arrow.pyspark.enabled", "true") \
      .getOrCreate()
READ
  spark.read.parquet("s3://...")
  spark.read.format("delta").load("...")
  spark.read.format("kafka").option("subscribe","topic").load()
TRANSFORM
  df.select, .filter, .withColumn, .groupBy, .agg, .join, .union
  df.select("a", F.col("b") + 1, F.lit("c").alias("d"))
  df.filter("status = 'active' AND date >= '2024
01'")
JOIN
  df1.join(df2, "id")                       # auto-broadcast if small
  df1.join(F.broadcast(df2), "id")          # force broadcast
  df1.join(df2, ["id"], "left")             # outer join
WINDOWS
  w = W.partitionBy("user_id").orderBy(F.desc("ts"))
  df.withColumn("rn", F.row_number().over(w))
WRITE
  df.write.mode("overwrite").format("delta") \
      .partitionBy("date").save("s3://...")
DEBUG
  df.explain("formatted")
  df.printSchema()
  Spark UI > Stages > task durations
  spark.sql("OPTIMIZE delta.`path` ZORDER BY (col)")
```

```
30.3 What Separates Senior from Staff
SENIOR                                STAFF / PRINCIPAL
■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■
"Add a repartition"                  "Repartition to 800 with key X
                                      because we have 100GB shuffle
                                      and 800 partitions hits the
                                      128MB sweet spot; we'll lose
                                      AQE's coalescing but gain
                                      determinism for downstream"
"Use cache"                          "Cache this only if the next 2
                                      operations both need it; cost
                                      is 50GB executor memory, payoff
```

```
                                      is one less S3 scan worth ~$X"
"Spark is fast"                       "Spark is throughput-optimized
                                      for code-heavy pipelines on
                                      TB-scale; for THIS workload
                                      (10GB ad-hoc SQL), BigQuery
                                      would be 10× cheaper and faster"
"It's a skew issue"                  "It's skew on user_id where the
                                      top-100 keys hold 60% of rows;
                                      AQE handles most of it but we
                                      should salt the top-10 keys
                                      because they cross the AQE
                                      threshold and split poorly"
"Use Delta"                          "Delta over Parquet for ACID,
                                      time-travel, MERGE; over Iceberg
                                      because we're Databricks-centric;
                                      over Hudi because we don't need
                                      streaming upserts at this scale"
■ Principal mindset: The staff Spark engineer has an opinion on every layer (storage, compute, format, partition
layout, cluster type) and can defend each opinion with measurement. They also know when to not use Spark —
which is the highest-leverage skill, because most teams over-use it.
 — End of PySpark Notes —
 Next in series: Airflow, dbt, Kafka, Cloud Data Platforms
```


---

[← Back to main](./README.md)
