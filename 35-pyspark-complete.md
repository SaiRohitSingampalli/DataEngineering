# Topic 35 — PySpark Complete End-to-End Reference

> Standalone deep-dive · The full PySpark reference from architecture to interview patterns

[← Back to main](./README.md)

---

> A single-file comprehensive PySpark reference for staff/principal data engineers. Covers architecture through structured streaming, performance tuning, deployment, testing, and interview patterns. ~250 KB; search-friendly.

## About

This chapter is a comprehensive PySpark reference consolidating what the shorter `02-pyspark.md` touches on into one deep standalone. Each sub-section is a `##` H2 chapter (16 chapters). All callouts, patterns, code, and interview angles are preserved. Use the Table of Contents below to jump; use Ctrl-F to find anything.

Target audience: 3-8+ years data engineering experience, preparing for staff/principal-level interviews or production-scale design. Primary Spark version: **Spark 3.5+ / 4.0**.

Pairs with:
- `02-pyspark.md` — concise core-series PySpark chapter
- `12-delta-lake.md` — Delta Lake standalone
- `28-kubernetes-for-data-engineering.md` — Spark on K8s depth
- `14-flink.md` — streaming alternative

## Table of Contents

- [About](#about)
- [Key Concepts Summary](#key-concepts-summary)
- [Spark Version Context (2026)](#spark-version-context-2026)
- [How to Use](#how-to-use)
- [0. Spark Architecture](#0-spark-architecture)
- [1. DataFrames, Columns, Expressions](#1-dataframes-columns-expressions)
- [2. I/O and File Formats](#2-io-and-file-formats)
- [3. Transformations & Actions](#3-transformations-actions)
- [4. Joins & Shuffles](#4-joins-shuffles)
- [5. Aggregations & Window Functions](#5-aggregations-window-functions)
- [6. Partitioning](#6-partitioning)
- [7. UDFs & Pandas UDFs](#7-udfs-pandas-udfs)
- [8. Structured Streaming](#8-structured-streaming)
- [9. MLlib / Spark ML](#9-mllib-spark-ml)
- [10. Performance Tuning](#10-performance-tuning)
- [11. Deployment](#11-deployment)
- [12. Delta Lake & Lakehouse Integration](#12-delta-lake-lakehouse-integration)
- [13. Testing PySpark](#13-testing-pyspark)
- [14. Debugging & Operations](#14-debugging-operations)
- [15. PySpark Interview Patterns](#15-pyspark-interview-patterns)

## Key Concepts Summary

| Concept | What it is |
|---------|------------|
| **Lazy evaluation** | Transformations build a plan; actions trigger execution |
| **DAG** | Directed acyclic graph of transformations |
| **Stages** | Groups of tasks with no shuffle in between |
| **Tasks** | Unit of work on one partition |
| **Shuffle** | Data redistribution across executors (expensive) |
| **Catalyst** | Spark's query optimizer |
| **Tungsten** | Memory + CPU optimization engine |
| **AQE** | Adaptive Query Execution — runtime re-planning |
| **Broadcast join** | Small table replicated to all executors (no shuffle) |
| **Sort-merge join** | Default large-table join (sort both, merge) |
| **Partition** | Data chunk owned by one executor at a time |
| **Narrow transformation** | Each output partition depends on one input (map, filter) |
| **Wide transformation** | Output needs multiple input partitions (groupBy, join) |
| **Checkpoint** | Materialized intermediate; truncates lineage |

## Spark Version Context (2026)

- **Spark 2.x (legacy, 2016-2020):** RDD-centric, limited optimizer. Many enterprises still have 2.x in production for legacy reasons.
- **Spark 3.0+ (2020):** AQE introduced (opt-in), dynamic partition pruning, better Pandas UDFs.
- **Spark 3.1-3.3:** AQE improvements, Structured Streaming improvements, Pandas API on Spark (koalas merge).
- **Spark 3.4 (2023):** Spark Connect introduced (client-server architecture); Protobuf support.
- **Spark 3.5 (2023):** DeepSpeed Distributor, English SDK (experimental), training APIs.
- **Spark 4.0 (2024):** Spark Connect GA, ANSI SQL by default, Variant data type, SQL state codes, string collation.
- **In 2026:** Spark 3.5+ or 4.0 is standard; 2.x legacy is rare but exists.

**Assume 3.5+ unless noted.** Spark Connect is the modern deployment pattern for new work.

## How to Use

**Zero to intermediate** — read sections 0 through 6. These establish the mental model.

**Intermediate to advanced** — sections 7 through 11. UDFs, streaming, tuning, deployment.

**Advanced to staff** — sections 12 through 15. Lakehouse integration, test strategy, debugging, interview patterns.

**For interviews** — study sections 0, 3, 4, 5, 10, 15 the week before.

**For search** — Ctrl-F for any term; this file is designed to be grep-friendly.

---

## 0. Spark Architecture


### The Cluster

```
                 ┌────────────────────────────────────────┐
                 │           CLIENT                        │
                 │   (your laptop / CI / notebook)         │
                 └──────────────────┬─────────────────────┘
                                    │ spark-submit / Spark Connect
                                    ▼
                 ┌────────────────────────────────────────┐
                 │           DRIVER                        │
                 │  • SparkContext / SparkSession          │
                 │  • DAG scheduler                        │
                 │  • Task scheduler                       │
                 │  • Catalyst optimizer                   │
                 │  • Metadata, broadcasts, accumulators   │
                 └──────────────────┬─────────────────────┘
                                    │ requests resources
                                    ▼
                 ┌────────────────────────────────────────┐
                 │       CLUSTER MANAGER                   │
                 │   (K8s / YARN / Standalone / Mesos)     │
                 └──────────────────┬─────────────────────┘
                                    │ allocates
                                    ▼
         ┌──────────────┬───────────┼──────────────┬──────────────┐
         ▼              ▼           ▼              ▼              ▼
     EXECUTOR      EXECUTOR     EXECUTOR      EXECUTOR       EXECUTOR
     • JVM         • JVM        • JVM         • JVM          • JVM
     • Task slots  • Cache      • Shuffle     • Task slots   • Cache
     • Heap mem    • Off-heap   • Local disk  • Heap mem     • Off-heap
```

### Component roles

- **Driver** — your `main()` plus Spark's planning. Owns the SparkSession. Compiles DataFrame operations into a logical plan → optimizes → physical plan → DAG of stages → tasks. Sends tasks to executors. Receives results.
- **Cluster Manager** — K8s, YARN, Mesos (deprecated), or Standalone. Negotiates executor allocation. Spark doesn't care which; the API is the same.
- **Executors** — JVM processes doing the actual work. One per worker node (or more). Store cached data, run tasks, hold shuffle data on local disk.
- **Workers** (YARN/Standalone concept) — physical nodes that host executors.

### Deploy modes

- **Client mode** — driver runs on your laptop or submit node. OK for interactive dev; bad for production (your laptop closing kills the job).
- **Cluster mode** — driver runs inside the cluster (one of the executor nodes becomes the driver host). Standard for production.

---

### SparkSession (the entry point)

```python
from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
        .appName("my_job")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .config("spark.sql.adaptive.skewJoin.enabled", "true")
        .config("spark.sql.shuffle.partitions", "200")
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .getOrCreate()
)
```

`SparkSession` is the modern, unified entry point. (Pre-2.0: `SparkContext`, `SQLContext`, `HiveContext` were separate.)

Access the lower-level API:
```python
sc = spark.sparkContext      # RDD API, broadcasts, accumulators, parallelize
```

---

### The Execution Model

### 1. Logical plan (unresolved → analyzed → optimized)

Your DataFrame code builds a **logical plan**:

```python
df = (spark.read.parquet("s3://bucket/orders")
      .filter(col("status") == "completed")
      .groupBy("customer_id")
      .agg(sum("amount").alias("total")))
```

Nothing executed yet. This is a tree of operations. Catalyst analyzes it, resolves column names, applies rule-based optimizations (predicate pushdown, projection pruning, constant folding, etc.).

Inspect it:
```python
df.explain(True)  # shows parsed, analyzed, optimized, physical plans
```

### 2. Physical plan

Catalyst picks a **physical plan** (which join algorithm, which aggregation strategy, which exchange method). Multiple physical plans are costed; the cheapest wins.

### 3. DAG of stages

The physical plan compiles to a **DAG of stages**:
- **Stage** = sequence of narrow transformations that can run without shuffle
- **Stage boundary** = wherever a shuffle happens (groupBy, join, repartition, distinct, orderBy, etc.)
- **Within a stage** = tasks run in parallel, one per input partition
- **Between stages** = data is shuffled across the network

### 4. Tasks

- Each stage spawns **N tasks** where N = number of input partitions
- Each task processes one partition
- Tasks run in **executor slots** (one slot per core by default)
- Example: 200 partitions, 10 executors × 4 cores = 40 slots → 5 waves of tasks per stage

### 5. Actions trigger execution

```python
df.count()        # action — triggers DAG execution
df.show()         # action
df.write.parquet("output")  # action
df.collect()      # action (DANGEROUS — see below)
```

Transformations (`filter`, `select`, `groupBy`) are lazy; actions force materialization.

---

### Catalyst Optimizer

Catalyst is Spark's query optimizer. It transforms the logical plan through:

### Rule-based optimizations
- **Predicate pushdown** — move filters as close to the source as possible
- **Projection pruning** — only read needed columns
- **Constant folding** — evaluate constants at compile time
- **Null propagation** — simplify `col IS NULL` on non-nullable cols
- **Expression simplification** — `col * 1 + 0` → `col`

### Cost-based optimizations (CBO, if statistics available)
- **Join reordering** — reorder join sequence based on table sizes
- **Join algorithm selection** — broadcast vs sort-merge vs shuffle hash

Catalyst is extensible — you can write custom rules, but almost no one does. The default rules are great.

---

### Tungsten

Tungsten is Spark's **execution engine optimization** layer:

- **Off-heap memory** — avoids JVM GC pressure
- **Cache-aware computation** — operates on contiguous memory for CPU cache efficiency
- **Code generation (Whole-stage codegen)** — generates optimized Java bytecode at runtime, fusing operators
- **Vectorized operations** — for Parquet reads + aggregations

You don't configure Tungsten directly — it's always on in modern Spark.

---

### Adaptive Query Execution (AQE)

AQE is the big post-3.0 win. It **re-plans the query at runtime** based on actual data sizes:

### What AQE does

1. **Coalesce small partitions after shuffle**
   - Spark normally produces `spark.sql.shuffle.partitions` partitions (default 200)
   - AQE merges tiny partitions into reasonable sizes
2. **Convert sort-merge joins to broadcast joins** when runtime statistics show one side is small enough
3. **Skew join handling** — detects skewed partitions and splits them

### Enable AQE (default in 3.2+ but confirm)
```python
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")
spark.conf.set("spark.sql.adaptive.localShuffleReader.enabled", "true")
```

### Key AQE configs
```
spark.sql.adaptive.coalescePartitions.minPartitionNum       # default: parallelism
spark.sql.adaptive.coalescePartitions.initialPartitionNum   # default: shuffle.partitions
spark.sql.adaptive.advisoryPartitionSizeInBytes             # default: 64MB
spark.sql.adaptive.skewJoin.skewedPartitionFactor           # default: 5
spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes # default: 256MB
```

> **🧠 Principal Mindset:** Before AQE (Spark 2.x), tuning Spark meant manually setting shuffle partition counts, manually broadcasting joins, manually salting skewed keys. AQE automates 80% of this. The remaining 20% is still real; see topic 10.

---

### Memory Model (per executor)

```
                EXECUTOR MEMORY
                 ┌──────────────┐
                 │  OFF-HEAP    │  spark.memory.offHeap.size (0 by default)
                 ├──────────────┤
                 │  RESERVED    │  300 MB (fixed)
                 ├──────────────┤
                 │  OTHER       │  (1 - spark.memory.fraction) × usable
                 │              │   • User data structures
                 │              │   • Spark internal metadata
                 ├──────────────┤
                 │  STORAGE +   │  spark.memory.fraction × usable (default 0.6)
                 │  EXECUTION   │   • Cache (RDDs, DataFrames)
                 │   (unified)  │   • Shuffle, join, agg buffers
                 └──────────────┘
                 Total = spark.executor.memory
```

### Key configs
```
spark.executor.memory         # heap size per executor (e.g. 8g)
spark.executor.memoryOverhead # off-heap overhead (default 10% of memory, min 384 MB)
spark.executor.cores          # cores per executor (slots)
spark.memory.fraction         # fraction for cache + execution (default 0.6)
spark.memory.storageFraction  # within above, reserved for cache (default 0.5)
```

### Executor sizing rule of thumb
- **Cores per executor:** 4-5 (above this, GC pressure dominates)
- **Memory per core:** 2-4 GB typical; 8 GB for memory-heavy workloads
- **Overhead:** always leave 10-15% buffer; set higher for Python UDF workloads

Example: 20 nodes, each 32 cores + 128 GB RAM
```
spark.executor.cores = 4
spark.executor.memory = 20g
spark.executor.memoryOverhead = 3g
# → 5-6 executors per node × 20 nodes = 100-120 executors
```

---

### The DAG, Stages, Tasks Walkthrough

Consider:
```python
df = (spark.read.parquet("s3://bucket/orders")
      .filter(col("status") == "completed")
      .join(spark.read.parquet("s3://bucket/customers"), "customer_id")
      .groupBy("country")
      .agg(sum("amount").alias("total"))
      .write.parquet("s3://output/"))
```

**Logical plan:** Read1 → Filter → Join(Read2) → Agg → Write

**Physical plan (simplified):**
- Scan orders (with predicate pushdown)
- Scan customers
- SortMergeJoin (if neither broadcastable)
- HashAggregate (partial)
- Shuffle
- HashAggregate (final)
- Write

**Stages:**
- Stage 1: scan orders, filter, map-side partial-agg + shuffle write
- Stage 2: scan customers, shuffle write
- Stage 3: shuffle read + join + partial-agg + shuffle write
- Stage 4: shuffle read + final-agg + write

**Tasks per stage** = number of partitions. If orders has 500 partitions, stage 1 has 500 tasks.

### Spark UI

Spark exposes a web UI (default port 4040 in client mode) showing:
- **Jobs tab** — one entry per action
- **Stages tab** — stage times, task breakdown, shuffle read/write
- **SQL tab** — query plan visualization + execution metrics
- **Executors tab** — per-executor metrics (CPU, memory, GC, task count)
- **Storage tab** — cached DataFrames + persistence levels
- **Environment tab** — all config values

In production: enable Spark History Server to view post-completion.

---

### RDDs, DataFrames, Datasets

- **RDD** (Resilient Distributed Dataset): low-level, strongly typed Java objects. Rare in modern PySpark; use for custom partitioning or when you need `.mapPartitions` with Python objects.
- **DataFrame**: high-level, schema-aware, Catalyst-optimized. Primary PySpark API.
- **Dataset**: Scala/Java only — typed DataFrame. Not available in PySpark.

### When to drop to RDD in PySpark
- Custom partitioner
- `foreachPartition` for per-partition side effects
- Legacy code

95%+ of modern PySpark is DataFrames.

---

### Spark Connect (3.4+ preview; 4.0 GA)

Spark Connect decouples the client from the Spark cluster:

```
    CLIENT (your Python process)
        │ gRPC
        ▼
    SPARK CLUSTER (driver + executors)
```

Benefits:
- Thin client (no full Spark install)
- Multiple languages (Python, Scala, Rust, Go, R)
- Long-lived cluster shared across clients
- Better versioning (client/server compatibility)

Enable:
```bash
./bin/start-connect-server.sh --packages ...
```

Client:
```python
spark = SparkSession.builder.remote("sc://host:port").getOrCreate()
```

For 2026 architectural work: **Spark Connect is the default pattern** for new deployments.

---

### Interview Angle

> **🎯 Interview Angle — "Explain Spark's execution model."** Staff answer:
> 
> 1. User writes DataFrame ops → builds logical plan (unresolved).
> 2. Catalyst analyzes, resolves names, applies rule-based + cost-based optimizations → optimized logical plan.
> 3. Physical planner selects algorithms (broadcast vs sort-merge, hash vs sort agg) → physical plan.
> 4. Physical plan compiles to DAG of stages (stage boundary = shuffle).
> 5. Each stage spawns tasks (one per partition).
> 6. Driver schedules tasks to executor slots.
> 7. AQE re-plans at runtime based on actual stage statistics (coalesce, broadcast, skew split).
> 8. Actions (write, collect, count) trigger execution.
> 9. Lazy transformations mean the plan is only materialized once per action.

> **⚠️ Warning — `.collect()`:** Pulls ALL rows to the driver. On a 100 GB DataFrame, driver OOMs. For sampling, use `.limit(1000).collect()`. For writes, use `.write.*`. For aggregated results, use `.collect()` only AFTER aggregation.

> **💡 Tip — Spark UI first.** When debugging slow jobs, open Spark UI → SQL tab → identify the slow stage. 80% of problems are visible there without reading code.

---

### Pattern Summary

| Concept | What you need to know |
|---------|------------------------|
| Driver | Your `main` + planner; owns SparkSession; schedules tasks |
| Executor | JVM worker; runs tasks on partitions; caches data |
| Cluster manager | K8s/YARN/Standalone; allocates executors |
| Deploy mode | client (dev) vs cluster (prod) |
| Catalyst | Query optimizer (rule-based + CBO) |
| Tungsten | Memory + CPU execution engine (whole-stage codegen) |
| AQE | Runtime re-planning: coalesce + broadcast + skew |
| Stage | Sequence of narrow transformations between shuffles |
| Task | Unit of work on one partition |
| Lazy evaluation | Transformations plan; actions execute |
| Spark Connect | Client/server architecture (3.4+ GA in 4.0) |

---

## 1. DataFrames, Columns, Expressions


### What a DataFrame is

A DataFrame is:
- A **distributed, partitioned, immutable** collection of rows
- With a **schema** (column names + types)
- Lazily evaluated — operations build a plan
- Backed by **Catalyst** for optimization
- Represented physically as **partitions** across executors

Think "pandas DataFrame but distributed, lazy, and typed."

```python
df = spark.read.parquet("s3://bucket/data/")
df.printSchema()   # inspect schema
df.show(5)         # show first 5 rows (triggers action)
df.count()         # row count (triggers action)
df.columns         # list of column names (metadata only)
df.dtypes          # list of (name, type) tuples
df.schema          # StructType object
```

---

### Creating DataFrames

### From files

```python
# Parquet (preferred for all big data)
df = spark.read.parquet("s3://bucket/path/")
df = spark.read.parquet("s3://bucket/path/*.parquet")  # glob
df = spark.read.parquet("s3://bucket/path/year=2024/month=10/")  # partition filter

# Delta Lake
df = spark.read.format("delta").load("s3://bucket/delta/")

# Iceberg (requires catalog config)
df = spark.read.format("iceberg").load("catalog.db.table")
# or
df = spark.table("catalog.db.table")

# CSV
df = (spark.read
      .option("header", "true")
      .option("inferSchema", "true")     # expensive; prefer explicit schema
      .option("mode", "FAILFAST")        # or PERMISSIVE / DROPMALFORMED
      .csv("s3://bucket/path/*.csv"))

# JSON (one JSON per line, aka "ndjson")
df = spark.read.json("s3://bucket/path/*.json")

# ORC
df = spark.read.orc("s3://bucket/path/")

# JDBC
df = (spark.read
      .format("jdbc")
      .option("url", "jdbc:postgresql://host:5432/db")
      .option("dbtable", "schema.table")
      .option("user", "u").option("password", "p")
      .option("numPartitions", "10")  # parallelism
      .option("partitionColumn", "id").option("lowerBound", "1").option("upperBound", "1000000")
      .load())
```

### From Python objects

```python
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

# From list of tuples (schema inferred — avoid in prod)
df = spark.createDataFrame([(1, "Alice"), (2, "Bob")], ["id", "name"])

# From list of dicts
df = spark.createDataFrame([{"id": 1, "name": "Alice"}])

# With explicit schema (recommended)
schema = StructType([
    StructField("id", IntegerType(), nullable=False),
    StructField("name", StringType(), nullable=True),
])
df = spark.createDataFrame([(1, "Alice"), (2, "Bob")], schema=schema)

# From Pandas DataFrame
import pandas as pd
pdf = pd.DataFrame({"id": [1, 2], "name": ["Alice", "Bob"]})
df = spark.createDataFrame(pdf)   # driver materializes; small DFs only
```

### From existing DataFrames

```python
df2 = df.select("col1", "col2")              # projection
df2 = df.filter(df.col1 > 10)                # row filter
df2 = df.withColumn("new_col", df.col1 * 2)  # add column
df2 = df.drop("col1")                        # drop column
df2 = df.distinct()                          # dedupe (triggers shuffle!)
```

---

### Schema

Schema = `StructType` of `StructField`s. Types are in `pyspark.sql.types`.

### Common types

```python
from pyspark.sql.types import (
    StringType, IntegerType, LongType, ShortType, ByteType,
    FloatType, DoubleType, DecimalType,
    BooleanType, DateType, TimestampType, TimestampNTZType,  # TimestampNTZ is 3.4+
    ArrayType, MapType, StructType, StructField,
    BinaryType, NullType,
    VariantType,  # Spark 4.0+
)

# DecimalType with precision + scale
DecimalType(10, 2)  # 10 digits total, 2 after decimal

# Nested types
ArrayType(StringType())
MapType(StringType(), IntegerType())
StructType([StructField("city", StringType()), StructField("zip", StringType())])
```

### Programmatic schema

```python
schema = StructType([
    StructField("user_id", LongType(), False),
    StructField("name", StringType(), True),
    StructField("tags", ArrayType(StringType()), True),
    StructField("address", StructType([
        StructField("city", StringType()),
        StructField("zip", StringType()),
    ]), True),
])
```

### DDL schema (concise)

```python
schema = "user_id BIGINT NOT NULL, name STRING, tags ARRAY<STRING>, address STRUCT<city:STRING,zip:STRING>"
df = spark.read.schema(schema).json("s3://bucket/data/")
```

**Rule:** Always specify schema in production. `inferSchema` triggers a pre-pass over the data.

---

### Column Objects

A `Column` is an expression — not a value until evaluated.

```python
from pyspark.sql.functions import col, lit, expr

col("name")              # reference existing column
df.name                  # same thing (attribute access)
df["name"]               # same thing (bracket access)
lit(42)                  # literal constant
lit(None).cast("string") # typed NULL literal
expr("some_col + 1")     # SQL expression as Column
```

### Column operations

```python
# Arithmetic
col("price") * col("quantity")
col("x") + 1
col("x") - col("y")

# Comparison → boolean Column
col("age") > 18
col("name") == "Alice"
col("status").isin("A", "B", "C")
col("x").between(1, 10)
col("x").isNull()
col("x").isNotNull()

# Boolean combinators (use & | ~, NOT and/or/not)
(col("age") > 18) & (col("country") == "US")
(col("status") == "A") | (col("status") == "B")
~col("deleted")

# Casting
col("price_str").cast("double")
col("ts").cast(TimestampType())

# Aliasing
col("x").alias("renamed")

# Column access on struct
col("address.city")       # dot syntax
col("address").getField("city")
```

### CASE WHEN

```python
from pyspark.sql.functions import when

df.withColumn("segment", 
    when(col("spend") > 1000, "VIP")
    .when(col("spend") > 100, "regular")
    .otherwise("new")
)

# NULLIF / IFNULL / COALESCE
from pyspark.sql.functions import coalesce, nullif
coalesce(col("a"), col("b"), lit("default"))
```

---

### Core Transformations

### select / selectExpr

```python
df.select("col1", "col2")
df.select(col("col1"), (col("x") + 1).alias("x_plus_1"))

df.selectExpr("col1", "x + 1 AS x_plus_1", "CASE WHEN y > 0 THEN 'pos' ELSE 'neg' END AS sign")
```

### withColumn / withColumnRenamed

```python
df.withColumn("full_name", concat_ws(" ", col("first"), col("last")))
df.withColumn("x", col("x") * 2)       # OVERWRITES existing column
df.withColumnRenamed("old_name", "new_name")

# Multiple columns at once (3.3+)
df.withColumns({
    "x": col("x") * 2,
    "y": col("y") + 1,
})

# Rename multiple (3.4+)
df.withColumnsRenamed({"old_a": "a", "old_b": "b"})
```

### drop / filter / where

```python
df.drop("col1", "col2")

df.filter(col("x") > 10)
df.filter("x > 10")                    # SQL expression as string
df.where(col("x") > 10)                # alias for filter

# Multiple conditions
df.filter((col("x") > 10) & (col("y") == "Alice"))
```

### distinct / dropDuplicates

```python
df.distinct()                          # all-column distinct
df.dropDuplicates(["user_id"])         # keep first row per key (NON-deterministic unless ordered)

# Deterministic "keep latest per user"
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number
w = Window.partitionBy("user_id").orderBy(col("event_time").desc())
df.withColumn("rn", row_number().over(w)).filter(col("rn") == 1).drop("rn")
```

### sort / orderBy

```python
df.orderBy("col1")                     # ascending
df.orderBy(col("col1").desc())
df.orderBy(col("x").desc(), col("y").asc_nulls_last())

# sort is alias for orderBy (global sort, triggers shuffle)
# sortWithinPartitions is cheaper if order within each partition is enough
df.sortWithinPartitions("x")
```

### limit

```python
df.limit(100)
```

### union / unionByName

```python
# Union by column POSITION (DANGEROUS if schemas differ)
df1.union(df2)

# Union by column NAME (preferred, 2.3+)
df1.unionByName(df2)
df1.unionByName(df2, allowMissingColumns=True)   # fill missing with NULL (3.1+)
```

### intersect / exceptAll

```python
df1.intersect(df2)         # set intersection (distinct)
df1.intersectAll(df2)      # bag intersection (keeps duplicates)
df1.exceptAll(df2)         # bag difference
df1.subtract(df2)          # set difference (distinct)
```

---

### Working with Nested Data

### Access struct fields

```python
df.select("address.city")                    # dot notation
df.select(col("address.city").alias("city"))
df.select(col("address").getField("city"))
```

### Flatten struct

```python
df.select("id", "address.*")  # flatten all struct fields
```

### Explode arrays

```python
from pyspark.sql.functions import explode, explode_outer, posexplode

df.select("id", explode(col("tags")).alias("tag"))

# explode drops rows with empty/NULL arrays
# explode_outer keeps them (tag = NULL)
# posexplode adds (pos, tag)
```

### Array / map functions

```python
from pyspark.sql.functions import (
    array, array_contains, array_distinct, array_intersect, array_union,
    size, element_at, array_sort, slice,
    map_from_entries, map_keys, map_values, map_entries,
    transform, filter as array_filter, aggregate, exists, forall
)

df.filter(array_contains(col("tags"), "premium"))
df.withColumn("num_tags", size(col("tags")))
df.withColumn("first_tag", element_at(col("tags"), 1))  # 1-indexed!
```

### Higher-order functions (3.0+)

```python
# Transform each element
df.withColumn("upper_tags", transform(col("tags"), lambda x: upper(x)))

# Filter elements
df.withColumn("long_tags", array_filter(col("tags"), lambda x: length(x) > 5))

# Aggregate (fold)
df.withColumn("tag_lengths_sum",
    aggregate(col("tags"), lit(0), lambda acc, x: acc + length(x)))

# Any / all
df.withColumn("has_long", exists(col("tags"), lambda x: length(x) > 10))
```

### from_json / to_json

```python
from pyspark.sql.functions import from_json, to_json, schema_of_json

json_schema = "struct<name:string,age:int>"
df.withColumn("parsed", from_json(col("json_str"), json_schema))
df.withColumn("json_str", to_json(col("struct_col")))
```

---

### SQL Interface

### Register and query

```python
df.createOrReplaceTempView("my_table")   # session-scoped
df.createGlobalTempView("my_table")      # cross-session (prefix: global_temp.)

result = spark.sql("""
    SELECT customer_id, SUM(amount) AS total
    FROM my_table
    WHERE status = 'completed'
    GROUP BY customer_id
""")
```

### Permanent tables (requires Hive support or catalog)

```python
df.write.saveAsTable("db.my_table")                   # managed table
df.write.option("path", "s3://...").saveAsTable(...)  # external table
```

### DSL vs SQL

Equivalent:

```python
# DSL
(df.filter(col("status") == "completed")
   .groupBy("customer_id")
   .agg(sum("amount").alias("total")))

# SQL
spark.sql("""
    SELECT customer_id, SUM(amount) AS total
    FROM my_table
    WHERE status = 'completed'
    GROUP BY customer_id
""")
```

Catalyst optimizes both the same way. Choose by team preference / tooling.

> **💡 Tip:** For complex transformations, DSL is more composable (CTEs via chained methods, reusable helpers). For analyst-friendly code, SQL wins. Many teams use SQL for business logic + DSL for scaffolding.

---

### Casting and NULL handling

```python
# Type cast
df.withColumn("x", col("x").cast("double"))
df.withColumn("ts", col("ts_str").cast(TimestampType()))

# NULL handling
df.na.drop()                              # drop any row with NULL
df.na.drop("any")                         # any (default)
df.na.drop("all")                         # all cols NULL
df.na.drop(subset=["col1", "col2"])
df.na.drop(thresh=3)                      # keep rows with ≥ 3 non-NULL

df.na.fill(0)                             # fill all numeric NULLs with 0
df.na.fill({"col1": 0, "col2": "unknown"})
df.na.replace("old", "new", subset=["col1"])
```

---

### Common Function Catalog

```python
from pyspark.sql import functions as F

# Aggregates
F.count("*"), F.count_distinct("col"), F.approx_count_distinct("col")
F.sum, F.avg, F.min, F.max, F.mean, F.stddev, F.variance
F.first, F.last
F.collect_list, F.collect_set

# String
F.concat, F.concat_ws, F.length, F.lower, F.upper, F.trim, F.ltrim, F.rtrim
F.substring, F.split, F.regexp_extract, F.regexp_replace
F.lpad, F.rpad, F.repeat, F.reverse
F.initcap  # Title Case

# Math
F.round, F.ceil, F.floor, F.abs, F.sqrt, F.log, F.exp, F.pow

# Date / time
F.current_date, F.current_timestamp, F.now
F.date_add, F.date_sub, F.datediff, F.months_between
F.year, F.month, F.dayofmonth, F.dayofweek, F.weekofyear, F.hour, F.minute, F.second
F.date_format, F.to_date, F.to_timestamp
F.date_trunc, F.unix_timestamp, F.from_unixtime

# Conditional
F.when, F.coalesce, F.greatest, F.least, F.nvl, F.nvl2

# Nullsafe
col1.eqNullSafe(col2)   # NULL-safe equality
```

See full function list: `help(F)` or [Spark docs](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/functions.html).

---

### Caching & Persistence

```python
df.cache()                        # MEMORY_AND_DISK (default)
df.persist()                      # same
df.persist(StorageLevel.MEMORY_ONLY)
df.persist(StorageLevel.MEMORY_AND_DISK_SER)   # serialized → less memory

df.unpersist()                    # free from cache

# Check cached
df.is_cached
```

**When to cache:**
- DataFrame reused in multiple actions
- Caching takes memory from execution; monitor Spark UI Storage tab

**When NOT to cache:**
- One-shot pipelines where DF used once
- When memory is tight (will cause disk spills + eviction)

```python
# Checkpoint truncates lineage; writes to reliable storage
spark.sparkContext.setCheckpointDir("s3://bucket/checkpoints/")
df.checkpoint()           # eager, triggers computation
df.localCheckpoint()      # on executor local disk; faster but not fault-tolerant
```

**Use checkpoint** when iterative algorithms grow lineage (ML, graph) — otherwise plan rebuild per action gets slow.

---

### Interview Patterns

> **🎯 Interview Angle — "DataFrame vs RDD."** Staff answer: DataFrames are schema-aware, optimized by Catalyst + Tungsten, and expose a declarative API. RDDs are low-level, type-erased in PySpark (just Python objects), and bypass the optimizer. Modern PySpark is 99% DataFrames. Drop to RDD only for custom partitioners, `mapPartitions` with Python state, or legacy code.

> **🎯 Interview Angle — "When would you use SQL vs DSL in PySpark?"** They're equivalent — Catalyst optimizes both the same. Choose by team workflow. SQL: better for analysts, dbt-adjacent transforms, familiar queries. DSL: better for programmatic composition (loops, functions returning DataFrames), reusable helpers, type-safe-ish construction.

> **⚠️ Warning — `df.show()` in production.** `.show(100)` on a complex query triggers full computation to materialize the first 100 rows. In a long pipeline, this re-runs expensive upstream work. For debugging, add `.cache()` first or use `.limit(100).show()`.

> **⚠️ Warning — Nested `withColumn` chains.** Chaining 20 `withColumn` calls creates an increasingly expensive logical plan (Spark re-analyzes the whole chain each time). Prefer `.select()` with many columns at once or `withColumns({...})` (3.3+) for bulk additions.

---

### Pattern Summary

| Operation | Method |
|-----------|--------|
| Read file | `spark.read.{format}("path")` |
| Create from Python | `spark.createDataFrame(data, schema)` |
| Projection | `.select("col1", "col2")` |
| Add column | `.withColumn("name", expr)` |
| Filter | `.filter(col > N)` or `.where("col > N")` |
| Dedup | `.distinct()` or `.dropDuplicates(["key"])` |
| Union | `.unionByName(other)` |
| Explode array | `F.explode(col("arr"))` |
| CASE WHEN | `F.when(...).when(...).otherwise(...)` |
| Casting | `.cast("type")` |
| Null fill | `.na.fill(...)` |
| Register for SQL | `.createOrReplaceTempView("name")` |
| Cache | `.cache()` or `.persist(level)` |
| Checkpoint | `.checkpoint()` (truncates lineage) |

---

## 2. I/O and File Formats


### Format Comparison

| Format | Compressed | Columnar | Schema | Splittable | Compression ratio | Typical use |
|--------|-----------|----------|--------|------------|-------------------|-------------|
| **Parquet** | yes | yes | embedded | yes | high (3-10x) | OLAP, lakehouse default |
| **ORC** | yes | yes | embedded | yes | high (3-10x) | Hive-ecosystem OLAP |
| **Avro** | yes | row | embedded | yes | medium | Kafka messages, long-term storage |
| **Delta** | Parquet underneath | yes | embedded + metadata | yes | high | ACID lakehouse (Databricks) |
| **Iceberg** | Parquet/ORC/Avro | yes | embedded + metadata | yes | high | Open table format |
| **Hudi** | Parquet | yes | embedded + metadata | yes | high | Streaming-first lakehouse |
| **CSV** | gzip optional | row | header or none | yes with uncompressed; no with gzip | low | Interchange, exports |
| **JSON** | gzip optional | row | per-record | one-per-line → yes | low | Nested data, logs |

**Rule:** Parquet is the default for lake storage. CSV/JSON only for interchange with external systems.

---

### Parquet

### Read

```python
df = spark.read.parquet("s3://bucket/path/")
df = spark.read.parquet("s3://bucket/path/*.parquet")
df = spark.read.parquet(
    "s3://bucket/path/year=2024/month=10/",
    "s3://bucket/path/year=2024/month=11/",
)  # multiple paths

# With explicit schema (faster — skips schema inference pass)
schema = StructType([...])
df = spark.read.schema(schema).parquet("s3://bucket/path/")

# Options
df = (spark.read
      .option("mergeSchema", "false")      # don't merge schemas across files (default false, SAFER)
      .option("pathGlobFilter", "*.parquet")
      .parquet("s3://bucket/path/"))
```

### Write

```python
df.write.parquet("s3://bucket/output/")
df.write.mode("overwrite").parquet("s3://bucket/output/")
df.write.mode("append").parquet("s3://bucket/output/")
df.write.mode("ignore").parquet("s3://bucket/output/")       # skip if exists
df.write.mode("error").parquet("s3://bucket/output/")        # fail if exists (default)

# Partitioned write
df.write.partitionBy("year", "month").parquet("s3://bucket/output/")

# Target file size
df.coalesce(20).write.parquet(...)        # fewer, larger files
df.repartition(100, "key").write.parquet(...)  # repartition before write

# Compression
df.write.option("compression", "snappy").parquet(...)   # default
df.write.option("compression", "zstd").parquet(...)     # better ratio, slower
df.write.option("compression", "gzip").parquet(...)
df.write.option("compression", "lz4").parquet(...)      # fastest
```

### Partition pruning

```python
# Hive-style partitioning: s3://bucket/table/year=2024/month=10/day=15/
# Filters on partition columns prune at planning time
df = spark.read.parquet("s3://bucket/table/")
df.filter((col("year") == 2024) & (col("month") == 10))
# → Only reads matching directories; verify in Spark UI → SQL plan → "PartitionFilters"
```

### Column pruning

Spark reads ONLY the columns referenced in your query. Verify in `explain()` → look for `ReadSchema` showing a subset.

### Predicate pushdown

Filters on non-partition columns are pushed into Parquet readers using row-group min/max statistics:

```python
df = spark.read.parquet("s3://bucket/table/")
df.filter(col("amount") > 1000)
# → Row groups with max(amount) ≤ 1000 are skipped
# → See "PushedFilters" in explain() output
```

---

### Delta Lake

Delta is Parquet + a transaction log (`_delta_log/` directory with JSON files).

### Setup

```python
spark = (SparkSession.builder
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.2.0")
    .getOrCreate())
```

### Read

```python
df = spark.read.format("delta").load("s3://bucket/delta_table/")
df = spark.table("delta_table")                           # if registered

# Time travel
df = spark.read.format("delta").option("versionAsOf", 10).load("s3://...")
df = spark.read.format("delta").option("timestampAsOf", "2024-10-15").load("s3://...")

# CDC (change data feed)
df = (spark.read.format("delta")
      .option("readChangeFeed", "true")
      .option("startingVersion", 5)
      .load("s3://..."))
```

### Write

```python
df.write.format("delta").save("s3://bucket/delta_table/")
df.write.format("delta").mode("overwrite").save(...)
df.write.format("delta").mode("append").save(...)

# Partitioned
df.write.format("delta").partitionBy("date").save(...)

# Replace schema (schema evolution)
df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(...)
df.write.format("delta").mode("append").option("mergeSchema", "true").save(...)
```

### MERGE (upsert)

```python
from delta.tables import DeltaTable

target = DeltaTable.forPath(spark, "s3://bucket/delta_table/")

(target.alias("t").merge(
    df.alias("s"),
    "t.id = s.id")
    .whenMatchedUpdate(set={"name": "s.name", "updated_at": "s.updated_at"})
    .whenNotMatchedInsertAll()
    .whenMatchedDelete(condition="s.op = 'DELETE'")
    .execute())
```

### Delta operations

```python
target.vacuum(retentionHours=168)   # delete files unused > 7 days
target.history().show()
target.optimize().executeCompaction()                # Databricks: file compaction
target.optimize().executeZOrderBy("user_id", "event_date")  # Databricks: Z-ordering
```

See the sibling `bigdata-notes-flat/12-delta-lake.md` for depth.

---

### Iceberg

Iceberg has a 3-layer metadata (catalog → metadata → manifests → data files).

### Setup

```python
spark = (SparkSession.builder
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtension")
    .config("spark.sql.catalog.my_catalog", "org.apache.iceberg.spark.SparkCatalog")
    .config("spark.sql.catalog.my_catalog.type", "hive")   # or glue, nessie, rest, polaris
    .config("spark.sql.catalog.my_catalog.uri", "thrift://...")
    .config("spark.jars.packages", "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.0")
    .getOrCreate())
```

### Read / write

```python
df = spark.read.format("iceberg").load("my_catalog.db.table")
df = spark.table("my_catalog.db.table")

df.writeTo("my_catalog.db.table").create()         # CTAS
df.writeTo("my_catalog.db.table").append()
df.writeTo("my_catalog.db.table").overwritePartitions()
df.writeTo("my_catalog.db.table").replace()

# Time travel
spark.read.option("snapshot-id", "1234567890").format("iceberg").load(...)
spark.read.option("as-of-timestamp", "2024-10-15").format("iceberg").load(...)
```

### MERGE (via SQL)

```python
spark.sql("""
    MERGE INTO my_catalog.db.target t
    USING my_catalog.db.updates s
    ON t.id = s.id
    WHEN MATCHED THEN UPDATE SET name = s.name
    WHEN NOT MATCHED THEN INSERT (id, name) VALUES (s.id, s.name)
""")
```

### Schema evolution + partition evolution

Iceberg's selling point. Change partition spec without rewriting data:

```python
spark.sql("ALTER TABLE my_catalog.db.table ADD PARTITION FIELD days(event_time)")
```

See the sibling `bigdata-notes-flat/11-iceberg.md`.

---

### Hudi

Hudi (Copy-on-Write or Merge-on-Read) with streaming CDC.

```python
hudi_options = {
    "hoodie.table.name": "orders",
    "hoodie.datasource.write.recordkey.field": "order_id",
    "hoodie.datasource.write.partitionpath.field": "country",
    "hoodie.datasource.write.precombine.field": "updated_at",
    "hoodie.datasource.write.operation": "upsert",   # or 'insert', 'bulk_insert'
    "hoodie.datasource.write.table.type": "COPY_ON_WRITE",  # or MERGE_ON_READ
}

df.write.format("hudi").options(**hudi_options).mode("append").save("s3://bucket/hudi/orders")

# Read snapshot
df = spark.read.format("hudi").load("s3://bucket/hudi/orders")

# Incremental query
df = (spark.read.format("hudi")
      .option("hoodie.datasource.query.type", "incremental")
      .option("hoodie.datasource.read.begin.instanttime", "20241015000000")
      .load(...))
```

See `bigdata-notes-flat/13-hudi.md`.

---

### ORC

```python
df = spark.read.orc("s3://bucket/orc/")
df.write.orc("s3://bucket/output/")

# Same options as Parquet (partitionBy, compression, mode)
# Compression: zlib (default), snappy, lzo, zstd, lz4
```

ORC is still common in Hive/Hadoop ecosystems; Parquet dominates on the cloud.

---

### Avro

Row-oriented; schema-rich; typical for Kafka messages.

```python
# Requires org.apache.spark:spark-avro_2.12 package
spark = SparkSession.builder.config("spark.jars.packages", "org.apache.spark:spark-avro_2.12:3.5.0").getOrCreate()

df = spark.read.format("avro").load("s3://bucket/avro/")
df.write.format("avro").save("s3://bucket/output/")

# With explicit Avro schema
avro_schema = '{"type":"record","name":"Order",...}'
df = spark.read.format("avro").option("avroSchema", avro_schema).load(...)
```

For Kafka reading with Avro + Schema Registry, see topic 08.

---

### CSV

```python
df = (spark.read
      .option("header", "true")
      .option("inferSchema", "true")       # OR specify .schema()
      .option("sep", ",")                  # delimiter
      .option("quote", '"')
      .option("escape", '"')
      .option("multiLine", "true")         # for quoted fields with newlines
      .option("dateFormat", "yyyy-MM-dd")
      .option("timestampFormat", "yyyy-MM-dd HH:mm:ss")
      .option("mode", "PERMISSIVE")        # PERMISSIVE / DROPMALFORMED / FAILFAST
      .option("columnNameOfCorruptRecord", "_corrupt")
      .option("nullValue", "")             # treat as NULL
      .option("emptyValue", "")
      .csv("s3://bucket/path/*.csv"))

df.write.option("header", "true").csv("s3://bucket/output/")

# Compressed CSV (gzip is NOT splittable — avoid for big files)
df.write.option("compression", "gzip").csv(...)
df.write.option("compression", "bzip2").csv(...)   # splittable but slow
```

> **⚠️ Warning:** `inferSchema=true` on a large CSV reads the whole thing TWICE (once to infer types, once to parse). Specify schema explicitly in production.

---

### JSON (ndjson preferred)

```python
# One JSON object per line (ndjson) — splittable
df = spark.read.json("s3://bucket/path/*.json")

# Options
df = (spark.read
      .option("multiLine", "true")        # entire file is one JSON array/object (NOT splittable)
      .option("mode", "PERMISSIVE")
      .option("columnNameOfCorruptRecord", "_corrupt")
      .json(...))

# With explicit schema
df = spark.read.schema(schema).json(...)
```

### Reading nested JSON

Spark infers the full nested schema. Access via dot notation (see topic 01).

```python
# "pretty" JSON files (multi-line objects) require multiLine=true
# Each JSON file becomes one partition → bad parallelism if few large files
```

---

### JDBC

```python
# Read
df = (spark.read
      .format("jdbc")
      .option("url", "jdbc:postgresql://host:5432/mydb")
      .option("driver", "org.postgresql.Driver")
      .option("dbtable", "schema.table")
      # OR: .option("query", "SELECT ... FROM ... WHERE ...")
      .option("user", "u")
      .option("password", "p")
      # PARALLELISM:
      .option("numPartitions", "10")
      .option("partitionColumn", "id")
      .option("lowerBound", "1")
      .option("upperBound", "1000000")
      # FETCH SIZE:
      .option("fetchsize", "10000")
      .load())

# Write
(df.write
   .format("jdbc")
   .option("url", "jdbc:postgresql://host:5432/mydb")
   .option("dbtable", "schema.target")
   .option("user", "u").option("password", "p")
   .option("batchsize", "10000")
   .option("isolationLevel", "READ_COMMITTED")
   .mode("append")
   .save())
```

### JDBC parallelism

- WITHOUT `numPartitions`: ONE partition → ONE connection → ONE JDBC query → bottleneck
- WITH `numPartitions + partitionColumn`: Spark splits range into N partitions, N parallel queries

Partition column must be numeric or date; evenly distributed helps.

### JDBC gotchas

- Default fetchsize is often too small (10 for Postgres JDBC) → set to 1000-10000
- JDBC write is EXTREMELY slow for large data; use S3 + COPY (Redshift), bulk loaders (Postgres), or lakehouse formats instead

---

### Kafka (streaming source + batch read)

```python
# Batch read (reads all data between offsets)
df = (spark.read
      .format("kafka")
      .option("kafka.bootstrap.servers", "broker1:9092,broker2:9092")
      .option("subscribe", "my-topic")
      .option("startingOffsets", "earliest")  # or "latest" or {"topic":{"0":100}}
      .option("endingOffsets", "latest")
      .load())

# Columns: key, value, topic, partition, offset, timestamp, timestampType
# key and value are BINARY — cast to string / parse from Avro / Protobuf

df.select(
    col("key").cast("string"),
    col("value").cast("string"),
    "topic", "partition", "offset", "timestamp"
).show()
```

See topic 08 for streaming Kafka patterns.

---

### File writing patterns

### Single-file output (small result sets)

```python
df.coalesce(1).write.parquet("s3://bucket/output/")
# → one file: part-00000-...parquet
```

> **⚠️ Warning:** `coalesce(1)` collapses all data to one task → one executor must hold everything. OOM risk for large data.

### Target file size

Target ~128-512 MB per Parquet file. Too small = many small files + metadata overhead. Too large = poor parallelism on read.

```python
# Repartition to control output file count
df.repartition(100).write.parquet(...)                # 100 files
df.repartition(100, "country").write.parquet(...)     # 100 files, hash-partitioned by country
df.coalesce(20).write.parquet(...)                    # merge to 20 files (NO shuffle)
```

### Dynamic partition overwrite

By default, `.mode("overwrite")` deletes ALL target data. For partition-level overwrite:

```python
spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

(df.write
   .partitionBy("country", "date")
   .mode("overwrite")
   .parquet("s3://bucket/table/"))
# → only overwrites partitions present in df; others untouched
```

### Save as table (managed/external)

```python
# Managed table (Spark owns storage lifecycle)
df.write.saveAsTable("db.my_table")

# External table (specify path)
(df.write
   .option("path", "s3://bucket/data/")
   .saveAsTable("db.my_table"))

df.write.insertInto("db.my_table")  # append to existing table (by position!)
```

> **⚠️ Warning:** `insertInto` matches by column POSITION, not name. Easy source of silent data corruption.

### Overwrite behavior

| Mode | Behavior |
|------|----------|
| `error` / `errorifexists` | Fail if path exists (default) |
| `append` | Add new files to existing path |
| `overwrite` | Delete all + write new |
| `overwrite` with dynamic partition mode | Delete only matching partitions |
| `ignore` | Skip write if path exists |

---

### Reading tips

### Small files problem

Reading 10,000 × 1 MB Parquet files is MUCH slower than 1 × 10 GB. Each file = scheduling + metadata overhead.

**Fix at write time:** target 128-512 MB files via `repartition` or `coalesce`.

**Fix at read time:** 
```python
spark.conf.set("spark.sql.files.maxPartitionBytes", "128m")    # Spark groups small files into partitions up to this size
spark.conf.set("spark.sql.files.openCostInBytes", "4194304")   # cost model for grouping (default 4 MB)
```

### Schema evolution during read

```python
# Parquet files with slightly different schemas
spark.read.option("mergeSchema", "true").parquet(...)
# → scans metadata of all files to unify schemas; EXPENSIVE for many files
```

Delta / Iceberg handle schema evolution natively via metadata.

### Partition filter not applied?

Common pitfall: partition column data type mismatch.

```python
# Partition path: year=2024/   (string)
df.filter(col("year") == 2024)              # may NOT prune (type mismatch)
df.filter(col("year") == "2024")            # prunes correctly
df.filter(col("year").cast("int") == 2024)  # casts; may defeat pruning
```

**Fix:** check `explain()` → look for `PartitionFilters` with your condition. If missing, there's a mismatch.

---

### Interview Patterns

> **🎯 Interview Angle — "Explain partition pruning vs predicate pushdown."** They're different levels of filtering. Partition pruning uses the directory structure (Hive-style `col=value/`) to skip reading entire directories. Predicate pushdown uses the file-format's internal statistics (Parquet row-group min/max, Delta data-skipping stats, Iceberg manifests) to skip reading row groups within files. Both reduce I/O. Verify in `explain()`: look for `PartitionFilters:` and `PushedFilters:`.

> **🎯 Interview Angle — "Why Parquet over CSV?"** Columnar storage (only read needed columns), rich compression (3-10x), splittable with gzip equivalent, predicate pushdown via row-group stats, embedded schema, native type support (no string parsing). CSV is for interchange, not storage.

> **🔥 Production Scenario — Dynamic Partition Overwrite Save.** A pipeline wrote to a partitioned table with `.mode("overwrite")` — nuked all historical partitions on every run. Switched to dynamic partition overwrite mode (`spark.sql.sources.partitionOverwriteMode=dynamic`): overwrote only the partitions present in the current batch. 6 months of historical data preserved. Lesson: default overwrite mode in Spark is PATH-level, not partition-level.

---

### Pattern Summary

| Operation | Pattern |
|-----------|---------|
| Read Parquet | `spark.read.parquet(path)` |
| Read with schema | `spark.read.schema(s).parquet(path)` |
| Partition pruning | Hive-style `col=value/` dirs + filter on col |
| Write partitioned | `df.write.partitionBy("col").parquet(path)` |
| Dynamic partition overwrite | `spark.sql.sources.partitionOverwriteMode=dynamic` |
| Target file size | `df.repartition(N).write...` or `coalesce(N)` |
| Delta MERGE | `DeltaTable.forPath(...).merge(...).whenMatched...` |
| Iceberg write | `df.writeTo("catalog.db.table").append()` |
| JDBC parallel | `numPartitions + partitionColumn + lowerBound + upperBound` |
| Kafka batch | `spark.read.format("kafka").option("subscribe",...).load()` |

---

## 3. Transformations & Actions


### The Core Distinction

**Transformations** build the execution plan. They are lazy — nothing runs.
**Actions** trigger execution and return a result (to the driver or to storage).

```python
df = spark.read.parquet("...")              # transformation (lazy)
df2 = df.filter(col("x") > 10)              # transformation (lazy)
df3 = df2.groupBy("country").count()        # transformation (lazy)
df3.show()                                  # ACTION — triggers everything above
df3.show()                                  # ACTION — triggers everything AGAIN (unless cached)
```

---

### Narrow vs Wide Transformations

### Narrow transformations
- Each output partition depends on ONE input partition
- NO shuffle needed
- Can be pipelined within a single stage
- Examples: `filter`, `map`, `withColumn`, `select`, `drop`, `cast`

```python
# All of these run in ONE stage
(df
 .filter(col("x") > 10)
 .withColumn("y", col("y") * 2)
 .select("a", "b", "y"))
```

### Wide transformations
- Output partition depends on MULTIPLE input partitions
- Requires SHUFFLE (data moved across network)
- Introduces a STAGE BOUNDARY
- Examples: `groupBy`, `join`, `distinct`, `orderBy`, `repartition`, `window`

```python
# Each groupBy / join introduces a stage boundary + shuffle
(df
 .groupBy("country")   # ← shuffle
 .agg(sum("amount"))
 .join(other, "country"))  # ← another shuffle
```

### Why it matters

- **Shuffle = expensive** (network I/O, disk I/O for shuffle files, serialization)
- Each shuffle = one of the dominant cost drivers in Spark jobs
- Reducing wide transformations is often the fastest performance win

---

### Lazy Evaluation Deep Dive

```python
df = spark.read.parquet("...")           # nothing happens
df = df.filter(col("x") > 10)            # still nothing
df = df.withColumn("y", col("y") + 1)    # still nothing
df.count()                               # NOW it runs
```

### Why lazy?

Lazy evaluation enables Catalyst to:
1. **Fuse** multiple narrow transformations into one physical operator (whole-stage codegen)
2. **Push down** filters to the data source
3. **Prune** projections (only read used columns)
4. **Reorder** joins
5. **Choose** join algorithms based on size estimates

If Spark were eager (like pandas), it couldn't do any of this.

### Multiple actions = multiple executions

```python
df = spark.read.parquet("...").filter(col("x") > 10)

df.count()        # execute 1 — reads data, filters, counts
df.show()         # execute 2 — reads data, filters, shows (unless cached)
df.write.parquet(...)  # execute 3 — reads data, filters, writes
```

**Fix:** `.cache()` the DataFrame if reused:

```python
df.cache()
df.count()        # execute + cache
df.show()         # cache hit (no re-read)
df.write.parquet(...)   # cache hit
df.unpersist()    # free memory when done
```

---

### Full Action Catalog

### Collect-type actions (return to driver — DANGEROUS for large data)

```python
df.collect()              # entire DataFrame to driver as list of Rows
df.take(10)               # first 10 rows
df.first()                # first row
df.head()                 # first row (same as first)
df.head(5)                # first 5 rows
df.tail(10)               # last 10 rows (3.0+)
df.toPandas()             # entire DataFrame to driver as pandas DataFrame
```

> **⚠️ Warning:** `.collect()`, `.toPandas()` on 10 GB → driver OOM. Use `.limit(N)` first.

### Count / summary actions

```python
df.count()                        # row count
df.describe().show()              # min/max/avg/stddev per numeric column
df.summary().show()               # like describe + percentiles
df.summary("count", "min", "max", "50%", "95%").show()
df.schema                         # schema (metadata — no action)
df.explain()                      # explain plan (no action)
```

### Write actions

```python
df.write.parquet(...)
df.write.format("delta").save(...)
df.write.saveAsTable(...)
```

### Debug / show actions

```python
df.show()                     # 20 rows default
df.show(100)                  # 100 rows
df.show(100, truncate=False)  # don't truncate long strings
df.show(vertical=True)        # vertical format

df.printSchema()              # schema (metadata — triggers no computation)
```

### Checkpoint action

```python
df.checkpoint()               # materializes to reliable storage; TRIGGERS execution
df.localCheckpoint()          # materializes to executor local disk
```

### foreach / foreachPartition

```python
# Side-effect only — iterate rows (one per task, no shuffle)
df.foreach(lambda row: send_to_api(row))

# Prefer foreachPartition for batched I/O:
def process_partition(iterator):
    conn = open_connection()
    try:
        for row in iterator:
            conn.write(row)
    finally:
        conn.close()

df.foreachPartition(process_partition)
```

**Use case:** writing to a system Spark has no connector for.

---

### Lineage

Spark tracks the operation lineage. On executor failure, Spark can recompute lost partitions from the lineage.

```python
df = spark.read.parquet("input/")
df2 = df.filter(...)
df3 = df2.join(...)
df4 = df3.groupBy(...).agg(...)

df4.rdd.toDebugString()   # see lineage (RDD-level)
df4.explain(mode="formatted")   # better: see Spark plan
```

### Long lineage problem

In iterative algorithms (ML, graph), lineage grows each iteration. Plan compilation gets slow; failure recomputation gets expensive.

**Fix: checkpoint periodically** to truncate lineage:

```python
spark.sparkContext.setCheckpointDir("s3://bucket/checkpoints/")

for i in range(100):
    df = iterate(df)
    if i % 10 == 0:
        df.checkpoint()   # truncates lineage; writes to reliable storage
```

### Checkpoint vs cache

| | cache | checkpoint |
|-|-------|------------|
| Where | memory + spill to disk | reliable storage (S3/HDFS) |
| Lineage | preserved | **truncated** |
| On failure | recompute from lineage | re-read from checkpoint path |
| Cost | memory | storage + write cost |
| Speed | fast reuse | slower, but fault-tolerant |
| Use | reused DF in a job | long iterative lineage |

Combine both: `df.cache().checkpoint()` for in-memory reuse + lineage truncation.

---

### Common Patterns

### Reusing a DataFrame multiple times

```python
# WRONG — reads source twice
df = spark.read.parquet("...").filter(...)
count = df.count()
df.write.parquet("output/")

# RIGHT — cache
df = spark.read.parquet("...").filter(...).cache()
count = df.count()        # materializes + caches
df.write.parquet("output/")   # cache hit
df.unpersist()
```

### Conditional branching (write multiple outputs)

```python
df = spark.read.parquet("...").filter(...).cache()

df.filter(col("country") == "US").write.parquet("us/")
df.filter(col("country") == "EU").write.parquet("eu/")
df.filter(col("country") == "APAC").write.parquet("apac/")

df.unpersist()
```

Alternative: use `partitionBy("country")` for one-pass dynamic partitioned write.

### Driver-side iteration

```python
# WRONG — pulls to driver on each iteration
for country in df.select("country").distinct().collect():
    sub = df.filter(col("country") == country["country"])
    sub.write.parquet(f"output/{country['country']}/")

# RIGHT — one partitioned write
df.write.partitionBy("country").parquet("output/")
```

### Union multiple sources

```python
from functools import reduce
dfs = [spark.read.parquet(p) for p in paths]
combined = reduce(lambda a, b: a.unionByName(b, allowMissingColumns=True), dfs)
```

For many paths, `spark.read.parquet(*paths)` is cleaner and more efficient (reads in parallel).

---

### Lazy evaluation gotchas

### Printing debug info with `.show()`

```python
# This triggers a full action, re-running upstream work:
df = spark.read.parquet("huge/").filter(...).groupBy(...).agg(...)
df.show()           # runs the full query

# To debug without re-running:
df.cache()
df.show()           # runs + caches
df.explain()        # no execution
```

### "Why is my counter zero?"

Variables defined on the driver and read in UDFs/map functions won't be mutated as expected. Lambdas close over driver values; executor mutations don't propagate back.

```python
# WRONG
counter = 0
def process(row):
    global counter
    counter += 1  # happens on executor; driver counter stays 0

df.foreach(process)
print(counter)   # 0
```

**Fix: use accumulators** (one of the few RDD-level APIs needed in DataFrames):

```python
counter = spark.sparkContext.accumulator(0)

def process(row):
    counter.add(1)

df.foreach(process)
print(counter.value)   # correct count
```

### Checkpoint doesn't help on cached DataFrames alone

```python
# This caches but doesn't truncate lineage
df.cache()

# This truncates AND caches (materializes to checkpoint dir)
df.cache().checkpoint()
```

---

### Transformation Reference

### Narrow (no shuffle)

```python
df.select(...)
df.selectExpr(...)
df.withColumn(...)
df.withColumnRenamed(...)
df.drop(...)
df.filter(...), df.where(...)
df.sample(fraction, seed)
df.rdd.map(...)                # drops to RDD level
df.limit(N)                    # IF followed by action that doesn't require global ordering
```

### Wide (shuffle)

```python
df.distinct()                  # shuffle to dedupe
df.dropDuplicates([...])       # shuffle
df.groupBy(...).agg(...)       # shuffle on group key
df.join(other, ...)            # shuffle both sides (unless broadcast)
df.orderBy(...), df.sort(...)  # GLOBAL sort → shuffle
df.repartition(N)              # explicit shuffle
df.repartition(N, "col")       # hash-shuffle by col
df.repartitionByRange(N, "col")# range-shuffle (for sorted output)
```

### Hybrid

```python
df.coalesce(N)                 # NARROW if N < current partitions (reduces); WIDE if N > (upcasts shuffle)
df.sortWithinPartitions(...)   # narrow — sorts within each partition, no shuffle
```

---

### When Spark DOES shuffle (checklist)

- `groupBy().agg()` — always
- `join()` — unless broadcast or partitioned-by-join-key
- `distinct()`, `dropDuplicates()` — always
- `orderBy()` with no PARTITION BY — always (global sort)
- `window().OVER(PARTITION BY ...)` — once per partition clause
- `repartition()` — explicit shuffle
- `union()` — NO shuffle (concatenates partitions)
- `intersect()`, `except()` — shuffle

Verify in Spark UI → SQL plan: shuffle operators show as `Exchange hashpartitioning(...)` or similar.

---

### Explain Plans

Four levels:
```python
df.explain()                  # simple physical plan
df.explain(True)              # ALL plans (parsed, analyzed, optimized, physical)
df.explain(mode="simple")     # default
df.explain(mode="formatted")  # organized by operator with details
df.explain(mode="cost")       # with stats + cost estimates (if CBO stats available)
df.explain(mode="extended")   # same as explain(True)
```

### Reading a plan

Bottom-up: data flows from leaf (scan) up through operators to the top (output).

```
== Physical Plan ==
AdaptiveSparkPlan isFinalPlan=false
+- HashAggregate(keys=[country#1], functions=[sum(amount#2)])
   +- Exchange hashpartitioning(country#1, 200)         ← SHUFFLE
      +- HashAggregate(keys=[country#1], functions=[partial_sum(amount#2)])
         +- Project [country#1, amount#2]
            +- Filter (isnotnull(status#3) AND (status#3 = completed))
               +- FileScan parquet [country#1, amount#2, status#3] ...
                     PartitionFilters: []
                     PushedFilters: [IsNotNull(status), EqualTo(status, completed)]
                     ReadSchema: struct<country:string, amount:double, status:string>
```

**What to look for:**
- `PartitionFilters` — partition pruning working?
- `PushedFilters` — predicates pushed to source?
- `ReadSchema` — column pruning working?
- `Exchange ...` — where shuffles happen
- `BroadcastExchange` / `BroadcastHashJoin` — small join broadcast
- `SortMergeJoin` — large join
- `AdaptiveSparkPlan isFinalPlan=false` — AQE will re-plan at runtime

---

### Interview Patterns

> **🎯 Interview Angle — "Explain narrow vs wide transformations."** Narrow: output partition depends on one input (filter, map, select) — no shuffle, fits in one stage. Wide: output depends on multiple inputs (groupBy, join, distinct) — triggers a shuffle, stage boundary. Shuffle is the #1 cost in Spark; reducing wide transformations is often the fastest tuning win.

> **🎯 Interview Angle — "What's the difference between cache and checkpoint?"** Cache stores DataFrames in memory (+ spill to disk) for reuse within the same job; lineage is preserved. Checkpoint writes to reliable storage (S3/HDFS) and TRUNCATES lineage. Cache for reuse; checkpoint for long iterative lineage. For both: `df.cache().checkpoint()`.

> **🔥 Production Scenario — Multiple writes without cache.** A pipeline wrote filtered subsets to 5 output paths. Reading source + applying filter 5 times (once per write). Added `.cache()` once + `.unpersist()` at end: runtime dropped from 2 hours to 25 minutes. Lesson: when the same transformed DataFrame feeds multiple actions, cache it.

> **🔥 Production Scenario — The sneaky shuffle.** `df.join(small_df, "key").groupBy("key").agg(...)`. Both ops shuffle on the same key, but Spark did it twice. Added `df.repartition("key")` before to let the join + groupBy share the same shuffle, then AQE coalesce afterwards. 30% reduction. (AQE can sometimes detect this automatically; verify in plan.)

---

### Pattern Summary

| Concept | Rule |
|---------|------|
| Transformation | Lazy; returns DataFrame; builds plan |
| Action | Eager; triggers execution; returns value or writes |
| Narrow | No shuffle (filter, select, withColumn) |
| Wide | Shuffle (groupBy, join, distinct, orderBy, repartition) |
| Cache | Reuse in same job; memory + disk; lineage preserved |
| Checkpoint | Fault-tolerant materialization; truncates lineage |
| `.collect()` | DANGEROUS — pulls all to driver |
| `.show()` | Triggers full action; re-reads unless cached |
| explain() | Read bottom-up; check PartitionFilters, PushedFilters, Exchange |
| AQE | Runtime re-plan: coalesce + broadcast + skew split |

---

## 4. Joins & Shuffles


### Join Types

```python
df1.join(df2, "key")                     # INNER (default)
df1.join(df2, "key", "inner")
df1.join(df2, "key", "left")             # LEFT OUTER
df1.join(df2, "key", "right")            # RIGHT OUTER
df1.join(df2, "key", "outer")            # FULL OUTER (= "full")
df1.join(df2, "key", "left_semi")        # rows in df1 that MATCH df2 (no df2 columns)
df1.join(df2, "key", "left_anti")        # rows in df1 that DON'T match df2
df1.crossJoin(df2)                       # cartesian — SET spark.sql.crossJoin.enabled=true

# Join on different column names
df1.join(df2, df1.user_id == df2.id, "inner")

# Join on multiple columns
df1.join(df2, ["user_id", "date"], "inner")
df1.join(df2, (df1.user_id == df2.user_id) & (df1.date == df2.date))

# Join on expressions
df1.join(df2, df1.date.between(df2.start_date, df2.end_date))
```

### Semi/anti joins

- `left_semi`: like `WHERE EXISTS` — rows from df1 WITH match in df2; df2 columns NOT in output
- `left_anti`: like `WHERE NOT EXISTS` — rows from df1 WITHOUT match in df2

**Prefer semi/anti over inner+distinct** for existence checks — no duplication, cleaner plan.

---

### Join Algorithms

Spark picks ONE of three physical join algorithms:

### 1. Broadcast Hash Join (BHJ) — small × large

```
┌────────────┐
│ Small df   │ ──→ sent to every executor (broadcast)
└────────────┘
                                   ▼
┌─────────────────────────────────┐
│ Large df (unchanged partitions) │ ──→ probe hash table locally
└─────────────────────────────────┘
```

- **NO SHUFFLE** of the large side (huge win)
- Small side must fit in each executor's memory
- Default threshold: `spark.sql.autoBroadcastJoinThreshold = 10MB`
- Can broadcast up to ~8 GB in modern Spark (depends on executor memory)

Force broadcast:
```python
from pyspark.sql.functions import broadcast
df1.join(broadcast(df2), "key")

# OR via SQL hint
spark.sql("SELECT /*+ BROADCAST(df2) */ * FROM df1 JOIN df2 USING (key)")
```

Disable auto-broadcast:
```python
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")
```

### 2. Sort-Merge Join (SMJ) — large × large (DEFAULT for big tables)

```
Both sides partitioned by key (shuffle)
       ↓
Both sides sorted by key within each partition
       ↓
Merge: walk both sides in sorted order, emit matches
```

- Default large-table join
- Shuffles BOTH sides (expensive)
- Scales to any size

### 3. Shuffle Hash Join (SHJ) — medium × medium

```
Both sides partitioned by key (shuffle)
       ↓
Build hash table on smaller side; probe with larger
       ↓
No sort required
```

- Faster than SMJ when one side fits in memory per partition
- Spark 3.0+ enables SHJ when the smaller side is < a threshold

Enable:
```python
spark.conf.set("spark.sql.join.preferSortMergeJoin", "false")   # prefer SHJ when possible
```

### 4. Broadcast Nested Loop Join

Cartesian or non-equi join when one side is tiny. Rare; avoid.

### 5. AQE Join Optimization

With AQE enabled (default 3.2+):
- **Dynamic BHJ promotion:** Runtime statistics may convert a planned SMJ to BHJ if actual data shrinks after upstream filters.
- **Skew join handling:** detects skewed partitions and splits them (see below).

Check actual algorithm used in Spark UI → SQL plan.

---

### Verify Which Algorithm Spark Picked

```python
df1.join(df2, "key").explain()
```

Look for:
- `BroadcastHashJoin` ✓
- `SortMergeJoin`
- `ShuffleHashJoin`
- `BroadcastExchange` (prepares broadcast)
- `Exchange hashpartitioning(...)` (shuffle for SMJ/SHJ)

---

### When Broadcast Fails

Catalyst skips broadcast when:
- Both sides are large (> threshold)
- Statistics are missing (unknown table sizes — Delta/Iceberg/Parquet with stats usually OK)
- OUTER join with wrong side (RIGHT OUTER can't broadcast LEFT side; LEFT OUTER can't broadcast LEFT side without workaround)

Force stats computation:
```python
spark.sql("ANALYZE TABLE my_table COMPUTE STATISTICS FOR ALL COLUMNS")
```

For external files (not registered as tables):
```python
# Collect size hint on-the-fly
small = spark.read.parquet(small_path)
spark.sparkContext.setJobDescription("Compute small size")
# or just use broadcast() explicitly
```

---

### Data Skew

Skew = one key has disproportionately many rows → one partition balloons → one task takes forever → stragglers.

### Diagnosis

- Spark UI → stage shows one task 10x slower
- "Shuffle Read Size / Records" distribution is wildly uneven
- SMJ with huge shuffle read on one task

### AQE skew handling (3.0+, enabled by default in 3.2+)

```python
spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")
spark.conf.set("spark.sql.adaptive.skewJoin.skewedPartitionFactor", "5")   # partition > 5x median
spark.conf.set("spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes", "256m")
```

Spark detects skewed partitions and SPLITS them, replicating the small side to match.

### Manual skew handling: salting

If AQE doesn't help (e.g., non-join skew, or you want control):

```python
from pyspark.sql.functions import rand, concat, lit

# Add salt to the skewed side
N_SALTS = 100
large_salted = large.withColumn("salt", (rand() * N_SALTS).cast("int"))

# Replicate the small side N_SALTS times
small_replicated = (small
    .withColumn("salt", F.explode(F.array([lit(i) for i in range(N_SALTS)]))))

# Join on (key, salt)
result = large_salted.join(small_replicated, ["key", "salt"])
```

**Trade-off:** small side blows up by N_SALTS × (so needs to fit); large side's skewed partition splits into N_SALTS smaller ones.

### Alternative: isolate hot keys

```python
# Separate the hot keys and handle them differently
hot_keys = {"user_1", "user_2"}  # known hot keys

large_hot = large.filter(col("key").isin(*hot_keys))
large_cold = large.filter(~col("key").isin(*hot_keys))

# Broadcast join for hot (small result set per hot key)
result_cold = large_cold.join(small, "key")
result_hot = large_hot.join(broadcast(small.filter(col("key").isin(*hot_keys))), "key")

result = result_cold.unionByName(result_hot)
```

---

### Shuffle Internals

### What happens on shuffle

1. **Shuffle write phase** (end of previous stage)
   - Map tasks hash input rows by partition key
   - Write to local disk, one file per output partition (actually: one file per executor per shuffle, with index)
   - Metadata registered with driver

2. **Shuffle read phase** (start of next stage)
   - Reduce tasks fetch their assigned partition's data from all previous map tasks
   - Over the network (or local disk if same executor)
   - Deserialize, process

### Shuffle configs

```python
# Number of partitions post-shuffle (BIG one)
spark.conf.set("spark.sql.shuffle.partitions", "200")   # default 200

# With AQE, this is the INITIAL partition count; AQE coalesces smaller
spark.conf.set("spark.sql.adaptive.coalescePartitions.initialPartitionNum", "500")
spark.conf.set("spark.sql.adaptive.advisoryPartitionSizeInBytes", "128m")

# Shuffle file compression (default: snappy)
spark.conf.set("spark.shuffle.compress", "true")
spark.conf.set("spark.io.compression.codec", "lz4")   # options: lz4, snappy, zstd

# Service for external shuffle (K8s: DaemonSet; YARN: ESS)
spark.conf.set("spark.shuffle.service.enabled", "true")   # requires setup
```

### Shuffle sizing rule

Rule of thumb: aim for ~100-200 MB per shuffle partition after shuffle.

- Total shuffle size / shuffle.partitions = per-partition size
- Too few partitions → each task does too much → OOM or straggler
- Too many → scheduler overhead + many small shuffle files

Example: 100 GB shuffle, 200 partitions → 500 MB/partition (too big; set partitions higher). With AQE, let AQE coalesce.

### Shuffle service

- **Internal shuffle service:** each executor serves shuffle files; executor dying = shuffle files lost
- **External shuffle service (ESS):** separate daemon on each node; executors can die without losing shuffle data
  - YARN: configured once per cluster
  - K8s: deploy as DaemonSet; required for dynamic allocation
  - Alternative on K8s: shuffle tracking in Spark 3.1+ (keeps shuffle-holding executors alive)

---

### Bucketing (pre-shuffle at write time)

Bucketing writes data pre-partitioned by hash of a column. Future joins on that column skip the shuffle.

```python
# Write bucketed
(df.write
   .bucketBy(100, "user_id")           # 100 hash buckets on user_id
   .sortBy("user_id")                  # optional: sort within bucket
   .saveAsTable("my_table"))

# Future joins on user_id skip shuffle (if both tables bucketed with same scheme)
df1 = spark.table("my_table1")      # bucketed 100 on user_id
df2 = spark.table("my_table2")      # bucketed 100 on user_id
df1.join(df2, "user_id")            # NO SHUFFLE
```

**Caveats:**
- Only works for TABLES (not raw file paths)
- Both tables must have the SAME number of buckets AND the SAME column
- Managed by Hive-style metastore; limited support outside
- Iceberg has its own bucket partitioning (preferred modern approach)

In modern lakehouse: Iceberg's hidden bucket partitioning > Spark's bucketing table feature.

---

### Join Partitioning Strategy

Spark avoids shuffle if both sides are already partitioned on the join key.

```python
# These will shuffle before join
large.join(small, "user_id")

# Pre-partition both sides on user_id — join may skip shuffle
large_p = large.repartition(200, "user_id")
small_p = small.repartition(200, "user_id")
large_p.join(small_p, "user_id")  # may still shuffle if partitioner differs
```

In practice, Spark's optimizer doesn't trust that both are partitioned the same way; `repartition(N, col)` + join might not avoid shuffle unless the physical plan recognizes compatible partitioning (via bucketing or `HashPartitioner` on RDDs).

**Reliable way to skip shuffle:** bucketing (above) or Iceberg bucket partitioning.

---

### Pre-filter + Pre-aggregate Before Join

Reduce join data volume by pushing filters + aggregations BEFORE joining:

```python
# WRONG: join first, filter after
result = (df1.join(df2, "key")
             .filter(col("date") >= '2024-10-01'))

# RIGHT: filter first, then join
result = (df1.filter(col("date") >= '2024-10-01')
             .join(df2, "key"))

# Even better: Catalyst USUALLY pushes filters down automatically; verify in explain()
```

Catalyst does predicate pushdown automatically for most cases. Explicit filtering before helps readability + guarantees.

---

### Join Specific Hints

```python
# SQL hints
spark.sql("SELECT /*+ BROADCAST(small) */ ... FROM large JOIN small ...")
spark.sql("SELECT /*+ MERGE(df1, df2) */ ... FROM df1 JOIN df2 ...")
spark.sql("SELECT /*+ SHUFFLE_HASH(df2) */ ... FROM df1 JOIN df2 ...")
spark.sql("SELECT /*+ SHUFFLE_REPLICATE_NL(small) */ ... FROM large JOIN small ON large.key BETWEEN small.start AND small.end")

# DSL hints (3.2+)
df1.hint("broadcast").join(df2, "key")
df1.join(df2.hint("broadcast"), "key")
```

Available hints: `BROADCAST`, `MERGE`, `SHUFFLE_HASH`, `SHUFFLE_REPLICATE_NL`.

---

### Common Patterns

### Left anti-join to find missing

```python
# Find users in Users that have no orders
Users.join(Orders, "user_id", "left_anti")

# Equivalent to: NOT EXISTS (SELECT 1 FROM Orders ...)
```

### Deduplicate while joining (semi-join)

```python
# Users that have at least one order (no duplication from matching orders)
Users.join(Orders, "user_id", "left_semi")
```

### Multi-key join

```python
Users.join(Events, (Users.user_id == Events.user_id) & 
                   (Users.date == Events.date), "left")
```

### Range join (BETWEEN)

```python
# Match events to time windows
Events.join(Windows, 
    (Events.timestamp >= Windows.start) & (Events.timestamp < Windows.end),
    "inner")

# This produces a BROADCAST NESTED LOOP JOIN (slow for large × large)
# For big data: pre-bucket by discretized time, or use IcebergBucketTransform
```

### Join with aggregation (reduce before join)

```python
# Aggregate first, then join (cheaper than join + agg)
daily_sales = Orders.groupBy("date", "country").agg(sum("amount").alias("sales"))
result = daily_sales.join(Countries, "country")
```

### Fact + many dimensions

```python
# Idiomatic: start with fact, join dimensions one at a time
# Catalyst can broadcast each small dim individually
result = (Fact
    .join(broadcast(DimDate), "date_id")
    .join(broadcast(DimProduct), "product_id")
    .join(broadcast(DimCustomer), "customer_id"))
```

Each small dim broadcasts; fact table is shuffled once (or not at all if small enough).

---

### Debugging Joins

### Spark UI SQL tab

Look for:
- Join type (BroadcastHashJoin, SortMergeJoin, ...)
- Rows read / written at each stage
- Shuffle read size + spilled bytes (spill = ran out of memory)
- Per-task duration distribution (skew signal)

### Stage with one slow task

Classic skew. Verify in SQL tab or Executors tab:
- Max task time much > median
- One partition shuffle read much > others

### Join explode (rows multiplied)

Check for 1-to-many relationships you didn't expect:

```python
df1.join(df2, "key").count()  
# vs
df1.count()
df2.count()

# If output > df1.count() × avg_matches_per_key, you have cardinality explosion
```

Fix: dedupe one side; use semi-join if you don't need df2 columns.

---

### Common Mistakes

### 1. Using `.collect()` after join to "check"

```python
# Loads everything to driver
df1.join(df2, "key").collect()
```

Use `.limit(10).show()` instead.

### 2. Join on non-nullable comparison

```python
# NULLs don't match in equality (SQL semantics)
df1.join(df2, df1.col == df2.col)   # rows with NULL col excluded

# NULL-safe join: use eqNullSafe
df1.join(df2, df1.col.eqNullSafe(df2.col))
```

### 3. Not deduplicating join result columns

```python
df1.join(df2, df1.id == df2.id).select("id")  
# AMBIGUOUS — both have "id"

# Fix: use string form (collapses matching cols)
df1.join(df2, "id").select("id")

# OR disambiguate
df1.join(df2, df1.id == df2.id).select(df1.id)
```

### 4. Broadcasting too-large tables

Setting broadcast threshold too high → broadcast fails with OOM on driver (serializing the broadcast) or executor (holding it in memory).

Monitor: Spark UI → broadcast size in SQL tab.

### 5. Repartition before small join

```python
# WRONG: forces shuffle for small join that would've broadcast
large.repartition(200, "key").join(small, "key")

# RIGHT: let Catalyst decide
large.join(broadcast(small), "key")
```

---

### Interview Patterns

> **🎯 Interview Angle — "Walk me through the three join algorithms."** 
> 1. **BroadcastHashJoin:** small side replicated to every executor; large side's partitions probe locally; NO shuffle of large side. Default threshold ~10 MB; raise if memory allows.
> 2. **SortMergeJoin:** both sides shuffled by key, then sorted, then merged. Default for large × large.
> 3. **ShuffleHashJoin:** both shuffled; build hash table on smaller side. Faster than SMJ when the smaller side fits per-partition in memory.
> AQE can promote planned SMJ → BHJ at runtime if actual stats shrink.

> **🎯 Interview Angle — "How do you handle skew?"** First rely on AQE's skew join (enabled by default in 3.2+). It detects partitions >5x median or >256 MB and splits them, replicating the small side. If that's insufficient (e.g., non-join skew), manually salt the key: add random salt to large side, replicate small side across all salts, join on (key, salt). Trade-off: small side blows up by N_salts. For known hot keys, isolate them: broadcast the hot subset, SMJ the cold.

> **🔥 Production Scenario — The Join That Didn't Broadcast.** A pipeline joined a 100 GB fact with an 8 MB dim. Expected BHJ; got SMJ (6 hours). Root cause: Catalyst didn't have size statistics for the dim (external parquet without `ANALYZE`). Fix: `.hint("broadcast")` on the small side. Runtime: 15 min. Lesson: always hint-broadcast when you know a table is small; don't rely on stats being present.

> **🔥 Production Scenario — Salted Fix.** User_id = `0` was 40% of events (null-as-zero bug upstream). AQE skew split helped but still slow. Salted large side with N=100, replicated small side 100x: 20-min stage dropped to 2 min. Fixed upstream nulls afterward. Lesson: AQE handles mild skew; extreme skew needs salting.

---

### Pattern Summary

| Join need | Technique |
|-----------|-----------|
| Inner / outer | `df1.join(df2, "key", "type")` |
| EXISTS | `left_semi` |
| NOT EXISTS | `left_anti` |
| Small × large | Catalyst auto-broadcast OR `.hint("broadcast")` |
| Large × large | Sort-merge (default); AQE may promote to BHJ |
| Skewed key | AQE skew join (default); manually salt if needed |
| Pre-sorted/partitioned | Bucketing (table-level) or Iceberg bucket partitioning |
| NULL-safe join | `.eqNullSafe()` |
| Range join | BETWEEN in join condition; may need discretization for perf |
| Many dims | Broadcast each dim; one SMJ for fact (or none if facts also small) |

---

## 5. Aggregations & Window Functions


### groupBy / agg

### Basic aggregation

```python
from pyspark.sql import functions as F

df.groupBy("country").count()

df.groupBy("country").agg(
    F.count("*").alias("rows"),
    F.sum("amount").alias("total"),
    F.avg("amount").alias("avg_amount"),
    F.min("amount").alias("min_amount"),
    F.max("amount").alias("max_amount"),
    F.countDistinct("user_id").alias("unique_users"),
    F.approx_count_distinct("user_id", 0.05).alias("approx_users"),  # HLL
    F.stddev("amount").alias("std"),
    F.variance("amount").alias("var"),
    F.collect_list("item").alias("items"),       # duplicates kept
    F.collect_set("item").alias("unique_items"), # distinct
    F.first("name").alias("first_name"),         # NON-deterministic unless sorted
    F.last("name").alias("last_name"),
    F.skewness("amount").alias("skew"),
    F.kurtosis("amount").alias("kurt"),
)
```

### Multi-column grouping

```python
df.groupBy("country", "year").agg(F.sum("amount").alias("total"))
```

### Dict-style agg

```python
df.groupBy("country").agg({"amount": "sum", "user_id": "count"})
# → columns renamed to "sum(amount)" and "count(user_id)"
```

### Conditional aggregation

```python
df.groupBy("country").agg(
    F.sum(F.when(col("status") == "completed", col("amount")).otherwise(0)).alias("completed_total"),
    F.count(F.when(col("status") == "completed", 1)).alias("completed_count"),
)
```

---

### Approximate Aggregates

### approx_count_distinct (HyperLogLog)

```python
F.approx_count_distinct("user_id")            # default rsd (error) = 5%
F.approx_count_distinct("user_id", 0.01)      # rsd 1% (more accurate, more memory)
```

MUCH cheaper than exact `countDistinct` at scale.

### approx_percentile / percentile_approx

```python
F.percentile_approx("amount", 0.5)                           # median
F.percentile_approx("amount", [0.5, 0.9, 0.95, 0.99])        # multiple percentiles
F.percentile_approx("amount", 0.99, 10000)                   # accuracy param (higher = better, more memory)
```

### Exact percentile (3.1+)

```python
F.percentile("amount", 0.5)                   # exact; expensive — sorts
```

---

### Pivot

```python
# Pivot: long → wide
df.groupBy("product").pivot("quarter").sum("sales")

# → Columns: product, Q1, Q2, Q3, Q4

# Explicit pivot column values (faster — Spark doesn't scan to discover)
df.groupBy("product").pivot("quarter", ["Q1", "Q2", "Q3", "Q4"]).sum("sales")

# Multiple aggregates
df.groupBy("product").pivot("quarter").agg(
    F.sum("sales").alias("total"),
    F.avg("price").alias("avg"),
)
# → Columns: product, Q1_total, Q1_avg, Q2_total, Q2_avg, ...
```

**Pivot gotcha:** without explicit values list, Spark runs a job to discover distinct values before planning. For big data, provide the list.

### Unpivot (manual, until Spark 3.4)

Spark 3.4+ has native `unpivot`:
```python
# 3.4+
df.unpivot(ids=["product"], values=["Q1", "Q2", "Q3", "Q4"], 
           variableColumnName="quarter", valueColumnName="sales")
```

Pre-3.4 pattern (UNION ALL via `stack`):
```python
df.selectExpr("product", "stack(4, 'Q1', Q1, 'Q2', Q2, 'Q3', Q3, 'Q4', Q4) AS (quarter, sales)")
```

---

### GROUPING SETS, ROLLUP, CUBE

### ROLLUP — hierarchical totals

```python
df.rollup("country", "year").sum("sales").show()

# Produces:
# (country, year)        → leaf-level totals
# (country, NULL)        → country-level totals
# (NULL, NULL)           → grand total
```

### CUBE — all combinations

```python
df.cube("country", "year").sum("sales").show()

# Produces:
# (country, year)
# (country, NULL)
# (NULL, year)
# (NULL, NULL)
```

### GROUPING SETS — explicit control (via SQL)

```python
spark.sql("""
    SELECT country, year, SUM(sales) AS total
    FROM orders
    GROUP BY GROUPING SETS (
        (country, year),
        (country),
        (year),
        ()
    )
""")
```

### grouping() + grouping_id()

```python
# Identify which column is NULL due to rollup (vs actually NULL)
df.rollup("country", "year").agg(
    F.sum("sales").alias("total"),
    F.grouping("country").alias("is_rollup_country"),
    F.grouping("year").alias("is_rollup_year"),
    F.grouping_id("country", "year").alias("rollup_id"),
)
```

---

### Window Functions

Window functions compute values across rows WITHOUT collapsing (unlike aggregates).

### Window spec

```python
from pyspark.sql import Window

# PARTITION BY: scope the window
w = Window.partitionBy("country")

# PARTITION BY + ORDER BY: enables ranking + ordered aggregates
w = Window.partitionBy("country").orderBy(col("sales").desc())

# With explicit frame
w = (Window.partitionBy("country")
     .orderBy("date")
     .rowsBetween(Window.unboundedPreceding, Window.currentRow))

# Range frame (value-based, not row-count-based)
w = (Window.partitionBy("country")
     .orderBy("date")
     .rangeBetween(-7, 0))  # last 7 days (if date is numeric or ordered)
```

**Frame types:**
- `rowsBetween(start, end)` — count-based (physical offset)
- `rangeBetween(start, end)` — value-based (requires numeric ORDER BY)
- `Window.unboundedPreceding`, `Window.unboundedFollowing`, `Window.currentRow`

**Default frames:**
- ORDER BY present → `RANGE BETWEEN unboundedPreceding AND currentRow`
- No ORDER BY → `ROWS BETWEEN unboundedPreceding AND unboundedFollowing`

---

### Ranking functions

```python
from pyspark.sql.functions import row_number, rank, dense_rank, ntile, percent_rank, cume_dist

df.withColumn("rn", row_number().over(w))        # 1, 2, 3, 4, 5
df.withColumn("rk", rank().over(w))              # 1, 2, 2, 4, 5 (gap after ties)
df.withColumn("drk", dense_rank().over(w))       # 1, 2, 2, 3, 4 (no gap)
df.withColumn("quartile", ntile(4).over(w))      # 1, 1, 2, 2, 3, 3, 4, 4
df.withColumn("prk", percent_rank().over(w))     # 0.0, 0.25, 0.5, 0.75, 1.0
df.withColumn("cd", cume_dist().over(w))         # cumulative distribution 0..1
```

### Top-N per group

```python
w = Window.partitionBy("country").orderBy(col("sales").desc())

top_3 = (df.withColumn("rn", row_number().over(w))
           .filter(col("rn") <= 3)
           .drop("rn"))
```

Use `row_number` for exactly-N; `dense_rank` for all tied-at-rank-N.

### LAG / LEAD

```python
from pyspark.sql.functions import lag, lead

w = Window.partitionBy("user_id").orderBy("event_time")

df.withColumn("prev_event", lag("event_type", 1).over(w))
df.withColumn("next_event", lead("event_type", 1).over(w))
df.withColumn("prev_prev", lag("event_type", 2, "none").over(w))  # default when none
```

### Running / cumulative aggregates

```python
w_run = Window.partitionBy("user_id").orderBy("date").rowsBetween(Window.unboundedPreceding, Window.currentRow)

df.withColumn("cum_spend", F.sum("amount").over(w_run))
df.withColumn("cum_count", F.count("*").over(w_run))
```

### Moving / rolling aggregates

```python
# Last 7 rows (CURRENT + 6 preceding)
w_7row = Window.partitionBy("user_id").orderBy("date").rowsBetween(-6, 0)
df.withColumn("roll_7_sum", F.sum("amount").over(w_7row))

# Last 7 DAYS (value-based) — requires date_val expressed as number
w_7day = (Window.partitionBy("user_id")
          .orderBy(F.col("date").cast("timestamp").cast("long"))
          .rangeBetween(-86400 * 7, 0))  # 7 days in seconds
df.withColumn("roll_7d_sum", F.sum("amount").over(w_7day))
```

### FIRST_VALUE / LAST_VALUE (careful!)

```python
w = Window.partitionBy("user_id").orderBy("date")

# First value in partition
df.withColumn("first_date", F.first("date").over(w))

# "Last value so far" — because default frame is UNBOUNDED PRECEDING to CURRENT
df.withColumn("last_date_so_far", F.last("date").over(w))

# TRUE last value in partition — need UNBOUNDED FOLLOWING
w_full = (Window.partitionBy("user_id").orderBy("date")
          .rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing))
df.withColumn("true_last_date", F.last("date").over(w_full))
```

> **⚠️ Warning:** `last()` default frame is a classic gotcha. Explicitly specify the frame when you need "last in partition."

---

### Common Window Patterns

### Latest row per key

```python
w = Window.partitionBy("user_id").orderBy(col("event_time").desc())

latest = (df.withColumn("rn", row_number().over(w))
            .filter(col("rn") == 1)
            .drop("rn"))
```

### Difference from previous row

```python
w = Window.partitionBy("user_id").orderBy("date")
df.withColumn("days_since_prev", F.datediff(col("date"), F.lag("date").over(w)))
df.withColumn("amount_diff", col("amount") - F.lag("amount").over(w))
```

### Gap detection

```python
# Flag rows where gap from previous > 7 days
w = Window.partitionBy("user_id").orderBy("date")
df.withColumn("prev_date", F.lag("date").over(w))
  .withColumn("is_new_session", F.when(
      F.datediff(col("date"), col("prev_date")) > 7, 1
  ).otherwise(0))
```

### Sessionization (running SUM of new-session flag)

```python
w_sess = Window.partitionBy("user_id").orderBy("date").rowsBetween(Window.unboundedPreceding, Window.currentRow)
df.withColumn("session_id", F.sum("is_new_session").over(w_sess))
```

### Running total within partition

```python
w = Window.partitionBy("user_id").orderBy("date").rowsBetween(Window.unboundedPreceding, Window.currentRow)
df.withColumn("cum_amount", F.sum("amount").over(w))
```

### Nth highest per group

```python
w = Window.partitionBy("country").orderBy(col("salary").desc())
df.withColumn("rk", F.dense_rank().over(w)).filter(col("rk") == 3)
```

### Gaps-and-islands (consecutive detection)

See `../leetcode-sql-150/11-advanced-patterns.md` — same trick in Spark:

```python
# Group consecutive dates per user
w = Window.partitionBy("user_id").orderBy("date")

df.withColumn("rn", F.row_number().over(w))
  .withColumn("grp", F.datediff("date", F.lit("2000-01-01")) - col("rn"))
  .groupBy("user_id", "grp")
  .agg(F.min("date").alias("start"), F.max("date").alias("end"), F.count("*").alias("streak"))
```

---

### Performance of Window Functions

### Each distinct Window spec = one shuffle

```python
# BAD: 3 shuffles
df.withColumn("rn", F.row_number().over(w1))
  .withColumn("sum", F.sum("x").over(w2))
  .withColumn("avg", F.avg("y").over(w3))

# BETTER: group by window spec
# Only one shuffle for operations with the SAME window spec
df.withColumn("rn", F.row_number().over(w1)) \
  .withColumn("rank", F.rank().over(w1))     # same w → same shuffle
```

Catalyst merges operations sharing a window spec into one window operator.

### Window on large partitions = slow

If partition values are highly skewed (e.g., one user has 10M rows while others have 10), one task becomes a bottleneck.

**AQE doesn't help window skew directly.** Options:
- Pre-aggregate (if the window is for aggregates)
- Rewrite as join (shuffle + aggregate instead)
- Split hot keys and reunion

### Avoid full-partition windows when possible

```python
# BAD: window of ALL rows per user
w = Window.partitionBy("user_id")
df.withColumn("user_total", F.sum("amount").over(w))

# BETTER: groupBy + join back
totals = df.groupBy("user_id").agg(F.sum("amount").alias("user_total"))
df.join(totals, "user_id")
```

Catalyst usually can't rewrite window → agg+join. The group-by version is often faster for pure aggregation.

Use windows when you need PER-ROW output tied to windowed computation (running totals, LAG, LEAD, rank).

---

### Interview Patterns

> **🎯 Interview Angle — "Explain the three ranking functions."** `row_number`: unique sequential (1,2,3,4). `rank`: shared on ties with gap (1,2,2,4). `dense_rank`: shared on ties without gap (1,2,2,3). Pick by whether ties should (a) break arbitrarily (row_number), (b) tie with a visible rank gap (rank), or (c) tie compactly (dense_rank). For "top N per group" when ties should count as tied: dense_rank. For strict top N: row_number.

> **🎯 Interview Angle — "How does Spark execute a window function?"** Each distinct window specification causes a shuffle on the partition-by columns (to co-locate rows that share a partition value) + a sort by the order-by columns + apply the window operator per partition. Multiple window functions sharing a spec share the shuffle + sort. Window ops are more expensive than groupBy + join when you only need aggregates per group; use windows when you need ROW-LEVEL output.

> **🔥 Production Scenario — The LAST_VALUE Trap.** A pipeline computed `LAST_VALUE(status)` per user to get "most recent status." Values appeared off. Root cause: default frame is `UNBOUNDED PRECEDING TO CURRENT ROW` — "last value seen so far = current row". Fix: `.rowsBetween(unboundedPreceding, unboundedFollowing)`. Lesson: window frames aren't just a performance knob; they're correctness.

> **🔥 Production Scenario — Window Skew.** User activity table: 90% of rows belonged to a bot account. Running-total window per user: one task took 2 hours; others completed in 30s. Fix: separated the bot's data, ran window on the small set + the normal set independently, union'd results. Lesson: AQE doesn't split window shuffles; manual intervention needed for extreme skew in windows.

---

### Pattern Summary

| Need | Technique |
|------|-----------|
| Count distinct (exact) | `countDistinct()` |
| Count distinct (fast approx) | `approx_count_distinct()` (HLL) |
| Percentile | `percentile_approx(col, 0.5)` |
| Collect list of values | `collect_list()` or `collect_set()` (distinct) |
| Conditional count | `count(when(cond, 1))` |
| Conditional sum | `sum(when(cond, val).otherwise(0))` |
| Rank/row number | `row_number().over(w)`, `rank`, `dense_rank` |
| Top-N per group | `row_number ≤ N` filter |
| Previous row | `lag("col").over(w)` |
| Running total | `sum(col).over(w with UNBOUNDED PRECEDING)` |
| Rolling N-day | `sum(col).over(w with rangeBetween or rowsBetween)` |
| Latest per key | `row_number desc, filter rn=1` |
| Session detection | gap detection + running SUM of new-session flag |
| Pivot | `df.groupBy(...).pivot(col, [values]).agg(...)` |
| ROLLUP / CUBE | hierarchical / all-combinations totals |
| GROUPING SETS | explicit control via SQL |

---

## 6. Partitioning


### Three Kinds of "Partitioning"

| Term | Where | What it is |
|------|-------|------------|
| **Spark partitions** | In-memory | Logical chunks that executors process in parallel |
| **File / Hive partitions** | On disk | Directory structure (`col=value/`) for pruning |
| **Bucket partitions** | On disk | Hash-based files for shuffle-free joins |

These are different things that share a word. Keep them straight.

---

### Spark Partitions (in-memory)

The unit of parallel work. Each partition is processed by exactly one task on exactly one executor at a time.

### Current partition count

```python
df.rdd.getNumPartitions()        # how many partitions
df.rdd.glom().map(len).collect() # rows per partition (small data only)
```

### Default partition count

- **File read:** depends on file size + `spark.sql.files.maxPartitionBytes` (default 128 MB)
- **Shuffle output:** `spark.sql.shuffle.partitions` (default 200)
- **parallelize:** `spark.default.parallelism`

### Rule of thumb

- 1-4 partitions per CPU core across your cluster
- Target 100-200 MB of data per partition after any filter
- Executor cores × 2-4 = partition count for CPU-bound work

Example: 10 executors × 4 cores = 40 total cores → 80-200 partitions is typical

### repartition vs coalesce

| | repartition | coalesce |
|-|-------------|----------|
| What | Full shuffle | Combine partitions on same executor |
| Direction | Increase or decrease | **Decrease only** (increase does nothing) |
| Cost | Expensive (network) | Cheap (local) |
| Result | Evenly distributed | Possibly uneven |
| Use | Rebalance / parallelism | Reduce file count before write |

```python
df.repartition(100)                  # shuffle to 100 partitions
df.repartition(100, "country")       # hash-shuffle on country, 100 partitions
df.repartition("country")            # hash-shuffle on country, default partition count
df.repartition(100, F.col("x") % 100)# shuffle by expression

df.coalesce(10)                      # reduce to 10 partitions WITHOUT shuffle
```

**Common pattern — reduce file count before writing:**

```python
df.coalesce(20).write.parquet(...)   # 20 files out
```

**Trap:** `coalesce(1)` collapses to one partition → one executor must hold everything. OOM risk.

---

### Partitioning Strategies

### Before expensive wide operations

```python
# Sort large dataset: pre-partition to balance load
(df.repartition(500, "country")        # hash-partition on sort key
   .sortWithinPartitions("country", "date")   # cheap: no shuffle
   .write.parquet(...))
```

### Before groupBy on skewed key

AQE handles this at join time; for pure aggregation, pre-partitioning can help:

```python
# Not usually needed; AQE coalesces afterwards.
# If you have hot keys, isolate them.
```

### After reading many small files

```python
df = spark.read.parquet("s3://bucket/many_small_files/")
# → Potentially thousands of partitions
df = df.coalesce(100)   # reduce overhead
```

### Dynamic partition overwrite (write-time partitioning)

Default `.mode("overwrite")` deletes ALL target data (even partitions not in the current batch).

```python
# Classic mode (STATIC): nukes everything
df.write.mode("overwrite").partitionBy("date").parquet("s3://bucket/table/")

# Dynamic: only overwrite partitions present in df
spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
df.write.mode("overwrite").partitionBy("date").parquet("s3://bucket/table/")
```

This is the standard pattern for backfilling specific partitions in Hive-style tables.

---

### File / Hive Partitioning

Directory structure: `s3://bucket/table/col1=val1/col2=val2/file.parquet`

### Write partitioned

```python
df.write.partitionBy("country", "date").parquet("s3://bucket/table/")
# → s3://bucket/table/country=US/date=2024-10-19/part-0001.parquet
```

### Read with partition pruning

Spark auto-prunes based on WHERE on partition columns:

```python
df = spark.read.parquet("s3://bucket/table/")
df.filter(col("country") == "US").filter(col("date") >= "2024-10-01")
# → Only reads matching directories
```

Verify in `explain()` → `PartitionFilters: [isnotnull(country), (country = US), (date >= 2024-10-01)]`.

### Partition column selection

- Partition on **high-selectivity, often-filtered columns**: date, country, tenant_id
- **Cardinality:** keep file count manageable (ideally 100-10,000 total partition directories)
- **Avoid:** partitioning on high-cardinality columns like `user_id` (millions of tiny directories)
- **Hierarchy:** put higher-selectivity column first (date BEFORE country if date is more selective)

### Over-partitioning

Too many partition dirs → too many tiny files → metadata overhead + slow reads + listing cost.

Rule: aim for ≥ 128 MB per file within each partition.

---

### Bucketing

Pre-hash-partitions data by column at write time. Future joins/aggregations on the bucket column skip shuffle.

```python
(df.write
   .bucketBy(100, "user_id")
   .sortBy("user_id")                 # optional: sort within bucket
   .saveAsTable("db.my_table"))
```

### How it works

- Writes 100 files, each containing rows where `hash(user_id) % 100` is a specific value
- When reading + joining on `user_id` with another table bucketed the same way → NO SHUFFLE

### Requirements

- Must use `saveAsTable()` (not `.parquet()` path)
- Both sides of join must have the SAME number of buckets
- Both must bucket on the SAME column
- Managed by catalog (Hive, Glue, Unity)
- Spark pre-3.0 bucket discovery was limited; 3.x much better

### Bucket pruning (reading subset)

```python
df.filter(col("user_id") == "abc123")   # may read only bucket containing hash("abc123")
```

### Bucketing in Iceberg

Iceberg has HIDDEN bucket partitioning:
```sql
CREATE TABLE t (user_id BIGINT, ...) 
    PARTITIONED BY (bucket(100, user_id));
```

Preferred over Spark native bucketing in 2026 — plays better with multiple engines.

---

### Partitioning for Lakehouse Formats

### Delta Lake

```python
df.write.format("delta").partitionBy("date").save(...)
```

- Supports dynamic partition overwrite via `replaceWhere`:
  ```python
  (df.write.format("delta")
     .mode("overwrite")
     .option("replaceWhere", "date >= '2024-10-01' AND date < '2024-10-15'")
     .save("s3://bucket/delta/"))
  ```
- Supports Z-ORDER (Databricks): clustering within partitions for data skipping
- Supports Liquid Clustering (newer): no rigid partition structure

### Iceberg

```python
# Via SQL (preferred)
spark.sql("""
    CREATE TABLE catalog.db.table (
        id BIGINT, user_id STRING, event_time TIMESTAMP, amount DOUBLE
    ) USING iceberg
    PARTITIONED BY (days(event_time), bucket(50, user_id))
""")
```

Iceberg supports:
- Hidden partitioning (don't need to compute partition col in query)
- Partition evolution (change partition spec without rewriting data)
- Bucket + truncate + days/hours/months transforms

### Hudi

```python
hudi_options = {
    "hoodie.datasource.write.partitionpath.field": "country",
    "hoodie.datasource.write.recordkey.field": "id",
    ...
}
df.write.format("hudi").options(**hudi_options).mode("append").save(...)
```

---

### Partition Pruning in Depth

### What qualifies for pruning

- **Equality:** `WHERE col = 'X'`
- **IN list:** `WHERE col IN ('A', 'B', 'C')`
- **Range:** `WHERE col >= 'X' AND col < 'Y'`
- **Function on partition col (sometimes):** `WHERE YEAR(ts) = 2024` — may or may not prune

### When pruning breaks

```python
# BAD: function on partition col — may not prune
df.filter(F.year("date") == 2024)   

# GOOD: literal partition value
df.filter(col("date") >= "2024-01-01").filter(col("date") < "2025-01-01")
```

Partition column type must MATCH the literal:
```python
# Partition dirs: date=2024-10-19 (string in path)
df.filter(col("date") == 20241019)   # MAY FAIL to prune (type mismatch)
df.filter(col("date") == "2024-10-19")  # prunes
```

### Dynamic Partition Pruning (DPP, 3.0+)

Join filter dynamically prunes partitions on the fact table:

```python
# dim is small; fact is partitioned by date
fact.join(dim.filter(col("region") == "US"), "date_key")
# → Spark infers date_key values from dim.filter result and prunes fact's partitions
```

Enabled via `spark.sql.optimizer.dynamicPartitionPruning.enabled` (default true).

---

### Repartitioning Deep Patterns

### Repartition for skew

```python
# Skewed shuffle: use salt
df.withColumn("salt", (F.rand() * 100).cast("int"))
  .repartition(1000, "country", "salt")
  .drop("salt")

# But AQE skew split often handles this
```

### Repartition by range (for sorted output)

```python
# Default repartition hash-partitions
df.repartition(100, "date")

# Range partitioning: sort-friendly; evenly spread
df.repartitionByRange(100, "date")
```

Range partitioning samples the data to pick split points. Output is approximately sorted.

### mapPartitions / foreachPartition

```python
# RDD-level: process one partition at a time
def process(iterator):
    conn = open_connection()
    try:
        for row in iterator:
            yield transform(row)
    finally:
        conn.close()

df.rdd.mapPartitions(process)

# Or foreach for side effects:
df.foreachPartition(process)
```

Use when you need per-partition setup (DB connection, file handle, etc.).

---

### Partition Diagnostics

### Row count per partition

```python
# Small data OK
df.rdd.glom().map(len).collect()

# Big data: use sampling or Spark UI stage detail
```

### Partition size (bytes) estimate

```python
# Spark UI → Stage detail → task breakdown (shuffle read/write per task)
# Or:
df.groupBy(F.spark_partition_id()).count().show()   # row count per partition
```

### Verify pruning worked

```python
df.filter(...).explain(mode="formatted")
# Look for:
#   PartitionFilters: [...]  ← present + non-empty means pruning
#   number of files read in Spark UI → stage detail
```

---

### Common Partitioning Mistakes

### 1. coalesce(1) on big data

```python
# BAD: all data → one executor → OOM or very slow
df.coalesce(1).write.parquet(...)

# GOOD: use repartition if you need shuffle; or accept more files
df.repartition(10).write.parquet(...)
```

### 2. Many small files

Writing `df` without controlling partitions: if the DataFrame has 2000 partitions and you write without coalesce, you get 2000 files. Later reads deal with metadata explosion.

```python
# Target ~128 MB files
df.repartition(num_output_files).write.parquet(...)
# or
df.coalesce(num_output_files).write.parquet(...)
```

### 3. Partitioning by high-cardinality column

```python
# BAD: millions of tiny dirs
df.write.partitionBy("user_id").parquet(...)

# GOOD: partition by date + bucket by user_id (Iceberg)
# OR: just partition by date
```

### 4. Over-partitioning with multiple columns

```python
# BAD: 365 × 1000 × 1000 = 365 million partition dirs
df.write.partitionBy("date", "country", "product").parquet(...)
```

Rule: total partition count across the table should be ≤ 100K (ideally ≤ 10K).

### 5. Not using dynamic partition overwrite

```python
# This NUKES all data in the table even if df only has one day
df.write.mode("overwrite").partitionBy("date").parquet("s3://bucket/table/")

# Fix: dynamic mode (safer default for partitioned tables)
spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
```

### 6. Partition column type mismatch

See "When pruning breaks" above.

### 7. Expecting coalesce to increase partitions

```python
# df has 10 partitions
df.coalesce(100)   # STILL 10 partitions (coalesce doesn't increase)

# To increase: use repartition
df.repartition(100)
```

---

### Interview Patterns

> **🎯 Interview Angle — "Repartition vs coalesce."** Repartition is a full shuffle: redistributes data across N partitions. Coalesce combines existing partitions on the same executor (narrow transformation) and can ONLY decrease. For file-count reduction before writing, use coalesce; for rebalancing or skew mitigation, use repartition. coalesce(1) is dangerous on big data (one executor must hold all data).

> **🎯 Interview Angle — "How do you choose partition columns?"** Three criteria: (1) frequently used in WHERE filters (so pruning helps), (2) low-to-medium cardinality (so partition count stays manageable, ~ 100-10K), (3) good distribution (not 99% one value). Date is almost always a good choice. User_id is usually bad (too high cardinality). For dim-like columns + user_id-like columns, prefer Iceberg's bucket + days() combination.

> **🔥 Production Scenario — The Small Files Avalanche.** A streaming job appended to a Parquet table with 2000 partitions on write; produced 2000 small files per batch × 24 hours × 30 days = 1.4M files. Query time: 20 min. Added coalesce(50) before writing: 50 files per batch. Also added weekly compaction: `df.repartition(100, "date").write.mode("overwrite")...`. Query time: 2 min. Lesson: small-files management is non-negotiable for append pipelines.

> **🔥 Production Scenario — The Partition Pruning Bust.** Partitioned a table by `date` as string `'2024-10-19'`. Downstream used `WHERE date = CAST('2024-10-19' AS DATE)`. Pruning silently failed (type mismatch); scan read 2 years of data. Fixed by keeping `date` as string throughout. Lesson: partition column types must match the queries; verify in `explain()`.

---

### Pattern Summary

| Need | Technique |
|------|-----------|
| Reduce partitions (no shuffle) | `coalesce(N)` |
| Rebalance / increase | `repartition(N)` or `repartition(N, "col")` |
| Partitioned write | `.partitionBy("col")` |
| Dynamic partition overwrite | `spark.sql.sources.partitionOverwriteMode = dynamic` |
| Bucket + join shuffle-free | `.bucketBy(N, "col").saveAsTable(...)` |
| Partition pruning | Hive-style path + filter on col; type-match |
| Avoid tiny files | `coalesce()` or `repartition()` before write |
| Avoid partition explosion | Limit cardinality; prefer bucket + date |

---

## 7. UDFs & Pandas UDFs


### When to use UDFs

**AVOID UDFs when a built-in function exists.** Built-ins run in Catalyst/Tungsten — vectorized, codegen, pushdown-capable. UDFs:
- Serialize data to Python (expensive)
- Can't be optimized by Catalyst (black box)
- Can't push down filters

**Use UDFs ONLY when:**
- Native functions don't support the logic
- Calling external libraries (NLP, ML inference, image processing)
- Business logic too complex for SQL

Even then, prefer **Pandas UDFs** (vectorized) over scalar Python UDFs.

---

### Scalar Python UDFs (slowest; avoid for perf)

```python
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType, IntegerType

@udf(returnType=StringType())
def upper_py(s):
    if s is None:
        return None
    return s.upper()

df.withColumn("upper_name", upper_py("name"))
```

### How it executes

For each row:
1. Serialize Python object → send to Python interpreter on executor
2. Python function runs
3. Serialize result → send back to JVM

~10-100x slower than a native equivalent. Native: `F.upper("name")`.

### Register for SQL

```python
spark.udf.register("upper_py", upper_py)
spark.sql("SELECT upper_py(name) FROM my_table")
```

### Null handling

```python
# Python UDFs receive None for NULL; return None for NULL output
@udf(returnType=IntegerType())
def safe_len(s):
    return len(s) if s is not None else None
```

### DeterministicUDF vs non-deterministic

```python
# Mark non-deterministic to prevent Catalyst from caching/duplicating calls
my_udf = udf(my_fn, returnType=StringType()).asNondeterministic()
```

Non-deterministic UDFs force Spark to compute them in-place (can't be re-ordered or cached).

---

### Pandas UDFs (vectorized — PREFER)

Pandas UDFs operate on **batches** (Arrow-backed pandas Series), not row-at-a-time. 10-100x faster than scalar UDFs.

### Requirements

```bash
pip install pyspark[pandas]
# or
pip install pyarrow pandas
```

Spark 3.0+ uses Pandas UDF type hints (`@pandas_udf` with Python type annotations).

### Scalar Pandas UDF: Series → Series

```python
from pyspark.sql.functions import pandas_udf
from pyspark.sql.types import StringType
import pandas as pd

@pandas_udf(StringType())
def upper_pandas(s: pd.Series) -> pd.Series:
    return s.str.upper()

df.withColumn("upper_name", upper_pandas("name"))
```

### Scalar Iterator Pandas UDF (one-shot state per executor)

```python
from typing import Iterator

@pandas_udf(StringType())
def enriched(iter_s: Iterator[pd.Series]) -> Iterator[pd.Series]:
    # Setup once per executor task
    model = load_model()  
    for s in iter_s:
        yield s.apply(lambda x: model.predict(x))

df.withColumn("prediction", enriched("features"))
```

**Use this** when the UDF needs expensive per-task setup (model loading, DB connection).

### Multi-input Pandas UDF

```python
@pandas_udf("double")
def amount_in_usd(amount: pd.Series, rate: pd.Series) -> pd.Series:
    return amount * rate

df.withColumn("usd_amount", amount_in_usd("amount_local", "fx_rate"))
```

### Grouped Map Pandas UDF (applyInPandas, 3.0+)

```python
# For: df.groupBy(...).applyInPandas(fn, schema)
# fn takes pd.DataFrame of ONE GROUP, returns pd.DataFrame

def normalize_group(pdf: pd.DataFrame) -> pd.DataFrame:
    pdf["normalized"] = (pdf["value"] - pdf["value"].mean()) / pdf["value"].std()
    return pdf

result = df.groupBy("country").applyInPandas(
    normalize_group,
    schema="country string, user_id string, value double, normalized double"
)
```

**Use this** for per-group operations that are hard in SQL (regression per group, custom transformations).

### Grouped Agg Pandas UDF (one row per group)

```python
@pandas_udf("double")
def weighted_mean(values: pd.Series, weights: pd.Series) -> float:
    return (values * weights).sum() / weights.sum()

df.groupBy("country").agg(weighted_mean("value", "weight"))
```

### Map Pandas UDF (mapInPandas, 3.0+)

```python
# Transform batches of rows; schema stays the same or changes

def transform_batch(iter_pdf: Iterator[pd.DataFrame]) -> Iterator[pd.DataFrame]:
    for pdf in iter_pdf:
        pdf["new_col"] = pdf["col"].str.lower()
        yield pdf

result = df.mapInPandas(transform_batch, schema=new_schema)
```

**Use this** when you want to process batches but not group semantics (just row-wise but vectorized).

### Cogrouped Map (two DataFrames)

```python
def merge_fn(pdf_a: pd.DataFrame, pdf_b: pd.DataFrame) -> pd.DataFrame:
    return pd.merge_asof(pdf_a, pdf_b, on="time")

result = (dfA.groupBy("user_id")
             .cogroup(dfB.groupBy("user_id"))
             .applyInPandas(merge_fn, schema="..."))
```

---

### Arrow Optimization

Spark uses Apache Arrow for Pandas UDF data transfer: zero-copy columnar format between JVM and Python. Massive speedup over row-serialization.

Enable (default in 3.1+ but confirm):
```python
spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "true")
spark.conf.set("spark.sql.execution.arrow.pyspark.fallback.enabled", "true")   # fallback to non-Arrow on error
spark.conf.set("spark.sql.execution.arrow.maxRecordsPerBatch", "10000")        # batch size
```

Arrow accelerates:
- Pandas UDFs
- `toPandas()` and `createDataFrame(pdf)`
- Grouped-map ops

---

### Pandas UDFs vs mapInPandas vs applyInPandas

| Use case | Technique |
|----------|-----------|
| Row-wise vectorized (col in → col out) | Scalar Pandas UDF |
| Per-group logic (ML, pivoting) | `applyInPandas` on `groupBy` |
| Batch-wise transformation (same schema or new schema) | `mapInPandas` |
| Aggregate per group → scalar | Grouped Agg Pandas UDF |
| Multi-DataFrame per group | `cogroup().applyInPandas` |
| Expensive per-task setup (model load) | Scalar Iterator Pandas UDF |

---

### Pandas API on Spark (koalas merged)

```python
import pyspark.pandas as ps

psdf = ps.read_parquet("s3://bucket/...")

# Familiar pandas syntax, Spark execution
psdf["x"] = psdf["x"] * 2
grouped = psdf.groupby("country")["sales"].sum()
result = psdf.sort_values("date").head(100)
```

Interchangeable with pandas most of the time. Use for migrating pandas code to Spark with minimal rewrite.

**Caveat:** some pandas operations don't have efficient Spark equivalents; verify perf.

---

### Common UDF Patterns

### HTTP call per row (anti-pattern — rate limit, slow)

```python
# BAD: naive scalar UDF
@udf("string")
def call_api(x):
    return requests.get(f"api/{x}").json()["result"]
# → sequential; may rate-limit the API

# BETTER: Pandas UDF with batched requests
import httpx

@pandas_udf("string")
def call_api(s: pd.Series) -> pd.Series:
    # Batch call to API
    results = httpx.post("api/batch", json={"ids": s.tolist()}).json()
    return pd.Series(results)
```

### ML inference

```python
from pyspark.sql.functions import pandas_udf

broadcast_model = spark.sparkContext.broadcast(load_model("model.pkl"))

@pandas_udf("double")
def predict(features: pd.Series) -> pd.Series:
    model = broadcast_model.value
    return pd.Series(model.predict(np.vstack(features)))

df.withColumn("score", predict("features"))
```

Pattern: broadcast model to all executors; use in Pandas UDF.

### Image / NLP processing

Use Daft (for images, see sibling notes) or Spark + mapInPandas with batched inference. For large models + GPUs, Spark on K8s with GPU scheduling.

### Regex that's hard in Spark's regex

```python
import re

@pandas_udf("string")
def extract_pandas(s: pd.Series) -> pd.Series:
    return s.apply(lambda x: (re.search(r"complex_pattern", x) or [""])[0])
```

Prefer Spark's `F.regexp_extract` first; drop to Pandas UDF only when regex is beyond Spark's dialect.

---

### UDF Performance Tips

### 1. Use Pandas UDFs over scalar

Vectorization + Arrow = 10-100x speedup.

### 2. Batch external calls

Don't call APIs row-by-row. Group into batches, call once per batch.

### 3. Broadcast large read-only data

```python
broadcast_dict = spark.sparkContext.broadcast(my_big_dict)

@pandas_udf("string")
def lookup(key_series: pd.Series) -> pd.Series:
    d = broadcast_dict.value
    return key_series.map(d)
```

### 4. Set batch size appropriately

```python
spark.conf.set("spark.sql.execution.arrow.maxRecordsPerBatch", "10000")
```

Larger batches = better amortization of per-batch overhead, but higher memory.

### 5. Avoid chained UDFs

Each UDF = Python round-trip. Combine into one UDF if possible.

### 6. Prefer built-in functions

Before writing a UDF, grep for alternatives in `pyspark.sql.functions`. Often there's a native equivalent you missed.

### 7. Native type functions

```python
# These push into Catalyst — use them even if clumsier
F.transform(col("arr"), lambda x: F.when(x > 0, x * 2).otherwise(x))   # arr map
F.filter(col("arr"), lambda x: x > 0)                                  # arr filter
F.aggregate(col("arr"), F.lit(0), lambda acc, x: acc + x)             # arr sum
F.exists(col("arr"), lambda x: x > 100)
```

Higher-order functions (3.0+) replace many array UDF needs.

---

### UDF Debugging

### Debug a UDF locally

```python
# Create a small pandas df, run the Pandas UDF function directly
import pandas as pd
test_series = pd.Series([1.0, 2.0, 3.0])
my_pandas_udf.func(test_series)   # calls the raw function; no Spark
```

### UDF errors

Errors in UDFs appear as Py4J exceptions at the DRIVER. The actual Python traceback is in executor logs (yarn logs, k8s logs, or Spark UI → stdout/stderr).

```python
# Catch and surface issues
@pandas_udf("string")
def safe_fn(s: pd.Series) -> pd.Series:
    try:
        return s.apply(risky_op)
    except Exception as e:
        logger.exception(e)
        return pd.Series([None] * len(s))
```

---

### PyArrow Caveats

```python
# toPandas() with Arrow:
spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "true")
pdf = df.toPandas()
```

Arrow has type coverage edges — some types (map, nested struct) have had issues historically. Modern PySpark (3.3+) handles most cleanly.

If Arrow fails, Spark falls back to non-Arrow transfer (if `arrow.pyspark.fallback.enabled=true`). Verify in logs.

---

### Interview Patterns

> **🎯 Interview Angle — "Why avoid Python UDFs?"** Scalar Python UDFs serialize each row to Python and back → 10-100x slower than native. They also prevent Catalyst optimization (black box; no filter pushdown, no codegen). Modern Spark gives three alternatives: (1) native functions in `pyspark.sql.functions` (fastest), (2) higher-order functions for arrays (3.0+), (3) Pandas UDFs with Arrow (vectorized, 10-100x faster than scalar UDFs). Only drop to scalar Python UDF when no alternative works.

> **🎯 Interview Angle — "Explain the Pandas UDF types."** Four main types: (1) **Scalar** (series → series) — vectorized row-wise. (2) **Scalar Iterator** (iterator of series → iterator) — same but with per-task setup. (3) **Grouped Agg** (series → scalar) — one output per group. (4) **Grouped Map / applyInPandas** (DataFrame → DataFrame per group) — per-group complex logic. Plus `mapInPandas` for batch-wise non-grouped transforms and `cogroup().applyInPandas` for two-DataFrame-per-group ops.

> **🔥 Production Scenario — The 10x Speedup.** A Spark pipeline enriched 1B rows with a lookup via Python UDF. Runtime: 4 hours. Rewrote as Pandas UDF with broadcast dict for the lookup: 25 minutes. Lesson: Pandas UDFs with broadcast are often enough; no need to rewrite as join if the lookup is small.

> **🔥 Production Scenario — ML Inference Batching.** Image classification UDF: 1 request/row to a model service. Rate-limited, 2 req/sec, 10M rows = ~ 55 days. Switched to Pandas UDF with 1000-row batches sent to model service's /batch endpoint: 10M rows in 20 minutes. Lesson: external APIs should be batched; Pandas UDFs make batching natural.

---

### Pattern Summary

| Need | Technique |
|------|-----------|
| Row-wise transform | Scalar Pandas UDF: `pd.Series → pd.Series` |
| Per-group logic | `groupBy(...).applyInPandas(fn, schema)` |
| Per-group scalar agg | Grouped Agg Pandas UDF |
| Batch-wise transform | `df.mapInPandas(fn, schema)` |
| Expensive per-task setup | Scalar Iterator Pandas UDF |
| Two-DataFrame per group | `df1.groupBy(k).cogroup(df2.groupBy(k)).applyInPandas(fn, s)` |
| Simple Python | Scalar `@udf` — slow but easy |
| Avoid UDF entirely | `F.transform`, `F.filter`, `F.aggregate`, `F.exists`, native fns |
| Model inference | Broadcast model + Pandas UDF |
| External API | Pandas UDF with batched call |

---

## 8. Structured Streaming


### The Mental Model

Structured Streaming treats a stream as an **unbounded table** that keeps growing. Your query is defined once; Spark incrementally updates results as new rows arrive.

```
    SOURCE (Kafka/files/socket)
         │   new rows arrive
         ▼
    UNBOUNDED INPUT TABLE 
         │  (grows forever conceptually)
         ▼
    QUERY (same DataFrame API)
         ▼
    RESULT TABLE
         │
         ▼
    SINK (Kafka / Parquet / Delta / console / foreach)
```

Same DataFrame API as batch. Spark handles incremental execution + state + checkpointing.

---

### Reading a Stream

### Kafka

```python
stream = (spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "broker1:9092,broker2:9092")
    .option("subscribe", "events-topic")                 # or subscribePattern="prefix.*"
    .option("startingOffsets", "latest")                 # or "earliest", or {"topic":{"0":100}}
    .option("maxOffsetsPerTrigger", "100000")            # rate limit
    .option("failOnDataLoss", "false")                   # permissive mode
    .load())

# Deserialize
from pyspark.sql.functions import col
from pyspark.sql.types import StructType, StructField, StringType

schema = StructType([StructField("user_id", StringType()), StructField("action", StringType())])

events = (stream.selectExpr("CAST(key AS STRING) AS key", "CAST(value AS STRING) AS value_str", "timestamp")
    .withColumn("value", F.from_json("value_str", schema))
    .select("key", "value.*", "timestamp"))
```

### Files (auto-detect new files in a dir)

```python
stream = (spark.readStream
    .format("parquet")                           # or json, csv, orc
    .schema(schema)                              # REQUIRED for streaming (no inference)
    .option("maxFilesPerTrigger", 100)
    .option("latestFirst", "false")
    .option("fileNameOnly", "false")             # consider FULL path for dedup
    .load("s3://bucket/incoming/"))

# For Delta — reads append-only Delta tables
stream = (spark.readStream
    .format("delta")
    .option("maxFilesPerTrigger", 100)
    .load("s3://bucket/delta/"))

# For Delta with CDC (readChangeFeed)
stream = (spark.readStream
    .format("delta")
    .option("readChangeFeed", "true")
    .option("startingVersion", 100)
    .load("s3://bucket/delta/"))
```

### Rate source (for testing)

```python
stream = (spark.readStream.format("rate")
    .option("rowsPerSecond", 10)
    .option("rampUpTime", "5s")
    .load())
# → DataFrame with timestamp + value columns
```

### Databricks Auto Loader (file ingestion with notifications)

```python
stream = (spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "json")
    .option("cloudFiles.schemaLocation", "s3://bucket/schemas/")
    .option("cloudFiles.schemaEvolutionMode", "addNewColumns")   # or "failOnNewColumns", "rescue"
    .option("cloudFiles.useNotifications", "true")               # SNS/SQS for event-driven
    .load("s3://bucket/incoming/"))
```

Auto Loader is DATABRICKS-specific. Open-source Spark uses the plain file source.

---

### Writing a Stream (Sink)

### File sinks (Parquet, Delta)

```python
query = (events.writeStream
    .format("delta")                             # or parquet
    .outputMode("append")                        # append / complete / update
    .option("checkpointLocation", "s3://bucket/checkpoints/events/")
    .option("path", "s3://bucket/delta/events/")
    .trigger(processingTime="30 seconds")
    .start())

query.awaitTermination()
```

### Kafka sink

```python
query = (results.selectExpr("CAST(key AS STRING) AS key", 
                             "to_json(struct(*)) AS value")
    .writeStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "broker:9092")
    .option("topic", "output-topic")
    .option("checkpointLocation", "s3://bucket/ck/")
    .start())
```

### Console (dev only)

```python
query = (df.writeStream.format("console").outputMode("append").start())
```

### foreachBatch (write anywhere)

```python
def write_to_cassandra(batch_df, batch_id):
    (batch_df.write
        .format("org.apache.spark.sql.cassandra")
        .options(table="my_table", keyspace="my_ks")
        .mode("append")
        .save())

query = (df.writeStream
    .foreachBatch(write_to_cassandra)
    .outputMode("update")
    .option("checkpointLocation", "s3://bucket/ck/")
    .start())
```

**`foreachBatch` is the escape hatch** — write to any system by using the batch DataFrame API inside the callback.

### foreach (row-by-row)

```python
class RowWriter:
    def open(self, partition_id, epoch_id): return True
    def process(self, row): send_to_api(row)
    def close(self, error): pass

query = df.writeStream.foreach(RowWriter()).start()
```

---

### Output Modes

| Mode | What it emits | When applicable |
|------|---------------|-----------------|
| **append** | Only NEW rows | No aggregations, OR aggregations with watermark |
| **update** | Only CHANGED aggregation rows | Aggregations |
| **complete** | ENTIRE result table | Aggregations (expensive if result is big) |

### Append mode details

- Default mode when there's no aggregation
- With aggregations: only outputs when a window closes (needs watermark)
- No late re-emission of old rows

### Update mode details

- With aggregations: outputs rows that changed since last trigger
- Doesn't need watermark but state grows forever

### Complete mode details

- Entire result emitted every trigger
- Only usable with aggregations
- Doesn't need watermark but may be large

---

### Triggers

```python
# Micro-batch, every N seconds (default behavior)
.trigger(processingTime="10 seconds")

# As fast as possible (default when nothing specified)
.trigger(processingTime="0 seconds")

# One-time micro-batch (process all available data then stop)
.trigger(once=True)                   # deprecated in favor of availableNow

# Available now: process all backlog in multiple batches, then stop
.trigger(availableNow=True)           # 3.3+

# Continuous (experimental, low-latency; limited op support)
.trigger(continuous="1 second")
```

**Available-now pattern:** for "run every hour via Airflow but process ALL data in Kafka backlog." Combines batch-schedule with streaming semantics.

---

### Watermarks (for stateful aggregations)

A watermark tells Spark: "any data more than N minutes late can be discarded."

```python
events_with_watermark = events.withWatermark("event_time", "10 minutes")

aggregated = (events_with_watermark
    .groupBy(F.window("event_time", "5 minutes"), "user_id")
    .agg(F.count("*").alias("events")))
```

**Why watermarks matter:**
- Without them, aggregation state grows forever (keeps every window open)
- With them, state for windows older than `max_event_time - watermark` is dropped
- Late data within watermark: still included in the window update
- Late data beyond watermark: dropped

### Choosing watermark

- Short (seconds-minutes): low state size, drops late data
- Long (hours): higher state, catches more late data
- Match to your SLA for "complete-ness" of the window

### Window types

```python
# TUMBLING: non-overlapping fixed windows
F.window("event_time", "5 minutes")

# SLIDING: overlapping fixed windows (slide every N, size M)
F.window("event_time", "5 minutes", "1 minute")   # 5-min window slides every 1-min

# SESSION windows (3.2+): group events by inactivity gap
F.session_window("event_time", "10 minutes")      # 10-min inactivity ends session
```

### Example: event-time windowed count

```python
counts = (events
    .withWatermark("event_time", "10 minutes")
    .groupBy(F.window("event_time", "1 minute"), "user_id")
    .agg(F.count("*").alias("event_count"))
    .select(col("window.start").alias("window_start"), "user_id", "event_count"))

(counts.writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", "s3://bucket/ck/")
    .start("s3://bucket/output/"))
```

With `append` mode: results emit only when a window is "complete" (watermark passes window end).

---

### Streaming Joins

### Stream-static join

```python
# df_stream is streaming; df_static is batch
result = stream.join(static_df, "key", "left_outer")
# → Static side is re-read each trigger (or cached if small)
```

### Stream-stream join (with watermarks + time constraint)

```python
events = events.withWatermark("event_time", "10 minutes")
clicks = clicks.withWatermark("click_time", "10 minutes")

result = events.join(
    clicks,
    F.expr("""
        events.user_id = clicks.user_id
        AND clicks.click_time BETWEEN events.event_time AND events.event_time + INTERVAL 1 HOUR
    """),
    "inner"
)
```

Required for stream-stream joins:
- Watermarks on BOTH sides
- Time range condition relating the two time columns

### Outer stream-stream join

```python
# Left outer: left side emitted after watermark passes
result = events.join(clicks, join_condition, "left_outer")
```

Outer join delays emission of unmatched left-side rows until watermark expires on the right side.

---

### Stateful Operations

Any operation that remembers past rows uses STATE:
- Aggregations
- Joins
- Deduplication
- flatMapGroupsWithState / mapGroupsWithState

### State store

- Default: HDFS-backed (local disk + replication via checkpoint)
- RocksDB (3.2+): embedded database for state — handles larger state better
  ```python
  spark.conf.set("spark.sql.streaming.stateStore.providerClass",
                 "org.apache.spark.sql.execution.streaming.state.RocksDBStateStoreProvider")
  ```

### State monitoring

Spark UI → Structured Streaming → Query → state metrics (num state keys, bytes).

### flatMapGroupsWithState (custom stateful logic)

```python
# NOT PYTHON-NATIVE PRE-3.4; use Scala or 3.4+'s applyInPandasWithState

# Spark 3.4+ example (Python):
def update_state(key, iterator, state):
    curr = state.get() or {"total": 0, "count": 0}
    for row in iterator:
        curr["total"] += row["amount"]
        curr["count"] += 1
    state.update(curr)
    yield (key, curr["total"], curr["count"])

result = (df.groupBy("user_id")
            .applyInPandasWithState(
                update_state,
                outputStructType="user_id string, total double, count long",
                stateStructType="total double, count long",
                outputMode="update",
                timeoutConf="NoTimeout"))
```

For custom aggregation semantics (sessionization with custom logic, event-driven state machines).

---

### Deduplication

```python
events.withWatermark("event_time", "1 hour") \
    .dropDuplicates(["event_id", "event_time"])
```

**Note:** without watermark, dedup state grows forever.

---

### Exactly-Once Semantics

Structured Streaming provides **exactly-once end-to-end** when:
- Source supports replay (Kafka, Delta, files)
- Sink is **idempotent** or **transactional**
- Checkpoint location is reliable (S3/HDFS)

### Idempotent sinks

- Files with exactly-once write (part files committed atomically)
- Delta Lake (transactional)
- Kafka with `transactional-id`
- Databases via `foreachBatch` + MERGE

### At-least-once sinks

- Plain HTTP calls in `foreach` without idempotency
- UDPs
- Non-transactional database writes

### Checkpoint

- Stores offsets, state, metadata
- Must be reliable storage (not local disk unless local-only test)
- **Different stream = different checkpoint path**
- **Code changes** that break backwards compat may require new checkpoint

---

### Common Streaming Patterns

### Incremental ETL (Bronze → Silver via streaming)

```python
# Bronze: raw ingestion from Kafka
bronze = (spark.readStream.format("kafka")...load())

# Silver: parse + clean
silver = (bronze
    .select(F.from_json("value", schema).alias("data"))
    .select("data.*")
    .filter(col("event_type").isNotNull()))

(silver.writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", "s3://ck/silver/")
    .start("s3://silver/events/"))
```

### CDC apply (Bronze → Silver)

```python
from delta.tables import DeltaTable

bronze_stream = (spark.readStream.format("kafka")...load())
parsed = bronze_stream.select(F.from_json("value", cdc_schema).alias("data")).select("data.*")

def apply_cdc(batch_df, batch_id):
    target = DeltaTable.forPath(spark, "s3://silver/users/")
    (target.alias("t").merge(
        batch_df.alias("s"),
        "t.id = s.id")
        .whenMatchedUpdateAll(condition="s.op != 'DELETE'")
        .whenMatchedDelete(condition="s.op = 'DELETE'")
        .whenNotMatchedInsertAll(condition="s.op != 'DELETE'")
        .execute())

(parsed.writeStream
    .foreachBatch(apply_cdc)
    .outputMode("update")
    .option("checkpointLocation", "s3://ck/users_cdc/")
    .start())
```

Classic pattern: use `foreachBatch` to run MERGE on each micro-batch.

### Windowed metric aggregation

```python
events = events.withWatermark("event_time", "5 minutes")
counts = (events
    .groupBy(F.window("event_time", "1 minute"), "country")
    .agg(F.count("*").alias("c"),
         F.countDistinct("user_id").alias("users"))
    .select(col("window.start").alias("ts"), "country", "c", "users"))

(counts.writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", "s3://ck/metrics/")
    .start("s3://metrics/counts/"))
```

### Fan-out to multiple sinks

```python
def multi_sink(batch_df, batch_id):
    batch_df.cache()
    batch_df.write.format("delta").mode("append").save("s3://sink1/")
    batch_df.write.format("delta").mode("append").save("s3://sink2/")
    batch_df.unpersist()

(stream.writeStream.foreachBatch(multi_sink).start())
```

---

### Streaming Monitoring

### Programmatic query status

```python
query = ...start()

query.id                   # unique query ID
query.name                 # user-set name
query.status               # current state
query.lastProgress         # metrics from last trigger
query.recentProgress       # list of recent progresses

# Stop
query.stop()

# Wait for failure or stop
query.awaitTermination()
query.awaitTermination(timeout=60)
```

### StreamingQueryListener

```python
from pyspark.sql.streaming import StreamingQueryListener

class MyListener(StreamingQueryListener):
    def onQueryStarted(self, event): print(f"Started {event.id}")
    def onQueryProgress(self, event): 
        p = event.progress
        print(f"Processed {p.numInputRows} rows at {p.inputRowsPerSecond} r/s")
    def onQueryIdle(self, event): pass
    def onQueryTerminated(self, event): print(f"Terminated {event.id}")

spark.streams.addListener(MyListener())
```

### Metrics to monitor

- `numInputRows` — throughput
- `inputRowsPerSecond` — input rate
- `processedRowsPerSecond` — processing rate
- `batchDuration` — total batch time
- `durationMs.*` — breakdowns (addBatch, getBatch, latestOffset)
- `stateOperators[].numRowsTotal` — state size

**Alert on:**
- `inputRowsPerSecond > processedRowsPerSecond` for extended period (falling behind)
- `batchDuration` exceeding trigger interval
- State size growing without bound (missing watermark or dedup gone wrong)

---

### Common Streaming Mistakes

### 1. Missing checkpoint

Every streaming query MUST specify `checkpointLocation`. Without it, no fault tolerance; restart = reprocess everything.

### 2. Local checkpoint for production

```python
.option("checkpointLocation", "/tmp/checkpoint/")   # local disk — pod restart = data loss
```

Use reliable storage (S3, GCS, ADLS, HDFS).

### 3. Changing the query without new checkpoint

Some code changes require a new checkpoint path (schema changes, operator reordering). Spark may fail or produce wrong results otherwise.

### 4. Watermark too short with append mode

Append mode emits windows AFTER watermark passes window end. If watermark is 10 min + window is 5 min, you wait 15 min for the first emission.

### 5. State growth without bound

- Aggregation without watermark → forever state
- Deduplication without watermark → forever dedup table
- Stream-stream join without watermark → infinite state

### 6. Expecting foreach to be exactly-once by itself

`foreach` with external calls is at-least-once unless you make the external operation idempotent (e.g., include batch_id + offset in the API payload).

---

### Spark Connect + Streaming (3.5+)

Spark Connect supports streaming (3.5+ with caveats). Streaming queries run server-side; clients can monitor + stop.

---

### Interview Patterns

> **🎯 Interview Angle — "Explain watermarks."** A watermark declares "max event-time delay you'll tolerate." Spark tracks `max(event_time) - watermark` and drops state for windows older than that. Without a watermark, stateful operations (windowed agg, stream-stream join, dedup) grow state forever. With one, you trade some late-data correctness for bounded state. In append mode, watermark also controls WHEN a window emits (after watermark passes the window end).

> **🎯 Interview Angle — "Compare micro-batch vs continuous."** Micro-batch (default): Spark periodically triggers a batch over new data; latency = trigger interval (seconds). Continuous (experimental): true record-at-a-time; latency ~ ms. Continuous supports FEWER operations (no aggregations, limited joins) and has had reliability issues; micro-batch is production-standard. For true <100ms latency, use Flink.

> **🎯 Interview Angle — "Exactly-once in Structured Streaming."** Spark provides exactly-once given: (1) replayable source (Kafka, Delta, files), (2) idempotent or transactional sink, (3) reliable checkpoint storage. The sink is usually where exactly-once comes from: Delta Lake sink is transactional; `foreachBatch` + MERGE is idempotent; raw HTTP via `foreach` is at-least-once unless you idempotize.

> **🔥 Production Scenario — The State Explosion.** A streaming aggregation without watermark ran for a month. State: 500 GB; batch duration: 30 min for a 1-min trigger. Added `.withWatermark("event_time", "1 hour")`: state dropped to 2 GB; batch duration: 15s. Lesson: watermarks are not optional for stateful streaming.

> **🔥 Production Scenario — foreachBatch MERGE.** CDC pipeline: Kafka → Spark → Delta. Needed MERGE semantics (upsert + delete). Used `foreachBatch` to call `DeltaTable.merge()` per micro-batch. Idempotent (reruns a batch produces same result). Exactly-once end-to-end. Lesson: `foreachBatch` is the escape hatch for CDC apply patterns.

---

### Pattern Summary

| Need | Technique |
|------|-----------|
| Read Kafka | `spark.readStream.format("kafka")...load()` |
| Read files (auto-discover) | `spark.readStream.format("parquet").schema(s).load(path)` |
| Write Delta | `.writeStream.format("delta").outputMode("append").option("checkpointLocation", ...).start(path)` |
| Write anywhere | `.foreachBatch(fn)` |
| Micro-batch every N secs | `.trigger(processingTime="N seconds")` |
| Process-and-stop | `.trigger(availableNow=True)` |
| Windowed aggregation | `withWatermark("ts", "N min") + groupBy(F.window(...))` |
| Dedup | `.withWatermark(...).dropDuplicates([...])` |
| Stream-stream join | both sides watermarked + time range condition |
| CDC apply | `foreachBatch` + Delta MERGE |
| Custom state | `applyInPandasWithState` (3.4+) |

---

## 9. MLlib / Spark ML


### MLlib vs ML

- **`pyspark.mllib`** — the OLD, RDD-based API (deprecated for new work)
- **`pyspark.ml`** — the modern, DataFrame-based API (what you use)

Everything below uses `pyspark.ml`.

> **🧠 Principal Mindset:** In 2026, Spark ML is NOT the first choice for ML. Teams running serious ML use PyTorch / scikit-learn / XGBoost directly, or distributed via Ray / Dask / Databricks Mosaic AI. Spark ML is good for: (a) when data is already in Spark + model is simple enough, (b) GLM / tree ensembles on huge data, (c) feature engineering at scale. For deep learning or complex workflows, Spark becomes the DATA layer, not the training layer.

---

### The ML Pipeline Abstraction

```
Transformer  — DataFrame → DataFrame (adds columns)
Estimator    — DataFrame → Model (fits parameters)
Model        — Transformer with fitted parameters

Pipeline     — Sequence of stages (Transformers + Estimators)
```

Classic sklearn-style API — `fit`, `transform`, `predict`.

```python
from pyspark.ml import Pipeline
from pyspark.ml.feature import VectorAssembler, StandardScaler
from pyspark.ml.classification import LogisticRegression

assembler = VectorAssembler(inputCols=["x1", "x2", "x3"], outputCol="features_raw")
scaler = StandardScaler(inputCol="features_raw", outputCol="features")
lr = LogisticRegression(featuresCol="features", labelCol="label")

pipeline = Pipeline(stages=[assembler, scaler, lr])
model = pipeline.fit(train_df)
predictions = model.transform(test_df)
```

---

### Feature Engineering

### VectorAssembler

Combines multiple columns into a single Vector column (required input to most ML algorithms).

```python
from pyspark.ml.feature import VectorAssembler

assembler = VectorAssembler(
    inputCols=["age", "income", "balance"],
    outputCol="features",
    handleInvalid="skip"      # or "error", "keep"
)
df_vec = assembler.transform(df)
# → new column "features" is a Vector
```

### StringIndexer

Encodes string column as 0..N-1 numeric indices.

```python
from pyspark.ml.feature import StringIndexer

si = StringIndexer(inputCol="category", outputCol="category_idx", handleInvalid="keep")
model = si.fit(df)   # computes the mapping
indexed = model.transform(df)
```

### OneHotEncoder

Produces sparse one-hot vectors.

```python
from pyspark.ml.feature import OneHotEncoder

ohe = OneHotEncoder(inputCols=["category_idx"], outputCols=["category_vec"])
model = ohe.fit(indexed)
encoded = model.transform(indexed)
```

### StandardScaler / MinMaxScaler

```python
from pyspark.ml.feature import StandardScaler

scaler = StandardScaler(inputCol="features_raw", outputCol="features", 
                        withMean=True, withStd=True)
scalar_model = scaler.fit(df)
scaled = scalar_model.transform(df)
```

### Bucketizer / QuantileDiscretizer

```python
from pyspark.ml.feature import Bucketizer, QuantileDiscretizer

bucketizer = Bucketizer(
    splits=[-float("inf"), 0, 50, 100, float("inf")],
    inputCol="amount",
    outputCol="amount_bucket"
)

qd = QuantileDiscretizer(numBuckets=10, inputCol="amount", outputCol="amount_decile")
```

### Interaction / PolynomialExpansion

```python
from pyspark.ml.feature import PolynomialExpansion

poly = PolynomialExpansion(degree=2, inputCol="features", outputCol="poly_features")
```

### Text features

```python
from pyspark.ml.feature import Tokenizer, StopWordsRemover, HashingTF, IDF, Word2Vec, CountVectorizer

tok = Tokenizer(inputCol="text", outputCol="words")
sw = StopWordsRemover(inputCol="words", outputCol="filtered")
hashingTF = HashingTF(inputCol="filtered", outputCol="tf", numFeatures=10000)
idf = IDF(inputCol="tf", outputCol="features")
# OR
cv = CountVectorizer(inputCol="filtered", outputCol="tf", vocabSize=10000)
```

### Column-level transforms

```python
from pyspark.ml.feature import SQLTransformer

# Apply arbitrary SQL as a pipeline stage
sqlt = SQLTransformer(statement="SELECT *, x * y AS xy FROM __THIS__")
```

Useful for custom feature engineering within a Pipeline.

---

### Models

### Classification

```python
from pyspark.ml.classification import (
    LogisticRegression,
    DecisionTreeClassifier,
    RandomForestClassifier,
    GBTClassifier,         # Gradient Boosted Trees
    MultilayerPerceptronClassifier,
    NaiveBayes,
    LinearSVC,
    FMClassifier,          # Factorization Machines
)

lr = LogisticRegression(featuresCol="features", labelCol="label",
                        maxIter=100, regParam=0.01, elasticNetParam=0.5)
model = lr.fit(train)
predictions = model.transform(test)   # adds 'prediction', 'probability', 'rawPrediction'
```

### Regression

```python
from pyspark.ml.regression import (
    LinearRegression,
    GeneralizedLinearRegression,   # Gaussian, Poisson, Gamma, Tweedie
    DecisionTreeRegressor,
    RandomForestRegressor,
    GBTRegressor,
    FMRegressor,
    AFTSurvivalRegression,         # survival analysis
    IsotonicRegression,
)

lin = LinearRegression(featuresCol="features", labelCol="y", maxIter=100, regParam=0.1)
model = lin.fit(train)
```

### Clustering

```python
from pyspark.ml.clustering import KMeans, BisectingKMeans, GaussianMixture, LDA

km = KMeans(featuresCol="features", k=5, seed=42)
model = km.fit(train)
clustered = model.transform(df)   # adds 'prediction' column
```

### Recommendation (ALS)

```python
from pyspark.ml.recommendation import ALS

als = ALS(
    userCol="user_id", itemCol="product_id", ratingCol="rating",
    rank=10, maxIter=10, regParam=0.1, implicitPrefs=True,
    coldStartStrategy="drop"
)
model = als.fit(train)

# Recommend top-5 for each user
user_recs = model.recommendForAllUsers(5)

# Recommend top-5 users for each item
item_recs = model.recommendForAllItems(5)
```

### Frequent Pattern Mining

```python
from pyspark.ml.fpm import FPGrowth

fpg = FPGrowth(itemsCol="items", minSupport=0.1, minConfidence=0.6)
model = fpg.fit(df)
model.freqItemsets.show()
model.associationRules.show()
```

---

### Model Evaluation

### Classification metrics

```python
from pyspark.ml.evaluation import BinaryClassificationEvaluator, MulticlassClassificationEvaluator

bin_eval = BinaryClassificationEvaluator(labelCol="label", 
                                         rawPredictionCol="rawPrediction",
                                         metricName="areaUnderROC")
auc = bin_eval.evaluate(predictions)

multi_eval = MulticlassClassificationEvaluator(labelCol="label",
                                               predictionCol="prediction",
                                               metricName="f1")
f1 = multi_eval.evaluate(predictions)
# metrics: f1, accuracy, weightedPrecision, weightedRecall, logLoss, hammingLoss
```

### Regression metrics

```python
from pyspark.ml.evaluation import RegressionEvaluator

reg_eval = RegressionEvaluator(labelCol="y", predictionCol="prediction", metricName="rmse")
# metrics: rmse, mse, r2, mae, var
```

### Clustering metrics

```python
from pyspark.ml.evaluation import ClusteringEvaluator

cl_eval = ClusteringEvaluator(featuresCol="features", predictionCol="prediction")
silhouette = cl_eval.evaluate(predictions)
```

---

### Cross-Validation & Hyperparameter Tuning

### K-fold cross-validation

```python
from pyspark.ml.tuning import ParamGridBuilder, CrossValidator

paramGrid = (ParamGridBuilder()
    .addGrid(lr.regParam, [0.01, 0.1, 1.0])
    .addGrid(lr.elasticNetParam, [0.0, 0.5, 1.0])
    .build())

cv = CrossValidator(
    estimator=pipeline,
    estimatorParamMaps=paramGrid,
    evaluator=bin_eval,
    numFolds=5,
    parallelism=4,          # evaluate N param sets in parallel
    seed=42
)
cvModel = cv.fit(train)
bestModel = cvModel.bestModel
avgMetrics = cvModel.avgMetrics
```

### Train-validation split (cheaper than K-fold)

```python
from pyspark.ml.tuning import TrainValidationSplit

tvs = TrainValidationSplit(
    estimator=pipeline,
    estimatorParamMaps=paramGrid,
    evaluator=bin_eval,
    trainRatio=0.8,
    parallelism=4,
    seed=42
)
model = tvs.fit(data)
```

---

### Model Persistence

```python
# Save Pipeline (unfitted or fitted)
pipeline.write().overwrite().save("s3://bucket/pipeline/")
model.write().overwrite().save("s3://bucket/model/")

# Load
from pyspark.ml import Pipeline, PipelineModel
loaded_pipeline = Pipeline.load("s3://bucket/pipeline/")
loaded_model = PipelineModel.load("s3://bucket/model/")

# Apply
loaded_model.transform(df)
```

For Spark cluster deployment, saved models are reusable across sessions. For external deployment (Flask, FastAPI, batch inference elsewhere), export to a cross-framework format:

- **ONNX** — some Spark ML models export to ONNX
- **MLflow** — tracks + stores Spark ML models with signatures
- **PMML** — classic cross-framework format

---

### MLflow Integration

```python
import mlflow
import mlflow.spark

mlflow.set_experiment("my_experiment")

with mlflow.start_run():
    mlflow.log_param("regParam", 0.1)
    mlflow.log_param("maxIter", 100)
    
    model = pipeline.fit(train)
    predictions = model.transform(test)
    auc = evaluator.evaluate(predictions)
    
    mlflow.log_metric("auc", auc)
    mlflow.spark.log_model(model, "model")
```

MLflow is the standard tracking tool. See `../bigdata-notes-flat/09-supplementary.md` section on ML for DE.

---

### Common Patterns

### Train/test split

```python
train, test = df.randomSplit([0.8, 0.2], seed=42)
```

### Imbalanced classes — class weights

Spark ML has limited class-weight support out of the box. Workaround: resample manually.

```python
# Upsample minority class
pos = df.filter(col("label") == 1)
neg = df.filter(col("label") == 0)
ratio = neg.count() / pos.count()
pos_upsampled = pos.sample(True, ratio, seed=42)   # with replacement
balanced = pos_upsampled.unionByName(neg)
```

### Sparse feature vectors (large OneHot)

Spark's Vector type is sparse-aware. OneHotEncoder outputs sparse vectors. Downstream models handle them efficiently.

### Multi-label classification

Spark ML doesn't natively support multi-label; train one-vs-rest or use `OneVsRest`:

```python
from pyspark.ml.classification import OneVsRest
ovr = OneVsRest(classifier=lr)
```

### Feature store integration (via DataFrames)

Spark ML works on DataFrames; your feature store (Feast, Databricks FS) hands you a DataFrame → feed into pipeline.

### Streaming prediction

```python
# Load trained model
model = PipelineModel.load("s3://bucket/model/")

# Apply to streaming DataFrame
stream = spark.readStream.format("kafka")...load()
features = stream.select(F.from_json("value", schema).alias("data")).select("data.*")
predictions = model.transform(features)

(predictions.writeStream.format("delta")...start())
```

Spark ML models have no issue with streaming DataFrames (same `transform()` API).

---

### XGBoost / LightGBM on Spark

For tree ensembles at scale, native XGBoost / LightGBM Spark bindings often outperform MLlib GBT.

```python
# XGBoost4j-spark (requires separate package)
from xgboost.spark import SparkXGBClassifier

clf = SparkXGBClassifier(
    features_col="features",
    label_col="label",
    num_workers=10,
    max_depth=6,
    n_estimators=100,
)
model = clf.fit(train)
```

Databricks includes XGBoost + LightGBM Spark distributions out of the box.

---

### Spark + PyTorch / TensorFlow (Deep Learning)

Not a core Spark feature, but common patterns:

### Horovod on Spark (deprecated-ish; still works)

Distributed deep learning via MPI-like primitives.

### TorchDistributor (3.4+, Databricks + OSS)

```python
from pyspark.ml.torch.distributor import TorchDistributor

def train_fn():
    import torch.distributed as dist
    dist.init_process_group()
    # ... training loop

distributor = TorchDistributor(num_processes=4, local_mode=False, use_gpu=True)
distributor.run(train_fn)
```

### Databricks Mosaic AI

See `../bigdata-notes-flat/22-databricks-platform.md` and `../bigdata-notes-flat/25-genai-rag-for-data-engineers.md`.

---

### When Spark ML vs Alternatives

| Scenario | Best fit |
|----------|----------|
| Linear/logistic regression on 1 TB+ | Spark ML |
| Tree ensembles on 1 TB+ | XGBoost on Spark (not MLlib GBT) |
| Deep learning | PyTorch / Ray Train / TorchDistributor |
| sklearn-like workflow on < 100 GB | sklearn + Pandas / Polars |
| Collaborative filtering | Spark ML ALS |
| Topic modeling (LDA) | Spark ML or offline sklearn |
| Feature engineering only | Spark DataFrame → feed to any downstream ML |

**2026 pattern:** Spark for feature engineering + feature store writes; dedicated frameworks (PyTorch, XGBoost, LightGBM) for training.

---

### Interview Patterns

> **🎯 Interview Angle — "When would you use Spark ML?"** For simple models (GLM, tree ensembles) on data too big for a single machine (TB+), when data is already in Spark. For deep learning or complex workflows, use PyTorch/TF directly (via TorchDistributor or Ray). Spark ML's sweet spot has shrunk as dedicated frameworks became better.

> **🎯 Interview Angle — "Explain Spark ML Pipelines."** Pipeline is a sequence of Transformer and Estimator stages. Transformers add columns (StringIndexer, VectorAssembler, scalers). Estimators fit parameters (models, StringIndexer during fit). `pipeline.fit(train)` returns a PipelineModel (all stages fitted). `pipelineModel.transform(test)` applies all stages. Enables saving + loading the entire preprocessing + model as one artifact.

> **🔥 Production Scenario — The XGBoost Migration.** A classifier trained with Spark's GBT: 6 hours, AUC 0.78. Migrated to XGBoost on Spark: 90 min, AUC 0.82. Lesson: Spark's tree algorithms are competitive but not SOTA; XGBoost/LightGBM often win.

---

### Pattern Summary

| Need | Technique |
|------|-----------|
| Combine features | `VectorAssembler` |
| Encode strings | `StringIndexer` + `OneHotEncoder` |
| Scale features | `StandardScaler` / `MinMaxScaler` |
| Pipeline | `Pipeline(stages=[...])` → `.fit(train)` → `.transform(test)` |
| Classify | LogisticRegression, RF, GBT, SVC |
| Regress | LinearRegression, RF, GBT, GLM |
| Cluster | KMeans, GaussianMixture, LDA |
| Recommend | ALS |
| Cross-validate | `CrossValidator(estimator=p, estimatorParamMaps=g, evaluator=e)` |
| Save model | `model.write().save(path)` |
| Load model | `PipelineModel.load(path)` |
| Track experiments | MLflow (`mlflow.spark.log_model`) |
| Deep learning | TorchDistributor (3.4+) |

---

## 10. Performance Tuning


### The Tuning Hierarchy

Don't tune blindly. Prioritize:

1. **Read less data.** Partition pruning, column pruning, predicate pushdown.
2. **Shuffle less.** Reduce wide transformations. Broadcast where applicable.
3. **Prevent skew.** AQE + salting + hot-key isolation.
4. **Right-size parallelism.** `spark.sql.shuffle.partitions`, executor cores + count.
5. **Right-size memory.** Executor memory + overhead for your workload.
6. **Cache strategically.** Only when reused.
7. **Avoid UDFs.** Native functions > Pandas UDFs > scalar UDFs.
8. **File format + size.** Parquet; ~128-512 MB per file.
9. **Config knobs.** AQE, Arrow, codegen, serializer.

**Rule:** profile before tuning. Spark UI tells you where time goes.

---

### AQE (Adaptive Query Execution)

Enabled by default in 3.2+; verify:

```python
spark.conf.get("spark.sql.adaptive.enabled")   # should be "true"
```

### Key knobs

```python
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")
spark.conf.set("spark.sql.adaptive.localShuffleReader.enabled", "true")

# Coalesce
spark.conf.set("spark.sql.adaptive.coalescePartitions.initialPartitionNum", "500")  # start high
spark.conf.set("spark.sql.adaptive.advisoryPartitionSizeInBytes", "128m")           # target size

# Skew join
spark.conf.set("spark.sql.adaptive.skewJoin.skewedPartitionFactor", "5")            # 5x median = skewed
spark.conf.set("spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes", "256m")

# Broadcast promotion
spark.conf.set("spark.sql.adaptive.autoBroadcastJoinThreshold", "100m")             # higher than classic 10m
```

### What AQE does at runtime

1. **Coalesce small post-shuffle partitions** → reduces task count + startup overhead
2. **Promote planned SMJ → BHJ** when actual shuffle stats show a side is small
3. **Skew partition split** when actual data has hot keys

### Verify AQE applied

- `df.explain(mode="formatted")` shows `AdaptiveSparkPlan isFinalPlan=false` before execution, `true` after
- Spark UI → SQL plan shows `AdaptiveSparkPlan` nodes

### AQE won't help with

- Window function skew
- Non-join hot keys
- Skew during initial shuffle before any join

---

### Catalyst Optimizer

Catalyst runs rule-based + cost-based optimizations.

### Statistics for CBO

Without stats, Catalyst makes conservative choices. With stats, it can reorder joins + choose better algorithms.

```python
# Compute stats for a table
spark.sql("ANALYZE TABLE my_table COMPUTE STATISTICS")
spark.sql("ANALYZE TABLE my_table COMPUTE STATISTICS FOR ALL COLUMNS")

# Enable CBO
spark.conf.set("spark.sql.cbo.enabled", "true")
spark.conf.set("spark.sql.cbo.joinReorder.enabled", "true")
```

CBO is less important in 2026 because AQE handles many runtime adjustments. But for complex plans with many joins, stats help the initial plan.

### Explain plan reading

See topic 03. Key operators to watch:
- `Exchange` — shuffle
- `BroadcastExchange` — broadcast prep
- `BroadcastHashJoin` / `SortMergeJoin` / `ShuffleHashJoin`
- `HashAggregate` (partial + final)
- `Project` — column selection
- `Filter` — row filter
- `FileScan` — reads
- `AdaptiveSparkPlan` — AQE wrapper

---

### Shuffle Tuning

### `spark.sql.shuffle.partitions`

Default 200. Rule of thumb: **total shuffle data / 128 MB** = good partition count.

Example: 100 GB shuffle = 800 partitions.

With AQE enabled, set the INITIAL partition count HIGH (500-1000); AQE coalesces smaller.

### Shuffle compression

```python
spark.conf.set("spark.shuffle.compress", "true")                # default true
spark.conf.set("spark.io.compression.codec", "lz4")             # or zstd, snappy
spark.conf.set("spark.shuffle.spill.compress", "true")
```

- lz4: fastest, lower compression
- zstd: better compression, slower
- snappy: balanced (default)

For high network / low disk: lz4. For low network: zstd.

### External shuffle service (ESS)

Spark executors can lose shuffle files when they die. ESS runs separately:

```python
spark.conf.set("spark.shuffle.service.enabled", "true")
```

- YARN: usually enabled cluster-wide
- K8s: deploy as DaemonSet, OR use shuffle tracking (3.1+)
- Needed for: dynamic allocation + fault tolerance of shuffle data

### Alternative: shuffle tracking (3.1+)

```python
spark.conf.set("spark.dynamicAllocation.shuffleTracking.enabled", "true")
```

Keeps executors with shuffle data alive even when idle; avoids need for ESS. K8s-friendly.

---

### Broadcast

### Threshold

```python
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "100m")   # default 10m
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")     # disable auto
```

### Driver memory for broadcast

Broadcasting serializes the entire small side on the DRIVER before shipping. Large broadcasts need driver memory:

```python
spark.conf.set("spark.driver.memory", "8g")
spark.conf.set("spark.driver.maxResultSize", "4g")      # driver-side result cap
```

For large broadcasts (hundreds of MB), bump driver memory.

### Force / disable broadcast

```python
# Force
from pyspark.sql.functions import broadcast
df.join(broadcast(small), "key")

# Disable
df.join(small.hint("merge"), "key")   # force SMJ
# or
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")
```

---

### Memory

### Executor memory layout (recap of topic 00)

```
spark.executor.memory             # heap (e.g., 8g)
spark.executor.memoryOverhead     # off-heap overhead (default 10%, min 384 MB)
spark.memory.fraction = 0.6       # of heap: execution + storage
spark.memory.storageFraction = 0.5# of above: cache (dynamically borrowed)
```

### Common memory issues

**Executor OOM:**
- Partition too large → increase `spark.sql.shuffle.partitions`
- Skew → AQE skew split or salt
- UDF memory leak → check driver / executor logs

**Driver OOM:**
- `.collect()` on big data → use `.limit()` or `.write()` instead
- Big broadcast → increase `spark.driver.memory` or disable broadcast
- `toPandas()` on big data → same as collect

**OverheadLimitExceeded:**
- Classic: `spark.executor.memoryOverhead` too small
- Python-heavy UDFs → increase overhead (Python runs OUTSIDE heap)
- Set to 20-30% of executor memory for UDF-heavy jobs

```python
# For Python-heavy jobs:
spark.conf.set("spark.executor.memory", "8g")
spark.conf.set("spark.executor.memoryOverhead", "3g")   # 37% buffer
```

### Executor sizing rules of thumb

- **Cores per executor:** 4-5 (above this, HDFS/GC contention grows)
- **Memory per core:** 2-4 GB typical; 8+ GB for memory-heavy
- **Executors per node:** fit based on total node resources
- **Prefer MORE SMALLER executors** over few huge ones (better fault tolerance, parallelism)

Example: 10 nodes, each 32 cores + 128 GB RAM
```
cores_per_executor = 4
executors_per_node = 32 / 4 = 8  (leave 1 for OS → use 7)
memory_per_executor = (128 - 10 GB OS) / 7 ≈ 16 GB → use 15 GB + overhead 3 GB
total_executors = 10 × 7 = 70
total_cores = 70 × 4 = 280
```

---

### Caching

```python
df.cache()             # MEMORY_AND_DISK (default)
df.persist(StorageLevel.MEMORY_ONLY)       # faster if fits; fails/spills if not
df.persist(StorageLevel.MEMORY_AND_DISK_SER)  # serialized; smaller memory

df.unpersist()         # free
```

### Storage levels

| Level | Memory | Disk | Serialized |
|-------|--------|------|------------|
| MEMORY_ONLY | yes | — | no |
| MEMORY_AND_DISK | yes | spill | no |
| MEMORY_ONLY_SER | yes | — | yes (smaller) |
| MEMORY_AND_DISK_SER | yes | spill | yes |
| DISK_ONLY | — | yes | yes |
| OFF_HEAP | off-heap | — | yes |

### When to cache

- ✓ DataFrame reused in multiple actions
- ✓ Iterative algorithms (ML, graph)
- ✓ Intermediate result of expensive computation feeding multiple downstream

### When NOT to cache

- ✗ One-shot pipelines (DF used once)
- ✗ Memory tight (caching evicts cache → thrashing)
- ✗ When write already materializes it (don't cache input right before write)

### Cache uses memory from execution

Cache + execution share `spark.memory.fraction × heap`. Over-caching starves execution → spills. Monitor Spark UI → Storage tab (cached size vs available).

---

### Serialization

Default: Java serializer (slow). KRYO is much faster.

```python
spark.conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
spark.conf.set("spark.kryo.registrationRequired", "false")       # true = stricter
```

Kryo helps mostly for RDD / UDF data; DataFrames mostly go through Tungsten (optimized separately).

---

### Config Cheatsheet

Below is a comprehensive list of commonly-tuned configs. Pin them in a config class / spark-submit --conf.

### Session + core

```
spark.sql.shuffle.partitions                        # 200 default; set higher for large data
spark.default.parallelism                           # RDD parallelism
spark.sql.files.maxPartitionBytes = 128m            # per-partition file read size
spark.sql.files.openCostInBytes = 4m                # cost model for grouping files
```

### AQE

```
spark.sql.adaptive.enabled = true
spark.sql.adaptive.coalescePartitions.enabled = true
spark.sql.adaptive.skewJoin.enabled = true
spark.sql.adaptive.advisoryPartitionSizeInBytes = 128m
spark.sql.adaptive.autoBroadcastJoinThreshold = 100m
```

### Broadcast

```
spark.sql.autoBroadcastJoinThreshold = 10m          # auto-broadcast cutoff
spark.sql.broadcastTimeout = 300s                   # broadcast wait timeout
```

### Memory

```
spark.executor.memory = 8g
spark.executor.memoryOverhead = 2g                  # off-heap overhead; bump for Python UDFs
spark.executor.cores = 4
spark.driver.memory = 4g
spark.driver.maxResultSize = 2g                     # protect driver from huge collects
```

### Shuffle

```
spark.shuffle.compress = true
spark.io.compression.codec = lz4                    # or zstd / snappy
spark.shuffle.service.enabled = true                # for dynamic alloc on YARN
```

### Dynamic allocation

```
spark.dynamicAllocation.enabled = true
spark.dynamicAllocation.minExecutors = 2
spark.dynamicAllocation.maxExecutors = 50
spark.dynamicAllocation.initialExecutors = 5
spark.dynamicAllocation.executorIdleTimeout = 60s
spark.dynamicAllocation.shuffleTracking.enabled = true   # avoid needing ESS (3.1+)
```

### Arrow

```
spark.sql.execution.arrow.pyspark.enabled = true
spark.sql.execution.arrow.pyspark.fallback.enabled = true
spark.sql.execution.arrow.maxRecordsPerBatch = 10000
```

### Serializer

```
spark.serializer = org.apache.spark.serializer.KryoSerializer
```

### ANSI SQL (3.0+; default in 4.0+)

```
spark.sql.ansi.enabled = true     # stricter overflow / type checks
```

### Fault tolerance

```
spark.sql.maxCountRatioOverrideByColumnStats = 0.5
spark.task.maxFailures = 4                           # retries per task
spark.stage.maxConsecutiveAttempts = 4               # stage retries
spark.blacklist.enabled = true                       # avoid bad nodes (now called excludeOnFailure)
spark.excludeOnFailure.enabled = true                # 3.1+
```

---

### Diagnosis Workflow

### 1. Open Spark UI

Jobs tab → identify slow job → stages tab → identify slow stage.

### 2. Inside the slow stage

Check:
- **Task duration distribution** (max vs median — skew signal)
- **Shuffle read / write size**
- **Spill (memory + disk)** — ran out of memory
- **GC time** — too much garbage collection
- **Number of tasks** — too many or too few

### 3. SQL tab

Visualize the plan. Look for:
- Where the shuffle happens
- Join algorithm chosen
- Row counts at each operator
- Filter pushdown present

### 4. Executors tab

Per-executor metrics:
- Active tasks
- Task time
- GC time (should be < 10% of task time; else memory pressure)
- Memory used

### 5. Common patterns

| Symptom | Likely cause |
|---------|--------------|
| One task much slower | Skew |
| Many small tasks | Too many partitions after shuffle (bump AQE coalesce) |
| Few tasks, each slow | Too few partitions (increase shuffle.partitions) |
| High GC time | Memory pressure; increase executor memory or reduce cache |
| High spill | Partition too big; increase partitions or memory |
| Driver OOM | collect/toPandas on big data; or large broadcast |
| Executor lost | OOM in executor; check logs |
| Shuffle fetch timeout | Executor lost or slow; investigate executor logs |
| Slow write | Too few output partitions, or too many tiny files |

---

### Common Optimizations (checklist)

### Reads
- [ ] Partition pruning working? (`PartitionFilters` in explain)
- [ ] Predicate pushdown working? (`PushedFilters` in explain)
- [ ] Column pruning? (`ReadSchema` shows subset)
- [ ] File size reasonable? (128-512 MB target)
- [ ] Format columnar? (Parquet / ORC / Delta / Iceberg)
- [ ] Explicit schema? (avoid inferSchema)

### Compute
- [ ] AQE enabled?
- [ ] Shuffle partitions sized for workload?
- [ ] Skew diagnosed + addressed?
- [ ] Joins: small side broadcast? Right algorithm?
- [ ] Cache DataFrames reused in multiple actions?
- [ ] Native functions > Pandas UDFs > scalar UDFs?
- [ ] Window function spec shared where possible?

### Writes
- [ ] Target file size via coalesce / repartition?
- [ ] Dynamic partition overwrite mode?
- [ ] Avoid coalesce(1) on big data?

### Deployment
- [ ] Executor sizing: ~4 cores, 2-4 GB per core?
- [ ] Dynamic allocation enabled?
- [ ] memoryOverhead bumped for Python UDF workloads?
- [ ] Kryo serializer?
- [ ] Arrow enabled?

---

### Antipatterns

### 1. `coalesce(1)` on big data
→ Executor OOM. Use `.repartition(N)` or accept more files.

### 2. `.collect()` / `.toPandas()` on big data
→ Driver OOM. Use `.limit()` or `.write()`.

### 3. `.show()` deep in pipeline for debug
→ Triggers re-execution. Cache first.

### 4. UDFs for things built-ins can do
→ 10-100x slower. Grep `pyspark.sql.functions` first.

### 5. Chaining 20 `.withColumn` calls
→ Plan grows quadratically with chain length. Use `.select(...)` or `.withColumns({...})`.

### 6. Reading from JDBC without parallelism
→ ONE connection, ONE partition. Set `numPartitions + partitionColumn`.

### 7. Writing without partition / size control
→ Thousands of tiny files OR one giant file. Control with `.repartition()` + `.coalesce()`.

### 8. Hard-coded broadcast threshold too high
→ Driver OOM or executor OOM. Monitor.

### 9. Caching everything "just in case"
→ Cache evicts useful data; execution starves.

### 10. Not using AQE
→ Skew problems; bad partition sizing. Enable it.

---

### Interview Patterns

> **🎯 Interview Angle — "A Spark job is slow. Walk me through diagnosis."**
> 1. **Open Spark UI → Jobs tab.** Which job is slow?
> 2. **Stages tab.** Which stage dominates?
> 3. **Within the slow stage:** task duration distribution (max/median = skew?), shuffle read/write size (big shuffle?), spill bytes (memory pressure?), GC time (overhead?).
> 4. **SQL tab:** verify join algorithm, column pruning, predicate pushdown, partition pruning.
> 5. **Executors tab:** CPU, memory, GC per executor.
> 6. **Form a hypothesis**, change ONE thing, measure.
> 
> Common causes in order: data skew (fix with AQE skew join / salting), bad join algorithm (force broadcast), bad partitioning (increase/decrease shuffle.partitions), UDF overhead (replace with native/Pandas UDF), small files (coalesce before write), memory pressure (bump memory or reduce cache).

> **🎯 Interview Angle — "Why would you NOT enable AQE?"** AQE adds some planning overhead (small). Rare reasons to disable: deterministic plan requirements (reproducibility across runs), compatibility with legacy code expecting specific partition layouts, debugging specific plan choices. In 99% of cases, keep AQE on.

> **🔥 Production Scenario — The Classic Shuffle Partition Fix.** Pipeline wrote 10M files per run (default 200 partitions × many sources). Set `spark.sql.shuffle.partitions=50` + `coalesce(50)` before write. 50 files per run; 20x speedup downstream reads. Lesson: shuffle partition count IS the output file count; tune for workload.

> **🔥 Production Scenario — The Memory Overhead Fix.** Pipeline with heavy Python UDFs: `OutOfMemoryError: Container killed by YARN for exceeding memory limits. 10.5 GB of 10 GB physical memory used`. Default overhead = 10%; bumped to 30% (`spark.executor.memoryOverhead=3g` on 10g executors). Job completed. Lesson: Python runs OUTSIDE JVM heap; overhead must account for it.

---

### Pattern Summary

| Lever | When to tune |
|-------|--------------|
| `spark.sql.shuffle.partitions` | Shuffle data size / 128 MB |
| AQE enabled | Always on |
| Broadcast threshold | Up to 100m for medium dims |
| Executor cores | 4-5 |
| Executor memory | 2-4 GB / core typical; more for memory-heavy |
| memoryOverhead | 10-30% (higher for Python-heavy) |
| Dynamic allocation | On for shared clusters |
| Shuffle tracking | On for K8s (replaces ESS) |
| Kryo serializer | On |
| Arrow | On |
| Cache | Only reused DataFrames |

---

## 11. Deployment


### Deployment Mode Overview

| Mode | Where driver runs | Use for |
|------|-------------------|---------|
| **Client** | Your machine / submit node | Dev, notebooks, interactive |
| **Cluster** | Inside cluster | Production |
| **Spark Connect** | Spark cluster, client is thin | Modern pattern 2024+ |

Client mode = your laptop killing kills the job. Cluster mode = driver survives client disconnect.

---

### spark-submit

The canonical way to launch a Spark job.

```bash
spark-submit \
    --master yarn \
    --deploy-mode cluster \
    --name my_job \
    --num-executors 20 \
    --executor-cores 4 \
    --executor-memory 8g \
    --driver-memory 4g \
    --conf spark.sql.shuffle.partitions=500 \
    --conf spark.sql.adaptive.enabled=true \
    --conf spark.sql.adaptive.skewJoin.enabled=true \
    --conf spark.executor.memoryOverhead=2g \
    --packages org.apache.spark:spark-avro_2.12:3.5.0,io.delta:delta-spark_2.12:3.2.0 \
    --py-files my_lib.zip \
    --files config.yaml \
    main.py --input s3://bucket/in/ --output s3://bucket/out/
```

### Common `--master` values

- `local` — single JVM
- `local[*]` — all local cores
- `local[4]` — 4 local threads
- `yarn` — YARN cluster
- `k8s://https://api.k8s.cluster:6443` — K8s
- `spark://host:7077` — Standalone cluster

### Common args

| Flag | Meaning |
|------|---------|
| `--master` | Cluster manager + URL |
| `--deploy-mode` | `client` or `cluster` |
| `--name` | App name (Spark UI label) |
| `--class` | Main class (Scala/Java) |
| `--num-executors` | Executor count (YARN) |
| `--executor-cores` | Cores per executor |
| `--executor-memory` | Memory per executor |
| `--driver-memory` | Driver memory |
| `--driver-cores` | Driver cores |
| `--queue` | YARN queue |
| `--conf` | Any `spark.*` config |
| `--jars` | JARs to attach |
| `--packages` | Maven coords (auto-fetched) |
| `--py-files` | Python zip/egg/wheel |
| `--files` | Files to distribute to executors |
| `--properties-file` | Load configs from file |

---

### Deploying on YARN

Hadoop ecosystem; still common on-prem + older cloud deployments.

```bash
spark-submit \
    --master yarn \
    --deploy-mode cluster \
    --queue analytics \
    --num-executors 20 \
    main.py
```

Spark on YARN:
- YARN ResourceManager allocates containers
- Spark driver runs in one container (cluster mode) or on submit host (client mode)
- Dynamic allocation available (requires external shuffle service)

Common configs:
```
spark.yarn.queue = analytics
spark.yarn.maxAppAttempts = 2
spark.yarn.executor.failuresValidityInterval = 1h
spark.yarn.am.memory = 2g
spark.yarn.am.cores = 1
```

---

### Deploying on Kubernetes

Modern standard. Spark runs as native K8s pods (driver + executors).

### Via spark-submit

```bash
spark-submit \
    --master k8s://https://api.k8s.cluster:6443 \
    --deploy-mode cluster \
    --name my_job \
    --conf spark.kubernetes.namespace=spark \
    --conf spark.kubernetes.container.image=myregistry/spark-py:3.5.0 \
    --conf spark.kubernetes.authenticate.driver.serviceAccountName=spark-sa \
    --conf spark.executor.instances=10 \
    --conf spark.executor.cores=4 \
    --conf spark.executor.memory=8g \
    --conf spark.kubernetes.executor.limit.cores=4 \
    --conf spark.kubernetes.file.upload.path=s3://bucket/spark-apps/ \
    local:///opt/spark/work-dir/main.py
```

### Via Spark Operator (preferred)

See `../bigdata-notes-flat/28-kubernetes-for-data-engineering.md`.

SparkApplication CRD (`spark-app.yaml`):
```yaml
apiVersion: sparkoperator.k8s.io/v1beta2
kind: SparkApplication
metadata:
  name: my-spark-job
  namespace: spark
spec:
  type: Python
  pythonVersion: "3"
  mode: cluster
  image: myregistry/spark-py:3.5.0
  mainApplicationFile: s3://bucket/spark-apps/main.py
  arguments:
    - --input
    - s3://bucket/in/
  sparkVersion: 3.5.0
  driver:
    cores: 1
    memory: "4g"
    serviceAccount: spark-sa
    env:
      - name: AWS_ROLE_ARN
        value: arn:aws:iam::...
  executor:
    instances: 10
    cores: 4
    memory: "8g"
    coreLimit: "4"
  sparkConf:
    "spark.sql.shuffle.partitions": "500"
    "spark.sql.adaptive.enabled": "true"
  deps:
    jars:
      - local:///opt/spark/jars/aws-java-sdk-bundle-1.12.0.jar
```

Apply:
```bash
kubectl apply -f spark-app.yaml
kubectl get sparkapplication my-spark-job
kubectl logs my-spark-job-driver -n spark
```

### K8s + Spark gotchas

- **Shuffle data on pod death:** either use external shuffle service DaemonSet or enable shuffle tracking (`spark.dynamicAllocation.shuffleTracking.enabled`)
- **Local scratch space:** mount local SSD or use emptyDir for shuffle; S3 scratch is too slow
- **Driver survives:** cluster mode means driver pod survives client disconnect
- **IAM role:** use IRSA (AWS) / Workload Identity (GCP) / Pod Identity (Azure)

---

### Deploying on Databricks

Managed Spark. Clusters created via UI / API / Terraform.

### Jobs clusters (recommended for production)

Ephemeral clusters created per job run.

```yaml
# Via Databricks Asset Bundles (databricks.yml)
resources:
  jobs:
    my_job:
      name: nightly_etl
      tasks:
        - task_key: etl
          notebook_task:
            notebook_path: /notebooks/etl
          new_cluster:
            spark_version: 15.4.x-scala2.12
            node_type_id: i3.2xlarge
            num_workers: 10
            spark_conf:
              spark.sql.shuffle.partitions: "500"
              spark.sql.adaptive.enabled: "true"
```

### All-purpose clusters (interactive)

Long-running shared clusters for notebooks. Expensive at scale; prefer jobs clusters for production.

### Databricks-only features

- **Photon:** vectorized C++ engine (opt-in)
- **DBFS:** Databricks File System (abstraction over cloud storage)
- **Delta Live Tables:** declarative pipelines (see `../bigdata-notes-flat/22-databricks-platform.md`)
- **Auto Loader:** incremental file ingest
- **Databricks SQL warehouses:** SQL-optimized compute
- **Serverless Jobs / SQL:** no cluster management

### Spark version via Databricks Runtime

Databricks Runtime (DBR) is their packaged Spark + libraries. DBR 15.4 LTS ≈ Spark 3.5+ ML libraries. DBR 16.x on 4.0.

---

### Deploying on EMR (AWS)

### EMR on EC2 (classic)

```bash
aws emr create-cluster \
    --release-label emr-7.x.y \
    --applications Name=Spark \
    --instance-groups InstanceGroupType=MASTER,InstanceCount=1,InstanceType=m5.xlarge \
                      InstanceGroupType=CORE,InstanceCount=10,InstanceType=m5.2xlarge \
    --steps '[{"Type":"Spark","Name":"my_job","Args":["--deploy-mode","cluster","s3://bucket/main.py"]}]' \
    --use-default-roles
```

### EMR Serverless (newer, preferred for most)

```bash
aws emr-serverless create-application \
    --name my-app --release-label emr-7.x.y --type SPARK

aws emr-serverless start-job-run \
    --application-id ... \
    --job-driver '{"sparkSubmit": {"entryPoint": "s3://bucket/main.py", ...}}'
```

Pay per compute used; no cluster to manage.

### EMR on EKS

Spark on K8s running on EKS (AWS-managed K8s). Same as self-managed K8s but AWS runs the control plane.

---

### Deploying on Dataproc (GCP)

```bash
# Standard Dataproc cluster
gcloud dataproc clusters create my-cluster \
    --region us-central1 \
    --image-version 2.2-debian11 \
    --num-workers 10 \
    --master-machine-type n1-standard-4 \
    --worker-machine-type n1-standard-4

gcloud dataproc jobs submit pyspark gs://bucket/main.py \
    --cluster my-cluster \
    --region us-central1 \
    --properties spark.sql.shuffle.partitions=500

# Dataproc Serverless (preferred for burst)
gcloud dataproc batches submit pyspark gs://bucket/main.py \
    --region us-central1 \
    --properties spark.sql.shuffle.partitions=500

# Dataproc on GKE
gcloud dataproc clusters gke create ...
```

---

### Deploying on Azure Synapse / HDInsight

Covered in `../bigdata-notes-flat/18-azure-data-engineering.md`. Azure Synapse Spark pools or HDInsight Spark clusters.

---

### Spark Connect (3.4+ GA 4.0)

Client/server architecture:
- Server: long-running Spark cluster
- Client: thin, submits DataFrame operations via gRPC

```bash
# Start server
./sbin/start-connect-server.sh --packages ...
```

```python
# Client
from pyspark.sql import SparkSession
spark = SparkSession.builder.remote("sc://host:15002").getOrCreate()

df = spark.read.parquet(...)
df.show()
```

### Benefits

- Thin clients (no full Spark install)
- Multiple languages (Python, Scala, Rust, Go, R)
- Long-lived cluster shared across clients
- Decouple client version from server version

### Limitations (as of 3.5)

- Not all APIs supported (RDD API is limited; some streaming APIs limited)
- Observability from client side limited

### 2026 pattern

**Databricks has moved to Spark Connect by default.** Local sessions still work via `spark://local`. For new deployments, favor Spark Connect.

---

### Python Dependencies

### Approach 1: `--py-files`

```bash
# Zip up your package
zip -r my_lib.zip my_lib/

spark-submit --py-files my_lib.zip main.py
```

### Approach 2: PySpark package via pip (recommended)

```bash
pip install -t deps/ pandas scikit-learn
cd deps && zip -r ../deps.zip . && cd ..

spark-submit --py-files deps.zip main.py
```

### Approach 3: PySpark + conda/venv on executors

Package full environment:
```bash
conda-pack -o my_env.tar.gz
spark-submit --archives my_env.tar.gz#my_env main.py
```

```python
# In main.py
spark = (SparkSession.builder
    .config("spark.pyspark.python", "./my_env/bin/python")
    .config("spark.pyspark.driver.python", "./my_env/bin/python")
    .getOrCreate())
```

### Approach 4: Container image (K8s, Databricks)

Build Docker image with all deps baked in; cluster uses that image.

```dockerfile
FROM apache/spark-py:3.5.0
USER root
RUN pip install pandas scikit-learn boto3
COPY main.py /opt/spark/work-dir/
```

### Approach 5: `pip install` in cluster (Databricks)

Databricks notebooks can `%pip install`. Jobs can specify `libraries`.

---

### JAR Dependencies

### `--jars`

Local JAR files on classpath:
```bash
spark-submit --jars my.jar,other.jar main.py
```

### `--packages`

Maven coordinates — Spark downloads from Maven Central:
```bash
spark-submit --packages org.apache.spark:spark-avro_2.12:3.5.0,io.delta:delta-spark_2.12:3.2.0 main.py
```

### Common packages

```
# Avro
org.apache.spark:spark-avro_2.12:3.5.0

# Delta Lake
io.delta:delta-spark_2.12:3.2.0

# Iceberg
org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.0

# AWS S3 (hadoop-aws)
org.apache.hadoop:hadoop-aws:3.3.4
com.amazonaws:aws-java-sdk-bundle:1.12.262

# BigQuery
com.google.cloud.spark:spark-bigquery-with-dependencies_2.12:0.32.2

# Kafka
org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0
```

### Scala binary version matters

- Spark 3.x binaries typically built for Scala 2.12 (sometimes 2.13)
- Spark 4.0+ builds for 2.13 primarily
- Match package to runtime Scala version (`spark-shell` prints it)

---

### Deployment Patterns

### Blue/green cluster deployment

Deploy to a new cluster; validate; switch traffic. Minimizes risk of version upgrade issues.

### Rolling restart (streaming)

Stop old streaming queries, restart with new code, same checkpoint path. Validate restart compatibility.

### Canary deployment

Deploy new version to small cluster; compare metrics; promote.

### Multi-cluster for isolation

- Prod cluster for critical workloads
- Dev cluster for experimentation
- Shared cluster for ad-hoc analytics

---

### Spark on K8s — Operator vs spark-submit

| | spark-submit | Spark Operator |
|-|--------------|----------------|
| Declarative | no | yes (CRD) |
| K8s-native | partial | yes |
| Scheduling | manual | via Volcano / default |
| Monitoring | Spark UI only | + K8s events + CRD status |
| GitOps-friendly | awkward | easy |
| Cleanup | manual | automatic (OwnerReferences) |

**Modern preference:** Spark Operator for production K8s deployments.

---

### Environment Variables (common)

```bash
export PYSPARK_PYTHON=/path/to/python
export PYSPARK_DRIVER_PYTHON=/path/to/python
export SPARK_HOME=/path/to/spark
export HADOOP_CONF_DIR=/etc/hadoop/conf
export YARN_CONF_DIR=/etc/hadoop/conf

# AWS
export AWS_ACCESS_KEY_ID=...
export AWS_SECRET_ACCESS_KEY=...
# or use IAM role

# Memory debug
export SPARK_LOCAL_DIRS=/mnt/ssd   # local scratch for shuffle
```

---

### Interview Patterns

> **🎯 Interview Angle — "Compare YARN vs K8s for Spark."** YARN: Hadoop-ecosystem; mature; well-understood resource model; still common on-prem + legacy cloud. K8s: unified compute (Spark + services + ML); portable (works on-prem, EKS, GKE, AKS); better autoscaling (Karpenter, KEDA); modern standard. For NEW deployments: K8s. For legacy Hadoop: YARN. Both work; K8s is the future.

> **🎯 Interview Angle — "Explain Spark Connect."** Decouples client from server. Client is thin (Python module); server is Spark cluster. gRPC between them. Benefits: thin clients, multi-language (Python/Scala/Rust/Go), long-lived shared clusters, independent versioning. GA in Spark 4.0. In 2026 Databricks has adopted it; new deployments should default to it.

> **🔥 Production Scenario — The K8s Shuffle Fix.** Spark jobs on K8s with spot instances; executors dying mid-stage; shuffle data lost; cascading stage retries. Enabled `spark.dynamicAllocation.shuffleTracking.enabled=true` → shuffle-holding executors kept alive even when idle. Dropped retries 90%. Lesson: shuffle planning is the big difference on K8s vs YARN.

> **🔥 Production Scenario — The Dependency Nightmare.** Pipeline worked locally; failed on YARN with `ModuleNotFoundError`. Executors didn't have `pandas` installed. Fix: packaged conda env with `conda-pack` + `--archives`. Lesson: executor Python environment ≠ driver's; must distribute deps explicitly.

---

### Pattern Summary

| Need | Technique |
|------|-----------|
| Local dev | `local[*]` master |
| YARN prod | `--master yarn --deploy-mode cluster` |
| K8s prod | Spark Operator (CRD) or `spark-submit --master k8s://...` |
| Databricks prod | Jobs clusters via Asset Bundles |
| EMR | EMR Serverless (burst) or EMR on EC2 (sustained) |
| GCP | Dataproc Serverless |
| Modern pattern | Spark Connect (3.4+/4.0) |
| Python deps | `--py-files` (small) or `conda-pack --archives` (full env) or container image |
| JAR deps | `--jars` (local) or `--packages` (Maven) |
| Dynamic alloc on K8s | `shuffleTracking.enabled=true` |

---

## 12. Delta Lake & Lakehouse Integration


See the sibling notes for depth on the formats themselves:
- `../bigdata-notes-flat/12-delta-lake.md`
- `../bigdata-notes-flat/11-iceberg.md`
- `../bigdata-notes-flat/13-hudi.md`

This chapter focuses on the PySpark INTEGRATION patterns.

---

### Delta Lake with PySpark

### Setup

```python
from pyspark.sql import SparkSession

spark = (SparkSession.builder
    .appName("delta_app")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.2.0")
    .getOrCreate())
```

### Create table

```python
# Via DataFrame API
df.write.format("delta").save("s3://bucket/delta/orders/")

# Via SQL
spark.sql("""
    CREATE TABLE orders (
        order_id BIGINT,
        user_id STRING,
        amount DOUBLE,
        order_date DATE
    ) USING DELTA
    PARTITIONED BY (order_date)
    LOCATION 's3://bucket/delta/orders/'
""")
```

### Read

```python
df = spark.read.format("delta").load("s3://bucket/delta/orders/")

# Time travel
df_v10 = spark.read.format("delta").option("versionAsOf", 10).load(path)
df_hist = spark.read.format("delta").option("timestampAsOf", "2024-10-15").load(path)
```

### Write modes

```python
# Append
df.write.format("delta").mode("append").save(path)

# Overwrite (entire table)
df.write.format("delta").mode("overwrite").save(path)

# Overwrite WHERE (partition-level + predicate-level)
(df.write.format("delta")
   .mode("overwrite")
   .option("replaceWhere", "order_date >= '2024-10-01' AND order_date < '2024-11-01'")
   .save(path))

# Overwrite with schema change
df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(path)

# Append with schema evolution (merge new columns)
df.write.format("delta").mode("append").option("mergeSchema", "true").save(path)
```

### MERGE (upsert + delete)

```python
from delta.tables import DeltaTable

target = DeltaTable.forPath(spark, "s3://bucket/delta/orders/")

(target.alias("t").merge(
    df_updates.alias("s"),
    "t.order_id = s.order_id")
    .whenMatchedUpdate(set={
        "amount": "s.amount",
        "status": "s.status",
        "updated_at": "s.updated_at"
    })
    .whenMatchedDelete(condition="s.op = 'DELETE'")
    .whenNotMatchedInsert(values={
        "order_id": "s.order_id",
        "user_id": "s.user_id",
        "amount": "s.amount",
        "order_date": "s.order_date"
    })
    .execute())
```

Or whenMatchedUpdateAll / whenNotMatchedInsertAll for all columns:
```python
(target.alias("t").merge(df_updates.alias("s"), "t.order_id = s.order_id")
    .whenMatchedUpdateAll(condition="s.op != 'DELETE'")
    .whenMatchedDelete(condition="s.op = 'DELETE'")
    .whenNotMatchedInsertAll(condition="s.op != 'DELETE'")
    .execute())
```

### MERGE WHEN NOT MATCHED BY SOURCE (3.0+)

Handles deletions when a key no longer exists in source:
```python
(target.alias("t").merge(df_updates.alias("s"), "t.order_id = s.order_id")
    .whenMatchedUpdateAll()
    .whenNotMatchedInsertAll()
    .whenNotMatchedBySourceDelete()   # delete rows in target not in source
    .execute())
```

### Update / Delete

```python
# UPDATE
target.update(
    condition="status = 'pending' AND created_at < '2024-01-01'",
    set={"status": "'expired'"}
)

# DELETE
target.delete("created_at < '2023-01-01'")
```

### History + versioning

```python
# History
target.history().show()
# → columns: version, timestamp, userName, operation, operationParameters, ...

# Restore to older version
spark.sql("RESTORE TABLE delta.`s3://bucket/delta/orders` TO VERSION AS OF 10")
```

### Maintenance

```python
# OPTIMIZE (file compaction)
spark.sql("OPTIMIZE delta.`s3://bucket/delta/orders`")

# OPTIMIZE WHERE (specific partition)
spark.sql("OPTIMIZE delta.`s3://bucket/delta/orders` WHERE order_date >= '2024-10-01'")

# Z-ORDER (Databricks; multi-dim clustering)
spark.sql("OPTIMIZE delta.`s3://bucket/delta/orders` ZORDER BY (user_id, product_id)")

# VACUUM (delete unused files)
target.vacuum(retentionHours=168)   # 7 days
spark.sql("VACUUM delta.`s3://bucket/delta/orders` RETAIN 168 HOURS")
```

### Streaming

```python
# Delta as streaming source (append-only)
stream = (spark.readStream.format("delta").load(path))

# Change Data Feed (CDC)
cdc = (spark.readStream.format("delta")
       .option("readChangeFeed", "true")
       .option("startingVersion", 100)
       .load(path))
# → columns: _change_type ∈ {insert, update_preimage, update_postimage, delete}, _commit_version, _commit_timestamp, plus original columns

# Enable CDF on a table
spark.sql("ALTER TABLE delta.`s3://bucket/delta/orders` SET TBLPROPERTIES (delta.enableChangeDataFeed = true)")
```

### Delta sink pattern

```python
(stream.writeStream.format("delta")
    .outputMode("append")
    .option("checkpointLocation", "s3://bucket/ck/orders/")
    .option("mergeSchema", "true")
    .start("s3://bucket/delta/orders/"))
```

### Delta Lake 3.x features

- **Deletion Vectors:** fast DELETE/UPDATE without rewriting files
  ```sql
  ALTER TABLE t SET TBLPROPERTIES ('delta.enableDeletionVectors' = 'true');
  ```
- **UniForm:** writes Delta + Iceberg metadata in one table (readable by both)
- **Liquid Clustering (Databricks):** adaptive data layout; no partition needed

---

### Iceberg with PySpark

### Setup

```python
spark = (SparkSession.builder
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtension")
    .config("spark.sql.catalog.my_catalog", "org.apache.iceberg.spark.SparkCatalog")
    .config("spark.sql.catalog.my_catalog.type", "hive")   # or 'glue', 'nessie', 'rest', 'polaris'
    .config("spark.sql.catalog.my_catalog.uri", "thrift://...")
    # OR for AWS Glue:
    # .config("spark.sql.catalog.my_catalog.catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog")
    # .config("spark.sql.catalog.my_catalog.warehouse", "s3://bucket/warehouse/")
    .config("spark.jars.packages", "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.0")
    .getOrCreate())
```

### Create

```python
spark.sql("""
    CREATE TABLE my_catalog.db.orders (
        order_id BIGINT,
        user_id STRING,
        amount DOUBLE,
        event_time TIMESTAMP
    ) USING iceberg
    PARTITIONED BY (days(event_time), bucket(50, user_id))
""")
```

### Read / write

```python
df = spark.table("my_catalog.db.orders")
# OR
df = spark.read.format("iceberg").load("my_catalog.db.orders")

# Write
df.writeTo("my_catalog.db.orders").create()        # CTAS
df.writeTo("my_catalog.db.orders").append()
df.writeTo("my_catalog.db.orders").overwritePartitions()
df.writeTo("my_catalog.db.orders").replace()       # replace table contents

# Time travel
spark.read.option("snapshot-id", "1234567890").format("iceberg").load(...)
spark.read.option("as-of-timestamp", "2024-10-15").format("iceberg").load(...)
```

### MERGE

```python
spark.sql("""
    MERGE INTO my_catalog.db.target t
    USING my_catalog.db.updates s
    ON t.order_id = s.order_id
    WHEN MATCHED AND s.op = 'DELETE' THEN DELETE
    WHEN MATCHED THEN UPDATE SET amount = s.amount
    WHEN NOT MATCHED THEN INSERT (order_id, amount) VALUES (s.order_id, s.amount)
""")
```

### Maintenance (via SQL)

```sql
-- Expire old snapshots (reclaim storage)
CALL my_catalog.system.expire_snapshots('db.orders', TIMESTAMP '2024-10-01', 10);

-- Remove orphan files (not referenced in metadata)
CALL my_catalog.system.remove_orphan_files(table => 'db.orders');

-- Rewrite data files (compaction)
CALL my_catalog.system.rewrite_data_files('db.orders');

-- Rewrite manifests (metadata-level optimization)
CALL my_catalog.system.rewrite_manifests('db.orders');
```

### Partition evolution

```sql
-- Change partition spec WITHOUT rewriting data
ALTER TABLE my_catalog.db.orders ADD PARTITION FIELD months(event_time);
ALTER TABLE my_catalog.db.orders DROP PARTITION FIELD days(event_time);
```

### Snapshot operations

```sql
-- Rollback to snapshot
CALL my_catalog.system.rollback_to_snapshot('db.orders', 1234567890);
```

---

### Hudi with PySpark

### Setup

```python
spark = (SparkSession.builder
    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    .config("spark.jars.packages", "org.apache.hudi:hudi-spark3.5-bundle_2.12:0.14.0")
    .config("spark.sql.extensions", "org.apache.spark.sql.hudi.HoodieSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.hudi.catalog.HoodieCatalog")
    .getOrCreate())
```

### Write (upsert)

```python
hudi_options = {
    "hoodie.table.name": "orders",
    "hoodie.datasource.write.recordkey.field": "order_id",
    "hoodie.datasource.write.partitionpath.field": "country",
    "hoodie.datasource.write.precombine.field": "updated_at",
    "hoodie.datasource.write.operation": "upsert",       # or 'insert', 'bulk_insert'
    "hoodie.datasource.write.table.type": "COPY_ON_WRITE",  # or MERGE_ON_READ
    "hoodie.upsert.shuffle.parallelism": 200,
}

(df.write.format("hudi")
    .options(**hudi_options)
    .mode("append")
    .save("s3://bucket/hudi/orders"))
```

### Read

```python
# Snapshot query (current state)
df = spark.read.format("hudi").load("s3://bucket/hudi/orders")

# Incremental query (changes since commit time)
df = (spark.read.format("hudi")
      .option("hoodie.datasource.query.type", "incremental")
      .option("hoodie.datasource.read.begin.instanttime", "20241015000000")
      .load("s3://bucket/hudi/orders"))

# Read-optimized (MoR only; fast; reads base files only, misses recent updates)
df = (spark.read.format("hudi")
      .option("hoodie.datasource.query.type", "read_optimized")
      .load("s3://bucket/hudi/orders"))
```

### CoW vs MoR

- **COPY_ON_WRITE (CoW):** rewrites files on each write; reads are fast
- **MERGE_ON_READ (MoR):** writes delta log files; reads merge on-the-fly; writes fast, reads slower

Pick CoW for read-heavy; MoR for write-heavy/streaming.

See `../bigdata-notes-flat/13-hudi.md`.

---

### Lakehouse Patterns

### Medallion architecture

```
BRONZE: raw ingested data
   ↓
SILVER: cleaned, deduplicated, conformed
   ↓
GOLD: business-ready aggregates
```

#### Example

```python
# BRONZE: raw Kafka → Delta
bronze_stream = (spark.readStream.format("kafka")...load())
(bronze_stream.selectExpr("CAST(key AS STRING) AS key", "CAST(value AS STRING) AS raw_value", "timestamp")
    .writeStream.format("delta")
    .option("checkpointLocation", "s3://ck/bronze/")
    .start("s3://bronze/events/"))

# SILVER: parsed + validated
bronze = spark.readStream.format("delta").load("s3://bronze/events/")
silver = (bronze
    .select(F.from_json("raw_value", schema).alias("data"))
    .select("data.*")
    .filter(col("event_id").isNotNull())
    .withWatermark("event_time", "10 minutes")
    .dropDuplicates(["event_id"]))

(silver.writeStream.format("delta")
    .option("checkpointLocation", "s3://ck/silver/")
    .start("s3://silver/events/"))

# GOLD: aggregates
silver_batch = spark.read.format("delta").load("s3://silver/events/")
gold = (silver_batch
    .groupBy(F.date_trunc("day", "event_time").alias("day"), "country")
    .agg(F.count("*").alias("events"), F.countDistinct("user_id").alias("users")))

gold.write.format("delta").mode("overwrite").option("replaceWhere", "day >= '...'").save("s3://gold/daily_counts/")
```

### Streaming CDC apply (foreachBatch + MERGE)

```python
# See topic 08 for full pattern

def apply_cdc(batch_df, batch_id):
    target = DeltaTable.forPath(spark, "s3://silver/users/")
    (target.alias("t").merge(batch_df.alias("s"), "t.id = s.id")
        .whenMatchedUpdateAll(condition="s.op != 'DELETE'")
        .whenMatchedDelete(condition="s.op = 'DELETE'")
        .whenNotMatchedInsertAll(condition="s.op != 'DELETE'")
        .execute())

(cdc_stream.writeStream
    .foreachBatch(apply_cdc)
    .option("checkpointLocation", "s3://ck/users_cdc/")
    .start())
```

### Daily batch rebuild (gold)

```python
def rebuild_gold_daily(date):
    silver = spark.read.format("delta").load("s3://silver/events/")
    daily = silver.filter(col("event_date") == date).groupBy("country").agg(F.count("*").alias("c"))
    
    (daily.write.format("delta")
        .option("replaceWhere", f"event_date = '{date}'")
        .mode("overwrite")
        .save("s3://gold/daily_country_counts/"))

# Call per date, idempotent (replaceWhere limits overwrite scope)
```

### SCD Type 2 with MERGE

```python
# Incoming updates
incoming = spark.read.format("delta").load("s3://silver/customer_updates/")

# Target: SCD2 history
target = DeltaTable.forPath(spark, "s3://gold/dim_customer/")

# Step 1: close out existing rows that have changes
staged = incoming.alias("s").join(
    target.toDF().alias("t"),
    (col("s.customer_id") == col("t.customer_id")) & (col("t.is_current") == True),
    "left"
).filter(
    (col("t.customer_id").isNull()) |          # new row
    (col("s.name") != col("t.name")) |         # change detected
    (col("s.email") != col("t.email"))
)

# Step 2: use MERGE with WHEN MATCHED to close + WHEN NOT MATCHED to insert new version
# This is a classic "SCD2 merge" pattern; see Delta documentation for the full form
```

---

### Choosing Delta vs Iceberg vs Hudi

| Need | Best fit |
|------|----------|
| Databricks-centric | Delta (first-class) |
| Multi-engine (Spark + Trino + Snowflake + ...) | Iceberg |
| Streaming CDC with upserts | Hudi MoR |
| Simplest batch Spark upserts | Delta MERGE |
| Open ecosystem bet | Iceberg |
| Fastest query performance | Iceberg or Delta (similar) |
| Schema + partition evolution | Iceberg (strongest) |

In 2026 trends:
- Iceberg + Polaris catalog = multi-engine lakehouse standard
- Delta + UniForm = Delta-native + Iceberg-readable
- Hudi = streaming-heavy niche

---

### Interview Patterns

> **🎯 Interview Angle — "Walk me through a MERGE in Delta."** `DeltaTable.forPath(spark, path)` returns a DeltaTable. `.merge(source_df, "t.id = s.id")` begins. Chain `whenMatchedUpdate`, `whenMatchedDelete`, `whenNotMatchedInsert`. `.execute()` runs atomically. Use for upserts, SCD2, CDC apply. Transactional: no partial state visible to readers.

> **🎯 Interview Angle — "Streaming MERGE in Delta?"** Direct streaming MERGE is NOT supported. Pattern: `foreachBatch(fn)` where `fn(batch_df, batch_id)` calls `DeltaTable.merge(batch_df, ...).execute()`. Each micro-batch is a transactional MERGE. Idempotent (same batch_id = same result). Standard pattern for streaming CDC apply.

> **🔥 Production Scenario — The replaceWhere Idempotency Save.** Daily batch overwrote the entire gold table with each run (`mode("overwrite")`). A failed re-run corrupted the day's data. Changed to `.option("replaceWhere", "day = '{date}'")`: overwrite only the current day's partition. Re-runs are safe. Lesson: `replaceWhere` is the Delta pattern for idempotent partition-level overwrite.

---

### Pattern Summary

| Operation | Delta | Iceberg | Hudi |
|-----------|-------|---------|------|
| Create | `df.write.format("delta").save()` | `df.writeTo("cat.db.t").create()` | `df.write.format("hudi").options(...).save()` |
| Append | `.mode("append")` | `.append()` | `operation=insert` or `upsert` |
| Overwrite partitions | `replaceWhere` option | `.overwritePartitions()` | `operation=insert_overwrite` |
| MERGE | `DeltaTable.merge()` | `MERGE INTO` SQL | upsert (recordKey + precombine) |
| Time travel | `versionAsOf` / `timestampAsOf` | `snapshot-id` / `as-of-timestamp` | `instanttime` |
| CDC (change feed) | `readChangeFeed=true` | n/a native | incremental query |
| Streaming source | `spark.readStream.format("delta")` | ✓ | ✓ |
| Streaming sink | `.writeStream.format("delta")` | ✓ | ✓ |
| Maintenance | OPTIMIZE / VACUUM | expire_snapshots / rewrite_data_files | compaction / clustering |

---

## 13. Testing PySpark


### Testing Levels for PySpark

| Level | What | Tools |
|-------|------|-------|
| **Unit** | Individual transformations as pure functions | pytest + fixtures |
| **Integration** | Multi-step pipelines against local Spark | pytest-spark + Testcontainers |
| **End-to-end** | Full pipeline on real cluster w/ real data | Databricks CI, Airflow test envs |

See `../bigdata-notes-flat/31-testing-data-pipelines.md` for the broader testing framework.

---

### Pure Function Pattern (easiest to test)

Factor transformations into **pure functions** that take + return DataFrames. Each is independently testable.

```python
# bad: untestable pipeline mixing IO and transforms
def run_pipeline():
    df = spark.read.parquet("...")
    df = df.filter(...).join(...).groupBy(...).agg(...)
    df.write.parquet("...")

# good: testable transformations
def filter_completed(df):
    return df.filter(F.col("status") == "completed")

def join_customers(orders, customers):
    return orders.join(customers, "customer_id", "left")

def daily_revenue(df):
    return df.groupBy("country", "date").agg(F.sum("amount").alias("revenue"))

def run_pipeline():
    orders = spark.read.parquet("orders/")
    customers = spark.read.parquet("customers/")
    result = daily_revenue(join_customers(filter_completed(orders), customers))
    result.write.parquet("output/")
```

Now each function is unit-testable.

---

### Unit Testing with pytest

### Fixture: shared SparkSession

```python
# conftest.py
import pytest
from pyspark.sql import SparkSession

@pytest.fixture(scope="session")
def spark():
    spark = (SparkSession.builder
             .master("local[2]")
             .appName("unit-tests")
             .config("spark.sql.shuffle.partitions", "2")
             .config("spark.ui.enabled", "false")
             .getOrCreate())
    yield spark
    spark.stop()
```

### Basic unit test

```python
# test_transformations.py
from my_package.transforms import filter_completed, daily_revenue

def test_filter_completed(spark):
    data = [
        (1, "pending", 100),
        (2, "completed", 200),
        (3, "completed", 50),
    ]
    df = spark.createDataFrame(data, ["id", "status", "amount"])
    
    result = filter_completed(df)
    rows = result.collect()
    
    assert len(rows) == 2
    assert all(r["status"] == "completed" for r in rows)

def test_daily_revenue(spark):
    from pyspark.sql.types import StructType, StructField, StringType, DateType, DoubleType
    import datetime
    
    schema = StructType([
        StructField("country", StringType()),
        StructField("date", DateType()),
        StructField("amount", DoubleType()),
    ])
    data = [
        ("US", datetime.date(2024, 10, 19), 100.0),
        ("US", datetime.date(2024, 10, 19), 50.0),
        ("UK", datetime.date(2024, 10, 19), 30.0),
    ]
    df = spark.createDataFrame(data, schema)
    
    result = daily_revenue(df)
    rows = {(r["country"], r["date"]): r["revenue"] for r in result.collect()}
    
    assert rows[("US", datetime.date(2024, 10, 19))] == 150.0
    assert rows[("UK", datetime.date(2024, 10, 19))] == 30.0
```

### Tips

- Use `master("local[2]")` for speed (2 cores → light but parallel)
- Set `spark.sql.shuffle.partitions=2` (default 200 is slow for tests)
- Disable Spark UI: `spark.ui.enabled=false`
- Session-scoped fixture (one Spark per test session) — much faster than per-test

---

### pytest-spark

A library that provides Spark fixtures + helpers.

```bash
pip install pytest-spark
```

```python
# conftest.py (minimal setup — pytest-spark provides fixtures)
# Just add this to pytest.ini or pyproject.toml:
# [pytest]
# spark_options =
#     spark.master=local[2]
#     spark.sql.shuffle.partitions=2
#     spark.ui.enabled=false

def test_my_func(spark_session):
    df = spark_session.createDataFrame([(1,)], ["x"])
    assert df.count() == 1
```

`spark_session` fixture auto-provided.

---

### chispa — DataFrame assertions

```bash
pip install chispa
```

```python
from chispa.dataframe_comparer import assert_df_equality

def test_result(spark):
    actual = filter_completed(input_df)
    
    expected_data = [...]
    expected = spark.createDataFrame(expected_data, schema)
    
    # Compare row-by-row, order-insensitive
    assert_df_equality(actual, expected, ignore_row_order=True, ignore_column_order=True)
```

### Variants

```python
from chispa.dataframe_comparer import (
    assert_df_equality,        # strict equality
    assert_approx_df_equality, # numeric tolerance for doubles
)
from chispa.column_comparer import (
    assert_column_equality,
    assert_approx_column_equality,
)
from chispa.rows_comparer import assert_rows_equality

# Numeric tolerance
assert_approx_df_equality(actual, expected, precision=0.01)

# Compare single column
assert_column_equality(df, "actual_col", "expected_col")
```

### Common assertion patterns

```python
# Row count
assert df.count() == expected_count

# Columns present
assert set(df.columns) == {"col1", "col2", "col3"}

# No nulls in column
assert df.filter(F.col("col").isNull()).count() == 0

# Distinct values
distinct_vals = {r[0] for r in df.select("col").distinct().collect()}
assert distinct_vals == {"a", "b", "c"}

# Aggregated value
total = df.agg(F.sum("amount")).collect()[0][0]
assert total == 1500.0
```

---

### Testing SQL

```python
def test_sql_query(spark):
    data = [...]
    df = spark.createDataFrame(data, schema)
    df.createOrReplaceTempView("orders")
    
    result = spark.sql("""
        SELECT country, SUM(amount) AS total
        FROM orders
        WHERE status = 'completed'
        GROUP BY country
    """)
    
    # Assert as usual
    rows = {r["country"]: r["total"] for r in result.collect()}
    assert rows == {"US": 150.0, "UK": 30.0}
```

Register views; test like any other DataFrame.

---

### Testing UDFs

```python
# Test the raw function (NOT via Spark) for speed
def test_pandas_udf_logic():
    import pandas as pd
    from my_package.transforms import upper_pandas
    
    s = pd.Series(["abc", "DEF", None])
    result = upper_pandas.func(s)  # ← .func accesses the undecorated function
    
    assert result.tolist() == ["ABC", "DEF", None]

# Also test through Spark for integration
def test_pandas_udf_spark(spark):
    df = spark.createDataFrame([("abc",), ("DEF",)], ["name"])
    result = df.withColumn("upper", upper_pandas("name"))
    assert_df_equality(result, spark.createDataFrame([("abc", "ABC"), ("DEF", "DEF")], ["name", "upper"]))
```

---

### Integration Testing with Testcontainers

For pipelines that touch external systems (Postgres, Kafka, MinIO/S3):

```python
# conftest.py
from testcontainers.core.container import DockerContainer
from testcontainers.postgres import PostgresContainer
import pytest

@pytest.fixture(scope="session")
def postgres():
    with PostgresContainer("postgres:16") as pg:
        yield pg

def test_jdbc_write(spark, postgres):
    url = postgres.get_connection_url().replace("postgresql+psycopg2", "jdbc:postgresql")
    
    df = spark.createDataFrame([(1, "Alice"), (2, "Bob")], ["id", "name"])
    (df.write.format("jdbc")
        .option("url", url)
        .option("dbtable", "test_users")
        .option("user", postgres.POSTGRES_USER)
        .option("password", postgres.POSTGRES_PASSWORD)
        .mode("overwrite")
        .save())
    
    # Verify via psycopg2
    import psycopg2
    conn = psycopg2.connect(postgres.get_connection_url())
    with conn.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM test_users")
        assert cur.fetchone()[0] == 2
```

See `../bigdata-notes-flat/31-testing-data-pipelines.md` for Testcontainers depth.

---

### Testing Streaming Queries

### Testing with rate source

```python
def test_streaming(spark):
    stream = (spark.readStream.format("rate")
              .option("rowsPerSecond", 10)
              .load())
    
    # Transform
    result = stream.withColumn("doubled", F.col("value") * 2)
    
    # Memory sink (test-only)
    query = (result.writeStream
             .format("memory")
             .queryName("test_output")
             .outputMode("append")
             .start())
    
    import time
    time.sleep(3)
    query.stop()
    
    # Verify
    output = spark.sql("SELECT COUNT(*) AS c FROM test_output")
    assert output.collect()[0]["c"] > 0
```

### Testing with controlled source (file)

```python
def test_streaming_file(spark, tmp_path):
    input_dir = tmp_path / "input"
    input_dir.mkdir()
    
    # Write initial data
    spark.createDataFrame([(1, "a")], ["id", "val"]).write.json(str(input_dir / "batch1"))
    
    stream = (spark.readStream
              .schema("id INT, val STRING")
              .json(str(input_dir)))
    
    query = (stream.writeStream.format("memory").queryName("test").outputMode("append").start())
    query.processAllAvailable()
    
    # Add more data
    spark.createDataFrame([(2, "b")], ["id", "val"]).write.json(str(input_dir / "batch2"))
    query.processAllAvailable()
    query.stop()
    
    assert spark.sql("SELECT COUNT(*) FROM test").collect()[0][0] == 2
```

`.processAllAvailable()` processes all available batches then stops consuming.

---

### Snapshot Testing

Compare current output to a saved "golden" result:

```python
def test_snapshot(spark, tmp_path):
    actual = my_transform(input_df)
    
    golden_path = "tests/golden/my_transform.parquet"
    
    if not os.path.exists(golden_path):
        actual.coalesce(1).write.mode("overwrite").parquet(golden_path)
        pytest.skip("Golden file created; re-run")
    
    expected = spark.read.parquet(golden_path)
    assert_df_equality(actual, expected, ignore_row_order=True)
```

On intentional change: delete golden file, re-run (regenerates), commit new golden.

---

### Property-Based Testing

```python
from hypothesis import given, strategies as st

@given(
    st.lists(st.tuples(st.integers(min_value=0, max_value=1000)), min_size=1, max_size=100)
)
def test_sum_property(spark, data):
    df = spark.createDataFrame([(x,) for (x,) in data], ["x"])
    total = df.agg(F.sum("x")).collect()[0][0]
    assert total == sum(x for (x,) in data)
```

Spark + Hypothesis is slow (each test spins through Spark); use sparingly for complex invariants.

---

### CI Patterns

### GitHub Actions example

```yaml
name: PySpark CI

on: [push, pull_request]

jobs:
  test:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-python@v5
        with:
          python-version: '3.11'
      - uses: actions/setup-java@v4
        with:
          distribution: 'temurin'
          java-version: '11'
      
      - name: Install deps
        run: |
          pip install pyspark==3.5.0 pytest pytest-spark chispa
          pip install -e .
      
      - name: Run tests
        run: pytest tests/ -v
```

### Caching

- Cache pip + Maven JARs between CI runs
- Spark download can be slow — cache `~/.ivy2/`

### Keeping tests fast

- Session-scoped SparkSession (not per-test)
- Small DataFrames (< 100 rows)
- `spark.sql.shuffle.partitions=2`
- Avoid real cloud resources (use Testcontainers / local)

---

### Debugging Tests

### Show DataFrame in test

```python
def test_my_transform(spark):
    df = ...
    result = my_transform(df)
    result.show(truncate=False)   # printed on test failure (if captured by pytest)
    assert_df_equality(result, expected)
```

### Dump schema

```python
result.printSchema()
```

### Save for inspection

```python
result.coalesce(1).write.mode("overwrite").parquet("/tmp/test_output/")
# Open with duckdb / another tool
```

### pytest -s to see output

```bash
pytest -s tests/test_my_transform.py::test_specific
```

### `--pdb` for debugger on failure

```bash
pytest --pdb tests/
```

---

### Common Testing Mistakes

### 1. New SparkSession per test
Each takes ~5-10s to spin up. Use `scope="session"` fixture.

### 2. Default shuffle.partitions = 200 in tests
Each test burns 200 tasks per shuffle. Set to 2.

### 3. Testing against real S3 / cloud resources
Slow + flaky. Use local filesystem (`tmp_path`) or MinIO via Testcontainers.

### 4. Comparing DataFrames with `df == other`
Doesn't do what you think. Use chispa `assert_df_equality`.

### 5. Not cleaning up temp views
Views persist across tests in the same session; collisions cause mysterious failures.

```python
@pytest.fixture(autouse=True)
def cleanup_views(spark):
    yield
    for view in spark.catalog.listTables():
        if view.isTemporary:
            spark.catalog.dropTempView(view.name)
```

### 6. Non-deterministic tests
`rand()`, `monotonically_increasing_id()`, un-ordered `.first()` — set seeds, sort before assertions.

---

### Interview Patterns

> **🎯 Interview Angle — "How do you test PySpark code?"** Factor transformations into pure functions (DataFrame → DataFrame). Use pytest with a session-scoped SparkSession fixture (`local[2]`, shuffle.partitions=2, UI disabled). Create input DataFrames programmatically, run the function, assert on output with chispa. For external systems (DB, Kafka), use Testcontainers. For end-to-end, use dedicated staging environments. Mix unit (fast, many) + integration (fewer, slower).

> **🎯 Interview Angle — "Testing data correctness vs code correctness."** Code correctness: unit + integration tests prove transformations work on expected inputs. Data correctness: dbt tests, Great Expectations, Monte Carlo, data diff — prove the DATA itself is correct in production. Both are needed. See the sibling `31-testing-data-pipelines.md` for data correctness patterns.

> **🔥 Production Scenario — The Session Fixture Save.** CI took 1.5 hours for 200 tests, each spinning up Spark. Switched to session-scoped SparkSession + `spark.sql.shuffle.partitions=2`. 15 minutes total. Lesson: one Spark per test session is non-negotiable.

---

### Pattern Summary

| Need | Technique |
|------|-----------|
| Session Spark | `@pytest.fixture(scope="session")` with `local[2]` |
| DataFrame equality | chispa `assert_df_equality(ignore_row_order=True)` |
| Fast shuffles | `spark.sql.shuffle.partitions=2` |
| External systems | Testcontainers (Postgres, Kafka, MinIO) |
| UDF testing | call `.func` directly (bypass Spark) |
| Streaming | `rate` source or file source + `processAllAvailable()` |
| Snapshot | compare current output to saved golden |
| Property-based | Hypothesis (sparingly) |
| CI | GitHub Actions / etc.; cache deps |

---

## 14. Debugging & Operations


### The Spark UI

Default: `http://driver-host:4040` in client mode. In cluster mode: Spark History Server.

### Tabs

| Tab | What it shows |
|-----|---------------|
| **Jobs** | One entry per action; duration, status, # stages |
| **Stages** | Per-stage task metrics; shuffle read/write, duration distribution |
| **Storage** | Cached DataFrames + persistence levels |
| **Environment** | All config values at runtime |
| **Executors** | Per-executor CPU, memory, GC, task count |
| **SQL** | Query plan visualization + per-operator timing |
| **Structured Streaming** | Streaming query metrics (when applicable) |

### What to look at first

**Jobs tab:** which job dominates runtime?

**That job → Stages tab:** which stage? Open stage detail.

**Stage detail → Summary Metrics:**
- Task duration (min, 25%, median, 75%, max) — skew signal if max >> median
- Scheduler delay
- Deserialization/execution/getting-result time
- Shuffle read/write size per task
- Spill (memory + disk) — ran out of memory
- Peak execution memory per task

### Reading per-task metrics

```
Task Duration: min: 1s, 25%: 2s, median: 2s, 75%: 3s, max: 180s
                                                       ^^^ SKEW
```

If max >> median → one partition is huge. Fix via AQE skew join, salting, or partition redistribution.

### Shuffle metrics

- **Shuffle read size:** total bytes a task pulled across the network
- **Shuffle write size:** total bytes a task produced for downstream
- **Shuffle spill (memory):** data spilled from memory to disk due to pressure
- **Shuffle spill (disk):** disk size of spilled data

Spill → memory tight → increase partitions or executor memory.

---

### Spark History Server

For completed apps (cluster mode doesn't keep UI running after job finishes). Spark History Server reads event logs.

### Enable event logs

```python
spark.conf.set("spark.eventLog.enabled", "true")
spark.conf.set("spark.eventLog.dir", "s3://bucket/spark-logs/")
```

### Start History Server

```bash
export SPARK_HISTORY_OPTS="-Dspark.history.fs.logDirectory=s3://bucket/spark-logs/"
./sbin/start-history-server.sh
# → access at http://host:18080
```

### Databricks / managed Spark

Already has History Server built in; access from the Jobs UI.

---

### Debugging Specific Problems

### Problem: One task very slow (skew)

**Signals:**
- Stage summary: max task time 10x+ median
- One task's shuffle read size much > others
- Spill (memory + disk) on that one task

**Fixes:**
1. Enable AQE skew join: `spark.sql.adaptive.skewJoin.enabled=true` (should be default)
2. Verify in SQL plan: look for `SkewedShuffleRead` or similar
3. If AQE insufficient: manually salt the skewed key (see topic 04)
4. For known hot keys: isolate + handle separately

### Problem: Executor OOM

**Signals in logs:**
```
Container killed by YARN for exceeding memory limits.
10.5 GB of 10 GB physical memory used.
Consider boosting spark.yarn.executor.memoryOverhead.
```

OR:
```
java.lang.OutOfMemoryError: Java heap space
```

**Fixes:**
1. **Increase `spark.executor.memoryOverhead`** (10% → 20-30% for Python-heavy)
2. **Reduce partition size:** increase `spark.sql.shuffle.partitions`
3. **Reduce cache:** `df.unpersist()` or drop caching
4. **Reduce broadcast size:** disable auto or lower threshold
5. **Address skew:** one bloated partition causes OOM

### Problem: Driver OOM

**Signals:**
- `java.lang.OutOfMemoryError` on driver
- Driver silently killed; cluster hangs
- Spark UI not responding

**Causes:**
- `.collect()` / `.toPandas()` on large data
- Large broadcast variable
- Many small tasks (driver tracks metadata for each)
- Over-complex query plan

**Fixes:**
```python
spark.conf.set("spark.driver.memory", "8g")
spark.conf.set("spark.driver.maxResultSize", "4g")  # cap result size (fails loud instead)
```

Avoid `.collect()`; use `.limit().show()` or `.write()`.

### Problem: Shuffle fetch timeout

**Signals:**
```
org.apache.spark.shuffle.FetchFailedException: Failed to connect to host/IP
```

**Causes:**
- Executor died (OOM, lost node)
- Network issue
- Shuffle service down

**Fixes:**
- Investigate dead executor logs
- Enable retries: `spark.shuffle.io.maxRetries=10`
- For K8s: enable shuffle tracking or external shuffle service
- For YARN: ensure external shuffle service is running

### Problem: Pipeline hangs

**Investigate:**
- Spark UI Executors tab: all idle? → driver waiting on something
- `jstack` on driver / executor processes
- Scheduler delay (Jobs tab): tasks sitting unassigned → not enough executors

### Problem: Slow stage with many small tasks

**Signals:**
- 10000+ tasks per stage; each task < 1s
- Scheduler overhead dominates

**Fixes:**
- Enable AQE coalesce: `spark.sql.adaptive.coalescePartitions.enabled=true`
- Set target partition size: `spark.sql.adaptive.advisoryPartitionSizeInBytes=128m`
- Explicitly coalesce after shuffle in SQL

### Problem: Reading many small files is slow

**Signals:**
- Reading stage very slow before any compute
- Thousands of input files

**Fixes:**
- Set `spark.sql.files.maxPartitionBytes=128m` (files grouped per Spark partition)
- Pre-compact: periodically rewrite as larger files
- Delta / Iceberg: run compaction (OPTIMIZE / rewrite_data_files)

### Problem: Serialization errors

```
org.apache.spark.SparkException: Task not serializable
```

**Cause:** closure captures non-serializable object (DB connection, logger, etc.)

**Fix:** define non-serializable objects inside the function or use broadcast:
```python
# BAD
conn = get_db_connection()
df.foreach(lambda row: conn.write(row))

# GOOD
def process_partition(iterator):
    conn = get_db_connection()
    for row in iterator:
        conn.write(row)
    conn.close()

df.foreachPartition(process_partition)
```

---

### Logging

### Driver + executor logs

- **YARN:** `yarn logs -applicationId app_...`
- **K8s:** `kubectl logs pod-name -n namespace` + prior container logs with `--previous`
- **Databricks:** cluster logs tab + driver logs
- **EMR:** CloudWatch logs + S3 logs

### Spark-specific log config

```python
# Change log level
spark.sparkContext.setLogLevel("WARN")   # or INFO, DEBUG, ERROR

# log4j.properties for finer control
```

### Logging within UDFs / mapPartitions

```python
import logging

logger = logging.getLogger("my_pipeline")

def process_partition(iterator):
    logger.info("Starting partition")   # appears in EXECUTOR logs
    for row in iterator:
        try:
            ...
        except Exception as e:
            logger.exception(f"Row {row} failed")
            yield None
```

### Accumulators for metrics (RDD-level)

```python
counter = spark.sparkContext.accumulator(0)

def process(row):
    if row.is_valid:
        counter.add(1)

df.foreach(process)
print(f"Valid rows: {counter.value}")
```

Accumulators work on executors; the value is visible on the driver after action completes.

**Caveat:** accumulators may be double-counted if a stage retries. For important metrics, use proper output rather than accumulators.

---

### Common Error Diagnosis

### `java.lang.OutOfMemoryError: Java heap space`

Executor out of heap. Increase `spark.executor.memory` or reduce partition size.

### `Container killed by YARN for exceeding memory limits`

Off-heap overhead too small. Increase `spark.executor.memoryOverhead`.

### `java.lang.StackOverflowError`

Chained operations too deep (e.g., 100+ `withColumn` calls). Fuse into fewer operations.

### `org.apache.spark.SparkException: Task failed while writing rows`

Executor died mid-write. Usually a precursor error (OOM, lost node); check preceding log lines.

### `WARN TaskSetManager: Lost task 0.0 in stage N.0 (TID X): FetchFailed`

Shuffle fetch failed. Executor died or shuffle service unavailable. Usually recovers after retries.

### `AnalysisException: cannot resolve column 'xyz'`

Typo or column renamed earlier. Check `df.columns` or `df.printSchema()`.

### `AnalysisException: Reference 'xyz' is ambiguous`

Column name exists in both sides of a join. Disambiguate: `df1.join(df2, "key")` collapses the key; otherwise `df1.col`, `df2.col`.

### `AnalysisException: cannot broadcast the table that is larger than ...`

Broadcast threshold or memory exceeded. Lower threshold, bump driver memory, or force sort-merge.

### `PicklingError` / `cannot serialize`

Python object in a UDF closure can't be pickled. Move it inside the function or use broadcast for shared state.

---

### Shuffle Diagnostics Deep Dive

### Shuffle service unhealthy

- YARN: check NodeManager logs
- K8s: `kubectl get pods -n spark` → look for shuffle service DaemonSet
- Metrics: `BlockManager` fetch latency

### Shuffle data location

- Local disk (`SPARK_LOCAL_DIRS`) — point this to fast SSD when possible
- K8s: emptyDir or local SSD volume per executor pod

### Shuffle merge (3.2+ push-based shuffle)

Reduces shuffle fetch overhead by pre-merging on shuffle service:
```
spark.shuffle.push.enabled = true
spark.shuffle.push.minShuffleSizeToWait = 500m
```

Experimental on OSS; mature on Databricks.

---

### Query Plan Diagnosis

### Verify optimizations applied

```python
df.explain(mode="formatted")
```

Look for:
- `PushedFilters: [IsNotNull(status), EqualTo(status, completed)]` — predicate pushdown
- `PartitionFilters: [isnotnull(country), (country = US)]` — partition pruning
- `ReadSchema: struct<...>` — column pruning (only needed cols)
- `BroadcastHashJoin` vs `SortMergeJoin`
- `AdaptiveSparkPlan isFinalPlan=false` — AQE active

### If predicate NOT pushed down

- Function on column may block: `WHERE YEAR(ts) = 2024` vs `WHERE ts >= '2024-01-01'`
- UDF filter: always runs post-scan
- Non-pushable predicate type

### If broadcast NOT chosen

- Table size unknown (no stats, external file source)
- Table too large (above threshold)
- Explicit `.hint("merge")` suppresses

### If AQE NOT active

```python
spark.conf.get("spark.sql.adaptive.enabled")   # should be "true"
```

Verify in SQL plan: `AdaptiveSparkPlan` node.

---

### Databricks-Specific Debugging

### Query Profile

Open a query in Databricks SQL / notebook; click "Query profile." Interactive timeline + stage drill-down.

### Spark UI access

Clusters tab → cluster → Spark UI (while running).

### Driver / executor logs

Clusters tab → Logs → Driver / Executor. Live tail.

### Metrics

Clusters tab → Metrics → CPU, memory, network, disk per node.

### Photon-specific issues

- Falls back to JVM for unsupported ops (visible in plan: `! BroadcastHashJoinExec` prefix with `!` means Photon disabled for that op)
- Photon-eligible ops: most SQL + DataFrame; weak on UDFs

---

### Observability for Production

### Spark Metrics System

Spark emits metrics (CPU, memory, task counts, shuffle, etc.) via a pluggable sink:
- `GraphiteSink`
- `PrometheusServlet` (3.0+)
- `JmxSink`
- `GangliaSink`

### Prometheus setup (3.0+)

```
spark.metrics.conf.*.sink.prometheusServlet.class = org.apache.spark.metrics.sink.PrometheusServlet
spark.metrics.conf.*.sink.prometheusServlet.path = /metrics/prometheus
spark.ui.prometheus.enabled = true
```

Scrape driver `/metrics/prometheus` endpoint. Dashboards via Grafana.

### Key metrics to track

- Executor count (dynamic alloc)
- Memory used per executor
- GC time per executor
- Task duration distribution per stage
- Shuffle read/write per stage
- Spill bytes
- Streaming lag (`numInputRows`, `batchDuration`)

### Alerts

- Executor OOM (via log pattern or dead executor count)
- Streaming lag growing
- Batch duration exceeding trigger interval
- Job runtime 2x baseline

---

### Operational Runbooks

### Runbook: Slow job

1. Check if source data volume changed (upstream regression?)
2. Spark UI → which stage is slow?
3. Within stage: skew? OOM? GC? Shuffle size?
4. Compare config vs previous successful run
5. Apply targeted fix (see diagnosis table in topic 10)

### Runbook: Failed job

1. Scheduler reports failure (Airflow, Databricks Jobs)
2. Get error from driver log
3. Classify: data issue (bad schema), code issue (bug), infra (OOM, dead node)
4. For data issues: alert data producer; apply dead-letter
5. For code issues: hotfix + redeploy
6. For infra: re-run; if persists, raise cluster size or address memory

### Runbook: Streaming lag

1. Confirm lag via consumer lag metric (Kafka) or `inputRowsPerSecond` vs `processedRowsPerSecond`
2. Scale up: more executors, more cores, more partitions
3. Check for upstream backfill
4. Check state size growth (missing watermark?)
5. If necessary: skip ahead by advancing offset (data loss acceptable?)

---

### Interview Patterns

> **🎯 Interview Angle — "Walk me through debugging a slow Spark job."**
> 1. Open Spark UI → Jobs tab; identify slow job
> 2. Stages tab; identify slow stage
> 3. In the stage: task duration distribution (max vs median = skew), shuffle read/write, spill, GC
> 4. SQL tab: verify join algorithm, pruning, broadcast
> 5. Executors tab: CPU, memory, GC per executor
> 6. Form hypothesis (skew / memory / join / shuffle size); change ONE thing; measure
> 
> Rank root causes by frequency: skew > bad join algorithm > bad partition count > memory pressure > UDF overhead > small files.

> **🎯 Interview Angle — "How do you know AQE is working?"** Check `spark.sql.adaptive.enabled`. In `explain()`, look for `AdaptiveSparkPlan` wrapper. In Spark UI SQL tab, you'll see the final (post-AQE) plan + markers for coalesced partitions, promoted broadcasts, skew splits. For skew: `SkewedShuffleRead` or `SkewJoin` operators appear.

> **🔥 Production Scenario — The Mystery Hang.** A job appeared stuck at 95% completion for an hour. Investigation: 199 tasks completed in seconds; one task running 60+ min. Classic skew (null-as-zero in join key). AQE skew split was disabled (older runtime). Enabled it, re-ran: finished in 2 minutes. Lesson: enable AQE first; investigate skew second.

---

### Pattern Summary

| Problem | First step |
|---------|-----------|
| Slow job | Spark UI → Stages tab → find dominant stage |
| Skew | Task duration distribution; AQE skew join first; salt if needed |
| Executor OOM | Increase `memoryOverhead` (Python) or `executor.memory`; reduce partition size |
| Driver OOM | Avoid `.collect()`; increase `driver.memory`; reduce broadcast |
| Shuffle fetch error | Investigate dead executor; enable retries; shuffle tracking on K8s |
| Many small tasks | AQE coalesce; `advisoryPartitionSizeInBytes` |
| Many small files on read | `spark.sql.files.maxPartitionBytes`; compaction |
| Task not serializable | Move non-serializable into function; use broadcast for shared state |
| Query plan surprises | `explain(mode="formatted")`; verify PushedFilters, PartitionFilters, join type |
| Streaming lag | Scale executors; check state growth; verify watermark |

---

## 15. PySpark Interview Patterns


### Interview Format

PySpark interviews usually include:
1. **Conceptual questions** — architecture, execution model, join algorithms, AQE
2. **Debugging scenarios** — "my job is slow, how do you investigate"
3. **Code questions** — write PySpark to solve a problem
4. **System design** — "design an ETL for X data at Y scale"
5. **Performance tuning** — given a slow job + Spark UI screenshot, fix it

For principal/staff: expect scenario-based + architectural depth, not syntax quizzes.

---

### Conceptual Questions

### Q: What's the difference between a DataFrame and an RDD?

**Answer:**
- **RDD:** low-level, type-erased (in PySpark, just Python objects), no schema, no optimizer. Each operation runs opaquely.
- **DataFrame:** high-level, schema-aware, operations expressed declaratively. Catalyst optimizes (predicate pushdown, column pruning, join reordering). Tungsten executes (whole-stage codegen, off-heap, vectorized ops).

In modern PySpark, use DataFrames unless you need: custom partitioner, `mapPartitions` with Python state, or you're maintaining legacy RDD code.

### Q: Explain lazy evaluation.

**Answer:**
Transformations (`filter`, `select`, `groupBy`) build a logical plan without executing. Actions (`show`, `count`, `write`, `collect`) trigger execution. This lets Catalyst analyze and optimize the full plan before running:
- Push filters down to the data source
- Prune unused columns
- Fuse multiple operations (whole-stage codegen)
- Reorder joins

Multiple actions re-execute the plan unless you `.cache()`.

### Q: Narrow vs wide transformation?

**Answer:**
- **Narrow:** output partition depends on one input partition. No shuffle. Pipelined within a stage. Examples: `filter`, `map`, `withColumn`, `select`.
- **Wide:** output partition depends on multiple inputs. Requires shuffle. Introduces a stage boundary. Examples: `groupBy`, `join`, `distinct`, `orderBy`, `repartition`.

Shuffle is the dominant cost in big Spark jobs; reducing wide transformations is often the biggest performance win.

### Q: Explain the three join algorithms.

**Answer:**
1. **BroadcastHashJoin (BHJ):** small side replicated to every executor; large side's partitions probe locally. No shuffle of the large side. Default threshold ~10 MB; raise if memory allows. Fastest when applicable.
2. **SortMergeJoin (SMJ):** both sides shuffled by key, sorted, then merged. Default for large × large. Scales to any size.
3. **ShuffleHashJoin (SHJ):** both shuffled; build hash on smaller side. Faster than SMJ when smaller side fits per partition.

AQE can promote planned SMJ → BHJ at runtime based on actual shuffle statistics.

### Q: What is AQE and what does it do?

**Answer:**
Adaptive Query Execution (3.0+, default-on in 3.2+). Re-plans the query at runtime based on actual data sizes:
1. **Coalesce small partitions after shuffle** — merges tiny partitions into reasonable sizes
2. **Promote SMJ → BHJ** — if runtime stats show one side is small enough
3. **Skew join handling** — detects partitions >5x median or >256MB, splits them, replicates the small side

Enable: `spark.sql.adaptive.enabled=true`. Verify: `AdaptiveSparkPlan` wrapper in `explain()`.

### Q: Repartition vs coalesce?

**Answer:**
- **repartition:** full shuffle; can increase or decrease; results in balanced partitions
- **coalesce:** no shuffle (narrow); combines existing partitions on same executor; can only DECREASE; result may be uneven

Use coalesce before writing to reduce file count without shuffle. Use repartition when rebalancing or increasing parallelism.

> **⚠️ Trap:** `coalesce(1)` on big data collapses all data to one executor → OOM. Use repartition if you must shuffle.

### Q: Cache vs checkpoint?

**Answer:**
- **cache:** store DataFrame in memory + spill to disk for reuse within same job. Lineage preserved. Fast reuse.
- **checkpoint:** write to reliable storage (S3/HDFS). TRUNCATES lineage. Fault-tolerant across sessions. Slower but safer.

Use cache for reuse; checkpoint for long iterative lineage (ML, graph). For both: `df.cache().checkpoint()`.

### Q: How do you handle data skew?

**Answer:**
1. **AQE skew join** (first choice): detects skewed partitions and splits them, replicates the small side.
2. **Salting** (manual): add random salt to large side, replicate small side across all salts, join on (key, salt). Trade-off: small side blows up by N_salts.
3. **Isolate hot keys**: separate the known-skewed subset, handle differently (broadcast the hot subset).
4. **Fix upstream**: null-as-zero or default values often cause skew; address at source.

### Q: Why avoid Python UDFs?

**Answer:**
Scalar Python UDFs serialize each row to Python and back — 10-100x slower than native. Catalyst can't optimize them (black box). Alternatives in order of preference:
1. Native functions in `pyspark.sql.functions`
2. Higher-order functions (`F.transform`, `F.filter`, `F.aggregate`, etc.)
3. Pandas UDFs with Arrow (vectorized, 10-100x faster than scalar UDFs)
4. Scalar Python UDFs (last resort)

---

### Coding Problems

### Problem 1: Top-N per group

Given `(user_id, event_time, event_type)`, return the 3 most recent events per user.

**Solution:**
```python
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number, col

w = Window.partitionBy("user_id").orderBy(col("event_time").desc())

result = (df
    .withColumn("rn", row_number().over(w))
    .filter(col("rn") <= 3)
    .drop("rn"))
```

**Variants asked:**
- Latest event per user → `rn == 1`
- Top 3 unique → use `dense_rank` instead
- With ties → `rank <= 3`

### Problem 2: Running total

Given `(user_id, event_date, amount)`, compute running sum per user over time.

**Solution:**
```python
w = (Window.partitionBy("user_id")
     .orderBy("event_date")
     .rowsBetween(Window.unboundedPreceding, Window.currentRow))

result = df.withColumn("running_total", F.sum("amount").over(w))
```

### Problem 3: Dedupe keeping latest

Given rows with duplicates on `(user_id)`, keep the row with the latest `updated_at`.

**Solution:**
```python
w = Window.partitionBy("user_id").orderBy(col("updated_at").desc())
result = (df
    .withColumn("rn", row_number().over(w))
    .filter(col("rn") == 1)
    .drop("rn"))
```

### Problem 4: Pivot

Monthly sales by product (long form). Pivot into one row per product, columns per month.

**Solution:**
```python
result = (df
    .groupBy("product")
    .pivot("month", ["Jan", "Feb", "Mar", "Apr", "May", "Jun"])  # explicit values = faster
    .sum("sales"))
```

### Problem 5: Sessionization

Define a user session as consecutive events within 30 minutes. Assign a session_id per user.

**Solution:**
```python
w = Window.partitionBy("user_id").orderBy("event_time")

sessioned = (df
    .withColumn("prev_time", F.lag("event_time").over(w))
    .withColumn("is_new_session", 
                F.when(
                    (col("prev_time").isNull()) | 
                    (F.unix_timestamp("event_time") - F.unix_timestamp("prev_time") > 1800), 
                    1
                ).otherwise(0))
    .withColumn("session_id", F.sum("is_new_session").over(
        Window.partitionBy("user_id").orderBy("event_time").rowsBetween(Window.unboundedPreceding, Window.currentRow)
    )))
```

### Problem 6: Day-1 retention

Fraction of users whose FIRST login date has an activity the next day.

**Solution:**
```python
first_login = df.groupBy("user_id").agg(F.min("event_date").alias("install_dt"))

retained = (first_login
    .join(df, (first_login.user_id == df.user_id) & 
              (df.event_date == F.date_add(first_login.install_dt, 1)), "left")
    .withColumn("retained_1d", F.when(df.user_id.isNotNull(), 1).otherwise(0))
    .groupBy("install_dt")
    .agg(F.count("*").alias("installs"),
         F.sum("retained_1d").alias("retained"))
    .withColumn("retention_rate", col("retained") / col("installs")))
```

### Problem 7: Gaps-and-islands

Given a sequence of dates per user, find contiguous date ranges (no gap days).

**Solution:**
```python
w = Window.partitionBy("user_id").orderBy("date")

ranges = (df
    .withColumn("rn", row_number().over(w))
    .withColumn("grp", F.date_sub(col("date"), col("rn")))
    .groupBy("user_id", "grp")
    .agg(F.min("date").alias("start_date"),
         F.max("date").alias("end_date"),
         F.count("*").alias("streak_length"))
    .drop("grp"))
```

Trick: `date - row_number()` is constant within a consecutive run.

### Problem 8: Compare current row to previous

For each (user, event), what was the previous event? Flag transitions.

**Solution:**
```python
w = Window.partitionBy("user_id").orderBy("event_time")

transitions = (df
    .withColumn("prev_event", F.lag("event_type").over(w))
    .withColumn("is_transition", 
                F.when(col("prev_event").isNull() | (col("prev_event") != col("event_type")), 1)
                 .otherwise(0)))
```

### Problem 9: Join with slowly-changing dimension

Given fact table with `event_time` and SCD2 dimension with `(effective_from, effective_to)`, enrich facts with dim as of event_time.

**Solution:**
```python
result = (fact.join(
    dim,
    (fact.user_id == dim.user_id) &
    (fact.event_time >= dim.effective_from) &
    (fact.event_time < dim.effective_to),
    "left"
))
```

Range join; may be slow at scale. For performance, pre-bucket by time + use broadcast if dim small.

### Problem 10: Deduplicate within time window

Keep one event per user per 5-minute bucket.

**Solution:**
```python
result = (df
    .withColumn("bucket", F.window(col("event_time"), "5 minutes"))
    .withColumn("rn", row_number().over(
        Window.partitionBy("user_id", "bucket").orderBy("event_time")
    ))
    .filter(col("rn") == 1)
    .drop("rn", "bucket"))
```

### Problem 11: Explode and count

Given a column of arrays, count total occurrences of each element.

**Solution:**
```python
# df: (user_id, tags: array<string>)
result = (df
    .select(F.explode("tags").alias("tag"))
    .groupBy("tag").count()
    .orderBy(col("count").desc()))
```

### Problem 12: Compute median

Median of amount per group.

**Solution:**
```python
# Approximate (fast):
median = df.groupBy("group").agg(F.percentile_approx("amount", 0.5).alias("median"))

# Exact (slow, large groups):
median = df.groupBy("group").agg(F.percentile("amount", 0.5).alias("median"))  # 3.1+

# Manual via window:
w = Window.partitionBy("group").orderBy("amount")
counts = Window.partitionBy("group")

median = (df
    .withColumn("rn", row_number().over(w))
    .withColumn("cnt", F.count("*").over(counts))
    .filter((col("rn") == F.floor((col("cnt") + 1) / 2)) | 
            (col("rn") == F.floor((col("cnt") + 2) / 2)))
    .groupBy("group").agg(F.avg("amount").alias("median")))
```

### Problem 13: Find users with activity in both Jan and Feb

**Solution:**
```python
result = (df.filter(col("event_date").between("2024-01-01", "2024-02-29"))
    .groupBy("user_id")
    .agg(
        F.sum(F.when(F.month("event_date") == 1, 1).otherwise(0)).alias("jan_events"),
        F.sum(F.when(F.month("event_date") == 2, 1).otherwise(0)).alias("feb_events")
    )
    .filter((col("jan_events") >= 1) & (col("feb_events") >= 1))
    .select("user_id"))
```

Equivalent to SQL `HAVING SUM(CASE ...) >= 1`.

### Problem 14: Fuzzy join (join approximately)

Given events table and sessions table (both with timestamps), join each event to the session active at its timestamp.

**Solution:**
```python
# If sessions is small: broadcast + range join
from pyspark.sql.functions import broadcast

result = events.join(
    broadcast(sessions),
    (events.user_id == sessions.user_id) &
    (events.event_time >= sessions.start_time) &
    (events.event_time < sessions.end_time),
    "left"
)
```

For large sessions: pre-bucket by time (e.g., day) + use discretized join.

### Problem 15: CDC MERGE pattern

Given a stream of CDC events (`op` ∈ {I, U, D}), apply to a Delta table.

**Solution:**
```python
from delta.tables import DeltaTable

target = DeltaTable.forPath(spark, "s3://silver/users/")

def apply_cdc(batch_df, batch_id):
    (target.alias("t").merge(batch_df.alias("s"), "t.id = s.id")
        .whenMatchedUpdateAll(condition="s.op != 'D'")
        .whenMatchedDelete(condition="s.op = 'D'")
        .whenNotMatchedInsertAll(condition="s.op != 'D'")
        .execute())

(cdc_stream.writeStream
    .foreachBatch(apply_cdc)
    .option("checkpointLocation", "s3://ck/users/")
    .start())
```

Classic streaming-CDC pattern.

---

### System Design Scenarios

### Scenario 1: Nightly ETL for 50 TB click events

**Requirements:** 50 TB raw clicks/day → clean + enrich + aggregate → warehouse by 8am.

**Design:**
- **Storage:** raw clicks land in Delta Lake (Bronze) partitioned by date
- **Compute:** Databricks jobs cluster, autoscaling 20-100 workers, i3.4xlarge (memory + local SSD for shuffle)
- **Pipeline:**
  - Stage 1 (Silver): read Bronze day partition → parse → dedupe → join enrichment dims (broadcast small, SMJ large) → write Silver partitioned by date
  - Stage 2 (Gold): aggregate per (country, product_cat, hour) → write Gold
- **Config:** AQE on, shuffle.partitions=1000, memoryOverhead 20%, Kryo
- **Target file size:** 128-512 MB per Parquet
- **Monitoring:** Spark event logs → History Server; alert on batch duration > SLA

### Scenario 2: Real-time user session metrics for a product dashboard

**Requirements:** Kafka events → sub-second updating session metrics → serving layer.

**Design:**
- **Spark Structured Streaming** reads from Kafka; micro-batch 10s trigger
- **Session logic:** watermark 10 min; windowed agg per (user_id, product_id) with session_window
- **Output:** write to RT-OLAP engine (Pinot / ClickHouse) via `foreachBatch` + JDBC OR direct Kafka sink
- **Alternative:** Flink for sub-second latency (Spark micro-batch is seconds); see sibling `14-flink.md`

### Scenario 3: Lakehouse MERGE at scale

**Requirements:** 1B rows/day of CDC into a 500 TB lakehouse. Point-in-time correctness.

**Design:**
- **Streaming CDC into Delta via `foreachBatch` + MERGE**
- Partition by `op_date` so each batch MERGE scopes to recent partitions
- `OPTIMIZE` + `VACUUM` weekly to control small files
- Enable deletion vectors for fast DELETE/UPDATE
- Use Databricks Predictive Optimization or custom OPTIMIZE schedule

### Scenario 4: Machine Learning feature pipeline

**Requirements:** Compute 50 features per user per day from events + profile data. Serve in a feature store.

**Design:**
- Bronze events → Silver aggregates
- Spark job per feature group; output to feature store offline (Delta) + online (Hybrid Table / Redis)
- Features defined point-in-time correct (use event-time, not run-time)
- Scheduled via Airflow / Databricks Workflows
- Testing: unit tests on feature logic; data tests on output

### Scenario 5: Multi-cloud Spark

**Requirements:** Primary runs on AWS; DR failover to GCP.

**Design:**
- Store data in open format (Iceberg) on S3 + replicate to GCS
- Catalog in Polaris (cloud-agnostic REST catalog)
- Spark on EMR Serverless (AWS) + Dataproc Serverless (GCP) with same code
- Terraform-managed job definitions; apply to failover region
- Trade-off: egress cost + replication lag; see sibling `34-multi-region-disaster-recovery.md`

---

### Debugging Scenarios

### Scenario A: Job finished 10x slower than yesterday

**Approach:**
1. Compare Spark UI metrics vs last successful run (dates, row counts, shuffle size)
2. Check: did input data grow unexpectedly? Did a filter stop pruning (type mismatch)?
3. Check: did a dim table grow out of broadcast threshold?
4. Check: did AQE skew split get disabled?
5. Fix root cause; don't just bump cluster size.

### Scenario B: One stage runs 30 min, others seconds

Classic skew. See topic 04. First remedy: AQE skew join. If insufficient, salt. If known hot keys, isolate.

### Scenario C: Driver OOM

Usually `.collect()` / `.toPandas()` / large broadcast. Replace with `.limit().show()` or `.write()`. Lower broadcast threshold. Monitor.

### Scenario D: Executors keep dying

Likely OOM. Logs: "Container killed..." or "OutOfMemoryError." Increase `memoryOverhead` (Python-heavy) or `executor.memory`. Reduce partition size. Reduce cache. Address skew.

### Scenario E: Fetch failures everywhere

Shuffle service issue or dead executor. On K8s: enable shuffle tracking. On YARN: ensure external shuffle service running. Investigate underlying cause (OOM, node loss).

---

### Red-Flag Answers to Avoid

**❌ "Just increase executor memory."** 
Right answer: diagnose first; memory may not be the issue.

**❌ "Use coalesce(1) before writing."**
Right answer: coalesce(1) is dangerous on big data; use repartition or accept more files.

**❌ "I always cache DataFrames."**
Right answer: cache only when reused + enough memory.

**❌ "Spark SQL is faster than DataFrame API."**
Right answer: Catalyst optimizes both the same way; equivalent.

**❌ "UDFs are fine, just use them."**
Right answer: Python UDFs are 10-100x slower than native; use Pandas UDFs or built-ins.

**❌ "AQE will solve everything."**
Right answer: AQE handles many cases but not all — window skew, non-join hot keys, etc. still need manual work.

**❌ "Just use .collect() to see the data."**
Right answer: `.collect()` on big data = driver OOM.

**❌ "Set shuffle partitions to the default 200."**
Right answer: 200 is default; tune based on data size (shuffle data / 128 MB).

---

### Interview Preparation Checklist

Topics every PySpark interview candidate should master:

- [ ] Spark architecture (driver, executors, cluster manager)
- [ ] Lazy evaluation + DAG + stages + tasks
- [ ] Transformations: narrow vs wide
- [ ] Actions: show, collect, write (and `.collect()` pitfall)
- [ ] Catalyst + Tungsten + AQE at a conceptual level
- [ ] Join algorithms: BHJ, SMJ, SHJ; broadcast hint
- [ ] Data skew: diagnosis + remedies (AQE, salt, isolate)
- [ ] Window functions: ROW_NUMBER, RANK, LAG, SUM OVER
- [ ] Partitioning: physical (in-memory) vs Hive-style (on disk) vs bucketing
- [ ] File formats: Parquet > CSV; column + predicate + partition pruning
- [ ] UDFs: avoid scalar Python; prefer Pandas UDFs; prefer native
- [ ] Structured Streaming: triggers, watermarks, output modes, exactly-once
- [ ] Delta Lake / Iceberg integration + MERGE
- [ ] Caching: when + storage levels
- [ ] Spark UI: how to read; what to look for
- [ ] Common configs: shuffle.partitions, executor memory, memoryOverhead, AQE flags
- [ ] Deployment: K8s / YARN / Databricks / EMR
- [ ] Testing: pytest + chispa; session-scoped fixture; shuffle.partitions=2

---

### Pattern Summary

| Interview topic | Core answer |
|-----------------|-------------|
| Architecture | Driver + executors + cluster manager; Catalyst + Tungsten + AQE |
| Lazy evaluation | Transformations build plan; actions execute; cache for reuse |
| Join algos | BHJ (small), SMJ (large), SHJ (medium); AQE promotes |
| Skew | AQE skew join → salt → isolate hot keys |
| UDFs | Native > Pandas UDF > scalar UDF |
| Window top-N | ROW_NUMBER partition order filter rn <= N |
| Running total | SUM OVER partition order rowsBetween unboundedPreceding currentRow |
| Sessionization | gap detection + running SUM of new-session flag |
| CDC apply | foreachBatch + Delta MERGE |
| Debug slow job | Spark UI stages → task distribution → root cause |
| Driver OOM | Avoid collect; broadcast size; driver.memory |
| Streaming exactly-once | source replay + idempotent sink + reliable checkpoint |

---

### Final Advice

1. **Think in partitions.** Every operation acts on partitions. Shuffle = redistribute partitions. Skew = unbalanced partitions.
2. **Measure, don't guess.** Spark UI tells you where time goes. Change ONE thing, measure.
3. **Native over UDF.** Catalyst can only optimize what it can see.
4. **Match the tool.** Spark for 100+ GB with heavy shuffle. For smaller: DuckDB, Polars, pandas. For streaming: Flink for ms-latency; Spark for 1-10 s.
5. **Modern patterns:** AQE on; Delta/Iceberg; Pandas UDFs; Spark Connect; K8s + Operator.
6. **Operational excellence:** monitoring, testing, cost awareness, DR.

Good luck. The pattern-recognition-over-memorization rule applies here too.

---

## License

MIT.

---

*End of consolidated PySpark reference. ~250 KB in one file.*
