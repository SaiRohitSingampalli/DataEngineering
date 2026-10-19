# Topic 29 — Modern Query Engines & DataFrames

> Standalone deep-dive · Polars, Daft, Ray Data, Dask, Fugue

[← Back to main](./README.md)

---

## How to use this document

The "post-pandas, pre-Spark" tier matters more than it did five years ago. DuckDB (10) and PySpark (02) are the pillars; this chapter is for everything in between. Polars, Daft, Ray Data, Dask, Fugue. Each solves a different scale/workload point. Staff engineers argue when each wins.

> **🧠 Principal Mindset:** For 90% of data engineering tasks, Spark is overkill and pandas is a toy. The right tool is often one of this chapter's engines. Knowing them signals practical judgment — the opposite of "all problems are Spark problems."

## Table of Contents

**PART I — FOUNDATIONS**
- 1. The Single-Node Renaissance
- 2. The DataFrame Design Space
- 3. The Five Engines in 2026
- 4. Benchmarking Caveats

**PART II — POLARS**
- 5. Polars Architecture
- 6. Lazy Evaluation and Expression Framework
- 7. Streaming Mode
- 8. Polars Cloud

**PART III — DAFT**
- 9. Daft Architecture
- 10. Multimodal Columns
- 11. Daft on Ray

**PART IV — RAY DATA**
- 12. Ray Platform Overview
- 13. Ray Data for ETL
- 14. Ray + ML Pipelines

**PART V — DASK**
- 15. Dask Primitives
- 16. Dask DataFrame and Dask Array
- 17. When Dask Wins

**PART VI — FUGUE**
- 18. Unified API Over Backends
- 19. Pandas-to-Spark Translation
- 20. When Fugue Helps

**PART VII — DECISION FRAMEWORK**
- 21. Scale Thresholds
- 22. Workload Fit
- 23. Ecosystem Fit

**PART VIII — INTERVIEW PRACTICE**
- 24. The Staff-Level Interview Angle
- 25. Worked Example
- 26. War Stories
- 27. Cheat Sheet &amp; Mental Models

---

## PART I — FOUNDATIONS

### 1. The Single-Node Renaissance

```
A LAPTOP IN 2026:
   32-64 GB RAM
   16-24 cores (M3/M4 Max; or Ryzen 9 7950X)
   NVMe SSD with GB/s throughput
   100M+ row DataFrames: trivial in-memory

A SERVER NODE IN 2026:
   256-1024 GB RAM
   64-96 cores (EPYC Genoa, Xeon Emerald Rapids)
   10 TB NVMe
   Billions of rows: fits comfortably

CONSEQUENCE: a lot of work that used to need Spark can run on one machine.
   DuckDB (SQL)
   Polars (DataFrame)
   Daft (DataFrame + multimodal)
   
   Spark starts to pay off at ~100 GB+ actively processed data,
   with heavy shuffle or wide-cluster compute. Below that, 
   single-node often wins on latency AND cost.
```

### 2. The DataFrame Design Space

```
AXES OF VARIATION:
   
   EXECUTION:
      Eager: operate immediately (pandas style)
      Lazy: build plan + optimize + execute on collect (Spark, Polars)
   
   PARALLELISM:
      Single-node multithreaded (Polars default)
      Distributed (Spark, Dask, Ray, Daft)
   
   MEMORY MODEL:
      In-memory (pandas, Polars default)
      Streaming / out-of-core (Polars streaming, Dask, Daft)
      Column-store on disk (DuckDB, Daft on object store)
   
   TYPES:
      Native Python (pandas)
      Arrow-backed (Polars, Daft, pandas 2.x+)
      JVM-backed (Spark)
   
   LANGUAGE:
      Python-first (pandas, Polars, Daft, Dask)
      Scala/Java-first (Spark)
      Both (Spark PySpark)
```

### 3. The Five Engines in 2026

```
   NAME     | LANGUAGE      | SCALE          | STRENGTH
   ---------+---------------+---------------+---------------
   Polars   | Rust (Py/other)| Single-node to streaming
                                              Fastest single-node DataFrame
   Daft     | Rust + Py     | Dist. (Ray)    | Multimodal, lakehouse-friendly
   Ray      | Py + C++      | Distributed    | ML/compute framework; Data is a module
   Dask     | Python        | Distributed    | pandas-like API; scikit-learn integration
   Fugue    | Python wrapper| Any backend    | Portable API; pandas-to-Spark bridge

Also notable:
   Modin: drop-in pandas replacement; dist. via Ray/Dask
   Vaex: out-of-core pandas-like (less active in 2024-2026)
   cuDF (RAPIDS): GPU DataFrame (NVIDIA)
```

### 4. Benchmarking Caveats

```
BENCHMARK rankings depend heavily on:
   • Dataset size + shape (narrow vs wide; skewed vs uniform)
   • Query mix (filter-heavy vs join-heavy vs aggregation-heavy)
   • Hardware (SSD, RAM, cores)
   • File format (Parquet, CSV, Arrow)
   • Benchmark version (engines move fast)

RELIABLE BENCHMARKS:
   TPC-H: standard but synthetic
   ClickBench: real analytical queries
   DuckDB benchmarks: often cited (hosted by DuckDB team but credible)

STAFF SIGNAL: never pick an engine based on a single benchmark.
   Prototype on YOUR workload with YOUR data shape.
```

---

## PART II — POLARS

### 5. Polars Architecture

```
POLARS: DataFrame library in Rust with Python/R/JS/Java bindings.

CORE PROPERTIES:
   • Rust-native, Arrow-based columnar in-memory format
   • Vectorized execution engine
   • Lazy evaluation with query optimization
   • Multi-threaded by default
   • Streaming mode for out-of-core (larger-than-RAM)

STRENGTHS:
   • Fastest single-node DataFrame in most benchmarks
   • Expression framework (declarative; optimizable)
   • Query optimizer (predicate pushdown, projection pushdown, join reordering)
   • Mature + widely adopted (10M+ downloads/month in 2025)
```

### 6. Lazy Evaluation and Expression Framework

```python
import polars as pl

# LAZY: build a plan; nothing executes
lf = (pl.scan_parquet("s3://bucket/orders/*.parquet")
    .filter(pl.col("status") == "completed")
    .filter(pl.col("order_date") >= "2024-01-01")
    .group_by("customer_id")
    .agg([
        pl.col("amount").sum().alias("total_spent"),
        pl.col("order_id").count().alias("order_count"),
        pl.col("product_id").n_unique().alias("unique_products"),
    ])
    .sort("total_spent", descending=True))

# OPTIMIZATION happens: predicate pushdown to Parquet, projection pruning
# Nothing has read / computed yet

df = lf.collect()  # EXECUTES

# INSPECTION
print(lf.explain())
# Shows the optimized plan: predicates pushed to scan, projections pruned
```

EXPRESSIONS: Polars distinguishes columns (expressions) from values.
```python
pl.col("amount").sum()           # expression
df.select(pl.col("amount").sum())  # evaluates it
```

### 7. Streaming Mode

```python
# STREAMING: process data larger than RAM

(pl.scan_parquet("s3://big-bucket/data/*.parquet")  # TB of data
    .filter(pl.col("country") == "US")
    .group_by("state")
    .agg(pl.col("revenue").sum())
    .sink_parquet("s3://output/state_revenue.parquet")  # STREAMING!
)

# sink_* functions trigger streaming execution.
# Memory bounded; disk-spill tolerated.
```

Streaming mode is improving rapidly but has limits:
   Not all operations stream (some joins, window functions require buffering)
   Error messages less helpful than eager mode
   
STAFF SIGNAL: Polars streaming is viable for many ETL workloads that would
   historically require Spark. Benchmark it before dismissing.

### 8. Polars Cloud

```
POLARS CLOUD (2024+ public preview):
   Managed service for distributed Polars execution
   Partition data across cloud workers
   Submit lazy queries; execute at scale

Still early; worth watching. Positions Polars as a Spark alternative
for Python-first data teams.
```

---

## PART III — DAFT

### 9. Daft Architecture

```
DAFT: distributed DataFrame + ML framework; built for lakehouse workloads.
   Rust + Python; built on Ray for distribution.
   
   DESIGN GOALS:
      Multimodal data (text, images, embeddings, video) as first-class columns
      Lazy with query optimization
      Native Delta / Iceberg / Hudi / Parquet readers
      Horizontal scaling via Ray

Positioning:
   Spark replacement for Python-first teams building ML/data pipelines
   Especially: embedding pipelines, image preprocessing, ML feature prep
```

### 10. Multimodal Columns

```python
import daft

df = daft.read_parquet("s3://bucket/images.parquet")

# Image column with URLs → downloaded + decoded as arrays
df = df.with_column("image", df["url"].url.download().image.decode())
df = df.with_column("resized", df["image"].image.resize(224, 224))
df = df.with_column("embedding", df["resized"].apply(embed_model, dtype=daft.DataType.tensor(float32, [768])))

df.write_parquet("s3://output/embeddings.parquet")
```

Daft treats images, embeddings, tensors as first-class columns — a GAP
Spark + Polars have.

### 11. Daft on Ray

```
DAFT distributes execution via Ray cluster.
   Spin up Ray cluster on K8s
   Submit Daft queries from client
   Daft shards partitions across Ray workers
   
WHEN DAFT WINS:
   Embedding pipelines + ML feature prep (multimodal columns)
   Lakehouse-centric reads (Iceberg, Delta native)
   Python-first teams who want distributed without JVM
   Workloads that mix UDFs with structured data

WHEN DAFT LOSES:
   Pure SQL (DuckDB simpler)
   Spark-ecosystem shops (don't rock the boat)
   Established pandas / Polars codebases
```

---

## PART IV — RAY DATA

### 12. Ray Platform Overview

```
RAY: distributed compute framework.
   Not a DataFrame library per se — a GENERAL distributed Python runtime
   Core abstractions: tasks, actors, objects
   
   On top of Ray:
      Ray Data (DataFrame-like for distributed ETL)
      Ray Train (distributed training)
      Ray Tune (hyperparameter search)
      Ray Serve (model serving)
      KubeRay (Ray operator for K8s)

Positioning: unified distributed Python framework.
   Think "Spark for Python-first ML" more than "Spark for data."
```

### 13. Ray Data for ETL

```python
import ray

ds = ray.data.read_parquet("s3://bucket/data/")
ds = ds.filter(lambda row: row["status"] == "completed")
ds = ds.map_batches(preprocess_batch, batch_format="pandas")
ds = ds.add_column("embedding", lambda rows: embed_batch(rows))
ds.write_parquet("s3://output/")

# Executed across Ray cluster
# Streaming by default; memory bounded
```

Ray Data is competitive with Spark for ML-heavy ETL (lots of UDFs,
   especially GPU UDFs for embeddings/inference).
Less convenient than Spark/Polars/Daft for pure SQL / DataFrame work.

### 14. Ray + ML Pipelines

```
RAY'S KILLER FEATURE: unified ETL + training + tuning + serving.
   ray.data processes data
   ray.train runs distributed training
   ray.tune does hyperparameter search
   ray.serve deploys the model

Pattern:
   Load data → Ray Data
   Preprocess + feature engineer → Ray Data map_batches
   Train model → Ray Train (torch/jax)
   Tune → Ray Tune
   Serve → Ray Serve

Each stage inherits Ray's distributed execution.

STAFF SIGNAL: Ray is the dominant framework for distributed ML in 2026.
   For data engineering roles with ML scope, mentioning Ray + KubeRay
   signals current knowledge.
```

---

## PART V — DASK

### 15. Dask Primitives

```
DASK: parallelism + distributed computing in Python.

PRIMITIVES:
   dask.array: parallel NumPy
   dask.dataframe: parallel pandas
   dask.bag: parallel Python iterables
   dask.delayed: lazy computation graphs
   dask.distributed: distributed scheduler

Dask DAGs are "task graphs" — functions chained; executed by scheduler.
Multi-process by default; distributed via workers (K8s, Fargate, YARN, HPC).
```

### 16. Dask DataFrame and Dask Array

```python
import dask.dataframe as dd

# Reads large Parquet dataset partitioned across workers
df = dd.read_parquet("s3://bucket/data/*.parquet")

# Operations are LAZY — build a graph
result = (df[df.status == "completed"]
    .groupby("customer_id")["amount"]
    .sum()
    .compute())  # Triggers execution

# Dask DataFrame approximates pandas API
# Not 1:1 — some pandas operations unavailable; some perform badly
```

Dask Array:
```python
import dask.array as da

# NumPy-like but chunked for parallelism
x = da.random.random((100000, 100000), chunks=(1000, 1000))
y = (x + x.T).mean(axis=0)
result = y.compute()
```

### 17. When Dask Wins

```
DASK WINS:
   ✓ You have pandas / NumPy code; want to scale with minimal changes
   ✓ scikit-learn / scipy compatibility matters (dask-ml, joblib backend)
   ✓ Scientific computing (HPC-adjacent patterns)
   ✓ Python-first teams; small-medium scale (TB range)

DASK LOSES:
   ✗ Polars is faster single-node
   ✗ Spark is faster at huge scale (100TB+)
   ✗ Query optimizer less sophisticated than Polars / Spark
   ✗ API maturity still gaps vs Spark

PATTERN:
   Dask shines for scientific computing + ML workloads with pandas/numpy heritage
   Weaker as a Spark replacement for ETL
```

---

## PART VI — FUGUE

### 18. Unified API Over Backends

```
FUGUE: ONE API; EXECUTE ON ANY BACKEND.

Write pandas-like code → Fugue runs it on Spark, Dask, Ray, DuckDB.

   from fugue import FugueWorkflow
   
   def process(df: pd.DataFrame) -> pd.DataFrame:
       return df.groupby("customer_id")["amount"].sum().reset_index()
   
   with FugueWorkflow(engine="spark") as dag:
       df = dag.load("s3://bucket/data")
       processed = dag.transform(df, process, schema="customer_id:long, amount:double")
       processed.save("s3://output/")

Change engine="spark" to "duckdb" or "dask" or "ray" without changing logic.
```

### 19. Pandas-to-Spark Translation

```
Common Fugue pattern:
   Prototype with pandas (fast dev)
   Run same code on Spark for production (unchanged)

   from fugue_sql import FugueSQLWorkflow

   query = """
       SELECT customer_id, SUM(amount) AS total
       FROM df
       GROUP BY customer_id
   """
   
   with FugueSQLWorkflow(engine="pandas") as dag:
       dag(df=some_df).run()
   
   # Later, same query:
   with FugueSQLWorkflow(engine="spark") as dag:
       dag(df=some_spark_df).run()
```

### 20. When Fugue Helps

```
FUGUE HELPS:
   ✓ You have pandas code that needs to scale
   ✓ You want to prototype local, run distributed
   ✓ Teams split between pandas + Spark; want common patterns
   ✓ Testing: run on pandas in CI; Spark in prod

FUGUE DOESN'T HELP:
   ✗ You're already comfortable with Spark/Polars/Daft
   ✗ You want the engine's native optimizations
   ✗ Edge features of each engine (not all exposed via Fugue)
```

---

## PART VII — DECISION FRAMEWORK

### 21. Scale Thresholds

```
DATA SIZE (rows / GB after compression):
   
   Tiny (<1 GB, <10M rows):              pandas or Polars eager
   Small (1-50 GB, 10M-500M rows):       Polars (fits in RAM; fastest)
   Medium (50-500 GB, 500M-5B rows):     Polars streaming OR DuckDB OR Daft
   Large (500 GB - 10 TB):                Daft (dist) OR Spark OR Dask dist
   Huge (10+ TB):                         Spark OR Ray Data OR distributed warehouse

CAVEATS:
   Narrow data (few columns, many rows) fits in RAM longer than wide
   SSD + swap can push single-node limits further
   Query shape matters: joins blow up size more than filters
```

### 22. Workload Fit

```
WORKLOAD                           BEST FIT
=======================================================
Single-node SQL analysis           DuckDB (see 10)
Single-node DataFrame ETL          Polars
Mid-scale DataFrame ETL            Polars streaming OR Daft
ML feature prep (multimodal)       Daft OR Ray Data
Distributed ETL on lakehouse       Daft OR Spark
Scientific computing (numpy/pandas) Dask
Pandas-to-Spark portability        Fugue
Distributed ML                     Ray (Train + Tune + Serve)
Massive-scale ETL (10TB+)          Spark (JVM maturity still wins)
```

### 23. Ecosystem Fit

```
LANGUAGE:
   Python-first:    Polars, Daft, Ray, Dask (any of these)
   Scala/Java-first: Spark (don't fight ecosystem)

TEAM EXPERIENCE:
   Pandas-heavy: Polars (close API) or Dask (even closer)
   Spark-heavy: Spark unless there's compelling reason
   ML-focused: Daft or Ray Data for distributed; Polars for single-node

PLATFORM:
   Databricks: Spark (first-class); Daft possible (community)
   Snowflake: Snowpark (SQL/Python); less for these engines
   AWS: EMR / Glue (Spark); Ray supported; Polars/Daft on EC2
   GCP: Dataflow (Beam) / Dataproc (Spark); Ray on GKE
   Lakehouse: Daft or Polars natively friendly
```

---

## PART VIII — INTERVIEW PRACTICE

### 24. The Staff-Level Interview Angle

```
COMMON QUESTIONS:
   1. "Why not use Spark for this?"
   2. "Polars vs pandas vs DuckDB."
   3. "When would you use Daft?"
   4. "Design an embedding pipeline for 100M images."
   5. "Scientific computing in Python at scale: what tool?"
   6. "Ray vs Dask: which + why?"
   7. "How do you port pandas to Spark cheaply?"
   8. "Benchmark results you'd trust."
   9. "GPU DataFrames: when + why?"
   10. "The single-node renaissance: implications."
```

### 25. Worked Example

```
Q: Design an embedding pipeline for 100M images (compressed to features).
   Pipeline must:
     - Download images from S3 (URL per row)
     - Resize + preprocess
     - Generate embeddings via a model
     - Write embeddings to a vector DB + Delta Lake
   Budget: moderate; must complete in 24 hours.

STAFF ANSWER:

ENGINE: DAFT ON RAY
   WHY:
     Multimodal columns handle image download + decode + resize natively
     Ray provides distributed GPU scheduling for the embedding model
     Delta Lake write support native

ARCHITECTURE:
   1. KubeRay cluster on GKE/EKS, GPU nodes (A100 or L40S)
   2. Daft reads input Parquet with image URLs
   3. Daft downloads images in parallel (I/O bound; many workers)
   4. Daft resizes + normalizes (CPU)
   5. Daft .apply(embed_model) on GPU workers (bottleneck)
   6. Daft writes to Delta Lake + Databricks Vector Search

TUNING:
   Image download: 500 concurrent workers (I/O bound)
   Preprocessing: CPU scaling via Ray
   Embedding: 20 GPU workers, batched 64 images per batch
   Checkpoint: write intermediate Parquet shards for crash recovery

ALTERNATIVES:
   Spark + Pandas UDF: works but GPU scheduling harder; multimodal awkward
   Ray Data (without Daft): possible but more boilerplate for the dataframe ops
   Pure Python with multiprocessing: doesn't scale to this size

PIPELINE COST:
   Image download: cheap (S3 egress + I/O workers)
   GPU hours: ~500 (100M / 20 GPUs / 10K imgs/sec/GPU)
   Delta Lake writes: cheap
   Total: $1-5K for the single run; recurring if images grow
```

### 26. War Stories

> **🔥 Production Scenario — The Polars Migration Win.** A team ran nightly ETL on Spark on EMR: 10 TB processed, 4-hour runtime, $200/night in EMR. Analyzed the workload: 90% of operations were filter + aggregate on narrow partitions. Migrated the main pipeline to Polars on a single m6i.24xlarge: 90-minute runtime, $5/night. Saved $70K/year. Lesson: not every "big data" workload needs Spark; narrow partitions on fast single machines often win.

> **🔥 Production Scenario — The Dask Overcommit Problem.** A team used Dask DataFrame on 100 workers for a 500 GB dataset. Dask over-committed memory per worker; OOMs cascaded. Fix: tuned worker memory limits + partition sizes; switched to Polars streaming for the specific workload. Fewer workers, less fragile. Lesson: distributed Python Dataframes have ops gotchas; often simpler single-node wins.

> **🔥 Production Scenario — The Daft Multimodal Save.** A team built an image embedding pipeline in PySpark. Downloading images with Pandas UDF ran into executor OOMs; GPU scheduling was manual. Migrated to Daft on Ray: multimodal columns handled download + decode + resize cleanly; GPU scheduling via Ray tasks. Pipeline 3x faster + simpler. Lesson: multimodal data workloads are Daft's native habitat.

> **🔥 Production Scenario — The Fugue Compromise.** A data science team wrote pandas prototypes; eng team ported to PySpark. Rewrites introduced bugs 30% of the time. Adopted Fugue: data science writes Fugue-flavored pandas; eng changes engine="spark" for prod. Rewrites eliminated; bugs dropped. Trade-off: some Spark-native optimizations unavailable. Lesson: Fugue is a bridge, not a destination.

> **🔥 Production Scenario — The Ray + Spark Hybrid.** A team ran feature engineering in Spark but training on Ray (PyTorch). Data had to be shuffled between the two systems. Simplified: all pipelines in Ray (Ray Data + Ray Train). Single system, less data movement, simpler ops. Lesson: for ML-heavy orgs, Ray increasingly displaces Spark; evaluate it seriously.

### 27. Cheat Sheet &amp; Mental Models

#### 27.1 One-Minute Pitch

```
SINGLE-NODE RENAISSANCE: laptops + fat servers = 90% of ETL doesn't need Spark

POLARS: fastest single-node DataFrame; Rust + Arrow; streaming mode
DAFT: distributed Python DataFrame; multimodal columns; lakehouse-native
RAY DATA: part of Ray platform; strongest for distributed ML
DASK: pandas-API distributed; scientific computing + sklearn
FUGUE: write once; run on any engine

DECISION:
   < 50 GB: Polars (fastest single-node)
   50 GB - 1 TB: Polars streaming / DuckDB / Daft
   1 TB+: Daft or Spark; Daft if lakehouse + Python; Spark if JVM / scale
   ML/images/embeddings: Daft or Ray Data
   Scientific/pandas-heavy: Dask

STAFF SIGNAL: argue the TRADE-OFF; don't default to Spark.
```

#### 27.2 Senior vs Staff

| Senior | Staff |
|--------|-------|
| "Use Spark." | "Spark earns its complexity at 1 TB+ with heavy shuffle + wide cluster. For 50 GB with filter-heavy queries, Polars on one node is faster + cheaper. Scale the TOOL to the workload, not the reputation." |
| "Polars replaces pandas." | "Polars replaces pandas for most ETL. For pandas-ecosystem code (sklearn, scipy, specific libs), pandas or Dask remains better. Polars' expression framework is a different mental model — great for new code, awkward for legacy ports." |
| "Dask is distributed pandas." | "Dask has pandas-like API but divergences at scale. For truly pandas-shaped workflows (sklearn, scipy, HPC), Dask shines. For general ETL at scale, Polars/Daft/Spark usually win on performance." |
| "Daft is Spark replacement." | "Daft replaces Spark for Python-first teams building ML + multimodal pipelines on lakehouses. For pure structured ETL at huge scale, Spark's maturity + ecosystem still wins. Daft is strongest where its specific features (multimodal columns, lakehouse-native readers) matter." |

> **🧠 Principal Mindset:** The gap between "pandas on laptop" and "Spark on cluster" is where the modern data engineer lives. Polars, Daft, Dask, and Ray each solve a region of that gap. Staff engineers know when each wins, prototype in the smallest viable engine, and only reach for Spark when the workload genuinely demands it. "Everything is a Spark problem" is a 2017 answer; in 2026 it signals outdated judgment.

---

[← Back to main](./README.md)
