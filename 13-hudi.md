# Topic 13 — Apache Hudi

> Standalone deep-dive · The streaming-first lakehouse format

[← Back to main](./README.md)

---

## How to use this document

This is the architect-level reference for Apache Hudi: how its upsert-first design works, when it beats Iceberg and Delta, how to operate it in production. Every chapter follows the four lenses: **Concept · Mechanics · Trade-offs · Interview**.

> **🧠 Principal Mindset:** Hudi is the format most senior engineers under-rate and most staff engineers correctly rate. It's not trying to be Iceberg; it's optimizing a different axis. If your workload is high-velocity CDC with mutable records needing low-latency reads, Hudi often beats both alternatives. If you dismiss it as "the third format", you're missing the use case where it genuinely wins.

## Table of Contents

**PART I — FOUNDATIONS**
- 1. What Hudi Is and Why It's Different
- 2. The History: Uber → Apache
- 3. The Design Priorities
- 4. Hudi vs Delta vs Iceberg

**PART II — CORE ARCHITECTURE**
- 5. Table Types: CoW vs MoR
- 6. File Groups and File Slices
- 7. The Timeline
- 8. Record Keys and Partitioning

**PART III — INDEXES (THE HUDI DIFFERENTIATOR)**
- 9. Why Hudi Has Indexes
- 10. Bloom Index
- 11. Simple Index
- 12. HBase Index
- 13. Record-Level Index (0.14+)
- 14. Multi-Modal Indexes (1.0+)

**PART IV — WRITE OPERATIONS**
- 15. Insert, Upsert, Bulk Insert
- 16. Delete and Soft Delete
- 17. Clustering
- 18. The Writing Process Deep Dive

**PART V — READING**
- 19. Snapshot Queries
- 20. Read-Optimized Queries
- 21. Incremental Queries
- 22. Time Travel

**PART VI — STREAMING AND INGESTION**
- 23. HudiStreamer (DeltaStreamer)
- 24. Flink + Hudi
- 25. Spark Streaming + Hudi

**PART VII — OPERATIONS**
- 26. Compaction (MoR Specific)
- 27. Cleaning and Archival
- 28. Hudi 1.0 Highlights

**PART VIII — INTERVIEW PRACTICE**
- 29. The Staff-Level Hudi Interview
- 30. War Stories
- 31. Cheat Sheet & Mental Models

---

## PART I — FOUNDATIONS

### 1. What Hudi Is and Why It's Different

#### 1.1 The One-Sentence Definition

Apache Hudi is an **upsert-first, streaming-native lakehouse format** that was designed from day one around the problem of applying high-velocity CDC changes to large datasets stored as Parquet in object storage, with low-latency reads.

#### 1.2 Where Hudi Diverges from Iceberg/Delta

```
ICEBERG and DELTA were designed as:
   "Make data lakes act like warehouses: ACID, schema evolution, time travel."

HUDI was designed as:
   "Make data lakes handle UPSERTS at TB/day scale with sub-minute reads."

The upsert-first focus produces a different design:
   • Record-level primary keys are first-class
   • Multiple index types (Bloom, HBase, record-level, metadata)
   • Two table types (CoW for read-latency, MoR for write-latency)
   • Native streaming ingest tool (DeltaStreamer / HudiStreamer)
   • Timeline-based commit log (actions, not just snapshots)
```

#### 1.3 Who Uses Hudi

```
Hudi's reference customers:
   • Uber (its origin; multi-PB production)
   • Walmart
   • GE Aviation
   • Robinhood
   • TikTok / ByteDance
   • Alibaba
   • Hundreds of mid-size companies with CDC-heavy pipelines

Common thread: high-velocity CDC into analytical stores.
```

#### 1.4 The Big Numbers

```
Uber's Hudi deployment (2024):
   • Multiple petabytes
   • Billions of records ingested per day
   • Thousands of tables
   • Sub-minute end-to-end latency from source change to query

That workload shape — PB-scale CDC with low latency — is what Hudi is for.
```

> **🎯 Interview Angle:** Q: Why would you choose Hudi over Iceberg?
> **Staff:** "Specific workloads. Hudi wins when: (1) you have high-velocity upserts (CDC from OLTP, event correction streams); (2) you need record-level indexing (point lookups at scale); (3) you want low-latency reads over mutable data (Merge-on-Read tables); (4) you're already using DeltaStreamer / HudiStreamer for ingestion. Iceberg's design is optimized for append-heavy analytical workloads with occasional updates. Hudi's design is optimized for upsert-heavy transactional-feeding-analytical workloads. If your use case is heavy upserts, Hudi is often a better engineering fit than Iceberg + MERGE. Most teams don't have that workload shape, which is why Iceberg's broader applicability makes it the default; teams with heavy upserts should seriously evaluate Hudi."

### 2. The History: Uber → Apache

#### 2.1 Origins at Uber (2016-2017)

```
2016: Uber has a problem. Trip events are mutable (fare adjustments,
      corrections, cancellations). CDC from Postgres databases needs
      to land in HDFS/Parquet for analytics. Doing this with Hive
      requires full partition rewrites.
      
      Engineers (Vinoth Chandar and team) design "Hoodie" (later Hudi):
      Hadoop Upserts, Deletes, and Incrementals.

2017: Hudi open-sourced under Apache 2.0.
      First production use at Uber.
```

#### 2.2 Apache Incubation and Growth (2018-2021)

```
2019: Apache incubator.
2020: Apache top-level project.
2020: Hudi 0.6+ adds Merge-on-Read tables, Flink support.
2021: Spark 3.0 Hudi connector; Delta Streamer renamed to Hudi Streamer.
```

#### 2.3 Maturation (2022-2024)

```
2022: Hudi 0.11: Multi-modal index, better Flink integration.
2023: Hudi 0.13: Record-level index.
2024: Hudi 0.14: Metadata-based record index.
      Multi-modal indexes mature.
```

#### 2.4 Hudi 1.0 (2025)

```
2025: Hudi 1.0 — major release.
   • Table format v8 (new metadata improvements)
   • Non-blocking concurrency control
   • Secondary indexes
   • Spark 3.5 / Flink 1.17+ support
   • Better Iceberg interop
```

#### 2.5 Current Positioning (2026)

```
By 2026:
   • Hudi has a stable, mature niche for CDC-heavy workloads
   • Iceberg dominates general-purpose lakehouse
   • Delta dominates Databricks-centric lakehouse
   • Hudi dominates streaming-upsert lakehouse
   • Convergence: all three formats support reading each other via various bridges
```

### 3. The Design Priorities

#### 3.1 Priority 1: Low-Latency Upserts

```
Problem Hudi solves:
   Traditional lakehouse MERGE on a 10 TB table takes hours.
   For streaming CDC applying 10,000 upserts/sec, this is useless.

Hudi's answer:
   • File groups (bucket data by primary key)
   • Index locates which file group holds each key (fast lookup)
   • Merge-on-Read (MoR) tables: write deltas, merge on read

Result: upserts in seconds, not hours.
```

#### 3.2 Priority 2: Record-Level Operations

```
Hudi natively tracks RECORD KEYS, not just file-level identity.

   • Every record has a primary key
   • Indexes map (key → file group)
   • Upserts find the right file group via index
   • Deletes target specific records
   • Partial updates (change only certain columns)

This is unlike Iceberg/Delta where upserts are table-scan-and-rewrite.
```

#### 3.3 Priority 3: Streaming-First

```
Hudi was designed with STREAMING ingestion as the primary use case.

   • DeltaStreamer / HudiStreamer: continuous ingestion tool
   • Flink connector: production-ready streaming
   • Incremental queries: downstream consumers read only changes
   • Timeline: supports streaming semantics (action start, inflight, complete)

Compare to Iceberg which was append-first (streaming via Flink added later).
```

#### 3.4 Priority 4: Query-Latency Flexibility

```
Hudi offers TWO table types trading write/read latency differently:

   Copy-on-Write (CoW):
      Upserts rewrite affected files.
      Read latency: low (one Parquet read per file).
      Write latency: higher (file rewrites).
      Best for: read-heavy workloads.

   Merge-on-Read (MoR):
      Upserts append to delta files.
      Snapshot read: merges Parquet + deltas on read.
      Read-optimized view: only reads committed Parquet (faster but staler).
      Best for: write-heavy workloads with near-real-time needs.
```

#### 3.5 Priority 5: Operational Rich API

```
Hudi exposes many operational knobs:
   • Multiple compaction strategies
   • Clustering (sort + rewrite without upsert)
   • Rollback (undo last N commits)
   • Savepoint / restore
   • Multiple cleaner policies
   • Archival (move old commits out of active timeline)

More knobs = more flexibility but more operational complexity.
```

### 4. Hudi vs Delta vs Iceberg

#### 4.1 The Comparison

| Dimension | Hudi | Delta | Iceberg |
|-----------|------|-------|---------|
| **Primary focus** | Upserts, streaming | ACID + Databricks ecosystem | Open multi-engine lakehouse |
| **Table types** | CoW, MoR | One (deletion vectors optional) | One |
| **Record keys** | First-class | Via MERGE | Via MERGE |
| **Indexing** | Multiple (Bloom, HBase, record-level, multi-modal) | Delta stats; z-order; liquid cluster | File-level stats; Puffin |
| **Upsert API** | Native `.upsert()` call | `MERGE` SQL | `MERGE` SQL |
| **Incremental reads** | Native, first-class | CDF (change data feed) | V3 adds snapshot diffs |
| **Streaming ingest** | HudiStreamer, Flink | Spark Structured Streaming | Flink, Spark |
| **Query latency** | Flexible (CoW low, MoR tunable) | Low (single read) | Low (single read) |
| **Catalog** | Hive, custom | Primarily Unity | Pluggable (Polaris, Unity, Hive) |
| **Multi-engine** | Spark, Flink, Trino, Hive | Spark primary; UniForm bridges to Iceberg | Best-in-class |

#### 4.2 When Hudi Wins

```
PICK HUDI WHEN:
   ✓ High-velocity CDC from OLTP databases
   ✓ Record-level upsert patterns (not just batch MERGE)
   ✓ Need sub-minute read latency with mutable data (MoR)
   ✓ Have Uber-style ingestion architecture already
   ✓ Benefit from record-level indexing (point lookup patterns)
   ✓ Streaming + batch unified consumption via incremental queries
```

#### 4.3 When Hudi Doesn't Win

```
RECONSIDER HUDI WHEN:
   ✗ Workload is append-heavy (no upserts)
   ✗ Multi-engine access with Snowflake / BigQuery is critical (Iceberg better)
   ✗ Team is Databricks-centric (Delta better)
   ✗ Team doesn't have ops bandwidth for Hudi's richer tunables
   ✗ Operational complexity is a cost you're not ready to pay
```

#### 4.4 The Honest Take on Format Choice

```
In 2026, the format decision isn't primarily technical — all three are mature.
It's about WORKLOAD FIT:

   APPEND-HEAVY + MULTI-ENGINE  → Iceberg
   DATABRICKS SHOP              → Delta
   UPSERT-HEAVY + STREAMING     → Hudi

For a lakehouse of mixed workloads, pick based on YOUR dominant pattern.
Don't try to be clever with multiple formats — pick one and commit.
```

---

## PART II — CORE ARCHITECTURE

### 5. Table Types: CoW vs MoR

#### 5.1 Copy-on-Write (CoW)

```
Default table type.

On upsert:
   1. Index finds the file group containing the key
   2. Read the latest base file in that group
   3. Merge the update into the records
   4. Write a NEW base file (new file slice)
   5. Commit the new slice

File group contains ONLY Parquet base files.
Reader reads the latest base file; no merging on read.

PROS:
   • Simple read path
   • Low read latency
   • Full Parquet file compatibility

CONS:
   • Write amplification: whole files rewritten
   • Slower writes
   • Not suitable for sub-minute write latency
```

#### 5.2 Merge-on-Read (MoR)

```
Hudi's distinctive feature.

File group contains:
   • Base file (Parquet) — append-only historical data
   • Log files (Avro) — incremental deltas (inserts, updates, deletes)

On upsert:
   1. Index finds the file group
   2. Append update to the log file (no base rewrite!)
   3. Commit

On read (snapshot query):
   1. Read latest base file
   2. Read all log files for that file group
   3. Merge in memory
   4. Return merged view

On read (read-optimized query):
   1. Read latest base file only
   2. Return (ignores log files; may be stale)

Periodically, compaction merges log files into a new base.

PROS:
   • Low write latency (just append to log)
   • High ingestion throughput
   • Near-real-time with read-optimized queries

CONS:
   • Slower snapshot reads (merging on read)
   • Operational complexity (compaction tuning)
   • Memory pressure on readers (merging large logs)
```

#### 5.3 Choosing CoW vs MoR

```
PICK COW WHEN:
   ✓ Reads dominate writes (10:1 or more)
   ✓ Read latency is critical (BI dashboards)
   ✓ Write latency can tolerate seconds-to-minutes
   ✓ Simpler operations preferred

PICK MOR WHEN:
   ✓ Writes are frequent (streaming CDC)
   ✓ Near-real-time ingestion (sub-minute)
   ✓ Read latency can tolerate the merge overhead
   ✓ You can operate periodic compaction

HYBRID: some teams use MoR for recent partitions, CoW for older.
```

### 6. File Groups and File Slices

#### 6.1 The Hierarchy

```
TABLE
├── PARTITION (optional)
│   ├── FILE GROUP (identified by UUID, e.g. "fg-001")
│   │   ├── FILE SLICE (one per commit that affected this group)
│   │   │   ├── BASE FILE (Parquet)
│   │   │   └── LOG FILES (Avro, MoR only)
│   │   └── FILE SLICE (next commit)
│   ├── FILE GROUP ("fg-002")
│   └── ...
└── METADATA TABLE (internal, since 0.11+)
```

#### 6.2 File Group

```
File group = a logical unit of records.
   • Identified by stable fileId (UUID)
   • Associated with a specific partition
   • Grows over time via file slices
   • All records with the same key land in the same file group (via index)

Why?
   • Enables fast upsert: index tells you which file group → read + rewrite only that group
   • Independent of other file groups: parallel operations
```

#### 6.3 File Slice

```
File slice = a specific commit's version of a file group.

File group fg-001 might have slices:
   fg-001_commit1 (base=file.parquet, logs=[])
   fg-001_commit5 (base=file.parquet, logs=[.log1])
   fg-001_commit8 (base=file.parquet, logs=[.log1, .log2])

Each new commit on a file group creates a new slice.
Readers pick the slice corresponding to the queried snapshot.
```

#### 6.4 Why This Design

```
File groups allow:
   • Parallel upserts (each writer touches different groups)
   • Localized updates (don't rewrite whole table)
   • Historical versioning (slices preserve history)
   • Clustering (reorganize groups without affecting others)
```

### 7. The Timeline

#### 7.1 What the Timeline Is

```
The Hudi Timeline = ordered log of ACTIONS on the table.
Stored in .hoodie/ directory.

Actions:
   COMMIT              — ingest completed (CoW)
   DELTA_COMMIT        — delta log appended (MoR)
   COMPACTION          — merging logs into base (MoR)
   CLEANING            — removing old file slices
   ROLLBACK            — reverting a failed commit
   SAVEPOINT           — marking a commit as "don't clean"
   RESTORE             — restoring to a savepoint
   CLUSTERING          — reorganizing files
   REPLACECOMMIT       — bulk file replacement
   INDEXING            — building/updating indexes

Each action has three states:
   REQUESTED  — scheduled, not started
   INFLIGHT   — running (writer may crash here)
   COMPLETED  — done, committed
```

#### 7.2 The .hoodie Directory

```
.hoodie/
├── 20240315100000.commit                      ← completed CoW commit
├── 20240315100100.deltacommit                 ← completed MoR delta commit
├── 20240315100200.compaction.requested        ← requested compaction
├── 20240315100200.compaction.inflight         ← running compaction
├── 20240315100200.compaction                  ← completed compaction
├── 20240315100300.clean                       ← completed cleaning
├── hoodie.properties                          ← table configuration
├── archived/                                   ← archived (old) timeline entries
└── metadata/                                   ← metadata table (indexes etc)
```

#### 7.3 Transition Rules

```
A commit's three-state lifecycle:

1. requested — writer intends to make a change
2. inflight — writer is actively writing files
3. completed — writer finished and committed

If a writer crashes in step 2, the inflight record remains.
Hudi's rollback mechanism:
   • On next write, detect inflight records
   • Decide: rollback (remove partial files) or resume (if idempotent)
```

#### 7.4 Why Timeline > Simple Log

```
The timeline models WORK IN PROGRESS, not just completed state.
This matters for:
   • Streaming semantics (long-running writes)
   • Compaction (scheduled, inflight, complete — can span minutes)
   • Clustering (large reorg; need to track progress)
   • Failure recovery (clean up partial work)
```

### 8. Record Keys and Partitioning

#### 8.1 Record Keys Are First-Class

```
Every Hudi record has:
   • Record key (primary key)
   • Partition path (for partitioning)
   • Preserve-key field (ordering value for conflict resolution)

Example:
   Record key: "user_42_order_12345"
   Partition:  "date=2024-03-15"
   Pre-combine: "updated_at = 1710501780"
```

#### 8.2 Primary Keys

```
Hudi requires a primary key for upsert operations.

Simple key:
   hoodie.datasource.write.recordkey.field = "user_id"

Complex (composite) key:
   hoodie.datasource.write.recordkey.field = "user_id,order_id"

Hudi uses the key to:
   • Deduplicate within a batch
   • Route upserts to the correct file group (via index)
   • Resolve conflicts (last write wins by pre-combine key)
```

#### 8.3 Pre-Combine Key

```
When two updates arrive for the same key, Hudi needs to decide which wins.

   hoodie.datasource.write.precombine.field = "updated_at"

Rule: the record with the LARGER pre-combine value wins.
   Typically: a timestamp column, event sequence number, or LSN from CDC.

Without a pre-combine, Hudi may pick arbitrarily — order of updates matters.
```

#### 8.4 Partitioning

```
Hudi supports Hive-style partitioning.
   hoodie.datasource.write.partitionpath.field = "date"

Hudi doesn't do hidden partitioning like Iceberg.
Users must specify the partition column in their records.

For streaming CDC into analytics:
   Typical: partition by date (ingestion time)
   OR: partition by event_date
   OR: no partition (small tables)
```

---

## PART III — INDEXES (THE HUDI DIFFERENTIATOR)

### 9. Why Hudi Has Indexes

#### 9.1 The Fundamental Problem

```
Upsert needs to know:
   "For this record key, which file group holds the previous version?"

Without an index, upsert = full table scan:
   • Scan every file
   • Find matching keys
   • Rewrite affected files

With an index: O(1) lookup per key.
   • Index maps key → file group
   • Only the affected file group is touched
   • Massive performance gain
```

#### 9.2 Index Operations

```
During upsert, for each incoming record:
   1. Look up key in index → get file group ID
   2. If found: upsert into that file group
   3. If not found: insert into a new or sparse file group

Index must support:
   • Point lookup (key → fileId)
   • Bulk lookup (batch of keys → fileIds)
   • Index maintenance (new inserts update the index)
```

### 10. Bloom Index

#### 10.1 How It Works

```
Each Parquet base file has a Bloom filter stored in its footer.
Bloom filter indexes the record keys in that file.

On upsert:
   For each incoming record:
      Check Bloom filters of candidate files
      Files whose Bloom says "maybe": read and check actual keys
      Files whose Bloom says "no": skip

Pros:
   ✓ No external store needed
   ✓ Per-file; parallelizable
   ✓ Default; works out of the box

Cons:
   ✗ False positives (Bloom's nature)
   ✗ Slow for very selective upserts (check all files' Blooms)
   ✗ Memory-heavy for large tables
```

#### 10.2 Configuration

```
hoodie.index.type = BLOOM
hoodie.bloom.index.filter.type = DYNAMIC_V0   -- or SIMPLE
hoodie.bloom.index.use.caching = true
```

### 11. Simple Index

#### 11.1 How It Works

```
Full join between incoming records and all file records.
   For each incoming key: lookup against all stored keys.

Pros:
   ✓ Simple
   ✓ No Bloom false positives

Cons:
   ✗ Scales poorly (full table scan effectively)
   ✗ Only sensible for small tables
```

Rarely used in production.

### 12. HBase Index

#### 12.1 How It Works

```
External HBase table stores the key→fileId mapping.

On upsert:
   Hudi does an HBase point lookup for each key.
   Returns fileId instantly.
   Updates HBase as new inserts arrive.

Pros:
   ✓ True O(1) point lookup
   ✓ Scales to very large tables (billions of keys)
   ✓ Fast upserts regardless of table size

Cons:
   ✗ Requires HBase cluster (operational overhead)
   ✗ External dependency
   ✗ Consistency concerns if HBase and Hudi diverge
```

Used at Uber-scale CDC workloads where HBase is already available.

#### 12.2 Configuration

```
hoodie.index.type = HBASE
hoodie.index.hbase.zkquorum = "hbase-zk.example.com:2181"
hoodie.index.hbase.table = "hudi_index_my_table"
```

### 13. Record-Level Index (0.14+)

#### 13.1 The Breakthrough

```
Hudi 0.14 introduced the RECORD-LEVEL INDEX stored inside the metadata table.
   • No external store needed
   • O(1) point lookup
   • Scales to large tables

This essentially combined HBase index's speed with Bloom index's simplicity.

Hudi 1.0+ makes this the recommended default for large tables.
```

#### 13.2 How It Works

```
Inside the metadata table (also Hudi tables):
   record_level_index
      key_hash  →  (fileId, partition_path, slice_id)

Lookup = Bloom-filtered Parquet scan on the metadata index.
Very fast; scales to billions of records.
```

#### 13.3 Configuration

```
hoodie.index.type = RECORD_INDEX
hoodie.metadata.record.index.enable = true
```

### 14. Multi-Modal Indexes (1.0+)

#### 14.1 What Multi-Modal Means

```
Hudi 1.0 introduced MULTI-MODAL INDEXING: multiple indexes work together.

   Primary index: record-level (key → fileId)
   Secondary indexes: on OTHER columns (not just primary key)
      • Column stats (min/max per column per file)
      • Bloom filter (per column, optional)
      • Expression indexes (expression → fileId)
```

#### 14.2 Why Secondary Indexes Matter

```
Primary key lookup is only useful for upserts.
Secondary indexes help READS.

Example:
   SELECT * FROM orders WHERE user_id = 42
   
   Without secondary: scan all files; filter.
   With bloom on user_id: scan only files whose bloom "might" contain 42.
```

#### 14.3 Configuration

```
hoodie.metadata.index.column.stats.enable = true
hoodie.metadata.index.bloom.filter.enable = true

ALTER TABLE orders ADD INDEX idx_user_id (user_id) USING BLOOM;
```

> **🧠 Principal Mindset:** Indexing is Hudi's unique angle. Iceberg has stats and Puffin; Delta has deletion vectors and stats. Only Hudi has rich multiple index types with the record-level index as a native concept. For point-lookup-heavy workloads (CDC upserts, UI-driven queries), this is a real differentiator. The flipside: more operational knobs — index maintenance adds complexity.

---

## PART IV — WRITE OPERATIONS

### 15. Insert, Upsert, Bulk Insert

#### 15.1 Insert

```python
df.write.format("hudi") \
    .option("hoodie.datasource.write.operation", "insert") \
    .option("hoodie.datasource.write.recordkey.field", "id") \
    .option("hoodie.datasource.write.partitionpath.field", "date") \
    .option("hoodie.datasource.write.precombine.field", "updated_at") \
    .option("hoodie.table.name", "orders") \
    .mode("append") \
    .save("s3://bucket/orders")
```

- Just appends records without index lookup.
- Fast; doesn't deduplicate against existing data.
- Use when you know all records are new.

#### 15.2 Upsert

```python
df.write.format("hudi") \
    .option("hoodie.datasource.write.operation", "upsert") \
    .option("hoodie.datasource.write.recordkey.field", "id") \
    .option("hoodie.datasource.write.partitionpath.field", "date") \
    .option("hoodie.datasource.write.precombine.field", "updated_at") \
    .mode("append") \
    .save("s3://bucket/orders")
```

- Deduplicates within the batch (keeps highest pre-combine)
- Does index lookup to find existing records
- Updates existing; inserts new
- Default and most common Hudi operation

#### 15.3 Bulk Insert

```python
df.write.format("hudi") \
    .option("hoodie.datasource.write.operation", "bulk_insert") \
    .option("hoodie.bulkinsert.sort.mode", "GLOBAL_SORT") \
    .mode("overwrite") \
    .save("s3://bucket/orders")
```

- Highest throughput ingest; sorts data; writes big files
- No index lookup, no deduplication
- Use for: initial table loads, large batch backfills
- Downstream reads benefit from sorted layout

#### 15.4 Insert Overwrite

```python
df.write.format("hudi") \
    .option("hoodie.datasource.write.operation", "insert_overwrite") \
    .option("hoodie.datasource.write.partitionpath.field", "date") \
    .mode("append") \
    .save("s3://bucket/orders")
```

- Replaces entire partitions where records exist
- Doesn't touch other partitions
- Equivalent to Delta's dynamic partition overwrite

### 16. Delete and Soft Delete

#### 16.1 Hard Delete

```python
df_with_deletes = spark.read.option(...).load()
    .select("id", "partition_date")

df_with_deletes.write.format("hudi") \
    .option("hoodie.datasource.write.operation", "delete") \
    .option("hoodie.datasource.write.recordkey.field", "id") \
    .option("hoodie.datasource.write.partitionpath.field", "partition_date") \
    .mode("append") \
    .save("s3://bucket/orders")
```

Hard delete: removes records entirely.

#### 16.2 Soft Delete

```python
# Add a tombstone marker (requires appropriate config)
df_tombstones = source_df.withColumn("_hoodie_is_deleted", lit(True))

df_tombstones.write.format("hudi") \
    .option("hoodie.datasource.write.operation", "upsert") \
    .mode("append") \
    .save("s3://bucket/orders")
```

Soft delete: marks records as deleted; readers can filter.

### 17. Clustering

#### 17.1 What Clustering Does

```
Clustering = reorganize file layout for better read performance.
   • Sort records within partitions
   • Compact small files
   • Independent of upsert operations

Not the same as compaction (which merges MoR log files into base).
```

#### 17.2 Scheduling Clustering

```python
# Automatic clustering during writes
.option("hoodie.clustering.inline", "true")
.option("hoodie.clustering.inline.max.commits", "4")  -- cluster every 4 commits
.option("hoodie.clustering.plan.strategy.sort.columns", "user_id,event_ts")

# Async clustering
.option("hoodie.clustering.async.enabled", "true")
```

#### 17.3 Clustering Strategies

```
STRATEGIES:
   • SPARK_SORT (default): sort by specified columns
   • SPARK_RANGE: range-partition by column ranges
   • SPARK_Z_ORDER: Z-order on multiple columns
   • SPARK_HILBERT: Hilbert curve ordering

Z-order and Hilbert work for multi-column pruning similar to Delta/Iceberg.
```

### 18. The Writing Process Deep Dive

#### 18.1 CoW Upsert Flow

```
1. BATCH DEDUP
   Within incoming batch, keep highest pre-combine per key.

2. INDEX LOOKUP
   For each key: find existing file group (if any).
   
3. GROUP BY FILE GROUP
   Partition updates by target file group.

4. FOR EACH FILE GROUP:
   a. Read latest base file
   b. Merge updates (take new value where key matches)
   c. Write new base file (new file slice)

5. COMMIT
   Write .commit file to timeline.
```

#### 18.2 MoR Upsert Flow

```
1. BATCH DEDUP (same as CoW)
2. INDEX LOOKUP (same)
3. GROUP BY FILE GROUP

4. FOR EACH FILE GROUP:
   Append to the file group's log file:
      - Inserts: as INSERT blocks
      - Updates: as UPDATE blocks
      - Deletes: as DELETE blocks
   Log file is Avro; appends are O(1).

5. COMMIT
   Write .deltacommit file to timeline.

6. SCHEDULE COMPACTION (eventually)
   When log files accumulate, trigger compaction to merge into base.
```

#### 18.3 Compaction Flow (MoR Only)

```
1. SCHEDULE
   Writer or scheduler identifies file groups needing compaction.
   Criteria: log file count, log file size, time since last compaction.

2. REQUESTED STATE
   Write .compaction.requested to timeline.

3. INFLIGHT
   For each file group being compacted:
      a. Read base file + all log files
      b. Merge (apply updates, deletes to base)
      c. Write new base file (new file slice)

4. COMPLETED
   Write .compaction completed file.
   New slice becomes current; old slices eligible for cleaning.
```

> **🔥 Production Scenario:** A team ran MoR tables without inline compaction. After 2 months, some file groups had 50+ log files. Snapshot reads on those groups took 5+ seconds (merging 50 files on the fly). Enabling `hoodie.compact.inline = true` with `hoodie.compact.inline.max.delta.commits = 5` triggered compaction after every 5 commits. Read latency dropped to sub-second. Lesson: MoR without regular compaction is a time bomb.

---

## PART V — READING

### 19. Snapshot Queries

#### 19.1 What a Snapshot Query Does

```
Returns the TABLE AS OF THE LATEST COMMIT.

For CoW: just read the latest base files per file group.
For MoR: read base file + merge log files per file group.

Snapshot query is the DEFAULT for Hudi.
```

#### 19.2 Example

```sql
-- Snapshot query (default)
SELECT * FROM orders WHERE date = '2024-03-15';

-- Explicitly specify
SELECT * FROM orders /*+ snapshot */ WHERE date = '2024-03-15';
```

Reader:
```
1. Read Hudi metadata (timeline, file groups)
2. For each matched partition's file groups:
   - Read base file
   - If MoR: also read log files and merge
3. Return rows
```

### 20. Read-Optimized Queries

#### 20.1 What It Does

```
MoR-specific. Reads ONLY the base files; ignores log files.

Pros: fast (just Parquet reads, no merging)
Cons: data is stale (as of last compaction, not last commit)
```

#### 20.2 Example

```sql
-- Hudi SQL syntax
SELECT * FROM orders /*+ READ_OPTIMIZED */ WHERE date = '2024-03-15';

-- Spark
spark.read.format("hudi") \
    .option("hoodie.datasource.query.type", "read_optimized") \
    .load("s3://bucket/orders")
```

#### 20.3 Use Cases

```
Use read-optimized when:
   • Dashboards tolerate some staleness (data is OK up to last compaction)
   • Low-latency reads critical
   • Snapshot query is too slow

Compaction frequency determines staleness:
   • Compact every commit (inline): read-optimized is near-fresh
   • Compact hourly: read-optimized is up to an hour stale
```

### 21. Incremental Queries

#### 21.1 The Hudi Superpower

```
INCREMENTAL QUERY: read only records that CHANGED between two commits.

Native to Hudi — doesn't require CDF-like opt-in or external tooling.

Query:
   "Give me all records inserted/updated between commit T1 and T2."
```

#### 21.2 Example

```python
# Spark incremental read
df = spark.read.format("hudi") \
    .option("hoodie.datasource.query.type", "incremental") \
    .option("hoodie.datasource.read.begin.instanttime", "20240315100000") \
    .option("hoodie.datasource.read.end.instanttime", "20240315110000") \
    .load("s3://bucket/orders")

# Returns only records changed in that hour window
```

#### 21.3 Use Cases

```
Incremental query enables:
   • Downstream silver → gold incrementally
   • CDC-like streaming via periodic polling
   • ML feature store incremental updates
   • Reverse-ETL (sync only changes to CRM)
```

### 22. Time Travel

```sql
-- Query as of a specific commit
SELECT * FROM orders
    TIMESTAMP AS OF '2024-03-15 10:00:00';

-- Or via Spark reader options
spark.read.format("hudi") \
    .option("as.of.instant", "20240315100000") \
    .load(path)
```

Reads the state of the table at the specified point in time.

---

## PART VI — STREAMING AND INGESTION

### 23. HudiStreamer (DeltaStreamer)

#### 23.1 What It Is

```
HudiStreamer (formerly DeltaStreamer) = batteries-included continuous ingestion tool.

Features:
   • Ingest from Kafka, S3, DFS, JDBC, HTTP, Pulsar
   • Transform via SQL (write.operation.sql.transforms)
   • Schema evolution
   • Checkpointing (resumable after failure)
   • Backfill support
   • Multiple delivery modes (once, continuous)

Powers Uber's production ingestion pipelines.
```

#### 23.2 Running HudiStreamer

```bash
spark-submit \
  --class org.apache.hudi.utilities.deltastreamer.HoodieDeltaStreamer \
  hudi-utilities-bundle.jar \
  --props /path/to/config.properties \
  --source-class org.apache.hudi.utilities.sources.AvroKafkaSource \
  --source-ordering-field updated_at \
  --target-base-path s3://bucket/orders \
  --target-table orders \
  --op UPSERT \
  --continuous
```

Properties file:
```
hoodie.datasource.write.recordkey.field=id
hoodie.datasource.write.partitionpath.field=date
hoodie.deltastreamer.source.kafka.topic=orders_cdc
hoodie.deltastreamer.schemaprovider.registry.url=http://schema-registry/schemas
```

#### 23.3 Schema Registry Integration

```
HudiStreamer integrates with Confluent Schema Registry.
Reads Avro schemas from Kafka messages.
Supports schema evolution across versions.
```

#### 23.4 Multiple Table Patterns

```
For 100+ tables from CDC:
   • One HudiStreamer per table (process isolation)
   • OR MultiTableDeltaStreamer: one process handles many tables
     Shared infrastructure; per-table configs
```

### 24. Flink + Hudi

#### 24.1 The Flink Connector

```java
// Flink DataStream → Hudi
tableEnv.executeSql("""
    CREATE TABLE orders_hudi (
        id BIGINT,
        order_date DATE,
        amount DECIMAL(18,2),
        updated_at TIMESTAMP,
        PRIMARY KEY(id) NOT ENFORCED
    ) WITH (
        'connector' = 'hudi',
        'path' = 's3://bucket/orders',
        'table.type' = 'MERGE_ON_READ',
        'write.operation' = 'upsert',
        'write.precombine.field' = 'updated_at',
        'partition.field' = 'order_date'
    )
""");

tableEnv.executeSql("INSERT INTO orders_hudi SELECT * FROM kafka_source");
```

#### 24.2 Checkpointing

```
Flink checkpoints = Hudi commits (roughly).

Default: Flink checkpoint triggers a Hudi commit.
   Frequent checkpoints → many small commits
   Infrequent checkpoints → fewer, larger commits

Tune checkpoint interval to balance latency vs file size.
Typical: 30s - 2 minutes.
```

#### 24.3 Flink-Specific Optimizations

```
Flink has first-class Hudi integrations:
   • Bucket index (co-located with Flink keyBy)
   • Inline clustering (sorted during Flink's bucketing)
   • Async compaction (separate Flink job)
```

### 25. Spark Streaming + Hudi

```python
# Spark Structured Streaming → Hudi
streaming_df = spark.readStream.format("kafka").option(...).load()

query = streaming_df.writeStream \
    .format("hudi") \
    .outputMode("append") \
    .option("hoodie.datasource.write.operation", "upsert") \
    .option("hoodie.datasource.write.recordkey.field", "id") \
    .option("checkpointLocation", "s3://.../_checkpoints/") \
    .option("path", "s3://bucket/orders") \
    .start()
```

Each micro-batch triggers a Hudi commit. Spark's checkpointing ensures resumability.

---

## PART VII — OPERATIONS

### 26. Compaction (MoR Specific)

#### 26.1 Why Compaction Matters

```
MoR writes to log files to maintain write throughput.
Over time, log files accumulate per file group.
Reads must merge more log files → slower.

Compaction merges log files into a new base file.
   Preserves history (snapshots) but creates a fresh base.
```

#### 26.2 Compaction Modes

```
INLINE:
   hoodie.compact.inline = true
   hoodie.compact.inline.max.delta.commits = 5
   
   After every N commits, trigger compaction.
   Blocks the writer during compaction.

ASYNC:
   hoodie.compact.async.enable = true
   
   Separate thread/job runs compaction.
   Writer continues without blocking.

SCHEDULED:
   Separate job triggered by cron / scheduler.
   Hudi has standalone compaction tools.
```

#### 26.3 Compaction Strategies

```
hoodie.compact.strategy = ...

LOG_FILE_SIZE_BASED:
   Compact file groups whose logs exceed a size threshold.

LOG_FILE_NUM_BASED:
   Compact file groups with more than N log files.

NUM_COMMITS:
   Compact every N commits regardless.

UNBOUNDED:
   Compact when inline.max.delta.commits reached.
```

#### 26.4 Tuning

```
Compaction frequency trade-off:
   TOO FREQUENT → compute waste, write amplification
   TOO INFREQUENT → read latency degrades

Rules of thumb:
   • Streaming (per-minute commits): compact every 5-30 commits
   • Batch (per-hour commits): compact every 2-5 commits
   • Monitor: p95 snapshot read latency; log file counts per group
```

### 27. Cleaning and Archival

#### 27.1 Cleaning

```
CLEANING = remove old file slices that are no longer needed for retention.

hoodie.cleaner.policy = KEEP_LATEST_COMMITS
hoodie.cleaner.commits.retained = 24    -- keep last 24 commits worth of versions

Hudi runs cleaner inline or async:
   hoodie.clean.automatic = true
   hoodie.clean.async = true
```

#### 27.2 Archival

```
ARCHIVAL = move completed timeline entries from .hoodie/ to .hoodie/archived/

Prevents the main timeline from growing indefinitely.
Archived commits are compressed into fewer files.

hoodie.keep.min.commits = 30
hoodie.keep.max.commits = 40
```

#### 27.3 Operational Pattern

```
TYPICAL CONFIG:
   • Keep 24h of commits for incremental queries
   • Archive after that
   • Clean file slices older than retention

MONITORING:
   • Timeline file count (should stabilize)
   • Archived directory size
   • Total storage (should grow proportional to actual data)
```

### 28. Hudi 1.0 Highlights

#### 28.1 Table Format v8

```
Hudi 1.0 ships table format v8 with:
   • Improved metadata layout
   • Better concurrency primitives
   • Richer index infrastructure
   • Backward compatibility with v7 readers (mostly)
```

#### 28.2 Non-Blocking Concurrency Control

```
Pre-1.0 Hudi used lock-based concurrency for conflicting writes.
1.0 introduces NBCC: multiple writers can commit concurrently without locks
     for non-overlapping file groups.

Benefit: higher parallelism for independent writers.
```

#### 28.3 Secondary Indexes

```
CREATE INDEX idx_user_id ON orders (user_id);
CREATE INDEX idx_region ON orders (region) USING BLOOM;
```

Rich indexing beyond primary key enables fast reads on arbitrary columns.

#### 28.4 Expression Indexes

```
Index on an expression, not just a column:

CREATE INDEX idx_year ON orders (YEAR(order_date));

Query: WHERE YEAR(order_date) = 2024
   Uses the expression index for pruning.
```

#### 28.5 Partial Updates

```
Update only specific columns without reading the full record:

df_partial.write.format("hudi") \
    .option("hoodie.datasource.write.operation", "upsert") \
    .option("hoodie.datasource.write.payload.class",
            "org.apache.hudi.common.model.PartialUpdateAvroPayload") \
    .mode("append") \
    .save(path)
```

Useful for CDC where only changed columns are available.

---

## PART VIII — INTERVIEW PRACTICE

### 29. The Staff-Level Hudi Interview

#### 29.1 Common Questions

```
1. "Why does Hudi exist alongside Iceberg and Delta?"
2. "Explain CoW vs MoR. When would you pick each?"
3. "What are Hudi's indexes and why does it need them?"
4. "Walk me through an upsert on an MoR table."
5. "Design a CDC pipeline from Postgres to analytics using Hudi."
6. "Hudi vs Iceberg: when does Hudi win?"
7. "Explain the Hudi timeline. Why not just a flat log?"
8. "What's the operational complexity trade-off with Hudi?"
9. "A snapshot query on an MoR table is slow. What do you check?"
10. "Explain Hudi 1.0's non-blocking concurrency control."
```

#### 29.2 Worked Example

```
Q: Design CDC ingestion from Postgres to analytics using Hudi.

STAFF ANSWER:

ARCHITECTURE:
   Postgres (prod) 
     → Debezium → Kafka (ordered CDC stream)
     → HudiStreamer (continuous mode)
     → Hudi MoR table on S3
     → Downstream consumers (Spark, Trino, incremental readers)

CONFIG RATIONALE:
   • MoR because streaming latency matters more than snapshot read speed
   • Record-level index (0.14+) for O(1) upsert routing
   • Pre-combine on Postgres LSN or updated_at
   • Partition by ingestion_date (not business date — simpler for CDC)

HUDI SETTINGS:
   hoodie.table.type = MERGE_ON_READ
   hoodie.datasource.write.recordkey.field = pg_primary_key
   hoodie.datasource.write.precombine.field = __source_ts_ms
   hoodie.datasource.write.partitionpath.field = ingestion_date
   hoodie.index.type = RECORD_INDEX
   hoodie.compact.inline = true
   hoodie.compact.inline.max.delta.commits = 10
   hoodie.cleaner.policy = KEEP_LATEST_COMMITS
   hoodie.cleaner.commits.retained = 48

OPERATIONS:
   HudiStreamer continuous mode:
      - Fetches from Kafka in micro-batches (30s)
      - Applies upserts
      - Each micro-batch = one Hudi delta commit
      - Inline compaction every 10 commits
      - Async cleaning every N minutes
   
   Monitoring:
      - Kafka lag
      - Hudi commit times
      - Log file counts per file group (compaction health)
      - Snapshot read latency on downstream queries

DOWNSTREAM:
   Analysts query via Trino or Spark SQL (snapshot queries by default).
   Pipelines use incremental queries for silver → gold.
   Dashboards may use read-optimized queries if staleness OK.

WHY NOT DELTA/ICEBERG:
   Delta: MERGE on 10 TB table takes hours; Hudi upserts in seconds.
   Iceberg: MERGE similar story; equality deletes OK but slower than Hudi index.
   For high-velocity CDC specifically, Hudi's architecture wins.

TRADE-OFFS ACCEPTED:
   • Smaller multi-engine ecosystem (Hudi has Spark/Flink/Trino; not Snowflake/BQ as mature)
   • More operational knobs (compaction, cleaning, indexes)
   • Steeper learning curve for the team
```

#### 29.3 Senior vs Staff

| Senior Answer | Staff Answer |
|---------------|--------------|
| "Hudi is for upserts." | "Hudi's design optimizes for high-velocity upserts via multiple mechanisms: record-level primary keys, pluggable indexes (Bloom, HBase, record-level, metadata-table), two table types (CoW/MoR) trading write/read latency, native streaming ingest (HudiStreamer, Flink). For CDC at TB/day with sub-minute freshness, Hudi beats Iceberg's MERGE and Delta's MERGE on throughput by 10-100x in production reports. The trade-off: smaller ecosystem, more operational complexity." |
| "MoR writes to log files." | "MoR appends updates to Avro log files per file group, avoiding the base file rewrite that CoW does on every upsert. This cuts write latency from seconds (CoW) to milliseconds (MoR). The cost: snapshot reads must merge base + logs on the fly, which gets slow as logs accumulate. Compaction periodically merges logs into a fresh base. Tuning compaction frequency is a key MoR operational knob — too frequent wastes compute; too infrequent degrades reads. Read-optimized queries read only the base, trading freshness for speed when staleness is acceptable." |
| "Pick Hudi for streaming." | "Pick Hudi when: (1) CDC at TB/day with mutable records (upsert-heavy); (2) record-level primary keys needed; (3) can tolerate smaller multi-engine ecosystem; (4) team can operate the extra complexity. Don't pick Hudi for append-only analytical lakehouses — Iceberg wins there. Don't pick Hudi for Databricks-centric shops — Delta wins. Hudi is a specialized tool; use it where its specialization matters." |

### 30. War Stories

> **🔥 Production Scenario — The MoR Log Explosion.** A team ran MoR tables with 1-minute Flink checkpoints but no inline compaction. After 2 weeks: some file groups had 300+ log files; snapshot reads took 60+ seconds. Fix: enabled inline compaction every 10 commits + nightly async compaction. Read latency dropped to sub-second. Lesson: MoR demands compaction discipline; it's not optional.

> **🔥 Production Scenario — The Bloom False-Positive Storm.** A team used Bloom index on a 50 TB table. Upserts were slow because 100M+ Bloom checks hit many false positives, requiring real key lookups. Switching to RECORD_INDEX (metadata-table-based, Hudi 0.14+): upsert throughput 20x. Lesson: index choice matters hugely at scale; evaluate record-level index for large tables.

> **🔥 Production Scenario — The Pre-Combine Bug.** A team configured pre-combine on `updated_at`. But their CDC source sometimes emitted the same `updated_at` with different values (due to multiple source changes within the millisecond). Hudi silently picked one; downstream data was wrong intermittently. Fix: use `source_lsn` (strictly increasing from Postgres WAL) as pre-combine. Lesson: pre-combine must be strictly monotonic; timestamps alone may not be.

> **🔥 Production Scenario — The Timeline Growth.** A team ran a Hudi table for 2 years without archival. The .hoodie/ directory had 500K+ files. Loading the table (reading timeline) took 10+ minutes. Fix: enabled archival with `hoodie.keep.max.commits = 50`; after archival ran, load time dropped to seconds. Lesson: Hudi's timeline needs active management via archival + cleaning.

> **🔥 Production Scenario — The Snapshot vs Read-Optimized Mix-up.** A dashboard team queried a Hudi MoR table via Trino. By default, Trino was configured to use read-optimized queries. Dashboards showed data up to 1 hour old (last compaction) while business expected real-time. Fix: explicitly configured Trino to use snapshot queries. Lesson: understand which query type your engine uses by default; MoR + read-optimized is NOT real-time.

### 31. Cheat Sheet & Mental Models

#### 31.1 The One-Minute Pitch

```
Apache Hudi = upsert-first, streaming-native lakehouse format.
   • Record keys first-class; indexed for O(1) upsert
   • Two table types: CoW (read-fast) vs MoR (write-fast)
   • Native incremental queries (changed-records only)
   • HudiStreamer / Flink for continuous ingest
   • Timeline-based commit log (actions with lifecycle)
   • Multi-modal indexes (Bloom, HBase, record-level, 1.0+ secondary)

Pick when: CDC at scale, upsert-heavy, streaming freshness matters.
Avoid when: append-only; multi-engine with Snowflake/BQ critical;
            team can't operate complexity.
```

#### 31.2 The Four Lenses — Compressed

```
CONCEPT
   Lakehouse format optimized for upserts and streaming.
   Record-centric, not file-centric.

MECHANICS
   File groups (keyed logical units) + file slices (per-commit versions).
   Timeline tracks actions with requested/inflight/completed states.
   CoW rewrites base files; MoR appends to log files + periodic compaction.
   Indexes route upserts (Bloom, HBase, record-level, multi-modal).

TRADE-OFFS
   Pros: fast upserts, streaming-native, record-level ops, incremental queries.
   Cons: smaller ecosystem, more ops knobs, steeper learning curve.

INTERVIEW
   Know: CoW vs MoR, index types, timeline, compaction tuning, incremental queries.
   Defend: Hudi vs Iceberg/Delta based on workload shape (upsert velocity).
```

#### 31.3 Essential Commands

```python
# Create a table (via writing)
df.write.format("hudi") \
    .option("hoodie.table.name", "orders") \
    .option("hoodie.table.type", "MERGE_ON_READ") \
    .option("hoodie.datasource.write.recordkey.field", "id") \
    .option("hoodie.datasource.write.partitionpath.field", "date") \
    .option("hoodie.datasource.write.precombine.field", "updated_at") \
    .option("hoodie.index.type", "RECORD_INDEX") \
    .option("hoodie.compact.inline", "true") \
    .mode("append") \
    .save("s3://bucket/orders")

# Upsert
df_updates.write.format("hudi") \
    .option("hoodie.datasource.write.operation", "upsert") \
    .mode("append") \
    .save("s3://bucket/orders")

# Snapshot query (default)
spark.read.format("hudi").load("s3://bucket/orders")

# Incremental query
spark.read.format("hudi") \
    .option("hoodie.datasource.query.type", "incremental") \
    .option("hoodie.datasource.read.begin.instanttime", "20240315100000") \
    .load("s3://bucket/orders")

# Read-optimized (MoR, fast but stale)
spark.read.format("hudi") \
    .option("hoodie.datasource.query.type", "read_optimized") \
    .load("s3://bucket/orders")
```

```sql
-- SQL commands (via Hudi SQL extension)
CREATE TABLE orders (id BIGINT, date DATE, ...) USING HUDI
TBLPROPERTIES (
    'type' = 'mor',
    'primaryKey' = 'id',
    'preCombineField' = 'updated_at'
);

MERGE INTO orders tgt USING updates src ON tgt.id = src.id
WHEN MATCHED THEN UPDATE SET ...
WHEN NOT MATCHED THEN INSERT ...;

-- Clustering
CALL run_clustering(table => 'orders');

-- Compaction (MoR)
CALL run_compaction(table => 'orders', op => 'scheduleAndExecute');

-- Clean
CALL run_cleaner(table => 'orders');
```

#### 31.4 Mental Model

```
Think of Hudi as:
   • The "database engine" for data lakes
   • Record-centric, not file-centric
   • Streaming-first, batch-compatible
   • The specialized choice for upsert-heavy workloads

When to reach for it:
   • CDC pipelines at TB/day with mutable records
   • Need O(1) point upserts at scale
   • Streaming + batch unified via incremental queries
   • Have ops bandwidth for its complexity

When to reach past it:
   • Append-only analytical warehouse
   • Databricks-centric (Delta wins)
   • Multi-engine with Snowflake/BQ critical (Iceberg wins)
   • Small team without ops expertise
```

> **🧠 Principal Mindset:** Hudi's existence is a reminder that formats aren't commodities. All three (Hudi, Delta, Iceberg) solve "ACID on object storage" but their optimization targets differ substantially. Iceberg optimizes for openness and multi-engine. Delta optimizes for Databricks ecosystem integration. Hudi optimizes for upserts at CDC scale. Staff engineers who understand this tri-split pick the right format for the workload. The ones who dismiss Hudi as "the third option" miss cases where it genuinely wins by 10x on the metric that matters.

---

[← Back to main](./README.md)
