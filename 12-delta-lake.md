# Topic 12 — Delta Lake

> Standalone deep-dive · The open table format that pioneered the lakehouse, now the Databricks-native lakehouse

[← Back to main](./README.md)

---

## How to use this document

This is the architect-level reference for Delta Lake: how its transaction log works, how it compares to Iceberg and Hudi, when to pick it, and how to operate it in production. Every chapter follows the four lenses: **Concept · Mechanics · Trade-offs · Interview**.

> **🧠 Principal Mindset:** Delta Lake invented the modern lakehouse. For a few years it was the undisputed leader. Then Iceberg's open governance pulled multi-engine support ahead. By 2026, the two have largely converged in capability — the real choice is organizational: is your write side Databricks-committed? Delta still wins there. Otherwise, Iceberg's broader ecosystem usually wins. Knowing both is table stakes at staff level; defending your choice is where the signal lies.

## Table of Contents

**PART I — FOUNDATIONS**
- 1. What Delta Lake Is and Why It Mattered
- 2. The History: Databricks → Linux Foundation
- 3. Core Design Principles
- 4. Delta vs Iceberg vs Hudi

**PART II — THE TRANSACTION LOG**
- 5. The _delta_log Directory
- 6. Actions and Protocol Versions
- 7. Checkpoints
- 8. Reader and Writer Protocol Versions

**PART III — ACID & CONCURRENCY**
- 9. Optimistic Concurrency Control
- 10. Isolation Levels
- 11. Conflict Resolution

**PART IV — WRITING & READING**
- 12. The Write Path
- 13. MERGE: Deep Dive
- 14. Deletion Vectors
- 15. The Read Path

**PART V — DATABRICKS-SPECIFIC FEATURES**
- 16. OPTIMIZE and Compaction
- 17. Z-Order Clustering
- 18. Liquid Clustering (The Modern Answer)
- 19. Predictive Optimization

**PART VI — INTEROP**
- 20. Delta UniForm (Reading as Iceberg)
- 21. Delta Sharing
- 22. Multi-Engine Delta (OSS)

**PART VII — OPERATIONS**
- 23. VACUUM and Retention
- 24. Change Data Feed (CDF)
- 25. Deep Clone and Shallow Clone
- 26. Table Properties Reference

**PART VIII — INTERVIEW PRACTICE**
- 27. The Staff-Level Delta Interview
- 28. War Stories
- 29. Cheat Sheet & Mental Models

---

## PART I — FOUNDATIONS

### 1. What Delta Lake Is and Why It Mattered

#### 1.1 The One-Sentence Definition

Delta Lake is an **open-source storage layer** that brings ACID transactions, scalable metadata handling, and unified batch/streaming data processing to data lakes. It's built on Parquet files + a transaction log (JSON files) in object storage, and is the native format of Databricks.

#### 1.2 Why It Mattered

```
Before Delta Lake (2018):
   Data lakes = Parquet files on S3.
   No transactions. No schema enforcement. No ACID.
   Partial writes = corrupted tables.
   Concurrent writes = races + silent corruption.
   Schema changes = re-write everything.
   
   Data warehouses had all these guarantees, but on proprietary storage
   at warehouse pricing.

Delta Lake's breakthrough (2019):
   You can have warehouse-grade ACID on cheap S3 storage.
   The lakehouse architecture was born.

By 2026:
   The lakehouse pattern is universal.
   Delta, Iceberg, Hudi all offer this; the format war is secondary.
```

#### 1.3 The Shape of a Delta Table

```
A Delta table is a directory with:

   s3://bucket/warehouse/db/orders/
   ├── _delta_log/
   │   ├── 00000000000000000000.json       ← version 0
   │   ├── 00000000000000000001.json       ← version 1
   │   ├── 00000000000000000002.json       ← version 2
   │   ├── 00000000000000000010.checkpoint.parquet
   │   ├── 00000000000000000010.json
   │   └── _last_checkpoint
   ├── part-00000-xxxxx-xxxxx-xxxxx.c000.snappy.parquet
   ├── part-00001-xxxxx-xxxxx-xxxxx.c000.snappy.parquet
   └── ...
```

The `_delta_log/` directory is the transaction log — the heart of Delta Lake. The Parquet files are the actual data.

#### 1.4 Key Properties

```
   • ACID transactions (atomic, consistent, isolated, durable)
   • Scalable metadata (via checkpoints, no HMS required)
   • Schema enforcement and evolution
   • Time travel (query any historical version)
   • Unified batch + streaming (same table, both modes)
   • Upserts and deletes (MERGE)
   • Audit history (full change log)
```

> **🎯 Interview Angle:** Q: Why not just use Parquet directly in a data lake?
> **Staff:** "Parquet gives you columnar storage with compression. It doesn't give you atomicity across multiple files — if a job writes 100 Parquet files and crashes after 80, you have a corrupted table. It doesn't give you schema enforcement — a reader can mix incompatible files silently. It doesn't give you snapshot isolation — concurrent readers and writers see partial state. Delta Lake adds a transaction log over Parquet that solves all of these. The lakehouse pattern — Parquet + transaction log — gives you warehouse semantics on lake storage. Iceberg and Hudi do similar things with different metadata designs; Delta pioneered the pattern."

### 2. The History: Databricks → Linux Foundation

#### 2.1 Origins (2017-2019)

```
2017: Databricks engineers struggle with Parquet-on-S3 reliability.
      Teams implement home-grown transaction tracking.
      Databricks starts building Delta Lake internally.

2019 Apr: Delta Lake open-sourced under Apache 2.0.
          First release of the spec and reference implementation.
          Spark connector first-class.

2019: Databricks introduces the term "lakehouse."
      Delta positioned as the lakehouse foundation.
```

#### 2.2 Growth (2020-2022)

```
2020: Delta Lake 0.7+ adds MERGE, time travel GA.
      Databricks's lakehouse marketing picks up.
      Linux Foundation stewardship announced.

2021: Delta Lake 1.0 — stable protocol.
      Delta Sharing announced (open protocol for cross-platform data sharing).
      Early multi-engine support begins (reads from Presto/Trino).

2022: Delta Lake 2.0 — merge-on-read deletes, column mapping (schema evolution).
      Flink Delta connector matures.
```

#### 2.3 Challenged (2023-2024)

```
2023: Iceberg momentum accelerates.
      Snowflake, BigQuery, Microsoft Fabric all adopt Iceberg.
      Databricks introduces UniForm — write Delta, read Iceberg.
      Delta 3.0 — UniForm GA, deletion vectors, Liquid Clustering preview.

2024 Jun: Databricks acquires Tabular ($1B+).
          Databricks commits to Iceberg interop long-term.
          Unity Catalog open-sourced (June 2024).
          
2024: Delta becomes "Databricks's lakehouse format" rather than
      "the lakehouse format".
      Iceberg becomes the multi-engine default.
      UniForm bridges the two.
```

#### 2.4 Current State (2025-2026)

```
2025: Delta 4.0 — improved MERGE performance, better Iceberg interop.
      Databricks's primary writes remain Delta.
      Growing acceptance that Delta + Iceberg coexist (UniForm).
      
2026: Delta is the Databricks-native format.
      Delta 4.x matures.
      For greenfield non-Databricks projects, Iceberg usually wins.
      For Databricks shops, Delta is still the natural choice.
```

### 3. Core Design Principles

#### 3.1 The Transaction Log is the Source of Truth

```
Every change to a Delta table is a new JSON file in _delta_log/:

   00000000000000000000.json  ← version 0 (initial create)
   00000000000000000001.json  ← version 1 (first insert)
   00000000000000000002.json  ← version 2 (an update)
   ...

Each file is an atomic unit:
   • Appears in object storage (put-if-absent)
   • Readers list the log and read all files in order
   • Writers commit by writing a new file with the next version number
```

#### 3.2 Optimistic Concurrency on File Creation

```
Writing version N requires that version N doesn't exist yet.

Two writers both try to write 00000000000000000003.json simultaneously.
Object storage's "put-if-absent" semantics guarantee only one succeeds.
The other must retry: read version 3, re-base its changes, try version 4.
```

#### 3.3 Separation of Metadata from Data

```
Metadata actions in _delta_log/:
   • AddFile (new data file)
   • RemoveFile (data file logically deleted)
   • Metadata (schema, partition spec)
   • Protocol (reader/writer version)
   • CommitInfo (operation details)

Data files are immutable Parquet.
Deleting a row = write a RemoveFile action + new AddFile without that row.
(Or in modern Delta, use deletion vectors.)
```

#### 3.4 Checkpoints for Scalability

```
After every N versions (default 10), Delta writes a CHECKPOINT:
   00000000000000000010.checkpoint.parquet

The checkpoint consolidates all metadata actions up to that version
into one Parquet file. Readers can start from the latest checkpoint
rather than replaying every JSON from version 0.
```

### 4. Delta vs Iceberg vs Hudi

#### 4.1 The Comparison

| Dimension | Delta | Iceberg | Hudi |
|-----------|-------|---------|------|
| **Origin** | Databricks (2019) | Netflix (2017) | Uber (2017) |
| **Governance** | Linux Foundation (Databricks-driven) | Apache (open) | Apache |
| **Metadata model** | Flat transaction log + checkpoints | Hierarchical (snapshot → manifest list → manifests) | Timeline + file groups |
| **Partition handling** | Explicit columns, column mapping | Hidden partitioning (transforms) | Explicit columns |
| **Partition evolution** | Hard (requires rewrite) | Metadata-only | Hard |
| **Deletions** | Deletion vectors (2.4+) | Position/equality deletes (V2), DVs (V3) | Native MoR/CoW |
| **Catalog** | Primarily Unity Catalog | Pluggable (Polaris, Unity, Hive, REST, ...) | Hive, custom |
| **Multi-engine** | Spark/Databricks primary; UniForm bridges to Iceberg | Best-in-class | Spark, Flink, Trino, Hive |
| **Streaming** | Spark Structured Streaming native | Flink/Spark via connectors | DeltaStreamer/HudiStreamer |
| **Specific strengths** | OPTIMIZE, Liquid Clustering, Photon tuning | Open governance, hidden partitioning | Record-level indexing, true upsert |

#### 4.2 When Delta Wins

```
PICK DELTA WHEN:
   ✓ Databricks is your primary (or only) engine
   ✓ You want Databricks-specific optimizations (Liquid Clustering, Photon)
   ✓ Your team already has Delta expertise
   ✓ You use Delta Sharing for cross-org data exchange
   ✓ Simplicity (one transaction log) is a value
```

#### 4.3 When Delta Doesn't Win

```
RECONSIDER DELTA WHEN:
   ✗ Multi-engine access is a hard requirement (Snowflake, BQ as readers)
   ✗ Your writers are mixed (not just Databricks)
   ✗ Open governance / vendor-neutrality is a core principle
   ✗ You need hidden partitioning or partition evolution
```

#### 4.4 The UniForm Era

```
Databricks's answer to Iceberg: UniForm.
   Write Delta; have UniForm produce Iceberg metadata simultaneously.
   Iceberg readers see your Delta data as Iceberg.

This is PRAGMATIC — not a surrender, but an acknowledgment:
   "Databricks shops can keep Delta; non-Databricks readers get Iceberg."

For architectural decisions in 2026:
   • If writers are Databricks-only + readers are mixed → Delta + UniForm
   • If writers are mixed → Iceberg (as common denominator)
   • If writers are Databricks + Databricks reads only → Delta (no UniForm needed)
```

---

## PART II — THE TRANSACTION LOG

### 5. The _delta_log Directory

#### 5.1 Structure

```
_delta_log/
├── 00000000000000000000.json
├── 00000000000000000001.json
├── 00000000000000000002.json
├── ...
├── 00000000000000000010.checkpoint.parquet
├── 00000000000000000010.json
├── 00000000000000000011.json
├── ...
├── 00000000000000000020.checkpoint.parquet
└── _last_checkpoint
```

- JSON files: one per version, 20-digit zero-padded.
- Checkpoint files: compacted Parquet representations.
- `_last_checkpoint`: tiny pointer file.

#### 5.2 Finding the Current State

```
To load a Delta table at the latest version:

1. Read _last_checkpoint (if exists)
   → gives version N of the latest checkpoint
2. Read 000...000N.checkpoint.parquet
   → gives consolidated metadata state at version N
3. List _delta_log/ for files > version N
4. Read each JSON file in order
5. Apply each action: add/remove files, update metadata, bump version

Final state = checkpoint state + all actions after.
```

#### 5.3 Version-Based Access

```sql
-- Latest version
SELECT * FROM delta.`s3://bucket/orders`;

-- Specific version
SELECT * FROM delta.`s3://bucket/orders@v12`;
SELECT * FROM delta.`s3://bucket/orders` VERSION AS OF 12;

-- Timestamp
SELECT * FROM delta.`s3://bucket/orders` TIMESTAMP AS OF '2024-03-15';
```

### 6. Actions and Protocol Versions

#### 6.1 Action Types

Every JSON file in _delta_log/ contains one or more actions:

```json
{
  "metaData": {
    "id": "a1b2c3-...",
    "format": {"provider": "parquet"},
    "schemaString": "{\"type\":\"struct\",\"fields\":[...]}",
    "partitionColumns": ["event_date"],
    "configuration": {},
    "createdTime": 1710501780000
  }
}
{
  "add": {
    "path": "part-00000-xxxxx.snappy.parquet",
    "partitionValues": {"event_date": "2024-03-15"},
    "size": 1048576,
    "modificationTime": 1710501780000,
    "dataChange": true,
    "stats": "{\"numRecords\":1000,\"minValues\":...,\"maxValues\":...}"
  }
}
{
  "remove": {
    "path": "part-00000-yyyyy.snappy.parquet",
    "deletionTimestamp": 1710501780000,
    "dataChange": true
  }
}
{
  "commitInfo": {
    "timestamp": 1710501780000,
    "operation": "WRITE",
    "operationParameters": {...},
    "readVersion": 11,
    "isolationLevel": "WriteSerializable"
  }
}
```

#### 6.2 The Six Core Actions

```
METADATA — Table schema, partition spec, properties
PROTOCOL — Minimum reader/writer versions required
ADD      — New data file added to the table
REMOVE   — Data file logically removed
TXN      — Idempotent streaming transaction markers
COMMITINFO — Metadata about the commit (operation, params, user)
```

#### 6.3 Protocol Versions

```
Delta uses a protocol version pair: (minReaderVersion, minWriterVersion).

A reader must support at least minReaderVersion to open the table.
A writer must support at least minWriterVersion to write.

Versions correspond to features:
   (1, 1): Basic Delta (initial)
   (1, 2): Writes with invariants (CHECK constraints)
   (2, 5): Column mapping (schema evolution)
   (3, 7): Table features (fine-grained feature flags)

TABLE FEATURES (modern approach):
   Each feature can be independently enabled.
   Replaces monolithic version bumps.

   Examples: deletionVectors, columnMapping, timestampNtz,
             appendOnly, invariants, changeDataFeed
```

### 7. Checkpoints

#### 7.1 Why Checkpoints Exist

```
Without checkpoints:
   Reading a table at version 10000 requires reading 10001 JSON files.
   Startup becomes O(N) in number of commits.

With checkpoints:
   Every N commits (default 10), write a consolidated snapshot.
   Readers start from the latest checkpoint, then apply remaining JSONs.
   
   Reading at version 10000 with checkpoints every 10:
      Read checkpoint at version 10000 (one Parquet file)
      Done. No JSON replay needed.
```

#### 7.2 Checkpoint Format

```
A checkpoint is a Parquet file containing all currently-active
AddFile actions plus metadata/protocol state.

Format: one row per action.

Delta writes checkpoints after every N commits (configurable):
   SET delta.checkpoint.writeStatsAsStruct = true
   SET spark.databricks.delta.properties.defaults.checkpointInterval = 10
```

#### 7.3 Checkpoint Operations

```
WRITE_CHECKPOINT:
   1. Materialize all active actions at version N
   2. Write 000...000N.checkpoint.parquet
   3. Update _last_checkpoint pointer

Checkpoints are AUTOMATIC — managed by writers.
Readers DON'T need to know about them explicitly; the Delta library handles.
```

### 8. Reader and Writer Protocol Versions

#### 8.1 The Compatibility Problem

```
As Delta adds features, old readers/writers may not understand them.

Protocol version pair ensures compatibility:
   Table at (reader=3, writer=7) requires:
      Reader library supporting at least version 3 features
      Writer library supporting at least version 7 features

Attempting to read with older client → error, clear message.
```

#### 8.2 Table Features (The Modern Mechanism)

```
Instead of monotonic version bumps, Delta 3.0+ uses TABLE FEATURES.

Each feature is a named flag:
   deletionVectors
   columnMapping
   timestampNtz
   appendOnly
   checkConstraints
   changeDataFeed
   generatedColumns
   identityColumns
   ...

Clients advertise which features they support.
Tables require specific features.
Clients can open tables only if they support ALL required features.

Advantage: adding one feature doesn't force upgrades on unrelated features.
```

#### 8.3 Checking Protocol Version

```sql
DESCRIBE DETAIL delta.`s3://bucket/orders`;

-- Returns: minReaderVersion, minWriterVersion, tableFeatures
```

---

## PART III — ACID & CONCURRENCY

### 9. Optimistic Concurrency Control

#### 9.1 The Model

```
Writers don't lock. They:
   1. Read the current table state (latest version N)
   2. Compute changes based on N
   3. Attempt to commit as version N+1
   4. If object storage says N+1 already exists → retry with new base

Object storage's "put-if-absent" semantics are the primitive.
On S3: originally relied on DynamoDB conditional-put;
       since S3 strong consistency (2020), direct put-if-absent works.
```

#### 9.2 Retry Logic

```
Delta libraries retry automatically with exponential backoff.
Default: up to ~N retries per write (configurable).

If retries exhausted, writer sees ConcurrentModificationException.
Application must decide: retry, fail, or branch.
```

#### 9.3 Fast Paths

```
Not every concurrent write conflicts.

APPEND vs APPEND: No conflict. Both can succeed by retrying with new base.
APPEND vs OVERWRITE on same partition: Conflict.
DELETE vs anything on same partition: Potential conflict.

Delta uses PARTITION-LEVEL conflict detection to reduce false conflicts.
```

### 10. Isolation Levels

#### 10.1 WriteSerializable (Default)

```
Default isolation level.

Writes are serialized globally (one at a time, by version number).
Readers see one consistent snapshot.

Trade-off: writers may conflict more often than strictly necessary.
Benefit: strongest guarantees.
```

#### 10.2 Serializable

```
Strictest isolation level.

Even reads participate in the serialization order (via read-your-writes).
Used mostly for tables where strong consistency is critical.
```

#### 10.3 Snapshot Isolation (Less Strict)

```
Not Delta's default. Available via table properties for some operations.

Writers see a snapshot but concurrent transactions may interleave.
Allows more concurrent throughput at the cost of some anomalies.
```

### 11. Conflict Resolution

#### 11.1 Common Conflict Scenarios

```
SCENARIO 1: Two appends to different partitions
   Writer A: INSERT INTO orders (date='2024-03-15', ...) VALUES ...
   Writer B: INSERT INTO orders (date='2024-03-16', ...) VALUES ...
   → No conflict after retry (both succeed, at adjacent versions)

SCENARIO 2: Two updates to the same partition
   Writer A: UPDATE orders SET status='shipped' WHERE id = 1 (partition 2024-03-15)
   Writer B: DELETE FROM orders WHERE id = 2 (same partition)
   → Potential conflict; one retries with new base, succeeds

SCENARIO 3: Overwrite + append
   Writer A: INSERT OVERWRITE orders PARTITION (date='2024-03-15') ...
   Writer B: INSERT INTO orders (date='2024-03-15', ...) VALUES ...
   → Conflict; Writer B must retry on top of A's overwrite
```

#### 11.2 Conflict Error Handling

```python
from delta.exceptions import ConcurrentModificationException

try:
    # Perform Delta operation
    df.write.format("delta").mode("append").save(path)
except ConcurrentModificationException as e:
    # Retry logic
    time.sleep(backoff)
    # Retry the operation
```

Delta libraries handle most retries internally; explicit handling needed only for pathological patterns.

---

## PART IV — WRITING & READING

### 12. The Write Path

#### 12.1 The Basic Append

```python
df.write.format("delta").mode("append").save("s3://bucket/orders")
```

Steps:
```
1. Writer reads current table state (version N)
2. Writes Parquet data files to table location
3. Prepares AddFile actions for new files
4. Attempts commit:
   a. Writes _delta_log/000...000(N+1).json with AddFile actions
   b. Object storage put-if-absent determines success
5. On conflict, re-reads state (now version N+k), writes new JSON with next version
```

#### 12.2 Overwrite

```python
# Overwrite entire table
df.write.format("delta").mode("overwrite").save(path)

# Dynamic partition overwrite (replace only partitions in source data)
df.write.format("delta") \
    .mode("overwrite") \
    .option("partitionOverwriteMode", "dynamic") \
    .save(path)

# Overwrite with specific partition filter
df.write.format("delta") \
    .mode("overwrite") \
    .option("replaceWhere", "date >= '2024-03-15'") \
    .save(path)
```

#### 12.3 Schema Evolution

```python
# Automatically evolve schema (add new columns)
df.write.format("delta") \
    .option("mergeSchema", "true") \
    .mode("append") \
    .save(path)

# Overwrite with new schema
df.write.format("delta") \
    .option("overwriteSchema", "true") \
    .mode("overwrite") \
    .save(path)
```

#### 12.4 Streaming Writes

```python
# Spark Structured Streaming → Delta
streaming_df.writeStream \
    .format("delta") \
    .outputMode("append") \
    .option("checkpointLocation", "s3://.../_checkpoints/") \
    .start("s3://bucket/orders")
```

Each micro-batch commits as a new Delta version. Checkpointing ensures exactly-once semantics for the sink.

### 13. MERGE: Deep Dive

#### 13.1 The MERGE Statement

```sql
MERGE INTO orders tgt
USING updates src
ON tgt.id = src.id
WHEN MATCHED AND src.deleted = TRUE THEN DELETE
WHEN MATCHED THEN UPDATE SET
    status = src.status,
    updated_at = src.updated_at
WHEN NOT MATCHED THEN INSERT (id, status, created_at)
    VALUES (src.id, src.status, src.updated_at);
```

#### 13.2 How It Works

```
1. SCAN target to find matching rows (based on ON condition)
2. SCAN source
3. JOIN target × source on the ON condition
4. CLASSIFY rows:
   • MATCHED: tgt row + src row (apply WHEN MATCHED clauses)
   • NOT_MATCHED_SRC: src row only (apply WHEN NOT MATCHED clauses)
   • NOT_MATCHED_TGT: tgt row only (unchanged; may still be deleted via other clauses)
5. WRITE updates:
   • Modified rows + unchanged rows from affected files → new files
   • New rows → new files
6. COMMIT: new AddFile actions + RemoveFile for old files
```

#### 13.3 Copy-on-Write vs Merge-on-Read

```sql
-- Set MERGE mode
ALTER TABLE orders SET TBLPROPERTIES (
    'delta.feature.deletionVectors' = 'supported',
    'delta.enableDeletionVectors' = 'true'
);
```

```
COPY-ON-WRITE (default pre-DV):
   Any file with a matched row is REWRITTEN with the modifications applied.
   Reader sees clean files.
   Writer does more work (whole-file rewrites).

MERGE-ON-READ (with deletion vectors):
   Modified rows are written as new files.
   Deletion vectors mark affected rows in old files as deleted.
   Reader applies deletion vectors on scan.
   
   Benefits:
   • 10-100x faster writes for selective MERGE
   • Smaller write amplification
   
   Costs:
   • Reads slightly slower (apply deletion vectors)
   • Eventually need compaction (OPTIMIZE merges deletion vectors + rewrites files)
```

#### 13.4 MERGE Performance Tips

```
1. SKIP SKIPPING WITH FILTERS
   Add predicates to source and target scans:
      MERGE ... USING (SELECT * FROM src WHERE updated_at > '2024-03-01') src
      ... WHEN MATCHED AND tgt.updated_at > '2024-03-01' THEN ...

2. Z-ORDER or Liquid Cluster on the merge key
   Enables file-level pruning on target during MERGE scan.

3. BROADCAST SMALL SOURCE
   If updates are small, broadcast to all workers:
      /*+ BROADCAST(src) */

4. BATCH MERGE OPERATIONS
   Don't MERGE row-by-row; batch into fewer, larger operations.
```

### 14. Deletion Vectors

#### 14.1 What Deletion Vectors Are

```
DELETION VECTOR = a bitmap stored alongside a data file
                  marking which rows in that file are logically deleted.

Format: Roaring bitmap compressed; stored in a .bin file or inline.

For file part-00000.parquet with 10,000 rows:
   deletion vector: 10K bits
   Each bit N = 0 (keep) or 1 (deleted)
```

#### 14.2 Why They Matter

```
Before deletion vectors:
   DELETE WHERE id = 42
   
   Delta had to:
   • Find the file containing id=42
   • Rewrite that entire file without id=42
   • Mark the old file as removed, new file as added
   
   For 10,000 scattered deletes across 100 files:
   → Rewrite 100 files = GB of I/O for KB of logical change

After deletion vectors:
   DELETE WHERE id = 42
   
   Delta:
   • Find the file containing id=42
   • Write a deletion vector marking id=42's position as deleted
   • Commit the deletion vector
   
   For 10,000 scattered deletes:
   → Write ~100 deletion vectors (MB, not GB)
```

#### 14.3 Enabling Deletion Vectors

```sql
-- Table creation
CREATE TABLE orders (...) USING DELTA
TBLPROPERTIES (
    'delta.enableDeletionVectors' = 'true'
);

-- On existing table
ALTER TABLE orders SET TBLPROPERTIES (
    'delta.enableDeletionVectors' = 'true'
);
```

#### 14.4 Reading with Deletion Vectors

```
Reader for each data file:
   1. Open Parquet file
   2. Read deletion vector (if exists)
   3. For each row, check deletion vector bit
   4. Skip deleted rows
   
Overhead: small; usually dominated by I/O.
```

#### 14.5 Compacting Deletion Vectors

```sql
-- OPTIMIZE compacts deletion vectors into actual file rewrites
OPTIMIZE orders;
```

OPTIMIZE periodically "flushes" deletion vectors by rewriting affected files cleanly. Prevents indefinite deletion vector accumulation.

### 15. The Read Path

#### 15.1 Loading a Table

```
Reader at time T:
   1. Read _last_checkpoint (if exists) → version N of checkpoint
   2. Read checkpoint Parquet → set of currently-active files
   3. List _delta_log/ for files > N
   4. Replay JSON files to apply AddFile/RemoveFile actions
   5. Build in-memory snapshot: {active_files, schema, partition_spec, properties}
```

#### 15.2 Query Planning

```
Query: SELECT * FROM orders WHERE event_date = '2024-03-15'

1. Load snapshot (as above)
2. Partition pruning: filter active files by event_date = '2024-03-15'
3. File-level stats pruning: filter files by column min/max
4. Return list of (file, deletion_vector) pairs to execution engine
5. Engine opens each file, applies deletion vector, applies additional predicates
```

#### 15.3 Caching

```
Delta library caches:
   • Latest snapshot per table path
   • Checkpoint Parquet contents
   • Metadata for recently-accessed tables

Spark + Delta cache:
   • Data files on executor local disk (via Spark cache)
   • Metadata in driver memory
```

#### 15.4 Time Travel

```sql
-- Read a specific version
SELECT * FROM orders VERSION AS OF 100;

-- Read as of a timestamp
SELECT * FROM orders TIMESTAMP AS OF '2024-03-15 10:00:00';
```

Internally, Delta replays the transaction log up to the specified version/timestamp.

---

## PART V — DATABRICKS-SPECIFIC FEATURES

### 16. OPTIMIZE and Compaction

#### 16.1 The OPTIMIZE Command

```sql
-- Compact small files into larger ones (target ~1 GB)
OPTIMIZE orders;

-- Compact only specific partitions
OPTIMIZE orders WHERE event_date >= '2024-03-01';

-- Compact with Z-order
OPTIMIZE orders ZORDER BY (user_id, product_id);
```

#### 16.2 What OPTIMIZE Does

```
1. Identify partitions / file groups needing compaction
2. For each group:
   a. Read all data files (apply deletion vectors)
   b. Combine into fewer, larger output files
   c. Write new files
   d. Commit: AddFile for new files, RemoveFile for old files
3. New files have optimized layout + no deletion vectors
```

#### 16.3 Target File Size

```sql
-- Default target file size (varies by table size)
-- Databricks dynamically picks based on table size
-- Typical: 256 MB - 1 GB

ALTER TABLE orders SET TBLPROPERTIES (
    'delta.targetFileSize' = '1073741824'  -- 1 GB
);
```

#### 16.4 Operational Patterns

```
NIGHTLY:
   OPTIMIZE on tables with heavy write activity.

AFTER BATCH:
   OPTIMIZE affected partitions after large batch jobs.

NEVER:
   OPTIMIZE append-only tables with already-large files (no benefit).
   OPTIMIZE while heavy writes are concurrent (conflicts).
```

### 17. Z-Order Clustering

#### 17.1 What Z-Order Does

```
Z-ORDER interleaves bits from multiple columns to achieve
MULTI-COLUMN pruning.

Standard sort: ORDER BY A, B
   → Great pruning on A, poor pruning on B

Z-ORDER (A, B):
   → Good pruning on A AND B (not optimal for either alone)

Z-order is a mathematical trick: it creates a space-filling curve
that keeps co-located values near each other in storage.
```

#### 17.2 When to Z-Order

```
USE Z-ORDER WHEN:
   ✓ Queries filter on multiple columns (not a single column dominantly)
   ✓ 2-4 columns are the primary filter dimensions
   ✓ You want flexible pruning (not committed to one sort order)

DON'T Z-ORDER WHEN:
   ✓ One column dominates queries (regular sort is better)
   ✓ You need Liquid Clustering's flexibility (see next section)
   ✓ Table is small (benefit doesn't justify compaction cost)
```

#### 17.3 Running Z-Order

```sql
OPTIMIZE orders ZORDER BY (user_id, product_id);
```

Note: Z-ORDER is a COMPACTION operation, not a persistent clustering specification. Future writes don't maintain Z-order; need to re-OPTIMIZE periodically.

### 18. Liquid Clustering (The Modern Answer)

#### 18.1 What It Replaces

```
Z-ORDER limitations:
   ✗ Not maintained on write; needs periodic re-OPTIMIZE
   ✗ Column set is per-OPTIMIZE invocation
   ✗ Can't change without full rewrite
   ✗ No concurrent writes during OPTIMIZE

LIQUID CLUSTERING addresses these:
   ✓ Maintained incrementally on writes
   ✓ Column set is a table property
   ✓ Can be changed without rewriting old data
   ✓ Writes don't block clustering (concurrent)
```

#### 18.2 Enabling Liquid Clustering

```sql
-- At table creation
CREATE TABLE orders (
    id BIGINT,
    user_id BIGINT,
    event_ts TIMESTAMP,
    amount DECIMAL
)
USING DELTA
CLUSTER BY (user_id, event_ts);

-- On existing table (metadata-only operation)
ALTER TABLE orders CLUSTER BY (user_id, event_ts);
```

#### 18.3 Clustering Columns

```
Pick 1-4 columns commonly filtered on.
Avoid very high-cardinality columns (like UUID) as sole cluster key.
Order matters slightly, but not as much as Z-ORDER.
```

#### 18.4 Trade-offs

```
LIQUID CLUSTERING benefits:
   ✓ Incremental maintenance
   ✓ Can change cluster columns without rewrite
   ✓ Writes don't block clustering
   ✓ Future-proof vs partition-column choice

LIQUID CLUSTERING costs:
   ✗ Databricks-specific feature (not in open Delta spec, though adopted upstream)
   ✗ Slightly higher write overhead
   ✗ Benefits take time to materialize (need enough data)

For greenfield in 2026 on Databricks: default to Liquid Clustering.
For non-Databricks: regular partitioning + periodic OPTIMIZE ZORDER.
```

### 19. Predictive Optimization

#### 19.1 What It Is

```
PREDICTIVE OPTIMIZATION (Databricks, 2023+):
   Databricks's service runs maintenance operations automatically:
      • OPTIMIZE
      • VACUUM
      • Statistics collection
   
   Based on heuristics / ML about which tables benefit.
```

#### 19.2 Enabling

```sql
-- Enable at the catalog level
ALTER CATALOG my_catalog SET PREDICTIVE_OPTIMIZATION = ENABLE;

-- Or per schema / table
ALTER SCHEMA my_schema SET PREDICTIVE_OPTIMIZATION = ENABLE;
```

#### 19.3 What Gets Managed

```
Databricks evaluates:
   • Table write patterns (bursty vs steady)
   • Query patterns (what's commonly filtered)
   • File fragmentation levels
   • Snapshot accumulation

And runs:
   • OPTIMIZE at appropriate intervals
   • VACUUM to clean up old files
   • Statistics refresh

Cost: predictable (Databricks publishes pricing).
Benefit: less manual maintenance.
```

---

## PART VI — INTEROP

### 20. Delta UniForm (Reading as Iceberg)

#### 20.1 What UniForm Does

```
UniForm = Universal Format.

When you write a Delta table with UniForm enabled, Delta ALSO writes
Iceberg metadata referencing the same Parquet files.

Result: external engines (Snowflake, BigQuery, Trino, DuckDB)
         can read the Iceberg metadata and query your Delta data.
```

#### 20.2 Enabling

```sql
CREATE TABLE orders (...)
USING DELTA
TBLPROPERTIES (
    'delta.universalFormat.enabledFormats' = 'iceberg',
    'delta.columnMapping.mode' = 'name',
    'delta.enableIcebergCompatV2' = 'true'
);
```

#### 20.3 How It Works

```
On every Delta commit:
   1. Delta writes _delta_log entry (normal)
   2. UniForm asynchronously generates:
      • Iceberg metadata.json
      • Iceberg manifest list
      • Iceberg manifest files
      Pointing to the SAME Parquet files
   3. External engines query via Iceberg catalog pointing to the UniForm metadata
```

#### 20.4 Limitations

```
⚠ Asynchronous: Iceberg metadata lags Delta commits (seconds to minutes typical)
⚠ Not all Delta features map perfectly to Iceberg
⚠ Liquid Clustering doesn't fully surface (clusters remain Delta-specific)
⚠ Deletion vectors need Iceberg V3 reader support
```

### 21. Delta Sharing

#### 21.1 The Protocol

```
Delta Sharing is an open protocol for sharing data across platforms.
   Shared datasets are Delta tables.
   Recipients access via Delta Sharing clients (Python, Spark, Java, ...)
   Authentication via bearer tokens or OIDC.
```

#### 21.2 Why Important

```
Problem: sharing data across orgs/clouds historically meant:
   • Export CSV/Parquet files (point-in-time, no updates)
   • Grant warehouse access (vendor lock-in)
   • Build custom APIs (expensive)

Delta Sharing: standardized, open, live.
   • Recipient reads live Delta tables
   • No data copying (reads provider's storage)
   • Credential-vended access (short-lived STS)
   • Works across clouds
```

#### 21.3 Sharing a Table

```sql
-- Provider side
CREATE SHARE sales_data;
ALTER SHARE sales_data ADD TABLE catalog.schema.orders;
GRANT SELECT ON SHARE sales_data TO RECIPIENT acme_corp;
```

```python
# Recipient side
import delta_sharing

client = delta_sharing.SharingClient("~/credentials.share")
df = delta_sharing.load_as_pandas("<share>.<schema>.<table>")
```

### 22. Multi-Engine Delta (OSS)

#### 22.1 Supporting Engines

```
Delta Lake OSS support (reads at minimum):
   • Spark (full read/write; reference impl)
   • Flink (via Delta connector; reads + writes)
   • Trino (reads; writes limited)
   • Presto (reads)
   • DuckDB (reads via delta extension)
   • Hive (reads)
   • Rust library (delta-rs; multi-language bindings)
   • Python API (python-deltalake, built on delta-rs)
```

#### 22.2 delta-rs (The Rust Library)

```
delta-rs is a Rust implementation of the Delta protocol.
   • No JVM required
   • Powers python-deltalake
   • Powers Node.js, Ruby, other bindings
   • Reads most Delta features; writes basic operations
```

```python
from deltalake import DeltaTable, write_deltalake

# Read
dt = DeltaTable("s3://bucket/orders")
df = dt.to_pandas()

# Write
write_deltalake("s3://bucket/orders", new_df, mode="append")
```

#### 22.3 Where OSS Lags Databricks

```
Databricks-only (or lagged OSS):
   ✗ Liquid Clustering (added upstream slowly)
   ✗ Predictive Optimization
   ✗ Photon-specific optimizations
   ✗ Some newer features arrive in Databricks first

OSS Delta covers 90% of production use cases.
Enterprise Databricks workloads use additional features for performance.
```

---

## PART VII — OPERATIONS

### 23. VACUUM and Retention

#### 23.1 What VACUUM Does

```
Over time, Delta accumulates:
   • Removed data files (no longer referenced by current snapshot)
   • Old versions of data files (referenced only by old snapshots)

VACUUM physically deletes these from storage.
```

#### 23.2 Default Retention

```
DEFAULT: 7 days retention.
   VACUUM only deletes files older than 7 days + 1 hour.

RATIONALE: readers taking a snapshot may be reading old files during a slow query.
          7 days gives slack for very long-running jobs.

OVERRIDING: possible but risky
   -- Can be set lower, requires explicit override
   SET spark.databricks.delta.retentionDurationCheck.enabled = false;
   VACUUM orders RETAIN 0 HOURS;  -- IMMEDIATELY delete removed files; dangerous
```

#### 23.3 Running VACUUM

```sql
-- Default retention
VACUUM orders;

-- Specific retention
VACUUM orders RETAIN 168 HOURS;  -- 7 days

-- Dry run (list what would be deleted)
VACUUM orders DRY RUN;
```

#### 23.4 Operational Pattern

```
WEEKLY:
   VACUUM each table that has had writes.

NEVER:
   VACUUM aggressively (< retention) without understanding reader patterns.
   VACUUM a table being actively read by a long-running query.

MONITOR:
   Storage size over time.
   Should grow proportional to actual data, not transaction count.
   If storage grows faster, VACUUM may not be running.
```

### 24. Change Data Feed (CDF)

#### 24.1 What CDF Is

```
Change Data Feed = per-row change log from a Delta table.

For every change (INSERT, UPDATE, DELETE), CDF records:
   • _change_type: insert, update_preimage, update_postimage, delete
   • _commit_version
   • _commit_timestamp
   • Row values at that point

Downstream consumers (Flink, Spark Streaming, ML pipelines) 
can read ONLY what changed, not full tables.
```

#### 24.2 Enabling

```sql
-- At table creation
CREATE TABLE orders (...) USING DELTA
TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true');

-- On existing table
ALTER TABLE orders SET TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true');
```

#### 24.3 Reading CDF

```sql
-- Read changes between two versions
SELECT * FROM table_changes('orders', 10, 20);

-- Read changes since a timestamp
SELECT * FROM table_changes('orders', '2024-03-15 10:00:00');
```

```python
# Spark Structured Streaming on CDF
df = (spark.readStream
    .format("delta")
    .option("readChangeFeed", "true")
    .option("startingVersion", 10)
    .table("orders"))
```

#### 24.4 Use Cases

```
TYPICAL:
   • Downstream silver → gold replication (only process changes)
   • ML feature store updates (recompute features on changed rows)
   • Audit / compliance (what changed when)
   • Reverse-ETL (sync changes to CRM/marketing tools)
   • CDC publishing (emit changes to Kafka)
```

### 25. Deep Clone and Shallow Clone

#### 25.1 Shallow Clone

```sql
-- Create a shallow clone (metadata copy, shares data files)
CREATE TABLE orders_dev SHALLOW CLONE orders;
```

```
Metadata is copied (fast, cheap).
Data files remain in original location; both tables reference them.

Use for: dev/staging environments, zero-cost table forking.
```

#### 25.2 Deep Clone

```sql
-- Create a deep clone (full copy of data + metadata)
CREATE TABLE orders_backup DEEP CLONE orders;
```

```
Both metadata and data files copied.
Independent lifecycle; original can VACUUM without affecting clone.

Use for: backups, cross-region replication, long-term archives.
```

### 26. Table Properties Reference

Common Delta table properties:

```sql
ALTER TABLE orders SET TBLPROPERTIES (
    -- Features
    'delta.feature.deletionVectors'    = 'supported',
    'delta.enableDeletionVectors'      = 'true',
    'delta.enableChangeDataFeed'       = 'true',
    'delta.columnMapping.mode'         = 'name',

    -- UniForm
    'delta.universalFormat.enabledFormats'  = 'iceberg',
    'delta.enableIcebergCompatV2'           = 'true',

    -- File targeting
    'delta.targetFileSize'             = '1073741824',  -- 1 GB
    'delta.tuneFileSizesForRewrites'  = 'true',

    -- Retention
    'delta.logRetentionDuration'       = 'interval 30 days',
    'delta.deletedFileRetentionDuration' = 'interval 7 days',
    'delta.checkpointInterval'         = '10',

    -- Constraints
    'delta.constraints.minOrderAmount' = 'amount >= 0'
);
```

---

## PART VIII — INTERVIEW PRACTICE

### 27. The Staff-Level Delta Interview

#### 27.1 Common Questions

```
1. "How does Delta Lake achieve ACID on object storage?"
2. "Walk me through what happens on a MERGE command."
3. "Explain the Delta transaction log. How are checkpoints involved?"
4. "Delta vs Iceberg — defend your choice for a greenfield project."
5. "What are deletion vectors and when do they matter?"
6. "Explain Liquid Clustering. Why does it beat Z-ORDER?"
7. "A Delta table has 10,000 small files. What's your remediation plan?"
8. "How does UniForm enable Iceberg readers of Delta tables?"
9. "What's the risk of VACUUM with a short retention?"
10. "Design a lakehouse for a Databricks-committed enterprise."
```

#### 27.2 Worked Example

```
Q: Explain exactly-once semantics in a Delta streaming write.

STAFF ANSWER:
   Spark Structured Streaming + Delta provides exactly-once by combining:
   
   1. Checkpointing: Spark records the offset of the last processed source.
      On restart, resumes from that offset.
   
   2. Idempotent writes: Delta's transaction log uses optimistic concurrency.
      If Spark's streaming batch N commits version 42 successfully then crashes
      before marking its checkpoint, on restart it tries to commit batch N again
      — but the Delta commit for version 42 already exists.
   
   3. The TXN action: Delta writes a TXN action with (appId, batchId) in each
      commit. If the same (appId, batchId) is attempted again, Delta detects
      the duplicate and skips the write, preserving exactly-once.
   
   The combination of Spark checkpoints + Delta's TXN actions makes
   exactly-once reliable even across crashes.
```

#### 27.3 Design Problem

```
Q: Design a Delta lakehouse for a 50-engineer analytics team.
   Must support batch ETL, streaming ingest, ad-hoc analytics,
   and ML. Primary engine: Databricks.

STAFF ANSWER:

ARCHITECTURE:
   • All data on Delta Lake (S3)
   • Unity Catalog for governance
   • Medallion architecture: bronze → silver → gold

BRONZE (raw):
   • Streaming ingest from Kafka via Spark Structured Streaming
   • Delta tables with ingestion metadata
   • Retention: 90 days

SILVER (cleaned):
   • Batch dbt-databricks or Spark jobs
   • MERGE for CDC-style updates
   • Deletion vectors enabled
   • Change Data Feed enabled for downstream consumers

GOLD (business):
   • dbt models producing marts
   • Liquid Clustering on common filter columns
   • Shared via Unity Catalog to BI tools

GOVERNANCE:
   • Unity Catalog as single source of truth
   • Row-level security + column masking for PII
   • Delta Sharing for external partners

INTEROP:
   • UniForm enabled on gold layer → readable by Snowflake/BigQuery
   • Enables customer-side analytics without moving data

OPERATIONS:
   • Predictive Optimization enabled (Databricks manages OPTIMIZE/VACUUM)
   • Monitoring: Delta table health metrics via system tables
   • Audit: Unity Catalog audit log

ML:
   • Feature store on Delta tables (offline + online)
   • MLflow for experiments
   • Model serving via Databricks

COST:
   • Databricks compute: $$ (depends on usage)
   • S3 storage: ~$23/TB/month
   • Unity Catalog: included
   • Predictive Optimization: ~5% premium, saves manual effort

OFF-RAMPS:
   • UniForm means data readable by non-Databricks engines
   • Delta is open spec; can read with Spark/Trino/DuckDB outside Databricks
   • Migration to Iceberg possible via metadata rewriting if ever needed
```

#### 27.4 Senior vs Staff

| Senior Answer | Staff Answer |
|---------------|--------------|
| "Delta gives ACID on S3." | "Delta's ACID derives from optimistic concurrency on put-if-absent file creation plus a flat JSON transaction log. Writers commit by writing version N+1; concurrent writers either succeed uniquely or retry. Checkpoints every 10 commits consolidate state for fast reads. The design trades some write throughput (serialized via version numbers) for simplicity and strong consistency. Compare to Iceberg's hierarchical metadata — Delta's model is simpler but has more contention at high write rates." |
| "Use OPTIMIZE for small files." | "File size management has multiple layers: (1) write-time: target file size (default auto, usually 128 MB - 1 GB) and dynamic file sizing for incremental writes; (2) OPTIMIZE for batch compaction when fragmentation accumulates; (3) Liquid Clustering for ongoing maintenance that doesn't require periodic OPTIMIZE. For streaming-heavy tables, I set an aggressive OPTIMIZE schedule or enable Predictive Optimization. The signal to watch: average file size trending below 128 MB indicates compaction is lagging." |
| "Delta or Iceberg?" | "Depends on write patterns and ecosystem commitment. All-Databricks writes + mixed readers → Delta + UniForm. Mixed writes → Iceberg. All-Databricks + Databricks reads only → pure Delta. The decision is less about technical capability (both are mature) and more about: (a) catalog preference (Unity Catalog vs Polaris); (b) multi-engine neutrality; (c) team expertise. For greenfield non-Databricks, I default to Iceberg. For greenfield Databricks, Delta is natural." |

### 28. War Stories

> **🔥 Production Scenario — The VACUUM Disaster.** A team ran `VACUUM orders RETAIN 0 HOURS` to reclaim storage on a 50 TB table. They had disabled the retention check. A long-running analytical job was reading an older snapshot; VACUUM deleted its data files mid-query. Job crashed; results invalid; required 2-day rebuild. Lesson: never override VACUUM retention without 100% understanding of all concurrent readers.

> **🔥 Production Scenario — The MERGE Amplification.** A team's nightly MERGE job on a 10 TB Delta table was taking 6 hours. Investigation: without deletion vectors, every file touched by a matched row was fully rewritten. For 1% matches, this amplified writes 100x. Enabled deletion vectors (`delta.enableDeletionVectors = true`): job dropped to 25 minutes. Lesson: deletion vectors are a huge win for selective MERGE; enable them.

> **🔥 Production Scenario — The Protocol Lockout.** A team upgraded a Delta table to use Column Mapping (`delta.columnMapping.mode = 'name'`), which bumped the writer protocol. Downstream pipelines using older Delta libraries failed to write ("protocol version too new"). Rollback was impossible (Column Mapping is one-way). Workaround: upgrade all client libraries first. Lesson: protocol upgrades require coordinated client updates; check writer versions before enabling features.

> **🔥 Production Scenario — The UniForm Lag.** A team used Databricks writes with UniForm enabled; Snowflake read the same tables as external Iceberg. Dashboards occasionally showed stale data (10-15 min lag). Root cause: UniForm's async Iceberg metadata generation. Fix: added monitoring on commit-timestamp lag between Delta and UniForm's Iceberg snapshot; alerted when drift > 5 min. Lesson: UniForm bridges formats but is NOT synchronous.

> **🔥 Production Scenario — The Transaction Log Explosion.** A streaming job wrote to a Delta table every 5 seconds for 2 months without any maintenance. Transaction log had 1 million JSON files. Loading the table took 15 minutes just to replay the log. Fix: set `delta.checkpointInterval = 10` + ran manual checkpoint. Load time dropped to seconds. Lesson: checkpointing is critical for streaming writers; monitor log directory size.

### 29. Cheat Sheet & Mental Models

#### 29.1 The One-Minute Pitch

```
Delta Lake = ACID storage layer for data lakes.
   • Transaction log (JSON files + checkpoints) over Parquet
   • Optimistic concurrency via put-if-absent file creation
   • Schema evolution + time travel + MERGE
   • Deletion vectors for efficient deletes/MERGE
   • Liquid Clustering for maintained multi-column pruning
   • UniForm for Iceberg interop
   • Delta Sharing for cross-platform data exchange

Primary deployment: Databricks (native format).
OSS deployment: Spark, Flink, Trino, DuckDB, delta-rs (Rust).

Pick when: Databricks-centric; need its specific features (Liquid Clustering, etc.)
Reconsider when: multi-engine access critical; vendor neutrality mandated.
```

#### 29.2 The Four Lenses — Compressed

```
CONCEPT
   Transaction log + Parquet. ACID via put-if-absent OCC. One commit = one JSON file.

MECHANICS
   _delta_log/ holds JSONs + periodic checkpoints.
   Actions: Metadata, Protocol, Add, Remove, Txn, CommitInfo.
   Protocol versions + Table Features gate compatibility.

TRADE-OFFS
   Pros: mature, Databricks-optimized, deletion vectors, Liquid Clustering.
   Cons: Databricks-centric (vs Iceberg's broader ecosystem).
   Multi-engine: via UniForm (async bridge to Iceberg).

INTERVIEW
   Know: log layout, OCC mechanics, DVs, Liquid Clustering, UniForm,
         when Delta vs Iceberg.
   Defend: your platform choice; retention + VACUUM safety; MERGE optimization.
```

#### 29.3 The Essential Commands

```sql
-- Create
CREATE TABLE orders (id BIGINT, ts TIMESTAMP, amount DECIMAL)
USING DELTA
PARTITIONED BY (date(ts))
TBLPROPERTIES (
    'delta.enableDeletionVectors' = 'true',
    'delta.enableChangeDataFeed' = 'true'
);

-- Modern: Liquid Clustering (preferred over partitioning for many cases)
CREATE TABLE orders (...) USING DELTA
CLUSTER BY (user_id, ts);

-- Write
INSERT INTO orders VALUES (...);
MERGE INTO orders USING updates ON orders.id = updates.id
    WHEN MATCHED THEN UPDATE SET ...
    WHEN NOT MATCHED THEN INSERT ...;

-- Read
SELECT * FROM orders VERSION AS OF 42;
SELECT * FROM orders TIMESTAMP AS OF '2024-03-15';
SELECT * FROM table_changes('orders', 10, 20);  -- CDF

-- Maintenance
OPTIMIZE orders;
OPTIMIZE orders ZORDER BY (user_id);
VACUUM orders RETAIN 168 HOURS;

-- Clone
CREATE TABLE orders_dev SHALLOW CLONE orders;
CREATE TABLE orders_bak DEEP CLONE orders;

-- Enable interop
ALTER TABLE orders SET TBLPROPERTIES (
    'delta.universalFormat.enabledFormats' = 'iceberg'
);
```

#### 29.4 Mental Model

```
Think of Delta Lake as:
   • "Postgres on S3" for analytical workloads
   • The Databricks-native lakehouse format
   • The format that invented the lakehouse pattern
   • A good default inside Databricks; UniForm bridges elsewhere

When to reach for it:
   • Databricks is your platform
   • You want its ecosystem (OPTIMIZE, Liquid Clustering, Predictive Opt)
   • You need Delta Sharing for cross-org data
   • You want strongest Spark-native integration

When to reach past it:
   • Multi-engine access without UniForm overhead
   • Open governance mandate (prefer Apache)
   • Hidden partitioning important
```

> **🧠 Principal Mindset:** Delta Lake is a case study in platform strategy. Databricks invented the lakehouse pattern; competitors (Snowflake, BigQuery) adopted it; Iceberg won the open-format war. Databricks's response — UniForm, Unity Catalog open-sourcing, Tabular acquisition — shows they chose INTEROP over isolation. For most new projects in 2026, Delta is only correct if your write side is Databricks-committed. That's still a huge market. But the days of "Delta is the lakehouse format" are over; the days of "Delta is Databricks's native format alongside Iceberg interop" are here.

---

[← Back to main](./README.md)
