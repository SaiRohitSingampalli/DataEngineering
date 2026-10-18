# Topic 11 — Apache Iceberg

> Standalone deep-dive · The open table format that won the lakehouse war

[← Back to main](./README.md)

---

## How to use this document

This is the architect-level reference for Apache Iceberg: how its three-layer metadata works, how it achieves ACID on object storage, how it compares to Delta and Hudi, and how to defend architectural choices around it at staff level. Every chapter follows the four lenses: **Concept · Mechanics · Trade-offs · Interview**.

> **🧠 Principal Mindset:** By 2026, Iceberg has essentially won the open table format war for multi-engine lakehouses. Snowflake, BigQuery, Databricks (via UniForm), Trino, Spark, Flink, DuckDB all read Iceberg. The real question isn't "which format" anymore — it's "which catalog, which compaction strategy, which operational model". The staff-level signal is reasoning about the layer above and below Iceberg, not just reciting its features.

## Table of Contents

**PART I — FOUNDATIONS**
- 1. What Iceberg Is and Why It Won
- 2. The History: From Netflix to Apache Top-Level
- 3. The Design Principles
- 4. Iceberg vs Delta vs Hudi

**PART II — CORE ARCHITECTURE**
- 5. The Three-Layer Metadata Model
- 6. Snapshots and Time Travel
- 7. Partition Specs and Hidden Partitioning
- 8. Schema Evolution

**PART III — FILE LAYOUT & STATISTICS**
- 9. Data Files, Delete Files, Puffin
- 10. Manifest Files and Manifest Lists
- 11. Column Statistics and Pruning

**PART IV — WRITING & READING**
- 12. The Write Path: Append, Overwrite, Merge
- 13. Row-Level Updates: V2 vs V3
- 14. The Read Path and Query Planning
- 15. Concurrency and Isolation

**PART V — CATALOGS**
- 16. The Catalog Protocol
- 17. Hive, Hadoop, Nessie, REST
- 18. Polaris and Unity Catalog
- 19. Catalog HA and Operations

**PART VI — MAINTENANCE**
- 20. The Small Files Problem
- 21. Compaction Strategies
- 22. Snapshot Expiry and Orphan Files
- 23. Table Properties Reference

**PART VII — MULTI-ENGINE**
- 24. Engine Interop (Spark, Flink, Trino, DuckDB, Snowflake, BigQuery)
- 25. UniForm and Delta Interop
- 26. Streaming Writes

**PART VIII — INTERVIEW PRACTICE**
- 27. The Staff-Level Iceberg Interview
- 28. War Stories
- 29. Cheat Sheet & Mental Models

---

## PART I — FOUNDATIONS

### 1. What Iceberg Is and Why It Won

#### 1.1 The One-Sentence Definition

Apache Iceberg is an **open table format** — a specification plus a reference implementation — that gives Parquet files sitting in object storage (S3, GCS, Azure Blob) the ACID properties, schema evolution, time travel, and metadata-driven planning of a database table, while remaining readable by any engine that implements the spec.

#### 1.2 The Problem It Solves

```
BEFORE OPEN TABLE FORMATS:
   A "table" in a data lake was a convention:
      s3://bucket/warehouse/orders/
         year=2024/month=03/day=15/*.parquet
         year=2024/month=03/day=16/*.parquet
         ...

   Problems:
   • No atomic multi-file writes (partial failures leak)
   • No schema evolution (adding a column breaks readers)
   • No time travel (can't query "as of last Tuesday")
   • File listings are slow at scale (S3 LIST is paginated)
   • No row-level updates (whole-file rewrites only)
   • Statistics scattered across Parquet footers; no central index
   • Partition pruning via directory structure is brittle

AFTER ICEBERG:
   A table is a METADATA FILE pointing to a tree of manifest files
   pointing to data files. All operations go through metadata.
   Atomic. Evolvable. Time-travelable. Fast to plan.
```

#### 1.3 The Shape of the Thing

```
Iceberg is THREE things stacked:

   1. A SPECIFICATION
      Format version 2 (current stable) and 3 (released, adoption growing)
      Written in a public repo; multi-vendor review
      Anyone can implement a reader or writer

   2. A REFERENCE JAVA IMPLEMENTATION
      Used by Spark, Flink, Trino, Hive via connector
      APIs for catalog, table operations, scan planning

   3. AN ECOSYSTEM
      Catalog implementations (Hive, Hadoop, REST, Nessie, Polaris, Unity, Glue)
      Engine integrations (Spark, Flink, Trino, Impala, Presto, DuckDB,
         Snowflake, BigQuery, Starburst, Dremio, Athena, EMR, ...)
      Client libraries (PyIceberg, iceberg-rust, iceberg-go, iceberg-cpp)
```

#### 1.4 Why Iceberg Won

```
TECHNICAL REASONS:
   • Clean separation of metadata and data (vs Delta's tightly-coupled log)
   • Hidden partitioning (query doesn't need to know the partition column)
   • Schema evolution without rewriting data
   • Snapshot isolation by construction
   • Pluggable catalog (no Databricks dependency)
   • Format v2 added row-level updates with equality + positional deletes
   • Format v3 (2024-2025) added deletion vectors, variant type, geometry

ORGANIZATIONAL REASONS:
   • Open governance (Apache; not vendor-controlled)
   • Netflix's commitment to open-sourcing it (unlike proprietary formats)
   • Tabular's independent commercialization (then Databricks acquisition)
   • Snowflake, AWS, Google all back it (vs Delta being Databricks-only)
   • Polaris Catalog (open) emerged; Unity Catalog open-sourced in response

ECOSYSTEM REASONS:
   • Early adopters: Netflix, Apple, Adobe, Expedia, LinkedIn
   • Snowflake adopted external Iceberg tables in 2022
   • BigQuery adopted managed Iceberg tables in 2023
   • Databricks acquired Tabular in 2024 (effectively endorsing Iceberg)
```

> **🎯 Interview Angle:** Q: Why did Iceberg win over Delta?
> **Staff:** "Three reasons, roughly in order of importance. (1) Governance — Iceberg has open Apache governance; Delta is primarily Databricks-driven. For enterprises wary of vendor lock-in, Iceberg was a safer bet. (2) Multi-engine support — Iceberg had Snowflake, BigQuery, Trino, Flink, and Spark readers before Delta had wide multi-engine support; UniForm came later. (3) Technical cleanliness — Iceberg's three-layer metadata separates concerns well; Delta's single transaction log is simpler but less flexible for very large tables. The Tabular acquisition in 2024 was arguably Databricks acknowledging the market had chosen; they positioned UniForm as the bridge rather than trying to force Delta."

### 2. The History: From Netflix to Apache Top-Level

#### 2.1 Origins (2017-2019)

```
2017: Ryan Blue and Daniel Weeks at Netflix hit scaling limits with
      Hive-style tables on S3. Listing partitions was slow;
      schema changes were painful; atomic multi-partition writes
      were impossible.
      
      They start designing a new table format.

2018: Iceberg open-sourced under Apache 2.0.
      First version of the spec published.
      Netflix migrates production workloads.

2019: Apache incubator.
      Early adopters: Apple, Expedia, LinkedIn.
```

#### 2.2 Growth (2020-2022)

```
2020: Spark 3.0 ships with Iceberg connector.
      Trino (then Presto) adds Iceberg support.
      Snowflake and others begin internal evaluation.

2021: Format v2 released (row-level deletes, upserts).
      Apache top-level project.
      Flink Iceberg sink matures.

2022: Snowflake external Iceberg tables (reads).
      AWS Athena / Glue Iceberg integration.
      Ryan Blue, Daniel Weeks, Jason Reid leave Netflix,
      found Tabular (commercial Iceberg company).
```

#### 2.3 Winning (2023-2024)

```
2023: BigQuery managed Iceberg tables.
      Snowflake Iceberg tables (writes, not just reads).
      Microsoft Fabric adopts Iceberg.
      Hugging Face adopts Iceberg for datasets.

2024 Jun: Databricks acquires Tabular for $1B+.
          The acquisition that signals Iceberg has won.
          Snowflake announces Polaris Catalog (open-source Iceberg catalog).
          Databricks open-sources Unity Catalog with Iceberg read support.
          
2024-25: Format v3 released: deletion vectors, variant type,
         geometry type, better statistics (Puffin).
```

#### 2.4 Standardization (2025-2026)

```
2025: Puffin stats widely adopted; faster NDV / theta sketch planning.
      REST catalog becomes de facto standard.
      Snowflake deeply integrates Polaris as its Iceberg catalog.

2026 Feb: Apache Polaris graduates to Apache top-level project.
          Validation of the open-catalog vision.
          
2026: Format v3 adoption increases.
      Iceberg + REST catalog + Parquet is the de facto open stack.
      Delta UniForm supports bidirectional Iceberg-Delta interop.
```

### 3. The Design Principles

#### 3.1 Separation of Metadata and Data

```
A traditional database couples storage, metadata, and compute.
Iceberg separates:

   DATA           — Parquet (immutable files; chosen by writer)
   METADATA       — JSON + Avro files describing data files
   CATALOG        — small key-value store: table name → metadata location
   COMPUTE        — any engine (Spark, Flink, Trino, DuckDB, Snowflake, ...)

Each layer can evolve independently. The spec is small enough to implement;
the compute layer can be any engine.
```

#### 3.2 Snapshot-Based Isolation

```
Every write produces a NEW SNAPSHOT of the entire table state.
Readers see one snapshot throughout their query.
Writers never modify in place.

This gives:
   • Serializable isolation by construction
   • Time travel (query past snapshots)
   • Atomic commits (one metadata pointer swap)
   • Easy rollback (point to older snapshot)
```

#### 3.3 Pluggable Catalog

```
The catalog is just a key-value map:
   (namespace, table_name) → metadata_file_location
   plus atomic compare-and-swap on the pointer.

Any system that supports CAS can be an Iceberg catalog:
   • Hive Metastore
   • Hadoop filesystem (directory-based)
   • JDBC database
   • REST service
   • Nessie (git-like)
   • Polaris
   • Unity Catalog
   • AWS Glue
```

#### 3.4 Hidden Partitioning

```
Hive-style tables: partition column must be in the query.
   WHERE year = 2024 AND month = 3   ← user must write this

Iceberg hidden partitioning: partition is derived from a column.
   PARTITIONED BY (days(event_ts))
   WHERE event_ts BETWEEN '2024-03-01' AND '2024-03-31'
   
   Iceberg automatically figures out which day-partitions to scan.
   Users don't need to know the partition scheme.

This decouples physical layout from query API.
Repartitioning the table doesn't break existing queries.
```

#### 3.5 Schema Evolution Without Rewriting Data

```
Every column has a stable field ID (not just a name).
Renaming a column: update metadata, no data rewrite.
Dropping a column: mark it dropped; readers skip it.
Adding a column: new column defaults to null for old rows.
Promoting int → bigint: metadata-only operation.

The spec defines exactly which evolutions are safe:
   ✓ Add column
   ✓ Rename column
   ✓ Reorder columns
   ✓ Promote int → bigint, float → double, decimal(p1,s) → decimal(p2,s) with p2 > p1
   ✗ Narrow (bigint → int): data loss possible
   ✗ Change type to incompatible: forbidden
```

> **🧠 Principal Mindset:** The design principle most under-appreciated is "field ID over field name." It sounds trivial but it's what makes schema evolution without data rewrites possible. Delta rediscovered this later (via Delta Lake column mapping). If you understand why field IDs matter, you understand half of what makes Iceberg good.

### 4. Iceberg vs Delta vs Hudi

#### 4.1 The Three-Way Comparison

| Aspect | Iceberg | Delta Lake | Hudi |
|--------|---------|-----------|------|
| **Origin** | Netflix (2017) | Databricks (2019) | Uber (2017) |
| **Governance** | Apache (open) | Linux Foundation (Databricks-led) | Apache |
| **Primary file format** | Parquet (Avro, ORC optional) | Parquet | Parquet |
| **Metadata location** | Hierarchical files (snapshot → manifest list → manifest) | Flat transaction log (JSON files + checkpoints) | Timeline log + file groups |
| **Partition handling** | Hidden partitioning (transform-based) | Explicit partition columns | Explicit partition columns |
| **Row-level deletes** | V2: positional + equality deletes; V3: deletion vectors | Deletion vectors (since 2.4) | Native (MoR / CoW) |
| **Upsert model** | MERGE statement | MERGE statement | First-class upsert API |
| **Catalog** | Pluggable (Hive, REST, Nessie, Polaris, Unity, Glue) | Databricks Unity, Hive | Hive, custom |
| **Multi-engine** | Best-in-class (Snowflake, BQ, Databricks, Spark, Flink, Trino, DuckDB) | Delta UniForm bridges to Iceberg; direct support in Spark/Databricks primarily | Spark, Flink, Trino, Hive |
| **Streaming writes** | Via Flink / Spark Structured Streaming | Via Spark Structured Streaming | Native streaming (DeltaStreamer/HudiStreamer) |
| **Time travel** | Snapshot-based (seconds to query any snapshot) | Version/time-based (vacuum removes old versions) | Timeline-based |
| **Schema evolution** | Field ID-based (safest) | Column mapping (added later) | Avro schema |

#### 4.2 Workload Fit

```
PICK ICEBERG WHEN:
   ✓ Multi-engine access matters (Snowflake + Spark + Trino + ML)
   ✓ Long-term vendor neutrality is important
   ✓ Schema evolves frequently
   ✓ You want hidden partitioning / partition evolution
   ✓ Fewer features, cleaner model

PICK DELTA WHEN:
   ✓ Databricks-centric shop
   ✓ Need Databricks-specific features (Liquid Clustering, Photon tuning)
   ✓ Primary engine is Spark/Databricks
   ✓ Already have Delta investments

PICK HUDI WHEN:
   ✓ Streaming upserts at TB/day scale
   ✓ Record-level indexing is important
   ✓ Mixed streaming + batch with low-latency reads (MoR)
   ✓ Already have Uber-style CDC workflows
```

#### 4.3 The Convergence Story

```
By 2026:
   • Delta UniForm writes Delta + Iceberg metadata simultaneously
     → Delta tables readable by Iceberg engines (Snowflake, BigQuery)
   • Hudi 1.0 (2025) adds Iceberg table-type support
   • Iceberg V3 adds deletion vectors (matching Delta's efficient deletes)
   • All three formats converge on similar capabilities
   
   The format war is largely over. The REAL questions are:
   • Which CATALOG do you commit to? (Polaris, Unity, Glue, custom)
   • Which engines do you NEED to read from?
   • What's your write-path?
   
   For new projects: default to Iceberg unless you have a specific reason.
```

> **🎯 Interview Angle:** Q: Delta UniForm reads Iceberg — doesn't that make the choice moot?
> **Staff:** "Not entirely. UniForm is a bridge, not full parity. (1) Latency — UniForm writes Iceberg metadata asynchronously; Iceberg readers may see stale views. (2) Feature gaps — some Delta features (Liquid Clustering, specific Databricks optimizations) don't surface through UniForm. (3) Operational — you still need to decide which format is your SOURCE OF TRUTH. If your writers are Databricks → source of truth is Delta → UniForm is the Iceberg view. If writers are mixed, you need a committed primary format. UniForm helped de-escalate the format war but didn't eliminate the choice. For greenfield in 2026 I default to Iceberg; the only reason to pick Delta is if your writer side is 100% Databricks-committed."

---

## PART II — CORE ARCHITECTURE

### 5. The Three-Layer Metadata Model

#### 5.1 The Picture

```
                    ┌─────────────────────────────┐
                    │       CATALOG               │
                    │   (table_name → metadata)   │
                    └──────────────┬──────────────┘
                                   │ atomic swap
                                   ▼
                 ┌─────────────────────────────────────┐
                 │   METADATA FILE (vN.metadata.json)  │
                 │   • Schema                           │
                 │   • Partition spec                   │
                 │   • Snapshot history                 │
                 │   • Current snapshot ID              │
                 │   • Properties                       │
                 └──────────────┬──────────────────────┘
                                │
                                ▼
                 ┌─────────────────────────────────────┐
                 │   MANIFEST LIST (snap-N.avro)       │
                 │   • One row per manifest file       │
                 │   • Partition summaries              │
                 │   • Data file counts                 │
                 └──────────┬──────────────────────────┘
                            │
             ┌──────────────┼──────────────┐
             ▼              ▼              ▼
       ┌─────────┐   ┌─────────┐    ┌─────────┐
       │MANIFEST │   │MANIFEST │    │MANIFEST │
       │  FILE   │   │  FILE   │    │  FILE   │
       │ (avro)  │   │ (avro)  │    │ (avro)  │
       └────┬────┘   └────┬────┘    └────┬────┘
            │             │              │
            ▼             ▼              ▼
       data files    data files     data files
       (parquet)     (parquet)      (parquet)
```

#### 5.2 Why Three Layers

```
The three-layer design optimizes for different access patterns:

   CATALOG → fast "find the table" lookup; one small KV read.
   
   METADATA FILE → load table state; one JSON read (<MB typically).
                   Contains the full snapshot history for time travel.
   
   MANIFEST LIST → scan planning entry point; one Avro read.
                   One row per manifest; partition stats pre-aggregated.
                   Lets planner prune at manifest level.
   
   MANIFEST FILE → the actual data file catalog.
                   Pruned by partition; opened only for relevant manifests.
                   
   DATA FILES → actual Parquet data.
                Read only after planning has pruned to candidate files.

Scanning a 1 PB table might open:
   1 catalog lookup
   1 metadata JSON
   1 manifest list
   10-50 manifest files (99.9% pruned)
   100-1000 data files (99.9%+ pruned)
```

#### 5.3 Metadata File Structure

```json
{
  "format-version": 2,
  "table-uuid": "9c12d441-...",
  "location": "s3://bucket/warehouse/db/orders",
  "last-updated-ms": 1710501780000,
  "last-column-id": 42,
  "schemas": [
    {"schema-id": 0, "fields": [...]},
    {"schema-id": 1, "fields": [...]}
  ],
  "current-schema-id": 1,
  "partition-specs": [...],
  "default-spec-id": 0,
  "sort-orders": [...],
  "properties": {"write.format.default": "parquet"},
  "current-snapshot-id": 12345,
  "snapshots": [
    {"snapshot-id": 12345, "parent-snapshot-id": 12344, ...}
  ],
  "snapshot-log": [...],
  "metadata-log": [...]
}
```

#### 5.4 Writing a Row (the Full Path)

```
INSERT INTO orders VALUES (...)

1. Writer resolves current metadata via catalog
2. Writer creates new Parquet data file(s) in table's data location
3. Writer creates new manifest file(s) listing those data files
4. Writer creates new manifest list, including existing + new manifests
5. Writer creates new metadata file (vN+1.metadata.json)
   referencing new manifest list as the new current snapshot
6. Writer requests catalog: CAS vN → vN+1
   
   If success: commit visible to future readers
   If failure (concurrent write): retry from step 1 with new base
```

#### 5.5 Reading a Query (the Full Path)

```
SELECT * FROM orders WHERE date = '2024-03-15'

1. Reader queries catalog: get current metadata location
2. Reader loads metadata file
3. Reader loads current snapshot's manifest list
4. Reader filters manifests whose partition summaries might include date='2024-03-15'
5. Reader loads candidate manifest files
6. Reader filters data files whose min/max stats might match
7. Reader opens candidate Parquet files, reads needed row groups
```

> **💡 Tip:** The three-layer design means metadata operations never require listing object storage (slow and expensive). Everything is explicit pointer traversal. This is the operational superpower vs Hive-style tables: planning is O(log N) not O(N) in the number of files.

### 6. Snapshots and Time Travel

#### 6.1 What a Snapshot Is

```
A SNAPSHOT = the complete state of a table at one point in time.

Properties:
   • snapshot-id (long)
   • timestamp-ms
   • parent-snapshot-id
   • summary (operation type, added/deleted file counts)
   • manifest-list (location of the manifest list for this snapshot)
   • schema-id (which schema was active)
```

#### 6.2 Operations That Create Snapshots

```
   APPEND    → write new files
   OVERWRITE → replace files (with new data)
   DELETE    → add delete files (V2) or deletion vectors (V3)
   REPLACE   → full partition replacement
   
Every write creates a new snapshot. Metadata grows ~linearly with writes.
```

#### 6.3 Time Travel Queries

```sql
-- Time travel by snapshot ID
SELECT * FROM orders FOR VERSION AS OF 12345;

-- Time travel by timestamp
SELECT * FROM orders FOR TIMESTAMP AS OF '2024-03-15 10:00:00';

-- Spark syntax (older)
SELECT * FROM orders.snapshot_12345;
SELECT * FROM orders.as_of_timestamp('2024-03-15');

-- Diff between snapshots
SELECT * FROM orders.changes BETWEEN 12345 AND 12399;
```

#### 6.4 Rollback

```sql
-- Set table to older snapshot (dangerous; new data since will be invisible)
CALL system.rollback_to_snapshot('db.orders', 12345);

-- Rollback by timestamp
CALL system.rollback_to_timestamp('db.orders', '2024-03-15 10:00:00');

-- Set current snapshot (same as rollback but conceptually "replay to here")
CALL system.set_current_snapshot('db.orders', 12345);
```

#### 6.5 Snapshot Retention

```sql
-- Snapshots accumulate; set retention via table properties
ALTER TABLE orders SET TBLPROPERTIES (
    'history.expire.max-snapshot-age-ms' = '604800000',  -- 7 days
    'history.expire.min-snapshots-to-keep' = '10'
);

-- Manually expire old snapshots (reclaims storage)
CALL system.expire_snapshots('db.orders', TIMESTAMP '2024-01-01');
```

Caveat: expired snapshots can't be time-traveled to anymore, and their referenced data files may be deleted if no other snapshot references them.

### 7. Partition Specs and Hidden Partitioning

#### 7.1 The Hive Partitioning Problem

```
Hive-style tables partition by column:
   s3://bucket/orders/year=2024/month=03/day=15/*.parquet
   
   Query: WHERE year=2024 AND month=3 AND day=15

Problems:
   • User must know the partition column
   • User must write the partition predicate explicitly
   • Changing partition scheme breaks existing queries
   • Timestamp → partition conversion is manual: 
       WHERE event_ts >= '2024-03-15' AND event_ts < '2024-03-16'
     doesn't automatically prune year/month/day partitions.
```

#### 7.2 Iceberg Hidden Partitioning

```sql
CREATE TABLE orders (
    event_ts TIMESTAMP,
    user_id BIGINT,
    amount DECIMAL(18,2)
)
PARTITIONED BY (days(event_ts));

-- User queries without knowing the partition
SELECT * FROM orders WHERE event_ts >= '2024-03-15'
                       AND event_ts <  '2024-03-16';

-- Iceberg automatically derives: day_of_event_ts = 2024-03-15
-- And prunes to the right partitions.
```

#### 7.3 Supported Partition Transforms

```
From a timestamp:
   year(ts), month(ts), day(ts), hour(ts)
   
From any type:
   bucket(N, column)     — hash into N buckets
   truncate(N, column)   — truncate to N chars (string) or N (numeric)
   identity(column)      — the column value itself
```

#### 7.4 Partition Evolution

```sql
-- Change partitioning without rewriting data
ALTER TABLE orders ADD PARTITION FIELD hours(event_ts);
ALTER TABLE orders DROP PARTITION FIELD days(event_ts);

-- Old data stays in day-partitioned layout.
-- New writes go into hour-partitioned layout.
-- Readers handle both; no rewrite needed.
```

This is uniquely powerful. Delta and Hudi require data rewrites to change partitioning; Iceberg can do it metadata-only.

#### 7.5 Partition Statistics

```
Each manifest's partition summary includes:
   • Min partition value
   • Max partition value
   • Row count
   • File count
   
Planning can prune manifests just from this summary, without
even opening the manifest files themselves.
```

### 8. Schema Evolution

#### 8.1 Field IDs

```
Every column has a stable, immutable field ID assigned at creation.
The column NAME is stored separately and can change.

   CREATE TABLE users (id BIGINT, name STRING);
   
   Internal representation:
      field_id=1: id (bigint)
      field_id=2: name (string)
   
   ALTER TABLE users RENAME COLUMN name TO full_name;
   
   Internal representation:
      field_id=1: id (bigint)
      field_id=2: full_name (string)   ← same field_id, new name
   
   Parquet files still have field_id=2 for the data.
   No data rewrite needed.
```

#### 8.2 Allowed Evolutions

```
✓ ADD column:
    ALTER TABLE users ADD COLUMN email STRING;
    Old rows: email = NULL.

✓ DROP column:
    ALTER TABLE users DROP COLUMN email;
    Column marked as dropped; data retained but ignored.

✓ RENAME column:
    ALTER TABLE users RENAME COLUMN name TO full_name;
    Metadata-only.

✓ REORDER columns:
    ALTER TABLE users ALTER COLUMN email AFTER id;
    Metadata-only.

✓ WIDEN type:
    ALTER TABLE users ALTER COLUMN count TYPE BIGINT;  -- from INT
    Metadata-only; readers upcast on read.
```

#### 8.3 Forbidden Evolutions

```
✗ NARROW type (bigint → int): data loss possible
✗ Change type to incompatible (string → int): forbidden
✗ Change primitive to complex (int → struct): forbidden

If you NEED one of these, write new data to a new column:
   ALTER TABLE users ADD COLUMN count_v2 INT;
   UPDATE users SET count_v2 = CAST(count AS INT);
   ALTER TABLE users DROP COLUMN count;
   ALTER TABLE users RENAME COLUMN count_v2 TO count;
```

#### 8.4 Nested Schema Evolution

```
Iceberg supports evolution of nested fields:
   
   STRUCT with new field: add
   STRUCT with removed field: drop
   LIST element type: evolve same rules as top-level
   MAP key/value types: evolve same rules
```

> **🔥 Production Scenario:** A team renamed a column on a 50 TB Iceberg table. Operation completed in milliseconds (metadata-only). The equivalent operation on the Hive-style predecessor table had required a full table rewrite — 14 hours, $800 in Spark compute. The field-ID design pays dividends constantly, not just at special moments. Teams that migrate to Iceberg usually underestimate how often schema evolution happens; it's dozens of times per month in active products.

---

## PART III — FILE LAYOUT & STATISTICS

### 9. Data Files, Delete Files, Puffin

#### 9.1 Data Files

```
Each snapshot references a set of data files.
Default format: Parquet (recommended).
Also supported: Avro, ORC (less common).

Data file metadata (stored in manifests):
   • File path (S3/GCS/Azure URL)
   • File format
   • Partition values
   • Record count
   • File size in bytes
   • Column-level stats: 
      - Value counts
      - Null counts
      - NaN counts
      - Lower bounds (min)
      - Upper bounds (max)
   • Column size in bytes
   • Sort order ID
   • Equality field IDs (for V2 equality deletes)
```

#### 9.2 Position Delete Files (V2)

```
When you DELETE rows in Iceberg V2 format, the old data files aren't rewritten.
Instead, a DELETE FILE is created that lists positions to skip.

Position delete format:
   file_path  | pos
   -----------|-----
   data.parquet | 42
   data.parquet | 157
   data.parquet | 999

At read time: reader opens both data file and delete file.
             Skips rows at those positions.
```

Position deletes are efficient to apply but less flexible — they tie deletes to specific file+position.

#### 9.3 Equality Delete Files (V2)

```
Equality deletes specify rows to delete by value, not position:

   id = 42
   id = 157
   id = 999

At read time: reader checks each row against delete values.
             Skips rows that match.

More flexible than position deletes — don't require knowing file+position.
Used for streaming CDC where you don't track positions.
```

#### 9.4 Deletion Vectors (V3)

```
V3 introduced DELETION VECTORS, borrowed from Delta Lake:

   One bitmap per data file.
   Bit N set = row N is deleted.
   
   Format: Puffin blob with roaring bitmap compression.

Advantages over position + equality deletes:
   • Compact (bitmap for millions of deletes)
   • Fast to apply (bitwise skip)
   • Single file per data file (vs many delete files)
```

#### 9.5 Puffin Files

```
PUFFIN = a container format for TABLE STATISTICS and BLOBS.
Added in late 2023; standardized in V3.

Blobs include:
   • Theta sketches (approximate distinct count)
   • Bloom filters (quick existence check)
   • HLL sketches (cardinality)
   • Deletion vectors (V3)

Puffin files sit alongside data files; referenced in manifests.
Engines can use these for query planning without opening data files.

Example: COUNT DISTINCT user_id → read theta sketch from Puffin
         No need to scan 50 TB of data for an approximate answer.
```

#### 9.6 File Format Choices

```sql
-- Set default file format for writes
ALTER TABLE orders SET TBLPROPERTIES (
    'write.format.default' = 'parquet',
    'write.parquet.compression-codec' = 'zstd',
    'write.parquet.row-group-size-bytes' = '134217728',  -- 128 MB
    'write.target-file-size-bytes' = '536870912'          -- 512 MB
);
```

Target file size matters for small-files problem — see maintenance.

### 10. Manifest Files and Manifest Lists

#### 10.1 Manifest Files

```
Each manifest file lists data files + delete files + their metadata.
Stored as Avro (compact, columnar-ish, schema-evolution-friendly).

Columns in a manifest:
   status (0=existing, 1=added, 2=deleted)
   snapshot_id (which snapshot added/removed this file)
   data_file:
      content (0=data, 1=position deletes, 2=equality deletes)
      file_path
      file_format
      partition
      record_count
      file_size_in_bytes
      column_sizes (map)
      value_counts (map)
      null_value_counts (map)
      lower_bounds (map)
      upper_bounds (map)
      key_metadata (encryption)
      split_offsets (list)
      equality_ids (list, for equality delete files)
      sort_order_id
```

#### 10.2 Manifest Lists

```
A manifest list = index of all manifests in a snapshot.
One row per manifest file.

Columns:
   manifest_path
   manifest_length
   partition_spec_id
   content (0=data, 1=delete)
   sequence_number
   min_sequence_number
   added_snapshot_id
   added_data_files_count
   existing_data_files_count
   deleted_data_files_count
   partitions (list of partition summaries)
   partition_summaries:
      contains_null, contains_nan
      lower_bound, upper_bound
```

#### 10.3 Why This Design

```
PRUNING AT TWO LEVELS:

LEVEL 1: Manifest list prunes WHICH MANIFESTS to open.
         Partition summaries per manifest let planner skip entire manifests
         that don't contain any matching partitions.

LEVEL 2: Within manifests, per-data-file stats let planner skip files
         whose column ranges don't match the predicate.

For a 1 PB table with 100,000 data files and 1,000 manifests:
   Planning might open:
      1 manifest list (<1 MB)
      5-50 manifests (5-50 MB total)
      Then read only matching data files
   
Total planning I/O: tens of MB.
Data read: whatever satisfies the predicate.
```

#### 10.4 Manifest Compaction

```
As snapshots accumulate, manifest count grows.
Too many manifests → slower planning.

Rewriting manifests consolidates them:
   CALL system.rewrite_manifests('db.orders');

Groups small manifests into larger ones.
Metadata-only operation (no data rewrite).
```

### 11. Column Statistics and Pruning

#### 11.1 Statistics Layers

```
Iceberg maintains column stats at THREE levels:

1. PARTITION SUMMARY (per manifest, per partition value):
   • lower_bound, upper_bound of partition keys
   
2. DATA FILE STATS (per column, per file):
   • lower_bound, upper_bound (per column)
   • null_count, nan_count, value_count
   • column_size (bytes)

3. PUFFIN BLOBS (optional, per column):
   • Theta sketches, Bloom filters, HLL

Statistics are PESSIMISTIC: a file "might" contain matching rows
if its range overlaps the predicate. Only reads can confirm.
```

#### 11.2 Predicate Pushdown

```
Query: WHERE user_id = 42 AND event_ts BETWEEN '2024-03-01' AND '2024-03-31'

Pruning:
   1. Partition pruning (hidden): event_ts range → day partitions
   2. Manifest pruning: manifests whose partition summaries miss the range
   3. Data file pruning:
      - user_id range: file's lower_bound ≤ 42 ≤ upper_bound
      - event_ts range: overlapping
   4. Within chosen Parquet files, row-group pruning via Parquet stats

Combined: 99.9%+ pruning typical for selective queries.
```

#### 11.3 When Stats Fail

```
Stats don't help when:
   • Predicate uses complex expression: WHERE md5(user_id) = '...'
   • High-cardinality string predicates without Bloom filter
   • NULL predicates on columns without null_count stats
   • Multiple columns correlated (stats are per-column, not multi-dimensional)

Workaround: write data sorted by common predicate columns.
Lower bounds + upper bounds become tight → pruning works.
```

#### 11.4 Sort Orders

```sql
-- Define a sort order; writers will try to maintain it
CREATE TABLE orders (
    event_ts TIMESTAMP,
    user_id BIGINT,
    amount DECIMAL(18,2)
)
PARTITIONED BY (days(event_ts))
TBLPROPERTIES ('write.sort-by' = 'user_id');

-- Or set after:
ALTER TABLE orders WRITE ORDERED BY user_id;
```

Sorting by common filter columns makes data file stats tight, enabling pruning on those columns.

#### 11.5 Z-Order and Multi-Column Sort (Spark 3.4+)

```sql
CALL system.rewrite_data_files(
    table => 'db.orders',
    strategy => 'sort',
    sort_order => 'zorder(user_id, product_id)'
);
```

Z-order interleaves bits of multiple columns, giving pruning on either column. Trade-off: higher write cost, slightly worse pruning than full sort on primary column but good pruning on any column.

> **🎯 Interview Angle:** Q: Why does a query on a 100 TB Iceberg table return a sampled count in milliseconds?
> **Staff:** "Three possibilities depending on what the engine does. (1) If it's `SELECT COUNT(*)`, each manifest entry has record_count — engine sums these from manifests without opening data files. (2) If it's `SELECT APPROX_COUNT_DISTINCT(col)`, Puffin theta sketches give the answer from metadata. (3) If it's a selective predicate, stats-based pruning reduces to a few files quickly. The common thread: Iceberg's metadata is rich enough to answer many analytical questions WITHOUT TOUCHING DATA FILES. Delta has similar capability for simple counts; Puffin extends this to approximate aggregates — a key Iceberg differentiator."

---

## PART IV — WRITING & READING

### 12. The Write Path: Append, Overwrite, Merge

#### 12.1 APPEND

```sql
INSERT INTO orders VALUES (...);
INSERT INTO orders SELECT * FROM staging;
```

Simplest write mode. New data files added; existing data untouched.

```
Mechanism:
   1. Writer buffers rows per partition
   2. Writes Parquet files to data location
   3. Creates new manifest listing these files
   4. Creates new manifest list including existing + new manifests
   5. Creates new metadata file with new snapshot
   6. Catalog CAS commits
```

#### 12.2 DELETE

```sql
DELETE FROM orders WHERE status = 'cancelled';
```

V2 format with position deletes:
```
   1. Reader-side query finds matching rows
   2. For each affected data file, writes a position delete file
      listing (file_path, pos) for deleted rows
   3. Commits new snapshot with delete files
```

V3 with deletion vectors:
```
   1. Query identifies matching rows
   2. Per data file, writes/updates a deletion vector
   3. Commits new snapshot
```

#### 12.3 UPDATE

```sql
UPDATE orders SET status = 'shipped' WHERE id IN (1, 2, 3);
```

Implemented as DELETE + INSERT:
```
   1. Read matching rows
   2. Compute new values
   3. Delete old rows (via position or equality deletes)
   4. Insert new rows
   5. Commit atomically
```

#### 12.4 MERGE

```sql
MERGE INTO orders tgt
USING updates src
ON tgt.id = src.id
WHEN MATCHED AND src.deleted = TRUE THEN DELETE
WHEN MATCHED THEN UPDATE SET status = src.status, updated_at = src.updated_at
WHEN NOT MATCHED THEN INSERT (id, status, created_at) VALUES (src.id, src.status, src.updated_at);
```

MERGE is the primary pattern for CDC and SCD2.

```
Mechanism depends on engine; Spark 3.5+ handles well via:
   • Scan target + source
   • Compute matches, updates, inserts, deletes
   • Write new files for inserts/updates
   • Write delete files for matched deletes
   • Commit atomically
```

#### 12.5 OVERWRITE

```sql
-- Replace matching rows / partitions
INSERT OVERWRITE orders PARTITION (event_date = '2024-03-15') ...

-- Dynamic partition overwrite (replace only partitions in source)
INSERT INTO orders ... REPLACE WHERE event_date >= '2024-03-15';
```

Replaces entire partitions atomically.

### 13. Row-Level Updates: V2 vs V3

#### 13.1 V1 (Append-Only)

```
Iceberg V1 was append-only.
No deletes, no updates — only new partitions.
Rewrite entire partitions to simulate updates.

Appropriate for immutable analytical workloads only.
Still some tables use V1 today.
```

#### 13.2 V2 (Row-Level Deletes)

```
V2 introduced two delete mechanisms:

POSITION DELETES:
   File = (file_path, pos) tuples
   Reader checks positions on scan
   
   PROS: Simple, efficient per-file
   CONS: Must know file + position (not CDC-friendly)
   
EQUALITY DELETES:
   File = (column, value) tuples
   Reader checks row values on scan
   
   PROS: Flexible, CDC-friendly (just know the keys)
   CONS: Slower (check every row against delete set)
```

#### 13.3 V3 (Deletion Vectors + More)

```
Released 2024-2025; adoption growing through 2026.

NEW IN V3:
   • Deletion vectors (bitmap per data file)
     More efficient than position/equality delete files
     Faster scans (bitwise AND vs row lookup)
     
   • Variant type (semi-structured, like Snowflake VARIANT)
     First-class JSON-like data without schema
     
   • Geometry type (GIS data)
     Native geo support
     
   • Default values for columns
     Added columns can have default (not just NULL)
     
   • Row lineage tracking
     See which snapshot added each row
```

#### 13.4 Choosing V2 vs V3

```
STICK WITH V2 WHEN:
   ✓ Your engine ecosystem doesn't fully support V3
   ✓ Current performance is adequate
   ✓ Mature deletes work for your workload

MIGRATE TO V3 WHEN:
   ✓ Delete-heavy workload (deletion vectors much faster)
   ✓ Need variant / geometry types
   ✓ All your engines support V3
   
Migration: SET TBLPROPERTIES ('format-version' = '3');
   Writers must support V3; readers must support V3.
```

### 14. The Read Path and Query Planning

#### 14.1 The Full Planning Flow

```
1. ENGINE INITIATES SCAN
   SELECT * FROM orders WHERE user_id = 42 AND date = '2024-03-15';

2. CATALOG LOOKUP
   Resolve current metadata location.

3. SNAPSHOT RESOLUTION
   Default: current snapshot.
   Time travel: FOR VERSION AS OF / FOR TIMESTAMP AS OF.

4. PARTITION FILTER DERIVATION
   Translate WHERE clause into partition predicates via hidden partitioning.
   date = '2024-03-15' → days_of_date = 2024-03-15.

5. MANIFEST LIST SCAN
   Read manifest list.
   Prune manifests whose partition summaries don't overlap filter.

6. MANIFEST FILE SCAN
   For each matched manifest, read it.
   Prune data files whose min/max don't satisfy predicate.
   Collect delete files that apply.

7. SPLIT GENERATION
   Create scan tasks (one per data file split).
   Attach applicable delete files.

8. EXECUTION
   For each task:
      Open data file (Parquet)
      Read required columns
      Apply predicate
      Apply deletes
      Emit rows
```

#### 14.2 Column Projection

```
Readers only decode columns referenced in the query.
Parquet's columnar layout means I/O is proportional to projected columns.

SELECT user_id FROM orders WHERE user_id = 42
   → Reader opens Parquet, decodes only user_id column.
```

#### 14.3 Delete Application

```
For a scan task with applicable deletes:

Position deletes:
   Read the position delete file into memory.
   Build a bitmap of deleted positions per data file.
   Skip rows at those positions during scan.

Equality deletes:
   Read equality delete values into memory (e.g., set of IDs).
   For each row, check if its equality fields are in the delete set.
   Skip if matched.

Deletion vectors (V3):
   Read the bitmap.
   Bitwise AND with row positions.
```

#### 14.4 Caching

```
Metadata is often cached:
   • Catalog metadata pointer (seconds)
   • Metadata file (per table)
   • Manifest list (per snapshot)
   • Puffin stats (per table)

Data files are cached by the storage layer or engine (not Iceberg).
```

### 15. Concurrency and Isolation

#### 15.1 The Isolation Model

```
Iceberg provides SNAPSHOT ISOLATION (also called serializable snapshot isolation).

   Readers see one snapshot throughout their query.
   Writers never see partial writes.
   Writes are atomic: either the snapshot is committed, or not.
```

#### 15.2 Optimistic Concurrency Control

```
Writers don't lock. They:
   1. Read current metadata (snapshot = N)
   2. Compute new data files + manifests + manifest list + metadata (N+1)
   3. Catalog CAS: swap pointer from N to N+1
   
If another writer committed N+1 first, our CAS fails.
We retry:
   1. Re-read current metadata (now = N+1)
   2. Re-compute merges + commits on top of N+1
   3. CAS N+1 → N+2
```

#### 15.3 Conflict Classification

```
CONFLICTS depend on operation type:

APPEND vs APPEND: No conflict. Both can commit.
APPEND vs DELETE: No conflict (deletes don't affect new rows).
APPEND vs OVERWRITE: Conflict if same partitions touched.
DELETE vs DELETE: Usually no conflict (idempotent).
UPDATE vs UPDATE on same row: Conflict; retry.
OVERWRITE vs anything on same partition: Conflict.

Iceberg allows configurable "conflict detection filters" to narrow
when retries actually need to happen.
```

#### 15.4 The Write Isolation Levels

```
ALTER TABLE orders SET TBLPROPERTIES (
    'write.isolation-level' = 'serializable'      -- strongest (default)
    -- OR 'snapshot' for weaker (allows some concurrent conflicts)
);
```

#### 15.5 Streaming Writes

```
Streaming writers (Flink, Spark Structured Streaming) commit micro-batches
as snapshots. 

Flink commits ~ every checkpoint interval (e.g., 60 seconds).
Each commit = one new snapshot.

For very high write rates, snapshot count grows fast — monitor it.
```

> **🧠 Principal Mindset:** Iceberg's optimistic concurrency works well when the common case is non-conflicting (appends to different partitions). It struggles under pathological patterns — multiple writers hammering the same partition with overwrites. Design your write patterns to avoid conflicts: shard by partition, serialize overwrites, batch updates. Don't fight the model; design to it.

---

## PART V — CATALOGS

### 16. The Catalog Protocol

#### 16.1 What the Catalog Does

```
Minimum catalog interface:

   createNamespace(ns)
   dropNamespace(ns)
   listNamespaces()
   
   createTable(name, schema, spec, properties)
   dropTable(name)
   listTables(ns)
   
   loadTable(name) → metadata pointer
   commitTable(name, oldMetadata, newMetadata) → atomic swap
```

The atomic swap (compare-and-set) is the KEY primitive. It serializes concurrent writes to the same table.

#### 16.2 Why the Catalog Matters

```
The catalog is the only CONSISTENT STATE in the Iceberg stack.
Everything else (metadata files, data files) is immutable once written.

The catalog's job: hold the pointer; swap it atomically.

If the catalog is unavailable, writes block (can't commit).
If the catalog is slow, latency is dominated by catalog ops.
If the catalog is inconsistent, Iceberg's guarantees break.
```

### 17. Hive, Hadoop, Nessie, REST

#### 17.1 Hive Metastore Catalog

```
Oldest Iceberg catalog implementation.
Uses Hive Metastore (HMS) as the pointer store.

PROS:
   ✓ Mature; most engines support it
   ✓ Integrates with existing Hive tooling

CONS:
   ✗ HMS is a database (MySQL/Postgres) — operational overhead
   ✗ Limited to Hive's locking semantics
   ✗ No multi-table transactions
```

#### 17.2 Hadoop Catalog

```
Pointer stored as a file in the filesystem (e.g., v123.metadata.json).
Latest file is the current version.

PROS:
   ✓ No external service
   ✓ Simple

CONS:
   ✗ Depends on atomic rename at the filesystem level
   ✗ S3 doesn't guarantee this (strong consistency arrived 2020, but not atomic rename)
   ✗ Risky in cloud object stores
   ✗ Hard to list all tables
```

Not recommended for production S3-based systems.

#### 17.3 Nessie Catalog

```
Project Nessie (Dremio): git-like catalog with branches and tags.

PROS:
   ✓ Branch for experimentation (dev/staging/prod branches)
   ✓ Reproducible query via commit hash
   ✓ Rollback by moving a branch pointer

CONS:
   ✗ Smaller ecosystem than REST / Polaris
   ✗ Specific use case (data versioning)
```

Good fit for teams who need git-like data workflows.

#### 17.4 REST Catalog

```
The REST Catalog is a SPEC — any service implementing it can be a catalog.

HTTP endpoints:
   GET  /v1/{prefix}/namespaces
   POST /v1/{prefix}/namespaces/{ns}/tables
   GET  /v1/{prefix}/namespaces/{ns}/tables/{tbl}
   POST /v1/{prefix}/namespaces/{ns}/tables/{tbl}/commit
   ...

Implementations:
   • Polaris (Apache)
   • Unity Catalog (Databricks)
   • Lakekeeper
   • Snowflake Iceberg Catalog
   • Many others

PROS:
   ✓ Vendor-neutral protocol
   ✓ Any engine supporting REST catalog works with any implementation
   ✓ HTTP = easy to firewall, load-balance, authenticate

CONS:
   ✗ Requires a service to run
   ✗ Added latency vs direct metastore access (usually negligible)
```

By 2026 the REST catalog is the de facto standard for new deployments.

### 18. Polaris and Unity Catalog

#### 18.1 Apache Polaris

```
Open-source Iceberg catalog, originally donated by Snowflake (June 2024).
Graduated to Apache top-level project (February 2026).

Features:
   • Iceberg REST catalog API
   • RBAC (fine-grained permissions)
   • Multi-engine integrations
   • Credential vending (short-lived STS tokens per engine)
   • Namespaces and roles

Deployment:
   • Self-hosted Polaris server
   • OR Snowflake's hosted Polaris (as Snowflake Open Catalog)
   
   Backing store: Postgres or similar.
```

Polaris is positioned as the vendor-neutral choice. If you want to be portable across Snowflake, Databricks, Spark, Trino, DuckDB, and others — Polaris is a solid default.

#### 18.2 Unity Catalog (Open Source)

```
Databricks's catalog; open-sourced June 2024 under Apache 2.0.

Features:
   • Multi-format: Delta, Iceberg, Hudi (reads)
   • Table-level, column-level, row-level security
   • Lineage tracking (table-to-table and column-to-column)
   • Audit logging
   • Volume support (unstructured data registration)

Deployment:
   • Databricks-hosted (SaaS)
   • Self-hosted (open-source Unity Catalog server)
```

Unity's strength is governance features (lineage, column-level masking). Weakness vs Polaris: historically Databricks-centric; multi-engine support still catching up.

#### 18.3 Polaris vs Unity

| Feature | Polaris | Unity Catalog |
|---------|---------|---------------|
| **Origin** | Snowflake | Databricks |
| **Protocol** | Iceberg REST | Iceberg REST + Delta Sharing + Unity-specific |
| **Multi-engine** | Mature | Growing |
| **Governance depth** | RBAC | RBAC + lineage + masking + audit |
| **Deployment** | Self-host or Snowflake-hosted | Self-host or Databricks-hosted |
| **Backing** | Apache, multi-vendor | Apache, Databricks-led |
| **License** | Apache 2.0 | Apache 2.0 |

#### 18.4 AWS Glue, Google Dataplex, Azure Purview

```
Cloud-managed catalogs:
   • AWS Glue — mature; AWS integration tight
   • Google Dataplex — Iceberg support (growing)
   • Azure Purview — multi-cloud governance

All now support Iceberg to varying degrees.

Trade-off: cloud lock-in vs operational simplicity.
```

### 19. Catalog HA and Operations

#### 19.1 Catalog is a Single Point of Failure

```
If catalog is down, Iceberg writes fail.
Reads can continue if clients cache metadata, but new tables / new snapshots require catalog.

Operational requirements:
   • High availability (multi-region or clustered)
   • Low-latency reads (< 100 ms per catalog call)
   • Backup / restore procedures
   • Monitoring and alerting
```

#### 19.2 Scaling the Catalog

```
Most catalogs are stateless services backed by a database (Postgres typical).

Horizontal scaling:
   Multiple catalog service pods
   Shared Postgres (vertical scale) or sharded (horizontal)
   Load balancer in front

Caching:
   Clients cache metadata between reads
   Reduces catalog load for read-heavy workloads
```

#### 19.3 Catalog Migration

```
Moving from catalog A to catalog B:
   1. Create namespaces in B
   2. For each table in A:
      - Read metadata pointer from A
      - Register table in B (point to same metadata)
      - Verify consistency
   3. Switch engines to use B
   4. Deprecate A

Iceberg's spec-driven design makes this relatively clean —
the data and metadata don't move, only the pointer.
```

#### 19.4 Monitoring

```
Catalog metrics to track:
   • QPS (reads, commits)
   • Commit success rate
   • Commit retry rate (indicates conflicts)
   • P50/P95/P99 latency
   • Database connection pool health
   • Error rates by table / namespace
```

> **🔥 Production Scenario:** A team ran AWS Glue as their Iceberg catalog. Glue had a 3-hour partial outage. Writes failed; reads mostly worked (cached metadata). After this, they moved to self-hosted Polaris with multi-AZ Postgres backing. Catalog outages dropped from quarterly to never-measured. Lesson: catalog is the hot spot; design for it.

---

## PART VI — MAINTENANCE

### 20. The Small Files Problem

#### 20.1 How Small Files Accumulate

```
Streaming writes commit frequently → many small files
Partition-heavy writes with low cardinality → few rows per partition
MERGE operations rewrite partially → fragmentation

Over time:
   • File count grows faster than data size
   • Each query opens 10-100x more files than needed
   • Metadata overhead dominates query planning
   • Engine CPU spent opening/closing files instead of reading data
```

#### 20.2 The Impact

```
Example: table with 1 TB data split into 1 million 1 MB files
vs: same 1 TB in 1,000 1 GB files

Query performance:
   1M files: planning time ~minutes, scan time dominated by file open overhead
   1K files: planning time ~seconds, scan time proportional to data read

Cost in S3 GET charges:
   1M files: $4 per full-table scan (just in GETs)
   1K files: cents

Rule of thumb: target 128 MB - 1 GB per file.
```

### 21. Compaction Strategies

#### 21.1 Data File Compaction

```sql
-- Combine small files into larger ones
CALL system.rewrite_data_files(
    table => 'db.orders',
    options => map(
        'target-file-size-bytes', '536870912',   -- 512 MB
        'min-file-size-bytes',    '134217728'    -- compact files < 128 MB
    )
);
```

Modes:
```
BIN-PACKING (default):
   Just combine small files, no sort
   Fastest; works for any workload

SORT:
   Combine + sort by specified columns
   Enables tight pruning on sort columns

ZORDER:
   Combine + Z-order by multiple columns
   Enables multi-column pruning
```

#### 21.2 When to Compact

```
Triggers:
   • Count of small files > N
   • Average file size < target
   • Period: nightly or weekly
   • Partition-specific: when a partition's files fragment

Scheduling patterns:
   • Nightly compaction job
   • Hourly for streaming-heavy tables
   • On-demand triggered by alerts
```

#### 21.3 Manifest Rewriting

```sql
-- Combine many small manifests into fewer
CALL system.rewrite_manifests('db.orders');
```

Reduces planning time when manifest count has grown.

#### 21.4 Partition-Specific Compaction

```sql
-- Only compact a specific partition
CALL system.rewrite_data_files(
    table => 'db.orders',
    where => 'event_date = ''2024-03-15'''
);
```

Useful for streaming scenarios where only recent partitions need compaction.

### 22. Snapshot Expiry and Orphan Files

#### 22.1 Snapshot Expiry

```sql
-- Expire snapshots older than threshold
CALL system.expire_snapshots(
    table => 'db.orders',
    older_than => TIMESTAMP '2024-01-01',
    retain_last => 10
);
```

After expiry:
```
• Can no longer time-travel to those snapshots
• Data files referenced ONLY by expired snapshots are candidates for deletion
```

#### 22.2 Orphan Files

```
Data files written during failed commits may not be referenced by any snapshot.
These are "orphan files" — storage cost without benefit.

Cleanup:
   CALL system.remove_orphan_files(
       table => 'db.orders',
       older_than => TIMESTAMP '2024-01-01'
   );
```

The `older_than` threshold is important — don't delete files currently being written.

#### 22.3 Deleting Underlying Files

```sql
-- Delete data files that are no longer referenced
-- (usually runs after expire_snapshots)
CALL system.remove_orphan_files('db.orders');
```

Note: expire_snapshots alone does NOT delete data files. Only the metadata reference is removed. The storage cleanup is separate.

#### 22.4 Operational Playbook

```
WEEKLY MAINTENANCE:
   1. Compact data files (small file consolidation)
   2. Rewrite manifests (metadata consolidation)
   3. Expire old snapshots (keep 7-30 days typically)
   4. Remove orphan files (reclaim storage)

MONITORING:
   • Snapshot count (should stabilize after retention policy)
   • Average file size (should match target)
   • Total metadata size (shouldn't dominate data size)
   • Orphan file count (should trend to zero after cleanup)
```

### 23. Table Properties Reference

Common properties:

```sql
ALTER TABLE orders SET TBLPROPERTIES (
    -- Format
    'format-version'                       = '2',
    
    -- File writing
    'write.format.default'                 = 'parquet',
    'write.target-file-size-bytes'         = '536870912',   -- 512 MB
    'write.parquet.compression-codec'      = 'zstd',
    'write.parquet.row-group-size-bytes'   = '134217728',   -- 128 MB
    'write.parquet.page-size-bytes'        = '1048576',     -- 1 MB
    
    -- Delete file strategy
    'write.delete.mode'                    = 'merge-on-read',  -- or 'copy-on-write'
    'write.update.mode'                    = 'merge-on-read',
    'write.merge.mode'                     = 'merge-on-read',
    
    -- Sort / distribution
    'write.distribution-mode'              = 'hash',  -- or 'range', 'none'
    
    -- Snapshot retention
    'history.expire.max-snapshot-age-ms'   = '604800000',    -- 7 days
    'history.expire.min-snapshots-to-keep' = '10',
    
    -- Metadata size
    'write.metadata.compression-codec'     = 'gzip',
    'write.metadata.delete-after-commit.enabled' = 'true',
    'write.metadata.previous-versions-max' = '100',
    
    -- Isolation
    'write.isolation-level'                = 'serializable',
    
    -- Pruning
    'read.split.target-size'               = '134217728'
);
```

---

## PART VII — MULTI-ENGINE

### 24. Engine Interop

#### 24.1 Spark

```sql
-- Spark + Iceberg: the reference implementation
spark.sql("""
    CREATE TABLE catalog.db.orders (id BIGINT, ts TIMESTAMP, amount DECIMAL)
    USING ICEBERG
    PARTITIONED BY (days(ts))
""")

spark.sql("INSERT INTO catalog.db.orders VALUES (...)")
spark.sql("SELECT * FROM catalog.db.orders WHERE ts > '2024-03-01'")
```

Supports: full DDL, MERGE, time travel, stored procedures (CALL).

#### 24.2 Flink

```java
// Flink Iceberg sink
dataStream.sinkTo(IcebergSink.builderForRow(...)
    .table(table)
    .writeFormat(FileFormat.PARQUET)
    .build()
);
```

Streaming writes commit every checkpoint. Suitable for 10-second to minute-level freshness.

#### 24.3 Trino

```sql
-- Trino with Iceberg connector
SELECT * FROM iceberg.db.orders WHERE ts > TIMESTAMP '2024-03-01';

-- Supports CTAS, INSERT, DELETE, UPDATE, MERGE
INSERT INTO iceberg.db.orders VALUES (...);
```

Trino is a leading query engine for federated Iceberg; often paired with Polaris.

#### 24.4 DuckDB

```sql
INSTALL iceberg;
LOAD iceberg;

ATTACH 'rest' AS lake (
    TYPE ICEBERG,
    ENDPOINT 'https://polaris.example.com/',
    WAREHOUSE 'my_warehouse'
);

SELECT * FROM lake.db.orders;
```

DuckDB can read Iceberg; writes (DuckDB 1.4+) support append and basic MERGE.

#### 24.5 Snowflake

```sql
-- External Iceberg table (read-only or writable)
CREATE ICEBERG TABLE orders
    EXTERNAL_VOLUME = 'my_vol'
    CATALOG = 'polaris'
    CATALOG_TABLE_NAME = 'db.orders';

-- Query like any Snowflake table
SELECT * FROM orders;
```

Snowflake supports both external (read mostly) and managed (writes) Iceberg tables.

#### 24.6 BigQuery

```sql
-- BigLake Iceberg tables
CREATE TABLE my_dataset.orders (...)
WITH CONNECTION ...
OPTIONS (table_format = 'ICEBERG', ...);

SELECT * FROM my_dataset.orders;
```

#### 24.7 PyIceberg (Python Native)

```python
from pyiceberg.catalog import load_catalog

catalog = load_catalog("polaris", **{"type": "rest", "uri": "https://..."})
table = catalog.load_table("db.orders")

# Read as Arrow / pandas / Polars
arrow_table = table.scan().to_arrow()
df = table.scan().to_pandas()
polars_df = table.scan().to_polars()

# Write
table.append(new_df)
```

PyIceberg (2024-2026 maturing fast) enables Python-native Iceberg work without JVM.

### 25. UniForm and Delta Interop

#### 25.1 What UniForm Does

```
Delta UniForm (introduced 2023, matured 2024-2025):
   
   When you write a Delta table, UniForm ALSO writes Iceberg metadata
   pointing to the same Parquet files.
   
   Result:
      Delta writers (Databricks) continue writing Delta.
      Iceberg readers (Snowflake, BigQuery, Trino) read the same data
      via the Iceberg metadata UniForm emits.
```

#### 25.2 Why It Matters

```
UniForm is Databricks's response to the Iceberg momentum:
   "You don't have to pick. Write Delta; read Iceberg."

For teams:
   ✓ Keep Databricks as the primary platform
   ✓ But let other engines (Snowflake, BigQuery) read the data as Iceberg
   ✓ No format migration required
```

#### 25.3 The Caveats

```
UniForm emits Iceberg metadata ASYNCHRONOUSLY.
Iceberg readers may lag behind Delta writers by seconds to minutes.

Some Delta features don't fully translate to Iceberg:
   • Liquid Clustering (Delta-specific)
   • Deletion vectors (translated via Iceberg V3)
   • Delta-specific stats blobs

For greenfield: pick one format as source of truth.
For brownfield migration: UniForm bridges gracefully.
```

### 26. Streaming Writes

#### 26.1 Flink + Iceberg

```
Flink is the production streaming engine for Iceberg.

Checkpoint interval controls commit frequency:
   Short (1-10 sec): low latency, many snapshots, more compaction needed
   Medium (30-60 sec): balanced
   Long (5 min): fewer snapshots, higher freshness latency
```

#### 26.2 Spark Structured Streaming + Iceberg

```scala
val query = spark.readStream
    .format("kafka")
    .option(...)
    .load()
    .selectExpr(...)
    .writeStream
    .format("iceberg")
    .option("checkpointLocation", "s3://...")
    .toTable("catalog.db.orders")
```

#### 26.3 The Streaming Lakehouse Pattern

```
Kafka (events) 
   → Flink (processing, enrichment)
   → Iceberg (silver layer with ACID)
   → Spark / dbt (gold layer aggregations)
   → BI / warehouse readers

Benefits:
   ✓ Single storage layer for streaming + batch
   ✓ Downstream consumers choose their engine
   ✓ Time travel for debugging
```

#### 26.4 Operational Considerations

```
STREAMING WRITES CREATE MANY SMALL FILES:
   • Each checkpoint commits new files
   • Minute-level checkpoints → thousands of files per day per table
   • Compaction MUST run regularly

TUNING:
   • target-file-size vs checkpoint frequency
   • Use "roll-over" writers that batch across checkpoints
   • Separate hot (recent) and cold (compacted) partitions
```

> **🔥 Production Scenario:** A team ran Flink + Iceberg with 10-second checkpoints on a high-volume topic. After 2 months: 15 million small files, planning time climbed to minutes, S3 LIST charges hit $500/day. They moved to 60-second checkpoints + hourly compaction. File count stabilized at ~500K, planning dropped to seconds, LIST costs fell to pennies. Lesson: streaming + Iceberg demands active compaction; it's not a "set and forget" architecture.

---

## PART VIII — INTERVIEW PRACTICE

### 27. The Staff-Level Iceberg Interview

#### 27.1 Common Questions

```
1. "Walk me through what happens when I INSERT a row into an Iceberg table."
2. "Why does Iceberg scale better than Hive-style tables?"
3. "Compare Iceberg vs Delta Lake. When would you pick each?"
4. "Design the catalog layer for a 100-TB Iceberg deployment."
5. "A query that was fast is now slow. Where do you look?"
6. "How does Iceberg handle concurrent writes?"
7. "What's the small files problem and how do you solve it?"
8. "Explain Iceberg's partition evolution. When would you use it?"
9. "How do you migrate from Hive tables to Iceberg?"
10. "What are deletion vectors and why do they matter in V3?"
```

#### 27.2 Worked Example: Deep Dive

```
Q: "A COUNT(*) on a 100 TB Iceberg table returns in 50 ms. How?"

STAFF ANSWER:
   Three layers of magic:
   
   (1) The MANIFEST metadata itself tracks record_count per data file.
       COUNT(*) = SUM of record_counts across all data files
       referenced by the current snapshot's manifests.
       No data files need to be opened.
   
   (2) The MANIFEST LIST summarizes record counts per manifest, so
       even without opening all manifests, the sum can be computed
       from the manifest list alone — one file read.
   
   (3) Engines like Spark, Trino, DuckDB have specific optimizations
       to detect `SELECT COUNT(*)` and short-circuit to this path.
   
   Delta Lake has similar capability via its transaction log.
   Hive-style tables would require listing all files and reading
   Parquet footers — orders of magnitude slower.
```

#### 27.3 Worked Example: Design Problem

```
Q: Design the Iceberg layer for a 10-team org, 500 TB total,
   mixed batch + streaming, with Snowflake + Spark + Flink + Trino as engines.

STAFF ANSWER:

CATALOG: Apache Polaris (self-hosted, 3 pods, Postgres multi-AZ).
   • Open REST protocol → all 4 engines read natively
   • RBAC per namespace → team isolation
   • Short-lived STS credentials via credential vending

STORAGE: S3 with intelligent tiering.
   • Hot (last 30 days): Standard
   • Warm (30-180 days): Standard-IA
   • Cold (>180 days): Glacier Instant

NAMESPACE STRUCTURE:
   catalog
   ├── raw           (bronze, append-only)
   ├── team_a_silver (per-team silver)
   ├── team_b_silver
   ├── team_a_gold   (per-team marts)
   ├── shared_gold   (cross-team definitions)
   └── sandbox       (team playgrounds)

PARTITIONING:
   • Fact tables: hidden partition by days(event_ts)
   • Dim tables: no partitioning (small, broadcast-friendly)
   • Partition evolution supported but prefer stability

FORMAT VERSION: V2 by default, V3 for new tables where deletion
   vectors matter (CDC-heavy tables).

WRITERS:
   • Flink: streaming CDC with 60-second checkpoints
   • Spark: batch ETL via dbt-spark adapter
   • Snowflake: writes to specific per-team namespaces for serving layer

COMPACTION:
   • Nightly compact job (Spark) per team's silver
   • Hourly for streaming tables (small file prevention)
   • Weekly manifest rewrite

RETENTION:
   • 30 days snapshot history (standard)
   • 7 days for high-churn tables (CDC)
   • Permanent for audit-sensitive (manual expiry only)

MONITORING:
   • Catalog metrics: QPS, latency, retries
   • Table metrics: file counts, snapshot counts, size
   • Alerts: file count > N, planning time > N sec, failed commits

COST ESTIMATE:
   • S3 storage: $8K/month (500 TB with tiering)
   • Catalog infra: $500/month (3 pods + Postgres RDS)
   • Compaction compute: $2K/month (nightly Spark EMR)
   • Total ongoing: ~$10K/month beyond engine compute

MIGRATION:
   • From existing Hive tables: use Iceberg's MIGRATE procedure
   • Preserves existing Parquet files; adds Iceberg metadata
   • Zero data duplication; incremental migration per table

OFF-RAMPS:
   • Open format: data stays in S3 as Parquet
   • Can switch catalogs with metadata-pointer migration
   • Can read from any Iceberg-compatible engine
```

#### 27.4 The Senior-vs-Staff Table

| Senior Answer | Staff Answer |
|---------------|--------------|
| "Iceberg uses manifests to find data." | "Iceberg's three-layer metadata (metadata file → manifest list → manifest → data files) lets planners prune at each level. For a 1 PB table with 100K files, planning typically opens ~10 metadata files and reads ~50 MB before touching any data. This is why queries scale to PB — the planning cost is logarithmic in data size, not linear." |
| "Pick Iceberg for multi-engine." | "Pick Iceberg when vendor neutrality or multi-engine access matters more than Databricks-specific features. Delta UniForm has closed some of the multi-engine gap, so if you're all-Databricks for writes, UniForm plus Iceberg reads is viable. For greenfield I default to Iceberg + Polaris because the write/read optionality compounds over 5+ year horizons. The decision is organizational commitment, not technical capability." |
| "Compact small files." | "The compaction strategy depends on the write pattern. For append-heavy tables, nightly bin-pack. For streaming (10-sec commits), hourly compaction. For MERGE-heavy, use the sort strategy with the primary filter column. Also rewrite manifests separately — manifest fragmentation is subtle but kills planning performance. Measure file count and average file size as leading indicators, not just query latency." |
| "Use REST catalog." | "The REST catalog is the right default in 2026 because it's the vendor-neutral protocol. Pick an implementation based on governance needs: Polaris if multi-engine neutrality is critical; Unity Catalog if lineage and column masking matter; Glue/Dataplex if cloud-native fit is paramount. Avoid Hadoop filesystem catalog in cloud — the atomic rename assumption breaks on S3." |

### 28. War Stories

> **🔥 Production Scenario — The MERGE Explosion.** A team had an Iceberg table with 500 GB of data. They ran a MERGE operation that matched 10% of rows. Expected: update 50 GB. Actual outcome: wrote 500 GB of new data files + massive position delete files. Investigation: MERGE in copy-on-write mode rewrites entire data files touched by any update. Switching to `write.merge.mode = 'merge-on-read'` cut write amplification 10x. Lesson: COPY-ON-WRITE vs MERGE-ON-READ is a critical setting; defaults vary by engine.

> **🔥 Production Scenario — The Snapshot Explosion.** A Flink job wrote to an Iceberg table every 10 seconds for 3 months without snapshot expiry. Snapshot count: 777,600. Metadata file size: 2.5 GB. Loading the table took minutes. Fix: ran expire_snapshots with 7-day retention; metadata size dropped to 20 MB. Lesson: retention policies are not optional for streaming writers.

> **🔥 Production Scenario — The Hidden Partition Save.** A team was debugging slow queries on a Hive-style table partitioned by `date_str` (string). Queries used `WHERE event_ts BETWEEN '2024-03-01' AND '2024-03-15'`. Hive couldn't prune because the predicate was on event_ts, not date_str. Migration to Iceberg with hidden `days(event_ts)` partition: queries dropped from 20 minutes to 15 seconds. Lesson: hidden partitioning solves the predicate-mismatch problem that plagues Hive tables.

> **🔥 Production Scenario — The Catalog Failover.** A team ran AWS Glue as their Iceberg catalog. Glue had a 3-hour partial outage. Writes failed; reads mostly worked (cached metadata). After this they moved to self-hosted Polaris with multi-AZ Postgres backing. Catalog outages dropped from quarterly to never. Lesson: catalog is the hot spot in the Iceberg stack; design for HA.

> **🔥 Production Scenario — The Schema Rename.** A team renamed a column on a 50 TB production Iceberg table. Operation completed in 20 milliseconds (metadata-only). The equivalent on their previous Hive table would have required a 14-hour rewrite job costing $800. Over the next quarter they did 40 similar schema changes; cumulative time saved was weeks of engineering effort. Lesson: schema evolution is more frequent than you think; Iceberg's field-ID design pays off continuously.

> **🔥 Production Scenario — The UniForm Lag.** A team used Databricks for writes (Delta + UniForm) and Snowflake for reads (external Iceberg). They assumed real-time sync but discovered that UniForm's Iceberg metadata could lag Delta writes by up to 10 minutes under heavy load. Dashboards occasionally showed stale data. Fix: added a monitoring query comparing Delta's commit timestamp to UniForm's generated Iceberg snapshot timestamp; alerted when drift exceeded 5 minutes. Lesson: UniForm bridges formats but is NOT synchronous; design for the lag.

### 29. Cheat Sheet & Mental Models

#### 29.1 The One-Minute Pitch

```
Apache Iceberg = open table format for data lakes.
   • ACID on object storage (atomic snapshot commits)
   • Hidden partitioning (predicate-smart, user-friendly)
   • Schema evolution via field IDs (no data rewrites)
   • Three-layer metadata (metadata → manifest list → manifest → data)
   • Pluggable catalogs (Polaris, Unity, Glue, Hive, REST)
   • Multi-engine: Snowflake, BQ, Databricks, Spark, Flink, Trino, DuckDB

Pick for: vendor neutrality, multi-engine access, long-term storage.
Avoid when: Databricks-centric write side + no multi-engine need.
```

#### 29.2 The Four Lenses — Compressed

```
CONCEPT
   Table format spec. Metadata atop files. ACID via atomic pointer swap.

MECHANICS
   Three layers: metadata.json → manifest list → manifests → data files.
   Field IDs for schema evolution. Partition transforms for hidden partitioning.
   V2 for row-level deletes; V3 for deletion vectors + variant + geometry.

TRADE-OFFS
   Pros: open, multi-engine, mature, best-in-class metadata.
   Cons: operational complexity (catalog, compaction, snapshots).
   Not for: OLTP, high-concurrency writes, non-analytical workloads.

INTERVIEW
   Know: 3-layer metadata, hidden partitioning, V2 vs V3 deletes,
   catalog options, compaction strategy, UniForm interop.
   Defend: Iceberg vs Delta vs Hudi decision; catalog choice.
```

#### 29.3 The Essential Commands

```sql
-- Create
CREATE TABLE catalog.db.orders (id BIGINT, ts TIMESTAMP, amount DECIMAL)
USING ICEBERG
PARTITIONED BY (days(ts))
TBLPROPERTIES ('format-version' = '2');

-- Query
SELECT * FROM catalog.db.orders WHERE ts > '2024-01-01';

-- Time travel
SELECT * FROM catalog.db.orders FOR VERSION AS OF 12345;
SELECT * FROM catalog.db.orders FOR TIMESTAMP AS OF '2024-03-15';

-- MERGE
MERGE INTO orders tgt USING updates src ON tgt.id = src.id
WHEN MATCHED THEN UPDATE SET ...
WHEN NOT MATCHED THEN INSERT ...;

-- Maintenance
CALL system.rewrite_data_files('db.orders');
CALL system.rewrite_manifests('db.orders');
CALL system.expire_snapshots('db.orders', TIMESTAMP '...', 10);
CALL system.remove_orphan_files('db.orders');

-- Schema evolution
ALTER TABLE orders ADD COLUMN discount DECIMAL;
ALTER TABLE orders RENAME COLUMN amount TO total_amount;
ALTER TABLE orders ALTER COLUMN total_amount TYPE DECIMAL(18, 4);

-- Partition evolution
ALTER TABLE orders ADD PARTITION FIELD hours(ts);
ALTER TABLE orders DROP PARTITION FIELD days(ts);
```

#### 29.4 Mental Model

```
Think of Iceberg as:
   • A versioned database on top of Parquet files
   • A spec, not a system
   • The industry-standard open table format for 2024+
   • The "safe default" for lakehouse storage
   • A metadata tree pointing to immutable data files

When to reach for it:
   • New lakehouse project
   • Multi-engine access required
   • Open-format storage mandate
   • Need schema / partition evolution
   • Petabyte scale with query planning performance

When to reach past it:
   • OLTP or sub-second serving
   • Spark-only, Databricks-locked (Delta is fine)
   • Record-level upserts at TB/day (Hudi may fit)
   • Need lakehouse metadata simplicity (DuckLake)
```

> **🧠 Principal Mindset:** Iceberg's success teaches a durable lesson: open protocols beat closed ones over long time horizons. In 2019, Delta looked unstoppable with Databricks's momentum. By 2024, Iceberg had won the multi-vendor layer because its spec was open and its ecosystem was multi-engine from inception. If you're betting on a storage format for 10 years, bet on the open one. The pattern holds beyond Iceberg: Postgres over proprietary RDBMS, Kubernetes over proprietary orchestrators, Parquet over proprietary columnar formats. Staff engineers who recognize this pattern bet right more often.

---

[← Back to main](./README.md)
