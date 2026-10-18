# Topic 6 — Lakehouses (Delta / Iceberg / Hudi)

> Section 6 of 10 · 58 pages in source PDF · pages 334–391

[← Back to main](./README.md)

---

Lakehouses Architect-Level Reference Principal / Staff Engineer Interview Notes Delta · Iceberg · Hudi · ACID on S3 · MERGE · Time Travel · Catalogs · Compaction · Migration · Open Table Format Wars How databases became files — and how to architect on top.

How to use this document Lakehouses are the most consequential architectural shift in data engineering since the introduction of cloud warehouses. They merge the cheap storage of data lakes (S3, GCS) with the transactional guarantees of warehouses, by introducing open table formats: Delta Lake, Apache Iceberg, Apache Hudi. At staff level you're expected to understand their internals, pick the right one for the workload, and architect a migration without breaking production. ■ Principal mindset: Three staff-level signals: (1) you can explain how a table format gives ACID on object storage, and why the metadata layer is the magic; (2) you can compare Delta vs Iceberg vs Hudi by workload not by hype; (3) you've thought about catalog choice, file layout, compaction strategy, and the operational realities — not just SELECT performance.

Table of Contents

## PART I — THE LAKEHOUSE CONCEPT

#### 1. From Data Lake to Lakehouse

#### 3. The Open Table Format

#### 4. Catalogs: The Underrated Layer

## PART II — DELTA LAKE

#### 5. Delta Architecture & Transaction Log

#### 6. Delta Operations: MERGE, OPTIMIZE, Z-ORDER

#### 7. Time Travel, Vacuum & CDC Feed

## PART III — APACHE ICEBERG

#### 8. Iceberg Architecture & Metadata Tree

#### 9. Hidden Partitioning & Schema Evolution

#### 10. Iceberg Catalogs: REST, Glue, Nessie, Polaris

## PART IV — APACHE HUDI

#### 11. Hudi Architecture: CoW vs MoR

#### 12. Hudi's Upserts, Indexes & Timeline

## PART V — COMPARISON & PICKING

#### 13. Feature-by-Feature Comparison

#### 14. Performance & Cost Characteristics

#### 15. The Open Table Format Wars (2024-2026)

## PART VI — OPERATIONAL CONCERNS

#### 16. File Layout, Sizing & Compaction

#### 17. Streaming Writes & Concurrent Writers

#### 18. Multi-Engine Access: Spark, Trino, Snowflake, BigQuery

#### 19. Migration Patterns & Anti-Patterns

## PART VII — INTEGRATIONS & ECOSYSTEM

#### 20. Lakehouse + dbt

#### 21. Lakehouse + Streaming (Kafka, Flink, Spark Streaming)

#### 23. Governance: Permissions, Quality, Lineage

## PART VIII — INTERVIEW PRACTICE

#### 24. The Staff-Level Lakehouse Interview

#### 25. War Stories

#### 26. Cheat Sheet & Mental Models

## PART I — THE LAKEHOUSE CONCEPT

#### 1. From Data Lake to Lakehouse

```
1.1 The History
1990s-2000s   DATA WAREHOUSE
              Tightly coupled storage + compute (Teradata, Vertica, Oracle)
              Strong ACID, SQL, governance.
              Expensive, slow to scale, schema-on-write.
2010-2015     DATA LAKE
              Cheap object storage (S3, HDFS) + flexible schema-on-read.
              Hadoop / Spark / Hive. Parquet/ORC files in directories.
              No ACID, no atomic operations, no row-level updates.
              "Data swamp" became the cliché for what this produced.
2015-2018     CLOUD DATA WAREHOUSE
              Snowflake, BigQuery, Redshift.
              Separated storage and compute, kept ACID + SQL.
              Proprietary storage formats, vendor lock-in.
2019-now      LAKEHOUSE
              ACID on top of open file formats on object storage.
              Open table formats: Delta (2019), Iceberg (2018, broader 2020+),
              Hudi (2017). All open-sourced.
              Multi-engine reads, decoupled storage and compute.
              The "best of both worlds" pitch.
```

```
1.2 What's Wrong with a Raw Data Lake
PARQUET FILES IN S3 DIRECTORIES — what you can't do:
  ✗ ATOMIC OPERATIONS
    Writer crashes mid-write → partial files visible
    Concurrent writers overwrite each other
    Readers see inconsistent state during writes
  ✗ ROW-LEVEL UPDATES / DELETES
    "Delete user X" = rewrite every file containing user X
    "Update price for product Y" = same problem
  ✗ SCHEMA EVOLUTION
    Add a column = old files don't have it, queries break
    Rename a column = nothing knows about the rename
  ✗ TIME TRAVEL
    Want yesterday's state? Hope you kept a backup.
  ✗ TRANSACTIONS
    No way to atomically write multiple files
  ✗ EFFICIENT FILTERING
    Without statistics, queries scan everything in a partition
The LAKEHOUSE adds a METADATA LAYER on top that solves all of these
without sacrificing the cheap-storage advantage.
```

```
1.3 What a Lakehouse Adds
TRANSACTION LOG / METADATA LAYER:
   ACID: atomic commits via metadata file replacement
   Versioning: every commit is a new metadata file; old ones retained
   Schema: stored centrally, enforced on write
   Statistics: file-level min/max/null counts for skipping
   File-level operations: "this commit adds files X,Y and removes Z"
THE PHYSICAL DATA STAYS PARQUET:
   Compatible with every existing tool (eventually)
   Open format, no lock-in
   Cheap to store
THE METADATA IS THE INNOVATION:
   Delta: JSON log files in _delta_log/
   Iceberg: tree of JSON + Avro metadata files
   Hudi: timeline of action logs
```

```
1.4 The Lakehouse Promise
ONE STORAGE LAYER FOR EVERYTHING:
   BI analytics (Trino, Spark, Snowflake reading external)
   ML training (Spark, Pandas, Polars)
   Streaming (Flink, Spark Structured Streaming)
   Operational (some serving from Iceberg/Delta directly)
OPEN FORMATS, BYO COMPUTE:
  Store in S3 → query with whatever engine fits the workload
  No vendor lock-in
  Storage costs ~$0.023/GB/month (S3 standard) vs $40-100/TB/month warehouse-tier
DECOUPLED SCALING:
  Storage and compute scale independently
  Multiple compute clusters can read the same data concurrently
■ Interview angle: Q: What's the difference between a data lake and a lakehouse?
Staff: The lake is the storage layer — Parquet files in S3, no guarantees beyond 'these files exist'. The lakehouse
adds a metadata layer (transaction log) on top of those files that provides ACID transactions, schema enforcement,
time travel, and row-level operations. The data files are still Parquet — what changed is that there's now a manifest
telling you which exact files compose 'the table' at any given commit. Concurrent writers coordinate via the
metadata; readers get a consistent snapshot. The big deal: you get warehouse-like semantics on cheap object
storage, with open formats that any engine can read.
```

2. ACID on Object Storage: How?

```
2.1 The Object Storage Reality
S3 / GCS / AZURE BLOB — what they actually provide:
  ✓ PUT object — atomic single-key write
  ✓ GET object — atomic read
  ✓ DELETE object — atomic
  ✓ COPY object — atomic at single-key level
  ✓ LIST objects in a prefix — eventually consistent (mostly)
  ✗ No multi-object transaction
  ✗ No conditional writes (S3 added compare-and-swap in 2024, see below)
  ✗ No row-level operations (it's KV at file granularity)
  ✗ Historically: eventual consistency on list-after-write (S3 fixed in 2020)
```

```
2.2 The Atomic-Rename Trick
TRADITIONAL FILE SYSTEMS (HDFS, POSIX):
  Atomic rename of file → use this as the commit primitive.
  Hudi's original design + Iceberg's HadoopFileIO depend on this.
OBJECT STORES:
  S3 has no atomic rename (rename = copy + delete, not atomic).
  → Need a different commit mechanism.
THE METADATA POINTER PATTERN:
  Each "version" of the table is a metadata file.
  "Current version" is determined by a pointer somewhere.
  To commit: write new metadata file → update pointer atomically.
  The pointer can be:
     A file with a specific name (Delta: _last_checkpoint, log version files)
     A catalog entry (Iceberg with REST/Glue: a row in a database)
     A row in an external table (older patterns)
THE POINTER UPDATE IS THE COMMIT.
The atomicity of that one operation gives the whole table ACID.
```

```
2.3 Conditional Writes & Compare-and-Swap
CONCURRENT WRITERS PROBLEM:
  Writer A reads current version v5, prepares v6.
  Writer B reads current version v5, prepares v6.
  Both write v6 metadata file → one wins, the other's commit is lost.
SOLUTION 1: External catalog with row-level locking
  Iceberg + Glue / Nessie / REST catalog
  → Catalog enforces optimistic concurrency on the table pointer
SOLUTION 2: Conditional writes
  S3 PutObject with If-None-Match: * (Nov 2024)
  → "Write this file only if it doesn't exist"
  → Writer attempts to write v6; if it exists, retry from latest
SOLUTION 3: File-based commit protocols
  Delta: writers attempt to write log file N+1.
  Two writers race; one succeeds, the other gets a conflict error.
  Originally used DynamoDB (Databricks Delta) or filesystem rename (HDFS).
  S3 conditional writes now enable native Delta on S3 without DynamoDB.
ALL THREE rely on SOME atomic primitive that resolves the race.
```

```
2.4 The Two Concurrency Models
OPTIMISTIC CONCURRENCY CONTROL (OCC)
  Writers don't lock. Both try to commit. One wins; loser retries.
  All three formats (Delta, Iceberg, Hudi) use this.
  WORKS WELL when conflicts are rare (different partitions, append-only)
  WORKS POORLY when many writers touch the same partition
CONFLICT TYPES:
   Modified same file: conflict, retry needed
   Different partitions: usually no conflict (depends on format/operation)
   Append-only: never conflicts (different partitions, different files)
→ ARCHITECT FOR LOW CONTENTION:
    Single writer per partition is the easy mode
    Streaming writers + periodic compaction job: writer separation
    Multiple appenders with row partitioning: usually fine
```

```
2.5 What ACID Actually Means Here
ATOMICITY:
  ✓ A write either fully commits or doesn't. No partial states visible.
CONSISTENCY:
  ✓ Schema enforced on write. No malformed rows in committed data.
  ✓ Constraints (if defined) checked at commit time.
ISOLATION:
  ✓ Readers see a consistent snapshot. SNAPSHOT ISOLATION semantics.
  ✗ Generally NOT serializable across concurrent writers.
DURABILITY:
  ✓ Once committed, data is in S3 (which is 99.999999999% durable).
THIS IS NOT A FULL ACID DATABASE.
  ✗ No per-row transactions
  ✗ No real-time concurrency for OLTP
  ✗ No traditional indexes
```

It's WAREHOUSE-GRADE ACID at OLAP scale, on cheap storage. ■ Principal mindset: ACID on object storage is the central technical innovation. Once you internalize that the 'commit' is an atomic update of a single metadata file/pointer, every design choice in Delta, Iceberg, and Hudi falls into place. They differ in HOW they organize the metadata — flat log vs tree vs timeline — but they all reduce ACID to 'flip the pointer atomically'.

#### 3. The Open Table Format

  - 3.1 What a Table Format Defines
- A TABLE FORMAT is a specification for:
- 1. PHYSICAL DATA LAYOUT
- Parquet / ORC / Avro files in a folder hierarchy
- 2. METADATA STRUCTURE
- What files exist, their schemas, their stats, their partitions
- 3. TRANSACTION PROTOCOL
- How writes commit; how readers see consistent state
- 4. EVOLUTION RULES
- How schemas, partitioning, and properties change over time
- 5. OPERATION SEMANTICS
- What MERGE, DELETE, UPDATE mean and how to execute them

3.2 The Three Major Formats Format Origin Year Strength Delta Lake Databricks 2019 Mature, Spark-native, MERGE Apache Iceberg Netflix 2018 Open governance, multi-engine Apache Hudi Uber 2017 Streaming upserts at scale

```
3.3 What Each Format Looks Like on Disk
DELTA LAKE:
  s3://bucket/my_table/
    part
...snappy.parquet              ← data
    part
...snappy.parquet
    _delta_log/
      00000000000000000000.json               ← commit 0
      00000000000000000001.json               ← commit 1
      00000000000000000010.checkpoint.parquet ← periodic checkpoint
      _last_checkpoint                        ← pointer to latest checkpoint
ICEBERG:
  s3://bucket/my_table/
    data/
      00000
...parquet                      ← data files (any layout)
    metadata/
      v1.metadata.json                        ← table version 1
      v2.metadata.json                        ← table version 2
      snap-1234.avro                          ← manifest list for snapshot
      00001-...avro                           ← manifest file
      00002-...avro
HUDI (Copy-on-Write):
  s3://bucket/my_table/
    partition=2024
01/
      <fileId>_<commitTime>.parquet           ← versioned file slices
    .hoodie/
      hoodie.properties
      20240301120000.commit                   ← commit metadata file
      20240301120000.commit.requested
      20240301120000.inflight
```

...

```
3.4 The Universal Operations
All three support:
  INSERT (append)
  UPSERT (MERGE)
  DELETE (with predicate)
  UPDATE (with predicate)
  ALTER TABLE (schema evolution)
  TIME TRAVEL (read at version/timestamp)
  VACUUM (remove old file versions)
  COMPACTION (rewrite small files)
WHERE THEY DIFFER:
   Performance characteristics (especially for upserts)
   Engine support (Spark always; Trino, Flink, Snowflake, BigQuery vary)
   Catalog requirements
   Streaming write semantics
   Schema evolution flexibility
   Partition evolution support
```

```sql
3.5 SQL on All Three
# Spark SQL syntax — works on all three with the right catalog
CREATE TABLE my_table (id BIGINT, name STRING, ts TIMESTAMP) USING DELTA;
CREATE TABLE my_table (id BIGINT, name STRING, ts TIMESTAMP) USING ICEBERG;
CREATE TABLE my_table (id BIGINT, name STRING, ts TIMESTAMP) USING HUDI;
# MERGE works the same way (mostly):
MERGE INTO my_table tgt USING updates src
  ON tgt.id = src.id
  WHEN MATCHED THEN UPDATE SET ...
  WHEN NOT MATCHED THEN INSERT ...;
# Time travel:
SELECT * FROM my_table VERSION AS OF 5;
SELECT * FROM my_table TIMESTAMP AS OF '2024
01 00:00:00';
SELECT * FROM my_table FOR VERSION AS OF 5;     # Iceberg syntax variant
The SQL surface is increasingly converging.
The differences are operational and in the metadata layer below.
■ Principal mindset: Think of table formats as 'POSIX for analytical data lakes' — a specification that lets many
engines interoperate. The format wars are partially about performance, partially about governance, but mostly about
which one becomes the de facto standard. As of 2024-2025, Iceberg is winning the standardization race; Delta still
dominates Databricks shops; Hudi remains niche but strong in streaming-upsert workloads. We'll cover this in detail
in Chapter 15.
```

#### 4. Catalogs: The Underrated Layer

4.1 What a Catalog Does A catalog is the directory that maps table names to their physical location and current version. Without a catalog you have files in S3 with no central knowledge of 'what tables exist' or 'what version is current'. The catalog choice is as important as the format choice — and is the layer that determines multi-engine compatibility.

  - 4.2 Catalog Responsibilities
- 1. NAMESPACE → TABLE → LOCATION mapping
- catalog.database.table_name → s3://bucket/path/
- 2. CURRENT VERSION POINTER (for Iceberg specifically)
- table_name → metadata_v5.json
- 3. CONCURRENCY CONTROL
- Atomic compare-and-swap on the version pointer
- 4. SCHEMA / PROPERTIES (often duplicated; canonical source varies)
- 5. ACCESS CONTROL
- Who can read/write which tables (in catalogs that support it)
- 6. DISCOVERY
- "Show me all tables in domain X"
- "What's the schema of table Y?"

4.3 The Catalog Choices Catalog Provider Best for Trade-off Hive Metastore Apache Legacy compat Old, heavyweight AWS Glue AWS AWS-native, managed AWS lock-in Unity Catalog Databricks Databricks shops Vendor-tied (opening up) Iceberg REST Apache Multi-engine, future-proof Newer, less mature Nessie Dremio Git-like branching Smaller ecosystem Polaris Snowflake Iceberg REST + governance Open-sourced 2024 File-system (no catalog) — Single-engine, simple No concurrency, no discovery

```
4.4 The Iceberg Catalog Story
ICEBERG IS DELIBERATELY CATALOG-AGNOSTIC.
The metadata files (under metadata/) are the source of truth for table state.
The CATALOG only provides:
   The mapping from name to location
   The current metadata file pointer
   Atomic CAS on that pointer
DIFFERENT CATALOGS, SAME ICEBERG TABLE:
  HiveCatalog       — stores pointer in Hive Metastore
  GlueCatalog       — stores in AWS Glue
  HadoopCatalog     — file-system rename-based
  RESTCatalog       — talks to an HTTP REST service
  NessieCatalog     — Git-like with branches/tags
```

SqlCatalog — backed by a relational DB You can MIGRATE between catalogs by re-pointing — the table files don't change. This is one of Iceberg's strongest features.

```
4.5 The Open Catalog Movement
2023-2024 saw a wave of open-source catalogs aimed at multi-engine, multi-vendor
governance for Iceberg tables:
  POLARIS  (Snowflake, open-source 2024)
    REST-based, RBAC, integrates with Snowflake managed Iceberg
    Snowflake's strategic move to commoditize the format
  UNITY CATALOG (Databricks, open-source 2024)
    Originally Databricks-only; opened to attract Iceberg users
    Now supports Delta, Iceberg, Hudi tables under unified governance
  REST CATALOG (Apache, spec-driven)
    The protocol; any vendor can implement
    Used by Tabular (acquired by Databricks), Lakekeeper, etc.
  GRAVITINO (Datastrato)
    Cross-cloud catalog federation
THE PUNCHLINE:
  Catalogs are becoming the new battleground. Whoever owns the catalog
  controls the lakehouse experience. Expect rapid consolidation.
```

```
4.6 The Catalog Decision Framework
ALL-AWS, SPARK/EMR: Glue Catalog (simplest)
ALL-DATABRICKS:    Unity Catalog (best UX)
ALL-SNOWFLAKE:     Snowflake-managed catalog or Polaris
MULTI-VENDOR:      Iceberg REST catalog (Polaris, Tabular, Nessie)
LEGACY HIVE:       Hive Metastore (well-supported, but plan migration)
SINGLE NODE / DEV: File-system catalog (HadoopCatalog) — DON'T use in prod
KEY QUESTION:
  Which engines will need to read this data over the next 5 years?
  If just one → vendor catalog OK.
  If multiple → Iceberg REST catalog is the future-proof choice.
■ Interview angle: Q: Your team is on Databricks but plans to add Snowflake and Trino in the next year. What
catalog do you pick?
Staff: Iceberg REST catalog. Snowflake supports it natively (as of 2024, they read external Iceberg with REST
catalogs). Trino supports it. Databricks supports Iceberg reads (and writes, with caveats). If you start with Unity
Catalog for Delta tables, you'll have to migrate later — possibly painfully. Polaris (Snowflake's open-source catalog)
or a managed Iceberg catalog from a vendor like Tabular is the cleanest path. Note: Delta+Unity is still the best
Databricks-native experience; you trade some Databricks UX for future-proofing. Often worth it.
```

## PART II — DELTA LAKE

#### 5. Delta Architecture & Transaction Log

```
5.1 The On-Disk Layout
s3://bucket/my_table/
  ■■ _delta_log/                                  ← THE METADATA
  ■    ■■■ 00000000000000000000.json              ← commit 0
  ■    ■■■ 00000000000000000001.json              ← commit 1
  ■    ■■■ 00000000000000000002.json
  ■    ■■■ ...
  ■    ■■■ 00000000000000000010.checkpoint.parquet ← every 10 commits
  ■    ■■■ _last_checkpoint                        ← pointer to latest checkpoint
  ■■ year=2024/                                    ← partitions
  ■    ■■■ month=01/
  ■         ■■■ part
...snappy.parquet
  ■         ■■■ part
...snappy.parquet
  ■■■ year=2024/
       ■■■ month=02/
            ■■■ ...
```

```
5.2 The Transaction Log
EACH COMMIT WRITES ONE JSON FILE:
  00000000000000000007.json
CONTAINS A SEQUENCE OF "ACTIONS":
  {"protocol": {"minReaderVersion": 1, "minWriterVersion": 2}}
  {"metaData": {"schemaString": ..., "partitionColumns": ...}}
  {"add": {"path": "part
abc.parquet", "size": 123456, "stats": "..."}}
  {"add": {"path": "part
def.parquet", "size": 234567, "stats": "..."}}
  {"remove": {"path": "part
xyz.parquet", "deletionTimestamp": ...}}
  {"commitInfo": {"timestamp": ..., "operation": "WRITE", "userMetadata": ...}}
EACH JSON FILE = ONE TRANSACTION.
  Each transaction is a sequence of file-level adds + removes.
  Reading the log = building current table state by replaying actions.
WHY JSON: Human-readable, easy to debug, append-only.
WHY VERSIONED FILE NAMES: lexicographic ordering = commit order.
WHY CHECKPOINTS: replaying 100K JSON files would be slow.
```

```sql
5.3 Checkpoints
CHECKPOINT = compacted snapshot of the table state at a specific version.
EVERY 10 COMMITS (configurable):
  Write 00000000000000000020.checkpoint.parquet
  Contains: union of all 'add' actions from commits 0..20 minus 'remove' actions
  Format: Parquet (fast to read)
THEN UPDATE _last_checkpoint:
  {"version": 20, "size": 1234, ...}
READING THE TABLE STATE NOW:
```

#### 1. Read _last_checkpoint → know latest checkpoint version

#### 2. Load that checkpoint.parquet (fast)

#### 3. Apply any commits AFTER that version (rare, recent)

#### 5.4 The Commit Protocol

#### 1. Read latest log version (e.g., current is 47)

#### 2. Compute target version: 48

#### 3. Prepare new log file content (actions list)

```
  4. Try to write: _delta_log/00000000000000000048.json
      S3 with conditional writes: PUT with If-None-Match: *
      DynamoDB-backed (Databricks): atomic CAS on version row
      HDFS: rename from temp file
  5. SUCCESS → commit done
     FAILURE → version 48 already exists; conflict
                Resolve: re-read log up to current, re-check conflicts,
                         re-try as version 49
CONFLICT RESOLUTION:
  Some conflicts are recoverable (different partitions touched)
  Some are not (same files modified) → caller must retry the operation
```

```
5.5 The Protocol Versions
protocol = {minReaderVersion: M, minWriterVersion: N}
THIS IS HOW DELTA EVOLVES.
   New features require higher protocol versions
   Old readers refuse to read tables with newer protocol versions
   Backward-compatible additions don't bump protocol
WRITER VERSION SAMPLES:
  1: Original write protocol
  2: CHECK constraints
  3: IDENTITY columns
  4: change data feed
  5: column mapping
  6: ...
  7: row tracking, deletion vectors
READER VERSION SAMPLES:
  1: Read add/remove actions
  2: column mapping (rename columns without rewriting data)
  3: deletion vectors (efficient deletes)
UPGRADING A TABLE:
  Once you upgrade, older clients can't read. Plan upgrades carefully.
■ Principal mindset: Delta's log structure is its simplicity advantage: append-only JSON files, periodic checkpoints,
single pointer file (_last_checkpoint). It's easy to debug — cat a JSON file, see what changed. The flat structure also
makes operations like time travel and CDC straightforward. The cost: at very high commit rates the log directory
grows, requiring more frequent checkpoints. Most teams never hit that limit.
```

#### 6. Delta Operations: MERGE, OPTIMIZE, Z-ORDER

```sql
6.1 INSERT, UPDATE, DELETE
# INSERT (append)
INSERT INTO my_table VALUES (1, 'foo'), (2, 'bar');
# Or via DataFrame
df.write.format("delta").mode("append").save("s3://bucket/my_table/")
# UPDATE
UPDATE my_table SET status = 'active' WHERE id = 42;
# DELETE
DELETE FROM my_table WHERE created_at < '2023
01';
WHAT HAPPENS UNDER THE HOOD (UPDATE/DELETE):
```

- 1. Find files containing matching rows (using file-level stats)
- 2. READ those files, REWRITE them without the affected rows
- (or with updated rows for UPDATE)

```sql
  3. Commit: add new files, remove old files in one transaction
→ UPDATE / DELETE COST IS PROPORTIONAL TO FILE SIZE, NOT ROW COUNT.
  A small WHERE clause touching 1 row in a 1 GB file = rewrite 1 GB.
  This is the read-modify-write pattern of immutable file systems.
```

```
6.2 Deletion Vectors (Delta 3.0+)
DELETION VECTORS (DV) — the modern alternative.
INSTEAD OF rewriting files for UPDATE/DELETE:
   Write a small bitmap indicating which rows in each file are deleted
   Readers skip those rows
   Periodically, compact tables to actually rewrite files
BENEFITS:
  ✓ DELETE / UPDATE are much faster (no full file rewrite)
  ✓ Especially valuable for GDPR-style point deletes
  ✓ MERGE benefits enormously
CONFIG:
  ALTER TABLE my_table SET TBLPROPERTIES (
    'delta.enableDeletionVectors' = 'true'
  );
CONSIDERATIONS:
   Increases reader protocol version (older readers reject)
   Requires periodic OPTIMIZE / REORG to materialize deletions
   Some consumers (e.g., external readers) may not support DVs yet
```

```sql
6.3 MERGE (Upsert)
MERGE INTO target_table tgt
USING source_data src
  ON tgt.user_id = src.user_id
WHEN MATCHED AND src.updated_at > tgt.updated_at THEN
  UPDATE SET name = src.name, updated_at = src.updated_at
WHEN MATCHED AND src.is_deleted THEN
  DELETE
WHEN NOT MATCHED THEN
  INSERT (user_id, name, updated_at) VALUES (src.user_id, src.name, src.updated_at);
THE WORKHORSE OPERATION FOR LAKEHOUSES.
USE CASES:
```

```sql
  ✓ CDC ingestion (apply changes to a table)
  ✓ Slowly-changing dimensions
  ✓ Deduplication during ingestion
  ✓ Idempotent writes from streaming
COST CHARACTERISTICS:
   Inner join between source and target on merge key
   Files containing matched target rows are REWRITTEN
   Cost ≈ |source| + |matched_target_files|
   If matched rows are spread across many files → high cost
    → CLUSTER target by merge key for efficient MERGEs (see Z-ORDER)
```

```
6.4 OPTIMIZE (File Compaction)
STREAMING WRITES PRODUCE MANY SMALL FILES.
  Each Spark Streaming micro-batch writes per-partition files (often KB-sized).
  Hundreds of thousands of small files = slow reads, expensive metadata.
OPTIMIZE rewrites small files into larger ones.
  OPTIMIZE my_table;
  OPTIMIZE my_table WHERE partition_col = '2024
01';
WHAT IT DOES:
   Reads small files
   Writes new larger files (target: 1 GB by default)
   Atomically commits: add new files, remove old files
WHEN TO RUN:
   Periodic (hourly / daily) for streaming-fed tables
   After bulk inserts that produce many small files
   Pre-emptively before known heavy read workloads
CONFIG:
  spark.databricks.delta.optimize.maxFileSize = 1073741824  # 1 GB
```

```
6.5 Z-ORDER
Z-ORDER = multi-dimensional file-level clustering.
PROBLEM:
  Files are stored sorted by partition column.
  For OTHER common WHERE clauses, files have to be scanned (or stats checked).
  Example: partitioned by date, but queries also filter by user_id.
  All user_ids are spread across all files within a day.
Z-ORDER:
  Re-cluster files using a space-filling curve over multiple columns.
  Files have tighter min/max ranges per column.
  Statistics enable better file-pruning.
  OPTIMIZE my_table ZORDER BY (user_id, region);
EFFECT:
   Queries with WHERE user_id = X or WHERE region = Y skip many files
   Up to 10× speedup for selective queries on Z-ORDERed columns
LIMITATIONS:
   Max 3-4 columns effectively (the curve degrades with more dims)
   Re-Z-ORDER needed after large inserts (it's a snapshot operation)
   Costs significant compute on the OPTIMIZE run
LIQUID CLUSTERING (Delta 3.0+) — newer alternative:
  Like Z-ORDER but maintained INCREMENTALLY (no full re-cluster needed)
```

Better operational characteristics; replacing Z-ORDER in new code ■ Production scenario: A team had a 200-million-row Delta table, MERGE was taking 4 hours nightly. Investigation: the merge key was user_id, but the table was partitioned by event_date. Every MERGE had to scan all files in the affected dates (every user could be in any file). After Z-ORDER on user_id (within each date partition), the same MERGE took 18 minutes. The optimizer could prune files where the user_id range didn't include the merge keys. Z-ORDER on join keys is essential for MERGE-heavy tables.

#### 7. Time Travel, Vacuum & CDC Feed

```sql
7.1 Time Travel
DELTA RETAINS old file versions for the configured retention period.
This enables querying past states.
# By version (commit number)
SELECT * FROM my_table VERSION AS OF 42;
SELECT * FROM delta.`s3://path/` VERSION AS OF 42;
# By timestamp
SELECT * FROM my_table TIMESTAMP AS OF '2024
15 12:00:00';
# DataFrame API
df = spark.read.format("delta").option("versionAsOf", 42).load(path)
df = spark.read.format("delta").option("timestampAsOf", "2024
15").load(path)
USE CASES:
  ✓ Audit ("what did this look like yesterday at 9 AM?")
  ✓ Reproducibility ("re-train ML model on same snapshot")
  ✓ Recovery from bad write ("rollback to version before the corruption")
  ✓ Compliance / debugging
# Rollback (creates a new commit that reverts to old state)
RESTORE TABLE my_table TO VERSION AS OF 42;
```

```
7.2 Retention & Vacuum
RETENTION = how long old file versions stick around.
  delta.deletedFileRetentionDuration = INTERVAL 7 DAYS  (default)
  delta.logRetentionDuration         = INTERVAL 30 DAYS (default)
VACUUM REMOVES OLD FILES:
  VACUUM my_table;                      # default retention
  VACUUM my_table RETAIN 168 HOURS;     # explicit (7 days)
  VACUUM my_table RETAIN 0 HOURS;       # remove all old (DANGEROUS)
WHAT VACUUM REMOVES:
   Data files marked as 'remove' in the log AND older than retention
   Does NOT touch the log files (use log retention setting for those)
WHAT IS NOT REMOVED:
   Files referenced by the current commit
   Files within the retention window
DANGER ZONE:
  RETAIN 0 HOURS = remove all unreferenced files immediately
  → Can break in-flight queries that are reading old versions
  → Can break time-travel queries within the window
  → Default retention exists for a reason
```

```
7.3 Change Data Feed (CDF)
DELTA CDF = capture INSERTs/UPDATEs/DELETEs as a stream of change records.
ENABLE:
  ALTER TABLE my_table SET TBLPROPERTIES (
    'delta.enableChangeDataFeed' = 'true'
  );
# Read CDC events:
SELECT *
```

```
FROM table_changes('my_table', start_version, end_version);
OUTPUT SCHEMA:
  | original_cols... | _change_type | _commit_version | _commit_timestamp |
  _change_type:
    'insert'              — new row
    'update_preimage'     — value before UPDATE
    'update_postimage'    — value after UPDATE
    'delete'              — deleted row
USE CASES:
  ✓ Propagate changes to downstream systems (search index, cache)
  ✓ Build incremental ETL on top of Delta tables
  ✓ Audit trails
  ✓ Stream from a Delta table to Kafka or another system
STREAMING:
  spark.readStream.format("delta")
      .option("readChangeFeed", "true")
      .option("startingVersion", 100)
      .load("s3://path/")
```

7.4 The Three Pillars Together TIME TRAVEL gives you point-in-time reads of the past CDF gives you a stream of WHAT CHANGED between two points VACUUM gives you control over how much history to retain OPERATIONAL PATTERN:  Retain 30 days for time travel + audit  CDF for downstream streaming (each consumer tracks its own offset)  Periodic OPTIMIZE + VACUUM (daily or weekly)  Monthly review of table size and file counts ■ Interview angle: Q: A pipeline accidentally inserted millions of bad rows into a Delta table 2 days ago. What do you do? Staff: Three options in order of safety. (1) If you can identify the bad rows precisely, DELETE them with a WHERE clause — surgical, no time-travel needed. (2) If they're hard to identify but you know exactly when the bad commit happened, RESTORE TABLE to the version just before — this creates a new commit reverting to that state. Quickest recovery. (3) If neither, read the table VERSION AS OF the pre-bad-commit version into a new table, validate, and atomically swap. The CDF is also useful for diagnosis: read the changes between the two versions and see exactly what got inserted. Crucially: this all works only if VACUUM hasn't run with too-short retention. Default 7 days saves you here.

## PART III — APACHE ICEBERG

#### 8. Iceberg Architecture & Metadata Tree

```
8.1 The Three-Level Metadata Tree
ICEBERG'S METADATA IS A TREE, not a flat log.
  CATALOG  →  v3.metadata.json    ← TABLE METADATA (current state pointer)
                  ■
                  ■■→ snap-1234.avro    ← MANIFEST LIST (for snapshot 1234)
                  ■       ■
                  ■       ■■→ manifest-001.avro   ← MANIFEST FILE
                  ■       ■       ■
                  ■       ■       ■■→ data/0001.parquet
                  ■       ■       ■■→ data/0002.parquet
                  ■       ■       ■■→ ...
                  ■       ■
                  ■       ■■→ manifest-002.avro
                  ■       ■
                  ■       ■■→ manifest-003.avro
                  ■
                  ■■→ snap-1233.avro    ← previous snapshot
                  ■■→ snap-1232.avro
EACH LEVEL HAS A PURPOSE:
  table metadata: schemas, partition specs, snapshots, properties
  manifest list:  set of manifests in a snapshot, partition-summary stats
  manifest file:  set of data files with column-level stats
  data files:     Parquet/ORC/Avro
```

```sql
8.2 Snapshots
SNAPSHOT = an atomic point-in-time view of the table.
EACH SNAPSHOT POINTS TO A MANIFEST LIST → set of manifests → set of data files.
COMMITS CREATE NEW SNAPSHOTS:
   Append: new snapshot adds manifests pointing to new data files
   Delete: new snapshot adds DELETE FILES (more on this)
   Overwrite: new snapshot replaces some manifests entirely
SNAPSHOT METADATA:
   snapshot_id (monotonic integer)
   parent_snapshot_id (for lineage)
   timestamp_ms
   operation (append, overwrite, delete, ...)
   manifest_list (S3 path)
   summary (added/deleted files counts, partition info)
   summary stats (rows added/deleted)
TIME TRAVEL:
  SELECT * FROM my_table.snapshots;           -- list all snapshots
  SELECT * FROM my_table FOR VERSION AS OF 1234567;
  SELECT * FROM my_table FOR TIMESTAMP AS OF '2024
15';
```

#### 8.3 Manifest Lists

```
MANIFEST LIST = inventory of all manifests in a snapshot.
Stored as Avro file: snap-{snapshot_id}-{...}.avro
EACH ENTRY:
   manifest_path (S3 location of the manifest file)
   partition_summary (min/max partition values represented)
   added/deleted/existing file counts
   added/deleted rows counts
   snapshot_id (which snapshot added this manifest)
WHY THIS LEVEL EXISTS:
  Without manifest list: reading 100K manifests to plan a query
  With manifest list: read 1 file, prune manifests by partition stats,
                      read only relevant manifests
PARTITION PRUNING HAPPENS HERE:
  Query: WHERE event_date = '2024
15'
  Reader: read manifest list, skip manifests whose partition_summary
          doesn't include 2024
15
```

```
8.4 Manifest Files
MANIFEST FILE = inventory of data files (or delete files).
Stored as Avro file: {manifest_id}.avro
EACH ENTRY:
   file_path
   file_format (parquet/orc/avro)
   partition values
   record_count
   file_size_bytes
   column-level stats:
      column_sizes, value_counts, null_value_counts,
      lower_bounds, upper_bounds  (per column!)
   status (added, existing, deleted)
THE STATS ARE THE PERFORMANCE STORY:
  Same as Parquet row-group stats, but at FILE level in the metadata.
  Readers can prune files BEFORE opening them.
  Query: WHERE user_id = 42
  Reader: skip files whose user_id range doesn't include 42
  → MASSIVE file pruning for selective queries
```

```
8.5 Why a Tree Instead of a Log?
DELTA: flat log of commits.
ICEBERG: tree of manifests pointing to data files.
ADVANTAGE OF THE TREE:
   Manifest reuse across snapshots
    (a manifest unchanged across N snapshots is referenced N times, stored once)
   Better partition pruning at the manifest-list level
   Better statistics propagation
ADVANTAGE OF THE LOG:
   Simpler to reason about
   Easier to debug (cat a JSON file)
   Slightly faster commits (one file write vs many)
THE TREE PAYS OFF at scale (1000+ partitions, frequent commits).
THE LOG IS SIMPLER for smaller tables and quick iteration.
```

#### 8.6 Iceberg Commit Protocol

#### 1. Read current table metadata: v5.metadata.json (via catalog)

#### 2. Prepare new data files (write Parquet to s3://path/data/)

#### 3. Prepare new manifest file(s) referencing new data files

#### 4. Prepare new manifest list (existing manifests + new ones)

#### 5. Prepare new table metadata file: v6.metadata.json

```
  6. Ask catalog to atomically update current pointer:
     "v5 → v6"
  7. SUCCESS: commit done
     FAILURE (catalog says current is now v6, not v5): conflict
              → retry: re-read v6, re-plan, try v7
THE CATALOG IS THE COMMIT BOTTLENECK.
  Fast catalog = fast commits.
  Catalog with row-level locking handles concurrency well.
■ Principal mindset: Iceberg's design reflects a different philosophy: optimize for VERY LARGE tables and
multi-engine access. The tree structure has overhead for small tables but pays off enormously at scale (PB-class
tables with 100K+ partitions). Netflix designed it for their needs; the rest of the industry is catching up. Delta
optimizes for write throughput on Databricks; Iceberg optimizes for read efficiency across engines. Both are valid.
```

#### 9. Hidden Partitioning & Schema Evolution

```
9.1 The Hive Partitioning Problem
TRADITIONAL HIVE PARTITIONING:
  Table partitioned by event_date.
  Directories: /year=2024/month=01/day=15/
  TO QUERY EFFICIENTLY:
    Query: WHERE event_date >= '2024
01'
    Reader: looks at directory names, finds matching partitions.
  THE TRAP:
    Query: WHERE event_date >= '2024
01' AND status = 'active'
    Reader: still scans all files in those partitions for status filter
  WORSE:
    Query: WHERE day_of_week(event_date) = 1  (Mondays only)
    Reader: directory names tell nothing about day-of-week
            → full scan of all partitions
  USER MUST KNOW PARTITION SCHEME to write efficient queries.
  Schema changes are catastrophic — files in old layout become unreadable.
```

```sql
9.2 Iceberg Hidden Partitioning
ICEBERG DECOUPLES partition values from file paths.
PARTITION SPEC IS METADATA:
  partition_spec = [
    { source: 'event_ts', transform: 'day', name: 'event_day' }
  ]
DATA FILES STORE partition values in manifests, not directory paths.
QUERIES JUST USE THE COLUMN:
  SELECT * FROM events WHERE event_ts > '2024
01';
  → Iceberg matches on the TRANSFORM (day(event_ts))
  → Files with day=2024
28 are skipped automatically
USER NEVER WRITES 'WHERE event_day = ...'.
USER WRITES THE NATURAL PREDICATE.
ICEBERG FIGURES OUT THE PARTITION FILTER.
SUPPORTED TRANSFORMS:
  identity (raw value)
  year(col), month(col), day(col), hour(col)
  bucket(N, col)    — hash partition into N buckets
  truncate(W, col)  — string/integer truncation
  void              — no partitioning
```

9.3 Partition Evolution WANT TO CHANGE PARTITIONING? In Hive: rewrite the whole table. In Iceberg: just update the partition spec. ALTER TABLE my_table ADD PARTITION FIELD month(event_ts); -- Existing data keeps its old partition spec. -- New data uses the new spec. -- Iceberg handles file pruning across both specs. EXAMPLE: Originally partitioned by day.

```
  Volume grows → switch to hour for new data.
  Old data stays day-partitioned (no rewrite needed).
  Queries with hour-level filters use hour pruning on new data,
    day pruning on old data, automatically.
→ NO MIGRATION NEEDED for partitioning changes.
```

```
9.4 Schema Evolution
ICEBERG TRACKS COLUMN IDs (integers), not names.
  Column 'amount_cents' has internal ID 5.
  Rename to 'amount': ID 5 still points to the same data.
  Parquet files store column ID alongside name.
OPERATIONS:
   ADD column      → forward-compatible
   DROP column     → old data ignored, new writes omit
   RENAME column   → metadata-only change (no rewrite)
   REORDER columns → metadata-only change
   PROMOTE type    → safe widenings (int → long, float → double)
WHAT'S NOT SAFE (require rewrite):
  ✗ Narrowing types (long → int)
  ✗ Changing date formats
  ✗ Lossy conversions
DELTA also supports many of these, with COLUMN MAPPING (newer feature).
But Iceberg's column-ID approach is more robust.
```

9.5 The Multi-Engine Implication HIDDEN PARTITIONING + SCHEMA EVOLUTION mean:  Same query works on the same data across engines  Partition changes don't require coordinating with consumers  Schema changes don't break old data THIS IS WHY ICEBERG IS WINNING THE FORMAT WARS for multi-engine setups. Snowflake, BigQuery, Spark, Trino, DuckDB, Polars — all can read Iceberg with consistent behavior. ■ Interview angle: Q: Your table is partitioned by day. Daily volume grew 10x. Repartitioning would take a week. Options? Staff: In Iceberg, no migration needed: ALTER TABLE ADD PARTITION FIELD hour(event_ts). New data partitions hourly; old data stays daily. Iceberg handles both at query time. In Delta or Hive, you'd need to rewrite the table — copy to a new partition layout, atomically swap. That's a real outage planning exercise. This is one of Iceberg's strongest features for long-lived tables: partition evolution without rewrite. It's the kind of thing you don't think you need until production volume changes shape.

#### 10. Iceberg Catalogs: REST, Glue, Nessie, Polaris

```
10.1 The Catalog Spectrum
ICEBERG DECOUPLES the catalog from the table format.
THE CATALOG INTERFACES:
  Catalog.load_table(identifier)
  Catalog.create_table(identifier, schema, partition_spec, location)
  Catalog.drop_table(identifier)
  Table.refresh()
  Table.new_transaction().new_append().commit()
UNDER THE INTERFACE:
  HadoopCatalog       — file system, no external services
  HiveCatalog         — backed by Hive Metastore
  GlueCatalog         — AWS Glue
  SqlCatalog          — backed by JDBC (Postgres, MySQL)
  RestCatalog         — HTTP API
  NessieCatalog       — Nessie's branch-aware catalog
  DynamoDbCatalog     — DynamoDB-backed
YOU CAN MIGRATE BETWEEN CATALOGS without rewriting data.
```

```
10.2 The Iceberg REST Catalog
REST CATALOG = open HTTP specification.
   Any catalog can implement it
   Any engine can talk to it
   Standardized: list namespaces, list tables, load table metadata,
                  commit transactions
IMPLEMENTATIONS (2024):
  Tabular              — managed (now Databricks)
  Polaris              — Snowflake's open-source REST catalog
  Lakekeeper          — community implementation
  Unity Catalog        — Databricks (supports REST API)
  AWS Glue REST        — AWS's REST endpoint over Glue
  Iceberg REST adapter — wraps any catalog as REST
WHY REST WON:
   Engine-agnostic (no JVM dependency for clients)
   Easy to layer governance / audit / access control
   Standardized — pick the catalog impl that fits, swap later
```

10.3 The Catalog Comparison Catalog Hosting Strength Best for REST BYO server Standard, future-proof Multi-engine Glue AWS managed Zero-ops, native AWS AWS-only shops Unity Databricks managed RBAC, lineage, UI Databricks-centric Polaris Self-host or SF Snowflake-native, open Snowflake + others Hive Self-host Legacy compat Existing Hive shops Nessie Self-host Git-like branches/tags Data versioning use cases HadoopCatalog None No infra Dev/test only

```
10.4 Nessie: Git-Like Lakehouse
NESSIE adds GIT-LIKE BRANCHING to Iceberg.
  CREATE BRANCH dev_alice;
  USE BRANCH dev_alice;
  INSERT INTO my_table ...;       -- only visible on this branch
```

```
  -- merge back into main
  MERGE BRANCH dev_alice INTO main;
USE CASES:
  ✓ Isolated dev environments (no real data copy needed)
  ✓ Long-running experiments (compute on a branch for a week)
  ✓ Atomic multi-table commits (commit changes to several tables as one)
  ✓ Rollback-friendly deployments
CAVEATS:
   Adds operational complexity
   Engine support varies (Spark, Trino solid; Snowflake/BigQuery not native)
   Less mature than mainstream catalogs
```

```
10.5 Polaris & The Open Catalog Movement
POLARIS (Snowflake, open-sourced 2024):
   Iceberg REST catalog implementation
   RBAC, multi-tenancy
   Designed for cross-vendor governance
   Snowflake's strategic play: commoditize the lakehouse layer
    so they compete on compute and BI, not storage
WHY THIS MATTERS:
  Pre-2024: catalogs were vendor-tied. Migrate to a new engine = migrate catalog.
  Post-Polaris/Unity-open: catalogs become a portable layer.
```

```
  Long-term: catalog is the data-stack layer that survives engine churn.
  Pick wisely.
```

## PART IV — APACHE HUDI

#### 11. Hudi Architecture: CoW vs MoR

11.1 Hudi's Design Goal Hudi was built at Uber (2017) for a specific problem: ingesting 10s of TB/day of CDC events with row-level updates and the need for near-real-time freshness. Its design priorities differ from Delta and Iceberg — Hudi is upsert-first, streaming-native.

```
11.2 The Two Table Types
HUDI HAS TWO MAJOR TABLE TYPES:
COPY-ON-WRITE (CoW)
   Updates rewrite affected files (like Delta default)
   Storage cost: every update = file rewrite
   Read cost: cheap — just read Parquet
   Write cost: high for updates (rewrite affected files)
   Best for: read-heavy workloads, fewer updates
MERGE-ON-READ (MoR) — Hudi's distinctive feature
   Updates append to LOG FILES (small Avro files)
   Periodically compact log into base Parquet files
   Storage cost: lower (avoid rewriting until compaction)
   Read cost: higher — merge base + logs at read time
   Write cost: very low (just append a small log entry)
   Best for: streaming upserts at high volume, write-heavy
```

```
11.3 The On-Disk Layout
s3://bucket/my_table/
  ■■■ partition=2024
01/
  ■     ■■■ <fileId>_<commitTime>_<writeToken>.parquet    ← base file
  ■     ■■■ .<fileId>_<commitTime>_<writeToken>.log.1     ← log file (MoR only)
  ■     ■■■ .<fileId>_<commitTime>_<writeToken>.log.2
  ■■■ .hoodie/
  ■     ■■■ hoodie.properties                              ← table config
  ■     ■■■ 20240301120000.commit                          ← commit metadata
  ■     ■■■ 20240301120000.commit.requested
  ■     ■■■ 20240301120000.inflight
  ■     ■■■ 20240301130000.deltacommit                     ← MoR commit
  ■     ■■■ 20240301140000.compaction                      ← compaction action
  ■     ■■■ ...
KEY FILES:
  hoodie.properties      — table type, key gen, etc.
  *.commit / *.deltacommit / *.compaction — timeline entries (one per action)
  base files (Parquet)   — versioned per file group
  log files (Avro)       — MoR change logs
```

11.4 File Groups & Slices HUDI ORGANIZES DATA BY FILE GROUP, not just by partition. FILE GROUP = a set of records, identified by a fileId.

```
  Each file group is a "shard" of the partition.
  Records are routed to file groups via the record key (hashed or sorted).
FILE SLICE = one version of a file group.
  Each commit creates new slices.
  Old slices kept for time travel, eventually cleaned.
EXAMPLE:
  partition=2024
01/
    fileId=abc123:
      Slice at t1: abc123_t1.parquet
      Slice at t2: abc123_t2.parquet + abc123_t2.log.1
      Slice at t3: abc123_t3.parquet (compacted)
This design is the foundation of efficient upserts:
  Update finds the file group containing the key, rewrites/logs only that group.
  Not the whole partition.
```

```
11.5 The Timeline
HUDI'S COMMIT LOG = the TIMELINE.
Each commit is an "instant" with a timestamp.
Instants have a state lifecycle:
  REQUESTED → INFLIGHT → COMPLETED
Each instant has an action type:
  commit       — CoW write
  deltacommit  — MoR write (log file)
  compaction   — MoR compaction
  clean        — file cleanup
  restore      — rollback
  savepoint    — snapshot marker
READING THE TIMELINE TELLS YOU:
  What's been committed, what's in progress, what failed.
QUERY-TIME:
  Hudi reads timeline to know current valid file slices.
  Snapshot isolation via timeline state.
```

#### 12. Hudi's Upserts, Indexes & Timeline

```
12.1 Upserts: The Killer Feature
HUDI'S UPSERT API:
  spark.write.format("hudi")
    .option("hoodie.datasource.write.operation", "upsert")
    .option("hoodie.datasource.write.recordkey.field", "user_id")
    .option("hoodie.datasource.write.precombine.field", "updated_at")
    .mode("append")
    .save(table_path)
WHAT HAPPENS:
```

#### 1. For each incoming record, compute its KEY.

#### 3. CoW: rewrite the file group's base file with merged data.

```
  4. Commit by updating the timeline.
PRECOMBINE FIELD:
  When multiple updates for the same key arrive, the "winner" is the one
  with the highest precombine value (typically a timestamp or version).
  → Built-in deduplication.
```

```
12.2 Indexes: Where Hudi Differs
HUDI HAS RICH INDEX TYPES TO MAP KEYS TO FILE GROUPS.
WHY: efficient upserts need to know "which file group does this key live in?"
INDEX TYPES:
  SIMPLE        — full scan of partition to find matching keys
                  (cheap setup, expensive at scale)
  BLOOM         — Bloom filter per file
                  default for CoW; good when keys are mostly inserts
  GLOBAL BLOOM  — Bloom filter across partitions
                  for cross-partition key uniqueness
  HBASE         — external HBase as index (very fast, ops-heavy)
  RECORD-LEVEL  — Hudi 0.14+ built-in index in metadata table
                  fast like HBase without external system
  BUCKET        — hash-partition keys to fixed buckets
                  no index lookup needed; deterministic mapping
THE INDEX CHOICE IS THE PRIMARY HUDI PERFORMANCE LEVER.
Pick wrong → upserts crawl. Pick right → blazing.
```

```
12.3 Compaction (MoR-specific)
MoR PERIODICALLY MERGES log files into base Parquet files.
INLINE COMPACTION:
  Triggered every N delta commits.
  Blocks the writer.
ASYNC COMPACTION:
  Separate process compacts in background.
  Writer keeps appending; reader sees pre-compaction or post (depending on timing).
CONFIG:
```

```
  hoodie.compact.inline = true
  hoodie.compact.inline.max.delta.commits = 5  # trigger after 5 delta commits
  hoodie.compact.schedule.inline = true / false
WHEN COMPACTION MATTERS:
   Read latency degrades as log size grows
   Storage cost increases as logs accumulate
   Pick compaction frequency to balance read perf vs write throughput
```

```
12.4 Read Modes
HUDI EXPOSES THREE READ MODES:
SNAPSHOT  (default)
  Reads the latest committed state.
  CoW: just read base Parquet files.
  MoR: merge base + log files on the fly.
INCREMENTAL
  Reads only changes between commits.
  Powers CDC-style consumers.
  spark.read.format("hudi")
       .option("hoodie.datasource.query.type", "incremental")
       .option("hoodie.datasource.read.begin.instanttime", "20240301120000")
       .load(...)
READ-OPTIMIZED  (MoR-only)
  Reads ONLY base Parquet files. Skips log files.
  Faster but slightly stale.
  Good for: dashboards where 5-minute staleness is OK in exchange for speed.
```

```sql
12.5 When Hudi Wins
HUDI'S SWEET SPOT:
  ✓ High-volume streaming upserts (CDC, IoT, user-event streams)
  ✓ Sub-minute freshness requirement
  ✓ Mostly-update workloads (rather than mostly-append)
  ✓ Tables with hot keys needing many updates per partition
  ✓ Workloads where MoR's write speed matters more than read latency
WHERE DELTA / ICEBERG WIN:
  ✗ Read-heavy analytics workloads
  ✗ Simpler operational model (Hudi is the most complex of the three)
  ✗ Multi-engine access (Hudi support in non-Spark engines is improving but lags)
  ✗ Smaller / simpler tables
MARKET REALITY (2024):
  Hudi is the smallest of the three by mindshare.
  Strong in specific niches (Uber, ByteDance, financial streaming).
  Most new lakehouse projects pick Delta or Iceberg.
■ Principal mindset: Hudi's MoR architecture is genuinely innovative for upsert-heavy workloads. If you have
10TB/day of CDC events with row-level updates, Hudi's write performance can be 5-10x Delta's. But operational
complexity (timeline, compaction tuning, index selection) is significant. Most teams overestimate their upsert volume
and pick Hudi when Delta/Iceberg would have been simpler. Pick Hudi when your workload genuinely demands it;
don't pick it because it's interesting.
```

## PART V — COMPARISON & PICKING

#### 13. Feature-by-Feature Comparison

```
13.1 The Big Matrix
Feature
Delta
Iceberg
Hudi
ACID
✓
✓
✓
Time travel
✓
✓
✓
MERGE / UPSERT
✓ (DV-enhanced)
✓ (delete files)
✓ (best perf)
Schema evolution
✓ + col mapping
✓ (best, by ID)
✓ (basic)
Partition evolution
✗ (rewrite)
✓ (no rewrite)
✓ (cluster keys)
Hidden partitioning
✗
✓
✗
CDC feed
✓ (CDF)
✓ (changelog scan)
✓ (incremental)
Streaming write
✓ (Spark)
✓ (Flink, Spark)
✓ (best perf)
Spark support
✓✓✓
✓✓✓
✓✓✓
Trino support
✓✓
✓✓✓
✓ (read-only)
Snowflake support
✓ (read)
✓ (read+write)
✗
BigQuery support
✓ (read)
✓ (read)
limited
Flink support
✓ (writes)
✓✓ (best)
✓✓
Open governance
Linux Foundation
Apache top-level
Apache top-level
Maturity
High
High
High
```

```
13.2 Where Delta Wins
✓ Databricks ecosystem (deepest integration, Unity Catalog, Photon)
✓ MERGE performance (especially with Deletion Vectors)
✓ Mature; longest-running in production at scale
✓ Simpler mental model (flat log)
✓ Best Spark Structured Streaming integration
✓ Liquid Clustering (newer, incremental clustering)
✓ Change Data Feed for downstream propagation
WEAK SPOTS:
  ✗ Multi-engine support has lagged (improving fast in 2024)
  ✗ Partition evolution requires rewrite
  ✗ Catalog was tied to Hive Metastore or Databricks (Unity now open)
```

```
13.3 Where Iceberg Wins
✓ Multi-engine support (Snowflake, BigQuery, Trino, Spark, Flink all native)
✓ Hidden partitioning (best feature for query authors)
✓ Partition evolution without rewrite
✓ Column-ID-based schema evolution (most robust)
✓ Apache governance (no single vendor controls it)
✓ REST catalog enabling open governance ecosystem
✓ Better for very large tables (PB-class with 100K+ partitions)
```

```
WEAK SPOTS:
  ✗ More complex metadata layer (tree vs log)
  ✗ Commit overhead slightly higher
  ✗ MERGE was historically slower than Delta (catching up)
  ✗ Catalog dependency is more critical (must run a catalog service)
```

```
13.4 Where Hudi Wins
✓ Streaming upserts at extreme volume
✓ MoR table type for write-heavy workloads
✓ Record-level index (fast key→file lookup)
✓ Incremental read for CDC consumers
✓ Origin story aligned with operational use cases
WEAK SPOTS:
  ✗ Most complex of the three
  ✗ Smaller community / less mindshare
  ✗ Multi-engine support weaker
  ✗ Operational tuning has steep learning curve
```

13.5 The Sub-Decision: Catalog Format DELTA + UNITY CATALOG — Databricks-best UX DELTA + Hive Metastore — Spark-only, legacy DELTA + Glue Catalog — AWS-only, fewer features DELTA + uniform — Delta tables exposed via Iceberg REST (Databricks Uniform, 2023) ICEBERG + REST Catalog — multi-engine, future-proof ICEBERG + Glue Catalog — AWS-only, mature ICEBERG + Polaris — Snowflake-friendly ICEBERG + Unity Catalog — Databricks-friendly Iceberg HUDI + Hive Metastore — typical HUDI + Glue Catalog — AWS-managed Hudi shops

#### 14. Performance & Cost Characteristics

  - 14.1 The Performance Levers
- ACROSS ALL THREE FORMATS, performance depends on:
- 1. FILE SIZE — too small = metadata overhead; too large = poor parallelism
- Target: 128 MB - 1 GB per file
- 2. PARTITIONING — query patterns must match partition keys
- 3. CLUSTERING / SORTING within files
- Z-ORDER (Delta), Sort orders (Iceberg), Clustering keys (Hudi)
- Critical for selective filters on non-partition columns
- 4. METADATA SIZE — manifest counts (Iceberg), log files (Delta), timeline (Hudi)
- Periodic compaction / checkpoint required
- 5. STATISTICS QUALITY — min/max/null counts per file
- Outdated or missing stats = poor pruning

```
14.2 Read Performance
READING IS BOUNDED BY:
  1. METADATA READ (manifest list / log)
     Delta: O(checkpoints + recent commits) per table
     Iceberg: O(snapshots) at root + manifest traversal
     Hudi: O(timeline scan) + file slice resolution
  2. FILE PRUNING (which files to actually read)
     Better stats = more pruning = less I/O
  3. DATA READ + DECOMPRESSION (the bulk of cost)
     Parquet + zstd/snappy compression
ROUGH ORDER (typical analytical query):
  Iceberg ≈ Delta < Hudi (CoW) < Hudi (MoR with logs to merge)
THE FORMAT IS RARELY THE BOTTLENECK in real-world queries.
Pruning quality and file size dominate.
```

14.3 Write Performance WRITE PATH MATTERS for streaming and bulk loads. PURE APPENDS: All three similar — write Parquet files, update metadata.

```
UPSERTS (write a row that might already exist):
  Delta:    rewrite affected files (or use DVs for hot keys)
  Iceberg:  rewrite or use position deletes / equality deletes
  Hudi:     CoW rewrites; MoR appends to log (much faster)
DELETES:
  Delta:    rewrite files (or DVs)
  Iceberg:  position delete files or equality delete files
  Hudi:     log-based delete in MoR
ICEBERG'S DELETE FILES:
  Position deletes: "rows at offset X in file Y are deleted"
  Equality deletes: "rows matching predicate Z are deleted"
  Readers merge these at scan time.
  Like Hudi MoR — defers the rewrite cost.
```

```
DELTA DELETION VECTORS:
  Bitmap of deleted rows per data file.
  Same idea, different implementation.
These deferred-delete features have brought Delta and Iceberg
close to Hudi MoR's write performance for many workloads.
```

```
14.4 Storage Cost
ALL THREE store data as Parquet (or similar).
Storage cost = base data + metadata overhead + retained old versions.
METADATA OVERHEAD:
  Delta: tiny (JSON log files + occasional checkpoint Parquet)
  Iceberg: small (Avro manifests + JSON table metadata)
  Hudi: moderate (timeline directory + log files until compaction)
RETAINED VERSIONS:
  Same trade-off across formats: longer retention = more storage.
  Tune retention windows based on time-travel needs.
COMPACTION OVERHEAD:
  Delta: OPTIMIZE rewrites files; old versions retained until VACUUM
  Iceberg: rewrite_data_files action; old snapshots retained
  Hudi: compaction merges logs into base files; old slices retained until clean
THE STAFF-LEVEL MENTAL MODEL:
  Cost = active_data + retention_factor × (old_files + change_files)
  retention_factor depends on update churn and retention window
```

```
14.5 The Real-World Performance Story
HONEST ASSESSMENT (2024):
   For READ-heavy analytics: all three are similar in well-tuned setups.
    Differences emerge at extreme scale or specific access patterns.
   For WRITE-heavy streaming: Hudi MoR can be 2-5x faster.
    But Iceberg with equality deletes / Delta with DVs are closing the gap.
   For MERGE: depends heavily on clustering. With proper Z-ORDER /
    sort orders, all three handle 100s of GB MERGE in minutes.
DO NOT PICK BASED ON BENCHMARKS YOU READ.
Benchmarks are workload-specific; vendors publish wins, not losses.
Pick based on:
   Engine ecosystem you need
   Operational simplicity for your team
   Vendor relationships and roadmap
   Specific feature needs (partition evolution? Iceberg. MoR? Hudi.)
■ Production scenario: A team benchmarked Delta vs Iceberg for their CDC workload and chose Iceberg based on
a 20% read performance edge. After migrating they discovered their Spark version had immature Iceberg writer
integration, leading to subtle data loss on concurrent writes. Six months of debugging later, they migrated back to
Delta. Lesson: the format is a small part of the production picture. Engine maturity, catalog reliability, and operational
know-how matter more than synthetic benchmark numbers.
```

#### 15. The Open Table Format Wars (2024-2026)

15.1 The Stakes By 2024, every major data platform vendor had picked a side or hedged. The format that becomes the de facto standard determines: which catalogs win, which engines see deeper investment, where governance tooling consolidates. This is the biggest standardization moment in data engineering since SQL itself.

```
15.2 The 2024 State
DELTA LAKE
  Owner: Databricks (open-sourced via Linux Foundation 2019)
  Position: Dominant within Databricks (~70% of customers); growing externally
  Strategy: Open up — Unity Catalog open-sourced 2024;
            Delta tables exposed via Iceberg REST (Uniform).
  Risk: still perceived as "the Databricks format"
APACHE ICEBERG
  Owner: Apache Software Foundation (Netflix-founded)
  Position: Multi-engine darling; native support in
            Snowflake (since 2022), BigQuery (since 2023), Trino, Spark
  Strategy: Be the neutral standard. Win via openness.
  Risk: success depends on continued multi-vendor investment
APACHE HUDI
  Owner: Apache Software Foundation (Uber-founded)
  Position: Strong niche (streaming upserts); smaller share overall
  Strategy: Excel at workloads where it's structurally better (MoR streaming)
  Risk: getting squeezed by Delta+DV and Iceberg+position deletes
```

```
15.3 The Key Moves of 2024
 SNOWFLAKE → POLARIS (open-source Iceberg REST catalog, June 2024)
  Snowflake commits to Iceberg as the open standard.
  Polaris donated to Apache (2024).
 DATABRICKS → ACQUIRED TABULAR (June 2024, $1B+)
  Tabular = founded by Iceberg creators.
  Databricks signals serious Iceberg commitment.
 DATABRICKS UNIFORM (2023, expanded 2024)
  Delta tables exposed via Iceberg REST simultaneously.
  No migration needed to be 'Iceberg-readable'.
 UNITY CATALOG OPEN-SOURCED (June 2024)
  Databricks's catalog now community-driven.
  Supports Delta, Iceberg, Hudi.
 SNOWFLAKE NATIVE ICEBERG WRITES (2024)
  Snowflake can write Iceberg tables to external storage.
  Customers can use Snowflake compute on lake-stored data.
 BIGQUERY ICEBERG MANAGED TABLES (2024 preview)
  Native Iceberg storage option in BigQuery.
THE PATTERN:
  Every major vendor is converging on Iceberg as the open standard.
  Delta remains for Databricks-centric workloads.
  Hudi for specific use cases.
THE FORMAT WAR IS ENDING.
The CATALOG WAR is just beginning.
```

15.4 Predictions (Staff-Level Reasoning) HIGH CONFIDENCE:  Iceberg becomes the dominant interoperability format by 2026  Delta remains strong within Databricks; uniformity blurs the line  Hudi stays specialized in streaming-upsert niches  Catalog choice becomes more strategic than format choice UNCERTAIN:  Will catalogs consolidate (Polaris vs Unity vs REST)?  Will vendors offer "managed Iceberg" as the default storage? (Snowflake-managed Iceberg, AWS S3 Tables, etc.)  How will governance unify across catalogs? WILDCARDS:  S3 Tables (AWS 2024) — managed Iceberg with built-in optimizations  Lance, Vortex — newer formats targeting ML/AI workloads  Materialized views: who owns them across engines? ■ Principal mindset: If you're picking a format in 2024-2025, the staff-level answer is: Iceberg for new projects with multi-engine ambitions; Delta if you're Databricks-native and want the deepest integration. Hudi for the specific streaming-upsert niche. The bigger decision is the catalog: pick one that's open (REST-compatible) and that you can swap implementations of without rewriting your data. The format is the file layer; the catalog is the API to your lakehouse. Get both right. ■ Interview angle: Q: You're starting a new lakehouse in 2025. Delta or Iceberg? Staff: Depends on the compute landscape. If your team is Databricks-centric and likely to stay, Delta — deeper integration, Liquid Clustering, Photon, Unity Catalog UX. If multi-engine matters now or in the foreseeable future (Snowflake, BigQuery, Trino), Iceberg — native support across engines, hidden partitioning, partition evolution. Hudi only if your primary workload is high-volume streaming upserts where MoR's architecture is materially better. In 2025, I'd default to Iceberg with a REST catalog for new projects — the open governance and engine support make it the safer long-term bet.

## PART VI — OPERATIONAL CONCERNS

#### 16. File Layout, Sizing & Compaction

```
16.1 The File Size Sweet Spot
TARGET: 128 MB - 1 GB per file (Parquet, compressed)
TOO SMALL (<32 MB):
   Lots of metadata overhead (manifest entries, JSON log)
   Many small files = many S3 requests = slow listing, expensive
   Each task processes too little data → scheduling dominates
TOO LARGE (>2 GB):
   Poor parallelism (each task processes one file)
   Updates rewrite huge files
   Row group boundaries less effective
DELTA:    spark.databricks.delta.optimize.maxFileSize = 1073741824  (1 GB)
ICEBERG:  write.target-file-size-bytes = 536870912  (512 MB)
HUDI:     hoodie.parquet.max.file.size = 125829120 (120 MB) (older default)
          hoodie.parquet.small.file.limit = 104857600 (100 MB)
```

```sql
16.2 The Small Files Problem
WHERE SMALL FILES COME FROM:
   Streaming writes (each micro-batch = one or more small files per partition)
   Many small upserts
   High partition cardinality (each partition has few rows but many files)
OBSERVABILITY:
  Delta: DESCRIBE DETAIL my_table → numFiles, sizeInBytes
  Iceberg: SELECT * FROM my_table.files;
  Hudi: hudi-cli stats
DETECTION RULE:
  avg_file_size = total_size / num_files
  If avg < 100 MB → compaction needed
COMPACTION FREQUENCY:
  Streaming-heavy tables: hourly
  Daily batch: daily
  Append-only logs: weekly (or as needed)
```

```
16.3 Compaction Commands
DELTA:
  OPTIMIZE my_table;
  OPTIMIZE my_table WHERE date = '2024
15';
  OPTIMIZE my_table ZORDER BY (user_id, region);
ICEBERG:
  CALL system.rewrite_data_files(table => 'my_table');
  CALL system.rewrite_data_files(table => 'my_table', strategy => 'binpack');
  CALL system.rewrite_data_files(table => 'my_table', strategy => 'sort',
                                   sort_order => 'user_id, region');
HUDI:
```

```
  # via hudi-cli or programmatic API
  scheduler.scheduleCompaction(...)
  scheduler.compact(...)
  # or via DeltaStreamer / HoodieDeltaStreamer with auto-compaction
ALL THREE: should be automated as a recurring job, not manual.
```

```sql
16.4 Clustering / Sort Orders
Z-ORDER (Delta) — multi-dimensional clustering via space-filling curve
LIQUID CLUSTERING (Delta 3.0+) — newer, incremental, better operational fit
SORT ORDER (Iceberg) — global sort within partitions
CLUSTERING KEY (Hudi) — bucket / hash within partitions
WHEN TO CLUSTER:
  ✓ Selective queries on non-partition columns
  ✓ MERGE / UPDATE / DELETE on a specific key
  ✓ Join performance (clustered on join key)
WHEN NOT TO:
  ✗ Tables read fully each time anyway
  ✗ Clustering on too many columns (degrades pruning)
  ✗ Tables that change shape frequently
```

```
16.5 The Operational Routine
DAILY:
   Run OPTIMIZE / rewrite_data_files on hot tables
   Run VACUUM / expire_snapshots within retention window
WEEKLY:
   Audit file sizes (alert on tables with avg file < 32 MB)
   Review manifest counts (Iceberg)
   Check log retention (Delta)
MONTHLY:
   Review partition design (too granular?)
   Review clustering keys (still aligned with queries?)
   Audit time-travel retention (paying for old versions?)
   Check for orphan files (uncommitted, never cleaned)
QUARTERLY:
   Cost review (storage growth vs business value)
   Re-evaluate format choice if ecosystem has shifted
■■ Warning: VACUUM with too-aggressive retention can permanently delete data still needed by in-flight queries
or time-travel use cases. Always vacuum with at least 7-day retention in production. Test compaction settings on
staging first.
```

#### 17. Streaming Writes & Concurrent Writers

```
17.1 Streaming Writes Into Lakehouse
SPARK STRUCTURED STREAMING → DELTA:
  stream_df.writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", "s3://path/_checkpoints/")
    .start("s3://path/my_table/")
SPARK STRUCTURED STREAMING → ICEBERG:
  stream_df.writeStream
    .format("iceberg")
    .outputMode("append")
    .option("path", "catalog.db.table")
    .start()
FLINK → ICEBERG:
  IcebergSink for Flink — first-class integration
  Excellent for high-throughput, low-latency streaming writes
FLINK → HUDI:
  HoodieSinkFunction
  Hudi's MoR shines here — log files instead of file rewrites
SPARK → HUDI:
  DataStreamWriter with format("hudi")
  Both CoW and MoR supported
```

```
17.2 The Streaming Small Files Problem
STREAMING + LAKEHOUSE = small files problem at scale.
EACH MICRO-BATCH:
   Spark Streaming: writes N files per partition (one per task)
   Even with 30-second batches, daily file count explodes
MITIGATIONS:
  1. INCREASE BATCH INTERVAL (less frequent but larger writes)
     trigger=Trigger.ProcessingTime("5 minutes") vs default seconds
```

```
  2. COALESCE BEFORE WRITE
     stream_df.coalesce(target_partitions).writeStream...
```

```
  3. SEPARATE COMPACTION JOB
     Streaming writes small files; periodic batch job compacts
     Decouples write throughput from read performance
  4. HUDI MoR
     Log files instead of new Parquet — naturally avoids the problem
```

```
17.3 Concurrent Writers
LAKEHOUSE FORMATS USE OPTIMISTIC CONCURRENCY.
CONFLICTS HAPPEN WHEN:
   Two writers modify the same files
   Two writers commit at the same version number (race)
CONFLICT-FREE PATTERNS:
  ✓ Single writer per partition
  ✓ Streaming writer + periodic compactor (different operations)
  ✓ Multiple appenders writing to different partitions
```

```sql
  ✓ Read-modify-write protected by application-level coordination
CONFLICT-RIDDEN PATTERNS:
  ✗ Multiple writers all running MERGE on same partitions
  ✗ Bulk update job + streaming writer hitting the same table
  ✗ Frequent retries without backoff (thundering herd)
WHEN A CONFLICT OCCURS:
  Delta:    DeltaConcurrentModificationException — retry from latest
  Iceberg:  CommitFailedException — retry
  Hudi:     HoodieWriteConflictException — retry
ALL THREE PROVIDE AUTO-RETRY in their writer libraries, with limits.
```

```
17.4 The Streaming + Batch Architecture
COMMON PATTERN:
  STREAMING WRITER (Spark/Flink) — small writes, hot data
       ↓
  LAKEHOUSE TABLE (Delta/Iceberg/Hudi)
       ↓
  BATCH COMPACTOR (scheduled job, hourly/daily) — rewrite for read perf
       ↓
  READERS (Trino/Spark/Snowflake/BigQuery) — query optimized table
WHY THIS WORKS:
   Streaming writer doesn't worry about file sizes
   Compactor doesn't worry about freshness latency
   Readers see (mostly) optimized layout
   Conflicts minimized: compactor reads, writes, atomically swaps
```

```
17.5 Exactly-Once Semantics
STREAMING → LAKEHOUSE EXACTLY-ONCE:
DELTA STRUCTURED STREAMING:
   Spark checkpoint tracks last committed batch
   Delta writes are idempotent per (batch_id, partition_id)
   Restart: skip batches already committed, re-process from checkpoint
  → exactly-once end-to-end with Kafka source
ICEBERG + FLINK:
   Flink checkpoint + Iceberg's atomic commit
   2PC: Flink prepares Iceberg commit at checkpoint, commits on confirmation
  → exactly-once with proper config
HUDI + SPARK STRUCTURED STREAMING:
   Same pattern as Delta; checkpoint-based idempotency
   Hudi's precombine field also deduplicates within batches
KEY: all three integrate with engine checkpointing to provide
     exactly-once semantics. This was the original problem with raw Parquet.
■ Interview angle: Q: You're streaming 50 GB/hour from Kafka into a lakehouse table that's also queried by
analysts. How do you architect this?
Staff: Decouple the writers. (1) Streaming writer with frequent commits (30-60 sec) but a coalesce to bound file
count. (2) Hourly compaction job — OPTIMIZE on Delta, rewrite_data_files on Iceberg, scheduled compaction on
Hudi. (3) Sort/cluster on common query columns during compaction. (4) Reader queries use snapshot isolation —
they read a consistent view even while writer commits. (5) Conflict resolution: streaming writer and compactor rarely
conflict because they operate on different files (streaming writes to new files; compactor rewrites old ones). Set up
retries on the writer with exponential backoff just in case. The pattern is robust at most scales.
```

#### 18. Multi-Engine Access: Spark, Trino, Snowflake,

18.1 The Multi-Engine Promise One of the lakehouse's main pitches is: store data once, query with whatever engine fits the workload. Spark for ETL, Trino for ad-hoc SQL, Snowflake for BI users, BigQuery for ML, Polars for local analysis. The reality is improving fast but has gaps.

```
18.2 The Engine Support Matrix
Engine
Delta Read
Delta Write
Iceberg Read
Iceberg Write
Hudi Read
Hudi Write
Spark
✓
✓
✓
✓
✓
✓
Trino
✓
✓
✓
✓
✓
limited
Snowflake
✓
limited
✓
✓
✗
✗
BigQuery
✓
limited
✓
preview
limited
✗
Flink
✓
✓
✓
✓
✓
✓
DuckDB
✓
✗
✓
✗
✗
✗
Polars
✓
limited
✓
limited
✗
✗
Athena
✓
✓
✓
✓
✓
limited
```

```
18.3 Engine-Specific Notes
SPARK
  All three formats: first-class support. Spark is the reference implementation.
TRINO
  Excellent Iceberg / Delta support. Hudi support read-only (improving).
  Good for ad-hoc SQL with no warm-up cost.
SNOWFLAKE
  External Iceberg: native, full read+write (as of 2024).
  External Delta: read via Uniform or direct path; write more limited.
  Snowflake-managed Iceberg (uses Snowflake storage but Iceberg format).
BIGQUERY
  BigLake supports Iceberg, Hudi, Delta as external tables (read).
  Iceberg managed tables (preview, 2024) — BigQuery stores in Iceberg format.
DATABRICKS
  Delta-native. Iceberg via Universal Format / Tabular acquisition.
  Hudi: supported but not preferred.
FLINK
  Iceberg + Hudi have first-class Flink connectors (streaming + batch).
  Delta streaming support has lagged but improving.
```

```
18.4 The Catalog Compatibility Story
ENGINES MUST READ THE SAME CATALOG to share tables.
OPTIONS:
   Iceberg REST catalog → broadest engine support (Snowflake, Spark, Trino, BQ)
   Glue Catalog → AWS-native engines
```

```
   Unity Catalog → Databricks + (increasingly) others
   Hive Metastore → legacy, still widely supported
THE PATTERN:
  Pick a catalog that all your engines support.
  Iceberg REST is the safest bet for multi-engine future.
```

```sql
18.5 Practical Multi-Engine Architecture
COMMON SETUP:
  CATALOG: Iceberg REST (e.g., Polaris, Tabular)
       ↑
       ■■■ SPARK (Databricks / EMR): ETL, ML pipelines
       ■■■ TRINO (Starburst / Athena): ad-hoc SQL
       ■■■ SNOWFLAKE: BI consumers, business users
       ■■■ FLINK: streaming ingestion
       ■■■ BIGQUERY: select analytics workloads
ONE COPY OF DATA, MANY ENGINES.
Each engine optimized for its workload.
No data movement between engines — just queries on shared storage.
CAVEATS:
   Some engines may have feature gaps (e.g., schema evolution
    quirks across engines)
   Statistics and clustering hints may not transfer
   Each engine has its own caching layer (warm vs cold reads vary)
■ Principal mindset: Multi-engine access is the lakehouse's biggest practical advantage over closed warehouses.
But it requires deliberate architectural choices: open format (Iceberg is safest), open catalog (REST-based), and
discipline about not creating engine-specific tables. The teams that get this right have a 'data ocean' where any team
can use any engine for any workload — without migrations, exports, or duplications. That's the real promise of
lakehouse.
```

#### 19. Migration Patterns & Anti-Patterns

```
19.1 Migration Triggers
TEAMS MIGRATE BETWEEN FORMATS WHEN:
   Engine support no longer aligns (e.g., adopting Snowflake)
   Vendor lock-in concerns (Delta → Iceberg)
   Performance issues (Hudi → Delta or Iceberg)
   Governance unification (multiple formats → one)
   New feature need (partition evolution, hidden partitioning)
MIGRATION IS RARELY UNIFORM.
Some tables migrate; others stay.
A multi-format lakehouse is normal during transitions.
```

```
19.2 In-Place vs Copy Migration
IN-PLACE MIGRATION (Iceberg specific):
  Iceberg can "wrap" existing Parquet files into an Iceberg table
  without rewriting data.
```

```
  CALL system.migrate('hive.db.table_name')
  → registers the table with Iceberg, points to existing files,
    generates metadata layer
  GOOD: no data copy, fast
  BAD: some optimizations (column IDs, hidden partitioning) require rewrite later
COPY MIGRATION (universal):
  Read source format, write destination format.
```

```sql
  CREATE TABLE new_iceberg_table USING ICEBERG AS
  SELECT * FROM old_delta_table;
  GOOD: clean break, optimized layout
  BAD: full data copy (storage cost, time)
HYBRID:
  In-place for initial conversion (fast cutover)
  Then rewrite in background for optimization
```

```sql
19.3 The Migration Playbook
1. INVENTORY
   List all tables. Estimate size, partition counts, update frequency.
2. PRIORITIZE
    Greatest pain (Hudi MoR being misused, etc.)
    Easiest wins (small tables, less critical)
    Multi-engine readiness needs (most critical for cross-team tables)
3. PILOT
   Migrate 3-5 tables. Validate read parity, write parity, performance.
   Stress test concurrent writes. Test rollback.
4. PARALLEL OPERATION
   For critical tables: write to both formats, read from old, validate.
   Cut over when confident.
5. CUTOVER
   Switch readers to new format. Stop writing to old. Decommission after period.
6. CLEANUP
   Delete old data after retention window (often 30-90 days for rollback safety).
```

NEVER DO BIG-BANG ENTIRE-WAREHOUSE MIGRATIONS. Always per-table or per-domain, with validation gates.

```
19.4 Format Migration Specifics
DELTA → ICEBERG (most common in 2024-2025):
  Use Databricks Uniform: same Delta table exposed via Iceberg REST.
  No data copy. Consumers can switch incrementally.
  Or: copy migration with CTAS.
ICEBERG → DELTA:
  Less common. Usually a copy migration.
  Databricks Uniform doesn't go this direction.
HUDI → DELTA / ICEBERG:
  Usually copy migration.
  Hudi's complexity makes in-place wrapping less practical.
HIVE / RAW PARQUET → ICEBERG:
  CALL system.migrate() (in-place, fastest)
  Or: snapshot-based migration (point Iceberg at existing files)
```

```
19.5 Anti-Patterns
MIGRATION ANTI-PATTERNS:
  ■ Big-bang migrations without staging
  ■ No rollback plan
  ■ Inadequate read/write validation post-migration
  ■ Migrating because of hype rather than business need
  ■ Skipping the catalog migration (data converts but catalog still old)
  ■ Not retraining the team on new format's operational tools
GENERAL LAKEHOUSE ANTI-PATTERNS:
  ■ Tiny files (< 32 MB average) without compaction
  ■ Too many partitions (1M+ small partitions = metadata explosion)
  ■ Over-clustering (clustering on 10 columns ≈ no clustering)
  ■ Never running VACUUM / expire_snapshots → unbounded storage
  ■ Treating lakehouse as a row store (point queries don't scale)
  ■ Bypassing the catalog (writing files outside table boundaries)
  ■ Catalog as a single point of failure with no HA
  ■ Different formats for different teams without governance
■ Production scenario: A team migrated 200 Delta tables to Iceberg in a 'big-bang' weekend. They had validated 5
tables in staging. After cutover, 23 tables had subtle schema differences (Iceberg's stricter type enforcement caught
Delta's lax behaviors). 10 downstream pipelines broke. The team spent 3 weeks fixing in production. Lesson: every
table needs validation, not a representative sample. A migration tool that validates row counts, sums, and schemas
before cutover is essential. Budget 2-3x the time you think it'll take.
```

## PART VII — INTEGRATIONS & ECOSYSTEM

#### 20. Lakehouse + dbt

20.1 The Marriage dbt + Lakehouse is one of the most popular combinations in modern data engineering. dbt provides the transformation framework; lakehouse provides the storage + ACID + multi-engine capabilities. They complement each other naturally. 20.2 dbt Adapters for Lakehouses dbt-databricks — best Delta support (Databricks-managed) dbt-spark — generic Spark + Delta / Iceberg / Hudi dbt-iceberg — community-maintained, growing dbt-snowflake — Snowflake with external Iceberg tables dbt-trino — Trino on Iceberg / Delta dbt-duckdb — DuckDB reading Iceberg / Delta locally dbt-fabric — Microsoft Fabric (Delta-native) dbt-athena — AWS Athena on Iceberg / Hudi KEY POINT: dbt is format-agnostic via adapters. Same dbt code can target different formats by changing the adapter.

```
20.3 Materializations on Lakehouse
dbt MATERIALIZATIONS map naturally to lakehouse operations:
TABLE → CREATE OR REPLACE TABLE
  Full rebuild per run. Atomic via lakehouse commit.
INCREMENTAL → MERGE
  MERGE strategy uses the format's MERGE (Delta MERGE, Iceberg merge, Hudi upsert).
  Highly efficient with proper clustering.
VIEW → CREATE VIEW
  Some engines (Snowflake) support views over lakehouse tables.
CUSTOM:
  Format-specific materializations for advanced patterns:
    dbt-databricks 'streaming_table' materialization
    Custom Iceberg materializations for partition evolution
    Hudi MoR custom materializations
```

```
20.4 Lakehouse-Specific dbt Patterns
# Delta-specific: Z-ORDER as a post_hook
{{ config(
    materialized='incremental',
    unique_key='user_id',
    incremental_strategy='merge',
    post_hook="OPTIMIZE {{ this }} ZORDER BY (user_id, event_date)"
) }}
# Iceberg-specific: hidden partitioning in config
{{ config(
```

```sql
    materialized='incremental',
    partition_by={'field': 'event_ts', 'data_type': 'timestamp', 'granularity': 'day'},
    incremental_strategy='merge'
) }}
# Hudi-specific: MoR for high-write tables
{{ config(
    materialized='incremental',
    file_format='hudi',
    options={
        'type': 'mor',
        'primaryKey': 'user_id',
        'preCombineField': 'updated_at'
    }
) }}
# General: time-travel for testing
SELECT * FROM {{ ref('upstream_model') }} VERSION AS OF 42
```

20.5 The Layered Pattern TYPICAL LAKEHOUSE + dbt SETUP: BRONZE (raw landed data) — direct from ingestion, lakehouse table ↓ SILVER (staging in dbt) — dbt views or incremental Iceberg tables ↓ GOLD (marts in dbt) — dbt tables, often materialized as Iceberg with Z-ORDER / hidden partitions for hot queries ↓ PLATINUM (BI extracts) — wide tables, fewer transformations on top ALL LAYERS IN THE LAKEHOUSE. All consumed by whatever engine fits (Spark for ML, Snowflake for BI, etc.). ■ Principal mindset: dbt + Lakehouse is the modern stack default. dbt brings software engineering to SQL transformations; lakehouse brings ACID + multi-engine to cheap storage. Together they replace the old warehouse-only stack at lower cost with more flexibility. The trade-off is operational complexity — you're managing catalog, format, compaction, governance separately rather than getting them bundled. Worth it for most teams above 50 engineers; questionable below.

#### 21. Lakehouse + Streaming (Kafka, Flink, Spark)

```
21.1 The Streaming Lakehouse Pattern
STREAMING SOURCES (Kafka, Kinesis, EventHub)
       ■
       ▼
STREAMING PROCESSOR (Flink, Spark Structured Streaming, Kafka Streams)
       ■
       ▼
LAKEHOUSE TABLE (Delta / Iceberg / Hudi)
       ■
       ■■■→ BATCH READERS (Spark/Trino/Snowflake)
       ■■■→ STREAMING READERS (Flink, Spark CDC)
THE LAKEHOUSE IS THE UNIFIED LANDING ZONE.
  Streams arrive in near-real-time.
  Batch readers see consistent snapshots.
  Streaming readers consume change feeds for downstream propagation.
```

```
21.2 Flink + Iceberg (the Modern Stream Stack)
FLINK HAS THE BEST ICEBERG INTEGRATION.
WRITE PATH:
  Flink job consumes Kafka topic.
  Buffers records.
  At each Flink checkpoint: commit a batch of records to Iceberg.
  Exactly-once via Flink's 2PC + Iceberg's atomic commit.
READ PATH:
  Flink can read Iceberg incrementally (snapshot-by-snapshot).
  Powers stream-to-stream pipelines through the lakehouse.
CONFIG:
   Checkpoint interval = freshness latency (e.g., 1 minute)
   Commit batch size = compaction-friendly file sizes
   Periodic compaction job to merge small files
CHALLENGES:
   Small files at high commit frequency
   Long-tail commit conflicts under heavy concurrent load
   Compaction must keep up with write rate
```

```
21.3 Spark Structured Streaming + Delta
SPARK STRUCTURED STREAMING → DELTA is the Databricks-native pattern.
PRODUCER:
  spark.readStream.format("kafka")
    ... transforms ...
    .writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", "...")
    .trigger(processingTime="1 minute")
    .start("s3://path/")
DELTA CHANGE DATA FEED (CDF):
  Downstream consumer reads as a stream of changes:
    spark.readStream.format("delta")
      .option("readChangeFeed", "true")
      .load("s3://path/")
```

```
This enables CDC-style propagation:
  Source → Kafka → Spark → Delta → CDF → Spark → another Delta → ...
```

```
21.4 The Streaming + Batch Synchronization
COMMON PROBLEM:
  Streaming writer writes constantly.
  Batch reader at 09:00 reads a snapshot at 09:00:00.000.
  Writer continues writing.
  Reader sees consistent snapshot from 09:00 — good.
  BUT: reader's "today" partition may be incomplete (data still streaming in).
SOLUTIONS:
   Watermark-based partitioning (don't read partition until watermark passes)
   Soft cutoff: read partition only after a grace period
   Use Iceberg/Delta snapshot ID for true snapshot isolation
DESIGN PATTERN:
  Streaming writer commits with metadata (timestamp / batch id).
  Batch reader filters by metadata, not by partition column alone.
```

```
21.5 The Lambda → Kappa Convergence
LAMBDA (Marz, 2011):
  Two pipelines — batch (correct, slow) + streaming (fast, approximate)
  Lambda was needed because batch couldn't be fast AND streaming couldn't be correct.
KAPPA (Kreps, 2014):
  ONE pipeline — streaming all the way. Replay from Kafka for re-computation.
LAKEHOUSE BLURS THE DISTINCTION:
  Streaming writer → lakehouse → batch and streaming readers
  Same data, multiple consumption modes.
  Re-computation = batch read of older snapshots.
THE STAFF ANSWER (2024+):
  Pure Lambda is dead.
  Kappa is the conceptual ideal, lakehouse is the practical realization.
  In practice: stream into lakehouse, batch and stream readers from there.
■ Interview angle: Q: A Kafka topic gets 5 GB/s. You need to land it in a lakehouse for BI + ML. How?
Staff: Flink → Iceberg is the strongest stack here. Flink handles 5 GB/s ingest comfortably; Iceberg's atomic
commits + Flink's 2PC give exactly-once. Checkpoint every 30-60 sec → that's your freshness. Each commit creates
manifest entries; periodic rewrite_data_files job compacts files (every 15 min or hourly). Partition by event_hour with
hidden partitioning. Consumers (BI, ML) read snapshots; freshness lag is ~2x checkpoint interval. If you need <10
sec latency, lakehouse isn't the primary path — feed BI from Kafka + Flink-materialized views, while still landing in
lakehouse for historical analysis. The lakehouse is the durable log + analytical store, not the real-time query path.
```

22. CDC into Lakehouse

```
22.1 The CDC → Lakehouse Pipeline
SOURCE DB (Postgres, MySQL, Mongo, etc.)
       ■ CDC capture
       ▼
DEBEZIUM (or AWS DMS, Fivetran, etc.)
       ■ change events
       ▼
KAFKA (or direct sink)
       ■
       ▼
STREAMING PROCESSOR (Flink / Spark)
       ■ apply changes as MERGE
       ▼
LAKEHOUSE TABLE (Delta MERGE, Iceberg MERGE, Hudi upsert)
       ■ table represents current source state
       ▼
DOWNSTREAM (BI, ML, analytics)
```

```sql
22.2 The MERGE Pattern
EACH CHANGE EVENT contains:
   Operation (insert / update / delete)
   Before image
   After image
   Source metadata (table, timestamp, LSN)
MERGE INTO target tgt
USING (
  SELECT user_id, name, email, _op, _ts
  FROM cdc_batch
  WHERE _ts = (SELECT MAX(_ts) FROM cdc_batch WHERE user_id = cdc_batch.user_id)
  -- dedup within batch: take latest per key
) src
  ON tgt.user_id = src.user_id
WHEN MATCHED AND src._op = 'd' THEN DELETE
WHEN MATCHED AND src._op IN ('u', 'r') THEN UPDATE SET ...
WHEN NOT MATCHED AND src._op IN ('i', 'r') THEN INSERT ...
KEY POINTS:
   Dedup within batch (multiple events for same key)
   Handle delete events
   Handle reads (snapshot of full source on init)
```

```
22.3 Format-Specific CDC Patterns
DELTA + DEBEZIUM:
   Spark Streaming consumes Kafka, runs MERGE
   Deletion Vectors make deletes much cheaper
   Z-ORDER on PK for fast MERGE locating
   CDF enables downstream cascade
ICEBERG + DEBEZIUM:
   Flink + Iceberg sink with upsert mode
   Position deletes / equality deletes for efficient delete handling
   Hidden partitioning on event time (when CDC change happened)
   Iceberg's changelog scan for downstream consumers
HUDI + DEBEZIUM:
   HoodieDeltaStreamer is purpose-built for this
   MoR shines: change events written to log files, periodically compacted
   Highest write throughput of the three
```

 Record-level index for fast key lookups

```
22.4 The Operational Trade-offs
WRITE THROUGHPUT (CDC ingestion rate):
  Hudi MoR > Delta (with DV) ≈ Iceberg (with deletes) > Hudi CoW
READ LATENCY (BI/analytics on the resulting table):
  Iceberg ≈ Delta > Hudi CoW > Hudi MoR (pre-compaction)
OPERATIONAL COMPLEXITY:
  Delta < Iceberg < Hudi (Hudi has the steepest learning curve)
MULTI-ENGINE READS:
  Iceberg > Delta > Hudi
PICK BASED ON:
   Source change velocity
   Required freshness
   Downstream engine diversity
   Team operational expertise
```

```
22.5 The Soft-Delete Question
CDC EVENTS INCLUDE DELETES.
  Should the lakehouse hard-delete or soft-delete?
HARD DELETE:
  ✓ Reflects source faithfully
  ✗ Loses history (can't audit who was deleted when)
  ✗ Compliance: easier (GDPR right-to-be-forgotten)
SOFT DELETE (deleted_at column):
  ✓ Preserves audit history
  ✓ Easier to detect "deleted recently" cohorts
  ✗ Storage grows; query filters needed
HYBRID:
  Soft-delete in raw layer (bronze), hard-delete in marts (gold).
  Best of both: audit trail when needed, clean state for consumers.
■ Principal mindset: CDC into a lakehouse is the modern pattern that replaces nightly batch dumps of operational
databases. Done well, it gives analytical systems near-real-time access to operational data without burdening the
source. Done poorly, you get duplicates, missing deletes, schema drift, and silent data loss. The staff-level
discipline: idempotent MERGE patterns, dedup within batch, explicit handling of deletes, schema-evolution
coordination with source-system owners, and end-to-end row-count reconciliation.
```

#### 23. Governance: Permissions, Quality, Lineage

```
23.1 The Governance Layers
LAKEHOUSE GOVERNANCE SPANS:
  CATALOG       → who can see what tables
  TABLE-LEVEL   → who can read/write which tables
  COLUMN-LEVEL  → PII masking, row-level security
  AUDIT         → who accessed what, when
  LINEAGE       → upstream/downstream dependencies
  QUALITY       → tests, expectations, monitoring
```

```
23.2 Permissions Patterns
CATALOG-NATIVE PERMISSIONS:
  Unity Catalog: GRANT SELECT ON TABLE x TO ROLE y
  Glue Catalog + Lake Formation: fine-grained ACLs
  Polaris: RBAC on tables, schemas, namespaces
  REST Catalog: implementations vary
ENGINE-NATIVE PERMISSIONS:
  Snowflake / BigQuery have their own RBAC on top of external tables
  Spark + Ranger / Apache Sentry for fine-grained
  Trino: SQL-standard GRANT / REVOKE
CHALLENGE: MULTI-ENGINE ACCESS REQUIRES UNIFIED PERMISSIONS
  If Spark and Snowflake both read the same table, who decides who can read?
  Catalog-level permissions (Unity, Polaris) are the cleanest answer.
ANTI-PATTERN: managing permissions per-engine
  → permission drift → governance impossible at scale
```

```
23.3 Row-Level Security & Column Masking
ROW-LEVEL SECURITY:
  User A sees only rows where region = A's region.
  Implementations:
     Unity Catalog: row-level filters
     Snowflake: row access policies
     Iceberg: views with filters (less granular)
COLUMN MASKING:
  PII columns shown as hashed / redacted to certain users.
  Implementations:
     Unity Catalog: column masking functions
     Snowflake: masking policies
     BigQuery: data masking via column ACLs
THE LANDSCAPE:
  Catalog-native solutions (Unity, Polaris) are catching up to warehouses.
  In 2024: still a gap for some patterns.
  Plan for it; ask vendors about their roadmap.
```

```
23.4 Lineage
LAKEHOUSE LINEAGE:
   Which jobs write to this table?
   Which queries read it?
   What's upstream / downstream?
SOURCES:
   dbt manifest (transformation lineage)
```

```
   OpenLineage (engine-level events: Spark, Flink, Airflow, dbt)
   Snowflake / BigQuery query history
   Catalog metadata (lineage extensions in Unity, Polaris)
CONSUMERS:
   DataHub, OpenMetadata, Atlan, Marquez (visualization, catalog)
   Internal data catalogs
OPERATIONAL VALUE:
   Impact analysis: "If we drop this column, what breaks?"
   Root cause: "Why is this dashboard wrong?"
   Compliance: "Where did this PII end up?"
```

```
23.5 Quality Monitoring
TESTING AT MULTIPLE LAYERS:
INGEST LAYER:
   Schema validation (Avro / Protobuf via Schema Registry)
   Row count vs source
   Freshness checks
dbt LAYER:
   Generic tests (unique, not_null, accepted_values, relationships)
   dbt-expectations for statistical checks
   Custom singular tests
RUNTIME / CONTINUOUS:
   Elementary (open-source dbt observability)
   Monte Carlo, Bigeye, Anomalo (commercial)
   Great Expectations, Soda Core
THE STAFF-LEVEL MENTAL MODEL:
  Tests catch known problems.
  Monitoring catches unknown problems.
  You need both.
```

## PART VIII — INTERVIEW PRACTICE

#### 24. The Staff-Level Lakehouse Interview

  - 24.1 Question Categories
- 1. CONCEPT     — "Explain how Delta achieves ACID"
- 2. ARCHITECTURE — "Design a lakehouse for [scenario]"
- 3. COMPARISON  — "Delta vs Iceberg vs Hudi for [workload]"
- 4. OPERATIONS  — "Compaction strategy for streaming writes"
- 5. MIGRATION   — "Migrate from Hive to Iceberg with no downtime"
- 6. PERFORMANCE — "MERGE is slow; how do you debug and fix?"
- 7. SCALE       — "100,000 tables, 50 PB; what breaks first?"

```
24.2 The Architecture Framework
STEP 1: WORKLOAD CHARACTERIZATION
   Read pattern: ad-hoc SQL? scheduled batch? ML? streaming?
   Write pattern: append-only? upsert-heavy? bulk?
   Volume: GB/day? TB/day? Total: TB? PB?
   Latency: real-time? near-real-time? batch?
   Engines: Spark? Trino? Snowflake? BigQuery?
   Multi-engine future?
STEP 2: FORMAT SELECTION
  Based on workload + engine diversity:
   Multi-engine future + read-heavy → Iceberg
   Databricks-centric + write-heavy → Delta
   Streaming upsert at scale → Hudi (MoR)
   Read-only multi-engine warehousing → Iceberg
STEP 3: CATALOG SELECTION
   Multi-engine → Iceberg REST (Polaris, Tabular)
   All-AWS → Glue
   Databricks-only → Unity (now open)
   All-Snowflake → Snowflake-managed or Polaris
STEP 4: PARTITIONING & CLUSTERING
   Partition by most common WHERE column (often event_date)
   Hidden partitioning if Iceberg
   Z-ORDER / Liquid Clustering / Sort orders for selectivity
STEP 5: FILE / COMMIT TUNING
   Target file size: 256 MB - 1 GB
   Compaction frequency based on streaming write rate
   Snapshot retention based on time travel needs
STEP 6: WRITE PATH
   Streaming: Flink (Iceberg) or Spark Streaming (Delta) or Hudi DeltaStreamer
   Batch: dbt, Spark, etc.
   CDC: Debezium → Kafka → MERGE pattern
   Concurrent writers: design to avoid same-partition contention
STEP 7: GOVERNANCE
   Permissions via catalog
   Lineage via OpenLineage + DataHub/Atlan/etc.
   Quality via dbt tests + continuous monitoring
   PII / compliance: column masking, audit, GDPR delete patterns
```

```
STEP 8: OBSERVABILITY
   Storage growth per table
   Commit success rate, conflict rate
   File count and avg file size
   Read query latency (per engine)
   End-to-end freshness (source → consumer)
```

```
24.3 Worked Example: PB-Scale Lakehouse for Mixed Workload
INTERVIEWER: "Design a lakehouse for 50 PB of data, mix of:
              - 10 TB/day clickstream (append-only, BI)
              - 5 TB/day CDC from production DBs (upsert, BI + ML)
              - 100 GB/day curated facts (dbt-managed, BI)
              Multi-engine: Spark for ETL, Trino for ad-hoc, Snowflake for BI."
STAFF RESPONSE:
"CLARIFY: latency targets? compliance requirements (GDPR)? team size /
 expertise? budget?
 ASSUMED: 5-minute freshness for clickstream + CDC; GDPR applies;
 30-engineer team; cost-sensitive but not extreme.
 FORMAT: Iceberg.
   Multi-engine requirement makes Iceberg the natural choice.
   Snowflake supports it natively; Trino has best-in-class support;
   Spark and Flink are excellent.
 CATALOG: Iceberg REST (Polaris, hosted on K8s or managed by vendor).
   Permits all three engines to share governance.
 TABLE STRUCTURE:
   BRONZE (raw landed):
     clickstream_raw  — Iceberg, partitioned by hour(event_ts), append-only
     cdc_<table>_raw  — Iceberg, partitioned by hour(_commit_ts), append-only
   SILVER (cleaned + merged):
     clickstream_silver — Iceberg, partitioned by day(event_ts), hidden,
                          Z-ORDER on user_id; built via Flink streaming
     <table>_silver     — Iceberg with equality deletes; MERGE applied from
                          CDC; partitioned by day(updated_at), Z-ORDER on PK
   GOLD (marts):
     fct_orders, dim_users, etc.  — Iceberg, built via dbt
     hidden partitioning, smaller (100 GB-class) tables
 WRITE PATH:
   Clickstream: Kafka → Flink → Iceberg (1-minute checkpoints, 5-min compaction)
   CDC:         Debezium → Kafka → Flink → Iceberg with MERGE pattern
   Curated:     dbt-iceberg adapter, daily/hourly schedule via Airflow
 OPERATIONAL JOBS:
   Hourly: rewrite_data_files for clickstream + CDC silver tables
   Daily:  expire_snapshots (keep 30 days for time travel)
           rewrite_manifests if manifest count grows
   Weekly: optimize sort orders if access patterns changed
 GOVERNANCE:
   Polaris RBAC on tables/namespaces
   Column masking for PII (email, phone) via catalog policies
   OpenLineage feeding DataHub for impact analysis
   dbt tests + Elementary for continuous monitoring
 SCALING CHECKPOINTS:
    Storage: 50 PB across S3 buckets (lifecycle to S3 Glacier for cold tables)
    Catalog: Polaris HA setup, monitor commit latency
    Engines: separate Spark clusters per team, Trino on K8s, Snowflake reserved
```

```
    Costs: lifecycle policies, retention reviews, query cost monitoring
 RISKS:
    Catalog as SPOF — multi-AZ Polaris deployment
    Streaming small files — aggressive compaction schedule
    Schema evolution drift — contracts at silver→gold boundary
    Engine version mismatch — coordinate version bumps across teams"
WHAT MAKES THIS STAFF-LEVEL:
   Started with clarifying questions
   Picked format based on requirements, not preferences
   Addressed governance and operations, not just storage
   Named concrete tools and trade-offs
   Mentioned what could go wrong
```

```
25. War Stories
■ Production scenario: The Manifest Explosion. An Iceberg table received 10 commits per minute for a year. The
metadata directory had 5 million manifest entries. Query planning time exceeded 2 minutes per query — the catalog
had to read all manifests to plan even simple SELECTs. Fix: scheduled rewrite_manifests job, plus aggressive
expire_snapshots. Manifest count dropped 99%, query planning returned to 200ms. Manifest hygiene is critical at
high commit rates.
■ Production scenario: The VACUUM Disaster. A team ran VACUUM RETAIN 0 HOURS to 'clean up' a Delta
table. An in-flight Spark query had open file handles on old versions; VACUUM deleted those files. The query
errored out with 'file not found'. Worse: a downstream consumer doing time-travel queries against that table now had
broken reads. Recovery required restoring from a Glue Catalog snapshot. Lesson: never use RETAIN 0 in
production; the default 7 days is there for a reason.
■ Production scenario: The Hudi Compaction Crisis. A MoR Hudi table had inline compaction disabled and was
being written to at 1 GB/min. After 48 hours, log files consumed 30 TB. Read performance was atrocious. Async
compaction couldn't keep up because it had to coordinate with the writer. Fix: scale compaction to a separate Spark
cluster with higher resources; convert to CoW for read-critical partitions; tune compaction.inline.max.delta.commits.
Hudi's operational complexity is real.
■ Production scenario: The Catalog Failover. A team ran Glue Catalog in a single region. Glue had a partial
outage. Reads from S3-stored Iceberg tables failed because the catalog was unreachable. Even though the DATA
was in S3 and unaffected, consumers couldn't resolve table → current metadata location. Lesson: catalog HA is just
as important as storage HA. Plan for catalog failure: multi-region catalog, cache layer at consumers, graceful
degradation.
■ Production scenario: The Engine Version Mismatch. Spark 3.3 writes to a Delta table with protocol version 5.
Spark 3.1 reader refuses to read it. The team had rolled out Spark 3.3 to a downstream system but not all of them.
Half the consumers stopped working. Recovery: roll back the writer's protocol version (possible via tableprop
downgrade), then coordinate Spark upgrade across all consumers. Lesson: protocol upgrades are one-way; plan
upgrades cluster-wide and test compatibility before the writer publishes new protocol.
■ Production scenario: The Schema Evolution Trap. A Delta table had a column renamed. Column mapping was
enabled. Downstream Spark jobs handled it fine. But a Pandas consumer using the deltalake Python library didn't
fully support column mapping at that version — it returned NULL for the renamed column. Silent data loss for 3
weeks before an analyst noticed. Lesson: every consumer library / engine has slightly different format support. Test
schema evolution across the full consumer set before deploying.
```

#### 26. Cheat Sheet & Mental Models

```sql
26.1 The Six Mental Models
1. METADATA IS THE LAKEHOUSE
   The Parquet files are just files. The metadata layer (log, tree, timeline)
   IS what makes it a lakehouse. ACID = atomic metadata pointer update.
2. OCC + ATOMIC POINTER FLIP = ACID
   Both Delta (file naming + S3 conditional writes) and Iceberg (catalog CAS)
   reduce ACID to atomic update of a single small thing.
3. FILES ARE IMMUTABLE; UPDATES = NEW FILES
   UPDATE/DELETE/MERGE rewrite files. DV / position deletes / log files defer
   the rewrite cost. Always returns to: read, modify, write new, commit.
4. PARTITIONING IS THE COARSE INDEX; CLUSTERING IS THE FINE INDEX
   Partition = directory-level pruning.
   Z-ORDER / sort order = file-level pruning via stats.
   Both matter; both have limits.
5. CATALOG IS THE SECRET PROTAGONIST
   Format choice matters; catalog choice matters MORE for multi-engine.
   The catalog is where governance lives.
6. THE LAKEHOUSE IS A BATCH SYSTEM WITH STREAMING APPEARANCES
   Real-time = under 1 minute is hard.
   Near-real-time = under 5 minutes is straightforward.
   Sub-second = wrong tool; use Flink/Kafka with materialized views.
```

```
26.2 The One-Page Reference
FORMAT QUICK PICK
  Multi-engine + read-heavy   → ICEBERG
  Databricks + Delta-native    → DELTA
  Streaming upsert at scale    → HUDI (MoR)
CATALOG QUICK PICK
  Multi-engine future         → Iceberg REST (Polaris, Tabular)
  AWS-managed                 → Glue
  Databricks                  → Unity Catalog (now open)
  Legacy compat               → Hive Metastore
FILE SIZE TARGET
  128 MB - 1 GB per file (Parquet, compressed)
COMPACTION (run as scheduled job)
  Delta:    OPTIMIZE [ZORDER BY (cols)]
  Iceberg:  CALL system.rewrite_data_files(...)
  Hudi:     HoodieDeltaStreamer or programmatic
VACUUM / EXPIRE
  Delta:    VACUUM RETAIN 168 HOURS (7 days)
  Iceberg:  CALL system.expire_snapshots(...)
  Hudi:     hoodie.cleaner.commits.retained
STREAMING WRITER + COMPACTION
  Writer: small frequent commits
  Compactor: periodic rewrites
  Conflict-free by design (different operations on different files)
CDC PATTERN
  Source → Debezium → Kafka → Spark/Flink → MERGE INTO lakehouse table
  Use deletion vectors / position deletes / Hudi MoR for delete efficiency
```

```sql
TIME TRAVEL
  SELECT * FROM my_table VERSION AS OF 42;
  SELECT * FROM my_table TIMESTAMP AS OF '2024
15';
ANTI-PATTERNS
  ■ Too-small files (<32 MB avg)
  ■ Too-many partitions (>1M)
  ■ Never compacting / vacuuming
  ■ Lakehouse as OLTP / row-store
  ■ Catalog as SPOF without HA
  ■ Mixing formats without governance
```

```
26.3 What Separates Senior from Staff
SENIOR                                STAFF / PRINCIPAL
■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■
"Use Delta"                          "Delta for Databricks-native; Iceberg
                                      for multi-engine; the catalog matters
                                      more than the format at our scale"
"Add a partition"                    "Hidden partitioning by hour if Iceberg;
                                      Z-ORDER on user_id; we'll re-cluster
                                      monthly; here's the cost/benefit"
"MERGE the changes"                  "MERGE on PK with DV-enabled target;
                                      pre-dedupe in source for batch
                                      uniqueness; Z-ORDER target on the
                                      merge key so file pruning works"
"It supports ACID"                   "It supports ACID via atomic metadata
                                      pointer update; OCC means we need to
                                      design writers to minimize conflicts;
                                      here are the patterns"
"Use Iceberg"                        "Iceberg with REST catalog (Polaris) so
                                      Snowflake and Spark share governance;
                                      not Glue because we may add GCP, not
                                      Unity because we're not on Databricks"
■ Principal mindset: Lakehouse mastery at staff level requires holding the whole stack in mind: storage costs, file
layout, metadata structure, catalog choice, engine ecosystems, governance, streaming integration, and the
migration paths between them. The format is the easy part. The hard parts are: choosing the right catalog for your
future, designing for concurrent writers, operating compaction at scale, and unifying governance across engines.
These are architectural decisions with multi-year consequences.
 — End of Lakehouses Notes —
 Next in series: Cloud Data Warehouses, Data System Design
```


---

[← Back to main](./README.md)
