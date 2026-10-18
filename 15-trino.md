# Topic 15 — Trino (and Presto)

> Standalone deep-dive · The distributed SQL query engine for the lakehouse and federated analytics

[← Back to main](./README.md)

---

## How to use this document

This is the architect-level reference for Trino: how its MPP engine works, how it federates across data sources, how it compares to Spark SQL and warehouses, and when it's the right query engine for a lakehouse. Every chapter follows the four lenses: **Concept · Mechanics · Trade-offs · Interview**.

> **🧠 Principal Mindset:** Trino is the forgotten pillar of modern data stacks. Everyone talks about Snowflake and Databricks; Trino quietly powers the lakehouse at Netflix, LinkedIn, Shopify, Lyft, and most big tech companies. For multi-engine lakehouses, cross-source federation, and interactive SQL on open formats, Trino is often the right answer. Staff engineers who know only warehouses miss a major tool.

## Table of Contents

**PART I — FOUNDATIONS**
- 1. What Trino Is and Why It Exists
- 2. The History: Presto → PrestoSQL → Trino
- 3. Core Design Principles
- 4. Trino vs Spark SQL vs Snowflake/BigQuery vs DuckDB

**PART II — ARCHITECTURE**
- 5. Coordinator and Workers
- 6. The Query Lifecycle
- 7. Connectors: The Federation Model
- 8. Distributed Execution Plans

**PART III — CONNECTORS**
- 9. Hive Connector
- 10. Iceberg Connector
- 11. Delta Lake Connector
- 12. Hudi Connector
- 13. PostgreSQL / MySQL / Other RDBMS
- 14. The Full Catalog

**PART IV — PERFORMANCE**
- 15. Vectorized Execution and Operator Pipelines
- 16. Dynamic Filtering
- 17. Cost-Based Optimizer (CBO)
- 18. Join Strategies

**PART V — SQL AND FEATURES**
- 19. Standards Compliance and Dialect
- 20. ANSI SQL Features
- 21. Query Federation Patterns
- 22. User-Defined Functions

**PART VI — SECURITY AND MULTI-TENANCY**
- 23. Authentication and Authorization
- 24. Resource Groups
- 25. Row and Column Filters

**PART VII — DEPLOYMENT AND OPS**
- 26. Deployment Patterns
- 27. Starburst: Enterprise Trino
- 28. The Trino + Iceberg Lakehouse Stack

**PART VIII — INTERVIEW PRACTICE**
- 29. The Staff-Level Trino Interview
- 30. War Stories
- 31. Cheat Sheet & Mental Models

---

## PART I — FOUNDATIONS

### 1. What Trino Is and Why It Exists

#### 1.1 The One-Sentence Definition

Trino is an **open-source distributed SQL query engine** that executes interactive analytical queries across multiple data sources (data lakes, warehouses, operational databases) without moving data, scaling from gigabytes to petabytes.

#### 1.2 The Problem It Solves

```
Before Trino / Presto:
   Analytics data was in a warehouse.
   Operational data was in Postgres/MySQL/MongoDB.
   Logs were in S3.
   
   To query across these: 
      • ETL everything into one warehouse (slow, expensive)
      • Or query each separately, combine in app code
      • Or build a custom federation layer

Trino solves this:
   One SQL interface. One query. Spans multiple sources.
      SELECT ... FROM hive.events e
                 JOIN postgres.users u ON e.user_id = u.id
                 WHERE u.region = 'EU' AND e.date = '2024-03-15';
```

#### 1.3 The Shape

```
Trino is a QUERY ENGINE — not a database, not a storage system.
   • It stores no data persistently
   • It brings compute to data (via connectors)
   • It's deployed as a cluster (coordinator + workers)
   • Clients submit SQL via JDBC, HTTP, CLI, Python/Go/etc.
```

#### 1.4 Key Properties

```
   • MPP (massively parallel processing)
   • In-memory execution (streams results through operators)
   • Interactive latency (seconds, not minutes)
   • Federated: ~50+ connectors to different sources
   • ANSI SQL (very compliant)
   • Scales to thousands of workers
   • Pluggable architecture
```

#### 1.5 Who Uses Trino

```
Reference deployments:
   • Netflix (where Presto was born; thousands of nodes)
   • LinkedIn (ad-hoc analytics)
   • Shopify (lakehouse queries)
   • Lyft (federated analytics)
   • Pinterest, Twitter/X, Snap, Uber
   • Airbnb (one of the earliest adopters)

Shared pattern: Iceberg lakehouse + Trino as the interactive SQL engine
alongside Spark for batch and Flink for streaming.
```

> **🎯 Interview Angle:** Q: Why not just use Snowflake / BigQuery instead of Trino?
> **Staff:** "Three reasons: (1) Multi-source federation — Trino queries lakehouse, RDBMS, Kafka (via connector), and more in one query without ETL; warehouses can't do this cleanly. (2) Open storage — Trino is the query engine over YOUR data in YOUR S3; warehouses tie compute and storage to the vendor (though both have evolved toward external Iceberg). (3) Cost at scale — self-hosted Trino on commodity instances is often 3-5x cheaper than warehouse per TB scanned for ad-hoc workloads. The flip side: warehouses have superior UX, serverless simplicity, and bundled governance. Pick Trino when federation + open storage + cost matter; pick a warehouse when operational simplicity + analyst UX win."

### 2. The History: Presto → PrestoSQL → Trino

#### 2.1 Origins at Facebook (2012-2013)

```
2012: Facebook has 300+ PB in Hadoop. Hive is too slow for interactive queries
      (minutes per query). Facebook engineers (Dain Sundstrom, David Phillips,
      Eric Hwang, Martin Traverso) start building Presto for sub-second analytics
      on the same Hadoop data.

2013: Presto open-sourced under Apache 2.0.
      Netflix, Airbnb, LinkedIn adopt it quickly.
```

#### 2.2 Growth (2014-2018)

```
2014: Teradata, Facebook, and others contribute heavily.
      Presto gains connectors to RDBMS, Cassandra, Kafka.

2015: Netflix open-sources Big Data Portal built on Presto.
      Starburst founded (commercial Presto distribution).

2017: Facebook creators leave to found "PrestoSQL" fork.
      Facebook maintains "PrestoDB" fork.
      Two forks exist for years with similar but diverging features.
```

#### 2.3 The Rename (2020-2021)

```
2020 Dec: PrestoSQL renames to Trino.
          Resolves trademark conflict and clarifies the fork story.
          Starburst Trino is the enterprise distribution.

2021:     Trino continues rapid development.
          PrestoDB (Facebook/Linux Foundation) still exists but less momentum.
```

#### 2.4 Lakehouse Era (2022-2024)

```
2022: Trino Iceberg connector matures (production use at scale).
      Trino Delta Lake connector added.
      Starburst Galaxy (hosted Trino) launches.

2023: Iceberg + Trino becomes the modern open lakehouse default
      for interactive queries.

2024: Trino 440+ releases (Trino does weekly releases).
      Delta Kernel integration (shared library with other Delta readers).
      Hudi connector matures.
```

#### 2.5 Current State (2025-2026)

```
2025: Trino 450+ series.
   • Better REST catalog integration (Polaris, Unity Catalog)
   • Improved Iceberg performance
   • Advanced dynamic filtering

2026: Trino is the de facto open-source SQL engine for lakehouse.
   Managed options: Starburst Galaxy, AWS Athena (based on Trino),
                     Google Dataproc, Alibaba AnalyticDB.
```

### 3. Core Design Principles

#### 3.1 Decoupled from Storage

```
Trino owns NO DATA.
It reads from (and writes to) external systems via CONNECTORS.

Connectors handle:
   • Metadata lookup (what tables exist?)
   • Data access (read bytes from storage)
   • Predicate pushdown (hand filters to source where possible)
```

#### 3.2 In-Memory Pipeline Execution

```
Trino streams rows through operator pipelines IN MEMORY.
   No intermediate disk writes between stages.
   
Advantages:
   • Low latency (no disk I/O overhead)
   • High throughput for interactive queries
   
Disadvantages:
   • Memory-bound (large joins may OOM if intermediate state doesn't fit)
   • Not optimal for huge ETL (Spark can spill to disk)

In 2024+ Trino added fault-tolerant execution with disk spill
   for very large queries. Still primarily memory-first.
```

#### 3.3 MPP Architecture

```
A query is split into STAGES, each with multiple TASKS, each running in parallel.
Tasks exchange data across the network.
   
   Query → Stages → Tasks → Operator pipelines

Example: 
   SELECT COUNT(*) FROM huge_table GROUP BY country;
   
   Stage 1: scan huge_table, partial-aggregate per node
   Stage 2: shuffle by country, final-aggregate
   
   100 workers × 10 tasks per worker = 1000 parallel scanners.
```

#### 3.4 ANSI SQL First

```
Trino targets high ANSI SQL compliance.
Supports:
   • Standard window functions
   • Complex subqueries, CTEs, recursive CTEs
   • Set operations, lateral joins
   • Rich type system (array, map, row, decimal, varchar, varbinary)
   • Window and set-returning functions

Compared to Spark SQL and some warehouses:
   Trino tends to be stricter on standards compliance.
   Fewer vendor-specific extensions.
```

### 4. Trino vs Spark SQL vs Snowflake/BigQuery vs DuckDB

#### 4.1 The Comparison

| Dimension | Trino | Spark SQL | Snowflake/BQ | DuckDB |
|-----------|-------|-----------|--------------|--------|
| **Deployment** | Dedicated cluster | Spark cluster | Managed cloud | In-process |
| **Primary use** | Interactive SQL + federation | Batch + stream + ML | Managed analytics | Single-node analytics |
| **Latency** | Interactive (seconds) | Batch (minutes) | Interactive (seconds) | Interactive (seconds, single-node) |
| **Scale** | PB (clustered) | PB (clustered) | PB (managed) | TB (single node) |
| **Federation** | Best (50+ connectors) | Good (JDBC + file formats) | Limited (external Iceberg) | Good (via extensions) |
| **State** | Memory-first + spill | Memory + heavy disk spill | Managed | Memory + spill |
| **Operation** | Self-host or managed | Self-host or managed | Fully managed | None (library) |
| **Cost model** | Infra cost | Infra cost | Credits/slots | Infra or free |
| **Best for** | Lakehouse queries, federation, open-source | Batch ETL, ML, large shuffle | Analyst UX + simplicity | Local, embedded, small-to-mid |

#### 4.2 When Trino Wins

```
PICK TRINO WHEN:
   ✓ Multi-source federation (one query across lakehouse + Postgres + Kafka)
   ✓ Open lakehouse (Iceberg + Trino is the production default)
   ✓ Need interactive latency on PB-scale data
   ✓ Want to avoid warehouse vendor lock-in
   ✓ Have ops team for cluster management (or use managed Starburst/Athena)
```

#### 4.3 When Trino Doesn't Win

```
RECONSIDER TRINO WHEN:
   ✗ Workload is pure batch ETL (Spark better: disk spill, longer-running)
   ✗ Need streaming (Flink / Spark Streaming)
   ✗ Need ML (Spark MLlib or dedicated ML platforms)
   ✗ Small team wanting fully managed (Snowflake/BigQuery easier)
   ✗ Single-source analytics on small data (DuckDB cheaper)
```

#### 4.4 The Hybrid Pattern

```
Many large data stacks use multiple engines:
   • Spark for batch ETL
   • Flink for streaming
   • Trino for ad-hoc + BI
   • Snowflake for executive dashboards (optional)
   • All reading from the same Iceberg lakehouse

Each engine fits a specific workload. Trino's role: interactive federated SQL.
```

---

## PART II — ARCHITECTURE

### 5. Coordinator and Workers

#### 5.1 The Two Roles

```
COORDINATOR:
   • Parses SQL
   • Plans and optimizes the query
   • Schedules tasks on workers
   • Returns results to the client
   
   One active coordinator per cluster (plus optional backup).

WORKERS:
   • Execute tasks (operator pipelines)
   • Read data from source via connectors
   • Exchange data with other workers
   • Return results to coordinator
   
   10 - 10000 workers per cluster typical.
```

#### 5.2 Cluster Topology

```
                    CLIENT (SQL)
                       │
                       ▼
                ┌────────────┐
                │COORDINATOR │  (parse, plan, schedule)
                └──────┬─────┘
                       │
          ┌────────────┼────────────┐
          ▼            ▼            ▼
      ┌───────┐    ┌───────┐    ┌───────┐
      │WORKER │    │WORKER │ ...│WORKER │  (parallel execution)
      └───┬───┘    └───┬───┘    └───┬───┘
          │            │            │
          ▼            ▼            ▼
    [connector 1] [connector 2]  [connector 3]
    (Iceberg,     (Postgres,     (Kafka, etc.)
    Delta, etc.)  MySQL, etc.)
```

#### 5.3 Communication

```
HTTP (REST):
   Client → Coordinator: submit query
   Coordinator → Workers: schedule tasks
   Workers → Workers: exchange data (shuffles)
   Workers → Coordinator: progress + results

All over HTTP. Pluggable auth: Kerberos, OAuth, JWT, LDAP.
```

### 6. The Query Lifecycle

#### 6.1 The Full Flow

```
1. CLIENT submits SQL to coordinator.
2. COORDINATOR parses, validates, builds logical plan.
3. COORDINATOR runs CBO (cost-based optimizer) to build physical plan.
4. COORDINATOR splits plan into STAGES (plan fragments separated by shuffles).
5. COORDINATOR schedules each stage: creates TASKS on workers.
6. WORKERS execute tasks:
   a. Source tasks: read via connector
   b. Intermediate tasks: process + shuffle
   c. Final tasks: aggregate, return to coordinator
7. COORDINATOR streams results to client as they come.
```

#### 6.2 Stages

```
A stage = a plan fragment that can be executed as one unit.
Stages are separated by SHUFFLES (data redistribution across workers).

Example query:
   SELECT country, SUM(amount) FROM sales GROUP BY country

Stage 1 (SOURCE): scan sales, partial-aggregate per worker
Stage 2 (AGGREGATE): shuffle by country, final-aggregate
Stage 3 (OUTPUT): return results to coordinator
```

#### 6.3 Tasks

```
A task = one instance of a stage running on one worker.
Each task runs a pipeline of OPERATORS in sequence.

Example:
   Task "scan + filter + partial-aggregate":
      ScanOperator → FilterOperator → HashAggregateOperator
   
   Runs on one worker, operating on one slice of the input.
```

#### 6.4 Pipelines

```
Within a task, operators are organized into PIPELINES.
A pipeline = a sequence of operators that run on one thread.

   Pipeline 1: Scan → Filter → Project  (produces hash partitions)
   Pipeline 2: Exchange → Aggregate       (consumes hash partitions)

Pipelines break at EXCHANGE boundaries (where threads hand off).
```

### 7. Connectors: The Federation Model

#### 7.1 What a Connector Is

```
A CONNECTOR = a plugin that exposes an external data source as Trino tables.

The connector implements:
   • Metadata (what schemas / tables / columns exist?)
   • Splits (how to divide the data for parallel reading?)
   • Pages (how to produce rows from each split?)
   • Predicate pushdown (which filters can be handled at the source?)
```

#### 7.2 Catalog = Connector Instance

```
In Trino:
   CATALOG = a configured instance of a connector.
   SCHEMA = a namespace within a catalog.
   TABLE = a specific entity within a schema.

Fully-qualified name:
   catalog.schema.table
   
Example:
   hive.sales.orders        (Hive catalog, sales schema, orders table)
   postgres.public.users    (Postgres catalog, public schema, users table)
   iceberg.gold.revenue     (Iceberg catalog, gold schema, revenue table)
```

#### 7.3 Configuration

```properties
# etc/catalog/iceberg.properties
connector.name=iceberg
iceberg.catalog.type=rest
iceberg.rest-catalog.uri=https://polaris.example.com/
iceberg.rest-catalog.warehouse=my-warehouse
```

```properties
# etc/catalog/postgres.properties
connector.name=postgresql
connection-url=jdbc:postgresql://db.example.com:5432/prod
connection-user=analytics_ro
connection-password=***
```

Each catalog file = one connector instance = one source accessible via SQL.

#### 7.4 Federation in Action

```sql
-- Join across sources in one query
SELECT 
    o.order_id,
    o.amount,
    u.name,
    u.region
FROM iceberg.gold.orders o
JOIN postgres.public.users u ON o.user_id = u.id
WHERE o.order_date >= DATE '2024-03-01';
```

Trino:
   1. Reads orders from Iceberg
   2. Reads users from Postgres
   3. Joins them in memory
   4. Returns results

No ETL. No pre-loading. No copies.

### 8. Distributed Execution Plans

#### 8.1 Example Plan

```
Query: SELECT country, SUM(amount) FROM sales WHERE year = 2024 GROUP BY country

Logical plan:
   Project [country, sum]
      → Aggregate [country, SUM(amount)]
         → Filter [year = 2024]
            → TableScan [sales]

Physical plan (distributed):
   Stage 0 (coordinator, OUTPUT):
      Aggregate [country, SUM(partial_sum)]
         → Exchange (gathering from Stage 1)
   
   Stage 1 (workers, FINAL AGGREGATE):
      PartialAggregate [country, SUM(amount)]
         → Exchange (hash-partition by country from Stage 2)
   
   Stage 2 (workers, SOURCE):
      Filter [year = 2024]
         → TableScan [sales]
```

#### 8.2 Shuffles

```
Between stages, data is SHUFFLED:
   Partitioning strategy depends on the next operator:
      HASH (by join/group key): most common
      BROADCAST (small side of join replicated everywhere)
      SINGLE (all data to one node): for final aggregation / sort
      ROUND_ROBIN: for balancing

Shuffles are the main performance concern:
   Network I/O can dominate query time
   Dynamic filtering (next chapter) reduces shuffle data substantially
```

#### 8.3 EXPLAIN

```sql
EXPLAIN SELECT ... FROM ...;

Returns the plan tree:
   • Stages
   • Operators per stage
   • Partitioning strategies
   • Estimated row counts (from CBO)
```

```sql
EXPLAIN ANALYZE SELECT ...;

Executes the query and returns:
   • Actual row counts
   • Time per operator
   • Memory per task
   • Shuffle sizes
   • CPU time per stage
```

---

## PART III — CONNECTORS

### 9. Hive Connector

#### 9.1 The Legacy Workhorse

```
HIVE CONNECTOR:
   Reads Hive-style tables (Parquet/ORC files + Hive Metastore for metadata).
   The original Presto/Trino connector; still widely used.
```

#### 9.2 Configuration

```properties
# etc/catalog/hive.properties
connector.name=hive
hive.metastore.uri=thrift://hms:9083
hive.metastore.username=trino
hive.allow-drop-table=true
```

Supports: Parquet, ORC, Avro, CSV, JSON, SequenceFile.

#### 9.3 Legacy, But Still Common

```
Many enterprises still have Hive Metastore + Parquet lakes.
Trino's Hive connector reads them well.
Migration path: Hive → Iceberg via `CALL system.migrate(...)`.
```

### 10. Iceberg Connector

#### 10.1 The Modern Default

```
ICEBERG CONNECTOR:
   Reads and writes Iceberg tables.
   Supports V1 and V2 (and emerging V3).
   Full catalog pluggability: Hive, REST, Glue, Nessie, Polaris, Unity.
```

#### 10.2 Configuration

```properties
# etc/catalog/iceberg.properties
connector.name=iceberg
iceberg.catalog.type=rest
iceberg.rest-catalog.uri=https://polaris.example.com/
iceberg.rest-catalog.warehouse=my-warehouse

# Or Hive Metastore
# iceberg.catalog.type=hive_metastore
# hive.metastore.uri=thrift://hms:9083

# Or AWS Glue
# iceberg.catalog.type=glue
```

#### 10.3 SQL Operations

```sql
-- Create
CREATE TABLE iceberg.sales.orders (
    id BIGINT, 
    user_id BIGINT, 
    amount DECIMAL(18,2),
    order_date DATE
)
WITH (
    partitioning = ARRAY['day(order_date)'],
    sorted_by = ARRAY['user_id']
);

-- Insert
INSERT INTO iceberg.sales.orders VALUES (...);

-- Update / Delete (requires V2)
UPDATE iceberg.sales.orders SET amount = 100 WHERE id = 42;
DELETE FROM iceberg.sales.orders WHERE id = 42;

-- MERGE
MERGE INTO iceberg.sales.orders tgt
USING updates src ON tgt.id = src.id
WHEN MATCHED THEN UPDATE SET amount = src.amount
WHEN NOT MATCHED THEN INSERT VALUES (src.id, src.user_id, src.amount, src.date);

-- Maintenance (via CALL)
CALL iceberg.system.optimize('sales', 'orders');
CALL iceberg.system.expire_snapshots('sales', 'orders', TIMESTAMP '2024-01-01');
CALL iceberg.system.remove_orphan_files('sales', 'orders');
```

#### 10.4 Time Travel

```sql
-- Time travel by snapshot ID
SELECT * FROM iceberg.sales.orders FOR VERSION AS OF 12345;

-- By timestamp
SELECT * FROM iceberg.sales.orders FOR TIMESTAMP AS OF TIMESTAMP '2024-03-15';
```

### 11. Delta Lake Connector

#### 11.1 Reading Delta

```properties
# etc/catalog/delta.properties
connector.name=delta_lake
hive.metastore.uri=thrift://hms:9083
delta.metastore.type=HIVE
```

#### 11.2 Features

```
Reads Delta tables (full support in modern Trino).
Writes Delta tables (added in 2022, matured since).
Supports: time travel, deletion vectors (V3+), column mapping.

Delta Kernel integration (newer):
   Trino uses the shared Delta Kernel library.
   Benefits: feature parity with the Delta spec; same bugs across engines.
```

### 12. Hudi Connector

```properties
# etc/catalog/hudi.properties
connector.name=hudi
hive.metastore.uri=thrift://hms:9083
```

Reads Hudi tables (snapshot queries + incremental queries).
Writes support more limited than Iceberg/Delta.

### 13. PostgreSQL / MySQL / Other RDBMS

#### 13.1 JDBC Connectors

```properties
# etc/catalog/postgres.properties
connector.name=postgresql
connection-url=jdbc:postgresql://db.example.com:5432/prod
connection-user=analytics_ro
```

#### 13.2 Predicate Pushdown

```sql
-- Trino pushes down compatible predicates to Postgres
SELECT * FROM postgres.public.users WHERE created_at > DATE '2024-01-01';

-- Postgres executes the filter; only matching rows transit to Trino.
-- Trino further processes if needed.
```

Predicate pushdown is critical for performance:
- Supported: equality, inequality, range, IN
- Not pushed: complex functions, UDFs

#### 13.3 Join Pushdown

```sql
-- Join pushdown: Trino may push a join into Postgres
SELECT u.name, o.amount
FROM postgres.public.users u
JOIN postgres.public.orders o ON u.id = o.user_id;

-- If both tables are in Postgres, Trino can push the entire join.
```

### 14. The Full Catalog

Trino has 50+ connectors:

```
STORAGE:
   Hive, Iceberg, Delta Lake, Hudi
   File formats: Parquet, ORC, Avro, CSV, JSON

CLOUD STORAGE:
   S3, GCS, Azure Blob (via Hive/Iceberg/Delta connectors)

OLTP DATABASES:
   PostgreSQL, MySQL, Oracle, SQL Server, Db2, SAP HANA

NOSQL:
   MongoDB, Cassandra, Redis, Elasticsearch

STREAMING:
   Kafka

DATA WAREHOUSES:
   BigQuery, Snowflake, Redshift

SPECIALIZED:
   Prometheus, Pinot, Druid, ClickHouse
   Blackhole (dev/testing)
   Memory (in-memory tables)
   System (Trino's own metrics)
```

---

## PART IV — PERFORMANCE

### 15. Vectorized Execution and Operator Pipelines

#### 15.1 Page-Based Execution

```
Trino processes data in PAGES.
   Page = a columnar batch (default: ~1 MB or 10,000-ish rows per column).
   
Operators consume + produce pages.
Vectorized processing within pages:
   • Columnar layout in memory (one array per column)
   • Operations work on whole columns at once (SIMD-friendly)
```

#### 15.2 Why Page-Based

```
Row-at-a-time:
   • One method call per operator per row
   • Branch prediction struggles

Page-at-a-time (vectorized):
   • One method call per operator per page (thousands of rows amortized)
   • CPU cache locality (column stays warm through pipeline)
   • SIMD operations on columns
```

#### 15.3 Operator Pipelines

```
Within a task:
   Operators are organized into pipelines.
   A pipeline runs on one thread.
   Pages flow through operators sequentially.

   Pipeline:
      ScanOperator (produces pages from Parquet)
        → FilterOperator (drops rows based on predicate)
          → ProjectOperator (keeps/transforms columns)
            → PartialAggregateOperator (local aggregation)
   
   One thread, no synchronization between operators = fast.
```

### 16. Dynamic Filtering

#### 16.1 The Insight

```
Classic join:
   SELECT * FROM facts f JOIN dims d ON f.dim_id = d.id WHERE d.region = 'EU'

   Without optimization:
      1. Scan ALL facts
      2. Scan dims; filter where region = 'EU'
      3. Hash join
      
   Wasteful: facts not matching 'EU' dim_ids are scanned and sent across network.

With dynamic filtering:
      1. Scan dims; filter where region = 'EU'; extract matched dim_ids
      2. Push filter: f.dim_id IN (matched_ids) back to the facts scan
      3. Scan facts WITH filter — skip row groups/files without matching dim_ids
      4. Join only the needed rows
   
   Massive reduction in scan + shuffle data.
```

#### 16.2 How Trino Implements It

```
Trino identifies dim-fact join patterns.
Build side (dim) runs first (small).
Extracts predicate values; passes to probe side (fact) scans at runtime.
Fact scan uses the dynamic filter for partition pruning + Parquet row group pruning.

Massively improves star-schema queries.
```

#### 16.3 Enabling

```sql
-- Enabled by default in modern Trino
SHOW SESSION LIKE '%dynamic_filter%';
```

Dynamic filtering is one of Trino's biggest performance wins vs older MPP engines.

### 17. Cost-Based Optimizer (CBO)

#### 17.1 What CBO Does

```
Given a query, choose the plan with lowest estimated cost.
Cost based on:
   • Table statistics (row count, column stats, data size)
   • Join reorder possibilities
   • Partitioning / sorting information
```

#### 17.2 Key Optimizations

```
JOIN REORDERING:
   Reorder N-way joins to minimize intermediate size.
   
JOIN DISTRIBUTION:
   PARTITIONED (hash-shuffle both sides): default for large tables
   BROADCAST (replicate small side): when one side is small
   
   Trino picks automatically based on stats.

PREDICATE PUSHDOWN:
   Push WHERE predicates toward sources.
   
PROJECTION PUSHDOWN:
   Push column projection toward sources (don't read unneeded columns).
```

#### 17.3 Collecting Statistics

```sql
-- Collect stats for CBO
ANALYZE iceberg.sales.orders;

-- Specific partitions
ANALYZE iceberg.sales.orders WITH (partitions = ARRAY[ARRAY['2024-03-15']]);
```

Modern Iceberg tables have per-column stats embedded in metadata; CBO uses these directly without an explicit ANALYZE.

### 18. Join Strategies

#### 18.1 Partitioned (Hash) Join

```
Both sides are hash-partitioned by join key, redistributed.
Each worker joins its matching partition locally.

Pros: scales to any join size
Cons: shuffle cost
```

#### 18.2 Broadcast Join

```
Small side is BROADCAST to every worker.
Each worker has the full small side in memory.
Each worker joins its slice of large side locally.

Pros: no shuffle for the small side; faster
Cons: small side must fit in memory on every worker
```

#### 18.3 Automatic Selection

```
Trino's CBO picks:
   If small side's size < broadcast threshold (default 100 MB):
      → Broadcast join
   Else:
      → Partitioned join

Tune threshold:
   SET SESSION broadcast_join_threshold = '500MB';
```

#### 18.4 Hint

```sql
-- Explicit broadcast
SELECT /*+ broadcast(d) */ * 
FROM facts f JOIN dims d ON f.dim_id = d.id;
```

---

## PART V — SQL AND FEATURES

### 19. Standards Compliance and Dialect

#### 19.1 ANSI SQL

```
Trino targets high ANSI SQL compliance.
Fewer surprises for SQL-fluent users than many MPP systems.

Nearly standard-compliant for:
   SELECT, JOIN, GROUP BY, ORDER BY, HAVING, UNION
   Subqueries, CTEs (including recursive)
   Window functions (full standard)
   Set operations
```

#### 19.2 Trino-Specific Extensions

```sql
-- Array literal syntax
SELECT ARRAY[1, 2, 3];

-- Map / Row types
SELECT MAP(ARRAY['a', 'b'], ARRAY[1, 2]);
SELECT ROW(1, 'a', TRUE);

-- Lambda functions
SELECT transform(ARRAY[1, 2, 3], x -> x * 2);
SELECT filter(ARRAY[1, 2, 3, 4], x -> x > 2);
SELECT reduce(ARRAY[1, 2, 3], 0, (s, x) -> s + x, s -> s);

-- UNNEST (flatten arrays)
SELECT user_id, tag
FROM users
CROSS JOIN UNNEST(tags) AS t(tag);
```

#### 19.3 Date/Time

```sql
-- Various date functions
SELECT date_trunc('day', ts), date_add('day', 1, ts), date_diff('hour', a, b);

-- Interval
SELECT ts + INTERVAL '1' DAY;

-- Timestamp with timezone
SELECT CURRENT_TIMESTAMP AT TIME ZONE 'America/New_York';
```

### 20. ANSI SQL Features

#### 20.1 Window Functions

```sql
SELECT 
    user_id, order_date, amount,
    SUM(amount) OVER (
        PARTITION BY user_id 
        ORDER BY order_date
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS cumulative_total,
    ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY amount DESC) AS rank
FROM orders;
```

#### 20.2 CTEs and Recursion

```sql
WITH RECURSIVE org_tree AS (
    SELECT employee_id, manager_id, name, 0 AS depth
    FROM employees WHERE manager_id IS NULL
    UNION ALL
    SELECT e.employee_id, e.manager_id, e.name, ot.depth + 1
    FROM employees e
    JOIN org_tree ot ON e.manager_id = ot.employee_id
)
SELECT * FROM org_tree ORDER BY depth, name;
```

#### 20.3 Lateral Joins

```sql
SELECT u.*, latest.*
FROM users u
CROSS JOIN LATERAL (
    SELECT * FROM orders WHERE user_id = u.id
    ORDER BY order_date DESC LIMIT 1
) latest;
```

#### 20.4 Approximate Functions

```sql
SELECT 
    approx_distinct(user_id) AS unique_users,
    approx_percentile(response_time, 0.95) AS p95,
    approx_percentile(response_time, ARRAY[0.5, 0.9, 0.95, 0.99]) AS percentiles
FROM events;
```

### 21. Query Federation Patterns

#### 21.1 Lakehouse + Operational DB

```sql
-- Combine lakehouse facts with operational dimensions
SELECT 
    o.order_id,
    o.amount,
    u.email,
    u.region
FROM iceberg.gold.orders o
JOIN postgres.public.users u ON o.user_id = u.id
WHERE o.order_date >= DATE '2024-03-01'
  AND u.account_status = 'active';
```

#### 21.2 Cross-Warehouse Queries

```sql
-- Join Snowflake and BigQuery data
SELECT 
    sf.customer_id,
    sf.orders,
    bq.events
FROM snowflake.sales.customers sf
JOIN bigquery.analytics.event_counts bq ON sf.customer_id = bq.user_id;
```

#### 21.3 Kafka + Table

```sql
-- Query recent Kafka data joined to lakehouse dimensions
SELECT k.event_time, k.event_type, d.region
FROM kafka.events.raw k
JOIN iceberg.dim.users d ON k.user_id = d.id
WHERE k._timestamp > CURRENT_TIMESTAMP - INTERVAL '1' HOUR;
```

#### 21.4 Reverse ETL via Federated Writes

```sql
-- Insert into a Postgres table from Trino aggregation of lakehouse data
INSERT INTO postgres.marketing.campaign_targets
SELECT user_id, segment, score
FROM iceberg.ml.churn_scores
WHERE score > 0.7;
```

### 22. User-Defined Functions

#### 22.1 SQL UDFs

```sql
-- Create a SQL UDF
CREATE FUNCTION full_name(first VARCHAR, last VARCHAR)
RETURNS VARCHAR
RETURN first || ' ' || last;

-- Use it
SELECT full_name(first_name, last_name) FROM users;
```

#### 22.2 Scalar vs Aggregate UDFs

```
Scalar: one input row → one output value
Aggregate: many input rows → one output value
Window: input rows + window → output values
```

#### 22.3 Java / Plugin UDFs

```
For performance-critical UDFs, write in Java as a Trino plugin.
Compile to JAR; drop into plugin directory; restart cluster.
Not for the faint of heart; usually overkill unless truly needed.
```

---

## PART VI — SECURITY AND MULTI-TENANCY

### 23. Authentication and Authorization

#### 23.1 Authentication

```
Authentication methods:
   • Password (basic)
   • Kerberos (enterprise)
   • LDAP (enterprise)
   • JWT (modern)
   • OAuth 2.0 (modern)
   • mTLS (certificates)
```

#### 23.2 Authorization

```
Trino itself has basic authorization (SystemAccessControl).

For production: use external access control systems:
   • Apache Ranger (role-based policies)
   • Starburst's built-in RBAC
   • OPA (Open Policy Agent) via plugin
   • Custom plugin

Policies can be:
   • Catalog-level (who can access which catalogs)
   • Schema-level
   • Table-level
   • Column-level (restrict specific columns)
   • Row-level (filter rows based on user attributes)
```

### 24. Resource Groups

#### 24.1 What Resource Groups Do

```
Resource groups manage CONCURRENT QUERY LOAD.

Classify queries into groups based on user/source.
Each group has:
   • Maximum concurrent queries
   • Memory limits
   • CPU priority
   • Queue priority
```

#### 24.2 Example Config

```json
{
  "rootGroups": [
    {
      "name": "global",
      "softMemoryLimit": "80%",
      "hardConcurrencyLimit": 100,
      "maxQueued": 1000,
      "subGroups": [
        {
          "name": "etl",
          "softMemoryLimit": "40%",
          "hardConcurrencyLimit": 20
        },
        {
          "name": "ad_hoc",
          "softMemoryLimit": "30%",
          "hardConcurrencyLimit": 50
        },
        {
          "name": "bi",
          "softMemoryLimit": "20%",
          "hardConcurrencyLimit": 30
        }
      ]
    }
  ],
  "selectors": [
    {"user": "etl_user", "group": "global.etl"},
    {"source": "tableau", "group": "global.bi"},
    {"group": "global.ad_hoc"}
  ]
}
```

### 25. Row and Column Filters

#### 25.1 Column Masking

```
Mask sensitive columns based on user role:
   • Analysts see hashed emails
   • Admins see plaintext

Implemented via plugins (Starburst, custom, Ranger).
```

#### 25.2 Row Filtering

```
Return only rows matching user's permissions:
   • User from EU → only EU rows
   • Tenant X → only tenant X's rows

Essential for multi-tenant deployments.
```

---

## PART VII — DEPLOYMENT AND OPS

### 26. Deployment Patterns

#### 26.1 Dedicated Cluster

```
Set up:
   • One coordinator (dedicated instance; failover optional)
   • N workers (commodity instances)
   • Configure catalogs
   • Deploy to K8s or VMs

Typical sizes:
   • Small team: 1 coordinator + 5-10 workers
   • Mid-size: 1 coordinator + 20-50 workers
   • Large: 1 coordinator + 100-1000 workers (sharded)
```

#### 26.2 Kubernetes

```
Trino has official Kubernetes operator and Helm charts.
Deploy coordinator + worker StatefulSets.
Scale workers by changing replicas.

Advantages:
   • Elastic scaling
   • Rolling upgrades
   • Standard K8s observability
```

#### 26.3 Managed Options

```
STARBURST GALAXY:
   Fully managed hosted Trino.
   Pay per compute + storage.
   
AWS ATHENA:
   Based on Presto / Trino (started as Presto, now Trino-aligned).
   Serverless; pay per TB scanned.
   
GOOGLE DATAPROC:
   Managed Hadoop + Trino cluster.
   
IBM / ALIBABA / OTHER CLOUD VENDORS:
   Hosted Trino / Presto distributions.
```

### 27. Starburst: Enterprise Trino

#### 27.1 What Starburst Adds

```
STARBURST ENTERPRISE / GALAXY = commercial Trino distribution with:
   • More connectors (SAP, Teradata, etc.)
   • Advanced access control (built-in RBAC)
   • Caching layer (query result + data caching)
   • Query optimization improvements
   • Enterprise support
   • Managed cloud version (Galaxy)

Open-source Trino covers 90% of use cases.
Starburst adds enterprise polish and operational features.
```

#### 27.2 When to Pay for Starburst

```
PICK OSS TRINO WHEN:
   ✓ You have ops team to maintain Trino
   ✓ Standard connectors cover your sources
   ✓ Advanced governance not required

PICK STARBURST WHEN:
   ✓ Need enterprise support (SLAs, incident response)
   ✓ Have specialized sources (SAP, Teradata, etc.)
   ✓ Need advanced caching for cost reduction
   ✓ Prefer managed over self-hosted (Galaxy)
```

### 28. The Trino + Iceberg Lakehouse Stack

#### 28.1 The Reference Architecture

```
       ┌─────────────────────────────────────┐
       │   SOURCES (OLTP, SaaS, Events)      │
       └──────────────┬──────────────────────┘
                      │
          ┌───────────┼───────────┐
          │           │           │
          ▼           ▼           ▼
      FLINK       SPARK       FIVETRAN
      (streaming) (batch)     (SaaS CDC)
          │           │           │
          └───────────┼───────────┘
                      │
                      ▼
       ┌─────────────────────────────────────┐
       │        ICEBERG LAKEHOUSE            │
       │        (S3 + Polaris catalog)       │
       └──────────────┬──────────────────────┘
                      │
          ┌───────────┼───────────┐
          │           │           │
          ▼           ▼           ▼
       SPARK       TRINO      DUCKDB
       (batch      (interact- (local
       ETL +ML)    ive SQL + ad-hoc)
                   federation)
          │           │           │
          └───────────┼───────────┘
                      │
                      ▼
             BI TOOLS + APPS
```

#### 28.2 Role of Each Engine

```
FLINK: streaming ingest into Iceberg (bronze layer)
SPARK: batch ETL producing silver/gold (via dbt-spark)
TRINO: interactive SQL for analysts; federation across catalogs
DUCKDB: local ad-hoc for individual analysts
```

#### 28.3 Why This Stack

```
• Open storage (Iceberg) — no vendor lock-in
• Specialized engines per workload
• Each team picks their comfortable engine
• Catalog (Polaris) coordinates access + governance
• Cost-efficient: no warehouse credit multiplier
```

#### 28.4 Comparison to Databricks

```
This stack vs Databricks:
   • More components to manage
   • More vendor-neutral
   • Potentially lower cost (self-hosted compute)
   • Need stronger ops team

Databricks:
   • Single platform (SQL, Spark, ML, DLT)
   • Easier operations
   • Vendor-locked (Delta primary; UniForm bridges)
   • Higher cost but less engineering overhead
```

> **🧠 Principal Mindset:** The "Trino + Iceberg + Polaris" stack is the modern open lakehouse architecture. It's what Netflix, Shopify, Lyft, and others run. Compared to Databricks, it trades operational simplicity for openness and cost efficiency. Trino's role in this stack is irreplaceable — it's the interactive SQL layer for analysts and the federation layer for multi-source queries. If you're designing a lakehouse that avoids vendor lock-in, Trino belongs on the diagram.

---

## PART VIII — INTERVIEW PRACTICE

### 29. The Staff-Level Trino Interview

#### 29.1 Common Questions

```
1. "What is Trino and when do you pick it over Spark SQL?"
2. "Explain Trino's architecture — coordinator, workers, stages, tasks."
3. "How does Trino federate across data sources? What's a connector?"
4. "Explain dynamic filtering. Why is it a big performance win?"
5. "How does Trino's CBO decide between broadcast and partitioned joins?"
6. "Trino vs Snowflake / BigQuery — when do you pick which?"
7. "A Trino query is slow. Walk me through diagnosis."
8. "Design a lakehouse using Trino as the SQL engine."
9. "What are the ops challenges of running Trino at scale?"
10. "How does Trino integrate with Iceberg / Delta / Hudi?"
```

#### 29.2 Worked Example

```
Q: Design a lakehouse platform for a 500-person company with:
   - Mixed workloads: batch ETL, interactive BI, ad-hoc analysis
   - 100 TB current, growing 2x/year
   - Must query lakehouse + operational Postgres data
   - Budget-sensitive; want vendor-neutral

STAFF ANSWER:

STACK:
   • Storage: Iceberg on S3 (open format)
   • Catalog: Apache Polaris (open REST catalog)
   • Streaming: Flink + Iceberg sink (CDC into bronze)
   • Batch: Spark + dbt-spark (bronze → silver → gold)
   • Interactive SQL: Trino (ad-hoc + BI)
   • Local ad-hoc: DuckDB (for individual analysts)

TRINO SPECIFICALLY:
   • Deployment: Kubernetes with Trino K8s operator
   • Size: 1 coordinator (c5.4xlarge) + 20 workers (c5.4xlarge)
     (16 vCPU, 32 GB RAM each; scale as data grows)
   • Catalogs configured:
      - iceberg (Polaris) → lakehouse
      - postgres (JDBC) → operational
      - mysql (JDBC) → legacy ops
      - delta (if any Delta tables exist from past projects)

CROSS-SOURCE FEDERATION:
   Analysts write queries like:
      SELECT * FROM iceberg.gold.orders o
      JOIN postgres.ops.customers c ON o.cust_id = c.id
      WHERE c.tier = 'enterprise' AND o.date >= ...;
   
   Trino handles the federation; no ETL needed.

PERFORMANCE:
   • Dynamic filtering enabled (default)
   • Iceberg stats collected (via Trino ANALYZE or via writers)
   • Resource groups: etl / ad_hoc / bi with quotas

GOVERNANCE:
   • SSO via LDAP + JWT
   • Row/column filters via Ranger plugin
   • Audit log forwarded to Datadog
   • Costs tracked per resource group

COST ESTIMATE:
   • Trino cluster: ~$2K/month (K8s on EC2)
   • S3 storage: ~$2.5K/month (100 TB)
   • Compute for Flink + Spark: ~$3K/month
   • Polaris: ~$300/month (small Postgres backing)
   • Monitoring + ops: ~$1K/month
   • Total: ~$9K/month

   Compare: equivalent Snowflake deployment might run $30-50K/month
   for same workload. Trade-off: ops effort.

OFF-RAMPS:
   • Open format: data stays in S3 as Parquet
   • Can swap Trino for another SQL engine
   • Can swap Polaris for another catalog
   • Not locked into any component
```

#### 29.3 Senior vs Staff

| Senior Answer | Staff Answer |
|---------------|--------------|
| "Trino is a distributed SQL engine." | "Trino is an MPP distributed SQL engine designed for interactive analytical queries, with first-class support for federated access across data sources via pluggable connectors. Its architectural distinction from Spark SQL: memory-first (vs disk-spill-friendly) execution for low-latency; separate cluster (vs reusing Spark cluster); and the connector model making multi-source queries first-class rather than requiring explicit source copies." |
| "Use Trino for federation." | "Trino federation is powerful but has limits. Predicate pushdown works well for RDBMS joins to lakehouse facts, but complex joins across many disparate sources can be slow (data must transit through Trino's memory). The right pattern: Trino federates for ad-hoc exploration and lightweight cross-source joins; for heavy regular cross-source queries, materialize to Iceberg via ETL. Don't federate when you'd query the same combination 100x/day — materialize." |
| "Trino is cheaper than Snowflake." | "Compute cost is often 3-5x lower for self-hosted Trino vs Snowflake on the same workload. The gap narrows considering ops effort: Snowflake eliminates cluster management entirely; Trino needs a team. The honest comparison: Trino + ops team < Snowflake for large deployments; Snowflake < Trino + ops team for small deployments. Starburst Galaxy (managed Trino) sits between. Pick based on team capacity + workload scale." |

### 30. War Stories

> **🔥 Production Scenario — The Federation Trap.** A team queried a Postgres dim table (10M rows) joined to an Iceberg fact table (10 TB) via Trino. Without dynamic filtering, Trino pulled all 10M Postgres rows for every query. Fix: ensured Postgres connector had primary key info; dynamic filtering kicked in and transferred only matching rows. Query latency dropped from 2 minutes to 15 seconds. Lesson: federation performance depends heavily on predicate pushdown and dynamic filtering working correctly.

> **🔥 Production Scenario — The Memory OOM.** A Trino query joined two large fact tables (1 TB each) with no good filter. Workers OOM'd mid-query. Fix: (1) enabled fault-tolerant execution for this specific resource group (spills to S3); (2) rewrote the query to filter earlier; (3) increased worker heap. Query completed but took 20 minutes vs expected 5. Lesson: Trino's memory-first design has limits for pure "big data ETL" jobs; use Spark for those or enable fault-tolerant mode.

> **🔥 Production Scenario — The Hive Metastore Bottleneck.** A team's Trino cluster relied on a single Hive Metastore for 500K Iceberg tables. Metastore became the bottleneck during query planning (slow metadata lookups). Fix: migrated to Apache Polaris REST catalog with multi-AZ Postgres backing. Catalog ops 10x faster; cluster capacity freed for actual query work. Lesson: catalog performance is often the hidden bottleneck in Trino deployments; invest in it.

> **🔥 Production Scenario — The Connector Mismatch.** A team configured Trino to query a Snowflake warehouse via the Snowflake connector for cross-platform ad-hoc analysis. They expected pushdown; it worked for simple predicates but complex Trino-only functions didn't translate. Result: full table pulls at times. Fix: taught analysts which SQL patterns were "safe" (predicate pushdown works) vs "unsafe" (full scan). Lesson: federation isn't magic; understand each connector's pushdown capabilities.

> **🔥 Production Scenario — The Resource Group Fight.** A Trino cluster had one resource group with 100 concurrent query limit. ETL ran at 2am; dashboards ran all day. During overlap (6am), dashboards queued behind ETL. Fix: split into etl/ad_hoc/bi groups with separate memory + concurrency budgets. Dashboard latency dropped from 90s p95 to 5s. Lesson: resource group tuning matters hugely for multi-tenant Trino.

### 31. Cheat Sheet & Mental Models

#### 31.1 The One-Minute Pitch

```
Trino = distributed SQL engine for interactive queries over diverse sources.
   • Federates 50+ data sources via connectors
   • MPP execution with memory-first pipelines
   • ANSI SQL (high compliance)
   • Dynamic filtering + CBO for performance
   • Open source; Starburst for commercial
   • Powers Netflix, LinkedIn, Shopify, etc.
   • The interactive SQL layer for open lakehouses (with Iceberg)

Pick for: interactive SQL on lakehouse, federation, open architecture.
Avoid when: pure batch ETL (Spark), small single-source (DuckDB), fully managed shop (Snowflake/BQ).
```

#### 31.2 The Four Lenses — Compressed

```
CONCEPT
   Distributed SQL engine. No storage of its own. Federates via connectors.

MECHANICS
   Coordinator + workers. Query → stages → tasks → operator pipelines.
   Vectorized page-based execution. Hash + broadcast joins.
   Dynamic filtering reduces scan sizes dramatically.

TRADE-OFFS
   Pros: fast interactive SQL, federation, open, scales to PB.
   Cons: memory-first (not best for huge ETL), ops burden,
         federation performance depends on pushdown.

INTERVIEW
   Know: coordinator/worker, stages, dynamic filtering, connector model.
   Defend: Trino vs Spark SQL vs warehouse vs DuckDB.
   Design: Iceberg + Trino + Polaris lakehouse stack.
```

#### 31.3 Essential Commands

```sql
-- Show catalogs / schemas / tables
SHOW CATALOGS;
SHOW SCHEMAS FROM iceberg;
SHOW TABLES FROM iceberg.sales;

-- Query across sources
SELECT * FROM iceberg.sales.orders o
JOIN postgres.public.users u ON o.user_id = u.id;

-- EXPLAIN
EXPLAIN SELECT ...;
EXPLAIN ANALYZE SELECT ...;

-- Iceberg table operations (via CALL)
CALL iceberg.system.optimize('sales', 'orders');
CALL iceberg.system.expire_snapshots('sales', 'orders', TIMESTAMP '...');

-- Session settings
SET SESSION query_max_memory = '20GB';
SET SESSION broadcast_join_threshold = '500MB';
SHOW SESSION;

-- Analyze
ANALYZE iceberg.sales.orders;
```

#### 31.4 Mental Model

```
Think of Trino as:
   • The SQL layer for anything (not just one storage)
   • The "bring compute to data" engine
   • The interactive sibling of Spark (which is batch)
   • The open-source alternative to Snowflake for lakehouse

When to reach for it:
   • Lakehouse + interactive queries
   • Multi-source federation needed
   • Cost-sensitive at scale
   • Open-architecture mandate

When to reach past it:
   • Pure batch ETL (Spark)
   • Fully managed desire (Snowflake / BQ / Starburst Galaxy)
   • Single-node, small data (DuckDB)
   • Streaming (Flink)
```

> **🧠 Principal Mindset:** Trino is the most under-covered important component in modern data platforms. Snowflake and Databricks dominate the marketing narrative; Trino quietly powers production at the biggest tech companies. If your organization has or will have a lakehouse, know Trino. If your team federates across sources, know Trino. The staff-level signal in interviews isn't just naming it — it's confidently positioning it alongside Spark (for batch) and Flink (for streaming) in a unified architecture, understanding its specific strengths and where it isn't the right tool. Many engineers reach staff level without ever using Trino directly; the ones who understand it have an architectural tool the others don't.

---

[← Back to main](./README.md)
