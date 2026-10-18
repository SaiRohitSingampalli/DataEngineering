# Topic 7 — Cloud Data Warehouses

> Section 7 of 10 · 60 pages in source PDF · pages 392–451

[← Back to main](./README.md)

---

Cloud Warehouses

Cloud Data Warehouses Architect-Level Reference Snowflake · BigQuery · Redshift · Databricks SQL · Micro-Partitions · Slots · Sort/Dist Keys · Cost Modeling How the warehouse layer actually works — and how to pay for it sanely. Principal/Staff Engineer Level Reference

Cloud Warehouses

How to use this document Cloud warehouses look interchangeable from the SQL surface. They are not. Snowflake, BigQuery, Redshift, and Databricks SQL have profoundly different internal architectures, cost models, and operational characteristics. At staff level, you're expected to pick the right warehouse for the workload, tune it to its strengths, and architect a migration when one is needed — all while controlling cost, which at scale is the dominant concern. ■ PRINCIPAL MINDSET: Three staff-level signals: (1) you can explain HOW each warehouse achieves its performance — micro-partitions, slot reservations, sort+dist keys, Photon — not just THAT it does; (2) you understand cost models well enough to predict and control them at the workload level; (3) you know when the warehouse is the wrong tool — when a lakehouse, OLTP DB, or specialized store fits better.

Cloud Warehouses

Table of Contents

## PART I — FOUNDATIONS

#### 1. What a Cloud Warehouse Is

#### 2. Storage / Compute Separation

#### 3. Columnar Storage Universal Concepts

#### 4. The Four Major Warehouses: Overview

## PART II — SNOWFLAKE

#### 5. Snowflake Architecture: 3-Layer Design

#### 6. Micro-Partitions & Pruning

#### 7. Virtual Warehouses, Caching & Concurrency

#### 8. Snowflake Cost Model

## PART III — BIGQUERY

#### 9. BigQuery Architecture: Dremel & Serverless

#### 10. Slots, Reservations & Pricing Models

#### 11. Partitioning, Clustering & BI Engine

## PART IV — REDSHIFT

#### 12. Redshift Architecture: Leader + Compute

#### 13. Sort Keys, Dist Styles & Vacuum

#### 14. Redshift Serverless & RA3 Nodes

## PART V — DATABRICKS SQL

#### 15. Databricks SQL & Photon Engine

## PART VI — COMPARISON & SELECTION

#### 17. Feature & Performance Comparison

#### 18. Cost Comparison: Dollar-by-Dollar

#### 19. When to Pick Which Warehouse

#### 20. Warehouse vs Lakehouse Decision

## PART VII — OPERATIONS

#### 21. Query Tuning Across Engines

#### 22. Workload Management & Multi-Tenancy

#### 23. Cost Control Playbook

#### 24. Migration Patterns

#### 25. Security, Compliance & Governance

## PART VIII — INTERVIEW PRACTICE

#### 26. The Staff-Level Warehouse Interview

#### 27. War Stories

Cloud Warehouses

#### 28. Cheat Sheet & Mental Models

Cloud Warehouses

## PART I — FOUNDATIONS

#### 1. What a Cloud Warehouse Is

1.1 The One-Sentence Definition A cloud data warehouse is a fully-managed, columnar, SQL-native database with separated storage and compute, designed for analytical workloads (OLAP) at scales where traditional databases break — terabytes to petabytes, thousands of concurrent users, complex aggregations.

```
1.2 What Cloud Warehouses Replaced
1990s-2000s ON-PREM MPP WAREHOUSES:
  Teradata, Netezza, Vertica, Greenplum
   Tightly coupled storage + compute
   Shared-nothing architecture across a fixed cluster
   Capacity planning years in advance
   Million-dollar appliances, multi-month installs
   Scaling = buy more hardware
2012-2015 EARLY CLOUD WAREHOUSES:
  Redshift (AWS, 2013) — based on ParAccel; first to make MPP cloud-native
  BigQuery (Google, 2010) — Dremel-based, serverless from day one
```

```
2014-2017 SECOND GENERATION:
  Snowflake (2014) — separated storage from compute architecturally
  Redshift Spectrum (2017) — external table queries on S3
2019+ THIRD GENERATION:
  Databricks SQL (2020+) — lakehouse-native SQL
  BigQuery Omni — multi-cloud
  Redshift RA3 / Serverless — caught up architecturally
THE ARC: closer to the cloud-native ideal each generation.
```

```
1.3 The Defining Properties
ALL CLOUD WAREHOUSES SHARE:
  1. COLUMNAR STORAGE
     Data stored column-wise, not row-wise
     → only read columns referenced in SELECT
     → high compression ratios (similar values clustered)
  2. MPP (Massively Parallel Processing)
     Queries split across many nodes/workers
     → linear scaling for typical analytical queries
  3. MANAGED INFRASTRUCTURE
     No DBA managing disks, replication, backups
     → cloud vendor handles HA, durability, upgrades
  4. ELASTIC COMPUTE
     Spin compute up and down on demand
     → pay for what you use, not peak capacity
  5. SQL-NATIVE
     Standard SQL with vendor extensions
     → familiar interface, broad tool support
  6. ACID TRANSACTIONS (varying levels)
     Snowflake: full SI on a per-table basis
     BigQuery: per-statement transactional semantics + multi-statement txns
```

Cloud Warehouses

```
     Redshift: standard SQL transactions
     Databricks SQL: ACID via Delta Lake
  7. STRUCTURED + SEMI-STRUCTURED
     Native JSON / VARIANT / STRUCT support
     → not just relational rows; nested data first-class
```

```
1.4 What Warehouses Are NOT
✗ OLTP databases
   ✗ Sub-millisecond reads, point updates at high concurrency
   → use Postgres / MySQL / DynamoDB
✗ Real-time streaming engines
   ✗ Sub-second event-driven processing
   → use Flink / Kafka Streams
✗ Document / key-value stores
   ✗ Application-side persistence
   → use MongoDB / Redis / DynamoDB
✗ Search engines
   ✗ Full-text relevance ranking
   → use Elasticsearch / OpenSearch
✗ ML training platforms
   ✗ Tensor computation, GPU workloads
   → use Spark / Databricks ML / SageMaker / Vertex AI
OLAP is the lane. Stay in the lane unless you have a very specific reason.
■ INTERVIEW ANGLE: Q: Why do we need cloud warehouses if we have OLTP databases?
Staff: OLTP databases are optimized for transactional workloads — many concurrent users doing point
reads/writes with sub-millisecond latency. Their row-oriented storage and B-tree indexes work well for that.
Analytics is different: few users, complex aggregations over millions of rows, no need for low-latency point
updates. Running analytics on an OLTP DB hurts both: the analytical query is slow because of row-orientation,
and the transactional users see latency spikes from lock contention and resource hogging. Cloud warehouses
solve this with columnar storage, MPP execution, separated compute (so analytical queries don't impact
transactional systems), and pricing models tuned for occasional heavy computation rather than constant
low-latency reads.
```

Cloud Warehouses

#### 2. Storage / Compute Separation

```
2.1 The Pre-Separation World
TRADITIONAL MPP WAREHOUSES (Teradata, Netezza, original Redshift):
  ■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■
  ■  COMPUTE NODE 1   ■   COMPUTE NODE 2  ■ ...
  ■  ■■■■■■■■■■■■■■   ■   ■■■■■■■■■■■■■■  ■
  ■  ■ CPU + RAM  ■   ■   ■ CPU + RAM  ■  ■
  ■  ■            ■   ■   ■            ■  ■
  ■  ■ Local disk ■   ■   ■ Local disk ■  ■
  ■  ■ (data slice)■  ■   ■ (data slice)■  ■
  ■  ■■■■■■■■■■■■■■   ■   ■■■■■■■■■■■■■■  ■
  ■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■
  PROBLEMS:
  ✗ Scale storage → must add compute (and vice versa)
  ✗ Compute idle 70% of the day → wasted money
  ✗ Multiple workloads contend for same nodes
  ✗ Adding capacity = redistributing data (slow, risky)
  ✗ Backup = duplicating across nodes
```

```
2.2 The Modern Separated Architecture
              ■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■
              ■      COMPUTE LAYER (stateless)     ■
              ■  ■■■■■■■■  ■■■■■■■■  ■■■■■■■■    ■
              ■  ■ Node ■  ■ Node ■  ■ Node ■ ...■
              ■  ■      ■  ■      ■  ■      ■    ■
              ■  ■■■■■■■■  ■■■■■■■■  ■■■■■■■■    ■
              ■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■
                     ■         ■         ■
                     ▼         ▼         ▼
              ■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■
              ■     STORAGE LAYER (durable)        ■
              ■  Object storage: S3 / GCS / Azure ■
              ■  Columnar files (proprietary or    ■
              ■  Parquet/Iceberg/Delta)            ■
              ■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■
  PROPERTIES:
  ✓ Scale compute independently of storage
  ✓ Multiple compute clusters can read same storage
  ✓ Stateless compute → fast scale-up, easy upgrades
  ✓ Storage is durable independent of any compute
```

2.3 Why It Took Until 2014 ENABLING TECHNOLOGIES:  Cheap, durable object storage (S3 GA: 2006; S3 strong consistency: 2020)  Fast network within data centers (10/40/100 Gbps standard)  Columnar formats with good compression  Page cache at compute layer mitigates cold-storage latency  Cloud-native vendors willing to bet on it (Snowflake's founding bet) PRE-SEPARATION WAREHOUSES had to rebuild from scratch. Redshift took until RA3 (2019) to ship a true separated architecture. BigQuery had this from day one (Dremel was always serverless). Snowflake led the design school for it.

  - 2.4 The Implications
- 1. PER-WORKLOAD COMPUTE
- Different teams / workloads get different compute clusters.
- No contention; clean SLAs.
- 2. INSTANT SCALING
- Need more power? Spin up a new compute cluster.

Cloud Warehouses

```
   No data reshuffling.
3. PAY-PER-USE
   Compute billed by second/hour of use.
   Storage billed by GB/month.
   Match cost to value.
4. ZERO-COPY CLONES
   Clone a multi-TB table instantly → same storage, new metadata pointer
   Powers dev/test environments without copying data
5. TIME TRAVEL / RETENTION
   Old data versions kept until expiry (Snowflake: 90 days fail-safe)
   Cheap because storage is cheap
6. CHALLENGES THAT REMAIN
   ✗ Cold reads slower than co-located storage
   ✗ Network becomes the bottleneck for shuffle-heavy queries
   ✗ Caching strategy critical (warm vs cold pricing)
■ PRINCIPAL MINDSET: Storage/compute separation is the most important architectural shift in data
warehousing since the column store itself. Once you internalize the model — stateless compute + durable cheap
storage — every design choice in modern warehouses falls into place. Per-workload virtual warehouses,
zero-copy clones, time travel, multi-cluster auto-scaling, even the lakehouse — all are consequences of this
decoupling.
```

Cloud Warehouses

#### 3. Columnar Storage Universal Concepts

```
3.1 Row vs Column Storage
ROW STORAGE (OLTP):
  row1: [id=1, name='Alice', age=30, city='SF', email='a@x.com']
  row2: [id=2, name='Bob',   age=25, city='NY', email='b@x.com']
  row3: [id=3, name='Carol', age=35, city='LA', email='c@x.com']
  GOOD: read/write whole rows (transactional)
  BAD: SELECT AVG(age) reads ALL columns of all rows
COLUMN STORAGE (OLAP):
  id    column: [1, 2, 3, ...]
  name  column: ['Alice', 'Bob', 'Carol', ...]
  age   column: [30, 25, 35, ...]
  city  column: ['SF', 'NY', 'LA', ...]
  email column: ['a@x.com', 'b@x.com', 'c@x.com', ...]
  GOOD: SELECT AVG(age) reads ONLY the age column
        Similar values → high compression
        Vectorized CPU instructions
  BAD: SELECT * scattered reads
```

```
3.2 Compression: The Free Win
COLUMNAR ENABLES BETTER COMPRESSION because adjacent values are similar.
ENCODING TYPES:
  RUN-LENGTH       — [A,A,A,A,B,B,B] → [(A,4),(B,3)]
  DICTIONARY       — values stored once, references everywhere
  DELTA            — store differences between consecutive (good for timestamps)
  BIT-PACKING      — minimum bits for the value range
  PARQUET'S RLE_DICTIONARY combines RLE + dictionary
REAL COMPRESSION RATIOS:
  Numeric data:     3-10x
  Categorical:      10-50x (dictionary + RLE)
  Timestamps:       5-20x (delta + bit-pack)
  Free text:        2-5x (zstd/snappy general purpose)
WAREHOUSE STORAGE COSTS:
  Snowflake: ~$23/TB/month (compressed, "as-stored")
  BigQuery:  ~$20/TB/month (logical bytes for active; physical for billing in newer pricing)
  Redshift:  ~$24/TB/month (RA3 managed storage)
  S3:        ~$23/TB/month (standard)
KEY INSIGHT: warehouse storage cost ≈ S3 storage cost.
The differentiator is compute / pricing model, not storage cost itself.
```

```
3.3 The Vectorized Execution Model
CLASSICAL ROW-AT-A-TIME PROCESSOR:
  for each row in table:
      evaluate predicates
      apply operations
      emit result
```

```
  PROBLEM: function-call overhead per row. Branch prediction loses.
  CPU caches under-utilized.
VECTORIZED (BATCHED) PROCESSING:
  for each batch of N rows (e.g., 1024):
      load N column values into CPU registers (SIMD)
      apply operation across all N (single instruction, multiple data)
      emit batch
  BENEFIT: 10-100x faster CPU utilization.
  Modern columnar engines (Photon, BigQuery, Snowflake) all vectorize.
```

Cloud Warehouses

```
3.4 File-Level Statistics
COLUMNAR FORMATS STORE METADATA per file / row group:
   Min / max per column
   Null count per column
   Distinct count (approximate)
   Bloom filters (optional)
WHEN A QUERY HAS WHERE x = 42:
  Engine reads file metadata FIRST.
  Files where min > 42 or max < 42 → SKIP entirely.
  Files that might contain 42 → open and scan.
THIS IS HOW WAREHOUSES SCAN PB-CLASS TABLES IN SECONDS.
  Without selectivity: full scan.
  With selectivity + statistics: 99%+ files pruned.
THE OPTIMIZATION FLOW:
  partition pruning → file pruning → row-group pruning → page pruning
  Each layer drops orders of magnitude of data.
```

```sql
3.5 Clustering / Sort Within Files
IF DATA WITHIN A FILE IS SORTED ON A COLUMN:
   Min/max stats are TIGHT — pruning is highly effective
   Range queries (WHERE col BETWEEN x AND y) skip many files
WAREHOUSES NAME THIS DIFFERENTLY:
  Snowflake:  clustering keys (automatic via natural insertion order + reclustering)
  BigQuery:   clustering (up to 4 columns)
  Redshift:   sort keys (compound, interleaved, or AUTO)
  Databricks: Z-ORDER, Liquid Clustering
THE TRADE-OFF:
  Sorting = file rewrite cost.
  Helps queries that filter on the sort column.
  Doesn't help (and may hurt) queries that don't.
DESIGN HEURISTIC:
  Cluster/sort on the columns 80%+ of selective queries filter on.
  Usually: event_timestamp, user_id, or another high-cardinality predicate column.
■ INTERVIEW ANGLE: Q: A SELECT COUNT(*) on a 100 TB Snowflake table returns in 50ms. How?
Staff: Three things. (1) Columnar storage with file-level metadata: each micro-partition stores its row count. (2)
Snowflake's metadata layer aggregates this — COUNT(*) is answered from metadata alone, no data scan. (3)
The metadata service is highly available and the count is essentially precomputed. Other examples: SELECT
MIN/MAX(col) on columns with file-level stats can also return without touching data. It's not magic; it's that the
metadata layer carries enough information for many common queries. Most warehouses do this — BigQuery,
Redshift, Databricks SQL all answer trivial aggregates from metadata.
```

Cloud Warehouses

#### 4. The Four Major Warehouses: Overview

4.1 The Big Four (2024) Warehouse Vendor Model Year Snowflake Snowflake Multi-cloud (AWS/GCP/Azure) 2014 BigQuery Google GCP-only 2010 Redshift AWS AWS-only 2013 Databricks SQL Databricks Lakehouse-native 2020

```
4.2 Their Distinctive DNA
SNOWFLAKE
  Built FROM SCRATCH for cloud (no on-prem heritage)
  Cleanest separation of storage/compute (the design school)
  Multi-cloud from inception
  Best UX for analysts/SQL users
  Cost: per-second compute, fixed storage
BIGQUERY
  Based on Google's Dremel (internal since 2006)
  Serverless from day one — no clusters to manage
  Slot-based pricing (or per-query on-demand)
  Best for ad-hoc, sporadic workloads
  Tight integration with GCP ecosystem
  Cost: pay per byte scanned OR slot reservations
REDSHIFT
  Started 2013, original architecture from ParAccel/Vertica heritage
  Recent (RA3, Serverless) caught up with separation of storage/compute
  Tight AWS integration; same VPC as your apps
  Best for AWS-native shops
  Cost: cluster-hour for provisioned; RPU-hour for Serverless
DATABRICKS SQL
  Came from the lakehouse direction (Spark + Delta + Photon)
  SQL on Delta Lake (or Iceberg via UniForm)
  Lakehouse-native: same storage layer as ETL workloads
  Best for shops already on Databricks
  Cost: DBU per second (sized by warehouse cluster)
```

```
4.3 The Categorization
ARCHITECTURAL FAMILY:
  ELASTIC, PER-QUERY OR PER-CLUSTER COMPUTE:
    Snowflake, BigQuery, Redshift Serverless, Databricks SQL Serverless
  TRADITIONAL MPP (improved):
    Redshift provisioned (RA3), some Snowflake patterns
  SERVERLESS ALL-THE-WAY:
    BigQuery (on-demand), Snowflake (smallest XS warehouse + autosuspend)
  LAKEHOUSE-NATIVE:
    Databricks SQL (Delta-first), BigQuery managed Iceberg (preview),
    Snowflake managed Iceberg (Polaris)
```

#### 4.4 What All Four Got Right

Cloud Warehouses

```
✓ Columnar storage with compression
✓ MPP query execution
✓ Vectorized processing
✓ File-level / partition-level pruning
✓ Standard SQL (ANSI + extensions)
✓ Managed: no DBA managing disks/replication
✓ Eventually: separated storage + compute
✓ Native semi-structured (JSON / VARIANT / STRUCT)
✓ ACID for writes
✓ Time travel (varying retention)
→ THE BAR FOR A MODERN WAREHOUSE.
  All four cleared it; smaller players have caught up too.
```

4.5 The Mental Map for Picking IF AWS-ONLY + analyst-heavy: Snowflake or Redshift IF GCP-only: BigQuery IF Databricks-centric / lakehouse: Databricks SQL IF multi-cloud: Snowflake IF maximum cost predictability: BigQuery slot reservations OR Redshift RU IF spiky workloads: BigQuery on-demand OR Snowflake autosuspend IF mostly streaming + lakehouse storage: Databricks SQL OR Snowflake managed Iceberg IF lowest cost for steady BI workload: BigQuery flat-rate OR Redshift provisioned IF SQL+Python+ML in same platform: Databricks SQL ■ PRINCIPAL MINDSET: The 'which warehouse' question is rarely about features anymore — they've converged at the surface. It's about: cloud lock-in, cost model fit, ecosystem alignment, and team familiarity. The technical question is secondary. At staff level you're expected to walk a CTO through the decision matrix and defend each axis with concrete reasoning.

Cloud Warehouses

## PART II — SNOWFLAKE

#### 5. Snowflake Architecture: 3-Layer Design

5.1 The Three Layers ■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■ ■ CLOUD SERVICES LAYER ■ ■  Authentication, query parsing, optimization ■ ■  Metadata management (table definitions, statistics) ■ ■  Transaction management, security, infrastructure mgmt ■ ■  Stateless; horizontally scaled by Snowflake ■ ■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■ ■ ▼ ■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■ ■ COMPUTE LAYER (Virtual Warehouses) ■ ■ ■■■■■■■■■■■■■■ ■■■■■■■■■■■■■■ ■■■■■■■■■■■■■■ ■ ■ ■ VW: ETL ■ ■ VW: BI ■ ■ VW: ML ■ ■ ■ ■ size: L ■ ■ size: M ■ ■ size: XL ■ ■ ■ ■ 8 nodes ■ ■ 4 nodes ■ ■ 16 nodes ■ ■ ■ ■■■■■■■■■■■■■■ ■■■■■■■■■■■■■■ ■■■■■■■■■■■■■■ ■ ■ ■ ■ Each VW = independent compute cluster. ■ ■ Different VWs don't share CPU/RAM; share cache rarely. ■ ■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■ ■ ▼ ■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■ ■ STORAGE LAYER ■ ■  Columnar files (FDN format) in cloud blob storage ■ ■  Immutable; new versions on update ■ ■  Shared by all VWs in the account ■ ■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■

```
5.2 Cloud Services Layer
WHAT IT DOES:
   Authentication, RBAC, user/role/grant management
   Query parsing → logical plan → physical plan
   Result caching (query result cache)
   Metadata storage (table catalog, file metadata, stats)
   Transaction management (multi-statement transactions)
   Infrastructure orchestration
KEY POINT:
  Cloud Services is RUN BY SNOWFLAKE, not you.
  You don't see or scale it — it just works.
  It's BILLED only as a small overhead on compute (~10% typical).
METADATA STORE:
  Stored in a foundational service (Snowflake's secret sauce).
  Tracks every micro-partition: its location, min/max per column, etc.
  This is what enables fast pruning.
```

```
5.3 Compute Layer: Virtual Warehouses
A VIRTUAL WAREHOUSE (VW) is a compute cluster:
   Size: XS, S, M, L, XL, 2XL, 3XL, 4XL, 5XL, 6XL
   Each size doubles compute (1, 2, 4, 8, 16, 32, 64, 128, 256, 512 nodes)
   Hold local SSD cache for hot data
   Independent — different VWs share nothing except storage
LIFECYCLE:
```

Cloud Warehouses

```
  CREATE WAREHOUSE my_wh
    WITH WAREHOUSE_SIZE = 'MEDIUM'
    AUTO_SUSPEND = 60     -- suspend after 60 seconds idle
    AUTO_RESUME = TRUE    -- resume on next query
    MIN_CLUSTER_COUNT = 1
    MAX_CLUSTER_COUNT = 4 -- multi-cluster auto-scale for concurrency
  RUNNING:  paying per second
  SUSPENDED: not paying compute (storage costs continue)
  RESUMING: ~1 second to come back
NO QUERY ROUTING:
  Each session is bound to one VW.
  Pick the VW per use case (ETL, BI, ad-hoc).
  This is workload isolation.
```

```
5.4 Multi-Cluster Warehouses
MULTI-CLUSTER WAREHOUSE = automatically add clusters under concurrency load.
  WAREHOUSE my_wh
    MIN_CLUSTER_COUNT = 1
    MAX_CLUSTER_COUNT = 4
    SCALING_POLICY = STANDARD  (or ECONOMY)
  At 1 cluster: ~8 concurrent queries (depends on size)
  Queue builds → Snowflake spins up cluster 2
  Queue empties → cluster 2 spins down
  USED FOR: dashboard/BI workloads with bursty concurrency.
  NOT FOR: making single queries faster — that's larger WAREHOUSE_SIZE.
POLICIES:
  STANDARD: more aggressive scale-up (responsiveness)
  ECONOMY:  more conservative (cost-saving)
```

```sql
5.5 The Storage Layer
STORAGE FORMAT: FDN (proprietary, columnar, similar to Parquet conceptually)
   Immutable micro-partitions (50-500 MB compressed)
   Each partition has its own metadata in cloud services
DATA UPDATES:
   UPDATE / DELETE / MERGE → creates NEW partitions, old marked invalid
   Old partitions retained for Time Travel + Fail-safe
   Background reclustering optimizes file layout over time
PRICING:
   Compressed bytes × $23/TB/month (typical region)
   Same regardless of access frequency
■ INTERVIEW ANGLE: Q: A 1TB query is fast on an XL warehouse but slow on an S. Why?
Staff: XL has 16 nodes, S has 2. The query plan parallelizes across nodes — more nodes = more concurrent
scan + processing. For scan-heavy queries, the speedup is roughly linear with size up to the point where the
query can no longer be effectively parallelized (small data, low selectivity, or operations that don't parallelize like
global sort with small result). Choosing warehouse size is essentially choosing parallelism: S for small queries
and short interactive work, XL/2XL+ for big ETL jobs. Picking too small means slow; too big means waste. The
right size is the smallest that hits your latency target.
```

Cloud Warehouses

#### 6. Micro-Partitions & Pruning

```
6.1 What a Micro-Partition Is
MICRO-PARTITION = a chunk of data, the unit of storage in Snowflake.
  Size: 50-500 MB compressed (typically ~150 MB)
  Contains: a sequential range of inserted rows
  Stored: in cloud blob storage (S3/GCS/Azure Blob)
  Metadata: in Cloud Services layer
  Created automatically by Snowflake — you don't manage them.
EACH MICRO-PARTITION CONTAINS METADATA:
   Min / max of EACH column
   Distinct count (approximate)
   Null count
   File location
→ Snowflake's "indexes" are these per-column min/max stats per partition.
  No traditional B-tree indexes; pruning is the optimization.
```

6.2 How Pruning Works QUERY: SELECT * FROM orders WHERE order_date = '2024 15'; PLAN:

#### 1. Snowflake's metadata service has min/max of order_date per partition.

#### 2. Skip every partition where 2024

```
  3. Scan only partitions that potentially contain matching rows.
  Table: 100,000 micro-partitions, ~15 TB total
  Pruned: 99,997 partitions skipped
  Scanned: 3 partitions, ~450 MB
  Cost: tiny
EFFECT: full-table-scan SQL still works well IF the predicate is pruning-friendly.
```

```
6.3 Natural Clustering: Insertion Order
SNOWFLAKE DEFAULTS to NATURAL CLUSTERING based on insertion order.
If you INSERT rows ordered by event_time:
  → Recently-inserted rows are in recent micro-partitions
  → Recent partitions have similar event_time min/max
  → Queries filtering by event_time prune effectively
THIS IS USUALLY ENOUGH FOR DATE-BASED WORKLOADS.
  No explicit partitioning required.
  Time-series tables work great out of the box.
WHEN IT BREAKS DOWN:
   Bulk inserts in random order (CSV with no sort)
   Frequent updates that "shuffle" partitions
   Multi-key access patterns (one filter on date, another on user_id)
   Large historical backfills out of order
```

```
6.4 Explicit Clustering Keys
CREATE TABLE orders (...)
  CLUSTER BY (event_date, user_id);
EFFECT:
   Snowflake reorganizes partitions in the background
   Partitions become more selective on the cluster keys
   Queries filtering on those columns prune more partitions
AUTOMATIC RECLUSTERING:
   Triggered when clustering depth degrades
```

Cloud Warehouses

```
   Runs as background service (no manual scheduling)
   Charged on credits (compute used for reclustering)
COSTS:
   Reclustering is NOT free; consumes credits
   For static or rarely-queried tables, may not be worth it
   For high-update tables, can be substantial
WHEN TO ADD CLUSTER BY:
  ✓ Table > 1 TB with frequent selective queries on specific columns
  ✓ Queries currently scan too many partitions (CHECK with EXPLAIN)
  ✓ Natural insertion order doesn't match query patterns
WHEN NOT TO:
  ✗ Small tables (< 1 TB)
  ✗ Tables with insertion order already matching query patterns
  ✗ Static / archival tables
```

```
6.5 Search Optimization Service
FOR HIGHLY SELECTIVE POINT LOOKUPS on high-cardinality columns:
  WHERE user_id = '...'  (5 billion users, looking for one)
NATIVE PRUNING is poor — every partition might contain any user_id.
SEARCH OPTIMIZATION SERVICE (SOS):
  ALTER TABLE my_table ADD SEARCH OPTIMIZATION;
  ALTER TABLE my_table ADD SEARCH OPTIMIZATION ON EQUALITY(user_id, email);
WHAT IT DOES:
  Builds bloom-filter-like indexes per micro-partition for specified columns.
  Pruning improves drastically for those columns.
COST:
  Significant credit usage — re-maintained as data changes.
  Storage overhead.
  Worth it for specific use cases (lookup tables, security audit queries).
```

```sql
6.6 Checking Clustering Quality
SELECT SYSTEM$CLUSTERING_INFORMATION('orders', '(event_date)');
  Returns clustering depth:
    Lower = better (well-clustered)
    Higher = worse (data spread across many partitions)
  Average overlap per partition:
    Lower = better
    Higher = more partitions scanned per query
DECISION:
  Clustering depth > 50 → consider CLUSTER BY
  Average overlap > 5 → consider reclustering
EXPLAIN PLAN SHOWS:
  "Partitions total" vs "Partitions scanned"
  If scanned > 10% of total on selective query → poor clustering
■ PRODUCTION SCENARIO: A team had a fact_orders table that grew to 8 TB. Queries filtering on customer_id
were slow because the table was naturally clustered by event_date but most BI queries filter on customer. Adding
CLUSTER BY (customer_id, event_date) dropped p95 query latency from 45 seconds to 1.8 seconds.
The trade-off: ~$2K/month in reclustering credits. Worth it for 200 analysts saving 43 seconds each many times
per day. Always check clustering quality on tables your queries scan frequently.
```

Cloud Warehouses

#### 7. Virtual Warehouses, Caching & Concurrency

```
7.1 The Three Caches
SNOWFLAKE HAS THREE LAYERS OF CACHING:
1. RESULT CACHE (Cloud Services layer)
    Caches FULL QUERY RESULTS for 24 hours
    Free reuse if query identical and underlying data unchanged
    Cross-VW: any warehouse can serve from result cache
    Triggered by exact SQL match (whitespace OK; comments OK)
2. LOCAL DISK CACHE (Per-VW SSD)
    Caches micro-partitions read from cloud storage
    Survives only while VW is running
    Suspending a VW PURGES the cache
    Auto-resume gets a COLD cache → first queries slow again
3. METADATA CACHE (Cloud Services)
    Stats, table structure cached
    Powers fast COUNT(*), MIN/MAX on indexed columns
```

```
7.2 Cache Implications
COST: cached results = free (no warehouse compute)
PERFORMANCE: same query repeated = milliseconds
IMPACTS DESIGN:
   Dashboards refreshing same queries → mostly cache hits → cheap
   Auto-suspend savings vs cold-cache cost: trade-off
    aggressive auto-suspend (60s) = cheap when idle, slow first query
    less aggressive (600s) = warmer cache, more idle cost
   DEV environments: small VW with 60s auto-suspend
   PROD BI: medium VW with 300-600s auto-suspend
   PROD ETL: large VW, run-on-demand, no auto-resume
INVALIDATION:
  Any DML on underlying tables INVALIDATES result cache for those queries
  Time-dependent functions (CURRENT_TIMESTAMP) also invalidate
  → Many "dynamic" dashboards inadvertently miss cache because of NOW() calls
```

```
7.3 Concurrency Model
ONE VW HAS LIMITED CONCURRENCY:
  Default MAX_CONCURRENCY_LEVEL = 8 per cluster
  9th query waits in QUEUE
OPTIONS WHEN HIT:
  1. INCREASE WAREHOUSE_SIZE (more nodes, but doesn't directly help concurrency)
  2. MULTI-CLUSTER WAREHOUSE (add clusters under load)
  3. SEPARATE WORKLOADS into different VWs
ANTI-PATTERN: one giant VW for everything
  → contentious queueing, poor isolation
GOOD PATTERN:
  ETL_WH (large, run on schedule)
  BI_WH  (medium, multi-cluster, always-warm)
  AD_HOC_WH (small, auto-suspend aggressively)
  ML_WH  (large, run on demand)
```

7.4 Right-Sizing Warehouses HEURISTIC: PICK THE SMALLEST VW THAT MEETS YOUR LATENCY TARGET. PROCESS:

#### 1. Start at S or M for the workload

#### 2. Run representative queries

Cloud Warehouses

#### 3. Check warehouse load (DASHBOARD: warehouse load history)

#### 4. If queries fast + warehouse not saturated → try smaller

#### 5. If queries slow but warehouse not saturated → consider query tuning first

```
  6. If warehouse saturated → step up size
DOUBLING SIZE ≈ HALVING TIME (for parallelizable work)
   XS: $2-3/hour     S: $4-6      M: $8-12      L: $16-24
   XL: $32-48         2XL: $64-96   3XL: $128-192 ...
COST EQUIVALENCE:
  Running M for 4 hours ≈ running 2XL for 30 minutes (same credits)
  → Optimize for time-to-result, not size of warehouse
WORKLOAD-DEPENDENT:
  Small queries: size doesn't help much (overhead dominates)
  Large scan: size scales linearly
  Joins/aggs: size scales until shuffle saturates network
```

```
7.5 Query Acceleration Service (QAS)
SOMETIMES A SMALL VW + OCCASIONAL HEAVY QUERIES = wrong fit.
QUERY ACCELERATION SERVICE:
  Offloads parts of a heavy query to additional serverless compute.
  Set max scale factor (1-10×).
  Pay only when QAS activates.
USE CASE:
  Small VW running mostly small queries.
  Occasional large scan needs more power.
  QAS adds capacity dynamically.
CONFIG:
  ALTER WAREHOUSE my_wh SET
    ENABLE_QUERY_ACCELERATION = TRUE
    QUERY_ACCELERATION_MAX_SCALE_FACTOR = 5;
CAVEATS:
  Only certain queries qualify (scan-heavy, parallelizable)
  Newer feature; not all teams need it
■ PRINCIPAL MINDSET: Snowflake's per-second compute pricing makes operational thinking different from
on-prem MPP. You're not 'sized for peak' — you're 'right-sized for now, auto-resumed when needed, suspended
otherwise'. The biggest cost mistake teams make: leaving warehouses running 24/7 because that's how Redshift
worked. With Snowflake, idle is free — but only if you auto-suspend. Audit warehouses quarterly; one always-on
XL is $30K/month.
```

Cloud Warehouses

#### 8. Snowflake Cost Model

```
8.1 What You Pay For
SNOWFLAKE BILLS THREE THINGS:
1. STORAGE
    Compressed bytes (active + time travel + fail-safe)
    ~$23/TB/month flat rate (varies by region)
    Active storage: current table state
    Time Travel: last 1-90 days of changes (default 1 day Standard, 90 day Enterprise)
    Fail-safe: 7 days after Time Travel expires (cannot be disabled)
2. COMPUTE (credits)
    Virtual warehouses: per-second billing while running
    Cloud services overhead: usually included unless >10% of compute
    Serverless features: Snowpipe, automated reclustering, search optimization
3. DATA TRANSFER
    Egress to other regions / clouds
    Often the surprise on the bill
```

```
8.2 Credits
ONE CREDIT = compute unit
  Warehouse Size  Credits/Hour
  XS              1
  S               2
  M               4
  L               8
  XL              16
  2XL             32
  3XL             64
  4XL             128
  5XL             256
  6XL             512
CREDIT COST: $2-4 each (varies by edition: Standard, Enterprise, Business Critical, VPS)
EXAMPLE:
  L warehouse running 8 hours/day weekdays =
  8 credits × 8 hours × 22 days = 1,408 credits/month
  × $3/credit = $4,224/month per L warehouse
  Same L warehouse 24/7 = $4,224 × ~3.75 = ~$15,800/month
  → AUTO-SUSPEND when idle = 75% savings
```

```
8.3 Per-Second Billing
WAREHOUSE BILLING:
   60-second minimum on resume
   After that: per-second billing
   Auto-suspend: stops billing immediately
  Example:
    Resume at 10:00:00, run 30-second query, finish at 10:00:30
    BILL: 60 seconds (minimum)
```

```
    Resume at 10:00:00, idle until 10:05:00 with auto-suspend=60s
    BILL: 5 minutes (60s minimum * 5)? NO — billing continues until suspend.
    Actually: 5 minutes 60 seconds = 360 seconds.
AGGRESSIVE AUTO-SUSPEND (60s) suits sporadic ETL.
MODERATE (300s) suits BI dashboards (warm cache helps).
LONG (1800s) only if cache rebuild is very expensive AND continuous usage.
```

#### 8.4 Storage Cost Components

Cloud Warehouses

```
ACTIVE STORAGE: current table state, all rows.
TIME TRAVEL STORAGE:
  Old versions retained for DATA_RETENTION_TIME_IN_DAYS
  Default: 1 day (Standard), up to 90 days (Enterprise+)
  Each UPDATE/DELETE creates new partitions; old versions kept
  HIGH-CHURN TABLES: time travel storage can be 5-10x active
FAIL-SAFE:
  7 days after Time Travel
  Cannot be disabled, cannot be configured
  For disaster recovery; only Snowflake can restore from it
  Adds to your storage bill (~7 days more retention)
CLONED OBJECTS:
  Zero-copy clones share storage until modified
  Only modifications cost extra storage
```

```sql
8.5 The High-Cost Patterns
COMMON COST WASTE:
1. WAREHOUSES NEVER AUTO-SUSPENDING
   → 24/7 billing for sporadic use
   FIX: enable auto-suspend with reasonable timeout
2. LARGE WAREHOUSES FOR SMALL QUERIES
   → 16x credit cost vs S
   FIX: right-size; use multi-cluster for concurrency, not size
3. RESULT CACHE INVALIDATED BY CURRENT_TIMESTAMP() / NOW()
   → Every dashboard query is a cache miss
   FIX: parameterize timestamps; use DATE_TRUNC() at fixed boundaries
4. SELECT * IN ETL
   → Scans columns you don't use
   FIX: project only needed columns
5. NESTED CTES MATERIALIZING REPEATEDLY
   → Same logic recomputed across query
   FIX: materialize intermediate tables when used 3+ times
6. HEAVY UPDATE WORKLOAD ON LARGE TABLES
   → Each UPDATE rewrites entire affected partitions
   → Time Travel storage explodes
   FIX: batch updates; reduce retention if business allows; consider MERGE
7. CROSS-REGION DATA TRANSFER
   → Egress charges 5-10x storage costs
   FIX: keep compute + data in same region
8. UNUSED LONG-RETENTION TIME TRAVEL
   → 90 days × high-churn = huge storage
   FIX: set DATA_RETENTION_TIME_IN_DAYS per-table based on actual need
```

```sql
8.6 Cost Monitoring
SNOWFLAKE EXPOSES USAGE VIA INFORMATION_SCHEMA:
-- Warehouse credit usage
SELECT * FROM SNOWFLAKE.ACCOUNT_USAGE.WAREHOUSE_METERING_HISTORY
WHERE START_TIME > DATEADD(day, -7, CURRENT_TIMESTAMP);
-- Per-query credit usage
SELECT * FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
WHERE START_TIME > DATEADD(day, -1, CURRENT_TIMESTAMP)
ORDER BY EXECUTION_TIME DESC;
-- Storage by table
SELECT table_name, active_bytes, time_travel_bytes, failsafe_bytes
FROM SNOWFLAKE.ACCOUNT_USAGE.TABLE_STORAGE_METRICS
ORDER BY active_bytes DESC LIMIT 50;
```

Cloud Warehouses

THIRD-PARTY TOOLS: Capital One Slingshot, SELECT.dev, Sigil — all read these tables and provide dashboards + recommendations. ■ INTERVIEW ANGLE: Q: Your Snowflake bill jumped from $30K to $80K in one month. What do you investigate? Staff: Three places in order. (1) Warehouse credit usage by warehouse — WAREHOUSE_METERING_HISTORY. Identify the warehouse with the biggest jump. (2) Query history for that warehouse — find expensive queries, new patterns, or new users. Common causes: a new dashboard with bad SQL, an analyst running huge ad-hoc queries, a runaway scheduled job. (3) Storage growth — TABLE_STORAGE_METRICS. Sometimes Time Travel storage explodes from a high-churn table. Operational controls: auto-suspend on every warehouse, resource monitors with thresholds, alert on daily credit anomalies. The discipline isn't reactive debugging; it's proactive monitoring that catches the $5K/day waste before it becomes $50K/month.

Cloud Warehouses

## PART III — BIGQUERY

#### 9. BigQuery Architecture: Dremel & Serverless

9.1 The Dremel Heritage BigQuery is the externalized form of Google's Dremel, used internally since 2006 for analyzing logs and metrics at Google scale. Dremel was the first system to combine columnar storage, MPP execution, and a tree-based query execution model — the foundation of every cloud warehouse since.

```
9.2 The Architecture
BIGQUERY IS RADICALLY SEPARATED:
  Storage (Colossus) ←→ Compute (Dremel) ←→ Network (Jupiter)
■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■
■                   BIGQUERY QUERY ENGINE                       ■
■                                                                ■
■           ■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■                  ■
■           ■  ROOT NODE (query coordinator) ■                  ■
■           ■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■                  ■
■                        ■                                       ■
■              ■■■■■■■■■■■■■■■■■■■■■                            ■
■              ▼         ▼         ▼                             ■
■         ■■■■■■■■■■ ■■■■■■■■■■ ■■■■■■■■■■                     ■
■         ■MIXER 1 ■ ■MIXER 2 ■ ■MIXER 3 ■  (intermediate agg)  ■
■         ■■■■■■■■■■ ■■■■■■■■■■ ■■■■■■■■■■                     ■
■             ■          ■          ■                            ■
■        ■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■                       ■
■        ▼    ▼          ▼          ▼    ▼                        ■
■   ■■■■■■■ ■■■■■■■  ■■■■■■■  ■■■■■■■ ■■■■■■■                  ■
■   ■SLOT ■ ■SLOT ■  ■SLOT ■  ■SLOT ■ ■SLOT ■  (leaf workers)    ■
■   ■■■■■■■ ■■■■■■■  ■■■■■■■  ■■■■■■■ ■■■■■■■                   ■
■      ■       ■         ■         ■       ■                      ■
■      ▼       ▼         ▼         ▼       ▼                      ■
■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■
          ■       ■         ■         ■       ■
          ▼       ▼         ▼         ▼       ▼
     ■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■
     ■       COLOSSUS (storage layer)             ■
     ■  Columnar files (Capacitor format)         ■
     ■  Sharded, replicated, durable              ■
     ■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■
```

```
9.3 The Tree of Execution
DREMEL'S INSIGHT: hierarchical query execution.
LEAF NODES (SLOTS):
  Read storage. Scan column data. Apply filters and projections.
  Partial aggregation.
MIXER LEVEL:
  Receive partial results from leaves. Merge / aggregate intermediate results.
ROOT:
  Final aggregation, sort, limit.
  Returns result to client.
WHY A TREE:
  Aggregation pushed DOWN to leaves (combiner pattern).
  Network traffic minimized (aggregated results, not raw rows).
  Scales horizontally — add slots = more leaves.
```

Cloud Warehouses

```
9.4 Capacitor: The Storage Format
COLUMNAR FORMAT optimized for OLAP. Proprietary.
   Strong compression (dictionary, RLE, etc.)
   Striping across many physical blocks
   File-level statistics (min/max, distinct counts)
   Replicated across multiple Colossus shards
   Independent of query execution layer
WRITE-OPTIMIZED REGION (cache):
  Recent inserts land in a row-oriented cache
  Background convertor migrates to columnar Capacitor
  Reads merge cache + Capacitor for consistency
THIS IS WHY STREAMING INSERTS work well in BigQuery —
they go to the row cache first, queryable within seconds.
```

```
9.5 Serverless From Day One
BIGQUERY HAS NO CONCEPT OF "CLUSTER" OR "INSTANCE":
  Submit a query → BigQuery allocates slots → runs → done
  No warm-up, no clusters to size, no auto-suspend
YOU PAY FOR:
   Storage you keep (per byte)
   Compute used (per byte scanned OR slot-hour reserved)
OPERATIONAL CONSEQUENCE:
  Cold start: BigQuery is ALWAYS warm. First query as fast as Nth.
  No "resume my warehouse" step.
  No idle clusters to suspend.
THE PRICE PAID:
  Less control over compute (you don't pick instance sizes)
  Performance depends on Google's capacity scheduling
  Spike contention: at heavy multi-tenant times, slots are constrained
  Reservations protect SLAs (see next chapter)
■ PRINCIPAL MINDSET: BigQuery's serverless model is fundamentally different from Snowflake's virtual
warehouses. Snowflake gives you control over compute (pick a size, manage warmth). BigQuery hides it (you
just submit queries). The trade-off: Snowflake = predictability + tuning levers; BigQuery = simplicity + zero ops.
For sporadic workloads with unpredictable volume, BigQuery often wins on cost. For steady, high-volume ETL,
Snowflake or reserved BigQuery slots win on predictability.
```

Cloud Warehouses

#### 10. Slots, Reservations & Pricing Models

```
10.1 What a Slot Is
SLOT = unit of compute in BigQuery.
   A "virtual CPU" for query execution
   Each slot processes one task in the query DAG
   Queries use as many slots as they need + slots are available
EXAMPLE:
  Query has 50 stages, each parallelizable to 20 workers.
  If 1000 slots available → query uses 1000 in parallel.
  If 100 slots available → query slowed (queueing within query).
SLOTS ARE EPHEMERAL:
  Per-query allocation by the BigQuery scheduler.
  No "my slots" in on-demand mode.
  Reserved capacity gives you guaranteed slots.
```

```
10.2 The Two Pricing Models
ON-DEMAND PRICING ("pay per byte scanned")
  $6.25 per TB scanned (varies by region)
  Slots are allocated from a shared Google pool (up to 2000 per project by default)
  No commit; no reservations
  Cost varies wildly with query patterns
  GOOD FOR: sporadic / unpredictable / small workloads
  BAD FOR: large repeating workloads (scan cost adds up)
CAPACITY PRICING ("pay per slot-hour")
  Reserve N slots for a project / org
  $40-60 per slot-hour depending on edition (Standard / Enterprise / Plus)
  PREDICTABLE cost; UNPREDICTABLE performance (queries queue if slots full)
  GOOD FOR: steady, high-volume workloads
  BAD FOR: bursty / sporadic (idle reservations waste $$)
EDITIONS (since 2023):
  STANDARD       — basic SQL, no advanced features
  ENTERPRISE     — multi-statement, materialized views, BI Engine, ...
  ENTERPRISE PLUS — full feature set + better SLAs
```

```
10.3 The Slot Math
ON-DEMAND COST EXAMPLE:
  Scan 100 TB/month at $6.25/TB = $625
  Sounds cheap!
  But: every query scans, including ad-hoc / repeat / mistakes.
  At higher volumes (1+ PB/month), costs balloon.
CAPACITY COST EXAMPLE:
  500 slots × $40/hour × 730 hours = $14,600/month (Enterprise edition)
  Predictable. Queries run only when slots free.
CROSSOVER POINT (rough):
  Beyond ~200 TB scanned/month, capacity tends to be cheaper
  Below that, on-demand often wins
REAL COMMON PATTERNS:
   Mix: capacity for predictable ETL + on-demand for ad-hoc
   Capacity baseline + autoscaling (Slot Autoscaler, since 2023)
   Per-project allocation: dev gets on-demand, prod gets capacity
```

10.4 Reservations RESERVATION = a slot allocation owned by a project / folder / org. CREATE RESERVATION my_reservation

Cloud Warehouses

SLOTS = 500;

```
  ASSIGN PROJECT 'my-project' TO RESERVATION my_reservation;
QUERIES IN my-project USE my_reservation's slots.
Other projects use their own reservations or fall back to on-demand.
SUB-RESERVATIONS (allocations):
  Split a reservation by project / workload
  ETL gets 300 slots, BI gets 200 slots
  Both queue within their allocation, can borrow if other idle
AUTOSCALING (since 2023):
  reservation_size BETWEEN 100 AND 1000
  Reservation grows / shrinks based on demand
  Closes the gap between on-demand and reserved
EDITIONS APPLY:
  Reserved slots must be in an edition (Standard/Enterprise/Enterprise Plus)
  More features = higher slot cost
```

```
10.5 The Cost Strategy
TYPICAL ENTERPRISE BIGQUERY COST STRUCTURE:
PRODUCTION ETL:
  Reservation: 1000 slots, autoscaling 500-2000
  Edition: Enterprise (need materialized views)
  Cost: ~$30K-60K/month predictable
PRODUCTION BI (dashboards):
  Reservation: 500 slots in BI-specific reservation
  Often paired with BI Engine for sub-second queries
  Cost: $15-25K/month
DEV / AD-HOC:
  On-demand
  Per-user query quotas to prevent runaway scans
  Cost: variable, $1-10K/month
WHY THIS WORKS:
  ETL is predictable → reservation
  BI is steady → reservation (with BI Engine for hot paths)
  Ad-hoc is bursty/variable → on-demand
■ INTERVIEW ANGLE: Q: Your BigQuery on-demand bill is $50K/month. Should you switch to reservations?
Staff: Probably yes, but it depends on the query pattern. Look at INFORMATION_SCHEMA.JOBS — how
concurrent are queries? What's the peak slot demand? On-demand at $6.25/TB means $50K = 8 PB scanned. At
Enterprise rates ($40/slot-hour), that's roughly 1000 reserved slots at $30K, IF they're utilized 80%+. The
decision: (1) measure peak vs average slot usage; (2) if average > 50% of peak, reservations save real money;
(3) if very bursty, consider autoscaling reservations (baseline + flex). One bad pattern: blanket reservation
without per-team allocations — then one team's runaway job starves everyone. Use sub-allocations with
per-project quotas.
```

Cloud Warehouses

#### 11. Partitioning, Clustering & BI Engine

```sql
11.1 Partitioning in BigQuery
BIGQUERY SUPPORTS THREE PARTITION TYPES:
1. TIME-UNIT (DATE / TIMESTAMP / DATETIME)
   CREATE TABLE events (...) PARTITION BY DATE(event_ts);
    Most common
    Each day's data lives in a separate "shard"
    Pruning happens automatically when WHERE event_ts BETWEEN ... AND ...
2. INTEGER RANGE
   CREATE TABLE users (...) PARTITION BY RANGE_BUCKET(user_id, GENERATE_ARRAY(0, 1000000, 100
0));
    Hash-style partition by integer column
    Buckets of 1000
3. INGESTION TIME
   CREATE TABLE events (...) PARTITION BY _PARTITIONTIME;
    Automatic partition based on when row was loaded
    Default for streaming inserts
LIMITS:
   4,000 partitions per table
   Choose a column with reasonable cardinality
```

```sql
11.2 Clustering
WITHIN PARTITIONS, CLUSTERING SORTS DATA.
  CREATE TABLE events (...)
    PARTITION BY DATE(event_ts)
    CLUSTER BY user_id, country;
   Up to 4 clustering columns
   Order matters: first column most effective for pruning
   Auto-reclustering: BigQuery does it for you (no scheduled jobs)
EFFECT:
  Queries with WHERE user_id = X AND DATE(event_ts) = Y
  → Partition pruning eliminates other days
  → Within partition, files clustered by user_id → file-level pruning
COST OF CLUSTERING:
  Reclustering uses slots (charged against your reservation or on-demand)
  Usually small overhead, worth it for selective queries
```

```
11.3 BI Engine
BI ENGINE = in-memory cache for BigQuery query results.
  CREATE BI ENGINE RESERVATION my_engine
    MEMORY_SIZE_GB = 100
    PROJECT_ID = 'my-project';
WHAT IT DOES:
   Caches frequently-accessed data in RAM
   Sub-second query response for cached portions
   Especially valuable for Looker / Tableau dashboards on BigQuery
WHEN IT HELPS:
  ✓ Dashboards repeating similar queries
  ✓ Selective queries on the same hot data
  ✓ BI users hitting the same partitions repeatedly
LIMITS:
   Up to 250 GB memory reservation
   Not all query types accelerate (some functions / operations excluded)
   Cache misses fall back to regular BigQuery (slower)
```

Cloud Warehouses

```
PRICING:
  Pay for reserved memory (per GB-hour).
  Pays for itself when query latency reduction matters.
```

```sql
11.4 Materialized Views
MATERIALIZED VIEWS in BigQuery:
  Pre-computed aggregates that auto-refresh on base table change
CREATE MATERIALIZED VIEW daily_revenue AS
  SELECT DATE(event_ts) AS day, SUM(amount) AS revenue
  FROM events
  GROUP BY day;
BENEFITS:
  ✓ Sub-second queries for common aggregations
  ✓ Query rewriting: BigQuery may use MV automatically if base table queried
  ✓ Incrementally refreshed
LIMITS:
   Only certain aggregations supported (SUM, COUNT, AVG, MIN, MAX)
   Cannot include certain features (JOINs limited, no subqueries)
   Each refresh consumes slots
WHEN TO USE:
  ✓ Heavily-queried daily/hourly aggregations
  ✓ BI dashboard hot paths
  ✓ Pre-computed analytical staging
```

```
11.5 Query Optimization in BigQuery
KEY LEVERS:
1. PARTITION YOUR LARGE TABLES on date column
2. CLUSTER on common filter columns
3. PROJECT only needed columns (NOT SELECT *)
4. FILTER on partition column to prune scans
5. APPROX_* aggregates for huge cardinality
   APPROX_COUNT_DISTINCT, APPROX_QUANTILES — orders of magnitude cheaper
6. AVOID CROSS JOINS (cartesian explosions)
7. USE STRUCTS / ARRAYS for nested data instead of joins
8. MATERIALIZED VIEWS for common aggregations
9. BI ENGINE for dashboard hot paths
DEBUGGING:
  EXPLAIN: bytes scanned, slot ms, stages
  Query Plan tab in console: see DAG, per-stage time
  INFORMATION_SCHEMA.JOBS: full query history with metrics
■ PRINCIPAL MINDSET: BigQuery's serverless nature seduces teams into ignoring optimization — 'just throw
queries at it'. At small scale that's fine. At enterprise scale ($50K+/month), every query pattern matters.
Partitioning + clustering on large tables is non-negotiable. SELECT * is the most expensive habit. BI Engine for
dashboards pays for itself if you have 100+ analysts. Materialized views are the secret weapon for predictable BI
cost.
```

Cloud Warehouses

## PART IV — REDSHIFT

#### 12. Redshift Architecture: Leader + Compute

```
12.1 The Classic Architecture
■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■
■              LEADER NODE                           ■
■   Parses, plans, optimizes queries                ■
■   Coordinates query execution                     ■
■   Single point of metadata + coordination         ■
■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■
                       ■
       ■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■
       ▼               ▼               ▼
  ■■■■■■■■■■■    ■■■■■■■■■■■    ■■■■■■■■■■■
  ■COMPUTE 1■    ■COMPUTE 2■    ■COMPUTE 3■  ... up to 128 nodes
  ■         ■    ■         ■    ■         ■
  ■SLICE 1A ■    ■SLICE 2A ■    ■SLICE 3A ■
  ■SLICE 1B ■    ■SLICE 2B ■    ■SLICE 3B ■  (multiple slices per node)
  ■■■■■■■■■■■    ■■■■■■■■■■■    ■■■■■■■■■■■
LEADER + COMPUTE NODES PATTERN.
  Leader is single (no HA in original Redshift; managed by AWS)
  Each compute node has multiple "slices" (CPU cores essentially)
  Data is DISTRIBUTED across slices per the DIST style
```

```
12.2 RA3 Nodes: The Architectural Refresh
ORIGINAL REDSHIFT (DS2, DC2 node types):
   Storage tied to compute nodes
   Add capacity = add nodes (data redistributes; slow)
   All-or-nothing scaling
RA3 NODES (2019+):
   Storage decoupled — uses Redshift Managed Storage (RMS) on S3 underneath
   Local SSDs as cache; data ultimately in S3
   Add/remove nodes without data movement
   Closer to Snowflake's separation model
  ra3.xlplus, ra3.4xlarge, ra3.16xlarge — different node sizes
THE MIGRATION:
  Most Redshift customers have moved to RA3.
  DS2/DC2 are legacy at this point.
REDSHIFT SERVERLESS (2022):
   No nodes to manage; AWS provisions on demand
   Pay-per-second based on RPU (Redshift Processing Units)
   Closer to BigQuery / Snowflake's model
   Best for: variable workloads, dev/test, non-24/7 use
```

```
12.3 The Data Distribution Model
WHEN YOU CREATE A TABLE, you choose a DISTRIBUTION STYLE:
KEY DISTRIBUTION
  CREATE TABLE orders (...) DISTKEY (user_id);
   Hash on user_id → place row on slice
   Same user_id → same slice
   Joins on user_id are co-located (fast)
   Risk: skew if some user_ids dominate
ALL DISTRIBUTION
```

Cloud Warehouses

```
  CREATE TABLE small_dim (...) DISTSTYLE ALL;
   Replicate full table to every slice
   Good for small dimension tables
   Cost: storage multiplied by slice count
EVEN DISTRIBUTION
  CREATE TABLE staging (...) DISTSTYLE EVEN;
   Round-robin across slices
   No skew, but joins require redistribution
AUTO DISTRIBUTION (default in newer Redshift)
  Let Redshift choose based on table size / usage
  Small → ALL, large → KEY (if it can infer), default → EVEN
THE DISTKEY CHOICE IS YOUR BIGGEST PERFORMANCE LEVER.
Joins on the distkey are fast; otherwise data shuffles.
```

```
12.4 What Makes Redshift Different
COMPARED TO SNOWFLAKE / BIGQUERY:
  ✓ AWS-native: tight integration with S3, Lambda, IAM, VPC
  ✓ Federated queries: query operational DBs (Postgres, MySQL) directly
  ✓ Redshift Spectrum: query Parquet on S3 like external tables
  ✓ Concurrency Scaling: auto-add clusters for read concurrency
  ✗ Historically: more tuning required (distkey/sortkey choices)
  ✗ Vacuum / analyze maintenance (newer auto-versions help)
  ✗ Resize operations historically slow
  ✗ Less elastic than Snowflake for ad-hoc scaling
EVOLUTION:
  Newer features (RA3, Serverless, AQUA, Concurrency Scaling, AUTO distribution/sort)
  have closed many gaps. Redshift in 2024 ≠ Redshift in 2018.
```

Cloud Warehouses

#### 13. Sort Keys, Dist Styles & Vacuum

```sql
13.1 Sort Keys: The Performance Multiplier
SORT KEY: column(s) by which data is physically sorted within a slice.
  CREATE TABLE orders (...)
    DISTKEY (user_id)
    SORTKEY (event_date);
EFFECT:
   Data within a slice is sorted by event_date
   Range queries on event_date are fast (sequential scan)
   Zone maps store min/max per block (1 MB blocks)
   Queries can skip blocks where ranges don't match
```

```
13.2 Compound vs Interleaved Sort Keys
COMPOUND SORT KEY (default):
  SORTKEY (a, b, c)
  Sorts by a first, then b, then c
  GOOD: queries filtering on a (and a+b, a+b+c)
  BAD: queries filtering on c alone
INTERLEAVED SORT KEY:
  SORTKEY INTERLEAVED (a, b, c)
  Equal weight to all sort columns (Z-order curve)
  GOOD: queries filtering on any single column
  BAD: requires regular re-sorting; higher vacuum overhead
RULE OF THUMB:
   Compound for time-series + occasional secondary filter (most common)
   Interleaved only if queries truly use any column equally
   AUTO sort key in newer Redshift versions
```

```
13.3 VACUUM & ANALYZE
VACUUM = reclaim space + re-sort rows.
  After many INSERT/UPDATE/DELETE, rows can be unsorted within slices.
  VACUUM re-sorts and removes deleted row markers.
  VACUUM SORT ONLY;    -- re-sort without space reclaim
  VACUUM DELETE ONLY;  -- reclaim space from deletes
  VACUUM FULL;         -- both
  CAUTION: VACUUM is expensive on large tables (rewrites everything).
  Newer Redshift: AUTO VACUUM SORT runs in background.
ANALYZE = refresh statistics for the query optimizer.
  Stale stats → bad query plans → slow queries.
```

ANALYZE my_table;

```
  Newer Redshift: AUTO ANALYZE handles this.
THE OLD REDSHIFT OPERATIONAL PAIN:
  Manually scheduling VACUUM + ANALYZE.
  Doing it during off-hours (it impacts performance).
  Forgetting → slow queries blamed on Redshift itself.
NEW REDSHIFT (RA3 + auto features):
  Most of this is automated. Operational burden significantly reduced.
```

```
13.4 Concurrency Scaling
WHAT IT IS:
  When concurrent queries exceed the main cluster's capacity,
  Redshift auto-provisions additional CLUSTERS to handle the overflow.
```

Cloud Warehouses

```
ENABLE:
  ALTER USER my_user SET enable_query_offloading TO TRUE;
EFFECT:
  Read-only queries that would queue are routed to a concurrency-scaling cluster.
  Up to 10 additional clusters per main cluster.
  Pricing: ~free for first hour/day; pay per second after.
GOOD FOR:
  BI workloads with bursty concurrency (mornings, end-of-month).
  Reports running at the same time clogging the main cluster.
```

```
13.5 The Redshift Workload Manager (WLM)
WLM divides cluster resources among QUEUES.
DEFAULT WLM:
   One queue with all the cluster's memory + slots
   Queries compete for resources
MANUAL WLM:
  Create multiple queues, each with allocated memory + concurrency.
  Assign users / groups to queues.
```

QUEUE: ETL — 50% memory, 5 slots QUEUE: BI — 30% memory, 15 slots QUEUE: ADHOC — 20% memory, 10 slots AUTO WLM (recommended): Redshift dynamically allocates based on query type and history. Less manual tuning. THE PATTERN: split workloads, prevent one runaway query from starving BI. ■ PRODUCTION SCENARIO: A team ran Redshift with default DISTSTYLE on a fact_orders table. Joins to dim_users were slow (50+ second queries). The dim_users was small, so they set DISTSTYLE ALL on it — replicated to every slice. Joins became local; queries dropped to 2 seconds. The trade-off: 1 GB dimension × 32 slices = 32 GB storage (vs 1 GB without ALL). Worth it for small dimensions. Lesson: distribution style choice has order-of-magnitude performance implications. Audit it for the top 10 most-queried tables.

Cloud Warehouses

#### 14. Redshift Serverless & RA3 Nodes

```
14.1 Redshift Serverless
REDSHIFT SERVERLESS (2022):
  No cluster to manage. AWS provisions compute on demand.
  Pricing in RPUs (Redshift Processing Units) per second.
  CONFIG:
    BASE_CAPACITY: 8 to 512 RPUs
    Auto-scales up to MAX_CAPACITY
    Pause/resume automatic
  COST:
    $0.36 per RPU-hour (typical region)
    8 RPU base = ~$2.88/hour minimum when active
    Pauses when idle (no charges)
WHEN IT FITS:
  ✓ Dev / test environments
  ✓ Variable workloads (some hours busy, others idle)
  ✓ Spiky BI use
  ✓ Multi-tenant where individual workloads are small
WHEN PROVISIONED IS BETTER:
  ✓ Predictable 24/7 workloads (reserved instances cheaper)
  ✓ Need specific node types (some features only on certain RA3 sizes)
  ✓ Detailed WLM tuning
```

```
14.2 RA3 Node Specifics
RA3 NODE TYPES:
  ra3.xlplus    — 4 vCPU, 32 GB, ~$1.09/hour
  ra3.4xlarge   — 12 vCPU, 96 GB, ~$3.26/hour
  ra3.16xlarge  — 48 vCPU, 384 GB, ~$13.04/hour
MIN/MAX:
  Minimum: 2 nodes (single-node ra3 not allowed for production)
  Maximum: 128 nodes per cluster (cluster-scaling tier dependent)
STORAGE: Redshift Managed Storage (RMS)
   Hot data on local SSD; cold data on S3 (transparent to you)
   Storage billed separately from compute (~$24/TB/month)
   Can scale TBs of cold data cheaply
```

```
14.3 Reserved Instances & Savings
ON-DEMAND PRICING:
  Pay per hour of cluster runtime.
  Most flexible; most expensive.
RESERVED INSTANCES:
  Commit to 1-year or 3-year:
    1-year reserved: ~40% savings vs on-demand
    3-year reserved: ~70% savings vs on-demand
```

```
  WHEN: predictable 24/7 production cluster.
  Don't reserve dev/test that can be paused.
REDSHIFT SERVERLESS NO RESERVATIONS:
  Pay-per-RPU-second. No commitment.
  But: at very steady load, provisioned + reserved is cheaper than serverless.
DECISION:
  Bursty workload → Serverless
  Steady, predictable → Provisioned RA3 with reserved instances
```

#### 14.4 Spectrum and Federated Queries

Cloud Warehouses

```sql
REDSHIFT SPECTRUM:
  External tables on S3 — query Parquet/ORC/Avro directly
  Storage is in S3 (cheap), compute in Redshift (Spectrum charges per byte scanned)
  Like BigQuery on-demand: $5/TB scanned
  CREATE EXTERNAL TABLE spectrum.events
    ... LOCATION 's3://bucket/events/' STORED AS PARQUET;
  SELECT * FROM spectrum.events e
    JOIN local_table l ON e.id = l.id;
  → Massive lake of cold data in S3, hot data in Redshift, joins work.
FEDERATED QUERIES:
  Query Postgres / RDS / Aurora from Redshift:
    CREATE EXTERNAL SCHEMA pg_finance
      FROM POSTGRES
      DATABASE 'finance_prod'
      URI 'finance-db.xyz.amazonaws.com';
```

```sql
    SELECT * FROM pg_finance.transactions WHERE date = TODAY();
  → Avoid ETL by querying operational DB directly
  → Slow for big scans; great for small lookups + joins to warehouse data.
■ INTERVIEW ANGLE: Q: Your team is on AWS and considering Snowflake vs Redshift. What's the decision
framework?
Staff: Several axes. (1) Ecosystem: Redshift is in the same VPC as your apps and tightly integrated with S3,
Lambda, IAM, KMS. Snowflake is separate but multi-cloud-portable. (2) Operational simplicity: Snowflake wins
(less to tune); Redshift has improved but still has more knobs. (3) Cost: depends on workload — Snowflake's
per-second is great for sporadic; Redshift reserved is great for steady 24/7. (4) Features: Snowflake has cleaner
zero-copy clones, time travel; Redshift has Spectrum, Federated Queries, deeper AWS integration. (5) Vendor
lock-in: Redshift = deeper AWS commitment; Snowflake = portable. The 'right' answer is rarely about
performance — they're similar at typical scale. It's about ecosystem fit and operational preference.
```

Cloud Warehouses

## PART V — DATABRICKS SQL

#### 15. Databricks SQL & Photon Engine

15.1 The Databricks SQL Pitch Databricks SQL came at the warehouse problem from the lakehouse direction. Instead of building a closed warehouse, Databricks built a SQL execution engine (Photon) that runs on Delta Lake (and increasingly Iceberg). The pitch: ETL, ML, and BI all on the same data, same platform, same storage.

```
15.2 Photon: The C++ Vectorized Engine
PHOTON (2020+) = native C++ vectorized query engine for Databricks.
   Replaces Spark's JVM SQL engine for SQL workloads
   10-50x faster for many SQL queries
   Compatible with Spark SQL (drop-in replacement)
   Available in: SQL warehouses, jobs clusters, all-purpose clusters
KEY TECHNICAL FEATURES:
   Vectorized execution (SIMD where possible)
   Memory-efficient layout (columnar in CPU cache)
   Native code (no JVM GC pauses)
   Optimized for Parquet / Delta reads
ENABLED BY DEFAULT in SQL warehouses (since ~2022).
```

```sql
15.3 Storage: Delta Lake (Mostly)
DATABRICKS SQL READS / WRITES Delta Lake tables natively.
  CREATE TABLE my_table (...) USING DELTA;
ICEBERG SUPPORT:
   Read: increasing support (via Unity Catalog Iceberg endpoint)
   Write: via Delta UniForm (writes Delta + Iceberg metadata simultaneously)
THE LAKEHOUSE INTEGRATION:
   Same table queryable from Spark notebooks, Databricks SQL, external tools
   No data movement between "warehouse" and "lake"
   Unified governance via Unity Catalog
```

```
15.4 Performance vs Other Warehouses
BENCHMARKS (variously published):
  Databricks claims TPC-DS leadership at certain scales
  Snowflake / BigQuery dispute methodology
REAL-WORLD:
  Databricks SQL is competitive with Snowflake for analytical workloads
  Slightly behind Snowflake for short ad-hoc queries (cold start)
  Strong for large ETL-style queries (Spark heritage)
  Weak compared to BigQuery for absolutely-massive scans (BigQuery's elasticity)
WHERE IT WINS:
  ✓ Same data accessible from Spark ML / Python workflows
  ✓ Lakehouse-native (no separate warehouse copy)
  ✓ Liquid Clustering (newer feature for clustering management)
  ✓ Strong cost-performance for ETL-heavy shops
```

#### 15.5 The Convergence Story

Cloud Warehouses

```
DATABRICKS HAS BEEN ADDING WAREHOUSE FEATURES:
   SQL Warehouses (formerly SQL Endpoints) — 2020+
   Serverless SQL Warehouses — 2022+
   Predictive Optimization — auto-OPTIMIZE/VACUUM
   Lakehouse Federation — query external catalogs from Databricks SQL
   Unity Catalog — managed catalog with RBAC, lineage
SNOWFLAKE HAS BEEN ADDING LAKEHOUSE FEATURES:
   External tables on S3 (Iceberg + Delta)
   Snowpark for Python / Java
   Cortex AI for ML on warehouse data
THE TWO ARE CONVERGING:
  Databricks: lakehouse → adds warehouse features
  Snowflake:  warehouse → adds lakehouse features
  By 2026, the difference may be philosophical, not architectural.
■ PRINCIPAL MINDSET: Databricks SQL is the right warehouse for shops already on Databricks for Spark / ML.
The lakehouse integration is the killer feature: no separate warehouse to ETL into; same data for BI and ML. For
pure warehouse workloads from scratch, Snowflake usually wins on ergonomics. The Databricks-vs-Snowflake
choice is increasingly about ecosystem commitment (Spark? lakehouse-first?) vs warehouse-first thinking.
```

Cloud Warehouses

16. SQL Warehouses, Serverless & Unity Catalog

```
16.1 SQL Warehouses
SQL WAREHOUSE = a compute cluster specialized for SQL queries.
  CLASSIC SQL WAREHOUSE:
     Provisioned in your cloud VPC (AWS/GCP/Azure)
     Sizes: 2X-Small to 4X-Large (similar to Snowflake's XS-4XL)
     Always-on options or auto-stop
  PRO SQL WAREHOUSE:
     Photon-enabled by default
     Predictive I/O, query results caching
     Faster cold starts
  SERVERLESS SQL WAREHOUSE:
     Runs on Databricks-managed infrastructure (not your VPC)
     Fastest startup (~seconds)
     Highest per-second cost but lowest idle cost
     Available in major cloud regions
SIZING:
  Match Snowflake roughly: 2X-Small ≈ Snowflake XS, etc.
  Auto-scale clusters: scale up to N for concurrency
```

```
16.2 DBU Pricing
DATABRICKS UNITS (DBU) = unit of compute.
  Classic SQL warehouse Pro: ~$0.55/DBU (varies)
  Serverless SQL warehouse:  ~$0.70/DBU (slightly more expensive but no VM cost)
```

```
  Plus: cloud infrastructure (EC2/GCE/Azure VM) for classic warehouses.
  Serverless: all-inclusive (Databricks pays cloud costs, charges higher DBU rate).
EXAMPLE:
  Medium SQL warehouse: ~6 DBU/hour
  Running 8 hours/day, 22 days/month:
    6 × 8 × 22 = 1,056 DBU-hours × $0.55 = $581/month (Pro classic)
    + EC2 cost: ~$200/month for the cluster instances
    Total: ~$780/month
OR Serverless: ~$0.70 × 1,056 = $740/month (no separate EC2)
OPERATIONAL CONSIDERATIONS:
  Classic: cluster in YOUR VPC; can talk to private services directly
  Serverless: cleaner separation, faster start, slightly more expensive
```

```
16.3 Unity Catalog
UNITY CATALOG = Databricks's managed catalog (open-sourced 2024).
  HIERARCHY:
    metastore → catalog → schema → table
  Permissions:
    GRANT SELECT ON TABLE x TO `user@company.com`;
    GRANT SELECT ON SCHEMA finance TO `finance_team`;
    GRANT USE_CATALOG ON CATALOG prod TO `engineering`;
  Features:
     Table-level + column-level + row-level access control
     Lineage (table-to-table, column-to-column)
     Audit logs of access
     Cross-workspace sharing
     Supports Delta, Iceberg, Hudi tables (since 2024)
WHY IT MATTERS FOR WAREHOUSES:
  Multi-engine governance:
```

Cloud Warehouses

```
    Spark jobs, SQL warehouse queries, external tools all enforced same way
  Cross-cloud governance:
    Same catalog can serve Delta on S3, Azure Blob, GCS
  Open standard (since 2024):
    Other vendors can integrate (Trino, others adding support)
```

```
16.4 Comparison with Snowflake
DATABRICKS SQL vs SNOWFLAKE:
  STORAGE:
    Databricks: Delta Lake on your cloud bucket (open format)
    Snowflake:  proprietary FDN files in Snowflake-managed buckets (now also external Iceberg
)
  COMPUTE:
    Both: per-second billed virtual warehouses
  COMPATIBILITY:
    Databricks: same data → ML, BI, ETL on same tables
    Snowflake:  Snowpark added Python; ML is newer
```

```
  ECOSYSTEM:
    Databricks: deep ML, Delta, lakehouse architecture
    Snowflake:  deep BI, data sharing, analyst-friendly
  COST:
    Roughly comparable for similar workloads
    Databricks may save on storage (no warehouse-format duplication)
PICK DATABRICKS SQL:
  ✓ Already on Databricks for Spark/ML
  ✓ Lakehouse-first architecture
  ✓ Want open format storage
PICK SNOWFLAKE:
  ✓ Pure analytics-and-BI focus
  ✓ Want simplest operations
  ✓ Multi-cloud important
```

Cloud Warehouses

## PART VI — COMPARISON & SELECTION

#### 17. Feature & Performance Comparison

```
17.1 The Feature Matrix
Feature
Snowflake
BigQuery
Redshift
Databricks SQL
Storage/compute sep
✓ (cleanest)
✓ (serverless)
✓ (RA3+)
✓ (lakehouse)
Multi-cloud
✓ AWS/GCP/Azure
✗ GCP only
✗ AWS only
✓ AWS/Azure/GCP
Per-second billing
✓
✓ (slots)
✓ (Serverless)
✓
Auto-suspend
✓
✓ (auto)
✓ (Serverless)
✓
Zero-copy clones
✓
✓ (table clones)
limited
✓ (Delta clone)
Time travel
✓ (up to 90d)
✓ (7d default)
✓ via snapshots
✓ (Delta)
Native JSON
✓ VARIANT
✓ STRUCT/JSON
✓ SUPER
✓
Streaming ingest
✓ Snowpipe
✓ streaming insert
✓ via Kinesis
✓ Auto Loader
ML/Python in-DB
✓ Snowpark
✓ ML/BigFrames
limited
✓ MLflow
Open format storage
partial (Iceberg)
partial (Iceberg)
✗
✓ Delta/Iceberg
ANSI SQL compliance
✓
✓ (mostly)
✓
✓
```

```
17.2 Performance Reality Check
ALL FOUR CAN PROCESS PB-SCALE DATA.
All four are competitive on most TPC-style benchmarks.
Performance differences in the real world depend on:
  1. WORKLOAD SHAPE
     Short ad-hoc → BigQuery / Snowflake (warm)
     Long ETL    → all similar; Databricks strong
     Concurrency → Snowflake multi-cluster, BigQuery slot reservations
  2. DATA SIZE
     < 1 TB: anything works
     1-100 TB: all four fine
     100 TB+: warehouse choice less important than table design
     PB+: real architectural differences emerge
  3. QUERY PATTERN
     Selective queries: all four with proper clustering
     Full scans: BigQuery's elasticity helps
     Skewed joins: requires specific optimization per warehouse
  4. CACHING BEHAVIOR
     Warm vs cold matters; first-query latency differs
     Snowflake: SSD cache per VW
     BigQuery: BI Engine for hot paths
     Databricks: Photon caching + Delta cache
NEVER PICK BASED ON BENCHMARK NUMBERS.
Run your actual workload on each for 30 days. That's the only valid test.
```

Cloud Warehouses

```
17.3 The Operational Pain Points
SNOWFLAKE:
   Cost runs away if warehouses left on
   Time Travel storage explosion on high-churn tables
   Result cache invalidation surprises
   Cross-region costs
BIGQUERY:
   On-demand cost variability
   Slot starvation in shared reservations
   Query plan opacity (less control than Snowflake)
   Storage billing changes (logical vs physical)
REDSHIFT:
   DISTKEY / SORTKEY choices have order-of-magnitude impact
   Vacuum / analyze maintenance (less so with newer auto features)
   Single-leader bottleneck for some operations
   Cluster resize complexity (improved with RA3)
DATABRICKS SQL:
   Cluster startup latency (less with serverless)
   Delta operational concerns (OPTIMIZE, VACUUM)
   Photon licensing tier confusion
   Less mature than the big three for pure BI workloads
```

Cloud Warehouses

#### 18. Cost Comparison: Dollar-by-Dollar

```
18.1 The Cost Models in Plain Terms
SNOWFLAKE
   Compute: per-second per running warehouse, by size
   Storage: ~$23/TB/month compressed
   Cloud Services: usually included
   Egress: cross-region/cloud charges
BIGQUERY
   On-demand: $6.25/TB scanned
   Capacity: $40-60 per slot-hour (edition-dependent)
   Storage: ~$20/TB/month (active), $10/TB/month (long-term)
   Streaming inserts: $0.01 per 200 MB
REDSHIFT
   Provisioned: per-node-hour (RA3 nodes)
   Serverless: $0.36/RPU-hour (varies)
   RMS storage: ~$24/TB/month
   Concurrency Scaling: ~$ for additional clusters
DATABRICKS SQL
   Compute: DBU per second of warehouse running
   Plus underlying cloud VMs (for classic warehouses)
   Or all-inclusive Serverless DBU rate
   Storage: cloud bucket cost (~$23/TB on S3 standard)
```

```
18.2 A Comparable-Workload Example
WORKLOAD: 200 TB of data, 50 concurrent users (peak),
          1000 dashboards refreshing 4x daily, daily ETL of 10TB.
ROUGH MONTHLY COST ESTIMATES (2024, US regions):
SNOWFLAKE:
  Storage: 200 TB × $23 = $4,600
  ETL warehouse (L, runs 6 hrs/day): 8 cred/hr × 6 × 22 × $3 = ~$3,170
  BI warehouse (M multi-cluster, ~16 hrs/day): ~$8,500
  Ad-hoc (S, 4 hrs/day): ~$300
  TOTAL: ~$16,500-18,000/month
BIGQUERY (capacity pricing):
  Storage: 200 TB × $20 = $4,000
  Slots: 500 baseline + autoscaling: ~$30,000 (Enterprise edition)
  BI Engine: 100 GB × $400 = $400
  TOTAL: ~$34,000/month (predictable)
BIGQUERY (on-demand):
  Storage: 200 TB × $20 = $4,000
  Scans: ~50 TB/day × 30 × $6.25 = $9,375
  ETL scans: ~10 TB/day × 30 × $6.25 = $1,875
  TOTAL: ~$15,250/month (assuming partition pruning works well)
REDSHIFT (provisioned, reserved):
  ra3.4xlarge × 6 nodes, 1-year reserved: ~$10,500
  RMS storage: 200 TB × $24 = $4,800
  Concurrency scaling: ~$500
  TOTAL: ~$15,800/month (steady)
DATABRICKS SQL:
  Storage: 200 TB on S3 × $23 = $4,600
  SQL warehouse (M Pro, ~20 hrs/day): ~$8,000
  ETL via job cluster: ~$3,000
  TOTAL: ~$15,600/month
CRITICAL CAVEATS:
  These are ballpark; real workloads vary 2-3x in either direction.
  Cost-tuning expertise matters as much as choice of warehouse.
  Reserved/committed pricing reduces 30-50% on all four.
```

Cloud Warehouses

```sql
18.3 Cost Patterns by Workload Type
SPORADIC / DEV / TEST:
  Best: BigQuery on-demand, Snowflake (auto-suspend), Redshift Serverless
  Worst: Provisioned Redshift / Snowflake left running 24/7
STEADY 24/7 PRODUCTION:
  Best: BigQuery capacity reserved, Redshift provisioned reserved
  Worst: BigQuery on-demand (if scans are heavy)
BURSTY (mornings + month-end):
  Best: Snowflake multi-cluster auto-scale, BigQuery autoscaling reservations
  Worst: Fixed-size Redshift cluster sized for peak
ML / PYTHON HEAVY:
  Best: Databricks SQL (same data as Spark/ML)
  Other: Snowflake Snowpark / BigQuery ML (less mature but improving)
MULTI-CLOUD:
  Best: Snowflake (multi-cloud native), Databricks SQL (works on AWS/Azure/GCP)
  Limited: Redshift (AWS), BigQuery (GCP)
■ PRODUCTION SCENARIO: A team migrated from Snowflake to BigQuery to 'save money' based on a
consultant's analysis. The consultant compared Snowflake's bill ($60K/month) to BigQuery's on-demand estimate
($25K). After migration, BigQuery bill came in at $90K — three times projected — because the team's ad-hoc
queries did SELECT * on partitioned tables without WHERE partition filters. Every query scanned everything.
Lesson: cost estimates need a representative workload sample; warehouses have very different cost shapes,
and naive query patterns hit different cost cliffs. Plan migrations with weeks of parallel running, not
back-of-envelope math.
```

Cloud Warehouses

#### 19. When to Pick Which Warehouse

```
19.1 The Decision Tree
STEP 1: CLOUD COMMITMENT
  All AWS? → Redshift or Snowflake-on-AWS
  All GCP? → BigQuery (or Snowflake-on-GCP)
  All Azure? → Snowflake or Synapse (or Databricks)
  Multi-cloud? → Snowflake or Databricks
STEP 2: WORKLOAD PROFILE
  Spiky / unpredictable → BigQuery on-demand or Snowflake auto-suspend
  Steady 24/7 → Redshift reserved or BigQuery capacity
  Heavy ML/Python → Databricks SQL
  Pure analytics/BI → Snowflake or BigQuery
STEP 3: ECOSYSTEM
  Already on Databricks? → Databricks SQL is obvious
  Heavy Looker/Tableau on AWS? → Redshift / Snowflake
  Heavy Looker on GCP? → BigQuery (best LookerML integration)
STEP 4: TEAM SKILLS
  Strong Spark team → Databricks SQL
  Strong SQL analyst team → Snowflake (best UX)
  GCP-native engineers → BigQuery
  AWS-native engineers + want simple → Snowflake; want control → Redshift
STEP 5: GOVERNANCE
  Strict multi-region compliance → BigQuery, Snowflake, Databricks
  Data residency → Snowflake or Databricks (most regions)
  Lineage / catalog critical → Databricks (Unity Catalog mature)
```

```
19.2 The 'Pick Snowflake When' List
✓ Multi-cloud strategy
✓ Sporadic / variable usage (auto-suspend is killer feature)
✓ Strong analyst-first culture
✓ Want minimum operational overhead
✓ Need clean storage/compute separation
✓ Cross-team workload isolation (per-workload virtual warehouses)
✓ Data Sharing across orgs is a goal (Snowflake Data Marketplace)
```

```
19.3 The 'Pick BigQuery When' List
✓ All-in on GCP
✓ Truly serverless preference (zero ops)
✓ Sporadic workloads where on-demand wins
✓ Strong integration with Looker, GA, Firebase, Ads
✓ Geographic distribution important (BigQuery is multi-region native)
✓ ML/AI in-warehouse important (BigQuery ML, BigFrames)
✓ Streaming inserts at scale (Pub/Sub → BQ is golden path)
```

```
19.4 The 'Pick Redshift When' List
✓ All-in on AWS, especially deep VPC integration
✓ Need direct query into operational DBs (Federated Queries)
✓ Tight ETL with S3 / Lambda / Step Functions
✓ Predictable 24/7 workload (Reserved Instances)
✓ Need fine-grained WLM control
✓ Already invested in Redshift (migration cost matters)
✓ Spectrum for hybrid lake/warehouse access
```

```
19.5 The 'Pick Databricks SQL When' List
✓ Already on Databricks for Spark/ML
✓ Lakehouse-first architecture
✓ Want open format storage (Delta, Iceberg)
✓ ML + BI on same data without ETL between
✓ Unity Catalog as governance backbone
```

Cloud Warehouses

```
✓ Cross-team Spark + SQL workflows
✓ Multi-cloud + lakehouse-native
■ INTERVIEW ANGLE: Q: Your CTO asks: 'Should we be on Snowflake or BigQuery?' How do you frame the
answer?
Staff: 'It depends on these axes' — and walk through them. Cloud commitment is usually the biggest filter
(all-GCP → BigQuery; multi-cloud → Snowflake). Then: workload shape (bursty → BQ on-demand; steady
analyst-heavy → Snowflake). Then: ecosystem fit (Looker on GCP loves BQ; Tableau/Power BI work great on
both). Then: team skills (SQL-analyst-heavy → Snowflake's UX; Python/data-science-heavy → both work, slight
BQ edge for BigQuery ML). My recommendation depends on which axis dominates — and I'd want to talk to a
few engineering teams and a few business stakeholders before giving a firm answer. The technical capabilities
are similar; the right answer is about organizational fit.
```

Cloud Warehouses

#### 20. Warehouse vs Lakehouse Decision

20.1 The False Binary Warehouses and lakehouses are converging. Snowflake supports external Iceberg. BigQuery supports Iceberg managed tables. Databricks SQL operates on Delta and Iceberg. In 2025, the question isn't 'warehouse OR lakehouse' — it's what role does each play, and where do they overlap?

```
20.2 Where Warehouses Still Win
✓ ANALYST-FACING SQL
   Snowflake/BigQuery UX for SQL analysts is unbeaten
   Looker / Tableau / Power BI integration is mature
✓ TIGHTLY-MANAGED OPERATIONS
   "Just submit queries" — no compaction, no catalog tuning
   Warehouse handles everything
✓ TIME-TO-FIRST-QUERY
   No clusters to spin up, no Spark to learn
   New user productive in minutes
✓ COMPLEX TRANSACTIONAL SEMANTICS
   Multi-statement transactions, deep ACID
   Snowflake/BigQuery > most lakehouse setups
✓ MICRO-OPTIMIZATIONS
   Query optimizers tuned over decades
   Materialized views, result caching, BI Engine
```

```
20.3 Where Lakehouses Win
✓ MULTI-ENGINE ACCESS
   Same data → Spark, Flink, Trino, DuckDB, warehouse engines
   No data movement
✓ OPEN FORMATS
   No vendor lock-in on storage
   Migrate compute without migrating data
✓ STORAGE COST AT EXTREME SCALE
   PB+ on S3 with intelligent tiering = cheaper than warehouse storage
   Especially for archival / cold data
✓ ML/PYTHON WORKLOADS
   Spark / Pandas / Polars on raw lakehouse files
   No JDBC round-trip to a warehouse
✓ STREAMING-NATIVE WRITES
   Flink + Iceberg / Delta = end-to-end streaming with ACID
   Warehouses struggle with high-velocity row-level updates
```

```
20.4 The Modern Hybrid Architecture
■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■
■  LAKEHOUSE (Iceberg / Delta on S3)                     ■
■   Source-of-truth for raw + silver layers             ■
■   Engineering pipelines (Spark, Flink)                ■
■   ML feature stores                                   ■
■   PB-scale cold storage                               ■
■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■
                     ■ external tables / replication
                     ▼
■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■
■  WAREHOUSE (Snowflake / BigQuery / Databricks SQL)    ■
■   Gold-layer modeled marts                            ■
■   Analyst-facing BI queries                           ■
■   Sub-second BI dashboard hot paths                   ■
```

Cloud Warehouses

```
■   Reverse-ETL / customer-facing data                  ■
■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■
OR:
  Single platform doing both (Databricks SQL on Delta;
                              Snowflake on external Iceberg)
  Same compute fabric on lakehouse storage
```

```sql
20.5 The Long-Term Bet
WHAT WE'RE LIKELY TO SEE BY 2026-2027:
   OPEN FORMATS WIN AT THE STORAGE LAYER
    Iceberg becomes dominant; everyone supports it
    Proprietary formats (Snowflake FDN) become "internal optimization,
    optional external Iceberg" pattern
   CATALOG IS THE BATTLEGROUND
    Polaris, Unity Catalog (open), AWS Glue, Snowflake Horizon
    Governance moves to catalog layer
   COMPUTE BECOMES INTERCHANGEABLE
    Same Iceberg data → query from any engine
    Pick compute per workload (Snowflake for BI, Spark for ETL, etc.)
   WAREHOUSES BECOME "MANAGED COMPUTE ON LAKEHOUSE"
    Less differentiation in storage; more in compute UX, optimizer, governance
THE STAFF-LEVEL BET:
  Architect for open formats, even if your current warehouse is proprietary.
  Use Iceberg-compatible storage where possible.
  Plan migration paths (storage stays; compute can move).
■ PRINCIPAL MINDSET: The warehouse vs lakehouse debate is becoming the wrong frame. The real question
is: what storage format and catalog do I commit to long-term, and what compute engines do I rent for which
workloads? Storage choices have 10-year consequences (you'll have PB of it). Compute choices are revisitable
every 2-3 years. Optimize for storage flexibility (open formats, Iceberg-friendly catalogs); use whatever
warehouse compute serves your current workloads best.
```

Cloud Warehouses

## PART VII — OPERATIONS

#### 21. Query Tuning Across Engines

```sql
21.1 The Universal Tuning Levers
ACROSS ALL FOUR WAREHOUSES:
  1. REDUCE DATA SCANNED
      Partition pruning (predicate on partition column)
      Projection (SELECT only needed columns)
      Predicate pushdown to storage layer
      Approximate functions (APPROX_COUNT_DISTINCT, etc.)
  2. REDUCE NETWORK SHUFFLE
      Co-locate joined tables (DISTKEY, clustering on join key)
      Broadcast small tables
      Pre-aggregate before joining when possible
  3. REUSE COMPUTATION
      Materialized views for repeated aggregations
      CTEs only when used 2-3 times (not for clarity alone)
      Result caching (Snowflake / BigQuery / Databricks)
  4. RIGHT-SIZE COMPUTE
      Don't oversize the cluster for small queries
      Don't undersize for big aggregations
      Use multi-cluster for concurrency, larger for query speed
```

```sql
21.2 SARGable Predicates
SARGABLE = Search ARGument Able → uses indexes / pruning / stats
  GOOD: WHERE event_date = '2024
15'
        WHERE created_at >= '2024
01' AND created_at < '2024
01'
        WHERE user_id IN (1, 2, 3, ...)
  BAD: WHERE DATE_TRUNC('month', event_date) = '2024
01'
       WHERE EXTRACT(year FROM created_at) = 2024
       WHERE user_id::text LIKE '12%'
  Reason: function on the column side prevents the engine from using
          stats / pruning / partitioning.
REWRITES:
  Bad: WHERE EXTRACT(year FROM event_date) = 2024
  Good: WHERE event_date >= '2024
01' AND event_date < '2025
01'
```

```
21.3 Engine-Specific Tuning
SNOWFLAKE:
   CLUSTER BY for tables with selective queries on non-natural columns
   Materialized views for aggregations (Enterprise+)
   Search Optimization for point-lookup workloads
   Query Profile in UI for stage-by-stage timing
   Auto-suspend warehouses; right-size per workload
BIGQUERY:
   Partition + cluster every large table
   Materialized views with smart query rewriting
   BI Engine for BI hot paths
   Use INFORMATION_SCHEMA.JOBS for query history analysis
   Reservation autoscaling vs on-demand: pick right model
```

Cloud Warehouses

```
REDSHIFT:
   DISTKEY on join column for largest join
   SORTKEY on most-filtered column
   Run ANALYZE on stat-stale tables
   Enable Concurrency Scaling for BI
   Use AUTO WLM unless you have specific needs
DATABRICKS SQL:
   OPTIMIZE + Z-ORDER / Liquid Clustering on Delta tables
   Photon-enabled SQL warehouses
   Predictive I/O (newer feature)
   Use Query History to find slow queries
   Same patterns as Spark tuning
```

```
21.4 The Query Profile / EXPLAIN
ALL WAREHOUSES EXPOSE QUERY PROFILES.
SNOWFLAKE: Query Profile in UI
   Per-stage time + bytes
   "Spilling to local/remote" → memory pressure
   "Pruning efficiency" → partition pruning quality
BIGQUERY: Query Plan in console
   Stage-by-stage DAG visualization
   Slot ms per stage = where compute went
   Shuffle bytes between stages
REDSHIFT: EXPLAIN [VERBOSE]
   Plan tree with cost estimates
   Look for nested loop joins (often a sign of bad distribution)
   SVL_QUERY_REPORT for actual execution stats
DATABRICKS SQL: Spark UI / Query Profile
   Same as Spark SQL
   Look for stage skew, shuffle spill, photon coverage
■ INTERVIEW ANGLE: Q: A query that ran in 3 seconds last week now takes 5 minutes. Same data size.
What's your investigation?
Staff: Check the query profile/plan FIRST, not the data. Compare with a previous run if available. Common
causes: (1) Statistics went stale — optimizer chose worse plan; run ANALYZE / wait for auto-stats. (2) Other
workload contention — concurrency scaling triggered or queue formed; check warehouse load / slot usage. (3)
Clustering degraded — new data inserted in non-cluster order, pruning weaker; check clustering depth. (4)
Cache cold — warehouse suspended and resumed; first run after resume is slow. (5) Engine config change —
auto-feature flipped; check release notes. The discipline: always look at the plan before guessing. Tuning by
intuition is how teams spend days on red herrings.
```

Cloud Warehouses

#### 22. Workload Management & Multi-Tenancy

```
22.1 The Multi-Tenant Problem
A WAREHOUSE SERVES MANY WORKLOADS:
   ETL jobs (heavy, scheduled)
   BI dashboards (medium, repeated)
   Ad-hoc analyst queries (variable, sometimes runaway)
   ML training extracts (occasional, very heavy)
   External-facing APIs (low-latency, high-concurrency)
WITHOUT ISOLATION:
  One runaway query → BI dashboards crawl → analyst complaints → outage call
  Big ETL job → ML training waits → pipeline delays
THE SOLUTION: ISOLATE WORKLOADS.
```

```
22.2 Snowflake's Multi-Warehouse Isolation
SNOWFLAKE'S NATURAL PATTERN: virtual warehouses.
  ETL_WH:    L size, scheduled hours, owns ETL workload
  BI_WH:     M size, multi-cluster, owns dashboards
  ADHOC_WH:  S size, aggressive auto-suspend, owns ad-hoc
  ML_WH:     XL size, on-demand, owns ML extracts
  API_WH:    XS multi-cluster, always-warm, owns API queries
Each workload has its own VW.
Different teams can have different cost-attribution.
One workload's spike doesn't impact others.
GOTCHA: shared storage means one workload's big WRITE can still cause
        cache invalidation across all VWs.
```

```
22.3 BigQuery's Reservation Model
BIGQUERY USES SLOT RESERVATIONS for isolation.
  RESERVATION: prod-bi (500 slots, autoscale to 1000)
    Assigned to project: prod-bi
  RESERVATION: prod-etl (200 slots, autoscale to 800)
    Assigned to project: prod-etl
  RESERVATION: dev (50 slots, baseline)
    Assigned to project: dev
ASSIGNMENT:
  CREATE ASSIGNMENT prod-bi-assignment
    ASSIGNEE = 'projects/prod-bi'
    JOB_TYPE = QUERY
    RESERVATION = 'prod-bi';
SUB-ALLOCATIONS:
  Within prod-bi reservation, split between teams:
    finance team: 200 slots
    marketing team: 200 slots
    product team: 100 slots
UNUSED SLOTS borrow across (within same edition tier).
PROTECTS one workload from another's spikes.
```

22.4 Redshift WLM REDSHIFT WORKLOAD MANAGER divides cluster resources into QUEUES. CONFIGURING (Auto WLM recommended for most): Auto WLM: Redshift learns and adapts; less manual Manual WLM: explicit queue config MANUAL EXAMPLE:

Cloud Warehouses

```
  Queue: etl   — 30% memory, 5 concurrent slots
  Queue: bi    — 50% memory, 15 concurrent slots
  Queue: adhoc — 20% memory, 10 concurrent slots
  Users routed to queues by group membership.
QUERY MONITORING RULES (QMR):
  Kill / log queries that violate rules:
    "queries scanning > 100 GB in adhoc queue → abort"
    "queries running > 30 min → switch queue"
This is operational discipline: cap runaway queries.
```

```
22.5 Databricks SQL Warehouse Isolation
DATABRICKS SQL: similar to Snowflake — multiple SQL warehouses per workload.
  prod-etl warehouse
  prod-bi warehouse
  dev warehouse
UNITY CATALOG ENFORCES PERMISSIONS:
  Permissions on catalogs/schemas/tables determine who reads what.
  Warehouse choice = compute isolation.
  Catalog perms = data access control.
```

22.6 The Best Practices 1. SEPARATE WAREHOUSES PER WORKLOAD ETL ≠ BI ≠ ad-hoc ≠ ML 2. RIGHT-SIZE EACH WAREHOUSE TO ITS WORKLOAD Not "one giant warehouse for everything" 3. SET QUERY TIMEOUTS / MONITORING RULES Kill queries running > N minutes; alert on long-runners 4. PER-TEAM COST ATTRIBUTION Resource monitors / tags / labels Each team sees its own bill 5. CHARGE-BACK / SHOW-BACK Engineering teams optimize when they see their cost 6. QUARTERLY REVIEW Warehouses no longer used? Right-sized? Workloads moved teams? Permissions current? ■ PRODUCTION SCENARIO: An analytics platform team had one massive Snowflake warehouse (4XL) serving everything: ETL, BI, ad-hoc, ML extracts. Costs were ~$100K/month with constant complaints about slowness. After splitting into 5 right-sized warehouses (L for ETL, M with multi-cluster for BI, S for ad-hoc, XL on-demand for ML, XS multi-cluster for APIs), costs dropped to ~$60K/month AND latency complaints disappeared. The big warehouse was over-provisioned for most workloads and under-provisioned for some. Workload isolation almost always pays for itself.

Cloud Warehouses

#### 23. Cost Control Playbook

```
23.1 The Cost Hierarchy
COST = WAREHOUSE COMPUTE + STORAGE + EGRESS
USUALLY:
  WAREHOUSE COMPUTE: 70-90% of bill
  STORAGE: 10-30%
  EGRESS: 0-10% (can spike with cross-region)
FOCUS COST OPTIMIZATION ON COMPUTE FIRST.
```

```sql
23.2 The Top 10 Cost Levers
1. AUTO-SUSPEND / AUTO-PAUSE EVERY WAREHOUSE
   Single biggest lever. 24/7 idle = 100% waste.
2. RIGHT-SIZE WAREHOUSES PER WORKLOAD
   Stop using 4XL for queries that fit in M.
3. ELIMINATE SELECT * FROM ETL
   Project only needed columns.
4. PARTITION + CLUSTER LARGE TABLES
   Pruning saves orders of magnitude scan.
5. MATERIALIZED VIEWS for repeated aggregations
   Pre-compute once, query cheap.
6. CONSOLIDATE SIMILAR QUERIES
   Result cache helps; same SQL = free reuse.
7. RETENTION TUNING
   Time travel + fail-safe storage can dwarf active.
   Tune per-table based on actual recovery needs.
8. RESOURCE MONITORS / QUOTAS
   Hard limits per team / project.
   Prevents one runaway query from $50K event.
9. RESERVED / COMMITTED USAGE for steady workloads
   30-70% off on-demand for predictable consumption.
10. EGRESS / CROSS-REGION DISCIPLINE
    Co-locate compute and data. Audit transfers.
```

```
23.3 The Monitoring Stack
WAREHOUSE-NATIVE:
  Snowflake: SNOWFLAKE.ACCOUNT_USAGE.* schemas, Resource Monitors
  BigQuery:  INFORMATION_SCHEMA.JOBS, Cloud Billing reports
  Redshift:  STL_/SVL_ system views, CloudWatch
  Databricks: System tables, Cost Explorer
THIRD-PARTY:
  Snowflake: SELECT.dev, Sigil, Capital One Slingshot
  BigQuery:  Bluesky, Coalesce
  Multi-warehouse: Datafold, Whaly, BigEye
  Generic FinOps: AWS Cost Explorer, GCP Cost, Vantage, CloudZero
WHAT TO TRACK:
   Daily credit/slot/RPU/DBU usage
   Top-N queries by cost
   Cost per workload / team / project
   Anomaly detection (week-over-week jumps)
   Storage growth by table
```

#### 23.4 Cost Alerts & Anomaly Detection

Cloud Warehouses

```
SET HARD LIMITS:
  Snowflake: Resource Monitor with notify + suspend thresholds
    CREATE RESOURCE MONITOR rm_etl
      WITH CREDIT_QUOTA = 1000
      TRIGGERS
        ON 75 PERCENT DO NOTIFY
        ON 90 PERCENT DO SUSPEND
        ON 100 PERCENT DO SUSPEND_IMMEDIATE;
  BigQuery: query quotas per user/project
  Redshift: WLM rules + CloudWatch alarms
  Databricks: cluster policies with cost caps
ANOMALY DETECTION:
  Alert if today's cost > 1.5x rolling 7-day average
  Alert if any single query > 100 credits / 1000 slot-hours / etc.
  Alert if storage growth > N% in a day
```

```
23.5 The Quarterly Cost Review
EVERY QUARTER:
1. WAREHOUSE INVENTORY
    Each warehouse: still needed? right-sized? owned by whom?
    Suspend / delete unused
2. TOP-N QUERIES BY COST
    Are they optimized?
    Can they be cached / materialized?
    Are they critical?
3. STORAGE AUDIT
    Largest tables: still needed?
    Time travel retention appropriate?
    Cold tables → cheaper tier (BigQuery long-term, S3 IA, etc.)
4. RESERVED USAGE MATCHING
    Are reservations utilized?
    Should we increase / decrease?
5. PER-TEAM COST REVIEW
    Show each team their bill
    Set quarterly cost targets
    Celebrate wins
■ PRINCIPAL MINDSET: Warehouse cost optimization is staff-level work. Most teams treat it as 'someone else's
problem' until the bill triples. The discipline: cost is a first-class engineering metric, monitored daily, reviewed
quarterly, owned by someone. The teams that do this well save 30-50% routinely without sacrificing performance.
The teams that don't see their bills compound.
```

Cloud Warehouses

#### 24. Migration Patterns

```
24.1 Why Teams Migrate
 COST: another warehouse projects cheaper for their workload
 PERFORMANCE: current can't keep up
 ECOSYSTEM: moving to a different cloud
 FEATURES: need something the current lacks
 VENDOR CONFIDENCE: company decisions, pricing changes
 CONSOLIDATION: M&A → multiple warehouses → consolidate
MIGRATION IS EXPENSIVE.
Budget 6-12 months for a large migration; expect surprises.
```

```
24.2 The Standard Migration Phases
PHASE 1: ASSESS (2-4 weeks)
   Inventory: tables, views, schedules, dashboards, users
   Identify SQL incompatibilities
   Estimate cost in target warehouse
   Identify low-risk pilots
PHASE 2: DUAL-RUN INFRASTRUCTURE (2-4 weeks)
   Set up target warehouse
   Connect ETL to write both warehouses
   Migrate dbt project (adapter change usually small)
   CI / monitoring in place
PHASE 3: DATA PARITY (4-8 weeks)
   Reconcile row counts, sums, distincts
   Fix SQL incompatibilities iteratively
   Validate critical KPIs match
PHASE 4: CONSUMER MIGRATION (8-16 weeks)
   Migrate dashboards one at a time
   Migrate analyst access
   Run both in parallel during transition
PHASE 5: CUTOVER & DECOMMISSION (4 weeks)
   Cut writes; only target warehouse receives data
   Sunset old warehouse
   Hold for 60-90 days as safety net
   Decommission for real
```

```
24.3 SQL Translation Pain Points
COMMON INCOMPATIBILITIES:
DATE / TIMESTAMP FUNCTIONS:
  Snowflake DATEADD(day, n, ts)
  BigQuery DATE_ADD(ts, INTERVAL n DAY)
  Redshift DATEADD(day, n, ts)
  Databricks DATE_ADD(ts, n) -- order varies!
WINDOW FUNCTIONS:
  Mostly ANSI standard; subtle differences in RANGE/ROWS semantics
JSON FUNCTIONS:
  Each has its own JSON_EXTRACT / json_extract / GET_PATH / etc.
SEMI-STRUCTURED TYPES:
  Snowflake VARIANT
  BigQuery JSON or STRUCT
  Redshift SUPER
  Databricks MAP / STRUCT
USER-DEFINED FUNCTIONS:
  Different languages supported, syntax varies
  May need rewriting
```

Cloud Warehouses

```
STORED PROCEDURES:
  Significant rewrite usually; languages differ
```

```
24.4 Migration Tools
COMMERCIAL:
   Datafold's Data Diff (reconciliation)
   Compilerworks (SQL translation)
   CHAOSSEARCH, Polynom, EvenUp (BQ/Snowflake/Redshift translation)
   SQLGlot (open-source SQL translation; widely used)
DBT MAKES MIGRATION EASIER:
   Models in dbt are warehouse-agnostic templates
   Change adapter, fix engine-specific SQL, re-run
   dbt's audit_helper package for reconciliation
ANTI-PATTERN: hand-translating every model
  Use SQLGlot or similar for 80% automated translation
  Hand-fix the 20% that doesn't translate cleanly
```

```
24.5 The Migration Anti-Patterns
■ BIG-BANG WEEKEND CUTOVER
   Always parallel-run. Always.
■ MIGRATING WITHOUT RECONCILIATION
   Row counts and sums must match. Trust = burned otherwise.
■ DOWN-PRIORITIZING SQL TRANSLATION
   "It's just SQL, it'll work" — until it doesn't.
■ UNDERESTIMATING ANALYST DISRUPTION
   Analysts have muscle memory for dashboards / table names.
   Train, communicate, migrate slowly.
■ TURNING OFF OLD WAREHOUSE TOO SOON
   60-90 day overlap is cheap insurance.
■ NO ROLLBACK PLAN
   Always know how to fall back to the old warehouse.
```

Cloud Warehouses

#### 25. Security, Compliance & Governance

```
25.1 The Security Layers
1. NETWORK
    VPC / Private Link / IP allow-listing
    Snowflake: PrivateLink, network policies
    BigQuery: VPC Service Controls
    Redshift: VPC + Enhanced VPC Routing
    Databricks: Private Cluster Routing, IP allow-list
2. AUTHENTICATION
    SSO via SAML / OAuth / OIDC
    Service accounts / IAM for app access
    MFA mandatory for users
3. AUTHORIZATION (RBAC)
    Roles / groups → permissions
    Snowflake: roles + grants
    BigQuery: IAM roles
    Redshift: groups + grants
    Databricks: Unity Catalog grants
4. DATA-LEVEL CONTROLS
    Column masking (PII)
    Row-level security (multi-tenant filtering)
    Dynamic data masking
5. ENCRYPTION
    At rest: AES-256 standard
    In transit: TLS 1.2+
    Customer-managed keys (CMK) for high-compliance
6. AUDIT
    Login history, query history, access logs
    All four warehouses have rich audit logs
```

```
25.2 Column Masking & Row-Level Security
SNOWFLAKE:
  CREATE MASKING POLICY email_mask AS (val STRING) RETURNS STRING ->
    CASE WHEN CURRENT_ROLE() IN ('PII_VIEWER') THEN val
         ELSE '****@****.com' END;
```

```
  ALTER TABLE users MODIFY COLUMN email SET MASKING POLICY email_mask;
BIGQUERY:
  Column masking via data taxonomy + policy tags
  Row-level security via row access policies:
    CREATE OR REPLACE ROW ACCESS POLICY filter_by_region
      ON sales_table GRANT TO ('user:bob@example.com')
      FILTER USING (region = 'US');
REDSHIFT:
  Row-level security via row access policies (newer feature)
  Column-level encryption
DATABRICKS:
  Column masks + row filters via Unity Catalog
  Dynamic masking based on group membership
```

```
25.3 Compliance
STANDARDS SUPPORTED (most warehouses):
  SOC 2 Type II
  ISO 27001, 27017, 27018
  HIPAA
  PCI DSS
  FedRAMP (Snowflake, BigQuery, AWS gov)
  GDPR
```

Cloud Warehouses

```
GDPR / DATA RESIDENCY:
  Snowflake: choose region; data stays
  BigQuery: dataset region setting
  Redshift: cluster region
  Databricks: workspace region
RIGHT-TO-DELETE (GDPR):
  Hard: warehouses are append-optimized
  Solutions:
    Snowflake: DELETE on Time Travel; FAILSAFE cannot be removed early
               (this is a known GDPR challenge)
    BigQuery: DELETE works; physical removal eventually
    Lakehouse: Delta Lake VACUUM with RETAIN 0 (cautious!)
```

```
25.4 The Governance Layer
DATA CATALOG (Unity Catalog, Polaris, Glue, custom):
   Searchable inventory of tables
   Permissions managed centrally
   Lineage tracking
   Tagging (PII, sensitivity, owner)
ROLE OF DBT IN GOVERNANCE:
   exposures: tracking downstream consumers
   contracts: enforced schemas at gold layer
   docs: tables documented for discoverability
   dbt-checkpoint: enforce governance via CI
CONTINUOUS QUALITY:
  Tests + monitoring = trustworthy data
  See dbt notes for the full quality strategy
■ PRINCIPAL MINDSET: Security and governance are not afterthoughts; they're product features. At staff level
you're expected to defend network controls, RBAC, masking policies, audit, and right-to-delete patterns. The
warehouse provides the tools; your team provides the operational discipline. Compliance audits are won or lost
on this discipline, not on warehouse choice.
```

Cloud Warehouses

## PART VIII — INTERVIEW PRACTICE

#### 26. The Staff-Level Warehouse Interview

  - 26.1 Question Categories
- 1. ARCHITECTURE — "Explain how Snowflake's storage/compute separation works"
- 2. SELECTION   — "Why Snowflake vs BigQuery for this workload?"
- 3. PERFORMANCE — "Query is slow; debug and fix"
- 4. COST        — "Bill jumped 3x; what do you investigate?"
- 5. SCALE       — "100 TB table, queries on user_id are slow; what do you do?"
- 6. MIGRATION   — "Migrate from Redshift to Snowflake without downtime"
- 7. GOVERNANCE  — "PII column masking + row-level security at scale"
- 8. ANTI-USE    — "When would you NOT use a warehouse?"

```
26.2 The Selection Framework
STEP 1: CLARIFY REQUIREMENTS
   Workload shape (steady / bursty / sporadic)
   Volume (active TB, total TB, growth rate)
   Concurrency (peak users / queries)
   Latency SLO (sub-second BI? 1-min ETL? batch?)
   Cloud commitments
   Budget constraints
STEP 2: ENUMERATE CANDIDATES
  Snowflake / BigQuery / Redshift / Databricks SQL
  + lakehouse-only architectures
  + warehouse + lakehouse hybrid
STEP 3: SCORE ON AXES
   Performance fit (workload shape)
   Cost projection (realistic, with growth)
   Ecosystem fit (BI tools, ML stack, existing investments)
   Operational simplicity
   Vendor lock-in vs portability
   Team familiarity
STEP 4: PILOT
   Pick 2 finalists
   Run representative workload on each for 30 days
   Measure: performance, cost, operational toil
   Decide based on data, not pitch decks
STEP 5: COMMIT WITH OFF-RAMPS
   Use open formats (Iceberg) where possible
   Avoid deep vendor-specific features unless necessary
   Document migration path even if not planning one
```

26.3 Worked Example: Architect Warehouse for 200-person Startup INTERVIEWER: "Series C startup, 200 engineers, 50 analysts. On AWS. Need analytics warehouse from scratch. Recommend." STAFF RESPONSE: "Clarifying: budget signal? data sources? ML/Python needs? compliance? Future cloud strategy? Assumed: $100-300K/year budget, ~10 SaaS + 5 internal DB sources, some ML feature engineering, moderate compliance (SOC 2, no FedRAMP). RECOMMENDATION: Snowflake. REASONING:

Cloud Warehouses

```
    AWS workload but Snowflake-on-AWS doesn't lock to AWS forever
    50 analysts → Snowflake's UX is best-in-class for SQL users
    $100-300K budget aligns with Snowflake's per-second + smart auto-suspend
    Auto-suspend means dev/test costs are minimal
    Multi-warehouse isolation lets ETL/BI/ad-hoc separate cleanly
    Snowpark for the Python/ML side
    Zero-copy clones for dev environments
 ALTERNATIVE CONSIDERED:
    Redshift: cheaper at steady-state with reserved instances, but
     more ops overhead; analyst UX weaker
    BigQuery: would require cloud migration; not justified
    Databricks SQL: makes sense if ML/lakehouse is dominant; assumption
     is BI-dominant here
 ARCHITECTURE:
   Storage: Snowflake-managed (proprietary FDN initially)
            Plan migration to external Iceberg in 2-3 years
            (Snowflake supports both; future-proof)
   Catalog: native Snowflake account; revisit if multi-engine needs emerge
   Compute:
     ETL_WH: Large, scheduled hours (4-6/day), auto-suspend
     BI_WH:  Medium with multi-cluster (1-3), 15-min auto-suspend
     ADHOC_WH: Small, 60-sec auto-suspend
     ML_WH: XL, on-demand for feature engineering
   Modeling: dbt-snowflake adapter, layered (staging → marts)
   Governance:
     SSO + MFA via Okta
     PII tags + masking policies on sensitive columns
     Resource monitors with $-threshold alerts
     Daily cost dashboards via SELECT.dev or similar
 SCALE PATH:
   100 GB → 10 TB → 100 TB → 1 PB
    At 10 TB: introduce CLUSTER BY on largest tables
    At 100 TB: consider external Iceberg + Snowflake compute on lake
    At 1 PB: serious architecture review; storage strategy critical
 OFF-RAMPS:
    dbt portability: SQL adapter swap
    External tables: data lives in our S3, queryable from elsewhere
    Migration cost realistic at 2-3 years out: ~3 months effort"
WHAT MAKES THIS STAFF-LEVEL:
   Asked clarifying questions
   Considered alternatives explicitly
   Picked based on workload + team, not hype
   Architecture spanning storage, catalog, compute, modeling, governance
   Acknowledged scale-path and off-ramps
```

Cloud Warehouses

27. War Stories ■ PRODUCTION SCENARIO: The Always-On Warehouse. A team had AUTO_SUSPEND set to NEVER on their main Snowflake warehouse (Large size). They paid ~$15K/month for compute that was actively used ~6 hours/day. Setting auto-suspend to 60 seconds dropped the bill to ~$4K/month. The 'reason' for never-suspend was 'so the cache stays warm' — but the team's actual usage pattern didn't benefit from constant warmth. Always question always-on warehouses; the default should be aggressive auto-suspend with exceptions justified. ■ PRODUCTION SCENARIO: The BigQuery SELECT * Disaster. An analyst wrote a Looker dashboard that did SELECT * on a 50 TB events table filtered by user_id (no partition predicate). The query scanned 50 TB. It ran every 5 minutes from auto-refresh. At $6.25/TB on-demand, that's $312/run × 288 runs/day × 30 days = $2.7M/month projected. Caught on day 3 by anomaly alert; fixed with partition filter + LIMIT. Lesson: on-demand BigQuery needs query-level cost monitoring and review of every dashboard query before production. ■ PRODUCTION SCENARIO: The Redshift DISTKEY Tragedy. A team had a 10TB fact_orders table with DISTKEY on a low-cardinality status field. 80% of rows had status = 'completed'. One node held 80% of the table. Queries were slow (skewed load) and node disk filled to 95%. Redistributing to a high-cardinality user_id DISTKEY took 8 hours but rebalanced everything. Lesson: DISTKEY must be high-cardinality AND match join patterns. Check skew via SVV_DISKUSAGE. ■ PRODUCTION SCENARIO: The Time Travel Storage Explosion. A team's Snowflake bill grew by $5K/month over a year with no obvious cause. Active storage was 50 TB. Investigation: Time Travel + Fail-safe storage totaled 600 TB — they had high-churn tables with 90-day retention. Setting retention to 7 days on high-churn tables cut storage by 90%. Lesson: Time Travel retention defaults can be a slow money leak; tune per-table based on actual recovery needs. ■ PRODUCTION SCENARIO: The Concurrent Cluster Cascade. A Redshift cluster hit concurrency limits during morning dashboard refresh; Concurrency Scaling kicked in and added 8 clusters. The CS cluster cost ~$30 for the 90-minute event. This happened daily. Annual cost: ~$11K from a single configurable. Switching from one massive scheduled refresh to staggered refreshes across 30 minutes eliminated the CS firing. Pattern matters as much as warehouse choice. ■ PRODUCTION SCENARIO: The Migration That Didn't Reconcile. A team migrated from Redshift to Snowflake over 4 months. Cutover went smoothly. Two months later, finance noticed revenue reports were 4% lower than the legacy Redshift numbers. Investigation: a date-rounding difference in a quarterly aggregation. The team had migrated 200 dbt models without row-level reconciliation. Trust destroyed for 6 months. Lesson: NEVER cutover without dbt-audit-helper or equivalent row-by-row reconciliation of critical metrics.

Cloud Warehouses

#### 28. Cheat Sheet & Mental Models

```
28.1 The Six Mental Models
1. STORAGE / COMPUTE SEPARATION IS THE FOUNDATION
   Everything modern flows from this — auto-suspend, zero-copy clones,
   multi-cluster, time travel, lakehouse interop.
2. COLUMNAR + COMPRESSION + STATS = FAST SCANS
   The universal warehouse magic: skip data you don't need, fast.
   Optimization at every layer: partition, file, row group, page.
3. PRUNING IS THE BIGGEST PERFORMANCE LEVER
   Partition keys, clustering, sort keys, dist keys — all reduce data read.
   Get these right; everything else is secondary.
4. PER-SECOND BILLING CHANGES OPERATIONS
   Auto-suspend mindset, right-size per workload, cost is engineering metric.
   Old habits (size for peak, run 24/7) leak money.
5. CACHING MATTERS MORE THAN YOU THINK
   Result cache, SSD cache, BI Engine — all bypass compute entirely.
   Design queries and dashboards to maximize hit rate.
6. THE FORMAT/CATALOG/COMPUTE TRIANGLE
   Pick storage carefully (10-year decision).
   Pick catalog carefully (governance core).
   Pick compute flexibly (re-evaluate every 2-3 years).
```

```
28.2 The One-Page Reference
SNOWFLAKE
  Compute: virtual warehouses (XS-6XL), per-second
  Storage: ~$23/TB/month
  Cost lever: AUTO_SUSPEND every warehouse
  Tuning: CLUSTER BY, materialized views, SOS
  Best for: multi-cloud, analyst-heavy, sporadic
BIGQUERY
  Compute: slots (on-demand $6.25/TB, capacity $40-60/slot-hour)
  Storage: ~$20/TB/month
  Cost lever: partition + cluster + BI Engine
  Tuning: APPROX_*, materialized views, partition pruning
  Best for: all-GCP, sporadic, ML-friendly
REDSHIFT
  Compute: provisioned RA3 nodes or Serverless RPUs
  Storage: ~$24/TB/month (RMS)
  Cost lever: reserved instances, WLM
  Tuning: DISTKEY, SORTKEY, vacuum, analyze
  Best for: AWS-deep, steady 24/7
DATABRICKS SQL
  Compute: SQL warehouses (2XS-4XL), per-second DBU
  Storage: cloud bucket (~$23/TB on S3)
  Cost lever: auto-stop, Photon, Predictive Optimization
  Tuning: OPTIMIZE, Z-ORDER, Liquid Clustering
  Best for: lakehouse-native, Spark+SQL+ML
UNIVERSAL TUNING:
   Partition + cluster large tables
   Project only needed columns
   SARGable predicates
   Materialized views for repeats
   Right-size compute per workload
   Auto-suspend / auto-stop everything
```

#### 28.3 What Separates Senior from Staff

Cloud Warehouses

```
SENIOR                                STAFF / PRINCIPAL
■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■
"Use Snowflake"                      "Snowflake for our workload because:
                                      multi-cloud, analyst-heavy, sporadic
                                      with auto-suspend savings; here are
                                      the comparison numbers vs alternatives"
"Add a cluster key"                  "CLUSTER BY (event_date, user_id);
                                      reclustering cost ~$2K/month; saves
                                      30 sec × 200 analysts × ... per day;
                                      ROI in 2 weeks"
"It's slow because of data size"     "EXPLAIN shows full scan; partition
                                      column isn't in WHERE; add filter or
                                      reshape query; if data growth pattern
                                      continues, time to partition by week"
"BigQuery is more cost-effective"     "On our workload: BigQuery on-demand
                                      would be cheaper IF we discipline
                                      queries; with current patterns it'd
                                      be 2x; we'd need 3 months of query
                                      optimization first to make migration
                                      cost-positive"
"Use auto-WLM"                       "Auto-WLM with two override queues:
                                      one for ad-hoc with strict timeout
                                      (kill > 5 min) to prevent runaways;
                                      one for ETL with priority boost during
                                      ingestion window"
■ PRINCIPAL MINDSET: Cloud warehouse expertise at staff level is a triple: deep understanding of one
warehouse, broad understanding of the others, and operational discipline around cost. The technical capabilities
have converged; the differentiator is which one fits your organization and how well you operate it. The best staff
engineers I know can defend their warehouse choice in three sentences and tell you what would change their
mind.
 — End of Cloud Warehouses Notes —
 Next in series: Data System Design (Capstone)
```


---

[← Back to main](./README.md)
