# Topic 24 — Real-Time OLAP Engines

> Standalone deep-dive · ClickHouse, Pinot, Druid, StarRocks, Doris

[← Back to main](./README.md)

---

## How to use this document

Real-time OLAP (RT-OLAP) engines serve a different problem than cloud warehouses: **user-facing analytics with sub-second latency and millions of concurrent queries**. If an interview asks "design metrics for an in-product dashboard," the right answer is often an RT-OLAP engine, not Snowflake. This chapter covers the five dominant engines and the architectural patterns.

> **🧠 Principal Mindset:** Warehouses optimize for analyst productivity on arbitrary SQL. RT-OLAP engines optimize for pre-defined query shapes at scale. They are not competitors; they are different layers in a mature data stack. Staff engineers argue when each wins and when they belong together.

## Table of Contents

**PART I — FOUNDATIONS**
- 1. What Real-Time OLAP Is (and Isn't)
- 2. Why Warehouses Fall Short
- 3. The Five Dominant Engines in 2026
- 4. Architectural Patterns

**PART II — CLICKHOUSE**
- 5. ClickHouse Architecture
- 6. Table Engines (MergeTree Family)
- 7. ClickHouse at Scale
- 8. ClickHouse Cloud and Shared Storage

**PART III — APACHE PINOT**
- 9. Pinot Architecture
- 10. Pinot Table Types (Realtime, Offline, Hybrid)
- 11. Pinot Indexing + Star-Tree
- 12. Pinot Operations

**PART IV — APACHE DRUID**
- 13. Druid Architecture
- 14. Druid Segments
- 15. Druid Query Model
- 16. Druid vs Pinot

**PART V — STARROCKS AND DORIS**
- 17. StarRocks Architecture
- 18. Apache Doris
- 19. Shared-Data vs Shared-Nothing
- 20. StarRocks on the Lakehouse

**PART VI — COMPARISON AND DECISION**
- 21. Feature Comparison
- 22. Latency, Throughput, Concurrency
- 23. When Each Engine Wins

**PART VII — ARCHITECTURE PATTERNS**
- 24. Kafka → RT-OLAP → Serving API
- 25. Lambda / Kappa for RT-OLAP
- 26. Multi-Tier Serving

**PART VIII — INTERVIEW PRACTICE**
- 27. The Staff-Level RT-OLAP Interview
- 28. Worked System Design
- 29. War Stories
- 30. Cheat Sheet &amp; Mental Models

---

## PART I — FOUNDATIONS

### 1. What Real-Time OLAP Is (and Isn't)

```
REAL-TIME OLAP = analytical queries with:
   • Sub-second P99 latency
   • Thousands to millions of concurrent queries
   • Freshness measured in seconds (not minutes/hours)
   • Pre-defined query shapes (filter + aggregate on known dimensions)

USE CASES:
   • User-facing analytics dashboards in SaaS products
   • Real-time personalization + segmentation
   • Observability platforms (log analytics, metrics explore)
   • Ad tech reporting (campaign metrics in advertiser portals)
   • Fraud detection scoring
   • Monitoring and alerting at high cardinality

NOT REAL-TIME OLAP:
   • Ad-hoc analyst exploration (Snowflake / BigQuery better)
   • Complex multi-join SQL on raw data (warehouse territory)
   • Large-table scans without pre-aggregation (lake + Trino)
   • ML training queries (lakehouse / warehouse)
```

### 2. Why Warehouses Fall Short

```
SNOWFLAKE / BIGQUERY / REDSHIFT: optimized for
   ANALYST workloads: arbitrary SQL, large-but-infrequent queries,
      elastic compute, cost per query (not per read).
   
   They WOULD work for user-facing analytics except:
      • Startup latency: warehouses have cold-start overhead (seconds)
      • Concurrency: multi-cluster or query acceleration costs scale
      • Cost: per-query compute × thousands of users = expensive
      • Freshness: micro-batch ingest; seconds-to-minute latency
      • Query planning overhead: SQL parse/plan tax per query

RT-OLAP engines sacrifice generality for:
   • Memory-resident indexes
   • Pre-computed aggregations (star-tree indexes, materialized views)
   • Highly concurrent read paths
   • Streaming ingest with second-level latency
   • Per-query cost near zero

STAFF SIGNAL: "we embed a user-facing dashboard in our product" is a
   RT-OLAP question. "we have 50 analysts running SQL all day" is a
   warehouse question. They solve different problems.
```

### 3. The Five Dominant Engines in 2026

```
   ENGINE       | ORIGIN           | STRENGTH
   -------------+------------------+-------------------------
   ClickHouse   | Yandex 2016      | SQL-friendly, broad adoption
   Apache Pinot | LinkedIn 2014    | User-facing, high QPS, star-tree
   Apache Druid | Metamarkets 2012 | Time-series, OG real-time OLAP
   StarRocks    | DorisDB fork 2020| MPP + lakehouse-native (Iceberg)
   Apache Doris | Baidu Palo 2017  | China-strong; MPP; MySQL protocol

Also notable:
   Rockset (acquired by OpenAI 2024, product shut down)
   Timescale (PostgreSQL extension; different niche)
   InfluxDB (time-series specialist)
   Materialize (streaming SQL; different niche)
   Tinybird (ClickHouse SaaS; developer-focused)
   Snowflake Hybrid Tables (adjacent niche; see 21)
```

### 4. Architectural Patterns

```
RT-OLAP in a modern stack:

   SOURCES (apps, services, databases)
       ↓
   STREAMING LAYER (Kafka / Pulsar / Kinesis)
       ↓
   RT-OLAP ENGINE (ClickHouse / Pinot / Druid / StarRocks)
       ↓
   SERVING LAYER (API, dashboard, in-product analytics)

Lake / warehouse coexist for:
   • Historical / analyst queries
   • ML training
   • Compliance / archive
   
PATTERN: ingest to BOTH:
   Kafka → RT-OLAP (hot, 90-180 day retention)
   Kafka → Lake (cold, all history)
```

---

## PART II — CLICKHOUSE

### 5. ClickHouse Architecture

```
CLICKHOUSE: columnar DBMS, C++, single-binary, embedded or distributed.

   Columnar storage with per-column compression (LZ4 default, ZSTD optional)
   Vectorized execution engine
   SQL dialect (ClickHouse SQL — ANSI-ish with extensions)
   Primary key = sort key, not UNIQUE constraint (!)

DEPLOYMENT MODES:
   SINGLE NODE: embedded or standalone
   DISTRIBUTED: shards + replicas via ReplicatedMergeTree + Distributed table
   CLICKHOUSE CLOUD (and ClickHouse Enterprise): managed with shared storage

PROTOCOL:
   Native (binary) — fastest
   HTTP (JSON/CSV) — easy to use
   MySQL / Postgres wire protocols — compatibility
```

### 6. Table Engines (MergeTree Family)

```
MergeTree ENGINES (ClickHouse's core abstraction):

   MergeTree: base engine; sort by primary key; background merges
   ReplicatedMergeTree: distributed replicas via ZooKeeper / Keeper
   SummingMergeTree: pre-aggregates numeric columns on merge
   AggregatingMergeTree: pre-aggregates with any AggregateFunction
   ReplacingMergeTree: deduplicates on primary key via LATEST value
   CollapsingMergeTree: + / - signs cancel out old versions
   VersionedCollapsingMergeTree: + version column for correctness
   GraphiteMergeTree: optimized for Graphite metric ingestion

PATTERN:
   Base facts: MergeTree (or Replicated)
   Rollups: AggregatingMergeTree with materialized views
   Latest-value tables: ReplacingMergeTree
```

#### 6.1 Materialized Views in ClickHouse

```
A MATERIALIZED VIEW in ClickHouse is an INGESTION TRIGGER:
   INSERT into source table → MV computes transformation → INSERT into target

   CREATE MATERIALIZED VIEW hourly_user_events
   ENGINE = AggregatingMergeTree
   ORDER BY (user_id, hour)
   POPULATE
   AS SELECT 
       user_id,
       toStartOfHour(ts) AS hour,
       countState() AS event_count,
       uniqState(session_id) AS unique_sessions
   FROM raw_events
   GROUP BY user_id, hour;

QUERY uses the aggregated table:
   SELECT user_id, hour, countMerge(event_count) FROM hourly_user_events
   WHERE hour >= today() - 7 GROUP BY user_id, hour;

This pattern pre-aggregates during ingestion for sub-ms query latency.
```

### 7. ClickHouse at Scale

```
SINGLE-NODE: ClickHouse scales REMARKABLY on one machine.
   Petabyte+ datasets on single big machines (yes, really)
   Massively vectorized; cache-friendly

DISTRIBUTED: sharding + replicas
   Shards: horizontal partitioning (manual sharding key)
   Replicas: redundancy + read scaling

KEEPER (ClickHouse's ZooKeeper replacement, 2022+):
   Metadata coordination for replicas + distributed DDL
   Simpler ops than ZooKeeper

CONCURRENCY: thousands of QPS per node for simple queries;
   hundreds for complex. Scale via replicas.
```

### 8. ClickHouse Cloud and Shared Storage

```
CLICKHOUSE CLOUD (2022+ managed):
   Shared storage: S3 as the storage layer (not local disk)
   Separation of storage + compute
   Autoscaling compute (serverless)
   Pay for storage + compute separately

Open-source equivalent: SharedMergeTree engine
   Available in ClickHouse Enterprise
   Allows self-hosted shared-storage ClickHouse

STAFF SIGNAL: ClickHouse's move to shared storage mirrors Snowflake's
   architecture. Pre-Cloud ClickHouse was shared-nothing (local disk);
   post-Cloud is shared-storage (S3). This is a major architectural
   shift that affects cost + scaling patterns.
```

---

## PART III — APACHE PINOT

### 9. Pinot Architecture

```
APACHE PINOT: distributed real-time OLAP, originated at LinkedIn.

COMPONENTS:
   CONTROLLER: cluster metadata, segment assignment (uses Helix + ZK)
   BROKER: query routing + merge (stateless)
   SERVER: holds segments, executes queries
   MINION: offline tasks (compaction, backfill)
   DEEP STORE: segment persistence (S3, HDFS, GCS)

KEY IDEA: SEGMENTS
   Immutable, columnar, pre-indexed units of data
   Stored on servers; mirrored to deep store
   Query routing: broker finds servers holding relevant segments
```

### 10. Pinot Table Types (Realtime, Offline, Hybrid)

```
OFFLINE TABLE:
   Batch-ingested segments (Spark job writes to deep store)
   Immutable; no updates
   Used for historical data

REALTIME TABLE:
   Streams from Kafka (or Pulsar / Kinesis)
   Segments start in-memory; flush to disk/deep store as sealed
   Serves fresh data (<1 min latency)

HYBRID TABLE:
   Combines realtime + offline for ONE logical table
   Realtime for recent (last N days)
   Offline for historical (bulk-loaded from batch pipeline)
   Query time: broker routes to realtime OR offline servers based on time range

STAFF SIGNAL: hybrid tables are the standard pattern. Realtime for
   hot data; offline backed by Spark reprocessing for correctness
   and bulk history. Mimics lambda architecture at the engine level.
```

### 11. Pinot Indexing + Star-Tree

```
PINOT INDEXES (per column):
   Forward (default): stored column values
   Inverted: value → row list (fast equality filters)
   Range: for range filters on sorted-ish columns
   Bloom: approximate membership (filter out non-matches cheaply)
   Text (Lucene): full-text search within a column
   JSON: index JSON fields
   StarTree: pre-aggregated indexes for GROUP BY
   Geo: spatial queries
   Vector: approximate nearest neighbor (2024+)

STAR-TREE INDEX:
   Pre-materialized aggregation hierarchy
   For queries that GROUP BY on known dimensions, lookup time ~ constant
   Space-time trade-off: segments larger, queries faster

STAFF SIGNAL: star-tree is Pinot's signature feature. If an interview
   asks "how do you get sub-ms latency on 1B-row GROUP BY", star-tree
   is a correct answer: pre-materialize the aggregation hierarchy during
   segment build; query hits the pre-computed value, not the raw rows.
```

### 12. Pinot Operations

```
PINOT STRENGTHS:
   • Highest-concurrency engine of the five (10K+ QPS at linkedin scale)
   • Lucene-based text + geo
   • Star-tree for GROUP BY performance
   • Upserts via partial-record primary key tables

PINOT WEAKNESSES:
   • SQL dialect limited (no complex joins; growing in 2024-2026)
   • Operational complexity (Helix + ZK)
   • Learning curve
   • Smaller community than ClickHouse

PINOT MANAGED SERVICES:
   StarTree Cloud (founded by Pinot creators)
   Confluent integration (streaming to Pinot via Kafka)
```

---

## PART IV — APACHE DRUID

### 13. Druid Architecture

```
APACHE DRUID: original real-time OLAP; time-series oriented.

COMPONENT ROLES:
   MASTER NODES:
      Coordinator (segment management)
      Overlord (task management)
   
   QUERY NODES:
      Broker (query routing)
      Router (routing to brokers; multi-tier queries)
   
   DATA NODES:
      Historical (immutable segment serving)
      MiddleManager (ingestion + realtime segment serving)
      Indexer (newer; replaces MM for some workloads)
   
   DEEP STORE: segment persistence (S3/HDFS)
   METADATA STORE: Postgres/MySQL (segments, task state)

KEY IDEA: TIME-PARTITIONED SEGMENTS
   Every table has a timestamp dimension
   Segments are chunks of time (hour, day typically)
   Enables time-based pruning + rollup
```

### 14. Druid Segments

```
Druid segments are:
   Columnar
   Per-column dictionary encoded + compressed
   Immutable once sealed
   Named by: datasource + time interval + version + partition

INGESTION:
   Realtime: streaming ingest with rollup during ingestion
   Batch: Hadoop or native batch (Spark-like) tasks
   
ROLLUP:
   Aggregation performed DURING ingestion
   Trade data granularity for storage + query speed
   
   Example: 1 billion raw events → 10 million rows after rollup
   (grouped by minute, user_id, country, event_type)
```

### 15. Druid Query Model

```
QUERY TYPES (native JSON):
   Timeseries: time-bucket aggregates (count, sum, etc.)
   TopN: top-N per time bucket (fastest for ranking)
   GroupBy: generic group-by (most flexible, slowest)
   Select / Scan: row-level
   Search: value search in a dimension
   
   SQL layer (via Druid SQL) translates to native queries.

APPROXIMATION:
   Druid pioneered HLL / theta sketches for distinct counts at scale
   Approximate percentiles via quantiles sketches
   Trade accuracy for speed on cardinality-heavy workloads

STAFF SIGNAL: Druid's design assumes you query along time + known dimensions.
   Great for time-series dashboards; weak for exploratory ad-hoc.
```

### 16. Druid vs Pinot

```
These engines are architecturally similar; Pinot was partly inspired by Druid.

   DIMENSION           DRUID              PINOT
   -------------------+------------------+------------------
   Time partitioning   Mandatory          Optional
   Query language      Native JSON + SQL  SQL (growing complete)
   Indexes             Limited            Rich (inverted, text, geo, star-tree)
   Rollup              At ingestion       Via star-tree + schema design
   Upserts             Weak               Supported (primary key tables)
   Deployment          More components    Similar complexity
   Community           Larger             Growing fast
   Managed             Imply, AWS managed Pinot preview, StarTree

CHOICE:
   Pure time-series dashboards: Druid (mature, understood)
   User-facing + high concurrency: Pinot (star-tree + upserts)
   Both work; team familiarity + ecosystem often decides
```

---

## PART V — STARROCKS AND DORIS

### 17. StarRocks Architecture

```
STARROCKS: MPP columnar engine (fork of Apache Doris; now its own).

COMPONENTS:
   FE (Frontend): coordinator, metadata, query planner (Java)
   BE (Backend): compute + storage; column store engine (C++)
   CN (Compute Node): compute-only for separation of storage/compute

SQL: MySQL-compatible protocol + strong ANSI SQL support

KEY FEATURES:
   Pipeline execution + vectorized operators
   Cost-Based Optimizer (CBO)
   Primary key tables (upserts; most engines lack this)
   Materialized views (synchronous + asynchronous)
   Multiple table types (Duplicate, Aggregate, Unique, Primary)
```

### 18. Apache Doris

```
APACHE DORIS: MPP analytical DB, originated at Baidu.

Fork relationship: StarRocks forked from Doris in 2020.
Doris continued independently; strong in China ecosystem.

Current (2026):
   Both engines are mature + production-ready
   StarRocks has slight lead on cloud/lakehouse features
   Doris has larger community in China; growing globally
   Both: MySQL protocol, columnar MPP, materialized views, PK tables
   
Choice often based on team geography + specific features.
```

### 19. Shared-Data vs Shared-Nothing

```
TRADITIONAL StarRocks: shared-nothing
   Each BE node has local storage
   Compute co-located with data
   Scale: add BE nodes
   
SHARED-DATA (2023+):
   Storage on object store (S3, OSS)
   Compute Nodes (CN) are stateless
   Separation of storage + compute
   Scale compute independently; pay per-use
   Matches Snowflake / Databricks architecture

STAFF SIGNAL: shared-data StarRocks is their answer to Snowflake-style
   elasticity. The classic shared-nothing mode is faster but less elastic.
   For bursty workloads or multi-tenant SaaS, shared-data is often better.
```

### 20. StarRocks on the Lakehouse

```
STARROCKS AS LAKEHOUSE QUERY ENGINE:
   
   Direct Iceberg / Hudi / Delta Lake / Paimon reads
   No ETL into StarRocks required
   Push-down optimization + materialized views over lake tables
   Compete with Trino for lakehouse SQL

PATTERN:
   S3 Iceberg tables (written by Spark/Flink)
   StarRocks shared-data on top
   Materialized views cache hot query shapes
   
   Serve user-facing analytics directly from the lake.

STAFF SIGNAL: this is a NEW ARCHITECTURE PATTERN in 2024-2026 —
   RT-OLAP engines as the serving layer OVER a lakehouse rather than
   a separate hot store. Reduces data duplication; simplifies 
   governance. Not yet dominant but growing.
```

---

## PART VI — COMPARISON AND DECISION

### 21. Feature Comparison

| Feature | ClickHouse | Pinot | Druid | StarRocks | Doris |
|---------|-----------|-------|-------|-----------|-------|
| Latency P99 | ~10-100ms | <100ms | <100ms | <100ms | <100ms |
| Max QPS/node | 1-5K | 10K+ | 1-5K | 5K+ | 5K+ |
| Streaming ingest | Via Kafka engine | Native | Native | Via Routine Load | Via Routine Load |
| Joins | Growing (hash joins) | Weak (growing) | Weak | Strong | Strong |
| Upserts | Via engines | PK tables | Weak | Primary key tables | Primary key tables |
| Materialized views | Yes (ingestion trigger) | Via star-tree | Rollup at ingestion | Yes (ASYNC + SYNC) | Yes |
| Lakehouse federation | Limited | Weak | Limited | Strong (Iceberg/Hudi) | Growing |
| Managed cloud | ClickHouse Cloud, Tinybird | StarTree | Imply, AWS | CelerData | Managed options |
| SQL completeness | Strong-ish | Growing | Growing | Strong | Strong |

### 22. Latency, Throughput, Concurrency

```
LATENCY targets:
   RT-OLAP target: P99 < 100ms, P50 < 10ms
   All five engines hit this with proper schema design
   
THROUGHPUT (ingest):
   Kafka → RT-OLAP: 100K-1M events/sec per table typical
   Backfill: TB/hour from object store

CONCURRENCY:
   Pinot has the strongest story (10K+ QPS per cluster)
   Druid, StarRocks: 1-5K QPS per cluster
   ClickHouse: 100-1K per node; scale via replicas

STAFF SIGNAL: concurrency is often the binding constraint for user-facing
   analytics. "100K users each loading a dashboard with 10 widgets" is
   1M QPS. Benchmark concurrency, not just single-query latency.
```

### 23. When Each Engine Wins

```
CLICKHOUSE:
   ✓ SQL-first teams; broad analytical workloads
   ✓ Dense time-series + log analytics
   ✓ Single-node simplicity for small scale
   ✓ Shared-storage (Cloud / Enterprise) for elasticity
   ✗ Not for extremely high per-user concurrency

PINOT:
   ✓ User-facing analytics with 10K+ QPS
   ✓ Pre-defined query shapes (star-tree pre-aggregation)
   ✓ Mixed hot (realtime) + cold (offline) via hybrid tables
   ✗ Not for arbitrary SQL exploration

DRUID:
   ✓ Pure time-series dashboards
   ✓ High-cardinality aggregations with approximate algorithms
   ✓ Rollup-friendly workloads
   ✗ Weak for operational mutations + complex joins

STARROCKS / DORIS:
   ✓ MySQL-protocol compatibility (BI tool friendly)
   ✓ Primary key tables (upserts first-class)
   ✓ Lakehouse federation (Iceberg direct)
   ✓ Materialized views + CBO
   ✗ Smaller global community than ClickHouse
```

---

## PART VII — ARCHITECTURE PATTERNS

### 24. Kafka → RT-OLAP → Serving API

```
STANDARD PATTERN:

   SERVICES ──→ KAFKA ──→ RT-OLAP ──→ API ──→ IN-PRODUCT UI
                   │                      
                   └──→ LAKE (cold storage)

Why Kafka in the middle:
   Multi-consumer (RT-OLAP + lake + ML)
   Replay for backfill
   Buffering + decoupling
   Schema registry contracts

Streaming ingest to RT-OLAP:
   ClickHouse: Kafka Engine + materialized view → MergeTree
   Pinot: realtime table with Kafka consumer
   Druid: Kafka indexing service
   StarRocks: Routine Load from Kafka

API layer:
   Thin layer in Go / Rust / Node
   Query RT-OLAP via SQL (or native API)
   Apply tenant/row-level security
   Cache aggressively (Redis / in-memory)
```

### 25. Lambda / Kappa for RT-OLAP

```
LAMBDA (traditional, pre-2020):
   Speed layer: RT-OLAP (fast but approximate)
   Batch layer: Spark + warehouse (slow but correct)
   Serving: merge both at query time
   
   WEAKNESS: dual implementation, dual bugs

KAPPA (streaming-only):
   Everything through streaming
   Correct state computed by stream processor + stored in RT-OLAP
   Reprocess by replaying stream from scratch

HYBRID (common in practice):
   Streaming to RT-OLAP (hot path)
   Batch to RT-OLAP via hybrid tables (bulk history)
   RT-OLAP serves both uniformly

See 04-kafka-streaming + 14-flink for streaming context.
```

### 26. Multi-Tier Serving

```
Not every query belongs in RT-OLAP; multi-tier serving matches cost + latency:

   TIER-1 (hot, sub-10ms):
      Redis / Memcached caches of pre-computed queries
      Serves most common dashboard page loads
   
   TIER-2 (warm, sub-100ms):
      RT-OLAP engine
      Serves ad-hoc filter changes, time range slides
   
   TIER-3 (cool, seconds):
      Warehouse or lake
      Serves exploratory ad-hoc analysis + ML + reports
   
   TIER-4 (cold, minutes):
      Lake / archive
      Compliance queries + bulk reprocessing

STAFF SIGNAL: strong system design answers describe the full tiering.
   Senior engineers pick ONE engine; staff engineers pick a HIERARCHY.
```

---

## PART VIII — INTERVIEW PRACTICE

### 27. The Staff-Level RT-OLAP Interview

```
COMMON QUESTIONS:
   1. "Design user-facing analytics for a SaaS product. 1M daily users."
   2. "Compare ClickHouse vs Pinot vs Druid."
   3. "Why not just use Snowflake / BigQuery?"
   4. "How do you handle upserts in a user-facing dashboard?"
   5. "Explain star-tree indexes."
   6. "Design observability analytics for 10B events/day."
   7. "How would you give 100K concurrent users sub-second dashboards?"
   8. "Lambda vs Kappa for RT-OLAP."
   9. "Multi-tenancy in RT-OLAP: how?"
   10. "RT-OLAP on top of a lakehouse — pros + cons."
```

### 28. Worked System Design

```
Q: Design user-facing analytics for a SaaS product.
   50K enterprise customers (tenants). 500M events/day.
   Each customer sees their own dashboard + raw event explorer.
   Target: P99 < 500ms. 10K concurrent dashboard loads at peak.
   Freshness: < 30 seconds for most metrics.

STAFF ANSWER:

INGESTION:
   App publishes events → Kafka (partitioned by tenant_id)
   Schema registry for Avro contracts
   Dead letter queue for schema violations
   Pre-aggregation at ingest (reduce raw volume by 10x)

STORAGE / COMPUTE:
   Apache Pinot (chosen for concurrency + star-tree + hybrid tables)
   
   PINOT SCHEMA:
      Realtime table: last 7 days from Kafka
      Offline table: 90 days historical, backfilled from S3 via Spark
      Hybrid table combines them transparently
   
   TENANT ISOLATION:
      All queries filter by tenant_id
      Row-level security at API layer (never trust client)
      Dedicated Pinot cluster for top-5 enterprise tenants
      (avoid noisy-neighbor; justify by revenue tier)

INDEXING:
   Inverted index on (tenant_id, event_type)
   Star-tree on common dashboard queries:
      (tenant_id, date, event_type) → counts, uniques
      Covers 80% of dashboard widgets
   Range index on timestamp
   Text index on free-form event properties (occasionally queried)

API LAYER:
   Go service in front of Pinot
   JWT auth + tenant extraction
   Query translation (abstract product queries to Pinot SQL)
   Redis cache: tenant-scoped, 10-second TTL for dashboard widgets

OBSERVABILITY:
   P99 latency SLO: 500ms (budget monitored)
   Query cost per tenant tracked (anti-abuse)
   Alert if P99 exceeds 1s for 5 minutes

HISTORICAL + ML:
   Same Kafka → S3 → Iceberg
   Spark + warehouse for ML training
   Lake accessible to data science; not user-facing

COST (estimated):
   Pinot cluster: 20 brokers + 60 servers + storage ≈ $80K/month
   Kafka: 20-node cluster ≈ $30K/month
   API + Redis: $5K/month
   Lake (S3 + processing): $20K/month
   Total: ~$135K/month for the user-facing analytics

WHY PINOT (NOT CLICKHOUSE / DRUID):
   • 10K concurrent QPS target → Pinot's strongest story
   • Pre-defined dashboard shapes → star-tree ideal
   • Hybrid tables for 7-day realtime + 90-day historical
   • Primary key tables for upserts (occasional backfill correction)

ALTERNATIVES CONSIDERED:
   ClickHouse: strong on cost; weaker on 10K+ QPS
   Druid: weaker on upserts + modern joins
   StarRocks: strong, but team lacks Pinot/Druid experience
   Snowflake: cost prohibitive at concurrency; freshness lags

OFF-RAMP: if Pinot doesn't scale, migration via Kafka re-ingest.
   Lake is the source of truth; Pinot is a materialization.
```

### 29. War Stories

> **🔥 Production Scenario — The Concurrency Wall.** A team put Snowflake behind user-facing dashboards. 500 concurrent users = Snowflake multi-cluster scaled to 10 clusters = $200K/month. Migrated to ClickHouse; same workload = $25K/month. Lesson: warehouses are not priced for user-facing concurrency.

> **🔥 Production Scenario — The Star-Tree Win.** A LinkedIn-style feed engagement dashboard: 20B events/day; P99 was 2s on raw aggregation. Added star-tree index on (publisher_id, date, engagement_type). P99 dropped to 15ms. Storage cost: 1.4x. Lesson: pre-aggregation at index time trades predictable space cost for predictable query speedup.

> **🔥 Production Scenario — The Hybrid Table Correction.** A Pinot hybrid table deployment: real-time was slightly ahead of offline (Kafka latency fluctuations). Dashboards showed brief dips during the transition window. Fix: configured broker to prefer realtime for the overlap window + wait for offline convergence. Lesson: hybrid tables have a real-time/offline transition that needs explicit handling.

> **🔥 Production Scenario — The Druid Rollup Loss.** A team pre-aggregated events in Druid during ingestion (hour, country, event_type). Later, product wanted queries by (hour, country, event_type, device). Raw data was gone; needed to replay from S3. 2 weeks of work. Lesson: rollup at ingestion LOSES fidelity. Only roll up dimensions you're sure will never be queried otherwise.

> **🔥 Production Scenario — The StarRocks Lakehouse Save.** A team had Delta Lake tables + Spark SQL for ad-hoc, Snowflake for BI (expensive), ClickHouse for user-facing (duplicate ingestion). Replaced ClickHouse with StarRocks reading Delta directly + materialized views on hot queries. One source of truth; cost dropped 40%. Lesson: in 2026, RT-OLAP on lakehouse is a viable architecture; evaluate it against the duplicate-storage model.

### 30. Cheat Sheet &amp; Mental Models

#### 30.1 One-Minute Pitch

```
RT-OLAP engines serve USER-FACING analytics:
   • Sub-second latency, thousands of concurrent queries
   • Streaming ingest with second-level freshness
   • Pre-defined query shapes (not arbitrary SQL)

TOP ENGINES:
   • ClickHouse: SQL-first, broad adoption, 1-5K QPS/node
   • Pinot: highest concurrency (10K+ QPS), star-tree pre-agg
   • Druid: time-series pioneer, approximate algorithms
   • StarRocks / Doris: MPP, PK tables, lakehouse-native

ARCHITECTURE: Kafka → RT-OLAP → API → UI
PATTERN: multi-tier serving (cache → RT-OLAP → warehouse → lake)

STAFF SIGNAL: argue when RT-OLAP wins vs warehouse; design
   full tiering + ingestion contracts; defend concurrency math.
```

#### 30.2 Senior vs Staff

| Senior | Staff |
|--------|-------|
| "Use ClickHouse." | "RT-OLAP for user-facing, warehouse for analyst, lake for ML. For 10K+ QPS user-facing, Pinot's star-tree wins. For SQL-first, ClickHouse. For lakehouse-native, StarRocks. Each has trade-offs — I'd benchmark the top-2 for the actual query shapes." |
| "Low latency means fast." | "Latency = planning + fetching + computing + returning. RT-OLAP wins by killing planning overhead (pre-defined shapes), fetching from indexed structures, computing on pre-aggregated data. The architecture is a specialization against generality." |
| "RT-OLAP replaces warehouse." | "They coexist. Warehouse serves analysts and ML; RT-OLAP serves users and products. Lake serves everyone over time. Multi-tier is right; one engine is wrong." |
| "Pre-aggregate everything." | "Pre-aggregate the queries you KNOW you'll run; keep raw for queries you don't. The trade-off is query speed vs query flexibility. Loss of fidelity bites months later; keep raw at low cost in the lake." |

> **🧠 Principal Mindset:** RT-OLAP is one of the clearest places where the staff signal is architectural: you're designing a specialized layer in a bigger stack, not picking an engine. "I'd use ClickHouse" is not an answer. "I'd use RT-OLAP for the serving layer, size it for 10K QPS peak, pre-aggregate the top 10 query shapes, keep the lake as source of truth for replay, and cache aggressively above it" is an answer.

---

[← Back to main](./README.md)
