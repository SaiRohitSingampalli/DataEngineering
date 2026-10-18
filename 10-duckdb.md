# Topic 10 — DuckDB

> Section 10 of the series · The in-process analytical database that made distributed systems optional for most analytical workloads

[← Back to main](./README.md)

---

## How to use this document

This is not a syntax reference. This is a principal engineer's view of DuckDB: how it works internally, when it fits and when it doesn't, how it reshapes the "big data is dead" argument, and how to reason about it alongside Spark, warehouses, and lakehouses.

Every section follows the same four lenses used throughout this series:

1. **CONCEPT** — what it is, and the computer-science / database-theory grounding
2. **MECHANICS** — how it's implemented (DuckDB's actual C++ engine)
3. **TRADE-OFFS** — when to use it, when it hurts, what alternatives exist
4. **INTERVIEW** — the staff-level question and the answer that separates seniors

> **🧠 Principal Mindset:** DuckDB is important not just as a tool but as a lens on where the data industry is heading. The question "why is DuckDB getting adopted so fast" is really the question "what did we overbuild in the distributed era, and what can we take back?" If you can defend both sides of that argument, you have the architectural judgment the staff bar is measuring.

## Table of Contents

**PART I — FOUNDATIONS**
- 1. What DuckDB Is (and Isn't)
- 2. The History: From CWI to Industry Standard
- 3. The Single-Node Renaissance
- 4. DuckDB vs The Alternatives

**PART II — SQL IN DUCKDB**
- 5. The DuckDB SQL Dialect (Friendly SQL)
- 6. Advanced SQL Features
- 7. Nested Types & Semi-Structured Data

**PART III — ENGINE INTERNALS**
- 8. Vectorized Query Execution
- 9. Morsel-Driven Parallelism
- 10. Native Storage Format
- 11. MVCC & Concurrency Model

**PART IV — READING EXTERNAL DATA**
- 12. Parquet, CSV, JSON Zero-Copy
- 13. HTTPFS, S3, GCS, Azure
- 14. Iceberg, Delta, Hudi Reading

**PART V — ECOSYSTEM & INTEGRATION**
- 15. Python Integration (pandas, polars, arrow)
- 16. The Extension System
- 17. MotherDuck & Hosted DuckDB
- 18. dbt-duckdb

**PART VI — DUCKLAKE**
- 19. The DuckLake Thesis
- 20. DuckLake Architecture
- 21. DuckLake vs Iceberg vs Delta

**PART VII — PRODUCTION PATTERNS**
- 22. Performance Tuning
- 23. Memory & Resource Management
- 24. When to Use DuckDB (and When Not)
- 25. Deployment Patterns

**PART VIII — INTERVIEW PRACTICE**
- 26. The Staff-Level DuckDB Interview
- 27. War Stories
- 28. Cheat Sheet & Mental Models

---

## PART I — FOUNDATIONS

### 1. What DuckDB Is (and Isn't)

#### 1.1 The One-Sentence Definition

DuckDB is an **in-process, columnar, vectorized OLAP SQL database** — the SQLite of analytics. It runs inside your Python / R / Node / Java / Rust / C++ process with zero network hops, zero server to manage, and performance that beats most distributed analytical engines on datasets that fit on a single machine (which, in 2026, means most workloads).

#### 1.2 The Shape of the Thing

```
Traditional analytical DB (e.g., Snowflake, BigQuery, Redshift):
   Your app  ──[network]──→  Server(s)  ──→  Storage
                 ms-s latency per query
                 Credentials, VPC, scaling, cost per query

DuckDB:
   Your app  ──[in-process library call]──→  DuckDB  ──→  Storage
                 μs latency per query
                 No server, no network, no credentials
```

DuckDB is a **library** you import, like SQLite — not a service you connect to. It is **columnar** and **vectorized**, like Snowflake / BigQuery / ClickHouse — but all three of those run on a cluster. DuckDB runs in your process.

#### 1.3 The Design Principles

```
1. SIMPLE
   One binary, 20 MB, zero dependencies.
   Install in seconds in any language runtime.

2. PORTABLE
   Linux, macOS, Windows, WebAssembly, iOS, Android.
   Same engine; same file format; everywhere.

3. FEATURE-RICH
   Full SQL. Window functions. Recursive CTEs. UDFs.
   Nested types. Advanced types. SQL/PGQ (graph queries, since 2024).

4. FAST
   Vectorized execution. Columnar storage. Morsel-driven parallelism.
   ClickBench: #1 on single-node analytical benchmarks.

5. EXTENSIBLE
   Pluggable extensions for Parquet, HTTPFS, Iceberg, S3, spatial, ...
   Load at runtime; don't pay for what you don't use.

6. FREE AND OPEN
   MIT license. No commercial strings. No lock-in.
```

#### 1.4 What DuckDB Is NOT

```
✗ A transactional database (OLTP)
  It supports ACID transactions, but it's optimized for analytics.
  → Use PostgreSQL / MySQL / SQLite for OLTP.

✗ A distributed system
  Scales within one process, not across machines.
  → Use Spark / Snowflake / BigQuery for PB-scale.

✗ A multi-user server
  No concurrent writers; multi-reader with one writer at a time.
  → Use PostgreSQL / Snowflake for high-concurrency serving.

✗ A streaming engine
  Batch-first. No continuous queries built in.
  → Use Flink / Materialize / RisingWave for streaming.

✗ A key-value store
  SQL-first; no sub-ms point lookups by key at high QPS.
  → Use Redis / DynamoDB / RocksDB.
```

> **🎯 Interview Angle:** Q: Why would you choose DuckDB over Pandas for data analysis?<br>
> **Senior:** "It's faster." <br>
> **Staff:** "Three concrete reasons: (1) DuckDB uses vectorized columnar execution — single-threaded it beats Pandas for most aggregations, and multi-threaded it beats it by 5-20x; (2) DuckDB operates out-of-core — it can process datasets larger than RAM by spilling to disk gracefully, whereas Pandas crashes; (3) DuckDB is SQL-first, which composes better across teams and is reviewable. The honest trade-off: Pandas gives you Python's full flexibility for row-level logic; DuckDB forces you into SQL semantics. For analytics they overlap heavily, but for exploratory Python-native work pandas still has its place. In 2026 I'd default to Polars + DuckDB, keeping Pandas for interop at the edges."

### 2. The History: From CWI to Industry Standard

#### 2.1 Origins (2019-2021)

```
2019: DuckDB founded at CWI (Centrum Wiskunde & Informatica) 
      in Amsterdam by Hannes Mühleisen and Mark Raasveldt.
      Hannes had co-authored the "Monet" / vectorized execution 
      papers that shaped modern columnar engines (ClickHouse, 
      BigQuery, Snowflake all trace lineage to this work).

2020: First open-source releases. "SQLite for analytics" positioning.

2021: DuckDB Labs founded as the commercial sponsor.
      Still MIT-licensed; corporate steward model similar to Postgres.
```

#### 2.2 The Rise (2022-2024)

```
2022: Growing adoption in notebook / data-science communities.
      Replacement for Pandas on larger datasets.

2023: MotherDuck emerges (hosted DuckDB + hybrid compute).
      DuckDB-WASM: full DuckDB in the browser.
      dbt-duckdb adapter matures; teams use DuckDB for small-med dbt projects.

2024: Enterprise adoption accelerates. Companies realize most
      P99 queries fit on one modern instance.
      "Big data is dead" meme takes hold (Jordan Tigani, ex-BigQuery).
```

#### 2.3 Maturity (2025-2026)

```
2025 May: DuckLake manifesto published.
          First proposal to put lakehouse metadata in a SQL database
          rather than files in object storage.

2025 Oct: DuckDB 1.4 LTS released.
          AES-256 encryption, MERGE statement, Iceberg writes,
          sort engine rewrite, enhanced query optimizer.
          First Long-Term Support release.

2026 Apr: DuckLake v1.0 production release.
          Backward-compatibility guarantee.
          Iceberg interop.

2026    : DuckDB is now the default analytical engine for:
          - Notebook-scale analytics
          - Small-to-medium data pipelines (<10 TB)
          - Embedded analytics in applications
          - Serverless analytics (MotherDuck)
          - Local prototyping before cloud deployment
          - Edge analytics (iOS, Android, WASM)
```

#### 2.4 The Lineage

```
VECTORIZED EXECUTION pedigree:
   MonetDB (CWI, 2000s) 
      → "MonetDB/X100" vectorized paper (Boncz, 2005)
      → ClickHouse (Yandex, 2016+)
      → DuckDB (CWI alumni, 2019)
      → All modern columnar engines borrow from this school.

COLUMNAR STORAGE pedigree:
   C-Store (MIT, 2005) → Vertica → Redshift
   The DuckDB file format descends from this lineage too.

PHILOSOPHICAL ANCESTOR:
   SQLite (D. Richard Hipp, 2000)
      → "You don't need a server for most of what you do."
   DuckDB applied that insight to OLAP.
```

> **🧠 Principal Mindset:** DuckDB's lineage matters because it tells you what's being optimized. The CWI vectorized-execution school produces engines that beat distributed systems on single-node workloads. DuckDB isn't a "lightweight toy" — it's the production-grade descendant of 20 years of academic research on how to execute SQL fast on commodity hardware. When you argue for DuckDB in a design review, grounding it in this lineage shuts down the "but is it ready for serious work" objection.

### 3. The Single-Node Renaissance

#### 3.1 The "Big Data Is Dead" Argument

```
Thesis (Jordan Tigani, 2023; echoed by Mühleisen et al.):

   Most companies do not have big data.
   Most queries do not need distributed compute.
   The last decade over-rotated to distributed-by-default.

Supporting evidence:
   • Modern instances: 192 vCPUs, 24 TB RAM available on-demand
   • NVMe SSDs: 10 GB/s sequential, sub-microsecond random
   • Most organization data warehouses are <10 TB active hot
   • Most individual queries scan <100 GB
   • Distributed systems' coordination overhead (shuffle, scheduler, 
     task scheduling) dominates at small-medium scale

Counter-evidence (don't ignore):
   • True PB-scale workloads exist (Netflix, Meta, Google)
   • Multi-user concurrency sometimes genuinely needs distribution
   • Enterprise governance ties you to specific vendors
```

#### 3.2 Where the Pendulum Settled

```
2015: "Use Spark for anything > 100 GB"
      → Distributed by default. 
      → Spin up EMR / Databricks cluster per job.

2020: "Lakehouse means Spark + Delta/Iceberg on everything"
      → Still distributed. 
      → Cluster warm-up time dominates small jobs.

2024-2026: "Single-node first; distributed only when proven necessary"
      → Default: DuckDB / Polars for TB-scale.
      → Escalate: Spark / Trino for 10+ TB or truly concurrent.

The industry rediscovered a pattern databases knew for decades:
match the engine to the data size, not the hype cycle.
```

#### 3.3 The Workload Shape That Favors DuckDB

```
GOOD FIT:
   ✓ Data fits in RAM or spills modestly to disk (up to ~1 TB working set)
   ✓ Analytical queries (aggregations, joins, windows)
   ✓ Single-user or low-concurrency reads
   ✓ Batch pipelines where latency to start matters
   ✓ Interactive notebooks
   ✓ Embedded analytics in applications
   ✓ Edge / mobile analytics

BAD FIT:
   ✗ PB-scale data (coordination of multiple nodes required)
   ✗ High-concurrency serving (100+ simultaneous writers)
   ✗ Real-time stream processing
   ✗ OLTP transactional workloads
   ✗ Multi-tenant SaaS back-end (single-writer limits concurrency)
```

#### 3.4 The Economics

```
A Databricks cluster with 10 workers, 32 cores each (320 cores total):
   ~$30/hour compute + storage + overhead
   Spin-up: ~2-5 minutes before first query
   Minimum cost per job: $5-30 depending on startup overhead

A single m7a.16xlarge EC2 instance (64 vCPU, 256 GB RAM) with DuckDB:
   ~$3-4/hour on-demand
   Spin-up: ~30 seconds
   Minimum cost per job: cents

For an 8-hour daily pipeline processing 500 GB:
   Spark/Databricks: $240/day = ~$7,200/month
   DuckDB on EC2:    $25/day  = ~$750/month
   
   10x cost reduction for workloads that fit single-node.
```

> **🔥 Production Scenario:** A fintech team was running 40 daily Spark pipelines on Databricks, each processing 50-500 GB of transaction data. Average pipeline cost: $8/run. Total: ~$10K/month in compute. They migrated 32 of the 40 pipelines to DuckDB on Lambda + S3. Each ran in 2-4 minutes end-to-end for under $0.50. Monthly bill dropped to $1.5K. The 8 pipelines that couldn't move were the ones with genuine multi-TB joins; those stayed on Spark. Lesson: audit your "distributed" pipelines — most of them aren't actually distributed workloads, they're small jobs you forced through a distributed engine.

> **⚠️ Warning:** Don't assume DuckDB is always cheaper. The crossover point depends on your workload shape. For truly distributed workloads (multi-TB joins, hundreds of concurrent users, globally distributed data) Spark / Snowflake / BigQuery are still correct. The staff-level signal is knowing where YOUR workloads sit on this spectrum, not blanket statements.

### 4. DuckDB vs The Alternatives

#### 4.1 The Comparison Matrix

| Dimension | DuckDB | SQLite | Pandas | Polars | ClickHouse | Spark |
|-----------|--------|--------|--------|--------|------------|-------|
| **Primary workload** | OLAP | OLTP | OLAP | OLAP | OLAP | OLAP |
| **Storage** | Columnar | Row | In-memory | In-memory + Arrow | Columnar | Row/Columnar |
| **Execution** | Vectorized | Interpreted | Row-at-a-time | Vectorized (Rust) | Vectorized | JIT-compiled |
| **Deployment** | In-process | In-process | In-process | In-process | Server | Cluster |
| **Scale** | ~1 TB | ~10 GB | ~RAM/5 | ~RAM | PB (clustered) | PB |
| **Multi-user** | 1 writer, N readers | 1 writer | N/A | N/A | Many | Many |
| **SQL** | Full | Full | No (via query) | Partial | Full | Full |
| **Language bindings** | 15+ | Everywhere | Python | Python/Rust | Many | Many |
| **ClickBench rank** | #1 | N/A | N/A | Top 10 | Top 5 | ~#20 |
| **License** | MIT | Public domain | BSD | MIT | Apache 2 | Apache 2 |

#### 4.2 Head-to-Head Decisions

**DuckDB vs Pandas**
```
Use DuckDB when:
   • Data > 1 GB (Pandas starts slowing down)
   • You need SQL readability for team review
   • You need out-of-core processing
   • You want multi-threaded aggregations
   • Interop across Python/R/Node matters

Use Pandas when:
   • Deep Python-native row-level manipulation
   • Legacy codebase, migration cost too high
   • Advanced statistical / ML pipelines downstream
```

**DuckDB vs Polars**
```
Use DuckDB when:
   • Team is SQL-fluent
   • Multi-language interop (Python + R + Node)
   • You need built-in Parquet/CSV/Iceberg/Delta readers
   • You want MotherDuck or DuckLake

Use Polars when:
   • Python/Rust-only team
   • You want Pythonic DataFrame API (chainable)
   • You're building a library/package (lighter imports)
   • Streaming / lazy evaluation API matters

They complement well: DuckDB for SQL, Polars for programmatic DataFrames.
In practice many teams use both, with Arrow as the zero-copy bridge.
```

**DuckDB vs ClickHouse**
```
Use DuckDB when:
   • Single-user analytics
   • Embedded in application
   • Notebook / exploration
   • No ops team to run servers

Use ClickHouse when:
   • Multi-tenant real-time dashboards
   • 10K+ QPS of analytical queries
   • Hundreds of concurrent users
   • Operational analytics at scale
```

**DuckDB vs Spark**
```
Use DuckDB when:
   • Data fits single node (up to ~1 TB)
   • Batch jobs where startup matters
   • Avoiding cluster ops
   • Cost sensitivity

Use Spark when:
   • Data > 10 TB and growing
   • Already Databricks-committed
   • Need distributed ML (MLlib, horovod)
   • Streaming workloads
```

#### 4.3 The Decision Framework

```
STEP 1: Does your data truly need distribution?
   NO  → DuckDB tier
   YES → Spark / Trino / Snowflake tier

STEP 2: Within DuckDB tier, who are the users?
   Me, in a notebook → DuckDB alone
   My Python app     → DuckDB as library
   My team (shared)  → MotherDuck (hosted DuckDB)
   My multi-user app → ClickHouse (not DuckDB)

STEP 3: What's the storage?
   Local files       → DuckDB native
   S3/GCS/Azure      → DuckDB HTTPFS extension
   Iceberg/Delta     → DuckDB with respective extensions
   Shared lakehouse  → DuckLake OR Iceberg via Polaris
```

> **🎯 Interview Angle:** Q: Your team has 5 analysts running ad-hoc SQL against 2 TB of data on S3. They use Snowflake today, costing $25K/month. Should they move to DuckDB?<br>
> **Staff:** "Depends on the access pattern. If they're running tens of queries per day with results cached between (typical), DuckDB on an EC2 instance (~$500/month) with Parquet on S3 would cover it, and I'd prototype that. If they're running concurrent queries all day (say 20+ simultaneous), DuckDB's single-writer and modest multi-reader concurrency might not fit; a hosted DuckDB like MotherDuck or sticking with Snowflake makes sense. Also check: do they rely on Snowflake-specific features (semi-structured, Snowpark, data sharing)? Those take non-trivial work to replicate. My recommendation would be a 30-day parallel pilot on the real workload — then decide with data, not slogans."

---

## PART II — SQL IN DUCKDB

### 5. The DuckDB SQL Dialect (Friendly SQL)

#### 5.1 The Base: Postgres-Compatible

DuckDB targets Postgres compatibility as its baseline. If you know Postgres SQL, you know most of DuckDB SQL. On top of that, DuckDB adds ergonomic extensions it calls "Friendly SQL" — ideas it adopted or innovated that make SQL less painful to write.

#### 5.2 Trailing Commas (The Simple Win)

```sql
-- Valid DuckDB; Postgres rejects this
SELECT
    customer_id,
    order_date,
    SUM(amount),  -- trailing comma allowed
FROM orders
GROUP BY customer_id, order_date,;
```

Trailing commas eliminate the "re-order columns, remove the dangling comma" dance that every SQL author hates.

#### 5.3 GROUP BY ALL

```sql
-- Instead of listing every non-aggregated column
SELECT 
    customer_id, 
    order_date, 
    region, 
    product_category,
    SUM(amount) AS total
FROM orders
GROUP BY ALL;  -- DuckDB infers the grouping columns

-- Equivalent in standard SQL:
GROUP BY customer_id, order_date, region, product_category;
```

Also supports `ORDER BY ALL` — sort by all projected columns in order.

#### 5.4 Column Aliases in WHERE / GROUP BY

```sql
-- Valid DuckDB; most dialects reject this
SELECT
    EXTRACT(month FROM order_date) AS order_month,
    SUM(amount) AS revenue
FROM orders
WHERE order_month BETWEEN 1 AND 6  -- reference the alias
GROUP BY order_month;
```

#### 5.5 EXCLUDE and REPLACE in SELECT *

```sql
-- Select all columns except PII
SELECT * EXCLUDE (ssn, dob, email) FROM users;

-- Select all columns, but transform one
SELECT * REPLACE (UPPER(name) AS name) FROM users;

-- Combine
SELECT * EXCLUDE (ssn, dob) REPLACE (UPPER(name) AS name) FROM users;
```

For wide tables with 50+ columns, this is a lifesaver. No more typing out every column just to drop two.

#### 5.6 COLUMNS() Expressions

```sql
-- Apply a function to a group of columns
SELECT 
    MIN(COLUMNS(* EXCLUDE (id))),  -- MIN of every column except id
    MAX(COLUMNS('temp_.*'))        -- MAX of every column matching regex
FROM sensor_readings;

-- Equivalent to writing MIN(col1), MIN(col2), MIN(col3)... for every column.
```

This dramatically shortens exploratory analysis queries.

#### 5.7 Lambda Functions in SQL

```sql
-- Transform elements of an array
SELECT list_transform([1, 2, 3, 4], x -> x * 2);
-- Returns: [2, 4, 6, 8]

-- Filter array elements
SELECT list_filter([1, 2, 3, 4, 5], x -> x > 2);
-- Returns: [3, 4, 5]

-- Aggregate across array
SELECT list_reduce([1, 2, 3, 4], (acc, x) -> acc + x);
-- Returns: 10
```

#### 5.8 ASOF Joins

```sql
-- "Give me the most recent price AS OF each trade's time"
SELECT 
    t.trade_id,
    t.trade_time,
    p.price AS price_at_trade_time
FROM trades t
ASOF LEFT JOIN prices p
  ON t.symbol = p.symbol 
  AND t.trade_time >= p.quote_time;
```

ASOF joins are first-class in DuckDB — critical for time-series work and point-in-time correctness in feature stores. Most other engines require correlated subqueries or complex window logic.

#### 5.9 PIVOT and UNPIVOT

```sql
-- PIVOT
PIVOT sales
ON region
USING SUM(amount)
GROUP BY product_id;

-- UNPIVOT
UNPIVOT wide_table
ON jan, feb, mar, apr
INTO 
    NAME month
    VALUE sales;
```

First-class statements, not awkward subquery incantations.

> **💡 Tip:** The friendly SQL features are DuckDB-specific but increasingly adopted. BigQuery added GROUP BY ALL in 2023. Snowflake supports some of this. If you write DuckDB SQL, be aware which features are portable (standard Postgres) vs DuckDB-only (friendly SQL) when the output needs to run elsewhere.

### 6. Advanced SQL Features

#### 6.1 Full Window Function Support

```sql
-- Running total with proper framing
SELECT 
    customer_id,
    order_date,
    amount,
    SUM(amount) OVER (
        PARTITION BY customer_id 
        ORDER BY order_date
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS lifetime_spend,
    AVG(amount) OVER (
        PARTITION BY customer_id
        ORDER BY order_date
        ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
    ) AS rolling_7_avg
FROM orders;
```

All standard window functions supported: ROW_NUMBER, RANK, DENSE_RANK, LAG, LEAD, FIRST_VALUE, LAST_VALUE, NTH_VALUE, NTILE, CUME_DIST, PERCENT_RANK, aggregate-as-window.

#### 6.2 QUALIFY Clause

```sql
-- Filter by window function result without a subquery
SELECT customer_id, order_date, amount
FROM orders
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY customer_id ORDER BY order_date DESC
) = 1;

-- Standard SQL equivalent:
-- SELECT ... FROM (
--   SELECT ..., ROW_NUMBER() OVER (...) AS rn FROM orders
-- ) WHERE rn = 1;
```

#### 6.3 Recursive CTEs

```sql
-- Hierarchical traversal (e.g., org chart)
WITH RECURSIVE org_tree AS (
    SELECT employee_id, manager_id, name, 0 AS depth
    FROM employees 
    WHERE manager_id IS NULL
    
    UNION ALL
    
    SELECT e.employee_id, e.manager_id, e.name, ot.depth + 1
    FROM employees e
    JOIN org_tree ot ON e.manager_id = ot.employee_id
)
SELECT * FROM org_tree ORDER BY depth, name;
```

#### 6.4 SQL/PGQ (Property Graph Queries)

Added in 2024, DuckDB supports the SQL/PGQ standard for graph queries over relational tables.

```sql
-- Define a property graph over existing tables
CREATE PROPERTY GRAPH social_graph
VERTEX TABLES (users)
EDGE TABLES (
    friendships SOURCE KEY (user_a) REFERENCES users (user_id)
                DESTINATION KEY (user_b) REFERENCES users (user_id)
);

-- Query the graph
SELECT *
FROM GRAPH_TABLE (social_graph
    MATCH (u1:users) -[:friendships]-> (u2:users) -[:friendships]-> (u3:users)
    WHERE u1.user_id = 42
    COLUMNS (u3.user_id AS friend_of_friend)
);
```

Few analytical engines offer graph queries natively; DuckDB is ahead of the curve here.

#### 6.5 MERGE Statement (DuckDB 1.4+)

```sql
MERGE INTO target t
USING source s
ON t.id = s.id
WHEN MATCHED AND s.deleted THEN DELETE
WHEN MATCHED THEN UPDATE SET name = s.name, updated_at = s.updated_at
WHEN NOT MATCHED THEN INSERT (id, name, created_at) 
                     VALUES (s.id, s.name, s.updated_at);
```

Full MERGE support arrived in DuckDB 1.4 LTS (October 2025) — essential for CDC-style patterns.

#### 6.6 Statistical Functions

```sql
-- Approximate aggregates
SELECT 
    approx_count_distinct(user_id) AS unique_users,
    approx_quantile(response_time, 0.95) AS p95,
    approx_quantile(response_time, [0.5, 0.9, 0.95, 0.99]) AS percentiles
FROM events;

-- Exact statistics
SELECT 
    corr(x, y) AS correlation,
    regr_slope(y, x) AS slope,
    regr_intercept(y, x) AS intercept,
    stddev_pop(amount) AS std,
    skewness(amount),
    kurtosis(amount)
FROM data;
```

#### 6.7 Array / List Operations

```sql
-- Native array handling
SELECT 
    [1, 2, 3, 4, 5] AS my_list,
    list_sum([1, 2, 3, 4, 5]),           -- 15
    list_sort([3, 1, 4, 1, 5]),           -- [1, 1, 3, 4, 5]
    list_distinct([1, 2, 2, 3, 3, 3]),    -- [1, 2, 3]
    list_slice([1, 2, 3, 4, 5], 2, 4),    -- [2, 3, 4]
    list_contains([1, 2, 3], 2);          -- true
```

> **🎯 Interview Angle:** Q: What SQL features would you miss if you moved from Snowflake to DuckDB?<br>
> **Staff:** "Not many for analytical workloads. DuckDB covers everything I use in production Snowflake: windows, QUALIFY, CTEs, MERGE, nested types, array operations, approximate aggregates, and more. The real gaps are operational: Snowflake's data sharing, cross-account marketplace, result cache, warehouse sizing — those are platform features, not SQL. For a pure analytical SQL comparison, DuckDB sometimes has MORE expressive features (lambda functions, EXCLUDE/REPLACE, PGQ graph queries). The question is really about ecosystem, not language."

### 7. Nested Types & Semi-Structured Data

#### 7.1 First-Class Nested Types

DuckDB has first-class support for complex nested types, borrowed from Parquet / Arrow conventions:

```sql
-- STRUCT: named-field record
SELECT {'name': 'Alice', 'age': 30} AS person;

-- LIST: ordered collection
SELECT [1, 2, 3, 4] AS numbers;

-- MAP: key-value pairs
SELECT MAP {'a': 1, 'b': 2, 'c': 3} AS my_map;

-- Nested combinations
SELECT [{'name': 'Alice', 'tags': ['admin', 'user']},
        {'name': 'Bob',   'tags': ['user']}] AS users;
```

#### 7.2 Struct Access

```sql
WITH data AS (
    SELECT {'name': 'Alice', 'address': {'city': 'NYC', 'zip': '10001'}} AS person
)
SELECT 
    person.name,
    person.address.city,
    person.address.zip
FROM data;

-- Also with bracket notation
SELECT person['address']['city'] FROM data;
```

#### 7.3 List Comprehensions

```sql
-- Python-like list comprehensions in SQL
SELECT [x * 2 FOR x IN [1, 2, 3, 4] IF x > 1] AS doubled_filtered;
-- Returns: [4, 6, 8]
```

#### 7.4 JSON Handling

```sql
-- Parse and query JSON
WITH data AS (
    SELECT '{"name": "Alice", "orders": [{"id": 1, "amount": 100}, 
                                          {"id": 2, "amount": 200}]}'::JSON AS doc
)
SELECT 
    doc->>'$.name' AS name,
    doc->'$.orders[0].id' AS first_order_id,
    doc->'$.orders[*].amount' AS all_amounts
FROM data;

-- Read a JSON file directly
SELECT * FROM read_json_auto('data.json');

-- Read newline-delimited JSON (common for logs)
SELECT * FROM read_ndjson('logs.jsonl');
```

#### 7.5 UNNEST (Flatten Nested)

```sql
-- Flatten a list column into rows
WITH data AS (
    SELECT 'Alice' AS name, [1, 2, 3] AS orders
)
SELECT name, unnest(orders) AS order_id FROM data;
-- Returns: 
--   Alice, 1
--   Alice, 2
--   Alice, 3

-- Flatten with index
SELECT name, order_id, order_num
FROM data, unnest(orders, 'index') AS t(order_id, order_num);
```

#### 7.6 Struct Introspection

```sql
-- Get struct field names dynamically
SELECT struct_extract({'a': 1, 'b': 2}, 'a');  -- 1

-- Access all keys / values
SELECT 
    struct_keys({'a': 1, 'b': 2, 'c': 3}),    -- ['a', 'b', 'c']
    struct_values({'a': 1, 'b': 2, 'c': 3});   -- [1, 2, 3]
```

> **🧠 Principal Mindset:** DuckDB's nested-type story is as mature as Snowflake's VARIANT or BigQuery's STRUCT. For teams working with semi-structured data (events, API payloads, JSON logs), DuckDB handles it natively without extensions. This is a frequent surprise in interviews — people assume DuckDB is only for flat tables and don't bring up nested types as a strength.

---

## PART III — ENGINE INTERNALS

### 8. Vectorized Query Execution

#### 8.1 The Classical Problem

```
TRADITIONAL ROW-AT-A-TIME EXECUTION:
   for each row in table:
       for each operator in query_plan:
           evaluate(operator, row)
       emit result

Problems:
   • One function call per operator per row
   • Branch prediction struggles
   • CPU cache under-utilized
   • Instruction cache thrash
   
Result: typical overhead of 50-100 ns per row per operator
```

#### 8.2 The Vectorized Alternative

DuckDB uses the **vectorized execution model** popularized by MonetDB/X100 (Boncz, Zukowski, Nes 2005).

```
VECTORIZED EXECUTION:
   for each batch of N rows (typically N=2048):
       for each operator in query_plan:
           evaluate(operator, batch)
       emit batch

Key insight: amortize per-call overhead across many rows.
```

#### 8.3 Why 2048?

```
Vector size = 2048 rows per chunk.

Trade-off:
   Too small (e.g., 32): doesn't amortize call overhead
   Too large (e.g., 100k): doesn't fit in L1/L2 cache
   
2048 = ~16 KB per column (for int32):
   Fits comfortably in L1 cache (32-64 KB typical)
   Each batch stays "hot" through the entire operator pipeline
```

#### 8.4 The Pipeline Model

```
Example query: SELECT SUM(amount) FROM orders WHERE status = 'completed';

Pipeline:
   READ(2048 rows from storage)   
      → FILTER status = 'completed' (operates on batch)
      → PROJECT amount (keep only needed column)
      → AGGREGATE SUM (accumulate into state)
   
   Next 2048 rows, same pipeline, same operators, warm cache.

Compare: row-at-a-time would call read, filter, project, aggregate
functions once per row = 4 × N function calls for N rows.
Vectorized: 4 × (N/2048) function calls.
```

#### 8.5 Why This Matters

```
Benchmark on 100M row aggregation (simple SUM):
   Postgres (row-at-a-time): ~45 seconds
   SQLite (row-at-a-time):   ~30 seconds
   DuckDB (vectorized):      ~2 seconds
   
Order-of-magnitude difference — purely from execution model.
No new hardware, no distribution; just better use of the CPU you have.
```

#### 8.6 SIMD Operations

```
Where possible, DuckDB uses SIMD (Single Instruction, Multiple Data) 
CPU instructions to process multiple values per instruction:

AVX-512 (modern Intel): 16 int32 values per instruction
AVX2 (older Intel):      8 int32 values per instruction
NEON (ARM):              4 int32 values per instruction

Example: comparing a column of int32 to a constant
   Scalar: 1 comparison per CPU cycle = N cycles for N values
   SIMD:   16 comparisons per CPU cycle = N/16 cycles for N values
   
This is a 16x speedup on filtering, hashing, and arithmetic.
```

> **🎯 Interview Angle:** Q: Why is DuckDB faster than Pandas for aggregations even on a single thread?<br>
> **Staff:** "Two reasons. (1) Vectorized execution: DuckDB processes batches of 2048 values through each operator at a time, amortizing function-call overhead and keeping data in CPU cache. Pandas evaluates row-by-row or column-by-column with Python-level overhead per operation. (2) SIMD: DuckDB's operators use CPU vector instructions where possible — 4-16 values per instruction instead of one. Pandas has limited SIMD; the Python wrappers around NumPy prevent many optimizations. Add multi-threading via Morsel-driven parallelism and the gap widens to 10-50x on real workloads."

### 9. Morsel-Driven Parallelism

#### 9.1 The Classical Problem

```
HOW DO YOU PARALLELIZE A QUERY ACROSS CORES?

Naive approach: partition data by core, run same query on each, merge.
   
Problems:
   • Skew: some partitions larger than others
   • Load imbalance: some cores finish early and idle
   • Static partitioning: can't adapt to runtime cost
```

#### 9.2 Morsel-Driven Parallelism (Leis et al., 2014)

DuckDB adopted morsel-driven parallelism, originally from HyPer:

```
MORSEL = a small chunk of input data (e.g., 100,000 rows)

Workers:
   • Each core is a worker thread
   • Workers pull morsels from a shared queue
   • When a worker finishes a morsel, it grabs the next one
   • Workers don't own specific partitions; they consume dynamically

Result:
   • Perfect load balancing (fast cores take more morsels)
   • Adapts to skew (a fast worker helps a slow worker's data)
   • No idle cores until the queue is empty
```

#### 9.3 The Flow

```
Imagine a 64-core machine running a query on 1 billion rows:

   Split 1B rows into 10,000 morsels of 100K rows each.
   
   64 worker threads start.
   
   Each thread grabs a morsel, processes it through the operator pipeline,
   emits intermediate results, then grabs another morsel.
   
   Final merge happens when the queue is empty.
   
   All 64 cores stay busy until the end.
```

#### 9.4 Why This Is Hard

```
NAIVE multi-threading would:
   • Statically assign thread 1 to rows 1-16M, thread 2 to 17-32M, ...
   • If thread 1's data has more "completed" rows than thread 2's,
     thread 1 does more aggregation work.
   • Thread 2 finishes early and sits idle.

Morsel-driven avoids this by making work ALLOCATION DYNAMIC.
The slow worker simply takes fewer morsels; the queue drains at a uniform rate.
```

#### 9.5 Scaling Behavior

```
Benchmark: SUM aggregation on 1B rows, varying core count:

   1 core:    120 seconds
   2 cores:    62 seconds   (1.9x speedup)
   4 cores:    32 seconds   (3.75x speedup)
   8 cores:    17 seconds   (7.0x speedup)
   16 cores:    9 seconds   (13.3x speedup)
   32 cores:    5 seconds   (24x speedup)
   64 cores:    3 seconds   (40x speedup)

Near-linear scaling until memory bandwidth saturates.
On modern hardware with high-bandwidth RAM, DuckDB scales to 64+ cores.
```

> **💡 Tip:** DuckDB uses all available CPU cores by default. You can control this with `SET threads = N;`. For benchmarking, set threads=1 to isolate single-threaded performance. For production, leave at default (use all cores).

### 10. Native Storage Format

#### 10.1 The .duckdb File Format

```
DuckDB has its OWN native format: a single file with .duckdb extension.
   
Characteristics:
   • Single-file (like SQLite)
   • Columnar storage internally
   • ACID transactions
   • Supports multiple tables, views, indexes, extensions
   • Backwards-compatible with earlier format versions
```

#### 10.2 Columnar Layout

```
Each table is stored as a set of ROW GROUPS.
Within each row group, data is organized BY COLUMN.
Each column has its own compression and encoding.

   ┌──────────────────────────────────────────────┐
   │ ROW GROUP 1 (122,880 rows)                   │
   │   ├─ Column A: compressed vector of int32    │
   │   ├─ Column B: compressed vector of varchar  │
   │   ├─ Column C: compressed vector of double   │
   │   └─ ...                                     │
   ├──────────────────────────────────────────────┤
   │ ROW GROUP 2 (122,880 rows)                   │
   │   ├─ ...                                     │
   └──────────────────────────────────────────────┘

Row groups default to 122,880 rows = 60 × 2048 (vector size × 60).
```

#### 10.3 Compression Schemes

```
DuckDB chooses compression per column, per block:

   • CONSTANT: single-value columns (all NULL, all "A")
   • RLE (Run-Length Encoding): long runs of same value
   • DICTIONARY: low-cardinality strings/ints
   • BIT-PACKING: integers that fit in fewer bits
   • FOR (Frame-of-Reference): sorted integers
   • DELTA: time series with slow-changing differences
   • FSST: fast string compression (CWI research)
   • ALP: Adaptive Lossless floating-Point compression

Typical compression ratios:
   Numeric data:   3-10x
   Categorical:    10-50x
   Timestamps:     5-20x
   Free text:      2-5x
```

#### 10.4 Zone Maps / Min-Max Statistics

```
Each row group stores per-column statistics:
   • Min value
   • Max value
   • Null count
   • Approximate distinct count

Query: WHERE event_date = '2024-03-15'
   Engine reads row group min/max first.
   Skips row groups where '2024-03-15' isn't in range.
   Scans only candidate row groups.

Result: automatic pruning without explicit indexes.
```

#### 10.5 Transactions & Durability

```
DuckDB supports full ACID transactions:

   BEGIN TRANSACTION;
   INSERT INTO orders VALUES (...);
   UPDATE inventory SET qty = qty - 1 WHERE id = 42;
   COMMIT;

Mechanism:
   • Write-ahead log (WAL) for durability
   • MVCC for isolation (readers don't block writers)
   • Checkpointing to merge WAL into main file
```

#### 10.6 File Size & Portability

```
A .duckdb file is self-contained:
   • Copy it between machines
   • Share via S3, HTTPS, email
   • Open with any DuckDB client (Python, R, Node, CLI, ...)
   
Example sizes:
   100 GB raw CSV → ~15 GB .duckdb file (6.7x compression typical)
   10 GB Parquet → ~8-12 GB .duckdb file (similar compression)
```

### 11. MVCC & Concurrency Model

#### 11.1 The Model

DuckDB implements MVCC (Multi-Version Concurrency Control):

```
Readers see a snapshot of the database at the time their transaction started.
Writers create new versions of modified rows.
Old versions are kept until all transactions that could see them finish.
```

#### 11.2 Concurrency Limits

```
WITHIN ONE PROCESS:
   • Multiple threads can read concurrently
   • One writer at a time per database file
   • Readers don't block writers, writers don't block readers

ACROSS PROCESSES:
   • By default, only one process can open a .duckdb file (exclusive lock)
   • Read-only mode: multiple processes can open the same file for reading
   • Workaround for writes: use external coordination or MotherDuck
```

#### 11.3 Why Not Multi-Writer

```
DuckDB's design prioritizes:
   • Simplicity (no distributed consensus)
   • Low latency (no coordination overhead)
   • Local control (one process owns the file)

Trade-off:
   • Not suitable as a shared transactional database for many apps
   • Perfect for one-writer, many-reader analytical workloads
   • If you need multi-writer, use Postgres or MotherDuck
```

#### 11.4 Patterns for Multi-User Access

```
PATTERN 1: One writer, many readers via file replication
   • Writer writes to primary .duckdb file
   • Snapshot copies pushed to readers (S3, filesystem)
   • Readers open local copies in read-only mode

PATTERN 2: External coordination (dbt, Airflow)
   • A single scheduled job writes to the file
   • Downstream readers consume the result
   • No simultaneous writers needed

PATTERN 3: MotherDuck (hosted DuckDB)
   • Multi-user access via a service
   • Hybrid local + cloud execution
   • Pay for shared concurrency

PATTERN 4: DuckLake (lakehouse)
   • Multiple DuckDB processes write to shared Parquet files
   • Transactional coordination via SQL metadata database
   • Multi-writer by design
```

> **🎯 Interview Angle:** Q: Can DuckDB serve as the back-end for a multi-tenant SaaS?<br>
> **Staff:** "In general, no — DuckDB's single-writer-per-file model doesn't fit high-concurrency transactional workloads. But there are specific patterns where it does: (1) per-tenant isolated .duckdb files, each serving one tenant's analytics with modest concurrency; (2) MotherDuck as the hosted layer handling multi-tenant for you; (3) read-only replicas distributed to serving nodes with a central writer. For traditional multi-tenant SaaS back-ends with hundreds of simultaneous writers, Postgres or a distributed DB is right. DuckDB fits where the workload is read-heavy analytical with controlled write cadence."

---

## PART IV — READING EXTERNAL DATA

### 12. Parquet, CSV, JSON Zero-Copy

#### 12.1 Reading Parquet Directly

```sql
-- Query a local Parquet file as if it were a table
SELECT * FROM 'data.parquet';

-- Multiple files via glob
SELECT * FROM 'data/*.parquet';

-- Partitioned directory
SELECT * FROM 'data/year=*/month=*/*.parquet';

-- Explicit reader with options
SELECT * FROM read_parquet('data.parquet', 
                            hive_partitioning = true,
                            filename = true);
```

#### 12.2 Why This Matters

```
NO ETL REQUIRED:
   • Point DuckDB at your Parquet files
   • Query them as tables
   • No import step, no schema definition

PREDICATE PUSHDOWN:
   WHERE year = 2024 → only files/row-groups where year = 2024 are read
   SELECT col1, col2 → only col1 and col2 columns are read

PROJECTION PUSHDOWN:
   Only requested columns are decoded
   Massive I/O savings on wide tables
```

#### 12.3 CSV Reading

```sql
-- Auto-detect schema and parse
SELECT * FROM 'data.csv';

-- Explicit control
SELECT * FROM read_csv('data.csv', 
                        header = true,
                        delim = ',',
                        quote = '"',
                        null_padding = true,
                        auto_type_candidates = ['BIGINT', 'VARCHAR']);

-- Reject rows that fail parsing (keep processing)
SELECT * FROM read_csv('messy.csv', 
                        ignore_errors = true,
                        rejects_table = 'parse_errors');
```

DuckDB's CSV reader is arguably the best in the industry — handles messy real-world CSVs where pandas chokes.

#### 12.4 JSON Reading

```sql
-- Auto-detect structured JSON
SELECT * FROM 'data.json';

-- Newline-delimited JSON (logs)
SELECT * FROM read_ndjson('logs.jsonl');

-- Nested JSON preserved as STRUCT
SELECT doc.user.name, doc.user.age 
FROM read_json('users.json');

-- JSON path extraction
SELECT 
    json_extract(data, '$.user.name') AS name,
    json_extract_string(data, '$.user.email') AS email
FROM read_ndjson('events.jsonl');
```

#### 12.5 Writing Back

```sql
-- Write a query result to Parquet
COPY (SELECT * FROM my_table) TO 'output.parquet' (FORMAT PARQUET);

-- Write partitioned Parquet
COPY (SELECT * FROM events) TO 'output/' 
    (FORMAT PARQUET, PARTITION_BY (year, month), OVERWRITE_OR_IGNORE);

-- Write with compression / row group tuning
COPY (SELECT * FROM my_table) TO 'output.parquet' 
    (FORMAT PARQUET, COMPRESSION 'zstd', ROW_GROUP_SIZE 100000);
```

#### 12.6 The Workflow Pattern

```
PATTERN: Transform-in-place without ETL

   raw/*.csv → DuckDB query with filters/joins → gold.parquet
   
   No table definitions. No schema registration.
   SQL does everything: read, transform, write.
```

> **🔥 Production Scenario:** A team was running a 90-minute daily Spark job that read 200 CSVs totaling 50 GB, joined them with reference tables, and wrote Parquet to S3. Replaced with a 50-line DuckDB script running on a Lambda-equivalent instance: 8 minutes end-to-end, $0.80/run. The team realized the Spark job wasn't actually distributed — it was single-stage reads and joins that fit in memory. Lesson: audit your Spark workloads; many are small jobs forced into a distributed engine.

### 13. HTTPFS, S3, GCS, Azure

#### 13.1 The HTTPFS Extension

```sql
-- Enable HTTP/S3/GCS/Azure access
INSTALL httpfs;
LOAD httpfs;

-- Query a Parquet file directly from a URL
SELECT * FROM 'https://example.com/data.parquet';

-- Query S3 (uses AWS SDK; credentials via env/profile)
SELECT * FROM 's3://my-bucket/data/*.parquet';
```

#### 13.2 S3 Credentials

```sql
-- Set credentials inline
CREATE SECRET aws_creds (
    TYPE S3,
    KEY_ID 'AKIA...',
    SECRET 'wJalr...',
    REGION 'us-east-1'
);

-- Or use the AWS SDK chain (env vars, profiles, IAM role)
CREATE SECRET aws_creds (TYPE S3, PROVIDER CREDENTIAL_CHAIN);

-- Then query
SELECT * FROM 's3://my-bucket/data/*.parquet';
```

#### 13.3 GCS & Azure

```sql
-- Google Cloud Storage
CREATE SECRET gcs_creds (TYPE GCS, KEY_ID '...', SECRET '...');
SELECT * FROM 'gs://my-bucket/data.parquet';

-- Azure Blob Storage
CREATE SECRET azure_creds (TYPE AZURE, CONNECTION_STRING '...');
SELECT * FROM 'azure://my-container/data.parquet';
```

#### 13.4 Range Reads & Predicate Pushdown

```
DuckDB reads only the bytes it needs:

   Parquet footer: ~KB (always read)
   Row group metadata: ~KB per row group
   Column data: only requested columns, only matching row groups

Example: SELECT user_id FROM 's3://bucket/events/*.parquet' 
         WHERE event_date = '2024-03-15'
         
   1. Read file list (listing)
   2. For each file, read footer (metadata)
   3. For files with event_date in range: read user_id and event_date columns 
      from matching row groups
   4. Skip the rest

Transfer: possibly < 1% of the file sizes.
```

#### 13.5 Caching

```sql
-- Enable local caching of remote files
SET enable_object_cache = true;

-- Pin specific files in memory
PRAGMA memory_limit = '10GB';
```

#### 13.6 The Economics

```
WORKFLOW: Daily analytics on 500 GB Parquet in S3

Snowflake-style:
   Load into warehouse → query → pay compute + storage separately
   Storage: $23/TB/month warehouse storage
   Compute: credits per query

DuckDB-style:
   Query S3 directly from a Lambda-equivalent instance
   Storage: $23/TB/month on S3 Standard (cheaper than warehouse)
   Compute: pay for the instance only while running

For intermittent queries, DuckDB on S3 is often 5-10x cheaper.
```

### 14. Iceberg, Delta, Hudi Reading

#### 14.1 Iceberg Reading

```sql
-- Install the Iceberg extension
INSTALL iceberg;
LOAD iceberg;

-- Read a table from a filesystem catalog
SELECT * FROM iceberg_scan('s3://bucket/warehouse/db/table/');

-- Via a REST catalog (Polaris, Unity Catalog)
ATTACH 'rest' AS catalog (
    TYPE ICEBERG,
    ENDPOINT 'https://polaris.example.com/',
    TOKEN '...',
    WAREHOUSE 'my_warehouse'
);

SELECT * FROM catalog.db.table;
```

#### 14.2 Delta Reading

```sql
INSTALL delta;
LOAD delta;

-- Read a Delta table
SELECT * FROM delta_scan('s3://bucket/warehouse/db/table/');

-- Via Unity Catalog
ATTACH 'uc' AS catalog (
    TYPE UC_CATALOG,
    ENDPOINT 'https://databricks.example.com/',
    TOKEN '...'
);

SELECT * FROM catalog.db.table;
```

#### 14.3 Writing to Open Formats

```sql
-- Iceberg writes (DuckDB 1.4+)
CREATE TABLE catalog.db.my_table AS SELECT * FROM my_data;
INSERT INTO catalog.db.my_table SELECT * FROM new_rows;

-- Delta writes
COPY (SELECT * FROM my_table) TO 's3://bucket/delta/' (FORMAT DELTA);
```

#### 14.4 Why This Matters

```
UNIFIED ACCESS:
   Same DuckDB engine reads Parquet, Iceberg, Delta, CSV, JSON.
   Choose the format for your workload; DuckDB adapts.

INTEROPERABILITY:
   Data written by Spark/Snowflake/Databricks can be queried by DuckDB.
   DuckDB fits anywhere in a modern lakehouse stack.

LOCAL DEVELOPMENT:
   Point DuckDB at your production Iceberg data (read-only).
   Prototype queries locally; deploy to warehouse engine for production.
```

#### 14.5 Current State of the Art (2026)

```
READING:
   Parquet:  ✓ (first-class, since v0.3)
   CSV:      ✓ (industry-leading auto-detection)
   JSON:     ✓
   Iceberg:  ✓ (reads + metadata + time travel)
   Delta:    ✓
   Hudi:     Experimental
   DuckLake: ✓ (native)

WRITING:
   Parquet:  ✓
   CSV:      ✓
   JSON:     ✓
   Iceberg:  ✓ (since DuckDB 1.4, October 2025)
   Delta:    ✓ (via extension, newer)
   Hudi:     Not yet
   DuckLake: ✓ (native)
```

> **🧠 Principal Mindset:** DuckDB's multi-format support is often undersold. In 2026, you can design a lakehouse where Spark writes Iceberg, DuckDB reads it for ad-hoc analytics, Snowflake reads it for BI, and Trino reads it for cross-cutting queries. Open formats + DuckDB are the "no lock-in" architecture for the small-to-mid data organization.

---

## PART V — ECOSYSTEM & INTEGRATION

### 15. Python Integration (pandas, polars, arrow)

#### 15.1 Zero-Copy Integration

DuckDB and Python's data stack share Apache Arrow as the common memory layout. This enables **zero-copy** data exchange:

```python
import duckdb
import pandas as pd
import polars as pl
import pyarrow as pa

# Query a pandas DataFrame directly (no copy!)
df_pandas = pd.read_csv('data.csv')
result = duckdb.sql("SELECT COUNT(*) FROM df_pandas").df()

# Query a polars DataFrame
df_polars = pl.read_csv('data.csv')
result = duckdb.sql("SELECT * FROM df_polars WHERE x > 10").pl()

# Query an Arrow table
arrow_tbl = pa.table({"a": [1, 2, 3]})
result = duckdb.sql("SELECT SUM(a) FROM arrow_tbl").arrow()
```

#### 15.2 The DuckDB Python API

```python
import duckdb

# Create an in-memory database
con = duckdb.connect()

# Or persistent
con = duckdb.connect('mydb.duckdb')

# Execute SQL
con.execute("SELECT * FROM 'data.parquet' WHERE x > 10")

# Fetch results
rows = con.fetchall()      # list of tuples
df   = con.df()            # pandas DataFrame
pl_df = con.pl()           # polars DataFrame
arr  = con.arrow()         # Arrow table
```

#### 15.3 The Relational API (DataFrame-style)

```python
import duckdb

# Chain operations like a DataFrame API
result = (
    duckdb.read_csv('data.csv')
          .filter('x > 10')
          .project('x, y, z')
          .aggregate('AVG(z) AS avg_z', 'x, y')
          .df()
)

# Mix SQL and DataFrame
r1 = duckdb.sql("SELECT * FROM 'data.csv' WHERE x > 10")
r2 = r1.aggregate('AVG(y) AS avg_y', 'z')
```

#### 15.4 UDFs in Python

```python
import duckdb

# Register a Python function as a SQL UDF
def my_formatter(value):
    return f"{value:,.2f}"

duckdb.create_function('fmt', my_formatter, 
                        parameters=[duckdb.typing.DOUBLE],
                        return_type=duckdb.typing.VARCHAR)

# Use it in SQL
duckdb.sql("SELECT fmt(amount) FROM orders").df()
```

#### 15.5 Pandas-to-DuckDB Migration Pattern

```python
# BEFORE: pure pandas
import pandas as pd

df = pd.read_csv('big.csv')          # Loads everything into RAM
df = df[df['status'] == 'active']    # Filter
df['total'] = df.groupby('cust_id')['amt'].transform('sum')
result = df[df['total'] > 1000]

# AFTER: DuckDB with Pandas interop
import duckdb

result = duckdb.sql("""
    WITH filtered AS (
        SELECT * FROM 'big.csv' WHERE status = 'active'
    )
    SELECT *, SUM(amt) OVER (PARTITION BY cust_id) AS total
    FROM filtered
    QUALIFY total > 1000
""").df()
```

The DuckDB version:
- Doesn't load everything into RAM (streaming read)
- Uses vectorized execution
- Parallelizes across cores
- Pushes filtering down to Parquet reader

#### 15.6 Jupyter Integration

```python
# Load the SQL magic
%load_ext duckdb_magic

# Now use SQL in cells
%%sql
SELECT customer_id, SUM(amount)
FROM 'orders.parquet'
GROUP BY customer_id
ORDER BY SUM(amount) DESC
LIMIT 10
```

> **💡 Tip:** The Pandas-DuckDB zero-copy integration means you can mix SQL and Python in one pipeline without performance cost. Many teams use DuckDB SQL for the heavy aggregations and Pandas / Polars for the final presentation / plotting steps.

### 16. The Extension System

#### 16.1 How Extensions Work

```sql
-- List available extensions
SELECT * FROM duckdb_extensions();

-- Install (downloads to local cache)
INSTALL icu;

-- Load into current session
LOAD icu;

-- Many extensions auto-load on first use
SELECT * FROM 's3://bucket/file.parquet';  -- auto-loads httpfs
```

#### 16.2 Official Extensions

```
CORE EXTENSIONS (shipped with DuckDB):
   parquet         Parquet read/write (actually built-in now)
   json            JSON types + reading
   icu             Unicode / internationalization
   fts             Full-text search
   httpfs          HTTP, S3, GCS, Azure
   sqlite_scanner  Query SQLite files
   postgres_scanner Query Postgres databases
   mysql_scanner   Query MySQL databases
   
LAKEHOUSE / ANALYTICS:
   iceberg         Read/write Iceberg tables
   delta           Read Delta tables
   ducklake        Native DuckLake format
   
STRING / TEXT:
   fts             Full-text search
   excel           Read .xlsx files
   
SPATIAL / GEO:
   spatial         Geometry types, GEOS/GDAL functions
   
STATISTICS / ML:
   tpch            TPC-H benchmark generator
   tpcds           TPC-DS benchmark generator
```

#### 16.3 Community Extensions

```
DuckDB has a community extensions registry.
Add third-party extensions with:

   INSTALL httpserver FROM community;
   LOAD httpserver;

Popular community extensions:
   • httpserver      — expose DuckDB as a REST API
   • prql            — write queries in PRQL instead of SQL
   • lindel          — lossless image compression
   • quack           — pluggable duckdb extension (educational)
```

#### 16.4 Querying Other Databases

```sql
-- Query Postgres as if it were a DuckDB table
INSTALL postgres_scanner;
LOAD postgres_scanner;

ATTACH 'host=db.example.com port=5432 dbname=prod user=analyst' 
    AS pg (TYPE POSTGRES, READ_ONLY);

SELECT * FROM pg.public.orders WHERE date = TODAY();

-- Join Postgres data with local Parquet
SELECT o.*, c.name
FROM pg.public.orders o
JOIN 'customers.parquet' c ON o.customer_id = c.id;
```

#### 16.5 Writing Your Own Extensions

Extensions are written in C++ against the DuckDB extension API. Common reasons to write one:
- Custom data source (proprietary format)
- Custom aggregations (business-specific)
- Hardware-specific SIMD routines

For most teams, the built-in extensions cover 99% of needs.

### 17. MotherDuck & Hosted DuckDB

#### 17.1 The MotherDuck Thesis

```
MotherDuck = DuckDB as a hosted, multi-user service.

Founded 2022 by Jordan Tigani (ex-BigQuery) + DuckDB Labs collaboration.

Pitch: 
   "DuckDB is amazing on one machine. 
    MotherDuck gives you DuckDB in the cloud,
    with multi-user access, shared data, and hybrid execution."
```

#### 17.2 Hybrid Execution

```
MotherDuck's key innovation: 
   Queries can run partly on YOUR machine, partly on THEIR cloud.

   Local DuckDB  ←→  Cloud DuckDB
      Data on local SSD        Shared data in cloud storage
      CPU for small queries    CPU for big queries
      
The optimizer decides which engine runs which part of the query.

Example:
   Query joins a 10 GB local file with a 500 GB cloud table.
   10 GB local-join → local DuckDB
   500 GB aggregation → cloud DuckDB
   Final merge → whichever is faster
```

#### 17.3 When MotherDuck Fits

```
GOOD FIT:
   ✓ Small team sharing DuckDB-sized data
   ✓ Analysts who want DuckDB UX without managing infrastructure
   ✓ Hybrid local + cloud workflows
   ✓ Teams coming off Snowflake/BigQuery looking for cheaper option

NOT A FIT:
   ✓ PB-scale distributed workloads (still want BigQuery/Snowflake)
   ✓ High-concurrency serving (ClickHouse better)
   ✓ Teams that need deep Snowflake-specific features (data sharing, etc.)
```

#### 17.4 Pricing (Approximate)

```
MotherDuck pricing scales with:
   • Compute units consumed
   • Storage retained in cloud
   • Users

Typical small team (5 users, 500 GB cloud data, moderate query volume):
   $200-500/month

Compare: equivalent Snowflake deployment might run $2-5K/month.

(Prices subject to change; verify with vendor.)
```

### 18. dbt-duckdb

#### 18.1 The Adapter

dbt-duckdb is a mature adapter maintained by Josh Wills (ex-Google, ex-Slack):

```yaml
# profiles.yml
my_project:
  target: dev
  outputs:
    dev:
      type: duckdb
      path: '/path/to/my.duckdb'
      extensions:
        - httpfs
        - iceberg
```

#### 18.2 Why Use dbt-duckdb

```
GOOD FITS:
   ✓ Small-to-medium dbt projects (up to ~1000 models)
   ✓ Local development before deploying to prod warehouse
   ✓ CI/CD testing of dbt models
   ✓ Teams prototyping dbt before committing to a warehouse
   ✓ Learning dbt without a cloud account
```

#### 18.3 The DuckLake + dbt-duckdb Combination

```
Modern pattern (2025+):

   Source data: Parquet / Iceberg in S3
      ↓
   dbt-duckdb models: read sources, apply transformations
      ↓
   Output: DuckLake tables (multi-writer lakehouse)
      ↓
   Downstream: BI tools / Snowflake / other engines read DuckLake

Advantages:
   • Cheap compute (DuckDB on EC2)
   • Open format storage (Parquet)
   • Multi-engine compatible (Snowflake can also read)
```

#### 18.4 Materialization Modes

```
dbt-duckdb supports:
   view         Not materialized; executed on each query
   table        Full rebuild each run
   incremental  Only process new/changed records
   external     Write to Parquet files (via COPY)

external is DuckDB-specific and powerful:
   {{ config(materialized='external', 
             location='s3://bucket/warehouse/my_model.parquet') }}
   
   Write transformed data to Parquet in cloud storage.
   Downstream engines read it directly.
```

> **🎯 Interview Angle:** Q: Would you use dbt-duckdb in production?<br>
> **Staff:** "Yes, in the right spot. For small-to-medium analytics (sub-500 GB working set, 5-50 analysts, batch pipelines), dbt-duckdb on an EC2 instance can replace a Snowflake deployment at a fraction of the cost. We'd write to Parquet or DuckLake, so the output is portable. For large analytics or high-concurrency serving, I'd keep dbt-snowflake or dbt-databricks. The deciding factor is scale and concurrency, not the dbt adapter itself."

---

## PART VI — DUCKLAKE

### 19. The DuckLake Thesis

#### 19.1 The Problem DuckLake Solves

```
EXISTING LAKEHOUSE FORMATS (Iceberg, Delta, Hudi) all share a design:
   Data files: Parquet in object storage
   Metadata: files in object storage (manifest files, transaction logs)
   
This works but has issues:
   1. METADATA SCALING
      For a table with many partitions and snapshots, the metadata
      itself becomes huge (thousands of manifest files).
      
   2. SMALL CHANGES ARE EXPENSIVE
      Adding one row = write new Parquet file + new manifest file 
      + update manifest list + commit. 
      
   3. CONCURRENT WRITES ARE HARD
      Object storage has limited atomic operations.
      Iceberg uses "atomic rename" or REST catalog coordination.
      
   4. OPERATIONAL COMPLEXITY
      Maintenance (compaction, snapshot expiry) is required.
      Eventual consistency bugs on some object stores.
```

#### 19.2 The DuckLake Insight

```
"We already have a technology that solves consensus, concurrency, and 
small-record updates well: a transactional database."

Store the METADATA in a SQL database (Postgres, MySQL, SQLite, DuckDB).
Store the DATA as Parquet files in object storage.

Result:
   • Metadata operations are SQL transactions (ACID, fast, consistent)
   • Multi-writer coordination is handled by the database
   • Small changes become cheap database inserts
   • Compaction is a database operation + Parquet rewrite
```

#### 19.3 The Published Manifesto (May 2025)

The DuckLake manifesto (Mühleisen et al., May 27, 2025) argued:

```
"The lakehouse community invented complex file-based metadata formats
to solve problems that databases solved 40 years ago. By putting metadata
back in a database, we recover decades of research on transactions,
indexing, and concurrency — while keeping the open data format benefits."
```

#### 19.4 Production Status (2026)

```
Timeline:
   2025 May: Manifesto + initial release
   2025-2026: Development through 108+ PRs, community adoption
   2026 Apr: DuckLake v1.0 — production-ready with 
             backward-compatibility guarantee
   
Current state:
   • Production-ready specification
   • Reference implementation (DuckDB ducklake extension)
   • Iceberg interop (read Iceberg from DuckLake)
   • Multi-database catalog backends (Postgres, MySQL, SQLite)
```

### 20. DuckLake Architecture

#### 20.1 The Two-Layer Design

```
┌──────────────────────────────────────────────────────┐
│  CATALOG DATABASE (Postgres, MySQL, SQLite, DuckDB)  │
│                                                       │
│  Tables (regular SQL):                               │
│    • ducklake_table (schema definitions)             │
│    • ducklake_snapshot (snapshots)                   │
│    • ducklake_data_file (file manifest)              │
│    • ducklake_delete_file (deletion vectors)         │
│    • ducklake_file_column_stats (zone maps)          │
│    • ducklake_column (column definitions)            │
│                                                       │
│  All metadata operations: SQL INSERT/UPDATE/DELETE   │
│  Full ACID transactions                              │
└──────────────────────────────────────────────────────┘
                        │
                        │ pointers (filenames)
                        ▼
┌──────────────────────────────────────────────────────┐
│  OBJECT STORAGE (S3, GCS, Azure Blob, local FS)      │
│                                                       │
│  Parquet files:                                       │
│    data/table_1/file_001.parquet                     │
│    data/table_1/file_002.parquet                     │
│    data/table_2/file_001.parquet                     │
│                                                       │
│  Delete files:                                        │
│    deletes/table_1/delete_001.parquet                │
└──────────────────────────────────────────────────────┘
```

#### 20.2 Transactions

```
TRADITIONAL ICEBERG WRITE:
   1. Write Parquet data file(s)
   2. Write new manifest file (Avro)
   3. Write new manifest list
   4. Update metadata.json atomically (rename or REST call)
   5. Commit

DUCKLAKE WRITE:
   1. Write Parquet data file(s)
   2. BEGIN TRANSACTION in catalog DB
   3. INSERT INTO ducklake_data_file (file info)
   4. INSERT INTO ducklake_snapshot (new snapshot)
   5. COMMIT

Fewer round-trips, standard database ACID, easier to reason about.
```

#### 20.3 Reading a DuckLake Table

```sql
-- Attach a DuckLake table
ATTACH 'ducklake:postgres:host=db.example.com dbname=catalog' 
    AS lake (DATA_PATH 's3://bucket/warehouse/');

-- Query it like any table
SELECT * FROM lake.main.orders WHERE date >= '2024-01-01';

-- Time travel
SELECT * FROM lake.main.orders AT (VERSION => 42);
SELECT * FROM lake.main.orders AT (TIMESTAMP => '2024-01-01');
```

#### 20.4 Multi-Writer Concurrency

```
Multiple DuckDB processes can write to the same DuckLake table:

   Process A: writes some new Parquet files, commits metadata
   Process B: writes different Parquet files, commits metadata
   
   The catalog DB serializes commits via SQL transactions.
   Each process sees a consistent view based on its snapshot.
   
   This is CLEAN multi-writer behavior that Iceberg has to simulate
   via careful object-storage protocols.
```

#### 20.5 Compaction (Small File Problem)

```
Many small writes create many small Parquet files.
This slows down reads (per-file overhead).

DuckLake compaction:
   1. Query catalog for small files in a table
   2. Read them, merge into a few larger files
   3. In one transaction: 
      • Delete old file records
      • Insert new file records
      • Commit

Simple, atomic, reliable.
```

### 21. DuckLake vs Iceberg vs Delta

#### 21.1 The Comparison

| Aspect | Iceberg | Delta | DuckLake |
|--------|---------|-------|----------|
| **Metadata location** | Files in object storage | Files (JSON log) in object storage | SQL database |
| **Metadata ACID** | File-based, catalog-coordinated | File-based, log-coordinated | Database-ACID |
| **Small writes** | Expensive (per-write files) | Expensive (per-write log entries) | Cheap (DB rows) |
| **Multi-writer** | Via REST catalog | Via log coordination | Via DB serialization |
| **Setup complexity** | Catalog + storage | Storage + log | DB + storage |
| **Dependencies** | Object storage only | Object storage only | Object storage + DB |
| **Maturity** | Very high (since 2018) | Very high (since 2019) | New (v1.0 April 2026) |
| **Multi-engine** | Snowflake, Spark, Trino, DuckDB, Flink, more | Databricks, Spark, DuckDB, Trino | DuckDB (primary), growing |
| **Interoperability** | De facto standard | Delta UniForm reads Iceberg | Reads Iceberg; Iceberg reading DuckLake coming |

#### 21.2 When to Pick Each

```
PICK ICEBERG WHEN:
   ✓ Multi-engine access required (Snowflake + Spark + Trino + ...)
   ✓ Industry-standard choice
   ✓ Already have Polaris / Unity Catalog / Nessie
   ✓ Existing Iceberg investments

PICK DELTA WHEN:
   ✓ Databricks-centric shop
   ✓ Want Databricks optimizations (Z-order, Liquid Clustering)
   ✓ Already have Delta tables

PICK DUCKLAKE WHEN:
   ✓ You already have a database (Postgres is common)
   ✓ High-frequency small writes
   ✓ Simpler operational model preferred
   ✓ DuckDB is the primary query engine
   ✓ Multi-writer with strong consistency matters
```

#### 21.3 The Honest Trade-Off

```
DUCKLAKE'S DOWNSIDE:
   You need a SQL database for the catalog.
   That's one more thing to operate (though many teams already have Postgres).
   
   Ecosystem is newer than Iceberg/Delta.
   Fewer engines can read it (though Iceberg interop closes this).

DUCKLAKE'S UPSIDE:
   Operational simplicity (one familiar component).
   Better small-write performance.
   Fewer edge cases around eventual consistency.
   Database transactions > file atomicity hacks.
```

#### 21.4 The Likely Future

```
2026-2028 projections:
   • Iceberg remains the industry standard for multi-engine lakehouse.
   • DuckLake grows for small-to-mid teams who want simplicity.
   • Delta continues in Databricks-centric shops; UniForm bridges to Iceberg.
   • Catalog interoperability: DuckLake catalogs become Iceberg-compatible,
     allowing other engines to read.
   
The lakehouse format wars are de-escalating.
Future may be: open file format (Parquet) + choice of catalog metadata layer.
```

> **🧠 Principal Mindset:** DuckLake is best understood not as "another lakehouse format" but as a philosophical statement: metadata management is a solved problem (databases), and the industry should stop reinventing it. Even if your organization doesn't adopt DuckLake specifically, the thinking it represents — "put the catalog in a real database" — is likely where the industry is heading. Polaris and Unity Catalog are already moving this direction with their REST-API-backed designs.

---

## PART VII — PRODUCTION PATTERNS

### 22. Performance Tuning

#### 22.1 The Golden Rules

```
1. READ LESS DATA
   • Project only needed columns
   • Push down predicates (WHERE on partition columns)
   • Use Parquet statistics (file-level min/max)

2. AVOID UNNECESSARY WORK
   • APPROX_* for cardinality / quantiles when exactness isn't required
   • Pre-filter before joining
   • Use LIMIT for exploration

3. USE PARALLELISM
   • DuckDB uses all cores by default
   • More cores = linear speedup on parallelizable work (up to I/O/memory bandwidth)

4. CHOOSE THE RIGHT STORAGE
   • .duckdb native: fastest for repeated queries
   • Parquet: cheapest, portable, slower for very selective queries
   • CSV: last resort; parse overhead on each query
```

#### 22.2 Reading EXPLAIN

```sql
-- See query plan
EXPLAIN SELECT * FROM 'data.parquet' WHERE x > 10;

-- See runtime stats
EXPLAIN ANALYZE SELECT * FROM 'data.parquet' WHERE x > 10;
```

Example output interpretation:
```
┌───────────────────────────┐
│         PROJECTION        │ ← columns being emitted
│   x, y, z                 │
├───────────────────────────┤
│          FILTER           │ ← predicate applied in memory
│       x > 10              │
├───────────────────────────┤
│       PARQUET_SCAN        │ ← reading from Parquet file
│   data.parquet            │
│   Projected: x, y, z      │ ← columns pushed down
│   Filters: x > 10         │ ← predicate pushed to Parquet reader
│   Rows read: 10K / 100K   │ ← files/rows pruned
└───────────────────────────┘
```

#### 22.3 Common Optimizations

```sql
-- 1. ORDER DATA BY COMMON FILTER COLUMNS
COPY (SELECT * FROM my_data ORDER BY date, customer_id) 
    TO 'optimized.parquet' (FORMAT PARQUET, ROW_GROUP_SIZE 100000);
-- Parquet stats will now effectively prune on date and customer_id

-- 2. PARTITION BY FOR SELECTIVE QUERIES
COPY (SELECT * FROM events) TO 'events/' 
    (FORMAT PARQUET, PARTITION_BY (year, month));
-- Queries with WHERE year = 2024 AND month = 3 scan 1/N of data

-- 3. USE COLUMNAR-FRIENDLY QUERIES
-- GOOD: SELECT only columns you need
SELECT customer_id, SUM(amount) FROM orders GROUP BY customer_id;

-- BAD: SELECT * forces reading all columns
SELECT * FROM orders;
```

#### 22.4 Threads & Memory

```sql
-- Control parallelism
SET threads = 16;             -- use 16 threads

-- Control memory limit
SET memory_limit = '32GB';    -- cap at 32 GB

-- Spill to disk when exceeded
SET temp_directory = '/mnt/fast-ssd/duckdb-temp';
```

#### 22.5 When Queries Are Slow

```
CHECKLIST:
   1. EXPLAIN ANALYZE — where is time spent?
   2. Is data columnar-friendly? (Parquet > CSV)
   3. Are filters SARGable? (indexed/pruned columns)
   4. Is parallelism active? (threads > 1)
   5. Is memory limit sufficient? (watch for spills)
   6. Is data sorted appropriately? (for Parquet statistics)
   7. Are you reading remote data without caching?
```

### 23. Memory & Resource Management

#### 23.1 The Default Behavior

```
By default, DuckDB:
   • Uses all available CPU cores
   • Sets memory_limit to 80% of total RAM
   • Spills to disk when memory_limit is reached
   • Streams Parquet reads (constant memory for large files)
```

#### 23.2 Out-of-Core Processing

```
Many operations can run on data LARGER than RAM:
   
   ✓ Aggregations (streaming hash)
   ✓ Sorts (external merge sort via disk)
   ✓ Hash joins (partition and spill)
   ✓ Window functions over partitioned data
   
   ⚠ Non-partitioned window functions can require full in-memory sort
   ⚠ Very wide intermediate joins might need more careful tuning
```

#### 23.3 Monitoring Memory Usage

```sql
-- Check current memory usage
PRAGMA database_size;
PRAGMA memory_limit;

-- See per-operator memory from EXPLAIN ANALYZE
```

#### 23.4 Tips for Large Data

```
TIP 1: Project early
   Reduce column count ASAP; downstream operators process less data.

TIP 2: Filter early
   Push WHERE predicates to the earliest point in the query plan.

TIP 3: Pre-aggregate
   If downstream needs averages/sums, aggregate before joining.

TIP 4: Use streaming reads
   Parquet reader streams row groups; doesn't load whole file.

TIP 5: Enable disk spill
   SET temp_directory = '/path/to/fast/ssd';
   
TIP 6: Partition large tables
   Logical partition by date / tenant allows per-partition processing.
```

### 24. When to Use DuckDB (and When Not)

#### 24.1 The Clear Fits

```
EXCELLENT FIT:
   ✓ Notebook analysis (replace Pandas for anything > 1 GB)
   ✓ Local prototyping before deploying to cloud warehouse
   ✓ Small-to-medium batch ETL pipelines (<1 TB working set)
   ✓ Embedded analytics in applications
   ✓ CI/CD testing of dbt projects
   ✓ Edge/mobile analytics (via WASM, iOS, Android)
   ✓ Interactive data exploration tools
   ✓ Serverless analytics (Lambda + DuckDB + S3)

GOOD FIT:
   ✓ Replacement for a modest Snowflake/BigQuery deployment
   ✓ Multi-user team analytics (via MotherDuck)
   ✓ Lakehouse with DuckLake format
   ✓ Dashboarding for small teams
```

#### 24.2 The Honest Non-Fits

```
NOT A FIT:
   ✗ True distributed workloads (PB-scale, multi-TB joins)
   ✗ High-concurrency serving (100+ simultaneous writers)
   ✗ Real-time stream processing
   ✗ OLTP transactional workloads
   ✗ Multi-tenant SaaS back-end (one-writer limit)
   ✗ Interactive dashboards with 10K+ QPS
   
IN THESE CASES, use the right tool:
   Distributed: Spark, Snowflake, BigQuery, Databricks, Trino
   High-concurrency: ClickHouse, Druid, Pinot, StarRocks
   Streaming: Flink, Materialize, RisingWave, Kafka Streams
   OLTP: Postgres, MySQL, CockroachDB
```

#### 24.3 The Decision Matrix

```
Data Size   | Users       | Workload          | Recommended Engine
------------|-------------|-------------------|-------------------
< 1 TB      | 1-50        | Batch/Analytical  | DuckDB
< 1 TB      | 100+        | Real-time         | ClickHouse / MotherDuck
1-10 TB     | 1-20        | Batch/Analytical  | DuckDB on big machine
1-10 TB     | 50+         | BI/Dashboards     | Snowflake/BigQuery/Databricks
10-100 TB   | Any         | Analytical        | Snowflake/BigQuery/Databricks
100 TB+     | Any         | Analytical        | Snowflake/BigQuery/Databricks + lakehouse
Any         | Many        | Streaming         | Flink / Materialize / Kafka
Any         | Many        | OLTP              | Postgres / distributed OLTP
```

#### 24.4 The Hybrid Pattern

Many teams use DuckDB AND something else:

```
PATTERN: DuckDB for development, warehouse for production
   • Prototype dbt models with dbt-duckdb locally
   • CI tests use dbt-duckdb for fast feedback
   • Production runs on dbt-snowflake or dbt-databricks
   • Same SQL across all stages (mostly)

PATTERN: DuckDB as the analyst's local engine
   • Analysts query Parquet / Iceberg on S3 via DuckDB on their laptop
   • Snowflake / BigQuery for production dashboards
   • Choose cost per workload: free local vs paid cloud

PATTERN: DuckDB for serverless analytics
   • Lambda-equivalent functions run DuckDB
   • Query S3 Parquet on-demand
   • No cluster to manage, pay per invocation
```

> **🔥 Production Scenario:** A B2B SaaS data team had three analytics patterns: (1) nightly dashboards (steady workload, 100 analysts), (2) ad-hoc analyst queries (bursty, 10 analysts), (3) customer-facing embedded analytics (real-time, 1000 customers). They architected: Snowflake for pattern 1, DuckDB on dev laptops for pattern 2, ClickHouse for pattern 3. Each tool fit its workload; combined cost was 40% lower than their original "put everything in Snowflake" setup. Lesson: use multiple engines where their shapes match different workloads.

### 25. Deployment Patterns

#### 25.1 Local / Developer

```
Simplest: pip install duckdb
   
Workflow:
   • Analyst opens Jupyter / notebook
   • import duckdb
   • Point at Parquet files on local disk or S3
   • Query

No infrastructure. No credentials beyond S3 access.
```

#### 25.2 Serverless (Lambda / Cloud Functions)

```python
# AWS Lambda handler
import duckdb

def lambda_handler(event, context):
    con = duckdb.connect(':memory:')
    con.execute("INSTALL httpfs; LOAD httpfs;")
    result = con.execute(f"""
        SELECT customer_id, SUM(amount) 
        FROM 's3://bucket/orders/*.parquet'
        WHERE date = '{event['date']}'
        GROUP BY customer_id
    """).fetchall()
    return result

# Deployed: zero infrastructure, pay-per-execution
# Scales: handles bursts automatically
# Cost: pennies per query on moderate datasets
```

#### 25.3 Dedicated Instance

```
EC2 / Compute Engine / Azure VM with DuckDB installed.
   
Setup:
   • Pick instance size matched to your data (RAM = ~2x working set)
   • Install DuckDB
   • Mount S3 / blob storage via DuckDB HTTPFS
   • Run queries via SSH or notebook

Use for:
   • Regular batch workloads
   • Dedicated analyst machines
   • CI/CD runners
```

#### 25.4 Containerized

```yaml
# docker-compose.yml
services:
  duckdb-analytics:
    image: python:3.12-slim
    volumes:
      - ./data:/data
      - ./scripts:/scripts
    command: python /scripts/run_analysis.py
    environment:
      - AWS_ACCESS_KEY_ID
      - AWS_SECRET_ACCESS_KEY
```

For reproducible pipelines in Kubernetes, ECS, or any container platform.

#### 25.5 Hosted (MotherDuck)

```
MotherDuck provides:
   • Multi-user shared databases
   • Cloud + local hybrid execution
   • Authentication / authorization
   • Centralized data governance
   
Setup:
   duckdb> ATTACH 'md:my_database';
   duckdb> SELECT * FROM my_database.main.my_table;

Use for:
   • Team sharing without self-managing infrastructure
   • Enterprises wanting managed DuckDB
```

#### 25.6 Embedded in Applications

```
DuckDB can be embedded in:
   • Desktop apps (Electron + duckdb-node)
   • Mobile apps (iOS Swift binding, Android)
   • Browser apps (DuckDB-WASM, runs in-browser via WebAssembly)
   • Web services (Node.js, Python, Go)

The user never sees DuckDB; it's an implementation detail.
Example: a BI tool that bundles DuckDB for in-browser analytics.
```

---

## PART VIII — INTERVIEW PRACTICE

### 26. The Staff-Level DuckDB Interview

#### 26.1 Common Question Themes

```
1. ARCHITECTURE
   "How does DuckDB execute a query? Walk me through the pipeline."
   
2. COMPARISON
   "DuckDB vs ClickHouse / Snowflake / Spark — when do you pick each?"
   
3. SCALE LIMITS
   "At what point does DuckDB break down? What do you do then?"
   
4. INTEGRATION
   "How does DuckDB fit alongside your existing data stack?"
   
5. OPERATIONAL
   "You're running DuckDB in production. How do you monitor it? 
    How do you handle schema evolution? Multi-user access?"
   
6. LAKEHOUSE
   "Compare DuckLake to Iceberg. When would you pick each?"
   
7. ECONOMICS
   "You're migrating from Snowflake to DuckDB. How do you build 
    the business case? What risks do you flag?"
```

#### 26.2 Worked Example: Architecture Deep-Dive

```
Q: Walk me through what happens when I run:
     SELECT customer_id, SUM(amount) 
     FROM 's3://bucket/orders/*.parquet'
     WHERE date >= '2024-01-01'
     GROUP BY customer_id
     ORDER BY SUM(amount) DESC
     LIMIT 10;

STAFF ANSWER:
   1. PARSE
      SQL parser builds AST. Validates syntax.
   
   2. BIND
      Resolves table 's3://...' to Parquet reader.
      Resolves columns (reads Parquet schema from the first file).
      Validates types.
   
   3. PLAN
      Logical plan: SCAN → FILTER → PROJECT → HASH_AGG → SORT → LIMIT.
      Optimizer:
         • Pushes WHERE date >= '2024-01-01' down to Parquet reader
         • Pushes projection (customer_id, amount) down
         • Rewrites LIMIT + ORDER BY to TOP-K operator (saves memory)
   
   4. EXECUTE
      Pipeline:
         - Parquet reader lists S3 bucket (one listing call)
         - For each file, reads footer (parallel across files)
         - Prunes files/row-groups where date range doesn't match
         - Reads only customer_id and amount columns
         - Decodes columnar data into vector batches (2048 rows each)
         - FILTER operator applies predicate in-batch
         - HASH_AGG builds partial hash table per thread (morsel-driven)
         - Final merge of per-thread partials
         - TOP-K operator maintains heap of top 10
         - Returns 10 rows
   
   5. RESULT
      Fetched by the client.

Entire pipeline executes in parallel across cores via morsel-driven 
parallelism. Memory usage stays bounded by the hash table size + 
intermediate vector batches, not the total data scanned.
```

#### 26.3 Worked Example: Design Problem

```
Q: Design an analytics platform for a 50-person startup. They have 
   100 GB of operational data (Postgres), ~50 GB of events in S3,
   10 analysts, and no dedicated data team. Budget: $500/month.

STAFF ANSWER:

Clarifications first:
   • What are "operational data" use cases? (joins across prod?)
   • Latency requirements? (daily OK? sub-hour?)
   • Any ML / data science? (just BI?)
   • Analyst skill level? (SQL fluent? Python?)

Assuming daily refresh, pure BI, SQL-fluent analysts:

Architecture:
   
   SOURCES:
      • Postgres (prod): read via dbt-duckdb postgres_scanner
      • S3 events: Parquet files
   
   ETL:
      • dbt-duckdb project on an EC2 t3.xlarge (~$150/month)
      • Scheduled by cron / GitHub Actions
      • Reads Postgres + S3 Parquet
      • Writes gold-layer Parquet to S3
   
   STORAGE:
      • Raw sources untouched
      • Gold layer: Parquet in S3 (~$10/month at this scale)
   
   SERVING:
      • Metabase or Preset (open-source BI) on cheap VM (~$30/month)
      • Points at DuckDB or at Parquet directly
      • Analysts query gold models
   
   AD-HOC:
      • Analysts use DuckDB locally (free) against the same S3 Parquet
      • Powerful enough for 50-100 GB queries on a laptop
   
   Total cost: ~$250-400/month (under budget)

Why this works:
   • No warehouse license cost
   • Open file formats = no lock-in
   • Can scale to Snowflake later if needed
   • Analysts get full SQL power locally

Risks I'd flag:
   • Single-writer DuckDB: coordinate ETL writes (easy via cron)
   • Monitoring: need to add observability (Elementary / custom)
   • If org grows to 200 analysts + complex workflows, revisit
```

#### 26.4 The Senior vs Staff Difference

| Senior Answer | Staff Answer |
|--------------|--------------|
| "DuckDB is fast because it's columnar." | "DuckDB is fast because it combines columnar storage with vectorized execution (2048-row batches), morsel-driven parallelism (dynamic work allocation), and SIMD instructions (16+ values per CPU cycle). The MonetDB/X100 paper from 2005 established the technique; DuckDB brought it to a production, embedded form factor." |
| "Pick DuckDB for small data, Spark for big." | "Pick DuckDB when the working set fits comfortably on one machine (up to ~1 TB), the workload is batch/analytical, and concurrency is limited. The crossover to Spark isn't just data size — it's whether distributed shuffle is actually needed. Many 'big' Spark jobs are really small jobs forced through a distributed engine; those are DuckDB candidates. True distributed workloads (multi-TB joins, 100+ concurrent writers) stay on Spark/warehouse." |
| "DuckLake stores metadata in a database." | "DuckLake's thesis is that lakehouse metadata is a solved problem (SQL databases have 40 years of research on transactions, concurrency, indexing). File-based metadata formats like Iceberg's manifest files were invented to work around object-storage limitations. If you already have a database, DuckLake simplifies operations substantially — especially for workloads with high small-write velocity. The trade-off: one more dependency (the catalog DB) and a newer, smaller ecosystem than Iceberg." |

### 27. War Stories

> **🔥 Production Scenario:** **"The Snowflake Migration That Didn't Happen."**
> A mid-sized SaaS company (Series C, ~$2M ARR) was spending $18K/month on Snowflake. A new VP of Engineering wanted to cut costs 50%. The team spent 3 months evaluating migration options. Along the way, they discovered 70% of their Snowflake spend was from 5 analysts running ad-hoc queries on a single warehouse that never auto-suspended. Fixing warehouse configuration brought the bill to $6K/month — a 67% reduction without migration. They also moved 3 nightly batch pipelines to DuckDB-on-Lambda for an extra $500/month savings. Final architecture: Snowflake for shared analytics (now right-sized), DuckDB for isolated batch workloads. Lesson: before changing engines, audit how you're using the current one.

> **🔥 Production Scenario:** **"The Lambda-Scale Analytics."**
> An ad-tech startup needed to compute daily aggregate reports from 20 customer accounts, each with 1-10 GB of event data in their own S3 bucket. Original design: Spark on EMR, one job per customer, costing $4K/month. Replaced with AWS Lambda functions running DuckDB, one invocation per customer. Each job: 2-5 minutes, under $0.10. Monthly cost: $200. The Spark overhead dominated at this per-customer scale. Lesson: when you have many small independent jobs, serverless DuckDB often beats Spark.

> **🔥 Production Scenario:** **"The dbt-duckdb CI Speedup."**
> A data team had a 350-model dbt project running on Snowflake. CI ran all tests on every PR, cost $40 per CI run, took 20 minutes. They migrated the CI pipeline to dbt-duckdb against a sampled subset of production data stored in S3 Parquet. CI runs: 3 minutes, cost: $0.10 per run. Developers merged PRs faster; CI cost dropped $1500/month. Production still ran on Snowflake (correct for their scale). Lesson: use DuckDB for dev/CI even when production stays elsewhere.

> **🔥 Production Scenario:** **"The Memory Spill."**
> A team ran a DuckDB job that worked fine on their 32 GB dev laptop but crashed on a 16 GB EC2 instance in production — same data, same query. Investigation: a window function with a huge partition caused DuckDB to materialize the full partition in memory. Fix: `SET temp_directory = '/mnt/fast-ssd'` to enable disk spill, and `SET memory_limit = '12GB'` to keep headroom for the OS. Query took 2x longer but succeeded reliably. Lesson: DuckDB can process data larger than RAM, but you have to configure it. Dev laptops often "accidentally" fit more in RAM than production instances.

> **🔥 Production Scenario:** **"The Parquet Statistics Save."**
> A team had a Parquet dataset of 5 TB in S3 with 3 months of events, partitioned by date. Queries filtering on date were fast. Queries filtering on customer_id (not a partition column) took 20+ minutes. Fix: re-wrote the Parquet files sorted by customer_id within each date partition, so Parquet column statistics could prune row groups. Customer-id queries dropped to under 1 minute. No change to the query — just to how the data was physically organized. Lesson: Parquet statistics give you a "second-level index" if your data is physically ordered to enable pruning.

> **🔥 Production Scenario:** **"The MotherDuck Hybrid."**
> A startup's analytics ran on local DuckDB — fast on their laptops, free, perfect for 5 analysts. As they grew to 30 analysts and a shared gold dataset of 2 TB, they hit two problems: no shared source of truth, and some queries scanned too much data for individual laptops. Moved shared gold to MotherDuck ($300/month) while keeping local DuckDB for ad-hoc work. The hybrid execution model let analysts' local DuckDB query shared cloud data when needed. Analysts kept the DuckDB UX; the team got multi-user access. Lesson: DuckDB and MotherDuck complement; one isn't a replacement for the other.

### 28. Cheat Sheet & Mental Models

#### 28.1 The One-Minute Pitch

```
DuckDB = SQLite for analytics.

In-process, columnar, vectorized SQL database.
Beats distributed engines on single-node workloads (up to ~1 TB).
Open source, MIT license, no server to manage.
Reads Parquet, CSV, JSON, Iceberg, Delta directly from S3.
Full ACID with its own file format.
Industry-leading performance on ClickBench.

Key insight: the single-node renaissance.
Most queries today don't need distribution; DuckDB proves it.
```

#### 28.2 The Four Lenses — Compressed

```
1. CONCEPT
   In-process OLAP engine. Vectorized columnar execution.
   SQL-first. Reads multiple formats directly.

2. MECHANICS
   Pipeline: parse → bind → optimize → vectorized exec via morsels.
   Storage: columnar row groups + zone maps + SIMD.
   Parallelism: morsel-driven, scales near-linearly.

3. TRADE-OFFS
   Fits: single-node analytics up to ~1 TB.
   Doesn't fit: distributed PB-scale, high concurrency, OLTP.
   Cheaper and simpler than warehouses at modest scale.

4. INTERVIEW
   Know: comparisons, workload sizing, DuckLake thesis.
   Defend: when to use vs when to escalate to Spark/warehouse.
```

#### 28.3 The Cheat Table

| Topic | Key Fact |
|-------|----------|
| **Origin** | CWI Amsterdam, Hannes Mühleisen & Mark Raasveldt, 2019 |
| **License** | MIT (fully open source) |
| **Model** | In-process, like SQLite |
| **Execution** | Vectorized (2048-row batches) + SIMD |
| **Parallelism** | Morsel-driven, scales to 64+ cores |
| **Storage format** | .duckdb (columnar), reads Parquet/CSV/JSON directly |
| **Lakehouse** | DuckLake (May 2025, v1.0 April 2026) |
| **Hosted** | MotherDuck (hybrid local + cloud) |
| **dbt adapter** | dbt-duckdb (mature) |
| **Python integration** | Zero-copy with pandas/polars/arrow |
| **Scale ceiling** | ~1 TB working set on one machine |
| **Concurrency** | One writer, many readers per file |
| **Latest LTS** | DuckDB 1.4 (October 2025): MERGE, Iceberg writes, encryption |
| **Benchmarks** | #1 on ClickBench single-node |

#### 28.4 The Essential SQL

```sql
-- Query files directly
SELECT * FROM 'data.parquet';
SELECT * FROM 's3://bucket/data/*.parquet';
SELECT * FROM read_csv('messy.csv', ignore_errors=true);

-- Friendly SQL
SELECT * EXCLUDE (ssn) REPLACE (UPPER(name) AS name) FROM users;
SELECT customer_id, SUM(amount) FROM orders GROUP BY ALL;
SELECT * FROM orders QUALIFY ROW_NUMBER() OVER (...) = 1;

-- ASOF joins
SELECT * FROM trades t ASOF LEFT JOIN prices p ON t.ts >= p.ts;

-- Nested types
SELECT {'name': 'Alice', 'tags': ['admin']} AS user;
SELECT list_transform([1,2,3], x -> x * 2);

-- Write optimized files
COPY (SELECT ... ORDER BY date, customer_id) 
    TO 'out.parquet' (FORMAT PARQUET);

-- Attach other databases
ATTACH 'host=... dbname=...' AS pg (TYPE POSTGRES, READ_ONLY);
SELECT * FROM pg.public.orders JOIN 'local.parquet' USING(id);
```

#### 28.5 The Mental Model

```
Think of DuckDB as:
   • A SQL "process" that runs where you need it
   • A reader that queries files in place (no ETL)
   • A fast local engine that scales up to one big machine
   • An open-source alternative to small-scale warehouses
   • A programming primitive, not a system

When to reach for it:
   • You have data in Parquet / Iceberg / Delta
   • Your working set fits comfortably on one machine
   • You want SQL power without server infrastructure
   • You're prototyping, embedded, or edge
   • You want to replace Pandas with something faster

When to reach past it:
   • You need distribution (multi-TB joins, multi-region)
   • You need high-concurrency serving (ClickHouse)
   • You need streaming (Flink)
   • You need multi-tenant transactional (Postgres)
```

#### 28.6 The Senior-to-Staff Progression Table

| What a senior says | What a staff engineer says |
|-------------------|---------------------------|
| "DuckDB is for small data." | "DuckDB fits workloads up to ~1 TB working set on a single modern instance. The ceiling has moved up every year as hardware improves; by 2026, that covers 70%+ of analytical workloads in typical companies." |
| "I'd use Spark for 500 GB." | "At 500 GB, Spark's coordination overhead may dominate the actual work. DuckDB on a well-sized EC2 instance would likely be 5-10x cheaper and comparable or faster. I'd benchmark both on the real workload before committing." |
| "Pandas is good enough." | "Pandas starts hitting limits around 1 GB — memory pressure, slow aggregations, no parallelism. For modern Python analytics I'd default to Polars + DuckDB: Polars for DataFrame-style programming, DuckDB for SQL and multi-format reads. Pandas stays for ecosystem interop (plotting, ML libraries that require it)." |
| "DuckLake is just another format." | "DuckLake is a philosophical statement: lakehouse metadata is a solved problem (databases). Whether or not DuckLake specifically wins, the direction — catalog metadata backed by SQL databases rather than file-based manifests — is where the industry seems to be heading. Polaris and Unity Catalog's REST designs are already partway there." |
| "DuckDB is faster than Snowflake." | "On a per-node basis for queries that fit, yes. Snowflake's compute at scale with many warehouses can serve more concurrent users and larger datasets than any single DuckDB instance. The right framing is: DuckDB is 10x cheaper AND faster for workloads that fit; above that it stops fitting and Snowflake wins. Pick based on the shape of YOUR workload." |

> **🧠 Principal Mindset:** DuckDB's rise represents an industry self-correction. For a decade, "distributed by default" was the mantra. DuckDB's success — adopted by Netflix, Shopify, Mozilla, Hugging Face, and dozens of other large organizations — is evidence that we overbuilt. Staff engineers who recognize this early and right-size their architectures save their organizations millions. The lesson transcends DuckDB specifically: match the tool to the workload, not to the hype cycle. In 2026, DuckDB is right most of the time; in 2028, the right default may shift again. The judgment is what matters.

---

[← Back to main](./README.md)
