# Topic 5 — dbt & Data Modeling

> Section 5 of 10 · 66 pages in source PDF · pages 268–333

[← Back to main](./README.md)

---

dbt &amp; Data Modeling: Principal Engineer Notes dbt & Data Modeling Architect-Level Reference Principal / Staff Engineer Interview Notes Materializations · Tests · Snapshots · Macros · Modeling Patterns · Lineage · CI/CD · Performance · Governance The transformation layer of the modern data stack — patterns that scale.

dbt &amp; Data Modeling: Principal Engineer Notes How to use this document dbt is the lingua franca of analytics engineering. It's deceptively simple — Jinja-templated SQL, four materializations, a YAML config — but the patterns that work for 10 models break at 1000. At staff/principal level, interviews test whether you can design a warehouse, define ownership boundaries, scale a dbt project across teams, and recognize when dbt is the wrong tool. ■ Principal mindset: Three staff-level signals: (1) you can defend a dimensional vs wide-table vs Data Vault choice with concrete reasoning, (2) you treat dbt as a software engineering discipline (CI/CD, code review, testing), (3) you know what dbt doesn't do — and what fills those gaps (orchestration, data quality observability, OLAP performance tuning).

## PART I — FOUNDATIONS

#### 1. What dbt Is For

#### 2. Project Structure & Conventions

#### 3. Models, Sources & Refs

#### 4. The Compile → Execute Pipeline

## PART II — MATERIALIZATIONS

#### 5. View, Table, Incremental, Ephemeral

#### 6. Incremental Models Deep Dive

#### 7. Snapshots & SCD Type 2

#### 8. Custom Materializations

## PART III — TESTING & QUALITY

#### 9. Tests: Generic & Singular

#### 11. Data Contracts & Constraints

## PART IV — JINJA & MACROS

#### 12. Jinja Fundamentals in dbt

#### 13. Macros: When to Use Them

#### 14. Packages & The dbt Ecosystem

## PART V — DATA MODELING

#### 15. Dimensional Modeling (Kimball)

#### 16. Wide Tables & One Big Table

#### 17. Data Vault 2.0

#### 18. Activity Schema

#### 19. Slowly Changing Dimensions

#### 20. Choosing a Modeling Style

## PART VI — PROJECT ARCHITECTURE

#### 21. The Layered Model: Staging → Marts

#### 22. Naming, Folder Structure & Conventions

#### 23. Monorepo vs Multi-Project

#### 24. Cross-Project Dependencies (Mesh)

## PART VII — PRODUCTION OPERATIONS

#### 26. Performance & Cost Optimization

#### 27. Lineage, Documentation & Discovery

#### 28. Anti-Patterns at Scale

## PART VIII — INTERVIEW PRACTICE

#### 29. The Staff-Level Modeling Interview

#### 30. War Stories & Cheat Sheet

## PART I — FOUNDATIONS

#### 1. What dbt Is For

```
1.1 The Problem dbt Solves
Before dbt, transformations lived in stored procedures, hand-rolled Python pipelines, BI tool layers, or —
worst — in dashboards themselves. There was no version control, no testing, no lineage, no consistent
development workflow. Two analysts solving the same business question produced different answers.
dbt = data Build Tool
CORE IDEA:
   Write transformations as SQL SELECT statements
   dbt handles CREATE / INSERT / MERGE on your behalf
   Reference other models with ref('other_model') — dbt builds the DAG
   Test, document, deploy as software engineering artifacts
   Materialize the result in your warehouse (Snowflake/BigQuery/etc.)
KEY PRINCIPLE: ELT not ETL
   Load raw into the warehouse first
   Transform IN the warehouse (it's cheap compute)
   dbt is the T
```

```
1.2 What dbt Is (and Isn't)
DBT IS:
  ✓ A SQL transformation framework
  ✓ A DAG of model dependencies
  ✓ A testing framework for data quality
  ✓ A documentation + lineage generator
  ✓ The standard for analytics engineering
DBT IS NOT:
  ✗ An orchestrator (it runs ONCE per invocation; needs Airflow/Dagster/Prefect)
  ✗ An ingestion tool (use Fivetran, Airbyte, Meltano, custom EL)
  ✗ A BI tool (use Looker, Tableau, Mode, Hex)
  ✗ A streaming engine (dbt is batch by design; Materialize / Tinybird for streaming)
  ✗ A data catalog (it has docs but not full catalog features — use DataHub, Atlan, Alation)
  ✗ A data quality observability platform (use Monte Carlo, Bigeye, Soda)
  ✗ A reverse ETL (use Hightouch, Census)
1.3 dbt Core vs dbt Cloud
DBT CORE  — open-source CLI
  pip install dbt-snowflake (or dbt-bigquery, dbt-redshift, dbt-postgres, ...)
  Run with: dbt run, dbt test, dbt build
  Self-hosted; integrate with your own orchestrator
  Free
DBT CLOUD — commercial SaaS by dbt Labs
  Web IDE, built-in scheduler, CI integration, hosted docs, semantic layer
  Per-seat pricing
  Good for: small/medium teams without devops capacity
OPEN-SOURCE ALTERNATIVES TO DBT CLOUD:
   SQLMesh (Tobiko Data) — challenger; better dev semantics
```

```
dbt &amp; Data Modeling: Principal Engineer Notes
   Dagster + dbt — Dagster's asset-based model wraps dbt nicely
   Airflow + dbt-core — most common at scale
```

1.4 The Adapter Ecosystem dbt-<adapter> packages provide warehouse-specific behavior: OFFICIAL (dbt Labs): dbt-snowflake, dbt-bigquery, dbt-redshift, dbt-postgres, dbt-databricks, dbt-spark, dbt-trino COMMUNITY: dbt-duckdb, dbt-clickhouse, dbt-firebolt, dbt-athena, dbt-mssql, dbt-oracle, dbt-fabric THE MODEL SQL IS PORTABLE-ISH: Same model.sql can run on many adapters IF you avoid warehouse-specific syntax. In practice: macros + dispatch handle differences. ■ Interview angle: Q: What's the difference between dbt and Airflow? Senior: 'dbt does transformations, Airflow orchestrates.' Staff: 'They solve different problems. dbt is a SQL transformation framework: write SELECTs, dbt builds a DAG of models and materializes them in your warehouse. Airflow is an orchestrator: schedules tasks, manages dependencies between heterogeneous workloads, handles retries and failures. Together they're the modern stack — Airflow schedules dbt run at the right time and connects it to upstream loads and downstream consumers. dbt without orchestration only works for ad-hoc runs; Airflow without dbt means writing CTAS/MERGE by hand.'

#### 2. Project Structure & Conventions

2.1 The Standard Layout my_dbt_project/ ■■■ dbt_project.yml # project config (REQUIRED) ■■■ profiles.yml # connection config (often outside repo) ■■■ packages.yml # 3rd-party package dependencies ■■■ requirements.txt # Python deps for dbt-core itself ■■■ models/ ■ ■■■ staging/ # 1:1 with sources — light cleanup ■ ■ ■■■ _src_stripe.yml # sources declaration ■ ■ ■■■ _stg_stripe__models.yml # model contracts/tests ■ ■ ■■■ stg_stripe__charges.sql ■ ■ ■■■ stg_stripe__customers.sql ■ ■■■ intermediate/ # business logic stepping stones ■ ■ ■■■ int_orders__joined.sql ■ ■■■ marts/ # final consumable models ■ ■■■ finance/ ■ ■ ■■■ _fct_finance__models.yml ■ ■ ■■■ dim_customers.sql ■ ■ ■■■ fct_revenue.sql ■ ■■■ marketing/ ■■■ seeds/ # static CSV files loaded as tables ■■■ snapshots/ # SCD2 captures ■■■ tests/ # singular SQL tests ■■■ macros/ # reusable Jinja macros ■■■ analyses/ # one-off queries, not materialized ■■■ docs/ # markdown doc blocks 2.2 dbt_project.yml — The Manifest name: 'my_project' version: '1.0.0' config-version: 2 profile: 'my_project' # which profile in profiles.yml to use model-paths: ["models"] seed-paths: ["seeds"] test-paths: ["tests"] snapshot-paths: ["snapshots"] macro-paths: ["macros"] analysis-paths: ["analyses"] target-path: "target" # compiled SQL + run artifacts clean-targets: - "target" - "dbt_packages" # Per-folder configurations models: my_project: staging: +materialized: view # all staging models = views +schema: staging intermediate: +materialized: ephemeral marts: +materialized: table finance: +schema: finance marketing: +schema: marketing

```
dbt &amp; Data Modeling: Principal Engineer Notes
# Vars usable in models
vars:
  start_date: '2020
01'
  finance_email_domain: 'company.com'
2.3 profiles.yml — Connection Config
# Usually ~/.dbt/profiles.yml (NOT in git)
my_project:
  target: dev
  outputs:
    dev:
      type: snowflake
      account: xyz.us-east-1
      user: "{{ env_var('DBT_USER') }}"
      password: "{{ env_var('DBT_PASSWORD') }}"
      role: ANALYTICS_DEV
      database: ANALYTICS_DEV
      warehouse: TRANSFORM_DEV
      schema: dbt_{{ env_var('USER') }}    # per-dev sandbox
      threads: 8
    prod:
      type: snowflake
      account: xyz.us-east-1
      user: "{{ env_var('DBT_PROD_USER') }}"
      private_key_path: "{{ env_var('DBT_PROD_KEY') }}"
      role: ANALYTICS_PROD
      database: ANALYTICS
      warehouse: TRANSFORM_PROD
      schema: analytics
      threads: 32
```

```
2.4 The Folder Conventions
STAGING (stg_*)
   1:1 with source tables (one staging model per source table)
   Rename columns to consistent style (snake_case, common conventions)
   Cast types
   Light validation / filtering of obviously bad rows
   Materialized: view (default; cheap, no storage)
INTERMEDIATE (int_*)
   Business logic stepping stones
   Joins, calculations, intermediate aggregations
   NEVER selected directly by BI / consumers
   Often: ephemeral (no storage; inlined into next model)
MARTS (fct_*, dim_*)
   Final consumable models
   Optimized for BI / analyst access
   Star schema dim_ + fct_ OR wide tables, depending on style
   Materialized: table or incremental
■ Principal mindset: Folder structure is not cosmetic — it encodes your project's contracts. Staging is a contract:
'this is a clean view of source X'. Marts are a contract: 'this is the canonical business view for stakeholders'.
Intermediate is private. Without these boundaries, downstream consumers couple to internal models you can't
refactor without breaking dashboards.
```

#### 3. Models, Sources & Refs

```sql
3.1 A Model is a SELECT
-- models/marts/finance/fct_orders.sql
{{ config(
    materialized='incremental',
    unique_key='order_id',
    on_schema_change='append_new_columns'
) }}
SELECT
    o.order_id,
    o.customer_id,
    o.order_date,
    SUM(li.amount) AS total_amount,
    COUNT(li.line_item_id) AS n_items
FROM {{ ref('stg_orders') }} o
LEFT JOIN {{ ref('stg_line_items') }} li USING (order_id)
WHERE o.status = 'completed'
{% if is_incremental() %}
  AND o.updated_at > (SELECT MAX(updated_at) FROM {{ this }})
{% endif %}
GROUP BY 1, 2, 3
3.2 ref() — The DAG Builder
ref('model_name') compiles to the fully-qualified table reference:
   dev: analytics_dev.dbt_alice.stg_orders
   prod: analytics.staging.stg_orders
WHY THIS MATTERS:
   Each developer has their own schema (no collisions)
   Same code works in dev/staging/prod
   dbt automatically infers DAG dependencies
   dbt knows what's downstream of a model (for cascading rebuilds)
ALWAYS USE ref() — NEVER hardcode table names.
  ✗ SELECT * FROM staging.stg_orders
  ✓ SELECT * FROM {{ ref('stg_orders') }}
3.3 source() — Reading From Raw
# Declare source in YAML
# models/staging/_src_stripe.yml
version: 2
sources:
  - name: stripe                       # logical group name
    database: raw                       # warehouse database
    schema: stripe                      # warehouse schema
    description: "Stripe events loaded by Fivetran"
    loaded_at_field: _fivetran_synced   # for freshness checks
    freshness:
      warn_after: {count: 12, period: hour}
      error_after: {count: 24, period: hour}
    tables:
      - name: charge
        description: "Stripe Charge events"
        columns:
          - name: id
```

```sql
dbt &amp; Data Modeling: Principal Engineer Notes
            tests: [unique, not_null]
# Use in models:
SELECT * FROM {{ source('stripe', 'charge') }}
# Freshness check:
dbt source freshness
# → fails if data hasn't loaded within thresholds
```

3.4 The Model Config Hierarchy dbt resolves config in order (later overrides earlier): 1. dbt_project.yml (project defaults)

```
  2. Individual model YAML (in schema.yml files)
  3. {{ config(...) }} block in the .sql file itself
EXAMPLE:
  # dbt_project.yml
  models:
    my_project:
      marts:
        +materialized: table        # default for all marts
  # models/marts/finance/_models.yml
  models:
    - name: fct_orders
      config:
        materialized: incremental    # override for this model
  # models/marts/finance/fct_orders.sql
  {{ config(materialized='incremental', unique_key='order_id') }}
  ^^ this takes precedence
USE PROJECT-LEVEL for defaults across many models.
USE INLINE for model-specific overrides.
```

3.5 The Selection Syntax dbt run -s model_name # one model dbt run -s tag:nightly # tagged dbt run -s +model_name # model AND all upstream dbt run -s model_name+ # model AND all downstream dbt run -s +model_name+ # model AND all up + down dbt run -s state:modified # everything that changed (vs manifest) dbt run -s state:modified+ # changed + downstream ← KEY for CI dbt run -s path:models/marts/ # by folder dbt run -s package:my_pkg.* # by package dbt run -s tag:finance,tag:nightly # AND dbt run -s tag:finance tag:nightly # OR ■ Interview angle: Q: How does dbt know what to run, and in what order? Staff: Every ref('x') call creates an edge in the DAG. dbt parses all model files, builds a graph, and topologically sorts it. dbt run walks the graph in dependency order, parallelizing within each level up to the thread count. Source nodes are not built — they're declared via source(). On --full-refresh, all models are rebuilt; otherwise incrementals append. The state:modified selector compares to a stored manifest to run only what changed — that's how slim CI works.

#### 4. The Compile → Execute Pipeline

```
4.1 What `dbt run` Actually Does
dbt run lifecycle:
  1. PARSE
     Read dbt_project.yml, all .sql/.yml files
     Build the manifest.json (full project metadata)
     Resolve all ref()/source() calls
     Construct the DAG
  2. COMPILE
     For each model: render Jinja templating
     Produce pure SQL → target/compiled/...
     Wrap in DDL/DML based on materialization
     Produce target/run/... (the actual SQL to execute)
  3. EXECUTE
     Walk DAG in topological order
     For each node: open warehouse connection from thread pool
     Run the compiled SQL
     Collect run results, write to target/run_results.json
  4. ARTIFACTS
     manifest.json:    project state
     run_results.json: per-model status, timing
     graph.gpickle:    DAG
     catalog.json:     post-build columns/types (from `dbt docs generate`)
```

```sql
4.2 Compiled SQL vs Source SQL
SOURCE (models/marts/fct_orders.sql):
  {{ config(materialized='incremental', unique_key='order_id') }}
  SELECT * FROM {{ ref('stg_orders') }}
  {% if is_incremental() %}
    WHERE updated_at > (SELECT MAX(updated_at) FROM {{ this }})
  {% endif %}
COMPILED (target/compiled/my_project/models/marts/fct_orders.sql):
  SELECT * FROM analytics_dev.dbt_alice.stg_orders
  WHERE updated_at > (SELECT MAX(updated_at) FROM analytics_dev.dbt_alice.fct_orders)
EXECUTED (target/run/.../fct_orders.sql) — wrapped in MERGE for Snowflake:
  MERGE INTO analytics_dev.dbt_alice.fct_orders AS DBT_INTERNAL_DEST
  USING (
    SELECT * FROM analytics_dev.dbt_alice.stg_orders
    WHERE updated_at > (SELECT MAX(updated_at) FROM analytics_dev.dbt_alice.fct_orders)
  ) AS DBT_INTERNAL_SOURCE
  ON DBT_INTERNAL_DEST.order_id = DBT_INTERNAL_SOURCE.order_id
  WHEN MATCHED THEN UPDATE SET ...
  WHEN NOT MATCHED THEN INSERT ...
```

```
4.3 The Artifacts You'll Read
target/manifest.json
  THE entire project state — every node, dependency, config, columns, tests
  Used by: dbt docs, lineage tools, slim CI, catalog integrations
  Diff between manifests = what changed
```

```
dbt &amp; Data Modeling: Principal Engineer Notes
target/run_results.json
  Per-model: status (success/error/skip), timing, rows affected, error message
  Used by: monitoring, post-run notifications
target/catalog.json (from dbt docs generate)
  Post-execution: real columns + types in warehouse
  Used by: docs, data catalog tools
target/graph.gpickle / graph_summary.json
  The DAG itself, in a more graph-friendly form
```

4.4 The Common Commands dbt run # build all models dbt run -s my_model # build one dbt run --full-refresh # rebuild even incrementals from scratch dbt run --threads 16 # override thread pool size dbt test # run all tests dbt build # run + test + snapshot + seed, in DAG order # (PREFERRED in production) dbt seed # load CSV files dbt snapshot # capture SCD2 snapshots dbt source freshness # check source freshness dbt compile # compile without executing dbt parse # parse only (validate) dbt docs generate # build docs site (uses manifest + catalog) dbt docs serve # serve docs locally dbt deps # install packages.yml dbt debug # check connection / configuration dbt clean # delete target/ and dbt_packages/ dbt list -s tag:finance # show models matching selector ■ Tip: Use dbt build over dbt run + dbt test separately. build tests each model immediately after it's built, and SKIPS downstream models if upstream tests fail — preventing bad data propagation. It's the production-grade command.

## PART II — MATERIALIZATIONS

#### 5. View, Table, Incremental, Ephemeral

```sql
5.1 The Four Built-in Materializations
Type
What it does
Best for
view
CREATE VIEW
Light staging, no caching
table
CREATE TABLE AS
Marts, frequent reads
incremental
MERGE / DELETE+INSERT for new rows
Large, append-mostly facts
ephemeral
Inlined as CTE in downstream
Reusable logic, no storage
5.2 View — The Cheap Default
{{ config(materialized='view') }}
SELECT a, b, c FROM {{ source('raw', 'orders') }}
WHEN TO USE:
  ✓ Staging models (just renaming/casting)
  ✓ Models that are small and read rarely
  ✓ Logic that should always reflect upstream changes
DON'T USE WHEN:
  ✗ View is queried by BI tools frequently — every query re-runs the SELECT
  ✗ Underlying query is expensive
  ✗ You need a materialized snapshot of point-in-time data
```

```
5.3 Table — Materialized
{{ config(materialized='table') }}
SELECT ... -- materialized as a full table, dropped + recreated each run
WHEN TO USE:
  ✓ Marts read frequently by BI
  ✓ Aggregations / joins that are expensive to recompute on every query
  ✓ Small-to-medium tables (full refresh is fast)
DON'T USE WHEN:
  ✗ Table is huge and full rebuild takes hours
  ✗ Underlying source data changes infrequently (consider materialized view)
  ✗ You need only new rows since last run (use incremental)
```

```
5.4 Ephemeral — Inlined
{{ config(materialized='ephemeral') }}
SELECT ... -- never written to warehouse; inlined as CTE in downstream models
WHEN TO USE:
  ✓ Intermediate transformations used in only 1-2 downstream models
  ✓ Reusable logic without storage cost
```

```
dbt &amp; Data Modeling: Principal Engineer Notes
  ✓ Splitting complex queries into readable pieces
DON'T USE WHEN:
  ✗ Used by many downstream models (CTE inlines repeat compilation)
  ✗ Underlying query is expensive (re-runs every time)
  ✗ You want to test it directly (ephemeral models can't be tested standalone)
5.5 Incremental — The Workhorse for Big Tables
Incremental models are dbt's answer to 'don't recompute 5 years of data every night'. Instead, dbt computes
only NEW or CHANGED rows and merges them into the existing table. See Chapter 6 for the full deep dive.
```

```
5.6 The Decision Tree
IS THIS A STAGING MODEL?
  → view (default)
IS THIS A MART USED IN BI?
  IS IT SMALL (< 10M rows) AND CHEAP TO REBUILD (< 1 min)?
    → table (rebuilds nightly; simple)
  IS IT HUGE AND ONLY APPENDS?
    → incremental with append strategy
  IS IT HUGE WITH UPDATES (events with state changes)?
    → incremental with merge strategy
IS IT USED IN ONE OTHER MODEL?
  → ephemeral (avoid materializing if reused only once)
IS IT REFERENCE DATA / LOOKUPS?
  → table (small, stable, fast)
■ Interview angle: Q: When would you choose table over view, or view over incremental?
Staff: Three dimensions: cost-per-build, cost-per-query, freshness. View = no build cost, high query cost, always
fresh. Table = build cost, low query cost, fresh as of last build. Incremental = low build cost (only new rows), low
query cost, fresh as of last build. For staging models, view almost always wins — the cost-per-query is paid by
downstream models in the same dbt run, then they're materialized as tables. For marts, table if small, incremental if
large. The mistake teams make: defaulting all marts to table even when they're 500GB and growing — the nightly
rebuild eats warehouse credits unnecessarily.
```

#### 6. Incremental Models Deep Dive

```sql
6.1 The Anatomy
{{ config(
    materialized='incremental',
    unique_key='order_id',
    incremental_strategy='merge',       # or 'append' / 'delete+insert' / 'insert_overwrite'
    on_schema_change='append_new_columns'
) }}
SELECT
    order_id,
    customer_id,
    order_date,
    status,
    updated_at,
    -- ...
FROM {{ source('raw', 'orders') }}
{% if is_incremental() %}
  WHERE updated_at >= (SELECT MAX(updated_at) FROM {{ this }})
{% endif %}
```

6.2 The Four Incremental Strategies Strategy What it does When to use append INSERT new rows; no dedup Pure append, no updates merge MERGE INTO with unique_key (update if exists) Has updates; default delete+insert DELETE rows that will be re-inserted; INSERT Bulk replace of changed keys insert_overwrite Overwrite specific partitions Partitioned tables (BQ/Spark)

```sql
6.3 The Append Strategy
{{ config(materialized='incremental', incremental_strategy='append') }}
SELECT *
FROM {{ source('raw', 'events') }}
{% if is_incremental() %}
  WHERE event_time >= (SELECT MAX(event_time) FROM {{ this }})
{% endif %}
GUARANTEES:
  ✓ Fastest strategy (pure INSERT)
  ✓ No locking complications
  ✗ NO DEDUPLICATION — duplicates possible on overlap
USE WHEN:
   Source is append-only (events, logs)
   You handle dedup downstream or it doesn't matter
   You can be PRECISE about the WHERE clause boundary
```

```
6.4 The Merge Strategy
{{ config(
    materialized='incremental',
    unique_key='order_id',                # required for merge
    incremental_strategy='merge'
) }}
```

```sql
dbt &amp; Data Modeling: Principal Engineer Notes
SELECT order_id, status, amount, updated_at
FROM {{ source('raw', 'orders') }}
{% if is_incremental() %}
  WHERE updated_at >= (SELECT MAX(updated_at) FROM {{ this }})
{% endif %}
→ Compiles to MERGE INTO ... ON order_id = order_id
   WHEN MATCHED THEN UPDATE SET ...
   WHEN NOT MATCHED THEN INSERT ...
GUARANTEES:
  ✓ Idempotent (same key updates in place)
  ✓ Handles late-arriving updates
  ✗ Slower than append (MERGE is more expensive)
  ✗ Requires unique_key
USE WHEN:
   Entity events where state changes (order status updates)
   CDC sources with updates
   You want idempotent re-runs
```

```sql
6.5 The Lookback Pattern (Critical for Late-Arriving Data)
PROBLEM:
  Events arriving late (1-hour delay) get missed by:
    WHERE event_time >= MAX(event_time)
SOLUTION: include a LOOKBACK window
{% if is_incremental() %}
  WHERE event_time >= (
    SELECT DATEADD(hour, -3, MAX(event_time))   -
hour lookback
    FROM {{ this }}
  )
{% endif %}
  + Use MERGE strategy with unique_key so re-processed events upsert.
THIS PATTERN IS THE DIFFERENCE BETWEEN A WORKING AND A BROKEN incremental.
  Without lookback: silent data loss for any source with reordering or lateness.
  With lookback + merge: idempotent, late-arrival-safe.
```

```sql
6.6 The Insert Overwrite Strategy
# BigQuery / Databricks / Spark with partitioned tables
{{ config(
    materialized='incremental',
    incremental_strategy='insert_overwrite',
    partition_by={'field': 'event_date', 'data_type': 'date'}
) }}
SELECT event_date, user_id, ...
FROM {{ source('raw', 'events') }}
{% if is_incremental() %}
  WHERE event_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 3 DAY)
{% endif %}
→ Atomically OVERWRITES the partitions for event_date >= 3 days ago.
USE WHEN:
   Daily partitioned tables (most BigQuery facts)
   Late-arriving data within a bounded window
   You want idempotency without MERGE's cost
```

```
6.7 Schema Changes
on_schema_change behavior:
  ignore             # default; new columns silently dropped
  append_new_columns # ALTER TABLE ADD COLUMN for additions (RECOMMENDED)
  sync_all_columns   # add new, drop missing
  fail               # error if schema changes
ADD A COLUMN to an existing incremental model:
  {{ config(on_schema_change='append_new_columns') }}
  → next run adds the column; old rows have NULL
■ Production scenario: A team's incremental model used WHERE updated_at >= MAX(updated_at) with no
lookback and 'append' strategy. Source system occasionally sent events out of order (1-2 minute reordering at the
message bus). Over 6 months, 0.3% of events were silently lost — about 50K rows in a 200M-row fact. Discovered
when reconciling against the source's own totals. Fix: switched to merge strategy with a 6-hour lookback. Re-ran a
backfill to recover. Lesson: always assume late-arriving data exists, even when the team swears it doesn't.
■ Interview angle: Q: Walk me through designing an incremental model for an events table that gets 10M new rows
daily, with occasional late arrivals up to 24 hours.
Staff: Strategy: insert_overwrite if BigQuery (partition by event_date), merge with unique_key if Snowflake.
Lookback window of 48 hours (2× the worst late-arrival). Compose unique_key as (event_id) or (user_id,
event_time) — whatever's actually unique. Add a freshness test on the staging source and a recency test on the fact
itself. On the first build (--full-refresh), expect it to take much longer; we'll size the warehouse up for that one
run. In CI, slim build with state:modified+ so we don't rebuild this 10B-row table on every PR.
```

#### 7. Snapshots & SCD Type 2

7.1 What Snapshots Solve Source systems usually overwrite state. The orders table has the CURRENT status of every order — not the history. If you need 'what was the status as of last Tuesday', you need to capture state OVER TIME. dbt snapshots build SCD Type 2 tables automatically.

```
7.2 The Snapshot Definition
-- snapshots/orders_snapshot.sql
{% snapshot orders_snapshot %}
{{ config(
    target_schema='snapshots',
    unique_key='order_id',
    strategy='timestamp',
    updated_at='updated_at',
    invalidate_hard_deletes=True
) }}
SELECT *
FROM {{ source('raw', 'orders') }}
{% endsnapshot %}
WHAT IT PRODUCES:
  Table: snapshots.orders_snapshot
  Adds columns:
    dbt_valid_from     - when this version became current
    dbt_valid_to       - when this version was superseded (NULL = current)
    dbt_scd_id         - hash for uniqueness
    dbt_updated_at     - timestamp of capture
```

```sql
7.3 The Two Strategies
TIMESTAMP STRATEGY (preferred when source has reliable updated_at)
   Captures change when updated_at column changes
   Cheap, accurate
   Requires source to have a maintained updated_at
CHECK STRATEGY (when no updated_at)
   Hashes specified columns each run; new hash = new version
   Slower (compares column values, not a single timestamp)
   Useful for tables without change tracking
{% snapshot orders_check %}
{{ config(
    strategy='check',
    unique_key='order_id',
    check_cols=['status', 'amount', 'customer_id']
) }}
SELECT * FROM {{ source('raw', 'orders') }}
{% endsnapshot %}
```

7.4 Querying a Snapshot -- Current state only SELECT * FROM {{ ref('orders_snapshot') }} WHERE dbt_valid_to IS NULL -- Historical: state as of a specific date

```sql
dbt &amp; Data Modeling: Principal Engineer Notes
SELECT * FROM {{ ref('orders_snapshot') }}
WHERE dbt_valid_from <= '2024
15'
  AND (dbt_valid_to IS NULL OR dbt_valid_to > '2024
15')
-- Join a fact to dim at the time of the fact
SELECT f.*, d.*
FROM {{ ref('fct_orders') }} f
JOIN {{ ref('orders_snapshot') }} d
  ON f.order_id = d.order_id
  AND f.order_date BETWEEN d.dbt_valid_from
                       AND COALESCE(d.dbt_valid_to, '9999
31')
```

```sql
7.5 Snapshot Best Practices
1. RUN SNAPSHOTS BEFORE MODELS
   dbt snapshot && dbt run
   Or use dbt build (which orders correctly)
2. SNAPSHOT THE RAW, NOT THE TRANSFORMED
   Snapshot sources before any transformation
   You can rebuild marts from the snapshot if logic changes
3. invalidate_hard_deletes=true
   Mark rows that disappear from source as deleted
   Without this, deleted rows show as "still current" forever
4. SCHEDULE FREQUENTLY
   Snapshots only capture changes since the last run
   If you snapshot daily but the source changes hourly,
   you miss intermediate states
5. NEVER MODIFY A SNAPSHOT MANUALLY
   The history table is the truth. Treat it as append-only.
   To "fix" bad data, snapshot a corrected source — don't update history.
6. CONSIDER SOURCE CDC INSTEAD
   If your source has CDC (Debezium → Kafka → warehouse), use that.
   dbt snapshots are a backstop when CDC isn't available.
■ Principal mindset: Snapshots are dbt's most underrated feature. They give you cheap, reliable history without
setting up CDC pipelines. The mistake teams make: snapshotting TOO LATE — capturing dim_customer after
transformation. The right pattern: snapshot raw sources at the freshness you need, then build transformations on
top. The snapshot is the durable record; the marts are the consumable views.
```

#### 8. Custom Materializations

```
8.1 When You Need One
BUILT-IN MATERIALIZATIONS COVER 95% of needs.
CUSTOM MATERIALIZATION WORTHWHILE when:
   Specific warehouse feature not exposed (Snowflake Iceberg tables,
    BigQuery time-travel, Delta Lake CHECK constraints)
   Highly optimized incremental pattern (e.g., insert into specific clusters)
   Atomic blue-green table swaps for zero-downtime publishing
   Materialize as a streaming view (real-time materialization)
EXAMPLE PACKAGES THAT ADD CUSTOM MATERIALIZATIONS:
  dbt-utils                   adds: insert_by_period
  dbt-materialized-views      adds: materialized_view (where supported)
  dbt-event-logging           adds: snapshot-style audit
  custom internal             adds: blue-green publish, archive-and-rebuild
```

```
8.2 The Anatomy of a Materialization
-- macros/materializations/my_blue_green.sql
{% materialization blue_green, default %}
  {%- set target_relation = this -%}
  {%- set staging_relation = make_temp_relation(target_relation) -%}
  -- 1. Build into staging
  {% call statement('main') -%}
    CREATE OR REPLACE TABLE {{ staging_relation }} AS (
      {{ sql }}
    )
  {%- endcall %}
  -- 2. Run tests against staging (handled by dbt build framework)
  -- 3. Atomic swap
  {% call statement('swap') -%}
    ALTER TABLE {{ staging_relation }} RENAME TO {{ target_relation }}
    -- (Snowflake: SWAP WITH for atomic; varies by adapter)
  {%- endcall %}
  {{ return({'relations': [target_relation]}) }}
{% endmaterialization %}
USE IN A MODEL:
  {{ config(materialized='blue_green') }}
  SELECT ...
```

```
8.3 The Big Three Custom Patterns
1. BLUE-GREEN PUBLISHING
   Build new version in staging, atomic rename to prod
   Zero-downtime for BI consumers reading the table
2. INSERT-BY-PERIOD
   Loop over time periods (days/weeks), insert each separately
   Avoids huge transactions; bounded memory; retryable per period
3. MATERIALIZED VIEW (warehouse-native)
   Snowflake / BigQuery / Redshift have native MVs
   Custom materialization wraps the warehouse syntax cleanly
```

dbt &amp; Data Modeling: Principal Engineer Notes ■■ Warning: Custom materializations are powerful but operationally expensive — every engineer must understand them. Default to built-in materializations + a package if you need exotic behavior. Roll your own only when no package fits and the use case is critical.

## PART III — TESTING & QUALITY

#### 9. Tests: Generic & Singular

```
9.1 Two Kinds of Tests
GENERIC TESTS
  Declared in YAML; parameterized; reusable
  Built-in: unique, not_null, accepted_values, relationships
  Custom: anything you implement as a macro
SINGULAR TESTS
  One-off SQL queries in tests/
  Each test = a SELECT that returns rows; non-empty = failure
```

```yaml
9.2 Built-in Generic Tests
# models/marts/_models.yml
version: 2
models:
  - name: fct_orders
    columns:
      - name: order_id
        tests:
          - unique
          - not_null
      - name: status
        tests:
          - accepted_values:
              values: ['pending', 'shipped', 'delivered', 'cancelled']
      - name: customer_id
        tests:
          - not_null
          - relationships:
              to: ref('dim_customers')
              field: customer_id
# Run:
dbt test                    # all
dbt test -s fct_orders      # one model's tests
dbt test --select test_type:generic
```

```
9.3 Singular Tests
-- tests/assert_revenue_is_positive.sql
SELECT order_id, amount
FROM {{ ref('fct_orders') }}
WHERE amount < 0
-- If this query returns ANY rows, the test FAILS.
-- More expressive example
-- tests/assert_daily_revenue_is_within_3_sigma.sql
WITH daily AS (
    SELECT order_date, SUM(amount) AS revenue
```

```sql
dbt &amp; Data Modeling: Principal Engineer Notes
    FROM {{ ref('fct_orders') }}
    GROUP BY order_date
),
stats AS (
    SELECT AVG(revenue) AS mean, STDDEV(revenue) AS std
    FROM daily
    WHERE order_date < CURRENT_DATE
)
SELECT d.*
FROM daily d, stats s
WHERE d.order_date = CURRENT_DATE - 1
  AND (d.revenue < s.mean - 3*s.std OR d.revenue > s.mean + 3*s.std)
```

```yaml
9.4 Test Severity
# Tests can warn or error
models:
  - name: fct_orders
    columns:
      - name: order_id
        tests:
          - unique:
              severity: error             # fails the build (default)
          - not_null:
              severity: warn              # warns but continues
# Useful for:
#  - "should be unique but exceptions exist" → warn
#  - "absolutely must hold for downstream" → error
# Combine with where filters:
- not_null:
    where: "status != 'cancelled'"        # only check non-cancelled
```

9.5 Custom Generic Tests # macros/test_at_least_one.sql {% test at_least_one(model, column_name) %} SELECT 1 FROM {{ model }} HAVING COUNT({{ column_name }}) = 0 {% endtest %} # Now use in YAML: columns: - name: amount tests: - at_least_one GENERIC TESTS = parameterized SQL that returns failing rows. Empty result = pass. Non-empty = fail.

```
9.6 Test Organization at Scale
TIER YOUR TESTS:
  CRITICAL (every PR, every run, BLOCKING)
    Primary keys not null + unique
    FK relationships intact
    Status enums valid
    No future-dated events
    Revenue within reasonable bounds
  IMPORTANT (every run, WARN)
```

```sql
dbt &amp; Data Modeling: Principal Engineer Notes
    Row count not less than 80% of previous day
    Specific business invariants
  NICE-TO-HAVE (nightly, info-only)
    Statistical anomaly detection
    Drift from baseline distributions
    Correlation checks across tables
This avoids 'test fatigue' where every PR has 50 warnings and engineers ignore all.
■ Interview angle: Q: How do you prevent bad data from propagating downstream in dbt?
Staff: Use dbt build instead of run+test separately. build runs each model, then runs its tests immediately. If a
test fails, downstream models are SKIPPED for this run — preventing bad data from reaching marts. Combine with:
(1) tier-1 tests on PKs and FKs as errors; (2) freshness checks on sources; (3) incremental rebuild markers so
downstream knows what's stale; (4) CI that runs dbt build -s state:modified+ against a warehouse-loaded
sample. The naive 'run, then test' separation lets failed models flow into dashboards.
```

```sql
dbt &amp; Data Modeling: Principal Engineer Notes
10. dbt-utils & dbt_expectations
10.1 dbt-utils — The Standard Toolkit
packages.yml:
  - package: dbt-labs/dbt_utils
    version: [">=1.1.0", "<2.0.0"]
dbt deps
KEY MACROS:
  generate_series(upper_bound)        — sequence of integers
  star(from)                           — SELECT all columns except some
  unpivot                              — wide → long
  pivot                                — long → wide
  get_column_values(table, column)     — distinct values as Jinja list
  generate_surrogate_key(['col1','col2']) — md5 hash of multiple cols
  group_by(n)                          — GROUP BY 1, 2, ..., n
  current_timestamp_in_utc             — portable
KEY TESTS:
  - dbt_utils.equal_rowcount: { compare_model: ref('other_model') }
  - dbt_utils.expression_is_true:
      expression: "amount > 0"
      where: "status = 'completed'"
  - dbt_utils.unique_combination_of_columns:
      combination_of_columns: ['order_id', 'line_item_id']
  - dbt_utils.recency:
      datepart: day
      field: created_at
      interval: 1
10.2 dbt_expectations — Great Expectations for dbt
packages.yml:
  - package: calogica/dbt_expectations
    version: [">=0.10.0", "<0.11.0"]
400+ TESTS inspired by Great Expectations:
  - dbt_expectations.expect_column_values_to_be_between:
      min_value: 0
      max_value: 100000
  - dbt_expectations.expect_table_row_count_to_be_between:
      min_value: 1000
  - dbt_expectations.expect_column_value_lengths_to_be_between:
      min_value: 5
      max_value: 50
  - dbt_expectations.expect_column_proportion_of_unique_values_to_be_between:
      min_value: 0.95
  - dbt_expectations.expect_column_distinct_count_to_equal_other_table:
      compare_model: ref('source_dim')
      compare_column_name: id
10.3 dbt_audit_helper
For COMPARING old vs new versions of a model (during refactors):
  {{ audit_helper.compare_queries(
       a_query=ref('old_model'),
```

```
dbt &amp; Data Modeling: Principal Engineer Notes
       b_query=ref('new_model'),
       primary_key='id'
  ) }}
  Returns: rows that differ between the two queries.
  Use to: validate refactors don't change output
  Use to: validate dim_customer rebuild matches the old version
```

```
10.4 The Package Ecosystem
MUST-HAVE:
  dbt-labs/dbt_utils                 — utility macros + tests
  calogica/dbt_expectations          — 400+ data quality tests
  dbt-labs/audit_helper              — refactor validation
SOMETIMES-HAVE:
  dbt-labs/codegen                   — generate boilerplate models / YAML
  dbt-labs/dbt_external_tables       — external tables (S3 → Snowflake)
  dbt-labs/metrics  (deprecated for semantic layer)
  fivetran/quickbooks, fivetran/stripe — pre-built models for connectors
ENGINE-SPECIFIC:
  dbt-labs/snowflake_utils
  dbt-labs/redshift
  dbt-labs/spark_utils
  re-data — data quality monitoring built on dbt
  elementary — observability + alerting for dbt
■ Principal mindset: Don't reinvent the testing wheel. dbt-utils + dbt_expectations covers 90% of common data
quality assertions. Spend energy on the 10% custom tests specific to your business invariants. The teams that
succeed don't write fewer tests — they write the RIGHT tests using off-the-shelf where possible and saving custom
code for business logic.
```

#### 11. Data Contracts & Constraints

```yaml
11.1 Why Contracts Matter
A data contract is an explicit, enforced agreement about what a model produces — column names, types,
nullability, semantics. Without contracts, downstream consumers couple to internal structure and break on
every change.
11.2 dbt Model Contracts (1.5+)
# models/marts/_models.yml
models:
  - name: fct_orders
    config:
      contract:
        enforced: true
    columns:
      - name: order_id
        data_type: varchar
        constraints:
          - type: primary_key
          - type: not_null
      - name: customer_id
        data_type: bigint
        constraints:
          - type: not_null
          - type: foreign_key
            expression: ref('dim_customers') (customer_id)
      - name: amount_cents
        data_type: bigint
        constraints:
          - type: check
            expression: "amount_cents >= 0"
      - name: order_date
        data_type: date
WHAT THIS ENFORCES:
  ✓ At build time: model output must match schema (column names, types)
  ✓ Build FAILS if a developer changes a column name without updating contract
  ✓ Generates DDL with PK/FK/CHECK constraints (where adapter supports)
LIMITATIONS:
  ✗ Not all warehouses enforce all constraint types
    (Snowflake: PK/FK are informational only, not enforced)
  ✗ CHECK constraints work in Postgres / Redshift; informational elsewhere
```

```sql
11.3 Versioned Models (1.6+)
# models/marts/_models.yml
models:
  - name: dim_customers
    latest_version: 2
    versions:
      - v: 1
        config:
          alias: dim_customers_v1
      - v: 2
        config:
          alias: dim_customers       # current = no suffix
# Downstream:
SELECT * FROM {{ ref('dim_customers', v=1) }}    # explicit version
SELECT * FROM {{ ref('dim_customers') }}          # latest
```

#### 1. Create v2 alongside v1; deprecate v1

#### 2. Tell consumers to migrate

#### 3. After grace period, remove v1

GOOD FOR: breaking schema changes without consumer outages.

```sql
11.4 Source Contracts
# Declare what you EXPECT from upstream:
sources:
  - name: stripe
    tables:
      - name: charges
        columns:
          - name: id
            tests: [unique, not_null]
            data_type: varchar
          - name: amount
            tests: [not_null]
            data_type: numeric
# Tests run on dbt source freshness / dbt test --select source:*
# Source schema changes show up as test failures.
```

```
11.5 The Data Contracts Movement
BEYOND DBT'S CONTRACTS:
   Producer-side contracts (the system writing the data)
   Schema registry integration (Kafka producers conform)
   Pre-merge data contract checks
   Automated impact analysis when contract changes
EMERGING TOOLS:
   DataContract.com spec — YAML standard for contracts
   Soda + dbt — runtime contract validation
   Atlan / DataHub — contract registry + lineage
   PrintMaker (recap) — schema-as-code
PRINCIPLE: SHIFT DATA QUALITY LEFT
  Producer side (in the source system or extraction tool)
    > Pipeline side (in dbt)
    > Consumer side (in dashboards) — too late
■ Interview angle: Q: How do you handle breaking schema changes in your data warehouse without breaking
consumers?
Staff: Several patterns, used together. (1) Model contracts in dbt — explicit column/type/constraint declarations that
fail the build on violation. (2) Versioned models — v1 stays available while v2 is rolled out; consumers migrate on
their schedule. (3) Deprecation warnings — mark old columns as deprecated for one quarter before removal. (4)
Lineage scan — before merging the change, identify every downstream consumer (dashboards, reverse ETL syncs,
ML features); notify owners. (5) Automated compatibility checks in CI — compile both versions, run audit_helper
compare, alert on row-count or value diffs. Doing this properly takes real time; the alternative is the all-hands fire drill
when finance can't close the month.
```

## PART IV — JINJA & MACROS

#### 12. Jinja Fundamentals in dbt

```
12.1 Jinja in dbt
Jinja is the templating engine dbt uses. SQL files are Jinja templates compiled to pure SQL at runtime.
Variables, conditionals, loops, function calls — all expressible in {{ ... }} and {% ... %} syntax.
TWO DELIMITERS:
  {{ expression }}    — render an expression as text
  {% statement %}     — control flow, no output
EXAMPLES:
  {{ ref('stg_orders') }}                — function call, renders to "warehouse.schema.stg_orders"
  {{ var('start_date') }}                — render a project variable
  {{ config(materialized='table') }}     — set config (no output, but Jinja syntax)
  {% if is_incremental() %}              — control flow
    WHERE updated_at >= ...
  {% endif %}
  {% set columns = ['a', 'b', 'c'] %}    — variable assignment
  {% for col in columns %}
    SUM({{ col }}) AS sum_{{ col }}{% if not loop.last %},{% endif %}
  {% endfor %}
```

12.2 Useful Built-in Functions ref('model_name') — reference a dbt model source('source_name', 'table') — reference a source table var('name', default) — project variable env_var('NAME', default) — env var target.name / .schema / .database — current target info this — the current model (target relation) run_query(sql) — execute SQL at compile time, return Agate table log('msg', info=true) — log to dbt CLI exceptions.raise_compiler_error() — fail compilation

```
12.3 Iteration Patterns
# Pivot multiple columns
{% set statuses = ['pending', 'shipped', 'delivered', 'cancelled'] %}
SELECT
    order_date,
    {% for status in statuses %}
    SUM(CASE WHEN status = '{{ status }}' THEN amount END) AS amount_{{ status }}
    {%- if not loop.last -%}, {%- endif %}
    {% endfor %}
FROM {{ ref('fct_orders') }}
GROUP BY 1
# Compiled output (status list expanded):
SELECT
    order_date,
    SUM(CASE WHEN status = 'pending'    THEN amount END) AS amount_pending,
```

```
dbt &amp; Data Modeling: Principal Engineer Notes
    SUM(CASE WHEN status = 'shipped'    THEN amount END) AS amount_shipped,
    ...
FROM warehouse.schema.fct_orders
GROUP BY 1
```

```sql
12.4 Compile-Time Data Access
# Generate code based on warehouse state
{% set columns = adapter.get_columns_in_relation(ref('source_model')) %}
SELECT
  {% for col in columns %}
    {% if col.dtype.startswith('NUMBER') %}
      {{ col.name }} AS {{ col.name }}_numeric,
    {% else %}
      {{ col.name }},
    {% endif %}
  {% endfor %}
  1 AS _placeholder
FROM {{ ref('source_model') }}
# OR query at compile-time:
{% set result = run_query("SELECT DISTINCT country FROM " ~ ref('users')) %}
{% set countries = result.columns[0].values() %}
# Use the values in your SQL...
WHERE country IN ({%- for c in countries -%}'{{ c }}'{%- if not loop.last %},{% endif -%}{% endfor %})
■■ Warning: Jinja runs at COMPILE time, SQL runs at QUERY time. run_query() in Jinja executes against
the warehouse during dbt compilation — every dbt compile / dbt run hits the warehouse. Use sparingly; cache
where possible. Don't put run_query in macros executed for hundreds of models.
```

#### 13. Macros: When to Use Them

```
13.1 The Macro
# macros/cents_to_dollars.sql
{% macro cents_to_dollars(column_name, precision=2) %}
  ROUND({{ column_name }}::numeric / 100.0, {{ precision }})
{% endmacro %}
# Usage in model:
SELECT
    order_id,
    {{ cents_to_dollars('amount_cents') }} AS amount_dollars,
    {{ cents_to_dollars('tax_cents', 4) }} AS tax_dollars
FROM {{ ref('stg_orders') }}
# Compiled:
SELECT
    order_id,
    ROUND(amount_cents::numeric / 100.0, 2) AS amount_dollars,
    ROUND(tax_cents::numeric / 100.0, 4) AS tax_dollars
FROM warehouse.schema.stg_orders
```

```
13.2 The Three Tiers of Macros
TIER 1: TRANSFORMATIONS
  Reusable SQL expressions
  cents_to_dollars(), pivot_by_(), surrogate_key(), date_spine()
  → Mostly available in dbt-utils; write your own only for domain-specific.
TIER 2: GENERATORS
  Produce entire models / queries
  generate_schema_name(), generate_alias_name() (already overridable hooks)
  build_dim_from_audit_table()
  → Use for: code consolidation across many models.
TIER 3: HOOKS & DISPATCH
  Adapter-specific dispatch (write once, run on multiple warehouses)
  generate_database_name() overrides
  → Use for: multi-warehouse codebases, pre/post-hooks.
```

13.3 Adapter Dispatch # macros/concat_strings.sql {% macro concat_strings(left, right) %} {{ return(adapter.dispatch('concat_strings')(left, right)) }} {% endmacro %} {% macro default__concat_strings(left, right) %} {{ left }} || {{ right }} {% endmacro %} {% macro bigquery__concat_strings(left, right) %} CONCAT({{ left }}, {{ right }}) {% endmacro %} # Usage: SELECT {{ concat_strings('first_name', 'last_name') }} AS full_name FROM ... # dbt picks the right implementation based on the target adapter.

```sql
13.4 Hooks: Run Code Before/After
# Project-level: dbt_project.yml
models:
  my_project:
    +post-hook: "GRANT SELECT ON {{ this }} TO bi_role"
    +pre-hook:  "ALTER WAREHOUSE SET WAREHOUSE_SIZE = 'X-LARGE'"
# Model-level:
{{ config(
    materialized='table',
    post_hook=[
        "GRANT SELECT ON {{ this }} TO reporting",
        "ANALYZE {{ this }}"
    ]
) }}
# On-run-start / on-run-end (whole-run hooks):
on-run-start:
  - "CREATE SCHEMA IF NOT EXISTS audit"
on-run-end:
  - "INSERT INTO audit.dbt_runs SELECT '{{ run_started_at }}', ..."
USE FOR:
   Permissions management
   Vacuum / analyze / cluster after rebuild
   Audit logging of runs
   Warehouse sizing per stage of pipeline
```

```
13.5 Macro Anti-Patterns
■ MACRO PYRAMIDS
  Macro that calls macro that calls macro that calls macro
  → Compiled SQL is unreadable; debugging is hellish.
  → Refactor: flatten or write the SQL inline if it's used once.
■ MACROS THAT QUERY THE WAREHOUSE AT COMPILE TIME
  Every dbt compile / run hits the warehouse extra times
  → Cache results, or move logic into the model SQL itself
■ JINJA-HEAVY MODELS
  60% Jinja, 40% SQL
  → Hard to read; hard to debug; CI test pain
  → Refactor: pull logic into macros, keep models clean SQL
■ MAGIC NUMBERS BURIED IN MACROS
  threshold = 0.95 hidden 5 levels deep
  → Use vars or YAML config; macros should be parameterized
■ COPY-PASTED MACROS FROM dbt-utils
  Use the package instead — get bug fixes for free
■ Principal mindset: Macros are dbt's most-abused feature. The pattern: junior writes a model, discovers Jinja,
extracts everything into macros, ends up with unreadable code that's hard to test or modify. Default to SQL,
abstract to macros only when the same pattern repeats 3+ times. Macros should be small, focused, well-tested,
and well-named. If you can't read the compiled SQL and immediately understand what the model does, you've
over-abstracted.
```

```
14. Packages & The dbt Ecosystem
14.1 packages.yml
packages:
  - package: dbt-labs/dbt_utils
    version: [">=1.1.0", "<2.0.0"]
  - package: calogica/dbt_expectations
    version: 0.10.3
  - git: "https://github.com/myorg/my-dbt-shared.git"
    revision: v1.2.3
  - local: ../shared-dbt-package
dbt deps           # install all packages to dbt_packages/
# .gitignore typically includes:
dbt_packages/
target/
```

```
14.2 The Top Packages by Category
UTILITY / GENERAL
  dbt-labs/dbt_utils                  — macros + tests (REQUIRED for most projects)
  dbt-labs/codegen                    — generate models / YAML scaffolding
DATA QUALITY
  calogica/dbt_expectations           — 400+ assertions
  re-data/re_data                     — anomaly detection over models
  elementary-data/elementary          — observability + alerting
CONNECTOR-SPECIFIC (pre-built models)
  fivetran/* (for Fivetran sources)
  dbt-labs/snowplow (event analytics)
  segment/dbt-segment
WAREHOUSE-SPECIFIC
  dbt-labs/snowflake_utils
  dbt-labs/redshift
  dbt-labs/external_tables
REFACTORING
  dbt-labs/audit_helper               — compare old vs new model versions
  dbt-labs/dbt_project_evaluator      — project-level best-practice audits
GOVERNANCE / METADATA
  dbt-labs/dbt_meta_testing           — enforce model meta requirements
  flexible-dataflows/dbt-coverage     — test coverage reporting
```

```
14.3 Building Your Own Internal Package
WHEN TO BUILD ONE:
  ✓ Common macros used across multiple dbt projects
  ✓ Shared sources (companies with central data warehouse)
  ✓ Custom tests / materializations
  ✓ Schema / naming conventions enforced as macros
STRUCTURE:
  my-dbt-shared/
    dbt_project.yml
    macros/
      our_naming_convention.sql
      our_id_generators.sql
    models/
      shared_dims/
```

```
dbt &amp; Data Modeling: Principal Engineer Notes
        dim_currencies.sql
        dim_calendar.sql
    tests/
      generic/
        our_assertion.sql
CONSUMING PROJECT:
  packages.yml:
    - git: "https://github.com/myorg/my-dbt-shared.git"
      revision: v1.2.3
VERSIONING: pin versions, run dbt deps in CI, manage migrations
```

14.4 Package Hygiene  PIN VERSIONS — never use latest  REVIEW SOURCE before installing 3rd-party packages (you're running their SQL)  UPDATE REGULARLY for security / bug fixes  AVOID PACKAGE BLOAT — every dependency is operational surface area  PREFER OFFICIAL packages (dbt-labs/*) over random GitHub  For internal packages: full CI pipeline + semantic versioning

## PART V — DATA MODELING

#### 15. Dimensional Modeling (Kimball)

```
15.1 The Star Schema, Revisited
        ■■■■■■■■■■■         ■■■■■■■■■■■■
        ■ Date    ■         ■ Product  ■
        ■  dim    ■         ■   dim    ■
        ■■■■■■■■■■■         ■■■■■■■■■■■■
             ■■■■■         ■■■■■■■
                 ▼         ▼
              ■■■■■■■■■■■■■■■■■■■
              ■  Sales FACT     ■
              ■  date_key       ■
              ■  product_key    ■
              ■  store_key      ■
              ■  customer_key   ■
              ■  quantity       ■
              ■  amount         ■
              ■■■■■■■■■▲■■■■■■■■■
              ■■■■■■■■■■■■■■■■■■■
        ■■■■■■■■■■■■■■    ■■■■■■■■■■■■
        ■   Store    ■    ■ Customer ■
        ■    dim     ■    ■   dim    ■
        ■■■■■■■■■■■■■■    ■■■■■■■■■■■■
FACT: measurements (transactions, events)
DIMENSION: descriptive context (who, what, where, when)
```

```
15.2 Grain
The grain of a fact table is the meaning of one row. Choose grain FIRST, before dimensions. Mixed-grain
facts (some rows at order level, some at line-item) are the most common modeling failure.
GRAIN EXAMPLES:
  fct_orders          → one row per ORDER         (header-level)
  fct_order_items     → one row per LINE ITEM      (item-level)
  fct_daily_metrics   → one row per (date, dim)    (aggregated)
  fct_inventory_snapshot → one row per (product, date)  (snapshot)
THE GRAIN STATEMENT:
  "A row in fct_orders represents ONE COMPLETED ORDER at the moment of completion."
  "A row in fct_inventory_snapshot represents inventory on hand for ONE PRODUCT at END OF DAY."
WRITE THIS DOWN IN THE MODEL DOCSTRING.
Future engineers will thank you.
```

```
15.3 Fact Table Types
TRANSACTION FACT
  One row per event. Append-only. Most common.
  Grain: one transaction.
  Examples: fct_orders, fct_payments, fct_page_views.
PERIODIC SNAPSHOT
  One row per entity per period.
  Grain: (entity, period).
```

```
dbt &amp; Data Modeling: Principal Engineer Notes
  Examples: fct_account_balance_daily, fct_inventory_snapshot.
ACCUMULATING SNAPSHOT
  One row per process instance, UPDATED as it progresses.
  Grain: one process instance.
  Multiple date columns: placed_at, picked_at, shipped_at, delivered_at.
  Examples: fct_order_pipeline, fct_loan_application.
FACTLESS FACT
  One row per event, but no measures (just key combinations).
  Grain: one event.
  Examples: fct_user_logins, fct_class_attendance.
```

```
15.4 Dimension Table Types
CONFORMED DIMENSIONS
  Reused across multiple fact tables.
  dim_customer used by fct_orders, fct_support_tickets, fct_payments.
  Consistency: a customer means the same thing everywhere.
ROLE-PLAYING DIMENSIONS
  One physical dimension used in multiple "roles".
  dim_date used as order_date, ship_date, delivery_date.
  In dbt: views aliasing the dimension by role.
DEGENERATE DIMENSIONS
  Dimension attributes stored INSIDE the fact (no separate table).
  E.g., order_number, transaction_id — IDs not joined to anything.
JUNK DIMENSIONS
  Aggregate low-cardinality flags into one dim.
  dim_order_status_flags: (is_high_value, is_subscription, is_first_order).
  Avoid bloating fact tables with N boolean columns.
```

```
15.5 Surrogate Keys
NATURAL KEY = business key (order_id, customer_id, sku)
SURROGATE KEY = synthetic key, owned by the warehouse
WHY SURROGATE:
  ✓ Stable across source-system changes
  ✓ Type-uniform (all SKs are bigint or UUID)
  ✓ Handle SCD2 (multiple versions of same natural key)
  ✓ Handle multi-source dims (same customer in CRM + billing)
IN DBT:
  {{ dbt_utils.generate_surrogate_key(['source_id', 'tenant_id']) }}
  → md5 hash; deterministic from inputs
PATTERN:
  Dimensions: include both natural_key AND surrogate_key
  Facts: reference dimensions via surrogate_key
```

```
15.6 The Bus Matrix
Kimball's bus matrix = grid of fact tables × dimensions used
                    date  customer  product  store
  fct_sales           ✓      ✓         ✓       ✓
  fct_inventory       ✓                ✓       ✓
  fct_returns         ✓      ✓         ✓       ✓
  fct_promotions      ✓                ✓       ✓
  fct_customer_calls  ✓      ✓
WHAT IT TELLS YOU:
```

dbt &amp; Data Modeling: Principal Engineer Notes  Where conformity is needed (date, customer, product across facts)  What dimensions are unique to one fact (store only in inventory/sales/returns)  Coverage gaps in your model PRACTICAL: maintain in a wiki / docs. Update before adding new facts. ■ Interview angle: Q: Walk me through designing a star schema for an e-commerce business. Staff: Start with the bus matrix — what facts do we need? Order placement, order line items, inventory snapshots, returns, customer interactions. Then the conformed dimensions — date, customer, product, store/channel. Grain statements for each fact: fct_order_items is one row per line item per order; fct_inventory is one row per (sku, location, snapshot_date). Surrogate keys via dbt_utils — natural keys preserved for tracing back to source. SCD2 on customer and product (snapshot dimensions for history). Test: PK uniqueness on dimensions, FK relationships from facts, accepted_values on status columns, expected row counts vs source. Document the grain in every fact's docstring.

#### 16. Wide Tables & One Big Table

16.1 The Anti-Kimball Argument Star schemas were designed when join performance mattered enormously (row stores). Modern columnar warehouses (Snowflake, BigQuery, Redshift) handle wide tables exceptionally well. The argument: denormalize fully into wide tables ("One Big Table" or OBT) for analytics. Trade storage and duplication for query simplicity and performance.

```sql
16.2 What OBT Looks Like
obt_orders:
  order_id, order_date, order_status,
  customer_id, customer_name, customer_email, customer_country,
  customer_tier, customer_signup_date,
  product_id, product_name, product_category, product_supplier,
  store_id, store_name, store_region,
  amount, quantity, discount, tax,
  shipping_method, shipping_cost, ...
ONE 80-column table. Replace 5-table star join with a single SELECT *.
```

```sql
16.3 Pros and Cons
PROS:
  ✓ No joins in BI queries → faster ad-hoc analytics
  ✓ Simpler for analysts (one table, one SELECT)
  ✓ Self-contained — historical data preserved with the fact (built-in SCD)
  ✓ Easier to materialize / clustering / partitioning
CONS:
  ✗ Massive duplication — customer attributes repeated billions of times
  ✗ Updates: change a customer's name → update billions of rows
     (mitigated: don't update; rebuild from sources)
  ✗ Storage cost (columnar compression helps but not free)
  ✗ Complex to maintain — one wide table per fact, lots of joins in dbt
  ✗ Schema changes affect ALL consumers of the wide table
```

```
16.4 Hybrid: Star + Wide Mart
COMMON PRODUCTION PATTERN:
  STAGING → CONFORMED DIMS + FACTS (star schema)
                    ■
                    ▼
            (the analytics layer, normalized)
                    ■
                    ▼
            WIDE MARTS (denormalized)
            obt_orders, obt_customers
                    ■
                    ▼
            BI tools / dashboards
WHY:
   Conformed dims + facts give you a single source of truth
   Wide marts give analysts/BI fast, simple consumption
   If wide mart is wrong, rebuild from the canonical model
```

#### 16.5 When OBT Wins

```
dbt &amp; Data Modeling: Principal Engineer Notes
GOOD FIT:
  ✓ Columnar warehouse (Snowflake, BigQuery, Redshift, ClickHouse)
  ✓ Analytics use cases (mostly aggregations, group-bys)
  ✓ Stable dimensions (don't change often)
  ✓ Performance-critical dashboards
  ✓ ML feature stores (need wide, denormalized rows)
POOR FIT:
  ✗ OLTP-style operational queries
  ✗ Frequently changing dimensions (customer name updates → rebuild)
  ✗ Row-store warehouse (Postgres, MySQL) — wide rows hurt
  ✗ Many independent consumers wanting different shapes
■ Principal mindset: The 'star schema vs wide table' debate is mostly settled in 2024: build both. Conformed dims
+ facts as your core model (canonical, versioned, contracted). Wide tables / OBT as the consumption layer for BI
and ML. The mistake is choosing only one. Pure star schema in a columnar warehouse is over-engineered for
analysts who just want to SELECT-and-GROUP. Pure wide tables lose the modeling rigor that catches errors at the
right layer.
```

#### 17. Data Vault 2.0

```
17.1 The Three Entity Types
HUB
   One per business entity (customer, product, order)
   Stores: natural business key + surrogate hash key + load metadata
   NEVER UPDATED — append-only
LINK
   Represents a RELATIONSHIP between hubs
   Stores: relationship hash + parent hub hashes + load metadata
   Many-to-many by default
SATELLITE
   Descriptive attributes for a hub or link
   One satellite PER source-OR-rate-of-change
   Inherently SCD2 (every change is a new row)
   A hub may have many satellites (CRM data + web data + ...)
```

```
17.2 Example Tables
hub_customer
  customer_hk  (sha256 of natural key + load source)
  customer_id  (natural business key)
  load_dt
  rec_src
link_customer_order
  link_hk
  customer_hk
  order_hk
  load_dt
  rec_src
sat_customer_crm
  customer_hk
  load_dt
  end_dt          (null = current)
  hash_diff       (hash of all descriptive cols, for change detection)
  name, email, phone, ...
  rec_src
sat_customer_billing
  customer_hk
  load_dt
  end_dt
  hash_diff
  billing_address, billing_zip, ...
  rec_src
  → Same hub, different sources contributing different satellites.
```

```
17.3 Strengths and Weaknesses
STRENGTHS:
  ✓ INSERT-ONLY everything — parallel loads, no update contention
  ✓ AUDIT TRAIL by construction — every value timestamped + sourced
  ✓ SCHEMA-EVOLUTION FRIENDLY — add satellites without changing existing tables
  ✓ SOURCE-SYSTEM ISOLATION — one source = one satellite; conflicts don't cross
  ✓ Provable lineage and history
WEAKNESSES:
  ✗ Many joins for meaningful queries
```

```
dbt &amp; Data Modeling: Principal Engineer Notes
  ✗ Not analyst-friendly — needs a presentation layer (marts) on top
  ✗ Operationally complex
  ✗ Overkill for small or stable warehouses
WHEN TO PICK DV:
  ✓ Enterprise integrating MANY source systems with conflicting schemas
  ✓ Strict regulatory audit requirements (HIPAA, SOX, GDPR)
  ✓ Schema changes you can't predict in advance
  ✓ History over performance
17.4 dbt + Data Vault
PACKAGES:
  datavault4dbt (Scalefree)            — most comprehensive
  dbt-vault                            — older, less active
THE LAYERED ARCHITECTURE WITH DV:
  RAW STAGING        ← source replicas
       ▼
  DV (Hubs, Links, Sats)               ← long-term audited storage
       ▼
  INFORMATION MART (Kimball star)      ← analyst-facing
       ▼
  CONSUMERS
dbt makes DV manageable: macros generate hashes, manage load timestamps,
handle SCD2 in satellites automatically.
■ Principal mindset: Data Vault is the right answer for ~5% of warehouses: large enterprises with 20+ source
systems, strict audit requirements, or a need to absorb schemas that change unpredictably. For most teams it's
over-engineering. The Kimball + OBT hybrid is right for 80%. Don't pick DV because it sounds rigorous; pick it when
the alternative (Kimball + custom history) breaks down under your specific constraints.
```

#### 18. Activity Schema

18.1 The One-Table-To-Rule-Them-All Activity Schema (by Ahmed Elsamadisi / Narrator) proposes a single table for every customer-time activity, with the same shape regardless of business domain. Built on the observation that most analytics questions are sequences of activities per entity over time.

```sql
18.2 The Schema
ACTIVITY STREAM (one table):
  customer            -- the entity (could be user, account, device)
  ts                  -- timestamp of the activity
  activity            -- enum: 'placed_order', 'opened_email', 'logged_in', ...
  feature_json        -- payload (semi-structured)
  revenue_impact      -- monetary value, optional
  link                -- foreign key to source object, optional
ALL CUSTOMER EVENTS, ALL TIME, ONE TABLE.
QUERIES USE TEMPORAL JOINS:
  "How many customers opened an email then placed an order within 7 days?"
  → JOIN the activity stream on itself with date filters
  "What's the conversion from sign-up to first purchase?"
  → Self-join from activity='signed_up' to activity='first_purchase'
```

```
18.3 The Selling Points
✓ ONE TABLE replaces ~50 fact tables for customer analytics
✓ Generalizable — same questions answered across domains
✓ Excellent for funnels, cohorts, lifecycle analysis
✓ Composable via "datasets" (saved query patterns)
✓ Self-joining is the primary access pattern
TRADE-OFFS:
✗ All-events table can get huge (billions of rows)
✗ Heavy temporal self-joins are expensive
✗ Loses some Kimball clarity (no separate customer dim)
✗ Tooling immature compared to star schema
✗ Less familiar to traditional analysts
```

```
18.4 When Activity Schema Fits
GOOD FIT:
  ✓ B2C / B2B analytics centered on customer behavior
  ✓ Heavy funnel / lifecycle / cohort analysis
  ✓ Want consistent shape across many analytics questions
POOR FIT:
  ✗ Business is not customer-time-centric (e.g., inventory analytics)
  ✗ Many analysts unfamiliar with temporal joins
  ✗ Strong existing investment in star schema
  ✗ Need rich dimensional attributes joined explicitly
REALISTIC:
  Used alongside star schema, not as a replacement.
  Activity table for behavior analytics, star schema for finance/ops.
```

#### 19. Slowly Changing Dimensions

19.1 The SCD Types Type Name What happens on change 0 Retain original Never update — historical fact 1 Overwrite Replace old value, lose history 2 Add new row New version, mark old inactive (timestamps) 3 Add new column Keep "current" and "previous" columns 4 History table Current dim + separate history table 6 Hybrid (1+2+3) Current value + version history

```sql
19.2 SCD2 in dbt: The Canonical Pattern
# OPTION A: Use dbt snapshots (covered in Ch 7)
#   Easiest. dbt manages the history table.
# OPTION B: Build SCD2 manually with incremental
{{ config(
    materialized='incremental',
    unique_key='customer_sk'
) }}
WITH source AS (
    SELECT * FROM {{ source('raw', 'customers') }}
),
changes AS (
    SELECT *,
           md5(CONCAT(name, email, address)) AS hash_diff
    FROM source
),
{% if is_incremental() %}
existing AS (
    SELECT customer_id, hash_diff
    FROM {{ this }}
    WHERE valid_to IS NULL
),
new_rows AS (
    SELECT c.*
    FROM changes c
    LEFT JOIN existing e ON c.customer_id = e.customer_id
    WHERE e.customer_id IS NULL                -- new customer
       OR e.hash_diff != c.hash_diff           -- changed customer
)
{% else %}
new_rows AS (SELECT * FROM changes)
{% endif %}
SELECT
    {{ dbt_utils.generate_surrogate_key(['customer_id', 'CURRENT_TIMESTAMP']) }} AS customer_sk,
    customer_id,
    name, email, address,
    hash_diff,
    CURRENT_TIMESTAMP AS valid_from,
    CAST(NULL AS TIMESTAMP) AS valid_to
FROM new_rows
# Post-hook: close out old versions
post_hook="UPDATE {{ this }} t SET valid_to = c.valid_from
```

```sql
dbt &amp; Data Modeling: Principal Engineer Notes
           FROM (SELECT customer_id, MIN(valid_from) AS valid_from
                 FROM {{ this }} WHERE valid_from > '{{ run_started_at }}'
                 GROUP BY 1) c
           WHERE t.customer_id = c.customer_id AND t.valid_to IS NULL
```

```
19.3 Choosing an SCD Type
TYPE 1 — Overwrite (lose history)
  ✓ Customer's preferred language updated
  ✓ Typo fixes
  Not worth keeping the old value.
TYPE 2 — Full history (most common for SCD2)
  ✓ Customer address changes
  ✓ Product price changes
  ✓ Sales territory changes
  → Reports show old vs new state correctly over time.
TYPE 3 — Current + previous only
  ✓ "What was the customer's last manager?"
  ✓ Need fast lookup of immediate previous state, nothing further back.
TYPE 4 — History in separate table
  Used when fact tables don't need to join to history;
  history is for audit / compliance only.
DON'T USE TYPE 6 unless you have a specific need —
adds complexity without clear win.
■ Interview angle: Q: A customer's billing country changed. Old orders should report in the old country; new orders
in the new country. How do you model this?
Staff: SCD Type 2 on the dim_customer. Each row has valid_from / valid_to. Facts store the surrogate key that was
current at fact time — either by snapshotting the SK at fact insert (cleanest) or by joining fact.event_time BETWEEN
dim.valid_from AND dim.valid_to at query time. The first approach is faster but locks in attribution; the second
handles late-arriving dim changes. Either way, dbt snapshots build this with one configuration block — way easier
than rolling your own SCD2.
```

#### 20. Choosing a Modeling Style

20.1 The Decision Matrix Factor Star (Kimball) Wide (OBT) Data Vault Team analyst skill High Low High Warehouse type Any Columnar Any Number of source systems < 10 < 10 > 20 Schema stability Stable Stable Volatile Audit requirements Modest Modest Strict Query simplicity Med High Low Storage cost concern Med High Med Time to first dashboard Med Fast Slow Tier-1 dimensional history Yes (SCD2) Built-in Built-in

```
20.2 The Realistic Production Stack
WHAT MOST MATURE COMPANIES ACTUALLY USE:
  STAGING                    ← 1:1 source clean-up (views)
        ■
        ▼
  INTERMEDIATE               ← business logic stepping stones (ephemeral)
        ■
        ▼
  CORE: Conformed dims + facts  ← Kimball star schema (tables / incremental)
        ■
        ▼
  MARTS: Wide tables / OBT       ← denormalized for BI/ML (tables)
        ■
        ▼
  CONSUMERS                  ← Looker, Tableau, Mode, reverse ETL
WHY THIS STACK:
   Star schema = the canonical model, history-preserving, governable
   Wide marts = consumption layer for analysts/BI/ML, denormalized
If wide marts are wrong, rebuild from star.
If star is wrong, rebuild from staging.
If staging is wrong, fix the source.
```

```sql
20.3 The Anti-Patterns
■ PURE STAR SCHEMA + 5 levels of joins for every dashboard
   → BI tools die; analysts learn SQL gymnastics; consumers complain
   → Fix: add a wide-table consumption layer
■ PURE OBT + no canonical dimensions
   → Update one customer name = rebuild 10B rows
   → Same metric calculated differently in different wide tables
   → Fix: build the wide tables FROM canonical dims/facts
■ DATA VAULT for a 5-source warehouse
   → 10× the modeling effort for marginal benefit
   → Analysts can't query the vault directly
   → Fix: Kimball is fine here
```

```
dbt &amp; Data Modeling: Principal Engineer Notes
■ NO MODELING AT ALL
   → "Just query the raw tables; we'll figure it out later"
   → Same metric calculated 12 different ways across 12 dashboards
   → Fix: at minimum, conformed dims and a few core facts
■ Principal mindset: The choice isn't ideological — it's situational. At staff level, you can explain WHY you'd pick
each approach in different contexts. The wrong answer is 'always X'. The right answer is 'star + wide marts for most
things; vault when the audit/integration needs justify it; activity schema when customer behavior analytics is the
primary use case'. Defend the choice with the constraints.
```

## PART VI — PROJECT ARCHITECTURE

#### 21. The Layered Model: Staging → Marts

```
21.1 The Standard dbt Layering
SOURCES (raw)              -- declared, not built
   ■
   ▼
STAGING (stg_*)            -- 1:1 with source tables
    View materialized
    Renaming, type casts
    NO joins, NO aggregations
    Owns the contract with sources
   ■
   ▼
INTERMEDIATE (int_*)       -- business logic in pieces
    Ephemeral or view
    Joins of staging models
    Pre-aggregations
    NEVER queried by consumers
   ■
   ▼
MARTS (fct_*, dim_*)       -- canonical business model
    Table or incremental
    Conformed dimensions, facts
    Tested heavily
    CONSUMED by BI / reverse-ETL / downstream
   ■
   ▼ (optionally)
WIDE MARTS (obt_*)         -- denormalized for fast BI
    Table
    Joins of dims + facts pre-computed
    One per use case (obt_sales, obt_customers, obt_marketing)
```

```sql
21.2 The Staging Layer Rules
ONE STG MODEL PER SOURCE TABLE.
  stg_stripe__charges
  stg_stripe__customers
  stg_salesforce__accounts
  stg_segment__events
NAMING CONVENTION: stg_<source_name>__<entity>
  Double underscore separates source from entity.
INSIDE A STAGING MODEL:
  ✓ Rename columns to project conventions (snake_case, consistent naming)
  ✓ Cast types
  ✓ Filter clearly invalid rows (e.g., test data)
  ✓ Extract from JSON if needed
  ✓ Add a meaningful 'loaded_at' timestamp
✗ DO NOT:
  ✗ Join multiple sources
  ✗ Aggregate
  ✗ Apply business logic
  ✗ Cherry-pick fields → preserve everything you might need
```

```
dbt &amp; Data Modeling: Principal Engineer Notes
→ Staging is the ONE PLACE where source schema is allowed to leak.
  Marts depend on staging, not on sources. If a source schema changes,
  only the staging model needs to update.
```

```
21.3 The Intermediate Layer
USE INTERMEDIATE MODELS WHEN:
  ✓ A complex join / aggregation is used by multiple marts
  ✓ Business logic is too complex to fit cleanly in a single mart
  ✓ You want to test an intermediate result independently
  ✓ A computation is expensive — materialize once, reuse
ANTI-PATTERN: 50 intermediate models for a 100-model project
  → over-decomposed; flatten unless reuse is clear
NAMING: int_<entity>__<verb>
  int_orders__pivoted
  int_customers__joined_to_subs
  int_revenue__monthly_aggregated
```

```
21.4 The Marts Layer
NAMING:
  dim_<entity>     for dimensions     (dim_customers, dim_products, dim_dates)
  fct_<event>      for facts          (fct_orders, fct_sessions, fct_payments)
  rpt_<topic>      for reports        (rpt_executive_dashboard)
  obt_<topic>      for wide tables    (obt_sales)
FOLDER STRUCTURE BY DOMAIN:
  models/marts/finance/
  models/marts/marketing/
  models/marts/product/
  models/marts/operations/
  → Domain ownership clear; schemas can be split per domain.
EVERY MART HAS:
   A YAML entry with description
   Column-level docs for primary columns
   Tests on PK/FK and key business invariants
   Owner tag (which team owns this)
   Tier tag (tier-1 = on-call, tier-3 = best effort)
■ Interview angle: Q: Why do you separate staging from marts?
Staff: Staging is a contract with sources: 'this is what arrives, cleaned and renamed'. Marts are a contract with
consumers: 'this is the business view, stable and tested'. If you skip staging and reference sources directly in marts,
every source schema change cascades through every mart that touches it. Staging is the shock absorber. It also
makes the staging layer testable — you can validate the contract with sources independent of business logic.
Skipping it works at 10 models, breaks at 100.
```

#### 22. Naming, Folder Structure & Conventions

```
22.1 The Conventions That Matter
MODEL NAMING:
  stg_<source>__<entity>            staging
  int_<entity>__<verb>              intermediate
  dim_<entity>                       dimension
  fct_<event>                        fact
  rpt_<topic>                        report
  obt_<topic>                        wide table
COLUMN NAMING:
  snake_case_always
  *_id                              foreign keys to dimensions
  *_sk                              surrogate keys
  *_at                              timestamps
  *_date                            dates (not timestamps)
  is_*, has_*                       booleans
  amount_<currency>_cents           monetary, with unit + currency
  count_*                           counts (plural)
  total_*, sum_*                    aggregates
TABLE / VIEW NAMING in WAREHOUSE:
  Database: ANALYTICS (or analytics_dev for dev)
  Schema:   <domain> (finance, marketing, product, ...)
            staging  (all staging models)
            snapshots (all snapshots)
            audit / metadata
```

```sql
22.2 The Style Guide
SQL STYLE (a popular convention from dbt Labs):
SELECT
    column_a,
    column_b,
    SUM(amount) AS total_amount,
    COUNT(*) AS row_count
FROM {{ ref('stg_orders') }} AS o
LEFT JOIN {{ ref('stg_customers') }} AS c
    ON o.customer_id = c.customer_id
WHERE o.status = 'completed'
  AND o.order_date >= '2024
01'
GROUP BY 1, 2
ORDER BY total_amount DESC
KEY RULES:
   Keywords: lowercase
   Identifiers: lowercase, snake_case
   Trailing commas (Snowflake/BigQuery support; helps PR diffs)
   One column per line in SELECT
   Indentation: 4 spaces
   Joins: ON on the next line, indented
   Use AS for aliases explicitly
   CTE names same convention as model names
```

```
22.3 Linters & Formatters
SQLFLUFF
   Industry-standard SQL linter
   Supports dbt templating
   Configurable rules
```

```
dbt &amp; Data Modeling: Principal Engineer Notes
  pip install sqlfluff sqlfluff-templater-dbt
  sqlfluff lint models/
  sqlfluff fix models/
DBT PROJECT EVALUATOR
   Project-level audit of best practices
   Checks: every model has docs, tests, owner; staging shape; etc.
CONFIG (.sqlfluff):
  [sqlfluff]
  dialect = snowflake
  templater = dbt
  [sqlfluff:rules]
  max_line_length = 120
  capitalisation_keywords = lower
```

```
22.4 Folder Structure for Scale
SMALL (10-50 models):
  models/
    staging/
    intermediate/
    marts/
MEDIUM (50-500 models):
  models/
    staging/
      stripe/
      salesforce/
      segment/
    intermediate/
    marts/
      finance/
      marketing/
      product/
LARGE (500+ models or multi-team):
  models/
    staging/
      stripe/
      salesforce/
    domains/
      finance/
        intermediate/
        marts/
      marketing/
        intermediate/
        marts/
      product/
        intermediate/
        marts/
    shared/
      dimensions/
      utilities/
  → Domain boundaries clearer; team ownership obvious.
■ Principal mindset: Conventions are about REDUCING COGNITIVE LOAD for everyone reading the code. Pick a
style guide on day one (dbt Labs' is fine, or your own variant). Document it. Enforce with sqlfluff in CI. Don't bikeshed
about which style — bikeshedding about inconsistent code costs more than any style choice does.
```

#### 23. Monorepo vs Multi-Project

```
23.1 The Single Project (Default)
ONE REPO, ONE dbt PROJECT.
  Works well for: small/medium teams (< 30 engineers writing models).
PROS:
  ✓ Simple — one CI pipeline, one docs site, one DAG
  ✓ Cross-team visibility (everyone sees everyone's models)
  ✓ Easy refactors across the project
CONS:
  ✗ All teams share CI lockstep — slow when many models
  ✗ One bad model can break the entire project's run
  ✗ Permissions are coarse (everyone sees raw sources)
  ✗ Hard to enforce team boundaries
```

```sql
23.2 The Multi-Project Pattern
MULTIPLE dbt PROJECTS, often in separate repos.
  finance-dbt        — finance team's models
  marketing-dbt      — marketing team's models
  product-dbt        — product team's models
  shared-dbt         — common dims, utilities, conventions (as a package)
DOWNSTREAM PROJECTS DEPEND ON SHARED:
  packages.yml:
    - git: "https://github.com/myorg/shared-dbt.git"
      revision: v1.2.3
PROS:
  ✓ Team ownership clear
  ✓ Independent CI / deploy
  ✓ Bad model in marketing doesn't break finance
  ✓ Permissions per project
CONS:
  ✗ Cross-project lineage harder (no unified DAG)
  ✗ Version management across projects
  ✗ Possible duplication if shared isn't comprehensive
23.3 dbt Mesh (dbt 1.6+)
DBT MESH = official multi-project dependency management.
  PROJECT A defines models with access control:
    models:
      - name: dim_customers
        access: public           # accessible from other projects
      - name: int_customer_pivot
        access: private          # NOT accessible (default)
  PROJECT B references:
    SELECT * FROM {{ ref('project_a', 'dim_customers') }}
  → dbt enforces access; mesh-level lineage available
  → Each project is its own dbt run; selectable independently
REQUIREMENTS:
   dbt 1.6+
   Shared manifest visibility (often via dbt Cloud or artifact registry)
```

23.4 When to Split Signal Action <30 engineers, single warehouse, shared schemas Single project Multiple teams with conflicting release cadences Multi-project / Mesh CI takes > 30 min on every PR Split (or slim CI harder) Compliance: team A can't see team B's data Multi-project (likely) Cross-team refactors common Stay single Sharing dims/utilities across teams Shared package + multi-project ■ Principal mindset: Most companies should start with a single project and split only when the pain is acute. Mesh is great when needed but adds operational complexity. The premature multi-project split is more common than the late one — teams over-engineer for hypothetical future scale and end up with five dbt repos that all import from each other and break in non-obvious ways.

#### 24. Cross-Project Dependencies (Mesh)

24.1 The Cross-Project Problem When team A's models depend on team B's outputs, the question is: how do you express that dependency? Three answers, in order of formality.

24.2 Option 1: Cross-Schema References # Team A references team B's tables directly SELECT * FROM analytics.team_b.dim_products PROS: simple, no coordination CONS: no contract; team B can break team A with any change WORKS FOR: very small orgs with high trust

```sql
24.3 Option 2: Sources Across Teams
# Team A declares team B's outputs as SOURCES
# models/sources.yml (team A)
sources:
  - name: team_b_marts
    database: analytics
    schema: team_b
    tables:
      - name: dim_products
        columns:
          - name: product_id
            tests: [unique, not_null]
# In models:
SELECT * FROM {{ source('team_b_marts', 'dim_products') }}
PROS: explicit dependency, freshness checks, tested contract
CONS: still no formal cross-project DAG; manual coordination on schema changes
WORKS FOR: medium teams with established conventions
```

```sql
24.4 Option 3: dbt Mesh
# Team B exposes public models
# project_b/models/marts/_models.yml
models:
  - name: dim_products
    access: public
    version: 1
    contract:
      enforced: true
    columns:
      - name: product_id
        data_type: varchar
        constraints: [{type: primary_key}]
# Team A references with cross-project ref
SELECT * FROM {{ ref('project_b', 'dim_products') }}
PROS:
  ✓ Enforced contracts
  ✓ Cross-project lineage in docs
  ✓ Versioning support (project_b dim_products v1 and v2 coexist)
  ✓ Access control (private vs public)
CONS:
```

```
dbt &amp; Data Modeling: Principal Engineer Notes
  ✗ Requires dbt 1.6+
  ✗ Mesh setup needed (dbt Cloud or manifest sharing)
  ✗ Operational overhead
```

24.5 The Governance Layer WITH MULTI-PROJECT MESH, YOU NEED: 1. CONTRACTS — explicit schema agreements per public model 2. VERSIONING — old + new versions coexist during migration 3. DEPRECATION POLICY — N quarters notice before removing 4. CHANGE REVIEW — cross-team review for public model changes 5. LINEAGE TOOLING — see who depends on what (DataHub, Atlan, Lightdash) 6. SLAs — what's the freshness commitment for each public model? WITHOUT THESE: mesh becomes microservices' downsides without the upsides. ■ Interview angle: Q: Your company has 10 data teams each running their own dbt project. How do you handle dependencies between them? Staff: First, scrutinize whether 10 separate projects is right — often this is 'every team wants autonomy' over-applied. If the boundaries are real (different stakeholders, different release cadences, different compliance requirements), then dbt Mesh with contracts. Each team designates PUBLIC models as their contracts; private models can change freely. Cross-team consumers use ref('other_project', 'public_model'). Versioned models support phased migrations. Backed by an audit process: any change to a public model goes through cross-team review. Without that governance, you've just distributed your monolith without removing any complexity.

## PART VII — PRODUCTION OPERATIONS

  - 25.1 The Pull Request Pipeline
- ON PR OPEN / UPDATE:
- 1. PARSE  — dbt parse (catch syntax/ref errors fast)
- 2. COMPILE — dbt compile (validate against target)
- 3. LINT   — sqlfluff lint
- 4. SLIM BUILD
- dbt build -s state:modified+ --defer --state ./prod_manifest
- → Only builds CHANGED models and their downstream
- → Reuses prod tables for unchanged upstream (defer)
- 5. SLIM TESTS
- Test against the slim build's output
- 6. COMPARE
- audit_helper to compare old vs new model output where applicable
- ON MERGE TO MAIN:
- 7. FULL BUILD on prod (scheduled by Airflow / Dagster / dbt Cloud)

```
  8. Notify on failure (Slack, PagerDuty)
25.2 Slim CI — The Key Optimization
PROBLEM: dbt project has 2000 models, taking 90 min to build.
         Every PR can't rebuild everything.
SOLUTION: slim CI
```

#### 1. Store prod's manifest.json in a known location (S3, artifact registry)

2. On PR, compare to prod manifest: dbt build -s state:modified+ --defer --state /path/to/prod-manifest 3. state:modified+ = models that changed in this PR + their downstream 4. --defer = unchanged upstream models read from PROD warehouse, not built RESULT: PR builds only what changed (5-50 models, not 2000). CI runs in 2-10 minutes instead of 90.

```
25.3 Setting Up Slim CI
# 1. Production run uploads manifest:
- name: Upload prod manifest
  run: aws s3 cp target/manifest.json s3://dbt-artifacts/prod/manifest.json
# 2. CI downloads it:
- name: Download prod manifest
  run: aws s3 cp s3://dbt-artifacts/prod/manifest.json ./prod-manifest/
# 3. Run slim CI:
- name: dbt build (slim)
  run: |
    dbt deps
    dbt build \
      -s state:modified+ \
      --defer \
      --state ./prod-manifest \
```

dbt &amp; Data Modeling: Principal Engineer Notes --target ci \ --threads 8 # CI target writes to ephemeral schema: # dbt_ci_pr_<pr_number> # Cleaned up after PR closes.

```
25.4 Schema Isolation Per Dev / PR
IN profiles.yml:
  schema: "dbt_{{ env_var('USER') }}"             # personal dev: dbt_alice
  Or for CI:
  schema: "ci_pr_{{ env_var('PR_NUMBER') }}"      # ci_pr_1234
  Or for dbt Cloud:
  schema: "dbt_cloud_pr_{{ env_var('DBT_CLOUD_PR_ID') }}"
ENSURES:
   Each developer's work is isolated
   PRs don't collide with each other or main
   Tests run against the dev/PR copy
   Easy cleanup after merge/close
```

```
25.5 Production Run Configuration
# Run by Airflow / Dagster / dbt Cloud Scheduler:
dbt build --threads 32 --target prod --vars "{run_date: 2024
01}"
POST-RUN:
   Upload run_results.json + manifest.json (for slim CI, monitoring)
   Generate docs: dbt docs generate
   Publish docs to a hosting service
   Send notifications on failures
   Update lineage tools (DataHub, Atlan, etc.)
FAILURE HANDLING:
   Tests fail → notify, but downstream models SKIPPED automatically (dbt build)
   Errors in models → fail the run, page on-call for tier-1
   Source freshness fails → distinct alert (upstream issue, not dbt)
■ Production scenario: A team had no slim CI. dbt builds took 90 min per PR. Engineers waited or skipped 'small'
changes through other channels. Some changes broke prod because they weren't built in CI. Adding slim CI
(state:modified+ --defer) dropped median PR build to 4 min. PR review turnaround tripled. Engineers trusted the
system and stopped routing around it. Slim CI is the single biggest velocity improvement for any non-trivial dbt
project.
```

#### 26. Performance & Cost Optimization

  - 26.1 The Cost Drivers
- WHERE WAREHOUSE COST COMES FROM:
- 1. FULL REBUILDS of huge tables (table materialization on big data)
- → Switch to incremental, bigger savings come from longer time between rebuilds
- 2. UNNECESSARY MATERIALIZATIONS (table when view would do)
- → Use views for cheap models; tables only for expensive recomputes
- 3. INEFFICIENT QUERIES IN MODELS
- → SELECT * on wide tables; full scans where filter would prune
- 4. EXCESSIVE CTE NESTING
- → 8-layer CTEs where the optimizer can't push predicates
- 5. TESTS QUERYING HUGE TABLES INEFFICIENTLY
- → Tests with full table scans on tier-1 fact tables
- 6. RUNS DURING PEAK BI HOURS
- → Compute contention with analyst queries
- 7. SAME TRANSFORMATION COMPUTED REPEATEDLY
- → Common upstream calculation duplicated across many marts

```sql
26.2 The Optimization Playbook
1. PROFILE — find your cost hogs
    Snowflake: QUERY_HISTORY + ACCOUNT_USAGE for top spend by query/user
    BigQuery: INFORMATION_SCHEMA.JOBS_BY_PROJECT for cost per query
    Look for: longest-running models, most-bytes-scanned, most-credits-used
2. SWITCH MATERIALIZATIONS
    Staging: view (not table)
    Mart small: table (rebuild fast)
    Mart big: incremental
    Reused intermediate: ephemeral (if only used 1-2 places) or table
3. INCREMENTAL WITH PROPER LOOKBACK
    Stop full-refreshing fact tables nightly
    Lookback window for late data, merge strategy for idempotency
4. CLUSTER / PARTITION
    Snowflake: CLUSTER BY (warehouse auto-clusters; choose keys)
    BigQuery: PARTITION BY (date), CLUSTER BY (high-card filter columns)
5. PREDICATE PUSHDOWN
    Filter early; WHERE clauses on smallest table first
    Avoid SELECT * on wide tables
6. PRE-AGGREGATE WHERE POSSIBLE
    Daily aggregates table for time-series dashboards
    Don't sum 5B rows on every dashboard load
7. SHARED INTERMEDIATE MODELS
    If 5 marts all compute the same join, materialize it once
8. RUN OFF-PEAK
    Schedule big builds for low-traffic windows
    Separate warehouse for transforms vs BI
```

```
26.3 Snowflake-Specific Optimizations
 AUTO-CLUSTERING — Snowflake handles re-clustering; you choose CLUSTER BY keys
   {{ config(cluster_by=['event_date', 'user_id']) }}
 WAREHOUSE SIZING — bigger for full rebuilds, smaller for incrementals
   pre_hook="ALTER WAREHOUSE TRANSFORM_PROD SET WAREHOUSE_SIZE = 'X-LARGE'"
 QUERY_TAG for cost attribution
   pre_hook="ALTER SESSION SET QUERY_TAG = 'dbt:{{ this.name }}'"
   → Then group QUERY_HISTORY by QUERY_TAG to see cost per model
 SEARCH OPTIMIZATION SERVICE — for point lookups on big tables
 MATERIALIZED VIEWS — when applicable; auto-maintained by Snowflake
```

```
26.4 BigQuery-Specific Optimizations
 PARTITION + CLUSTER — essential at scale
   {{ config(
       partition_by={'field': 'event_date', 'data_type': 'date'},
       cluster_by=['user_id', 'country']
   ) }}
 PARTITION FILTERS REQUIRED — set on huge tables to prevent full scans
   require_partition_filter=true
 REQUIRE PARTITION FILTER in incremental's WHERE
   {% if is_incremental() %}
     WHERE event_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 3 DAY)
   {% endif %}
 LABELS for cost attribution (similar to query tags)
 MATERIALIZED VIEWS for sub-second BI queries
 DEDUPLICATE BY USING MERGE rather than DELETE+INSERT (cheaper)
■ Principal mindset: Cost optimization is iterative — you don't fix it once. Build a monthly review: top 20 most
expensive models, top 20 most expensive queries, month-over-month change. Assign owners. The biggest wins are
usually switching unnecessary table materializations to view or incremental, and adding lookback windows to
incrementals that were doing full refreshes 'just in case'. At staff level, you own this — and you report it as KPI to
leadership.
```

```yaml
27. Lineage, Documentation & Discovery
27.1 dbt's Built-in Docs
# Model documentation
models:
  - name: fct_orders
    description: |
      One row per completed order, used by finance for revenue reporting.
      Owner: finance-data@company.com
      SLA: refreshed by 6am UTC daily
    config:
      meta:
        owner: finance
        tier: 1
    columns:
      - name: order_id
        description: "Stripe charge ID for the order"
        tests: [unique, not_null]
      - name: amount_cents
        description: "Order total in USD cents (multiply by 0.01 for dollars)"
# Generate docs
dbt docs generate
dbt docs serve            # local preview
# Docs include:
#    Model descriptions and column descriptions
#    DAG lineage graph
#    Compiled SQL (read what was actually executed)
#    Tests and their state
#    Source freshness
```

27.2 Doc Blocks for Reuse # docs/marts/finance.md {% docs amount_cents %} The order total in USD cents. Multiply by 0.01 to convert to dollars. This includes tax but excludes shipping. Refunds are negative. {% enddocs %} # Reference in YAML: columns: - name: amount_cents description: '{{ doc("amount_cents") }}' # Useful for: long descriptions, reused across models, formatted markdown

```
27.3 Lineage Tools
DBT DOCS — basic lineage graph, fine for small/medium projects
OPEN-SOURCE:
   DataHub (LinkedIn) — column-level lineage, search, governance
   OpenMetadata — open standard, broad ecosystem
   Marquez — built on OpenLineage spec
COMMERCIAL:
   Atlan — modern catalog, polished UX
   Alation — legacy enterprise; broad integrations
   Monte Carlo — observability + lineage
   Lightdash — open-source semantic layer + catalog
   Castor / data.world — discovery-focused
```

```
dbt &amp; Data Modeling: Principal Engineer Notes
WHAT THEY ADD:
   Cross-project lineage (dbt + Airflow + Tableau + ...)
   Column-level lineage (which fields drive which dashboards)
   Search across the data estate
   Stakeholder annotations and certifications
   Schema change impact analysis
```

```
27.4 The Documentation Discipline
ENFORCE IN CI:
   Every model has description
   Every PK / FK has description
   Every tier-1 model has owner tag
   Documentation tests via dbt_project_evaluator
dbt-labs/dbt_project_evaluator checks:
  - Models have descriptions
  - PK columns have unique + not_null tests
  - No models reference sources directly (must go through staging)
  - Staging models reference at most one source
  - All sources used / declared
  ... 50+ best practice rules
ADD TO CI:
  dbt build -s package:dbt_project_evaluator
■ Principal mindset: Documentation is operational, not cosmetic. A model without docs is a model that can't be
trusted by downstream consumers. At staff level, you enforce documentation as a CI requirement — not 'we should
document more', but 'PRs without descriptions fail CI'. The mindset shift: documentation is part of the model, not
separate from it.
```

#### 28. Anti-Patterns at Scale

```sql
28.1 The Catalog
MODELING ANTI-PATTERNS
  ■ No staging layer — marts reference sources directly
  ■ Mixed grain in fact tables (some rows at order, some at line item)
  ■ Joining facts directly to facts (no shared dimension)
  ■ Using natural keys as fact FKs (no SCD2 history)
  ■ Single mega-fact with 200 columns "to cover everything"
  ■ Overusing intermediate models (50 ints for a 100-model project)
  ■ Repeating same join logic in multiple marts (extract to intermediate)
  ■ Naming inconsistency: customer_id vs cust_id vs cid in different models
MATERIALIZATION ANTI-PATTERNS
  ■ Defaulting all marts to table
  ■ Incremental without lookback (silent late-arrival data loss)
  ■ Incremental with append on data with updates (duplicates)
  ■ Ephemeral models used by 10 downstream (inlined CTE explosion)
  ■ View materialization on expensive joins (every query reruns the join)
JINJA / MACRO ANTI-PATTERNS
  ■ Macros calling macros calling macros (debugging hellish)
  ■ run_query in macros run hundreds of times per dbt run
  ■ Magic numbers buried in macros (use vars or config)
  ■ Over-abstracting before pattern repetition justifies it
TESTING ANTI-PATTERNS
  ■ No tests at all
  ■ Tests warn instead of error on PKs (no enforcement)
  ■ Tests that take longer than the build itself
  ■ Same test logic copy-pasted (use generic tests or dbt-utils)
  ■ dbt run + dbt test (use dbt build to skip downstream on failure)
OPERATIONAL ANTI-PATTERNS
  ■ No CI / no slim CI (PR builds take 90 min)
  ■ No per-dev / per-PR schemas (collisions, hard to debug)
  ■ Production runs without alerts on failure
  ■ Docs never generated / served
  ■ Single warehouse for transforms + BI (contention)
  ■ Hand-edited tables in prod outside of dbt (drift, untracked)
  ■ No source freshness checks (silent upstream outages)
GOVERNANCE ANTI-PATTERNS
  ■ No model ownership (no one's on call for tier-1 marts)
  ■ No deprecation policy (consumers break on schema changes)
  ■ No data contracts (every PR can break dashboards)
  ■ Permissions: all team can edit all models (no domain boundaries)
  ■ Reverse ETL pulling from intermediate models (couples to internals)
■ Production scenario: A team's CI was 90 minutes per PR. Engineers stopped writing tests because 'CI takes
forever already'. Tests stagnated; data quality degraded. Adding slim CI dropped CI to 5 min. Engineers started
writing tests again. Quality recovered. The lesson: friction in your developer workflow has compounding downstream
effects. Slow CI doesn't just slow PRs — it changes engineer behavior in ways that hurt the whole system.
```

## PART VIII — INTERVIEW PRACTICE

#### 29. The Staff-Level Modeling Interview

  - 29.1 Question Categories
- 1. MODELING DESIGN  — "Design a star schema / dim model for X"
- 2. DBT MECHANICS    — "How does incremental work? When do you use snapshot?"
- 3. SCALE / PERF     — "Project has 2000 models; what slows down?"
- 4. TRADE-OFFS       — "Star vs OBT vs Data Vault for this workload"
- 5. CI/CD            — "How do you ship a dbt change safely?"
- 6. GOVERNANCE       — "Multi-team dbt project — how do you organize?"
- 7. ANTI-USE         — "When is dbt the wrong tool?"

```
29.2 The Design Framework
STEP 1: CLARIFY REQUIREMENTS
   Sources: what systems, what cadence?
   Consumers: who reads what; latency tolerance
   Volume: rows per day, total size
   Compliance: audit, PII, residency
STEP 2: SOURCES → STAGING
   One staging model per source table
   Conventions: column renames, type casts
   Tests on staging contracts
STEP 3: CONFORMED DIMENSIONS
   Identify dimensions used across multiple facts
   SCD decisions (most → SCD2 via snapshot)
   Surrogate keys via dbt_utils
STEP 4: FACTS
   Grain statement for each
   Materialization (table for small, incremental for huge)
   Tests: PK, FK, range, accepted_values
STEP 5: WIDE MARTS (optional)
   One per consumer use case
   Built FROM conformed dims + facts
STEP 6: TESTING + CI
   Tier-1 tests as errors
   Slim CI with state:modified+
   dbt build (not run + test)
STEP 7: ORCHESTRATION + MONITORING
   Airflow / Dagster / dbt Cloud schedules
   Failure alerts; SLA on tier-1
   Source freshness checks
```

```
29.3 Worked Example: E-commerce Analytics
INTERVIEWER: "Design dbt project for an e-commerce company.
              Sources: orders, customers, products (Postgres CDC).
              Consumers: finance daily dashboards, marketing cohort analysis,
              data science feature store."
```

```
dbt &amp; Data Modeling: Principal Engineer Notes
STAFF RESPONSE:
"CLARIFICATIONS:
    Volume: how many orders/day? (assume 100K/day, 5B historical)
    Refresh frequency? (assume hourly; daily for some dashboards)
    PII concerns on customer data? (assume yes; mask in dev)
 STAGING LAYER (views):
   stg_postgres__orders            (cleaned, renamed)
   stg_postgres__customers
   stg_postgres__products
   stg_postgres__order_items
```

```
   Each: column renames to snake_case, type casts, contract tests.
 INTERMEDIATE (ephemeral or view):
   int_orders__pivoted              (orders × line_items joined)
   int_customers__metrics           (LTV, order count, signup cohort)
 SNAPSHOTS (raw, before any transform):
   snapshot_postgres_customers       (timestamp strategy)
   snapshot_postgres_products
   → For SCD2 needs downstream
 CONFORMED DIMENSIONS (table or incremental):
   dim_customers       (from snapshot — SCD2)
   dim_products        (from snapshot — SCD2)
   dim_date            (statically generated; reused)
 FACT TABLES:
   fct_orders          (incremental, merge, unique_key=order_id,
                        24-hour lookback)
   fct_order_items     (incremental, merge, unique_key=order_item_id)
   fct_daily_revenue   (table, rebuilt nightly from aggregating fct_orders)
 WIDE MARTS (for consumers):
   obt_orders          (denormalized for finance dashboards)
   obt_customer_lifecycle  (for marketing cohorts)
   feat_customer_aggregates  (for data science feature store)
 ORCHESTRATION (Airflow):
    Hourly: dbt build -s tag:hourly (incremental facts)
    Daily 2 AM: dbt build (full refresh of small models + tests)
    Daily 6 AM: dbt docs generate + publish
 CI:
    Every PR: sqlfluff lint + dbt build -s state:modified+ --defer
    Tests against PR's schema
    audit_helper compare for refactor PRs
 OPERATIONAL:
    Sources freshness: warn 6h, error 12h
    Slack alerts on tier-1 model failures
    Per-team schemas: finance.*, marketing.*, ds.*"
WHAT MAKES THIS STAFF-LEVEL:
   Started with clarifying questions
   Layered architecture explicit
   Materialization choices reasoned, not defaulted
   Snapshots used for SCD2 (not custom incremental)
   Wide marts AS WELL AS canonical model
   CI / orchestration mentioned, not skipped
```

#### 30. War Stories & Cheat Sheet

30.1 War Stories ■ Production scenario: The Refresh That Wouldn't Refresh. A team's incremental model was supposedly running hourly. Customer support reported stale data. Investigation: the model's WHERE updated_at > MAX(updated_at) had been working fine for a year. Then one day the source system reset updated_at on some rows to 1970 01 (Unix epoch — null defaulted by a migration script). MAX(updated_at) in the target table was now 2024 — the incremental was permanently filtering out those rows. Fix: use a separate ingestion timestamp column controlled by us, not the source's updated_at. ■ Production scenario: The Materialization Storm. A team had ~800 models, all defaulted to table materialization. Nightly dbt run took 4 hours, cost $2K. Switching staging models to view, marts to incremental, and ephemeral for intermediate models dropped runtime to 25 minutes and cost to $100/night. Material choices compound. ■ Production scenario: The Macro Pyramid. A senior engineer abstracted a date-handling pattern into a macro. Then that macro became part of another macro. Then another. Six months later, debugging a date issue required understanding 5 levels of Jinja expansion. The compiled SQL was unreadable. Fix: flattened the macro hierarchy; lost ~50 lines of code; team velocity doubled because PRs could be reviewed. ■ Production scenario: The Forgotten Source Freshness. A Fivetran sync silently broke. Downstream marts ran on stale data for 5 days. BI dashboards showed slowly declining numbers (the daily incremental wasn't getting new rows). Finance noticed when month-end totals didn't add up. Adding dbt source freshness as a critical alert (both warn and error thresholds) would have caught this within hours. ■ Production scenario: The Test Stampede. An overzealous team added 12 tests to every column of every model. dbt test took 90 minutes and cost $300 per run. Most tests were redundant (testing the same invariant from multiple angles). Pruning to tier-1 critical tests + a few business invariants per model brought test time to 8 minutes.

```
30.2 The Five Mental Models
1. LAYERED ARCHITECTURE
   Sources → Staging → Intermediate → Marts → Wide Marts.
   Each layer is a contract.
2. ELT, NOT ETL
   Load raw first. Transform in the warehouse. dbt is the T.
3. MATERIALIZATIONS ARE COST CHOICES
   View = compute on every query. Table = compute on build.
   Incremental = compute on new data only.
4. dbt IS SOFTWARE ENGINEERING
   Tests, CI, code review, versioning, documentation.
5. DBT DOESN'T ORCHESTRATE
   You need Airflow / Dagster / Prefect / dbt Cloud Scheduler.
```

```
30.3 The One-Page Reference
PROJECT STRUCTURE
  models/
    staging/                      view materialized
      stg_<source>__<table>.sql
    intermediate/                 ephemeral or view
      int_<entity>__<verb>.sql
```

```sql
dbt &amp; Data Modeling: Principal Engineer Notes
    marts/                        table or incremental
      <domain>/
        dim_<entity>.sql
        fct_<event>.sql
INCREMENTAL TEMPLATE (production-grade)
  {{ config(
      materialized='incremental',
      unique_key='id',
      incremental_strategy='merge',
      on_schema_change='append_new_columns'
  ) }}
  SELECT ...
  FROM {{ ref('source_model') }}
  {% if is_incremental() %}
    WHERE updated_at >= (
      SELECT DATEADD(hour, -6, MAX(updated_at)) FROM {{ this }}
    )
  {% endif %}
SNAPSHOT TEMPLATE (SCD2)
  {% snapshot dim_customers_snapshot %}
  {{ config(
      target_schema='snapshots',
      unique_key='customer_id',
      strategy='timestamp',
      updated_at='updated_at',
      invalidate_hard_deletes=True
  ) }}
  SELECT * FROM {{ source('postgres', 'customers') }}
  {% endsnapshot %}
ESSENTIAL COMMANDS
  dbt build -s state:modified+   slim CI build
  dbt build                       full prod run
  dbt build -s tag:hourly         scheduled subset
  dbt run --full-refresh -s X     rebuild from scratch
  dbt test --select source:*      validate source contracts
  dbt source freshness            check upstream
ESSENTIAL TESTS
  - unique
  - not_null
  - accepted_values (status enums)
  - relationships (FKs)
  - dbt_utils.expression_is_true
  - dbt_expectations.* (richer assertions)
DEPLOY CHECKLIST
  ■ Slim CI passing (state:modified+)
  ■ Tests passing
  ■ sqlfluff lint passing
  ■ Docs updated (model + columns)
  ■ Owner tag set
  ■ Tier tag set
  ■ Source freshness configured
  ■ Failure alerts wired
```

```
30.4 What Separates Senior from Staff
SENIOR                                STAFF / PRINCIPAL
■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■
"Star schema"                        "Star for conformed core, wide
                                      marts for consumption, snapshots
                                      for history. Different layers
                                      serve different consumers"
```

```
dbt &amp; Data Modeling: Principal Engineer Notes
"Use incremental"                    "Incremental with merge strategy,
                                      24h lookback for late arrivals,
                                      on_schema_change=append_new_columns.
                                      Full refresh on weekend if drift"
"Add a test"                         "Tier-1 tests as errors on PK/FK,
                                      tier-2 as warn for invariants,
                                      tier-3 in nightly anomaly detect.
                                      Don't drown engineers in warnings"
"Use dbt"                            "dbt for SQL transforms in the
                                      warehouse; orchestrated by Airflow.
                                      dbt isn't an orchestrator, isn't
                                      ingestion, isn't observability —
                                      we have separate tools for each"
"CI runs dbt test"                   "Slim CI: state:modified+ --defer
                                      against prod manifest. Per-PR
                                      schema. audit_helper for refactor
                                      validation. Avg PR build: 4 min"
"Document models"                    "Documentation enforced in CI via
                                      dbt_project_evaluator. Owners and
                                      tiers on every model. Lineage
                                      flows into DataHub for cross-tool
                                      discovery"
"Star vs OBT"                        "Build both. Star is canonical
                                      (governance, history, dim conformity);
                                      OBT is consumption (BI/ML perf).
                                      Source of truth, then optimized
                                      derivatives"
■ Principal mindset: dbt mastery at staff level is mostly about discipline — software engineering applied to SQL.
Tests, CI, code review, documentation, versioning, contracts. The technical complexity is modest; the cultural
complexity is high. The best staff engineers I've worked with don't write more clever models — they make the team's
average model better through conventions, tooling, and review.
 — End of dbt & Data Modeling Notes —
 Next in series: Cloud Data Platforms, Lakehouses, System Design
```


---

[← Back to main](./README.md)
