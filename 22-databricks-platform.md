# Topic 22 — Databricks Platform

> Standalone deep-dive · The Databricks Data Intelligence Platform (beyond just Spark)

[← Back to main](./README.md)

---

## How to use this document

This chapter covers Databricks as a **platform**, not just managed Spark. Separate chapters cover Delta Lake (`12-delta-lake.md`), PySpark (`02-pyspark.md`), and warehouses (`07-cloud-warehouses.md`). This chapter is for the full platform: Unity Catalog, Delta Live Tables, Workflows, Mosaic AI, Databricks SQL, Model Serving, Lakehouse Federation, Lakehouse Monitoring, and deployment patterns. Every chapter follows the four lenses: **Concept · Mechanics · Trade-offs · Interview**.

> **🧠 Principal Mindset:** Databricks in 2026 markets itself as the "Data Intelligence Platform" — not a warehouse, not Spark, not an ML tool, but a unified platform spanning all of them. The staff signal is articulating what that MEANS architecturally (lakehouse foundation, Unity Catalog as metadata backbone, Mosaic AI as the AI layer, Workflows as orchestration) and defending it against alternatives (Snowflake, Microsoft Fabric, open lakehouse). "We use Databricks" in 2026 is as ambiguous as "we use AWS" — the platform surface is large.

## Table of Contents

**PART I — FOUNDATIONS**
- 1. The Databricks Vision in 2026
- 2. Lakehouse Architecture Recap
- 3. Workspace, Account, and Deployment Model

**PART II — STORAGE AND GOVERNANCE**
- 4. Delta Lake as the Storage Foundation
- 5. Unity Catalog (The Governance Layer)
- 6. Delta Sharing and the Marketplace
- 7. Lakehouse Federation

**PART III — COMPUTE**
- 8. Databricks Cluster Types
- 9. Databricks SQL Warehouses
- 10. Serverless Compute
- 11. Photon Engine

**PART IV — DATA ENGINEERING**
- 12. Delta Live Tables (DLT)
- 13. Databricks Workflows
- 14. Auto Loader (File Ingestion)
- 15. Lakeflow Connect (Managed CDC)
- 16. Streaming on Databricks

**PART V — SQL AND BI**
- 17. Databricks SQL (DBSQL)
- 18. Materialized Views and Streaming Tables
- 19. AI/BI Dashboards and Genie

**PART VI — AI AND ML**
- 20. Mosaic AI Overview
- 21. Model Serving and AI Gateway
- 22. Vector Search
- 23. Mosaic AI Agent Framework
- 24. MLflow 3.x on Databricks

**PART VII — PLATFORM AND OPERATIONS**
- 25. Databricks Asset Bundles (IaC)
- 26. Lakehouse Monitoring
- 27. Cost Optimization Patterns

**PART VIII — INTERVIEW PRACTICE**
- 28. The Staff-Level Databricks Interview
- 29. War Stories
- 30. Cheat Sheet &amp; Mental Models

---

## PART I — FOUNDATIONS

### 1. The Databricks Vision in 2026

#### 1.1 From Managed Spark to Data Intelligence Platform

```
2013-2017: DATABRICKS = managed Spark.
   Positioning: easier Spark than running EMR yourself.

2017-2020: DATABRICKS + DELTA LAKE.
   Added Delta Lake, invented the "Lakehouse" category.
   Positioning: warehouse-like SQL on open storage.

2020-2024: UNIFIED PLATFORM.
   Added DLT, Workflows, SQL warehouses, Unity Catalog, Mosaic AI.
   Positioning: one platform for ETL + SQL + ML + AI.

2024-2026: DATA INTELLIGENCE PLATFORM.
   AI-native: Genie (NL to SQL), Agent Framework, AI Functions, 
      Mosaic AI Model Serving, Vector Search.
   Positioning: your data + your AI, one platform.
```

#### 1.2 The Platform Surface in 2026

```
Databricks platform components (2026):

   STORAGE:
      Delta Lake (default)
      Iceberg (UniForm interop; managed Iceberg GA)
      Unity Catalog managed storage
   
   GOVERNANCE:
      Unity Catalog (metadata, lineage, access control)
      Lakehouse Monitoring (quality + drift)
      Delta Sharing (cross-org)
   
   COMPUTE:
      All-Purpose clusters (interactive notebooks)
      Jobs clusters (ephemeral; cheaper)
      Databricks SQL warehouses (BI)
      Serverless (SQL, Jobs, Model Serving, DLT)
      Photon (vectorized C++ engine)
   
   DATA ENGINEERING:
      Delta Live Tables (DLT) — declarative pipelines
      Workflows — orchestration
      Auto Loader — incremental file ingestion
      Lakeflow Connect — managed CDC (Postgres, SF, SQL Server, etc.)
   
   SQL / BI:
      Databricks SQL (DBSQL)
      Materialized Views + Streaming Tables
      AI/BI Dashboards + Genie (NL to analytics)
   
   AI / ML:
      Mosaic AI (model serving, vector search, agent framework)
      MLflow 3.x (unified GenAI + ML)
      Model Serving (REST endpoints)
      AI Gateway (govern LLM calls)
      Vector Search
      Foundation Model APIs (Llama, Mistral, DBRX)
      Agent Evaluation
   
   OPERATIONS:
      Asset Bundles (deploy as code)
      REST API, Terraform provider
      Observability + lineage
```

#### 1.3 Why This Matters

```
The Databricks platform in 2026 overlaps Snowflake, Microsoft Fabric,
cloud-native data stacks (AWS Lake Formation + Athena + Glue), 
and MLOps vendors (Weights &amp; Biases, SageMaker).

Staff engineers must know:
   • What Databricks does well vs others
   • Where it's weakest
   • What the lock-in surface is
   • What "going multi-engine" looks like with Databricks as one engine
```

### 2. Lakehouse Architecture Recap

#### 2.1 Lakehouse = Lake + Warehouse

```
Databricks invented the "Lakehouse" category:
   
   LAKE: open storage (S3/ADLS/GCS), open formats (Parquet, Delta, Iceberg)
   WAREHOUSE: ACID transactions, performance, governance, BI tool support
   
   = LAKEHOUSE: get both.

Databricks' lakehouse foundation:
   Delta Lake (ACID + performance)
   Unity Catalog (governance)
   Photon (SQL execution engine)
   = compete with warehouses on their turf
```

#### 2.2 Medallion Architecture (Databricks' pattern)

```
BRONZE (raw): data as it arrives, minimal transformation
   Typically append-only
   Raw CDC, raw events, raw files

SILVER (cleaned, conformed): deduped, validated, joined with reference data
   The "operational truth" layer
   Streams-friendly format (append or SCD2)

GOLD (business): aggregates, metrics, dimensional models
   Consumed by BI, ML features, apps
   Often star schema or OBT
```

See `05-dbt-modeling.md` (section on medallion) for depth.

### 3. Workspace, Account, and Deployment Model

#### 3.1 Hierarchy

```
ACCOUNT (one Databricks account per customer)
   ├── METASTORES (one per region typically; Unity Catalog metastore)
   │     ├── CATALOGS (namespace)
   │     │     ├── SCHEMAS
   │     │     │     ├── TABLES, VIEWS, FUNCTIONS, VOLUMES, MODELS
   └── WORKSPACES (environment-specific; dev, staging, prod)
         ├── CLUSTERS
         ├── NOTEBOOKS
         ├── JOBS / WORKFLOWS
         ├── SQL WAREHOUSES
         ├── MODELS, SERVING ENDPOINTS

A workspace attaches to ONE metastore.
Multiple workspaces can share one metastore (dev + staging + prod).
```

#### 3.2 Deployment Model

```
Databricks is MULTI-CLOUD:
   AWS: deployed as a SaaS layer; compute runs in your AWS account 
       via the Databricks control plane
   Azure: deployed as a first-party Azure service (Azure Databricks)
   GCP: deployed in your GCP project
   
   Control plane: Databricks-managed (notebooks, UI, metadata)
   Data plane: in your cloud account (compute, data)
```

#### 3.3 Workspace Patterns

```
PATTERNS:
   Single-workspace: dev/staging/prod separation via catalogs + folders
      Simpler; cheaper; less isolation
   
   Multi-workspace (per environment): dev / staging / prod workspaces
      Standard enterprise pattern
      Share one Unity Catalog metastore for consistent lineage
   
   Multi-workspace (per team): team isolation
      Good for very large orgs with team-specific needs
      Shared metastore for cross-team sharing
```

---

## PART II — STORAGE AND GOVERNANCE

### 4. Delta Lake as the Storage Foundation

#### 4.1 Why Delta is Databricks' Default

```
Delta Lake was invented at Databricks.
   Open-sourced; Linux Foundation project.
   But Databricks owns the MANAGED experience + optimizations.

Databricks-exclusive Delta features (2024-2026):
   • Liquid Clustering: data layout without rigid partitioning
   • Predictive Optimization: auto-OPTIMIZE and VACUUM
   • Deletion Vectors: ms-latency deletes
   • UniForm: writes Delta + Iceberg metadata in one table
   • Z-Ordering: multi-column data skipping (via OPTIMIZE)

See `12-delta-lake.md` for full depth.
```

#### 4.2 Iceberg Interop

```
UNIFORM (introduced 2023, GA 2024):
   Writes Delta data files + Iceberg metadata simultaneously
   One physical table readable by Delta engines AND Iceberg engines
   Only one write engine (Databricks) but many read engines

MANAGED ICEBERG TABLES (2024+):
   Databricks can be the WRITE engine for native Iceberg tables too
   Compatible with external catalogs (Polaris, Glue)

TAKEAWAY:
   Databricks is positioning for the open-format future.
   You can standardize on Databricks compute while keeping storage open.
```

### 5. Unity Catalog (The Governance Layer)

#### 5.1 What Unity Catalog Is

```
UNITY CATALOG = Databricks' unified governance layer.
   
   Provides:
      Three-level namespace: catalog.schema.object
      Access control (ANSI SQL grants + attribute-based rules)
      Lineage (table + column level)
      Audit logs
      Data discovery (search across the metastore)
      Data classification
      Column + row masking
      External locations (storage credentials)
      Volumes (non-tabular files: images, documents, model artifacts)
      Managed models (ML model registry)
      Service credentials
```

#### 5.2 Why Unity Catalog Matters

```
Before Unity Catalog:
   Hive Metastore per workspace (siloed)
   Table ACLs limited; cluster-mode-specific
   Lineage was a separate product (or absent)
   Cross-workspace sharing was manual

With Unity Catalog:
   One metastore across workspaces
   Fine-grained access control (RBAC with attribute policies)
   Automatic lineage for Delta operations
   Governance for ML models, files (volumes), features
   Cross-workspace is native

Unity Catalog is the TABLE STAKES for enterprise Databricks in 2026.
```

#### 5.3 Unity Catalog Objects

```sql
-- Three-level namespace
SELECT * FROM finance.silver.transactions;
            ^^^^^^^ ^^^^^^ ^^^^^^^^^^^^^
            catalog schema table

-- External location (points to a cloud storage path)
CREATE EXTERNAL LOCATION finance_data
    URL 's3://finance-bucket/data'
    WITH (CREDENTIAL my_cred);

-- Volume (non-tabular files)
CREATE VOLUME finance.ml.model_artifacts;

-- Row filter
CREATE FUNCTION region_filter(region STRING) RETURNS BOOLEAN
RETURN CURRENT_USER() IN (SELECT u FROM allowed_regions WHERE r = region);

ALTER TABLE sales SET ROW FILTER region_filter ON (region);

-- Column mask
CREATE FUNCTION mask_ssn(ssn STRING) RETURNS STRING
RETURN CASE WHEN is_member('pii_viewer') THEN ssn 
            ELSE 'XXX-XX-' || RIGHT(ssn, 4) END;

ALTER TABLE customers ALTER COLUMN ssn SET MASK mask_ssn;
```

### 6. Delta Sharing and the Marketplace

#### 6.1 Delta Sharing

```
DELTA SHARING = open protocol for data sharing.
   Open source + Databricks-managed
   Share Delta tables (or Iceberg) across:
      • Workspaces
      • Databricks accounts
      • Non-Databricks consumers (Spark, Pandas, Power BI, Tableau)
   
   Zero data copying; consumers query live
```

#### 6.2 Databricks Marketplace

```
MARKETPLACE = catalog of shared datasets, AI models, notebooks
   Powered by Delta Sharing
   Free + paid listings
   Install directly into your Unity Catalog
   
   USE: discover enrichment data, AI models, 
   reusable notebooks without ETL
```

### 7. Lakehouse Federation

#### 7.1 What Lakehouse Federation Is

```
LAKEHOUSE FEDERATION = query external databases FROM Databricks.
   
   Register a connection (Postgres, MySQL, SQL Server, Snowflake, Redshift, BigQuery, ...)
   The connection becomes a FOREIGN CATALOG in Unity Catalog
   SQL queries can join Databricks tables to external tables transparently
   
   Query pushdown where supported
```

#### 7.2 Use Cases

```
✓ Query operational Postgres from Databricks notebooks (no CDC pipeline for ad-hoc)
✓ Join Snowflake data with Delta tables for one-off analysis
✓ Unified governance over federated sources (Unity Catalog grants apply)
✗ Not a replacement for proper ETL (performance limited by federation overhead)
```

---

## PART III — COMPUTE

### 8. Databricks Cluster Types

#### 8.1 All-Purpose vs Jobs Clusters

```
ALL-PURPOSE CLUSTER:
   Shared, long-running, interactive
   Multiple notebooks attached
   Pay for uptime (even idle)
   USE: notebook development, ad-hoc analysis

JOBS CLUSTER:
   Ephemeral; created per job run, terminated after
   Cheaper per DBU (lower rate)
   USE: scheduled production jobs

RULE: never run production on all-purpose clusters.
```

#### 8.2 Cluster Modes (legacy context)

```
Historically Databricks had cluster modes (High Concurrency, etc.)
Modern Unity Catalog deployments use Access Mode:

ACCESS MODES:
   SINGLE USER: isolated per user; full language support
   SHARED: multi-user; Unity Catalog required; SQL + Python/Scala limited
   NO ISOLATION SHARED: legacy; avoid
   
Choose based on who uses the cluster and what languages they need.
```

#### 8.3 Node Types and Pools

```
NODE TYPES: cloud VM shapes (e.g., AWS m5d.4xlarge, Azure Standard_D16s_v3)
   Memory-optimized, compute-optimized, GPU, etc.

INSTANCE POOLS: pre-warmed VMs
   Reduce cluster start time from minutes to seconds
   Pay only when borrowed by a cluster
   USE: fast-start workloads, interactive performance
```

### 9. Databricks SQL Warehouses

```
DATABRICKS SQL WAREHOUSES = SQL-optimized compute pools.
   
   Three types:
      CLASSIC: runs in your cloud account; traditional cluster
      PRO: adds Photon + Serverless features; still in your account
      SERVERLESS: fully managed by Databricks; starts in seconds; cheapest for 
                  variable workloads (recommended default in 2026)
   
   Sizes: 2X-Small → 4X-Large
   Scales horizontally with min/max clusters for concurrency
   
   USE: BI dashboards, interactive SQL, analytics
```

### 10. Serverless Compute

```
DATABRICKS SERVERLESS (expanding since 2023):
   
   Serverless SQL warehouses (GA, mature)
   Serverless Jobs (GA)
   Serverless DLT (GA)
   Serverless Model Serving (GA)
   Serverless notebooks (GA for most clouds)
   
   PROPERTIES:
      Fast startup (seconds)
      No cluster management
      Databricks manages compute pools
      Pay for compute used
   
   TRADE-OFF: pricier per DBU than classic, 
      but much cheaper for bursty/short-lived workloads
```

### 11. Photon Engine

```
PHOTON = native (C++) vectorized execution engine.
   
   REPLACES: parts of Spark's JVM-based query execution with vectorized C++ code
   
   PERFORMANCE: 2-10x faster for SQL and DataFrame workloads
   
   USAGE: enabled by default on DBSQL warehouses + many cluster types
          Transparent; no code change needed
   
   COST: Photon-enabled compute has higher DBU rate (justified by speed)
   
   LIMITS: some operations fall back to JVM Spark (UDFs, 
     exotic functions) — minor performance cliff
```

---

## PART IV — DATA ENGINEERING

### 12. Delta Live Tables (DLT)

#### 12.1 What DLT Is

```
DELTA LIVE TABLES (DLT) = declarative framework for building pipelines.
   
   You declare: table definitions as SQL or Python
   DLT handles:
      Dependency resolution (DAG of tables)
      Incremental or full refresh
      Expectations (data quality checks)
      Streaming or batch (unified)
      Autoscaling
      Serverless execution

Think "dbt for the lakehouse, with built-in streaming + quality."
```

#### 12.2 Example

```python
import dlt

@dlt.table
def bronze_orders():
    return spark.readStream.format("cloudFiles") \
        .option("cloudFiles.format", "json") \
        .load("/mnt/raw/orders")

@dlt.table
@dlt.expect("valid_amount", "amount > 0")
@dlt.expect_or_drop("has_customer", "customer_id IS NOT NULL")
def silver_orders():
    return dlt.read_stream("bronze_orders") \
        .select("id", "customer_id", "amount", "ts")

@dlt.table
def gold_daily_revenue():
    return dlt.read("silver_orders") \
        .groupBy(F.date_trunc("day", "ts").alias("day")) \
        .agg(F.sum("amount").alias("revenue"))
```

#### 12.3 DLT Properties

```
✓ Declarative: you define tables, DLT builds the DAG
✓ Streaming + batch unified: use read() or read_stream()
✓ Data quality: expectations with warn / drop / fail modes
✓ Lineage: auto-generated, visible in Unity Catalog
✓ Observable: DLT UI shows DAG + run status + metrics
✓ Serverless option: no cluster management
```

#### 12.4 DLT vs dbt vs Workflows

```
DLT:
   Native Databricks; declarative tables
   Streaming + batch unified
   Built-in quality checks
   Best for: lakehouse-centric pipelines with streaming

dbt on Databricks:
   SQL-first; software engineering workflow (git, tests)
   Mature ecosystem (packages, docs, lineage)
   Best for: SQL-heavy analytics engineering

Workflows:
   General orchestration (any task)
   Best for: multi-tool DAGs (notebook + SQL + Python script + DLT)

STAFF ANSWER: often use ALL THREE:
   Workflows orchestrates the end-to-end pipeline
   DLT handles declarative transformations
   dbt for pure SQL analytics models
```

### 13. Databricks Workflows

#### 13.1 What Workflows Is

```
DATABRICKS WORKFLOWS = orchestration service.
   
   Create jobs with tasks:
      Notebook tasks
      Python script tasks
      JAR tasks
      SQL tasks (dashboard, query, alert)
      DLT pipeline tasks
      dbt tasks
      Spark Submit tasks
   
   Tasks can depend on each other (DAG)
   Trigger on schedule, continuously, or on file arrival
```

#### 13.2 Workflow vs Airflow

```
WORKFLOWS:
   Native Databricks
   Simpler for Databricks-only pipelines
   Serverless option
   Tight integration with DLT, DBSQL, Unity Catalog

AIRFLOW:
   Industry-standard orchestrator
   Multi-system (not Databricks-only)
   Rich ecosystem (operators, plugins)
   Mature observability

CHOICE:
   Databricks-only pipelines: Workflows (simpler, cheaper)
   Multi-system pipelines: Airflow (orchestrates Databricks + other systems)
```

### 14. Auto Loader (File Ingestion)

#### 14.1 What Auto Loader Is

```
AUTO LOADER = incremental file ingestion from cloud storage.
   
   Discovers new files automatically
   Uses cloud notification (SNS, Event Grid, Pub/Sub) or directory listing
   Checkpoints progress (exactly-once semantics)
   Infers schema; evolves schema automatically
   Handles billions of files efficiently
```

#### 14.2 Example

```python
df = (spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "parquet")
    .option("cloudFiles.schemaLocation", "/mnt/schemas/orders")
    .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
    .load("/mnt/raw/orders"))

(df.writeStream
    .format("delta")
    .option("checkpointLocation", "/mnt/checkpoints/orders")
    .toTable("bronze.orders"))
```

Auto Loader is Databricks' answer to "efficient streaming file ingest at scale."

### 15. Lakeflow Connect (Managed CDC)

```
LAKEFLOW CONNECT (GA 2024+):
   Managed CDC ingestion from operational databases.
   
   Supported sources (2026):
      SQL Server, PostgreSQL, MySQL, Oracle
      Salesforce, Workday, ServiceNow
      Google Analytics
   
   Features:
      Managed; no Debezium or Kafka to run
      Schema evolution
      Transforms applied in-flight
      Lands in Delta via DLT

Replaces: Fivetran-style ingestion, Debezium pipelines, DMS, Datastream
```

### 16. Streaming on Databricks

```
Databricks runs Structured Streaming (Spark Streaming) at scale.
   
   CAPABILITIES:
      Micro-batch (sub-second to minutes)
      Continuous processing (experimental, low-latency)
      Stateful stream processing
      Checkpointing (exactly-once with sinks that support it)
      Watermarks for late data

PATTERNS:
   Kafka → Structured Streaming → Delta (standard)
   Auto Loader → Structured Streaming → Delta (file-based)
   CDC source → Lakeflow Connect → Delta (managed)
   DLT with streaming → Delta (declarative)

For true millisecond latency: Flink (not Databricks).
Databricks Structured Streaming is best for seconds-to-minute latency.
```

---

## PART V — SQL AND BI

### 17. Databricks SQL (DBSQL)

```
DBSQL = SQL-first experience on the lakehouse.
   
   Components:
      SQL Editor (notebook-like for SQL)
      SQL Warehouses (compute)
      Queries (saved SQL)
      Dashboards (visualizations)
      Alerts (threshold-based notifications)

PERFORMANCE:
   Photon engine
   Query caching (results + disk cache)
   Serverless warehouses for second-start times
   
   Competitive with Snowflake/BigQuery on most BI workloads.
```

### 18. Materialized Views and Streaming Tables

#### 18.1 Materialized Views (MVs)

```
MATERIALIZED VIEWS on DBSQL:
   Precomputed results stored as Delta tables
   Auto-refresh (incremental where possible)
   Query from BI tools / Genie
   Pay for refresh compute + storage
   
   USE: expensive aggregations consumed repeatedly
```

#### 18.2 Streaming Tables

```
STREAMING TABLES:
   Delta tables continuously fed by streaming source
   Managed refresh via DLT under the hood
   Queryable like regular tables
   
   USE: real-time dashboards fed by Kafka / Auto Loader
```

### 19. AI/BI Dashboards and Genie

#### 19.1 AI/BI Dashboards

```
AI/BI DASHBOARDS (GA 2024):
   Databricks-native BI tool
   Built on DBSQL
   Visualizations, filters, drill-downs
   Share via embedded dashboards or links
   
   Competitive with: Power BI, Tableau for lakehouse-centric BI
```

#### 19.2 Genie (NL to SQL)

```
AI/BI GENIE = natural language to analytics.
   
   Admin defines "Genie Spaces" with:
      Allowed tables + certified metrics
      Example queries + explanations
   
   Business users ask questions in English
   Genie generates SQL + visualizations
   Results explainable; user can drill into the SQL
   
   USE: self-service analytics with guardrails
```

---

## PART VI — AI AND ML

### 20. Mosaic AI Overview

```
MOSAIC AI = Databricks' AI/ML platform (acquired MosaicML in 2023).

Components:
   • Model Serving (endpoints)
   • AI Gateway (govern LLM calls)
   • Vector Search
   • Foundation Model APIs (hosted LLMs)
   • Agent Framework
   • Agent Evaluation
   • Mosaic AI Model Training (fine-tuning + pre-training)
   • Playground (LLM testing UI)
   • MLflow 3.x integration

POSITIONING: compete with OpenAI, Anthropic (as hosted LLM APIs on your data), 
   Weights &amp; Biases (MLOps), SageMaker (end-to-end ML).
```

### 21. Model Serving and AI Gateway

#### 21.1 Model Serving

```
DATABRICKS MODEL SERVING:
   REST endpoints for your ML models (classical or GenAI)
   Serverless (auto-scaling)
   Governed via Unity Catalog
   
   Supports:
      MLflow models (custom)
      Foundation models (Llama, DBRX, Mistral, Mixtral)
      External models (OpenAI, Anthropic, Google)
      Vector Search indexes
```

#### 21.2 AI Gateway

```
AI GATEWAY = governed access to LLMs.
   
   Features:
      Route to multiple providers (OpenAI, Anthropic, Databricks, etc.)
      Rate limiting
      Usage tracking per user / team
      PII filtering
      Payload logging for audit
      Fallback routing
   
   USE: control LLM spend + govern access centrally
```

### 22. Vector Search

```
VECTOR SEARCH on Databricks:
   
   Managed vector database integrated with Unity Catalog
   Index Delta tables (automatically embedding + indexing)
   Hybrid search (vector + keyword)
   Delta-sync (updates propagate when source changes)
   
   USE: RAG applications, semantic search, 
        similarity recommendations.
   
   Competes with: Pinecone, Weaviate, Elastic for Databricks-committed shops.
```

### 23. Mosaic AI Agent Framework

```
MOSAIC AI AGENT FRAMEWORK (GA 2024+):
   Build, deploy, evaluate LLM agents on Databricks data.
   
   Capabilities:
      LangChain / LlamaIndex integration
      Deploy as Model Serving endpoint
      Agent Evaluation: LLM-as-judge quality + grounding metrics
      MLflow Tracing: step-by-step agent execution logs
      Govern via Unity Catalog
   
   USE: production RAG + agentic workflows on your data.
```

### 24. MLflow 3.x on Databricks

```
MLflow 3.x (2024+) UNIFIED tracking:
   Traditional ML experiments
   GenAI / LLM experiments
   Agents
   
   Managed on Databricks:
      No MLflow server to run
      Integrated with Unity Catalog (models as first-class UC objects)
      Model Serving one-click deploy
      Audit logs, lineage

STAFF SIGNAL: in 2026, MLflow is the standard tracking tool for both
   classical ML AND GenAI. Databricks hosts it as a managed service.
```

---

## PART VII — PLATFORM AND OPERATIONS

### 25. Databricks Asset Bundles (IaC)

```
ASSET BUNDLES = infrastructure-as-code for Databricks resources.
   
   YAML definitions for:
      Jobs / Workflows
      DLT pipelines
      ML experiments
      Dashboards
      Clusters, pools, SQL warehouses
      Permissions, grants

Deploy via CLI:
   databricks bundle deploy --target prod
   
   Mix-match with Terraform (for broader infra) or standalone.
```

### 26. Lakehouse Monitoring

```
LAKEHOUSE MONITORING (GA 2024+):
   
   Auto-tracks data quality metrics for Delta tables:
      Statistical profiles (null rates, distributions)
      Drift detection (how data changes over time)
      Freshness (SLA checks)
      Volume (expected row counts)
   
   Dashboard + alerts
   USE: production data quality monitoring without custom tooling
```

### 27. Cost Optimization Patterns

```
TOP COST CONTROLS:
   1. Jobs clusters, not all-purpose, for production
   2. Autoscaling with sensible min/max
   3. Spot instances for jobs (with on-demand driver + fallback)
   4. Serverless for bursty workloads
   5. Photon on for SQL + eligible DataFrame work
   6. Scheduled cluster termination (90-120 min max for all-purpose)
   7. Predictive Optimization for Delta tables (OPTIMIZE + VACUUM)
   8. DBSQL serverless for BI (vs classic)
   9. Instance pools to amortize startup
   10. Table-level and user-level cost attribution (via system tables)

Databricks SYSTEM TABLES expose:
   usage.billing.list_prices
   usage.workflows.jobs_timeline
   system.access.audit
   system.billing.usage
   
   Join these for cost allocation.
```

---

## PART VIII — INTERVIEW PRACTICE

### 28. The Staff-Level Databricks Interview

#### 28.1 Common Questions

```
1. "Explain the Lakehouse architecture."
2. "Compare DLT vs dbt vs Workflows."
3. "What does Unity Catalog provide beyond Hive Metastore?"
4. "When would you use streaming vs batch on Databricks?"
5. "Compare Databricks Model Serving vs SageMaker."
6. "Explain Mosaic AI components."
7. "Databricks on AWS vs Azure vs GCP: differences?"
8. "Design a RAG application on Databricks."
9. "Cost optimization on Databricks: your top 10."
10. "Lakehouse Federation: when + why?"
```

#### 28.2 Worked Example

```
Q: Design a Databricks platform for a 500-person retailer.
   Use case: operational analytics + recommendation ML + 
      conversational analytics for store managers.
   Data: 100 TB point-of-sale events + inventory + customer profile.
   Budget: $400K/month.
   
STAFF ANSWER:

ARCHITECTURE:

STORAGE (Delta / Unity Catalog):
   Bronze: raw POS events, inventory snapshots, customer updates
   Silver: cleaned, deduped, SCD2 for customer profile
   Gold: 
      • store_daily_metrics (aggregates for BI)
      • product_recommendations (ML model output)
      • customer_360 (for personalization)
   UniForm enabled for optional Iceberg reads by partners

INGESTION:
   • POS events: Kafka → DLT with Auto Loader (incremental ingest)
   • Inventory: Lakeflow Connect from Postgres (managed CDC)
   • Customer updates: Lakeflow Connect from Salesforce

TRANSFORMATION:
   • DLT pipelines for bronze → silver (declarative, with expectations)
   • dbt for complex gold-layer modeling (git workflow, tests, docs)
   • Workflows orchestrates both

SQL / BI:
   • DBSQL serverless warehouses (dev, staging, prod)
   • Materialized views for expensive aggregations (daily_store_metrics)
   • AI/BI Dashboards for store manager reports
   • Genie Space on gold tables for NL-to-SQL

ML / AI:
   • Feature store on Delta tables
   • Recommendation model trained via MLflow on PySpark
   • Model Serving for low-latency inference endpoint
   • Vector Search on product descriptions for semantic search
   • Mosaic AI Agent Framework for store manager chat assistant:
       - Retrieves metrics via Genie
       - Retrieves inventory details via SQL functions
       - Explains recommendations

GOVERNANCE:
   • Unity Catalog three-level namespace (retail.silver, retail.gold)
   • Row filters for region-level access
   • Column masks on PII
   • Lineage via UC (auto)
   • Lakehouse Monitoring on silver tables for drift + quality

OPERATIONS:
   • Asset Bundles for IaC (jobs, DLT, dashboards, SQL)
   • Serverless Jobs for ETL
   • Predictive Optimization enabled
   • AI Gateway governing all LLM calls (for cost + audit)

COST BREAKDOWN:
   DBSQL serverless (BI + Genie):          $80K
   Jobs clusters (DLT + Workflows):        $120K
   Model Serving (recommendation):         $40K
   Vector Search:                           $10K
   Mosaic AI Agent Framework:               $40K
   AI Gateway (LLM pass-through):           $60K
   Lakeflow Connect:                        $20K
   Storage (Delta + volumes):               $10K
   Monitoring + Horizon tools:               $5K
   Observability + tooling:                 $15K
   Total: ~$400K/month

WHY DATABRICKS (NOT SNOWFLAKE):
   • ML workload is first-class (Mosaic AI, MLflow, Model Serving, Vector Search)
   • Open storage (Delta + Iceberg via UniForm): less lock-in
   • Agent Framework for the conversational analytics use case
   • Streaming + batch unified (DLT)

OFF-RAMPS:
   • Delta with UniForm → Iceberg readable by other engines
   • Standard MLflow → portable to other platforms
   • Spark workloads → portable to EMR / Dataproc / Fabric
   • Lock-in is less on storage, more on Mosaic AI + Unity Catalog features
```

### 29. War Stories

> **🔥 Production Scenario — The Jobs Cluster Fix.** A team ran nightly ETL on all-purpose clusters: 50 clusters running 24/7, costing $200K/month. Audit showed <10% utilization at night. Migrated to Jobs clusters (ephemeral) + reduced cluster count by 70%. Costs: $45K/month. Lesson: all-purpose for notebooks; Jobs clusters for production — this is the single biggest Databricks cost mistake.

> **🔥 Production Scenario — The Photon Cost Shock.** A team enabled Photon on an all-purpose cluster. DBU rate jumped 2x. For their mixed workload (lots of UDFs falling back to JVM), the speedup was only 20%. Net cost went UP. Fix: enabled Photon only on DBSQL warehouses + eligible SQL-heavy jobs. Lesson: Photon is a performance/cost trade-off — benchmark before enabling globally.

> **🔥 Production Scenario — The Unity Catalog Migration.** A 1000-table org migrated from Hive Metastore to Unity Catalog. Took 6 months. Hit issues: external paths needed re-registration, SCALA notebook access-mode compatibility, access control model required re-thinking (group hierarchies). Lesson: Unity Catalog migrations are a quarter of work, not a sprint; invest in planning.

> **🔥 Production Scenario — The DLT Serverless Win.** A team ran a complex Structured Streaming pipeline manually on classic clusters. Reliability issues (OOMs, state size growth, operator complexity). Migrated to Serverless DLT with declarative tables + expectations. Reliability improved; operational burden dropped; cost roughly the same. Lesson: for streaming lakehouse work, DLT is often the right default unless you need customizations DLT doesn't support.

> **🔥 Production Scenario — The Lakehouse Federation Trap.** A team used Lakehouse Federation to join a 1 TB Databricks table to a Snowflake fact table (nightly). The federation pushed down filters but pulled 10 GB from Snowflake per run. Cost: $5K/month in Snowflake egress + compute. Fix: materialized the Snowflake data into Delta via Lakeflow Connect once-daily. Federation cost dropped to zero; Snowflake use dropped. Lesson: Federation is for ad-hoc / exploration, not production joins at volume.

### 30. Cheat Sheet &amp; Mental Models

#### 30.1 The One-Minute Pitch

```
Databricks in 2026 is the "Data Intelligence Platform":
   
   LAKEHOUSE FOUNDATION: Delta Lake + Unity Catalog + Photon
   DATA ENGINEERING: DLT + Workflows + Lakeflow Connect + Auto Loader
   SQL / BI: DBSQL + Materialized Views + AI/BI + Genie
   AI / ML: Mosaic AI (Model Serving, Vector Search, Agent Framework, 
       Foundation Models) + MLflow 3.x
   GOVERNANCE: Unity Catalog + Lakehouse Monitoring + Delta Sharing

Pick Databricks when: ML/AI workloads are first-class, lakehouse-committed,
   want open storage (Delta + Iceberg), Spark expertise in-house.
Reconsider: pure analyst-centric BI shops (Snowflake often better UX),
   simple warehousing (BigQuery might be enough).
```

#### 30.2 The Four Lenses — Compressed

```
CONCEPT
   Data Intelligence Platform = lakehouse + AI + BI + governance
   Delta Lake foundation; Unity Catalog governance; Mosaic AI for AI

MECHANICS
   Compute: all-purpose / jobs / SQL warehouses / serverless
   Storage: Delta (default) + Iceberg (via UniForm or managed)
   Pipelines: DLT (declarative) + Workflows (orchestration)
   AI: Mosaic AI (Model Serving + Vector Search + Agents)

TRADE-OFFS
   Pros: open storage, strong ML/AI, multi-cloud, lakehouse maturity
   Cons: pricing complexity (DBUs + cloud compute + storage),
         Unity Catalog migration non-trivial, less analyst-polished than Snowflake

INTERVIEW
   Know: Unity Catalog, DLT, Delta features, Workflows,
         Mosaic AI components, Serverless features, cost patterns
   Defend: when to use Databricks vs Snowflake vs Fabric vs open stack
```

#### 30.3 Service Decision Guide (Databricks-Specific)

```
NEED                                   SERVICE
================================================
Interactive notebook work              All-purpose cluster
Production ETL                         Jobs cluster + Workflows
Declarative pipelines                  Delta Live Tables (DLT)
Multi-system orchestration             Workflows (or Airflow)
Streaming file ingest                  Auto Loader + DLT
Managed CDC                            Lakeflow Connect
Streaming stateful ops                 Structured Streaming (or Flink)
BI / dashboards                        DBSQL + AI/BI Dashboards
Natural language analytics             Genie
Classical ML                           MLflow + Model Serving
GenAI / RAG                            Vector Search + Agent Framework
Model governance                       Unity Catalog (models as UC objects)
Govern LLM calls                       AI Gateway
Open storage                           Delta + UniForm, or Managed Iceberg
Cross-org data sharing                 Delta Sharing + Marketplace
Federated queries                      Lakehouse Federation (ad-hoc only)
Data quality                           DLT expectations + Lakehouse Monitoring
IaC                                    Asset Bundles
PII masking                            Unity Catalog column masks
Row-level security                     Unity Catalog row filters
```

#### 30.4 Senior vs Staff

| Senior Answer | Staff Answer |
|---------------|--------------|
| "Databricks is managed Spark." | "Databricks in 2026 is a Data Intelligence Platform: lakehouse foundation (Delta + Unity Catalog + Photon), data engineering (DLT + Workflows + Lakeflow Connect), SQL/BI (DBSQL + Genie), and AI (Mosaic AI: Model Serving, Vector Search, Agent Framework). Spark is a compute engine within it. For design questions, I'd articulate which layers of the platform are being used, because 'use Databricks' is vague." |
| "Use Delta Lake for ACID." | "Delta is the storage default. Modern features: Liquid Clustering (adaptive layout), Deletion Vectors (fast deletes), Predictive Optimization (auto OPTIMIZE/VACUUM), UniForm (dual Delta+Iceberg). The staff signal is choosing between Delta-only, UniForm-enabled, or Managed Iceberg based on multi-engine needs. For Databricks-only shops: Delta with Predictive Opt. For multi-engine: UniForm or Managed Iceberg + Polaris." |
| "Databricks is for ML." | "ML/AI is a strength but not the whole platform. In 2026, Mosaic AI is first-class: Model Serving endpoints, Vector Search, Agent Framework, Foundation Model APIs, Agent Evaluation, MLflow 3.x unified tracking. Pair that with Unity Catalog governing models as first-class objects. The staff signal is recognizing Databricks as end-to-end for GenAI apps (data → vector search → agent → serve → monitor), not just training models." |
| "Databricks is expensive." | "Cost is a function of workload shape + mode choice. Top levers: jobs clusters (not all-purpose) for production, serverless for bursty, spot for jobs, autoscaling with sensible min, Photon only when the workload benefits, Predictive Optimization for storage, AI Gateway to govern LLM spend. System tables (system.billing.usage) enable per-team / per-pipeline attribution. Staff engineers set up this cost allocation on day one, not after bills surprise finance." |

> **🧠 Principal Mindset:** Databricks' bet in 2026 is "the AI layer belongs with the data layer." Mosaic AI brings LLM serving, vector search, agents, and governance to the data platform rather than requiring a separate ML stack. The platform competes less with Snowflake (analyst-first, warehouse-first) and more with "Snowflake + a separate AI/ML stack." Staff engineers should articulate when consolidating onto Databricks reduces operational burden enough to justify the lock-in, and when a best-of-breed stack (Snowflake for BI + Databricks or SageMaker for ML + Pinecone for vectors) is worth the integration cost. The right answer varies by org maturity, team expertise, and existing commitments.

---

[← Back to main](./README.md)
