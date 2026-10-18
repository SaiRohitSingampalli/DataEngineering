# Topic 19 — GCP for Data Engineering

> Standalone deep-dive · The Google Cloud Platform data engineering service landscape for principal/staff engineers

[← Back to main](./README.md)

---

## How to use this document

This chapter covers GCP's data engineering service portfolio at architect level. GCP's data story is distinctive: BigQuery sits at the center, and most other services orbit it. The strengths are serverless simplicity, deep ML integration (Vertex AI), and strong analytics UX. Every chapter follows the four lenses: **Concept · Mechanics · Trade-offs · Interview**.

> **🧠 Principal Mindset:** GCP data engineering is BigQuery-centric. For companies that fit BigQuery's model (sporadic workloads, serverless simplicity, analyst-heavy, SQL-centric), GCP is often the lowest-friction cloud. For companies with heavy Spark/ML + non-SQL workloads, GCP's offerings are competitive but less dominant. The staff-level question isn't "do you know GCP services" — it's "do you know when BigQuery is the right spine vs when it's a constraint."

## Table of Contents

**PART I — FOUNDATIONS**
- 1. The GCP Data Service Landscape
- 2. Projects, IAM, and Networking
- 3. Pricing Patterns on GCP
- 4. The BigQuery-Centric Architecture

**PART II — STORAGE**
- 5. Cloud Storage (GCS)
- 6. GCS Tiers and Lifecycle
- 7. BigQuery Storage Deep Dive
- 8. BigLake (Governance Layer)

**PART III — COMPUTE AND PROCESSING**
- 9. Dataflow (Managed Apache Beam)
- 10. Dataproc (Managed Spark/Hadoop)
- 11. Cloud Functions and Cloud Run
- 12. Cloud Composer (Managed Airflow)

**PART IV — INGESTION**
- 13. Pub/Sub and Pub/Sub Lite
- 14. Datastream (CDC)
- 15. Dataflow for Streaming
- 16. Transfer Service

**PART V — ANALYTICS**
- 17. BigQuery Deep Dive
- 18. BigQuery External Tables and BigLake
- 19. Dataform (SQL Transformations)
- 20. Looker

**PART VI — ML AND GOVERNANCE**
- 21. Vertex AI
- 22. BigQuery ML
- 23. Dataplex (Governance)

**PART VII — OPERATIONS**
- 24. Cloud Logging, Monitoring, and Trace
- 25. Deployment Patterns on GCP

**PART VIII — INTERVIEW PRACTICE**
- 26. The Staff-Level GCP Data Interview
- 27. War Stories
- 28. Cheat Sheet &amp; Mental Models

---

## PART I — FOUNDATIONS

### 1. The GCP Data Service Landscape

#### 1.1 The Core Service Map

```
STORAGE
   Cloud Storage (GCS)       Object storage
   BigQuery Storage           Columnar warehouse storage (proprietary)
   BigLake                    Unified governance across GCS + BQ
   Persistent Disk            Block storage
   Filestore                  Managed NFS

INGESTION
   Pub/Sub                    Managed message bus / streaming
   Pub/Sub Lite               Capacity-based (cheaper for large steady loads)
   Datastream                 CDC from Postgres/MySQL/Oracle
   Transfer Service           File transfer (on-prem ↔ cloud)
   BigQuery Data Transfer     Scheduled transfers from SaaS/other

COMPUTE / PROCESSING
   Dataflow                   Managed Apache Beam (batch + stream)
   Dataproc                   Managed Spark/Hadoop
   Dataproc Serverless        Serverless Spark
   Cloud Functions            Serverless (2nd gen on Cloud Run)
   Cloud Run                  Serverless containers
   GKE                        Managed Kubernetes
   Dataform                   SQL transformations (dbt-like)
   Composer                   Managed Airflow

ANALYTICS
   BigQuery                   The center of the GCP data story
   BigQuery BI Engine          In-memory accelerator
   AlloyDB                    Postgres-compatible with analytics
   Spanner                    Globally distributed SQL
   Looker                      BI; semantic layer

ML
   Vertex AI                  Full ML platform
   BigQuery ML                 Train models in SQL
   AutoML (within Vertex)     No-code model training
   Vertex AI Agent Builder     Build gen-AI agents

GOVERNANCE
   Dataplex                   Data mesh / governance layer
   Data Catalog (merged into Dataplex)
   Cloud KMS                  Key management
   DLP (Data Loss Prevention) PII discovery + masking

OBSERVABILITY
   Cloud Logging              Logs
   Cloud Monitoring           Metrics + alerts
   Cloud Trace                Distributed tracing
```

#### 1.2 The Shape of a GCP Data Stack

```
Sources → Ingestion → GCS / BQ → Processing → BigQuery → Looker / Vertex
             │              │             │           │
             │             BigLake / Dataplex Catalog
             │                                          │
         Pub/Sub            Dataflow (Beam)        BigQuery
         Datastream         Dataproc Serverless    Looker Studio
         Transfer           Dataform (SQL)          Vertex AI

   Orchestration: Composer (Airflow) or Workflows (serverless)
```

#### 1.3 The BigQuery Gravity Well

```
In AWS: S3 is the gravitational center; everything orbits storage.
In Azure: identity (Entra) and Fabric are centers.
In GCP: BIGQUERY is the center.

Nearly every GCP data decision touches BigQuery:
   • GCS is often just "pre-landing before BQ loads"
   • Dataflow is "write to BQ"
   • Vertex AI reads from BQ
   • Looker queries BQ
   • Dataform transforms IN BQ

GCP is designed to pull you toward BigQuery. For workloads that fit BigQuery,
this is a feature (simplicity, UX). For workloads that don't (very large Spark
batch, custom real-time serving), it can be a constraint.
```

> **🎯 Interview Angle:** Q: Why is GCP's data story so BigQuery-centric?
> **Staff:** "BigQuery was Google's first and most differentiated data product (launched 2010, descended from internal Dremel). Google invested a decade of engineering before competitors matched its serverless + Dremel-based architecture. Rather than compete across every service category, GCP optimized the ecosystem AROUND BigQuery — Dataflow writes to BQ natively, BigQuery ML lets you train without leaving SQL, BigLake extends BQ-style governance to GCS data, Looker is deeply integrated. The strategy: make BigQuery so convenient that it's the center of your analytics gravity. Trade-off: workloads that don't fit BigQuery (very large Spark, complex streaming without BQ as sink) are less first-class on GCP than on AWS or Azure."

### 2. Projects, IAM, and Networking

#### 2.1 GCP's Resource Hierarchy

```
Organization
  └── Folders (optional)
       └── Projects
            └── Resources (BigQuery datasets, GCS buckets, etc.)

PROJECTS are the primary billing + policy boundary.
   Each project has its own:
      - Billing account linkage
      - IAM policies
      - APIs enabled
      - Quotas
   
   Typical pattern:
      prod-data, staging-data, dev-data (one project per env)
      OR by workload: data-platform, data-warehouse, ml-platform
```

#### 2.2 IAM

```
IDENTITIES:
   Users (Google accounts; or federated via Workforce Identity)
   Service accounts (for apps + services)
   Groups

ROLES:
   Primitive (Owner, Editor, Viewer) — too broad; avoid
   Predefined (e.g., "BigQuery Data Viewer") — most common
   Custom roles — tailored permissions

GRANTS AT MULTIPLE LEVELS:
   Organization → Folder → Project → Resource
   
   Permissions INHERIT down the hierarchy

WORKLOAD IDENTITY:
   For GKE: Kubernetes service accounts map to Google service accounts
   For other clouds: Workload Identity Federation (connect from AWS/Azure
      to GCP services without storing credentials)
```

#### 2.3 Networking

```
VPC (Virtual Private Cloud):
   Global by default (unique to GCP; others are regional)
   Subnets are regional

PRIVATE SERVICE ACCESS:
   Reach managed services (BQ, SQL, GKE) via private IPs
   No internet routing

PRIVATE SERVICE CONNECT:
   Service producer-consumer private connection pattern

VPC SERVICE CONTROLS:
   Create "perimeters" around services (e.g., BigQuery, GCS)
   Block data exfiltration (even by authorized identities) outside perimeter
   Critical for enterprise data security on GCP
```

### 3. Pricing Patterns on GCP

#### 3.1 GCP Pricing Dimensions

```
COMPUTE:    per vCPU-hour (varies by machine family)
STORAGE:    per GB-month (tier-dependent)
NETWORK:    egress to internet; cross-region; some cross-zone
OPERATIONS: per 10K ops (for storage, Firestore, etc.)
MANAGED:    serverless pricing is often per-use (bytes scanned, invocations, etc.)
```

#### 3.2 BigQuery Pricing (Critical to Understand)

```
TWO MODELS:

ON-DEMAND:
   $6.25 / TB scanned
   Flat fee per query, scales linearly with data scanned
   Best for: sporadic workloads

EDITIONS (CAPACITY-BASED, 2023+):
   Standard / Enterprise / Enterprise Plus
   Billed per SLOT-HOUR (slot = compute unit)
   Minimum 100 slots; auto-scaling to configured max
   
   Pricing (approximate per slot-hour):
      Standard:         ~$0.04
      Enterprise:        ~$0.06
      Enterprise Plus:    ~$0.10
   
   Features by edition:
      Standard: basic analytics
      Enterprise: materialized views, BI Engine, Workload Management
      Enterprise Plus: multi-region replication, column-level security, etc.

AUTOSCALER (newer):
   Scale from 0 to configured max within an edition
   Only pay for slots used
   Can replace on-demand for most workloads
   
STORAGE (BigQuery native):
   Active storage: $0.02/GB/mo (first 10 GB free)
   Long-term storage: $0.01/GB/mo (after 90 days unmodified)

STORAGE (BigQuery billing in physical bytes, 2023+):
   Alternative pricing: pay for physical compressed bytes (not logical)
   Can be 50-80% cheaper for well-compressed data

STREAMING INSERTS:
   $0.01/MB (ingestion) — surprising cost at scale
   Use batch loads where possible
```

#### 3.3 Hidden Costs

```
Egress:
   Standard GCP egress: $0.08-0.12/GB to internet
   Cross-region (within GCP): $0.01-0.08/GB
   Cross-project (same region): free!
   
Pub/Sub:
   Throughput-based; can be surprising on high-volume topics
   Pub/Sub Lite is cheaper for steady high throughput

Dataflow:
   Per vCPU-hour + memory-hour + storage
   Streaming jobs run 24/7 → always billing

Streaming inserts to BQ:
   Easy to accidentally stream 1 TB/day = ~$30K/month in insert fees
   Batch loads are nearly free; use when possible
```

#### 3.4 Discount Patterns

```
COMMITTED USE DISCOUNTS (CUDs):
   1 or 3-year commitments; 20-50% off
   For Compute Engine, GKE, Cloud SQL

SUSTAINED USE DISCOUNTS:
   Automatic for VMs run >25% of the month

BIGQUERY RESERVATIONS:
   Reserved slots for 1-3 years, with discounts
   Can share across projects

FLAT-RATE STORAGE for BigQuery:
   Reserved storage pricing available for very large workloads
```

### 4. The BigQuery-Centric Architecture

#### 4.1 The Reference Architecture

```
                    SOURCES
                       │
         ┌─────────────┼─────────────┐
         ▼             ▼             ▼
      Pub/Sub      Datastream     Transfer /
     (streaming)    (CDC)         Fivetran (SaaS)
         │             │             │
         └─────────────┼─────────────┘
                       │
                       ▼
              ┌────────────────┐
              │  GCS (bronze)  │   (optional landing; some skip)
              └────────┬───────┘
                       │
                       ▼
              ┌────────────────┐
              │   Dataflow /   │
              │   Dataproc /   │   (processing — or load directly to BQ)
              │   Dataform     │
              └────────┬───────┘
                       │
                       ▼
              ┌────────────────┐
              │   BIGQUERY     │   (the center of gravity)
              │                │
              │ Bronze/Silver/ │
              │ Gold layers    │
              └────────┬───────┘
                       │
       ┌───────────────┼────────────────┐
       ▼               ▼                ▼
    LOOKER          VERTEX AI       REVERSE-ETL
   (dashboards)     (ML)            (Hightouch →
                                     SaaS)
```

#### 4.2 Variations

```
PATTERN A: BIGQUERY-ONLY
   Pub/Sub → Dataflow → BigQuery → Looker
   
   Pros: minimal infrastructure; serverless everywhere
   Cons: BQ lock-in; cost at scale

PATTERN B: BIGQUERY + GCS LAKEHOUSE
   Sources → GCS (Iceberg/Hudi via BigLake) → BigQuery external tables + Spark
   
   Pros: open storage; multi-engine via BigLake
   Cons: more operational layers

PATTERN C: DATAPROC-CENTRIC (Spark heavy)
   Sources → Dataproc Serverless → GCS Delta/Iceberg → Vertex AI, BigQuery
   
   Pros: Spark expertise maps; portable to other clouds
   Cons: less GCP-native; losses BQ's serverless UX

PATTERN D: HYBRID WITH DATABRICKS
   Sources → Databricks on GCP → GCS Delta → BigQuery for BI
   
   Pros: best Spark; portable
   Cons: multi-vendor; Databricks premium cost
```

---

## PART II — STORAGE

### 5. Cloud Storage (GCS)

#### 5.1 What GCS Is

```
GOOGLE CLOUD STORAGE = object storage, equivalent to S3 / ADLS.
   Globally consistent, strongly consistent reads
   11 nines durability
   
   Namespace: gs://bucket-name/path/to/object
   
   Multi-region buckets: data stored in multiple regions automatically
      (vs AWS where you replicate manually)
```

#### 5.2 Storage Classes

```
STANDARD:        $20/TB/mo    Frequent access
NEARLINE:        $10/TB/mo    30-day min storage; infrequent
COLDLINE:        $4/TB/mo     90-day min; rare access
ARCHIVE:         $1.2/TB/mo   365-day min; archive

AUTOCLASS (newer):
   Automatic tiering between classes based on access
   Similar to S3 Intelligent-Tiering
```

#### 5.3 Bucket Types

```
REGIONAL:       data in one region; lowest latency; cheapest
DUAL-REGION:    two regions; high availability + lower egress
MULTI-REGION:   many regions in a continent (US, EU, ASIA); highest availability

Multi-region egress within the multi-region: free
Multi-region storage: more expensive than regional
```

#### 5.4 Access Models

```
IAM at bucket/object level
Signed URLs (time-limited access)
Uniform bucket-level access (recommended): disable ACLs; use IAM only
VPC Service Controls: perimeter around buckets

Service account impersonation for cross-project access.
```

### 6. GCS Tiers and Lifecycle

```
Lifecycle rules can:
   Transition between classes based on age / conditions
   Delete objects after N days
   Delete non-current versions after N days (object versioning)

Example rule (JSON):
{
  "rule": [
    {"action": {"type": "SetStorageClass", "storageClass": "NEARLINE"},
     "condition": {"age": 30}},
    {"action": {"type": "SetStorageClass", "storageClass": "COLDLINE"},
     "condition": {"age": 90}},
    {"action": {"type": "Delete"},
     "condition": {"age": 2555}}  -- 7 years
  ]
}

AUTOCLASS: hands-off alternative (small monthly fee per object)
```

### 7. BigQuery Storage Deep Dive

#### 7.1 BigQuery's Proprietary Storage

```
BigQuery's primary storage is proprietary columnar format (Capacitor), 
NOT GCS Parquet.

Characteristics:
   • Columnar (one file per column; compressed)
   • Internal to BigQuery; not directly accessible as files
   • Managed storage (BQ handles compaction, encryption, replication)
   • Priced separately from compute
```

#### 7.2 Storage Options

```
BIGQUERY NATIVE STORAGE:
   Load data into BQ tables (via INSERT, LOAD, streaming)
   Stored in Capacitor format
   Best query performance
   Priced $0.02/GB active, $0.01/GB long-term

EXTERNAL TABLES:
   Point BQ at Parquet/Avro/ORC/CSV in GCS
   Query via BQ engine; data stays in GCS
   Slower than native (no column stats, no pruning magic)

BIGLAKE TABLES (modern):
   External tables with FINE-GRAINED SECURITY
   Row-level, column-level security, policy tags
   BigLake = unified governance across BQ and GCS

MANAGED ICEBERG TABLES (2024+):
   BigQuery can create/manage Iceberg tables in GCS
   Writes handled by BQ; readable by other engines (Spark, Trino, etc.)
   Combines BQ UX with open-format portability
```

#### 7.3 Partitioning and Clustering

```
PARTITIONING:
   TIME_UNIT (DAY/HOUR/MONTH/YEAR) on a timestamp column
   INTEGER_RANGE on an integer column
   INGESTION_TIME (if no timestamp column)
   
   Benefit: query pruning (reads only relevant partitions)
   Mandatory for large tables with time-based queries

CLUSTERING:
   Up to 4 columns; data sorted within partitions
   Enables fine-grained pruning
   No cost; "free" optimization
   
   Common pattern:
      PARTITION BY DATE(event_ts)
      CLUSTER BY user_id, event_type

Together: a 10 TB table queried for one user on one day might scan < 10 MB.
```

### 8. BigLake (Governance Layer)

#### 8.1 What BigLake Is

```
BIGLAKE = unified governance across BigQuery and GCS data.
   
   Capabilities:
      Create BigLake tables (metadata in BQ, data in GCS Parquet/Iceberg)
      Apply row-level security, column-level masking, policy tags
      Access from multiple engines (BQ, Spark on Dataproc, external via connectors)
      
   Comparable to: AWS Lake Formation, Microsoft Purview + Fabric
```

#### 8.2 BigLake Tables vs External Tables

```
EXTERNAL TABLES (classic):
   BQ queries GCS Parquet
   Permissions via GCS ACLs (coarse)
   No fine-grained security

BIGLAKE TABLES:
   BQ queries GCS Parquet
   Permissions via BQ IAM (fine-grained)
   Row-level security, column masking supported
   
   USE: when you want BQ-style governance on GCS data without ingesting
```

#### 8.3 Iceberg in BigQuery

```
BIGQUERY MANAGED ICEBERG TABLES (2024+):
   BQ creates and manages Iceberg tables in your GCS
   Full BQ feature parity (writes, time travel, security)
   Data readable by Iceberg clients (Spark, Trino, Snowflake)
   
Why it matters:
   For multi-engine lakehouses, BQ becomes an equal citizen
   You're not locked to BQ's proprietary storage
   Trade-off: slightly slower than native BQ storage for some queries
```

---

## PART III — COMPUTE AND PROCESSING

### 9. Dataflow (Managed Apache Beam)

#### 9.1 What Dataflow Is

```
DATAFLOW = managed Apache Beam execution.
   
   Apache Beam: unified batch + streaming programming model.
      Write once; run on Dataflow, Flink, Spark, etc.
   
   Dataflow is the premier GCP managed runner for Beam.
   
   Pricing: per vCPU-hour + memory-hour + per GB shuffled
```

#### 9.2 Dataflow for Streaming

```
Common pattern:
   Pub/Sub → Dataflow (Beam streaming) → BigQuery / GCS / Pub/Sub

Features:
   Exactly-once semantics (sink-dependent)
   Autoscaling workers
   Windowing + watermarks (Beam's model, same as Flink)
   Streaming inserts to BQ (or storage API for cheaper writes)

For streaming lakehouse:
   Pub/Sub → Dataflow → BigQuery managed Iceberg table

For streaming analytics:
   Pub/Sub → Dataflow → BigQuery streaming insert OR
                       → GCS Parquet with Firehose-style patterns
```

#### 9.3 Dataflow vs Flink

```
DATAFLOW:
   ✓ Managed; no cluster to run
   ✓ Autoscaling
   ✓ Deep GCP integration (Pub/Sub, BQ, GCS)
   ✗ Beam programming model has learning curve
   ✗ Debugging across the Beam → Dataflow translation layer

FLINK (on GKE or Confluent):
   ✓ Native Flink; broader community
   ✓ More control
   ✗ More operational burden

For GCP-committed shops: Dataflow is often the right call.
For multi-cloud or Flink expertise: Flink.
```

### 10. Dataproc (Managed Spark/Hadoop)

#### 10.1 Dataproc Flavors

```
DATAPROC:
   Managed Spark/Hadoop clusters
   Provisioned clusters you manage lifecycle for
   Pre-installed with Spark, Hadoop, Hive, Pig, HBase, Kafka

DATAPROC SERVERLESS:
   Submit Spark jobs; GCP manages execution
   No cluster to size or terminate
   Faster startup than Dataproc (ephemeral clusters)
   
DATAPROC ON GKE:
   Spark on Kubernetes with GKE
   Shared compute with other workloads
```

#### 10.2 When to Use

```
PICK DATAPROC WHEN:
   ✓ Need full Spark/Hadoop stack
   ✓ Have Spark expertise
   ✓ Need specific versions / custom configs

PICK DATAPROC SERVERLESS WHEN:
   ✓ Intermittent Spark workloads
   ✓ Don't want cluster lifecycle management

PICK DATAFLOW WHEN:
   ✓ Streaming workloads
   ✓ Beam programming model fits
   ✓ Deep BigQuery / Pub/Sub integration
```

### 11. Cloud Functions and Cloud Run

#### 11.1 Cloud Functions

```
CLOUD FUNCTIONS (2nd gen) = serverless functions, built on Cloud Run.
   Triggers: HTTP, Pub/Sub, GCS events, Firestore changes, Scheduler
   Languages: Python, Node, Go, Java, Ruby, PHP, .NET
   
   For data engineering:
      ✓ Pub/Sub trigger: process messages
      ✓ GCS trigger: process files when they land
      ✓ Scheduled jobs via Cloud Scheduler
```

#### 11.2 Cloud Run

```
CLOUD RUN = serverless containers.
   Deploy any Docker image; auto-scales to zero
   Longer-running (up to 60 min) than Cloud Functions
   
   For data engineering:
      ✓ Containerized ETL jobs
      ✓ Custom data APIs
      ✓ dbt runs (Cloud Run jobs trigger dbt)
      ✓ Serverless DuckDB queries (!)
```

#### 11.3 The Serverless DuckDB on GCP Pattern

```python
# Cloud Run job running DuckDB on GCS Parquet
import duckdb

def main():
    con = duckdb.connect(':memory:')
    con.execute("INSTALL httpfs; LOAD httpfs;")
    
    result = con.execute("""
        SELECT user_id, SUM(amount) 
        FROM 'gs://my-bucket/events/*.parquet'
        WHERE date = '2024-03-15'
        GROUP BY user_id
    """).fetchall()
    
    # Load results somewhere
    bq_client.load_table_from_json(result, "my_table")
```

Cheap, serverless, no cluster, serves many data engineering needs.

### 12. Cloud Composer (Managed Airflow)

```
CLOUD COMPOSER = managed Apache Airflow (GCP's MWAA equivalent).
   
   Composer 2 (current): runs on GKE Autopilot
   Composer 3 (newer): more serverless, better scaling

Standard Airflow patterns apply:
   GCP operators: BigQueryExecuteQueryOperator, DataprocSubmitJobOperator,
                   DataflowCreateJavaJobOperator, GCSObjectExistenceSensor, etc.
   
   Deep integration with Secret Manager, IAM, networking.
```

---

## PART IV — INGESTION

### 13. Pub/Sub and Pub/Sub Lite

#### 13.1 Pub/Sub

```
GOOGLE CLOUD PUB/SUB = managed message bus / event streaming.
   
   Core model:
      Topics (publishers write messages)
      Subscriptions (consumers read from subscriptions)
      Push or pull delivery

   Scale: millions of messages/sec; global
   Retention: 7 days default; up to 31 days
   Ordering: optional per-key ordering (opt-in)
   Exactly-once: available (opt-in; requires client config)

Comparable to: AWS Kinesis + SNS/SQS combined, or Kafka.
```

#### 13.2 Pub/Sub Lite

```
PUB/SUB LITE = capacity-based, cheaper for steady workloads.
   
   You provision throughput capacity (publish + subscribe)
   Flat per-hour cost
   Zonal (not regional); less durable than Pub/Sub
   
   Pros: 10x cheaper for high steady throughput
   Cons: zonal (single AZ); less reliable; no global
   
   USE: logs, telemetry, high-volume streams where cost matters more than availability
```

#### 13.3 Pub/Sub vs Kafka

```
PUB/SUB:
   ✓ Fully managed; zero ops
   ✓ Global service (automatic multi-region)
   ✓ No topic partition management
   ✗ Not Kafka protocol
   ✗ Less fine-grained control
   ✗ GCP-only

KAFKA on GCP (via Confluent Cloud or self-hosted):
   ✓ Standard Kafka ecosystem
   ✓ Multi-cloud portable
   ✓ Flink / Kafka Connect integration
   ✗ More operational work OR vendor premium
```

### 14. Datastream (CDC)

#### 14.1 What Datastream Is

```
GCP DATASTREAM = managed CDC service.
   Sources: Postgres, MySQL, SQL Server, Oracle
   Destinations: BigQuery, GCS, Pub/Sub
   
   Serverless; auto-scales; charges per GB processed.
```

#### 14.2 The Datastream + BigQuery Pattern

```
Datastream can write directly to BigQuery with ongoing CDC:
   Postgres → Datastream → BigQuery
      (BQ table reflects live state of source)

Also:
   Postgres → Datastream → GCS (as JSON/Avro files)
      → Dataflow / Dataform → BigQuery

For simple CDC to BQ, Datastream is often the right call.
```

### 15. Dataflow for Streaming

Covered under Dataflow (section 9) — streaming patterns:
- Pub/Sub → Dataflow → BigQuery
- Pub/Sub → Dataflow → GCS (Parquet)
- Pub/Sub → Dataflow → Pub/Sub (pipeline chaining)

### 16. Transfer Service

```
STORAGE TRANSFER SERVICE:
   Transfer files from on-prem, AWS S3, Azure Blob to GCS
   Scheduled or one-time
   Managed; GCS-native

BIGQUERY DATA TRANSFER SERVICE:
   Scheduled ingestion from:
      Google Analytics, Google Ads, YouTube, Search Ads 360
      Amazon S3, Azure Blob
      Teradata, Redshift (as source for migration)
   
   Lands data in BigQuery directly.
```

---

## PART V — ANALYTICS

### 17. BigQuery Deep Dive

#### 17.1 BigQuery Architecture

```
BIGQUERY = serverless columnar warehouse built on Google's Dremel.
   
   Key architectural properties:
      - Serverless (no clusters to size, unless using Editions)
      - Columnar storage (Capacitor format)
      - MPP execution
      - Separation of storage + compute
      - Petabyte-scale queries in seconds (with proper partitioning)

   Pricing: on-demand ($/TB scanned) OR Editions (slot-based)
```

#### 17.2 BigQuery Features

```
STANDARD SQL:
   ANSI-compliant with GCP extensions
   Nested + repeated fields (STRUCT, ARRAY)
   
WINDOW FUNCTIONS: full standard
RECURSIVE CTE: supported
APPROXIMATE: APPROX_COUNT_DISTINCT, APPROX_QUANTILES

MATERIALIZED VIEWS:
   Automatic incremental refresh
   Query rewriting to use materialized views automatically

BI ENGINE:
   In-memory accelerator; sub-second queries for cached data
   Deep Looker integration

STORED PROCEDURES: declarative + imperative SQL

ML (BigQuery ML): train models inline with SQL (see section 22)

GEOSPATIAL: full GIS support (GEOGRAPHY type, spatial functions)

TIME TRAVEL: query past snapshots (7 days default; longer with configuration)

STREAMING INSERTS: via tabledata.insertAll API
STORAGE WRITE API: higher throughput, exactly-once, cheaper than streaming inserts

CROSS-REGION QUERIES: query data across regions (with egress charges)
```

#### 17.3 Editions vs On-Demand

```
ON-DEMAND:
   $6.25/TB scanned
   Easy; no capacity planning
   Costs can be unpredictable
   
EDITIONS:
   Reserve slots; pay per slot-hour
   Three tiers: Standard, Enterprise, Enterprise Plus
   
   AUTOSCALER (within editions):
      Scale slots from 0 to configured max
      Pay only for used slots
      Essentially a hybrid of on-demand ease + editions features

CROSSOVER:
   For >100 TB scanned/month: Editions usually cheaper
   For sporadic workloads: on-demand remains cheap
   
By 2026, Editions + Autoscaler is the recommended default for most workloads.
```

### 18. BigQuery External Tables and BigLake

Covered under Storage section 7 and 8.

Key use cases:
- Query GCS Parquet without ingesting (cheap storage)
- Query Iceberg tables (BigLake Iceberg integration)
- Query Delta Lake (via BigLake Delta support)
- Cross-cloud federation (BigQuery Omni queries data in AWS/Azure)

### 19. Dataform (SQL Transformations)

#### 19.1 What Dataform Is

```
DATAFORM = dbt-like tool, acquired by Google in 2020.
   
   What it provides:
      SQL-based transformations with Jinja-like templating
      Dependency management (DAG of models)
      Version control via Git
      Scheduled execution via Cloud Scheduler or Workflows
      Deep BigQuery integration

Positioning: GCP's native answer to dbt.
```

#### 19.2 Dataform vs dbt

```
DATAFORM:
   ✓ Deeper BigQuery integration
   ✓ Free (within BQ pricing)
   ✓ GCP-managed
   ✗ Smaller community than dbt
   ✗ Less flexible (works best for BQ)

dbt:
   ✓ Multi-engine (BigQuery, Snowflake, Databricks, Postgres, etc.)
   ✓ Largest community
   ✓ Rich package ecosystem
   ✗ Separate product to manage (dbt Cloud or dbt Core)

FOR BIGQUERY-ONLY SHOPS:
   Both work. dbt is more future-proof.
   Dataform is simpler to operate (no separate service).

STAFF-LEVEL CHOICE: dbt remains the industry standard; Dataform is a reasonable
   alternative if you're committed to GCP-native tooling.
```

### 20. Looker

#### 20.1 What Looker Is

```
LOOKER = Google's BI platform (acquired 2019).
   
   Key differentiator: LookML — a modeling language for defining semantic layers.
   
   Features:
      LookML models define metrics, dimensions, joins
      Business users explore via dashboards
      Explore interface for ad-hoc analysis
      API-first; embeddable
      Deep BigQuery integration (query generation optimized for BQ)
```

#### 20.2 Looker vs Looker Studio

```
LOOKER (Enterprise):
   Full product; LookML semantic layer; enterprise features
   Paid; per-user or per-capacity pricing

LOOKER STUDIO (formerly Data Studio):
   Free; simpler dashboarding
   No semantic layer (built-in)
   Good for lightweight dashboards

For enterprise analytics: Looker (not Studio).
```

#### 20.3 LookML

```
LookML EXAMPLE:

view: orders {
  sql_table_name: `project.dataset.orders` ;;
  
  dimension: user_id {
    type: number
    sql: ${TABLE}.user_id ;;
  }
  
  measure: total_amount {
    type: sum
    sql: ${TABLE}.amount ;;
  }
}

explore: orders {
  join: users {
    sql_on: ${orders.user_id} = ${users.id} ;;
    relationship: many_to_one
  }
}
```

Analysts explore via UI; LookML centralizes definitions.

---

## PART VI — ML AND GOVERNANCE

### 21. Vertex AI

#### 21.1 What Vertex AI Is

```
VERTEX AI = GCP's unified ML platform.
   
   Components:
      Workbench: managed Jupyter + VM
      Training: custom + AutoML
      Pipelines: Kubeflow-based orchestration
      Model Registry
      Endpoints: online + batch prediction
      Feature Store
      Experiments / Metadata
      Model Monitoring
      Vertex AI Agent Builder: build gen-AI agents
      Model Garden: pre-trained models (incl. Gemini)
```

#### 21.2 Vertex AI vs Alternatives

```
VERTEX AI:
   ✓ Deeply integrated with BigQuery (BQ as training data source)
   ✓ Managed Gemini / foundation models
   ✓ Good UX for GCP shops

DATABRICKS ML (on GCP):
   ✓ Better Spark + MLflow integration
   ✓ Portable to other clouds
   ✓ Delta Lake-based feature stores

SAGEMAKER (if cross-cloud):
   AWS-only; comparable depth to Vertex AI

For GCP-committed: Vertex AI is first-class.
```

### 22. BigQuery ML

#### 22.1 Training Models in SQL

```sql
-- Train a model in BigQuery with SQL
CREATE MODEL my_project.ml.churn_model
OPTIONS (
    model_type = 'LOGISTIC_REG',
    input_label_cols = ['churned']
) AS
SELECT 
    customer_id,
    recency_days,
    frequency,
    monetary_value,
    churned
FROM my_project.features.customer_signals
WHERE training_set = 'TRAIN';

-- Evaluate
SELECT * FROM ML.EVALUATE(MODEL my_project.ml.churn_model,
    (SELECT * FROM features WHERE training_set = 'TEST'));

-- Predict
SELECT customer_id, predicted_churned_probs
FROM ML.PREDICT(MODEL my_project.ml.churn_model,
    (SELECT * FROM features WHERE scoring = TRUE));
```

#### 22.2 BigQuery ML Capabilities

```
Model types:
   LINEAR_REG, LOGISTIC_REG
   KMEANS
   MATRIX_FACTORIZATION (recommender)
   ARIMA_PLUS (time series)
   BOOSTED_TREE_CLASSIFIER / REGRESSOR
   DNN_CLASSIFIER / REGRESSOR
   AUTOML_CLASSIFIER / REGRESSOR
   RANDOM_FOREST_CLASSIFIER
   
Advanced:
   Imports: load TensorFlow / ONNX models for inference
   Remote models: call Vertex AI endpoints from SQL
   Gemini: call Gemini models for text/vision via SQL

USE:
   ✓ ML directly in SQL for analysts
   ✓ Quick models without separate ML infrastructure
   ✗ Not for complex deep learning (use Vertex AI for that)
```

### 23. Dataplex (Governance)

#### 23.1 What Dataplex Is

```
DATAPLEX = GCP's data mesh / governance platform.
   Absorbed Data Catalog in 2023.
   
   Capabilities:
      Lakes: logical container across GCS + BQ
      Zones: raw / curated / etc. within a lake
      Assets: GCS buckets + BQ datasets
      
      Automatic metadata discovery
      Data quality (built on Great Expectations-style checks)
      Lineage (table-level)
      Data profiling
      Access policies
```

#### 23.2 For Multi-Workspace Governance

```
Dataplex positions GCP for data mesh patterns:
   Each domain has its own project / lake
   Dataplex catalogs and governs across them
   Policies apply via tags / taxonomies
```

---

## PART VII — OPERATIONS

### 24. Cloud Logging, Monitoring, and Trace

#### 24.1 Cloud Logging

```
CLOUD LOGGING = managed log store.
   Auto-collected from GCP services + custom app logs
   Log Explorer: UI for querying
   Log-based metrics: convert logs to metrics
   Log sinks: export to BigQuery, Pub/Sub, GCS

For data engineering:
   BigQuery job logs → analyze query costs
   Dataflow logs → debug streaming jobs
   Composer DAG logs → debug Airflow
```

#### 24.2 Cloud Monitoring

```
CLOUD MONITORING = managed metrics + alerting.
   Integration with Grafana, Datadog
   Service-level objectives (SLOs)
   Custom metrics via API or Open Telemetry

For data engineering:
   Pipeline health dashboards
   BigQuery slot usage, materialized view freshness
   Pub/Sub subscription backlogs
```

#### 24.3 Billing + Cost Analysis

```
BILLING EXPORT TO BIGQUERY:
   Daily billing data exported as BigQuery tables
   Query via SQL for custom cost dashboards
   Example: cost per project per team per month

BIGQUERY INFORMATION_SCHEMA:
   Query cost of individual BQ jobs
   Identify most expensive users / queries
   Essential for FinOps
```

### 25. Deployment Patterns on GCP

```
INFRASTRUCTURE AS CODE:
   Terraform (most common)
   Google Deployment Manager (less common now)
   Config Connector (Kubernetes-native)

CI/CD:
   Cloud Build (GCP-native)
   GitHub Actions / GitLab CI (external)

DEPLOYMENT PATTERN FOR DATA:
   dbt or Dataform code → Git → CI validates → deploy to BQ
   Dataflow code → Git → CI builds + deploys template → Scheduler
   Airflow DAGs → Git → sync to Composer bucket
```

---

## PART VIII — INTERVIEW PRACTICE

### 26. The Staff-Level GCP Data Interview

#### 26.1 Common Questions

```
1. "Explain why GCP data engineering centers on BigQuery."
2. "Design a data platform on GCP for a mid-size company."
3. "BigQuery pricing: on-demand vs Editions — when do you pick each?"
4. "Explain Dataflow vs Dataproc vs BigQuery for processing."
5. "How does BigLake fit alongside BigQuery?"
6. "What's your orchestration choice: Composer, Workflows, or Scheduler?"
7. "Dataform vs dbt — when do you pick each?"
8. "Explain BigQuery ML and when it's enough vs Vertex AI."
9. "Design a CDC pipeline from Postgres to GCP analytics."
10. "GCP's cross-cloud story — BigQuery Omni, BigLake federated."
```

#### 26.2 Worked Example

```
Q: Design a GCP data platform for a 100-person e-commerce startup.
   50 TB current, 2x/yr growth. 30 analysts, 10 engineers, 3 ML engineers.
   SaaS tools (Stripe, Shopify, Zendesk), Postgres operational DB,
   event stream from mobile apps. Budget: $20K/month.

STAFF ANSWER:

CORE STACK (BigQuery-centric):

STORAGE:
   • BigQuery native storage for warehouse data
   • GCS for raw landing + archive
   • BigLake Iceberg tables for ML feature stores (open format)

INGESTION:
   • Datastream: Postgres → BigQuery (CDC)
   • Pub/Sub → Dataflow → BigQuery: mobile app events (streaming)
   • Fivetran: Stripe, Shopify, Zendesk → BigQuery
     (AppFlow equivalent in GCP is less mature; Fivetran is worth the cost)

PROCESSING:
   • Dataform: BigQuery-native SQL transformations (bronze → silver → gold)
   • Dataflow: streaming processing from Pub/Sub
   • Cloud Run jobs: lightweight serverless tasks (DuckDB queries, custom)
   • Dataproc Serverless: occasional heavy Spark (if needed)

ANALYTICS:
   • BigQuery (editions with autoscaler): primary warehouse
   • Looker: BI for 30 analysts
   • Looker Studio: lightweight exploratory

ML:
   • BigQuery ML: for analyst-driven ML (churn, segmentation)
   • Vertex AI: for ML engineer-led models (recommender, personalization)
   • Vertex AI Feature Store: online + offline features

ORCHESTRATION:
   • Cloud Composer: complex DAGs
   • Cloud Workflows: lightweight serverless orchestration
   • Dataform scheduler for SQL pipelines

GOVERNANCE:
   • Dataplex: catalog + lineage
   • VPC Service Controls: perimeter around BQ + GCS
   • Column-level security + policy tags for PII

COST BREAKDOWN:
   BigQuery (Enterprise edition, autoscaler, ~500 slots avg): ~$9K
   BigQuery storage (50 TB active + archive):                  ~$1K
   Dataflow (streaming + batch):                                ~$2K
   Pub/Sub (events):                                             ~$500
   Datastream:                                                   ~$300
   Fivetran:                                                     ~$2K
   Looker:                                                       ~$2.5K (per-user)
   Vertex AI (training + endpoints):                             ~$1.5K
   Composer:                                                     ~$500
   GCS storage + ops:                                            ~$300
   Monitoring + Datadog + buffer:                                ~$400
   Total: ~$20K/month

WHY THIS WORKS:
   • BigQuery handles 90% of analytics with minimal ops
   • Dataflow/Pub/Sub for the streaming use case
   • Fivetran for SaaS (saves engineering time)
   • Dataform for native SQL transforms; dbt is also fine
   • Mix of BQ ML (easy) and Vertex (deep) satisfies both personas

OFF-RAMPS:
   • BigLake Iceberg means data is portable (readable by Snowflake / Databricks)
   • dbt could replace Dataform if multi-engine needs arise
   • Fivetran portable to other clouds
   • Caveat: BQ native storage not directly readable by others
     Mitigation: periodic exports to Iceberg if needed
```

### 27. War Stories

> **🔥 Production Scenario — The Streaming Insert Bill.** A team used BigQuery streaming inserts for every mobile event. 1 TB/day streamed = ~$10K/month in INSERT fees alone, before any query cost. Fix: switched to Storage Write API (cheaper, exactly-once) + batched inserts to BQ via Dataflow. Monthly insert cost dropped 90% to ~$1K. Lesson: streaming inserts to BQ are a cost gotcha; prefer Storage Write API or Dataflow batching.

> **🔥 Production Scenario — The Autoscaler Overrun.** A team enabled BQ Editions autoscaler with max=2000 slots, "just to be safe." A runaway query (SELECT * on a 500 TB table with no filter) hit the max, running 2 hours at full capacity. Cost: $240 for one query. Fix: set reasonable max (e.g., 500 for ad-hoc workloads); added per-user query cost limits via workload management. Lesson: autoscaler max is a hard ceiling; set it based on reasonable expectations, not just budget.

> **🔥 Production Scenario — The Partition Nightmare.** A team's main fact table was partitioned by DATE(event_ts) but most queries filtered by DATE(load_ts) (different column). Every query scanned all partitions = full table scans costing $$. Fix: added a load_ts partition key (requires table re-create or new table with partition change). Fixed queries dropped from $50 to $0.50 each. Lesson: partition on the column you ACTUALLY filter on; it's not always the obvious one.

> **🔥 Production Scenario — The Multi-Region Egress.** A team's BigQuery was in US multi-region. Their Vertex AI training was in us-central1. Every training job pulled 500 GB from BQ → us-central1 Vertex. Multi-region to single-region = egress cost. $400/day cumulative. Fix: moved BQ to us-central1 (single region matched training). Lesson: multi-region storage is nice for availability but costs egress to single-region compute.

> **🔥 Production Scenario — The VPC Service Controls Lockout.** A team enabled VPC Service Controls to prevent data exfiltration. Also inadvertently blocked legitimate access from CI pipelines running in a different project. 4-hour outage of all ETL jobs. Fix: carefully crafted ingress/egress policies with explicit allow for CI service accounts. Lesson: VPC-SC is powerful but requires careful policy design; test in dry-run mode before enforcement.

### 28. Cheat Sheet &amp; Mental Models

#### 28.1 The One-Minute Pitch

```
GCP data engineering = BigQuery at the center + a tight serverless ecosystem around it.

CORE SERVICES:
   BigQuery (warehouse + ML)
   GCS (lake storage)
   Dataflow (streaming + batch)
   Pub/Sub (streaming ingest)
   Datastream (CDC)
   Dataform (SQL transformations)
   Composer (Airflow)
   Looker (BI)
   Vertex AI (ML platform)
   BigLake (lakehouse governance)

STRENGTHS:
   Serverless-first (less ops)
   BigQuery is best-in-class for ad-hoc + sporadic analytics
   Deep ML integration (Vertex AI + BQ ML)
   Elegant pricing for right-shaped workloads

WEAKNESSES:
   BigQuery-centricity can constrain architectures
   Less mature in some areas (streaming vs AWS; Fabric-style unified in Azure)
   Multi-cloud: BQ data is proprietary (unless using BigLake Iceberg)
```

#### 28.2 The Four Lenses — Compressed

```
CONCEPT
   GCP data = BigQuery-centric; everything else serves BQ.
   Serverless-first philosophy. ML + analytics tight integration.

MECHANICS
   BQ: serverless columnar, slot-based or on-demand, Dremel heritage.
   Dataflow: Beam-based streaming/batch; autoscale.
   Pub/Sub: global message bus.
   BigLake: unified governance across BQ + GCS.

TRADE-OFFS
   Pros: serverless simplicity, BQ ad-hoc UX, deep ML, excellent Looker integration.
   Cons: BQ lock-in (native storage), smaller ecosystem than AWS, Microsoft Fabric
         competes on consolidated UX.

INTERVIEW
   Know: BQ pricing models, Dataflow vs Dataproc, BigLake positioning,
   Vertex AI + BQ ML, Datastream, Composer vs Workflows.
   Defend: BQ-centric architecture; when GCP wins vs AWS/Azure.
```

#### 28.3 Service Decision Guide

```
NEED                                   SERVICE
================================================
Object/lake storage                    Cloud Storage (GCS)
Warehouse (SQL analytics)              BigQuery
Lakehouse with open format             BigLake Iceberg tables
Streaming ingest                       Pub/Sub (or Pub/Sub Lite for cheap)
CDC                                    Datastream
Managed Spark                          Dataproc or Dataproc Serverless
Streaming processing                   Dataflow (Beam)
Serverless functions                   Cloud Functions / Cloud Run
Container compute                      Cloud Run / GKE
Orchestration                          Composer (Airflow) or Workflows
SQL transformations                    Dataform (or dbt)
BI                                     Looker (enterprise) or Looker Studio (free)
ML platform                            Vertex AI (full) or BigQuery ML (SQL)
Operational SQL                         AlloyDB / Cloud SQL
Globally distributed SQL                Spanner
NoSQL document                          Firestore
Governance                              Dataplex
Identity                                Workload Identity + IAM
Secrets                                 Secret Manager
```

#### 28.4 Mental Model

```
Think of GCP data as:
   • BigQuery as the center of gravity
   • Serverless-first for most components
   • Deep ML + analytics integration
   • Excellent for sporadic, analyst-heavy, SQL-centric workloads
   • Less dominant for streaming-heavy or Spark-heavy workloads

When to reach for GCP data services:
   • Sporadic / unpredictable workloads (BQ on-demand shines)
   • Analyst-heavy teams (BigQuery UX is best-in-class)
   • Deep ML integration important (Vertex + BQ ML)
   • Multi-region analytics (BQ handles this natively)

When to reach past them:
   • Databricks-committed shop (Databricks on GCP works but you lose BQ benefits)
   • Need multi-cloud with heavy Spark/streaming (AWS or Azure may fit better)
   • Avoid BQ native-storage lock-in (use BigLake Iceberg instead)
```

> **🧠 Principal Mindset:** GCP data engineering's strongest answer is BigQuery + Dataflow + Vertex AI for workloads that fit. The staff-level signal is knowing when they DO fit (sporadic analytics, serverless preference, analyst-heavy, SQL-centric) and when they don't (heavy Spark, high-concurrency transactional, deep multi-cloud). Don't recommend GCP because "serverless is cool" — recommend it when the workload shape matches BigQuery's economics and UX advantages. For companies committed to open-format lakehouse with multi-cloud intent, GCP works but is less of a clear winner than AWS or Azure in that mode.

---

[← Back to main](./README.md)
