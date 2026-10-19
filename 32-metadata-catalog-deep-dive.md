# Topic 32 — Metadata & Catalog Deep Dive

> Standalone deep-dive · Hive Metastore, Glue, Unity Catalog, Polaris, Nessie, DataHub, OpenMetadata

[← Back to main](./README.md)

---

## How to use this document

The catalog wars of 2024-2026 are a staff-level topic. Chapter 11 (Iceberg) touches on catalogs; this chapter is the full survey. Technical metadata (Hive, Glue, Unity, Polaris, Nessie, Lakekeeper, Gravitino) vs discovery catalogs (DataHub, Amundsen, OpenMetadata, Atlas) vs runtime lineage (OpenLineage, Marquez).

> **🧠 Principal Mindset:** Metadata is where lakehouse politics plays out. Who writes? Who reads? Who owns? Who audits? "We use Unity Catalog" means Databricks-centric. "We use Polaris" means multi-engine commitment. The catalog choice signals strategy, not just tooling.

## Table of Contents

**PART I — FOUNDATIONS**
- 1. The Three Metadata Layers
- 2. The Catalog Wars of 2024-2026
- 3. Decision Factors

**PART II — TECHNICAL METADATA CATALOGS**
- 4. Hive Metastore
- 5. AWS Glue Data Catalog
- 6. Unity Catalog (Databricks)
- 7. Snowflake Horizon as a Catalog Surface

**PART III — ICEBERG CATALOGS**
- 8. Why Iceberg Needs Its Own Catalog Layer
- 9. Apache Polaris
- 10. Project Nessie
- 11. Lakekeeper
- 12. Apache Gravitino

**PART IV — DISCOVERY CATALOGS**
- 13. The Discovery Problem
- 14. DataHub
- 15. OpenMetadata
- 16. Amundsen
- 17. Apache Atlas

**PART V — LINEAGE**
- 18. OpenLineage Specification
- 19. Marquez
- 20. Column-Level Lineage

**PART VI — BUSINESS METADATA**
- 21. Business Glossaries
- 22. Stewardship
- 23. Semantic Layers Revisited

**PART VII — ARCHITECTURE PATTERNS**
- 24. Multi-Catalog Federation
- 25. Catalog Migration
- 26. Catalogs + Governance

**PART VIII — INTERVIEW PRACTICE**
- 27. The Staff-Level Catalog Interview
- 28. Worked Example
- 29. War Stories
- 30. Cheat Sheet &amp; Mental Models

---

## PART I — FOUNDATIONS

### 1. The Three Metadata Layers

```
TECHNICAL METADATA:
   "What is this table? Where are its files? What's the schema?"
   Required for ENGINES to query data
   Examples: Hive Metastore, Glue, Unity Catalog, Polaris
   
DISCOVERY METADATA:
   "Who owns this table? What does this column mean? What depends on it?"
   For HUMANS to find + understand data
   Examples: DataHub, Amundsen, OpenMetadata, Atlas
   
LINEAGE METADATA:
   "What produced this table? What consumes it?"
   For IMPACT ANALYSIS + ROOT CAUSE
   Examples: OpenLineage, Marquez, Alvin, Castor (Coalesce)

STAFF SIGNAL: these layers are complementary, not competitors. 
   Mature orgs have all three.
```

### 2. The Catalog Wars of 2024-2026

```
TIMELINE:
   2010s: Hive Metastore de-facto standard
   2019: AWS Glue Data Catalog (Hive-compatible)
   2020: Nessie (versioned Iceberg catalog) emerges
   2022: Databricks Unity Catalog GA
   2024: Snowflake open-sources Polaris Catalog → Apache
   2024: Snowflake Open Catalog (managed Polaris)
   2024: Databricks also offers managed Polaris
   2024: Lakekeeper (Rust Iceberg catalog) emerges
   2024: Apache Gravitino (unified multi-source catalog) 
   2025: Iceberg REST API becoming the lingua franca
   
WHY WARS NOW:
   Iceberg's REST catalog API created a PROTOCOL standard
   Multiple implementations compete on features/ops
   Vendors want catalog relationship (lock-in surface)
```

### 3. Decision Factors

```
SINGLE-ENGINE ORG:
   Databricks-only → Unity Catalog
   Snowflake-only → Horizon
   AWS-only → Glue
   Simple; vendor-aligned

MULTI-ENGINE / MULTI-VENDOR:
   Polaris / Nessie / Lakekeeper (open protocol via Iceberg REST)
   Multiple engines (Spark, Trino, Databricks, Snowflake) 
      read + write SAME Iceberg tables via SAME catalog

DISCOVERY NEEDS:
   DataHub (most mature OSS; LinkedIn)
   OpenMetadata (newer; strong UX)
   Amundsen (Lyft; still maintained)
   Atlas (legacy Hadoop; declining)

LINEAGE NEEDS:
   OpenLineage spec + Marquez or vendor backend
   Monte Carlo / Datafold (commercial lineage from query logs)
   Unity Catalog native lineage (if Databricks)
```

---

## PART II — TECHNICAL METADATA CATALOGS

### 4. Hive Metastore

```
HIVE METASTORE (HMS): the OG catalog.
   
   Originally for Hive; became de-facto standard for Hadoop ecosystem
   Thrift API; MySQL/Postgres backend
   Stores: databases, tables, partitions, schemas, locations, serdes
   
Still used by Spark, Trino, Presto, Flink SQL as a fallback.

LIMITATIONS:
   • Hadoop-era design
   • Scalability challenges at large partition counts
   • No ACID per-se (just metadata about external tables)
   • Access control bolted on (via Ranger / Sentry)
   
WHERE IT STILL LIVES:
   Legacy Hadoop clusters
   AWS EMR default
   On-prem lakehouses
   Starting point for migrations to newer catalogs
```

### 5. AWS Glue Data Catalog

```
AWS GLUE DATA CATALOG:
   AWS's managed Hive Metastore-compatible catalog
   Backs: EMR, Athena, Redshift Spectrum, Glue Spark, Lake Formation
   
STRENGTHS:
   • Native to AWS; auto-integrated with analytics services
   • Crawlers (schema discovery from S3 files)
   • Lake Formation integration (fine-grained access control)
   • Iceberg support (as of 2022+)

LIMITATIONS:
   • AWS-only; vendor lock
   • Slower than purpose-built lakehouse catalogs for Iceberg write ops
   • Permissions model is complex (IAM + Lake Formation + Glue grants)

PATTERN: default for AWS-centric lakehouses; migrate to Polaris
   when multi-engine or non-AWS writers emerge.
```

### 6. Unity Catalog (Databricks)

See 22-databricks-platform for depth.

```
UNITY CATALOG = Databricks' governance + metadata layer.
   
   Three-level namespace: catalog.schema.object
   Objects: tables, views, functions, VOLUMES (files), MODELS, UDFs
   Access control: ANSI SQL + attribute policies
   Lineage: auto for Delta operations
   Audit logs: who queried what
   
   OPEN SOURCE (2024): Unity Catalog OSS
      Reference implementation
      Compatibility with Iceberg REST
      Enables non-Databricks engines to read Unity Catalog tables

POSITIONING:
   Databricks' bet: UC becomes the lingua franca for governed lakehouse
   Compete with Polaris for multi-engine lakehouse metadata
```

### 7. Snowflake Horizon as a Catalog Surface

```
SNOWFLAKE HORIZON: unified governance (not just a catalog).

Includes technical metadata (micro-partition pointers, schemas, etc.)
Plus: masking, row filters, lineage, access history, tagging, classification

For external engines (Spark, Trino) reading Snowflake managed Iceberg:
   Snowflake Open Catalog (= Snowflake-hosted Polaris) exposes 
      Snowflake tables via Iceberg REST
   External engines use Polaris API → Snowflake data accessible

STAFF SIGNAL: in 2026, Snowflake's catalog story has two surfaces:
   Horizon (for Snowflake-native work)
   Open Catalog / Polaris (for external engines)
   The combination weakens the "Snowflake lock-in" argument.
```

---

## PART III — ICEBERG CATALOGS

### 8. Why Iceberg Needs Its Own Catalog Layer

```
Iceberg tables have:
   • Metadata files (manifests, snapshots)
   • A CURRENT snapshot pointer (what the "latest" version is)

The CATALOG LAYER's job:
   Track table existence (list / lookup)
   Track CURRENT snapshot pointer (atomic swap on commit)
   Enforce access control
   Multi-table transactions (potentially)

Different Iceberg catalogs implement these differently:
   Hadoop catalog: snapshot pointer in a filesystem file (not atomic in all cases)
   Hive catalog: use HMS as backing
   AWS Glue: use Glue tables
   REST catalog: standard HTTP API; multiple implementations
```

### 9. Apache Polaris

See 11-iceberg for depth.

```
POLARIS: open-source Iceberg REST catalog.
   Donated by Snowflake to Apache in 2024
   Managed by Snowflake (Open Catalog) + Databricks + others
   
   FEATURES:
      Iceberg REST API (spec-compliant)
      Catalog + namespace + table ops
      Access control (RBAC + principal roles)
      Multi-cloud (storage credentials pluggable)
      Audit logs
      
   SELF-HOSTABLE: deploy Polaris server anywhere (K8s typical)
   MANAGED: Snowflake Open Catalog, Databricks Managed Polaris, etc.
```

### 10. Project Nessie

```
NESSIE: Iceberg catalog with GIT-LIKE SEMANTICS.
   Dremio-originated; open-source
   
   KEY FEATURES:
      Branches: create a branch; make changes; merge back
      Tags: version markers (releases)
      Transactions: multi-table atomic commits
      
   USE CASES:
      Isolate dev work (dev branch of catalog)
      Multi-table transactions (dbt rebuilds a whole model group atomically)
      Rollback entire catalog version
      Audit via commit history

POSITIONING: more features than vanilla Polaris; smaller ecosystem.
   Dremio-tight; works with Trino / Spark / Flink.
```

### 11. Lakekeeper

```
LAKEKEEPER: Rust-based Iceberg REST catalog.
   
   Positioning: performance + reliability-focused Polaris alternative
   Open-source
   Growing community (2024+)
   
   STRENGTHS:
      Rust = fast + memory-safe
      Minimal dependencies (easier ops than JVM Polaris)
      Pluggable storage + auth
   
   TRADE-OFFS:
      Newer; less battle-tested than Polaris
      Smaller ecosystem

STAFF SIGNAL: evaluate for teams that value Rust-native tools + 
   simpler ops than JVM catalogs.
```

### 12. Apache Gravitino

```
APACHE GRAVITINO (incubating 2024+):
   Unified metadata catalog across heterogeneous sources.
   
   Idea: ONE catalog for Iceberg + Hive + MySQL + Kafka + ...
   Expose unified metadata via REST
   Query engines (Trino, Spark) use Gravitino as metadata source
   
POSITIONING:
   The "catalog of catalogs" pattern
   Ambitious; still proving in production
   
Datastrato (the main sponsor) was acquired; project continues at Apache.
```

---

## PART IV — DISCOVERY CATALOGS

### 13. The Discovery Problem

```
TECHNICAL METADATA tells engines HOW to query data.
DISCOVERY METADATA tells HUMANS what data EXISTS + means + who owns it.

Questions discovery catalogs answer:
   • "Is there a table that tells me monthly active users?"
   • "What does column `metrics.churn_pct` mean?"
   • "Who owns this dataset? When was it last updated?"
   • "What dashboards use this table?"
   • "Is this certified / blessed?"
   • "What's the data quality score?"

Discovery catalogs COMPLEMENT technical catalogs; they usually HARVEST
   metadata from technical catalogs + warehouses + BI tools.
```

### 14. DataHub

```
DATAHUB (LinkedIn → open-source):
   Most mature open-source discovery catalog.
   
   CORE CONCEPTS:
      Entities: datasets, dashboards, charts, models, users, pipelines
      Aspects: metadata about entities (schema, ownership, lineage, tags)
      Metadata model: strongly typed via protobuf
   
   ARCHITECTURE:
      GMS (metadata service): backs Elasticsearch + Postgres + Kafka
      Ingestion framework: pull-based ingest from 50+ sources
      Frontend: React UI
      
   SOURCES: Snowflake, BigQuery, Databricks, dbt, Airflow, 
      Great Expectations, Tableau, Looker, Mode, 100+ more
   
   FEATURES:
      Search + browse
      Lineage visualization (table + column level)
      Ownership + stewardship
      Business glossary
      Data quality integration (dbt tests, GX)

MANAGED: Acryl Data (now Datahub Cloud)
```

### 15. OpenMetadata

```
OPENMETADATA (2021+):
   Open-source discovery catalog with strong UX.
   
   DIFFERENTIATORS:
      Opinionated metadata model (JSON schemas)
      Built-in data quality (TestSuite + Testcases)
      Collaboration features (conversations on datasets)
      Lineage from query logs
   
   SOURCES: 50+ connectors (similar coverage to DataHub)
   
   MANAGED: Collate (OpenMetadata vendor)

Positioning:
   Modern UX, Python-first ingestion, built-in DQ
   Growing fast; smaller ecosystem than DataHub
```

### 16. Amundsen

```
AMUNDSEN (Lyft, 2019):
   Pioneer in open-source discovery catalogs.
   
   SIMPLER than DataHub/OpenMetadata:
      Search-first UX
      Fewer features but gentler learning curve
      Less active development than DataHub (as of 2024-2026)
   
   USE: teams that want simple discovery without enterprise features
```

### 17. Apache Atlas

```
APACHE ATLAS (Hortonworks, 2015):
   Hadoop-era metadata + governance catalog.
   
   Still used in on-prem Hadoop deployments
   Lineage via Hive/HBase hooks
   Access control via Ranger integration

Declining in cloud-native deployments; DataHub/OpenMetadata are modern replacements.
```

---

## PART V — LINEAGE

### 18. OpenLineage Specification

See 23-data-quality-observability for depth.

```
OPENLINEAGE: open standard for lineage events.
   Linux Foundation project.
   JSON event schema: Job + Dataset + facets.
   
   INTEGRATIONS:
      Airflow (built-in since 2.3)
      dbt (dbt-ol wrapper)
      Spark (listener)
      Flink (listener)
      GX (integration)
      Snowflake, BigQuery, Redshift (custom emitters)

PRINCIPLE: emit events; let consumers (Marquez, DataHub, Monte Carlo) use them.
```

### 19. Marquez

```
MARQUEZ: reference OpenLineage server.
   Open-source
   REST API + Web UI
   PostgreSQL backend
   
   For orgs who want DIY lineage without a big discovery catalog.
```

### 20. Column-Level Lineage

```
APPROACHES (see 23 for depth):
   SQL parsing (most tools) — static analysis
   Runtime capture — more accurate, more complex
   Query log mining — commercial tools (Monte Carlo, Datafold, Alvin)

CRITICAL FOR:
   PII compliance (track sensitive columns)
   Impact analysis
   Debugging transformations

Unity Catalog has native column lineage for Databricks workloads.
```

---

## PART VI — BUSINESS METADATA

### 21. Business Glossaries

```
BUSINESS GLOSSARY: definitions of business terms + their mappings to columns.
   
   "Monthly Active User (MAU)": business definition + which column represents it
   
   FEATURES in catalogs:
      Term hierarchies (metric → sub-metric)
      Approvals (certified terms vs draft)
      Mapping to physical columns
      Related terms, synonyms

STAFF SIGNAL: a business glossary is where DATA GOVERNANCE begins.
   Without agreed definitions, "revenue" means different things in
   finance, sales, and marketing — and the DE team debugs the gap endlessly.
```

### 22. Stewardship

```
STEWARDSHIP MODEL:
   
   DATA OWNER: team / role accountable for the dataset
   DATA STEWARD: individual responsible for day-to-day quality + definitions
   DATA CONSUMER: team that uses the dataset
   
   RESPONSIBILITIES:
      Owner: SLAs, business meaning, approval of changes
      Steward: data quality, documentation, deprecation
      Consumer: feedback, bug reports, usage patterns

Operationalized in catalogs:
   Datasets tagged with owner + steward
   Workflow: issues on datasets routed to stewards
   SLAs tracked per owner
```

### 23. Semantic Layers Revisited

```
See 05-dbt-modeling for semantic layer depth.

Semantic layer = business-logic metadata between warehouse + BI tool.

Modern semantic layers:
   dbt Semantic Layer (powered by MetricFlow)
   Cube
   Looker Explores (LookML)
   AtScale
   Steep

CATALOG-ADJACENT: metrics in semantic layer should be CATALOGED like datasets.
   DataHub + OpenMetadata both support metrics as first-class entities.

Pattern:
   dbt Semantic Layer → DataHub → users search for "MAU" → find the certified metric
```

---

## PART VII — ARCHITECTURE PATTERNS

### 24. Multi-Catalog Federation

```
Mature orgs often have MULTIPLE catalogs:
   
   Technical: Glue + Unity Catalog + Polaris (across different lakes)
   Discovery: DataHub (central discovery UI)
   Lineage: OpenLineage emitted to Marquez + DataHub

PATTERN:
   Technical catalogs own engine-facing metadata
   Discovery catalog PULLS from all technical catalogs + warehouses + BI
   Lineage flows into discovery catalog for user-facing lineage view

Users have ONE UI (DataHub) that covers all data; engines use their
   native technical catalog.
```

### 25. Catalog Migration

```
COMMON MIGRATIONS in 2024-2026:
   
   HMS → GLUE: HMS Federation feature; or migrate schemas by script
   HMS → UNITY CATALOG: UC includes migration tooling + managed location APIs
   HMS → POLARIS: via Iceberg migration first; then catalog switch
   GLUE → UNITY CATALOG: Databricks migration tooling
   HIVE TABLES → ICEBERG: table-by-table rewrite; use Iceberg "migrate" procedure

RISKS:
   • Pointer-in-time ambiguity (which catalog is authoritative when)
   • Access control model differences
   • Partition metadata (large partition tables)

STAFF SIGNAL: argue the DUAL-WRITE PERIOD. Write to old + new catalog;
   switch readers one by one; retire old catalog last.
```

### 26. Catalogs + Governance

```
Access control patterns:
   
   HIVE/GLUE + RANGER / LAKE FORMATION:
      External authorization layer
      Fine-grained but complex
   
   UNITY CATALOG: 
      Native ANSI SQL grants + attribute policies
      Audit logs built-in
   
   POLARIS:
      Native RBAC + principal roles
      Growing in capability
   
   SNOWFLAKE HORIZON:
      Native masking + row filters + tagging
      Richest native governance

STAFF SIGNAL: pick a catalog based on governance surface, not just metadata features.
   The access control story matters as much as the schema catalog.
```

---

## PART VIII — INTERVIEW PRACTICE

### 27. The Staff-Level Catalog Interview

```
COMMON QUESTIONS:
   1. "Compare Unity Catalog, Polaris, and Nessie."
   2. "Why not just use Hive Metastore?"
   3. "Design the metadata strategy for a multi-engine lakehouse."
   4. "DataHub vs OpenMetadata."
   5. "What does OpenLineage give you?"
   6. "Column-level lineage: how + when."
   7. "Business glossary: value?"
   8. "Catalog migration from HMS to Unity Catalog."
   9. "Multi-catalog federation: when + how."
   10. "Discovery vs technical catalogs."
```

### 28. Worked Example

```
Q: Design the metadata strategy for a 1000-person enterprise with:
   • Iceberg on S3 (lakehouse)
   • Databricks for ML
   • Snowflake for BI
   • Trino for ad-hoc
   • dbt for transformations in both
   • 50 producer teams; 200 consumer teams

STAFF ANSWER:

TECHNICAL CATALOGS:
   APACHE POLARIS (managed via Snowflake Open Catalog or Databricks Polaris)
      Lingua franca Iceberg catalog
      All engines (Databricks, Snowflake, Trino) read + write via Iceberg REST
      Storage credentials centralized
      RBAC per catalog + namespace
   
   Unity Catalog ADJUNCT:
      Databricks-specific: Volumes, Models, UDFs
      Not Iceberg tables (those live in Polaris)
      Governs ML artifacts + non-tabular data

DISCOVERY CATALOG:
   DATAHUB (self-hosted or Acryl managed):
      Harvests metadata from Polaris, Unity Catalog, Snowflake, Trino, dbt,
         Airflow, Tableau, Looker
      Business glossary (with term approvals + mapping)
      Lineage visualization (table + column from query log mining)
      Ownership + stewardship tracked per dataset
      Search = primary data discovery UX

LINEAGE:
   OPENLINEAGE:
      Emitted by Airflow + dbt + Spark + Flink
      Flows to Marquez (central) + DataHub (via DataHub's OpenLineage integration)
   COMMERCIAL (optional): Monte Carlo for column-level lineage from query logs

ACCESS CONTROL:
   Catalog-layer: Polaris RBAC + Databricks Unity Catalog + Snowflake Horizon
   Harmonized via centralized identity (SSO + SCIM)
   Attribute policies for PII (data classification tagged in DataHub)
   Dynamic masking via Snowflake Horizon + UC column masks

GOVERNANCE WORKFLOWS:
   New dataset: register in DataHub; owner approves; auto-register in Polaris
   Breaking change: RFC in DataHub; impact analysis via column lineage; 
      consumer sign-off → change executed in catalogs
   Access request: workflow in DataHub → auto-grant in Polaris on approval

MIGRATION PATH (if from HMS):
   Phase 1: HMS tables migrated to Iceberg; still catalog in Glue initially
   Phase 2: Catalog migrates to Polaris; engines reconfigure
   Phase 3: DataHub deployed; harvests Polaris + warehouses + BI
   Phase 4: Full stewardship + lineage program

COST:
   Polaris (self-hosted or SaaS):            $30-100K/year
   DataHub (self-hosted or Acryl Cloud):     $100-300K/year
   Marquez (self-hosted):                     $20K/year (infra)
   Monte Carlo column lineage (optional):     $200K/year
   Platform team (stewardship + ops):         $500K/year (3 FTE)
   Total: ~$1M/year for enterprise-grade metadata

ALTERNATIVES REJECTED:
   Vendor-lock: Unity Catalog as primary → excludes Snowflake users
   Vendor-lock: Horizon as primary → excludes Databricks users
   Open catalog (Polaris) is the only answer for multi-engine
```

### 29. War Stories

> **🔥 Production Scenario — The Hive-to-Iceberg + Catalog Migration.** A team migrated 2000 Hive tables to Iceberg over 6 months. Catalog stayed in Glue initially (Hive-compatible). Then: migrated catalog to Polaris to enable Snowflake writes. The sequence was critical: tables first (storage format), catalog second (metadata). Lesson: migration is a MULTI-STEP architecture change; sequencing matters.

> **🔥 Production Scenario — The DataHub Deployment Investment.** A team deployed DataHub expecting magic. 6 months later, adoption was low. Investigation: no stewardship; metadata harvested was raw (table names, no definitions, no owners). Fix: hired 2 data stewards; built business glossary; drove documentation campaigns. 1 year later: 70% of tier-1 tables documented; search became primary data access path. Lesson: discovery catalogs require ORGANIZATIONAL investment, not just technical deployment.

> **🔥 Production Scenario — The Column Lineage Save.** A team deployed Monte Carlo with column-level lineage. A product engineer renamed a column in production. Impact analysis: 15 downstream models + 30 dashboards affected. Blocked at code review by the column lineage report. Fix properly coordinated. Lesson: column lineage is a cheap, high-value capability for mid-to-large orgs.

> **🔥 Production Scenario — The Polaris Migration from HMS.** A multi-engine shop had HMS on EMR. Added Databricks + Snowflake with their own catalogs. Metadata diverged; discovery was a nightmare. Standardized on Polaris: all engines read via Iceberg REST; single source of truth for Iceberg tables. Discovery via DataHub. Lesson: in multi-engine orgs, catalog standardization is high-leverage.

> **🔥 Production Scenario — The Glossary Collision.** A finance team defined "Revenue" as "accrual-based net revenue"; a sales team defined "Revenue" as "booking value." Both terms existed; queries used them interchangeably; executive reports disagreed. Fix: business glossary enforced ONE DEFINITION per metric; aliases allowed but mapped to the canonical. Discovery via DataHub. Lesson: definitional alignment is a data engineering problem too.

### 30. Cheat Sheet &amp; Mental Models

#### 30.1 One-Minute Pitch

```
THREE METADATA LAYERS:
   TECHNICAL: engines query data (Hive, Glue, UC, Polaris, Nessie)
   DISCOVERY: humans find + understand (DataHub, OpenMetadata, Amundsen)
   LINEAGE: impact + root cause (OpenLineage + Marquez + commercial)

MULTI-ENGINE LAKEHOUSE:
   Polaris (open Iceberg REST) as technical catalog
   DataHub as discovery catalog
   OpenLineage as lineage spec
   
SINGLE-ENGINE:
   Unity Catalog if Databricks; Horizon if Snowflake; Glue if AWS

STAFF SIGNAL: argue the CATALOG STRATEGY; recognize that open catalog
   is a strategic decision (reduces lock-in; enables multi-engine).
```

#### 30.2 Senior vs Staff

| Senior | Staff |
|--------|-------|
| "We use Unity Catalog." | "Unity Catalog for Databricks-native work; Polaris for open Iceberg across engines; DataHub for cross-engine discovery. Catalog choice is strategic — it determines whether engines can be swapped and whether multi-engine is viable." |
| "DataHub is a data catalog." | "DataHub is a DISCOVERY catalog; it harvests metadata from TECHNICAL catalogs (Polaris, UC, Snowflake). The two layers coexist; discovery doesn't replace technical catalogs." |
| "Polaris is Snowflake's." | "Polaris is Apache; Snowflake donated it. It's an open REST catalog, not Snowflake-only. Managed offerings include Snowflake Open Catalog, Databricks Managed Polaris, Dremio, and self-hosted options. The ecosystem is converging on Polaris as the open standard." |
| "Column lineage is nice-to-have." | "Column lineage is essential for GDPR / PII compliance, for impact analysis on breaking changes, and for debugging transformation errors. Monte Carlo / Datafold / Unity Catalog do it from query logs. For mid-to-large orgs it's quickly worth the investment." |

> **🧠 Principal Mindset:** The catalog is the TABLE OF CONTENTS of your data platform. If it's bad, nothing else matters — people won't find data they could use, consumers won't trust what they find, and changes will keep breaking the system. Staff engineers invest in the catalog layer early and defend its priority against vendor lock-in attempts.

---

[← Back to main](./README.md)
