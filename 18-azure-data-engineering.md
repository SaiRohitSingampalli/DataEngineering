# Topic 18 — Azure for Data Engineering

> Standalone deep-dive · The Microsoft Azure data engineering service landscape for principal/staff engineers

[← Back to main](./README.md)

---

## How to use this document

This chapter covers Azure's data engineering service portfolio at architect level. Microsoft Azure has rebranded, consolidated, and renamed services more aggressively than AWS or GCP over the past decade, so this chapter emphasizes **what the services actually do** rather than their marketing names — and tracks the big consolidation story of **Microsoft Fabric** (2023+) which bundles previously-separate services.

> **🧠 Principal Mindset:** Azure's data story in 2026 is largely "Microsoft Fabric" (the unified data platform) plus a few best-in-class pieces (Databricks on Azure, ADLS Gen2, Event Hubs). If you're interviewing for an Azure-centric role, Fabric is the dominant conversation — know its components, pricing model, and limitations. If you're interviewing for a multi-cloud role, know that Fabric is less portable than its AWS or GCP counterparts.

## Table of Contents

**PART I — FOUNDATIONS**
- 1. The Azure Data Service Landscape
- 2. Entra ID, Resource Groups, and Networking
- 3. Pricing Patterns on Azure
- 4. The Microsoft Fabric Story

**PART II — STORAGE**
- 5. Azure Data Lake Storage Gen2 (ADLS Gen2)
- 6. Blob Storage Tiers and Lifecycle
- 7. OneLake (Fabric's Unified Lake)

**PART III — COMPUTE**
- 8. Azure Databricks
- 9. Synapse Analytics (Legacy + Dedicated SQL Pools)
- 10. Fabric Spark and Data Engineering Experience
- 11. HDInsight (Legacy Hadoop)
- 12. Azure Functions and Logic Apps

**PART IV — INGESTION**
- 13. Azure Event Hubs
- 14. Azure Stream Analytics
- 15. Azure Data Factory
- 16. Fabric Data Pipelines

**PART V — ANALYTICS**
- 17. Synapse Dedicated SQL Pool
- 18. Fabric Warehouse and Lakehouse
- 19. Azure SQL and Cosmos DB
- 20. Power BI

**PART VI — GOVERNANCE AND ML**
- 21. Microsoft Purview
- 22. Azure Machine Learning
- 23. Fabric Data Science

**PART VII — OPERATIONS**
- 24. Azure Monitor and Log Analytics
- 25. Azure DevOps and Deployment Patterns

**PART VIII — INTERVIEW PRACTICE**
- 26. The Staff-Level Azure Data Interview
- 27. War Stories
- 28. Cheat Sheet &amp; Mental Models

---

## PART I — FOUNDATIONS

### 1. The Azure Data Service Landscape

#### 1.1 The Core Service Map

```
STORAGE
   Azure Blob Storage         Object storage
   ADLS Gen2                   Blob + HDFS-compatible API (the data lake)
   Azure Files                 Managed SMB/NFS
   OneLake                     Fabric's unified lake abstraction

INGESTION
   Event Hubs                  Kafka-compatible event ingestion
   Event Hubs for Kafka        Native Kafka protocol
   IoT Hub                     Device/IoT ingestion
   Stream Analytics            Managed streaming SQL
   Azure Data Factory          ETL + orchestration
   Fabric Data Pipelines       Fabric-native ETL
   Fabric Dataflow Gen2        Low-code ETL

COMPUTE / PROCESSING
   Azure Databricks            Databricks on Azure (first-class)
   Synapse Spark Pool          Serverless or dedicated Spark
   Fabric Spark                Fabric-native Spark
   HDInsight                   Legacy managed Hadoop/Spark/Kafka
   Functions                   Serverless functions
   Logic Apps                  Workflow service
   Container Apps              Managed containers

ANALYTICS
   Synapse Dedicated SQL Pool  MPP data warehouse (formerly SQL DW)
   Synapse Serverless SQL      Serverless T-SQL on ADLS
   Fabric Warehouse            Fabric's warehouse offering
   Fabric Lakehouse            Delta-based lakehouse in Fabric
   Azure SQL Database          Managed SQL Server
   Azure SQL Managed Instance  Larger-scale SQL Server
   Cosmos DB                   Multi-model NoSQL
   Azure Data Explorer         Kusto; time-series + logs
   Power BI                    BI and dashboarding

ML
   Azure Machine Learning      ML workbench + MLOps
   Fabric Data Science         Fabric-integrated ML
   Azure AI Services           Pre-built AI (vision, speech, LLMs)
   Azure OpenAI Service        GPT-4 / Azure-hosted LLMs

GOVERNANCE
   Microsoft Purview           Unified data governance + catalog
   Azure Key Vault             Secrets + encryption keys
   Microsoft Entra ID          Identity (formerly Azure AD)

ORCHESTRATION
   Data Factory                Primary orchestrator
   Fabric Data Pipelines       Fabric-native orchestrator
   Logic Apps                  Event-driven workflows
   Azure Automation            Scheduled runbooks

OBSERVABILITY
   Azure Monitor               Metrics + logs
   Log Analytics               Log query (Kusto Query Language)
   Application Insights        APM
```

#### 1.2 Why Fabric Changes Everything

```
BEFORE FABRIC (pre-2023):
   Separate services with separate billing, separate UIs:
      • Synapse Analytics (SQL + Spark + Pipelines)
      • Power BI (BI)
      • Purview (governance)
      • Data Factory (ETL)
      • ADLS Gen2 (storage)
   
   Hard to coordinate across them. Multiple contracts, confusing UX.

MICROSOFT FABRIC (launched May 2023, GA Nov 2023):
   Unified SaaS data platform. One UI, one lake (OneLake), one pricing (capacity units).
   Combines: Data Factory (pipelines), Synapse (SQL + Spark + ingestion),
             Data Science, Real-Time Analytics (Kusto), Power BI, Purview.
   
   One shared storage layer: OneLake (built on ADLS Gen2 under the hood).
   One shared compute capacity (F SKUs, measured in CU-hours).

By 2026, Fabric is Microsoft's primary Azure data story.
Legacy Synapse / Data Factory / Power BI still exist but Fabric is where investment goes.
```

#### 1.3 The Multi-Workload Nature of Azure

```
Azure data stacks typically combine:
   Fabric for mainstream data engineering + analytics + BI
   Databricks on Azure for serious Spark / ML
   Azure SQL / Cosmos DB for operational databases
   Event Hubs for streaming ingestion

NOT every workload fits in Fabric. For teams with existing
Databricks investments or heavy Spark/ML workloads, Databricks
remains the better choice even in Azure.
```

> **🎯 Interview Angle:** Q: What is Microsoft Fabric and why does it matter?
> **Staff:** "Fabric is Microsoft's unified SaaS data platform launched in 2023 — it bundles Data Factory, Synapse, Power BI, Purview, and new experiences into one UI with shared storage (OneLake) and shared compute (capacity units). It matters because: (1) it simplifies Azure's historically fragmented data story; (2) it changes the pricing model from per-service to capacity-based, which can be very cost-effective for mixed workloads; (3) it's the direction Microsoft is investing. The trade-offs: Fabric is less portable than AWS or GCP equivalents (OneLake is Delta-only), the pricing can be confusing (capacity units are abstract), and features are still maturing. For Azure-native shops, Fabric is the default. For multi-cloud shops, Databricks on Azure often makes more sense."

### 2. Entra ID, Resource Groups, and Networking

#### 2.1 Microsoft Entra ID (formerly Azure AD)

```
MICROSOFT ENTRA ID = Azure's identity service.
   Renamed in 2023 (was Azure Active Directory).
   
   For data engineering:
      Users + groups + service principals + managed identities
      SSO + MFA + conditional access
      Role-based access control (RBAC) for Azure resources

Managed Identities (critical):
   Automatically-managed service principals assigned to Azure resources
   Avoid storing credentials
   System-assigned: tied to one resource's lifecycle
   User-assigned: shared across resources
```

#### 2.2 Resource Groups and Subscriptions

```
HIERARCHY:
   Management Group → Subscription → Resource Group → Resource
   
   RESOURCE GROUP: logical container; shared location + lifecycle
   SUBSCRIPTION: billing + policy boundary
   MANAGEMENT GROUP: group multiple subscriptions

For data engineering:
   One resource group per environment (dev/staging/prod)
   OR one resource group per workload (data-lake, data-warehouse, ml-platform)
   Pick a convention and enforce it with policy
```

#### 2.3 Networking Basics

```
VNET (Virtual Network) = Azure's VPC equivalent
   Subnets within VNets
   NSGs (Network Security Groups) = security groups
   
   PRIVATE ENDPOINTS: connect to PaaS services via private IP
      Essential for data engineering: Storage, SQL, Cosmos DB, etc.
      Lets your lake be only reachable from your VNet
   
   SERVICE ENDPOINTS: older alternative; still used for some services
   
   AZURE PRIVATE LINK: service-to-service PrivateLink equivalent

PATTERNS:
   Data services in private subnets with Private Endpoints
   NAT Gateway for outbound internet (if needed)
   ExpressRoute for on-prem connectivity
```

### 3. Pricing Patterns on Azure

#### 3.1 Azure Billing Dimensions

```
Compute:    per vCPU-hour / vCore-hour (varies by service)
Storage:    per GB-month (tier-dependent)
Operations: per 10K transactions (storage)
Egress:     per GB (first 100 GB free; then tiered)
Networking: inter-region, inter-VNet charges

Reserved Instances / Savings Plans:
   1 or 3 year commitments; 30-70% off list
```

#### 3.2 Fabric's Capacity Model (The Big Change)

```
MICROSOFT FABRIC uses CAPACITY UNITS (CU) instead of per-service billing.
   
   SKUs: F2 (2 CUs) through F2048 (2048 CUs)
   Billed per hour (or per second for pay-as-you-go)
   
   One capacity SHARES across all Fabric workloads:
      Pipelines, Spark, Warehouse, Power BI, Data Science, Real-Time Analytics
   
   SMOOTHING: Fabric averages workload demand over 24h
      Allows short bursts beyond capacity without extra charge
      Protects against sudden spikes

PRICING (approximate):
   F64 (minimum for Power BI): ~$8,500/month pay-as-you-go
   F64 reserved 1-year:         ~$5,000/month
   F2:                           ~$265/month
   
   One capacity can serve a small team at F2-F8 or an enterprise at F512+.
```

#### 3.3 Databricks on Azure Pricing

```
Separate from Fabric. Priced in DBUs (Databricks Units) + Azure VMs.

   Standard DBU: $0.15 / DBU
   Premium DBU: $0.30 / DBU (adds features like Unity Catalog)
   
   Each cluster size = certain DBU/hour.
   + underlying VM cost
```

#### 3.4 Hidden Costs

```
Egress:
   Within Azure region: free
   Cross-region: $0.02-0.09/GB
   Internet: $0.087/GB first 10 TB

Transactions (ADLS/Blob):
   LIST, PUT, GET all cost
   Small files = many transactions = surprise bill

Private Endpoint hourly fees:
   ~$0.01/hour per endpoint
   100+ endpoints adds up

Fabric Capacity waste:
   Pay for F64 24/7 even if used only 1h/day
   Smoothing helps but doesn't eliminate idle cost
   Consider pay-as-you-go vs reserved based on usage
```

### 4. The Microsoft Fabric Story

#### 4.1 What Fabric Replaces

```
FABRIC CONSOLIDATES (and partially replaces):
   • Azure Synapse Analytics  → Fabric Warehouse / Lakehouse / Spark / Pipelines
   • Azure Data Factory       → Fabric Data Pipelines + Dataflow Gen2
   • Power BI                 → Power BI inside Fabric
   • Azure Data Explorer      → Fabric Real-Time Analytics (Kusto)
   • Microsoft Purview        → Microsoft Purview + Fabric integration
   • Azure ML (partially)     → Fabric Data Science (notebooks + ML experiments)

Not replaced (standalone products continue):
   • Azure Databricks (first-party; Microsoft-owned stake)
   • Azure SQL (operational DB)
   • Cosmos DB
   • ADLS Gen2 (underlies OneLake)
   • Event Hubs (ingestion)
```

#### 4.2 Fabric Core Concepts

```
WORKSPACE:
   Container for Fabric items (lakehouses, warehouses, notebooks, pipelines, reports)
   Permissions set at workspace level
   Think "project" or "team folder"

CAPACITY:
   Compute pool shared across workspaces assigned to it
   F SKU determines size
   Smoothing + bursting handles spikes

ONELAKE:
   Shared logical lake across your tenant
   Built on ADLS Gen2 under the hood
   Delta Lake is the primary table format
   Shortcuts can link to external storage (S3, GCS, ADLS, other OneLake)

ITEMS:
   Lakehouse, Warehouse, Semantic Model, Notebook, Pipeline, Dataflow,
   KQL Database, Real-Time Dashboard, ML Experiment, etc.
   All stored in a workspace; share OneLake storage.
```

#### 4.3 Fabric Experiences

```
Fabric is organized into "experiences" (which map to personas):

   SYNAPSE DATA ENGINEERING:
      Lakehouse, Spark notebooks, Spark job definitions
   
   SYNAPSE DATA WAREHOUSE:
      Fabric Warehouse (T-SQL, columnar)
   
   SYNAPSE DATA SCIENCE:
      Notebooks for ML, model management, experiments
   
   SYNAPSE REAL-TIME ANALYTICS:
      KQL Databases, Eventstreams, real-time dashboards
   
   DATA FACTORY:
      Data Pipelines (orchestration), Dataflow Gen2 (low-code ETL)
   
   POWER BI:
      Reports, dashboards, semantic models
   
   DATA ACTIVATOR:
      Trigger actions on data patterns

All share OneLake. Users switch experiences in one UI.
```

#### 4.4 When to Use Fabric vs Alternatives

```
PICK FABRIC WHEN:
   ✓ Azure-committed shop
   ✓ Mixed workloads (BI, data eng, ML, real-time)
   ✓ Power BI is strategic (deep integration)
   ✓ Want simple SaaS; no infra management
   ✓ Mid-size scale (TB-PB; up to F512 or so)

PICK DATABRICKS ON AZURE WHEN:
   ✓ Serious Spark workloads
   ✓ Delta Lake-first with Unity Catalog
   ✓ ML-heavy pipelines (MLflow deep integration)
   ✓ Open-architecture, portable, multi-cloud intent
   ✓ Already have Databricks expertise

PICK HYBRID WHEN:
   ✓ Databricks for heavy processing + Fabric for BI
   ✓ Shared storage via OneLake shortcuts to Databricks Delta tables
```

---

## PART II — STORAGE

### 5. Azure Data Lake Storage Gen2 (ADLS Gen2)

#### 5.1 What ADLS Gen2 Is

```
AZURE DATA LAKE STORAGE GEN2 (ADLS GEN2):
   Blob storage + HDFS-compatible API ("hierarchical namespace")
   The storage foundation for Azure data lakes
   What OneLake (Fabric) runs on under the hood

Key properties:
   • Hierarchical namespace (true directories, not S3-style flat keys)
   • POSIX-like ACLs
   • Compatible with Hadoop API (abfss://)
   • Same durability + availability as Blob Storage
   • Up to 5 PiB per account
```

#### 5.2 ADLS vs S3

```
ADLS GEN2:
   ✓ True hierarchical directories (fast rename, list)
   ✓ POSIX ACLs per directory
   ✓ Azure-native integration (Databricks, Synapse, Fabric)
   ✗ Azure-only (S3 is multi-cloud ubiquitous via other-cloud S3-compatibles)

S3:
   ✓ De facto standard; multi-cloud and local tooling
   ✓ Enormous ecosystem
   ✗ Flat namespace emulates directories (rename is O(N))

Rule: use ADLS Gen2 in Azure, S3 in AWS, GCS in GCP.
      Don't try to be clever with cross-cloud storage.
```

#### 5.3 Access Models

```
Entra ID RBAC (preferred):
   Grant users / service principals / managed identities
   Roles: Storage Blob Data Reader/Contributor/Owner
   Fine-grained via Access Control (ACLs) on paths

Shared Keys (legacy):
   Account-level access keys
   Avoid in production

SAS Tokens:
   Time-limited scoped tokens
   Use for cross-service access or temporary sharing
```

#### 5.4 Pricing

```
STANDARD STORAGE (per GB-month):
   Hot:    ~$18/TB
   Cool:   ~$10/TB
   Cold:   ~$4/TB
   Archive: ~$1/TB

PREMIUM STORAGE:
   ~$150/TB (fast; for high-IOPS workloads like transactional lakes)

TRANSACTIONS (per 10K ops):
   Hot: $0.004 reads, $0.065 writes
   Cold tiers: higher per-op costs

EGRESS: standard Azure egress rates
```

### 6. Blob Storage Tiers and Lifecycle

```
HOT:     frequent access
COOL:    infrequent (>30 days typical)
COLD:    rare (>90 days)
ARCHIVE: compliance (hours to retrieve)

LIFECYCLE RULES: automatic tier transitions
   Based on last modified / last access
   
   Example rule:
     - Hot for 30 days → Cool
     - Cool for 60 days → Cold
     - Cold for 275 days → Archive
     - Archive for 7 years → delete

Not an afterthought: lifecycle rules can save 60-80% on cold data.
```

### 7. OneLake (Fabric's Unified Lake)

#### 7.1 What OneLake Is

```
ONELAKE = Fabric's "lake for all your data."
   
   Properties:
      • One per Microsoft tenant (not per subscription)
      • Built on ADLS Gen2 under the hood
      • Delta Lake is the primary format
      • Hierarchical: tenant → workspace → item → files

   Items in OneLake:
      Lakehouse: Spark + Delta tables + files
      Warehouse: T-SQL + columnar storage
      KQL Database: Kusto for logs/metrics
      Semantic Model: Power BI layer
```

#### 7.2 OneLake Shortcuts

```
SHORTCUT = symbolic link to external storage.

Types:
   OneLake → OneLake: reference tables in another workspace
   OneLake → ADLS Gen2: external Azure storage
   OneLake → S3: external AWS storage (!)
   OneLake → GCS: external GCP storage

USE CASES:
   Centralize access to data in multiple locations
   Avoid data copying across clouds
   Enable Fabric to query data living in Databricks

LIMITATIONS:
   Read-only through shortcuts (mostly)
   Shortcuts don't move data; they reference
   Permissions enforced where data physically lives
```

#### 7.3 Delta Lake in OneLake

```
Fabric stores Lakehouse tables as Delta Lake format.
UniForm support: Delta + Iceberg metadata both written (via Fabric settings).

Multi-engine implications:
   Fabric Spark reads/writes Delta
   Fabric Warehouse (T-SQL) reads Delta (and writes to its own warehouse)
   Power BI DirectLake reads Delta directly (no import/refresh)
   
Delta Lake is the LINGUA FRANCA inside Fabric.
```

---

## PART III — COMPUTE

### 8. Azure Databricks

#### 8.1 The First-Party Databricks

```
AZURE DATABRICKS is a first-party Microsoft service.
   Not just Databricks running on Azure — tight integration:
      Entra ID authentication
      Azure networking (VNets, Private Endpoints)
      Azure billing (shows up on Azure bill)
      Azure RBAC + resource groups
      Direct ADLS Gen2 integration

For many Azure-committed enterprises, Databricks is the primary Spark + ML platform.
```

#### 8.2 Capabilities

```
Standard Databricks capabilities apply:
   Delta Lake (native)
   Unity Catalog (governance)
   Databricks SQL (warehouse-like querying)
   MLflow + model serving
   Workflows (orchestration)
   Vector search

Azure-specific integrations:
   Databricks tables accessible via OneLake shortcuts (Fabric reads them)
   Can orchestrate from Azure Data Factory
   Logs can route to Azure Monitor
   SCIM provisioning from Entra ID
```

#### 8.3 When Azure Databricks vs Fabric

```
DATABRICKS WHEN:
   ✓ Serious Spark workloads
   ✓ Delta Lake with Unity Catalog is strategic
   ✓ MLflow / ML platform is primary
   ✓ Already have Databricks expertise
   ✓ Portability (works same on AWS, GCP)

FABRIC WHEN:
   ✓ BI-first with Power BI as endpoint
   ✓ Mid-size workloads (TB range)
   ✓ Want SaaS simplicity
   ✓ Shared capacity across workloads simpler to budget

HYBRID (common at mid-size enterprises):
   Databricks for ETL + ML + heavy transforms
   Fabric for BI consumption + semantic models
   OneLake shortcuts bridge Databricks Delta tables into Fabric
```

### 9. Synapse Analytics (Legacy + Dedicated SQL Pools)

#### 9.1 The Synapse Legacy

```
AZURE SYNAPSE ANALYTICS (launched 2019):
   Combined service: SQL + Spark + Pipelines + Data Flows
   Was positioned as "data warehouse + lake unified"

Where it stands in 2026:
   Still exists and supported
   New investment is going to Fabric (which absorbed Synapse)
   Existing Synapse workloads can continue; migration to Fabric is encouraged

PIECES OF SYNAPSE:
   Dedicated SQL Pool (formerly SQL DW): MPP columnar warehouse
   Serverless SQL Pool: pay-per-TB-scanned T-SQL on ADLS
   Spark Pool: managed Spark clusters
   Pipelines: data orchestration (same engine as Data Factory)
   Data Flows: visual ETL
   Synapse Studio: unified UI
```

#### 9.2 Dedicated SQL Pool

```
Massively parallel processing (MPP) columnar warehouse.
   Precursor to Fabric Warehouse.
   
   Pricing: DWU (Data Warehouse Units)
   Scale: DW100c (small) to DW30000c (very large)
   
   LEGACY STATUS: still supported, but Fabric Warehouse is the strategic successor.
```

#### 9.3 Serverless SQL Pool (Still Useful)

```
Serverless T-SQL endpoint over data in ADLS.
   Query Parquet / Delta / CSV / JSON without a cluster
   Pay-per-TB scanned ($5/TB)
   
Similar to AWS Athena.

Still used in 2026 for:
   Ad-hoc queries over lake data
   Lightweight analytics without provisioning compute
   Scenarios where Fabric capacity isn't yet in play
```

### 10. Fabric Spark and Data Engineering Experience

#### 10.1 Fabric Spark

```
FABRIC SPARK:
   Managed Spark running on Fabric capacity
   Uses Delta Lake natively
   Notebooks + Spark job definitions
   Spark Pool types: Starter (shared) or custom
   Auto-scaling + session management

Different from standalone Databricks:
   Fabric Spark runs on Fabric capacity (CU-based pricing)
   Deeper Fabric integration (OneLake, Semantic Models)
   Less mature than Databricks Spark but improving fast
```

#### 10.2 Lakehouses in Fabric

```
FABRIC LAKEHOUSE:
   A storage + compute item in a Fabric workspace
   
   Contains:
      Tables (Delta Lake managed tables)
      Files (unstructured area)
      SQL endpoint (automatic for T-SQL queries)
      
   Underneath: OneLake storage (Delta tables + raw files)
   
PATTERN:
   Ingest raw → Lakehouse (Bronze)
   Transform via Spark/Dataflow → Lakehouse (Silver)
   Model via Spark/dbt/Dataflow → Lakehouse or Warehouse (Gold)
   Power BI DirectLake reads Gold without import
```

### 11. HDInsight (Legacy Hadoop)

```
AZURE HDINSIGHT = managed Hadoop/Spark/Kafka/HBase.
   
   Status in 2026: LEGACY. Microsoft nudges toward Fabric or Databricks.
   Still exists for customers with existing deployments.
   
   USE: avoid for new workloads. Migrate to Databricks (Spark) or Fabric.
```

### 12. Azure Functions and Logic Apps

#### 12.1 Azure Functions

```
Serverless functions (like AWS Lambda).
   Triggers: HTTP, Timer, Blob, Event Grid, Service Bus, Cosmos DB changes
   Languages: C#, Python, Java, JavaScript, PowerShell

For data engineering:
   ✓ Blob trigger: process files when they land
   ✓ Event Grid trigger: react to Azure resource events
   ✓ Timer trigger: scheduled lightweight jobs
   ✗ Long-running compute (10 min default; up to 60 min on Premium)
```

#### 12.2 Logic Apps

```
LOGIC APPS = visual workflow service.
   
   Connectors to Azure + Microsoft 365 + many SaaS
   Low-code/no-code workflow design
   
For data engineering:
   Less common than Data Factory for ETL
   Useful for: event-driven glue, SaaS integrations, notification workflows
   Overlaps with Fabric Data Pipelines in some patterns
```

---

## PART IV — INGESTION

### 13. Azure Event Hubs

#### 13.1 What Event Hubs Is

```
AZURE EVENT HUBS = managed event streaming service.
   
   Positioning:
      Started as "Kafka-alternative on Azure"
      Added Kafka protocol compatibility (2017+): consumers/producers can use Kafka clients
      
   Scale: millions of events/sec
   Retention: 1-90 days (Dedicated SKU) or up to 7 days (Standard)
   Partitioned per Event Hub (like Kafka topics)
```

#### 13.2 Event Hubs vs Kafka

```
EVENT HUBS:
   ✓ Fully managed
   ✓ Kafka protocol compatible (migrate from Kafka with minimal code change)
   ✓ Deep Azure integration (Stream Analytics, Functions, ADLS Capture)
   ✗ Fewer features than full Kafka (e.g., no custom partitioners)
   ✗ Azure-only

KAFKA ON AZURE:
   Options: Confluent Cloud, HDInsight Kafka (legacy), self-hosted on AKS
   ✓ Full Kafka features
   ✓ Multi-cloud portability
   ✗ More operational burden (self-hosted) or vendor cost (Confluent)

STAFF ANSWER:
   For Azure-committed with simple streaming: Event Hubs
   For Kafka ecosystem (Debezium, Streams, Flink): Confluent on Azure
```

#### 13.3 Event Hubs Capture

```
AUTOMATIC capture to ADLS Gen2 or Blob Storage.

Config:
   Capture every N minutes OR N MB
   Format: Avro (default) or Parquet
   Partitioned by capture time
   
Result: streaming events automatically land in your lake as Parquet/Avro.
   Similar to Kinesis Firehose on AWS.
```

### 14. Azure Stream Analytics

#### 14.1 What ASA Is

```
AZURE STREAM ANALYTICS = managed streaming SQL.
   Older service; declarative SQL on streams.
   
   Sources: Event Hubs, IoT Hub, Blob Storage
   Sinks: SQL, Cosmos DB, Blob, Event Hubs, Power BI
   
Status in 2026: still used but overshadowed by:
   Databricks Structured Streaming (for Spark shops)
   Fabric Real-Time Analytics (Kusto, event streams)
   Flink (via Confluent Cloud or self-hosted)
```

#### 14.2 When to Use ASA

```
PICK ASA WHEN:
   ✓ Simple SQL-based streaming
   ✓ Light workloads (not TB/day)
   ✓ All-Azure, don't want to introduce Flink/Spark

PICK FLINK or DATABRICKS WHEN:
   ✓ Complex state, large joins, event-time semantics
   ✓ Higher throughput
   ✓ Multi-cloud intent
```

### 15. Azure Data Factory

#### 15.1 What ADF Is

```
AZURE DATA FACTORY = managed ETL + orchestration.
   
   Capabilities:
      Pipelines (orchestration DAGs)
      Data Flows (visual Spark-based ETL)
      Copy Activity (bulk data movement, 90+ connectors)
      Integration Runtimes (compute for pipelines; auto-managed or self-hosted)

HISTORICAL: the primary ETL tool in Azure for years.
CURRENT: Fabric Data Pipelines is the strategic successor; ADF continues to be supported.
```

#### 15.2 ADF Patterns

```
PATTERN 1: Copy data
   On-prem SQL → ADF Copy → ADLS Gen2 as Parquet

PATTERN 2: Orchestrate across services
   Pipeline:
      1. Trigger Databricks notebook
      2. Wait for completion
      3. Trigger Synapse SQL stored proc
      4. Email on success/failure

PATTERN 3: Scheduled ingestion
   SaaS source (via connector) → ADF → ADLS or Synapse

Data Flows for visual Spark-based transforms (less common pattern now).
```

#### 15.3 ADF vs Fabric Data Pipelines

```
ADF:
   Standalone product; separate billing
   Mature; broad connectors; integration runtime concept
   Still the right choice if you're not fully in Fabric

FABRIC DATA PIPELINES:
   Within Fabric; shared capacity pricing
   Mostly same engine as ADF (ADF team's work)
   Deeper integration with Fabric items (Lakehouse, Warehouse, Notebooks)
   
   Direction: new work in Fabric; existing ADF workloads can migrate (same JSON format mostly).
```

### 16. Fabric Data Pipelines

```
Pipeline authoring UI inside Fabric.
   Same conceptual model as ADF
   Activities trigger Fabric items (Notebooks, Spark jobs, Warehouse procedures)
   Can also trigger external (Databricks, HTTP, Function, etc.)

DATAFLOW GEN2:
   Power Query-based visual ETL
   Great for business users doing low-code transforms
   Good for Silver → Gold layer when SQL/Spark would be overkill
```

---

## PART V — ANALYTICS

### 17. Synapse Dedicated SQL Pool

```
Legacy MPP columnar warehouse (formerly SQL Data Warehouse).
   Pre-dates Fabric; still supported.
   
In 2026:
   Existing deployments continue.
   New deployments → Fabric Warehouse OR Snowflake OR Databricks SQL.
   
   Microsoft positions Fabric Warehouse as the successor.
```

### 18. Fabric Warehouse and Lakehouse

#### 18.1 Fabric Warehouse

```
FABRIC WAREHOUSE = Microsoft's cloud warehouse in Fabric.
   T-SQL endpoint; columnar storage
   Stored in OneLake as Delta Lake (!)
   
   Why Delta: enables cross-workload interop (Spark can read it; Power BI DirectLake queries it directly)

Features:
   T-SQL (familiar for Microsoft shops)
   Columnar + MPP execution
   Separation of storage/compute
   Zero-copy clones
   Time travel
   Multi-table transactions
```

#### 18.2 Fabric Lakehouse

```
FABRIC LAKEHOUSE: 
   Spark-centric; emphasizes files + Delta tables + SQL endpoint
   
   SQL endpoint is read-only on top of Delta tables (via Serverless SQL)
   Writes happen through Spark notebooks or Dataflow Gen2
```

#### 18.3 Warehouse vs Lakehouse in Fabric

```
WAREHOUSE:
   T-SQL first-class; multi-statement transactions; writes via SQL
   Analyst-friendly (familiar for SQL Server shops)
   
LAKEHOUSE:
   Spark-first; writes via Spark
   SQL endpoint is read-only (query Delta via T-SQL)
   
Choice:
   SQL-centric team → Warehouse
   Spark/data-engineering team → Lakehouse
   Mixed → both (they interop)
```

### 19. Azure SQL and Cosmos DB

```
AZURE SQL DATABASE:
   Managed SQL Server as a service
   OLTP workload
   Multiple tiers (General Purpose, Business Critical, Hyperscale)

AZURE SQL MANAGED INSTANCE:
   SQL Server with near-full compatibility (for lift-and-shift)

AZURE DATABASE FOR POSTGRESQL / MYSQL:
   Managed open-source databases
   Flexible Server is the modern deployment option

COSMOS DB:
   Multi-model NoSQL (document, graph, key-value, column, Mongo API)
   Globally distributed with multi-master
   SLA: single-digit ms reads/writes
   
   USE: operational database for apps needing global scale
   Pricing: RU/sec (Request Units)
   Expensive if misused; cost-effective for right workloads

Change feed: Cosmos → trigger Function or Event Hubs for streaming downstream
```

### 20. Power BI

#### 20.1 What Power BI Is

```
POWER BI = Microsoft's BI platform.
   Reports + dashboards + semantic models
   Industry-leading installed base in enterprises
   
   Flavors:
      Power BI Pro: per-user, basic
      Power BI Premium: capacity-based
      Fabric: Premium capability included (F64+ SKU gets Power BI Premium)
```

#### 20.2 Power BI Semantic Models

```
SEMANTIC MODEL (formerly dataset):
   Tabular model with measures, calculated columns, hierarchies
   DAX (query language) for measures
   Supports DirectQuery, Import, DirectLake modes

MODES:
   IMPORT: data loaded into VertiPaq in-memory engine; fast reads; needs refresh
   DIRECTQUERY: live SQL to source each query; slower; no refresh
   DIRECTLAKE: reads Delta tables directly from OneLake; fast + fresh; Fabric-only
```

#### 20.3 Power BI DirectLake

```
NEW IN FABRIC (2023+):
   Semantic model reads Delta tables directly from OneLake
   No import step; always fresh
   Nearly as fast as Import mode
   
Changed the game for analyst performance:
   Analyst writes a report; data auto-refreshes without import jobs
   Reduces data duplication (no copy from lake to tabular model)
```

---

## PART VI — GOVERNANCE AND ML

### 21. Microsoft Purview

#### 21.1 What Purview Does

```
MICROSOFT PURVIEW = unified governance.
   
   Capabilities:
      Data catalog (automated discovery + classification)
      Data map (lineage, schema evolution)
      Data policy (access control integration)
      Data loss prevention
      Compliance management
      Audit + eDiscovery

For data engineering:
   ✓ Automatic cataloging of ADLS, Synapse, SQL, Fabric
   ✓ Lineage tracking across tools
   ✓ PII / sensitive data classification
   ✓ Access policy enforcement

Integrates with Fabric: Fabric items automatically appear in Purview.
```

#### 21.2 vs AWS Glue Catalog, Unity Catalog, Polaris

```
PURVIEW:
   Broadest scope (Microsoft 365, PowerBI, Azure, cross-cloud)
   More enterprise-governance-focused than lake-table-focused
   Not an Iceberg catalog itself

LAKE TABLE CATALOGS (Unity, Polaris, Glue):
   Deeper table-format integration (Iceberg/Delta read/write)
   Less compliance/governance focus (that's a layer above)

In Azure shops with Fabric:
   Fabric Lakehouse exposes tables; Purview catalogs them
   Databricks Unity Catalog manages tables; Purview integrates for governance view
```

### 22. Azure Machine Learning

#### 22.1 What Azure ML Is

```
AZURE MACHINE LEARNING = Microsoft's ML platform.
   
   Capabilities:
      Workspaces for team ML
      Notebooks + compute instances
      Managed training (CPU/GPU clusters)
      Model registry + versioning
      Managed endpoints for inference
      MLOps (pipelines, triggers)
      AutoML
      Responsible AI tools (fairness, interpretability)
```

#### 22.2 Azure ML vs Databricks ML

```
AZURE ML:
   ✓ Microsoft-first integrations
   ✓ Deep Entra ID + Azure networking
   ✓ Good for teams rooted in Microsoft stack

DATABRICKS ML (on Azure):
   ✓ MLflow is best-in-class
   ✓ Deep lakehouse integration
   ✓ Portable to other clouds

HYBRID common: Azure ML for serving infrastructure; Databricks for training.
```

### 23. Fabric Data Science

```
Within Fabric: notebooks + MLflow + model management + experiments
   Deep OneLake integration
   
Status in 2026: maturing. Less feature-complete than Azure ML or Databricks ML yet.
Good for Fabric-centric shops doing light-to-moderate ML.
Heavy ML workloads still go to Azure ML or Databricks.
```

---

## PART VII — OPERATIONS

### 24. Azure Monitor and Log Analytics

```
AZURE MONITOR:
   Metrics + logs + alerts across Azure
   Integrates with Grafana, Datadog, Dynatrace

LOG ANALYTICS:
   Log store queried via KQL (Kusto Query Language)
   Underlying tech is the same as Azure Data Explorer / Fabric Real-Time Analytics
   
   Use for: diagnostic logs, security events, custom app logs
   KQL is powerful; learn it if you're on Azure deeply

APPLICATION INSIGHTS:
   APM; code-level request + exception tracking

For data engineering:
   Monitor Databricks job metrics via Azure Monitor
   Fabric capacity utilization via Capacity Metrics app
   Pipeline failures via Data Factory activity logs
```

### 25. Azure DevOps and Deployment Patterns

```
AZURE DEVOPS (Repos + Pipelines + Boards):
   CI/CD for data engineering code

Common patterns:
   Databricks notebooks + dbt → GitHub (or Azure Repos) → GitHub Actions / Azure DevOps Pipelines
      → Deploy to dev/staging/prod Databricks workspaces
   
   Fabric: deploy items (notebooks, pipelines, semantic models) via git integration
      Fabric has git support (Azure DevOps / GitHub) for version control

INFRASTRUCTURE AS CODE:
   Bicep (Azure-native) or Terraform
   Deploy workspaces, clusters, storage, networking
```

---

## PART VIII — INTERVIEW PRACTICE

### 26. The Staff-Level Azure Data Interview

#### 26.1 Common Questions

```
1. "Explain Microsoft Fabric. What problem does it solve?"
2. "Fabric vs Databricks on Azure — when do you pick each?"
3. "Design a data lake architecture on Azure."
4. "What's the role of OneLake? How does it differ from ADLS Gen2?"
5. "Explain Delta Lake's role in Fabric."
6. "Power BI DirectLake — what is it and why does it matter?"
7. "How does Entra ID authenticate data services?"
8. "Design a CDC pipeline from Azure SQL to a lake."
9. "Compare Event Hubs vs Kafka on Azure."
10. "What's the pricing model of Fabric capacity? Trade-offs vs per-service billing?"
```

#### 26.2 Worked Example

```
Q: Design the Azure data platform for a 200-person retail company.
   100 TB current, growing 2x/year. Mix of operational SQL,
   e-commerce events, SaaS data (Salesforce, HubSpot).
   50 analysts, 10 data engineers, 5 ML engineers.
   Budget: $50K/month.

STAFF ANSWER:

CORE STACK: Fabric (primary) + Databricks on Azure (heavy processing).

STORAGE:
   • OneLake as the primary lake (Delta Lake under the hood)
   • ADLS Gen2 for raw landing (shortcut into OneLake)
   • Databricks Delta tables in ADLS Gen2 (OneLake shortcut makes them visible to Fabric)

INGESTION:
   • Azure SQL CDC: Debezium on AKS → Event Hubs → Databricks → OneLake Bronze
     (OR Fabric Mirroring if simplicity > flexibility)
   • E-commerce events: Event Hubs (Kafka API) → Databricks Structured Streaming → Bronze
   • SaaS: Fivetran → OneLake (more mature than AppFlow equivalents)

PROCESSING:
   • Databricks for Bronze → Silver (CDC apply, cleaning)
   • dbt-databricks for Silver → Gold modeling
   • Fabric Spark for Fabric-native pipelines

ANALYTICS:
   • Fabric Warehouse: Gold-layer SQL for analysts (T-SQL familiar)
   • Fabric Lakehouse SQL endpoint: ad-hoc queries
   • Power BI DirectLake: dashboards (no refresh, always fresh)

ML:
   • Databricks ML + MLflow for training and feature store
   • Fabric Data Science for lighter notebooks
   • Model serving via Databricks Model Serving

ORCHESTRATION:
   • Fabric Data Pipelines for pipeline + Databricks orchestration
   • Databricks Workflows for pure-Databricks DAGs

GOVERNANCE:
   • Purview catalogs Fabric items + Databricks Unity Catalog
   • Entra ID + Fabric workspace permissions

COST BREAKDOWN (approximate):
   Databricks (Standard/Premium DBUs + VMs):  ~$20K
   Fabric F128 Capacity (reserved):             ~$10K
   ADLS Gen2 storage + transactions:            ~$3K
   Event Hubs (standard throughput units):      ~$2K
   Fivetran:                                     ~$5K
   Azure SQL (operational):                      ~$2K
   Power BI (included in F128+):                 included
   Purview:                                      ~$1K
   Networking, egress, misc:                     ~$3K
   Observability (Datadog + Azure Monitor):      ~$2K
   Buffer:                                        ~$2K
   Total: ~$50K/month

WHY THIS BLEND:
   • Databricks for heavy lifting (serious Spark, ML)
   • Fabric for analyst-facing + BI (Power BI deep integration)
   • OneLake shortcuts bridge them (same data visible in both)
   • Minimizes duplication; each tool does what it's best at

OFF-RAMPS:
   • Databricks is portable (works on AWS, GCP)
   • Delta Lake format is open
   • Fabric is Microsoft-specific; harder to migrate off
   • Mitigation: data stays as Delta on ADLS; Fabric is "the UI layer" that could be swapped
```

### 27. War Stories

> **🔥 Production Scenario — The Fabric Capacity Pause.** A team ran Fabric F64 capacity 24/7, usage averaged 20%. Monthly cost: ~$8K. Switched to pay-as-you-go with scheduled scale-down to F2 overnight + scale-up to F32 business hours. Monthly cost: $2.5K. 70% savings. Lesson: Fabric capacity sizing is a critical FinOps lever; don't leave big capacity running 24/7.

> **🔥 Production Scenario — The Private Endpoint Explosion.** An enterprise locked everything to Private Endpoints. 150+ endpoints at ~$0.01/hr each = ~$1K/month just in endpoint fees, before any data processing. Fix: consolidated endpoints (one per service per environment, shared across workloads). Dropped to ~$300/month. Lesson: Private Endpoints are per-service, per-service-instance; architect your network to minimize count.

> **🔥 Production Scenario — The Databricks + Fabric Confusion.** A team ran Databricks for ETL and Fabric for BI. Analysts kept complaining Power BI showed stale data. Investigation: Fabric Lakehouse was pointing at a COPIED version of the Databricks Delta tables (not shortcut). ETL updates weren't visible for hours. Fix: switched to OneLake shortcuts referencing Databricks Delta directly. Instant freshness. Lesson: OneLake shortcuts > copying; understand the difference.

> **🔥 Production Scenario — The Event Hubs Partition Rebalance.** A team scaled Event Hubs from 4 to 32 partitions mid-day. Consumer group rebalance stalled their Databricks Structured Streaming job for 15 minutes. Fix: planned partition changes during maintenance windows; monitored consumer lag during rebalances. Lesson: Event Hubs partition changes aren't zero-impact.

> **🔥 Production Scenario — The Purview Classification Cost.** A team enabled Purview automatic classification on their entire ADLS Gen2 account. Scan ran for a week, cost $4K (per-GB classification scan). Fix: scoped Purview scans to specific containers; scheduled incremental scans. Lesson: Purview automatic features aren't free; configure scope.

### 28. Cheat Sheet &amp; Mental Models

#### 28.1 The One-Minute Pitch

```
Azure data engineering in 2026 is MICROSOFT FABRIC + Azure Databricks + core infra.

FABRIC: unified SaaS data platform (Lakehouse, Warehouse, Pipelines, Power BI).
   Shared OneLake (Delta Lake-based). Shared capacity (CU-based pricing).
   Great for BI-centric mid-size enterprises.

DATABRICKS ON AZURE: first-party Databricks service.
   For serious Spark + ML + Delta Lake with Unity Catalog.
   Portable to other clouds.

CORE INFRA: ADLS Gen2 (storage), Event Hubs (streaming), Entra ID (auth),
   Azure SQL/Cosmos DB (operational DB), Purview (governance).

PATTERN FOR LARGE ENTERPRISES:
   Databricks for ETL/ML + Fabric for BI, bridged by OneLake shortcuts.
```

#### 28.2 The Four Lenses — Compressed

```
CONCEPT
   Azure data = Fabric (new; unified SaaS) + Databricks (specialized) + core infra.

MECHANICS
   OneLake (Delta on ADLS Gen2) + capacity-based pricing.
   Entra ID across services. Private Endpoints for networking.
   Fabric experiences organize UI; items live in workspaces.

TRADE-OFFS
   Pros: integrated BI story (Power BI DirectLake), unified pricing (Fabric), great Databricks integration.
   Cons: Fabric is Microsoft-specific (harder to migrate off); naming/branding churn.

INTERVIEW
   Know: Fabric structure, OneLake, Databricks positioning, Power BI DirectLake,
   Entra ID + Private Endpoints, pricing patterns.
   Defend: when to use Fabric vs Databricks vs hybrid.
```

#### 28.3 Service Decision Guide

```
NEED                                   SERVICE
================================================
Object/lake storage                    ADLS Gen2 (or OneLake)
Unified data platform (BI + ETL + ML)  Fabric
Serious Spark + ML                     Azure Databricks
Operational SQL (SQL Server family)    Azure SQL Database
Operational NoSQL                      Cosmos DB
Managed Postgres/MySQL                 Azure Database for Postgres/MySQL
Streaming ingest (Kafka-compatible)    Event Hubs
Managed Kafka ecosystem                Confluent Cloud on Azure
Streaming SQL                          Fabric Real-Time Analytics (Kusto) or ASA
Batch ETL                              Fabric Data Pipelines or Databricks
Warehouse (T-SQL)                      Fabric Warehouse (new) or Synapse Dedicated (legacy)
Serverless lake SQL                    Synapse Serverless SQL Pool
BI                                     Power BI
ML platform                            Azure ML or Databricks ML
Governance                             Microsoft Purview
Logs / Kusto analytics                 Log Analytics or Fabric RTA
Identity                               Entra ID + Managed Identities
```

#### 28.4 Mental Model

```
Think of Azure data as:
   • A platform heavily influenced by Microsoft's enterprise strengths
     (Power BI, SQL Server, Entra ID)
   • Fabric is the big consolidation bet (2023+)
   • Databricks is the premium add-on for serious data engineering
   • OneLake is the Delta Lake-based "lake for everything"
   • Capacity pricing (Fabric) is the new model; per-service billing fades

When to reach for Azure data services:
   • Microsoft-committed enterprise
   • Power BI is strategic
   • Deep Entra ID + M365 integration matters

When to reach past them:
   • Multi-cloud mandate
   • Avoid Microsoft lock-in
   • Open-format portability is critical
```

> **🧠 Principal Mindset:** Microsoft Azure's data story has shifted dramatically with Fabric. If your interview is Azure-centric, know Fabric well — components, capacity model, Delta/OneLake, Power BI DirectLake. But also know its boundaries: Fabric is less portable than its AWS/GCP counterparts, and for serious Spark/ML, Databricks on Azure often remains the right choice. Staff-level answers position Fabric and Databricks as complementary, not competing — the question is "which does what in YOUR architecture," not "which do you pick overall."

---

[← Back to main](./README.md)
