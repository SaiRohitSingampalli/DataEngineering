# Topic 17 — AWS for Data Engineering

> Standalone deep-dive · The AWS data engineering service landscape for principal/staff engineers

[← Back to main](./README.md)

---

## How to use this document

This chapter covers AWS's data engineering service portfolio at architect level: what each service is, when to use it, how services compose, and the gotchas. The focus is on **engineering judgment** — not click-through-wizard tutorials. Every chapter follows the four lenses: **Concept · Mechanics · Trade-offs · Interview**.

> **🧠 Principal Mindset:** AWS has 50+ data services. Nobody uses all of them. Staff engineers know the ~15 that matter, how they compose into reference architectures, and when to say "no, we don't need Service X" because its job is already done by what you have. The real signal is being able to defend a minimal, coherent AWS data stack — not naming the most services.

## Table of Contents

**PART I — FOUNDATIONS**
- 1. The AWS Data Service Landscape
- 2. Shared Responsibility, IAM, and Networking Basics
- 3. Pricing Patterns and FinOps on AWS
- 4. The AWS Data Reference Architecture

**PART II — STORAGE**
- 5. Amazon S3: The Foundation of AWS Data
- 6. S3 Tables (Managed Iceberg, 2024+)
- 7. S3 Tiering, Lifecycle, and Cost Control
- 8. Lake Formation: Governance Layer

**PART III — COMPUTE**
- 9. EMR and EMR Serverless (Managed Spark/Hadoop)
- 10. AWS Glue (ETL + Catalog)
- 11. Lambda for Data Engineering
- 12. Athena (Serverless Trino SQL)

**PART IV — INGESTION**
- 13. Kinesis Data Streams and Firehose
- 14. Amazon MSK (Managed Kafka)
- 15. AWS DMS (CDC and Migration)
- 16. DataSync, AppFlow, Transfer Family

**PART V — ANALYTICS**
- 17. Amazon Redshift Deep Dive
- 18. OpenSearch and Serverless OpenSearch
- 19. Timestream (Time Series)

**PART VI — ORCHESTRATION**
- 20. Step Functions
- 21. MWAA (Managed Airflow)
- 22. EventBridge
- 23. Choosing Between Them

**PART VII — ML AND GOVERNANCE**
- 24. SageMaker for the Data Engineer
- 25. Lake Formation Governance Patterns
- 26. CloudWatch, CloudTrail, and Cost Observability

**PART VIII — INTERVIEW PRACTICE**
- 27. The Staff-Level AWS Data Interview
- 28. War Stories
- 29. Cheat Sheet &amp; Mental Models

---

## PART I — FOUNDATIONS

### 1. The AWS Data Service Landscape

#### 1.1 The Service Map

AWS has over 200 services. For data engineering, roughly 20 matter:

```
STORAGE
   S3              Object storage (foundation)
   S3 Tables       Managed Iceberg tables (2024+)
   EBS             Block storage for EC2
   EFS             Shared filesystem
   FSx             Managed Lustre/Windows/NetApp

INGESTION
   Kinesis Data Streams  Kafka-like streaming
   Kinesis Firehose      Managed delivery to S3/Redshift
   MSK                   Managed Kafka
   DMS                   CDC from RDBMS to S3/Redshift
   DataSync              File sync (on-prem ↔ cloud)
   AppFlow               SaaS source connectors
   Transfer Family       SFTP/FTP endpoints

COMPUTE / PROCESSING
   EMR                   Managed Spark/Hadoop clusters
   EMR Serverless        Serverless Spark/Hive
   Glue                  Serverless ETL + catalog
   Lambda                Serverless functions
   Fargate               Serverless containers
   Batch                 Managed batch compute

ANALYTICS
   Athena                Serverless Trino SQL on S3
   Redshift              Columnar warehouse
   Redshift Serverless   Serverless Redshift
   OpenSearch            Search + analytics (Elasticsearch fork)
   Timestream            Time-series database
   QuickSight            BI dashboarding

ORCHESTRATION
   Step Functions        Stateful workflows
   MWAA                  Managed Airflow
   EventBridge           Event bus + scheduler
   Lambda + SQS/SNS      Event-driven glue

ML
   SageMaker             End-to-end ML platform
   Bedrock               Managed LLMs
   Comprehend/Translate  Pre-trained NLP

GOVERNANCE
   Lake Formation        Fine-grained lake access
   Glue Data Catalog     Metastore
   Macie                 PII discovery
   DataZone              Data mesh / discovery (newer)

OBSERVABILITY
   CloudWatch            Metrics, logs, alarms
   CloudTrail            API audit log
   Cost Explorer         Billing analysis
   AWS Budgets           Alerts on spend
```

#### 1.2 The Shape of a Modern AWS Data Stack

```
Sources → Ingestion → Lake (S3) → Processing → Serving → Consumers
             │              │             │           │
             │          Lake Formation Catalog         │
             │           (Glue Data Catalog)           │
             │                                          │
         Kinesis            Spark/Glue              Athena
         MSK                SQL (Athena)            Redshift
         DMS                Serverless                QuickSight
         AppFlow            Lambda                    SageMaker

   Orchestration overlaying everything:
         Step Functions (stateful workflows)
         MWAA (DAGs)
         EventBridge (schedules + event bus)
```

#### 1.3 What You DON'T Need

```
If you have an open lakehouse architecture (S3 + Iceberg + external catalog):
   ✗ You often don't need Lake Formation (use Polaris / Unity Catalog instead)
   ✗ You often don't need Athena (use Trino, DuckDB, or Databricks)
   ✗ You don't need Glue ETL (use Spark / dbt directly)

If you have an all-in-AWS strategy:
   ✓ Lake Formation + Glue + Athena make sense (tight AWS integration)
   ✓ You pay more; you lose portability; operations are simpler

STAFF-LEVEL FRAMING: "Pick the minimum coherent set of AWS services"
   that serves your workloads. More services ≠ better architecture.
```

> **🎯 Interview Angle:** Q: AWS has 50+ data services. How do you decide which to use?
> **Staff:** "I start from workloads, not services. For each workload, identify the specific job to be done: ingest, store, transform, serve. Then pick the simplest AWS service that does that job well AND composes with what else is already in the stack. Three principles: (1) prefer serverless where usage is bursty (Athena, Glue, Lambda); (2) prefer managed-open when portability matters (MSK over Kinesis if we might leave AWS); (3) accept vendor lock-in deliberately when the integration tax is worth it (Lake Formation + Glue is tight AWS lock-in but reduces operational effort if committed to AWS). What I don't do: adopt services because they're there. Each service is operational surface area."

### 2. Shared Responsibility, IAM, and Networking Basics

#### 2.1 Shared Responsibility Model

```
AWS RESPONSIBLE FOR:
   Physical infrastructure
   Hypervisors / managed service runtimes
   Network backbone
   Service availability within SLA

CUSTOMER RESPONSIBLE FOR:
   IAM configuration (who can do what)
   Data encryption keys
   Network configuration (VPC, security groups)
   Application-level security
   Backup / DR strategy
   Cost management
```

#### 2.2 IAM — The Core

```
IDENTITY:
   Users (humans; rare for apps; prefer roles)
   Roles (temporary credentials; what services + apps use)
   Groups (collections of users)
   Service accounts (via roles)

PERMISSIONS:
   Policies (JSON documents) attached to identities or resources
   Allow / Deny with conditions

PATTERNS:
   Role-based access (apps assume roles, not use static keys)
   STS (Security Token Service) for temporary credentials
   AssumeRole chains for cross-account access
   IAM Identity Center (formerly SSO) for human identity

LEAST PRIVILEGE:
   Start with nothing. Add permissions as needed.
   Audit via Access Analyzer periodically.
```

#### 2.3 Networking Basics for Data Engineers

```
VPC — your isolated network in AWS
   Public subnets (internet-reachable via IGW)
   Private subnets (no direct internet; use NAT GW)
   
VPC Endpoints — reach AWS services without internet
   Gateway endpoints (S3, DynamoDB; free)
   Interface endpoints (most other services; charged per hour + GB)
   
   WHY: lower cost (no NAT GW data processing fees), 
        better security (traffic stays inside AWS network)

Security Groups — stateful firewalls for ENIs
Network ACLs — stateless rules at subnet level
PrivateLink — expose services across VPCs/accounts securely

For data engineering specifically:
   Data landing/processing almost always lives in private subnets
   S3 access via Gateway endpoint (free, mandatory)
   RDS/Redshift via subnet groups
   Cross-VPC data access via PrivateLink or VPC peering
```

### 3. Pricing Patterns and FinOps on AWS

#### 3.1 The AWS Pricing Pattern

AWS pricing combines several dimensions per service:

```
STORAGE:  per GB per month (tier-dependent)
COMPUTE:  per instance-hour OR per vCPU-second (serverless)
I/O:      per request (GET, PUT, LIST, DELETE)
NETWORK:  egress to internet (expensive), cross-region (expensive),
          cross-AZ (sometimes charged)
MANAGED:  often a management premium above raw infra

Example — S3:
   Storage:   $0.023/GB/month (Standard)
   Requests:  $0.005 / 1000 PUT; $0.0004 / 1000 GET
   Egress:    $0.09/GB to internet (first 10 TB)
   Cross-region replication: adds inter-region transfer costs
```

#### 3.2 The "Three Hidden Costs"

```
EGRESS:
   Transferring data OUT of AWS is expensive.
   Example: 50 TB/month to internet = ~$4,500/month in egress alone.
   
   MITIGATIONS:
   ✓ CloudFront caching
   ✓ Direct Connect / Transit Gateway (lower per-GB rate)
   ✓ Keep processing in-cloud
   ✓ Compress aggressively before transfer

CROSS-REGION:
   Replication for DR is useful but costs real money.
   50 TB replicated to another region = ~$1,000/month in transfer
   + double storage.
   
   DECISION: is multi-region actually required or just "nice to have"?

CROSS-AZ:
   Historically $0.01/GB in some service pairs.
   Kafka/MSK, Elasticache, cross-AZ chatter adds up.
   
   MITIGATIONS: AZ-aware routing, co-locate producers + consumers.
```

#### 3.3 Reserved Capacity Patterns

```
EC2 / Redshift / OpenSearch:
   Reserved Instances (RI) or Savings Plans
   Commit 1 or 3 years for 30-70% discount
   
S3:
   S3 Intelligent-Tiering auto-tiers; no commit needed
   S3 Reserved Capacity for Glacier Deep Archive

Glue / Athena / Lambda:
   Pay-per-use (no reservations)

Rule: anything running 24/7 at predictable load → reserve it.
      Anything bursty / unpredictable → pay-per-use or Savings Plan.
```

#### 3.4 FinOps Discipline

```
MONITOR:
   AWS Cost Explorer: historical and projected spend
   AWS Budgets: alerts when forecast > threshold
   Cost Anomaly Detection: ML-based spike alerts
   Tagging strategy: tag ALL resources for cost attribution

OPTIMIZE:
   Rightsize: check CloudWatch metrics; downsize over-provisioned
   Lifecycle policies: move cold data to cheaper tiers
   Delete orphaned resources: EBS volumes, snapshots, unused ENIs
   Spot instances: for interruptible compute (EMR, Batch)
   
QUARTERLY REVIEW:
   Top-cost services and their trends
   Unused resources (last-accessed logging)
   Reservation utilization
   Egress and cross-region spikes
```

### 4. The AWS Data Reference Architecture

#### 4.1 The Four Common Patterns

```
PATTERN A: ALL-SERVERLESS LAKE
   Sources → Kinesis Firehose → S3 (Iceberg via Glue) 
          → Athena / Glue ETL → QuickSight
   
   Pros: nothing to manage, pay-per-use
   Cons: cost unpredictable at scale, less control
   Fits: startups, exploratory analytics, moderate scale

PATTERN B: LAKEHOUSE WITH DATABRICKS/SNOWFLAKE
   Sources → MSK/DMS → S3 (Iceberg) → Databricks or Snowflake
          → Looker/Tableau/QuickSight → Reverse ETL
   
   Pros: open storage, best-in-class compute, portable
   Cons: multi-vendor contracts, cross-cloud complexity
   Fits: enterprises, multi-cloud intent, large data teams

PATTERN C: REDSHIFT-CENTRIC
   Sources → DMS/Kinesis → S3 → Redshift (via Redshift Spectrum or COPY)
          → QuickSight / Tableau
   
   Pros: tight AWS integration, mature, zero-ETL available
   Cons: tied to AWS, less open-format
   Fits: AWS-committed shops, operational-DB-heavy integration

PATTERN D: STREAMING-NATIVE
   Sources → Kinesis / MSK → KDA (Flink) → ClickHouse/OpenSearch/S3
          → Grafana / Custom UI
   
   Pros: sub-second analytics, event-driven
   Cons: operational complexity, limited batch flexibility
   Fits: real-time dashboards, fraud detection, personalization
```

#### 4.2 Picking the Pattern

```
CRITERIA:                   PATTERN
=========================   =======
Scale < 100 TB              A (serverless)
Scale 100 TB - 1 PB         B or C (based on vendor preference)
Scale > 1 PB                B (lakehouse) or very carefully-architected C

Latency: minutes-OK         A, B, C
Latency: seconds required   D (plus one of A/B/C for history)

Team: 1-3 engineers         A (minimize ops)
Team: 5-15 engineers        B or C
Team: 15+ engineers         B typically wins

Multi-cloud intent          B
Pure AWS commitment         A or C
```

> **🧠 Principal Mindset:** Pattern A looks appealing because "no servers to manage." At scale it becomes expensive — Athena at TB/day scanned, Glue DPU-hours, Lambda GB-sec, Kinesis shard-hour. The crossover where B or C becomes cheaper is usually around 50-100 TB of hot data. Know where YOUR workload sits on that curve.

---

## PART II — STORAGE

### 5. Amazon S3: The Foundation of AWS Data

#### 5.1 The One-Sentence Definition

Amazon S3 (Simple Storage Service) is **highly durable object storage** — the foundation of nearly every AWS data architecture. 11 nines of durability, infinite scale, and the backbone of lakes, lakehouses, backups, and content delivery.

#### 5.2 S3 Properties

```
DURABILITY:  11 nines (99.999999999%)
AVAILABILITY: 99.99% (Standard); varies by tier
CONSISTENCY: Strong read-after-write (since 2020)
SCALE:       Virtually unlimited
OBJECT SIZE: Up to 5 TB per object
PRICING:     Per-GB-month + per-request

CORE API:
   PUT        Upload an object
   GET        Download an object
   LIST       Enumerate keys with prefix
   DELETE     Remove an object
   HEAD       Metadata only
   COPY       Server-side copy

ADVANCED:
   Multipart upload: for objects > 100 MB
   Pre-signed URLs: time-limited access
   S3 Select: SQL on individual objects (CSV, JSON, Parquet)
   S3 Batch Operations: bulk ops across many objects
```

#### 5.3 Storage Classes

```
STANDARD                 $23/TB/mo  Default; multi-AZ
STANDARD-IA              $12.5/TB   Infrequent access; retrieval cost
ONE ZONE-IA              $10/TB     Single AZ; cheaper
INTELLIGENT-TIERING      Auto-tier  Analyzes access; tiers automatically
GLACIER INSTANT RETRIEVAL $4/TB    Milliseconds retrieval
GLACIER FLEXIBLE RETRIEVAL $3.6/TB  Minutes retrieval
GLACIER DEEP ARCHIVE     $1/TB     Hours retrieval; long-term archive

Rule: Standard for hot (< 30 days typical access)
      IA for warm (less-than-monthly)
      Glacier for cold (quarterly+ or compliance archive)
```

#### 5.4 S3 for Data Engineering Specifically

```
LAKES AND LAKEHOUSES:
   S3 is the de facto storage for data lakes
   Partitioned Parquet in S3 + a catalog = a lake
   Add Iceberg/Delta/Hudi metadata → lakehouse

ORGANIZATION:
   bucket/
     warehouse/
       db=sales/
         table=orders/
           year=2024/
             month=03/
               day=15/
                 part-00000.parquet
                 part-00001.parquet

   Convention: Hive-style partitioning (key=value directories)
   Or: Iceberg hidden partitioning (DIR structure less meaningful)

ACCESS PATTERNS:
   ANALYTICAL: large sequential reads, long-lived connections
   STREAMING INGEST: many small PUTs; need multipart for performance
   BACKUP: large infrequent writes; Standard-IA or Glacier
```

#### 5.5 Performance Considerations

```
PARTITIONING FOR SCALE:
   S3 can scale to very high request rates per prefix
   For 100K+ RPS on one prefix, spread load across prefixes:
      Instead of  logs/YYYY/MM/DD/...
      Use         logs/<hash>/YYYY/MM/DD/...
      (hash the key for even distribution)
   
   For most data engineering workloads: not needed.
   The 3,500 RPS per prefix limit is rarely hit.

OBJECT SIZE:
   Too small (< 10 MB): metadata overhead dominates
   Too large (> 5 GB): slow retrieval, no partial fetch benefit
   Sweet spot for analytics: 128 MB - 1 GB per Parquet file

LIST PERFORMANCE:
   LIST is paginated (1000 keys per request)
   For millions of objects, use S3 Inventory (daily CSV manifests)
```

#### 5.6 Multipart Upload

```
For objects > 100 MB:
   1. Initiate multipart upload → upload ID
   2. Upload parts in parallel (5 MB - 5 GB each)
   3. Complete multipart upload

PITFALL: incomplete multiparts count toward storage billing!
   Set lifecycle rule: delete incomplete multiparts after 7 days.
```

> **🔥 Production Scenario:** A team discovered 2 TB of "mystery storage" on an S3 bucket not shown in object listings. Investigation: 50,000 incomplete multipart uploads from a buggy data loader. Each one abandoned, each one holding its parts in storage. Enabled lifecycle rule to abort incompletes after 7 days; storage dropped from 2 TB to 500 GB. Lesson: multipart cleanup is a quiet money leak — set the lifecycle rule on every bucket.

### 6. S3 Tables (Managed Iceberg, 2024+)

#### 6.1 What S3 Tables Is

```
S3 TABLES = AWS's managed Iceberg table storage.
   Launched: December 2024 (GA 2025)
   
   What it provides:
      • S3-native Iceberg storage
      • Automatic compaction
      • Automatic snapshot management
      • Automatic unreferenced file removal
      • Up to 3x faster TPS than DIY Iceberg on S3
      • Integrated with Athena, Redshift, EMR, Glue
```

#### 6.2 Why It Exists

```
BEFORE S3 TABLES:
   DIY Iceberg on S3 required:
      • Setting up a catalog (Glue, Nessie, custom)
      • Scheduling compaction (manual Spark jobs)
      • Snapshot expiry
      • Orphan file cleanup
   
   Operational burden was non-trivial.

WITH S3 TABLES:
   AWS does maintenance automatically.
   You just query and write.
   Catalog is managed (table bucket has its own namespace).
```

#### 6.3 When to Use

```
PICK S3 TABLES WHEN:
   ✓ All-AWS commitment
   ✓ Want managed Iceberg without operational burden
   ✓ Small-medium data engineering team
   ✓ Primary readers are Athena, Redshift, EMR, Glue

RECONSIDER WHEN:
   ✗ Multi-engine with Snowflake/Databricks (use Polaris + regular S3)
   ✗ Need advanced Iceberg features before AWS adds them
   ✗ Already have Iceberg + external catalog that works
```

#### 6.4 Pricing

```
S3 Tables pricing (approximate):
   Storage: ~$29/TB/month (vs $23 for standard S3)
   PUT/GET: standard S3 rates
   Compaction: no separate charge (included in storage premium)
   
Trade-off: pay ~25% more on storage; save compute cost of DIY compaction + ops.
```

### 7. S3 Tiering, Lifecycle, and Cost Control

#### 7.1 Lifecycle Policies

```json
{
  "Rules": [
    {
      "Id": "ArchiveOldData",
      "Status": "Enabled",
      "Filter": { "Prefix": "warehouse/raw/" },
      "Transitions": [
        { "Days": 30, "StorageClass": "STANDARD_IA" },
        { "Days": 90, "StorageClass": "GLACIER_IR" },
        { "Days": 365, "StorageClass": "DEEP_ARCHIVE" }
      ],
      "Expiration": { "Days": 2555 }
    },
    {
      "Id": "DeleteIncompleteMultiparts",
      "Status": "Enabled",
      "AbortIncompleteMultipartUpload": { "DaysAfterInitiation": 7 }
    }
  ]
}
```

#### 7.2 Intelligent Tiering

```
S3 Intelligent-Tiering automatically tiers objects between:
   • Frequent Access (same as Standard)
   • Infrequent Access (30 days without access)
   • Archive Instant Access (90 days)
   • Archive Access (opt-in, 90+ days)
   • Deep Archive Access (opt-in, 180+ days)

PRICING:
   Small monitoring fee (~$0.0025/1000 objects/month)
   Storage charged at the appropriate tier

GOOD FOR:
   Workloads with unpredictable or changing access patterns.
   You don't have to analyze access manually.

BAD FOR:
   Very small objects (monitoring fee dominates)
   Workloads with known, predictable access (manual lifecycle is cheaper)
```

#### 7.3 Monitoring and Analytics

```
S3 Storage Lens:
   Dashboard across all buckets
   Metrics: storage by tier, request patterns, cost by prefix
   Insights: incomplete multiparts, non-current versions, access patterns

S3 Inventory:
   Daily or weekly CSV/Parquet manifests of your bucket contents
   Useful for: compliance, large-scale analysis, orphan detection

CloudWatch metrics for S3:
   NumberOfObjects, BucketSizeBytes (daily)
   Request metrics per bucket (optional, charged)
```

### 8. Lake Formation: Governance Layer

#### 8.1 What Lake Formation Does

```
AWS Lake Formation sits on top of S3 + Glue Data Catalog, providing:
   • Fine-grained access control (table, column, row, cell)
   • Centralized permissions across multiple accounts
   • Audit logging (via CloudTrail)
   • Row-level filters
   • Column-level security (hide sensitive columns per role)
   • Data cells filtering (combine row + column)
   • Tag-based access control (apply policies via tags)
```

#### 8.2 How It Fits

```
Without Lake Formation:
   S3 IAM policies — all-or-nothing for a bucket/prefix
   Not fine-grained enough for multi-tenant lakes
   
With Lake Formation:
   Permissions managed at TABLE level
   Access brokered through Glue → engines (Athena, Redshift, EMR)
   
Lake Formation becomes the "data layer" permission plane for AWS lakes.
```

#### 8.3 Example

```sql
-- Grant SELECT on specific columns only (hiding PII)
GRANT SELECT ON TABLE sales.orders
    EXCLUDE (customer_ssn, customer_email)
    TO ROLE analyst;

-- Row-level filter: analysts only see their region
CREATE ROW_FILTER region_filter ON sales.orders
    FOR ROLE regional_analyst
    USING (region = CURRENT_USER_REGION());
```

#### 8.4 Trade-offs

```
LAKE FORMATION PROS:
   ✓ Centralized AWS-native governance
   ✓ Deep integration with Athena, Redshift, EMR, Glue
   ✓ Tag-based policies at scale

CONS:
   ✗ AWS lock-in (only AWS engines enforce)
   ✗ Policies don't apply to external engines (Databricks, Snowflake) reading S3 directly
   ✗ Learning curve

WHEN TO USE:
   ✓ All-AWS data lake with multi-tenant requirements
   ✗ Multi-cloud or multi-engine (use Polaris / Unity Catalog instead)
```

---

## PART III — COMPUTE

### 9. EMR and EMR Serverless (Managed Spark/Hadoop)

#### 9.1 What EMR Is

```
EMR (Elastic MapReduce) = managed Spark/Hadoop/Hive/Presto clusters.
   
   What AWS provides:
      • Pre-built AMIs with Spark, Hadoop, Hive, HBase, Flink
      • Auto-scaling
      • Spot instance support
      • Managed EMR Studio (notebooks)
      • Integration with S3, Glue, DynamoDB, Kinesis
   
   What you manage:
      • Cluster lifecycle (persistent or job-flow)
      • Configuration (Spark configs, HDFS settings)
      • Monitoring, logs
```

#### 9.2 EMR Flavors

```
EMR ON EC2:
   Traditional cluster of EC2 instances
   Most control; most ops burden
   Spot instances save 70%+ on compute

EMR SERVERLESS:
   Submit Spark/Hive jobs; AWS runs them
   No cluster management
   Pay per DPU-second (vs instance-hour)
   Better for intermittent / bursty workloads
   Latency: ~1-2 min to start (vs seconds for warm EMR on EC2)

EMR ON EKS:
   Spark on Kubernetes with EKS
   Share compute with other EKS workloads
   Good for teams already Kubernetes-committed

EMR ON OUTPOSTS:
   EMR on-prem (hybrid)
```

#### 9.3 EMR vs Databricks vs Glue

```
EMR vs DATABRICKS:
   EMR cheaper raw compute (no Databricks premium)
   Databricks has better UX, Delta Lake, MLflow, better optimizations
   EMR for cost-sensitive Spark; Databricks for productivity-sensitive

EMR vs GLUE:
   EMR = full Spark/Hadoop stack
   Glue = narrower (Spark + Python shell), serverless-only
   Glue cheaper for simple ETL; EMR for complex / large workloads
   Glue has nicer catalog integration out of box
```

#### 9.4 When to Use EMR

```
PICK EMR WHEN:
   ✓ Need full Spark/Hadoop/Hive stack
   ✓ Cost-sensitive at scale (Spot instances)
   ✓ Have Spark/Hadoop expertise in-house
   ✓ Need specific versions or custom configs

PICK EMR SERVERLESS WHEN:
   ✓ Intermittent Spark workloads
   ✓ Don't want cluster management
   ✓ Can tolerate 1-2 min cold start

PICK GLUE WHEN:
   ✓ Simpler ETL (not complex Spark)
   ✓ Want tight AWS integration
   ✓ Serverless + schema registry out of box
```

### 10. AWS Glue (ETL + Catalog)

#### 10.1 The Dual Nature of Glue

```
GLUE DATA CATALOG:
   Metastore for your lake tables
   Managed Hive Metastore replacement
   Used by Athena, EMR, Redshift Spectrum, Glue ETL

GLUE ETL:
   Serverless Spark + Python Shell
   Visual job authoring (Studio) or code
   Crawlers for schema discovery
```

#### 10.2 Glue Data Catalog

```
The default catalog for AWS lakes.

STRUCTURE:
   Databases
     → Tables (with schema, location, partitions, formats)
       → Partitions
   Connections (JDBC endpoints)
   Crawlers (schema discovery)

INTEGRATIONS:
   Athena uses it by default
   Redshift Spectrum queries external tables defined here
   EMR Hive metastore can be Glue Catalog
   Snowflake can read external Iceberg via Glue
   Databricks can mount it

OPEN QUESTION:
   Glue Catalog vs Polaris vs Unity Catalog in 2026?
   Glue is AWS-native but AWS-only.
   Polaris is open REST; works with Snowflake, Databricks, Trino.
   Many enterprises adopt Polaris and sync to Glue for AWS-native tools.
```

#### 10.3 Glue ETL Jobs

```python
# Minimal Glue ETL job (PySpark style)
import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Read from catalog
df = glueContext.create_dynamic_frame.from_catalog(
    database="raw", table_name="orders"
)

# Transform
df = df.filter(lambda r: r["status"] == "completed")

# Write to S3 + register in catalog
glueContext.write_dynamic_frame.from_options(
    frame=df,
    connection_type="s3",
    connection_options={"path": "s3://my-bucket/silver/orders/",
                        "partitionKeys": ["year", "month"]},
    format="parquet",
    format_options={"compression": "snappy"}
)

job.commit()
```

#### 10.4 Glue Pricing

```
GLUE ETL (Spark jobs):
   $0.44 per DPU-hour (G.1X: 4 vCPU, 16 GB)
   $0.44 per DPU-hour (G.2X: 8 vCPU, 32 GB)
   Billed per second with 1 min minimum

GLUE PYTHON SHELL JOBS:
   $0.44 per DPU-hour (1/16 DPU minimum = tiny jobs)

GLUE CATALOG:
   First 1M objects free
   $1 per 100K objects beyond
   Essentially free for most teams

GLUE CRAWLER:
   $0.44 per DPU-hour (same as ETL)
```

### 11. Lambda for Data Engineering

#### 11.1 Lambda's Role

```
LAMBDA = serverless function execution.
   Up to 15-min max runtime
   Up to 10 GB memory
   Pay per invocation + GB-second
   
For data engineering:
   ✓ Event-driven pipelines (S3 → process → S3)
   ✓ Lightweight transformations
   ✓ Glue code between services
   ✓ Scheduled small jobs (via EventBridge Scheduler)
   ✓ Serverless DuckDB queries (!)

NOT for:
   ✗ Long-running Spark jobs (15 min cap)
   ✗ Large memory needs (10 GB max)
   ✗ High-latency acceptable workloads (Fargate / EC2 better)
```

#### 11.2 Event-Driven Pattern

```python
# Lambda triggered by S3 PUT
def lambda_handler(event, context):
    import boto3, duckdb
    
    s3 = boto3.client('s3')
    for record in event['Records']:
        bucket = record['s3']['bucket']['name']
        key = record['s3']['object']['key']
        
        # Query the just-uploaded file with DuckDB
        con = duckdb.connect(':memory:')
        con.execute("INSTALL httpfs; LOAD httpfs;")
        result = con.execute(f"""
            SELECT customer_id, SUM(amount)
            FROM 's3://{bucket}/{key}'
            GROUP BY customer_id
        """).fetchall()
        
        # Write results somewhere
        output_key = f"aggregates/{key.replace('.parquet', '_agg.json')}"
        s3.put_object(
            Bucket='my-output-bucket',
            Key=output_key,
            Body=json.dumps(result)
        )
```

#### 11.3 The Serverless Analytics Pattern

```
Pattern: Lambda + DuckDB + S3 = ultra-cheap serverless analytics
   
   1. Data lands in S3 (via Firehose or batch)
   2. S3 event triggers Lambda
   3. Lambda runs DuckDB queries on the Parquet file(s)
   4. Writes results to another bucket / RDS / DynamoDB
   
   Cost: pennies per invocation
   Scale: thousands of concurrent Lambdas
```

### 12. Athena (Serverless Trino SQL)

#### 12.1 What Athena Is

```
Athena = serverless SQL on data in S3.
   Based on Trino / Presto (historically).
   
   Pricing: $5/TB scanned (compressed data)
   Or: Athena Capacity Reservations (per hour)
   
   What you do: write SQL
   What AWS does: parses, plans, runs, returns results
```

#### 12.2 Athena Workflow

```sql
-- Create a table in Glue Catalog pointing to S3
CREATE EXTERNAL TABLE sales.orders (
    order_id BIGINT,
    customer_id BIGINT,
    order_date DATE,
    amount DECIMAL(18,2)
)
PARTITIONED BY (year INT, month INT)
STORED AS PARQUET
LOCATION 's3://my-bucket/warehouse/sales/orders/';

-- Query
SELECT customer_id, SUM(amount)
FROM sales.orders
WHERE year = 2024 AND month = 3
GROUP BY customer_id;
```

#### 12.3 Athena Versions

```
ATHENA ENGINE V2:
   Based on Presto 0.217
   Being deprecated

ATHENA ENGINE V3:
   Based on Trino 411+
   Faster, more features
   Default for new workgroups

ATHENA FOR APACHE SPARK:
   Serverless PySpark via Athena
   Alternative to Glue for Python-centric teams

ATHENA FEDERATED QUERY:
   Query non-S3 sources (DynamoDB, RDS, Kafka, DocumentDB)
   Via Lambda connectors
```

#### 12.4 Cost Control

```
Athena bills per TB scanned. Watch for:
   
   ✗ SELECT * on huge tables (reads all columns)
   ✗ Queries without partition filters (scans everything)
   ✗ Non-Parquet formats (CSV reads much more data)
   ✗ Analysts running the same query many times (no cache by default)

MITIGATIONS:
   ✓ Partition tables appropriately
   ✓ Use Parquet with snappy/zstd compression
   ✓ Project only needed columns (never SELECT *)
   ✓ Use Athena Workgroups with per-user data scan limits
   ✓ Use materialized views for repeated aggregations
   ✓ CTAS (CREATE TABLE AS SELECT) to materialize intermediate results

PRICING MODELS:
   Pay-per-query (default): $5/TB scanned
   Provisioned capacity: fixed hourly cost for a reservation
   
   Crossover: ~200 TB scanned/month = reserved cheaper
```

> **🎯 Interview Angle:** Q: Your Athena bill went from $500/mo to $5,000/mo. Walk me through investigation.
> **Staff:** "First, check CloudTrail + Athena query logs — who's running what? Look at the top queries by bytes scanned. Common findings: (1) a new dashboard with SELECT * on partitioned tables without partition filters — fix with required filters or materialized views. (2) analysts running EXPLAIN or test queries on huge tables — education + workgroup limits. (3) non-Parquet source formats — convert CSV/JSON to Parquet. (4) Someone running a batch job as Athena when it should be Glue/EMR. Also check: did data volume grow 10x? If so, Athena may genuinely cost that much; consider provisioned capacity. The fix is almost always query hygiene + partition discipline, not switching engines."

---

## PART IV — INGESTION

### 13. Kinesis Data Streams and Firehose

#### 13.1 Kinesis Data Streams (KDS)

```
AWS's Kafka-equivalent (older but mature).

MODEL:
   Streams (like Kafka topics)
   Partitioned by SHARDS (vs partitions)
   Each shard: 1 MB/s or 1000 records/s ingress; 2 MB/s egress
   
   Scale by splitting / merging shards
   Default retention: 24h; up to 365 days extended retention

MODE:
   Provisioned (manage shard count)
   On-Demand (auto-scales; costs more per GB)

PRICING:
   Provisioned: shard-hour + PUT payload units
   On-Demand: GB-ingested + GB-egressed + shard-hour (adaptive)
```

#### 13.2 KDS vs Kafka

```
KINESIS:
   ✓ Managed; AWS handles everything
   ✓ Deep AWS integration (Firehose, Lambda, KDA)
   ✗ Less flexible than Kafka (no custom partitioner, limited consumer groups)
   ✗ 24h default retention
   ✗ AWS lock-in

KAFKA (MSK or Confluent):
   ✓ Open ecosystem
   ✓ Flexible config
   ✓ Longer retention + Tiered Storage
   ✗ More operational knobs

DECISION:
   If all-AWS and streaming is simple → KDS
   If multi-cloud / complex streaming → MSK or Confluent
   
   By 2026: MSK with KRaft + tiered storage is often the better choice
   even in AWS; Kinesis loses popularity for new deployments.
```

#### 13.3 Kinesis Firehose

```
KINESIS DATA FIREHOSE = managed delivery to S3/Redshift/OpenSearch/Snowflake.
   
   Not a streaming service itself; a sink for Kinesis / direct PUTs.
   
   Features:
      Buffering by size or time (e.g., 128 MB or 5 min, whichever first)
      Automatic Parquet conversion from JSON (with Glue schema)
      Partitioned writes (year/month/day/hour)
      Lambda transformations in-flight
      Error record capture to a separate bucket

   Pricing: per GB ingested
   
   USE CASES:
      • Firehose → S3 is the simplest ingestion to a lake
      • Firehose → Redshift via S3 (automatic COPY)
      • Firehose → OpenSearch for log streaming
```

#### 13.4 The Firehose + S3 Pattern

```
Simplest serverless ingestion:
   App → Firehose → S3 (Parquet, partitioned)
   
   Firehose handles:
      ✓ Buffering (reduces small-file problem)
      ✓ Compression (gzip, snappy, zstd)
      ✓ Format conversion (JSON → Parquet with Glue schema)
      ✓ Partitioning by timestamp
   
   You get Parquet files on S3 ready to query with Athena.
   For most "data from app to lake" patterns, this is the right choice.
```

### 14. Amazon MSK (Managed Kafka)

#### 14.1 What MSK Is

```
MSK = managed Apache Kafka.
   AWS handles cluster provisioning, patching, monitoring
   You still manage topics, partitions, consumers (standard Kafka ops)
   
   Flavors:
      MSK Provisioned: pick broker instance type + count
      MSK Serverless: AWS auto-scales based on throughput
      MSK with Tiered Storage (2023+): offloads cold data to S3
```

#### 14.2 Why MSK Over Kinesis

```
PICK MSK WHEN:
   ✓ Need Kafka ecosystem (Kafka Streams, Flink, Debezium)
   ✓ Want flexibility (custom partitioners, consumer groups)
   ✓ Longer retention (tiered storage to S3)
   ✓ Multi-cloud / portability intent
   ✓ Already Kafka-fluent team

PICK KINESIS WHEN:
   ✓ All-AWS commitment
   ✓ Simpler streaming (Firehose chain to S3/Redshift)
   ✓ Don't want operational Kafka burden
```

#### 14.3 MSK Pricing

```
MSK Provisioned:
   Broker-hour (varies by instance type)
   Storage per GB
   Data transfer in cross-AZ

MSK Serverless:
   Partition-hour
   GB in / GB out
   More expensive per unit but no cluster to manage

MSK with Tiered Storage:
   Hot storage: broker SSD
   Cold storage: S3 (much cheaper)
   Significant savings for long-retention topics
```

### 15. AWS DMS (CDC and Migration)

#### 15.1 What DMS Does

```
AWS DATABASE MIGRATION SERVICE:
   • One-time full-load migration between databases
   • Continuous CDC (Change Data Capture) from source to target
   
   Sources: Postgres, MySQL, SQL Server, Oracle, MongoDB, S3, Kafka, etc.
   Targets: Postgres, MySQL, Redshift, S3, Kinesis, DocumentDB, etc.
```

#### 15.2 Common DMS Patterns

```
PATTERN 1: RDBMS → S3 (CDC into lake)
   Postgres → DMS → S3 (Parquet) → Athena / Spark / etc.

PATTERN 2: RDBMS → Redshift (zero-ETL feel)
   Postgres → DMS → Redshift
   (Note: Zero-ETL integrations now exist for Aurora → Redshift)

PATTERN 3: Database migration
   Oracle → DMS (full load + CDC) → Postgres on RDS
```

#### 15.3 DMS vs Debezium

```
DMS:
   ✓ Managed; no cluster to run
   ✓ Deep AWS integration
   ✓ Supports many RDBMS sources
   ✗ Less flexible than Debezium
   ✗ Occasional data-type conversion gotchas
   ✗ Not open source

DEBEZIUM (on MSK Connect or EC2):
   ✓ Open source, flexible
   ✓ Native Kafka integration
   ✓ Richer configuration options
   ✗ Operational burden

FOR AWS-NATIVE SIMPLICITY: DMS
FOR FLEXIBILITY + KAFKA ECOSYSTEM: Debezium on MSK Connect

AWS also launched "Zero-ETL" integrations (2023-2024):
   Aurora → Redshift (bypasses DMS)
   RDS → Redshift
   DynamoDB → OpenSearch
   This is AWS trying to make DMS obsolete for same-cloud paths.
```

### 16. DataSync, AppFlow, Transfer Family

#### 16.1 AWS DataSync

```
File synchronization service.
   Sources: on-prem NFS/SMB, HDFS, S3, EFS, FSx
   Targets: S3, EFS, FSx, on-prem
   
   USE: migrate / sync large files between on-prem and AWS
   NOT for: real-time streaming; CDC
```

#### 16.2 AWS AppFlow

```
Managed SaaS integration.
   Sources: Salesforce, Slack, Google Analytics, SAP, etc.
   Targets: S3, Redshift, Snowflake, Salesforce
   
   USE: SaaS data pipelines without writing custom connectors
   COMPETES WITH: Fivetran, Stitch, Airbyte
   
   PRICING: pay per flow run + data transferred
   
   WHEN TO USE: all-AWS commitment, limited SaaS sources
   WHEN NOT: broad SaaS coverage needed (Fivetran has 400+)
```

#### 16.3 AWS Transfer Family

```
Managed SFTP, FTP, FTPS endpoints.
   Target: S3 or EFS.
   
   USE: receive data from external partners who still use FTP
```

---

## PART V — ANALYTICS

### 17. Amazon Redshift Deep Dive

#### 17.1 What Redshift Is

```
AMAZON REDSHIFT = AWS's columnar MPP data warehouse.
   Originally based on ParAccel (2013); significantly evolved since.
   
   Positioning: competitor to Snowflake, BigQuery, Databricks SQL.
   AWS-native with deep integration (S3, Lambda, SageMaker, QuickSight).
```

#### 17.2 Redshift Flavors

```
REDSHIFT PROVISIONED (classic):
   Pick node type + count
   RA3 nodes: separates compute from storage (via managed storage)
   DS2/DC2: older; being deprecated

REDSHIFT SERVERLESS (2022+):
   No cluster to manage
   Auto-scales based on workload
   Priced in RPUs (Redshift Processing Units)
   Good for intermittent / variable workloads
```

#### 17.3 Redshift Features

```
STORAGE:
   Columnar (compressed) on S3-backed managed storage (RA3)
   Automatic compression selection
   Auto-vacuum, auto-analyze (modern)

QUERY:
   MPP execution
   Materialized views
   Federated queries (query Postgres, MySQL directly from Redshift)
   Redshift Spectrum (query S3 external tables)
   Zero-ETL integrations (Aurora → Redshift auto-sync)

ML:
   Redshift ML: train SageMaker models via SQL

CONCURRENCY:
   Concurrency Scaling: automatic scaling for read bursts
   Query Priority: short-query acceleration

INTEGRATION:
   COPY from S3 (bulk load)
   UNLOAD to S3 (bulk export)
   Streaming ingestion from Kinesis/MSK
```

#### 17.4 Redshift vs Snowflake on AWS

```
REDSHIFT:
   ✓ Deeper AWS integration (IAM, VPC, Lake Formation)
   ✓ Zero-ETL integrations
   ✓ Cheaper for predictable 24/7 workloads (reserved pricing)
   ✓ Redshift Spectrum for S3 queries
   ✗ Historically more operational knobs (though improved)
   ✗ Less analyst-friendly UX than Snowflake

SNOWFLAKE (on AWS):
   ✓ Best-in-class analyst UX
   ✓ Multi-cloud portable
   ✓ Zero-copy clones, deeper time travel
   ✓ Snowflake Data Cloud / Marketplace
   ✗ More expensive per compute hour (typically)
   ✗ Separate contract from AWS

AT STAFF LEVEL:
   For AWS-committed shops with ops bandwidth → Redshift
   For analyst-heavy, multi-cloud, or want simplest UX → Snowflake
```

### 18. OpenSearch and Serverless OpenSearch

#### 18.1 What OpenSearch Is

```
OPENSEARCH = AWS's fork of Elasticsearch (post-license dispute 2021).
   
   Services:
      Amazon OpenSearch Service (managed clusters)
      Amazon OpenSearch Serverless (serverless indices)
   
   USES:
      Log analytics (CloudWatch Logs → OpenSearch)
      Full-text search
      Real-time analytics / dashboards
      Metrics / observability
```

#### 18.2 For Data Engineering

```
PATTERN 1: Observability stack
   Logs → Firehose → OpenSearch → OpenSearch Dashboards (Kibana fork)

PATTERN 2: Real-time analytics
   Events → Kinesis / MSK → Flink → OpenSearch → Grafana

PATTERN 3: Full-text search backend
   Catalog of products / docs → OpenSearch → application API

ALTERNATIVES:
   ClickHouse: often beats OpenSearch for analytics-heavy workloads
   OpenSearch: better for search + light analytics
```

### 19. Timestream (Time Series)

```
AMAZON TIMESTREAM = serverless time-series DB.
   
   Features:
      Auto-tiering (memory → magnetic → S3)
      SQL-like query language
      Native scheduled queries
      Integration with Grafana, QuickSight
   
   USE:
      IoT metrics
      Application monitoring
      Industrial sensor data
   
   ALTERNATIVES: InfluxDB, TimescaleDB, Prometheus (self-managed)
   
   WHEN TO USE: already AWS-committed and want managed time-series.
   WHEN NOT: open source preference; or very high ingest rate.
```

---

## PART VI — ORCHESTRATION

### 20. Step Functions

#### 20.1 What Step Functions Is

```
AWS STEP FUNCTIONS = managed state machine for workflows.
   
   Define workflows as JSON (Amazon States Language).
   Steps are tasks (Lambda, Glue, EMR, Batch, SageMaker, etc.)
   
   Features:
      Visual designer
      Automatic retries + error handling
      Parallel/choice/map states
      Standard vs Express workflows (short vs long-running)
```

#### 20.2 Example Workflow

```json
{
  "Comment": "Nightly ETL pipeline",
  "StartAt": "ExtractData",
  "States": {
    "ExtractData": {
      "Type": "Task",
      "Resource": "arn:aws:states:::glue:startJobRun.sync",
      "Parameters": {"JobName": "extract_sales"},
      "Next": "TransformData"
    },
    "TransformData": {
      "Type": "Task",
      "Resource": "arn:aws:states:::glue:startJobRun.sync",
      "Parameters": {"JobName": "transform_sales"},
      "Next": "LoadToRedshift"
    },
    "LoadToRedshift": {
      "Type": "Task",
      "Resource": "arn:aws:states:::lambda:invoke",
      "Parameters": {
        "FunctionName": "arn:aws:lambda:...:load_redshift"
      },
      "End": true
    }
  }
}
```

#### 20.3 Step Functions vs Airflow

```
STEP FUNCTIONS:
   ✓ Serverless; nothing to manage
   ✓ Cheap for small workloads
   ✓ Deep AWS integration
   ✗ JSON state machine (less expressive than Python)
   ✗ Harder to version / test / review

AIRFLOW (MWAA):
   ✓ Python DAGs (expressive, testable)
   ✓ Huge connector ecosystem
   ✓ Standard across clouds
   ✗ Managed cluster to pay for
   ✗ More operational overhead

PICK STEP FUNCTIONS WHEN:
   ✓ Short, event-driven workflows
   ✓ AWS-native pipelines
   ✓ Smaller team preferring serverless

PICK AIRFLOW WHEN:
   ✓ Complex DAGs with many dependencies
   ✓ Already have Airflow expertise
   ✓ Multi-cloud orchestration
```

### 21. MWAA (Managed Airflow)

```
AMAZON MWAA = managed Apache Airflow.
   AWS runs the scheduler, webserver, workers.
   You write DAGs in Python; store in S3.
   
   Pricing: per environment-hour + per worker-hour
   
   STANDARD AIRFLOW PATTERNS APPLY:
      Use operators to call AWS services (GlueOperator, EMROperator, etc.)
      Deferrable operators for long-running tasks
      Dynamic DAG generation
      Sensors + wait patterns
   
   WHEN TO USE:
      Complex multi-step pipelines
      Teams already Airflow-fluent
      Multi-service orchestration
```

### 22. EventBridge

#### 22.1 What EventBridge Is

```
AMAZON EVENTBRIDGE = managed event bus.
   
   Capabilities:
      Receive events from AWS services, SaaS apps, custom sources
      Route via rules to targets (Lambda, SQS, Step Functions, Kinesis, etc.)
      Scheduled rules (replaces CloudWatch Scheduled Events)
      Schema Registry (define/discover event schemas)
      Pipes (stream between sources and targets with filtering/transform)
```

#### 22.2 For Data Engineering

```
EVENT-DRIVEN PATTERNS:
   S3 upload → EventBridge → Lambda → process + load
   DynamoDB change → EventBridge → Firehose → S3
   SaaS webhook → EventBridge → Step Functions → ETL pipeline

SCHEDULED PATTERNS:
   EventBridge Scheduler → trigger pipelines on cron
   Replaces older CloudWatch Scheduled Events
   More features: time zones, flexible schedules

PIPES (newer):
   Point-to-point event streaming
   Source (Kinesis, SQS, DynamoDB Streams) → optional filter + transform → target
   Simpler than Step Functions for linear flows
```

### 23. Choosing Between Them

```
STEP FUNCTIONS   — stateful, visual workflows with branching logic
EventBridge      — event-driven routing + scheduling
MWAA (Airflow)   — complex DAGs with Python expressiveness
Lambda + SQS     — simple event-driven glue

TYPICAL PATTERN:
   EventBridge (scheduler) → Step Functions (workflow) → Glue/EMR (compute)
   
   OR
   
   EventBridge (scheduler) → MWAA (orchestrates everything)

Pick based on team expertise + workflow complexity.
Avoid using all three for one pipeline — pick one as primary.
```

---

## PART VII — ML AND GOVERNANCE

### 24. SageMaker for the Data Engineer

#### 24.1 What SageMaker Is

```
SAGEMAKER = AWS's end-to-end ML platform.
   
   Components:
      SageMaker Studio: IDE for ML
      SageMaker Training: managed training jobs
      SageMaker Processing: data prep jobs
      SageMaker Pipelines: ML workflows (like Airflow for ML)
      SageMaker Model Registry
      SageMaker Endpoints: real-time inference
      SageMaker Batch Transform: batch inference
      SageMaker Feature Store
      SageMaker Monitor: drift detection
      SageMaker Clarify: fairness / explainability
```

#### 24.2 Data Engineer's SageMaker Touch Points

```
AS A DATA ENGINEER, YOU'LL TOUCH:
   ✓ Feature Store (online + offline features)
   ✓ Pipelines (ML workflow orchestration)
   ✓ Processing jobs (data prep)
   ✓ Model Registry (versioning deployed models)
   ✓ Endpoints (if you're responsible for serving infra)

LEAVE TO ML ENGINEERS:
   ✗ Training algorithm selection
   ✗ Hyperparameter tuning
   ✗ Model-architecture decisions

STAFF-LEVEL FRAMING:
   Position yourself as the owner of the data+infra side of ML.
   Feature stores, pipeline orchestration, monitoring infrastructure.
   You don't need to train models to be valuable in ML platforms.
```

#### 24.3 SageMaker Feature Store

```
SAGEMAKER FEATURE STORE:
   Online store: DynamoDB-backed; sub-10ms lookups
   Offline store: S3 Parquet; for training queries
   Feature groups: schema + metadata
   Time travel for point-in-time queries
   
   Pros: AWS-native; integrates with other SageMaker components
   Cons: lock-in; less flexible than Feast or Tecton
```

### 25. Lake Formation Governance Patterns

#### 25.1 Core Governance Pattern

```
Lake Formation + Glue Data Catalog + AWS engines:

   USER + ROLE
       ↓
   LAKE FORMATION (grants, filters, tags)
       ↓
   GLUE DATA CATALOG (metastore)
       ↓
   ATHENA / REDSHIFT / EMR / GLUE (query execution)
       ↓
   S3 (data)

Lake Formation enforces permissions regardless of which AWS engine queries.
External engines (Databricks, Snowflake) must manage permissions separately.
```

#### 25.2 LF-Tags for Scale

```
LF-TAGS = tag-based access control.
   Define tags like Classification=PII, Owner=marketing, Env=prod
   Attach to databases, tables, columns
   Grant permissions by tag (not by name)

Example:
   GRANT SELECT on tables TAGGED Classification=NonPII TO ROLE analyst;
   
   New tables automatically inherit permissions via tags.
   No per-table GRANTs needed.
   
   Essential for enterprises with thousands of tables.
```

### 26. CloudWatch, CloudTrail, and Cost Observability

#### 26.1 CloudWatch

```
METRICS: service-emitted + custom metrics
LOGS: structured application + service logs
ALARMS: notifications on thresholds
DASHBOARDS: visualize metrics
INSIGHTS: log query language (LogsInsights)
SYNTHETICS: canary HTTP checks
RUM: real-user monitoring (web apps)
```

#### 26.2 CloudTrail

```
AUDIT LOG of all API calls in your AWS account.

Features:
   S3 Data Events: track object-level access
   Lambda Data Events: track invocations
   Insights: anomaly detection on API patterns

For data engineering:
   Audit: who deleted / modified data
   Compliance: SOC 2, HIPAA audit evidence
   Security: detect suspicious access patterns

ENABLE: trail with S3 destination + CloudWatch Logs for alerting
```

#### 26.3 Cost Observability

```
AWS COST EXPLORER: historical trends, filters by service/tag/account
AWS BUDGETS: alerts on forecast > threshold; action triggers
AWS COST ANOMALY DETECTION: ML-based spike alerts
CUR (COST AND USAGE REPORT): raw CSV/Parquet of every line item

Advanced: ingest CUR into Athena / QuickSight for custom dashboards
Third-party: Vantage, CloudZero, Datadog for FinOps
```

---

## PART VIII — INTERVIEW PRACTICE

### 27. The Staff-Level AWS Data Interview

#### 27.1 Common Questions

```
1. "Design a data lake on AWS."
2. "Explain the trade-offs between Athena, Redshift, and EMR."
3. "How would you CDC from Postgres to a lake on AWS?"
4. "Compare Kinesis vs MSK — when do you pick each?"
5. "Your Athena bill is $20K/month. Walk me through diagnosis."
6. "Design an ML pipeline on AWS."
7. "How does Lake Formation enable multi-tenant lakes?"
8. "Explain the trade-offs of serverless (Glue/Athena) vs provisioned (EMR/Redshift)."
9. "What's your orchestration choice: Step Functions, MWAA, or EventBridge?"
10. "Zero-ETL integrations — when do they help, when don't they?"
```

#### 27.2 Worked Example

```
Q: Design the AWS data platform for a 100-person FinTech startup.
   100 TB current, 2x growth per year.
   CDC from Postgres (operational), events from app, 20 analysts, 
   ML team of 5. Budget $30K/month.

STAFF ANSWER:

ARCHITECTURE:
   • Storage: S3 + Iceberg via Glue Data Catalog
   • Ingest:
      - Postgres CDC: AWS DMS → Kinesis Firehose → S3 (Bronze Iceberg)
      - App events: KPL → Kinesis Data Streams → Firehose → S3
      - SaaS: Fivetran → S3 (not AWS-native but worth it; AppFlow has gaps)
   • Processing:
      - Glue ETL for simple transforms (Bronze → Silver)
      - EMR Serverless for heavier Spark jobs
      - dbt-athena or dbt-redshift for modeling (Silver → Gold)
   • Serving:
      - Athena for ad-hoc analytics (serverless)
      - Redshift Serverless for BI dashboards (QuickSight / Looker)
      - OpenSearch for log/event dashboards
   • ML:
      - SageMaker Feature Store (offline on S3, online on DynamoDB)
      - SageMaker Pipelines for training
      - SageMaker Endpoints for serving
   • Orchestration:
      - MWAA for complex DAGs (nightly ETL, feature pipelines)
      - EventBridge for schedules + S3-triggered Lambda jobs
   • Governance:
      - Lake Formation + LF-Tags for role-based access
      - CloudTrail audit log → S3 → Athena for queries
   • Monitoring:
      - CloudWatch dashboards per pipeline
      - Datadog for app-level observability

COST BREAKDOWN (estimates):
   S3 (100 TB with tiering):       ~$1,500
   Kinesis/MSK (10K events/sec):    ~$2,000
   DMS CDC:                          ~$500
   Glue ETL + Catalog:               ~$2,000
   EMR Serverless (nightly):         ~$3,000
   Redshift Serverless (BI):         ~$6,000
   Athena ($5/TB, ~2 TB/day):        ~$300
   SageMaker (training + endpoints): ~$5,000
   MWAA:                             ~$500
   OpenSearch (log analytics):       ~$3,000
   Fivetran (SaaS sources):          ~$2,000
   Networking + egress:              ~$1,000
   Observability (CW + Datadog):     ~$2,000
   Buffer:                           ~$1,000
   Total: ~$30,000/month

SCALE PATH:
   At 500 TB: migrate Bronze to Standard-IA; introduce EMR provisioned for cost
   At 1 PB: consider Snowflake external Iceberg for analyst UX
   
OFF-RAMPS:
   • Iceberg + Glue is portable (Polaris swap possible)
   • Databricks or Snowflake can be added as alternate compute later
   • Not deeply locked into Redshift (Spectrum enables external storage)
```

### 28. War Stories

> **🔥 Production Scenario — The Egress Explosion.** A team hosted their ML training in us-east-1 but their training data in us-west-2 (legacy S3 bucket). Every training run (daily) transferred 500 GB cross-region = $45/day in transfer = $16K/year. Fix: replicated the training dataset to us-east-1 once, then kept it synced via S3 Replication. Transfer cost dropped to pennies; replication cost ~$200/month. Lesson: always check region of data vs region of compute; cross-region egress is a slow money leak.

> **🔥 Production Scenario — The NAT Gateway Surprise.** A team's Lambda functions in a private VPC called S3 via the public endpoint. All S3 traffic went through NAT Gateway at $0.045/GB in data processing. 20 TB/month → $900/month. Added S3 Gateway Endpoint (free): $900/month saved overnight. Lesson: Gateway endpoints for S3/DynamoDB are free; always use them from private subnets.

> **🔥 Production Scenario — The DMS Replication Slot.** A team's DMS task from a Postgres replica ran continuously. The replication slot kept WAL files from being cleaned up. The replica's WAL grew to 2 TB, filling the disk. Postgres crashed. Fix: added monitoring on `pg_replication_slots` lag; alerted on growth > 10 GB. Lesson: DMS + Postgres requires watching replication slot health; disk-full crashes are a classic failure mode.

> **🔥 Production Scenario — The Athena Workgroup Save.** An analytics team's Athena bill grew 300% in a month. Investigation: one dashboard did `SELECT *` on a 100 TB table every 5 minutes. Fix: workgroup with per-query bytes-scanned limit of 100 GB; the bad dashboard immediately failed; team fixed the query. Lesson: Athena workgroups with scan limits catch runaway queries before they bankrupt you.

> **🔥 Production Scenario — The MWAA Upgrade Pain.** A team upgraded MWAA from 2.5 to 2.7 (minor version). Several DAGs broke due to deprecated operator signatures. 2-hour outage of nightly pipelines. Fix: test environment matching prod; version-pinned requirements; staged rollout. Lesson: managed services still require version management discipline.

### 29. Cheat Sheet &amp; Mental Models

#### 29.1 The One-Minute Pitch

```
AWS data engineering = pick the minimum coherent set of services for your workload.

CORE 10 SERVICES:
   S3, Glue (ETL+Catalog), EMR Serverless, Athena, Redshift,
   Kinesis Firehose, MSK, DMS, Step Functions / MWAA, Lake Formation

ADD AS NEEDED:
   SageMaker (ML), OpenSearch (search/logs), EventBridge (events),
   QuickSight (BI), Lambda (glue code)

PATTERNS:
   Serverless lake:  S3 → Firehose → Athena/Glue → QuickSight
   Lakehouse:        S3 Iceberg → EMR/Databricks → Snowflake/Athena
   Streaming:        Kinesis/MSK → KDA (Flink) → OpenSearch/ClickHouse
   Classical DW:     DMS → S3 → Redshift
```

#### 29.2 The Four Lenses — Compressed

```
CONCEPT
   AWS data = 20 services that compose into recognizable patterns.
   S3 is the foundation; everything else serves specific jobs.

MECHANICS
   Serverless (Athena, Glue, Lambda) vs Provisioned (EMR, Redshift, OpenSearch).
   Managed (MSK, Redshift) vs Serverless-of-managed (Serverless variants).
   Compose via IAM roles, VPC endpoints, EventBridge events.

TRADE-OFFS
   Pros: deepest ecosystem, mature, cost-effective at scale, best managed services.
   Cons: AWS lock-in, learning curve, egress costs, 50+ services to navigate.

INTERVIEW
   Know: 10 core services, their trade-offs, when each is wrong.
   Defend: your pattern choice (serverless vs provisioned vs lakehouse).
   Design: minimum coherent stack for a given workload + budget.
```

#### 29.3 Essential Service Decision Guide

```
NEED                           SERVICE
================================================
Object storage                 S3
Managed Iceberg                S3 Tables
Metastore / catalog            Glue Data Catalog (or Polaris)
Serverless SQL                 Athena
Columnar warehouse             Redshift
Managed Spark                  EMR (provisioned) or EMR Serverless
Serverless ETL                 Glue ETL
Simple ingest to S3            Kinesis Firehose
Streaming with Kafka API       MSK (over Kinesis for flexibility)
Simple streaming               Kinesis Data Streams
CDC from RDBMS                 DMS (or Debezium on MSK Connect)
SaaS source connectors         Fivetran > AppFlow (usually)
Orchestration (visual, AWS)    Step Functions
Orchestration (Airflow)        MWAA
Event bus + schedules          EventBridge
Serverless functions           Lambda
Containerized compute          Fargate
ML platform                    SageMaker
Governance / RBAC on lake      Lake Formation
Search / log analytics         OpenSearch (or ClickHouse)
Time series DB                 Timestream (or InfluxDB)
BI                             QuickSight (or Tableau / Looker)
```

#### 29.4 Mental Model

```
Think of AWS data as:
   • A buffet of services; most teams need 10 of them
   • S3 as the gravitational center — everything orbits it
   • Serverless-first for small; provisioned at scale
   • Deep AWS lock-in in exchange for best integration
   • A 10-year commitment; design with migration costs in mind

When to reach for AWS data services:
   • AWS-committed shop
   • Need deep integration with other AWS (SageMaker, Lambda, etc.)
   • Enterprise security/compliance requires AWS-native

When to reach past them:
   • Multi-cloud mandate
   • Open-format portability is critical
   • Specific tool is best-in-class elsewhere (Snowflake, Databricks)
```

> **🧠 Principal Mindset:** The AWS data interview isn't about naming services; it's about defending choices. "I'd use S3 + Glue + Athena" is senior-level. "I'd use S3 + Glue Catalog + Athena because our workload is 100 TB with sporadic analytics, the serverless cost model fits our usage shape, and we're AWS-committed; if we grew to 1 PB with continuous queries I'd introduce Redshift Serverless for predictability" is staff-level. The reasoning around trade-offs is the signal.

---

[← Back to main](./README.md)
