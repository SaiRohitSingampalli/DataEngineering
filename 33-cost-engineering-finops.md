# Topic 33 — Cost Engineering & FinOps for Data

> Standalone deep-dive · Showback, chargeback, per-platform optimization

[← Back to main](./README.md)

---

## How to use this document

Cost engineering is where staff engineers earn their title in the eyes of finance leadership. Data stacks are expensive and getting more so. Chapter 33 is the FinOps playbook: showback/chargeback models, per-platform patterns, open-source tools, and the budgeting conversations.

> **🧠 Principal Mindset:** "Our data cost grew 60% last year" is the single most common CFO conversation about data in 2026. The staff engineer either walks in with a tiered cost story + reduction plan, or walks out without a budget. This chapter is for that walk-in.

## Table of Contents

**PART I — FOUNDATIONS**
- 1. Why FinOps for Data
- 2. The Cost Attribution Problem
- 3. The Three Phases of FinOps

**PART II — ATTRIBUTION MODELS**
- 4. Showback
- 5. Chargeback
- 6. Hybrid Models
- 7. Tagging Strategies

**PART III — PER-PLATFORM COSTS**
- 8. Snowflake Cost Patterns
- 9. Databricks Cost Patterns
- 10. BigQuery Cost Patterns
- 11. Redshift Cost Patterns
- 12. Spark on K8s Cost Patterns

**PART IV — STORAGE ECONOMICS**
- 13. Storage Tiering (S3/GCS/ADLS)
- 14. Format Economics
- 15. Compaction and Deletion

**PART V — NETWORK AND EGRESS**
- 16. Egress Pricing
- 17. Cross-Region Patterns
- 18. VPC Endpoints and PrivateLink

**PART VI — TOOLS**
- 19. Native Cost Dashboards
- 20. Open-Source Tools (Kubecost, OpenCost, SELECT)
- 21. Commercial Tools (Finout, Vantage, CloudZero)

**PART VII — GOVERNANCE**
- 22. Budgeting + Forecasting
- 23. Cost Review Cadence
- 24. The Cost Culture

**PART VIII — INTERVIEW PRACTICE**
- 25. The Staff-Level FinOps Interview
- 26. Worked Example
- 27. War Stories
- 28. Cheat Sheet &amp; Mental Models

---

## PART I — FOUNDATIONS

### 1. Why FinOps for Data

```
DATA COSTS compound:
   Compute (warehouses, Spark, Flink)
   Storage (object storage, warehouse storage)
   Egress (cross-region, cross-cloud, to internet)
   Observability (logs, metrics, traces)
   Vendor subscriptions (Snowflake, Databricks, Monte Carlo, Fivetran, etc.)
   GenAI (LLM API, embeddings, vector DBs)
   
Typical data stack cost at mid-size (100-500 engineers): $1-10M/year.

GROWTH DRIVERS (2024-2026):
   GenAI workloads adding 30-100% to spend
   Streaming expanding ingestion
   Observability platforms scaling with data
   
UNCONTROLLED growth = CFO cuts budget blind, team scrambles.
CONTROLLED growth = tiered levers, measurable value, defensible asks.
```

### 2. The Cost Attribution Problem

```
MULTI-TENANT SHARED PLATFORMS make attribution hard:
   
   Snowflake WAREHOUSES used by many teams
   Databricks CLUSTERS shared across jobs
   S3 BUCKETS with many prefixes from many teams
   Kafka TOPICS owned by many services

Native tools help but have gaps:
   Snowflake tags → warehouse-level attribution; query-level needs work
   Databricks system tables → granular but requires engineering
   AWS Cost Explorer → tag-based, imperfect for shared resources

STAFF SIGNAL: cost attribution is an ENGINEERING INVESTMENT. 
   Instrumentation (tagging, labels, query metadata) must be designed in.
```

### 3. The Three Phases of FinOps

```
FINOPS FOUNDATION's three-phase model:
   
   INFORM: visibility (who spent what)
      Dashboards; cost allocation; benchmarks
   
   OPTIMIZE: reduce waste (actionable savings)
      Rightsizing, reserved instances, scheduling, dead compute
   
   OPERATE: continuous governance (process)
      Reviews, budgets, forecasting, policy enforcement

Most orgs are stuck in INFORM. Moving to OPERATE is the staff mandate.
```

---

## PART II — ATTRIBUTION MODELS

### 4. Showback

```
SHOWBACK: show teams their costs; don't bill them.
   
   PROS:
      Low friction (no money changes hands)
      Builds awareness
      Baseline for later chargeback
   
   CONS:
      Weak incentive (seen cost ≠ felt cost)
      Easy to ignore
   
   PATTERN:
      Monthly email: "your team used $45K in Snowflake last month, 
         up 20% from previous"
      Dashboards per team
      Benchmarks (how do you compare to similar teams?)
```

### 5. Chargeback

```
CHARGEBACK: bill teams actual costs; impact their budgets.
   
   PROS:
      Strong incentive (felt in the budget)
      Forces optimization
      Aligns technical + financial decisions
   
   CONS:
      Operational overhead (billing accuracy, disputes)
      Shared resource allocation politically contentious
      Can create perverse incentives (hoard private resources)
   
   REQUIREMENTS:
      Accurate attribution (tagging, labels)
      Finance team buy-in
      Dispute resolution process

Enterprise orgs usually land here eventually; startups rarely.
```

### 6. Hybrid Models

```
HYBRID: showback for most; chargeback for the top spend categories.
   
   Example:
      Snowflake + Databricks: full chargeback (high spend, easy attribution)
      S3 storage: showback (small vs compute; attribution harder)
      Observability tools: platform cost center (shared service)
      LLM Gateway: chargeback by team via gateway attribution

STAFF SIGNAL: hybrid is the DEFAULT at mid-size. Argue which categories 
   warrant chargeback based on spend concentration + attribution clarity.
```

### 7. Tagging Strategies

```
TAG DESIGN:
   
   TEAM / COST-CENTER: who OWNS this
   ENVIRONMENT: dev / staging / prod
   PROJECT / FEATURE: what this spend is for
   WORKLOAD TYPE: etl / ml / adhoc / bi
   DATA TIER: tier-1 / tier-2 / tier-3

ENFORCEMENT:
   Policy-as-code (OPA/Kyverno for K8s, Config Rules for AWS)
   Untagged resources flagged / auto-deleted after N days
   New resources require tags at creation

PROPAGATION:
   Spark job tag → cluster tag → S3 PUT request header → S3 bucket cost allocation
   Snowflake tag on warehouse → cost query → team dashboard

Tags are the FOUNDATION of attribution; invest early.
```

---

## PART III — PER-PLATFORM COSTS

### 8. Snowflake Cost Patterns

See 21-snowflake-platform for depth.

```
TOP SNOWFLAKE SPEND DRIVERS:
   Virtual warehouses: usually 60-80% of spend
   Serverless (Snowpipe, Tasks, DT): growing with modern pipelines
   Cortex AI: growing with GenAI adoption
   Storage: often <10% of total

TOP SAVINGS LEVERS:
   AUTO_SUSPEND to 60 seconds (default is 10 minutes)
   Right-size warehouses (benchmark; don't just pick XL)
   Multi-cluster vs single: concurrency vs peak
   Query acceleration service only when it pays
   Materialized views when expensive aggregations consumed repeatedly
   Query tagging + ACCOUNT_USAGE for per-team attribution
   Resource monitors to cap monthly spend per warehouse
   Clustering only when the benefit > reclustering cost
   Caching: result cache free; warehouse disk cache mitigates clearance
```

### 9. Databricks Cost Patterns

See 22-databricks-platform for depth.

```
TOP DATABRICKS SPEND DRIVERS:
   Interactive (all-purpose) clusters: often 50%+ of spend
   DBSQL warehouses: growing with BI adoption
   Jobs clusters: production workloads
   Model Serving: with ML adoption
   Mosaic AI (LLM gateway, Vector Search, Agent): growing fast
   Cloud compute (underlying VM cost): separate from DBU

TOP SAVINGS LEVERS:
   Jobs clusters, not all-purpose, for production (DBU rate lower)
   Autoscale with sensible min
   Spot instances with on-demand driver + retries
   Serverless for bursty workloads
   Photon only when workload benefits
   Predictive Optimization (auto OPTIMIZE + VACUUM) on Delta tables
   AI Gateway to govern LLM spend
   System tables for cost attribution (system.billing.usage)
   Cluster termination: aggressive timeouts for all-purpose
```

### 10. BigQuery Cost Patterns

See 19-gcp-data-engineering for depth.

```
TWO BIGQUERY PRICING MODELS:
   
   ON-DEMAND: $6.25 per TB scanned
      Pros: no commitment; variable spend
      Cons: expensive at scale; cost unpredictable
   
   EDITIONS (CAPACITY-BASED, 2023+):
      Standard / Enterprise / Enterprise Plus
      Pay per slot per hour
      Pros: predictable; autoscaling
      Cons: commitment required

TOP SAVINGS LEVERS:
   Partitioning + clustering on query predicates (reduces scan)
   Materialized views on expensive aggregates
   BI Engine for interactive dashboards
   Reservations for predictable workloads
   Flat-rate / editions above $2K/month in on-demand
   Query cache (free)
   Slot usage monitoring via INFORMATION_SCHEMA
```

### 11. Redshift Cost Patterns

See 17-aws-data-engineering for depth.

```
REDSHIFT PRICING:
   Provisioned: pay per node per hour (RA3, DC2 types)
   Serverless: pay per RPU-hour
   Reserved: 1-3 year commits for discount

TOP SAVINGS LEVERS:
   Serverless for variable workloads
   Reserved for predictable baseline
   Managed storage (RA3) separates storage + compute cost
   Workload Management (WLM): isolate expensive from BI queries
   Materialized views for repeated aggregations
   Concurrency scaling (free credits earned via sustained usage)
   DISTKEY + SORTKEY tuning (scan reduction)
```

### 12. Spark on K8s Cost Patterns

See 28-kubernetes-for-data-engineering for depth.

```
TOP SPARK-ON-K8S SPEND DRIVERS:
   Node compute cost (EC2/GCE)
   Idle nodes between jobs
   Over-provisioned executors (requested memory not used)

TOP SAVINGS LEVERS:
   Spot/preemptible for executors (50-70% save for batch)
   Karpenter for right-sized nodes + fast scale-up
   Cluster Autoscaler for scale-down (critical)
   Resource request tuning (actual used, not peak estimate)
   Jobs on ephemeral node pools (not shared with services)
   Shuffle local SSDs (reduce S3 roundtrips on shuffle)
   Spark 3.5+ dynamic allocation with shuffle tracking
   Idle namespace detection + reclaim
```

---

## PART IV — STORAGE ECONOMICS

### 13. Storage Tiering (S3/GCS/ADLS)

```
S3 STORAGE CLASSES (2026):
   S3 Standard:               $0.023/GB  (hot)
   S3 Intelligent-Tiering:    auto-tier based on access
   S3 Standard-IA:            $0.0125/GB (infrequent)
   S3 One Zone-IA:            $0.01/GB   (infrequent, 1 AZ)
   S3 Glacier Instant:        $0.004/GB  (archival, instant retrieval)
   S3 Glacier Flexible:       $0.0036/GB (archival, mins-hours)
   S3 Glacier Deep Archive:   $0.00099/GB (archival, hours)

TIERING PATTERN:
   Hot (last 30 days): Standard
   Warm (30-90 days): Intelligent Tiering
   Cold (90+ days): Glacier Flexible or Deep Archive
   Archival (regulatory): Deep Archive

LIFECYCLE POLICIES:
   S3 lifecycle rules automate tiering
   But: retrieval cost + delay for deeper tiers; verify use case

STAFF SIGNAL: storage is usually <20% of data costs; TIERING can save 50%
   on storage specifically. Not always worth the ops burden.
```

### 14. Format Economics

```
PARQUET + GOOD PARTITIONING:
   Compression ratio: 3-10x typical
   Query cost: scan less data (partition pruning, Parquet statistics)

ICEBERG MANAGED OVERHEAD:
   Manifests + snapshots: 2-5% of data size
   Trade-off: more metadata, better pruning
   
COMPACTION cost:
   Small files inflate metadata + slow queries
   Compaction has compute cost (OPTIMIZE)
   Target: 128-512 MB files for most workloads

Z-ORDERING / CLUSTERING cost:
   Reorganization has compute cost
   Benefit: scan reduction on target columns
   
   STAFF ANSWER: measure before + after; don't blindly cluster.
```

### 15. Compaction and Deletion

```
COMPACTION:
   For streaming / CDC-heavy tables: small files accumulate
   OPTIMIZE / compaction jobs rewrite into fewer, larger files
   Trade-off: compaction compute vs query speed on compacted data
   
   Databricks Predictive Optimization: auto-compacts based on usage
   
VACUUM / FILE RETENTION:
   Delta / Iceberg keep old file versions for time travel
   Retention default: 7 days; configurable
   Longer retention = more storage; shorter = less recovery options
   
   STAFF SIGNAL: tier retention by TABLE CRITICALITY:
      Tier-1: 30-day retention (restore window)
      Tier-2: 7 days
      Tier-3: 1 day
```

---

## PART V — NETWORK AND EGRESS

### 16. Egress Pricing

```
EGRESS = DATA LEAVING THE CLOUD:
   AWS: ~$0.05-0.09/GB (varies by destination)
   GCP: similar
   Azure: similar
   
   FREE: traffic within same region, between services in same cloud

HIDDEN EGRESS COSTS:
   Cross-AZ transfer (within region): $0.01/GB (both directions)
   NAT Gateway: $0.045/GB processed + hourly
   Transit Gateway: per-GB processed

STAFF SIGNAL: egress charges are OFTEN THE SURPRISE in cloud bills.
   Multi-region apps + multi-cloud = massive egress bills.
```

### 17. Cross-Region Patterns

```
DESIGN PRINCIPLES:
   Compute data NEAR the data (not vice versa)
   Replication: expensive but may be required (DR, latency, residency)
   One-way: cheaper than active-active

PATTERNS:
   SNOWFLAKE: cross-region replication built-in; pays Snowflake egress + storage
   DATABRICKS: cross-region clone/share patterns
   S3: Cross-Region Replication (CRR) adds storage + egress cost
   
   DELTA SHARING / SNOWFLAKE SHARING: read-only cross-region; no copies

AVOID:
   Multi-region joins at query time
   Returning large result sets across regions
```

### 18. VPC Endpoints and PrivateLink

```
VPC ENDPOINTS (AWS):
   Private connectivity to AWS services (S3, DynamoDB, etc.)
   Avoids NAT Gateway + internet egress charges
   
PRIVATELINK:
   Private connectivity to third-party services (Snowflake, Databricks)
   Reduces network cost + improves security

PATTERN:
   S3 Gateway VPC Endpoint: FREE; routes S3 traffic without NAT
   Snowflake PrivateLink: paid per endpoint but reduces egress to Snowflake
   
STAFF SIGNAL: VPC Endpoints are the SINGLE EASIEST network cost optimization
   in AWS data stacks. Deploy them day one.
```

---

## PART VI — TOOLS

### 19. Native Cost Dashboards

```
AWS COST EXPLORER:
   Tag-based filtering; forecasts; recommendations
   Weakness: service-level, not query-level

GCP COST MANAGEMENT:
   Reports + budgets + alerts
   BigQuery flat-rate console

AZURE COST MANAGEMENT:
   Similar scope

SNOWFLAKE ACCOUNT_USAGE:
   SQL views with query + credit history
   Build custom dashboards

DATABRICKS SYSTEM TABLES:
   system.billing.usage, system.workflows.jobs_timeline
   Attribution + utilization reports

PATTERN:
   Start with native dashboards; graduate to specialized tools 
   as spend grows past $1-2M/year.
```

### 20. Open-Source Tools (Kubecost, OpenCost, SELECT)

```
KUBECOST / OPENCOST:
   Cost attribution for K8s workloads
   Per-namespace, per-pod, per-label
   Open-source (OpenCost); hosted (Kubecost SaaS)
   Essential for K8s data platforms

SELECT.DEV:
   Snowflake cost + performance analysis
   Open-source; freemium hosted
   Deeply Snowflake-specific

GRAFANA + PROMETHEUS:
   Generic dashboarding; needs cost source
   Pattern: pipe billing data to Prometheus; dashboard

STAFF SIGNAL: open-source is usually enough until $5M+ spend; 
   commercial tools pay back at that scale.
```

### 21. Commercial Tools (Finout, Vantage, CloudZero)

```
FINOUT:
   Multi-cloud + Snowflake + Databricks + Datadog
   Unified cost view; chargeback automation
   Enterprise pricing

VANTAGE:
   Multi-cloud cost tracking
   Developer-friendly; API-first

CLOUDZERO:
   Unit economics focus ("cost per customer")
   Enterprise target

APPTIO CLOUDABILITY:
   Enterprise FinOps tooling
   Strong for large shared services

Choose based on: scale, cloud mix, data warehouse spend, team maturity.
```

---

## PART VII — GOVERNANCE

### 22. Budgeting + Forecasting

```
PATTERNS:
   
   ANNUAL BUDGET: top-down from finance
      Breakdown per cost center / team
      Expected 10-30% growth typically
   
   ROLLING FORECAST: monthly refresh of year-end projection
      Accounts for actuals + known upcoming initiatives
   
   SCENARIO FORECAST:
      Baseline + optimistic + pessimistic
      Linked to initiatives (new ML models, GenAI expansion, etc.)
   
   UNIT ECONOMICS:
      Cost per X (customer, transaction, model request)
      Enables scaling conversations
      "If we 10x usage, what's the cost?"

STAFF SIGNAL: unit economics tie TECH to BUSINESS. Senior reports costs;
   staff reports cost per customer / per feature.
```

### 23. Cost Review Cadence

```
PATTERNS:
   
   WEEKLY: platform team reviews top-N spenders
      Catch runaway queries, misconfigured clusters
   
   MONTHLY: team-level review
      Per-team dashboards shared
      Benchmarks across teams
   
   QUARTERLY: executive review
      Trends, forecasts, major initiatives
      ROI on tool investments
   
   ANNUALLY: budget negotiation
      Historical + forecast
      Alignment with business objectives

TOOLS:
   Automated reports + alerts on anomalies
   Slack / email channel for cost awareness
```

### 24. The Cost Culture

```
BUILDING A COST-AWARE CULTURE:
   
   EDUCATION:
      Internal wiki with cost patterns
      Lunch-and-learns ("How to query Snowflake cheaply")
      New-hire onboarding includes cost basics
   
   INCENTIVES:
      Celebrate cost reductions
      Include cost efficiency in team OKRs
      Avoid punitive measures (creates hoarding behavior)
   
   TRANSPARENCY:
      Dashboards accessible to all engineers
      No "secret" costs; publish per-team
      Benchmark reports (how do we compare?)

STAFF SIGNAL: a cost culture is a 1-2 year investment. Starts with
   showback, builds trust, graduates to chargeback where appropriate.
```

---

## PART VIII — INTERVIEW PRACTICE

### 25. The Staff-Level FinOps Interview

```
COMMON QUESTIONS:
   1. "Snowflake cost went up 50%. Your plan?"
   2. "Showback vs chargeback: which + why?"
   3. "Design cost attribution for a shared data platform."
   4. "Snowflake vs Databricks on cost."
   5. "Top 10 cost optimizations in your toolkit."
   6. "Egress cost traps."
   7. "FinOps tool evaluation."
   8. "Spot instances for Spark: how + when."
   9. "Materialized views: cost vs benefit math."
   10. "Unit economics for a data platform."
```

### 26. Worked Example

```
Q: Your Snowflake bill grew from $100K/month to $300K/month over 6 months.
   CFO asks for a plan. 48 hours.

STAFF ANSWER:

HOUR 1-4: DIAGNOSIS
   Run ACCOUNT_USAGE queries:
      Top 10 warehouses by credit consumption (last 6 months)
      Top 10 queries by credit consumption
      Daily spend trend per warehouse
      Breakdown: warehouses vs serverless vs storage vs AI (Cortex)

   Hypothesis 1: new workload (team X onboarded)
   Hypothesis 2: query regression (someone's inefficient dbt model)
   Hypothesis 3: wrong-sized warehouses (XL for XS workload)
   Hypothesis 4: Cortex AI running wild
   Hypothesis 5: idle warehouses with long AUTO_SUSPEND

HOUR 5-16: VERIFICATION
   For each hypothesis, confirm with data.
   
   Example findings:
      1. "team X" is actually "team X launching ML feature with 10x queries"
      2. One dbt model regressed to 30-second queries (was 3-second); 
         running hourly; $20K/month impact
      3. 3 warehouses at XL where M would suffice; $30K/month impact
      4. Cortex spending normal
      5. 5 warehouses with AUTO_SUSPEND = 600s; 
         idle $8K/month impact

HOUR 17-40: PLAN
   Immediate (this week):
      Fix the regressed dbt model: $20K/month save
      Resize 3 warehouses: $30K/month save
      Set AUTO_SUSPEND = 60s: $8K/month save
      Set resource monitors (hard cap per warehouse)
   
   Short-term (next 30 days):
      Enable clustering on top-5 scan-heavy tables: $15K/month save estimated
      Materialize views for top-10 repeated queries: $20K/month save
      Review team X workload with their lead: optimize + reassign to tier-3 warehouse

   Medium-term (quarter):
      Deploy SELECT.dev or similar for ongoing visibility
      Monthly team-level dashboards + reviews
      New-hire training on Snowflake efficiency

HOUR 41-48: PRESENTATION
   CFO deck:
      Slide 1: TL;DR — $300K → $200K/month achievable in 90 days
      Slide 2: Breakdown of growth (team X: justified; regression: fixable; 
               rightsizing: fixable; good-citizenship: cultural)
      Slide 3: Immediate actions (this week)
      Slide 4: Monthly savings glide path (target line)
      Slide 5: Governance (resource monitors; monthly reviews; forecasting)
      Slide 6: Investment ask ($250K/year for SELECT + 0.5 FTE)
         ROI: $100K/month savings = $1.2M/year; investment 20% of ROI
```

### 27. War Stories

> **🔥 Production Scenario — The Resource Monitor Save.** A team deployed Snowflake without resource monitors. A new analyst wrote a cartesian join on 10B rows; query ran for 6 hours on an XL warehouse. $8K burned in one query. Fix: set per-warehouse resource monitor with daily credit cap + alerts. Lesson: monitors are the TRIP WIRE for cost incidents.

> **🔥 Production Scenario — The Chargeback Culture Shift.** An org moved from showback to chargeback for Snowflake. Teams fought it initially. 6 months later: average team cost down 25% as teams fixed their own queries. Lesson: cost accountability drives behavior; freedom from accountability drives waste.

> **🔥 Production Scenario — The Egress Shock.** A team ran weekly ML training in us-east-1 with data in us-west-2. Egress: ~$30K/month. Discovered during cost review. Fix: replicated the dataset to us-east-1 S3; retrained local. Egress dropped to $500/month; replication storage cost $200/month. Net save: ~$29K/month. Lesson: cross-region data movement is expensive; co-locate compute and data.

> **🔥 Production Scenario — The Idle Databricks Cluster.** A team had an all-purpose cluster for notebook work: 20 nodes, 10-hour auto-terminate. Nights + weekends: idle. $15K/month wasted. Fix: 2-hour auto-terminate + user-triggered start. Idle cost dropped 80%. Lesson: all-purpose clusters are the single biggest Databricks waste source.

> **🔥 Production Scenario — The Materialized View Payoff.** A team had a BI dashboard refreshing 1000x/day via a 30-second Snowflake query. $20K/month in warehouse time. Added a materialized view: refresh cost $500/month; query time: milliseconds; warehouse scaled down. Net save: $15K/month. Lesson: for predictable repeated queries, materialized views pay back fast.

### 28. Cheat Sheet &amp; Mental Models

#### 28.1 One-Minute Pitch

```
DATA FINOPS = three phases: INFORM → OPTIMIZE → OPERATE

ATTRIBUTION: tags + system tables + chargeback for top spend; showback elsewhere

TOP PATTERNS PER PLATFORM:
   Snowflake: AUTO_SUSPEND, right-size warehouses, clustering, materialized views
   Databricks: jobs clusters not all-purpose, spot executors, serverless for bursty
   BigQuery: partition + cluster, reservations, materialized views
   Spark on K8s: spot, Karpenter, resource request tuning
   Storage: tier by access pattern; retention by table criticality
   Network: VPC endpoints, co-locate compute + data

GOVERNANCE: monthly reviews, forecasts, unit economics, resource monitors

STAFF SIGNAL: argue the TIERED cost story (not uniform cuts), tie to
   unit economics, and build the cost CULTURE (education + transparency).
```

#### 28.2 Senior vs Staff

| Senior | Staff |
|--------|-------|
| "Cut costs." | "Diagnose first. Tag the growth: justified (new workload), waste (regression + rightsizing), cultural (education). Each gets a different response — more capacity, immediate fix, long-term education. Blunt cuts hurt justified work and leave waste." |
| "Use spot instances." | "Spot for batch executors with retries; on-demand for drivers + stateful. Spot interruption rate depends on instance type + capacity strategy. Measure first; tune second; roll back if SLAs break." |
| "Enable Photon." | "Photon is a performance/cost trade: 2x DBU rate for 2-10x speedup. For UDF-heavy workloads, speedup lags DBU premium and cost rises. Benchmark first on YOUR workload." |
| "FinOps is finance's job." | "FinOps is a cross-functional practice: finance owns the budget, platform engineering owns the tooling + attribution, teams own their own efficiency. The staff engineer builds the ATTRIBUTION PLUMBING + the CULTURE so teams can act on their own cost signals." |

> **🧠 Principal Mindset:** Cost is where data engineering meets business. "Our data stack costs $4M a year, generates $40M in revenue impact, and costs 2.3x less than Snowflake-replacement would" is a staff conversation. "Snowflake is expensive" is a senior conversation. The difference is UNIT ECONOMICS tied to BUSINESS VALUE.

---

[← Back to main](./README.md)
