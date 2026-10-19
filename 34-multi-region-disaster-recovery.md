# Topic 34 — Multi-Region & Disaster Recovery

> Standalone deep-dive · RTO/RPO, replication, DR testing, data residency

[← Back to main](./README.md)

---

## How to use this document

Multi-region + DR is where staff engineers prove they can design for the WORST case, not just the common case. This chapter covers the quantitative frameworks (RTO/RPO), replication patterns, per-platform DR capabilities, geo-partitioning, and compliance (GDPR, data residency, data localization).

> **🧠 Principal Mindset:** Every data platform promises durability; few deliver on recovery when it's actually needed. "We have backups" is a senior answer. "RPO 15 min, RTO 2 hours for tier-1 data, tested monthly" is a staff answer.

## Table of Contents

**PART I — FOUNDATIONS**
- 1. What DR Actually Means
- 2. RTO and RPO
- 3. The DR Strategy Spectrum

**PART II — REPLICATION PATTERNS**
- 4. Synchronous vs Asynchronous
- 5. Object Store Replication (S3/GCS/Azure)
- 6. Warehouse Replication
- 7. Lake Replication (Delta / Iceberg / Hudi)

**PART III — MULTI-REGION ARCHITECTURES**
- 8. Active-Passive
- 9. Active-Active
- 10. Geo-Partitioned
- 11. Multi-Cloud DR

**PART IV — PER-PLATFORM DR**
- 12. Snowflake DR
- 13. Databricks DR
- 14. BigQuery DR
- 15. Redshift DR
- 16. Lake DR Patterns

**PART V — BACKUP STRATEGIES**
- 17. Backup Types
- 18. Time Travel vs Backup
- 19. Retention Policies

**PART VI — DR TESTING**
- 20. Why Testing Matters
- 21. Game Days
- 22. Chaos Engineering for Data

**PART VII — COMPLIANCE**
- 23. Data Residency and GDPR
- 24. Data Localization Laws
- 25. Right to Be Forgotten (RTBF)

**PART VIII — INTERVIEW PRACTICE**
- 26. The Staff-Level DR Interview
- 27. Worked Example
- 28. War Stories
- 29. Cheat Sheet &amp; Mental Models

---

## PART I — FOUNDATIONS

### 1. What DR Actually Means

```
Disaster Recovery addresses: how do we RESUME operations after:
   • Region outage (fire, flood, network partition, cloud failure)
   • Catastrophic data corruption (bad deploy, cryptolocker)
   • Account compromise (deleted or encrypted all data)
   • Compliance event (regulatory takedown)
   • Human error (DROP TABLE prod.critical_table)

DR is NOT:
   • High availability within a region (that's HA, different problem)
   • Backup for occasional restore (that's data protection)
   
DR requires FULL OPERATIONAL CAPABILITY in another region/account
with minimal data loss, in a specified time.
```

### 2. RTO and RPO

```
RTO (Recovery Time Objective): how long to be BACK UP after disaster
RPO (Recovery Point Objective): how much DATA LOSS is acceptable

   RPO = 0 (no data loss): synchronous replication required (expensive)
   RPO = 15 minutes: async replication every 15 min
   RPO = 1 hour: hourly backup
   RPO = 24 hours: nightly backup
   
   RTO = 0 (immediate): active-active (both regions always running)
   RTO = minutes: hot standby (passive region ready)
   RTO = hours: warm standby (compute scales up on failover)
   RTO = days: cold standby (restore from backup)

Pricing:
   Lower RTO + lower RPO = more infrastructure = more cost
   
STAFF SIGNAL: pick RTO/RPO per TIER:
   Tier-1 (customer-facing, revenue): RTO 15 min, RPO 5 min
   Tier-2 (internal analytics): RTO 4 hours, RPO 1 hour
   Tier-3 (exploratory): RTO 24 hours, RPO 24 hours
```

### 3. The DR Strategy Spectrum

```
       COLD           WARM            HOT          ACTIVE
       STANDBY        STANDBY         STANDBY      ACTIVE
      ──────────────────────────────────────────────────→
       Backup only    Minimal compute Scaled compute Full dual
       Cheapest       Moderate         Expensive      Most expensive
       RTO: days      RTO: hours      RTO: minutes   RTO: 0
       RPO: hours     RPO: ~1 hour    RPO: minutes   RPO: ~0
   
Match the strategy to the TIER. Hot standby for tier-1 is right;
   hot standby for tier-3 is wasteful.
```

---

## PART II — REPLICATION PATTERNS

### 4. Synchronous vs Asynchronous

```
SYNCHRONOUS:
   Writes replicate before being acknowledged
   RPO = 0 (no data loss)
   Latency: write time = distance between regions
   Cross-region sync: write latency 50-200ms
   Risk: if remote region is slow, writes stall

ASYNCHRONOUS:
   Writes acknowledged locally; replicate in background
   RPO > 0 (some loss window)
   Low write latency
   Risk: data loss if primary dies before replication completes

PATTERN:
   Within region: synchronous (multi-AZ)
   Cross-region: asynchronous (except highest-tier use cases)
```

### 5. Object Store Replication (S3/GCS/Azure)

```
S3 CROSS-REGION REPLICATION (CRR):
   Enable per bucket; new objects replicate to target bucket
   Asynchronous; replication lag typically minutes
   Separate cost (storage in target + egress for replication traffic)
   
S3 CRR CAVEATS:
   Deletes propagate (configurable); but delete markers also replicate
   RTC (Replication Time Control): 99.99% within 15 minutes SLA
   
GCS CROSS-REGION:
   Multi-region or dual-region buckets (built-in redundancy)
   Cross-region replication for single-region buckets via config

AZURE BLOB:
   RA-GZRS (Read-Access Geo-Zone-Redundant Storage): built-in
   Replication is automatic for geo-redundant tiers

STAFF SIGNAL: object store replication is the FOUNDATION of lake DR.
   Enable CRR with RTC for tier-1 data; alert if replication lag exceeds SLA.
```

### 6. Warehouse Replication

```
SNOWFLAKE:
   Database replication: async replication across accounts (regions, clouds)
   Primary + secondary; manual or automated failover
   Supported: tables, views, warehouses, roles, pipelines
   Metadata replication included
   Replication lag: minutes typical
   
BIGQUERY:
   Dataset cross-region replication (preview feature at 2024-2026)
   Native: SNAPSHOT + export + import for DR
   Cross-region backup via scheduled queries + exports
   
REDSHIFT:
   Snapshot copying to another region (continuous after enabled)
   Cross-region snapshot restore
   
DATABRICKS:
   Delta Live Tables + Delta Sharing for cross-region replication
   Workspace-level DR via Terraform redeployment
   Unity Catalog metastore in HA mode
```

### 7. Lake Replication (Delta / Iceberg / Hudi)

```
LAKE REPLICATION approaches:
   
   OBJECT STORE REPLICATION:
      S3 CRR replicates the files
      Metadata (manifests, snapshots) also files → replicated
      Target bucket queryable by any engine
      Pattern: simplest, cheapest
   
   CATALOG REPLICATION:
      Metadata catalog (Polaris, Unity Catalog, Glue) also replicated
      Enables querying from the standby region
   
   APPLICATION-LEVEL:
      Spark/Flink job writes to BOTH regions concurrently
      Dual-write: more complex; stronger consistency
      Pattern: for strictly consistent cross-region access

ICEBERG SPECIFICALLY:
   Snapshot-based replication: copy manifests + data files
   Pattern: scheduled job replicates to target bucket; registers in target catalog
   
DELTA SPECIFICALLY:
   Delta DEEP CLONE: copies table including transaction log; incremental refresh
   Delta Sharing: read-only share (not full DR but useful for distribution)
```

---

## PART III — MULTI-REGION ARCHITECTURES

### 8. Active-Passive

```
STRUCTURE:
   PRIMARY region handles all traffic
   PASSIVE region replicates data; minimal compute
   On disaster: scale up passive; redirect traffic
   
PROS:
   Simpler to reason about (one source of truth)
   Lower cost than active-active
   
CONS:
   Passive region may have stale data (async replication)
   Failover is manual or scripted; takes minutes
   RTO: minutes to hours
   RPO: minutes

GOOD FOR: tier-1 data where RTO ≤ 1 hour is acceptable.
```

### 9. Active-Active

```
STRUCTURE:
   BOTH regions serve traffic simultaneously
   Writes propagate between regions
   Reads served locally for latency
   
PROS:
   Zero downtime failover (one region fails → other continues)
   Low read latency globally
   
CONS:
   Concurrency conflicts (write in region A + region B to same row)
   Resolution: last-write-wins, CRDTs, or app-level conflict resolution
   Significantly more expensive (double infrastructure + replication)
   
GOOD FOR: tier-0 globally-distributed apps where RTO = 0 is required.

STAFF SIGNAL: active-active is EXPENSIVE and COMPLEX.
   Argue it only when the SLA genuinely demands it.
```

### 10. Geo-Partitioned

```
STRUCTURE:
   DATA PARTITIONED by region ownership
   EU customer data in EU region; US customer data in US region
   
PROS:
   Compliance with data residency (GDPR, data localization)
   Latency: each region's data served locally
   Scaling: regions grow independently
   
CONS:
   Analytics across regions requires aggregation (join across regions)
   Deployment complexity (per-region data stores)
   
PATTERN:
   Metadata layer indicates which region holds which data
   Query routing: send query to the right region
   Cross-region analytics via local pre-aggregation + central aggregation
```

### 11. Multi-Cloud DR

```
MOTIVATION:
   Cloud-level failure resilience (AWS us-east-1 outage pattern)
   Avoid single-cloud lock-in for critical systems
   
CHALLENGES:
   Different managed services per cloud (DMS vs Datastream vs Data Factory)
   Data transfer cost (massive egress between clouds)
   Operational complexity (two cloud teams, two ops stacks)
   
PATTERN:
   Store primary data in Cloud A; replicate to Cloud B
   Minimal compute in Cloud B (standby)
   Scale up Cloud B on Cloud A failure

STAFF SIGNAL: multi-cloud DR is EXPENSIVE and few orgs actually
   execute it. Most settle for multi-region within one cloud. 
   Multi-cloud for cost or regulatory reasons, not primarily DR.
```

---

## PART IV — PER-PLATFORM DR

### 12. Snowflake DR

```
SNOWFLAKE DR PRIMITIVES:
   Database Replication: replicate database to target account (another region/cloud)
   Failover Groups: coordinated failover of multiple databases
   Client Redirect: DNS-level redirect to failover account

PATTERN:
   Primary account: us-east-1
   Secondary account: us-west-2 (replicated)
   Failover Group includes: finance_db, product_db
   On disaster:
      ALTER FAILOVER GROUP ... PRIMARY TO secondary;
      Client redirect updates URL; apps reconnect
   
   Typical RTO: 30 min - 2 hours
   Typical RPO: 10 min - 1 hour (async replication lag)

COST:
   Secondary account: storage (full copy) + replication compute + 
      standby warehouses (optional)
   Typically 50-100% of primary cost for warm standby
```

### 13. Databricks DR

```
DATABRICKS DR:
   WORKSPACE-LEVEL: terraform-deploy workspace in target region
   METASTORE: Unity Catalog metastore in HA mode (multi-region)
   DATA: Delta tables on S3; use S3 CRR for file replication
      PLUS Deep Clone or Delta Sharing for consistent snapshots
   JOBS: terraform-manage workflows; deploy to standby workspace
   SECRETS: external secrets manager (AWS Secrets Manager, etc.)

PATTERN:
   Primary workspace in us-east-1
   Standby workspace in us-west-2 (terraform-deployed; same config)
   S3 CRR replicates lake data
   Delta DEEP CLONE for consistent point-in-time snapshots
   On disaster: enable standby workspace; redirect traffic
```

### 14. BigQuery DR

```
BIGQUERY DR:
   MULTI-REGION DATASETS: built-in redundancy across region (not for individual DR)
   CROSS-REGION DATASET REPLICATION: available 2024+ (preview → GA)
   SNAPSHOT + EXPORT: historically the DR pattern
   
PATTERN:
   Dataset in US multi-region (built-in cross-region resilience)
   Scheduled queries create snapshots nightly
   Snapshots exported to GCS cross-region buckets
   On disaster: import to target region

NEW (2024+): dataset-level cross-region replication is in preview;
   graduating to GA will make BQ DR much simpler.
```

### 15. Redshift DR

```
REDSHIFT DR:
   AUTO SNAPSHOTS: scheduled, retained up to 35 days
   CROSS-REGION SNAPSHOT COPYING: enable per cluster
   CLUSTER RESTORE: from snapshot in target region

RA3 CLUSTERS: separation of compute + managed storage; faster failover

PATTERN:
   Primary cluster snapshots copied to secondary region
   On disaster: restore cluster from latest snapshot in secondary region
   Typical restore time: 1-2 hours for TB-scale cluster
```

### 16. Lake DR Patterns

```
TIER-1 ICEBERG TABLES:
   Source: us-east-1 bucket, catalog in Polaris
   
   STRATEGY:
      1. S3 CRR to us-west-2 bucket (metadata + data replicated)
      2. Polaris catalog in HA mode (or secondary catalog in us-west-2)
      3. Secondary Polaris registers tables pointing to us-west-2 bucket
      4. On disaster: Polaris failover; engines query us-west-2 bucket + catalog
   
   RPO: 15 min (S3 CRR with RTC)
   RTO: < 1 hour (catalog + engine failover)

CROSS-ENGINE: Iceberg's open format means any engine (Spark, Trino, Snowflake)
   can be the recovery engine in secondary region.
```

---

## PART V — BACKUP STRATEGIES

### 17. Backup Types

```
TYPES:
   
   FULL: complete copy of data
      Pros: self-contained restore
      Cons: expensive storage + copy time
   
   INCREMENTAL: changes since last backup
      Pros: small; fast
      Cons: restore requires chain of backups
   
   DIFFERENTIAL: changes since last FULL
      Pros: faster restore than incremental chain
      Cons: grows larger between fulls
   
   CONTINUOUS (CDC-based):
      Append-only log of changes
      Point-in-time recovery to any timestamp

MODERN LAKEHOUSE:
   TIME TRAVEL = point-in-time recovery (built-in)
   S3 versioning + lifecycle = file-level recovery
   Combined: lake has built-in DR primitives
```

### 18. Time Travel vs Backup

```
TIME TRAVEL (Delta, Iceberg, Snowflake, BigQuery):
   Query any historical version within retention
   NOT a disaster recovery mechanism per se
   (storage is co-located; disaster destroys both current + history)
   
   USE: recovery from LOGICAL errors (bad deploy, DROP TABLE)
   
BACKUP:
   Independent copy in separate location
   
   USE: recovery from PHYSICAL disaster (region outage)

BOTH ARE NEEDED. Time travel is NOT a substitute for backup.

STAFF SIGNAL: a staff answer always distinguishes these; a senior
   answer often confuses "time travel" with "DR."
```

### 19. Retention Policies

```
RETENTION TIERS by use case:
   
   OPERATIONAL (time travel in current region): 7-30 days
      "oops, I ran a bad UPDATE" recovery
   
   BACKUP (cross-region snapshots): 30-90 days
      "us-east-1 is down" recovery
   
   ARCHIVAL (long-term immutable): 1-7 years
      Compliance / regulatory
   
   LEGAL HOLD: indefinite
      Legal / litigation requirements

RETENTION POLICY:
   Encoded as code (Terraform, Lifecycle policies)
   Reviewed annually
   Alerted on changes
```

---

## PART VI — DR TESTING

### 20. Why Testing Matters

```
UNTESTED DR = NO DR.
   Teams discover failed replication in disasters
   Documentation always lags reality
   Runbooks always have holes
   
MATURITY LADDER:
   LEVEL 0: no testing; "we trust the vendor"
   LEVEL 1: annual tabletop exercises (talk through disasters)
   LEVEL 2: annual full failover test (actually failover prod)
   LEVEL 3: quarterly or monthly automated DR tests
   LEVEL 4: continuous chaos testing (random failures)

Most orgs are at Level 0-1. Mature financial services at Level 3-4.
```

### 21. Game Days

```
GAME DAY: scheduled DR exercise.
   
   PREPARATION:
      Define scenario (which region fails, which services, time window)
      Notify stakeholders (customers, exec team, compliance)
      Set success criteria (RTO + RPO measurement)
   
   EXECUTION:
      Simulate failure (block traffic to primary region)
      Follow runbook: failover
      Measure: time to failover, data loss, service impact
   
   DEBRIEF:
      What worked, what didn't
      Runbook updates
      Infrastructure improvements
      Next game day's scenario

Repeat quarterly or more often.
```

### 22. Chaos Engineering for Data

```
CHAOS ENGINEERING: continuous random failure injection.
   
   TOOLS: Chaos Monkey (Netflix), Chaos Mesh, Litmus (K8s)
   
   FOR DATA:
      Kill replica nodes
      Delete file (test S3 versioning / CRR)
      Fail connector tasks
      Simulate region partition
   
   PRINCIPLES:
      Start small (test environments)
      Measure impact
      Graduate to production carefully
      Blast radius controlled

STAFF SIGNAL: chaos testing data systems is MATURE org territory.
   For most orgs, scheduled game days are the right level.
```

---

## PART VII — COMPLIANCE

### 23. Data Residency and GDPR

```
DATA RESIDENCY: data must STAY in a specific jurisdiction.
   GDPR: EU personal data stays in EU (or in approved countries)
   China: data about Chinese citizens stays in China (CSL + PIPL)
   Russia: similar (On Personal Data Act)
   India: emerging (DPDP Act)
   
IMPLICATIONS FOR DR:
   DR region must be in SAME jurisdiction
   EU primary → EU DR (not US DR)
   
   For global apps: geo-partitioned architecture (section 10)

GDPR SPECIFIC:
   Data processing records
   Lawful basis for processing
   Data subject rights (access, portability, erasure)
   DPO responsibilities
```

### 24. Data Localization Laws

```
LOCALIZATION LAWS (2024-2026 landscape):
   
   HEAVY: China (CSL + PIPL), Russia (152-FZ), 
          Vietnam, Indonesia (growing)
   MODERATE: India (DPDPA), Nigeria (NDPR)
   LIGHT: EU (GDPR allows transfers to adequate jurisdictions)
   DEBATED: US (sector-specific; patchwork of state laws)

ARCHITECTURAL IMPLICATIONS:
   Multi-region required for global reach
   Each market has own data store
   Analytics requires cross-region aggregation
   DR strategy is PER REGION, not global

STAFF SIGNAL: data localization is a GROWING constraint. 
   Design multi-region from the start if targeting global markets.
```

### 25. Right to Be Forgotten (RTBF)

```
RTBF (GDPR Article 17): data subjects can request deletion.
   
IMPLEMENTATION CHALLENGES in data lakes:
   • Lake is immutable (append-only files)
   • Historical versions preserved (time travel)
   • Backups + replicas duplicate data
   • Downstream caches + aggregates derive from deleted data

PATTERNS:
   TOMBSTONE: mark records as deleted; filter in queries
      Still has personal data; not true deletion
   
   HARD DELETE: Delta DELETE / Iceberg DELETE statements
      Rewrites files; expensive but true delete
      Does NOT remove historical snapshots (time travel still has data)
      Must set retention to purge: VACUUM / expire_snapshots
      Must propagate to replicas + backups
   
   PERSONAL DATA SEGREGATION:
      Keep PII in a dedicated table
      Delete the PII row; downstream aggregates no longer resolve
      Non-PII history preserved

TOOLING:
   Databricks: Lakehouse Monitoring + GDPR workflow templates
   Snowflake: DELETE + VACUUM; Horizon classification
   Compliance-first vendors: Immuta, OneTrust integration
```

---

## PART VIII — INTERVIEW PRACTICE

### 26. The Staff-Level DR Interview

```
COMMON QUESTIONS:
   1. "What are your RTO and RPO targets?"
   2. "Design a DR strategy for a $100M-revenue SaaS."
   3. "Active-passive vs active-active."
   4. "Lake DR via S3 CRR + Iceberg + Polaris."
   5. "Time travel ≠ DR. Why?"
   6. "GDPR + DR: how do they interact?"
   7. "DR testing: how often?"
   8. "RTBF in a data lake."
   9. "Multi-cloud DR: when + why not."
   10. "Snowflake failover: walk me through it."
```

### 27. Worked Example

```
Q: Design DR for a global SaaS fintech with:
   • 50M users (US, EU, APAC)
   • $100M ARR
   • Snowflake for analytics, Databricks for ML, Postgres for OLTP
   • Data residency: EU users in EU, US users in US, 
      APAC users in Singapore
   • Regulated: PCI DSS + SOC 2 + GDPR

STAFF ANSWER:

TIERING:
   Tier-1 (revenue-impacting): payments, user data, auth → 
      RPO 15 min, RTO 1 hour
   Tier-2 (analytics): BI dashboards → RPO 1 hour, RTO 4 hours
   Tier-3 (exploratory): data science sandboxes → RPO 24h, RTO best-effort

GEO-PARTITIONED ARCHITECTURE:
   US REGION (us-east-1 primary, us-west-2 DR):
      Postgres (RDS Multi-AZ + Cross-Region replica)
      Snowflake US account (replicated to us-west-2 account)
      Databricks us-east-1 workspace (DR in us-west-2 via terraform)
      S3 CRR for Iceberg lake
   
   EU REGION (eu-west-1 primary, eu-central-1 DR):
      Same stack; data stays in EU
      Separate Snowflake EU account; separate Databricks workspace
   
   APAC REGION (ap-southeast-1 primary, ap-northeast-1 DR):
      Same pattern

DR STRATEGY:
   Tier-1: active-passive with WARM STANDBY
      Postgres: Multi-AZ synchronous + cross-region async replica
         Automatic failover within AZ; manual cross-region
      Snowflake: Database Replication to DR account; Failover Group
         Automated client redirect on failover (manual trigger)
      Lake (Iceberg): S3 CRR + secondary Polaris catalog pointing to DR bucket
      Databricks: terraform-deployed DR workspace; dormant until needed
   
   Tier-2: scheduled backups cross-region (nightly)
      Restore from backup within 4-hour RTO
   
   Tier-3: snapshots in same region; accept region-level loss

CROSS-REGION ANALYTICS (geo-partitioned):
   Each region computes local aggregates nightly
   Global dashboards: aggregate the aggregates (small cross-region transfer)
   Avoids GDPR issues (no raw EU PII in US)

TESTING:
   Monthly: tabletop exercise for one tier
   Quarterly: full regional failover (one of three regions)
   Annually: end-to-end DR exercise with external audit

MONITORING:
   Replication lag per source (Postgres replica, S3 CRR, Snowflake)
   Backup success rate (alerts on failures)
   DR runbook currency (annual review)

RTBF:
   PII in dedicated Postgres + Snowflake dim tables
   On RTBF request: delete from Postgres → CDC propagates
   Snowflake: DELETE + VACUUM + propagate to DR replica
   Lake Iceberg: DELETE + expire_snapshots on both primary + DR
   Downstream aggregates: regenerate without the user's rows

COST:
   DR infrastructure: ~30% of primary cost
   Replication (egress + storage): ~$30K/month
   Monitoring + testing + runbooks: ~2 FTE
   Total DR cost: ~$500K-1M/year
   Business continuity vs cost: justified by ARR + regulatory exposure

AUDITS:
   Annual SOC 2 Type II: validate DR capability
   Quarterly compliance review
   Documented RTO/RPO + evidence from tests
```

### 28. War Stories

> **🔥 Production Scenario — The us-east-1 Outage.** During a 4-hour us-east-1 S3 outage, a team's "DR" couldn't fail over: their secondary Snowflake account's replication had quietly stopped 2 weeks earlier (no alert configured). Analytics down for 6 hours; downstream BI dashboards wrong. Lesson: monitoring + alerting on replication lag is NON-NEGOTIABLE for DR.

> **🔥 Production Scenario — The Game Day Discovery.** A team's quarterly game day failed over Snowflake. Discovery: secondary account's warehouses were XS; primary's were L. BI queries ran 10x slower. Lesson: standby capacity must be sized for the full workload, not minimum.

> **🔥 Production Scenario — The RTBF Backfill Nightmare.** An RTBF request for a user who signed up 3 years ago. Deleted from Postgres; CDC → warehouse → lake. BUT: all historical aggregates still contained their contribution. Took 2 months to regenerate affected aggregates. Lesson: RTBF in a data lake is a HARD PROBLEM; design for it (PII segregation) from the start.

> **🔥 Production Scenario — The Residency Trap.** A SaaS deployed BI dashboards from EU customer data in US Snowflake. Easier for the US team. GDPR audit flagged the arrangement. Had to migrate EU data back to EU Snowflake; 6-month project; blocked product launches. Lesson: data residency determines WHERE compute happens, not just WHERE data is stored.

> **🔥 Production Scenario — The Terraform-Driven DR.** A team wrote their entire Databricks workspace in Terraform (jobs, DLT, SQL warehouses, permissions). DR workspace in another region was a `terraform apply` away. During a real incident: workspace deployed in 30 minutes; data was already replicated via S3 CRR. Total RTO: 45 minutes. Lesson: IaC enables rapid regional reconstruction; non-IaC workspaces are DR disasters.

### 29. Cheat Sheet &amp; Mental Models

#### 29.1 One-Minute Pitch

```
DR = RTO + RPO + REPLICATION + TESTING + RUNBOOKS

TIER DATA BY DR TARGET:
   Tier-1: RPO 15 min, RTO 1 hour (warm standby + sync replication)
   Tier-2: RPO 1 hour, RTO 4 hours (async replication + backup)
   Tier-3: RPO 24 hours, RTO best-effort (daily backup)

PER-PLATFORM:
   Snowflake: database replication + failover groups
   Databricks: Terraform-deployed workspace + S3 CRR + Deep Clone
   BigQuery: cross-region dataset replication (new) or snapshot + export
   Lake: S3 CRR + Polaris HA
   Postgres: Multi-AZ + cross-region replica

COMPLIANCE:
   Data residency / GDPR: DR region in SAME jurisdiction
   RTBF: design PII segregation up front

TEST:
   Quarterly game days + monthly tabletops
   Automated monitoring on replication lag
```

#### 29.2 Senior vs Staff

| Senior | Staff |
|--------|-------|
| "We have backups." | "Tier-1 data: RPO 15 min via async replication + WARM standby in us-west-2; RTO 1 hour via scripted failover. Tier-2: nightly backups + 4-hour RTO. Monthly game days verify. SLAs documented + tracked." |
| "Time travel is DR." | "Time travel recovers from LOGICAL errors (bad deploy, DROP TABLE) within same region. For PHYSICAL disaster (region outage), you need cross-region replication to a separate location. Both are needed; they solve different problems." |
| "Multi-cloud is safer." | "Multi-cloud DR is expensive (double service familiarity, data egress between clouds) and operationally complex. Most orgs are better with multi-region within one cloud. Multi-cloud for compliance or cost reasons, rarely primarily for DR." |
| "DR is tested annually." | "Annual testing is too infrequent — runbooks go stale. Quarterly full game days + monthly tabletop exercises + continuous replication monitoring is the right cadence. Untested DR often fails on real disaster." |

> **🧠 Principal Mindset:** DR is the one topic where "we do it like everyone else" is wrong. Each org has specific RTO/RPO per tier based on revenue impact, regulation, and cost tolerance. Senior engineers follow runbooks. Staff engineers write the runbooks, test them, prune the ones that don't fire, and defend the DR budget against finance's cost pressure with the business continuity math.

---

[← Back to main](./README.md)
