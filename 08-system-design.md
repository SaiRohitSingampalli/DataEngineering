# Topic 8 — Data System Design (Capstone)

> Section 8 of 10 · 66 pages in source PDF · pages 452–517

[← Back to main](./README.md)

---

Data System Design (Capstone)

```
 Data System Design
 The Capstone — Architect-Level Synthesis
 Lambda · Kappa · Medallion · CDC · Idempotency · Late Data · Multi-Region ·
 Reference Architectures · The Architecture Interview
 The synthesis topic: SQL + Spark + Airflow + Kafka + dbt + Lakehouses + Warehouses →
 designing complete data platforms.
 Principal/Staff Engineer Level Reference
```

Data System Design (Capstone)

How to use this document This is the capstone for the data engineering interview series. The previous seven topics covered individual technologies. This one is about weaving them together: turning a vague business need into a concrete architecture; reasoning about trade-offs that cross many layers; defending choices in front of a panel. At staff/principal level, system design isn't tested with toy questions like 'design Twitter'. It's tested with real architectural decisions: how do you build the platform that ingests CDC from 30 production DBs, streams clicks at 5 GB/s, models a semantic layer for 200 analysts, serves features to ML, and stays under a $2M annual budget? That's what we drill here. ■ PRINCIPAL MINDSET: Three staff-level signals: (1) you can frame design problems with the right clarifying questions before drawing boxes; (2) you reason about CONSEQUENCES (operational, cost, organizational) of architectural choices, not just the choices themselves; (3) you have a vocabulary of patterns — Lambda, Kappa, medallion, outbox, CQRS — and know when each fits and when it doesn't.

Data System Design (Capstone)

Table of Contents

## PART I — THE DESIGN FRAMEWORK

#### 1. The Architecture Interview Format

#### 2. Requirements Gathering & Clarification

#### 3. The Universal Design Loop

#### 4. Estimation & Capacity Planning

## PART II — ARCHITECTURAL PATTERNS

#### 5. Lambda vs Kappa

#### 6. The Medallion Architecture (Modern Default)

#### 7. Data Mesh, Data Vault & OBT

#### 8. Event-Driven & CQRS Patterns

## PART III — INGESTION PATTERNS

#### 9. Batch Ingestion at Scale

#### 10. Streaming Ingestion End-to-End

#### 12. Idempotency & Exactly-Once Semantics

## PART IV — STORAGE & PROCESSING DECISIONS

#### 13. Warehouse vs Lakehouse vs Hybrid

#### 14. The Modeling Layer Decision

#### 15. Late-Arriving Data & Out-of-Order Events

#### 16. Multi-Region Architectures

## PART V — REFERENCE ARCHITECTURES

#### 17. The Modern Analytics Platform

#### 18. The Real-Time Analytics Platform

#### 19. The ML Platform

#### 20. The Multi-Tenant SaaS Data Platform

## PART VI — CROSS-CUTTING CONCERNS

#### 21. Data Quality at Platform Scale

#### 22. Governance, Catalog & Lineage

#### 23. Cost Engineering as a Discipline

#### 24. Observability & SLOs

## PART VII — MIGRATION & EVOLUTION

#### 25. Replatforming Patterns

#### 26. The Tech Debt Inventory

## PART VIII — INTERVIEW PRACTICE

#### 27. Five Worked Design Interviews

Data System Design (Capstone)

#### 28. War Stories from the Field

#### 29. Cheat Sheet & Mental Models

Data System Design (Capstone)

## PART I — THE DESIGN FRAMEWORK

#### 1. The Architecture Interview Format

  - 1.1 What's Actually Being Tested
- SYSTEM DESIGN INTERVIEWS at staff/principal level test:
- 1. CLARIFYING THE PROBLEM  (do you ask the right questions?)
- 2. STRUCTURED THINKING     (can you decompose the problem?)
- 3. TECHNICAL DEPTH          (do you know the building blocks?)
- 4. TRADE-OFF REASONING      (can you weigh alternatives explicitly?)
- 5. OPERATIONAL THINKING     (cost, monitoring, failure, evolution?)
- 6. COMMUNICATION            (can you defend choices clearly?)
- 7. SCOPE MANAGEMENT          (where do you spend time vs hand-wave?)
- NOT TESTED:
- ✗ Memorizing reference architectures verbatim
- ✗ Knowing every vendor's pricing
- ✗ Having a "perfect" answer
- ✗ Drawing aesthetic diagrams

```sql
1.2 The Typical 45-60 Minute Structure
MINUTES 0-5: PROBLEM RESTATEMENT + CLARIFICATION
  "Let me make sure I understand. You want X to handle Y volume serving Z users.
   Some questions: What's the latency target? Multi-tenant? Budget signal?"
MINUTES 5-10: NON-FUNCTIONAL REQUIREMENTS + CAPACITY ESTIMATION
  Scale (events/sec, TB stored, users)
  SLAs (latency, freshness, uptime)
  Compliance / data residency
MINUTES 10-20: HIGH-LEVEL ARCHITECTURE
  Boxes and arrows: sources → ingestion → storage → processing → serving
  Justify each box choice briefly
  Don't get lost in details yet
MINUTES 20-35: DEEP DIVE INTO 1-3 CRITICAL COMPONENTS
  Interviewer chooses (or you propose): "Let's zoom into the streaming ingest"
  Show real depth here — schemas, partitioning, failure modes
MINUTES 35-45: TRADE-OFFS & ALTERNATIVES
  "I picked Kafka for X, but Kinesis would also work — here's why I chose..."
  "If volume doubled, this is what would break first"
MINUTES 45-55: OPERATIONS & EVOLUTION
  Monitoring, cost, on-call burden
  How would you migrate to this from a legacy stack?
  Where would you evolve next?
MINUTES 55-60: WRAP + QUESTIONS YOU HAVE
```

```
1.3 The Mistakes That Sink Candidates
1. DIVING INTO THE SOLUTION TOO FAST
   Asked: "Design a real-time analytics platform."
   Bad:    [starts drawing Kafka → Flink → Redis]
   Good:   "Real-time meaning sub-second to user, or sub-minute updates to
           dashboards? Which volume? Which kind of analytics?"
2. NO CAPACITY MATH
   Saying "we'll use Kafka" without thinking about whether 1 cluster handles
   the volume. Always do back-of-envelope math.
```

Data System Design (Capstone)

```
3. NAMING TOOLS WITHOUT EXPLAINING WHY
   "We'll use Iceberg." Why Iceberg vs Delta vs Hudi? Why not warehouse-native?
4. IGNORING OPERATIONS
   Beautiful architecture diagrams that don't address: who's on call,
   how do alerts work, how do you debug?
5. NOT REVISITING ASSUMPTIONS
   30 minutes in, interviewer changes a requirement.
   Bad: cling to original plan.
   Good: "That changes things. Now I'd lean toward X instead of Y because..."
6. AVOIDING TRADE-OFFS
   "X is better than Y." Better at WHAT? At what cost? When does it flip?
7. NOT ENGAGING WITH THE INTERVIEWER
   Designing in a vacuum. Good designers narrate, ask, adjust.
```

```
1.4 The Mental Model: You're a Consultant
IMAGINE: a CTO has asked you to architect a data platform.
You have 1 hour. The interviewer is the CTO.
Your job:
   Understand their problem deeply before solving
   Give them options with clear trade-offs
   Defend your recommendation
   Anticipate what could go wrong
   Lay out a path forward
This frame helps you avoid:
   Showing off  (you're solving THEIR problem)
   Being defensive (changes in requirements = new info)
   Going down rabbit holes (consultant time is expensive)
   Pretending certainty when there's none
■ INTERVIEW ANGLE: Q: Design a real-time analytics platform.
Senior response: Kafka for ingest, Flink for stream processing, ClickHouse for serving, Grafana on top.
Staff response: Before I architect, can I ask a few things? (1) Real-time meaning what — sub-second
user-facing, or sub-minute dashboards? (2) What volume — ballpark events/sec and total daily data? (3) What
kind of analytics — pre-defined dashboards, ad-hoc SQL, or both? (4) Who consumes — engineers, analysts,
end users? (5) Existing stack — are we greenfield or replacing something? (6) Budget order of magnitude? Once
I know those, the architecture practically picks itself.
```

Data System Design (Capstone)

#### 2. Requirements Gathering & Clarification

```
2.1 The Question Categories
1. FUNCTIONAL REQUIREMENTS
    What's the platform for?
    Who are the users?
    What use cases must it support?
    What's explicitly out of scope?
2. SCALE
    Events / records per second (peak vs average)
    Total data volume (GB / TB / PB)
    Growth rate
    Concurrent users / queries
    Number of producers / consumers / sources
3. LATENCY & FRESHNESS
    Ingestion latency (event → queryable)
    Query latency (sub-sec? sec? min?)
    End-to-end freshness target
    Tolerance for late data
4. RELIABILITY
    Availability target (99%? 99.9%? 99.99%?)
    RPO (data loss tolerance)
    RTO (recovery time tolerance)
    What happens during partial outages?
5. CONSISTENCY
    Strong vs eventual?
    Exactly-once vs at-least-once acceptable?
    Transactional needs?
6. COMPLIANCE & SECURITY
    Data residency (regions, sovereignty)
    PII / PCI / HIPAA / GDPR
    Audit requirements
    Right-to-delete
7. COST
    Budget order of magnitude
    Cost-sensitivity vs latency-sensitivity
    Reserved capacity vs elastic
8. ECOSYSTEM
    Cloud commitments
    Existing stack to integrate
    Team skills / preferences
    Vendor relationships
```

```
2.2 The Hidden Requirements
ALWAYS PROBE:
   "How is this currently being done?" (reveals pain, constraints)
   "What would success look like in 12 months?" (reveals real goals)
   "What kept you from doing this earlier?" (reveals organizational reality)
   "What's the team that will operate this?" (size, skills, on-call capacity)
   "What's the worst that could happen?" (drives reliability + compliance)
THESE QUESTIONS REVEAL:
   Real (vs stated) priorities
   Organizational constraints
   Hidden complexity
   Political dimensions
STAFF-LEVEL ANSWER changes based on these.
Junior gives the same answer regardless.
```

#### 2.3 The Right Level of Clarification

Data System Design (Capstone)

```
TOO LITTLE:
  Dive into solution, miss the real problem.
TOO MUCH:
  Spend 20 minutes asking; never get to the architecture.
THE 5-MINUTE RULE:
  5-10 minutes of focused clarification, then commit to assumptions
  out loud and proceed.
  "I'll assume: AWS-only, ~10K events/sec peak, dashboards refreshing
   every 5 minutes is the latency target, 100 analysts will use it,
   budget under $200K/year. Stop me if any of those is wrong."
This frees you to design while making assumptions explicit.
Interviewer can correct in real-time.
```

```sql
2.4 Documenting Assumptions
WRITE THEM DOWN AS YOU GO:
  In a corner of the whiteboard or chat:
  ASSUMPTIONS:
     Cloud: AWS
     Scale: 10K events/s peak, ~1 TB/day
     Latency: 5-min dashboard freshness
     Users: 100 internal analysts
     Budget: < $200K/year
     Compliance: SOC 2 (no GDPR / HIPAA)
WHY:
   Lets you reference back ("recall I assumed X")
   Interviewer can flag issues
   Forces you to be specific
   Shows structured thinking
■ PRINCIPAL MINDSET: Requirements gathering separates senior from staff faster than any other skill. Most
senior engineers can architect a sensible system once the requirements are clear. Staff engineers find the right
requirements when they're vague — and reframe the problem when needed. The first 5 minutes of a design
interview is where most of the signal lives.
```

Data System Design (Capstone)

#### 3. The Universal Design Loop

```
3.1 The Steps
1. CLARIFY (5-10 min)
   Ask questions; document assumptions; commit.
2. ESTIMATE (3-5 min)
   Volume, latency, capacity math.
3. SKETCH HIGH-LEVEL (5-10 min)
   Sources → ingestion → storage → processing → serving → consumers.
   Box per component; name your picks; brief justification.
4. DEEP DIVE (15-20 min)
   On 1-3 components. Often: the data model, the streaming layer, or
   the failure modes. Let the interviewer guide which.
5. TRADE-OFFS (5-10 min)
   What did you NOT pick, and why? Where would you change your mind?
6. OPERATIONS (5-10 min)
   Monitoring, cost, on-call, evolution, migration.
7. WRAP (2-5 min)
   "If I had more time, I'd dive into..."
   Questions you have.
```

```
3.2 The Five-Layer Mental Model
EVERY DATA PLATFORM HAS 5 LAYERS:
  ■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■
  ■ 5. CONSUMPTION                                  ■
  ■    BI tools, ML, reverse-ETL, APIs              ■
  ■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■
                       ▲
  ■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■
  ■ 4. SERVING / MODELING                           ■
  ■    Warehouse, marts, semantic layer             ■
  ■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■
                       ▲
  ■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■
  ■ 3. PROCESSING                                   ■
  ■    Spark, dbt, Flink (batch + stream)           ■
  ■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■
                       ▲
  ■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■
  ■ 2. STORAGE                                       ■
  ■    Lakehouse / Warehouse / Object Store         ■
  ■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■
                       ▲
  ■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■
  ■ 1. INGESTION                                     ■
  ■    Batch ETL, Kafka, CDC                        ■
  ■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■
                       ▲
  ■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■
  ■ 0. SOURCES                                       ■
  ■    Production DBs, SaaS APIs, files, events     ■
  ■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■
DESIGN TOP-DOWN OR BOTTOM-UP — but ALWAYS COVER ALL 5 LAYERS.
Forgetting one is the most common scoring miss.
```

```
3.3 Choosing Tools per Layer
LAYER 1: INGESTION
  Batch:     Fivetran / Airbyte / Stitch (SaaS), custom Python/Spark
  Streaming: Kafka / Kinesis / Pulsar / EventHub
```

Data System Design (Capstone)

```
  CDC:       Debezium / DMS / Fivetran CDC / Hudi DeltaStreamer
LAYER 2: STORAGE
  Lakehouse: Iceberg / Delta / Hudi on S3/GCS/Azure
  Warehouse: Snowflake / BigQuery / Redshift / Databricks SQL
  Hybrid:    Lakehouse + warehouse for hot serving
LAYER 3: PROCESSING
  Batch:    Spark (Databricks/EMR/Glue), dbt, BigQuery SQL
  Stream:   Flink, Spark Structured Streaming, Kafka Streams
  Orchestration: Airflow, Dagster, Prefect, dbt Cloud
LAYER 4: SERVING / MODELING
  Modeling: dbt (universal)
  Semantic: dbt semantic layer, Cube, LookML
  Serving:  Warehouse SQL, materialized views, BI Engine, ClickHouse for real-time
LAYER 5: CONSUMPTION
  BI:        Looker, Tableau, Power BI, Metabase, Sigma
  ML:        SageMaker, Vertex, Databricks ML
  Reverse-ETL: Hightouch, Census, Polytomic
  APIs:      Custom (FastAPI on warehouse), Cube, Snowflake / BQ direct
PICK PER LAYER based on workload — not "the modern stack" by reflex.
```

```
3.4 The Diagram Discipline
WHEN YOU DRAW:
  ✓ Boxes for components
  ✓ Arrows for data flow (directional)
  ✓ Labels on arrows (batch / stream / CDC)
  ✓ Boundaries for security / network zones
  ✓ Annotations for SLAs / volumes
DON'T:
  ✗ Vendor logos everywhere (let names speak)
  ✗ Too many colors (distracts)
  ✗ Aesthetic perfection (you have 60 min)
  ✗ More than ~15 components at top level (overwhelm)
GOOD DIAGRAMS HAVE LAYERS:
  Top of board: sources
  Middle: ingestion / processing
  Bottom: consumers
  Or left-to-right pipeline.
■ TIP: Narrate while you draw. "Sources here on the left. They flow into our ingestion layer — Kafka for
streaming, Fivetran for SaaS APIs. From there...". Quiet drawing is a missed opportunity to demonstrate your
thinking.
```

Data System Design (Capstone)

#### 4. Estimation & Capacity Planning

```
4.1 The Numbers Every Staff Engineer Should Know
LATENCY (rough order of magnitude):
  L1 cache:         0.5 ns
  L2 cache:         7 ns
  RAM:              100 ns
  SSD random read:  150 µs
  HDD seek:         10 ms
  Datacenter RTT:   0.5 ms
  Cross-region RTT: 50-150 ms
  Cross-continent:  150-300 ms
THROUGHPUT (rough):
  10 Gbps NIC:      ~1.25 GB/s
  S3 single object: ~100 MB/s, ~3500 GET/s per prefix
  Kafka broker:     500 MB/s - 1 GB/s
  Postgres write:   ~10K rows/s on commodity
  Snowflake L:      ~1 GB/s effective scan
STORAGE COSTS (rough monthly):
  S3 standard:      ~$23/TB
  S3 IA:            ~$13/TB
  S3 Glacier:       ~$4/TB
  Warehouse:        ~$23/TB compressed
  Lakehouse total:  storage + catalog (~$23/TB + minor)
```

```
4.2 Capacity Math Examples
EXAMPLE 1: STREAMING INGEST CAPACITY
  Spec: 50K events/sec, avg 1 KB/event, peak 3x average
  Peak: 50K × 3 × 1 KB = 150 MB/s ingest
```

```
  Kafka: 1 broker @ 100-500 MB/s
3 brokers needed
  Plus replication factor 3 → 9 brokers minimum (with HA)
  Daily volume: 50K × 86400 × 1 KB ≈ 4.3 TB/day raw
  With compression (3x): ~1.4 TB/day
EXAMPLE 2: WAREHOUSE STORAGE
  Daily 1.4 TB compressed × 7 days hot retention = 10 TB
  +90 days cool = 130 TB
  +1 year cold = 510 TB
  At $23/TB/month: ~$12K/month storage
EXAMPLE 3: QUERY COMPUTE
  100 analysts × 50 queries/day × avg 30 sec each on M warehouse
  M warehouse = 4 credits/hour
  Total compute hours: ~42 hours/day (assuming serialization on warehouse)
  Spread across 8-hour business day with multi-cluster: ~5 concurrent
  Cost: 4 × 8 × 22 (days) × $3 (credit) = ~$2,100/month single warehouse
```

```
4.3 The Quick Math Heuristics
ROW SIZE × ROWS PER SECOND × SECONDS PER DAY = DAILY BYTES
  1 KB × 50,000 × 86,400 ≈ 4 TB/day
DAILY × DAYS RETAINED × COMPRESSION FACTOR = TOTAL STORAGE
  4 TB/day × 365 × 0.3 (3x compression) ≈ 440 TB
CONCURRENT QUERIES / WAREHOUSE CONCURRENCY = WAREHOUSES NEEDED
  20 concurrent / 8 per cluster ≈ 3 clusters (multi-cluster)
PEAK FACTOR = 2-5x typical
  Plan capacity for peak, not average
  Auto-scaling helps if peak is brief
```

#### 4.4 Cost Estimation Framework

Data System Design (Capstone)

```
TYPICAL ENTERPRISE PLATFORM ROUGH BREAKDOWN:
STORAGE: 10-20% of total
  Lakehouse + warehouse + backups + archive
COMPUTE: 60-75% of total
  Warehouse credits / slots / DBUs / RPUs
  ETL processing (Spark, dbt)
  Streaming processing (Flink, Spark)
INGESTION: 5-15%
  Kafka, Fivetran, Airbyte, custom
EGRESS: 1-5% (can spike with bad architecture)
  Cross-region, cross-cloud
OBSERVABILITY: 2-5%
  DataDog, Monte Carlo, Elementary, etc.
LICENSES / SAAS: variable
  dbt Cloud, Looker, Tableau, etc.
→ ROUGH PLATFORM COSTS by company size:
  Series A startup:    $20-80K/year
  Mid-size:            $200K-1M/year
  Large enterprise:    $5-50M/year
```

```sql
4.5 The Right Precision
IN INTERVIEW:
  Don't aim for precise numbers. Aim for ORDERS OF MAGNITUDE.
  "Roughly 100 TB" is fine. "100.3 TB" is fake precision.
  Show the MATH ON THE BOARD:
    "1 KB × 50K × 86400 = ~4 TB/day"
  Even if rough, it demonstrates structured thinking.
WHEN INTERVIEWER PROBES:
  "How would that scale to 10x?"
  Don't recompute everything. Identify the bottleneck:
    "At 500K events/sec, Kafka would need 30+ brokers, and the warehouse
     ingest pattern would shift from MERGE to incremental partitions."
THE STAFF SIGNAL: comfortable with rough math, identifies bottlenecks,
                   doesn't get stuck on calculator-precision.
■ INTERVIEW ANGLE: Q: How would you estimate the cost of an architecture you just sketched?
Staff: Break it into the cost-bearing components — compute, storage, ingestion, egress — and estimate each.
Storage is easy: daily volume × retention × $/TB. Warehouse compute: think in credits/slots/DBUs per hour of
active workload, times hours per day. Streaming compute: Kafka cluster size + Flink jobs. Ingestion tools:
Fivetran is per-MAR; custom is engineer-time. I'd give a 3x range — 'between $200K and $600K/year, most likely
around $350K' — because at this stage precision is fake. The real value is identifying which component
dominates, which is the cost lever, and where the surprises hide (egress, time travel storage, etc.).
```

Data System Design (Capstone)

## PART II — ARCHITECTURAL PATTERNS

#### 5. Lambda vs Kappa

```
5.1 The Lambda Architecture (Marz, 2011)
LAMBDA = batch + speed layers in parallel.
  SOURCE
    ■
    ■■■→ BATCH LAYER (high latency, accurate)
    ■      Hadoop / Spark, computes daily/hourly
    ■      Master dataset on HDFS/S3
    ■      Re-computable from scratch
    ■
    ■■■→ SPEED LAYER (low latency, approximate)
    ■      Storm / Flink, processes streams
    ■      Computes recent data
    ■
    ▼
  SERVING LAYER (merges batch + speed views)
    Query merges authoritative batch + recent speed
WHY IT EXISTED:
  In 2011, batch (Hadoop) was correct but slow.
  Stream processors were fast but couldn't easily re-process.
  Lambda: get both via two separate pipelines.
THE PAIN:
  ✗ Two codebases (batch + stream) for the same business logic
  ✗ Bugs in only one path
  ✗ Operational complexity: two pipelines to monitor
```

```
5.2 The Kappa Architecture (Kreps, 2014)
KAPPA = ONE pipeline. Streaming all the way.
  SOURCE
    ■
    ▼
  KAFKA (durable log; replay possible)
    ■
    ▼
  STREAM PROCESSOR (Flink / Spark Streaming / Kafka Streams)
    ■
    ▼
  SERVING LAYER
RE-COMPUTATION:
  Replay events from Kafka with a new processor.
  Two stream processors run in parallel during transition.
  Cut over when new one matches.
WHY KAPPA:
  ✓ One codebase
  ✓ One operational pattern
  ✓ Streaming is the source of truth
```

WHY IT TOOK A WHILE: Required durable, replayable streams (Kafka with sufficient retention) Required stream processors that could handle batch-scale loads

#### 5.3 The Lakehouse Convergence

Data System Design (Capstone)

```
MODERN ANSWER (2020+):
  Lambda is dead in practice.
  Kappa is the conceptual ideal.
  LAKEHOUSE makes both batch and stream operate on the same data.
  SOURCES ■→ STREAMING INGEST ■→ LAKEHOUSE ■→ BATCH CONSUMERS
                                          ■→ STREAMING CONSUMERS
ONE STORAGE LAYER.
Multiple compute patterns (batch, stream, ad-hoc) on the same tables.
Re-processing = re-reading from the lakehouse (which retains history).
THE STAFF ANSWER for "Lambda or Kappa?":
  "Neither, really. Stream into a lakehouse with ACID; that's the modern pattern.
   Batch and stream both consume from there. Re-processing is just running
   a job at an older snapshot. The Lambda/Kappa distinction is a 2010s frame.
```

```
5.4 When You Still See Lambda
LEGACY LAMBDA STACKS still exist where:
   Historical investment in Hadoop batch + Spark Streaming
   Two teams own the two paths politically
   Real-time SLAs incompatible with lakehouse latency
INDICATORS OF GOOD MIGRATION TARGETS:
   Discrepancies between batch and stream results (the classic bug)
   Heavy maintenance burden
   Logic drift between the two paths
MODERN REPLACEMENT:
  Stream into a Delta/Iceberg lakehouse.
  Real-time consumers read via CDC streams (Delta CDF, Iceberg changelog).
  Batch consumers query the lakehouse as snapshots.
  Single logic path.
■ PRINCIPAL MINDSET: Lambda vs Kappa is a great litmus test. A senior engineer can describe both. A staff
engineer says: 'In 2024, neither — you stream into a lakehouse with ACID and serve both batch and streaming
consumers from there. Lambda was the right architecture in 2012 when stream processors couldn't be correct.
Kappa was right when Kafka could be the source of truth. Today the lakehouse subsumes both.' Knowing the
history shows depth; not being trapped by it shows judgment.
```

Data System Design (Capstone)

#### 6. The Medallion Architecture (Modern Default)

```
6.1 The Three Layers
POPULARIZED BY DATABRICKS, NOW NEAR-UNIVERSAL.
■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■
■ BRONZE (raw)                                         ■
■   Schema-on-read; preserves source as-is            ■
■   Append-only landings from ingestion               ■
■   Never deleted; source-of-truth for re-derivation  ■
■   Examples: raw.stripe.charges, raw.kafka.events    ■
■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■
                   ▼
■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■
■ SILVER (cleaned + conformed)                         ■
■   Schema-on-write; types enforced                   ■
■   Deduplicated, late-data handled                   ■
■   Joined with reference / lookup data               ■
■   dbt staging + intermediate layers                 ■
■   Examples: stg_orders, int_orders_enriched         ■
■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■
                   ▼
■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■
■ GOLD (business-ready)                                 ■
■   Dimensional models or wide marts                  ■
■   Aggregated, denormalized                          ■
■   Optimized for BI / ML / reverse-ETL               ■
■   dbt marts                                          ■
■   Examples: fct_revenue, dim_users, user_features   ■
■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■
OPTIONAL FOURTH LAYER:
  PLATINUM / PRESENTATION:
  Denormalized wide tables for specific consumers (Looker, ML).
```

```
6.2 Why Layers Matter
WITHOUT LAYERING:
  Each team queries raw, defines its own metrics.
  Same KPI computed 5 different ways.
  Source schema change breaks 100 downstream queries.
  Re-derivation is impossible (no source of truth captured).
WITH LAYERING:
   Bronze NEVER changes (source of truth, append-only).
   Silver handles change ONCE for all consumers.
   Gold provides ONE definition per metric.
   Schema changes ripple predictably through layers.
   Re-derivation: re-run silver+gold from bronze.
THIS IS ARCHITECTURE, NOT JUST CONVENTION.
```

```
6.3 Layer-by-Layer Decisions
BRONZE:
  Storage: lakehouse (Iceberg/Delta) on S3
  Format: as-ingested + metadata columns (ingest_ts, source, batch_id)
  Partition: by ingest date
  Retention: long (months/years; cheap)
  Ownership: data engineering / platform team
  Tests: minimal (schema validation, freshness, row count vs source)
SILVER:
  Storage: lakehouse (same store, different schema)
  Format: typed, conformed
  Partition: by business date (event_date, etc.)
  Retention: medium (months; aligned with reprocessing needs)
  Ownership: data engineering / domain teams
  Tests: thorough (unique PKs, FKs, business rules)
```

Data System Design (Capstone)

```
GOLD:
  Storage: warehouse OR lakehouse (depending on consumers)
  Format: dimensional or wide
  Partition: by query pattern
  Retention: aligned with BI needs (1-5 years typical)
  Ownership: domain teams / analytics engineers
  Tests: comprehensive (statistical anomalies, contracts)
```

```
6.4 The Ingestion-to-Consumption Flow
TYPICAL DATA PATH:
  KAFKA (stream) ■■→ Flink/Spark Streaming ■■→ BRONZE LAKEHOUSE
                                                     ■
                                                     ▼
  Fivetran/Airbyte ■■→ landing tables ■■■■■■→ BRONZE LAKEHOUSE
                                                     ■
                                                     ▼
                                            SILVER (dbt staging)
                                                     ■
                                                     ▼
                                          SILVER (dbt intermediate)
                                                     ■
                                                     ▼
                                             GOLD (dbt marts)
                                                     ■
                              ■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■
                              ▼                      ▼                      ▼
                          BI tools             Reverse-ETL              ML / feature store
                          (Looker)             (Hightouch)              (Feast, etc.)
```

```
6.5 Common Medallion Mistakes
■ BRONZE WITH SCHEMA ENFORCEMENT
  Defeats the point. Bronze should accept anything from source.
■ SILVER MISSING — JUMP FROM BRONZE TO GOLD
  Same logic duplicated across gold models.
  No reusable building blocks.
■ GOLD WITHOUT MODELING DISCIPLINE
  Just wide tables with no dimensional structure.
  Analysts can't reason about it.
■ BRONZE WITH UPDATES / DELETES
  Defeats the immutable-source-of-truth principle.
  Re-derivation becomes impossible.
■ ALL THREE LAYERS IN ONE PHYSICAL PLACE WITH NO BOUNDARIES
  No permissions / contracts between layers.
  Anyone can read raw; no governance.
■ FORGETTING THE METADATA COLUMNS
  Bronze needs: ingest_ts, source, batch_id, schema_version
  Without these, debugging is impossible.
■ PRODUCTION SCENARIO: A team had bronze, silver, and gold tables in one Snowflake database, all with the
same permissions. An analyst accidentally wrote a query against bronze that bypassed all the cleanup logic in
silver. Their dashboard showed different numbers than the official one. Trust took months to rebuild. Lesson:
enforce permission boundaries between medallion layers. Most consumers should only see gold. Silver for
analytics engineers. Bronze for platform team.
```

Data System Design (Capstone)

#### 7. Data Mesh, Data Vault & OBT

7.1 Data Mesh (Dehghani, 2019) DATA MESH = decentralized data architecture organized around DOMAINS. PRINCIPLES:

#### 1. Domain ownership

#### 2. Data as a product

#### 3. Self-serve data platform

```
  4. Federated computational governance
     Org-wide rules (PII, compliance) enforced everywhere automatically.
CONTRAST WITH CENTRALIZED:
  Central data team builds everything → bottleneck at company scale.
  Mesh: each domain has its own data engineering capacity.
WHEN IT MAKES SENSE:
  ✓ Large org (50+ data engineers, multiple domains)
  ✓ Strong central platform with good developer experience
  ✓ Mature data culture (teams can be trusted to own data)
```

```
WHEN IT FAILS:
  ✗ Small org without domain expertise
  ✗ No central platform → mesh becomes anarchy
  ✗ Teams treat 'ownership' as 'do whatever; no standards'
```

```
7.2 Data Vault 2.0
DATA VAULT = enterprise modeling pattern, source-system agnostic.
  HUBS:     business entity by business key (customer, order, product)
  LINKS:    relationships between hubs
  SATELLITES: descriptive attributes, history-preserved
INSERT-ONLY DESIGN:
  ✓ Highly parallelizable loads
  ✓ Auditable (full history by construction)
  ✓ Schema-evolution friendly (new satellites)
VAULT IS A SILVER-LAYER pattern, typically.
GOLD layer (Kimball / OBT) sits on top for consumers.
WHEN TO USE:
  ✓ Enterprise (10+ source systems, complex integration)
  ✓ Strict regulatory audit (SOX, financial)
  ✓ Long-lived warehouse (10+ years)
```

```
WHEN NOT TO:
  ✗ Small / simple warehouses
  ✗ Analyst-direct-query workloads (Vault joins are painful)
```

```
7.3 One Big Table (OBT)
OBT = denormalized wide table per entity.
  user_360:
    user_id, email, signup_date, ...,
    total_orders, lifetime_revenue, churn_risk_score,
    [200+ columns]
WHEN OBT WINS:
  ✓ Feature stores for ML
  ✓ Specific BI use cases (one big dashboard table)
  ✓ Columnar engines (only scan used columns)
WHEN OBT LOSES:
```

Data System Design (Capstone)

```
  ✗ Multiple consumer use cases (denormalize differently)
  ✗ Frequent schema changes
  ✗ History matters (history × width = unwieldy)
```

```
7.4 Picking the Modeling Approach
LAYER-BY-LAYER:
  BRONZE:  no modeling — preserve raw
  SILVER:  Data Vault OR 3NF (audit-friendly source-of-truth)
            OR clean staging (simpler shops)
  GOLD:    Kimball star schema (BI workloads)
            OR OBT / Activity Schema (ML / specific use cases)
            OR both, with stars feeding OBT presentation tables
ORGANIZATIONALLY:
  Centralized small team → simple staging + Kimball gold
  Decentralized domains → Data Mesh with Kimball or OBT per domain
  Enterprise with audit → Data Vault silver + Kimball gold
THE STAFF ANSWER: rarely one-size-fits-all.
  Pick the modeling approach BY LAYER and BY DOMAIN.
  Most large warehouses are layered hybrids.
■ INTERVIEW ANGLE: Q: Should we adopt Data Mesh?
Staff: Depends on scale and maturity. Mesh works when you have 50+ data engineers across multiple domains
AND a strong central platform team providing self-serve infrastructure. At smaller scale, it's premature
decentralization — you don't have enough domain expertise to staff it, and you end up with inconsistent tooling
and quality. Most organizations under 200 engineers are better served by a centralized team with domain-specific
models. Mesh is the right answer for FAANG-scale; it's the wrong answer for most companies that adopt it
because of hype. The conversation should start with: 'what problem are you trying to solve?' If the answer is
'central team is a bottleneck', mesh might help; if it's 'we want to be modern', it won't.
```

Data System Design (Capstone)

#### 8. Event-Driven & CQRS Patterns

```
8.1 Event-Driven Architecture
EVENT-DRIVEN = services emit EVENTS for state changes;
                 other services subscribe and react.
  Service A: UserRegistered event
             ■
             ▼
  Kafka topic: user_events
             ■
       ■■■■■■■■■■■■■
       ▼     ▼     ▼
  Welcome   Analytics   ML
  email     loader      feature update
BENEFITS:
  ✓ Loose coupling between producers and consumers
  ✓ Easy to add new consumers (just subscribe)
  ✓ Audit log built-in (events are durable history)
  ✓ Resilient to slow consumers (Kafka buffers)
  ✓ Replay-able (re-process history with new logic)
CHALLENGES:
  ✗ Schema evolution coordination (use Schema Registry)
  ✗ Eventual consistency (no synchronous read-your-write)
  ✗ Cross-service queries are hard
  ✗ Debugging distributed flows
```

```
8.2 CQRS (Command Query Responsibility Segregation)
CQRS = separate WRITE model from READ model.
  WRITE SIDE (commands):
    User → POST /place-order → service writes to its OLTP DB
                              → emits OrderPlaced event
  READ SIDE (queries):
    Multiple consumers subscribe to OrderPlaced events.
    Each maintains its OWN read model optimized for its queries.
     Search service: indexes orders for search
     Analytics: loads into warehouse
     Reporting: maintains aggregates in materialized views
     CRM: updates customer LTV
WHY:
  ✓ Read models optimized per use case
  ✓ Write side doesn't block under read load
  ✓ Independent scaling
  ✓ Different consistency per read model
THE COST:
  ✗ Eventual consistency (read may lag write)
  ✗ More moving parts
  ✗ Coordination of schemas / events
```

```
8.3 The Outbox Pattern
THE PROBLEM:
  Service writes to OLTP DB AND publishes Kafka event.
   If DB commits but Kafka fails: lost event
   If Kafka publishes but DB rollbacks: ghost event
   No distributed transaction across both systems
OUTBOX PATTERN:
```

```
  1. Service writes to OUTBOX TABLE in the SAME DB transaction:
       BEGIN;
         INSERT INTO orders (...) VALUES (...);
```

Data System Design (Capstone)

```
         INSERT INTO outbox (event_type, payload, status)
            VALUES ('OrderPlaced', {...}, 'pending');
       COMMIT;
```

```
  2. A separate process (or CDC) reads outbox and publishes to Kafka.
      Polling: simple, can have higher latency
      CDC on outbox: real-time, more infrastructure
GUARANTEES:
  At-least-once: Kafka event for every DB change
  No ghost events: no event without DB change
  Idempotency required: downstream may see duplicates
THIS IS THE STAFF-LEVEL ANSWER for "how do you publish reliably from a
transactional service?". Anything else has correctness gaps.
```

```sql
8.4 Event Sourcing
EVENT SOURCING = storing STATE as a sequence of EVENTS, not current state.
  Traditional: orders table has current row
  Event-sourced: events table has all changes; current state derived by replay
  events:
    1: OrderPlaced  (order_id=X, items=[...], total=$100)
    2: OrderShipped (order_id=X, carrier='UPS', tracking='123')
    3: OrderDelivered (order_id=X, delivered_at='2024
15')
  Current state of order X = fold over its events.
WHEN IT WINS:
  ✓ Audit-heavy domains (finance, healthcare)
  ✓ Complex business state machines
  ✓ Need historical state reconstruction
  ✓ Multiple consumers building different views
WHEN IT LOSES:
  ✗ Simple CRUD where current state is enough
  ✗ Read-heavy workloads without snapshot strategy
  ✗ Teams unprepared for the operational complexity
EVENT-SOURCED SERVICES often have:
   Events as the source of truth (in Kafka or DB)
   Periodic snapshots for fast state reconstruction
   Read models materialized from events (CQRS)
■ PRINCIPAL MINDSET: Event-driven architectures + outbox + CQRS are the staff-level toolkit for distributed
transactional systems. They aren't always needed — most CRUD services don't justify the complexity. But when
you have multiple downstream consumers with different needs, when reliability of cross-service consistency
matters, or when audit history is a feature, they're the answer. The biggest mistake is using them prematurely;
the second biggest is NOT using outbox when you need cross-system consistency.
```

Data System Design (Capstone)

## PART III — INGESTION PATTERNS

#### 9. Batch Ingestion at Scale

```sql
9.1 The Batch Ingestion Spectrum
MANUAL UPLOADS (rare, but exists):
  Analyst uploads CSV via UI to land in warehouse.
SCHEDULED FILE TRANSFERS (legacy):
  SFTP / S3 file drop, ingested via Airflow DAG.
SAAS CONNECTORS:
  Fivetran, Airbyte, Stitch — sync data from SaaS APIs.
CUSTOM ETL:
  Spark / Python jobs reading from various sources.
DATABASE DUMPS:
  Snapshot exports (pg_dump, MySQL dump) → load.
OPERATIONAL DB FULL SYNCS:
  SELECT * from prod, write to warehouse. Periodic refresh.
  Replaced by CDC for most modern workloads.
```

```
9.2 SaaS Connectors
FIVETRAN, AIRBYTE, STITCH — managed (or open-source) ETL.
  Source: Salesforce / Stripe / Zendesk / HubSpot / ...
  Sync:   incremental (via source-specific change tracking)
  Schema: auto-detected; evolves with source
  Destination: warehouse / lakehouse
PROS:
  ✓ Pre-built connectors (100s of sources)
  ✓ Schema evolution handled
  ✓ Managed = no DBA work
  ✓ Compliance certifications
CONS:
  ✗ Pricing scales with volume (can be expensive at scale)
  ✗ Limited transformation (just E and L; T happens in dbt)
  ✗ Schema drift can break downstream
  ✗ Some sources have rate limits / API costs
WHEN TO USE:
  ✓ SaaS sources (less reason to build custom)
  ✓ Lower data volumes
  ✓ Team without ETL engineering capacity
WHEN TO BUILD CUSTOM:
  ✓ Very high volume (cost crossover)
  ✓ Custom transformations during ingest
  ✓ Sources without good off-the-shelf connectors
```

  - 9.3 Custom Batch ETL
- TYPICAL CUSTOM BATCH PATTERN:
- 1. AIRFLOW DAG TRIGGERED ON SCHEDULE (e.g., hourly)
- 2. SPARK JOB READS SOURCE
- - Database (JDBC), files (S3), API
- 3. APPLIES BUSINESS LOGIC

Data System Design (Capstone)

```
     - Filter, transform, deduplicate
  4. WRITES TO LAKEHOUSE / WAREHOUSE
     - Append-only or MERGE
  5. EMITS METRICS + LOGS
  6. TRIGGERS DOWNSTREAM (dbt run, etc.)
ORCHESTRATION:
  Airflow:    most common; mature; declarative DAGs
  Dagster:    asset-centric; data-aware; newer
  Prefect:    Python-native; flexible
  dbt Cloud:  simple cases (dbt-only)
LARGE PATTERN:
  Airflow orchestrates → Spark on Kubernetes / EMR / Databricks runs job
  Spark writes to lakehouse → triggers downstream dbt run → notifies BI tools
```

```sql
9.4 Incremental vs Full Loads
FULL LOAD:
  Re-extract everything every run. Simple but expensive.
  Good for: small tables, occasional schemas, when you can't track changes.
INCREMENTAL LOAD:
  Only new/changed rows since last sync.
  Requires: updated_at column, sequence ID, or CDC.
  HIGH-WATER MARK PATTERN:
    SELECT * FROM source
    WHERE updated_at > <last_run_max_updated_at>
  PROBLEMS:
     Late-arriving rows (timestamp in past after run) → lost
     Clock skew between source and ingest
     Deletes invisible (source row gone; can't compare)
```

```
  SOLUTIONS:
     Lookback window (re-process last N hours/days)
     CDC for deletes
     Periodic full refresh for self-healing
```

```
9.5 Schema Evolution in Batch
COMMON ISSUES:
  Source adds a column → ingest doesn't pick it up
  Source renames column → downstream models break
  Source changes type (int → bigint) → cast errors
PATTERNS:
  AUTO-EVOLVE:
    Lakehouse / warehouse accepts new columns automatically
    Old rows have NULL for new columns
    Downstream models handle gracefully
  CONTRACT-FIRST:
    Schema registered explicitly (Avro / Protobuf via Schema Registry)
    Schema changes go through review
    Breaking changes blocked
  HYBRID (recommended):
    Bronze layer auto-evolves
    Silver / gold layers have contracts
    Translations from bronze → silver handle schema drift
```

Data System Design (Capstone)

#### 10. Streaming Ingestion End-to-End

```
10.1 The Streaming Pipeline
TYPICAL STREAMING PIPELINE:
  PRODUCERS (apps, services)
       ■
       ▼
  KAFKA / KINESIS / PULSAR (durable buffer)
       ■
       ▼
  STREAM PROCESSOR (Flink / Spark Streaming / Kafka Streams)
       ■
       ▼
  SINK (lakehouse / warehouse / search / cache)
       ■
       ▼
  CONSUMERS (BI / ML / APIs)
```

```
10.2 The Sources
1. APPLICATION EVENTS
   Producer SDK in service code
   Emit on user actions, state changes
   Examples: page views, clicks, signups
2. CDC FROM DATABASES
   Debezium / DMS reads WAL
   Each DB change → Kafka event
   Examples: order changes, user profile updates
3. IOT / TELEMETRY
   Devices emit measurements
   Often via MQTT → Kafka bridge
4. WEBHOOKS / API STREAMS
   External services push events
   Receiving service forwards to Kafka
5. LOG STREAMS
   Application logs, system metrics
   Via Fluentd / Filebeat / Vector → Kafka
```

```
10.3 The Buffer Layer
KAFKA (most common):
  ✓ High throughput (MB/s per broker)
  ✓ Long retention (days/weeks)
  ✓ Replay possible
  ✓ Multiple consumer groups
  ✓ Self-hosted or managed (Confluent Cloud, MSK)
  ✗ Operational complexity at scale
KINESIS (AWS):
  ✓ Fully managed
  ✓ Tight AWS integration
  ✗ Less flexible than Kafka (24h default retention; up to 365 days extended)
  ✗ Per-shard throughput limits
PULSAR:
  ✓ Multi-tenancy native
  ✓ Geo-replication
  ✓ Tiered storage
  ✗ Smaller ecosystem
EVENTHUB (Azure):
  ✓ Managed Azure-native
  ✓ Kafka-compatible API
  ✗ Azure-only
```

Data System Design (Capstone)

```
DECISION:
  Kafka if multi-cloud or self-hosted preference
  Kinesis if all-AWS and want managed
  Confluent Cloud (Kafka) if want Kafka without ops
```

```
10.4 The Stream Processor
FLINK:
  ✓ Best-in-class for complex stream processing
  ✓ True event-time + watermarks
  ✓ Exactly-once with checkpoints
  ✓ Multiple language APIs (Java, Python, SQL)
  ✓ Iceberg / Delta sinks first-class
  ✗ Operational complexity (separate cluster)
SPARK STRUCTURED STREAMING:
  ✓ Spark ecosystem; same engine as batch
  ✓ Good for Databricks-centric shops
  ✓ Micro-batch model (not true streaming)
  ✗ Higher latency than Flink
  ✗ Some limitations on stateful operators
KAFKA STREAMS:
  ✓ Library, not separate cluster
  ✓ Great for Kafka-in, Kafka-out
  ✗ JVM only
  ✗ Limited compared to Flink
KSQLDB:
  ✓ SQL interface over Kafka Streams
  ✓ Quick prototyping
  ✗ Confluent-tied; less mature than Flink SQL
DECISION:
  Kafka in, Kafka out, JVM team → Kafka Streams
  Multi-source, complex state, sub-second → Flink
  Spark shop, micro-batch OK → Spark Streaming
```

```
10.5 The Sinks
LAKEHOUSE (Iceberg / Delta):
  Flink + Iceberg = best modern stack
  Spark Streaming + Delta = Databricks-native
  Sub-minute freshness; full ACID
REAL-TIME OLAP STORES:
  ClickHouse: best for real-time dashboards (sub-second queries)
  Druid:      time-series analytics; older but proven
  Pinot:      LinkedIn-origin; user-facing analytics
  StarRocks:  newer; OLAP at scale
CACHES / KEY-VALUE:
  Redis: hot lookup data
  DynamoDB: serving cached results
SEARCH:
  Elasticsearch / OpenSearch: full-text + analytics
PUB/SUB DOWNSTREAM:
  Forward to other Kafka topics for cascading consumers
```

```
10.6 End-to-End Latency Budget
TARGET: 5-second end-to-end freshness for dashboards
BUDGET:
  Producer → Kafka:        100 ms
  Kafka retention:          (constant; not latency)
  Flink processing:         500 ms - 2 s
```

Data System Design (Capstone)

```
  Flink → lakehouse commit: 1-3 s (checkpoint interval)
  Lakehouse → BI query:     1-2 s
  Total:                    3-7 s
SUB-SECOND TARGET (e.g., real-time dashboard):
  Lakehouse path WON'T work — minute-scale at best.
  Need ClickHouse / Druid as serving layer.
  Producer → Kafka:         50 ms
  Flink processing:         200 ms
  Flink → ClickHouse:       100 ms
  ClickHouse query:         200 ms
  Total:                    ~550 ms
UNDERSTAND THE BUDGET BEFORE PICKING TOOLS.
■ INTERVIEW ANGLE: Q: Design streaming ingest for 50K events/sec with 5-second freshness target.
Staff: Kafka cluster sized for 150 MB/s peak (3x for safety) with RF=3 — call it 6 brokers on the safe side. Topics
partitioned by user_id for ordering; 32-64 partitions for downstream parallelism. Flink consumes with 1-second
checkpoints, applies any business logic, writes to Iceberg with 1-2 second commit intervals. Iceberg's hidden
partitioning by hour keeps the table manageable. Compaction job runs every 15 minutes to keep file sizes
healthy. Total latency budget: ~3-5 seconds. Trade-offs: I chose Flink over Spark Streaming because of the
lower-latency commits needed. I chose Iceberg over Delta because we want Snowflake to query it later. If the
freshness target tightened to sub-second, the lakehouse path breaks; we'd add ClickHouse as a real-time serving
layer alongside the lakehouse for historical analysis.
```

Data System Design (Capstone)

11. CDC: The Complete Picture

11.1 What CDC Is Change Data Capture is the pattern for replicating changes from a transactional database into analytical or other systems in near-real-time. It's one of the most important architectural patterns in modern data engineering, replacing nightly batch dumps of operational data.

```
11.2 The Full Pipeline
SOURCE DB (Postgres / MySQL / MongoDB / SQL Server / Oracle)
       ■
       ■ Logical replication / binlog / oplog
       ▼
DEBEZIUM (or AWS DMS / Fivetran CDC / Striim)
       ■
       ■ Change events with before / after / op
       ▼
KAFKA TOPIC (one per source table)
       ■
       ■ Schema Registry tracks schema
       ▼
STREAM PROCESSOR (Flink / Spark / DeltaStreamer)
       ■
       ■ Apply MERGE / UPSERT
       ▼
LAKEHOUSE TABLE (Iceberg / Delta / Hudi)
       ■
       ■ Reflects current source state
       ▼
DOWNSTREAM: BI, ML, reverse-ETL, analytics
```

```
11.3 The Change Event
DEBEZIUM EVENT FORMAT (simplified):
  {
    "before": {
      "id": 42,
      "name": "Alice",
      "email": "old@x.com",
      "updated_at": "2024
14T10:00:00"
    },
    "after": {
      "id": 42,
      "name": "Alice",
      "email": "new@x.com",
      "updated_at": "2024
15T11:23:00"
    },
    "op": "u",  // c=insert, u=update, d=delete, r=snapshot read
    "ts_ms": 1710501780000,
    "source": {
      "db": "production",
      "schema": "public",
      "table": "users",
      "lsn": "0/1A2B3C4D",
      "txId": 12345
    }
  }
   "before" / "after" let you compute deltas
   "op" tells you the operation
   "source" provides LSN / position for ordering
```

  - 11.4 Initial Snapshot + Ongoing Stream
- CDC NEEDS TO HANDLE:
- 1. INITIAL SNAPSHOT — current state of the table when CDC starts

Data System Design (Capstone)

```
  2. ONGOING CHANGES — every change since snapshot
DEBEZIUM'S APPROACH:
   Acquires a brief lock to record current LSN
   Streams a snapshot of current rows with op='r'
   Then streams ongoing changes from that LSN forward
CONSUMER MUST HANDLE BOTH:
  Snapshot events: just INSERT (or initial MERGE on PK)
  Change events: MERGE based on op type
ONE-OFF CONCERNS:
   Snapshot can be huge (hours for big tables)
   Source DB load during snapshot
   Snapshot consistency vs ongoing changes window
```

```sql
11.5 The MERGE Pattern in the Sink
TYPICAL FLINK / SPARK SINK LOGIC:
  MERGE INTO target tgt
  USING (
    -- dedup within batch: take latest per key
    SELECT *
    FROM cdc_batch
    QUALIFY ROW_NUMBER() OVER (PARTITION BY id ORDER BY ts_ms DESC) = 1
  ) src
  ON tgt.id = src.id
  WHEN MATCHED AND src.op = 'd' THEN DELETE
  WHEN MATCHED AND src.op IN ('u', 'r') THEN UPDATE SET ...
  WHEN NOT MATCHED AND src.op IN ('i', 'r') THEN INSERT ...
KEY DETAILS:
   Dedup within batch (multiple events per key possible)
   Handle delete events
   Snapshot 'r' events treated as insert OR update (idempotent)
   Idempotent operation (replay-safe)
```

#### 11.6 The Soft-Delete Question

```
HARD DELETE:
  ✓ Reflects source faithfully
  ✗ Loses history ("who got deleted when?")
  ✗ Harder GDPR audit
  ✓ Easier GDPR right-to-be-forgotten
SOFT DELETE (add deleted_at column):
  ✓ Audit trail preserved
  ✓ Can detect "recently deleted" cohorts
  ✗ Storage grows
  ✗ Consumers must filter
HYBRID (recommended):
  Bronze: soft-delete (preserve everything)
  Silver: soft-delete (clean handling)
  Gold:  hard-delete (consumers see clean state)
```

```
11.7 CDC Architecture Decisions
CDC TOOL:
  Open source: Debezium (most flexible)
  AWS-native: DMS (managed, less flexible)
  SaaS: Fivetran CDC, Striim, Striim, others
  Cloud-native: Snowflake Streams (in-warehouse CDC)
DESTINATION:
  Lakehouse: Iceberg / Delta with MERGE
  Warehouse: Snowflake / BigQuery via streaming inserts
```

Data System Design (Capstone)

```
  Hybrid: lakehouse for source-of-truth + warehouse for BI
DESIGN AXES:
   One topic per table or one combined topic?
    → One per table (cleaner schema, independent consumers)
```

```
   Compacted topics or time-retention?
    → Compacted for current-state CDC; time-retention for event-history
```

```
   Sink directly to warehouse or through lakehouse?
    → Through lakehouse for flexibility + cost
    → Direct to warehouse if BI is the only consumer
■ PRODUCTION SCENARIO: A team used Debezium to CDC a 500 GB Postgres table. The initial snapshot
took 18 hours and held a brief lock — long enough to crash the production DB. They had to roll back, set up a
read replica, run Debezium against the replica, and use 'never-lock' mode. The replica trick is the standard
answer for big tables, but it has its own gotchas (replication lag, additional infra). Always test CDC initial
snapshots on a non-prod replica before pointing at production.
```

Data System Design (Capstone)

#### 12. Idempotency & Exactly-Once Semantics

```
12.1 The Three Delivery Semantics
AT-MOST-ONCE:
  Each event delivered 0 or 1 times. May be lost.
  Use only for: lossy metrics, telemetry where loss is OK.
AT-LEAST-ONCE:
  Each event delivered 1+ times. May be duplicated.
  THE COMMON DEFAULT. Combined with idempotent processing = effectively exactly-once.
EXACTLY-ONCE:
  Each event delivered exactly 1 time. No loss, no duplicates.
  Requires: transactional producer + consumer + state coordination.
  Possible WITHIN a system (Kafka exactly-once), HARD across systems.
```

```
12.2 Why Idempotency Matters
IN DISTRIBUTED SYSTEMS:
   Network retries cause duplicates
   Crash recovery replays events
   Out-of-order delivery happens
IDEMPOTENT OPERATION:
  Applying it multiple times produces the same result as applying once.
```

```sql
  IDEMPOTENT:
    UPSERT INTO users (id, name) VALUES (42, 'Alice');
    UPDATE counts SET total = (SELECT COUNT(*) FROM events) WHERE day = today;
    SET cache[key] = value;
```

```sql
  NOT IDEMPOTENT:
    INSERT INTO logs (event) VALUES ('clicked');     -- creates duplicates
    UPDATE counts SET total = total + 1;             -- double-counts on retry
    SEND email to user                               -- sends twice
```

  - 12.3 Idempotency Patterns
- 1. UNIQUE KEY / UPSERT
- Use a unique identifier; insert with ON CONFLICT DO NOTHING / UPDATE

```sql
   INSERT INTO events (event_id, ...) VALUES (...) ON CONFLICT (event_id) DO NOTHING;
2. IDEMPOTENCY TOKEN
   Caller passes a unique token with each request.
   Server checks: have I seen this token? If yes, return cached result.
   Stripe API: idempotency_key header.
3. DERIVE STATE, NOT MUTATE
   Compute counts from source-of-truth, not by incrementing.
   "Today's revenue = SUM(amount) FROM orders WHERE date = today"
   Re-runs produce the same result.
4. MERGE INSTEAD OF INSERT
   For tables that track entity state:
     MERGE on PK; insert if not exists, update if exists.
   Re-running with same input = same output.
5. EVENT-TIME-DEDUPLICATION
   Two events with same event_id → keep one (idempotent dedup)
   Built into Kafka Streams, Flink, Hudi.
6. COMMUTATIVE OPERATIONS
   Operations whose order doesn't matter (sum, max, set union)
   Replaying produces same result regardless of order.
```

#### 12.4 End-to-End Exactly-Once

Data System Design (Capstone)

```
KAFKA EXACTLY-ONCE (within Kafka):
  Producer: idempotent + transactional
  Consumer: read_committed isolation
  Result: exactly-once within Kafka topic-to-topic processing
KAFKA + EXTERNAL SYSTEM:
  Pure exactly-once requires distributed transaction across systems.
  Kafka can't do that with most external systems.
```

```
  PRACTICAL ANSWER:
    At-least-once + idempotent sink = effectively exactly-once.
```

```
  PATTERNS:
    Kafka → Flink → Iceberg: 2PC between Flink and Iceberg = exactly-once
    Kafka → Spark Streaming → Delta: checkpoint-based idempotency
    Kafka → Snowflake: at-least-once + MERGE-based loading = effectively exactly-once
THE STAFF ANSWER:
  Almost no production system needs "true" exactly-once.
  At-least-once + idempotent downstream is the right pattern 95% of the time.
  Real exactly-once (Kafka transactions, Flink 2PC) only when state participates.
```

```sql
12.5 The Late-Data Question
IN STREAMING:
  Events arrive out of order or after the watermark passes.
OPTIONS:
  1. DROP late events (simple, lossy)
  2. SIDE-OUTPUT late events to a 'late' topic for separate handling
  3. ALLOWED LATENESS — keep window open longer; re-emit on late data
  4. ACCEPT into a separate "late corrections" table
IN BATCH:
  Lookback window in incremental models (re-process recent days)
  MERGE-based loading handles updates of past data
THE PATTERN:
  Bronze: accept everything, including late
  Silver: reprocess windows where late data has likely arrived
          (e.g., last 7 days every night)
  Gold: stable view (after silver reprocessing settles)
ARCHITECTURALLY:
  Late data is normal, not an exception. Design for it from day 1.
■ PRINCIPAL MINDSET: Idempotency is the single most important pattern for building reliable distributed data
systems. 'Exactly-once' is mostly a myth across system boundaries; what works is 'at-least-once + idempotent
processing'. Every staff-level design should make idempotency a first-class concern: unique keys, UPSERT
semantics, derived-from-source-of-truth aggregates. Teams that skip this build pipelines that produce subtly
wrong numbers — and spend years not knowing why.
```

Data System Design (Capstone)

## PART IV — STORAGE & PROCESSING DECISIONS

#### 13. Warehouse vs Lakehouse vs Hybrid

```
13.1 The Decision Framework
WAREHOUSE WINS WHEN:
  ✓ Analyst-first culture
  ✓ Strong SQL skill set
  ✓ BI is the dominant workload
  ✓ Want minimum operational overhead
  ✓ Don't need multi-engine access
  ✓ Smaller scale (<100 TB hot data)
LAKEHOUSE WINS WHEN:
  ✓ Multi-engine access (Spark, Trino, BI, ML)
  ✓ Petabyte-scale storage
  ✓ Open format requirements (no vendor lock-in)
  ✓ Heavy ML / Python workloads
  ✓ Streaming-native writes
  ✓ Cost-sensitive at scale
HYBRID WINS WHEN:
  ✓ Mixed workloads (BI + ML + streaming)
  ✓ Want lake economics + warehouse UX
  ✓ Petabyte historical + terabyte hot
  ✓ Multiple consumer types
```

```
13.2 The Modern Hybrid Architecture
TYPICAL ENTERPRISE 2024+ PATTERN:
  STREAMING + BATCH INGEST
            ↓
  ■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■
  ■ LAKEHOUSE (Iceberg / Delta on S3)         ■
  ■                                            ■
  ■ Bronze + Silver layers                     ■
  ■ ML feature stores                          ■
  ■ Long retention, cheap storage              ■
  ■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■
           ■                 ■
           ▼                 ▼
  ■■■■■■■■■■■■■■■■    ■■■■■■■■■■■■■■■■
  ■ WAREHOUSE    ■    ■ SPARK / ML   ■
  ■ (Snowflake / ■    ■              ■
  ■  BigQuery)   ■    ■ Feature gen, ■
  ■              ■    ■ model train  ■
  ■ Gold marts   ■    ■              ■
  ■ BI queries   ■    ■              ■
  ■■■■■■■■■■■■■■■■    ■■■■■■■■■■■■■■■■
         ■
         ▼
       BI / DASHBOARDS / REVERSE-ETL
```

13.3 The Layer-by-Layer Choice Layer Lakehouse-first Warehouse-first Hybrid Bronze Iceberg/Delta on S3 External tables on S3 Lakehouse Silver Iceberg/Delta + Spark/Flink Warehouse tables Lakehouse

Data System Design (Capstone)

Gold Lakehouse + multi-engine Warehouse-native marts Warehouse for BI Serving Trino + ClickHouse Warehouse + BI Engine Warehouse ML Lakehouse + Spark/Polars Snowpark / BQ ML Lakehouse

#### 13.4 The Migration Patterns

#### 1. Set up lakehouse storage (Iceberg) on cloud bucket

#### 2. New tables / pipelines write to lakehouse

#### 3. Warehouse reads from lakehouse via external tables

#### 4. Gradually migrate cold data out of warehouse storage

```
  5. Eventually: warehouse is "managed compute on lakehouse"
LAKEHOUSE-ONLY → HYBRID (less common):
  Add a warehouse for BI workloads where lakehouse query latency lags
  Materialized gold tables to warehouse
  Reverse-ETL or direct warehouse queries on lakehouse data
DOWN ROAD:
  The line between warehouse and lakehouse is blurring.
  Snowflake reads external Iceberg natively.
  BigQuery has Iceberg managed tables.
  Databricks SQL is lakehouse-native.
  By 2026, the choice may be philosophical, not architectural.
```

```
13.5 What Changes the Decision
ECONOMIC:
  Lakehouse storage is 5-10x cheaper at scale
  → at PB scale, lakehouse wins on storage alone
ECOSYSTEM:
  Multiple engines need to read data
  → lakehouse wins (open formats)
PEOPLE:
  Analyst-heavy team
  → warehouse UX is better
  Engineering / ML-heavy team
  → lakehouse fits workflows
REGULATORY:
  Multi-region with strict data residency
  → both can do it; lakehouse + open formats are more portable
TIME:
  Greenfield in 2025+
  → lean lakehouse-first; add warehouse only when BI demands it
■ INTERVIEW ANGLE: Q: When would you NOT use a lakehouse?
Staff: Small scale, simple workload, analyst-heavy team. If you have 5 TB of data, 20 analysts, no ML, no
streaming, and a 3-person data team — the operational overhead of lakehouse (catalogs, compaction, schema
evolution coordination) is wasted. A pure Snowflake or BigQuery setup is simpler and faster to get value from.
Lakehouses pay off when scale, multi-engine access, ML workflows, or streaming demands justify the
operational complexity. I'd guess that's roughly: above 100 TB, with ML or streaming, with multiple downstream
engine types. Below that, pure warehouse is often the right call.
```

Data System Design (Capstone)

#### 14. The Modeling Layer Decision

```
14.1 The Modeling Decision Tree
STEP 1: WHO ARE THE CONSUMERS?
  BI analysts → Kimball star schema (best BI tool fit)
  ML engineers → Wide tables / feature stores
  Operational apps → Reverse-ETL, possibly OBT
  Other engineers → Probably both, depending on use case
STEP 2: WHAT'S THE COMPLEXITY?
  Few sources, simple → 3NF or staging-only
  Many sources, complex → Data Vault silver + Kimball gold
  Enterprise integration → Data Vault definitely
STEP 3: WHAT'S THE TEAM?
  Strong analyst team → Kimball (industry standard)
  Strong engineering team → Comfort with Vault or custom
  Mixed team → Layered (vault silver, Kimball gold)
STEP 4: WHAT'S THE EVOLUTION?
  Static product → modeling can be tight
  Rapidly evolving → loose silver, flexible gold
USUALLY:
  Bronze: raw, no modeling
  Silver: 3NF or Vault (depending on complexity)
  Gold:  Kimball (BI) + OBT (specific use cases)
```

```sql
14.2 The Semantic Layer
SEMANTIC LAYER = consistent business definitions, queryable from BI.
  Without semantic layer:
    Looker has its own definition of "revenue"
    Tableau has another
    Python notebooks compute differently
    → "the disagreement problem"
  With semantic layer:
    One definition: revenue = SUM(amount) FROM orders WHERE status = 'completed'
    All consumers query via the layer
    Numbers match across tools
OPTIONS:
   dbt Semantic Layer (formerly Metrics Layer; via MetricFlow)
   Cube (open-source / hosted, BI-tool agnostic)
   LookML (Looker-specific but powerful)
   Atlan Atlas, Atscale (enterprise semantic layers)
VALUE:
  ✓ One source of truth for business metrics
  ✓ Drag-and-drop BI tools query consistently
  ✓ Version-controlled definitions
14.3 dbt as the Modeling Spine
dbt has become the universal modeling layer because:
  ✓ Works on every warehouse + lakehouse
  ✓ SQL-based (analyst-accessible)
  ✓ Version controlled
  ✓ Testing + documentation built in
  ✓ Lineage tracking
  ✓ Semantic layer support
THE STAFF DEFAULT:
  Unless there's a specific reason not to, dbt is the modeling layer.
  Even at companies using SQLMesh or Dataform, dbt is the most common.
WHERE dbt FALLS SHORT:
```

Data System Design (Capstone)

```
  ✗ Real-time / streaming transformations
  ✗ Complex Python-required transformations
  ✗ Very large scale (10K+ models become slow to parse)
  ✗ Some advanced data engineering patterns (orchestrated with stateful logic)
```

```
14.4 The Modern Modeling Stack
TYPICAL 2024+ ANALYTICS PLATFORM MODELING:
  RAW (bronze) ■→ STAGING (dbt: 1:1 cleanup)
                       ■
                       ▼
                INTERMEDIATE (dbt: reusable logic)
                       ■
                       ▼
                MARTS (dbt: business-ready)
                       ■
                       ▼
                SEMANTIC LAYER (dbt MetricFlow / Cube)
                       ■
                       ▼
                BI TOOLS / APIs
PROPERTIES:
   Layered separation
   Tests at every layer
   Documentation co-located with code
   Lineage end-to-end
   Semantic layer ensures metric consistency
```

```
14.5 The Mistakes That Sink Modeling
■ NO LAYERING — flat dbt project
  → unmaintainable at scale
■ ONE FACT TABLE FOR EVERYTHING
  → no grain discipline; mixed semantics
■ DENORMALIZED EVERYTHING (OBT-only)
  → consumers can't navigate; ML works, BI doesn't
■ NORMALIZED EVERYTHING (3NF in gold)
  → analysts can't query without joining 8 tables
■ NO METRICS DEFINITIONS
  → "revenue" means something different to each team
■ NO TESTS
  → silent data quality issues
■ NO OWNERSHIP
  → tables that no one maintains; analysts shy away
```

Data System Design (Capstone)

#### 15. Late-Arriving Data & Out-of-Order Events

15.1 The Reality IN PRACTICE: Events with timestamp 09:00 may arrive at 09:05, 10:00, or 2024 15.

```
  CAUSES:
   Mobile app offline; uploads later
   Producer retry after failure
   Network partitions
   Buffering at intermediate layers
   Producer clock skew
   Backfills
DESIGN CONSEQUENCE:
  "Today's data" is not stable until enough time has passed.
  Build for inevitability of late data, not as an exception.
```

```
15.2 Strategies
1. WATERMARKS (streaming)
   "No more events with timestamp < X will arrive"
    Bounded out-of-orderness: max N seconds lateness
    Beyond that: drop or side-output
2. ALLOWED LATENESS (streaming)
   Keep windows open longer than the watermark
   Re-emit window results when late data arrives
   Downstream must be idempotent
3. LOOKBACK WINDOWS (batch)
   Re-process the last N days every run
   MERGE-based loading handles updates
   N depends on observed lateness distribution
4. SEPARATE LATE TABLE
   Late events → 'late' table for forensics
   Main table is "as of cutoff"; reconcile periodically
5. CORRECTIONS TABLE
   Append corrections; consumers see corrected view via UNION
   Audit trail of what changed
```

```
15.3 The Architecture Implications
BRONZE (raw):
  Accept everything. No deduplication. No 'late' filter.
SILVER (cleaned):
  Reprocess windows where late data arrives (e.g., last 7 days nightly).
  MERGE on (event_id, ingest_ts) for idempotency.
GOLD (aggregated):
  Period boundaries (daily, weekly) get re-aggregated when underlying
  silver changes. dbt incremental with lookback handles this naturally.
DOWNSTREAM:
  Dashboards know that "today" is provisional.
  Yesterday's number may change as late data arrives.
  Stable point: T-7 days (depending on workload).
```

```sql
15.4 The Late-Data Trap in Aggregates
NAIVE PATTERN:
  Daily INSERT INTO daily_revenue SELECT day, SUM(amount) FROM orders;
  → Once a day is computed, it's static.
  → Late orders for that day are LOST in the aggregate.
```

Data System Design (Capstone)

```sql
CORRECT PATTERN:
  Daily MERGE INTO daily_revenue
    USING (SELECT day, SUM(amount) AS revenue
           FROM orders WHERE day > current_date - 7) updates
    ON daily_revenue.day = updates.day
    WHEN MATCHED THEN UPDATE SET revenue = updates.revenue
    WHEN NOT MATCHED THEN INSERT (...);
  → Recent days re-aggregated; late data picked up.
OR DBT INCREMENTAL WITH LOOKBACK:
  {{ config(materialized='incremental', unique_key='day') }}
  SELECT day, SUM(amount) AS revenue
  FROM {{ ref('stg_orders') }}
  {% if is_incremental() %}
    WHERE day > (SELECT MAX(day) FROM {{ this }}) - INTERVAL '7 DAYS'
  {% endif %}
  GROUP BY day
■ PRODUCTION SCENARIO: A finance team had a daily revenue model that was 'finished' once computed.
Mobile orders sometimes uploaded 24-48 hours late. Their daily revenue numbers were systematically 3% lower
than the totals reconciled by accounting weekly. Three years of slightly wrong numbers. The fix was a 7-day
lookback in the dbt incremental, but the trust damage took 18 months to rebuild. Lesson: bake late-data handling
into your aggregates from day 1; retrofitting it requires admitting errors.
```

Data System Design (Capstone)

#### 16. Multi-Region Architectures

  - 16.1 Why Multi-Region
- DRIVERS:
- 1. DISASTER RECOVERY: survive a region outage
- 2. LATENCY: consumers in different regions
- 3. COMPLIANCE: data residency (GDPR, etc.)
- 4. SCALE: per-region capacity limits
- 5. PROXIMITY: collect data near sources

```
16.2 The Topology Options
ACTIVE-PASSIVE (DR):
  Primary region handles everything.
  Replicate to standby for failover.
  On primary failure: cut over.
```

```
  PROS: simple, well-understood
  CONS: standby idle (expensive)
  RPO: replication lag (seconds-minutes)
  RTO: failover time (minutes-hours)
ACTIVE-ACTIVE (PARTITIONED):
  Different data lives in different regions
  E.g., EU users in Frankfurt; US users in Virginia
  Each region serves its own data
```

```
  PROS: better latency, no idle resources
  CONS: cross-region queries are slow; complex routing
ACTIVE-ACTIVE (REPLICATED):
  Same data in multiple regions
  Both regions can write
  Replicate bidirectionally
```

```
  PROS: full DR, lowest user latency
  CONS: conflict resolution, complexity
AGGREGATE:
  Multiple regional clusters → one central aggregate
  Regional analytics local, global analytics central
  Common for global enterprises
```

```
16.3 Multi-Region Data Stack
WAREHOUSE LAYER:
  Snowflake: account per region; replication via DR feature
  BigQuery: dataset region; multi-region locations exist
  Redshift: cluster per region; manual replication
  Databricks: workspace per region
STORAGE LAYER:
  S3: cross-region replication (CRR)
  GCS: multi-regional buckets
  Azure: geo-redundant storage
LAKEHOUSE:
  Iceberg / Delta tables replicated via storage replication
  Catalog: regional catalogs with sync (or central catalog with regional caches)
  Engines query closest replica
STREAMING:
  Kafka: MirrorMaker 2 for cross-DC replication
  Kinesis: cross-region streams (manual setup)
  Confluent Cloud: native multi-region clusters
```

#### 16.4 The Cost of Multi-Region

Data System Design (Capstone)

```
DOUBLED OR TRIPLED COSTS:
  Storage: 2-3x (replicated)
  Egress: cross-region transfer fees ($$)
  Operational: 2-3x infrastructure to monitor
  Personnel: on-call across timezones
QUESTIONS TO ASK BEFORE GOING MULTI-REGION:
  Is the DR scenario really worth this cost?
  Could you handle a 24-hour regional outage differently?
  Is the latency requirement real (does the user notice 50ms)?
  Is the compliance requirement real (or just nice-to-have)?
THE STAFF ANSWER:
  Single-region with strong backups → 95% of companies' real needs.
  Multi-region: when there's a SPECIFIC requirement (regulatory or business).
```

16.5 Data Residency GDPR / DATA SOVEREIGNTY: EU data must stay in EU. Implementation patterns:

#### 1. Per-region storage: EU data lands in EU bucket; US data in US bucket

#### 2. Per-region warehouse: EU analytics in eu-west; US in us-east

#### 3. Aggregates only crossing borders: PII stays regional, only

```
    4. Right-to-be-forgotten: track lineage, delete across systems
COMPLIANCE-AWARE PIPELINES:
  Tag data with residency requirements at ingest
  Routing: each region pipeline only processes its own data
  Cross-region queries blocked or audited
WAREHOUSE FEATURES:
  Snowflake: account region pins data to region; multi-region selectable
  BigQuery: dataset region/location
  Databricks: workspace region
■ PRINCIPAL MINDSET: Multi-region adds 2-3x cost and complexity. Justify it with specific requirements, not
'we should be resilient'. Most companies need strong single-region with cross-region backups + a documented
recovery procedure. True multi-region active-active is the right answer only when the business demands it
(regulatory residency, sub-50ms global latency, multi-region uptime SLA). Don't over-engineer; the operational
burden compounds.
```

Data System Design (Capstone)

## PART V — REFERENCE ARCHITECTURES

#### 17. The Modern Analytics Platform

```
17.1 The Use Case
SCENARIO:
  Series C startup, 200 engineers, 50 analysts.
  Mix of operational DBs (Postgres) + SaaS (Stripe, Salesforce, etc.).
  BI for executives, analysts, departmental dashboards.
  No real-time requirements (5-min freshness is fine).
  Some ML for product recommendations.
  Budget: ~$300K/year for data platform.
  Cloud: AWS.
```

```
17.2 The Architecture
SOURCES:
  Postgres (5 production DBs)
  Stripe, Salesforce, Zendesk, HubSpot (SaaS)
  Application events (clicks, signups via Segment)
INGESTION:
  CDC: Debezium → Kafka (MSK) → Spark Structured Streaming → Iceberg
  SaaS: Fivetran → Iceberg (Bronze)
  Events: Segment → S3 → Spark batch → Iceberg
STORAGE:
  Lakehouse: Iceberg on S3 (Bronze + Silver)
  Catalog: AWS Glue (cheap, AWS-native)
  Warehouse: Snowflake (Gold marts for BI)
PROCESSING:
  Orchestration: Airflow on MWAA
  Transformation: dbt-snowflake for Silver → Gold (via Snowflake reading Iceberg)
  Streaming: Spark Structured Streaming for CDC apply
SERVING:
  BI: Looker on Snowflake
  ML: Spark on Databricks reading Iceberg silver layer
  Reverse-ETL: Hightouch → Salesforce, HubSpot
```

```
GOVERNANCE:
  dbt for modeling discipline + tests + docs
  AWS Glue catalog for table inventory
  Datafold for lineage / regression
  Cost monitoring via Snowflake account usage views
OBSERVABILITY:
  Airflow logs to CloudWatch
  dbt artifacts → BigQuery (or own warehouse) via dbt_artifacts pkg
  Elementary for dbt observability
  Datadog for infra
```

```
17.3 The Cost Breakdown
ANNUAL COST ESTIMATE:
  Snowflake (compute + storage):     $120K
  Databricks (ML):                    $40K
  MSK (Kafka):                        $30K
  Fivetran (SaaS connectors):         $50K
  Hightouch (reverse-ETL):            $20K
  Airflow MWAA:                       $15K
```

Data System Design (Capstone)

```
  dbt Cloud:                          $15K
  Datafold + Elementary:              $15K
  Looker:                             $50K (separate license)
  Misc (S3, Glue, monitoring):        $15K
```

```
  TOTAL:                              ~$370K (slightly over budget)
OPTIMIZATIONS:
   Self-host Airflow if team can ($5K savings)
   dbt Core instead of Cloud ($10K savings)
   Reduce Fivetran by building Stripe / SF integrations ($30K savings)
  → ~$320K, slightly under budget
```

```
17.4 The Why-Each-Choice
ICEBERG OVER DELTA:
  Multi-engine read (Snowflake, Spark, future Trino if needed)
SNOWFLAKE FOR GOLD MARTS:
  Analyst UX is best; Looker integration mature
  Could be all-Databricks but team is more SQL-native than Spark
MSK OVER CONFLUENT CLOUD:
  AWS-native, cheaper for our volume
  Could migrate to Confluent if multi-region
dbt FOR MODELING:
  Industry standard, team familiarity, governance built-in
DATABRICKS FOR ML:
  Single platform for Spark + ML + experimentation
  Reads Iceberg directly (no data movement)
FIVETRAN OVER CUSTOM:
  SaaS connectors are commoditized; team focus on differentiating work
  Reconsider at higher scale
```

```
17.5 The Failure Modes
WHAT BREAKS FIRST:
   CDC PIPELINE LAG (most common)
    Causes: source DB slow query, replication slot stuck, broker outage
    Mitigation: lag monitoring, alert at >5 min
```

```
   dbt RUN TIMEOUT
    Causes: warehouse contention, model becomes too expensive
    Mitigation: slot reservation, query optimization, materialization review
```

```
   COST ANOMALIES
    Causes: runaway query, dashboard with bad SQL, unused warehouse left running
    Mitigation: budget alerts, query cost monitoring
```

```
   SCHEMA DRIFT
    Causes: source schema change without coordination
    Mitigation: schema contracts, auto-detection, alerting
```

 DATA QUALITY ANOMALIES Causes: source bug, upstream data issue, transform regression Mitigation: dbt tests, statistical anomaly detection (Elementary)

Data System Design (Capstone)

#### 18. The Real-Time Analytics Platform

```
18.1 The Use Case
SCENARIO:
  Ad tech company. User-facing real-time dashboards.
  Bid + impression + conversion events.
  100K events/sec peak, 30K average.
  Sub-second dashboard refresh required.
  Plus standard analytical batch needs.
  Multi-tenant (advertisers see their own data only).
  Cloud: GCP.
```

```
18.2 The Architecture
SOURCES:
  Ad servers emit events to Kafka
INGESTION:
  Application → Kafka (Confluent Cloud, ~20 brokers)
  Schema: Avro via Schema Registry
```

```
REAL-TIME PROCESSING:
  Flink reads Kafka topics
  Stream-stream joins (bid ↔ impression ↔ conversion)
  Tumbling windows for per-minute aggregates
  Writes to:
    - ClickHouse (real-time serving)
    - Iceberg (historical analytics)
STORAGE:
  ClickHouse cluster (self-hosted on GKE): real-time serving
  Iceberg on GCS: long-term storage
  BigQuery: analyst-facing on Iceberg external tables
SERVING:
  Real-time dashboard: ClickHouse via Grafana / custom UI
  Per-advertiser auth: row-level security
  Analytical: BigQuery for analysts
  ML: Spark on Dataproc reading Iceberg
ORCHESTRATION:
  Flink job graph for streaming
  Airflow on Cloud Composer for batch
  dbt-bigquery for modeling
GOVERNANCE:
  Schema Registry enforces contracts
  Per-tenant access via row-level security
  Unity-like catalog via DataHub on top
```

```
18.3 The Latency Budget
TARGET: < 1 second event → visible on dashboard
BREAKDOWN:
  Ad server → Kafka:      < 50 ms
  Kafka buffer:            ~100 ms (acks)
  Flink processing:        ~200 ms (with window)
  Flink → ClickHouse:      ~50 ms
  ClickHouse insert:       ~100 ms
  Dashboard query:         ~300 ms
  Total:                  ~800 ms ✓
CRITICAL:
  ClickHouse is the secret sauce for sub-second analytics
  Iceberg is for historical / drill-down / billing
  Two paths because no single tool does both well
```

Data System Design (Capstone)

```
18.4 The Capacity Math
KAFKA:
  100K events/sec × 1 KB = 100 MB/s
  RF=3, peak 3x = 900 MB/s aggregate
  Per broker @ 200 MB/s sustained → 5 brokers minimum
  Plus headroom: 10-15 brokers
FLINK:
  100K events/sec × ~10 stages × parallelism = sized accordingly
  Likely 50-100 task manager cores
  TaskManagers on Kubernetes with autoscaling
CLICKHOUSE:
  100K inserts/sec → use batch inserts (every 200ms) = 20K batches/sec
  ClickHouse handles this on a few nodes
  Storage: 100K × 86400 × 1 KB × 30 days = ~250 GB (compressed ~80 GB)
  Single-cluster 6-9 nodes with replication
ICEBERG / BIGQUERY:
  Mostly batch consumption
  100 GB/day, ~30 TB/year retention
  Plus query compute on BigQuery
```

```
18.5 Multi-Tenancy
ROW-LEVEL SECURITY required:
  Each advertiser sees only their data.
PATTERNS:
  ClickHouse:
    Row-level security policies on tenant_id column
    Or per-tenant table (if cardinality manageable)
    Or per-tenant database (full isolation)
  BigQuery:
    Row access policies on tenant_id
    Or per-tenant dataset (if tenant count low)
  Iceberg:
    Catalog-level permissions (more limited)
    Often handled at the query engine layer
TRADE-OFFS:
  More tenants → more overhead with per-tenant tables/dbs
  Fewer tenants → can afford strong isolation
```

Data System Design (Capstone)

#### 19. The ML Platform

```
19.1 The Use Case
SCENARIO:
  Mid-size tech co, 30 ML engineers + data scientists.
  Multiple models: recommendations, fraud, churn, ad targeting.
  Need feature engineering, training, serving, monitoring.
  Cloud: AWS.
  Already have an analytics platform (Snowflake + Iceberg).
```

```
19.2 The Architecture
FEATURE STORE:
  Built on Iceberg (re-using existing lakehouse)
  Offline features: large historical tables for training
  Online features: served via Redis / DynamoDB for inference
  Feature definition: Feast or custom
TRAINING:
  Databricks for distributed training on Iceberg data
  MLflow for experiment tracking
  Notebooks + jobs for repeatable training
MODEL REGISTRY:
  MLflow Model Registry (versioning, staging → prod)
SERVING:
  Real-time: SageMaker endpoints (or Databricks Serving)
  Batch: Airflow → Spark scoring → write predictions to Iceberg
MONITORING:
  Prediction logs → S3 → Iceberg
  Feature drift detection: Evidently / WhyLabs
  Model performance: re-evaluation on labeled data
  A/B test infrastructure for new model rollout
PIPELINES:
  Airflow orchestrates training pipelines
  CI/CD via GitHub Actions: model PR → test → train → register → deploy
  Promotion: staging → canary → full prod
```

```
19.3 The Online / Offline Pattern
FEATURES NEED TWO VIEWS:
  OFFLINE (training):
    All historical features for a user up to some point in time
    Joins, aggregations across long history
    Stored in: Iceberg, queried by Spark
  ONLINE (inference):
    Single user's latest features
    Sub-10 ms lookup
    Stored in: Redis, DynamoDB
THE PROBLEM:
  Same feature definitions in both places
  If they diverge: training-serving skew (model performs differently in prod)
THE SOLUTION:
  Feature store (Feast, Tecton, Hopsworks, etc.)
  Define features ONCE
  Materialize to both offline (Iceberg) and online (Redis) stores
  Same code paths used at training and inference
```

#### 19.4 The ML Pipeline Lifecycle

Data System Design (Capstone)

```
1. FEATURE ENGINEERING
   Define features (Feast / dbt + custom)
   Materialize offline (Iceberg) + online (Redis)
2. TRAINING
   Spark / sklearn / PyTorch / TensorFlow
   Track experiments (MLflow)
   Validate on holdout
3. EVALUATION
   Metrics, fairness checks, comparison to baseline
   Manual sign-off gate
4. REGISTRATION
   Push to MLflow Model Registry
   Stage: staging → production
5. DEPLOYMENT
   Serving endpoint (real-time) or batch scoring (Airflow)
   Canary first; ramp traffic
6. MONITORING
   Prediction logs
   Feature drift
   Model performance (when ground truth available)
   Business metrics (clicks, conversions, etc.)
7. RETRAIN
   Trigger: scheduled, drift-based, or performance-based
   Repeat cycle
```

```
19.5 The Common ML Platform Mistakes
■ NO FEATURE STORE
  Features re-implemented per model → drift between training and serving
■ TRAINING ON LAKEHOUSE; SERVING ON SEPARATE PIPELINE
  Same features computed two ways
■ NO MODEL VERSIONING
  Can't reproduce yesterday's model
■ NO MONITORING
  Model rots silently; performance degrades
■ NO A/B TEST INFRA
  Can't safely test new models
■ ML PIPELINE NOT IN VERSION CONTROL
  Click-ops on Databricks notebooks; not reproducible
■ INTERVIEW ANGLE: Q: How does an ML platform integrate with your data platform?
Staff: ML platform reads from the lakehouse (Iceberg silver/gold), not from the warehouse — gives Spark /
Python direct access without warehouse compute. Features defined in the same modeling layer (dbt OR feature
store like Feast), materialized to BOTH offline (Iceberg, for training) and online (Redis, for inference). This shared
definition is what prevents training-serving skew. Predictions written back to Iceberg so they're queryable for
monitoring. The platform is layered on top of the same data infrastructure; it's not a separate ecosystem.
```

Data System Design (Capstone)

#### 20. The Multi-Tenant SaaS Data Platform

```
20.1 The Use Case
SCENARIO:
  B2B SaaS analytics tool. Customers see THEIR OWN data.
  1000+ tenants, each with TB to hundreds of TB.
  Customers query via embedded dashboards in your app.
  Different per-tenant retention / governance.
  Cloud: multi-cloud (AWS, GCP, Azure depending on customer).
```

```
20.2 The Tenancy Patterns
SHARED INFRASTRUCTURE, LOGICAL SEPARATION:
  One warehouse / one lakehouse
  Tenant_id column on every table
  Row-level security policies enforce isolation
```

```
  PROS: cheaper; one set of pipelines
  CONS: noisy-neighbor risk; cross-tenant data leakage risk
PER-TENANT INFRASTRUCTURE:
  Each tenant gets their own warehouse / catalog / schema
```

```
  PROS: strong isolation; tenant-specific tuning
  CONS: expensive; operational complexity at scale (1000+ warehouses)
HYBRID:
  Small tenants: shared infrastructure with RLS
  Large tenants: dedicated infrastructure
  Enterprise tenants: option of either based on contract
```

```
20.3 Architecture
INGESTION:
  Tenant data lands in tenant-tagged bronze
  Per-tenant transformations in silver
  Tenant-specific marts in gold (or shared marts with tenant filter)
STORAGE:
  Iceberg with tenant_id partitioning for shared tables
  Or per-tenant tables for very large tenants
SERVING:
  Embedded analytics: Cube or custom query API on top of warehouse
  Per-tenant query quota / rate limit
  Cache per-tenant results aggressively
ISOLATION:
  Auth: JWT carries tenant_id; query layer enforces
  Network: tenant-specific VPCs for enterprise
  Compute: separate warehouses for noisy-neighbor isolation
  Storage: per-tenant catalog for some tenants
GOVERNANCE:
  Per-tenant audit logs (compliance)
  Per-tenant data residency (some in EU, some in US, etc.)
  Per-tenant retention (some 90 days, some 7 years)
```

```
20.4 The Cost Allocation Problem
WITH SHARED INFRASTRUCTURE:
  How do you bill tenants based on usage?
OPTIONS:
  Estimate via query history (warehouse query history has tenant_id)
  Tag queries with tenant_id; aggregate cost
  Some warehouses (Snowflake) have query tags
```

Data System Design (Capstone)

```
DECISIONS:
  Flat fee per tenant tier (simple but unfair)
  Usage-based (fair but complex to compute)
  Hybrid (base fee + overage)
REPORTING:
  Customer-facing usage dashboard
  Internal: per-tenant profitability
  Engineering: identify expensive tenants for optimization
```

```
20.5 The Operational Reality
COMMON CHALLENGES:
   NOISY NEIGHBORS
    One tenant's huge query slows everyone
    Mitigation: per-tenant warehouse, query timeouts, fair scheduling
   SCHEMA EVOLUTION ACROSS TENANTS
    Different tenants on different versions
    Mitigation: schema versioning, backward-compat, multi-version support
   CROSS-TENANT FEATURES
    Anonymized aggregates across tenants for benchmarking
    Mitigation: strict opt-in, differential privacy where needed
   PER-TENANT PIPELINES
    1000 tenants × N pipelines = unmanageable manually
    Mitigation: parameterized / templated pipelines, dbt vars
   GOVERNANCE AT SCALE
    Each tenant may have compliance requirements
    Mitigation: tag-based governance, automated policy enforcement
```

Data System Design (Capstone)

## PART VI — CROSS-CUTTING CONCERNS

#### 21. Data Quality at Platform Scale

```
21.1 The Layers of Quality
1. INGEST QUALITY
   Schema validation (Schema Registry)
   Row count vs source reconciliation
   Source freshness checks (dbt source freshness)
2. TRANSFORM QUALITY
   dbt tests (unique, not_null, accepted_values, relationships)
   Statistical tests (dbt-expectations)
   Custom singular tests
3. RUNTIME QUALITY
   Anomaly detection (Elementary, Monte Carlo, Bigeye)
   Drift detection
   Volume anomalies (today's data 10% of yesterday's?)
4. CONSUMPTION QUALITY
   Dashboard health (do BI tools see expected numbers?)
   ML model performance (drift in feature distribution?)
   API SLAs (are queries returning in expected time?)
```

```
21.2 The Quality Tier System
TIER 1 — BUSINESS-CRITICAL
  Revenue, customer counts, executive KPIs
   Tests: comprehensive (unique, not_null, FK, anomaly, recon)
   SLA: hourly freshness, 99.9% uptime
   Alerts: page on-call immediately
   Owners: assigned, documented, accountable
TIER 2 — IMPORTANT
  Departmental dashboards, standard marts
   Tests: structural + key business rules
   SLA: daily freshness, 99% uptime
   Alerts: team channel
TIER 3 — EXPLORATORY
  Analyst-owned, ad-hoc, ML drafts
   Tests: minimal (PK if exists)
   SLA: best-effort
   Alerts: warn-level
ENFORCE TIERS via tags + CI rules.
Tier-1 models MUST have specific tests; CI fails otherwise.
```

```
21.3 The Anomaly Detection Stack
HAND-WRITTEN TESTS (dbt tests):
  Catch KNOWN problems.
  Cheap, predictable.
STATISTICAL TESTS (dbt-expectations):
  Catch range/distribution violations.
  Still based on hand-defined expectations.
ANOMALY DETECTION (Elementary, Monte Carlo, Bigeye, Anomalo):
  Catch UNKNOWN problems.
  Learn patterns automatically.
  Alert on deviations.
```

Data System Design (Capstone)

```
INCIDENT MANAGEMENT:
  PagerDuty / Opsgenie for alerts
  Runbooks for common incidents
  Post-mortems for systemic issues
THE COMPLETE STACK:
  Tests + monitoring + anomaly detection + incident response
  Each layer catches different classes of issues.
```

```
21.4 The Quality Process
DURING DEVELOPMENT:
   Write tests for new models
   Validate against existing data
   Code review for SQL quality
CI:
   Slim CI runs tests on changed models
   Statistical comparison vs prod (Datafold, audit_helper)
   Block merge on tier-1 test failures
DEPLOYMENT:
   Staging environment with prod-shaped data
   Smoke tests post-deploy
   Rollback plan
PRODUCTION:
   Continuous quality monitoring
   Daily / weekly quality reports
   Incident management workflow
QUARTERLY REVIEW:
   Test coverage audit
   SLA review
   Tier reassessment
   Tool stack evaluation
```

Data System Design (Capstone)

#### 22. Governance, Catalog & Lineage

  - 22.1 What Governance Covers
- 1. DISCOVERY
- Which datasets exist? What do they mean? Who owns them?
- 2. ACCESS CONTROL
- Who can read/write/modify which datasets?
- PII access; need-to-know basis
- 3. LINEAGE
- Where does this data come from? What depends on it?
- 4. QUALITY
- Is this data trustworthy? When was it last updated?
- 5. AUDIT
- Who accessed what when? Compliance trails.
- 6. COMPLIANCE
- GDPR, HIPAA, SOC 2 — required documentation + controls

```
22.2 The Data Catalog Layer
DATA CATALOG = system of record for datasets across the platform.
EXAMPLES:
   DataHub (LinkedIn, open-source)
   OpenMetadata (open-source)
   Atlan (commercial)
   Collibra (enterprise)
   Alation (enterprise)
   Unity Catalog (Databricks-native, now open)
   AWS Glue + Lake Formation (AWS-native)
   dbt Explorer (dbt-specific)
WHAT TO LOOK FOR:
  ✓ Auto-discovery (no manual cataloging)
  ✓ Lineage (column-level ideally)
  ✓ Search (find what you need)
  ✓ Tagging (PII, sensitivity)
  ✓ Ownership tracking
  ✓ Integration with engines (Snowflake, BigQuery, Iceberg, etc.)
  ✓ API for custom integrations
```

```
22.3 Lineage
LINEAGE = which datasets feed which.
TABLE-LEVEL:
  fct_orders → daily_revenue → executive_dashboard
  Easy to compute from dbt manifest
COLUMN-LEVEL:
  fct_orders.amount_cents → daily_revenue.revenue_dollars (after /100)
  Harder; requires SQL parsing
  Tools: DataHub, SQLLineage, OpenLineage
WHY LINEAGE MATTERS:
  ✓ Impact analysis: "If I rename this column, what breaks?"
  ✓ Root cause: "Why did this dashboard change?"
  ✓ Compliance: "Where did this PII propagate?"
  ✓ Trust: "How was this number computed?"
OPENLINEAGE:
  Open standard for lineage events
  Spark, Flink, Airflow, dbt all emit events
  Consumed by Marquez, DataHub, OpenMetadata, Atlan
```

Data System Design (Capstone)

```
22.4 Access Control
LAYERED APPROACH:
NETWORK:
  VPC / Private Link / IP allow-listing for engines
AUTHENTICATION:
  SSO via SAML/OIDC; MFA required
AUTHORIZATION (RBAC):
  Roles map to permissions
  Permissions on databases, schemas, tables, columns
DATA-LEVEL:
  Column masking for PII (mask email for non-PII roles)
  Row-level security (per-tenant, per-region)
  Dynamic masking based on user context
AUDIT:
  Log access; queryable
  Anomaly detection on access patterns
THE STAFF DEFAULT:
  Use catalog-level RBAC (Unity, Polaris, Lake Formation)
  Don't duplicate permissions across engines
  Tag columns for automatic policy enforcement
```

```
22.5 The Governance Maturity Levels
LEVEL 0: AD-HOC
  Permissions granted manually; no catalog; no lineage
  → 'Slack the right person to find what you need'
LEVEL 1: BASIC
  Permissions in warehouse; spreadsheet of datasets; dbt docs
  → 'Most analysts can find what they need; PII is mostly secure'
LEVEL 2: CATALOG
  Data catalog deployed; auto-discovery; basic lineage
  → 'Search-driven discovery; lineage helps debugging'
LEVEL 3: GOVERNED
  Tag-based access; column-level masking; full lineage
  → 'PII automatically protected; compliance audits straightforward'
LEVEL 4: SELF-SERVE
  Data products; contracts; SLAs; automatic compliance
  → 'Data mesh; teams own their products; central platform enforces'
MOST COMPANIES ARE LEVEL 1-2.
Reaching Level 3 requires dedicated platform team investment.
Level 4 requires organizational maturity, not just tooling.
■ PRINCIPAL MINDSET: Governance is the layer most teams under-invest in. They build pipelines fast; they
don't catalog them; they grant permissions inconsistently; lineage is in someone's head. By the time it matters
(compliance audit, PII leak, dashboard outage with unknown root cause), the catalog is too far behind reality to
recover. Treat governance as a continuous investment from year 1, not a remediation later.
```

Data System Design (Capstone)

#### 23. Cost Engineering as a Discipline

```
23.1 Why Cost Is Engineering
DATA PLATFORM COSTS COMPOUND SILENTLY:
   New pipelines added; old ones never removed
   Warehouses sized for peak that no longer exists
   Storage retention left at default forever
   Egress from unintended cross-region flows
   Always-on infrastructure for sporadic use
AT $1M/year platform, 30% waste = $300K leak.
At $10M, $3M wasted.
COST IS ENGINEERING because:
   Architecture decisions determine cost shape
   Code patterns determine query cost
   Operational hygiene determines waste rate
```

23.2 The Cost Hierarchy Component % of platform cost Warehouse / lakehouse compute 50-70% Streaming infrastructure 10-20% ETL processing (Spark, dbt) 10-15% Storage 5-15% Ingestion tools (Fivetran) 5-10% BI / observability 5-10% Egress 1-5% (can spike)

#### 23.3 The Top Levers

#### 1. Auto-suspend everything

#### 2. Right-size per workload

#### 3. Materialized views for repeated queries

#### 4. Reserved capacity for steady use

#### 5. Partition + cluster large tables

#### 1. Time travel retention tuning

#### 2. Lifecycle policies (S3 IA, Glacier)

#### 3. Lakehouse for cold tiers

#### 4. Compaction (small files explode cost)

#### 1. Dbt incremental over full refresh

#### 2. Slim CI (build only what changed)

#### 3. Spark resource tuning

#### 4. Streaming compaction discipline

#### 1. Audit Fivetran connectors (sync only what's used)

#### 2. Custom builds for highest-volume sources

#### 3. Filter at source where possible

#### 1. Co-locate compute and data

Data System Design (Capstone)

#### 2. Avoid unintended cross-region transfers

#### 3. Audit data shares + replications

```
23.4 The FinOps Process
FINOPS = Financial Operations for cloud.
DAILY:
  Anomaly alerts (cost > 1.5x rolling average)
  Top-N expensive queries
  Top-N expensive jobs
WEEKLY:
  Cost dashboard review per team
  Identify optimization candidates
MONTHLY:
  Cross-team cost report
  Forecast vs actual
  Optimization wins celebrated
QUARTERLY:
  Architecture cost review
  Reserved capacity rebalancing
  Tool consolidation opportunities
  Dead pipeline cleanup
ANNUALLY:
  Vendor renegotiation
  Strategic stack review
  Migration ROI analysis
```

```
23.5 The Cost-Visible Engineering Culture
SHOW ENGINEERS THEIR COSTS:
  Dashboards showing per-pipeline cost
  Per-PR cost impact (Datafold can show this)
  Per-team / per-product cost attribution
INCENTIVIZE:
  Celebrate cost reductions
  Quarterly cost targets
  Engineering OKRs include efficiency
EDUCATE:
  Patterns vs anti-patterns documented
  Common-pitfall guide
  Cost-aware code review
THE STAFF SIGNAL:
  Cost discussions happen in design reviews, not after the bill arrives.
  'How much will this cost?' is the second question after 'will it work?'.
■ PRODUCTION SCENARIO: A team's data platform bill grew 30% year over year for 3 years (from $1M to
~$2.2M) without anyone noticing — buried in cloud spend. A new VP asked for a cost review. They found: 12
unused Snowflake warehouses (some sized L) running 24/7; 8 dbt models that scanned hundreds of TB each but
were unused; 200 dead pipelines from past experiments; 3-year-old Fivetran connectors syncing data no one
queried. Total identified waste: $600K/year. Lesson: cost reviews must be quarterly, not yearly. Waste
compounds.
```

Data System Design (Capstone)

#### 24. Observability & SLOs

```
24.1 The Three Pillars (Adapted for Data)
TRADITIONAL OBSERVABILITY (software):
  Metrics, Logs, Traces
DATA OBSERVABILITY:
  Freshness, Volume, Schema, Quality, Lineage
  FRESHNESS: when did this table last update?
  VOLUME:   how many rows arrived; is it normal?
  SCHEMA:   has the structure changed unexpectedly?
  QUALITY:  do the values pass tests?
  LINEAGE:  what does this depend on; what depends on it?
```

```
24.2 SLOs for Data Platforms
TYPICAL DATA SLOs:
FRESHNESS:
  Tier-1 tables: 95% of days, updated by 6 AM
  Tier-2 tables: 95% of days, updated by 9 AM
  Tier-3 tables: best effort
UPTIME:
  Warehouse availability: 99.9%
  Critical pipelines: 99% success rate weekly
QUALITY:
  Tier-1 tests passing: 99% over rolling 7 days
  Statistical anomalies: < N per month per tier
LATENCY:
  Streaming end-to-end: p95 < 5 sec
  BI query p95: < 10 sec
ERROR BUDGETS:
  Define and burn-tracker
  When budget exhausted: pause feature work, focus on reliability
```

```
24.3 The Observability Stack
INFRASTRUCTURE OBSERVABILITY:
  Datadog / New Relic / Grafana
  Logs to ELK / Splunk / Datadog Logs
  Metrics, traces, dashboards
DATA-SPECIFIC OBSERVABILITY:
  Elementary (open-source dbt observability)
  Monte Carlo, Bigeye, Anomalo (commercial data observability)
  Datafold (data diffs + lineage)
  OpenLineage + Marquez
WORKFLOW OBSERVABILITY:
  Airflow UI, Dagster UI
  Custom dashboards via dbt artifacts package
  Failure / SLA breach alerts to PagerDuty
```

```
24.4 The Incident Management Workflow
1. ALERT FIRES
   PagerDuty / Slack
   On-call engineer paged
2. TRIAGE (5 min)
   Severity? Tier? Customer-facing?
   Acknowledge alert
```

Data System Design (Capstone)

```
3. INVESTIGATE
   What changed recently? (deploys, schema, source)
   Lineage: what's affected downstream?
   Logs / metrics for clues
4. MITIGATE (asap)
   Rollback, disable, route around
   Restore service before perfect fix
5. RESOLVE (longer)
   Root cause; permanent fix
   Update tests / monitors to catch in future
6. POSTMORTEM (within 7 days for SEV-1)
   Blameless review
   Action items with owners + dates
   Share learnings across team
```

```
24.5 The Runbook Discipline
EVERY ALERT SHOULD HAVE A RUNBOOK.
  Alert: "fct_orders freshness > 6 hours"
  Runbook:
```

#### 1. Check Airflow DAG for fct_orders → upstream failures

#### 2. Check source freshness (Fivetran / CDC pipelines)

#### 3. Check warehouse status (Snowflake status page)

#### 4. Common causes: source DB slow, Fivetran sync failed,

#### 5. Escalation: page DBA if source issue; page platform team

```
    6. Communication: post to #data-incidents Slack
WHY:
   Reduces MTTR (mean time to repair)
   Anyone on rotation can respond
   Captures institutional knowledge
   Improves over time
■ PRINCIPAL MINDSET: Observability is the difference between operating a platform and praying over a
platform. SLOs make priorities explicit. Runbooks scale on-call expertise across a team. Postmortems prevent
repeated failures. At staff level, you're expected to design observability AS PART of the architecture, not as an
afterthought. Every component should answer: how would I know if this is broken?
```

Data System Design (Capstone)

## PART VII — MIGRATION & EVOLUTION

#### 25. Replatforming Patterns

```
25.1 Why Teams Migrate
DRIVERS:
   Cost: current stack 2-3x what it should be
   Performance: hitting fundamental limits
   Reliability: incidents per month unsustainable
   Maintainability: tribal knowledge, no documentation
   Talent: can't hire because of legacy stack
   Strategic: M&A, cloud change, vendor relationships
   Compliance: new regulatory requirements
MIGRATION IS EXPENSIVE.
Budget 12-24 months for a major data platform migration.
Expect surprises at every layer.
```

```
25.2 The Standard Migration Phases
PHASE 1: ASSESS (1-2 months)
   Inventory: tables, pipelines, dashboards, users
   Identify dependencies and constraints
   Cost estimate (current vs target)
   Risk assessment
   Get organizational buy-in
PHASE 2: PILOT (2-3 months)
   Migrate one non-critical workload end-to-end
   Learn the gotchas
   Build tooling (data diff, automation)
   Validate cost projections
PHASE 3: DUAL-RUN (3-6 months)
   Production data flows to both systems
   Reconciliation runs continuously
   Build confidence
PHASE 4: ROLLING CUTOVER (3-6 months)
   Migrate consumer by consumer (dashboards, ML models)
   Each migration validated independently
   Parallel writes continue
PHASE 5: DECOMMISSION (1-3 months)
   Stop writing to old system
   Keep readable for safety period (60-90 days)
   Final decommission
```

```
25.3 The Strangler Fig Pattern
ANTI-PATTERN: BIG BANG MIGRATION
  Build new system; cutover everything in one weekend.
  Why it fails: too many unknowns; can't validate at scale; no rollback.
STRANGLER FIG PATTERN:
  New system grows alongside old
  Each migrated component is one "branch growing"
  Eventually the old system is "strangled" — no traffic
APPLIED TO DATA:
   One pipeline at a time
   One mart at a time
   One dashboard at a time
```

Data System Design (Capstone)

 Validated at each step THIS IS THE STAFF-LEVEL APPROACH. Never propose a big bang for data platform migration.

```
25.4 The Reconciliation Discipline
DURING DUAL-RUN, RECONCILE CONTINUOUSLY:
ROW-LEVEL:
  COUNT(*) match
  SUM(metric_column) match
  Sample row-level comparison
STATISTICAL:
  Distribution of key metrics (mean, p50, p95)
  Daily totals match across systems
BUSINESS:
  KPIs match in both systems
  Specific calculations validated by domain experts
TOOLING:
  Datafold for column-level diffs
  dbt audit_helper for SQL-level reconciliation
  Custom comparison dashboards
CUTOVER GATE:
  Don't cut over without proof of parity.
  Reconcile for a FULL CYCLE (week, month) before switching.
```

```
25.5 The Migration Anti-Patterns
■ MIGRATING WITHOUT BUSINESS BUY-IN
  Engineering effort with no executive sponsor → stalls
■ NO RECONCILIATION
  "It looks right" — until accounting notices revenue is 4% off
■ MIGRATING DURING OTHER MAJOR INITIATIVES
  Migration + product rewrite + reorg = chaos
■ TURNING OFF OLD SYSTEM TOO SOON
  60-90 day overlap is cheap insurance
■ NOT INVOLVING CONSUMERS EARLY
  Analysts find out from outage; trust destroyed
■ NO ROLLBACK PLAN
  Always have a way to revert
■ UNDERESTIMATING TIMELINE
  Migrations always take 2-3x estimate
  Budget accordingly
```

Data System Design (Capstone)

#### 26. The Tech Debt Inventory

```
26.1 Identifying Data Platform Tech Debt
COMMON DEBT CATEGORIES:
1. ARCHITECTURAL
    Lambda architecture that should be Kappa/lakehouse
    Warehouse-only when lakehouse fits
    Tightly coupled monoliths
2. MODELING
    Flat dbt project (no layering)
    Same metric defined 5 ways
    Stale, undocumented tables
3. TOOLING
    Multiple ETL tools with overlapping responsibilities
    Outdated versions of frameworks
    Custom code where SaaS would suffice
4. OPERATIONAL
    Manual processes that should be automated
    Tribal knowledge instead of runbooks
    No SLOs or observability
5. COST
    Always-on infrastructure
    Unused pipelines / tables
    Inefficient queries
6. GOVERNANCE
    Permissions managed manually
    No catalog / discovery
    PII not tagged or controlled
7. PEOPLE
    Single points of failure (one person knows X)
    Onboarding takes months
    No documentation
```

```
26.2 The Inventory Process
QUARTERLY DEBT AUDIT:
1. INVENTORY EXISTING SYSTEMS
   List all pipelines, tables, tools, owners
2. ASSESS EACH:
    Still used?
    Cost / value ratio?
    Reliability?
    Documented?
3. PRIORITIZE
   Impact × ease vs cost
   What's blocking new work?
   What's the highest cost waste?
4. BUDGET DEBT WORK
   20-30% of engineering time
   Otherwise it compounds
5. CELEBRATE WINS
   "Removed $200K/year of waste"
   "Deleted 200 unused tables"
   "Migrated to dbt mesh: CI time -80%"
```

#### 26.3 The Right Time to Pay Debt

Data System Design (Capstone)

```
PAY DOWN DEBT BEFORE:
   Major scaling event (you'll hit walls otherwise)
   Hiring spree (debt slows onboarding)
   Regulatory audit (debt is risk)
   Cost crunch (debt is waste)
```

```
LEAVE DEBT WHEN:
   System is being decommissioned soon
   Cost of fix > expected lifetime benefit
   Higher-priority work exists
DEBT IS NOT INHERENTLY BAD.
Accumulated wisely, it's leverage.
Accumulated unwisely, it's drag.
THE STAFF SIGNAL:
  You don't try to eliminate all debt.
  You manage it strategically.
```

```
26.4 Communicating Debt to Leadership
IN BUSINESS TERMS:
  Don't: "the dbt project needs refactoring"
  Do: "the dbt project takes 90 minutes to test; PRs slow down; we
        lose ~$50K/year in engineering time. Refactoring takes 2
        engineer-months but saves 30 minutes per PR going forward."
QUANTIFY:
  Cost of debt today (engineer-hours, dollars)
  Cost of fixing
  Payback period
STAFF SIGNAL:
  Frame debt work as ROI, not ideology.
  Make trade-offs explicit.
  Get executive buy-in before major projects.
■ PRINCIPAL MINDSET: Tech debt at the platform level is the staff engineer's job to identify, prioritize, and
communicate. Most teams accrue debt without measuring it; by the time it's painful, it's expensive. The discipline:
continuous audit, business-framed proposals, budgeted refactoring time. The teams that pay debt down regularly
stay nimble; the ones that don't end up doing huge migrations every 5 years.
```

Data System Design (Capstone)

## PART VIII — INTERVIEW PRACTICE

#### 27. Five Worked Design Interviews

```
27.1 Design Interview #1: Greenfield Series A Platform
PROMPT:
  "Series A startup, 20 engineers, on AWS. Build the data platform from scratch.
   Mostly e-commerce, mix of operational data + click events. Budget: $50K/yr."
CLARIFY:
   Volume: roughly how much data? (assume ~100 GB/day events, 50 GB ops data)
   Latency: daily freshness is OK?
   Team: 1-2 data engineers
   Goal: standard analytics + future ML
ARCHITECTURE:
  Sources: Postgres production DB; Stripe; Segment for events
  Ingestion: Fivetran (Stripe), Postgres CDC via Fivetran, Segment → S3
  Storage: Iceberg on S3 + Snowflake (smallest size, aggressive auto-suspend)
  Processing: dbt-snowflake; later add Spark when ML emerges
  Serving: Metabase (cheap, BI)
  Orchestration: dbt Cloud (handles scheduling)
COST:
  Snowflake (XS auto-suspending): ~$10K
  Fivetran (low volume): ~$15K
  dbt Cloud: ~$5K
  Metabase: ~$5K
  S3 + Glue: ~$3K
  Misc: ~$5K
  Total: ~$43K (under budget)
WHY THIS WORKS:
   Simple stack
2 engineers can maintain
   Snowflake auto-suspend means low idle cost
   Iceberg storage future-proofs (can swap engines later)
   dbt for discipline from day 1
EVOLUTION PATH:
  At 10x scale: move to BigQuery or larger Snowflake; add Spark for ML
```

```
27.2 Design Interview #2: Real-Time Fraud Detection
PROMPT:
  "Online payments company. Detect fraud in <100 ms during transaction.
   1M transactions/day. Need both real-time scoring + offline model retraining."
CLARIFY:
   Latency: <100 ms for scoring (hard SLA)
   Throughput: ~12 TPS average, peaks 100 TPS
   Existing stack: Postgres for ops, no warehouse yet
ARCHITECTURE:
  ONLINE (real-time scoring):
    Transaction → API → Feature lookup (Redis, < 5ms)
                     → Model inference (SageMaker endpoint, < 50ms)
                     → Decision returned
```

FEATURES: Real-time features: computed in Flink streaming, written to Redis Historical features: batch-computed daily, written to Redis + Iceberg

```
  OFFLINE (training):
    Production events → Kafka → Flink → Iceberg (feature store)
```

Data System Design (Capstone)

```
    Spark on Databricks reads Iceberg for training
    MLflow tracks experiments
    Model promoted to SageMaker for serving
  MONITORING:
    Predictions logged to Iceberg
    Drift detection (Evidently)
    Performance: continuous evaluation on labeled fraud cases
BUDGET CONSIDERATIONS:
  Redis cluster: ~$2K/month
  Flink (managed via Kinesis Analytics or self-hosted): ~$5K/month
  SageMaker serving: ~$3K/month
  Databricks (training): ~$5K/month
  Iceberg storage: minimal
  Kafka (MSK): ~$2K/month
  Total: ~$200K/year
WHY THIS WORKS:
   Sub-100 ms latency met (Redis + SageMaker)
   Same feature store for training + serving (no skew)
   Lakehouse stores everything for retraining
   Standard ML platform components
```

```
27.3 Design Interview #3: PB-Scale Multi-Tenant SaaS
PROMPT:
  "B2B SaaS analytics tool. 5000 customers, each with TB to 100 TB.
   Customers query their own data via embedded dashboards. Sub-second response."
CLARIFY:
   Total data: ~5 PB across all tenants
   Concurrent queries: ~500 peak
   Cloud: multi-cloud (some customers AWS, some Azure)
   Sensitive: row-level security mandatory
ARCHITECTURE:
  TENANT DATA STORAGE:
    Iceberg per tenant (or per tenant cluster for small tenants)
    Partitioned by tenant_id within tables for shared schema
    Per-tenant catalog in Polaris (Iceberg REST)
    Multi-region: tenant data lives in their preferred region
  INGEST:
    Per-tenant ingestion via REST API + S3 upload
    Streaming: per-tenant Kafka topics
    Schema enforcement at ingest
  PROCESSING:
    Spark on Databricks for batch transforms
    Per-tenant resource quotas
  SERVING:
    Hot tier: ClickHouse cluster per region for sub-second queries
    Materialized aggregates pushed to ClickHouse via Flink
    Embedded dashboard queries ClickHouse (with tenant_id filter, enforced)
    Cold tier: Iceberg for drill-down queries (slower, ~1-2 sec)
  ISOLATION:
    Per-tenant API keys → JWT with tenant_id
    Query layer (Cube or custom) enforces tenant filter
    Resource quotas per tenant
    Audit logs per tenant
  GOVERNANCE:
    Per-tenant data residency
    Per-tenant retention (some 1 year, some 7 years)
    GDPR delete: per-tenant tagged for compliance flows
  COST ATTRIBUTION:
    Query tags carry tenant_id
    Per-tenant cost dashboards
```

Data System Design (Capstone)

```
    Used for billing
WHY THIS WORKS:
   Sub-second response via ClickHouse + materialized views
   Iceberg for cheap long-term storage
   Multi-region for residency
   Tenant isolation at multiple layers
```

```
27.4 Design Interview #4: Replace Legacy Hadoop with Lakehouse
PROMPT:
  "Large enterprise. 10 PB on Hadoop with Hive Metastore. Hundreds of Spark
   jobs. Move to lakehouse. Minimize downtime."
CLARIFY:
   Current state: HDFS + Hive + Spark + Oozie
   Volume: 10 PB total, 50 TB/day new
   Consumers: 500 dashboards, 200 data scientists, 30 production ML models
PHASED MIGRATION:
  PHASE 1 (3 months): NEW DATA TO ICEBERG
    New tables written to Iceberg via Spark (no change to existing)
    Hive Metastore migrated to Iceberg-compatible catalog (Glue or REST)
    Existing Hive tables exposed via Iceberg's MIGRATE for read-only
  PHASE 2 (6 months): PIPELINE MIGRATION
    Spark jobs writing Hive → Spark writing Iceberg (one job at a time)
    Reconciliation: row counts, sums, distinct counts
    Consumers still query old Hive tables until parity verified
  PHASE 3 (6 months): CONSUMER MIGRATION
    Dashboards / models switched to Iceberg tables
    Old Hive tables become read-only
```

```
  PHASE 4 (3 months): DECOMMISSION
    Stop writes to Hive
    Hold 90 days for safety
    Decommission HDFS clusters
TOOLING:
   SQLGlot for SQL translation (Hive SQL → Spark SQL)
   Data diff (Datafold) for reconciliation
   Per-pipeline migration runbook
   Rollback procedures
TIMELINE: 18 months
COST: parallel infrastructure (2x storage briefly); offset by long-term savings
WHY THIS WORKS:
   Strangler pattern; no big bang
   Reconciliation at every step
   Customer experience preserved
   Cost crossover validated early
```

```
27.5 Design Interview #5: Real-Time + Batch Hybrid for IoT
PROMPT:
  "IoT platform. 100K devices, each emitting metrics every second. Need
   real-time monitoring + 5-year historical analytics."
CLARIFY:
   Volume: 100K × 1 reading/sec = 100K events/sec, 1 KB each
   Daily: ~8.6 TB/day; 5 years = ~15 PB
   Real-time: device dashboard within 10 sec
   Historical: trends, anomalies, long-term analytics
ARCHITECTURE:
  INGEST:
    MQTT broker → Kafka (high throughput)
    100K events/sec → 8 brokers RF=3
```

Data System Design (Capstone)

```
  STREAMING:
    Flink consumes Kafka
    Three sinks:
```

#### 1. Real-time: ClickHouse for device monitoring (last 7 days hot)

#### 2. Aggregates: Materialize per-device per-minute rollups

```
      3. Archive: Iceberg for full history
  STORAGE:
    Hot (ClickHouse): 7 days × 100K × 86400 × 1 KB = ~60 GB compressed
    Warm (Iceberg): 90 days × 1 TB/day = ~90 TB compressed
    Cold (Iceberg + S3 IA / Glacier): years 1-5 = ~15 PB
  CONSUMERS:
    Device dashboard: ClickHouse (sub-sec)
    Analytics dashboards: Iceberg via Trino (5-30 sec)
    ML / data science: Spark on Iceberg
    Alerts: Flink rule engine (real-time)
  COMPACTION + TIERING:
    Hourly Iceberg compaction
    Lifecycle: 90 days → S3 IA; 1 year → S3 Glacier
    Old Iceberg partitions can be re-read but slower
  COST:
    Kafka (MSK): ~$50K/year
    Flink (Kinesis Analytics or self-hosted): ~$100K/year
    ClickHouse: ~$50K/year
    Iceberg storage: 15 PB on tiered S3 = ~$200K/year storage + $50K query
    Total: ~$500K/year
WHY THIS WORKS:
   Real-time (ClickHouse) + historical (Iceberg) clean separation
   Lifecycle tiering for cost control on 15 PB
   Streaming aggregates reduce serving cost
   Multiple consumer patterns supported on same source data
```

Data System Design (Capstone)

28. War Stories from the Field ■ PRODUCTION SCENARIO: The Spreadsheet Migration. A team spent 8 months migrating a warehouse pipeline. After cutover, the CFO noticed his quarterly numbers were now 3% higher. Investigation: legacy pipeline had a date-rounding bug that under-counted weekend transactions. The new pipeline was CORRECT, but the CFO had been making decisions on the wrong numbers for years. Lesson: data correctness isn't always celebrated. Communicate that migrations may surface latent issues; bring stakeholders along. ■ PRODUCTION SCENARIO: The CDC Cascade. A team set up Debezium CDC for 20 Postgres tables. Schema Registry had compatibility set to BACKWARD, but one developer set an individual subject to NONE for a quick fix. A schema change later broke 200 downstream consumers silently — they read nulls for the renamed field. Lesson: enforce schema policies at the registry level, not per subject. CI checks for individual overrides. ■ PRODUCTION SCENARIO: The Time-Travel Storage Bomb. A Snowflake account's bill jumped $50K/month with no clear cause. Investigation: Time Travel + Fail-safe storage had grown to 5 PB. The culprit: a high-churn table with DATA_RETENTION_TIME_IN_DAYS = 90, getting massive daily updates. Every update created new partitions; 90 days × 100 GB churn/day = huge. Lesson: per-table retention based on actual recovery needs. Default 90 is too high for high-churn tables. ■ PRODUCTION SCENARIO: The Lakehouse Catalog SPOF. A team ran Glue Catalog in one region. Glue had a 3-hour partial outage. Reads from Iceberg tables failed because metadata couldn't be resolved — the DATA in S3 was fine. Consumers across 5 teams were down. Lesson: catalog HA is critical. Plan for catalog outages: multi-region setup, client-side caching, graceful degradation. ■ PRODUCTION SCENARIO: The dbt Mesh Coordination Crisis. A team split their 2000-model monolith into 8 dbt projects (one per domain). Velocity improved. Then the finance team renamed a 'public' model, breaking 5 downstream projects. No contracts; no coordinated upgrade. Lesson: contracts at project boundaries are non-optional. Version policies, deprecation timelines, and cross-team communication need to be in place before splitting. ■ PRODUCTION SCENARIO: The Streaming Replay Disaster. A team had a bug in their Flink job that produced wrong aggregates. They wanted to replay from Kafka with the fixed code. Problem: Kafka retention was 7 days, and the bug had been live for 14 days. They lost 7 days of correct re-processed data. Lesson: Kafka retention should be longer than your investigation + fix cycle. For critical streams, retain 30-90 days. Or: stream into a lakehouse where history is preserved indefinitely. ■ PRODUCTION SCENARIO: The Multi-Tenant Cost Surprise. A SaaS platform billed customers a flat fee. One enterprise customer's workload grew 50x in a year. They were costing the company $40K/month in warehouse compute alone. The shared-warehouse model had been profitable on average but lost money on this customer. Lesson: at SaaS scale, usage-based pricing or tiering isn't optional. Plan for the long tail of heavy users from day 1. ■ PRODUCTION SCENARIO: The Migration That Wasn't. A team spent 2 years migrating from Redshift to Snowflake. Halfway through, AWS announced major Redshift improvements (Serverless, RA3). By the time migration completed, Redshift had closed most of the gap. Cost savings projected: $100K/year. Actual: $20K/year. Two engineer-years of opportunity cost. Lesson: technology moves; long migrations might end up chasing yesterday's problems. Validate the business case quarterly during multi-year migrations.

Data System Design (Capstone)

#### 29. Cheat Sheet & Mental Models

```
29.1 The Seven Mental Models
1. CLARIFY BEFORE SOLVING
   The first 5 minutes determine the next 55.
   Ask: scale, latency, users, ecosystem, budget.
2. THE 5-LAYER STACK
   Sources → Ingestion → Storage → Processing → Serving → Consumption
   Always cover all 5; missing one is the most common gap.
3. STORAGE IS LONG-LIVED; COMPUTE IS RENEWABLE
   Storage decisions affect you for 10 years.
   Compute can be swapped every 2-3 years.
   Invest in storage portability (open formats).
4. AT-LEAST-ONCE + IDEMPOTENT > EXACTLY-ONCE
   True exactly-once across systems is mostly mythical.
   Idempotent processing is the practical answer.
5. LATE DATA IS NORMAL, NOT EXCEPTIONAL
   Design lookback windows, allowed lateness, reprocessing from day 1.
6. EVERY ARCHITECTURE HAS COST + OPS CONSEQUENCES
   The 'best' architecture is the one your team can operate sustainably.
7. STRANGLER FIG > BIG BANG
   Replace systems incrementally with reconciliation at every step.
```

```
29.2 The Decision-Framework Cheat Sheet
LAMBDA vs KAPPA vs LAKEHOUSE
  Default: stream into lakehouse with ACID; batch + stream consumers from there.
  Lambda only if legacy; Kappa only if no batch consumers.
WAREHOUSE vs LAKEHOUSE vs HYBRID
  Small / analyst-heavy: warehouse only
  Multi-engine / large scale / ML: lakehouse-first
  Most real cases: hybrid (lakehouse storage + warehouse for BI)
INGESTION:
  SaaS sources: Fivetran / Airbyte
  CDC: Debezium / DMS
  Streaming: Kafka / Kinesis
  Files: Airflow + Spark
STORAGE:
  Open table format: Iceberg (winning in 2024-2025)
  Warehouse storage: pick based on workload + ecosystem
PROCESSING:
  Batch: Spark (Databricks) + dbt
  Stream: Flink for complex, Spark Streaming for simple
SERVING:
  BI: warehouse (Snowflake / BigQuery)
  Real-time: ClickHouse / Druid for sub-second
  ML: lakehouse + Spark/Polars
ORCHESTRATION:
  Default: Airflow
  Asset-centric: Dagster
  Simple dbt-only: dbt Cloud
```

  - 29.3 The Interview Pitfalls to Avoid
- 1. SKIPPING CLARIFICATION
- Going straight to architecture without understanding requirements

Data System Design (Capstone)

- 2. NOT DOING THE MATH
- "We'll use Kafka" without estimating broker count or throughput
- 3. NAMING TOOLS WITHOUT JUSTIFICATION
- "Let's use Iceberg" — why? Vs alternatives?
- 4. IGNORING OPERATIONS
- Beautiful design with no on-call story
- 5. NOT REVISITING DECISIONS
- Interviewer changes a requirement; you keep your plan
- 6. AVOIDING TRADE-OFFS
- "X is better" — at what? At what cost?
- 7. NOT NARRATING
- Drawing in silence; missed signal opportunities
- 8. PERFECTIONISM
- Trying for the 'right' answer instead of a defensible one
- 9. SCOPE CREEP
- Diving into one component for 30 min; missing other layers
- 10. NO EVOLUTION STORY
- Architecture as a snapshot, not a 3-year roadmap

```
29.4 The One-Page Synthesis
WHAT MAKES A STAFF-LEVEL DESIGN ANSWER:
  STRUCTURE:
    Clarify → estimate → high-level → deep dive → trade-offs → ops → wrap
    5-10 min clarification; 20+ min deep work; 10+ min trade-offs/ops
  CONTENT:
    All 5 layers covered
    Tool choices justified
    Math grounding (not hand-waving)
    Failure modes addressed
    Cost considered
    Migration / evolution path
  COMMUNICATION:
    Narrate while drawing
    Acknowledge alternatives
    Defend choices clearly
    Adjust when interviewer changes context
    Manage time consciously
  PATTERNS REFERENCED:
    Medallion architecture
    Outbox / CQRS
    CDC patterns
    Lambda vs Kappa vs Lakehouse
    Idempotency / late data
  ANTI-PATTERNS AVOIDED:
    Big-bang migrations
    Single-region without backup
    Permanent always-on warehouses
    No layering / no testing
    Ignoring observability
```

29.5 What Separates Senior from Staff (One More Time) SENIOR STAFF / PRINCIPAL ■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■ "Use Kafka + Flink + Iceberg" "Given 50K events/sec and 5-min freshness: Kafka with 6 brokers + 32 partitions, Flink with 1-sec checkpoints, Iceberg

Data System Design (Capstone)

```
                                      with hourly compaction. If sub-second
                                      latency was required, add ClickHouse."
"Snowflake is good"                  "Snowflake for this team because:
                                      analyst-heavy, sporadic workload,
                                      auto-suspend saves 75%, multi-cloud
                                      flexibility, Looker integration.
                                      Trade-off vs BigQuery: GCP commitment
                                      not present; BQ on-demand cheaper if
                                      queries are well-tuned but ours aren't."
"We have data quality issues"        "Test coverage at 60%, three quality
                                      tiers, two anomaly detection tools.
                                      Critical issues: silver layer missing
                                      idempotency on the orders CDC. Plan:
                                      add MERGE keys, lookback window, weekly
                                      reconciliation. Eliminates 80% of
                                      observed defects."
"Migration is hard"                  "Migration is incremental. Strangler fig:
                                      one pipeline at a time, dual-write,
                                      reconcile via Datafold, cut over per
                                      pipeline. Budget 18 months for 200
                                      pipelines. Off-ramp: keep old system
                                      readable for 90 days post-cutover."
"We should adopt data mesh"          "Data mesh works when you have 50+ data
                                      engineers and a strong platform team.
                                      We're at 15 data engineers; we're not
                                      ready. We should invest in central
                                      tooling first, revisit at year 2-3."
■ PRINCIPAL MINDSET: The system design interview at staff level is where you demonstrate that you can think
across all layers, all time horizons, and all stakeholder perspectives simultaneously. Technology choices are
secondary; trade-off reasoning is primary. The best staff engineers I know can hold competing concerns in their
heads (cost vs latency, simplicity vs flexibility, speed vs correctness) and articulate the trade-off explicitly. That's
the skill being tested. Practice it deliberately.
 — End of Data System Design Capstone —
 This concludes the 8-topic Principal Engineer Notes Series.
 Total: 477+ pages of architect-level data engineering interview prep.
```


---

[← Back to main](./README.md)
