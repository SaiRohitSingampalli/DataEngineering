# Topic 23 — Data Quality, Observability & Contracts

> Standalone deep-dive · The cross-cutting staff-level signal

[← Back to main](./README.md)

---

## How to use this document

Data quality, observability, and contracts are the areas where **staff** and **senior** engineers diverge most sharply in interviews. Senior engineers can describe tools; staff engineers describe operating models, blast radius, SLAs, and ownership. This chapter is the one you re-read the week of the interview.

> **🧠 Principal Mindset:** The question is never "do you have tests?" — it's "when the pipeline breaks silently at 3am on a Sunday during an earnings close, how do you know *before* finance does?" That reframing is the entire chapter.

## Table of Contents

**PART I — FOUNDATIONS**
- 1. Why Data Quality Is Hard
- 2. The Six Dimensions of Data Quality
- 3. Senior vs Staff Framing
- 4. The Data Reliability Lifecycle

**PART II — TESTING FRAMEWORKS**
- 5. dbt Tests (built-in + custom)
- 6. Great Expectations
- 7. Soda Core / Soda Cloud
- 8. Elementary (dbt-native observability)

**PART III — OBSERVABILITY PLATFORMS**
- 9. The Observability Platform Landscape
- 10. Monte Carlo
- 11. Bigeye, Metaplane, Datafold

**PART IV — LINEAGE**
- 12. Why Lineage Matters
- 13. OpenLineage + Marquez
- 14. Column-Level Lineage

**PART V — DATA CONTRACTS**
- 15. What a Data Contract Is
- 16. Schema Registry Patterns
- 17. Contract Enforcement
- 18. Breaking Change Management

**PART VI — SLAs, SLOs, INCIDENT RESPONSE**
- 19. SLIs, SLOs, SLAs for Data
- 20. On-Call for Data Teams
- 21. Incident Response Playbook

**PART VII — OPERATING MODEL**
- 22. Data Mesh and Ownership
- 23. Blast Radius and Change Management
- 24. The Shift-Left Pattern

**PART VIII — INTERVIEW PRACTICE**
- 25. The Staff-Level Data Quality Interview
- 26. Worked System Design
- 27. War Stories
- 28. Cheat Sheet &amp; Mental Models

---

## PART I — FOUNDATIONS

### 1. Why Data Quality Is Hard

```
Software bugs usually fail LOUDLY (exception, 500, crash).
Data bugs usually fail SILENTLY:
   • Rows missing (silent drop)
   • Values wrong (silent corruption)
   • Timestamps shifted (silent lag)
   • Nulls where values expected (silent null-propagation)
   • Duplicates where unique expected (silent fan-out)

Loud failures bother systems.
Silent failures bother humans — and reach executives.

STAFF SIGNAL: data quality is primarily a DETECTION problem,
   not a PREVENTION problem. You cannot prevent all bugs;
   you can bound how long one hides.
```

### 2. The Six Dimensions of Data Quality

```
Classical model (Loshin, Redman, DAMA DMBOK):

   ACCURACY: does the value correctly reflect reality?
      "Alice's address is 123 Main St." → was it ever?

   COMPLETENESS: are all expected rows / fields present?
      "80% of today's events landed. Where are the other 20%?"

   CONSISTENCY: do related datasets agree?
      "Order totals in Shopify = order totals in warehouse?"

   TIMELINESS: is data available when needed?
      "Dashboard refresh at 9am, but data arrived at 11?"

   VALIDITY: does the value conform to format / rules?
      "Phone number is a valid E.164 number?"

   UNIQUENESS: are duplicates absent where unique required?
      "Each order_id appears once?"

STAFF SIGNAL: knowing which dimension(s) a given test exercises
   lets you argue trade-offs. "I'd prioritize completeness over
   accuracy for this pipeline because downstream dedup can handle
   a 1% false positive but can't fabricate missing rows."
```

### 3. Senior vs Staff Framing

| Senior | Staff |
|--------|-------|
| "We have dbt tests." | "We have four layers: schema tests at ingest (contract), column tests in silver (dbt generic), business rules in gold (dbt singular), anomaly detection on key metrics (Monte Carlo). Each layer has an owner, an SLA, and a failure mode." |
| "We alert on failures." | "We tier alerts: P1 wakes on-call (SLA breach on revenue-impacting dataset), P2 fires in Slack during business hours (quality warning on a non-critical dataset), P3 logs for weekly review (anomalies worth investigating). We measure alert noise and tune down anything that fires without action." |
| "Failures roll back." | "We use blue/green at the table level with Delta/Iceberg time travel: a failed job doesn't poison the current snapshot; the previous snapshot remains the queried version. Rollback is 'repoint the view,' not 'restore from backup.'" |
| "We own data quality." | "The producer owns schema and freshness contracts. The platform owns observability tooling. Consumers own semantic validation of their outputs. We publish an RACI and audit it quarterly." |

### 4. The Data Reliability Lifecycle

```
PRODUCE → VALIDATE → PROPAGATE → DETECT → DIAGNOSE → RESOLVE → REVIEW

PRODUCE: the upstream system writes data (app, SaaS, CDC, batch)
VALIDATE: contracts + ingestion-time checks catch schema/basic issues
PROPAGATE: transformations may introduce issues (bad joins, late data)
DETECT: observability platforms catch what tests missed
DIAGNOSE: lineage + metadata + traces identify where
RESOLVE: fix forward (new run) or rollback (previous snapshot)
REVIEW: incident postmortem → add test or alert so this specific bug
   doesn't recur without noise

STAFF SIGNAL: the hard part is the REVIEW loop. Without it,
   you accumulate tests that didn't catch past bugs and will
   fire on new ones. Good teams delete tests too.
```

---

## PART II — TESTING FRAMEWORKS

### 5. dbt Tests (built-in + custom)

#### 5.1 Generic Tests

```yaml
# models/schema.yml
version: 2

models:
  - name: dim_customer
    tests:
      - dbt_utils.unique_combination_of_columns:
          combination_of_columns: [customer_id, effective_date]
    columns:
      - name: customer_id
        tests:
          - not_null
          - unique
      - name: email
        tests:
          - not_null
          - dbt_utils.not_empty_string
      - name: country_code
        tests:
          - accepted_values:
              values: ['US', 'CA', 'GB', 'DE', 'FR', 'JP']
      - name: lifetime_value
        tests:
          - dbt_utils.expression_is_true:
              expression: ">= 0"
```

#### 5.2 Singular Tests

```sql
-- tests/revenue_matches_source.sql
-- Returns rows that FAIL (empty = pass)

SELECT 
    DATE_TRUNC('day', order_ts) AS day,
    SUM(amount) AS wh_revenue,
    (SELECT SUM(amount) FROM {{ source('shopify', 'orders') }}
     WHERE DATE_TRUNC('day', created_at) = DATE_TRUNC('day', order_ts)) AS src_revenue
FROM {{ ref('fact_orders') }}
WHERE order_ts >= DATEADD('day', -7, CURRENT_DATE)
GROUP BY DATE_TRUNC('day', order_ts)
HAVING ABS(wh_revenue - src_revenue) / NULLIF(src_revenue, 0) > 0.01
```

#### 5.3 Severity + Store Failures

```yaml
columns:
  - name: email
    tests:
      - not_null:
          config:
            severity: error        # fail the build
      - dbt_utils.not_empty_string:
          config:
            severity: warn         # pass the build but log
      - unique:
          config:
            severity: error
            store_failures: true   # persist failed rows for investigation
            where: "created_at >= '2024-01-01'"  # scope to recent data
```

> **💡 Tip:** `store_failures: true` is underused. It turns failed tests into queryable tables, which you can trend over time and share with data stewards.

### 6. Great Expectations

#### 6.1 What GX Is

```
GREAT EXPECTATIONS (GX): Python-first data quality framework.
   
   STRUCTURED FOR:
      Non-SQL sources (parquet, pandas DataFrames, Spark DataFrames)
      Rich expectation library (100+ built-in)
      Expectation suites versioned in code or JSON
      Data Docs: auto-generated HTML quality reports
      Checkpoints: run suites against data on schedule

   USE CASES:
      Lake-level validation (not just warehouse)
      Ingestion-time checks for Spark/PySpark pipelines
      Python-native teams who find dbt overkill
```

#### 6.2 Minimal Example

```python
import great_expectations as gx
from great_expectations.core.batch import RuntimeBatchRequest

context = gx.get_context()

batch_request = RuntimeBatchRequest(
    datasource_name="my_s3_source",
    data_connector_name="default_runtime_data_connector",
    data_asset_name="daily_orders",
    runtime_parameters={"path": "s3://bucket/orders/2024-10-18/"},
    batch_identifiers={"default_identifier_name": "2024-10-18"},
)

validator = context.get_validator(
    batch_request=batch_request,
    expectation_suite_name="orders_suite",
)

# Expectations
validator.expect_column_values_to_not_be_null("order_id")
validator.expect_column_values_to_be_unique("order_id")
validator.expect_column_values_to_be_between("amount", min_value=0)
validator.expect_column_value_lengths_to_be_between(
    "email", min_value=5, max_value=320
)
validator.expect_table_row_count_to_be_between(
    min_value=10_000, max_value=1_000_000
)

results = validator.validate()
if not results["success"]:
    raise RuntimeError("Data quality failed")
```

#### 6.3 GX vs dbt Tests

```
dbt tests:
   SQL-native; run in warehouse; cheap when data already lands
   Best for warehouse-layer tests (silver, gold)
   
GX:
   Python-native; can run before data lands in warehouse
   Best for ingestion-layer tests (bronze), lake-level validation,
      and non-SQL sources

PATTERN:
   Bronze: GX (shape, schema, freshness)
   Silver + Gold: dbt tests (business rules, cross-table)
   Observability: platform (anomalies, drift)
```

### 7. Soda Core / Soda Cloud

#### 7.1 Soda Checks Language (SodaCL)

```yaml
# soda/checks.yml
checks for orders:
  - row_count > 1000
  - missing_count(email) = 0
  - duplicate_count(order_id) = 0
  - invalid_percent(email) < 1%:
      valid regex: ^[^@]+@[^@]+\.[^@]+$
  - freshness(created_at) < 1h
  - avg(amount) between 10 and 1000
  - schema:
      fail:
        when required column missing: [order_id, customer_id, amount, created_at]
        when forbidden column present: [password, ssn]

  # Custom SQL
  - revenue_matches_source:
      revenue_matches_source query: |
        SELECT ABS(wh.total - src.total) / NULLIF(src.total, 0) AS diff
        FROM ...
      fail: when > 0.01
```

Run via CLI or in orchestrator:
```
soda scan -d my_datasource -c configuration.yml checks.yml
```

#### 7.2 Positioning vs dbt Tests + GX

```
SODA strengths:
   SodaCL (YAML) is more readable than dbt tests for complex checks
   Native regex, freshness, schema diff
   Soda Cloud (SaaS) adds dashboards + alerts
   
Choose:
   Mostly SQL warehouse? dbt tests + Elementary
   Mostly Python / Spark? Great Expectations
   Multi-source + want YAML DSL? Soda
```

### 8. Elementary (dbt-native observability)

```
ELEMENTARY = dbt package + web UI = observability for dbt projects.

CAPABILITIES:
   Anomaly detection on dbt models (row count, freshness, nulls, distributions)
   Alerts via Slack / email / Teams / PagerDuty
   dbt test results dashboard
   Column-level lineage
   Model execution history + metrics

PRICING:
   Open-source for dbt package
   Elementary Cloud for hosted UI + advanced detection

STAFF SIGNAL:
   For dbt-centric shops, Elementary is often enough — no need
   for a separate observability platform until scale demands it.
```

---

## PART III — OBSERVABILITY PLATFORMS

### 9. The Observability Platform Landscape

```
Data observability platforms (2024-2026):
   
   MONTE CARLO: category-defining; most mature; largest footprint
   BIGEYE: strong anomaly detection + metadata
   METAPLANE: lightweight; dbt-friendly; smaller teams
   DATAFOLD: diff-first (data diff, column lineage)
   ANOMALO: ML-based anomaly detection; no-SQL config
   ACCELDATA: enterprise + pipeline observability
   SIFFLET: EU-based; strong lineage focus
   LIGHTUP: ML-heavy anomaly detection
   
   PLUS: open-source entrants (Elementary, re_data) and 
         vendor-specific tools (Snowflake Data Metric Functions,
         Databricks Lakehouse Monitoring, BigQuery data profiling)

COMMON FEATURES:
   Automatic column profiling + anomaly detection
   Freshness + volume + distribution monitoring
   Lineage (table + column level)
   Incident grouping (which alerts share a root cause)
   Slack / PagerDuty integration
   Rich UI for investigation
```

### 10. Monte Carlo

#### 10.1 What Monte Carlo Does

```
MONTE CARLO = "Data Reliability" platform.

CAPABILITIES:
   
   AUTOMATIC MONITORING:
      Connects to warehouse (Snowflake, BigQuery, Redshift, Databricks)
      Profiles tables: schema, row count, freshness, column distributions
      Builds baselines over time, alerts on anomalies

   LINEAGE:
      Reconstructed from query logs
      Column-level in supported warehouses
      Visualization showing upstream + downstream of any table

   INCIDENTS:
      Alerts grouped by root cause
      Impact analysis: downstream dashboards + models affected
      Workflow: assign, triage, resolve, postmortem

   CUSTOM CHECKS:
      SQL-defined custom monitors (expression returning boolean)
      Field-level health scoring

   DATA PRODUCTS:
      Group tables into "data products" with owners + SLAs
      Report SLA attainment per product
```

#### 10.2 When Monte Carlo Is Worth It

```
WORTH IT WHEN:
   • 100+ critical tables with downstream BI/ML impact
   • Multiple teams producing data (shared platform)
   • Executives read dashboards; silent failures are reputational risk
   • You have a data platform team to operate it

NOT WORTH IT WHEN:
   • Small dbt-centric team (Elementary + alerts is enough)
   • Early stage (optimize for speed, not reliability yet)
   • Budget-constrained (list prices start at tens of thousands/year)

STAFF SIGNAL: observability platforms replace a HOMEGROWN
   system of SQL-based data tests + alerts. Build vs buy is
   genuinely close; make it based on team size and criticality.
```

### 11. Bigeye, Metaplane, Datafold

```
BIGEYE:
   Strong at: anomaly detection with time-series ML
   Visualization: metric trend charts per column
   Pricing: mid-market enterprise

METAPLANE:
   Strong at: dbt integration + startup-friendly pricing
   UX: simplest to onboard
   Target: 10-100 person data teams

DATAFOLD:
   Strong at: data diff (compare any two tables / branches)
   USE: pre-merge checks in CI (dbt PR comparison)
   USE: catches silent schema evolution + value drift pre-deploy
   Unique angle: column-level lineage from query logs is top-tier
```

> **🎯 Interview Angle:** When asked "how would you catch silent data corruption?", strong staff answers are:
> (1) **Data diff** between runs (Datafold-style, or dbt-audit-helper)
> (2) **Distribution monitors** on key metrics (platform-level)
> (3) **Reconciliation** against source systems (SQL check)
> Weak answers list tool names without saying what they CATCH.

---

## PART IV — LINEAGE

### 12. Why Lineage Matters

```
LINEAGE answers:
   • Impact analysis: "If this source changes, what breaks?"
   • Root cause: "This metric is wrong — which upstream broke?"
   • Compliance: "Where does this PII column flow?"
   • Deprecation: "Can I delete this table? Who still queries it?"

TABLE-LEVEL vs COLUMN-LEVEL:
   Table-level: "A → B → C"
   Column-level: "A.col_x → B.col_y → C.col_z" (specific columns)

Column-level matters when:
   • Compliance: track PII across joins and transforms
   • Impact analysis: know which columns of downstream models break
   • Debugging: trace a bad value back to its origin
```

### 13. OpenLineage + Marquez

#### 13.1 OpenLineage Spec

```
OPENLINEAGE = open standard for lineage events.
   Linux Foundation project.
   
   An orchestrator / query engine emits events when a job runs:
      JobEvent: START / COMPLETE / FAIL
      Dataset inputs + outputs
      Facets (optional metadata: schema, column lineage, SQL, source)

   Integrations:
      Airflow (built-in)
      dbt (via dbt-ol wrapper)
      Spark (via listener)
      Flink (via listener)
      Great Expectations (via plugin)
      Snowflake, BigQuery, Redshift (via custom emitters)
```

#### 13.2 Marquez

```
MARQUEZ = reference OpenLineage server.
   Stores events + exposes:
      REST API for lineage graph queries
      Web UI for visualization
      Namespacing per dataset source

PATTERN:
   Airflow + dbt + Spark all emit OpenLineage events to Marquez.
   Marquez builds a unified lineage graph.
   Query API: "what are the upstream datasets of fact_orders?"
```

### 14. Column-Level Lineage

```
APPROACHES:
   
   SQL PARSING (most tools):
      Parse SQL → extract column references in SELECT list → map to source columns
      Works for warehouse SQL (Snowflake, BigQuery, Redshift, Databricks SQL)
      Limitations: dynamic SQL, UDFs, procedural logic

   RUNTIME CAPTURE:
      Integrate with query execution to capture actual flow
      More accurate; harder to deploy
      
   STATIC ANALYSIS of transformations:
      Parse dbt models, PySpark code
      Works for structured transforms; struggles with imperative code

VENDORS:
   Monte Carlo, Datafold, Alvin, Castor (acquired by Coalesce) 
   do column-level from query logs.
   Unity Catalog does it natively on Databricks.
   OpenLineage spec includes a ColumnLineageDatasetFacet.

STAFF SIGNAL: column-level lineage is a differentiating capability
   for compliance-sensitive orgs (GDPR, HIPAA). Table-level is often
   enough for engineering productivity.
```

---

## PART V — DATA CONTRACTS

### 15. What a Data Contract Is

```
A DATA CONTRACT = formal agreement between a DATA PRODUCER and a DATA CONSUMER
   describing the shape, meaning, and guarantees of a dataset.

CONTENTS (typically):
   • Schema: columns, types, nullability
   • Semantics: what each column means in prose
   • Freshness SLA: how often it updates
   • Quality guarantees: non-null fields, uniqueness, value ranges
   • Breaking change policy: how changes are communicated
   • Ownership: who to contact; on-call rotation

Operationalized as:
   YAML / JSON files in git (reviewed in PRs)
   Enforced by CI (block merges that violate)
   Published to a catalog (Unity Catalog, DataHub, OpenMetadata)
```

### 16. Schema Registry Patterns

#### 16.1 Confluent Schema Registry (streaming)

```
For Kafka topics with Avro / Protobuf / JSON Schema payloads.

Capabilities:
   Register schemas per subject (topic)
   Enforce compatibility (BACKWARD, FORWARD, FULL, NONE)
   Serdes integrate with producer/consumer

Compatibility modes:
   BACKWARD: consumers using NEW schema can read OLD data
      → safe to upgrade consumers first
   FORWARD: consumers using OLD schema can read NEW data
      → safe to upgrade producers first
   FULL: both
   NONE: no checks

PATTERN:
   BACKWARD = default for most teams
   Add fields: safe (default values); 
   Remove fields: not safe (break old consumers)
```

#### 16.2 Warehouse / Lakehouse Contracts

```
No equivalent of Schema Registry for warehouses; emerging patterns:

   dbt CONTRACTS (dbt 1.5+):
      model-level: enforce column types + constraints
      compile-time check: if schema doesn't match, dbt fails

   UNITY CATALOG CONSTRAINTS:
      Primary key, foreign key, NOT NULL (informational by default)
      Can block writes that violate (Databricks)

   OPEN SPECS (2024-2026):
      Data Contract CLI (data-contract.com)
      ODCS (Open Data Contract Standard, Bitol)
      PayPal Data Contract Template
```

Example dbt contract:
```yaml
models:
  - name: dim_customer
    config:
      contract:
        enforced: true
    columns:
      - name: customer_id
        data_type: bigint
        constraints:
          - type: not_null
          - type: unique
      - name: email
        data_type: varchar(320)
        constraints:
          - type: not_null
```

### 17. Contract Enforcement

```
ENFORCEMENT POINTS:
   
   SOURCE-SIDE (shift-left):
      Producer's CI validates schema before deploy
      Prevents breaking changes from being published
      HARDEST to implement politically (producers push back)
   
   INGESTION-SIDE:
      Validation at the first warehouse touchpoint
      Reject or quarantine data that violates
      Easy to implement; late to detect
   
   CI-SIDE (dbt / transformation layer):
      PR checks ensure downstream models conform to contracts
      Catch schema evolution breaks before merge
   
   RUNTIME (observability):
      Monitor + alert on contract violations
      Last line of defense

STAFF SIGNAL: a contract that only runs on consumer side is a
   WARNING SYSTEM, not a contract. True contracts enforce at the
   producer. Argue for shift-left; recognize the organizational cost.
```

### 18. Breaking Change Management

```
PRINCIPLES:
   Breaking changes are a FACT of life — never zero.
   The question is: do they ambush consumers or go through a process?

PROCESS:
   1. PROPOSE: producer opens an RFC / PR
   2. REVIEW: consumers comment on impact
   3. SCHEDULE: breaking change gets a deprecation window (30+ days typical)
   4. DUAL-WRITE / DUAL-READ: old + new versions coexist during window
   5. CUTOVER: coordinated migration
   6. REMOVE: old version deleted

ANTIPATTERNS:
   • Change schema silently; consumers find out when it breaks
   • "Dumping ground" approach: new column added without notice; consumers 
      must opt-in to the breakage
   • No deprecation window; "we're removing X next Tuesday"

STAFF ANSWER: an org's breaking-change process reveals its data maturity.
   Senior-level answer: "we announce changes in Slack."
   Staff-level answer: "contracts are versioned; breaking changes bump
   major version; old version supported for a quarter; consumers migrate
   on their timeline; CI gates block regressions."
```

---

## PART VI — SLAs, SLOs, INCIDENT RESPONSE

### 19. SLIs, SLOs, SLAs for Data

```
Taxonomy from SRE applied to data:

   SLI (indicator): what you MEASURE
      "data freshness" = time from source event to warehouse row
      "row count" = daily rows ingested
      "quality score" = fraction of rows passing all tests

   SLO (objective): internal TARGET
      "99% of daily jobs complete by 8am UTC"
      "freshness < 1 hour, 99.5% of the time"

   SLA (agreement): EXTERNAL commitment to consumers
      "revenue dashboard refreshed by 9am; penalty if missed 3× in a quarter"

PATTERN:
   SLOs are stricter than SLAs (buffer for the business)
   Error budgets: 1 - SLO = allowed failure budget
   Burn alerts: fire when the budget depletes faster than expected

STAFF SIGNAL: in interviews, when asked "how do you measure data quality?"
   the staff answer defines SLIs, SLOs, SLAs; the senior answer lists tests.
```

### 20. On-Call for Data Teams

```
MATURITY LADDER:
   Level 0: no on-call. Someone notices during business hours.
   Level 1: Slack channel + opportunistic triage.
   Level 2: PagerDuty + rotation + severity policies.
   Level 3: on-call compensation, runbooks, blameless postmortems,
            MTTR/MTBF tracking.

TIERED ALERTING:
   P1 — WAKE: revenue-impacting or regulatory dataset breaks SLA
      Response: < 15 min acknowledge
   P2 — DURING-HOURS: non-critical quality issue
      Response: next business day
   P3 — LOG: anomaly worth investigating; no immediate action
      Response: weekly review

RULES:
   • Alerts must have a runbook
   • Alerts that fire with no action get DELETED (not snoozed)
   • Postmortems for every P1
   • MTTR trending down over quarters, not just absolute number
```

### 21. Incident Response Playbook

```
When a P1 fires:

   1. ACKNOWLEDGE (<5 min)
      On-call pages, acknowledges on PagerDuty
      Opens incident channel in Slack

   2. ASSESS (<15 min)
      Scope: which tables? how many downstream consumers?
      Impact: dashboards wrong? ML models stale? exec report broken?

   3. COMMUNICATE
      Status page updated
      Message to affected teams: known issue, ETA, no action required (or asked)
      Executives notified if revenue/compliance-impacting

   4. MITIGATE
      Short-term: freeze the broken table, serve previous snapshot
      Mid-term: fix the pipeline, rerun, validate
      Repoint consumers once validated

   5. RESOLVE
      Confirm all consumers unblocked
      Close incident

   6. POSTMORTEM (within 1 week)
      Timeline
      Root cause
      Contributing factors
      Action items with owners + dates
      Shared broadly (blameless)

STAFF SIGNAL: the POSTMORTEM is where staff engineers shine.
   Not "whose fault" — "what systemic property allowed this?"
   The best postmortems leave the team with permanently better
   detection, not just a specific bug fix.
```

---

## PART VII — OPERATING MODEL

### 22. Data Mesh and Ownership

```
DATA MESH (Zhamak Dehghani, 2019):
   Four principles:
      1. Domain-oriented decentralized data ownership
      2. Data as a product
      3. Self-serve data platform
      4. Federated computational governance

Not every org needs data mesh. In practice:
   • Small orgs: central team is fine (1-2 pizza)
   • Medium: hybrid — platform team + embedded engineers in domain teams
   • Large: domain ownership becomes mandatory

ROLES:
   DATA PRODUCT OWNER (in domain): defines what the product is + its SLAs
   DATA PLATFORM TEAM: provides the tools + standards + observability
   DATA GOVERNANCE COUNCIL: cross-domain decisions (naming, security, etc.)
```

### 23. Blast Radius and Change Management

```
BLAST RADIUS = how many downstream consumers / dollars / dashboards
   a given change can affect.

PATTERNS:
   • Tier tables by blast radius (TIER-1 = critical, TIER-4 = exploratory)
   • Reviews required proportional to tier
   • Tier-1 changes require: contract RFC + migration plan + 
     consumer sign-off + rollback plan + staging validation
   • Tier-4 changes: just push

Lineage + usage telemetry drive the tiering:
   "This table is queried 10,000 times/day by 50 users and powers
      the CFO dashboard" → Tier 1
   "This table was created by an analyst 2 months ago; 2 queries 
      in the last month" → Tier 4

STAFF SIGNAL: proportional-process is a staff-level signal.
   Senior engineers want uniform rigor; staff engineers want
   RIGHT-SIZED rigor.
```

### 24. The Shift-Left Pattern

```
SHIFT-LEFT = move validation and ownership upstream.

Traditional pattern (consumer-side):
   Producer writes whatever → warehouse ingests → consumer tests detect bugs
   PROBLEM: by the time consumer detects, data is already stale, 
            downstream reports are already wrong, and the producer
            doesn't feel the pain they caused.

Shift-left pattern:
   Producer writes to a schema registry FIRST → CI validates → 
      only valid data flows → consumers rely on contracts
   
   PRODUCER feels the breakage directly (CI block, deploy fail).

RISKS:
   Political: producers don't want to be slowed down
   Technical: not all sources support schema registration (SaaS APIs)
   Cultural: requires shared ownership of data quality

STAFF SIGNAL: shift-left is a MULTI-QUARTER CHANGE, not a quarter.
   It involves CFO conversations about data as an asset, 
   engineering leadership conversations about team scope,
   and tool choices (dbt contracts, UC constraints, 
   schema registry, data contract CLI).
```

---

## PART VIII — INTERVIEW PRACTICE

### 25. The Staff-Level Data Quality Interview

```
COMMON QUESTIONS:
   1. "How do you know your data pipelines are correct?"
   2. "A dashboard shows wrong numbers — walk me through debugging."
   3. "Design a data quality strategy for a 500-person org."
   4. "Compare dbt tests vs Great Expectations vs Soda."
   5. "What is a data contract? Who enforces it?"
   6. "You've just inherited a flaky pipeline — your first week?"
   7. "SLAs for data: how do you set them?"
   8. "Build vs buy observability platform?"
   9. "How do you prevent breaking schema changes?"
   10. "Data mesh — what does it actually mean?"
```

### 26. Worked System Design

```
Q: Design the data quality program for a 400-person 
   fintech. 2,000 critical tables. 60 producer teams.
   Daily financial reporting. Monthly regulatory filings.
   
STAFF ANSWER (compressed):

LAYERS OF VALIDATION:
   
   L1 (SOURCE / INGEST):
      Schema registry for streaming (Confluent for Kafka)
      Avro contracts enforced at producer CI
      Dead-letter queue for messages violating schema
      
   L2 (RAW LANDING):
      Great Expectations suites on first warehouse touchpoint
      Row count, null rate, schema shape
      Fail fast; alert producer team
      
   L3 (SILVER TRANSFORMS):
      dbt generic tests (unique, not_null, accepted_values)
      Business rules via singular tests
      Reconciliation to source systems (SQL check nightly)
      
   L4 (GOLD / CONSUMPTION):
      Metric-level contracts: deliberately certified metrics
      Audit test: this month's revenue = sum of this month's orders
      Monte Carlo for anomaly detection on tier-1 tables
      
   L5 (OBSERVABILITY):
      Monte Carlo for all tier-1 and tier-2 tables (~300 total)
      Column-level lineage (Monte Carlo + OpenLineage)
      Elementary for dbt-specific operational metrics

ORGANIZATION:
   Platform team: owns tooling, standards, platform SLAs
   Domain teams: own their data products
   Governance council: cross-cutting policy
   
   RACI published, reviewed quarterly.

SLA / SLO:
   Daily financial report: freshness < 6h, SLA 99.9%
   Regulatory filings: 100% accuracy; signed off by data steward
   BI dashboards (non-critical): freshness < 24h, 99% SLA
   Exploratory tables: no SLA

INCIDENT RESPONSE:
   PagerDuty for L1-L2 of tier-1 tables (page on freshness breach)
   Slack-only for tier-2
   Incident channel template, status page, blameless postmortems
   Monthly reliability review with execs

CONTRACTS:
   Data contracts in git (YAML, published to DataHub)
   Breaking changes require RFC + consumer approval + 30-day window
   Dual-write pattern for breaking migrations

COST:
   Monte Carlo: ~$200K/year (list)
   DataHub self-hosted: ~$100K/year (infra + team time)
   Elementary open source: free
   Internal tooling + processes: ~1.5 FTE on platform team
   Total: ~$500K/year — defensible given regulatory exposure

MIGRATION PLAN:
   Q1: tier the 2,000 tables; focus on 300 tier-1
   Q2: deploy Monte Carlo + contracts for tier-1
   Q3: shift-left to source schemas with top-10 producer teams
   Q4: full data mesh rollout; retire legacy quality tools
```

### 27. War Stories

> **🔥 Production Scenario — The Silent Null Explosion.** A team shipped a schema change in Postgres: a column became nullable. Downstream warehouse quietly started seeing nulls in a field used as a JOIN key. 2 weeks later, revenue reports showed a 15% drop. Root cause: silent null propagation; no contract enforcement at source. Fix: contract added, backward incompatibility caught in CI from then on. Lesson: contracts prevent an entire CLASS of silent failures.

> **🔥 Production Scenario — The Observability Alert Fatigue Problem.** A team deployed Monte Carlo on 500 tables. Within 2 weeks: 300+ alerts/week; team ignored them. Fix: tiered alerts by table criticality, deleted monitors that fired without action, invested in alert hygiene. Alert volume dropped 80%, actionable rate rose to 80%. Lesson: observability is only as good as alert quality; prune aggressively.

> **🔥 Production Scenario — The Schema Change That Took Down Marketing.** A product engineer renamed a column `user_id → account_id`. CI passed (unit tests). Deploy went through. 20 downstream dbt models + 15 dashboards broke simultaneously at the next refresh. Fix: added dbt contracts on source models, mandatory lineage check in product CI (if column changed, flag impacted downstream). Lesson: shift-left requires UPSTREAM visibility into downstream impact.

> **🔥 Production Scenario — The Freshness SLO Negotiation.** Data team committed to 4h freshness. Business wanted 1h. Infra cost to go from 4h → 1h: 4x compute. Instead: team negotiated SLO per CONSUMER (tier-1 use cases got 1h via dedicated streaming path; tier-2 got batch 4h). Cost: 1.3x, not 4x. Lesson: uniform SLOs are expensive; TIERED SLOs match cost to value.

> **🔥 Production Scenario — The Postmortem That Changed Everything.** An incident: wrong metric on CFO dashboard due to a daylight-saving-time timestamp bug. Postmortem's action items: (a) fix the specific bug, (b) add a test for DST transitions, (c) require all timestamp-sensitive aggregations to run DST tests, (d) staff training on time zones in SQL. Months later: a different DST bug was caught by the generalized check from (c). Lesson: good postmortems create systemic learning, not just point fixes.

### 28. Cheat Sheet &amp; Mental Models

#### 28.1 The One-Minute Pitch

```
Data quality is a DETECTION problem solved in LAYERS:
   
   L1 CONTRACTS at source (schema registry, dbt contracts)
   L2 INGEST tests (Great Expectations, Soda)
   L3 TRANSFORM tests (dbt generic + singular)
   L4 METRIC validations (reconciliation to source)
   L5 OBSERVABILITY (Monte Carlo, Elementary, Bigeye, Datafold)
   
Combined with:
   SLIs / SLOs / SLAs; tiered by criticality
   Incident response with blameless postmortems
   Lineage (OpenLineage / Marquez) for impact + root cause
   Shift-left discipline + data mesh ownership

The STAFF SIGNAL is the operating model, not the tools.
```

#### 28.2 Senior vs Staff Compressed

| Senior | Staff |
|--------|-------|
| Writes tests. | Designs the layer of tests + what each layer catches. |
| Alerts on failures. | Tiers alerts, deletes noisy ones, measures MTTR. |
| Documents schema. | Enforces contracts; shift-left; manages breaking changes. |
| Does postmortems. | Writes postmortems that create systemic learning. |
| Picks a tool. | Picks a TIERED tool strategy sized to criticality + cost. |

> **🧠 Principal Mindset:** The data quality program is the single clearest reflection of a data org's maturity. Immature orgs have ad-hoc testing and firefighting; mature ones have layered defense, SLOs, lineage, and blameless learning. In interviews, if you can describe not just the tools but the *organizational contract* behind them, you signal staff.

---

[← Back to main](./README.md)
