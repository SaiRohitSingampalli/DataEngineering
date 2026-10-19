# Topic 30 — Advanced Data Modeling

> Standalone deep-dive · Data Vault 2.0, Anchor Modeling, Activity Schema, Entity-Centric

[← Back to main](./README.md)

---

## How to use this document

The dbt chapter (`05-dbt-modeling.md`) covers Kimball dimensional modeling in depth. This chapter is for the alternatives that come up in modeling-heavy roles: Data Vault 2.0 (financial-services favorite), Anchor Modeling (highly normalized), Activity Schema (Narrator-pioneered OBT variant), Entity-Centric modeling (behavioral data). Each solves a different tension in dimensional modeling.

> **🧠 Principal Mindset:** Modeling is a FORCING FUNCTION on data quality, auditability, and change management. The "right" model depends on your trade-off priorities: speed to insight (Kimball), auditability (Vault), flexibility (Anchor), behavior analysis (Activity). Staff engineers pick the model to fit the ORG, not the data.

## Table of Contents

**PART I — FOUNDATIONS**
- 1. What Advanced Modeling Addresses
- 2. The Trade-off Space
- 3. Decision Factors

**PART II — DATA VAULT 2.0**
- 4. The Three Entities
- 5. Hubs
- 6. Links
- 7. Satellites
- 8. Vault Load Patterns
- 9. Business Vault + Information Marts

**PART III — ANCHOR MODELING**
- 10. Anchor Design Principles
- 11. The Four Components
- 12. Temporal Modeling in Anchor
- 13. Anchor vs Vault

**PART IV — ACTIVITY SCHEMA**
- 14. The Single-Table Pattern
- 15. The 11-Column Schema
- 16. Narrator's Methodology
- 17. Trade-offs

**PART V — ENTITY-CENTRIC MODELING**
- 18. From Events to Entities
- 19. Feature Store Patterns
- 20. Snapshot Tables

**PART VI — SCD PATTERNS BEYOND TYPE 2**
- 21. Classical SCD Types Recap
- 22. SCD Type 2 Variants
- 23. Bi-Temporal Modeling

**PART VII — MIGRATION PATTERNS**
- 24. Kimball to Vault
- 25. Vault to Information Marts
- 26. Model Change Management

**PART VIII — INTERVIEW PRACTICE**
- 27. The Staff-Level Modeling Interview
- 28. Worked Example
- 29. War Stories
- 30. Cheat Sheet &amp; Mental Models

---

## PART I — FOUNDATIONS

### 1. What Advanced Modeling Addresses

```
KIMBALL (star schema): optimized for analyst queries + BI tools.
   Strengths: easy to query, denormalized, fast.
   Weaknesses: schema evolution hard, audit trail limited, 
      source-of-truth questions ambiguous.

DATA VAULT: optimized for AUDITABILITY + agile schema evolution.
   Strengths: track all source changes, scale schema across many sources.
   Weaknesses: complex; queries require assembly; needs information marts.

ANCHOR MODELING: optimized for SCHEMA FLEXIBILITY.
   Strengths: every attribute change is zero-impact.
   Weaknesses: extreme normalization; many joins.

ACTIVITY SCHEMA: optimized for BEHAVIORAL ANALYSIS.
   Strengths: time-series of actions; natural for marketing/analytics.
   Weaknesses: not a fit for all data (strongly behavioral bias).

ENTITY-CENTRIC: optimized for FEATURE ENGINEERING.
   Strengths: time-aware features from events; ML-friendly.
   Weaknesses: harder for ad-hoc analytics than star schema.
```

### 2. The Trade-off Space

```
                   AUDIT TRAIL
                      ↑
                  Data Vault
                      │
                 Anchor Modeling
                      │
   SCHEMA FLEXIBILITY ─┼─ Entity-Centric ─ QUERY SIMPLICITY
                      │
                 Activity Schema
                      │
                   Kimball
                      ↓
                    SIMPLICITY

NO MODEL WINS ALL AXES. Choose by what the org needs.
```

### 3. Decision Factors

```
PICK DATA VAULT if:
   • Regulated industry (finance, insurance, healthcare)
   • Many sources with volatile schemas
   • Compliance requires source-fidelity audit trail
   • Team willing to invest in modeling discipline

PICK KIMBALL if:
   • BI-centric; analysts drive the model
   • Few sources, stable schemas
   • Speed to insight is top priority

PICK ACTIVITY SCHEMA if:
   • Behavioral analytics is the primary use case
   • Customer journey analysis
   • Marketing attribution

PICK ANCHOR if:
   • Schema volatility is extreme
   • Regulatory requirements around historical accuracy are strict
   • You have strong SQL + optimizer confidence (many joins)

PICK ENTITY-CENTRIC if:
   • ML / feature store is the primary consumer
   • Time-aware features needed
   • Combining events and state

STAFF SIGNAL: in interviews, articulate WHY a model fits a given org,
   not WHICH model is "best."
```

---

## PART II — DATA VAULT 2.0

### 4. The Three Entities

```
DATA VAULT uses three primary entity types:

   HUB:
      Represents a business entity (Customer, Product, Order)
      Contains: business key + load timestamp + source
      Insert-only (append-only grain)
   
   LINK:
      Represents a relationship between hubs
      Contains: hub references + load timestamp + source
      Insert-only
   
   SATELLITE:
      Contains attributes (descriptive data) of hubs or links
      Historical versions kept via load timestamp
      Insert-only; new row per change

Three types, three tables per logical entity. Normalization is aggressive.
```

### 5. Hubs

```sql
-- HUB_CUSTOMER
CREATE TABLE hub_customer (
    customer_hk CHAR(32)    PRIMARY KEY,  -- hash of business key
    customer_bk VARCHAR(50) NOT NULL,     -- the business key itself
    load_ts     TIMESTAMP   NOT NULL,
    source      VARCHAR(50) NOT NULL      -- Shopify, Stripe, etc.
);

-- The hash key is deterministic (MD5/SHA2 of business key)
-- Enables multi-system load without coordination
```

HUB PROPERTIES:
   • Insert-only (append)
   • Hash key for uniqueness + parallelism
   • Multiple sources can insert; conflicts resolved via same hash

### 6. Links

```sql
-- LINK_CUSTOMER_ORDER
CREATE TABLE link_customer_order (
    customer_order_hk  CHAR(32)    PRIMARY KEY,  -- hash of customer_bk + order_bk
    customer_hk        CHAR(32)    NOT NULL,
    order_hk           CHAR(32)    NOT NULL,
    load_ts            TIMESTAMP   NOT NULL,
    source             VARCHAR(50) NOT NULL
);

-- Link also contains hash key; relates two hubs
-- Insert-only; records the EXISTENCE of a relationship at a point in time
```

### 7. Satellites

```sql
-- SAT_CUSTOMER
CREATE TABLE sat_customer (
    customer_hk    CHAR(32)   NOT NULL,
    load_ts        TIMESTAMP  NOT NULL,
    load_end_ts    TIMESTAMP,            -- when superseded
    hash_diff      CHAR(32)   NOT NULL,  -- hash of all attributes (for change detection)
    source         VARCHAR(50) NOT NULL,
    first_name     VARCHAR(100),
    last_name      VARCHAR(100),
    email          VARCHAR(100),
    phone          VARCHAR(20),
    address        VARCHAR(500),
    PRIMARY KEY (customer_hk, load_ts)
);

-- New row inserted when ANY attribute changes
-- hash_diff enables fast change detection on load
-- load_end_ts closes the old row (optional; some vaults keep it open-ended)
```

### 8. Vault Load Patterns

```
LOAD ORDER:
   1. HUBS first (parallel loads from all sources)
   2. LINKS next
   3. SATELLITES last

PARALLELISM: hash keys enable parallel loads from multiple sources
   without race conditions.

CHANGE DETECTION:
   For each source row, compute hash_diff of attributes
   If hash_diff differs from current satellite row → new row inserted

DELETES:
   Soft deletes via "zero records" (dedicated satellite indicating deletion)
   OR: effectivity satellite with start/end dates

PATTERN:
   Stage source data → compute hashes → UPSERT hubs → UPSERT links → 
   INSERT new satellites (only when hash_diff changes)

TOOLING:
   automate_dv (dbt package): opinionated Vault loader
   datavault4dbt: another dbt package for Vault
```

### 9. Business Vault + Information Marts

```
RAW VAULT: source-fidelity (ingested as-is)

BUSINESS VAULT: derived structures
   • Point-in-time (PIT) tables: pre-joined satellites for a given timestamp
   • Bridge tables: pre-joined link chains
   • Computed satellites: derived attributes

INFORMATION MARTS: consumer-facing dimensional model (Kimball-shaped)
   • Views or materialized tables
   • Star schema for BI tools
   • Query-friendly abstraction over raw vault

ARCHITECTURE:
   SOURCE → RAW VAULT (insert-only, audit) → BUSINESS VAULT (derived) → MARTS (Kimball)

STAFF SIGNAL: the information mart layer is NON-NEGOTIABLE. End users
   don't query the vault directly. The vault is for ETL + audit; marts
   are for analytics.
```

---

## PART III — ANCHOR MODELING

### 10. Anchor Design Principles

```
ANCHOR MODELING (Lars Rönnbäck, 2004+):
   Highly normalized; 6th Normal Form
   Each attribute in its own table
   Schema changes are additive (never destructive)
   Time-aware throughout

Core idea: every change is a NEW TABLE, never an ALTER.
   Need a new attribute? Create a new attribute table.
   Need to track versions? Automatic.
```

### 11. The Four Components

```
   ANCHOR: identity (business entity)
      Table with just an ID column (surrogate key)
   
   ATTRIBUTE: a property of an anchor
      Table with (anchor_id, value, valid_from, source) — one PER attribute
   
   TIE: relationship between anchors
      Table with multiple anchor_ids + metadata
   
   KNOT: enumeration / small reference set
      Table with constrained values (gender, status, etc.)

Example:
   ANCHOR: AC_Customer   (CustomerID)
   ATTRIBUTE: AT_Customer_Name     (CustomerID, Name, ValidFrom)
   ATTRIBUTE: AT_Customer_Email    (CustomerID, Email, ValidFrom)
   ATTRIBUTE: AT_Customer_DOB      (CustomerID, DOB)
   TIE: TI_Customer_Order          (CustomerID, OrderID, ValidFrom)
   KNOT: KN_Status                 (StatusID, StatusCode)
```

### 12. Temporal Modeling in Anchor

```
ANCHOR embraces TEMPORALITY natively:
   
   Every attribute row has a VALID_FROM timestamp
   Current value = row with latest valid_from
   Historical point-in-time = row with largest valid_from ≤ query time
   
   No "end" timestamp usually — derived from NEXT row's valid_from
   This keeps attributes insert-only (nice for CDC and audit)

Queries assemble current state via self-join:
   SELECT c.id, 
          n.Name,
          e.Email
   FROM AC_Customer c
   LEFT JOIN AT_Customer_Name n ON c.id = n.CustomerID
       AND n.ValidFrom = (SELECT MAX(ValidFrom) FROM AT_Customer_Name 
                          WHERE CustomerID = c.id)
   LEFT JOIN ...
```

### 13. Anchor vs Vault

```
BOTH:
   Insert-only (audit-friendly)
   Agile schema evolution
   Modeling discipline required

DIFFERENCES:
   
   VAULT: three tables per entity (hub, link, N satellites)
      Attributes GROUPED in satellites
      Satellites can split by source + rate of change
   
   ANCHOR: hundreds of tables per entity (one per attribute)
      Attributes INDIVIDUALLY tabled
      Even stricter normalization

VAULT WINS:
   More operationally common
   Better tooling (dbt packages, Vault SMEs)
   Satellites group related attributes practically

ANCHOR WINS:
   Theoretically cleanest
   Changes to one attribute never ripple
   Academic rigor

In practice: VAULT dominates in industry; ANCHOR has niche advocates.
```

---

## PART IV — ACTIVITY SCHEMA

### 14. The Single-Table Pattern

```
ACTIVITY SCHEMA (Narrator-pioneered; open-sourced ~2020):
   
   One table (per domain, often per customer type).
   One row per activity (an event / interaction / action).
   
   Example: "customer visited site", "customer purchased",
      "customer opened email", "customer called support".
   
   The ENTIRE model for customer behavior is ONE TABLE.
```

### 15. The 11-Column Schema

```sql
CREATE TABLE customer_activity (
    activity_id         UUID,
    customer_id         VARCHAR(50),
    ts                  TIMESTAMP,          -- when the activity happened
    activity            VARCHAR(100),       -- what happened ("purchased", "viewed_product")
    feature_1           VARCHAR,            -- activity-specific property
    feature_2           VARCHAR,
    feature_3           VARCHAR,
    revenue_impact      NUMERIC,
    link                VARCHAR,            -- URL or reference
    activity_occurrence INTEGER,            -- Nth occurrence for this customer
    activity_repeated_at TIMESTAMP          -- when the next occurrence happened
);

-- Columns designed for the common analytical patterns:
--   funnel analysis (did activity A lead to activity B)
--   retention (occurrence counting)
--   attribution (revenue_impact)
--   cohort analysis
```

### 16. Narrator's Methodology

```
Narrator (the vendor) pioneered activity schema with:
   
   RELATIONSHIPS: declarative links between activities ("before", "after", "within")
   
   QUERY LAYER: a semantic layer that generates SQL for business questions
   
   DATASETS: pre-computed derived tables for performance
   
   EXAMPLE QUESTION:
      "What % of customers who visited the pricing page purchased within 7 days?"
      → Translates to activity-schema SQL with "before" relationship
```

### 17. Trade-offs

```
STRENGTHS:
   ✓ Natural for behavioral analytics (marketing, product, growth teams)
   ✓ Single table = simple queries + joins
   ✓ Easy to add new activity types (just more rows)
   ✓ Great for funnels, cohorts, retention, attribution

WEAKNESSES:
   ✗ Not a fit for relational / transactional data (inventory, finance)
   ✗ Wide, sparse table (most columns NULL for any given activity type)
   ✗ Query optimizer can struggle with activity-specific predicates
   ✗ Partitioning strategy matters a lot

PATTERN:
   Pair activity schema with Kimball dimensional for non-behavioral domains.
   Activity for "what happened"; Kimball for "what the entities are".
```

---

## PART V — ENTITY-CENTRIC MODELING

### 18. From Events to Entities

```
ENTITY-CENTRIC MODELING (common in ML / feature stores):
   
   SOURCE: event streams (clicks, orders, logins, support calls)
   TARGET: entity snapshots with time-aware features
   
   For each ENTITY (customer, product, device):
      For each TIME:
         Compute features from events UP TO that time

   Example features for a customer:
      total_orders_30d (as of yesterday)
      days_since_last_order (as of yesterday)
      lifetime_revenue (as of yesterday)
      avg_order_size_90d
      first_order_date
```

### 19. Feature Store Patterns

```
FEATURE STORE architecture:
   OFFLINE STORE: historical features for training (lake / warehouse)
   ONLINE STORE: current features for inference (Redis, DynamoDB, pgvector)
   REGISTRY: feature definitions + versions + metadata

VENDORS/TOOLS:
   Feast (open-source)
   Databricks Feature Store (now called Online Tables + Feature Serving)
   Tecton (managed)
   Hopsworks
   SageMaker Feature Store
   Snowflake Feature Store (part of ML)

FEATURE DEFINITIONS:
   Point-in-time correct: features computed as of the LABEL event time
   (avoid data leakage: don't use features computed from future events)
   
PATTERN:
   Events → feature pipeline (Spark/Flink) → offline store (training)
   Events → feature pipeline → online store (inference)
   Features reconciled: training-serving skew eliminated by shared definitions
```

### 20. Snapshot Tables

```
SNAPSHOT TABLE PATTERN:
   
   For each date, store ENTIRE entity state as it was that date.
   
   customer_snapshots:
      (customer_id, snapshot_date, name, segment, status, lifetime_revenue, ...)
   
   Row per (customer, date) — explosion if daily × years
   
   USE: training ML models with time-aware labels
   USE: ad-hoc point-in-time queries without temporal SQL

Variants:
   DAILY SNAPSHOTS (full daily state)
   CHANGE-ONLY SNAPSHOTS (only rows that changed; smaller)
   EVENT-SOURCED (reconstruct snapshot from events on demand)

PATTERN:
   Snapshots complement activity schema for ML:
      Activity schema: what happened
      Snapshots: what the state was at each time
```

---

## PART VI — SCD PATTERNS BEYOND TYPE 2

### 21. Classical SCD Types Recap

```
SCD = Slowly Changing Dimension

TYPE 0: no change tracked (overwrite)
TYPE 1: overwrite (no history)
TYPE 2: historical versions (new rows with start/end dates)
TYPE 3: previous value in a separate column (limited history)
TYPE 4: history in separate table
TYPE 6: hybrid of 1+2+3 (current + historical + previous)

Kimball's standard is Type 2 for most dimensions.
```

### 22. SCD Type 2 Variants

```
EFFECTIVITY DATES:
   valid_from, valid_to
   current indicator (is_current flag)
   Query current: WHERE is_current = TRUE
   Query historical: WHERE valid_from ≤ event_ts < valid_to

HASH-DIFF LOAD:
   Compute hash of attributes; new row only if hash differs
   Fast change detection without row comparison

LATE-ARRIVING DIMENSION:
   Event arrives REFERENCING a dim row that doesn't exist yet
   Pattern 1: insert placeholder dim row; populate later
   Pattern 2: hold event in staging until dim row arrives
   Pattern 3: use natural keys (not surrogate) in facts; join at query time

LATE-ARRIVING FACTS:
   Fact arrives referencing a point-in-time dim state
   Query must use valid_from ≤ event_ts < valid_to to find the right dim row
```

### 23. Bi-Temporal Modeling

```
BI-TEMPORAL: track TWO time axes:
   
   VALID TIME: when the fact is true in the real world
   TRANSACTION TIME: when the fact was recorded in the system
   
   Enables: "what did we KNOW on date X about state at time Y?"

Columns:
   valid_from, valid_to  (business time)
   trans_from, trans_to  (system time)

Four combinations give FULL historical AUDIT:
   • Current state (both current)
   • What we know now about past (valid past, trans current)
   • What we knew then about future (valid current, trans past)
   • What we knew then about past (both past)

USE CASES:
   Insurance claims (when was the policy effective; when did we KNOW)
   Financial reporting (as-of-date reporting with restatements)
   Regulatory audits ("show me what you reported in Q3 2023")

STAFF SIGNAL: bi-temporal is advanced; mention when the use case
   genuinely requires it. Overkill for most analytics.
```

---

## PART VII — MIGRATION PATTERNS

### 24. Kimball to Vault

```
MIGRATION STEPS:
   1. Identify business keys per dim/fact
   2. Create hubs for each business entity
   3. Create links for relationships (facts become links + satellites)
   4. Create satellites: SCD2 history → satellite history
   5. Build information marts matching current Kimball shape
   6. Switch BI tools to information marts
   7. Rebuild downstream from marts (not raw tables)

RISKS:
   • Query complexity increases (ingestion); mitigate with marts
   • Load process becomes more complex; invest in tooling
   • Team learning curve: 2-3 months typical
```

### 25. Vault to Information Marts

```
INFORMATION MART types:
   
   DIMENSIONAL (Kimball-style): fact + dim tables
      Pre-computed via dbt from vault
      Updated incrementally
   
   WIDE / OBT: one wide table per subject area
      Faster for BI tools; harder to maintain
   
   DATAVERSE-STYLE: typed views on vault
      Current-state views for operational analytics

MOST COMMON: dimensional marts for BI; OBT for simple reports;
   derived point-in-time tables for ML.
```

### 26. Model Change Management

```
CHANGE TYPES:
   
   ADDITIVE (safe):
      New hub, link, satellite
      New column in existing satellite
   
   NON-ADDITIVE (dangerous):
      Rename / drop column in satellite
      Change hash key definition
      Restructure satellite split

PROCESS for non-additive changes:
   RFC → impact review → migration plan → dual-write period → cutover → cleanup

STAFF SIGNAL: Vault explicitly LIMITS non-additive changes.
   Discipline enforced by the model, not by heroics.
```

---

## PART VIII — INTERVIEW PRACTICE

### 27. The Staff-Level Modeling Interview

```
COMMON QUESTIONS:
   1. "Compare Kimball and Data Vault."
   2. "When would you use Data Vault?"
   3. "Explain hub, link, satellite."
   4. "What's an activity schema?"
   5. "Bi-temporal modeling: when + why?"
   6. "SCD Type 2 vs snapshot tables for ML features."
   7. "Design a model for a 50-source insurance data platform."
   8. "Model schema evolution: your approach."
   9. "Feature store vs analytical model."
   10. "Activity Schema vs event sourcing."
```

### 28. Worked Example

```
Q: Design a data model for a mid-size insurance company:
   40 source systems (policy admin, claims, underwriting, billing,
      broker/agent CRM, regulatory feeds, etc.)
   Compliance: 7-year retention; auditable; point-in-time recovery
   Analytics: BI dashboards + actuarial reporting + ML pricing

STAFF ANSWER:

MODEL: DATA VAULT 2.0 + DIMENSIONAL INFORMATION MARTS + 
       FEATURE STORE FOR ML

ARCHITECTURE:
   
   SOURCE SYSTEMS → BRONZE (raw ingested) → RAW VAULT (hubs, links, satellites)
       ↓
   BUSINESS VAULT (point-in-time, bridge, derived)
       ↓
   INFORMATION MARTS (dimensional for BI; wide for ad-hoc)
       ↓
   FEATURE STORE (entity-centric features for ML)

RAW VAULT:
   Hubs: Policy, Claim, Customer, Agent, Vehicle, Property, Underwriter, 
         Payment, Policy_Type, Risk_Factor
   Links: Policy_Customer, Claim_Policy, Payment_Policy, Agent_Policy, etc.
   Satellites per source and per rate-of-change:
      sat_policy_core (slow-changing attributes)
      sat_policy_pricing (medium-changing)
      sat_policy_status (fast-changing)

BUSINESS VAULT:
   PIT_Policy: pre-joined view of all satellites for a policy at any time
   Bridge_Policy_Claims: pre-joined policy to all claims
   Derived satellites: computed KPIs

INFORMATION MARTS:
   Dim_Policy, Dim_Customer, Dim_Agent, Dim_Date, Dim_Product
   Fact_Premium, Fact_Claim, Fact_Payment
   Materialized daily
   dbt-based with tests + lineage

FEATURE STORE (Feast or Databricks FS):
   Point-in-time features for pricing models:
      customer_claim_frequency_1y, 2y, 5y
      customer_total_claims_lifetime
      customer_payment_delinquency_count
   Backed by Vault PIT tables + event streams

COMPLIANCE:
   7-year retention: handled naturally by vault's insert-only pattern
   Point-in-time recovery: 
      "What did we know on date X about customer Y?"
      Query bi-temporal satellite with trans_from/trans_to
   Regulatory audits: full lineage from vault to marts

MIGRATION PATH (if from legacy Kimball):
   Q1: pilot vault with 2-3 source systems
   Q2-3: expand to 20 sources; build marts for critical dashboards
   Q4: cut legacy marts; all sources on vault

TOOLS:
   dbt + automate_dv or datavault4dbt
   Databricks or Snowflake as compute
   Feast or managed feature store
   Monte Carlo for observability
```

### 29. War Stories

> **🔥 Production Scenario — The Vault Rescue.** An insurance co's Kimball warehouse was constantly broken by source schema changes. Every quarter: 2 weeks of fixes. Migrated to Data Vault: schema changes became additive (new satellites, new satellite columns). Information marts updated separately. Downstream BI stable. Lesson: Vault pays for its complexity when schema volatility is high.

> **🔥 Production Scenario — The Activity Schema Simplification.** A product analytics team had 50 fact + dim tables for customer behavior. Queries required 5+ joins; analysts struggled. Migrated to activity schema: 1 wide table with 10 columns. Queries became trivial. Caveat: non-behavioral data (inventory, finance) stayed in Kimball. Lesson: match MODEL to USE CASE; don't force one model everywhere.

> **🔥 Production Scenario — The Bi-Temporal Audit.** A financial regulator asked: "What did your firm KNOW about customer X's risk profile on March 15, 2023?" With bi-temporal vault: query trans_from ≤ 2023-03-15 < trans_to to see system-knowledge-at-time. Answered in minutes. Without: would have taken days of forensic reconstruction. Lesson: bi-temporal is a regulatory asset.

> **🔥 Production Scenario — The Vault-Mart Divorce.** A team built a vault but exposed it directly to analysts. Queries required 10+ joins; adoption cratered. Fix: built dimensional marts on top; vault became ETL internal. Lesson: vault is NOT the user-facing layer. Marts are.

> **🔥 Production Scenario — The Feature Store Realization.** An ML team computed features ad-hoc from analytical tables. Point-in-time correctness bugs: training saw future data (data leakage). Model performance degraded in production. Fix: adopted Feast; features defined point-in-time with explicit AS OF timestamps. Training-serving skew eliminated. Lesson: ML features need entity-centric modeling, not analytical-table-reuse.

### 30. Cheat Sheet &amp; Mental Models

#### 30.1 One-Minute Pitch

```
PICK MODEL by ORG NEEDS, not aesthetics:

   KIMBALL: BI-first; speed to insight; stable sources
   DATA VAULT: audit-first; many volatile sources; regulatory
   ANCHOR: extreme flexibility (niche)
   ACTIVITY SCHEMA: behavioral analytics
   ENTITY-CENTRIC: ML features
   
VAULT architecture: Raw Vault → Business Vault → Information Marts
   NEVER expose vault to end users; marts are the UX.

SCD + TEMPORAL:
   Type 2 for most dims
   Bi-temporal when you need to answer "what did we know when"
   Snapshots for ML point-in-time correctness
```

#### 30.2 Senior vs Staff

| Senior | Staff |
|--------|-------|
| "We use Kimball." | "We layer Kimball marts on a Data Vault core. Vault for source-of-truth + audit + schema evolution. Marts for BI UX. Feature store for ML. One architecture, three consumer surfaces." |
| "Data Vault is complex." | "Vault's complexity is a feature when source volatility is high. For a 5-source stable warehouse, Kimball is right. For a 40-source regulated enterprise, Vault's discipline PREVENTS the whack-a-mole that Kimball produces. Match MODEL to CHANGE RATE." |
| "Use SCD Type 2 for dims." | "Type 2 is default. For ML, snapshot tables often beat SCD2 (easier point-in-time features without temporal SQL). For regulatory audit, bi-temporal. Pick the specific temporal pattern to the specific consumer, not one for all." |
| "Activity Schema is just OBT." | "Activity Schema is OBT with STRUCTURE — the 11-column shape makes funnels, cohorts, and attribution first-class queries. Narrator's methodology adds a semantic layer. For behavioral analytics specifically, it's a step up from generic wide tables." |

> **🧠 Principal Mindset:** Advanced modeling is where data engineering becomes a design discipline. Senior engineers execute the chosen model. Staff engineers choose the model — defending the choice against org, team, use-case, and change-rate constraints. If you can argue "Vault with Kimball marts and a feature store" over "pure Kimball" for a specific scenario, you've demonstrated staff-level modeling judgment.

---

[← Back to main](./README.md)
