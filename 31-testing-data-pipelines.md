# Topic 31 — Testing Data Pipelines

> Standalone deep-dive · Unit, integration, data, contract tests + CI/CD

[← Back to main](./README.md)

---

## How to use this document

Testing data pipelines is where many data engineering teams underinvest. Pairs with chapter 23 (Data Quality & Observability). This chapter is for the TEST STRATEGY — what to test, at what level, with what tool, in what pipeline.

> **🧠 Principal Mindset:** Data tests have no equivalent in software engineering's test pyramid. The data world's pyramid is: unit tests on transformations, integration tests on pipelines, data tests on outputs, contract tests on interfaces. Each layer catches a different class of bug.

## Table of Contents

**PART I — FOUNDATIONS**
- 1. Why Data Testing Is Different
- 2. The Testing Pyramid for Data
- 3. Test Types and What They Catch

**PART II — UNIT TESTING**
- 4. Unit Testing Transformations
- 5. Spark Unit Tests
- 6. dbt Unit Tests
- 7. Mocking Sources

**PART III — INTEGRATION TESTING**
- 8. Integration Test Patterns
- 9. Docker Compose for Pipelines
- 10. Testcontainers

**PART IV — DATA TESTS**
- 11. dbt Tests Revisited
- 12. Great Expectations
- 13. Custom SQL Audits

**PART V — CONTRACT TESTS**
- 14. Schema Contract Tests
- 15. Semantic Contract Tests
- 16. Consumer-Driven Contracts

**PART VI — TEST ENVIRONMENTS**
- 17. Fixtures and Synthetic Data
- 18. Production-Like Data (safely)
- 19. Snapshot Testing

**PART VII — CI/CD**
- 20. CI for dbt Projects
- 21. CI for Spark/PySpark
- 22. CD + Promotion
- 23. Blue/Green at the Table Level

**PART VIII — INTERVIEW PRACTICE**
- 24. The Staff-Level Testing Interview
- 25. Worked Example
- 26. War Stories
- 27. Cheat Sheet &amp; Mental Models

---

## PART I — FOUNDATIONS

### 1. Why Data Testing Is Different

```
SOFTWARE TESTING: inputs → outputs; same inputs → same outputs; deterministic.

DATA TESTING: three variables shift simultaneously:
   
   CODE (transformations, SQL, dbt models)
   DATA (sources change daily; distribution drift; schema drift)
   INFRA (warehouse version, lib version, cluster size)

Any of three can break the pipeline; test strategy must address all three.

TYPES OF BUGS:
   LOGIC bugs: in your code (unit tests catch)
   INTEGRATION bugs: across systems (integration tests catch)
   DATA bugs: in the data itself (data tests catch)
   CONTRACT bugs: between producers and consumers (contract tests catch)
```

### 2. The Testing Pyramid for Data

```
   ╱ ╲
  ╱E2E╲         End-to-end (few; slow; expensive)
 ╱─────╲
 ╱       ╲     Integration (some; moderate speed)
╱─────────╲
╱  Unit    ╲   Unit (many; fast; cheap)
╱───────────╲

Data-world additions:
   • DATA TESTS (dbt tests, GX) — run against actual data on every build
   • CONTRACT TESTS — run on PR + at ingestion
```

### 3. Test Types and What They Catch

| Test type | Catches | Example | Speed |
|-----------|---------|---------|-------|
| Unit | Logic bugs | `calculate_tax(100, 0.08) == 8` | ms |
| Integration | System interactions | "Spark reads S3 and writes Delta correctly" | seconds-minutes |
| Data | Data quality issues | "no nulls in `email` column" | seconds-minutes |
| Contract | Interface violations | "producer sent incompatible schema" | ms |
| Regression | Prevents known bugs | "ensure DST bug doesn't recur" | varies |
| Snapshot | Unexpected output drift | "today's output matches golden file" | seconds |

---

## PART II — UNIT TESTING

### 4. Unit Testing Transformations

```python
# A transformation is a FUNCTION: inputs → outputs
# Unit test it like any software function.

def calculate_order_value(items: list[dict]) -> float:
    return sum(item["price"] * item["quantity"] for item in items)

def test_calculate_order_value_single_item():
    assert calculate_order_value([{"price": 10, "quantity": 2}]) == 20

def test_calculate_order_value_empty():
    assert calculate_order_value([]) == 0

def test_calculate_order_value_free_item():
    assert calculate_order_value([{"price": 0, "quantity": 5}]) == 0
```

PRINCIPLE: isolate transformations as pure functions where possible.
   Easier to test, mock, reuse.

### 5. Spark Unit Tests

```python
# Spark unit tests: construct small DataFrames; apply transformation; assert
import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

@pytest.fixture(scope="session")
def spark():
    return SparkSession.builder.master("local[2]").appName("test").getOrCreate()

def apply_discount(df, discount_pct):
    return df.withColumn("final_price", df.price * (1 - discount_pct / 100))

def test_apply_discount(spark):
    schema = StructType([
        StructField("product", StringType()),
        StructField("price", DoubleType()),
    ])
    input_df = spark.createDataFrame([
        ("Widget", 100.0),
        ("Gadget", 50.0),
    ], schema=schema)
    
    result = apply_discount(input_df, 10)
    rows = result.collect()
    
    assert rows[0]["final_price"] == 90.0
    assert rows[1]["final_price"] == 45.0
```

TOOLS:
   `pytest-spark`: fixture + helpers
   `chispa`: compare DataFrames (handles ordering, nulls)
   `spark-testing-base`: Scala/Java helpers

### 6. dbt Unit Tests

```yaml
# dbt 1.8+ has native unit tests

unit_tests:
  - name: test_customer_segmentation
    model: dim_customer_segments
    given:
      - input: ref('stg_customers')
        rows:
          - {customer_id: 1, total_spent: 1000, days_since_signup: 400}
          - {customer_id: 2, total_spent: 50, days_since_signup: 10}
          - {customer_id: 3, total_spent: 5000, days_since_signup: 1000}
    expect:
      rows:
        - {customer_id: 1, segment: "regular"}
        - {customer_id: 2, segment: "new"}
        - {customer_id: 3, segment: "vip"}
```

Run: `dbt test --select "test_type:unit"`

Before dbt 1.8: dbt-unit-testing package (community); less ergonomic but same idea.

### 7. Mocking Sources

```python
# Mock external systems in unit tests (don't hit real APIs/DBs)

from unittest.mock import patch, MagicMock

@patch("my_module.boto3.client")
def test_s3_upload(mock_boto_client):
    mock_s3 = MagicMock()
    mock_boto_client.return_value = mock_s3
    
    my_s3_upload_function(data="hello", bucket="test", key="obj.txt")
    
    mock_s3.put_object.assert_called_once_with(
        Bucket="test", Key="obj.txt", Body="hello"
    )
```

For warehouse-dependent code: use DuckDB or SQLite as a stand-in SQL engine
   in tests (if dialect allows).
```

---

## PART III — INTEGRATION TESTING

### 8. Integration Test Patterns

```
INTEGRATION tests exercise the WIRING: can the pipeline read from S3,
   transform with Spark, write to Delta Lake?

PATTERNS:
   
   LOCAL DOCKER STACK:
      Docker Compose with Postgres + Kafka + MinIO (S3-compatible) + Spark
      Spin up for test suite; tear down after
   
   TESTCONTAINERS:
      Programmatic Docker container management from test code
      Great for Java/Python; integrates with pytest
   
   CLOUD TEST ENVIRONMENTS:
      Dedicated test accounts in AWS/GCP/Azure
      Slower + more expensive but tests real services
```

### 9. Docker Compose for Pipelines

```yaml
# docker-compose.test.yml

version: '3'
services:
  postgres:
    image: postgres:16
    environment:
      POSTGRES_USER: test
      POSTGRES_PASSWORD: test
    ports: ["5432:5432"]
  
  kafka:
    image: confluentinc/cp-kafka:7.5.0
    environment:
      KAFKA_BROKER_ID: 1
      KAFKA_ZOOKEEPER_CONNECT: zookeeper:2181
      KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: PLAINTEXT:PLAINTEXT
      KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://kafka:9092
    depends_on: [zookeeper]
  
  zookeeper:
    image: confluentinc/cp-zookeeper:7.5.0
    environment:
      ZOOKEEPER_CLIENT_PORT: 2181
  
  minio:
    image: minio/minio
    command: server /data
    ports: ["9000:9000"]
    environment:
      MINIO_ROOT_USER: minio
      MINIO_ROOT_PASSWORD: minio123
```

Run tests against this stack:
```python
# Setup fixture that waits for services to be ready
# Test runs end-to-end pipeline using real services
```

### 10. Testcontainers

```python
# Testcontainers manages containers from test code

from testcontainers.postgres import PostgresContainer
import pytest

@pytest.fixture(scope="session")
def postgres():
    with PostgresContainer("postgres:16") as pg:
        yield pg

def test_pipeline_write(postgres):
    conn_url = postgres.get_connection_url()
    # Run pipeline that writes to Postgres
    run_pipeline(connection_url=conn_url, data=sample_data)
    # Query postgres to assert correctness
    with psycopg2.connect(conn_url) as conn:
        rows = conn.cursor().execute("SELECT * FROM target_table").fetchall()
        assert len(rows) == expected_count
```

Testcontainers supports: Postgres, Kafka, MinIO, Localstack, Elasticsearch,
Redis, Clickhouse, and more.

---

## PART IV — DATA TESTS

### 11. dbt Tests Revisited

```
See 23-data-quality-observability for depth.

Standard dbt tests:
   not_null, unique, accepted_values, relationships (FK),
   dbt_utils.not_empty_string, dbt_utils.expression_is_true, ...

Singular tests: SQL that returns failing rows.

Severity + store_failures: tune noise + enable investigation.

RUN:
   dbt test                                   # all tests
   dbt test --select tag:critical             # critical only
   dbt test --select source:raw.orders         # source tests only
```

### 12. Great Expectations

```
See 23-data-quality-observability.

Pattern:
   Checkpoint runs expectation suite against a dataset
   Pass/fail; data docs generated
   Integrated into orchestrator (Airflow, Dagster) as a task

STRENGTHS over dbt tests:
   Python-first; works outside warehouse
   Rich expectation library
   Data docs auto-generated

WEAKNESSES:
   More ceremony for simple cases
   Pre-dbt-1.5 better than now (dbt tests have caught up for warehouse work)
```

### 13. Custom SQL Audits

```
Reconciliation audits: compare downstream to upstream.

Pattern (dbt singular test):
   SELECT ...
   FROM {{ ref('fact_orders') }} wh
   LEFT JOIN {{ source('raw', 'shopify_orders') }} src ON ...
   WHERE wh.amount <> src.amount
      OR src.id IS NULL
   LIMIT 100

Returns rows where warehouse doesn't match source.
Fails (returns rows) if reconciliation breaks.

STAFF SIGNAL: write reconciliation audits for TOP-N tables early.
   This single pattern catches 50% of real data bugs.
```

---

## PART V — CONTRACT TESTS

### 14. Schema Contract Tests

```
See 23-data-quality-observability for depth.

dbt 1.5+ CONTRACTS:

models:
  - name: dim_customer
    config:
      contract:
        enforced: true
    columns:
      - name: customer_id
        data_type: bigint
        constraints: [not_null, unique]

   On dbt run: if actual table schema diverges from contract → ERROR.

   Prevents accidental schema breakage before deploy.
```

### 15. Semantic Contract Tests

```
Beyond schema: TEST THE MEANING of a column.

Example:
   fact_orders.revenue == source.gross_revenue - source.refund_amount
   (not just that revenue is a decimal, but that the FORMULA matches)

Pattern: singular dbt test that implements the formula and asserts equality.

PRINCIPLE: don't just test THAT revenue exists; test THAT revenue is CORRECT.
```

### 16. Consumer-Driven Contracts

```
CONSUMER-DRIVEN CONTRACT TESTING (software pattern applied to data):
   
   Each CONSUMER expresses what they NEED from a dataset
   Producer runs consumer contract tests → if producer changes break 
      consumer expectations → fail CI
   
   Tool patterns:
      Pact (for API contracts); emerging for data
      Custom dbt tests representing consumer contracts
      Monte Carlo + similar observability for runtime consumer tracking

STAFF SIGNAL: consumer-driven contracts are the SHIFT-LEFT ideal 
   (see 23). In practice: still immature in data world. Advocate for
   this; recognize operational complexity.
```

---

## PART VI — TEST ENVIRONMENTS

### 17. Fixtures and Synthetic Data

```
FIXTURES: small controlled datasets for tests.
   
   YAML / JSON for small inputs
   CSV / Parquet for larger
   Generated programmatically for property-based testing

LIBRARIES:
   Faker (Python): generates names, addresses, dates, emails
   dbt seeds: CSV files loaded as tables during tests
   dbt-datamocktool: scaffolded fixtures for dbt

PROPERTY-BASED:
   Hypothesis (Python): generate many inputs, assert properties
   Example: for any valid order, calculate_tax(order) >= 0
```

### 18. Production-Like Data (safely)

```
APPROACHES:
   
   SYNTHETIC GENERATION:
      Faker + domain rules
      Pros: no PII risk
      Cons: may not catch real-data edge cases
   
   SAMPLED PRODUCTION:
      Random or stratified sample of production
      Pros: real distributions
      Cons: PII; GDPR issues
   
   MASKED PRODUCTION:
      Production data with PII masked / anonymized
      Pros: realistic + safer
      Cons: masking is hard to do rigorously
   
   SYNTHETIC FROM PRODUCTION:
      ML-generated data preserving statistical properties but no actual rows
      Tools: Mostly AI, Gretel, Tonic
      Pros: safe + realistic
      Cons: tooling investment

STAFF SIGNAL: for most teams, mask production + stratified sample
   is the right balance. Pure synthetic rarely covers edge cases;
   raw production is a liability.
```

### 19. Snapshot Testing

```
SNAPSHOT TESTS: capture current output; fail if it changes unexpectedly.
   
   Run model → compare output to GOLDEN FILE (previous output)
   Diff = test failure (either bug or intended change)
   
Pattern:
   Save output of model run with specific input to a golden file
   On next test run: compare current output to golden file
   On intentional change: update golden file in the PR

USE:
   Protects against unintentional output drift
   Great for complex transformations where fully specifying expected
      output is tedious

TOOLS:
   dbt-expectations: expect_column_values_to_match_regex, etc.
   Custom: dbt-audit-helper for row-level comparison
```

---

## PART VII — CI/CD

### 20. CI for dbt Projects

```
dbt CI PIPELINE (standard pattern):

   PR OPENED:
      1. Clone repo + install dbt
      2. dbt deps (install packages)
      3. dbt compile (sanity check SQL)
      4. dbt build --target ci --models state:modified+ 
          # Only run models changed in this PR + downstream
          # Materialize in a PR-specific schema
      5. dbt test on changed models
      6. Run data-diff (compare PR output to prod) if used
      7. Clean up PR schema after merge/close

   MERGE TO MAIN:
      1. Full dbt build on production target
      2. Full dbt test
      3. Deploy artifacts (manifest.json, catalog.json)

TOOLS:
   dbt Cloud CI jobs (native)
   GitHub Actions / GitLab CI with dbt
   Datafold for data diff integration
```

### 21. CI for Spark/PySpark

```
PYSPARK CI PIPELINE:
   
   1. pytest-spark unit tests
   2. Integration tests against Docker-composed stack
   3. Lint (black, ruff, mypy)
   4. Build + push container image
   5. Deploy to staging; smoke test
   6. Promote to prod on main merge

PATTERNS:
   Spark unit tests should be FAST (seconds, not minutes)
      Use small DataFrames; master="local[2]"
   Integration tests: slower but confidence-building
   E2E in staging environment with sample data
```

### 22. CD + Promotion

```
PROMOTION pattern:
   
   DEV → STAGING → PROD
   
   DEV: engineer's local branch; freely materializes to dev schema
   STAGING: main-branch builds against staging data; CI tests
   PROD: tagged releases; manual promotion gate (common)

   GITOPS APPROACH:
      Infrastructure (Airflow DAGs, dbt projects) in git
      Merge to main → auto-deploy to staging
      Tag release → deploy to prod
```

### 23. Blue/Green at the Table Level

```
BLUE/GREEN DEPLOYMENTS for tables:
   
   Delta Lake / Iceberg time travel enables:
      Current version = BLUE
      New write = GREEN (as new version)
      If GREEN passes tests → promote (point view to new version)
      If GREEN fails → stay on BLUE
   
Pattern:
   1. Compute new table version
   2. Run data tests on new version
   3. If pass: ALTER VIEW to point to new table version
   4. If fail: alert; new version NOT exposed to consumers
   5. Blue remains the queried version

STAFF SIGNAL: blue/green at the table level means failed runs DON'T POISON
   consumers' queries. This is a staff-level architectural signal.
```

---

## PART VIII — INTERVIEW PRACTICE

### 24. The Staff-Level Testing Interview

```
COMMON QUESTIONS:
   1. "How do you test a data pipeline?"
   2. "The testing pyramid for data."
   3. "Unit test a Spark transformation."
   4. "What's a contract test?"
   5. "Testing with Testcontainers."
   6. "CI for dbt projects."
   7. "Reconciliation tests: what + how."
   8. "Snapshot testing in data."
   9. "Synthetic data vs masked production."
   10. "Blue/green at the table level."
```

### 25. Worked Example

```
Q: Design the testing + CI/CD strategy for a 500-person data org
   with 2000 dbt models, 300 Airflow DAGs, 50 streaming pipelines
   on PySpark, serving 10K dashboards.

STAFF ANSWER:

LEVELS OF TESTS:
   
   LEVEL 1 (UNIT):
      dbt unit tests on complex SQL logic (medallion silver → gold transforms)
      PySpark unit tests via pytest-spark for all transformation functions
      Target: 70% code coverage on transformation logic
   
   LEVEL 2 (INTEGRATION):
      Testcontainers for Postgres + Kafka + MinIO
      PySpark pipelines run against local stack in CI
      dbt pipelines run against DuckDB fixtures where possible
      Spark on EMR for mission-critical integration tests
   
   LEVEL 3 (DATA):
      dbt tests on every model (not_null, unique, accepted_values, FK)
      Great Expectations for Spark pipeline outputs
      Reconciliation audits on tier-1 tables (daily)
      Monte Carlo for anomaly detection (production)
   
   LEVEL 4 (CONTRACT):
      dbt 1.5+ contracts on model outputs that cross team boundaries
      Schema Registry (Confluent) for Kafka topics
      Breaking changes gated by RFC + consumer approval

CI PIPELINES:
   
   dbt PR CI:
      compile → build state:modified+ in PR schema → test modified models → 
      data-diff vs prod (via Datafold) → PR comment with impact
   
   PySpark PR CI:
      Lint + mypy → unit tests (pytest-spark) → integration (Testcontainers) → 
      build container → promote to staging on merge
   
   Airflow DAG CI:
      DAG parse check (airflow dag-processor) → unit tests on 
      callables → integration tests → DAG deploy to staging

CD / PROMOTION:
   Staging: auto on main merge; smoke tests
   Prod: on tag; manual approval; blue/green at table level 
         (dbt swap after test pass)

ENVIRONMENTS:
   DEV: individual sandbox per engineer; subset of masked prod data
   STAGING: full-size masked prod; mirrors prod topology
   PROD: real production

TEST DATA:
   Synthetic fixtures for unit tests (small, YAML/CSV)
   Masked production for integration/staging (daily refresh)
   Monte Carlo + reconciliation audits for prod data health

MONITORING + REGRESSION:
   Every production incident → corresponding regression test
   Tests tagged with incident ID + runbook link
   Monthly review: which tests fire without action → prune

TEAM:
   Platform team: owns Testcontainers stack, CI templates, data diff tooling
   Domain teams: own tests for their models
   RACI published

COST + VALUE:
   Testing infra: ~$30K/month in CI compute (ephemeral envs, Testcontainers)
   Monte Carlo + Datafold: ~$250K/year
   Return: 50-70% fewer data incidents reaching production (measurable)
```

### 26. War Stories

> **🔥 Production Scenario — The Data Diff Save.** A team used dbt + Datafold. An analyst merged a PR with a seemingly minor formula change. Datafold's data diff showed 30% of rows would change value. PR blocked for review. Reviewed: the formula was wrong. Avoided a huge metric regression in prod. Lesson: data diff on PRs is a cheap, high-value gate.

> **🔥 Production Scenario — The Testcontainers Resurrection.** A team had an unreliable "staging" env (shared, often broken). CI took 2 hours. Migrated integration tests to Testcontainers: each PR got isolated containers in CI. Flakes dropped 90%; CI runtime: 20 minutes. Lesson: Testcontainers = reliable, hermetic, fast integration tests.

> **🔥 Production Scenario — The dbt Contract Catch.** A team added `contract: enforced: true` to a tier-1 dim model. 2 weeks later, a PR tried to change a column type from INT to STRING. dbt refused to build. Found the downstream models that would have broken (grepped for the column). Fixed properly, migrated downstream, then evolved the contract. Lesson: dbt contracts turn breaking schema changes into visible CI failures.

> **🔥 Production Scenario — The Masked Prod Trap.** A team masked PII in staging. Masking rule: replace SSN with "XXX-XX-{{last 4}}". Tests passed; prod broke. The masking replaced valid SSN format with a slightly different format a downstream validator rejected. Fix: use faker-generated FORMAT-COMPATIBLE values rather than literal placeholders. Lesson: masking rules must preserve format AND distribution.

> **🔥 Production Scenario — The Blue/Green Deploy.** A team deployed a nightly dbt build via blue/green: wrote new table version, ran all tests, promoted on pass. One night tests failed; blue remained. Consumers unaffected. Investigation: upstream API changed; schema fix needed. Debugged + fixed in daylight. Lesson: blue/green at the table level means FAILURES CANNOT POISON CONSUMERS.

### 27. Cheat Sheet &amp; Mental Models

#### 27.1 One-Minute Pitch

```
Data testing pyramid: unit → integration → data → contract.
   Each catches a different bug class.

TOOLS:
   UNIT: pytest, pytest-spark, dbt unit tests
   INTEGRATION: Testcontainers, Docker Compose
   DATA: dbt tests, Great Expectations, Soda
   CONTRACT: dbt contracts, Schema Registry

CI/CD:
   PR: run tests on modified models + data diff
   MAIN: full build + smoke
   PROD: tagged + blue/green deploy

STAFF SIGNAL: argue the FULL pyramid; argue which bug CLASS each layer
   catches; argue blue/green so failed runs don't poison consumers.
```

#### 27.2 Senior vs Staff

| Senior | Staff |
|--------|-------|
| "We have dbt tests." | "We layer: unit tests (dbt + pytest-spark) for logic bugs, integration tests (Testcontainers) for wiring bugs, data tests (dbt + GX) for data bugs, contract tests (dbt contracts + Schema Registry) for interface bugs. Each layer has distinct responsibility." |
| "CI runs tests." | "CI runs tests on PR for modified models + downstream, with data diff for value impact. Main-merge triggers full build + staging deploy. Prod promotion is tagged + blue/green at the table level so failed builds don't expose consumers. Promotion gates are explicit." |
| "Mock the DB." | "Mock for unit tests. Testcontainers for integration — real Postgres/Kafka/MinIO in containers; same code as prod, fully isolated per PR. For warehouse code: DuckDB-in-tests where dialect allows. Fakes catch different bugs than real services." |
| "Synthetic data is fine." | "Synthetic for unit; masked production for integration/staging. Synthetic misses edge cases; raw prod is a liability. Masking rules must preserve format + distribution — a careless mask hides real-world bugs." |

> **🧠 Principal Mindset:** Testing data pipelines is where "software engineering rigor" meets "data unpredictability." Senior engineers write tests. Staff engineers design the layered strategy, enforce it via CI gates, build the environments that make tests fast + reliable, and tie each layer to a specific bug class. The payoff: 50-70% fewer production incidents, which is measurable in operating cost and incident response time.

---

[← Back to main](./README.md)
