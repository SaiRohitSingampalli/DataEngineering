# Topic 26 — Orchestration Alternatives

> Standalone deep-dive · Dagster, Prefect, Temporal, Mage, Kestra, Argo

[← Back to main](./README.md)

---

## How to use this document

Airflow is covered in `03-airflow.md`. This chapter is for the alternatives — because "why not Dagster?" is one of the most common staff-level interview probes. Every alternative addresses something Airflow does poorly: Dagster attacks the asset model, Prefect attacks dynamic workflows, Temporal attacks durability, Mage/Kestra attack UX, Argo attacks Kubernetes-native orchestration.

> **🧠 Principal Mindset:** Senior engineers pick one orchestrator. Staff engineers argue the FIT — which orchestrator's abstractions match the workload's primitives. "Pipelines of SQL assets with lineage" is a different question than "durable long-running workflows with human steps."

## Table of Contents

**PART I — FOUNDATIONS**
- 1. The Orchestration Problem
- 2. Airflow's Legacy and Where It Falls Short
- 3. The Six Alternatives in 2026

**PART II — DAGSTER**
- 4. Software-Defined Assets
- 5. Dagster's Programming Model
- 6. Dagster vs Airflow
- 7. Dagster in Production

**PART III — PREFECT**
- 8. Prefect 2 and 3
- 9. Dynamic Workflows and Deployments
- 10. Prefect Cloud vs Self-Hosted

**PART IV — TEMPORAL**
- 11. Durable Execution Primitives
- 12. Workflows and Activities
- 13. When Temporal Beats Airflow

**PART V — MAGE AND KESTRA**
- 14. Mage: UI-First Orchestration
- 15. Kestra: YAML + Scalability

**PART VI — ARGO WORKFLOWS**
- 16. Argo on Kubernetes
- 17. Argo Events
- 18. Argo vs Everything Else

**PART VII — MANAGED ORCHESTRATION**
- 19. MWAA / Composer / Astronomer
- 20. Databricks Workflows and Snowflake Tasks
- 21. Managed vs Self-Hosted Trade-offs

**PART VIII — INTERVIEW PRACTICE**
- 22. The Staff-Level Orchestration Interview
- 23. Decision Framework
- 24. War Stories
- 25. Cheat Sheet &amp; Mental Models

---

## PART I — FOUNDATIONS

### 1. The Orchestration Problem

```
WHAT ORCHESTRATION DOES:
   • Scheduling (time or event-based triggers)
   • Dependency management (DAG of tasks)
   • Retry + failure handling
   • Observability (what ran, when, with what result)
   • Resource management (where tasks execute)
   • Backfill + reprocessing
   • Alerting on failures

ORCHESTRATION IS NOT:
   • Compute (Spark, warehouse, etc. do the actual work)
   • Transformation framework (dbt, Spark, Flink do that)
   • Storage (lake, warehouse)
   • CDC (though it schedules CDC jobs)

STAFF SIGNAL: pick orchestration based on what you WANT the orchestrator
   to do; many teams add second-tier tools (dbt, Spark) that handle
   the actual work and want the orchestrator to be a thin coordinator.
   Other teams want rich asset metadata and lineage from the orchestrator.
```

### 2. Airflow's Legacy and Where It Falls Short

```
AIRFLOW STRENGTHS:
   Mature; largest community
   Operators for everything
   Battle-tested at scale (Airbnb, Lyft, etc.)
   On every cloud as managed (MWAA, Composer, Astronomer)

AIRFLOW LIMITATIONS:
   • TASK-CENTRIC (not asset-centric): "ran a job" vs "produced this dataset"
   • Dynamic DAGs are awkward (TaskFlow helps but historical pain)
   • DAG-as-code: python file is the state; versioning is hard
   • Testing is painful
   • Scheduling is time-based primarily (sensors for events)
   • Big dependency tree (Airflow 2.x improved; still heavy)
   • UI shows runs, not data

See 03-airflow.md for depth.

STAFF SIGNAL: don't trash Airflow in interviews. Argue trade-offs.
   For asset-centric data workflows: alternatives win.
   For general job orchestration: Airflow is often fine.
```

### 3. The Six Alternatives in 2026

```
   NAME       | STRENGTH                         | ORIGIN
   -----------+---------------------------------+------------
   Dagster    | Software-defined assets          | Elementl 2019
   Prefect    | Dynamic workflows, Python-first  | Prefect 2017
   Temporal   | Durable execution, long-running  | Uber Cadence 2019
   Mage       | UI-first, batch + streaming      | Mage 2022
   Kestra     | YAML DSL + Java scalability      | Kestra 2019
   Argo       | Kubernetes-native workflows      | Intuit 2017

Plus:
   Luigi (Spotify; legacy)
   Flyte (Lyft; ML-focused Kubernetes-native)
   Pachyderm (data versioning + pipelines)
   Oozie (Hadoop; legacy)
```

---

## PART II — DAGSTER

### 4. Software-Defined Assets

```
DAGSTER'S CENTRAL IDEA: ASSETS, not tasks.

An ASSET = a persistent data object (table, file, ML model).
   You declare: @asset def customers(): ...
   Dagster tracks: "what produces customers? what depends on it?"
   
This is a FUNDAMENTAL SHIFT from Airflow:
   Airflow: "run task A, then task B"
   Dagster: "produce asset X; its dependencies are known"

Benefits:
   • Lineage is built-in (DAG derived from assets)
   • Partial materialization (rebuild only what's stale)
   • Asset metadata (freshness, size, column stats) first-class
   • Observability shows which ASSETS are stale, not which jobs failed
```

### 5. Dagster's Programming Model

```python
# dagster definitions
from dagster import asset, Definitions, AssetKey
import pandas as pd

@asset
def raw_orders() -> pd.DataFrame:
    return pd.read_csv("s3://bucket/orders.csv")

@asset
def clean_orders(raw_orders: pd.DataFrame) -> pd.DataFrame:
    return raw_orders.dropna(subset=["order_id"])

@asset
def daily_revenue(clean_orders: pd.DataFrame) -> pd.DataFrame:
    return (clean_orders
        .groupby("order_date")["amount"].sum().reset_index())

defs = Definitions(assets=[raw_orders, clean_orders, daily_revenue])
```

Dagster builds the DAG from asset dependencies (function signatures).

#### 5.1 Partitions

```python
from dagster import DailyPartitionsDefinition, asset

daily = DailyPartitionsDefinition(start_date="2024-01-01")

@asset(partitions_def=daily)
def daily_orders(context):
    date = context.partition_key
    return pd.read_csv(f"s3://bucket/orders/{date}.csv")
```

Partitions are first-class: each partition of an asset is independently materializable.

#### 5.2 IO Managers

```python
# IO managers separate "what the computation produces" from "how it is stored"
from dagster import IOManager, io_manager

class S3ParquetIOManager(IOManager):
    def handle_output(self, context, obj):
        obj.to_parquet(f"s3://bucket/{context.asset_key.path[-1]}.parquet")
    def load_input(self, context):
        return pd.read_parquet(f"s3://bucket/{context.upstream_output.asset_key.path[-1]}.parquet")

@io_manager
def s3_parquet_io_manager():
    return S3ParquetIOManager()
```

### 6. Dagster vs Airflow

| Dimension | Airflow | Dagster |
|-----------|---------|---------|
| Primitive | Task | Asset |
| Lineage | Add-on (OpenLineage) | Built-in |
| Testing | Awkward | Pythonic, pytest-native |
| Observability | Task runs | Asset materializations + freshness |
| Backfill | Partial, manual | Partition-aware, native |
| Local dev | Hard | Easy (dagster dev) |
| Managed | MWAA, Composer, Astronomer | Dagster+ Cloud, self-host |
| Ecosystem | Vast | Smaller but growing |
| Learning curve | High | Moderate |
| Dynamic DAGs | TaskFlow (good) | Native |

### 7. Dagster in Production

```
STRENGTHS AT SCALE:
   • Materialization reports → observability of staleness
   • Software-defined assets → clean lineage with no extra tooling
   • Testing stories → stronger PR CI
   • Local development → faster iteration

WEAKNESSES AT SCALE:
   • Smaller ecosystem (fewer pre-built connectors)
   • Larger operational surface (asset DB, run storage)
   • Learning curve for Airflow teams

HOSTING:
   Dagster+ Cloud (managed by Dagster)
   Dagster OSS self-hosted (K8s is standard pattern)

STAFF SIGNAL: Dagster is strongest for data-platform teams that
   care about lineage + assets + typed data. For pure job scheduling,
   Airflow is still often simpler.
```

---

## PART III — PREFECT

### 8. Prefect 2 and 3

```
PREFECT 2 (2022+): rewrite from Prefect 1
   Native async
   Flow + task abstractions
   Prefect Cloud or self-hosted server (Prefect Server)
   Agents / workers pull work from Prefect Cloud
   Dynamic parameters at runtime

PREFECT 3 (2024+): further refinement
   Faster, more scalable
   Better async + typing
   Marks as mature for enterprise
```

### 9. Dynamic Workflows and Deployments

```python
from prefect import flow, task

@task
def fetch_data(partition: str):
    return read_s3(f"s3://bucket/{partition}")

@task
def transform(data):
    return data.filter(data.amount > 0)

@task
def load(data):
    write_warehouse(data)

@flow
def pipeline(partitions: list[str]):
    for p in partitions:
        d = fetch_data(p)
        t = transform(d)
        load(t)

if __name__ == "__main__":
    pipeline(["2024-10-01", "2024-10-02", "2024-10-03"])
```

Dynamic: the flow decides how many tasks to run at runtime.

Deployments: a flow + schedule + infra config pushed to Prefect Cloud/Server.

### 10. Prefect Cloud vs Self-Hosted

```
PREFECT CLOUD:
   Managed; pay per-task-run tier
   UI for flows + runs + states
   Workspaces + RBAC
   Audit logs
   
PREFECT SERVER (self-hosted):
   OSS; run in your own K8s / VM
   Postgres backend
   Agents / workers in your infra
   
POSITIONING:
   Prefect: Python-first; strong for ML pipelines + dynamic DAGs
   Dagster: data-first; strong for assets + lineage
   Both: better dev UX than Airflow; smaller ecosystem than Airflow
```

---

## PART IV — TEMPORAL

### 11. Durable Execution Primitives

```
TEMPORAL = durable execution for long-running workflows.
   
   DURABLE: the workflow state is persisted; survives process restarts
   NOT just scheduling; the workflow CODE is checkpointed at every await
   
   USE CASES:
      Long-running sagas (seconds to months)
      Human-in-the-loop (wait for approval)
      Idempotent retries with history
      Multi-step business processes (order fulfillment, loan approval)

Temporal is for workflows that span real-world events, not just data pipelines.

TEMPORAL IS NOT a data orchestrator in the Airflow/Dagster sense.
It's an application orchestration framework that CAN do data work.
```

### 12. Workflows and Activities

```python
# Simplified Temporal Python example
from temporalio import workflow, activity

@activity.defn
async def charge_customer(customer_id: str, amount: int):
    # Call payment service; may fail; auto-retried
    return payment_service.charge(customer_id, amount)

@activity.defn
async def ship_order(order_id: str):
    return shipping_service.ship(order_id)

@workflow.defn
class OrderWorkflow:
    @workflow.run
    async def run(self, order_id: str):
        # These calls are DURABLE — survive restarts
        await workflow.execute_activity(
            charge_customer, args=[order.customer_id, order.amount],
            start_to_close_timeout=timedelta(minutes=5)
        )
        # Wait for a human approval (durable!)
        await workflow.wait_condition(lambda: self.approved)
        await workflow.execute_activity(
            ship_order, args=[order_id],
            start_to_close_timeout=timedelta(hours=1)
        )
```

### 13. When Temporal Beats Airflow

```
TEMPORAL WINS:
   ✓ Long-running workflows (hours → months)
   ✓ Human steps (approvals, manual review)
   ✓ Workflows that span services (microservices orchestration)
   ✓ Compensation logic (sagas with rollback)
   ✓ Testing: workflow logic is deterministic, replayable

AIRFLOW/DAGSTER WIN:
   ✓ Scheduled data pipelines (nightly ETL)
   ✓ Dataset materialization + lineage
   ✓ Bulk operations

PATTERN:
   Data pipelines: Airflow/Dagster
   Business workflows: Temporal
   Mix: use both for different workload shapes

STAFF SIGNAL: in interviews, if asked to design a human-in-the-loop
   or multi-week workflow, Temporal is the right answer. Trying to force
   this into Airflow is a weak answer.
```

---

## PART V — MAGE AND KESTRA

### 14. Mage: UI-First Orchestration

```
MAGE = UI-first + Python/SQL blocks + batch + streaming.

PROGRAMMING MODEL:
   Pipelines are sequences of BLOCKS (Python functions or SQL queries)
   Blocks are typed: data_loader, transformer, data_exporter
   UI editor + Jupyter-like dev experience
   Preview outputs after each block

STRENGTHS:
   • Fast developer experience for small teams
   • UI reduces onboarding friction
   • Streaming support (Kafka → batch windows)
   • Mage Pro managed service

WEAKNESSES:
   • Smaller community
   • Less mature at scale
   • "Low-code" positioning worries engineers who want full code control
```

### 15. Kestra: YAML + Scalability

```
KESTRA = YAML-based DSL + Java backend; scalable orchestration.

FLOW DEFINITION:
   id: my_flow
   namespace: company.data
   tasks:
     - id: extract
       type: io.kestra.plugin.jdbc.postgresql.Query
       sql: SELECT * FROM users
     - id: transform
       type: io.kestra.plugin.scripts.python.Script
       script: ...
     - id: load
       type: io.kestra.plugin.gcp.bigquery.Load
       destinationTable: project.ds.users

STRENGTHS:
   • YAML is clean + GitOps-friendly
   • Java backend scales well (Kafka-based under the hood)
   • Plugin ecosystem is growing
   • Open-source + managed

STAFF SIGNAL: Kestra appeals to GitOps / platform teams who want
   declarative workflow definitions. Trade-off: less flexibility
   than Python when custom logic is needed.
```

---

## PART VI — ARGO WORKFLOWS

### 16. Argo on Kubernetes

```
ARGO WORKFLOWS = Kubernetes-native workflow engine.
   
   Workflows defined as Kubernetes CRDs (Custom Resources)
   Each step is a container
   Scheduler is K8s itself
   Scales to tens of thousands of parallel tasks

USE CASES:
   • Infrastructure-heavy workflows (ML training, batch processing)
   • Teams already deep in Kubernetes
   • Massive parallelism (thousands of containers)
   • CI/CD pipelines (Argo + Argo CD)
```

### 17. Argo Events

```
ARGO EVENTS = event-driven workflow triggers.
   
   Event sources: Kafka, S3, webhooks, Pub/Sub, GitHub, etc.
   Sensors: react to events; trigger workflows
   
   Pattern:
      Kafka message arrives → Argo Event source → Sensor → Workflow starts

Together with Argo Workflows: event-driven, K8s-native, massively parallel.
```

### 18. Argo vs Everything Else

```
ARGO WINS when:
   Already on K8s; want K8s-native
   Massive parallelism (10K+ concurrent containers)
   Infrastructure pipelines, not just data

ARGO LOSES when:
   Not on K8s (overhead not worth it)
   Pure data pipelines without heavy compute per step
   Want asset-level lineage (Dagster better)

STAFF SIGNAL: Argo is often the right choice for ML training 
   pipelines + CI/CD + large-scale batch — less for traditional ETL.
   Mentioning Argo in an interview signals K8s depth.
```

---

## PART VII — MANAGED ORCHESTRATION

### 19. MWAA / Composer / Astronomer

```
AWS MWAA: managed Airflow on AWS
   Monthly cost: ~$500-5000+ depending on environment size
   Scales via multiple MWAA environments
   Pros: no cluster to run; integrates with IAM
   Cons: expensive at scale; slower feature cadence than OSS Airflow

GCP COMPOSER: managed Airflow on GCP
   Similar positioning; GCP ecosystem

ASTRONOMER: enterprise managed Airflow
   Multi-cloud; richer tooling (Astro Observe, lineage)
   Deployment: Astro's K8s-based platform
   Target: large enterprises wanting managed Airflow with support
```

### 20. Databricks Workflows and Snowflake Tasks

```
DATABRICKS WORKFLOWS (see 22-databricks-platform):
   Orchestrates Databricks jobs (notebooks, DLT, SQL, scripts)
   Serverless option
   Good when: Databricks-only pipelines

SNOWFLAKE TASKS (see 21-snowflake-platform):
   Scheduled SQL within Snowflake
   Streams + Tasks + Dynamic Tables for in-Snowflake ELT
   Good when: Snowflake-only pipelines

Both are LIMITED to their own ecosystem.
Pure Databricks or Snowflake teams can use them;
Multi-system orgs still need Airflow / Dagster / etc.
```

### 21. Managed vs Self-Hosted Trade-offs

```
MANAGED (MWAA / Composer / Dagster+ / Prefect Cloud):
   ✓ No infra to run
   ✓ SOC 2, HIPAA, enterprise support
   ✓ Faster start-up
   ✗ Vendor pricing
   ✗ Less customization
   ✗ Egress + integration costs

SELF-HOSTED (K8s / VM):
   ✓ Full control + customization
   ✓ Cheaper at very large scale
   ✗ Ops burden (upgrades, HA, security)
   ✗ Need platform team

PATTERN:
   Small-medium orgs: managed by default
   Large orgs: often self-hosted for cost + control
   Hybrid: managed in production, OSS for dev
```

---

## PART VIII — INTERVIEW PRACTICE

### 22. The Staff-Level Orchestration Interview

```
COMMON QUESTIONS:
   1. "Why not Dagster?" (or Prefect/Temporal)
   2. "Compare Airflow and Dagster."
   3. "When would you use Temporal?"
   4. "Design orchestration for a 50-team data platform."
   5. "Dynamic DAGs: how in each tool?"
   6. "What's an asset in Dagster?"
   7. "Managed Airflow vs self-hosted."
   8. "Argo Workflows vs Airflow for ML pipelines."
   9. "How do you test workflows?"
   10. "Migration from Airflow to Dagster: your plan."
```

### 23. Decision Framework

```
WORKFLOW SHAPE                             BEST FIT
=====================================================
Scheduled data pipelines, SQL/ETL         Airflow / Dagster
Asset-centric with rich lineage           Dagster
Dynamic DAGs, ML-heavy                    Prefect
Long-running, human-in-the-loop           Temporal
Massively parallel infra workflows        Argo Workflows
K8s-native, event-driven                  Argo + Argo Events
Databricks-only                           Databricks Workflows
Snowflake-only                            Snowflake Tasks + Dynamic Tables
Fast dev experience, small team           Mage
GitOps + YAML-first                       Kestra
Enterprise managed Airflow                MWAA / Composer / Astronomer
```

### 24. War Stories

> **🔥 Production Scenario — The Airflow Monorepo Hellfire.** A 100-engineer team's Airflow DAGs lived in one monorepo. 500 DAGs, 3000 tasks, 15-minute DAG parsing time. Any change triggered re-parse; webserver crashed under load. Fix: split DAGs by team; moved to Dagster with code locations (independent deploys per team). Parsing drops to seconds. Lesson: Airflow doesn't scale politically; Dagster's deployment model does.

> **🔥 Production Scenario — The Temporal Save for Approvals.** A team built an insurance claims workflow in Airflow using sensor polling for human approval. Sensors burned slots; DAG was fragile; approval timeouts were awkward. Rewrote in Temporal: 300 lines of code; approvals as `wait_condition`; dramatically simpler. Lesson: Airflow is wrong for workflows that span human steps; Temporal fits natively.

> **🔥 Production Scenario — The Dagster Asset Migration Payoff.** A team migrated from Airflow to Dagster. 6-month effort. Benefits: lineage visible in UI without a separate tool; partial rebuilds (only materialize stale assets); testing stories let them catch regressions in CI. Operational cost: similar. Developer productivity: 30% boost on data platform work. Lesson: migrations are expensive but asset-centric model pays off at mid-scale.

> **🔥 Production Scenario — The MWAA Cost Blow.** An org ran MWAA with 4 environments (dev, staging, prod, backfill). $5K/month each = $20K/month just for Airflow. Switched to self-hosted Airflow on K8s with a shared platform. Cost dropped to $4K/month (K8s + ops time). Lesson: managed Airflow is affordable to start, expensive at scale; break-even is usually around $10K/month MWAA cost.

### 25. Cheat Sheet &amp; Mental Models

#### 25.1 One-Minute Pitch

```
Pick orchestration by WORKFLOW SHAPE:
   
   Scheduled data pipelines:    Airflow / Dagster
   Asset-centric + lineage:     Dagster
   Dynamic + ML:                Prefect
   Long-running + human steps:  Temporal
   K8s-native + massive parallel: Argo
   Vendor-ecosystem only:       Databricks Workflows / Snowflake Tasks

MANAGED (MWAA, Composer, Astronomer, Dagster+ Cloud, Prefect Cloud):
   Default for small-medium
   Expensive at large scale

STAFF SIGNAL: argue the FIT between orchestrator primitives
   and workflow primitives, not the "better" tool.
```

#### 25.2 Senior vs Staff

| Senior | Staff |
|--------|-------|
| "We use Airflow." | "Our data pipelines run on Dagster for asset-centric lineage, our business workflows run on Temporal for durability, and Airflow still handles legacy SaaS integrations. Pick orchestration by workflow shape." |
| "Dagster is better." | "Dagster shines when assets are the right primitive — lineage, partial rebuilds, freshness. Airflow is right when tasks are the primitive. For a 50-DAG legacy shop with heavy sensors, Airflow often stays; for a greenfield data-platform rebuild, Dagster wins." |
| "Airflow has sensors." | "Sensors burn slots. For event-driven workflows, prefer Argo Events + Argo Workflows (K8s-native) or Prefect's event triggers. For human steps, use Temporal. Airflow sensors are a last resort." |
| "We need managed Airflow." | "Managed vs self-hosted is a cost + ops surface decision. Break-even is around $10K/month in managed spend; above that, self-hosted on K8s is cheaper if you have the ops team. Below that, managed almost always wins." |

> **🧠 Principal Mindset:** Orchestration is where "we always used Airflow" meets "the workload has outgrown Airflow's primitives." Staff engineers see this as a tool-fit problem, not a loyalty problem. Multi-orchestrator orgs are increasingly common and defensible: Airflow for legacy, Dagster for new data platform work, Temporal for business workflows, Databricks Workflows/Snowflake Tasks for intra-ecosystem flows.

---

[← Back to main](./README.md)
