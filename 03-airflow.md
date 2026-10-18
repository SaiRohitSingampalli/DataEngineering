# Topic 3 — Airflow & Orchestration

> Section 3 of 10 · 58 pages in source PDF · pages 150–207

[← Back to main](./README.md)

---

Airflow & Orchestration Architect-Level Reference Principal / Staff Engineer Interview Notes DAG Design · Executors · Scheduling · Sensors · XComs · Deferrable Tasks · TaskFlow · Scaling Airflow Itself The orchestration layer that ties SQL, Spark, and dbt together in production.

How to use this document Airflow looks deceptively simple: Python decorators, a UI, a scheduler that runs your DAGs. The complexity is hidden — in how the scheduler parses DAGs, how executors dispatch tasks, what happens during a backfill, why your task fails intermittently at scale, and when Airflow itself is the wrong tool. ■ Principal mindset: Staff-level orchestration interviews test three things: (1) can you design a pipeline that's resilient, idempotent, and observable; (2) can you operate Airflow at scale (10K+ tasks/day, 100+ DAGs); (3) do you know its limits — where Dagster, Prefect, Argo, or Step Functions would be better.

Table of Contents

## PART I — FOUNDATIONS

#### 1. What Orchestration Is For

#### 2. The Airflow Architecture

#### 4. The Scheduler & Executor Model

## PART II — AUTHORING DAGS

#### 6. TaskFlow API & Modern Patterns

#### 7. Sensors, Triggers & Deferrable Operators

#### 9. Dynamic Task Mapping & Task Groups

## PART III — SCHEDULING & EXECUTION

#### 10. Schedule Intervals, Cron & Data Intervals

#### 11. Catchup, Backfill & Reruns

#### 12. Executors: Local, Celery, Kubernetes

#### 13. Queues, Pools & Concurrency Control

## PART IV — RELIABILITY & CORRECTNESS

#### 14. Idempotency & Re-runnability

#### 15. Retries, SLAs & Failure Modes

#### 16. Data-Aware Scheduling (Datasets)

## PART V — SCALING AIRFLOW

#### 17. Scaling the Scheduler & Webserver

#### 18. Metadata DB & Performance Tuning

#### 19. Multi-Tenancy & Team Isolation

#### 20. Observability: Logs, Metrics, Lineage

## PART VI — PRODUCTION PATTERNS

#### 22. Secrets & Security

#### 23. Common Anti-Patterns

## PART VII — THE COMPETITIVE LANDSCAPE

#### 24. Airflow vs Dagster vs Prefect

#### 25. Airflow vs Argo, Step Functions, Temporal

#### 26. When Airflow Is the Wrong Tool

## PART VIII — INTERVIEW PRACTICE

#### 27. The Staff-Level Orchestration Interview

#### 28. War Stories

#### 29. Cheat Sheet & Mental Models

## PART I — FOUNDATIONS

#### 1. What Orchestration Is For

1.1 The Problem A modern data pipeline involves dozens to thousands of tasks: extract from sources, load to warehouse, transform with dbt/Spark, validate quality, refresh BI tables, train ML models, send alerts. These tasks have dependencies (B can't start until A completes), schedules (run hourly/daily/on-demand), and failure modes (retry, alert, skip downstream). Doing this by hand with cron + bash scripts works at small scale and fails at every other scale.

  - 1.2 What an Orchestrator Provides
- 1. DAG (Directed Acyclic Graph) of tasks with explicit dependencies
- 2. SCHEDULING — time-based, event-based, dependency-based
- 3. EXECUTION — dispatch tasks to compute, track state
- 4. RETRIES — automatic, configurable backoff
- 5. OBSERVABILITY — logs, metrics, history, lineage
- 6. ALERTING — failure notifications, SLA misses
- 7. BACKFILL — re-run for historical date ranges
- 8. PARAMETERIZATION — same DAG with different inputs

1.3 The Airflow History 2014 — Airbnb open-sources Airflow (Maxime Beauchemin) 2016 — Apache incubation 2019 — Top-level Apache project 2020 — Airflow 2.0: TaskFlow API, scheduler HA, REST API 2022 — Airflow 2.4: Datasets (data-aware scheduling) 2023 — Airflow 2.7: Setup/teardown tasks 2024 — Airflow 2.10: dynamic task mapping improvements 2025 — Airflow 3.0: DAG versioning, multi-language, ExecutionAPI Airflow's dominance: ~80% of orchestration mindshare in data engineering. Reasons: Python-native, open-source, mature ecosystem, hireable skill. ■ Principal mindset: The orchestration question at staff level is rarely 'Airflow yes/no' — it's 'which orchestrator for this workload, and what trade-offs are you accepting?' Airflow is the safe default; Dagster is rising; Argo wins for Kubernetes-native ML; Step Functions for serverless AWS; Temporal for stateful workflows. Knowing the dimensions of choice is the staff skill. ■ Interview angle: Q: Why do you need an orchestrator? Why not cron + bash? Senior: 'Dependencies between tasks.' Staff: 'Cron works until something fails. Cron doesn't know about dependencies — if A fails, B runs anyway on bad data. Cron has no retry, no backfill, no SLA tracking, no visibility into history, no central log aggregation. You can hack these in with scripts, but you're building an orchestrator badly. At ~5 jobs cron is fine; at 50 jobs you need an orchestrator; at 500 you need a good one. The question is which.'

#### 2. The Airflow Architecture

```
2.1 The Components
■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■
■                       USER / DEVELOPER                        ■
■                            ■                                  ■
■                            ▼                                  ■
■                      DAG FILES (.py)                          ■
■                            ■                                  ■
■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■
                             ■ (filesystem / git-sync / S3)
                             ▼
■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■
■                                                                ■
■   ■■■■■■■■■■■■■■■■    ■■■■■■■■■■■■■■■■    ■■■■■■■■■■■■■■■■  ■
■   ■  SCHEDULER   ■■■■→■  EXECUTOR    ■■■■→■   WORKER(S)  ■  ■
■   ■   Parses    ■    ■   Dispatch  ■    ■   Run tasks ■  ■
■   ■    DAGs      ■    ■    tasks     ■    ■   Local /   ■  ■
■   ■   Schedules ■    ■   Tracks    ■    ■    Celery /  ■  ■
■   ■    runs      ■    ■    state     ■    ■    K8s pod   ■  ■
■   ■   Heartbeat ■    ■              ■    ■              ■  ■
■   ■■■■■■■■■■■■■■■■    ■■■■■■■■■■■■■■■■    ■■■■■■■■■■■■■■■■  ■
■          ■                   ■                   ■           ■
■          ■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■           ■
■                              ▼                                ■
■                      ■■■■■■■■■■■■■■■■                         ■
■                      ■  METADATA DB ■   (Postgres / MySQL)   ■
■                      ■   DAG state ■                         ■
■                      ■   Task state■                         ■
■                      ■   Logs ptrs ■                         ■
■                      ■   XComs     ■                         ■
■                      ■   Connections■                        ■
■                      ■■■■■■■■■■■■■■■■                         ■
■                              ■                                ■
■                              ▼                                ■
■   ■■■■■■■■■■■■■■■■    ■■■■■■■■■■■■■■■■                      ■
■   ■  WEBSERVER   ■←■■■■   TRIGGERER  ■                       ■
■   ■   UI        ■    ■   Deferred  ■                       ■
■   ■   REST API  ■    ■    tasks     ■                       ■
■   ■■■■■■■■■■■■■■■■    ■■■■■■■■■■■■■■■■                       ■
■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■
```

```
2.2 Component Responsibilities
SCHEDULER
   Continuously parses DAG files (every ~30 seconds)
   Decides which task instances should run based on schedule + deps
   Writes 'queued' task instances to metadata DB
   In Airflow 2.0+: HA (multiple schedulers can run active-active)
EXECUTOR
   Pulls queued tasks from metadata DB
   Dispatches to workers
   Reports task state back to DB
   Types: Sequential, Local, Celery, Kubernetes, CeleryKubernetes (hybrid)
WORKER
   Actually runs the task code
   Local: subprocess on scheduler host (single-node only)
   Celery: pre-spawned worker pool consuming from message broker
   Kubernetes: pod-per-task, dies after completion
WEBSERVER
```

```
   Flask app for the UI
   Reads from metadata DB
   Stateless — can be scaled horizontally behind load balancer
TRIGGERER (Airflow 2.2+)
   Async event loop for deferrable operators
   Handles tens of thousands of waiting tasks without consuming worker slots
   Game-changer for sensor-heavy workloads
METADATA DB
   The source of truth for DAG/task state
   Heavily-trafficked OLTP workload
   THE most common scaling bottleneck
```

```
2.3 What Lives Where
ON DISK (scheduler + workers):
   DAG files (the Python code)
   Plugins, custom operators
   Logs (written locally, optionally shipped to S3/GCS)
IN METADATA DB:
   DAG metadata (last parsed time, hash)
   DAG runs (each scheduled execution)
   Task instances (each task in each run)
   XComs (small inter-task messages)
   Connections, Variables (encrypted)
   User accounts (if using built-in auth)
   Pools, queues, slot reservations
IN EXTERNAL STORAGE (configurable):
   Logs (S3, GCS, Elasticsearch)
   Large datasets (NEVER in XCom)
■■ Warning: The metadata DB is the single most important scaling concern. It handles thousands of writes per
minute at scale: scheduler heartbeats, task state transitions, log pointers. Use Postgres (not SQLite or MySQL) for
any production deployment. Plan for HA replicas, monitoring, and tuning.
```

3. DAGs, Tasks & Operators

```python
3.1 The DAG
A DAG is a Python file defining a collection of tasks with their dependencies. It's loaded by the scheduler,
parsed periodically, and used to schedule runs. Crucially: a DAG is Python code that runs at parse time,
not run time. This trips up nearly every newcomer.
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
# This code runs EVERY ~30 SECONDS during DAG parsing.
# Don't put expensive computation, API calls, or heavy imports here.
default_args = {
    'owner': 'data-eng',
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
}
with DAG(
    dag_id='daily_etl',
    default_args=default_args,
    schedule='0 2 * * *',                # 2 AM daily
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['etl', 'finance'],
) as dag:
    def extract(**ctx):
        return {"rows": 1000}            # XCom return (small only!)
    extract_task = PythonOperator(
        task_id='extract',
        python_callable=extract,
    )
    # ... define more tasks ...
    extract_task >> next_task             # dependency
```

```
3.2 Tasks
A TASK is an instance of an Operator with parameters.
A TASK INSTANCE is a specific execution of a task on a specific date.
Lifecycle states:
   none → scheduled → queued → running → success / failed / up_for_retry / ...
Other states: skipped, upstream_failed, removed, restarting, deferred,
              up_for_reschedule, sensing, shutdown
```

```
3.3 Operators: The Building Blocks
CORE OPERATORS
  PythonOperator           — run a Python function
  BashOperator             — run a shell command
  EmptyOperator            — placeholder (was DummyOperator)
  BranchPythonOperator     — choose downstream path
  ShortCircuitOperator     — skip all downstream if False
  TriggerDagRunOperator    — start another DAG
DATABASE OPERATORS
```

```
  SQLExecuteQueryOperator  — generic, modern (Airflow 2.5+)
  PostgresOperator, MySqlOperator, SnowflakeOperator, BigQueryInsertJobOperator
DATA TRANSFER
  S3ToRedshiftOperator, GCSToBigQueryOperator, ...
  Many specific transfer operators in provider packages
CLOUD / SERVICE
  EmrCreateJobFlowOperator, DataprocSubmitJobOperator,
  KubernetesPodOperator, EcsRunTaskOperator, DatabricksSubmitRunOperator
SENSORS (wait for a condition — see Ch 7)
  S3KeySensor, ExternalTaskSensor, SqlSensor, HttpSensor
→ Always import from `airflow.providers.<vendor>.<service>.operators...`
   Core operators are in `airflow.operators...`
```

```
3.4 The KubernetesPodOperator Pattern
At scale, the most flexible operator is KubernetesPodOperator — it runs any container as a task. Decouples
Airflow's Python environment from your task code; each task is isolated; resource limits are first-class.
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
task = KubernetesPodOperator(
    task_id='run_spark_job',
    image='my-registry/spark-job:v1.2.3',
    cmds=['python', '/app/run.py'],
    arguments=['--date', '{{ ds }}'],
    namespace='airflow',
    resources={'request_cpu': '2', 'request_memory': '4Gi',
               'limit_cpu': '4', 'limit_memory': '8Gi'},
    is_delete_operator_pod=True,
    get_logs=True,
)
■ Principal mindset: At staff scale, the operator hierarchy collapses to two: KubernetesPodOperator for anything
heavy (Spark, dbt, custom Python), and the cloud-native operator for managed services (Databricks submit,
BigQuery job, EMR step). Avoid running heavy code in PythonOperator — the task lives in the worker's Python env,
fights for dependencies, and OOMs the worker on bad inputs.
```

#### 4. The Scheduler & Executor Model

```sql
4.1 The Scheduler Loop
Every ~5 seconds the scheduler:
  1. PARSE DAG FILES that changed since last parse
      In-process by default (DAGFileProcessorManager)
      Heavy DAGs slow this loop — parsing == scheduling latency
  2. EXAMINE EACH ACTIVE DAG
      Calculate next run if scheduled time passed
      Create a DagRun row if not already present
  3. EVALUATE TASK INSTANCES
      For each unfinished DagRun, check tasks whose upstream is complete
      Mark as scheduled if ready
      Mark as queued and ready for executor pickup
  4. SEND TO EXECUTOR
      Executor decides where to run (which worker / pod / queue)
      Updates state when worker reports back
  5. HEARTBEAT
      Update scheduler_health row in DB
      In HA mode, multiple schedulers coordinate via DB row locks
```

```python
4.2 Why Parse Time Matters
Each DAG file is FULLY EXECUTED on every parse.
  with DAG(...):
      # ANYTHING HERE runs every ~30 seconds.
      # ■ BAD: API call at parse time
      data = requests.get("https://...").json()
      for item in data:
          PythonOperator(task_id=item['id'], ...)
      # ■ BAD: DB query at parse time
      results = pg_hook.get_records("SELECT id FROM jobs")
      for r in results:
          BashOperator(task_id=f"job_{r[0]}", ...)
      # ✓ GOOD: dynamic task mapping at RUN time
      @task
      def get_jobs():
          return [...]   # runs when DAG actually executes
      MyOp.partial(...).expand(job=get_jobs())
PARSE-TIME RULES OF THUMB:
   <2 seconds per DAG file
   No external I/O (HTTP, DB) at module level
   No expensive computation
   No heavyweight imports (load lazily in callables)
```

```
4.3 The Scheduler HA Story (Airflow 2.0+)
Pre-2.0: single scheduler. SPOF. Restart = downtime.
2.0+:    Multiple schedulers can run active-active.
         Coordinate via row-level locks on metadata DB.
         Each scheduler:
            Parses DAGs (work split via DAGFileProcessor)
```

```
            Examines task instances (with SKIP LOCKED on DB queries)
            Heartbeats independently
         REQUIRES: Postgres ≥ 9.6 or MySQL ≥ 8.0 (for SKIP LOCKED)
PRODUCTION SETUP:
   2-3 scheduler replicas (more rarely helps)
   All read the same DAG folder (git-sync, EFS, or persistent volume)
   All connect to same metadata DB
```

```
4.4 What Triggers a Task to Run
A task instance becomes runnable when ALL of:
  ✓ Its DAG run exists
  ✓ Its schedule_interval has passed (data_interval_end ≤ now)
  ✓ All upstream tasks have succeeded (or trigger_rule allows it)
  ✓ Concurrency limits not exceeded (pool slots, dag/task concurrency)
  ✓ No earlier run of same task is still pending (depends_on_past)
  ✓ Task is not in a no-go state (deferred, removed)
TRIGGER RULES (override default 'all_success'):
  all_success (default)   — all upstreams succeeded
  all_failed              — all upstreams failed
  all_done                — all upstreams done (any state)
  one_success             — at least one upstream succeeded
  one_failed              — at least one upstream failed
  none_failed             — no upstream failed (success or skipped OK)
  none_failed_min_one_success — useful with branching
  none_skipped            — no upstream skipped
  always                  — always run
■ Interview angle: Q: A DAG is scheduled hourly but tasks are starting 15 minutes late. What's happening?
Staff: Scheduling latency, several possible causes. (1) Slow DAG parsing — too many DAGs, or expensive code at
module level. Check dag_processing.last_duration metric and
scheduler.dag_processing.processing_time. (2) Scheduler loop saturated — not enough scheduler
replicas, or max_tis_per_query too low. (3) Worker pickup lag — Celery queue backed up, Kubernetes pod
scheduling delayed. (4) Pool or concurrency limits hit. The metric scheduler.tasks.starving tells you how
many tasks are waiting for slots.
```

## PART II — AUTHORING DAGs

```python
5.1 The Anatomy of a Production DAG
"""Daily ETL: load yesterday's sales, transform, materialize gold table."""
from datetime import datetime, timedelta
from airflow import DAG
from airflow.decorators import task
from airflow.providers.amazon.aws.operators.s3 import S3CreateObjectOperator
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from airflow.operators.empty import EmptyOperator
DAG_ID = 'sales__daily_etl'
OWNER = 'data-eng'
DOC_MD = """
### Daily Sales ETL
Loads yesterday's sales from S3, runs Spark transform, writes gold table.
On-call: #data-eng-oncall. SLA: complete by 6 AM UTC.
"""
with DAG(
    dag_id=DAG_ID,
    description='Daily sales ETL pipeline',
    doc_md=DOC_MD,
    start_date=datetime(2024, 1, 1),
    schedule='0 2 * * *',
    catchup=False,
    max_active_runs=1,
    default_args={
        'owner': OWNER,
        'retries': 3,
        'retry_delay': timedelta(minutes=10),
        'retry_exponential_backoff': True,
        'max_retry_delay': timedelta(hours=1),
        'sla': timedelta(hours=4),
        'email_on_failure': False,        # we use Slack alerts via on_failure_callback
    },
    tags=['sales', 'etl', 'tier-1'],
    on_failure_callback=slack_alert,
) as dag:
    start = EmptyOperator(task_id='start')
    @task
    def discover_files(execution_date):
        # returns list of file paths
        ...
    transform = KubernetesPodOperator(
        task_id='spark_transform',
        image='registry/spark-etl:1.4.2',
        arguments=['--date', '{{ ds }}'],
        ...
    )
    end = EmptyOperator(task_id='end', trigger_rule='none_failed')
```

start >> discover_files() >> transform >> end

```
5.2 The DAG Authoring Rules
1. PARSE-TIME IS CHEAP, RUN-TIME IS REAL
   No I/O, no DB queries, no API calls at module level.
2. ONE DAG = ONE BUSINESS PURPOSE
   "daily_sales_etl" is a DAG; "etl_everything" is not.
   Smaller DAGs are easier to operate, retry, and reason about.
3. EVERY TASK MUST BE IDEMPOTENT
   Running the same task instance twice produces the same result.
   Use {{ ds }} / {{ data_interval_start }} for date partitions.
4. NO HEAVY WORK IN PYTHONOPERATOR
   Spawn it elsewhere: KubernetesPod, Databricks job, EMR, ECS.
   PythonOperator should orchestrate, not compute.
5. DAG IDs ARE FOREVER
   Renaming a DAG = losing history. Pick stable IDs.
6. EVERY DAG NEEDS OWNER, TAGS, DESCRIPTION, ON_FAILURE_CALLBACK
   Operational hygiene — who do I page when this fails?
7. NEVER STORE LARGE DATA IN XCOM
   XCom is for small messages (job IDs, row counts, paths). Big data → S3.
8. AVOID `depends_on_past=True` AT SCALE
   One bad day blocks all future runs. Use idempotency + monitoring instead.
9. SET `max_active_runs` SENSIBLY
   1 for serial pipelines; N for parallelizable backfills.
10. WRITE `doc_md` FOR EVERY DAG
    The UI renders it. On-call engineers will thank you.
```

5.3 The Doc-Driven DAG @task(doc_md=""" ### Validate sales rows Checks row count, null fractions, primary-key uniqueness. Fails the DAG if checks don't pass — investigate via [runbook](...) """) def validate_sales(date): ... # Each task can have its own doc_md, rendered in the UI under the task. ■ Production scenario: A team had 200 DAGs created by 30 engineers over 2 years. On-call had no idea who owned what. After enforcing owner tags + doc_md as a CI check, on-call MTTR dropped from 45 min to 8 min. The DAG metadata is documentation; treat it like code.

#### 6. TaskFlow API & Modern Patterns

```python
6.1 The Old vs New API
OLD STYLE (Airflow 1.x → still works)
  def extract(**ctx):
      ti = ctx['ti']
      ti.xcom_push(key='count', value=100)
  def transform(**ctx):
      ti = ctx['ti']
      count = ti.xcom_pull(task_ids='extract', key='count')
      return count * 2
  extract_op = PythonOperator(task_id='extract', python_callable=extract)
  transform_op = PythonOperator(task_id='transform', python_callable=transform)
  extract_op >> transform_op
NEW STYLE: TaskFlow (Airflow 2.0+)
  @task
  def extract():
      return 100         # auto-pushed to XCom
  @task
  def transform(count):  # auto-pulls from XCom
      return count * 2
  extract() >> transform()
  # Or even cleaner:
  transform(extract())
```

```
6.2 Why TaskFlow Is Better
✓ Less boilerplate; reads like a Python function call
✓ XComs are implicit — no manual push/pull
✓ Type hints work
✓ Refactoring-friendly (function arguments, not magic string IDs)
✓ Combines naturally with dynamic task mapping (Ch 9)
LIMITS:
  ✗ Can't easily mix with non-Python operators in the same chain
  ✗ XCom backend still required (so output size still bounded)
  ✗ Function source is hashed for parsing — large functions slow parse
```

```python
6.3 TaskFlow in Practice
from airflow.decorators import task, dag
@dag(
    schedule='@daily',
    start_date=datetime(2024, 1, 1),
    catchup=False,
)
def sales_etl():
    @task
    def extract(date: str) -> str:
        # ... extract logic ...
        return f's3://bucket/raw/{date}/data.parquet'
    @task
    def transform(path: str) -> dict:
        # ... transform ...
        return {'rows': 1000, 'path': 'gold/...'}
```

```
    @task
    def alert(result: dict):
        slack.post(f"Processed {result['rows']} rows")
    raw = extract('{{ ds }}')
    result = transform(raw)
    alert(result)
dag_instance = sales_etl()
```

```sql
6.4 Mixing TaskFlow with Operators
# TaskFlow output → classical operator
@task
def get_query() -> str:
    return f"SELECT * FROM events WHERE dt = '{{{{ ds }}}}'"
run_query = SnowflakeOperator(
    task_id='run',
    sql="{{ ti.xcom_pull(task_ids='get_query') }}",  # awkward
)
# Cleaner: use templating directly in the operator
run_query = SnowflakeOperator(
    task_id='run',
    sql="SELECT * FROM events WHERE dt = '{{ ds }}'",
)
■ Principal mindset: TaskFlow has won for Python-heavy DAGs. For SQL-heavy DAGs (Snowflake/BigQuery
operators), the classical API is fine. Most production DAGs mix both, and that's OK. Don't religiously rewrite working
classical DAGs into TaskFlow — there's no operational benefit.
```

#### 7. Sensors, Triggers & Deferrable Operators

```
7.1 Sensors: Wait for Something
A sensor is an operator that waits for a condition to be true: a file to appear, an external task to complete, a
SQL row to return TRUE, an HTTP endpoint to return 200. Sensors are how DAGs synchronize with the
world.
COMMON SENSORS:
  ExternalTaskSensor  — wait for a task in another DAG to finish
  S3KeySensor         — wait for an S3 object
  FileSensor          — wait for a local file
  SqlSensor           — wait until a SQL query returns rows / TRUE
  HttpSensor          — wait until an HTTP endpoint returns 200
  DateTimeSensor      — wait until a specific datetime
EXAMPLE:
  wait_for_file = S3KeySensor(
      task_id='wait_for_input',
      bucket_key='s3://bucket/data/{{ ds }}/READY',
      poke_interval=60,         # check every 60 seconds
      timeout=60 * 60 * 4,      # give up after 4 hours
      mode='reschedule',        # don't hog worker slot
  )
```

```
7.2 Sensor Modes: poke vs reschedule
POKE (default)
   Sensor holds a worker slot the entire time it's waiting
   Pros: tight latency once condition met
   Cons: 10 sensors each waiting 1 hour = 10 worker slots tied up
RESCHEDULE
   Sensor checks once, then "reschedules" itself, releasing the slot
   Worker is free for other tasks between checks
   Pros: scales to thousands of waiting sensors
   Cons: extra DB load (each reschedule is a state transition)
   Use for poke_interval > 60 seconds
PICK: reschedule, unless poke_interval < 30s AND latency matters.
OR — better — use DEFERRABLE operators (next).
```

7.3 Deferrable Operators (Airflow 2.2+) Deferrable operators are the modern answer to 'wait for external thing'. They release the worker slot completely while waiting, and the wait is handled by a separate triggerer process running asyncio. A single triggerer can handle tens of thousands of concurrent waits. # Classic sensor: holds worker slot S3KeySensor(task_id='wait', bucket_key=..., mode='reschedule') # Deferrable version: releases slot, async wait in triggerer S3KeySensor(task_id='wait', bucket_key=..., deferrable=True) # Many operators now have `deferrable=True`: DatabricksSubmitRunOperator(deferrable=True) BigQueryInsertJobOperator(deferrable=True) ExternalTaskSensor(deferrable=True) TimeSensorAsync, DateTimeSensorAsync, ... REQUIRES: airflow triggerer

```
  process running (separate from scheduler).
  Same DB, talks to executor via state transitions.
```

```
7.4 Why Deferrable Changes Everything
SCENARIO: 500 DAGs, each with a sensor waiting up to 6 hours for upstream
  NON-DEFERRABLE (poke):
    500 worker slots permanently consumed
    Need ~500+ worker slots cluster-wide
    Cost: $$$ for idle workers
  RESCHEDULE MODE:
    Slots freed between pokes — but every poke = DB write
    Scheduler / DB load high
  DEFERRABLE:
    All 500 waits handled by ONE triggerer process
    Worker slots used only for actual work
    Cost: tiny
If you're operating Airflow at scale and not using deferrable, that's
your single biggest cost-reduction opportunity.
■ Interview angle: Q: You have 1000 DAGs, each starting with a sensor that waits up to 12 hours for a file. Workers
are constantly saturated. What do you do?
Staff: Convert to deferrable. With poke mode, you'd need 1000+ worker slots permanently dedicated to waiting.
Reschedule mode helps but still loads the scheduler and DB. Deferrable lets one triggerer process handle all 1000
waits concurrently via asyncio — worker slots are released entirely until the condition is met. Single change, massive
cost reduction. Also worth examining whether the file dependency is the right pattern at all — Datasets (Ch 16) may
be cleaner for downstream-of-upstream patterns.
```

8. XComs, Variables & Connections

```
8.1 XComs (Cross-Communication)
XComs are KEY-VALUE messages between tasks, stored in the metadata DB.
USE FOR:
  ✓ Job IDs, run IDs, sequence numbers
  ✓ Row counts, summary stats
  ✓ File paths, table names
  ✓ Configuration / state flags
DON'T USE FOR:
  ✗ Large data (DataFrames, raw rows, anything > a few KB)
  ✗ Sensitive secrets (use Connections / Variables)
DEFAULT SIZE LIMITS:
   Postgres backend: ~1 GB per XCom (but DON'T)
   MySQL backend: 64 KB per XCom
XCom backend can be customized to use S3 / GCS for big payloads
(via custom XCom backend class). But usually better: pass a PATH
to S3, let the next task read it itself.
```

```python
8.2 XCom in TaskFlow
@task
def extract():
    return {'count': 100, 'path': 's3://...'}    # auto-XCom
@task
def transform(input_):
    print(input_['count'])
transform(extract())
# Old-style XCom push/pull:
def extract(**ctx):
    ctx['ti'].xcom_push(key='count', value=100)
def transform(**ctx):
    count = ctx['ti'].xcom_pull(task_ids='extract', key='count')
```

8.3 Variables VARIABLES = global key-value store for DAG configuration. # Set in UI or via CLI / API: airflow variables set max_parallel_jobs 10 # Read in DAG: from airflow.models import Variable max_jobs = Variable.get('max_parallel_jobs', default_var=5) ■ Pitfalls:  Variable.get() at MODULE level = DB query on every parse = slow + load  Prefer Variable.get() inside tasks, not at module level  For static config that rarely changes: env var or YAML in repo  For dynamic config: Variables, but read in tasks RECOMMENDED: only put truly dynamic configuration in Variables. Static config belongs in code or env.

```
8.4 Connections
CONNECTIONS = stored credentials for external systems
   Database connections (host, port, user, password, schema)
   Cloud credentials (AWS, GCP, Azure)
   API endpoints
Stored ENCRYPTED in the metadata DB (using Fernet key).
Optionally backed by an EXTERNAL SECRETS BACKEND:
   AWS Secrets Manager
   HashiCorp Vault
   GCP Secret Manager
   Azure Key Vault
CONFIG:
  AIRFLOW__SECRETS__BACKEND=airflow.providers.amazon.aws.secrets.secrets_manager...
USE:
  conn = BaseHook.get_connection('snowflake_default')
  # or in operators:
  SnowflakeOperator(snowflake_conn_id='snowflake_default', ...)
■■ Warning: Never hard-code credentials in DAG code, not even temporarily. The git history is forever. Use
Connections + a secrets backend from day one.
■ Principal mindset: At staff scale, secrets management belongs in Vault / Secrets Manager, not in Airflow's
metadata DB. The metadata DB's Fernet encryption is fine, but centralized secrets management gives you rotation,
audit, and access control for free. Airflow becomes a consumer, not a store.
```

#### 9. Dynamic Task Mapping & Task Groups

```python
9.1 Dynamic Task Mapping (Airflow 2.3+)
Sometimes you don't know at parse time how many tasks you need — depends on data discovered at run
time (number of files to process, number of partitions to backfill). Dynamic task mapping (.expand) creates
tasks at run time.
@task
def discover_files(date):
    # returns a list of paths
    return [f's3://bucket/{date}/file-{i}.json' for i in range(N)]
@task
def process_file(path):
    # process one file
    ...
# .expand creates one task per item in the list
process_file.expand(path=discover_files('{{ ds }}'))
# Result: N parallel process_file tasks, one per file
# UI shows them as a "mapped" group
```

```
9.2 Multi-argument Mapping
# expand() — Cartesian product
my_task.expand(x=[1, 2], y=['a', 'b'])
# → 4 tasks: (1,a), (1,b), (2,a), (2,b)
# expand_kwargs() — zipped pairs
my_task.expand_kwargs([
    {'x': 1, 'y': 'a'},
    {'x': 2, 'y': 'b'},
])
# → 2 tasks: (1,a), (2,b)
# partial() — fixed args
my_task.partial(config='prod').expand(file=files)
```

```python
9.3 Limits on Mapping
 Default cap: max_map_length = 1024 mapped tasks per task
  Configurable via [core] max_map_length
 Mapped tasks each have their own task instance row in DB
  → 10K mapped tasks = 10K DB rows per run, hurts metadata DB
 Mapped task return values can be reduced by next task:
    @task
    def reduce(results):
        return sum(results)
    reduce(process_file.expand(path=files))
PATTERN AT SCALE:
   Don't map to millions of tasks; group at a higher level
   For 10K+ items, run them inside ONE task that does the parallelism
    (e.g., Spark job processing 10K files internally)
   Mapped tasks shine for tens to a few hundred parallel units
```

#### 9.4 Task Groups

with TaskGroup('preprocess') as preprocess: validate = PythonOperator(task_id='validate', ...) clean = PythonOperator(task_id='clean', ...) validate >> clean with TaskGroup('main') as main: @task def process(): ... process() preprocess >> main # Renders as collapsible groups in UI. Task IDs become preprocess.validate, etc. # Useful for: complex DAGs with logical phases, repeated subgraphs. ■ Interview angle: Q: A DAG processes a directory of files. The number varies day-to-day from 10 to 10,000. How do you structure it? Staff: Depends on volume and per-file work. If files are small (KBs) and the work is light (validate, move), dynamic task mapping works up to a few hundred. Beyond that, the metadata DB overhead becomes the bottleneck — each mapped task is a row. For 10K files: one task that lists, then one Spark/dbt task that processes all of them as a batch. The orchestrator's job is scheduling boundaries, not fine-grained parallelism — that's what the compute engine does. Picking the right grain of task is a staff skill.

## PART III — SCHEDULING & EXECUTION

#### 10. Schedule Intervals, Cron & Data Intervals

10.1 The Critical Concept: Data Intervals This is the single most misunderstood part of Airflow. Airflow schedules DAGs based on data intervals, not wall-clock 'start' times. A DAG with schedule='@daily' and start_date=2024 01 creates its first run at the END of the first interval — i.e., midnight 2024 02 — and that run processes data for 2024 01. KEY DATES (Airflow 2.2+ terminology): data_interval_start — beginning of data window this run covers data_interval_end — end of data window logical_date — same as data_interval_start; canonical run ID execution_date — DEPRECATED alias for logical_date EXAMPLE: @daily DAG with start_date=2024 01, first run executes at 2024 02: data_interval_start = 2024 01 00:00 data_interval_end = 2024 02 00:00 logical_date = 2024 01 In your task: WHERE event_date >= '{{ data_interval_start }}' AND event_date < '{{ data_interval_end }}' WHY: when the daily run for 2024 01 EXECUTES, the day must be over. You can't process Jan 1 data until Jan 1 is finished.

```
10.2 Schedule Expressions
STRING PRESETS:
  '@once'          — run exactly once
  '@hourly'        — at the top of every hour
  '@daily'         — at midnight UTC
  '@weekly'        — Sunday at midnight UTC
  '@monthly'       — 1st of month at midnight UTC
  '@yearly'        — Jan 1 at midnight UTC
CRON:
  '0 2 * * *'      — daily at 02:00 UTC
  '*/15 * * * *'   — every 15 minutes
  '0 0 * * MON'    — Mondays at midnight UTC
TIMEDELTA:
  timedelta(hours=6)   — every 6 hours from start_date
  timedelta(days=1)    — every day (different from @daily — anchored to start_date)
DATASET-BASED (data-aware, Ch 16):
  schedule=[Dataset("s3://bucket/raw/")]    — runs when dataset updated
NONE:
  schedule=None        — manually triggered only
```

#### 10.3 Common Schedule Mistakes

```
1. EXPECTING @DAILY TO MEAN "RUN AT 12:00 AM EVERY DAY"
   It runs AFTER midnight for the previous day's data interval.
2. CONFUSING execution_date WITH "NOW"
   {{ ds }} = the logical_date (data_interval_start)
   {{ ts_nodash }} = "now" at execution time
   For partition values, use {{ ds }} — NOT now().
3. CHANGING SCHEDULE WITHOUT REPLACING DAG
   Airflow doesn't always handle schedule changes cleanly on existing DAGs.
   Safer: new DAG with new dag_id, deprecate old.
4. START_DATE = NOW
   Avoid. The DAG won't be eligible to run until next interval boundary.
   Better: fixed start_date in the past (often a year+ ago).
5. NOT SETTING catchup=False
   Default catchup=True means a DAG with start_date a year ago will
   immediately try to run 365 historical runs on first deploy.
■■ Warning: Always set catchup=False by default. Enable explicit catchup only when you actually want to
backfill historical runs. The default-True behavior has caused countless production incidents.
```

#### 11. Catchup, Backfill & Reruns

```
11.1 Catchup
CATCHUP = automatic execution of all missed runs since start_date.
with DAG(start_date=datetime(2024, 1, 1), schedule='@daily', catchup=True):
    # Deploy this on 2024
01 → Airflow immediately schedules
    # 152 historical runs (Jan 1 through May 31).
WHEN YOU WANT IT:
  ✓ True backfill workflow: you've added a new pipeline, want history processed
  ✓ DAG was paused for a while; want to catch up missed scheduled runs
WHEN YOU DON'T:
  ✗ Most cases. catchup=True is responsible for many "what happened?!" incidents.
  ✗ Pipelines where historical re-runs cost real $$ (warehouse compute, API calls)
  ✗ Streaming-style sources where there's no history to re-process
DEFAULT: catchup=True (don't rely on it — set explicitly)
RECOMMENDED: catchup=False as default; explicit backfill via CLI when needed.
```

```
11.2 Backfill
BACKFILL = explicit re-run of a date range.
# CLI:
airflow dags backfill my_dag -s 2024
01 -e 2024
31
# UI: "Trigger DAG" with date range, or "Run Failed Tasks"
CONTROLS:
   max_active_runs limits parallelism
   catchup setting affects whether scheduler also catches up
   Idempotency is essential — running the same date twice should be safe
BACKFILL AT SCALE:
   1-year backfill of an hourly DAG = 8760 runs
   Workers must keep up; metadata DB writes spike
   Consider running backfill against a separate worker pool
   Or: run the underlying compute as one big batch job, skip Airflow per-day
```

```
11.3 Rerunning a Single Task
COMMON RERUN PATTERNS:
1. RETRY FAILED TASKS
   In UI: click task instance → "Clear" → it'll re-run.
   CLI: airflow tasks clear my_dag -t my_task -s 2024
01
2. RUN DOWNSTREAM AFTER FIXING UPSTREAM
   Clear failed task + downstream (option in UI).
3. RE-RUN A WHOLE DAG RUN
   Clear all task instances for that DAG run.
4. RUN A SPECIFIC PAST DATE
   "Trigger DAG with config" + set logical_date.
ANTI-PATTERN: re-running with depends_on_past=True
  One failed past run blocks all future. depends_on_past is a maintenance
  liability at scale — avoid unless your business logic truly requires it.
```

```sql
11.4 Idempotency in Practice
GOAL: same task instance, run twice, produces same end state.
# Bad: append-only INSERT
INSERT INTO sales SELECT * FROM raw WHERE date = '{{ ds }}';
# Re-run = duplicates.
# Good: idempotent MERGE / replace partition
DELETE FROM sales WHERE date = '{{ ds }}';
INSERT INTO sales SELECT * FROM raw WHERE date = '{{ ds }}';
# Or: MERGE INTO (Delta, Snowflake, BigQuery)
# Or: write to PARTITION = '{{ ds }}' (overwrite that partition)
# Files:
s3://bucket/output/date={{ ds }}/...    # one path per date; overwrite cleanly
# External API calls (non-idempotent by nature):
# Use idempotency keys in the request itself, OR a "did this run already" check.
■ Principal mindset: Idempotency is the single non-negotiable property of every task. If you can't explain how to
safely re-run a task, the task is broken. At staff level, every PR review starts with 'is this idempotent?'.
```

#### 12. Executors: Local, Celery, Kubernetes

12.1 The Executor Choices Executor Scale Setup Best for SequentialExecutor 1 task Trivial Testing only LocalExecutor ~50 None Small teams CeleryExecutor ~10K Redis/Rabbit Mid-large, stable workloads KubernetesExecutor ~10K K8s cluster Mixed workloads, isolation CeleryKubernetes ~10K Both Hybrid (rare) LocalKubernetes ~100 K8s Small K8s setups

12.2 LocalExecutor  Workers are subprocesses on the scheduler host  No external infrastructure  Parallelism = number of CPU cores on host (default) GOOD: dev, very small teams, lightweight workloads BAD: single point of failure; doesn't scale beyond one host CONFIG: AIRFLOW__CORE__EXECUTOR=LocalExecutor AIRFLOW__CORE__PARALLELISM=32

```
12.3 CeleryExecutor
ARCHITECTURE:
  Scheduler → Redis/RabbitMQ (broker) → N Celery workers
PROS:
  ✓ Horizontal worker scaling (add nodes)
  ✓ Mature, stable, well-understood
  ✓ Workers can be specialized (queues)
  ✓ Lower latency than KubernetesExecutor (no pod startup)
CONS:
  ✗ Workers are long-lived → dependency conflicts ("works on worker A, not B")
  ✗ Resource sharing between tasks (a memory hog affects neighbors)
  ✗ Operational overhead: broker, worker autoscaling
CONFIG:
  AIRFLOW__CORE__EXECUTOR=CeleryExecutor
  AIRFLOW__CELERY__BROKER_URL=redis://...
  AIRFLOW__CELERY__RESULT_BACKEND=db+postgresql://...
  AIRFLOW__CELERY__WORKER_CONCURRENCY=16
QUEUES:
  Different workers listen on different queues:
  worker --queues high_priority,default
  worker --queues spark_only
  task = MyOp(task_id='x', queue='spark_only')
```

```
12.4 KubernetesExecutor
ARCHITECTURE:
  Scheduler → K8s API → pod per task
```

```
EACH TASK = ONE POD
   Pod is launched, runs the task, dies
   Resource isolation (CPU, memory limits per pod)
   Image flexibility (every task can use different image)
   No dependency conflicts (each task is its own container)
PROS:
  ✓ Strong isolation
  ✓ Auto-scaling via K8s HPA / cluster autoscaler
  ✓ Per-task resource control
  ✓ No idle workers (pods only exist when tasks run)
CONS:
  ✗ Pod startup latency (5-30 seconds)
  ✗ K8s operational complexity
  ✗ More moving parts to debug
CONFIG:
  AIRFLOW__CORE__EXECUTOR=KubernetesExecutor
  AIRFLOW__KUBERNETES_EXECUTOR__POD_TEMPLATE_FILE=/path/to/template.yaml
```

```
12.5 Picking an Executor
STARTUP / SMALL TEAM (≤ 20 DAGs, ≤ 1000 task-runs/day)
  LocalExecutor — done. Don't over-engineer.
GROWING (≤ 200 DAGs, ≤ 10K task-runs/day, stable workloads)
  CeleryExecutor — most operationally simple at this scale.
LARGE / VARIABLE (200+ DAGs, mixed workloads, peak/trough patterns)
  KubernetesExecutor — pay startup latency for autoscaling + isolation.
VERY LARGE
  KubernetesExecutor with multi-cluster routing
  OR managed Airflow (MWAA, Astronomer, Composer)
■ Interview angle: Q: Celery vs Kubernetes executor — when to switch?
Staff: Celery is great for steady-state high throughput — pre-warmed workers, no startup latency. The trouble starts
when you have variable load (idle workers cost real money), heterogeneous workloads (one task needs 32 GB,
another 256 MB), or dependency conflicts (different tasks need different Python envs). Kubernetes solves all three:
pods only exist when tasks run, per-pod resources, per-pod images. Cost: 5-30s pod startup per task. For tasks that
run for hours, that's noise; for tasks that run in seconds, it's 10× overhead. So the rule: short tasks → Celery; long
heterogeneous tasks → Kubernetes; mixed → KubernetesPodOperator on Celery (best of both).
```

#### 13. Queues, Pools & Concurrency Control

```
13.1 The Concurrency Knobs
GLOBAL (scheduler/cluster level)
  AIRFLOW__CORE__PARALLELISM=32              # max tasks running cluster-wide
  AIRFLOW__CORE__MAX_ACTIVE_TASKS_PER_DAG=16 # max concurrent tasks per DAG
PER-DAG
  with DAG(..., max_active_runs=1):          # only 1 run of this DAG at a time
  with DAG(..., max_active_tasks=8):         # max 8 concurrent tasks in this DAG
  with DAG(..., concurrency=8):              # DEPRECATED alias
PER-TASK
  task = MyOp(task_id='x', max_active_tis_per_dag=4)  # ≤ 4 of this task across runs
  task = MyOp(task_id='x', max_active_tis_per_dagrun=2) # ≤ 2 within one run
POOLS (cluster-wide bottleneck resources)
  airflow pools set snowflake_warehouse 10 "Snowflake compute concurrency"
  task = SnowflakeOp(task_id='x', pool='snowflake_warehouse')
  # Only 10 tasks in this pool run concurrently
QUEUES (route tasks to specific workers — Celery)
  task = MyOp(task_id='heavy', queue='gpu_workers')
```

```
13.2 When to Use What
POOLS for shared external resources
   Snowflake/BigQuery compute limits
   API rate limits
   Database connection pools
  → Multiple DAGs share a pool, fair queue
QUEUES for routing to specialized workers
   GPU workers for ML tasks
   High-memory workers for big jobs
   Tier-1 workers reserved for critical DAGs
  → Worker capacity, not resource semaphore
max_active_runs for serial pipelines
   "Only one daily ETL should run at a time"
   Prevents concurrent runs (e.g., during catchup)
max_active_tasks for fair-share within a DAG
   "This DAG can do at most 16 tasks at once"
   Prevents one DAG from hogging the cluster
PARALLELISM as the safety ceiling
   Cluster-wide max — never let any combination exceed this
```

13.3 The Pool Pattern in Practice # Snowflake limits us to 8 concurrent queries on this warehouse airflow pools set snowflake_warehouse_xs 8 "XS warehouse capacity" # In DAG: load = SnowflakeOperator( task_id='load', pool='snowflake_warehouse_xs', pool_slots=1, sql=..., ) # A heavy task can claim more slots:

big_load = SnowflakeOperator( task_id='big_load', pool='snowflake_warehouse_xs', pool_slots=4, # uses 4 of 8 slots sql=..., ) ■ Production scenario: A team ran 200 daily DAGs each loading to the same Snowflake warehouse. Warehouse spilled and queued queries — total run time stretched from 90 min to 6 hours. Adding a Pool with slots = warehouse's intended concurrency (16) made the queue Airflow-side instead of Snowflake-side. Total time dropped to 100 min, Snowflake spill disappeared, and the UI showed exactly which tasks were waiting. Pools turn invisible contention into visible queuing. ■ Interview angle: Q: Three teams' DAGs all hit the same BigQuery slot reservation, causing query queuing and unpredictable SLAs. How do you fix it in Airflow? Staff: Pool with slots = BigQuery reservation slots. All tasks targeting that reservation use the pool. Airflow now queues at the pool level, and you control fairness via pool_slots per task (big queries claim more slots). For team isolation, two pools (team_a_bq, team_b_bq) with split reservation. Bonus: pool stats are exposed as Airflow metrics, so you can alert on 'pool saturation > 90% for 1 hour'.

## PART IV — RELIABILITY & CORRECTNESS

#### 14. Idempotency & Re-runnability

```
14.1 The Idempotency Principle
DEFINITION:
  A task is idempotent if running it N times produces the same final state
  as running it once.
WHY:
   Retries (Airflow's default behavior on failure)
   Manual re-runs (operator clearing a task)
   Backfills (running historical date ranges)
   Failure recovery (re-running incomplete pipeline)
EVERY TASK IN PRODUCTION MUST BE IDEMPOTENT.
No exceptions. The cost of fixing non-idempotent tasks under fire is far
higher than the discipline of writing them right the first time.
```

```sql
14.2 Patterns by Workload Type
1. SQL TRANSFORMS
   ■ INSERT INTO ... SELECT ... WHERE date = '{{ ds }}';
   ✓  DELETE / TRUNCATE the partition first:
       DELETE FROM sales WHERE date = '{{ ds }}';
       INSERT INTO sales SELECT ... WHERE date = '{{ ds }}';
   ✓  OR use MERGE / UPSERT with the date as part of the merge key
   ✓  OR write to PARTITION='{{ ds }}' (Hive/Iceberg/Delta-style overwrite)
2. FILE WRITES
   ■ Append to s3://bucket/output.csv
   ✓  Date-partitioned path: s3://bucket/dt={{ ds }}/output.parquet
       (overwrite that specific path; downstream reads whole prefix)
3. API CALLS
   ■ Naively send POST /charge ...
   ✓  POST /charge with Idempotency-Key: <deterministic_uuid_from_task>
   ✓  Or: check-then-do pattern with a stored 'completed' marker
4. SPARK / DBT JOBS
   ✓  Use Delta/Iceberg MERGE for upserts
   ✓  dbt models are idempotent by design (replace whole table or incremental)
5. SIDE EFFECTS (alerts, notifications)
   ■ "Send email when this completes"
   ✓  Send-once mechanism: write completion flag, send only on first transition
   ✓  Or accept that alerts may dup — make recipient idempotent
```

14.3 The Deterministic Key Pattern For any operation that needs a unique ID across retries, derive the ID from the task context — NOT from random/now(). # WRONG: random ID changes per attempt import uuid job_id = str(uuid.uuid4())

```
# RIGHT: derived from logical_date + task_id, stable across retries
@task
def run_job(**ctx):
    job_id = f"{ctx['dag'].dag_id}__{ctx['task'].task_id}__{ctx['ds']}"
    api.submit(idempotency_key=job_id, ...)
This pattern works for:
   External API submissions (Snowflake task IDs, Databricks run names)
   Outbox-style message emission
   Distributed lock keys
```

```
14.4 Stateful Operations and Idempotency
SOMETIMES YOU CAN'T MAKE A TASK IDEMPOTENT
   Sending a one-time payment
   Charging a customer credit card
   Sending a notification
OPTIONS:
  1. PUSH IDEMPOTENCY UPSTREAM
     The API/service itself accepts idempotency keys.
  2. WRITE-ONCE LEDGER
     Before executing, write "intent" row. Check ledger before re-run.
  3. ACCEPT MANUAL INTERVENTION
     Mark the task as "do not auto-retry"; on failure, on-call investigates.
  4. SAGAS WITH COMPENSATING ACTIONS
     If retry happens, also run a compensating action (refund, etc.).
■ Principal mindset: Idempotency is a property of the task code, not the orchestrator. Airflow gives you retries,
backfills, manual reruns — these are POWERFUL when tasks are idempotent, and DEVASTATING when they're
not. The cost of non-idempotent tasks compounds: every incident requires manual fixes, every pipeline change is
scary, no one trusts the data. Make 'is this idempotent?' the first PR review question.
```

#### 15. Retries, SLAs & Failure Modes

```
15.1 The Retry Configuration
default_args = {
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'retry_exponential_backoff': True,      # 5min, 10min, 20min ...
    'max_retry_delay': timedelta(hours=1),  # cap
    'retry_jitter': 0.5,                    # 0..1 fraction, helps thundering herd
}
WHEN A TASK FAILS:
   State → up_for_retry
   Scheduled again after retry_delay
   After N retries → state → failed
   Downstream tasks check trigger_rule (default all_success → skip)
EXCEPTIONS:
  Some exceptions skip retry: AirflowSkipException, AirflowFailException
  Sensors with timeout reach state 'failed' on timeout, retried per config
```

```
15.2 Setting Retry Counts by Failure Type
TRANSIENT FAILURES (network blip, rate limit, momentary API timeout)
  retries: 3-5 with exponential backoff + jitter
DETERMINISTIC FAILURES (bad SQL, missing column, type error)
  retries: 0 — retrying won't help, just delays the fix
  Set retries=0 explicitly to fail fast and alert
EXTERNAL SYSTEM ISSUES (warehouse spilling, infra outage)
  retries: high (5-10) with long backoff — the system may recover
SENSORS
  Configure timeout vs retries carefully:
    timeout=4 * 3600, retries=0 → fail after 4 hours
    timeout=1 * 3600, retries=3 → fail after up to 4 hours of cumulative wait
```

```
15.3 SLAs (Service Level Agreements)
TASK-LEVEL SLA:
  task = MyOp(task_id='x', sla=timedelta(hours=2))
  # If task doesn't complete within 2 hours of scheduled time,
  # an SLA miss is recorded and on_sla_miss_callback fires.
DAG-LEVEL SLA:
  Not directly supported — implement via task SLA on the final task.
SLAs ARE NOTIFICATIONS, NOT GUARANTEES:
   SLA miss fires a callback — doesn't kill or restart the task
   Useful for alerting: "we haven't seen success by 6 AM, page on-call"
CAVEATS (Airflow SLA has historical bugs):
   SLA callbacks are checked by scheduler, not exact
   If scheduler is busy, SLA detection is delayed
   Many teams use EXTERNAL SLO monitoring instead (Datadog, Grafana)
    Track task completion via metrics, alert via SLO tool.
```

15.4 Failure Callbacks def slack_alert(context): task = context['task_instance']

```
    msg = f":red_circle: {task.dag_id} / {task.task_id} failed at {task.try_number}"
    slack.post_message(channel='#data-alerts', text=msg)
default_args = {
    'on_failure_callback': slack_alert,
    'on_retry_callback': retry_alert,        # also called on retries
    'on_success_callback': None,             # rarely useful
    'sla_miss_callback': sla_alert,
}
DAG-LEVEL:
  with DAG(..., on_failure_callback=dag_failure_alert):
    # Fires when ANY task in the DAG fails the run
CALLBACK BEST PRACTICES:
   Keep them fast (run synchronously in scheduler/executor)
   Don't make external API calls that can hang
   Don't raise exceptions inside callbacks (silent failures)
   Log heavily — debugging callbacks is painful
```

```
15.5 Common Failure Modes
 Task TIMEOUT (operator hung)
   → Set execution_timeout=timedelta(...) on long-running tasks
   → Without it, a hung task holds worker slot indefinitely
 OOM in worker
   → KubernetesPodOperator with proper resources; Celery worker_memory limits
 Scheduler heartbeat failure
   → Scheduler considered dead after AIRFLOW__SCHEDULER__SCHEDULER_HEALTH_CHECK_THRESHOLD seconds
   → Another scheduler (in HA) takes over; orphaned tasks retried
 Worker dies mid-task
   → Task becomes 'up_for_retry' after timeout
   → Configure max_active_tis_per_dag to avoid retry storms
 Task succeeds but state not reported (network partition)
   → Airflow may retry; task code MUST be idempotent
 Metadata DB outage
   → Airflow becomes unresponsive; tasks can't transition state
   → Implies HA Postgres, monitoring, automatic failover
```

#### 16. Data-Aware Scheduling (Datasets)

16.1 The Pre-Dataset World Before Datasets (Airflow 2.4+), inter-DAG dependencies required either: (1) ExternalTaskSensor — poll the other DAG's task, brittle and slow, or (2) TriggerDagRunOperator — explicit downstream trigger, tight coupling. Both patterns scale poorly across team boundaries.

```python
16.2 Datasets: The Modern Pattern
from airflow import Dataset
raw_sales = Dataset('s3://lake/raw/sales')
gold_sales = Dataset('s3://lake/gold/sales')
# DAG A produces the dataset
@dag(schedule='@daily', ...)
def extract_dag():
    @task(outlets=[raw_sales])
    def extract(): ...
# DAG B is triggered when the dataset updates
@dag(schedule=[raw_sales], ...)
def transform_dag():
    @task(outlets=[gold_sales])
    def transform(): ...
# DAG C is triggered when EITHER raw_sales OR gold_sales updates
@dag(schedule=[raw_sales, gold_sales], ...)
def consumer_dag():
    ...
KEY POINTS:
   Dataset URI is a STRING identifier — no actual storage check by Airflow
   Producer DAG marks the dataset 'updated' when its task succeeds
   Consumer DAGs scheduled to run on dataset update
   Multiple datasets in schedule → ALL must update (AND semantics)
   Cross-DAG, decoupled, no sensors needed
```

```
16.3 When to Use Datasets
GOOD FIT:
  ✓ Cross-team DAG dependencies (team A produces, team B consumes)
  ✓ Conditional downstream triggering (run only when data actually arrived)
  ✓ Removing brittle ExternalTaskSensor patterns
  ✓ Multi-producer fan-in ("run when both A and B finish")
LIMITATIONS:
  ✗ Datasets are identifiers, not real-time tracking — Airflow doesn't poll
  ✗ Only one producer DAG should claim outlets (multiple producers = race)
  ✗ Manual triggering doesn't update dataset state (only scheduled runs do)
  ✗ Dataset URIs are strings — no validation, typos silently break links
ADVANCED (Airflow 2.9+):
   Dataset aliases (rename without breaking downstream)
   DatasetEvent extras (pass metadata about the update)
   Conditional triggering with custom expressions
```

16.4 Migration Path OLD: with DAG(...) as transform_dag:

```
      wait_for_extract = ExternalTaskSensor(
          external_dag_id='extract_dag',
          external_task_id='extract',
          mode='reschedule',
          timeout=4*3600,
      )
      wait_for_extract >> transform_task
NEW:
  raw_data = Dataset('s3://bucket/raw/')
  with DAG('extract_dag', ...) as extract_dag:
      @task(outlets=[raw_data])
      def extract(): ...
  with DAG('transform_dag', schedule=[raw_data]) as transform_dag:
      @task
      def transform(): ...
BENEFITS:
   No sensor → no worker slot consumed waiting
   No polling latency
   Schedule is documented (you can see "I run when X updates")
   Cross-team contract is explicit
■ Interview angle: Q: Three DAGs depend on the output of an upstream DAG that may run anywhere from 2 to 8
hours. How do you wire them up?
Staff: Three patterns to choose between. (1) ExternalTaskSensor with deferrable=True — works, but slightly stale,
and you're polling. (2) Datasets — the modern answer; producer DAG has outlet, three consumers schedule on that
dataset. Decoupled, no sensors, automatic. (3) TriggerDagRunOperator from upstream — works but couples
upstream to downstream (upstream must know about consumers). I'd default to Datasets — it scales to N
consumers without changing the producer.
```

## PART V — SCALING AIRFLOW

#### 17. Scaling the Scheduler & Webserver

```
17.1 The Scheduler Scaling Path
SMALL (≤ 50 DAGs)
   1 scheduler instance
   Default config works
   Watch: DAG parse time, scheduler heartbeat
MEDIUM (50-500 DAGs)
   Tune: PARSING_PROCESSES, MIN_FILE_PROCESS_INTERVAL
   Consider 2 scheduler replicas (HA)
   Optimize DAG files for fast parsing
   Monitor: scheduler.dag_processing.total_parse_time
LARGE (500-5000 DAGs)
   3+ scheduler replicas
   DAG files possibly distributed across multiple folders
   Metadata DB tuning critical
   Externalize logs (S3) so webserver doesn't hit local FS
   Consider Dag bundle / serialization (Airflow 3.0+)
VERY LARGE (5000+ DAGs)
   Multi-cluster Airflow (split by team/domain)
   Consider managed offerings (Astronomer, MWAA, Composer)
   Custom monitoring and on-call rotation
```

```
17.2 DAG Parsing Tuning
# How many DAG-parser processes per scheduler
AIRFLOW__SCHEDULER__PARSING_PROCESSES=4         # default 2
# How often each DAG file is parsed (seconds)
AIRFLOW__SCHEDULER__MIN_FILE_PROCESS_INTERVAL=30 # default 30
# → Increasing reduces load; decreasing improves DAG-change latency
# Max number of TaskInstances examined per query
AIRFLOW__SCHEDULER__MAX_TIS_PER_QUERY=512        # default 512
# How long to wait between scheduler "loops"
AIRFLOW__SCHEDULER__SCHEDULER_HEARTBEAT_SEC=5    # default 5
# Limit on simultaneously active DAG runs across cluster
AIRFLOW__CORE__MAX_ACTIVE_TASKS_PER_DAG=16
AIRFLOW__CORE__MAX_ACTIVE_RUNS_PER_DAG=16
AIRFLOW__CORE__PARALLELISM=32                     # cluster-wide cap
```

```
17.3 DAG Serialization
SERIALIZED DAGs (default since 2.0):
   DAG structure stored in serialized_dag table
   Webserver reads from DB, not by importing Python files
   Faster webserver startup, smaller memory footprint
   Scheduler still parses Python files (creates the serialized form)
DAG VERSIONING (Airflow 3.0):
```

```
   Multiple versions of a DAG can coexist
   Old runs use the old serialized form
   Resolves the "I changed the DAG but old runs look weird" problem
```

17.4 Webserver Scaling  Stateless — scale horizontally behind LB  Memory grows with: serialized DAG size × number of DAGs  Tune: AIRFLOW__WEBSERVER__WORKER_REFRESH_INTERVAL  Log access patterns: read from S3/GCS, not local DEPLOYMENT: 3 webserver replicas behind LB Connection pooling to metadata DB (PgBouncer) Caching headers for static assets ■ Production scenario: A 2000-DAG Airflow installation had 5-minute scheduling latency: DAGs scheduled for 09:00 actually started at 09:05. Investigation: total DAG parse time was 200 seconds with only 2 parsing processes. Each DAG was being re-parsed every 30 seconds. With min_file_process_interval=60 and parsing_processes=8, parse time dropped to 35 seconds; latency to <30 seconds. The fix was a one-line config change — but you have to know which knob to turn.

#### 18. Metadata DB & Performance Tuning

18.1 The Metadata DB is the Bottleneck At scale, the metadata DB is the single most important component to monitor and tune. Every scheduler heartbeat, every task state transition, every XCom, every UI page load hits it. A slow metadata DB = slow Airflow.

```
18.2 Schema Hot Spots
TABLES WITH HIGH WRITE LOAD:
  task_instance       — state transitions
  job                 — scheduler heartbeats
  log                 — per-task logs (avoidable; ship logs to S3)
  xcom                — inter-task messages
  dag_run             — one row per run
  serialized_dag      — updated on DAG change
TABLES WITH HIGH READ LOAD:
  task_instance       — scheduler queries it constantly
  serialized_dag      — webserver reads on each request
  dag_run             — scheduler + UI
```

```
18.3 Tuning Postgres for Airflow
 Use Postgres 13+ (better partitioning, vacuum, parallelism)
 Connection pool: PgBouncer in transaction-pooling mode
 Tuning:
    shared_buffers = 25% of RAM
    work_mem = 16-32 MB (per-query, watch out for many connections)
    effective_cache_size = 75% of RAM
    max_connections = 100-200 (don't go higher; use PgBouncer)
    autovacuum_naptime = 30s (default 60s; we churn fast)
 MONITOR:
    pg_stat_statements — find slow queries
    n_dead_tup on task_instance — bloat indicator
    Long-running transactions (Airflow shouldn't have any)
 PARTITION (advanced):
    Partition task_instance by execution_date if > 100M rows
    Requires careful migration
```

18.4 Cleaning Up Old Data DB GROWS UNBOUNDED if you don't clean. # Airflow CLI: archive old metadata airflow db clean --clean-before-timestamp '2024 01' --tables dag_run,task_instance # Common retention: 90 days for runs, 180 days for logs # Schedule via cron / Airflow itself ARCHIVE BEFORE DELETE: Some teams export to S3 for compliance / debugging: airflow db export-archived-records --output-path s3://airflow-archive/ CAUTION: airflow db clean is destructive Test in staging first Run during low-traffic window

```
18.5 Log Shipping
DEFAULT: logs written to local disk of worker that ran the task.
  PROBLEM:
     Webserver can't show logs from a worker it doesn't have FS access to
     Worker died → logs lost
     Local disk fills up
PRODUCTION: remote logging to S3 / GCS / Azure Blob / Elasticsearch
  AIRFLOW__LOGGING__REMOTE_LOGGING=True
  AIRFLOW__LOGGING__REMOTE_BASE_LOG_FOLDER=s3://bucket/airflow-logs/
  AIRFLOW__LOGGING__REMOTE_LOG_CONN_ID=aws_default
BENEFITS:
   Persistent across worker failures
   Webserver can fetch from S3 regardless of which worker ran the task
   Log lifecycle policies (delete after N days)
CAVEATS:
   Latency: logs uploaded after task completion, not streamed
   Cost: many small log files in S3 add up; lifecycle policies essential
■ Principal mindset: The metadata DB is to Airflow what the master is to a distributed system. Monitor it: query
latency, connection count, bloat, replication lag. Treat it as a tier-1 production database with HA, backups,
monitoring, on-call. When Airflow is slow at scale, the metadata DB is the prime suspect 80% of the time.
```

#### 19. Multi-Tenancy & Team Isolation

```
19.1 The Multi-Tenant Problem
SINGLE-INSTANCE PAIN POINTS:
   One team's bad DAG can crash the scheduler for everyone
   Resource contention (one team hogs Celery workers)
   Credentials visible across teams
   DAG name collisions
   Schema drift in shared Variables / Connections
   Permissions: who can see/edit/trigger what
OPTIONS:
```

#### 1. Single instance + RBAC (simpler, weaker isolation)

#### 2. Single instance + namespacing conventions

#### 3. Multiple Airflow instances (one per team/domain)

#### 4. Managed offering with cross-tenancy features

```
19.2 RBAC in a Single Instance
ROLES (built-in):
  Public, Viewer, User, Op, Admin
CUSTOM ROLES: define per-DAG or per-tag access
  airflow roles create team_a
  airflow roles add-perms team_a --resource Dag:* --action can_read can_edit
DAG TAGS for filtering:
  with DAG(..., tags=['team-a', 'tier-1']):
AUTHENTICATION:
  Built-in (DB-backed)
  LDAP / Okta / OAuth / Keycloak via Flask-AppBuilder
  Always use SSO in production
CAVEATS:
   RBAC is for UI/API access — not for code-level isolation
   Anyone with DAG-write permission to a folder can write any code
   Variables/Connections are not natively namespaced (all global)
```

```
19.3 Namespacing Patterns
DAG IDs:
  team_a__sales_etl
  team_b__marketing_etl
  domain-prefixed makes filtering / RBAC sane
CONNECTIONS:
  team_a_snowflake_default
  team_b_snowflake_default
VARIABLES:
  team_a/max_jobs
  team_b/max_jobs
  (Airflow has nested variable support via path-like keys)
POOLS:
  per-team or per-resource:
    team_a_snowflake (20 slots)
    shared_external_apis (100 slots)
```

#### 19.4 When to Split Into Multiple Instances

```
SIGNALS:
  ✓ Different teams have different SLA requirements
  ✓ Compliance: one team must isolate (HIPAA, SOX)
  ✓ Scheduling latency suffers across teams
  ✓ Upgrade cadence varies (one team wants bleeding edge)
  ✓ Headcount > ~50 engineers writing DAGs
PATTERNS:
   Per-team Airflow on shared K8s cluster (low ops cost)
   Shared "tier-0 critical" instance + team instances
   Managed Airflow per team (MWAA, Composer, Astro)
TRADE-OFF:
  Multiple instances = more ops surface
  Cross-instance dependencies = harder (Datasets help, sort of)
■ Production scenario: A 30-engineer team ran one Airflow instance. Team A's DAG had a Variable.get() at
module level. Team B updated that Variable. Suddenly all of team A's DAGs failed parsing because of a key
mismatch. Resolution: split into per-domain instances. Variables/Connections became domain-owned, blast radius
shrunk, autonomy increased. Operational cost rose, but team cohesion and reliability dramatically improved.
```

#### 20. Observability: Logs, Metrics, Lineage

```
20.1 The Three Pillars
LOGS:
   Per-task logs (already covered — ship to S3/GCS/ES)
   Scheduler logs (helpful for diagnosing scheduling issues)
   Webserver logs (HTTP requests, audit)
METRICS:
   Airflow emits StatsD metrics (also Prometheus via airflow-prometheus-exporter)
   Key metrics:
      scheduler.heartbeat
      scheduler.dag_processing.processing_time
      executor.queued_tasks
      executor.running_tasks
      pool.*.used_slots / queued_slots / open_slots
      ti.duration.<dag>.<task>
      ti.start.<dag>.<task>
      ti.finish.<dag>.<task>.<state>
LINEAGE:
   Built-in lineage (limited)
   OpenLineage integration — emits lineage events to Marquez / Datahub
   Allows: "if this dataset is bad, what's downstream?"
```

```
20.2 The Dashboard You Need
SCHEDULER HEALTH
   Heartbeat freshness
   DAG parse time (p50, p99)
   Queued tasks (should not grow unbounded)
EXECUTION HEALTH
   Running tasks count vs parallelism cap
   Task duration distribution per DAG
   Failure rate per DAG / per task
POOLS / QUEUES
   Pool utilization (used / total)
   Queue depth in Celery
   Pod creation latency in K8s
DATA HEALTH (per DAG)
   Last successful run age
   SLA miss count
   Tasks duration vs historical baseline
EXTERNAL DEPENDENCIES
   Metadata DB query latency
   Redis/RabbitMQ broker health
   Worker count (Celery) / Pod count (K8s)
```

```
20.3 OpenLineage
OpenLineage = standard for lineage events.
Integration: airflow-openlineage-provider
Emits events on task start / complete with:
   Inputs (datasets read)
   Outputs (datasets written)
   Schema, row counts
   Run metadata
```

```
CONSUMERS:
   Marquez (OSS)
   DataHub (LinkedIn OSS)
   Atlan, Monte Carlo, Alation (commercial)
VALUE:
   "If sales_gold is broken, what dashboards depend on it?"
   "Why did this dashboard's number change? Trace upstream."
   Impact analysis before deploying a schema change
```

```
20.4 Alerting Strategy
TIER-1 (page on-call immediately):
   Scheduler down for >2 minutes
   Metadata DB unreachable
   Critical DAG SLA missed (tier-1 DAGs only)
   Task failure on tier-1 DAG with retries exhausted
TIER-2 (slack channel, business hours):
   Non-critical DAG failure (handled next business day)
   Pool saturation > 90% for 1 hour
   DAG parse time > 60s
TIER-3 (dashboard only):
   Individual task retries (info)
   SLA misses on tier-3 DAGs
   Worker pod restarts (transient)
■ Principal mindset: Observability is what separates a hobbyist Airflow from a production Airflow. At staff level, you
don't just deploy DAGs — you instrument them, you alert on them, you build dashboards for the operations team.
The first quarter at any new role: build the dashboards. They reveal more about the system than any code review.
```

## PART VI — PRODUCTION PATTERNS

```
21.1 The DAG Deployment Loop
COMMIT → CI tests → BUILD artifact → DEPLOY to Airflow → Airflow parses
CI CHECKS (essential):
  ✓ Python syntax / linting (ruff, mypy)
  ✓ DAG import test (catches parse errors before deploy)
  ✓ DAG validity test (no cycles, valid schedule)
  ✓ Unit tests for custom operators / Python callables
  ✓ Integration test in ephemeral environment (optional)
DEPLOYMENT PATTERNS:
  1. SHARED FILESYSTEM (NFS, EFS): copy DAG files, Airflow picks up
  2. GIT-SYNC: sidecar container in Airflow pods pulls latest from git
  3. CONTAINER IMAGES: DAGs baked into image, redeploy on change
  4. S3 / GCS sync: DAGs in bucket, Airflow workers download
POPULAR STACK (K8s + Airflow):
   DAGs in git repo
   CI builds + pushes container image with DAGs
   git-sync sidecar pulls latest into Airflow scheduler/worker pods
   New DAG visible in ~30 seconds without restart
```

```python
21.2 The DAG Import Test
# tests/test_dag_imports.py
import pytest
from airflow.models import DagBag
def test_dag_bag_imports():
    dag_bag = DagBag(dag_folder='dags/', include_examples=False)
    assert dag_bag.import_errors == {}, f"DAG import errors: {dag_bag.import_errors}"
def test_dag_count():
    dag_bag = DagBag(dag_folder='dags/', include_examples=False)
    assert len(dag_bag.dags) > 0
@pytest.mark.parametrize('dag_id', DagBag(dag_folder='dags/').dag_ids)
def test_dag_has_owner_and_tags(dag_id):
    dag_bag = DagBag(dag_folder='dags/', include_examples=False)
    dag = dag_bag.get_dag(dag_id)
    assert dag.default_args.get('owner') != 'airflow', f"{dag_id} missing owner"
    assert dag.tags, f"{dag_id} missing tags"
THESE TESTS CATCH 80% OF PRODUCTION BREAKAGES BEFORE DEPLOY.
```

```python
21.3 Testing DAG Logic
# Unit-test callables directly
def my_etl_function(data):
    return [x * 2 for x in data]
def test_my_etl():
    assert my_etl_function([1, 2, 3]) == [2, 4, 6]
```

# Test operators with mocks def test_my_dag_structure(): dag = create_my_dag() assert len(dag.tasks) == 5 assert dag.task_dict['extract'].downstream_task_ids == {'transform'} # Integration: ephemeral Airflow via Docker compose docker-compose -f tests/docker-compose.test.yml up # Run DAG, assert outputs in test DB

```
21.4 Deployment Strategies
1. BIG-BANG DEPLOY (all DAGs at once)
   Simple. Risky for many DAGs.
2. CANARY DEPLOY
   Deploy to a staging Airflow first. Run for 1 day. Promote.
   Requires staging Airflow with prod-like data access.
3. PER-DAG VERSIONING (Airflow 3.0+)
   Each DAG version stored separately. Old runs use old version.
   Safer for active development.
4. BLUE/GREEN
   Two Airflow instances. Promote DAGs to green; cut traffic over.
   Rare for Airflow; more common for application stacks.
■ Interview angle: Q: A DAG change broke production. What's your CI gap?
Staff: Common gaps: (1) no DAG import test — syntax/import errors only caught at deploy; (2) no DAG validity test
— cycles, invalid schedules slip through; (3) no environment parity — works in dev with mocked connections, fails in
prod with real ones; (4) no smoke test on staging Airflow; (5) no lineage check — schema change in upstream wasn't
caught until consumer DAG ran. The minimum bar: DAG import test, owner/tag presence test, smoke test on
staging. Most teams skip the staging step because 'Airflow doesn't have good staging' — but you can run an
ephemeral Airflow via docker-compose for PR-level integration tests.
```

#### 22. Secrets & Security

```
22.1 What Lives Where
METADATA DB (Fernet-encrypted)
   Connections (passwords, tokens)
   Variables marked sensitive (keys matching *secret*, *password*, *key*)
EXTERNAL SECRETS BACKEND (production)
   Connections AND Variables fetched on-demand from vault
   Airflow ONLY HOLDS A REFERENCE, not the secret value
EXAMPLES:
  AIRFLOW__SECRETS__BACKEND=airflow.providers.amazon.aws.secrets.secrets_manager.SecretsManagerBackend
  AIRFLOW__SECRETS__BACKEND_KWARGS={"connections_prefix":"airflow/connections"}
  When Connection 'snowflake_default' is requested:
    1. Look up secret 'airflow/connections/snowflake_default' in AWS Secrets Manager
```

#### 2. Cache for AIRFLOW__SECRETS__BACKEND_CACHE_TTL seconds

#### 3. Return Connection object to caller

```
22.2 The Fernet Key
Fernet key encrypts Connections + sensitive Variables in metadata DB.
REQUIREMENTS:
   One key per Airflow instance (or rotated keys with key list)
   NEVER commit to git
   Mount via secret (K8s secret, env var injected by orchestrator)
   Rotate: requires re-encrypting metadata DB
KEY ROTATION:
  AIRFLOW__CORE__FERNET_KEY=NEW_KEY,OLD_KEY
  # Airflow decrypts with any key in list, encrypts with first
  airflow rotate-fernet-key
  # Re-encrypts all entries with NEW_KEY
```

```
22.3 Access Control
AUTHENTICATION
   DB-backed (built-in, not for production)
   LDAP, OAuth, OpenID Connect via Flask-AppBuilder
   Always SSO in prod (Okta, Azure AD, Google Workspace)
AUTHORIZATION
   Built-in RBAC roles + custom roles
   Per-DAG permissions (read, edit, trigger)
   Per-resource (Variables, Connections, Pools)
API ACCESS
   Use REST API with token auth, not basic auth
   Rotate tokens; scope to least privilege
NETWORK
   Webserver behind VPN / SSO portal
   Workers in private subnet, only outbound to data sources
   Metadata DB never exposed publicly
```

22.4 Code-Level Security  Never hard-code secrets in DAG code  Never print secrets in task logs (use *args sanitization)  Sanitize Connection IDs in user-facing logs/errors

 Audit DAG code for git history leaks (truffleHog, gitleaks)  Pin operator versions to avoid silent supply-chain changes ■■ Warning: Airflow's metadata DB is a high-value target. A compromised metadata DB exposes every Connection, every Variable, and the encryption keys to your data stack. Treat it like your production Postgres: VPC isolation, audit logging, mandatory MFA for admins, regular access review.

#### 23. Common Anti-Patterns

```
23.1 The Catalog
DAG AUTHORING
  ■ I/O at module level (API calls, DB queries during parse)
  ■ Heavy imports at module level (slow parse)
  ■ Non-idempotent tasks
  ■ Storing large data in XCom
  ■ Hard-coded credentials in DAG code
  ■ Using PythonOperator for heavy compute
  ■ Sensors in poke mode for long waits (use deferrable!)
  ■ Same dag_id reused after schema change (history breaks)
  ■ catchup=True without explicitly wanting backfill
  ■ Naming DAGs inconsistently (no namespacing)
  ■ Skipping doc_md and tags
SCHEDULING
  ■ depends_on_past=True (one bad day blocks future runs forever)
  ■ schedule='@hourly' on a task that takes 90 min (overlapping runs)
  ■ Using execution_date as 'now' (it's the data interval start)
  ■ Mixing time-based and dataset-based schedules without thought
OPERATIONAL
  ■ No on_failure_callback / no alerting
  ■ Logs only on local disk (lost when worker dies)
  ■ Metadata DB never cleaned (unbounded growth)
  ■ Single scheduler (no HA)
  ■ All teams in one Airflow with no namespacing
  ■ No CI tests for DAG imports
  ■ Manual DAG deploys (copy files via SSH)
  ■ "Airflow is slow" treated as inevitable, not investigated
ARCHITECTURE
  ■ Using Airflow for sub-second triggers (use Kafka + service)
  ■ Using Airflow for stream processing (use Flink/Spark Streaming)
  ■ Using Airflow to do the actual data processing in PythonOperator
     (Airflow ORCHESTRATES; the work happens in Spark/dbt/Snowflake)
  ■ One mega-DAG with 500 tasks (split into many smaller DAGs)
  ■ Cross-DAG dependencies via ExternalTaskSensor (use Datasets)
■ Production scenario: A team built an Airflow DAG that processed 100GB of CSV files using PythonOperator and
Pandas — all in the worker process. Worker OOMed daily. They added more memory. Now it took 6 hours. They
added more parallelism. Now the metadata DB struggled. Resolution: replaced PythonOperator with
KubernetesPodOperator running a Spark job. 12 minutes, no OOM. Airflow's job is to schedule the work, not to do
the work.
■ Principal mindset: The biggest mistake teams make with Airflow: they use it as a compute platform instead of an
orchestrator. PythonOperator should orchestrate (extract job IDs, parameterize calls, validate outputs); the heavy
lifting belongs in Spark, dbt, Snowflake, or a containerized service. The thinner the worker, the more resilient the
cluster.
```

## PART VII — THE COMPETITIVE LANDSCAPE

#### 24. Airflow vs Dagster vs Prefect

24.1 The Three Pythonic Orchestrators Aspect Airflow Dagster Prefect Released 2014 2019 2018 Mental model DAG of operators Software-defined assets Flow of tasks Strength Ecosystem, hireability Lineage, types, data-first Modern API, hybrid execution Local dev UX Painful (run scheduler) Best in class Excellent Cloud offering MWAA, Composer, Astro Dagster Cloud Prefect Cloud Data lineage OpenLineage (add-on) First-class Limited Schedule model Cron + Datasets Asset-based Cron + events

```python
24.2 Dagster's Asset-Based Model
DAGSTER inverts the model: declare ASSETS (datasets), not tasks.
@asset
def raw_sales():
    return spark.read.parquet("s3://...")
@asset
def gold_sales(raw_sales):     # dependency inferred from arg
    return transform(raw_sales)
@asset
def sales_dashboard(gold_sales):
    return build_dashboard(gold_sales)
DAGSTER infers:
   Lineage (which assets depend on which)
   Schedule (recompute downstream when upstream changes)
   Tests (per-asset data quality)
ADVANTAGES OVER AIRFLOW:
  ✓ Lineage built-in (no OpenLineage bolt-on)
  ✓ Type-checking between assets
  ✓ Better local dev (asset materialization is testable)
  ✓ Conceptual fit for data-first thinking
DISADVANTAGES:
  ✗ Smaller ecosystem (fewer operators, fewer hires)
  ✗ Steeper learning curve for ops folks used to DAG view
  ✗ Less mature for non-data orchestration (general workflows)
```

24.3 Prefect's Hybrid Execution PREFECT separates ORCHESTRATION from EXECUTION. Prefect Cloud orchestrates (schedules, state, UI)

```
  Your infra executes (workers in your VPC, your K8s, your cloud)
@flow
def sales_etl():
    raw = extract()
    transformed = transform(raw)
    load(transformed)
ADVANTAGES:
  ✓ Better UX than Airflow for many tasks (modern Python, async-first)
  ✓ Hybrid model — control plane managed, data stays in your infra
  ✓ Dynamic flows easier than Airflow's dynamic mapping
  ✓ Strong observability OOTB
DISADVANTAGES:
  ✗ Smaller ecosystem
  ✗ Vendor lock-in for managed Prefect Cloud
  ✗ Self-hosted requires Prefect Server (operational overhead)
```

```
24.4 When to Pick Which
PICK AIRFLOW IF:
   You need the biggest ecosystem (provider packages, integrations)
   Hiring is a constraint (most data engineers know Airflow)
   You're already invested
   You need maturity over modernity
PICK DAGSTER IF:
   Lineage and data quality are first-order concerns
   Local dev velocity matters
   Asset-based thinking fits your team (analytics engineers)
   You're starting fresh
PICK PREFECT IF:
   You want managed orchestration without giving up data control
   Modern Python developer experience is a priority
   You have non-data workflows too (ML, ops)
   You're starting fresh
IN PRACTICE:
   Established teams: 90% stay on Airflow (migration cost too high)
   New teams in 2024-2025: more pick Dagster or Prefect
   Hybrid: some teams use Airflow for ops, Dagster for analytics
■ Principal mindset: The orchestrator wars matter less than people think. All three (Airflow, Dagster, Prefect) work
for 95% of workloads. The differentiator is the data team's mental model: if you think in pipelines (tasks), Airflow is
natural; if you think in datasets (assets), Dagster is natural. Picking based on hiring availability and ecosystem
maturity is usually the right answer for established teams.
```

#### 25. Airflow vs Argo, Step Functions, Temporal

```
25.1 Argo Workflows (Kubernetes-Native)
ARGO = Kubernetes-native workflow engine.
   Workflows defined in YAML (or Argo Python SDK)
   Each step = one or more pods
   Tight K8s integration: events, RBAC, resource limits
STRENGTHS:
  ✓ Native K8s — no separate scheduler infra
  ✓ Excellent for ML pipelines (Kubeflow built on Argo)
  ✓ Better resource model than Airflow on K8s
  ✓ Container-first (no Python wrapping)
WEAKNESSES:
  ✗ Weaker scheduling primitives than Airflow (cron is bolt-on)
  ✗ Worse for SQL/dbt-style data pipelines
  ✗ YAML workflow definition (less expressive than Python)
  ✗ Smaller ecosystem for data sources
PICK ARGO IF:
   Your stack is K8s-native and ML-heavy
   You're already running Argo for other workflows
   You're building Kubeflow
```

```
25.2 AWS Step Functions
STEP FUNCTIONS = serverless workflow on AWS.
   State machine defined in Amazon States Language (JSON)
   Integrates natively with Lambda, ECS, EMR, Glue, etc.
   Serverless: no Airflow infra to operate
STRENGTHS:
  ✓ No infra (AWS manages everything)
  ✓ Tight AWS integration
  ✓ Pay-per-use ($0.025 per 1000 state transitions)
  ✓ Built-in retries, error handling, parallel execution
WEAKNESSES:
  ✗ AWS lock-in
  ✗ Weak for complex Python-driven pipelines
  ✗ Limited observability vs Airflow UI
  ✗ State language is verbose; not Python-friendly
PICK STEP FUNCTIONS IF:
   You're all-in on AWS
   Workflows are simple and event-driven
   You hate operating Airflow
   Costs are dominated by workflow steps, not data
```

```
25.3 Temporal (Stateful Workflows)
TEMPORAL = workflow engine for application-level workflows.
   Workflows in code (Python, Go, Java, TypeScript)
   Durable execution: workflow state persisted, can replay after crash
   Designed for LONG-RUNNING business processes (saga, compensation)
STRENGTHS:
  ✓ Killer for application workflows (order processing, signup)
  ✓ Code-as-config — full programming model
```

```
  ✓ Strong consistency guarantees, replayable
  ✓ Great for sagas, compensating transactions
WEAKNESSES:
  ✗ Not a data orchestrator (no Spark/dbt operators)
  ✗ Cron scheduling secondary
  ✗ Steeper learning curve (durable execution mental model)
  ✗ Smaller data ecosystem
PICK TEMPORAL IF:
   Application workflows (not just data ETL)
   Long-running, stateful business processes
   Saga patterns, distributed transactions
   Polyglot teams (Go/Java/Python all targeting same workflows)
```

25.4 The Selection Matrix Need Best fit Data ETL (SQL, Spark, dbt) Airflow / Dagster ML pipelines on K8s Argo / Kubeflow AWS-native serverless workflows Step Functions Stateful business workflows Temporal Analytics-engineer-friendly Dagster Maximum ecosystem Airflow Real-time stream processing Flink (NOT orchestrator) Sub-second event triggers Kafka + service ■ Interview angle: Q: Your team uses Airflow but is considering Temporal for a new order-processing workflow. Should you? Staff: Yes, almost certainly. Temporal is designed for what you're describing — long-running business processes with stateful steps (reserve inventory, charge card, ship, refund on failure). Airflow can do it but you'll fight the model: it's a batch scheduler at heart. Temporal's saga primitives, retries, compensations are first-class. Different tool, different domain. Keep Airflow for data ETL; pick Temporal for app workflows. The mistake is forcing one orchestrator to do every kind of workflow.

#### 26. When Airflow Is the Wrong Tool

```
26.1 The Anti-Use-Cases
1. SUB-SECOND LATENCY
   Airflow scheduler loops every 5 seconds. Best case latency: 30 seconds.
   For real-time: Kafka + consumer service, Flink, Lambda.
2. STREAM PROCESSING
   Airflow is a BATCH scheduler. Don't use it for continuous streams.
   For streams: Kafka Streams, Flink, Spark Structured Streaming.
3. APPLICATION WORKFLOWS (with strong consistency)
   Airflow is eventually consistent (task state lags reality).
   For app workflows: Temporal, Cadence, AWS Step Functions.
4. HIGH-VOLUME EVENT TRIGGERS (>1000/sec)
   Each event = at least one Airflow task = metadata DB write. Won't scale.
   For high-volume triggers: serverless functions (Lambda, Cloud Functions).
5. SIMPLE CRON ALTERNATIVES
   1 hourly job? Use cron. 5 jobs with no dependencies? Cron.
   Airflow has substantial operational overhead.
6. ML TRAINING ORCHESTRATION (heavy K8s)
   Airflow + KubernetesPodOperator works, but Kubeflow/Argo is purpose-built.
   For ML pipelines on K8s: Kubeflow, Argo Workflows.
7. STATEFUL WORKFLOWS WITH COMPENSATIONS
   Airflow has no native saga support.
   For sagas: Temporal, custom orchestration.
8. LIVE INTERACTIVE WORKFLOWS (human-in-the-loop)
   Airflow's polling model isn't great for "wait for human approval".
   For human-in-the-loop: workflow engines like Camunda, n8n.
```

```
26.2 The Airflow Sweet Spot
WHERE AIRFLOW EXCELS:
  ✓ Scheduled batch data pipelines (hourly to monthly)
  ✓ Mixed-tech orchestration (SQL + Spark + Python + APIs)
  ✓ Dependencies between ETL jobs
  ✓ Backfill / replay workflows
  ✓ Mid-sized teams (10-200 engineers)
  ✓ 100s to 10000s of DAGs
PERFECT FIT EXAMPLE:
   Daily: Extract from 30 SaaS APIs to S3 (PythonOperator)
   Daily: Run 50 dbt models in dependency order (dbt-airflow)
   Daily: Refresh 10 BI extracts (SnowflakeOperator)
   Hourly: Refresh dashboards (HTTPOperator)
   Triggered: ML scoring on new data (KubernetesPodOperator)
   All with: retries, alerting, lineage, SLA, audit trail
■ Principal mindset: The staff-level orchestration interview always tests 'when not to use Airflow'. Showing you
know its boundaries — and naming alternatives credibly — is what differentiates senior from staff. Most teams use
Airflow for too much; auditing which workflows could move off Airflow is a high-leverage exercise for a new staff
engineer.
```

## PART VIII — INTERVIEW PRACTICE

#### 27. The Staff-Level Orchestration Interview

  - 27.1 Question Categories
- 1. CONCEPT — "Explain how the Airflow scheduler works"
- 2. DESIGN  — "Design pipelines for a [scenario]"
- 3. DEBUG   — "Tasks are starting 30 min late; what do you check?"
- 4. SCALE   — "We have 5000 DAGs; what breaks first?"
- 5. TRADE-OFFS — "Airflow vs Dagster vs Argo for this workload"
- 6. OPS     — "How do you do CI/CD, secrets, multi-tenancy?"
- 7. ANTI-USE — "When would you NOT use Airflow?"

```
27.2 The Design Question Framework
STEP 1: CLARIFY
   Frequency: real-time, hourly, daily, weekly?
   Volume: tasks per day? data size?
   SLA: what's the deadline?
   Dependencies: between DAGs? cross-team?
   Idempotency: can we re-run safely?
   Failure handling: retry? alert? skip?
STEP 2: SKETCH
   DAG boundaries (one purpose per DAG)
   Task granularity (orchestrate, don't compute)
   Operator choices (KubernetesPod for heavy, native for SQL)
   Dependencies (within DAG, across DAGs via Datasets)
STEP 3: RELIABILITY
   Retries with appropriate backoff
   Idempotency for every task
   Alerting on failure / SLA miss
   Monitoring / dashboards
STEP 4: OPERATIONS
   Schedule strategy (catchup, max_active_runs)
   Pools / queues for resource control
   Secrets via external backend
   Tags / docs for discoverability
STEP 5: SCALE / TRADE-OFFS
   What's the bottleneck at 10×?
   What would we do differently at 100×?
   When would we migrate to a different tool?
```

27.3 Worked Example: Daily ETL for 500 Tables INTERVIEWER: "Design Airflow for daily ingestion of 500 source tables into our warehouse. Volume: 100K-100M rows each. SLA: 6am UTC." STAFF RESPONSE: "First clarifying questions: are tables independent or with dependencies? Same source or many? Schema changes frequent? Allowed re-extraction or strict incremental? Assuming: 500 tables across 10 source systems, mostly independent,

```sql
 incremental loads where possible.
 STRUCTURE:
    One DAG per SOURCE SYSTEM (10 DAGs), each containing tasks for
     its tables. Why: blast radius (one source outage doesn't fail others),
     team ownership often by source.
    Within each DAG, dynamic task mapping over the table list — pulled
     from a config table or YAML at run time.
    Each table task is a KubernetesPodOperator running our standard
     ingestion container (parameterized by table name).
    Idempotency: each task writes to a date-partitioned path; safe to re-run.
 SCHEDULE:
    All 10 DAGs scheduled at 02:00 UTC, catchup=False.
    max_active_runs=1 per DAG.
    Pools: warehouse_load (limit to warehouse concurrency),
     api_calls (limit to API rate).
 RELIABILITY:
    Retries: 3 with exponential backoff, 5min initial.
    on_failure_callback: Slack alert to source-owning team.
    SLA: 4 hours from schedule (give buffer for 6am deadline).
 OBSERVABILITY:
    Each task emits row count, duration, partition path as XCom.
    Tag DAGs with source name + tier.
    OpenLineage integration to feed lineage to catalog.
 SCALE:
    500 tasks/day is comfortable.
    At 5000 tables: split into more DAGs, consider per-tier scheduling.
    At 50000 tables: Airflow becomes a router/scheduler;
     actual loads handled by a separate ingestion service.
 NOT AIRFLOW IF:
    CDC (continuous capture) — use Debezium + Kafka instead.
    Sub-minute freshness — Airflow's 5s scheduler loop is the floor."
WHAT MAKES THIS A STAFF ANSWER:
   Started with clarifying questions
   Considered blast radius / team ownership
   Picked operators based on workload
   Discussed scale boundaries
   Mentioned when NOT to use Airflow
```

27.4 The Red Flags / Green Flags GREEN FLAGS RED FLAGS ■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■ "Depends on the workload" "Always do X" Discusses idempotency unprompted Doesn't mention idempotency Mentions KubernetesPodOperator "PythonOperator for everything" Knows Datasets / deferrable Talks like it's Airflow 1.x "Airflow orchestrates; X computes" Treats Airflow as compute platform Mentions metadata DB tuning Treats Airflow as black box Knows the alternatives Religious about Airflow "Catchup=False by default" No mention of catchup Brings up alerting / on-call Talks only about happy path

```
28. War Stories
■ Production scenario: The catchup=True Apocalypse. A team deployed a new DAG with
start_date=datetime(2020, 1, 1) and the default catchup=True. Airflow immediately scheduled 1500
historical runs. The DAG hit Snowflake, which queued every query. The warehouse autoscaled. Bill: $40K for that
one Sunday. Always set catchup=False by default; enable explicit backfill.
■ Production scenario: The Parse-Time API Call. A senior engineer wrote a DAG that fetched a list of tables from
an API at module level — to dynamically create one task per table. The scheduler parses every DAG every 30
seconds. The API got 10K requests/minute. Outage at the API vendor. Fix: move the discovery into a task with
dynamic mapping. Parse-time code is not the right place for I/O.
■ Production scenario: The Mystery Slowdown. Task scheduling latency went from 10s to 4 min over six months.
Cause: metadata DB had 50M task_instance rows, autovacuum couldn't keep up, queries crawled. Added retention
policy (airflow db clean 90 days), tuned Postgres autovacuum, scheduling returned to normal. Operational
hygiene at scale matters more than 'clever' DAGs.
■ Production scenario: The XCom Bomb. A task returned a 100MB DataFrame via XCom. The downstream task
pulled it. Both tasks worked in dev. In prod with 200 concurrent runs, the metadata DB write-buffer filled,
autovacuum stalled, the whole cluster ground to a halt. Fix: write the DataFrame to S3, pass the path via XCom.
XComs are for messages, not payloads.
■ Production scenario: The Sensor Storm. 800 DAGs each had an ExternalTaskSensor in poke mode waiting for
an upstream DAG. Workers had 100 slots total. 800 sensors > 100 slots → constant queuing, scheduling lag,
sensors timed out. Fix: switched to deferrable sensors. One triggerer process handled all 800 waits via asyncio.
Worker slots freed up entirely. Job latency dropped 80%. Deferrable was the largest single optimization the team
made that year.
■ Production scenario: The Silent Data Loss. Upstream Kafka producer renamed a field. The downstream Airflow
DAG used get('old_field', 0) and silently filled zeros. No alert, no failure. Two weeks of bad metrics before
BI noticed. Fix: schema validation as a first task in every ingestion DAG; alert on any null fraction > baseline. The
orchestrator can't catch data quality issues unless you tell it to.
```

#### 29. Cheat Sheet & Mental Models

  - 29.1 The Six Mental Models
- 1. AIRFLOW ORCHESTRATES, X COMPUTES
- Workers should be thin. Spark/dbt/Snowflake/K8s do the heavy lifting.
- 2. DAGs ARE PARSED EVERY ~30 SECONDS
- No I/O at module level. Imports lazy. Discover at run-time.
- 3. DATA INTERVALS, NOT WALL-CLOCK TIMES
- A @daily DAG for Jan 1 runs at Jan 2 00:00.
- {{ ds }} = data_interval_start.
- 4. EVERY TASK MUST BE IDEMPOTENT
- Re-running produces same final state. Always.
- 5. THE METADATA DB IS THE SCALING BOTTLENECK
- Treat it as production Postgres. Tune. Monitor. Clean.
- 6. CATCHUP=FALSE BY DEFAULT
- Explicit backfill via CLI when needed.

```
29.2 The One-Page Reference
DAG SKELETON
  with DAG(
      dag_id='team__pipeline_name',
      schedule='0 2 * * *',
      start_date=datetime(2024, 1, 1),
      catchup=False,
      max_active_runs=1,
      default_args={'owner': 'team', 'retries': 3, ...},
      tags=['team', 'tier-1'],
      doc_md=DOC_MD,
      on_failure_callback=slack_alert,
  ) as dag:
      ...
KEY OPERATORS
  KubernetesPodOperator   — heavy custom code (Spark, ML, Python)
  SnowflakeOperator       — SQL on Snowflake (and other warehouses)
  S3KeySensor             — wait for file (deferrable=True!)
  BashOperator            — quick shell commands
  PythonOperator          — light orchestration only
  EmptyOperator           — visual placeholders, start/end markers
DATASETS (Airflow 2.4+)
  ds = Dataset('s3://bucket/path/')
  @task(outlets=[ds])               # producer side
  schedule=[ds]                      # consumer side
DYNAMIC MAPPING
  task.expand(arg=list_of_values)    # one task per value
  task.partial(fixed=x).expand(...)  # fix some args
POOLS / QUEUES
  task = MyOp(pool='snowflake_xs', pool_slots=2, queue='heavy')
SCHEDULING METADATA
  {{ ds }}                  data_interval_start (YYYY-MM-DD)
  {{ data_interval_start }} same as ds but full datetime
  {{ data_interval_end }}   end of interval
  {{ ts }}                  ISO timestamp of logical_date
```

```
  {{ dag.dag_id }}          DAG ID
  {{ task.task_id }}        Task ID
```

  - 29.3 The Tuning Order
- 1. SET catchup=False ON EVERY DAG
- 2. CONVERT SENSORS TO DEFERRABLE
- 3. MOVE HEAVY COMPUTE OUT OF PYTHONOPERATOR
- 4. SHIP LOGS TO S3 / GCS
- 5. ENABLE METADATA DB MONITORING
- 6. ADD POOLS FOR EXTERNAL RESOURCE LIMITS
- 7. RUN MULTIPLE SCHEDULERS (HA)
- 8. ENABLE DAG IMPORT TESTS IN CI
- 9. CONFIGURE EXTERNAL SECRETS BACKEND
- 10. INTEGRATE OPENLINEAGE

```
29.4 What Separates Senior from Staff
SENIOR                                STAFF / PRINCIPAL
■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■
"Use Airflow"                        "Airflow for daily batch; Flink for
                                      streaming; Temporal for app workflows;
                                      Step Functions for AWS-native simple"
"Add retries"                        "Retries=3 with exponential backoff
                                      and jitter; retries=0 for deterministic
                                      failures; SLA callback for tier-1;
                                      idempotency is the prerequisite"
"Use sensors"                        "Deferrable sensors via triggerer;
                                      Datasets for cross-DAG; ExternalTask-
                                      Sensor is a code smell we should retire"
"PythonOperator works"               "PythonOperator for orchestration only;
                                      KubernetesPodOperator for any work that
                                      could OOM a worker or block neighbors"
"Airflow is the orchestrator"         "Airflow is one orchestrator; we use
                                      Argo for ML on K8s, Temporal for order
                                      workflows; this workload fits Airflow
                                      because [batch + dependencies + mixed
                                      tech + SLA tolerance]"
■ Principal mindset: Airflow is widely used and widely misused. Staff-level orchestration is about knowing what
belongs in the orchestrator vs the compute layer, when to push back on 'Airflow for everything', and how to operate it
at scale. The best staff engineers I've worked with don't add features to Airflow; they remove anti-patterns from it.
 — End of Airflow Notes —
 Next in series: Kafka & Streaming, dbt, Cloud Data Platforms
```


---

[← Back to main](./README.md)
