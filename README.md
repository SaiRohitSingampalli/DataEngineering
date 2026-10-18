# Data Engineering — Principal / Staff Engineer Notes

> A ~1,500-page architect-level reference for principal and staff data engineering interviews, covering the full modern data stack plus standalone deep-dives on open table formats, streaming engines, query engines, DBA operations, and all three major clouds.

## About

These notes were authored as a comprehensive reference for data engineers preparing for principal or staff-level interviews. They cover both the technical depth (SQL through system design synthesis) and the non-technical signals that determine outcomes at this level (behavioral storytelling, cross-topic synthesis, interview operations).

The content assumes 5-8+ years of data engineering experience. If terms like CDC, idempotency, or micro-partitions are new, start with intermediate resources first.

## Table of Contents

### Core Series (foundational topics)

| # | File | Pages | What it covers |
|---|------|-------|----------------|
| 0 | [00-front-matter.md](./00-front-matter.md) | 34 | Study paths, behavioral interview framework, 8 story types, STAR+ framework for staff, cross-topic connection map, final interview playbook, salary negotiation |
| 1 | [01-sql.md](./01-sql.md) | 57 | SQL from the software engineering lens: window functions, query plans, joins, indexing, internals (PostgreSQL-flavored) |
| 2 | [02-pyspark.md](./02-pyspark.md) | 58 | Distributed compute, shuffles, joins, AQE, performance tuning |
| 3 | [03-airflow.md](./03-airflow.md) | 58 | DAGs, sensors, deferrable operators, dynamic DAGs, alternatives |
| 4 | [04-kafka-streaming.md](./04-kafka-streaming.md) | 60 | Partitioning, ordering, exactly-once, Schema Registry, KRaft |
| 5 | [05-dbt-modeling.md](./05-dbt-modeling.md) | 66 | Layered modeling, Kimball vs Vault vs OBT, semantic layer, dbt mesh |
| 6 | [06-lakehouses.md](./06-lakehouses.md) | 58 | Delta vs Iceberg vs Hudi overview, catalogs, CDC, format wars |
| 7 | [07-cloud-warehouses.md](./07-cloud-warehouses.md) | 60 | Snowflake, BigQuery, Redshift, Databricks SQL deep dive |
| 8 | [08-system-design.md](./08-system-design.md) | 66 | Lambda vs Kappa, medallion architecture, reference architectures, 5 worked design interviews |
| 9 | [09-supplementary.md](./09-supplementary.md) | 53 | Coding patterns at staff+, FAANG behavioral styles, ML for data engineers, state of the stack 2024-2026 |

### Standalone Deep Dives — Technologies

| # | File | Pages | What it covers |
|---|------|-------|----------------|
| 10 | [10-duckdb.md](./10-duckdb.md) | ~90 | DuckDB architecture, vectorized execution, morsel-driven parallelism, DuckLake, MotherDuck |
| 11 | [11-iceberg.md](./11-iceberg.md) | ~90 | Apache Iceberg three-layer metadata, hidden partitioning, V2/V3 deletes, catalogs, multi-engine interop |
| 12 | [12-delta-lake.md](./12-delta-lake.md) | ~85 | Delta Lake transaction log, deletion vectors, Liquid Clustering, UniForm, Delta Sharing |
| 13 | [13-hudi.md](./13-hudi.md) | ~90 | Apache Hudi CoW/MoR, record-level indexes, multi-modal indexing, HudiStreamer, timeline |
| 14 | [14-flink.md](./14-flink.md) | ~90 | Apache Flink streaming engine, Chandy-Lamport checkpoints, event-time + watermarks, state backends |
| 15 | [15-trino.md](./15-trino.md) | ~85 | Trino distributed SQL, connectors, dynamic filtering, CBO, federation patterns, Iceberg lakehouse stack |
| 16 | [16-sql-dba-reference.md](./16-sql-dba-reference.md) | ~90 | SQL &amp; DBA ops across MS SQL / PostgreSQL / MySQL / Oracle: vendor syntax, indexes, isolation, backups, HA |

### Standalone Deep Dives — Clouds

| # | File | Pages | What it covers |
|---|------|-------|----------------|
| 17 | [17-aws-data-engineering.md](./17-aws-data-engineering.md) | ~90 | AWS: S3, Glue, EMR, Athena, Redshift, Kinesis, MSK, Lake Formation, Step Functions, SageMaker |
| 18 | [18-azure-data-engineering.md](./18-azure-data-engineering.md) | ~90 | Azure: Microsoft Fabric, ADLS Gen2, Azure Databricks, Synapse, Event Hubs, Data Factory, Purview, Power BI |
| 19 | [19-gcp-data-engineering.md](./19-gcp-data-engineering.md) | ~90 | GCP: BigQuery, Dataflow, Pub/Sub, Datastream, Dataproc, Dataform, Looker, Vertex AI, BigLake |

### Standalone Deep Dives — Foundations &amp; Platforms

| # | File | Pages | What it covers |
|---|------|-------|----------------|
| 20 | [20-row-vs-columnar-storage.md](./20-row-vs-columnar-storage.md) | ~85 | Row vs columnar storage: physical layout, compression, vectorized execution, Volcano vs vectorized, Parquet/ORC/Arrow, OLTP vs OLAP vs HTAP, interview-ready mental models |
| 21 | [21-snowflake-platform.md](./21-snowflake-platform.md) | ~90 | Snowflake Data Cloud: micro-partitions, clustering, Snowpipe Streaming, Dynamic Tables, Snowpark, Cortex AI, Hybrid Tables, Managed Iceberg, Horizon governance, cost patterns |
| 22 | [22-databricks-platform.md](./22-databricks-platform.md) | ~90 | Databricks Data Intelligence Platform: Unity Catalog, DLT, Workflows, Mosaic AI (Model Serving, Vector Search, Agent Framework), Lakeflow Connect, Photon, cost patterns |

**Total: ~1,750 pages across 23 files**

### Two SQL chapters — how they complement

- **[01-sql.md](./01-sql.md)** — SQL from the **software engineering lens**. PostgreSQL-focused, deep on internals (optimizer, MVCC, EXPLAIN, index structures).
- **[16-sql-dba-reference.md](./16-sql-dba-reference.md)** — SQL from the **DBA / operational lens**. Cross-vendor (MS SQL + PostgreSQL + MySQL + Oracle), oriented toward operating production databases.

### Cloud chapters — how they complement

The three cloud chapters (17-19) are **structurally parallel** so you can compare directly:
- Same 8-part structure (foundations, storage, compute, ingestion, analytics, orchestration, ML/governance, interview practice)
- Same cross-cloud comparison tables
- Same reference architecture patterns, re-expressed per cloud

Read all three before any multi-cloud interview. Read your target cloud deeply if focused.

## Study Paths by Interview Type

**Pure platform / infrastructure role**
Priority: System Design → Kafka → Flink → Trino → Lakehouses → Iceberg → one cloud deep
Focus: cross-topic synthesis, cost engineering, multi-region, open architectures

**Analytics engineering / dbt-centric role**
Priority: SQL (01) → dbt → Warehouses → System Design → DuckDB → one cloud (likely GCP or Azure)
Focus: modeling philosophy, semantic layer, data quality, dbt-duckdb

**Streaming / real-time role**
Priority: Kafka → Flink → System Design → Hudi → Lakehouses → one cloud (AWS or Azure)
Focus: CDC, idempotency, end-to-end latency budgets, exactly-once, streaming lakehouse

**Lakehouse / data platform role**
Priority: Lakehouses → Iceberg → System Design → Trino → DuckDB → Delta/Hudi → one cloud
Focus: open formats, catalog strategy, format wars, multi-engine

**Databricks-centric role**
Priority: Databricks Platform (22) → Delta Lake (12) → PySpark → Databricks SQL → System Design → Azure (Databricks on Azure)
Focus: Unity Catalog, DLT, Mosaic AI, Delta features, Spark + ML, UniForm

**Snowflake-centric role**
Priority: Snowflake Platform (21) → Warehouses (7) → SQL (01) → System Design → Row vs Columnar (20)
Focus: micro-partitions, Dynamic Tables, Snowpark, Cortex, Horizon governance, Managed Iceberg

**Storage internals / performance tuning role**
Priority: Row vs Columnar (20) → Iceberg (11) or Delta (12) → PySpark (02) → DuckDB (10) → Trino (15)
Focus: columnar execution, vectorization, pruning, file formats, Parquet/ORC internals

**AWS-centric data engineering role**
Priority: AWS (17) → S3/Iceberg (11) → Warehouses (7, Redshift section) → System Design → Lakehouses
Focus: AWS service composition, Lake Formation, Athena, DMS, EMR

**Azure-centric data engineering role**
Priority: Azure (18) → Databricks → Delta Lake (12) → Fabric → System Design
Focus: Fabric, OneLake, Power BI DirectLake, Databricks on Azure

**GCP-centric data engineering role**
Priority: GCP (19) → BigQuery (in 7) → Dataflow → System Design → Vertex AI
Focus: BigQuery editions, serverless patterns, Looker, BigLake

**DBA / operational DB role**
Priority: SQL (16) → SQL (01) → System Design → Lakehouses → Warehouses
Focus: vendor comparisons, backup/HA, isolation levels, routine ops

**Generalist staff role**
Read all 20 files. Budget 110-150 hours.

## 4-Week Study Plan

**Week 1 — Technical depth (bottom layers)**
Days 1-2 SQL (01 + skim 16), 3-4 PySpark, 5-6 Airflow, 7 self-test

**Week 2 — Streaming + modeling**
Days 1-2 Kafka, 3 Flink deep dive, 4-5 dbt, 6-7 practice CDC + exactly-once verbally

**Week 3 — Storage + compute + cloud**
Days 1 Lakehouses overview, 2-3 Iceberg + Delta + Hudi (pick most relevant 2),
4-5 Warehouses + DuckDB, 6 Trino, 7 your target cloud chapter

**Week 4 — Synthesis + behavioral**
Days 1-3 System Design (all 5 worked interviews), 4 behavioral (write 6 STAR stories),
5 cross-topic connection map + state of stack, 6 interview playbook + mock, 7 rest

## Characteristic callouts

Throughout the notes you'll see:

- **🎯 Interview Angle** — common question types + how to answer at staff level
- **🔥 Production Scenario** — real-world war stories
- **🧠 Principal Mindset** — the thinking patterns that distinguish staff+ from senior
- **⚠️ Warning** — common pitfalls
- **💡 Tip** — practical shortcuts
- **📝 Note** — important context

## Reading Tips

1. **Don't try cover-to-cover in a week.** Pick a study path above.
2. **Read out loud for behavioral + synthesis.** The gap between knowing and articulating is where most candidates lose points.
3. **Each topic has a cheat sheet at the end.** Review those the week of the interview.
4. **The senior-vs-staff tables are gold.** They tell you what level the interviewer is measuring.
5. **Cross-cloud reading pays off.** Even if your target role is one cloud, understanding how the others solve the same problems makes you sharper on trade-offs.

## Format Notes

This markdown version was converted from the source PDF, with the standalone deep-dives (10-19) added natively in markdown. Code blocks, callouts, and prose paragraphs are preserved.

## License

MIT License for the core series (files 00-15, 17-22). See [LICENSE](./LICENSE).

The DBA reference chapter (`16-sql-dba-reference.md`) is licensed under CC BY 4.0 as noted in that file.

## A Final Thought

The data engineering field moves fast. By 2027, parts of this document will be outdated — specific tools will shift, vendors will consolidate, new patterns will emerge. What stays true: the **frameworks for thinking** — capacity math, trade-off analysis, requirements gathering, cost engineering. Tools change; judgment compounds.

Good luck.
