# Data Engineering — Principal / Staff Engineer Notes

> A ~570-page architect-level reference for principal and staff data engineering interviews, covering the full modern data stack plus behavioral prep, FAANG-style frameworks, ML for data engineers, and the state of the stack through 2026.

## About

These notes were authored as a comprehensive reference for data engineers preparing for principal or staff-level interviews. They cover both the technical depth (SQL through system design synthesis) and the non-technical signals that determine outcomes at this level (behavioral storytelling, cross-topic synthesis, interview operations).

The content assumes 5-8+ years of data engineering experience. If terms like CDC, idempotency, or micro-partitions are new, start with intermediate resources first.

## Table of Contents

| # | File | Pages | What it covers |
|---|------|-------|----------------|
| 0 | [00-front-matter.md](./00-front-matter.md) | 34 | Study paths, behavioral interview framework, 8 story types, STAR+ framework for staff, cross-topic connection map, final interview playbook, salary negotiation |
| 1 | [01-sql.md](./01-sql.md) | 57 | Window functions, query plans, joins, indexing, the analyst-level depth tested at staff+ |
| 2 | [02-pyspark.md](./02-pyspark.md) | 58 | Distributed compute, shuffles, joins, AQE, performance tuning |
| 3 | [03-airflow.md](./03-airflow.md) | 58 | DAGs, sensors, deferrable operators, dynamic DAGs, alternatives |
| 4 | [04-kafka-streaming.md](./04-kafka-streaming.md) | 60 | Partitioning, ordering, exactly-once, Schema Registry, KRaft |
| 5 | [05-dbt-modeling.md](./05-dbt-modeling.md) | 66 | Layered modeling, Kimball vs Vault vs OBT, semantic layer, dbt mesh |
| 6 | [06-lakehouses.md](./06-lakehouses.md) | 58 | Delta vs Iceberg vs Hudi, catalogs, CDC, format wars |
| 7 | [07-cloud-warehouses.md](./07-cloud-warehouses.md) | 60 | Snowflake, BigQuery, Redshift, Databricks SQL deep dive |
| 8 | [08-system-design.md](./08-system-design.md) | 66 | Lambda vs Kappa, medallion architecture, reference architectures, 5 worked design interviews |
| 9 | [09-supplementary.md](./09-supplementary.md) | 53 | Coding patterns at staff+, FAANG behavioral styles, ML for data engineers, state of the stack 2024-2026 |

**Total: ~570 pages**

## Study Paths by Interview Type

**Pure platform / infrastructure role**
Priority: System Design → Kafka → Lakehouses → Warehouses
Focus: cross-topic synthesis, cost engineering, multi-region

**Analytics engineering / dbt-centric role**
Priority: SQL → dbt → Warehouses → System Design
Focus: modeling philosophy, semantic layer, data quality

**Streaming / real-time role**
Priority: Kafka → System Design → Lakehouses → PySpark
Focus: CDC, idempotency, end-to-end latency budgets, exactly-once

**Lakehouse / data platform role**
Priority: Lakehouses → System Design → PySpark → Warehouses
Focus: open formats, catalog strategy, format wars, multi-engine

**Generalist staff role**
Read all 8 topics + front matter + supplementary. Budget 40-80 hours.

## 4-Week Study Plan

**Week 1 — Technical depth (bottom layers)**
Days 1-2 SQL, 3-4 PySpark, 5-6 Airflow, 7 self-test

**Week 2 — Data flow (middle layers)**
Days 1-3 Kafka, 4-5 dbt, 6-7 practice CDC end-to-end verbally

**Week 3 — Storage + compute (top layers)**
Days 1-3 Lakehouses, 4-5 Warehouses, 6-7 practice warehouse-vs-lakehouse arguments out loud

**Week 4 — Synthesis**
Days 1-3 System Design (all 5 worked interviews), 4 behavioral (write 6 STAR stories), 5 cross-topic connection map, 6 interview playbook + mock, 7 rest

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

## Format Notes

This markdown version was converted from the source PDF. Code blocks, callouts, and prose paragraphs are preserved. For precise table layouts and visual callout boxes, see the original PDF version.

## License

MIT License — use freely. See [LICENSE](./LICENSE).

## A Final Thought

The data engineering field moves fast. By 2027, parts of this document will be outdated — specific tools will shift, vendors will consolidate, new patterns will emerge. What stays true: the **frameworks for thinking** — capacity math, trade-off analysis, requirements gathering, cost engineering. Tools change; judgment compounds.

Good luck.
