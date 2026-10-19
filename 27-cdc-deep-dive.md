# Topic 27 — CDC Deep Dive

> Standalone deep-dive · Change Data Capture internals, patterns, and operations

[← Back to main](./README.md)

---

## How to use this document

CDC appears across many chapters (Kafka, Lakehouses, Clouds). This chapter is the deep dive: how log-based CDC actually works, the snapshot problem, exactly-once semantics, Debezium internals, and the practical war stories. CDC is where data engineering meets database engineering — a classic staff-level signal.

> **🧠 Principal Mindset:** CDC looks simple in architecture diagrams. In practice, 90% of production issues come from CDC: schema evolution, snapshot interleaving, idempotency, lag amplification, and reorg events. Mastering CDC is one of the clearest staff signals.

## Table of Contents

**PART I — FOUNDATIONS**
- 1. What CDC Is
- 2. Why CDC Over Full Snapshots
- 3. The Three CDC Patterns
- 4. CDC Guarantees (and Limits)

**PART II — LOG-BASED CDC INTERNALS**
- 5. How Database WALs Work
- 6. Postgres Logical Replication
- 7. MySQL Binary Log
- 8. Oracle LogMiner and GoldenGate
- 9. SQL Server Change Tracking vs CDC
- 10. MongoDB Change Streams

**PART III — DEBEZIUM**
- 11. Debezium Architecture
- 12. Connector Lifecycle
- 13. Snapshot Modes
- 14. Transformations and Routing

**PART IV — SNAPSHOT PATTERNS**
- 15. The Snapshot Problem
- 16. Netflix DBLog (Watermarking)
- 17. Incremental Snapshots
- 18. Backfill Strategies

**PART V — EXACTLY-ONCE CDC**
- 19. Idempotency in CDC
- 20. Transactional Boundaries
- 21. CDC into Kafka Transactions

**PART VI — MANAGED CDC**
- 22. AWS DMS, Datastream, Fivetran, Airbyte
- 23. Lakeflow Connect, Debezium Server
- 24. Build vs Buy

**PART VII — OPERATIONS**
- 25. Monitoring and SLAs
- 26. Schema Evolution
- 27. Common Failure Modes

**PART VIII — INTERVIEW PRACTICE**
- 28. The Staff-Level CDC Interview
- 29. War Stories
- 30. Cheat Sheet &amp; Mental Models

---

## PART I — FOUNDATIONS

### 1. What CDC Is

```
CHANGE DATA CAPTURE = emit a change event for every INSERT / UPDATE / DELETE
   on a source database, in order, with full before/after state.

EVENT SCHEMA (typical):
   {
     "op": "u",                 // c=create, u=update, d=delete, r=read (snapshot)
     "ts_ms": 1697740800000,
     "source": {
       "db": "orders_db",
       "table": "orders",
       "pos": "0001234/56",     // position in source log (LSN, binlog offset, SCN, etc.)
       "txId": 98765            // source transaction id
     },
     "before": { "id": 1, "status": "pending", "amount": 100 },
     "after":  { "id": 1, "status": "shipped", "amount": 100 }
   }

OUTPUT: typically Kafka topic (one per table), or direct to warehouse / lake.
```

### 2. Why CDC Over Full Snapshots

```
FULL SNAPSHOTS (periodic SELECT * FROM table):
   ✗ Load on source DB
   ✗ Snapshot captures a point in time; deletes between snapshots are lost
   ✗ High latency (minutes to hours)
   ✗ Doesn't scale with table size

CDC:
   ✓ Low load (reads from DB log, not table)
   ✓ Every change captured; no gaps
   ✓ Low latency (seconds)
   ✓ Scales independently of table size

TRADE-OFF: CDC is harder operationally. Snapshots are simple.
```

### 3. The Three CDC Patterns

```
TRIGGER-BASED:
   Triggers on source tables write to shadow/audit tables
   Pros: works on any DB; application-level control
   Cons: Load on source; schema-coupled; often legacy

QUERY-BASED (polling):
   SELECT * WHERE updated_at > last_run
   Pros: no DB-side changes
   Cons: Misses deletes (!!), updates to updated_at required, race conditions

LOG-BASED (modern standard):
   Reads the DB's transaction log directly
   Pros: Captures all changes including deletes; low load; exact ordering
   Cons: DB-specific; needs replication slot or similar

STAFF SIGNAL: query-based CDC is a RED FLAG in interviews because it
   misses deletes. Log-based is the right answer.
```

### 4. CDC Guarantees (and Limits)

```
TYPICAL GUARANTEES:
   ORDER PRESERVED per primary key (not globally)
   AT-LEAST-ONCE delivery (dupes possible on failures)
   EVENTUAL consistency with the source
   
TYPICAL LIMITS:
   NOT STRICTLY TRANSACTIONAL (writing to Kafka ≠ source commit)
   SCHEMA EVOLUTION requires coordination
   TRUNCATE / DROP / RENAME are tricky (varies by connector)
   DDL events may need separate handling

STAFF SIGNAL: understand that CDC event streams reflect source state
   eventually — not that they're perfectly synchronous. Downstream
   consumers must handle out-of-order, duplicate, and late events.
```

---

## PART II — LOG-BASED CDC INTERNALS

### 5. How Database WALs Work

```
EVERY RELATIONAL DB has a write-ahead log (WAL / transaction log):
   • Durability: writes go to log first, then to data files
   • Replication: standby replicas replay the log
   • Recovery: on crash, log replay restores committed state
   
CDC piggybacks on this mechanism — reads the SAME log that replicas read.

Database-specific names:
   PostgreSQL: WAL + logical decoding
   MySQL: binary log (binlog) with row-based format
   SQL Server: transaction log + CDC/CT features
   Oracle: redo log + LogMiner / XStream / GoldenGate
   MongoDB: oplog (operation log)
   DynamoDB: streams (via DynamoDB Streams or Kinesis Data Streams)
```

### 6. Postgres Logical Replication

```
POSTGRES CDC uses LOGICAL REPLICATION:
   
   1. Enable: wal_level=logical in postgresql.conf
   2. Create replication user: 
      CREATE ROLE cdc WITH REPLICATION LOGIN PASSWORD '...';
   3. Create a publication (which tables to replicate):
      CREATE PUBLICATION my_pub FOR TABLE orders, users, items;
   4. Create a replication slot (per consumer):
      SELECT pg_create_logical_replication_slot('cdc_slot', 'pgoutput');
   5. CDC connector reads the slot via streaming replication protocol
   6. Each change decoded + emitted as event

PITFALLS:
   • Replication slots PREVENT WAL CLEANUP → if consumer lags, disk fills
   • Toast values (large columns stored out-of-row) may be absent on UPDATE
   • Primary key required for logical replication (or REPLICA IDENTITY FULL)
   • DDL changes NOT in logical replication; must handle separately
```

> **⚠️ Warning:** The #1 Postgres CDC incident is a slow consumer causing WAL to accumulate and fill the disk, taking down the database. Always alert on `pg_wal_lsn_diff(pg_current_wal_lsn(), restart_lsn) > N GB`.

### 7. MySQL Binary Log

```
MYSQL CDC uses the BINARY LOG (binlog):
   
   1. Enable binlog: 
      [mysqld]
      log_bin = /var/lib/mysql/mysql-bin.log
      binlog_format = ROW
      binlog_row_image = FULL     # include before + after images
   2. Create user with REPLICATION SLAVE / REPLICATION CLIENT
   3. CDC connector connects as a slave; reads binlog
   4. Each row event decoded

FORMATS:
   STATEMENT: logs SQL; less reliable for CDC (nondeterministic queries)
   ROW: logs each row change; THE RIGHT choice for CDC
   MIXED: switches between statement and row

GTIDs (Global Transaction IDs):
   Enable: gtid_mode=ON, enforce_gtid_consistency=ON
   Benefits: durable offset tracking; failover resilience
   CDC uses GTIDs as position markers

PITFALLS:
   • Binlog retention: default 7 days; must retain long enough for backfills
   • Replica lag indicators: Seconds_Behind_Master
   • AUTO_INCREMENT UPDATE events can be noisy
```

### 8. Oracle LogMiner and GoldenGate

```
ORACLE CDC options:
   
   LOGMINER:
      Free; built into Oracle
      Reads redo logs
      Slower; not suitable for high-throughput CDC
      Debezium Oracle connector uses this
   
   XSTREAM (Oracle Streams):
      Paid add-on; faster
      Deprecated in newer Oracle versions
   
   ORACLE GOLDENGATE:
      Paid product; the enterprise standard
      Very low latency; feature-rich (filtering, transformation)
      Pricey; often tolerated in Oracle shops

   DEBEZIUM ORACLE:
      Uses LogMiner by default; OpenLogReplicator (community) for faster
      
Oracle CDC is notoriously painful due to the ecosystem's complexity.
```

### 9. SQL Server Change Tracking vs CDC

```
SQL SERVER has TWO features that sound similar:
   
   CHANGE TRACKING (CT):
      Tracks WHICH rows changed, not WHAT changed
      Lightweight; good for sync use cases
      Returns a boolean "changed" flag + current row
   
   CHANGE DATA CAPTURE (CDC):
      Full before/after; separate CDC tables maintained
      Reads from transaction log
      Richer but heavier

DEBEZIUM SQL SERVER:
   Uses the CDC feature (not CT)
   Requires SQL Server Developer / Enterprise / Standard with CDC enabled

PITFALLS:
   • CT and CDC are OFTEN CONFUSED; ensure correct one is enabled
   • CDC retention: default 3 days; configure
   • AlwaysOn Availability Groups: CDC must be on primary
```

### 10. MongoDB Change Streams

```
MONGODB uses CHANGE STREAMS (built on oplog):
   
   db.collection.watch() returns a cursor emitting change events
   Supports resumeTokens for position tracking
   Available for replica sets + sharded clusters

Event shape:
   { "operationType": "insert|update|delete|replace|invalidate",
     "ns": {"db": "mydb", "coll": "orders"},
     "documentKey": {"_id": ObjectId("...")},
     "fullDocument": {...},            // post-image (requires setting)
     "fullDocumentBeforeChange": {...}  // pre-image (6.0+, requires setting)
   }

Debezium MongoDB connector or native drivers can consume change streams.
```

---

## PART III — DEBEZIUM

### 11. Debezium Architecture

```
DEBEZIUM = open-source CDC platform.

TRADITIONAL ARCHITECTURE (Kafka Connect):
   Kafka Connect cluster
   Debezium source connector per DB (Postgres, MySQL, SQL Server, Oracle, Mongo...)
   Each connector:
      - Connects to source DB log
      - Decodes + schematizes events
      - Writes to Kafka topics (one per table typically)
   
NEWER: DEBEZIUM SERVER (Operator) / DEBEZIUM ENGINE:
   Embedded (library) or standalone server
   Writes directly to sinks (Kafka, Pulsar, Kinesis, Pub/Sub, HTTP)
   Reduces Kafka Connect operational overhead

DEBEZIUM UI (2024+): web UI for managing connectors

OFFSET MANAGEMENT:
   Kafka Connect stores offsets in a Kafka topic
   Debezium Engine stores in files or external store
```

### 12. Connector Lifecycle

```
When a Debezium connector STARTS:
   
   1. Reads offset from last run (if any)
   2. SNAPSHOT (if no offset or configured):
      Takes consistent snapshot of configured tables
      Emits "r" (read) events for every row
   3. STREAMING:
      Starts reading log from offset
      Emits "c" / "u" / "d" events

When it STOPS (restart):
   Picks up from last offset
   AT-LEAST-ONCE guarantee: may replay recent events on restart

When it FAILS (schema change, etc.):
   May enter error state; manual intervention sometimes needed
   Alerts should be configured
```

### 13. Snapshot Modes

```
Debezium snapshot modes (varies by connector):
   
   INITIAL (default): snapshot once, then stream
   NEVER: skip snapshot, start streaming now (lose history)
   WHEN_NEEDED: snapshot if offset invalid
   SCHEMA_ONLY: snapshot schemas, not data
   INCREMENTAL: snapshot in CHUNKS, interleaving with streaming (modern pattern)

INCREMENTAL SNAPSHOTS (2021+):
   Signal table triggers snapshot of specific tables
   Snapshot is BATCHED; CDC streaming CONTINUES during snapshot
   Enables backfilling specific tables without stopping production
   Based on Netflix DBLog algorithm (next section)

STAFF SIGNAL: understand that initial snapshot of a 10 TB production DB
   can take DAYS and stresses the source. Incremental snapshots solve 
   this. Argue them.
```

### 14. Transformations and Routing

```
SINGLE MESSAGE TRANSFORMATIONS (SMTs) in Kafka Connect / Debezium:
   
   Apply to events in-flight before writing to Kafka:
      - UnwrapFromEnvelope: flatten the Debezium envelope
      - TopicRouter: rename / route topics
      - FieldFilter: drop sensitive columns
      - Masking: mask PII fields
      - TimestampConverter: normalize timestamps
      - HeaderFrom: move fields to Kafka headers

Example:
   "transforms": "unwrap,mask",
   "transforms.unwrap.type": "io.debezium.transforms.ExtractNewRecordState",
   "transforms.mask.type": "org.apache.kafka.connect.transforms.MaskField$Value",
   "transforms.mask.fields": "ssn,credit_card"

CAVEAT: SMTs are per-message; expensive transforms kill throughput.
   For heavy transforms, stream to Flink/ksqlDB downstream instead.
```

---

## PART IV — SNAPSHOT PATTERNS

### 15. The Snapshot Problem

```
Classical snapshot-then-stream has an EDGE CASE:
   
   Snapshot runs: SELECT * at time T1
   Streaming starts: from offset at time T1
   
   But: during snapshot (minutes to hours), CHANGES OCCUR
      These changes are IN THE LOG from T0 (snapshot start) onwards
      Replayed on streaming start → DUPLICATE events for those rows
   
   Downstream needs IDEMPOTENT consumers to handle this correctly.

   OR: a row inserted after T1 but captured by snapshot (if snapshot not
      strictly consistent) → missed by log stream → MISSING downstream.

CORRECTNESS requires:
   Consistent snapshot (point-in-time)
   Streaming from the EXACT offset at snapshot start
   OR deduplication / upsert semantics downstream
```

### 16. Netflix DBLog (Watermarking)

```
NETFLIX DBLog (2019): a WATERMARK-BASED algorithm for snapshotting while 
   streaming without pausing.

ALGORITHM (simplified):
   1. Start streaming
   2. To snapshot a chunk of rows:
      a. Write a LOW WATERMARK marker to a dedicated "signal" table
      b. SELECT the chunk
      c. Write a HIGH WATERMARK marker
   3. In the stream:
      a. Events between low + high watermark MAY be duplicates of chunk rows
      b. For chunk rows: if ALSO in events between watermarks, USE EVENT (more recent)
      c. Otherwise, use SNAPSHOT value
   4. Emit result

This gives EXACTLY-ONCE snapshot semantics while streaming CONTINUES.

Debezium's INCREMENTAL SNAPSHOT implements this.
Reference: Netflix DBLog paper (ICDE 2021)
```

### 17. Incremental Snapshots

```
INCREMENTAL SNAPSHOTS (Debezium 1.6+, mature in 2.x):
   
   Chunk-based snapshot that INTERLEAVES with streaming
   Streaming never pauses
   Based on DBLog watermarks
   
   Trigger via signal table:
      INSERT INTO debezium_signal (id, type, data) 
      VALUES ('snapshot-1', 'execute-snapshot',
              '{"data-collections": ["public.orders"]}');
   
   Benefits:
      Add a new table to CDC without stopping
      Resume failed snapshots mid-way (chunk tracking)
      Backfill specific tables on demand
   
STAFF SIGNAL: for any question about "how do you add a new table to CDC
   without downtime," incremental snapshots are the answer.
```

### 18. Backfill Strategies

```
BACKFILL = producing historical data that CDC can't give you.

STRATEGIES:
   
   SNAPSHOT + STREAM: standard pattern; initial snapshot provides history
   
   INCREMENTAL SNAPSHOT: on-demand reload specific tables
   
   PARALLEL BULK LOAD: 
      One-time Spark job reads source via JDBC in parallel
      Writes to warehouse / lake
      CDC takes over from a point AFTER the bulk load's point-in-time
      Reconcile overlapping period via upsert
   
   TIME-TRAVEL REBUILD:
      Lakehouse with time travel: rebuild downstream from CDC log history
      Replay from CDC Kafka topic (if retention is long enough)
      
   LOG REPLAY from source:
      If source WAL / binlog retention is long enough, replay history
      Rare at any scale; retention is usually days, not months

PATTERN:
   Keep CDC topic retention generous (30+ days) for in-flight replay
   Keep source table archived (parquet in S3) for deep rebuild
```

---

## PART V — EXACTLY-ONCE CDC

### 19. Idempotency in CDC

```
AT-LEAST-ONCE delivery means duplicates WILL happen.
Downstream must be IDEMPOTENT.

PRIMARY KEY UPSERT:
   For each event, UPSERT (INSERT ... ON CONFLICT DO UPDATE) using PK
   Updates with same values are no-ops
   Works if events are ORDERED PER KEY (and they are, in log-based CDC)

MERGE / APPLY semantics:
   Lakehouse: MERGE INTO target USING cdc_batch ON pk WHEN MATCHED...
   Common in medallion architecture

OUT-OF-ORDER within a key is RARE but possible:
   Multi-threaded consumption without partition-key alignment
   Mitigate: partition Kafka topic by primary key

STAFF SIGNAL: for any CDC design question, articulate the IDEMPOTENCY
   mechanism explicitly. "We upsert on primary key with the log position
   as the version marker, discarding events with position less than
   current" is a staff-level answer.
```

### 20. Transactional Boundaries

```
IF SOURCE DB EMITS MULTI-ROW TRANSACTIONS, CDC events are:
   • Per-row (not per-transaction)
   • Tagged with source transaction ID (txId)
   • Emitted in order WITHIN a transaction

Downstream reconstruction of transactional atomicity:
   • Buffer by txId; apply all rows for a tx as one batch
   • Debezium supports "transaction metadata events" (BEGIN / END markers)
   
PRACTICALLY: most CDC consumers DON'T preserve atomicity downstream.
   The lake/warehouse accepts rows as they arrive.
   Queries see eventually-consistent state.

STAFF SIGNAL: if a question specifies atomicity requirements (e.g., 
   "transfers must debit + credit together"), argue the design with 
   explicit transaction grouping. Otherwise, the eventual model is fine.
```

### 21. CDC into Kafka Transactions

```
KAFKA TRANSACTIONS (exactly-once semantics):
   Producer writes to Kafka inside a transaction
   Consumer reads with read_committed isolation
   Enables exactly-once ingestion patterns

PATTERN:
   Debezium → Kafka (producer in transaction mode)
   Downstream Flink / Kafka Streams reads with EOS
   Writes to sink in a transaction tied to Kafka offset commit
   
RESULT: exactly-once from source → sink (modulo edge cases)

CAVEATS:
   Kafka transactions add latency (coordinator overhead)
   Debezium's EOS mode requires specific connector config
   Not all sinks support Kafka transactions
```

---

## PART VI — MANAGED CDC

### 22. AWS DMS, Datastream, Fivetran, Airbyte

```
AWS DMS (Database Migration Service):
   Managed CDC + migration
   Sources: most relational DBs, MongoDB, S3
   Targets: most relational DBs, S3, Kinesis
   Pros: AWS-native; simple
   Cons: slower than Debezium at scale; expensive for high throughput

GOOGLE DATASTREAM:
   Managed CDC on GCP
   Sources: Oracle, MySQL, Postgres, SQL Server
   Targets: BigQuery, GCS
   Pros: BigQuery-tight; serverless
   Cons: fewer sources than DMS

FIVETRAN:
   Managed data ingestion (CDC + SaaS)
   Massive connector catalog (500+ sources)
   Opinionated: schema evolution, transformations
   Pros: just works; wide source coverage
   Cons: expensive at scale; opinionated (less control)

AIRBYTE:
   Open-source + managed Airbyte Cloud
   Growing connector catalog
   Pros: self-host option; connector dev kit
   Cons: fewer sources than Fivetran historically
```

### 23. Lakeflow Connect, Debezium Server

```
DATABRICKS LAKEFLOW CONNECT (2024+):
   Managed CDC into Delta Lake
   Sources: Postgres, SQL Server, MySQL, Salesforce, Workday, ServiceNow
   Managed; no Debezium / Kafka to run
   Target: Delta tables via DLT

DEBEZIUM SERVER:
   Open-source Debezium as a standalone server
   Writes directly to sinks (Kafka, Pulsar, Kinesis, Pub/Sub, Redis, HTTP)
   No Kafka Connect operational burden
   Good for small-medium deployments

PATTERN FOR 2026:
   Managed if budget allows + sources are supported
   Debezium Server for medium-scale self-host
   Debezium on Kafka Connect for maximum control + extensibility
```

### 24. Build vs Buy

```
BUILD (self-managed Debezium):
   ✓ Full control + flexibility
   ✓ Lower unit cost at scale
   ✗ Ops burden (upgrades, HA, monitoring)
   ✗ Expertise required

BUY (Fivetran, DMS, Datastream, Lakeflow Connect):
   ✓ No ops
   ✓ Fast time-to-value
   ✗ Pricier at scale (Fivetran MAR pricing can be brutal)
   ✗ Less control (schema evolution, naming, performance)

HYBRID (common):
   SaaS sources (Salesforce, HubSpot): Fivetran
   Primary operational DBs (high throughput): self-managed Debezium
   Low-volume sources: whatever is cheapest

STAFF SIGNAL: argue the DECOMPOSITION of CDC workloads.
   Monolithic "we use Fivetran for everything" or "we built it all"
   are both weaker than tiered strategies.
```

---

## PART VII — OPERATIONS

### 25. Monitoring and SLAs

```
CDC-SPECIFIC METRICS:
   
   LAG: time between source event and CDC emission
      Postgres: pg_wal_lsn_diff(current_lsn, slot_restart_lsn)
      MySQL: Seconds_Behind_Master or Binlog position lag
      Measured in seconds + GB of unprocessed WAL/binlog
   
   THROUGHPUT: events per second per table
   
   ERROR RATE: schema evolution failures, decoding errors
   
   SNAPSHOT PROGRESS: for incremental snapshots
   
   SOURCE DB IMPACT: CPU, I/O, replication slot WAL retention

SLAs:
   P50 lag < 5 seconds
   P99 lag < 60 seconds
   Lag never exceeds X GB (prevents disk fill)

ALERTS:
   Lag > SLO for N minutes → page
   Replication slot WAL retention > X GB → page (disk risk)
   Connector in error state → page
   Snapshot stalled → page
```

### 26. Schema Evolution

```
SCHEMA CHANGES in source → downstream must adapt:
   
   SAFE CHANGES (backward compatible):
      Add nullable column
      Add column with default
      Widen numeric types (INT → BIGINT)
   
   DANGEROUS CHANGES:
      Drop column
      Rename column
      Narrow numeric type
      Change type incompatibly (STRING → INT)
      Rename / drop table

DEBEZIUM behavior:
   Schema changes typically land in a DEDICATED schema change topic
   Table topics continue with the new schema
   Downstream MUST handle schema evolution

PATTERNS:
   Schema registry (Confluent) for Kafka Avro / Protobuf
      Enforced compatibility (BACKWARD, FORWARD, FULL)
   dbt / Spark / warehouse: schema evolution auto-detection
      Add columns automatically; alert on breaking changes

STAFF SIGNAL: the staff-level answer connects CDC to data contracts
   (see 23-data-quality-observability). Breaking schema changes
   should go through CONTRACT review, not surprise downstream teams.
```

### 27. Common Failure Modes

```
FAILURE MODES (and mitigations):
   
   1. SLOW CONSUMER → WAL BACKUP → DISK FILL
      Mitigation: alert on slot retention; auto-advance if consumer dies
   
   2. SNAPSHOT PAUSED → NEW DATA LOSS
      Classic non-incremental snapshots pause CDC → changes between snapshot
      start and end are OK (captured by snapshot) but need careful offsetting
      Mitigation: incremental snapshots
   
   3. SCHEMA CHANGE BREAKS CONNECTOR
      Mitigation: schema evolution testing; proactive consumer updates
   
   4. SOURCE DB FAILOVER
      Replication slot / position may be invalid on failover
      Mitigation: GTIDs (MySQL), logical slots on failover (Postgres hard),
         or re-snapshot
   
   5. CDC EVENT ORDER OUT OF PARTITION
      Kafka partitions preserve order; cross-partition does not
      Mitigation: partition by primary key always
   
   6. DUPLICATES ON CONNECTOR RESTART
      At-least-once: dupes expected
      Mitigation: idempotent sinks + primary key upserts
   
   7. TOASTED VALUES MISSING (Postgres)
      Large column values stored out-of-line may be absent from CDC
      Mitigation: REPLICA IDENTITY FULL (costs source performance)
```

---

## PART VIII — INTERVIEW PRACTICE

### 28. The Staff-Level CDC Interview

```
COMMON QUESTIONS:
   1. "How does Postgres logical replication work?"
   2. "Difference between log-based and query-based CDC?"
   3. "Explain the snapshot problem and how DBLog solves it."
   4. "Design CDC from 20 production DBs to a lakehouse."
   5. "Debezium vs Fivetran vs DMS?"
   6. "Schema evolution in CDC: how?"
   7. "Exactly-once CDC: is it possible?"
   8. "Monitoring + SLAs for CDC."
   9. "A production DB disk fills due to CDC. Debug."
   10. "Why incremental snapshots?"
```

### 29. Worked System Design

```
Q: Design CDC architecture for a 50-service fintech.
   Source: 50 Postgres databases (operational); 10 MySQL; 5 Oracle.
   Sinks: Snowflake (analytics) + Delta Lake (ML).
   Latency SLA: 60 seconds P99 for tier-1 tables.
   Volume: 100K events/sec peak across all sources.
   Compliance: PII masking required; audit log of CDC events.

STAFF ANSWER:

ARCHITECTURE:
   
   SOURCE DBS → DEBEZIUM (Kafka Connect) → KAFKA → DUAL SINK
      → Snowflake (via Snowpipe Streaming)
      → Delta Lake (via Databricks DLT with Auto Loader on Kafka)

DEBEZIUM:
   Self-hosted Kafka Connect cluster on K8s
   One connector per DB; configured with:
      - Incremental snapshot support
      - PII masking SMTs for tier-1 tables
      - Schema registry integration (Confluent)
      - Primary-key partitioning on Kafka topics
   
   For Oracle: Debezium Oracle connector + LogMiner
      Oracle's extra complexity: isolated connector pool, extra monitoring

KAFKA:
   30-node cluster (3 racks for fault tolerance)
   Topics: one per source table (standard Debezium pattern)
   Retention: 30 days (enables replay for backfill + consumer catchup)
   Partitions by primary key for order preservation
   Avro + Schema Registry for schema evolution
   ACLs: strict per-topic access

SINK 1: SNOWFLAKE
   Snowpipe Streaming consumer per table
   Target: raw.<db>.<table> bronze tables
   Dynamic Tables for silver (deduped, SCD1 merge)
   
SINK 2: DELTA LAKE  
   Databricks DLT reading from Kafka
   Bronze: raw change events
   Silver: MERGE semantics with Delta's primary key support

SCHEMA EVOLUTION:
   Schema Registry: BACKWARD compatibility enforced
   Dev CI: fails if a service makes incompatible schema change
   Breaking changes: require RFC + deprecation window (see 23-data-quality)

PII HANDLING:
   SMTs mask known PII columns (ssn, credit_card, dob) at CDC level
   Unmasked PII goes to a RESTRICTED tier-0 topic (encrypted, limited consumers)
   Audit log: all CDC events landed to a separate audit sink

MONITORING:
   Grafana dashboards:
      Lag per connector (Debezium JMX metrics)
      Postgres replication slot retention
      Kafka consumer lag per sink
      Event throughput per table
   
   PagerDuty alerts:
      P1: lag > 5 min on tier-1 table
      P1: Postgres slot retention > 10 GB
      P1: Debezium connector in error state
      P2: schema evolution incompatible event

BACKFILL:
   Incremental snapshots triggered on-demand
   For deep history: parallel Spark job reads source via JDBC → Delta;
      reconciles with CDC via log position
   Kafka 30-day retention covers in-flight catchup after outages

HA / FAILOVER:
   Kafka Connect in HA mode (3+ workers)
   Postgres failover: logical replication slots don't auto-failover;
      scripted re-snapshot procedure in runbook
   MySQL: GTID-based failover resilience
   Oracle: Data Guard + GoldenGate (if budget) or re-snapshot

COST:
   Debezium cluster (K8s): $20K/month
   Kafka cluster: $40K/month
   Snowpipe Streaming: $30K/month
   Databricks DLT: $25K/month
   Monitoring + ops: $10K/month
   Total: ~$125K/month — defensible for a regulated fintech

KEY DECISIONS:
   Why Debezium (not Fivetran): 100K events/sec is Fivetran-cost-prohibitive
   Why dual-sink (not sink-and-replicate): downstream SLAs differ; direct is simpler
   Why 30-day Kafka retention: in-flight recovery for sink outages
```

### 30. Cheat Sheet &amp; Mental Models

#### 30.1 One-Minute Pitch

```
CDC = log-based change capture → downstream pipelines

STACK:
   DB WAL / binlog → Debezium (or managed CDC) → Kafka → sinks

INTERNALS:
   Postgres: logical replication + slots
   MySQL: binlog ROW format + GTIDs
   SQL Server: CDC feature (not CT)
   Oracle: LogMiner / GoldenGate

PATTERNS:
   Snapshot + stream for initial; incremental snapshots for add-ons
   Idempotent sinks (upsert on PK) for at-least-once
   Partition Kafka by PK for order preservation

OPERATIONS:
   Monitor lag + slot retention religiously
   Schema evolution via Schema Registry + contracts
   Incremental snapshots for in-flight table additions
```

#### 30.2 Senior vs Staff

| Senior | Staff |
|--------|-------|
| "We use Debezium." | "Postgres sources via Debezium with incremental snapshots; MySQL with GTID-aware config; Oracle via LogMiner or GoldenGate depending on tier. CDC is DB-specific plumbing; one connector pattern doesn't fit all sources." |
| "CDC gives exactly-once." | "CDC is at-least-once by default. Exactly-once requires: Kafka transactions on producer, read_committed on consumer, idempotent sinks (upsert on PK with LSN as version). The right question is 'is my sink idempotent' not 'is CDC exactly-once.'" |
| "Snapshots pause CDC." | "Classical snapshots pause CDC; incremental snapshots (Debezium 1.6+, based on Netflix DBLog) interleave snapshot with streaming via watermarks. For any production system adding tables mid-flight, incremental is the right pattern." |
| "Schema changes break CDC." | "Schema evolution requires: Schema Registry with BACKWARD compatibility, contracts between producer teams, deprecation windows for breaking changes, CI blocks on incompatible evolution. The CDC pipeline is where source-vs-consumer contracts become operational." |

> **🧠 Principal Mindset:** CDC is where database internals meet distributed systems — WAL formats, replication slots, logical decoding, exactly-once semantics, backfill patterns. Mastering CDC at this level is one of the clearest staff signals because it requires knowing (a) the source DB's log semantics, (b) the streaming system's delivery guarantees, (c) the sink's idempotency properties, and (d) the organizational discipline to not break the whole chain with a surprise schema change.

---

[← Back to main](./README.md)
