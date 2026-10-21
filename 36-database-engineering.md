# Topic 36 — Database Engineering: Internals, Indexing, Query Optimization

> Standalone deep-dive · The complete database engineering reference in one file

[← Back to main](./README.md)

---

> A single-file comprehensive database engineering reference for staff/principal engineers. Covers ACID, storage internals, indexing (B-Tree/B+Tree/LSM/hash), query execution and optimization, partitioning, sharding, concurrency control, replication, distributed transactions, consensus, database engines, cursors, NoSQL, security, and homomorphic encryption. ~230 KB; search-friendly.

## About

This chapter is a comprehensive reference for how databases ACTUALLY work under the hood. Target audience: data engineers, platform engineers, and backend engineers preparing for staff/principal interviews or production work. If you've used databases but never looked inside, start here. If you've looked inside, use this as a lookup reference.

Pairs with:
- `01-sql.md` — SQL from the software-engineering lens
- `16-sql-dba-reference.md` — SQL DBA operations across vendors
- `30-advanced-data-modeling.md` — Data Vault, Anchor, Activity Schema
- `../leetcode-sql-150/` — SQL interview module

## Table of Contents

- [About](#about)
- [How to Use](#how-to-use)
- [Core Concepts Cheat Sheet](#core-concepts-cheat-sheet)
- [1. Database Internals Foundations](#1-database-internals-foundations)
- [2. Storage Engines](#2-storage-engines)
- [3. Buffer Pool & Page Management](#3-buffer-pool-page-management)
- [4. Write-Ahead Log (WAL) & Recovery](#4-write-ahead-log-wal-recovery)
- [5. Database Indexing](#5-database-indexing)
- [6. B-Tree vs B+Tree in Production](#6-b-tree-vs-btree-in-production)
- [7. LSM Trees & Modern Storage](#7-lsm-trees-modern-storage)
- [8. Query Execution Model](#8-query-execution-model)
- [9. Query Optimization (CBO, RBO, EXPLAIN)](#9-query-optimization-cbo-rbo-explain)
- [10. ACID Transactions](#10-acid-transactions)
- [11. Concurrency Control (2PL, MVCC)](#11-concurrency-control-2pl-mvcc)
- [12. Isolation Levels & Phenomena](#12-isolation-levels-phenomena)
- [13. Database Partitioning](#13-database-partitioning)
- [14. Database Sharding](#14-database-sharding)
- [15. Database Replication](#15-database-replication)
- [16. Distributed Transactions (2PC, Saga)](#16-distributed-transactions-2pc-saga)
- [17. Consensus (Raft, Paxos)](#17-consensus-raft-paxos)
- [18. Database Engines Compared](#18-database-engines-compared)
- [19. Database Cursors](#19-database-cursors)
- [20. NoSQL Architecture](#20-nosql-architecture)
- [21. Database Security](#21-database-security)
- [22. Homomorphic Encryption](#22-homomorphic-encryption)
- [23. System Design Patterns](#23-system-design-patterns)
- [24. Interview Patterns](#24-interview-patterns)

## How to Use

**Zero to intermediate** — read sections 1 through 6. Internals + indexing is the foundation.

**Intermediate to advanced** — sections 7 through 12. Query optimization, transactions, concurrency.

**Distributed systems** — sections 13 through 17. Sharding, replication, consensus.

**Specialized** — 18 through 22. Engines, cursors, NoSQL, security, HE.

**For interviews** — sections 6, 9, 10, 11, 12, 15, 24 the week before.

## Core Concepts Cheat Sheet

| Concept | Meaning |
|---------|---------|
| **Page** | Fixed-size block (usually 4-16 KB) — unit of disk I/O |
| **Buffer pool** | In-memory cache of pages |
| **WAL** | Write-Ahead Log — durability via sequential log |
| **MVCC** | Multi-Version Concurrency Control — readers don't block writers |
| **B+Tree** | The dominant index structure for OLTP |
| **LSM Tree** | Log-Structured Merge Tree — write-optimized (RocksDB, Cassandra) |
| **ACID** | Atomicity, Consistency, Isolation, Durability |
| **Isolation level** | How concurrent transactions see each other |
| **2PL** | Two-Phase Locking — classical concurrency |
| **Snapshot isolation** | Each tx sees a consistent snapshot (MVCC default) |
| **CBO** | Cost-Based Optimizer — picks plan using statistics |
| **Sharding** | Horizontal partitioning across machines |
| **Replication** | Copying data to multiple nodes |
| **2PC** | Two-Phase Commit — distributed atomic transactions |
| **CAP** | Consistency, Availability, Partition tolerance — pick 2 |
| **PACELC** | CAP + "else, Latency vs Consistency when no partition" |

---

## 1. Database Internals Foundations

### 1.1 The Database Stack

A traditional RDBMS stack from top to bottom:

```
┌────────────────────────────────────────────┐
│       CLIENT (JDBC / libpq / psycopg2)      │
└────────────────────────────────────────────┘
                     │  SQL text + bind params
                     ▼
┌────────────────────────────────────────────┐
│       QUERY PARSER                          │
│       → Abstract Syntax Tree (AST)          │
└────────────────────────────────────────────┘
                     │
                     ▼
┌────────────────────────────────────────────┐
│       QUERY REWRITER / BINDER               │
│       → Resolve names, apply views, rewrites│
└────────────────────────────────────────────┘
                     │
                     ▼
┌────────────────────────────────────────────┐
│       QUERY OPTIMIZER (CBO / RBO)           │
│       → Choose join order, access methods,   │
│         scan vs index, hash vs sort-merge    │
└────────────────────────────────────────────┘
                     │  Physical plan
                     ▼
┌────────────────────────────────────────────┐
│       EXECUTOR                              │
│       → Volcano-style tuple-at-a-time       │
│         or vectorized batch execution       │
└────────────────────────────────────────────┘
                     │  Table / index access
                     ▼
┌────────────────────────────────────────────┐
│       STORAGE ENGINE                        │
│       → Page I/O, buffer pool, locking      │
│         transactions, MVCC, WAL             │
└────────────────────────────────────────────┘
                     │  Pages
                     ▼
┌────────────────────────────────────────────┐
│       DISK (SSD/NVMe/HDD)                   │
└────────────────────────────────────────────┘
```

### 1.2 Why Pages?

Databases store rows on **pages** (fixed-size blocks, typically 4-16 KB). Why not row-by-row?

**Reasons:**
- **Disk I/O is paged.** HDDs read sectors (512B-4KB); SSDs read pages (4-16KB). Reading < one page costs the same as reading the whole page.
- **CPU cache efficiency.** Pages fit cache lines; sequential access on a page is fast.
- **Lock granularity.** Many DBs lock at page or row level; pages are a natural unit.
- **Buffer pool management.** Fixed-size slots in RAM = simpler eviction (clock, LRU-K).

**Typical page sizes:**
- PostgreSQL: 8 KB (compile-time constant)
- MySQL InnoDB: 16 KB (configurable 4-64 KB)
- SQL Server: 8 KB
- Oracle: 8 KB (configurable 2-32 KB)
- SQLite: 4 KB (configurable 512B-64KB)

### 1.3 Page Layout (Slotted Page)

Most RDBMS use a **slotted page** layout:

```
┌──────────────────────────────────────────────────┐
│ Page Header (checksum, LSN, next page ptr, ...)  │
├──────────────────────────────────────────────────┤
│ Slot 0 | Slot 1 | Slot 2 | ... | Slot N          │  ← tiny ptrs
├──────────────────────────────────────────────────┤
│                                                   │
│         FREE SPACE                                │
│                                                   │
├──────────────────────────────────────────────────┤
│ Row N data                                        │
│ Row 2 data                                        │
│ Row 1 data                                        │
│ Row 0 data                                        │
└──────────────────────────────────────────────────┘
```

- Slot array grows **downward** from top; row data grows **upward** from bottom.
- Slots store (offset, length) of each row.
- Row IDs stay stable even if the row moves within the page (slot points to new offset).
- Deletes mark slots as free; compaction reclaims space.

### 1.4 Row vs Columnar Storage

- **Row store (OLTP):** entire row stored contiguously on a page. Fast for point reads and full-row scans. InnoDB, Postgres heap tables.
- **Columnar store (OLAP):** values of one column stored contiguously. Fast for aggregations over few columns. Parquet, ClickHouse, Snowflake micro-partitions.

See `20-row-vs-columnar-storage.md` for depth.

### 1.5 Heap vs Clustered Storage

- **Heap table** (Postgres, SQL Server default): rows appended in insertion order; indexes point to rows via physical pointers (TID/RID/CTID).
- **Index-organized table / clustered index** (MySQL InnoDB default, Oracle IOT): primary key IS the storage order; secondary indexes point to the primary key (not physical location).

Trade-offs:
- Heap: faster inserts; secondary indexes point directly (fast access).
- Clustered: fast range queries on PK; secondary indexes do extra lookup (one more index traversal).

> **🎯 Interview Angle — "Why does InnoDB require a primary key?"** Because InnoDB is index-organized — the table IS the primary key B+Tree. If you don't specify a PK, InnoDB creates a hidden one (`GEN_CLUST_INDEX`) of 6-byte row IDs. Secondary indexes store the PK value (not row pointer), so a secondary-index lookup = 2 B+Tree traversals.

### 1.6 TOAST / Large Objects

Values larger than a page need special handling:
- **Postgres TOAST:** auto-stores large values (>2 KB) in a separate TOAST table; the row holds a pointer.
- **MySQL InnoDB:** long text/blob stored out-of-row with Dynamic/Compressed row format.
- **Trade-off:** keeps main pages dense (good scans) but adds one disk access per large value.

---

## 2. Storage Engines

A **storage engine** is the component responsible for persisting data: pages, indexes, transactions, recovery. Many DBs let you choose.

### 2.1 Pluggable Storage Engines

- **MySQL:** InnoDB (default), MyISAM (legacy), MEMORY, ARCHIVE, CSV, Blackhole, NDB (clustered).
- **MongoDB:** WiredTiger (default), MMAPv1 (legacy), In-Memory.
- **PostgreSQL:** heap (default); extensions bring columnar (Citus, Zedstore, Hydra).
- **TiDB:** TiKV (row), TiFlash (columnar).
- **CockroachDB:** Pebble (default), RocksDB (legacy).

### 2.2 Storage Engine Taxonomy

| Family | Examples | Writes | Reads | Use |
|--------|----------|--------|-------|-----|
| **Update-in-place B+Tree** | InnoDB, SQLite, Oracle | Medium | Fast | OLTP, mixed |
| **Append-only MVCC** | PostgreSQL heap | Medium | Fast | OLTP with heavy reads |
| **LSM Tree** | RocksDB, LevelDB, Cassandra, Scylla | Very fast | Medium (bloom + bloom-filtered SSTable scan) | Write-heavy workloads |
| **Columnar** | ClickHouse MergeTree, Snowflake | Medium | Very fast analytic | OLAP |
| **In-memory** | Redis, Memcached, SAP HANA | Very fast | Very fast | Caches, hot state |
| **Log-only** | Kafka, Pulsar | Very fast append | Sequential scan | Streaming |

### 2.3 Storage Engine Internal Components

Every production engine implements:
- **Page/block layout:** how rows are packed, slot management, free space tracking.
- **Buffer pool:** memory cache for pages, eviction policy, dirty-page flushing.
- **Index structures:** B+Tree, hash, LSM, inverted, GIN/GiST (Postgres), etc.
- **Transaction manager:** begins/commits/rollbacks, locks, undo logs or MVCC.
- **WAL:** write-ahead log for durability + recovery.
- **Recovery:** redo + undo during crash restart (ARIES is canonical).
- **Vacuum / compaction:** reclaim space from dead rows (MVCC) or compact SSTables (LSM).

---

## 3. Buffer Pool & Page Management

### 3.1 The Buffer Pool

The **buffer pool** (aka buffer cache, shared buffers, buffer manager) is an in-memory cache of pages. Nearly all database performance centers on it.

Key operations:
- **Fetch:** read page into buffer; if cold, fetch from disk.
- **Pin:** prevent eviction while in use.
- **Dirty:** mark page modified; must be flushed before eviction.
- **Flush:** write dirty page back to disk (triggered by eviction, checkpoint, or background writer).

### 3.2 Eviction Policies

- **LRU (Least Recently Used):** simplest; works OK but one-off scans pollute the cache.
- **LRU-K (K=2):** tracks the Kth most recent access; large K resists scans.
- **CLOCK / Second Chance:** approximates LRU without a linked list; cheap.
- **ARC (Adaptive Replacement Cache):** balances recency and frequency; used in ZFS, previously in Postgres.
- **2Q:** separates one-shot scans from frequent accesses.

### 3.3 Postgres Shared Buffers

```sql
SHOW shared_buffers;        -- default often 128 MB — too small for prod
```

Rule of thumb: 15-25% of RAM. Rest goes to OS cache (Postgres relies on both layers).

```sql
-- Check buffer hit ratio
SELECT 
    sum(heap_blks_read) AS disk_reads,
    sum(heap_blks_hit) AS cache_hits,
    sum(heap_blks_hit) * 100.0 / (sum(heap_blks_hit) + sum(heap_blks_read)) AS hit_ratio
FROM pg_statio_user_tables;
-- Aim > 99% for OLTP
```

### 3.4 InnoDB Buffer Pool

```sql
SHOW VARIABLES LIKE 'innodb_buffer_pool_size';   -- aim 50-80% of RAM on dedicated boxes
SHOW ENGINE INNODB STATUS;   -- shows buffer pool stats, dirty pages, etc.
```

InnoDB uses a **modified LRU** with a young/old sublist: new pages enter the "old" (midpoint) list and only promote to "young" after a second access. Protects cache from one-off scans.

### 3.5 Write Patterns

- **Write-through:** writes synchronously to disk. Simple but slow.
- **Write-back:** writes to buffer, dirty pages flushed later. Fast but needs WAL for durability.

All production RDBMS use **write-back + WAL**. The WAL ensures durability even if dirty pages haven't flushed yet.

### 3.6 Page Replacement vs OS Cache

Databases traditionally bypass OS cache (`O_DIRECT`) to avoid double-caching. In practice:
- **Postgres:** uses OS cache + shared buffers (double cache; by design).
- **InnoDB:** uses `O_DIRECT` by default; buffer pool is the only cache.
- **RocksDB:** uses OS cache for data blocks, bypasses for logs.

> **💡 Tip — Monitoring buffer efficiency:**
> ```sql
> -- Postgres: cache hit ratio per table
> SELECT relname, heap_blks_read, heap_blks_hit,
>        round(heap_blks_hit::numeric * 100 /
>              nullif(heap_blks_hit + heap_blks_read, 0), 2) AS hit_pct
> FROM pg_statio_user_tables
> ORDER BY heap_blks_read + heap_blks_hit DESC LIMIT 20;
> ```

---

## 4. Write-Ahead Log (WAL) & Recovery

### 4.1 The WAL Protocol

The **Write-Ahead Logging** protocol:

> **Before** writing a dirty page to disk, the log record describing that modification MUST be written to the WAL first.

This guarantees that even if the database crashes mid-write, we can **recover** the committed state from the log.

```
Transaction T1:
   1. UPDATE row R   (modify page P in buffer)
   2. Write WAL record "T1 modified P: before=X, after=Y"  ← flushed to disk
   3. COMMIT → write "T1 COMMIT" to WAL + flush
   4. Return success to client
   
   [Later, in background]
   5. Flush dirty page P back to data file
```

### 4.2 ARIES — Recovery Done Right

ARIES (Algorithm for Recovery and Isolation Exploiting Semantics; IBM, 1992) is the canonical recovery algorithm. Three phases:

**1. Analysis** — scan WAL from last checkpoint; identify dirty pages + in-flight transactions.
**2. Redo** — replay ALL log records (even for uncommitted txns) to restore in-memory state.
**3. Undo** — rollback uncommitted txns using undo log entries (written during normal operation).

ARIES features:
- **Log Sequence Number (LSN):** every log record + page is tagged.
- **No-force, Steal:** pages can be flushed before commit (steal); not forced on commit (no-force). Requires WAL to work.
- **Fine-grained locking:** per-row concurrency.
- **Repeating history:** redo phase replays everything, then undo rolls back uncommitted.

### 4.3 Checkpoints

A **checkpoint** flushes all dirty pages up to a given LSN, then writes a checkpoint record. Benefits:
- Reduces recovery time (don't replay from the beginning of the log).
- Allows WAL files to be recycled or archived.

**Trade-off:** too frequent = write amplification; too rare = long recovery.

```sql
-- Postgres
SHOW checkpoint_timeout;     -- default 5 min
SHOW max_wal_size;           -- triggers checkpoint when WAL exceeds this

-- MySQL InnoDB
SHOW VARIABLES LIKE 'innodb_log_file_size';
```

### 4.4 Fsync & Durability

**fsync()** forces the kernel to flush buffered writes to physical storage. Databases call fsync on WAL writes to make commits durable.

- **synchronous_commit=on** (Postgres default): fsync WAL on commit. Durable but slow.
- **synchronous_commit=off**: ack commit before fsync. Fast but up to `wal_writer_delay` of data loss on crash.
- **synchronous_commit=remote_apply** (replicated): wait for standby to apply WAL. Strongest durability.

MySQL analog: `innodb_flush_log_at_trx_commit`:
- `1` (default): fsync per commit. ACID.
- `2`: write to OS buffer per commit, fsync once/sec. Fast; OS crash = 1s loss.
- `0`: no per-commit flush. Fastest; DB crash = 1s loss.

### 4.5 WAL Compression & Archiving

- **WAL compression:** reduces I/O for pages with many small changes.
- **WAL archiving:** copy WAL files to backup storage (S3/HDFS) for point-in-time recovery (PITR).
- **Log shipping / streaming replication:** stream WAL to standby for HA.

### 4.6 Group Commit

When many transactions commit concurrently, each fsync is expensive. **Group commit** batches multiple commits into one fsync. Postgres and MySQL both support this; usually automatic.

### 4.7 Logical vs Physical WAL

- **Physical WAL** (Postgres, MySQL binlog ROW format): records page-level or row-level changes.
- **Logical WAL** (Postgres logical replication, MySQL binlog STATEMENT): records SQL statements or logical row events.

CDC (see `27-cdc-deep-dive.md`) typically consumes logical WAL.

---

## 5. Database Indexing

### 5.1 Why Index?

Without an index, finding a row requires a full table scan (O(N)). An index is an auxiliary data structure that makes lookups fast (typically O(log N)).

**Trade-offs:**
- Reads faster
- Writes slower (every insert/update/delete updates each affected index)
- Storage overhead (indexes take disk + buffer pool space)

### 5.2 Index Taxonomy

| Type | Structure | Good for | Examples |
|------|-----------|----------|----------|
| **B-Tree / B+Tree** | Balanced tree | Range + equality queries | Default for most RDBMS |
| **Hash** | Hash table | Equality only (no ranges) | Memory engine, Postgres `USING HASH` |
| **LSM** | Sorted on-disk tables + mem buffer | Write-heavy, equality + range | RocksDB, Cassandra |
| **Bitmap** | Bitmap per distinct value | Low-cardinality columns, OLAP | Oracle, Postgres w/ BRIN |
| **GiST / GIN** (Postgres) | Generalized tree / inverted | Full-text, arrays, geo | Postgres JSON/text/array indexes |
| **BRIN** (Postgres) | Block Range IN dex | Very large tables, correlated data | Time-series, append-only |
| **R-Tree** | Hierarchical bounding boxes | Spatial / multidimensional | PostGIS |
| **Inverted** | Term → list of docs | Full-text search | Elasticsearch, Lucene |

### 5.3 Clustered vs Non-Clustered

- **Clustered index** (primary) — the table IS the index. One per table (physical storage order). InnoDB PK, Oracle IOT.
- **Non-clustered / secondary** — separate structure; leaves contain pointer to row (RID / TID / CTID / PK).

In InnoDB, secondary indexes store the primary key, not a physical pointer. Lookup via secondary = 2 B+Tree traversals (secondary → PK, PK → row).

### 5.4 Composite / Multi-Column Indexes

```sql
CREATE INDEX idx_users_country_age ON users(country, age);
```

The index can serve:
- Equality on `country` alone
- Equality on `(country, age)`
- Range on `age` only if `country` is first-filtered by equality

The **leftmost prefix rule:** the index can be used only for prefixes of its columns. `WHERE age = 30` alone cannot use this index.

**Column ordering guide:**
1. Equality columns first
2. Then range columns
3. Order equality cols by selectivity (highest first)

### 5.5 Covering Indexes

An index that contains ALL columns needed by a query. The optimizer can use the index alone without touching the table ("index-only scan").

```sql
-- Query:
SELECT country, SUM(amount) FROM orders WHERE date > '2024-01-01' GROUP BY country;

-- Covering index:
CREATE INDEX idx_orders_cover ON orders(date, country, amount);
-- Index contains everything; no heap access needed
```

Postgres: `CREATE INDEX ... INCLUDE (col)` adds non-key columns to the index for coverage without affecting the ordering/B-tree key.

### 5.6 Partial Indexes

Index only a subset of rows (saves space + speeds specific queries).

```sql
-- Postgres: index only unprocessed orders (small fraction)
CREATE INDEX idx_orders_pending ON orders(created_at) WHERE status = 'pending';

-- Queries with the same predicate use it:
SELECT * FROM orders WHERE status = 'pending' AND created_at < '2024-10-01';
```

### 5.7 Expression / Functional Indexes

Index the result of an expression.

```sql
CREATE INDEX idx_users_lower_email ON users(LOWER(email));
SELECT * FROM users WHERE LOWER(email) = 'alice@example.com';   -- uses index
```

Without the expression index, `LOWER(email)` can't use a regular `email` index.

### 5.8 Hash Indexes

```sql
CREATE INDEX idx_users_id_hash ON users USING HASH (id);
```

- **O(1) equality lookups** (vs O(log N) for B-Tree)
- **No range queries** (hash loses ordering)
- **Not durable in old Postgres** (fixed in 10+)

Niche; B-Tree usually wins because the practical difference between O(1) and O(log N) is tiny.

### 5.9 BRIN Indexes (Postgres)

**Block Range IN dex** stores min/max per block range. Tiny (KB for TB-scale tables) but only useful when data is physically ordered by the indexed column.

```sql
CREATE INDEX idx_events_ts_brin ON events USING BRIN (event_time);
-- Great for append-only time-series
```

Query: `WHERE event_time BETWEEN '...' AND '...'` → BRIN eliminates ranges that can't contain matching rows.

### 5.10 GIN / GiST (Postgres)

- **GIN** (Generalized Inverted Index): good for arrays, JSONB, full-text search. Stores a mapping from "value" → list of rows containing it.
- **GiST** (Generalized Search Tree): extensible balanced tree; used for geo, range types, trigrams, k-NN.

```sql
-- JSONB
CREATE INDEX idx_data_gin ON events USING GIN (data);
SELECT * FROM events WHERE data @> '{"status": "active"}';

-- Full-text
CREATE INDEX idx_articles_fts ON articles USING GIN (to_tsvector('english', body));
```

### 5.11 Index Maintenance

- **Fragmentation:** random deletes/updates fragment B-Tree pages. Rebuild periodically (`REINDEX`, `OPTIMIZE TABLE`).
- **Statistics:** optimizer needs up-to-date stats (ANALYZE, `UPDATE STATISTICS`).
- **Monitoring:** index size, usage counts, scan ratios.

```sql
-- Postgres: unused indexes (candidates for drop)
SELECT indexrelname, idx_scan FROM pg_stat_user_indexes WHERE idx_scan = 0;

-- Index size
SELECT pg_size_pretty(pg_relation_size('idx_name'));
```

### 5.12 Over-Indexing

Common pitfall: adding indexes for every query. Each index slows writes.

**Rule of thumb:** < 5-10 indexes per OLTP table. Analyze actual usage; drop unused.

> **🎯 Interview Angle — "When does an index HURT performance?"**
> 1. Writes slow (every index updated on INSERT/UPDATE/DELETE).
> 2. Small tables (full scan faster than tree traversal).
> 3. Low selectivity queries (optimizer might ignore the index anyway).
> 4. Unused indexes (storage + maintenance for nothing).
> 5. Duplicate indexes (multiple indexes with the same leading columns).

---

## 6. B-Tree vs B+Tree in Production

### 6.1 Why Trees for Indexes?

- O(log N) search/insert/delete (vs O(N) for arrays/lists)
- Sorted (enables range queries, ORDER BY optimization)
- Balanced (worst case = average case)
- Fit disk access (fan-out matches page size)

### 6.2 B-Tree Properties

A **B-Tree** of order M (M children per node):
- Each node holds up to M-1 keys + M child pointers
- Keys are sorted within a node
- All leaves are at the SAME depth (balanced)
- Each internal node stores (key, value_pointer, child_ptr) tuples
- Values can be stored in INTERNAL nodes too

```
                [20, 50, 80]              ← internal node
               /    |    |    \
         [10, 15] [30, 40] [60, 70] [90, 95]   ← leaves AND internals have values
```

**Properties:**
- Minimum M/2 children per non-root
- Balanced via splits (on insert overflow) and merges (on delete underflow)

### 6.3 B+Tree Properties

A **B+Tree** differs from a B-Tree:
- **All values stored in LEAVES** (internal nodes hold only keys for routing)
- **Leaves form a LINKED LIST** (sibling pointers)
- Internal nodes are compact → higher fan-out → shallower tree

```
                   [50]                ← internal (routing only)
                  /    \
            [20, 35]  [70, 85]         ← internal (routing only)
            /  |  \   /  |  \
         [L1][L2][L3][L4][L5][L6]      ← leaves hold VALUES, linked
           ↕ ↕ ↕ ↕ ↕ ↕                  ← sibling pointers
```

### 6.4 Why B+Tree Wins in Production

**1. Higher fan-out → shallower tree → fewer disk reads.**
Internal nodes have no values, so they fit more keys per page → higher fan-out → log₂(N) depth drops.

Example at 100M rows, 16 KB page:
- B-Tree: fan-out ~100 → depth ~4
- B+Tree: fan-out ~300 → depth ~3

One less level = one less disk read per lookup. Huge.

**2. Range queries are O(log N + K) via linked leaves.**
After finding the start of a range, follow leaf pointers sequentially — no need to re-traverse the tree.

B-Tree range query: in-order traversal via recursion → O(log N + K) but with worse cache behavior.

**3. Full-leaf scans are easy.**
Walking the leaf linked list = scan the whole index without traversing internals.

**4. Clean separation of index + data.**
In B+Tree, internal nodes don't hold data → smaller internal node layer → fits more cleanly in buffer pool.

### 6.5 Why Not B-Tree in Production?

B-Trees do have one advantage: finding a key in an internal node means you're done (don't descend to leaf). But in practice:
- The win is small (saves 1 level average)
- Range queries are slower (no leaf list)
- Variable-size node structure is more complex

**Virtually all production RDBMS use B+Tree variants** for indexes.

### 6.6 B+Tree Variants Used in Production

- **B-link Tree** (Lehman & Yao, 1981): adds right-sibling pointers for lock-free concurrent access. Used in PostgreSQL.
- **Blink-Tree:** variant with "high key" per node for concurrency protocol.
- **Bε-Tree** (B-Epsilon): write-optimized; buffers inserts at internal nodes. TokuDB, Percona.
- **Fractal Tree:** marketing name for Bε-Tree (TokuDB).
- **COW B-Tree** (Copy-On-Write): never modifies pages in place; writes new versions. SQLite, LMDB.
- **B+Tree with prefix compression:** compresses common prefixes in leaf nodes (common for string keys).

### 6.7 B+Tree Operations

**Search (key K):**
1. Start at root.
2. Binary-search internal node to find subtree containing K.
3. Descend to child; repeat.
4. At leaf: binary-search for K; return value.
5. Total I/O: tree depth (typically 3-5 for billions of rows).

**Insert (K, V):**
1. Find leaf where K belongs.
2. Insert; if leaf has space, done.
3. If full, SPLIT: create new leaf, redistribute keys, promote middle key to parent.
4. If parent full, split recursively up. If root splits, tree grows one level.

**Delete (K):**
1. Find and remove from leaf.
2. If leaf underflows (< M/2 keys), BORROW from sibling or MERGE.
3. If merge, parent loses a key; may cascade upward.
4. If root ends up with 1 child, demote.

In practice, many engines use **lazy deletion** (mark as deleted; reclaim later during compaction / vacuum) to avoid the cost of structural rebalancing.

### 6.8 B+Tree Concurrency

Classic approach: **lock coupling** (also "crabbing"):
- Lock root; descend; lock child; release root; descend; etc.
- Writes acquire exclusive locks; reads acquire shared.

Problems: writers block readers under high concurrency. Modern engines use:
- **B-link trees** (Lehman-Yao): right-sibling pointers allow lock-free reads during splits.
- **Optimistic concurrency:** read without locks; validate on finish.

### 6.9 B+Tree Page Layout (Postgres example)

Postgres B+Tree page:
```
┌────────────────────────────────────────────┐
│ Page header (LSN, type, free space, ...)   │
├────────────────────────────────────────────┤
│ Line pointer array (slot per item)          │
├────────────────────────────────────────────┤
│                                             │
│         FREE SPACE                          │
│                                             │
├────────────────────────────────────────────┤
│ Item N data (key + ptr)                     │
│ Item 2 data                                 │
│ Item 1 data                                 │
└────────────────────────────────────────────┘
```

Internal page items: `(key, child_page_ptr)`.
Leaf page items: `(key, heap_tid)` — pointer to the heap tuple.

### 6.10 Interview-Ready Comparison

| Dimension | B-Tree | B+Tree |
|-----------|--------|--------|
| Values stored | Internal + leaves | Leaves only |
| Fan-out | Lower (internals hold values) | Higher (routing only) |
| Depth | Deeper | Shallower |
| Range queries | Slower (in-order traversal) | Fast (linked leaves) |
| Point lookup | Slightly faster (can hit internal) | O(depth) always |
| Full scan | Harder | Walk leaf list |
| Production use | Rare | Dominant (99%) |

> **🎯 Interview Angle — "Why B+Tree over B-Tree in production databases?"**
> Three reasons: (1) Internal nodes are smaller (no values) → higher fan-out → shallower tree → fewer disk reads per lookup. (2) Range queries are O(log N + K) via leaf linked list. (3) Full-table scans don't require traversing internal nodes. The trade-off is slightly slower point lookups (always traverse to leaf), but the range + fan-out advantages dominate for the workloads databases actually run.

---

## 7. LSM Trees & Modern Storage

### 7.1 LSM Motivation

B+Trees are great for reads but writes update pages in place → random I/O. For write-heavy workloads (log ingestion, time-series, event stores), you want sequential writes.

**Log-Structured Merge Tree (LSM):**
- Writes go to an in-memory sorted structure (MemTable)
- When MemTable fills, flush to disk as an immutable Sorted String Table (SSTable)
- Reads check MemTable + all SSTables (expensive, mitigated by bloom filters + layering)
- Background compaction merges SSTables (reclaims deleted/overwritten data)

### 7.2 LSM Structure

```
WRITE PATH:
   Client → MemTable (sorted in-memory, e.g., skip list)
            │
            ▼ (when full)
         SSTable Level 0 (newest; may overlap)
            │
            ▼ (compaction)
         SSTable Level 1 (sorted, non-overlapping)
            │
            ▼
         SSTable Level 2
            │
            ▼
         ... (deeper levels, larger size)

READ PATH:
   Client query → MemTable (newest) → L0 → L1 → ... → Ln
                  Return first match (newer wins)
                  Bloom filter per SSTable skips most levels
```

### 7.3 Compaction Strategies

- **Size-tiered (STCS):** merge SSTables of similar size. Fast writes, bursty disk usage.
- **Leveled (LCS):** keep SSTables in levels; each level N × size of previous. Smoother reads, more write amplification.
- **Time-window (TWCS):** group SSTables by time windows. Good for time-series (TTL cleanup).
- **Hybrid:** mix of above.

### 7.4 LSM Trade-offs

- **Write amplification:** data rewritten multiple times during compaction (3-10x typical).
- **Space amplification:** multiple versions on disk until compacted (1.1-2x).
- **Read amplification:** multiple SSTables may need checking (1-10x; bloom filters help).

### 7.5 Bloom Filters

Each SSTable has a **bloom filter**: a bit array that says "this key is DEFINITELY NOT here" or "this key MIGHT be here." Lets reads skip SSTables that can't contain the key.

False positive rate ~1%; false negatives impossible.

### 7.6 LSM-Based Systems

- **RocksDB** (Facebook, open-source): embeddable LSM engine. Powers MyRocks, CockroachDB (legacy), TiKV, YugabyteDB.
- **LevelDB** (Google): predecessor of RocksDB.
- **Cassandra:** distributed LSM, eventual consistency.
- **Scylla:** C++ Cassandra rewrite; LSM-based.
- **HBase:** Hadoop-era LSM distributed DB.
- **DynamoDB:** uses LSM internally.
- **MongoDB WiredTiger:** supports both LSM and B+Tree variants.

### 7.7 LSM vs B+Tree Comparison

| | B+Tree | LSM |
|-|--------|-----|
| Write speed | Medium (page update) | Fast (sequential append) |
| Read speed | Fast (O(log N)) | Medium (bloom + layer scan) |
| Range read | Fast (leaf list) | Fast per SSTable + merge |
| Space amp | Low (1.0x) | Medium (1.1-2x) |
| Write amp | 1-3x (page writes, WAL) | 3-10x (compaction) |
| Latency P99 | Predictable | Variable (compaction spikes) |
| Use case | OLTP, read-heavy | Write-heavy, time-series, streaming |

---

## 8. Query Execution Model

### 8.1 Volcano / Iterator Model

Classical execution: each operator exposes `open()`, `next()` (returns one tuple), `close()`. Tuples flow one at a time up the tree.

```
         FINAL RESULT
              ▲
              │ next() returns tuple
          [AGGREGATE]
              ▲
              │ next()
            [JOIN]
            /    \
         [SCAN T1] [SCAN T2]
```

Simple, elegant. But: one `next()` call per tuple = function-call overhead = slow for OLAP.

### 8.2 Vectorized Execution

Each operator processes a BATCH (vector) of tuples per call. Reduces function-call overhead; enables CPU vectorization (SIMD).

Used by: Snowflake, DuckDB, ClickHouse, Databricks Photon, Pandas, Polars.

### 8.3 Code Generation (Whole-Stage Codegen)

Compile the query plan into native code (JVM bytecode, LLVM, native binary) at runtime. Spark (Tungsten), Impala, HyPer, Umbra.

### 8.4 Push vs Pull

- **Pull-based (Volcano):** parent calls `next()` on child.
- **Push-based:** child pushes tuples to parent as they're produced.

Push-based is better for streaming and pipelined execution. Modern engines use hybrid.

### 8.5 Physical Operators

Common physical operators:
- **Scan:** sequential or indexed table scan.
- **Filter:** apply predicate.
- **Project:** select columns.
- **Join:** nested-loop / hash / sort-merge.
- **Group-by / Aggregate:** hash agg or sort agg.
- **Sort:** external sort if too big for memory.
- **Limit / TopN:** bounded sort or heap-based.
- **Exchange / Shuffle** (distributed): redistribute data by key.

### 8.6 Join Algorithms

- **Nested Loop Join:** O(N × M). For each row in R, scan S. OK for small S or indexed S. Default fallback.
- **Block Nested Loop:** NL but process R in blocks, scanning S once per block.
- **Hash Join:** build hash table on smaller side (R); probe with larger (S). O(N + M). Standard for equi-joins.
- **Sort-Merge Join:** sort both by join key, merge-walk. O(N log N + M log M). Good when inputs already sorted.
- **Grace Hash / Partitioned Hash:** for data larger than memory; partition both into buckets fitting memory; join bucket-by-bucket.

### 8.7 Aggregation Algorithms

- **Hash Aggregation:** build hash table keyed by group columns; accumulate aggregates. Fast but needs memory.
- **Sort Aggregation:** sort by group columns; walk sorted stream and emit aggregates per group change. Scales to any size.
- **Streaming (pipelined) agg:** if input is already sorted by group key.

### 8.8 Sort

**External merge sort** when data > memory:
1. Read chunks that fit in memory.
2. Sort each chunk; write to temp disk.
3. Merge sorted runs k-way until one output.

I/O: 2 × (log_k(N/M)) × N where M = memory, k = merge fan-in.

### 8.9 Parallelism

- **Intra-query parallelism:** one query uses multiple cores.
  - **Intra-operator:** one operator processes partitions in parallel.
  - **Inter-operator:** pipelined operators run on different cores.
- **Inter-query parallelism:** multiple queries at once.

Postgres supports parallel sequential scans, parallel hash joins, parallel aggregates (via `max_parallel_workers_per_gather`).

### 8.10 Execution Model Comparison

| Model | Latency | Throughput | Example |
|-------|---------|-----------|---------|
| Volcano / iterator | Low per query | Low per-row | PostgreSQL (classic), SQLite |
| Vectorized | Low | High | DuckDB, ClickHouse, Polars |
| Codegen | Low | Highest | Spark (Tungsten), Impala |
| Push-based pipelined | Low (streaming-friendly) | High | Flink, Materialize |

---

## 9. Query Optimization (CBO, RBO, EXPLAIN)

### 9.1 The Optimizer's Job

Given a SQL query, choose:
1. **Access method per table:** full scan / index scan / index-only scan
2. **Join order:** which tables first, which join next
3. **Join algorithm:** hash / sort-merge / nested loop
4. **Aggregation algorithm:** hash / sort
5. **Parallelism:** how many workers
6. **Data layout:** sort before / after; redistribute

Different plans can have vastly different cost (100-1000x). Picking the right one is the optimizer's purpose.

### 9.2 Rule-Based vs Cost-Based

- **Rule-Based (RBO):** hardcoded rules ("if there's an index, use it"). Deterministic but inflexible.
- **Cost-Based (CBO):** estimates cost per plan using statistics; picks cheapest. Standard today.

Modern optimizers are mostly CBO with some rule-based rewrites (view inlining, subquery flattening, constant folding).

### 9.3 Statistics the Optimizer Uses

- **Table row count** (NTuples, reltuples)
- **Number of distinct values per column** (NDV)
- **Column histograms** (data distribution)
- **Null fraction**
- **Correlation** (physical order vs column value)
- **Common values list** (most-common values for skewed columns)
- **Index statistics** (depth, leaf page count, fill factor)

### 9.4 Keeping Statistics Fresh

```sql
-- Postgres: manual + auto via autovacuum
ANALYZE table_name;              -- single table
ANALYZE table_name (col1, col2); -- specific columns
VACUUM ANALYZE table_name;       -- vacuum + stats

-- MySQL
ANALYZE TABLE table_name;
UPDATE STATISTICS;  -- SQL Server
```

**Autovacuum / Auto-Analyze** (Postgres) runs periodically. Thresholds configurable.

Stale stats → bad plans → performance regressions. Classic incident: a table grows 10x; old stats say 1M rows; optimizer picks nested loop; query runs for hours.

### 9.5 Cardinality Estimation

The optimizer estimates how many rows each operator produces. Errors compound:

```
Table T: 10M rows
WHERE col1 = X AND col2 = Y
   → Est(col1=X) × Est(col2=Y) × 10M (assumes independence)
```

If col1 and col2 are correlated, the estimate is way off. Fixes:
- **Multi-column statistics** (Postgres `CREATE STATISTICS`)
- **Explicit hints** (MySQL, Oracle; Postgres has `pg_hint_plan` extension)
- **Query rewriting**

### 9.6 Join Order

For N tables, there are N! join orders (and more with left-deep vs bushy trees). Optimizer uses:
- **Dynamic programming** (Postgres: up to 12 tables; falls back to GEQO beyond)
- **Heuristics** (left-deep tree; prefer smaller first)
- **Genetic algorithms** (GEQO for complex queries)

### 9.7 EXPLAIN

```sql
-- Postgres
EXPLAIN SELECT * FROM orders WHERE date > '2024-01-01';
EXPLAIN (ANALYZE, BUFFERS) ...;   -- actual execution + I/O metrics
EXPLAIN (FORMAT JSON) ...;        -- machine-readable

-- MySQL
EXPLAIN SELECT ...;
EXPLAIN FORMAT=TREE SELECT ...;   -- 8.0+ nicer output
EXPLAIN ANALYZE SELECT ...;       -- 8.0.18+

-- SQL Server
SET SHOWPLAN_ALL ON;
SET STATISTICS PROFILE ON;

-- Oracle
EXPLAIN PLAN FOR SELECT ...;
SELECT * FROM TABLE(DBMS_XPLAN.DISPLAY());
```

### 9.8 Reading a Postgres EXPLAIN

```
Hash Join  (cost=2.52..18.52 rows=100 width=68) (actual time=0.3..2.1 rows=103 loops=1)
  Hash Cond: (o.customer_id = c.id)
  Buffers: shared hit=42 read=5
  ->  Seq Scan on orders o  (cost=0.00..15.00 rows=1000 width=32) (actual time=0.01..1.2 rows=1000)
        Filter: (status = 'completed')
        Rows Removed by Filter: 500
  ->  Hash  (cost=1.25..1.25 rows=100 width=36)
        ->  Seq Scan on customers c  (cost=0.00..1.25 rows=100 width=36)
```

Key fields:
- **cost:** estimated (startup..total), unitless
- **rows:** estimated rows out
- **actual time:** real time (with ANALYZE)
- **loops:** how many times executed
- **width:** row width in bytes
- **Buffers:** `shared hit` = cache hit; `shared read` = disk I/O

**Rule:** compare `rows` (estimated) to `actual rows`. If off by 10x+, stats are stale or correlations are missing.

### 9.9 Common Plan Operators

- **Seq Scan:** full table scan
- **Index Scan:** B+Tree traversal then heap lookup
- **Index Only Scan:** index covers the query — no heap access
- **Bitmap Scan:** combine multiple indexes via bitmaps, then heap
- **Nested Loop:** nested-loop join
- **Hash Join:** build hash on inner, probe with outer
- **Merge Join:** sort-merge
- **Sort:** external if needed
- **Hash Aggregate:** group by hash
- **GroupAggregate:** group by sort order
- **Gather:** parallel workers merging back
- **Materialize:** cache intermediate for inner reuse

### 9.10 Common Optimizer Rewrites

- **Predicate pushdown:** push WHERE down past joins (reduce row count early)
- **Projection pruning:** drop unused columns
- **Subquery flattening:** `WHERE x IN (SELECT ...)` → semi-join
- **View inlining:** replace view reference with its definition
- **Join elimination:** drop unnecessary joins (e.g., LEFT JOIN with foreign key when no columns used)
- **Constant folding:** `WHERE col > 2 + 3` → `WHERE col > 5`
- **CTE inlining:** inline simple CTEs (Postgres 12+ by default)
- **LIMIT pushdown:** avoid sorting whole result for ORDER BY ... LIMIT
- **Partition pruning:** skip partitions that can't match WHERE

### 9.11 Query Hints

Most DBs allow hints to override the optimizer:

```sql
-- Oracle
SELECT /*+ USE_HASH(o c) INDEX(o idx_orders_date) */ ...

-- SQL Server
SELECT ... FROM orders o WITH (INDEX(idx_orders_date))
OPTION (HASH JOIN)

-- MySQL
SELECT /*+ JOIN_ORDER(o, c) */ ...

-- Postgres: no native hints; use pg_hint_plan extension
SELECT /*+ HashJoin(o c) IndexScan(o idx_orders_date) */ ...
```

**Rule:** hints are last-resort. Preferable to fix stats + rewrite the query.

### 9.12 Query Rewriting Patterns

Common manual rewrites to help the optimizer:

**Push predicates past aggregations:**
```sql
-- Slow:
SELECT * FROM (SELECT country, SUM(amount) AS total FROM orders GROUP BY country) t
WHERE total > 1000;

-- Fast: push WHERE amount > 0 inside if applicable; use HAVING for aggregated filter
SELECT country, SUM(amount) AS total FROM orders GROUP BY country HAVING SUM(amount) > 1000;
```

**EXISTS over IN for correlated subqueries:**
```sql
-- EXISTS short-circuits on first match
SELECT * FROM orders o WHERE EXISTS (SELECT 1 FROM customers c WHERE c.id = o.customer_id);

-- IN materializes the full subquery result
SELECT * FROM orders WHERE customer_id IN (SELECT id FROM customers);
```

For modern optimizers (Postgres 12+, SQL Server), these are often equivalent after rewrite.

**Avoid functions on indexed columns:**
```sql
-- BAD: can't use index on created_at
SELECT * FROM orders WHERE DATE(created_at) = '2024-10-19';

-- GOOD
SELECT * FROM orders WHERE created_at >= '2024-10-19' AND created_at < '2024-10-20';
```

**Avoid implicit casts:**
```sql
-- BAD if customer_id is int: implicit cast of literal
SELECT * FROM orders WHERE customer_id = '123';

-- GOOD
SELECT * FROM orders WHERE customer_id = 123;
```

**OR → UNION ALL:**
```sql
-- Optimizer may not use indexes on both sides of OR
SELECT * FROM orders WHERE customer_id = 1 OR product_id = 5;

-- Alternative: UNION ALL to use each index
SELECT * FROM orders WHERE customer_id = 1
UNION ALL
SELECT * FROM orders WHERE product_id = 5 AND customer_id <> 1;
```

### 9.13 Query Plan Caching

Many DBs cache compiled plans:
- **SQL Server:** plan cache + parameter sniffing
- **Oracle:** library cache
- **PostgreSQL:** prepared statements cache in session; `pg_prewarm` extension
- **MySQL:** query cache (removed in 8.0; prepared statements cached)

**Parameter sniffing problem:** plan compiled for first parameter value may be bad for other values. Fixes: `OPTION (RECOMPILE)`, `OPTIMIZE FOR UNKNOWN`, plan guides.

### 9.14 Adaptive Query Execution

Runtime re-planning based on actual stats:
- **Spark AQE** (see `35-pyspark-complete.md`)
- **SQL Server Adaptive Joins, Memory Grant Feedback**
- **Oracle Adaptive Plans**
- **Postgres pg_stat_plans**

Catches cardinality mis-estimations after an operator completes and chooses a better plan for the rest.

> **🧠 Principal Mindset:** The optimizer is the most complex single component in a database. Learning to read EXPLAIN plans + understand estimation errors is one of the highest-leverage skills in data engineering. Spend 10 hours on EXPLAIN; save 1000 hours of production incidents.

---

## 10. ACID Transactions

### 10.1 ACID Defined

- **Atomicity:** all-or-nothing. A transaction either commits fully or has no effect.
- **Consistency:** before and after a transaction, invariants hold (keys, constraints).
- **Isolation:** concurrent transactions don't interfere (modeled by isolation levels).
- **Durability:** committed data survives crashes.

### 10.2 Atomicity Implementation

Two primary mechanisms:
- **Undo log** (Oracle, InnoDB, SQL Server): record old values before modifying; rollback = replay undo in reverse.
- **Shadow paging** (SQLite with WAL mode disabled; LMDB): write to new pages; atomic pointer swap on commit; rollback = discard new pages.

Both plus WAL for durability.

### 10.3 Consistency

Enforced by:
- Constraints (PRIMARY KEY, FOREIGN KEY, CHECK, NOT NULL, UNIQUE)
- Triggers
- Application code

If a transaction would violate a constraint, it's aborted. "Consistency" in ACID is loosely defined; usually means invariant preservation.

### 10.4 Isolation Implementation

Via **concurrency control** (next section): locking (2PL) or multi-versioning (MVCC).

### 10.5 Durability Implementation

Via WAL + fsync. Covered in section 4.

### 10.6 Transaction Lifecycle

```
BEGIN;                      -- allocate txn ID, start timestamp
    SELECT ...              -- acquire read locks or MVCC snapshot
    UPDATE ...              -- acquire write locks; log undo
    INSERT ...              -- write WAL
COMMIT;                     -- flush WAL; release locks; mark txn committed
   -- or
ROLLBACK;                   -- undo via undo log; release locks
```

### 10.7 Transaction Primitives

```sql
BEGIN;                        -- or START TRANSACTION
SAVEPOINT sp1;                -- intermediate point
ROLLBACK TO SAVEPOINT sp1;
RELEASE SAVEPOINT sp1;
COMMIT;
ROLLBACK;
SET TRANSACTION ISOLATION LEVEL REPEATABLE READ;
```

### 10.8 Autocommit

By default, most clients commit after each statement (autocommit=on). To group statements into a transaction, explicitly BEGIN.

Some DBs default differently:
- MySQL CLI: autocommit=on
- PostgreSQL psql: autocommit=on
- JDBC: autocommit=on
- Python psycopg2: autocommit=OFF by default — remember to commit!

### 10.9 Transaction Durability vs Replication

ACID guarantees local durability. For geographic durability (DC failure):
- **Synchronous replication:** commit waits for N replicas to ack. Strong durability; higher latency.
- **Asynchronous replication:** commit returns immediately; replicas catch up later. Fast; some loss window on primary failure.
- **Quorum replication:** commit when majority ack (3/5 or 2/3). Balance.

---

## 11. Concurrency Control (2PL, MVCC)

### 11.1 The Concurrency Problem

Without concurrency control, concurrent transactions can produce incorrect results:
- **Lost update:** two transactions read then write the same row; one overwrites the other.
- **Dirty read:** read uncommitted data; the writer might roll back.
- **Non-repeatable read:** same query twice returns different results.
- **Phantom read:** range query returns different rows across runs.

### 11.2 Two-Phase Locking (2PL)

Each transaction follows two phases:
1. **Growing phase:** acquire locks; never release.
2. **Shrinking phase:** release locks; never acquire.

**Strict 2PL** (standard): all locks released only at commit/abort. Prevents cascading aborts.

Lock types:
- **Shared (S):** multiple readers OK, no writers.
- **Exclusive (X):** sole access.

Compatibility matrix:
```
          S    X
    S  [ OK  NO ]
    X  [ NO  NO ]
```

**Deadlock:** T1 holds lock on A, waits for B; T2 holds B, waits for A. Resolution: deadlock detector picks victim and rolls back.

### 11.3 Lock Granularity

- Row-level: fine; high concurrency; more lock overhead.
- Page-level: medium; some false conflicts (two txns touch different rows on same page).
- Table-level: coarse; low concurrency; low overhead.

Modern RDBMS use row-level by default + lock escalation to page/table under memory pressure (SQL Server).

### 11.4 Intent Locks (Multi-granularity)

To coordinate between row and table locks, DBs use **intent locks**:
- IS (Intent Shared): "I plan to take S on child."
- IX (Intent Exclusive): "I plan to take X on child."
- SIX (Shared + Intent Exclusive).

Lock a table in IX before locking individual rows in X. Enables other txns to safely take S or IS on different rows.

### 11.5 Multi-Version Concurrency Control (MVCC)

MVCC replaces locks for reads: each row keeps multiple VERSIONS. Readers see a consistent snapshot without blocking writers.

**Mechanism:**
- On update, don't modify in place — create a NEW version with the new value.
- Each version has (txn_id_created, txn_id_deleted).
- Reader at snapshot time T sees versions where (created ≤ T < deleted) and (creator was committed by T).

**Writers still take locks** on rows they modify (to prevent lost updates).

**Snapshot isolation** is MVCC's natural isolation level.

### 11.6 PostgreSQL MVCC

Postgres uses MVCC with:
- Each row has `xmin` (creating txn) and `xmax` (deleting txn) columns (hidden).
- UPDATE = INSERT new row + mark old row's xmax.
- Readers see rows where xmin visible + xmax not visible to their snapshot.
- Dead rows accumulate → VACUUM reclaims them.

**Postgres VACUUM** is NON-NEGOTIABLE for production. Without it, dead rows pile up (table + index bloat); eventually txn ID wraparound disaster.

### 11.7 InnoDB MVCC

InnoDB stores:
- Current row in the data pages
- Older versions in the **undo log**
- On read: start from current row, traverse undo chain backward to find version visible at your snapshot time

Undo log is purged in background by the "purge thread."

### 11.8 MVCC Trade-offs

Pros:
- Readers never block writers (great for mixed OLTP + reporting)
- Snapshot isolation is natural
- Point-in-time queries easy

Cons:
- Storage overhead (multiple versions)
- Need periodic cleanup (VACUUM, purge thread, compaction)
- Complex visibility logic

### 11.9 Deadlock Detection

DBs maintain a wait-for graph. If a cycle exists, pick a victim (youngest txn, or the one holding fewest locks) and roll it back. Victim retries.

```sql
-- Postgres: automatic; see pg_stat_activity for blocking
SELECT * FROM pg_locks WHERE NOT granted;

-- MySQL: 
SHOW ENGINE INNODB STATUS;   -- shows last deadlock
```

Applications should EXPECT deadlock errors and retry with exponential backoff.

### 11.10 Optimistic vs Pessimistic

- **Pessimistic:** take locks preemptively. Default in 2PL.
- **Optimistic:** proceed without locks; on commit, check for conflicts; abort if conflict detected. Used in some NoSQL + distributed systems (Cassandra LWT, FoundationDB).

> **🎯 Interview Angle — "Explain MVCC."** Multiple versions of each row coexist; writers create new versions, readers see snapshots. Advantages: readers don't block writers; snapshot isolation natural; point-in-time queries easy. Cost: storage for old versions + cleanup process (Postgres VACUUM, InnoDB purge). Nearly all modern OLTP databases (Postgres, Oracle, SQL Server since 2005, InnoDB, Cockroach, Spanner) use MVCC.

---

## 12. Isolation Levels & Phenomena

### 12.1 The SQL Standard Isolation Levels

The ANSI SQL standard defines four isolation levels and three phenomena:

| Level | Dirty read | Non-repeatable read | Phantom read |
|-------|-----------|---------------------|--------------|
| Read Uncommitted | POSSIBLE | POSSIBLE | POSSIBLE |
| Read Committed | prevented | POSSIBLE | POSSIBLE |
| Repeatable Read | prevented | prevented | POSSIBLE (ANSI) / prevented (SI) |
| Serializable | prevented | prevented | prevented |

The standard is historical and imprecise. Actual behavior varies by implementation.

### 12.2 The Phenomena

**Dirty read:** read uncommitted data from another txn.
```
T1: UPDATE accounts SET balance = balance - 100 WHERE id = 1;   -- uncommitted
T2: SELECT balance FROM accounts WHERE id = 1;                    -- sees -100
T1: ROLLBACK;                                                     -- T2 saw never-existing data
```

**Non-repeatable read:** same row read twice returns different values.
```
T1: SELECT balance FROM accounts WHERE id = 1;   -- 500
T2: UPDATE accounts SET balance = 400 WHERE id = 1; COMMIT;
T1: SELECT balance FROM accounts WHERE id = 1;   -- 400 (changed!)
```

**Phantom read:** same range query returns different sets of rows.
```
T1: SELECT COUNT(*) FROM orders WHERE date = '2024-10-19';   -- 10
T2: INSERT INTO orders ... (date = '2024-10-19'); COMMIT;
T1: SELECT COUNT(*) FROM orders WHERE date = '2024-10-19';   -- 11 (phantom!)
```

### 12.3 Additional Anomalies (Beyond ANSI)

**Write skew (snapshot isolation anomaly):**
Two txns read a shared set, each writes a disjoint subset, both commit — but together violate a constraint.

```
Rule: at least 1 doctor on call at any time.
Current: Alice and Bob both on call.

T1: SELECT COUNT(*) WHERE on_call = TRUE;  -- 2
T1: UPDATE SET on_call = FALSE WHERE name = 'Alice';  -- commits
T2: SELECT COUNT(*) WHERE on_call = TRUE;  -- 2 (its snapshot)
T2: UPDATE SET on_call = FALSE WHERE name = 'Bob';    -- commits
Result: 0 doctors on call. Constraint violated.
```

Snapshot isolation allows this; serializable does not.

**Lost update:**
T1 reads balance=100, T2 reads balance=100, both write balance+=10, both commit → final balance = 110 (should be 120).

### 12.4 Isolation Levels Per Database

| DB | Default | Available |
|----|---------|-----------|
| **Postgres** | Read Committed | RC, RR (= Snapshot Isolation), Serializable (SSI) |
| **MySQL InnoDB** | Repeatable Read | RU, RC, RR, Serializable |
| **SQL Server** | Read Committed | RU, RC, RC Snapshot, RR, Snapshot, Serializable |
| **Oracle** | Read Committed | RC, Serializable (actually Snapshot Isolation) |
| **SQLite** | Serializable | RU, Serializable |

### 12.5 Snapshot Isolation (SI)

Each transaction reads from a snapshot taken at start. Writes conflict only if two txns write the same row.

- Does NOT prevent write skew
- Does NOT prevent phantom reads for predicates not reflected in writes
- Fast (uses MVCC)

Postgres `REPEATABLE READ` = SI.
Oracle `SERIALIZABLE` = SI (misnomer!).

### 12.6 Serializable Snapshot Isolation (SSI)

Postgres's actual Serializable: SI + runtime conflict detection. Rolls back transactions that would violate serializability.

- Prevents write skew.
- Some overhead; may abort transactions.

### 12.7 Choosing an Isolation Level

- **Read Committed:** most apps; default; no dirty reads.
- **Repeatable Read / Snapshot:** for queries that need consistent snapshots.
- **Serializable:** for correctness-critical txns (financial transfers, inventory).

**Rule of thumb:** start at Read Committed; upgrade specific transactions to Serializable or implement application-level locking (e.g., `SELECT ... FOR UPDATE`).

### 12.8 Explicit Locking

```sql
-- Lock rows against concurrent updates (pessimistic)
SELECT * FROM accounts WHERE id = 1 FOR UPDATE;         -- exclusive
SELECT * FROM accounts WHERE id = 1 FOR SHARE;          -- shared
SELECT * FROM accounts WHERE id = 1 FOR UPDATE NOWAIT;  -- fail if locked
SELECT * FROM accounts WHERE id = 1 FOR UPDATE SKIP LOCKED;  -- skip locked rows

-- Advisory locks (application-level)
SELECT pg_advisory_lock(123);
SELECT pg_advisory_unlock(123);
```

### 12.9 Isolation Interview Patterns

> **🎯 Interview Angle — "What's the difference between Repeatable Read and Serializable?"** Repeatable Read (Snapshot Isolation in practice) prevents dirty reads + non-repeatable reads but allows WRITE SKEW: two txns read a shared set, write disjoint subsets, both commit, together violating a constraint. Serializable prevents write skew by detecting conflicts and aborting at least one txn. Postgres implements true Serializable via SSI; many DBs' "Serializable" is actually SI.

> **🎯 Interview Angle — "How do you prevent lost updates?"**
> 1. `SELECT ... FOR UPDATE` to pessimistically lock the row.
> 2. `UPDATE ... WHERE version = ?` (optimistic — check version column).
> 3. Serializable isolation + retry on failure.
> 4. Atomic in-place update: `UPDATE ... SET x = x + 1` (no read-then-write).

---

## 13. Database Partitioning

### 13.1 What is Partitioning?

Divide a table into smaller physical chunks (partitions) based on a column value. Each partition is a separate storage unit.

**Benefits:**
- Partition pruning: queries filter the relevant partition
- Easier maintenance (drop/truncate old partitions)
- Parallel scans
- Storage tiering (hot/warm/cold)

**Not for horizontal scaling** — all partitions still live on one database instance (that's sharding).

### 13.2 Partitioning Strategies

**Range partitioning:**
```sql
CREATE TABLE events (ts timestamp, data jsonb) 
    PARTITION BY RANGE (ts);
    
CREATE TABLE events_2024_10 PARTITION OF events 
    FOR VALUES FROM ('2024-10-01') TO ('2024-11-01');
```
- Great for time-series, dates
- Easy archival (drop old partition)

**List partitioning:**
```sql
CREATE TABLE orders (country text, amount numeric)
    PARTITION BY LIST (country);
    
CREATE TABLE orders_us PARTITION OF orders FOR VALUES IN ('US');
CREATE TABLE orders_eu PARTITION OF orders FOR VALUES IN ('UK', 'FR', 'DE');
CREATE TABLE orders_other PARTITION OF orders DEFAULT;
```

**Hash partitioning:**
```sql
CREATE TABLE users (id bigint, data jsonb) 
    PARTITION BY HASH (id);
    
CREATE TABLE users_0 PARTITION OF users FOR VALUES WITH (modulus 4, remainder 0);
CREATE TABLE users_1 PARTITION OF users FOR VALUES WITH (modulus 4, remainder 1);
CREATE TABLE users_2 PARTITION OF users FOR VALUES WITH (modulus 4, remainder 2);
CREATE TABLE users_3 PARTITION OF users FOR VALUES WITH (modulus 4, remainder 3);
```
- Even distribution (good)
- No partition pruning for range queries

**Composite / Sub-partitioning:**
Range by date THEN hash by user_id, etc.

### 13.3 Partition Pruning

The optimizer uses WHERE clause to skip partitions:

```sql
-- events partitioned by month
SELECT * FROM events WHERE ts >= '2024-10-01' AND ts < '2024-10-15';
-- Only reads events_2024_10 partition
```

Verify in EXPLAIN:
```
Append
  Subplans Removed: 11   ← 11 partitions pruned
  -> Seq Scan on events_2024_10
```

### 13.4 Partition-Wise Join

If both tables are partitioned the SAME WAY, the optimizer can join partition-by-partition:
```sql
ALTER SYSTEM SET enable_partitionwise_join = ON;
```

Each pair of matching partitions joins independently → parallelism + no cross-partition shuffle.

### 13.5 Vertical Partitioning

Split a table by COLUMNS instead of rows. Keep hot columns in one table, cold in another:
```sql
CREATE TABLE users_core (id, email, name);
CREATE TABLE users_profile (id, bio, avatar_url, preferences_json);
```

JOIN on id. Benefits: narrower rows → more rows per page → faster scans.

### 13.6 Local vs Global Indexes

- **Local index:** per-partition. Independent; good for pruned queries.
- **Global index:** across all partitions. Required for uniqueness across partitions, but expensive to maintain and precludes some partition operations (DROP PARTITION invalidates).

Postgres: local only. Oracle, SQL Server: both.

### 13.7 Partition Management

```sql
-- Add new partition
CREATE TABLE events_2024_11 PARTITION OF events FOR VALUES FROM ('2024-11-01') TO ('2024-12-01');

-- Drop old partition (instant, no row deletion)
DROP TABLE events_2023_10;

-- Detach without dropping
ALTER TABLE events DETACH PARTITION events_2023_10;
```

### 13.8 Interview Patterns

> **🎯 Interview Angle — "When would you partition a table?"**
> Rules of thumb: table > 100M rows, queries filter on a specific column (date, country), data has lifecycle (old data archived), parallelism from partition scans helps. Partitioning adds management overhead; don't use for small tables.

---

## 14. Database Sharding

### 14.1 Sharding = Horizontal Scale-Out

Partitioning splits a table within one DB instance. **Sharding** splits data across multiple DB instances (nodes/servers). Also called "horizontal scaling."

Benefits:
- Scales storage + CPU + throughput beyond one machine
- Fault isolation (one shard failure ≠ total outage)

Costs:
- Cross-shard queries hard
- Transactions across shards slow (2PC)
- Rebalancing complexity
- Hot shards possible

### 14.2 Sharding Strategies

**Range sharding:**
Shard = range of key values. Easy for ordered data but hot shards if traffic skewed to recent data.

**Hash sharding:**
`shard_id = hash(key) % N`. Even distribution. Range queries hard.

**Directory-based / lookup:**
Explicit mapping table from key → shard. Flexible, but lookup is overhead.

**Consistent hashing:**
Place shards on a hash ring. Adding/removing a shard only moves a small fraction of data. Standard for distributed caches + NoSQL (Cassandra, Dynamo, memcached).

### 14.3 Consistent Hashing

```
Hash ring (0..2^160):

      shard_A (hash=100)
         |
         |
   shard_C (hash=1500)
         |
   shard_B (hash=800)

Key K with hash(K)=1200 → stored on shard_C (next shard clockwise)
```

Adding a shard between A and B at hash=400:
- Only keys with hash 100..400 move (from B to new shard)
- Other data untouched

**Virtual nodes:** each physical shard owns multiple ring positions → smoother distribution + resharding.

### 14.4 Hot Shards

One shard gets disproportionate traffic → bottleneck.

Causes:
- Celebrity user (one key = 10% of writes)
- Trending content
- Poor shard key choice (`created_at` → all new writes to one shard)

Fixes:
- Composite shard key (`user_id + hour_bucket`)
- Pre-split hot keys
- Dedicated shard for celebrities
- Caching layer

### 14.5 Cross-Shard Queries

**Fan-out:** issue the query to all shards; aggregate on application.
- Simple but slow (bound by slowest shard = tail latency)
- Can't do joins without extra work

**Scatter-gather:** application sends N queries, merges results.

**Co-location:** shard related tables by the SAME key (user_orders + user_profile both sharded by user_id → joins within shard).

**Denormalization:** replicate common data to all shards (star schema dim tables).

### 14.6 Rebalancing (Resharding)

Adding/removing shards requires moving data. Challenges:
- Must happen without downtime
- Range scans continue during move
- Writes during move must land correctly

Approach: dual-write to old + new shards; read from old; backfill; switch reads; stop old writes.

Cassandra/Scylla do this via streaming + token range transfer.

### 14.7 Transaction Patterns Across Shards

- **Single-shard transaction:** fast, uses local ACID.
- **Cross-shard transaction:** requires 2PC or Saga pattern (see section 16).

Design for single-shard transactions whenever possible (choose shard key accordingly).

### 14.8 Shard-per-Tenant

For multi-tenant SaaS:
- **Shared schema:** all tenants in one table, filtered by `tenant_id`. Simple.
- **Shared DB, isolated schema:** each tenant has own schema. Isolation but limited scale.
- **DB per tenant:** full isolation; expensive at scale.
- **Shard per tenant group:** tenants hashed into shards. Scale + reasonable isolation.

### 14.9 Automated Sharding

- **MongoDB:** auto-shards collections by shard key; config servers track chunks.
- **Cassandra:** token-ring (consistent hashing).
- **Vitess (MySQL):** shards MySQL transparently; app sees one DB.
- **Citus (PostgreSQL):** sharding extension; co-located distributed tables.
- **CockroachDB / YugabyteDB / Spanner:** auto-sharded + distributed transactions.
- **PlanetScale:** managed Vitess.

### 14.10 Sharding Mistakes

1. **Wrong shard key.** Picking a column with low cardinality (e.g., status) = few shards get all traffic.
2. **Shard key based on time** (`created_at`) = hot shard problem (all new writes to one shard).
3. **No plan for resharding.** Start small, data grows, find out you can't easily split a shard.
4. **Cross-shard transactions in common path.** Choose keys to make common txns single-shard.
5. **Not monitoring shard imbalance.** 90-10 distribution is common.

### 14.11 Vertical vs Horizontal Scale

Before sharding, exhaust vertical scaling:
- Bigger machines (CPU, RAM, NVMe)
- Read replicas for read-heavy
- Caching (Redis in front)
- Query/index optimization
- Partitioning (within one DB)

Sharding is OPERATIONAL COMPLEXITY. Delay as long as feasible.

---

## 15. Database Replication

### 15.1 Why Replicate?

- **High availability:** replica takes over if primary dies.
- **Read scaling:** route reads to replicas; writes to primary.
- **Geographic distribution:** low-latency reads near users.
- **Backups / analytics:** run on replica without impacting primary.

### 15.2 Replication Topologies

**Primary-Replica (Leader-Follower):**
- One primary accepts writes
- N replicas receive updates from primary
- Reads to primary or replicas

**Multi-Primary (Multi-Leader):**
- Multiple nodes accept writes
- Conflict resolution needed
- Rare (CouchDB, Cassandra, some MySQL setups)

**Primary-Primary (Active-Active):**
- Two or more nodes; writes go to any
- Needs conflict resolution (CRDTs or application logic)

### 15.3 Replication Modes

**Synchronous:**
- Primary waits for replica to ack before commit returns
- Zero data loss on primary failure (if replica up to date)
- Latency = primary + replica + network

**Asynchronous:**
- Primary commits locally; replicates in background
- Fast
- Data loss window on primary failure
- Replica lag possible

**Semi-synchronous (MySQL):**
- Wait for ≥1 replica to receive (not apply) the log; others async
- Balance of safety and speed

**Quorum (Postgres synchronous_commit = `remote_write`, `on`, `remote_apply`):**
- Variations on when replica acks (received, written to disk, applied)

### 15.4 Replication Methods

**Physical / Statement / Row-based:**

- **Statement-based:** replicate SQL statements. Simple but non-deterministic (NOW(), random, triggers).
- **Row-based:** replicate individual row changes. Deterministic. Larger log but reliable. MySQL binlog ROW format, Postgres physical replication.
- **Logical:** replicate at the row-event level with schema awareness. Can replicate between different major versions, filter tables, transform. Postgres logical replication, Debezium.

### 15.5 PostgreSQL Replication

**Streaming replication (physical):**
- Primary streams WAL to standby
- Standby replays WAL in real-time
- Hot standby = read-only queries during replay
- Same major version required

**Logical replication:**
- Publication on primary (what to replicate)
- Subscription on replica (what to receive)
- Row-level events; replica can have different schema
- Can replicate between different major versions

```sql
-- Primary
CREATE PUBLICATION my_pub FOR TABLE orders, customers;

-- Replica
CREATE SUBSCRIPTION my_sub CONNECTION '...' PUBLICATION my_pub;
```

### 15.6 MySQL Replication

- Primary writes binlog
- Replica's IO thread fetches binlog, writes to relay log
- Replica's SQL thread applies relay log
- Binlog formats: STATEMENT, ROW, MIXED

**GTID (Global Transaction ID):** each txn gets a globally-unique ID; failover simpler.

### 15.7 Replica Lag

Async replication → replica is behind primary. Lag spike:
- Big transactions (bulk update)
- Replication single-threaded (classical MySQL; fixed in multi-threaded in 5.7+)
- Replica resource exhaustion

Monitor:
```sql
-- Postgres
SELECT pg_wal_lsn_diff(pg_current_wal_lsn(), replay_lsn) AS lag_bytes
FROM pg_stat_replication;

-- MySQL
SHOW REPLICA STATUS;    -- Seconds_Behind_Source
```

### 15.8 Read-Your-Writes Consistency

Classic replica lag issue: user writes, immediately reads from replica, sees old data.

Fixes:
- Route the user's reads to primary for N seconds after their write
- Track per-user LSN; route to a replica caught up to that LSN
- Session consistency: read-your-writes within session

### 15.9 Failover

- **Manual:** operator promotes replica.
- **Automatic:** tools like Patroni (Postgres), Orchestrator (MySQL), MongoDB primary election.
- **Split-brain risk:** two nodes think they're primary → data divergence. Mitigated by fencing (STONITH), quorum requirements.

### 15.10 Postgres Patroni / Replica Failover

Patroni + etcd/Consul/Zookeeper:
- Patroni on each node
- etcd stores cluster state
- Automatic primary election
- Health checks + failover
- Read replicas registered in service discovery

### 15.11 Replication for Analytics

- Dedicated reporting replica avoids OLTP impact
- Can use different storage engine (e.g., ClickHouse-style column store replica)
- Or logical replication to a lakehouse (Debezium → Kafka → Delta/Iceberg)

---

## 16. Distributed Transactions (2PC, Saga)

### 16.1 The Problem

A single transaction needs to update data on multiple DB instances. Local ACID doesn't help; need distributed coordination.

Example: payment service debits bank DB; order service increments order DB. Either both or neither.

### 16.2 Two-Phase Commit (2PC)

```
COORDINATOR          PARTICIPANT 1         PARTICIPANT 2
   │                     │                      │
   │─── PREPARE ────────▶│                      │
   │                     │ (locks + prepare)    │
   │◀─── OK ─────────────│                      │
   │─── PREPARE ──────────────────────────────▶│
   │                                           │
   │◀─── OK ───────────────────────────────────│
   │                     │                      │
   │─── COMMIT ─────────▶│                      │
   │                     │ (commits + unlocks)  │
   │─── COMMIT ──────────────────────────────▶│
   │                                           │
   │◀─── OK ─────────────│                      │
   │◀─── OK ───────────────────────────────────│
```

**Phase 1 (Prepare):** coordinator asks each participant to prepare (locks + checks). All say OK or ABORT.
**Phase 2 (Commit):** if all OK, coordinator says commit. All ack. If any abort, coordinator says rollback.

### 16.3 2PC Failure Modes

- Coordinator fails before phase 2: participants stuck holding locks (blocking!).
- Participant fails after prepare: coordinator times out, aborts.
- Network partition: participants may hold locks indefinitely.

**2PC is BLOCKING** in failure cases. In practice, needs timeouts + manual intervention OR a highly-available coordinator.

### 16.4 Three-Phase Commit (3PC)

Adds a PRE-COMMIT phase to avoid blocking in some failure cases. Still has edge cases; rarely used in practice.

### 16.5 Saga Pattern

For long-running distributed transactions, 2PC is too locking-heavy. **Saga:**

1. Break the transaction into a sequence of local transactions.
2. Each local transaction has a corresponding COMPENSATING transaction (undo logic).
3. On failure at step K, run compensating transactions for steps K-1, K-2, ..., 1.

```
Book flight    →  Book hotel   →  Book rental car
     │                 │                  │
Cancel flight  ←  Cancel hotel  ←         ↓ (fail)
                                   Cancel both
```

**Properties:**
- No locking across services
- Eventually consistent (not atomic — intermediate states observable)
- Compensation ≠ perfect rollback (irreversible side effects possible)

### 16.6 Choreography vs Orchestration

- **Choreography:** each service reacts to events; emits events for next service. Decentralized. Hard to reason about.
- **Orchestration:** central orchestrator calls each service; manages state. Easier to monitor; central point of complexity.

Tools: Temporal (orchestration), Zeebe, Camunda.

### 16.7 Outbox Pattern

To reliably publish events alongside DB commits:
1. In the DB transaction, also INSERT into an `outbox` table.
2. A separate process reads `outbox` and publishes to message queue.
3. Message queue consumers do their part.

Why: avoid "dual writes" problem (writing to DB AND Kafka in one logical operation isn't atomic without this trick).

### 16.8 Distributed Transactions in Modern DBs

- **Spanner, CockroachDB, YugabyteDB:** external consistency via TrueTime (Spanner) or hybrid logical clocks (Cockroach). Full ACID across shards.
- **TiDB:** Percolator-style distributed transactions on top of TiKV.
- **FoundationDB:** optimistic concurrency with transparent sharding.

For OLTP at hyper-scale with strong consistency, these are the modern options vs sharding + Saga.

### 16.9 Interview Patterns

> **🎯 Interview Angle — "2PC vs Saga."** 2PC: atomic distributed commit via blocking coordination. Works for short, same-system transactions. Blocks on coordinator failure; low availability. Saga: sequence of local transactions + compensations; eventual consistency; non-blocking; intermediate states visible. For microservices with disparate systems, Saga wins. For same-DB cross-shard transactions, modern distributed DBs (Spanner, Cockroach) are better than 2PC.

---

## 17. Consensus (Raft, Paxos)

### 17.1 Why Consensus?

In a distributed system, nodes must agree on:
- Primary election (which node is leader?)
- Order of writes (what's the official log?)
- Membership changes (who's in the cluster?)

Consensus algorithms let a set of nodes agree on a value even if some nodes fail or messages are delayed.

### 17.2 FLP Impossibility

Fischer-Lynch-Paterson (1985): no deterministic consensus algorithm can guarantee progress in an asynchronous system with even one faulty node. Practical consensus relies on timing assumptions.

### 17.3 Paxos

Leslie Lamport, 1989 (published 1998). Notoriously hard to understand.

Phases:
1. **Prepare:** proposer sends proposal number N; acceptors ack if N > any seen before.
2. **Accept:** proposer sends (N, value); acceptors accept if N still highest.
3. **Decided:** value chosen when majority accepts.

Variants: Multi-Paxos, Fast Paxos, Flexible Paxos, Egalitarian Paxos.

### 17.4 Raft

Diego Ongaro, 2014. Designed for understandability. Standard choice today.

**Roles:**
- **Leader:** handles all writes
- **Followers:** passively replicate
- **Candidate:** during election

**Mechanism:**
- Followers vote for a candidate during election
- Winner (majority votes) becomes leader
- Leader appends to its log and replicates to followers
- Entries "committed" when majority have written to log
- Leader applies committed entries to state machine
- Heartbeats keep leader alive; silence triggers election

**Guarantees:**
- Election Safety: at most one leader per term
- Leader Append-Only: leader only appends to its log
- Log Matching: logs with same (index, term) are identical before that point
- Leader Completeness: committed entries survive leader change
- State Machine Safety: all nodes apply the same sequence

### 17.5 Raft in Production

- **etcd** (Kubernetes config): Raft-based KV store
- **Consul:** Raft for consensus
- **CockroachDB, TiKV, YugabyteDB:** Raft per range
- **MongoDB** (replica set election): Raft-inspired
- **Patroni** (Postgres HA): uses etcd/Consul/ZooKeeper for Raft/Paxos-backed state

### 17.6 ZAB (Zookeeper Atomic Broadcast)

Zookeeper's protocol. Similar to Multi-Paxos. ZooKeeper is used for coordination in many systems (Hadoop, Kafka pre-KRaft, HBase).

### 17.7 KRaft (Kafka's new consensus)

Kafka moved from ZooKeeper to self-managed KRaft (Kafka Raft) in 3.3+. Removes Zookeeper dependency.

### 17.8 Quorum Reads/Writes

For N replicas, R = read quorum, W = write quorum:
- `W + R > N` → read sees all writes (strong consistency)
- `W = N` → every write to every replica (slow)
- `W = 1` → fastest writes; need `R = N` to see writes

Cassandra/Dynamo: tune per-query.
Typical: N=3, W=2, R=2 (one node can fail, still safe).

### 17.9 Interview Patterns

> **🎯 Interview Angle — "Explain Raft."** Raft is a consensus algorithm for a cluster to agree on a replicated log. One node is the leader (elected via votes); all writes go through it. Leader appends entries to its log and replicates to followers. An entry is "committed" once a majority (quorum) have it. If the leader dies, a follower starts an election; needs majority votes to become new leader. Guarantees: at most one leader per term, committed entries survive leader change, all nodes apply the same sequence. Used by etcd, Consul, CockroachDB, TiKV, Kafka KRaft.

---

## 18. Database Engines Compared

### 18.1 Row-Oriented OLTP Engines

- **InnoDB** (MySQL default): B+Tree clustered, MVCC via undo logs, foreign keys, crash-safe, row-level locking.
- **MyISAM** (MySQL legacy): no transactions, no FK, table-level locks. Fast reads for append-only. Deprecated.
- **PostgreSQL heap:** heap tables, MVCC via row versions, no clustered index concept (indexes point to TIDs).
- **SQLite:** B+Tree per table, WAL mode, serializable isolation, embedded.
- **Oracle Database:** B+Tree, undo tablespace, read-consistency via MVCC, extensive features.
- **SQL Server:** B+Tree, snapshot isolation (opt-in since 2005), extensive OLTP features.

### 18.2 Columnar Engines

- **ClickHouse MergeTree:** append-only columnar, LSM-style merges, sparse primary index.
- **Snowflake micro-partitions:** immutable columnar chunks, metadata catalog.
- **BigQuery Capacitor:** columnar + Dremel-style distributed execution.
- **Databricks Delta:** Parquet + transaction log, time travel, MERGE.
- **DuckDB:** vectorized in-process analytics; see `../bigdata-notes-flat/10-duckdb.md`.

### 18.3 LSM-Based Engines

- **RocksDB:** C++ LSM; embedded; most popular LSM engine.
- **LevelDB:** Google predecessor of RocksDB.
- **MyRocks:** MySQL + RocksDB storage engine. Facebook built this.
- **Cassandra:** distributed LSM, Dynamo-inspired.
- **ScyllaDB:** C++ Cassandra rewrite, shard-per-core.
- **MongoDB WiredTiger (LSM mode):** supports both LSM and B+Tree.

### 18.4 In-Memory Engines

- **Redis:** data structure server, single-threaded command loop + I/O multiplexing.
- **Memcached:** cache KV.
- **MemSQL / SingleStore:** in-memory row + columnar for analytics.
- **SAP HANA:** in-memory + persistent, column store.

### 18.5 Specialized Engines

- **TimescaleDB:** PostgreSQL + time-series extensions (hypertables).
- **InfluxDB:** time-series native (TSM engine).
- **Elasticsearch:** inverted-index full-text search; built on Lucene.
- **Neo4j:** graph database.
- **FoundationDB:** ordered KV with transactions; building block for layered DBs (Snowflake, iCloud).

### 18.6 Choosing an Engine

| Need | Engine |
|------|--------|
| OLTP (classic) | PostgreSQL, MySQL (InnoDB) |
| OLTP at scale | CockroachDB, YugabyteDB, Spanner |
| Mixed OLTP+analytics | PostgreSQL + Citus, TiDB |
| Pure analytics | ClickHouse, DuckDB, Snowflake, BigQuery, Databricks |
| Write-heavy KV | RocksDB, Cassandra |
| Cache | Redis, Memcached |
| Full-text search | Elasticsearch, Postgres FTS |
| Time-series | TimescaleDB, InfluxDB, ClickHouse |
| Graph | Neo4j, TigerGraph, Postgres pgRouting |
| Document | MongoDB, Postgres JSONB |
| Embedded | SQLite, DuckDB |

---

## 19. Database Cursors

### 19.1 What is a Cursor?

A **cursor** is a pointer into a query result set, allowing row-by-row traversal rather than fetching all rows at once.

```sql
-- Postgres
BEGIN;
DECLARE mycursor CURSOR FOR SELECT * FROM huge_table WHERE ...;
FETCH 100 FROM mycursor;      -- next 100 rows
FETCH 100 FROM mycursor;
CLOSE mycursor;
COMMIT;
```

### 19.2 Why Use Cursors?

- **Large result sets:** can't fit in memory on the client.
- **Streaming processing:** process row-by-row without materializing all.
- **Interactive navigation:** scrollable cursors for UI pagination.

### 19.3 Server-Side vs Client-Side Cursors

- **Client-side:** server sends all rows; client holds them. Simple; memory-heavy; higher latency.
- **Server-side:** server holds result set; client fetches batches. Memory-light; keeps a server-side transaction/connection open.

JDBC: `setFetchSize(1000)` on PreparedStatement switches to server-side-ish streaming (depends on driver).

### 19.4 Cursor Types

- **Forward-only (default):** can only `FETCH NEXT`.
- **Scrollable:** can `FETCH PRIOR / FIRST / LAST / ABSOLUTE N / RELATIVE N`.
- **Holdable (WITH HOLD):** survives transaction boundary.
- **Insensitive:** sees snapshot at open time; later changes invisible.
- **Sensitive:** sees later changes to underlying data.

### 19.5 Common Pitfalls

- **Holding transactions open:** cursor requires an open transaction; long-running cursors block vacuum (Postgres) or hold locks.
- **N+1 queries:** cursor fetching one row at a time → many round trips. Batch fetch (`FETCH 1000`).
- **Scroll cursors on big results:** expensive; server must materialize or re-execute.

### 19.6 Alternatives to Cursors

- **Keyset pagination:** `WHERE id > ? ORDER BY id LIMIT 100` — fast, no cursor state.
- **OFFSET / LIMIT:** easy but slow for deep pages (OFFSET 1,000,000 scans + skips).
- **Server-side batch processing:** stored procedures that process rows in-DB.

### 19.7 Cursors in Application Frameworks

- **psycopg2 (Python):** server-side cursors via `conn.cursor(name='my_cursor')`.
- **JDBC:** `stmt.setFetchSize(1000)` + Postgres driver with autocommit=off.
- **SQLAlchemy:** `yield_per(100)` for streaming.

### 19.8 Interview Patterns

> **🎯 Interview Angle — "When would you use a cursor?"** When the result set is too large for memory AND you need to process it row-by-row (ETL, migrations, reporting). Modern alternatives (keyset pagination, batch processing in SQL, server-side streaming with fetchSize) are usually better. Cursors come with gotchas: open transactions, lock holds, server-side state.

---

## 20. NoSQL Architecture

### 20.1 NoSQL Taxonomy

- **Key-Value:** Redis, Memcached, DynamoDB (basic).
- **Document:** MongoDB, Couchbase, Firestore. JSON-ish documents.
- **Wide-column / Column-family:** Cassandra, HBase, ScyllaDB. Row key + flexible columns.
- **Graph:** Neo4j, Amazon Neptune, TigerGraph. Nodes + edges.
- **Time-series:** InfluxDB, TimescaleDB, Prometheus.
- **Search:** Elasticsearch, OpenSearch, Solr. Inverted index.
- **Vector:** Pinecone, Weaviate, Milvus, pgvector. Embedding similarity.

### 20.2 Why NoSQL?

Motivation (early 2010s):
- Relational DBs don't scale horizontally easily
- Rigid schemas slow iteration
- CAP constraints forced new designs

Reality by 2026:
- NewSQL (Cockroach, Spanner, TiDB) brings horizontal scale to RDBMS
- Postgres JSONB handles document use cases
- Many NoSQL systems added SQL interfaces + ACID (DynamoDB transactions, MongoDB multi-doc txns)
- The lines are blurred

### 20.3 Key-Value Stores

Simplest NoSQL. GET/PUT/DELETE by key.

- **Redis:** rich data structures (strings, lists, sets, sorted sets, hashes, streams), pub/sub, single-threaded command loop.
- **Memcached:** cache only; no persistence.
- **DynamoDB:** managed KV + document; auto-sharded; eventual + strong consistency options.
- **FoundationDB:** ordered KV + ACID transactions; building block.

### 20.4 Document Stores

- **MongoDB:** BSON documents; collections; indexes; sharded clusters; replica sets.
- **Couchbase:** memory-first + persistence; N1QL (SQL-like).
- **Firestore:** real-time sync; client SDKs for mobile.

Document schema example (MongoDB):
```json
{
  "_id": ObjectId("..."),
  "user_id": 123,
  "name": "Alice",
  "orders": [
    {"id": 1, "amount": 50},
    {"id": 2, "amount": 100}
  ]
}
```

### 20.5 Wide-Column (Cassandra-style)

```
Row key: user_123
  columns:
    timestamp:2024-10-19:event: "login"
    timestamp:2024-10-20:event: "purchase"
    timestamp:2024-10-21:event: "logout"
```

- Dynamic column names per row (not fixed schema)
- Great for time-series or sparse data
- Cassandra/Scylla: distributed via consistent hashing; multi-datacenter; eventual consistency

### 20.6 CAP Theorem

Eric Brewer, 2000. In a networked DB, you can have at most 2 of 3:
- **Consistency:** every read sees the most recent write
- **Availability:** every request gets a response
- **Partition tolerance:** system works despite network partitions

In the real world, partitions happen. So the real choice is C vs A during partition.

- **CP (consistency-preferring):** Spanner, HBase, MongoDB (configurable), Zookeeper
- **AP (availability-preferring):** DynamoDB (default), Cassandra, CouchDB

### 20.7 PACELC

Daniel Abadi's extension: when there's no Partition, you still face a trade-off between Latency and Consistency.

PACELC = During Partition: PA or PC; Else: EL or EC
- **PA/EL:** Dynamo-style (AP, prioritize latency normally)
- **PA/EC:** rare
- **PC/EL:** Spanner-ish (prioritize consistency during partition, latency normally)
- **PC/EC:** classical ACID DBs (Postgres primary)

### 20.8 Consistency Models

- **Strong (linearizable):** every operation appears to happen atomically at some point. Hardest. Spanner, Cockroach, Zookeeper.
- **Sequential:** operations ordered consistently across all nodes but not necessarily real-time.
- **Causal:** operations with cause-effect order preserved.
- **Eventual:** replicas converge given no new writes. Weakest.
- **Read-your-writes:** your own writes visible to you immediately.
- **Monotonic reads:** subsequent reads don't go backward in time.

NoSQL defaults vary. Pick based on requirements.

### 20.9 CRDTs (Conflict-Free Replicated Data Types)

Data structures where concurrent updates automatically converge without conflict:
- **G-Counter:** grow-only counter
- **PN-Counter:** +/- counter
- **G-Set:** grow-only set
- **OR-Set:** add/remove set with "last writer wins" semantics
- **LWW Register:** last-writer-wins value
- **Yjs / Automerge:** collaborative text editing

Used in CouchDB, Riak, Redis CRDTs, Figma, Google Docs.

### 20.10 Secondary Indexes

Hard in NoSQL (globally consistent secondary indexes on sharded data = expensive):
- **DynamoDB GSI:** eventually consistent; separate throughput.
- **Cassandra Secondary Indexes:** local to each shard; scatter-gather for queries. Slow for high-cardinality.
- **MongoDB:** secondary indexes maintained per shard.

Workarounds: application-managed indexes, materialized views, denormalize + duplicate.

### 20.11 When NoSQL

- Massive write throughput (Cassandra for events)
- Flexible/evolving schema (document stores for product catalogs)
- Geographic distribution with weak consistency OK (DynamoDB, Cassandra)
- Specialized workloads (graph, search, vector, time-series)

**When NOT NoSQL:**
- You need complex joins (RDBMS wins)
- Strong consistency across many entities (distributed SQL like Cockroach)
- Small-medium data (just use PostgreSQL)

---

## 21. Database Security

### 21.1 Authentication

Verify WHO you are.

- **Password:** username + password. Store as HASH (bcrypt, Argon2) — NEVER plaintext.
- **Certificate (mTLS):** client certificate validated by server CA.
- **OAuth / OIDC:** token-based; delegated to identity provider.
- **IAM-based (AWS, GCP):** cloud identity authenticates to DB.
- **Kerberos:** enterprise single sign-on.

Postgres `pg_hba.conf` configures auth per host/user/database.

### 21.2 Authorization

Verify WHAT you can do.

- **GRANT/REVOKE:** standard SQL permissions on tables, views, functions, schemas.
- **Roles:** group of permissions; users granted to roles.
- **RBAC (Role-Based Access Control):** standard pattern.
- **ABAC (Attribute-Based):** policies based on user/resource attributes.
- **Row-Level Security (RLS):** policies filter which rows a user sees.
- **Column-level permissions:** restrict access to specific columns.

```sql
-- Postgres RLS
ALTER TABLE orders ENABLE ROW LEVEL SECURITY;
CREATE POLICY tenant_policy ON orders FOR SELECT USING (tenant_id = current_setting('app.tenant_id')::int);
```

### 21.3 Encryption at Rest

- **Transparent Data Encryption (TDE):** encrypt data files at the storage layer. SQL Server, Oracle, MySQL Enterprise.
- **Full-disk / volume encryption:** cloud-provided (EBS KMS, GCS CMEK). Simpler.
- **Column-level encryption:** encrypt specific columns via application or DB features.

### 21.4 Encryption in Transit

- **TLS/SSL** for all connections (not optional in 2026).
- Postgres `ssl=on` in postgresql.conf.
- MySQL `require_secure_transport=ON`.

### 21.5 Audit Logging

Track who did what:
- Postgres `pgaudit` extension
- MySQL audit plugins
- SQL Server Audit
- Oracle Fine-Grained Auditing

Logs: connections, DDL, DML, queries, failures. Shipped to SIEM.

### 21.6 Data Masking

Hide sensitive data from unauthorized users:
- **Static masking:** replace values in dev/test copies (e.g., FAKE SSN).
- **Dynamic masking:** return masked data at query time based on user role.

Snowflake Dynamic Data Masking, Postgres `pgcrypto` for format-preserving encryption.

### 21.7 SQL Injection

Classic vulnerability: user input concatenated into SQL.

```python
# BAD
query = f"SELECT * FROM users WHERE id = {user_input}"

# GOOD: parameterized / prepared statements
cursor.execute("SELECT * FROM users WHERE id = %s", (user_input,))
```

Modern frameworks (SQLAlchemy, Hibernate) use parameterized queries by default. Still, review and lint.

### 21.8 Connection Security

- Use TLS
- Require strong authentication (passwords are weakest)
- Firewall: only allowed IPs/VPC
- Separate DB users per application (least privilege)
- Rotate credentials; use secret managers (AWS Secrets Manager, HashiCorp Vault)
- Use connection pooling (PgBouncer) to limit connection count

### 21.9 Backups + Security

- Encrypted backups
- Backup access controlled separately from main DB
- Test backup restore regularly (untested backups are no backups)

### 21.10 Compliance

- **PCI DSS:** credit card data
- **HIPAA:** healthcare data (US)
- **GDPR:** personal data of EU residents (RTBF requirements, see `../34-multi-region-disaster-recovery.md`)
- **SOC 2:** operational security
- **ISO 27001:** general infosec

Compliance affects: encryption requirements, access controls, audit logging, retention, cross-border data flow.

---

## 22. Homomorphic Encryption

### 22.1 What It Is

**Homomorphic Encryption (HE)** allows computation on encrypted data without decrypting it. The server operates on ciphertexts and returns an encrypted result that only the client can decrypt.

Example:
```
Client: encrypts [5, 10] → [E(5), E(10)]
Server: adds encrypted values → E(5) + E(10) = E(15)   (without knowing the values)
Client: decrypts result → 15
```

### 22.2 Types of HE

**Partially Homomorphic (PHE):** supports ONE operation.
- RSA, ElGamal: multiplication only
- Paillier: addition only
- Fast; limited.

**Somewhat Homomorphic (SHE):** supports addition + multiplication but limited depth (noise accumulates).

**Fully Homomorphic (FHE):** arbitrary computation on ciphertexts.
- Gentry's breakthrough (2009)
- BGV, BFV, CKKS schemes; libraries: SEAL (Microsoft), HElib, PALISADE, OpenFHE
- STILL very slow (operations 10,000x+ slower than plaintext)

### 22.3 Use Cases

- **Cloud computation on private data:** send encrypted data to cloud, cloud computes, you decrypt.
- **Private database queries:** query a DB without the server knowing what you asked.
- **Federated analytics:** multiple parties contribute encrypted data for joint analysis.
- **Private ML inference:** send encrypted features to model, get encrypted prediction.

### 22.4 Reality in 2026

- FHE is still ORDERS OF MAGNITUDE slower than plaintext
- Niche production uses: Microsoft Edge (encrypted search), some financial fraud detection, DARPA/ML research
- Not yet mainstream for databases

### 22.5 Alternatives for Encrypted-Query Needs

- **Trusted Execution Environments (TEE / Enclaves):** Intel SGX, AWS Nitro Enclaves. Data decrypted inside secure hardware.
- **Confidential Computing:** cloud-vendor-managed TEEs.
- **Secure Multi-Party Computation (MPC):** multiple parties jointly compute without revealing inputs.
- **Differential Privacy:** add noise to statistical results; preserve privacy of individuals.

### 22.6 Database Products

- **Microsoft SQL Server Always Encrypted:** column-level deterministic + randomized encryption; limited HE for equality queries.
- **MongoDB Field Level Encryption:** client-side encryption; server stores ciphertexts.
- **Partisia Blockchain, Zama:** FHE-based data processing products emerging.

### 22.7 Interview Patterns

> **🎯 Interview Angle — "Explain homomorphic encryption."** HE allows computation on encrypted data without decryption. Partial (PHE) supports one operation (RSA = multiply, Paillier = add). Fully (FHE) supports arbitrary; Gentry's 2009 breakthrough + modern schemes (BFV, CKKS). STILL slow (10,000x+ slower than plaintext) → niche use. For most "encrypted query" needs, TEEs/enclaves + confidential computing are more practical in 2026. Monitor; HE performance is improving; may become mainstream in the next decade.

---

## 23. System Design Patterns

### 23.1 Primary-Replica for Read Scaling

```
       WRITES
         │
         ▼
     [PRIMARY]
         │
    ┌────┼────┐
    ▼    ▼    ▼
  [R1]  [R2]  [R3]
    │    │    │
    ▼    ▼    ▼
       READS
```

Pros: simple, read scaling.
Cons: write bottleneck (one node); replica lag; read-your-writes challenge.

### 23.2 Multi-Primary / Active-Active

For low-latency writes in multiple regions:
```
     [US Primary] ◀───▶ [EU Primary]
          │                   │
          ▼                   ▼
       US USERS            EU USERS
```

Conflict resolution: last-writer-wins (LWW), CRDTs, application logic.

Hard to get right. Modern distributed DBs (Spanner, Cockroach) handle this transparently.

### 23.3 Sharded Primary-Replica

```
Shard 1: [Primary 1] + Replicas
Shard 2: [Primary 2] + Replicas
Shard 3: [Primary 3] + Replicas
...
```

Each shard has its own replication group. Scales both reads AND writes.

Used by: MongoDB sharded clusters, Vitess, Citus, DynamoDB.

### 23.4 Read-Through / Write-Through Cache

```
    App → Cache (Redis) → DB
          hit: return
          miss: fetch from DB; populate cache
    
    Writes: app → cache + DB (write-through) OR just DB + invalidate cache
```

Caching patterns:
- **Cache-aside (lazy):** app manages; populate on miss. Standard.
- **Write-through:** writes go to cache + DB synchronously.
- **Write-behind:** writes to cache; async flush to DB. Fast but risky.
- **Refresh-ahead:** proactive refresh of hot items.

### 23.5 Materialized Views

Pre-computed query results stored as a table; refreshed periodically or incrementally.

- **PostgreSQL `MATERIALIZED VIEW`:** manual refresh, `REFRESH MATERIALIZED VIEW CONCURRENTLY`.
- **Snowflake materialized views:** automatic incremental refresh.
- **Databricks Materialized Views:** auto-refresh via DLT.
- **Oracle Materialized Views:** rich; incremental.

Use when: expensive aggregation, read-heavy, slight staleness OK.

### 23.6 CQRS (Command Query Responsibility Segregation)

Separate the model for writes (commands) from reads (queries).
- Writes → normalized schema (OLTP style)
- Reads → denormalized / optimized tables, materialized views, elasticsearch index

Enables scaling + optimizing each path separately. Common in event-sourced architectures.

### 23.7 Event Sourcing

Instead of storing current state, store a log of all events. Current state is a projection.

```
Events: UserCreated, EmailChanged, OrderPlaced, OrderCancelled, ...
Projection: current user state, order status, etc.
```

Benefits: audit log, time travel, event replay for new projections.
Costs: query complexity; event schema evolution; replay cost.

### 23.8 Hot/Warm/Cold Data Tiering

- **Hot:** RAM / NVMe. Last 7 days.
- **Warm:** standard SSD. Last 30 days.
- **Cold:** HDD / S3 Glacier. Older.

Automatic in Snowflake (micro-partitions), Elasticsearch (ILM policies), Postgres with `pg_cron` + manual archiving.

### 23.9 Multi-Tenancy Patterns

- Shared schema (row-level filter by tenant_id)
- Shared DB, isolated schemas
- DB per tenant
- Pod per customer tier (enterprise = dedicated)

Trade-offs: isolation vs scalability vs cost. See section 14.

### 23.10 Change Data Capture (CDC)

Stream row-level changes from a DB to downstream systems. Full chapter: `../27-cdc-deep-dive.md`.

### 23.11 Interview Patterns

> **🎯 Interview Angle — "Design the data layer for a 100M-user social network."**
> 
> Writes: shard by user_id (hash). Each shard = Postgres primary + 2 replicas.
> 
> Reads:
> - User profile: shard key → direct lookup
> - User's feed: pre-computed (fan-out on write to each follower's feed in Redis / Cassandra)
> - Search: Elasticsearch cluster
> - Analytics: CDC → Kafka → ClickHouse / Snowflake
> 
> Cross-cutting:
> - Cache (Redis) in front of DB for hot users
> - CDN for static content
> - Rate limiting at API gateway
> - DR: cross-region replication, warm standby
> 
> Why: shard-per-user keeps most queries single-shard (profile, posts, friends). Fan-out-on-write amortizes feed cost at post time (expensive once vs expensive on every read).

---

## 24. Interview Patterns

### 24.1 Common Database Engineering Interview Questions

**Internals:**
- How does a B+Tree index work? Why not B-Tree?
- Explain MVCC. How does Postgres implement it?
- What is a WAL? What does ARIES do?
- Explain the buffer pool and eviction policies.
- What's a slotted page?

**Query optimization:**
- How does the query optimizer choose a join algorithm?
- Explain EXPLAIN output.
- Why would an index NOT be used even if present?
- How do you fix a slow query without touching the application?
- What stats does the optimizer need?

**Concurrency:**
- Define each ACID property.
- Compare 2PL and MVCC.
- Explain each isolation level + what phenomena they prevent.
- What is write skew? Why does Snapshot Isolation allow it?
- How do you handle lost updates?

**Distributed:**
- CAP theorem — explain. What's PACELC?
- 2PC vs Saga — when each?
- Explain Raft at a high level.
- Replica lag — causes and mitigations.
- Shard key selection — what's a good vs bad choice?

**System design:**
- Design a URL shortener's data layer.
- Design the data layer for a chat app.
- Design multi-tenant SaaS DB architecture.
- Design a feed system for 100M users.

### 24.2 Deep-Dive Walkthroughs

**Q: "Walk me through what happens when I run `SELECT * FROM users WHERE id = 123`."**

Staff answer:
1. **Parse:** SQL text → AST (parser).
2. **Analyze/bind:** resolve table/column names using catalog; type check.
3. **Rewrite:** apply view substitution, constant folding.
4. **Plan:** optimizer chooses: index on `id` exists → index scan.
5. **Execute:** 
   - Start transaction (even for SELECT).
   - Acquire shared lock on table (or none if MVCC).
   - Buffer manager: find index root page.
   - Traverse B+Tree: root → internal → leaf (2-3 I/Os for billion-row table; buffer pool hits for upper levels).
   - Leaf has (id=123, TID→heap page X). Fetch heap page X.
   - Visibility check (MVCC): is row visible to this txn's snapshot? If yes, return.
   - Return row to client via protocol (libpq, JDBC, etc.).
6. **Finalize:** release locks (if any); update stats (if autocommit); commit.

**Q: "Walk me through what happens when I run `UPDATE orders SET status = 'shipped' WHERE id = 123`."**

Staff answer:
1. Parse, analyze, plan: similar.
2. Begin txn (if not in one).
3. Acquire ROW LOCK (exclusive) on orders row where id=123.
4. Read current row via index (as above).
5. MVCC: create new row version with status='shipped' and current txn_id.
   - Mark old row's xmax = current txn_id (soft delete).
   - INSERT new row with xmin = current txn_id.
6. Update index: insert new TID into index (both versions coexist until vacuum).
7. Write WAL record describing the change (before image + after image).
8. On COMMIT: fsync WAL; mark txn as committed in txn table; release row lock.
9. Return success to client.
10. Later: VACUUM reclaims dead row; HOT (Heap-Only Tuple) in Postgres may avoid new index entry.

**Q: "Database is slow. Walk me through diagnosis."**

Staff answer:
1. **What's slow?** Specific query or whole DB?
2. **Query-specific:**
   - Get EXPLAIN ANALYZE; identify dominant operator.
   - Check: full scan vs index scan; sort spilling to disk; nested loops over large sets.
   - Compare estimated vs actual rows (stats stale?).
3. **DB-wide:**
   - Resource saturation: CPU, memory, disk I/O, network? (OS tools: top, iostat, vmstat.)
   - DB metrics: buffer hit ratio (<99% for OLTP = too small shared_buffers), long transactions, lock waits, replica lag.
   - Blocking: `pg_locks`, `SHOW ENGINE INNODB STATUS`.
   - Autovacuum running? Tables bloated?
4. **Fix root cause:** add index, update stats, kill blocker, scale resources, rewrite query, add replica for read offload.

**Q: "Design a URL shortener's data layer."**

Staff answer:
- Core table: `urls(short_code VARCHAR(10) PRIMARY KEY, long_url TEXT, created_at TIMESTAMP, expires_at TIMESTAMP, user_id BIGINT, click_count BIGINT)`.
- Read-heavy: 100:1 or more; most ops are "look up short_code → get long_url and redirect."
- Write-heavy on click_count — move clicks to append-only `clicks(short_code, ts, ip)` and update count via batch job (don't update main row on every click).
- Scale: PostgreSQL + aggressive caching (Redis in front) handles most URL shorteners. Index on short_code (clustered or B-Tree).
- For celebrity viral URLs: cache in Redis + CDN.
- Sharding only needed at absurd scale (bit.ly scale); shard by short_code hash.
- Analytics: CDC `clicks` → warehouse (Snowflake / BigQuery) for reporting.
- Short code generation: base62 encoding of auto-increment ID, or random with uniqueness check. Hash-based to avoid centralized counter.

### 24.3 Pitfalls to Avoid

**❌ "I'd use NoSQL because it scales."**
Right answer: Postgres scales further than most people think. NoSQL has specific trade-offs (schema flexibility, write throughput, geographic distribution). Pick based on actual requirements, not "scalability" buzzword.

**❌ "Always add an index."**
Right answer: indexes cost on writes. Analyze usage. Drop unused.

**❌ "Serializable is always safest."**
Right answer: Serializable has overhead + abort cost. Use RC/RR for most; upgrade specific txns to Serializable where correctness demands.

**❌ "Just shard it."**
Right answer: sharding is expensive operational complexity. Exhaust vertical + replication + partitioning first.

**❌ "Use 2PC for distributed transactions."**
Right answer: 2PC blocks on coordinator failure. Modern options: distributed DBs (Spanner, Cockroach) OR Saga pattern for workflows.

**❌ "Replication = backup."**
Right answer: replication copies corruption too. Backups are separate (and tested).

**❌ "The optimizer will figure it out."**
Right answer: optimizers are great but imperfect. Stats, hints, and manual rewrites still matter. EXPLAIN religiously.

### 24.4 Database Engineering Study Checklist

- [ ] Pages, slotted page layout, heap vs clustered
- [ ] Buffer pool, LRU, write-back, dirty pages
- [ ] WAL + ARIES recovery; fsync; group commit
- [ ] B+Tree internals: nodes, splits, concurrency (B-link)
- [ ] B+Tree vs B-Tree (why B+Tree wins)
- [ ] LSM Trees: MemTable, SSTables, compaction, bloom filters
- [ ] Index types: B+Tree, hash, LSM, bitmap, GIN/GiST, BRIN
- [ ] Covering, partial, expression, composite indexes
- [ ] Volcano / vectorized / codegen execution models
- [ ] Join algorithms: NL, hash, sort-merge, grace hash
- [ ] Aggregation: hash agg, sort agg, streaming
- [ ] Query optimizer: RBO vs CBO; statistics; cardinality estimation
- [ ] EXPLAIN plans for Postgres, MySQL, SQL Server
- [ ] ACID + implementations (undo log, MVCC)
- [ ] Isolation levels + phenomena (dirty, non-repeatable, phantom, write skew)
- [ ] 2PL (shared/exclusive, intent locks)
- [ ] MVCC (Postgres xmin/xmax, InnoDB undo)
- [ ] Snapshot Isolation vs Serializable (SSI)
- [ ] Partitioning: range, list, hash, composite
- [ ] Sharding: hash, range, consistent hashing; hot shards
- [ ] Replication: primary-replica, multi-primary; sync/async/semi-sync
- [ ] Replica lag; read-your-writes
- [ ] 2PC + Saga; outbox pattern
- [ ] Consensus: Raft (preferred), Paxos; quorum reads/writes
- [ ] CAP + PACELC; consistency models
- [ ] Database engines: InnoDB, PG heap, RocksDB, WiredTiger
- [ ] NoSQL categories: KV, document, column-family, graph, vector
- [ ] Security: auth, authz, RLS, encryption at rest + in transit, SQL injection
- [ ] Caching: cache-aside, write-through, invalidation
- [ ] Materialized views; CQRS; event sourcing
- [ ] Cursors (server-side vs client-side; alternatives)
- [ ] Modern CDC + outbox + streaming to warehouse

### 24.5 Final Advice

1. **Learn one DB deeply, then compare.** Picking Postgres as your deep-dive gives you the broadest baseline (most concepts transfer).
2. **Read EXPLAIN like code.** The plan is the single most informative artifact in SQL work.
3. **Measure, don't theorize.** Benchmark your workload; profile; iterate.
4. **ACID is the floor, not the ceiling.** Understand what each letter costs; know when to relax.
5. **Shard last, cache often.** Caching + replication + vertical scale go far.
6. **Trust the engine, understand its limits.** Modern optimizers are smart. But stale stats, data skew, and correlated columns still break them.
7. **Distributed = hard.** CAP, 2PC, Saga, Raft — these aren't academic; they shape real systems.
8. **Security is non-negotiable.** TLS, parameterized queries, least privilege, encrypted backups.
9. **The data lifecycle matters.** Hot/warm/cold tiering, retention, archival — budget concerns reward engineering.
10. **Database engineering is deep.** This chapter alone is a surface treatment of each topic. For principal-level, go deeper where you're weakest.

Good luck. Databases reward understanding what's happening under the hood.

---

## License

MIT.

[← Back to main](./README.md)
