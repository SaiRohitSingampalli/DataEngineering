# SQL & Database Administration — Comprehensive Notes

> A reference guide covering **MS SQL Server**, **PostgreSQL**, **MySQL**, and **Oracle** — concepts, internals, optimization, query plans, and DBA operations.

[![License: CC BY 4.0](https://img.shields.io/badge/License-CC_BY_4.0-blue.svg)](https://creativecommons.org/licenses/by/4.0/)
[![Databases](https://img.shields.io/badge/Databases-4-blueviolet)]()
[![Topics](https://img.shields.io/badge/Topics-16-brightgreen)]()

---

## Table of Contents

1. [SQL Fundamentals](#1-sql-fundamentals)
2. [Vendor-Specific Syntax Differences](#2-vendor-specific-syntax-differences)
3. [Joins, Set Operations, and Advanced Queries](#3-joins-set-operations-and-advanced-queries)
4. [Indexes — The DBA's Primary Lever](#4-indexes--the-dbas-primary-lever)
5. [Transactions, Isolation Levels, and Locking](#5-transactions-isolation-levels-and-locking)
6. [Database Internals](#6-database-internals)
7. [The Query Optimizer and Execution Plans](#7-the-query-optimizer-and-execution-plans)
8. [Performance Tuning Methodology](#8-performance-tuning-methodology)
9. [Partitioning, Sharding, and Replication](#9-partitioning-sharding-and-replication)
10. [Backup, Restore, and Recovery](#10-backup-restore-and-recovery)
11. [Security and Access Control](#11-security-and-access-control)
12. [High Availability and Disaster Recovery](#12-high-availability-and-disaster-recovery)
13. [Monitoring and Observability](#13-monitoring-and-observability)
14. [Stored Procedures, Functions, and Triggers](#14-stored-procedures-functions-and-triggers)
15. [Routine DBA Tasks](#15-routine-dba-tasks)
16. [Beyond Classic SQL](#16-beyond-classic-sql)
17. [Appendix A — Cheat Sheets](#appendix-a--cheat-sheets)
18. [Appendix B — Suggested Learning Path](#appendix-b--suggested-learning-path)
19. [Appendix C — Recommended Books](#appendix-c--recommended-books)

---

## 1. SQL Fundamentals

SQL (Structured Query Language) is the standard language for relational databases. It is **declarative** — you describe the result you want, not the procedure to obtain it. The database engine's optimizer translates your SQL into an execution plan.

### 1.1 SQL Sub-languages

| Sub-language | Purpose | Example Statements |
|---|---|---|
| **DDL** (Data Definition) | Define schema objects | `CREATE`, `ALTER`, `DROP`, `TRUNCATE`, `RENAME` |
| **DML** (Data Manipulation) | Modify rows | `INSERT`, `UPDATE`, `DELETE`, `MERGE` |
| **DQL** (Data Query) | Retrieve rows | `SELECT` |
| **DCL** (Data Control) | Permissions | `GRANT`, `REVOKE` |
| **TCL** (Transaction Control) | Manage transactions | `BEGIN`, `COMMIT`, `ROLLBACK`, `SAVEPOINT` |

### 1.2 Logical Query Processing Order

Although `SELECT` appears first in the syntax, it is logically processed almost last. This is why aliases declared in `SELECT` cannot be referenced in `WHERE`, and why `GROUP BY` columns must appear before `HAVING`.

```
1. FROM        -- gather source tables, apply joins
2. ON          -- evaluate join predicates
3. WHERE       -- filter individual rows
4. GROUP BY    -- collapse rows into groups
5. HAVING      -- filter groups
6. SELECT      -- evaluate expressions, apply aliases
7. DISTINCT    -- remove duplicates
8. ORDER BY    -- sort the result
9. LIMIT/OFFSET (or TOP, FETCH FIRST)
```

### 1.3 ACID Properties

| Property | Meaning | How it is enforced |
|---|---|---|
| **Atomicity** | All-or-nothing execution | Transaction log / undo segments |
| **Consistency** | DB moves from one valid state to another | Constraints, triggers, referential integrity |
| **Isolation** | Concurrent txns appear serial | Locking, MVCC, isolation levels |
| **Durability** | Committed changes survive crashes | Write-ahead logging (WAL/REDO), fsync, checkpoints |

### 1.4 Data Types Across Engines

| Logical Type | MS SQL | PostgreSQL | MySQL | Oracle |
|---|---|---|---|---|
| Integer 32-bit | `INT` | `INTEGER` | `INT` | `NUMBER(10)` |
| Integer 64-bit | `BIGINT` | `BIGINT` | `BIGINT` | `NUMBER(19)` |
| Decimal | `DECIMAL(p,s)` | `NUMERIC(p,s)` | `DECIMAL(p,s)` | `NUMBER(p,s)` |
| Variable string | `VARCHAR` / `NVARCHAR` | `VARCHAR` / `TEXT` | `VARCHAR` / `TEXT` | `VARCHAR2` / `CLOB` |
| Boolean | `BIT` | `BOOLEAN` | `TINYINT(1)` | `NUMBER(1)` |
| Date+Time | `DATETIME2` / `DATETIMEOFFSET` | `TIMESTAMP` / `TIMESTAMPTZ` | `DATETIME` / `TIMESTAMP` | `DATE` / `TIMESTAMP WITH TZ` |
| JSON | `NVARCHAR(MAX)` + JSON funcs | `JSON` / `JSONB` | `JSON` | `JSON` (21c+) |
| UUID | `UNIQUEIDENTIFIER` | `UUID` | `CHAR(36)` / `BINARY(16)` | `RAW(16)` |
| Auto-increment | `IDENTITY` / `SEQUENCE` | `SERIAL` / `IDENTITY` | `AUTO_INCREMENT` | `SEQUENCE` / `IDENTITY` |

### 1.5 NULL Semantics

`NULL` means **"unknown"**. Any direct comparison with `NULL` yields `UNKNOWN`, which behaves like `FALSE` in `WHERE`.

```sql
-- These return NO rows:
SELECT * FROM employees WHERE bonus = NULL;
SELECT * FROM employees WHERE bonus <> NULL;

-- These work correctly:
SELECT * FROM employees WHERE bonus IS NULL;
SELECT * FROM employees WHERE bonus IS NOT NULL;

-- NULL-safe comparisons:
-- MySQL:        a <=> b
-- PostgreSQL:   a IS NOT DISTINCT FROM b
-- MS SQL:       (a = b OR (a IS NULL AND b IS NULL))
-- Oracle:       DECODE(a, b, 0, 1) = 0
```

> ⚠️ **Warning:** `NULL` in aggregates is silently ignored. `AVG(salary)` excludes `NULL` rows from both numerator and denominator. `COUNT(*)` counts all rows; `COUNT(col)` skips `NULL`.

---

## 2. Vendor-Specific Syntax Differences

### 2.1 Pagination / TOP-N

```sql
-- ANSI standard (PostgreSQL, MS SQL 2012+, Oracle 12c+, MySQL 8+)
SELECT * FROM orders ORDER BY id OFFSET 20 ROWS FETCH NEXT 10 ROWS ONLY;

-- MS SQL legacy
SELECT TOP 10 * FROM orders ORDER BY id;

-- MySQL / PostgreSQL
SELECT * FROM orders ORDER BY id LIMIT 10 OFFSET 20;

-- Oracle pre-12c (rownum trick)
SELECT * FROM (
  SELECT a.*, ROWNUM rn FROM (
    SELECT * FROM orders ORDER BY id
  ) a WHERE ROWNUM <= 30
) WHERE rn > 20;
```

### 2.2 String Concatenation

| Engine | Operator / Function | Notes |
|---|---|---|
| ANSI / PostgreSQL / Oracle | `\|\|` | Standard pipe-pipe operator |
| MS SQL | `+` or `CONCAT()` | `+` propagates NULL; `CONCAT` treats NULL as empty |
| MySQL | `CONCAT()` only | `\|\|` means OR by default |

### 2.3 Date / Time Functions

| Operation | MS SQL | PostgreSQL | MySQL | Oracle |
|---|---|---|---|---|
| Current date | `GETDATE()` | `NOW()` | `NOW()` | `SYSDATE` |
| Add days | `DATEADD(day, n, d)` | `d + n * INTERVAL '1 day'` | `DATE_ADD(d, INTERVAL n DAY)` | `d + n` |
| Diff in days | `DATEDIFF(day, a, b)` | `b - a` | `DATEDIFF(b, a)` | `b - a` |
| Truncate to month | `DATEFROMPARTS(...)` | `DATE_TRUNC('month', d)` | `DATE_FORMAT(d,'%Y-%m-01')` | `TRUNC(d, 'MM')` |
| Format | `FORMAT(d,'yyyy-MM-dd')` | `TO_CHAR(d,'YYYY-MM-DD')` | `DATE_FORMAT(d,'%Y-%m-%d')` | `TO_CHAR(d,'YYYY-MM-DD')` |

### 2.4 UPSERT (INSERT or UPDATE)

```sql
-- PostgreSQL (recommended)
INSERT INTO inventory (sku, qty) VALUES ('A1', 10)
ON CONFLICT (sku) DO UPDATE SET qty = inventory.qty + EXCLUDED.qty;

-- MySQL
INSERT INTO inventory (sku, qty) VALUES ('A1', 10)
ON DUPLICATE KEY UPDATE qty = qty + VALUES(qty);

-- MS SQL & Oracle (ANSI MERGE)
MERGE INTO inventory AS t
USING (SELECT 'A1' AS sku, 10 AS qty) AS s
ON t.sku = s.sku
WHEN MATCHED THEN UPDATE SET qty = t.qty + s.qty
WHEN NOT MATCHED THEN INSERT (sku, qty) VALUES (s.sku, s.qty);
```

> ⚠️ **Warning:** MS SQL `MERGE` has had several documented concurrency bugs. Many DBAs prefer separate `UPDATE` + `INSERT` inside a serializable transaction.

### 2.5 Identifier Quoting

| Engine | Quote Char | Case Behavior |
|---|---|---|
| ANSI / PostgreSQL / Oracle | `"name"` | Quoted = case-sensitive; unquoted folded (PG lower, Oracle upper) |
| MS SQL | `[brackets]` or `"name"` | Case-insensitive by default (collation-driven) |
| MySQL | `` `backticks` `` | Depends on filesystem; `lower_case_table_names` variable |

### 2.6 Random Sampling

```sql
-- MS SQL:      SELECT TOP 100 * FROM t ORDER BY NEWID();   -- exact
--              SELECT * FROM t TABLESAMPLE (5 PERCENT);    -- approximate
-- PostgreSQL:  SELECT * FROM t TABLESAMPLE BERNOULLI (5);
-- MySQL:       SELECT * FROM t ORDER BY RAND() LIMIT 100;
-- Oracle:      SELECT * FROM t SAMPLE(5);
```

---

## 3. Joins, Set Operations, and Advanced Queries

### 3.1 Join Types

| Join | Returns | Common Uses |
|---|---|---|
| `INNER JOIN` | Rows present in both tables | Normal relational reads |
| `LEFT OUTER JOIN` | All from left, matched from right | Optional related data |
| `RIGHT OUTER JOIN` | All from right, matched from left | Rarely used |
| `FULL OUTER JOIN` | All from both, NULL where no match | Reconciliation, diff reports |
| `CROSS JOIN` | Cartesian product | Combinations, calendar tables |
| `SELF JOIN` | Table joined to itself | Hierarchies, comparisons |
| `LATERAL` / `CROSS APPLY` | Per-row correlated subquery | Top-N per group |

#### Anti-join and Semi-join

```sql
-- Semi-join (rows in A that have a match in B)
SELECT * FROM customers c WHERE EXISTS (
  SELECT 1 FROM orders o WHERE o.cust_id = c.id
);

-- Anti-join (rows in A that have NO match in B)
SELECT * FROM customers c WHERE NOT EXISTS (
  SELECT 1 FROM orders o WHERE o.cust_id = c.id
);
```

> 💡 **Tip:** Prefer `NOT EXISTS` over `NOT IN`. `NOT IN` returns **no rows** if the subquery contains a single `NULL`, due to three-valued logic.

### 3.2 Set Operations

| Operator | Behavior | Notes |
|---|---|---|
| `UNION` | Combine, remove duplicates | Implicit DISTINCT |
| `UNION ALL` | Combine, keep duplicates | **Faster** — always prefer when duplicates impossible |
| `INTERSECT` | Rows in both | Distinct by default |
| `EXCEPT` (`MINUS` in Oracle) | Rows in first not in second | Distinct |

### 3.3 Common Table Expressions (CTEs)

```sql
WITH recent_orders AS (
    SELECT * FROM orders WHERE order_date >= CURRENT_DATE - 30
),
big_spenders AS (
    SELECT customer_id, SUM(total) AS spend
    FROM recent_orders GROUP BY customer_id HAVING SUM(total) > 1000
)
SELECT c.name, b.spend
FROM big_spenders b JOIN customers c ON c.id = b.customer_id;
```

> 📝 **Note:** PostgreSQL versions before 12 always materialized CTEs. PG 12+ inlines simple non-recursive CTEs by default. Use `WITH ... AS MATERIALIZED` to force the old behavior.

#### Recursive CTE — Hierarchies

```sql
WITH RECURSIVE org_chart AS (
    -- Anchor
    SELECT employee_id, manager_id, name, 1 AS depth
    FROM employees WHERE manager_id IS NULL
    UNION ALL
    -- Recursive
    SELECT e.employee_id, e.manager_id, e.name, oc.depth + 1
    FROM employees e
    JOIN org_chart oc ON e.manager_id = oc.employee_id
)
SELECT * FROM org_chart ORDER BY depth, name;
```

### 3.4 Window Functions

| Category | Functions | Use Case |
|---|---|---|
| **Ranking** | `ROW_NUMBER`, `RANK`, `DENSE_RANK`, `NTILE` | Top-N per group, percentiles |
| **Offset** | `LAG`, `LEAD`, `FIRST_VALUE`, `LAST_VALUE` | Compare to prior/next row |
| **Aggregate as window** | `SUM`, `AVG`, `COUNT` ... `OVER(...)` | Running totals, moving averages |
| **Distribution** | `PERCENT_RANK`, `CUME_DIST`, `PERCENTILE_CONT` | Statistical analysis |

```sql
-- Top 3 highest-paid per department
SELECT * FROM (
    SELECT name, dept_id, salary,
           ROW_NUMBER() OVER (PARTITION BY dept_id ORDER BY salary DESC) AS rn
    FROM employees
) x WHERE rn <= 3;

-- Running total
SELECT order_date, total,
       SUM(total) OVER (ORDER BY order_date
                        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_total
FROM orders;

-- 7-day moving average
SELECT day, sales,
       AVG(sales) OVER (ORDER BY day
                        ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS ma7
FROM daily_sales;
```

### 3.5 Pivoting

```sql
-- ANSI / portable: conditional aggregation
SELECT product,
       SUM(CASE WHEN region='NA' THEN amount END) AS na,
       SUM(CASE WHEN region='EU' THEN amount END) AS eu,
       SUM(CASE WHEN region='APAC' THEN amount END) AS apac
FROM sales GROUP BY product;

-- MS SQL native PIVOT
SELECT product, [NA], [EU], [APAC]
FROM sales
PIVOT (SUM(amount) FOR region IN ([NA],[EU],[APAC])) p;
```

---

## 4. Indexes — The DBA's Primary Lever

Indexes are auxiliary data structures that let the engine locate rows without scanning the entire table. Every index speeds up `SELECT`s but slows down `INSERT`, `UPDATE`, `DELETE`.

### 4.1 Index Types and Structures

| Type | Structure | Best For | Vendor Support |
|---|---|---|---|
| **B-tree** (default) | Balanced search tree | Equality, range, ORDER BY | All engines |
| **Hash** | Hash table | Equality only | PG, MySQL MEMORY |
| **Bitmap** | Bitmap per distinct value | Low-cardinality, OLAP | Oracle |
| **Clustered** | Table is the index leaf | Primary access path | MS SQL, MySQL InnoDB |
| **Heap** | Unordered row storage | Default when no clustered idx | MS SQL, Oracle, PG |
| **Covering / INCLUDE** | Extra columns in leaf | Eliminate key lookups | MS SQL, PG 11+ |
| **Filtered / Partial** | WHERE clause on index | Sparse predicates | MS SQL (filtered), PG (partial) |
| **Functional / Expression** | Index on expression | Computed predicates | All engines |
| **GIN / GiST** | Inverted, multi-column | JSONB, full text, arrays | PostgreSQL |
| **Full-text** | Tokenized | Word search, ranking | All engines |
| **Spatial (R-tree)** | Bounding-box tree | Geographic data | All engines via extensions |
| **Columnstore** | Column-compressed | OLAP / analytics | MS SQL, Oracle, MySQL HeatWave |

### 4.2 The B-tree in Detail

Almost every "normal" index is a **B+tree** — all data lives in leaves, leaves are linked in a doubly-linked list. Fanout is huge (hundreds of keys per node), so even a billion-row table is reachable in 4–5 page reads.

```
                [50 | 100]                     <- root
                /     |     \
        [10|30]  [60|80]  [110|140]            <- internal
        /  |  \   ...
   [1..9][11..29][31..49] ...                  <- leaves (linked list)
       |       |       |
       +---->--+---->--+--> ...   (range scan via leaf links)
```

**Key properties:**
- Lookup is `O(log n)` but in practice 2–4 random I/O reads
- Range scans are very efficient — descend once, then walk the leaf chain
- Random-key inserts (UUIDv4 PKs) fragment indexes; sequential keys (`IDENTITY`, `SERIAL`, UUIDv7) do not
- Each non-clustered index entry stores the row locator (heap RID) or clustered key — wide clustered keys bloat every secondary index

### 4.3 Clustered vs Non-Clustered

| Aspect | Clustered Index | Non-Clustered (Secondary) |
|---|---|---|
| Physical order | Table data IS the index leaves | Separate structure pointing to base table |
| Per-table count | One only | Many |
| Read cost | Range scans extremely fast | Range scans need extra lookups |
| Write cost | Splits / fragmentation on bad key | Each index adds write overhead |
| Vendor | MS SQL, MySQL InnoDB (PK always clustered) | All engines |

> 💡 **Tip:** Pick clustered keys that are: **narrow, unique, ever-increasing, and never updated**. A surrogate `IDENTITY`/`SERIAL` is usually ideal.

### 4.4 Composite Indexes and Column Order

An index on `(A, B, C)` supports queries that filter on a leading subset: `A`; `A,B`; `A,B,C`. It does **not** efficiently support filtering only on `B` or `C` alone.

```sql
-- Index: CREATE INDEX ix_orders_cust_date ON orders(customer_id, order_date);

SELECT * FROM orders WHERE customer_id = 7;                                -- USES INDEX
SELECT * FROM orders WHERE customer_id = 7 AND order_date > '2024-01-01';  -- USES INDEX
SELECT * FROM orders WHERE order_date > '2024-01-01';                      -- IGNORES INDEX
```

Put the most selective equality predicate first, then the next most selective, with range predicates last.

### 4.5 Covering Indexes

If an index contains every column the query needs, the engine can answer purely from the index without touching the base table.

```sql
-- MS SQL & PostgreSQL 11+
CREATE INDEX ix_orders_cust_inc
ON orders(customer_id) INCLUDE (order_date, total);

-- MySQL / Oracle: add columns to the key
CREATE INDEX ix_orders_cust ON orders(customer_id, order_date, total);
```

### 4.6 Filtered / Partial Indexes

```sql
-- MS SQL: filtered index
CREATE INDEX ix_orders_open ON orders(customer_id)
WHERE status = 'OPEN';

-- PostgreSQL: partial index
CREATE INDEX ix_orders_open ON orders(customer_id)
WHERE status = 'OPEN';
```

### 4.7 Index Anti-patterns That Defeat Index Use

- **Functions on indexed columns:** `WHERE UPPER(name) = 'BOB'` → use functional index
- **Implicit type conversion:** `WHERE varchar_col = 12345` forces CAST on every row
- **Leading wildcard:** `WHERE name LIKE '%bob%'` cannot seek a B-tree
- **OR across different columns:** `WHERE a = 1 OR b = 2` often scans; rewrite as `UNION ALL`
- **NULL handling:** varies by engine — test before relying on `IS NULL` using an index
- **Negation:** `WHERE col <> 5` typically scans

### 4.8 Index Maintenance

| Engine | Detect Fragmentation | Rebuild / Reorganize |
|---|---|---|
| MS SQL | `sys.dm_db_index_physical_stats` | `ALTER INDEX ... REBUILD / REORGANIZE` |
| PostgreSQL | `pgstattuple`, `pg_stat_user_indexes` | `REINDEX (CONCURRENTLY)` on PG 12+ |
| MySQL InnoDB | `INFORMATION_SCHEMA.INNODB_TABLES` | `OPTIMIZE TABLE` |
| Oracle | `INDEX_STATS` view | `ALTER INDEX ... REBUILD [ONLINE]` |

> ⚠️ **Warning:** Don't rebuild every index on a schedule. Rebuild when fragmentation is >30% AND the index is large enough to matter (>1000 pages).

---

## 5. Transactions, Isolation Levels, and Locking

### 5.1 Concurrency Anomalies

| Anomaly | What Happens | Example |
|---|---|---|
| **Dirty Read** | T1 reads uncommitted data from T2 | T2 updates, T1 reads, T2 rolls back |
| **Non-repeatable Read** | Same row read twice gives different values | T2 commits update between T1's two reads |
| **Phantom Read** | Same predicate returns different rows | T2 inserts rows matching T1's WHERE |
| **Lost Update** | T1's update overwritten by T2 | Both read, both write — second wins |
| **Write Skew** | Two txns each maintain invariant individually, not collectively | On-call schedule with at-least-one constraint |

### 5.2 ANSI Isolation Levels

| Level | Dirty Read | Non-rep. Read | Phantom | Notes |
|---|---|---|---|---|
| `READ UNCOMMITTED` | ✅ | ✅ | ✅ | Rare; MySQL & MS SQL allow |
| `READ COMMITTED` | ❌ | ✅ | ✅ | Default in MS SQL, PG, Oracle |
| `REPEATABLE READ` | ❌ | ❌ | ✅* | Default MySQL InnoDB (prevents phantoms via gap locks) |
| `SERIALIZABLE` | ❌ | ❌ | ❌ | PG uses SSI |
| `SNAPSHOT` | ❌ | ❌ | ❌* | MVCC-based; can have write-skew |

### 5.3 Locks vs MVCC

**Pure locking** (older MS SQL default): readers acquire shared locks, writers exclusive. Readers block writers → high contention.

**MVCC** — Multi-Version Concurrency Control: each row has versions stamped with transaction IDs. "Readers don't block writers; writers don't block readers." Cost: storage for old versions.

| Engine | Concurrency Model | Old-version Store |
|---|---|---|
| **PostgreSQL** | MVCC always | Row versions in heap; `VACUUM` removes |
| **Oracle** | MVCC always | `UNDO` tablespace |
| **MySQL InnoDB** | MVCC always | Undo log + rollback segments |
| **MS SQL** | Locking by default; MVCC when `READ_COMMITTED_SNAPSHOT = ON` | Version store in `tempdb` |

### 5.4 Lock Modes

| Lock Mode | Compatible With |
|---|---|
| Shared (S) | Other S; not X |
| Exclusive (X) | Nothing |
| Update (U) | S only — avoids deadlocks during read-then-write |
| Intent (IS, IX, SIX) | Placed on parent objects |
| Schema (Sch-S / Sch-M) | DDL coordination (MS SQL) |

### 5.5 Deadlocks

A cycle of waits — T1 holds A waiting for B, T2 holds B waiting for A. The engine detects the cycle and aborts a victim.

**Prevention:**
- Always acquire locks in a consistent order
- Keep transactions short
- Use lower isolation when safe (`READ COMMITTED` instead of `SERIALIZABLE`)
- Add covering indexes — table scans take many more locks
- In MS SQL, consider snapshot isolation
- Catch the deadlock error code in app code and retry

### 5.6 Two-Phase Commit

Across multiple resource managers, 2PC ensures atomic commit:
1. **Prepare:** coordinator asks each participant; each writes a prepared record and replies OK or ABORT
2. **Commit:** if all OK, coordinator says COMMIT; otherwise ROLLBACK

Vendor implementations: MS SQL (MS DTC), Oracle (XA / DBLINK), PostgreSQL (`PREPARE TRANSACTION`), MySQL (XA).

---

## 6. Database Internals

### 6.1 Storage Hierarchy

```
Application
    |
    v
   SQL Layer       <- parser, optimizer, executor
    |
    v
   Buffer Pool    <- in-memory cache of pages (most important DBA tunable)
    |
    v
   Data Files    <- tablespaces / filegroups, pages of fixed size
    |
    v
   Storage       <- disks, SAN, NVMe, cloud volumes
   +
   Transaction Log / WAL / REDO  <- sequential, durable, crash recovery
```

### 6.2 Page (Block) Layout

| Engine | Page Size (default) | Notes |
|---|---|---|
| MS SQL | 8 KB | Fixed; 8 pages = 64 KB extent |
| PostgreSQL | 8 KB | Compile-time configurable (4–32 KB) |
| MySQL InnoDB | 16 KB | Configurable (4, 8, 16, 32, 64 KB) |
| Oracle | 8 KB default | Multiple block sizes per DB allowed |

Within a page: page header → row directory (slot array) → free space → rows growing toward each other.

### 6.3 Buffer Pool / Cache

Most production DB workloads are bottlenecked on buffer-pool misses, not CPU. Hit ratio above 95% is typical for healthy OLTP.

| Engine | Setting | Common Sizing |
|---|---|---|
| MS SQL | `max server memory` | Total RAM minus 2–4 GB for OS |
| PostgreSQL | `shared_buffers` | 25% of RAM |
| MySQL InnoDB | `innodb_buffer_pool_size` | 70–80% of RAM on dedicated host |
| Oracle | `SGA_TARGET` / `DB_CACHE_SIZE` | Plus PGA for sorts/hashes |

### 6.4 Write-Ahead Logging (WAL / REDO)

**Rule:** every change is written to the log on disk BEFORE the corresponding data page is flushed.

- Transactions append log records (LSN — log sequence number)
- `COMMIT` forces (fsyncs) the log up to that point
- Data pages flushed lazily later — the log is the source of truth
- On restart: redo log records since last checkpoint, then undo uncommitted txns (ARIES algorithm)

| Engine | Log Component |
|---|---|
| MS SQL | Transaction Log (`.ldf`), VLFs, LSN-based |
| PostgreSQL | WAL — `pg_wal` directory, 16 MB segments |
| MySQL InnoDB | `ib_logfile` / redo log + binlog (replication, separate) |
| Oracle | Online redo logs (cycled), archived redo logs, plus UNDO |

> ⚠️ **Warning:** MySQL has **TWO logs**: InnoDB redo log (crash recovery) and binlog (replication, PITR). Both must be durable for full safety: `sync_binlog=1` and `innodb_flush_log_at_trx_commit=1`.

### 6.5 Checkpoints

A checkpoint flushes all dirty pages up to a given LSN, records this in the log, and lets the engine truncate / recycle older log space. Checkpoints bound recovery time.

### 6.6 MVCC Row Versioning Internals

**PostgreSQL** — each tuple stores `xmin` (creating txn) and `xmax` (deleting/updating txn). `UPDATE` = `INSERT` new tuple + mark old `xmax`. Old versions stay until `VACUUM`.

**Oracle** — old versions reconstructed on demand from UNDO segments. Risk: `ORA-01555 snapshot too old`.

**MySQL InnoDB** — undo log holds previous row images; cleaned up by purge thread.

**MS SQL** (snapshot) — versions live in `tempdb`.

### 6.7 Statistics & Histograms

The optimizer estimates how many rows each operation will produce from statistics — histograms, distinct-value counts, correlation.

> ⚠️ **Warning:** Stale stats are the #1 cause of bad plans after data growth. Check row-estimate vs actual gaps in execution plans before tuning anything else.

---

## 7. The Query Optimizer and Execution Plans

### 7.1 How the Optimizer Works

```
SQL text
   |
   v
Parser  -> AST (validates syntax, names, types)
   |
   v
Algebrizer / Binder  -> logical operator tree
   |
   v
Logical rewrites  -> simplifications, subquery flattening, predicate pushdown
   |
   v
Physical optimization  -> enumerate join orders, join algorithms,
                          access methods; cost each plan using statistics
   |
   v
Best plan  -> cached / executed
```

### 7.2 Physical Operators

| Operator | Purpose | Best When |
|---|---|---|
| Table Scan / Seq Scan | Read every row | Small table, >~20% of rows |
| Index Seek / Index Scan | Navigate B-tree to range | Selective predicate |
| Index-only Scan / Covering Scan | Answer entirely from index | All needed cols in index |
| Key Lookup / Bookmark Lookup | Per-row trip back to table | Non-covering index — kills perf for many rows |
| Nested Loops Join | For each outer, probe inner | Small outer, indexed inner |
| Hash Join | Build hash on smaller, probe with larger | Large unsorted joins, equality |
| Merge Join (Sort Merge) | Walk two sorted inputs in parallel | Both sides already sorted |
| Sort | Order rows | ORDER BY without index |
| Stream / Hash Aggregate | Compute aggregates | Stream needs sorted; Hash doesn't |
| Spool / Materialize | Cache intermediate result | Reused subexpressions |

### 7.3 Join Algorithms

| Algorithm | Cost (rough) | Memory | Best Scenario |
|---|---|---|---|
| Nested Loop | O(N × M), O(N log M) with index | Tiny | Small N, indexed inner |
| Hash Join | O(N + M) | Build side fits memory | Large equi-joins |
| Merge Join | O(N + M) if presorted | Small | Inputs already sorted |

### 7.4 Reading Execution Plans

#### MS SQL Server

```sql
-- Estimated plan only
SET SHOWPLAN_XML ON;  -- or Ctrl+L in SSMS

-- Actual plan
SET STATISTICS PROFILE ON;
SET STATISTICS IO ON;
SET STATISTICS TIME ON;

-- Top cached plans
SELECT TOP 20 qs.execution_count,
       qs.total_elapsed_time/qs.execution_count AS avg_us,
       SUBSTRING(st.text, qs.statement_start_offset/2,
                 (qs.statement_end_offset-qs.statement_start_offset)/2) AS stmt,
       qp.query_plan
FROM sys.dm_exec_query_stats qs
CROSS APPLY sys.dm_exec_sql_text(qs.sql_handle) st
CROSS APPLY sys.dm_exec_query_plan(qs.plan_handle) qp
ORDER BY qs.total_elapsed_time DESC;
```

**Watch for:** Thick arrows = many rows; Estimated vs Actual rows mismatch = bad stats; yellow/red warning triangles; Key Lookups on hot queries.

#### PostgreSQL

```sql
EXPLAIN SELECT * FROM orders WHERE customer_id = 7;
EXPLAIN (ANALYZE, BUFFERS, VERBOSE, SETTINGS)   -- runs the query!
  SELECT * FROM orders WHERE customer_id = 7;
```

Sample output:
```
Index Scan using ix_orders_cust on orders  (cost=0.43..8.45 rows=1 width=64)
                                           (actual time=0.05..0.06 rows=2 loops=1)
  Index Cond: (customer_id = 7)
  Buffers: shared hit=4
Planning Time: 0.10 ms
Execution Time: 0.08 ms
```

**Watch for:** `rows=X` (estimate) vs actual `rows=Y`; `Buffers: shared read` = disk I/O; `Rows Removed by Filter`; `Sort Method: external merge` = spilled to disk.

> 💡 **Tip:** Paste plans into [explain.depesz.com](https://explain.depesz.com) or [explain.dalibo.com](https://explain.dalibo.com) for visualization.

#### MySQL

```sql
EXPLAIN SELECT * FROM orders WHERE customer_id = 7;
EXPLAIN FORMAT=JSON SELECT ...;    -- richer output
EXPLAIN ANALYZE SELECT ...;        -- 8.0+, runs the query
```

| Column | Meaning |
|---|---|
| `type` | Access method: `const` < `eq_ref` < `ref` < `range` < `index` < `ALL` (worst) |
| `key` | Index actually used |
| `rows` | Estimated rows examined |
| `filtered` | % of rows passing WHERE after index |
| `Extra` | `Using index` = covering; `Using filesort` = disk sort; `Using temporary` = temp table |

#### Oracle

```sql
EXPLAIN PLAN FOR SELECT * FROM orders WHERE customer_id = 7;
SELECT * FROM TABLE(DBMS_XPLAN.DISPLAY);

-- Real plan with actuals
ALTER SESSION SET statistics_level=ALL;
SELECT /*+ GATHER_PLAN_STATISTICS */ * FROM orders WHERE customer_id = 7;
SELECT * FROM TABLE(DBMS_XPLAN.DISPLAY_CURSOR(FORMAT=>'ALLSTATS LAST'));
```

**Watch for:** E-Rows vs A-Rows skew; FULL TABLE ACCESS on big table with selective predicate; HASH JOIN with high TempSpc.

### 7.5 Plan Caching and Parameter Sniffing

The optimizer compiles a plan based on the **first** parameter values. If atypical, the plan is bad for typical executions.

| Engine | Tools |
|---|---|
| MS SQL | `OPTION (RECOMPILE)`, `OPTIMIZE FOR`, Query Store forced plans, plan guides |
| PostgreSQL | `PREPARE`/`EXECUTE` custom vs generic plans after 5 executions; `plan_cache_mode` |
| MySQL | Statement digest cache; less tooling |
| Oracle | Adaptive Cursor Sharing, SQL Plan Baselines, SQL Profiles |

### 7.6 Optimizer Hints

```sql
-- MS SQL
SELECT * FROM orders WITH (INDEX(ix_orders_date)) WHERE order_date > '2024-01-01';
OPTION (MAXDOP 1, RECOMPILE);

-- Oracle (in /* */ comments)
SELECT /*+ INDEX(o ix_orders_date) USE_HASH(a b) PARALLEL(4) */ ...

-- PostgreSQL: no native hints. Use pg_hint_plan extension.

-- MySQL
SELECT /*+ INDEX(o ix_orders_date) */ ...
```

> ⚠️ Hints are last-resort tools. Prefer fixing statistics or schema.

---

## 8. Performance Tuning Methodology

### 8.1 The Diagnostic Loop

```
1. Identify    -> What's slow? Wait stats, top-query views, app traces.
2. Measure     -> Baseline: latency, CPU, I/O, locks, plan.
3. Hypothesize -> What's the bottleneck?
4. Change      -> One change at a time.
5. Re-measure  -> Did it help? Quantify.
6. Document    -> Why the change worked. Commit to source control.
```

### 8.2 Wait-Time Analysis

| Engine | Source |
|---|---|
| MS SQL | `sys.dm_os_wait_stats`, `sys.dm_exec_session_wait_stats` |
| PostgreSQL | `pg_stat_activity.wait_event`, `pg_stat_statements` |
| MySQL | `performance_schema.events_waits_summary_global_by_event_name` |
| Oracle | `v$system_event`, `v$session_wait`, AWR / ASH reports |

**Common wait categories:**

| Wait Type | Meaning | Likely Cause |
|---|---|---|
| I/O (`PAGEIOLATCH`, `db file sequential read`) | Reading from disk | Buffer pool too small, missing indexes |
| Lock / row lock waits | Blocked by another txn | Long transactions, hot-row contention |
| Latch | In-memory page contention | Hot pages (last-page insert, index leaf) |
| CPU (`SOS_SCHEDULER_YIELD`) | CPU-bound | Bad plans, missing indexes, parallelism |
| Network (`ASYNC_NETWORK_IO`) | Client not consuming | App slow to read, large result sets |
| Log write (`WRITELOG`, `log file sync`) | Waiting for fsync | Slow disk for log, too many tiny commits |

### 8.3 Top Query Identification

```sql
-- MS SQL
SELECT TOP 20
    qs.total_worker_time/1000 AS cpu_ms,
    qs.total_logical_reads, qs.execution_count,
    SUBSTRING(st.text, qs.statement_start_offset/2,
              (CASE qs.statement_end_offset WHEN -1 THEN DATALENGTH(st.text)
                    ELSE qs.statement_end_offset END - qs.statement_start_offset)/2) AS query
FROM sys.dm_exec_query_stats qs
CROSS APPLY sys.dm_exec_sql_text(qs.sql_handle) st
ORDER BY qs.total_worker_time DESC;

-- PostgreSQL
CREATE EXTENSION IF NOT EXISTS pg_stat_statements;
SELECT query, calls, total_exec_time, mean_exec_time, rows
FROM pg_stat_statements ORDER BY total_exec_time DESC LIMIT 20;
```

### 8.4 Common Tuning Wins (priority order)

1. **Add the missing index.** The optimizer often suggests one.
2. **Make existing indexes covering** (`INCLUDE` columns).
3. **Update statistics** — especially after large data loads.
4. **Rewrite query:** replace correlated subquery with JOIN; remove `SELECT *`; add WHERE that prunes early.
5. **Avoid N+1 patterns** — fetch in bulk.
6. **Batch DML** — break 10M-row updates into 100K chunks.
7. **Tune buffer pool / shared_buffers.**
8. **Move hot tempdb / sort area to faster storage.**
9. **Partition large tables.**
10. **Read replicas for reporting workload.**

### 8.5 Schema-Level Concerns

**Normalization vs denormalization:** Normalize for OLTP, denormalize for read-heavy analytics. Hybrid: normalized store + denormalized reporting refreshed asynchronously.

**Identity type choice:**

| Type | Pros | Cons |
|---|---|---|
| `INT IDENTITY/SERIAL` | Narrow, fast, sequential | 32-bit limit (~2B) |
| `BIGINT` | 9 quintillion range | 8 bytes adds up in indexes |
| UUID v4 (random) | Globally unique | Fragments clustered indexes; 16 bytes |
| UUID v7 (timestamp-prefixed) | Sortable, no fragmentation | 16 bytes; relatively new |

---

## 9. Partitioning, Sharding, and Replication

### 9.1 Partitioning

| Strategy | Key Idea | Best For |
|---|---|---|
| **Range** | Each partition covers a key range | Time-series, archival rolloff |
| **List** | Each partition holds explicit values | Region, tenant |
| **Hash** | `Hash(key) mod N` | Even distribution |
| **Composite** | Hash within range, etc. | Combined needs |

```sql
-- PostgreSQL (declarative, PG 10+)
CREATE TABLE orders (id BIGINT, order_date DATE, ...)
PARTITION BY RANGE (order_date);
CREATE TABLE orders_2024_01 PARTITION OF orders
  FOR VALUES FROM ('2024-01-01') TO ('2024-02-01');

-- MS SQL
CREATE PARTITION FUNCTION pf_orders_date (date)
AS RANGE RIGHT FOR VALUES ('2024-01-01','2024-02-01','2024-03-01');
CREATE PARTITION SCHEME ps_orders ON pf_orders_date ALL TO ([PRIMARY]);
CREATE TABLE orders (...) ON ps_orders(order_date);
```

> 💡 Partitioning shines when the partition key is in nearly every `WHERE` clause.

### 9.2 Sharding

Horizontal scaling by spreading data across separate database instances. Unlike partitioning, the engine cannot transparently join across shards — application or middleware (Vitess, Citus, ShardingSphere) must route queries.

- Shard key choice is critical
- Cross-shard transactions require 2PC or sagas
- Re-sharding is operationally painful — design with it in mind

### 9.3 Replication

| Type | Description | Use Case |
|---|---|---|
| **Asynchronous** | Primary commits before replica confirms | Read scaling, low primary latency |
| **Synchronous** | Primary waits for replica confirmation | Zero data-loss |
| **Semi-synchronous** | At least one replica must confirm | Compromise |
| **Logical / Statement** | Replicate SQL statements | Heterogeneous |
| **Physical / Block** | Replicate WAL/log records | Exact standby |
| **Multi-master** | All nodes accept writes | HA + write scaling, needs conflict resolution |

| Engine | Options |
|---|---|
| MS SQL | Always On Availability Groups, Log Shipping, Transactional/Snapshot/Merge replication |
| PostgreSQL | Streaming (physical), Logical (PG 10+), Patroni/repmgr for HA |
| MySQL | Async source-replica, GTID, semi-sync, Group Replication, InnoDB Cluster |
| Oracle | Data Guard, GoldenGate, Active Data Guard, RAC |

---

## 10. Backup, Restore, and Recovery

### 10.1 Recovery Objectives

| Term | Meaning |
|---|---|
| **RPO** (Recovery Point Objective) | Maximum acceptable data loss (time) |
| **RTO** (Recovery Time Objective) | Maximum acceptable downtime to restore |
| **MTTR** | Mean time to recover — measure it |

### 10.2 Backup Types

| Type | Captures | Speed |
|---|---|---|
| Full | Entire database | Slowest backup, fastest restore |
| Differential | Changes since last full | Medium both ways |
| Incremental / Log | Changes since last backup | Fastest backup, slow restore (chain replay) |
| Snapshot | Point-in-time COW | Near-instant |

### 10.3 Recovery Models

**MS SQL:** `SIMPLE` (no PITR), `FULL` (supports PITR), `BULK_LOGGED`.

**PostgreSQL:** Always WAL-based. PITR via base backup + archived WAL (`archive_mode=on`).

**MySQL:** Logical (`mysqldump`, mydumper) or Physical (Percona XtraBackup). Binlogs enable PITR.

**Oracle:** RMAN is canonical. `ARCHIVELOG` mode required for PITR.

### 10.4 Backup Best Practices

- **3-2-1 rule:** 3 copies, 2 different media, 1 offsite
- Automate; monitor success; alert on failure
- **Periodically restore** to a separate machine — an untested backup is not a backup
- Encrypt backups at rest and in transit
- Verify integrity (`RESTORE VERIFYONLY`, `RMAN VALIDATE`, `pg_basebackup` checksums)
- Document the runbook and rehearse the recovery

### 10.5 Crash Recovery vs Restore

Crash recovery is **automatic** on engine restart: replay log records since the last checkpoint. Restore is **operator-initiated**: load a backup and replay logs.

---

## 11. Security and Access Control

### 11.1 Principle of Least Privilege

Grant the minimum permissions required. Use roles to bundle permissions; assign users to roles. **Never let applications connect as superuser/sysadmin.**

### 11.2 Authentication Mechanisms

| Engine | Options |
|---|---|
| MS SQL | Windows Auth (Kerberos/NTLM), SQL Auth, Azure AD, certificate |
| PostgreSQL | `pg_hba.conf`: trust, md5, scram-sha-256 (preferred), peer, ldap, gss, cert |
| MySQL | `caching_sha2_password` (default 8.0+), `mysql_native_password`, pam, ldap |
| Oracle | OS auth, Oracle-managed, Kerberos, RADIUS, certificate, IAM |

### 11.3 Roles and Privileges

```sql
CREATE ROLE app_reader;
GRANT SELECT ON ALL TABLES IN SCHEMA reporting TO app_reader;   -- PG
CREATE USER app_user WITH PASSWORD '...';
GRANT app_reader TO app_user;
```

### 11.4 Row-Level Security

- **MS SQL:** Row-Level Security (`CREATE SECURITY POLICY`) + Dynamic Data Masking
- **PostgreSQL:** `ALTER TABLE ... ENABLE ROW LEVEL SECURITY` + `CREATE POLICY`
- **Oracle:** Virtual Private Database (VPD)
- **MySQL:** views with WHERE filters; no native RLS

### 11.5 Encryption

| Layer | Technology |
|---|---|
| At rest (file) | TDE (MS SQL, Oracle, MySQL Enterprise); pgcrypto/disk-level for PG |
| In transit | TLS; require strong cipher suites |
| Column-level | Always Encrypted (MS SQL), Oracle TDE column, pgcrypto, MySQL AES_ENCRYPT |
| Backup | Encrypted backup in all engines |

### 11.6 SQL Injection Prevention

The single most common vulnerability.

```sql
-- BAD (string interpolation)
sql = "SELECT * FROM users WHERE name = '" + name + "'";

-- GOOD (parameterized)
sql = "SELECT * FROM users WHERE name = ?";
stmt.setString(1, name);
```

**Always** use parameterized queries. Defense in depth: least-privilege DB account, WAF, input validation.

### 11.7 Auditing

- **MS SQL:** SQL Server Audit (Extended Events)
- **PostgreSQL:** `pgaudit` extension
- **MySQL:** Audit Plugin (Enterprise) or Percona
- **Oracle:** Unified Auditing (12c+), Database Vault

---

## 12. High Availability and Disaster Recovery

### 12.1 HA Topologies

| Pattern | Failover Time | Data Loss | Cost |
|---|---|---|---|
| Cold standby | Hours | Up to last backup | Low |
| Warm standby (log shipping) | Minutes | Last shipped log | Low |
| Hot standby (sync replication) | Seconds | Zero (sync) / minimal (async) | Medium |
| Shared-disk cluster | Seconds | Zero | High (Oracle RAC, MS SQL FCI) |
| Multi-master | Zero | Needs conflict resolution | Highest |

### 12.2 Vendor HA Solutions

**MS SQL:** Always On Availability Groups (sync/async, readable secondaries), Failover Cluster Instance, Log Shipping.

**PostgreSQL:** Streaming replication + Patroni/repmgr/Stolon for failover. PgBouncer/HAProxy for routing. Cloud: RDS Multi-AZ, Aurora.

**MySQL:** InnoDB Cluster (Group Replication + Router + Shell). Galera (Percona XtraDB, MariaDB). Async with Orchestrator.

**Oracle:** Data Guard (sync/async), RAC (shared-disk active-active), GoldenGate (logical, heterogeneous).

---

## 13. Monitoring and Observability

### 13.1 Key Metrics to Track

| Category | Metrics |
|---|---|
| System | CPU, memory, disk I/O (IOPS, latency, queue), network |
| Database | Active sessions, txns/sec, deadlocks, blocked sessions, longest query |
| Cache | Buffer pool hit ratio, page life expectancy |
| Storage | Database size growth, log size, free space, fragmentation |
| Replication | Replication lag, transactions behind primary |
| Backup | Last successful backup, duration, size |
| Workload | Top SQL by CPU/IO/duration, query frequency, plan changes |

### 13.2 Vendor Monitoring Tools

| Engine | Built-in | Common Add-ons |
|---|---|---|
| MS SQL | DMVs, Extended Events, Query Store, Activity Monitor | RedGate, SentryOne, SolarWinds DPA, Datadog |
| PostgreSQL | `pg_stat_*`, `pg_stat_statements`, `auto_explain` | pgBadger, pgwatch2, pganalyze, Prometheus |
| MySQL | Performance Schema, sys schema, slow query log | Percona PMM, MySQL Enterprise Monitor |
| Oracle | AWR, ASH, Statspack, V$ views, Enterprise Manager | Oracle Cloud Control, APMs |

### 13.3 Alerting Hygiene

- Alert on **user-visible symptoms**, not just resource thresholds
- Tier alerts: critical (paged), warning (logged), info (dashboard)
- Avoid noisy alerts — false positives train on-call to ignore real ones
- Always have a runbook for any alert that wakes someone up

---

## 14. Stored Procedures, Functions, and Triggers

### 14.1 Stored Procedures

| Engine | Procedural Language |
|---|---|
| MS SQL | T-SQL |
| PostgreSQL | PL/pgSQL (default), PL/Python, PL/Perl, PL/V8 |
| MySQL | SQL/PSM-based |
| Oracle | PL/SQL — full language with packages, exceptions, collections |

```sql
-- T-SQL
CREATE OR ALTER PROCEDURE up_customer_total
    @cust_id INT, @total MONEY OUTPUT
AS
BEGIN
    SET NOCOUNT ON;
    SELECT @total = SUM(total) FROM orders WHERE customer_id = @cust_id;
END;

-- PL/pgSQL
CREATE OR REPLACE FUNCTION customer_total(p_cust_id INT)
RETURNS NUMERIC AS $$
DECLARE v_total NUMERIC;
BEGIN
    SELECT SUM(total) INTO v_total FROM orders WHERE customer_id = p_cust_id;
    RETURN v_total;
END;
$$ LANGUAGE plpgsql;
```

### 14.2 Functions

Scalar (single value), table-valued (result set), aggregate (combine rows).

> ⚠️ **Warning:** Scalar UDFs called row-by-row in `SELECT` can devastate performance. MS SQL 2019+ inlines scalar UDFs in many cases; otherwise refactor to inline table-valued functions.

### 14.3 Triggers

| Variant | Description |
|---|---|
| Row-level | Fires once per affected row (Oracle, PG, MySQL) |
| Statement-level | Fires once per statement (MS SQL default; Oracle, PG) |
| BEFORE / AFTER | Pre or post action |
| INSTEAD OF | Replaces the action — used to make views updatable |

> ⚠️ Triggers are hard to debug, easy to nest unintentionally, and add hidden cost. Prefer constraints and application logic.

---

## 15. Routine DBA Tasks

### 15.1 Daily

- [ ] Verify backups completed successfully
- [ ] Check replication lag and broken replicas
- [ ] Review error log / alert log for new errors
- [ ] Scan top-resource queries for new offenders
- [ ] Disk space and growth check

### 15.2 Weekly

- [ ] Review long-running, high-cost queries; tune as needed
- [ ] Check index fragmentation; rebuild/reorganize the worst offenders
- [ ] Review wait stats trends (deltas, not just current)
- [ ] Test a backup restore to a non-production environment
- [ ] Apply non-urgent patches in dev/test

### 15.3 Monthly / Quarterly

- [ ] Capacity review — predict storage and CPU/IO needs
- [ ] Patch production with vendor security & cumulative updates
- [ ] Failover drill — actually fail over to the standby
- [ ] Review user access — remove stale accounts, audit privilege creep
- [ ] Rotate credentials, certificates, encryption keys

### 15.4 Sample Health-Check Queries

```sql
-- MS SQL — current blocking sessions
SELECT blocking_session_id, session_id, wait_type, wait_time, last_wait_type,
       text = SUBSTRING(t.text, r.statement_start_offset/2,
              (CASE r.statement_end_offset WHEN -1 THEN DATALENGTH(t.text)
                    ELSE r.statement_end_offset END - r.statement_start_offset)/2)
FROM sys.dm_exec_requests r
CROSS APPLY sys.dm_exec_sql_text(r.sql_handle) t
WHERE blocking_session_id <> 0;

-- PostgreSQL — running queries with locks
SELECT pid, usename, application_name, state, wait_event_type, wait_event,
       now() - query_start AS duration, query
FROM pg_stat_activity
WHERE state <> 'idle'
ORDER BY duration DESC;

-- MySQL — long-running queries
SELECT id, user, host, db, command, time, state, info
FROM information_schema.processlist
WHERE command <> 'Sleep' AND time > 30
ORDER BY time DESC;

-- Oracle — sessions and blocking
SELECT s.sid, s.serial#, s.username, s.status, s.event, s.blocking_session,
       s.sql_id, s.last_call_et AS seconds_in_state
FROM v$session s
WHERE s.username IS NOT NULL
ORDER BY s.last_call_et DESC;
```

---

## 16. Beyond Classic SQL

### 16.1 NoSQL Families

| Family | Examples | Sweet Spot |
|---|---|---|
| Document | MongoDB, Couchbase | Schema-flexible, hierarchical data |
| Key-Value | Redis, DynamoDB, Memcached | Fast lookups, caches, sessions |
| Wide-Column | Cassandra, ScyllaDB, HBase | Massive write throughput, time-series |
| Graph | Neo4j, Amazon Neptune | Relationship-heavy queries |
| Search | Elasticsearch, OpenSearch | Full-text and analytics |
| Time-series | InfluxDB, TimescaleDB, Prometheus | Metric ingestion, downsampling |
| Vector | pgvector, Pinecone, Milvus | Similarity search (AI workloads) |

### 16.2 Newer Capabilities in Mainstream RDBMS

- **JSON support** — JSONB in PG, JSON path in MySQL/MS SQL — one store handles relational + document
- **Columnstore indexes** (MS SQL) and **HeatWave** (MySQL) bring HTAP into the same engine
- **Distributed SQL:** CockroachDB, YugabyteDB, TiDB, Spanner
- **Cloud-managed:** RDS, Cloud SQL, Azure SQL DB, Aurora
- **Serverless SQL:** Aurora Serverless v2, Azure SQL serverless

---

## Appendix A — Cheat Sheets

### A.1 "Why is my query slow?" Checklist

- [ ] Get the **actual** execution plan (with row counts)
- [ ] Are estimated rows close to actual? If not → stale stats
- [ ] Scans where seeks should be? → missing or unusable index
- [ ] Key lookups on hot operators? → make index covering
- [ ] Spills to disk (sort, hash)? → increase work memory, add sorting index
- [ ] Predicates SARGable (no functions on indexed side)?
- [ ] Parameter types matching column types?
- [ ] Plan reused with bad parameters? → recompile / `OPTIMIZE FOR` / baselines
- [ ] Concurrency issue? → check waits, blocking, deadlocks

### A.2 Quick Index Decision Guide

**Add an index when ALL these are true:**
- Query runs frequently or is performance-critical
- Predicate is selective (returns < a few % of rows)
- Column(s) appear in `WHERE`, `JOIN`, `ORDER BY`, or `GROUP BY`
- Write penalty is acceptable (read:write ratio favors reads)

**Don't add an index when:**
- Table is small (a scan is faster)
- Column has very low cardinality
- Existing indexes already cover the predicate via a leading column
- Query is rare and not on a critical path

### A.3 Vendor Quick-Reference Card

| Need | MS SQL | PostgreSQL | MySQL | Oracle |
|---|---|---|---|---|
| Show plan | `SET STATISTICS XML ON` | `EXPLAIN ANALYZE` | `EXPLAIN [ANALYZE]` | `EXPLAIN PLAN` + `DBMS_XPLAN` |
| Top queries | `sys.dm_exec_query_stats` | `pg_stat_statements` | `events_statements_summary_by_digest` | `DBA_HIST_SQLSTAT` / AWR |
| Sessions | `sys.dm_exec_sessions` | `pg_stat_activity` | `INFORMATION_SCHEMA.PROCESSLIST` | `V$SESSION` |
| Update stats | `UPDATE STATISTICS` | `ANALYZE` | `ANALYZE TABLE` | `DBMS_STATS.GATHER_TABLE_STATS` |
| Lock waits | `sys.dm_tran_locks` | `pg_locks` + `pg_stat_activity` | `INNODB_LOCKS` / `data_locks` | `V$LOCK` / `DBA_BLOCKERS` |
| Backup | `BACKUP DATABASE` | `pg_basebackup` / `pg_dump` | `mysqldump` / XtraBackup | `RMAN BACKUP` |

---

## Appendix B — Suggested Learning Path

1. **Stage 1 — SQL fluency:** CRUD, joins, group-by, window functions, CTEs. Build queries reflexively.
2. **Stage 2 — One vendor deep:** Pick MS SQL, PostgreSQL, MySQL, or Oracle. Learn installation, configuration, tools, idioms cold.
3. **Stage 3 — Internals:** Pages, buffer pool, WAL, MVCC vs locking, isolation levels, statistics.
4. **Stage 4 — Performance:** Read execution plans daily. Diagnose top queries on real workload.
5. **Stage 5 — Operations:** Backups, restores, HA setup, monitoring, security, patching, runbooks.
6. **Stage 6 — Second vendor:** Compare and translate concepts across engines.
7. **Stage 7 — Architecture:** Sharding, distributed SQL, hybrid cloud, CDC pipelines, lakehouse.

The best DBAs blend three skill sets: **software engineering** (think in systems, automate everything), **database internals** (know why, not just what), and **operations** (calm under fire, document everything).

---

## Appendix C — Recommended Books

### Foundations

- **SQL Performance Explained** — Markus Winand. The single best SQL performance book. ~200 pages, focused on indexes across all four engines. [Free online](https://use-the-index-luke.com/).
- **SQL Antipatterns** — Bill Karwin (2nd ed. 2022). Bad designs every DBA sees and what to do instead.
- **SQL Cookbook** — Anthony Molinaro (2nd ed.). Recipe-format problems with solutions across engines.

### Database Internals & Theory

- **Database Internals** — Alex Petrov (O'Reilly, 2019). Modern classic on storage engines and distributed databases.
- **Designing Data-Intensive Applications** — Martin Kleppmann. The book that ties everything together.
- **Database System Concepts** — Silberschatz, Korth, Sudarshan (7th ed.). The textbook.

### MS SQL Server

- **T-SQL Fundamentals** → **T-SQL Querying** — Itzik Ben-Gan
- **SQL Server 2022 Query Performance Tuning** — Grant Fritchey
- **SQL Server Internals** — Kalen Delaney
- **Pro SQL Server Internals** — Dmitri Korotkevitch

### PostgreSQL

- **PostgreSQL: Up and Running** — Regina Obe & Leo Hsu (3rd ed.)
- **The Internals of PostgreSQL** — Hironobu Suzuki ([free online](https://www.interdb.jp/pg/))
- **PostgreSQL 14 Administration Cookbook** — Simon Riggs et al.
- **PostgreSQL Query Optimization** — Dombrovskaya, Novikov, Bailliekova (Apress, 2024)

### MySQL

- **High Performance MySQL** — Silvia Botros & Jeremy Tinley (4th ed. 2021)
- **MySQL 8 Query Performance Tuning** — Jesper Wisborg Krogh
- **MySQL Concurrency** — Jesper Wisborg Krogh

### Oracle

- **Oracle Database 19c DBA Handbook** — Bob Bryla
- **Oracle Core: Essential Internals for DBAs and Developers** — Jonathan Lewis
- **Cost-Based Oracle Fundamentals** — Jonathan Lewis
- **Troubleshooting Oracle Performance** — Christian Antognini
- **Expert Oracle Database Architecture** — Thomas Kyte

### Specialist Topics

- **The Art of SQL** — Stéphane Faroult
- **Refactoring Databases** — Scott Ambler & Pramod Sadalage
- **The Data Warehouse Toolkit** — Ralph Kimball & Margy Ross (3rd ed.)
- **Streaming Systems** — Tyler Akidau et al.

### Free Online

- [use-the-index-luke.com](https://use-the-index-luke.com/) — Winand's book, browsable
- [modern-sql.com](https://modern-sql.com/) — SQL features added since SQL-92
- [PostgreSQL docs](https://www.postgresql.org/docs/) — one of the best technical manuals ever written
- [Microsoft Learn for SQL Server](https://learn.microsoft.com/sql/)
- [MySQL Reference Manual](https://dev.mysql.com/doc/)
- [explain.depesz.com](https://explain.depesz.com/) and [explain.dalibo.com](https://explain.dalibo.com/) — PostgreSQL plan visualizers

---

## Contributing

Spotted an error, have a better example, or want to add a vendor-specific detail? PRs welcome.

1. Fork the repo
2. Create a feature branch (`git checkout -b fix/typo-in-chapter-7`)
3. Commit your changes with a clear message
4. Open a pull request

Please keep the tone consistent — practical, vendor-neutral where possible, and show-not-tell with code examples.

## License

This work is licensed under a [Creative Commons Attribution 4.0 International License](https://creativecommons.org/licenses/by/4.0/).

You are free to share and adapt, provided you give appropriate credit.

---

<p align="center">
  <sub>Built as a reference for the daily DBA.<br>If this helped, give it a ⭐</sub>
</p>
