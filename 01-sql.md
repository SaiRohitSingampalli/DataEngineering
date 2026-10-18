# Topic 1 — SQL

> Section 1 of 10 · 57 pages in source PDF · pages 35–91

[← Back to main](./README.md)

---

SQL Architect-Level Reference Principal / Staff Engineer Interview Notes Internals · Optimizer · Execution Plans · Indexing · Concurrency · Isolation · Replication · Sharding · Modeling · Trade-offs The only document you need for SQL deep-dives in staff-level interviews.

How to use this document This is not a syntax reference. This is a principal engineer's view of SQL and relational databases: how they work internally, what trade-offs the system makes for you, where they break, and how to reason about them under load. Every section combines four lenses: 1. CONCEPT — what is it, and the relational/CS theory behind it 2. MECHANICS — how the engine implements it (Postgres flavor primarily) 3. TRADE-OFFS — when to use it, when it hurts, what alternatives exist 4. INTERVIEW — the question they'll actually ask, and the answer that separates seniors from staff/principal candidates ■ Principal mindset: At staff/principal level, interviewers don't test syntax. They test whether you can reason about data volume, access patterns, consistency requirements, and failure modes — and whether you can defend a design under pushback.

Table of Contents

## PART I — FOUNDATIONS

#### 1. Relational Theory & Why SQL Won

#### 2. The SELECT Statement: Logical vs Physical Execution

#### 3. Joins: Algorithms, Cost, and the Optimizer

#### 4. Aggregations, GROUP BY & HAVING

#### 5. Subqueries, CTEs & Lateral Joins

## PART II — ADVANCED SQL

#### 6. Window Functions Deep Dive

#### 7. Set Operations & Recursive CTEs

## PART III — ENGINE INTERNALS

#### 9. Storage: Heaps, Pages, Rows & TOAST

#### 10. Indexes: B-tree, Hash, GIN, GiST, BRIN

#### 11. The Query Optimizer & Statistics

#### 12. Reading EXPLAIN / EXPLAIN ANALYZE

## PART IV — CONCURRENCY & CORRECTNESS

#### 14. Transactions & ACID

#### 15. Isolation Levels & Anomalies

#### 16. Locking, Deadlocks & Concurrency Patterns

## PART V — PERFORMANCE ENGINEERING

#### 17. Query Tuning Playbook

#### 18. Index Design Patterns

#### 19. Partitioning Strategies

#### 20. Anti-Patterns at Scale

## PART VI — DATA MODELING

#### 21. Normalization vs Denormalization

#### 22. Dimensional Modeling & SCDs

#### 23. Modeling for OLTP vs OLAP

## PART VII — DISTRIBUTED & SCALE

#### 24. Replication: Sync, Async, Logical, Physical

#### 25. Sharding & Read Replicas

#### 26. Distributed Transactions: 2PC & Saga

## PART VIII — INTERVIEW PRACTICE

#### 28. The Staff-Level Interview Loop

#### 29. War Stories & Common Pitfalls

#### 30. Cheat Sheet & Mental Models

## PART I — FOUNDATIONS

#### 1. Relational Theory & Why SQL Won

  - 1.1 The Relational Model
- E. F. Codd published A Relational Model of Data for Large Shared Data Banks in 1970. The core insight: data
- should be modeled as relations (sets of tuples), and access should be declarative — you state what you
- want, not how to get it. The DBMS figures out the how.
- Three pillars of the relational model:
- 1. STRUCTURE     — data is organized into relations (tables) of tuples (rows)
- 2. INTEGRITY     — constraints enforce correctness (PK, FK, CHECK, NOT NULL)
- 3. MANIPULATION  — relational algebra (and SQL) operates on whole sets

```
1.2 Relational Algebra → SQL
Every SQL operator maps to relational algebra. Knowing this gives you a mental model for what the optimizer
can rewrite.
Relational Algebra
SQL
What it does
σ (selection)
WHERE
Filter rows
π (projection)
SELECT cols
Pick columns
■ (natural join)
JOIN ... ON
Match tuples
× (Cartesian)
CROSS JOIN
All pairs
∪ (union)
UNION
Combine relations
∩ (intersect)
INTERSECT
Common tuples
− (difference)
EXCEPT
Tuples in A not in B
γ (group/aggregate)
GROUP BY
Fold a relation
```

1.3 Closure & Composability The deepest property of relational algebra is closure: every operator takes relations and returns a relation. This is why subqueries, CTEs, and views all work the same way — they produce relations you can feed into the next operator. SQL's expressive power comes directly from this.

  - 1.4 Why SQL Beat Alternatives
- 1. DECLARATIVE     — describe results, not navigation
- 2. PHYSICAL INDEPENDENCE — engine can reorganize storage without breaking queries
- 3. SET-BASED       — one query operates on millions of rows
- 4. STANDARD        — same language across vendors (mostly)
- 5. OPTIMIZABLE     — the engine can rewrite your query if a faster plan exists
- ■ Principal mindset: When asked 'why SQL vs NoSQL', the staff-level answer is not about scale. It's about
- schema enforcement, transactional consistency, and the cost of expressing joins in application code.
- NoSQL didn't kill SQL — it carved out workloads where the rigid relational model was the wrong abstraction.

> **🎯 Interview Angle:** Q: Why are JOIN operations expensive? Senior: 'They combine rows from multiple tables.' Staff: 'A join's cost depends on the algorithm (nested loop, hash, merge), the cardinality of each input, the selectivity of the join predicate, whether indexes exist on the join keys, and whether intermediate state fits in memory. A nested loop on indexed columns over a small outer is cheap; a hash join across two 100M-row tables is bounded by memory and may spill to disk.'

#### 2. The SELECT Statement: Logical vs Physical

```sql
2.1 Logical Execution Order
SQL is written in one order and evaluated in another. The logical order is fixed by the standard.
WRITTEN ORDER:                      LOGICAL ORDER:
SELECT     col_list                 1. FROM       → sources, joins
FROM       table                    2. WHERE      → filter rows
WHERE      filter                   3. GROUP BY   → form groups
GROUP BY   cols                     4. HAVING     → filter groups
HAVING     filter                   5. SELECT     → compute columns, windows
ORDER BY   cols                     6. DISTINCT   → dedupe
LIMIT      n                        7. ORDER BY   → sort
                                    8. LIMIT      → cap rows
This order explains the famous quirks: you can't reference a SELECT alias in WHERE (WHERE runs first),
but you can in ORDER BY. Window functions run in SELECT, so they see post-WHERE data but
pre-DISTINCT.
```

2.2 Physical Execution The optimizer is free to do anything as long as the result is equivalent. It may push filters down through joins, reorder joins, choose different algorithms, materialize intermediates, or evaluate in parallel. Logical plan: Physical plan: SELECT u.name, SUM(o.amount) Aggregate (HashAggregate) FROM users u ■■ Hash Join JOIN orders o ON o.user_id=u.id ■■ Seq Scan users WHERE u.country='US' ■ filter: country='US' GROUP BY u.name; ■■ Index Scan orders_user_id_idx

```sql
2.3 Predicate Pushdown
The most important rewrite. The optimizer pushes WHERE predicates as deep into the plan as possible — to
the scan level — so the engine reads fewer rows.
-- You wrote:
SELECT * FROM (SELECT id, name FROM users) u WHERE u.id = 42;
-- Optimizer rewrites to:
SELECT id, name FROM users WHERE id = 42;
-- ...and uses the PK index on users.id.
```

2.4 Projection Pushdown & Column Pruning Unused columns are dropped early. In columnar engines (BigQuery, Snowflake, Parquet) this is huge — only the read columns are loaded from disk. Implication: SELECT * defeats this. ■ Production scenario: A reporting query at scale was reading 4 TB/day from Snowflake. The query used SELECT * into a CTE then projected three columns. Rewriting it to project the three columns inside the CTE dropped the scan to 180 GB/day — same result, 22× less I/O. The optimizer can't always see through complex CTEs. ■ Principal mindset: Logical order is what you debug with. Physical order is what you pay for. Staff candidates can switch between the two views during a discussion without losing the thread.

#### 3. Joins: Algorithms, Cost, and the Optimizer

3.1 The Five Logical Join Types INNER JOIN — only matched rows from both sides LEFT [OUTER] JOIN — all left rows; nulls where right unmatched RIGHT [OUTER] JOIN — mirror of LEFT (rarely used; prefer LEFT) FULL [OUTER] JOIN — all rows from both, nulls where unmatched CROSS JOIN — Cartesian product (no ON clause) -- ANTI / SEMI joins are LOGICAL forms — written as EXISTS / NOT EXISTS / IN / NOT IN -- but the optimizer treats them as first-class join types internally.

#### 3.2 The Three Physical Join Algorithms

```
3.2.1 Nested Loop Join
for each row R in outer:
    for each row S in inner:
        if R.key = S.key: emit (R, S)
Cost: O(N × M)       worst case
Cost: O(N × log M)   if inner has index on the join key
Cost: O(N + M)       with index on inner & outer iterates ordered
OPTIMIZER PICKS IT WHEN:
   Outer side is very small
   Inner side has an index on the join key
   Correlated subqueries
```

3.2.2 Hash Join 1. BUILD phase: scan smaller side, build in-memory hash table on join key 2. PROBE phase: scan larger side, hash each row's key, look up in hash table Cost: O(N + M) in memory Cost: O((N + M) × passes) if it spills to disk OPTIMIZER PICKS IT WHEN:  Equi-join only (cannot use < or >)  Both inputs are large and unsorted  Smaller side fits in work_mem

3.2.3 Sort-Merge Join 1. SORT both sides on the join key (skip if already sorted) 2. MERGE: walk both sorted streams in lockstep Cost: O(N log N + M log M) if sorts needed Cost: O(N + M) if both pre-sorted (clustered index) OPTIMIZER PICKS IT WHEN:  Both sides already sorted  Very large inputs that don't fit hash table in memory  Range joins (< or >) DISTRIBUTED SYSTEMS: Sort-merge is the dominant join in Spark — it streams and parallelizes.

3.3 Picking the Right Algorithm Outer size Inner size Index on inner? Best algorithm Small (<1K) Any Yes Nested Loop

Small Any No Hash (build on small) Large Large No Hash, or Sort-Merge Pre-sorted Pre-sorted — Merge Non-equi (<, >) Any — Nested Loop or Merge

```sql
3.4 Join Ordering
Given N tables, there are N! join orders. The optimizer uses statistics to estimate cardinality at each step and
picks the order that minimizes intermediate result sizes. Rule of thumb: join the smallest, most-selective
tables first.
-- Three tables, join order matters
SELECT * FROM A
JOIN B ON A.bid = B.id
JOIN C ON B.cid = C.id;
-- Order 1: (A ■ B) ■ C  →  intermediate = |A ■ B|
-- Order 2: A ■ (B ■ C)  →  intermediate = |B ■ C|
-- For large N, the search space is too big. Most optimizers use
-- dynamic programming up to ~12 tables, genetic algorithm beyond.
```

```sql
3.5 Anti-Joins & Semi-Joins
-- Semi-join: rows in A that have a match in B (don't return B's cols)
SELECT * FROM users u WHERE EXISTS (
    SELECT 1 FROM orders o WHERE o.user_id = u.id);
-- Optimizer stops at first match. No duplicates.
-- Anti-join: rows in A with NO match in B
SELECT * FROM users u WHERE NOT EXISTS (
    SELECT 1 FROM orders o WHERE o.user_id = u.id);
-- DANGER: don't do this:
SELECT * FROM users WHERE id NOT IN (SELECT user_id FROM orders);
-- ■■ If user_id has NULLs, this returns ZERO rows (3-valued logic trap).
-- Prefer NOT EXISTS, which handles NULLs correctly.
■■ Warning: NOT IN with a nullable subquery is the most common SQL bug in production. Always prefer NOT
EXISTS unless you've explicitly filtered NULLs from the subquery.
■ Interview angle: Q: A join between a 10M-row table and a 100-row table is slow. Why?
Staff answer: Possibilities: (1) Stats are stale and the optimizer picked nested loop with the 10M table as outer —
fix: ANALYZE. (2) The join column on the small side has no index, forcing seq scans of both. (3) Skew: the 100 rows
all match the same 9M rows in the big table. (4) The join column has mismatched types causing implicit cast that
bypasses the index. EXPLAIN ANALYZE shows which.
```

#### 4. Aggregations, GROUP BY & HAVING

```sql
4.1 The Two Aggregation Algorithms
HASH AGGREGATE
   Build a hash table keyed by grouping cols
   For each input row, look up / insert and update aggregate state
   Memory: O(#distinct groups)
   Spills to disk if too many groups
SORT AGGREGATE (Group Aggregate)
   Sort input by grouping cols
   Walk sorted stream, emit a group every time the key changes
   Memory: O(1) — only current group held in memory
   Cost: O(N log N) for the sort
PICK: Hash if groups fit in memory; Sort if input is already sorted
      (e.g. by an index on the GROUP BY columns).
```

```sql
4.2 WHERE vs HAVING
WHERE   filters ROWS  (before grouping)  — can use indexes
HAVING  filters GROUPS (after grouping)   — runs on aggregate results
-- BAD: filtering with HAVING when WHERE would do
SELECT dept_id, COUNT(*)
  FROM employees
 GROUP BY dept_id
HAVING dept_id = 5;
-- Computes counts for ALL departments, then throws all but one away.
-- GOOD:
SELECT dept_id, COUNT(*)
  FROM employees
 WHERE dept_id = 5
 GROUP BY dept_id;
```

4.3 Aggregate Functions Function Returns Note COUNT(*) all rows incl. NULLs fastest COUNT(col) non-NULL count rare bug source COUNT(DISTINCT x) distinct non-NULLs expensive — hash SUM, AVG, MIN, MAX over non-NULLs NULLs ignored PERCENTILE_CONT continuous percentile WITHIN GROUP (ORDER BY) STRING_AGG concat strings ordering needs ORDER BY ARRAY_AGG aggregate to array great for list-rollups BOOL_AND / BOOL_OR logical aggregation "all/any" checks

4.4 GROUPING SETS, ROLLUP, CUBE -- ROLLUP: hierarchical subtotals SELECT region, country, SUM(sales) FROM orders GROUP BY ROLLUP(region, country); -- Returns: (region, country), (region, NULL), (NULL, NULL)

```sql
-- CUBE: all combinations
SELECT region, country, SUM(sales)
  FROM orders
 GROUP BY CUBE(region, country);
-- Returns all 4 subsets: (R,C), (R), (C), ()
-- GROUPING SETS: explicit list (most powerful)
SELECT region, country, product, SUM(sales)
  FROM orders
 GROUP BY GROUPING SETS (
     (region, country),
     (region, product),
     (region),
     ()
 );
-- Replaces N separate queries UNION ALL'd together — one pass over the data.
■ Tip: GROUPING SETS is the single biggest performance win for OLAP reporting queries that compute multiple
aggregation levels. Most engines compute it in a single scan.
```

4.5 DISTINCT and APPROX SELECT DISTINCT a, b FROM t; -- semantically equivalent to ↓ SELECT a, b FROM t GROUP BY a, b; -- COUNT(DISTINCT x): pricey for high-cardinality x -- For approximate distinct counts, use HyperLogLog: SELECT APPROX_COUNT_DISTINCT(user_id) FROM events; -- BigQuery, Redshift SELECT count(DISTINCT user_id) FROM events; -- exact, slower ■ Principal mindset: COUNT(DISTINCT) is the silent killer of OLAP performance. At staff level you should know its alternatives: HyperLogLog (approximate, O(1) memory), pre-aggregation via a daily roll-up table, or Theta sketches when you need DISTINCT counts across dimensions you'll later intersect.

#### 5. Subqueries, CTEs & Lateral Joins

  - 5.1 Four Kinds of Subquery
- 1. SCALAR SUBQUERY      — returns single value
- SELECT name, (SELECT MAX(amount) FROM orders WHERE user_id=u.id) FROM users u;
- 2. ROW SUBQUERY         — returns one row
- WHERE (a, b) = (SELECT x, y FROM ...)
- 3. TABLE SUBQUERY       — returns a table
- FROM (SELECT * FROM ...) AS t
- 4. CORRELATED SUBQUERY  — references outer query columns
- SELECT * FROM users u WHERE EXISTS (
- SELECT 1 FROM orders WHERE user_id = u.id);

```sql
5.2 Correlated vs Uncorrelated & Decorrelation
Correlated subqueries are conceptually 'evaluated once per outer row'. In practice, modern optimizers
decorrelate them — rewriting EXISTS as a semi-join, NOT EXISTS as an anti-join, and scalar correlated
subqueries as LEFT JOIN + aggregation. If decorrelation fails, performance collapses.
-- Looks O(N²) but optimizer turns into a semi-join → O(N+M):
SELECT * FROM users u
WHERE EXISTS (SELECT 1 FROM orders o WHERE o.user_id = u.id);
-- May NOT decorrelate cleanly — verify with EXPLAIN:
SELECT u.id, (SELECT COUNT(*) FROM orders WHERE user_id = u.id) AS n
  FROM users u;
-- Many engines run this per-row. Rewrite with join + agg:
SELECT u.id, COALESCE(o.n, 0) AS n
  FROM users u LEFT JOIN
       (SELECT user_id, COUNT(*) AS n FROM orders GROUP BY user_id) o
       ON o.user_id = u.id;
```

```sql
5.3 Common Table Expressions (CTEs)
WITH active_users AS (
    SELECT id FROM users WHERE last_login > now() - interval '30 days'
)
SELECT au.id, COUNT(o.id)
  FROM active_users au LEFT JOIN orders o ON o.user_id = au.id
 GROUP BY au.id;
-- BEWARE:
--   Postgres < 12: CTEs were "optimization fences" — always materialized
--   Postgres ≥ 12: CTEs are inlined by default (NOT MATERIALIZED)
--   Force materialization: WITH x AS MATERIALIZED (...)
--   BigQuery, Snowflake: typically inline always
■■ Warning: If you wrote CTE-heavy code on Postgres 11 expecting an optimization fence, upgrading to 12+
silently changed your plans. Check with EXPLAIN — use MATERIALIZED to force the old behavior when you
actually wanted it (e.g. expensive computation reused multiple times).
```

5.4 Lateral Joins A lateral join lets a subquery on the right side reference columns from the left side. It's a 'for each row, run this subquery' join, properly part of the FROM clause. -- Top 3 orders per user — classic top-N-per-group SELECT u.id, o.*

```sql
  FROM users u
  CROSS JOIN LATERAL (
      SELECT * FROM orders
       WHERE user_id = u.id
       ORDER BY created_at DESC
       LIMIT 3
  ) o;
-- LATERAL shines when subquery returns 0–N rows per outer row
-- and you want dependent execution semantics.
■ Interview angle: Q: How do you compute the top 3 orders per user?
Senior: 'ROW_NUMBER() window function.'
Staff: 'Two main approaches. ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY created_at DESC)
then filter rn ≤ 3 — scales linearly with table size. Or a LATERAL join — better when there are few users but many
orders per user and an index on (user_id, created_at). The lateral version uses the index to fetch just 3 rows per
user; the window version sorts all orders. Pick based on the ratio of users to orders and what indexes exist.'
```

## PART II — ADVANCED SQL

#### 6. Window Functions Deep Dive

6.1 The Mental Model A window function computes a value per row, looking at a window of related rows. Unlike aggregates, the row is preserved. SELECT user_id, order_date, amount, SUM(amount) OVER (PARTITION BY user_id ORDER BY order_date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW ) AS running_total FROM orders; -- 3 parts to a window spec: -- PARTITION BY — like GROUP BY, but rows preserved -- ORDER BY — defines row order WITHIN the partition -- FRAME — which rows to include (ROWS / RANGE)

```
6.2 The Function Families
1. RANKING
   ROW_NUMBER()    — unique sequential number within partition
   RANK()          — same rank for ties; gaps after  (1,2,2,4)
   DENSE_RANK()    — same rank for ties; no gaps     (1,2,2,3)
   NTILE(n)        — bucket rows into n tiles
   PERCENT_RANK()  — (rank - 1) / (count - 1)
   CUME_DIST()     — cumulative distribution
2. NAVIGATION
   LAG(col, n=1)   — value n rows before current
   LEAD(col, n=1)  — value n rows after current
   FIRST_VALUE     — first in frame
   LAST_VALUE      — last in frame ■ default frame surprises people
   NTH_VALUE(col, n)
3. AGGREGATES OVER WINDOW
   SUM/AVG/COUNT/MIN/MAX/STDDEV OVER (...)
```

```
6.3 Frames: ROWS vs RANGE vs GROUPS
ROWS    — physical row offsets        (1 row = 1 unit)
RANGE   — logical value range         (within ±X of current value)
GROUPS  — peer groups (SQL:2003, only some engines)
-- Running total (physical rows)
SUM(x) OVER (ORDER BY t ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
-
day moving sum (logical range on date)
SUM(x) OVER (ORDER BY t RANGE BETWEEN INTERVAL '6 days' PRECEDING AND CURRENT ROW)
-- DEFAULT FRAME if you omit it:
--   With ORDER BY:    RANGE UNBOUNDED PRECEDING → CURRENT ROW
--   Without ORDER BY: entire partition
-- That's why LAST_VALUE often surprises — its default frame ends at CURRENT ROW.
```

> **⚠️ Warning:** ■ LAST_VALUE with the default frame returns the current row's value, not the last in the partition. Use ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING.

```sql
6.4 Common Window Patterns
-- 1. TOP N PER GROUP
SELECT * FROM (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY user_id
                                  ORDER BY created_at DESC) rn
    FROM orders
) t WHERE rn <= 3;
-- 2. RUNNING TOTAL / CUMULATIVE
SUM(amount) OVER (PARTITION BY user_id ORDER BY order_date)
-- 3. MOVING AVERAGE (7-day)
AVG(amount) OVER (PARTITION BY user_id ORDER BY order_date
                  ROWS BETWEEN 6 PRECEDING AND CURRENT ROW)
-- 4. PERIOD-OVER-PERIOD
LAG(revenue, 1) OVER (PARTITION BY product ORDER BY month) AS prev_month
-- 5. GAPS AND ISLANDS (find consecutive runs)
SELECT user_id, MIN(date), MAX(date)
FROM (
    SELECT user_id, date,
           date - INTERVAL (ROW_NUMBER() OVER
                            (PARTITION BY user_id ORDER BY date)) DAY AS grp
    FROM daily_login
) t GROUP BY user_id, grp;
-- 6. PERCENT OF TOTAL
amount * 100.0 / SUM(amount) OVER (PARTITION BY category) AS pct_of_cat
-- 7. DEDUPE: keep only latest version per key
SELECT * FROM (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY id ORDER BY updated_at DESC) rn
    FROM raw_events
) t WHERE rn = 1;
```

```sql
6.5 Performance Notes
 Windows require a SORT (unless an index provides the order).
 Same OVER() across multiple windows → ONE sort pass.
  Use WINDOW clauses to dedupe specs:
    SELECT a, SUM(x) OVER w, AVG(x) OVER w
    FROM t
    WINDOW w AS (PARTITION BY g ORDER BY t);
 Partitioning by a high-cardinality column makes the sort scale poorly.
 For huge datasets, consider precomputing rolling aggregates in ETL.
■ Interview angle: Q: Walk me through the gaps-and-islands problem.
Staff answer: Given dates per user, find consecutive-day runs. Trick: subtract a row-number (per user) from the
date — consecutive dates share the same difference, so they share a 'group key'. Then GROUP BY user_id and that
group key, taking MIN and MAX. Works for any monotone sequence: timestamps, IDs, page numbers. The
canonical example of combining window functions with grouping.
```

#### 7. Set Operations & Recursive CTEs

7.1 Set Operations UNION — concat + dedupe (expensive: hash or sort) UNION ALL — concat, keep duplicates (cheap: just stream) INTERSECT — rows in both EXCEPT/MINUS — rows in A not in B -- Rule of thumb: default to UNION ALL. -- Use UNION only if you genuinely need dedup — most of the time you don't, -- and you're paying for an unnecessary sort.

```sql
7.2 Recursive CTEs
Recursive CTEs implement fixed-point iteration in SQL. They have two parts: an anchor (seed rows) and a
recursive step that references the CTE itself. The engine iterates until the recursive step returns no rows.
WITH RECURSIVE org_chart AS (
    -- Anchor: the CEO
    SELECT id, name, manager_id, 1 AS depth
      FROM employees WHERE manager_id IS NULL
    UNION ALL
    -- Recursive step: anyone reporting to a row we've already found
    SELECT e.id, e.name, e.manager_id, oc.depth + 1
      FROM employees e
      JOIN org_chart oc ON e.manager_id = oc.id
)
SELECT * FROM org_chart ORDER BY depth, name;
```

```sql
7.3 Practical Recursive Patterns
-- 1. GRAPH TRAVERSAL with cycle prevention
WITH RECURSIVE reachable AS (
    SELECT from_node, to_node, ARRAY[from_node] AS path
      FROM edges WHERE from_node = 1
    UNION ALL
    SELECT r.from_node, e.to_node, r.path || e.to_node
      FROM reachable r
      JOIN edges e ON e.from_node = r.to_node
     WHERE NOT (e.to_node = ANY(r.path))     -- cycle prevention
)
SELECT * FROM reachable;
-- 2. SEQUENCE GENERATION (but prefer generate_series in Postgres)
WITH RECURSIVE nums(n) AS (
    SELECT 1 UNION ALL
    SELECT n+1 FROM nums WHERE n < 100
) SELECT * FROM nums;
-- 3. NESTED JSON / BOM EXPLOSION / DATE RANGES
■■ Warning: Recursive CTEs don't terminate if your recursive step doesn't shrink the frontier. Always add a depth
limit or visited-set check.
■ Principal mindset: Recursive CTEs are O(N) per level — total cost O(N × depth). For deep hierarchies (>1000
levels), consider materialized path ('1/4/17/89'), nested set (left/right boundaries), or a closure table
(precomputed ancestor pairs). Each has trade-offs for reads vs writes.
■ Interview angle: Q: How would you represent and query a 10M-node hierarchy, 6 levels deep, read-heavy (show
all descendants of X)?
Staff answer: Closure table. Precompute every (ancestor, descendant, depth) pair. Queries become a single
```

indexed lookup. Inserts cost O(depth) per node but happen rarely. Materialized path is the runner-up — string operations are fast but require care around updates. Recursive CTE on every query is the wrong choice for read-heavy workloads.

```
8. NULL Semantics & Three-Valued Logic
8.1 NULL is not a value — it is the absence of a value
NULL = NULL        →  UNKNOWN (not TRUE!)
NULL <> NULL       →  UNKNOWN
NULL + 1           →  NULL
NULL OR TRUE       →  TRUE     (the only short-circuit that works)
NULL AND FALSE     →  FALSE    (the other)
NULL OR FALSE      →  NULL
NULL AND TRUE      →  NULL
-- WHERE only passes rows where the predicate is TRUE.
-- UNKNOWN is rejected — same as FALSE for filtering purposes.
```

```
8.2 Testing for NULL
x = NULL          ■ always UNKNOWN
x IS NULL         ✓
x IS NOT NULL     ✓
x IS DISTINCT FROM y    -- treats NULLs as equal (NULL=NULL → TRUE)
COALESCE(x, default)    -- replace NULL
NULLIF(x, y)            -- NULL if x=y, else x
```

```
8.3 NULL in Aggregates
COUNT(*)       — all rows
COUNT(col)     — rows where col IS NOT NULL
SUM, AVG, ...  — ignore NULLs
   SUM of all-NULL column → NULL (not 0!)
   COALESCE(SUM(x), 0) if you want zero for empty/all-null
```

8.4 NULL in JOINs and Constraints  Join keys with NULL never match (NULL = NULL is UNKNOWN).  UNIQUE: most engines allow multiple NULLs (Postgres, Oracle, MySQL). SQL Server treats NULL as a value and allows only one NULL.  PRIMARY KEY cannot be NULL.  FK can be NULL — a NULL FK means 'unrelated'.

```sql
8.5 The NOT IN NULL Trap (revisited)
-- Setup
SELECT * FROM A WHERE id NOT IN (SELECT id FROM B);
-- If ANY row in B has id=NULL:
-- NOT IN expands to: id != b1 AND id != b2 AND ... AND id != NULL
-- The "!= NULL" makes the entire AND chain UNKNOWN.
-- Result: ZERO rows returned, even though most should match.
-- Fixes:
SELECT * FROM A WHERE id NOT IN (SELECT id FROM B WHERE id IS NOT NULL);
SELECT * FROM A WHERE NOT EXISTS (SELECT 1 FROM B WHERE B.id = A.id);
■ Interview angle: Q: A user complains WHERE status <> 'shipped' doesn't return rows where status is
NULL. What do you tell them?
Staff answer: Three-valued logic. NULL <> 'shipped' evaluates to UNKNOWN, which WHERE treats as a
rejection. To include NULLs: WHERE status IS NULL OR status <> 'shipped', or WHERE
COALESCE(status, '') <> 'shipped'. Better: enforce NOT NULL + a default value at the schema level so
the application never reasons about NULLs.
```

> **🧠 Principal Mindset:** Staff candidates often have strong opinions about NULL. Defensible positions: (1) prefer NOT NULL with a sentinel where possible — eliminates 3VL bugs; (2) when NULL is required, wrap all comparisons in COALESCE; (3) never use NOT IN with a subquery without filtering NULLs.

## PART III — ENGINE INTERNALS

9. Storage: Heaps, Pages, Rows & TOAST 9.1 The Page — the Atomic Unit of I/O Disk I/O is too expensive to do one row at a time. Every relational engine groups rows into fixed-size pages (8 KB in Postgres, 16 KB in InnoDB, 8 KB in SQL Server). The page is the unit of read, write, lock, and cache. Postgres page layout (8 KB default): ■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■ ■ Header (24 bytes) ■ ■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■ ■ Item pointers (4 bytes each, grow ↓) ■ ■ ↓ ↓ ↓ ■ ■ ■ ■ FREE SPACE ■ ■ ■ ■ ↑ ↑ ↑ ■ ■ Tuples (grow ↑, variable size) ■ ■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■

```
9.2 Heaps and Clustered Indexes
HEAP TABLE (Postgres default, Oracle)
   Rows stored in insertion order, no inherent ordering
   All indexes are SECONDARY: index entry → (page, offset)
   A SELECT with index lookup → 2 reads: index page + heap page
CLUSTERED / INDEX-ORGANIZED TABLE (MySQL InnoDB, SQL Server, Oracle IOT)
   Rows stored in PRIMARY KEY order, inside the PK B-tree leaves
   PK lookup → 1 read (data is in the leaf)
   Secondary indexes contain (key → PK), so secondary lookup = 2 B-tree walks
   Random PK inserts cause page splits → fragmentation
TRADE-OFF:
  Heap:      faster inserts, slightly slower PK lookups
  Clustered: faster range scans on PK, slower secondary lookups, write amp
```

```
9.3 Row Layout & TOAST
A Postgres tuple has:
   Header (~23 bytes): xmin, xmax, ctid, t_infomask, null bitmap
   Padding for alignment
   Column values, NULL bits set in null bitmap
If a tuple would exceed ~2 KB, Postgres uses TOAST:
   Compress large values
   Move oversized columns to a separate table (TOAST table)
   Original row stores a pointer
TOAST IMPLICATIONS:
   Large TEXT/JSON columns don't bloat your main table — that's good
   BUT: SELECTing the column requires extra reads from TOAST table
   Updates to TOASTed columns can be expensive
   EXPLAIN ANALYZE may show extra time from TOAST dereferencing
```

```
9.4 Row Stores vs Column Stores
ROW STORE                          COLUMN STORE
  row1: [a1, b1, c1, d1]            col_a: [a1, a2, a3, ...]
  row2: [a2, b2, c2, d2]            col_b: [b1, b2, b3, ...]
  Good for OLTP:                     Good for OLAP:
  - whole-row reads                  - column scans (SUM, AVG)
  - point updates                    - high compression (RLE, dict)
  - random access                    - vectorized execution
                                     - terrible for whole-row UPDATE/DELETE
Examples:  Postgres, MySQL, Oracle ■ row store
           Redshift, BigQuery, Snowflake, ClickHouse, DuckDB ■ column store
           SQL Server, Oracle ■ hybrid (columnstore indexes)
■ Principal mindset: Staff-level mental model: OLTP workloads (point reads/writes, transactional) want row
stores. OLAP workloads (aggregations over large scans of few columns) want column stores. If you're bolting
columnar features onto Postgres for analytics, you've outgrown it — that's when a separate analytical store
(BigQuery, Snowflake, ClickHouse) earns its keep.
```

```
10. Indexes: B-tree, Hash, GIN, GiST, BRIN
10.1 B-tree — The Default
The B-tree (B+tree in most engines) is the default for a reason: it supports equality, range, ordering, prefix
matching, NULL handling, and is balanced under concurrent updates.
Properties:
   O(log N) lookup, insert, delete
   Range scans are cheap (leaves are linked left-to-right)
   Composite indexes: ordering matters
   Leftmost-prefix rule: index on (a, b, c) helps queries on
    (a), (a,b), (a,b,c) — NOT on (b) or (c) alone
What B-tree CAN'T help:
   Full-text search (use GIN)
   Substring / contains: WHERE x LIKE '%foo%'
   Geometric / range overlap queries (use GiST)
   Very-low-cardinality columns: optimizer prefers seq scan anyway
```

10.2 The Other Index Types (Postgres) Type Best for Operators supported B-tree Default; equality, range, sort =, <, >, BETWEEN, LIKE prefix Hash Equality-only, very large data = only GIN Multi-valued: arrays, JSONB, FTS @>, ?, &&, full-text GiST Geo, ranges, KNN, custom types <<, >>, &&, <-> SP-GiST Non-balanced trees, IP, geometry partition-based BRIN Huge tables, naturally ordered block ranges; tiny size

10.3 Composite Index Design -- Index on (a, b, c) — leftmost-prefix rule -- HELPS: WHERE a = 1 WHERE a = 1 AND b = 2 WHERE a = 1 AND b = 2 AND c = 3 WHERE a = 1 AND b BETWEEN 2 AND 5 -- range allowed on LAST equality col -- DOES NOT HELP (no a): WHERE b = 2 WHERE c = 3 -- PARTIAL HELP (range stops further usage): WHERE a = 1 AND b > 5 AND c = 7 ↑ uses index for (a,b), c filtered after DESIGN PRINCIPLES

#### 1. Most selective EQUALITY column first

#### 2. Range columns LAST

#### 3. Match the index to the query's WHERE/ORDER BY/GROUP BY shape

#### 4. Avoid redundant indexes: (a, b) makes (a) unnecessary

#### 10.4 Covering Indexes (INCLUDE)

```sql
-- An index lookup → heap fetch to get other columns
-- COVERING INDEX: stores extra non-key columns in the leaf
CREATE INDEX idx_orders_user_covering
  ON orders (user_id) INCLUDE (status, amount, created_at);
-- Now this query is "index-only":
SELECT status, amount, created_at FROM orders WHERE user_id = 42;
-- No heap fetch, much faster
-- COST: index is larger, writes slower; visibility map still must update
```

10.5 Partial & Functional Indexes -- PARTIAL INDEX: only rows matching a predicate are indexed CREATE INDEX idx_active_users ON users (email) WHERE status = 'active'; -- Smaller index, only useful for queries with the same predicate -- FUNCTIONAL / EXPRESSION INDEX CREATE INDEX idx_lower_email ON users (LOWER(email)); SELECT * FROM users WHERE LOWER(email) = 'alice@example.com'; -- uses index SELECT * FROM users WHERE email = 'alice@example.com'; -- does NOT 10.6 BRIN — Block Range Indexes BRIN stores summaries (min/max) for ranges of physical blocks. Tiny on disk (megabytes for a TB table). Useful when data is naturally sorted on disk — e.g. time-series tables where new rows go to the end. CREATE INDEX idx_events_ts ON events USING BRIN (created_at); -- Caveats: --  Effective only when physical order ≈ logical order --  If rows are reshuffled, range overlap kills effectiveness --  Slower than B-tree for selective lookups; better for bulk scans ■ Production scenario: A team added 47 indexes to a high-traffic OLTP table thinking 'more indexes = more speed'. Throughput dropped 40%. Every INSERT updated 47 B-trees, every UPDATE locked all affected indexes, autovacuum couldn't keep up. The pg_stat_user_indexes view showed 30 indexes had never been used. We dropped them. Throughput recovered. Index writes cost as much as index reads save. ■ Interview angle: Q: What's the trade-off of adding an index? Staff answer: Reads on matching queries get faster (log N instead of N). Writes get slower (every INSERT/UPDATE/DELETE updates every index). Storage grows. Lock pressure on the table during index maintenance. Statistics overhead. At small scale you don't notice; at large scale, an over-indexed OLTP table becomes write-bound. The answer to 'should we add this index' is always 'what does pg_stat_statements say about the query, and what does pg_stat_user_indexes say about existing indexes'.

#### 11. The Query Optimizer & Statistics

```
11.1 The Two Optimizer Models
RULE-BASED OPTIMIZER (RBO)
   Applies fixed rewrite rules in priority order
   No knowledge of data — assumes uniform distribution
   Modern engines no longer use this in pure form
COST-BASED OPTIMIZER (CBO) — universal today
```

#### 1. Enumerate possible plans (join orders, algorithms, access paths)

#### 2. Estimate the cost of each plan using statistics

#### 3. Pick the cheapest plan

```
11.2 What Statistics the Optimizer Uses
For each table:
   Row count (reltuples)
   Page count (relpages)
For each column:
   NDV (number of distinct values)
   Null fraction
   Most common values (MCVs) and their frequencies
   Histogram of value distribution (~100 buckets by default)
   Correlation: physical vs logical ordering
UPDATED BY:
   ANALYZE (manual)
   autovacuum (automatic, threshold-triggered)
```

11.3 Cardinality Estimation The single most important thing the optimizer does — and the most common source of bad plans. The estimator predicts how many rows each operator produces. Small errors compound through joins. Single-column estimates: usually OK (histograms + MCVs) Multi-column (a=1 AND b=2): often wrong — assumes INDEPENDENCE -- Postgres: extended statistics for correlated columns CREATE STATISTICS stats_users_city_state (dependencies, ndistinct) ON city, state FROM users; ANALYZE users; JOIN CARDINALITY for equi-join A.x = B.y: estimated_rows = (|A| * |B|) / max(ndv_A_x, ndv_B_y) Why max? It's the standard System R assumption, conservative. If the join column has skew, this is wildly wrong.

```
11.4 When the Optimizer is Wrong
SYMPTOMS:
   EXPLAIN ANALYZE: estimated rows >> actual or << actual
   Same query has wildly different times depending on parameters
   A simple query takes 10 minutes; rewriting trivially makes it instant
CAUSES:
```

#### 1. Stale stats → ANALYZE the table

#### 2. Correlated columns assumed independent → extended statistics

#### 3. Function in predicate not understood → IMMUTABLE marker or functional index

#### 4. Parameter sniffing: cached plan for non-representative parameter

#### 5. Bad join order due to large fan-out → restructure or hints

#### 6. Bind variable peeking (Oracle) → adaptive cursor sharing

11.5 Optimizer Hints  Oracle: /*+ INDEX(...) */ /*+ USE_HASH(...) */  SQL Server: OPTION (HASH JOIN), WITH (INDEX(idx))  MySQL: STRAIGHT_JOIN, USE INDEX (...)  Postgres: NONE BUILT IN (philosophy: fix the optimizer) pg_hint_plan extension provides them ■ Principal mindset: Hints are a confession that you understand the data better than the optimizer did. Use them as a last resort and document why. The better long-term fix is usually: better statistics, restructured query, additional index, or extended statistics. Postgres deliberately omits hints to push you toward root causes.

#### 12. Reading EXPLAIN / EXPLAIN ANALYZE

```
12.1 EXPLAIN vs EXPLAIN ANALYZE
EXPLAIN            — show planned tree + cost estimates (FAST, no exec)
EXPLAIN ANALYZE    — ACTUALLY RUN the query, show real timings + row counts
Always use ANALYZE for tuning. Cost numbers without actual timings can lie.
EXPLAIN (ANALYZE, BUFFERS, VERBOSE)  -- Postgres
  → shows I/O (shared/local hits, reads, dirties) — critical for cache analysis
```

```sql
12.2 Anatomy of a Postgres Plan
EXPLAIN ANALYZE
SELECT u.name, COUNT(*) FROM users u
  JOIN orders o ON o.user_id = u.id
 WHERE u.country = 'US' GROUP BY u.name;
HashAggregate  (cost=1234..1456 rows=100 width=40)
               (actual time=15.3..15.8 rows=98 loops=1)
  Group Key: u.name
  ->  Hash Join  (cost=120..1100 rows=5000 width=32)
                 (actual time=2.1..14.7 rows=4892 loops=1)
        Hash Cond: (o.user_id = u.id)
        ->  Seq Scan on orders o  (rows=100000)
        ->  Hash  (cost=80..80 rows=500 width=20)
              ->  Index Scan using users_country_idx on users u
                  Index Cond: (country = 'US')
                  (rows=503)
 Planning Time: 0.4 ms
 Execution Time: 15.9 ms
```

```
12.3 The Numbers Decoded
cost=A..B
  A = startup cost (rows can begin emitting)
  B = total cost (after returning all rows)
  Units: arbitrary; relative comparisons only
rows=N
  Estimated row count
width=W
  Estimated bytes per row
actual time=A..B rows=R loops=L
  Real timing in ms; rows is per-loop, total = rows*loops
  loops > 1 means this node ran multiple times (nested loop inner side)
```

```
12.4 Red Flags in EXPLAIN ANALYZE
■ ESTIMATED vs ACTUAL rows differ by >10x
   → stats are wrong; ANALYZE the table, consider extended statistics
■ SEQ SCAN on a large table with a selective filter
   → missing index, or index bypassed (type mismatch, function)
■ NESTED LOOP with high outer rows and no index on inner
   → optimizer guessed wrong size; force hash with rewrite or stats
■ SORT method: external merge Disk: NN KB
   → spilled to disk; raise work_mem or rewrite to avoid the sort
```

```
■ HASH BATCHES > 1
   → hash table didn't fit; raise work_mem
■ Rows Removed by Filter > rows kept
   → predicate isn't pushed down; index missing or unusable
■ BITMAP HEAP SCAN with high "lossy" pages
   → bitmap too big for memory; degraded to page-level matching
```

12.5 EXPLAIN Across Engines POSTGRES: EXPLAIN (ANALYZE, BUFFERS, FORMAT TEXT) ... pgMustard, depesz.com/explain — visualization tools MYSQL: EXPLAIN ANALYZE ... (8.0+) or EXPLAIN FORMAT=TREE SQL SERVER: SET STATISTICS IO ON, SET STATISTICS TIME ON Display Estimated Execution Plan in SSMS ORACLE: EXPLAIN PLAN FOR ...; SELECT * FROM TABLE(DBMS_XPLAN.DISPLAY); BIGQUERY: Query plan tab in console; INFORMATION_SCHEMA.JOBS SNOWFLAKE: Query profile in UI ■ Interview angle: Q: EXPLAIN ANALYZE shows 'Estimated rows: 10, Actual rows: 1,000,000'. What does it mean and what do you do? Staff answer: Optimizer underestimated by 5 orders of magnitude. It almost certainly chose a plan tuned for 10 rows (nested loop, no hashing) that is terrible for a million. Check: when was ANALYZE last run? If stats are fresh, the likely cause is correlated columns or a non-statistically-tracked predicate. Fix in order: ANALYZE, add extended statistics, rewrite to expose cardinality, or as last resort use a hint. Document it.

13. MVCC, Vacuum & Bloat

```
13.1 Multi-Version Concurrency Control
MVCC lets readers and writers proceed without blocking each other. Each transaction sees a consistent
snapshot of the database without locking what it reads. Postgres, Oracle, MySQL InnoDB, SQL Server
(RCSI), and most modern engines use it.
HOW POSTGRES DOES MVCC:
   Every row has xmin (creating txn id) and xmax (deleting/updating txn id)
   UPDATE creates a NEW row version; old version's xmax is set
   DELETE just sets xmax on the existing version
   A transaction with snapshot S sees row R iff:
      xmin(R) < S  AND  (xmax(R) is null OR xmax(R) > S OR xmax txn aborted)
CONSEQUENCES:
   Dead row versions accumulate — must be cleaned (vacuum)
   Long-running transactions hold back vacuum → bloat
   UPDATE = INSERT + mark old — write amplification
```

```
13.2 Vacuum in Postgres
VACUUM (regular)
   Marks dead tuples reusable; does NOT shrink file
   Updates visibility map → enables index-only scans
   Runs automatically (autovacuum)
VACUUM FULL
   Rewrites entire table to new file; reclaims disk
   Takes ACCESS EXCLUSIVE lock (blocks everything)
   Use only when bloat is severe
ANALYZE
   Refreshes statistics for the optimizer
AUTOVACUUM TRIGGERS:
   Dead tuples > autovacuum_vacuum_threshold + scale_factor * row_count
   Default scale_factor = 0.2 → vacuum when 20% rows dead
   For huge tables this is too lax → tune per-table
```

```
13.3 Bloat
BLOAT = dead space in a table or index that wasn't reclaimed.
CAUSES:
   High UPDATE/DELETE rate
   Long-running transactions blocking autovacuum (THE killer)
   Lock conflict preventing autovacuum from completing
   autovacuum_naptime too long
DETECTING:
   pgstattuple extension: actual vs free vs dead bytes
   pg_stat_user_tables: n_dead_tup, last_autovacuum
   Index bloat: compare expected vs actual size
FIXING (without VACUUM FULL):
   pg_repack: rebuild table/indexes without exclusive lock
   CREATE INDEX CONCURRENTLY + DROP old (index bloat)
   CLUSTER (takes exclusive lock, reorders + compacts)
■ Production scenario: An analytics service ran a 6-hour transaction every night, summarizing the day before.
autovacuum couldn't reclaim any rows whose xmax was after the analytics txn's snapshot started. Within 3 months
```

the orders table was 80% bloat (200 GB table holding 40 GB of live data). The fix: split the analytics into many short transactions so the snapshot didn't span hours. Could also have used a logical replica for analytics. ■ Principal mindset: Long-running transactions are the silent killer of MVCC databases. At staff level, you should instinctively design ETL and analytics to use SHORT transactions, REPEATABLE READ snapshots only where needed, and offload long-running reads to replicas. Monitor pg_stat_activity.xact_start and alert on any transaction older than N minutes. ■ Interview angle: Q: Explain MVCC vs locking-based concurrency. Staff answer: In MVCC, readers see a snapshot — a logical point-in-time view — without taking locks on rows they read. Writers create new versions of rows rather than overwriting in place. Reads don't block writes and vice versa, which is enormous for OLTP throughput. The cost is storage and garbage collection: dead versions accumulate, and long-running transactions prevent cleanup. Locking-based DBs take shared locks for reads, which serialize against writes — simpler internally, but contention is constant. Most modern systems use MVCC and reserve locks for write-write conflicts.

## PART IV — CONCURRENCY & CORRECTNESS

#### 14. Transactions & ACID

14.1 ACID, Properly Defined Letter Property What it really means A Atomicity All operations in a txn succeed, or none do C Consistency Txns transition the DB from valid state to valid state I Isolation Concurrent txns appear to run as if serial (varies) D Durability Once committed, the change survives crashes (WAL)

```
14.2 The Two Mechanisms
WRITE-AHEAD LOG (WAL) / REDO LOG
   Before modifying a page in memory, write the change to a sequential
    log on disk
   On crash: replay log to recover committed txns, ignore uncommitted
   Provides DURABILITY (D) and ATOMICITY (A — uncommitted = ignored)
LOCKS or MVCC
   Provide ISOLATION (I)
   Locks: pessimistic — block until safe
   MVCC: optimistic — snapshots avoid contention; only writes can conflict
```

14.3 Transactional Control BEGIN [TRANSACTION]; -- start txn SAVEPOINT sp1; -- nested rollback point ROLLBACK TO sp1; -- undo to savepoint, txn continues COMMIT; -- make changes durable ROLLBACK; -- discard all changes -- Autocommit: each statement is its own txn (default in many tools) -- BEGIN EXPLICITLY when you need multiple statements to be atomic -- READ ONLY transactions BEGIN TRANSACTION READ ONLY;

```
14.4 The Durability Spectrum
 synchronous_commit=on        — wait for WAL flush to disk before ack (default)
 synchronous_commit=off       — ack immediately, flush asynchronously
                                  (~50% faster, small data-loss window on crash)
 synchronous_commit=remote_apply — wait for replica to apply WAL (zero-RPO)
 fsync=off                    — DANGEROUS. Almost never appropriate in prod.
Different workloads pick different points. Financial → remote_apply.
Analytics ingestion → off is reasonable.
```

#### 14.5 Idempotent Operations

```sql
NOT IDEMPOTENT:
  INSERT INTO accounts (id, balance) VALUES (1, 100);     -- duplicate key on retry
  UPDATE balance = balance + 100 WHERE id = 1;            -- double-credits on retry
IDEMPOTENT VERSIONS:
  INSERT ... ON CONFLICT (id) DO NOTHING / DO UPDATE      -- UPSERT
  UPDATE balance = 100 WHERE id = 1;                       -- absolute, not relative
  Deduplication key:
    INSERT INTO transactions (txn_uuid, from_id, to_id, amount) ...
    ON CONFLICT (txn_uuid) DO NOTHING;                     -- safe to retry
■ Principal mindset: Every external-facing write API at scale must be designed for idempotency. Client retries
during network blips are guaranteed. The simplest mechanism is a client-supplied idempotency key, stored with a
unique constraint. Stripe's Idempotency-Key header is the canonical pattern.
```

#### 15. Isolation Levels & Anomalies

```
15.1 The Four ANSI Isolation Levels
Level
Dirty
Non-Rep.
Phantom
Lost Upd.
Write Skew
READ UNCOMMITTED
✓
✓
✓
✓
✓
READ COMMITTED
✗
✓
✓
✓
✓
REPEATABLE READ
✗
✗
depends
depends
✓
SERIALIZABLE
✗
✗
✗
✗
✗
```

```sql
15.2 The Anomalies, Properly
DIRTY READ
  Txn A reads uncommitted changes by Txn B. If B aborts, A saw data
  that never existed.
NON-REPEATABLE READ
  Txn A reads row R, Txn B updates and commits R, Txn A re-reads R —
  different value.
PHANTOM READ
  Txn A: SELECT count(*) WHERE x>0 returns 5.
  Txn B: INSERT a row with x>0, commits.
  Txn A: re-runs same query → 6. The new row is a "phantom".
LOST UPDATE
  Txn A reads x=10, computes x+1, writes 11.
  Txn B reads x=10, computes x+5, writes 15.
  A's +1 is lost. (Classic counter-increment race.)
WRITE SKEW
  Two txns read overlapping data, write disjoint data, both commit.
  Each is individually correct; the combination violates an invariant.
  Example: two doctors both on-call. Each checks "is at least one
  other on-call?", sees true, both go off-call. Constraint
  "≥1 on call" now violated.
  This is the anomaly that makes SNAPSHOT ISOLATION not equal SERIALIZABLE.
```

```
15.3 Snapshot Isolation (SI) vs Serializable
Many engines that say 'REPEATABLE READ' actually provide Snapshot Isolation (Postgres, Oracle, SQL
Server RCSI). SI prevents dirty/non-repeatable reads AND phantoms within a transaction's snapshot, but
allows write skew. True SERIALIZABLE requires extra machinery.
POSTGRES:
  READ COMMITTED  — default. Each statement sees a fresh snapshot.
  REPEATABLE READ — Snapshot Isolation. Txn-wide snapshot. Write skew possible.
  SERIALIZABLE   — Serializable Snapshot Isolation (SSI). Detects rw-dependency
                    cycles, aborts the offender with serialization_failure.
MYSQL InnoDB:
  REPEATABLE READ — default. SI + gap locking blocks phantoms.
  SERIALIZABLE   — adds shared lock on reads (lock-based).
SQL SERVER:
  READ COMMITTED + RCSI (snapshot) is common default.
  SNAPSHOT isolation is opt-in.
  SERIALIZABLE uses range locks.
```

```
ORACLE:
  SERIALIZABLE — actually provides Snapshot Isolation. No true serializable.
  (Yes, Oracle's "SERIALIZABLE" allows write skew.)
```

```sql
15.4 Handling Lost Updates
-- The pattern: read, compute, write
SELECT balance FROM accounts WHERE id = 1;     -- get 100
-- ... app computes new value
UPDATE accounts SET balance = 105 WHERE id = 1;
-- Concurrent txn could overwrite our work. SOLUTIONS:
1. ATOMIC UPDATE (best)
   UPDATE accounts SET balance = balance + 5 WHERE id = 1;
2. SELECT ... FOR UPDATE (pessimistic)
   BEGIN;
   SELECT balance FROM accounts WHERE id = 1 FOR UPDATE;
   UPDATE accounts SET balance = 105 WHERE id = 1;
   COMMIT;
3. OPTIMISTIC: version column / CAS
   SELECT balance, version FROM accounts WHERE id = 1;  -- v=7
   UPDATE accounts SET balance=105, version=8
     WHERE id = 1 AND version = 7;
   -- check rows-affected; if 0, retry from start
4. SERIALIZABLE
   Run the txn at SERIALIZABLE; on serialization_failure, retry.
■ Interview angle: Q: Two users transfer money from the same account at the same time. How do you prevent
overdraft?
Senior: 'Use a transaction.'
Staff: 'A transaction alone isn't enough at READ COMMITTED — both txns can read balance=100, both check >=
50, both decrement to 50. Three options: (a) atomic conditional update — UPDATE ... SET balance =
balance - 50 WHERE id = X AND balance >= 50 — fail if rows affected = 0; (b) SELECT FOR UPDATE to
serialize; (c) SERIALIZABLE isolation with retry on conflict. I'd default to (a) — it's the simplest, performs well, no
extra locks. SERIALIZABLE is heavier and needs retry-loop infrastructure.'
```

#### 16. Locking, Deadlocks & Concurrency Patterns

```
16.1 Lock Granularity
ROW LOCK         — narrow, high concurrency, more overhead per row
PAGE LOCK        — coarser; SQL Server can escalate row→page→table
TABLE LOCK       — taken implicitly for some DDL; rare for DML
PREDICATE LOCK   — covers a range of values (needed for SERIALIZABLE)
LOCK ESCALATION
  Many engines escalate when row locks become too many (memory).
  SQL Server: ~5000 row locks → escalate to table.
```

```sql
16.2 Lock Modes (Postgres)
TABLE-LEVEL LOCKS (weakest → strongest):
  ACCESS SHARE         — SELECT
  ROW SHARE            — SELECT FOR UPDATE/SHARE
  ROW EXCLUSIVE        — INSERT, UPDATE, DELETE
  SHARE UPDATE EXCL.   — VACUUM, CREATE INDEX CONCURRENTLY, ANALYZE
  SHARE                — CREATE INDEX (blocks writes)
  SHARE ROW EXCL.      — rare
  EXCLUSIVE            — REFRESH MATERIALIZED VIEW (non-CONCURRENT)
  ACCESS EXCLUSIVE     — DROP, TRUNCATE, ALTER TABLE, VACUUM FULL
ROW-LEVEL LOCKS:
  FOR UPDATE           — strongest; blocks other writers
  FOR NO KEY UPDATE    — slightly weaker; doesn't block FKs
  FOR SHARE            — allow other readers, block writers
  FOR KEY SHARE        — weakest write-blocking
```

```sql
16.3 SELECT FOR UPDATE Patterns
-- Pessimistic lock pattern
BEGIN;
SELECT * FROM jobs WHERE id = 1 FOR UPDATE;
UPDATE jobs SET status = 'done' WHERE id = 1;
COMMIT;
-- Skip locked rows (queue worker pattern) — Postgres 9.5+
SELECT * FROM jobs
 WHERE status = 'pending'
 ORDER BY created_at
 LIMIT 1
 FOR UPDATE SKIP LOCKED;
-- Multiple workers poll concurrently without blocking each other.
-- NOWAIT — fail fast instead of waiting
SELECT * FROM jobs WHERE id = 1 FOR UPDATE NOWAIT;
```

```
16.4 Deadlocks
Txn A: locks row 1, wants row 2.
Txn B: locks row 2, wants row 1.
Neither can proceed. Cycle in the lock wait graph.
Engines detect deadlocks by periodically searching for cycles. On detection:
pick a victim (cheapest to abort), roll it back, release its locks.
Postgres: ERROR: deadlock detected, SQLSTATE 40P01.
PREVENTION:
  1. CONSISTENT LOCK ORDER — always acquire locks in a fixed order
  2. SHORT TRANSACTIONS — fewer held locks → smaller cycle window
```

- 3. FOR UPDATE SKIP LOCKED for queues — workers don't deadlock
- 4. RETRY LOGIC — every transaction wrapped in retry-with-backoff
- 5. AVOID UPDATING THE SAME HOT ROW — shard the counter, async aggregate

```sql
16.5 Optimistic vs Pessimistic
Scenario
Pessimistic
Optimistic
High write contention same row
✓
✗ (lots of retries)
Low contention, many writers
✗ (overhead)
✓
Long-running validation
✗
✓ (CAS at end)
Web app: user edits a record
✗
✓ (version)
Job queues
✓ (SKIP LOCKED)
—
■ Production scenario: An app slow-ran a SELECT in a long-lived txn, then issued an UPDATE on the same table.
The SELECT held an ACCESS SHARE lock; the UPDATE took ROW EXCLUSIVE — no conflict. Meanwhile, a
deploy script ran ALTER TABLE ADD COLUMN requiring ACCESS EXCLUSIVE, which queued behind the SELECT.
Every subsequent query queued behind the ALTER. Result: total table outage until the long txn finished. Lesson:
keep transactions short; never run DDL during business hours without lock_timeout.
■ Interview angle: Q: A real-time auction gets 10K bids/sec on a hot item. How do you handle concurrent 'place
highest bid' operations?
Staff answer: Pessimistic locking on the item row serializes everything — at 10K/s with 1ms holds, you're queuing
badly. Optimistic with version column means 99% of bids retry-and-fail, wasted work. The right pattern: INSERT all
bids into a log table (no contention), maintain 'highest bid' via async process. Or atomic conditional UPDATE:
UPDATE auction SET high_bid = $new WHERE id = X AND high_bid < $new RETURNING * — losers'
rows-affected is 0, no retry. Sometimes the answer is 'change the data model'.
```

## PART V — PERFORMANCE ENGINEERING

#### 17. Query Tuning Playbook

  - 17.1 The Five-Step Process
- 1. IDENTIFY    — find the slow query (pg_stat_statements, slow log, APM)
- 2. ISOLATE     — reproduce it with EXPLAIN ANALYZE
- 3. ANALYZE     — find the expensive operator (high cost, big estimate gap)
- 4. HYPOTHESIZE — missing index? bad stats? wrong join order? bad plan?
- 5. FIX & MEASURE — apply one change at a time, re-measure

```sql
17.2 The Triage Checklist
■ Are stats fresh?                ANALYZE table_name;
■ Is the right index being used?  EXPLAIN; Index Scan vs Seq Scan
■ Is the index even usable?       Type mismatch, function call, NULL?
■ Is the query SARGable?          (Search ARGument-able)
                                  WHERE func(col) = X    ✗ bypasses index
                                  WHERE col = func(X)    ✓ uses index
■ Selectivity?                    Is the predicate <10% of rows?
                                  If not, seq scan may be CORRECT.
■ Join algorithm appropriate?     Hash for big-big, NL for big-small with index
■ Sort/agg fitting in memory?     Check work_mem usage
■ Hot row contention?             SELECT FOR UPDATE delays?
■ Parameter sniffing?             Same query, different params, different times?
```

  - 17.3 The Top 10 Rewrites
- 1. SELECT *   →  SELECT only needed columns
- 2. WHERE func(col) = X  →  rewrite to expose col, or add functional index
- 3. OR across columns  →  UNION ALL of indexed queries
- WHERE a=1 OR b=1  →  SELECT WHERE a=1 UNION ALL SELECT WHERE b=1 AND a!=1
- 4. NOT IN with nullable subquery  →  NOT EXISTS

- 5. Correlated scalar subquery  →  LEFT JOIN + aggregate
- 6. DISTINCT on join result  →  EXISTS instead
- 7. LIKE '%foo%'  →  full-text index (GIN/tsvector) or trigram (pg_trgm)
- 8. ORDER BY ... LIMIT N on large set  →  index with same ordering
- 9. COUNT(*) for pagination  →  cursor / keyset pagination
- 10. UPDATE with JOIN affecting many rows  →  batch in chunks of N rows

```
17.4 SARGability
-- NOT SARGable: function on the COLUMN side
WHERE UPPER(name) = 'ALICE'              ✗ unless functional index
WHERE date_trunc('month', t) = '2024-01' ✗
WHERE EXTRACT(year FROM t) = 2024        ✗
-- SARGable rewrites:
WHERE name = 'Alice' COLLATE "C"
WHERE name LIKE 'A%'                     ✓ uses index with text_pattern_ops
WHERE t >= '2024
01' AND t < '2024
01'   ✓
WHERE t >= '2024
01' AND t < '2025
01'   ✓
```

#### 17.5 Keyset Pagination (vs OFFSET)

```sql
SELECT * FROM posts ORDER BY id LIMIT 20 OFFSET 100000;
-- scans 100020 rows
-- Keyset: use last-seen value
SELECT * FROM posts WHERE id > $last_id ORDER BY id LIMIT 20;
-- O(log N) lookup, O(1) per row after
-- For non-unique sort keys, use composite:
SELECT * FROM posts
 WHERE (created_at, id) < ($last_created_at, $last_id)
 ORDER BY created_at DESC, id DESC
 LIMIT 20;
■ Production scenario: A pagination endpoint used OFFSET. As datasets grew past a million rows, p99 latency on
page 1000 hit 8 seconds. The fix wasn't a faster server or better index — it was switching to keyset. P99 dropped to
12 ms. OFFSET is the most common 'works in dev, dies in prod' anti-pattern at scale.
■ Interview angle: Q: A query that took 50ms last week takes 5 seconds today, same parameters. Debug it.
Staff answer: Compare today's plan to a baseline. (1) EXPLAIN ANALYZE now; (2) check stats freshness
(last_analyze); (3) table size growth — did seq scan become viable due to fewer pages, or intolerable due to more?
(4) parameter sniffing — is the cached plan from a non-representative param? (5) lock waits? Most often it's stale
stats or parameter sniffing.
```

#### 18. Index Design Patterns

  - 18.1 The Design Loop
- 1. CAPTURE the workload — pg_stat_statements gives top N queries
- 2. CATEGORIZE — equality? range? join? sort? aggregation?
- 3. DESIGN minimal indexes that cover the workload
- 4. VERIFY with EXPLAIN ANALYZE that they're used
- 5. MEASURE write overhead — pg_stat_user_indexes, write throughput
- 6. DROP unused indexes (idx_scan = 0 for weeks)

```
18.2 Heuristics
 Every PK has an index (auto)
 Every FK should have an index on referring column (NOT auto in most engines!)
  Without it: DELETE on parent → seq scan of child for FK check → terrible
 Heavily filtered columns get indexes (selectivity < 10%)
 Sort columns with LIMIT get indexes in matching order
 Don't index very-low-cardinality columns alone (sex, boolean)
  → instead, prefix a higher-cardinality column
COMPOSITE ORDERING:
```

#### 1. Equality columns (most selective first)

#### 2. Range column (always last)

#### 3. Sort column (if covered by range)

```
18.3 Index Maintenance
 REINDEX [CONCURRENTLY] — rebuild bloated indexes
 CREATE INDEX CONCURRENTLY — doesn't block writes; can fail (INVALID index)
 DROP INDEX CONCURRENTLY — doesn't block writes
 Monitor pg_stat_user_indexes:
   - idx_scan high, idx_tup_fetch low → covering / index-only working
   - idx_scan zero for weeks → drop
   - idx_tup_read >> idx_tup_fetch → many filter-after-fetch → reconsider
```

#### 18.4 Common Index Mistakes

#### 1. Indexing every column "just in case"  → write overhead, bloat

#### 2. Forgetting FK indexes                  → cascade ops catastrophic

#### 3. Single-column indexes when composite was needed

#### 4. Index order wrong for the query        → optimizer can't use it

#### 5. Indexing low-cardinality alone (boolean) → useless; partial index better

#### 6. Not using partial indexes for hot subsets

```
7. Letting indexes bloat                  → REINDEX CONCURRENTLY periodically
■ Principal mindset: Index design at scale is inventory management, not per-query tuning. You should be able to
enumerate every index, name the query that justifies it, and show that query is in the workload's top N. Indexes
without owners accumulate. Quarterly index review is a staff-level habit.
```

#### 19. Partitioning Strategies

  - 19.1 What Partitioning Buys
- 1. PARTITION PRUNING — query only touches relevant partitions
- 2. MAINTENANCE — drop/truncate old partitions instantly
- 3. SMALLER INDEXES per partition → faster lookups, less bloat
- 4. PARALLEL OPERATIONS — vacuum, analyze, query per partition
- 5. TIERED STORAGE — old partitions on slow disks
- NOT FREE:
-  Constraints (UNIQUE, PK) become partition-local unless they include key
-  Cross-partition joins can be more expensive
-  Many small partitions hurt: optimizer overhead, file handles

```
19.2 Partitioning Schemes
RANGE        — by value range (most common: by date)
  e.g. orders_2024_q1, orders_2024_q2
LIST         — by enum of values
  e.g. partition by country = {'US'}, {'EU'}, {'APAC'}, DEFAULT
HASH         — by hash of key
  e.g. 16 partitions for even distribution; no pruning unless query on key
COMPOSITE / SUBPARTITIONING
  RANGE on date, then HASH on user_id within each date partition
```

```sql
19.3 Postgres Declarative Partitioning
CREATE TABLE events (
    id BIGSERIAL,
    user_id BIGINT,
    event_at TIMESTAMPTZ,
    payload JSONB,
    PRIMARY KEY (id, event_at)         -- PK must include partition key
) PARTITION BY RANGE (event_at);
CREATE TABLE events_2024_q1 PARTITION OF events
  FOR VALUES FROM ('2024
01') TO ('2024
01');
CREATE INDEX ON events (user_id, event_at);
-- Pruning works if WHERE includes partition key:
SELECT * FROM events WHERE event_at >= '2024
01' AND event_at < '2024
01';
-- → scans only events_2024_q1
-- Drop old (instant)
DROP TABLE events_2022_q1;
```

19.4 When NOT to Partition  Table is <50GB and queries are fast — added complexity not worth it  You can't put partition key in queries — no pruning  Heavy cross-partition joins/aggregations — overhead may exceed benefit  You need GLOBAL uniqueness on non-partition-key columns ■ Production scenario: A team partitioned a 200GB events table by tenant_id (hash, 64 partitions). Tenant-scoped queries got 2x faster. But analytics queries (cross-tenant aggregations) slowed 10x because they now hit 64 partitions and 64 indexes. Right call: RANGE on event_at for the time-series workload that dominated, with secondary index on tenant_id. They re-partitioned six months later.

> **🎯 Interview Angle:** Q: When would you partition a table, and what's the partition key? Staff: Partition when (a) table is large enough that operational tasks — vacuum, index rebuild, backup — become painful, typically >100GB; (b) natural drop-old-data lifecycle; or (c) >90% of queries can be pruned by a single column. The partition key should be the one 90%+ of queries filter on — usually time. Tenant-id is tempting but rarely right unless workload is truly per-tenant and balanced. Never partition just because the table is big.

#### 20. Anti-Patterns at Scale

```sql
20.1 The Catalog
SCHEMA ANTI-PATTERNS
  ■ EAV (Entity-Attribute-Value) tables — every query is N joins
  ■ Storing CSV/JSON arrays in TEXT columns
  ■ Boolean status field that should be a state machine
  ■ Soft-delete column without partial indexes
  ■ UUIDv4 as primary key on a clustered index (hot pages, write amp)
     → use UUIDv7, ULID, or SERIAL where possible
  ■ Wide tables with 100+ columns and 90% NULLs
QUERY ANTI-PATTERNS
  ■ SELECT * everywhere
  ■ N+1 query pattern from ORMs
  ■ Pagination via OFFSET on large sets
  ■ Computing aggregates in app code after fetching rows
  ■ COUNT(*) for pagination's "total pages"
  ■ Date functions in WHERE clauses (non-SARGable)
  ■ LIKE '%foo%' on large text columns without trigram/FTS
OPERATIONAL ANTI-PATTERNS
  ■ Long-running transactions (hold back vacuum, MVCC bloat)
  ■ DDL during business hours without lock_timeout
  ■ Indexes added "in case" without measurement
  ■ Backups that are never restored (untested = no backup)
  ■ ALTER TABLE ADD COLUMN with default (rewrites in older PG)
  ■ Killing autovacuum because it's "slow"
DISTRIBUTED ANTI-PATTERNS
  ■ 2PC across services (slow, fragile; prefer Sagas)
  ■ Distributed transactions across heterogeneous DBs
  ■ Reading from replica without considering lag
  ■ Sharding without shard key in every query
  ■ Cross-shard joins as a hot path
■ Production scenario: An EAV schema with ~80M attribute rows had a single 'show user' page issuing 47 queries
via the ORM (N+1 across attribute types). p99 page load: 4 seconds. Rewriting to a single JOIN + pivot via FILTER
aggregations dropped it to 35ms. Six months later, they moved the user object to a proper relational shape with a
JSONB column for the truly-variable fields. p99 dropped to 8ms. EAV is a code smell; use only when attributes are
truly unbounded and rarely queried.
■ Interview angle: Q: What's wrong with UUIDv4 as a primary key on a clustered index?
Staff: Random UUIDs scatter inserts across the entire B-tree. Each INSERT touches a different leaf page, evicting
hot pages from cache and causing page splits. Write throughput collapses and the index bloats faster. Solutions: (a)
sequential ID for clustered key, UUID as non-clustered unique index; (b) UUIDv7 / ULID — time-prefixed, roughly
sequential; (c) some engines support COMB GUIDs. This is a case where 'random keys are great for distribution'
(true in hash partitioning) conflicts with 'sequential inserts are great for B-trees'.
```

## PART VI — DATA MODELING

#### 21. Normalization vs Denormalization

```
21.1 The Normal Forms
1NF — Atomic values; no repeating groups
2NF — 1NF + every non-key column depends on the WHOLE primary key
       (only meaningful for composite PKs)
3NF — 2NF + no transitive dependencies (non-key → non-key)
BCNF — 3NF + every determinant is a candidate key
4NF — Eliminate multi-valued dependencies
5NF — Eliminate join dependencies
THE RULE OF THUMB:
  "The key, the whole key, and nothing but the key, so help me Codd."
In practice:
  OLTP   → 3NF or BCNF
  OLAP   → denormalized (star schema)
  Hybrid → 3NF with strategic denormalization
```

```
21.2 What Normalization Buys You
✓ No update anomalies (change a value in one place)
✓ No insert anomalies (no need to know X to record Y)
✓ No delete anomalies (deleting doesn't lose unrelated facts)
✓ Smaller tables, less storage, less redundancy
✓ Constraints enforce integrity
COSTS:
  ✗ More joins (harder to read, can be slower)
  ✗ Complex queries spanning many tables
  ✗ Reporting workloads suffer
```

```
21.3 When to Denormalize
STRATEGIC DENORMALIZATION:
   Read-heavy paths where joins are bottlenecks
   Reporting / analytics
   Aggregates frequently queried (precompute & store)
   Derived columns kept consistent via triggers / CDC
PATTERNS:
```

#### 1. Computed columns: total_amount on orders (instead of SUM(items))

#### 2. Cached counts: post.comment_count via trigger or async

#### 3. Audit history: store snapshots, not just current

#### 4. Wide row for ML features

```
  5. Star schema for analytics
THE TRADE-OFF:
  Denormalization moves work from READ time to WRITE time.
  Worth it iff reads >> writes.
■ Principal mindset: Normalization is the default; denormalization is a chosen exception. At staff level you defend
every denormalization with a measurement (these queries dropped from X ms to Y ms) and accept the maintenance
burden (triggers, app logic, or batch reconciliation). 'It was simpler this way' is not an answer.
```

#### 22. Dimensional Modeling & SCDs

```
22.1 Star Schema
        ■■■■■■■■■■■         ■■■■■■■■■■■■
        ■ Date    ■         ■ Product  ■
        ■  dim    ■         ■   dim    ■
        ■■■■■■■■■■■         ■■■■■■■■■■■■
             ■■■■■         ■■■■■■■
                 ▼         ▼
              ■■■■■■■■■■■■■■■■■■■
              ■   Sales Fact    ■
              ■  date_key       ■
              ■  product_key    ■
              ■  store_key      ■
              ■  customer_key   ■
              ■  quantity       ■
              ■  amount         ■
              ■■■■■■■■■▲■■■■■■■■■
              ■■■■■■■■■■■■■■■■■■■
        ■■■■■■■■■■■■■■    ■■■■■■■■■■■■
        ■   Store    ■    ■ Customer ■
        ■    dim     ■    ■   dim    ■
        ■■■■■■■■■■■■■■    ■■■■■■■■■■■■
FACT TABLE: measurements (sales, events, transactions)
   Foreign keys to all dimensions
   Numeric measures (additive: sum-able)
   Tall and narrow
DIMENSION TABLES: descriptive context
   Surrogate key (synthetic) + natural key
   Wide; many descriptive attributes
```

22.2 Grain The grain of a fact table is the meaning of one row: one sale, one line item, one daily snapshot. Choose grain first, then dimensions. Mixed-grain fact tables are an anti-pattern.

```
22.3 Fact Table Types
TRANSACTION FACT
  One row per event (sale, click, login). Most common.
PERIODIC SNAPSHOT
  One row per entity per time period (inventory at end-of-day).
ACCUMULATING SNAPSHOT
  One row per process instance; updated as it progresses.
  E.g., one order with columns for placed_at, shipped_at, delivered_at.
```

22.4 Slowly Changing Dimensions (SCDs) Type Name Behavior 0 Retain original Never update — historical fact 1 Overwrite Replace old, lose history 2 Add new row New version, mark old inactive (timestamps) 3 Add new column Keep "current" and "previous" columns 4 History table Current dim + separate history table

```sql
6
Hybrid (1+2+3)
Combo: current value + version history
22.5 SCD Type 2 — The Workhorse
CREATE TABLE dim_customer (
    customer_sk    BIGSERIAL PRIMARY KEY,    -- surrogate key
    customer_id    BIGINT,                   -- natural / business key
    name           TEXT,
    address        TEXT,
    valid_from     TIMESTAMPTZ NOT NULL,
    valid_to       TIMESTAMPTZ,              -- NULL = current
    is_current     BOOLEAN,
    UNIQUE (customer_id, valid_from)
);
-- Fact table joins on the SK that was current at fact time:
SELECT f.*, d.name
  FROM fact_sales f
  JOIN dim_customer d
    ON d.customer_id = f.customer_id
   AND f.sale_at >= d.valid_from
   AND (d.valid_to IS NULL OR f.sale_at < d.valid_to);
-- Or precompute customer_sk at ETL time for stable joins.
■ Interview angle: Q: A customer changes address. Sales team wants reports to show old sales at old address,
new at new. What dimension type, how do you join?
Staff: SCD Type 2. Surrogate key per version, natural key common to all versions, valid_from/valid_to columns.
Fact table either stores the customer_sk current at sale time (locking history at fact creation) or joins on (customer_id
+ sale_at BETWEEN valid_from AND valid_to) at query time. First is faster, second handles late-arriving dim
changes. Choose based on whether dim updates can be retroactive.
```

23. Modeling for OLTP vs OLAP Aspect OLTP OLAP Workload Many small txns; reads+writes Few large queries; mostly reads Latency Milliseconds Seconds to minutes Concurrency Thousands of users 10s of analysts; internal jobs Data volume GBs–low TBs TBs–PBs Schema Normalized (3NF) Denormalized (star) Storage Row store Column store Index strategy Many narrow indexes Few, cluster + sort keys Update pattern In-place; small txns Append + occasional MERGE Engines Postgres, MySQL, SQL Server BigQuery, Snowflake, Redshift

23.1 The Wrong-Engine Trap The most common scaling failure: running OLAP workloads on an OLTP database. A reporting query scanning 100M rows kills transactional latency. The fix is not bigger hardware — it's workload separation: read replicas, logical replication into a column store, or CDC into a lakehouse. 23.2 HTAP — The Hybrid HTAP (Hybrid Transactional/Analytical Processing) databases try both:  TiDB, CockroachDB, Singlestore  Postgres + Citus / Hydra extensions  SQL Server columnstore indexes  Oracle In-Memory Column Store REALITY CHECK:  Real HTAP works for moderate scale  Above that, separation wins — different storage formats, different paths  Most "HTAP" still requires column stores for the analytical side ■ Interview angle: Q: Your transactional Postgres is at 70% CPU and reports take 30 seconds. Architect a fix. Staff: Move reporting off the primary. Read replica with logical replication is quickest stopgap. Medium term: CDC (Debezium) into a columnar warehouse. Long term: model the analytics target as a dimensional star schema with daily/hourly refreshes; OLTP only serves operational queries. Don't try to make the OLTP cluster do analytics with materialized views unless the workload is small — that path leads to bloat and lock contention.

## PART VII — DISTRIBUTED & SCALE

#### 24. Replication: Sync, Async, Logical, Physical

```
24.1 The Replication Matrix
              ■  PHYSICAL (byte-level WAL)     LOGICAL (row-level changes)
■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■
ASYNCHRONOUS  ■  Default Postgres streaming   Logical replication (PG 10+)
              ■   Same major version           Cross-version, cross-OS
              ■   Whole cluster                Selective tables/cols
              ■   Fastest                      Slight CPU + latency overhead
■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■
SYNCHRONOUS   ■  synchronous_commit=remote_apply
              ■   Commit waits for replica
              ■   Zero RPO; higher latency
              ■   Quorum config possible
```

```
24.2 Replication Modes
ASYNC
  Primary commits, then ships WAL to replica.
  REPLICA LAG possible (seconds to minutes under load).
  RPO > 0: if primary dies before WAL shipped, last txns lost.
SYNC (commit + fsync at replica)
  Primary commits, fsync local + wait for replica's fsync.
  RPO = 0 to that replica.
  Latency = remote round-trip + replica fsync.
QUORUM (Postgres 10+: synchronous_standby_names with ANY)
  Wait for ANY N of M replicas.
  Balances durability and tail latency.
```

```
24.3 Logical Replication Use Cases
 Cross-version upgrades (PG 13 → 16 without downtime)
 Selective replication (only certain tables)
 Multi-master via conflict resolution (pglogical, BDR)
 CDC pipelines into Kafka / data warehouses
 Cross-database replication
LIMITATIONS
   DDL not replicated (ALTER TABLE on primary doesn't show on replica)
   Sequences not replicated by default
   Large transactions buffer in memory until commit
■ Production scenario: A team enabled synchronous replication across two data centers (200ms RTT). Every
commit took 200ms+. Throughput dropped from 5K TPS to 25. They didn't realize sync replication waits for replica
fsync on every commit. Fix: synchronous_commit = remote_write (faster, slightly weaker durability), and moved the
sync replica to the same DC. Cross-DC went async with a second pair of replicas.
```

#### 25. Sharding & Read Replicas

```
25.1 Sharding Strategies
1. HASH SHARDING
   shard = hash(key) % N
    Even distribution
    No range scans across shards
    Rebalancing on N change = data movement
    Consistent hashing minimizes movement
2. RANGE SHARDING
   shard = lookup(key) → range table
    Range scans local to one shard
    Hot ranges → hotspots
    Splitting hot ranges = manual or automatic
3. DIRECTORY-BASED
   lookup table: key → shard
    Most flexible; lookup is bottleneck
4. GEOGRAPHY-BASED
    Comply with data residency laws
    Latency optimization
```

```
25.2 The Shard Key Choice
The single most consequential decision in a sharded system. Once chosen, queries that don't filter on it
become expensive (cross-shard scatter-gather).
CRITERIA:
  ✓ High cardinality (many distinct values → even distribution)
  ✓ Even access pattern (no shard becomes a hot one)
  ✓ Present in 90%+ of queries
  ✓ Stable (doesn't change → no data migration)
COMMON CHOICES:
   user_id          — natural for multi-tenant SaaS
   tenant_id        — coarse-grained; risk of skew (big tenant)
   hash(id)         — uniform but no range queries
   date + tenant_id — composite for time-series-per-tenant
```

```
25.3 Cross-Shard Operations
1. SCATTER-GATHER QUERY
   Coordinator sends to all shards, aggregates results.
   Latency = MAX(shard latencies).
2. CROSS-SHARD JOIN
   Either replicate small table to every shard (good for dim tables),
   or co-locate by shard key (orders + items, both sharded by user_id).
3. CROSS-SHARD TRANSACTIONS
   Almost always avoided. 2PC works but bad latency + fragility.
   Better: saga / outbox pattern (Ch. 26).
4. GLOBAL UNIQUE CONSTRAINTS
   Difficult. Options:
    PK includes shard_id (compound)
    Centralized ID generator (Snowflake IDs)
    UUIDs (skip the problem)
```

#### 25.4 Read Replicas

```
WHY:
  ✓ Scale reads (most workloads are 80%+ reads)
  ✓ Isolate analytics from primary
  ✓ HA / failover candidates
  ✓ Geographic distribution
NOT FOR:
  ✗ Scaling writes (writes still go to primary)
  ✗ Read-your-own-writes (without care)
READ-YOUR-OWN-WRITES PROBLEM:
  User updates profile → write goes to primary.
  User refreshes → read may hit replica with old data.
SOLUTIONS:
  1. STICKY SESSION — route user's reads to primary for N seconds after write
  2. CAUSAL CONSISTENCY VIA LSN — capture write LSN; reads wait for it
  3. READ FROM PRIMARY for user-owned data
  4. APP-LEVEL CACHE of recent writes
■ Principal mindset: Shard late, shard wisely. Most teams shard prematurely. When you do, pick a key that fits
95% of queries, and over-provision shard count. A 256-shard system that's 10% full is much easier to operate than a
32-shard system at 90%.
```

#### 26. Distributed Transactions: 2PC & Saga

```
26.1 Two-Phase Commit (2PC)
PHASE 1: PREPARE
  Coordinator → all participants: "prepare to commit"
  Each participant: write WAL, lock resources, reply YES/NO
PHASE 2: COMMIT or ABORT
  If all YES: coordinator → all: COMMIT
  If any NO:  coordinator → all: ABORT
PROBLEMS
   BLOCKING: if coordinator crashes after PREPARE before PHASE 2,
    participants hold locks indefinitely
   Latency: 2 network round-trips + 2 disk syncs per participant
   Doesn't tolerate participant failures well
REAL USE
   Within a single DB across shards (XA in MySQL, Postgres 2PC)
   Cross-DB distributed txns — rare and fragile
```

26.2 The Saga Pattern Long-running business workflow as a sequence of LOCAL transactions, each with a COMPENSATING action. Example: Order checkout

#### 1. Reserve inventory  →  on failure: cancel reservation

#### 2. Charge payment     →  on failure: refund

#### 3. Create shipment    →  on failure: cancel + above

```
  4. Send confirmation  →  on failure: log and retry
VARIANTS:
  CHOREOGRAPHY — each service publishes events; others react
  ORCHESTRATION — central coordinator drives the workflow
PROS:
  ✓ No distributed lock; each step is a normal local txn
  ✓ Failure modes are explicit (compensations)
  ✓ Scales to many services
CONS:
  ✗ No atomic rollback — eventual consistency
  ✗ Compensations may not perfectly undo (refund != un-charge)
  ✗ Complex to reason about; observability critical
```

```
26.3 Outbox Pattern
Goal: emit messages reliably WITHIN a DB transaction.
The naive (wrong) way:
  BEGIN
    INSERT INTO orders (...);
    publish_to_kafka(order_created_event);   -- not transactional with DB
  COMMIT
  -- If commit fails after publish, you sent event for non-existent order.
The outbox way:
  BEGIN
    INSERT INTO orders (...);
    INSERT INTO outbox (event_type, payload, status)
    VALUES ('order_created', ..., 'pending');
  COMMIT
```

-- A separate process polls outbox or uses CDC to publish to Kafka. GUARANTEES: At-least-once delivery (consumer must be idempotent). Event order matches commit order if outbox has monotonic IDs. ■ Principal mindset: Sagas + outbox are the staff-engineer's answer to 'how do I do distributed transactions'. 2PC works for tightly-coupled single-vendor environments (XA inside a DB cluster) but does not survive at the boundary between independently-operated services. Embrace eventual consistency; design compensations explicitly; make every consumer idempotent.

```
27. CAP, PACELC & The Consistency Spectrum
27.1 CAP — The Sound Bite
The CAP theorem (Brewer 2000, proved by Gilbert & Lynch 2002): in the presence of a network Partition, a
distributed system can provide either Consistency (linearizability) or Availability — not both.
IMPORTANT CLARIFICATIONS:
   CAP is about NETWORK PARTITIONS, not just any failure
   "Consistency" in CAP = linearizability (strongest), not ACID consistency
   You don't "pick 2 of 3" — partitions happen, you choose CP or AP
   Non-partitioned operation gives you both — CAP says nothing about it
CP SYSTEMS: refuse to serve during partition (lose A)
   CockroachDB, Spanner, etcd, ZooKeeper
AP SYSTEMS: keep serving, accept inconsistency
   DynamoDB, Cassandra (QUORUM tunable), Riak
27.2 PACELC — More Honest
IF (Partition):   choose Availability OR Consistency
ELSE (no partition): choose Latency OR Consistency
Most systems characterized as:
  PA/EL — Cassandra: A during partition; low latency normally
  PC/EC — Spanner: C during partition (unavailable); C normally
  PA/EC — most async-replicated SQL
```

```
27.3 The Consistency Spectrum
STRONGEST → WEAKEST:
LINEARIZABILITY (CAP's "C")
  Operations appear instantaneous between invocation and completion.
SEQUENTIAL CONSISTENCY
  All processes see operations in same order, not real-time order.
CAUSAL CONSISTENCY
  Causally related operations seen in order; unrelated can reorder.
READ-YOUR-OWN-WRITES
  After you write X, you read X. Others might still see old value.
MONOTONIC READS
  Once you've seen X=v1, you won't later see X=v0.
EVENTUAL CONSISTENCY
  If no new writes, eventually all replicas converge.
  No guarantee on when, or intermediate states.
```

```
27.4 Picking a Consistency Level
PER-WORKLOAD DECISION:
ACCOUNT BALANCE / PAYMENTS    → linearizable
SHOPPING CART                  → eventual ("Add to cart" appearing 200ms later is fine)
USER PROFILE READS             → read-your-own-writes
ANALYTICS DASHBOARDS           → eventual
CONFIGURATION / FEATURE FLAGS  → eventual with bounded staleness
LEADERBOARDS                   → eventual; periodic recompute
```

> **🎯 Interview Angle:** Q: Explain CAP, then critique it. Staff: CAP says in a partition you choose C or A — Linearizability vs Availability. The common misreading is 'pick 2 of 3' which doesn't make sense — partitions aren't a choice. The bigger critique is PACELC: even without partitions, you trade Consistency for Latency. Most decisions in real distributed systems are about the 'else' branch — how much sync coordination are you willing to pay for? Spanner says 'a lot, with TrueTime'. DynamoDB says 'almost none'. I think in per-operation consistency requirements, not system-wide labels.

## PART VIII — INTERVIEW PRACTICE

#### 28. The Staff-Level Interview Loop

  - 28.1 What They're Testing
- STAFF / PRINCIPAL INTERVIEWS ARE NOT ABOUT SYNTAX.
- They are about:
- 1. DEPTH        — internals (indexes, optimizer, MVCC, replication)
- 2. TRADE-OFFS   — every choice has costs; can you articulate?
- 3. SCALING      — how does this break at 1B rows / 10K QPS / 5 regions?
- 4. DEBUGGING    — your process when something is wrong in prod?
- 5. JUDGMENT     — when to apply which tool; when to NOT optimize
- 6. COMMUNICATION — explain to a non-DB engineer?

```
28.2 Common Round Types
 QUERY WRITING (45m)
   Top
per-group, gaps-and-islands, retention cohorts.
   Show you know window functions cold.
 DEBUGGING (45m)
   "This query is slow." → EXPLAIN, ANALYZE, identify, fix.
   PROCESS > answer.
 SCHEMA DESIGN (60m)
   "Design a database for X." Multi-tenant SaaS, analytics, marketplace.
   Discuss normalization, indexing, partitioning, sharding,
   read/write patterns, evolution.
 SYSTEM DESIGN (60m+)
   "Design a global ad-serving DB" or "real-time fraud detection".
   Storage, replication, consistency, throughput, cost.
 DEEP DIVE (45m)
   "Explain how Postgres VACUUM works." Internals.
```

```
28.3 The Schema Design Framework
1. CLARIFY REQUIREMENTS
    Read/write ratio
    Volume (rows/day, total size)
    Latency SLO
    Consistency requirements
    Geographic distribution
    Audit / compliance
2. SKETCH ENTITIES & RELATIONSHIPS
    Tables, PKs, FKs
    Multi-tenant: tenant_id everywhere?
    Time-series: time as partition key?
3. ACCESS PATTERNS → INDEXES
    For each top query: what index makes it fast?
    Composite indexes for compound predicates
    Covering indexes for hot reads
```

```
4. SCALE BREAKERS & MITIGATIONS
    Hot rows → sharding, async aggregation
    Hot keys → consistent hashing
    Long-running txns → replicas, snapshot offload
    Cross-region → replication topology, lag tolerance
5. EVOLUTION
    Schema migrations (zero-downtime patterns)
    Backfills (chunked, online)
    Versioning (event sourcing? snapshot?)
```

```
28.4 Demonstrating Staff-Level Thinking
SIGNALS THEY WANT TO SEE:
  ✓ Asks clarifying questions before designing
  ✓ States assumptions explicitly
  ✓ Proposes simpler solution first, complexity only as needed
  ✓ Names trade-offs unprompted: "this gives X but costs Y"
  ✓ Discusses failure modes, not just happy path
  ✓ Knows when to push back: "we should question this requirement"
  ✓ References concrete tools/products with current understanding
  ✓ Differentiates 'theoretically possible' from 'operationally feasible'
  ✓ Talks about the team, deployment, on-call — not just the schema
RED FLAGS:
  ✗ Jumping to a design without requirements
  ✗ "Microservices everything" / "NoSQL everything"
  ✗ Can't explain internals when asked
  ✗ No mention of monitoring, backups, schema evolution
  ✗ One-size-fits-all answers
```

#### 29. War Stories & Common Pitfalls

```
29.1 The Greatest Hits
■ Production scenario: The Phantom Index. A B-tree on a NULLABLE timestamp column with the predicate
WHERE created_at IS NULL. Postgres B-tree doesn't index NULLs by default. Solution: CREATE INDEX ...
WHERE created_at IS NULL (partial index) or include NULL via (created_at) NULLS FIRST — fix at index
design time.
■ Production scenario: The Migration That Locked Everything. ALTER TABLE ADD COLUMN foo TEXT
DEFAULT 'bar' NOT NULL on PG < 11 rewrote the entire 500GB table while holding ACCESS EXCLUSIVE.
Outage. Fix: PG 11+ does this without rewrite if default is constant. Workaround for older: add nullable column →
backfill in batches → set NOT NULL CHECK in VALIDATE step.
■ Production scenario: The Connection Pool Exhaustion. An app opened a transaction, made an HTTP call
inside it (3s p99), then committed. Under load, all 100 connections in the pool were idle-in-transaction. Database had
spare CPU; app threw 'connection refused'. Lesson: never hold a DB transaction across a network I/O.
Restructure: do the I/O outside the txn, retry on conflict.
■ Production scenario: The Index That Wasn't Used. Query: WHERE user_id = '42' on a BIGINT column
with index. Seq scan every time. The application sent user_id as a string; the implicit cast (TEXT → BIGINT on the
column side) disabled the index. Either fix the app to send the right type, or the type mismatch lives in your hot path
forever.
■ Production scenario: The Backup That Wasn't. Nightly pg_dump ran for hours, completed successfully, file
uploaded to S3. When ransomware hit, restore failed: pg_dump had silently been missing 30% of tables due to a
permission issue. Backup tests never restored. Lesson: untested backups are not backups. Restore to a staging
DB monthly and run a checksum query.
```

```
29.2 The Operational Discipline
WEEKLY:
  ■ Review pg_stat_statements top 20 queries
  ■ Check pg_stat_user_indexes for unused indexes
  ■ Replication lag SLO check
  ■ Backup restore test (rotate which DB you restore)
DAILY:
  ■ Long-running transactions alert (>5 min = page)
  ■ Lock-wait alert (>30s = warning)
  ■ Disk space trend
PER RELEASE:
  ■ Schema migration review for ACCESS EXCLUSIVE locks
  ■ Estimated table rewrites
  ■ Lock timeouts in migrations
  ■ Index changes reviewed by DBA / SME
PER QUARTER:
  ■ Vacuum / bloat analysis
  ■ Capacity plan refresh
  ■ Schema design review for new features
```

#### 30. Cheat Sheet & Mental Models

```
30.1 The Five Mental Models You Need
1. LOGICAL vs PHYSICAL execution
   What the query says vs how the engine runs it.
2. ACCESS METHOD HIERARCHY (cheap → expensive)
   Index Only Scan → Index Scan → Bitmap Heap Scan → Seq Scan
   The optimizer picks the lowest if stats permit.
3. MVCC mental model
   Every row has versions. Every txn has a snapshot.
   Dead versions accumulate; vacuum cleans them; long txns block vacuum.
4. WORKLOAD SEPARATION
   OLTP and OLAP want different storage formats.
   Replicate / CDC at the seam.
5. CONSISTENCY SPECTRUM per-operation
   Different reads need different consistency.
   Most "eventually consistent" complaints are unmet read-your-own-writes.
```

```sql
30.2 The One-Page Reference
WRITE A QUERY
  SELECT only what you need (no SELECT *)
  JOIN on indexed columns, type-matched
  WHERE keep predicates SARGable
  GROUP BY use FILTER for conditional aggregates
  ORDER BY + LIMIT — index that order
  WINDOW one OVER per partition spec → one sort
DESIGN A SCHEMA
  3NF baseline; denormalize with measurement
  PK + indexed FKs everywhere
  NOT NULL with defaults whenever possible
  Composite indexes for common predicate combos
  Partial indexes for hot subsets
  Functional indexes for case-insensitive search
DESIGN AN INDEX
  Equality columns first, range last
  Sort columns must match query ORDER
  Selectivity > 10% probably not worth indexing
  Don't index NULLs alone; use partial
DEBUG SLOW QUERY
  EXPLAIN (ANALYZE, BUFFERS) first
  Compare estimated vs actual rows (>10x = stats issue)
  Look for Seq Scan, Sort spill, large Bitmap Heap lossy
  Check pg_stat_statements, last_analyze
OPERATE A DB
  Short transactions, retry loops, lock_timeout
  No DDL during business hours without timeouts
  Monitor: replication lag, long txns, bloat, dead tuples
  Test backups, monthly
```

30.3 What Separates Senior from Staff SENIOR STAFF/PRINCIPAL ■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■ "Add an index" "Add this index, drop these

```
                                       three unused; here's the
                                       trade-off in write throughput"
"Use a transaction"                   "READ COMMITTED is fine here
                                       because we use atomic
                                       conditional update; if we
                                       needed write-skew prevention
                                       I'd switch to SERIALIZABLE"
"Shard by user_id"                    "Shard by user_id with 256
                                       virtual shards via consistent
                                       hashing; here's how we'd reshard;
                                       these queries become cross-shard
                                       and we'll handle them with X"
"Use Postgres"                        "Postgres for OLTP, replicate
                                       via Debezium to BigQuery for
                                       analytics; here's why not
                                       Snowflake at this scale"
"This query is slow because no index" "EXPLAIN shows a Seq Scan with
                                       Rows Removed by Filter at 99%.
                                       Adding (a, b) gives Index Scan;
                                       I've checked pg_stat_user_indexes
                                       and three existing indexes are
                                       unused, so net write cost is flat"
■ Principal mindset: The deepest principal-level skill is defensive humility: knowing when to question
requirements, when to refuse complexity, when to insist on measurement, and when to admit you don't know but
here's how you'd find out. The candidate who says 'I'd need to know the workload before answering' outranks the
one with a confident wrong answer.
 — End of SQL Notes —
 Next in series: PySpark, Airflow, dbt, Kafka, Cloud Data Platforms
```


---

[← Back to main](./README.md)
