# Supplementary — Coding, FAANG, ML, State of Stack 2024-2026

> Section 9 of 10 · 53 pages in source PDF · pages 518–570

[← Back to main](./README.md)

---

Supplementary — Coding, FAANG, ML, State of Stack

Supplementary Additions The Four Chapters That Fill Remaining Gaps Coding Patterns at Staff+ · FAANG-Style Behavioral · ML for Data Engineers · State of the Stack 2024-2026 Additions to the Principal Engineer Notes series — covering the four gaps the original 8 topics deliberately skipped. Principal/Staff Engineer Level Reference

Supplementary — Coding, FAANG, ML, State of Stack

About This Supplement The original 8-topic series + front matter deliberately skipped four areas: coding interview patterns, company-specific behavioral guidance, ML-specific data engineering depth, and current state-of-the-art post-2024 developments. Each was skipped for a reason — and each is worth adding for completeness.

Chapter E — Coding Patterns That Actually Show Up at Staff+ Skipped originally because staff+ interviews rarely have hard LeetCode problems. But coding still happens at staff+: live SQL debugging, PySpark transformation challenges, custom data structures for specific platforms (LRU caches for feature stores, bloom filters, top-K). This chapter covers the coding that STAFF actually gets asked — different from the senior bar.

Chapter F — FAANG-Style Behavioral Skipped originally because company guides go stale fast. But the BIG THREE frameworks (Amazon's Leadership Principles, Meta's execution focus, Google's design-first depth) are durable enough to write about — and they shape interviews at hundreds of companies that copy these styles. This chapter covers the three dominant interview philosophies and how to map your experience to each.

Chapter G — ML for Data Engineers Skipped originally because ML felt like a separate book. But for data engineers interviewing for ML-platform-adjacent roles, there's specific depth needed: feature stores end-to-end, online/offline serving, training-serving skew, prediction monitoring, MLOps pipelines. This isn't data science (modeling), it's data engineering FOR ML. ~40 pages.

Chapter H — State of the Stack 2024-2026 Skipped originally because of staleness risk. But certain shifts are important and durable enough to cover: the catalog wars (Polaris, Unity Catalog OSS, Nessie, DuckLake), the Iceberg-vs-Delta convergence after the Tabular acquisition, dbt Fusion, DuckDB's rise, and what's changed in the warehouse landscape. Written with explicit acknowledgment of what may shift by 2027. ■ PRINCIPAL MINDSET: These four chapters add roughly 100 pages to the series. Read them only if relevant to your interview track: coding patterns help any role; FAANG behavioral helps if you're targeting those companies or their spiritual imitators; ML-for-DE helps for ML-platform roles; state-of-stack helps for anyone whose training data may be stale.

Supplementary — Coding, FAANG, ML, State of Stack

## Chapter E — Coding Patterns That Actually Show

```
E.1 The Reality of Coding at Staff+
What Staff+ Coding Looks Like
STAFF+ CODING INTERVIEWS are NOT:
  ✗ LeetCode Hard algorithm problems
  ✗ Dynamic programming gymnastics
  ✗ Tree / graph algorithm puzzles
  ✗ "Reverse a binary tree" trivia
STAFF+ CODING INTERVIEWS ARE:
  ✓ Live SQL debugging on real-looking schemas
  ✓ PySpark transformations under time pressure
  ✓ Designing a specific data structure for a platform need
  ✓ Writing code WHILE explaining the design trade-offs
  ✓ Optimizing slow code in front of the interviewer
  ✓ Reading and improving someone else's code
THE BAR:
  Senior:  can you SOLVE the problem?
  Staff+:  can you solve it CLEARLY, EXPLAIN it, and EVOLVE it
           as the interviewer adds constraints?
  Staff+ coding tests COMMUNICATION + PRAGMATISM alongside correctness.
The Three Categories
1. PRACTICAL DATA TRANSFORMATIONS
   "Given this DataFrame, do X" — SQL or PySpark
   Tests fluency with the language and the data model
2. DATA STRUCTURE DESIGN
   "Design a structure that supports operations X, Y, Z"
   Tests trade-off thinking and pragmatic choice
3. DEBUGGING / OPTIMIZATION
   "Here's a slow query / failing job — fix it"
   Tests diagnostic ability and depth of understanding
You'll see all three at staff+, often blended:
  "Here's a Spark job that times out. What do you do?"
  → debugging (diagnostic) + transformation (rewrite) + design (alternative)
```

Supplementary — Coding, FAANG, ML, State of Stack

```sql
E.2 SQL Patterns Tested at Staff+
Pattern 1: Dedup with a Twist
PROMPT: Given a table of user events with possible duplicates (same user_id,
event_time, event_type), return the LATEST event per (user, event_type),
breaking ties by event_id (lexicographic).
SOLUTION (window function):
  SELECT user_id, event_type, event_time, event_id, payload
  FROM (
    SELECT *,
      ROW_NUMBER() OVER (
        PARTITION BY user_id, event_type
        ORDER BY event_time DESC, event_id DESC
      ) AS rn
    FROM events
  ) ranked
  WHERE rn = 1;
VARIATIONS INTERVIEWER MIGHT ADD:
   What if there are 1B events? → partition pruning, incremental processing
   What if event_time can be NULL? → COALESCE or NULL ordering
   What if you only want CONFLICTS (events with duplicates)? → QUALIFY COUNT(*) > 1
   How would you stream this? → state per (user, event_type) keeping the max
```

```sql
STAFF SIGNAL: solving + handling the follow-ups smoothly without rewrites.
Pattern 2: Sessionization
PROMPT: Given web events with (user_id, event_time), define a "session" as a
sequence of events where consecutive events are within 30 minutes. Assign
session_id to each event.
SOLUTION (gap-and-island pattern):
  WITH gaps AS (
    SELECT user_id, event_time, event_type,
      CASE
        WHEN event_time - LAG(event_time) OVER (
               PARTITION BY user_id ORDER BY event_time
             ) > INTERVAL '30 minutes'
          THEN 1 ELSE 0
      END AS new_session
    FROM events
  ),
  sessions AS (
    SELECT *,
      SUM(new_session) OVER (
        PARTITION BY user_id ORDER BY event_time
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
      ) AS session_num
    FROM gaps
  )
  SELECT user_id, event_time, event_type,
    CONCAT(user_id, '_', session_num) AS session_id
  FROM sessions;
THE INSIGHT BEING TESTED:
  Gap detection (LAG) + cumulative sum to assign session numbers.
  Classic interview pattern; if you've seen it, you can crush it.
  If you haven't, you'll struggle to derive it on the fly.
VARIATIONS:
   Session timeout based on event type (more complex)
   Multi-device sessions (more identifiers)
   Backwards-looking session merging
Pattern 3: Top-N Per Group
```

Supplementary — Coding, FAANG, ML, State of Stack

```sql
PROMPT: Given sales (user_id, product_id, amount, date), return the top 3
products by total revenue PER user.
SOLUTION:
  WITH product_totals AS (
    SELECT user_id, product_id, SUM(amount) AS revenue
    FROM sales
    GROUP BY user_id, product_id
  ),
  ranked AS (
    SELECT *,
      ROW_NUMBER() OVER (
        PARTITION BY user_id ORDER BY revenue DESC, product_id
      ) AS rn
    FROM product_totals
  )
  SELECT user_id, product_id, revenue
  FROM ranked
  WHERE rn <= 3;
WHY THE TIE-BREAK (product_id):
  Without it, ties produce non-deterministic results.
  Staff candidates always think about ties; senior often forget.
VARIATIONS:
   DENSE_RANK vs ROW_NUMBER (returns more rows on ties)
   Top 3% instead of top 3 (NTILE or PERCENT_RANK)
   Top N where N varies per user (CROSS JOIN to per-user N)
Pattern 4: Running / Cumulative Aggregates
PROMPT: For each user, compute the cumulative spend over time AND the 7-day
rolling average spend.
SOLUTION:
  SELECT user_id, date,
    daily_spend,
    SUM(daily_spend) OVER (
      PARTITION BY user_id ORDER BY date
      ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS cumulative_spend,
    AVG(daily_spend) OVER (
      PARTITION BY user_id ORDER BY date
      ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
    ) AS rolling_7day_avg
  FROM daily_spend_table;
FOLLOW-UPS YOU'LL SEE:
   RANGE vs ROWS (RANGE handles gaps in dates differently)
   What if there are missing dates? → generate_series / date dimension
   What if you want 7 calendar days, not 7 records?
    RANGE BETWEEN INTERVAL '6' DAY PRECEDING AND CURRENT ROW
    (some warehouses don't support RANGE with intervals; Snowflake does)
STAFF SIGNAL: know the ROWS vs RANGE distinction; handle missing data.
Pattern 5: Joins With Edge Cases
PROMPT: Given two tables, return all users with their LATEST order (if any).
Some users have no orders.
SOLUTION (LEFT JOIN + window or correlated subquery):
  -- Option A: LEFT JOIN with window function
  SELECT u.user_id, u.name, o.order_id, o.order_date, o.amount
  FROM users u
  LEFT JOIN (
    SELECT *,
      ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY order_date DESC) AS rn
    FROM orders
```

Supplementary — Coding, FAANG, ML, State of Stack

```sql
  ) o ON u.user_id = o.user_id AND o.rn = 1;
  -- Option B: LATERAL JOIN (Postgres, Snowflake support)
  SELECT u.*, o.*
  FROM users u
  LEFT JOIN LATERAL (
    SELECT * FROM orders WHERE user_id = u.user_id
    ORDER BY order_date DESC LIMIT 1
  ) o ON TRUE;
FOLLOW-UPS:
   Which is faster? Depends on cardinality + indexes/clustering
   What about users with 0 orders? Both handle correctly (LEFT JOIN)
   How about avg order amount instead of latest? Aggregation, not window
   Multiple "latest" criteria (e.g., latest paid AND latest shipped)?
    Subquery per criterion, then LEFT JOIN both
Pattern 6: The Slow Query
PROMPT: This query takes 4 minutes on a 100M-row table. Make it fast.
  SELECT user_id, COUNT(*) AS event_count
  FROM events
  WHERE EXTRACT(YEAR FROM event_time) = 2024
  GROUP BY user_id
  HAVING COUNT(*) > 100;
STAFF DIAGNOSIS:
  1. EXTRACT() on event_time blocks pruning (non-SARGable)
     FIX: WHERE event_time >= '2024
01' AND event_time < '2025
01'
```

- 2. No partition / cluster hint mentioned — verify the table has them
- If event_time IS the partition column, the rewrite alone might fix everything
- 3. HAVING is applied AFTER GROUP BY — fine here but be aware

```sql
  4. COUNT(*) on this scale: consider APPROX_COUNT_DISTINCT if approximate is OK
REWRITTEN:
  SELECT user_id, COUNT(*) AS event_count
  FROM events
  WHERE event_time >= '2024
01' AND event_time < '2025
01'
  GROUP BY user_id
  HAVING COUNT(*) > 100;
THE STAFF MOMENT:
  Don't just fix the query. Explain WHY the rewrite helps.
  "EXTRACT() forces the engine to evaluate the function per row instead of
   using partition stats. The rewrite expresses the same predicate as a
   range, which the optimizer can use for partition pruning."
  This is the difference between "I made it fast" and "I understand WHY it
  was slow".
```

Supplementary — Coding, FAANG, ML, State of Stack

E.3 PySpark Patterns Tested at Staff+ Pattern 1: Wide-to-Long Transformation PROMPT: Given a DataFrame with columns (user_id, jan, feb, mar, apr, may, jun, ...) where each month column holds a monthly value, reshape to (user_id, month, value). SOLUTION (using stack): from pyspark.sql import functions as F

months = ['jan', 'feb', 'mar', 'apr', 'may', 'jun']

```
  expr = "stack({}, {}) AS (month, value)".format(
      len(months),
      ", ".join([f"'{m}', {m}" for m in months])
  )
```

```python
  long_df = df.select("user_id", F.expr(expr))
OR USING UNPIVOT (newer Spark):
  long_df = df.unpivot(
    ids=["user_id"],
    values=months,
    variableColumnName="month",
    valueColumnName="value"
  )
FOLLOW-UPS:
   What if some month columns are missing? → conditional inclusion
   Long to wide (pivot)? → df.groupBy().pivot('month').agg(F.first('value'))
   Same problem in pandas? → df.melt()
Pattern 2: Skewed Join Handling
PROMPT: You have orders (1B rows) joined to users (100M rows). Most orders
are from a few power users. The join hangs. Fix it.
DIAGNOSIS:
  Skew: a few user_id values dominate orders.
  Default sort-merge join puts all rows for one user_id on one task.
  That task takes hours; others finish quickly.
SOLUTIONS:
  1. ENABLE AQE WITH SKEW HANDLING
     spark.conf.set("spark.sql.adaptive.enabled", "true")
     spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")
     # AQE auto-splits skewed partitions
  2. BROADCAST IF SMALL TABLE FITS
     joined = orders.join(F.broadcast(users), "user_id")
     # Only if users is small enough (a few GB max)
  3. SALTING (manual)
     # Add salt to skewed keys
     SALT = 10
     orders_salted = orders.withColumn(
       "user_id_salted",
       F.concat(F.col("user_id"), F.lit("_"), (F.rand() * SALT).cast("int"))
     )
     # Replicate users by salt
     users_replicated = users.crossJoin(
       spark.range(SALT).toDF("salt")
     ).withColumn(
       "user_id_salted",
       F.concat(F.col("user_id"), F.lit("_"), F.col("salt"))
     )
```

Supplementary — Coding, FAANG, ML, State of Stack

```sql
     # Join on salted keys
     joined = orders_salted.join(users_replicated, "user_id_salted")
STAFF SIGNAL:
  Know all three approaches. Pick AQE first (cheap). Broadcast next.
  Salting only if data won't broadcast and AQE isn't available/sufficient.
  Explain WHY salting works (distributes skewed key across partitions).
Pattern 3: Incremental Processing
PROMPT: You have a daily Spark job that processes new events. The full
events table has 5 years of history. How do you process only NEW events?
SOLUTION:
  # Read state: last processed timestamp
  last_processed = spark.sql(
    "SELECT MAX(event_time) AS last_ts FROM processed_events"
  ).collect()[0]['last_ts']
```

```
  if last_processed is None:
    last_processed = '1970
01'  # Initial run
```

```
  # Read only new events
  new_events = spark.read.table("events").filter(
    F.col("event_time") > F.lit(last_processed)
  )
```

```
  # Process
  result = new_events.groupBy("user_id").agg(
    F.count("*").alias("new_events"),
    F.max("event_time").alias("max_event_time")
  )
```

```sql
  # MERGE into target (Delta / Iceberg)
  delta_table.merge(
    result.alias("source"),
    "target.user_id = source.user_id"
  ).whenMatchedUpdate(set={
    "events": "target.events + source.new_events",
    "last_seen": "source.max_event_time"
  }).whenNotMatchedInsert(values={
    "user_id": "source.user_id",
    "events": "source.new_events",
    "last_seen": "source.max_event_time"
  }).execute()
FOLLOW-UPS:
   What about late-arriving data? → lookback window (process last 7 days)
   What about exactly-once? → idempotent MERGE; replay-safe
   What if the events table is partitioned by date? → predicate pushdown
    eliminates old partitions; even more efficient
Pattern 4: UDF Avoidance
PROMPT: Apply a complex business rule per row. You wrote a Python UDF.
The job is 10x slower than expected. Why and what do you do?
PYTHON UDF COSTS:
   Serialize row from Spark JVM to Python
   Python interpreter evaluates the function
   Serialize result back to JVM
   No optimizer awareness; can't push down predicates
   Sometimes 10-100x slower than native expressions
ALTERNATIVES (in order of preference):
  1. EXPRESS IN NATIVE SPARK SQL FUNCTIONS
     If the logic uses CASE WHEN, math, string ops: use F.col() and F.expr()
     Spark's optimizer can plan around it; Photon can vectorize
```

Supplementary — Coding, FAANG, ML, State of Stack

```python
  2. PANDAS UDF (vectorized)
     @F.pandas_udf(returnType=DoubleType())
     def my_udf(s: pd.Series) -> pd.Series:
       return s * 2  # vectorized; per-batch
     # 10-100x faster than row-at-a-time UDF
  3. SCALA UDF (if absolutely needed)
     Faster than Python; runs in JVM
     More setup; requires Scala build
  4. ARROW-BASED UDF (newest)
     pandas_udf is built on Arrow; minimizes serialization
STAFF SIGNAL:
  Recognize Python UDF as a smell; know the cheap alternatives;
  justify when a UDF is necessary (truly complex logic) vs when it isn't.
WHEN A UDF IS THE RIGHT ANSWER:
   Complex logic that DOESN'T map to SQL primitives
   External API calls (rare)
   ML inference (use pandas_udf for batching)
Pattern 5: Window Function vs GroupBy
PROMPT: For each order, compute the running total of orders by that user.
WRONG (loses other columns):
  totals = df.groupBy("user_id").agg(F.sum("amount").alias("total"))
  # This collapses to one row per user; loses order-level detail
RIGHT (window function):
  from pyspark.sql import Window
```

```
  w = Window.partitionBy("user_id").orderBy("order_date") \
            .rowsBetween(Window.unboundedPreceding, Window.currentRow)
```

```
  result = df.withColumn(
    "running_total", F.sum("amount").over(w)
  )
KEY DISTINCTION:
  GROUP BY collapses rows.
  WINDOW preserves rows; adds derived columns.
WHEN TO USE EACH:
  GROUP BY: when you want one row per group
  WINDOW: when you want to keep row-level grain + add aggregated context
PERFORMANCE NOTE:
  Window functions require sorting/shuffling within partitions.
  For very large windows, can be expensive; consider pre-aggregating
  + joining back.
```

Supplementary — Coding, FAANG, ML, State of Stack

```python
E.4 Data Structure Design Challenges
Challenge 1: LRU Cache for Feature Store
PROMPT: Design an in-memory LRU cache for an ML feature store.
Constraints: 1M keys, ~100 GB total, sub-ms get latency, ~10K QPS.
DATA STRUCTURE:
  HashMap (key → value pointer) + Doubly Linked List (LRU ordering)
   Get: O(1) — hash lookup, move node to head
   Put: O(1) — insert at head, evict from tail if full
IMPLEMENTATION SKETCH:
  class LRUCache:
    def __init__(self, capacity):
      self.capacity = capacity
      self.cache = {}  # key → node
      self.head = Node()  # dummy
      self.tail = Node()  # dummy
      self.head.next = self.tail
      self.tail.prev = self.head
```

```python
    def get(self, key):
      if key in self.cache:
        node = self.cache[key]
        self._move_to_head(node)
        return node.value
      return None
```

```python
    def put(self, key, value):
      if key in self.cache:
        self.cache[key].value = value
        self._move_to_head(self.cache[key])
      else:
        if len(self.cache) >= self.capacity:
          lru = self.tail.prev
          self._remove(lru)
          del self.cache[lru.key]
        new_node = Node(key, value)
        self.cache[key] = new_node
        self._add_to_head(new_node)
STAFF-LEVEL EXTENSIONS:
   Thread safety: locks per shard
   Distributed: hash keys to N nodes; consistent hashing for resizing
   Persistence: write-back to Redis/disk
   TTL: time-based eviction in addition to LRU
   Metrics: hit rate, eviction rate
INTERVIEWER VARIATIONS:
   "Now make it thread-safe" → sharded locks
   "Now distribute it" → consistent hashing + cluster of LRU nodes
   "Now persist" → write-through to Redis
Challenge 2: Top-K Streaming
PROMPT: Stream of events (user_id, event_type, timestamp). Track top-K
most active users in the last hour, updating in near-real-time. K=100.
NAIVE: keep all counts, sort. O(N log N) per refresh. Doesn't scale.
DATA STRUCTURE: Min-Heap of size K
  Maintain heap of (count, user_id) pairs.
  Heap is bounded; smallest count at top.
```

```
  On each event:
    Increment user's count in a HashMap
    If user already in heap: heap might need re-ordering
    Else if count > heap_min: replace heap_min with this user
```

Supplementary — Coding, FAANG, ML, State of Stack

```
  Issue: counts change continuously; heap can become stale.
BETTER: Count-Min Sketch + Top-K
  Count-Min Sketch (probabilistic): O(1) increment, approximate count
  Pair with min-heap of top-K candidates
  Or use SPACE-SAVING ALGORITHM:
    Bounded counter list of K entries
    On event: if user in counters, increment;
              else replace smallest counter with this user
    Tracks top-K approximately
WINDOWING (last hour):
  Bucket events by minute; maintain 60 sliding buckets
  When a bucket rotates out: decrement all its counts
```

```
LIBRARY IMPLEMENTATIONS:
  Apache DataSketches: production-grade Count-Min + top-K
  Redis: ZADD/ZINCRBY + ZREVRANGE (exact, in memory)
  ClickHouse: topK() aggregate function
STAFF SIGNAL:
  Don't reinvent. Recognize probabilistic data structures save orders of
  magnitude memory at the cost of approximate answers — usually acceptable.
Challenge 3: Bloom Filter for Existence Check
PROMPT: You need to check whether a user has been seen before across
100B events. Exact answer in a hash table needs ~5 TB RAM. Alternatives?
BLOOM FILTER:
  Probabilistic structure: tells you DEFINITELY NOT or MAYBE.
  No false negatives; false positives at configurable rate.
  m bits, k hash functions
  Add: set k bits to 1 based on k hashes
  Check: all k bits = 1? maybe; any = 0? definitely not
  For 1B unique users at 1% false positive rate:
    m ≈ 1.44 × 1B × log2(1/0.01) ≈ 9.6 Gbits ≈ 1.2 GB
```

```
  That's 4000x less memory than a hash table.
WHEN TO USE:
  ✓ "Have I seen this before?" lookup
  ✓ Pre-filter to expensive lookup (e.g., DB check)
  ✓ Deduplication in streaming
WHEN NOT TO USE:
  ✗ Need exact answer
  ✗ Need to DELETE entries (use Counting Bloom Filter; still not perfect)
  ✗ Need to retrieve values, not just existence
EXTENSIONS:
   Cuckoo Filter: supports deletes, slightly less false positive
   Counting Bloom Filter: counts; supports decrements
   RoaringBitmap: exact, compressed; great for IDs in range
INTERVIEWER FOLLOW-UPS:
   What's the optimal k (hash functions)? → k = (m/n) ln 2
   How does false positive rate scale? → exponentially with bits
   Distributed bloom filter? → bitwise OR across nodes
Challenge 4: HyperLogLog for Cardinality
PROMPT: Count distinct users across 500B events. Exact count requires
hashing all unique users in memory. Approximate alternatives?
HYPERLOGLOG:
  Estimates cardinality with O(1) memory per measurement.
```

Supplementary — Coding, FAANG, ML, State of Stack

```sql
  Standard error: ~1% with 16 KB of state (2048 registers × 6 bits).
  Insight: Hash each value uniformly to [0, 1). The MAXIMUM number of
  leading zeros in any hash gives a logarithmic estimate of cardinality.
  Use multiple "registers" (sub-hashing) for variance reduction.
  Operations:
    Add: hash item, update one register with max leading zeros
    Estimate: harmonic mean across registers × correction
WHEN TO USE:
  ✓ COUNT(DISTINCT user_id) at scale
  ✓ Real-time unique counters (active users, unique impressions)
  ✓ Mergeable across machines (just take max per register)
WHEN NOT TO USE:
  ✗ Need exact count
  ✗ Need to enumerate the distinct values
  ✗ Very small cardinalities (overhead dominates)
LIBRARY IMPLEMENTATIONS:
   APPROX_COUNT_DISTINCT in BigQuery, Snowflake, Redshift, Databricks SQL
   Apache DataSketches HLL
   Redis PFADD / PFCOUNT
   Spark approx_count_distinct
REAL EXAMPLE:
  COUNT(DISTINCT user_id) on a 1 PB table = full scan
  APPROX_COUNT_DISTINCT(user_id) = sub-second from pre-computed sketches
```

Cost difference: 1000x. Accuracy: within 1%. Trade-off is almost always worth it for analytical use cases. ■ PRINCIPAL MINDSET: Probabilistic data structures (Bloom, HLL, Count-Min) are a staff-level vocabulary. They show up in real platforms (Snowflake's Search Optimization Service uses bloom filters internally; warehouse APPROX functions use HLL). Knowing when to reach for them — and being able to explain the trade-offs — is a signal. Memorize the use cases, not the math.

Supplementary — Coding, FAANG, ML, State of Stack

```sql
E.5 Live Debugging Scenarios
Scenario 1: The Spark Job That Won't Finish
SETUP: 'This Spark job has been running 4 hours. Should finish in 30 min.
What do you check?'
STAFF DIAGNOSTIC PROCESS (out loud):
  1. SPARK UI — STAGES TAB
     Is one stage taking 90% of the time? → stage-specific issue.
     All stages slow? → cluster sizing / config issue.
  2. WITHIN THE PROBLEM STAGE
     Look at task duration distribution.
     One task taking forever, others fast? → SKEW.
     All tasks slow uniformly? → too few partitions OR weak nodes.
  3. SKEW SIGNALS:
      Task input size: one task reading 100 GB, others 1 GB
      Task duration: one task at 90% of stage runtime
      Specific stage type: usually shuffle stages (joins, groupBy)
  4. POSSIBLE FIXES:
      Enable AQE: spark.sql.adaptive.skewJoin.enabled=true
      Broadcast small side of join
      Salting (manual partition split)
      Filter or pre-aggregate before the skewed operation
  5. OTHER CAUSES:
      OOM / GC pressure → memory tuning
      Slow source (S3 listings, JDBC) → predicate pushdown, partition discovery
      Spilling to disk excessively → memory tuning, repartition
WHAT INTERVIEWER WANTS TO HEAR:
  The DIAGNOSTIC PROCESS, not just the answer.
  Show that you have a framework for debugging.
Scenario 2: The Pipeline With Wrong Numbers
SETUP: 'Dashboard shows 15% drop in revenue today. Pipeline ran successfully.
What do you investigate?'
STAFF PROCESS:
  1. CONFIRM THE ISSUE
     Is the dashboard right? (Check the underlying gold table directly.)
     Is the drop real? (Check yesterday's data path, source.)
  2. CHECK PIPELINE FRESHNESS
     When did each upstream model last run?
     Did any task fail and not alert?
     Was there a partial backfill?
  3. CHECK SOURCE DATA
     Did the source system have an outage / change?
     Is the volume normal? (Anomaly: 15% drop = 15% fewer rows?)
     Are there schema changes?
  4. CHECK TRANSFORMATIONS
     Recent dbt model changes? (git log)
     New filters added?
     Type changes that might silently drop rows (e.g., NULL handling)?
  5. CHECK LATE DATA
     Is yesterday's data still arriving?
     Does the lookback window account for typical lateness?
  6. RECONCILE
     Sum from raw → silver → gold; find the layer where the drop appears.
```

Supplementary — Coding, FAANG, ML, State of Stack

```sql
     Lineage tools help here.
THE PATTERN:
  Start at the symptom; trace upstream layer by layer.
  Find the FIRST layer where the number is wrong.
  Fix at that layer.
NEVER:
  Adjust downstream to "match expectations" — fix the cause.
Scenario 3: The Slow Warehouse Query
SETUP: 'This Snowflake query was fast yesterday, slow today. Same query.'
STAFF DIAGNOSTIC:
  1. CHECK QUERY HISTORY
     Compare today vs yesterday: same plan? same warehouse size?
     Was the warehouse suspended and resumed? → cold cache
     Did concurrency spike? → queueing
  2. CHECK CLUSTERING
     Did data growth move out-of-cluster?
     SELECT SYSTEM$CLUSTERING_INFORMATION('table', '(col)')
     Compare to yesterday.
  3. CHECK STATISTICS
     For warehouses with manual statistics (older Redshift): are they stale?
     For Snowflake/BQ: auto-stats — usually not the issue.
  4. CHECK PHYSICAL LAYOUT
     Recent large UPDATE/DELETE? Created lots of small partitions?
     For Snowflake: re-clustering might help
     For lakehouse: compaction
  5. CHECK CONCURRENCY
     Other heavy queries on the same warehouse?
     Multi-cluster might not have scaled up
     Look at warehouse load history
  6. EXPLAIN PLAN
     Always re-check the plan.
     Did the optimizer change strategy? (Snowflake auto-adapts)
  7. CACHE-RELATED
     Result cache miss? (CURRENT_TIMESTAMP() invalidates)
     Local SSD cache cold? (warehouse resumed)
WORTH MEMORIZING THIS LIST.
It's the universal warehouse debugging checklist.
■ INTERVIEW ANGLE: Q: Walk me through how you'd debug a query that suddenly got slow.
Staff: [recites checklist above with clarifying questions: 'when did it get slow', 'is the data size different', 'is the
warehouse the same size'] — the discipline is to RULE OUT systematically, not guess. Most slow-query
debugging is about finding what CHANGED since it was fast. Statistics, clustering, concurrency, cache state, or
data shape — usually one of those five.
```

Supplementary — Coding, FAANG, ML, State of Stack

```sql
E.6 Coding Cheat Sheet for Staff+ Interviews
SQL Patterns to Have Cold
 ROW_NUMBER / RANK / DENSE_RANK with PARTITION BY
 LAG / LEAD for sessionization, gap detection
 Window with ROWS BETWEEN for running totals + rolling averages
 ROWS vs RANGE distinction
 QUALIFY (Snowflake / BigQuery / Databricks; replaces subquery with rn=1)
 PIVOT / UNPIVOT
 Recursive CTEs (for hierarchies)
 MERGE (UPSERT semantics)
 ANTI / SEMI JOIN (EXISTS / NOT EXISTS patterns)
 APPROX_* functions (COUNT_DISTINCT, QUANTILES)
 ARRAY_AGG / STRING_AGG for grouped aggregation
 CASE WHEN with multiple conditions
 SARGable predicates (avoid functions on indexed columns)
PySpark Patterns to Have Cold
 DataFrame API: groupBy, agg, window, withColumn, filter, join
 Window functions same syntax as SQL but via pyspark.sql.window.Window
 Broadcast hints: F.broadcast(small_df)
 AQE settings for skew + partition coalescing
 Pandas UDFs (vectorized, fast) > Python UDFs (slow)
 Caching: .cache() / .persist() with storage levels
 Avoid .collect() on large data
 repartition vs coalesce (partition count up vs down)
 Reading parquet with predicate pushdown
 Writing partitioned outputs
Data Structures to Know By Name
 Bloom Filter — existence; false positives only
 Cuckoo Filter — Bloom + deletes
 Count-Min Sketch — frequency estimation
 HyperLogLog — cardinality estimation
 Top-K (Space-Saving Algorithm) — heavy hitters
 T-Digest — quantile estimation
 Reservoir Sampling — uniform random sample from stream
 LSM Tree — write-optimized storage (Cassandra, RocksDB)
 B+ Tree — range queries (Postgres, MySQL)
 Skip List — alternative to balanced tree (Redis sorted sets)
 Consistent Hashing — distributed sharding
Debugging Frameworks
SLOW QUERY:
  Statistics → Plan → Clustering → Concurrency → Cache → Data Shape
WRONG NUMBERS:
  Confirm symptom → Pipeline fresh? → Source healthy? → Code change?
  Late data? → Reconcile layer by layer
SPARK JOB SLOW:
  Stage durations → Task distribution → Skew? → Memory? → Source?
DBT MODEL FAILS:
  Logs → Compiled SQL → Run manually → Source data → Schema change?
KAFKA LAG:
  Producer rate → Broker capacity → Consumer fetch → Processing time
  → Downstream backpressure
■ PRINCIPAL MINDSET: At staff+, coding interviews are 30-50% communication. The candidate who silently
solves the problem loses to the one who narrates the diagnostic process, considers alternatives out loud,
acknowledges trade-offs, and asks clarifying questions. Practice solving problems while talking through your
reasoning. The silent-genius archetype is the wrong one for staff+.
```

Supplementary — Coding, FAANG, ML, State of Stack

## Chapter F — FAANG-Style Behavioral Interviews

F.1 The Three Dominant Styles Why These Three Amazon, Meta, and Google have outsized influence on tech industry interview practices. Amazon's Leadership Principles, Meta's execution-first culture, and Google's design-depth philosophy are explicitly copied by hundreds of companies. Even if you're not interviewing at FAANG, knowing these three styles helps you recognize what your interviewer is probing for. The Three at a Glance Company Behavioral Focus Probing For Amazon Leadership Principles (16 LPs) Customer obsession, ownership, dive deep, bias for action Meta Execution + impact at scale Move fast, ship metrics, navigate ambiguity Google Design depth + technical leadership Rigorous thinking, system architecture, "Googleyness" The Common Substrate ALL THREE TEST FOR:  Scope (did you operate at the level claimed?)  Influence (did you make others better?)  Judgment (did you make sound decisions in ambiguity?)  Ownership (did you fight for what matters?)  Growth (did you learn from failures?) WHAT VARIES:  Framework / vocabulary (LPs vs metrics vs Googleyness)  Emphasis (Amazon: ownership; Meta: speed; Google: depth)  Format (Amazon: explicit LP per question; Meta: project-driven; Google: cross-functional + technical) PREPARATION OVERLAP: Your 8 STAR stories from the front matter apply to all three. The DIFFERENCE is how you FRAME them. Same story, different framing per company.

Supplementary — Coding, FAANG, ML, State of Stack

F.2 The Amazon Style — Leadership Principles The Core Idea Amazon interviews are explicitly behavioral-heavy. Most rounds are framed around one or two Leadership Principles (LPs). The interviewer is trained to map your stories to specific LPs and rate you on each. Coming with the framework in mind is essentially required. The 16 LPs (As of 2024-2026) CUSTOMER-FACING:

#### 1. Customer Obsession

#### 2. Earn Trust

#### 3. Are Right, A Lot

#### 4. Dive Deep

#### 5. Have Backbone; Disagree and Commit

#### 6. Bias for Action

#### 7. Deliver Results

#### 8. Insist on the Highest Standards

#### 9. Ownership

#### 10. Frugality

#### 11. Learn and Be Curious

#### 12. Hire and Develop the Best

#### 13. Think Big

#### 14. Invent and Simplify

#### 15. Strive to be Earth's Best Employer (added 2021)

```
  16. Success and Scale Bring Broad Responsibility (added 2021)
Each Amazon interview round will explicitly probe 2-4 LPs.
You should know which LPs match each of your stories.
The Amazon STAR (with LP framing)
TRADITIONAL STAR + EXPLICIT LP NAMING
  S — Situation: brief context (10 sec)
  T — Task: your specific responsibility (10 sec)
  A — Action: what YOU did, mapping to LP behaviors (40-50 sec)
       "I dove deep into the data to understand..."  (Dive Deep)
       "I took ownership of the customer outcome..."  (Customer Obsession + Ownership)
       "I made a call without complete data..."       (Bias for Action)
  R — Result: outcome with numbers (15-20 sec)
       "Lifted X by Y%; saved $Z; learned A about B..."
THE AMAZON INTERVIEWER IS LITERALLY SCORING YOU ON LPs.
Don't be subtle; explicitly use LP language.
Story-to-LP Mapping (Examples)
STORY: "Drove migration from Hadoop to Iceberg lakehouse"
  Maps to:
     Think Big (architectural transformation)
     Ownership (owned multi-quarter initiative)
     Bias for Action (started despite uncertainty)
     Insist on Highest Standards (technical rigor)
     Deliver Results (measurable outcome)
STORY: "Disagreed with VP on warehouse choice; persuaded with data"
  Maps to:
     Have Backbone; Disagree and Commit
```

Supplementary — Coding, FAANG, ML, State of Stack

```
     Are Right, A Lot
     Dive Deep (used data, not opinion)
     Earn Trust (maintained relationship despite disagreement)
STORY: "Cleaned up 200 unused dbt models; reduced CI time 80%"
  Maps to:
     Invent and Simplify
     Frugality (efficiency)
     Ownership (took on unglamorous work)
     Deliver Results (measurable)
STORY: "Mentored 3 engineers to promotion to senior"
  Maps to:
     Hire and Develop the Best
     Earn Trust
     Insist on Highest Standards (raising the bar)
Amazon-Specific Question Patterns
"Tell me about a time you had to dive deep into a problem."
  → Story with technical depth; surface root cause not symptom
  → Concrete numbers, specific data sources used
  → What you discovered that others missed
"Tell me about a time you took a risk."
  → Bias for Action + Think Big
  → Real stakes; conscious decision-making
  → Acknowledge the downside even if outcome was positive
"Tell me about a customer-related decision."
  → Customer Obsession is THE top LP
  → Even infra/data engineering should have a customer angle
  → Internal "customers" count (engineers, analysts) — frame it that way
"Tell me about a time you disagreed."
  → Have Backbone; Disagree and Commit
  → Show maturity in HOW you disagreed (data, private first)
  → Show the COMMIT part — if you lost, you still executed
"Tell me about a time you failed."
  → Real failure; specific impact; concrete learning
  → Bias for Action means you'll have made mistakes; this is OK
"Why Amazon?" (yes, even for senior+)
  → Specific reasons: scale, ownership model, LPs, the role itself
  → Avoid: pay, prestige, "I want to learn"
Amazon Pitfalls
■ "WE" INSTEAD OF "I"
  Amazon especially harsh on this. They want YOUR contribution clear.
■ NO METRICS
  "I improved things" = no impact. Numbers always.
■ NOT KNOWING THE LPs
  Read all 16 before the interview. Be ready to reference them.
■ AVOIDING DISAGREEMENT STORIES
  "I never disagree with my team" = bad fit; not staff material.
■ TOO MUCH TECHNICAL DETAIL
  Behavioral is about YOU, not the tech. Keep tech terse; expand on
  decisions and actions.
■ NOT FOLLOWING UP
  Amazon interviewers love to ask "and then?" Have 2-3 layers of detail
  ready for each story.
■ PRINCIPAL MINDSET: Amazon's LP framework is the most explicit behavioral framework in tech. Some find it
formulaic; others appreciate the structure. Either way, prepare 8-12 stories with explicit LP mappings. Many other
```

Supplementary — Coding, FAANG, ML, State of Stack

companies (Wayfair, Comcast, etc.) have copied the LP model — same preparation transfers.

Supplementary — Coding, FAANG, ML, State of Stack

```sql
F.3 The Meta Style — Execution & Impact at Scale
The Core Idea
Meta interviews emphasize speed, impact, and the ability to navigate ambiguity at scale. The cultural ethos is
execution — what did you ship, how fast, what was the impact, did it move metrics? Meta tends to be less
explicit-framework than Amazon and more conversational, but the implicit signals are clear.
Meta's Cultural Themes
1. MOVE FAST
   Ship velocity is celebrated.
   "I waited for perfect data before deciding" is a red flag.
   "I shipped a v1 in 2 weeks, iterated based on real usage" is gold.
2. IMPACT > OUTPUT
   Metrics-driven culture.
   What metric moved? By how much?
   "Improved load time" → "Improved p95 load time 40%, lifted DAU 3%"
3. NAVIGATE AMBIGUITY
   Meta's scale + speed creates ambiguity.
   Showing you can make sound calls without complete info is critical.
4. STRONG OPINIONS, LOOSELY HELD
   Have a view; defend it; update on new information.
   Wishy-washy = weak signal.
5. DIRECT COMMUNICATION
   Meta culture is famously direct.
   Hedging and corporate-speak get docked.
6. SCALE AWARENESS
   Solutions should work at billions-of-users scale or scale gracefully.
   "I built a quick fix that works for now" should be followed with
   "but the proper fix would be X".
Meta Behavioral Question Patterns
"Tell me about your biggest impact."
  → Single story; significant scale; metric-driven
  → Show your specific role in the impact
  → "I led 4 engineers to build X; impacted N users; moved metric Y by Z%"
"Tell me about a project where you had to move fast."
  → Real time pressure
  → Trade-offs you made (perfect vs done)
  → How you de-risked while still shipping fast
"Tell me about ambiguity you faced."
  → Goal was unclear / requirements were vague
  → How you brought structure
  → How you made progress despite uncertainty
"Tell me about a technical decision and what you'd do differently."
  → Real decision with real consequences
  → Honest reflection (not "everything was perfect")
  → Show evolved thinking
"Why are you leaving your current company?"
  → Growth / impact / mission
  → No bitterness toward current employer
  → Specific reasons Meta excites you
"What kind of work energizes you?"
  → Self-knowledge signal
  → Should align with the role
  → Don't pretend; mismatch hurts both sides
```

Supplementary — Coding, FAANG, ML, State of Stack

```sql
The 'Move Fast' Frame
META LOVES STORIES WHERE YOU CHOSE SPEED OVER PERFECTION.
GOOD: 'Saw the analytics dashboard was broken. Senior team said it'd take
       a month. I built a minimum-viable replacement in 4 days using
       existing tools. Got us back to data-driven decisions immediately.
       Later, the senior team's better solution superseded mine. But the
       4-day version saved us from a month of flying blind.'
WHY THIS WORKS:
   Real stakes
   Conscious trade-off (acknowledged the "right" solution)
   Bias toward action over waiting
   Humility (your version was superseded; that's fine)
CONTRAST WITH AMAZON FRAMING:
  Amazon would expect more reflection on customer impact, the LP framing.
  Meta wants the speed + impact + practical judgment narrative.
Meta Pitfalls
■ ANALYSIS PARALYSIS STORIES
  "We spent 6 months evaluating tools before deciding" — red flag.
  Meta values bias for action.
■ NO METRICS
  Same as Amazon, but doubly true at Meta.
■ VAGUE SCOPE
  "Many users" → how many? "Big project" → how big?
■ HEDGING IN ANSWERS
  Meta culture is direct. "It depends" is a non-answer.
  Pick a position; acknowledge trade-offs; commit.
■ NOT CONNECTING TO BUSINESS IMPACT
  Technical achievements need a "so what?" — what business outcome resulted?
■ OVER-PREPARED STORIES
  Meta interviewers can spot rehearsed answers. Sound natural.
  Have the structure; vary the delivery.
Meta-Specific Questions for Data Engineers
"How did you choose tools for your last platform decision?"
  → Show the trade-off matrix
  → Acknowledge alternatives
  → Defend the choice with criteria
"Walk me through how you'd scale a pipeline 10x."
  → Identify bottlenecks
  → Specific changes per layer
  → Cost vs latency vs reliability trade-offs
"How do you measure platform success?"
  → SLIs / SLOs concrete
  → Customer (analyst, engineer) satisfaction
  → Cost / latency / reliability metrics
  → Strategic outcomes (enabled new product, etc.)
```

Supplementary — Coding, FAANG, ML, State of Stack

```
F.4 The Google Style — Design Depth &
Googleyness
The Core Idea
Google's interview philosophy emphasizes rigorous technical design thinking and what they call
'Googleyness' — intellectual humility, collaborative problem-solving, and curiosity. The behavioral rounds are
typically less explicit-framework than Amazon but probe deeply for design judgment and cross-functional
partnership.
The Google Themes
1. INTELLECTUAL RIGOR
   Show your reasoning explicitly.
   "I considered X and Y because Z; rejected W because of A."
   Not just "I chose X."
2. SCALE AT THE LIMIT
   Google works at hyperscale.
   Stories that involve billions of operations, exabytes of data,
   or sub-millisecond latency are bonus.
3. GOOGLEYNESS (intentionally vague; multiple meanings)
   Intellectual humility (can admit you don't know)
   Collaborative (work well across functions)
   Curiosity (learn beyond your immediate scope)
   Comfort with ambiguity
   Bias toward action (similar to Meta but more thoughtful)
4. TECHNICAL DEPTH UNDER PRESSURE
   Expect to be probed deeply on any architectural choice.
   "Why this and not X?" repeatedly.
   You should have layered answers — surface, then deeper, then deepest.
5. CROSS-FUNCTIONAL COLLABORATION
   Google emphasizes work across teams, regions, disciplines.
   Stories that include working with non-engineering functions score well.
6. ETHICAL / RESPONSIBLE CONSIDERATIONS
   Google increasingly tests for awareness of broader implications.
   Privacy, fairness, environmental cost — bonus if you address.
Google Question Patterns
"Walk me through a design decision you're proud of."
  → Multi-layer trade-off narrative
  → Alternatives considered
  → How you'd evolve it if you started over
"Tell me about a project that required cross-team collaboration."
  → Real cross-functional work (not just within your team)
  → How you navigated different priorities
  → What you learned about working with [other discipline]
"Describe a time you had to learn something quickly."
  → Curiosity signal
  → Show specific learning approach (not just "I read docs")
  → How you applied the learning
"Tell me about ambiguity in a project."
  → Similar to Meta but Google wants more reflection on the process
  → How you brought structure
  → How you knew when to commit
"How do you decide what NOT to do?"
  → Trade-off judgment
  → Prioritization frameworks
```

Supplementary — Coding, FAANG, ML, State of Stack

```
  → Examples of things you de-prioritized
"What's a hard problem you've worked on?"
  → Real technical depth
  → Honest about what made it hard
  → What you learned about problem-solving
Google Pitfalls
■ SUPERFICIAL TECHNICAL ANSWERS
  Google probes deeper than most. Be ready for 4-5 'why' follow-ups.
■ NOT KNOWING YOUR TOOLS DEEPLY
  Claim to know X? Expect to be tested on X. Don't overclaim.
■ INTELLECTUAL ARROGANCE
  Google specifically tests for humility. "I always know best" loses.
  Show that you've changed your mind, admit gaps, ask questions.
■ NO CROSS-FUNCTIONAL STORIES
  If all your stories are within-team, you'll seem narrow.
  Have stories with PMs, designers, business, infra, security, legal.
■ BEING TOO POLISHED
  Google interviewers often appreciate genuine struggle and reflection.
  Stories where you have evolved thinking are stronger than perfect ones.
■ MISSING THE 'WHY GOOGLE' THREAD
  Mission alignment matters at Google more than many companies.
  Have a real reason beyond pay/prestige.
Google-Specific for Staff+ Data Engineers
"Design a system to..." (very common; even in behavioral rounds)
  → They want to see your design thinking process
  → Walk through requirements, alternatives, trade-offs
  → Acknowledge scaling implications
"How would you measure success?"
  → SLIs, business metrics, indirect signals
  → Time horizons (immediate, 6-month, 2-year)
"How would your system fail?"
  → Failure modes
  → Mitigation
  → Detection
```

```
"What would you do if leadership disagreed with your design?"
  → Influence narrative
  → Persuasion via data
  → Disagree-and-commit fluency
■ PRINCIPAL MINDSET: Google's bar at staff+ is layered: behavioral signals + technical design + Googleyness,
evaluated holistically. The candidate who shines on one and fails on another may still be rejected. Prepare all
three dimensions; treat the behavioral as testing your JUDGMENT (not just your stories), the technical as testing
your DEPTH (not just your knowledge), and Googleyness as testing your self-awareness.
```

Supplementary — Coding, FAANG, ML, State of Stack

F.5 Story Adaptation Matrix Same Story, Three Framings Take a single STAR story and frame it three ways. Below is a working example: a story about leading a Hadoop-to-lakehouse migration. Amazon Framing 'I drove a 14-month migration from Hadoop to Iceberg-based lakehouse. DIVE DEEP: I started by deeply analyzing our existing platform — 800 DAGs, 50 PB of data, 30 production pipelines. Built a spreadsheet tracking failure rates, latency, and ownership for every system. Found that 60% of engineer time was spent firefighting; documented specific failure modes. OWNERSHIP: I owned the technical strategy and the migration plan end- to-end. Made the call to go Iceberg over Delta because we needed multi-engine support — Snowflake, BigQuery, and Spark all needed to read the same data. BIAS FOR ACTION: Despite incomplete information, I committed to the approach and proposed a 4-month POC to test feasibility. The POC validated 80% of assumptions and surfaced critical issues we hadn't foreseen — for example, catalog HA being more important than expected. DELIVER RESULTS: 14 months later: 5 PB migrated, 30 pipelines moved, $1.5M annual storage savings, 40% reduction in firefighting time. Two ML teams unblocked because they could now query the lake directly. Looking back, I'd start the catalog HA discussion 6 months earlier; late-stage rework cost us 2 months.' Meta Framing 'Led the migration of our analytics platform from Hadoop to Iceberg- based lakehouse. THE PROBLEM: We were burning $5M/year on Hadoop ops, with 30% pipeline failure rate and engineering velocity stuck. Two ML teams were blocked. WHAT I DID: I shipped a POC in 4 weeks — a single Spark job writing to Iceberg. That gave us confidence to commit. Then I broke the migration into 8 phases, each shippable independently, with reconciliation gates. THE PACE: Rather than the consultant's recommended 18-month sequential plan, I argued for parallel phases — different teams owning different pipelines, with shared infrastructure. We compressed to 14 months. IMPACT: $1.5M annual storage savings (35% of platform cost). 40% reduction in firefighting. Unblocked 2 ML teams; one shipped a recommendation system 6 months ahead of schedule. WHAT I'D DO DIFFERENTLY: Start the catalog HA work earlier. We rushed it in the last 3 months because we underestimated the complexity, and that crunch was avoidable.' Google Framing 'I led a multi-year platform migration from Hadoop to a lakehouse architecture. The most interesting part was the design judgment. THE CONTEXT: 50 PB Hadoop cluster, 800 jobs, 30% failure rate. The obvious answer was a cloud warehouse — Snowflake or BigQuery. The complicated answer was a lakehouse. THE TRADE-OFF: I built a decision matrix across 7 axes: cost,

Supplementary — Coding, FAANG, ML, State of Stack

performance, multi-engine support, lock-in, operational complexity, team familiarity, future ML support. Cloud warehouses won on operational simplicity. Lakehouse won on multi-engine and cost-at- scale. The deciding factor: our ML teams needed direct access to data without warehouse compute. That tipped me to lakehouse. THE PROCESS: I presented the decision to engineering leadership with explicit trade-offs. Two senior engineers pushed back hard, advocating for Snowflake-only. I steelmanned their position, then identified the specific scenario where it would fail (ML feature engineering at scale). They agreed to a phased commitment. CROSS-FUNCTIONAL: I worked closely with the security team (catalog governance), the cost team (FinOps), and the ML platform team (their needs were the deciding factor). The collaboration with the ML team was the most rewarding — we co-designed the feature store layer. REFLECTION: I underestimated the catalog operational complexity. If I did this again, I'd start a dedicated catalog team 6 months earlier. That's the kind of decision I now make more readily.' What Each Framing Emphasizes Element Amazon Meta Google Opening LP language explicit Problem + metric Context + decision space Process LP-mapped behaviors Speed + iteration Rigorous reasoning Trade-offs Implicit in actions Acknowledged briefly Explicit, multi-axis Outcome Numbers + LP closure Metrics + impact Outcome + reflection Reflection Brief; "differently" Brief; honest Detailed; thoughtful ■ PRINCIPAL MINDSET: Don't memorize different versions of every story. Internalize the three framing styles instead. Then adapt naturally based on the company. The underlying STORY is the same; the EMPHASIS shifts.

Supplementary — Coding, FAANG, ML, State of Stack

## Chapter G — ML for Data Engineers

```
G.1 The Data Engineer's Role in ML
Where Data Engineering Meets ML
DATA SCIENTISTS / ML ENGINEERS own:
  Model selection, feature engineering, training, evaluation, monitoring
DATA ENGINEERS own:
  Feature store infrastructure
  Online/offline data parity
  Pipeline reliability for ML workloads
  Training data freshness and lineage
  Model serving infrastructure (sometimes)
  ML pipeline observability
OVERLAP:
  Feature engineering pipelines (often joint)
  ML metric instrumentation (joint)
  Model deployment pipelines (joint)
```

THE STAFF DATA ENGINEER WORKING ON ML PLATFORMS: Builds the infrastructure that lets ML teams move fast. Doesn't necessarily build models themselves. Knows enough about modeling to architect around it. Why This Matters for Interviews Many staff+ data engineering roles touch ML platforms. Even pure data platform roles get asked 'how does your platform support ML?'. The depth needed isn't statistical modeling — it's the infrastructure and pipelines that make ML teams productive.

Supplementary — Coding, FAANG, ML, State of Stack

```
G.2 Feature Stores — The Core Concept
What a Feature Store Solves
THE PROBLEM IT SOLVES:
  Without a feature store:
    Team A defines 'user_30day_avg_spend' for their model.
    Team B redefines 'user_30day_avg_spend' for theirs.
    Subtle differences (one uses 30 calendar days, other 30 events).
    Models disagree; debugging is painful.
    Online serving recomputes features differently than training.
  With a feature store:
    Define 'user_30day_avg_spend' ONCE.
    All teams reference the same definition.
    Same logic computes the offline (training) and online (serving) versions.
    Training-serving skew eliminated by construction.
The Architecture
■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■
■  FEATURE DEFINITIONS (code / config)                      ■
■   Python / YAML / SQL definitions                         ■
■   Version controlled; reviewed; tested                    ■
■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■
                     ■
            ■■■■■■■■■■■■■■■■■■■
            ▼                 ▼
■■■■■■■■■■■■■■■■■■■■  ■■■■■■■■■■■■■■■■■■■■
■ OFFLINE STORE     ■  ■ ONLINE STORE      ■
■ (lakehouse / DWH) ■  ■ (Redis / DynamoDB)■
■                   ■  ■                   ■
■ Training data:    ■  ■ Inference reads:  ■
■ historical;       ■  ■ latest features;  ■
■ point-in-time     ■  ■ sub-10ms lookup   ■
■ joins             ■  ■                   ■
■                   ■  ■                   ■
■ TB-scale, cheap   ■  ■ GB-scale,         ■
■                   ■  ■ expensive         ■
■■■■■■■■■■■■■■■■■■■■  ■■■■■■■■■■■■■■■■■■■■
        ▲                       ▲
        ■                       ■
        ■      MATERIALIZATION  ■
        ■     (batch or stream) ■
        ■■■■■■■■■■■■■■■■■■■■■■■■■
                    ■
            ■■■■■■■■■■■■■■■■■■
            ■ COMPUTE LAYER   ■
            ■ (Spark, Flink)  ■
            ■ Reads raw data; ■
            ■ writes both     ■
            ■ stores          ■
            ■■■■■■■■■■■■■■■■■■■
The Online/Offline Split
Aspect
Offline Store
Online Store
Purpose
Training, batch scoring
Real-time inference
Storage
Iceberg / Delta on S3
Redis, DynamoDB, Bigtable
Latency
Seconds OK
< 10 ms
Cost (per GB)
Cheap
Expensive (~100x)
```

Supplementary — Coding, FAANG, ML, State of Stack

Data Full history Latest values only Access pattern Bulk scan, joins Point lookup by key Consistency Eventually consistent OK Strong consistency Compute on top Spark, dbt, SQL Lookup only Point-in-Time Correctness (Critical) THE PROBLEM: Training: 'what did user_30day_spend look like as of 2024 15 for user X?' Naive: look up current value. WRONG — it includes data after 2024 15. This causes DATA LEAKAGE — model trained on future-knowledge features. Production accuracy way lower than evaluation. THE SOLUTION: Point-in-time joins. For each training example with timestamp T, fetch the feature value AS OF time T (not the current value). Conceptually: For each row (entity_id, label_ts, label): feature = MAX(feature_value) WHERE entity_id = X AND ts < label_ts

```
  Implemented as ASOF JOIN or temporal join.
PROPER FEATURE STORES handle this AUTOMATICALLY.
The data engineer's job is to make sure the underlying data supports it:
   Features have ingestion_timestamp columns
   No retroactive overwrites without audit trail
   Time-versioned tables (Iceberg / Delta time travel helps)
Training-Serving Skew
TRAINING-SERVING SKEW = features computed differently at training
than at serving. Causes models to perform poorly in production.
CAUSES:
  1. DIFFERENT CODE PATHS
     Training: SQL query with one definition
     Serving: Python rewriting same logic with subtle difference
  2. STALE ONLINE FEATURES
     Online store updated daily; training uses fresh data
     Model trained on minute-fresh features, served with day-old
  3. TIME DRIFT
     Feature 'days_since_last_login' computed differently
     Training: as of label time
     Serving: as of NOW (different by hours)
  4. SCHEMA EVOLUTION
     Column renamed in training pipeline; not in serving pipeline
     Or type changed; or NULL handling different
PREVENTION:
   Single feature definition (in code or config)
   Same compute engine reads + writes both stores
   Monitoring: compare distribution of features at training vs serving
   Drift alerts when distributions diverge
```

Supplementary — Coding, FAANG, ML, State of Stack

G.3 Feature Store Implementations Build vs Buy Landscape Implementation Type Best For Feast Open source Teams wanting flexibility, K8s-native Tecton Commercial Enterprises wanting managed end-to-end Hopsworks Open + commercial Strong feature engineering UX Databricks Feature Store Platform-native Databricks-centric shops Vertex AI Feature Store GCP-native GCP-heavy shops SageMaker Feature Store AWS-native AWS-heavy shops Custom (in-house) DIY Very specific needs or scale The DIY Stack (Common Pattern) LAYER 1: FEATURE DEFINITIONS Define features in Python (Feast-style) or SQL (dbt-style):

```sql
  @feature_view(entities=[user], ttl=Duration(days=7))
  def user_features(...):
    return query("SELECT ... FROM events GROUP BY user_id")
LAYER 2: STORAGE
  Offline: Iceberg / Delta table per feature view
  Online: Redis or DynamoDB; key = entity_id, value = feature values
LAYER 3: MATERIALIZATION
  Batch: Airflow → Spark → write to both stores
  Stream: Flink → write to both stores (for low-latency features)
LAYER 4: SERVING
  Online: REST/gRPC service that fetches from Redis given entity_id
  Offline: Spark UDF or SQL to join with training data + point-in-time
LAYER 5: GOVERNANCE
  Feature catalog (which features exist, owners, freshness)
  Quality (tests on feature distributions)
  Access control (who can read which features)
Common Pitfalls
■ BUILDING TOO EARLY
  Feature stores have real overhead. < 5 models, < 3 ML engineers:
  raw lakehouse + dbt is often sufficient.
■ DUAL-WRITING WITHOUT TRANSACTIONS
  Materialize to offline + online stores separately;
  failure between them = inconsistency.
  Use orchestrator with retries + idempotent writes.
■ NO POINT-IN-TIME SUPPORT
  Building a 'feature store' that just stores current values isn't enough.
  Need historical, time-versioned features for correct training.
■ NOT MONITORING FOR DRIFT
  Features change distribution over time; models degrade.
  Distribution monitoring (Evidently, Arize, WhyLabs) is essential.
```

Supplementary — Coding, FAANG, ML, State of Stack

■ ONLINE STORE TOO EXPENSIVE Redis at scale is $$$. Tier: hot features in Redis, warm in cheaper KV (DynamoDB), cold in lakehouse only. ■ MIXING REAL-TIME AND BATCH FEATURES SLOPPILY Real-time features (stream-derived): fresh, expensive Batch features (daily): stale, cheap Some features can be either; the choice affects model performance. ■ INTERVIEW ANGLE: Q: How would you design a feature store for a fraud detection model serving 10K TPS? Staff: Online: Redis cluster (or DynamoDB) for sub-10ms reads. Features keyed by user_id; ~50 features per user; TTL of 7 days. Offline: Iceberg tables on S3, partitioned by event_date, with ingest_timestamp for point-in-time joins during training. Materialization: Flink for the streaming features (recent transaction velocity); Spark batch for the slow-changing features (account age, 30-day stats). Both write to Redis (online) and Iceberg (offline). Single definition layer in Python; same code generates both writes. Monitoring: feature distribution daily; alert on drift > 2σ. Cost trade-off: $5K/month Redis for 50K active users; $500/month Iceberg for full history. Worth it for the 100x speedup on serving.

Supplementary — Coding, FAANG, ML, State of Stack

```
G.4 ML Pipelines End-to-End
The Six Pipeline Stages
1. DATA INGESTION
   Raw events → bronze → silver in lakehouse
   Streaming + batch; same pipelines as analytics platform
2. FEATURE ENGINEERING
   Silver → feature views (offline store)
   Optionally: silver → real-time features (online store)
   Owned: data engineering + ML engineers (joint)
3. TRAINING DATA PREPARATION
   Feature joins with labels via point-in-time correctness
   Output: training dataset on lakehouse
   Owned: ML engineers; data engineers provide infrastructure
4. TRAINING
   Spark / PyTorch / TensorFlow reads training dataset
   MLflow tracks experiments
   Output: model artifact
   Owned: ML engineers
5. EVALUATION + DEPLOYMENT
   Compare against baseline; canary rollout
   Promote to production registry
   Output: model serving endpoint
   Owned: ML engineers + platform team (joint)
6. MONITORING + RETRAINING
   Prediction logs → lakehouse
   Drift detection
   Retraining triggers (drift, schedule, performance)
   Owned: data engineering + ML engineers (joint)
Orchestration Patterns
WHO ORCHESTRATES WHAT?
OPTION A: AIRFLOW ORCHESTRATES EVERYTHING
  DAG: ingest → features → train → evaluate → deploy
  Pros: single tool; familiar
  Cons: Airflow not great for ML-specific (experiment tracking, etc.)
OPTION B: AIRFLOW + ML TOOL
  Airflow: data pipelines (ingest, features)
  Kubeflow / MLflow: ML pipelines (train, deploy)
  Airflow triggers Kubeflow
  Pros: best tool per layer
  Cons: two systems to operate
OPTION C: DAGSTER OR PREFECT (NEWER)
  Asset-centric; ML-friendly
  Single tool; handles data + ML
  Pros: unified
  Cons: less mature than Airflow
OPTION D: METAFLOW, ZenML, FLYTE (ML-FIRST)
  ML-pipeline-focused; treat training as code
  Pros: best dev UX for ML engineers
  Cons: limited for non-ML pipelines
STAFF-LEVEL DEFAULT:
  Most large platforms: Airflow + dedicated ML tool (MLflow + Kubeflow or similar)
  Smaller / unified: Dagster for both
The CI/CD for ML Pipelines
```

Supplementary — Coding, FAANG, ML, State of Stack

```
PIPELINE-AS-CODE:
  All ML pipelines in version control.
  PR triggers CI: lint, test, integration test on sample data.
  Merge to main triggers deploy.
ENVIRONMENTS:
  Dev: experiment freely; data scientists' playground
  Staging: production-like; full pipeline runs on shadow data
  Prod: real traffic; canary rollout
MODEL PROMOTION:
  Trained model → MLflow staging
  Validation passes → MLflow production
  Canary 1% traffic → if metrics OK → 10% → 50% → 100%
ROLLBACK:
  Bad model in production: instant rollback via registry
  Don't redeploy; just point traffic to previous version
THE STAFF SIGNAL:
  Treat ML pipelines as SOFTWARE — not as one-off notebooks.
  Same discipline: tests, versions, CI, monitoring, runbooks.
```

Supplementary — Coding, FAANG, ML, State of Stack

```
G.5 Model Serving Architectures
The Three Serving Modes
1. BATCH SCORING (Offline Inference)
   Spark / dbt job: score all eligible users overnight
   Predictions written to warehouse / lakehouse
   Read by downstream systems
```

```
   When to use: predictions can be stale (daily fresh enough)
   Examples: recommendation refresh, churn scores, lead scoring
   Latency: hours; throughput: massive
2. REAL-TIME SERVING (Online Inference)
   Model deployed as REST/gRPC endpoint
   Application calls endpoint with features; gets prediction
```

```
   When to use: low-latency required
   Examples: fraud detection, ad ranking, search ranking
   Latency: < 100 ms; throughput: thousands of QPS
3. STREAMING INFERENCE
   Flink / Spark Streaming applies model to event stream
   Predictions written to topic / lakehouse / online store
```

```
   When to use: high-throughput + low-latency + asynchronous
   Examples: anomaly detection in metrics, real-time personalization
   Latency: seconds; throughput: very high
DATA ENGINEERING ROLE:
  Provides the data infrastructure each mode needs.
  Often owns the feature serving for real-time mode.
Serving Platforms
Platform
Real-time
Batch
Strengths
SageMaker Endpoints
Yes
Yes (Batch Transform)
AWS-native, auto-scaling
Vertex AI Prediction
Yes
Yes
GCP-native, AutoML integration
Databricks Model Serving
Yes
Yes
Unified with training
Seldon Core
Yes
No
Open source, K8s
BentoML
Yes
No
Open source, framework agnostic
Triton Inference Server
Yes
No
NVIDIA, GPU-optimized
Custom FastAPI
Yes
No
Maximum control
Real-Time Serving Architecture
TYPICAL REAL-TIME ML SERVING (e.g., recommendation request):
  CLIENT REQUEST: 'recommendations for user X'
       ■
       ▼
  API GATEWAY: auth, rate limit
       ■
       ▼
  ORCHESTRATION SERVICE:
```

#### 1. Fetch user features from ONLINE FEATURE STORE (Redis): ~5ms

#### 2. Fetch candidate items (catalog cache): ~5ms

#### 3. Call MODEL ENDPOINT with (user_features, item_features): ~20ms

Supplementary — Coding, FAANG, ML, State of Stack

```
    4. Post-process ranking: ~5ms
       ■
       ▼
  MODEL ENDPOINT: model server (Triton, Seldon, etc.)
       Scoring: ~10-20 ms
       ■
       ▼
  RESPONSE: ranked list to client
```

```
  TOTAL LATENCY: ~50 ms p95
  LOGGING:
    Request features + model output → Kafka → Iceberg
    Used for: monitoring, training data, A/B test analysis
Cost Considerations
REAL-TIME SERVING IS EXPENSIVE:
  GPU endpoints: $1-10/hour per instance (NVIDIA T4 to A100)
  CPU endpoints: $0.1-1/hour
  Plus: 24/7 always-on (unless auto-scaled to zero)
OPTIMIZATIONS:
  1. AUTO-SCALING: scale-to-zero during off-hours
  2. BATCHING: collect multiple requests; serve as batch (10-50x throughput)
  3. QUANTIZATION: int8 inference; 4x faster, slightly less accurate
  4. CACHING: same inputs → cached output; common in rec systems
  5. MULTI-MODEL ENDPOINTS: many small models on one instance
BATCH IS MUCH CHEAPER:
  Spark cluster: $10/hour for nodes
  Process millions of predictions in minutes
  Trade-off: predictions are 1+ days old
```

Supplementary — Coding, FAANG, ML, State of Stack

```
G.6 ML Monitoring at Platform Level
What to Monitor
1. INPUT MONITORING (data quality)
    Feature distributions vs training baseline
    Missing values; NULL spikes
    Schema changes
    Volume anomalies
2. PREDICTION MONITORING
    Prediction distribution (output skew)
    Prediction confidence
    Decision distribution (% predicted positive, etc.)
3. PERFORMANCE MONITORING (where ground truth eventually arrives)
    Accuracy / AUC / RMSE vs training metrics
    Calibration (predicted prob vs actual rate)
    Performance by segment (fairness)
4. INFRASTRUCTURE MONITORING
    Endpoint latency
    Throughput
    Error rate
    Resource utilization
5. BUSINESS METRICS (the ultimate)
    Did the model lift the business metric?
    CTR, conversion, revenue per session
    A/B test analysis
Drift Detection
TYPES OF DRIFT:
1. FEATURE DRIFT
   Distribution of inputs changes
   Example: user demographics shift; new market segment
   Detection: KS test, PSI (Population Stability Index)
2. CONCEPT DRIFT
   Relationship between inputs and outputs changes
   Example: COVID changed everything; pre-COVID model fails
   Detection: performance degradation when ground truth arrives
3. LABEL DRIFT
   Distribution of labels changes
   Example: fraud patterns evolve
   Detection: monitor label distribution over time
TOOLS:
  Evidently: open-source; comprehensive drift suite
  WhyLabs: managed drift detection
  Arize: ML observability platform
  Fiddler: ML monitoring with explainability
The Retraining Triggers
WHEN TO RETRAIN?
OPTION A: SCHEDULE-BASED
  Retrain weekly / monthly regardless
  Pros: predictable; ensures freshness
  Cons: wasteful if model still good
OPTION B: DRIFT-BASED
  Retrain when feature drift exceeds threshold
  Pros: efficient; data-driven
  Cons: needs robust drift detection
```

Supplementary — Coding, FAANG, ML, State of Stack

```
OPTION C: PERFORMANCE-BASED
  Retrain when prediction accuracy drops
  Pros: ideal in theory
  Cons: requires ground truth lag — may be too late
OPTION D: HYBRID (recommended)
  Schedule weekly retraining
  PLUS drift-triggered emergency retraining
  PLUS performance-triggered (when GT available)
THE PIPELINE:
  Retrain trigger → fetch training data → train → evaluate →
  shadow deploy → A/B test → promote (or roll back)
■ PRINCIPAL MINDSET: ML platforms are increasingly the core of staff data engineering work. The data
engineering bar isn't 'know how to train models' — it's 'know how to build the infrastructure that makes ML teams
10x more productive'. Feature stores, training-serving parity, monitoring, and pipeline orchestration are the
staff-level concerns. Master these and you can architect ML platforms even if you've never trained a model
yourself.
```

Supplementary — Coding, FAANG, ML, State of Stack

## Chapter H — State of the Stack 2024-2026

H.1 Why This Chapter Exists The original 8 topics in this series cover frameworks, patterns, and trade-offs that are durable across years. But some specific developments post-mid-2024 are important enough — and stable enough — to call out. This chapter covers what's changed, what's emerging, and what the staff engineer should know going into 2026. ■■ WARNING: By 2027, parts of this chapter will be outdated. The point isn't memorization — it's awareness of the DIRECTION of change. Always verify current state when it matters; vendor docs and recent talks are better than any reference book.

Supplementary — Coding, FAANG, ML, State of Stack

```
H.2 The Catalog Wars
Why Catalogs Suddenly Matter
Through 2023, catalogs were a back-office concern. Hive Metastore was the de facto standard; AWS Glue
was the cloud-managed equivalent. By 2024-2026, catalogs became a strategic battleground — because the
catalog is what enables multi-engine access to lakehouse storage.
The Players (As of 2026)
APACHE POLARIS (incubated by Snowflake, donated 2024, graduated 2026)
  Open-source REST catalog for Iceberg
  Snowflake-led but cross-vendor adoption
  Multi-engine support: Spark, Flink, Trino, Snowflake, StarRocks, Dremio
  Graduated to top-level Apache project Feb 2026
  Best fit: organizations wanting an open, vendor-neutral catalog
```

```
UNITY CATALOG (Databricks, open-sourced June 2024)
  Originally Databricks-proprietary; now Apache 2.0 license
  Multi-format: Delta, Iceberg, Hudi (read-only for Iceberg)
  Stronger lineage + governance than Polaris
  Best fit: Databricks-centric shops; multi-format teams
PROJECT NESSIE (Dremio)
  Git-style: branches, commits, tags on data
  Newer than Polaris/Unity; smaller adoption
  Best fit: teams wanting experimental data workflows
APACHE GRAVITINO (formerly Datastrato)
  Multi-source metadata catalog (not just Iceberg)
  Bridges lakehouse + traditional sources
  Best fit: heterogeneous data landscapes
LAKEKEEPER
  Newer; Rust-based REST catalog
  Performance-focused
  Best fit: high-throughput catalog workloads
AWS GLUE / GCP DATA CATALOG / AZURE PURVIEW
  Cloud-native catalogs
  Multi-format support added
  Best fit: cloud-locked shops
What Changed and Why
THE 2024 INFLECTION:
  Iceberg's REST Catalog spec (since v0.14, 2022) standardized the catalog API.
  Vendors could now compete on catalog IMPLEMENTATION while preserving
  interoperability via the standard.
  This unlocked the 'catalog wars': Snowflake (Polaris), Databricks (Unity),
  Dremio (Nessie), AWS (Glue), and others all racing to be the standard.
  Result: 2025-2026 has multiple production-ready open catalogs.
  Choice depends on ecosystem + governance needs.
THE 2024-2025 LANDMARK EVENTS:
   June 2024: Databricks acquires Tabular (Iceberg creators) for $1B+
   June 2024: Snowflake announces Polaris Catalog
   June 2024: Databricks open-sources Unity Catalog (Apache 2.0)
   2024-2025: Format wars de-escalate; both major vendors pledge Iceberg + Delta interop
   Feb 2026: Apache Polaris graduates to top-level Apache project
THE PRACTICAL IMPACT:
  Vendor lock-in at storage layer is dramatically reduced.
  Same Iceberg data accessible from Snowflake, Databricks, BigQuery, Spark, etc.
  Choice of compute engine becomes a more flexible, less strategic decision.
```

Supplementary — Coding, FAANG, ML, State of Stack

Which Catalog When (2026 Default) IF YOU'RE PRIMARILY DATABRICKS: Unity Catalog (native, integrated, lineage, multi-format). IF YOU'RE MULTI-ENGINE BUT PREFER OPEN STANDARDS: Apache Polaris (Snowflake-led but vendor-neutral; graduated TLP). IF YOU NEED GIT-STYLE BRANCHING: Project Nessie (data versioning workflows; experimentation). IF YOU'RE AWS-NATIVE AND HAPPY: AWS Glue + Lake Formation (cloud-managed, well-integrated). IF YOU HAVE HETEROGENEOUS METADATA (lakehouse + relational + APIs): Gravitino or DataHub (broader catalog scope). STAFF SIGNAL: The catalog choice is increasingly more important than the warehouse choice. 10-year decision; affects governance, lineage, multi-engine support. ■ PRINCIPAL MINDSET: The catalog layer in 2026 looks like the database layer in 2010 — multiple competing implementations, an open standard binding them, and a slow consolidation toward 2-3 winners. At staff+, you're expected to have a defensible opinion on catalog choice and to architect with the open-format thesis (Iceberg + open catalog) as the default. Vendor lock-in is increasingly an active choice rather than a default.

Supplementary — Coding, FAANG, ML, State of Stack

```
H.3 The Iceberg-Delta Convergence
The Setup (Pre-2024)
ICEBERG vs DELTA was a real war:
   Iceberg: Netflix → Apache; cross-vendor (Snowflake, AWS, etc.)
   Delta:   Databricks → Linux Foundation; Databricks-led
   Hudi:    Uber → Apache; less adoption
EACH HAD ADVANTAGES:
  Iceberg: cross-engine; cleaner specification; production-grade ACID
  Delta:   tightly integrated with Databricks; mature; UniForm read support
  Hudi:    record-level deletes; streaming-first; specific niche
TEAMS HAD TO CHOOSE; migration was painful.
The June 2024 Inflection
DATABRICKS ACQUIRED TABULAR ($1B+, June 2024)
  Tabular was founded by the original Iceberg creators (Ryan Blue, et al.)
  Acquired during Snowflake's annual summit — competitive theater
```

```
  Databricks now had ownership/influence over BOTH formats:
     Delta Lake (created by Databricks)
     Iceberg (most influential committers via Tabular)
  Stated goal: format compatibility, not winner-takes-all.
  Long-term: 'evolve toward a single, open, common standard'
  Delta UniForm: Delta tables with Iceberg metadata,
                  readable by Iceberg-only engines.
Where We Are in 2026
THE PRACTICAL STATE:
   Iceberg has won the multi-engine support battle
    Snowflake, BigQuery, Databricks (via UniForm) all read Iceberg
   Delta UniForm enables Delta-as-Iceberg for cross-engine access
   Hudi remains for specific high-write workloads
   The format choice matters less than the catalog + engine choice
THE STAFF ANSWER (2026):
  'Iceberg if multi-engine is a hard requirement.
   Delta if Databricks-centric AND multi-engine is unlikely.
   Both if hedging; use UniForm for compatibility.
   Hudi for record-level deletes at scale (specific niche).'
  The format war is largely DE-ESCALATED.
  Choose based on team familiarity + ecosystem fit, not theology.
Spec Evolution Highlights
ICEBERG V2 (mainstream): row-level updates via positional deletes
ICEBERG V3 (introduced 2024-2025): deletion vectors, variant data type,
                                    geometry types
DELTA 3.0+: deletion vectors, UniForm, liquid clustering
DELTA 4.0 (2025): improvements in MERGE performance, better Iceberg interop
WHAT THIS MEANS:
  Both formats are evolving in parallel.
  Feature parity is closer than ever.
  Catalog + engine choice matters more than format choice.
■ PRODUCTION SCENARIO: A team in 2024 spent 6 months evaluating Iceberg vs Delta, comparing feature
matrices in detail. By the time they decided (Iceberg), Databricks had announced UniForm + acquired Tabular,
making the choice nearly moot. Lesson: at moments of rapid convergence, premature optimization between
similar options wastes time. Pick based on ecosystem fit; defer detailed comparison until you actually hit a
```

Supplementary — Coding, FAANG, ML, State of Stack

limitation.

Supplementary — Coding, FAANG, ML, State of Stack

```
H.4 The Rise of DuckDB and DuckLake
The DuckDB Story
DUCKDB: in-process analytical SQL database (like SQLite for OLAP).
   Created 2019; matured rapidly 2022-2026
   Single-node; embedded; columnar
   Reads Parquet, CSV, JSON, Iceberg, Delta directly
   ~20 MB binary; zero dependencies; installs in seconds
   Benchmark-leading on single-node workloads
WHY IT MATTERS:
  Modern hardware (single instance with 256 GB - 2 TB RAM) handles
  workloads that previously REQUIRED distributed systems.
  'Big data is dead' for most P99 queries — workloads fit on one machine.
USE CASES:
   Local development and prototyping
   Notebook analytics (replaces pandas for big-ish data)
   Embedded analytics in applications
   Cheap serverless analytics (MotherDuck, etc.)
   As compute engine over Iceberg/Delta tables
DuckLake (May 2025, v1.0 April 2026)
DUCKLAKE: lakehouse format from DuckDB Labs.
   Stores METADATA in a SQL database (Postgres, MySQL, SQLite, DuckDB)
   Stores DATA as Parquet files
   Released as DuckLake manifesto in May 2025
   Production-ready v1.0 reached April 2026
THE INSIGHT:
  Iceberg / Delta store metadata as files in object storage.
  This creates inherent issues with small changes (file overhead) and
  concurrency (eventual consistency).
  DuckLake puts metadata in a REAL database — which already solves
  transactions, concurrency, and consistency.
  Result: simpler, faster for small changes, easier to operate at small-medium scale.
LIMITATIONS:
   Newer than Iceberg/Delta; smaller ecosystem
   Requires a database for metadata (additional moving part)
   Less proven at PB scale (yet)
   Cross-engine support more limited
WHEN TO CONSIDER DUCKLAKE:
   Single-node or small-cluster workloads
   Heavy small-change workloads (record-level updates)
   Team wants simpler operations than Iceberg/Delta
   Not at PB scale yet
The Single-Node Renaissance
THE PATTERN:
  Through 2020s, single instances grew to:
     192 vCPUs (AWS m7a.96xlarge)
     24 TB RAM (high-memory instances)
     Sub-microsecond NVMe storage
```

Most analytical workloads (P99) fit in 256 GB.

```
  This means:
    Distributed systems' coordination overhead now dominates
    Single-node columnar engines (DuckDB, ClickHouse) often FASTER
THE SHIFT:
  Pre-2020: 'Use Spark for anything > 100 GB'
```

Supplementary — Coding, FAANG, ML, State of Stack

```
  2026:     'Use Spark only if you genuinely need >1 TB or distributed reasons'
STAFF SIGNAL:
  Know when distributed is overkill.
  DuckDB or single-node Polars often beat Spark on TB-scale workloads.
  Many ETL jobs are smaller than the team's tooling assumes.
```

Supplementary — Coding, FAANG, ML, State of Stack

H.5 dbt Fusion and the Rust Rewrite The Announcement DBT FUSION (announced May 28, 2025; public beta): Complete rewrite of dbt's core engine from Python to RUST.

```
  Key features:
     Real-time SQL validation (in IDE; pre-compile errors)
     State-aware orchestration (build only what changed in code OR data)
     Native SQL parsing (understands SQL semantically, not just templates)
     Faster compile times (10x+ for large projects)
     New VSCode extension as primary dev environment
     Hybrid OSS + commercial license model
WHY IT MATTERS:
  Python's compile time was a scaling pain for large dbt projects (10K+ models).
  Fusion compiles in seconds where Python took minutes.
  State-aware orchestration: ~40% model reuse, 30% compute cost savings reported.
The Licensing Shift
dbt CORE remained Apache 2.0 (open source).
dbt FUSION introduces a hybrid licensing model:
   Free source-available version for limited use
   Commercial license for production use beyond limits
   Driven by dbt Labs' business model evolution
CONTROVERSY:
  Some community members concerned about the open-source ethos.
  Counter: similar to Elastic, Confluent, Hashicorp licensing pivots.
```

```
PRACTICAL IMPACT:
   dbt Core continues; many shops will stay on it
   dbt Cloud users get Fusion automatically
   Self-hosted Fusion users navigate the licensing
STAFF DECISION:
   If on dbt Cloud already: get Fusion benefits for free
   If self-hosted dbt Core: continue, or migrate to Cloud, or accept license
   Watch for community forks (SQLMesh, Datafold's competing tools)
State-Aware Orchestration
TRADITIONAL DBT:
  'dbt run' rebuilds all selected models, regardless of whether data changed.
  Cost: full compute for every run.
FUSION:
  Tracks which columns are used where + source data freshness.
  Rebuilds ONLY models where input data has actually changed.
  Reuses cached results when inputs are unchanged.
  Result: in real customer report (EQT):
    40% of models reused per run
    30% reduction in warehouse compute cost
WHY THIS IS A BIG DEAL:
  Large dbt projects often re-run thousands of models on schedules
  where most upstream data hasn't actually changed.
  State-aware orchestration is the biggest dbt optimization in years.
Competitors / Alternatives Emerging
Tool
Approach
Best For
SQLMesh
Stateful Python; semantic SQL parsing
Teams wanting alternatives to dbt
```

Supplementary — Coding, FAANG, ML, State of Stack

Dataform Google-acquired; integrated with BQ BigQuery-heavy shops Coalesce Visual GUI for transformation Less-engineering-heavy teams Cube Semantic layer + caching Embedded BI; metric APIs dbt Fusion Next-gen dbt; Rust-based dbt users at scale

Supplementary — Coding, FAANG, ML, State of Stack

```
H.6 The Warehouse Landscape Update
Snowflake (2024-2026 Shifts)
KEY ANNOUNCEMENTS:
   Polaris Catalog (June 2024) — donated to Apache
   Iceberg tables: dramatically improved (read + write parity with native FDN)
   Cortex AI: in-warehouse LLM functions
   Hybrid tables: OLTP-style in Snowflake (limited)
   Streaming Snowpipe: sub-second ingestion (general availability)
THE STRATEGIC POSITIONING:
  Embracing open formats — external Iceberg with Snowflake compute.
  Reducing vendor lock-in concern; emphasizing the compute engine + UX.
  Cortex AI for ML/AI directly on warehouse data.
PRACTICAL IMPLICATIONS:
  Snowflake is now lakehouse-friendly.
  Iceberg as primary storage with Snowflake as one of N readers is viable.
  Best UX in the warehouse space; broader open-format support.
Databricks (2024-2026 Shifts)
KEY ANNOUNCEMENTS:
   Tabular acquisition (June 2024)
   Unity Catalog open-sourced (June 2024, Apache 2.0)
   Delta UniForm: write Delta, read Delta + Iceberg simultaneously
   Liquid Clustering: replaces Z-order in Delta
   Predictive Optimization: auto-OPTIMIZE/VACUUM
   Mosaic AI: ML platform expansion
   Lakehouse Federation: query external catalogs from Databricks SQL
THE STRATEGIC POSITIONING:
  Lakehouse-native; ML + data unified.
  Format-agnostic (via UniForm and Iceberg).
  Push into AI: foundation models, RAG, vector search.
PRACTICAL IMPLICATIONS:
  Databricks SQL increasingly competitive with Snowflake for pure BI.
  Best fit for shops wanting unified ML + data platform.
  Strong governance via Unity Catalog (now open).
BigQuery (2024-2026 Shifts)
KEY ANNOUNCEMENTS:
   BigQuery managed Iceberg tables (writes externally; queries via BQ)
   BigQuery Studio: unified analytics + ML in one UI
   Gemini integration: AI for SQL generation, data exploration
   BigLake: query external data (Iceberg, Delta) from BQ
   Continuous queries: streaming queries against ingested data
   Editions pricing model (Standard / Enterprise / Enterprise Plus, 2023+)
THE STRATEGIC POSITIONING:
  Serverless analytics + Google AI capabilities.
  Tight integration with GCP ecosystem.
  Open formats supported but BQ-managed remains primary.
PRACTICAL IMPLICATIONS:
  Still the king of all-GCP, sporadic-workload shops.
  Less compelling for multi-cloud or open-format-first.
Redshift (2024-2026 Shifts)
KEY ANNOUNCEMENTS:
   Zero-ETL integrations: Aurora, DynamoDB, RDS automatic sync
   Redshift Serverless: maturing; growing adoption
   Iceberg + Lake Formation integration
   Performance improvements (RA3 nodes evolved)
```

Supplementary — Coding, FAANG, ML, State of Stack

```
THE STRATEGIC POSITIONING:
  AWS-native; zero-ETL to reduce data movement.
  Catching up on serverless and open formats; less leading on innovation.
PRACTICAL IMPLICATIONS:
  Best fit for deep-AWS shops with operational DB integration needs.
  Less compelling for greenfield where Snowflake/Databricks lead.
The Convergence Trend
ALL FOUR MAJOR WAREHOUSES (2026):
   Support external Iceberg (read at minimum; some write)
   Have streaming ingest paths
   Offer ML / AI in-warehouse features
   Have semantic layer or SQL-on-everything capability
   Are competitive on cost for typical workloads
DIFFERENCES NOW ABOUT:
   Cloud ecosystem
   Team preference
   Specific feature edge cases
   Cost shape (steady vs bursty)
   Vendor relationships
THE STAFF SIGNAL:
  Warehouses have converged on capabilities.
  Pick based on ecosystem fit and team preference, not feature checklists.
  All four can do most workloads competently.
```

Supplementary — Coding, FAANG, ML, State of Stack

```sql
H.7 The Streaming Stack Evolution
Kafka in 2026
KEY DEVELOPMENTS:
   KRaft (Kafka Raft) replaced ZooKeeper (Kafka 3.5+, GA 2023)
    Simpler operations; one less system to manage
   Tiered storage GA (Kafka 3.6+)
    Cold data on S3; reduces cluster storage needs significantly
   Confluent Cloud serverless options matured
   Iceberg sink connectors (Kafka Connect) now production-grade
STRATEGIC SHIFTS:
  Kafka is no longer 'the choice' — alternatives:
     Redpanda: Kafka API; Rust-based; simpler ops
     Pulsar: tiered storage from day one; geo-replication
     WarpStream: Kafka API over S3 directly (no brokers)
THE STAFF DECISION:
  Kafka (managed: Confluent Cloud / MSK) remains the default
  Redpanda for ops-light shops at smaller scale
  WarpStream for cost-sensitive at PB scale (S3-direct)
  Pulsar for multi-tenant + geo-replication needs
Flink in 2026
KEY DEVELOPMENTS:
   Flink SQL massively improved; near parity with batch SQL
   Flink CDC connectors mature (Debezium-style without Kafka)
   Flink + Iceberg = first-class streaming lakehouse
   Flink Table Store / Paimon: stream-native lakehouse format
THE STREAMING LAKEHOUSE PATTERN:
  Flink reads Kafka or directly CDC
  Writes to Iceberg with 30-second to minute-level commits
  Downstream consumers: Spark batch, dbt, BI tools
  Real-time alternative: Flink + ClickHouse for sub-second
Apache Paimon (formerly Flink Table Store)
PAIMON: streaming-first lakehouse format.
  Built for high-write, mutable workloads.
  CDC-friendly; record-level updates efficient.
  Originated at Flink; now Apache top-level (2024).
WHEN PAIMON FITS:
   High-frequency updates / deletes
   CDC into lakehouse at scale
   Need streaming + batch with same storage
VS ICEBERG / DELTA / HUDI:
  Paimon: streaming-first; mutable; cheaper for high-update
  Iceberg: read-heavy; analytical; multi-engine
  Hudi: also streaming-first; similar niche to Paimon
  Delta: batch + streaming; Databricks-aligned
STAFF DECISION (2026):
  Default: Iceberg for analytical lakehouse
  High-update + Flink-heavy: consider Paimon
  Databricks shop + mutability: Delta with deletion vectors
■ PRINCIPAL MINDSET: Streaming systems have matured significantly. The infrastructure is no longer the
differentiator; the operational expertise is. At staff+, you should defend WHY you chose Kafka vs Redpanda vs
WarpStream, and WHY you chose Flink vs Spark Streaming, and what the streaming sink should be (Iceberg vs
Paimon vs ClickHouse). The answers depend on workload.
```

Supplementary — Coding, FAANG, ML, State of Stack

```
H.8 AI/LLM Impact on Data Engineering
Where LLMs Touched Data Engineering
1. SQL GENERATION
   GitHub Copilot, dbt Copilot, Snowflake Cortex Analyst
   Natural language → SQL
   Quality: good for simple queries; struggles with complex business logic
2. PIPELINE DEVELOPMENT ASSISTANCE
   Cursor, Copilot for dbt models, Airflow DAGs, PySpark
   Boilerplate generation; refactoring assistance
3. DATA EXPLORATION
   'Ask your data' interfaces (Cortex Analyst, Hex, Vertex)
   Translate questions to queries
4. SCHEMA INFERENCE / MAPPING
   LLMs infer schemas from samples
   Map source schemas to target
5. DOCUMENTATION GENERATION
   dbt model descriptions; column documentation
   Lineage annotations
6. CODE REVIEW + LINTING
   SQL anti-pattern detection
   Performance issue flagging
What Hasn't Changed
DESPITE THE LLM HYPE, AT STAFF LEVEL:
   Architecture decisions still require human judgment
   Trade-off analysis still requires understanding context
   Operational decisions still require organizational awareness
   Business requirements still need humans translating
LLMs ARE TOOLS, NOT REPLACEMENTS for:
   Designing data platforms
   Negotiating SLAs with stakeholders
   Debugging production incidents
   Mentoring junior engineers
THE STAFF MINDSET:
  Use LLMs to accelerate the boilerplate work.
  Maintain judgment + architecture skills as the core differentiator.
  Don't outsource thinking to the model.
Vector Databases & RAG Infrastructure
NEW WORKLOAD TYPE:
  Vector embeddings; semantic search; RAG (retrieval-augmented generation)
INFRASTRUCTURE PATTERNS:
  Vector DB: Pinecone, Weaviate, Qdrant, pgvector, Snowflake vector functions
  Embedding generation: typically GPU jobs or external API calls
  Storage: vectors stored in vector DB + raw text in lakehouse / warehouse
  Pipelines: ingest → chunk → embed → index → query
DATA ENGINEERING ROLE:
  Pipeline for embedding generation (batch + streaming)
  Vector DB infrastructure (operations, capacity)
  RAG orchestration (retrieve + LLM call + response)
  Monitoring (embedding drift, retrieval quality)
STAFF QUESTIONS WORTH PREPARING:
   'How would you architect a RAG system over our corpus?'
   'How do you keep embeddings fresh as source data changes?'
   'How do you measure RAG quality?'
```

Supplementary — Coding, FAANG, ML, State of Stack

```
AI in the Warehouse
ALL MAJOR WAREHOUSES added AI/LLM functions:
  Snowflake Cortex: COMPLETE, TRANSLATE, SUMMARIZE functions; Cortex Analyst
  BigQuery ML: GEMINI, EMBED functions; ML.GENERATE_TEXT
  Databricks Mosaic AI: foundation models; vector search; RAG
  Redshift ML: SageMaker integration
WHAT THESE ENABLE:
   Inline summarization of text columns
   Translation, classification, sentiment without external services
   Vector search within the warehouse
   RAG patterns without leaving the warehouse
STAFF AWARENESS:
  These are convenient but expensive at scale.
  Cost per LLM call adds up; consider when warehouse-native is right vs
  building a dedicated inference pipeline.
```

Supplementary — Coding, FAANG, ML, State of Stack

```
H.9 What Looks Wobbly Going into 2027
Watch Carefully
1. CATALOG CONSOLIDATION
   Polaris vs Unity Catalog vs others — likely consolidation to 2-3 by 2027
   Bet: Polaris + Unity Catalog co-exist; smaller ones fade or merge
2. dbt LICENSING / COMMUNITY
   Fusion's licensing pivot creates space for alternatives
   SQLMesh, custom solutions may gain ground
   Watch dbt Labs' commercial trajectory + community sentiment
3. SINGLE-NODE RESURGENCE
   DuckDB/Polars momentum suggests rethinking distributed defaults
   Smaller teams adopting non-Spark engines
   Spark not going away; but less default
4. WAREHOUSE-LAKEHOUSE CONVERGENCE
   By 2027: warehouses likely 'compute on open formats' fully
   Difference becomes UX + pricing, not architecture
   Storage layer wins; compute layer becomes commoditized
5. LLM-DRIVEN DATA WORKFLOWS
   How much of analytics will be natural-language-first?
   Unclear; high upside but quality issues persist
   Watch enterprise adoption rates
6. STREAMING-AS-DEFAULT
   Will most platforms be streaming-first by 2028?
   Watch: Iceberg + Flink + real-time serving stacks
   Could displace daily batch as the norm
7. AI/ML INFRASTRUCTURE CONSOLIDATION
   Many ML platforms; consolidation likely
   Watch Databricks vs SageMaker vs Vertex
What Will Probably NOT Change
DURABLE PATTERNS THROUGH 2027+:
   Open table formats winning at storage layer
   SQL as the universal interface
   Idempotency + at-least-once semantics
   Cost engineering as a discipline
   Layered architectures (bronze/silver/gold)
   The need for governance + lineage
   Multi-engine over single-vendor lock-in
   Staff+ behavioral signals (influence, judgment, scope)
THESE ARE THE BETS WORTH MAKING:
  When in doubt, design for open formats.
  When in doubt, design for idempotency.
  When in doubt, design for cost visibility.
  When in doubt, design for multi-engine future.
These principles age well; specific tool choices age poorly.
■ PRINCIPAL MINDSET: Staying current is part of staff+ work. The specific tools will change; the underlying
frameworks won't. Invest in understanding the WHY behind every architectural trend (storage/compute
separation, open formats, streaming convergence) rather than memorizing today's leaders. The engineers who
do this stay relevant; the engineers who memorize vendor-specific details get blindsided by the next pivot.
```

Supplementary — Coding, FAANG, ML, State of Stack

```
H.10 2026 Quick Reference
Defaults for 2026 Greenfield Projects
STORAGE: Iceberg on S3/GCS/Azure
  Multi-engine future-proof; broad vendor support
CATALOG: Apache Polaris OR Unity Catalog (depending on ecosystem)
  Polaris if multi-engine focus
  Unity if Databricks-centric
WAREHOUSE COMPUTE: depends on workload
  Snowflake: multi-cloud, analyst-heavy, mixed
  BigQuery: all-GCP, sporadic
  Databricks SQL: lakehouse-native, ML-heavy
  Redshift: AWS-deep, steady
STREAMING:
  Kafka (managed: MSK / Confluent Cloud) for default
  Redpanda for ops-light
  Flink for stream processing
  ClickHouse for real-time serving
ORCHESTRATION:
  Airflow (default; mature)
  Dagster (asset-centric; growing)
  dbt Cloud (dbt-only pipelines)
MODELING:
  dbt (or Fusion if scale demands)
  Semantic layer: dbt MetricFlow or Cube
ML PLATFORM:
  Feature store: Feast (open) or Tecton (managed)
  Training: Databricks or SageMaker
  Serving: SageMaker, Vertex, or Databricks Serving
  Monitoring: Evidently, WhyLabs, or Arize
GOVERNANCE:
  Catalog: Polaris/Unity (table-level)
  Lineage: OpenLineage + Marquez or DataHub
  Quality: dbt tests + Elementary/Monte Carlo
```

OBSERVABILITY: Infra: Datadog Data: Elementary, Monte Carlo, Bigeye Cost: SELECT.dev, Sigil, custom Things Less Obvious Today Than In 2023 2023 Mental Model 2026 Mental Model Warehouse vs lakehouse choice Layered (lakehouse + warehouse compute) Iceberg vs Delta is a major decision Format converged; catalog matters more Kafka + ZooKeeper Kafka + KRaft (no ZooKeeper) dbt Core dominant dbt Fusion + alternatives emerging Spark for anything > 100 GB Single-node + DuckDB for many TB-scale jobs Hive Metastore for catalog Polaris / Unity Catalog ecosystem Heavy migration to move data Open formats: data stays, engines change Manual capacity planning Auto-scaling, serverless options mature

Supplementary — Coding, FAANG, ML, State of Stack

```
ML platform = separate ecosystem
ML platform on shared lakehouse
Vendor lock-in expected
Open formats enable optionality
Final Thoughts on Staying Current
WHERE TO LEARN (durable):
   Vendor engineering blogs (Snowflake, Databricks, Confluent, dbt)
   Conference talks (re:Invent, Snowflake Summit, Data+AI Summit,
    Coalesce, Apache Iceberg Summit, Kafka Summit)
   Practitioner-led publications (Substack: Maxime Beauchemin,
    Tristan Handy, Jamin Ball)
   OSS project release notes (read them as they ship)
   Hands-on experimentation (set up new tools yourself)
WHERE NOT TO RELY:
   Static reference books (like this one) for current state
   Generic 'modern data stack' guides (often vendor-pitched)
   Year-old blog posts (the field moves fast)
STAFF DISCIPLINE:
  Build a habit of reading 1-2 hours/week of current content.
  Experiment with at least one new tool per quarter.
  Connect with practitioners outside your company.
```

This is the difference between staff engineers who lead vs follow. — End of Supplementary Additions — The supplementary additions close out the Principal Engineer Notes series. Total combined edition with supplements: ~620 pages.


---

[← Back to main](./README.md)
