# Topic 20 — Row vs Columnar Storage

> Standalone deep-dive · The foundational data layout decision underneath every analytical system

[← Back to main](./README.md)

---

## How to use this document

This chapter covers the single most important architectural concept in data engineering: **how data is physically laid out on disk and in memory**, and why that choice shapes everything from query performance to compression ratios to CPU utilization. Every chapter follows the four lenses: **Concept · Mechanics · Trade-offs · Interview**.

> **🧠 Principal Mindset:** The row-vs-columnar distinction is the quiet foundation underneath every other topic in this series. Why do warehouses crush OLTP databases at analytics? Because columnar. Why does Parquet compress 10x better than CSV? Because columnar. Why can DuckDB beat Spark on single-node analytics? Because columnar + vectorized. Staff engineers who deeply understand the physical layout can predict performance characteristics of systems they've never touched. Senior engineers memorize that "warehouses are columnar"; staff engineers can explain why that implies 90% of the properties they care about.

## Table of Contents

**PART I — FOUNDATIONS**
- 1. The Fundamental Question: Row or Column?
- 2. Where Row Storage Comes From (OLTP History)
- 3. Where Columnar Storage Comes From (OLAP History)
- 4. Why the Choice Matters So Much

**PART II — PHYSICAL LAYOUT**
- 5. Row Layout: How Rows Live on Pages
- 6. Column Layout: Vectors, Blocks, and Row Groups
- 7. The Hybrid: PAX and the Middle Ground
- 8. Memory Representation: Arrow and In-Memory Formats

**PART III — COMPRESSION**
- 9. Why Columnar Compresses So Much Better
- 10. Encoding Schemes (RLE, Dictionary, Delta, Bit-Packing, FSST)
- 11. Compression Ratios in Practice
- 12. Compression's Cost (CPU vs I/O Trade-off)

**PART IV — EXECUTION**
- 13. Row-at-a-Time (Volcano) vs Vectorized
- 14. SIMD and the CPU Angle
- 15. Query Optimization: Projection and Predicate Pushdown
- 16. Late Materialization and Zone Maps

**PART V — WORKLOADS**
- 17. OLTP: Why Row Still Wins
- 18. OLAP: Why Columnar Dominates
- 19. HTAP: The Hybrid Workload Problem
- 20. Mixed-Workload Systems (TiDB, SingleStore, PlanetScale, Databricks Hybrid)

**PART VI — FILE FORMATS**
- 21. Parquet Deep Dive
- 22. ORC (Optimized Row Columnar — misleading name)
- 23. Avro (Row-Oriented for Streaming)
- 24. Arrow (In-Memory Columnar)
- 25. Format Decision Guide

**PART VII — DATABASE SYSTEMS**
- 26. Row-Oriented Databases (Postgres, MySQL, Oracle)
- 27. Columnar Databases (Snowflake, BigQuery, Redshift, ClickHouse, DuckDB)
- 28. The Hybrid Approach (SQL Server Columnstore, Postgres cstore_fdw)

**PART VIII — INTERVIEW PRACTICE**
- 29. The Staff-Level Row/Column Interview
- 30. War Stories
- 31. Cheat Sheet &amp; Mental Models

---

## PART I — FOUNDATIONS

### 1. The Fundamental Question: Row or Column?

#### 1.1 The One-Picture Explanation

```
A TABLE conceptually:
   
    id   | name    | age | city      | salary
    -----+---------+-----+-----------+--------
    1    | Alice   | 30  | NYC       | 85000
    2    | Bob     | 25  | SF        | 92000
    3    | Carol   | 35  | LA        | 78000
    4    | Dave    | 28  | NYC       | 95000

ROW LAYOUT on disk:
   [1, Alice, 30, NYC, 85000] [2, Bob, 25, SF, 92000] 
   [3, Carol, 35, LA, 78000]  [4, Dave, 28, NYC, 95000]

   All fields of row 1 together, then all fields of row 2, etc.
   Natural when you access whole rows (transactional lookups).

COLUMN LAYOUT on disk:
   id:     [1, 2, 3, 4]
   name:   [Alice, Bob, Carol, Dave]
   age:    [30, 25, 35, 28]
   city:   [NYC, SF, LA, NYC]
   salary: [85000, 92000, 78000, 95000]
   
   All values of a column together.
   Natural when you access some columns across many rows (analytics).
```

#### 1.2 The Core Insight

```
Analytical query:
   SELECT AVG(salary) FROM employees WHERE age > 25;

ROW STORAGE must read:
   Every row's id, name, age, city, salary (every byte of every row)
   Even though you only care about age + salary.

COLUMN STORAGE reads:
   Only the age column (filter)
   Only the salary column (aggregate)
   Skip id, name, city entirely.

For a wide table (100 columns), reading 2 columns = 2% of the data.
That's a 50x I/O reduction. Add compression and vectorized execution, 
and you get 100-1000x faster queries. This is why warehouses crush OLTP
engines on analytics.
```

#### 1.3 The Analogy

```
Think of a filing cabinet:

ROW LAYOUT (traditional file folder):
   Each folder = one person's complete record
   Pulling a person's info = fast (one folder)
   Finding "average salary of all employees in NYC" = pull every folder, 
      check city, record salary. Painful.

COLUMN LAYOUT (one drawer per attribute):
   Drawer 1: all names
   Drawer 2: all ages
   Drawer 3: all cities
   Drawer 4: all salaries
   
   Pulling a person's info = run around to every drawer, grab their value.
      Painful.
   "Average salary of all employees in NYC" = open city + salary drawers.
      Pleasant.
```

> **🎯 Interview Angle:** Q: Why are analytical databases almost always columnar?
> **Staff:** "Four reasons compound. (1) I/O reduction — analytical queries typically read 2-5 columns from tables that may have 50-500 columns; column storage only reads what you need. (2) Compression — adjacent values in a column are similar, so column data compresses 5-20x; row data compresses 2-3x. (3) Vectorized execution — operating on batches of column values uses CPU SIMD instructions and keeps data in CPU cache. (4) Zone maps / block statistics — each column block stores min/max, enabling pruning before any data is read. Combine these and columnar can be 100-1000x faster than row for analytical queries, with the same data stored on the same disks. For OLTP (write a row, read a row), row still wins because each operation touches a complete row anyway."

### 2. Where Row Storage Comes From (OLTP History)

#### 2.1 The Historical Default

```
EARLY RELATIONAL DATABASES (1970s-80s):
   System R, Ingres, DB2, Oracle
   
   Workloads: transaction processing (OLTP)
      - Order management, banking, inventory
      - Characteristic: insert/update/delete whole rows frequently
      - Read patterns: lookup one record by primary key
      
   Row storage was the natural fit.
   Became the default for all databases through the 1990s.
```

#### 2.2 Why Row Storage Was the Right Choice (Then)

```
HARDWARE CONTEXT (1970s-80s):
   Disk I/O was the bottleneck.
   Rotational disks: ~10ms seek times.
   RAM was expensive and small.
   
   For OLTP workloads:
   - A single business transaction touches a few rows.
   - Each row touched = one disk seek + read.
   - Row storage means you fetch the entire record in one I/O.
   - Column storage would require multiple seeks per record.

CPU wasn't the bottleneck.
   Instructions were cheap relative to I/O.
   Clever execution tricks weren't needed; just do what's conceptually simple.
```

#### 2.3 The Row-Oriented Processing Model

```
EXECUTION:
   For each row in the table:
      Apply predicates (filter)
      Project needed columns
      Compute expressions
      Emit result
   
   This "row-at-a-time" model mirrors how data is stored.
   Called the VOLCANO or ITERATOR model.
   Still used in most OLTP databases today (Postgres, MySQL).
```

#### 2.4 What Row Storage Does Well

```
✓ POINT LOOKUPS: fetch one row by primary key
✓ TRANSACTIONS: ACID with small write amplification
✓ SMALL WRITES: a row INSERT touches one page
✓ FLEXIBLE SCHEMA: variable-length rows, nulls cheap
✓ APPEND-HEAVY: new row = append to the heap
✓ UPDATES: modify a row in place (if space allows)
```

### 3. Where Columnar Storage Comes From (OLAP History)

#### 3.1 The Early Pioneers

```
EARLIEST COLUMNAR IDEAS:
   Sybase IQ (1990s) — first commercial columnar DB
   
2000s: ACADEMIC + COMMERCIAL FLURRY
   C-Store (MIT, 2005) — Stonebraker et al., led to Vertica
   MonetDB (CWI, 2000s) — vectorized execution research
   kdb+ (quants love it) — array-oriented, column-centric
   
2010s: THE COLUMNAR WAVE
   Vertica (HPE), Teradata Columnar, SAP HANA
   Open source: Druid, ClickHouse (2016)
   File formats: Parquet (Twitter + Cloudera, 2013), ORC (Hive, 2013)
   
2015+: CLOUD WAREHOUSES
   Redshift, Snowflake, BigQuery, Databricks SQL — all columnar
   DuckDB (2019) — columnar in-process
```

#### 3.2 Why Now (vs 1980s)

```
HARDWARE CONTEXT (2010s+):
   Disk I/O still relatively expensive — but data volumes exploded
   SSDs made random I/O cheaper
   RAM grew large and cheap
   CPUs got FAST but memory bandwidth became the new bottleneck
   
   For analytical workloads:
   - Scan billions of rows, aggregate a few columns
   - I/O volume dominates; compression + column pruning matter enormously
   - CPU time matters per row processed (vectorization wins)
   
   Columnar matches the shape of modern analytics perfectly.
```

#### 3.3 The Columnar Processing Model

```
EXECUTION:
   For each column of interest:
      Read column data in blocks
      Apply predicates (filter → bitmap of matching rows)
      Materialize only matching rows for next operator
   
   Also called "vectorized" execution — process arrays of values, not row-by-row.
   Keep data in CPU cache; use SIMD instructions.
   
   Function call overhead amortized across batches of 1000+ values.
```

#### 3.4 What Columnar Does Well

```
✓ ANALYTICAL QUERIES: scan huge tables, aggregate columns
✓ COMPRESSION: 5-20x typical ratios
✓ COLUMN PRUNING: ignore columns you don't SELECT
✓ ZONE MAPS: per-block stats enable file-level pruning
✓ VECTORIZED EXECUTION: SIMD-friendly, cache-friendly
✓ APPEND-HEAVY batch inserts
✓ WIDE TABLES: 200-column fact tables stay fast
```

### 4. Why the Choice Matters So Much

#### 4.1 Order-of-Magnitude Performance Difference

```
Query: SELECT SUM(revenue) FROM sales WHERE region = 'EU';

TABLE: 1 billion rows, 50 columns, 500 bytes avg row size
   Total size: 500 GB uncompressed

ROW storage (PostgreSQL on same hardware):
   Must scan all 500 GB
   Read speed: 100 MB/s sequential → 5000 seconds (~1.4 hours)
   
COLUMN storage (DuckDB on same hardware):
   Only scan region + revenue columns (~4% of data, ~20 GB uncompressed)
   Compressed to ~3 GB
   Scan time: 30 seconds
   Plus vectorized execution: 5 seconds
   
   100-1000x faster, same hardware, same data.
```

#### 4.2 Compression Difference

```
Same 1 billion rows:

ROW (CSV): ~500 GB
ROW (Postgres heap with compression): ~200 GB
COLUMN (Parquet with Snappy): ~50 GB
COLUMN (Parquet with ZSTD): ~30 GB
COLUMN (specialized columnar with dict + RLE): ~15 GB

Column storage is 10-30x more compressed than row storage for typical analytical data.
This affects: storage cost, I/O time, memory needs, network transfer.
```

#### 4.3 The Entire Modern Data Stack Reflects This

```
WHY WAREHOUSES EXIST SEPARATELY FROM OLTP:
   Different physical layouts → different workloads → different products
   
WHY PARQUET WON OVER CSV:
   Columnar compression + predicate pushdown + schema

WHY LAKEHOUSES ADOPTED PARQUET:
   Enables warehouse-style performance on cheap S3 storage

WHY VECTORIZED EXECUTION DOMINATES:
   Matches column layout; exploits modern CPUs

WHY STREAMING SYSTEMS OFTEN WRITE AVRO:
   Row-oriented is natural for row-at-a-time events;
   batched to columnar later

Every architectural pattern in modern data engineering is downstream of this choice.
```

> **🧠 Principal Mindset:** When a senior engineer learns "warehouses are columnar," they treat it as a fact to memorize. When a staff engineer learns it, they realize it predicts almost every other property of warehouses: why they're bad at transactional updates, why they compress so much better, why wide tables don't hurt performance, why SELECT * is a cost smell, why they scale differently. One concept, many implications. Master the concept and the implications fall out.

---

## PART II — PHYSICAL LAYOUT

### 5. Row Layout: How Rows Live on Pages

#### 5.1 The Page Structure

```
Row-oriented databases organize data as PAGES (fixed-size blocks).
   Common sizes: 4 KB, 8 KB, 16 KB.
   
A page typically contains:
   PAGE HEADER (metadata: free space, LSN, slot count, ...)
   SLOT ARRAY (grows from one end; small pointers to rows)
   ROW DATA (grows from the other end)
   FREE SPACE in the middle
   
   ┌──────────────────────────────────────────┐
   │ PAGE HEADER                               │
   ├──────────────────────────────────────────┤
   │ SLOT 1 | SLOT 2 | SLOT 3 | ... →          │
   ├──────────────────────────────────────────┤
   │                                           │
   │          FREE SPACE                       │
   │                                           │
   ├──────────────────────────────────────────┤
   │ ... ← ROW 3 | ROW 2 | ROW 1               │
   └──────────────────────────────────────────┘
```

#### 5.2 Row Format on Disk

```
A ROW is a packed binary record:
   
   NULL bitmap (1 bit per column, indicating which columns are NULL)
   FIXED-LENGTH FIELDS (int, float, timestamp)
   VARIABLE-LENGTH FIELDS (varchar, text) — stored inline or in overflow pages
   
   Example row for (id INT, name VARCHAR(50), age INT, email VARCHAR(100)):
   
   [null_bitmap: 00000][id: 42][age: 30][name_offset: 20][email_offset: 30]
   [name: "Alice"][email: "alice@example.com"]
   
   or with inline variable-length:
   [null_bitmap][id: 42][age: 30][len: 5]"Alice"[len: 17]"alice@example.com"
```

#### 5.3 Why This Layout for OLTP

```
A SELECT by primary key:
   1. Index points to (page_id, slot_id)
   2. Read ONE page (8 KB) from disk
   3. Look up slot → row pointer
   4. Read the row into memory
   5. Done
   
   One disk I/O. Low latency. Perfect for OLTP.

AN UPDATE in place:
   1. Find the row (as above)
   2. Modify fields in the row
   3. Mark page dirty; write back later
   
   Minimal write amplification.

AN INSERT:
   1. Find a page with free space (via free space map)
   2. Append row data; add slot
   3. Mark dirty
```

#### 5.4 Weaknesses for Analytics

```
A COLUMN-ONLY QUERY (SELECT AVG(age) FROM employees):
   Must read EVERY page containing employees.
   Each page contains ALL columns of its rows.
   You read 100% of the data to get at <5% that you want.
   
   For a 100 GB table, you read 100 GB.
   For a columnar store with same data, you read ~1-5 GB.
```

### 6. Column Layout: Vectors, Blocks, and Row Groups

#### 6.1 Column Storage Organization

```
COLUMN-ORIENTED STORAGE organizes data as:
   
   One or more COLUMN CHUNKS per column
   Each chunk contains encoded + compressed values
   Chunks have per-column statistics (min, max, null count, distinct count)
   
   Logical TABLE still has a schema; physical storage is per-column.
```

#### 6.2 Row Groups (Parquet, ORC)

```
To support efficient scans and splits, columnar files use ROW GROUPS:
   
   ROW GROUP = horizontal slice of the table
      Contains ~100K to 1M rows (configurable; default ~100K)
      Within a row group: columns stored separately
   
   PARQUET FILE STRUCTURE:
   
   ┌────────────────────────────────────┐
   │ MAGIC NUMBER (PAR1)                 │
   ├────────────────────────────────────┤
   │ ROW GROUP 1                         │
   │  ├── Column A (chunk)              │
   │  ├── Column B (chunk)              │
   │  ├── Column C (chunk)              │
   │  └── ...                            │
   ├────────────────────────────────────┤
   │ ROW GROUP 2                         │
   │  ├── Column A (chunk)              │
   │  └── ...                            │
   ├────────────────────────────────────┤
   │ ROW GROUP 3                         │
   │  └── ...                            │
   ├────────────────────────────────────┤
   │ FILE METADATA (footer)              │
   │  ├── Schema                         │
   │  ├── Row group metadata             │
   │  ├── Column chunk metadata          │
   │  │   (stats: min, max, nulls)       │
   │  └── Encodings                      │
   ├────────────────────────────────────┤
   │ FOOTER LENGTH + MAGIC NUMBER        │
   └────────────────────────────────────┘
```

#### 6.3 Why Row Groups Instead of One Giant Column

```
TRADE-OFF with row group size:
   TOO LARGE: 
      - Harder to parallelize scans
      - Can't prune granularly
      - Memory to load one group grows
   TOO SMALL:
      - Compression ratio drops (less context)
      - More metadata overhead
      - Statistics less useful (narrower windows)

Default: 128 MB per row group is a common target.
Snowflake / Databricks tune: 128 MB - 1 GB.
```

#### 6.4 Pages Within Column Chunks

```
Within a column chunk, data is further split into PAGES:
   
   PAGE = unit of compression + encoding (typically ~1 MB)
   Per-page: encoding type + optional statistics
   Pages within a column chunk compressed independently
   
   Benefit: random access to a page without decompressing whole chunk
   Trade-off: slightly worse compression than compressing whole chunk
```

### 7. The Hybrid: PAX and the Middle Ground

#### 7.1 PAX (Partition Attributes Across)

```
PAX = academic design that organizes data as:
   
   Pages are still fixed size (as in row stores).
   Within each page, data organized by COLUMN (as in columnar).
   
   Each page = one "mini-columnar store" of ~100-1000 rows.
   
   Benefits:
      - Rows co-locate (as in row stores) — good for row lookups
      - Columns stored contiguously within a page — good for analytical scans
      - Modular; works with existing page-based databases
```

#### 7.2 Where PAX Shows Up in Practice

```
Parquet row groups are PAX-like:
   Rows co-locate (within a row group)
   Columns separate (within the row group)
   
Column stores often borrow PAX ideas for small-batch workloads.
   SingleStore (formerly MemSQL), SQL Server Columnstore both use related designs.
```

### 8. Memory Representation: Arrow and In-Memory Formats

#### 8.1 Apache Arrow

```
APACHE ARROW = standardized columnar in-memory format.
   
   Why important:
      Enables zero-copy data interchange between systems
      Common memory layout: Pandas, Polars, DuckDB, Spark, 
         PyTorch, R, BigQuery, Snowpark, more
   
   Properties:
      Columnar in RAM
      Fixed-width or variable-width columns
      Null bitmaps per column
      SIMD-friendly alignment
      No serialization overhead to pass between tools
```

#### 8.2 The Zero-Copy Pattern

```python
# Zero-copy data interchange via Arrow
import duckdb
import pandas as pd

# A Pandas DataFrame
df = pd.read_csv('data.csv')

# DuckDB queries it directly without copying
result = duckdb.sql("SELECT * FROM df WHERE value > 100").df()
# ↑ No serialization; both use Arrow underneath
```

Arrow has become the lingua franca of in-memory analytical data in Python/Rust/C++.

#### 8.3 Columnar in RAM vs On Disk

```
COLUMNAR ON DISK: Parquet, ORC
   Compressed; encoded for storage efficiency
   Must be decoded for computation

COLUMNAR IN RAM: Arrow, PAX
   Decoded; aligned for SIMD
   Typically 2-5x larger than compressed Parquet
   Fast to compute on

Modern engines read Parquet → decode to Arrow in RAM → compute vectorized.
```

---

## PART III — COMPRESSION

### 9. Why Columnar Compresses So Much Better

#### 9.1 The Core Insight

```
Adjacent values in a COLUMN are similar.
Adjacent fields in a ROW are different (int, string, date, int, ...).
   
   Compression algorithms exploit similarity.
   Column: easy win (10x compression ratios common).
   Row: harder (2-3x typical).

Example — "city" column of 1M rows:
   Likely only ~100 distinct cities
   Dictionary encoding: 1M × 1 byte = 1 MB
   Vs row-stored city values: 1M × ~15 bytes = 15 MB
   15x compression just from dictionary encoding.
```

#### 9.2 Compression Depends on the Data Shape

```
HIGHLY COMPRESSIBLE columns:
   • Sorted integers (delta encoding)
   • Low-cardinality strings (dictionary encoding)
   • Repeated values (run-length encoding)
   • Monotonic timestamps (delta + bit-packing)

LESS COMPRESSIBLE:
   • Random high-entropy data (hashes, UUIDs)
   • Fully-unique strings (keys)
   • Encrypted data (by design)

Even "incompressible" data compresses modestly with general-purpose algorithms (zstd, LZ4).
```

#### 9.3 Why This Matters Economically

```
A 10 PB raw dataset:
   
   CSV on S3: 10 PB × $23/TB/mo = $230,000/mo
   Parquet (3x): ~3.3 PB × $23 = $75,000/mo
   Parquet (10x): ~1 PB × $23 = $23,000/mo
   
   Compression ratio DIRECTLY = storage cost ratio.
   For big enterprises, this is tens of millions in savings per year.
```

### 10. Encoding Schemes (RLE, Dictionary, Delta, Bit-Packing, FSST)

#### 10.1 Run-Length Encoding (RLE)

```
IDEA: encode a run of identical values as (value, count).

   Original: [A, A, A, A, B, B, B, C, C, C, C, C]
   RLE:      [(A, 4), (B, 3), (C, 5)]

WHEN IT HELPS:
   Sorted data with repeated values
   Partition columns (country = 'US' for millions of rows)

WHEN IT DOESN'T:
   Random data (RLE could make it bigger)
```

#### 10.2 Dictionary Encoding

```
IDEA: build a dictionary of distinct values; store indices.

   Original: ["Alice", "Bob", "Alice", "Carol", "Bob", "Alice"]
   Dictionary: ["Alice" → 0, "Bob" → 1, "Carol" → 2]
   Encoded: [0, 1, 0, 2, 1, 0]

WHEN IT HELPS:
   Low-to-medium cardinality strings/integers
   Even with 100 distinct values, dict + 1-byte indices beats raw strings

WHEN IT DOESN'T:
   Fully unique values (dictionary as big as data)
```

#### 10.3 Delta Encoding

```
IDEA: store differences between consecutive values.

   Original: [100, 102, 101, 103, 105, 104]
   Delta:    [100, 2, -1, 2, 2, -1]
   
   Deltas are typically small → bit-pack them.

WHEN IT HELPS:
   Sorted or near-sorted integers (IDs, timestamps)
   Time series (timestamps increment regularly)

WHEN IT DOESN'T:
   Random data
```

#### 10.4 Bit-Packing

```
IDEA: use only the bits needed for the actual value range.

   Values 0-7 only need 3 bits each (not 32)
   10 million such values: 3.75 MB vs 40 MB.

Common with delta encoding: after taking deltas, the range shrinks,
so bit-packing becomes efficient.
```

#### 10.5 FSST (Fast Static Symbol Table)

```
IDEA: new (CWI research, 2020+) string compression.
   Build a dictionary of common SUBSTRINGS, not whole strings.
   
   Original: ["https://example.com/path/a", "https://example.com/path/b", ...]
   Common substring: "https://example.com/path/"
   Encoded: each row only stores the suffix.

WHEN IT HELPS:
   Long strings with common prefixes/subsequences
   URLs, log lines, file paths

DuckDB and others adopted FSST; much faster than general compression on strings.
```

#### 10.6 Combining Encodings

```
A real column uses MULTIPLE encodings in sequence:
   
   Column: event_timestamp (int64, mostly monotonic)
   1. DELTA encoding: produces small ints
   2. BIT-PACKING: 7 bits per delta instead of 64
   3. ZSTD or SNAPPY: general compression on the bytes
   
   Combined ratio: often 10-30x for sorted timestamps.

Dictionary + RLE pattern:
   Column: country
   1. DICTIONARY encode (short indices instead of strings)
   2. RLE on the indices (if sorted by country)
   
   Combined ratio: often 100-1000x for low-cardinality sorted data.
```

#### 10.7 ALP (Adaptive Lossless floating-Point)

```
Recent (2024) technique for floating-point compression.
   Analyzes patterns in floats; applies several encodings adaptively.
   Compresses floats 2-5x on real-world data where general compression gets 1.2x.
   
   Used by DuckDB, growing adoption.
```

### 11. Compression Ratios in Practice

#### 11.1 Typical Ratios

```
BY DATA TYPE:

Numeric columns:
   Sorted (keys, timestamps):    10-50x
   Random (metrics, measurements): 1.5-3x
   
Categorical / low-cardinality strings:
   10-50x via dictionary + RLE

Free text (log messages, descriptions):
   2-5x via general compression

Nested / JSON:
   3-10x via dictionary of structure + value compression

OVERALL:
   Typical analytical dataset: 5-10x
   Well-tuned columnar: 10-20x
   Best cases: 100x+ (highly repetitive, sorted)
```

#### 11.2 Compression vs Decompression Cost

```
FAST (low CPU cost):
   Snappy, LZ4 — fast decompress; moderate compression
   Common default for Parquet
   
MEDIUM:
   ZSTD — much better compression; slightly slower
   Default for many modern Parquet writers

HIGH COMPRESSION:
   gzip — slow but widely-supported
   LZMA / xz — best compression; too slow for analytics

CHOICE:
   For analytics (read-heavy, scan huge): ZSTD or Snappy
   For archival: ZSTD (level 15+) or gzip
```

### 12. Compression's Cost (CPU vs I/O Trade-off)

```
Compression trades CPU time for I/O time.

In modern cloud:
   Disk/network I/O is often the bottleneck
   CPU is relatively abundant
   
   Compressing more aggressively = less I/O = faster queries
   Up to a point — if decompression itself becomes slow, lose the gain

RULE: for analytical queries, almost always compress.
      Choose Snappy (fast) or ZSTD (better ratio) based on your bottleneck.
```

---

## PART IV — EXECUTION

### 13. Row-at-a-Time (Volcano) vs Vectorized

#### 13.1 The Volcano / Iterator Model (Row-at-a-Time)

```
CLASSICAL EXECUTION (Postgres, MySQL, SQLite, Oracle default):
   
   Each operator has a NEXT() method.
   Operators chain: SELECT → FILTER → SCAN.
   Each NEXT() call returns ONE row.
   
   Pipeline:
      scan.next() → row
      filter.next() → calls scan.next(), checks predicate, returns row or loops
      project.next() → calls filter.next(), computes columns

PROBLEMS:
   - One method call per row per operator
   - Branch prediction fails frequently
   - CPU cache under-utilized
   - Instruction cache thrash
   - No SIMD opportunity
```

#### 13.2 The Vectorized Model

```
VECTORIZED EXECUTION (DuckDB, ClickHouse, Snowflake, BigQuery, Databricks Photon):
   
   Operators process BATCHES of rows (typically 1024-4096).
   Each operator produces vectors of values.
   
   Pipeline:
      scan.next() → vector of 2048 values per column
      filter.apply(vector) → bitmap of matches; filtered vectors
      project.apply(vectors) → computed vectors

BENEFITS:
   - Method call overhead amortized across 1000+ rows
   - SIMD: operate on 4-16 values per instruction
   - Data stays in CPU L1/L2 cache through operator pipeline
   - Branch prediction works (same branch taken for whole batch)
   - 10-100x faster than row-at-a-time
```

#### 13.3 The Batch Size Sweet Spot

```
Why ~2048 rows per batch?

TOO SMALL: amortization doesn't kick in.
TOO LARGE: data falls out of L1/L2 cache during the pipeline.

Typical batch sizes:
   DuckDB: 2048
   Postgres (optional vectorized path): 1024
   Snowflake: ~1024
   ClickHouse: configurable; often 65536 for scan, smaller for ops

Modern CPUs have ~32-64 KB L1 cache per core.
2048 int32 values = 8 KB per column. 4 columns fit in L1. Perfect.
```

#### 13.4 Code Example (Conceptual)

```c
// Volcano / row-at-a-time filter
Row filter_next() {
    while (true) {
        Row r = scan_next();
        if (r.is_end()) return END;
        if (r.city == "NYC") return r;
    }
}

// Vectorized filter
Vector filter_apply(Vector city_col) {
    Vector matches = alloc_vector();
    for (int i = 0; i < BATCH_SIZE; i++) {
        matches[i] = (city_col[i] == "NYC");
    }
    return matches;  // CPU vectorizes this loop with SIMD
}
```

### 14. SIMD and the CPU Angle

#### 14.1 What SIMD Is

```
SIMD = Single Instruction, Multiple Data.
   
   One CPU instruction operates on multiple values at once:
      SSE: 4 int32 at a time
      AVX2: 8 int32 at a time
      AVX-512: 16 int32 at a time
      ARM NEON: 4 int32 at a time
      
   Example: comparing a column of 1 million int32 to a constant:
      Scalar: 1M instructions
      AVX2:   125K instructions (8x fewer)
      AVX-512: 62K instructions (16x fewer)
```

#### 14.2 Columnar + SIMD

```
Columnar storage makes SIMD effective because:
   Values of same type are adjacent in memory (SIMD requires this)
   Vectorized operators naturally process arrays
   Modern compilers auto-vectorize column loops

Row-oriented storage makes SIMD hard:
   Values of different types interleave
   Rows have variable widths
   Explicit "unpack from row" step before SIMD
```

#### 14.3 Why This Matters

```
A 10x SIMD speedup on scan + filter + aggregate operations
   = 10x faster queries without any new hardware
   
   Combined with columnar storage, this is where the "warehouses are 100x
   faster than Postgres on analytics" number comes from.
```

### 15. Query Optimization: Projection and Predicate Pushdown

#### 15.1 Projection Pushdown

```
QUERY: SELECT age, salary FROM employees WHERE department = 'ENG';

ROW storage:
   Reads EVERY column of EVERY row (whole table).

COLUMN storage:
   Reads age, salary, department columns ONLY.
   Even if the table has 100 columns, you touch 3.

The "projection" (SELECT list) is pushed DOWN to the storage layer,
   so the storage only materializes what's needed.
```

#### 15.2 Predicate Pushdown

```
QUERY: SELECT * FROM events WHERE event_date = '2024-03-15';

COLUMN storage with per-chunk stats:
   Each column chunk has min/max of event_date.
   Chunks where '2024-03-15' is outside the range: SKIP (no I/O at all).
   
   Could skip 99%+ of chunks for selective predicates.

ROW storage with indexes:
   Can also skip, but requires an index on event_date.
   Full table scan without index.
```

#### 15.3 Combining Both

```
QUERY: SELECT user_id, SUM(amount) FROM orders 
       WHERE date = '2024-03-15' GROUP BY user_id;

COLUMNAR EXECUTION:
   1. Read date column (projection pushdown)
   2. Prune row groups by min/max on date (predicate pushdown)
   3. Only in matching row groups, read user_id + amount columns
   4. For each row group: user_id + amount into vectors
   5. Filter by date (now we know exact matches)
   6. Hash aggregate on user_id

For a 1 TB table with 0.01% selectivity: physical I/O ~100 MB. 
Row store would read full 1 TB.
```

### 16. Late Materialization and Zone Maps

#### 16.1 Zone Maps

```
Also called "min-max indexes" or "per-block statistics":
   
   Each column chunk (or row group) stores:
      • min, max
      • null count
      • distinct count (approximate)
   
   USED FOR PRUNING at scan time without reading data.

Example: scan 1 million row groups, each with min/max of event_date.
   Query: event_date = '2024-03-15'
   Load footer, check 1M min/max pairs
   Open only matching ~100 row groups (0.01%)
   Read column data from those 100
   
   Pruning happens in metadata, before data read.
```

#### 16.2 Late Materialization

```
LATE MATERIALIZATION = delay reading data until necessary.

Scan plan (without late materialization):
   1. Read all columns of all rows
   2. Apply predicates
   3. Output rows

Scan plan (with late materialization):
   1. Read only predicate columns
   2. Apply predicates → row position bitmap
   3. Read other needed columns only at the surviving positions
   4. Materialize final rows

For selective predicates: 99% of data from "other columns" is skipped.
This is a core columnar optimization.
```

---

## PART V — WORKLOADS

### 17. OLTP: Why Row Still Wins

#### 17.1 OLTP Characteristics

```
TYPICAL OLTP:
   - High-concurrency, small transactions
   - 1000s TPS of insert/update/delete
   - Each transaction touches a few rows
   - Latency matters (ms)
   - Reads are often point lookups (by PK)

Example: e-commerce orders.
   Place order: INSERT one order, UPDATE inventory, INSERT shipping.
   Each operation touches 1-10 rows.
```

#### 17.2 Why Row Is Right

```
Row storage shines because:
   
   1. Each transaction's rows are co-located
      Writing a row = one page I/O
   
   2. Random point lookups
      Index → page → row directly
   
   3. Row-level locking
      Lock the row's page/row ID
   
   4. ACID with minimal write amplification
      Only the touched rows' pages are dirty

If you used columnar:
   An INSERT would require writing to every column file
   An UPDATE requires modifying every column
   Row-level operations become multi-file operations
   Latency, throughput, write amplification all suffer
```

### 18. OLAP: Why Columnar Dominates

#### 18.1 OLAP Characteristics

```
TYPICAL OLAP:
   - Batch or interactive analytical queries
   - Scan millions-to-billions of rows
   - Aggregate / join / window across columns
   - Latency: seconds to minutes acceptable
   - Writes are append-heavy (bulk loads)

Example: analyze last quarter's sales by region/product/channel.
   Scan the fact_orders table (hundreds of millions of rows)
   Project a few columns
   Group and aggregate
```

#### 18.2 Why Columnar Is Right

```
Columnar storage shines because:
   
   1. Only needed columns are read (I/O win)
   2. High compression ratios (I/O + storage win)
   3. Zone maps enable pruning (I/O win)
   4. Vectorized execution + SIMD (CPU win)
   5. Pretty wide tables (100 columns) are fine
   6. Aggregations are cache-friendly
```

### 19. HTAP: The Hybrid Workload Problem

#### 19.1 What HTAP Is

```
HTAP = Hybrid Transactional / Analytical Processing.
   
   The dream: one database that handles both OLTP and OLAP.
   Avoid the ETL pipeline from OLTP → warehouse.
   Fresh analytics on live operational data.
```

#### 19.2 Why It's Hard

```
Row and column layouts are fundamentally different.
   Can't be both efficient simultaneously on the same storage.
   
HTAP systems typically:
   Option A: maintain BOTH representations (row + columnar) and keep them in sync
   Option B: pick one layout and compromise on the other workload
   Option C: tiered — recent data in row, older data in column
   Option D: on-the-fly columnar from row (e.g., SQL Server Columnstore Index)
```

#### 19.3 Why Many Teams Still Separate

```
Even in 2026:
   Most enterprises run OLTP (Postgres/MySQL) separate from OLAP (Snowflake/BQ).
   CDC pipelines them together.
   
   Reasons:
      Specialized systems outperform HTAP on each workload
      Scale analytical compute independently
      Analytical crashes don't affect OLTP
      Different concurrency / isolation models

HTAP is tempting; separation is still the dominant architecture.
```

### 20. Mixed-Workload Systems

#### 20.1 Systems That Attempt HTAP

```
TIDB:
   Row + columnar replicas kept in sync via Raft
   Columnar replicas (TiFlash) serve analytical queries
   Row replicas (TiKV) serve OLTP
   Each workload hits its optimal storage

SINGLESTORE (formerly MemSQL):
   In-memory row for recent data; disk columnar for older
   Automatic tiering

PLANETSCALE:
   Vitess-based MySQL + columnar replicas
   Target: ops data with analytical overlay

SNOWFLAKE HYBRID TABLES (2023+):
   Row-oriented table type for OLTP-like access
   Co-exists with columnar tables
   Underlying: unistore (hybrid storage)
   Still experimental for serious OLTP

DATABRICKS Photon with Delta:
   Columnar Delta with row-level primary key support (via Lakebase / Hybrid)
   Target: HTAP within the lakehouse paradigm
```

#### 20.2 When to Pick HTAP

```
PICK HTAP WHEN:
   ✓ Operational dashboards must reflect live state (seconds-fresh)
   ✓ Can't afford ETL pipeline latency
   ✓ Have small-ish data (sub-TB working set typical)

RECONSIDER WHEN:
   ✗ Workloads differ dramatically in scale
   ✗ Analytical teams want decoupled compute
   ✗ Separation is simpler to operate
```

> **🎯 Interview Angle:** Q: Why don't we just use a HTAP database and avoid the ETL pipeline?
> **Staff:** "HTAP is appealing in principle but the trade-offs are real. (1) Any HTAP system is making compromises on BOTH axes — it won't match specialized OLTP OR specialized OLAP at scale. (2) Operationally, the OLTP crashes shouldn't bring down analytics; separation enables independent scaling and reliability. (3) The ETL pipeline that teams dread is often actually VALUABLE — it enforces modeling, data quality, and governance that keeps analytics trustworthy. (4) Modern CDC + lakehouse latency is seconds-to-minutes; HTAP's milliseconds-fresh is rarely required. For specific workloads (operational dashboards, real-time personalization on small data), HTAP works. For general enterprise analytics, the OLTP + warehouse split remains the right architecture."

---

## PART VI — FILE FORMATS

### 21. Parquet Deep Dive

#### 21.1 Why Parquet Won

```
PARQUET (Twitter + Cloudera, 2013):
   The de facto columnar file format for data lakes.
   
   Why it won:
      Open specification (Apache)
      Multi-engine support (every major engine reads it)
      Great compression (encoded + general compression)
      Row group organization for parallel reads
      Rich statistics (min/max, nulls, histograms via Bloom filters)
      Nested type support (lists, structs, maps)
      Schema evolution

Alternatives (ORC, Avro) exist but Parquet dominates.
```

#### 21.2 Parquet File Structure (Revisited)

```
Covered in section 6.2 above. Key properties:
   • Columnar within row groups
   • Row groups ~128 MB default
   • Pages within column chunks (~1 MB)
   • Footer with metadata + statistics
   • Can read footer first, prune row groups, read only needed chunks
```

#### 21.3 Parquet Encodings

```
Supported encodings:
   PLAIN: raw bytes
   DICTIONARY: dictionary-encoded
   RLE: run-length encoding
   DELTA_BINARY_PACKED: delta + bit-packing (for int)
   DELTA_LENGTH_BYTE_ARRAY: delta for byte arrays
   DELTA_BYTE_ARRAY: for strings with common prefix

Parquet writer picks per-column dynamically based on data.
Modern writers add FSST (via DuckDB's Parquet extension) and ALP for floats.
```

#### 21.4 Parquet with Different Engines

```
PERFORMANCE varies by Parquet reader:
   DuckDB's reader: ultra-fast; vectorized decode
   Spark's reader: solid, mature
   PyArrow / Pandas: good for Python workflows
   Snowflake/BigQuery readers: optimized for their engines
   
   Writing: pick row group size + compression per workload
      Large row groups (1 GB): fewer files, slower random access
      Small row groups (64 MB): more parallelism, more metadata overhead
```

### 22. ORC (Optimized Row Columnar — misleading name)

```
ORC (Optimized Row Columnar) — Hortonworks, 2013.
   Despite "Row" in name, ORC IS columnar.
   
   Advantages over Parquet:
      Slightly better compression on some data
      Richer indexes (predicate pushdown granularity)
      Better suited for Hive originally
   
   Disadvantages:
      Smaller ecosystem than Parquet
      Fewer tools / engines support it
   
Status in 2026:
   ORC is a legacy choice; Parquet has broader support.
   Hive deployments may still use it; new projects → Parquet.
```

### 23. Avro (Row-Oriented for Streaming)

#### 23.1 What Avro Is

```
APACHE AVRO = row-oriented binary format for data serialization.
   
   Designed for Hadoop; now widely used for Kafka messages.
   
   Properties:
      Row-oriented (each row self-contained)
      Compact binary encoding
      Rich schema (JSON-defined; evolves cleanly)
      Schema stored separately or embedded
```

#### 23.2 Why Avro for Streaming

```
Streaming events arrive row-by-row:
   One Kafka message = one event = one row
   
   Avro fits this naturally:
      Each message is a self-contained row
      Schema registry provides type safety across producers/consumers
      Backward/forward compatible via Avro's schema evolution rules

Later:
   Streaming events in Avro → batch convert to Parquet for analytical queries.
   Row for the point of ingestion; column for analysis.
```

#### 23.3 Protobuf / JSON Alternatives

```
PROTOBUF: Google's row format; similar role to Avro.
JSON: human-readable but much larger and slower.

For Kafka in 2026:
   Avro + Schema Registry is the most common choice
   Protobuf has growing adoption
   JSON works but is wasteful at scale
```

### 24. Arrow (In-Memory Columnar)

#### 24.1 Arrow's Role

```
APACHE ARROW = in-memory columnar format standard.
   
   Not a disk format (that's Parquet).
   The RAM representation that enables fast computation and zero-copy transfer.
   
   Used by:
      Pandas 2.0+
      Polars
      DuckDB
      Apache Spark (increasingly)
      BigQuery read API
      Snowpark
      PyTorch, R, dozens more
```

#### 24.2 Arrow Flight

```
ARROW FLIGHT = network protocol for Arrow data.
   
   Replaces slow row-by-row JDBC with batch-oriented Arrow transfer.
   10-100x faster data transfer between services.
   
   Used by: Dremio, Snowflake connectors, DataFusion, more.
```

### 25. Format Decision Guide

```
WHEN TO USE EACH:

PARQUET:
   ✓ Analytical workloads (lakehouse, warehouse external tables)
   ✓ Long-term storage of analytical data
   ✓ Multi-engine access

ORC:
   Only if legacy Hive-centric; otherwise prefer Parquet

AVRO:
   ✓ Streaming events (Kafka messages)
   ✓ Row-at-a-time serialization
   ✓ Schema evolution with Schema Registry

ARROW:
   ✓ In-memory representation (not for storage)
   ✓ Zero-copy data interchange between tools

CSV:
   ✓ Human-readable interchange
   ✗ Never for production analytics

JSON:
   ✓ Semi-structured data interchange
   ✗ Never for analytical storage at scale (use Parquet or Avro)
```

---

## PART VII — DATABASE SYSTEMS

### 26. Row-Oriented Databases (Postgres, MySQL, Oracle)

#### 26.1 The Row-Oriented Dominators

```
POSTGRES:
   Row-based heap tables
   Index-organized options via extensions
   OLTP-first; analytical via extensions (cstore_fdw, Citus)

MYSQL INNODB:
   Clustered index (table IS the primary key index)
   Rows co-located by PK
   Great for PK-sorted scans

ORACLE:
   Heap tables (default)
   Index-organized tables (IOT) for PK-sorted
   Hybrid columnar compression (archive)

MS SQL SERVER:
   Row-oriented default
   Columnstore indexes for HTAP (see section 28)
```

#### 26.2 When You See Row in Analytics

```
Analytical queries on Postgres: usually suboptimal
   Full table scans
   No column pruning
   Compression limited

Patterns:
   - Use for OLTP + CDC out to warehouse for analytics
   - Add columnar extensions for small-scale analytics (cstore_fdw)
   - Don't try to be a warehouse
```

### 27. Columnar Databases (Snowflake, BigQuery, Redshift, ClickHouse, DuckDB)

#### 27.1 Pure Columnar Systems

```
Covered in detail across topics 7, 10, 11, 12 of this series.
Common properties:
   ✓ Columnar storage
   ✓ Vectorized execution
   ✓ MPP (except DuckDB — single node)
   ✓ Rich statistics for pruning
   ✓ Encoding + compression

Differences:
   Snowflake: micro-partitions (Snowflake-specific columnar format)
   BigQuery: Capacitor format
   Redshift: columnar Postgres fork
   ClickHouse: MergeTree with columnar sort
   DuckDB: in-process columnar (SQLite-of-analytics)
```

### 28. The Hybrid Approach (SQL Server Columnstore, Postgres cstore_fdw)

#### 28.1 Columnstore Indexes

```
SQL SERVER COLUMNSTORE INDEXES:
   Add a columnar "secondary" to a row-oriented table.
   
   Row store serves OLTP (small lookups, updates).
   Columnstore serves analytics (large scans, aggregates).
   
   Modes:
      Clustered Columnstore Index (table IS columnstore)
      Non-clustered Columnstore (secondary index)
   
   Compression: 10x+ typical
   Automatic batch-mode (vectorized) execution for columnstore scans
   
   USE: SQL Server shops doing light analytics without separate warehouse.
```

#### 28.2 Postgres Columnar Extensions

```
CSTORE_FDW / HYDRA:
   Postgres extension providing columnar tables.
   Approach: separate tablespaces for row vs column.
   
   Limited vs full warehouse but useful for:
      Analytical reporting on Postgres data without a warehouse
      Small-scale OLAP ($5K data warehouse) without migration

Timescale is similar for time-series (compressed hypertables).
```

#### 28.3 The Future: Specialized Still Wins

```
Attempts to make OLTP databases "also do analytics" via columnstore:
   Solve the small-data case
   Fall over at scale (dedicated warehouses still dominate large analytics)
   
   The specialization gap doesn't close; it stabilizes:
      OLTP: row-first for its workload
      OLAP: column-first for its workload
      HTAP: hybrid for narrow use cases
```

---

## PART VIII — INTERVIEW PRACTICE

### 29. The Staff-Level Row/Column Interview

#### 29.1 Common Questions

```
1. "Explain row vs columnar storage."
2. "Why do analytical databases use columnar storage?"
3. "How does Parquet achieve such good compression?"
4. "What's vectorized execution and why does it matter?"
5. "Can you use columnar storage for OLTP? Why or why not?"
6. "Explain predicate pushdown and projection pushdown."
7. "How does Snowflake / BigQuery / Redshift achieve interactive 
    performance on billions of rows?"
8. "What's HTAP and when is it the right choice?"
9. "Why can Pandas be slower than DuckDB on large data?"
10. "Design the storage layer for a system supporting both 
    operational and analytical queries."
```

#### 29.2 Worked Example: Design a Storage Layer

```
Q: Design the storage layer for an e-commerce company. OLTP transactions
   (hundreds of TPS) + analytics (daily dashboards, ad-hoc exploration).
   100 TB of historical data, 1 TB/month new.

STAFF ANSWER:

Separate storage layers for separate workloads. Don't try to be HTAP.

OPERATIONAL (OLTP):
   PostgreSQL for transactional data
   Row-oriented; indexes on query patterns
   Replication for read scaling
   
ANALYTICAL (OLAP):
   Lakehouse: S3 + Iceberg + Parquet
   Delta Lake OR Iceberg format (both columnar underneath)
   Compressed Parquet files (~10x compression)
   Partitioned by date for most queries
   
CDC BRIDGE:
   Debezium or AWS DMS: Postgres WAL → Kafka → Iceberg
   Latency: seconds to minutes
   
QUERY ENGINES:
   Snowflake / BigQuery / Databricks SQL (reading Iceberg)
   OR Trino + Iceberg
   DuckDB for ad-hoc exploration
   
   All columnar execution engines operating on columnar storage.

TRADE-OFFS:
   Pros: each workload runs on its optimal storage; scales independently.
   Cons: ETL complexity, data duplication, freshness lag.
   
   Vs HTAP: HTAP compromises on both axes; separation lets each
            optimize for its workload. At 100 TB, separation wins.

Why this matters at interview:
   The question tests whether you understand WHY you'd separate.
   "Postgres for OLTP, Snowflake for OLAP" is senior.
   "Row storage is right for OLTP's write patterns; columnar is right for 
    OLAP's read patterns; separation lets each use its optimal physical 
    layout; the ETL cost is justified by the specialization gains" is staff.
```

#### 29.3 Senior vs Staff Comparison

| Senior Answer | Staff Answer |
|---------------|--------------|
| "Warehouses are columnar; OLTP databases are row-oriented." | "Row and columnar storage reflect fundamentally different trade-offs. Row stores co-locate fields of a record, optimizing for whole-record access (OLTP). Columnar stores co-locate values of the same column, optimizing for column-wise operations (OLAP). The implications ripple everywhere: compression ratios differ 5-10x, I/O volume for analytical queries differs 10-100x, suitable execution models differ (volcano vs vectorized), workload semantics differ. The 'warehouses are columnar' fact predicts almost every performance characteristic that follows." |
| "Parquet compresses better than CSV." | "Parquet compresses 5-20x better than CSV because columnar storage puts similar values adjacent, enabling dictionary encoding, run-length encoding, delta + bit-packing, and specialized string compression like FSST. CSV compression (gzip, zstd) sees only general-purpose patterns; Parquet layers specialized columnar encodings UNDER general compression. For a 1 TB dataset, Parquet at $0.023/GB/mo on S3 costs ~$2.3K/year; equivalent CSV would be $23K+. Compression ratio directly translates to storage cost + I/O time + network transfer." |
| "Use columnar for analytics." | "Columnar is right when queries touch few columns of many rows. Specifically: analytical queries typically project 2-5 columns from tables of 50+ columns, with filters on 1-2 columns. Columnar gives you projection pushdown (don't read columns you don't need), predicate pushdown (zone maps prune row groups), compression (adjacent similar values), vectorized execution (operate on vectors, use SIMD). For transactional workloads touching few rows but all columns, these advantages flip into disadvantages: writing one row touches every column file." |

### 30. War Stories

> **🔥 Production Scenario — The 100x Query Speedup.** A team ran daily analytical queries on their Postgres primary. Queries took 2-4 hours; ate CPU during business hours. Added a columnar extension (cstore_fdw) and moved the top 10 analytical tables to columnar storage. Queries dropped to 2-4 minutes. 60-120x speedup. The row/columnar switch alone was most of the win. Lesson: when you're forced to run analytics on OLTP, columnar extensions can be a huge win.

> **🔥 Production Scenario — The Parquet Row Group Trap.** A team wrote Parquet files with default settings. Row group size came out to ~500 rows per group (writer bug). Analytical queries were slow because the per-row-group metadata overhead dominated. Fix: specified `rowGroupSize = 128MB` on write. Queries dropped from minutes to seconds. Lesson: Parquet writer defaults aren't always optimal; verify row group sizes.

> **🔥 Production Scenario — The HTAP Mirage.** A team adopted a HTAP database for their SaaS backend. OLTP worked well. Analytics on top worked but was slow for large aggregations (because the row layout under the hood). Eventually added a separate warehouse anyway for serious analytics. The HTAP system kept the operational benefits but gave up on OLAP use cases. Lesson: HTAP works for narrow use cases (operational dashboards); doesn't eliminate the need for a warehouse at scale.

> **🔥 Production Scenario — The Pandas Death March.** A data scientist's Pandas pipeline processed 100 GB of CSV, took 6 hours, OOM'd intermittently. Rewrote in DuckDB SQL: 90 seconds, no OOM. The storage format (CSV, read into row-ish Python structures) and execution model (row-at-a-time Python) were both wrong for the workload. DuckDB used Parquet in RAM (Arrow), vectorized execution, SIMD. Lesson: Pandas for small data; DuckDB/Polars for anything approaching GB+.

> **🔥 Production Scenario — The Vectorized Postgres Discovery.** A team benchmarked their OLTP Postgres against DuckDB on an analytical workload (same hardware, same CSV data loaded). DuckDB: 3 seconds. Postgres: 4 minutes. Same data, same machine. The difference was storage format (columnar in DuckDB; row in Postgres) and execution (vectorized vs volcano). This 80x gap on single-node analytics is why columnar + vectorized dominates OLAP. Lesson: when you benchmark across layouts, the gap is bigger than most engineers expect.

### 31. Cheat Sheet &amp; Mental Models

#### 31.1 The One-Minute Pitch

```
Row vs columnar storage is the FUNDAMENTAL physical layout decision.

ROW: fields of a record together. OLTP's natural fit. Postgres, MySQL, Oracle.
COLUMN: values of a column together. OLAP's natural fit. Snowflake, BigQuery, Parquet.

Columnar enables:
   Column pruning (skip unneeded columns)
   Compression (10-20x typical, vs 2-3x for row)
   Vectorized execution + SIMD (10-100x faster CPU)
   Zone maps / per-chunk statistics (predicate pushdown)

Combined: columnar can be 100-1000x faster than row for analytics.
Row remains right for transactional workloads where you access whole records.
```

#### 31.2 The Four Lenses — Compressed

```
CONCEPT
   Physical layout of data on disk + in memory.
   Row = fields together. Column = values together.
   
MECHANICS
   Row: pages with slot arrays, variable-length rows.
   Column: row groups → column chunks → pages, with encodings + compression.
   Memory: Arrow is the standard columnar in-memory format.
   
TRADE-OFFS
   Row: fast point access, cheap updates, flexible schema.
   Column: fast scans, compression, vectorized, pruning.
   HTAP: compromise on both; narrow use cases.
   
INTERVIEW
   Know: why warehouses are columnar; Parquet structure; vectorized execution;
         why Pandas/Postgres are slow on analytics; HTAP trade-offs.
   Defend: separation of OLTP + OLAP as architectural pattern.
```

#### 31.3 Compression Ratios Quick Table

| Data Type | Row Compression | Column Compression |
|-----------|-----------------|---------------------|
| Sorted integers | 1.5-2x | 10-50x (delta + bit-pack) |
| Random integers | 1-1.5x | 1.5-3x |
| Low-cardinality strings | 2-3x | 10-50x (dict + RLE) |
| Timestamps (monotonic) | 1.5-2x | 10-30x |
| Free text | 2-3x | 2-5x |
| Overall analytical dataset | 2-3x | 5-20x |

#### 31.4 The Decision Guide

```
WHEN TO USE ROW STORAGE:
   ✓ OLTP (frequent single-row writes/reads)
   ✓ Point lookups by key
   ✓ ACID transactions
   ✓ Small, flexible-schema data

WHEN TO USE COLUMNAR STORAGE:
   ✓ Analytical queries (scan + aggregate)
   ✓ Wide tables (many columns, few touched per query)
   ✓ Append-heavy workloads
   ✓ Large data (GB-PB)

WHEN TO USE BOTH (SEPARATELY):
   ✓ Most enterprises: OLTP + OLAP separate, bridged by CDC
   ✓ This is the default modern pattern

WHEN TO USE HTAP:
   ✓ Narrow use cases: operational dashboards; real-time personalization
   ✓ Small working set (sub-TB)
   ✗ Not for large-scale analytics
```

#### 31.5 Mental Model

```
Think of row vs columnar as the "orientation" of your data.

   Row orientation = "each person's folder has all their info"
   Column orientation = "one drawer per attribute, all people's values in each drawer"

For OLTP ("fetch this person's info"): row wins.
For OLAP ("average salary of all people"): column wins.

Everything else — compression, execution model, workload fit — follows from this.

The modern data stack is largely about:
   Having row storage for operations (Postgres)
   Having columnar storage for analytics (Snowflake / Parquet / etc.)
   Bridging them with CDC / ETL
   Keeping both specialized for their workload
```

> **🧠 Principal Mindset:** Row vs columnar is the "fundamental theorem" of data storage. Once you deeply understand WHY columnar wins analytics (compression + I/O reduction + vectorization + statistics), you can predict performance characteristics of nearly every data system. "Will this scale to a billion rows?" depends on storage layout. "Why is this query slow?" often traces to row vs columnar mismatch. "Why is Snowflake fast?" is 70% columnar storage + 30% other optimizations. Staff engineers who master this one concept gain outsized intuition across the entire data stack.

---

[← Back to main](./README.md)
