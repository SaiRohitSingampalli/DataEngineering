# Topic 4 — Kafka & Streaming

> Section 4 of 10 · 60 pages in source PDF · pages 208–267

[← Back to main](./README.md)

---

Kafka &amp; Streaming: Principal Engineer Notes Kafka & Streaming Architect-Level Reference Principal / Staff Engineer Interview Notes Brokers · Partitions · ISR · Consumer Groups · Exactly-Once · Schema Registry · Kafka Streams · Flink · Topology Design How streaming actually works — and how to architect systems that don't lose data.

Kafka &amp; Streaming: Principal Engineer Notes How to use this document Kafka is straightforward to start using and devastatingly easy to misuse. Publish a message, consume it — done. The complexity hides in partitioning decisions, consumer group semantics, exactly-once guarantees, schema evolution, and operational failure modes. At staff/principal level, interviewers test whether you understand the distributed system Kafka is, not just the client API. ■ Principal mindset: Three staff-level signals: (1) you reason about partitions and consumer groups as primary design constraints, (2) you understand the difference between at-most-once, at-least-once, and exactly-once with operational precision, (3) you know when Kafka is the wrong tool — and what you'd use instead.

## PART I — FOUNDATIONS

#### 1. What Kafka Is (and Isn't)

#### 2. The Architecture: Brokers, Topics, Partitions

#### 3. The Log Abstraction

#### 4. Producers, Consumers & Consumer Groups

## PART II — STORAGE & REPLICATION

#### 5. The Partition Log on Disk

#### 6. Replication, ISR & Leader Election

#### 7. Retention, Compaction & Tiered Storage

#### 8. ZooKeeper, KRaft & Metadata

## PART III — PRODUCING & CONSUMING

#### 9. Producer Internals & Configuration

#### 10. Consumer Internals & Rebalancing

#### 11. Delivery Semantics: At-most/least/Exactly-Once

#### 12. Idempotent & Transactional Producers

## PART IV — SCHEMA & DATA

#### 13. Schema Registry: Avro, Protobuf, JSON Schema

#### 14. Schema Evolution & Compatibility Modes

#### 15. Serialization, Headers & Keys

## PART V — STREAM PROCESSING

#### 16. Kafka Streams: The Library Approach

#### 17. Apache Flink: The Engine Approach

#### 19. State, Time, Watermarks & Windows

## PART VI — PATTERNS & TOPOLOGIES

#### 20. Event-Driven Architecture Patterns

#### 22. Multi-Datacenter & Geo-Replication

## PART VII — OPERATIONS & SCALE

#### 23. Cluster Sizing & Capacity Planning

#### 24. Monitoring & Common Failure Modes

#### 25. Security: TLS, SASL, ACLs

#### 26. Anti-Patterns at Scale

## PART VIII — INTERVIEW PRACTICE

#### 27. The Staff-Level Streaming Interview

#### 28. War Stories

#### 29. Cheat Sheet & Mental Models

## PART I — FOUNDATIONS

#### 1. What Kafka Is (and Isn't)

1.1 The One-Sentence Definition Kafka is a distributed, replicated, append-only commit log that decouples producers from consumers via topics, allowing many independent consumers to read the same stream of events at their own pace.

```
1.2 What Kafka Is Not
KAFKA IS NOT:
  ✗ A message queue (RabbitMQ, SQS) — consumers don't compete for messages
                                       within a partition; they each get a copy
  ✗ A database — no random reads, no rich query, no secondary indexes
  ✗ An RPC system — async one-way, not request/response
  ✗ A workflow engine — no orchestration, no retries built in
  ✗ Real-time low-latency — best case ~1-5ms, typical 5-50ms
  ✗ Push-based — consumers PULL at their own pace
KAFKA IS:
  ✓ A durable, ordered log per partition
  ✓ A pub/sub broker with fan-out via consumer groups
  ✓ A backbone for event-driven architectures
  ✓ A "source of truth" for events between systems
  ✓ A buffer for impedance-mismatched producers and consumers
  ✓ A retention-based storage system (replay historical events)
```

```
1.3 The Mental Model
Think of Kafka as a SHARED LOG, not a message queue.
  PRODUCER → [append-only log] → CONSUMER A (reads from offset 100)
                              → CONSUMER B (reads from offset 50)
                              → CONSUMER C (reads from offset 200)
Each consumer maintains its own offset (position in the log).
Reading does NOT remove the message — the log is durable for the retention period.
This is the FUNDAMENTAL DIFFERENCE from traditional message queues.
```

```
1.4 When to Use Kafka
GOOD FIT:
  ✓ Event-driven microservices (many consumers per event)
  ✓ Data pipelines feeding multiple downstream systems
  ✓ Change data capture (CDC) from databases
  ✓ Activity tracking, metrics, logs aggregation
  ✓ Buffering between producer and slow consumer
  ✓ Replay-able event stream (audit, reprocessing)
  ✓ Stream processing source (Flink, Kafka Streams, Spark)
POOR FIT:
  ✗ Request/response RPC (use gRPC, HTTP)
  ✗ Task queues with strict per-message acks (use SQS, RabbitMQ)
  ✗ Long-term archival storage (use S3; Kafka tiered storage helps)
  ✗ Database-style queries on events (use ksqlDB or stream into a DB)
  ✗ Very few, very large messages (>1 MB each — Kafka prefers small + many)
```

```
Kafka &amp; Streaming: Principal Engineer Notes
  ✗ Strict ordered processing of millions of unrelated entities
     (partition cardinality / parallelism trade-off)
```

1.5 The Competitive Landscape (Brief) System Strength Where it fits Kafka Throughput, ecosystem, durability High-volume event log RabbitMQ Flexible routing, ACK semantics Task queues, complex topology SQS Managed, simple, AWS-native Best-effort task queue Pulsar Multi-tenancy, geo-replication Kafka-like with extras Kinesis AWS-managed, no ops AWS-native, less control NATS / JetStream Lightweight, low latency Edge, IoT, microservice msging Redpanda Kafka-API, no JVM, lower latency Kafka workloads on bare metal EventBridge Routing, schema AWS event router, not stream ■ Interview angle: Q: Why Kafka instead of RabbitMQ? Staff: Different abstractions. RabbitMQ is a smart broker: routes messages based on rules, manages acknowledgments per-message, fans out via exchanges. Great for task queues, RPC-like patterns. Kafka is a dumb log: append-only, partitioned, durable. The smarts live in the consumers. Better for high throughput (Kafka does millions/sec, RabbitMQ tens of thousands), replay (Kafka stores days/weeks), and fan-out to many independent consumers. Pick RabbitMQ for task distribution with per-message acks; pick Kafka for the event backbone of a distributed system.

#### 2. The Architecture: Brokers, Topics, Partitions

2.1 The Components ■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■ ■ KAFKA CLUSTER ■ ■ ■ ■ ■■■■■■■■■■■■ ■■■■■■■■■■■■ ■■■■■■■■■■■■ ■■■■■■■■■■■■ ■ ■ ■ Broker 1 ■ ■ Broker 2 ■ ■ Broker 3 ■ ■ Broker 4 ■ ■ ■ ■ ■ ■ ■ ■ ■ ■ ■ ■ ■ ■ Topic A ■ ■ Topic A ■ ■ Topic A ■ ■ Topic A ■ ■ ■ ■ P0 (L) ■ ■ P0 (F) ■ ■ P0 (F) ■ ■ ... ■ ■ ■ ■ P1 (F) ■ ■ P1 (L) ■ ■ P2 (F) ■ ■ ■ ■ ■ ■ P2 (L) ■ ■ ... ■ ■ P2 (F) ■ ■ ■ ■ ■ ■ ■ ■ ■ ■ ■ ■ ■ ■ ■ ■■■■■■■■■■■■ ■■■■■■■■■■■■ ■■■■■■■■■■■■ ■■■■■■■■■■■■ ■ ■ ■ ■ Controller (one elected broker; manages metadata) ■ ■ KRaft or ZooKeeper handles cluster coordination ■ ■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■ ▲ ▲ ■ produce (write) ■ consume (read) ■ ■ ■■■■■■■■■■■■ ■■■■■■■■■■■■ ■ Producer ■ ■ Consumer ■ ■■■■■■■■■■■■ ■■■■■■■■■■■■ L = Leader replica F = Follower replica Producers/consumers ALWAYS talk to the LEADER of a partition.

```
2.2 Topic
TOPIC = a category of events.
   Named (e.g., "orders", "payments", "page-views")
   Has N PARTITIONS (configurable at creation; usually changeable, with caveats)
   Has a REPLICATION FACTOR (typically 3)
   Has a RETENTION POLICY (time- or size-based)
Topics are CHEAP to create — many production clusters have thousands.
Common pattern: one topic per logical event type, not per service.
```

```
2.3 Partition
PARTITION = a single ordered, immutable, append-only log file.
   Each message gets a sequential OFFSET (0, 1, 2, ...)
   Order is guaranteed WITHIN a partition, NOT across partitions
   Each partition is REPLICATED to N brokers (replication factor)
   One replica is LEADER (handles all reads/writes), rest are FOLLOWERS
   Partition is the UNIT OF PARALLELISM in Kafka:
      - Producers can write to multiple partitions concurrently
      - Within a consumer group, each partition is read by exactly one consumer
      - So max consumer parallelism = number of partitions
```

```
2.4 The Cardinal Rule: Partition Count is a Design Decision
WHY IT MATTERS:
   Too few partitions → consumer parallelism cap → throughput bottleneck
   Too many partitions → broker overhead, longer leader election,
                          more open file handles, ZK/KRaft pressure
GUIDELINES:
   Start with: partitions = peak_throughput_MB_per_sec / per_partition_throughput
   Per-partition throughput is typically 10-100 MB/sec
```

```
Kafka &amp; Streaming: Principal Engineer Notes
   Plan for 2-3× peak throughput to allow rebalancing headroom
   Multiple of consumer count to avoid uneven assignment
   Hard cap: ~4000 partitions per broker (KRaft makes this softer)
REPARTITIONING IS PAINFUL:
   Can only INCREASE partition count (kafka-topics --alter)
   Increasing changes which partition a key hashes to → ordering broken
   Best to over-provision at topic creation: pick 1.5-2× expected future need
```

```
2.5 Broker
BROKER = a Kafka server process.
   Hosts one or more PARTITION REPLICAS (leaders or followers)
   Identified by broker.id (integer)
   Listens on configured ports (typically 9092 plaintext, 9093 TLS)
CONTROLLER:
   One broker is elected as CONTROLLER
   Responsible for: partition leader election, broker membership,
                     metadata propagation
   In ZooKeeper mode: ZK elects controller via ephemeral node
   In KRaft mode (Kafka 3.0+): brokers themselves form a Raft quorum
BOOTSTRAP SERVERS:
   The list of broker addresses clients use to find the cluster
   Client connects to any bootstrap broker → receives full metadata
   Production: use multiple addresses behind DNS / LB
■ Interview angle: Q: A topic has 10 partitions and 12 consumers in the same group. What happens?
Staff: 10 consumers each get exactly one partition, 2 consumers sit idle. Consumer count > partition count is a
configuration smell — wasted processes. The fix is either reduce consumers to ≤ partition count, or increase
partitions if more parallelism is genuinely needed (but that breaks ordering for keys that hash to different partitions
after the change). Better: design the partition count from the start based on max expected consumer parallelism ×
headroom.
```

#### 3. The Log Abstraction

```
3.1 Why "Log"
Kafka's core design — borrowed from database write-ahead logs — is the distributed log. Every event is
appended to the end of a partition's log file. Reads are sequential offsets. This simplicity is the source of
Kafka's throughput: sequential disk writes are nearly as fast as RAM.
APPEND-ONLY:
  Producers always append to the END of the log (offset = current_max + 1)
  No updates, no deletes (at the storage level; tombstones are special)
OFFSETS:
  Every record gets a monotonically-increasing offset within its partition
  Consumers track their position via offset
  Reading is "give me everything from offset X onward"
DURABILITY:
  Once written to disk and replicated, an offset is committed
  Consumers can read committed offsets safely
```

```
3.2 The Sequential I/O Advantage
WHY KAFKA IS FAST:
  1. SEQUENTIAL WRITES
     Modern HDDs: 200 MB/s sequential, 1 MB/s random
     SSDs:        sequential ~5× faster than random
     Kafka's append-only design hits sequential paths
  2. ZERO-COPY (sendfile)
     Linux sendfile() bypasses user-space copy on read
     Disk → page cache → socket buffer (kernel-only)
     No serialization on the consumer pull path
  3. BATCHING
     Producers batch records; brokers batch to disk; consumers fetch batches
     Amortizes per-message overhead
  4. PAGE CACHE EXPLOITATION
     Hot reads served from OS page cache, not disk
     Recent writes are already in cache when consumers fetch
  5. NO MESSAGE-LEVEL OPS
     No per-message ACK, no per-message state on broker
     Broker just appends bytes and serves byte ranges
```

```
3.3 Records, Batches, Segments
HIERARCHY:
  RECORD          — one logical message (key, value, headers, timestamp)
  RECORD BATCH    — group of records sent/written together (compression unit)
  LOG SEGMENT     — fixed-size file on disk (~1 GB default) containing many batches
  PARTITION LOG   — sequence of segment files
ON DISK (per partition):
  /var/kafka-logs/topic-name-0/
    00000000000000000000.log      (segment file: records 0..N)
    00000000000000000000.index    (offset → byte position index)
    00000000000000000000.timeindex (timestamp → offset index)
    00000000000123456789.log      (next segment after rollover)
    ...
    leader-epoch-checkpoint
    partition.metadata
```

Kafka &amp; Streaming: Principal Engineer Notes Active segment = current file being appended to. Old segments are sealed and eligible for retention / compaction.

```
3.4 The Offset
OFFSET = monotonically-increasing position within a partition
KEY POINTS:
   Offsets are PER-PARTITION (partition 0 offset 100 ≠ partition 1 offset 100)
   Offsets are STABLE — once assigned, never change
   Gaps possible: compaction removes records, but offset numbers stay
   Consumer commits offset to track "last processed"
SPECIAL POSITIONS:
  earliest    — first available offset (may be > 0 due to retention)
  latest      — next-to-be-written offset (end of log)
  committed   — offset stored by consumer group in __consumer_offsets
WHEN CONSUMER STARTS:
  auto.offset.reset = earliest   → start from beginning if no committed offset
                    = latest     → start from now (lose backlog)
                    = none       → fail if no committed offset (strict)
■ Principal mindset: Kafka's simplicity at the storage layer is what makes it so fast and so predictable. Most other
messaging systems try to be smart about per-message state — Kafka deliberately pushes that complexity to the
consumers. The broker just appends bytes; consumers track their own progress. This is a design choice, not a
limitation. Embrace it.
```

#### 4. Producers, Consumers & Consumer Groups

#### 4.1 The Producer

#### 1. Take a record (key, value, headers, optional partition/timestamp)

#### 2. Serialize key and value

```
  3. Determine target partition:
      Explicit partition → use it
      Key present → hash(key) % num_partitions (default: murmur2)
      No key → sticky partitioner (Kafka 2.4+) batches to one partition
```

#### 4. Buffer in memory (record accumulator)

#### 5. Background sender thread BATCHES records by (topic, partition)

#### 6. Send to LEADER broker for the partition

```
  7. Receive acknowledgment, retry or report success
KEY CONFIGS:
  bootstrap.servers   — list of brokers for discovery
  key.serializer / value.serializer
  acks                — 0, 1, all (durability)
  retries             — automatic retry count
  enable.idempotence  — exactly-once-per-partition
  compression.type    — none, gzip, snappy, lz4, zstd
  batch.size          — bytes per batch
  linger.ms           — wait time to accumulate batch
```

#### 4.2 The Consumer

#### 1. Subscribe to topic(s) or assign specific partitions

#### 2. Join a CONSUMER GROUP (identified by group.id)

#### 3. Group coordinator assigns partitions to consumer instances

#### 4. Consumer fetches records in batches from leader

#### 5. Process records

#### 6. Commit offset (manually or auto)

```
  7. On crash: rebalance reassigns partitions
KEY CONFIGS:
  bootstrap.servers
  group.id              — consumer group identifier
  key.deserializer / value.deserializer
  enable.auto.commit    — auto-commit offsets every N ms (DANGEROUS — see Ch 11)
  auto.offset.reset     — earliest / latest / none
  max.poll.records      — max records per poll() call
  session.timeout.ms    — max time consumer can be unresponsive before kick
  heartbeat.interval.ms — how often to ping coordinator
```

4.3 The Consumer Group CONSUMER GROUP = set of consumer instances cooperatively reading a topic. KEY RULES:

#### 1. Each partition is assigned to EXACTLY ONE consumer in the group

#### 2. Multiple consumers can subscribe to the same topic in DIFFERENT groups

#### 3. Adding/removing consumers triggers a REBALANCE

```
  4. Max parallelism within a group = number of partitions
EXAMPLES:
  Topic with 6 partitions:
    Group A: 1 consumer  → consumer reads all 6 partitions
    Group A: 3 consumers → each reads 2 partitions
    Group A: 6 consumers → each reads 1 partition (max parallelism)
    Group A: 8 consumers → 6 active, 2 idle
```

```
Kafka &amp; Streaming: Principal Engineer Notes
  Topic with 6 partitions:
    Group A: 3 consumers (each reads 2 partitions)
    Group B: 6 consumers (each reads 1)
    → both groups see ALL records; they're independent streams
```

```
4.4 The Rebalance Protocol
WHEN REBALANCE HAPPENS:
   Consumer joins or leaves the group
   Consumer session timeout (heartbeat missed)
   New partition added to a subscribed topic
   Consumer.assign() with manual partition changes
OLD PROTOCOL ("stop-the-world"):
  1. ALL consumers stop processing
```

#### 2. All revoke their partitions

#### 3. Coordinator reassigns

```
  4. All resume with new partitions
  Cost: brief outage (seconds) on every rebalance
INCREMENTAL COOPERATIVE REBALANCING (Kafka 2.4+):
```

#### 1. Only the AFFECTED partitions are reassigned

#### 2. Other consumers keep processing during rebalance

3. Much lower latency impact Config: partition.assignment.strategy=org.apache.kafka.clients.consumer.CooperativeStickyAssignor ■ Production scenario: A consumer group with 50 consumers and 200 partitions had a rebalance every few minutes — one consumer was deploying a slow new version, missing heartbeats. Each rebalance paused processing for 30 seconds across all 50 consumers. Backlog grew. Fix: enabled CooperativeStickyAssignor (partial rebalances) AND tuned heartbeat.interval.ms / session.timeout.ms so slow consumers had more grace. Stop-the-world rebalances are the most common operational pain at scale. ■ Interview angle: Q: You have a topic with 100 partitions and want 200 parallel consumers. How do you architect this? Staff: You can't — within one consumer group, you're capped at 100 (one consumer per partition). Options: (1) Increase partitions to 200, but ordering for existing keys changes; (2) Use two consumer groups in different applications (each gets 100 parallel readers); (3) Push compute downstream — consumer batches records and dispatches to a worker pool. Often (3) is right — the partition count is a Kafka design constraint, not your application's parallelism constraint. Use Kafka for ordered intake, then fan out to whatever parallelism your workload needs.

## PART II — STORAGE & REPLICATION

#### 5. The Partition Log on Disk

```
5.1 Segment File Layout
Each partition lives in its own directory:
  /kafka-logs/orders-3/
    00000000000000000000.log         ← segment with records starting at offset 0
    00000000000000000000.index       ← offset index
    00000000000000000000.timeindex   ← timestamp index
    00000000000012345678.log         ← next segment (after roll)
    00000000000012345678.index
    00000000000012345678.timeindex
    leader-epoch-checkpoint
    partition.metadata
ACTIVE SEGMENT = the file currently being appended to
SEGMENT ROLLS when:
   segment.bytes hit (default 1 GB)
   segment.ms elapsed (default 7 days)
ROLLING SEGMENTS = sealed; eligible for retention or compaction
```

```
5.2 Inside a Segment File
SEGMENT FILE CONTENT (binary, packed):
  ■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■
  ■ Record Batch 1 (header + N records)                     ■
  ■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■
  ■ Record Batch 2 (compressed if compression enabled)      ■
  ■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■
  ■ ...                                                     ■
  ■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■
RECORD BATCH HEADER includes:
  base offset, magic byte, attributes, max timestamp,
  producer id, producer epoch, base sequence, CRC32
RECORD includes:
  length, attributes, timestamp delta, offset delta,
  key length + key, value length + value,
  headers count + headers
KEY POINT: records are stored COMPRESSED AS A BATCH, not individually
  → consumers fetch compressed batches, decompress client-side
  → producer compression yields the biggest size win
```

```
5.3 Indexes
TWO INDEXES PER SEGMENT (sparse, memory-mapped):
OFFSET INDEX (.index)
  Maps offset → byte position in .log file
  Sparse: one entry per ~4 KB of log data
  Used for: "find the record at offset 12345678"
  Binary search the index, then sequential read from .log
```

```
Kafka &amp; Streaming: Principal Engineer Notes
TIMESTAMP INDEX (.timeindex)
  Maps timestamp → offset
  Sparse like offset index
  Used for: "find the first record with timestamp >= X"
  Powers consumer.offsetsForTimes() — start replay from a wall-clock time
WHY SPARSE: memory-efficient. Full index would be huge.
            Sequential read between sparse entries is cheap.
```

```
5.4 Zero-Copy Reads
TRADITIONAL READ PATH (4 copies):
  Disk → kernel buffer → app buffer → socket buffer → network
KAFKA ZERO-COPY (sendfile):
  Disk → page cache → [DMA to NIC]
  No app-buffer copy. No CPU spent on data movement.
REQUIREMENTS:
   Plaintext (TLS breaks zero-copy because data must be encrypted in app)
   No transformation between disk and wire (so no broker-side processing)
IMPACT: Kafka brokers achieve ~600 MB/s/disk on commodity hardware
        because the broker is barely involved in serving reads.
TLS BREAKS THIS — encrypted reads bring CPU back into the loop.
Modern: SSL with kTLS (kernel TLS) restores zero-copy for some workloads.
```

```
5.5 The Page Cache Strategy
KAFKA INTENTIONALLY RELIES ON THE OS PAGE CACHE.
   No application-level cache
   Writes go to page cache, fsync'd later (configurable)
   Reads served from page cache if hot
IMPLICATIONS:
   DON'T allocate huge JVM heaps — leave RAM for page cache
   Heap recommended: 4-8 GB; the rest of RAM → page cache
   A 64 GB broker should have ~6 GB heap, ~56 GB page cache
   Avoid sharing the broker host with other memory-heavy processes
   Watch for: cache eviction during large historical reads can hurt live consumers
■ Interview angle: Q: Why is Kafka so fast?
Staff: Four big ideas: (1) Sequential disk writes — append-only log, no random I/O; modern disks deliver 200+ MB/s
sequential. (2) Zero-copy reads via sendfile — broker barely touches data on the read path. (3) Batching —
producers batch records, brokers write batches as units, consumers fetch batches; amortizes overhead. (4) Page
cache reliance — broker doesn't implement an app-level cache; OS does it better. Combine those and you get
millions of records/sec on commodity hardware. Note: TLS breaks zero-copy unless you use kTLS, so there's a real
CPU cost to encryption.
```

#### 6. Replication, ISR & Leader Election

```
6.1 Replication Mechanics
Each partition has REPLICATION FACTOR replicas (typically 3).
   ONE is the LEADER — handles all reads/writes
   Others are FOLLOWERS — passively replicate from leader
REPLICATION FLOW:
```

#### 1. Producer sends record to leader

#### 2. Leader appends to its log

#### 3. Followers FETCH from leader (like consumers!)

#### 4. Once all in-sync followers have the record, it's COMMITTED

#### 5. Producer receives ack (if acks=all)

6. Committed offset becomes visible to consumers FOLLOWERS USE THE SAME FETCH PROTOCOL AS CONSUMERS. This is elegant: replication = an internal consumer.

```
6.2 In-Sync Replicas (ISR)
ISR = set of replicas that are CAUGHT UP to the leader.
A REPLICA IS IN-SYNC IF:
   It has fetched within replica.lag.time.max.ms (default 30 sec)
   It's not crashed
ISR INCLUDES THE LEADER ITSELF.
WHY ISR MATTERS:
   acks=all means "wait until all ISR have the record"
   Only ISR are eligible to become leader on failure
   A shrinking ISR is the #1 cluster health warning sign
CONFIG:
  min.insync.replicas = 2     (broker / topic-level)
   Producer with acks=all gets an error if ISR drops below this
   Combined with replication.factor=3 and acks=all → can lose 1 broker without data loss
```

```
6.3 The Durability Knobs
THREE LEVELS OF DURABILITY:
acks=0       — producer fires and forgets. May lose messages even before leader sees.
              Use only for: telemetry, lossy metrics where you really don't care.
acks=1       — producer waits for LEADER to write. May lose messages if leader
              crashes before followers replicate.
              Old default. Don't use for important data.
acks=all     — producer waits for all ISR to have the message.
              Combined with min.insync.replicas=2 (in a RF=3 setup), this
              guarantees no data loss even if one broker fails.
THE RIGHT PRODUCTION CONFIG:
  acks=all
  min.insync.replicas=2
  replication.factor=3
  enable.idempotence=true       (Ch 12)
  retries=Integer.MAX_VALUE     (idempotent retries are safe)
→ Survives one broker failure with zero data loss.
```

#### 6.4 Leader Election

#### 1. Controller detects (heartbeat timeout or ZK/KRaft notification)

#### 2. Controller picks a new leader from ISR

#### 3. Cluster metadata updated; clients refresh metadata on next request

```
  4. Brief unavailability (seconds) for that partition
PREFERRED LEADER:
   Each partition has a "preferred" replica (first in the replica list)
   Periodic auto.leader.rebalance moves leadership back to preferred
   Distributes leader load evenly across brokers
UNCLEAN LEADER ELECTION:
  unclean.leader.election.enable = false (RECOMMENDED, default in Kafka 2.0+)
   Without unclean election: if ALL ISR die, partition is UNAVAILABLE until ISR recovers
   With unclean election: a non-ISR replica can become leader → DATA LOSS
  Trade-off: availability vs durability. Default is durability.
```

```
6.5 The Replication Lag Story
HEALTHY: ISR includes all replicas, lag is milliseconds
WARNING SIGNS:
   ISR shrinks (follower removed because too slow)
   Under-replicated partition count > 0
   Replication lag (max - min offset across replicas) growing
CAUSES OF LAG:
   Slow disk on a broker
   Network saturation
   GC pause on a broker
   Hot partition (high throughput overwhelms one broker)
   Replication throttle (if intentionally configured)
OPS RESPONSE:
   Check broker CPU/disk/network on the lagging broker
   Check if hot partitions need re-balancing
   Avoid running compaction-heavy work during peak
■ Principal mindset: ISR is the single most important Kafka health metric. Watch it like a hawk. A shrinking ISR
predicts every Kafka outage. Under-replicated partition count > 0 for more than a minute should page on-call.
Static ISR with all replicas in sync = healthy cluster. Anything else = investigate.
```

#### 7. Retention, Compaction & Tiered Storage

```
7.1 Retention: How Long Does Kafka Keep Data?
Kafka deletes old data based on RETENTION POLICY.
TIME-BASED (default):
  retention.ms = 604800000      (7 days default)
  After this, segments are deleted
SIZE-BASED:
  retention.bytes = -1          (unlimited default)
  After log exceeds this size, oldest segments deleted
BOTH ARE PER-PARTITION:
  retention.bytes=10GB on a 100-partition topic → up to 1 TB total
CHECK INTERVAL:
  log.retention.check.interval.ms = 5 minutes
  Background thread checks and deletes expired segments
GRANULARITY: WHOLE SEGMENTS at a time, not individual records.
  Active segment is never deleted (only sealed segments are eligible).
```

```
7.2 Log Compaction: The Different Mode
COMPACTION = keep ONLY THE LATEST VALUE PER KEY.
  cleanup.policy = compact
  Initial log:
    (key=user-1, value={name: "Alice", v: 1}) at offset 0
    (key=user-2, value={name: "Bob",   v: 1}) at offset 1
    (key=user-1, value={name: "Alice", v: 2}) at offset 2
    (key=user-1, value={name: "Alice", v: 3}) at offset 3
    (key=user-2, value={name: "Bob",   v: 2}) at offset 4
  After compaction (background process):
    (key=user-1, value=v3) at offset 3
    (key=user-2, value=v2) at offset 4
KEY POINTS:
   Offsets are PRESERVED (gaps are OK)
   Order within a key is preserved
   NULL value = TOMBSTONE → eventually removed entirely
   A consumer reading from offset 0 sees the FULL HISTORY
   A consumer joining LATER sees only the compacted state
```

```
7.3 Why Compaction Matters: The Stream-Table Duality
COMPACTED TOPIC = a TABLE expressed as a stream of changes.
  Topic "user-profiles" with cleanup.policy=compact
    Producer publishes user profile updates keyed by user_id
    Consumer rebuilds current state by reading from beginning
  This is the foundation of:
     Kafka Streams' KTable (stateful tables backed by topics)
     CDC streams (changes to a database table)
     Configuration distribution (broadcast latest config)
     Materialized views
USE CASES:
  ✓ Current state of an entity (user profile, product inventory)
```

```
Kafka &amp; Streaming: Principal Engineer Notes
  ✓ Configuration that changes over time
  ✓ CDC of database tables
DON'T USE FOR:
  ✗ Events you need full history of (use time-based retention)
  ✗ Append-only logs
```

7.4 Tombstones & Deletion DELETE A KEY: publish a record with that key and NULL value. After compaction, the tombstone marks the key for deletion. After delete.retention.ms (default 24 hours), the tombstone itself is removed. WHY THE DELAY: Consumers reading old data should be able to see the tombstone to remove the key from their local state. 24 hours gives them time. CONFIG: delete.retention.ms = 86400000 (tombstone retention) min.cleanable.dirty.ratio = 0.5 (when to trigger compaction) segment.ms = 86400000 (force segment roll daily)

```
7.5 Mixed Cleanup ("compact,delete")
cleanup.policy = compact,delete
Behavior: compact AS WELL AS delete based on time/size
Useful when you want:
   Latest-per-key semantics (compaction)
   BUT also expire keys not seen recently (time-based)
Example: Active user sessions
   Keep latest session state per user (compact)
   But drop sessions inactive for 30 days (delete)
```

```
7.6 Tiered Storage (Kafka 3.6+)
TIERED STORAGE = move old segments from broker disk to remote storage (S3/GCS).
WITHOUT TIERED STORAGE:
  All retention data lives on broker local disk
  Long retention (90 days) → huge brokers, expensive
WITH TIERED STORAGE:
  Recent data on broker disk (fast access for live consumers)
  Old data archived to S3 / GCS
  Consumers reading historical data fetch transparently from remote
CONFIG (Kafka 3.6+):
  remote.log.storage.system.enable = true
  rsm.config.* (remote storage manager class & config)
BENEFITS:
  ✓ Effectively unlimited retention
  ✓ Cheaper storage tier
  ✓ Brokers stay small
CAVEATS:
  ✗ Reads from remote = slower (seconds vs ms)
  ✗ More moving parts to operate
  ✗ Network costs for replay
```

Kafka &amp; Streaming: Principal Engineer Notes ■ Interview angle: Q: When would you use log compaction vs time-based retention? Staff: Different semantic: time-based stores events (immutable history of what happened); compaction stores state (latest value per key). Use time-based for activity streams, transaction logs, anything where the history matters. Use compaction for entity state, configuration, CDC of database tables — anywhere a 'current value per key' is what you need. The stream-table duality is the core abstraction: a compacted topic IS a table, exposed as a stream. Kafka Streams, Materialize, and most CDC tools rely on this.

#### 8. ZooKeeper, KRaft & Metadata

```
8.1 The ZooKeeper Era (Pre-3.0)
ZOOKEEPER stored Kafka's metadata:
   Cluster membership (which brokers are alive)
   Topic / partition metadata (assignments, ISR, leaders)
   Configurations
   Controller election
   ACLs
CHALLENGES:
   Separate distributed system to operate
   Scaling cap: ~200K partitions per cluster (ZK metadata limits)
   Slow controller startup with many partitions
   Two clusters to monitor, secure, upgrade
ZK MODE IS DEPRECATED.
Kafka 3.3+ KRaft is production-ready.
Kafka 4.0 will remove ZooKeeper entirely (planned).
```

```
8.2 KRaft: Kafka Raft Metadata Mode
KRAFT = Kafka manages its OWN metadata using a Raft consensus protocol.
   A subset of brokers act as CONTROLLERS (Raft quorum)
   Controllers store metadata in a special internal topic
   No external ZooKeeper required
BENEFITS:
  ✓ Single cluster to operate (no ZK)
  ✓ Faster controller startup (no full ZK scan)
  ✓ Scales to millions of partitions
  ✓ Smaller failure domain
  ✓ Faster recovery
CONFIG:
  process.roles = broker,controller        # combined mode (dev)
  process.roles = controller               # dedicated (production)
  controller.quorum.voters = 1@host1:9093,2@host2:9093,3@host3:9093
PRODUCTION DEPLOYMENT:
  3 or 5 dedicated controllers (odd number for quorum)
  N broker-only nodes for data
  Controllers can be tiny machines
```

#### 8.3 Migration ZK → KRaft

#### 1. Add KRaft controllers running in "ZK migration mode"

#### 2. They sync state from ZK to KRaft

#### 3. Promote KRaft as authoritative

#### 4. Drain ZK

```
  5. Decommission ZooKeeper
CAUTION:
   Migration is one-way (no rollback)
   Test thoroughly on staging
   Many production clusters still on ZK as of 2024
   Migrating large clusters (10K+ partitions) takes hours of coordination
■ Principal mindset: ZK-to-KRaft is the biggest operational migration the Kafka community has seen. New clusters
in 2024+ should start KRaft. Migrating an existing cluster is straightforward but requires testing — and there's no
```

Kafka &amp; Streaming: Principal Engineer Notes rollback. If you're operating Kafka and haven't planned this migration, it's on your next-quarter roadmap whether you want it or not.

## PART III — PRODUCING & CONSUMING

#### 9. Producer Internals & Configuration

```
9.1 The Producer Flow
producer.send(record)
       ■
       ▼
   ■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■
   ■ 1. SERIALIZER                       ■
   ■    key → bytes, value → bytes       ■
   ■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■
       ■
       ▼
   ■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■
   ■ 2. PARTITIONER                      ■
   ■    Pick partition (key hash / sticky■
   ■                    / explicit)      ■
   ■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■
       ■
       ▼
   ■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■
   ■ 3. RECORD ACCUMULATOR (buffer)      ■
   ■    Per-(topic, partition) batch     ■
   ■    Wait for: batch.size OR linger.ms■
   ■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■
       ■
       ▼ (background SENDER thread)
   ■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■
   ■ 4. SEND TO LEADER BROKER            ■
   ■    With retries on transient errors  ■
   ■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■
       ■
       ▼
   ■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■
   ■ 5. CALLBACK on success/failure      ■
   ■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■
```

```
9.2 The Key Configurations
DURABILITY
  acks                    — 0 / 1 / all  (Ch 6)
  enable.idempotence      — true (recommended; Ch 12)
  retries                 — INT_MAX with idempotence
  max.in.flight.requests.per.connection
                          — 5 default; ≤5 with idempotence; 1 if strict order required
THROUGHPUT
  batch.size              — 16384 (16 KB default); larger = more throughput, more latency
  linger.ms               — 0 default; raise to 5-100 ms for better batching
  compression.type        — snappy / lz4 / zstd  (lz4 = best speed/ratio balance)
  buffer.memory           — 32 MB default; total producer-side buffer
LATENCY
  max.block.ms            — how long send() can block if buffer full (default 60s)
  request.timeout.ms      — broker response timeout (default 30s)
CORRECTNESS
```

```
Kafka &amp; Streaming: Principal Engineer Notes
  key.serializer / value.serializer
  partitioner.class       — usually default (sticky partitioner in 2.4+)
```

```
9.3 The Sticky Partitioner (Kafka 2.4+)
OLD DEFAULT (no key): round-robin across partitions
  Problem: small batches per partition, poor batching efficiency
STICKY PARTITIONER:
  All keyless records go to ONE partition until that batch ships
  Then pick a new sticky partition
  Result: larger batches, better compression, higher throughput
EVEN BETTER: STRICT UNIFORM STICKY (Kafka 3.3+)
  Same idea but also tries to balance load across partitions
```

```
9.4 Send Modes
FIRE-AND-FORGET (deprecated in spirit)
  producer.send(record);
  // never check result
  → Don't. Errors lost.
ASYNCHRONOUS WITH CALLBACK (recommended for throughput)
  producer.send(record, (metadata, exception) -> {
      if (exception != null) handleError(exception);
      else trackSuccess(metadata);
  });
SYNCHRONOUS (blocks per send)
  Future<RecordMetadata> f = producer.send(record);
  RecordMetadata md = f.get();   // BLOCKS until ack
  → Use only when you need strict ordering and acceptance per record.
    Throughput collapses (one in-flight request at a time).
```

9.5 The Compression Choice Codec Speed Ratio Best for none Fastest 1× Tiny messages, internal gzip Slow CPU ~6× High-throughput archival snappy Fast ~4× General-purpose default lz4 Fastest compressed ~4× Production default (Kafka) zstd Tunable ~6×+ Best ratio, modern CPUs ■ Tip: Default to lz4 or zstd. Compression is one of the biggest wins in throughput AND storage. Compression happens at the BATCH level — so larger batches compress better. Pair with reasonable linger.ms (10-50).

#### 10. Consumer Internals & Rebalancing

```
10.1 The Consumer Poll Loop
while (running) {
    ConsumerRecords<K,V> records = consumer.poll(Duration.ofMillis(100));
    for (ConsumerRecord<K,V> record : records) {
        process(record);
    }
    consumer.commitSync();  // or commitAsync, or rely on auto-commit
}
THIS LOOP IS THE HEART OF KAFKA CONSUMPTION.
KEY PROPERTIES:
   poll() returns up to max.poll.records (default 500) per call
   poll() ALSO sends heartbeats and triggers rebalances
   If you don't poll within max.poll.interval.ms, the consumer is kicked
```

```
10.2 The Two Timers That Kill You
session.timeout.ms (default 45 sec, K 3.0+)
  Max time between HEARTBEATS before consumer is kicked.
  Heartbeats sent in a BACKGROUND thread.
max.poll.interval.ms (default 5 min)
  Max time between POLLS before consumer is kicked.
  If processing one batch takes longer, you're kicked → rebalance.
YOUR PROCESSING TIME PER POLL MUST BE < max.poll.interval.ms
COMMON FIX FOR SLOW CONSUMERS:
   Reduce max.poll.records (process fewer per poll)
   Increase max.poll.interval.ms (give yourself longer)
   Move slow work to async processing, ack quickly
```

```
10.3 Offset Management
COMMITTED OFFSET = stored in internal __consumer_offsets topic.
  On consumer restart / rebalance, new owner resumes from committed offset.
AUTO-COMMIT (dangerous default):
  enable.auto.commit = true
  auto.commit.interval.ms = 5000
  → Every 5 sec, committed offsets updated based on current position
  → Risk: you commit BEFORE processing → message lost on crash
  → Or: you commit AFTER processing → duplicate on crash
MANUAL COMMIT (recommended):
  enable.auto.commit = false
  After processing a batch:
    consumer.commitSync();        // blocks, retries
    consumer.commitAsync();       // non-blocking, no retry
  At-LEAST-ONCE pattern (most common):
```

#### 1. Poll records

```
    2. Process them ALL
    3. commitSync()
    → On crash before commit: records re-delivered (idempotent processing required)
```

#### 10.4 Manual Partition Assignment

```
Kafka &amp; Streaming: Principal Engineer Notes
Two modes for joining a consumer:
SUBSCRIBE (group-aware)
  consumer.subscribe(List.of("orders"));
  → Joins consumer group, partitions assigned by coordinator
  → Rebalances on join/leave
ASSIGN (manual)
  consumer.assign(List.of(new TopicPartition("orders", 0)));
  → No consumer group; you manage partition assignment
  → Use for: testing, replay, specific partition processing,
    or systems that want explicit control (e.g., Flink)
```

10.5 Rebalance Listener consumer.subscribe(topics, new ConsumerRebalanceListener() { public void onPartitionsRevoked(Collection<TopicPartition> partitions) { // Called BEFORE losing partitions // Flush state, commit current offsets, clean up consumer.commitSync(); } public void onPartitionsAssigned(Collection<TopicPartition> partitions) { // Called AFTER getting new partitions // Initialize state, seek to specific offset if needed } }); CRITICAL FOR STATEFUL CONSUMERS:  Flink, Kafka Streams use rebalance listeners to checkpoint state  Without flushing on revoke: state corruption on next assignment ■ Production scenario: A consumer processed batches by writing to a database, then committing Kafka. Auto-commit was enabled. Consumer crashed mid-batch after auto-commit fired. On restart, it resumed past the lost batch — leaving rows missing in the database. The fix: disable auto-commit, commit only after successful DB write. Auto-commit is the default but rarely correct for production. ■ Interview angle: Q: A consumer is processing slowly and keeps getting kicked from the group. What's happening and how do you fix it? Staff: Almost certainly max.poll.interval.ms — processing one poll's worth of records takes longer than that interval, so the coordinator thinks the consumer is dead. Fixes in order of preference: (1) reduce max.poll.records so each batch is smaller and finishes faster; (2) increase max.poll.interval.ms if processing is genuinely long; (3) restructure so the poll loop hands off work to a worker pool and acknowledges quickly — but then YOU manage at-least-once semantics across the hand-off. Option 3 is the most robust for variable processing times.

#### 11. Delivery Semantics:

```
11.1 The Three Semantics
AT-MOST-ONCE  — record delivered 0 or 1 times (may be lost)
                Commit offset BEFORE processing.
                Crash before processing = lost.
                Used for: lossy metrics, telemetry where loss is OK.
AT-LEAST-ONCE — record delivered 1+ times (may be duplicated)
                Commit offset AFTER processing.
                Crash after processing but before commit = re-delivered.
                Used for: most production workloads (with idempotent processing).
EXACTLY-ONCE  — record delivered exactly 1 time (no loss, no duplicates)
                Requires producer transactions + consumer read_committed isolation
                + idempotent state updates downstream.
                Used for: financial transactions, exact counting,
                aggregations where duplicates matter.
```

  - 11.2 The Default Is At-Least-Once
- STANDARD PATTERN:
- 1. Producer with acks=all + enable.idempotence=true (no duplicates within partition)

#### 2. Consumer commits offset AFTER processing

```
  3. Downstream system handles duplicates idempotently
THIS COVERS 95% OF USE CASES.
DOWNSTREAM IDEMPOTENCY:
   Database: UPSERT by unique key (not blind INSERT)
   S3 / file: write to deterministic path (overwrite-safe)
   External API: pass idempotency key
   Aggregation: derive from current state, not increment-by-delta
```

```
11.3 When Exactly-Once Matters
EXACTLY-ONCE MAKES SENSE WHEN:
  ✓ Aggregations that can't be idempotent (counting, summing without dedup key)
  ✓ State machines in stream processing (Kafka Streams, Flink)
  ✓ Financial calculations (exact transfer amounts)
  ✓ End-to-end semantics across Kafka boundary
EXACTLY-ONCE IS OVERKILL WHEN:
  ✗ Downstream is naturally idempotent (DB UPSERT, S3 path-based write)
  ✗ Loss-tolerant streams (logs, metrics)
  ✗ Performance is critical and dedup is cheap downstream
OVERHEAD: ~10-20% throughput penalty vs at-least-once.
COMPLEXITY: more configs, more failure modes to understand.
```

```
11.4 The Configuration Recipes
AT-MOST-ONCE
  Producer: acks=0 or acks=1, retries=0
  Consumer: enable.auto.commit=true, commit before processing
  → Rare. Don't use unless you really, really need it.
AT-LEAST-ONCE (recommended default)
  Producer: acks=all, enable.idempotence=true, retries=INT_MAX
  Consumer: enable.auto.commit=false, commitSync() AFTER processing
```

```
Kafka &amp; Streaming: Principal Engineer Notes
  Downstream: idempotent writes
EXACTLY-ONCE (Kafka transactions)
  Producer: transactional.id=<unique>, enable.idempotence=true (implicit), acks=all
  Consumer: isolation.level=read_committed
  Producer wraps work in: beginTransaction / sendOffsetsToTransaction / commitTransaction
  → Atomic: produce + offset commit happen together
■ Principal mindset: Most teams say they need exactly-once but actually need at-least-once + idempotent
downstream. Real exactly-once requires Kafka transactions, the right consumer isolation level, AND state that
participates in the transaction. It's a full architectural commitment, not a config flag. When in doubt, default to
at-least-once + idempotency. It's simpler, faster, and harder to get wrong.
```

#### 12. Idempotent & Transactional Producers

```
12.1 Idempotent Producer
enable.idempotence = true
WHAT IT PREVENTS:
   Duplicates from PRODUCER retries within a single partition
   Out-of-order messages within a partition (despite retries)
HOW IT WORKS:
   Each producer gets a unique Producer ID (PID) on first send
   Each record has a sequence number per (topic, partition)
   Broker tracks last-seen sequence and dedups on retry
LIMITATIONS:
   Only prevents duplicates WITHIN a producer session
   Producer restart = new PID = potential duplicates across sessions
   Per-partition guarantee, not cross-partition
THE GOOD NEWS:
   Free (very small overhead)
   Required when retries > 0 with acks=all (otherwise out-of-order possible)
   In Kafka 3.0+, idempotence is ENABLED BY DEFAULT
```

```
12.2 Transactional Producer
TRANSACTIONS = atomic write across multiple partitions / topics +
               atomic commit of consumer offsets.
producer.initTransactions();                  // once at startup
while (more work) {
    producer.beginTransaction();
    producer.send(record1);                   // to topic A
    producer.send(record2);                   // to topic B
    producer.sendOffsetsToTransaction(        // commit consumer offsets
        consumerOffsetsMap, consumerGroupMetadata);
    producer.commitTransaction();             // all-or-nothing
    // on error: producer.abortTransaction();
}
GUARANTEES:
   All records in the transaction are committed atomically
   Consumer-offset commits are part of the transaction
   Other consumers with isolation.level=read_committed see ALL or NOTHING
```

```
12.3 Read-Committed Consumer
isolation.level = read_committed
CONSUMER BEHAVIOR:
   Reads only records from committed transactions
   Aborted transaction records are SKIPPED entirely
   Adds latency: consumer waits to see commit/abort marker before delivering
   The Last Stable Offset (LSO) is the high-water mark for read_committed
DEFAULT: read_uncommitted (sees all records, including aborted)
```

#### 12.4 The Read-Process-Write Pattern

```
Kafka &amp; Streaming: Principal Engineer Notes
  Consumer reads from topic A
       ■
       ▼
  Application processes / transforms
       ■
       ▼
  Producer writes to topic B
       ■
       ▼
  Consumer commits offset on topic A
EXACTLY-ONCE VERSION (Kafka transactions):
  Wrap in transaction:
     Reads (consumer is in same group as the txn-id)
     Writes to topic B
     Offset commit
  → Either ALL succeed or ALL fail. No partial state.
This is the foundation of exactly-once stream processing in
Kafka Streams and Flink.
```

```
12.5 Operational Gotchas
 Transactions add LATENCY (~10-100ms per transaction)
 transactional.id must be UNIQUE per producer instance
 producer.id is recycled on broker (transactional.id is the stable identity)
 Long-running transactions block consumers (LSO doesn't advance)
 transaction.timeout.ms limits how long a transaction can run
 Stale transactional.id from a previous host can cause "ProducerFenced" errors
  → Resolve by ensuring transactional.id is unique + reset cleanly on restart
■ Interview angle: Q: Explain Kafka exactly-once semantics.
Staff: 'Exactly-once' is shorthand for atomic read-process-write across Kafka's boundaries. Three pieces. (1)
Idempotent producer prevents duplicate writes within a partition from retries — usually already on. (2) Transactional
producer atomically writes to N output partitions AND commits consumer offsets as one operation. Either all succeed
or none. (3) read_committed consumer skips records from aborted transactions. With those three, the consume →
process → produce → commit-offset loop is atomic. The catch: it's exactly-once within Kafka. If your processing
writes to a database, the database has to be in the transaction too — which Kafka can't do directly. So in practice
exactly-once still requires idempotency at the external boundaries.
```

## PART IV — SCHEMA & DATA

#### 13. Schema Registry: Avro, Protobuf, JSON

```
13.1 Why You Need a Schema Registry
Kafka values are bytes. Without coordination, every producer and consumer must agree on the format — and
any unilateral change breaks downstream. A schema registry centralizes schemas, validates compatibility
on registration, and lets producers/consumers reference schemas by ID.
WITHOUT SCHEMA REGISTRY:
   Producer changes value format → consumers crash silently
   New consumer can't infer schema from bytes
   Schema drift across teams; documentation is the only source of truth
   Migrations are full org-wide stop-the-world events
WITH SCHEMA REGISTRY:
   Producer registers schema; gets schema ID
   Message wire format: <magic byte><schema ID><payload>
   Consumer fetches schema by ID, deserializes
   Registry enforces compatibility on schema evolution
```

13.2 The Big Three Formats Format Strengths Weaknesses Avro Mature, compact, schema in registry Less ecosystem outside JVM Protobuf Google-standard, multi-language Schema verbose, less Kafka-native JSON Schema Human-readable, simple Larger payload, weaker types

```
13.3 Avro Example
// Schema (Avro IDL or JSON)
{
  "type": "record",
  "name": "Order",
  "namespace": "com.example.events",
  "fields": [
    {"name": "order_id",    "type": "string"},
    {"name": "user_id",     "type": "long"},
    {"name": "amount_cents","type": "long"},
    {"name": "created_at",  "type": {"type":"long","logicalType":"timestamp-millis"}}
  ]
}
// Producer config
props.put("value.serializer", KafkaAvroSerializer.class);
props.put("schema.registry.url", "http://schema-registry:8081");
// Wire format:
// [magic 0x00][4-byte schema ID][Avro-encoded payload]
// Consumer fetches schema by ID, deserializes appropriately
```

```
13.4 The Schema Registry Server
CONFLUENT SCHEMA REGISTRY (de facto standard):
   REST API on port 8081
   Backed by an internal Kafka topic (_schemas)
   Stateless (state in Kafka), can scale horizontally
   Supports Avro, Protobuf, JSON Schema
ALTERNATIVES:
  Apicurio (Red Hat) — supports more formats, integrates with Kafka Connect
  AWS Glue Schema Registry — Confluent-compatible, AWS-managed
  Karapace (Aiven) — open-source Confluent-compatible
OPERATIONS:
   Registry MUST be highly available
   Producers/consumers FAIL if registry is unreachable
   Cache schemas client-side; many do
   Monitor: schema versions per subject, registration failure rate
```

```
13.5 Subject Naming
SUBJECT = name under which a schema is registered
Multiple schema VERSIONS can exist per subject (evolution).
NAMING STRATEGIES:
  TopicNameStrategy (default)
    subject = <topic-name>-value or <topic-name>-key
    → One schema per topic; simple
  RecordNameStrategy
    subject = fully-qualified record name (e.g., com.example.Order)
    → Multiple record types in one topic
  TopicRecordNameStrategy
    subject = <topic>-<record-name>
    → Multiple record types per topic, but topic-scoped
TYPICAL: TopicNameStrategy; switch only if you need polymorphic topics.
■ Principal mindset: A schema registry is not optional at staff scale. Without one, schema drift is the leading cause
of streaming pipeline outages — invisible until a consumer crashes in production. Pick one (Confluent, Apicurio,
Glue), make it tier-1 critical, monitor it, and enforce that every topic with a non-trivial payload has a registered
schema.
```

#### 14. Schema Evolution & Compatibility Modes

14.1 The Compatibility Modes Mode Producer can use Consumer can use BACKWARD (default) New schema Old + new schemas BACKWARD_TRANSITIVE New schema All old + new FORWARD Old schema New schema FORWARD_TRANSITIVE All old + new New schema FULL Same as below Same as above FULL_TRANSITIVE BACKWARD_TRANSITIVE + FORWARD_TRANSITIVE NONE Anything Anything (DANGER)

```
14.2 BACKWARD Compatibility (the common default)
BACKWARD: New schema can read data written with old schema.
ALLOWED CHANGES (for Avro):
  ✓ Add a NEW field with a default value
  ✓ Remove a field that had a default
  ✓ Change a field's docstring or alias
DISALLOWED:
  ✗ Add a required field (no default)
  ✗ Remove a required field
  ✗ Rename a field
  ✗ Change a field's type
USE CASE:
  Consumers updated first (handle new + old schema), then producers updated.
```

```
14.3 FORWARD Compatibility
FORWARD: Old schema can read data written with new schema.
ALLOWED:
  ✓ Add a required field (consumer ignores unknown)
  ✓ Remove a field with a default (consumer fills in default)
USE CASE:
  Producers updated first, consumers later.
  Less common — usually you update consumers first.
```

#### 14.4 The Evolution Playbook

#### 1. Update schema: add field WITH DEFAULT VALUE

#### 2. Register new schema (compat check passes)

#### 3. Deploy consumers (handle both old + new — easy, they just see the default)

#### 4. Deploy producers (start populating the field)

#### 1. Stop using the field in producers (still populate but don't depend on it)

#### 2. Update consumers to not require it

#### 3. Update schema: mark field deprecated or remove default

#### 4. After all old messages aged out: remove field from schema

#### 1. Add new field with new name + default

#### 2. Producers populate BOTH

#### 3. Consumers migrate to new name

```
  4. Eventually deprecate old name
BREAKING CHANGE:
  Create a new topic with new schema (v2). Migrate consumers. Eventually deprecate v1.
```

```
14.5 Schema Governance
ORG-LEVEL POLICIES:
   Every topic has a registered schema (no schemaless topics in prod)
   Compatibility mode is BACKWARD or FULL by default
   Schema changes go through code review
   Schema registry change events emit audit log
CI INTEGRATION:
   Compile schema → check registry compatibility BEFORE merge
   Reject PRs that introduce breaking changes without explicit migration plan
   Track schema usage (which consumer groups read which subjects)
TOOLING:
   Buf (for Protobuf)
   Confluent Schema Registry CLI
   Apicurio's compatibility checker
■ Production scenario: A producer team renamed a field from 'amount' to 'amount_cents' — registered as a new
schema version, BACKWARD compatibility was set, registry accepted it (the old field was removed, new one added
with default). All consumers running the OLD code now read the new messages and saw a NULL amount because
their old schema didn't know about amount_cents. Result: every order looked like $0. The compatibility check didn't
catch it because the rename is syntactically a remove + add. Fix: enforce stricter review for field-removal changes;
consider FULL compatibility for high-stakes topics.
```

#### 15. Serialization, Headers & Keys

```
15.1 The Record Anatomy
KAFKA RECORD:
   KEY (bytes, nullable)       — determines partition, used in compaction
   VALUE (bytes, nullable)      — the payload
   TIMESTAMP (long)             — when produced (or log-append time)
   HEADERS (list<key=value>)    — metadata, since 0.11
   OFFSET                       — assigned by broker
   PARTITION                    — chosen by producer
```

#### 15.2 The Key

#### 1. Determine PARTITION via hash(key) % num_partitions (default partitioner)

#### 2. All records with same key → same partition → ordered → same consumer

```
  3. Used for log compaction (latest-per-key)
CHOOSING A KEY:
  ✓ Entity ID (user_id, order_id) → events for same entity stay together
  ✓ Hash key in advance to spread load if natural key is skewed
  ✓ Tenant ID for multi-tenant systems
ANTI-PATTERNS:
  ✗ Random UUID as key → no benefit; same as no key
  ✗ Timestamp as key → terrible skew (all 'now' records to same partition)
  ✗ Low-cardinality key (boolean, status) → only N partitions ever used
NULL KEY: round-robin / sticky partitioner. No ordering guarantee.
```

```
15.3 The Value
ANYTHING. Bytes. Most common formats:
   Avro (schema registry)
   Protobuf (schema registry)
   JSON (often without schema; risky)
   JSON Schema (with registry)
   String / raw bytes (rare; use a schema)
SIZE GUIDELINES:
   Typical: 100 B - 100 KB
   Hard limit: message.max.bytes (default 1 MB, can be raised)
   Going beyond ~10 MB: put the payload elsewhere (S3) and pass a reference
    Kafka is not built for large messages — they kill throughput, fill memory, "
    "and slow replication
```

```
15.4 Headers
HEADERS = list of (name, byte-value) pairs, like HTTP headers.
USE FOR:
  ✓ Tracing context (trace_id, span_id)
  ✓ Source system metadata (region, env)
  ✓ Schema version / type hints
  ✓ Routing info that should NOT be part of the payload
EXAMPLE:
  ProducerRecord<String, String> r = new ProducerRecord<>("orders", key, value);
  r.headers().add("trace-id", traceId.getBytes());
  r.headers().add("source-region", "us-east-1".getBytes());
WHY NOT PUT IN VALUE?
```

```
Kafka &amp; Streaming: Principal Engineer Notes
   Headers are accessible without deserializing the value
   Headers don't need a schema
   Consumers can route / filter based on headers without parsing payload
```

```
15.5 Wire Format with Schema Registry
CONFLUENT WIRE FORMAT (de facto):
  ■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■
  ■ MAGIC    ■ SCHEMA ID      ■ PAYLOAD                 ■
  ■ (1 byte) ■ (4 bytes, BE)  ■ (Avro/Proto/JSON bytes) ■
  ■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■
  magic byte = 0x00 (Confluent)
  schema ID = registry lookup key
ON CONSUME:
```

#### 1. Read first byte (verify magic)

#### 2. Read next 4 bytes (schema ID)

#### 3. Fetch schema from registry by ID (cache!)

4. Deserialize payload with that schema ■ Interview angle: Q: Should you put the schema version inside the value, or use Schema Registry, or both? Staff: Schema Registry. Embedding the version in the value works but you've now invented a custom protocol — every consumer needs to parse the envelope and look up the schema somewhere. Schema Registry standardizes this: wire format embeds the schema ID, registry resolves to schema. Headers are NOT for schema version (consumers shouldn't have to fetch the value bytes AND headers AND a registry — Registry already gives you the schema ID lookup). Use headers for tracing, source metadata, routing — not for schema info.

## PART V — STREAM PROCESSING

#### 16. Kafka Streams: The Library Approach

16.1 What Kafka Streams Is Kafka Streams is a Java library (no separate cluster) that lets you write stream processing as a Java/Kotlin/Scala application. The application is a normal JVM process; it consumes from Kafka, processes, produces to Kafka. Parallelism = partition count × instance count. StreamsBuilder builder = new StreamsBuilder(); KStream<String, Order> orders = builder.stream("orders"); KStream<String, Order> bigOrders = orders.filter( (key, order) -> order.getAmountCents() > 100_000 ); bigOrders.to("high-value-orders"); KTable<String, Long> orderCounts = orders .groupByKey() .count(); orderCounts.toStream().to("order-counts-per-user"); KafkaStreams streams = new KafkaStreams(builder.build(), config); streams.start();

```
16.2 The Streams Topology
TOPOLOGY = DAG of processing nodes:
  SOURCE → PROCESSOR(s) → SINK
  Source: reads from a Kafka topic
  Processor: filter, map, groupBy, join, aggregate, window
  Sink: writes to a Kafka topic
  STATE STORE: optional per-processor state (RocksDB-backed)
EACH APPLICATION INSTANCE:
   Runs the SAME topology
   Gets assigned a SUBSET of partitions (one per partition)
   Maintains its OWN state stores (for its partitions)
   Coordinates via Kafka consumer-group protocol
SCALING:
  Add another instance → coordinator rebalances → partitions redistribute
  State stores rehydrate from internal CHANGELOG topics (compacted)
```

```
16.3 KStream vs KTable
KStream — sequence of events (each record is a new event)
  Append-only. Same key can appear many times.
  E.g., "user clicked", "order placed".
KTable — sequence of updates (each record is the latest value per key)
  Like a database table represented as a stream of changes.
```

```
Kafka &amp; Streaming: Principal Engineer Notes
  Backed by a compacted topic.
  E.g., "user profile", "current inventory".
GlobalKTable — KTable replicated to ALL instances (broadcast)
  Use for: small reference data joined to many streams.
CONVERSIONS:
  stream.toTable()              → latest value per key
  table.toStream()              → emit change events
  stream.groupByKey().count()   → KTable of counts per key
```

```
16.4 Stateful Operations
STATE STORES (default: RocksDB on local disk):
   Each instance maintains stores for its assigned partitions
   Changelog topic in Kafka is the source of truth
   On crash / rebalance: state rehydrated from changelog
STATEFUL OPERATIONS:
  groupByKey() then aggregate() / reduce() / count()
  join(KStream, KStream)         — windowed
  join(KStream, KTable)          — non-windowed
  join(KTable, KTable)           — non-windowed
  windowedBy(TimeWindows.of(...))
EXAMPLE:
  KTable<Windowed<String>, Long> hourlyOrderCount = orders
      .groupByKey()
      .windowedBy(TimeWindows.of(Duration.ofHours(1)))
      .count();
```

```
16.5 Why Kafka Streams
GOOD FIT:
  ✓ Java/JVM shop already using Kafka
  ✓ Self-contained microservices doing stream processing
  ✓ Want exactly-once with low operational overhead
  ✓ Per-key state, modest scale (millions of keys, GBs of state)
BAD FIT:
  ✗ Non-JVM language
  ✗ Need a SQL interface (use ksqlDB or Flink SQL)
  ✗ Very large state (TB+) or extreme parallelism — Flink scales better
  ✗ Complex windowing or event-time semantics — Flink is more powerful
  ✗ Multi-source joins beyond Kafka — Flink connectors win
```

17. Apache Flink: The Engine Approach 17.1 Kafka Streams vs Flink — the Big Picture Aspect Kafka Streams Flink Deployment Embedded library in your app Separate cluster Languages Java/JVM Java, Scala, Python, SQL Sources/sinks Mostly Kafka Kafka, Kinesis, JDBC, files, ... State backend RocksDB (local + changelog) RocksDB, in-mem, RocksDB+S3 Event time Supported First-class, more powerful Exactly-once Yes (Kafka transactions) Yes (checkpoints + 2PC) Scale Hundreds of partitions Tens of thousands; PB-scale Ops complexity Low (just your app) High (Flink cluster) Best for JVM apps, micro-services Big pipelines, complex logic

```
17.2 Flink Architecture
■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■
■                    JOBMANAGER                          ■
■   Schedules tasks                                     ■
■   Coordinates checkpoints                             ■
■   HA: ZK or K8s-based leader election                 ■
■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■
                          ■ dispatches
       ■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■
       ▼                  ▼                  ▼
   ■■■■■■■■■■■■      ■■■■■■■■■■■■      ■■■■■■■■■■■■
   ■TASKMANAGER■     ■TASKMANAGER■     ■TASKMANAGER■
   ■  slots  ■      ■  slots  ■      ■  slots  ■
   ■  runs   ■      ■  runs   ■      ■  runs   ■
   ■   tasks  ■      ■   tasks  ■      ■   tasks  ■
   ■  state  ■      ■  state  ■      ■  state  ■
   ■■■■■■■■■■■■      ■■■■■■■■■■■■      ■■■■■■■■■■■■
                                ▼
                       ■■■■■■■■■■■■■■■■
                       ■ STATE BACKEND■
                       ■ (S3 / HDFS)  ■ — for checkpoints
                       ■■■■■■■■■■■■■■■■
```

```sql
17.3 Flink's Key Concepts
DATASTREAM API:
  DataStream<Order> orders = env.addSource(new FlinkKafkaConsumer<>(...));
  DataStream<Order> highValue = orders.filter(o -> o.amount > 100_000);
  highValue.addSink(new FlinkKafkaProducer<>(...));
TABLE API / FLINK SQL:
  CREATE TABLE orders (...) WITH ('connector'='kafka', ...);
  SELECT TUMBLE_END(event_time, INTERVAL '1' HOUR) AS hour,
         user_id,
         SUM(amount_cents) AS total
  FROM orders
  GROUP BY TUMBLE(event_time, INTERVAL '1' HOUR), user_id;
CHECKPOINTS:
   Periodic snapshots of operator state to S3/HDFS
```

```
Kafka &amp; Streaming: Principal Engineer Notes
   Asynchronous, barrier-based (Chandy-Lamport variant)
   Failure: restore from latest checkpoint, replay Kafka from offset
SAVEPOINTS:
   Manual checkpoints — used for upgrades, scale changes
```

```
17.4 When to Pick Flink
PICK FLINK WHEN:
  ✓ Multi-source pipelines (Kafka + JDBC + files)
  ✓ Complex event-time processing (event-time windows, late events)
  ✓ Large state (TB of keyed state with RocksDB + S3 backend)
  ✓ SQL-first stream processing (Flink SQL)
  ✓ Multi-language teams (Python, Scala, Java)
  ✓ Pipelines with intricate windowing (session, hopping, custom)
STAY ON KAFKA STREAMS WHEN:
  ✓ Pure Kafka in / Kafka out
  ✓ JVM-only team
  ✓ Want zero extra infra
  ✓ Modest scale and state
■ Interview angle: Q: Kafka Streams or Flink — how do you choose?
Staff: Operational scope. Kafka Streams is a library — your app has streaming logic, period. Deploy via your normal
app deploy process. Great for microservice-shaped stream processing in JVM. Flink is a platform — you operate a
Flink cluster, jobs are submitted to it. Worth it when you have multiple stream jobs, complex windowing, multi-source
pipelines, or large state. The dividing line: do you want streaming as a feature of your service, or as a shared
platform? At a small team, Streams. At a 20+ stream-engineer org, Flink. There's no middle ground that's stable
long-term.
```

```sql
18.1 What ksqlDB Is
ksqlDB is Confluent's SQL-on-Kafka engine. You define streams and tables with CREATE STREAM /
CREATE TABLE, then write SQL queries that continuously execute. Built on top of Kafka Streams; ships as a
separate server.
CREATE STREAM orders (
    order_id  STRING KEY,
    user_id   BIGINT,
    amount    BIGINT,
    event_ts  TIMESTAMP
) WITH (KAFKA_TOPIC='orders', VALUE_FORMAT='AVRO');
CREATE TABLE high_value_users AS
  SELECT user_id, COUNT(*) AS n_orders
  FROM orders
  WHERE amount > 100000
  GROUP BY user_id
  EMIT CHANGES;
-- pull query (point lookup)
SELECT * FROM high_value_users WHERE user_id = 42;
18.2 ksqlDB Trade-offs
GOOD FOR:
  ✓ Quick SQL prototypes on Kafka data
  ✓ Real-time materialized views
  ✓ Stream-table joins via SQL
  ✓ Teams comfortable with SQL but not Java
WEAKNESSES:
  ✗ Less expressive than Flink SQL
  ✗ Confluent-tied
  ✗ Operational overhead (ksqlDB cluster + Kafka cluster)
  ✗ Performance: each ksqlDB server is essentially a Kafka Streams app
IN PRACTICE:
  Many teams adopted ksqlDB for quick wins, then migrated to Flink SQL
  as needs grew. Flink SQL is more standard and engine-portable.
```

#### 19. State, Time, Watermarks & Windows

```
19.1 Event Time vs Processing Time
PROCESSING TIME — wall-clock when the engine sees the record
  Easy: just use system time
  But: results depend on when records arrive, not when events happened
  Late or out-of-order events produce wrong answers
EVENT TIME — timestamp embedded in the record (when the event actually happened)
  Correct: same input → same output regardless of arrival time
  Hard: requires knowing when to "close" a time window (events can arrive late)
THIS IS WHY WATERMARKS EXIST.
```

```
19.2 Watermarks
WATERMARK W(t) = "no more events with timestamp < t will arrive"
   A statement about what the engine has seen, not a hard guarantee
   Computed from observed event times + a configurable allowed lateness
   E.g., "max event time seen - 5 minutes"
WATERMARK USES:
   Tells the engine when to FINALIZE time windows
   Events arriving after the watermark are "late"
    Options: drop, side-output, allowed-lateness re-computation
CONFIGURING:
  Bounded out-of-orderness: events arrive at most N seconds out of order
  Maximum lateness: events can be up to M minutes late before being dropped
```

```
19.3 Window Types
TUMBLING — fixed-size, non-overlapping
  [0-1min] [1-2min] [2-3min] ...
  Each event belongs to exactly ONE window
  Use: hourly counts, daily aggregates
HOPPING (sliding) — fixed-size, overlapping
  [0-5min, step=1min]: [0-5], [1-6], [2-7], ...
  Each event belongs to MULTIPLE windows (window_size / step)
  Use: moving averages, rolling counts
SESSION — variable-size, defined by gap of inactivity
  Events for a user; new session starts after N minutes idle
  Use: user activity sessions, application sessions
GLOBAL — single window, manually triggered (advanced)
  Use: custom triggering logic
```

19.4 Late Events Handling WHEN AN EVENT ARRIVES WITH event_time < current_watermark: OPTION 1: DROP Default. Simple. Lossy. OPTION 2: SIDE-OUTPUT Route to a "late events" topic for separate handling Common pattern: re-aggregate or alert on late arrivals OPTION 3: ALLOWED LATENESS

```
Kafka &amp; Streaming: Principal Engineer Notes
  Window stays "open" for grace_period beyond the watermark
  Late events trigger window re-emit (downstream must be idempotent)
  Bounded memory usage (windows can be discarded after grace_period)
```

```
19.5 State Management
STATE BACKENDS:
  In-memory: fast, bounded by JVM heap
  RocksDB:   on local disk, can be 100GB+ per task
  RocksDB + S3 incremental: best for huge state with checkpointing
CHECKPOINTING:
  Periodic snapshot of all state to durable storage
  On crash: restore from latest checkpoint, replay from saved Kafka offset
  Result: at-least-once or exactly-once (with 2PC) state recovery
STATE TUNING:
   Minimize state size (use TTL, expire old entries)
   Use RocksDB for large state
   Tune checkpoint interval: more often = less re-process on failure,
                              less often = less I/O overhead
■ Principal mindset: Event-time processing with watermarks is the single biggest correctness investment in stream
processing. Processing-time semantics produce subtly wrong results that pass tests in dev. Event-time produces
correct results that survive late data, replays, and reprocessing. Always design new streaming pipelines with
event-time as the default — even if it means more work to embed timestamps in events upstream.
```

## PART VI — PATTERNS & TOPOLOGIES

#### 20. Event-Driven Architecture Patterns

```
20.1 The Common Topologies
1. FAN-OUT
   One producer → N consumer groups, each independent
   Classic Kafka use case. Adding a new consumer doesn't affect others.
2. FAN-IN
   N producers → one topic → consumer aggregates
   Watch for partition skew (all producers writing to same key/partition)
3. PIPELINE
   Topic A → processor → Topic B → processor → Topic C
   Each stage scales independently. Backpressure via topic buffering.
4. CDC (Change Data Capture)
   Database changes → Kafka topic → many downstream systems
   Debezium reads MySQL/Postgres/Mongo write-ahead logs
5. EVENT SOURCING
   Application writes events (not state) to Kafka
   State derived by replaying events; full audit history
6. CQRS (Command-Query Responsibility Segregation)
   Commands → events → Kafka → materialized read models
   Reads served from optimized views, writes from event log
```

```
20.2 Event-Driven Microservices
EACH SERVICE:
   Owns its own data (DB)
   Emits EVENTS for state changes (not commands)
   Subscribes to events from upstream services
   Maintains LOCAL read models if needed
BENEFITS:
  ✓ Decoupling — service A doesn't know about service B
  ✓ Resilience — slow consumer doesn't break producer
  ✓ Replay — new service can read history
  ✓ Audit — events are the truth
CHALLENGES:
  ✗ Eventual consistency (no synchronous read-your-write)
  ✗ Schema evolution coordination
  ✗ Hard to query across services
  ✗ Operational complexity (more moving parts)
```

20.3 The Read Model Pattern SERVICE A: writes orders to its database, emits OrderPlaced event SERVICE B (reporting): subscribes to OrderPlaced events maintains LOCAL view of orders for fast queries SERVICE C (analytics): subscribes to OrderPlaced events

```
Kafka &amp; Streaming: Principal Engineer Notes
                         loads into warehouse
  → A is the system of record
  → B and C have read models tailored to their needs
  → They don't query A's database (no coupling)
  → Eventual consistency: B and C see events some milliseconds after A commits
→ This is the fundamental shape of event-driven architectures.
■ Interview angle: Q: Walk me through how you'd build a 'shopping cart' service in an event-driven architecture.
Staff: Shopping cart owns its state — likely in a transactional DB. Cart emits events for: ItemAdded, ItemRemoved,
CartAbandoned, CartCheckedOut. Other services consume those events: Inventory reserves on Add/Remove,
Recommendation tracks for personalization, Notifications sends abandonment reminders, Analytics loads to
warehouse. Cart doesn't know any of these exist — it just publishes. Crucially: cart writes its DB AND publishes
events atomically via the OUTBOX pattern — a same-transaction insert into an outbox table, which a separate
process reads and produces to Kafka. Without that, you risk events without DB changes or vice versa.
```

```
21.1 Change Data Capture (CDC)
CDC = capture INSERTs/UPDATEs/DELETEs from a database and publish as events.
HOW:
   Read the database's write-ahead log (WAL / binlog)
   Decode into structured change events
   Publish to Kafka
TOOLS:
   DEBEZIUM (open-source, most popular)
    Connectors for: Postgres, MySQL, MongoDB, SQL Server, Oracle, Cassandra
   Maxwell (MySQL only)
   AWS DMS (managed, multi-source)
   Confluent connectors (commercial)
EVENT FORMAT (Debezium):
  {
    "before": { ... old row ... },
    "after":  { ... new row ... },
    "op": "u",                       # c=create, u=update, d=delete, r=read (snapshot)
    "ts_ms": 1700000000000,
    "source": { db, table, lsn, ... }
  }
```

```
21.2 CDC Patterns
ONE TOPIC PER TABLE:
  topic: dbserver1.public.orders
  Compacted (latest state per primary key)
  → consumers can rebuild current state by reading from beginning
CDC INTO LAKEHOUSE:
  Postgres → Debezium → Kafka → Spark/Flink → Iceberg/Delta MERGE
  Real-time analytical replicas of operational DBs
CDC FOR EVENT-DRIVEN SERVICES:
  Operational DB → CDC → Kafka → downstream services
  → "outbox-lite" without app changes
DOWNSIDE:
   CDC events expose DB schema externally → coupling
   Schema changes in DB break consumers
   Often better to design EXPLICIT events (outbox pattern) for stable contracts
```

```
21.3 The Outbox Pattern
PROBLEM:
  Service updates DB AND publishes Kafka event.
   If DB commits but Kafka fails: lost event
   If Kafka publishes but DB fails: ghost event
   No distributed transaction across both
OUTBOX SOLUTION:
```

```
  1. Service writes to OUTBOX TABLE in the same DB transaction:
     BEGIN;
       INSERT INTO orders (...) VALUES (...);
       INSERT INTO outbox (event_type, payload, status)
         VALUES ('OrderPlaced', {...}, 'pending');
     COMMIT;
```

```
Kafka &amp; Streaming: Principal Engineer Notes
  2. A separate process polls/streams the outbox table:
      OR: CDC the outbox table directly (Debezium)
      Publishes events to Kafka
      Marks outbox row as 'published' (or deletes)
GUARANTEES:
  At-least-once: Kafka event for every committed DB change
  Idempotency: downstream consumers must handle duplicates
```

```
21.4 Stream-Table Duality (Revisited)
A TABLE = current state of an entity
A STREAM = the history of changes
THEY'RE TWO VIEWS OF THE SAME THING:
  Stream → fold → Table
  Table → diff → Stream
EXAMPLES:
   DB table → CDC → stream of changes → compacted topic → table
   Kafka Streams KTable IS a compacted topic seen as a table
   Materialize (the database) materializes streams as queryable tables
THIS DUALITY IS THE CORE INSIGHT OF MODERN DATA SYSTEMS.
   Real-time databases (Materialize, RisingWave) maintain materialized views
    over streams as their primary abstraction
   Lakehouses (Delta, Iceberg) blur the stream/batch line
■ Principal mindset: Outbox is the staff-level answer to 'how do I publish reliably from a transactional service'.
Without it, you have two-phase commit or eventual inconsistency between DB and message bus. Outbox makes the
entire emit atomic with the DB write. Every event-driven architecture I've seen at scale uses this pattern or its
CDC-equivalent. The naive 'commit then publish' is the leading source of data integrity bugs in event-driven
systems.
```

#### 22. Multi-Datacenter & Geo-Replication

  - 22.1 Why Replicate Across DCs
- 1. DISASTER RECOVERY — survive region/DC failure
- 2. LATENCY — consumers near their region
- 3. COMPLIANCE — data residency
- 4. AGGREGATION — collect from edge to central
- THE FUNDAMENTAL TENSION:
- Tight consistency across DCs = high latency for writes
- Eventual consistency = simpler but app must handle drift

```
22.2 The Replication Tools
MIRRORMAKER 2 (MM2) — open-source Kafka project
  Built on Kafka Connect.
   Replicates topics from source cluster to target
   Maintains "remote topic" naming (e.g., source.orders)
   Offset translation for failover
   Supports active-active (bi-directional) with cycle detection
CONFLUENT REPLICATOR — commercial; richer config than MM2
CLUSTER LINKING (Confluent) — broker-level replication
  More efficient than MM2 (no separate process)
  Same offsets across clusters (no translation needed)
UBER uREPLICATOR — open-source, Uber-built; alternative to MM2
CHOICE depends on Confluent vs open-source preference.
```

```
22.3 Topology Patterns
1. ACTIVE-PASSIVE (DR)
   Primary handles all traffic.
   Replicate to standby for failover.
   On primary failure: cut over consumers to standby.
   Challenge: offset translation, lost in-flight messages.
2. ACTIVE-ACTIVE
   Both clusters serve writes (different topics or partitions).
   Replicate each direction.
   Watch for: loops (MM2 prevents via topic naming), conflict resolution.
3. AGGREGATE
   N edge clusters → one central aggregate cluster.
   Edge clusters produce; central consumes everything.
   Good for analytics, regulatory aggregation.
4. STRETCH CLUSTER
   ONE Kafka cluster across DCs (replicas span DCs).
   Lowest RPO/RTO but high latency (replication acks across WAN).
   Use only for: very tight DR needs in nearby DCs (<5ms apart).
```

22.4 RPO and RTO RPO (Recovery Point Objective) — how much data can you lose? RTO (Recovery Time Objective) — how long can you be down? ASYNC REPLICATION: RPO = replication lag (typically seconds) RTO = failover time (typically minutes if manual)

Kafka &amp; Streaming: Principal Engineer Notes SYNC ACROSS DCs (stretch cluster, sync replication): RPO = 0 But: every write waits for cross-DC ack — slow PRAGMATIC: async replication is the right answer for most. Accept seconds of RPO. Focus on fast RTO via automated failover. ■ Production scenario: A team set up MM2 active-active replication between US and EU. Producers wrote to both regions; consumers ran in both. Within minutes, the same event appeared in both regions' topics multiple times. The replication created a loop they hadn't configured to break. MM2's topic-renaming convention prevents it, but they'd customized the config and broken the loop detection. Lesson: read the MM2 docs carefully, leave loop-prevention defaults alone, and test failover scenarios in staging.

## PART VII — OPERATIONS & SCALE

#### 23. Cluster Sizing & Capacity Planning

```
23.1 The Sizing Inputs
GATHER:
   Peak throughput in MB/sec (produce AND consume)
   Average message size
   Required retention (days)
   Number of consumer groups (fan-out factor)
   Acceptable latency
   Replication factor (typically 3)
COMPUTE:
  Storage = peak_MB/s × seconds_in_retention × RF
  Network out = peak_in × num_consumer_groups + RF
  Disk I/O = network throughput (each broker writes AND reads)
```

```
23.2 Per-Broker Capacity
TYPICAL BROKER (commodity hardware):
  Network: 10 Gbps NIC = 1.25 GB/s
  Disk:    NVMe SSD ~3-5 GB/s; cheap HDD ~200 MB/s sequential
  Memory:  64-128 GB (mostly for page cache)
  CPU:     16-32 cores
PER-BROKER THROUGHPUT (realistic):
  Plaintext, lz4 compressed: 500-1000 MB/s
  TLS-encrypted: 200-400 MB/s (CPU-bound on encryption)
  HDD-based: 100-200 MB/s (disk-bound)
```

```
23.3 Sizing Examples
EXAMPLE 1: medium streaming workload
  Peak produce: 100 MB/s, avg message 1 KB → 100K msg/s
  Retention: 7 days
  Replication factor: 3
  Consumer groups: 3 (each reading all data)
  Storage per broker:
    100 MB/s × 86400 × 7 × 3 / N_brokers
    With N=6: 30 TB per broker
  Network:
    Produce in: 100 MB/s split across brokers
    Replication out: 100 × 2 / N_brokers (each leader sends to 2 followers)
    Consume out: 100 × 3 (3 groups) = 300 MB/s total
    Per broker total: ~75-150 MB/s
  Sizing: 6 brokers, 50TB each disk, 25 Gbps network → safe headroom
```

23.4 Partition Count Planning TARGET: enough partitions to allow consumer parallelism + future growth FORMULA:

```
Kafka &amp; Streaming: Principal Engineer Notes
  partitions = max(
    target_throughput / per_partition_throughput,    # produce/consume capacity
    desired_consumer_parallelism,                     # max consumers in a group
    headroom factor (e.g., 2× current need)
  )
  per_partition_throughput typically 10-100 MB/s
RULE OF THUMB: 100K partitions per cluster is a comfortable ceiling.
Each broker should hold < 4000 partitions.
KRaft mode raises both ceilings significantly.
```

```
23.5 Cost Levers
1. COMPRESSION
   lz4 / zstd reduces storage 3-6×
   Single biggest cost-down for storage
2. TIERED STORAGE (3.6+)
   Old data on S3 (cheap) instead of broker disks (expensive)
3. RETENTION TUNING
   Do you really need 7 days for every topic?
   Some topics need 1 hour; some need 90 days.
   Per-topic retention.ms is your friend.
4. PARTITION COUNT
   Fewer partitions = less broker overhead
   Don't over-provision "just in case"; you can add (with caveats)
5. REPLICATION FACTOR
   RF=3 is standard. RF=2 saves 33% storage but no fault tolerance to single failure.
   Tier-0 topics: RF=3. Tier-3 dev topics: RF=2.
```

#### 24. Monitoring & Common Failure Modes

```
24.1 The Metrics That Matter
CLUSTER HEALTH (alert immediately)
  UnderReplicatedPartitions       — should be 0
  ActiveControllerCount           — should be 1 (across cluster)
  OfflinePartitionsCount          — should be 0
  IsrShrinksPerSec                — sustained > 0 = problem
THROUGHPUT
  MessagesInPerSec, BytesInPerSec, BytesOutPerSec
    Per topic and cluster-wide
  TotalProduceRequestsPerSec, TotalFetchRequestsPerSec
LATENCY
  RequestQueueTimeMs (p99)
  TotalTimeMs for Produce / Fetch / Metadata
CONSUMER LAG  (the #1 most important consumer metric)
  current_offset - committed_offset = unprocessed records
  records / messages_per_second = time behind
JVM / SYSTEM
  GC pauses (G1, ZGC, Shenandoah for low pause)
  Disk usage, disk IOPS, network bytes
PER-TOPIC
  Bytes / messages in & out
  Number of partitions, replicas, ISR size
```

```
24.2 Lag Monitoring
TOOLS:
   Burrow (LinkedIn open-source) — best in class for lag
   Cruise Control (LinkedIn) — for rebalancing too
   Confluent Control Center
   kafka-consumer-groups CLI tool
WHAT TO ALERT:
   Lag growing for a consumer group
   Lag > N records (depends on workload)
   Lag > N seconds of time-behind
LAG VS THROUGHPUT:
  Lag growing = consumer can't keep up
  Throughput stable but lag growing = consumer slower than producer
CAUSES OF LAG GROWING:
   Consumer slowed down (deploy of bad version)
   Producer surge (more data than capacity)
   Rebalance churn (consumers stop processing)
   Downstream system slow (DB write throttle)
```

```
24.3 Common Failure Modes
1. DISK FULL ON BROKER
   Symptom: producers fail with NotEnoughReplicasException
   Cause: retention not aging fast enough, or skewed partitions
   Fix: increase disk, reduce retention, balance partitions
2. UNDER-REPLICATED PARTITIONS
   Cause: slow follower (disk, network, GC)
   Fix: investigate the lagging broker; restart if needed
```

```
Kafka &amp; Streaming: Principal Engineer Notes
3. CONTROLLER FAILOVER
   In ZK mode: controller dies, ZK elects new one; brief pause
   In KRaft: faster (~seconds)
   Watch: ActiveControllerCount metric
4. CONSUMER GROUP STUCK REBALANCING
   Symptom: continuous rebalances, group never stable
   Cause: consumer keeps failing heartbeat (slow processing or GC)
   Fix: increase max.poll.interval.ms; reduce max.poll.records; CooperativeStickyAssignor
5. PRODUCER BUFFER FULL
   Symptom: send() blocks, then throws TimeoutException
   Cause: producer can't keep up with sending to brokers
   Fix: more producer instances; larger buffer.memory; check broker health
6. METADATA REQUEST STORM
   Symptom: all clients querying metadata constantly
   Cause: cluster instability, leader elections
   Fix: stabilize cluster; tune metadata.max.age.ms in clients
7. OOM ON BROKER
   Often: too-large messages, too many fetch sessions
   Fix: enforce message.max.bytes; tune replica.fetch.max.bytes
```

```
24.4 The On-Call Playbook
SYMPTOM: producer errors / consumer lag
  → Check cluster: ISR, under-replicated partitions, controller
  → Check brokers: disk, network, GC, CPU
  → Check specific topic: leader changes, partition skew
  → Check client: config drift, version mismatch
SYMPTOM: schema-related deser errors
  → Check schema registry health
  → Check recent schema registrations
  → Check consumer schema cache state
SYMPTOM: end-to-end latency spike
  → Check: producer linger.ms, consumer poll latency, downstream sink
SYMPTOM: cluster brain split (network partition)
  → KRaft: quorum protects you
  → ZK: similar protection via majority
  → BUT clients during partition may see stale leaders
```

#### 25. Security: TLS, SASL, ACLs

25.1 The Three Security Layers ENCRYPTION IN TRANSIT — TLS / SSL AUTHENTICATION — who is the client? AUTHORIZATION — what can they do?

```
25.2 TLS
ENCRYPTS TRAFFIC between clients ↔ brokers and broker ↔ broker.
CONFIG (broker):
  listeners=PLAINTEXT://:9092,SSL://:9093
  ssl.keystore.location=/path/to/server.keystore.jks
  ssl.keystore.password=...
  ssl.truststore.location=/path/to/server.truststore.jks
CONFIG (client):
  security.protocol=SSL
  ssl.truststore.location=/path/to/client.truststore.jks
COST:
  TLS BREAKS zero-copy → broker CPU goes up
  ~30-50% throughput impact for plaintext-vs-TLS
MODERN MITIGATION: kTLS (kernel TLS) restores zero-copy for some kernels.
mTLS (mutual TLS): client also presents a cert → authentication built in.
```

```
25.3 SASL Authentication
SASL = Simple Authentication and Security Layer.
Common Kafka mechanisms:
  PLAIN          — username + password (over TLS!)
  SCRAM-SHA-256/512 — challenge-response, no password on wire
  GSSAPI         — Kerberos (enterprise)
  OAUTHBEARER    — OAuth 2.0 tokens
CONFIG:
  security.protocol=SASL_SSL
  sasl.mechanism=SCRAM-SHA-512
  sasl.jaas.config=org.apache.kafka.common.security.scram.ScramLoginModule required
    username="alice" password="...";
INTEGRATION:
   Active Directory: GSSAPI (Kerberos) or LDAP
   Cloud: OAUTHBEARER with cloud IAM (AWS IAM, GCP, Azure AD)
   Self-hosted: SCRAM with users in ZK / KRaft
```

25.4 ACLs (Authorization) ACL = Access Control List entry: principal can/cannot do action on resource. RESOURCES: Topic, ConsumerGroup, Cluster, TransactionalId, DelegationToken ACTIONS: Read, Write, Describe, Create, Delete, Alter, ClusterAction, ... CONFIG (broker): authorizer.class.name=org.apache.kafka.metadata.authorizer.StandardAuthorizer

```
Kafka &amp; Streaming: Principal Engineer Notes
  super.users=User:admin
EXAMPLES:
  # Allow producer User:alice to write to topic 'orders'
  kafka-acls --bootstrap-server localhost:9092 --add \
    --allow-principal User:alice --operation Write --topic orders
  # Allow consumer group 'analytics' to read 'orders'
  kafka-acls --bootstrap-server localhost:9092 --add \
    --allow-principal User:bob --operation Read --topic orders --group analytics
```

25.5 Production Security Checklist ■ TLS on all listeners (client-broker AND broker-broker) ■ SASL authentication (no anonymous in production) ■ ACLs configured per principal (default-deny) ■ Audit logging enabled (track ACL allows/denies) ■ Schema Registry behind same auth ■ Encrypt at rest (disk-level encryption on broker hosts) ■ Secret rotation procedure (passwords, keystores, certs) ■ Network isolation (VPC, security groups) ■ Monitor: failed auth attempts, ACL denies ■ Principal mindset: Kafka security is multi-layered and easy to get wrong. The common failure: TLS-only without ACLs — encrypted in transit but anyone with network access can read everything. The right setup: TLS for encryption, SASL for authentication, ACLs for authorization, audit logging for accountability. Treat your Kafka cluster like a production database; don't ship without those four.

#### 26. Anti-Patterns at Scale

```
26.1 The Catalog
TOPIC DESIGN
  ■ One topic per microservice (vs per logical event type)
  ■ Too few partitions (caps consumer parallelism)
  ■ Too many partitions (broker overhead, slow recovery)
  ■ High-cardinality keys causing skew
  ■ No schema → schemaless JSON breaking consumers
  ■ Mixing event types in one topic without discriminator
  ■ Using Kafka as a database (random key lookups)
  ■ Storing > 1 MB messages (kills throughput)
PRODUCER
  ■ acks=1 in production (data loss on leader crash)
  ■ enable.idempotence=false (duplicates on retry)
  ■ Sync send for everything (10K× slowdown)
  ■ No compression
  ■ Tiny linger.ms with tiny batches (per-message overhead)
  ■ Custom partitioner causing hot partition
CONSUMER
  ■ enable.auto.commit=true with manual processing (data loss / dup)
  ■ Processing > max.poll.interval.ms (constant rebalances)
  ■ One consumer group for everything (no fan-out flexibility)
  ■ Polling, processing, polling — no async pipeline
  ■ Catching exceptions silently (data loss without alert)
OPERATIONS
  ■ No retention policy review (disk fills slowly over months)
  ■ Ignoring ISR alerts ("it'll be fine")
  ■ Single-broker clusters (no replication)
  ■ No schema registry
  ■ Plaintext in production
  ■ "We'll add ACLs later"
  ■ No consumer lag monitoring
  ■ Manual broker restarts during peak traffic
ARCHITECTURE
  ■ Kafka as request/response (use HTTP/gRPC)
  ■ Kafka as long-term archive (use S3)
  ■ Producer publishing without outbox (DB/Kafka inconsistency)
  ■ Consumer making sync external calls during processing
  ■ Using Kafka for sub-millisecond latency
  ■ Cross-region sync replication (just don't)
  ■ Stream processing in plain consumers (use Streams/Flink)
■ Production scenario: A 'real-time' pipeline was actually 30-second batched because the producer had
linger.ms=10000 (10 seconds) and the consumer used auto.commit at default 5 seconds with manual processing
taking 8 seconds. Each layer added latency. Total p99: 30+ seconds. The team had been debating whether to
migrate to Flink for 'lower latency'. The actual fix was tuning configurations — linger.ms=20, manual commit after
each batch, parallel processing of records. Sub-second latency without a single architectural change.
```

## PART VIII — INTERVIEW PRACTICE

#### 27. The Staff-Level Streaming Interview

  - 27.1 Question Categories
- 1. CONCEPT     — "Explain how Kafka achieves durability"
- 2. DESIGN      — "Design event-driven system for [scenario]"
- 3. DEBUG       — "Consumer lag is growing — walk me through"
- 4. SCALE       — "Kafka cluster handling 5GB/s, what breaks first?"
- 5. TRADE-OFFS  — "Kafka vs Kinesis vs Pulsar"
- 6. SEMANTICS   — "Explain exactly-once" (the big one)
- 7. ANTI-USE    — "When would you NOT use Kafka?"

```
27.2 The Design Framework
STEP 1: CLARIFY THE WORKLOAD
   Throughput: messages/sec, MB/sec, peak vs average
   Latency: end-to-end SLO
   Retention: how long is the data needed?
   Consumers: how many groups? fan-out?
   Ordering: per-key? global?
   Delivery semantics: at-least-once or exactly-once?
STEP 2: TOPIC DESIGN
   One topic per LOGICAL EVENT TYPE
   Partition count = max(parallelism, throughput / per-partition)
   Partition key: choose for ordering + balance
STEP 3: PRODUCER CONFIG
   acks=all, idempotence=true, compression=lz4
   Batching: linger.ms, batch.size
STEP 4: CONSUMER CONFIG
   Group strategy (per consumer responsibility)
   Commit strategy (manual after processing)
   Max poll records / interval
STEP 5: SCHEMA & EVOLUTION
   Format (Avro/Protobuf via Schema Registry)
   Compatibility mode (BACKWARD usually)
STEP 6: FAILURE MODES
   Producer fails → retry, idempotency, dead letter
   Consumer fails → rebalance, replay, dead letter
   Broker fails → ISR, leader election, downtime
   Schema breaks → version compatibility checks
STEP 7: OPERATIONS
   Monitoring (ISR, lag, throughput)
   Multi-DC: needed? topology?
   Security: TLS, SASL, ACLs
```

27.3 Worked Example: Design CDC Pipeline for 50 DBs INTERVIEWER: "You're building CDC for 50 production Postgres DBs. Downstream: warehouse, search index, ML feature store. Design it."

Kafka &amp; Streaming: Principal Engineer Notes STAFF RESPONSE: "Clarifying first: which warehouse, which search index, what end-to-end SLA? Assume Snowflake, OpenSearch, Feast feature store. SLA: 5 minutes freshness; eventual consistency OK. ARCHITECTURE:

#### 1. Postgres logical replication slots → Debezium connector per DB

#### 2. Each connector publishes to topics: <dbname>.<schema>.<table>

#### 3. Schema Registry holds Avro schemas (auto-registered by Debezium)

```
   4. Three independent consumer groups:
       Warehouse loader → batches to Snowflake MERGE
       Search indexer → upserts to OpenSearch
       Feature loader → writes to feature store
 TOPIC DESIGN:
    One topic per table (50 DBs × avg 100 tables = ~5000 topics)
    Partitioned by primary key (preserves per-row order)
    Partition count: 6-12 per topic (modest; CDC throughput isn't huge per table)
    Compacted (latest state per PK) for tables that don't need history
    Retention: 7 days (replay window for downstream)
 SCHEMA STRATEGY:
    Subject naming: TopicNameStrategy
    BACKWARD compatibility default
    CI rejects breaking changes
 RELIABILITY:
    acks=all, idempotence=true (in Debezium connector config)
    RF=3, min.insync.replicas=2
    Outbox pattern via Debezium's own outbox event router
     (for services that emit logical events, not raw CDC)
 SECURITY:
    TLS everywhere, SASL/SCRAM
    ACLs: each consumer group restricted to its tables
    Schema registry behind same auth
 OPERATIONS:
    Monitor: connector status, replication slot lag,
     consumer lag, topic throughput
    Schema registry HA
    Burrow for lag, Cruise Control for rebalancing
    DR: MirrorMaker 2 to secondary region, async
 SCALE LIMITS:
    5000 topics × 6 partitions = 30K partitions — within KRaft comfort
    Plan for 2-3 broker clusters by team if grows beyond 100K partitions
    CDC volume per DB: ~10-100 MB/s, cluster total ~5 GB/s — sizing for
     12-16 brokers with 10 Gbps NIC, lz4 compression"
WHAT MAKES THIS STAFF-LEVEL:
   Started with clarifying questions
   Explicit topic design choices with rationale
   Mentioned schema governance
   Addressed security AND operations
   Sized the cluster
   Knew when to bring in MM2 for DR
```

28. War Stories ■ Production scenario: The Phantom Duplicates. A team noticed ~0.1% duplicate orders in their data warehouse. They blamed Kafka. After two weeks of investigation, the duplicates were in the OUTBOX TABLE — a bug in the producer code marked the outbox row 'published' before Kafka acked. On retry, it re-sent the same event with a new outbox ID. The downstream consumer was correctly idempotent on Kafka offset, but the duplicate had a different ID. Kafka was innocent. Lesson: end-to-end deduplication needs a business-key idempotency token, not just Kafka offsets. ■ Production scenario: The 4 AM Lag Mystery. Every night at 4 AM, consumer lag spiked. Investigation: a batch job in another team's pipeline kicked off at 4 AM, produced a burst of CDC events from a large UPDATE. Lag grew, then drained. The fix wasn't to scale Kafka — it was to coordinate with the batch team to spread the UPDATE across smaller transactions. The producer was healthy; the upstream system was the bottleneck. ■ Production scenario: The Schema Tombstone. A team's compacted topic kept growing despite compaction running. They deleted user records by setting value=null (tombstone). But: their producer code used a schema that required all fields. The serializer threw an error, and the producer silently fell back to NOT sending the null. Compaction couldn't reclaim the keys because no tombstone arrived. Lesson: schema constraints can prevent compaction; test deletion paths explicitly. ■ Production scenario: The Slow Consumer Avalanche. A consumer group of 100 instances started lagging slowly. The downstream DB throttled. Latency grew. session.timeout.ms expired for some consumers — rebalance. The rebalance moved partitions to OTHER instances that were ALSO slow, which then timed out — cascading rebalances. The whole group thrashed. Recovery: stop deploys, let consumers catch up, then carefully reintroduce. Lesson: backpressure manages itself; configure your timeouts longer than your worst-case processing time. ■ Production scenario: The Wrong Partition Key. A team partitioned a 'user-activity' topic by session_id, thinking sessions were the natural unit. But analytics queries wanted per-user aggregates — which now required re-partitioning at consume time (a SHUFFLE). Throughput tanked. Lesson: partition keys are chosen for downstream access patterns. Get them right or pay forever.

#### 29. Cheat Sheet & Mental Models

  - 29.1 The Six Mental Models
- 1. KAFKA IS A DISTRIBUTED LOG
- Not a queue. Append-only. Many consumers, each their own offset.
- 2. PARTITION IS THE UNIT OF PARALLELISM
- In and out. Order is per-partition. Pick count carefully.
- 3. ISR IS YOUR HEALTH METRIC
- Under-replicated = pre-outage. Watch it.
- 4. EXACTLY-ONCE IS THREE THINGS
- Idempotent producer + transactions + read-committed consumer.
- 5. STREAM = HISTORY, TABLE = STATE
- Stream-table duality. Compacted topic is a table.
- 6. EVENT TIME WINS
- Always design with event-time, even if processing-time works in dev.

```
29.2 The One-Page Reference
PRODUCER (production defaults)
  acks=all
  enable.idempotence=true
  retries=Integer.MAX_VALUE
  compression.type=lz4 (or zstd)
  linger.ms=20
  batch.size=32768
CONSUMER (production defaults)
  enable.auto.commit=false             # commit manually
  isolation.level=read_committed       # for exactly-once topics
  max.poll.records=500
  max.poll.interval.ms=300000          # tune for your processing
  session.timeout.ms=45000
  partition.assignment.strategy=CooperativeStickyAssignor
BROKER / TOPIC (production defaults)
  replication.factor=3
  min.insync.replicas=2
  unclean.leader.election.enable=false
  log.retention.hours=168 (1 week — adjust per topic)
  num.partitions=<thought-through>
OPERATIONS
  3 (or 5) brokers minimum
  KRaft mode for new clusters
  TLS + SASL + ACLs
  Schema Registry
  Monitor: ISR, lag, throughput, GC
ANTI-PATTERNS
  ✗ acks=1
  ✗ enable.auto.commit=true (with manual processing)
  ✗ Plaintext in prod
  ✗ No schema registry
  ✗ One topic per microservice
  ✗ < 0 ms linger (per-message overhead)
  ✗ message.max.bytes > 1 MB
```

```
29.3 What Separates Senior from Staff
SENIOR                                STAFF / PRINCIPAL
■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■
"Use Kafka"                          "Kafka for fan-out event log;
                                      RabbitMQ for task queues; Flink
                                      for stream processing; Pulsar
                                      if multi-tenancy critical"
"Pick a partition count"             "Partition count = throughput /
                                      per-partition + consumer parallelism
                                      + 2× headroom. We're at ~150 because
                                      peak is ~5GB/s and we need 100-way
                                      consumer parallelism"
"Use acks=all"                       "acks=all + min.insync.replicas=2 +
                                      enable.idempotence + RF=3. That
                                      survives one broker failure with
                                      zero data loss. Lower RF saves
                                      money but breaks the durability
                                      guarantee"
"Exactly-once"                       "Within Kafka, yes — transactional
                                      producer + read-committed consumer.
                                      End-to-end depends on the downstream:
                                      if it's idempotent (Delta MERGE,
                                      DB UPSERT), at-least-once is fine.
                                      If not, we need the full transaction"
"Add a consumer"                     "New consumer group, doesn't affect
                                      existing groups. But check: does
                                      this topic have the schema this
                                      consumer expects? what's our
                                      retention vs their replay needs?"
■ Principal mindset: Kafka mastery is about thinking in distributed systems terms — partitions, replication,
consumer groups, ordering guarantees, failure modes. The client API is the easy part. At staff level you're expected
to design topologies that survive broker failure, schema drift, traffic bursts, and multi-region operation. And you're
expected to know when something else (RabbitMQ, Pulsar, Kinesis, Flink) is the right answer instead.
 — End of Kafka & Streaming Notes —
 Next in series: dbt, Cloud Data Platforms, Lakehouses
```


---

[← Back to main](./README.md)
