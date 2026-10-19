# Topic 25 — GenAI / RAG Platform for Data Engineers

> Standalone deep-dive · Vector DBs, embeddings, RAG, agents, LLMOps from a DE lens

[← Back to main](./README.md)

---

## How to use this document

By 2026, GenAI/RAG is a data engineering problem as much as an ML problem. Embedding pipelines, vector stores, retrieval quality, cost control, evaluation — these are DE concerns. Interviews will probe this. This chapter is the GenAI chapter for people who build *platforms*, not models.

> **🧠 Principal Mindset:** The LLM is a commodity. The DIFFERENTIATION is your retrieval, your evaluation, your data freshness, your governance. Senior engineers call OpenAI. Staff engineers build the surrounding system that makes the call good.

## Table of Contents

**PART I — FOUNDATIONS**
- 1. Why GenAI Is a DE Problem Now
- 2. The RAG Reference Architecture
- 3. Embeddings and Vector Spaces (fast refresher)
- 4. The LLMOps Stack

**PART II — EMBEDDING PIPELINES**
- 5. Chunking Strategies
- 6. Choosing an Embedding Model
- 7. Batch and Streaming Embedding Pipelines
- 8. Metadata Hygiene

**PART III — VECTOR DATABASES**
- 9. The Vector DB Landscape (2026)
- 10. Pinecone and Weaviate
- 11. pgvector and PostgreSQL as a Vector Store
- 12. Milvus, Chroma, LanceDB, Qdrant
- 13. Lakehouse Vector Indexes (Databricks, Snowflake)

**PART IV — RETRIEVAL**
- 14. Dense, Sparse, and Hybrid Retrieval
- 15. Reranking
- 16. Query Transformations
- 17. GraphRAG and Beyond

**PART V — AGENTIC PATTERNS**
- 18. Tool Use and Function Calling
- 19. Planning + Reflection
- 20. Memory
- 21. Agent Frameworks (2026)

**PART VI — EVALUATION**
- 22. The RAG Evaluation Framework
- 23. Ragas, DeepEval, TruLens
- 24. LLM-as-Judge Caveats

**PART VII — OPERATIONS**
- 25. Prompt Caching and Cost Control
- 26. LLM Gateways and Governance
- 27. Observability for LLM Systems

**PART VIII — INTERVIEW PRACTICE**
- 28. The Staff-Level GenAI Interview
- 29. Worked System Design
- 30. Cheat Sheet &amp; Mental Models

---

## PART I — FOUNDATIONS

### 1. Why GenAI Is a DE Problem Now

```
WHAT MOVED FROM ML TO DE:
   • Embedding pipelines are ETL
   • Vector stores are databases
   • Retrieval is a search problem
   • Freshness is a streaming problem
   • Cost control is a FinOps problem
   • Evaluation is a data quality problem
   • Governance is a lineage + contract problem

LLM teams own prompts + model choice + agents.
DATA teams own the data + retrieval + freshness + governance.

The intersection has NEW roles:
   AI Engineer: productionizes GenAI apps (new title, 2023+)
   Data + AI Engineer: builds the data platform FOR GenAI
   
In 2026, principal/staff DE roles increasingly include GenAI scope.
```

### 2. The RAG Reference Architecture

```
   ┌──────────────────────────────────────────────────────────┐
   │                      SOURCES                              │
   │  Docs │ PDFs │ Wikis │ Tickets │ Transcripts │ SQL        │
   └────────────────────────┬─────────────────────────────────┘
                            │
   ┌────────────────────────▼─────────────────────────────────┐
   │                 INGESTION + CHUNKING                      │
   │  Loaders → Chunkers (semantic, structural) → Metadata    │
   └────────────────────────┬─────────────────────────────────┘
                            │
   ┌────────────────────────▼─────────────────────────────────┐
   │                 EMBEDDING + INDEXING                      │
   │  Embedding model → Vector DB + Lexical index              │
   └────────────────────────┬─────────────────────────────────┘
                            │
   ┌────────────────────────▼─────────────────────────────────┐
   │                 RETRIEVAL + GENERATION                    │
   │  Query → (embed + rewrite) → retrieve → rerank → LLM     │
   └────────────────────────┬─────────────────────────────────┘
                            │
   ┌────────────────────────▼─────────────────────────────────┐
   │              EVALUATION + OBSERVABILITY                   │
   │  Traces │ Groundedness │ Faithfulness │ Latency │ Cost  │
   └──────────────────────────────────────────────────────────┘
```

### 3. Embeddings and Vector Spaces (fast refresher)

```
EMBEDDING: dense vector representation of text (image, audio, etc.)
   Typical dimensions: 384, 768, 1024, 1536, 3072
   Semantic distance in vector space ≈ semantic similarity in meaning
   
SIMILARITY METRICS:
   COSINE: angle between vectors (most common for text)
   DOT PRODUCT: magnitude + angle (faster; equivalent after normalization)
   EUCLIDEAN (L2): straight-line distance

MODEL CHOICES (as of 2026):
   OpenAI text-embedding-3-small / large
   Cohere embed-v3 (multilingual leader)
   Voyage voyage-3 (very strong; smaller company)
   BGE / E5 / GTE open-source (self-host)
   Snowflake arctic-embed-m-v1.5 (open; competitive)
   Nomic Embed Text v1.5 (open; long context)

STAFF SIGNAL: don't marry one embedding model. Models rotate every 6 months.
   Design the pipeline to re-embed on model change (cost-aware).
```

### 4. The LLMOps Stack

```
LAYER                   TOOLS (sample)
======================================================
LLM                     OpenAI, Anthropic, Google, open-source
Gateway                 Portkey, LiteLLM, Databricks AI Gateway, Kong AI
Orchestration           LangChain, LlamaIndex, Haystack, DSPy, Semantic Kernel
Vector Store            Pinecone, Weaviate, Milvus, pgvector, Qdrant, Chroma
Embedding               OpenAI, Cohere, Voyage, Nomic, open models
Retrieval augmentation  Reranker (Cohere Rerank, BGE, Jina)
Evaluation              Ragas, DeepEval, TruLens, Phoenix, Weights &amp; Biases
Observability           LangSmith, Arize, Phoenix, Helicone, Langfuse
Prompt management       PromptLayer, PromptHub, LangFuse
Feature flagging        LaunchDarkly, Statsig for prompt variants
Serving                 Modal, Fly, Anyscale, Databricks Model Serving

STAFF SIGNAL: this stack is still consolidating. Pick layers you can
   swap. Avoid vendors that lock you across layers.
```

---

## PART II — EMBEDDING PIPELINES

### 5. Chunking Strategies

```
WHY CHUNKING MATTERS:
   LLM context limits (though growing: 200K+ in 2026)
   Retrieval precision (small chunks = focused match; large chunks = context)
   Embedding model limits (most embed <8K tokens)

STRATEGIES:
   
   FIXED-SIZE (chars or tokens): simple; respects no structure
      Pro: fast; easy to tune
      Con: splits mid-sentence; loses semantic boundaries
   
   RECURSIVE: try paragraph → sentence → word if too large
      LangChain's RecursiveCharacterTextSplitter default
      Preserves structure; still loses semantic groupings
   
   SEMANTIC: embed sentences, cluster by similarity, chunk by cluster
      Preserves topic coherence
      Expensive (per-sentence embedding during ingest)
   
   STRUCTURAL: respect document structure (headings, sections)
      PDF: pages, sections; Markdown: headings; Code: functions/classes
      Best fit when source has structure
   
   HIERARCHICAL / PARENT-CHILD: chunk small, retrieve large
      Index small chunks; return the parent section for generation
      Balances precision + context

OVERLAP: 10-20% overlap between chunks mitigates boundary cutoff

STAFF SIGNAL: chunking is tuned per corpus. Legal docs chunk differently
   than slack transcripts. One strategy doesn't fit all.
```

### 6. Choosing an Embedding Model

```
TRADE-OFFS:
   
   DIMENSION: 384 (fast, cheap) → 3072 (slow, pricey; mild quality gains)
   
   CONTEXT LENGTH: typical 512-8K tokens
      Long-context embeddings (Nomic, Voyage) help with structural chunks
   
   DOMAIN FIT: 
      General text: most models are competitive
      Code: specialized models (CodeBERT, Jina Code) outperform
      Medical/legal: domain-specific models or fine-tuned
   
   HOSTING:
      API (OpenAI, Cohere, Voyage): fast; API cost; dependency
      Self-host (BGE, GTE, Nomic): no API cost; infra + ops cost
   
   MULTILINGUAL: Cohere embed-v3, mBGE, mE5 lead

BENCHMARK: MTEB (Massive Text Embedding Benchmark)
   Public leaderboard; a starting point not a conclusion
   Always test on your own data

RE-EMBEDDING cost at scale:
   10M chunks × $0.0001/chunk (API) = $1,000 per re-embed
   Plan for this when model changes
```

### 7. Batch and Streaming Embedding Pipelines

```
BATCH:
   Spark / Databricks job reads source docs → chunks → embeds → writes to vector DB
   Scheduled nightly or on-demand
   Cheap per-doc; higher latency to availability

STREAMING:
   Doc change event (Kafka / webhook / DB CDC) → chunker → embedder → vector DB
   Fresh retrieval (seconds)
   Higher per-doc cost (small batches); throughput engineering needed

PATTERN:
   Base corpus: batch ingest once
   Updates: streaming for changed docs
   Scheduled re-embed: when model changes

TOOLS:
   Spark UDF for embedding (batches API calls)
   Flink + async for streaming
   Databricks Vector Search: embedding + indexing in one pipeline
   Snowflake Cortex EMBED_TEXT_768: SQL-native embed on existing tables
```

### 8. Metadata Hygiene

```
Every chunk needs METADATA for filtering + attribution:
   
   source_id, source_url, source_type
   section_title / heading
   author
   created_at, updated_at (for freshness filters)
   tenant_id (for multi-tenant isolation)
   access_level / tags (for ACLs applied at retrieval)
   checksum (for change detection + idempotent updates)
   embedding_model_version
   chunk_index + total_chunks

Filtering during retrieval requires metadata indexed in vector DB:
   • Most vector DBs support metadata filters (hybrid filter + similarity)
   • But filtering can be slow if cardinality is high — DESIGN carefully

STAFF SIGNAL: ACLs via metadata are table-stakes for multi-tenant RAG.
   Never trust the client; filter at retrieval, re-filter at return.
```

---

## PART III — VECTOR DATABASES

### 9. The Vector DB Landscape (2026)

```
PURE-PLAY VECTOR DBS:
   Pinecone      — managed SaaS, serverless
   Weaviate      — open-source + managed; hybrid search strong
   Milvus / Zilliz — open-source + managed; GPU-accelerated
   Qdrant        — Rust-based; strong hybrid filtering
   Chroma        — Python-first; developer-friendly
   LanceDB       — embedded + object store; file-based

TRADITIONAL DBS + VECTOR:
   pgvector (Postgres extension)
   Elasticsearch dense_vector + k-NN
   Redis Vector
   MongoDB Atlas Vector Search
   ClickHouse (vector index via libraries)
   DuckDB vss extension

CLOUD-NATIVE:
   AWS OpenSearch k-NN
   Vertex AI Vector Search (Google)
   Azure AI Search (vector + hybrid)

LAKEHOUSE-NATIVE:
   Databricks Vector Search (Delta-synced)
   Snowflake vector type + VECTOR_COSINE_SIMILARITY

OBSOLETE (as of 2026):
   Rockset (OpenAI acquired, shut down)
```

### 10. Pinecone and Weaviate

```
PINECONE:
   Fully-managed, serverless (compute scales to zero)
   Simple API, strong SDKs
   Pricing: per-pod or serverless (per-GB stored + queries)
   Scales to billions of vectors
   HNSW indexes
   
   STRENGTH: operational simplicity
   WEAKNESS: no self-host option; vendor lock-in

WEAVIATE:
   Open-source + Weaviate Cloud managed
   Hybrid search (BM25 + vector) is first-class
   Multi-tenancy support
   Modules for embedding integration (plug-in model providers)
   
   STRENGTH: hybrid search, open-source, modular
   WEAKNESS: operational complexity if self-hosted
```

### 11. pgvector and PostgreSQL as a Vector Store

```
PGVECTOR = Postgres extension for vector operations.
   
   CREATE EXTENSION vector;
   CREATE TABLE docs (id BIGINT, embedding vector(1536), metadata JSONB);
   CREATE INDEX ON docs USING hnsw (embedding vector_cosine_ops);
   
   SELECT * FROM docs
   ORDER BY embedding <=> '[0.1, 0.2, ...]'::vector
   LIMIT 10;

STRENGTHS:
   • Single DB for structured + vector data
   • Mature operations
   • Transactional guarantees
   • Cost-effective at small-medium scale
   
WEAKNESSES:
   • Scales well to ~100M vectors; harder beyond
   • Index build is memory-intensive
   • HNSW tuning is manual

PATTERN:
   Default to pgvector for prototypes + small-medium apps
   Switch to dedicated vector DB when:
      • >100M vectors
      • >1000 QPS query load
      • Multi-tenant isolation needs
      • Hybrid search is critical

STAFF SIGNAL: pgvector is a "no-regret default." Only move off it when
   you have concrete limits you've hit, not speculatively.
```

### 12. Milvus, Chroma, LanceDB, Qdrant

```
MILVUS:
   Enterprise-grade; GPU support; distributed
   Zilliz Cloud for managed
   Choose for: very large scale, GPU acceleration

CHROMA:
   Python-first; embedded or server mode
   Great for prototypes and local development
   Not for enterprise production at scale (yet)

LANCEDB:
   File-based; stores on S3/GCS as Lance format
   Serverless-friendly (no DB server)
   Embedded + distributed modes
   Choose for: lakehouse-adjacent, serverless, cost-sensitive

QDRANT:
   Rust-based; strong per-filter performance
   Open-source + managed
   Choose for: high-cardinality metadata filtering
```

### 13. Lakehouse Vector Indexes (Databricks, Snowflake)

```
DATABRICKS VECTOR SEARCH (GA 2024):
   Vector indexes on Delta tables
   Delta-sync: index updates when source changes
   Hybrid search (BM25 + vector)
   Managed via Unity Catalog governance
   Pattern: table of chunks → vector search → serving endpoint

SNOWFLAKE VECTOR SEARCH:
   VECTOR type + VECTOR_COSINE_SIMILARITY function
   Store embeddings in regular Snowflake tables
   Query with SQL
   Pros: no separate vector DB; Cortex integration
   Cons: not as optimized as purpose-built vector DBs at scale

STAFF SIGNAL: lakehouse-native vector search is a strong default for
   teams already on Databricks / Snowflake. Avoids a separate system;
   governance is unified. Switch to purpose-built when scale/latency demands.
```

---

## PART IV — RETRIEVAL

### 14. Dense, Sparse, and Hybrid Retrieval

```
DENSE: embedding similarity (semantic)
   Captures meaning; misses exact terms
   "GDPR compliance" and "data protection law" match
   
SPARSE: BM25 / TF-IDF (lexical)
   Captures exact terms; misses semantic matches
   Rare terms, acronyms, product names: sparse wins

HYBRID: both scores combined
   Reciprocal Rank Fusion (RRF) is standard
   Weights the two by score rank, not score magnitude

PATTERN:
   For QA on documents: hybrid almost always beats pure dense
   Pure dense fails on acronyms, part numbers, specific jargon
   Pure sparse fails on paraphrased queries

VECTOR DBs that handle hybrid natively:
   Weaviate, Qdrant, OpenSearch, Databricks Vector Search,
   Azure AI Search
```

### 15. Reranking

```
RERANKING: a two-stage retrieval pattern.

Stage 1: retrieve top-100 by dense/hybrid (cheap, fast)
Stage 2: rerank those 100 with a RERANKER model (expensive, accurate)
Return top-5 to the LLM.

RERANKER MODELS:
   Cohere Rerank 3 (API; strong default)
   BGE Reranker (open; self-host)
   Jina Reranker (open)
   Cross-encoder models (bi-encoder for initial retrieval; cross-encoder for rerank)

WHY RERANKING WINS:
   Bi-encoders (used for initial retrieval) encode query + doc separately
   Cross-encoders see them together → more accurate relevance scoring
   But cross-encoders can't be run across the whole corpus (too slow)
   Reranking is the compromise

PATTERN:
   Retrieve 50-200 candidates → rerank to 5-20 → LLM
   Measurable quality improvement; worth the latency ~100-300ms
```

### 16. Query Transformations

```
TECHNIQUES to improve retrieval quality:

   HYDE (Hypothetical Document Embeddings):
      LLM generates a hypothetical answer
      Embed the hypothetical answer, not the query
      Retrieve docs similar to the hypothetical answer
      Works well for complex questions
   
   MULTI-QUERY (Query Expansion):
      LLM generates N variants of the query
      Retrieve for each; union results
      Reduces missed terminology
   
   STEP-BACK:
      LLM reformulates the question at a higher level
      "What were Q1 earnings?" → "What is Q1 earnings reporting?"
      Retrieve background + specifics
   
   SUB-QUESTIONS:
      Decompose complex question into sub-questions
      Retrieve + answer each; synthesize
   
   SELF-QUERY:
      LLM extracts metadata filters from the question
      "Show me 2024 financial reports" → filter(year=2024, type=financial)

STAFF SIGNAL: query transformations trade latency for retrieval quality.
   Measure impact on your eval set; add only transformations that help.
```

### 17. GraphRAG and Beyond

```
GRAPHRAG: retrieve from a knowledge graph, not just vector similarity.

   Build phase:
      LLM extracts entities + relationships from docs
      Store as graph (Neo4j, custom)
      Can combine with vector embeddings of entities
   
   Query phase:
      LLM identifies entities in query
      Traverse graph for connected context
      Return combined graph context to LLM

STRENGTHS:
   Multi-hop reasoning
   Structured retrieval (follow explicit relationships)
   Entity-centric QA

WEAKNESSES:
   Graph construction is expensive
   Quality of graph determines quality of retrieval
   Operational complexity

Microsoft open-sourced GraphRAG in 2024.
Promising for domains with rich entity relationships (legal, med, enterprise).
```

---

## PART V — AGENTIC PATTERNS

### 18. Tool Use and Function Calling

```
TOOL USE: LLM decides when + how to call external functions.

Modern LLMs (GPT-4, Claude 3+, Gemini) support structured function calling.

PATTERN:
   Define tool: name, description, parameters schema
   LLM reads user query + tool definitions
   LLM outputs: {tool_name: X, parameters: {...}}
   App executes tool; returns result to LLM
   LLM generates final response using result

EXAMPLE:
   Tools: [sql_query(query), fetch_user(user_id), send_slack(channel, msg)]
   User: "DM alice the top 10 customers by revenue this month"
   LLM: calls sql_query → gets results → calls send_slack → done

STAFF SIGNAL: tool use shifts the architectural question from "which 
   LLM" to "what tools + what guardrails". Tool catalogs become 
   first-class resources.
```

### 19. Planning + Reflection

```
PLANNING: LLM breaks a goal into steps.
   • ReAct (Reasoning + Acting): interleave thought → action → observation
   • Plan-and-Execute: generate full plan first; execute step by step
   • Tree-of-Thought: explore multiple reasoning paths; backtrack

REFLECTION: LLM critiques its own output.
   • Generate draft → generate critique → regenerate improved version
   • Costs more tokens but improves quality on hard tasks

ANTI-PATTERN:
   • Infinite loops (LLM retries forever) — ALWAYS set max iterations
   • Unbounded tool calls — set budgets
   • Hallucinated tool parameters — validate against schemas

STAFF SIGNAL: planning + reflection help on hard tasks but add latency + cost.
   Measure quality lift vs cost; use selectively.
```

### 20. Memory

```
TYPES OF MEMORY:
   
   SHORT-TERM: conversation history within a session
      Simple: pass full history each turn (up to context window)
      Summarization: compress older turns into a summary when context fills
   
   LONG-TERM: facts about user / world across sessions
      Store user preferences, past interactions
      Retrieve relevant long-term memory at each turn
   
   EPISODIC: specific past interactions ("the time user X asked Y")
      Vector-indexed; retrieve similar past episodes
   
   SEMANTIC: knowledge extracted from interactions
      "User prefers brief answers"
      Updated periodically; applied to each turn

PATTERNS:
   Mem0, Zep: dedicated memory layers for LLM apps
   LangChain Memory abstractions
   Custom: store memories as embeddings; retrieve via RAG

PRIVACY: memory is PII — treat accordingly.
```

### 21. Agent Frameworks (2026)

```
LANGCHAIN / LANGGRAPH: most popular; LangGraph adds state machine semantics
LLAMAINDEX: data-focused; strong for RAG over docs
DSPY: Stanford; focus on prompt optimization + compilation
HAYSTACK: Deepset; pipelines + evaluation
SEMANTIC KERNEL: Microsoft; strong on .NET + Python
AUTOGEN: Microsoft; multi-agent collaboration
CREWAI: role-based multi-agent patterns
PYDANTIC AI: new (2024); type-safe tool definitions

MANAGED AGENT PLATFORMS:
   OpenAI Assistants / GPT Store
   Anthropic Claude agents + MCP (Model Context Protocol, 2024+)
   Databricks Mosaic AI Agent Framework
   Snowflake Cortex Agents
   AWS Bedrock Agents
   Google Vertex AI Agent Builder

STAFF SIGNAL: frameworks are commodified. Pick based on team language,
   observability support, and what plays well with your gateway + eval
   tools, not on "which framework is best."
```

---

## PART VI — EVALUATION

### 22. The RAG Evaluation Framework

```
RAG QUALITY has multiple dimensions:
   
   RETRIEVAL metrics:
      • Recall@k: did the right chunk appear in top-k?
      • MRR: inverse rank of first relevant result
      • NDCG: ranking quality with graded relevance
   
   GENERATION metrics:
      • Faithfulness: does the answer only use retrieved context?
      • Answer relevance: does the answer address the question?
      • Groundedness: are claims cited from retrieved context?
   
   END-TO-END:
      • Correctness: compared to ground truth
      • Completeness: all parts of question addressed
      • Freshness: time between source update and answer reflecting it

STAFF SIGNAL: BUILD AN EVAL SET DAY ONE.
   50-200 question/answer pairs reviewed by humans
   Measure retrieval + generation separately
   Any prompt / model / chunking change evaluates against the set
   This is the ANCHOR of your quality program.
```

### 23. Ragas, DeepEval, TruLens

```
RAGAS:
   Open-source; Python
   Metrics: faithfulness, answer relevance, context precision/recall
   Uses LLM-as-judge for most metrics
   Standard choice for RAG eval

DEEPEVAL:
   Open-source; Python; pytest-style
   Rich metrics beyond RAG (bias, toxicity, hallucination)
   Good CI integration

TRULENS:
   Open-source; Python
   "Feedback functions" model: define per-trace evaluations
   Strong on interactive debugging + logging

PHOENIX (Arize):
   Open-source observability + eval
   Traces + metrics in one UI

Choose based on: eval metrics needed, CI integration, UI preferences.
```

### 24. LLM-as-Judge Caveats

```
LLM-AS-JUDGE: use an LLM to score outputs.
   PRACTICAL: scales to thousands of evals cheaply
   PROBLEMATIC: judge model has biases + inconsistencies

CAVEATS:
   • Position bias: judge prefers first answer when comparing
   • Verbosity bias: judge prefers longer answers
   • Self-preference: judge prefers its own style
   • Low variance capture: judges miss subtle quality differences

MITIGATIONS:
   • Use a STRONGER judge than generator
   • Randomize position when comparing
   • Use multiple judges + aggregate
   • Sample-based human verification on judge disagreements
   • Measure judge-human correlation on a hand-labeled set
   • Avoid single-score; use rubric-based

STAFF SIGNAL: never ship a RAG system that was evaluated ONLY by
   LLM-as-judge. Human eval on a sample is non-negotiable.
```

---

## PART VII — OPERATIONS

### 25. Prompt Caching and Cost Control

```
PROMPT CACHING (Anthropic 2024, OpenAI 2024, Google 2024):
   LLM providers now cache common prefixes
   Cost: ~90% off for cached tokens
   
PATTERN:
   System prompt + static context: pin at prompt start
   Dynamic query + retrieved context: at the end
   Reduces cost 50-90% for apps with large shared context

OTHER COST LEVERS:
   • Routing: cheap model for easy queries, expensive for hard
   • Semantic caching: cache LLM response for similar queries
   • Request batching: reduce overhead per request
   • Streaming output: better UX; same cost but perceived faster
   • Model distillation: train a smaller model from the big one's outputs
   • Prompt compression: LLMLingua, remove redundant tokens

MEASUREMENT:
   Cost per query (track per feature, per team, per tenant)
   P99 latency
   Quality metrics (don't degrade for cost)

STAFF SIGNAL: GenAI costs can explode. Early investment in cost
   observability prevents end-of-quarter surprises.
```

### 26. LLM Gateways and Governance

```
LLM GATEWAY = reverse proxy for LLM calls.

CAPABILITIES:
   • Multi-provider routing (OpenAI, Anthropic, Google, open-source)
   • Fallback / failover
   • Rate limiting per user / team
   • PII filtering / masking
   • Audit logging (every prompt + response)
   • Cost allocation per team / project
   • Semantic caching
   • Payload rewriting (prompt library inject)

GATEWAYS (2026):
   Portkey, LiteLLM, Kong AI, Databricks AI Gateway,
   Cloudflare AI Gateway, OpenRouter

STAFF SIGNAL: a gateway becomes mandatory in any org with 10+ LLM
   applications. Without it: no audit, no cost control, no PII hygiene,
   no provider switching.
```

### 27. Observability for LLM Systems

```
LLM OBSERVABILITY ≠ APP OBSERVABILITY:
   
   TRACES: full request → retrieval → generation chain, step by step
      Visualize what the LLM "saw" and what it output
   
   TOKEN METRICS: input tokens, output tokens, cost per request
   
   LATENCY BREAKDOWN: embed, retrieve, rerank, generate separately
   
   QUALITY METRICS: eval scores on live traffic (sampled)
   
   DRIFT: does input distribution change over time? Output quality?

TOOLS:
   LangSmith (LangChain-aligned)
   Phoenix (open-source; Arize)
   Langfuse (open-source; Langfuse Cloud)
   Helicone (open-source + hosted)
   Arize AI (full MLOps)
   Datadog LLM Observability

STAFF SIGNAL: LLM observability is still immature. Invest in traces
   from day one; the alternative is debugging in the dark.
```

---

## PART VIII — INTERVIEW PRACTICE

### 28. The Staff-Level GenAI Interview

```
COMMON QUESTIONS:
   1. "Design a RAG system for 1M documents."
   2. "Why is retrieval the hard part of RAG?"
   3. "Hybrid vs dense retrieval."
   4. "How do you evaluate RAG quality?"
   5. "Explain prompt caching."
   6. "LLM Gateway: what and why?"
   7. "pgvector vs Pinecone — when each?"
   8. "Agents: planning + reflection patterns."
   9. "How do you keep a RAG index fresh?"
   10. "Multi-tenant RAG — architecture concerns."
```

### 29. Worked System Design

```
Q: Design a RAG platform for an enterprise with:
   • 2M internal documents (wikis, PDFs, Slack, Jira, Confluence)
   • 10K employees
   • Fine-grained ACLs per document
   • Freshness: < 5 minutes for most sources
   • LLM: Claude 3.5 Sonnet + GPT-4o (multi-provider)
   • Budget: $300K/year total

STAFF ANSWER:

INGESTION:
   Source connectors:
     • Confluence: REST API + webhooks for incremental updates
     • Slack: Events API + archive import
     • Jira: webhooks + REST
     • Google Drive / SharePoint: changes API
   
   Streaming pipeline:
     Source webhook → Kafka → chunker → embedder → vector DB
     Flink or Databricks DLT for stream processing
     Dead-letter queue for failures

CHUNKING:
   Structural per source type:
     Confluence: by heading hierarchy
     Slack: by thread, then by message window
     PDF: by page + section
   Overlap: 15% between chunks
   Hierarchical: index chunks, retrieve + return parent context

METADATA:
   doc_id, source_type, source_url, author, created_at, updated_at,
   permissions (list of user_ids and group_ids with access),
   tenant_id (customer tenant for SaaS use case),
   checksum, embedding_model_version

EMBEDDING:
   Model: Voyage voyage-3 (strongest open-field quality in 2026)
   Dimension: 1024
   Batched via Spark UDF for bulk; async for streaming
   Re-embed job quarterly on model updates (budget allocated)

VECTOR DB:
   Databricks Vector Search (lakehouse-native):
     Delta table of chunks as source of truth
     Vector index Delta-synced to source
     Hybrid search (BM25 + vector)
     Scale: 2M docs × ~5 chunks avg = 10M vectors (fits comfortably)

RETRIEVAL:
   Query transformation:
     Multi-query (3 variants) for important queries
     Self-query to extract metadata filters (date, source, author)
   Hybrid retrieval: BM25 + vector; RRF fusion
   Reranker: Cohere Rerank 3 (top-50 → top-5)
   ACL filter: applied in vector DB WHERE clause (user's groups)

GENERATION:
   LLM Gateway: Portkey (multi-provider, fallback, cost tracking, audit)
   Routing rule: 
     Easy queries (classification) → Claude Haiku
     Standard RAG → Claude Sonnet or GPT-4o (A/B)
     Hard reasoning (multi-doc synthesis) → Claude Sonnet (longer context)
   Prompt caching: system prompt + source templates (not query/context)

EVALUATION:
   Internal eval set: 500 Q+A pairs reviewed by SMEs
   CI check: no regression on key metrics for PRs
   Live monitoring: Phoenix traces, Ragas sampled on 1% traffic
   Weekly review: lowest-scoring 20 queries triaged

OBSERVABILITY:
   Phoenix for traces
   Portkey for cost / rate / error
   Datadog LLM Observability for alerting
   SLO: P95 end-to-end < 3 seconds

GOVERNANCE:
   LLM Gateway logs all prompts (PII masked in logs)
   ACLs applied at retrieval time (filter in Databricks)
   Unity Catalog governs source Delta tables
   RTBF (right to be forgotten): delete source → chunks removed from index

COST:
   Vector Search (Databricks):           $40K/year
   Embedding (Voyage API):                $25K/year
   LLM (Claude + GPT-4o combined):       $150K/year
   LLM Gateway (Portkey):                 $15K/year
   Observability (Phoenix OSS + hosted):  $10K/year
   Compute (streaming, ingestion):        $30K/year
   Reranker (Cohere Rerank):              $20K/year
   Human eval + ops time:                 $10K/year
   Total: ~$300K/year — within budget

KEY DECISIONS DEFENDED:
   Why hybrid retrieval: internal docs have jargon + acronyms (lexical wins there)
   Why reranker: measurable +15% retrieval quality on eval set
   Why multi-provider: Claude for reasoning, GPT-4o for broad; failover
   Why Databricks Vector Search: data already in Delta; governance unified
   Why not pure pgvector: scale (10M vectors + 1000 QPS peak) + hybrid search
```

### 30. Cheat Sheet &amp; Mental Models

#### 30.1 One-Minute Pitch

```
RAG = retrieval + generation. The LLM is commodity; RETRIEVAL + 
   EVALUATION are the differentiators.

STACK:
   Ingestion: structural chunking + metadata
   Embedding: current best + plan for re-embed
   Vector DB: pgvector → Databricks VS → Pinecone, depending on scale
   Retrieval: hybrid (dense + sparse) + reranker
   Generation: LLM Gateway with multi-provider + routing
   Evaluation: eval set day one; Ragas or DeepEval; human sample
   Operations: traces, cost allocation, prompt caching

AGENTS add: tool use, planning, memory. Keep budgets + guardrails.

STAFF SIGNAL: argue the SYSTEM, not the LLM. 
```

#### 30.2 Senior vs Staff

| Senior | Staff |
|--------|-------|
| "We use OpenAI." | "LLM is behind a gateway with fallback, routing, cost tracking, PII masking, and audit. Model choice is a config decision per use case." |
| "Vector DB stores embeddings." | "Pick the vector store by scale + hybrid search need + governance surface. pgvector for small-medium; Databricks/Snowflake-native for lakehouse-centric; Pinecone/Milvus/Qdrant at large scale with multi-tenant SLAs." |
| "We chunk documents." | "Chunking is per-source structural. Hierarchical pattern: index small, return parent. Overlap 10-15%. Metadata rich. Re-chunk on strategy change." |
| "We measure quality." | "Eval set 50-200 pairs from day one, SME-reviewed. Separate retrieval (recall@k, MRR) and generation (faithfulness, groundedness) metrics. LLM-as-judge for scale + human sample for truth." |
| "Agents call tools." | "Agents have tool catalogs with schemas, planning + reflection where quality lift exceeds cost, memory layers with privacy policies, and bounded iteration budgets. Guardrails are non-negotiable." |

> **🧠 Principal Mindset:** In 2026, the GenAI stack is a NORMAL data engineering problem with new primitives (embeddings, vectors, LLMs, tools). The staff signal is treating it like other data problems: pipelines, contracts, SLAs, observability, cost. Teams that approach it as "magic" or "just call an API" fail to productionize. Teams that approach it as "ingestion → index → retrieve → evaluate → operate" ship reliably.

---

[← Back to main](./README.md)
