# TurDB: Vector-Enhanced SQLite-like Database in Go

## Project Context and Design Decisions

### Date: 2025-12-07

---

## 1. Project Overview

Building a database system in Go that combines SQLite's architecture with specialized support for both traditional row-based data and vector data. The implementation follows SQLite's structure and design patterns while adding native vector capabilities.

---

## 2. Requirements Summary

| Requirement | Decision | Rationale |
|-------------|----------|-----------|
| **Primary Use Case** | General-purpose | Must support RAG, recommendations, multi-modal search, and flexible vector+relational needs |
| **Vector Dimensions** | Large (1024-4096) | Support modern embedding models like OpenAI text-embedding-3-large, Cohere |
| **Data Scale** | Large (10M-100M vectors) | Requires efficient ANN algorithms, memory-mapped files, sharding strategies |
| **Distance Metric** | Cosine similarity | Normalized embeddings for semantic similarity; can normalize on insert |
| **Query Patterns** | Hybrid queries | Combine vector similarity scores with SQL aggregations, JOINs, GROUP BY |
| **Concurrency** | Multiple writers, multiple readers | Requires MVCC or sophisticated locking (beyond SQLite's single-writer model) |
| **Recall/Latency Tradeoff** | Configurable | Users can tune per query or per index |
| **SQL Compatibility** | Full SQLite compatibility | Parse and execute any valid SQLite SQL, plus vector extensions |

---

## 3. Architectural Decisions

### 3.1 Implementation Approach: Pure Go with mmap

**Decision:** Rewrite SQLite's storage engine and query processor entirely in Go, using memory-mapped files for performance.

**Rationale:**
- Maximum control over storage layout and vector integration
- No CGo overhead or complexity
- Native vector support from ground up
- Clean Go idioms and interfaces

**Trade-offs:**
- Massive implementation effort (estimated 50K+ lines)
- Must reimplement battle-tested SQLite logic
- Need to prove correctness through extensive testing

### 3.2 Vector Indexing: HNSW (Hierarchical Navigable Small World)

**Decision:** Implement HNSW as the primary vector indexing algorithm.

**Rationale:**
- State-of-the-art for approximate nearest neighbor (ANN) search
- Excellent query speed with configurable recall
- Used by leading vector databases (Pinecone, Qdrant, Weaviate, pgvector)
- Well-documented algorithm with clear implementation path

**Trade-offs:**
- Higher memory usage than alternatives like IVF
- Complex implementation (~3K lines for production quality)
- Requires careful tuning of M and efConstruction parameters

---

## 4. Technical Constraints

### 4.1 Storage
- **File format:** Inspired by SQLite's page-based format
- **Memory mapping:** Use `mmap` for efficient large file access
- **Page size:** Configurable, default 4KB (like SQLite)

### 4.2 Concurrency Model
- **MVCC (Multi-Version Concurrency Control):** Required for multiple writers
- **Isolation level:** Snapshot isolation (similar to SQLite WAL mode, but with write concurrency)
- **Lock granularity:** Row-level or page-level locks for write operations

### 4.3 Vector Storage
- **Normalization:** Vectors normalized on insert for cosine similarity
- **Precision:** float32 for storage (16KB per 4096-dim vector)
- **Storage calculation:** 100M vectors × 4096 dims × 4 bytes = ~1.6TB raw vector data

### 4.4 Query Processing
- **SQL parser:** Build or adapt from SQLite grammar
- **Execution engine:** VDBE-like bytecode interpreter
- **Vector extensions:** SQL functions for similarity search

---

## 5. Proposed Architecture

```
┌─────────────────────────────────────────────────────────────────────┐
│                           User Interface                             │
│                  (Go API + SQL Interface + CLI)                      │
├─────────────────────────────────────────────────────────────────────┤
│                         Query Processor                              │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────────────────┐  │
│  │  SQL Parser  │  │  Optimizer   │  │  VDBE Bytecode Executor  │  │
│  └──────────────┘  └──────────────┘  └──────────────────────────┘  │
├─────────────────────────────────────────────────────────────────────┤
│                         Storage Engine                               │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────────────────┐  │
│  │  B-Tree      │  │  HNSW Index  │  │  Transaction Manager     │  │
│  │  (Row Data)  │  │  (Vectors)   │  │  (MVCC)                  │  │
│  └──────────────┘  └──────────────┘  └──────────────────────────┘  │
├─────────────────────────────────────────────────────────────────────┤
│                           Pager Layer                                │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────────────────┐  │
│  │  Page Cache  │  │  WAL/Journal │  │  mmap Manager            │  │
│  └──────────────┘  └──────────────┘  └──────────────────────────┘  │
├─────────────────────────────────────────────────────────────────────┤
│                         File System                                  │
│              (Single database file + WAL + vector index files)       │
└─────────────────────────────────────────────────────────────────────┘
```

---

## 6. Vector SQL Extensions (Table-Valued Functions)

Vector operations are exposed as **table-valued functions** (like SQLite FTS5), not WHERE clause extensions:

```sql
-- Create table with vector column
CREATE TABLE documents (
    id INTEGER PRIMARY KEY,
    content TEXT,
    embedding VECTOR(1536)  -- 1536-dimensional vector
);

-- Insert with vector
INSERT INTO documents (content, embedding)
VALUES ('Hello world', X'...');  -- binary float32 array

-- Build HNSW index for a vector column
SELECT vector_quantize('documents', 'embedding');

-- KNN search as table-valued function (returns rowid, distance)
SELECT * FROM vector_quantize_scan('documents', 'embedding', ?, 10);

-- Typical usage: JOIN to get full rows with distances
SELECT d.id, d.content, v.distance
FROM documents AS d
JOIN vector_quantize_scan('documents', 'embedding', ?, 20) AS v
ON d.id = v.rowid;

-- Compute distance between two vectors
SELECT vector_distance(v1.embedding, v2.embedding)
FROM documents v1, documents v2
WHERE v1.id = 1 AND v2.id = 2;
```

**Function Signatures:**

| Function | Parameters | Returns |
|----------|------------|---------|
| `vector_quantize` | (table TEXT, column TEXT) | INT (rows indexed) |
| `vector_quantize_scan` | (table TEXT, column TEXT, query BLOB, k INT) | TABLE(rowid INT, distance REAL) |
| `vector_distance` | (v1 BLOB, v2 BLOB) | REAL |

---

## 7. Project Name: TurDB

Suggested name for the project (can be changed).

---

## 8. Reference Materials

- SQLite source code: `./.claude/sqlite/`
- Key SQLite files to study:
  - `btree.c` - B-tree implementation
  - `pager.c` - Page management
  - `vdbe.c` - Virtual machine execution
  - `select.c` - Query processing
  - `btreeInt.h` - Page format specification

---

## 9. Open Questions

1. **Sharding strategy:** How to distribute 100M+ vectors across files?
2. **Compression:** Should we compress vectors (e.g., product quantization)?
3. **Incremental indexing:** How to handle HNSW updates without full rebuilds?
4. **Backup strategy:** Hot backups with MVCC?
5. **Replication:** Future consideration for distributed deployments?

---

## 10. Success Criteria

1. Pass SQLite compatibility test suite (core subset)
2. Vector search achieves >95% recall at <50ms for 10M vectors
3. Support 100 concurrent readers and 10 concurrent writers
4. Single-file database portability (like SQLite)
5. Memory-efficient operation (don't require all vectors in RAM)
