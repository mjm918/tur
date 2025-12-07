# TurDB Design Document

**Date:** 2025-12-07
**Status:** Approved

---

## 1. Overview

TurDB is a database system in Go that combines SQLite's architecture with native vector support. It provides full SQLite SQL compatibility plus vector operations via table-valued functions.

### Goals

- Full SQLite SQL compatibility
- Native HNSW vector indexing for 1024-4096 dimensional vectors
- Scale to 10M-100M vectors
- Multiple concurrent readers and writers (MVCC)
- Single-file database with mmap for performance
- Configurable recall/latency tradeoff

---

## 2. Architecture

### 2.1 Layer Diagram

```
┌─────────────────────────────────────────┐
│  Layer 1: SQL Interface                 │
│  - User API (db.Query, db.Exec)         │
│  - SQL parser                           │
├─────────────────────────────────────────┤
│  Layer 2: Query Processor               │
│  - AST compiler                         │
│  - VDBE bytecode executor               │
├─────────────────────────────────────────┤
│  Layer 3: Storage Engine                │
│  - B-tree (row data)                    │
│  - HNSW index (vectors)                 │
├─────────────────────────────────────────┤
│  Layer 4: Pager                         │
│  - Page cache (LRU)                     │
│  - mmap manager                         │
│  - WAL for durability                   │
├─────────────────────────────────────────┤
│  Layer 5: MVCC                          │
│  - Transaction management               │
│  - Version chains                       │
│  - Conflict detection                   │
├─────────────────────────────────────────┤
│  Layer 6: OS Interface                  │
│  - File I/O                             │
│  - mmap syscalls                        │
└─────────────────────────────────────────┘
```

### 2.2 Package Structure

```
tur/
├── cmd/tur/              # CLI tool
├── pkg/
│   ├── database/         # DB handle, connections
│   ├── sql/              # Lexer, parser, AST, compiler
│   ├── vdbe/             # Bytecode VM, opcodes, cursors
│   ├── btree/            # B-tree, nodes, cursors
│   ├── hnsw/             # HNSW index, search, build
│   ├── pager/            # Page cache, mmap
│   ├── wal/              # Write-ahead log, checkpoint
│   ├── mvcc/             # Transactions, versions
│   └── types/            # Values, vectors
├── internal/encoding/    # Varint, page serialization
└── test/                 # Test suites
```

---

## 3. File Format

### 3.1 Database File Layout

```
┌────────────────────────────────────────────────────────────┐
│  Page 0: Database Header (4KB)                             │
│  - Magic: "TurDB format 1\0" (16 bytes)                    │
│  - Page size (4 bytes)                                     │
│  - Version (4 bytes)                                       │
│  - Schema root page (4 bytes)                              │
│  - Free list head (4 bytes)                                │
│  - Vector index roots (variable)                           │
├────────────────────────────────────────────────────────────┤
│  Pages 1-N: B-tree pages (4KB default)                     │
├────────────────────────────────────────────────────────────┤
│  Pages N+1-M: HNSW index pages (64KB for vectors)          │
├────────────────────────────────────────────────────────────┤
│  Pages M+1-...: Overflow pages                             │
└────────────────────────────────────────────────────────────┘
```

### 3.2 Page Types

| Type | ID | Size | Purpose |
|------|-----|------|---------|
| `PAGE_BTREE_INTERIOR` | 0x01 | 4KB | B-tree interior node |
| `PAGE_BTREE_LEAF` | 0x02 | 4KB | B-tree leaf with row data |
| `PAGE_HNSW_NODE` | 0x10 | 64KB | HNSW graph node |
| `PAGE_HNSW_META` | 0x11 | 4KB | HNSW index metadata |
| `PAGE_OVERFLOW` | 0x20 | 4KB | Large value continuation |
| `PAGE_FREELIST` | 0x30 | 4KB | Free page tracking |

### 3.3 B-tree Page Structure

```
B-tree Leaf Page (4KB):
┌──────────────────────────────────────┐
│ Header (12 bytes)                    │
│ - Page type (1 byte)                 │
│ - Flags (1 byte)                     │
│ - Cell count (2 bytes)               │
│ - Free space start (2 bytes)         │
│ - Free space end (2 bytes)           │
│ - Right child/overflow ptr (4 bytes) │
├──────────────────────────────────────┤
│ Cell pointer array                   │
│ [2-byte offsets, sorted by key]      │
├──────────────────────────────────────┤
│ Free space                           │
├──────────────────────────────────────┤
│ Cell content (grows upward)          │
│ - Key length (varint)                │
│ - Value length (varint)              │
│ - Key data                           │
│ - Value data (or overflow pointer)   │
└──────────────────────────────────────┘
```

---

## 4. HNSW Vector Index

### 4.1 Configuration

```go
type HNSWConfig struct {
    M              int     // Max connections per node (default: 16)
    MMax0          int     // Max connections at layer 0 (default: 32)
    EfConstruction int     // Build-time search width (default: 200)
    EfSearch       int     // Query-time search width (default: 50)
    Dimension      int     // Vector dimension (1024-4096)
}
```

### 4.2 Node Structure

```
HNSW Node (on disk):
┌─────────────────────────────────────────────────────────┐
│ Node ID (8 bytes)                                       │
│ Level (1 byte)                                          │
│ Row ID (8 bytes) - foreign key to B-tree                │
│ Vector (dimension × 4 bytes) - normalized float32       │
│ Neighbor counts per level (level+1 bytes)               │
│ Neighbor lists:                                         │
│   Level 0: [node_id...] up to MMax0 entries             │
│   Level 1+: [node_id...] up to M entries each           │
└─────────────────────────────────────────────────────────┘
```

### 4.3 Search Algorithm

```
SearchKNN(query, k, efSearch):
  1. ep = entry_point (top level)
  2. For level = max_level down to 1:
       ep = greedy_search(query, ep, level, ef=1)
  3. candidates = search_layer_0(query, ep, efSearch)
  4. Return top-k from candidates by distance
```

### 4.4 Distance Function

Cosine similarity (vectors normalized on insert):
```
cosine_distance(a, b) = 1 - dot_product(a, b)
```

---

## 5. Vector SQL Interface

### 5.1 Table-Valued Functions

Vector operations exposed as SQL functions, not WHERE clause extensions:

```sql
-- Build HNSW index for a vector column
SELECT vector_quantize('table_name', 'column_name');

-- KNN search returning (rowid, distance)
SELECT * FROM vector_quantize_scan('table_name', 'column_name', query_vec, k);

-- Typical usage: JOIN to get full rows
SELECT e.id, e.content, v.distance
FROM documents AS e
JOIN vector_quantize_scan('documents', 'embedding', ?, 20) AS v
ON e.id = v.rowid;
```

### 5.2 Function Signatures

| Function | Parameters | Returns |
|----------|------------|---------|
| `vector_quantize` | (table TEXT, column TEXT) | INT (rows indexed) |
| `vector_quantize_scan` | (table TEXT, column TEXT, query BLOB, k INT) | TABLE(rowid INT, distance REAL) |
| `vector_distance` | (v1 BLOB, v2 BLOB) | REAL |

### 5.3 VECTOR Column Type

```sql
CREATE TABLE documents (
    id INTEGER PRIMARY KEY,
    content TEXT,
    embedding VECTOR(1536)  -- dimension in parentheses
);

INSERT INTO documents (content, embedding)
VALUES ('Hello', X'...');  -- binary float32 array
```

---

## 6. Concurrency Model

### 6.1 MVCC Design

```go
type Transaction struct {
    ID        uint64
    StartTS   uint64    // Snapshot timestamp
    CommitTS  uint64    // 0 if uncommitted
    State     TxState   // Active, Committed, Aborted
}

type RowVersion struct {
    Data      []byte
    CreatedBy uint64    // Creating transaction
    DeletedBy uint64    // Deleting transaction (0 = visible)
    Next      *RowVersion
}
```

### 6.2 Visibility Rules

Row version V visible to transaction T if:
1. V.CreatedBy committed before T.StartTS
2. V.DeletedBy == 0 OR V.DeletedBy not committed at T.StartTS

### 6.3 Write-Ahead Log

```
WAL Frame:
┌─────────────────────────────────────┐
│ Page number (4 bytes)               │
│ Transaction ID (8 bytes)            │
│ Checksum (4 bytes)                  │
│ Commit flag (1 byte)                │
│ Page data (page_size bytes)         │
└─────────────────────────────────────┘
```

### 6.4 Concurrency Guarantees

| Operation | Behavior |
|-----------|----------|
| Read-Read | No blocking (snapshot isolation) |
| Read-Write | No blocking (readers see pre-write state) |
| Write-Write | Row-level conflict detection |

### 6.5 HNSW Concurrency

- **Reads:** Lock-free traversal, atomic neighbor list reads
- **Writes:** Per-node fine-grained locking during updates
- **Rebuilds:** Copy-on-write with atomic swap

---

## 7. User API

### 7.1 Go Interface

```go
// Open database
db, err := tur.Open("mydb.tur")
defer db.Close()

// Configure (optional)
db, err := tur.Open("mydb.tur", tur.Config{
    PageSize:    4096,
    CacheSize:   1000,  // pages
    WALMode:     true,
})

// Standard SQL
rows, err := db.Query("SELECT * FROM users WHERE id = ?", 42)
_, err = db.Exec("INSERT INTO users (name) VALUES (?)", "Alice")

// Transactions
tx, err := db.Begin()
tx.Exec(...)
tx.Commit()

// Vector operations
_, err = db.Exec(`INSERT INTO docs (text, emb) VALUES (?, ?)`,
    "Hello", tur.Vector([]float32{0.1, 0.2, ...}))

db.Exec("SELECT vector_quantize('docs', 'emb')")

rows, err := db.Query(`
    SELECT d.id, v.distance FROM docs d
    JOIN vector_quantize_scan('docs', 'emb', ?, 10) v
    ON d.id = v.rowid`, tur.Vector(query))
```

### 7.2 Core Interfaces

```go
type Database interface {
    Query(sql string, args ...interface{}) (*Rows, error)
    Exec(sql string, args ...interface{}) (Result, error)
    Begin() (*Tx, error)
    Close() error
}

type Tx interface {
    Query(sql string, args ...interface{}) (*Rows, error)
    Exec(sql string, args ...interface{}) (Result, error)
    Commit() error
    Rollback() error
}
```

---

## 8. Implementation Phases

### Phase 1: Foundation
- Pager with mmap
- Basic page read/write
- WAL infrastructure

### Phase 2: B-tree
- B-tree insert/get/delete
- Cursor iteration
- Page splits and merges

### Phase 3: SQL Core
- Lexer and parser
- Basic VDBE opcodes
- CREATE TABLE, INSERT, SELECT

### Phase 4: MVCC
- Transaction management
- Version chains
- Snapshot isolation

### Phase 5: HNSW
- HNSW build algorithm
- KNN search
- Persistence to pages

### Phase 6: Vector SQL
- VECTOR column type
- vector_quantize function
- vector_quantize_scan virtual table

### Phase 7: Polish
- Full SQL compatibility
- Performance optimization
- CLI tool

---

## 9. Success Criteria

1. Pass core SQLite compatibility tests
2. Vector search: >95% recall at <50ms for 10M vectors
3. Support 100 concurrent readers, 10 concurrent writers
4. Single portable database file
5. Memory-efficient (don't require all vectors in RAM)

---

## 10. Open Questions (Deferred)

1. Sharding for 100M+ vectors
2. Vector compression (product quantization)
3. Incremental HNSW updates vs. rebuild
4. Hot backup strategy
5. Future distributed mode
