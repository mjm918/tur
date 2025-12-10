# Memory Optimization Guide

TurDB supports configurable memory usage through PRAGMA commands. By default, the database uses reasonable cache sizes optimized for performance. For memory-constrained environments, you can reduce memory usage to approximately 1MB when idle.

## Quick Start: Minimal Memory Configuration

To configure TurDB for minimal memory usage (~1MB idle):

```sql
PRAGMA optimize_memory;
```

This single command sets all memory-related configurations to their minimal values.

## Individual PRAGMA Commands

### PRAGMA page_cache_size

Controls the maximum number of database pages kept in memory.

**Default:** 1000 pages (~4 MB)

**Set cache size:**
```sql
PRAGMA page_cache_size = 10;  -- Minimal: 10 pages (~40 KB)
```

**Get current size:**
```sql
PRAGMA page_cache_size;
```

**Recommendations:**
- Minimal (10): ~40 KB, slower for repeated access
- Small (50): ~200 KB, good for embedded systems
- Default (1000): ~4 MB, good for general use
- Large (5000): ~20 MB, good for heavy workloads

---

### PRAGMA query_cache_size

Controls the maximum number of cached query results.

**Default:** 1000 entries (~24+ MB depending on result sizes)

**Set cache size:**
```sql
PRAGMA query_cache_size = 0;   -- Disable query cache
PRAGMA query_cache_size = 10;  -- Minimal cache
```

**Get current size:**
```sql
PRAGMA query_cache_size;
```

**Recommendations:**
- Disabled (0): No cache overhead, slower for repeated queries
- Minimal (10): ~240 KB, good for memory-constrained systems
- Default (1000): ~24+ MB, good for read-heavy workloads

---

### PRAGMA vdbe_max_registers

Controls the initial number of registers allocated for VDBE virtual machines.

**Default:** 16 registers (~2 KB per VM)

**Set register count:**
```sql
PRAGMA vdbe_max_registers = 4;  -- Minimal
```

**Get current count:**
```sql
PRAGMA vdbe_max_registers;
```

**Note:** VMs can dynamically expand if more registers are needed, so this only affects initial allocation.

**Recommendations:**
- Minimal (4): ~512 bytes, may require reallocation
- Default (16): ~2 KB, good for most queries

---

### PRAGMA vdbe_max_cursors

Controls the initial number of cursors allocated for VDBE virtual machines.

**Default:** 8 cursors (~640 bytes per VM)

**Set cursor count:**
```sql
PRAGMA vdbe_max_cursors = 2;  -- Minimal
```

**Get current count:**
```sql
PRAGMA vdbe_max_cursors;
```

**Note:** VMs can dynamically expand if more cursors are needed.

**Recommendations:**
- Minimal (2): ~160 bytes, good for simple queries
- Default (8): ~640 bytes, good for complex joins

---

### PRAGMA memory_budget

Controls the total memory budget limit (in MB) for the database.

**Default:** 256 MB

**Set memory budget:**
```sql
PRAGMA memory_budget = 1;  -- 1 MB (minimal)
```

**Get current budget:**
```sql
PRAGMA memory_budget;
```

**Minimum:** 1 MB

**Recommendations:**
- Minimal (1 MB): Extreme memory constraint, frequent evictions
- Small (10 MB): Good for embedded systems
- Default (256 MB): Good for general use
- Large (1024+ MB): Good for in-memory caching

---

### PRAGMA result_streaming

Enables streaming mode for query results to avoid buffering entire result sets.

**Default:** OFF

**Enable streaming:**
```sql
PRAGMA result_streaming = 'ON';
```

**Disable streaming:**
```sql
PRAGMA result_streaming = 'OFF';
```

**Get current mode:**
```sql
PRAGMA result_streaming;
```

**Behavior:**
- OFF: All results buffered in memory before returning (faster, more memory)
- ON: Results buffered in batches of 100 rows (slower, less memory)

**Recommendations:**
- Use ON for large result sets (1000+ rows)
- Use OFF for small result sets and when performance is critical

---

## Example Configurations

### Configuration 1: Ultra-Low Memory (~1 MB idle)

```sql
PRAGMA optimize_memory;
```

Or manually:

```sql
PRAGMA page_cache_size = 10;
PRAGMA query_cache_size = 0;
PRAGMA vdbe_max_registers = 4;
PRAGMA vdbe_max_cursors = 2;
PRAGMA memory_budget = 1;
PRAGMA result_streaming = 'ON';
```

**Use case:** IoT devices, embedded systems, containers with strict memory limits

---

### Configuration 2: Balanced (~10 MB)

```sql
PRAGMA page_cache_size = 100;
PRAGMA query_cache_size = 10;
PRAGMA vdbe_max_registers = 8;
PRAGMA vdbe_max_cursors = 4;
PRAGMA memory_budget = 10;
PRAGMA result_streaming = 'ON';
```

**Use case:** Mobile apps, small services, edge computing

---

### Configuration 3: Performance (Default)

```sql
-- No configuration needed, these are the defaults
PRAGMA page_cache_size = 1000;
PRAGMA query_cache_size = 1000;
PRAGMA vdbe_max_registers = 16;
PRAGMA vdbe_max_cursors = 8;
PRAGMA memory_budget = 256;
PRAGMA result_streaming = 'OFF';
```

**Use case:** Servers, desktop applications, development

---

## Memory Usage Estimation

| Component | Default | Minimal | Ultra-Low |
|-----------|---------|---------|-----------|
| Page Cache | ~4 MB | ~400 KB | ~40 KB |
| Query Cache | ~24 MB | ~240 KB | 0 KB |
| VDBE Registers | ~2 KB | ~1 KB | ~512 bytes |
| VDBE Cursors | ~640 bytes | ~320 bytes | ~160 bytes |
| **Idle Total** | ~28+ MB | ~640 KB | ~41 KB |

**Note:** Actual memory usage depends on workload. Active queries, transactions, and large result sets will increase memory usage.

---

## Best Practices

1. **Set PRAGMAs at connection time:** Configure memory settings immediately after opening the database for consistent behavior.

2. **Profile your workload:** Use different configurations and measure actual memory usage with your queries.

3. **Trade-offs:** Lower memory = slower performance due to cache misses and reallocations.

4. **Streaming for large results:** Always enable `result_streaming` when querying large tables.

5. **Monitor pressure:** The memory budget system will automatically evict cached data when under pressure.
