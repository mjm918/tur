# Memory Optimization Example

This example demonstrates how to configure TurDB for minimal memory usage using PRAGMA commands.

## Running the Example

```bash
cd examples/memory-optimization
go run main.go
```

## What This Example Shows

1. Opens a database with default settings
2. Inserts 100 test rows
3. Queries the data and shows memory usage
4. Applies `PRAGMA optimize_memory` for minimal memory footprint
5. Queries again and compares memory usage
6. Displays current configuration

## Expected Output

You should see significantly lower memory usage after applying `PRAGMA optimize_memory`:

- Default settings: ~28+ MB idle
- Optimized settings: ~1-2 MB idle

## Customization

To try different configurations, replace:

```go
_, err = db.Exec("PRAGMA optimize_memory")
```

With individual PRAGMA commands:

```go
_, err = db.Exec("PRAGMA page_cache_size = 50")
_, err = db.Exec("PRAGMA query_cache_size = 10")
_, err = db.Exec("PRAGMA result_streaming = 'ON'")
// etc.
```

See `docs/memory-optimization.md` for detailed configuration options.
