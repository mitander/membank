# Query Engine Architecture

## Overview

The query engine provides graph traversal and block retrieval operations over the LSM-Tree storage layer. The design prioritizes simplicity and performance with zero-allocation iteration patterns.

## Core Components

### Query Operations (`src/query/operations.zig`)

Provides fundamental block retrieval operations:

- **`FindBlocksQuery`**: Retrieve specific blocks by ID list
- **`QueryResult`**: Zero-allocation streaming iterator over results
- **`SemanticQuery`**: Keyword-based search with similarity scoring
- **`SemanticQueryResult`**: Results container for semantic search

### Query Engine (`src/query/engine.zig`)

High-level query coordination and caching:

- **Block retrieval**: Direct storage lookups
- **Query result formatting**: LLM-optimized output
- **Query caching**: LRU cache with TTL for expensive operations

### Graph Traversal (`src/query/traversal.zig`)

Advanced graph algorithms for relationship exploration:

- **A* search**: Optimal pathfinding with heuristics
- **Bidirectional search**: Efficient path discovery
- **Depth-limited traversal**: Controlled exploration depth

## Performance Optimizations

### Zero-Allocation Iteration

**QueryResult** uses arena-based memory management to eliminate per-result allocation overhead:

- Returns `*const ContextBlock` pointers instead of cloned values
- Uses temporary arena with periodic reset (every 100 iterations)
- Achieves <10μs per iteration for large result sets

### Query Caching (`src/query/cache.zig`)

LRU-based caching for expensive operations:

- **Cache key**: Query hash for deterministic lookup
- **TTL**: Configurable time-based expiration
- **Invalidation**: Cache cleared on storage mutations
- **Memory**: Arena-based cache entries for O(1) cleanup

## Query Types

### Block Retrieval

Direct lookup operations for known block IDs:

```zig
const query = FindBlocksQuery{ .block_ids = &[_]BlockId{id1, id2, id3} };
var result = try execute_find_blocks(allocator, storage_engine, query);
while (try result.next()) |block| {
    // Process block - no cleanup needed (arena-managed)
}
```

### Semantic Search

Keyword-based search with similarity scoring:

```zig
const query = SemanticQuery.init("function authentication");
query.max_results = 50;
query.similarity_threshold = 0.7;
var result = try execute_keyword_query(allocator, storage_engine, query);
```

### Graph Traversal

Explore relationships between blocks:

```zig
// Find all blocks that depend on target_id (2 hops max)
var traversal = GraphTraversal.init(storage_engine, target_id);
var paths = try traversal.depth_limited_search(.outgoing, 2, allocator);
```

## Design Principles

1. **Zero-allocation hot paths**: Simple queries avoid allocator overhead
2. **Streaming execution**: Large result sets use iterator patterns
3. **Deterministic results**: Query results are consistent and reproducible
4. **Arena-based cleanup**: Bulk memory deallocation for complex operations

## Error Handling

All query operations return `QueryError` for consistent error handling:

- `BlockNotFound`: Requested block doesn't exist
- `TooManyResults`: Query exceeds configured limits
- `InvalidSemanticQuery`: Malformed search parameters
- `OutOfMemory`: Insufficient memory for operation
