# Query Engine: Code Intelligence

## What It Does

The query engine turns code relationships into AI context. It traverses the knowledge graph and retrieves blocks at microsecond speed, delivering exactly the code context that LLMs need to reason about software.

## The Architecture

### Query Operations
- **FindBlocksQuery**: Get specific blocks by ID
- **SemanticQuery**: Keyword search with relevance scoring
- **QueryResult**: Zero-allocation streaming over results

### Query Engine
- **Block retrieval**: Direct storage access
- **Result formatting**: LLM-optimized output
- **Caching**: LRU cache with TTL for expensive operations

### Graph Traversal
- **A* pathfinding**: Optimal paths with heuristics
- **Bidirectional search**: Meet-in-the-middle for efficiency
- **Depth-limited exploration**: Controlled traversal scope

## Speed Optimizations

### Zero-Allocation Queries
QueryResult returns direct pointers, not copies. Arena memory gets bulk-reset every 100 iterations. Result: <10μs per iteration, even for large result sets.

### Smart Caching
LRU cache with configurable TTL. Cache keys are query hashes for deterministic lookups. Gets invalidated when storage changes.
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
