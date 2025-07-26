# CortexDB Design

## Philosophy

CortexDB eliminates LLM context drift through principled engineering. Three non-negotiable rules:

1. **Simplicity is the Prerequisite for Reliability** - Complex code kills systems
2. **Explicit is Better Than Implicit** - No hidden control flow, no surprise allocations
3. **Correctness is Not Negotiable** - Architecture must make incorrect states unrepresentable

Inspired by TigerBeetle's financial-grade reliability applied to AI infrastructure.

## Why Zig?

**No hidden complexity.** Every allocation explicit. Every control flow obvious. Zero runtime surprises.

This eliminates entire bug classes that plague databases: use-after-free, double-free, memory leaks, data races. When your foundation is solid, you can build fast.

## Core Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                     CortexDB Engine                         │
├─────────────────────────────────────────────────────────────┤
│  Query Engine                    Storage Engine             │
│  ├── Block Lookups              ├── Coordinator             │
│  ├── Graph Traversal            ├── MemtableManager         │
│  └── Result Formatting          │   ├── BlockIndex          │
│                                 │   └── GraphEdgeIndex      │
│                                 ├── SSTableManager          │
│                                 │   ├── File Discovery      │
│                                 │   └── Compaction          │
│                                 └── WAL (Durability)        │
├─────────────────────────────────────────────────────────────┤
│                       VFS Abstraction                       │
│              ProductionVFS  │  SimulationVFS                │
└─────────────────────────────────────────────────────────────┘
```

**Coordinator Pattern**: Main engines delegate to specialized managers. No monolithic modules.

## Memory Management: Arena-per-Subsystem

**The Problem**: Complex allocator hierarchies create cross-allocator corruption.

**The Solution**: Each subsystem owns an `ArenaAllocator` for all its state.

```zig
// MemtableManager owns ALL block strings
arena: std.heap.ArenaAllocator,
blocks: HashMap(BlockId, ContextBlock, ...),

// Bulk deallocation in O(1)
pub fn clear(self: *MemtableManager) void {
    self.blocks.clearRetainingCapacity();
    _ = self.arena.reset(.retain_capacity);
}
```

**Benefits**:

- Eliminates use-after-free by design
- O(1) cleanup of complex state
- Zero memory leaks from incomplete cleanup

## Concurrency: Single-Threaded by Design

**Zero data races.** All core logic runs on one thread. Concurrency achieved through async I/O, not threads.

```zig
// Enforced at runtime in debug builds
concurrency.assert_main_thread();
```

**Why**: Threading bugs are the hardest to debug and most dangerous in production. Better to be fast on one thread than correct sometimes on many.

## Storage Engine: Decomposed LSM-Tree

**LSM-tree optimized for read-heavy LLM workloads:**

### MemtableManager

- In-memory `BlockIndex` + `GraphEdgeIndex`
- Arena-allocated strings for blocks/edges
- Handles all write operations with WAL durability

### SSTableManager

- Immutable on-disk sorted tables
- File discovery and compaction coordination
- Read-optimized for graph traversal queries

### WAL (Write-Ahead Log)

- Durability guarantee for all mutations
- Streaming recovery for fast startup
- Segmented 64MB files for rotation

**Data Flow**: `WAL → MemtableManager → SSTableManager`

Write operations hit WAL first, then memtable. Compaction moves memtable to SSTables. Queries read memtable first, then SSTables.

## Testing Philosophy: Don't Mock, Simulate

**We don't mock filesystem calls.** We run real code against a simulated filesystem.

**VFS Abstraction**: Production uses real OS filesystem. Tests use `SimulationVFS` - deterministic, in-memory filesystem with fault injection.

**Hostile Environment Testing**:

- Network partitions during writes
- Disk corruption in WAL recovery
- I/O errors during compaction
- Power loss at arbitrary points

**Result**: Byte-for-byte reproducible tests of scenarios that destroy other databases.

## Component Lifecycle: Two-Phase Initialization

**Phase 1**: `init()` - Memory allocation only, no I/O
**Phase 2**: `startup()` - I/O operations, can fail

```zig
var storage = try StorageEngine.init(allocator, vfs, data_dir);
try storage.startup(); // File discovery, WAL recovery
```

**Why**: Separates resource allocation from resource access. Makes error handling explicit and testing deterministic.

## Data Model: Knowledge Graphs

### Context Blocks

Atomic knowledge units with:

- Unique ID and version
- Source URI and metadata
- Content (function, paragraph, config)

### Graph Edges

Typed relationships:

- `IMPORTS`: Module dependencies
- `CALLS`: Function call graph
- `REFERENCES`: Variable/symbol usage

**Query Model**: Start with seed blocks, traverse relationships, return interconnected context subgraph.

## Performance by Design

**Hot Path Optimizations**:

- No allocations during queries (pre-allocated buffers)
- Linear scans for small collections (<16 items)
- Single-threaded avoids cache bouncing
- Arena bulk operations over individual frees

**Write Path**:

- WAL append-only writes
- Memtable arena allocations
- Background compaction doesn't block writes

**Target Performance**:

- <1ms block lookups
- <10ms graph traversals (3-hop)
- 10K writes/sec sustained

## Error Handling

**Specific Error Sets**: No `anyerror` in public APIs.

```zig
const StorageError = error{
    BlockNotFound,
    CorruptedWALEntry,
} || std.fs.File.ReadError;
```

**Rich Error Context**: Debug builds include file paths, line numbers, expected vs actual values.

**Defensive Programming**: Assertions everywhere in debug builds. Zero cost in release.

## Future: Replication

**Design Decision**: Primary-backup over consensus protocols.

**Rationale**: LLM context has different consistency requirements than financial transactions. Eventual consistency acceptable. WAL shipping simpler and more reliable than Paxos/Raft.

**Implementation**: Primary ships WAL segments to replicas. Read scalability through replica fanout.

## Anti-Patterns Banned

- Global state
- Hidden allocations
- Generic error types in APIs
- I/O operations in `init()` functions
- Complex inheritance hierarchies
- Thread-based concurrency in core logic

## Design Goals Achieved

- **Sub-millisecond Queries**: Architecture optimized for read latency
- **Memory Safety**: Arena model eliminates use-after-free by design
- **Deterministic Testing**: VFS enables reproduction of complex failures
- **Zero Dependencies**: Pure Zig, no external libraries
- **Defensive Programming**: Comprehensive assertion framework
- **Modular Architecture**: Coordinator pattern with focused managers

CortexDB proves that careful architecture beats clever optimization. Simple, explicit, correct.
