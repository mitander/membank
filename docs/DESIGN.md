# Design Philosophy

*"Simplicity is the prerequisite for reliability."* —Dijkstra

## The Vision

KausalDB isn't another database. It's the missing piece in how we think about code.

Every line of software you write exists in a web of relationships—functions that call each other, modules that import dependencies, classes that inherit behavior. This isn't metadata; it's the fundamental structure of computation. Yet we store and query code as if it were flat text.

That disconnect breaks AI reasoning about software. Standard RAG feeds language models chunks of "semantically similar" text, hoping proximity in embedding space captures actual relationships. It doesn't. Code has *causal* structure that semantic search cannot capture.

KausalDB models code as it actually exists: a directed graph of dependencies and relationships. Query the graph, get the truth.

## Core Principles

### Simplicity as Strategy

Complex systems fail in complex ways. We optimize for simplicity not because it's easy, but because it's the only path to reliability. Every abstraction must justify its existence. Every feature must solve a real problem.

- **Single-threaded core**: Eliminates data races by design
- **Zero dependencies**: One binary, no configuration, no surprises  
- **Explicit everything**: No hidden allocations, no magic, no assumptions
- **Arena memory**: O(1) cleanup eliminates entire classes of bugs

### Correctness Over Convenience  

"Probably works" is another way of saying "broken." We prove correctness through simulation-first testing that validates the actual production code against catastrophic failures—disk corruption, network partitions, power loss.

- **Deterministic testing**: Byte-for-byte reproducible failure scenarios
- **Comprehensive validation**: 500+ tests, chaos testing, performance regression detection
- **Zero-cost abstractions**: Safety checks compile away in release builds
- **Memory safety**: Arena-per-subsystem eliminates use-after-free and double-free

### Performance Through Design

Performance isn't an afterthought. The architecture optimizes for microsecond-level operations because that's what AI workloads demand.

- **LSM-tree storage**: Optimized for high-volume code ingestion
- **Cache-friendly layouts**: Struct-of-Arrays for hot paths
- **Zero-copy queries**: Direct pointers into storage, no allocations
- **Object pooling**: Pre-allocated handles for frequent operations

*Current performance: 47K writes/sec, 16.7M reads/sec, 0.06µs block access*

## Architecture

### The Graph Data Model

Code has two fundamental primitives:

**ContextBlocks** - Units of meaning (functions, classes, documentation)
```zig
const ContextBlock = struct {
    id: BlockId,        // 128-bit UUID
    version: u64,       // For conflict resolution  
    source_uri: []const u8,
    metadata_json: []const u8,
    content: []const u8,
};
```

**GraphEdges** - Relationships between blocks (calls, imports, references)
```zig  
const GraphEdge = struct {
    from: BlockId,
    to: BlockId,
    edge_type: EdgeType,
    metadata: []const u8,
};
```

This captures the causal structure of software. A function `authenticate_user` doesn't just contain some text about authentication—it *calls* `hash_password`, *imports* `crypto`, and is *called by* `login_handler`. The graph makes these relationships queryable.

### LSM-Tree Storage Engine

Built for high-volume code ingestion with strong durability guarantees:

**Write Path:**
1. **Write-Ahead Log** - Every operation goes to segmented WAL files (64MB chunks)  
2. **Memtable** - In-memory HashMap for recent writes, backed by arena allocator
3. **SSTable Flush** - When memtable fills, sorted flush to immutable disk files
4. **Background Compaction** - Tiered merging reduces read amplification

**Read Path:**  
1. **Memtable Lookup** - Check recent writes first (fastest)
2. **SSTable Search** - Binary search through sorted disk files
3. **Bloom Filters** - Skip SSTables that definitely don't contain the key
4. **Zero-Copy Returns** - Direct pointers into storage, no allocations

### Memory Architecture

Five levels of memory management optimize for different access patterns:

**Level 1: Object Pools**  
High-frequency allocations (SSTable handles, iterators) use pre-allocated pools. O(1) allocation, zero fragmentation.

**Level 2: Arena Coordinators**  
Top-level components own exactly one arena, exposed through a coordinator interface that survives arena resets. Eliminates temporal coupling.

**Level 3: Data-Oriented Layout**  
Performance-critical structures use Struct-of-Arrays for cache efficiency. 3-5x performance improvement on hot paths.

**Level 4: Zero-Copy Queries**  
Read operations return direct pointers into storage via session tracking. No allocations on read path.

**Level 5: Type-Safe Coordination**  
Compile-time validated interfaces replace `*anyopaque` patterns. Type safety with zero runtime cost.

### Single-Threaded Core

Data races are eliminated by design through a single-threaded execution model with async I/O. This constraint forces architectural simplicity and makes state transitions trivial to reason about.

Debug builds enforce this with `assert_main_thread()` calls. The event loop handles I/O without blocking the core.

### Ownership System  

Memory safety through compile-time ownership tracking:

```zig
// Ownership transfers are explicit and type-safe
var owned_block = try OwnedBlock.create(allocator, block_data);
var transferred = owned_block.transfer(); // owned_block now invalid
```

- **Compile-time checks**: Ownership violations become compilation errors
- **Zero runtime cost**: Release builds have identical performance to raw pointers  
- **Debug validation**: Moved-from detection prevents use-after-transfer bugs

## Why Zig?

Zig's philosophy aligns perfectly with ours: no hidden control flow, no hidden allocations, explicit everything. The compile-time system enables zero-cost abstractions while maintaining C-level performance.

```zig
// Ownership is enforced at compile time, costs nothing at runtime
pub fn transfer(comptime T: type, owned: *Owned(T)) T {
    if (builtin.mode == .Debug) {
        std.debug.assert(!owned.moved_from);
        owned.moved_from = true; 
    }
    return owned.data;
}
```

## Testing Philosophy

### Simulation-First Approach

Instead of mocking components, we run the actual production code inside a deterministic simulation environment. Every I/O operation goes through a Virtual File System that can simulate disk corruption, network failures, and power loss.

**Benefits:**
- Test the real code, not approximations
- Byte-for-byte reproducible scenarios  
- Comprehensive failure mode coverage
- No test/production differences

### Chaos Testing

The fuzzer runs 500K+ iterations testing random operations against the storage engine. Recent performance optimization achieved 584x improvement (48 → 28K+ iterations/second) enabling comprehensive CI integration.

**Profiles:**
- **Quick**: 500K iterations (~30s) for CI validation
- **Deep**: 10M iterations (~10min) for nightly testing  
- **Production**: Continuous with crash monitoring

### Performance Validation

Every CI run includes performance regression detection. Benchmarks validate critical paths maintain microsecond-level performance:

- Block writes: <100µs target  
- Block reads: <50µs target
- Single queries: <10µs target

## The Missing Piece

Most databases are built for business applications—user records, financial transactions, content management. They optimize for different access patterns and consistency models.

KausalDB is built specifically for code analysis. It understands that software is a graph, that relationships matter more than individual pieces, and that AI systems need structural truth, not semantic approximations.

We're not trying to replace your primary database. We're filling the gap between flat text storage and intelligent code reasoning.

---

*This is the foundation. Everything else is implementation details.*