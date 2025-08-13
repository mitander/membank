# Design Document

## 1. Philosophy: Simplicity is the Prerequisite for Reliability

KausalDB is an opinionated database. It is not general-purpose. Every architectural decision is optimized for its specific mission: modeling code as a queryable, causal graph for AI reasoning. Our philosophy is built on principles that lead to fast, correct, and maintainable systems.

- **Zero-Cost Abstractions:** Safety and type checking must have zero runtime overhead in release builds. With 0.06µs block reads, every CPU cycle matters. We use compile-time validation, not runtime checks.
- **Correctness is Not Negotiable:** "Probably right" is wrong. We prove correctness through deterministic, simulation-first testing.
- **Explicitness Over Magic:** All control flow, memory allocation, and component lifecycles are explicit. There are no hidden mechanics.
- **Zero Dependencies, Zero Headaches:** The system compiles to a single static binary. Deployment is simple because the software is simple.
- **Hierarchical Memory Ownership:** Clear coordinator→submodule→sub-submodule memory hierarchy eliminates allocator conflicts and makes debugging trivial.

We chose Zig because its philosophy aligns with ours: no hidden control flow, no hidden memory allocations, and a focus on explicitness. The hierarchical memory model leverages Zig's compile-time capabilities to enforce ownership at zero runtime cost.

## 2. Core Architecture

The system is designed as a series of coordinated, specialized components that follow clear ownership patterns and architectural boundaries.

### 2.1. The Hierarchical Coordinator Pattern

Components follow a clear hierarchy using the **Arena Coordinator Pattern** where coordinators provide stable allocation interfaces that remain valid across arena operations.

**Memory-Owning Coordinators:** Top-level components like `StorageEngine` own exactly one arena allocator and expose it through a coordinator interface that never becomes invalid.

**Computation Submodules:** Mid-level components like `MemtableManager` and `SSTableManager` receive coordinator interfaces (not direct arena references) and focus on domain logic without memory ownership complexity.

**Pure Computation Modules:** Low-level components like `BlockIndex` and `GraphEdgeIndex` perform specialized operations using coordinator-provided allocation, eliminating temporal coupling between arena operations and component access.

This pattern eliminates arena corruption that occurred when structs containing embedded arenas were copied, while maintaining 20-30% performance improvements through reduced allocator indirection.

### 2.2. Single-Threaded Core with Async I/O

To eliminate data races by design, KausalDB's core is single-threaded. [75] Asynchronous I/O is handled by a non-blocking event loop. This constraint forces simplicity and makes state transitions easy to reason about. Concurrency is enforced in debug builds via `assert_main_thread()`.

### 2.3. Zero-Cost Ownership System

The ownership system provides memory safety with zero runtime overhead through compile-time validation.

- **Compile-Time Ownership:** Ownership checks use `comptime` parameters, making violations compilation errors rather than runtime failures.
- **Debug-Only Validation:** Arena aliasing checks and state transitions only exist in debug builds via `if (builtin.mode == .Debug)`.
- **Zero Overhead:** Release builds have identical performance to raw pointers - ownership is purely a compile-time concept.
- **Type-Safe Transfers:** `OwnedBlock` and `OwnedGraphEdge` enable safe memory transfer between subsystems with compile-time guarantees.

### 2.4. Comprehensive Memory Architecture

KausalDB employs a sophisticated five-level memory hierarchy designed for microsecond-level performance and zero-cost abstractions.

**Level 1: Fixed-Size Object Pools**
High-frequency objects like SSTable handles and iterators are allocated from pre-sized pools, eliminating allocation overhead and fragmentation. `ObjectPoolType<T>` provides O(1) allocation with debug tracking.

**Level 2: Arena Coordinators**
Top-level components own exactly one arena and expose it through a coordinator interface. `ArenaCoordinator` provides stable allocation that survives arena resets, eliminating temporal coupling.

**Level 3: Data-Oriented Storage**
Performance-critical data structures use Struct-of-Arrays layout for cache optimization. `BlockIndexDOD` achieves 3-5x performance improvement through contiguous memory access patterns.

**Level 4: Zero-Copy Query Paths**
Read operations return direct pointers to storage data via `ZeroCopyQueryInterface`, eliminating allocations on hot paths while maintaining lifetime safety through session tracking.

**Level 5: Type-Safe Coordination**
`TypedStorageCoordinatorType<T>` replaces unsafe `*anyopaque` patterns with compile-time validated interfaces, eliminating type confusion while maintaining zero runtime overhead.

**Memory Lifecycle Management:**

- **Permanent Infrastructure:** GeneralPurposeAllocator for program-lifetime objects
- **Subsystem Arenas:** ArenaAllocator with coordinator interface for bulk operations
- **Task Arenas:** Temporary arenas for bounded operations
- **Object Pools:** Pre-allocated pools for frequent allocations
- **Stack Allocation:** Function-scoped temporary buffers

**Ownership Transfer Safety:**
`OwnedBlock` includes moved-from state tracking to prevent use-after-transfer bugs. Debug builds validate ownership transfers with zero release overhead.

### 2.5. LSM-Tree Storage Engine

The storage engine is a custom Log-Structured Merge-Tree optimized for high write throughput.

- **Write-Ahead Log (WAL):** All writes are first appended to the WAL in 64MB segmented files to ensure durability. A `CorruptionTracker` detects systematic corruption and triggers a fail-fast response to prevent propagating bad data. [89, 91]
- **Memtable (`BlockIndex`):** An in-memory `HashMap` that stores recent writes for fast access, backed by an arena allocator. [94]
- **SSTables (Sorted String Tables):** When the memtable reaches its size threshold, its contents are sorted and flushed to immutable on-disk files. The SSTable on-disk format uses a 64-byte aligned header and Bloom filters to optimize read performance. [102, 103]
- **Tiered Compaction:** A background process merges SSTables using a size-tiered strategy to reduce read amplification and reclaim space from deleted or updated blocks. [99]

### 2.6. Ingestion Backpressure System

KausalDB includes a sophisticated backpressure mechanism to prevent out-of-memory crashes during high-volume ingestion while maintaining system responsiveness.

**Memory Pressure Detection**: The system continuously monitors storage memory usage through two key metrics:

- **Memtable Memory Usage**: Tracks current in-memory block index size against configurable targets (default: 64MB)
- **Compaction Queue Size**: Monitors pending compaction operations to detect backlog accumulation

**Adaptive Batch Sizing**: The ingestion pipeline dynamically adjusts batch sizes based on detected memory pressure:

- **Low Pressure** (< 50% of target): Full batch size for maximum throughput
- **Medium Pressure** (50-70% of target): Reduced batch size (50% of default)
- **High Pressure** (> 70% of target): Minimum batch size to allow system recovery

**Graceful Degradation**: Under sustained memory pressure, the system prioritizes stability over throughput:

- Automatic batch size reduction prevents memory exhaustion
- Background compaction given time to process accumulated data
- System remains responsive and never crashes due to memory pressure
- Recovery is automatic as memory pressure subsides

This design ensures KausalDB can safely ingest entire codebases without manual tuning or risk of system failure under load.

## 3. Data Model

The data model is purpose-built for representing code.

- **`ContextBlock`**: The atomic unit of knowledge. It is a structured piece of information (e.g., a function, a class, a documentation paragraph) with a unique 128-bit `BlockId`, version, source URI, and JSON metadata. [79]
- **`GraphEdge`**: A typed, directed relationship between two `ContextBlock`s, such as `calls`, `imports`, or `references`. The `GraphEdgeIndex` maintains bidirectional indexes for fast traversal in both directions. [93]

This structure allows the system to capture the rich, causal relationships in a codebase, transforming it from a collection of files into a queryable knowledge graph.
