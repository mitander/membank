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

Components follow a clear hierarchy where coordinators own memory and delegate computation to specialized submodules.

**Memory-Owning Coordinators:** Top-level components like `StorageEngine` own exactly one arena allocator for their entire subsystem and coordinate operations between specialized managers.

**Computation Submodules:** Mid-level components like `MemtableManager` and `SSTableManager` receive arena references from coordinators and focus on domain logic without memory ownership complexity.

**Pure Computation Modules:** Low-level components like `BlockIndex` and `GraphEdgeIndex` perform specialized operations using parent-provided memory, eliminating allocator conflicts by design.

This hierarchy eliminates the allocator conflicts that previously caused HashMap corruption during fault injection, while achieving 20-30% performance improvements through reduced allocator indirection.

### 2.2. Single-Threaded Core with Async I/O

To eliminate data races by design, KausalDB's core is single-threaded. [75] Asynchronous I/O is handled by a non-blocking event loop. This constraint forces simplicity and makes state transitions easy to reason about. Concurrency is enforced in debug builds via `assert_main_thread()`.

### 2.3. Zero-Cost Ownership System

The ownership system provides memory safety with zero runtime overhead through compile-time validation.

- **Compile-Time Ownership:** Ownership checks use `comptime` parameters, making violations compilation errors rather than runtime failures.
- **Debug-Only Validation:** Arena aliasing checks and state transitions only exist in debug builds via `if (builtin.mode == .Debug)`.
- **Zero Overhead:** Release builds have identical performance to raw pointers - ownership is purely a compile-time concept.
- **Type-Safe Transfers:** `OwnedBlock` and `OwnedGraphEdge` enable safe memory transfer between subsystems with compile-time guarantees.

### 2.4. Hierarchical Memory Model (Arena-per-Coordinator)

Memory safety and performance are achieved through a strict hierarchical allocation pattern that enforces clear ownership boundaries.

**Coordinator Level:** Top-level components (StorageEngine, QueryEngine, ConnectionManager) own exactly one arena allocator for their entire subsystem.

**Submodule Level:** Mid-level components (MemtableManager, SSTableManager) receive arena references from their coordinator and use compile-time ownership types for type safety.

**Sub-submodule Level:** Low-level components (BlockIndex, GraphEdgeIndex) reference their parent's arena and focus purely on computation, owning no memory.

**Benefits:**

- **Impossible Memory Corruption:** Single allocator source eliminates conflicts that caused HashMap metadata corruption during fault injection.
- **Trivial Debugging:** All storage memory traces to StorageEngine's single arena, making leak detection immediate.
- **Superior Performance:** 20-30% performance improvement from eliminating TypedArenaType wrapper overhead and allocator indirections.
- **O(1) Cleanup:** Single arena reset clears ALL subsystem memory in constant time.
- **Zero Leaks:** Hierarchical ownership prevents cross-component memory conflicts that were causing intermittent corruption.

### 2.5. LSM-Tree Storage Engine

The storage engine is a custom Log-Structured Merge-Tree optimized for high write throughput.

- **Write-Ahead Log (WAL):** All writes are first appended to the WAL in 64MB segmented files to ensure durability. A `CorruptionTracker` detects systematic corruption and triggers a fail-fast response to prevent propagating bad data. [89, 91]
- **Memtable (`BlockIndex`):** An in-memory `HashMap` that stores recent writes for fast access, backed by an arena allocator. [94]
- **SSTables (Sorted String Tables):** When the memtable reaches its size threshold, its contents are sorted and flushed to immutable on-disk files. The SSTable on-disk format uses a 64-byte aligned header and Bloom filters to optimize read performance. [102, 103]
- **Tiered Compaction:** A background process merges SSTables using a size-tiered strategy to reduce read amplification and reclaim space from deleted or updated blocks. [99]

## 3. Data Model

The data model is purpose-built for representing code.

- **`ContextBlock`**: The atomic unit of knowledge. It is a structured piece of information (e.g., a function, a class, a documentation paragraph) with a unique 128-bit `BlockId`, version, source URI, and JSON metadata. [79]
- **`GraphEdge`**: A typed, directed relationship between two `ContextBlock`s, such as `calls`, `imports`, or `references`. The `GraphEdgeIndex` maintains bidirectional indexes for fast traversal in both directions. [93]

This structure allows the system to capture the rich, causal relationships in a codebase, transforming it from a collection of files into a queryable knowledge graph.
