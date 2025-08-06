# Design Document

## 1. Philosophy: Simplicity is the Prerequisite for Reliability

KausalDB is an opinionated database. It is not general-purpose. Every architectural decision is optimized for its specific mission: modeling code as a queryable, causal graph for AI reasoning. Our philosophy is built on principles that lead to fast, correct, and maintainable systems.

-   **Correctness is Not Negotiable:** "Probably right" is wrong. We prove correctness through deterministic, simulation-first testing.
-   **Explicitness Over Magic:** All control flow, memory allocation, and component lifecycles are explicit. There are no hidden mechanics.
-   **Zero Dependencies, Zero Headaches:** The system compiles to a single static binary. Deployment is simple because the software is simple.

We chose Zig because its philosophy aligns with ours: no hidden control flow, no hidden memory allocations, and a focus on explicitness.

## 2. Core Architecture

The system is designed as a series of coordinated, specialized components that follow clear ownership patterns and architectural boundaries.

### 2.1. The Pure Coordinator Pattern

Components are designed to have a single, well-defined responsibility. Higher-level components act as **pure coordinators**, orchestrating operations between stateful subsystems without owning state themselves.

-   The `StorageEngine` is the primary example. It owns no in-memory state but coordinates operations between the `MemtableManager` (in-memory state and WAL) and the `SSTableManager` (on-disk immutable state). [96]
-   This pattern was extended to the TCP server, where the `Server` handler was refactored into a pure coordinator that delegates all connection lifecycle management to a dedicated `ConnectionManager`. [55, 84]

### 2.2. Single-Threaded Core with Async I/O

To eliminate data races by design, KausalDB's core is single-threaded. [75] Asynchronous I/O is handled by a non-blocking event loop. This constraint forces simplicity and makes state transitions easy to reason about. Concurrency is enforced in debug builds via `assert_main_thread()`.

### 2.3. Arena-per-Subsystem Memory Model

Memory safety and performance are achieved through a strict arena allocation pattern.

-   **Stateful Subsystems:** Components with a clear lifecycle, like the `BlockIndex` (memtable) or `ConnectionManager`, are backed by an `ArenaAllocator`.
-   **O(1) Cleanup:** When a memtable is flushed or a connection is closed, all associated memory is freed in a single, constant-time operation by resetting the arena. This is critical for high-throughput ingestion and connection handling.
-   **Zero Leaks:** This pattern eliminates complex manual memory management and is a key reason KausalDB has zero memory leaks across its entire test suite. [55] Our test suites validate this model under fault-injection scenarios. [51]

### 2.4. LSM-Tree Storage Engine

The storage engine is a custom Log-Structured Merge-Tree optimized for high write throughput.

-   **Write-Ahead Log (WAL):** All writes are first appended to the WAL in 64MB segmented files to ensure durability. A `CorruptionTracker` detects systematic corruption and triggers a fail-fast response to prevent propagating bad data. [89, 91]
-   **Memtable (`BlockIndex`):** An in-memory `HashMap` that stores recent writes for fast access, backed by an arena allocator. [94]
-   **SSTables (Sorted String Tables):** When the memtable reaches its size threshold, its contents are sorted and flushed to immutable on-disk files. The SSTable on-disk format uses a 64-byte aligned header and Bloom filters to optimize read performance. [102, 103]
-   **Tiered Compaction:** A background process merges SSTables using a size-tiered strategy to reduce read amplification and reclaim space from deleted or updated blocks. [99]

## 3. Data Model

The data model is purpose-built for representing code.

-   **`ContextBlock`**: The atomic unit of knowledge. It is a structured piece of information (e.g., a function, a class, a documentation paragraph) with a unique 128-bit `BlockId`, version, source URI, and JSON metadata. [79]
-   **`GraphEdge`**: A typed, directed relationship between two `ContextBlock`s, such as `calls`, `imports`, or `references`. The `GraphEdgeIndex` maintains bidirectional indexes for fast traversal in both directions. [93]

This structure allows the system to capture the rich, causal relationships in a codebase, transforming it from a collection of files into a queryable knowledge graph.
