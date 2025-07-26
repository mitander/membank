# CortexDB Design Document

## 1. The CortexDB Philosophy: Principled Engineering

CortexDB is a specialized database with a specialized purpose: to serve as a fast, reliable, and deterministic context source for Large Language Models. Its design is not a collection of features, but a reflection of a core engineering philosophy inspired by mission-critical systems like TigerBeetle.

These principles are not suggestions; they are the laws by which this project is governed.

*   **Correctness is Not Negotiable:** The system must be verifiably correct. We favor simplicity and deterministic testing over features. A simple, correct system is the only foundation upon which performance can be built.

*   **Simplicity is the Prerequisite for Reliability:** We aggressively fight complexity. A system that is easy to reason about is a system that can be made robust. This means avoiding complex abstractions, hidden control flow, and intricate state management.

*   **Explicit is Better Than Implicit:** All control flow and memory allocations must be explicit and obvious. If a function allocates memory, it must take an `Allocator`. There is no global state, no hidden magic. The code does what it says on the tin.

*   **Performance is Designed, Not Tweaked:** We achieve performance through architecture, not through late-stage optimization hacks. A data-oriented, single-threaded design that avoids dynamic allocations and lock contention on the hot path is our primary performance strategy.

## 2. High-Level Architecture

CortexDB is a modular, single-threaded, asynchronous engine. Its architecture is designed to enforce a clean separation of concerns, enabling our core principle of deterministic testing.

```
cortex-core (The Database Engine)
│
├── Storage Engine (LSMT Persistence Layer - Coordinator)
│   ├── MemtableManager (Complete In-Memory State)
│   │   ├── BlockIndex (Context Block Storage)
│   │   └── GraphEdgeIndex (Relationship Storage)
│   ├── SSTableManager (Complete On-Disk State)
│   │   ├── SSTable Discovery & Creation
│   │   ├── Read Coordination
│   │   └── CompactionManager Integration
│   └── Write-Ahead Log (WAL) for Durability
│
├── Query Engine (Context Retrieval)
│   ├── ID-Based Lookups
│   └── Graph Traversal (BFS/DFS)
│
├── Replication Layer (High Availability - Future)
│   └── WAL-Based Primary-Backup Replication
│
└── VFS (Virtual File System Abstraction)
    ├── ProductionVFS (Real OS Filesystem)
    └── SimulationVFS (In-Memory, Deterministic Test Filesystem)
```

## 3. The Core Design Pillars

These are the fundamental architectural decisions that define CortexDB.

### 3.1. Memory Management: The Arena-per-Subsystem Pattern

Our experience has shown that complex, mixed-allocator models are a source of profound instability. Therefore, CortexDB's memory safety is built on a single, simple, and non-negotiable pattern: **the arena-per-subsystem.**

-   **Clear Ownership:** State-heavy subsystems like the `BlockIndex` (our in-memory memtable) are given their own `std.heap.ArenaAllocator`. This arena owns all dynamic memory associated with the subsystem's state (e.g., the strings within all `ContextBlock`s).

-   **Bulk Deallocation:** We do not manually free individual objects. When the subsystem's state is no longer needed (e.g., when the memtable is flushed to an SSTable), the entire arena is reset with a single `arena.reset(.retain_capacity)`. This is an O(1) operation that eliminates an entire class of memory bugs (double-frees, use-after-frees, memory leaks) by design.

-   **Consistent Allocators:** A component and its children must use a single, consistent backing allocator. This prevents cross-allocator corruption, which was the root cause of our previous `HashMap` alignment issues.

-   **No General-Purpose Allocations on Hot Paths:** The `StorageEngine` uses this arena model for its memtable. The performance-critical WAL writing and query paths will use temporary, function-scoped arenas to avoid the overhead of a general-purpose allocator. The complex `BufferPool` has been removed in favor of this simpler, safer, and often faster model.

### 3.2. Concurrency Model: Single-Threaded by Design

CortexDB's core logic is **strictly single-threaded**. This is an architectural choice to eliminate complexity.

-   **No Locks, No Data Races:** By restricting all core state modification to a single thread, we eliminate the need for mutexes, atomics, and other complex synchronization primitives. This makes the system dramatically easier to reason about and debug.
-   **Concurrency via Async I/O:** Concurrency is achieved at the I/O boundary. The single thread drives an event loop, handing off I/O operations to the OS kernel and processing results as they complete.
-   **Enforcement:** The `concurrency.assert_main_thread()` function is used liberally in debug builds to guarantee this invariant is never violated.

### 3.3. Storage Engine: A Decomposed LSM-Tree

We use a Log-Structured Merge-Tree for its exceptional write performance, which is critical for the high-volume ingestion of context data. The storage engine follows a **state-oriented decomposition** with clear ownership boundaries.

**Storage Engine Coordinator:**
The main `StorageEngine` acts as a pure coordinator, orchestrating LSM-tree operations across specialized subsystems without implementing storage logic directly.

**Subsystem Architecture:**

1.  **MemtableManager (In-Memory State):** Encapsulates the complete in-memory write buffer including both `BlockIndex` (context blocks) and `GraphEdgeIndex` (relationships). Uses arena-per-subsystem memory management for O(1) bulk cleanup during flushes.

2.  **SSTableManager (On-Disk State):** Manages the complete collection of SSTable files including discovery, creation, read coordination, and compaction management. Handles all persistent storage concerns independently.

3.  **Write-Ahead Log (WAL):** All writes (`put_block`, `put_edge`, `delete_block`) are first appended to the WAL for durability. A write is considered successful once it is flushed to the WAL file.

**LSM-Tree Operation Flow:**
1. WAL-first writes ensure durability before in-memory updates
2. MemtableManager handles all in-memory state changes
3. When memtable reaches size threshold, SSTableManager creates new immutable SSTables  
4. MemtableManager performs O(1) arena cleanup
5. SSTableManager coordinates background compaction as needed

This decomposition provides clear ownership boundaries, enhanced testability, and simplified reasoning about system behavior while maintaining all LSM-tree performance characteristics.

### 3.4. Testing Philosophy: Deterministic Simulation

**We do not mock.** Mocking is fragile and often fails to capture the emergent behavior of a complex system. Our primary testing strategy is **holistic, deterministic simulation.**

-   **The VFS Abstraction:** The `VFS` (Virtual File System) is the key. In production, it maps to the real OS filesystem. In tests, it maps to `SimulationVFS`—an in-memory, deterministic filesystem.
-   **Reproducible Realities:** The simulation harness controls time, I/O, and networking. We can write byte-for-byte reproducible tests for complex failure scenarios:
    -   A network partition occurs during a heavy write workload.
    -   The disk returns a corrupted byte in the middle of a WAL read.
    -   The system crashes after compacting an SSTable but before deleting the old files.
-   This approach allows us to find and fix bugs that would be nearly impossible to reproduce in a traditional testing environment.

## 4. Future Systems: A Pragmatic Vision

### 4.1. Replication: High Availability, Not Strict Consistency

While inspired by TigerBeetle, we recognize that LLM context data has different consistency requirements than financial ledgers. We can therefore choose a simpler, more robust replication strategy.

-   **Design Decision: Primary-Backup over Paxos/VSR.** We will implement a simple **WAL-shipping replication** model. A single primary node streams its WAL to one or more backup replicas.
-   **Rationale:** This model provides excellent read scalability and high availability for failover. It is dramatically simpler to implement and test than a full consensus protocol like Paxos. The eventual consistency it provides is a perfectly acceptable trade-off for this problem domain.

### 4.2. Ingestion Pipeline: A Pluggable Framework

The goal is to continuously and automatically keep the database's context up-to-date.

-   **Connectors:** Pluggable modules that know how to fetch data from a source (e.g., cloning a Git repository, scraping a website).
-   **Parsers:** Source-specific modules that understand a file format (e.g., a Zig parser that can extract functions, comments, and relationships).
-   **Chunkers:** Intelligent splitters that break down large documents into meaningful, semantically-related `ContextBlock`s.
