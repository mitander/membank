# Architectural Design

This document specifies the architectural principles and design patterns that govern the Kausal codebase. Adherence to these principles is non-negotiable, as they are the foundation of the system's reliability and performance.

## 1. Core Philosophy

The system is engineered according to a strict set of laws. These are not guidelines; they are constraints.

*   **Simplicity is the Prerequisite for Reliability:** A complex system cannot be proven reliable. We choose the simplest path that is correct.
*   **Correctness is Not Negotiable:** All operations must be deterministic and verifiable. "Probably right" is wrong.
*   **Explicitness Over Magic:** All control flow, memory lifecycles, and state transitions must be obvious from reading the code. There are no hidden mechanisms.

## 2. System Architecture

Kausal is built on a state-oriented, coordinator-based architecture inspired by high-performance financial databases.

### 2.1. The Coordinator Pattern

The `StorageEngine` (`src/storage/engine.zig`) is a **pure coordinator**. It contains no business logic. Its sole function is to orchestrate operations between the state-managing subsystems. This enforces a clean separation of concerns.

### 2.2. State-Oriented Subsystems

Ownership of state is strictly segregated to prevent ambiguity and ensure clear lifecycle management.

*   **MemtableManager (`src/storage/memtable_manager.zig`):** Owns the complete in-memory state of the database, including the `BlockIndex` and `GraphEdgeIndex`. It is also the sole owner of the Write-Ahead Log (WAL), as durability is a function of the in-memory write buffer.
*   **SSTableManager (`src/storage/sstable_manager.zig`):** Owns the complete on-disk state of immutable, sorted data files (SSTables). It is responsible for all SSTable discovery, read coordination, and compaction management.

### 2.3. The LSM-Tree

The storage layer is a custom Log-Structured Merge-Tree, optimized for the project's write-heavy ingestion patterns.

1.  **Write-Ahead Log (WAL):** All writes are first serialized to the WAL to guarantee durability.
2.  **Memtable (`BlockIndex`):** An in-memory index holds the most recent writes for low-latency queries.
3.  **SSTables:** When the memtable reaches its size threshold, its contents are flushed to immutable, sorted files on disk.
4.  **Compaction:** A background process merges SSTables to maintain read performance and reclaim space.

## 3. Memory Model

The memory model is designed to eliminate entire classes of bugs at the architectural level.

*   **Arena-per-Subsystem:** State-heavy components (`BlockIndex`, `GraphEdgeIndex`) use an `ArenaAllocator`. When a memtable is flushed, all of its associated memory is freed in a single O(1) `reset()` operation. This is critical for performance and eliminates the possibility of memory leaks from complex object graphs.
*   **Single-Threaded Core:** The database core is single-threaded. This is a deliberate design choice that eliminates data races by design, removing the need for locks, mutexes, or other complex concurrency primitives. The `assert_main_thread()` function in `src/core/concurrency.zig` enforces this constraint in debug builds.

## 4. Testing Philosophy

The system is built on a "simulation-first" testing doctrine.

*   **VFS Abstraction (`src/core/vfs.zig`):** All I/O operations are routed through a Virtual File System interface. This is a critical abstraction that allows the *identical production code* to run against either the real filesystem or a deterministic, in-memory simulated filesystem.
*   **Deterministic Simulation (`src/sim/simulation_vfs.zig`):** For testing, we use a `SimulationVFS` that provides byte-for-byte reproducible tests. The simulation harness can deterministically inject hostile conditions, including network partitions, I/O errors, disk corruption, and torn writes, allowing us to validate the system's correctness under catastrophic failure scenarios.
