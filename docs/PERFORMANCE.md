# Performance Specification

## 1. Overview

This document specifies the performance characteristics and architectural principles that enable the Kausal system's low-latency operation. Performance is a direct result of deliberate architectural choices, not post-hoc optimization.

## 2. Performance Metrics

### 2.1. Measured Latency

The following metrics were measured on development hardware (`AMD Ryzen 9 5950X, NVMe SSD`). CI thresholds are set with a 5-20x margin to account for hardware variance.

| Operation | Target Latency | Measured Latency |
| :--- | :--- | :--- |
| Block Write | < 50 µs | ~21 µs |
| Block Read | < 10 µs | ~0.06 µs |
| Block Update | < 50 µs | ~10 µs |
| Single Query | < 10 µs | ~0.12 µs |
| Batch Query (10) | < 100 µs | ~0.33 µs |

### 2.2. Measured Throughput

-   **Block Writes**: ~47,000 ops/sec
-   **Block Reads**: ~16.7 million ops/sec
-   **Single Queries**: ~8.6 million ops/sec
-   **Batch Queries**: ~3 million ops/sec

## 3. Architectural Foundations of Performance

The system's performance is derived from four foundational architectural decisions.

### 3.1. Single-Threaded Core

The database core is single-threaded by design. This constraint eliminates data races and the need for synchronization primitives (e.g., locks, mutexes), which removes a significant source of performance overhead and non-determinism. Concurrency is handled by an async I/O event loop. This is enforced at compile time in debug builds via `concurrency.assert_main_thread()`.

### 3.2. Arena-Based Memory Management

State-heavy subsystems (`BlockIndex`, `QueryResult`) utilize an `ArenaAllocator`. This pattern enables bulk allocation and deallocation. When a subsystem's state is no longer needed (e.g., a memtable flush), its entire memory is freed in a single, O(1) `reset()` operation. This avoids the overhead and fragmentation of per-object `malloc`/`free` calls and makes memory management a non-bottleneck.

### 3.3. LSM-Tree Storage Architecture

The storage engine is a Log-Structured Merge-Tree, an architecture optimized for write-heavy workloads.

*   All writes are append-only operations to a Write-Ahead Log (WAL), which is cache-friendly.
*   Recent data is held in an in-memory hash map (`BlockIndex`) for O(1) lookups.
*   The background compaction process merges on-disk SSTables without blocking new writes, preventing stop-the-world pauses.

### 3.4. VFS-Based Deterministic Testing

All filesystem I/O is routed through a Virtual File System (VFS) abstraction. In production, this maps to the OS filesystem. In testing, it maps to a deterministic, in-memory `SimulationVFS`. This allows the identical production codebase to be tested against simulated failure modes (disk corruption, I/O errors) at memory speed, enabling aggressive optimization with high confidence.

## 4. Benchmark Protocol

### 4.1. Execution

The primary tool for performance validation is the benchmark suite.

```bash
# Build the benchmark executable
./zig/zig build benchmark

# Run a specific benchmark category and output JSON
./zig-out/bin/benchmark storage --json
```

### 4.2. Regression Detection

The CI pipeline automates regression detection via `scripts/benchmark.sh`.

*   **Mechanism**: The script compares the JSON output of a benchmark run against a committed baseline (`.github/performance-baseline.json`).
*   **Thresholds**: A regression is flagged if the mean latency of an operation exceeds the baseline by **15%**. The baseline itself has a 5-20x margin over measured development performance to prevent CI flakiness. This is designed to catch significant architectural regressions, not minor fluctuations.

## 5. 1.0 Performance Targets

### 5.1. API Latency Guarantees

*   **Block Operations**: `< 50µs` for Puts, Updates, and Deletes of 1KB blocks.
*   **Query Operations**: `< 10µs` for single block lookups; `< 100µs` for 3-hop graph traversals and 10-block batch queries.
*   **Durability**: `< 1ms` for a WAL flush to confirm a write.
*   **Recovery**: `< 1s` per 1GB of WAL data.

### 5.2. Scalability Targets

*   **Dataset Size**: The system must maintain specified latency targets up to 1 million blocks.
*   **Memory Usage**: Memory growth must be linear with dataset size, with a target of `< 1GB` RAM for 1 million blocks.

## 6. Performance Diagnostics

### 6.1. Common Causes of Regressions

1.  **Heap Allocations in Hot Paths**: Introduction of `malloc`/`free` patterns where an arena should be used.
2.  **Synchronous I/O**: Introduction of blocking I/O calls that bypass the async VFS model.
3.  **Algorithmic Complexity**: Changes to data structures that degrade their O(1) or O(log n) performance characteristics.

### 6.2. Diagnostic Tooling

*   **Tier 1 (Built-in)**: The benchmark suite's JSON output provides mean latency and standard deviation.
    ```bash
    ./zig-out/bin/benchmark storage --json | jq
    ```
*   **Tier 2 (System Profiling)**: `perf` is used for deep analysis of CPU-bound issues.
    ```bash
    perf record ./zig-out/bin/benchmark storage
    perf report
    ```
*   **Tier 3 (Memory Analysis)**: The `-fsanitize-address` build flag and Valgrind are used for memory-related performance issues.
    ```bash
    ./zig/zig build test-sanitizer
    ```
