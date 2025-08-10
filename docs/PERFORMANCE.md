# Performance Specification

## 1. Overview

This document specifies the performance characteristics and architectural principles that enable the KausalDB system's low-latency operation. Performance is a direct result of deliberate architectural choices, not post-hoc optimization.

## 2. Performance Metrics

### 2.1. Current Performance Baselines

#### Direct Benchmark Performance (M1 MacBook Pro, 2023)

The following metrics represent core storage engine performance measured via direct benchmarking:

| Operation      | Target Latency | Measured Latency | Status         |
| :------------- | :------------- | :--------------- | :------------- |
| Block Write    | < 50 µs        | ~15 µs           | Exceeds target |
| Block Read     | < 10 µs        | ~0.08 µs         | Exceeds target |
| Block Update   | < 50 µs        | ~15 µs           | Exceeds target |
| Block Delete   | < 15 µs        | ~13 µs           | Meets target   |
| WAL Flush      | < 10 µs        | ~0.02 µs         | Exceeds target |
| Zero-Cost Read | < 1 µs         | ~0.02 µs         | Exceeds target |

#### Test Framework Performance

Performance tests using the test harness show higher latency due to measurement overhead:

| Test Type        | Measured Latency | Overhead Factor |
| :--------------- | :--------------- | :-------------- |
| Direct Benchmark | 15 µs            | 1x (baseline)   |
| Test Suite       | 240 µs           | 16x             |

**Key Finding**: Core storage engine performance exceeds specifications. Test framework overhead requires separate thresholds for CI validation.

### 2.2. Test Framework Threshold Configuration

The performance testing framework uses tiered thresholds that adapt to different execution environments:

```zig
const multipliers: Multipliers = switch (tier) {
    .local => .{ .latency = 5.0, .throughput = 0.8, .memory = 2.0 },      // 250µs limit
    .parallel => .{ .latency = 4.0, .throughput = 0.5, .memory = 3.0 },   // 200µs limit
    .ci => .{ .latency = 20.0, .throughput = 0.2, .memory = 5.0 },        // 1000µs limit
    .production => .{ .latency = 1.0, .throughput = 1.0, .memory = 1.0 }, // 50µs limit
};
```

#### Tier Detection Logic

1. **PRODUCTION**: `KAUSALDB_BENCHMARK_MODE=production`
2. **CI**: `CI=true`, `GITHUB_ACTIONS`, or `GITLAB_CI` environment variables
3. **PARALLEL**: `KAUSALDB_PARALLEL_TESTS=true` (set by local CI script)
4. **LOCAL**: Default for development

#### Threshold Rationale

- **LOCAL (5x)**: Accommodates test framework overhead (240µs measured vs 15µs core)
- **PARALLEL (4x)**: Resource contention from parallel test execution
- **CI (20x)**: GitHub runners 4x slower + test framework overhead
- **PRODUCTION (1x)**: Direct benchmark validation, exact target enforcement

### 2.3. Measured Throughput

Current throughput (M1 MacBook Pro):

- **Block Writes**: ~67,000 ops/sec
- **Block Reads**: ~12.6 million ops/sec
- **Block Updates**: ~64,000 ops/sec
- **Block Deletes**: ~77,000 ops/sec

## 3. Architectural Foundations of Performance

The system's performance is derived from four foundational architectural decisions.

### 3.1. Single-Threaded Core

The database core is single-threaded by design. This constraint eliminates data races and the need for synchronization primitives (e.g., locks, mutexes), which removes a significant source of performance overhead and non-determinism. Concurrency is handled by an async I/O event loop. This is enforced at compile time in debug builds via `concurrency.assert_main_thread()`.

### 3.2. Arena-Based Memory Management

State-heavy subsystems (`BlockIndex`, `QueryResult`) utilize an `ArenaAllocator`. This pattern enables bulk allocation and deallocation. When a subsystem's state is no longer needed (e.g., a memtable flush), its entire memory is freed in a single, O(1) `reset()` operation. This avoids the overhead and fragmentation of per-object `malloc`/`free` calls and makes memory management a non-bottleneck.

### 3.3. LSM-Tree Storage Architecture

The storage engine is a Log-Structured Merge-Tree, an architecture optimized for write-heavy workloads.

- All writes are append-only operations to a Write-Ahead Log (WAL), which is cache-friendly.
- Recent data is held in an in-memory hash map (`BlockIndex`) for O(1) lookups.
- The background compaction process merges on-disk SSTables without blocking new writes, preventing stop-the-world pauses.

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

- **Mechanism**: The script compares the JSON output of a benchmark run against a committed baseline (`.github/performance-baseline.json`).
- **Thresholds**: A regression is flagged if the mean latency of an operation exceeds the baseline by **15%**. The baseline itself has a 5-20x margin over measured development performance to prevent CI flakiness. This is designed to catch significant architectural regressions, not minor fluctuations.

## 5. 1.0 Performance Targets

### 5.1. API Latency Guarantees

- **Block Operations**: `< 50µs` for Puts, Updates, and Deletes of 1KB blocks.
- **Query Operations**: `< 10µs` for single block lookups; `< 100µs` for 3-hop graph traversals and 10-block batch queries.
- **Durability**: `< 1ms` for a WAL flush to confirm a write.
- **Recovery**: `< 1s` per 1GB of WAL data.

### 5.2. Scalability Targets

- **Dataset Size**: The system must maintain specified latency targets up to 1 million blocks.
- **Memory Usage**: Memory growth must be linear with dataset size, with a target of `< 1GB` RAM for 1 million blocks.

## 6. Performance Diagnostics

### 6.1. Common Causes of Regressions

1.  **Heap Allocations in Hot Paths**: Introduction of `malloc`/`free` patterns where an arena should be used.
2.  **Synchronous I/O**: Introduction of blocking I/O calls that bypass the async VFS model.
3.  **Algorithmic Complexity**: Changes to data structures that degrade their O(1) or O(log n) performance characteristics.

### 6.2. Diagnostic Tooling

- **Tier 1 (Built-in)**: The benchmark suite's JSON output provides mean latency and standard deviation.
  ```bash
  ./zig-out/bin/benchmark storage --json | jq
  ```
- **Tier 2 (System Profiling)**: `perf` is used for deep analysis of CPU-bound issues.
  ```bash
  perf record ./zig-out/bin/benchmark storage
  perf report
  ```
- **Tier 3 (Memory Analysis)**: The `-fsanitize-address` build flag and Valgrind are used for memory-related performance issues.
  ```bash
  ./zig/zig build test-sanitizer
  ```

## 7. Test Framework vs Direct Benchmarking

### 7.1. Performance Measurement Methods

**Direct Benchmarking** (`./zig-out/bin/benchmark storage`):

- Measures core storage engine performance
- Minimal overhead, direct API calls
- Used for regression detection and optimization
- Results: ~15µs block writes

**Test Framework** (`tests/performance/streaming_memory_benchmark.zig`):

- Measures end-to-end test execution including setup/teardown
- Statistical measurement framework overhead
- Used for CI validation with appropriate tolerances
- Results: ~240µs block writes (16x overhead)

### 7.2. When to Use Each Method

- **Performance Regression Analysis**: Use direct benchmarking
- **CI Threshold Validation**: Use test framework with tiered thresholds
- **Optimization Work**: Use direct benchmarking for precise measurements
- **Release Validation**: Use both methods to ensure comprehensive coverage

### 7.3. Troubleshooting Performance Issues

1. **Suspected Regression**: Run direct benchmark first
   - If benchmark shows >50µs: Real performance regression
   - If benchmark shows ~15µs: Test framework threshold adjustment needed

2. **CI Failures**: Check tier detection and threshold configuration
   - Verify environment variables are set correctly
   - Adjust CI multiplier if GitHub runners slower than expected

3. **Local Development**: Use LOCAL tier for normal development workflow
   - Allows for system variance while catching major regressions
