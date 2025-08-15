# Microsecond Discipline

Performance isn't something we add later. In KausalDB, speed is designed in from the foundation—every architectural decision optimizes for the microsecond scale that AI workloads demand.

## Current Performance Reality

### M1 MacBook Pro (2023) - Development Reference

Core storage engine performance on Apple Silicon:

| Operation      | Target   | Actual    | Status      |
| :------------- | :------- | :-------- | :---------- |
| Block Write    | < 50 µs  | ~14.4 µs  | 3.5x faster |
| Block Read     | < 10 µs  | ~0.023 µs | 435x faster |
| Block Update   | < 50 µs  | ~14.6 µs  | 3.4x faster |
| Block Delete   | < 15 µs  | ~10.6 µs  | 1.4x faster |
| WAL Flush      | < 10 µs  | ~0.024 µs | 417x faster |
| Zero-Copy Read | < 1 µs   | ~0.020 µs | 50x faster  |

### Intel i7-4770K (2013) - Production Server

Same codebase on older hardware shows the architecture's adaptability:

| Operation      | Target   | Actual     | Status      |
| :------------- | :------- | :--------- | :---------- |
| Block Write    | < 50 µs  | ~50.5 µs   | Meets target|
| Block Read     | < 10 µs  | ~0.04 µs   | 250x faster |
| Block Update   | < 50 µs  | ~57.8 µs   | 13% over*   |
| Block Delete   | < 15 µs  | ~39.3 µs   | 2.6x over*  |
| WAL Flush      | < 10 µs  | ~0.01 µs   | 1000x faster|
| Zero-Copy Read | < 1 µs   | ~0.03 µs   | 33x faster  |

*_Performance degradation on 11-year-old hardware is expected and still competitive_

### Hardware Context

**M1 MacBook Pro**: 8-core ARM, unified memory, 5nm process, 2023  
**Intel i7-4770K**: 4-core x86, DDR3-1600, 22nm process, 2013

The 10-year hardware gap shows in write-heavy operations, but read performance stays excellent. Even on decade-old hardware, KausalDB meets or exceeds most targets.

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

## Why It's This Fast

Four architectural decisions that make microsecond performance possible.

### Single-Threaded Core

No locks, no races, no synchronization overhead. The core runs on one thread, eliminating an entire class of performance killers. Concurrency happens in the async I/O layer, not the data structures. Enforced in debug builds with `concurrency.assert_main_thread()`.

### Arena-Based Memory

When a memtable flushes, all its memory disappears in one O(1) operation. No per-object malloc/free dance. Arena allocators eliminate fragmentation and make cleanup trivial. State-heavy systems (`BlockIndex`, `QueryResult`) get their own arena, bulk allocate what they need, then bulk free it all.

### LSM-Tree Architecture

Optimized for the write-heavy workloads that code analysis creates:

- Writes go straight to the append-only WAL - cache-friendly, no seeks
- Recent data lives in a hash map for O(1) lookups
- Background compaction runs without blocking new writes
- No stop-the-world pauses that kill latency

### VFS Testing at Memory Speed  

All I/O goes through our VFS abstraction. Production uses real disks. Tests use in-memory simulation. Same code, different backend. Test against disk corruption and I/O failures at memory speed, then optimize with confidence.

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
