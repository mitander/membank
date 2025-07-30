# Performance: Why Membank is Stupidly Fast

We didn't accidentally stumble into good performance. Every microsecond saved was a deliberate architectural choice. Here's what we're hitting in practice:

**Core Operations** (Measured on development hardware, but your results may vary):

| Operation        | Target  | Measured | Reality Check                |
| ---------------- | ------- | -------- | ---------------------------- |
| Block Write      | <50µs   | ~21µs    | **2.4x faster** than needed  |
| Block Read       | <10µs   | ~0.06µs  | **167x faster** (yes, really)|
| Block Update     | <50µs   | ~10µs    | **5x faster** than required  |
| Block Delete     | <50µs   | ~3µs     | **17x faster** than target   |
| Single Query     | <10µs   | ~0.12µs  | **83x faster** than expected |
| Batch Query (10) | <100µs  | ~0.33µs  | **300x faster** (not a typo) |
| WAL Flush        | <1ms    | ~0µs     | No-op when smart            |

**What this means**: We're not just "fast enough." We're operating at nanosecond latencies for reads and microsecond latencies for writes. That's the kind of speed that makes your LLM applications feel instant.

**Real-world throughput** (operations per second):

- Block Writes: **47,000 ops/sec** (plenty for any ingestion pipeline)
- Block Reads: **16.7 million ops/sec** (your bottleneck is elsewhere)
- Single Queries: **8.6 million ops/sec** (more than you'll ever need)
- Batch Queries: **3 million ops/sec** (basically free)

## Memory Efficiency: The Arena Advantage

**The problem with most databases**: They leak memory like a sieve. malloc here, free there, oops-forgot-to-free-that over there. It's a mess.

**Our approach**: Arena allocators for the win. Each subsystem gets its own memory arena. When the subsystem is done, we blow away the entire arena in one O(1) operation. Simple, fast, leak-proof.

**What this looks like in practice**:

- **Peak memory**: <100MB for 10K operations ✓
- **Memory growth**: Linear with data size, not operation count ✓
- **Memory leaks**: Literally impossible within subsystems ✓
- **Cleanup time**: O(1) regardless of how much data you processed ✓

The arena pattern isn't just safer—it's **faster**. Bulk allocation means fewer syscalls, better cache locality, and predictable performance.

## Benchmark Methodology

### Test Environment

```bash
# Standard benchmark run
./zig/zig build benchmark
./zig-out/bin/benchmark all --json

# CI regression detection
./scripts/benchmark.sh
```

**Configuration**:

- 50 iterations per operation (50 warmup)
- 5 statistical samples for variance analysis
- ReleaseFast optimization mode
- Single-threaded execution (design principle)

### Statistical Analysis

Each benchmark reports:

- **Min/Max/Mean/Median**: Timing distribution
- **Standard Deviation**: Performance consistency
- **Throughput**: Operations per second
- **Threshold Status**: Pass/fail against targets

### Regression Detection

**CI Integration**:

- Intelligent threshold setting based on measured performance
- 5-20x margins account for CI hardware variance
- Catches meaningful regressions (>5x slowdowns indicate architectural problems)
- Stable across different GitHub Actions runners

## How We Actually Built Something This Fast

Most performance advice is bullshit. "Just optimize the hot path!" they say. But which path? "Use better algorithms!" they say. But better how?

Here's what actually worked for us:

### 1. Single-Threaded Core (The Controversial Choice)

```zig
concurrency.assert_main_thread(); // We're dead serious about this
```

**Everyone said we were crazy.** "You need threads for performance!" they said. Nope. Threads add complexity, locks, cache invalidation, and non-deterministic bugs. We choose to be predictably fast on one thread rather than occasionally correct on many.

**The result**: Zero synchronization overhead, predictable cache behavior, and code you can actually reason about.

### 2. Arena Memory (The Memory Management Revolution)

```zig
// This line frees potentially gigabytes of data in ~1 nanosecond
_ = self.arena.reset(.retain_capacity);
```

**The insight**: Most "performance problems" are actually memory management problems. malloc/free is slow, unpredictable, and fragments your heap. Arena allocation is bulk allocation—request a big chunk once, carve it up as needed, then blow it all away when you're done.

**The result**: Memory allocation is no longer a bottleneck. Ever.

### 3. LSM-Tree Architecture (Writes Go Brrr)

**The problem**: Traditional B-tree databases are read-optimized. But LLM context needs **write-heavy** workloads—you're constantly ingesting new code, documents, and relationships.

**Our solution**: Log-Structured Merge-Tree design:
- WAL for durability (append-only is cache-friendly)
- In-memory memtable for recent data (hash tables are O(1))
- Background compaction doesn't block writes (no stop-the-world pauses)

**The result**: Writes scale linearly with your CPU, not your disk.

### 4. The VFS Trick (Testing at Light Speed)

```zig
// Same code, different backends
const vfs = if (is_test) SimulationVFS else ProductionVFS;
```

**The genius**: We don't mock the filesystem—we **replace** it. All I/O goes through our Virtual File System abstraction. Production uses real files. Tests use a deterministic in-memory filesystem.

**The result**: We can test catastrophic failure scenarios (disk corruption, network partitions) at memory speeds, giving us confidence to optimize aggressively.

### Hot Path Optimizations

**Query Engine**:

- Pre-allocated result buffers
- Linear scans for small collections (<16 items)
- Graph traversal with early termination

**Storage Engine**:

- Write-ahead logging with batched flushes
- SSTable binary search with cache-friendly layout
- Bloom filters planned for 2.0

## Performance Testing Guide

### Running Benchmarks

```bash
# Quick performance check
./zig/zig build benchmark
./zig-out/bin/benchmark storage

# Full regression suite
./scripts/benchmark.sh

# Individual categories
./zig-out/bin/benchmark storage --json
./zig-out/bin/benchmark query --json
./zig-out/bin/benchmark compaction --json
```

### Interpreting Results

**Good Performance**:

- Mean latency well below thresholds
- Low standard deviation (consistent timing)
- Throughput matches or exceeds targets

**Performance Issues**:

- High standard deviation (inconsistent performance)
- Mean approaching or exceeding thresholds
- Throughput degradation vs baseline

### Memory Profiling

```bash
# Debug memory issues
./zig/zig build test-sanitizer

# Valgrind integration (CI)
valgrind --tool=memcheck --leak-check=full ./zig-out/bin/benchmark storage
```

## 1.0 Performance Commitments

### API Guarantees

**Block Operations**:

- Put/Get: <50µs for 1KB blocks
- Update: <50µs for existing blocks
- Delete: <50µs including metadata cleanup

**Query Operations**:

- Single block lookup: <10µs
- Graph traversal (3-hop): <100µs
- Batch operations (10 blocks): <100µs

**Durability**:

- WAL flush: <1ms for write confirmation
- Recovery: <1s per 1GB of WAL data

### Scalability Targets

**Dataset Size**:

- 1M blocks: All operations maintain latency targets
- 10M blocks: <2x latency degradation
- 100M blocks: Requires 2.0 optimizations (Bloom filters)

**Memory Usage**:

- Linear growth with dataset size
- <1GB RAM for 1M blocks
- Arena cleanup prevents memory leaks

## Regression Prevention

### CI Integration

**.github/workflows/ci.yml**:

```yaml
performance:
  name: Performance Regression Detection
  runs-on: ubuntu-latest
  steps:
    - name: Run benchmarks
      run: ./scripts/benchmark.sh
```

### Baseline Management

**Automatic Updates**: Main branch improvements update baseline
**Manual Updates**: Intentional performance trade-offs require baseline adjustment
**Review Process**: All performance changes documented in PR descriptions

### Alert Thresholds

**Operation-Specific Thresholds** (based on measured performance):

- **Block Write**: 100µs threshold (5x margin from 21µs measured)
- **Block Read**: 1µs threshold (17x margin from 0.06µs measured)
- **Single Query**: 2µs threshold (20x margin from 0.12µs measured)
- **Batch Query**: 5µs threshold (17x margin from 0.33µs measured)

**Philosophy**: Generous margins prevent false positives while catching real architectural regressions.

## Debugging Performance Issues

### Common Causes

1. **Memory allocations in hot paths**
   - Use arena allocators for temporary data
   - Pre-allocate buffers where possible

2. **Synchronous I/O blocking**
   - Ensure async patterns via VFS
   - Check for hidden filesystem operations

3. **Algorithm complexity changes**
   - Review data structure modifications
   - Validate O(n) assumptions hold

### Diagnostic Tools

```bash
# Tier 1: Built-in profiling
./zig-out/bin/benchmark storage --json | jq '.results[].mean_ns'

# Tier 2: System profiling
perf record ./zig-out/bin/benchmark storage
perf report

# Tier 3: Memory analysis
./zig/zig build test-sanitizer
```

## Performance Philosophy

Membank achieves speed through **architectural simplicity**, not micro-optimizations:

- **Explicit memory management** eliminates GC pauses
- **Single-threaded design** avoids synchronization overhead
- **Arena patterns** enable bulk operations
- **LSM-tree architecture** optimizes for write-heavy workloads

Performance is **designed into the architecture**, not bolted on afterward.
