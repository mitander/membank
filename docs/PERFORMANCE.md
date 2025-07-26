# Performance

**Core Operations** (Release mode, Ubuntu 22.04, x86_64):

| Operation        | Target | Current | Status       |
| ---------------- | ------ | ------- | ------------ |
| Block Write      | <50µs  | ~27µs   | 46% faster   |
| Block Read       | <10µs  | ~48ns   | 208x faster  |
| Block Update     | <50µs  | ~13µs   | 74% faster   |
| Block Delete     | <50µs  | ~2µs    | 96% faster   |
| Single Query     | <10µs  | ~71ns   | 140x faster  |
| Batch Query (10) | <100µs | ~244ns  | 409x faster  |
| WAL Flush        | <1ms   | ~200ns  | 5000x faster |

**Throughput** (operations per second):

- Block Writes: 36,544 ops/sec
- Block Reads: 20,833,333 ops/sec
- Single Queries: 14,084,507 ops/sec
- Batch Queries: 4,098,360 ops/sec

## Memory Efficiency

**Target Goals**:

- Peak memory: <100MB for 10K operations
- Memory growth: <1KB per operation average
- Zero leaks in sustained workloads

**Current Status**: Meets all memory efficiency targets via arena-per-subsystem pattern.

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

- 1,000 iterations per operation (100 warmup)
- 10 statistical samples for variance analysis
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

- Automatic baseline comparison on every PR
- 15% slowdown threshold triggers failure
- Performance artifacts uploaded for analysis
- Baseline stored in `.github/performance-baseline.json`

## Architecture Performance Design

### Why CortexDB is Fast

**Single-Threaded Core**: No lock contention, predictable cache behavior

```zig
concurrency.assert_main_thread(); // Enforced in debug builds
```

**Arena Memory Model**: Bulk allocation/deallocation eliminates malloc overhead

```zig
// O(1) cleanup of entire memtable
_ = self.arena.reset(.retain_capacity);
```

**LSM-Tree Optimization**: Write-optimized for LLM context ingestion

- WAL for durability
- In-memory memtable for recent data
- Background compaction doesn't block writes

**VFS Abstraction**: Zero-copy I/O patterns, minimal syscalls

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

- **15% slowdown**: CI failure, blocks merge
- **5-15% slowdown**: Warning, requires investigation
- **>15% improvement**: Consider baseline update

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

CortexDB achieves speed through **architectural simplicity**, not micro-optimizations:

- **Explicit memory management** eliminates GC pauses
- **Single-threaded design** avoids synchronization overhead
- **Arena patterns** enable bulk operations
- **LSM-tree architecture** optimizes for write-heavy workloads

Performance is **designed into the architecture**, not bolted on afterward.
