# KausalDB Performance Baseline

## Overview

This document defines performance baselines and testing thresholds for KausalDB. Performance tests use tiered thresholds that adapt to different execution environments, from isolated benchmarking to resource-constrained CI runners.

## Current Baseline Measurements

### Target Performance
- **Block Write Latency**: 50µs (50,000ns)
- **Block Read Latency**: 10µs (10,000ns)
- **Single Query Latency**: 10µs (10,000ns)

### Measured Performance (M1 MacBook Pro, 2023)

| Scenario | Latency Range | Mean | Multiplier vs Target |
|----------|---------------|------|---------------------|
| Isolated | 70-77µs | 73µs | 1.46x |
| Under CPU Load | 72-77µs | 75µs | 1.50x |
| Full Test Suite | 240-285µs | ~260µs | 5.2x |

**Key Findings:**
- Single test execution: 1.5x target (reasonable)
- Full test suite: 5x target (resource contention)
- CPU load has minimal impact vs I/O contention

## Performance Tier Configuration

The performance framework uses environment-aware tiers with different tolerance levels:

```zig
const multipliers: Multipliers = switch (tier) {
    .local => .{ .latency = 5.0, .throughput = 0.8, .memory = 2.0 },      // 250µs limit
    .parallel => .{ .latency = 4.0, .throughput = 0.5, .memory = 3.0 },   // 200µs limit
    .ci => .{ .latency = 8.0, .throughput = 0.3, .memory = 4.0 },         // 400µs limit
    .production => .{ .latency = 1.0, .throughput = 1.0, .memory = 1.0 }, // 50µs limit
};
```

### Tier Detection Logic

1. **PRODUCTION**: `KAUSALDB_BENCHMARK_MODE=production`
2. **CI**: `CI=true`, `GITHUB_ACTIONS`, or `GITLAB_CI` environment variables
3. **PARALLEL**: `KAUSALDB_PARALLEL_TESTS=true` (set by local CI script)
4. **LOCAL**: Default for development

### Threshold Rationale

#### LOCAL (5x - 250µs limit)
- **Reasoning**: M1 MacBook Pro measured up to 240µs under test suite load
- **Use Case**: Development workflow, full test suite execution
- **Margin**: 5x provides headroom for system variance

#### PARALLEL (4x - 200µs limit)
- **Reasoning**: Resource contention from parallel test execution
- **Use Case**: `./scripts/local_ci.sh --parallel`
- **Margin**: Stricter than LOCAL since parallel tests should have less I/O contention per test

#### CI (8x - 400µs limit)
- **Reasoning**: GitHub runners are ~4x slower than M1 + additional CI overhead
- **Use Case**: GitHub Actions, GitLab CI
- **Margin**: Conservative estimate for variable CI performance

#### PRODUCTION (1x - 50µs limit)
- **Reasoning**: Isolated benchmarking with dedicated resources
- **Use Case**: Performance regression detection, release benchmarks
- **Margin**: Exact target for detecting real performance changes

## Expected GitHub Runner Performance

Based on M1 MacBook Pro measurements, GitHub runners should see:

- **Expected Range**: 200-800µs (4-16x target)
- **CI Threshold**: 400µs (8x target)
- **Likely Outcome**: Should pass with current thresholds

If GitHub runners exceed 400µs consistently, increase CI multiplier to 12-15x.

## Measurement Methodology

### Local Measurement
```bash
# Individual test (best case)
./zig/zig build performance_streaming_memory_benchmark -Doptimize=ReleaseSafe

# Full test suite (realistic case)
./scripts/local_ci.sh --skip-setup --jobs test

# Parallel execution
./scripts/local_ci.sh --skip-setup --parallel --jobs test
```

### Forced Failure Analysis
To see actual measurements, temporarily reduce thresholds in `src/test/performance_assertions.zig`:

```zig
.local => .{ .latency = 0.5, .throughput = 0.8, .memory = 2.0 }, // Force failure
```

Look for output like:
```
Performance assertion failed in LOCAL mode
Expected: ≤ 25000ns (base: 50000ns)
Actual: 240000ns
```

## Updating Baselines

### When to Update
- New hardware baselines (M2, M3, etc.)
- Significant performance improvements/regressions
- CI runner performance changes
- Major architectural changes

### Process
1. Run measurement script: `./scripts/measure_performance.sh` (if available)
2. Gather data from failed tests with reduced thresholds
3. Update `PerformanceThresholds.for_tier()` multipliers
4. Test both local and CI execution
5. Document changes in git commit

### GitHub Runner Calibration

Once GitHub Actions runs complete, update this document with:
- Actual measured latencies from CI failures
- Recommended CI threshold adjustments
- Comparison with local measurements

## Performance Test Architecture

### Test Structure
- **Target**: `tests/performance/streaming_memory_benchmark.zig`
- **Framework**: `src/test/performance_assertions.zig`
- **CI Integration**: Set via `KAUSALDB_PARALLEL_TESTS` environment variable

### Resource Sensitivity
Performance tests are highly sensitive to:
- Concurrent I/O (other tests, system processes)
- Memory pressure (parallel test execution)
- CPU scheduling (background tasks)
- Storage performance (SSD vs HDD, file system)

**Recommendation**: Run performance tests in isolation for accurate measurements, use relaxed thresholds for CI validation.

## Troubleshooting

### Test Passes Individually but Fails in CI
- **Cause**: Resource contention from parallel test execution
- **Solution**: Verify `KAUSALDB_PARALLEL_TESTS` is set correctly
- **Check**: Environment variable propagation to child processes

### Consistent CI Failures Above Threshold
- **Cause**: CI runners slower than expected
- **Solution**: Increase CI multiplier in performance_assertions.zig
- **Process**: Gather data from failures, update thresholds, document

### Sudden Performance Regression
- **Cause**: Code change affecting hot path performance
- **Investigation**: Compare individual test vs full suite performance
- **Action**: Identify regression cause or update baseline if intentional

## Historical Performance Data

| Date | Hardware | LOCAL Measured | CI Threshold | Notes |
|------|----------|----------------|--------------|-------|
| 2024-12 | M1 MacBook Pro | 240µs (5x) | 400µs (8x) | Initial baseline |

*Add GitHub runner data once available*
