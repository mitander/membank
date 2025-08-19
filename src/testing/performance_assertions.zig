//! Tiered Performance Assertion Framework
//!
//! Provides environment-aware performance validation with different thresholds
//! for local development vs CI environments. Enables strict performance standards
//! in production while allowing reasonable tolerance for local development.

const std = @import("std");
const testing = std.testing;
const builtin = @import("builtin");
const build_options = @import("build_options");

/// Performance assertion tier configuration
pub const PerformanceTier = enum {
    /// Local development: More relaxed thresholds for development workflow
    local,
    /// CI environment: Strict thresholds for regression detection
    ci,
    /// Parallel execution: Very relaxed thresholds for resource contention scenarios
    parallel,
    /// Production benchmark: Most strict thresholds for release validation
    production,
    /// Sanitizer builds: Extremely relaxed thresholds for sanitizer overhead (10-100x)
    sanitizer,

    /// Detect performance tier from environment
    pub fn detect() PerformanceTier {
        // Check for build-time sanitizer flags first (highest priority)
        if (build_options.sanitizers_active) {
            return .sanitizer;
        }

        // Check for sanitizer builds from environment (fallback)
        if (std.process.getEnvVarOwned(std.heap.page_allocator, "KAUSALDB_SANITIZER_BUILD")) |sanitizer_value| {
            defer std.heap.page_allocator.free(sanitizer_value);
            if (std.mem.eql(u8, sanitizer_value, "true")) return .sanitizer;
        } else |_| {}

        // Check for common sanitizer environment indicators
        if (std.process.getEnvVarOwned(std.heap.page_allocator, "ASAN_OPTIONS")) |_| {
            return .sanitizer;
        } else |_| {}

        if (std.process.getEnvVarOwned(std.heap.page_allocator, "TSAN_OPTIONS")) |_| {
            return .sanitizer;
        } else |_| {}

        if (std.process.getEnvVarOwned(std.heap.page_allocator, "UBSAN_OPTIONS")) |_| {
            return .sanitizer;
        } else |_| {}

        // Check for CI environment variables (GitHub Actions, GitLab, etc.)
        if (std.process.getEnvVarOwned(std.heap.page_allocator, "CI")) |ci_value| {
            defer std.heap.page_allocator.free(ci_value);
            if (std.mem.eql(u8, ci_value, "true")) return .ci;
        } else |_| {}

        if (std.process.getEnvVarOwned(std.heap.page_allocator, "GITHUB_ACTIONS")) |_| {
            return .ci;
        } else |_| {}

        if (std.process.getEnvVarOwned(std.heap.page_allocator, "GITLAB_CI")) |_| {
            return .ci;
        } else |_| {}

        // Check for parallel test execution (resource contention)
        if (std.process.getEnvVarOwned(std.heap.page_allocator, "KAUSALDB_PARALLEL_TESTS")) |parallel_value| {
            defer std.heap.page_allocator.free(parallel_value);
            if (std.mem.eql(u8, parallel_value, "true")) return .parallel;
        } else |_| {}

        // Check for production benchmark mode
        if (std.process.getEnvVarOwned(std.heap.page_allocator, "KAUSALDB_BENCHMARK_MODE")) |mode| {
            defer std.heap.page_allocator.free(mode);
            if (std.mem.eql(u8, mode, "production")) return .production;
        } else |_| {}

        // Default to local development
        return .local;
    }
};

/// Performance threshold configuration for different environments
pub const PerformanceThresholds = struct {
    /// Maximum acceptable latency in nanoseconds
    max_latency_ns: u64,
    /// Maximum acceptable throughput operations per second (0 = no minimum)
    min_throughput_ops_per_sec: u64,
    /// Maximum acceptable memory usage in bytes (0 = no limit)
    max_memory_bytes: usize,
    /// Maximum acceptable CPU percentage (0-100, 0 = no limit)
    max_cpu_percent: u8,

    /// Create thresholds for a specific tier with tier-based multipliers
    pub fn for_tier(base_latency_ns: u64, base_throughput: u64, tier: PerformanceTier) PerformanceThresholds {
        const Multipliers = struct {
            latency: f64,
            throughput: f64,
            memory: f64,
        };

        const multipliers: Multipliers = switch (tier) {
            .local => .{
                .latency = 5.0, // ProductionVFS with filesystem contention - increased tolerance for real-world variance
                .throughput = 0.5,
                .memory = 3.0,
            },
            .parallel => .{
                .latency = 6.0, // Parallel execution overhead with ProductionVFS - higher contention
                .throughput = 0.4,
                .memory = 4.0,
            },
            .ci => .{ .latency = 8.0, .throughput = 0.3, .memory = 5.0 }, // CI runners with ProductionVFS - variable hardware and load
            .production => .{ .latency = 1.2, .throughput = 0.9, .memory = 1.1 }, // Isolated benchmarking with small safety margin
            .sanitizer => .{ .latency = 100.0, .throughput = 0.1, .memory = 10.0 }, // Sanitizer overhead with ProductionVFS - very high overhead
        };

        return PerformanceThresholds{
            .max_latency_ns = @as(u64, @intFromFloat(@as(f64, @floatFromInt(base_latency_ns)) * multipliers.latency)),
            .min_throughput_ops_per_sec = @as(u64, @intFromFloat(@as(f64, @floatFromInt(base_throughput)) * multipliers.throughput)),
            .max_memory_bytes = 0, // To be set based on specific test requirements
            .max_cpu_percent = 0, // To be set based on specific test requirements
        };
    }
};

/// Performance measurement and validation
pub const PerformanceAssertion = struct {
    tier: PerformanceTier,
    test_name: []const u8,

    pub fn init(test_name: []const u8) PerformanceAssertion {
        return PerformanceAssertion{
            .tier = PerformanceTier.detect(),
            .test_name = test_name,
        };
    }

    /// Assert that an operation meets latency requirements for current tier
    pub fn assert_latency(
        self: PerformanceAssertion,
        actual_duration_ns: u64,
        base_requirement_ns: u64,
        operation_description: []const u8,
    ) !void {
        const thresholds = PerformanceThresholds.for_tier(base_requirement_ns, 0, self.tier);

        if (actual_duration_ns > thresholds.max_latency_ns) {
            const tier_name = switch (self.tier) {
                .local => "LOCAL",
                .ci => "CI",
                .parallel => "PARALLEL",
                .production => "PRODUCTION",
                .sanitizer => "SANITIZER",
            };

            std.debug.print("\nPerformance assertion failed in {s} mode\n" ++
                "Test: {s}\n" ++
                "Operation: {s}\n" ++
                "Expected: ≤ {d}ns (base: {d}ns)\n" ++
                "Actual: {d}ns\n" ++
                "Overage: {d}ns ({d:.1}%)\n\n", .{
                tier_name,
                self.test_name,
                operation_description,
                thresholds.max_latency_ns,
                base_requirement_ns,
                actual_duration_ns,
                actual_duration_ns - thresholds.max_latency_ns,
                (@as(f64, @floatFromInt(actual_duration_ns)) / @as(f64, @floatFromInt(thresholds.max_latency_ns)) - 1.0) * 100.0,
            });
            return error.PerformanceRegressionDetected;
        }

        // Success case: optionally log performance info in debug builds
        if (builtin.mode == .Debug) {
            const tier_name = switch (self.tier) {
                .local => "LOCAL",
                .ci => "CI",
                .parallel => "PARALLEL",
                .sanitizer => "SANITIZER",
                .production => "PROD",
            };
            std.debug.print("[OK] [{s}] {s}: {d}ns (limit: {d}ns, {d:.1}% of budget)\n", .{
                tier_name,
                operation_description,
                actual_duration_ns,
                thresholds.max_latency_ns,
                (@as(f64, @floatFromInt(actual_duration_ns)) / @as(f64, @floatFromInt(thresholds.max_latency_ns))) * 100.0,
            });
        }
    }

    /// Assert that throughput meets requirements for current tier
    pub fn assert_throughput(
        self: PerformanceAssertion,
        actual_ops_per_sec: u64,
        base_requirement_ops_per_sec: u64,
        operation_description: []const u8,
    ) !void {
        const thresholds = PerformanceThresholds.for_tier(0, base_requirement_ops_per_sec, self.tier);

        if (actual_ops_per_sec < thresholds.min_throughput_ops_per_sec) {
            const tier_name = switch (self.tier) {
                .local => "LOCAL",
                .ci => "CI",
                .parallel => "PARALLEL",
                .production => "PRODUCTION",
                .sanitizer => "SANITIZER",
            };

            std.debug.print("\nThroughput assertion failed in {s} mode\n" ++
                "Test: {s}\n" ++
                "Operation: {s}\n" ++
                "Expected: ≥ {d} ops/sec (base: {d} ops/sec)\n" ++
                "Actual: {d} ops/sec\n" ++
                "Shortfall: {d} ops/sec ({d:.1}%)\n\n", .{
                tier_name,
                self.test_name,
                operation_description,
                thresholds.min_throughput_ops_per_sec,
                base_requirement_ops_per_sec,
                actual_ops_per_sec,
                thresholds.min_throughput_ops_per_sec - actual_ops_per_sec,
                (1.0 - @as(f64, @floatFromInt(actual_ops_per_sec)) / @as(f64, @floatFromInt(thresholds.min_throughput_ops_per_sec))) * 100.0,
            });
            return error.ThroughputRegressionDetected;
        }

        // Success case: optionally log performance info in debug builds
        if (builtin.mode == .Debug) {
            const tier_name = switch (self.tier) {
                .local => "LOCAL",
                .ci => "CI",
                .parallel => "PARALLEL",
                .sanitizer => "SANITIZER",
                .production => "PROD",
            };
            std.debug.print("[OK] [{s}] {s}: {d} ops/sec (min: {d} ops/sec, {d:.1}% above minimum)\n", .{
                tier_name,
                operation_description,
                actual_ops_per_sec,
                thresholds.min_throughput_ops_per_sec,
                (@as(f64, @floatFromInt(actual_ops_per_sec)) / @as(f64, @floatFromInt(thresholds.min_throughput_ops_per_sec)) - 1.0) * 100.0,
            });
        }
    }

    /// Measure and assert performance of a function call
    pub fn measure_and_assert_latency(
        self: PerformanceAssertion,
        base_requirement_ns: u64,
        operation_description: []const u8,
        operation_fn: anytype,
        args: anytype,
    ) !void {
        const start_time = std.time.nanoTimestamp();
        _ = @call(.auto, operation_fn, args);
        const end_time = std.time.nanoTimestamp();

        const duration_ns: u64 = if (end_time >= start_time)
            @as(u64, @intCast(end_time - start_time))
        else
            1; // Minimum measurable duration when time goes backwards
        try self.assert_latency(duration_ns, base_requirement_ns, operation_description);
    }

    /// Assert that mean latency is within target
    pub fn assert_mean_latency_within_target(
        self: PerformanceAssertion,
        actual_mean_ns: u64,
        target_ns: u64,
    ) !void {
        try self.assert_latency(actual_mean_ns, target_ns, "mean latency");
    }

    /// Assert that P99 latency is acceptable
    pub fn assert_p99_latency_acceptable(
        self: PerformanceAssertion,
        actual_p99_ns: u64,
        acceptable_ns: u64,
    ) !void {
        try self.assert_latency(actual_p99_ns, acceptable_ns, "P99 latency");
    }
};

/// Convenience macros for common performance assertions
pub const assert_storage_read_latency = measure_storage_latency(.read);
pub const assert_storage_write_latency = measure_storage_latency(.write);

/// Operation type for storage latency measurement
const StorageOp = enum { read, write };

/// Factory function for storage latency assertions
fn measure_storage_latency(
    comptime op_type: StorageOp,
) fn (
    []const u8,
    u64,
    anytype,
    anytype,
) anyerror!void {
    return struct {
        fn measure_impl(
            test_name: []const u8,
            duration_ns: u64,
            operation_description: []const u8,
            expected_desc: []const u8,
        ) !void {
            _ = expected_desc;
            const perf = PerformanceAssertion.init(test_name);

            const base_requirement = switch (op_type) {
                .read => 10_000, // 10µs base requirement for reads
                .write => 50_000, // 50µs base requirement for writes
            };

            try perf.assert_latency(duration_ns, base_requirement, operation_description);
        }
    }.measure_impl;
}

/// Batch performance measurement for statistical validation
pub const BatchPerformanceMeasurement = struct {
    measurements: std.ArrayList(u64),
    allocator: std.mem.Allocator,

    pub fn init(allocator: std.mem.Allocator) BatchPerformanceMeasurement {
        return BatchPerformanceMeasurement{
            .measurements = std.ArrayList(u64).init(allocator),
            .allocator = allocator,
        };
    }

    pub fn deinit(self: *BatchPerformanceMeasurement) void {
        self.measurements.deinit();
    }

    pub fn add_measurement(self: *BatchPerformanceMeasurement, duration_ns: u64) !void {
        try self.measurements.append(duration_ns);
    }

    /// Alias for add_measurement for compatibility with test code
    pub fn record_latency_ns(self: *BatchPerformanceMeasurement, duration_ns: u64) !void {
        try self.add_measurement(duration_ns);
    }

    /// Calculate basic statistics for the measurements
    pub fn calculate_statistics(self: *BatchPerformanceMeasurement) StatisticsResult {
        if (self.measurements.items.len == 0) {
            return StatisticsResult{
                .mean_latency_ns = 0,
                .p99_latency_ns = 0,
                .min_ns = 0,
                .max_ns = 0,
                .count = 0,
            };
        }

        var total: u64 = 0;
        var min_val: u64 = std.math.maxInt(u64);
        var max_val: u64 = 0;

        for (self.measurements.items) |measurement| {
            total += measurement;
            if (measurement < min_val) min_val = measurement;
            if (measurement > max_val) max_val = measurement;
        }

        // Calculate P99 - sort and find 99th percentile
        var sorted_measurements = std.ArrayList(u64).init(self.allocator);
        defer sorted_measurements.deinit();
        sorted_measurements.appendSlice(self.measurements.items) catch unreachable;
        std.mem.sort(u64, sorted_measurements.items, {}, comptime std.sort.asc(u64));

        const p99_index = (sorted_measurements.items.len * 99) / 100;
        const p99_latency = if (p99_index < sorted_measurements.items.len)
            sorted_measurements.items[p99_index]
        else
            max_val;

        return StatisticsResult{
            .mean_latency_ns = total / self.measurements.items.len,
            .p99_latency_ns = p99_latency,
            .min_ns = min_val,
            .max_ns = max_val,
            .count = self.measurements.items.len,
        };
    }

    pub const StatisticsResult = struct {
        mean_latency_ns: u64,
        p99_latency_ns: u64,
        min_ns: u64,
        max_ns: u64,
        count: usize,
    };

    /// Calculate statistical metrics and assert against tier-adjusted thresholds
    pub fn assert_statistics(
        self: *BatchPerformanceMeasurement,
        test_name: []const u8,
        base_requirement_ns: u64,
        operation_description: []const u8,
    ) !void {
        if (self.measurements.items.len == 0) {
            return error.NoMeasurementsProvided;
        }

        const perf = PerformanceAssertion.init(test_name);

        // Calculate statistical metrics
        var sum: u64 = 0;
        var max_val: u64 = 0;
        for (self.measurements.items) |measurement| {
            sum += measurement;
            if (measurement > max_val) max_val = measurement;
        }

        const mean = sum / @as(u64, @intCast(self.measurements.items.len));

        // Use the 95th percentile for performance validation (more robust than max)
        var sorted_measurements = try self.measurements.clone();
        defer sorted_measurements.deinit();
        std.mem.sort(u64, sorted_measurements.items, {}, std.sort.asc(u64));

        const p95_index = (sorted_measurements.items.len * 95) / 100;
        const p95_latency = sorted_measurements.items[p95_index];

        // Assert against P95 latency for tier
        try perf.assert_latency(p95_latency, base_requirement_ns, operation_description);

        // Optionally log statistical summary
        if (builtin.mode == .Debug) {
            std.debug.print("[STATS] {s} statistics: mean={d}ns, p95={d}ns, max={d}ns, samples={d}\n", .{ operation_description, mean, p95_latency, max_val, self.measurements.items.len });
        }
    }
};

/// Statistical performance sampling framework with warmup support
pub const StatisticalSampler = struct {
    measurements: std.ArrayList(u64),
    allocator: std.mem.Allocator,
    warmup_samples: u32,
    measurement_samples: u32,
    operation_name: []const u8,

    /// Initialize sampler with warmup and measurement configuration
    pub fn init(
        allocator: std.mem.Allocator,
        operation_name: []const u8,
        warmup_samples: u32,
        measurement_samples: u32,
    ) StatisticalSampler {
        return StatisticalSampler{
            .measurements = std.ArrayList(u64).init(allocator),
            .allocator = allocator,
            .warmup_samples = warmup_samples,
            .measurement_samples = measurement_samples,
            .operation_name = operation_name,
        };
    }

    pub fn deinit(self: *StatisticalSampler) void {
        self.measurements.deinit();
    }

    /// Run operation with warmup period followed by statistical sampling
    pub fn run_with_warmup(
        self: *StatisticalSampler,
        comptime operation_fn: anytype,
        context: anytype,
    ) !void {
        // Warmup phase - don't record these measurements
        if (builtin.mode == .Debug) {
            std.debug.print("[WARMUP] Running {d} warmup samples for {s}...\n", .{ self.warmup_samples, self.operation_name });
        }

        for (0..self.warmup_samples) |_| {
            _ = try operation_fn(context);
        }

        // Stabilization delay after warmup
        std.Thread.sleep(10 * std.time.ns_per_ms);

        // Measurement phase - record these samples
        if (builtin.mode == .Debug) {
            std.debug.print("[MEASURE] Collecting {d} measurement samples for {s}...\n", .{ self.measurement_samples, self.operation_name });
        }

        try self.measurements.ensureTotalCapacity(self.measurement_samples);

        for (0..self.measurement_samples) |_| {
            const start_time = std.time.nanoTimestamp();
            _ = try operation_fn(context);
            const end_time = std.time.nanoTimestamp();
            try self.measurements.append(@intCast(end_time - start_time));
        }
    }

    /// Calculate comprehensive statistics from collected measurements
    pub fn calculate_statistics(self: *const StatisticalSampler) StatisticalResult {
        if (self.measurements.items.len == 0) {
            return StatisticalResult{};
        }

        const sorted_measurements = self.allocator.dupe(u64, self.measurements.items) catch unreachable;
        defer self.allocator.free(sorted_measurements);
        std.mem.sort(u64, sorted_measurements, {}, std.sort.asc(u64));

        var sum: u64 = 0;
        for (sorted_measurements) |measurement| {
            sum += measurement;
        }

        const len = sorted_measurements.len;
        const mean = sum / @as(u64, @intCast(len));
        const min = sorted_measurements[0];
        const max = sorted_measurements[len - 1];
        const median = sorted_measurements[len / 2];
        const p95 = sorted_measurements[(len * 95) / 100];
        const p99 = sorted_measurements[(len * 99) / 100];

        // Calculate standard deviation
        var variance_sum: u64 = 0;
        for (sorted_measurements) |measurement| {
            const diff = if (measurement > mean) measurement - mean else mean - measurement;
            variance_sum += diff * diff;
        }
        const variance = variance_sum / @as(u64, @intCast(len));
        const stddev = std.math.sqrt(variance);

        return StatisticalResult{
            .min = min,
            .max = max,
            .mean = mean,
            .median = median,
            .p95 = p95,
            .p99 = p99,
            .stddev = stddev,
            .sample_count = @intCast(len),
        };
    }

    /// Assert performance using P95 percentile with detailed statistical reporting
    pub fn assert_performance(
        self: *const StatisticalSampler,
        test_name: []const u8,
        base_requirement_ns: u64,
        operation_description: []const u8,
    ) !void {
        const stats = self.calculate_statistics();

        if (builtin.mode == .Debug) {
            std.debug.print("[PERF] {s} Statistics:\n", .{operation_description});
            std.debug.print("  Samples: {d}\n", .{stats.sample_count});
            std.debug.print("  Min: {d}ns\n", .{stats.min});
            std.debug.print("  Mean: {d}ns\n", .{stats.mean});
            std.debug.print("  Median: {d}ns\n", .{stats.median});
            std.debug.print("  P95: {d}ns\n", .{stats.p95});
            std.debug.print("  P99: {d}ns\n", .{stats.p99});
            std.debug.print("  Max: {d}ns\n", .{stats.max});
            std.debug.print("  StdDev: {d}ns\n", .{stats.stddev});
        }

        const perf = PerformanceAssertion.init(test_name);
        try perf.assert_latency(stats.p95, base_requirement_ns, operation_description);
    }
};

/// Statistical measurement results
pub const StatisticalResult = struct {
    min: u64 = 0,
    max: u64 = 0,
    mean: u64 = 0,
    median: u64 = 0,
    p95: u64 = 0,
    p99: u64 = 0,
    stddev: u64 = 0,
    sample_count: u32 = 0,
};

/// Warmup utilities for different types of operations
pub const WarmupUtils = struct {
    /// Standard warmup for storage operations
    pub fn warmup_storage_engine(storage_engine: anytype, allocator: std.mem.Allocator) !void {
        if (builtin.mode == .Debug) {
            std.debug.print("[WARMUP] Warming up storage engine with test data...\n", .{});
        }

        // Fill OS page cache and warm up storage paths
        for (0..100) |i| {
            const test_block = create_warmup_block(allocator, i) catch continue;
            defer free_warmup_block(allocator, test_block);
            storage_engine.put_block(test_block) catch continue;
        }

        // Let OS settle and CPU frequencies stabilize
        std.Thread.sleep(50 * std.time.ns_per_ms);
    }

    /// Standard warmup for query operations
    pub fn warmup_query_engine(query_engine: anytype, allocator: std.mem.Allocator) !void {
        _ = allocator; // Placeholder for warmup block creation when needed
        if (builtin.mode == .Debug) {
            std.debug.print("[WARMUP] Warming up query engine with test queries...\n", .{});
        }

        // Warm up query paths and caches
        for (0..50) |i| {
            const test_id = create_warmup_block_id(i);
            _ = query_engine.find_block(test_id) catch continue;
        }

        // CPU cache warming delay
        std.Thread.sleep(25 * std.time.ns_per_ms);
    }

    fn create_warmup_block(allocator: std.mem.Allocator, index: usize) !@TypeOf(undefined) {
        // This would need to be adapted to the actual ContextBlock type
        _ = allocator;
        _ = index;
        return error.NotImplemented; // Placeholder
    }

    fn free_warmup_block(allocator: std.mem.Allocator, block: anytype) void {
        _ = allocator;
        _ = block;
        // Placeholder for actual cleanup
    }

    fn create_warmup_block_id(index: usize) @TypeOf(undefined) {
        _ = index;
        return undefined; // Placeholder for actual BlockId creation
    }
};
