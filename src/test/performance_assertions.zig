//! Tiered Performance Assertion Framework
//!
//! Provides environment-aware performance validation with different thresholds
//! for local development vs CI environments. Enables strict performance standards
//! in production while allowing reasonable tolerance for local development.

const std = @import("std");
const testing = std.testing;
const builtin = @import("builtin");

/// Performance assertion tier configuration
pub const PerformanceTier = enum {
    /// Local development: More relaxed thresholds for development workflow
    local,
    /// CI environment: Strict thresholds for regression detection
    ci,
    /// Production benchmark: Most strict thresholds for release validation
    production,

    /// Detect performance tier from environment
    pub fn detect() PerformanceTier {
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
            .local => .{ .latency = 5.0, .throughput = 0.3, .memory = 3.0 }, // 5x latency, 30% throughput, 3x memory (generous for local dev)
            .ci => .{ .latency = 2.5, .throughput = 0.6, .memory = 2.0 }, // 2.5x latency, 60% throughput, 2x memory
            .production => .{ .latency = 1.0, .throughput = 1.0, .memory = 1.0 }, // Exact requirements
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
                .production => "PRODUCTION",
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
                .production => "PRODUCTION",
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

        const duration_ns = @as(u64, @intCast(end_time - start_time));
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
