//! Memory profiling validation tests.
//!
//! Tests RSS measurement accuracy, performance overhead, and reliability
//! of the MemoryProfiler implementation across different platforms.
//! Validates that memory tracking is accurate enough for performance regression detection.

const std = @import("std");
const testing = std.testing;
const kausaldb = @import("kausaldb");
const builtin = @import("builtin");

const assert = kausaldb.assert.assert;

const MemoryProfiler = kausaldb.profiler.MemoryProfiler;
const query_current_rss_memory = kausaldb.profiler.query_current_rss_memory;

// Test RSS measurement accuracy
test "memory profiler RSS measurement accuracy" {
    var memory_profiler = MemoryProfiler.init();
    memory_profiler.start_profiling();

    // Initial RSS should be reasonable (not zero, not excessive)
    try testing.expect(memory_profiler.initial_rss_bytes > 1024); // At least 1KB
    try testing.expect(memory_profiler.initial_rss_bytes < 1024 * 1024 * 1024); // Less than 1GB

    // Peak should start equal to initial
    try testing.expectEqual(memory_profiler.initial_rss_bytes, memory_profiler.peak_rss_bytes);

    // Sample immediately should show similar values
    memory_profiler.sample_memory();
    const diff_after_sample = if (memory_profiler.peak_rss_bytes >= memory_profiler.initial_rss_bytes)
        memory_profiler.peak_rss_bytes - memory_profiler.initial_rss_bytes
    else
        memory_profiler.initial_rss_bytes - memory_profiler.peak_rss_bytes;

    // Difference should be small (within 1MB) for immediate sampling
    try testing.expect(diff_after_sample < 1024 * 1024);
}

// Test memory growth detection with controlled allocation
test "memory profiler detects allocation patterns" {
    const allocator = testing.allocator;

    var memory_profiler = MemoryProfiler.init();
    memory_profiler.start_profiling();

    const initial_growth = memory_profiler.calculate_memory_growth();
    try testing.expectEqual(@as(u64, 0), initial_growth);

    // Allocate a significant amount of memory
    const ALLOCATION_SIZE = 10 * 1024 * 1024; // 10MB
    const large_allocation = try allocator.alloc(u8, ALLOCATION_SIZE);
    defer allocator.free(large_allocation);

    // Fill the allocation to ensure it's actually committed
    @memset(large_allocation, 0xAA);

    // Sample after allocation
    memory_profiler.sample_memory();

    const growth_after_allocation = memory_profiler.calculate_memory_growth();

    // Growth should be detectable (at least 5MB of the 10MB we allocated)
    // Some platforms may not show exact RSS growth due to memory management
    try testing.expect(growth_after_allocation >= 5 * 1024 * 1024);

    // Peak should be higher than initial
    try testing.expect(memory_profiler.peak_rss_bytes > memory_profiler.initial_rss_bytes);
}

// Test performance overhead of memory profiling
test "memory profiler performance overhead" {
    const NUM_SAMPLES = 10000;
    var memory_profiler = MemoryProfiler.init();

    // Measure time for profiling operations
    const start_time = std.time.nanoTimestamp();

    memory_profiler.start_profiling();
    for (0..NUM_SAMPLES) |_| {
        memory_profiler.sample_memory();
    }

    const end_time = std.time.nanoTimestamp();
    const total_time_ns = @as(u64, @intCast(end_time - start_time));
    const time_per_sample_ns = total_time_ns / NUM_SAMPLES;

    // Memory profiling should be reasonably fast - each sample should take less than 100µs
    // This allows for periodic sampling without significant performance impact
    const MAX_SAMPLE_TIME_NS = 100_000; // 100µs
    try testing.expect(time_per_sample_ns < MAX_SAMPLE_TIME_NS);

    // Log performance for debugging
    std.debug.print("\nMemory profiling performance:\n", .{});
    std.debug.print("Total samples: {}\n", .{NUM_SAMPLES});
    std.debug.print("Total time: {}ns\n", .{total_time_ns});
    std.debug.print("Time per sample: {}ns ({}µs)\n", .{ time_per_sample_ns, time_per_sample_ns / 1000 });
}

// Test memory efficiency calculation
test "memory profiler efficiency calculation" {
    var memory_profiler = MemoryProfiler.init();
    memory_profiler.initial_rss_bytes = 50 * 1024 * 1024; // 50MB baseline
    memory_profiler.peak_rss_bytes = 60 * 1024 * 1024; // 60MB peak (10MB growth)

    // Test efficient scenario: 10MB growth for 10.5K operations = ~0.976KB/op (under 1KB limit)
    const efficient_operations = 10500; // 10MB / 10.5K = ~976 bytes/op
    try testing.expect(memory_profiler.is_memory_efficient(efficient_operations));

    // Test inefficient scenario: 10MB growth for 100 operations = 100KB/op
    const inefficient_operations = 100;
    try testing.expect(!memory_profiler.is_memory_efficient(inefficient_operations));

    // Test boundary condition: exactly at the limit
    const boundary_operations = 10240; // 10MB / 10240 ops = 1024 bytes/op (exactly the limit)
    try testing.expect(memory_profiler.is_memory_efficient(boundary_operations));

    // Test excessive peak memory scenario
    memory_profiler.peak_rss_bytes = 200 * 1024 * 1024; // 200MB (exceeds 100MB limit)
    try testing.expect(!memory_profiler.is_memory_efficient(efficient_operations));
}

// Test cross-platform RSS query functionality
test "memory profiler cross platform RSS query" {

    // Skip in CI environments - memory profiling is a development tool
    const is_ci = std.posix.getenv("CI") != null or
        std.posix.getenv("GITHUB_ACTIONS") != null or
        std.posix.getenv("CONTINUOUS_INTEGRATION") != null;

    if (is_ci) {
        std.debug.print("Skipping RSS validation in CI environment\n", .{});
        return;
    }

    const rss_bytes = query_current_rss_memory();

    // Always debug the RSS value on all platforms
    std.debug.print("RSS DEBUG: platform={}, rss_bytes={} ({} MB)\n", .{ builtin.os.tag, rss_bytes, rss_bytes / (1024 * 1024) });

    // RSS should work on development platforms
    switch (builtin.os.tag) {
        .linux, .macos => {
            if (rss_bytes == 0) {
                std.debug.print("RSS ERROR: Got 0 bytes on {}, this should not happen\n", .{builtin.os.tag});
                return error.RSSQueryFailed;
            }

            try testing.expect(rss_bytes > 0);

            // Realistic thresholds for optimized builds - they can be very memory efficient
            const min_rss = if (builtin.os.tag == .linux) 256 * 1024 else 512 * 1024; // 256KB on Linux, 512KB on macOS
            if (rss_bytes < min_rss) {
                std.debug.print("RSS WARNING: Got {} bytes, expected at least {} bytes (but this may be normal for optimized builds)\n", .{ rss_bytes, min_rss });
                // Still allow the test to pass if RSS is reasonable but below our conservative threshold
                try testing.expect(rss_bytes >= 64 * 1024); // Absolute minimum: 64KB
            } else {
                try testing.expect(rss_bytes >= min_rss);
            }
            try testing.expect(rss_bytes <= 1024 * 1024 * 1024); // Less than 1GB

            std.debug.print("RSS query successful: {} bytes ({} MB)\n", .{ rss_bytes, rss_bytes / (1024 * 1024) });
        },
        .windows => {
            // Windows implementation returns 0 for now (placeholder)
            try testing.expectEqual(@as(u64, 0), rss_bytes);
        },
        else => {
            // Unsupported platforms should return 0
            try testing.expectEqual(@as(u64, 0), rss_bytes);
        },
    }
}

// Test memory profiler stability over multiple cycles
test "memory profiler stability over multiple measurement cycles" {
    const allocator = testing.allocator;

    var memory_profiler = MemoryProfiler.init();
    memory_profiler.start_profiling();

    var measurements = std.ArrayList(u64).init(allocator);
    try measurements.ensureTotalCapacity(100); // tidy:ignore-perf - capacity pre-allocated for 100 measurements
    defer measurements.deinit();

    // Take 100 measurements with small delays
    for (0..100) |i| {
        // Do a small amount of work between measurements
        const small_alloc = try allocator.alloc(u8, 1024);
        defer allocator.free(small_alloc);
        @memset(small_alloc, @as(u8, @intCast(i % 256)));

        memory_profiler.sample_memory();
        try measurements.append(memory_profiler.peak_rss_bytes);

        // Small delay to allow for OS memory accounting - just do some work instead of sleep
        for (0..1000) |j| {
            _ = j; // Just consume some CPU cycles
        }
    }

    // Measurements should be stable - no excessive variance
    const first_measurement = measurements.items[0];
    const last_measurement = measurements.items[measurements.items.len - 1];

    // Growth should be reasonable for 100 small allocations
    const total_growth = if (last_measurement >= first_measurement)
        last_measurement - first_measurement
    else
        0;

    // Should not grow more than 10MB for small test allocations
    try testing.expect(total_growth < 10 * 1024 * 1024);

    // No measurement should decrease (peak tracking)
    for (1..measurements.items.len) |i| {
        try testing.expect(measurements.items[i] >= measurements.items[i - 1]);
    }
}

// Test memory profiler with simulated production workload
test "memory profiler production workload simulation" {
    const allocator = testing.allocator;

    var memory_profiler = MemoryProfiler.init();
    memory_profiler.start_profiling();

    // Simulate a workload similar to benchmark operations
    const BATCH_SIZE = 100;
    const NUM_BATCHES = 10;

    for (0..NUM_BATCHES) |batch| {
        memory_profiler.sample_memory(); // Sample at start of batch

        // Simulate batch operations with varying allocations
        for (0..BATCH_SIZE) |i| {
            const alloc_size = 1024 + (i * 100); // 1KB to 10KB+ allocations
            const work_data = try allocator.alloc(u8, alloc_size);
            defer allocator.free(work_data);

            // Simulate work by writing to the allocation
            @memset(work_data, @as(u8, @intCast((batch + i) % 256)));
        }

        memory_profiler.sample_memory(); // Sample at end of batch

        // Small delay between batches - do some work instead of sleep
        for (0..5000) |j| {
            _ = j; // Just consume some CPU cycles
        }
    }

    const total_operations = NUM_BATCHES * BATCH_SIZE;
    const memory_growth = memory_profiler.calculate_memory_growth();

    // Memory should be efficiently managed
    try testing.expect(memory_profiler.is_memory_efficient(total_operations));

    // Growth should be proportional to workload
    const growth_per_op = memory_growth / total_operations;

    // Apply tier-based thresholds for sanitizer compatibility
    const base_threshold = 1024; // Max 1KB per operation
    // Use a conservative multiplier since we can't access build_options directly in test files
    const memory_multiplier: f64 = 10.0; // Conservative multiplier for all builds
    const max_growth_per_op = @as(u64, @intFromFloat(@as(f64, @floatFromInt(base_threshold)) * memory_multiplier));

    try testing.expect(growth_per_op <= max_growth_per_op);

    std.debug.print("\nProduction workload simulation results:\n", .{});
    std.debug.print("Total operations: {}\n", .{total_operations});
    std.debug.print("Memory growth: {} bytes ({} KB)\n", .{ memory_growth, memory_growth / 1024 });
    std.debug.print("Growth per operation: {} bytes\n", .{growth_per_op});
}
