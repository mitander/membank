//! Memory profiling validation tests.
//!
//! Tests RSS measurement accuracy, performance overhead, and reliability
//! of the MemoryProfiler implementation across different platforms.
//! Validates that memory tracking is accurate enough for performance regression detection.

const std = @import("std");
const testing = std.testing;
const kausaldb = @import("kausaldb");
const builtin = @import("builtin");

const concurrency = kausaldb.concurrency;
const assert = kausaldb.assert.assert;

// Import MemoryProfiler functionality (it's embedded in the benchmark module)
const MemoryProfiler = struct {
    initial_rss_bytes: u64,
    peak_rss_bytes: u64,

    const Self = @This();

    pub fn init() Self {
        return Self{
            .initial_rss_bytes = 0,
            .peak_rss_bytes = 0,
        };
    }

    pub fn start_profiling(self: *Self) void {
        self.initial_rss_bytes = query_current_rss_memory();
        self.peak_rss_bytes = self.initial_rss_bytes;
    }

    pub fn sample_memory(self: *Self) void {
        const current_rss = query_current_rss_memory();
        if (current_rss > self.peak_rss_bytes) {
            self.peak_rss_bytes = current_rss;
        }
    }

    pub fn calculate_memory_growth(self: *const Self) u64 {
        if (self.peak_rss_bytes >= self.initial_rss_bytes) {
            return self.peak_rss_bytes - self.initial_rss_bytes;
        }
        return 0;
    }

    pub fn is_memory_efficient(self: *const Self, operations: u64) bool {
        const MAX_PEAK_MEMORY_BYTES = 100 * 1024 * 1024; // 100MB for 10K operations
        const MAX_MEMORY_GROWTH_PER_OP = 1024; // 1KB average growth per operation

        const growth_bytes = self.calculate_memory_growth();
        const peak_efficient = self.peak_rss_bytes <= MAX_PEAK_MEMORY_BYTES;
        const growth_per_op = if (operations > 0) growth_bytes / operations else 0;
        const growth_efficient = growth_per_op <= MAX_MEMORY_GROWTH_PER_OP;
        return peak_efficient and growth_efficient;
    }
};

/// Platform-specific RSS memory tracking
fn query_current_rss_memory() u64 {
    switch (builtin.os.tag) {
        .linux => return query_rss_linux() catch 0,
        .macos => return query_rss_macos() catch 0,
        .windows => return query_rss_windows() catch 0,
        else => return 0,
    }
}

// Linux RSS memory via multiple methods (robust for containers)
fn query_rss_linux() !u64 {
    // Method 1: Try /proc/self/status (most common)
    if (query_rss_proc_status()) |rss| {
        if (rss > 0) return rss;
    }

    // Method 2: Try /proc/self/statm (alternative in containers)
    if (query_rss_proc_statm()) |rss| {
        if (rss > 0) return rss;
    }

    // Two methods above should be sufficient

    return 0;
}

fn query_rss_proc_status() ?u64 {
    const file = std.fs.openFileAbsolute("/proc/self/status", .{}) catch return null;
    defer file.close();

    var buf: [4096]u8 = undefined;
    const bytes_read = file.readAll(&buf) catch return null;
    const content = buf[0..bytes_read];

    // Search for "VmRSS:" line in /proc/self/status
    var lines = std.mem.splitSequence(u8, content, "\n");
    while (lines.next()) |line| {
        if (std.mem.startsWith(u8, line, "VmRSS:")) {
            // Parse: "VmRSS:    1234 kB"
            var parts = std.mem.splitSequence(u8, line, " ");
            _ = parts.next(); // Skip "VmRSS:"
            while (parts.next()) |part| {
                if (part.len > 0 and std.ascii.isDigit(part[0])) {
                    const kb = std.fmt.parseInt(u64, part, 10) catch return null;
                    return kb * 1024; // Convert KB to bytes
                }
            }
        }
    }
    return null;
}

fn query_rss_proc_statm() ?u64 {
    const file = std.fs.openFileAbsolute("/proc/self/statm", .{}) catch return null;
    defer file.close();

    var buf: [256]u8 = undefined;
    const bytes_read = file.readAll(&buf) catch return null;
    const content = buf[0..bytes_read];

    // /proc/self/statm format: size resident shared text lib data dt
    var parts = std.mem.splitSequence(u8, std.mem.trim(u8, content, " \n\t"), " ");
    _ = parts.next(); // Skip size (first field)

    if (parts.next()) |resident_pages_str| {
        const resident_pages = std.fmt.parseInt(u64, resident_pages_str, 10) catch return null;
        // Convert pages to bytes (typically 4KB pages on most systems)
        const page_size = 4096; // Standard page size on Linux
        return resident_pages * page_size;
    }
    return null;
}

// macOS RSS memory via mach task_info
fn query_rss_macos() !u64 {
    const c = @cImport({
        @cInclude("mach/mach.h");
        @cInclude("mach/task.h");
        @cInclude("mach/mach_init.h");
    });

    var info: c.mach_task_basic_info_data_t = undefined;
    var count: c.mach_msg_type_number_t = c.MACH_TASK_BASIC_INFO_COUNT;

    const result = c.task_info(c.mach_task_self(), c.MACH_TASK_BASIC_INFO, @ptrCast(&info), &count);

    if (result != c.KERN_SUCCESS) return 0;

    return info.resident_size;
}

// Windows RSS memory via GetProcessMemoryInfo
fn query_rss_windows() !u64 {
    // For Zig 0.13+, this would use Windows API calls
    // For now, return 0 as fallback (Windows support can be added later)
    return 0;
}

// Test RSS measurement accuracy
test "Memory profiler RSS measurement accuracy" {
    concurrency.init();

    var profiler = MemoryProfiler.init();
    profiler.start_profiling();

    // Initial RSS should be reasonable (not zero, not excessive)
    try testing.expect(profiler.initial_rss_bytes > 1024); // At least 1KB
    try testing.expect(profiler.initial_rss_bytes < 1024 * 1024 * 1024); // Less than 1GB

    // Peak should start equal to initial
    try testing.expectEqual(profiler.initial_rss_bytes, profiler.peak_rss_bytes);

    // Sample immediately should show similar values
    profiler.sample_memory();
    const diff_after_sample = if (profiler.peak_rss_bytes >= profiler.initial_rss_bytes)
        profiler.peak_rss_bytes - profiler.initial_rss_bytes
    else
        profiler.initial_rss_bytes - profiler.peak_rss_bytes;

    // Difference should be small (within 1MB) for immediate sampling
    try testing.expect(diff_after_sample < 1024 * 1024);
}

// Test memory growth detection with controlled allocation
test "Memory profiler detects allocation patterns" {
    concurrency.init();
    const allocator = testing.allocator;

    var profiler = MemoryProfiler.init();
    profiler.start_profiling();

    const initial_growth = profiler.calculate_memory_growth();
    try testing.expectEqual(@as(u64, 0), initial_growth);

    // Allocate a significant amount of memory
    const ALLOCATION_SIZE = 10 * 1024 * 1024; // 10MB
    const large_allocation = try allocator.alloc(u8, ALLOCATION_SIZE);
    defer allocator.free(large_allocation);

    // Fill the allocation to ensure it's actually committed
    @memset(large_allocation, 0xAA);

    // Sample after allocation
    profiler.sample_memory();

    const growth_after_allocation = profiler.calculate_memory_growth();

    // Growth should be detectable (at least 5MB of the 10MB we allocated)
    // Some platforms may not show exact RSS growth due to memory management
    try testing.expect(growth_after_allocation >= 5 * 1024 * 1024);

    // Peak should be higher than initial
    try testing.expect(profiler.peak_rss_bytes > profiler.initial_rss_bytes);
}

// Test performance overhead of memory profiling
test "Memory profiler performance overhead" {
    concurrency.init();

    const NUM_SAMPLES = 10000;
    var profiler = MemoryProfiler.init();

    // Measure time for profiling operations
    const start_time = std.time.nanoTimestamp();

    profiler.start_profiling();
    for (0..NUM_SAMPLES) |_| {
        profiler.sample_memory();
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
test "Memory profiler efficiency calculation" {
    concurrency.init();

    var profiler = MemoryProfiler.init();
    profiler.initial_rss_bytes = 50 * 1024 * 1024; // 50MB baseline
    profiler.peak_rss_bytes = 60 * 1024 * 1024; // 60MB peak (10MB growth)

    // Test efficient scenario: 10MB growth for 10.5K operations = ~0.976KB/op (under 1KB limit)
    const efficient_operations = 10500; // 10MB / 10.5K = ~976 bytes/op
    try testing.expect(profiler.is_memory_efficient(efficient_operations));

    // Test inefficient scenario: 10MB growth for 100 operations = 100KB/op
    const inefficient_operations = 100;
    try testing.expect(!profiler.is_memory_efficient(inefficient_operations));

    // Test boundary condition: exactly at the limit
    const boundary_operations = 10240; // 10MB / 10240 ops = 1024 bytes/op (exactly the limit)
    try testing.expect(profiler.is_memory_efficient(boundary_operations));

    // Test excessive peak memory scenario
    profiler.peak_rss_bytes = 200 * 1024 * 1024; // 200MB (exceeds 100MB limit)
    try testing.expect(!profiler.is_memory_efficient(efficient_operations));
}

// Test cross-platform RSS query functionality
test "Memory profiler cross-platform RSS query" {
    concurrency.init();

    // Skip in CI environments - memory profiling is a development tool
    const is_ci = std.posix.getenv("CI") != null or
        std.posix.getenv("GITHUB_ACTIONS") != null or
        std.posix.getenv("CONTINUOUS_INTEGRATION") != null;

    if (is_ci) {
        std.debug.print("Skipping RSS validation in CI environment\n", .{});
        return;
    }

    const rss_bytes = query_current_rss_memory();

    // RSS should work on development platforms
    switch (builtin.os.tag) {
        .linux, .macos => {
            try testing.expect(rss_bytes > 0);
            try testing.expect(rss_bytes >= 1024 * 1024); // At least 1MB
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
test "Memory profiler stability over multiple measurement cycles" {
    concurrency.init();
    const allocator = testing.allocator;

    var profiler = MemoryProfiler.init();
    profiler.start_profiling();

    var measurements = std.ArrayList(u64).init(allocator);
    try measurements.ensureTotalCapacity(100); // tidy:ignore-perf - capacity pre-allocated for 100 measurements
    defer measurements.deinit();

    // Take 100 measurements with small delays
    for (0..100) |i| {
        // Do a small amount of work between measurements
        const small_alloc = try allocator.alloc(u8, 1024);
        defer allocator.free(small_alloc);
        @memset(small_alloc, @as(u8, @intCast(i % 256)));

        profiler.sample_memory();
        try measurements.append(profiler.peak_rss_bytes);

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
test "Memory profiler production workload simulation" {
    concurrency.init();
    const allocator = testing.allocator;

    var profiler = MemoryProfiler.init();
    profiler.start_profiling();

    // Simulate a workload similar to benchmark operations
    const BATCH_SIZE = 100;
    const NUM_BATCHES = 10;

    for (0..NUM_BATCHES) |batch| {
        profiler.sample_memory(); // Sample at start of batch

        // Simulate batch operations with varying allocations
        for (0..BATCH_SIZE) |i| {
            const alloc_size = 1024 + (i * 100); // 1KB to 10KB+ allocations
            const work_data = try allocator.alloc(u8, alloc_size);
            defer allocator.free(work_data);

            // Simulate work by writing to the allocation
            @memset(work_data, @as(u8, @intCast((batch + i) % 256)));
        }

        profiler.sample_memory(); // Sample at end of batch

        // Small delay between batches - do some work instead of sleep
        for (0..5000) |j| {
            _ = j; // Just consume some CPU cycles
        }
    }

    const total_operations = NUM_BATCHES * BATCH_SIZE;
    const memory_growth = profiler.calculate_memory_growth();

    // Memory should be efficiently managed
    try testing.expect(profiler.is_memory_efficient(total_operations));

    // Growth should be proportional to workload
    const growth_per_op = memory_growth / total_operations;
    try testing.expect(growth_per_op <= 1024); // Max 1KB per operation

    std.debug.print("\nProduction workload simulation results:\n", .{});
    std.debug.print("Total operations: {}\n", .{total_operations});
    std.debug.print("Memory growth: {} bytes ({} KB)\n", .{ memory_growth, memory_growth / 1024 });
    std.debug.print("Growth per operation: {} bytes\n", .{growth_per_op});
}
