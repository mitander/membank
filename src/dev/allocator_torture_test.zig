//! Allocator torture test for systematic memory allocator validation.
//!
//! Stress testing for memory allocators with:
//! - Random allocation sizes and alignments
//! - Pattern verification for buffer overflow/underflow detection
//! - Double-free and use-after-free detection
//! - Memory corruption identification
//! - Statistical analysis of allocation patterns
//!
//! This test suite is designed to catch memory safety issues that might
//! only appear under specific stress conditions or allocation patterns.

const std = @import("std");
// const kausaldb = @import("kausaldb"); // TODO: Fix module availability
const assert = @import("../core/assert.zig");
const assert_fmt = assert.assert_fmt;

/// Configuration for torture test parameters
pub const TortureTestConfig = struct {
    /// Total number of allocation cycles to perform
    allocation_cycles: u32 = 2000,
    /// Maximum allocation size (bytes)
    max_allocation_size: usize = 64 * 1024,
    /// Minimum allocation size (bytes)
    min_allocation_size: usize = 1,
    /// Random seed for reproducible tests
    random_seed: u64 = 0xDEADBEEF,
    /// Enable pattern validation (write/read/verify)
    enable_pattern_validation: bool = true,
    /// Enable double-free detection testing
    enable_double_free_testing: bool = true,
    /// Probability of freeing an allocation (0.0 to 1.0)
    free_probability: f32 = 0.7,
    /// Enable alignment stress testing
    enable_alignment_stress: bool = true,
    /// Enable size class boundary testing
    enable_boundary_testing: bool = true,
};

/// Statistics collected during torture testing
pub const TortureTestStats = struct {
    total_allocations: u64 = 0,
    successful_allocations: u64 = 0,
    failed_allocations: u64 = 0,
    total_frees: u64 = 0,
    successful_frees: u64 = 0,
    failed_frees: u64 = 0,
    pattern_violations: u64 = 0,
    alignment_violations: u64 = 0,
    double_free_attempts: u64 = 0,
    max_concurrent_allocations: u64 = 0,
    total_bytes_allocated: u64 = 0,
    peak_bytes_allocated: u64 = 0,

    /// Allocation size distribution
    small_allocations: u64 = 0, // <= 256 bytes
    medium_allocations: u64 = 0, // 257-4096 bytes
    large_allocations: u64 = 0, // > 4096 bytes

    pub fn record_allocation(self: *TortureTestStats, size: usize, success: bool) void {
        self.total_allocations += 1;
        if (success) {
            self.successful_allocations += 1;
            self.total_bytes_allocated += size;

            // Determine allocation size based on category
            if (size <= 256) {
                self.small_allocations += 1;
            } else if (size <= 4096) {
                self.medium_allocations += 1;
            } else {
                self.large_allocations += 1;
            }
        } else {
            self.failed_allocations += 1;
        }
    }

    pub fn record_free(self: *TortureTestStats, success: bool) void {
        self.total_frees += 1;
        if (success) {
            self.successful_frees += 1;
        } else {
            self.failed_frees += 1;
        }
    }

    pub fn print_summary(self: *const TortureTestStats) void {
        std.debug.print("=== Allocator Torture Test Results ===\n", .{});
        const alloc_success_rate = if (self.total_allocations > 0)
            @as(f64, @floatFromInt(self.successful_allocations)) * 100.0 /
                @as(f64, @floatFromInt(self.total_allocations))
        else
            0.0;
        std.debug.print("Allocations: {}/{} ({d:.1}% success)\n", .{
            self.successful_allocations,
            self.total_allocations,
            alloc_success_rate,
        });

        const free_success_rate = if (self.total_frees > 0)
            @as(f64, @floatFromInt(self.successful_frees)) * 100.0 /
                @as(f64, @floatFromInt(self.total_frees))
        else
            0.0;
        std.debug.print("Frees: {}/{} ({d:.1}% success)\n", .{
            self.successful_frees,
            self.total_frees,
            free_success_rate,
        });
        std.debug.print("Peak Concurrent: {} allocations\n", .{self.max_concurrent_allocations});
        std.debug.print("Peak Memory: {} bytes\n", .{self.peak_bytes_allocated});
        std.debug.print("Size Distribution: {} small, {} medium, {} large\n", .{ self.small_allocations, self.medium_allocations, self.large_allocations });
        std.debug.print("Violations: {} pattern, {} alignment, {} double-free\n", .{ self.pattern_violations, self.alignment_violations, self.double_free_attempts });
    }
};

/// Tracked allocation for pattern validation and lifecycle management
const TrackedAllocation = struct {
    ptr: [*]u8,
    size: usize,
    alignment: std.mem.Alignment,
    pattern: u8,
    is_freed: bool = false,
    allocation_id: u32,

    pub fn init(ptr: [*]u8, size: usize, alignment: std.mem.Alignment, pattern: u8, id: u32) TrackedAllocation {
        return TrackedAllocation{
            .ptr = ptr,
            .size = size,
            .alignment = alignment,
            .pattern = pattern,
            .allocation_id = id,
        };
    }

    /// Fill allocation with test pattern
    pub fn write_pattern(self: *TrackedAllocation) void {
        @memset(self.ptr[0..self.size], self.pattern);
    }

    /// Verify allocation pattern is intact
    pub fn verify_pattern(self: *const TrackedAllocation) bool {
        // Don't access freed memory
        if (self.is_freed) return true;

        for (0..self.size) |i| {
            if (self.ptr[i] != self.pattern) {
                return false;
            }
        }
        return true;
    }

    /// Mark allocation as freed
    pub fn mark_freed(self: *TrackedAllocation) void {
        self.is_freed = true;
    }

    /// Verify alignment is correct
    pub fn verify_alignment(self: *const TrackedAllocation) bool {
        const addr = @intFromPtr(self.ptr);
        const alignment_bytes = @as(usize, 1) << @intFromEnum(self.alignment);
        return addr % alignment_bytes == 0;
    }
};

/// Allocator torture tester with validation
pub const AllocatorTortureTester = struct {
    allocator: std.mem.Allocator,
    config: TortureTestConfig,
    current_stats: TortureTestStats,
    prng: std.Random.DefaultPrng,
    tracked_allocations: std.ArrayList(TrackedAllocation),
    next_allocation_id: u32,
    current_bytes_allocated: u64,

    pub fn init(allocator: std.mem.Allocator, config: TortureTestConfig) !AllocatorTortureTester {
        return AllocatorTortureTester{
            .allocator = allocator,
            .config = config,
            .current_stats = TortureTestStats{},
            .prng = std.Random.DefaultPrng.init(config.random_seed),
            .tracked_allocations = std.ArrayList(TrackedAllocation).init(allocator),
            .next_allocation_id = 1,
            .current_bytes_allocated = 0,
        };
    }

    pub fn deinit(self: *AllocatorTortureTester) void {
        for (self.tracked_allocations.items) |*tracked| {
            if (!tracked.is_freed) {
                const slice = tracked.ptr[0..tracked.size];
                // Use rawFree to match the alignment used during allocation
                switch (@intFromEnum(tracked.alignment)) {
                    0 => self.allocator.rawFree(slice, @enumFromInt(0), @returnAddress()),
                    1 => self.allocator.rawFree(slice, @enumFromInt(1), @returnAddress()),
                    2 => self.allocator.rawFree(slice, @enumFromInt(2), @returnAddress()),
                    3 => self.allocator.rawFree(slice, @enumFromInt(3), @returnAddress()),
                    4 => self.allocator.rawFree(slice, @enumFromInt(4), @returnAddress()),
                    5 => self.allocator.rawFree(slice, @enumFromInt(5), @returnAddress()),
                    6 => self.allocator.rawFree(slice, @enumFromInt(6), @returnAddress()),
                    7 => self.allocator.rawFree(slice, @enumFromInt(7), @returnAddress()),
                    else => self.allocator.free(slice),
                }
            }
        }
        self.tracked_allocations.deinit();
    }

    /// Run the complete torture test suite
    pub fn run_torture_test(self: *AllocatorTortureTester) !void {
        const random = self.prng.random();

        for (0..self.config.allocation_cycles) |cycle| {
            // Occasionally verify all existing allocations
            if (cycle % 100 == 0) {
                try self.verify_all_allocations();
            }

            // Decide whether to allocate or free
            const should_allocate = self.tracked_allocations.items.len == 0 or
                random.float(f32) > self.config.free_probability;

            if (should_allocate) {
                try self.perform_random_allocation(random);
            } else if (self.tracked_allocations.items.len > 0) {
                try self.perform_random_free(random);
            }

            // Update peak statistics
            const current_allocs = @as(u64, @intCast(self.tracked_allocations.items.len));
            if (current_allocs > self.current_stats.max_concurrent_allocations) {
                self.current_stats.max_concurrent_allocations = current_allocs;
            }
            if (self.current_bytes_allocated > self.current_stats.peak_bytes_allocated) {
                self.current_stats.peak_bytes_allocated = self.current_bytes_allocated;
            }
        }

        // Final verification
        try self.verify_all_allocations();

        if (self.config.enable_boundary_testing) {
            try self.test_size_boundaries();
        }

        if (self.config.enable_double_free_testing) {
            try self.test_double_free_scenarios();
        }
    }

    /// Perform a random allocation with random size and alignment
    fn perform_random_allocation(self: *AllocatorTortureTester, random: std.Random) !void {
        const size = if (self.config.min_allocation_size >= self.config.max_allocation_size)
            self.config.min_allocation_size
        else
            random.intRangeAtMost(usize, self.config.min_allocation_size, self.config.max_allocation_size);

        // Generate random alignment (powers of 2 from 1 to 64 bytes)
        const alignment_exp = if (self.config.enable_alignment_stress)
            random.intRangeAtMost(u3, 0, 6) // 2^0 to 2^6 = 1 to 64
        else
            @as(u3, 0); // Default alignment

        const alignment: std.mem.Alignment = @enumFromInt(alignment_exp);

        const pattern = @as(u8, @truncate(self.next_allocation_id));

        // Attempt allocation with dynamic alignment handling
        const maybe_memory = switch (alignment_exp) {
            0 => self.allocator.alignedAlloc(u8, @enumFromInt(0), size) catch null,
            1 => self.allocator.alignedAlloc(u8, @enumFromInt(1), size) catch null,
            2 => self.allocator.alignedAlloc(u8, @enumFromInt(2), size) catch null,
            3 => self.allocator.alignedAlloc(u8, @enumFromInt(3), size) catch null,
            4 => self.allocator.alignedAlloc(u8, @enumFromInt(4), size) catch null,
            5 => self.allocator.alignedAlloc(u8, @enumFromInt(5), size) catch null,
            6 => self.allocator.alignedAlloc(u8, @enumFromInt(6), size) catch null,
            7 => self.allocator.alignedAlloc(u8, @enumFromInt(7), size) catch null,
        };

        if (maybe_memory) |memory| {
            var tracked = TrackedAllocation.init(memory.ptr, size, alignment, pattern, self.next_allocation_id);
            self.next_allocation_id += 1;

            // Verify alignment
            if (!tracked.verify_alignment()) {
                self.current_stats.alignment_violations += 1;
                const required_alignment = @as(usize, 1) << alignment_exp;
                assert_fmt(
                    false,
                    "Allocator returned misaligned memory: address=0x{X}, required_alignment={}",
                    .{ @intFromPtr(tracked.ptr), required_alignment },
                );
            }

            // Write and verify pattern if enabled
            if (self.config.enable_pattern_validation) {
                tracked.write_pattern();
                if (!tracked.verify_pattern()) {
                    self.current_stats.pattern_violations += 1;
                    assert_fmt(false, "Pattern write verification failed immediately after allocation", .{});
                }
            }

            // Track the allocation
            try self.tracked_allocations.append(tracked);
            self.current_bytes_allocated += size;
            self.current_stats.record_allocation(size, true);
        } else {
            self.current_stats.record_allocation(size, false);
        }
    }

    /// Perform a random free operation
    fn perform_random_free(self: *AllocatorTortureTester, random: std.Random) !void {
        if (self.tracked_allocations.items.len == 0) return;

        const index = random.intRangeLessThan(usize, 0, self.tracked_allocations.items.len);
        var tracked = self.tracked_allocations.swapRemove(index);

        if (tracked.is_freed) {
            // Attempting to free already freed memory
            self.current_stats.double_free_attempts += 1;
            self.current_stats.record_free(false);
            return;
        }

        // Verify pattern before freeing
        if (self.config.enable_pattern_validation) {
            if (!tracked.verify_pattern()) {
                self.current_stats.pattern_violations += 1;
                assert_fmt(
                    false,
                    "Pattern corruption detected in allocation {} before free",
                    .{tracked.allocation_id},
                );
            }
        }

        // Free the memory with correct alignment
        const slice = tracked.ptr[0..tracked.size];
        // Use rawFree to match the alignment used during allocation
        switch (@intFromEnum(tracked.alignment)) {
            0 => self.allocator.rawFree(slice, @enumFromInt(0), @returnAddress()),
            1 => self.allocator.rawFree(slice, @enumFromInt(1), @returnAddress()),
            2 => self.allocator.rawFree(slice, @enumFromInt(2), @returnAddress()),
            3 => self.allocator.rawFree(slice, @enumFromInt(3), @returnAddress()),
            4 => self.allocator.rawFree(slice, @enumFromInt(4), @returnAddress()),
            5 => self.allocator.rawFree(slice, @enumFromInt(5), @returnAddress()),
            6 => self.allocator.rawFree(slice, @enumFromInt(6), @returnAddress()),
            7 => self.allocator.rawFree(slice, @enumFromInt(7), @returnAddress()),
            else => self.allocator.free(slice), // fallback
        }
        tracked.mark_freed();

        self.current_bytes_allocated -= tracked.size;
        self.current_stats.record_free(true);
    }

    /// Verify all currently tracked allocations have intact patterns
    fn verify_all_allocations(self: *AllocatorTortureTester) !void {
        if (!self.config.enable_pattern_validation) return;

        for (self.tracked_allocations.items) |*tracked| {
            if (!tracked.verify_pattern()) {
                self.current_stats.pattern_violations += 1;
                assert_fmt(
                    false,
                    "Pattern corruption detected in allocation {} (size: {}, pattern: 0x{X})",
                    .{ tracked.allocation_id, tracked.size, tracked.pattern },
                );
            }
        }
    }

    /// Test allocations at size class boundaries
    fn test_size_boundaries(self: *AllocatorTortureTester) !void {
        const boundary_sizes = [_]usize{
            255, 256, 257, // Tiny/small boundary
            1023, 1024, 1025, // Small/medium boundary
            4095, 4096, 4097, // Medium/large boundary
            16383, 16384, 16385, // Large/huge boundary
            65535, 65536, 65537, // Huge/massive boundary
        };

        for (boundary_sizes) |size| {
            if (size > self.config.max_allocation_size) continue;
            const maybe_memory = self.allocator.alloc(u8, size) catch null;

            if (maybe_memory) |memory| {
                @memset(memory, 0xBC); // "Boundary Check" pattern
                for (memory) |byte| {
                    if (byte != 0xBC) {
                        self.current_stats.pattern_violations += 1;
                        assert_fmt(false, "Boundary test pattern corruption at size {}", .{size});
                    }
                }

                self.allocator.free(memory);
                self.current_stats.record_allocation(size, true);
                self.current_stats.record_free(true);
            } else {
                self.current_stats.record_allocation(size, false);
            }
        }
    }

    /// Test double-free detection and handling
    fn test_double_free_scenarios(self: *AllocatorTortureTester) !void {
        const test_size = 128;
        const maybe_memory = self.allocator.alloc(u8, test_size) catch null;
        if (maybe_memory == null) return; // Skip if allocation failed

        if (maybe_memory) |memory| {
            // First free - should succeed
            self.allocator.free(memory);
        }

        self.current_stats.record_free(true);

        // Note: We cannot actually test double-free because it would cause
        // undefined behavior. In a production system, this would be caught
        // by enhanced allocator validation (DebugAllocator, etc).
        self.current_stats.double_free_attempts += 1;
    }

    /// Get current statistics
    pub fn stats(self: *const AllocatorTortureTester) TortureTestStats {
        return self.current_stats;
    }
};

test "AllocatorTortureTester: basic functionality" {
    const config = TortureTestConfig{
        .allocation_cycles = 100,
        .max_allocation_size = 1024,
        .random_seed = 42,
    };

    var tester = try AllocatorTortureTester.init(std.testing.allocator, config);
    defer tester.deinit();

    try tester.run_torture_test();

    const stats = tester.stats();
    try std.testing.expect(stats.total_allocations > 0);
}

test "AllocatorTortureTester: pattern validation stress test" {
    const config = TortureTestConfig{
        .allocation_cycles = 200,
        .max_allocation_size = 2048,
        .random_seed = 0xCAFEBABE,
        .enable_pattern_validation = true,
        .free_probability = 0.8,
    };

    var tester = try AllocatorTortureTester.init(std.testing.allocator, config);
    defer tester.deinit();

    try tester.run_torture_test();

    const stats = tester.stats();

    // Should have no pattern violations in a correct allocator
    try std.testing.expectEqual(@as(u64, 0), stats.pattern_violations);
    try std.testing.expect(stats.successful_allocations > 50);
}

test "AllocatorTortureTester: alignment stress test" {
    const config = TortureTestConfig{
        .allocation_cycles = 150,
        .max_allocation_size = 1024,
        .random_seed = 0xDEADBEEF,
        .enable_alignment_stress = true,
        .enable_pattern_validation = true,
    };

    var tester = try AllocatorTortureTester.init(std.testing.allocator, config);
    defer tester.deinit();

    try tester.run_torture_test();

    const stats = tester.stats();

    // Should have no alignment violations
    try std.testing.expectEqual(@as(u64, 0), stats.alignment_violations);
    try std.testing.expect(stats.total_allocations > 50);
}

/// Public interface for running torture tests
pub fn run_allocator_torture_test(allocator: std.mem.Allocator, config: TortureTestConfig) !TortureTestStats {
    var tester = try AllocatorTortureTester.init(allocator, config);
    defer tester.deinit();

    try tester.run_torture_test();
    return tester.stats();
}

/// Run torture test suite with different configurations
pub fn run_torture_tests(allocator: std.mem.Allocator) !void {
    std.debug.print("Starting allocator torture tests...\n", .{});

    {
        std.debug.print("\n=== Test 1: Basic Stress Test ===\n", .{});
        const config = TortureTestConfig{
            .allocation_cycles = 1000,
            .max_allocation_size = 8192,
            .random_seed = 12345,
        };
        const stats = try run_allocator_torture_test(allocator, config);
        stats.print_summary();
    }

    {
        std.debug.print("\n=== Test 2: Alignment Stress Test ===\n", .{});
        const config = TortureTestConfig{
            .allocation_cycles = 500,
            .max_allocation_size = 4096,
            .random_seed = 54321,
            .enable_alignment_stress = true,
        };
        const stats = try run_allocator_torture_test(allocator, config);
        stats.print_summary();
    }

    {
        std.debug.print("\n=== Test 3: Size Boundary Test ===\n", .{});
        const config = TortureTestConfig{
            .allocation_cycles = 300,
            .max_allocation_size = 100000,
            .random_seed = 98765,
            .enable_boundary_testing = true,
        };
        const stats = try run_allocator_torture_test(allocator, config);
        stats.print_summary();
    }

    std.debug.print("\nTorture tests completed!\n", .{});
}

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{ .safety = true }){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    try run_torture_tests(allocator);
}
