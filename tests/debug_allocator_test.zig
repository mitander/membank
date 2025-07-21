//! Minimal test for DebugAllocator to verify basic functionality

const std = @import("std");
const debug_allocator = @import("../src/debug_allocator.zig");
const DebugAllocator = debug_allocator.DebugAllocator;

test "DebugAllocator simple allocation test" {
    var debug_alloc = DebugAllocator.init(std.testing.allocator);
    const allocator = debug_alloc.allocator();

    // Simple allocation test
    const memory = allocator.alloc(u8, 32) catch |err| {
        std.debug.print("Allocation failed: {}\n", .{err});
        return err;
    };
    defer allocator.free(memory);

    // Write some data to verify it works
    @memset(memory, 0xAB);

    // Verify statistics
    const stats = debug_alloc.statistics();
    try std.testing.expectEqual(@as(u64, 1), stats.active_allocations);
}

test "DebugAllocator zero allocation" {
    var debug_alloc = DebugAllocator.init(std.testing.allocator);
    const allocator = debug_alloc.allocator();

    // Zero-size allocation should return null
    const memory = allocator.alloc(u8, 0) catch null;
    try std.testing.expect(memory == null);
}

test "DebugAllocator statistics" {
    var debug_alloc = DebugAllocator.init(std.testing.allocator);
    const allocator = debug_alloc.allocator();

    const initial_stats = debug_alloc.statistics();
    try std.testing.expectEqual(@as(u64, 0), initial_stats.total_allocations);

    const mem1 = try allocator.alloc(u8, 100);
    defer allocator.free(mem1);

    const stats_after_alloc = debug_alloc.statistics();
    try std.testing.expectEqual(@as(u64, 1), stats_after_alloc.total_allocations);
    try std.testing.expectEqual(@as(u64, 1), stats_after_alloc.active_allocations);
    try std.testing.expectEqual(@as(u64, 100), stats_after_alloc.current_bytes_allocated);
}
