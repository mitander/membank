//! Memory safety tests for CortexDB arena-per-subsystem pattern.
//!
//! Validates comprehensive memory safety patterns and debug allocator functionality.

const cortexdb = @import("cortexdb");
const std = @import("std");
const testing = std.testing;

const debug_allocator = cortexdb.debug_allocator;
const DebugAllocator = debug_allocator.DebugAllocator;

test "arena allocator basic safety" {
    const allocator = testing.allocator;

    var arena = std.heap.ArenaAllocator.init(allocator);
    defer arena.deinit();
    const arena_allocator = arena.allocator();

    // Basic allocation test
    const memory = try arena_allocator.alloc(u8, 64);
    @memset(memory, 0xAB);

    // Verify pattern
    for (memory) |byte| {
        try testing.expectEqual(@as(u8, 0xAB), byte);
    }

    // Arena reset test
    _ = arena.reset(.retain_capacity);
}

test "debug allocator validation" {
    const allocator = testing.allocator;
    var debug_alloc = DebugAllocator.init(allocator);
    const debug_allocator_instance = debug_alloc.allocator();

    // Basic allocation
    const memory = try debug_allocator_instance.alloc(u8, 32);
    defer debug_allocator_instance.free(memory);
    @memset(memory, 0xCD);

    // Verify no corruption detected
    const stats = debug_alloc.statistics();
    try testing.expectEqual(@as(u64, 0), stats.double_frees);
    try testing.expectEqual(@as(u64, 0), stats.buffer_overflows);
}

test "arena subsystem isolation" {
    const allocator = testing.allocator;

    var arena1 = std.heap.ArenaAllocator.init(allocator);
    defer arena1.deinit();
    var arena2 = std.heap.ArenaAllocator.init(allocator);
    defer arena2.deinit();

    const alloc1 = arena1.allocator();
    const alloc2 = arena2.allocator();

    // Different patterns per arena
    const data1 = try alloc1.alloc(u8, 64);
    const data2 = try alloc2.alloc(u8, 64);
    @memset(data1, 0xAA);
    @memset(data2, 0xBB);

    // Verify isolation
    for (data1) |byte| try testing.expectEqual(@as(u8, 0xAA), byte);
    for (data2) |byte| try testing.expectEqual(@as(u8, 0xBB), byte);

    // Reset one arena, other remains valid
    _ = arena1.reset(.retain_capacity);
    for (data2) |byte| try testing.expectEqual(@as(u8, 0xBB), byte);
}

test "debug allocator memory accounting" {
    const allocator = testing.allocator;
    var debug_alloc = DebugAllocator.init(allocator);
    const debug_allocator_instance = debug_alloc.allocator();

    // Track allocations
    const mem1 = try debug_allocator_instance.alloc(u8, 64);
    const mem2 = try debug_allocator_instance.alloc(u8, 128);

    // Verify accounting
    const stats = debug_alloc.statistics();
    try testing.expectEqual(@as(u64, 2), stats.active_allocations);
    try testing.expectEqual(@as(u64, 192), stats.current_bytes_allocated);

    // Free and verify accounting
    debug_allocator_instance.free(mem1);
    debug_allocator_instance.free(mem2);

    const final_stats = debug_alloc.statistics();
    try testing.expectEqual(@as(u64, 0), final_stats.active_allocations);
    try testing.expectEqual(@as(u64, 0), final_stats.current_bytes_allocated);
}

test "buffer boundary safety with debug allocator" {
    const allocator = testing.allocator;
    var debug_alloc = DebugAllocator.init(allocator);
    const debug_allocator_instance = debug_alloc.allocator();

    const buffer = try debug_allocator_instance.alloc(u8, 32);
    defer debug_allocator_instance.free(buffer);

    // Fill buffer (should be safe)
    @memset(buffer, 0xDE);

    // Verify no boundary violations detected
    const stats = debug_alloc.statistics();
    try testing.expectEqual(@as(u64, 0), stats.buffer_overflows);
}

test "cross_allocator_isolation" {
    const allocator = testing.allocator;

    var arena1 = std.heap.ArenaAllocator.init(allocator);
    defer arena1.deinit();
    var arena2 = std.heap.ArenaAllocator.init(allocator);
    defer arena2.deinit();

    const alloc1 = arena1.allocator();
    const alloc2 = arena2.allocator();

    // Allocate from different arenas
    const data1 = try alloc1.alloc(u8, 256);
    const data2 = try alloc2.alloc(u8, 256);

    @memset(data1, 0xAA);
    @memset(data2, 0xBB);

    // Verify isolation
    for (data1) |byte| {
        try testing.expectEqual(@as(u8, 0xAA), byte);
    }
    for (data2) |byte| {
        try testing.expectEqual(@as(u8, 0xBB), byte);
    }

    // Reset one arena - should not affect the other
    _ = arena1.reset(.retain_capacity);

    // data2 should remain valid and uncorrupted
    for (data2) |byte| {
        try testing.expectEqual(@as(u8, 0xBB), byte);
    }
}
