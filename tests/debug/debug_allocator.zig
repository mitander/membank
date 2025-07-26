//! Enhanced Memory Debugging Infrastructure Tests
//!
//! Comprehensive test suite for the DebugAllocator as specified in Priority 0
//! of CORTEX_TODO.md. Validates memory debugging capabilities including
//! allocation tracking, statistics collection, and error detection.

const cortexdb = @import("cortexdb");
const std = @import("std");
const testing = std.testing;
const debug_allocator = cortexdb.debug_allocator;

const DebugAllocator = debug_allocator.DebugAllocator;

test "DebugAllocator basic allocation tracking" {
    // Arena per test for perfect isolation as per STYLE.md
    const allocator = testing.allocator;

    // Layer DebugAllocator over arena for enhanced debugging
    var debug_alloc = DebugAllocator.init(allocator);
    const debug_allocator_instance = debug_alloc.allocator();

    const memory = try debug_allocator_instance.alloc(u8, 32);
    defer debug_allocator_instance.free(memory);

    // Write pattern to verify memory integrity
    @memset(memory, 0xAB);

    // Verify statistics tracking
    const stats = debug_alloc.statistics();
    try testing.expectEqual(@as(u64, 1), stats.active_allocations);
    try testing.expectEqual(@as(u64, 32), stats.current_bytes_allocated);
}

test "DebugAllocator zero allocation handling" {
    const allocator = testing.allocator;

    var debug_alloc = DebugAllocator.init(allocator);
    const debug_allocator_instance = debug_alloc.allocator();

    // Zero-size allocation should return empty slice
    const memory = try debug_allocator_instance.alloc(u8, 0);
    try testing.expect(memory.len == 0);

    // Statistics should reflect no real allocation
    const stats = debug_alloc.statistics();
    try testing.expectEqual(@as(u64, 0), stats.total_allocations);
}

test "DebugAllocator comprehensive statistics tracking" {
    const allocator = testing.allocator;

    var debug_alloc = DebugAllocator.init(allocator);
    const debug_allocator_instance = debug_alloc.allocator();

    const initial_stats = debug_alloc.statistics();
    try testing.expectEqual(@as(u64, 0), initial_stats.total_allocations);

    // Multiple allocations to test peak tracking
    const mem1 = try debug_allocator_instance.alloc(u8, 100);
    const mem2 = try debug_allocator_instance.alloc(u8, 200);

    const peak_stats = debug_alloc.statistics();
    try testing.expectEqual(@as(u64, 2), peak_stats.total_allocations);
    try testing.expectEqual(@as(u64, 2), peak_stats.active_allocations);
    try testing.expectEqual(@as(u64, 300), peak_stats.current_bytes_allocated);
    try testing.expectEqual(@as(u64, 2), peak_stats.peak_allocations);

    debug_allocator_instance.free(mem1);
    debug_allocator_instance.free(mem2);

    const final_stats = debug_alloc.statistics();
    try testing.expectEqual(@as(u64, 2), final_stats.total_allocations);
    try testing.expectEqual(@as(u64, 2), final_stats.total_deallocations);
    try testing.expectEqual(@as(u64, 0), final_stats.active_allocations);
    try testing.expectEqual(@as(u64, 0), final_stats.current_bytes_allocated);
    try testing.expectEqual(@as(u64, 2), final_stats.peak_allocations);
}

test "DebugAllocator allocation validation and error detection" {
    const allocator = testing.allocator;

    var debug_alloc = DebugAllocator.init(allocator);
    const debug_allocator_instance = debug_alloc.allocator();

    // Allocate memory that will be validated
    const memory = try debug_allocator_instance.alloc(u8, 64);
    defer debug_allocator_instance.free(memory);

    // Write known pattern
    for (memory, 0..) |*byte, i| {
        byte.* = @as(u8, @truncate(i & 0xFF));
    }

    // Validate all allocations are intact
    try debug_alloc.validate_all_allocations();

    // Verify statistics show no corruption
    const stats = debug_alloc.statistics();
    try testing.expectEqual(@as(u64, 0), stats.buffer_overflows);
    try testing.expectEqual(@as(u64, 0), stats.double_frees);
    try testing.expectEqual(@as(u64, 0), stats.invalid_frees);
}

test "DebugAllocator mixed allocation sizes and alignments" {
    const allocator = testing.allocator;

    var debug_alloc = DebugAllocator.init(allocator);
    const debug_allocator_instance = debug_alloc.allocator();

    // Test various allocation sizes
    const small_mem = try debug_allocator_instance.alloc(u8, 16);
    const medium_mem = try debug_allocator_instance.alloc(u32, 128);
    const large_mem = try debug_allocator_instance.alloc(u64, 1024);

    // Verify all allocations are tracked
    const stats = debug_alloc.statistics();
    try testing.expectEqual(@as(u64, 3), stats.active_allocations);
    try testing.expect(stats.current_bytes_allocated > 0);

    // Free in different order to test tracking
    debug_allocator_instance.free(medium_mem);
    debug_allocator_instance.free(small_mem);
    debug_allocator_instance.free(large_mem);

    const final_stats = debug_alloc.statistics();
    try testing.expectEqual(@as(u64, 0), final_stats.active_allocations);
    try testing.expectEqual(@as(u64, 0), final_stats.current_bytes_allocated);
}

test "DebugAllocator stress test with multiple allocation cycles" {
    const allocator = testing.allocator;

    var debug_alloc = DebugAllocator.init(allocator);
    const debug_allocator_instance = debug_alloc.allocator();

    // Perform multiple allocation/deallocation cycles
    for (0..20) |i| {
        const size = 64 + (i * 32);
        const memory = try debug_allocator_instance.alloc(u8, size);

        // Write test pattern to verify memory integrity
        @memset(memory, @as(u8, @truncate(i)));

        debug_allocator_instance.free(memory);
    }

    // Verify clean statistics after all operations
    const final_stats = debug_alloc.statistics();
    try testing.expectEqual(@as(u64, 20), final_stats.total_allocations);
    try testing.expectEqual(@as(u64, 20), final_stats.total_deallocations);
    try testing.expectEqual(@as(u64, 0), final_stats.active_allocations);
    try testing.expectEqual(@as(u64, 0), final_stats.double_frees);
    try testing.expectEqual(@as(u64, 0), final_stats.buffer_overflows);
    try testing.expectEqual(@as(u64, 0), final_stats.alignment_violations);
    try testing.expectEqual(@as(u64, 0), final_stats.invalid_frees);
}

test "DebugAllocator configuration and feature control" {
    const allocator = testing.allocator;

    var debug_alloc = DebugAllocator.init(allocator);

    // Verify configuration can be modified
    debug_alloc.config.enable_guard_validation = false;
    debug_alloc.config.enable_poison_free = false;
    debug_alloc.config.enable_alignment_checks = true;

    const debug_allocator_instance = debug_alloc.allocator();

    // Test that allocator still functions with modified config
    const memory = try debug_allocator_instance.alloc(u8, 128);
    defer debug_allocator_instance.free(memory);

    // Verify basic functionality
    @memset(memory, 0xFF);
    try testing.expect(memory[0] == 0xFF);
    try testing.expect(memory[127] == 0xFF);

    const stats = debug_alloc.statistics();
    try testing.expectEqual(@as(u64, 1), stats.active_allocations);
}

test "DebugAllocator integration with arena pattern" {
    // Test the recommended pattern from docs/STYLE.md and docs/DEBUG_ALLOCATOR.md
    const allocator = testing.allocator;

    // Layer DebugAllocator over ArenaAllocator for enhanced debugging
    var debug_alloc = DebugAllocator.init(allocator);
    const debug_allocator_instance = debug_alloc.allocator();

    // Simulate complex subsystem allocation patterns
    var allocations = std.ArrayList([]u8).init(debug_allocator_instance);
    defer {
        for (allocations.items) |mem| {
            debug_allocator_instance.free(mem);
        }
        allocations.deinit();
    }

    // Allocate multiple blocks
    for (0..10) |i| {
        const size = (i + 1) * 16;
        const memory = try debug_allocator_instance.alloc(u8, size);
        try allocations.append(memory);
    }

    // Verify comprehensive tracking
    const stats = debug_alloc.statistics();
    try testing.expectEqual(@as(u64, 11), stats.active_allocations); // 10 + ArrayList internal
    try testing.expect(stats.current_bytes_allocated > 0);

    // Validate all allocations are intact
    try debug_alloc.validate_all_allocations();
}
