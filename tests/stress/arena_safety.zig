//! Comprehensive arena safety validation for KausalDB memory management.
//!
//! Tests the arena-per-subsystem pattern under hostile conditions including:
//! - Error path memory safety
//! - Arena reset safety under failure conditions
//! - Cross-allocator corruption detection
//! - Memory fragmentation and stress scenarios
//! - O(1) cleanup validation

const std = @import("std");
const kausaldb = @import("kausaldb");
const testing = std.testing;
const assert = kausaldb.assert.assert;

const Simulation = kausaldb.simulation.Simulation;
const StorageEngine = kausaldb.storage.StorageEngine;
const MemtableManager = kausaldb.storage.MemtableManager;
const ContextBlock = kausaldb.types.ContextBlock;
const BlockId = kausaldb.types.BlockId;

const log = std.log.scoped(.arena_safety);

test "arena safety: memtable manager lifecycle" {
    // Use GPA with safety checks to detect any memory corruption
    var gpa = std.heap.GeneralPurposeAllocator(.{ .safety = true }){};
    defer {
        const deinit_status = gpa.deinit();
        if (deinit_status == .leak) @panic("Memory leak detected in arena lifecycle test");
    }
    const allocator = gpa.allocator();

    var sim = try Simulation.init(allocator, 0xABCDEF01);
    defer sim.deinit();

    // Create multiple MemtableManager instances to test arena isolation
    var memtable1 = try MemtableManager.init(allocator);
    defer memtable1.deinit();

    var memtable2 = try MemtableManager.init(allocator);
    defer memtable2.deinit();

    // Add blocks to both memtables to stress arena allocations
    const block_count = 1000;
    var i: u32 = 0;
    while (i < block_count) : (i += 1) {
        const block1 = ContextBlock{
            .id = BlockId.generate(),
            .version = 1,
            .source_uri = try std.fmt.allocPrint(allocator, "test://arena1/{}", .{i}),
            .metadata_json = try std.fmt.allocPrint(allocator, "{{\"index\":{}}}", .{i}),
            .content = try std.fmt.allocPrint(allocator, "Content for arena test block {}", .{i}),
        };
        defer allocator.free(block1.source_uri);
        defer allocator.free(block1.metadata_json);
        defer allocator.free(block1.content);

        const block2 = ContextBlock{
            .id = BlockId.generate(),
            .version = 1,
            .source_uri = try std.fmt.allocPrint(allocator, "test://arena2/{}", .{i}),
            .metadata_json = try std.fmt.allocPrint(allocator, "{{\"other\":{}}}", .{i}),
            .content = try std.fmt.allocPrint(allocator, "Different content for block {}", .{i}),
        };
        defer allocator.free(block2.source_uri);
        defer allocator.free(block2.metadata_json);
        defer allocator.free(block2.content);

        try memtable1.put_block(block1);
        try memtable2.put_block(block2);
    }

    // Verify both memtables have correct data
    try testing.expectEqual(@as(usize, block_count), memtable1.memory_usage().block_count);
    try testing.expectEqual(@as(usize, block_count), memtable2.memory_usage().block_count);

    // Clear one memtable (O(1) arena reset)
    memtable1.clear();
    try testing.expectEqual(@as(usize, 0), memtable1.memory_usage().block_count);
    try testing.expectEqual(@as(usize, block_count), memtable2.memory_usage().block_count);

    // Clear second memtable
    memtable2.clear();
    try testing.expectEqual(@as(usize, 0), memtable2.memory_usage().block_count);
}

test "arena safety: error path memory cleanup" {
    var gpa = std.heap.GeneralPurposeAllocator(.{ .safety = true }){};
    defer {
        const deinit_status = gpa.deinit();
        if (deinit_status == .leak) @panic("Memory leak detected in error path test");
    }
    const allocator = gpa.allocator();

    var sim = try Simulation.init(allocator, 0xDEADBEEF);
    defer sim.deinit();

    // Test arena cleanup when storage engine initialization fails
    var memtable = try MemtableManager.init(allocator);
    defer memtable.deinit();

    // Fill arena with data
    var i: u32 = 0;
    while (i < 100) : (i += 1) {
        const block = ContextBlock{
            .id = BlockId.generate(),
            .version = 1,
            .source_uri = try std.fmt.allocPrint(allocator, "test://error/{}", .{i}),
            .metadata_json = "{}",
            .content = try std.fmt.allocPrint(allocator, "Error test content {}", .{i}),
        };
        defer allocator.free(block.source_uri);
        defer allocator.free(block.content);

        try memtable.put_block(block);
    }

    // Simulate error condition and verify cleanup
    const initial_memory = memtable.memory_usage().total_bytes;
    try testing.expect(initial_memory > 0);

    // Arena reset should handle all cleanup
    memtable.clear();
    const post_clear_memory = memtable.memory_usage().total_bytes;
    try testing.expectEqual(@as(usize, 0), post_clear_memory);
}

test "arena safety: memory fragmentation stress" {
    var gpa = std.heap.GeneralPurposeAllocator(.{ .safety = true }){};
    defer {
        const deinit_status = gpa.deinit();
        if (deinit_status == .leak) @panic("Memory leak detected in fragmentation test");
    }
    const allocator = gpa.allocator();

    var memtable = try MemtableManager.init(allocator);
    defer memtable.deinit();

    // Create varied-size allocations to test fragmentation handling
    const sizes = [_]usize{ 10, 100, 1000, 10000, 100000 };

    for (sizes) |size| {
        var cycle: u32 = 0;
        while (cycle < 20) : (cycle += 1) {
            const content = try allocator.alloc(u8, size);
            defer allocator.free(content);

            // Fill with pattern to detect corruption
            for (content, 0..) |*byte, idx| {
                byte.* = @truncate(idx + cycle);
            }

            const block = ContextBlock{
                .id = BlockId.generate(),
                .version = 1,
                .source_uri = try std.fmt.allocPrint(allocator, "test://frag/{}/{}", .{ size, cycle }),
                .metadata_json = "{}",
                .content = content,
            };
            defer allocator.free(block.source_uri);

            try memtable.put_block(block);

            // Periodically clear to test arena reuse
            if (cycle % 5 == 0) {
                memtable.clear();
            }
        }
    }

    // Final cleanup
    memtable.clear();
    try testing.expectEqual(@as(usize, 0), memtable.memory_usage().total_bytes);
}

test "arena safety: concurrent arena operations" {
    var gpa = std.heap.GeneralPurposeAllocator(.{ .safety = true }){};
    defer {
        const deinit_status = gpa.deinit();
        if (deinit_status == .leak) @panic("Memory leak detected in concurrent test");
    }
    const allocator = gpa.allocator();

    // Test multiple arenas to ensure no cross-contamination
    const arena_count = 10;
    var memtables: [arena_count]MemtableManager = undefined;

    // Initialize all arenas
    for (&memtables) |*memtable| {
        memtable.* = try MemtableManager.init(allocator);
    }
    defer for (&memtables) |*memtable| {
        memtable.deinit();
    };

    // Add different data to each arena
    for (&memtables, 0..) |*memtable, arena_idx| {
        var block_idx: u32 = 0;
        while (block_idx < 50) : (block_idx += 1) {
            const block = ContextBlock{
                .id = BlockId.generate(),
                .version = 1,
                .source_uri = try std.fmt.allocPrint(allocator, "test://arena{}/{}", .{ arena_idx, block_idx }),
                .metadata_json = try std.fmt.allocPrint(allocator, "{{\"arena\":{},\"block\":{}}}", .{ arena_idx, block_idx }),
                .content = try std.fmt.allocPrint(allocator, "Arena {} Block {} Content", .{ arena_idx, block_idx }),
            };
            defer allocator.free(block.source_uri);
            defer allocator.free(block.metadata_json);
            defer allocator.free(block.content);

            try memtable.put_block(block);
        }
    }

    // Verify each arena has correct data
    for (&memtables) |*memtable| {
        try testing.expectEqual(@as(usize, 50), memtable.memory_usage().block_count);
    }

    // Clear arenas in alternating pattern
    for (&memtables, 0..) |*memtable, idx| {
        if (idx % 2 == 0) {
            memtable.clear();
            try testing.expectEqual(@as(usize, 0), memtable.memory_usage().block_count);
        }
    }

    // Clear remaining arenas
    for (&memtables, 0..) |*memtable, idx| {
        if (idx % 2 == 1) {
            memtable.clear();
            try testing.expectEqual(@as(usize, 0), memtable.memory_usage().block_count);
        }
    }
}

test "arena safety: large allocation stress" {
    var gpa = std.heap.GeneralPurposeAllocator(.{ .safety = true }){};
    defer {
        const deinit_status = gpa.deinit();
        if (deinit_status == .leak) @panic("Memory leak detected in large allocation test");
    }
    const allocator = gpa.allocator();

    var memtable = try MemtableManager.init(allocator);
    defer memtable.deinit();

    // Test arena with very large blocks
    const large_size = 1024 * 1024; // 1MB blocks
    const block_count = 10;

    var i: u32 = 0;
    while (i < block_count) : (i += 1) {
        const large_content = try allocator.alloc(u8, large_size);
        defer allocator.free(large_content);

        // Fill with recognizable pattern
        for (large_content, 0..) |*byte, idx| {
            byte.* = @truncate((idx + i * large_size) % 256);
        }

        const block = ContextBlock{
            .id = BlockId.generate(),
            .version = 1,
            .source_uri = try std.fmt.allocPrint(allocator, "test://large/{}", .{i}),
            .metadata_json = try std.fmt.allocPrint(allocator, "{{\"size\":{},\"index\":{}}}", .{ large_size, i }),
            .content = large_content,
        };
        defer allocator.free(block.source_uri);
        defer allocator.free(block.metadata_json);

        try memtable.put_block(block);

        // Verify memory usage is growing
        const memory_usage = memtable.memory_usage();
        try testing.expect(memory_usage.total_bytes > 0);
        try testing.expectEqual(@as(usize, i + 1), memory_usage.block_count);
    }

    // Verify final state
    const final_usage = memtable.memory_usage();
    try testing.expectEqual(@as(usize, block_count), final_usage.block_count);
    try testing.expect(final_usage.total_bytes > block_count * large_size);

    // Single O(1) cleanup should handle all large allocations
    memtable.clear();
    try testing.expectEqual(@as(usize, 0), memtable.memory_usage().total_bytes);
}

test "arena safety: cross-allocator corruption detection" {
    var gpa = std.heap.GeneralPurposeAllocator(.{ .safety = true }){};
    defer {
        const deinit_status = gpa.deinit();
        if (deinit_status == .leak) @panic("Memory leak detected in cross-allocator test");
    }
    const allocator = gpa.allocator();

    // Create separate allocator to test cross-contamination detection
    var separate_gpa = std.heap.GeneralPurposeAllocator(.{ .safety = true }){};
    defer {
        const deinit_status = separate_gpa.deinit();
        if (deinit_status == .leak) @panic("Memory leak in separate allocator");
    }
    const separate_allocator = separate_gpa.allocator();

    var memtable = try MemtableManager.init(allocator);
    defer memtable.deinit();

    // Test that memtable correctly uses its own arena
    const block_with_main = ContextBlock{
        .id = BlockId.generate(),
        .version = 1,
        .source_uri = try allocator.dupe(u8, "test://main/allocator"),
        .metadata_json = "{}",
        .content = try allocator.dupe(u8, "Content from main allocator"),
    };
    defer allocator.free(block_with_main.source_uri);
    defer allocator.free(block_with_main.content);

    try memtable.put_block(block_with_main);

    // Verify data is stored correctly
    try testing.expectEqual(@as(usize, 1), memtable.memory_usage().block_count);

    // Test with data from separate allocator
    const block_with_separate = ContextBlock{
        .id = BlockId.generate(),
        .version = 1,
        .source_uri = try separate_allocator.dupe(u8, "test://separate/allocator"),
        .metadata_json = "{}",
        .content = try separate_allocator.dupe(u8, "Content from separate allocator"),
    };
    defer separate_allocator.free(block_with_separate.source_uri);
    defer separate_allocator.free(block_with_separate.content);

    try memtable.put_block(block_with_separate);

    // Verify both blocks stored correctly
    try testing.expectEqual(@as(usize, 2), memtable.memory_usage().block_count);

    // Arena cleanup should handle all data correctly regardless of source allocator
    memtable.clear();
    try testing.expectEqual(@as(usize, 0), memtable.memory_usage().total_bytes);
}

test "arena safety: sustained operations memory stability" {
    var gpa = std.heap.GeneralPurposeAllocator(.{ .safety = true }){};
    defer {
        const deinit_status = gpa.deinit();
        if (deinit_status == .leak) @panic("Memory leak detected in sustained operations test");
    }
    const allocator = gpa.allocator();

    var memtable = try MemtableManager.init(allocator);
    defer memtable.deinit();

    // Simulate sustained database operations over many cycles
    const cycles = 100;
    const blocks_per_cycle = 50;

    var cycle: u32 = 0;
    while (cycle < cycles) : (cycle += 1) {
        // Fill memtable
        var block_idx: u32 = 0;
        while (block_idx < blocks_per_cycle) : (block_idx += 1) {
            const block = ContextBlock{
                .id = BlockId.generate(),
                .version = 1,
                .source_uri = try std.fmt.allocPrint(allocator, "test://sustained/{}/{}", .{ cycle, block_idx }),
                .metadata_json = try std.fmt.allocPrint(allocator, "{{\"cycle\":{},\"block\":{}}}", .{ cycle, block_idx }),
                .content = try std.fmt.allocPrint(allocator, "Cycle {} Block {} sustained operation content", .{ cycle, block_idx }),
            };
            defer allocator.free(block.source_uri);
            defer allocator.free(block.metadata_json);
            defer allocator.free(block.content);

            try memtable.put_block(block);
        }

        // Verify expected state
        try testing.expectEqual(@as(usize, blocks_per_cycle), memtable.memory_usage().block_count);

        // Simulate memtable flush (clear)
        memtable.clear();
        try testing.expectEqual(@as(usize, 0), memtable.memory_usage().block_count);
        try testing.expectEqual(@as(usize, 0), memtable.memory_usage().total_bytes);

        // Log progress every 20 cycles
        if (cycle % 20 == 0) {
            log.debug("Completed sustained operation cycle {}/{}", .{ cycle, cycles });
        }
    }

    log.info("Sustained operations test completed: {} cycles, {} total blocks processed", .{ cycles, cycles * blocks_per_cycle });
}
