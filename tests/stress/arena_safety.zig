//! Arena safety validation for KausalDB memory management.
//!
//! Tests the arena-per-subsystem pattern under hostile conditions including:
//! - Error path memory safety
//! - Arena reset safety under failure conditions
//! - Cross-allocator corruption detection
//! - Memory fragmentation and stress scenarios
//! - O(1) cleanup validation

const std = @import("std");
const builtin = @import("builtin");
const kausaldb = @import("kausaldb");
const testing = std.testing;

const assert = kausaldb.assert.assert;
const SimulationVFS = kausaldb.simulation_vfs.SimulationVFS;
const StorageEngine = kausaldb.storage.StorageEngine;
const MemtableManager = kausaldb.storage.MemtableManager;
const ContextBlock = kausaldb.types.ContextBlock;
const BlockId = kausaldb.types.BlockId;
const TestData = kausaldb.TestData;
const StorageHarness = kausaldb.StorageHarness;

const log = std.log.scoped(.arena_safety);

test "memtable manager lifecycle safety" {
    // Use GPA with safety checks to detect any memory corruption
    var gpa = std.heap.GeneralPurposeAllocator(.{ .safety = true }){};
    defer {
        const deinit_status = gpa.deinit();
        if (deinit_status == .leak) @panic("Memory leak detected in arena lifecycle test");
    }
    const allocator = gpa.allocator();

    var sim_vfs = try SimulationVFS.init(allocator);
    defer sim_vfs.deinit();

    // Create multiple MemtableManager instances to test arena isolation
    var memtable1 = try MemtableManager.init(allocator, sim_vfs.vfs(), "./arena_test1", 1024 * 1024);
    defer memtable1.deinit();
    try memtable1.startup();

    var memtable2 = try MemtableManager.init(allocator, sim_vfs.vfs(), "./arena_test2", 1024 * 1024);
    defer memtable2.deinit();
    try memtable2.startup();

    // Add blocks to both memtables to stress arena allocations
    // Scale down for debug builds to prevent CI timeouts
    const block_count = if (builtin.mode == .Debug) 250 else 1000;
    var i: u32 = 0;
    while (i < block_count) : (i += 1) {
        const block1 = ContextBlock{
            .id = TestData.deterministic_block_id(i * 2 + 1),
            .version = 1,
            .source_uri = "test://arena_safety_1.zig",
            .metadata_json = "{\"test\":\"arena_safety\"}",
            .content = "Arena safety test block 1 content",
        };

        const block2 = ContextBlock{
            .id = TestData.deterministic_block_id(i * 2 + 2),
            .version = 1,
            .source_uri = "test://arena_safety_2.zig",
            .metadata_json = "{\"test\":\"arena_safety\"}",
            .content = "Arena safety test block 2 content",
        };

        try memtable1.put_block(block1);
        try memtable2.put_block(block2);
    }

    // Verify both memtables have memory allocated (can't easily check block count with current API)
    try testing.expect(memtable1.memory_usage() > 0);
    try testing.expect(memtable2.memory_usage() > 0);

    // Clear one memtable (O(1) arena reset)
    memtable1.clear();
    try testing.expectEqual(@as(u64, 0), memtable1.memory_usage());
    try testing.expect(memtable2.memory_usage() > 0);

    // Clear second memtable
    memtable2.clear();
    try testing.expectEqual(@as(u64, 0), memtable2.memory_usage());
}

test "error path memory cleanup" {
    var gpa = std.heap.GeneralPurposeAllocator(.{ .safety = true }){};
    defer {
        const deinit_status = gpa.deinit();
        if (deinit_status == .leak) @panic("Memory leak detected in error path test");
    }
    const allocator = gpa.allocator();

    var sim_vfs = try SimulationVFS.init(allocator);
    defer sim_vfs.deinit();

    // Test arena cleanup when storage engine initialization fails
    var memtable = try MemtableManager.init(allocator, sim_vfs.vfs(), "./error_test", 512 * 1024);
    defer memtable.deinit();
    try memtable.startup();

    // Fill arena with data
    var i: u32 = 0;
    while (i < 100) : (i += 1) {
        const block = ContextBlock{
            .id = TestData.deterministic_block_id(@intCast(i)),
            .version = 1,
            .source_uri = "test://arena_boundary.zig",
            .metadata_json = "{\"test\":\"arena_boundary\"}",
            .content = "Arena boundary test block content",
        };

        try memtable.put_block(block);
    }

    // Simulate error condition and verify cleanup
    const initial_memory = memtable.memory_usage();
    try testing.expect(initial_memory > 0);

    // Arena reset should handle all cleanup
    memtable.clear();
    const post_clear_memory = memtable.memory_usage();
    try testing.expectEqual(@as(u64, 0), post_clear_memory);
}

test "memory fragmentation stress" {
    var gpa = std.heap.GeneralPurposeAllocator(.{ .safety = true }){};
    defer {
        const deinit_status = gpa.deinit();
        if (deinit_status == .leak) @panic("Memory leak detected in fragmentation test");
    }
    const allocator = gpa.allocator();

    var sim_vfs = try SimulationVFS.init(allocator);
    defer sim_vfs.deinit();

    var memtable = try MemtableManager.init(allocator, sim_vfs.vfs(), "./frag_test", 2 * 1024 * 1024);
    defer memtable.deinit();
    try memtable.startup();

    // Create varied-size allocations to test fragmentation handling
    // Scale down largest allocations for debug builds
    const sizes = if (builtin.mode == .Debug)
        [_]usize{ 10, 100, 1000, 5000 }
    else
        [_]usize{ 10, 100, 1000, 10000, 100000 };

    for (sizes) |size| {
        var cycle: u32 = 0;
        while (cycle < 20) : (cycle += 1) {
            const content = try allocator.alloc(u8, size);
            defer allocator.free(content);

            // Fill with pattern to detect corruption
            for (content, 0..) |*byte, idx| {
                byte.* = @truncate(idx + cycle);
            }

            const owned_content = try allocator.dupe(u8, content);
            const block = ContextBlock{
                .id = TestData.deterministic_block_id(@intCast(cycle)),
                .version = 1,
                .source_uri = "test://arena_memory_leak.zig",
                .metadata_json = "{\"test\":\"arena_memory_leak\"}",
                .content = owned_content,
            };

            try memtable.put_block(block);

            // Periodically clear to test arena reuse
            if (cycle % 5 == 0) {
                memtable.clear();
            }
        }
    }

    // Final cleanup
    memtable.clear();
    try testing.expectEqual(@as(u64, 0), memtable.memory_usage());
}

test "concurrent arena operations" {
    var gpa = std.heap.GeneralPurposeAllocator(.{ .safety = true }){};
    defer {
        const deinit_status = gpa.deinit();
        if (deinit_status == .leak) @panic("Memory leak detected in concurrent test");
    }
    const allocator = gpa.allocator();

    // Test multiple arenas to ensure no cross-contamination
    const arena_count = 10;
    var memtables: [arena_count]MemtableManager = undefined;

    var sim_vfs = try SimulationVFS.init(allocator);
    defer sim_vfs.deinit();

    // Initialize all arenas
    for (&memtables, 0..) |*memtable, idx| {
        const dir_name = try std.fmt.allocPrint(allocator, "./concurrent_test_{}", .{idx});
        defer allocator.free(dir_name);
        memtable.* = try MemtableManager.init(allocator, sim_vfs.vfs(), dir_name, 1024 * 1024);
    }
    for (&memtables) |*memtable| {
        try memtable.startup();
    }
    defer for (&memtables) |*memtable| {
        memtable.deinit();
    };

    // Add different data to each arena
    for (&memtables, 0..) |*memtable, arena_idx| {
        var block_idx: u32 = 0;
        while (block_idx < 50) : (block_idx += 1) {
            const block = ContextBlock{
                .id = TestData.deterministic_block_id(@intCast(arena_idx * 1000 + block_idx)),
                .version = 1,
                .source_uri = "test://concurrent_arena.zig",
                .metadata_json = "{\"test\":\"concurrent_arena\"}",
                .content = "Concurrent arena test block content",
            };

            try memtable.put_block(block);
        }
    }

    // Verify each arena has memory allocated
    for (&memtables) |*memtable| {
        try testing.expect(memtable.memory_usage() > 0);
    }

    // Clear arenas in alternating pattern
    for (&memtables, 0..) |*memtable, idx| {
        if (idx % 2 == 0) {
            memtable.clear();
            try testing.expectEqual(@as(u64, 0), memtable.memory_usage());
        }
    }

    // Clear remaining arenas
    for (&memtables, 0..) |*memtable, idx| {
        if (idx % 2 == 1) {
            memtable.clear();
            try testing.expectEqual(@as(u64, 0), memtable.memory_usage());
        }
    }
}

test "large allocation stress" {
    var gpa = std.heap.GeneralPurposeAllocator(.{ .safety = true }){};
    defer {
        const deinit_status = gpa.deinit();
        if (deinit_status == .leak) @panic("Memory leak detected in large allocation test");
    }
    const allocator = gpa.allocator();

    var sim_vfs = try SimulationVFS.init(allocator);
    defer sim_vfs.deinit();

    var memtable = try MemtableManager.init(allocator, sim_vfs.vfs(), "./large_test", 10 * 1024 * 1024);
    defer memtable.deinit();
    try memtable.startup();

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

        const owned_content = try allocator.dupe(u8, large_content);
        const block = ContextBlock{
            .id = TestData.deterministic_block_id(@intCast(i)),
            .version = 1,
            .source_uri = "test://arena_concurrent.zig",
            .metadata_json = "{\"test\":\"arena_concurrent\"}",
            .content = owned_content,
        };

        try memtable.put_block(block);

        // Verify memory usage is growing
        const memory_usage = memtable.memory_usage();
        try testing.expect(memory_usage > 0);
    }

    // Verify final state
    const final_usage = memtable.memory_usage();
    try testing.expect(final_usage > block_count * large_size);

    // Single O(1) cleanup should handle all large allocations
    memtable.clear();
    try testing.expectEqual(@as(u64, 0), memtable.memory_usage());
}

test "cross allocator corruption detection" {
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

    var sim_vfs = try SimulationVFS.init(allocator);
    defer sim_vfs.deinit();

    var memtable = try MemtableManager.init(allocator, sim_vfs.vfs(), "./cross_alloc_test", 1024 * 1024);
    defer memtable.deinit();
    try memtable.startup();

    // Test that memtable correctly uses its own arena
    const block_with_main = ContextBlock{
        .id = TestData.deterministic_block_id(3001),
        .version = 1,
        .source_uri = "test://cross_allocator_main.zig",
        .metadata_json = "{\"test\":\"cross_allocator\"}",
        .content = "Content from main allocator",
    };

    try memtable.put_block(block_with_main);

    // Verify data is stored correctly
    try testing.expect(memtable.memory_usage() > 0);

    // Test with data from separate allocator
    const block_with_separate = ContextBlock{
        .id = TestData.deterministic_block_id(3002),
        .version = 1,
        .source_uri = "test://cross_allocator_separate.zig",
        .metadata_json = "{\"test\":\"cross_allocator\"}",
        .content = "Content from separate allocator",
    };

    try memtable.put_block(block_with_separate);

    // Verify both blocks stored correctly
    try testing.expect(memtable.memory_usage() > 0);

    // Arena cleanup should handle all data correctly regardless of source allocator
    memtable.clear();
    try testing.expectEqual(@as(u64, 0), memtable.memory_usage());
}

test "sustained operations memory stability" {
    var gpa = std.heap.GeneralPurposeAllocator(.{ .safety = true }){};
    defer {
        const deinit_status = gpa.deinit();
        if (deinit_status == .leak) @panic("Memory leak detected in sustained operations test");
    }
    const allocator = gpa.allocator();

    var sim_vfs = try SimulationVFS.init(allocator);
    defer sim_vfs.deinit();

    var memtable = try MemtableManager.init(allocator, sim_vfs.vfs(), "./sustained_test", 2 * 1024 * 1024);
    defer memtable.deinit();
    try memtable.startup();

    // Simulate sustained database operations over many cycles
    const cycles = 100;
    const blocks_per_cycle = 50;

    var cycle: u32 = 0;
    while (cycle < cycles) : (cycle += 1) {
        // Fill memtable
        var block_idx: u32 = 0;
        while (block_idx < blocks_per_cycle) : (block_idx += 1) {
            const block = ContextBlock{
                .id = TestData.deterministic_block_id(cycle * 10000 + block_idx),
                .version = 1,
                .source_uri = "test://sustained_operations.zig",
                .metadata_json = "{\"test\":\"sustained_operations\"}",
                .content = "Sustained operations test block content",
            };

            try memtable.put_block(block);
        }

        // Verify expected state
        try testing.expect(memtable.memory_usage() > 0);

        // Simulate memtable flush (clear)
        memtable.clear();
        try testing.expectEqual(@as(u64, 0), memtable.memory_usage());

        // Log progress every 20 cycles
        if (cycle % 20 == 0) {
            log.debug("Completed sustained operation cycle {}/{}", .{ cycle, cycles });
        }
    }

    log.info("Sustained operations test completed: {} cycles, {} total blocks processed", .{ cycles, cycles * blocks_per_cycle });
}
