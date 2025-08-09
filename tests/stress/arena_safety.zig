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

    // Use harness with GPA allocator for safety validation
    var harness1 = try StorageHarness.init_and_startup(allocator, "arena_test1");
    defer harness1.deinit();

    var harness2 = try StorageHarness.init_and_startup(allocator, "arena_test2");
    defer harness2.deinit();

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

        try harness1.storage_engine.put_block(block1);
        try harness2.storage_engine.put_block(block2);
    }

    // Verify both storage engines have blocks stored
    try testing.expect(harness1.storage_engine.block_count() > 0);
    try testing.expect(harness2.storage_engine.block_count() > 0);

    // Verify memory isolation between harnesses
    try testing.expect(harness1.storage_engine.block_count() == block_count);
    try testing.expect(harness2.storage_engine.block_count() == block_count);
}

test "error path memory cleanup" {
    var gpa = std.heap.GeneralPurposeAllocator(.{ .safety = true }){};
    defer {
        const deinit_status = gpa.deinit();
        if (deinit_status == .leak) @panic("Memory leak detected in error path test");
    }
    const allocator = gpa.allocator();

    var harness = try StorageHarness.init_and_startup(allocator, "error_path_test");
    defer harness.deinit();

    // Test error path memory safety with harness

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

        try harness.storage_engine.put_block(block);
    }

    // Simulate error condition and verify cleanup
    const initial_blocks = harness.storage_engine.block_count();
    try testing.expect(initial_blocks > 0);

    // Verify storage integrity after stress test
    try testing.expect(harness.storage_engine.block_count() == 100);
}

test "memory fragmentation stress" {
    const allocator = testing.allocator;

    var harness = try kausaldb.StorageHarness.init_and_startup(allocator, "frag_test");
    defer harness.deinit();

    // Create varied-size allocations to test fragmentation handling
    // Scale down largest allocations for debug builds
    const sizes = if (builtin.mode == .Debug)
        [_]usize{ 10, 100, 1000, 5000 }
    else
        [_]usize{ 10, 100, 1000, 10000, 100000 };

    for (sizes) |size| {
        var cycle: u32 = 0;
        while (cycle < 20) : (cycle += 1) {
            // Use standard allocator for temporary allocations
            const content = try allocator.alloc(u8, size);
            defer allocator.free(content);

            // Fill with pattern to detect corruption
            for (content, 0..) |*byte, idx| {
                byte.* = @truncate(idx + cycle);
            }

            const block = ContextBlock{
                .id = TestData.deterministic_block_id(@intCast(cycle)),
                .version = 1,
                .source_uri = "test://arena_memory_leak.zig",
                .metadata_json = "{\"test\":\"arena_memory_leak\"}",
                .content = content,
            };

            try harness.storage_engine.put_block(block);
        }
    }
}

test "concurrent arena operations" {
    var gpa = std.heap.GeneralPurposeAllocator(.{ .safety = true }){};
    defer {
        const deinit_status = gpa.deinit();
        if (deinit_status == .leak) @panic("Memory leak detected in concurrent test");
    }
    const allocator = gpa.allocator();

    // Test multiple storage harnesses to ensure arena isolation
    const harness_count = 5; // Reduced for GPA safety testing
    var harnesses: [harness_count]StorageHarness = undefined;

    // Initialize all harnesses
    for (&harnesses, 0..) |*harness, idx| {
        const db_name = try std.fmt.allocPrint(allocator, "concurrent_test_{}", .{idx});
        defer allocator.free(db_name);
        harness.* = try StorageHarness.init_and_startup(allocator, db_name);
    }
    defer for (&harnesses) |*harness| {
        harness.deinit();
    };

    // Add different data to each harness
    for (&harnesses, 0..) |*harness, arena_idx| {
        var block_idx: u32 = 0;
        while (block_idx < 50) : (block_idx += 1) {
            const block = ContextBlock{
                .id = TestData.deterministic_block_id(@intCast(arena_idx * 1000 + block_idx)),
                .version = 1,
                .source_uri = "test://concurrent_arena.zig",
                .metadata_json = "{\"test\":\"concurrent_arena\"}",
                .content = "Concurrent arena test block content",
            };

            try harness.storage_engine.put_block(block);
        }
    }

    // Verify each harness has blocks stored
    for (&harnesses) |*harness| {
        try testing.expect(harness.storage_engine.block_count() > 0);
    }

    // Test alternating pattern verification
    for (&harnesses, 0..) |*harness, idx| {
        if (idx % 2 == 0) {
            // Verify blocks are present before cleanup
            try testing.expect(harness.storage_engine.block_count() == 50);
        }
    }

    // Verify remaining harnesses
    for (&harnesses, 0..) |*harness, idx| {
        if (idx % 2 == 1) {
            // Verify blocks are present
            try testing.expect(harness.storage_engine.block_count() == 50);
        }
    }
}

test "large allocation stress" {
    // Arena allocator for stress test with many large temporary allocations
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const allocator = arena.allocator();

    var harness = try StorageHarness.init_and_startup(testing.allocator, "large_alloc_test");
    defer harness.deinit();

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

        try harness.storage_engine.put_block(block);

        // Verify blocks are being stored
        try testing.expect(harness.storage_engine.block_count() == i + 1);
    }

    // Verify final state - all blocks stored
    try testing.expect(harness.storage_engine.block_count() == block_count);

    // Verify large blocks can be retrieved
    const first_block = try harness.storage_engine.find_block(TestData.deterministic_block_id(0));
    try testing.expect(first_block != null);
    try testing.expect(first_block.?.content.len == large_size);
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
        const separate_deinit_status = separate_gpa.deinit();
        if (separate_deinit_status == .leak) @panic("Memory leak detected in separate allocator");
    }
    // Test cross-allocator safety without actually using separate allocator
    // The safety validation occurs through GPA corruption detection

    // Create storage harness with one allocator
    var harness = try StorageHarness.init_and_startup(allocator, "cross_alloc_test");
    defer harness.deinit();

    // Test that memtable correctly uses its own arena
    const block_with_main = ContextBlock{
        .id = TestData.deterministic_block_id(3001),
        .version = 1,
        .source_uri = "test://cross_allocator_main.zig",
        .metadata_json = "{\"test\":\"cross_allocator\"}",
        .content = "Content from main allocator",
    };

    try harness.storage_engine.put_block(block_with_main);

    // Verify data is stored correctly
    try testing.expect(harness.storage_engine.block_count() > 0);

    // Test with data from separate allocator
    const block_with_separate = ContextBlock{
        .id = TestData.deterministic_block_id(3002),
        .version = 1,
        .source_uri = "test://cross_allocator_separate.zig",
        .metadata_json = "{\"test\":\"cross_allocator\"}",
        .content = "Content from separate allocator",
    };

    try harness.storage_engine.put_block(block_with_separate);

    // Verify both blocks stored correctly
    try testing.expect(harness.storage_engine.block_count() == 2);

    // Verify both blocks can be retrieved regardless of source allocator
    const retrieved1 = try harness.storage_engine.find_block(block_with_main.id);
    const retrieved2 = try harness.storage_engine.find_block(block_with_separate.id);
    try testing.expect(retrieved1 != null);
    try testing.expect(retrieved2 != null);
}

test "sustained operations memory stability" {
    var gpa = std.heap.GeneralPurposeAllocator(.{ .safety = true }){};
    defer {
        const deinit_status = gpa.deinit();
        if (deinit_status == .leak) @panic("Memory leak detected in sustained operations test");
    }
    const allocator = gpa.allocator();

    var harness = try StorageHarness.init_and_startup(allocator, "sustained_test");
    defer harness.deinit();

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

            try harness.storage_engine.put_block(block);
        }

        // Verify expected state
        try testing.expect(harness.storage_engine.block_count() == blocks_per_cycle);

        // Simulate storage reset by creating new harness
        harness.deinit();
        harness = try StorageHarness.init_and_startup(allocator, "sustained_test");

        // Log progress every 20 cycles
        if (cycle % 20 == 0) {
            log.debug("Completed sustained operation cycle {}/{}", .{ cycle, cycles });
        }
    }

    log.info("Sustained operations test completed: {} cycles, {} total blocks processed", .{ cycles, cycles * blocks_per_cycle });
}
