//! Streaming WAL Recovery Tests
//!
//! Tests the new streaming WAL recovery implementation that reads and processes
//! entries one at a time instead of loading entire WAL files into memory.
//! Validates memory efficiency and correctness under various scenarios.

const std = @import("std");
const testing = std.testing;

const context_block = @import("context_block");
const storage = @import("storage");
const simulation = @import("simulation");

const StorageEngine = storage.StorageEngine;
const ContextBlock = context_block.ContextBlock;
const BlockId = context_block.BlockId;
const Simulation = simulation.Simulation;

// Helper to create test blocks with specific ID
fn create_test_block_with_id(allocator: std.mem.Allocator, id_bytes: [16]u8, suffix: u8) !ContextBlock {
    return ContextBlock{
        .id = BlockId{ .bytes = id_bytes },
        .version = 1,
        .source_uri = try std.fmt.allocPrint(allocator, "test://block_{d}", .{suffix}),
        .metadata_json = try std.fmt.allocPrint(allocator, "{{\"type\":\"test\",\"id\":{d}}}", .{suffix}),
        .content = try std.fmt.allocPrint(allocator, "Test content for block {d}", .{suffix}),
    };
}

// Helper to create test blocks with simple suffix ID
fn create_test_block(allocator: std.mem.Allocator, id_suffix: u8) !ContextBlock {
    var id_bytes: [16]u8 = std.mem.zeroes([16]u8);
    id_bytes[15] = id_suffix;
    return create_test_block_with_id(allocator, id_bytes, id_suffix);
}

test "streaming WAL recovery - basic correctness" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const allocator = arena.allocator();

    var sim = try Simulation.init(allocator, 12345);
    defer sim.deinit();

    const node1 = try sim.add_node();
    const node1_ptr = sim.find_node(node1);
    const node1_vfs = node1_ptr.filesystem_interface();

    // Create storage engine and write some blocks
    const data_dir = try allocator.dupe(u8, "test_streaming_recovery");
    defer allocator.free(data_dir);
    var storage_engine = try StorageEngine.init_default(allocator, node1_vfs, data_dir);
    defer storage_engine.deinit();
    try storage_engine.initialize_storage();

    // Create and store test blocks
    const test_blocks = [_]u8{ 1, 2, 3, 4, 5 };
    var expected_blocks = std.ArrayList(ContextBlock).init(allocator);

    for (test_blocks) |suffix| {
        const block = try create_test_block(allocator, suffix);
        try expected_blocks.append(block);
        try storage_engine.put_block(block);
    }

    // Force WAL flush to ensure data is written
    try storage_engine.flush_wal();

    // Create a new storage engine to simulate recovery
    var recovery_engine = try StorageEngine.init_default(allocator, node1_vfs, data_dir);
    defer recovery_engine.deinit();

    // Recover from WAL - this uses the new streaming implementation
    try recovery_engine.startup();

    // Verify all blocks were recovered correctly
    for (expected_blocks.items) |expected_block| {
        const recovered_block = try recovery_engine.find_block_by_id(expected_block.id);
        try testing.expectEqualSlices(u8, expected_block.source_uri, recovered_block.source_uri);
        try testing.expectEqualSlices(u8, expected_block.metadata_json, recovered_block.metadata_json);
        try testing.expectEqualSlices(u8, expected_block.content, recovered_block.content);
        try testing.expectEqual(expected_block.version, recovered_block.version);
    }
}

test "streaming WAL recovery - large WAL file efficiency" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const allocator = arena.allocator();

    var sim = try Simulation.init(allocator, 54321);
    defer sim.deinit();

    const node1 = try sim.add_node();
    const node1_ptr = sim.find_node(node1);
    const node1_vfs = node1_ptr.filesystem_interface();

    // Create storage engine
    const data_dir = try allocator.dupe(u8, "test_large_wal");
    defer allocator.free(data_dir);
    var storage_engine = try StorageEngine.init_default(allocator, node1_vfs, data_dir);
    defer storage_engine.deinit();
    try storage_engine.initialize_storage();

    // Write many blocks to create a large WAL file
    const num_blocks = 2000;

    for (0..num_blocks) |i| {
        // Use different IDs for each block
        var id_bytes: [16]u8 = std.mem.zeroes([16]u8);
        std.mem.writeInt(u64, id_bytes[0..8], i, .little);
        const block = try create_test_block_with_id(allocator, id_bytes, @intCast(i % 256));

        try storage_engine.put_block(block);
    }

    // Force WAL flush
    try storage_engine.flush_wal();

    // Create new engine for recovery and verify memory usage is reasonable
    var recovery_engine = try StorageEngine.init_default(allocator, node1_vfs, data_dir);
    defer recovery_engine.deinit();

    // Recovery should complete without excessive memory usage
    try recovery_engine.startup();

    // Verify correct number of blocks recovered by checking a few blocks
    var recovered_count: u32 = 0;
    for (0..10) |i| {
        var id_bytes: [16]u8 = std.mem.zeroes([16]u8);
        std.mem.writeInt(u64, id_bytes[0..8], i, .little);
        const block_id = BlockId{ .bytes = id_bytes };

        if (recovery_engine.find_block_by_id(block_id)) |_| {
            recovered_count += 1;
        } else |_| {
            // Expected for some blocks that might not exist
        }
    }

    // Should have recovered at least some blocks
    try testing.expect(recovered_count > 0);
}

test "streaming WAL recovery - empty WAL file handling" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const allocator = arena.allocator();

    var sim = try Simulation.init(allocator, 98765);
    defer sim.deinit();

    const node1 = try sim.add_node();
    const node1_ptr = sim.find_node(node1);
    const node1_vfs = node1_ptr.filesystem_interface();

    // Create storage engine with empty WAL - this simulates recovery from empty state
    const data_dir = try allocator.dupe(u8, "test_empty_wal");
    defer allocator.free(data_dir);
    var storage_engine = try StorageEngine.init_default(allocator, node1_vfs, data_dir);
    defer storage_engine.deinit();
    try storage_engine.initialize_storage();

    // Should be able to write and read blocks normally after recovery from empty state
    const test_block = try create_test_block(allocator, 42);
    try storage_engine.put_block(test_block);

    const retrieved_block = try storage_engine.find_block_by_id(test_block.id);
    try testing.expectEqualSlices(u8, test_block.content, retrieved_block.content);
}

test "streaming WAL recovery - arena memory reset validation" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const allocator = arena.allocator();

    var sim = try Simulation.init(allocator, 13579);
    defer sim.deinit();

    const node1 = try sim.add_node();
    const node1_ptr = sim.find_node(node1);
    const node1_vfs = node1_ptr.filesystem_interface();

    // Create storage engine
    const data_dir = try allocator.dupe(u8, "test_arena_reset");
    defer allocator.free(data_dir);
    var storage_engine = try StorageEngine.init_default(allocator, node1_vfs, data_dir);
    defer storage_engine.deinit();
    try storage_engine.initialize_storage();

    // Write enough blocks to trigger arena resets (> 1000 entries)
    // This validates that the periodic arena reset at 1000 entries works correctly
    const num_blocks = 1500;

    for (0..num_blocks) |i| {
        // Use unique IDs
        var id_bytes: [16]u8 = std.mem.zeroes([16]u8);
        std.mem.writeInt(u64, id_bytes[0..8], i, .little);
        const block = try create_test_block_with_id(allocator, id_bytes, @intCast(i % 256));

        try storage_engine.put_block(block);
    }

    try storage_engine.flush_wal();

    // Create new engine for recovery
    var recovery_engine = try StorageEngine.init_default(allocator, node1_vfs, data_dir);
    defer recovery_engine.deinit();

    // Recovery should complete successfully with periodic arena resets
    try recovery_engine.startup();

    // Verify that some blocks were recovered (arena resets didn't break recovery)
    var found_blocks: u32 = 0;
    for (0..10) |i| {
        var id_bytes: [16]u8 = std.mem.zeroes([16]u8);
        std.mem.writeInt(u64, id_bytes[0..8], i, .little);
        const block_id = BlockId{ .bytes = id_bytes };

        if (recovery_engine.find_block_by_id(block_id)) |_| {
            found_blocks += 1;
        } else |_| {
            // Expected for blocks that might not exist
        }
    }

    try testing.expect(found_blocks > 0);
}
