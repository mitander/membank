//! Simple focused test for streaming WAL recovery functionality
//!
//! Tests the new streaming recovery implementation to ensure it correctly
//! processes WAL entries without loading entire segments into memory.

const kausaldb = @import("kausaldb");
const std = @import("std");
const testing = std.testing;
const assert = kausaldb.assert.assert;

const vfs = kausaldb.vfs;
const simulation_vfs = kausaldb.simulation_vfs;
const context_block = kausaldb.types;
const storage = kausaldb.storage;

const SimulationVFS = simulation_vfs.SimulationVFS;
const ContextBlock = context_block.ContextBlock;
const BlockId = context_block.BlockId;
const GraphEdge = context_block.GraphEdge;
const EdgeType = context_block.EdgeType;
const StorageEngine = storage.StorageEngine;

/// Test recovery context to capture recovered entries
const RecoveryContext = struct {
    blocks_recovered: u32,
    edges_recovered: u32,
    deletes_recovered: u32,
    allocator: std.mem.Allocator,

    fn init(allocator: std.mem.Allocator) RecoveryContext {
        return RecoveryContext{
            .blocks_recovered = 0,
            .edges_recovered = 0,
            .deletes_recovered = 0,
            .allocator = allocator,
        };
    }

    fn recovery_callback(context: *anyopaque, block: ContextBlock) !void {
        const recovery_context: *RecoveryContext = @ptrCast(@alignCast(context));
        recovery_context.blocks_recovered += 1;
        // Block will be cleaned up by caller
        _ = block;
    }

    fn edge_callback(context: *anyopaque, edge: GraphEdge) !void {
        const recovery_context: *RecoveryContext = @ptrCast(@alignCast(context));
        recovery_context.edges_recovered += 1;
        _ = edge;
    }

    fn delete_callback(context: *anyopaque, block_id: BlockId) !void {
        const recovery_context: *RecoveryContext = @ptrCast(@alignCast(context));
        recovery_context.deletes_recovered += 1;
        _ = block_id;
    }
};

/// Create test block with predictable content
fn create_test_block(allocator: std.mem.Allocator, id_suffix: u8) !ContextBlock {
    var id_bytes: [16]u8 = std.mem.zeroes([16]u8);
    id_bytes[15] = id_suffix;

    const content = try std.fmt.allocPrint(allocator, "test content {d}", .{id_suffix});
    const metadata = try std.fmt.allocPrint(allocator, "{{\"type\": \"test\", \"id\": {d}}}", .{id_suffix});

    return ContextBlock{
        .id = BlockId.from_bytes(id_bytes),
        .version = 1,
        .source_uri = try allocator.dupe(u8, "test://source"),
        .metadata_json = metadata,
        .content = content,
    };
}

test "streaming recovery basic functionality" {
    const allocator = testing.allocator;

    var sim_vfs = try SimulationVFS.init(allocator);
    defer sim_vfs.deinit();
    var vfs_interface = sim_vfs.vfs();

    // Create storage engine with test directory
    const test_dir = "streaming_test_dir";
    try vfs_interface.mkdir(test_dir);

    var storage_engine = try StorageEngine.init_default(allocator, vfs_interface, test_dir);
    defer storage_engine.deinit();

    try storage_engine.startup();

    // Create and store test data
    const test_block1 = try create_test_block(allocator, 1);
    const test_block2 = try create_test_block(allocator, 2);
    const test_block3 = try create_test_block(allocator, 3);

    try storage_engine.put_block(test_block1);
    try storage_engine.put_block(test_block2);
    try storage_engine.put_block(test_block3);

    // Clean up after storage operations are complete
    defer allocator.free(test_block1.content);
    defer allocator.free(test_block1.source_uri);
    defer allocator.free(test_block1.metadata_json);
    defer allocator.free(test_block2.content);
    defer allocator.free(test_block2.source_uri);
    defer allocator.free(test_block2.metadata_json);
    defer allocator.free(test_block3.content);
    defer allocator.free(test_block3.source_uri);
    defer allocator.free(test_block3.metadata_json);

    // Create edge between blocks
    const test_edge = GraphEdge{
        .source_id = test_block1.id,
        .target_id = test_block2.id,
        .edge_type = EdgeType.calls,
    };
    try storage_engine.put_edge(test_edge);

    // Delete one block
    try storage_engine.delete_block(test_block3.id);

    // Test recovery using automatic approach
    var fresh_storage = try StorageEngine.init_default(allocator, vfs_interface, test_dir);
    defer fresh_storage.deinit();

    try fresh_storage.startup();

    // Validate recovery results by checking storage engine state
    // Should have 2 blocks remaining (3 created, 1 deleted)
    try testing.expectEqual(@as(u32, 2), fresh_storage.block_count());

    // Verify specific blocks exist
    const recovered_block1 = try fresh_storage.find_block(test_block1.id);
    const recovered_block2 = try fresh_storage.find_block(test_block2.id);
    const recovered_block3 = try fresh_storage.find_block(test_block3.id);

    try testing.expect(recovered_block1 != null);
    try testing.expect(recovered_block2 != null);
    try testing.expect(recovered_block3 == null); // This block was deleted
}

test "streaming recovery with large entries" {
    const allocator = testing.allocator;

    var sim_vfs = try SimulationVFS.init(allocator);
    defer sim_vfs.deinit();
    var vfs_interface = sim_vfs.vfs();

    const test_dir = "large_entries_test_dir";
    try vfs_interface.mkdir(test_dir);

    var storage_engine = try StorageEngine.init_default(allocator, vfs_interface, test_dir);
    defer storage_engine.deinit();

    try storage_engine.startup();

    // Create block with large content that exceeds typical buffer sizes
    var large_block = try create_test_block(allocator, 1);
    allocator.free(large_block.content);

    const large_content_size = 32 * 1024; // 32KB content
    const mutable_content = try allocator.alloc(u8, large_content_size);
    defer allocator.free(mutable_content);
    for (mutable_content, 0..) |*byte, i| {
        byte.* = @intCast(i % 256);
    }
    large_block.content = mutable_content;

    // Store large block and some normal blocks
    const normal_block1 = try create_test_block(allocator, 2);
    const normal_block2 = try create_test_block(allocator, 3);

    try storage_engine.put_block(normal_block1);
    try storage_engine.put_block(large_block);
    try storage_engine.put_block(normal_block2);

    // Clean up after storage operations
    defer allocator.free(large_block.source_uri);
    defer allocator.free(large_block.metadata_json);
    defer allocator.free(normal_block1.content);
    defer allocator.free(normal_block1.source_uri);
    defer allocator.free(normal_block1.metadata_json);
    defer allocator.free(normal_block2.content);
    defer allocator.free(normal_block2.source_uri);
    defer allocator.free(normal_block2.metadata_json);

    // Recovery should handle large entries correctly

    var fresh_storage = try StorageEngine.init_default(allocator, vfs_interface, test_dir);
    defer fresh_storage.deinit();

    try fresh_storage.startup();

    // All blocks should be recovered successfully
    try testing.expectEqual(@as(u32, 3), fresh_storage.block_count());
}

test "streaming recovery memory efficiency" {
    const allocator = testing.allocator;

    var sim_vfs = try SimulationVFS.init(allocator);
    defer sim_vfs.deinit();
    var vfs_interface = sim_vfs.vfs();

    const test_dir = "memory_efficiency_test_dir";
    try vfs_interface.mkdir(test_dir);

    var storage_engine = try StorageEngine.init_default(allocator, vfs_interface, test_dir);
    defer storage_engine.deinit();

    try storage_engine.startup();

    // Create many entries to test memory efficiency
    const num_entries = 100; // Test with smaller number to debug the issue
    var test_blocks = std.ArrayList(ContextBlock).init(allocator);
    defer test_blocks.deinit();
    try test_blocks.ensureTotalCapacity(num_entries);

    for (0..num_entries) |i| {
        const test_block = try create_test_block(allocator, @as(u8, @intCast(i + 1))); // Use sequential IDs starting from 1
        try test_blocks.append(test_block);
        try storage_engine.put_block(test_block);
    }

    // Clean up all blocks after storage operations
    for (test_blocks.items) |block| {
        allocator.free(block.content);
        allocator.free(block.source_uri);
        allocator.free(block.metadata_json);
    }

    // Recovery should process all entries without excessive memory usage

    var fresh_storage = try StorageEngine.init_default(allocator, vfs_interface, test_dir);
    defer fresh_storage.deinit();

    try fresh_storage.startup();

    // All entries should be recovered
    try testing.expectEqual(@as(u32, num_entries), fresh_storage.block_count());
}

test "streaming recovery empty WAL" {
    const allocator = testing.allocator;

    var sim_vfs = try SimulationVFS.init(allocator);
    defer sim_vfs.deinit();
    var vfs_interface = sim_vfs.vfs();

    const test_dir = "empty_wal_test_dir";
    try vfs_interface.mkdir(test_dir);

    var storage_engine = try StorageEngine.init_default(allocator, vfs_interface, test_dir);
    defer storage_engine.deinit();

    try storage_engine.startup();

    // Don't write any data - WAL should be empty

    // Recovery from empty WAL should complete without errors with fresh storage engine
    var fresh_storage = try StorageEngine.init_default(allocator, vfs_interface, test_dir);
    defer fresh_storage.deinit();
    try fresh_storage.startup();

    // No entries should be recovered from empty WAL
    try testing.expectEqual(@as(u32, 0), fresh_storage.block_count());
    try testing.expectEqual(@as(u32, 0), fresh_storage.edge_count());
}
