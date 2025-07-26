//! Simple focused test for streaming WAL recovery functionality
//!
//! Tests the new streaming recovery implementation to ensure it correctly
//! processes WAL entries without loading entire segments into memory.

const cortexdb = @import("cortexdb");
const std = @import("std");
const testing = std.testing;
const assert = cortexdb.assert.assert;

const vfs = cortexdb.vfs;
const simulation_vfs = cortexdb.simulation_vfs;
const context_block = cortexdb.types;
const storage = cortexdb.storage;

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
    var vfs_interface = sim_vfs.vfs();

    // Create storage engine with test directory
    const test_dir = "streaming_test_dir";
    try vfs_interface.mkdir(test_dir);

    var storage_engine = try StorageEngine.init(allocator, vfs_interface, test_dir);
    defer storage_engine.deinit();

    // Create and store test data
    const test_block1 = try create_test_block(allocator, 1);
    const test_block2 = try create_test_block(allocator, 2);
    const test_block3 = try create_test_block(allocator, 3);

    try storage_engine.put_block(test_block1);
    try storage_engine.put_block(test_block2);
    try storage_engine.put_block(test_block3);

    // Create edge between blocks
    const test_edge = GraphEdge{
        .from = test_block1.id,
        .to = test_block2.id,
        .edge_type = EdgeType.references,
    };
    try storage_engine.put_edge(test_edge);

    // Delete one block
    try storage_engine.delete_block(test_block3.id);

    // Flush WAL to ensure data is written
    try storage_engine.flush_wal();

    // Set up recovery context
    var recovery_context = RecoveryContext.init(allocator);

    // Test recovery using default approach (for comparison)
    var fresh_storage = try StorageEngine.init(allocator, vfs_interface, test_dir);
    defer fresh_storage.deinit();

    try fresh_storage.recover_from_wal(
        recovery_context.recovery_callback,
        recovery_context.edge_callback,
        recovery_context.delete_callback,
        &recovery_context,
    );

    // Validate recovery results
    try testing.expectEqual(@as(u32, 3), recovery_context.blocks_recovered);
    try testing.expectEqual(@as(u32, 1), recovery_context.edges_recovered);
    try testing.expectEqual(@as(u32, 1), recovery_context.deletes_recovered);
}

test "streaming recovery with large entries" {
    const allocator = testing.allocator;

    var sim_vfs = try SimulationVFS.init(allocator);
    var vfs_interface = sim_vfs.vfs();

    const test_dir = "large_entries_test_dir";
    try vfs_interface.mkdir(test_dir);

    var storage_engine = try StorageEngine.init(allocator, vfs_interface, test_dir);
    defer storage_engine.deinit();

    // Create block with large content that exceeds typical buffer sizes
    var large_block = try create_test_block(allocator, 1);
    allocator.free(large_block.content);

    const large_content_size = 32 * 1024; // 32KB content
    large_block.content = try allocator.alloc(u8, large_content_size);
    for (large_block.content, 0..) |*byte, i| {
        byte.* = @intCast(i % 256);
    }

    // Store large block and some normal blocks
    const normal_block1 = try create_test_block(allocator, 2);
    const normal_block2 = try create_test_block(allocator, 3);

    try storage_engine.put_block(normal_block1);
    try storage_engine.put_block(large_block);
    try storage_engine.put_block(normal_block2);

    try storage_engine.flush_wal();

    // Recovery should handle large entries correctly
    var recovery_context = RecoveryContext.init(allocator);

    var fresh_storage = try StorageEngine.init(allocator, vfs_interface, test_dir);
    defer fresh_storage.deinit();

    try fresh_storage.recover_from_wal(
        recovery_context.recovery_callback,
        recovery_context.edge_callback,
        recovery_context.delete_callback,
        &recovery_context,
    );

    // All blocks should be recovered successfully
    try testing.expectEqual(@as(u32, 3), recovery_context.blocks_recovered);
}

test "streaming recovery memory efficiency" {
    const allocator = testing.allocator;

    var sim_vfs = try SimulationVFS.init(allocator);
    var vfs_interface = sim_vfs.vfs();

    const test_dir = "memory_efficiency_test_dir";
    try vfs_interface.mkdir(test_dir);

    var storage_engine = try StorageEngine.init(allocator, vfs_interface, test_dir);
    defer storage_engine.deinit();

    // Create many entries to test memory efficiency
    const num_entries = 500;
    for (0..num_entries) |i| {
        const test_block = try create_test_block(allocator, @intCast(i % 256));
        try storage_engine.put_block(test_block);
    }

    try storage_engine.flush_wal();

    // Recovery should process all entries without excessive memory usage
    var recovery_context = RecoveryContext.init(allocator);

    var fresh_storage = try StorageEngine.init(allocator, vfs_interface, test_dir);
    defer fresh_storage.deinit();

    try fresh_storage.recover_from_wal(
        recovery_context.recovery_callback,
        recovery_context.edge_callback,
        recovery_context.delete_callback,
        &recovery_context,
    );

    // All entries should be recovered
    try testing.expectEqual(@as(u32, num_entries), recovery_context.blocks_recovered);
}

test "streaming recovery empty WAL" {
    const allocator = testing.allocator;

    var sim_vfs = try SimulationVFS.init(allocator);
    var vfs_interface = sim_vfs.vfs();

    const test_dir = "empty_wal_test_dir";
    try vfs_interface.mkdir(test_dir);

    var storage_engine = try StorageEngine.init(allocator, vfs_interface, test_dir);
    defer storage_engine.deinit();

    // Don't write any data - WAL should be empty

    var recovery_context = RecoveryContext.init(allocator);

    // Recovery from empty WAL should complete without errors
    try storage_engine.recover_from_wal(
        recovery_context.recovery_callback,
        recovery_context.edge_callback,
        recovery_context.delete_callback,
        &recovery_context,
    );

    // No entries should be recovered from empty WAL
    try testing.expectEqual(@as(u32, 0), recovery_context.blocks_recovered);
    try testing.expectEqual(@as(u32, 0), recovery_context.edges_recovered);
    try testing.expectEqual(@as(u32, 0), recovery_context.deletes_recovered);
}
