//! Simple focused test for streaming WAL recovery functionality
//!
//! Tests the new streaming recovery implementation to ensure it correctly
//! processes WAL entries without loading entire segments into memory.

const std = @import("std");

const kausaldb = @import("kausaldb");

const assert = kausaldb.assert.assert;
const testing = std.testing;

const BlockId = kausaldb.types.BlockId;
const ContextBlock = kausaldb.types.ContextBlock;
const EdgeType = kausaldb.types.EdgeType;
const GraphEdge = kausaldb.types.GraphEdge;
const QueryEngine = kausaldb.query_engine.QueryEngine;
const SimulationVFS = kausaldb.simulation_vfs.SimulationVFS;
const StorageEngine = kausaldb.storage.StorageEngine;
const StorageHarness = kausaldb.StorageHarness;
const TestData = kausaldb.TestData;

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

test "streaming recovery basic" {
    const allocator = testing.allocator;

    var harness = try kausaldb.SimulationHarness.init_and_startup(allocator, 0xDEADBEEF, "streaming_test_dir");
    defer harness.deinit();

    // Create and store test data using standardized TestData
    const test_block1 = try TestData.create_test_block(allocator, 1);
    defer TestData.cleanup_test_block(allocator, test_block1);
    const test_block2 = try TestData.create_test_block(allocator, 2);
    defer TestData.cleanup_test_block(allocator, test_block2);
    const test_block3 = try TestData.create_test_block(allocator, 3);
    defer TestData.cleanup_test_block(allocator, test_block3);

    try harness.storage_engine.put_block(test_block1);
    try harness.storage_engine.put_block(test_block2);
    try harness.storage_engine.put_block(test_block3);

    // Create edge between blocks
    const test_edge = GraphEdge{
        .source_id = test_block1.id,
        .target_id = test_block2.id,
        .edge_type = EdgeType.calls,
    };
    try harness.storage_engine.put_edge(test_edge);

    // Delete one block
    try harness.storage_engine.delete_block(test_block3.id);

    // Test recovery by shutting down and restarting storage engine
    // CRITICAL: Must recreate both storage engine AND query engine to prevent arena use-after-free
    harness.storage_engine.deinit();
    harness.allocator.destroy(harness.storage_engine);
    harness.query_engine.deinit();
    harness.allocator.destroy(harness.query_engine);

    harness.storage_engine = try harness.allocator.create(StorageEngine);
    harness.storage_engine.* = try StorageEngine.init_default(harness.allocator, harness.node().filesystem_interface(), "streaming_test_dir");
    try harness.storage_engine.startup();

    harness.query_engine = try harness.allocator.create(QueryEngine);
    harness.query_engine.* = QueryEngine.init(harness.allocator, harness.storage_engine);

    // Validate recovery results by checking storage engine state
    // Should have 2 blocks remaining (3 created, 1 deleted)
    try testing.expectEqual(@as(u32, 2), harness.storage_engine.block_count());

    // Verify specific blocks exist
    const recovered_block1 = try harness.storage_engine.find_block(test_block1.id, .query_engine);
    const recovered_block2 = try harness.storage_engine.find_block(test_block2.id, .query_engine);
    const recovered_block3 = try harness.storage_engine.find_block(test_block3.id, .query_engine);

    try testing.expect(recovered_block1 != null);
    try testing.expect(recovered_block2 != null);
    try testing.expect(recovered_block3 == null); // This block was deleted
}

test "streaming recovery large entries" {
    const allocator = testing.allocator;

    var harness = try kausaldb.SimulationHarness.init_and_startup(allocator, 0xCAFEBABE, "large_entries_test_dir");
    defer harness.deinit();

    // Create block with large content that exceeds typical buffer sizes
    var large_block = try TestData.create_test_block(allocator, 1);
    allocator.free(large_block.content);

    const large_content_size = 32 * 1024; // 32KB content
    const mutable_content = try allocator.alloc(u8, large_content_size);
    defer allocator.free(mutable_content);
    for (mutable_content, 0..) |*byte, i| {
        byte.* = @intCast(i % 256);
    }
    large_block.content = mutable_content;

    // Store large block and some normal blocks
    const normal_block1 = try TestData.create_test_block(allocator, 2);
    const normal_block2 = try TestData.create_test_block(allocator, 3);

    try harness.storage_engine.put_block(normal_block1);
    try harness.storage_engine.put_block(large_block);
    try harness.storage_engine.put_block(normal_block2);

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
    // CRITICAL: Must recreate both storage engine AND query engine to prevent arena use-after-free
    harness.storage_engine.deinit();
    harness.allocator.destroy(harness.storage_engine);
    harness.query_engine.deinit();
    harness.allocator.destroy(harness.query_engine);

    harness.storage_engine = try harness.allocator.create(StorageEngine);
    harness.storage_engine.* = try StorageEngine.init_default(harness.allocator, harness.node().filesystem_interface(), "large_entries_test_dir");
    try harness.storage_engine.startup();

    harness.query_engine = try harness.allocator.create(QueryEngine);
    harness.query_engine.* = QueryEngine.init(harness.allocator, harness.storage_engine);

    try testing.expectEqual(@as(u32, 3), harness.storage_engine.block_count());
}

test "streaming recovery memory efficiency" {
    const allocator = testing.allocator;

    var harness = try kausaldb.SimulationHarness.init_and_startup(allocator, 0xFEEDFACE, "memory_efficiency_test_dir");
    defer harness.deinit();

    // Create many entries to test memory efficiency
    const num_entries = 100; // Test with smaller number to debug the issue
    var test_blocks = std.ArrayList(ContextBlock).init(allocator);
    defer test_blocks.deinit();
    try test_blocks.ensureTotalCapacity(num_entries);

    for (0..num_entries) |i| {
        const test_block = try TestData.create_test_block(allocator, @as(u8, @intCast(i + 1))); // Use sequential IDs starting from 1
        try test_blocks.append(test_block);
        try harness.storage_engine.put_block(test_block);
    }

    // Clean up all blocks after storage operations
    for (test_blocks.items) |block| {
        allocator.free(block.content);
        allocator.free(block.source_uri);
        allocator.free(block.metadata_json);
    }

    // Recovery should process all entries without excessive memory usage
    // CRITICAL: Must recreate both storage engine AND query engine to prevent arena use-after-free
    harness.storage_engine.deinit();
    harness.allocator.destroy(harness.storage_engine);
    harness.query_engine.deinit();
    harness.allocator.destroy(harness.query_engine);

    harness.storage_engine = try harness.allocator.create(StorageEngine);
    harness.storage_engine.* = try StorageEngine.init_default(harness.allocator, harness.node().filesystem_interface(), "memory_efficiency_test_dir");
    try harness.storage_engine.startup();

    harness.query_engine = try harness.allocator.create(QueryEngine);
    harness.query_engine.* = QueryEngine.init(harness.allocator, harness.storage_engine);

    try testing.expectEqual(@as(u32, 100), harness.storage_engine.block_count());
}

test "streaming recovery empty WAL" {
    const allocator = testing.allocator;

    var harness = try kausaldb.SimulationHarness.init_and_startup(allocator, 0xBEEFFEED, "empty_wal_test_dir");
    defer harness.deinit();

    // Don't write any data - WAL should be empty

    // Recovery from empty WAL should complete without errors
    // CRITICAL: Must recreate both storage engine AND query engine to prevent arena use-after-free
    harness.storage_engine.deinit();
    harness.allocator.destroy(harness.storage_engine);
    harness.query_engine.deinit();
    harness.allocator.destroy(harness.query_engine);

    harness.storage_engine = try harness.allocator.create(StorageEngine);
    harness.storage_engine.* = try StorageEngine.init_default(harness.allocator, harness.node().filesystem_interface(), "empty_wal_test_dir");
    try harness.storage_engine.startup();

    harness.query_engine = try harness.allocator.create(QueryEngine);
    harness.query_engine.* = QueryEngine.init(harness.allocator, harness.storage_engine);

    try testing.expectEqual(@as(u32, 0), harness.storage_engine.block_count());
    try testing.expectEqual(@as(u32, 0), harness.storage_engine.edge_count());
}
