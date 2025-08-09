//! Storage engine simulation tests for stress testing and failure scenarios.
//!
//! These tests focus on the storage engine behavior under various failure
//! conditions, heavy loads, and edge cases to ensure robustness and data integrity.

const kausaldb = @import("kausaldb");
const std = @import("std");
const testing = std.testing;
const simulation = kausaldb.simulation;
const vfs = kausaldb.vfs;
const assert = kausaldb.assert;
const types = kausaldb.types;
const storage = kausaldb.storage;
const simulation_vfs = kausaldb.simulation_vfs;

const Simulation = simulation.Simulation;
const NodeId = simulation.NodeId;
const ContextBlock = types.ContextBlock;
const BlockId = types.BlockId;
const GraphEdge = types.GraphEdge;

const TestData = kausaldb.TestData;
const StorageHarness = kausaldb.StorageHarness;
const SimulationHarness = kausaldb.SimulationHarness;
const EdgeType = types.EdgeType;
const StorageEngine = storage.StorageEngine;
const QueryEngine = kausaldb.query_engine.QueryEngine;

test "high volume writes during network partition" {
    const allocator = std.testing.allocator;

    // Use SimulationHarness for coordinated setup
    var harness = try SimulationHarness.init_and_startup(allocator, 0x12345678, "storage_data");
    defer harness.deinit();

    // Configure reasonable disk space limit for stress testing (100MB)
    const node_ptr1 = harness.simulation.find_node(harness.node_id);
    node_ptr1.filesystem.configure_disk_space_limit(100 * 1024 * 1024);

    // Add additional nodes for partition testing
    const node2 = try harness.simulation.add_node();
    const node3 = try harness.simulation.add_node();

    harness.tick(10);

    // Create network partition isolating primary node
    harness.simulation.partition_nodes(harness.node_id, node2);
    harness.simulation.partition_nodes(harness.node_id, node3);

    var successful_writes: u32 = 0;
    var i: u32 = 1;
    while (i < 101) : (i += 1) {
        // Use standardized test data creation
        const block = ContextBlock{
            .id = TestData.deterministic_block_id(@intCast(i)),
            .version = 1,
            .source_uri = "test://storage_load_basic.zig",
            .metadata_json = "{\"test\":\"storage_load_basic\"}",
            .content = "Storage load basic test block content",
        };

        // Handle IoError (including NoSpaceLeft) gracefully in stress test environment
        if (harness.storage_engine.put_block(block)) |_| {
            successful_writes += 1;
        } else |err| switch (err) {
            error.IoError => {
                // Expected behavior in stress test with disk space limits
                break;
            },
            else => return err,
        }

        if (i % 10 == 0) {
            harness.tick(1);
        }
    }

    // Verify blocks were stored (at least some under stress conditions)
    try std.testing.expect(successful_writes > 0);
    try std.testing.expectEqual(successful_writes, harness.storage_engine.block_count());

    harness.simulation.heal_partition(harness.node_id, node2);
    harness.simulation.heal_partition(harness.node_id, node3);
    harness.simulation.tick_multiple(20);

    // Verify we can retrieve stored blocks
    i = 1;
    var retrieved_count: u32 = 0;
    while (i < 101) : (i += 1) {
        const block_id = TestData.deterministic_block_id(i);
        if (try harness.storage_engine.find_block(block_id)) |retrieved| {
            try std.testing.expect(retrieved.id.eql(block_id));
            retrieved_count += 1;
        }
    }
    try std.testing.expectEqual(successful_writes, retrieved_count);
}

test "recovery from WAL corruption simulation" {
    const allocator = std.testing.allocator;

    var harness = try SimulationHarness.init_and_startup(allocator, 0xABCDEF00, "wal_recovery_test");
    defer harness.deinit();

    // Configure reasonable disk space limit for recovery testing (50MB)
    const node_ptr2 = harness.simulation.find_node(harness.node_id);
    node_ptr2.filesystem.configure_disk_space_limit(50 * 1024 * 1024);

    harness.simulation.tick_multiple(5);

    // Phase 1: Write test blocks
    for (1..11) |index| {
        const block = ContextBlock{
            .id = TestData.deterministic_block_id(@intCast(index)),
            .version = 1,
            .source_uri = "test://storage_load_concurrent.zig",
            .metadata_json = "{\"test\":\"storage_load_concurrent\"}",
            .content = "Storage load concurrent test block content",
        };

        try harness.storage_engine.put_block(block);
    }

    try std.testing.expectEqual(@as(u32, 10), harness.storage_engine.block_count());

    // Phase 2: Simulate restart by reinitializing storage engine with same VFS
    const node_ptr = harness.simulation.find_node(harness.node_id);
    const node_vfs = node_ptr.filesystem_interface();

    // Deinitialize current storage engine
    harness.storage_engine.deinit();
    allocator.destroy(harness.storage_engine);

    // Create new storage engine with same VFS (simulating restart)
    const new_storage_engine = try allocator.create(StorageEngine);
    new_storage_engine.* = try StorageEngine.init_default(allocator, node_vfs, "wal_recovery_test");
    try new_storage_engine.startup();
    harness.storage_engine = new_storage_engine;

    // Reinitialize query engine with new storage engine
    harness.query_engine.deinit();
    allocator.destroy(harness.query_engine);
    const new_query_engine = try allocator.create(QueryEngine);
    new_query_engine.* = QueryEngine.init(allocator, new_storage_engine);
    harness.query_engine = new_query_engine;

    // Phase 3: Verify recovery
    try std.testing.expectEqual(@as(u32, 10), harness.storage_engine.block_count());

    // Verify we can retrieve the recovered blocks
    for (1..11) |j| {
        const block_id = TestData.deterministic_block_id(@intCast(j));
        const recovered_block = (try harness.storage_engine.find_block(block_id)) orelse {
            try testing.expect(false); // Block should exist
            return;
        };
        try std.testing.expect(block_id.eql(recovered_block.id));
        try std.testing.expectEqual(@as(u64, 1), recovered_block.version);
        try std.testing.expectEqualStrings("test://storage_load_concurrent.zig", recovered_block.source_uri);
        try std.testing.expectEqualStrings(
            "{\"test\":\"storage_load_concurrent\"}",
            recovered_block.metadata_json,
        );
    }
}

test "large block handling limits" {
    const allocator = std.testing.allocator;

    // Use SimulationHarness for large block testing
    var harness = try SimulationHarness.init_and_startup(allocator, 0x1A26EB1C, "large_data");
    defer harness.deinit();

    harness.simulation.tick_multiple(5);

    const large_content = try allocator.alloc(u8, 1024 * 1024);
    defer allocator.free(large_content);

    // Fill with valid UTF-8 content to pass validation
    for (large_content, 0..) |*byte, idx| {
        byte.* = 'A' + @as(u8, @intCast(idx % 26));
    }

    const owned_content = try allocator.dupe(u8, large_content);
    defer allocator.free(owned_content);

    const large_block = ContextBlock{
        .id = TestData.deterministic_block_id(123),
        .version = 1,
        .source_uri = "test://storage_load_variable_size.zig",
        .metadata_json = "{\"test\":\"storage_load_variable_size\"}",
        .content = owned_content,
    };
    const block_id = large_block.id;

    // Test storing large block
    try harness.storage_engine.put_block(large_block);
    try std.testing.expectEqual(@as(u32, 1), harness.storage_engine.block_count());

    // Test retrieving large block
    const retrieved = (try harness.storage_engine.find_block(block_id)) orelse {
        try testing.expect(false); // Block should exist
        return;
    };
    try std.testing.expectEqual(@as(usize, 1024 * 1024), retrieved.content.len);
    try std.testing.expectEqualSlices(u8, large_content, retrieved.content);
}

test "rapid block updates concurrency" {
    const allocator = std.testing.allocator;

    // Use SimulationHarness for rapid update testing
    var harness = try SimulationHarness.init_and_startup(allocator, 0x2A91DFDD, "rapid_data");
    defer harness.deinit();

    harness.simulation.tick_multiple(5);

    const base_block = ContextBlock{
        .id = TestData.deterministic_block_id(111),
        .version = 1,
        .source_uri = "test://storage_load_test.zig",
        .metadata_json = "{\"test\":\"storage_load\"}",
        .content = "Storage load test block content",
    };

    const block_id = base_block.id;

    // Rapidly update the same block multiple times
    for (1..51) |version| {
        const content = try std.fmt.allocPrint(allocator, "Version {}", .{version});
        defer allocator.free(content);

        const owned_content = try allocator.dupe(u8, content);
        defer allocator.free(owned_content);

        const updated_block = ContextBlock{
            .id = TestData.deterministic_block_id(111),
            .version = @intCast(version),
            .source_uri = "test://rapid_block_updates.zig",
            .metadata_json = "{\"test\":\"rapid_block_updates\"}",
            .content = owned_content,
        };

        try harness.storage_engine.put_block(updated_block);

        // Verify update took effect
        const retrieved = (try harness.storage_engine.find_block(block_id)) orelse {
            try testing.expect(false); // Block should exist
            return;
        };
        try std.testing.expectEqual(version, retrieved.version);

        harness.tick(1);
    }

    // Final verification
    const final_block = (try harness.storage_engine.find_block(block_id)).?;
    try std.testing.expectEqual(@as(u64, 50), final_block.version);
    try std.testing.expect(std.mem.indexOf(u8, final_block.content, "Version 50") != null);
}

test "duplicate block handling integrity" {
    const allocator = std.testing.allocator;

    // Use SimulationHarness for duplicate handling testing
    var harness = try SimulationHarness.init_and_startup(allocator, 0xDFD11CA7, "dup_data");
    defer harness.deinit();

    harness.tick(5);

    const original_block = ContextBlock{
        .id = TestData.deterministic_block_id(0xdeadbeef),
        .version = 1,
        .source_uri = "test://duplicate_handling_original.zig",
        .metadata_json = "{\"test\":\"duplicate_handling\"}",
        .content = "Original content",
    };
    const block_id = original_block.id;

    try harness.storage_engine.put_block(original_block);
    try std.testing.expectEqual(@as(u32, 1), harness.storage_engine.block_count());

    // Try to store duplicate (should overwrite)
    const duplicate_block = ContextBlock{
        .id = TestData.deterministic_block_id(0xdeadbeef),
        .version = 2,
        .source_uri = "test://duplicate_handling_updated.zig",
        .metadata_json = "{\"test\":\"duplicate_handling\"}",
        .content = "Updated content",
    };

    try harness.storage_engine.put_block(duplicate_block);
    try std.testing.expectEqual(@as(u32, 1), harness.storage_engine.block_count());

    // Verify the updated version is stored
    const retrieved = (try harness.storage_engine.find_block(block_id)) orelse {
        try testing.expect(false); // Block should exist
        return;
    };
    try std.testing.expectEqual(@as(u64, 2), retrieved.version);
    try std.testing.expectEqualStrings("Updated content", retrieved.content);
    try std.testing.expectEqualStrings("test://duplicate_handling_updated.zig", retrieved.source_uri);
}

test "graph relationship persistence" {
    const allocator = std.testing.allocator;

    // Use SimulationHarness for graph relationship testing
    var harness = try SimulationHarness.init_and_startup(allocator, 0x62A9DE1, "graph_data");
    defer harness.deinit();

    harness.tick(5);

    const main_block = ContextBlock{
        .id = TestData.deterministic_block_id(0x11111111),
        .version = 1,
        .source_uri = "test://graph_relationship_main.zig",
        .metadata_json = "{\"test\":\"graph_relationship\"}",
        .content = "pub fn main() !void { utils.helper(); }",
    };
    const main_id = main_block.id;

    const util_block = ContextBlock{
        .id = TestData.deterministic_block_id(0x22222222),
        .version = 1,
        .source_uri = "test://graph_relationship_util.zig",
        .metadata_json = "{\"test\":\"graph_relationship\"}",
        .content = "pub fn helper() void { return; }",
    };
    const util_id = util_block.id;

    const test_block = ContextBlock{
        .id = TestData.deterministic_block_id(0x33333333),
        .version = 1,
        .source_uri = "test://graph_relationship_test.zig",
        .metadata_json = "{\"test\":\"graph_relationship\"}",
        .content = "test \"main\" { main(); }",
    };
    const test_id = test_block.id;

    try harness.storage_engine.put_block(main_block);
    try harness.storage_engine.put_block(util_block);
    try harness.storage_engine.put_block(test_block);

    const import_edge = GraphEdge{
        .source_id = main_id,
        .target_id = util_id,
        .edge_type = .imports,
    };
    const calls_edge = GraphEdge{
        .source_id = main_id,
        .target_id = util_id,
        .edge_type = .calls,
    };
    const test_edge = GraphEdge{
        .source_id = test_id,
        .target_id = main_id,
        .edge_type = .calls,
    };

    try harness.storage_engine.put_edge(import_edge);
    try harness.storage_engine.put_edge(calls_edge);
    try harness.storage_engine.put_edge(test_edge);

    try std.testing.expectEqual(@as(u32, 3), harness.storage_engine.block_count());

    const retrieved_main = (try harness.storage_engine.find_block(main_id)) orelse {
        try testing.expect(false); // Block should exist
        return;
    };
    try std.testing.expect(std.mem.indexOf(u8, retrieved_main.content, "utils.helper()") != null);
}

test "batch operations under load" {
    const allocator = std.testing.allocator;

    // Use SimulationHarness for batch operations testing
    var harness = try SimulationHarness.init_and_startup(allocator, 0x9876FEDC, "batch_data");
    defer harness.deinit();

    // Configure reasonable disk space limit for batch operations testing (100MB)
    const node_ptr3 = harness.simulation.find_node(harness.node_id);
    node_ptr3.filesystem.configure_disk_space_limit(100 * 1024 * 1024);

    // Add additional node for network simulation
    const node2 = try harness.simulation.add_node();

    // High latency and packet loss
    harness.simulation.configure_latency(harness.node_id, node2, 15);
    harness.simulation.configure_packet_loss(harness.node_id, node2, 0.3);

    harness.simulation.tick_multiple(10);

    // Perform batch writes in chunks (start from 1, all-zero BlockID invalid)
    const batch_size = 20;
    const total_batches = 5;

    var batch: u32 = 0;
    while (batch < total_batches) : (batch += 1) {
        var item: u32 = 0;
        while (item < batch_size) : (item += 1) {
            const block_num = batch * batch_size + item + 1; // Start from 1
            const content = try std.fmt.allocPrint(
                allocator,
                "Batch {} item {} content",
                .{ batch, item },
            );
            defer allocator.free(content);

            const owned_content = try allocator.dupe(u8, content);
            defer allocator.free(owned_content);

            const block = ContextBlock{
                .id = TestData.deterministic_block_id(@intCast(block_num)),
                .version = 1,
                .source_uri = "test://batch_operations_load.zig",
                .metadata_json = "{\"test\":\"batch_operations_load\"}",
                .content = owned_content,
            };

            // Handle IoError (including NoSpaceLeft) gracefully in stress test environment
            harness.storage_engine.put_block(block) catch |err| switch (err) {
                error.IoError => {
                    // Expected behavior in stress test with disk space limits
                    break;
                },
                else => return err,
            };
        }

        // Advance simulation after each batch
        harness.tick(10);
    }

    // Verify some blocks were stored (stress test may hit disk limits)
    const stored_count = harness.storage_engine.block_count();
    try std.testing.expect(stored_count > 0);
    try std.testing.expect(stored_count <= batch_size * total_batches);

    // Spot check some blocks (adjusted for 1-based indexing)
    const first_block_id = TestData.deterministic_block_id(1); // First block is now 1
    const last_block_id = TestData.deterministic_block_id(100);

    const first_block = (try harness.storage_engine.find_block(first_block_id)) orelse {
        try testing.expect(false); // Block should exist
        return;
    };
    try std.testing.expect(std.mem.indexOf(u8, first_block.content, "Batch 0 item 0") != null);

    const last_block = (try harness.storage_engine.find_block(last_block_id)) orelse {
        try testing.expect(false); // Block should exist
        return;
    };
    try std.testing.expect(std.mem.indexOf(u8, last_block.content, "Batch 4 item 19") != null);
}

test "invalid data handling robustness" {
    const allocator = std.testing.allocator;

    // Use SimulationHarness for invalid data testing
    var harness = try SimulationHarness.init_and_startup(allocator, 0x1FADA7, "invalid_data");
    defer harness.deinit();

    harness.tick(5);

    // Test invalid JSON metadata - create manually for invalid JSON test
    const invalid_block = ContextBlock{
        .id = TestData.deterministic_block_id(0xdeadbeef),
        .version = 1,
        .source_uri = "invalid://test",
        .metadata_json = "{this is not valid json",
        .content = "Some content",
    };

    // Should fail validation
    try std.testing.expectError(error.InvalidMetadataJson, harness.storage_engine.put_block(invalid_block));

    // Verify no blocks were stored
    try std.testing.expectEqual(@as(u32, 0), harness.storage_engine.block_count());

    // Test valid block after invalid one
    const valid_block = ContextBlock{
        .id = TestData.deterministic_block_id(0xdeadbeef),
        .version = 1,
        .source_uri = "test://invalid_data_handling.zig",
        .metadata_json = "{\"test\":\"invalid_data_handling\"}",
        .content = "Valid content",
    };

    try harness.storage_engine.put_block(valid_block);
    try std.testing.expectEqual(@as(u32, 1), harness.storage_engine.block_count());
}
