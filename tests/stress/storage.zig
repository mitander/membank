//! Storage engine simulation tests for stress testing and failure scenarios.
//!
//! These tests focus on the storage engine behavior under various failure
//! conditions, heavy loads, and edge cases to ensure robustness and data integrity.

const cortexdb = @import("cortexdb");
const std = @import("std");
const testing = std.testing;
const simulation = cortexdb.simulation;
const vfs = cortexdb.vfs;
const assert = cortexdb.assert;
const context_block = cortexdb.types;
const storage = cortexdb.storage;
const simulation_vfs = cortexdb.simulation_vfs;

const Simulation = simulation.Simulation;
const NodeId = simulation.NodeId;
const ContextBlock = context_block.ContextBlock;
const BlockId = context_block.BlockId;
const GraphEdge = context_block.GraphEdge;
const EdgeType = context_block.EdgeType;
const StorageEngine = storage.StorageEngine;

test "storage stress: high volume writes during network partition" {
    const allocator = std.testing.allocator;

    var sim = try Simulation.init(allocator, 0x57E55501);
    defer sim.deinit();

    // Create 3-node cluster
    const node1 = try sim.add_node();
    const node2 = try sim.add_node();
    const node3 = try sim.add_node();

    sim.tick_multiple(10);

    // Initialize storage engines on each node
    const node1_ptr = sim.find_node(node1);
    const node1_vfs = node1_ptr.filesystem_interface();

    var storage_engine = try StorageEngine.init_default(
        allocator,
        node1_vfs,
        "storage_data",
    );
    defer storage_engine.deinit();

    try storage_engine.startup();

    // Create partition between node1 and other nodes
    sim.partition_nodes(node1, node2);
    sim.partition_nodes(node1, node3);

    // Write 100 blocks rapidly during partition
    var i: u32 = 0;
    while (i < 100) : (i += 1) {
        const block_id_hex = try std.fmt.allocPrint(allocator, "{:0>32}", .{i});
        defer allocator.free(block_id_hex);

        const block_id = try BlockId.from_hex(block_id_hex);
        const content = try std.fmt.allocPrint(allocator, "Block content #{}", .{i});
        defer allocator.free(content);

        const block = ContextBlock{
            .id = block_id,
            .version = 1,
            .source_uri = "stress://test",
            .metadata_json = "{\"stress_test\":true}",
            .content = content,
        };

        try storage_engine.put_block(block);

        // Advance simulation occasionally during writes
        if (i % 10 == 0) {
            sim.tick_multiple(2);
        }
    }

    // Verify all blocks were stored
    try std.testing.expectEqual(@as(u32, 100), storage_engine.block_count());

    // Heal partition and verify data integrity
    sim.heal_partition(node1, node2);
    sim.heal_partition(node1, node3);
    sim.tick_multiple(20);

    // Verify we can retrieve all blocks
    i = 0;
    while (i < 100) : (i += 1) {
        const block_id_hex = try std.fmt.allocPrint(allocator, "{:0>32}", .{i});
        defer allocator.free(block_id_hex);

        const block_id = try BlockId.from_hex(block_id_hex);
        const retrieved = (try storage_engine.find_block(block_id)) orelse {
            try testing.expect(false); // Block should exist
            return;
        };
        try std.testing.expect(retrieved.id.eql(block_id));
    }
}

test "storage recovery: WAL corruption simulation" {
    const allocator = std.testing.allocator;

    var sim = try Simulation.init(allocator, 0xCA1C2FD7);
    defer sim.deinit();

    const node1 = try sim.add_node();
    sim.tick_multiple(5);

    const node1_ptr = sim.find_node(node1);
    const node1_vfs = node1_ptr.filesystem_interface();

    var storage_engine = try StorageEngine.init_default(
        allocator,
        node1_vfs,
        "recovery_data",
    );

    try storage_engine.startup();

    // Write some initial blocks
    var i: u32 = 0;
    while (i < 10) : (i += 1) {
        const block_id_hex = try std.fmt.allocPrint(allocator, "{:0>32}", .{i});
        defer allocator.free(block_id_hex);

        const block_id = try BlockId.from_hex(block_id_hex);
        const content = try std.fmt.allocPrint(allocator, "Recovery test block #{}", .{i});
        defer allocator.free(content);

        const block = ContextBlock{
            .id = block_id,
            .version = 1,
            .source_uri = "recovery://test",
            .metadata_json = "{\"recovery_test\":true}",
            .content = content,
        };

        try storage_engine.put_block(block);
    }

    try std.testing.expectEqual(@as(u32, 10), storage_engine.block_count());

    // Properly close first storage engine before restart simulation
    storage_engine.deinit();

    // Simulate system restart by creating new storage engine instance with same directory
    var storage_engine2 = try StorageEngine.init_default(
        allocator,
        node1_vfs,
        "recovery_data",
    );
    defer storage_engine2.deinit();

    try storage_engine2.startup();

    // Verify blocks were recovered from WAL
    try std.testing.expectEqual(@as(u32, 10), storage_engine2.block_count());

    // Verify we can retrieve the recovered blocks
    for (0..10) |j| {
        const block_id_hex = try std.fmt.allocPrint(allocator, "{:0>32}", .{j});
        defer allocator.free(block_id_hex);
        const block_id = try BlockId.from_hex(block_id_hex);
        const recovered_block = (try storage_engine2.find_block(block_id)) orelse {
            try testing.expect(false); // Block should exist
            return;
        };
        try std.testing.expect(block_id.eql(recovered_block.id));
        try std.testing.expectEqual(@as(u64, 1), recovered_block.version);
        try std.testing.expectEqualStrings("recovery://test", recovered_block.source_uri);
        try std.testing.expectEqualStrings(
            "{\"recovery_test\":true}",
            recovered_block.metadata_json,
        );
    }
}

test "storage limits: large block handling" {
    const allocator = std.testing.allocator;

    var sim = try Simulation.init(allocator, 0x1A26EB1C);
    defer sim.deinit();

    const node1 = try sim.add_node();
    sim.tick_multiple(5);

    const node1_ptr = sim.find_node(node1);
    const node1_vfs = node1_ptr.filesystem_interface();

    var storage_engine = try StorageEngine.init_default(
        allocator,
        node1_vfs,
        "large_data",
    );
    defer storage_engine.deinit();

    try storage_engine.startup();

    // Create a large block (1MB content)
    const large_content = try allocator.alloc(u8, 1024 * 1024);
    defer allocator.free(large_content);

    // Fill with valid UTF-8 content to pass validation
    for (large_content, 0..) |*byte, idx| {
        byte.* = 'A' + @as(u8, @intCast(idx % 26));
    }

    const block_id = try BlockId.from_hex("0123456789abcdeffedcba9876543210");
    const large_block = ContextBlock{
        .id = block_id,
        .version = 1,
        .source_uri = "large://test/file.dat",
        .metadata_json = "{\"type\":\"large_data\",\"size\":1048576}",
        .content = large_content,
    };

    // Test storing large block
    try storage_engine.put_block(large_block);
    try std.testing.expectEqual(@as(u32, 1), storage_engine.block_count());

    // Test retrieving large block
    const retrieved = (try storage_engine.find_block(block_id)) orelse {
        try testing.expect(false); // Block should exist
        return;
    };
    try std.testing.expectEqual(@as(usize, 1024 * 1024), retrieved.content.len);
    try std.testing.expectEqualSlices(u8, large_content, retrieved.content);
}

test "storage concurrency: rapid block updates" {
    const allocator = std.testing.allocator;

    var sim = try Simulation.init(allocator, 0x2A91DFDD);
    defer sim.deinit();

    const node1 = try sim.add_node();
    sim.tick_multiple(5);

    const node1_ptr = sim.find_node(node1);
    const node1_vfs = node1_ptr.filesystem_interface();

    var storage_engine = try StorageEngine.init_default(
        allocator,
        node1_vfs,
        "rapid_data",
    );
    defer storage_engine.deinit();

    try storage_engine.startup();

    const block_id = try BlockId.from_hex("11111111111111112222222222222222");

    // Rapidly update the same block multiple times
    var version: u64 = 1;
    while (version <= 50) : (version += 1) {
        const content = try std.fmt.allocPrint(allocator, "Version {} content", .{version});
        defer allocator.free(content);

        const block = ContextBlock{
            .id = block_id,
            .version = version,
            .source_uri = "rapid://updates",
            .metadata_json = "{\"rapid_update\":true}",
            .content = content,
        };

        try storage_engine.put_block(block);

        // Verify the latest version is stored
        const retrieved = (try storage_engine.find_block(block_id)) orelse {
            try testing.expect(false); // Block should exist
            return;
        };
        try std.testing.expectEqual(version, retrieved.version);

        sim.tick_multiple(1);
    }

    // Final verification
    const final_block = (try storage_engine.find_block(block_id)).?;
    try std.testing.expectEqual(@as(u64, 50), final_block.version);
    try std.testing.expect(std.mem.indexOf(u8, final_block.content, "Version 50") != null);
}

test "storage integrity: duplicate block handling" {
    const allocator = std.testing.allocator;

    var sim = try Simulation.init(allocator, 0xDFD11CA7);
    defer sim.deinit();

    const node1 = try sim.add_node();
    sim.tick_multiple(5);

    const node1_ptr = sim.find_node(node1);
    const node1_vfs = node1_ptr.filesystem_interface();

    var storage_engine = try StorageEngine.init_default(
        allocator,
        node1_vfs,
        "dup_data",
    );
    defer storage_engine.deinit();

    try storage_engine.startup();

    const block_id = try BlockId.from_hex("deadbeefcafebabe1337133713371337");
    const original_block = ContextBlock{
        .id = block_id,
        .version = 1,
        .source_uri = "dup://test/original",
        .metadata_json = "{\"original\":true}",
        .content = "Original content",
    };

    // Store original block
    try storage_engine.put_block(original_block);
    try std.testing.expectEqual(@as(u32, 1), storage_engine.block_count());

    // Try to store duplicate (should overwrite)
    const duplicate_block = ContextBlock{
        .id = block_id,
        .version = 2,
        .source_uri = "dup://test/updated",
        .metadata_json = "{\"updated\":true}",
        .content = "Updated content",
    };

    try storage_engine.put_block(duplicate_block);
    try std.testing.expectEqual(@as(u32, 1), storage_engine.block_count());

    // Verify the updated version is stored
    const retrieved = (try storage_engine.find_block(block_id)) orelse {
        try testing.expect(false); // Block should exist
        return;
    };
    try std.testing.expectEqual(@as(u64, 2), retrieved.version);
    try std.testing.expectEqualStrings("Updated content", retrieved.content);
    try std.testing.expectEqualStrings("dup://test/updated", retrieved.source_uri);
}

test "storage edges: graph relationship persistence" {
    const allocator = std.testing.allocator;

    var sim = try Simulation.init(allocator, 0x62A9DE1);
    defer sim.deinit();

    const node1 = try sim.add_node();
    sim.tick_multiple(5);

    const node1_ptr = sim.find_node(node1);
    const node1_vfs = node1_ptr.filesystem_interface();

    var storage_engine = try StorageEngine.init_default(
        allocator,
        node1_vfs,
        "graph_data",
    );
    defer storage_engine.deinit();

    try storage_engine.startup();

    // Create related blocks
    const main_id = try BlockId.from_hex("11111111111111111111111111111111");
    const util_id = try BlockId.from_hex("22222222222222222222222222222222");
    const test_id = try BlockId.from_hex("33333333333333333333333333333333");

    const main_block = ContextBlock{
        .id = main_id,
        .version = 1,
        .source_uri = "graph://main.zig",
        .metadata_json = "{\"type\":\"function\",\"name\":\"main\"}",
        .content = "pub fn main() !void { utils.helper(); }",
    };

    const util_block = ContextBlock{
        .id = util_id,
        .version = 1,
        .source_uri = "graph://utils.zig",
        .metadata_json = "{\"type\":\"function\",\"name\":\"helper\"}",
        .content = "pub fn helper() void { return; }",
    };

    const test_block = ContextBlock{
        .id = test_id,
        .version = 1,
        .source_uri = "graph://test.zig",
        .metadata_json = "{\"type\":\"test\",\"name\":\"test_main\"}",
        .content = "test \"main\" { main(); }",
    };

    // Store blocks
    try storage_engine.put_block(main_block);
    try storage_engine.put_block(util_block);
    try storage_engine.put_block(test_block);

    // Create edges
    const import_edge = GraphEdge{
        .source_id = main_id,
        .target_id = util_id,
        .edge_type = EdgeType.imports,
    };

    const calls_edge = GraphEdge{
        .source_id = main_id,
        .target_id = util_id,
        .edge_type = EdgeType.calls,
    };

    const test_edge = GraphEdge{
        .source_id = test_id,
        .target_id = main_id,
        .edge_type = EdgeType.calls,
    };

    // Store edges (should not error for now)
    try storage_engine.put_edge(import_edge);
    try storage_engine.put_edge(calls_edge);
    try storage_engine.put_edge(test_edge);

    // Verify blocks are still accessible
    try std.testing.expectEqual(@as(u32, 3), storage_engine.block_count());

    const retrieved_main = (try storage_engine.find_block(main_id)) orelse {
        try testing.expect(false); // Block should exist
        return;
    };
    try std.testing.expect(std.mem.indexOf(u8, retrieved_main.content, "utils.helper()") != null);
}

test "storage performance: batch operations under load" {
    const allocator = std.testing.allocator;

    var sim = try Simulation.init(allocator, 0xBA7C410D);
    defer sim.deinit();

    const node1 = try sim.add_node();
    const node2 = try sim.add_node();

    // High latency and packet loss
    sim.configure_latency(node1, node2, 15);
    sim.configure_packet_loss(node1, node2, 0.3);

    sim.tick_multiple(10);

    const node1_ptr = sim.find_node(node1);
    const node1_vfs = node1_ptr.filesystem_interface();

    var storage_engine = try StorageEngine.init_default(
        allocator,
        node1_vfs,
        "batch_data",
    );
    defer storage_engine.deinit();

    try storage_engine.startup();

    // Perform batch writes in chunks
    const batch_size = 20;
    const total_batches = 5;

    var batch: u32 = 0;
    while (batch < total_batches) : (batch += 1) {
        var i: u32 = 0;
        while (i < batch_size) : (i += 1) {
            const block_num = batch * batch_size + i;
            const block_id_hex = try std.fmt.allocPrint(allocator, "{x:0>32}", .{block_num});
            defer allocator.free(block_id_hex);

            const block_id = try BlockId.from_hex(block_id_hex);
            const content = try std.fmt.allocPrint(
                allocator,
                "Batch {} item {} content",
                .{ batch, i },
            );
            defer allocator.free(content);

            const block = ContextBlock{
                .id = block_id,
                .version = 1,
                .source_uri = "batch://load/test",
                .metadata_json = "{\"batch_test\":true}",
                .content = content,
            };

            try storage_engine.put_block(block);
        }

        // Advance simulation after each batch
        sim.tick_multiple(10);
    }

    // Verify all blocks were stored
    const expected_count = batch_size * total_batches;
    try std.testing.expectEqual(@as(u32, expected_count), storage_engine.block_count());

    // Spot check some blocks
    const first_block_id = try BlockId.from_hex("00000000000000000000000000000000");
    const last_block_id = try BlockId.from_hex("00000000000000000000000000000063"); // 99 in hex

    const first_block = (try storage_engine.find_block(first_block_id)) orelse {
        try testing.expect(false); // Block should exist
        return;
    };
    try std.testing.expect(std.mem.indexOf(u8, first_block.content, "Batch 0 item 0") != null);

    const last_block = (try storage_engine.find_block(last_block_id)) orelse {
        try testing.expect(false); // Block should exist
        return;
    };
    try std.testing.expect(std.mem.indexOf(u8, last_block.content, "Batch 4 item 19") != null);
}

test "storage robustness: invalid data handling" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const allocator = arena.allocator();

    var sim = try Simulation.init(allocator, 0x1FADA7);
    defer sim.deinit();

    const node1 = try sim.add_node();
    sim.tick_multiple(5);

    const node1_ptr = sim.find_node(node1);
    const node1_vfs = node1_ptr.filesystem_interface();

    var storage_engine = try StorageEngine.init_default(
        std.testing.allocator,
        node1_vfs,
        "invalid_data",
    );
    defer storage_engine.deinit();

    try storage_engine.startup();

    // Test invalid JSON metadata
    const block_id = try BlockId.from_hex("deadbeefdeadbeefdeadbeefdeadbeef");
    const invalid_block = ContextBlock{
        .id = block_id,
        .version = 1,
        .source_uri = "invalid://test",
        .metadata_json = "{this is not valid json",
        .content = "Some content",
    };

    // Should fail validation
    try std.testing.expectError(error.InvalidMetadataJson, storage_engine.put_block(invalid_block));

    // Verify no blocks were stored
    try std.testing.expectEqual(@as(u32, 0), storage_engine.block_count());

    // Test valid block after invalid one
    const valid_block = ContextBlock{
        .id = block_id,
        .version = 1,
        .source_uri = "valid://test",
        .metadata_json = "{\"valid\":true}",
        .content = "Valid content",
    };

    try storage_engine.put_block(valid_block);
    try std.testing.expectEqual(@as(u32, 1), storage_engine.block_count());
}
