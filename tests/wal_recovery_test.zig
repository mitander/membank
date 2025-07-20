//! WAL Recovery Tests
//!
//! Comprehensive tests for Write-Ahead Log recovery functionality.
//! Tests cover successful recovery, corruption handling, and edge cases
//! using the deterministic simulation framework.

const std = @import("std");
const testing = std.testing;
const assert = std.debug.assert;

const context_block = @import("context_block");
const storage = @import("storage");
const simulation = @import("simulation");
const vfs = @import("vfs");
const simulation_vfs = @import("simulation_vfs");

const StorageEngine = storage.StorageEngine;
const ContextBlock = context_block.ContextBlock;
const BlockId = context_block.BlockId;
const GraphEdge = context_block.GraphEdge;
const EdgeType = context_block.EdgeType;
const Simulation = simulation.Simulation;

test "wal recovery: empty directory" {
    const allocator = testing.allocator;

    var sim = try Simulation.init(allocator, 12345);
    defer sim.deinit();

    const node1 = try sim.add_node();
    const node1_ptr = sim.find_node(node1);
    const node1_vfs = node1_ptr.filesystem_interface();

    const data_dir = try allocator.dupe(u8, "wal_empty_data");
    defer allocator.free(data_dir);
    var storage_engine = try StorageEngine.init(
        allocator,
        node1_vfs,
        data_dir,
    );
    defer storage_engine.deinit();

    try storage_engine.initialize_storage();

    // Recovery from empty WAL directory should succeed
    try storage_engine.recover_from_wal();

    // Verify no blocks were recovered
    try testing.expectEqual(@as(u32, 0), storage_engine.block_count());
}

test "wal recovery: missing wal directory" {
    const allocator = testing.allocator;

    var sim = try Simulation.init(allocator, 54321);
    defer sim.deinit();

    const node1 = try sim.add_node();
    const node1_ptr = sim.find_node(node1);
    const node1_vfs = node1_ptr.filesystem_interface();

    const data_dir = try allocator.dupe(u8, "wal_missing_data");
    defer allocator.free(data_dir);
    var storage_engine = try StorageEngine.init(
        allocator,
        node1_vfs,
        data_dir,
    );
    defer storage_engine.deinit();

    // Don't call initialize_storage to avoid creating WAL directory
    storage_engine.initialized = true;

    // Recovery should handle missing directory gracefully
    try storage_engine.recover_from_wal();

    try testing.expectEqual(@as(u32, 0), storage_engine.block_count());
}

test "wal recovery: single block recovery" {
    const allocator = testing.allocator;

    var sim = try Simulation.init(allocator, 98765);
    defer sim.deinit();

    const node1 = try sim.add_node();
    const node1_ptr = sim.find_node(node1);
    const node1_vfs = node1_ptr.filesystem_interface();

    // First storage engine: write data
    const data_dir = try allocator.dupe(u8, "wal_single_data");
    defer allocator.free(data_dir);
    var storage_engine1 = try StorageEngine.init(
        allocator,
        node1_vfs,
        data_dir,
    );
    defer storage_engine1.deinit();

    try storage_engine1.initialize_storage();

    // Create and store a block
    const test_block = ContextBlock{
        .id = try BlockId.from_hex("0123456789abcdeffedcba9876543210"),
        .version = 1,
        .source_uri = "test://single_block.zig",
        .metadata_json = "{\"type\":\"function\",\"language\":\"zig\"}",
        .content = "pub fn recovery_test() void { return; }",
    };

    try storage_engine1.put_block(test_block);
    try storage_engine1.flush_wal();

    // Second storage engine: recover from WAL
    const data_dir2 = try allocator.dupe(u8, "wal_single_data");
    defer allocator.free(data_dir2);
    var storage_engine2 = try StorageEngine.init(
        allocator,
        node1_vfs,
        data_dir2,
    );
    defer storage_engine2.deinit();

    try storage_engine2.initialize_storage();
    try storage_engine2.recover_from_wal();

    // Verify block was recovered
    try testing.expectEqual(@as(u32, 1), storage_engine2.block_count());

    const recovered_block = try storage_engine2.find_block_by_id(test_block.id);
    try testing.expect(test_block.id.eql(recovered_block.id));
    try testing.expectEqual(test_block.version, recovered_block.version);
    try testing.expectEqualStrings(test_block.source_uri, recovered_block.source_uri);
    try testing.expectEqualStrings(test_block.metadata_json, recovered_block.metadata_json);
    try testing.expectEqualStrings(test_block.content, recovered_block.content);
}

test "wal recovery: multiple blocks and types" {
    const allocator = testing.allocator;

    var sim = try Simulation.init(allocator, 13579);
    defer sim.deinit();

    const node1 = try sim.add_node();
    const node1_ptr = sim.find_node(node1);
    const node1_vfs = node1_ptr.filesystem_interface();

    // First storage engine: write data
    const data_dir = try allocator.dupe(u8, "wal_multiple_data");
    defer allocator.free(data_dir);
    var storage_engine1 = try StorageEngine.init(
        allocator,
        node1_vfs,
        data_dir,
    );
    defer storage_engine1.deinit();

    try storage_engine1.initialize_storage();

    // Create multiple blocks
    const block1 = ContextBlock{
        .id = try BlockId.from_hex("11111111111111111111111111111111"),
        .version = 1,
        .source_uri = "test://block1.zig",
        .metadata_json = "{\"type\":\"struct\"}",
        .content = "const Block1 = struct {};",
    };

    const block2 = ContextBlock{
        .id = try BlockId.from_hex("22222222222222222222222222222222"),
        .version = 2,
        .source_uri = "test://block2.zig",
        .metadata_json = "{\"type\":\"function\"}",
        .content = "pub fn block2_function() void {}",
    };

    const edge = GraphEdge{
        .source_id = block1.id,
        .target_id = block2.id,
        .edge_type = .imports,
    };

    // Store blocks and edge
    try storage_engine1.put_block(block1);
    try storage_engine1.put_block(block2);
    try storage_engine1.put_edge(edge);

    // Delete first block
    try storage_engine1.delete_block(block1.id);

    try storage_engine1.flush_wal();

    // Second storage engine: recover from WAL
    const data_dir2 = try allocator.dupe(u8, "wal_multiple_data");
    defer allocator.free(data_dir2);
    var storage_engine2 = try StorageEngine.init(
        allocator,
        node1_vfs,
        data_dir2,
    );
    defer storage_engine2.deinit();

    try storage_engine2.initialize_storage();
    try storage_engine2.recover_from_wal();

    // Verify final state: only block2 should exist (block1 was deleted)
    try testing.expectEqual(@as(u32, 1), storage_engine2.block_count());

    // Block1 should not exist
    try testing.expectError(
        storage.StorageError.BlockNotFound,
        storage_engine2.find_block_by_id(block1.id),
    );

    // Block2 should exist
    const recovered_block2 = try storage_engine2.find_block_by_id(block2.id);
    try testing.expect(block2.id.eql(recovered_block2.id));
    try testing.expectEqual(block2.version, recovered_block2.version);
}

test "wal recovery: multiple wal files" {
    const allocator = testing.allocator;

    var sim = try Simulation.init(allocator, 24680);
    defer sim.deinit();

    const node1 = try sim.add_node();
    const node1_ptr = sim.find_node(node1);
    var node1_vfs = node1_ptr.filesystem_interface();

    // Create multiple WAL files manually to test file discovery
    const data_dir = "wal_multifile_data";
    const wal_dir = try std.fmt.allocPrint(allocator, "{s}/wal", .{data_dir});
    defer allocator.free(wal_dir);

    // Create directory structure
    try node1_vfs.mkdir(data_dir);
    try node1_vfs.mkdir(wal_dir);

    // Create test WAL files with different blocks
    const blocks = [_]ContextBlock{
        ContextBlock{
            .id = try BlockId.from_hex("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"),
            .version = 1,
            .source_uri = "test://file1.zig",
            .metadata_json = "{\"file\":1}",
            .content = "content from file 1",
        },
        ContextBlock{
            .id = try BlockId.from_hex("bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"),
            .version = 1,
            .source_uri = "test://file2.zig",
            .metadata_json = "{\"file\":2}",
            .content = "content from file 2",
        },
        ContextBlock{
            .id = try BlockId.from_hex("cccccccccccccccccccccccccccccccc"),
            .version = 1,
            .source_uri = "test://file3.zig",
            .metadata_json = "{\"file\":3}",
            .content = "content from file 3",
        },
    };

    // Create WAL files manually
    for (blocks, 0..) |block, i| {
        const wal_filename = try std.fmt.allocPrint(
            allocator,
            "{s}/wal_{d:0>4}.log",
            .{ wal_dir, i },
        );
        defer allocator.free(wal_filename);

        // Create WAL entry
        const wal_entry = try storage.WALEntry.create_put_block(block, allocator);
        defer wal_entry.deinit(allocator);

        // Serialize entry
        const serialized_size = storage.WALEntry.HEADER_SIZE + wal_entry.payload.len;
        const buffer = try allocator.alloc(u8, serialized_size);
        defer allocator.free(buffer);

        _ = try wal_entry.serialize(buffer);

        // Write to file
        var file = try node1_vfs.create(wal_filename);
        defer file.close() catch {};
        _ = try file.write(buffer);
        try file.close();
    }

    // Now test recovery
    const data_dir_copy = try allocator.dupe(u8, data_dir);
    defer allocator.free(data_dir_copy);
    var storage_engine = try StorageEngine.init(
        allocator,
        node1_vfs,
        data_dir_copy,
    );
    defer storage_engine.deinit();

    storage_engine.initialized = true; // Skip normal initialization
    try storage_engine.recover_from_wal();

    // Verify all blocks were recovered
    try testing.expectEqual(@as(u32, 3), storage_engine.block_count());

    for (blocks) |expected_block| {
        const recovered = try storage_engine.find_block_by_id(expected_block.id);
        try testing.expect(expected_block.id.eql(recovered.id));
        try testing.expectEqualStrings(expected_block.content, recovered.content);
    }
}

test "wal recovery: corruption handling - invalid checksum" {
    const allocator = testing.allocator;

    var sim = try Simulation.init(allocator, 11111);
    defer sim.deinit();

    const node1 = try sim.add_node();
    const node1_ptr = sim.find_node(node1);
    var node1_vfs = node1_ptr.filesystem_interface();

    // Create storage and write valid block first
    const data_dir = try allocator.dupe(u8, "wal_corrupt_data");
    defer allocator.free(data_dir);
    var storage_engine1 = try StorageEngine.init(
        allocator,
        node1_vfs,
        data_dir,
    );
    defer storage_engine1.deinit();

    try storage_engine1.initialize_storage();

    const good_block = ContextBlock{
        .id = try BlockId.from_hex("1234567890abcdef1234567890abcdef"),
        .version = 1,
        .source_uri = "test://good.zig",
        .metadata_json = "{\"status\":\"good\"}",
        .content = "good content",
    };

    try storage_engine1.put_block(good_block);
    try storage_engine1.flush_wal();

    // Manually corrupt the WAL file by modifying checksum
    const wal_file_path = "wal_corrupt_data/wal/wal_0000.log";
    var corrupt_file = try node1_vfs.open(wal_file_path, .read_write);
    defer corrupt_file.close() catch {};

    // Read current content
    const file_size = try corrupt_file.file_size();
    const content = try allocator.alloc(u8, file_size);
    defer allocator.free(content);
    _ = try corrupt_file.read(content);

    // Corrupt the checksum (first 8 bytes)
    content[0] = ~content[0];

    // Write back corrupted content
    _ = try corrupt_file.seek(0, .start);
    _ = try corrupt_file.write(content);
    try corrupt_file.close();

    // Try to recover - should stop at corruption
    const data_dir2 = try allocator.dupe(u8, "wal_corrupt_data");
    defer allocator.free(data_dir2);
    var storage_engine2 = try StorageEngine.init(
        allocator,
        node1_vfs,
        data_dir2,
    );
    defer storage_engine2.deinit();

    try storage_engine2.initialize_storage();
    try storage_engine2.recover_from_wal(); // Should not error, but should not recover blocks

    // Should have no blocks due to corruption
    try testing.expectEqual(@as(u32, 0), storage_engine2.block_count());
}

test "wal recovery: corruption handling - incomplete entry" {
    const allocator = testing.allocator;

    var sim = try Simulation.init(allocator, 22222);
    defer sim.deinit();

    const node1 = try sim.add_node();
    const node1_ptr = sim.find_node(node1);
    var node1_vfs = node1_ptr.filesystem_interface();

    // Create directory structure
    const data_dir = "wal_incomplete_data";
    const wal_dir = try std.fmt.allocPrint(allocator, "{s}/wal", .{data_dir});
    defer allocator.free(wal_dir);

    try node1_vfs.mkdir(data_dir);
    try node1_vfs.mkdir(wal_dir);

    // Create WAL file with incomplete entry (truncated)
    const wal_file_path = try std.fmt.allocPrint(allocator, "{s}/wal_0000.log", .{wal_dir});
    defer allocator.free(wal_file_path);

    var wal_file = try node1_vfs.create(wal_file_path);
    defer wal_file.close() catch {};

    // Write only part of a WAL entry header (should be 9 bytes, write only 5)
    const incomplete_header = [_]u8{ 0x01, 0x02, 0x03, 0x04, 0x05 };
    _ = try wal_file.write(&incomplete_header);
    try wal_file.close();

    // Try recovery
    const data_dir_copy = try allocator.dupe(u8, data_dir);
    defer allocator.free(data_dir_copy);
    var storage_engine = try StorageEngine.init(
        allocator,
        node1_vfs,
        data_dir_copy,
    );
    defer storage_engine.deinit();

    storage_engine.initialized = true;
    try storage_engine.recover_from_wal(); // Should handle incomplete entry gracefully

    try testing.expectEqual(@as(u32, 0), storage_engine.block_count());
}

test "wal recovery: deterministic behavior" {
    const allocator = testing.allocator;

    const seed = 55555;
    var results: [3]u32 = undefined;

    // Run recovery multiple times with same seed
    for (0..3) |i| {
        var sim = try Simulation.init(allocator, seed);
        defer sim.deinit();

        const node1 = try sim.add_node();
        const node1_ptr = sim.find_node(node1);
        const node1_vfs = node1_ptr.filesystem_interface();

        const data_dir = try std.fmt.allocPrint(
            allocator,
            "wal_deterministic_data_{}",
            .{i},
        );
        defer allocator.free(data_dir);

        // Write some data
        const data_dir_copy = try allocator.dupe(u8, data_dir);
        defer allocator.free(data_dir_copy);
        var storage_engine1 = try StorageEngine.init(
            allocator,
            node1_vfs,
            data_dir_copy,
        );
        defer storage_engine1.deinit();

        try storage_engine1.initialize_storage();

        const test_block = ContextBlock{
            .id = try BlockId.from_hex("fedcba9876543210fedcba9876543210"),
            .version = 42,
            .source_uri = "test://deterministic.zig",
            .metadata_json = "{\"deterministic\":true}",
            .content = "deterministic test content",
        };

        try storage_engine1.put_block(test_block);
        try storage_engine1.flush_wal();

        // Recover
        const data_dir_copy2 = try allocator.dupe(u8, data_dir);
        defer allocator.free(data_dir_copy2);
        var storage_engine2 = try StorageEngine.init(
            allocator,
            node1_vfs,
            data_dir_copy2,
        );
        defer storage_engine2.deinit();

        try storage_engine2.initialize_storage();
        try storage_engine2.recover_from_wal();

        results[i] = storage_engine2.block_count();
    }

    // All results should be identical
    try testing.expectEqual(results[0], results[1]);
    try testing.expectEqual(results[1], results[2]);
    try testing.expectEqual(@as(u32, 1), results[0]);
}

test "wal recovery: large blocks" {
    const allocator = testing.allocator;

    var sim = try Simulation.init(allocator, 77777);
    defer sim.deinit();

    const node1 = try sim.add_node();
    const node1_ptr = sim.find_node(node1);
    const node1_vfs = node1_ptr.filesystem_interface();

    // Create large content (1MB)
    const large_content = try allocator.alloc(u8, 1024 * 1024);
    defer allocator.free(large_content);
    @memset(large_content, 'x');

    const large_block = ContextBlock{
        .id = try BlockId.from_hex("abcdef0123456789abcdef0123456789"),
        .version = 1,
        .source_uri = "test://large.zig",
        .metadata_json = "{\"size\":\"large\"}",
        .content = large_content,
    };

    // Write large block
    const data_dir = try allocator.dupe(u8, "wal_large_data");
    defer allocator.free(data_dir);
    var storage_engine1 = try StorageEngine.init(
        allocator,
        node1_vfs,
        data_dir,
    );
    defer storage_engine1.deinit();

    try storage_engine1.initialize_storage();
    try storage_engine1.put_block(large_block);
    try storage_engine1.flush_wal();
    storage_engine1.deinit();

    // Recover large block
    const data_dir2 = try allocator.dupe(u8, "wal_large_data");
    defer allocator.free(data_dir2);
    var storage_engine2 = try StorageEngine.init(
        allocator,
        node1_vfs,
        data_dir2,
    );
    defer storage_engine2.deinit();

    try storage_engine2.initialize_storage();
    try storage_engine2.recover_from_wal();

    try testing.expectEqual(@as(u32, 1), storage_engine2.block_count());

    const recovered = try storage_engine2.find_block_by_id(large_block.id);
    try testing.expectEqual(large_content.len, recovered.content.len);
    try testing.expect(std.mem.eql(u8, large_content, recovered.content));
}

test "wal recovery: stress test with many entries" {
    const allocator = testing.allocator;

    var sim = try Simulation.init(allocator, 99999);
    defer sim.deinit();

    const node1 = try sim.add_node();
    const node1_ptr = sim.find_node(node1);
    const node1_vfs = node1_ptr.filesystem_interface();

    const num_blocks = 100;

    // Write many blocks
    const data_dir = try allocator.dupe(u8, "wal_stress_data");
    defer allocator.free(data_dir);
    var storage_engine1 = try StorageEngine.init(
        allocator,
        node1_vfs,
        data_dir,
    );
    defer storage_engine1.deinit();

    try storage_engine1.initialize_storage();

    var expected_blocks = std.ArrayList(ContextBlock).init(allocator);
    defer {
        for (expected_blocks.items) |block| {
            allocator.free(block.source_uri);
            allocator.free(block.metadata_json);
            allocator.free(block.content);
        }
        expected_blocks.deinit();
    }

    // Create and store many blocks
    for (0..num_blocks) |i| {
        const block_id_hex = try std.fmt.allocPrint(allocator, "{x:0>32}", .{i});
        defer allocator.free(block_id_hex);

        const source_uri = try std.fmt.allocPrint(allocator, "test://block_{}.zig", .{i});
        const metadata_json = try std.fmt.allocPrint(allocator, "{{\"index\":{}}}", .{i});
        const content = try std.fmt.allocPrint(allocator, "Block {} content", .{i});

        const block = ContextBlock{
            .id = try BlockId.from_hex(block_id_hex),
            .version = @intCast(i + 1),
            .source_uri = source_uri,
            .metadata_json = metadata_json,
            .content = content,
        };

        try storage_engine1.put_block(block);
        try expected_blocks.append(block);
    }

    try storage_engine1.flush_wal();
    storage_engine1.deinit();

    // Recover all blocks
    const data_dir2 = try allocator.dupe(u8, "wal_stress_data");
    defer allocator.free(data_dir2);
    var storage_engine2 = try StorageEngine.init(
        allocator,
        node1_vfs,
        data_dir2,
    );
    defer storage_engine2.deinit();

    try storage_engine2.initialize_storage();
    try storage_engine2.recover_from_wal();

    try testing.expectEqual(@as(u32, num_blocks), storage_engine2.block_count());

    // Verify all blocks were recovered correctly
    for (expected_blocks.items) |expected| {
        const recovered = try storage_engine2.find_block_by_id(expected.id);
        try testing.expect(expected.id.eql(recovered.id));
        try testing.expectEqual(expected.version, recovered.version);
        try testing.expectEqualStrings(expected.source_uri, recovered.source_uri);
        try testing.expectEqualStrings(expected.metadata_json, recovered.metadata_json);
        try testing.expectEqualStrings(expected.content, recovered.content);
    }
}
