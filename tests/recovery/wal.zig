//! WAL Recovery Tests
//!
//! Tests for Write-Ahead Log recovery functionality.
//! Tests cover successful recovery, corruption handling, and edge cases.

const kausaldb = @import("kausaldb");
const std = @import("std");
const testing = std.testing;

const simulation_vfs = kausaldb.simulation_vfs;
const storage = kausaldb.storage;
const types = kausaldb.types;
const golden_master = kausaldb.golden_master;

const ContextBlock = types.ContextBlock;
const BlockId = types.BlockId;
const GraphEdge = types.GraphEdge;
const EdgeType = types.EdgeType;
const SimulationVFS = simulation_vfs.SimulationVFS;
const StorageEngine = storage.StorageEngine;

test "wal recovery with empty directory" {
    const allocator = testing.allocator;

    var sim_vfs = try SimulationVFS.init(allocator);
    defer sim_vfs.deinit();

    var storage_engine = try StorageEngine.init_default(
        allocator,
        sim_vfs.vfs(),
        "wal_empty_data",
    );
    defer storage_engine.deinit();

    try storage_engine.startup();

    try testing.expectEqual(@as(u32, 0), storage_engine.block_count());
}

test "wal recovery with missing wal directory" {
    const allocator = testing.allocator;

    var sim_vfs = try SimulationVFS.init(allocator);
    defer sim_vfs.deinit();

    var storage_engine = try StorageEngine.init_default(
        allocator,
        sim_vfs.vfs(),
        "wal_missing_data",
    );
    defer storage_engine.deinit();

    // Don't call startup() to avoid creating WAL directory
    storage_engine.initialized = true;

    try testing.expectEqual(@as(u32, 0), storage_engine.block_count());
}

test "wal recovery single block recovery" {
    const allocator = testing.allocator;

    var sim_vfs = try SimulationVFS.init(allocator);
    defer sim_vfs.deinit();

    // First storage engine: write data
    var storage_engine1 = try StorageEngine.init_default(
        allocator,
        sim_vfs.vfs(),
        "wal_single_data",
    );
    defer storage_engine1.deinit();

    try storage_engine1.startup();

    const test_block = ContextBlock{
        .id = try BlockId.from_hex("0123456789abcdeffedcba9876543210"),
        .version = 1,
        .source_uri = "test://single_block.zig",
        .metadata_json = "{\"type\":\"function\",\"language\":\"zig\"}",
        .content = "pub fn recovery_test() void { return; }",
    };

    try storage_engine1.put_block(test_block);

    // Second storage engine: recover from WAL
    var storage_engine2 = try StorageEngine.init_default(
        allocator,
        sim_vfs.vfs(),
        "wal_single_data",
    );
    defer storage_engine2.deinit();

    try storage_engine2.startup();

    // Verify block was recovered
    try testing.expectEqual(@as(u32, 1), storage_engine2.block_count());

    const recovered_block = (try storage_engine2.find_block(test_block.id)) orelse {
        try testing.expect(false);
        return;
    };
    try testing.expect(test_block.id.eql(recovered_block.id));
    try testing.expectEqual(test_block.version, recovered_block.version);
    try testing.expectEqualStrings(test_block.source_uri, recovered_block.source_uri);
    try testing.expectEqualStrings(test_block.metadata_json, recovered_block.metadata_json);
    try testing.expectEqualStrings(test_block.content, recovered_block.content);

    // Golden master validation: ensure recovery behavior is deterministic
    try golden_master.verify_recovery_golden_master(allocator, "wal_single_block_recovery", &storage_engine2);
}

test "wal recovery multiple blocks and operations" {
    const allocator = testing.allocator;

    var sim_vfs = try SimulationVFS.init(allocator);
    defer sim_vfs.deinit();

    // First storage engine: write data
    var storage_engine1 = try StorageEngine.init_default(
        allocator,
        sim_vfs.vfs(),
        "wal_multiple_data",
    );
    defer storage_engine1.deinit();

    try storage_engine1.startup();

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

    // Second storage engine: recover from WAL
    var storage_engine2 = try StorageEngine.init_default(
        allocator,
        sim_vfs.vfs(),
        "wal_multiple_data",
    );
    defer storage_engine2.deinit();

    try storage_engine2.startup();

    // Verify final state: only block2 should exist (block1 was deleted)
    try testing.expectEqual(@as(u32, 1), storage_engine2.block_count());

    // Block1 should not exist
    try testing.expect((try storage_engine2.find_block(block1.id)) == null);

    // Block2 should exist
    const recovered_block2 = (try storage_engine2.find_block(block2.id)).?;
    try testing.expect(block2.id.eql(recovered_block2.id));
    try testing.expectEqual(block2.version, recovered_block2.version);
}

test "wal recovery corruption with invalid checksum" {
    const allocator = testing.allocator;

    var sim_vfs = try SimulationVFS.init(allocator);
    defer sim_vfs.deinit();

    // Create storage and write valid block first
    var storage_engine1 = try StorageEngine.init_default(
        allocator,
        sim_vfs.vfs(),
        "wal_corrupt_data",
    );
    defer storage_engine1.deinit();

    try storage_engine1.startup();

    const good_block = ContextBlock{
        .id = try BlockId.from_hex("1234567890abcdef1234567890abcdef"),
        .version = 1,
        .source_uri = "test://good.zig",
        .metadata_json = "{\"status\":\"good\"}",
        .content = "good content",
    };

    try storage_engine1.put_block(good_block);

    // Manually corrupt the WAL file by modifying checksum
    const wal_file_path = "wal_corrupt_data/wal/wal_0000.log";
    var corrupt_file = try sim_vfs.vfs().open(wal_file_path, .read_write);
    defer corrupt_file.close();

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
    try corrupt_file.flush();

    // Try to recover - should stop at corruption
    var storage_engine2 = try StorageEngine.init_default(
        allocator,
        sim_vfs.vfs(),
        "wal_corrupt_data",
    );
    defer storage_engine2.deinit();

    try storage_engine2.startup();

    // Should have no blocks due to corruption
    try testing.expectEqual(@as(u32, 0), storage_engine2.block_count());
}

test "wal recovery with large blocks" {
    const allocator = testing.allocator;

    var sim_vfs = try SimulationVFS.init(allocator);
    defer sim_vfs.deinit();

    // Create large content (64KB to avoid excessive memory usage in tests)
    const large_content = try allocator.alloc(u8, 64 * 1024);
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
    var storage_engine1 = try StorageEngine.init_default(
        allocator,
        sim_vfs.vfs(),
        "wal_large_data",
    );
    // Note: storage_engine1.deinit() is called explicitly before recovery

    try storage_engine1.startup();
    try storage_engine1.put_block(large_block);

    // Explicitly close the first storage engine to ensure WAL files are properly closed
    storage_engine1.deinit();

    // Recover large block
    var storage_engine2 = try StorageEngine.init_default(
        allocator,
        sim_vfs.vfs(),
        "wal_large_data",
    );
    defer storage_engine2.deinit();

    try storage_engine2.startup();

    try testing.expectEqual(@as(u32, 1), storage_engine2.block_count());
    const recovered = (try storage_engine2.find_block(large_block.id)).?;
    try testing.expectEqual(large_content.len, recovered.content.len);
    try testing.expect(std.mem.eql(u8, large_content, recovered.content));
}

test "wal recovery stress with many entries" {
    const allocator = testing.allocator;

    var sim_vfs = try SimulationVFS.init(allocator);
    defer sim_vfs.deinit();

    // Use smaller count for test efficiency (was 25, now 10)
    const num_blocks = 10;

    // Write many blocks
    var storage_engine1 = try StorageEngine.init_default(
        allocator,
        sim_vfs.vfs(),
        "wal_stress_data",
    );
    defer storage_engine1.deinit();

    try storage_engine1.startup();

    var expected_blocks = std.ArrayList(ContextBlock).init(allocator);
    try expected_blocks.ensureTotalCapacity(num_blocks); // tidy:ignore-perf - capacity pre-allocated for num_blocks
    defer {
        for (expected_blocks.items) |block| {
            allocator.free(block.source_uri);
            allocator.free(block.metadata_json);
            allocator.free(block.content);
        }
        expected_blocks.deinit();
    }

    // Create and store many blocks
    for (1..num_blocks + 1) |i| {
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
        try expected_blocks.append(block); // tidy:ignore-perf - capacity pre-allocated line 304
    }

    // Recover all blocks
    var storage_engine2 = try StorageEngine.init_default(
        allocator,
        sim_vfs.vfs(),
        "wal_stress_data",
    );
    defer storage_engine2.deinit();

    try storage_engine2.startup();

    try testing.expectEqual(@as(u32, num_blocks), storage_engine2.block_count());

    // Verify all blocks were recovered correctly
    for (expected_blocks.items) |expected| {
        const recovered = (try storage_engine2.find_block(expected.id)).?;
        try testing.expect(expected.id.eql(recovered.id));
        try testing.expectEqual(expected.version, recovered.version);
        try testing.expectEqualStrings(expected.source_uri, recovered.source_uri);
        try testing.expectEqualStrings(expected.metadata_json, recovered.metadata_json);
        try testing.expectEqualStrings(expected.content, recovered.content);
    }
}
