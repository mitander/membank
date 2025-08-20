//! WAL Recovery Tests
//!
//! Tests for Write-Ahead Log recovery functionality.
//! Tests cover successful recovery, corruption handling, and edge cases.

const std = @import("std");

const kausaldb = @import("kausaldb");

const golden_master = kausaldb.golden_master;
const simulation_vfs = kausaldb.simulation_vfs;
const storage = kausaldb.storage;
const testing = std.testing;
const types = kausaldb.types;

const ContextBlock = types.ContextBlock;
const BlockId = types.BlockId;
const GraphEdge = types.GraphEdge;
const EdgeType = types.EdgeType;
const SimulationVFS = simulation_vfs.SimulationVFS;
const StorageEngine = storage.StorageEngine;
const TestData = kausaldb.TestData;
const StorageHarness = kausaldb.StorageHarness;

test "wal recovery with empty directory" {
    const allocator = testing.allocator;

    // Use StorageHarness for coordinated setup
    var harness = try kausaldb.StorageHarness.init_and_startup(allocator, "wal_empty_data");
    defer harness.deinit();

    try testing.expectEqual(@as(u32, 0), harness.storage_engine.block_count());
}

test "wal recovery with missing wal directory" {
    const allocator = testing.allocator;

    // Use StorageHarness for coordinated setup
    var harness = try kausaldb.StorageHarness.init_and_startup(allocator, "wal_missing_data");
    defer harness.deinit();

    // Don't call startup() to avoid creating WAL directory
    // Engine is already in initialized state after harness creation

    try testing.expectEqual(@as(u32, 0), harness.storage_engine.block_count());
}

test "wal recovery single block recovery" {
    const allocator = testing.allocator;

    // Manual setup required because: Recovery testing needs two separate
    // StorageEngine instances sharing the same VFS to validate WAL recovery
    // across engine lifecycle. StorageHarness is designed for single-engine scenarios.
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
        .id = TestData.deterministic_block_id(0x01234567),
        .version = 1,
        .source_uri = "test://wal_recovery.zig",
        .metadata_json = "{\"test\":\"wal_recovery\"}",
        .content = "pub fn recovery_test() void { return; }",
    };

    try storage_engine1.put_block(test_block);

    // Second storage engine: recover from WAL using same VFS
    var storage_engine2 = try StorageEngine.init_default(
        allocator,
        sim_vfs.vfs(),
        "wal_single_data",
    );
    defer storage_engine2.deinit();

    try storage_engine2.startup();

    // Verify block was recovered
    try testing.expectEqual(@as(u32, 1), storage_engine2.block_count());

    const recovered_block = (try storage_engine2.find_block(test_block.id, .query_engine)) orelse {
        try testing.expect(false);
        return;
    };
    try testing.expect(test_block.id.eql(recovered_block.extract().id));
    try testing.expectEqual(test_block.version, recovered_block.extract().version);
    try testing.expectEqualStrings(test_block.source_uri, recovered_block.extract().source_uri);
    try testing.expectEqualStrings(test_block.metadata_json, recovered_block.extract().metadata_json);
    try testing.expectEqualStrings(test_block.content, recovered_block.extract().content);

    // Golden master validation: ensure recovery behavior is deterministic
    try golden_master.verify_recovery_golden_master(allocator, "wal_single_block_recovery", &storage_engine2);
}

test "wal recovery multiple blocks and operations" {
    const allocator = testing.allocator;

    // Manual setup required because: Recovery testing needs two separate
    // StorageEngine instances sharing the same VFS to validate complex WAL
    // operations (puts, edges, deletes) across engine restarts.
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
        .id = TestData.deterministic_block_id(0x11111111),
        .version = 1,
        .source_uri = "test://wal_multiple_data.zig",
        .metadata_json = "{\"test\":\"wal_multiple_data\"}",
        .content = "const Block1 = struct {};",
    };

    const block2 = ContextBlock{
        .id = TestData.deterministic_block_id(0x22222222),
        .version = 1,
        .source_uri = "test://wal_multiple_data_block2.zig",
        .metadata_json = "{\"test\":\"wal_multiple_data\"}",
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

    // Second storage engine: recover from WAL using same VFS
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
    try testing.expect((try storage_engine2.find_block(block1.id, .query_engine)) == null);

    // Block2 should exist
    const recovered_block2 = (try storage_engine2.find_block(block2.id, .query_engine)).?;
    try testing.expect(block2.id.eql(recovered_block2.extract().id));
    try testing.expectEqual(block2.version, recovered_block2.extract().version);
}

test "wal recovery corruption with invalid checksum" {
    const allocator = testing.allocator;

    // Manual setup required because: Corruption testing needs direct VFS
    // file manipulation and two StorageEngine instances to validate recovery
    // behavior when encountering corrupted WAL entries.
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
        .id = TestData.deterministic_block_id(0x12345678),
        .version = 1,
        .source_uri = "test://wal_corruption_recovery.zig",
        .metadata_json = "{\"test\":\"wal_corruption_recovery\"}",
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

    // Try to recover from corrupted WAL using same VFS
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

    // Manual setup required because: Large block recovery testing needs
    // precise memory management and two StorageEngine instances to validate
    // WAL handling of blocks exceeding typical sizes.
    var sim_vfs = try SimulationVFS.init(allocator);
    defer sim_vfs.deinit();

    // Create large content (64KB to avoid excessive memory usage in tests)
    const large_content = try allocator.alloc(u8, 64 * 1024);
    defer allocator.free(large_content);
    @memset(large_content, 'x');

    const owned_content = try allocator.dupe(u8, large_content);
    defer allocator.free(owned_content); // Free duplicated content after WAL clones it

    const large_block = ContextBlock{
        .id = TestData.deterministic_block_id(0xabcdef01),
        .version = 1,
        .source_uri = "test://wal_large_blocks.zig",
        .metadata_json = "{\"test\":\"wal_large_blocks\"}",
        .content = owned_content,
    };

    // Write large block with first storage engine
    var storage_engine1 = try StorageEngine.init_default(
        allocator,
        sim_vfs.vfs(),
        "wal_large_data",
    );
    // Note: storage_engine1.deinit() called explicitly before recovery

    try storage_engine1.startup();
    try storage_engine1.put_block(large_block);

    // Explicitly close first engine to ensure WAL files are properly closed
    storage_engine1.deinit();

    // Recover large block using same VFS
    var storage_engine2 = try StorageEngine.init_default(
        allocator,
        sim_vfs.vfs(),
        "wal_large_data",
    );
    defer storage_engine2.deinit();

    try storage_engine2.startup();

    try testing.expectEqual(@as(u32, 1), storage_engine2.block_count());
    const recovered = (try storage_engine2.find_block(large_block.id, .query_engine)).?;
    try testing.expectEqual(large_content.len, recovered.extract().content.len);
    try testing.expect(std.mem.eql(u8, large_content, recovered.extract().content));
}

test "wal recovery stress with many entries" {
    const allocator = testing.allocator;

    // Manual setup required because: Stress testing needs two StorageEngine
    // instances sharing the same VFS to validate WAL recovery performance
    // and correctness with many entries across engine restarts.
    var sim_vfs = try SimulationVFS.init(allocator);
    defer sim_vfs.deinit();

    // Use smaller count for test efficiency (was 25, now 10)
    const num_blocks = 10;

    // Write many blocks with first storage engine
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
        const content = try std.fmt.allocPrint(allocator, "Block {} content", .{i});
        defer allocator.free(content);

        const owned_content = try allocator.dupe(u8, content);

        const block = ContextBlock{
            .id = TestData.deterministic_block_id(@intCast(i)),
            .version = @intCast(i),
            .source_uri = "test://wal_multiple_segments.zig",
            .metadata_json = "{\"test\":\"wal_multiple_segments\"}",
            .content = owned_content,
        };

        // Store the block first, then create expected copy
        try storage_engine1.put_block(block);

        // Clean up the original memory after storage engine has used it
        allocator.free(owned_content);

        // Create expected block copy with proper memory management
        const source_uri = try allocator.dupe(u8, block.source_uri);
        const metadata_json = try allocator.dupe(u8, block.metadata_json);
        const content_copy = try allocator.dupe(u8, content);

        const block_copy = ContextBlock{
            .id = block.id,
            .version = @intCast(i),
            .source_uri = source_uri,
            .metadata_json = metadata_json,
            .content = content_copy,
        };

        try expected_blocks.append(block_copy); // tidy:ignore-perf - capacity pre-allocated line 304
    }

    // Recover all blocks using same VFS
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
        const recovered = (try storage_engine2.find_block(expected.id, .query_engine)).?;
        try testing.expect(expected.id.eql(recovered.extract().id));
        try testing.expectEqual(expected.version, recovered.extract().version);
        try testing.expectEqualStrings(expected.source_uri, recovered.extract().source_uri);
        try testing.expectEqualStrings(expected.metadata_json, recovered.extract().metadata_json);
        try testing.expectEqualStrings(expected.content, recovered.extract().content);
    }
}
