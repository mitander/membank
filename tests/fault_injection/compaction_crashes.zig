//! Mid-Compaction Crash Recovery Tests
//!
//! Tests storage engine recovery from crashes that occur during compaction.
//! Validates that the system can recover gracefully from intermediate states
//! and maintain data integrity across crash boundaries.

const kausaldb = @import("kausaldb");
const std = @import("std");
const testing = std.testing;

const vfs = kausaldb.vfs;
const simulation_vfs = kausaldb.simulation_vfs;
const storage = kausaldb.storage;
const context_block = kausaldb.types;
const concurrency = kausaldb.concurrency;

const StorageEngine = storage.StorageEngine;
const ContextBlock = context_block.ContextBlock;
const BlockId = context_block.BlockId;
const SimulationVFS = simulation_vfs.SimulationVFS;

// Helper function to generate deterministic BlockId for testing
fn deterministic_block_id(seed: u32) BlockId {
    var bytes: [16]u8 = undefined;
    var i: usize = 0;
    while (i < 16) : (i += 4) {
        const value = seed + @as(u32, @intCast(i));
        std.mem.writeInt(u32, bytes[i .. i + 4], value, .little);
    }
    return BlockId.from_bytes(bytes);
}

// Helper to populate storage with enough data to trigger compaction
fn populate_storage_for_compaction(
    allocator: std.mem.Allocator,
    storage_engine: *StorageEngine,
    block_count: u32,
) !void {
    for (0..block_count) |i| {
        const source_uri = try std.fmt.allocPrint(allocator, "test://block{d}", .{i});
        defer allocator.free(source_uri);

        const metadata_json = try std.fmt.allocPrint(allocator, "{{\"index\":{d},\"compaction_test\":true}}", .{i});
        defer allocator.free(metadata_json);

        const content = try std.fmt.allocPrint(allocator, "Content for block {d} - this is deliberately large to trigger compaction sooner. " ++
            "Additional padding to ensure blocks are substantial in size for compaction testing. " ++
            "More content to reach the threshold for triggering background compaction processes.", .{i});
        defer allocator.free(content);

        const block = ContextBlock{
            .id = deterministic_block_id(@intCast(i)),
            .version = 1,
            .source_uri = source_uri,
            .metadata_json = metadata_json,
            .content = content,
        };

        try storage_engine.put_block(block);
    }
}

test "compaction crash - recovery from partial sstable write" {
    concurrency.init();

    const allocator = testing.allocator;

    var sim_vfs = SimulationVFS.init_with_fault_seed(allocator, 12345);
    defer sim_vfs.deinit();

    const vfs_interface = sim_vfs.vfs();

    // Initial setup and population
    var storage_engine = try StorageEngine.init_default(allocator, vfs_interface, "test_db");
    defer storage_engine.deinit();

    try storage_engine.startup();
    try populate_storage_for_compaction(allocator, &storage_engine, 50);

    // Force flush to create initial SSTables
    try storage_engine.flush_memtable();

    // Add more data to trigger compaction
    try populate_storage_for_compaction(allocator, &storage_engine, 30);

    // Configure I/O failure during SSTable write operations (simulate crash)
    sim_vfs.enable_io_failures(500, .{ .write = true }); // 50% failure rate

    // Attempt operations that would trigger compaction - some should fail
    var compaction_attempts: u32 = 0;
    var compaction_failures: u32 = 0;

    for (0..10) |_| {
        // Add blocks to potentially trigger compaction
        const result = populate_storage_for_compaction(allocator, &storage_engine, 5);
        compaction_attempts += 1;

        if (result) |_| {
            // Success - compaction completed despite possible I/O errors
        } else |_| {
            compaction_failures += 1;
        }
    }

    // Disable fault injection for recovery verification
    sim_vfs.enable_io_failures(0, .{ .write = true });

    // Create new storage engine instance to simulate restart after crash
    var recovered_engine = try StorageEngine.init_default(allocator, vfs_interface, "test_db");
    defer recovered_engine.deinit();

    try recovered_engine.startup();

    // Verify data integrity after recovery
    // All originally stored blocks should be accessible
    for (0..50) |i| {
        const id = deterministic_block_id(@intCast(i));
        const result = recovered_engine.find_block_by_id(id);

        if (result) |found_block| {
            try testing.expect(found_block.id.eql(id));
            try testing.expect(found_block.version == 1);
        } else |err| {
            // Some blocks might be lost due to crash, but we should not get corruption errors
            const acceptable_errors = [_]anyerror{
                error.BlockNotFound,
            };

            var is_acceptable = false;
            for (acceptable_errors) |acceptable| {
                if (err == acceptable) {
                    is_acceptable = true;
                    break;
                }
            }
            try testing.expect(is_acceptable);
        }
    }

    // Storage should be operational after recovery
    const new_block = ContextBlock{
        .id = deterministic_block_id(99999),
        .version = 1,
        .source_uri = "test://recovery_verification",
        .metadata_json = "{\"recovery_test\":true}",
        .content = "Recovery verification block",
    };

    try recovered_engine.put_block(new_block);
    const retrieved = try recovered_engine.find_block_by_id(new_block.id);
    try testing.expect(retrieved.id.eql(new_block.id));
}

test "compaction crash - recovery with orphaned files" {
    concurrency.init();

    const allocator = testing.allocator;

    var sim_vfs = SimulationVFS.init_with_fault_seed(allocator, 54321);
    defer sim_vfs.deinit();

    const vfs_interface = sim_vfs.vfs();

    var storage_engine = try StorageEngine.init_default(allocator, vfs_interface, "test_db");
    defer storage_engine.deinit();

    try storage_engine.startup();
    try populate_storage_for_compaction(allocator, &storage_engine, 40);
    try storage_engine.flush_memtable();

    // Create a scenario where compaction creates new files but crashes before cleanup
    // This tests the cleanup of orphaned SSTable files during recovery

    // Add more data and force another flush to create multiple SSTables
    try populate_storage_for_compaction(allocator, &storage_engine, 40);
    try storage_engine.flush_memtable();

    // Check that we have multiple SSTable files before attempting compaction
    const initial_metrics = storage_engine.metrics();
    const initial_sstables = initial_metrics.sstables_count.load();
    try testing.expect(initial_sstables >= 2);

    // Simulate crash during file operations (remove phase of compaction)
    sim_vfs.enable_io_failures(300, .{ .remove = true }); // 30% failure rate on removes

    // Trigger operations that may cause compaction cleanup to fail
    for (0..5) |_| {
        // Adding data may trigger background compaction with potential cleanup failures
        _ = populate_storage_for_compaction(allocator, &storage_engine, 10) catch {};
    }

    // Disable fault injection and restart
    sim_vfs.enable_io_failures(0, .{ .remove = true });

    var recovered_engine = try StorageEngine.init_default(allocator, vfs_interface, "test_db");
    defer recovered_engine.deinit();

    try recovered_engine.startup();

    // Verify storage is consistent after recovery
    // System should have cleaned up any orphaned files and be operational
    const post_recovery_metrics = recovered_engine.metrics();
    const post_recovery_sstables = post_recovery_metrics.sstables_count.load();

    // Should have reasonable number of SSTables (not excessive due to orphaned files)
    try testing.expect(post_recovery_sstables < initial_sstables + 10);

    // Data should be accessible
    for (0..20) |i| {
        const id = deterministic_block_id(@intCast(i));
        // Not all blocks are guaranteed to survive crash, but lookups should not fail with corruption
        _ = recovered_engine.find_block_by_id(id) catch |err| {
            try testing.expect(err == error.BlockNotFound);
        };
    }

    // Engine should accept new writes
    const post_crash_block = ContextBlock{
        .id = deterministic_block_id(88888),
        .version = 1,
        .source_uri = "test://post_crash",
        .metadata_json = "{\"post_crash\":true}",
        .content = "Block written after crash recovery",
    };

    try recovered_engine.put_block(post_crash_block);
    const retrieved = try recovered_engine.find_block_by_id(post_crash_block.id);
    try testing.expect(retrieved.id.eql(post_crash_block.id));
}

test "compaction crash - multiple sequential crash recovery" {
    concurrency.init();

    const allocator = testing.allocator;

    var sim_vfs = SimulationVFS.init_with_fault_seed(allocator, 99999);
    defer sim_vfs.deinit();

    const vfs_interface = sim_vfs.vfs();

    // Test multiple crashes in sequence to ensure cumulative recovery works
    var crash_count: u32 = 0;
    const max_crashes = 3;

    while (crash_count < max_crashes) : (crash_count += 1) {
        var storage_engine = try StorageEngine.init_default(allocator, vfs_interface, "test_db");
        defer storage_engine.deinit();

        try storage_engine.startup();

        // Add data that may trigger compaction
        try populate_storage_for_compaction(allocator, &storage_engine, 25);
        try storage_engine.flush_memtable();

        // Configure different types of I/O failures for each crash iteration
        switch (crash_count) {
            0 => sim_vfs.enable_io_failures(400, .{ .write = true }), // Write failures
            1 => sim_vfs.enable_io_failures(400, .{ .sync = true }), // Sync failures
            2 => sim_vfs.enable_io_failures(400, .{ .remove = true }), // Remove failures
            else => unreachable,
        }

        // Attempt operations that trigger compaction with potential failures
        for (0..10) |_| {
            _ = populate_storage_for_compaction(allocator, &storage_engine, 5) catch {};
        }

        // Disable faults for next iteration
        sim_vfs.enable_io_failures(0, .{ .write = true, .sync = true, .remove = true });
    }

    // Final recovery test
    var final_engine = try StorageEngine.init_default(allocator, vfs_interface, "test_db");
    defer final_engine.deinit();

    try final_engine.startup();

    // System should be operational after multiple crashes
    const final_block = ContextBlock{
        .id = deterministic_block_id(77777),
        .version = 1,
        .source_uri = "test://multi_crash_survivor",
        .metadata_json = "{\"survived_multiple_crashes\":true}",
        .content = "This block was written after surviving multiple crashes",
    };

    try final_engine.put_block(final_block);
    const retrieved = try final_engine.find_block_by_id(final_block.id);
    try testing.expect(retrieved.id.eql(final_block.id));
    try testing.expectEqualStrings("test://multi_crash_survivor", retrieved.source_uri);

    // Verify metrics show reasonable state
    const final_metrics = final_engine.metrics();
    const final_blocks = final_metrics.blocks_written.load();
    try testing.expect(final_blocks >= 1); // At least our final test block
}

test "compaction crash - torn write recovery" {
    concurrency.init();

    const allocator = testing.allocator;

    var sim_vfs = SimulationVFS.init_with_fault_seed(allocator, 11111);
    defer sim_vfs.deinit();

    const vfs_interface = sim_vfs.vfs();

    var storage_engine = try StorageEngine.init_default(allocator, vfs_interface, "test_db");
    defer storage_engine.deinit();

    try storage_engine.startup();
    try populate_storage_for_compaction(allocator, &storage_engine, 30);

    // Enable torn writes to simulate power loss during compaction file writes
    sim_vfs.enable_torn_writes(800, 1, 70); // 80% probability, 70% completion max

    // Force compaction with torn writes
    try storage_engine.flush_memtable();

    for (0..20) |_| {
        _ = populate_storage_for_compaction(allocator, &storage_engine, 5) catch {};
    }

    // Disable torn writes for recovery
    sim_vfs.enable_torn_writes(0, 0, 0);

    // Recovery test
    var recovered_engine = try StorageEngine.init_default(allocator, vfs_interface, "test_db");
    defer recovered_engine.deinit();

    try recovered_engine.startup();

    // System should detect and handle corrupted files during recovery
    // Some data may be lost, but system should be operational
    var accessible_blocks: u32 = 0;
    for (0..30) |i| {
        const id = deterministic_block_id(@intCast(i));
        if (recovered_engine.find_block_by_id(id)) |_| {
            accessible_blocks += 1;
        } else |err| {
            try testing.expect(err == error.BlockNotFound);
        }
    }

    // Should have recovered at least some blocks
    try testing.expect(accessible_blocks > 0);

    // Engine should accept new writes after torn write recovery
    const recovery_block = ContextBlock{
        .id = deterministic_block_id(55555),
        .version = 1,
        .source_uri = "test://torn_write_recovery",
        .metadata_json = "{\"recovered_from_torn_write\":true}",
        .content = "Block written after recovering from torn writes",
    };

    try recovered_engine.put_block(recovery_block);
    const retrieved = try recovered_engine.find_block_by_id(recovery_block.id);
    try testing.expect(retrieved.id.eql(recovery_block.id));
}
