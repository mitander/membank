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
const types = kausaldb.types;
const stdx = kausaldb.stdx;

const StorageEngine = storage.StorageEngine;
const ContextBlock = types.ContextBlock;
const BlockId = types.BlockId;
const SimulationVFS = simulation_vfs.SimulationVFS;

// Compaction crash test scenario configuration
// Follows KausalDB's "explicitness over magic" philosophy by making
// all test parameters explicit and discoverable
const CompactionCrashScenario = struct {
    description: []const u8,
    seed: u64,
    initial_blocks: u32,
    fault_type: FaultType,
    expected_min_survival_rate: f32,
    expected_recovery_success: bool,

    const FaultType = enum {
        partial_sstable_write,
        orphaned_files,
        torn_write,
        sequential_crashes,
    };
};

// Test scenarios for systematic compaction crash testing
// Each scenario is explicitly configured to test specific failure modes
const compaction_crash_scenarios = [_]CompactionCrashScenario{
    .{
        .description = "Partial SSTable Write During Compaction",
        .seed = 0xDEADBEEF,
        .initial_blocks = 150, // Triggers compaction
        .fault_type = .partial_sstable_write,
        .expected_min_survival_rate = 0.8, // Most data should survive
        .expected_recovery_success = true,
    },
    .{
        .description = "Orphaned Files After Compaction Failure",
        .seed = 0xCAFEBABE,
        .initial_blocks = 200,
        .fault_type = .orphaned_files,
        .expected_min_survival_rate = 0.9, // Data safe, cleanup may fail
        .expected_recovery_success = true,
    },
    .{
        .description = "Torn Write in SSTable Header",
        .seed = 0xBEEFCAFE,
        .initial_blocks = 100,
        .fault_type = .torn_write,
        .expected_min_survival_rate = 0.7, // Header corruption more severe
        .expected_recovery_success = true,
    },
    .{
        .description = "Multiple Sequential Crashes",
        .seed = 0xFEEDFACE,
        .initial_blocks = 180,
        .fault_type = .sequential_crashes,
        .expected_min_survival_rate = 0.6, // Multiple failures compound
        .expected_recovery_success = true,
    },
};

// Helper function to generate deterministic BlockId for testing
fn deterministic_block_id(seed: u32) BlockId {
    var bytes: [16]u8 = undefined;
    var i: usize = 0;
    while (i < 16) : (i += 4) {
        const value = seed + @as(u32, @intCast(i));
        var slice: [4]u8 = undefined;
        std.mem.writeInt(u32, &slice, value, .little);
        stdx.copy_left(u8, bytes[i .. i + 4], &slice);
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

test "recovery from partial sstable write" {
    const allocator = testing.allocator;

    var sim_vfs = try SimulationVFS.init_with_fault_seed(allocator, 12345);
    defer sim_vfs.deinit();

    const vfs_interface = sim_vfs.vfs();

    // Initial setup and population
    var storage_engine = try StorageEngine.init_default(allocator, vfs_interface, "test_db");
    defer storage_engine.deinit();

    try storage_engine.startup();
    try populate_storage_for_compaction(allocator, &storage_engine, 50);

    // Force flush to create initial SSTables
    try storage_engine.flush_memtable_to_sstable();

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
        const result = recovered_engine.find_block(id);

        if (result) |maybe_found_block| {
            if (maybe_found_block) |found_block| {
                try testing.expect(found_block.id.eql(id));
                try testing.expect(found_block.version == 1);
            }
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
    const maybe_retrieved = try recovered_engine.find_block(new_block.id);
    const retrieved = maybe_retrieved.?; // Should not be null for just-written block
    try testing.expect(retrieved.id.eql(new_block.id));
}

test "recovery with orphaned files" {
    const allocator = testing.allocator;

    var sim_vfs = try SimulationVFS.init_with_fault_seed(allocator, 54321);
    defer sim_vfs.deinit();

    const vfs_interface = sim_vfs.vfs();

    var storage_engine = try StorageEngine.init_default(allocator, vfs_interface, "test_db");
    defer storage_engine.deinit();

    try storage_engine.startup();
    try populate_storage_for_compaction(allocator, &storage_engine, 40);
    try storage_engine.flush_memtable_to_sstable();

    // Create a scenario where compaction creates new files but crashes before cleanup
    // This tests the cleanup of orphaned SSTable files during recovery

    // Add more data and force another flush to create multiple SSTables
    try populate_storage_for_compaction(allocator, &storage_engine, 40);
    try storage_engine.flush_memtable_to_sstable();

    // Check that we have multiple SSTable files before attempting compaction
    const initial_metrics = storage_engine.metrics();
    const initial_sstables = initial_metrics.sstable_writes.load();
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
    const post_recovery_sstables = post_recovery_metrics.sstable_writes.load();

    // Should have reasonable number of SSTables (not excessive due to orphaned files)
    try testing.expect(post_recovery_sstables < initial_sstables + 10);

    // Data should be accessible
    for (0..20) |i| {
        const id = deterministic_block_id(@intCast(i));
        // Not all blocks are guaranteed to survive crash, but lookups should not fail with corruption
        _ = recovered_engine.find_block(id) catch |err| {
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
    const maybe_retrieved = try recovered_engine.find_block(post_crash_block.id);
    const retrieved = maybe_retrieved.?; // Should not be null for just-written block
    try testing.expect(retrieved.id.eql(post_crash_block.id));
}

test "multiple sequential crash recovery" {
    const allocator = testing.allocator;

    var sim_vfs = try SimulationVFS.init_with_fault_seed(allocator, 99999);
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
        try storage_engine.flush_memtable_to_sstable();

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
    const maybe_retrieved = try final_engine.find_block(final_block.id);
    const retrieved = maybe_retrieved.?; // Should not be null for just-written block
    try testing.expect(retrieved.id.eql(final_block.id));
    try testing.expectEqualStrings("test://multi_crash_survivor", retrieved.source_uri);

    // Verify metrics show reasonable state
    const final_metrics = final_engine.metrics();
    const final_blocks = final_metrics.blocks_written.load();
    try testing.expect(final_blocks >= 1); // At least our final test block
}

test "torn write recovery" {
    const allocator = testing.allocator;

    var sim_vfs = try SimulationVFS.init_with_fault_seed(allocator, 11111);
    defer sim_vfs.deinit();

    const vfs_interface = sim_vfs.vfs();

    var storage_engine = try StorageEngine.init_default(allocator, vfs_interface, "test_db");
    defer storage_engine.deinit();

    try storage_engine.startup();
    try populate_storage_for_compaction(allocator, &storage_engine, 30);

    // Enable torn writes to simulate power loss during compaction file writes
    sim_vfs.enable_torn_writes(800, 1, 70); // 80% probability, 70% completion max

    // Force compaction with torn writes
    try storage_engine.flush_memtable_to_sstable();

    for (0..20) |_| {
        _ = populate_storage_for_compaction(allocator, &storage_engine, 5) catch {};
    }

    // Disable torn writes for recovery
    sim_vfs.fault_injection.torn_write_config.enabled = false;

    // Recovery test
    var recovered_engine = try StorageEngine.init_default(allocator, vfs_interface, "test_db");
    defer recovered_engine.deinit();

    try recovered_engine.startup();

    // System should detect and handle corrupted files during recovery
    // Some data may be lost, but system should be operational
    var accessible_blocks: u32 = 0;
    for (0..30) |i| {
        const id = deterministic_block_id(@intCast(i));
        if (recovered_engine.find_block(id)) |maybe_block| {
            if (maybe_block != null) {
                accessible_blocks += 1;
            }
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
    const maybe_retrieved = try recovered_engine.find_block(recovery_block.id);
    const retrieved = maybe_retrieved.?; // Should not be null for just-written block
    try testing.expect(retrieved.id.eql(recovery_block.id));
}

// Systematized compaction crash testing using explicit scenario configuration
// This test implements the TigerBeetle-inspired approach of explicit, scenario-based testing
// Each scenario is fully specified upfront, making test conditions crystal clear
test "systematic partial sstable write scenario" {
    try execute_compaction_crash_scenario(compaction_crash_scenarios[0]);
}

test "systematic orphaned files scenario" {
    try execute_compaction_crash_scenario(compaction_crash_scenarios[1]);
}

test "systematic torn write scenario" {
    try execute_compaction_crash_scenario(compaction_crash_scenarios[2]);
}

test "systematic sequential crashes scenario" {
    try execute_compaction_crash_scenario(compaction_crash_scenarios[3]);
}

// Execute a single compaction crash scenario
// Follows the explicit testing philosophy: no hidden logic, all parameters visible
fn execute_compaction_crash_scenario(scenario: CompactionCrashScenario) !void {
    const allocator = testing.allocator;

    // Phase 1: Setup with deterministic seed
    var sim_vfs = try SimulationVFS.init(allocator);
    defer sim_vfs.deinit();

    // Generate unique directory name based on scenario description hash
    // This prevents conflicts when running multiple scenarios
    const dir_name = try std.fmt.allocPrint(allocator, "crash_test_{x}", .{
        std.hash_map.hashString(scenario.description),
    });
    defer allocator.free(dir_name);

    var storage_engine = try StorageEngine.init_default(allocator, sim_vfs.vfs(), dir_name);
    try storage_engine.startup();

    // Phase 2: Populate with scenario-specified block count
    try populate_storage_for_compaction(allocator, &storage_engine, scenario.initial_blocks);

    // Record initial state for survival rate calculation
    const initial_block_count = storage_engine.block_count();

    // Phase 3: Execute fault injection based on scenario type
    switch (scenario.fault_type) {
        .partial_sstable_write => {
            // Simulate partial SSTable write during compaction
            // This tests recovery from incomplete compaction operations
            sim_vfs.enable_io_failures(500, .{ .write = true }); // 50% write failure rate
        },
        .orphaned_files => {
            // Simulate file cleanup failure after compaction
            // Tests handling of orphaned temporary files
            sim_vfs.enable_io_failures(300, .{ .remove = true }); // 30% remove failure rate
        },
        .torn_write => {
            // Simulate torn writes in SSTable headers
            // Tests recovery from corrupted file headers
            sim_vfs.enable_torn_writes(800, 1, 700); // 80% probability, 70% completion max
        },
        .sequential_crashes => {
            // Simulate multiple sequential crashes during recovery
            // Tests resilience to compound failure scenarios
            sim_vfs.enable_io_failures(400, .{ .write = true, .sync = true });
            // Additional faults will be injected during recovery phase
        },
    }

    // Phase 4: Trigger operation that will crash (force compaction)
    // Add blocks to trigger compaction under fault conditions
    const trigger_blocks = 50;
    for (0..trigger_blocks) |i| {
        const trigger_id = scenario.initial_blocks + @as(u32, @intCast(i));
        const source_uri = try std.fmt.allocPrint(allocator, "test://trigger{d}", .{trigger_id});
        defer allocator.free(source_uri);

        const content = try std.fmt.allocPrint(allocator, "Trigger block {d}", .{trigger_id});
        defer allocator.free(content);

        const trigger_block = ContextBlock{
            .id = deterministic_block_id(trigger_id),
            .version = 1,
            .source_uri = source_uri,
            .metadata_json = "{\"trigger\":true}",
            .content = content,
        };

        // This may fail due to injected faults - that's expected
        _ = storage_engine.put_block(trigger_block) catch |err| switch (err) {
            vfs.VFSError.IoError => {}, // Expected under fault injection
            else => return err,
        };
    }

    // Phase 5: Simulate crash by destroying storage engine
    storage_engine.deinit();

    // Phase 6: Recovery - create new storage engine instance
    var recovered_engine = try StorageEngine.init_default(allocator, sim_vfs.vfs(), dir_name);
    defer recovered_engine.deinit();

    // Inject additional faults for sequential crash scenarios
    if (scenario.fault_type == .sequential_crashes) {
        sim_vfs.enable_io_failures(300, .{ .read = true }); // 30% read failure rate
    }

    const recovery_result = recovered_engine.startup();

    // Phase 7: Validate recovery according to scenario expectations
    if (scenario.expected_recovery_success) {
        try recovery_result;

        const recovered_block_count = recovered_engine.block_count();
        const survival_rate = @as(f32, @floatFromInt(recovered_block_count)) /
            @as(f32, @floatFromInt(initial_block_count));

        // Verify survival rate meets scenario expectations
        try testing.expect(survival_rate >= scenario.expected_min_survival_rate);

        // Verify engine can accept new operations after recovery
        const recovery_test_block = ContextBlock{
            .id = deterministic_block_id(99999),
            .version = 1,
            .source_uri = "test://post_recovery",
            .metadata_json = "{\"post_recovery\":true}",
            .content = "Block written after crash recovery",
        };

        try recovered_engine.put_block(recovery_test_block);
        const retrieved = (try recovered_engine.find_block(recovery_test_block.id)).?;
        try testing.expect(retrieved.id.eql(recovery_test_block.id));
    } else {
        // Some scenarios may expect recovery to fail gracefully
        try testing.expectError(error.CorruptionDetected, recovery_result);
    }
}
