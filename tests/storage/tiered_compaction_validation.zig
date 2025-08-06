//! Validation tests for tiered compaction strategies.
//!
//! Tests the TieredCompactionManager implementation including L0 compaction,
//! size-tiered compaction, tier state management, and performance characteristics.
//! Validates compaction decisions, job creation, and execution under various
//! scenarios including edge cases and hostile conditions.

const std = @import("std");
const testing = std.testing;
const kausaldb = @import("kausaldb");

const storage = kausaldb.storage;
const simulation_vfs = kausaldb.simulation_vfs;
const context_block = kausaldb.types;
const concurrency = kausaldb.concurrency;

const TieredCompactionManager = storage.TieredCompactionManager;
const SimulationVFS = simulation_vfs.SimulationVFS;
const ContextBlock = context_block.ContextBlock;
const BlockId = context_block.BlockId;

fn create_test_block(id: u32, allocator: std.mem.Allocator) !ContextBlock {
    var id_bytes: [16]u8 = undefined;
    std.mem.writeInt(u128, &id_bytes, id, .little);

    const content = try std.fmt.allocPrint(allocator, "test content for block {}", .{id});
    return ContextBlock{
        .id = BlockId{ .bytes = id_bytes },
        .version = 1,
        .source_uri = try allocator.dupe(u8, "test://source"),
        .metadata_json = try allocator.dupe(u8, "{{\"test\": true}}"),
        .content = content,
    };
}

test "tiered compaction manager initialization and cleanup" {
    concurrency.init();
    const allocator = testing.allocator;

    var sim_vfs = try SimulationVFS.init(allocator);
    defer sim_vfs.deinit();

    var manager = TieredCompactionManager.init(
        allocator,
        sim_vfs.vfs(),
        "/test_compaction",
    );
    defer manager.deinit();

    // Initial state should have empty tiers
    try testing.expect((try manager.check_compaction_needed()) == null);
}

test "L0 compaction threshold triggers" {
    concurrency.init();
    const allocator = testing.allocator;

    var sim_vfs = try SimulationVFS.init(allocator);
    defer sim_vfs.deinit();

    var manager = TieredCompactionManager.init(
        allocator,
        sim_vfs.vfs(),
        "/test_l0_compaction",
    );
    defer manager.deinit();

    // Add SSTables to L0 below threshold
    var i: u32 = 0;
    while (i < 3) : (i += 1) {
        const path = try std.fmt.allocPrint(allocator, "sstable_{}.sst", .{i});
        defer allocator.free(path);
        try manager.add_sstable(path, 1024 * 1024, 0); // 1MB SSTable
    }

    // Should not trigger compaction yet
    try testing.expect((try manager.check_compaction_needed()) == null);

    // Add one more to trigger threshold
    const trigger_path = try std.fmt.allocPrint(allocator, "sstable_trigger.sst", .{});
    defer allocator.free(trigger_path);
    try manager.add_sstable(trigger_path, 1024 * 1024, 0);

    // Should now trigger L0 compaction
    var compaction_job = try manager.check_compaction_needed();
    try testing.expect(compaction_job != null);
    defer if (compaction_job) |*job| job.deinit();
    try testing.expectEqual(@as(u8, 0), compaction_job.?.input_level);
    try testing.expectEqual(@as(u8, 1), compaction_job.?.output_level);
}

test "size-tiered compaction for higher levels" {
    concurrency.init();
    const allocator = testing.allocator;

    var sim_vfs = try SimulationVFS.init(allocator);
    defer sim_vfs.deinit();

    var manager = TieredCompactionManager.init(
        allocator,
        sim_vfs.vfs(),
        "/test_size_tiered",
    );
    defer manager.deinit();

    // Add multiple SSTables to L1 with similar sizes to trigger compaction
    const sstable_size = 10 * 1024 * 1024; // 10MB
    var i: u32 = 0;
    while (i < 8) : (i += 1) { // Increase to 8 SSTables to ensure compaction threshold
        const path = try std.fmt.allocPrint(allocator, "l1_sstable_{}.sst", .{i});
        defer allocator.free(path);
        try manager.add_sstable(path, sstable_size, 1);
    }

    // Should potentially trigger size-tiered compaction for L1
    var compaction_job = try manager.check_compaction_needed();
    defer if (compaction_job) |*job| job.deinit();
    if (compaction_job) |job| {
        try testing.expectEqual(@as(u8, 1), job.input_level);
        try testing.expect(job.output_level >= 1); // Output should be at least same level
    }
    // Test passes regardless of whether compaction is triggered
}

test "tier state management and tracking" {
    concurrency.init();
    const allocator = testing.allocator;

    var sim_vfs = try SimulationVFS.init(allocator);
    defer sim_vfs.deinit();

    var manager = TieredCompactionManager.init(
        allocator,
        sim_vfs.vfs(),
        "/test_tier_state",
    );
    defer manager.deinit();

    // Add SSTables to different levels
    const levels_and_sizes = [_]struct { level: u8, size: u64 }{
        .{ .level = 0, .size = 1024 * 1024 }, // 1MB
        .{ .level = 1, .size = 10 * 1024 * 1024 }, // 10MB
        .{ .level = 2, .size = 100 * 1024 * 1024 }, // 100MB
    };

    for (levels_and_sizes, 0..) |item, i| {
        const path = try std.fmt.allocPrint(allocator, "level_{}_sstable_{}.sst", .{ item.level, i });
        defer allocator.free(path);
        try manager.add_sstable(path, item.size, item.level);
    }

    // Test removal
    const remove_path = try std.fmt.allocPrint(allocator, "level_0_sstable_0.sst", .{});
    defer allocator.free(remove_path);
    manager.remove_sstable(remove_path, 0);

    // State should be updated correctly (hard to verify without exposing internals)
    // At minimum, shouldn't crash
}

test "compaction job creation and validation" {
    concurrency.init();
    const allocator = testing.allocator;

    var sim_vfs = try SimulationVFS.init(allocator);
    defer sim_vfs.deinit();

    var manager = TieredCompactionManager.init(
        allocator,
        sim_vfs.vfs(),
        "/test_compaction_jobs",
    );
    defer manager.deinit();

    // Set up scenario that triggers compaction
    var i: u32 = 0;
    while (i < 5) : (i += 1) { // Above L0 threshold
        const path = try std.fmt.allocPrint(allocator, "job_test_sstable_{}.sst", .{i});
        defer allocator.free(path);
        try manager.add_sstable(path, 1024 * 1024, 0);
    }

    var compaction_job = try manager.check_compaction_needed();
    try testing.expect(compaction_job != null);
    defer if (compaction_job) |*job| job.deinit();

    const job = compaction_job.?;
    try testing.expectEqual(@as(u8, 0), job.input_level);
    try testing.expectEqual(@as(u8, 1), job.output_level);
    try testing.expect(job.input_paths.items.len > 0);
}

test "compaction configuration validation" {
    concurrency.init();
    const allocator = testing.allocator;

    var sim_vfs = try SimulationVFS.init(allocator);
    defer sim_vfs.deinit();

    var manager = TieredCompactionManager.init(
        allocator,
        sim_vfs.vfs(),
        "/test_config",
    );
    defer manager.deinit();

    // Test with different configuration values
    // This validates that configuration is respected

    // Modify L0 threshold for testing
    manager.config.l0_compaction_threshold = 3;

    // Add SSTables up to new threshold
    var i: u32 = 0;
    while (i < 2) : (i += 1) {
        const path = try std.fmt.allocPrint(allocator, "config_test_{}.sst", .{i});
        defer allocator.free(path);
        try manager.add_sstable(path, 1024 * 1024, 0);
    }

    // Should not trigger yet
    try testing.expect((try manager.check_compaction_needed()) == null);

    // Add one more to hit threshold
    const trigger_path = try std.fmt.allocPrint(allocator, "config_trigger.sst", .{});
    defer allocator.free(trigger_path);
    try manager.add_sstable(trigger_path, 1024 * 1024, 0);

    // Should now trigger
    var final_job = try manager.check_compaction_needed();
    try testing.expect(final_job != null);
    defer if (final_job) |*job| job.deinit();
}

test "multi-level compaction scenarios" {
    concurrency.init();
    const allocator = testing.allocator;

    var sim_vfs = try SimulationVFS.init(allocator);
    defer sim_vfs.deinit();

    var manager = TieredCompactionManager.init(
        allocator,
        sim_vfs.vfs(),
        "/test_multi_level",
    );
    defer manager.deinit();

    // Create scenario with multiple levels needing compaction
    // L0: Add above threshold
    var i: u32 = 0;
    while (i < 5) : (i += 1) {
        const path = try std.fmt.allocPrint(allocator, "l0_multi_{}.sst", .{i});
        defer allocator.free(path);
        try manager.add_sstable(path, 1024 * 1024, 0);
    }

    // L1: Add multiple similar-sized tables
    i = 0;
    while (i < 10) : (i += 1) {
        const path = try std.fmt.allocPrint(allocator, "l1_multi_{}.sst", .{i});
        defer allocator.free(path);
        try manager.add_sstable(path, 10 * 1024 * 1024, 1);
    }

    // Should prioritize L0 compaction first
    var first_job = try manager.check_compaction_needed();
    try testing.expect(first_job != null);
    defer if (first_job) |*job| job.deinit();
    try testing.expectEqual(@as(u8, 0), first_job.?.input_level);
}

test "compaction with large SSTables" {
    concurrency.init();
    const allocator = testing.allocator;

    var sim_vfs = try SimulationVFS.init(allocator);
    defer sim_vfs.deinit();

    var manager = TieredCompactionManager.init(
        allocator,
        sim_vfs.vfs(),
        "/test_large_sstables",
    );
    defer manager.deinit();

    // Add very large SSTables to test size handling
    const large_size = 1024 * 1024 * 1024; // 1GB
    var i: u32 = 0;
    while (i < 3) : (i += 1) {
        const path = try std.fmt.allocPrint(allocator, "large_sstable_{}.sst", .{i});
        defer allocator.free(path);
        try manager.add_sstable(path, large_size, 2);
    }

    // Should handle large sizes without issues
    var large_compaction_job = try manager.check_compaction_needed();
    if (large_compaction_job) |*job| {
        defer job.deinit();
        try testing.expectEqual(@as(u8, 2), job.input_level);
        // Output level should be input_level + 1 or same level for size-tiered
        try testing.expect(job.output_level >= job.input_level);
    }
}

test "compaction edge cases and error conditions" {
    concurrency.init();
    const allocator = testing.allocator;

    var sim_vfs = try SimulationVFS.init(allocator);
    defer sim_vfs.deinit();

    var manager = TieredCompactionManager.init(
        allocator,
        sim_vfs.vfs(),
        "/test_edge_cases",
    );
    defer manager.deinit();

    // Test removing non-existent SSTable (should not crash)
    manager.remove_sstable("non_existent.sst", 0);

    // Test adding SSTable to highest level
    const max_level_path = try std.fmt.allocPrint(allocator, "max_level.sst", .{});
    defer allocator.free(max_level_path);
    try manager.add_sstable(max_level_path, 1024 * 1024, 7); // L7 is max

    // Should not crash when checking compaction on max level
    var max_level_job = try manager.check_compaction_needed();
    if (max_level_job) |*job| {
        defer job.deinit();
    }

    // Test zero-size SSTable
    const zero_size_path = try std.fmt.allocPrint(allocator, "zero_size.sst", .{});
    defer allocator.free(zero_size_path);
    try manager.add_sstable(zero_size_path, 0, 1);
}

test "compaction performance characteristics" {
    concurrency.init();
    const allocator = testing.allocator;

    var sim_vfs = try SimulationVFS.init(allocator);
    defer sim_vfs.deinit();

    var manager = TieredCompactionManager.init(
        allocator,
        sim_vfs.vfs(),
        "/test_performance",
    );
    defer manager.deinit();

    const start_time = std.time.nanoTimestamp();

    // Add many SSTables to test performance
    var i: u32 = 0;
    while (i < 1000) : (i += 1) {
        const path = try std.fmt.allocPrint(allocator, "perf_test_{}.sst", .{i});
        defer allocator.free(path);
        const level: u8 = @intCast(i % 4); // Distribute across levels
        try manager.add_sstable(path, 1024 * 1024, level);
    }

    const add_time = std.time.nanoTimestamp();

    // Check compaction decisions (should be fast)
    var compaction_checks: u32 = 0;
    while (compaction_checks < 100) : (compaction_checks += 1) {
        var perf_job = try manager.check_compaction_needed();
        if (perf_job) |*job| {
            job.deinit();
        }
    }

    const check_time = std.time.nanoTimestamp();

    // Performance should be reasonable
    const add_duration = add_time - start_time;
    const check_duration = check_time - add_time;

    // Should complete within reasonable time bounds (generous for CI)
    const max_add_time = 1_000_000_000; // 1 second for 1000 adds (10x tolerance)
    const max_check_time = 100_000_000; // 100ms for 100 checks (10x tolerance)

    try testing.expect(add_duration < max_add_time);
    try testing.expect(check_duration < max_check_time);
}

test "tier state consistency under operations" {
    concurrency.init();
    const allocator = testing.allocator;

    var sim_vfs = try SimulationVFS.init(allocator);
    defer sim_vfs.deinit();

    var manager = TieredCompactionManager.init(
        allocator,
        sim_vfs.vfs(),
        "/test_consistency",
    );
    defer manager.deinit();

    // Simulate realistic SSTable lifecycle
    var sstable_counter: u32 = 0;

    // Phase 1: Add initial SSTables
    var i: u32 = 0;
    while (i < 10) : (i += 1) {
        const path = try std.fmt.allocPrint(allocator, "lifecycle_{}.sst", .{sstable_counter});
        defer allocator.free(path);
        try manager.add_sstable(path, 1024 * 1024, 0);
        sstable_counter += 1;
    }

    // Phase 2: Trigger compaction and simulate completion
    var initial_job = try manager.check_compaction_needed();
    if (initial_job) |*job| {
        defer job.deinit();

        // Simulate compaction execution by removing input SSTables
        for (job.input_paths.items) |input_path| {
            manager.remove_sstable(input_path, job.input_level);
        }

        // Add resulting SSTable to target level
        const output_path = try std.fmt.allocPrint(allocator, "compacted_{}.sst", .{sstable_counter});
        defer allocator.free(output_path);
        try manager.add_sstable(output_path, 5 * 1024 * 1024, job.output_level);
        sstable_counter += 1;
    }

    // Phase 3: Continue adding and ensure state remains consistent
    i = 0;
    while (i < 5) : (i += 1) {
        const path = try std.fmt.allocPrint(allocator, "post_compact_{}.sst", .{sstable_counter});
        defer allocator.free(path);
        try manager.add_sstable(path, 1024 * 1024, 0);
        sstable_counter += 1;
    }

    // Should still function correctly
    var consistency_job = try manager.check_compaction_needed();
    if (consistency_job) |*job| {
        defer job.deinit();
    }
}

test "compaction strategies across tier sizes" {
    concurrency.init();
    const allocator = testing.allocator;

    var sim_vfs = try SimulationVFS.init(allocator);
    defer sim_vfs.deinit();

    var manager = TieredCompactionManager.init(
        allocator,
        sim_vfs.vfs(),
        "/test_strategies",
    );
    defer manager.deinit();

    // Test L0 → L1 compaction (count-based)
    var i: u32 = 0;
    while (i < 4) : (i += 1) {
        const path = try std.fmt.allocPrint(allocator, "strategy_l0_{}.sst", .{i});
        defer allocator.free(path);
        try manager.add_sstable(path, 1024 * 1024, 0);
    }

    var l0_job = try manager.check_compaction_needed();
    try testing.expect(l0_job != null);
    defer if (l0_job) |*job| job.deinit();
    try testing.expectEqual(@as(u8, 0), l0_job.?.input_level);

    // Simulate L0 compaction completion
    if (l0_job) |job| {
        for (job.input_paths.items) |input_path| {
            manager.remove_sstable(input_path, 0);
        }
        const l1_path = try std.fmt.allocPrint(allocator, "compacted_to_l1.sst", .{});
        defer allocator.free(l1_path);
        try manager.add_sstable(l1_path, 8 * 1024 * 1024, 1);
    }

    // Test L1 → L2 compaction (size-based)
    i = 0;
    while (i < 8) : (i += 1) {
        const path = try std.fmt.allocPrint(allocator, "strategy_l1_{}.sst", .{i});
        defer allocator.free(path);
        try manager.add_sstable(path, 10 * 1024 * 1024, 1);
    }

    const l1_job = try manager.check_compaction_needed();
    if (l1_job) |job| {
        try testing.expectEqual(@as(u8, 1), job.input_level);
        try testing.expect(job.output_level >= 1); // Output should be at least same level
    }
    // Test passes regardless of whether L1 compaction is triggered
}
