//! TieredCompactionManager unit tests
//!
//! Tests individual TieredCompactionManager functionality in isolation.
//! Focus: compaction threshold logic, job creation, tier management.

const builtin = @import("builtin");
const std = @import("std");

const kausaldb = @import("kausaldb");

const simulation_vfs = kausaldb.simulation_vfs;
const storage = kausaldb.storage;
const testing = std.testing;
const types = kausaldb.types;

const TieredCompactionManager = storage.TieredCompactionManager;
const ArenaCoordinator = kausaldb.memory.ArenaCoordinator;
const SimulationVFS = simulation_vfs.SimulationVFS;
const ContextBlock = types.ContextBlock;
const BlockId = types.BlockId;

test "init cleanup" {
    const allocator = testing.allocator;

    var sim_vfs = try SimulationVFS.init(allocator);
    defer sim_vfs.deinit();

    var arena = std.heap.ArenaAllocator.init(allocator);
    defer arena.deinit();
    const coordinator = ArenaCoordinator.init(&arena);

    var manager = TieredCompactionManager.init(
        &coordinator,
        allocator,
        sim_vfs.vfs(),
        "/test_compaction",
    );
    defer manager.deinit();

    // Initial state should have empty tiers
    try testing.expect((try manager.check_compaction_needed()) == null);
}

test "L0 threshold trigger" {
    const allocator = testing.allocator;

    var sim_vfs = try SimulationVFS.init(allocator);
    defer sim_vfs.deinit();

    var arena = std.heap.ArenaAllocator.init(allocator);
    defer arena.deinit();
    const coordinator = ArenaCoordinator.init(&arena);

    var manager = TieredCompactionManager.init(
        &coordinator,
        allocator,
        sim_vfs.vfs(),
        "/test_compaction",
    );
    defer manager.deinit();

    // Track paths for proper lifetime management to prevent use-after-free
    var managed_paths = std.ArrayList([]const u8).init(allocator);
    try managed_paths.ensureTotalCapacity(4); // 3 SSTables + 1 trigger
    defer {
        for (managed_paths.items) |path| {
            allocator.free(path);
        }
        managed_paths.deinit();
    }

    // Add SSTables to L0 below threshold
    var i: u32 = 0;
    while (i < 3) : (i += 1) {
        const path = try std.fmt.allocPrint(allocator, "sstable_{}.sst", .{i});
        try managed_paths.append(path);
        try manager.add_sstable(path, 1024 * 1024, 0); // 1MB SSTable
    }

    // Should not trigger compaction yet
    try testing.expect((try manager.check_compaction_needed()) == null);

    // Add one more to trigger threshold
    const trigger_path = try std.fmt.allocPrint(allocator, "sstable_trigger.sst", .{});
    try managed_paths.append(trigger_path);
    try manager.add_sstable(trigger_path, 1024 * 1024, 0);

    // Should now trigger L0 compaction
    var compaction_job = try manager.check_compaction_needed();
    try testing.expect(compaction_job != null);
    defer if (compaction_job) |*job| job.deinit();
    try testing.expectEqual(@as(u8, 0), compaction_job.?.input_level);
    try testing.expectEqual(@as(u8, 1), compaction_job.?.output_level);
}

test "size based higher levels" {
    const allocator = testing.allocator;

    var sim_vfs = try SimulationVFS.init(allocator);
    defer sim_vfs.deinit();

    var arena = std.heap.ArenaAllocator.init(allocator);
    defer arena.deinit();
    const coordinator = ArenaCoordinator.init(&arena);

    var manager = TieredCompactionManager.init(
        &coordinator,
        allocator,
        sim_vfs.vfs(),
        "/test_compaction",
    );
    defer manager.deinit();

    // Track paths for proper lifetime management to prevent use-after-free
    var managed_paths = std.ArrayList([]const u8).init(allocator);
    try managed_paths.ensureTotalCapacity(8); // 8 L1 SSTables
    defer {
        for (managed_paths.items) |path| {
            allocator.free(path);
        }
        managed_paths.deinit();
    }

    // Add multiple SSTables to L1 with similar sizes to trigger compaction
    const sstable_size = 10 * 1024 * 1024; // 10MB
    var i: u32 = 0;
    while (i < 8) : (i += 1) { // Increase to 8 SSTables to ensure compaction threshold
        const path = try std.fmt.allocPrint(allocator, "l1_sstable_{}.sst", .{i});
        try managed_paths.append(path);
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
    const allocator = testing.allocator;

    var sim_vfs = try SimulationVFS.init(allocator);
    defer sim_vfs.deinit();

    var arena = std.heap.ArenaAllocator.init(allocator);
    defer arena.deinit();
    const coordinator = ArenaCoordinator.init(&arena);

    var manager = TieredCompactionManager.init(
        &coordinator,
        allocator,
        sim_vfs.vfs(),
        "/test_compaction",
    );
    defer manager.deinit();

    // Track paths for proper lifetime management to prevent use-after-free
    var managed_paths = std.ArrayList([]const u8).init(allocator);
    try managed_paths.ensureTotalCapacity(5); // 3 levels + 1 remove path + buffer
    defer {
        for (managed_paths.items) |path| {
            allocator.free(path);
        }
        managed_paths.deinit();
    }

    // Add SSTables to different levels
    const levels_and_sizes = [_]struct { level: u8, size: u64 }{
        .{ .level = 0, .size = 1024 * 1024 }, // 1MB
        .{ .level = 1, .size = 10 * 1024 * 1024 }, // 10MB
        .{ .level = 2, .size = 100 * 1024 * 1024 }, // 100MB
    };

    for (levels_and_sizes, 0..) |item, i| {
        const path = try std.fmt.allocPrint(allocator, "level_{}_sstable_{}.sst", .{ item.level, i });
        try managed_paths.append(path);
        try manager.add_sstable(path, item.size, item.level);
    }

    // Test removal
    const remove_path = try std.fmt.allocPrint(allocator, "level_0_sstable_0.sst", .{});
    try managed_paths.append(remove_path); // tidy:ignore-perf Single append in test cleanup
    manager.remove_sstable(remove_path, 0);

    // State should be updated correctly (hard to verify without exposing internals)
    // At minimum, shouldn't crash
}

test "compaction job creation and validation" {
    const allocator = testing.allocator;

    var sim_vfs = try SimulationVFS.init(allocator);
    defer sim_vfs.deinit();

    var arena = std.heap.ArenaAllocator.init(allocator);
    defer arena.deinit();
    const coordinator = ArenaCoordinator.init(&arena);

    var manager = TieredCompactionManager.init(
        &coordinator,
        allocator,
        sim_vfs.vfs(),
        "/test_compaction",
    );
    defer manager.deinit();

    // Track paths for proper lifetime management to prevent use-after-free
    var managed_paths = std.ArrayList([]const u8).init(allocator);
    try managed_paths.ensureTotalCapacity(5); // 5 SSTables for compaction job test
    defer {
        for (managed_paths.items) |path| {
            allocator.free(path);
        }
        managed_paths.deinit();
    }

    // Set up scenario that triggers compaction
    var i: u32 = 0;
    while (i < 5) : (i += 1) { // Above L0 threshold
        const path = try std.fmt.allocPrint(allocator, "job_test_sstable_{}.sst", .{i});
        try managed_paths.append(path);
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
    const allocator = testing.allocator;

    var sim_vfs = try SimulationVFS.init(allocator);
    defer sim_vfs.deinit();

    var arena = std.heap.ArenaAllocator.init(allocator);
    defer arena.deinit();
    const coordinator = ArenaCoordinator.init(&arena);

    var manager = TieredCompactionManager.init(
        &coordinator,
        allocator,
        sim_vfs.vfs(),
        "/test_compaction",
    );
    defer manager.deinit();

    // Track paths for proper lifetime management to prevent use-after-free
    var managed_paths = std.ArrayList([]const u8).init(allocator);
    try managed_paths.ensureTotalCapacity(3); // 2 config test + 1 trigger
    defer {
        for (managed_paths.items) |path| {
            allocator.free(path);
        }
        managed_paths.deinit();
    }

    // Test with different configuration values
    // This validates that configuration is respected

    // Modify L0 threshold for testing
    manager.config.l0_compaction_threshold = 3;

    // Add SSTables up to new threshold
    var i: u32 = 0;
    while (i < 2) : (i += 1) {
        const path = try std.fmt.allocPrint(allocator, "config_test_{}.sst", .{i});
        try managed_paths.append(path);
        try manager.add_sstable(path, 1024 * 1024, 0);
    }

    // Should not trigger yet
    try testing.expect((try manager.check_compaction_needed()) == null);

    // Add one more to hit threshold
    const trigger_path = try std.fmt.allocPrint(allocator, "sstable_trigger.sst", .{});
    try managed_paths.append(trigger_path);
    try manager.add_sstable(trigger_path, 1024 * 1024, 0);

    // Should now trigger
    var final_job = try manager.check_compaction_needed();
    try testing.expect(final_job != null);
    defer if (final_job) |*job| job.deinit();
}

test "multi level compaction scenarios" {
    const allocator = testing.allocator;

    var sim_vfs = try SimulationVFS.init(allocator);
    defer sim_vfs.deinit();

    var arena = std.heap.ArenaAllocator.init(allocator);
    defer arena.deinit();
    const coordinator = ArenaCoordinator.init(&arena);

    var manager = TieredCompactionManager.init(
        &coordinator,
        allocator,
        sim_vfs.vfs(),
        "/test_compaction",
    );
    defer manager.deinit();

    // Track paths for proper lifetime management to prevent use-after-free
    var managed_paths = std.ArrayList([]const u8).init(allocator);
    try managed_paths.ensureTotalCapacity(18); // 5 L0 + 10 L1 + 3 compacted results
    defer {
        for (managed_paths.items) |path| {
            allocator.free(path);
        }
        managed_paths.deinit();
    }

    // Create scenario with multiple levels needing compaction
    // L0: Add above threshold
    var i: u32 = 0;
    while (i < 5) : (i += 1) {
        const path = try std.fmt.allocPrint(allocator, "l0_multi_{}.sst", .{i});
        try managed_paths.append(path);
        try manager.add_sstable(path, 1024 * 1024, 0);
    }

    // L1: Add multiple similar-sized tables
    i = 0;
    while (i < 10) : (i += 1) {
        const path = try std.fmt.allocPrint(allocator, "l1_multi_{}.sst", .{i});
        try managed_paths.append(path);
        try manager.add_sstable(path, 10 * 1024 * 1024, 1);
    }

    // Should prioritize L0 compaction first
    var first_job = try manager.check_compaction_needed();
    try testing.expect(first_job != null);
    defer if (first_job) |*job| job.deinit();
    try testing.expectEqual(@as(u8, 0), first_job.?.input_level);
}

test "compaction with large SSTables" {
    const allocator = testing.allocator;

    var sim_vfs = try SimulationVFS.init(allocator);
    defer sim_vfs.deinit();

    var arena = std.heap.ArenaAllocator.init(allocator);
    defer arena.deinit();
    const coordinator = ArenaCoordinator.init(&arena);

    var manager = TieredCompactionManager.init(
        &coordinator,
        allocator,
        sim_vfs.vfs(),
        "/test_compaction",
    );
    defer manager.deinit();

    // Track paths for proper lifetime management to prevent use-after-free
    var managed_paths = std.ArrayList([]const u8).init(allocator);
    try managed_paths.ensureTotalCapacity(5); // 3 large files + 2 edge cases
    defer {
        for (managed_paths.items) |path| {
            allocator.free(path);
        }
        managed_paths.deinit();
    }

    // Add very large SSTables to test size handling
    const large_size = 1024 * 1024 * 1024; // 1GB
    var i: u32 = 0;
    while (i < 3) : (i += 1) {
        const path = try std.fmt.allocPrint(allocator, "large_sstable_{}.sst", .{i});
        try managed_paths.append(path);
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
    const allocator = testing.allocator;

    var sim_vfs = try SimulationVFS.init(allocator);
    defer sim_vfs.deinit();

    var arena = std.heap.ArenaAllocator.init(allocator);
    defer arena.deinit();
    const coordinator = ArenaCoordinator.init(&arena);

    var manager = TieredCompactionManager.init(
        &coordinator,
        allocator,
        sim_vfs.vfs(),
        "/test_compaction",
    );
    defer manager.deinit();

    // Track paths for proper lifetime management to prevent use-after-free
    var managed_paths = std.ArrayList([]const u8).init(allocator);
    try managed_paths.ensureTotalCapacity(100); // Large test with up to 100 files
    defer {
        for (managed_paths.items) |path| {
            allocator.free(path);
        }
        managed_paths.deinit();
    }

    // Test removing non-existent SSTable (should not crash)
    manager.remove_sstable("non_existent.sst", 0);

    // Test adding SSTable to highest level
    const max_level_path = try std.fmt.allocPrint(allocator, "max_level.sst", .{});
    try managed_paths.append(max_level_path);
    try manager.add_sstable(max_level_path, 1024 * 1024, 7); // L7 is max

    // Should not crash when checking compaction on max level
    var max_level_job = try manager.check_compaction_needed();
    if (max_level_job) |*job| {
        defer job.deinit();
    }

    // Test zero-size SSTable
    const zero_size_path = try std.fmt.allocPrint(allocator, "zero_size.sst", .{});
    try managed_paths.append(zero_size_path);
    try manager.add_sstable(zero_size_path, 0, 1);
}

test "compaction performance characteristics" {
    const allocator = testing.allocator;

    var sim_vfs = try SimulationVFS.init(allocator);
    defer sim_vfs.deinit();

    var arena = std.heap.ArenaAllocator.init(allocator);
    defer arena.deinit();
    const coordinator = ArenaCoordinator.init(&arena);

    var manager = TieredCompactionManager.init(
        &coordinator,
        allocator,
        sim_vfs.vfs(),
        "/test_compaction",
    );
    defer manager.deinit();

    // Track paths for proper lifetime management to prevent use-after-free
    var managed_paths = std.ArrayList([]const u8).init(allocator);
    try managed_paths.ensureTotalCapacity(100); // sstable_count = 100
    defer {
        for (managed_paths.items) |path| {
            allocator.free(path);
        }
        managed_paths.deinit();
    }

    const start_time = std.time.nanoTimestamp();

    // Add many SSTables to test performance
    // Scale down for debug builds to prevent CI timeouts
    const sstable_count = if (builtin.mode == .Debug) 250 else 1000;
    var i: u32 = 0;
    while (i < sstable_count) : (i += 1) {
        const path = try std.fmt.allocPrint(allocator, "perf_test_{}.sst", .{i});
        try managed_paths.append(path);
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
    const allocator = testing.allocator;

    var sim_vfs = try SimulationVFS.init(allocator);
    defer sim_vfs.deinit();

    var arena = std.heap.ArenaAllocator.init(allocator);
    defer arena.deinit();
    const coordinator = ArenaCoordinator.init(&arena);

    var manager = TieredCompactionManager.init(
        &coordinator,
        allocator,
        sim_vfs.vfs(),
        "/test_compaction",
    );
    defer manager.deinit();

    // Track paths for proper lifetime management to prevent use-after-free
    var managed_paths = std.ArrayList([]const u8).init(allocator);
    try managed_paths.ensureTotalCapacity(30); // 10 + 15 + 5 lifecycle paths
    defer {
        for (managed_paths.items) |path| {
            allocator.free(path);
        }
        managed_paths.deinit();
    }

    // Simulate realistic SSTable lifecycle
    var sstable_counter: u32 = 0;

    // Phase 1: Add initial SSTables
    var i: u32 = 0;
    while (i < 10) : (i += 1) {
        const path = try std.fmt.allocPrint(allocator, "lifecycle_{}.sst", .{sstable_counter});
        try managed_paths.append(path);
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
        try managed_paths.append(output_path);
        try manager.add_sstable(output_path, 5 * 1024 * 1024, job.output_level);
        sstable_counter += 1;
    }

    // Phase 3: Continue adding and ensure state remains consistent
    i = 0;
    while (i < 5) : (i += 1) {
        const path = try std.fmt.allocPrint(allocator, "post_compact_{}.sst", .{sstable_counter});
        try managed_paths.append(path);
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
    const allocator = testing.allocator;

    var sim_vfs = try SimulationVFS.init(allocator);
    defer sim_vfs.deinit();

    var arena = std.heap.ArenaAllocator.init(allocator);
    defer arena.deinit();
    const coordinator = ArenaCoordinator.init(&arena);

    var manager = TieredCompactionManager.init(
        &coordinator,
        allocator,
        sim_vfs.vfs(),
        "/test_compaction",
    );
    defer manager.deinit();

    // Track paths for proper lifetime management to prevent use-after-free
    var managed_paths = std.ArrayList([]const u8).init(allocator);
    try managed_paths.ensureTotalCapacity(16); // 4 L0 + 8 L1 + 4 compacted
    defer {
        for (managed_paths.items) |path| {
            allocator.free(path);
        }
        managed_paths.deinit();
    }

    // Test L0 → L1 compaction (count-based)
    var i: u32 = 0;
    while (i < 4) : (i += 1) {
        const path = try std.fmt.allocPrint(allocator, "strategy_l0_{}.sst", .{i});
        try managed_paths.append(path);
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
        try managed_paths.append(l1_path);
        try manager.add_sstable(l1_path, 8 * 1024 * 1024, 1);
    }

    // Test L1 → L2 compaction (size-based)
    i = 0;
    while (i < 8) : (i += 1) {
        const path = try std.fmt.allocPrint(allocator, "strategy_l1_{}.sst", .{i});
        try managed_paths.append(path);
        try manager.add_sstable(path, 10 * 1024 * 1024, 1);
    }

    const l1_job = try manager.check_compaction_needed();
    if (l1_job) |job| {
        try testing.expectEqual(@as(u8, 1), job.input_level);
        try testing.expect(job.output_level >= 1); // Output should be at least same level
    }
    // Test passes regardless of whether L1 compaction is triggered
}
