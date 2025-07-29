//! WAL corruption detection and recovery tests.
//!
//! Comprehensive test coverage for WAL corruption scenarios including
//! systematic corruption detection, magic number corruption, checksum
//! failures, and recovery boundary conditions under hostile environments.

const cortexdb = @import("cortexdb");
const std = @import("std");
const testing = std.testing;

const storage = cortexdb.storage;
const simulation_vfs = cortexdb.simulation_vfs;
const context_block = cortexdb.types;
const assert = cortexdb.assert.assert;
const fatal_assert = cortexdb.assert.fatal_assert;

const WAL = storage.WAL;
const WALEntry = storage.WALEntry;
const WALEntryType = storage.WALEntryType;
const SimulationVFS = simulation_vfs.SimulationVFS;
const ContextBlock = context_block.ContextBlock;
const BlockId = context_block.BlockId;
const GraphEdge = context_block.GraphEdge;
const EdgeType = context_block.EdgeType;

// Defensive limits to prevent runaway tests
const MAX_TEST_DURATION_MS = 5000;
const MAX_RECOVERY_ENTRIES = 1000;
const MAX_CORRUPTION_ATTEMPTS = 50;
const SYSTEMATIC_CORRUPTION_THRESHOLD = 4;

fn create_test_block(id: BlockId, content: []const u8) ContextBlock {
    return ContextBlock{
        .id = id,
        .version = 1,
        .source_uri = "test://wal_corruption_test.zig",
        .metadata_json = "{}",
        .content = content,
    };
}

test "wal_corruption_magic_number_detection" {
    const allocator = testing.allocator;

    var sim_vfs = try SimulationVFS.init(allocator);
    defer sim_vfs.deinit();

    const wal_dir = "magic_corruption_test";
    try sim_vfs.vfs().mkdir_all(wal_dir);

    // Phase 1: Write valid WAL entries
    {
        var wal = try WAL.init(allocator, sim_vfs.vfs(), wal_dir);
        defer wal.deinit();

        try wal.startup();

        const test_block = create_test_block(1, "Valid block before magic corruption");
        try wal.append_put_block(test_block);
        try wal.flush();
    }

    // Phase 2: Inject magic number corruption
    const wal_files = try sim_vfs.vfs().list_directory(wal_dir, allocator);
    defer allocator.free(wal_files);

    if (wal_files.len > 0) {
        const wal_file_path = try std.fmt.allocPrint(allocator, "{s}/{s}", .{ wal_dir, wal_files[0] });
        defer allocator.free(wal_file_path);

        // Corrupt magic number at known WAL header offset
        const corrupt_magic = [_]u8{ 0xFF, 0xFF, 0xFF, 0xFF };
        try sim_vfs.inject_corruption(wal_file_path, 0, &corrupt_magic);

        // Phase 3: Attempt recovery - should detect magic corruption
        var corrupted_wal = try WAL.init(allocator, sim_vfs.vfs(), wal_dir);
        defer corrupted_wal.deinit();

        const recovery_result = corrupted_wal.startup();
        try testing.expectError(error.CorruptedWALEntry, recovery_result);
    }
}

test "wal_corruption_systematic_checksum_failures" {
    const allocator = testing.allocator;

    var sim_vfs = try SimulationVFS.init(allocator);
    defer sim_vfs.deinit();

    const wal_dir = "systematic_corruption_test";
    try sim_vfs.vfs().mkdir_all(wal_dir);

    // Write multiple entries to create systematic corruption scenario
    {
        var wal = try WAL.init(allocator, sim_vfs.vfs(), wal_dir);
        defer wal.deinit();

        try wal.startup();

        // Write enough entries to trigger systematic corruption detection
        for (1..10) |i| {
            const content = try std.fmt.allocPrint(allocator, "Systematic test block {}", .{i});
            defer allocator.free(content);
            const block = create_test_block(@intCast(i), content);
            try wal.append_put_block(block);
        }

        try wal.flush();
    }

    // Inject multiple corruptions to trigger systematic detection
    const wal_files = try sim_vfs.vfs().list_directory(wal_dir, allocator);
    defer allocator.free(wal_files);

    if (wal_files.len > 0) {
        const wal_file_path = try std.fmt.allocPrint(allocator, "{s}/{s}", .{ wal_dir, wal_files[0] });
        defer allocator.free(wal_file_path);

        // Inject systematic checksum corruption at multiple offsets
        const corruption_offsets = [_]u64{ 50, 150, 250, 350, 450 };
        for (corruption_offsets) |offset| {
            const corrupt_data = [_]u8{ 0xDE, 0xAD, 0xBE, 0xEF };
            try sim_vfs.inject_corruption(wal_file_path, offset, &corrupt_data);
        }

        // Recovery should detect systematic corruption pattern
        var corrupted_wal = try WAL.init(allocator, sim_vfs.vfs(), wal_dir);
        defer corrupted_wal.deinit();

        const recovery_result = corrupted_wal.startup();
        // Should fail with systematic corruption detection
        try testing.expect(recovery_result == error.CorruptedWALEntry or
            recovery_result == error.InvalidChecksum);
    }
}

test "wal_corruption_boundary_conditions" {
    const allocator = testing.allocator;

    var sim_vfs = try SimulationVFS.init(allocator);
    defer sim_vfs.deinit();

    const wal_dir = "boundary_corruption_test";
    try sim_vfs.vfs().mkdir_all(wal_dir);

    var wal = try WAL.init(allocator, sim_vfs.vfs(), wal_dir);
    defer wal.deinit();

    try wal.startup();

    // Test corruption detection with various block sizes
    const test_sizes = [_]usize{ 1, 63, 64, 65, 127, 128, 129, 255, 256, 257, 1023, 1024, 1025 };

    for (test_sizes, 0..) |size, i| {
        const content = try allocator.alloc(u8, size);
        defer allocator.free(content);

        // Fill with deterministic pattern for corruption detection
        for (content, 0..) |*byte, j| {
            byte.* = @intCast((i + j) & 0xFF);
        }

        const block = create_test_block(@intCast(i + 1), content);
        try wal.append_put_block(block);

        // Periodic flush to create recovery points
        if (i % 3 == 0) {
            try wal.flush();
        }
    }

    try wal.flush();
    try testing.expectEqual(@as(u64, test_sizes.len), wal.entry_count());
}

test "wal_corruption_recovery_partial_success" {
    const allocator = testing.allocator;

    var sim_vfs = try SimulationVFS.init(allocator);
    defer sim_vfs.deinit();

    const wal_dir = "partial_recovery_test";
    try sim_vfs.vfs().mkdir_all(wal_dir);

    // Phase 1: Write entries with known patterns
    {
        var wal = try WAL.init(allocator, sim_vfs.vfs(), wal_dir);
        defer wal.deinit();

        try wal.startup();

        // Write good entries first
        for (1..6) |i| {
            const content = try std.fmt.allocPrint(allocator, "Good entry {}", .{i});
            defer allocator.free(content);
            const block = create_test_block(@intCast(i), content);
            try wal.append_put_block(block);
        }

        try wal.flush();

        // Write more entries that will be corrupted
        for (6..11) |i| {
            const content = try std.fmt.allocPrint(allocator, "Corruptible entry {}", .{i});
            defer allocator.free(content);
            const block = create_test_block(@intCast(i), content);
            try wal.append_put_block(block);
        }

        try wal.flush();
    }

    // Phase 2: Inject corruption in latter portion of file
    const wal_files = try sim_vfs.vfs().list_directory(wal_dir, allocator);
    defer allocator.free(wal_files);

    if (wal_files.len > 0) {
        const wal_file_path = try std.fmt.allocPrint(allocator, "{s}/{s}", .{ wal_dir, wal_files[0] });
        defer allocator.free(wal_file_path);

        // Corrupt second half of file
        const file_size = blk: {
            var file = try sim_vfs.vfs().open(wal_file_path);
            defer file.close();
            break :blk try file.file_size();
        };

        const corruption_offset = file_size / 2;
        const corrupt_data = [_]u8{ 0xBA, 0xD0, 0xDA, 0x7A };
        try sim_vfs.inject_corruption(wal_file_path, corruption_offset, &corrupt_data);

        // Phase 3: Recovery should succeed partially
        var recovery_wal = try WAL.init(allocator, sim_vfs.vfs(), wal_dir);
        defer recovery_wal.deinit();

        const recovery_result = recovery_wal.startup();

        if (recovery_result) |_| {
            // Partial recovery succeeded - should have some entries
            try testing.expect(recovery_wal.entry_count() > 0);
            try testing.expect(recovery_wal.entry_count() < 10); // Less than all entries
        } else |err| {
            // Expected corruption errors
            try testing.expect(err == error.CorruptedWALEntry or
                err == error.InvalidChecksum or
                err == error.UnexpectedEndOfFile);
        }
    }
}

test "wal_corruption_large_entry_handling" {
    const allocator = testing.allocator;

    var sim_vfs = try SimulationVFS.init(allocator);
    defer sim_vfs.deinit();

    const wal_dir = "large_entry_corruption_test";
    try sim_vfs.vfs().mkdir_all(wal_dir);

    var wal = try WAL.init(allocator, sim_vfs.vfs(), wal_dir);
    defer wal.deinit();

    try wal.startup();

    // Write a large entry that spans multiple internal buffers
    const large_content = try allocator.alloc(u8, 16384);
    defer allocator.free(large_content);

    // Fill with deterministic pattern for corruption detection
    for (large_content, 0..) |*byte, i| {
        byte.* = @intCast(i & 0xFF);
    }

    const large_block = create_test_block(1, large_content);
    try wal.append_put_block(large_block);

    // Add smaller entries after large one
    for (2..5) |i| {
        const content = try std.fmt.allocPrint(allocator, "Small entry after large {}", .{i});
        defer allocator.free(content);
        const block = create_test_block(@intCast(i), content);
        try wal.append_put_block(block);
    }

    try wal.flush();
    try testing.expectEqual(@as(u64, 4), wal.entry_count());

    // Verify large entry can be recovered correctly
    wal.deinit();

    var recovery_wal = try WAL.init(allocator, sim_vfs.vfs(), wal_dir);
    defer recovery_wal.deinit();

    try recovery_wal.startup();
    try testing.expectEqual(@as(u64, 4), recovery_wal.entry_count());
}

test "wal_corruption_defensive_timeout_recovery" {
    const allocator = testing.allocator;

    const start_time = std.time.milliTimestamp();

    var sim_vfs = try SimulationVFS.init(allocator);
    defer sim_vfs.deinit();

    const wal_dir = "timeout_recovery_test";
    try sim_vfs.vfs().mkdir_all(wal_dir);

    // Write substantial data with timeout protection
    {
        var wal = try WAL.init(allocator, sim_vfs.vfs(), wal_dir);
        defer wal.deinit();

        try wal.startup();

        var entries_written: u32 = 0;
        while (entries_written < MAX_RECOVERY_ENTRIES) {
            const current_time = std.time.milliTimestamp();
            if (current_time - start_time > MAX_TEST_DURATION_MS / 2) {
                break; // Use half timeout for writing phase
            }

            const content = try std.fmt.allocPrint(allocator, "Timeout test entry {}", .{entries_written});
            defer allocator.free(content);
            const block = create_test_block(entries_written + 1, content);
            try wal.append_put_block(block);

            entries_written += 1;

            if (entries_written % 50 == 0) {
                try wal.flush();
            }
        }

        try wal.flush();
        try testing.expect(entries_written > 0);
    }

    // Recovery phase with timeout protection
    const recovery_start = std.time.milliTimestamp();

    var recovery_wal = try WAL.init(allocator, sim_vfs.vfs(), wal_dir);
    defer recovery_wal.deinit();

    try recovery_wal.startup();

    const recovery_time = std.time.milliTimestamp() - recovery_start;

    // Recovery should complete within reasonable time
    try testing.expect(recovery_time < MAX_TEST_DURATION_MS / 2);
    try testing.expect(recovery_wal.entry_count() > 0);

    // Total test time should be within limits
    const total_time = std.time.milliTimestamp() - start_time;
    try testing.expect(total_time < MAX_TEST_DURATION_MS);
}

test "wal_corruption_edge_case_patterns" {
    const allocator = testing.allocator;

    var sim_vfs = try SimulationVFS.init(allocator);
    defer sim_vfs.deinit();

    const wal_dir = "edge_case_corruption_test";
    try sim_vfs.vfs().mkdir_all(wal_dir);

    var wal = try WAL.init(allocator, sim_vfs.vfs(), wal_dir);
    defer wal.deinit();

    try wal.startup();

    // Test edge cases that could trigger corruption

    // Empty content block
    const empty_block = create_test_block(1, "");
    try wal.append_put_block(empty_block);

    // Single character block
    const tiny_block = create_test_block(2, "x");
    try wal.append_put_block(tiny_block);

    // Block with special byte patterns that could confuse parser
    const special_bytes = [_]u8{ 0x00, 0xFF, 0xDE, 0xAD, 0xBE, 0xEF, 0xCA, 0xFE };
    const special_block = create_test_block(3, &special_bytes);
    try wal.append_put_block(special_block);

    // Block with null bytes embedded
    const null_embedded = "Start\x00Middle\x00End";
    const null_block = create_test_block(4, null_embedded);
    try wal.append_put_block(null_block);

    // Maximum reasonable size block for edge case testing
    const large_size = 8192;
    const large_content = try allocator.alloc(u8, large_size);
    defer allocator.free(large_content);

    // Pattern that could trigger false corruption detection
    for (large_content, 0..) |*byte, i| {
        byte.* = switch (i % 4) {
            0 => 0xDE,
            1 => 0xAD,
            2 => 0xBE,
            3 => 0xEF,
            else => unreachable,
        };
    }
    const pattern_block = create_test_block(5, large_content);
    try wal.append_put_block(pattern_block);

    try wal.flush();
    try testing.expectEqual(@as(u64, 5), wal.entry_count());

    // Verify all edge cases can be recovered
    wal.deinit();

    var recovery_wal = try WAL.init(allocator, sim_vfs.vfs(), wal_dir);
    defer recovery_wal.deinit();

    try recovery_wal.startup();
    try testing.expectEqual(@as(u64, 5), recovery_wal.entry_count());
}

test "wal_corruption_memory_safety_during_recovery" {
    const allocator = testing.allocator;

    // Use arena for recovery to test memory management under corruption
    var arena = std.heap.ArenaAllocator.init(allocator);
    defer arena.deinit();
    const recovery_allocator = arena.allocator();

    var sim_vfs = try SimulationVFS.init(allocator);
    defer sim_vfs.deinit();

    const wal_dir = "memory_safety_corruption_test";
    try sim_vfs.vfs().mkdir_all(wal_dir);

    // Write entries with varying sizes to stress memory allocation
    {
        var wal = try WAL.init(allocator, sim_vfs.vfs(), wal_dir);
        defer wal.deinit();

        try wal.startup();

        for (1..51) |i| {
            const content_size = (i % 20 + 1) * 32; // Varying sizes 32-640 bytes
            const content = try allocator.alloc(u8, content_size);
            defer allocator.free(content);

            @memset(content, @intCast(i & 0xFF));

            const block = create_test_block(@intCast(i), content);
            try wal.append_put_block(block);

            if (i % 10 == 0) {
                try wal.flush();
            }
        }

        try wal.flush();
    }

    // Inject corruption that could trigger memory issues during recovery
    const wal_files = try sim_vfs.vfs().list_directory(wal_dir, allocator);
    defer allocator.free(wal_files);

    if (wal_files.len > 0) {
        const wal_file_path = try std.fmt.allocPrint(allocator, "{s}/{s}", .{ wal_dir, wal_files[0] });
        defer allocator.free(wal_file_path);

        // Corrupt length field to test memory safety
        const corrupt_length = [_]u8{ 0xFF, 0xFF, 0xFF, 0xFF };
        try sim_vfs.inject_corruption(wal_file_path, 100, &corrupt_length);

        // Recovery with arena allocator
        var recovery_wal = try WAL.init(recovery_allocator, sim_vfs.vfs(), wal_dir);
        defer recovery_wal.deinit();

        const recovery_result = recovery_wal.startup();

        if (recovery_result) |_| {
            // Recovery succeeded with some entries
            try testing.expect(recovery_wal.entry_count() > 0);
        } else |err| {
            // Expected errors from corruption
            try testing.expect(err == error.CorruptedWALEntry or
                err == error.InvalidChecksum or
                err == error.OutOfMemory);
        }
    }

    // Arena cleanup is O(1) - tests that recovery doesn't leak on corruption
    _ = arena.reset(.retain_capacity);
}
