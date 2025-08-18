//! WAL segment header and boundary corruption recovery tests.
//!
//! Tests critical WAL corruption scenarios not covered by existing tests:
//! 1. Segment header magic number corruption
//! 2. Cross-segment entry corruption (entries spanning boundaries)
//! 3. Inter-segment boundary corruption during transitions
//!
//! These scenarios represent real-world corruption that could cause
//! silent data loss if not properly detected and handled.

const kausaldb = @import("kausaldb");
const std = @import("std");
const testing = std.testing;

const assert = kausaldb.assert.assert;
const types = kausaldb.types;
const storage = kausaldb.storage;
const simulation = kausaldb.simulation;

const StorageEngine = storage.StorageEngine;
const WAL = storage.WAL;
const WALEntry = storage.WALEntry;
const WALEntryType = storage.WALEntryType;
const ContextBlock = types.ContextBlock;
const BlockId = types.BlockId;
const TestData = kausaldb.TestData;
const StorageHarness = kausaldb.StorageHarness;

// WAL constants from corruption_tracker.zig
const WAL_MAGIC_NUMBER: u32 = 0x574C4147; // "WLAG"
const WAL_ENTRY_MAGIC: u32 = 0x57454E54; // "WENT"
const MAX_SEGMENT_SIZE: u64 = 64 * 1024 * 1024; // 64MB

test "segment header magic corruption detection" {
    const allocator = testing.allocator;

    var harness = try kausaldb.StorageHarness.init_and_startup(allocator, "segment_header_test");
    defer harness.deinit();

    const wal_dir = "segment_header_test";
    try harness.sim_vfs.vfs().mkdir_all(wal_dir);

    // Phase 1: Write valid WAL data to create segment file
    {
        var wal = try WAL.init(allocator, harness.sim_vfs.vfs(), wal_dir);
        defer wal.deinit();
        try wal.startup();

        const test_block = try TestData.create_test_block_with_content(allocator, 1, "Test content for segment header corruption");
        defer {
            allocator.free(test_block.source_uri);
            allocator.free(test_block.metadata_json);
            allocator.free(test_block.content);
        }

        const entry = try WALEntry.create_put_block(allocator, test_block);
        defer entry.deinit(allocator);
        try wal.write_entry(entry);
    }

    // Phase 2: Find and corrupt segment header magic number
    var dir_iterator = try harness.sim_vfs.vfs().iterate_directory(wal_dir, allocator);
    defer dir_iterator.deinit(allocator);

    var segment_file_path: ?[]u8 = null;
    defer if (segment_file_path) |path| allocator.free(path);

    while (dir_iterator.next()) |entry| {
        if (std.mem.endsWith(u8, entry.name, ".log")) {
            segment_file_path = try std.fmt.allocPrint(allocator, "{s}/{s}", .{ wal_dir, entry.name });
            break;
        }
    }

    try testing.expect(segment_file_path != null);

    if (segment_file_path) |path| {
        // Corrupt segment header magic at offset 0
        const corrupt_magic = [_]u8{ 0xDE, 0xAD, 0xBE, 0xEF };
        var file = try harness.sim_vfs.vfs().open(path, .write);
        defer file.close();
        _ = try file.write_at(0, &corrupt_magic);

        // Phase 3: Attempt recovery - should detect segment header corruption
        var corrupted_wal = try WAL.init(allocator, harness.sim_vfs.vfs(), wal_dir);
        defer corrupted_wal.deinit();

        const recovery_result = corrupted_wal.startup();
        if (recovery_result) |_| {
            // If recovery succeeds, verify it detected and handled corruption gracefully
            const stats = corrupted_wal.statistics();
            try testing.expect(stats.recovery_failures > 0);
        } else |err| {
            // Expected corruption detection
            try testing.expect(err == error.CorruptedWALEntry or
                err == error.InvalidChecksum or
                err == error.IoError);
        }
    }
}

test "cross-segment entry corruption recovery" {
    const allocator = testing.allocator;

    var harness = try kausaldb.StorageHarness.init_and_startup(allocator, "cross_segment_test");
    defer harness.deinit();

    const wal_dir = "cross_segment_test";
    try harness.sim_vfs.vfs().mkdir_all(wal_dir);

    // Phase 1: Write enough data to trigger segment rotation
    // Create entries that will span segment boundary
    {
        var wal = try WAL.init(allocator, harness.sim_vfs.vfs(), wal_dir);
        defer wal.deinit();
        try wal.startup();

        // Write large blocks to approach segment size limit
        const large_content_size = 1024 * 1024; // 1MB per block
        var large_content = try allocator.alloc(u8, large_content_size);
        defer allocator.free(large_content);
        std.mem.set(u8, large_content, 'A');

        // Write enough blocks to trigger segment rotation
        const blocks_per_segment = MAX_SEGMENT_SIZE / (large_content_size + 1024); // Estimate with header overhead
        var i: u32 = 0;
        while (i < blocks_per_segment + 5) : (i += 1) { // Extra blocks to ensure rotation
            const test_block = try TestData.create_test_block_with_content(allocator, i, large_content);
            defer {
                allocator.free(test_block.source_uri);
                allocator.free(test_block.metadata_json);
                allocator.free(test_block.content);
            }

            const entry = try WALEntry.create_put_block(allocator, test_block);
            defer entry.deinit(allocator);

            wal.write_entry(entry) catch |err| {
                // May fail due to segment rotation - this is expected
                if (err != error.SegmentFull) {
                    return err;
                }
                break;
            };
        }
    }

    // Phase 2: Find segment files and corrupt data at boundary
    var dir_iterator = try harness.sim_vfs.vfs().iterate_directory(wal_dir, allocator);
    defer dir_iterator.deinit(allocator);

    var segment_files = std.ArrayList([]u8).init(allocator);
    defer {
        for (segment_files.items) |path| {
            allocator.free(path);
        }
        segment_files.deinit();
    }

    while (dir_iterator.next()) |entry| {
        if (std.mem.endsWith(u8, entry.name, ".log")) {
            const path = try std.fmt.allocPrint(allocator, "{s}/{s}", .{ wal_dir, entry.name });
            try segment_files.append(path);
        }
    }

    // Should have multiple segments due to rotation
    if (segment_files.items.len >= 2) {
        // Corrupt the second segment's first entry magic number
        const second_segment = segment_files.items[1];
        var file = try harness.sim_vfs.vfs().open(second_segment, .write);
        defer file.close();

        // Find first entry after segment header and corrupt its magic
        const segment_header_size = 16; // Approximate segment header size
        const corrupt_entry_magic = [_]u8{ 0xBA, 0xD0, 0xDA, 0xD0 };
        _ = try file.write_at(segment_header_size, &corrupt_entry_magic);

        // Phase 3: Attempt recovery across segments
        var corrupted_wal = try WAL.init(allocator, harness.sim_vfs.vfs(), wal_dir);
        defer corrupted_wal.deinit();

        const recovery_result = corrupted_wal.startup();
        if (recovery_result) |_| {
            // Recovery should handle cross-segment corruption gracefully
            const stats = corrupted_wal.statistics();
            try testing.expect(stats.recovery_failures > 0);
            // Should recover some entries from first segment even if second is corrupted
            try testing.expect(stats.entries_recovered > 0);
        } else |err| {
            // Expected cross-segment corruption detection
            try testing.expect(err == error.CorruptedWALEntry or
                err == error.InvalidChecksum or
                err == error.InvalidEntryType);
        }
    }
}

test "inter-segment boundary checksum corruption" {
    const allocator = testing.allocator;

    var harness = try kausaldb.StorageHarness.init_and_startup(allocator, "boundary_checksum_test");
    defer harness.deinit();

    const wal_dir = "boundary_checksum_test";
    try harness.sim_vfs.vfs().mkdir_all(wal_dir);

    // Phase 1: Create WAL with multiple segments
    {
        var wal = try WAL.init(allocator, harness.sim_vfs.vfs(), wal_dir);
        defer wal.deinit();
        try wal.startup();

        // Write medium-sized blocks to create predictable segment boundaries
        const content_size = 512 * 1024; // 512KB per block
        var content = try allocator.alloc(u8, content_size);
        defer allocator.free(content);

        // Fill with pattern that makes corruption detection easier
        for (content, 0..) |*byte, i| {
            byte.* = @intCast(i % 256);
        }

        // Write enough to create 2-3 segments
        var i: u32 = 0;
        while (i < 150) : (i += 1) { // Should create multiple segments
            const test_block = try TestData.create_test_block_with_content(allocator, i, content);
            defer {
                allocator.free(test_block.source_uri);
                allocator.free(test_block.metadata_json);
                allocator.free(test_block.content);
            }

            const entry = try WALEntry.create_put_block(allocator, test_block);
            defer entry.deinit(allocator);

            wal.write_entry(entry) catch |err| {
                if (err == error.SegmentFull) {
                    break; // Expected segment rotation
                }
                return err;
            };
        }
    }

    // Phase 2: Corrupt checksums near segment boundaries
    var dir_iterator = try harness.sim_vfs.vfs().iterate_directory(wal_dir, allocator);
    defer dir_iterator.deinit(allocator);

    var segment_files = std.ArrayList([]u8).init(allocator);
    defer {
        for (segment_files.items) |path| {
            allocator.free(path);
        }
        segment_files.deinit();
    }

    while (dir_iterator.next()) |entry| {
        if (std.mem.endsWith(u8, entry.name, ".log")) {
            const path = try std.fmt.allocPrint(allocator, "{s}/{s}", .{ wal_dir, entry.name });
            try segment_files.append(path);
        }
    }

    if (segment_files.items.len >= 2) {
        // Corrupt checksum field in the last entry of first segment
        const first_segment = segment_files.items[0];
        var file = try harness.sim_vfs.vfs().open(first_segment, .write);
        defer file.close();

        // Find end of file and corrupt checksum in last entry
        const file_size = try file.get_size();
        if (file_size > 16) { // Ensure we have data to corrupt
            // Corrupt bytes near end of segment (likely checksum area)
            const corruption_offset = file_size - 12; // Checksum typically near end
            const corrupt_checksum = [_]u8{ 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF };
            _ = try file.write_at(corruption_offset, &corrupt_checksum);
        }

        // Phase 3: Test recovery with boundary checksum corruption
        var corrupted_wal = try WAL.init(allocator, harness.sim_vfs.vfs(), wal_dir);
        defer corrupted_wal.deinit();

        const recovery_result = corrupted_wal.startup();
        if (recovery_result) |_| {
            // Recovery should detect checksum corruption at boundaries
            const stats = corrupted_wal.statistics();

            // Should have some recovery failures due to corruption
            try testing.expect(stats.recovery_failures > 0);

            // Should recover at least some entries before corruption point
            try testing.expect(stats.entries_recovered > 0);

            // Should have processed multiple segments
            try testing.expect(stats.segments_rotated > 0);
        } else |err| {
            // Expected checksum corruption detection
            try testing.expect(err == error.InvalidChecksum or
                err == error.CorruptedWALEntry or
                err == error.IoError);
        }
    }
}

test "segment rotation during corruption scenario" {
    const allocator = testing.allocator;

    var harness = try kausaldb.StorageHarness.init_and_startup(allocator, "rotation_corruption_test");
    defer harness.deinit();

    const wal_dir = "rotation_corruption_test";
    try harness.sim_vfs.vfs().mkdir_all(wal_dir);

    // Phase 1: Create WAL and prepare to corrupt during rotation
    var wal = try WAL.init(allocator, harness.sim_vfs.vfs(), wal_dir);
    defer wal.deinit();
    try wal.startup();

    // Write some valid entries first
    var i: u32 = 0;
    while (i < 10) : (i += 1) {
        const test_block = try TestData.create_test_block_with_content(allocator, i, "Valid entry before corruption test");
        defer {
            allocator.free(test_block.source_uri);
            allocator.free(test_block.metadata_json);
            allocator.free(test_block.content);
        }

        const entry = try WALEntry.create_put_block(allocator, test_block);
        defer entry.deinit(allocator);
        try wal.write_entry(entry);
    }

    // Phase 2: Directly corrupt the active segment file
    const stats_before = wal.statistics();

    // Find current segment file
    var dir_iterator = try harness.sim_vfs.vfs().iterate_directory(wal_dir, allocator);
    defer dir_iterator.deinit(allocator);

    var current_segment: ?[]u8 = null;
    defer if (current_segment) |path| allocator.free(path);

    while (dir_iterator.next()) |entry| {
        if (std.mem.endsWith(u8, entry.name, ".log")) {
            current_segment = try std.fmt.allocPrint(allocator, "{s}/{s}", .{ wal_dir, entry.name });
            break;
        }
    }

    if (current_segment) |path| {
        // Corrupt middle of the file to simulate torn write during rotation
        var file = try harness.sim_vfs.vfs().open(path, .write);
        defer file.close();

        const file_size = try file.get_size();
        if (file_size > 100) {
            const corruption_offset = file_size / 2; // Middle corruption
            const torn_write = [_]u8{ 0x00, 0x00, 0x00, 0x00, 0x55, 0xAA, 0x55, 0xAA };
            _ = try file.write_at(corruption_offset, &torn_write);
        }

        // Phase 3: Try to write more entries after corruption
        const write_result = blk: {
            var j: u32 = 0;
            while (j < 5) : (j += 1) {
                const test_block = try TestData.create_test_block_with_content(allocator, j + 100, "Entry after corruption");
                defer {
                    allocator.free(test_block.source_uri);
                    allocator.free(test_block.metadata_json);
                    allocator.free(test_block.content);
                }

                const entry = try WALEntry.create_put_block(allocator, test_block);
                defer entry.deinit(allocator);

                if (wal.write_entry(entry)) |_| {
                    // Success
                } else |err| {
                    break :blk err;
                }
            }
            break :blk @as(?anyerror, null);
        };

        // Verify WAL either:
        // 1. Successfully handled corruption and continued writing, OR
        // 2. Detected corruption and failed gracefully
        const stats_after = wal.statistics();

        if (write_result == null) {
            // WAL successfully wrote after corruption - verify stats updated
            try testing.expect(stats_after.entries_written > stats_before.entries_written);
        } else {
            // WAL detected corruption - this is also valid behavior
            const expected_errors = [_]anyerror{ error.CorruptedWALEntry, error.InvalidChecksum, error.IoError, error.SegmentFull };

            var found_expected = false;
            for (expected_errors) |expected| {
                if (write_result.? == expected) {
                    found_expected = true;
                    break;
                }
            }
            try testing.expect(found_expected);
        }
    }
}

test "partial segment recovery with mixed corruption" {
    const allocator = testing.allocator;

    var harness = try kausaldb.StorageHarness.init_and_startup(allocator, "mixed_corruption_test");
    defer harness.deinit();

    const wal_dir = "mixed_corruption_test";
    try harness.sim_vfs.vfs().mkdir_all(wal_dir);

    // Phase 1: Create WAL with known good entries
    const num_good_entries = 20;
    {
        var wal = try WAL.init(allocator, harness.sim_vfs.vfs(), wal_dir);
        defer wal.deinit();
        try wal.startup();

        var i: u32 = 0;
        while (i < num_good_entries) : (i += 1) {
            const test_block = try TestData.create_test_block_with_content(allocator, i, "Good entry for partial recovery test");
            defer {
                allocator.free(test_block.source_uri);
                allocator.free(test_block.metadata_json);
                allocator.free(test_block.content);
            }

            const entry = try WALEntry.create_put_block(allocator, test_block);
            defer entry.deinit(allocator);
            try wal.write_entry(entry);
        }
    }

    // Phase 2: Inject mixed corruption patterns
    var dir_iterator = try harness.sim_vfs.vfs().iterate_directory(wal_dir, allocator);
    defer dir_iterator.deinit(allocator);

    var segment_path: ?[]u8 = null;
    defer if (segment_path) |path| allocator.free(path);

    while (dir_iterator.next()) |entry| {
        if (std.mem.endsWith(u8, entry.name, ".log")) {
            segment_path = try std.fmt.allocPrint(allocator, "{s}/{s}", .{ wal_dir, entry.name });
            break;
        }
    }

    if (segment_path) |path| {
        var file = try harness.sim_vfs.vfs().open(path, .write);
        defer file.close();

        const file_size = try file.get_size();

        // Inject multiple corruption types at different offsets
        if (file_size > 1000) {
            // Corruption 1: Entry magic number (early in file)
            const offset1 = file_size / 4;
            const corrupt_magic = [_]u8{ 0xBA, 0xAD, 0xF0, 0x0D };
            _ = try file.write_at(offset1, &corrupt_magic);

            // Corruption 2: Checksum corruption (middle)
            const offset2 = file_size / 2;
            const corrupt_checksum = [_]u8{ 0xFF, 0xFF, 0xFF, 0xFF };
            _ = try file.write_at(offset2, &corrupt_checksum);

            // Corruption 3: Size field corruption (later in file)
            const offset3 = (file_size * 3) / 4;
            const corrupt_size = [_]u8{ 0xFF, 0xFF, 0xFF, 0xFF }; // Invalid large size
            _ = try file.write_at(offset3, &corrupt_size);
        }

        // Phase 3: Test partial recovery with mixed corruption
        var corrupted_wal = try WAL.init(allocator, harness.sim_vfs.vfs(), wal_dir);
        defer corrupted_wal.deinit();

        const recovery_result = corrupted_wal.startup();
        if (recovery_result) |_| {
            const stats = corrupted_wal.statistics();

            // Should have detected multiple failures
            try testing.expect(stats.recovery_failures > 0);

            // Should have recovered at least some entries before first corruption
            try testing.expect(stats.entries_recovered > 0);
            try testing.expect(stats.entries_recovered < num_good_entries);
        } else |err| {
            // WAL completely failed recovery due to extensive corruption
            try testing.expect(err == error.CorruptedWALEntry or
                err == error.InvalidChecksum or
                err == error.InvalidEntryType or
                err == error.BufferTooSmall);
        }
    }
}
