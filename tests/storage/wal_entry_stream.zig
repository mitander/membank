//! Tests for WALEntryStream buffered I/O abstraction
//!
//! These tests validate the streaming WAL reader's ability to handle various
//! real-world scenarios including buffer boundaries, large entries, corruption,
//! and memory management. Uses simulation VFS for deterministic testing.

const kausaldb = @import("kausaldb");
const std = @import("std");
const testing = std.testing;
const assert = kausaldb.assert.assert;

const WALEntryStream = kausaldb.wal.stream.WALEntryStream;
const StreamEntry = kausaldb.wal.stream.StreamEntry;
const StreamError = kausaldb.wal.stream.StreamError;
const vfs = kausaldb.vfs;
const simulation_vfs = kausaldb.simulation_vfs;

const SimulationVFS = simulation_vfs.SimulationVFS;

/// Helper to create a valid WAL entry header + payload using VFile directly
fn create_wal_entry(file: *kausaldb.vfs.VFile, entry_type: u8, payload: []const u8) !void {
    // Calculate checksum over type + payload (matching WAL implementation)
    var hasher = std.hash.Wyhash.init(0);
    hasher.update(&[_]u8{entry_type});
    hasher.update(payload);
    const checksum = hasher.final();

    // Write WAL entry: 8 bytes checksum + 1 byte type + 4 bytes size + payload
    const checksum_bytes = std.mem.toBytes(checksum);
    _ = try file.write(&checksum_bytes);
    _ = try file.write(&[_]u8{entry_type});
    const size_bytes = std.mem.toBytes(@as(u32, @intCast(payload.len)));
    _ = try file.write(&size_bytes);
    _ = try file.write(payload);
}

test "stream basic entry reading" {
    const allocator = testing.allocator;

    var sim_vfs = try SimulationVFS.init(allocator);
    defer sim_vfs.deinit();
    var vfs_interface = sim_vfs.vfs();

    const test_file = "test_wal.log";
    var file = try vfs_interface.create(test_file);
    defer file.close();

    try create_wal_entry(&file, 1, "first entry");
    try create_wal_entry(&file, 2, "second entry with more data");
    try create_wal_entry(&file, 1, "third");

    // Reset file for reading
    _ = try file.seek(0, .start);

    var stream = try WALEntryStream.init(allocator, &file);

    // Read first entry
    const entry1 = (try stream.next()).?;
    defer entry1.deinit(allocator);
    try testing.expectEqual(@as(u8, 1), entry1.entry_type);
    try testing.expectEqualStrings("first entry", entry1.payload);
    try testing.expectEqual(@as(u64, 0), entry1.file_position);

    // Read second entry
    const entry2 = (try stream.next()).?;
    defer entry2.deinit(allocator);
    try testing.expectEqual(@as(u8, 2), entry2.entry_type);
    try testing.expectEqualStrings("second entry with more data", entry2.payload);

    // Read third entry
    const entry3 = (try stream.next()).?;
    defer entry3.deinit(allocator);
    try testing.expectEqual(@as(u8, 1), entry3.entry_type);
    try testing.expectEqualStrings("third", entry3.payload);

    // Should reach end of file
    const entry4 = try stream.next();
    try testing.expectEqual(@as(?StreamEntry, null), entry4);
}

test "stream large entry exceeding buffer" {
    const allocator = testing.allocator;

    var sim_vfs = try SimulationVFS.init(allocator);
    defer sim_vfs.deinit();
    var vfs_interface = sim_vfs.vfs();

    const large_payload_size = 32 * 1024; // 32KB payload
    const large_payload = try allocator.alloc(u8, large_payload_size);
    defer allocator.free(large_payload);
    // Fill with recognizable pattern for validation
    for (large_payload, 0..) |*byte, i| {
        byte.* = @intCast(i % 256);
    }

    const test_file = "large_entry_wal.log";
    var file = try vfs_interface.create(test_file);
    defer file.close();

    try create_wal_entry(&file, 1, "small before");
    try create_wal_entry(&file, 2, large_payload);
    try create_wal_entry(&file, 1, "small after");

    _ = try file.seek(0, .start);

    var stream = try WALEntryStream.init(allocator, &file);

    // Read small entry before large one
    const entry1 = (try stream.next()).?;
    defer entry1.deinit(allocator);
    try testing.expectEqualStrings("small before", entry1.payload);

    // Read large entry - should handle direct file access
    const large_entry = (try stream.next()).?;
    defer large_entry.deinit(allocator);
    try testing.expectEqual(@as(u8, 2), large_entry.entry_type);
    try testing.expectEqual(large_payload_size, large_entry.payload.len);

    // Validate pattern in large payload
    for (large_entry.payload, 0..) |byte, i| {
        try testing.expectEqual(@as(u8, @intCast(i % 256)), byte);
    }

    // Read small entry after large one
    const entry3 = (try stream.next()).?;
    defer entry3.deinit(allocator);
    try testing.expectEqualStrings("small after", entry3.payload);

    // Verify stats show correct entry count
    const stream_stats = stream.stats();
    try testing.expectEqual(@as(u32, 3), stream_stats.entries_read);
}

test "stream entry spanning buffer boundary" {
    const allocator = testing.allocator;

    var sim_vfs = try SimulationVFS.init(allocator);
    defer sim_vfs.deinit();
    var vfs_interface = sim_vfs.vfs();

    // Use size that fills most of initial buffer
    const boundary_payload = try allocator.alloc(u8, 8000); // Close to 8KB buffer
    defer allocator.free(boundary_payload);
    @memset(boundary_payload, 'B');

    const test_file = "boundary_wal.log";
    var file = try vfs_interface.create(test_file);
    defer file.close();

    // Small entry to partially fill first buffer
    try create_wal_entry(&file, 1, "start");
    // Large entry that will span buffer boundary
    try create_wal_entry(&file, 2, boundary_payload);
    // Another entry after boundary
    try create_wal_entry(&file, 1, "end");

    _ = try file.seek(0, .start);

    var stream = try WALEntryStream.init(allocator, &file);

    // All entries should be readable despite boundary spanning
    const entry1 = (try stream.next()).?;
    defer entry1.deinit(allocator);
    try testing.expectEqualStrings("start", entry1.payload);

    const entry2 = (try stream.next()).?;
    defer entry2.deinit(allocator);
    try testing.expectEqual(@as(u8, 2), entry2.entry_type);
    try testing.expectEqual(boundary_payload.len, entry2.payload.len);

    const entry3 = (try stream.next()).?;
    defer entry3.deinit(allocator);
    try testing.expectEqualStrings("end", entry3.payload);
}

test "stream corruption detection" {
    const allocator = testing.allocator;

    var sim_vfs = try SimulationVFS.init(allocator);
    defer sim_vfs.deinit();
    var vfs_interface = sim_vfs.vfs();

    const test_file = "corrupted_wal.log";
    var file = try vfs_interface.create(test_file);
    defer file.close();

    try create_wal_entry(&file, 1, "valid entry");

    // Write corrupted entry manually
    const checksum_bytes = std.mem.toBytes(@as(u64, 0x1234567890ABCDEF));
    _ = try file.write(&checksum_bytes);
    _ = try file.write(&[_]u8{1}); // valid type
    const size_bytes = std.mem.toBytes(@as(u32, std.math.maxInt(u32)));
    _ = try file.write(&size_bytes); // invalid huge size
    _ = try file.write("garbage");

    _ = try file.seek(0, .start);

    var stream = try WALEntryStream.init(allocator, &file);

    // First entry should read successfully
    const entry1 = (try stream.next()).?;
    defer entry1.deinit(allocator);
    try testing.expectEqualStrings("valid entry", entry1.payload);

    // Second entry should return corruption error
    const result = stream.next();
    try testing.expectError(StreamError.CorruptedEntry, result);
}

test "stream invalid entry type detection" {
    const allocator = testing.allocator;

    var sim_vfs = try SimulationVFS.init(allocator);
    defer sim_vfs.deinit();
    var vfs_interface = sim_vfs.vfs();

    const test_file = "invalid_type_wal.log";
    var file = try vfs_interface.create(test_file);
    defer file.close();

    // Write invalid entry manually
    const checksum_bytes = std.mem.toBytes(@as(u64, 0x1234567890ABCDEF));
    _ = try file.write(&checksum_bytes);
    _ = try file.write(&[_]u8{0}); // invalid type
    const size_bytes = std.mem.toBytes(@as(u32, 4));
    _ = try file.write(&size_bytes); // reasonable size
    _ = try file.write("test");

    _ = try file.seek(0, .start);

    var stream = try WALEntryStream.init(allocator, &file);

    // Should detect invalid entry type
    const result = stream.next();
    try testing.expectError(StreamError.CorruptedEntry, result);
}

test "stream empty file handling" {
    const allocator = testing.allocator;

    var sim_vfs = try SimulationVFS.init(allocator);
    defer sim_vfs.deinit();
    var vfs_interface = sim_vfs.vfs();

    const test_file = "empty_wal.log";
    var file = try vfs_interface.create(test_file);
    defer file.close();

    // File remains empty - no writes

    var stream = try WALEntryStream.init(allocator, &file);

    // Should immediately return null for empty file
    const entry = try stream.next();
    try testing.expectEqual(@as(?StreamEntry, null), entry);

    const stream_stats = stream.stats();
    try testing.expectEqual(@as(u32, 0), stream_stats.entries_read);
}

test "stream truncated entry handling" {
    const allocator = testing.allocator;

    var sim_vfs = try SimulationVFS.init(allocator);
    defer sim_vfs.deinit();
    var vfs_interface = sim_vfs.vfs();

    const test_file = "truncated_wal.log";
    var file = try vfs_interface.create(test_file);
    defer file.close();

    try create_wal_entry(&file, 1, "complete");

    // Write partial entry manually
    const checksum_bytes = std.mem.toBytes(@as(u64, 0x1234567890ABCDEF));
    _ = try file.write(&checksum_bytes);
    _ = try file.write(&[_]u8{1});
    // Missing size and payload - file ends here

    _ = try file.seek(0, .start);

    var stream = try WALEntryStream.init(allocator, &file);

    // First entry should read successfully
    const entry1 = (try stream.next()).?;
    defer entry1.deinit(allocator);
    try testing.expectEqualStrings("complete", entry1.payload);

    // Should reach end of file for truncated entry
    const entry2 = try stream.next();
    try testing.expectEqual(@as(?StreamEntry, null), entry2);
}

test "stream memory management" {
    const allocator = testing.allocator;

    var sim_vfs = try SimulationVFS.init(allocator);
    defer sim_vfs.deinit();
    var vfs_interface = sim_vfs.vfs();

    const test_file = "memory_test_wal.log";
    var file = try vfs_interface.create(test_file);
    defer file.close();

    // Create entries with different payload sizes
    const large_payload = try allocator.dupe(u8, "x" ** 1000); // 1KB payload
    defer allocator.free(large_payload);
    const payloads = [_][]const u8{
        "small",
        "medium sized payload for testing",
        large_payload,
    };

    for (payloads, 0..) |payload, i| {
        try create_wal_entry(&file, @intCast(i + 1), payload);
    }

    _ = try file.seek(0, .start);

    var stream = try WALEntryStream.init(allocator, &file);

    // Read all entries and verify proper cleanup
    for (payloads, 0..) |expected_payload, i| {
        const entry = (try stream.next()).?;
        defer entry.deinit(allocator); // Critical: test proper cleanup

        try testing.expectEqual(@as(u8, @intCast(i + 1)), entry.entry_type);
        try testing.expectEqualStrings(expected_payload, entry.payload);
    }

    // Verify end of stream
    const final_entry = try stream.next();
    try testing.expectEqual(@as(?StreamEntry, null), final_entry);
}

test "stream position tracking" {
    const allocator = testing.allocator;

    var sim_vfs = try SimulationVFS.init(allocator);
    defer sim_vfs.deinit();
    var vfs_interface = sim_vfs.vfs();

    const test_file = "position_wal.log";
    var file = try vfs_interface.create(test_file);
    defer file.close();

    try create_wal_entry(&file, 1, "first");
    try create_wal_entry(&file, 2, "second");

    _ = try file.seek(0, .start);

    var stream = try WALEntryStream.init(allocator, &file);

    // First entry should be at position 0
    const entry1 = (try stream.next()).?;
    defer entry1.deinit(allocator);
    try testing.expectEqual(@as(u64, 0), entry1.file_position);

    // Second entry should be at position after first entry
    const entry2 = (try stream.next()).?;
    defer entry2.deinit(allocator);

    // Position should be header + payload of first entry
    const expected_pos = 13 + "first".len; // 13 byte header + 5 byte payload
    try testing.expectEqual(@as(u64, expected_pos), entry2.file_position);
}
