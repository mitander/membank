//! Comprehensive tests for WALEntryStream buffered I/O abstraction
//!
//! These tests validate the streaming WAL reader's ability to handle various
//! real-world scenarios including buffer boundaries, large entries, corruption,
//! and memory management. Uses simulation VFS for deterministic testing.

const cortexdb = @import("cortexdb");
const std = @import("std");
const testing = std.testing;
const assert = cortexdb.assert.assert;

const cortexdb = @import("cortexdb");
const wal_entry_stream = @import("../../src/storage/wal_entry_stream.zig");
const WALEntryStream = wal_entry_stream.WALEntryStream;
const StreamEntry = wal_entry_stream.StreamEntry;
const StreamError = wal_entry_stream.StreamError;
const cortexdb = @import("cortexdb");
const vfs = @import("../../src/core/vfs.zig");
const cortexdb = @import("cortexdb");
const simulation_vfs = @import("../../src/sim/simulation_vfs.zig");

const SimulationVFS = simulation_vfs.SimulationVFS;

/// Helper to create a valid WAL entry header + payload
fn create_wal_entry(writer: anytype, entry_type: u8, payload: []const u8) !void {
    // Calculate checksum over type + size + payload (simplified for testing)
    var hasher = std.hash.Crc64.init();
    hasher.update(&[_]u8{entry_type});
    const size_bytes = std.mem.toBytes(@as(u32, @intCast(payload.len)));
    hasher.update(&size_bytes);
    hasher.update(payload);
    const checksum = hasher.final();

    // Write WAL entry: 8 bytes checksum + 1 byte type + 4 bytes size + payload
    try writer.writeInt(u64, checksum, .little);
    try writer.writeByte(entry_type);
    try writer.writeInt(u32, @intCast(payload.len), .little);
    try writer.writeAll(payload);
}

test "stream basic entry reading" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const allocator = arena.allocator();

    var sim_vfs = try SimulationVFS.init(allocator);
    var vfs_interface = sim_vfs.vfs();

    // Create test file with multiple small entries
    const test_file = "test_wal.log";
    var file = try vfs_interface.create(test_file);
    defer file.close();

    var writer = file.writer();
    try create_wal_entry(writer, 1, "first entry");
    try create_wal_entry(writer, 2, "second entry with more data");
    try create_wal_entry(writer, 1, "third");

    // Reset file for reading
    try file.seek(0, .start);

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
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const allocator = arena.allocator();

    var sim_vfs = try SimulationVFS.init(allocator);
    var vfs_interface = sim_vfs.vfs();

    // Create large payload that exceeds typical buffer sizes
    const large_payload_size = 32 * 1024; // 32KB payload
    const large_payload = try allocator.alloc(u8, large_payload_size);
    // Fill with recognizable pattern for validation
    for (large_payload, 0..) |*byte, i| {
        byte.* = @intCast(i % 256);
    }

    const test_file = "large_entry_wal.log";
    var file = try vfs_interface.create(test_file);
    defer file.close();

    var writer = file.writer();
    try create_wal_entry(writer, 1, "small before");
    try create_wal_entry(writer, 2, large_payload);
    try create_wal_entry(writer, 1, "small after");

    try file.seek(0, .start);

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
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const allocator = arena.allocator();

    var sim_vfs = try SimulationVFS.init(allocator);
    var vfs_interface = sim_vfs.vfs();

    // Create payload that will cause boundary spanning
    // Use size that fills most of initial buffer
    const boundary_payload = try allocator.alloc(u8, 8000); // Close to 8KB buffer
    @memset(boundary_payload, 'B');

    const test_file = "boundary_wal.log";
    var file = try vfs_interface.create(test_file);
    defer file.close();

    var writer = file.writer();
    // Small entry to partially fill first buffer
    try create_wal_entry(writer, 1, "start");
    // Large entry that will span buffer boundary
    try create_wal_entry(writer, 2, boundary_payload);
    // Another entry after boundary
    try create_wal_entry(writer, 1, "end");

    try file.seek(0, .start);

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
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const allocator = arena.allocator();

    var sim_vfs = try SimulationVFS.init(allocator);
    var vfs_interface = sim_vfs.vfs();

    const test_file = "corrupted_wal.log";
    var file = try vfs_interface.create(test_file);
    defer file.close();

    var writer = file.writer();

    // Write valid entry first
    try create_wal_entry(writer, 1, "valid entry");

    // Write corrupted entry with invalid payload size
    try writer.writeInt(u64, 0x1234567890ABCDEF, .little); // checksum
    try writer.writeByte(1); // valid type
    try writer.writeInt(u32, std.math.maxInt(u32), .little); // invalid huge size
    try writer.writeAll("garbage");

    try file.seek(0, .start);

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
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const allocator = arena.allocator();

    var sim_vfs = try SimulationVFS.init(allocator);
    var vfs_interface = sim_vfs.vfs();

    const test_file = "invalid_type_wal.log";
    var file = try vfs_interface.create(test_file);
    defer file.close();

    var writer = file.writer();

    // Write entry with invalid type (0 is invalid, valid range is 1-3)
    try writer.writeInt(u64, 0x1234567890ABCDEF, .little); // checksum
    try writer.writeByte(0); // invalid type
    try writer.writeInt(u32, 4, .little); // reasonable size
    try writer.writeAll("test");

    try file.seek(0, .start);

    var stream = try WALEntryStream.init(allocator, &file);

    // Should detect invalid entry type
    const result = stream.next();
    try testing.expectError(StreamError.CorruptedEntry, result);
}

test "stream empty file handling" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const allocator = arena.allocator();

    var sim_vfs = try SimulationVFS.init(allocator);
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
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const allocator = arena.allocator();

    var sim_vfs = try SimulationVFS.init(allocator);
    var vfs_interface = sim_vfs.vfs();

    const test_file = "truncated_wal.log";
    var file = try vfs_interface.create(test_file);
    defer file.close();

    var writer = file.writer();

    // Write valid complete entry
    try create_wal_entry(writer, 1, "complete");

    // Write partial entry header (only checksum and type, missing size and payload)
    try writer.writeInt(u64, 0x1234567890ABCDEF, .little);
    try writer.writeByte(1);
    // Missing size and payload - file ends here

    try file.seek(0, .start);

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
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const allocator = arena.allocator();

    var sim_vfs = try SimulationVFS.init(allocator);
    var vfs_interface = sim_vfs.vfs();

    const test_file = "memory_test_wal.log";
    var file = try vfs_interface.create(test_file);
    defer file.close();

    var writer = file.writer();

    // Create entries with different payload sizes
    const payloads = [_][]const u8{
        "small",
        "medium sized payload for testing",
        try allocator.dupe(u8, "x" ** 1000), // 1KB payload
    };

    for (payloads, 0..) |payload, i| {
        try create_wal_entry(writer, @intCast(i + 1), payload);
    }

    try file.seek(0, .start);

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
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const allocator = arena.allocator();

    var sim_vfs = try SimulationVFS.init(allocator);
    var vfs_interface = sim_vfs.vfs();

    const test_file = "position_wal.log";
    var file = try vfs_interface.create(test_file);
    defer file.close();

    var writer = file.writer();
    try create_wal_entry(writer, 1, "first");
    try create_wal_entry(writer, 2, "second");

    try file.seek(0, .start);

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
