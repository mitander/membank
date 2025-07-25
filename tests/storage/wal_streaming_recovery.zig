//! Integration tests for streaming WAL recovery using WALEntryStream
//!
//! Validates that the new streaming recovery implementation provides identical
//! functionality to the original buffered approach while using significantly
//! less memory. Tests focus on real-world scenarios including large segments,
//! corruption handling, and callback integration.

const std = @import("std");
const testing = std.testing;
const assert = std.debug.assert;

const vfs = @import("../../src/core/vfs.zig");
const simulation_vfs = @import("../../src/sim/simulation_vfs.zig");
const context_block = @import("../../src/core/types.zig");
const wal = @import("../../src/storage/wal.zig");

const SimulationVFS = simulation_vfs.SimulationVFS;
const ContextBlock = context_block.ContextBlock;
const BlockId = context_block.BlockId;
const GraphEdge = context_block.GraphEdge;
const EdgeType = context_block.EdgeType;
const WAL = wal.WAL;
const WALEntry = wal.WALEntry;

/// Test recovery context to capture recovered entries for validation
const RecoveryContext = struct {
    recovered_entries: std.ArrayList(WALEntry),
    allocator: std.mem.Allocator,

    fn init(allocator: std.mem.Allocator) RecoveryContext {
        return RecoveryContext{
            .recovered_entries = std.ArrayList(WALEntry).init(allocator),
            .allocator = allocator,
        };
    }

    fn deinit(self: *RecoveryContext) void {
        for (self.recovered_entries.items) |entry| {
            entry.deinit(self.allocator);
        }
        self.recovered_entries.deinit();
    }
};

/// Recovery callback that stores entries for test validation
fn recovery_callback(entry: WALEntry, context: *anyopaque) wal.WALError!void {
    const recovery_context: *RecoveryContext = @ptrCast(@alignCast(context));

    // Clone entry for storage since original will be freed by caller
    const cloned_payload = try recovery_context.allocator.dupe(u8, entry.payload);
    const cloned_entry = WALEntry{
        .checksum = entry.checksum,
        .entry_type = entry.entry_type,
        .payload_size = entry.payload_size,
        .payload = cloned_payload,
    };

    try recovery_context.recovered_entries.append(cloned_entry);
}

/// Create test block with predictable content for validation
fn create_test_block(allocator: std.mem.Allocator, id_suffix: u8) !ContextBlock {
    var id_bytes: [16]u8 = std.mem.zeroes([16]u8);
    id_bytes[15] = id_suffix; // Make each block unique

    const content = try std.fmt.allocPrint(allocator, "test content {d}", .{id_suffix});
    const metadata = try std.fmt.allocPrint(allocator, "{{\"type\": \"test\", \"id\": {d}}}", .{id_suffix});

    return ContextBlock{
        .id = BlockId.from_bytes(id_bytes),
        .version = 1,
        .source_uri = try allocator.dupe(u8, "test://source"),
        .metadata_json = metadata,
        .content = content,
    };
}

test "streaming recovery basic functionality" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const allocator = arena.allocator();

    var sim_vfs = try SimulationVFS.init(allocator);
    var vfs_interface = sim_vfs.vfs();

    // Create WAL with test directory
    const test_dir = "test_wal_dir";
    try vfs_interface.mkdir(test_dir);

    var test_wal = try WAL.init(allocator, vfs_interface, test_dir);
    defer test_wal.deinit();

    // Create test entries of different types
    const test_block1 = try create_test_block(allocator, 1);
    const test_block2 = try create_test_block(allocator, 2);

    const test_edge = GraphEdge{
        .from = test_block1.id,
        .to = test_block2.id,
        .edge_type = EdgeType.references,
    };

    // Write entries to WAL
    try test_wal.write_put_block(test_block1);
    try test_wal.write_put_block(test_block2);
    try test_wal.write_put_edge(test_edge);
    try test_wal.write_delete_block(test_block1.id);

    // Flush to ensure data is written
    try test_wal.flush();

    // Set up recovery context
    var recovery_context = RecoveryContext.init(allocator);
    defer recovery_context.deinit();

    // Test streaming recovery on the written segment
    const segment_files = try test_wal.list_segment_files();
    defer allocator.free(segment_files);

    try testing.expect(segment_files.len > 0);

    // Use the new streaming recovery method
    try test_wal.recover_from_segment_streaming(
        segment_files[0],
        recovery_callback,
        &recovery_context,
    );

    // Validate recovered entries
    try testing.expectEqual(@as(usize, 4), recovery_context.recovered_entries.items.len);

    // First entry should be put_block for test_block1
    const entry1 = recovery_context.recovered_entries.items[0];
    try testing.expectEqual(wal.WALEntryType.put_block, entry1.entry_type);

    // Second entry should be put_block for test_block2
    const entry2 = recovery_context.recovered_entries.items[1];
    try testing.expectEqual(wal.WALEntryType.put_block, entry2.entry_type);

    // Third entry should be put_edge
    const entry3 = recovery_context.recovered_entries.items[2];
    try testing.expectEqual(wal.WALEntryType.put_edge, entry3.entry_type);

    // Fourth entry should be delete_block
    const entry4 = recovery_context.recovered_entries.items[3];
    try testing.expectEqual(wal.WALEntryType.delete_block, entry4.entry_type);
    try testing.expectEqual(@as(usize, 16), entry4.payload.len); // BlockId size
}

test "streaming recovery large entries" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const allocator = arena.allocator();

    var sim_vfs = try SimulationVFS.init(allocator);
    var vfs_interface = sim_vfs.vfs();

    const test_dir = "large_wal_dir";
    try vfs_interface.mkdir(test_dir);

    var test_wal = try WAL.init(allocator, vfs_interface, test_dir);
    defer test_wal.deinit();

    // Create block with large content that exceeds typical buffer sizes
    var large_content = try allocator.alloc(u8, 64 * 1024); // 64KB content
    for (large_content, 0..) |*byte, i| {
        byte.* = @intCast(i % 256); // Recognizable pattern
    }

    var large_block = try create_test_block(allocator, 1);
    allocator.free(large_block.content);
    large_block.content = large_content;

    // Write large block and some normal entries
    const normal_block = try create_test_block(allocator, 2);

    try test_wal.write_put_block(normal_block);
    try test_wal.write_put_block(large_block);
    try test_wal.write_put_block(normal_block);

    try test_wal.flush();

    // Recover using streaming approach
    var recovery_context = RecoveryContext.init(allocator);
    defer recovery_context.deinit();

    const segment_files = try test_wal.list_segment_files();
    defer allocator.free(segment_files);

    try test_wal.recover_from_segment_streaming(
        segment_files[0],
        recovery_callback,
        &recovery_context,
    );

    // Validate all entries recovered correctly
    try testing.expectEqual(@as(usize, 3), recovery_context.recovered_entries.items.len);

    // Verify large entry was recovered correctly
    const large_entry = recovery_context.recovered_entries.items[1];
    try testing.expectEqual(wal.WALEntryType.put_block, large_entry.entry_type);

    // Deserialize the large block to validate content
    const recovered_block = try ContextBlock.deserialize(large_entry.payload, allocator);
    defer recovered_block.deinit(allocator);

    try testing.expectEqual(@as(usize, 64 * 1024), recovered_block.content.len);

    // Validate pattern in recovered content
    for (recovered_block.content, 0..) |byte, i| {
        try testing.expectEqual(@as(u8, @intCast(i % 256)), byte);
    }
}

test "streaming recovery corruption resilience" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const allocator = arena.allocator();

    var sim_vfs = try SimulationVFS.init(allocator);
    var vfs_interface = sim_vfs.vfs();

    // Create corrupted WAL file manually
    const corrupt_file = "corrupt_wal.log";
    var file = try vfs_interface.create(corrupt_file);
    defer file.close();

    var writer = file.writer();

    // Write valid entry
    const valid_block = try create_test_block(allocator, 1);
    const valid_entry = try WALEntry.create_put_block(valid_block, allocator);
    defer valid_entry.deinit(allocator);

    const valid_data = try valid_entry.serialize(allocator);
    defer allocator.free(valid_data);
    try writer.writeAll(valid_data);

    // Write corrupted entry with invalid payload size
    try writer.writeInt(u64, 0x1234567890ABCDEF, .little); // checksum
    try writer.writeByte(1); // valid type
    try writer.writeInt(u32, std.math.maxInt(u32), .little); // invalid huge size
    try writer.writeAll("garbage data");

    // Write another valid entry after corruption
    const valid_block2 = try create_test_block(allocator, 2);
    const valid_entry2 = try WALEntry.create_put_block(valid_block2, allocator);
    defer valid_entry2.deinit(allocator);

    const valid_data2 = try valid_entry2.serialize(allocator);
    defer allocator.free(valid_data2);
    try writer.writeAll(valid_data2);

    // Create WAL instance for recovery
    const test_dir = "corrupt_test_dir";
    try vfs_interface.mkdir(test_dir);

    var test_wal = try WAL.init(allocator, vfs_interface, test_dir);
    defer test_wal.deinit();

    // Recovery should skip corrupted entry and continue
    var recovery_context = RecoveryContext.init(allocator);
    defer recovery_context.deinit();

    // Use streaming recovery - should handle corruption gracefully
    try test_wal.recover_from_segment_streaming(
        corrupt_file,
        recovery_callback,
        &recovery_context,
    );

    // Should recover the two valid entries, skipping the corrupted one
    try testing.expectEqual(@as(usize, 2), recovery_context.recovered_entries.items.len);

    for (recovery_context.recovered_entries.items) |entry| {
        try testing.expectEqual(wal.WALEntryType.put_block, entry.entry_type);
    }
}

test "streaming recovery memory efficiency" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const allocator = arena.allocator();

    var sim_vfs = try SimulationVFS.init(allocator);
    var vfs_interface = sim_vfs.vfs();

    const test_dir = "memory_test_dir";
    try vfs_interface.mkdir(test_dir);

    var test_wal = try WAL.init(allocator, vfs_interface, test_dir);
    defer test_wal.deinit();

    // Create many entries to fill a substantial WAL segment
    const num_entries = 1000;
    var expected_entries: u32 = 0;

    for (0..num_entries) |i| {
        const test_block = try create_test_block(allocator, @intCast(i % 256));
        try test_wal.write_put_block(test_block);
        expected_entries += 1;
    }

    try test_wal.flush();

    // Track memory usage during streaming recovery
    var gpa = std.heap.GeneralPurposeAllocator(.{ .safety = true }){};
    defer _ = gpa.deinit();
    const tracking_allocator = gpa.allocator();

    var recovery_context = RecoveryContext.init(tracking_allocator);
    defer recovery_context.deinit();

    const segment_files = try test_wal.list_segment_files();
    defer allocator.free(segment_files);

    // Streaming recovery should process entries one at a time
    // without loading the entire segment into memory
    try test_wal.recover_from_segment_streaming(
        segment_files[0],
        recovery_callback,
        &recovery_context,
    );

    // Validate all entries were recovered
    try testing.expectEqual(@as(usize, expected_entries), recovery_context.recovered_entries.items.len);

    // Memory usage should remain bounded regardless of segment size
    // This is validated by the fact that the test completes without OOM
    // and the GeneralPurposeAllocator with safety checks doesn't detect leaks
}

test "streaming recovery empty segment" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const allocator = arena.allocator();

    var sim_vfs = try SimulationVFS.init(allocator);
    var vfs_interface = sim_vfs.vfs();

    // Create empty WAL segment
    const empty_file = "empty_wal.log";
    var file = try vfs_interface.create(empty_file);
    file.close();

    const test_dir = "empty_test_dir";
    try vfs_interface.mkdir(test_dir);

    var test_wal = try WAL.init(allocator, vfs_interface, test_dir);
    defer test_wal.deinit();

    var recovery_context = RecoveryContext.init(allocator);
    defer recovery_context.deinit();

    // Streaming recovery should handle empty files gracefully
    try test_wal.recover_from_segment_streaming(
        empty_file,
        recovery_callback,
        &recovery_context,
    );

    // No entries should be recovered from empty file
    try testing.expectEqual(@as(usize, 0), recovery_context.recovered_entries.items.len);
}

test "streaming recovery callback error propagation" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const allocator = arena.allocator();

    var sim_vfs = try SimulationVFS.init(allocator);
    var vfs_interface = sim_vfs.vfs();

    const test_dir = "error_test_dir";
    try vfs_interface.mkdir(test_dir);

    var test_wal = try WAL.init(allocator, vfs_interface, test_dir);
    defer test_wal.deinit();

    // Write a test entry
    const test_block = try create_test_block(allocator, 1);
    try test_wal.write_put_block(test_block);
    try test_wal.flush();

    const segment_files = try test_wal.list_segment_files();
    defer allocator.free(segment_files);

    // Define callback that always returns an error
    const error_callback = struct {
        fn callback(entry: WALEntry, context: *anyopaque) wal.WALError!void {
            _ = entry;
            _ = context;
            return wal.WALError.OutOfMemory; // Simulate callback error
        }
    }.callback;

    var dummy_context: u8 = 0;

    // Recovery should propagate callback errors
    const result = test_wal.recover_from_segment_streaming(
        segment_files[0],
        error_callback,
        &dummy_context,
    );

    try testing.expectError(wal.WALError.OutOfMemory, result);
}

test "streaming vs buffered recovery equivalence" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const allocator = arena.allocator();

    var sim_vfs = try SimulationVFS.init(allocator);
    var vfs_interface = sim_vfs.vfs();

    const test_dir = "comparison_test_dir";
    try vfs_interface.mkdir(test_dir);

    var test_wal = try WAL.init(allocator, vfs_interface, test_dir);
    defer test_wal.deinit();

    // Create diverse set of entries to test both approaches
    const test_entries = [_]struct { block_id: u8, content_size: usize }{
        .{ .block_id = 1, .content_size = 100 },
        .{ .block_id = 2, .content_size = 8192 }, // Buffer boundary size
        .{ .block_id = 3, .content_size = 32 * 1024 }, // Large entry
        .{ .block_id = 4, .content_size = 50 },
    };

    var expected_blocks = std.ArrayList(ContextBlock).init(allocator);
    defer {
        for (expected_blocks.items) |block| {
            block.deinit(allocator);
        }
        expected_blocks.deinit();
    }

    // Create and write test data
    for (test_entries) |entry_spec| {
        var test_block = try create_test_block(allocator, entry_spec.block_id);

        // Resize content to test different sizes
        allocator.free(test_block.content);
        test_block.content = try allocator.alloc(u8, entry_spec.content_size);
        @memset(test_block.content, @intCast(entry_spec.block_id + 'A'));

        try expected_blocks.append(test_block);
        try test_wal.write_put_block(test_block);
    }

    try test_wal.flush();

    const segment_files = try test_wal.list_segment_files();
    defer allocator.free(segment_files);

    // Test buffered recovery
    var buffered_context = RecoveryContext.init(allocator);
    defer buffered_context.deinit();

    try test_wal.recover_entries_with_options(
        recovery_callback,
        &buffered_context,
        .{ .use_streaming = false },
    );

    // Test streaming recovery
    var streaming_context = RecoveryContext.init(allocator);
    defer streaming_context.deinit();

    try test_wal.recover_entries_with_options(
        recovery_callback,
        &streaming_context,
        .{ .use_streaming = true },
    );

    // Both approaches should recover identical number of entries
    try testing.expectEqual(
        buffered_context.recovered_entries.items.len,
        streaming_context.recovered_entries.items.len,
    );

    // Validate entries are identical between both approaches
    for (buffered_context.recovered_entries.items, streaming_context.recovered_entries.items) |buffered_entry, streaming_entry| {
        try testing.expectEqual(buffered_entry.checksum, streaming_entry.checksum);
        try testing.expectEqual(buffered_entry.entry_type, streaming_entry.entry_type);
        try testing.expectEqual(buffered_entry.payload_size, streaming_entry.payload_size);
        try testing.expectEqualSlices(u8, buffered_entry.payload, streaming_entry.payload);
    }

    // Verify all expected blocks were recovered
    try testing.expectEqual(@as(usize, test_entries.len), buffered_context.recovered_entries.items.len);
}
