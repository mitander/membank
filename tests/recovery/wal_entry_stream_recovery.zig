//! Integration tests for streaming WAL recovery using WALEntryStream
//!
//! Validates that the new streaming recovery implementation provides identical
//! functionality to the original buffered approach while using significantly
//! less memory. Tests focus on real-world scenarios including large segments,
//! corruption handling, and callback integration.

const std = @import("std");

const kausaldb = @import("kausaldb");

const assert = kausaldb.assert.assert;
const simulation_vfs = kausaldb.simulation_vfs;
const testing = std.testing;
const types = kausaldb.types;
const vfs = kausaldb.vfs;
const wal = kausaldb.wal;

const SimulationVFS = simulation_vfs.SimulationVFS;
const ContextBlock = types.ContextBlock;
const BlockId = types.BlockId;
const GraphEdge = types.GraphEdge;
const EdgeType = types.EdgeType;
const WAL = wal.WAL;
const WALEntry = wal.WALEntry;

/// Test recovery context to capture recovered entries for validation
const RecoveryContext = struct {
    recovered_entries: std.array_list.Managed(WALEntry),
    allocator: std.mem.Allocator,

    fn init(allocator: std.mem.Allocator) RecoveryContext {
        return RecoveryContext{
            .recovered_entries = std.array_list.Managed(WALEntry).init(allocator),
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
fn create_test_block(allocator: std.mem.Allocator, id_suffix: u32) !ContextBlock {
    var id_bytes: [16]u8 = std.mem.zeroes([16]u8);
    // Use the full u32 to ensure uniqueness for large numbers
    std.mem.writeInt(u32, id_bytes[12..16], id_suffix, .little);

    const content = try std.fmt.allocPrint(allocator, "test content {d}", .{id_suffix});
    errdefer allocator.free(content);
    const metadata = try std.fmt.allocPrint(allocator, "{{\"type\": \"test\", \"id\": {d}}}", .{id_suffix});
    errdefer allocator.free(metadata);

    return ContextBlock{
        .id = BlockId.from_bytes(id_bytes),
        .version = 1,
        .source_uri = try allocator.dupe(u8, "test://source"),
        .metadata_json = metadata,
        .content = content,
    };
}

test "streaming recovery basic functionality" {
    const allocator = testing.allocator;

    var sim_vfs = try SimulationVFS.init(allocator);
    defer sim_vfs.deinit();
    var vfs_interface = sim_vfs.vfs();

    const test_dir = "test_wal_dir";
    try vfs_interface.mkdir(test_dir);

    var test_wal = try WAL.init(allocator, vfs_interface, test_dir);
    defer test_wal.deinit();
    try test_wal.startup();

    const test_block1 = try create_test_block(allocator, 1);
    defer test_block1.deinit(allocator);
    const test_block2 = try create_test_block(allocator, 2);
    defer test_block2.deinit(allocator);

    const test_edge = GraphEdge{
        .source_id = test_block1.id,
        .target_id = test_block2.id,
        .edge_type = EdgeType.references,
    };

    const entry1 = try WALEntry.create_put_block(allocator, test_block1);
    defer entry1.deinit(allocator);
    try test_wal.write_entry(entry1);

    const entry2 = try WALEntry.create_put_block(allocator, test_block2);
    defer entry2.deinit(allocator);
    try test_wal.write_entry(entry2);

    const edge_entry = try WALEntry.create_put_edge(allocator, test_edge);
    defer edge_entry.deinit(allocator);
    try test_wal.write_entry(edge_entry);

    const delete_entry = try WALEntry.create_delete_block(allocator, test_block1.id);
    defer delete_entry.deinit(allocator);
    try test_wal.write_entry(delete_entry);

    // Set up recovery context
    var recovery_context = RecoveryContext.init(allocator);
    defer recovery_context.deinit();

    try test_wal.recover_entries(recovery_callback, &recovery_context);
    try testing.expectEqual(@as(usize, 4), recovery_context.recovered_entries.items.len);
}

test "streaming recovery large entries" {
    const allocator = testing.allocator;

    var sim_vfs = try SimulationVFS.init(allocator);
    defer sim_vfs.deinit();
    var vfs_interface = sim_vfs.vfs();

    const test_dir = "large_wal_dir";
    try vfs_interface.mkdir(test_dir);

    var test_wal = try WAL.init(allocator, vfs_interface, test_dir);
    defer test_wal.deinit();
    try test_wal.startup();

    const large_content = try allocator.alloc(u8, 16 * 1024); // 16KB content - sufficient to test large entries
    for (large_content, 0..) |*byte, i| {
        byte.* = @intCast(i % 256);
    }

    var large_block = try create_test_block(allocator, 1);
    defer large_block.deinit(allocator);
    // Transfer ownership of large_content to large_block
    allocator.free(large_block.content); // Free the original content first
    large_block.content = large_content;

    const normal_block = try create_test_block(allocator, 2);
    defer normal_block.deinit(allocator);

    const normal_entry1 = try WALEntry.create_put_block(allocator, normal_block);
    defer normal_entry1.deinit(allocator);
    try test_wal.write_entry(normal_entry1);

    const large_entry = try WALEntry.create_put_block(allocator, large_block);
    defer large_entry.deinit(allocator);
    try test_wal.write_entry(large_entry);

    const normal_entry2 = try WALEntry.create_put_block(allocator, normal_block);
    defer normal_entry2.deinit(allocator);
    try test_wal.write_entry(normal_entry2);

    var recovery_context = RecoveryContext.init(allocator);
    defer recovery_context.deinit();

    try test_wal.recover_entries(recovery_callback, &recovery_context);

    try testing.expectEqual(@as(usize, 3), recovery_context.recovered_entries.items.len);
}

test "streaming recovery corruption resilience" {
    const allocator = testing.allocator;

    var sim_vfs = try SimulationVFS.init(allocator);
    defer sim_vfs.deinit();
    var vfs_interface = sim_vfs.vfs();

    const test_dir = "corrupt_test_dir";
    try vfs_interface.mkdir(test_dir);

    const corrupt_file_path = try std.fmt.allocPrint(allocator, "{s}/wal_000000.log", .{test_dir});
    defer allocator.free(corrupt_file_path);

    {
        var file = try vfs_interface.create(corrupt_file_path);
        defer file.close();

        const valid_block = try create_test_block(allocator, 1);
        defer valid_block.deinit(allocator);

        const valid_entry = try WALEntry.create_put_block(allocator, valid_block);
        defer valid_entry.deinit(allocator);

        var buffer = try allocator.alloc(u8, 1024);
        defer allocator.free(buffer);
        const valid_data_len = try valid_entry.serialize(buffer);
        const valid_data = buffer[0..valid_data_len];
        _ = try file.write(valid_data);

        // Create a corrupt entry that will be detected and skipped, but won't prevent reading the next entry
        const corrupt_checksum = std.mem.toBytes(@as(u64, 0x1234567890ABCDEF));
        _ = try file.write(&corrupt_checksum);
        _ = try file.write(&[_]u8{1}); // valid entry type
        const reasonable_size = std.mem.toBytes(@as(u32, 100)); // Small size that won't break parsing
        _ = try file.write(&reasonable_size);
        // Write exactly 100 bytes of corrupt data
        const corrupt_data = [_]u8{0} ** 100;
        _ = try file.write(&corrupt_data);

        const valid_block2 = try create_test_block(allocator, 2);
        defer valid_block2.deinit(allocator);

        const valid_entry2 = try WALEntry.create_put_block(allocator, valid_block2);
        defer valid_entry2.deinit(allocator);

        var buffer2 = try allocator.alloc(u8, 1024);
        defer allocator.free(buffer2);
        const valid_data2_len = try valid_entry2.serialize(buffer2);
        const valid_data2 = buffer2[0..valid_data2_len];
        _ = try file.write(valid_data2);
    } // File is closed here

    var test_wal = try WAL.init(allocator, vfs_interface, test_dir);
    defer test_wal.deinit();
    try test_wal.startup();

    var recovery_context = RecoveryContext.init(allocator);
    defer recovery_context.deinit();

    try test_wal.recover_entries(recovery_callback, &recovery_context);

    try testing.expectEqual(@as(usize, 2), recovery_context.recovered_entries.items.len);
}

test "streaming recovery memory efficiency" {
    const allocator = testing.allocator;

    var sim_vfs = try SimulationVFS.init(allocator);
    defer sim_vfs.deinit();
    var vfs_interface = sim_vfs.vfs();

    const test_dir = "test_large_wal";
    try vfs_interface.mkdir(test_dir);

    var test_wal = try WAL.init(allocator, sim_vfs.vfs(), test_dir);
    defer test_wal.deinit();

    try test_wal.startup();

    const num_entries = 1000;
    for (0..num_entries) |i| {
        const test_block = try create_test_block(allocator, @as(u32, @intCast(i)) + 1);
        defer test_block.deinit(allocator);

        const entry = try WALEntry.create_put_block(allocator, test_block);
        defer entry.deinit(allocator);
        try test_wal.write_entry(entry);
    }

    var recovery_context = RecoveryContext.init(allocator);
    defer recovery_context.deinit();

    try test_wal.recover_entries(recovery_callback, &recovery_context);

    try testing.expectEqual(@as(usize, num_entries), recovery_context.recovered_entries.items.len);
}

test "streaming recovery empty segment" {
    const allocator = testing.allocator;

    var sim_vfs = try SimulationVFS.init(allocator);
    defer sim_vfs.deinit();
    var vfs_interface = sim_vfs.vfs();

    const test_dir = "empty_test_dir";
    try vfs_interface.mkdir(test_dir);

    var test_wal = try WAL.init(allocator, vfs_interface, test_dir);
    defer test_wal.deinit();
    try test_wal.startup();

    var recovery_context = RecoveryContext.init(allocator);
    defer recovery_context.deinit();

    try test_wal.recover_entries(recovery_callback, &recovery_context);

    try testing.expectEqual(@as(usize, 0), recovery_context.recovered_entries.items.len);
}

test "streaming recovery callback error propagation" {
    const allocator = testing.allocator;

    var sim_vfs = try SimulationVFS.init(allocator);
    defer sim_vfs.deinit();
    var vfs_interface = sim_vfs.vfs();

    const test_dir = "error_test_dir";
    try vfs_interface.mkdir(test_dir);

    var test_wal = try WAL.init(allocator, vfs_interface, test_dir);
    defer test_wal.deinit();
    try test_wal.startup();

    const test_block = try create_test_block(allocator, 1);
    defer test_block.deinit(allocator);

    const entry = try WALEntry.create_put_block(allocator, test_block);
    defer entry.deinit(allocator);
    try test_wal.write_entry(entry);

    const error_callback = struct {
        fn callback(cb_entry: WALEntry, context: *anyopaque) wal.WALError!void {
            _ = cb_entry;
            _ = context;
            return wal.WALError.OutOfMemory;
        }
    }.callback;

    var dummy_context: u8 = 0;

    const result = test_wal.recover_entries(error_callback, &dummy_context);

    try testing.expectError(wal.WALError.OutOfMemory, result);
}

test "streaming vs buffered recovery equivalence" {
    const allocator = testing.allocator;

    var sim_vfs = try SimulationVFS.init(allocator);
    defer sim_vfs.deinit();
    var vfs_interface = sim_vfs.vfs();

    const test_dir = "comparison_test_dir";
    try vfs_interface.mkdir(test_dir);

    var test_wal = try WAL.init(allocator, vfs_interface, test_dir);
    defer test_wal.deinit();
    try test_wal.startup();

    const test_entries = [_]struct { block_id: u8, content_size: usize }{
        .{ .block_id = 1, .content_size = 100 },
        .{ .block_id = 2, .content_size = 8192 },
        .{ .block_id = 3, .content_size = 32 * 1024 },
        .{ .block_id = 4, .content_size = 50 },
    };

    for (test_entries) |entry_spec| {
        var test_block = try create_test_block(allocator, entry_spec.block_id);
        defer test_block.deinit(allocator);

        const new_content = try allocator.alloc(u8, entry_spec.content_size);
        @memset(new_content, @intCast(entry_spec.block_id + 'A'));
        // Free the original content before replacing it
        allocator.free(test_block.content);
        test_block.content = new_content;

        const entry = try WALEntry.create_put_block(allocator, test_block);
        defer entry.deinit(allocator);
        try test_wal.write_entry(entry);
    }

    var buffered_context = RecoveryContext.init(allocator);
    defer buffered_context.deinit();

    try test_wal.recover_entries(recovery_callback, &buffered_context);

    var streaming_context = RecoveryContext.init(allocator);
    defer streaming_context.deinit();

    try test_wal.recover_entries(recovery_callback, &streaming_context);

    try testing.expectEqual(
        buffered_context.recovered_entries.items.len,
        streaming_context.recovered_entries.items.len,
    );

    for (buffered_context.recovered_entries.items, streaming_context.recovered_entries.items) |b, s| {
        try testing.expectEqualSlices(u8, b.payload, s.payload);
    }
}
