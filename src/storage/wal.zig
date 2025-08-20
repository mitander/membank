//! Write-Ahead Log (WAL) implementation for KausalDB storage engine.
//!
//! Modular WAL implementation providing:
//! - Segmented file management with automatic rotation
//! - Streaming recovery with corruption resilience
//! - Entry serialization and validation
//! - Statistics tracking and monitoring
//!
//! Design rationale: 64MB segments prevent individual files from becoming
//! unmanageably large while allowing parallel recovery processing. Streaming
//! recovery avoids loading entire segments into memory, critical for embedded
//! deployments with memory constraints. CRC-64 provides strong corruption
//! detection while maintaining deterministic performance characteristics.

const std = @import("std");

const core = @import("wal/core.zig");
const entry = @import("wal/entry.zig");
const recovery = @import("wal/recovery.zig");
const types = @import("wal/types.zig");

pub const recover_from_segment = recovery.recover_from_segment;
pub const recover_from_segments = recovery.recover_from_segments;

pub const WALError = types.WALError;
pub const WALEntryType = types.WALEntryType;
pub const WALStats = types.WALStats;
pub const RecoveryCallback = types.RecoveryCallback;
pub const WALEntry = entry.WALEntry;

pub const MAX_SEGMENT_SIZE = types.MAX_SEGMENT_SIZE;
pub const MAX_PAYLOAD_SIZE = types.MAX_PAYLOAD_SIZE;
pub const WAL_FILE_PREFIX = types.WAL_FILE_PREFIX;
pub const WAL_FILE_SUFFIX = types.WAL_FILE_SUFFIX;
pub const WAL_FILE_NUMBER_DIGITS = types.WAL_FILE_NUMBER_DIGITS;
pub const MAX_PATH_LENGTH = types.MAX_PATH_LENGTH;
pub const WAL = core.WAL;

const testing = std.testing;
const context_block = @import("../core/types.zig");
const vfs = @import("../core/vfs.zig");
const simulation_vfs = @import("../sim/simulation_vfs.zig");

const ContextBlock = context_block.ContextBlock;
const BlockId = context_block.BlockId;
const SimulationVFS = simulation_vfs.SimulationVFS;

test "WAL entry serialization roundtrip" {
    const allocator = testing.allocator;

    const test_block = ContextBlock{
        .id = BlockId.from_hex("0123456789abcdeffedcba9876543210") catch unreachable, // Safety: hardcoded valid hex
        .version = 1,
        .source_uri = "test://example",
        .metadata_json = "{}",
        .content = "Hello, WAL!",
    };

    var wal_entry = try WALEntry.create_put_block(allocator, test_block);
    defer wal_entry.deinit(allocator);

    const serialized_size = WALEntry.HEADER_SIZE + wal_entry.payload.len;
    const buffer = try allocator.alloc(u8, serialized_size);
    defer allocator.free(buffer);

    const bytes_written = try wal_entry.serialize(buffer);
    try testing.expect(bytes_written == serialized_size);

    var deserialized_entry = try WALEntry.deserialize(allocator, buffer);
    defer deserialized_entry.deinit(allocator);

    try testing.expect(deserialized_entry.checksum == wal_entry.checksum);
    try testing.expect(deserialized_entry.entry_type == wal_entry.entry_type);
    try testing.expect(deserialized_entry.payload_size == wal_entry.payload_size);
    try testing.expect(std.mem.eql(u8, deserialized_entry.payload, wal_entry.payload));
}

test "WAL basic write and recovery" {
    const allocator = testing.allocator;

    var sim_vfs = try SimulationVFS.init(allocator);
    defer sim_vfs.deinit();

    const vfs_interface = sim_vfs.vfs();
    const wal_dir = "test_wal";

    var wal = try WAL.init(allocator, vfs_interface, wal_dir);
    defer wal.deinit();
    try wal.startup();

    const test_block = ContextBlock{
        .id = BlockId.from_hex("1234567890abcdef1234567890abcdef") catch unreachable, // Safety: hardcoded valid hex
        .version = 1,
        .source_uri = "test://source",
        .metadata_json = "{}",
        .content = "Test content",
    };

    var block_entry = try WALEntry.create_put_block(allocator, test_block);
    defer block_entry.deinit(allocator);

    try wal.write_entry(block_entry);

    const stats = wal.statistics();
    try testing.expect(stats.entries_written == 1);
    try testing.expect(stats.bytes_written > 0);

    const RecoveryContext = struct {
        entries_recovered: u32 = 0,
        allocator: std.mem.Allocator,
    };

    const callback = struct {
        fn recover(recovered_entry: WALEntry, ctx: *anyopaque) WALError!void {
            _ = recovered_entry; // Consumed by the callback, cleaned up by recovery system
            const context: *RecoveryContext = @ptrCast(@alignCast(ctx));
            context.entries_recovered += 1;
        }
    }.recover;

    var context = RecoveryContext{ .allocator = allocator };
    try wal.recover_entries(callback, &context);

    try testing.expect(context.entries_recovered == 1);
}

test "WAL segment rotation" {
    const allocator = testing.allocator;

    var sim_vfs = try SimulationVFS.init(allocator);
    defer sim_vfs.deinit();

    const vfs_interface = sim_vfs.vfs();
    const wal_dir = "test_rotation";

    var wal = try WAL.init(allocator, vfs_interface, wal_dir);
    defer wal.deinit();
    try wal.startup();

    const large_content = try allocator.alloc(u8, 1024 * 1024); // 1MB
    defer allocator.free(large_content);
    @memset(large_content, 'A');

    var entries_written: u32 = 0;
    const segments_before: u32 = 0;

    while (entries_written < 100) { // Reasonable upper bound
        const test_block = ContextBlock{
            .id = try BlockId.from_hex("deadbeefdeadbeefdeadbeefdeadbeef"),
            .version = 1,
            .source_uri = "test://large",
            .metadata_json = "{}",
            .content = large_content,
        };

        var block_entry = try WALEntry.create_put_block(allocator, test_block);
        defer block_entry.deinit(allocator);

        try wal.write_entry(block_entry);
        entries_written += 1;

        const stats = wal.statistics();
        if (stats.segments_rotated > segments_before) {
            try testing.expect(stats.segments_rotated == 1);
            break;
        }
    }

    const final_stats = wal.statistics();
    try testing.expect(final_stats.segments_rotated > 0);
}

test "WAL corruption resilience" {
    const allocator = testing.allocator;

    try testing.expectError(WALError.InvalidEntryType, WALEntryType.from_u8(0));
    try testing.expectError(WALError.InvalidEntryType, WALEntryType.from_u8(255));

    const oversized_payload = try allocator.alloc(u8, MAX_PAYLOAD_SIZE + 1);
    defer allocator.free(oversized_payload);

    const invalid_entry = WALEntry{
        .checksum = 0,
        .entry_type = .put_block,
        .payload_size = @intCast(oversized_payload.len),
        .payload = oversized_payload,
    };

    const buffer = try allocator.alloc(u8, oversized_payload.len + WALEntry.HEADER_SIZE);
    defer allocator.free(buffer);

    try testing.expectError(WALError.BufferTooSmall, invalid_entry.serialize(buffer[0..100]));
}
