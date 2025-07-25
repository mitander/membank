//! WAL Recovery Logic
//!
//! Contains all WAL recovery functionality including segment-level recovery
//! and streaming recovery using the WALEntryStream abstraction. This module
//! handles corruption detection, error recovery, and callback management.

const std = @import("std");
const builtin = @import("builtin");
const assert = std.debug.assert;
const log = std.log.scoped(.wal_recovery);

const types = @import("types.zig");
const entry_mod = @import("entry.zig");
const wal_entry_stream = @import("stream.zig");
const vfs = @import("../../core/vfs.zig");

const WALError = types.WALError;
const RecoveryCallback = types.RecoveryCallback;
const MAX_PATH_LENGTH = types.MAX_PATH_LENGTH;
const WALEntry = entry_mod.WALEntry;
const VFS = vfs.VFS;
const VFile = vfs.VFile;

/// Recover entries from a single WAL segment file using streaming approach
/// Processes entries one at a time to minimize memory usage and improve corruption resilience
pub fn recover_from_segment(
    filesystem: VFS,
    allocator: std.mem.Allocator,
    file_path: []const u8,
    callback: RecoveryCallback,
    context: *anyopaque,
    stats: *types.WALStats,
) WALError!void {
    // Path validation prevents buffer overflows in file operations
    assert(file_path.len > 0);
    assert(file_path.len < MAX_PATH_LENGTH);

    // Defensive limits to prevent runaway processing
    const MAX_CORRUPTION_SKIPS = 8192;
    const MAX_ENTRIES_PER_SEGMENT = 1_000_000;
    var corruption_skips: u32 = 0;
    var entries_processed: u32 = 0;

    var file = filesystem.open(file_path, .read) catch |err| switch (err) {
        error.FileNotFound => return WALError.FileNotFound,
        error.AccessDenied => return WALError.AccessDenied,
        error.OutOfMemory => return WALError.OutOfMemory,
        else => return WALError.IoError,
    };
    defer file.close();

    var stream = wal_entry_stream.WALEntryStream.init(allocator, &file) catch |err| switch (err) {
        wal_entry_stream.StreamError.OutOfMemory => return WALError.OutOfMemory,
        wal_entry_stream.StreamError.IoError => return WALError.IoError,
        else => return WALError.IoError,
    };

    var entries_recovered: u32 = 0;

    while (true) {
        // Defensive check: prevent runaway processing
        entries_processed += 1;
        if (entries_processed > MAX_ENTRIES_PER_SEGMENT) {
            log.err("WAL segment exceeded maximum entries limit: {d}", .{MAX_ENTRIES_PER_SEGMENT});
            return WALError.IoError;
        }

        const stream_entry = stream.next() catch |err| switch (err) {
            wal_entry_stream.StreamError.EndOfFile => break,
            wal_entry_stream.StreamError.CorruptedEntry => {
                corruption_skips += 1;
                if (corruption_skips > MAX_CORRUPTION_SKIPS) {
                    log.warn("WAL recovery failed after {d} corruption skips", .{MAX_CORRUPTION_SKIPS});
                    return WALError.CorruptedEntry;
                }
                continue;
            },
            wal_entry_stream.StreamError.EntryTooLarge => {
                log.warn("Entry too large in WAL stream", .{});
                corruption_skips += 1;
                if (corruption_skips > MAX_CORRUPTION_SKIPS) {
                    return WALError.CorruptedEntry;
                }
                continue;
            },
            wal_entry_stream.StreamError.IoError => return WALError.IoError,
            wal_entry_stream.StreamError.OutOfMemory => return WALError.OutOfMemory,
        } orelse break;

        defer stream_entry.deinit(allocator);

        // Convert stream entry to WAL entry format
        const wal_entry = WALEntry.from_stream_entry(stream_entry, allocator) catch |err| switch (err) {
            WALError.InvalidChecksum, WALError.InvalidEntryType => {
                corruption_skips += 1;
                if (corruption_skips > MAX_CORRUPTION_SKIPS) {
                    log.warn("WAL recovery failed: too many checksum/type conversion failures: {d}", .{MAX_CORRUPTION_SKIPS});
                    return WALError.CorruptedEntry;
                }
                continue;
            },
            else => return err,
        };
        defer wal_entry.deinit(allocator);

        callback(wal_entry, context) catch |err| return err;
        entries_recovered += 1;
    }

    stats.entries_recovered += entries_recovered;
    log.info("Recovered {d} entries from segment: {s}", .{ entries_recovered, file_path });
}

/// Recover entries from multiple WAL segments in chronological order
/// Segments are processed in order based on their file names to ensure replay consistency
pub fn recover_from_segments(
    filesystem: VFS,
    allocator: std.mem.Allocator,
    directory: []const u8,
    callback: RecoveryCallback,
    context: *anyopaque,
    stats: *types.WALStats,
) WALError!void {
    const segment_files = try list_segment_files(filesystem, allocator, directory);
    defer {
        for (segment_files) |file_name| {
            allocator.free(file_name);
        }
        allocator.free(segment_files);
    }

    const initial_recovery_failures = stats.recovery_failures;

    // Chronological processing ensures consistent replay ordering
    for (segment_files) |file_name| {
        assert(file_name.len > 0);
        assert(std.mem.startsWith(u8, file_name, "wal_"));
        assert(std.mem.endsWith(u8, file_name, ".log"));

        const file_path = try std.fmt.allocPrint(
            allocator,
            "{s}/{s}",
            .{ directory, file_name },
        );
        defer allocator.free(file_path);

        recover_from_segment(filesystem, allocator, file_path, callback, context, stats) catch |err| switch (err) {
            WALError.InvalidChecksum, WALError.InvalidEntryType, WALError.CorruptedEntry => {
                log.warn("WAL corruption detected in {s}, skipping segment", .{file_path});
                stats.recovery_failures += 1;
                continue;
            },
            else => return err,
        };
    }

    // Monotonic failure count prevents counter manipulation bugs
    assert(stats.recovery_failures >= initial_recovery_failures);
}

/// List all WAL segment files in the directory, sorted in chronological order
fn list_segment_files(filesystem: VFS, allocator: std.mem.Allocator, directory: []const u8) WALError![][]const u8 {
    var file_list = std.ArrayList([]const u8).init(allocator);
    defer file_list.deinit();

    var dir_iter = filesystem.iterate_directory(directory, allocator) catch |err| switch (err) {
        error.FileNotFound => return WALError.FileNotFound,
        error.AccessDenied => return WALError.AccessDenied,
        error.OutOfMemory => return WALError.OutOfMemory,
        else => return WALError.IoError,
    };
    defer dir_iter.deinit(allocator);

    while (dir_iter.next()) |entry| {
        if (entry.kind != .file) continue;

        if (std.mem.startsWith(u8, entry.name, types.WAL_FILE_PREFIX) and
            std.mem.endsWith(u8, entry.name, types.WAL_FILE_SUFFIX))
        {
            const owned_name = allocator.dupe(u8, entry.name) catch return WALError.OutOfMemory;
            file_list.append(owned_name) catch return WALError.OutOfMemory;
        }
    }

    const files = file_list.toOwnedSlice() catch return WALError.OutOfMemory;

    // Sort files by name to ensure chronological processing
    // WAL files are named wal_NNNN.log where NNNN is sequential
    std.sort.insertion([]const u8, files, {}, struct {
        fn less_than(_: void, lhs: []const u8, rhs: []const u8) bool {
            return std.mem.order(u8, lhs, rhs) == .lt;
        }
    }.less_than);

    return files;
}
