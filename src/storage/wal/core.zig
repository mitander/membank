//! WAL Core Management
//!
//! Main WAL struct and core operations including initialization, write operations,
//! segment management, and coordination with recovery subsystem. This module
//! provides the primary interface for WAL operations.

const std = @import("std");
const builtin = @import("builtin");
const assert = std.debug.assert;
const log = std.log.scoped(.wal);

const types = @import("types.zig");
const entry_mod = @import("entry.zig");
const recovery = @import("recovery.zig");
const vfs = @import("../../core/vfs.zig");
const concurrency = @import("../../core/concurrency.zig");
const error_context = @import("../../core/error_context.zig");

const WALError = types.WALError;
const WALStats = types.WALStats;
const RecoveryCallback = types.RecoveryCallback;
const WALEntry = entry_mod.WALEntry;
const MAX_SEGMENT_SIZE = types.MAX_SEGMENT_SIZE;
const MAX_PAYLOAD_SIZE = types.MAX_PAYLOAD_SIZE;
const WAL_FILE_PREFIX = types.WAL_FILE_PREFIX;
const WAL_FILE_SUFFIX = types.WAL_FILE_SUFFIX;
const WAL_FILE_NUMBER_DIGITS = types.WAL_FILE_NUMBER_DIGITS;
const VFS = vfs.VFS;
const VFile = vfs.VFile;

/// Write-Ahead Log manager with segmented files and streaming recovery
pub const WAL = struct {
    directory: []const u8,
    vfs: VFS,
    active_file: ?VFile,
    segment_number: u32,
    segment_size: u64,
    allocator: std.mem.Allocator,
    stats: WALStats,

    // File naming scheme limits maximum segments, preventing overflow
    comptime {
        assert(@sizeOf(u32) == 4);
        assert(@sizeOf(u64) == 8);

        const max_segment_number = std.math.pow(u32, 10, WAL_FILE_NUMBER_DIGITS) - 1;
        assert(max_segment_number > 1000);
    }

    /// Initialize WAL with specified directory and filesystem interface.
    /// Creates directory if it doesn't exist and discovers existing segments for continuation.
    /// Returns WALError.AccessDenied if directory is not writable.
    pub fn init(allocator: std.mem.Allocator, filesystem: VFS, directory: []const u8) WALError!WAL {
        assert(directory.len > 0);
        assert(directory.len < 4096); // Reasonable path length limit

        var vfs_copy = filesystem;
        vfs_copy.mkdir(directory) catch |err| switch (err) {
            error.FileExists => {},
            error.AccessDenied => return WALError.AccessDenied,
            error.FileNotFound => return WALError.FileNotFound,
            error.OutOfMemory => return WALError.OutOfMemory,
            else => return WALError.IoError,
        };

        var wal = WAL{
            .directory = try allocator.dupe(u8, directory),
            .vfs = filesystem,
            .active_file = null,
            .segment_number = 0,
            .segment_size = 0,
            .allocator = allocator,
            .stats = WALStats.init(),
        };

        // Initialization state must be consistent before segment discovery
        assert(wal.directory.len > 0);
        assert(wal.segment_number == 0);
        assert(wal.segment_size == 0);
        assert(wal.active_file == null);

        try wal.initialize_active_segment();

        return wal;
    }

    /// Clean up WAL resources and close active files
    pub fn deinit(self: *WAL) void {
        if (self.active_file) |*file| {
            file.close();
        }
        self.allocator.free(self.directory);
    }

    /// Write entry to WAL with automatic segment rotation and durability guarantee.
    /// Entry is immediately flushed to disk before returning.
    /// Returns WALError.IoError if disk space exhausted or I/O failure occurs.
    pub fn write_entry(self: *WAL, entry: WALEntry) WALError!void {
        const serialized_size = try self.validate_entry_for_write(entry);
        try self.ensure_segment_capacity(serialized_size);

        const write_buffer = try self.serialize_with_validation(entry, serialized_size);
        defer self.allocator.free(write_buffer);

        const bytes_written = try self.write_and_verify(entry, write_buffer);
        self.update_write_stats(bytes_written);
    }

    /// Validate entry structure and state before write operations
    fn validate_entry_for_write(self: *WAL, entry: WALEntry) WALError!usize {
        concurrency.assert_main_thread();

        // Entry validation prevents corruption from propagating to disk
        assert(self.active_file != null);
        assert(entry.payload.len <= MAX_PAYLOAD_SIZE);
        assert(entry.payload_size == entry.payload.len);

        if (self.active_file == null) return WALError.NotInitialized;

        const serialized_size = WALEntry.HEADER_SIZE + entry.payload.len;
        assert(serialized_size > WALEntry.HEADER_SIZE);
        assert(serialized_size <= WALEntry.HEADER_SIZE + MAX_PAYLOAD_SIZE);

        return serialized_size;
    }

    /// Ensure active segment has capacity for entry, rotating if necessary
    fn ensure_segment_capacity(self: *WAL, required_size: usize) WALError!void {
        // Rotation before write prevents partial entries spanning segments
        if (self.segment_size + required_size > MAX_SEGMENT_SIZE) {
            assert(self.segment_size <= MAX_SEGMENT_SIZE);
            try self.rotate_segment();
            assert(self.segment_size == 0);
            assert(self.active_file != null);
        }
    }

    /// Serialize entry with validation and corruption detection
    fn serialize_with_validation(self: *WAL, entry: WALEntry, serialized_size: usize) WALError![]u8 {
        // CRITICAL: Use completely separate allocator for WAL serialization buffer
        // to prevent any memory sharing with the entry payload data
        var write_arena = std.heap.ArenaAllocator.init(self.allocator);
        defer write_arena.deinit();
        const write_allocator = write_arena.allocator();

        // Allocate write buffer with isolation barriers
        const write_buffer = try write_allocator.alloc(u8, serialized_size);
        @memset(write_buffer, 0xDD); // Fill with distinctive pattern first
        @memset(write_buffer, 0); // Then zero-initialize for actual use

        const bytes_written = try entry.serialize(write_buffer);
        assert(bytes_written == serialized_size);

        // Early corruption detection: validate write_buffer before write
        if (write_buffer.len >= WALEntry.HEADER_SIZE) {
            const serialized_checksum = std.mem.readInt(u64, write_buffer[0..8], .little);
            const serialized_type = write_buffer[8];
            const serialized_payload_size = std.mem.readInt(u32, write_buffer[9..13], .little);

            // Detect garbage values that indicate memory corruption
            if (serialized_checksum == 0x5555555555555555 or
                serialized_checksum == 0xAAAAAAAAAAAAAAAA or
                serialized_payload_size > MAX_PAYLOAD_SIZE or
                serialized_type > 3)
            {
                log.err("WAL corruption detected before write: checksum=0x{X} type={} payload_size={}", .{ serialized_checksum, serialized_type, serialized_payload_size });
                return WALError.CorruptedEntry;
            }

            // Verify checksum matches expected value
            if (serialized_checksum != entry.checksum) {
                log.err("WAL checksum mismatch: expected 0x{X}, got 0x{X}", .{ entry.checksum, serialized_checksum });
                return WALError.InvalidChecksum;
            }

            // Additional corruption check: verify no 'xxxx' pattern in header
            if (serialized_payload_size == 0x78787878) {
                log.err("WAL header corrupted with content pattern (0x78787878)", .{});
                return WALError.CorruptedEntry;
            }
        }

        // Return owned buffer - caller must manage memory
        return try self.allocator.dupe(u8, write_buffer);
    }

    /// Write buffer to file with immediate verification
    fn write_and_verify(self: *WAL, entry: WALEntry, write_buffer: []const u8) WALError!usize {
        // Write entire WAL entry as single atomic operation with validation
        const written = self.active_file.?.write(write_buffer) catch return WALError.IoError;
        assert(written == write_buffer.len);

        if (written != write_buffer.len) {
            log.err("WAL write incomplete: expected {}, got {}", .{ write_buffer.len, written });
            return WALError.IoError;
        }

        self.active_file.?.flush() catch return WALError.IoError;

        // Immediate verification: read back WAL header to detect corruption
        if (write_buffer.len >= WALEntry.HEADER_SIZE) {
            const current_pos = self.active_file.?.tell() catch return WALError.IoError;
            const verify_pos = current_pos - write_buffer.len;

            _ = self.active_file.?.seek(@intCast(verify_pos), .start) catch return WALError.IoError;

            var verify_header: [WALEntry.HEADER_SIZE]u8 = undefined;
            const header_read = self.active_file.?.read(&verify_header) catch return WALError.IoError;
            if (header_read == WALEntry.HEADER_SIZE) {
                const verify_checksum = std.mem.readInt(u64, verify_header[0..8], .little);
                const verify_type = verify_header[8];
                const verify_payload_size = std.mem.readInt(u32, verify_header[9..13], .little);

                if (verify_checksum != entry.checksum or
                    verify_type != @intFromEnum(entry.entry_type) or
                    verify_payload_size != entry.payload_size)
                {
                    log.err("WAL write corruption detected: written header differs from buffer", .{});
                    log.err("Expected: checksum=0x{X}, type={}, payload_size={}", .{ entry.checksum, @intFromEnum(entry.entry_type), entry.payload_size });
                    log.err("Verified: checksum=0x{X}, type={}, payload_size={}", .{ verify_checksum, verify_type, verify_payload_size });
                    return WALError.CorruptedEntry;
                }
            }

            // Restore file position
            _ = self.active_file.?.seek(@intCast(current_pos), .start) catch return WALError.IoError;
        }

        return written;
    }

    /// Update WAL statistics after successful write
    fn update_write_stats(self: *WAL, bytes_written: usize) void {
        const old_segment_size = self.segment_size;
        self.segment_size += bytes_written;
        self.stats.entries_written += 1;
        self.stats.bytes_written += bytes_written;

        // Size tracking consistency prevents segment overflow bugs
        assert(self.segment_size == old_segment_size + bytes_written);
        assert(self.segment_size <= MAX_SEGMENT_SIZE);
        assert(self.stats.entries_written > 0);
        assert(self.stats.bytes_written >= bytes_written);
    }

    /// Recover all entries from WAL segments in chronological order.
    /// Callback is invoked for each valid entry; corrupted entries are skipped.
    /// Returns WALError.FileNotFound if no WAL segments exist (normal for new database).
    pub fn recover_entries(self: *WAL, callback: RecoveryCallback, context: *anyopaque) WALError!void {
        concurrency.assert_main_thread();
        return recovery.recover_from_segments(
            self.vfs,
            self.allocator,
            self.directory,
            callback,
            context,
            &self.stats,
        );
    }

    /// Get current WAL operation statistics including entry counts and recovery metrics.
    pub fn statistics(self: *const WAL) WALStats {
        return self.stats;
    }

    /// List all WAL segment files in the directory, sorted in chronological order
    fn list_segment_files(self: *WAL) WALError![][]const u8 {
        var file_list = std.ArrayList([]const u8).init(self.allocator);
        defer file_list.deinit();

        var dir_iter = self.vfs.iterate_directory(self.directory, self.allocator) catch |err| switch (err) {
            error.FileNotFound => return WALError.FileNotFound,
            error.AccessDenied => return WALError.AccessDenied,
            error.OutOfMemory => return WALError.OutOfMemory,
            else => return WALError.IoError,
        };
        defer dir_iter.deinit(self.allocator);

        while (dir_iter.next()) |entry| {
            if (entry.kind != .file) continue;

            if (std.mem.startsWith(u8, entry.name, types.WAL_FILE_PREFIX) and
                std.mem.endsWith(u8, entry.name, types.WAL_FILE_SUFFIX))
            {
                const owned_name = self.allocator.dupe(u8, entry.name) catch return WALError.OutOfMemory;
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

    /// Clean up old WAL segments, keeping only the current active segment.
    /// This should be called after successfully flushing data to SSTable.
    pub fn cleanup_old_segments(self: *WAL) WALError!void {
        concurrency.assert_main_thread();

        const segment_files = try self.list_segment_files();
        defer {
            for (segment_files) |file_name| {
                self.allocator.free(file_name);
            }
            self.allocator.free(segment_files);
        }

        // If we only have 1 or fewer segments, no cleanup needed
        if (segment_files.len <= 1) {
            return;
        }

        // Remove all segments except the last one (most recent)
        // Keep the active segment for ongoing writes
        for (segment_files[0 .. segment_files.len - 1]) |file_name| {
            const file_path = try std.fmt.allocPrint(
                self.allocator,
                "{s}/{s}",
                .{ self.directory, file_name },
            );
            defer self.allocator.free(file_path);

            self.vfs.remove(file_path) catch |err| switch (err) {
                error.FileNotFound => {
                    // File already removed, continue
                    continue;
                },
                else => return WALError.IoError,
            };

            log.info("Cleaned up old WAL segment: {s}", .{file_path});
        }
    }

    /// Rotate to a new WAL segment, closing the current one
    fn rotate_segment(self: *WAL) WALError!void {
        // Close current segment
        if (self.active_file) |*file| {
            file.flush() catch return WALError.IoError;
            file.close();
            self.active_file = null;
        }

        self.segment_number += 1;
        self.segment_size = 0;
        self.stats.segments_rotated += 1;

        // Create new segment file
        try self.open_segment_file();

        log.info("Rotated to WAL segment {d}", .{self.segment_number});
    }

    /// Initialize the active segment by discovering existing segments or creating the first one
    fn initialize_active_segment(self: *WAL) WALError!void {
        // Discover highest numbered segment
        self.segment_number = try self.discover_latest_segment_number();

        // Try to open existing segment or create new one
        self.open_segment_file() catch |err| switch (err) {
            WALError.FileNotFound => {
                // No existing segment, start with segment 0
                self.segment_number = 0;
                try self.create_new_segment();
            },
            else => return err,
        };

        // Set segment size based on current file size
        if (self.active_file) |*file| {
            self.segment_size = file.file_size() catch 0;
        }
    }

    /// Discover the highest numbered segment in the directory
    fn discover_latest_segment_number(self: *WAL) WALError!u32 {
        var highest_number: u32 = 0;
        var found_any = false;

        var dir_iter = self.vfs.iterate_directory(self.directory, self.allocator) catch return 0;
        defer dir_iter.deinit(self.allocator);

        while (dir_iter.next()) |entry| {
            if (entry.kind != .file) continue;

            if (std.mem.startsWith(u8, entry.name, WAL_FILE_PREFIX) and
                std.mem.endsWith(u8, entry.name, WAL_FILE_SUFFIX))
            {
                const number_part = entry.name[WAL_FILE_PREFIX.len .. entry.name.len - WAL_FILE_SUFFIX.len];
                if (std.fmt.parseInt(u32, number_part, 10)) |number| {
                    if (number > highest_number) {
                        highest_number = number;
                        found_any = true;
                    }
                } else |_| {
                    // Invalid segment file name, skip
                    continue;
                }
            }
        }

        return if (found_any) highest_number else 0;
    }

    /// Open existing segment file for append operations
    fn open_segment_file(self: *WAL) WALError!void {
        const filename = try self.segment_filename();
        defer self.allocator.free(filename);

        // Try to open existing file first in read-write mode (needed for file_size calls)
        self.active_file = self.vfs.open(filename, .read_write) catch |open_err| switch (open_err) {
            error.FileNotFound => self.vfs.create(filename) catch |create_err| switch (create_err) {
                error.AccessDenied => return WALError.AccessDenied,
                error.OutOfMemory => return WALError.OutOfMemory,
                else => return WALError.IoError,
            },
            error.AccessDenied => return WALError.AccessDenied,
            error.OutOfMemory => return WALError.OutOfMemory,
            else => return WALError.IoError,
        };

        // Seek to end for append operations
        _ = self.active_file.?.seek(0, .end) catch return WALError.IoError;
    }

    /// Create a new segment file
    fn create_new_segment(self: *WAL) WALError!void {
        const filename = try self.segment_filename();
        defer self.allocator.free(filename);

        self.active_file = self.vfs.create(filename) catch |err| switch (err) {
            error.AccessDenied => return WALError.AccessDenied,
            error.OutOfMemory => return WALError.OutOfMemory,
            else => return WALError.IoError,
        };

        self.segment_size = 0;
    }

    /// Generate segment filename based on current segment number
    fn segment_filename(self: *WAL) ![]u8 {
        return std.fmt.allocPrint(
            self.allocator,
            "{s}/{s}{:0>4}{s}",
            .{ self.directory, WAL_FILE_PREFIX, self.segment_number, WAL_FILE_SUFFIX },
        );
    }
};
