//! Write-Ahead Log (WAL) implementation for CortexDB storage engine.
//!
//! Design rationale: 64MB segments prevent individual files from becoming
//! unmanageably large while allowing parallel recovery processing. Streaming
//! recovery avoids loading entire segments into memory, critical for embedded
//! deployments with memory constraints. CRC-64 provides strong corruption
//! detection while maintaining deterministic performance characteristics.

const std = @import("std");
const stdx = @import("stdx");
const log = std.log.scoped(.wal);
const assert = std.debug.assert;
const testing = std.testing;
const vfs = @import("vfs");
const context_block = @import("context_block");
const concurrency = @import("concurrency");
const error_context = @import("error_context");

const VFS = vfs.VFS;
const VFile = vfs.VFile;
const ContextBlock = context_block.ContextBlock;
const GraphEdge = context_block.GraphEdge;
const BlockId = context_block.BlockId;

/// Maximum size of a WAL segment before rotation (64MB).
/// Power of two for efficient alignment and bitwise operations.
pub const MAX_SEGMENT_SIZE: u64 = 64 * 1024 * 1024;

/// Maximum payload size for a single WAL entry (16MB).
/// Prevents memory exhaustion attacks and ensures reasonable entry sizes.
pub const MAX_PAYLOAD_SIZE: u32 = 16 * 1024 * 1024;

/// WAL file naming constants for consistency and validation
const WAL_FILE_PREFIX = "wal_";
const WAL_FILE_SUFFIX = ".log";
const WAL_FILE_NUMBER_DIGITS = 4; // Supports 0000-9999 segments

/// Maximum file path length for defensive checks
const MAX_PATH_LENGTH = 4096;

// Power-of-two requirement enables efficient bitwise operations for
// alignment checks and prevents pathological fragmentation patterns
comptime {
    if (MAX_SEGMENT_SIZE & (MAX_SEGMENT_SIZE - 1) != 0) {
        @compileError("MAX_SEGMENT_SIZE must be a power of two");
    }
    if (MAX_PAYLOAD_SIZE > MAX_SEGMENT_SIZE) {
        @compileError("MAX_PAYLOAD_SIZE cannot exceed MAX_SEGMENT_SIZE");
    }
    if (WAL_FILE_PREFIX.len == 0 or WAL_FILE_SUFFIX.len == 0) {
        @compileError("WAL file naming constants cannot be empty");
    }
    if (WAL_FILE_NUMBER_DIGITS < 1 or WAL_FILE_NUMBER_DIGITS > 8) {
        @compileError("WAL_FILE_NUMBER_DIGITS must be between 1 and 8");
    }
}

/// WAL-specific errors distinct from generic I/O failures
pub const WALError = error{
    NotInitialized,
    InvalidChecksum,
    InvalidEntryType,
    BufferTooSmall,
    CorruptedEntry,
    SegmentFull,
    FileNotFound,
    AccessDenied,
    OutOfMemory,
    IoError,
} || std.mem.Allocator.Error;

/// WAL entry types as defined in the data model specification
pub const WALEntryType = enum(u8) {
    put_block = 0x01,
    delete_block = 0x02,
    put_edge = 0x03,

    pub fn from_u8(value: u8) WALError!WALEntryType {
        return std.meta.intToEnum(WALEntryType, value) catch WALError.InvalidEntryType;
    }
};

/// WAL entry header structure with corruption detection
pub const WALEntry = struct {
    checksum: u64,
    entry_type: WALEntryType,
    payload_size: u32,
    payload: []const u8,

    pub const HEADER_SIZE = 13; // 8 bytes checksum + 1 byte type + 4 bytes payload_size

    // Cross-platform binary compatibility requires fixed field sizes
    comptime {
        assert(@sizeOf(u64) == 8);
        assert(@sizeOf(u32) == 4);
        assert(@sizeOf(WALEntryType) == 1);

        const calculated_header_size = @sizeOf(u64) + @sizeOf(WALEntryType) + @sizeOf(u32);
        assert(HEADER_SIZE == calculated_header_size);
        assert(HEADER_SIZE == 13);

        // Minimum size prevents degenerate entries that waste storage
        assert(MAX_PAYLOAD_SIZE >= 1024);
        assert(MAX_PAYLOAD_SIZE <= MAX_SEGMENT_SIZE);
        assert(std.math.maxInt(u32) >= MAX_PAYLOAD_SIZE);
    }

    /// Calculate CRC-64 checksum of type and payload for corruption detection
    fn calculate_checksum(entry_type: WALEntryType, payload: []const u8) u64 {
        var hasher = std.hash.Wyhash.init(0);
        hasher.update(&[_]u8{@intFromEnum(entry_type)});
        hasher.update(payload);
        return hasher.final();
    }

    /// Serialize WAL entry to buffer for disk storage.
    /// Returns number of bytes written or WALError.BufferTooSmall if insufficient space.
    pub fn serialize(self: WALEntry, buffer: []u8) WALError!usize {
        const total_size = HEADER_SIZE + self.payload.len;
        if (buffer.len < total_size) return WALError.BufferTooSmall;

        var offset: usize = 0;

        // Write checksum (8 bytes, little-endian)
        std.mem.writeInt(u64, buffer[offset..][0..8], self.checksum, .little);
        offset += 8;

        // Write entry type (1 byte)
        buffer[offset] = @intFromEnum(self.entry_type);
        offset += 1;

        // Write payload size (4 bytes, little-endian)
        std.mem.writeInt(u32, buffer[offset..][0..4], self.payload_size, .little);
        offset += 4;

        // Write payload
        @memcpy(buffer[offset .. offset + self.payload.len], self.payload);
        offset += self.payload.len;

        return offset;
    }

    /// Deserialize WAL entry from buffer, allocating payload memory.
    /// Caller must call deinit() to free allocated payload memory.
    /// Returns WALError.InvalidChecksum or WALError.InvalidEntryType for corruption.
    pub fn deserialize(buffer: []const u8, allocator: std.mem.Allocator) WALError!WALEntry {
        if (buffer.len < HEADER_SIZE) return WALError.BufferTooSmall;

        var offset: usize = 0;

        // Read checksum
        const checksum = std.mem.readInt(u64, buffer[offset..][0..8], .little);
        offset += 8;

        // Read entry type
        const entry_type = try WALEntryType.from_u8(buffer[offset]);
        offset += 1;

        // Read payload size
        const payload_size = std.mem.readInt(u32, buffer[offset..][0..4], .little);
        offset += 4;

        // Validate payload size against remaining buffer
        if (offset + payload_size > buffer.len) return WALError.BufferTooSmall;

        // Read payload
        const payload = try allocator.dupe(u8, buffer[offset .. offset + payload_size]);
        errdefer allocator.free(payload);

        // Verify checksum for corruption detection
        const expected_checksum = calculate_checksum(entry_type, payload);
        if (checksum != expected_checksum) {
            return WALError.InvalidChecksum;
        }

        return WALEntry{
            .checksum = checksum,
            .entry_type = entry_type,
            .payload_size = payload_size,
            .payload = payload,
        };
    }

    /// Create WAL entry for storing a Context Block.
    /// Serializes block data into WAL payload with integrity checksum.
    /// Returns WALError.CorruptedEntry if block serialization fails.
    pub fn create_put_block(block: ContextBlock, allocator: std.mem.Allocator) WALError!WALEntry {
        const payload_size = block.serialized_size();

        // Zero-size blocks indicate serialization logic failure, not data corruption
        if (payload_size == 0) {
            return WALError.CorruptedEntry;
        }
        if (payload_size > MAX_PAYLOAD_SIZE) {
            return WALError.CorruptedEntry;
        }

        const payload = try allocator.alloc(u8, payload_size);
        errdefer allocator.free(payload);

        const bytes_written = try block.serialize(payload);

        // Serialization size mismatch indicates internal logic error
        assert(bytes_written == payload_size);
        if (bytes_written != payload_size) {
            return WALError.CorruptedEntry;
        }

        const checksum = calculate_checksum(.put_block, payload);

        const entry = WALEntry{
            .checksum = checksum,
            .entry_type = .put_block,
            .payload_size = @intCast(payload_size),
            .payload = payload,
        };

        // Invariant: payload size consistency prevents downstream corruption
        assert(entry.payload.len == payload_size);
        assert(entry.payload_size == payload_size);

        return entry;
    }

    /// Create WAL entry for deleting a Context Block.
    /// Payload contains only the 16-byte BlockId for efficient deletion replay.
    pub fn create_delete_block(block_id: BlockId, allocator: std.mem.Allocator) WALError!WALEntry {
        comptime assert(@sizeOf(BlockId) == 16);

        const payload = try allocator.dupe(u8, &block_id.bytes);
        assert(payload.len == 16);

        const checksum = calculate_checksum(.delete_block, payload);

        const entry = WALEntry{
            .checksum = checksum,
            .entry_type = .delete_block,
            .payload_size = @intCast(payload.len),
            .payload = payload,
        };

        // Invariant: delete entries must contain exactly one BlockId
        assert(entry.payload.len == 16);
        assert(entry.payload_size == 16);

        return entry;
    }

    /// Create WAL entry for storing a Graph Edge.
    /// Serializes edge relationship data for graph index replay.
    pub fn create_put_edge(edge: GraphEdge, allocator: std.mem.Allocator) WALError!WALEntry {
        const payload = try allocator.alloc(u8, 40); // GraphEdge.SERIALIZED_SIZE
        errdefer allocator.free(payload);

        _ = try edge.serialize(payload);
        const checksum = calculate_checksum(.put_edge, payload);

        return WALEntry{
            .checksum = checksum,
            .entry_type = .put_edge,
            .payload_size = @intCast(payload.len),
            .payload = payload,
        };
    }

    /// Free allocated payload memory.
    /// Must be called for all entries created via deserialize() or create_*() methods.
    pub fn deinit(self: WALEntry, allocator: std.mem.Allocator) void {
        // Size consistency prevents double-free and use-after-free bugs
        assert(self.payload.len == self.payload_size);

        if (self.payload.len > 0) {
            allocator.free(self.payload);
        }
    }
};

/// Recovery callback enables pluggable WAL replay strategies
pub const RecoveryCallback = *const fn (entry: WALEntry, context: *anyopaque) WALError!void;

/// Statistics for WAL operations and recovery
pub const WALStats = struct {
    entries_written: u64,
    entries_recovered: u64,
    segments_rotated: u32,
    recovery_failures: u32,
    bytes_written: u64,

    pub fn init() WALStats {
        return WALStats{
            .entries_written = 0,
            .entries_recovered = 0,
            .segments_rotated = 0,
            .recovery_failures = 0,
            .bytes_written = 0,
        };
    }
};

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

        // Active file must exist after successful initialization
        assert(wal.active_file != null);

        return wal;
    }

    /// Release all WAL resources including active file handles and directory path.
    /// Safe to call multiple times.
    pub fn deinit(self: *WAL) void {
        if (self.active_file) |*file| {
            file.close();
            file.deinit();
        }
        self.allocator.free(self.directory);
    }

    /// Write entry to WAL with automatic segment rotation and durability guarantee.
    /// Entry is immediately flushed to disk before returning.
    /// Returns WALError.IoError if disk space exhausted or I/O failure occurs.
    pub fn write_entry(self: *WAL, entry: WALEntry) WALError!void {
        concurrency.assert_main_thread();

        // Entry validation prevents corruption from propagating to disk
        assert(self.active_file != null);
        assert(entry.payload.len <= MAX_PAYLOAD_SIZE);
        assert(entry.payload_size == entry.payload.len);

        if (self.active_file == null) return WALError.NotInitialized;

        const serialized_size = WALEntry.HEADER_SIZE + entry.payload.len;
        assert(serialized_size > WALEntry.HEADER_SIZE);
        assert(serialized_size <= WALEntry.HEADER_SIZE + MAX_PAYLOAD_SIZE);

        // Rotation before write prevents partial entries spanning segments
        if (self.segment_size + serialized_size > MAX_SEGMENT_SIZE) {
            assert(self.segment_size <= MAX_SEGMENT_SIZE);
            try self.rotate_segment();
            assert(self.segment_size == 0);
            assert(self.active_file != null);
        }

        const buffer = try self.allocator.alloc(u8, serialized_size);
        defer self.allocator.free(buffer);

        const bytes_written = try entry.serialize(buffer);
        assert(bytes_written == serialized_size);

        const written = self.active_file.?.write(buffer) catch return WALError.IoError;
        assert(written == buffer.len);

        self.active_file.?.flush() catch return WALError.IoError;

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
        const segment_files = try self.list_segment_files();
        defer {
            for (segment_files) |file_name| {
                self.allocator.free(file_name);
            }
            self.allocator.free(segment_files);
        }

        const initial_recovery_failures = self.stats.recovery_failures;

        // Chronological processing ensures consistent replay ordering
        for (segment_files) |file_name| {
            assert(file_name.len > 0);
            assert(std.mem.startsWith(u8, file_name, "wal_"));
            assert(std.mem.endsWith(u8, file_name, ".log"));

            const file_path = try std.fmt.allocPrint(
                self.allocator,
                "{s}/{s}",
                .{ self.directory, file_name },
            );
            defer self.allocator.free(file_path);

            self.recover_from_segment(file_path, callback, context) catch |err| switch (err) {
                WALError.InvalidChecksum, WALError.InvalidEntryType, WALError.CorruptedEntry => {
                    log.warn("WAL corruption detected in {s}, skipping segment", .{file_path});
                    self.stats.recovery_failures += 1;
                    continue;
                },
                else => return err,
            };
        }

        // Monotonic failure count prevents counter manipulation bugs
        assert(self.stats.recovery_failures >= initial_recovery_failures);
    }

    /// Get current WAL operation statistics including entry counts and recovery metrics.
    pub fn statistics(self: *const WAL) WALStats {
        return self.stats;
    }

    fn initialize_active_segment(self: *WAL) WALError!void {
        // Clean initialization state prevents double-initialization bugs
        assert(self.active_file == null);
        assert(self.segment_number == 0);
        assert(self.segment_size == 0);

        const existing_segments = try self.list_segment_files();
        defer {
            for (existing_segments) |file_name| {
                self.allocator.free(file_name);
            }
            self.allocator.free(existing_segments);
        }

        if (existing_segments.len > 0) {
            const latest_segment = existing_segments[existing_segments.len - 1];
            assert(latest_segment.len > 0);
            assert(std.mem.startsWith(u8, latest_segment, WAL_FILE_PREFIX));
            assert(std.mem.endsWith(u8, latest_segment, WAL_FILE_SUFFIX));

            // Parse segment number from filename for continuation
            if (std.mem.indexOf(u8, latest_segment, "_")) |start_idx| {
                if (std.mem.indexOf(u8, latest_segment[start_idx + 1 ..], ".")) |end_idx| {
                    const num_str = latest_segment[start_idx + 1 .. start_idx + 1 + end_idx];
                    assert(num_str.len == WAL_FILE_NUMBER_DIGITS);
                    self.segment_number = std.fmt.parseInt(u32, num_str, 10) catch {
                        log.warn("Invalid segment number in filename: {s}", .{latest_segment});
                        return WALError.CorruptedEntry;
                    };
                }
            }

            const segment_path = try std.fmt.allocPrint(
                self.allocator,
                "{s}/{s}{d:0>4}{s}",
                .{ self.directory, WAL_FILE_PREFIX, self.segment_number, WAL_FILE_SUFFIX },
            );
            defer self.allocator.free(segment_path);
            assert(segment_path.len < MAX_PATH_LENGTH);

            if (self.vfs.exists(segment_path)) {
                const vfile = self.vfs.open(segment_path, .read_write) catch |err| switch (err) {
                    error.FileNotFound => return WALError.FileNotFound,
                    error.AccessDenied => return WALError.AccessDenied,
                    error.OutOfMemory => return WALError.OutOfMemory,
                    else => return WALError.IoError,
                };
                self.active_file = vfile;
                const end_pos = self.active_file.?.seek(0, VFile.SeekFrom.end) catch |err| switch (err) {
                    error.InvalidSeek => return WALError.IoError,
                    else => return WALError.IoError,
                };
                self.segment_size = @intCast(end_pos);

                // Oversized segments indicate corruption or configuration drift
                assert(self.segment_size <= MAX_SEGMENT_SIZE);
                if (self.segment_size > MAX_SEGMENT_SIZE) {
                    log.warn("Existing segment size exceeds maximum: {d} > {d}", .{ self.segment_size, MAX_SEGMENT_SIZE });
                    return WALError.CorruptedEntry;
                }
            } else {
                const vfile = self.vfs.create(segment_path) catch |err| switch (err) {
                    error.AccessDenied => return WALError.AccessDenied,
                    error.OutOfMemory => return WALError.OutOfMemory,
                    else => return WALError.IoError,
                };
                self.active_file = vfile;
                self.segment_size = 0;
            }
        } else {
            try self.create_new_segment();
        }
    }

    fn create_new_segment(self: *WAL) WALError!void {
        // Overflow protection prevents infinite segment creation
        assert(self.segment_number < std.math.maxInt(u32));

        self.segment_size = 0;

        const segment_path = try std.fmt.allocPrint(
            self.allocator,
            "{s}/{s}{d:0>4}{s}",
            .{ self.directory, WAL_FILE_PREFIX, self.segment_number, WAL_FILE_SUFFIX },
        );
        defer self.allocator.free(segment_path);
        assert(segment_path.len < MAX_PATH_LENGTH);

        const vfile = self.vfs.create(segment_path) catch |err| switch (err) {
            error.AccessDenied => return WALError.AccessDenied,
            error.OutOfMemory => return WALError.OutOfMemory,
            else => return WALError.IoError,
        };
        self.active_file = vfile;
        self.stats.segments_rotated += 1;

        // Segment creation must leave system in consistent write-ready state
        assert(self.active_file != null);
        assert(self.segment_size == 0);

        log.info("Created new WAL segment: {d}", .{self.segment_number});
    }

    fn rotate_segment(self: *WAL) WALError!void {
        // Rotation requires active segment with data to justify new segment creation
        assert(self.active_file != null);
        assert(self.segment_size > 0);
        assert(self.segment_number < std.math.maxInt(u32));

        if (self.active_file) |*file| {
            file.close();
            file.deinit();
            self.active_file = null;
        }

        const old_segment_number = self.segment_number;
        self.segment_number += 1;
        try self.create_new_segment();

        // Atomic rotation prevents inconsistent state during segment transition
        assert(self.segment_number == old_segment_number + 1);
        assert(self.active_file != null);
        assert(self.segment_size == 0);

        log.info("Rotated WAL to segment {d}", .{self.segment_number});
    }

    fn list_segment_files(self: *WAL) WALError![][]const u8 {
        // Use temporary arena for directory iteration to prevent memory leaks
        var temp_arena = std.heap.ArenaAllocator.init(self.allocator);
        defer temp_arena.deinit();
        const temp_allocator = temp_arena.allocator();

        // Defensive limit to prevent runaway directory iteration
        const MAX_SEGMENTS = 10000;
        var iteration_count: u32 = 0;

        var dir_iterator = self.vfs.iterate_directory(self.directory, temp_allocator) catch |err|
            switch (err) {
                error.FileNotFound => {
                    // Directory doesn't exist, return empty list
                    return try self.allocator.alloc([]const u8, 0);
                },
                else => return WALError.IoError,
            };

        var wal_files = std.ArrayList([]const u8).init(self.allocator);
        defer wal_files.deinit();

        // Strict filename validation prevents processing of unrelated files
        while (dir_iterator.next()) |entry| {
            iteration_count += 1;
            if (iteration_count > MAX_SEGMENTS) {
                log.err("Directory iteration exceeded maximum segments limit: {d}", .{MAX_SEGMENTS});
                return WALError.IoError;
            }

            const file_name = entry.name;
            if (std.mem.startsWith(u8, file_name, WAL_FILE_PREFIX) and
                std.mem.endsWith(u8, file_name, WAL_FILE_SUFFIX))
            {
                const expected_length = WAL_FILE_PREFIX.len + WAL_FILE_NUMBER_DIGITS + WAL_FILE_SUFFIX.len;
                if (file_name.len == expected_length) {
                    try wal_files.append(try self.allocator.dupe(u8, file_name));
                } else {
                    log.warn("Skipping malformed WAL filename: {s}", .{file_name});
                }
            }
        }

        // Lexicographic sort ensures chronological processing order
        const wal_file_slice = try wal_files.toOwnedSlice();
        std.sort.block([]const u8, wal_file_slice, {}, struct {
            fn less_than(context: void, lhs: []const u8, rhs: []const u8) bool {
                _ = context;
                return std.mem.order(u8, lhs, rhs) == .lt;
            }
        }.less_than);

        return wal_file_slice;
    }

    fn recover_from_segment(self: *WAL, file_path: []const u8, callback: RecoveryCallback, context: *anyopaque) WALError!void {
        // Path validation prevents buffer overflows in file operations
        assert(file_path.len > 0);
        assert(file_path.len < MAX_PATH_LENGTH);

        // Defensive limit to prevent runaway entry processing
        const MAX_ENTRIES_PER_SEGMENT = 1_000_000;
        var entries_processed: u32 = 0;

        var file = self.vfs.open(file_path, .read) catch |err| switch (err) {
            error.FileNotFound => return WALError.FileNotFound,
            error.AccessDenied => return WALError.AccessDenied,
            error.OutOfMemory => return WALError.OutOfMemory,
            else => return WALError.IoError,
        };
        defer file.close();

        var entries_recovered: u32 = 0;
        var buffer: [8192]u8 = undefined;
        var remaining: []u8 = &[_]u8{};

        // Buffer must accommodate multiple headers to prevent thrashing
        comptime assert(@sizeOf(@TypeOf(buffer)) >= WALEntry.HEADER_SIZE * 4);

        while (true) {
            const bytes_read = file.read(buffer[remaining.len..]) catch return WALError.IoError;
            if (bytes_read == 0 and remaining.len == 0) break;

            const available = remaining.len + bytes_read;
            assert(available <= buffer.len);
            var pos: usize = 0;

            while (pos + WALEntry.HEADER_SIZE <= available) {
                const payload_size = std.mem.readInt(u32, buffer[pos + 9 ..][0..4], .little);

                // Oversized payloads indicate corruption, not valid large blocks
                if (payload_size > MAX_PAYLOAD_SIZE) {
                    log.warn("Invalid payload size during recovery: {d} > {d}", .{ payload_size, MAX_PAYLOAD_SIZE });
                    pos += 1;
                    continue;
                }

                const entry_size = WALEntry.HEADER_SIZE + payload_size;
                assert(entry_size >= WALEntry.HEADER_SIZE);

                if (pos + entry_size > available) {
                    break;
                }

                const entry_buffer = buffer[pos .. pos + entry_size];
                var entry = WALEntry.deserialize(entry_buffer, self.allocator) catch |err| switch (err) {
                    WALError.InvalidChecksum, WALError.InvalidEntryType => {
                        pos += 1;
                        continue;
                    },
                    else => return err,
                };

                callback(entry, context) catch |err| {
                    entry.deinit(self.allocator);
                    return err;
                };

                entry.deinit(self.allocator);
                entries_recovered += 1;
                entries_processed += 1;

                // Defensive check: prevent runaway processing
                if (entries_processed > MAX_ENTRIES_PER_SEGMENT) {
                    log.err("WAL segment exceeded maximum entries limit: {d}", .{MAX_ENTRIES_PER_SEGMENT});
                    return WALError.IoError;
                }

                // Defensive check: ensure we're making progress
                const old_pos = pos;
                pos += entry_size;
                if (pos <= old_pos) {
                    log.err("WAL recovery not making progress at position {d}", .{pos});
                    return WALError.IoError;
                }

                // Position overflow indicates buffer management logic error
                assert(pos <= available);
            }

            // Preserve partial entry data across read boundaries to handle
            // entries spanning buffer chunks without re-reading from disk
            if (pos < available) {
                const leftover_size = available - pos;
                // Only copy if we actually need to move data (pos > 0)
                if (pos > 0) {
                    stdx.copy_left(u8, buffer[0..leftover_size], buffer[pos..available]);
                }
                remaining = buffer[0..leftover_size];
            } else {
                remaining = &[_]u8{};
            }
        }

        self.stats.entries_recovered += entries_recovered;
        log.info("Recovered {d} entries from segment: {s}", .{ entries_recovered, file_path });
    }
};

test "WAL entry serialization roundtrip" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const allocator = arena.allocator();

    // Create test block
    const test_block = ContextBlock{
        .id = BlockId.from_hex("0123456789abcdeffedcba9876543210") catch unreachable,
        .version = 1,
        .source_uri = "test://example",
        .metadata_json = "{}",
        .content = "test content",
    };

    // Create WAL entry
    var entry = try WALEntry.create_put_block(test_block, allocator);
    defer entry.deinit(allocator);

    // Serialize
    const serialized_size = WALEntry.HEADER_SIZE + entry.payload.len;
    const buffer = try allocator.alloc(u8, serialized_size);
    const bytes_written = try entry.serialize(buffer);
    try testing.expectEqual(serialized_size, bytes_written);

    // Deserialize
    var deserialized = try WALEntry.deserialize(buffer, allocator);
    defer deserialized.deinit(allocator);

    // Verify
    try testing.expectEqual(entry.checksum, deserialized.checksum);
    try testing.expectEqual(entry.entry_type, deserialized.entry_type);
    try testing.expectEqual(entry.payload_size, deserialized.payload_size);
    try testing.expectEqualSlices(u8, entry.payload, deserialized.payload);
}

test "WAL segment management" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const allocator = arena.allocator();

    // Create simulation VFS
    var sim_vfs = @import("sim").SimulationVFS.init(allocator) catch return error.SkipZigTest;
    defer sim_vfs.deinit();
    const vfs_interface = sim_vfs.vfs();

    // Initialize WAL
    var wal = try WAL.init(allocator, vfs_interface, "/test/wal");
    defer wal.deinit();

    // Verify initial state
    try testing.expect(wal.active_file != null);
    try testing.expectEqual(@as(u32, 1), wal.segment_number);
    try testing.expectEqual(@as(u64, 0), wal.segment_size);
}
