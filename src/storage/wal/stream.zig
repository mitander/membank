//! WAL Entry Stream - Buffered streaming reader for WAL entries
//!
//! Provides a clean abstraction for reading WAL entries from a file with proper
//! buffer management, boundary handling, and corruption resilience. Separates
//! I/O concerns from recovery business logic to enable focused testing and
//! maintainability.
//!
//! Design rationale: The original recover_from_segment function mixed buffered
//! I/O logic with recovery business logic, creating a 350+ line function that
//! was difficult to test and debug. This stream abstraction handles only the
//! I/O complexity, allowing recovery logic to focus on business rules.

const builtin = @import("builtin");
const std = @import("std");

const assert_mod = @import("../../core/assert.zig");
const simulation_vfs = @import("../../sim/simulation_vfs.zig");
const stdx = @import("../../core/stdx.zig");
const vfs = @import("../../core/vfs.zig");

const assert = assert_mod.assert;
const log = std.log.scoped(.wal_stream);
const testing = std.testing;

const SimulationVFS = simulation_vfs.SimulationVFS;
const VFile = vfs.VFile;

/// Maximum payload size for a single WAL entry (16MB)
/// Must match the constant in wal.zig for compatibility
const MAX_PAYLOAD_SIZE: u32 = 16 * 1024 * 1024;

/// WAL entry header size - must match wal.zig
const WAL_HEADER_SIZE: usize = 13; // 8 bytes checksum + 1 byte type + 4 bytes size

/// Buffer sizes optimized for typical WAL usage patterns
const READ_BUFFER_SIZE = 8192;
const PROCESS_BUFFER_SIZE = 16384;

/// Stream errors - focused on I/O concerns only
pub const StreamError = error{
    EndOfFile,
    IoError,
    OutOfMemory,
    CorruptedEntry,
    EntryTooLarge,
};

/// Represents a raw WAL entry read from the stream
/// Caller owns the payload memory and must free it
pub const StreamEntry = struct {
    checksum: u64,
    entry_type: u8,
    payload: []u8,
    file_position: u64,

    pub fn deinit(self: StreamEntry, allocator: std.mem.Allocator) void {
        allocator.free(self.payload);
    }
};

/// Streaming reader for WAL entries with buffered I/O and boundary handling
pub const WALEntryStream = struct {
    allocator: std.mem.Allocator,
    file: *VFile,

    // Buffering state - encapsulated to avoid external coupling
    process_buffer: [PROCESS_BUFFER_SIZE]u8,
    remaining_buffer: [PROCESS_BUFFER_SIZE]u8,
    remaining_len: usize,
    buffer_start_file_pos: u64,

    // Position tracking for corruption detection and debugging
    last_file_position: u64,
    entries_read: u32,

    // Defensive limits to prevent infinite loops from corruption
    read_iterations: u32,
    zero_progress_count: u32,

    const MAX_READ_ITERATIONS = 100_000;
    const MAX_ZERO_PROGRESS_ITERATIONS = 10;
    const MAX_ENTRIES_PER_STREAM = 1_000_000;

    pub fn init(allocator: std.mem.Allocator, file: *VFile) StreamError!WALEntryStream {
        _ = file.seek(0, .start) catch return StreamError.IoError;

        return WALEntryStream{
            .allocator = allocator,
            .file = file,
            .process_buffer = std.mem.zeroes([PROCESS_BUFFER_SIZE]u8),
            .remaining_buffer = std.mem.zeroes([PROCESS_BUFFER_SIZE]u8),
            .remaining_len = 0,
            .buffer_start_file_pos = 0,
            .last_file_position = 0,
            .entries_read = 0,
            .read_iterations = 0,
            .zero_progress_count = 0,
        };
    }

    /// Read the next WAL entry from the stream
    /// Returns null when end of file is reached
    /// Caller owns the returned entry and must call deinit()
    pub fn next(self: *WALEntryStream) StreamError!?StreamEntry {
        while (true) {
            // Defensive check: prevent infinite loops from corruption
            self.read_iterations += 1;
            if (self.read_iterations > MAX_READ_ITERATIONS) {
                log.err("WAL stream exceeded maximum read iterations: {d}", .{MAX_READ_ITERATIONS});
                return StreamError.IoError;
            }

            // Defensive check: prevent runaway entry processing
            if (self.entries_read > MAX_ENTRIES_PER_STREAM) {
                log.err("WAL stream exceeded maximum entries limit: {d}", .{MAX_ENTRIES_PER_STREAM});
                return StreamError.IoError;
            }

            const current_position = self.file.tell() catch return StreamError.IoError;

            const position_before_fill = self.file.tell() catch return StreamError.IoError;
            const available = try self.fill_process_buffer();
            const position_after_fill = self.file.tell() catch return StreamError.IoError;

            // Detect EOF: no data available and file position didn't advance
            if (available == 0) {
                return null; // End of file reached
            }

            // Detect EOF with incomplete data: we have some data but couldn't read more
            const reached_eof = (position_before_fill == position_after_fill) and (available > 0) and (available < WAL_HEADER_SIZE);
            if (reached_eof) {
                // Incomplete data at EOF is normal, not corruption - return EndOfFile
                return null;
            }

            if (self.zero_progress_count >= MAX_ZERO_PROGRESS_ITERATIONS) {
                if (available >= WAL_HEADER_SIZE) {
                    const checksum = std.mem.readInt(u64, self.process_buffer[0..8], .little);
                    const entry_type = self.process_buffer[8];
                    const payload_size = std.mem.readInt(u32, self.process_buffer[9..13], .little);

                    if ((checksum <= 0xFF and entry_type == 0 and payload_size == 0) or payload_size > MAX_PAYLOAD_SIZE) {
                        log.debug("WAL stream detected EOF padding after zero progress, terminating", .{});
                        return null;
                    }
                }
            }

            if (current_position != self.last_file_position) {
                self.last_file_position = current_position;
                self.zero_progress_count = 0;
            } else {
                self.zero_progress_count += 1;
            }

            if (try self.try_read_entry(available)) |entry| {
                self.entries_read += 1;
                return entry;
            }

            // No complete entry available, continue reading
        }
    }

    /// Fill the process buffer with data, handling remaining data from previous reads
    fn fill_process_buffer(self: *WALEntryStream) StreamError!usize {
        const position_before_read = self.file.tell() catch return StreamError.IoError;
        if (self.remaining_len == 0) {
            self.buffer_start_file_pos = position_before_read;
        }

        if (self.remaining_len > 0) {
            // copy remaining data to start of process_buffer
            @memcpy(self.process_buffer[0..self.remaining_len], self.remaining_buffer[0..self.remaining_len]);
        }

        const read_target = self.process_buffer[self.remaining_len..];
        const bytes_read = self.file.read(read_target) catch return StreamError.IoError;

        const available = self.remaining_len + bytes_read;
        self.remaining_len = 0; // it's all in process_buffer now

        return available;
    }

    /// Attempt to read a complete entry from the process buffer
    /// Returns null if no complete entry is available
    fn try_read_entry(self: *WALEntryStream, available: usize) StreamError!?StreamEntry {
        if (available < WAL_HEADER_SIZE) {
            // Preserve incomplete header for next read
            self.preserve_remaining_data(0, available);
            return null;
        }

        const checksum = std.mem.readInt(u64, self.process_buffer[0..8], .little);
        const entry_type = self.process_buffer[8];
        const payload_size = std.mem.readInt(u32, self.process_buffer[9..13], .little);

        // Validate entry structure before allocation
        if (payload_size > MAX_PAYLOAD_SIZE) {
            log.warn("WAL stream corruption: payload_size {} exceeds MAX_PAYLOAD_SIZE {} at buffer position {}", .{ payload_size, MAX_PAYLOAD_SIZE, self.buffer_start_file_pos });
            log.warn("Entry header bytes: checksum=0x{X} type={} payload_size={}", .{ checksum, entry_type, payload_size });

            // Advance past corrupted header to prevent infinite loop
            const skip_size = @min(WAL_HEADER_SIZE, available);
            self.preserve_remaining_data(skip_size, available);
            return StreamError.CorruptedEntry;
        }

        if (entry_type == 0 or entry_type > 3) {
            const looks_like_eof_padding = (checksum <= 0xFF) and (entry_type == 0) and (payload_size == 0);

            if (looks_like_eof_padding) {
                log.debug("WAL stream reached EOF padding at buffer position {}", .{self.buffer_start_file_pos});
                self.remaining_len = 0;
                return null;
            }

            log.warn("WAL stream corruption: invalid entry_type {} at buffer position {}", .{ entry_type, self.buffer_start_file_pos });
            log.warn("Entry header bytes: checksum=0x{X} type={} payload_size={}", .{ checksum, entry_type, payload_size });
            log.warn("Raw header bytes: {x:0>2} {x:0>2} {x:0>2} {x:0>2} {x:0>2} {x:0>2} {x:0>2} {x:0>2} {x:0>2} {x:0>2} {x:0>2} {x:0>2} {x:0>2}", .{ self.process_buffer[0], self.process_buffer[1], self.process_buffer[2], self.process_buffer[3], self.process_buffer[4], self.process_buffer[5], self.process_buffer[6], self.process_buffer[7], self.process_buffer[8], self.process_buffer[9], self.process_buffer[10], self.process_buffer[11], self.process_buffer[12] });

            // Advance past corrupted header to prevent infinite loop
            const skip_size = @min(WAL_HEADER_SIZE, available);
            self.preserve_remaining_data(skip_size, available);
            return StreamError.CorruptedEntry;
        }

        const entry_size = WAL_HEADER_SIZE + payload_size;

        if (entry_size > available) {
            if (entry_size > self.process_buffer.len) {
                // Large entry exceeds buffer - use direct file access
                return try self.read_large_entry(entry_size, checksum, entry_type, payload_size);
            } else {
                // Entry spans buffer boundary - preserve for next read
                self.preserve_remaining_data(0, available);
                return null;
            }
        }

        const entry_position = self.buffer_start_file_pos;
        const payload = try self.allocator.dupe(u8, self.process_buffer[WAL_HEADER_SIZE..entry_size]);

        self.preserve_remaining_data(entry_size, available);

        return StreamEntry{
            .checksum = checksum,
            .entry_type = entry_type,
            .payload = payload,
            .file_position = entry_position,
        };
    }

    /// Handle entries larger than the process buffer using direct file access
    fn read_large_entry(
        self: *WALEntryStream,
        entry_size: usize,
        checksum: u64,
        entry_type: u8,
        payload_size: u32,
    ) StreamError!StreamEntry {
        _ = payload_size; // Used for validation in caller
        const entry_position = self.buffer_start_file_pos;

        // Seek to start of entry for complete read
        _ = self.file.seek(entry_position, .start) catch return StreamError.IoError;

        const entry_buffer = self.allocator.alloc(u8, entry_size) catch return StreamError.OutOfMemory;
        defer self.allocator.free(entry_buffer);

        const bytes_read = self.file.read(entry_buffer) catch return StreamError.IoError;
        if (bytes_read != entry_size) {
            log.err("Failed to read complete large entry: expected {d}, got {d}", .{ entry_size, bytes_read });
            return StreamError.IoError;
        }

        const payload = try self.allocator.dupe(u8, entry_buffer[WAL_HEADER_SIZE..]);

        _ = self.file.seek(entry_position + entry_size, .start) catch return StreamError.IoError;

        self.remaining_len = 0;
        self.buffer_start_file_pos = entry_position + entry_size;

        return StreamEntry{
            .checksum = checksum,
            .entry_type = entry_type,
            .payload = payload,
            .file_position = entry_position,
        };
    }

    /// Preserve unprocessed data for the next buffer fill
    fn preserve_remaining_data(self: *WALEntryStream, consumed: usize, available: usize) void {
        assert(consumed <= available);

        if (consumed < available) {
            const leftover_size = available - consumed;
            assert(leftover_size <= self.remaining_buffer.len);

            @memcpy(self.remaining_buffer[0..leftover_size], self.process_buffer[consumed..available]);
            self.remaining_len = leftover_size;
            self.buffer_start_file_pos += consumed;
        } else {
            self.remaining_len = 0;
            self.buffer_start_file_pos += consumed;
        }
    }

    /// Get current position in the file for debugging and recovery context
    pub fn file_position(self: *WALEntryStream) u64 {
        return self.file.tell() catch 0;
    }

    /// Get statistics about the streaming operation
    pub fn stats(self: *WALEntryStream) struct { entries_read: u32, read_iterations: u32 } {
        return .{
            .entries_read = self.entries_read,
            .read_iterations = self.read_iterations,
        };
    }
};

// Compile-time validation of buffer relationships
comptime {
    // Process buffer must accommodate multiple headers to prevent thrashing
    assert_mod.comptime_assert(PROCESS_BUFFER_SIZE >= WAL_HEADER_SIZE * 4, "Process buffer too small for multiple headers");
    assert_mod.comptime_assert(READ_BUFFER_SIZE >= WAL_HEADER_SIZE * 4, "Read buffer too small for multiple headers");
    assert_mod.comptime_assert(PROCESS_BUFFER_SIZE >= READ_BUFFER_SIZE, "Process buffer must be at least as large as read buffer");
}

test "WALEntryStream initialization" {
    const allocator = testing.allocator;

    var vfs_sim = try SimulationVFS.init(allocator);
    defer vfs_sim.deinit();

    // Create empty test file
    const test_path = "test_stream.wal";
    var file = try vfs_sim.vfs().create(test_path);
    defer file.close();

    var stream = try WALEntryStream.init(allocator, &file);

    try testing.expectEqual(@as(u32, 0), stream.entries_read);
    try testing.expectEqual(@as(u32, 0), stream.read_iterations);
    try testing.expectEqual(@as(usize, 0), stream.remaining_len);
    try testing.expectEqual(@as(u64, 0), stream.last_file_position);

    const stats_result = stream.stats();
    try testing.expectEqual(@as(u32, 0), stats_result.entries_read);
}

test "WALEntryStream read from empty file" {
    const allocator = testing.allocator;

    var vfs_sim = try SimulationVFS.init(allocator);
    defer vfs_sim.deinit();

    // Create empty test file
    const test_path = "empty_stream.wal";
    var file = try vfs_sim.vfs().create(test_path);
    defer file.close();

    var stream = try WALEntryStream.init(allocator, &file);

    // Reading from empty file should return null (EOF)
    const entry = try stream.next();
    try testing.expect(entry == null);
}

test "WALEntryStream read single complete entry" {
    const allocator = testing.allocator;

    var vfs_sim = try SimulationVFS.init(allocator);
    defer vfs_sim.deinit();

    const test_path = "single_entry.wal";
    var file = try vfs_sim.vfs().create(test_path);

    const test_payload = "Hello, WAL stream!";
    const checksum: u64 = 0x1234567890abcdef;
    const entry_type: u8 = 0x01; // put_block
    const payload_size: u32 = @intCast(test_payload.len);

    var header_buffer: [WAL_HEADER_SIZE]u8 = undefined;
    std.mem.writeInt(u64, header_buffer[0..8], checksum, .little);
    header_buffer[8] = entry_type;
    std.mem.writeInt(u32, header_buffer[9..13], payload_size, .little);

    _ = try file.write(&header_buffer);
    _ = try file.write(test_payload);
    _ = try file.seek(0, .start);

    var stream = try WALEntryStream.init(allocator, &file);
    defer file.close();

    const entry = try stream.next();
    try testing.expect(entry != null);

    if (entry) |e| {
        defer e.deinit(allocator);

        try testing.expectEqual(checksum, e.checksum);
        try testing.expectEqual(entry_type, e.entry_type);
        try testing.expect(std.mem.eql(u8, test_payload, e.payload));
        try testing.expectEqual(@as(u64, 0), e.file_position);
    }

    // Second read should return EOF
    const next_entry = try stream.next();
    try testing.expect(next_entry == null);
}

test "WALEntryStream read multiple entries" {
    const allocator = testing.allocator;

    var vfs_sim = try SimulationVFS.init(allocator);
    defer vfs_sim.deinit();

    const test_path = "multiple_entries.wal";
    var file = try vfs_sim.vfs().create(test_path);

    // Write multiple entries
    const entries_data = [_]struct { payload: []const u8, checksum: u64, entry_type: u8 }{
        .{ .payload = "first entry", .checksum = 0x1111111111111111, .entry_type = 0x01 },
        .{ .payload = "second entry", .checksum = 0x2222222222222222, .entry_type = 0x02 },
        .{ .payload = "third entry", .checksum = 0x3333333333333333, .entry_type = 0x03 },
    };

    for (entries_data) |entry_data| {
        var header_buffer: [WAL_HEADER_SIZE]u8 = undefined;
        std.mem.writeInt(u64, header_buffer[0..8], entry_data.checksum, .little);
        header_buffer[8] = entry_data.entry_type;
        std.mem.writeInt(u32, header_buffer[9..13], @intCast(entry_data.payload.len), .little);

        _ = try file.write(&header_buffer);
        _ = try file.write(entry_data.payload);
    }

    _ = try file.seek(0, .start);

    var stream = try WALEntryStream.init(allocator, &file);
    defer file.close();

    // Read all entries
    for (entries_data, 0..) |expected, i| {
        const entry = try stream.next();
        try testing.expect(entry != null);

        if (entry) |e| {
            defer e.deinit(allocator);

            try testing.expectEqual(expected.checksum, e.checksum);
            try testing.expectEqual(expected.entry_type, e.entry_type);
            try testing.expect(std.mem.eql(u8, expected.payload, e.payload));
        } else {
            try testing.expect(false); // Should not happen
        }

        _ = i; // Silence unused variable warning
    }

    // Next read should be EOF
    const final_entry = try stream.next();
    try testing.expect(final_entry == null);

    // Verify statistics
    const stats_result = stream.stats();
    try testing.expectEqual(@as(u32, 3), stats_result.entries_read);
}

test "WALEntryStream corrupted entry handling" {
    const allocator = testing.allocator;

    var vfs_sim = try SimulationVFS.init(allocator);
    defer vfs_sim.deinit();

    const test_path = "corrupted_entry.wal";
    var file = try vfs_sim.vfs().create(test_path);

    // Write corrupted entry with invalid type
    var header_buffer: [WAL_HEADER_SIZE]u8 = undefined;
    std.mem.writeInt(u64, header_buffer[0..8], 0x1234567890abcdef, .little);
    header_buffer[8] = 0xFF; // Invalid entry type
    std.mem.writeInt(u32, header_buffer[9..13], 10, .little);

    _ = try file.write(&header_buffer);
    _ = try file.write("corrupted!");
    _ = try file.seek(0, .start);

    var stream = try WALEntryStream.init(allocator, &file);
    defer file.close();

    // Reading corrupted entry should return error
    try testing.expectError(StreamError.CorruptedEntry, stream.next());
}

test "WALEntryStream oversized payload" {
    const allocator = testing.allocator;

    var vfs_sim = try SimulationVFS.init(allocator);
    defer vfs_sim.deinit();

    const test_path = "oversized_payload.wal";
    var file = try vfs_sim.vfs().create(test_path);

    // Write entry with payload size exceeding maximum
    var header_buffer: [WAL_HEADER_SIZE]u8 = undefined;
    std.mem.writeInt(u64, header_buffer[0..8], 0x1234567890abcdef, .little);
    header_buffer[8] = 0x01; // put_block
    std.mem.writeInt(u32, header_buffer[9..13], MAX_PAYLOAD_SIZE + 1, .little); // Too large

    _ = try file.write(&header_buffer);
    _ = try file.seek(0, .start);

    var stream = try WALEntryStream.init(allocator, &file);
    defer file.close();

    // Reading oversized entry should return error
    try testing.expectError(StreamError.CorruptedEntry, stream.next());
}

test "WALEntryStream incomplete entry at EOF" {
    const allocator = testing.allocator;

    var vfs_sim = try SimulationVFS.init(allocator);
    defer vfs_sim.deinit();

    const test_path = "incomplete_entry.wal";
    var file = try vfs_sim.vfs().create(test_path);

    // Write partial header (only 8 bytes instead of 13)
    const partial_header = [_]u8{ 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08 };
    _ = try file.write(&partial_header);
    _ = try file.seek(0, .start);

    var stream = try WALEntryStream.init(allocator, &file);
    defer file.close();

    // Reading incomplete entry should return null (EOF)
    const entry = try stream.next();
    try testing.expect(entry == null);
}

test "WALEntryStream large entry handling" {
    const allocator = testing.allocator;

    var vfs_sim = try SimulationVFS.init(allocator);
    defer vfs_sim.deinit();

    const test_path = "large_entry.wal";
    var file = try vfs_sim.vfs().create(test_path);

    const large_payload_size = PROCESS_BUFFER_SIZE + 1000;
    const large_payload = try allocator.alloc(u8, large_payload_size);
    defer allocator.free(large_payload);
    @memset(large_payload, 0xAA);

    var header_buffer: [WAL_HEADER_SIZE]u8 = undefined;
    const checksum: u64 = 0x1234567890abcdef;
    std.mem.writeInt(u64, header_buffer[0..8], checksum, .little);
    header_buffer[8] = 0x01; // put_block
    std.mem.writeInt(u32, header_buffer[9..13], @intCast(large_payload_size), .little);

    _ = try file.write(&header_buffer);
    _ = try file.write(large_payload);
    _ = try file.seek(0, .start);

    var stream = try WALEntryStream.init(allocator, &file);
    defer file.close();

    const entry = try stream.next();
    try testing.expect(entry != null);

    if (entry) |e| {
        defer e.deinit(allocator);

        try testing.expectEqual(checksum, e.checksum);
        try testing.expectEqual(@as(u8, 0x01), e.entry_type);
        try testing.expectEqual(large_payload_size, e.payload.len);
        try testing.expect(std.mem.eql(u8, large_payload, e.payload));
    }
}

test "WALEntryStream position tracking" {
    const allocator = testing.allocator;

    var vfs_sim = try SimulationVFS.init(allocator);
    defer vfs_sim.deinit();

    const test_path = "position_tracking.wal";
    var file = try vfs_sim.vfs().create(test_path);

    // Write two entries to track position changes
    const entries_data = [_]struct { payload: []const u8, checksum: u64 }{
        .{ .payload = "first", .checksum = 0x1111111111111111 },
        .{ .payload = "second", .checksum = 0x2222222222222222 },
    };

    var expected_positions: [2]u64 = undefined;
    var current_pos: u64 = 0;

    for (entries_data, 0..) |entry_data, i| {
        expected_positions[i] = current_pos;

        var header_buffer: [WAL_HEADER_SIZE]u8 = undefined;
        std.mem.writeInt(u64, header_buffer[0..8], entry_data.checksum, .little);
        header_buffer[8] = 0x01; // put_block
        std.mem.writeInt(u32, header_buffer[9..13], @intCast(entry_data.payload.len), .little);

        _ = try file.write(&header_buffer);
        _ = try file.write(entry_data.payload);

        current_pos += WAL_HEADER_SIZE + entry_data.payload.len;
    }

    _ = try file.seek(0, .start);

    var stream = try WALEntryStream.init(allocator, &file);
    defer file.close();

    // Read entries and verify positions
    for (expected_positions) |expected_pos| {
        const entry = try stream.next();
        try testing.expect(entry != null);

        if (entry) |e| {
            defer e.deinit(allocator);
            try testing.expectEqual(expected_pos, e.file_position);
        }
    }
}

test "WALEntryStream buffer boundary handling" {
    const allocator = testing.allocator;

    var vfs_sim = try SimulationVFS.init(allocator);
    defer vfs_sim.deinit();

    const test_path = "buffer_boundary.wal";
    var file = try vfs_sim.vfs().create(test_path);

    // Create entry that will span buffer boundaries
    const payload_size = READ_BUFFER_SIZE - WAL_HEADER_SIZE + 100; // Span boundary
    const large_payload = try allocator.alloc(u8, payload_size);
    defer allocator.free(large_payload);
    @memset(large_payload, 0xBB);

    // Write entry
    var header_buffer: [WAL_HEADER_SIZE]u8 = undefined;
    const checksum: u64 = 0x9999999999999999;
    std.mem.writeInt(u64, header_buffer[0..8], checksum, .little);
    header_buffer[8] = 0x01; // put_block
    std.mem.writeInt(u32, header_buffer[9..13], @intCast(payload_size), .little);

    _ = try file.write(&header_buffer);
    _ = try file.write(large_payload);
    _ = try file.seek(0, .start);

    var stream = try WALEntryStream.init(allocator, &file);
    defer file.close();

    // Read entry that spans buffer boundary
    const entry = try stream.next();
    try testing.expect(entry != null);

    if (entry) |e| {
        defer e.deinit(allocator);

        try testing.expectEqual(checksum, e.checksum);
        try testing.expectEqual(@as(u8, 0x01), e.entry_type);
        try testing.expectEqual(payload_size, e.payload.len);
        try testing.expect(std.mem.eql(u8, large_payload, e.payload));
    }
}

test "StreamEntry memory management" {
    const allocator = testing.allocator;

    const test_payload = try allocator.dupe(u8, "test payload for memory management");

    const entry = StreamEntry{
        .checksum = 0x1234567890abcdef,
        .entry_type = 0x01,
        .payload = test_payload,
        .file_position = 0,
    };

    // deinit should properly free the payload
    entry.deinit(allocator);
    // If this doesn't crash, memory was freed correctly
}

test "WALEntryStream constants validation" {
    // Verify buffer size relationships
    try testing.expect(PROCESS_BUFFER_SIZE >= READ_BUFFER_SIZE);
    try testing.expect(PROCESS_BUFFER_SIZE >= WAL_HEADER_SIZE * 4);
    try testing.expect(READ_BUFFER_SIZE >= WAL_HEADER_SIZE * 4);

    // Verify size constants
    try testing.expectEqual(@as(u32, 16 * 1024 * 1024), MAX_PAYLOAD_SIZE);
    try testing.expectEqual(@as(usize, 13), WAL_HEADER_SIZE);
    try testing.expectEqual(@as(usize, 8192), READ_BUFFER_SIZE);
    try testing.expectEqual(@as(usize, 16384), PROCESS_BUFFER_SIZE);
}
