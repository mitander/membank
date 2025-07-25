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

const std = @import("std");
const builtin = @import("builtin");
const custom_assert = @import("../../core/assert.zig");
const assert = custom_assert.assert;
const testing = std.testing;
const log = std.log.scoped(.wal_stream);

const vfs = @import("../../core/vfs.zig");
const stdx = @import("../../core/stdx.zig");

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
    read_buffer: [READ_BUFFER_SIZE]u8,
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
        // Reset file position to start for consistent streaming behavior
        _ = file.seek(0, .start) catch return StreamError.IoError;

        return WALEntryStream{
            .allocator = allocator,
            .file = file,
            .read_buffer = std.mem.zeroes([READ_BUFFER_SIZE]u8),
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

            // Defensive check: prevent infinite loops from corruption (very high threshold)
            if (self.read_iterations > MAX_READ_ITERATIONS) {
                log.err("WAL stream exceeded maximum read iterations: {d}", .{MAX_READ_ITERATIONS});
                return StreamError.IoError;
            }

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

            // Update position tracking for debugging
            if (current_position != self.last_file_position) {
                self.last_file_position = current_position;
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

        // Update buffer position tracking on fresh reads
        if (self.remaining_len == 0) {
            self.buffer_start_file_pos = position_before_read;
        }

        const bytes_read = self.file.read(&self.read_buffer) catch return StreamError.IoError;

        // Clear process buffer for clean state
        @memset(&self.process_buffer, 0);

        const available = self.remaining_len + bytes_read;
        const copy_size = @min(available, self.process_buffer.len);

        // Copy remaining data first, then append new data up to buffer limit
        if (self.remaining_len > 0) {
            @memcpy(self.process_buffer[0..self.remaining_len], self.remaining_buffer[0..self.remaining_len]);
        }

        const new_bytes_to_copy = copy_size - self.remaining_len;
        if (new_bytes_to_copy > 0) {
            @memcpy(self.process_buffer[self.remaining_len..copy_size], self.read_buffer[0..new_bytes_to_copy]);
        }

        // Return the actual data available, let caller handle EOF detection
        return copy_size;
    }

    /// Attempt to read a complete entry from the process buffer
    /// Returns null if no complete entry is available
    fn try_read_entry(self: *WALEntryStream, available: usize) StreamError!?StreamEntry {
        if (available < WAL_HEADER_SIZE) {
            // Preserve incomplete header for next read
            self.preserve_remaining_data(0, available);
            return null;
        }

        // Parse entry header for size calculation
        const checksum = std.mem.readInt(u64, self.process_buffer[0..8], .little);
        const entry_type = self.process_buffer[8];
        const payload_size = std.mem.readInt(u32, self.process_buffer[9..13], .little);

        // Validate entry structure before allocation
        if (payload_size > MAX_PAYLOAD_SIZE) {
            return StreamError.CorruptedEntry;
        }

        if (entry_type == 0 or entry_type > 3) {
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

        // Complete entry available in buffer
        const entry_position = self.buffer_start_file_pos;
        const payload = try self.allocator.dupe(u8, self.process_buffer[WAL_HEADER_SIZE..entry_size]);

        // Advance buffer state past this entry
        self.preserve_remaining_data(entry_size, available);

        return StreamEntry{
            .checksum = checksum,
            .entry_type = entry_type,
            .payload = payload,
            .file_position = entry_position,
        };
    }

    /// Handle entries larger than the process buffer using direct file access
    fn read_large_entry(self: *WALEntryStream, entry_size: usize, checksum: u64, entry_type: u8, payload_size: u32) StreamError!StreamEntry {
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

        // Extract payload from complete entry buffer
        const payload = try self.allocator.dupe(u8, entry_buffer[WAL_HEADER_SIZE..]);

        // Position file after this entry for continued streaming
        _ = self.file.seek(entry_position + entry_size, .start) catch return StreamError.IoError;

        // Clear buffer state since we bypassed buffering
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
    custom_assert.comptime_assert(PROCESS_BUFFER_SIZE >= WAL_HEADER_SIZE * 4, "Process buffer too small for multiple headers");
    custom_assert.comptime_assert(READ_BUFFER_SIZE >= WAL_HEADER_SIZE * 4, "Read buffer too small for multiple headers");
    custom_assert.comptime_assert(PROCESS_BUFFER_SIZE >= READ_BUFFER_SIZE, "Process buffer must be at least as large as read buffer");
}
