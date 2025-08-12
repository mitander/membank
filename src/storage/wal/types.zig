//! WAL Constants and Type Definitions
//!
//! Core constants, error types, and fundamental structures used throughout
//! the WAL implementation. This module provides the foundational types
//! without implementation details, enabling clean module boundaries.

const std = @import("std");
const entry_mod = @import("entry.zig");

/// Maximum size of a WAL segment before rotation (64MB).
/// Power of two for efficient alignment and bitwise operations.
pub const MAX_SEGMENT_SIZE: u64 = 64 * 1024 * 1024;

/// Maximum payload size for a single WAL entry (16MB).
/// Prevents memory exhaustion attacks and ensures reasonable entry sizes.
pub const MAX_PAYLOAD_SIZE: u32 = 16 * 1024 * 1024;

/// WAL file naming constants for consistency and validation
pub const WAL_FILE_PREFIX = "wal_";
pub const WAL_FILE_SUFFIX = ".log";
pub const WAL_FILE_NUMBER_DIGITS = 4; // Supports 0000-9999 segments

/// Maximum file path length for defensive checks
pub const MAX_PATH_LENGTH = 4096;

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
    SerializationSizeMismatch,
    SegmentFull,
    FileNotFound,
    AccessDenied,
    OutOfMemory,
    IoError,
    CallbackFailed,
    InvalidArgument,
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

/// Recovery callback enables pluggable WAL replay strategies
pub const RecoveryCallback = *const fn (entry: entry_mod.WALEntry, context: *anyopaque) WALError!void;

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

const testing = @import("std").testing;

test "WALEntryType from_u8 valid values" {
    try testing.expectEqual(WALEntryType.put_block, try WALEntryType.from_u8(0x01));
    try testing.expectEqual(WALEntryType.delete_block, try WALEntryType.from_u8(0x02));
    try testing.expectEqual(WALEntryType.put_edge, try WALEntryType.from_u8(0x03));
}

test "WALEntryType from_u8 invalid values" {
    try testing.expectError(WALError.InvalidEntryType, WALEntryType.from_u8(0x00));
    try testing.expectError(WALError.InvalidEntryType, WALEntryType.from_u8(0x04));
    try testing.expectError(WALError.InvalidEntryType, WALEntryType.from_u8(0xFF));
    try testing.expectError(WALError.InvalidEntryType, WALEntryType.from_u8(255));
}

test "WALStats initialization" {
    const stats = WALStats.init();

    try testing.expectEqual(@as(u64, 0), stats.entries_written);
    try testing.expectEqual(@as(u64, 0), stats.entries_recovered);
    try testing.expectEqual(@as(u32, 0), stats.segments_rotated);
    try testing.expectEqual(@as(u32, 0), stats.recovery_failures);
    try testing.expectEqual(@as(u64, 0), stats.bytes_written);
}

test "WALStats field manipulation" {
    var stats = WALStats.init();

    stats.entries_written = 100;
    stats.entries_recovered = 95;
    stats.segments_rotated = 3;
    stats.recovery_failures = 2;
    stats.bytes_written = 1024;

    try testing.expectEqual(@as(u64, 100), stats.entries_written);
    try testing.expectEqual(@as(u64, 95), stats.entries_recovered);
    try testing.expectEqual(@as(u32, 3), stats.segments_rotated);
    try testing.expectEqual(@as(u32, 2), stats.recovery_failures);
    try testing.expectEqual(@as(u64, 1024), stats.bytes_written);
}

test "WAL constants validation" {
    try testing.expectEqual(@as(u64, 64 * 1024 * 1024), MAX_SEGMENT_SIZE);
    try testing.expectEqual(@as(u32, 16 * 1024 * 1024), MAX_PAYLOAD_SIZE);
    try testing.expectEqual(@as(usize, 4096), MAX_PATH_LENGTH);

    try testing.expect(std.mem.eql(u8, WAL_FILE_PREFIX, "wal_"));
    try testing.expect(std.mem.eql(u8, WAL_FILE_SUFFIX, ".log"));
    try testing.expectEqual(@as(usize, 4), WAL_FILE_NUMBER_DIGITS);

    try testing.expect(MAX_PAYLOAD_SIZE <= MAX_SEGMENT_SIZE);
    try testing.expect(MAX_SEGMENT_SIZE & (MAX_SEGMENT_SIZE - 1) == 0); // Power of 2
}

test "WALEntryType enum values" {
    try testing.expectEqual(@as(u8, 0x01), @intFromEnum(WALEntryType.put_block));
    try testing.expectEqual(@as(u8, 0x02), @intFromEnum(WALEntryType.delete_block));
    try testing.expectEqual(@as(u8, 0x03), @intFromEnum(WALEntryType.put_edge));
}

test "WALError enum contains expected errors" {
    // Verify critical error types exist (compilation test)
    const test_errors = [_]WALError{
        WALError.NotInitialized,
        WALError.InvalidChecksum,
        WALError.InvalidEntryType,
        WALError.BufferTooSmall,
        WALError.CorruptedEntry,
        WALError.SerializationSizeMismatch,
        WALError.SegmentFull,
        WALError.FileNotFound,
        WALError.AccessDenied,
        WALError.OutOfMemory,
        WALError.IoError,
        WALError.CallbackFailed,
    };

    for (test_errors) |_| {
        // Just verify error type exists
    }
}

test "file naming constants are non-empty" {
    try testing.expect(WAL_FILE_PREFIX.len > 0);
    try testing.expect(WAL_FILE_SUFFIX.len > 0);
    try testing.expect(WAL_FILE_NUMBER_DIGITS > 0);
    try testing.expect(WAL_FILE_NUMBER_DIGITS <= 8);
}
