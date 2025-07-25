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
