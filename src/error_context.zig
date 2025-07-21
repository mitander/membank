//! Structured error handling with debugging context for CortexDB.
//!
//! Provides rich debugging information in debug builds while maintaining
//! zero runtime overhead in release builds.

const std = @import("std");
const builtin = @import("builtin");
const context_block = @import("context_block");
const log = std.log.scoped(.error_context);

const BlockId = context_block.BlockId;

/// Context information for storage operations that can fail.
pub const StorageContext = struct {
    operation: []const u8,
    file_path: ?[]const u8 = null,
    block_id: ?BlockId = null,
    offset: ?u64 = null,
    size: ?usize = null,
    expected_value: ?u32 = null,
    actual_value: ?u32 = null,
    entry_type: ?u8 = null,

    pub fn format(
        self: StorageContext,
        comptime fmt: []const u8,
        options: std.fmt.FormatOptions,
        writer: anytype,
    ) !void {
        _ = fmt;
        _ = options;

        try writer.print("StorageContext{{ operation=\"{s}\"", .{self.operation});

        if (self.file_path) |path| {
            try writer.print(", file=\"{s}\"", .{path});
        }
        if (self.block_id) |id| {
            try writer.print(", block_id=\"", .{});
            for (id.bytes) |byte| {
                try writer.print("{x:0>2}", .{byte});
            }
            try writer.print("\"", .{});
        }
        if (self.offset) |off| {
            try writer.print(", offset={}", .{off});
        }
        if (self.size) |sz| {
            try writer.print(", size={}", .{sz});
        }
        if (self.expected_value) |exp| {
            try writer.print(", expected=0x{X}", .{exp});
        }
        if (self.actual_value) |act| {
            try writer.print(", actual=0x{X}", .{act});
        }
        if (self.entry_type) |etype| {
            try writer.print(", entry_type={}", .{etype});
        }

        try writer.print(" }}", .{});
    }
};

/// Context information for WAL operations.
pub const WALContext = struct {
    operation: []const u8,
    file_path: ?[]const u8 = null,
    entry_offset: ?u64 = null,
    entry_size: ?usize = null,
    entry_type: ?u8 = null,
    checksum_expected: ?u64 = null,
    checksum_actual: ?u64 = null,

    pub fn format(
        self: WALContext,
        comptime fmt: []const u8,
        options: std.fmt.FormatOptions,
        writer: anytype,
    ) !void {
        _ = fmt;
        _ = options;

        try writer.print("WALContext{{ operation=\"{s}\"", .{self.operation});

        if (self.file_path) |path| {
            try writer.print(", file=\"{s}\"", .{path});
        }
        if (self.entry_offset) |off| {
            try writer.print(", entry_offset={}", .{off});
        }
        if (self.entry_size) |sz| {
            try writer.print(", entry_size={}", .{sz});
        }
        if (self.entry_type) |etype| {
            try writer.print(", entry_type={}", .{etype});
        }
        if (self.checksum_expected) |exp| {
            try writer.print(", checksum_expected=0x{X}", .{exp});
        }
        if (self.checksum_actual) |act| {
            try writer.print(", checksum_actual=0x{X}", .{act});
        }

        try writer.print(" }}", .{});
    }
};

/// Context information for buffer operations.
pub const BufferContext = struct {
    operation: []const u8,
    required_size: ?usize = null,
    available_size: ?usize = null,
    buffer_type: ?[]const u8 = null,

    pub fn format(
        self: BufferContext,
        comptime fmt: []const u8,
        options: std.fmt.FormatOptions,
        writer: anytype,
    ) !void {
        _ = fmt;
        _ = options;

        try writer.print("BufferContext{{ operation=\"{s}\"", .{self.operation});

        if (self.required_size) |req| {
            try writer.print(", required_size={}", .{req});
        }
        if (self.available_size) |avail| {
            try writer.print(", available_size={}", .{avail});
        }
        if (self.buffer_type) |btype| {
            try writer.print(", buffer_type=\"{s}\"", .{btype});
        }

        try writer.print(" }}", .{});
    }
};

/// Log an error with context in debug builds only.
/// Returns the original error for easy chaining.
pub fn storage_error(err: anyerror, context: StorageContext) anyerror {
    if (builtin.mode == .Debug) {
        log.err("Storage operation failed: {any} - {any}", .{ err, context });
    }
    return err;
}

/// Log a WAL error with context in debug builds only.
pub fn wal_error(err: anyerror, context: WALContext) anyerror {
    if (builtin.mode == .Debug) {
        log.err("WAL operation failed: {any} - {any}", .{ err, context });
    }
    return err;
}

/// Log a buffer error with context in debug builds only.
pub fn buffer_error(err: anyerror, context: BufferContext) anyerror {
    if (builtin.mode == .Debug) {
        log.err("Buffer operation failed: {any} - {any}", .{ err, context });
    }
    return err;
}

/// Helper to create storage context for block operations.
pub fn block_context(operation: []const u8, block_id: BlockId) StorageContext {
    return StorageContext{
        .operation = operation,
        .block_id = block_id,
    };
}

/// Helper to create storage context for file operations.
pub fn file_context(operation: []const u8, file_path: []const u8) StorageContext {
    return StorageContext{
        .operation = operation,
        .file_path = file_path,
    };
}

/// Helper to create storage context for checksum validation.
pub fn checksum_context(
    operation: []const u8,
    file_path: []const u8,
    offset: u64,
    expected: u32,
    actual: u32,
) StorageContext {
    return StorageContext{
        .operation = operation,
        .file_path = file_path,
        .offset = offset,
        .expected_value = expected,
        .actual_value = actual,
    };
}

/// Helper to create WAL context for entry operations.
pub fn wal_entry_context(
    operation: []const u8,
    file_path: []const u8,
    entry_offset: u64,
    entry_type: u8,
) WALContext {
    return WALContext{
        .operation = operation,
        .file_path = file_path,
        .entry_offset = entry_offset,
        .entry_type = entry_type,
    };
}

/// Helper to create buffer context for size mismatches.
pub fn buffer_size_context(
    operation: []const u8,
    required: usize,
    available: usize,
) BufferContext {
    return BufferContext{
        .operation = operation,
        .required_size = required,
        .available_size = available,
    };
}

// Tests

test "StorageContext formatting" {
    const test_id = try BlockId.from_hex("0123456789abcdeffedcba9876543210");

    const ctx = StorageContext{
        .operation = "block_deserialization",
        .file_path = "test.sst",
        .block_id = test_id,
        .offset = 1024,
        .size = 256,
        .expected_value = 0xDEADBEEF,
        .actual_value = 0xCAFEBABE,
    };

    var buf: [512]u8 = undefined;
    const formatted = try std.fmt.bufPrint(&buf, "{any}", .{ctx});

    // Should contain all the context fields
    try std.testing.expect(std.mem.indexOf(u8, formatted, "block_deserialization") != null);
    try std.testing.expect(std.mem.indexOf(u8, formatted, "test.sst") != null);
    try std.testing.expect(std.mem.indexOf(u8, formatted, "offset=1024") != null);
}

test "error context helpers in debug mode" {
    const test_id = try BlockId.from_hex("1111111111111111111111111111111");

    // Test that helpers create proper context
    const block_ctx = block_context("test_operation", test_id);
    try std.testing.expectEqualStrings("test_operation", block_ctx.operation);
    try std.testing.expect(block_ctx.block_id != null);
    try std.testing.expect(block_ctx.block_id.?.eql(test_id));

    const file_ctx = file_context("file_read", "test.wal");
    try std.testing.expectEqualStrings("file_read", file_ctx.operation);
    try std.testing.expectEqualStrings("test.wal", file_ctx.file_path.?);

    const checksum_ctx = checksum_context(
        "validate_block",
        "data.sst",
        512,
        0x12345678,
        0x87654321,
    );
    try std.testing.expectEqualStrings("validate_block", checksum_ctx.operation);
    try std.testing.expectEqual(@as(u64, 512), checksum_ctx.offset.?);
    try std.testing.expectEqual(@as(u32, 0x12345678), checksum_ctx.expected_value.?);
    try std.testing.expectEqual(@as(u32, 0x87654321), checksum_ctx.actual_value.?);
}

test "error logging functions return original error" {
    const original_error = error.TestError;

    const storage_ctx = StorageContext{ .operation = "test" };
    const returned_error = storage_error(original_error, storage_ctx);
    try std.testing.expectEqual(original_error, returned_error);

    const wal_ctx = WALContext{ .operation = "test" };
    const wal_returned = wal_error(original_error, wal_ctx);
    try std.testing.expectEqual(original_error, wal_returned);

    const buffer_ctx = BufferContext{ .operation = "test" };
    const buffer_returned = buffer_error(original_error, buffer_ctx);
    try std.testing.expectEqual(original_error, buffer_returned);
}
