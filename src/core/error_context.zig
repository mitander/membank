//! Structured error handling with debugging context for Membank.
//!
//! Provides rich debugging information in debug builds while maintaining
//! zero runtime overhead in release builds.

const std = @import("std");
const builtin = @import("builtin");
const context_block = @import("types.zig");
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

/// Context information for ingestion operations that can fail.
pub const IngestionContext = struct {
    operation: []const u8,
    repository_path: ?[]const u8 = null,
    file_path: ?[]const u8 = null,
    content_type: ?[]const u8 = null,
    line_number: ?u32 = null,
    column_number: ?u32 = null,
    unit_type: ?[]const u8 = null,
    file_size: ?u64 = null,
    max_allowed_size: ?u64 = null,
    parsing_stage: ?[]const u8 = null,
    unit_count: ?usize = null,

    pub fn format(
        self: IngestionContext,
        comptime fmt: []const u8,
        options: std.fmt.FormatOptions,
        writer: anytype,
    ) !void {
        _ = fmt;
        _ = options;

        try writer.print("IngestionContext{{ operation=\"{s}\"", .{self.operation});

        if (self.repository_path) |path| {
            try writer.print(", repository=\"{s}\"", .{path});
        }
        if (self.file_path) |path| {
            try writer.print(", file=\"{s}\"", .{path});
        }
        if (self.content_type) |ctype| {
            try writer.print(", content_type=\"{s}\"", .{ctype});
        }
        if (self.line_number) |line| {
            try writer.print(", line={}", .{line});
        }
        if (self.column_number) |col| {
            try writer.print(", column={}", .{col});
        }
        if (self.unit_type) |utype| {
            try writer.print(", unit_type=\"{s}\"", .{utype});
        }
        if (self.file_size) |size| {
            try writer.print(", file_size={}", .{size});
        }
        if (self.max_allowed_size) |max_size| {
            try writer.print(", max_size={}", .{max_size});
        }
        if (self.parsing_stage) |stage| {
            try writer.print(", stage=\"{s}\"", .{stage});
        }
        if (self.unit_count) |count| {
            try writer.print(", unit_count={}", .{count});
        }

        try writer.print(" }}", .{});
    }
};

/// Context information for server operations that can fail.
pub const ServerContext = struct {
    operation: []const u8,
    connection_id: ?u32 = null,
    client_address: ?[]const u8 = null,
    message_type: ?u8 = null,
    message_size: ?usize = null,
    bytes_processed: ?usize = null,
    error_code: ?u32 = null,

    pub fn format(
        self: ServerContext,
        comptime fmt: []const u8,
        options: std.fmt.FormatOptions,
        writer: anytype,
    ) !void {
        _ = fmt;
        _ = options;

        try writer.print("ServerContext{{ operation=\"{s}\"", .{self.operation});

        if (self.connection_id) |id| {
            try writer.print(", connection_id={}", .{id});
        }
        if (self.client_address) |addr| {
            try writer.print(", client=\"{s}\"", .{addr});
        }
        if (self.message_type) |msg_type| {
            try writer.print(", message_type={}", .{msg_type});
        }
        if (self.message_size) |size| {
            try writer.print(", message_size={}", .{size});
        }
        if (self.bytes_processed) |bytes| {
            try writer.print(", bytes_processed={}", .{bytes});
        }
        if (self.error_code) |code| {
            try writer.print(", error_code={}", .{code});
        }

        try writer.print(" }}", .{});
    }
};

/// Log a buffer error with context in debug builds only.
pub fn log_buffer_error(err: anyerror, context: BufferContext) void {
    if (builtin.mode == .Debug) {
        log.warn("Buffer operation failed: {any} - {any}", .{ err, context });
    }
}

/// Check if we're in verbose mode (used by fuzz testing)
fn is_verbose_mode() bool {
    // Check if we're in a fuzz testing context with verbose enabled
    if (@hasDecl(@import("root"), "global_verbose_mode")) {
        const root = @import("root");
        return root.global_verbose_mode.load(.seq_cst);
    }
    return false;
}

/// Increment global validation error counter if available
fn increment_validation_errors() void {
    if (@hasDecl(@import("root"), "global_validation_errors")) {
        const root = @import("root");
        _ = root.global_validation_errors.fetchAdd(1, .seq_cst);
    }
}

/// Log a storage error with context in verbose mode only.
/// Counts validation errors for statistics but only logs details when verbose enabled.
pub fn log_storage_error(err: anyerror, context: StorageContext) void {
    // Always count validation errors for summary statistics
    increment_validation_errors();

    // Only log details in verbose mode (for fuzzing compatibility)
    if (is_verbose_mode()) {
        log.warn("Storage operation failed: {any} - {any}", .{ err, context });
    }
}

/// Log a WAL error with context in debug builds only.
pub fn log_wal_error(err: anyerror, context: WALContext) void {
    if (builtin.mode == .Debug) {
        log.warn("WAL operation failed: {any} - {any}", .{ err, context });
    }
}

/// Log an ingestion error with context in debug builds only.
pub fn log_ingestion_error(err: anyerror, context: IngestionContext) void {
    if (builtin.mode == .Debug) {
        log.warn("Ingestion operation failed: {any} - {any}", .{ err, context });
    }
}

/// Log a server error with context in debug builds only.
pub fn log_server_error(err: anyerror, context: ServerContext) void {
    if (builtin.mode == .Debug) {
        log.warn("Server operation failed: {any} - {any}", .{ err, context });
    }
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

/// Helper to create server context for connection operations.
pub fn connection_context(operation: []const u8, connection_id: u32) ServerContext {
    return ServerContext{
        .operation = operation,
        .connection_id = connection_id,
    };
}

/// Helper to create server context for I/O operations.
pub fn server_io_context(operation: []const u8, connection_id: u32, bytes_processed: ?usize) ServerContext {
    return ServerContext{
        .operation = operation,
        .connection_id = connection_id,
        .bytes_processed = bytes_processed,
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

/// Helper to create ingestion context for repository operations.
pub fn repository_context(operation: []const u8, repository_path: []const u8) IngestionContext {
    return IngestionContext{
        .operation = operation,
        .repository_path = repository_path,
    };
}

/// Helper to create ingestion context for file operations.
pub fn ingestion_file_context(
    operation: []const u8,
    repository_path: []const u8,
    file_path: []const u8,
    content_type: ?[]const u8,
) IngestionContext {
    return IngestionContext{
        .operation = operation,
        .repository_path = repository_path,
        .file_path = file_path,
        .content_type = content_type,
    };
}

/// Helper to create ingestion context for file size violations.
pub fn file_size_context(
    operation: []const u8,
    file_path: []const u8,
    file_size: u64,
    max_allowed_size: u64,
) IngestionContext {
    return IngestionContext{
        .operation = operation,
        .file_path = file_path,
        .file_size = file_size,
        .max_allowed_size = max_allowed_size,
    };
}

/// Helper to create ingestion context for parsing operations.
pub fn parsing_context(
    operation: []const u8,
    file_path: []const u8,
    content_type: []const u8,
    line_number: ?u32,
    parsing_stage: ?[]const u8,
) IngestionContext {
    return IngestionContext{
        .operation = operation,
        .file_path = file_path,
        .content_type = content_type,
        .line_number = line_number,
        .parsing_stage = parsing_stage,
    };
}

/// Helper to create ingestion context for chunking operations.
pub fn chunking_context(
    operation: []const u8,
    unit_type: []const u8,
    unit_count: usize,
) IngestionContext {
    return IngestionContext{
        .operation = operation,
        .unit_type = unit_type,
        .unit_count = unit_count,
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

test "error logging functions work correctly" {
    const original_error = error.TestError;

    const storage_ctx = StorageContext{ .operation = "test" };
    log_storage_error(original_error, storage_ctx);

    const wal_ctx = WALContext{ .operation = "test" };
    log_wal_error(original_error, wal_ctx);

    const buffer_ctx = BufferContext{ .operation = "test" };
    log_buffer_error(original_error, buffer_ctx);

    const ingestion_ctx = IngestionContext{ .operation = "test" };
    log_ingestion_error(original_error, ingestion_ctx);
}

test "IngestionContext formatting" {
    const ctx = IngestionContext{
        .operation = "parse_zig_function",
        .repository_path = "./my-repo",
        .file_path = "src/parser.zig",
        .content_type = "text/zig",
        .line_number = 245,
        .unit_type = "function",
        .file_size = 8192,
        .max_allowed_size = 10240,
        .parsing_stage = "tokenization",
    };

    var buf: [1024]u8 = undefined;
    const formatted = try std.fmt.bufPrint(&buf, "{any}", .{ctx});

    // Should contain all the context fields
    try std.testing.expect(std.mem.indexOf(u8, formatted, "parse_zig_function") != null);
    try std.testing.expect(std.mem.indexOf(u8, formatted, "./my-repo") != null);
    try std.testing.expect(std.mem.indexOf(u8, formatted, "src/parser.zig") != null);
    try std.testing.expect(std.mem.indexOf(u8, formatted, "text/zig") != null);
    try std.testing.expect(std.mem.indexOf(u8, formatted, "line=245") != null);
    try std.testing.expect(std.mem.indexOf(u8, formatted, "function") != null);
    try std.testing.expect(std.mem.indexOf(u8, formatted, "file_size=8192") != null);
    try std.testing.expect(std.mem.indexOf(u8, formatted, "tokenization") != null);
}

test "ingestion context helpers" {
    // Test repository context
    const repo_ctx = repository_context("validate_repository", "/path/to/repo");
    try std.testing.expectEqualStrings("validate_repository", repo_ctx.operation);
    try std.testing.expectEqualStrings("/path/to/repo", repo_ctx.repository_path.?);

    // Test file context
    const file_ctx = ingestion_file_context("read_file", "/repo", "file.zig", "text/zig");
    try std.testing.expectEqualStrings("read_file", file_ctx.operation);
    try std.testing.expectEqualStrings("/repo", file_ctx.repository_path.?);
    try std.testing.expectEqualStrings("file.zig", file_ctx.file_path.?);
    try std.testing.expectEqualStrings("text/zig", file_ctx.content_type.?);

    // Test file size context
    const size_ctx = file_size_context("validate_size", "large_file.txt", 20480, 10240);
    try std.testing.expectEqualStrings("validate_size", size_ctx.operation);
    try std.testing.expectEqualStrings("large_file.txt", size_ctx.file_path.?);
    try std.testing.expectEqual(@as(u64, 20480), size_ctx.file_size.?);
    try std.testing.expectEqual(@as(u64, 10240), size_ctx.max_allowed_size.?);

    // Test parsing context
    const parse_ctx = parsing_context("parse_function", "test.zig", "text/zig", 42, "ast_generation");
    try std.testing.expectEqualStrings("parse_function", parse_ctx.operation);
    try std.testing.expectEqualStrings("test.zig", parse_ctx.file_path.?);
    try std.testing.expectEqualStrings("text/zig", parse_ctx.content_type.?);
    try std.testing.expectEqual(@as(u32, 42), parse_ctx.line_number.?);
    try std.testing.expectEqualStrings("ast_generation", parse_ctx.parsing_stage.?);

    // Test chunking context
    const chunk_ctx = chunking_context("create_chunks", "function", 15);
    try std.testing.expectEqualStrings("create_chunks", chunk_ctx.operation);
    try std.testing.expectEqualStrings("function", chunk_ctx.unit_type.?);
    try std.testing.expectEqual(@as(usize, 15), chunk_ctx.unit_count.?);
}
