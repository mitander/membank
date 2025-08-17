//! Enhanced Fatal Assertion Validation Framework
//!
//! Provides specialized fatal assertion functions for critical KausalDB operations
//! with enhanced error reporting, debugging context, and validation categories.
//! Fatal assertions are always active regardless of build mode and provide
//! detailed forensic information for debugging critical failures.

const std = @import("std");
const builtin = @import("builtin");
const assert = @import("../core/assert.zig");

/// Categories of fatal assertion failures for better debugging
pub const FatalCategory = enum {
    /// Memory corruption or invalid memory access
    memory_corruption,
    /// Data structure invariant violation (corrupt state)
    invariant_violation,
    /// File system or I/O corruption
    data_corruption,
    /// Network protocol violation
    protocol_violation,
    /// Resource exhaustion or limits exceeded
    resource_exhaustion,
    /// Logic error that should never occur
    logic_error,
    /// Security violation or authentication failure
    security_violation,

    pub fn description(self: FatalCategory) []const u8 {
        return switch (self) {
            .memory_corruption => "MEMORY CORRUPTION",
            .invariant_violation => "INVARIANT VIOLATION",
            .data_corruption => "DATA CORRUPTION",
            .protocol_violation => "PROTOCOL VIOLATION",
            .resource_exhaustion => "RESOURCE EXHAUSTION",
            .logic_error => "LOGIC ERROR",
            .security_violation => "SECURITY VIOLATION",
        };
    }
};

/// Enhanced context for fatal assertion failures
/// Created lazily only when assertion fails to minimize performance overhead
pub const FatalContext = struct {
    category: FatalCategory,
    component: []const u8,
    operation: []const u8,
    file: []const u8,
    line: u32,
    thread_id: ?std.Thread.Id = null,
    timestamp: i64,

    pub fn init(
        category: FatalCategory,
        component: []const u8,
        operation: []const u8,
        comptime src: std.builtin.SourceLocation,
    ) FatalContext {
        return FatalContext{
            .category = category,
            .component = component,
            .operation = operation,
            .file = src.file,
            .line = src.line,
            .timestamp = std.time.timestamp(),
        };
    }

    pub fn format_header(self: FatalContext, writer: anytype) !void {
        try writer.print("\n" ++
            "================================================================================\n" ++
            "FATAL ASSERTION FAILURE: {s}\n" ++
            "================================================================================\n" ++
            "Component: {s}\n" ++
            "Operation: {s}\n" ++
            "Location:  {s}:{d}\n" ++
            "Time:      {d} (unix timestamp)\n", .{
            self.category.description(),
            self.component,
            self.operation,
            self.file,
            self.line,
            self.timestamp,
        });

        if (self.thread_id) |tid| {
            try writer.print("Thread ID: {d}\n", .{tid});
        }

        try writer.print("--------------------------------------------------------------------------------\n", .{});
    }
};

/// Fatal assertion with enhanced context and forensic information
/// Context is created lazily only when assertion fails to eliminate performance overhead
pub fn fatal_assert_ctx(
    condition: bool,
    category: FatalCategory,
    component: []const u8,
    operation: []const u8,
    comptime src: std.builtin.SourceLocation,
    comptime format: []const u8,
    args: anytype,
) void {
    if (!condition) {
        // Create context lazily only when assertion fails
        const context = FatalContext.init(category, component, operation, src);

        // Use a dummy writer struct that delegates to std.debug.print
        const DummyWriter = struct {
            pub fn print(self: @This(), comptime fmt: []const u8, print_args: anytype) !void {
                _ = self;
                std.debug.print(fmt, print_args);
            }
        };
        const dummy_writer = DummyWriter{};

        // Format enhanced header
        context.format_header(dummy_writer) catch {};

        // Format the specific assertion message
        std.debug.print("ASSERTION: " ++ format ++ "\n", args);

        // Add debugging hints based on category
        const debug_hint = switch (context.category) {
            .memory_corruption => "DEBUGGING HINT: Use AddressSanitizer or Valgrind to detect the source of corruption",
            .invariant_violation => "DEBUGGING HINT: Check data structure consistency and state transitions",
            .data_corruption => "DEBUGGING HINT: Verify file integrity and I/O error handling",
            .protocol_violation => "DEBUGGING HINT: Review network packet formats and protocol state machines",
            .resource_exhaustion => "DEBUGGING HINT: Check resource limits, memory usage, and cleanup procedures",
            .logic_error => "DEBUGGING HINT: Review algorithmic assumptions and edge case handling",
            .security_violation => "DEBUGGING HINT: Audit authentication, authorization, and input validation",
        };

        std.debug.print("{s}\n", .{debug_hint});
        std.debug.print("================================================================================\n\n", .{});

        std.process.exit(1);
    }
}

/// Assert that a BlockId is valid (non-zero)
pub fn fatal_assert_block_id_valid(
    block_id: anytype,
    comptime src: std.builtin.SourceLocation,
) void {
    // Check for zero BlockId (invalid)
    const is_zero = blk: {
        for (block_id.bytes) |byte| {
            if (byte != 0) break :blk false;
        }
        break :blk true;
    };

    fatal_assert_ctx(!is_zero, .invariant_violation, "Storage Engine", "Block ID validation", src, "Invalid BlockId: all bytes are zero (BlockId must be non-zero)", .{});
}

/// Assert that memory is properly aligned for DMA/mmap operations
pub fn fatal_assert_memory_aligned(
    ptr: anytype,
    alignment: usize,
    comptime src: std.builtin.SourceLocation,
) void {
    const addr = @intFromPtr(ptr);
    fatal_assert_ctx(addr % alignment == 0, .memory_corruption, "Memory Manager", "Alignment validation", src, "Memory alignment violation: address 0x{x} not aligned to {} bytes (offset: {})", .{ addr, alignment, addr % alignment });
}

/// Assert that a buffer write will not cause overflow (always fatal)
pub fn fatal_assert_buffer_bounds(
    pos: usize,
    write_len: usize,
    buffer_len: usize,
    comptime src: std.builtin.SourceLocation,
) void {
    // Check for overflow in the addition itself
    const overflow = @addWithOverflow(pos, write_len);
    if (overflow[1] != 0) {
        fatal_assert_ctx(false, .memory_corruption, "Buffer Manager", "Bounds checking", src, "Buffer position overflow: {} + {} causes integer overflow", .{ pos, write_len });
        return;
    }

    const end_pos = overflow[0];
    fatal_assert_ctx(end_pos <= buffer_len, .memory_corruption, "Buffer Manager", "Bounds checking", src, "Buffer bounds violation: write extends beyond buffer (pos: {}, write_len: {}, end_pos: {}, buffer_len: {})", .{ pos, write_len, end_pos, buffer_len });
}

/// Assert that file system operation succeeded (for critical I/O)
pub fn fatal_assert_file_operation(
    result: anytype,
    operation: []const u8,
    path: []const u8,
    comptime src: std.builtin.SourceLocation,
) void {
    if (@TypeOf(result) == anyerror) {
        fatal_assert_ctx(false, .data_corruption, "File System", operation, src, "Critical file operation '{s}' failed on path '{s}': {}", .{ operation, path, result });
        return;
    }

    // For non-error types, check if it's a boolean or optional indicating failure
    const TypeInfo = @typeInfo(@TypeOf(result));
    switch (TypeInfo) {
        .bool => {
            fatal_assert_ctx(result, .data_corruption, "File System", operation, src, "Critical file operation '{s}' failed on path '{s}' (returned false)", .{ operation, path });
        },
        .optional => {
            fatal_assert_ctx(result != null, .data_corruption, "File System", operation, src, "Critical file operation '{s}' failed on path '{s}' (returned null)", .{ operation, path });
        },
        else => {
            // For other types, assume non-zero means success
            fatal_assert_ctx(result != 0, .data_corruption, "File System", operation, src, "Critical file operation '{s}' failed on path '{s}' (returned zero)", .{ operation, path });
        },
    }
}

/// Assert that checksum validation passed (data integrity)
pub fn fatal_assert_crc_valid(
    actual: u32,
    expected: u32,
    data_description: []const u8,
    comptime src: std.builtin.SourceLocation,
) void {
    fatal_assert_ctx(actual == expected, .data_corruption, "Integrity Checker", "CRC validation", src, "Data corruption detected in {s}: CRC mismatch (actual: 0x{x}, expected: 0x{x})", .{ data_description, actual, expected });
}

/// Assert that a WAL entry is properly formatted and not corrupted
pub fn fatal_assert_wal_entry_valid(
    entry: anytype,
    comptime src: std.builtin.SourceLocation,
) void {
    // Check magic number
    fatal_assert_ctx(entry.magic == 0x57414C45, // "WALE" in hex
        .data_corruption, "Write-Ahead Log", "Entry validation", src, "WAL entry corruption: invalid magic number (actual: 0x{x}, expected: 0x57414C45)", .{entry.magic});

    // Check entry size is reasonable
    fatal_assert_ctx(entry.size > 0 and entry.size <= 64 * 1024 * 1024, // Max 64MB per entry
        .data_corruption, "Write-Ahead Log", "Entry validation", src, "WAL entry corruption: invalid size {} (must be between 1 and 67108864 bytes)", .{entry.size});
}

/// Assert that context state transitions are valid
pub fn fatal_assert_context_transition(
    from_state: anytype,
    to_state: @TypeOf(from_state),
    comptime valid_transitions: anytype,
    comptime src: std.builtin.SourceLocation,
) void {
    // Check if this transition is valid
    const transition_valid = for (valid_transitions) |transition| {
        if (transition.from == from_state and transition.to == to_state) {
            break true;
        }
    } else false;

    fatal_assert_ctx(transition_valid, .invariant_violation, "Context Manager", "State transition", src, "Invalid context state transition: {} -> {} (not in allowed transitions)", .{ from_state, to_state });
}

/// Assert that network protocol invariant holds
pub fn fatal_assert_protocol_invariant(
    condition: bool,
    protocol_name: []const u8,
    invariant_description: []const u8,
    comptime format: []const u8,
    args: anytype,
    comptime src: std.builtin.SourceLocation,
) void {
    fatal_assert_ctx(condition, .protocol_violation, protocol_name, invariant_description, src, format, args);
}

/// Assert that resource usage is within acceptable limits
pub fn fatal_assert_resource_limit(
    current_usage: u64,
    limit: u64,
    resource_name: []const u8,
    comptime src: std.builtin.SourceLocation,
) void {
    if (current_usage > limit) {
        const overage = current_usage - limit;
        fatal_assert_ctx(false, .resource_exhaustion, "Resource Manager", resource_name, src, "Resource limit exceeded for {s}: current usage {} exceeds limit {} (overage: {})", .{ resource_name, current_usage, limit, overage });
    }
}

/// Enhanced fatal assertions with better error messages
pub const fatal_assert = fatal_assert_ctx;

/// Example usage and test helpers
pub fn fatal_assert_simple(condition: bool, comptime message: []const u8) void {
    fatal_assert_ctx(condition, .logic_error, "Generic", "assertion", @src(), message, .{});
}
