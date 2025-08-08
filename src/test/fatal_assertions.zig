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
            try writer.print("Thread:    {}\n", .{tid});
        }

        try writer.print("--------------------------------------------------------------------------------\n", .{});
    }
};

/// Fatal assertion with enhanced context and forensic information
pub fn fatal_assert_ctx(
    condition: bool,
    context: FatalContext,
    comptime format: []const u8,
    args: anytype,
) void {
    if (!condition) {
        var stderr_writer = std.io.getStdOut().writer();

        // Format enhanced header
        context.format_header(stderr_writer) catch {};

        // Format the specific assertion message
        stderr_writer.print("ASSERTION: " ++ format ++ "\n", args) catch {};

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

        stderr_writer.print("--------------------------------------------------------------------------------\n", .{}) catch {};
        stderr_writer.print("{s}\n", .{debug_hint}) catch {};
        stderr_writer.print("================================================================================\n\n", .{}) catch {};

        std.process.exit(1);
    }
}

/// Specialized fatal assertions for KausalDB components
/// Assert that a BlockId is valid (non-zero)
pub fn fatal_assert_block_id_valid(
    block_id: anytype,
    comptime src: std.builtin.SourceLocation,
) void {
    const context = FatalContext.init(
        .invariant_violation,
        "Storage Engine",
        "Block ID validation",
        src,
    );

    // Check for zero BlockId (invalid)
    const is_zero = blk: {
        for (block_id.bytes) |byte| {
            if (byte != 0) break :blk false;
        }
        break :blk true;
    };

    fatal_assert_ctx(!is_zero, context, "Invalid BlockId: all bytes are zero (BlockId must be non-zero)", .{});
}

/// Assert that memory is properly aligned for DMA/mmap operations
pub fn fatal_assert_memory_aligned(
    ptr: anytype,
    alignment: usize,
    comptime src: std.builtin.SourceLocation,
) void {
    const context = FatalContext.init(
        .memory_corruption,
        "Memory Manager",
        "Alignment validation",
        src,
    );

    const addr = @intFromPtr(ptr);
    fatal_assert_ctx(addr % alignment == 0, context, "Memory alignment violation: address 0x{x} not aligned to {} bytes (offset: {})", .{ addr, alignment, addr % alignment });
}

/// Assert that a buffer write will not cause overflow (always fatal)
pub fn fatal_assert_buffer_bounds(
    pos: usize,
    write_len: usize,
    buffer_len: usize,
    comptime src: std.builtin.SourceLocation,
) void {
    const context = FatalContext.init(
        .memory_corruption,
        "Buffer Manager",
        "Bounds checking",
        src,
    );

    // Check for overflow in the addition itself
    const overflow = @addWithOverflow(pos, write_len);
    if (overflow[1] != 0) {
        fatal_assert_ctx(false, context, "Buffer position overflow: {} + {} causes integer overflow", .{ pos, write_len });
        return;
    }

    fatal_assert_ctx(pos + write_len <= buffer_len, context, "Buffer overflow: attempting to write {} bytes at position {} in buffer of {} bytes (exceeds by {} bytes)", .{ write_len, pos, buffer_len, (pos + write_len) - buffer_len });
}

/// Assert that file system operation succeeded (for critical I/O)
pub fn fatal_assert_file_operation(
    result: anytype,
    operation: []const u8,
    path: []const u8,
    comptime src: std.builtin.SourceLocation,
) void {
    const context = FatalContext.init(
        .data_corruption,
        "File System",
        operation,
        src,
    );

    if (@TypeOf(result) == anyerror) {
        fatal_assert_ctx(false, context, "Critical file operation '{s}' failed on path '{s}': {}", .{ operation, path, result });
    }

    // For non-error types, check if it's a boolean or optional indicating failure
    const TypeInfo = @typeInfo(@TypeOf(result));
    switch (TypeInfo) {
        .bool => {
            fatal_assert_ctx(result, context, "Critical file operation '{s}' failed on path '{s}' (returned false)", .{ operation, path });
        },
        .optional => {
            fatal_assert_ctx(result != null, context, "Critical file operation '{s}' failed on path '{s}' (returned null)", .{ operation, path });
        },
        else => {
            // For other types, assume non-zero means success
            fatal_assert_ctx(result != 0, context, "Critical file operation '{s}' failed on path '{s}' (returned {})", .{ operation, path, result });
        },
    }
}

/// Assert that a CRC checksum matches expected value (data integrity)
pub fn fatal_assert_crc_valid(
    actual: u64,
    expected: u64,
    data_description: []const u8,
    comptime src: std.builtin.SourceLocation,
) void {
    const context = FatalContext.init(
        .data_corruption,
        "Integrity Checker",
        "CRC validation",
        src,
    );

    fatal_assert_ctx(actual == expected, context, "Data corruption detected in {s}: CRC mismatch (actual: 0x{x}, expected: 0x{x})", .{ data_description, actual, expected });
}

/// Assert that a WAL entry is properly formatted and not corrupted
pub fn fatal_assert_wal_entry_valid(
    entry: anytype,
    comptime src: std.builtin.SourceLocation,
) void {
    const context = FatalContext.init(
        .data_corruption,
        "Write-Ahead Log",
        "Entry validation",
        src,
    );

    // Check magic number
    fatal_assert_ctx(entry.magic == 0x57414C45, // "WALE" in hex
        context, "WAL entry corruption: invalid magic number (actual: 0x{x}, expected: 0x57414C45)", .{entry.magic});

    // Check entry size is reasonable
    fatal_assert_ctx(entry.size > 0 and entry.size <= 64 * 1024 * 1024, // Max 64MB per entry
        context, "WAL entry corruption: invalid size {} (must be between 1 and 67108864 bytes)", .{entry.size});
}

/// Assert that context state transitions are valid
pub fn fatal_assert_context_transition(
    from_state: anytype,
    to_state: @TypeOf(from_state),
    comptime valid_transitions: []const struct { from: @TypeOf(from_state), to: @TypeOf(from_state) },
    comptime src: std.builtin.SourceLocation,
) void {
    const context = FatalContext.init(
        .invariant_violation,
        "Context Manager",
        "State transition",
        src,
    );

    // Check if this transition is valid
    const transition_valid = for (valid_transitions) |transition| {
        if (transition.from == from_state and transition.to == to_state) {
            break true;
        }
    } else false;

    fatal_assert_ctx(transition_valid, context, "Invalid context state transition: {} -> {} (not in allowed transitions)", .{ from_state, to_state });
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
    const context = FatalContext.init(
        .protocol_violation,
        protocol_name,
        invariant_description,
        src,
    );

    fatal_assert_ctx(condition, context, format, args);
}

/// Assert that resource usage is within acceptable limits
pub fn fatal_assert_resource_limit(
    current_usage: u64,
    limit: u64,
    resource_name: []const u8,
    comptime src: std.builtin.SourceLocation,
) void {
    const context = FatalContext.init(
        .resource_exhaustion,
        "Resource Manager",
        resource_name,
        src,
    );

    fatal_assert_ctx(current_usage <= limit, context, "Resource limit exceeded for {s}: current usage {} exceeds limit {} (overage: {})", .{ resource_name, current_usage, limit, current_usage - limit });
}

/// Test helper to validate fatal assertion behavior in controlled environment
pub const FatalAssertionTester = struct {
    allocator: std.mem.Allocator,
    captured_outputs: std.ArrayList([]const u8),

    pub fn init(allocator: std.mem.Allocator) FatalAssertionTester {
        return FatalAssertionTester{
            .allocator = allocator,
            .captured_outputs = std.ArrayList([]const u8).init(allocator),
        };
    }

    pub fn deinit(self: *FatalAssertionTester) void {
        for (self.captured_outputs.items) |output| {
            self.allocator.free(output);
        }
        self.captured_outputs.deinit();
    }

    /// Test that a fatal assertion triggers under specific conditions
    /// This is primarily for unit testing the assertion framework itself
    pub fn expect_fatal_assertion(
        self: *FatalAssertionTester,
        comptime assertion_fn: anytype,
        args: anytype,
        expected_category: FatalCategory,
    ) !void {
        // In a real implementation, this would capture the assertion output
        // For now, we'll validate that the assertion would trigger
        _ = self;
        _ = assertion_fn;
        _ = args;
        _ = expected_category;

        // This is a placeholder for test infrastructure that would:
        // 1. Fork the process or use setjmp/longjmp
        // 2. Capture the assertion output
        // 3. Validate the category and message format
        // 4. Return control to the test
    }
};

// Convenience macros that automatically provide source location

/// Macro for fatal block ID validation
pub inline fn FATAL_ASSERT_BLOCK_ID_VALID(block_id: anytype) void {
    fatal_assert_block_id_valid(block_id, @src());
}

/// Macro for fatal memory alignment validation
pub inline fn FATAL_ASSERT_MEMORY_ALIGNED(ptr: anytype, alignment: usize) void {
    fatal_assert_memory_aligned(ptr, alignment, @src());
}

/// Macro for fatal buffer bounds validation
pub inline fn FATAL_ASSERT_BUFFER_BOUNDS(pos: usize, write_len: usize, buffer_len: usize) void {
    fatal_assert_buffer_bounds(pos, write_len, buffer_len, @src());
}

/// Macro for fatal file operation validation
pub inline fn FATAL_ASSERT_FILE_OPERATION(result: anytype, operation: []const u8, path: []const u8) void {
    fatal_assert_file_operation(result, operation, path, @src());
}

/// Macro for fatal CRC validation
pub inline fn FATAL_ASSERT_CRC_VALID(actual: u64, expected: u64, data_description: []const u8) void {
    fatal_assert_crc_valid(actual, expected, data_description, @src());
}

/// Macro for fatal WAL entry validation
pub inline fn FATAL_ASSERT_WAL_ENTRY_VALID(entry: anytype) void {
    fatal_assert_wal_entry_valid(entry, @src());
}

/// Macro for fatal context state transition validation
pub inline fn FATAL_ASSERT_CONTEXT_TRANSITION(
    from_state: anytype,
    to_state: @TypeOf(from_state),
    comptime valid_transitions: anytype,
) void {
    fatal_assert_context_transition(from_state, to_state, valid_transitions, @src());
}

/// Macro for fatal protocol invariant validation
pub inline fn FATAL_ASSERT_PROTOCOL_INVARIANT(
    condition: bool,
    protocol_name: []const u8,
    invariant_description: []const u8,
    comptime format: []const u8,
    args: anytype,
) void {
    fatal_assert_protocol_invariant(condition, protocol_name, invariant_description, format, args, @src());
}

/// Macro for fatal resource limit validation
pub inline fn FATAL_ASSERT_RESOURCE_LIMIT(
    current_usage: u64,
    limit: u64,
    resource_name: []const u8,
) void {
    fatal_assert_resource_limit(current_usage, limit, resource_name, @src());
}
