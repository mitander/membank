//! Assertion framework for Membank defensive programming.
//!
//! Provides runtime checks that help catch bugs early in development
//! while being compiled out in release builds for performance.

const std = @import("std");
const builtin = @import("builtin");

/// Assert that a condition is true.
/// This assertion is active in debug builds and compiled out in release builds.
/// Use this for simple programming invariants that should never be false.
pub fn assert(condition: bool) void {
    if (builtin.mode == .Debug) {
        if (!condition) {
            std.debug.panic("Assertion failed", .{});
        }
    }
}

/// Assert with rich formatting for detailed debugging context.
/// Use when specific values are critical for debugging.
/// # Examples
/// ```zig
/// assert_fmt(index < array.len, "Index out of bounds: {} >= {}", .{ index, array.len });
/// assert_fmt(context.is_valid(), "Context is invalid: {}", .{context});
/// ```
pub fn assert_fmt(condition: bool, comptime format: []const u8, args: anytype) void {
    if (builtin.mode == .Debug) {
        if (!condition) {
            std.debug.panic("Assertion failed: " ++ format, args);
        }
    }
}

/// Assert that a condition is true with a descriptive message.
/// This variant is always active, even in release builds.
/// Use this for critical safety violations that must never occur,
/// such as buffer overflows or corrupted data structures.
/// # Examples
/// ```zig
/// fatal_assert(buffer_pos < buffer.len, "Buffer overflow: {} >= {}",
///               .{ buffer_pos, buffer.len });
/// ```
pub fn fatal_assert(condition: bool, comptime format: []const u8, args: anytype) void {
    if (!condition) {
        std.debug.panic("Critical assertion failed: " ++ format, args);
    }
}

/// @deprecated Use fatal_assert instead. This alias is provided for backward compatibility.
/// Assert that a condition is true with a descriptive message.
/// This variant is always active, even in release builds.
/// Use fatal_assert for critical safety violations that must never occur.
pub fn assert_always(condition: bool, comptime format: []const u8, args: anytype) void {
    fatal_assert(condition, format, args);
}

/// Assert that a value is within a valid range.
/// # Examples
/// ```zig
/// assert_range(value, 0, 100, "Value {} not in range 0-100", .{value});
/// ```
pub fn assert_range(
    value: anytype,
    min: @TypeOf(value),
    max: @TypeOf(value),
    comptime format: []const u8,
    args: anytype,
) void {
    assert_fmt(value >= min and value <= max, format, args);
}

/// Assert that a buffer write operation will not overflow.
/// # Examples
/// ```zig
/// assert_buffer_bounds(pos, data.len, buffer.len, "Buffer overflow: {} + {} > {}",
///                      .{ pos, data.len, buffer.len });
/// ```
pub fn assert_buffer_bounds(
    pos: usize,
    write_len: usize,
    buffer_len: usize,
    comptime format: []const u8,
    args: anytype,
) void {
    assert_fmt(pos + write_len <= buffer_len, format, args);
}

/// Assert that a counter will not overflow.
/// # Examples
/// ```zig
/// assert_counter_bounds(current_count, max_count, "Counter overflow: {} > {}",
///                       .{ current_count, max_count });
/// ```
pub fn assert_counter_bounds(
    current: anytype,
    max: @TypeOf(current),
    comptime format: []const u8,
    args: anytype,
) void {
    assert_fmt(current <= max, format, args);
}

/// Assert that a state transition is valid.
/// # Examples
/// ```zig
/// assert_state_valid(old_state == .initializing or old_state == .ready,
///                   "Invalid state transition from {}", .{old_state});
/// ```
pub fn assert_state_valid(condition: bool, comptime format: []const u8, args: anytype) void {
    assert_fmt(condition, "State violation: " ++ format, args);
}

/// Assert that a stride value is positive.
/// # Examples
/// ```zig
/// assert_stride_positive(stride, "Invalid stride: {} must be positive", .{stride});
/// ```
pub fn assert_stride_positive(
    stride: anytype,
    comptime format: []const u8,
    args: anytype,
) void {
    assert_fmt(stride > 0, format, args);
}

/// Compile-time assertion for constant conditions.
/// # Examples
/// ```zig
/// comptime_assert(@sizeOf(BlockHeader) == 64, "BlockHeader must be exactly 64 bytes");
/// ```
pub fn comptime_assert(comptime condition: bool, comptime message: []const u8) void {
    if (!condition) {
        @compileError(message);
    }
}

/// Compile-time assertion that a struct has no padding bytes.
/// This ensures the struct layout is densely packed and predictable for
/// on-disk formats and network protocols.
/// # Examples
/// ```zig
/// comptime_no_padding(BlockHeader);
/// ```
pub fn comptime_no_padding(comptime T: type) void {
    comptime {
        const type_info = @typeInfo(T);
        switch (type_info) {
            .@"struct" => |struct_info| {
                var expected_size: usize = 0;

                // Calculate expected size by summing field sizes
                for (struct_info.fields) |field| {
                    expected_size += @sizeOf(field.type);
                }

                const actual_size = @sizeOf(T);
                if (actual_size != expected_size) {
                    @compileError(std.fmt.comptimePrint("Struct {} has padding: actual size {} != expected size {} (padding = {} bytes). " ++
                        "Use packed struct or reorganize fields to eliminate padding.", .{ @typeName(T), actual_size, expected_size, actual_size - expected_size }));
                }
            },
            else => @compileError("comptime_no_padding can only be used with struct types"),
        }
    }
}

/// Assert that a pointer is not null.
/// # Examples
/// ```zig
/// assert_not_null(maybe_ptr, "Pointer cannot be null");
/// ```
pub fn assert_not_null(ptr: anytype, comptime format: []const u8, args: anytype) void {
    assert_fmt(ptr != null, format, args);
}

/// Assert that two values are equal.
/// # Examples
/// ```zig
/// assert_equal(actual, expected, "Values not equal: {} != {}", .{ actual, expected });
/// ```
pub fn assert_equal(
    actual: anytype,
    expected: @TypeOf(actual),
    comptime format: []const u8,
    args: anytype,
) void {
    const T = @TypeOf(actual);
    const type_info = @typeInfo(T);

    const equal = switch (type_info) {
        .array => |array_info| std.mem.eql(array_info.child, &actual, &expected),
        .pointer => |ptr_info| switch (ptr_info.size) {
            .slice => std.mem.eql(ptr_info.child, actual, expected),
            else => actual == expected,
        },
        else => actual == expected,
    };

    assert_fmt(equal, format, args);
}

/// Assert that a slice is not empty.
/// # Examples
/// ```zig
/// assert_not_empty(slice, "Slice cannot be empty");
/// ```
pub fn assert_not_empty(slice: anytype, comptime format: []const u8, args: anytype) void {
    assert_fmt(slice.len > 0, format, args);
}

/// Assert that an index is within bounds.
/// # Examples
/// ```zig
/// assert_index_valid(index, array.len, "Index out of bounds: {} >= {}",
///                      .{ index, array.len });
/// ```
pub fn assert_index_valid(
    index: usize,
    length: usize,
    comptime format: []const u8,
    args: anytype,
) void {
    assert_fmt(index < length, format, args);
}

/// Assert that memory regions do not overlap.
/// # Examples
/// ```zig
/// assert_no_overlap(src_ptr, src_len, dst_ptr, dst_len, "Memory regions overlap");
/// ```
pub fn assert_no_overlap(
    src_ptr: [*]const u8,
    src_len: usize,
    dst_ptr: [*]u8,
    dst_len: usize,
    comptime format: []const u8,
    args: anytype,
) void {
    const src_end = src_ptr + src_len;
    const dst_end = dst_ptr + dst_len;
    const no_overlap = (src_end <= dst_ptr) or (dst_end <= src_ptr);
    assert_fmt(no_overlap, format, args);
}

/// Utility to check if assertions are enabled.
/// Useful for conditional code that should only run when assertions are active.
pub fn assertions_enabled() bool {
    return builtin.mode == .Debug;
}

/// Utility to perform expensive checks only when assertions are enabled.
/// # Examples
/// ```zig
/// if (expensive_check_enabled()) {
///     assert(validate_data_structure(data), "Data structure is invalid");
/// }
/// ```
pub fn expensive_check_enabled() bool {
    return assertions_enabled();
}

test "assert basic functionality" {
    // Simple assert should not panic
    assert(true);

    // Rich assert should not panic
    assert_fmt(true, "This should not fail", .{});

    // Test with formatting
    const value = 42;
    assert_fmt(value == 42, "Expected 42, got {}", .{value});
}

test "assert_range functionality" {
    assert_range(50, 0, 100, "Value {} not in range 0-100", .{50});
    assert_range(0, 0, 100, "Value {} not in range 0-100", .{0});
    assert_range(100, 0, 100, "Value {} not in range 0-100", .{100});
}

test "assert_buffer_bounds functionality" {
    const buffer_len = 100;
    assert_buffer_bounds(
        0,
        50,
        buffer_len,
        "Buffer overflow: {} + {} > {}",
        .{ 0, 50, buffer_len },
    );
    assert_buffer_bounds(
        50,
        50,
        buffer_len,
        "Buffer overflow: {} + {} > {}",
        .{ 50, 50, buffer_len },
    );
}

test "assert_index_valid functionality" {
    const array_len = 10;
    assert_index_valid(0, array_len, "Index out of bounds: {} >= {}", .{ 0, array_len });
    assert_index_valid(9, array_len, "Index out of bounds: {} >= {}", .{ 9, array_len });
}

test "assert_not_empty functionality" {
    const slice = [_]u8{ 1, 2, 3 };
    assert_not_empty(slice[0..], "Slice cannot be empty", .{});
}

test "assert_equal functionality" {
    assert_equal(42, 42, "Values not equal: {} != {}", .{ 42, 42 });
    assert_equal("hello", "hello", "Strings not equal: {s} != {s}", .{ "hello", "hello" });
}

test "comptime_assert functionality" {
    comptime_assert(true, "This should not fail at compile time");
    comptime_assert(@sizeOf(u32) == 4, "u32 should be 4 bytes");
}

test "assertions_enabled utility" {
    // Should be true in debug builds
    const enabled = assertions_enabled();
    if (builtin.mode == .Debug) {
        try std.testing.expect(enabled);
    }
}

test "assert_counter_bounds functionality" {
    assert_counter_bounds(10, 100, "Counter overflow: {} > {}", .{ 10, 100 });
    assert_counter_bounds(100, 100, "Counter overflow: {} > {}", .{ 100, 100 });
}

test "assert_state_valid functionality" {
    const State = enum { init, ready, running, stopped };
    const current_state = State.ready;

    assert_state_valid(current_state == .ready, "Invalid state: {}", .{current_state});
}

test "assert_stride_positive functionality" {
    assert_stride_positive(1, "Invalid stride: {} must be positive", .{1});
    assert_stride_positive(100, "Invalid stride: {} must be positive", .{100});
}

test "comptime_no_padding functionality" {
    // Test struct with no padding
    const PackedStruct = packed struct {
        a: u8,
        b: u8,
        c: u16,
    };
    comptime_no_padding(PackedStruct);

    // Test struct with natural alignment (no padding needed)
    const AlignedStruct = struct {
        a: u32,
        b: u32,
    };
    comptime_no_padding(AlignedStruct);
}

test "assertion behavior matches documentation" {
    // Verify that assertion enablement matches documented behavior:
    // - Debug builds: assertions are active
    // - Release builds: assertions are compiled out (no-ops)

    const debug_mode = builtin.mode == .Debug;

    // Verify assertion enablement utilities match build mode
    try std.testing.expectEqual(debug_mode, assertions_enabled());
    try std.testing.expectEqual(debug_mode, expensive_check_enabled());

    // Test that debug assertions work when conditions are true
    // (These should be no-ops in release, active in debug)
    assert(true);
    assert_fmt(true, "Debug assertion with valid condition", .{});

    // Test that fatal assertions always work regardless of build mode
    fatal_assert(true, "Fatal assertion should always be active", .{});

    // Test backward compatibility alias
    assert_always(true, "Backward compatibility alias works", .{});

    // This test validates our fix to P0.2: Assertion Framework Inconsistency
    // The implementation now matches the documentation:
    // - assert() and assert_fmt() are no-ops in release builds
    // - fatal_assert() always panics on false conditions
}
