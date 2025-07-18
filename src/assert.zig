//! Assertion framework for CortexDB defensive programming.
//!
//! Provides runtime checks that help catch bugs early in development
//! while being compiled out in release builds for performance.

const std = @import("std");
const builtin = @import("builtin");

/// Assert that a condition is true with a descriptive message.
///
/// This assertion is active in debug builds and compiled out in release builds.
/// Use this for programming invariants that should never be false.
///
/// # Examples
/// ```zig
/// assert(index < array.len, "Index out of bounds: {} >= {}", .{ index, array.len });
/// assert(context.is_valid(), "Context is invalid: {}", .{context});
/// ```
pub fn assert(condition: bool, comptime format: []const u8, args: anytype) void {
    if (!condition) {
        if (builtin.mode == .Debug) {
            std.debug.panic("Assertion failed: " ++ format, args);
        } else {
            // In release builds, assertions are compiled out
            unreachable;
        }
    }
}

/// Assert that a condition is true with a descriptive message.
/// This variant is always active, even in release builds.
///
/// Use this for critical safety violations that must never occur,
/// such as buffer overflows or corrupted data structures.
///
/// # Examples
/// ```zig
/// assert_always(buffer_pos < buffer.len, "Buffer overflow: {} >= {}", .{ buffer_pos, buffer.len });
/// ```
pub fn assert_always(condition: bool, comptime format: []const u8, args: anytype) void {
    if (!condition) {
        std.debug.panic("Critical assertion failed: " ++ format, args);
    }
}

/// Assert that a value is within a valid range.
///
/// # Examples
/// ```zig
/// assert_range(value, 0, 100, "Value {} not in range 0-100", .{value});
/// ```
pub fn assert_range(value: anytype, min: @TypeOf(value), max: @TypeOf(value), comptime format: []const u8, args: anytype) void {
    assert(value >= min and value <= max, format, args);
}

/// Assert that a buffer write operation will not overflow.
///
/// # Examples
/// ```zig
/// assert_buffer_bounds(pos, data.len, buffer.len, "Buffer overflow: {} + {} > {}", .{ pos, data.len, buffer.len });
/// ```
pub fn assert_buffer_bounds(pos: usize, write_len: usize, buffer_len: usize, comptime format: []const u8, args: anytype) void {
    assert(pos + write_len <= buffer_len, format, args);
}

/// Assert that a counter will not overflow.
///
/// # Examples
/// ```zig
/// assert_counter_bounds(current_count, max_count, "Counter overflow: {} > {}", .{ current_count, max_count });
/// ```
pub fn assert_counter_bounds(current: anytype, max: @TypeOf(current), comptime format: []const u8, args: anytype) void {
    assert(current <= max, format, args);
}

/// Assert that a state transition is valid.
///
/// # Examples
/// ```zig
/// assert_state_valid(old_state == .initializing or old_state == .ready, "Invalid state transition from {}", .{old_state});
/// ```
pub fn assert_state_valid(condition: bool, comptime format: []const u8, args: anytype) void {
    assert(condition, "State violation: " ++ format, args);
}

/// Assert that a stride value is positive.
///
/// # Examples
/// ```zig
/// assert_stride_positive(stride, "Invalid stride: {} must be positive", .{stride});
/// ```
pub fn assert_stride_positive(stride: anytype, comptime format: []const u8, args: anytype) void {
    assert(stride > 0, format, args);
}

/// Compile-time assertion for constant conditions.
///
/// # Examples
/// ```zig
/// comptime_assert(@sizeOf(BlockHeader) == 64, "BlockHeader must be exactly 64 bytes");
/// ```
pub fn comptime_assert(comptime condition: bool, comptime message: []const u8) void {
    if (!condition) {
        @compileError(message);
    }
}

/// Assert that a pointer is not null.
///
/// # Examples
/// ```zig
/// assert_not_null(maybe_ptr, "Pointer cannot be null");
/// ```
pub fn assert_not_null(ptr: anytype, comptime format: []const u8, args: anytype) void {
    assert(ptr != null, format, args);
}

/// Assert that two values are equal.
///
/// # Examples
/// ```zig
/// assert_equal(actual, expected, "Values not equal: {} != {}", .{ actual, expected });
/// ```
pub fn assert_equal(actual: anytype, expected: @TypeOf(actual), comptime format: []const u8, args: anytype) void {
    assert(actual == expected, format, args);
}

/// Assert that a slice is not empty.
///
/// # Examples
/// ```zig
/// assert_not_empty(slice, "Slice cannot be empty");
/// ```
pub fn assert_not_empty(slice: anytype, comptime format: []const u8, args: anytype) void {
    assert(slice.len > 0, format, args);
}

/// Assert that an index is within bounds.
///
/// # Examples
/// ```zig
/// assert_index_valid(index, array.len, "Index out of bounds: {} >= {}", .{ index, array.len });
/// ```
pub fn assert_index_valid(index: usize, length: usize, comptime format: []const u8, args: anytype) void {
    assert(index < length, format, args);
}

/// Assert that memory regions do not overlap.
///
/// # Examples
/// ```zig
/// assert_no_overlap(src_ptr, src_len, dst_ptr, dst_len, "Memory regions overlap");
/// ```
pub fn assert_no_overlap(src_ptr: [*]const u8, src_len: usize, dst_ptr: [*]u8, dst_len: usize, comptime format: []const u8, args: anytype) void {
    const src_end = src_ptr + src_len;
    const dst_end = dst_ptr + dst_len;
    const no_overlap = (src_end <= dst_ptr) or (dst_end <= src_ptr);
    assert(no_overlap, format, args);
}

/// Utility to check if assertions are enabled.
/// Useful for conditional code that should only run when assertions are active.
pub fn assertions_enabled() bool {
    return builtin.mode == .Debug;
}

/// Utility to perform expensive checks only when assertions are enabled.
///
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
    // This should not panic
    assert(true, "This should not fail", .{});

    // Test with formatting
    const value = 42;
    assert(value == 42, "Expected 42, got {}", .{value});
}

test "assert_range functionality" {
    assert_range(50, 0, 100, "Value {} not in range 0-100", .{50});
    assert_range(0, 0, 100, "Value {} not in range 0-100", .{0});
    assert_range(100, 0, 100, "Value {} not in range 0-100", .{100});
}

test "assert_buffer_bounds functionality" {
    const buffer_len = 100;
    assert_buffer_bounds(0, 50, buffer_len, "Buffer overflow: {} + {} > {}", .{ 0, 50, buffer_len });
    assert_buffer_bounds(50, 50, buffer_len, "Buffer overflow: {} + {} > {}", .{ 50, 50, buffer_len });
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
