//! Assertion framework for KausalDB defensive programming.
//!
//! Provides runtime checks that help catch bugs early in development
//! while being compiled out in release builds for performance.

const builtin = @import("builtin");
const std = @import("std");

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

/// Re-export main thread assertion from concurrency module.
/// This provides a centralized assertion interface while keeping
/// concurrency logic in its dedicated module.
pub const assert_main_thread = @import("concurrency.zig").assert_main_thread;

/// Safe buffer slicing with bounds checking.
/// Prevents buffer overflows from malformed data or arithmetic errors.
/// # Examples
/// ```zig
/// const data = safe_slice(buffer, offset, size, "Reading block data") catch return error.Corruption;
/// ```
pub fn safe_slice(buffer: []const u8, offset: usize, size: usize, comptime operation: []const u8) ![]const u8 {
    fatal_assert(offset <= buffer.len, operation ++ ": slice offset {} beyond buffer length {}", .{ offset, buffer.len });

    fatal_assert(size <= buffer.len - offset, operation ++ ": slice size {} from offset {} exceeds buffer bounds (buffer: {})", .{ size, offset, buffer.len });

    // Check for arithmetic overflow in offset + size
    const end_pos = std.math.add(usize, offset, size) catch {
        fatal_assert(false, operation ++ ": arithmetic overflow in slice bounds {} + {}", .{ offset, size });
        unreachable;
    };

    fatal_assert(end_pos <= buffer.len, operation ++ ": computed end position {} exceeds buffer length {}", .{ end_pos, buffer.len });

    return buffer[offset .. offset + size];
}

/// Safe mutable buffer slicing with bounds checking.
/// # Examples
/// ```zig
/// var target = safe_slice_mut(output_buffer, write_pos, data.len, "Writing serialized data") catch return error.BufferTooSmall;
/// ```
pub fn safe_slice_mut(buffer: []u8, offset: usize, size: usize, comptime operation: []const u8) ![]u8 {
    fatal_assert(offset <= buffer.len, operation ++ ": slice offset {} beyond buffer length {}", .{ offset, buffer.len });

    fatal_assert(size <= buffer.len - offset, operation ++ ": slice size {} from offset {} exceeds buffer bounds (buffer: {})", .{ size, offset, buffer.len });

    // Check for arithmetic overflow in offset + size
    const end_pos = std.math.add(usize, offset, size) catch {
        fatal_assert(false, operation ++ ": arithmetic overflow in slice bounds {} + {}", .{ offset, size });
        unreachable;
    };

    fatal_assert(end_pos <= buffer.len, operation ++ ": computed end position {} exceeds buffer length {}", .{ end_pos, buffer.len });

    return buffer[offset .. offset + size];
}

/// Safe buffer copy with bounds validation.
/// Prevents buffer overflows during memory operations.
/// # Examples
/// ```zig
/// safe_copy(destination, src_data, "Copying block content");
/// ```
pub fn safe_copy(dest: []u8, src: []const u8, comptime operation: []const u8) void {
    fatal_assert(dest.len >= src.len, operation ++ ": destination buffer too small: {} < {} bytes", .{ dest.len, src.len });

    // Detect overlapping memory regions that would cause corruption
    const dest_start = @intFromPtr(dest.ptr);
    const dest_end = dest_start + dest.len;
    const src_start = @intFromPtr(src.ptr);
    const src_end = src_start + src.len;

    const overlaps = (src_start < dest_end) and (dest_start < src_end);
    fatal_assert(!overlaps, operation ++ ": overlapping memory regions detected - would cause corruption", .{});

    @memcpy(dest[0..src.len], src);
}

/// Validate buffer alignment for performance-critical operations.
/// # Examples
/// ```zig
/// assert_aligned(header_buffer, 64, "SSTable header must be cache-line aligned");
/// ```
pub fn assert_aligned(buffer: []const u8, alignment: usize, comptime operation: []const u8) void {
    const addr = @intFromPtr(buffer.ptr);
    fatal_assert(addr % alignment == 0, operation ++ ": buffer not aligned to {} bytes (address: 0x{x})", .{ alignment, addr });
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

test "safe buffer operations" {
    const test_data = [_]u8{ 1, 2, 3, 4, 5, 6, 7, 8 };

    // Test safe_slice with valid bounds
    const slice1 = try safe_slice(&test_data, 0, 4, "test slice");
    try std.testing.expectEqualSlices(u8, &[_]u8{ 1, 2, 3, 4 }, slice1);

    const slice2 = try safe_slice(&test_data, 2, 3, "test slice");
    try std.testing.expectEqualSlices(u8, &[_]u8{ 3, 4, 5 }, slice2);

    // Test safe_slice_mut
    var mutable_data = [_]u8{ 10, 20, 30, 40, 50 };
    const mut_slice = try safe_slice_mut(&mutable_data, 1, 3, "test mutable slice");
    try std.testing.expectEqualSlices(u8, &[_]u8{ 20, 30, 40 }, mut_slice);

    // Test safe_copy
    var dest = [_]u8{ 0, 0, 0, 0 };
    const src = [_]u8{ 100, 101, 102, 103 };
    safe_copy(&dest, &src, "test copy");
    try std.testing.expectEqualSlices(u8, &src, &dest);

    // Test assert_aligned with properly aligned buffer
    const aligned_data align(64) = [_]u8{ 1, 2, 3, 4 };
    assert_aligned(&aligned_data, 64, "test alignment");
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

    // This test validates our fix to P0.2: Assertion Framework Inconsistency
    // The implementation now matches the documentation:
    // - assert() and assert_fmt() are no-ops in release builds
    // - fatal_assert() always panics on false conditions
}
