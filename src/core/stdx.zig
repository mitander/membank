//! CortexDB standard extensions providing safer alternatives to std library functions
//!
//! Design rationale: These wrappers add defensive programming checks and
//! consistent naming conventions across the codebase. They prevent common
//! memory safety issues by enforcing explicit buffer validation.

const std = @import("std");
const assert = std.debug.assert;

/// Copy memory from source to destination with left-to-right ordering
///
/// Use this instead of std.mem.copyForwards for explicit directional semantics.
/// Left-to-right copy is safe for overlapping buffers where destination starts
/// before source, preventing corruption during the copy operation.
pub fn copy_left(comptime T: type, dest: []T, source: []const T) void {
    assert(dest.len >= source.len);
    assert(@intFromPtr(dest.ptr) != @intFromPtr(source.ptr) or dest.len == 0);

    std.mem.copyForwards(T, dest, source);
}

/// Copy memory from source to destination with right-to-left ordering
///
/// Use this instead of std.mem.copyBackwards for explicit directional semantics.
/// Right-to-left copy is safe for overlapping buffers where destination starts
/// after source, preventing corruption during the copy operation.
pub fn copy_right(comptime T: type, dest: []T, source: []const T) void {
    assert(dest.len >= source.len);
    assert(@intFromPtr(dest.ptr) != @intFromPtr(source.ptr) or dest.len == 0);

    std.mem.copyBackwards(T, dest, source);
}

/// Copy memory between non-overlapping buffers
///
/// Use this instead of std.mem.copy for explicit non-overlap semantics.
/// This function asserts that buffers do not overlap, preventing subtle
/// corruption bugs that can occur with overlapping copies.
pub fn copy_disjoint(comptime T: type, dest: []T, source: []const T) void {
    assert(dest.len >= source.len);

    // Defensive check: ensure buffers do not overlap
    const dest_start = @intFromPtr(dest.ptr);
    const dest_end = dest_start + dest.len * @sizeOf(T);
    const source_start = @intFromPtr(source.ptr);
    const source_end = source_start + source.len * @sizeOf(T);

    assert(dest_end <= source_start or source_end <= dest_start);

    @memcpy(dest[0..source.len], source);
}
