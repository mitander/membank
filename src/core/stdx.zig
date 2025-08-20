//! Standard library extensions with defensive programming checks.
//!
//! Provides safer alternatives to std library functions and thread-safe primitives
//! with explicit buffer validation and consistent error handling patterns.
//!
//! Design rationale: Wrappers prevent common memory safety issues while maintaining
//! zero-cost abstractions. Consistent naming and validation patterns reduce bugs
//! across subsystem boundaries.

const std = @import("std");

const assert_mod = @import("assert.zig");

const assert = assert_mod.assert;

/// Thread-safe metrics counter for tracking various statistics.
///
/// This provides atomic operations for incrementing, getting, and resetting
/// a counter value in a thread-safe manner.
pub const MetricsCounter = struct {
    value: std.atomic.Value(u64) = .{ .raw = 0 }, // tidy:ignore-arch - safe abstraction over atomics

    /// Initialize a new counter with an initial value.
    pub fn init(initial_value: u64) MetricsCounter {
        return .{ .value = .{ .raw = initial_value } };
    }

    /// Atomically increment the counter by the specified amount.
    pub fn add(self: *MetricsCounter, amount: u64) void {
        _ = self.value.fetchAdd(amount, .monotonic);
    }

    /// Atomically increment the counter by 1.
    pub fn incr(self: *MetricsCounter) void {
        _ = self.value.fetchAdd(1, .monotonic);
    }

    /// Load the current value of the counter atomically.
    pub fn load(self: *const MetricsCounter) u64 {
        return self.value.load(.monotonic);
    }

    /// Set the counter to a specific value.
    pub fn store(self: *MetricsCounter, new_value: u64) void {
        _ = self.value.swap(new_value, .monotonic);
    }

    /// Reset the counter to zero.
    pub fn reset(self: *MetricsCounter) void {
        _ = self.value.swap(0, .monotonic);
    }
};

/// Simple value container - no protection needed in single-threaded KausalDB.
/// This type exists for API consistency where thread-safety was once considered.
pub fn ProtectedType(comptime T: type) type {
    return struct {
        value: T,

        const Self = @This();

        /// Initialize a new value.
        pub fn init(value: T) Self {
            return .{ .value = value };
        }

        /// Access the value directly with a callback for API consistency.
        pub fn with(
            self: *Self,
            comptime F: type,
            context: anytype,
            func: F,
        ) @typeInfo(@TypeOf(func)).@"fn".return_type.? {
            const func_info = @typeInfo(@TypeOf(func)).@"fn";
            const Context = @TypeOf(context);
            if (func_info.params.len == 1) {
                return @call(.auto, func, .{&self.value});
            } else if (Context == void) {
                return @call(.auto, func, .{ &self.value, {} });
            } else {
                return @call(.auto, func, .{ &self.value, context });
            }
        }
    };
}

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

/// Copy memory with overlapping source and destination buffers
///
/// Use this for buffer compaction where source and destination overlap.
/// Specifically handles the case where destination starts before source,
/// which is safe with left-to-right copying semantics.
pub fn copy_overlapping(comptime T: type, dest: []T, source: []const T) void {
    assert(dest.len >= source.len);
    // Allow overlapping buffers - this is the key difference from copy_left
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

/// Safe wrapper around std.StaticBitSet with consistent naming conventions
///
/// Use this instead of std.StaticBitSet for consistent snake_case method names
/// and defensive programming checks. Provides the same functionality with
/// improved API consistency across the codebase.
pub fn bit_set_type(comptime size: comptime_int) type {
    return struct {
        inner: std.StaticBitSet(size),

        const Self = @This();

        pub fn init_empty() Self {
            return Self{ .inner = std.StaticBitSet(size).initEmpty() };
        }

        pub fn init_full() Self {
            return Self{ .inner = std.StaticBitSet(size).initFull() };
        }

        pub fn set(self: *Self, index: usize) void {
            assert(index < size);
            self.inner.set(index);
        }

        pub fn unset(self: *Self, index: usize) void {
            assert(index < size);
            self.inner.unset(index);
        }

        pub fn is_set(self: Self, index: usize) bool {
            assert(index < size);
            return self.inner.isSet(index);
        }

        pub fn toggle(self: *Self, index: usize) void {
            assert(index < size);
            self.inner.toggle(index);
        }

        pub fn count(self: Self) usize {
            return self.inner.count();
        }

        pub fn capacity(self: Self) usize {
            return self.inner.capacity();
        }
    };
}
