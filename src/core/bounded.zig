//! Compile-time bounded collections for type-safe resource limits.
//!
//! Design rationale: Provides collections with compile-time maximum sizes to prevent
//! runtime buffer overflows and unbounded memory growth. All bounds are validated
//! at compile time where possible, with runtime checks only for dynamic indices.
//!
//! The bounded collections integrate with the TypedArena system and provide zero-cost
//! abstractions in release builds while maintaining comprehensive validation in debug builds.

const builtin = @import("builtin");
const std = @import("std");

const assert_mod = @import("assert.zig");
const stdx = @import("stdx.zig");

const assert_fmt = assert_mod.assert_fmt;
const fatal_assert = assert_mod.fatal_assert;

/// Fixed-size array with compile-time maximum enforcement.
/// Prevents buffer overflows and provides O(1) append operations
/// up to the compile-time maximum size.
///
/// # Type Parameters
/// - `T`: Element type
/// - `max_size`: Compile-time maximum number of elements
///
/// # Design Principles
/// - Compile-time bounds prevent overflow
/// - Zero heap allocations (stack-allocated array)
/// - O(1) append, get, and clear operations
/// - Debug-mode validation for all operations
pub fn BoundedArrayType(
    comptime T: type,
    comptime max_size: usize,
) type {
    if (max_size == 0) {
        @compileError("BoundedArrayType max_size must be greater than 0");
    }
    if (max_size > 1048576) {
        @compileError("BoundedArrayType max_size too large: " ++ std.fmt.comptimePrint("{}", .{max_size}) ++ " (max: 1048576)");
    }

    return struct {
        const BoundedArray = @This();
        items: [max_size]T = undefined,
        len: usize = 0,

        const MAX_SIZE = max_size;

        /// Append element to the array.
        /// Returns error.Overflow if array is full.
        pub fn append(self: *BoundedArray, item: T) !void {
            if (self.len >= MAX_SIZE) {
                return error.Overflow;
            }

            if (builtin.mode == .Debug) {
                assert_fmt(self.len < MAX_SIZE, "BoundedArrayType append overflow: len={}, max={}", .{ self.len, MAX_SIZE });
            }

            self.items[self.len] = item;
            self.len += 1;
        }

        /// Get element at index, returns null if index out of bounds.
        pub fn get(self: *const BoundedArray, index: usize) ?T {
            if (index >= self.len) {
                return null;
            }
            return self.items[index];
        }

        /// Query mutable reference to element at index.
        /// Returns null if index out of bounds.
        pub fn query_mut(self: *BoundedArray, index: usize) ?*T {
            if (index >= self.len) {
                return null;
            }
            return &self.items[index];
        }

        /// Get element at index with bounds checking.
        /// Panics in debug mode if index out of bounds.
        pub fn at(self: *const BoundedArray, index: usize) T {
            if (builtin.mode == .Debug) {
                assert_fmt(index < self.len, "BoundedArrayType index out of bounds: {} >= {}", .{ index, self.len });
            }
            fatal_assert(index < self.len, "BoundedArrayType index out of bounds: {} >= {}", .{ index, self.len });
            return self.items[index];
        }

        /// Get mutable reference with bounds checking.
        /// Panics in debug mode if index out of bounds.
        pub fn at_mut(self: *BoundedArray, index: usize) *T {
            if (builtin.mode == .Debug) {
                assert_fmt(index < self.len, "BoundedArrayType index out of bounds: {} >= {}", .{ index, self.len });
            }
            fatal_assert(index < self.len, "BoundedArrayType index out of bounds: {} >= {}", .{ index, self.len });
            return &self.items[index];
        }

        /// Remove and return the last element.
        /// Returns null if array is empty.
        pub fn pop(self: *BoundedArray) ?T {
            if (self.len == 0) {
                return null;
            }
            self.len -= 1;
            return self.items[self.len];
        }

        /// Remove element at index, shifting remaining elements left.
        /// Returns error.IndexOutOfBounds if index >= len.
        pub fn remove_at(self: *BoundedArray, index: usize) !T {
            if (index >= self.len) {
                return error.IndexOutOfBounds;
            }

            const item = self.items[index];

            // Shift elements left
            if (index < self.len - 1) {
                stdx.copy_left(T, self.items[index .. self.len - 1], self.items[index + 1 .. self.len]);
            }

            self.len -= 1;
            return item;
        }

        /// Clear all elements, setting length to 0.
        /// Does not modify the underlying array data.
        pub fn clear(self: *BoundedArray) void {
            self.len = 0;
        }

        /// Get slice view of valid elements.
        /// The slice is valid until the next mutation operation.
        pub fn slice(self: *const BoundedArray) []const T {
            return self.items[0..self.len];
        }

        /// Get mutable slice view of valid elements.
        /// The slice is valid until the next mutation operation.
        pub fn slice_mut(self: *BoundedArray) []T {
            return self.items[0..self.len];
        }

        /// Check if array is empty.
        pub fn is_empty(self: *const BoundedArray) bool {
            return self.len == 0;
        }

        /// Check if array is full.
        pub fn is_full(self: *const BoundedArray) bool {
            return self.len == MAX_SIZE;
        }

        /// Get current length.
        pub fn length(self: *const BoundedArray) usize {
            return self.len;
        }

        /// Get compile-time maximum size.
        pub fn max_length() usize {
            return MAX_SIZE;
        }

        /// Get remaining capacity.
        pub fn remaining_capacity(self: *const BoundedArray) usize {
            return MAX_SIZE - self.len;
        }

        /// Try to reserve capacity for n additional elements.
        /// Returns error.Overflow if not enough space.
        pub fn try_reserve(self: *const BoundedArray, additional: usize) !void {
            if (self.len + additional > MAX_SIZE) {
                return error.Overflow;
            }
        }

        /// Find first index of item, returns null if not found.
        pub fn find_index(self: *const BoundedArray, item: T) ?usize {
            for (self.items[0..self.len], 0..) |existing, i| {
                if (std.meta.eql(existing, item)) {
                    return i;
                }
            }
            return null;
        }

        /// Check if array contains item.
        pub fn contains(self: *const BoundedArray, item: T) bool {
            return self.find_index(item) != null;
        }

        /// Copy all elements from another BoundedArrayType.
        /// Returns error.Overflow if source array is too large.
        pub fn copy_from(self: *BoundedArray, other: *const BoundedArray) !void {
            if (other.len > MAX_SIZE) {
                return error.Overflow;
            }

            self.len = other.len;
            stdx.copy_left(T, self.items[0..self.len], other.items[0..other.len]);
        }

        /// Extend array with elements from slice.
        /// Returns error.Overflow if not enough space.
        pub fn extend_from_slice(self: *BoundedArray, items: []const T) !void {
            if (self.len + items.len > MAX_SIZE) {
                return error.Overflow;
            }

            stdx.copy_left(T, self.items[self.len .. self.len + items.len], items);
            self.len += items.len;
        }

        /// Iterator for iterating over elements.
        pub const Iterator = struct {
            array: *const BoundedArray,
            index: usize = 0,

            pub fn next(self: *Iterator) ?T {
                if (self.index >= self.array.len) {
                    return null;
                }
                defer self.index += 1;
                return self.array.items[self.index];
            }

            pub fn reset(self: *Iterator) void {
                self.index = 0;
            }
        };

        /// Get iterator for read-only iteration.
        pub fn iterator(self: *const BoundedArray) Iterator {
            return Iterator{ .array = self };
        }
    };
}

/// Fixed-size hash map with compile-time bounds.
/// Uses linear probing for collision resolution with compile-time maximum capacity.
pub fn BoundedHashMapType(comptime K: type, comptime V: type, comptime max_size: usize) type {
    if (max_size == 0) {
        @compileError("BoundedHashMapType max_size must be greater than 0");
    }
    if (max_size > 32768) {
        @compileError("BoundedHashMapType max_size too large: " ++ std.fmt.comptimePrint("{}", .{max_size}) ++ " (max: 32768)");
    }

    // Hash table size is next power of 2 >= max_size * 1.5 for good performance
    const table_size = std.math.ceilPowerOfTwoAssert(usize, max_size + max_size / 2);

    return struct {
        const BoundedHashMap = @This();
        entries: [table_size]Entry = [_]Entry{Entry.empty} ** table_size,
        len: usize = 0,

        const MAX_SIZE = max_size;
        const TABLE_SIZE = table_size;

        const Entry = union(enum) {
            empty: void,
            occupied: struct { key: K, value: V },
            deleted: void,
        };

        /// Insert key-value pair.
        /// Returns error.Overflow if map is full.
        pub fn put(self: *BoundedHashMap, key: K, value: V) !void {
            if (self.len >= MAX_SIZE) {
                return error.Overflow;
            }

            const hash = self.hash_key(key);
            var index = hash % TABLE_SIZE;

            // Linear probing
            while (true) {
                switch (self.entries[index]) {
                    .empty, .deleted => {
                        self.entries[index] = Entry{ .occupied = .{ .key = key, .value = value } };
                        self.len += 1;
                        return;
                    },
                    .occupied => |*occupied| {
                        if (std.meta.eql(occupied.key, key)) {
                            // Update existing
                            occupied.value = value;
                            return;
                        }
                        // Continue probing
                        index = (index + 1) % TABLE_SIZE;
                    },
                }
            }
        }

        /// Get value for key, returns null if not found.
        pub fn get(self: *const BoundedHashMap, key: K) ?V {
            const hash = self.hash_key(key);
            var index = hash % TABLE_SIZE;

            // Linear probing
            var probes: usize = 0;
            while (probes < TABLE_SIZE) {
                switch (self.entries[index]) {
                    .empty => return null,
                    .deleted => {
                        // Continue probing
                    },
                    .occupied => |occupied| {
                        if (std.meta.eql(occupied.key, key)) {
                            return occupied.value;
                        }
                    },
                }
                index = (index + 1) % TABLE_SIZE;
                probes += 1;
            }
            return null;
        }

        /// Remove key from map.
        /// Returns true if key was found and removed.
        pub fn remove(self: *BoundedHashMap, key: K) bool {
            const hash = self.hash_key(key);
            var index = hash % TABLE_SIZE;

            // Linear probing
            var probes: usize = 0;
            while (probes < TABLE_SIZE) {
                switch (self.entries[index]) {
                    .empty => return false,
                    .deleted => {
                        // Continue probing
                    },
                    .occupied => |occupied| {
                        if (std.meta.eql(occupied.key, key)) {
                            self.entries[index] = Entry.deleted;
                            self.len -= 1;
                            return true;
                        }
                    },
                }
                index = (index + 1) % TABLE_SIZE;
                probes += 1;
            }
            return false;
        }

        /// Clear all entries.
        pub fn clear(self: *BoundedHashMap) void {
            self.entries = [_]Entry{Entry.empty} ** TABLE_SIZE;
            self.len = 0;
        }

        /// Get current number of entries.
        pub fn length(self: *const BoundedHashMap) usize {
            return self.len;
        }

        /// Check if map is empty.
        pub fn is_empty(self: *const BoundedHashMap) bool {
            return self.len == 0;
        }

        /// Check if map is full.
        pub fn is_full(self: *const BoundedHashMap) bool {
            return self.len >= MAX_SIZE;
        }

        /// Query current load factor as ratio of items to capacity.
        /// Used for performance monitoring and resize decisions.
        pub fn query_load_factor(self: *const BoundedHashMap) f32 {
            return @as(f32, @floatFromInt(self.len)) / @as(f32, @floatFromInt(TABLE_SIZE));
        }

        /// Generate hash value for key using auto hash function.
        /// Used internally for bucket selection in hash table.
        fn hash_key(self: *const BoundedHashMap, key: K) u64 {
            _ = self;
            return std.hash_map.getAutoHashFn(K, void)({}, key);
        }

        /// Iterator for key-value pairs.
        pub const Iterator = struct {
            map: *const BoundedHashMap,
            index: usize = 0,

            pub const IteratorEntry = struct { key: K, value: V };

            /// Advance iterator to next key-value pair.
            /// Returns null when iteration is complete.
            pub fn next(self: *Iterator) ?IteratorEntry {
                while (self.index < TABLE_SIZE) {
                    defer self.index += 1;
                    switch (self.map.entries[self.index]) {
                        .occupied => |occupied| {
                            return IteratorEntry{ .key = occupied.key, .value = occupied.value };
                        },
                        else => continue,
                    }
                }
                return null;
            }
        };

        /// Get iterator over all key-value pairs.
        pub fn iterator(self: *const BoundedHashMap) Iterator {
            return Iterator{ .map = self };
        }
    };
}

/// Fixed-size queue with compile-time bounds.
/// FIFO queue with O(1) enqueue and dequeue operations.
pub fn BoundedQueueType(comptime T: type, comptime max_size: usize) type {
    if (max_size == 0) {
        @compileError("BoundedQueueType max_size must be greater than 0");
    }
    if (max_size > 65536) {
        @compileError("BoundedQueueType max_size too large: " ++ std.fmt.comptimePrint("{}", .{max_size}) ++ " (max: 65536)");
    }

    return struct {
        const BoundedQueue = @This();
        items: [max_size]T = undefined,
        head: usize = 0,
        tail: usize = 0,
        len: usize = 0,

        const MAX_SIZE = max_size;

        /// Add element to back of queue.
        /// Returns error.Overflow if queue is full.
        pub fn enqueue(self: *BoundedQueue, item: T) !void {
            if (self.len >= MAX_SIZE) {
                return error.Overflow;
            }

            self.items[self.tail] = item;
            self.tail = (self.tail + 1) % MAX_SIZE;
            self.len += 1;
        }

        /// Remove and return element from front of queue.
        /// Returns null if queue is empty.
        pub fn dequeue(self: *BoundedQueue) ?T {
            if (self.len == 0) {
                return null;
            }

            const item = self.items[self.head];
            self.head = (self.head + 1) % MAX_SIZE;
            self.len -= 1;
            return item;
        }

        /// Peek at front element without removing it.
        /// Returns null if queue is empty.
        pub fn peek(self: *const BoundedQueue) ?T {
            if (self.len == 0) {
                return null;
            }
            return self.items[self.head];
        }

        /// Peek at back element without removing it.
        /// Returns null if queue is empty.
        pub fn peek_back(self: *const BoundedQueue) ?T {
            if (self.len == 0) {
                return null;
            }
            const back_index = if (self.tail == 0) MAX_SIZE - 1 else self.tail - 1;
            return self.items[back_index];
        }

        /// Clear all elements.
        pub fn clear(self: *BoundedQueue) void {
            self.head = 0;
            self.tail = 0;
            self.len = 0;
        }

        /// Get current number of elements.
        pub fn length(self: *const BoundedQueue) usize {
            return self.len;
        }

        /// Check if queue is empty.
        pub fn is_empty(self: *const BoundedQueue) bool {
            return self.len == 0;
        }

        /// Check if queue is full.
        pub fn is_full(self: *const BoundedQueue) bool {
            return self.len == MAX_SIZE;
        }

        /// Get remaining capacity.
        pub fn remaining_capacity(self: *const BoundedQueue) usize {
            return MAX_SIZE - self.len;
        }
    };
}

/// Compile-time validation for bounded collection usage.
/// Ensures collections are properly sized for their use case.
pub fn validate_bounded_usage(comptime T: type, comptime max_size: usize, comptime usage_context: []const u8) void {
    // Validate reasonable size limits
    if (max_size > 65536) {
        @compileError("Bounded collection too large in " ++ usage_context ++ ": " ++ std.fmt.comptimePrint("{}", .{max_size}) ++ " (consider using dynamic allocation)");
    }

    // Validate type size is reasonable
    const item_size = @sizeOf(T);
    const total_size = item_size * max_size;
    if (total_size > 1024 * 1024) { // 1MB stack allocation limit
        @compileError("Bounded collection memory usage too large in " ++ usage_context ++ ": " ++ std.fmt.comptimePrint("{} bytes", .{total_size}) ++ " (consider reducing max_size or using heap allocation)");
    }
}

// Compile-time validation
comptime {
    // Validate that our bounded collections work with basic types
    const TestArray = BoundedArrayType(u32, 10);
    const TestQueue = BoundedQueueType(u8, 20);
    const TestMap = BoundedHashMapType(u32, []const u8, 16);

    assert_mod.comptime_assert(@sizeOf(TestArray) > 0, "BoundedArrayType must have non-zero size");
    assert_mod.comptime_assert(@sizeOf(TestQueue) > 0, "BoundedQueueType must have non-zero size");
    assert_mod.comptime_assert(@sizeOf(TestMap) > 0, "BoundedHashMapType must have non-zero size");
}

// Tests

test "BoundedArrayType basic operations" {
    var array = BoundedArrayType(u32, 5){};

    // Test append
    try array.append(1);
    try array.append(2);
    try array.append(3);

    try std.testing.expect(array.length() == 3);
    try std.testing.expect(!array.is_empty());
    try std.testing.expect(!array.is_full());

    // Test get
    try std.testing.expect(array.get(0) == 1);
    try std.testing.expect(array.get(1) == 2);
    try std.testing.expect(array.get(2) == 3);
    try std.testing.expect(array.get(3) == null);

    // Test at
    try std.testing.expect(array.at(0) == 1);
    try std.testing.expect(array.at(2) == 3);
}

test "BoundedArrayType overflow behavior" {
    var array = BoundedArrayType(u8, 3){};

    // Fill to capacity
    try array.append(1);
    try array.append(2);
    try array.append(3);

    try std.testing.expect(array.is_full());

    // Should overflow
    try std.testing.expectError(error.Overflow, array.append(4));
}

test "BoundedArrayType slice operations" {
    var array = BoundedArrayType(u32, 10){};

    try array.append(10);
    try array.append(20);
    try array.append(30);

    const slice = array.slice();
    try std.testing.expect(slice.len == 3);
    try std.testing.expect(slice[0] == 10);
    try std.testing.expect(slice[1] == 20);
    try std.testing.expect(slice[2] == 30);

    // Mutable slice
    const mut_slice = array.slice_mut();
    mut_slice[1] = 25;
    try std.testing.expect(array.at(1) == 25);
}

test "BoundedArrayType remove operations" {
    var array = BoundedArrayType(u32, 5){};

    try array.append(1);
    try array.append(2);
    try array.append(3);
    try array.append(4);

    const removed = try array.remove_at(1);
    try std.testing.expect(removed == 2);
    try std.testing.expect(array.length() == 3);
    try std.testing.expect(array.at(0) == 1);
    try std.testing.expect(array.at(1) == 3); // Shifted left
    try std.testing.expect(array.at(2) == 4);

    // Pop from back
    const popped = array.pop();
    try std.testing.expect(popped == 4);
    try std.testing.expect(array.length() == 2);
}

test "BoundedArrayType search operations" {
    var array = BoundedArrayType([]const u8, 5){};

    try array.append("hello");
    try array.append("world");
    try array.append("test");

    try std.testing.expect(array.find_index("world") == 1);
    try std.testing.expect(array.find_index("missing") == null);
    try std.testing.expect(array.contains("test"));
    try std.testing.expect(!array.contains("missing"));
}

test "BoundedQueueType basic operations" {
    var queue = BoundedQueueType(u32, 4){};

    // Test enqueue
    try queue.enqueue(1);
    try queue.enqueue(2);
    try queue.enqueue(3);

    try std.testing.expect(queue.length() == 3);
    try std.testing.expect(!queue.is_empty());
    try std.testing.expect(!queue.is_full());

    // Test peek
    try std.testing.expect(queue.peek() == 1);
    try std.testing.expect(queue.peek_back() == 3);

    // Test dequeue
    try std.testing.expect(queue.dequeue() == 1);
    try std.testing.expect(queue.dequeue() == 2);
    try std.testing.expect(queue.length() == 1);

    try std.testing.expect(queue.peek() == 3);
}

test "BoundedQueueType wrap-around" {
    var queue = BoundedQueueType(u8, 3){};

    // Fill queue
    try queue.enqueue(1);
    try queue.enqueue(2);
    try queue.enqueue(3);

    try std.testing.expect(queue.dequeue() == 1);
    try queue.enqueue(4);

    // Check order is preserved
    try std.testing.expect(queue.dequeue() == 2);
    try std.testing.expect(queue.dequeue() == 3);
    try std.testing.expect(queue.dequeue() == 4);
    try std.testing.expect(queue.is_empty());
}

test "BoundedHashMapType basic operations" {
    var map = BoundedHashMapType(u32, []const u8, 8){};

    // Test put
    try map.put(1, "one");
    try map.put(2, "two");
    try map.put(3, "three");

    try std.testing.expect(map.length() == 3);

    // Test get
    try std.testing.expectEqualStrings("one", map.get(1).?);
    try std.testing.expectEqualStrings("two", map.get(2).?);
    try std.testing.expectEqualStrings("three", map.get(3).?);
    try std.testing.expect(map.get(999) == null);

    // Test update
    try map.put(2, "TWO");
    try std.testing.expectEqualStrings("TWO", map.get(2).?);
    try std.testing.expect(map.length() == 3); // Should not increase
}

test "BoundedHashMapType remove operations" {
    var map = BoundedHashMapType(u32, u32, 8){};

    try map.put(1, 10);
    try map.put(2, 20);
    try map.put(3, 30);

    try std.testing.expect(map.remove(2));
    try std.testing.expect(map.length() == 2);
    try std.testing.expect(map.get(2) == null);
    try std.testing.expect(map.get(1) == 10); // Others remain

    try std.testing.expect(!map.remove(999));
    try std.testing.expect(map.length() == 2);
}

test "BoundedHashMapType iterator" {
    var map = BoundedHashMapType(u8, u8, 8){};

    try map.put(1, 10);
    try map.put(2, 20);
    try map.put(3, 30);

    var iter = map.iterator();
    var count: usize = 0;
    var sum: u8 = 0;

    while (iter.next()) |entry| {
        count += 1;
        sum += entry.value;
    }

    try std.testing.expect(count == 3);
    try std.testing.expect(sum == 60); // 10 + 20 + 30
}

test "compile-time validation catches oversized collections" {
    // These would fail at compile time if uncommented:
    // const TooLarge = BoundedArrayType(u8, 100000); // Too large
    // const ZeroSize = BoundedArrayType(u8, 0); // Zero size not allowed

    // This should pass
    const Reasonable = BoundedArrayType(u8, 100);
    try std.testing.expect(Reasonable.max_length() == 100);
}

test "BoundedArrayType extend and copy operations" {
    var array = BoundedArrayType(u32, 10){};

    // Test extend from slice
    const data = [_]u32{ 1, 2, 3 };
    try array.extend_from_slice(&data);
    try std.testing.expect(array.length() == 3);
    try std.testing.expect(array.at(0) == 1);
    try std.testing.expect(array.at(2) == 3);

    // Test copy from another array
    var other = BoundedArrayType(u32, 10){};
    try other.copy_from(&array);
    try std.testing.expect(other.length() == 3);
    try std.testing.expect(other.at(1) == 2);

    // Test overflow on extend
    const big_data = [_]u32{ 4, 5, 6, 7, 8, 9, 10, 11 }; // 8 more items, total would be 11
    try std.testing.expectError(error.Overflow, array.extend_from_slice(&big_data));
}

test "BoundedArrayType iterator functionality" {
    var array = BoundedArrayType(u32, 5){};

    try array.append(10);
    try array.append(20);
    try array.append(30);

    var iter = array.iterator();
    try std.testing.expect(iter.next() == 10);
    try std.testing.expect(iter.next() == 20);
    try std.testing.expect(iter.next() == 30);
    try std.testing.expect(iter.next() == null);

    // Reset and iterate again
    iter.reset();
    try std.testing.expect(iter.next() == 10);
}
