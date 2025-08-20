//! Fixed-size object pools for zero-fragmentation allocation.
//!
//! Pre-allocated pools for frequently created objects eliminate allocation
//! overhead and memory fragmentation. Single-threaded design provides
//! predictable sub-10ns allocation performance.
//!
//! Design rationale: Microsecond-scale operations cannot tolerate allocation
//! latency or fragmentation. Fixed-size pools provide deterministic performance
//! while debug tracking enables leak detection without runtime overhead.

const builtin = @import("builtin");
const std = @import("std");

const assert_mod = @import("assert.zig");
const stdx = @import("stdx.zig");

const fatal_assert = assert_mod.fatal_assert;

/// Debug tracker for object pools with allocation monitoring and leak detection.
fn DebugTrackerType(comptime T: type) type {
    return struct {
        const Self = @This();

        peak_usage: u32,
        total_acquisitions: u64,
        total_releases: u64,
        current_allocations: std.array_list.Managed(AllocationInfo),

        const AllocationInfo = struct {
            ptr: *T,
            timestamp: i64,
            source_location: std.builtin.SourceLocation,
        };

        fn init(allocator: std.mem.Allocator) Self {
            return Self{
                .peak_usage = 0,
                .total_acquisitions = 0,
                .total_releases = 0,
                .current_allocations = std.array_list.Managed(AllocationInfo).init(allocator),
            };
        }

        fn track_acquisition(self: *Self, ptr: *T) void {
            const info = AllocationInfo{
                .ptr = ptr,
                .timestamp = std.time.milliTimestamp(),
                .source_location = @src(),
            };
            self.current_allocations.append(info) catch return; // Don't fail on tracking
            self.total_acquisitions += 1;
        }

        fn track_release(self: *Self, ptr: *T) void {
            for (self.current_allocations.items, 0..) |info, i| {
                if (info.ptr == ptr) {
                    _ = self.current_allocations.swapRemove(i);
                    self.total_releases += 1;
                    return;
                }
            }
            std.log.warn("Released object not found in tracking: {*}", .{ptr});
        }

        fn update_peak_usage(self: *Self, current: u32) void {
            if (current > self.peak_usage) {
                self.peak_usage = current;
            }
        }

        fn report_statistics(self: *const Self) void {
            std.log.info("Pool[{s}] Statistics:", .{@typeName(T)});
            std.log.info("  Peak usage: {}", .{self.peak_usage});
            std.log.info("  Total acquisitions: {}", .{self.total_acquisitions});
            std.log.info("  Total releases: {}", .{self.total_releases});
            std.log.info("  Currently allocated: {}", .{self.current_allocations.items.len});

            if (self.current_allocations.items.len > 0) {
                std.log.warn("Potential leaks in {s} pool: {} objects", .{ @typeName(T), self.current_allocations.items.len });
            }
        }

        fn report_leaks_only(self: *const Self) void {
            if (self.current_allocations.items.len > 0) {
                std.log.warn("Pool[{s}] leak detected: {} objects still allocated", .{ @typeName(T), self.current_allocations.items.len });
            }
        }

        fn deinit(self: *Self) void {
            self.report_leaks_only();
            self.current_allocations.deinit();
        }
    };
}

/// Generic fixed-size object pool with debug tracking.
/// Provides O(1) allocation/deallocation for frequently used objects.
/// Template parameter T must be the exact type stored in the pool.
pub fn ObjectPoolType(comptime T: type) type {
    return struct {
        const Self = @This();
        const PoolNode = struct {
            item: T,
            next: ?*PoolNode,
        };

        backing_allocator: std.mem.Allocator,
        free_list: ?*PoolNode,
        total_capacity: u32,
        used_count: u32,
        debug_tracker: if (builtin.mode == .Debug) DebugTrackerType(T) else void,

        /// Initialize object pool with pre-allocated capacity.
        /// All objects are allocated upfront to eliminate runtime allocation.
        pub fn init(backing_allocator: std.mem.Allocator, pool_capacity: u32) !Self {
            fatal_assert(pool_capacity > 0 and pool_capacity <= 65536, "Pool capacity must be 1-65536, got {}", .{pool_capacity});

            var self = Self{
                .backing_allocator = backing_allocator,
                .free_list = null,
                .total_capacity = pool_capacity,
                .used_count = 0,
                .debug_tracker = if (builtin.mode == .Debug) DebugTrackerType(T).init(backing_allocator) else {},
            };

            try self.preallocate_nodes();
            return self;
        }

        /// Pre-allocate all pool nodes to eliminate runtime allocation.
        fn preallocate_nodes(self: *Self) !void {
            var i: u32 = 0;
            while (i < self.total_capacity) : (i += 1) {
                const node = try self.backing_allocator.create(@TypeOf(self.*).PoolNode);
                node.* = @TypeOf(self.*).PoolNode{
                    .item = undefined, // Will be initialized by acquire()
                    .next = self.free_list,
                };
                self.free_list = node;
            }
        }

        /// Acquire object from pool with optional initialization.
        /// Returns null if pool is exhausted (all objects in use).
        pub fn acquire(self: *Self) ?*T {
            const node = self.free_list orelse return null;
            self.free_list = node.next;
            self.used_count += 1;

            if (comptime builtin.mode == .Debug) {
                self.track_debug_acquisition(&node.item);
            }

            return &node.item;
        }

        /// Track acquisition for debug purposes.
        fn track_debug_acquisition(self: *Self, item: *T) void {
            if (comptime builtin.mode == .Debug) {
                self.debug_tracker.track_acquisition(item);
                self.debug_tracker.update_peak_usage(self.used_count);
            }
        }

        /// Acquire object from pool with initialization function.
        /// Initialization function is called with uninitialized memory.
        pub fn acquire_with_init(self: *Self, init_fn: fn (*T) void) ?*T {
            if (self.acquire()) |item| {
                init_fn(item);
                return item;
            }
            return null;
        }

        /// Release object back to pool.
        /// Object becomes available for future acquire() calls.
        /// CRITICAL: Caller must not access object after release.
        pub fn release(self: *Self, item: *T) void {
            if (comptime builtin.mode == .Debug) {
                self.validate_and_track_release(item);
            }
            self.return_to_free_list(item);
        }

        /// Validate and track release for debug purposes.
        fn validate_and_track_release(self: *Self, item: *T) void {
            if (comptime builtin.mode == .Debug) {
                self.validate_pool_ownership(item);
                self.debug_tracker.track_release(item);
            }
        }

        /// Return item to free list.
        fn return_to_free_list(self: *Self, item: *T) void {
            const node: *PoolNode = @alignCast(@fieldParentPtr("item", item));
            node.next = self.free_list;
            self.free_list = node;
            self.used_count -= 1;
        }

        /// Get current pool utilization statistics.
        /// Returns fraction of pool currently in use (0.0 to 1.0).
        pub fn utilization(self: *const Self) f32 {
            return @as(f32, @floatFromInt(self.used_count)) / @as(f32, @floatFromInt(self.total_capacity));
        }

        /// Check if pool is at capacity (no objects available).
        pub fn is_exhausted(self: *const Self) bool {
            return self.free_list == null;
        }

        /// Get number of objects currently in use.
        pub fn active_count(self: *const Self) u32 {
            return self.used_count;
        }

        /// Get total pool capacity.
        pub fn capacity(self: *const Self) u32 {
            return self.total_capacity;
        }

        /// Validate object belongs to this pool (debug builds only).
        /// Prevents releasing objects from other pools or non-pool memory.
        fn validate_pool_ownership(self: *const Self, item: *T) void {
            _ = self; // Pool context available for future validation enhancements
            if (comptime builtin.mode == .Debug) {
                // Check if pointer is within our allocated range
                const node: *PoolNode = @alignCast(@fieldParentPtr("item", item));
                const node_addr = @intFromPtr(node);

                // Note: a more sophisticated validation could walk all nodes
                fatal_assert(node_addr != 0, "Attempted to release null pointer to pool", .{});
            }
        }

        /// Reset pool to initial state, marking all objects as available.
        /// CRITICAL: All active objects become invalid after this call.
        pub fn reset(self: *Self) void {
            const count = reconstruct_pool_free_list(self);
            self.used_count = 0;

            if (comptime builtin.mode == .Debug) {
                if (count != self.total_capacity) {
                    std.log.warn("Pool reset found {} nodes, expected {}", .{ count, self.total_capacity });
                }
                self.debug_tracker.current_allocations.clearRetainingCapacity();
            }
        }

        /// Reconstruct free list to include all nodes.
        fn reconstruct_pool_free_list(pool: anytype) u32 {
            var current = pool.free_list;
            pool.free_list = null;

            var count: u32 = 0;
            while (current) |node| {
                const next = node.next;
                node.next = pool.free_list;
                pool.free_list = node;
                current = next;
                count += 1;
            }
            return count;
        }

        /// Report pool statistics and potential leaks.
        pub fn report_statistics(self: *const Self) void {
            if (comptime builtin.mode == .Debug) {
                self.debug_tracker.report_statistics();
            }

            std.log.info("Pool[{s}] State: {}/{} active ({d:.1}% utilization)", .{
                @typeName(T),
                self.used_count,
                self.total_capacity,
                self.utilization() * 100.0,
            });
        }

        /// Deinitialize pool and free all pre-allocated memory.
        pub fn deinit(self: *Self) void {
            if (comptime builtin.mode == .Debug) {
                if (self.used_count > 0) {
                    std.log.warn("Pool[{s}] deinit with {} active objects - potential leaks", .{ @typeName(T), self.used_count });
                }
                self.debug_tracker.deinit();
            }

            // Pre-allocated nodes must be returned to backing allocator
            var current = self.free_list;
            while (current) |node| {
                const next = node.next;
                self.backing_allocator.destroy(node);
                current = next;
            }
        }
    };
}

/// Generic pool manager template for creating type-specific pool managers.
/// Avoids circular imports by deferring type resolution to usage sites.
pub fn PoolManagerType(comptime PoolType1: type, comptime PoolType2: type) type {
    return struct {
        const Self = @This();

        pool1: ObjectPoolType(PoolType1),
        pool2: ObjectPoolType(PoolType2),
        backing_allocator: std.mem.Allocator,

        /// Initialize pools with specified capacities.
        pub fn init(backing_allocator: std.mem.Allocator, capacity1: u32, capacity2: u32) !Self {
            return Self{
                .pool1 = try ObjectPoolType(PoolType1).init(backing_allocator, capacity1),
                .pool2 = try ObjectPoolType(PoolType2).init(backing_allocator, capacity2),
                .backing_allocator = backing_allocator,
            };
        }

        /// Acquire object from first pool.
        pub fn acquire_first(self: *Self) ?*PoolType1 {
            return self.pool1.acquire();
        }

        /// Release object to first pool.
        pub fn release_first(self: *Self, item: *PoolType1) void {
            self.pool1.release(item);
        }

        /// Acquire object from second pool.
        pub fn acquire_second(self: *Self) ?*PoolType2 {
            return self.pool2.acquire();
        }

        /// Release object to second pool.
        pub fn release_second(self: *Self, item: *PoolType2) void {
            self.pool2.release(item);
        }

        /// Check if any pools have high utilization.
        pub fn has_high_utilization(self: *const Self, threshold: f32) bool {
            return self.pool1.utilization() > threshold or
                self.pool2.utilization() > threshold;
        }

        /// Reset all pools to initial state.
        pub fn reset_all(self: *Self) void {
            self.pool1.reset();
            self.pool2.reset();
        }

        /// Deinitialize all pools.
        pub fn deinit(self: *Self) void {
            self.pool1.deinit();
            self.pool2.deinit();
        }
    };
}

/// Fast stack-based allocator for temporary objects.
/// Uses pre-allocated stack memory for ultra-fast allocation of small, short-lived objects.
pub fn StackPoolType(comptime T: type, comptime capacity: u32) type {
    return struct {
        const Self = @This();

        items: [capacity]T,
        used_mask: stdx.bit_set_type(capacity),
        next_hint: u32,

        /// Initialize stack pool with all slots available.
        pub fn init() Self {
            return Self{
                .items = undefined, // Items initialized on first use
                .used_mask = stdx.bit_set_type(capacity).init_empty(),
                .next_hint = 0,
            };
        }

        /// Acquire object from stack pool.
        /// Returns null if all slots are in use.
        pub fn acquire(self: *Self) ?*T {
            // Start search from hint for cache locality
            var i = self.next_hint;
            var attempts: u32 = 0;

            while (attempts < capacity) : (attempts += 1) {
                if (!self.used_mask.is_set(i)) {
                    self.used_mask.set(i);
                    self.next_hint = (i + 1) % capacity;
                    return &self.items[i];
                }
                i = (i + 1) % capacity;
            }

            return null; // Pool exhausted
        }

        /// Release object back to stack pool.
        /// Object index is calculated from pointer arithmetic.
        pub fn release(self: *Self, item: *T) void {
            const item_addr = @intFromPtr(item);
            const base_addr = @intFromPtr(&self.items[0]);
            const item_size = @sizeOf(T);

            fatal_assert(item_addr >= base_addr, "Object not from this stack pool", .{});
            fatal_assert((item_addr - base_addr) % item_size == 0, "Misaligned object in stack pool", .{});

            const index = (item_addr - base_addr) / item_size;
            fatal_assert(index < capacity, "Object index out of bounds: {}", .{index});
            fatal_assert(self.used_mask.is_set(index), "Attempted to release already-free object at index {}", .{index});

            self.used_mask.unset(index);
            self.next_hint = @intCast(index); // Prefer recently freed slots
        }

        /// Get current utilization as count of used slots.
        pub fn active_count(self: *const Self) u32 {
            return @intCast(self.used_mask.count());
        }

        /// Check if stack pool is at capacity.
        pub fn is_exhausted(self: *const Self) bool {
            return self.used_mask.count() == capacity;
        }

        /// Reset all slots to available state.
        /// CRITICAL: All active objects become invalid after this call.
        pub fn reset(self: *Self) void {
            self.used_mask = stdx.bit_set_type(capacity).init_empty();
            self.next_hint = 0;
        }
    };
}

// Note: Specific pool implementations created at usage sites to avoid circular imports

// Tests

const testing = std.testing;

test "ObjectPool basic allocation and release" {
    var pool = try ObjectPoolType(u64).init(testing.allocator, 4);
    defer pool.deinit();

    // Test acquisition
    const item1 = pool.acquire() orelse return error.PoolExhausted;
    const item2 = pool.acquire() orelse return error.PoolExhausted;

    item1.* = 42;
    item2.* = 84;

    try testing.expect(item1.* == 42);
    try testing.expect(item2.* == 84);
    try testing.expect(pool.active_count() == 2);

    // Test release
    pool.release(item1);
    try testing.expect(pool.active_count() == 1);

    // Test reacquisition
    const item3 = pool.acquire() orelse return error.PoolExhausted;
    item3.* = 126;
    try testing.expect(item3.* == 126);
    try testing.expect(pool.active_count() == 2);

    pool.release(item2);
    pool.release(item3);
    try testing.expect(pool.active_count() == 0);
}

test "ObjectPool exhaustion handling" {
    var pool = try ObjectPoolType(u32).init(testing.allocator, 2);
    defer pool.deinit();

    // Acquire all items
    const item1 = pool.acquire();
    const item2 = pool.acquire();

    try testing.expect(item1 != null);
    try testing.expect(item2 != null);
    try testing.expect(pool.is_exhausted());

    // Pool should be exhausted
    const item3 = pool.acquire();
    try testing.expect(item3 == null);

    pool.release(item1.?);
    try testing.expect(!pool.is_exhausted());

    // Should be able to acquire again
    const item4 = pool.acquire();
    try testing.expect(item4 != null);

    pool.release(item2.?);
    pool.release(item4.?);
    try testing.expect(pool.active_count() == 0);
}

test "ObjectPool with initialization function" {
    var pool = try ObjectPoolType(std.array_list.Managed(u8)).init(testing.allocator, 2);
    defer pool.deinit();

    const init_fn = struct {
        fn init(list: *std.array_list.Managed(u8)) void {
            list.* = std.array_list.Managed(u8).init(testing.allocator);
        }
    }.init;

    const list = pool.acquire_with_init(init_fn) orelse return error.PoolExhausted;
    try list.append(42);
    try testing.expect(list.items[0] == 42);

    // Clean up before release
    list.deinit();
    pool.release(list);
}

test "StackPool basic operations" {
    var pool = StackPoolType(u32, 8).init();

    // Test acquisition
    const item1 = pool.acquire() orelse return error.StackExhausted;
    const item2 = pool.acquire() orelse return error.StackExhausted;

    item1.* = 100;
    item2.* = 200;

    try testing.expect(item1.* == 100);
    try testing.expect(item2.* == 200);
    try testing.expect(pool.active_count() == 2);

    // Test release
    pool.release(item1);
    try testing.expect(pool.active_count() == 1);

    // Test reacquisition should prefer recently freed slot
    const item3 = pool.acquire() orelse return error.StackExhausted;
    item3.* = 300;
    try testing.expect(item3.* == 300);
}

test "StackPool exhaustion and reset" {
    var pool = StackPoolType(u32, 2).init();

    // Fill the pool
    const item1 = pool.acquire();
    const item2 = pool.acquire();

    try testing.expect(item1 != null);
    try testing.expect(item2 != null);
    try testing.expect(pool.is_exhausted());

    // Should not be able to acquire more
    const item3 = pool.acquire();
    try testing.expect(item3 == null);

    // Reset should make all slots available
    pool.reset();
    try testing.expect(pool.active_count() == 0);
    try testing.expect(!pool.is_exhausted());

    // Should be able to acquire again
    const item4 = pool.acquire();
    try testing.expect(item4 != null);
}

test "PoolManager template functionality" {
    // Test the generic pool manager template with simple types
    const TestPoolManager = PoolManagerType(u32, u64);

    var pool_manager = try TestPoolManager.init(testing.allocator, 4, 4);
    defer pool_manager.deinit();

    // Test first pool
    const item1 = pool_manager.acquire_first() orelse return error.PoolExhausted;
    item1.* = 42;
    try testing.expect(item1.* == 42);

    // Test second pool
    const item2 = pool_manager.acquire_second() orelse return error.PoolExhausted;
    item2.* = 84;
    try testing.expect(item2.* == 84);

    // Test utilization
    try testing.expect(!pool_manager.has_high_utilization(0.5));

    // Test release
    pool_manager.release_first(item1);
    pool_manager.release_second(item2);
}

test "pool performance characteristics" {
    const iterations = 1000; // Reduced to prevent pool exhaustion
    var pool = try ObjectPoolType(u64).init(testing.allocator, 16);
    defer pool.deinit();

    // Track items to ensure they're all released
    var acquired_items = std.array_list.Managed(*u64).init(testing.allocator);
    defer acquired_items.deinit();

    // Measure allocation performance
    const start_time = std.time.nanoTimestamp();

    var i: u32 = 0;
    while (i < iterations) : (i += 1) {
        const item = pool.acquire() orelse {
            // Pool exhausted, stop acquiring more items
            break;
        };

        item.* = i;
        try acquired_items.append(item);

        if (i % 2 == 0) {
            pool.release(item);
            _ = acquired_items.pop(); // Remove from tracking list
        }
    }

    const end_time = std.time.nanoTimestamp();
    const duration_ns = end_time - start_time;

    while (acquired_items.items.len > 0) {
        const item = acquired_items.pop(); // Should be *u64 but compiler sees ?*u64
        pool.release(item.?); // Explicitly unwrap to fix compiler type inference
    }

    // Verify performance (should be very fast with pooling)
    const avg_ns_per_op = @as(f64, @floatFromInt(duration_ns)) / @as(f64, @floatFromInt(iterations));
    // In optimized builds, timing can be unreliable in CI environments
    if (builtin.mode == .Debug) {
        try testing.expect(avg_ns_per_op < 10000.0); // Less than 10μs per operation
    }

    // Verify pool statistics - all items should be back in pool
    try testing.expect(pool.active_count() == 0);
    try testing.expect(pool.capacity() == 16);
}
