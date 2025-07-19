//! Static buffer pools for zero-allocation hot paths (TigerBeetle approach).
//!
//! Pre-allocates all buffers at startup with no dynamic allocation during runtime.
//! Provides fixed-size buffer pools for common allocation patterns in storage
//! and WAL operations.

const std = @import("std");
const assert = std.debug.assert;

/// Maximum buffer size supported by the pool
pub const MAX_BUFFER_SIZE = 1024 * 1024; // 1MB

/// Number of buffers per pool size class
const BUFFERS_PER_CLASS = 8;

/// Buffer size classes (powers of 2 for efficient allocation)
const BufferSizeClass = enum(u8) {
    tiny = 0, // 256 bytes
    small = 1, // 1KB
    medium = 2, // 4KB
    large = 3, // 16KB
    huge = 4, // 64KB
    massive = 5, // 256KB

    pub fn size(self: BufferSizeClass) usize {
        return switch (self) {
            .tiny => 256,
            .small => 1024,
            .medium => 4096,
            .large => 16384,
            .huge => 65536,
            .massive => 262144,
        };
    }

    pub fn from_size(requested_size: usize) BufferSizeClass {
        if (requested_size <= 256) return .tiny;
        if (requested_size <= 1024) return .small;
        if (requested_size <= 4096) return .medium;
        if (requested_size <= 16384) return .large;
        if (requested_size <= 65536) return .huge;
        if (requested_size <= 262144) return .massive;

        // Anything larger than 256KB is not supported by the pool
        unreachable;
    }
};

/// A buffer acquired from the pool
pub const PooledBuffer = struct {
    data: []u8,
    size_class: BufferSizeClass,
    pool: *BufferPool,

    pub fn release(self: PooledBuffer) void {
        self.pool.return_buffer(self);
    }

    pub fn slice(self: PooledBuffer, len: usize) []u8 {
        assert(len <= self.data.len);
        return self.data[0..len];
    }
};

/// Static buffer pool with pre-allocated buffers
pub const BufferPool = struct {
    // Pre-allocated buffer storage for each size class
    tiny_buffers: [BUFFERS_PER_CLASS][BufferSizeClass.tiny.size()]u8,
    small_buffers: [BUFFERS_PER_CLASS][BufferSizeClass.small.size()]u8,
    medium_buffers: [BUFFERS_PER_CLASS][BufferSizeClass.medium.size()]u8,
    large_buffers: [BUFFERS_PER_CLASS][BufferSizeClass.large.size()]u8,
    huge_buffers: [BUFFERS_PER_CLASS][BufferSizeClass.huge.size()]u8,
    massive_buffers: [BUFFERS_PER_CLASS][BufferSizeClass.massive.size()]u8,

    // Free masks - bit i set means buffer i is free
    tiny_free: std.atomic.Value(u8),
    small_free: std.atomic.Value(u8),
    medium_free: std.atomic.Value(u8),
    large_free: std.atomic.Value(u8),
    huge_free: std.atomic.Value(u8),
    massive_free: std.atomic.Value(u8),

    // Statistics for monitoring
    stats: Statistics,

    const Statistics = struct {
        allocations: std.atomic.Value(u64),
        deallocations: std.atomic.Value(u64),
        pool_hits: std.atomic.Value(u64),
        pool_misses: std.atomic.Value(u64),

        pub fn init() Statistics {
            return Statistics{
                .allocations = std.atomic.Value(u64).init(0),
                .deallocations = std.atomic.Value(u64).init(0),
                .pool_hits = std.atomic.Value(u64).init(0),
                .pool_misses = std.atomic.Value(u64).init(0),
            };
        }
    };

    pub fn init(allocator: std.mem.Allocator) !BufferPool {
        _ = allocator; // Not used - we pre-allocate everything statically

        return BufferPool{
            .tiny_buffers = undefined,
            .small_buffers = undefined,
            .medium_buffers = undefined,
            .large_buffers = undefined,
            .huge_buffers = undefined,
            .massive_buffers = undefined,

            // All buffers start as free (all bits set)
            .tiny_free = std.atomic.Value(u8).init(std.math.maxInt(u8)),
            .small_free = std.atomic.Value(u8).init(std.math.maxInt(u8)),
            .medium_free = std.atomic.Value(u8).init(std.math.maxInt(u8)),
            .large_free = std.atomic.Value(u8).init(std.math.maxInt(u8)),
            .huge_free = std.atomic.Value(u8).init(std.math.maxInt(u8)),
            .massive_free = std.atomic.Value(u8).init(std.math.maxInt(u8)),

            .stats = Statistics.init(),
        };
    }

    pub fn deinit(self: *BufferPool) void {
        // Nothing to clean up - all memory is statically allocated
        _ = self;
    }

    /// Get a buffer from the pool (zero-allocation fast path)
    pub fn acquire_buffer(self: *BufferPool, size: usize) ?PooledBuffer {
        if (size > MAX_BUFFER_SIZE) return null;

        const size_class = BufferSizeClass.from_size(size);
        _ = self.stats.allocations.fetchAdd(1, .monotonic);

        const buffer_data = self.allocate_from_class(size_class) orelse {
            _ = self.stats.pool_misses.fetchAdd(1, .monotonic);
            return null;
        };

        _ = self.stats.pool_hits.fetchAdd(1, .monotonic);
        return PooledBuffer{
            .data = buffer_data,
            .size_class = size_class,
            .pool = self,
        };
    }

    /// Return buffer to pool for reuse
    pub fn return_buffer(self: *BufferPool, buffer: PooledBuffer) void {
        _ = self.stats.deallocations.fetchAdd(1, .monotonic);
        self.free_to_class(buffer.size_class, buffer.data);
    }

    /// Allocate from specific size class
    fn allocate_from_class(self: *BufferPool, size_class: BufferSizeClass) ?[]u8 {
        const free_mask = switch (size_class) {
            .tiny => &self.tiny_free,
            .small => &self.small_free,
            .medium => &self.medium_free,
            .large => &self.large_free,
            .huge => &self.huge_free,
            .massive => &self.massive_free,
        };

        // Atomic find-and-clear first set bit
        while (true) {
            const current = free_mask.load(.acquire);
            if (current == 0) return null; // No free buffers

            // Find first set bit (free buffer)
            const buffer_index = @ctz(current);
            assert(buffer_index < BUFFERS_PER_CLASS);

            const new_mask = current & ~(@as(u8, 1) << @intCast(buffer_index));

            // Try to atomically update the mask
            if (free_mask.cmpxchgWeak(current, new_mask, .acquire, .acquire)) |_| {
                continue; // CAS failed, retry
            }

            // Success! Return buffer at this index
            return self.buffer_at_index(size_class, buffer_index);
        }
    }

    /// Free buffer back to specific size class
    fn free_to_class(self: *BufferPool, size_class: BufferSizeClass, buffer: []u8) void {
        const buffer_index = self.buffer_index(size_class, buffer);

        const free_mask = switch (size_class) {
            .tiny => &self.tiny_free,
            .small => &self.small_free,
            .medium => &self.medium_free,
            .large => &self.large_free,
            .huge => &self.huge_free,
            .massive => &self.massive_free,
        };

        // Atomic set bit for this buffer index
        const bit_mask = @as(u8, 1) << @intCast(buffer_index);
        _ = free_mask.fetchOr(bit_mask, .release);
    }

    /// Get buffer at specific index for size class
    fn buffer_at_index(self: *BufferPool, size_class: BufferSizeClass, index: usize) []u8 {
        assert(index < BUFFERS_PER_CLASS);

        return switch (size_class) {
            .tiny => &self.tiny_buffers[index],
            .small => &self.small_buffers[index],
            .medium => &self.medium_buffers[index],
            .large => &self.large_buffers[index],
            .huge => &self.huge_buffers[index],
            .massive => &self.massive_buffers[index],
        };
    }

    /// Get buffer index from buffer pointer
    fn buffer_index(self: *BufferPool, size_class: BufferSizeClass, buffer: []u8) usize {
        const buffers_start = switch (size_class) {
            .tiny => @intFromPtr(&self.tiny_buffers[0]),
            .small => @intFromPtr(&self.small_buffers[0]),
            .medium => @intFromPtr(&self.medium_buffers[0]),
            .large => @intFromPtr(&self.large_buffers[0]),
            .huge => @intFromPtr(&self.huge_buffers[0]),
            .massive => @intFromPtr(&self.massive_buffers[0]),
        };

        const buffer_ptr = @intFromPtr(buffer.ptr);
        const buffer_size = size_class.size();

        assert(buffer_ptr >= buffers_start);
        const offset = buffer_ptr - buffers_start;
        const index = offset / buffer_size;

        assert(index < BUFFERS_PER_CLASS);
        return index;
    }

    /// Get buffer pool statistics
    pub fn statistics(self: *const BufferPool) struct {
        allocations: u64,
        deallocations: u64,
        pool_hits: u64,
        pool_misses: u64,
        hit_rate: f64,
    } {
        const allocations = self.stats.allocations.load(.acquire);
        const deallocations = self.stats.deallocations.load(.acquire);
        const pool_hits = self.stats.pool_hits.load(.acquire);
        const pool_misses = self.stats.pool_misses.load(.acquire);

        const total_requests = pool_hits + pool_misses;
        const hit_rate = if (total_requests > 0)
            @as(f64, @floatFromInt(pool_hits)) / @as(f64, @floatFromInt(total_requests))
        else
            0.0;

        return .{
            .allocations = allocations,
            .deallocations = deallocations,
            .pool_hits = pool_hits,
            .pool_misses = pool_misses,
            .hit_rate = hit_rate,
        };
    }
};

// Tests

test "BufferPool basic allocation" {
    var pool = try BufferPool.init(std.testing.allocator);
    defer pool.deinit();

    // Test small allocation
    const buffer1 = pool.acquire_buffer(100) orelse return error.AllocationFailed;
    try std.testing.expectEqual(@as(usize, 256), buffer1.data.len); // Gets tiny buffer
    try std.testing.expectEqual(BufferSizeClass.tiny, buffer1.size_class);

    // Test large allocation
    const buffer2 = pool.acquire_buffer(5000) orelse return error.AllocationFailed;
    try std.testing.expectEqual(@as(usize, 16384), buffer2.data.len); // Gets large buffer
    try std.testing.expectEqual(BufferSizeClass.large, buffer2.size_class);

    // Return buffers
    buffer1.release();
    buffer2.release();

    // Verify we can allocate again
    const buffer3 = pool.acquire_buffer(100) orelse return error.AllocationFailed;
    buffer3.release();
}

test "BufferPool exhaustion" {
    var pool = try BufferPool.init(std.testing.allocator);
    defer pool.deinit();
    var buffers: [BUFFERS_PER_CLASS + 1]?PooledBuffer = undefined;

    // Allocate all tiny buffers
    for (0..BUFFERS_PER_CLASS) |i| {
        buffers[i] = pool.acquire_buffer(100);
        try std.testing.expect(buffers[i] != null);
    }

    // Next allocation should fail
    buffers[BUFFERS_PER_CLASS] = pool.acquire_buffer(100);
    try std.testing.expect(buffers[BUFFERS_PER_CLASS] == null);

    // Return one buffer
    buffers[0].?.release();

    // Should be able to allocate again
    buffers[0] = pool.acquire_buffer(100);
    try std.testing.expect(buffers[0] != null);

    // Clean up
    for (buffers[0..BUFFERS_PER_CLASS]) |maybe_buffer| {
        if (maybe_buffer) |buffer| {
            buffer.release();
        }
    }
}

test "BufferPool statistics" {
    var pool = try BufferPool.init(std.testing.allocator);
    defer pool.deinit();

    const initial_stats = pool.statistics();
    try std.testing.expectEqual(@as(u64, 0), initial_stats.allocations);

    const buffer = pool.acquire_buffer(100) orelse return error.AllocationFailed;

    const after_alloc_stats = pool.statistics();
    try std.testing.expectEqual(@as(u64, 1), after_alloc_stats.allocations);
    try std.testing.expectEqual(@as(u64, 1), after_alloc_stats.pool_hits);

    buffer.release();

    const after_free_stats = pool.statistics();
    try std.testing.expectEqual(@as(u64, 1), after_free_stats.deallocations);
}
