//! Static buffer pools for zero-allocation hot paths (TigerBeetle approach).
//!
//! Pre-allocates fixed-size buffers at startup to eliminate malloc/free overhead
//! in critical operations like block deserialization and WAL processing.

const std = @import("std");
const assert = std.debug.assert;

/// Maximum size for a single buffer allocation
pub const MAX_BUFFER_SIZE = 1024 * 1024; // 1MB

/// Number of buffers per pool size class
const BUFFERS_PER_CLASS = 64;

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

    pub fn from_size(required_size: usize) BufferSizeClass {
        if (required_size <= 256) return .tiny;
        if (required_size <= 1024) return .small;
        if (required_size <= 4096) return .medium;
        if (required_size <= 16384) return .large;
        if (required_size <= 65536) return .huge;
        if (required_size <= 262144) return .massive;
        @panic("Buffer size too large for pool allocation");
    }
};

/// A buffer that can be returned to the pool
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

/// Static buffer pool with multiple size classes
pub const BufferPool = struct {
    // Pre-allocated buffer storage for each size class
    tiny_buffers: [BUFFERS_PER_CLASS][BufferSizeClass.tiny.size()]u8,
    small_buffers: [BUFFERS_PER_CLASS][BufferSizeClass.small.size()]u8,
    medium_buffers: [BUFFERS_PER_CLASS][BufferSizeClass.medium.size()]u8,
    large_buffers: [BUFFERS_PER_CLASS][BufferSizeClass.large.size()]u8,
    huge_buffers: [BUFFERS_PER_CLASS][BufferSizeClass.huge.size()]u8,
    massive_buffers: [BUFFERS_PER_CLASS][BufferSizeClass.massive.size()]u8,

    // Free buffer tracking (atomic for thread safety)
    tiny_free: std.atomic.Value(u64),
    small_free: std.atomic.Value(u64),
    medium_free: std.atomic.Value(u64),
    large_free: std.atomic.Value(u64),
    huge_free: std.atomic.Value(u64),
    massive_free: std.atomic.Value(u64),

    // Statistics for monitoring
    stats: Statistics,

    const Statistics = struct {
        allocations: std.atomic.Value(u64),
        deallocations: std.atomic.Value(u64),
        pool_hits: std.atomic.Value(u64),
        pool_misses: std.atomic.Value(u64),
        fallback_allocations: std.atomic.Value(u64),

        pub fn init() Statistics {
            return Statistics{
                .allocations = std.atomic.Value(u64).init(0),
                .deallocations = std.atomic.Value(u64).init(0),
                .pool_hits = std.atomic.Value(u64).init(0),
                .pool_misses = std.atomic.Value(u64).init(0),
                .fallback_allocations = std.atomic.Value(u64).init(0),
            };
        }
    };

    pub fn init() BufferPool {
        return BufferPool{
            .tiny_buffers = undefined,
            .small_buffers = undefined,
            .medium_buffers = undefined,
            .large_buffers = undefined,
            .huge_buffers = undefined,
            .massive_buffers = undefined,
            // Initialize all buffers as available (all bits set)
            .tiny_free = std.atomic.Value(u64).init(std.math.maxInt(u64)),
            .small_free = std.atomic.Value(u64).init(std.math.maxInt(u64)),
            .medium_free = std.atomic.Value(u64).init(std.math.maxInt(u64)),
            .large_free = std.atomic.Value(u64).init(std.math.maxInt(u64)),
            .huge_free = std.atomic.Value(u64).init(std.math.maxInt(u64)),
            .massive_free = std.atomic.Value(u64).init(std.math.maxInt(u64)),
            .stats = Statistics.init(),
        };
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

            const index = @ctz(current);
            const new_mask = current & ~(@as(u64, 1) << @intCast(index));

            if (free_mask.cmpxchgWeak(current, new_mask, .acq_rel, .acquire)) |_| {
                // Someone else took this buffer, try again
                continue;
            }

            // Successfully claimed buffer at index
            return switch (size_class) {
                .tiny => &self.tiny_buffers[index],
                .small => &self.small_buffers[index],
                .medium => &self.medium_buffers[index],
                .large => &self.large_buffers[index],
                .huge => &self.huge_buffers[index],
                .massive => &self.massive_buffers[index],
            };
        }
    }

    /// Return buffer to specific size class pool
    fn free_to_class(self: *BufferPool, size_class: BufferSizeClass, buffer: []u8) void {
        const base_ptr = switch (size_class) {
            .tiny => @intFromPtr(&self.tiny_buffers[0]),
            .small => @intFromPtr(&self.small_buffers[0]),
            .medium => @intFromPtr(&self.medium_buffers[0]),
            .large => @intFromPtr(&self.large_buffers[0]),
            .huge => @intFromPtr(&self.huge_buffers[0]),
            .massive => @intFromPtr(&self.massive_buffers[0]),
        };

        const buffer_ptr = @intFromPtr(buffer.ptr);
        const buffer_size = size_class.size();
        const index = (buffer_ptr - base_ptr) / buffer_size;

        assert(index < BUFFERS_PER_CLASS);

        const free_mask = switch (size_class) {
            .tiny => &self.tiny_free,
            .small => &self.small_free,
            .medium => &self.medium_free,
            .large => &self.large_free,
            .huge => &self.huge_free,
            .massive => &self.massive_free,
        };

        // Atomic set bit to mark buffer as free
        _ = free_mask.fetchOr(@as(u64, 1) << @intCast(index), .release);
    }

    /// Get allocation statistics
    pub fn statistics(self: *BufferPool) struct {
        allocations: u64,
        deallocations: u64,
        pool_hits: u64,
        pool_misses: u64,
        fallback_allocations: u64,
        hit_rate: f64,
    } {
        const allocations = self.stats.allocations.load(.acquire);
        const deallocations = self.stats.deallocations.load(.acquire);
        const pool_hits = self.stats.pool_hits.load(.acquire);
        const pool_misses = self.stats.pool_misses.load(.acquire);
        const fallback_allocations = self.stats.fallback_allocations.load(.acquire);

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
            .fallback_allocations = fallback_allocations,
            .hit_rate = hit_rate,
        };
    }
};

/// Fallback allocator that uses buffer pool first, then heap allocation
pub const PooledAllocator = struct {
    pool: *BufferPool,
    fallback_allocator: std.mem.Allocator,

    const Self = @This();

    pub fn init(pool: *BufferPool, fallback_allocator: std.mem.Allocator) PooledAllocator {
        return PooledAllocator{
            .pool = pool,
            .fallback_allocator = fallback_allocator,
        };
    }

    pub fn allocator(self: *Self) std.mem.Allocator {
        return std.mem.Allocator{
            .ptr = self,
            .vtable = &.{
                .alloc = alloc,
                .resize = resize,
                .free = free,
            },
        };
    }

    fn alloc(ctx: *anyopaque, len: usize, ptr_align: u8, ret_addr: usize) ?[*]u8 {
        const self: *Self = @ptrCast(@alignCast(ctx));
        _ = ptr_align;
        _ = ret_addr;

        // Try pool allocation first
        if (self.pool.acquire_buffer(len)) |pooled_buffer| {
            // Store metadata about pooled allocation (we'll need this for free)
            // For now, return the raw pointer and track via pool statistics
            return pooled_buffer.data.ptr;
        }

        // Fall back to heap allocation
        _ = self.pool.stats.fallback_allocations.fetchAdd(1, .monotonic);
        const slice = self.fallback_allocator.alloc(u8, len) catch return null;
        return slice.ptr;
    }

    fn resize(ctx: *anyopaque, buf: []u8, buf_align: u8, new_len: usize, ret_addr: usize) bool {
        const self: *Self = @ptrCast(@alignCast(ctx));

        // For simplicity, don't support resize on pooled buffers
        // Fall back to the heap allocator
        return self.fallback_allocator.vtable.resize(
            self.fallback_allocator.ptr,
            buf,
            buf_align,
            new_len,
            ret_addr,
        );
    }

    fn free(ctx: *anyopaque, buf: []u8, buf_align: u8, ret_addr: usize) void {
        const self: *Self = @ptrCast(@alignCast(ctx));

        // Check if this buffer came from our pool
        // For now, assume it's from heap allocator (this needs improvement)
        self.fallback_allocator.vtable.free(self.fallback_allocator.ptr, buf, buf_align, ret_addr);
    }
};

// Tests

test "BufferPool basic allocation" {
    var pool = BufferPool.init();

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
    var pool = BufferPool.init();
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
    var pool = BufferPool.init();

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
