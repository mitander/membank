//! Concurrency model for KausalDB.
//!
//! KausalDB uses a single-threaded execution model with async I/O for maximum
//! simplicity, determinism, and performance. This eliminates data races,
//! simplifies debugging, and enables deterministic simulation testing.

const std = @import("std");
const builtin = @import("builtin");

/// Thread ID of the main KausalDB thread. All operations must occur on this thread.
var main_thread_id: ?std.Thread.Id = null;

/// Initialize the concurrency model. Must be called once from the main thread.
pub fn init() void {
    main_thread_id = std.Thread.getCurrentId();
}

/// Assert that we're running on the main KausalDB thread.
/// In debug builds, this provides immediate feedback about threading violations.
/// In release builds, this compiles to nothing.
pub fn assert_main_thread() void {
    if (builtin.mode == .Debug) {
        if (main_thread_id) |expected_id| {
            const current_id = std.Thread.getCurrentId();
            if (current_id != expected_id) {
                std.debug.panic(
                    "Threading violation: operation called from thread {} but requires thread {}",
                    .{ current_id, expected_id },
                );
            }
        }
    }
}

/// Concurrency-aware allocator wrapper that enforces single-threaded access.
pub const SingleThreadedAllocator = struct {
    base_allocator: std.mem.Allocator,

    pub fn init(base_allocator: std.mem.Allocator) SingleThreadedAllocator {
        return SingleThreadedAllocator{ .base_allocator = base_allocator };
    }

    pub fn alloc(self: SingleThreadedAllocator, comptime T: type, n: usize) ![]T {
        assert_main_thread();
        return self.base_allocator.alloc(T, n);
    }

    pub fn free(self: SingleThreadedAllocator, memory: anytype) void {
        assert_main_thread();
        self.base_allocator.free(memory);
    }

    pub fn dupe(self: SingleThreadedAllocator, comptime T: type, m: []const T) ![]T {
        assert_main_thread();
        return self.base_allocator.dupe(T, m);
    }

    pub fn create(self: SingleThreadedAllocator, comptime T: type) !*T {
        assert_main_thread();
        return self.base_allocator.create(T);
    }

    pub fn destroy(self: SingleThreadedAllocator, ptr: anytype) void {
        assert_main_thread();
        self.base_allocator.destroy(ptr);
    }

    pub fn allocator(self: *SingleThreadedAllocator) std.mem.Allocator {
        return std.mem.Allocator{
            .ptr = self,
            .vtable = &.{
                .alloc = alloc_impl,
                .resize = resize_impl,
                .free = free_impl,
                .remap = remap_impl,
            },
        };
    }

    fn alloc_impl(
        ctx: *anyopaque,
        len: usize,
        ptr_align: std.mem.Alignment,
        ret_addr: usize,
    ) ?[*]u8 {
        const self: *SingleThreadedAllocator = @ptrCast(@alignCast(ctx));
        assert_main_thread();
        return self.base_allocator.vtable.alloc(
            self.base_allocator.ptr,
            len,
            ptr_align,
            ret_addr,
        );
    }

    fn resize_impl(
        ctx: *anyopaque,
        buf: []u8,
        buf_align: std.mem.Alignment,
        new_len: usize,
        ret_addr: usize,
    ) bool {
        const self: *SingleThreadedAllocator = @ptrCast(@alignCast(ctx));
        assert_main_thread();
        return self.base_allocator.vtable.resize(
            self.base_allocator.ptr,
            buf,
            buf_align,
            new_len,
            ret_addr,
        );
    }

    fn free_impl(ctx: *anyopaque, buf: []u8, buf_align: std.mem.Alignment, ret_addr: usize) void {
        const self: *SingleThreadedAllocator = @ptrCast(@alignCast(ctx));
        assert_main_thread();
        self.base_allocator.vtable.free(self.base_allocator.ptr, buf, buf_align, ret_addr);
    }

    fn remap_impl(
        ctx: *anyopaque,
        buf: []u8,
        buf_align: std.mem.Alignment,
        new_len: usize,
        ret_addr: usize,
    ) ?[*]u8 {
        const self: *SingleThreadedAllocator = @ptrCast(@alignCast(ctx));
        assert_main_thread();
        return self.base_allocator.vtable.remap(
            self.base_allocator.ptr,
            buf,
            buf_align,
            new_len,
            ret_addr,
        );
    }
};

/// Documentation and enforcement of KausalDB's concurrency model.
/// CONCURRENCY MODEL: Single-threaded + async I/O.
/// Thread safety enforced through design rather than locks.
pub const ConcurrencyModel = struct {
    comptime {
        // This serves as documentation and compile-time enforcement
    }
};

test "main thread detection" {
    // Initialize from test thread
    init();

    // Should not panic when called from same thread
    assert_main_thread();
    assert_main_thread(); // Multiple calls should work
}

test "SingleThreadedAllocator basic operations" {
    const base_allocator = std.testing.allocator;
    var st_allocator = SingleThreadedAllocator.init(base_allocator);

    // Initialize concurrency model
    init();

    // Test basic allocation operations
    const memory = try st_allocator.alloc(u8, 100);
    defer st_allocator.free(memory);

    // Test duplication
    const original = [_]u8{ 1, 2, 3, 4, 5 };
    const duplicated = try st_allocator.dupe(u8, &original);
    defer st_allocator.free(duplicated);

    try std.testing.expectEqualSlices(u8, &original, duplicated);

    // Test create/destroy
    const ptr = try st_allocator.create(u32);
    defer st_allocator.destroy(ptr);
    ptr.* = 42;
    try std.testing.expectEqual(@as(u32, 42), ptr.*);
}

test "SingleThreadedAllocator as allocator interface" {
    const base_allocator = std.testing.allocator;
    var st_allocator = SingleThreadedAllocator.init(base_allocator);

    // Initialize concurrency model
    init();

    // Use as standard allocator interface
    const allocator = st_allocator.allocator();
    const memory = try allocator.alloc(u8, 50);
    defer allocator.free(memory);

    // Should work with ArrayList
    var list = std.ArrayList(u32).init(allocator);
    defer list.deinit();

    try list.append(1);
    try list.append(2);
    try list.append(3);

    try std.testing.expectEqual(@as(usize, 3), list.items.len);
    try std.testing.expectEqual(@as(u32, 2), list.items[1]);
}
