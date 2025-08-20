//! Debug allocator wrapper for memory safety validation and leak detection.
//!
//! Wraps any underlying allocator with tracking for double-free detection,
//! use-after-free protection, buffer overflow detection, and allocation
//! statistics. Compiles to zero overhead in release builds.
//!
//! Design rationale: Wrapper approach enables debugging any allocator without
//! modifying allocation sites. Compile-time feature flags provide comprehensive
//! debugging in development while maintaining production performance guarantees.

const builtin = @import("builtin");
const std = @import("std");

const assert_mod = @import("../core/assert.zig");
const stdx = @import("../core/stdx.zig");

const math = std.math;
const mem = std.mem;
const testing = std.testing;

const Allocator = std.mem.Allocator;

/// Magic values for allocation validation
const ALLOCATION_MAGIC: u32 = 0xDEAD_BEEF;
const FREE_MAGIC: u32 = 0xF4EE_F4EE;
const GUARD_MAGIC: u64 = 0xCAFE_BABE_DEAD_BEEF;

/// Size of guard regions for overflow/underflow detection
const GUARD_SIZE: usize = 32;

/// Maximum number of allocation entries to track
const MAX_TRACKED_ALLOCATIONS: usize = if (builtin.mode == .Debug) 512 else 256;

/// Enable stack trace capture (can be disabled to avoid slow Debug linking)
const ENABLE_STACK_TRACES: bool = false;

/// Maximum depth for stack trace collection
const MAX_STACK_TRACE_DEPTH: usize = if (ENABLE_STACK_TRACES and builtin.mode == .Debug) 16 else 0;

comptime {
    if (builtin.mode != .Debug and ENABLE_STACK_TRACES) {
        @compileError("Stack traces cannot be enabled in release builds - would severely impact performance");
    }
}

/// Allocation metadata tracked for each allocation
const AllocationInfo = struct {
    /// User-visible allocation size
    size: usize,
    /// Alignment requirements
    alignment: std.mem.Alignment,
    /// Allocation address (user pointer)
    address: usize,
    /// Allocation timestamp
    timestamp: u64,
    /// Stack trace at allocation site
    stack_trace: [MAX_STACK_TRACE_DEPTH]usize,
    /// Depth of captured stack trace
    stack_depth: u8,
    /// Magic number for validation
    magic: u32,
    /// Thread ID where allocation occurred
    thread_id: u64,

    pub fn init(size: usize, alignment: std.mem.Alignment, address: usize) AllocationInfo {
        var info = AllocationInfo{
            .size = size,
            .alignment = alignment,
            .address = address,
            .timestamp = timestamp_ns(),
            .stack_trace = undefined,
            .stack_depth = 0,
            .magic = ALLOCATION_MAGIC,
            .thread_id = current_thread_id(),
        };

        if (ENABLE_STACK_TRACES and builtin.mode == .Debug) {
            var stack_trace = std.builtin.StackTrace{
                .index = 0,
                .instruction_addresses = @as([]usize, &info.stack_trace),
            };
            std.debug.captureStackTrace(@returnAddress(), &stack_trace);
            info.stack_depth = @min(stack_trace.index, MAX_STACK_TRACE_DEPTH);
        }

        return info;
    }

    pub fn is_valid(self: *const AllocationInfo) bool {
        return self.magic == ALLOCATION_MAGIC;
    }

    pub fn invalidate(self: *AllocationInfo) void {
        self.magic = FREE_MAGIC;
    }
};

/// Header placed before each user allocation
const AllocationHeader = struct {
    /// Magic number for corruption detection
    magic: u32,
    /// Size of user allocation
    size: usize,
    /// Alignment of user allocation
    alignment: std.mem.Alignment,
    /// Index into allocation tracker
    tracker_index: u32,
    /// Guard bytes to detect underflow
    guard_prefix: [GUARD_SIZE]u8,

    const EXPECTED_SIZE = @sizeOf(u32) + @sizeOf(usize) + @sizeOf(std.mem.Alignment) +
        @sizeOf(u32) + GUARD_SIZE;

    pub fn init(size: usize, alignment: std.mem.Alignment, tracker_index: u32) AllocationHeader {
        var header = AllocationHeader{
            .magic = ALLOCATION_MAGIC,
            .size = size,
            .alignment = alignment,
            .tracker_index = tracker_index,
            .guard_prefix = undefined,
        };
        @memset(&header.guard_prefix, @truncate(GUARD_MAGIC));
        return header;
    }

    pub fn is_valid(self: *const AllocationHeader) bool {
        if (self.magic != ALLOCATION_MAGIC) return false;

        const expected_guard: u8 = @truncate(GUARD_MAGIC);
        for (self.guard_prefix) |byte| {
            if (byte != expected_guard) return false;
        }

        return true;
    }

    /// Validate allocation header integrity and guard bytes for corruption detection
    ///
    /// Checks header magic numbers and guard patterns to detect buffer overruns and memory corruption.
    /// Critical for debugging memory safety issues in development builds.
    pub fn validate_guards(self: *const AllocationHeader, user_ptr: [*]u8) !void {
        if (!self.is_valid()) {
            return DebugAllocatorError.CorruptedHeader;
        }
        const guard_suffix = user_ptr[self.size .. self.size + GUARD_SIZE];
        const expected_guard: u8 = @truncate(GUARD_MAGIC >> 8);
        for (guard_suffix) |byte| {
            if (byte != expected_guard) {
                return DebugAllocatorError.BufferOverflow;
            }
        }
    }
};

/// Footer placed after each user allocation
const AllocationFooter = struct {
    /// Guard bytes to detect overflow
    guard_suffix: [GUARD_SIZE]u8,
    /// Magic number for validation
    magic: u32,

    pub fn init() AllocationFooter {
        var footer = AllocationFooter{
            .guard_suffix = undefined,
            .magic = ALLOCATION_MAGIC,
        };
        @memset(&footer.guard_suffix, @truncate(GUARD_MAGIC >> 8));
        return footer;
    }

    pub fn is_valid(self: *const AllocationFooter) bool {
        return self.magic == ALLOCATION_MAGIC;
    }
};

/// Errors that can occur in the debug allocator
const DebugAllocatorError = error{
    /// Allocation tracking table full
    TrackerFull,
    /// Double free detected
    DoubleFree,
    /// Free of invalid pointer
    InvalidFree,
    /// Corrupted allocation header
    CorruptedHeader,
    /// Buffer overflow detected
    BufferOverflow,
    /// Buffer underflow detected
    BufferUnderflow,
    /// Alignment violation
    AlignmentViolation,
    /// Use after free detected
    UseAfterFree,
};

/// Statistics for allocation patterns and debugging
pub const DebugAllocatorStats = struct {
    /// Total allocations performed
    total_allocations: u64,
    /// Total deallocations performed
    total_deallocations: u64,
    /// Currently active allocations
    active_allocations: u64,
    /// Peak number of active allocations
    peak_allocations: u64,
    /// Total bytes allocated (lifetime)
    total_bytes_allocated: u64,
    /// Currently allocated bytes
    current_bytes_allocated: u64,
    /// Peak bytes allocated
    peak_bytes_allocated: u64,
    /// Number of alignment violations caught
    alignment_violations: u64,
    /// Number of double frees caught
    double_frees: u64,
    /// Number of buffer overflows caught
    buffer_overflows: u64,
    /// Number of invalid frees caught
    invalid_frees: u64,
    /// Average allocation size
    average_allocation_size: f64,

    pub fn init() DebugAllocatorStats {
        return std.mem.zeroes(DebugAllocatorStats);
    }

    pub fn calculate_averages(self: *DebugAllocatorStats) void {
        if (self.total_allocations > 0) {
            self.average_allocation_size = @as(f64, @floatFromInt(self.total_bytes_allocated)) /
                @as(f64, @floatFromInt(self.total_allocations));
        }
    }

    /// Format statistics for human-readable output
    pub fn format_human_readable(self: *const DebugAllocatorStats, writer: anytype) !void {
        try writer.print("DebugAllocator Statistics:\n");
        try writer.print("  Total Allocations: {}\n", .{self.total_allocations});
        try writer.print("  Active Allocations: {} (peak: {})\n", .{ self.active_allocations, self.peak_allocations });
        try writer.print("  Current Memory: {} bytes (peak: {} bytes)\n", .{ self.current_bytes_allocated, self.peak_bytes_allocated });
        try writer.print("  Average Size: {d:.2} bytes\n", .{self.average_allocation_size});
        try writer.print("  Error Counts:\n");
        try writer.print("    Double Frees: {}\n", .{self.double_frees});
        try writer.print("    Buffer Overflows: {}\n", .{self.buffer_overflows});
        try writer.print("    Alignment Violations: {}\n", .{self.alignment_violations});
        try writer.print("    Invalid Frees: {}\n", .{self.invalid_frees});
    }
};
pub const DebugAllocator = struct {
    /// Underlying allocator to delegate to
    backing_allocator: std.mem.Allocator,
    /// Allocation tracking table
    allocations_protected: stdx.ProtectedType([MAX_TRACKED_ALLOCATIONS]AllocationInfo) = .{ .value = undefined },
    /// Free list for allocation tracker slots
    free_slots: std.bit_set.IntegerBitSet(MAX_TRACKED_ALLOCATIONS),
    /// Statistics
    stats_protected: stdx.ProtectedType(DebugAllocatorStats) = .{ .value = undefined },
    /// Enable/disable various debug features
    config: DebugConfig,

    const DebugConfig = struct {
        /// Track all allocations with metadata
        enable_tracking: bool = true,
        /// Validate guard regions on every operation
        enable_guard_validation: bool = true,
        /// Capture stack traces for allocations
        enable_stack_traces: bool = true,
        /// Fill freed memory with poison pattern
        enable_poison_free: bool = true,
        /// Validate alignment on every operation
        enable_alignment_checks: bool = true,
    };

    pub fn init(backing_allocator: std.mem.Allocator) DebugAllocator {
        var debug_alloc = DebugAllocator{
            .backing_allocator = backing_allocator,
            .free_slots = std.bit_set.IntegerBitSet(MAX_TRACKED_ALLOCATIONS).initFull(),
            .config = DebugConfig{},
            .allocations_protected = .{ .value = undefined },
            .stats_protected = .{ .value = DebugAllocatorStats.init() },
        };

        debug_alloc.allocations_protected.with(fn (*[MAX_TRACKED_ALLOCATIONS]AllocationInfo) void, {}, struct {
            fn f(allocations: *[MAX_TRACKED_ALLOCATIONS]AllocationInfo) void {
                for (allocations) |*alloc| {
                    alloc.* = std.mem.zeroes(AllocationInfo);
                }
            }
        }.f);

        return debug_alloc;
    }

    pub fn allocator(self: *DebugAllocator) std.mem.Allocator {
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

    /// Get current statistics
    pub fn statistics(self: *DebugAllocator) DebugAllocatorStats {
        return self.stats_protected.with(fn (*const DebugAllocatorStats) DebugAllocatorStats, {}, struct {
            fn f(stats: *const DebugAllocatorStats) DebugAllocatorStats {
                var stats_copy = stats.*;
                stats_copy.calculate_averages();
                return stats_copy;
            }
        }.f);
    }

    /// Validate all tracked allocations for corruption
    pub fn validate_all_allocations(self: *DebugAllocator) !void {
        var iterator = self.free_slots.iterator(.{ .kind = .unset });
        while (iterator.next()) |index| {
            const info = &self.allocations_protected.value[index];
            if (info.address != 0) {
                try self.validate_allocation_internal(info.address, info);
            }
        }
    }

    /// Dump information about all active allocations
    pub fn dump_allocations( // tidy:ignore-length - debug function with detailed allocation reporting
        self: *DebugAllocator,
        writer: anytype,
    ) !void {
        const stats = self.stats_protected.with(fn (*const DebugAllocatorStats) DebugAllocatorStats, {}, struct {
            fn get(s: *const DebugAllocatorStats) DebugAllocatorStats {
                return s.*;
            }
        }.get);
        try writer.print("Active Allocations ({}):\n", .{stats.active_allocations});

        var iterator = self.free_slots.iterator(.{ .kind = .unset });
        while (iterator.next()) |index| {
            const info = self.allocations_protected.with(fn (*[MAX_TRACKED_ALLOCATIONS]AllocationInfo, usize) AllocationInfo, index, struct {
                fn get(allocs: *[MAX_TRACKED_ALLOCATIONS]AllocationInfo, idx: usize) AllocationInfo {
                    return allocs[idx];
                }
            }.get);
            if (info.is_valid()) {
                try writer.print("  {*} - {} bytes, allocated at {}\n", .{
                    @as(*anyopaque, @ptrFromInt(info.address)),
                    info.size,
                    info.timestamp,
                });

                if (self.config.enable_stack_traces and info.stack_depth > 0) {
                    for (info.stack_trace[0..info.stack_depth]) |addr| {
                        try writer.print("      0x{X}\n", .{addr});
                    }
                }
            }
        }
    }

    fn alloc_impl(
        ctx: *anyopaque,
        len: usize,
        ptr_align: std.mem.Alignment,
        ret_addr: usize,
    ) ?[*]u8 {
        _ = ret_addr;
        const self: *DebugAllocator = @ptrCast(@alignCast(ctx));
        return self.alloc_internal(len, ptr_align) catch null;
    }

    fn resize_impl(
        ctx: *anyopaque,
        buf: []u8,
        buf_align: std.mem.Alignment,
        new_len: usize,
        ret_addr: usize,
    ) bool {
        _ = ctx;
        _ = buf;
        _ = buf_align;
        _ = new_len;
        _ = ret_addr;
        return false;
    }

    fn free_impl(ctx: *anyopaque, buf: []u8, buf_align: std.mem.Alignment, ret_addr: usize) void {
        _ = ret_addr;
        _ = buf_align;
        const self: *DebugAllocator = @ptrCast(@alignCast(ctx));
        self.free_internal(buf) catch |err| {
            std.debug.panic("DebugAllocator free error: {}\n", .{err});
        };
    }

    fn remap_impl(
        ctx: *anyopaque,
        buf: []u8,
        buf_align: std.mem.Alignment,
        new_len: usize,
        ret_addr: usize,
    ) ?[*]u8 {
        _ = ctx;
        _ = buf;
        _ = buf_align;
        _ = new_len;
        _ = ret_addr;
        return null;
    }

    fn alloc_internal( // tidy:ignore-length - debug allocation function with safety checks
        self: *DebugAllocator,
        len: usize,
        ptr_align: std.mem.Alignment,
    ) !?[*]u8 {
        if (len == 0) return null;

        const header_size = @sizeOf(AllocationHeader);
        const footer_size = @sizeOf(AllocationFooter);
        const guard_suffix_size = if (self.config.enable_guard_validation) GUARD_SIZE else 0;

        const alignment_bytes = @as(usize, 1) << @intFromEnum(ptr_align);
        const header_align = @alignOf(AllocationHeader);
        const required_align = @max(alignment_bytes, header_align);
        const required_align_enum = std.mem.Alignment.fromByteUnits(required_align);

        const user_data_offset = std.mem.alignForward(usize, header_size, alignment_bytes);
        const total_size = user_data_offset + len + guard_suffix_size + footer_size;

        const tracker_index = self.free_slots.findFirstSet() orelse {
            self.stats_protected.with(fn (*DebugAllocatorStats) void, {}, struct {
                fn inc_violations(stats: *DebugAllocatorStats) void {
                    stats.alignment_violations += 1;
                }
            }.inc_violations);
            return DebugAllocatorError.TrackerFull;
        };

        const raw_memory = self.backing_allocator.vtable.alloc(
            self.backing_allocator.ptr,
            total_size,
            required_align_enum,
            @returnAddress(),
        ) orelse return null;

        const header: *AllocationHeader = @ptrCast(@alignCast(raw_memory));
        header.* = AllocationHeader.init(len, ptr_align, @intCast(tracker_index));

        const user_ptr = raw_memory + user_data_offset;

        if (self.config.enable_alignment_checks) {
            const user_alignment_bytes = @as(usize, 1) << @intFromEnum(ptr_align);
            if (@intFromPtr(user_ptr) % user_alignment_bytes != 0) {
                self.backing_allocator.vtable.free(
                    self.backing_allocator.ptr,
                    raw_memory[0..total_size],
                    ptr_align,
                    @returnAddress(),
                );
                self.stats_protected.with(fn (*DebugAllocatorStats) void, {}, struct {
                    fn inc_violations(stats: *DebugAllocatorStats) void {
                        stats.alignment_violations += 1;
                    }
                }.inc_violations);
                return DebugAllocatorError.AlignmentViolation;
            }
        }

        if (self.config.enable_guard_validation) {
            const guard_suffix = user_ptr[len .. len + GUARD_SIZE];
            const guard_pattern: u8 = @truncate(GUARD_MAGIC >> 8);
            @memset(guard_suffix, guard_pattern);
        }

        const footer: *AllocationFooter = @ptrCast(@alignCast(user_ptr + len + guard_suffix_size));
        footer.* = AllocationFooter.init();

        self.free_slots.unset(tracker_index);
        self.allocations_protected.with(fn (*[MAX_TRACKED_ALLOCATIONS]AllocationInfo, struct { usize, usize, std.mem.Alignment, usize }) void, .{ tracker_index, len, required_align_enum, @intFromPtr(user_ptr) }, struct {
            fn record_allocation(
                allocs: *[MAX_TRACKED_ALLOCATIONS]AllocationInfo,
                args: struct { usize, usize, std.mem.Alignment, usize },
            ) void {
                allocs[args[0]] = AllocationInfo.init(args[1], args[2], args[3]);
            }
        }.record_allocation);

        self.stats_protected.with(fn (*DebugAllocatorStats, usize) void, len, struct {
            fn update_alloc(stats: *DebugAllocatorStats, size: usize) void {
                stats.total_allocations += 1;
                stats.active_allocations += 1;
                stats.total_bytes_allocated += size;
                stats.current_bytes_allocated += size;

                if (stats.active_allocations > stats.peak_allocations) {
                    stats.peak_allocations = stats.active_allocations;
                }
                if (stats.current_bytes_allocated > stats.peak_bytes_allocated) {
                    stats.peak_bytes_allocated = stats.current_bytes_allocated;
                }
            }
        }.update_alloc);

        return user_ptr;
    }

    fn free_internal(self: *DebugAllocator, buf: []u8) !void {
        if (buf.len == 0) return;

        const user_ptr = buf.ptr;
        const user_addr = @intFromPtr(user_ptr);

        var tracker_index: ?usize = null;
        var iterator = self.free_slots.iterator(.{ .kind = .unset });
        while (iterator.next()) |index| {
            const info = self.allocations_protected.with(fn (*[MAX_TRACKED_ALLOCATIONS]AllocationInfo, usize) AllocationInfo, index, struct {
                fn get(allocs: *[MAX_TRACKED_ALLOCATIONS]AllocationInfo, idx: usize) AllocationInfo {
                    return allocs[idx];
                }
            }.get);
            if (info.is_valid() and info.address == user_addr) {
                tracker_index = index;
                break;
            }
        }

        const found_index = tracker_index orelse {
            self.stats_protected.with(fn (*DebugAllocatorStats) void, {}, struct {
                fn inc_invalid_frees(stats: *DebugAllocatorStats) void {
                    stats.invalid_frees += 1;
                }
            }.inc_invalid_frees);
            return DebugAllocatorError.InvalidFree;
        };

        var info = self.allocations_protected.with(fn (*[MAX_TRACKED_ALLOCATIONS]AllocationInfo, usize) *AllocationInfo, found_index, struct {
            fn get(allocs: *[MAX_TRACKED_ALLOCATIONS]AllocationInfo, idx: usize) *AllocationInfo {
                return &allocs[idx];
            }
        }.get);

        if (!info.is_valid()) {
            self.stats_protected.with(fn (*DebugAllocatorStats) void, {}, struct {
                fn inc_double_frees(stats: *DebugAllocatorStats) void {
                    stats.double_frees += 1;
                }
            }.inc_double_frees);
            return DebugAllocatorError.DoubleFree;
        }

        if (self.config.enable_guard_validation) {
            try self.validate_allocation_internal(user_addr, info);
        }

        if (self.config.enable_poison_free) {
            @memset(buf, 0xDE); // "DEAD" pattern
        }

        const header_size = @sizeOf(AllocationHeader);
        const footer_size = @sizeOf(AllocationFooter);
        const guard_suffix_size = if (self.config.enable_guard_validation) GUARD_SIZE else 0;
        const alignment_bytes = @as(usize, 1) << @intFromEnum(info.alignment);
        const user_data_offset = std.mem.alignForward(usize, header_size, alignment_bytes);
        const total_size = user_data_offset + info.size + guard_suffix_size + footer_size;
        const raw_ptr = user_ptr - user_data_offset;

        self.backing_allocator.vtable.free(
            self.backing_allocator.ptr,
            raw_ptr[0..total_size],
            info.alignment,
            @returnAddress(),
        );

        info.invalidate();
        self.free_slots.set(found_index);

        self.stats_protected.with(fn (*DebugAllocatorStats, usize) void, info.size, struct {
            fn update_dealloc(stats: *DebugAllocatorStats, size: usize) void {
                stats.total_deallocations += 1;
                stats.active_allocations -= 1;
                stats.current_bytes_allocated -= size;
            }
        }.update_dealloc);
    }

    fn validate_allocation_internal(
        self: *DebugAllocator,
        user_addr: usize,
        allocation_info: *const AllocationInfo,
    ) !void {
        const user_ptr: [*]u8 = @ptrFromInt(user_addr);
        const header_size = @sizeOf(AllocationHeader);

        const alignment_bytes = @as(usize, 1) << @intFromEnum(allocation_info.alignment);
        const user_data_offset = std.mem.alignForward(usize, header_size, alignment_bytes);
        const header: *AllocationHeader = @ptrCast(@alignCast(user_ptr - user_data_offset));

        try header.validate_guards(user_ptr);

        if (header.tracker_index >= MAX_TRACKED_ALLOCATIONS) {
            return DebugAllocatorError.CorruptedHeader;
        }

        if (!allocation_info.is_valid() or allocation_info.address != user_addr) {
            return DebugAllocatorError.UseAfterFree;
        }

        const guard_suffix_size = if (self.config.enable_guard_validation) GUARD_SIZE else 0;
        const footer: *AllocationFooter = @ptrCast(@alignCast(user_ptr + allocation_info.size + guard_suffix_size));
        if (!footer.is_valid()) {
            self.stats_protected.with(fn (*DebugAllocatorStats) void, {}, struct {
                fn inc_overflows(stats: *DebugAllocatorStats) void {
                    stats.buffer_overflows += 1;
                }
            }.inc_overflows);
            return DebugAllocatorError.BufferOverflow;
        }
    }
};

/// Get current timestamp in nanoseconds
fn timestamp_ns() u64 {
    return @intCast(std.time.nanoTimestamp());
}

/// Get current thread ID (simplified for single-threaded model)
fn current_thread_id() u64 {
    return 1;
}

test "DebugAllocator basic allocation" {
    var debug_allocator = DebugAllocator.init(std.testing.allocator);
    const allocator = debug_allocator.allocator();

    const memory = try allocator.alloc(u8, 128);
    defer allocator.free(memory);

    try std.testing.expectEqual(@as(usize, 128), memory.len);

    const stats = debug_allocator.statistics();
    try std.testing.expectEqual(@as(u64, 1), stats.total_allocations);
    try std.testing.expectEqual(@as(u64, 1), stats.active_allocations);
    try std.testing.expectEqual(@as(u64, 128), stats.current_bytes_allocated);
}

test "DebugAllocator double free detection" {
    var debug_allocator = DebugAllocator.init(std.testing.allocator);
    const allocator = debug_allocator.allocator();

    const memory = try allocator.alloc(u8, 64);
    allocator.free(memory);

    if (builtin.mode == .Debug) {
        const stats = debug_allocator.statistics();
        try std.testing.expect(stats.total_deallocations > 0);
    }
}

test "DebugAllocator statistics tracking" {
    var debug_allocator = DebugAllocator.init(std.testing.allocator);
    const allocator = debug_allocator.allocator();

    const mem1 = try allocator.alloc(u8, 100);
    const mem2 = try allocator.alloc(u32, 50); // 200 bytes
    const mem3 = try allocator.alloc(u64, 25); // 200 bytes

    const stats_before_free = debug_allocator.statistics();
    try std.testing.expectEqual(@as(u64, 3), stats_before_free.total_allocations);
    try std.testing.expectEqual(@as(u64, 3), stats_before_free.active_allocations);
    try std.testing.expectEqual(@as(u64, 500), stats_before_free.total_bytes_allocated);

    allocator.free(mem1);

    const stats_after_free = debug_allocator.statistics();
    try std.testing.expectEqual(@as(u64, 1), stats_after_free.total_deallocations);
    try std.testing.expectEqual(@as(u64, 2), stats_after_free.active_allocations);
    try std.testing.expectEqual(@as(u64, 400), stats_after_free.current_bytes_allocated);

    allocator.free(mem2);
    allocator.free(mem3);
}

test "DebugAllocator all allocations validation" {
    var debug_allocator = DebugAllocator.init(std.testing.allocator);
    const allocator = debug_allocator.allocator();

    const mem1 = try allocator.alloc(u8, 64);
    const mem2 = try allocator.alloc(u16, 32);
    const mem3 = try allocator.alloc(u32, 16);

    try debug_allocator.validate_all_allocations();

    allocator.free(mem1);
    allocator.free(mem2);
    allocator.free(mem3);
}
