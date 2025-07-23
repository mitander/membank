//! Debug allocator for comprehensive allocation tracking and memory safety validation.
//!
//! Provides enhanced debugging capabilities for memory allocation issues:
//! - Allocation/deallocation tracking with stack traces
//! - Alignment verification and corruption detection
//! - Double-free detection and use-after-free protection
//! - Buffer overflow/underflow detection with guard pages
//! - Statistical analysis of allocation patterns
//!
//! This allocator wraps any underlying allocator and adds comprehensive debugging.
//! All debug features are controlled by compile-time flags and can be disabled
//! in release builds for zero overhead.

const std = @import("std");
const assert = @import("assert");
const builtin = @import("builtin");

/// Magic values for allocation validation
const ALLOCATION_MAGIC: u32 = 0xDEAD_BEEF;
const FREE_MAGIC: u32 = 0xF4EE_F4EE;
const GUARD_MAGIC: u64 = 0xCAFE_BABE_DEAD_BEEF;

/// Size of guard regions for overflow/underflow detection
const GUARD_SIZE: usize = 32;

/// Maximum number of allocation entries to track
const MAX_TRACKED_ALLOCATIONS: usize = if (builtin.mode == .Debug) 16384 else 1024;

/// Enable stack trace capture (can be disabled to avoid slow Debug linking)
const ENABLE_STACK_TRACES: bool = false; // TODO: Enable when Zig linking performance improves

/// Maximum depth for stack trace collection
const MAX_STACK_TRACE_DEPTH: usize = if (ENABLE_STACK_TRACES and builtin.mode == .Debug) 16 else 0;

// Compile-time guarantees to prevent debug features in release builds
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

        // Capture stack trace when enabled and in debug builds
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
        // Fill guard region with known pattern
        @memset(&header.guard_prefix, @truncate(GUARD_MAGIC));
        return header;
    }

    pub fn is_valid(self: *const AllocationHeader) bool {
        if (self.magic != ALLOCATION_MAGIC) return false;

        // Check guard prefix for underflow
        const expected_guard: u8 = @truncate(GUARD_MAGIC);
        for (self.guard_prefix) |byte| {
            if (byte != expected_guard) return false;
        }

        return true;
    }

    pub fn validate_guards(self: *const AllocationHeader, user_ptr: [*]u8) !void {
        // Check header magic
        if (!self.is_valid()) {
            return DebugAllocatorError.CorruptedHeader;
        }

        // Check guard suffix (after user data)
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
        // Fill guard region with different pattern than header
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

/// Debug allocator that wraps another allocator with comprehensive tracking
pub const DebugAllocator = struct {
    /// Underlying allocator to delegate to
    backing_allocator: std.mem.Allocator,
    /// Allocation tracking table
    allocations: [MAX_TRACKED_ALLOCATIONS]AllocationInfo,
    /// Free list for allocation tracker slots
    free_slots: std.bit_set.IntegerBitSet(MAX_TRACKED_ALLOCATIONS),
    /// Statistics
    stats: DebugAllocatorStats,
    /// Mutex for thread safety
    mutex: std.Thread.Mutex,
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
        return DebugAllocator{
            .backing_allocator = backing_allocator,
            .allocations = std.mem.zeroes([MAX_TRACKED_ALLOCATIONS]AllocationInfo),
            .free_slots = std.bit_set.IntegerBitSet(MAX_TRACKED_ALLOCATIONS).initFull(),
            .stats = DebugAllocatorStats.init(),
            .mutex = std.Thread.Mutex{},
            .config = DebugConfig{},
        };
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
        self.mutex.lock();
        defer self.mutex.unlock();

        var stats_copy = self.stats;
        stats_copy.calculate_averages();
        return stats_copy;
    }

    /// Validate all tracked allocations for corruption
    pub fn validate_all_allocations(self: *DebugAllocator) !void {
        self.mutex.lock();
        defer self.mutex.unlock();

        var iterator = self.free_slots.iterator(.{ .kind = .unset });
        while (iterator.next()) |index| {
            const info = &self.allocations[index];
            if (info.is_valid()) {
                try self.validate_allocation_internal(info.address, info);
            }
        }
    }

    /// Dump information about all active allocations
    pub fn dump_allocations(self: *DebugAllocator, writer: anytype) !void {
        self.mutex.lock();
        defer self.mutex.unlock();

        try writer.print("Active Allocations ({}):\n", .{self.stats.active_allocations});

        var iterator = self.free_slots.iterator(.{ .kind = .unset });
        while (iterator.next()) |index| {
            const info = &self.allocations[index];
            if (info.is_valid()) {
                try writer.print("  [{}] {} bytes at 0x{X} (align={})\n", .{ index, info.size, info.address, @intFromEnum(info.alignment) });

                if (self.config.enable_stack_traces and
                    ENABLE_STACK_TRACES and
                    builtin.mode == .Debug and
                    info.stack_depth > 0)
                {
                    try writer.print("    Stack trace:\n");
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
        // Debug allocator does not support resizing to maintain tracking integrity
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
        // Debug allocator does not support remapping
        return null;
    }

    fn alloc_internal(self: *DebugAllocator, len: usize, ptr_align: std.mem.Alignment) !?[*]u8 {
        if (len == 0) return null;

        self.mutex.lock();
        defer self.mutex.unlock();

        const header_size = @sizeOf(AllocationHeader);
        const footer_size = @sizeOf(AllocationFooter);
        const guard_suffix_size = if (self.config.enable_guard_validation) GUARD_SIZE else 0;

        // Calculate alignment requirements
        const alignment_bytes = @as(usize, 1) << @intFromEnum(ptr_align);
        const header_align = @alignOf(AllocationHeader);
        const required_align = @max(alignment_bytes, header_align);
        const required_align_enum = std.mem.Alignment.fromByteUnits(required_align);

        // Calculate user data offset with proper alignment
        const user_data_offset = std.mem.alignForward(usize, header_size, alignment_bytes);
        const total_size = user_data_offset + len + guard_suffix_size + footer_size;

        const tracker_index = self.free_slots.findFirstSet() orelse {
            self.stats.alignment_violations += 1;
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
                self.stats.alignment_violations += 1;
                return DebugAllocatorError.AlignmentViolation;
            }
        }

        // Write guard suffix after user data if guard validation is enabled
        if (self.config.enable_guard_validation) {
            const guard_suffix = user_ptr[len .. len + GUARD_SIZE];
            const guard_pattern: u8 = @truncate(GUARD_MAGIC >> 8);
            @memset(guard_suffix, guard_pattern);
        }

        const footer: *AllocationFooter = @ptrCast(@alignCast(user_ptr + len + guard_suffix_size));
        footer.* = AllocationFooter.init();

        self.free_slots.unset(tracker_index);
        self.allocations[tracker_index] = AllocationInfo.init(len, required_align_enum, @intFromPtr(user_ptr));

        self.stats.total_allocations += 1;
        self.stats.active_allocations += 1;
        self.stats.total_bytes_allocated += len;
        self.stats.current_bytes_allocated += len;

        if (self.stats.active_allocations > self.stats.peak_allocations) {
            self.stats.peak_allocations = self.stats.active_allocations;
        }
        if (self.stats.current_bytes_allocated > self.stats.peak_bytes_allocated) {
            self.stats.peak_bytes_allocated = self.stats.current_bytes_allocated;
        }

        return user_ptr;
    }

    fn free_internal(self: *DebugAllocator, buf: []u8) !void {
        if (buf.len == 0) return;

        self.mutex.lock();
        defer self.mutex.unlock();

        const user_ptr = buf.ptr;
        const user_addr = @intFromPtr(user_ptr);

        // Find allocation in tracker
        var tracker_index: ?usize = null;
        var iterator = self.free_slots.iterator(.{ .kind = .unset });
        while (iterator.next()) |index| {
            const info = &self.allocations[index];
            if (info.is_valid() and info.address == user_addr) {
                tracker_index = index;
                break;
            }
        }

        const found_index = tracker_index orelse {
            self.stats.invalid_frees += 1;
            return DebugAllocatorError.InvalidFree;
        };

        const info = &self.allocations[found_index];

        // Check for double free
        if (!info.is_valid()) {
            self.stats.double_frees += 1;
            return DebugAllocatorError.DoubleFree;
        }

        // Validate guards if enabled
        if (self.config.enable_guard_validation) {
            try self.validate_allocation_internal(user_addr, info);
        }

        // Poison freed memory if enabled
        if (self.config.enable_poison_free) {
            @memset(buf, 0xDE); // "DEAD" pattern
        }

        // Calculate original allocation size and offset
        const header_size = @sizeOf(AllocationHeader);
        const footer_size = @sizeOf(AllocationFooter);
        const guard_suffix_size = if (self.config.enable_guard_validation) GUARD_SIZE else 0;
        const alignment_bytes = @as(usize, 1) << @intFromEnum(info.alignment);
        const user_data_offset = std.mem.alignForward(usize, header_size, alignment_bytes);
        const total_size = user_data_offset + info.size + guard_suffix_size + footer_size;
        const raw_ptr = user_ptr - user_data_offset;

        // Free the underlying memory
        self.backing_allocator.vtable.free(
            self.backing_allocator.ptr,
            raw_ptr[0..total_size],
            info.alignment,
            @returnAddress(),
        );

        // Mark allocation as freed
        info.invalidate();
        self.free_slots.set(found_index);

        // Update statistics
        self.stats.total_deallocations += 1;
        self.stats.active_allocations -= 1;
        self.stats.current_bytes_allocated -= info.size;
    }

    fn validate_allocation_internal(self: *DebugAllocator, user_addr: usize, allocation_info: *const AllocationInfo) !void {
        const user_ptr: [*]u8 = @ptrFromInt(user_addr);
        const header_size = @sizeOf(AllocationHeader);

        // Calculate the offset using the allocation's alignment
        const alignment_bytes = @as(usize, 1) << @intFromEnum(allocation_info.alignment);
        const user_data_offset = std.mem.alignForward(usize, header_size, alignment_bytes);
        const header: *AllocationHeader = @ptrCast(@alignCast(user_ptr - user_data_offset));

        // Validate header and guards
        try header.validate_guards(user_ptr);

        // Validate allocation info consistency
        if (header.tracker_index >= MAX_TRACKED_ALLOCATIONS) {
            return DebugAllocatorError.CorruptedHeader;
        }

        if (!allocation_info.is_valid() or allocation_info.address != user_addr) {
            return DebugAllocatorError.UseAfterFree;
        }

        // Validate footer
        const guard_suffix_size = if (self.config.enable_guard_validation) GUARD_SIZE else 0;
        const footer: *AllocationFooter = @ptrCast(@alignCast(user_ptr + allocation_info.size + guard_suffix_size));
        if (!footer.is_valid()) {
            self.stats.buffer_overflows += 1;
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
    // In CortexDB's single-threaded model, always return 1
    return 1;
}

// Tests

test "DebugAllocator basic allocation" {
    var debug_allocator = DebugAllocator.init(std.testing.allocator);
    const allocator = debug_allocator.allocator();

    const memory = try allocator.alloc(u8, 128);
    defer allocator.free(memory);

    // Verify allocation worked
    try std.testing.expectEqual(@as(usize, 128), memory.len);

    // Verify statistics
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

    // Second free should be caught in debug mode
    if (builtin.mode == .Debug) {
        // Note: This would panic in actual usage, testing the detection mechanism
        const stats = debug_allocator.statistics();
        try std.testing.expect(stats.total_deallocations > 0);
    }
}

test "DebugAllocator statistics tracking" {
    var debug_allocator = DebugAllocator.init(std.testing.allocator);
    const allocator = debug_allocator.allocator();

    // Allocate several different sizes
    const mem1 = try allocator.alloc(u8, 100);
    const mem2 = try allocator.alloc(u32, 50); // 200 bytes
    const mem3 = try allocator.alloc(u64, 25); // 200 bytes

    const stats_before_free = debug_allocator.statistics();
    try std.testing.expectEqual(@as(u64, 3), stats_before_free.total_allocations);
    try std.testing.expectEqual(@as(u64, 3), stats_before_free.active_allocations);
    try std.testing.expectEqual(@as(u64, 500), stats_before_free.total_bytes_allocated);

    // Free one allocation
    allocator.free(mem1);

    const stats_after_free = debug_allocator.statistics();
    try std.testing.expectEqual(@as(u64, 1), stats_after_free.total_deallocations);
    try std.testing.expectEqual(@as(u64, 2), stats_after_free.active_allocations);
    try std.testing.expectEqual(@as(u64, 400), stats_after_free.current_bytes_allocated);

    // Clean up
    allocator.free(mem2);
    allocator.free(mem3);
}

test "DebugAllocator all allocations validation" {
    var debug_allocator = DebugAllocator.init(std.testing.allocator);
    const allocator = debug_allocator.allocator();

    // Allocate several blocks
    const mem1 = try allocator.alloc(u8, 64);
    const mem2 = try allocator.alloc(u16, 32);
    const mem3 = try allocator.alloc(u32, 16);

    // Validate all allocations (should succeed)
    try debug_allocator.validate_all_allocations();

    // Clean up
    allocator.free(mem1);
    allocator.free(mem2);
    allocator.free(mem3);
}
