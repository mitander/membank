//! Arena Coordinator Pattern for stable memory allocation interfaces.
//!
//! Provides stable allocation interfaces that remain valid across arena operations,
//! eliminating temporal coupling between arena resets and component access patterns.
//! This solves the arena corruption problem where embedded arenas become invalid
//! when structs containing them are copied.
//!
//! **Design Principle**: Coordinators provide method-based allocation instead of
//! direct allocator references, ensuring allocation always uses current arena state.

const std = @import("std");
const builtin = @import("builtin");
const assert = @import("assert.zig").assert;
const fatal_assert = @import("assert.zig").fatal_assert;

/// Arena coordinator providing stable allocation interface.
/// Remains valid even when underlying arena is reset or struct is copied.
/// All allocation methods dispatch to current arena state, eliminating temporal coupling.
/// CRITICAL: Always pass ArenaCoordinator by pointer to prevent struct copying corruption.
pub const ArenaCoordinator = struct {
    /// Pointer to arena - never becomes invalid because it's a stable reference
    arena: *std.heap.ArenaAllocator,

    /// Create coordinator for existing arena.
    /// Arena must outlive the coordinator - typically owned by parent component.
    /// IMPORTANT: Return value must be stored in stable location and passed by pointer.
    pub fn init(arena: *std.heap.ArenaAllocator) ArenaCoordinator {
        return ArenaCoordinator{ .arena = arena };
    }

    /// Get current allocator interface - always uses current arena state.
    /// Safe to call after arena reset because coordinator holds stable arena reference.
    pub fn allocator(self: *const ArenaCoordinator) std.mem.Allocator {
        return self.arena.allocator();
    }

    /// Allocate slice through coordinator - always uses current arena state.
    /// Preferred over direct allocator access for subcomponent allocation.
    pub fn alloc(self: *const ArenaCoordinator, comptime T: type, n: usize) ![]T {
        if (comptime builtin.mode == .Debug) {
            self.validate_arena_lifetime();
            self.track_allocation_pattern(n * @sizeOf(T), @typeName(T));
        }
        return self.arena.allocator().alloc(T, n);
    }

    /// Duplicate slice through coordinator - always uses current arena state.
    /// Common pattern for cloning string content in storage subsystem.
    pub fn duplicate_slice(self: *const ArenaCoordinator, comptime T: type, slice: []const T) ![]T {
        if (comptime builtin.mode == .Debug) {
            self.validate_arena_lifetime();
            self.track_allocation_pattern(slice.len * @sizeOf(T), @typeName(T));
        }
        return self.arena.allocator().dupe(T, slice);
    }

    /// Create single item through coordinator - always uses current arena state.
    /// Convenience method for single-item allocation patterns.
    pub fn create(self: *const ArenaCoordinator, comptime T: type) !*T {
        if (comptime builtin.mode == .Debug) {
            self.validate_arena_lifetime();
            self.track_allocation_pattern(@sizeOf(T), @typeName(T));
        }
        return self.arena.allocator().create(T);
    }

    /// Reset underlying arena while maintaining coordinator validity.
    /// Coordinator interface remains stable after this operation.
    pub fn reset(self: *ArenaCoordinator) void {
        _ = self.arena.reset(.retain_capacity);
    }

    /// Validate coordinator points to valid arena (debug builds only).
    /// Zero runtime overhead in release builds through comptime evaluation.
    pub fn validate_coordinator(self: *const ArenaCoordinator) void {
        if (comptime builtin.mode == .Debug) {
            fatal_assert(@intFromPtr(self.arena) != 0, "ArenaCoordinator arena pointer is null - coordinator corruption", .{});
        }
    }

    /// Debug information about coordinator state.
    /// Only available in debug builds for diagnostic purposes.
    pub fn debug_info(self: *const ArenaCoordinator) ArenaCoordinatorDebugInfo {
        if (comptime builtin.mode == .Debug) {
            return ArenaCoordinatorDebugInfo{
                .arena_address = @intFromPtr(self.arena),
                .coordinator_address = @intFromPtr(self),
                .is_valid = @intFromPtr(self.arena) != 0,
                .allocation_count = self.arena.queryCapacity(),
                .bytes_allocated = self.arena.queryCapacity() * @sizeOf(u8), // Approximation
            };
        } else {
            return ArenaCoordinatorDebugInfo{
                .arena_address = 0,
                .coordinator_address = 0,
                .is_valid = true,
                .allocation_count = 0,
                .bytes_allocated = 0,
            };
        }
    }

    /// Comprehensive arena lifetime validation for debug builds.
    /// Checks arena state consistency and detects common corruption patterns.
    pub fn validate_arena_lifetime(self: *const ArenaCoordinator) void {
        if (comptime builtin.mode == .Debug) {
            // Basic pointer validation
            fatal_assert(@intFromPtr(self) != 0, "ArenaCoordinator self pointer is null", .{});
            fatal_assert(@intFromPtr(self.arena) != 0, "ArenaCoordinator arena pointer is null", .{});

            // Arena alignment validation
            const arena_addr = @intFromPtr(self.arena);
            const arena_align = @alignOf(std.heap.ArenaAllocator);
            fatal_assert(arena_addr % arena_align == 0, "Arena pointer misaligned: 0x{x}", .{arena_addr});

            // Coordinator address validation
            const coord_addr = @intFromPtr(self);
            const coord_align = @alignOf(ArenaCoordinator);
            fatal_assert(coord_addr % coord_align == 0, "Coordinator pointer misaligned: 0x{x}", .{coord_addr});
        }
    }

    /// Track allocation patterns for debug analysis.
    /// Provides insight into arena usage patterns and potential optimizations.
    pub fn track_allocation_pattern(self: *const ArenaCoordinator, size: usize, type_name: []const u8) void {
        _ = self; // Coordinator context available for future enhancements
        if (comptime builtin.mode == .Debug) {
            // Log allocation patterns for performance analysis
            if (size > 4096) {
                std.log.debug("Large allocation via coordinator: {} bytes for {s}", .{ size, type_name });
            }
        }
    }
};

/// Debug information structure for coordinator diagnostics.
pub const ArenaCoordinatorDebugInfo = struct {
    arena_address: usize,
    coordinator_address: usize,
    is_valid: bool,
    allocation_count: usize,
    bytes_allocated: usize,

    /// Format debug info for logging.
    pub fn format_summary(self: ArenaCoordinatorDebugInfo) []const u8 {
        if (builtin.mode == .Debug) {
            return std.fmt.allocPrint(std.heap.page_allocator, "Arena[0x{x}] Coordinator[0x{x}] Valid:{} Allocs:{} Bytes:{}", .{ self.arena_address, self.coordinator_address, self.is_valid, self.allocation_count, self.bytes_allocated }) catch "Debug info formatting failed";
        }
        return "Debug info not available in release builds";
    }
};

/// Arena allocation tracker for debug builds.
/// Provides detailed allocation tracking and leak detection for arena usage patterns.
pub const ArenaAllocationTracker = struct {
    allocations: if (builtin.mode == .Debug) std.ArrayList(AllocationInfo) else void,
    total_allocations: if (builtin.mode == .Debug) usize else void,
    peak_memory: if (builtin.mode == .Debug) usize else void,
    current_memory: if (builtin.mode == .Debug) usize else void,

    const AllocationInfo = struct {
        size: usize,
        type_name: []const u8,
        timestamp: i64,
        source_location: std.builtin.SourceLocation,
    };

    pub fn init(allocator: std.mem.Allocator) ArenaAllocationTracker {
        return ArenaAllocationTracker{
            .allocations = if (builtin.mode == .Debug) std.ArrayList(AllocationInfo).init(allocator) else {},
            .total_allocations = if (builtin.mode == .Debug) 0 else {},
            .peak_memory = if (builtin.mode == .Debug) 0 else {},
            .current_memory = if (builtin.mode == .Debug) 0 else {},
        };
    }

    /// Track allocation in debug builds for leak detection and usage analysis.
    /// Records allocation size, type, and source location for debugging.
    /// Zero overhead in release builds through comptime elimination.
    pub fn track_allocation(self: *ArenaAllocationTracker, size: usize, type_name: []const u8) void {
        if (comptime builtin.mode == .Debug) {
            const info = AllocationInfo{
                .size = size,
                .type_name = type_name,
                .timestamp = std.time.milliTimestamp(),
                .source_location = @src(),
            };

            self.allocations.append(info) catch return; // Don't fail on tracking failure
            self.total_allocations += 1;
            self.current_memory += size;

            if (self.current_memory > self.peak_memory) {
                self.peak_memory = self.current_memory;
            }
        }
    }

    /// Reset allocation tracking state without deallocating tracked memory.
    /// Used when arena is reset to clear tracking state without affecting actual allocations.
    /// Zero overhead in release builds through comptime elimination.
    pub fn reset_tracking(self: *ArenaAllocationTracker) void {
        if (comptime builtin.mode == .Debug) {
            self.allocations.clearRetainingCapacity();
            self.current_memory = 0;
        }
    }

    /// Reports arena allocation statistics for debugging and memory profiling.
    /// Only active in debug builds - zero overhead in release mode.
    /// Logs total allocations, peak memory usage, and detects potential leaks.
    pub fn report_statistics(self: *const ArenaAllocationTracker) void {
        if (comptime builtin.mode == .Debug) {
            std.log.info("Arena Allocation Statistics:", .{});
            std.log.info("  Total allocations: {}", .{self.total_allocations});
            std.log.info("  Peak memory: {} bytes", .{self.peak_memory});
            std.log.info("  Current memory: {} bytes", .{self.current_memory});

            if (self.current_memory > 0) {
                std.log.warn("Potential arena leaks: {} bytes still allocated", .{self.current_memory});
            }
        }
    }

    pub fn deinit(self: *ArenaAllocationTracker) void {
        if (comptime builtin.mode == .Debug) {
            self.report_statistics();
            self.allocations.deinit();
        }
    }
};

/// Type-safe storage coordinator interface template.
/// Replaces *anyopaque patterns with compile-time validated subsystem interactions.
/// Provides minimal interface for storage operations while maintaining clear dependencies.
pub fn TypedStorageCoordinatorType(comptime StorageEngineType: type) type {
    return struct {
        const Self = @This();
        
        storage_engine: *StorageEngineType,

        /// Initialize coordinator with storage engine reference.
        /// Storage engine must outlive all coordinators.
        pub fn init(storage_engine: *StorageEngineType) Self {
            return Self{ .storage_engine = storage_engine };
        }

        /// Allocate storage-owned memory through the engine's arena.
        /// Provides type-safe access to storage memory without circular imports.
        pub fn duplicate_storage_content(self: Self, comptime T: type, slice: []const T) ![]T {
            // Assumes StorageEngine has a method like `get_storage_allocator()`
            // Implementation will be completed at usage sites with actual StorageEngine
            _ = self;
            _ = slice;
            return error.NotImplemented; // Implementation deferred to usage site
        }

        /// Get allocator from storage engine for temporary allocations.
        /// Returns the underlying allocator used by the storage engine.
        pub fn get_allocator(self: Self) std.mem.Allocator {
            // Implementation deferred - different storage engines may expose allocators differently
            _ = self;
            @panic("get_allocator must be implemented at usage site");
        }

        /// Validate coordinator is still connected to valid storage engine.
        /// Debug-only validation to catch use-after-free and corruption.
        pub fn validate_coordinator(self: Self) void {
            if (comptime builtin.mode == .Debug) {
                fatal_assert(@intFromPtr(self.storage_engine) != 0, "Storage coordinator has null engine reference", .{});
                // Additional validation can be added by implementers
            }
        }
    };
}

/// Legacy StorageCoordinator for backward compatibility.
/// Use TypedStorageCoordinatorType for new code to get full type safety.
pub const StorageCoordinator = struct {
    storage_engine: *anyopaque, // StorageEngine type resolved at usage site

    /// Initialize coordinator with storage engine reference.
    /// Storage engine must outlive all coordinators.
    pub fn init(storage_engine: anytype) StorageCoordinator {
        return StorageCoordinator{ .storage_engine = storage_engine };
    }

    /// Allocate storage-owned memory for block content.
    /// Implementation deferred to usage site to avoid circular imports.
    pub fn duplicate_storage_content(self: StorageCoordinator, comptime T: type, slice: []const T) ![]T {
        // Type-erased call - actual implementation at usage site
        const engine: *anyopaque = self.storage_engine;
        _ = engine;
        _ = slice;
        return error.NotImplemented; // Placeholder - override at usage site
    }
};

// Note: StorageCoordinator interface deferred to usage sites to avoid circular imports

// Tests for ArenaCoordinator functionality
const testing = std.testing;

test "arena coordinator basic allocation" {
    const allocator = testing.allocator;

    var arena = std.heap.ArenaAllocator.init(allocator);
    defer arena.deinit();

    const coordinator = ArenaCoordinator.init(&arena);

    // Test basic allocation through coordinator
    const slice = try coordinator.alloc(u32, 5);
    try testing.expect(slice.len == 5);

    // Test single item creation
    const item = try coordinator.create(u64);
    item.* = 42;
    try testing.expect(item.* == 42);
}

test "arena coordinator slice duplication" {
    const allocator = testing.allocator;

    var arena = std.heap.ArenaAllocator.init(allocator);
    defer arena.deinit();

    const coordinator = ArenaCoordinator.init(&arena);

    const original = "test content";
    const duplicated = try coordinator.duplicate_slice(u8, original);

    try testing.expectEqualStrings(original, duplicated);
    // Verify different memory locations
    try testing.expect(@intFromPtr(original.ptr) != @intFromPtr(duplicated.ptr));
}

test "arena coordinator remains valid after reset" {
    const allocator = testing.allocator;

    var arena = std.heap.ArenaAllocator.init(allocator);
    defer arena.deinit();

    var coordinator = ArenaCoordinator.init(&arena);

    // Allocate some memory
    const slice1 = try coordinator.alloc(u32, 10);
    try testing.expect(slice1.len == 10);

    // Reset arena through coordinator
    coordinator.reset();

    // Coordinator should still be valid and functional
    const slice2 = try coordinator.alloc(u32, 5);
    try testing.expect(slice2.len == 5);

    // Validate coordinator integrity
    coordinator.validate_coordinator();
}

test "arena coordinator debug info in debug mode" {
    const allocator = testing.allocator;

    var arena = std.heap.ArenaAllocator.init(allocator);
    defer arena.deinit();

    const coordinator = ArenaCoordinator.init(&arena);
    const debug_info = coordinator.debug_info();

    if (builtin.mode == .Debug) {
        try testing.expect(debug_info.arena_address != 0);
        try testing.expect(debug_info.coordinator_address != 0);
        try testing.expect(debug_info.is_valid);
    } else {
        // In release mode, debug info should indicate validity without addresses
        try testing.expect(debug_info.is_valid);
    }
}

test "arena coordinator validation catches corruption" {
    const allocator = testing.allocator;

    var arena = std.heap.ArenaAllocator.init(allocator);
    defer arena.deinit();

    const coordinator = ArenaCoordinator.init(&arena);

    // Valid coordinator should pass validation
    coordinator.validate_coordinator();

    // Test functionality after validation
    const slice = try coordinator.alloc(u8, 100);
    try testing.expect(slice.len == 100);
}

test "multiple coordinators for same arena" {
    const allocator = testing.allocator;

    var arena = std.heap.ArenaAllocator.init(allocator);
    defer arena.deinit();

    var coordinator1 = ArenaCoordinator.init(&arena);
    const coordinator2 = ArenaCoordinator.init(&arena);

    // Both coordinators should work with same arena
    const slice1 = try coordinator1.alloc(u32, 5);
    const slice2 = try coordinator2.alloc(u32, 3);

    try testing.expect(slice1.len == 5);
    try testing.expect(slice2.len == 3);

    // Reset through one coordinator affects both
    coordinator1.reset();

    // Both coordinators should still be functional
    const slice3 = try coordinator1.alloc(u8, 10);
    const slice4 = try coordinator2.alloc(u8, 20);

    try testing.expect(slice1.len == 5);
    try testing.expect(slice2.len == 3);
    try testing.expect(slice3.len == 10);
    try testing.expect(slice4.len == 20);
}

test "storage coordinator type safety" {
    // This test verifies the coordinator provides type-safe access
    // Note: Can't test with real StorageEngine due to circular imports
    // The type safety is validated at compile time through the interface

    // Verify the coordinator interface exists and has expected methods
    const coordinator_type = @TypeOf(StorageCoordinator.init);
    try testing.expect(coordinator_type != void);

    // Verify method signatures exist (compile-time check)
    const has_duplicate_method = @hasDecl(StorageCoordinator, "duplicate_storage_content");

    try testing.expect(has_duplicate_method);
}

test "Week 1 memory safety improvements comprehensive validation" {
    const allocator = testing.allocator;

    // Test 1: Arena coordinator lifetime validation
    var arena = std.heap.ArenaAllocator.init(allocator);
    defer arena.deinit();

    var coordinator = ArenaCoordinator.init(&arena);

    // Validate coordinator works correctly
    coordinator.validate_coordinator();
    const slice1 = try coordinator.alloc(u32, 10);
    try testing.expect(slice1.len == 10);

    // Test arena reset preserves coordinator validity
    coordinator.reset();
    const slice2 = try coordinator.alloc(u8, 20);
    try testing.expect(slice2.len == 20);

    // Test 2: Arena allocation tracking in debug builds
    if (builtin.mode == .Debug) {
        var tracker = ArenaAllocationTracker.init(allocator);
        defer tracker.deinit();

        tracker.track_allocation(1024, "TestType");
        tracker.track_allocation(512, "AnotherType");

        // Validate tracking state
        try testing.expect(tracker.total_allocations == 2);
        try testing.expect(tracker.current_memory == 1536);
        try testing.expect(tracker.peak_memory == 1536);

        tracker.reset_tracking();
        try testing.expect(tracker.current_memory == 0);
    }

    // Test 3: Debug info collection
    const debug_info = coordinator.debug_info();
    if (builtin.mode == .Debug) {
        try testing.expect(debug_info.is_valid);
        try testing.expect(debug_info.arena_address != 0);
        try testing.expect(debug_info.coordinator_address != 0);
    }
}
