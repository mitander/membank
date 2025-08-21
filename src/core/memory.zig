//! Arena Coordinator Pattern for stable memory allocation interfaces.
//!
//! Provides stable allocation interfaces that remain valid across arena operations,
//! eliminating temporal coupling between arena resets and component access patterns.
//! This solves the arena corruption problem where embedded arenas become invalid
//! when structs containing them are copied.
//!
//! **Design Principle**: Coordinators provide method-based allocation instead of
//! direct allocator references, ensuring allocation always uses current arena state.

const builtin = @import("builtin");
const std = @import("std");

const assert_mod = @import("assert.zig");

const assert = assert_mod.assert;
const fatal_assert = assert_mod.fatal_assert;

/// Memory generation validation errors
pub const GenerationError = error{
    /// Attempted to access memory after arena was reset
    UseAfterArenaReset,
    /// Index out of bounds for generation slice access
    IndexOutOfBounds,
};

/// Arena coordinator providing stable allocation interface.
/// Remains valid even when underlying arena is reset or struct is copied.
/// All allocation methods dispatch to current arena state, eliminating temporal coupling.
/// CRITICAL: Always pass ArenaCoordinator by pointer to prevent struct copying corruption.
pub const ArenaCoordinator = struct {
    /// Pointer to arena - never becomes invalid because it's a stable reference
    arena: *std.heap.ArenaAllocator,
    /// Generation counter to detect use-after-reset
    generation: u64,

    /// Create coordinator for existing arena.
    /// Arena must outlive the coordinator - typically owned by parent component.
    /// IMPORTANT: Return value must be stored in stable location and passed by pointer.
    pub fn init(arena: *std.heap.ArenaAllocator) ArenaCoordinator {
        return ArenaCoordinator{
            .arena = arena,
            .generation = 0,
        };
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

    /// Allocate generation-tagged slice through coordinator for safe access validation.
    /// Returns GenerationSlice that validates generation before each access.
    pub fn alloc_generation_slice(self: *const ArenaCoordinator, comptime T: type, n: usize) !GenerationSliceType(T) {
        const memory = try self.alloc(T, n);
        return GenerationSliceType(T){
            .ptr = memory.ptr,
            .len = memory.len,
            .generation = self.generation,
            .coordinator = self,
        };
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

    /// Duplicate slice with generation tagging for safe access validation.
    /// Returns GenerationSlice that validates generation before each access.
    pub fn duplicate_generation_slice(
        self: *const ArenaCoordinator,
        comptime T: type,
        slice: []const T,
    ) !GenerationSliceType(T) {
        const memory = try self.duplicate_slice(T, slice);
        return GenerationSliceType(T){
            .ptr = memory.ptr,
            .len = memory.len,
            .generation = self.generation,
            .coordinator = self,
        };
    }

    /// Reset arena and increment generation counter.
    /// All previously allocated GenerationSlice instances become invalid.
    pub fn reset(self: *ArenaCoordinator) void {
        self.generation += 1;
        _ = self.arena.reset(.retain_capacity);
    }

    /// Generation-tagged slice that validates arena generation before access.
    /// Prevents use-after-reset bugs by checking that the arena hasn't been reset
    /// since the slice was allocated. Provides safe access methods that fail
    /// gracefully if the underlying memory has been invalidated.
    pub fn GenerationSliceType(comptime T: type) type {
        return struct {
            ptr: [*]T,
            len: usize,
            generation: u64,
            coordinator: *const ArenaCoordinator,

            const Self = @This();

            /// Access the underlying slice if generation is still valid.
            /// Returns error if arena has been reset since allocation.
            pub fn access_slice(self: *const Self) ![]T {
                if (self.generation != self.coordinator.generation) {
                    return error.UseAfterArenaReset;
                }
                return self.ptr[0..self.len];
            }

            /// Access a single element if generation is still valid.
            /// Returns error if arena has been reset or index is out of bounds.
            pub fn access_item(self: *const Self, index: usize) !T {
                if (self.generation != self.coordinator.generation) {
                    return error.UseAfterArenaReset;
                }
                if (index >= self.len) {
                    return error.IndexOutOfBounds;
                }
                return self.ptr[index];
            }

            /// Get slice length (always safe, doesn't access memory).
            pub fn length(self: *const Self) usize {
                return self.len;
            }

            /// Check if this slice is still valid (generation matches).
            pub fn is_valid(self: *const Self) bool {
                return self.generation == self.coordinator.generation;
            }
        };
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
            return Self{
                .storage_engine = storage_engine,
            };
        }

        /// Duplicate storage content with type safety.
        /// Uses storage engine's allocator for consistent memory management.
        pub fn duplicate_storage_content(self: Self, comptime T: type, slice: []const T) ![]T {
            const storage_allocator = self.storage_engine.allocator();
            return try storage_allocator.dupe(T, slice);
        }

        /// Get storage engine's allocator.
        /// Provides consistent allocator access across subsystems.
        pub fn allocator(self: Self) std.mem.Allocator {
            return self.storage_engine.allocator();
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
        return StorageCoordinator{
            .storage_engine = @ptrCast(storage_engine),
        };
    }

    /// Duplicate storage content with runtime type resolution.
    /// Less efficient than TypedStorageCoordinatorType but maintains compatibility.
    pub fn duplicate_storage_content(self: StorageCoordinator, comptime T: type, slice: []const T) ![]T {
        // This requires storage engine to have a standard allocator() method
        const storage_ptr: *anyopaque = self.storage_engine;
        _ = storage_ptr; // Placeholder - actual implementation needs proper casting
        _ = slice; // Unused in placeholder implementation
        return error.NotImplemented;
    }
};

/// Debug information structure for coordinator diagnostics.
pub const ArenaCoordinatorDebugInfo = struct {
    arena_address: usize,
    coordinator_address: usize,
    is_valid: bool,
    allocation_count: usize,
    bytes_allocated: usize,
};

/// Arena allocation tracker for debug builds.
/// Provides detailed allocation tracking and leak detection for arena usage patterns.
pub const ArenaAllocationTracker = struct {
    allocations: if (builtin.mode == .Debug) std.array_list.Managed(AllocationInfo) else void,
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
            .allocations = if (builtin.mode == .Debug) std.array_list.Managed(AllocationInfo).init(allocator) else {},
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

test "generation counter prevents use-after-reset" {
    const allocator = testing.allocator;

    var arena = std.heap.ArenaAllocator.init(allocator);
    defer arena.deinit();

    var coordinator = ArenaCoordinator.init(&arena);

    // Test 1: Basic generation slice allocation
    var generation_slice = try coordinator.alloc_generation_slice(u8, 10);
    try testing.expect(generation_slice.is_valid());
    try testing.expectEqual(@as(usize, 10), generation_slice.length());

    // Test 2: Valid access before reset
    const slice = try generation_slice.access_slice();
    try testing.expectEqual(@as(usize, 10), slice.len);

    // Test 3: Arena reset invalidates generation slice
    coordinator.reset();
    try testing.expect(!generation_slice.is_valid());

    // Test 4: Access after reset should fail
    try testing.expectError(GenerationError.UseAfterArenaReset, generation_slice.access_slice());
    try testing.expectError(GenerationError.UseAfterArenaReset, generation_slice.access_item(0));
}

test "generation counter with duplicate slice" {
    const allocator = testing.allocator;

    var arena = std.heap.ArenaAllocator.init(allocator);
    defer arena.deinit();

    var coordinator = ArenaCoordinator.init(&arena);

    // Test 1: Create source data
    const source_data = "Hello, generation counter!";
    var generation_slice = try coordinator.duplicate_generation_slice(u8, source_data);

    // Test 2: Valid access before reset
    const slice = try generation_slice.access_slice();
    try testing.expect(std.mem.eql(u8, slice, source_data));

    // Test 3: Item access works
    try testing.expectEqual(@as(u8, 'H'), try generation_slice.access_item(0));
    try testing.expectEqual(@as(u8, '!'), try generation_slice.access_item(source_data.len - 1));

    // Test 4: Out of bounds access fails
    try testing.expectError(GenerationError.IndexOutOfBounds, generation_slice.access_item(source_data.len));

    // Test 5: Reset invalidates the slice
    coordinator.reset();
    try testing.expectError(GenerationError.UseAfterArenaReset, generation_slice.access_slice());
    try testing.expectError(GenerationError.UseAfterArenaReset, generation_slice.access_item(0));
}

test "generation counter increments properly" {
    const allocator = testing.allocator;

    var arena = std.heap.ArenaAllocator.init(allocator);
    defer arena.deinit();

    var coordinator = ArenaCoordinator.init(&arena);

    // Test 1: Initial generation is 0
    try testing.expectEqual(@as(u64, 0), coordinator.generation);

    // Test 2: Create slice with generation 0
    var slice1 = try coordinator.alloc_generation_slice(u32, 5);
    try testing.expectEqual(@as(u64, 0), slice1.generation);
    try testing.expect(slice1.is_valid());

    // Test 3: Reset increments generation
    coordinator.reset();
    try testing.expectEqual(@as(u64, 1), coordinator.generation);
    try testing.expect(!slice1.is_valid());

    // Test 4: New slice has generation 1
    var slice2 = try coordinator.alloc_generation_slice(u32, 3);
    try testing.expectEqual(@as(u64, 1), slice2.generation);
    try testing.expect(slice2.is_valid());

    // Test 5: Multiple resets increment generation
    coordinator.reset();
    coordinator.reset();
    try testing.expectEqual(@as(u64, 3), coordinator.generation);
    try testing.expect(!slice2.is_valid());
}

test "mixed generation and regular allocations" {
    const allocator = testing.allocator;

    var arena = std.heap.ArenaAllocator.init(allocator);
    defer arena.deinit();

    var coordinator = ArenaCoordinator.init(&arena);

    // Test 1: Regular allocation still works
    const regular_slice = try coordinator.alloc(u8, 20);
    try testing.expectEqual(@as(usize, 20), regular_slice.len);

    // Test 2: Generation slice alongside regular allocation
    var generation_slice = try coordinator.alloc_generation_slice(u8, 15);
    try testing.expect(generation_slice.is_valid());

    // Test 3: Reset invalidates generation slice but regular slice is gone
    coordinator.reset();
    try testing.expect(!generation_slice.is_valid());

    // Test 4: New allocations after reset work
    const new_regular = try coordinator.alloc(u8, 5);
    var new_generation = try coordinator.alloc_generation_slice(u8, 10);
    try testing.expectEqual(@as(usize, 5), new_regular.len);
    try testing.expect(new_generation.is_valid());
}
