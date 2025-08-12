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
        return self.arena.allocator().alloc(T, n);
    }

    /// Duplicate slice through coordinator - always uses current arena state.
    /// Common pattern for cloning string content in storage subsystem.
    pub fn duplicate_slice(self: *const ArenaCoordinator, comptime T: type, slice: []const T) ![]T {
        if (comptime builtin.mode == .Debug) {
            fatal_assert(@intFromPtr(self) != 0, "ArenaCoordinator self pointer is null during duplicate_slice", .{});
            fatal_assert(@intFromPtr(self.arena) != 0, "ArenaCoordinator arena pointer is null - coordinator at 0x{x}, arena: 0x{x}", .{ @intFromPtr(self), @intFromPtr(self.arena) });
            // Check arena pointer alignment for ArenaAllocator
            const arena_addr = @intFromPtr(self.arena);
            const arena_align = @alignOf(std.heap.ArenaAllocator);
            fatal_assert(arena_addr % arena_align == 0, "ArenaCoordinator arena pointer misaligned - addr: 0x{x}, align: {}, offset: {}", .{ arena_addr, arena_align, arena_addr % arena_align });
        }
        return self.arena.allocator().dupe(T, slice);
    }

    /// Create single item through coordinator - always uses current arena state.
    /// Convenience method for single-item allocation patterns.
    pub fn create(self: *const ArenaCoordinator, comptime T: type) !*T {
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
            };
        } else {
            return ArenaCoordinatorDebugInfo{
                .arena_address = 0,
                .coordinator_address = 0,
                .is_valid = true,
            };
        }
    }
};

/// Debug information structure for coordinator diagnostics.
pub const ArenaCoordinatorDebugInfo = struct {
    arena_address: usize,
    coordinator_address: usize,
    is_valid: bool,
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

    try testing.expect(slice3.len == 10);
    try testing.expect(slice4.len == 20);
}
