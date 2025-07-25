const std = @import("std");
const testing = std.testing;
const custom_assert = @import("src/core/assert.zig");
const assert = custom_assert.assert;

/// Minimal test to isolate ArrayList corruption issues observed in VFS simulation
/// This reproduces the exact allocation patterns that cause corruption in WAL recovery

test "ArrayList large allocation corruption test" {
    var gpa = std.heap.GeneralPurposeAllocator(.{ .safety = true }){};
    defer {
        const deinit_status = gpa.deinit();
        if (deinit_status == .leak) @panic("Memory leak detected");
    }
    const allocator = gpa.allocator();

    std.debug.print("\n=== ArrayList Corruption Debug Test ===\n", .{});

    // Test 1: Basic large allocation
    {
        std.debug.print("Test 1: Basic large allocation\n", .{});
        var list = std.ArrayList(u8).init(allocator);
        defer list.deinit();

        const target_size = 1024 * 1024; // 1MB like the failing test
        try list.ensureTotalCapacity(target_size);
        list.items.len = target_size;

        // Fill with known pattern
        @memset(list.items, 0xAB);

        // Verify integrity
        for (list.items, 0..) |byte, i| {
            if (byte != 0xAB) {
                std.debug.print("CORRUPTION at index {}: expected 0xAB, got 0x{X}\n", .{ i, byte });
                return error.CorruptionDetected;
            }
        }
        std.debug.print("Test 1: PASSED - no corruption detected\n", .{});
    }

    // Test 2: Incremental expansion like VFS write operations
    {
        std.debug.print("Test 2: Incremental expansion\n", .{});
        var list = std.ArrayList(u8).init(allocator);
        defer list.deinit();

        const chunks = [_]usize{ 100, 1000, 10000, 100000, 1000000 };
        var total_size: usize = 0;

        for (chunks) |chunk_size| {
            const old_len = list.items.len;
            const
