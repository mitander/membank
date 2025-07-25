//! Minimal WAL processing test to isolate the buffer issue

const std = @import("std");
const testing = std.testing;

test "minimal WAL entry processing test" {
    _ = testing.allocator;

    // Known good WAL header bytes from successful write operation
    const wal_bytes = [_]u8{
        0xA6, 0xF8, 0x23, 0xCE, 0xF3, 0xF2, 0x33, 0xF4, // checksum (8 bytes)
        0x1,                                               // entry type (1 byte)
        0x62, 0x0, 0x2, 0x0,                              // payload size: 131170 (4 bytes)
        // Total: 13 bytes WAL header
    };

    std.debug.print("WAL header bytes: ", .{});
    for (wal_bytes, 0..) |byte, i| {
        std.debug.print("{}:0x{X} ", .{ i, byte });
    }
    std.debug.print("\n", .{});

    // Test payload size calculation
    const payload_size = std.mem.readInt(u32, wal_bytes[9..13], .little);
    std.debug.print("Payload size calculation: bytes[9..13] = [0x{X}, 0x{X}, 0x{X}, 0x{X}]\n", .{
        wal_bytes[9], wal_bytes[10], wal_bytes[11], wal_bytes[12]
    });
    std.debug.print("Calculated payload_size: {} (0x{X})\n", .{ payload_size, payload_size });

    // Verify this is reasonable
    const MAX_PAYLOAD_SIZE: u32 = 16 * 1024 * 1024;
    const is_valid = payload_size <= MAX_PAYLOAD_SIZE;
    std.debug.print("Is valid payload size: {} (max: {})\n", .{ is_valid, MAX_PAYLOAD_SIZE });

    try testing.expect(is_valid);
    try testing.expectEqual(@as(u32, 131170), payload_size);

    std.debug.print("✓ Minimal WAL header processing test passed\n", .{});
}