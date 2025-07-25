//! Direct VFS write/read test to isolate corruption

const std = @import("std");
const testing = std.testing;
const log = std.log.scoped(.debug_vfs);

const vfs = @import("vfs");
const sim = @import("simulation_vfs");

test "direct VFS large write corruption test" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const allocator = arena.allocator();

    log.info("=== Direct VFS Large Write Corruption Test ===", .{});

    // Create simulation VFS
    var sim_vfs = try sim.SimulationVFS.init(allocator);
    defer sim_vfs.deinit();
    const vfs_interface = sim_vfs.vfs();

    const test_file_path = "/large_write_test.dat";

    // Create WAL-like header (13 bytes)
    var wal_header: [13]u8 = undefined;
    std.mem.writeInt(u64, wal_header[0..8], 0x1234567890ABCDEF, .little); // checksum
    wal_header[8] = 0x01; // entry type
    std.mem.writeInt(u32, wal_header[9..13], 128 * 1024, .little); // payload size

    // Create large content (128KB of 'x')
    const large_size = 128 * 1024;
    const large_content = try allocator.alloc(u8, large_size);
    defer allocator.free(large_content);
    @memset(large_content, 'x');

    log.info("Header pattern: {x:0>2} {x:0>2} {x:0>2} {x:0>2} {x:0>2} {x:0>2} {x:0>2} {x:0>2} {x:0>2} {x:0>2} {x:0>2} {x:0>2} {x:0>2}", .{
        wal_header[0], wal_header[1], wal_header[2], wal_header[3], wal_header[4],
        wal_header[5], wal_header[6], wal_header[7], wal_header[8], wal_header[9],
        wal_header[10], wal_header[11], wal_header[12]
    });

    // Test 1: Write header + content as separate operations
    {
        log.info("--- Test 1: Separate header and content writes ---", .{});

        var file = try vfs_interface.create(test_file_path);
        defer file.close();
        defer file.deinit();

        // Write header first
        const header_written = try file.write(&wal_header);
        try testing.expectEqual(@as(usize, 13), header_written);
        log.info("Wrote header: {} bytes", .{header_written});

        // Write content
        const content_written = try file.write(large_content);
        try testing.expectEqual(large_size, content_written);
        log.info("Wrote content: {} bytes", .{content_written});

        // Verify total file size
        const file_size = try file.file_size();
        try testing.expectEqual(@as(u64, 13 + large_size), file_size);
        log.info("File size: {}", .{file_size});
    }

    // Test 2: Read back and verify header integrity
    {
        log.info("--- Test 2: Read and verify header ---", .{});

        var file = try vfs_interface.open(test_file_path, .read);
        defer file.close();
        defer file.deinit();

        // Read header back
        var read_header: [13]u8 = undefined;
        const header_read = try file.read(&read_header);
        try testing.expectEqual(@as(usize, 13), header_read);

        log.info("Read header: {x:0>2} {x:0>2} {x:0>2} {x:0>2} {x:0>2} {x:0>2} {x:0>2} {x:0>2} {x:0>2} {x:0>2} {x:0>2} {x:0>2} {x:0>2}", .{
            read_header[0], read_header[1], read_header[2], read_header[3], read_header[4],
            read_header[5], read_header[6], read_header[7], read_header[8], read_header[9],
            read_header[10], read_header[11], read_header[12]
        });

        // Check if header was corrupted
        if (!std.mem.eql(u8, &wal_header, &read_header)) {
            log.err("HEADER CORRUPTION DETECTED!", .{});

            // Check specific fields
            const original_checksum = std.mem.readInt(u64, wal_header[0..8], .little);
            const read_checksum = std.mem.readInt(u64, read_header[0..8], .little);
            log.err("Checksum: original=0x{X}, read=0x{X}", .{ original_checksum, read_checksum });

            const original_type = wal_header[8];
            const read_type = read_header[8];
            log.err("Type: original={}, read={}", .{ original_type, read_type });

            const original_payload_size = std.mem.readInt(u32, wal_header[9..13], .little);
            const read_payload_size = std.mem.readInt(u32, read_header[9..13], .little);
            log.err("Payload size: original={}, read={}", .{ original_payload_size, read_payload_size });

            if (read_payload_size == 0x78787878) {
                log.err("CONFIRMED: Header overwritten with 'x' pattern (0x78787878)", .{});
            }

            return error.CorruptionDetected;
        }

        log.info("✓ Header integrity verified", .{});

        // Read some content to verify it's correct
        var content_sample: [1024]u8 = undefined;
        const content_read = try file.read(&content_sample);
        try testing.expectEqual(@as(usize, 1024), content_read);

        // Verify content is all 'x'
        for (content_sample) |byte| {
            if (byte != 'x') {
                log.err("Content corruption: expected 'x' (0x78), got 0x{x}", .{byte});
                return error.CorruptionDetected;
            }
        }

        log.info("✓ Content integrity verified", .{});
    }

    log.info("=== Direct VFS test completed successfully ===", .{});
}