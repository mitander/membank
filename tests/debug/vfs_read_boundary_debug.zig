//! Test VFS read behavior with exact WAL boundary conditions

const cortexdb = @import("cortexdb");
const std = @import("std");
const testing = std.testing;
const log = std.log.scoped(.debug_vfs_read);

const vfs = cortexdb.vfs;
const sim = cortexdb.simulation_vfs;

test "VFS read boundary conditions that trigger WAL corruption" {
    const allocator = testing.allocator;

    log.info("=== VFS Read Boundary Test ===", .{});

    // Create simulation VFS
    var sim_vfs = try sim.SimulationVFS.init(allocator);
    defer sim_vfs.deinit();
    const vfs_interface = sim_vfs.vfs();

    const test_file_path = "/boundary_test.dat";

    // Create a pattern that matches the corruption: WAL header + large ContextBlock
    const wal_header = [_]u8{
        0xEF, 0xCD, 0xAB, 0x90, 0x78, 0x56, 0x34, 0x12, // checksum
        0x01, // entry type
        0x00, 0x00, 0x02, 0x00, // payload size: 131072 (128KB)
    };

    // Create ContextBlock-like payload: 64-byte header + content
    const block_header_size = 64;
    const content_size = 128 * 1024;
    const total_payload_size = block_header_size + content_size;

    var payload = try allocator.alloc(u8, total_payload_size);
    defer allocator.free(payload);

    // Set up realistic ContextBlock header pattern
    payload[0..4].* = [_]u8{ 0x58, 0x44, 0x42, 0x43 }; // "CXDB" magic
    const source_uri = "test://medium.zig";
    @memcpy(payload[4 .. 4 + source_uri.len], source_uri); // source_uri field
    const metadata = "{\"size\":\"medium\"}";
    @memcpy(payload[36 .. 36 + metadata.len], metadata); // metadata_json field
    @memset(payload[64..], 'x'); // content filled with 'x'

    // Write the WAL entry
    {
        var file = try vfs_interface.create(test_file_path);
        defer file.close();
        defer file.deinit();

        const header_written = try file.write(&wal_header);
        try testing.expectEqual(@as(usize, 13), header_written);

        const payload_written = try file.write(payload);
        try testing.expectEqual(total_payload_size, payload_written);

        log.info("Written: {} byte header + {} byte payload", .{ header_written, payload_written });
    }

    // Read back using WAL-like chunked reading pattern
    {
        var file = try vfs_interface.open(test_file_path, .read);
        defer file.close();
        defer file.deinit();

        // Simulate WAL recovery reading pattern
        var read_buffer: [8192]u8 = undefined;
        var total_read: usize = 0;
        var chunk_num: u32 = 0;

        while (true) {
            const bytes_read = try file.read(&read_buffer);
            if (bytes_read == 0) break;

            chunk_num += 1;
            total_read += bytes_read;

            log.info("Chunk {}: read {} bytes, total {}", .{ chunk_num, bytes_read, total_read });

            // Check specific positions where corruption was detected
            if (chunk_num == 1) {
                // First chunk should contain WAL header + start of ContextBlock
                const header_checksum = std.mem.readInt(u64, read_buffer[0..8], .little);
                const header_type = read_buffer[8];
                const header_payload_size = std.mem.readInt(u32, read_buffer[9..13], .little);

                log.info("Chunk 1 header: checksum=0x{X}, type={}, payload_size={}", .{ header_checksum, header_type, header_payload_size });

                // Check the transition point where corruption was detected (around offset 103-110)
                if (bytes_read > 110) {
                    log.info("Transition area [100..120]: ", .{});
                    for (read_buffer[100..120], 100..) |byte, offset| {
                        std.debug.print("{}:0x{X} ", .{ offset, byte });
                    }
                    std.debug.print("\n", .{});

                    // Verify this matches what we expect
                    const expected_at_100 = if (100 < 13) wal_header[100] else payload[100 - 13];
                    if (read_buffer[100] != expected_at_100) {
                        log.err("CORRUPTION at offset 100: expected 0x{X}, got 0x{X}", .{ expected_at_100, read_buffer[100] });
                        return error.CorruptionDetected;
                    }
                }
            }

            // Check for corruption patterns in every chunk
            for (read_buffer[0..bytes_read], total_read - bytes_read..) |byte, global_offset| {
                const expected_byte = if (global_offset < 13)
                    wal_header[global_offset]
                else
                    payload[global_offset - 13];

                if (byte != expected_byte) {
                    log.err("CORRUPTION at global offset {}: expected 0x{X}, got 0x{X}", .{ global_offset, expected_byte, byte });
                    return error.CorruptionDetected;
                }
            }
        }

        log.info("✓ All {} bytes read correctly across {} chunks", .{ total_read, chunk_num });
    }

    log.info("=== VFS boundary test completed successfully ===", .{});
}
