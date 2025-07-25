//! Debug file position during WAL recovery to identify why we're reading from wrong offset

const std = @import("std");
const testing = std.testing;
const log = std.log.scoped(.debug_position);

const vfs = @import("vfs");
const sim = @import("simulation_vfs");
const storage = @import("storage");
const context_block = @import("context_block");

const VFS = vfs.VFS;
const SimulationVFS = sim.SimulationVFS;
const WAL = storage.WAL;
const WALEntry = storage.WALEntry;
const ContextBlock = context_block.ContextBlock;
const BlockId = context_block.BlockId;

test "debug file position during WAL operations" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const allocator = arena.allocator();

    log.info("=== File Position Debug Test ===", .{});

    // Create simulation VFS
    var sim_vfs = try SimulationVFS.init(allocator);
    defer sim_vfs.deinit();
    const vfs_interface = sim_vfs.vfs();

    const test_file_path = "/position_debug.wal";

    // Create medium content (128KB)
    const medium_size = 128 * 1024;
    const medium_content = try allocator.alloc(u8, medium_size);
    defer allocator.free(medium_content);
    @memset(medium_content, 'x');

    const test_block = ContextBlock{
        .id = BlockId.from_hex("abcdef0123456789abcdef0123456789") catch unreachable,
        .version = 1,
        .source_uri = "test://position.zig",
        .metadata_json = "{\"test\":\"position\"}",
        .content = medium_content,
    };

    // Create WAL entry and serialize it directly
    var entry = try WALEntry.create_put_block(test_block, allocator);
    defer entry.deinit(allocator);

    log.info("Created WAL entry: payload_size={}, total_size={}", .{ 
        entry.payload_size, WALEntry.HEADER_SIZE + entry.payload_size 
    });

    // Step 1: Write entry manually using VFS
    var expected_file_size: u64 = undefined;
    {
        var file = try vfs_interface.create(test_file_path);
        defer file.close();
        defer file.deinit();

        // Write WAL header
        var wal_header: [13]u8 = undefined;
        std.mem.writeInt(u64, wal_header[0..8], entry.checksum, .little);
        wal_header[8] = @intFromEnum(entry.entry_type);
        std.mem.writeInt(u32, wal_header[9..13], entry.payload_size, .little);

        std.debug.print("WAL header before write: checksum=0x{X}, type={}, size={}\n", .{
            entry.checksum, @intFromEnum(entry.entry_type), entry.payload_size
        });
        std.debug.print("WAL header bytes: ", .{});
        for (wal_header, 0..) |byte, i| {
            std.debug.print("{}:0x{X} ", .{i, byte});
        }
        std.debug.print("\n", .{});

        const header_written = try file.write(&wal_header);
        try testing.expectEqual(@as(usize, 13), header_written);
        log.info("Wrote WAL header: {} bytes", .{header_written});

        // Write payload
        const payload_written = try file.write(entry.payload);
        try testing.expectEqual(entry.payload_size, payload_written);
        log.info("Wrote WAL payload: {} bytes", .{payload_written});

        expected_file_size = try file.file_size();
        log.info("Expected file size: {}", .{expected_file_size});
    }

    // Step 2: Open file for reading and check initial position
    {
        var file = try vfs_interface.open(test_file_path, .read);
        defer file.close();
        defer file.deinit();

        const file_size = try file.file_size();
        const initial_position = try file.tell();
        log.info("File opened: size={}, initial_position={}", .{ file_size, initial_position });

        try testing.expectEqual(expected_file_size, file_size);
        try testing.expectEqual(@as(u64, 0), initial_position);

        // Read first 64 bytes and examine content
        var first_bytes: [64]u8 = undefined;
        const bytes_read = try file.read(&first_bytes);
        log.info("First read: {} bytes", .{bytes_read});

        log.info("First 64 bytes: ", .{});
        for (first_bytes[0..bytes_read], 0..) |byte, i| {
            std.debug.print("{}:0x{X} ", .{ i, byte });
            if (i > 0 and (i + 1) % 16 == 0) std.debug.print("\n", .{});
        }
        if (bytes_read % 16 != 0) std.debug.print("\n", .{});

        // Check if these are the expected WAL header bytes
        const expected_checksum = entry.checksum;
        const actual_checksum = std.mem.readInt(u64, first_bytes[0..8], .little);
        const expected_type = @intFromEnum(entry.entry_type);
        const actual_type = first_bytes[8];
        const expected_payload_size = entry.payload_size;
        const actual_payload_size = std.mem.readInt(u32, first_bytes[9..13], .little);
        
        std.debug.print("Read verification:\n", .{});
        std.debug.print("  Expected checksum: 0x{X}\n", .{expected_checksum});
        std.debug.print("  Read checksum: 0x{X}\n", .{actual_checksum});
        std.debug.print("  Expected payload size: {}\n", .{expected_payload_size});
        std.debug.print("  Read payload size: {}\n", .{actual_payload_size});

        log.info("Header comparison:", .{});
        log.info("  Checksum: expected=0x{X}, actual=0x{X}, match={}", .{ 
            expected_checksum, actual_checksum, expected_checksum == actual_checksum 
        });
        log.info("  Type: expected={}, actual={}, match={}", .{ 
            expected_type, actual_type, expected_type == actual_type 
        });
        log.info("  Payload size: expected={}, actual={}, match={}", .{ 
            expected_payload_size, actual_payload_size, expected_payload_size == actual_payload_size 
        });

        if (actual_checksum != expected_checksum or 
            actual_type != expected_type or 
            actual_payload_size != expected_payload_size) {
            log.err("CORRUPTION: File content does not match expected WAL header!", .{});
            return error.CorruptionDetected;
        }

        log.info("✓ WAL header verification passed", .{});

        // Seek back to beginning and verify position management
        const new_position = try file.seek(0, .start);
        try testing.expectEqual(@as(u64, 0), new_position);
        log.info("After seek(0): position={}", .{new_position});

        // Read again to verify consistency
        var second_read: [64]u8 = undefined;
        const bytes_read_2 = try file.read(&second_read);
        try testing.expectEqual(bytes_read, bytes_read_2);

        if (!std.mem.eql(u8, first_bytes[0..bytes_read], second_read[0..bytes_read_2])) {
            log.err("CORRUPTION: Second read gives different results!", .{});
            return error.CorruptionDetected;
        }

        log.info("✓ Second read consistency verified", .{});
    }

    log.info("=== File position debug test completed successfully ===", .{});
}