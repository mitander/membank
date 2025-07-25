//! Test WAL write using WAL code but read using direct VFS to isolate write vs read corruption

const std = @import("std");
const testing = std.testing;
const log = std.log.scoped(.debug_wal_write_read);

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

test "WAL write then direct VFS read corruption test" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const allocator = arena.allocator();

    log.info("=== WAL Write -> Direct VFS Read Corruption Test ===", .{});

    // Create simulation VFS
    var sim_vfs = try SimulationVFS.init(allocator);
    defer sim_vfs.deinit();
    const vfs_interface = sim_vfs.vfs();

    // Create large content (128KB of 'x')
    const large_size = 128 * 1024;
    const large_content = try allocator.alloc(u8, large_size);
    defer allocator.free(large_content);
    @memset(large_content, 'x');

    const test_block = ContextBlock{
        .id = BlockId.from_hex("abcdef0123456789abcdef0123456789") catch unreachable,
        .version = 1,
        .source_uri = "test://write_read.zig",
        .metadata_json = "{\"test\":\"write_read\"}",
        .content = large_content,
    };

    log.info("Created test block with {} bytes of content", .{large_content.len});

    // Create WAL entry  
    var entry = try WALEntry.create_put_block(test_block, allocator);
    defer entry.deinit(allocator);

    log.info("Created WAL entry: type={}, payload_size={}, checksum=0x{X}", .{
        entry.entry_type, entry.payload_size, entry.checksum
    });

    const wal_dir = "/wal_write_read_test";

    // Step 1: Write using WAL infrastructure
    {
        log.info("--- Step 1: Writing using WAL infrastructure ---", .{});
        
        var test_wal = try WAL.init(allocator, vfs_interface, wal_dir);
        defer test_wal.deinit();

        try test_wal.write_entry(entry);
        log.info("WAL write completed", .{});
    }

    // Step 2: Read back using direct VFS to check if corruption happened during write
    {
        log.info("--- Step 2: Reading back using direct VFS ---", .{});
        
        const wal_file_path = "/wal_write_read_test/wal_0000.log";
        
        var file = try vfs_interface.open(wal_file_path, .read);
        defer file.close();
        defer file.deinit();

        const file_size = try file.file_size();
        log.info("WAL file size: {}", .{file_size});

        // Read the WAL header directly (first 13 bytes)
        var wal_header: [13]u8 = undefined;
        const header_read = try file.read(&wal_header);
        try testing.expectEqual(@as(usize, 13), header_read);

        log.info("WAL header bytes: {x:0>2} {x:0>2} {x:0>2} {x:0>2} {x:0>2} {x:0>2} {x:0>2} {x:0>2} {x:0>2} {x:0>2} {x:0>2} {x:0>2} {x:0>2}", .{
            wal_header[0], wal_header[1], wal_header[2], wal_header[3], wal_header[4],
            wal_header[5], wal_header[6], wal_header[7], wal_header[8], wal_header[9],
            wal_header[10], wal_header[11], wal_header[12]
        });

        // Parse header fields
        const checksum = std.mem.readInt(u64, wal_header[0..8], .little);
        const entry_type = wal_header[8];
        const payload_size = std.mem.readInt(u32, wal_header[9..13], .little);

        log.info("Parsed header: checksum=0x{X}, type={}, payload_size={}", .{
            checksum, entry_type, payload_size
        });

        // Check for corruption patterns
        if (checksum == 0x7878787878787878) {
            log.err("CORRUPTION: Checksum corrupted with 'x' pattern!", .{});
            return error.CorruptionDetected;
        }

        if (entry_type == 0x78) {
            log.err("CORRUPTION: Entry type corrupted with 'x' pattern!", .{});
            return error.CorruptionDetected;
        }

        if (payload_size == 0x78787878) {
            log.err("CORRUPTION: Payload size corrupted with 'x' pattern (0x78787878)!", .{});
            return error.CorruptionDetected;
        }

        if (payload_size > 16 * 1024 * 1024) {
            log.err("CORRUPTION: Invalid payload size {} > 16MB", .{payload_size});
            return error.CorruptionDetected;
        }

        // Verify the values match what we expect
        if (checksum != entry.checksum) {
            log.err("CORRUPTION: Checksum mismatch - expected 0x{X}, got 0x{X}", .{
                entry.checksum, checksum
            });
            return error.CorruptionDetected;
        }

        if (entry_type != @intFromEnum(entry.entry_type)) {
            log.err("CORRUPTION: Entry type mismatch - expected {}, got {}", .{
                @intFromEnum(entry.entry_type), entry_type
            });
            return error.CorruptionDetected;
        }

        if (payload_size != entry.payload_size) {
            log.err("CORRUPTION: Payload size mismatch - expected {}, got {}", .{
                entry.payload_size, payload_size
            });
            return error.CorruptionDetected;
        }

        log.info("✓ WAL header integrity verified - no corruption during write", .{});

        // Read some payload data to verify it starts correctly
        var payload_sample: [64]u8 = undefined;
        const payload_read = try file.read(&payload_sample);
        try testing.expectEqual(@as(usize, 64), payload_read);

        // The payload should start with the ContextBlock header
        const block_magic = std.mem.readInt(u32, payload_sample[0..4], .little);
        const expected_magic = 0x42444358; // "XDBC" in little endian
        
        if (block_magic != expected_magic) {
            log.err("CORRUPTION: Payload header corrupted - expected magic 0x{X}, got 0x{X}", .{
                expected_magic, block_magic
            });
            return error.CorruptionDetected;
        }

        log.info("✓ Payload header integrity verified", .{});
    }

    log.info("=== WAL write integrity test completed successfully ===", .{});
}