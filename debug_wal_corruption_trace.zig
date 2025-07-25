//! Trace-level debugging to isolate exact point of WAL corruption.

const std = @import("std");
const testing = std.testing;
const log = std.log.scoped(.debug_trace);

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

test "trace WAL corruption step by step" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const allocator = arena.allocator();

    log.info("=== WAL Corruption Step-by-Step Trace ===", .{});

    // Create large content (128KB of 'x')
    const large_size = 128 * 1024;
    const large_content = try allocator.alloc(u8, large_size);
    defer allocator.free(large_content);
    @memset(large_content, 'x');

    const test_block = ContextBlock{
        .id = BlockId.from_hex("abcdef0123456789abcdef0123456789") catch unreachable,
        .version = 1,
        .source_uri = "test://large.zig",
        .metadata_json = "{\"size\":\"large\"}",
        .content = large_content,
    };

    log.info("Step 1: Created test block with large content ({} bytes)", .{large_content.len});

    // Test 1: ContextBlock serialization in isolation
    {
        log.info("--- Step 2: Testing ContextBlock serialization in isolation ---", .{});
        
        const block_serialized_size = test_block.serialized_size();
        log.info("Block serialized size: {}", .{block_serialized_size});
        
        const block_buffer = try allocator.alloc(u8, block_serialized_size);
        defer allocator.free(block_buffer);
        
        // Fill with sentinel pattern to detect overwrites
        @memset(block_buffer, 0xAA);
        
        const block_bytes_written = try test_block.serialize(block_buffer);
        log.info("Block serialization wrote {} bytes", .{block_bytes_written});
        
        // Check header integrity after block serialization
        if (block_buffer.len >= 64) {
            const magic = std.mem.readInt(u32, block_buffer[0..4], .little);
            const version = std.mem.readInt(u16, block_buffer[4..6], .little);
            log.info("Block header after serialization: magic=0x{X}, version={}", .{magic, version});
        }
        
        log.info("✓ ContextBlock serialization completed successfully", .{});
    }

    // Test 2: WALEntry creation in isolation
    {
        log.info("--- Step 3: Testing WALEntry creation in isolation ---", .{});
        
        var entry = try WALEntry.create_put_block(test_block, allocator);
        defer entry.deinit(allocator);
        
        log.info("WAL entry created: type={}, payload_size={}, checksum=0x{X}", .{
            entry.entry_type, entry.payload_size, entry.checksum
        });
        
        // Verify entry payload integrity
        if (entry.payload.len >= 64) {
            const magic = std.mem.readInt(u32, entry.payload[0..4], .little);
            const version = std.mem.readInt(u16, entry.payload[4..6], .little);
            log.info("Entry payload header: magic=0x{X}, version={}", .{magic, version});
        }
        
        // Test 3: WALEntry serialization in isolation
        log.info("--- Step 4: Testing WALEntry serialization in isolation ---", .{});
        
        const entry_serialized_size = WALEntry.HEADER_SIZE + entry.payload.len;
        log.info("Entry serialization size: {}", .{entry_serialized_size});
        
        const entry_buffer = try allocator.alloc(u8, entry_serialized_size);
        defer allocator.free(entry_buffer);
        
        // Fill with different sentinel pattern
        @memset(entry_buffer, 0xBB);
        
        const entry_bytes_written = try entry.serialize(entry_buffer);
        log.info("Entry serialization wrote {} bytes", .{entry_bytes_written});
        
        // Check WAL header after serialization
        const wal_checksum = std.mem.readInt(u64, entry_buffer[0..8], .little);
        const wal_type = entry_buffer[8];
        const wal_payload_size = std.mem.readInt(u32, entry_buffer[9..13], .little);
        
        log.info("WAL header after serialization: checksum=0x{X}, type={}, payload_size={}", .{
            wal_checksum, wal_type, wal_payload_size
        });
        
        // Check for corruption pattern
        if (wal_payload_size == 0x78787878) {
            log.err("CORRUPTION DETECTED: WAL payload_size corrupted to 0x78787878 ('xxxx') during serialization!", .{});
            return error.CorruptionDetected;
        }
        
        if (wal_payload_size != entry.payload_size) {
            log.err("CORRUPTION DETECTED: WAL payload_size mismatch - expected {}, got {}", .{
                entry.payload_size, wal_payload_size
            });
            return error.CorruptionDetected;
        }
        
        log.info("✓ WALEntry serialization completed successfully", .{});
    }

    // Test 4: Multiple arena usage pattern (simulating WAL.write_entry)
    {
        log.info("--- Step 5: Testing multiple arena usage pattern ---", .{});
        
        // Create separate arenas like in WAL.write_entry
        var block_arena = std.heap.ArenaAllocator.init(allocator);
        defer block_arena.deinit();
        const block_allocator = block_arena.allocator();
        
        var entry_arena = std.heap.ArenaAllocator.init(allocator);
        defer entry_arena.deinit();
        const entry_allocator = entry_arena.allocator();
        
        var buffer_arena = std.heap.ArenaAllocator.init(allocator);
        defer buffer_arena.deinit();
        const buffer_allocator = buffer_arena.allocator();
        
        // Create entry with first arena
        var entry = try WALEntry.create_put_block(test_block, entry_allocator);
        defer entry.deinit(entry_allocator);
        
        log.info("Entry created with separate arena: type={}, payload_size={}", .{
            entry.entry_type, entry.payload_size
        });
        
        // Serialize with buffer arena (like in WAL.write_entry)
        const serialized_size = WALEntry.HEADER_SIZE + entry.payload.len;
        const buffer = try buffer_allocator.alloc(u8, serialized_size);
        @memset(buffer, 0); // Zero-initialize like in WAL code
        
        const bytes_written = try entry.serialize(buffer);
        log.info("Multi-arena serialization wrote {} bytes", .{bytes_written});
        
        // Check for corruption after multi-arena serialization
        const final_checksum = std.mem.readInt(u64, buffer[0..8], .little);
        const final_type = buffer[8];
        const final_payload_size = std.mem.readInt(u32, buffer[9..13], .little);
        
        log.info("Final WAL header: checksum=0x{X}, type={}, payload_size={}", .{
            final_checksum, final_type, final_payload_size
        });
        
        if (final_payload_size == 0x78787878) {
            log.err("CORRUPTION DETECTED: Multi-arena usage corrupted payload_size to 0x78787878!", .{});
            return error.CorruptionDetected;
        }
        
        if (final_payload_size != entry.payload_size) {
            log.err("CORRUPTION DETECTED: Multi-arena payload_size mismatch - expected {}, got {}", .{
                entry.payload_size, final_payload_size
            });
            return error.CorruptionDetected;
        }
        
        log.info("✓ Multi-arena serialization test passed", .{});
    }

    log.info("=== All corruption trace tests passed - no corruption detected ===", .{});
}