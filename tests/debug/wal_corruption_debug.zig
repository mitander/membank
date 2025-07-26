//! Minimal test to isolate WAL corruption in simulation VFS.
//!
//! This test creates the simplest possible WAL write/read cycle to identify
//! where memory corruption occurs in the simulation filesystem.

const cortexdb = @import("cortexdb");
const std = @import("std");
const testing = std.testing;
const log = std.log.scoped(.debug_wal);

const vfs = cortexdb.vfs;
const sim = cortexdb.simulation_vfs;
const storage = cortexdb.storage;
const context_block = cortexdb.types;

const VFS = vfs.VFS;
const SimulationVFS = sim.SimulationVFS;
const WAL = storage.WAL;
const WALEntry = storage.WALEntry;
const ContextBlock = context_block.ContextBlock;
const BlockId = context_block.BlockId;

test "minimal WAL corruption isolation" {
    const allocator = testing.allocator;

    log.info("=== Starting minimal WAL corruption test ===", .{});

    // Create simulation VFS with no fault injection
    var sim_vfs = try SimulationVFS.init(allocator);
    defer sim_vfs.deinit();
    const vfs_interface = sim_vfs.vfs();

    log.info("Created simulation VFS", .{});

    // Create test block with minimal data
    const test_block = ContextBlock{
        .id = BlockId.from_hex("0123456789abcdeffedcba9876543210") catch unreachable,
        .version = 1,
        .source_uri = "test://minimal",
        .metadata_json = "{}",
        .content = "test",
    };

    log.info("Created test block: id={}, content_len={}", .{ test_block.id, test_block.content.len });

    // Create WAL entry
    var entry = try WALEntry.create_put_block(test_block, allocator);
    defer entry.deinit(allocator);

    log.info("Created WAL entry: type={}, payload_size={}, checksum=0x{x}", .{ entry.entry_type, entry.payload_size, entry.checksum });

    // Test 1: Direct serialization/deserialization (no VFS)
    {
        log.info("--- Test 1: Direct serialization roundtrip ---", .{});

        const serialized_size = WALEntry.HEADER_SIZE + entry.payload.len;
        const buffer = try allocator.alloc(u8, serialized_size);
        defer allocator.free(buffer);

        // Serialize entry
        const bytes_written = try entry.serialize(buffer);
        log.info("Serialized entry: bytes_written={}", .{bytes_written});

        // Log first few bytes for corruption detection
        log.info("Serialized bytes: {x:0>2} {x:0>2} {x:0>2} {x:0>2} {x:0>2} {x:0>2} {x:0>2} {x:0>2} {x:0>2} {x:0>2} {x:0>2} {x:0>2} {x:0>2}", .{ buffer[0], buffer[1], buffer[2], buffer[3], buffer[4], buffer[5], buffer[6], buffer[7], buffer[8], buffer[9], buffer[10], buffer[11], buffer[12] });

        // Deserialize entry
        var deserialized = try WALEntry.deserialize(buffer, allocator);
        defer deserialized.deinit(allocator);

        log.info("Deserialized entry: type={}, payload_size={}, checksum=0x{x}", .{ deserialized.entry_type, deserialized.payload_size, deserialized.checksum });

        // Verify integrity
        try testing.expectEqual(entry.checksum, deserialized.checksum);
        try testing.expectEqual(entry.entry_type, deserialized.entry_type);
        try testing.expectEqual(entry.payload_size, deserialized.payload_size);

        log.info("✓ Direct serialization test passed", .{});
    }

    // Test 2: VFS file write/read without WAL
    {
        log.info("--- Test 2: Raw VFS write/read ---", .{});

        const test_file_path = "/test_file.dat";

        // Write data to VFS
        {
            var file = try vfs_interface.create(test_file_path);
            defer file.close();
            defer file.deinit();

            const serialized_size = WALEntry.HEADER_SIZE + entry.payload.len;
            const buffer = try allocator.alloc(u8, serialized_size);
            defer allocator.free(buffer);

            const bytes_written = try entry.serialize(buffer);
            log.info("Writing {} bytes to VFS file", .{bytes_written});

            const written = try file.write(buffer);
            try testing.expectEqual(bytes_written, written);

            log.info("Wrote and synced {} bytes", .{written});
        }

        // Read data back from VFS
        {
            var file = try vfs_interface.open(test_file_path, .read);
            defer file.close();
            defer file.deinit();

            const file_size = try file.seek(0, .end);
            _ = try file.seek(0, .start);

            log.info("File size on disk: {}", .{file_size});

            const buffer = try allocator.alloc(u8, @intCast(file_size));
            defer allocator.free(buffer);

            const bytes_read = try file.read(buffer);
            log.info("Read {} bytes from VFS file", .{bytes_read});

            // Log first few bytes for corruption detection
            if (bytes_read >= 13) {
                log.info("Read bytes: {x:0>2} {x:0>2} {x:0>2} {x:0>2} {x:0>2} {x:0>2} {x:0>2} {x:0>2} {x:0>2} {x:0>2} {x:0>2} {x:0>2} {x:0>2}", .{ buffer[0], buffer[1], buffer[2], buffer[3], buffer[4], buffer[5], buffer[6], buffer[7], buffer[8], buffer[9], buffer[10], buffer[11], buffer[12] });

                // Check payload size for corruption
                const payload_size = std.mem.readInt(u32, buffer[9..13], .little);
                log.info("Payload size from file: {}", .{payload_size});

                if (payload_size > 16 * 1024 * 1024) {
                    log.err("CORRUPTION DETECTED: payload_size {} > MAX_PAYLOAD_SIZE {}", .{ payload_size, 16 * 1024 * 1024 });
                    return error.CorruptionDetected;
                }
            }

            // Deserialize and verify
            var deserialized = try WALEntry.deserialize(buffer, allocator);
            defer deserialized.deinit(allocator);

            log.info("Deserialized from file: type={}, payload_size={}, checksum=0x{x}", .{ deserialized.entry_type, deserialized.payload_size, deserialized.checksum });

            try testing.expectEqual(entry.checksum, deserialized.checksum);
            try testing.expectEqual(entry.entry_type, deserialized.entry_type);
            try testing.expectEqual(entry.payload_size, deserialized.payload_size);
        }

        log.info("✓ VFS write/read test passed", .{});
    }

    // Test 3: Full WAL write/recovery cycle
    {
        log.info("--- Test 3: Full WAL write/recovery ---", .{});

        const wal_dir = "/wal_test";

        // Initialize WAL and write entry
        {
            var test_wal = try WAL.init(allocator, vfs_interface, wal_dir);
            defer test_wal.deinit();
            try test_wal.startup();

            log.info("Initialized WAL in directory: {s}", .{wal_dir});

            try test_wal.write_entry(entry);
            log.info("Wrote entry to WAL", .{});
        }

        // Recovery callback to capture recovered entries
        const RecoveryContext = struct {
            recovered_entries: std.ArrayList(WALEntry),
            allocator: std.mem.Allocator,
        };

        var recovery_ctx = RecoveryContext{
            .recovered_entries = std.ArrayList(WALEntry).init(allocator),
            .allocator = allocator,
        };
        defer {
            for (recovery_ctx.recovered_entries.items) |*recovered_entry| {
                recovered_entry.deinit(allocator);
            }
            recovery_ctx.recovered_entries.deinit();
        }

        const recovery_callback = struct {
            fn callback(recovered_entry: WALEntry, context: *anyopaque) !void {
                const ctx: *RecoveryContext = @ptrCast(@alignCast(context));

                log.info("Recovery callback: type={}, payload_size={}, checksum=0x{x}", .{ recovered_entry.entry_type, recovered_entry.payload_size, recovered_entry.checksum });

                // Clone the entry for storage
                const payload_copy = try ctx.allocator.dupe(u8, recovered_entry.payload);
                const cloned_entry = WALEntry{
                    .checksum = recovered_entry.checksum,
                    .entry_type = recovered_entry.entry_type,
                    .payload_size = recovered_entry.payload_size,
                    .payload = payload_copy,
                };

                try ctx.recovered_entries.append(cloned_entry);
            }
        }.callback;

        // Perform recovery
        {
            var recovery_wal = try WAL.init(allocator, vfs_interface, wal_dir);
            defer recovery_wal.deinit();
            try recovery_wal.startup();

            log.info("Starting WAL recovery...", .{});
            try recovery_wal.recover_entries(recovery_callback, &recovery_ctx);
            log.info("Recovery completed. Recovered {} entries", .{recovery_ctx.recovered_entries.items.len});
        }

        // Verify recovered entries
        try testing.expectEqual(@as(usize, 1), recovery_ctx.recovered_entries.items.len);

        const recovered = recovery_ctx.recovered_entries.items[0];
        try testing.expectEqual(entry.checksum, recovered.checksum);
        try testing.expectEqual(entry.entry_type, recovered.entry_type);
        try testing.expectEqual(entry.payload_size, recovered.payload_size);
        try testing.expectEqualSlices(u8, entry.payload, recovered.payload);

        log.info("✓ WAL recovery test passed", .{});
    }

    log.info("=== All tests passed ===", .{});
}

test "VFS large write corruption isolation" {
    const allocator = testing.allocator;

    log.info("=== Starting VFS large write corruption test ===", .{});

    // Create simulation VFS with no fault injection
    var sim_vfs = try SimulationVFS.init(allocator);
    defer sim_vfs.deinit();
    const vfs_interface = sim_vfs.vfs();

    // Create large buffer (1MB like the failing test)
    const large_size = 1024 * 1024;
    const large_buffer = try allocator.alloc(u8, large_size);
    defer allocator.free(large_buffer);

    // Fill with 'x' pattern like the failing test
    @memset(large_buffer, 'x');

    // Create WAL-like header pattern to detect corruption
    var wal_header_pattern: [13]u8 = undefined;
    // Simulate a WAL header: checksum(8) + type(1) + payload_size(4)
    std.mem.writeInt(u64, wal_header_pattern[0..8], 0x1234567890ABCDEF, .little);
    wal_header_pattern[8] = 0x01; // entry type
    std.mem.writeInt(u32, wal_header_pattern[9..13], @intCast(large_size), .little);

    log.info("WAL header pattern: {x:0>2} {x:0>2} {x:0>2} {x:0>2} {x:0>2} {x:0>2} {x:0>2} {x:0>2} {x:0>2} {x:0>2} {x:0>2} {x:0>2} {x:0>2}", .{ wal_header_pattern[0], wal_header_pattern[1], wal_header_pattern[2], wal_header_pattern[3], wal_header_pattern[4], wal_header_pattern[5], wal_header_pattern[6], wal_header_pattern[7], wal_header_pattern[8], wal_header_pattern[9], wal_header_pattern[10], wal_header_pattern[11], wal_header_pattern[12] });

    const test_file_path = "/large_test.dat";

    // Test 1: Write header then large content (simulating WAL write pattern)
    {
        log.info("--- Test 1: Header + Large Content Write ---", .{});

        var file = try vfs_interface.create(test_file_path);
        defer file.close();
        defer file.deinit();

        // Write header first
        const header_written = try file.write(&wal_header_pattern);
        try testing.expectEqual(@as(usize, 13), header_written);
        log.info("Wrote header: {} bytes", .{header_written});

        // Write large content
        const content_written = try file.write(large_buffer);
        try testing.expectEqual(large_size, content_written);
        log.info("Wrote large content: {} bytes", .{content_written});

        // Verify file size
        const file_size = try file.file_size();
        const expected_size = 13 + large_size;
        try testing.expectEqual(@as(u64, expected_size), file_size);
        log.info("File size: {} (expected: {})", .{ file_size, expected_size });
    }

    // Test 2: Read back and verify no corruption
    {
        log.info("--- Test 2: Read and Verify ---", .{});

        var file = try vfs_interface.open(test_file_path, .read);
        defer file.close();
        defer file.deinit();

        // Read header back
        var read_header: [13]u8 = undefined;
        const header_read = try file.read(&read_header);
        try testing.expectEqual(@as(usize, 13), header_read);

        log.info("Read header: {x:0>2} {x:0>2} {x:0>2} {x:0>2} {x:0>2} {x:0>2} {x:0>2} {x:0>2} {x:0>2} {x:0>2} {x:0>2} {x:0>2} {x:0>2}", .{ read_header[0], read_header[1], read_header[2], read_header[3], read_header[4], read_header[5], read_header[6], read_header[7], read_header[8], read_header[9], read_header[10], read_header[11], read_header[12] });

        // Check if header was corrupted
        if (!std.mem.eql(u8, &wal_header_pattern, &read_header)) {
            log.err("HEADER CORRUPTION DETECTED!", .{});

            // Check payload size specifically
            const original_payload_size = std.mem.readInt(u32, wal_header_pattern[9..13], .little);
            const read_payload_size = std.mem.readInt(u32, read_header[9..13], .little);

            log.err("Original payload size: {}", .{original_payload_size});
            log.err("Read payload size: {}", .{read_payload_size});

            if (read_payload_size == 0x78787878) {
                log.err("CONFIRMED: Header overwritten with 'x' pattern (0x78787878)", .{});
            }

            return error.CorruptionDetected;
        }

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

        log.info("✓ VFS large write test passed - no corruption detected", .{});
    }

    // Test 3: Single large write (different pattern)
    {
        log.info("--- Test 3: Single Large Write ---", .{});

        const single_file_path = "/single_large.dat";
        var file = try vfs_interface.create(single_file_path);
        defer file.close();
        defer file.deinit();

        // Create combined buffer (header + content)
        const combined_size = 13 + large_size;
        const combined_buffer = try allocator.alloc(u8, combined_size);
        defer allocator.free(combined_buffer);

        @memcpy(combined_buffer[0..13], &wal_header_pattern);
        @memcpy(combined_buffer[13..], large_buffer);

        // Single large write
        const written = try file.write(combined_buffer);
        try testing.expectEqual(combined_size, written);
        log.info("Single write: {} bytes", .{written});

        // Read back header immediately
        _ = try file.seek(0, .start);
        var verify_header: [13]u8 = undefined;
        const header_read = try file.read(&verify_header);
        try testing.expectEqual(@as(usize, 13), header_read);

        if (!std.mem.eql(u8, &wal_header_pattern, &verify_header)) {
            log.err("SINGLE WRITE CORRUPTION DETECTED!", .{});
            const read_payload_size = std.mem.readInt(u32, verify_header[9..13], .little);
            log.err("Corrupted payload size: 0x{x}", .{read_payload_size});
            return error.CorruptionDetected;
        }

        log.info("✓ Single large write test passed", .{});
    }

    log.info("=== VFS corruption tests completed successfully ===", .{});
}

test "small block WAL corruption test" {
    const allocator = testing.allocator;

    log.info("=== Starting small block WAL corruption test ===", .{});

    // Create simulation VFS with no fault injection
    var sim_vfs = try SimulationVFS.init(allocator);
    defer sim_vfs.deinit();
    const vfs_interface = sim_vfs.vfs();

    // Create small content (1KB instead of 1MB)
    const small_size = 1024;
    const small_content = try allocator.alloc(u8, small_size);
    defer allocator.free(small_content);
    @memset(small_content, 'x');

    const small_block = ContextBlock{
        .id = BlockId.from_hex("abcdef0123456789abcdef0123456789") catch unreachable,
        .version = 1,
        .source_uri = "test://small.zig",
        .metadata_json = "{\"size\":\"small\"}",
        .content = small_content,
    };

    log.info("Created small block: content_len={}", .{small_block.content.len});

    // Create WAL entry
    var entry = try WALEntry.create_put_block(small_block, allocator);
    defer entry.deinit(allocator);

    log.info("Created WAL entry: type={}, payload_size={}, checksum=0x{x}", .{ entry.entry_type, entry.payload_size, entry.checksum });

    const wal_dir = "/wal_small_test";

    // Initialize WAL and write entry
    {
        var test_wal = try WAL.init(allocator, vfs_interface, wal_dir);
        defer test_wal.deinit();
        try test_wal.startup();

        log.info("Initialized WAL in directory: {s}", .{wal_dir});

        try test_wal.write_entry(entry);
        log.info("Wrote small block entry to WAL", .{});
    }

    // Recovery callback to capture recovered entries
    const RecoveryContext = struct {
        recovered_entries: std.ArrayList(WALEntry),
        allocator: std.mem.Allocator,
    };

    var recovery_ctx = RecoveryContext{
        .recovered_entries = std.ArrayList(WALEntry).init(allocator),
        .allocator = allocator,
    };
    defer {
        for (recovery_ctx.recovered_entries.items) |*recovered_entry| {
            recovered_entry.deinit(allocator);
        }
        recovery_ctx.recovered_entries.deinit();
    }

    const recovery_callback = struct {
        fn callback(recovered_entry: WALEntry, context: *anyopaque) !void {
            const ctx: *RecoveryContext = @ptrCast(@alignCast(context));

            log.info("Recovery callback: type={}, payload_size={}, checksum=0x{x}", .{ recovered_entry.entry_type, recovered_entry.payload_size, recovered_entry.checksum });

            // Clone the entry for storage
            const payload_copy = try ctx.allocator.dupe(u8, recovered_entry.payload);
            const cloned_entry = WALEntry{
                .checksum = recovered_entry.checksum,
                .entry_type = recovered_entry.entry_type,
                .payload_size = recovered_entry.payload_size,
                .payload = payload_copy,
            };

            try ctx.recovered_entries.append(cloned_entry);
        }
    }.callback;

    // Perform recovery
    {
        var recovery_wal = try WAL.init(allocator, vfs_interface, wal_dir);
        defer recovery_wal.deinit();
        try recovery_wal.startup();

        log.info("Starting small block WAL recovery...", .{});
        try recovery_wal.recover_entries(recovery_callback, &recovery_ctx);
        log.info("Recovery completed. Recovered {} entries", .{recovery_ctx.recovered_entries.items.len});
    }

    // Verify recovered entries
    try testing.expectEqual(@as(usize, 1), recovery_ctx.recovered_entries.items.len);

    const recovered = recovery_ctx.recovered_entries.items[0];
    try testing.expectEqual(entry.checksum, recovered.checksum);
    try testing.expectEqual(entry.entry_type, recovered.entry_type);
    try testing.expectEqual(entry.payload_size, recovered.payload_size);
    try testing.expectEqualSlices(u8, entry.payload, recovered.payload);

    log.info("✓ Small block WAL test passed", .{});
}

test "trace WAL corruption step by step" {
    const allocator = testing.allocator;

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
            log.info("Block header after serialization: magic=0x{X}, version={}", .{ magic, version });
        }

        log.info("✓ ContextBlock serialization completed successfully", .{});
    }

    // Test 2: WALEntry creation in isolation
    {
        log.info("--- Step 3: Testing WALEntry creation in isolation ---", .{});

        var entry = try WALEntry.create_put_block(test_block, allocator);
        defer entry.deinit(allocator);

        log.info("WAL entry created: type={}, payload_size={}, checksum=0x{X}", .{ entry.entry_type, entry.payload_size, entry.checksum });

        // Verify entry payload integrity
        if (entry.payload.len >= 64) {
            const magic = std.mem.readInt(u32, entry.payload[0..4], .little);
            const version = std.mem.readInt(u16, entry.payload[4..6], .little);
            log.info("Entry payload header: magic=0x{X}, version={}", .{ magic, version });
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

        log.info("WAL header after serialization: checksum=0x{X}, type={}, payload_size={}", .{ wal_checksum, wal_type, wal_payload_size });

        // Check for corruption pattern
        if (wal_payload_size == 0x78787878) {
            log.err("CORRUPTION DETECTED: WAL payload_size corrupted to 0x78787878 ('xxxx') during serialization!", .{});
            return error.CorruptionDetected;
        }

        if (wal_payload_size != entry.payload_size) {
            log.err("CORRUPTION DETECTED: WAL payload_size mismatch - expected {}, got {}", .{ entry.payload_size, wal_payload_size });
            return error.CorruptionDetected;
        }

        log.info("✓ WALEntry serialization completed successfully", .{});
    }

    log.info("=== All corruption trace tests passed - no corruption detected ===", .{});
}

test "medium block WAL corruption test" {
    const allocator = testing.allocator;

    log.info("=== Starting medium block WAL corruption test ===", .{});

    // Create simulation VFS with no fault injection
    var sim_vfs = try SimulationVFS.init(allocator);
    defer sim_vfs.deinit();
    const vfs_interface = sim_vfs.vfs();

    // Create medium content (128KB)
    const medium_size = 128 * 1024;
    const medium_content = try allocator.alloc(u8, medium_size);
    defer allocator.free(medium_content);
    @memset(medium_content, 'x');

    const medium_block = ContextBlock{
        .id = BlockId.from_hex("abcdef0123456789abcdef0123456789") catch unreachable,
        .version = 1,
        .source_uri = "test://medium.zig",
        .metadata_json = "{\"size\":\"medium\"}",
        .content = medium_content,
    };

    log.info("Created medium block: content_len={}", .{medium_block.content.len});

    // Create WAL entry
    var entry = try WALEntry.create_put_block(medium_block, allocator);
    defer entry.deinit(allocator);

    log.info("Created WAL entry: type={}, payload_size={}, checksum=0x{x}", .{ entry.entry_type, entry.payload_size, entry.checksum });

    const wal_dir = "/wal_medium_test";

    // Initialize WAL and write entry
    {
        var test_wal = try WAL.init(allocator, vfs_interface, wal_dir);
        defer test_wal.deinit();
        try test_wal.startup();

        log.info("Initialized WAL in directory: {s}", .{wal_dir});

        try test_wal.write_entry(entry);
        log.info("Wrote medium block entry to WAL", .{});
    }

    // Recovery callback to capture recovered entries
    const RecoveryContext = struct {
        recovered_entries: std.ArrayList(WALEntry),
        allocator: std.mem.Allocator,
    };

    var recovery_ctx = RecoveryContext{
        .recovered_entries = std.ArrayList(WALEntry).init(allocator),
        .allocator = allocator,
    };
    defer {
        for (recovery_ctx.recovered_entries.items) |*recovered_entry| {
            recovered_entry.deinit(allocator);
        }
        recovery_ctx.recovered_entries.deinit();
    }

    const recovery_callback = struct {
        fn callback(recovered_entry: WALEntry, context: *anyopaque) !void {
            const ctx: *RecoveryContext = @ptrCast(@alignCast(context));

            log.info("Recovery callback: type={}, payload_size={}, checksum=0x{x}", .{ recovered_entry.entry_type, recovered_entry.payload_size, recovered_entry.checksum });

            // Clone the entry for storage
            const payload_copy = try ctx.allocator.dupe(u8, recovered_entry.payload);
            const cloned_entry = WALEntry{
                .checksum = recovered_entry.checksum,
                .entry_type = recovered_entry.entry_type,
                .payload_size = recovered_entry.payload_size,
                .payload = payload_copy,
            };

            try ctx.recovered_entries.append(cloned_entry);
        }
    }.callback;

    // Perform recovery
    {
        var recovery_wal = try WAL.init(allocator, vfs_interface, wal_dir);
        defer recovery_wal.deinit();
        try recovery_wal.startup();

        log.info("Starting medium block WAL recovery...", .{});
        try recovery_wal.recover_entries(recovery_callback, &recovery_ctx);
        log.info("Recovery completed. Recovered {} entries", .{recovery_ctx.recovered_entries.items.len});
    }

    // Verify recovered entries
    try testing.expectEqual(@as(usize, 1), recovery_ctx.recovered_entries.items.len);

    const recovered = recovery_ctx.recovered_entries.items[0];
    try testing.expectEqual(entry.checksum, recovered.checksum);
    try testing.expectEqual(entry.entry_type, recovered.entry_type);
    try testing.expectEqual(entry.payload_size, recovered.payload_size);
    try testing.expectEqualSlices(u8, entry.payload, recovered.payload);

    log.info("✓ Medium block WAL test passed", .{});
}
