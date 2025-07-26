//! WAL Hang Debug Tests
//!
//! Minimal debug tests to isolate hanging WAL operations.
//! Uses defensive programming patterns with timeouts and iteration limits.

const cortexdb = @import("cortexdb");
const std = @import("std");
const testing = std.testing;
const assert = cortexdb.assert.assert;

const simulation = cortexdb.simulation;
const context_block = cortexdb.types;
const storage = cortexdb.storage;
const vfs = cortexdb.vfs;

const Simulation = simulation.Simulation;
const ContextBlock = context_block.ContextBlock;
const BlockId = context_block.BlockId;
const StorageEngine = storage.StorageEngine;

// Defensive limits to prevent infinite loops
const MAX_TEST_DURATION_MS = 5000;
const MAX_DIRECTORY_ENTRIES = 1000;
const MAX_ITERATIONS = 100;

test "wal hang debug: minimal directory iteration" {
    const allocator = testing.allocator;

    // Test timeout mechanism
    const start_time = std.time.milliTimestamp();

    var sim = try Simulation.init(allocator, 12345);
    defer sim.deinit();

    const node = try sim.add_node();
    const node_ptr = sim.find_node(node);
    const sim_vfs = node_ptr.filesystem_interface();

    // Create minimal WAL directory structure
    const test_dir = "debug_wal_test";
    try sim_vfs.mkdir_all(test_dir);

    const wal_dir = try std.fmt.allocPrint(allocator, "{s}/wal", .{test_dir});
    defer allocator.free(wal_dir);
    try sim_vfs.mkdir_all(wal_dir);

    // Create a few test WAL files to iterate over
    const test_files = [_][]const u8{
        "wal_0000.log",
        "wal_0001.log",
        "wal_0002.log",
    };

    for (test_files) |filename| {
        const filepath = try std.fmt.allocPrint(allocator, "{s}/{s}", .{ wal_dir, filename });
        defer allocator.free(filepath);
        var file = try sim_vfs.create(filepath);
        defer file.close();
        _ = try file.write("test content");
        try file.flush();
    }

    // Test directory iteration with defensive patterns using arena
    var iter_arena = std.heap.ArenaAllocator.init(allocator);
    defer iter_arena.deinit();
    const iter_allocator = iter_arena.allocator();

    var dir_iterator = try sim_vfs.iterate_directory(wal_dir, iter_allocator);

    var entry_count: u32 = 0;
    var iteration_count: u32 = 0;

    while (dir_iterator.next()) |entry| {
        iteration_count += 1;

        // Defensive check: timeout
        const current_time = std.time.milliTimestamp();
        if (current_time - start_time > MAX_TEST_DURATION_MS) {
            std.debug.panic("Test timeout exceeded: {}ms", .{current_time - start_time});
        }

        // Defensive check: iteration limit
        if (iteration_count > MAX_ITERATIONS) {
            std.debug.panic("Iteration limit exceeded: {} iterations", .{iteration_count});
        }

        // Defensive check: entry count
        if (entry_count >= MAX_DIRECTORY_ENTRIES) {
            std.debug.panic("Directory entry limit exceeded: {} entries", .{entry_count});
        }

        // Validate entry data
        try testing.expect(entry.name.len > 0);
        try testing.expect(entry.name.len < 256);

        if (std.mem.startsWith(u8, entry.name, "wal_") and
            std.mem.endsWith(u8, entry.name, ".log"))
        {
            entry_count += 1;
        }
    }

    // Verify expected results
    try testing.expectEqual(@as(u32, 3), entry_count);
    try testing.expect(iteration_count <= MAX_ITERATIONS);

    const total_time = std.time.milliTimestamp() - start_time;
    try testing.expect(total_time < MAX_TEST_DURATION_MS);
}

test "wal hang debug: storage engine initialization" {
    const allocator = testing.allocator;

    const start_time = std.time.milliTimestamp();

    var sim = try Simulation.init(allocator, 54321);
    defer sim.deinit();

    const node = try sim.add_node();
    const node_ptr = sim.find_node(node);
    const sim_vfs = node_ptr.filesystem_interface();

    const data_dir = "debug_storage_init";

    // Initialize storage with timeout check
    var storage_engine = try StorageEngine.init_default(allocator, sim_vfs, data_dir);
    defer storage_engine.deinit();

    const init_time = std.time.milliTimestamp();
    if (init_time - start_time > MAX_TEST_DURATION_MS / 2) {
        std.debug.panic("Storage init timeout: {}ms", .{init_time - start_time});
    }

    try storage_engine.initialize_storage();

    const storage_init_time = std.time.milliTimestamp();
    if (storage_init_time - start_time > MAX_TEST_DURATION_MS) {
        std.debug.panic("Storage initialization timeout: {}ms", .{storage_init_time - start_time});
    }

    // Verify basic functionality
    try testing.expectEqual(@as(u32, 0), storage_engine.block_count());
}

test "wal hang debug: single block write and recovery" {
    // Use GPA with safety for memory corruption detection
    var gpa = std.heap.GeneralPurposeAllocator(.{ .safety = true }){};
    defer {
        const deinit_status = gpa.deinit();
        if (deinit_status == .leak) @panic("Memory leak detected");
    }
    const allocator = gpa.allocator();

    const start_time = std.time.milliTimestamp();

    var sim = try Simulation.init(allocator, 98765);
    defer sim.deinit();

    const node = try sim.add_node();
    const node_ptr = sim.find_node(node);
    const sim_vfs = node_ptr.filesystem_interface();

    const data_dir = "debug_single_block";

    // Write phase with timeout checks
    {
        var storage_engine = try StorageEngine.init_default(allocator, sim_vfs, data_dir);
        defer storage_engine.deinit();

        try storage_engine.initialize_storage();

        const write_start = std.time.milliTimestamp();
        if (write_start - start_time > MAX_TEST_DURATION_MS / 4) {
            std.debug.panic("Write phase setup timeout: {}ms", .{write_start - start_time});
        }

        const test_block = ContextBlock{
            .id = try BlockId.from_hex("00000000000000000000000000000001"),
            .version = 1,
            .source_uri = "test://debug.zig",
            .metadata_json = "{}",
            .content = "debug test content",
        };

        try storage_engine.put_block(test_block);

        const put_time = std.time.milliTimestamp();
        if (put_time - start_time > MAX_TEST_DURATION_MS / 2) {
            std.debug.panic("Put block timeout: {}ms", .{put_time - start_time});
        }

        try storage_engine.flush_wal();

        const flush_time = std.time.milliTimestamp();
        if (flush_time - start_time > MAX_TEST_DURATION_MS) {
            std.debug.panic("WAL flush timeout: {}ms", .{flush_time - start_time});
        }
    }

    // Recovery phase with timeout checks
    {
        var storage_engine = try StorageEngine.init_default(allocator, sim_vfs, data_dir);
        defer storage_engine.deinit();

        try storage_engine.initialize_storage();

        const recovery_start = std.time.milliTimestamp();
        if (recovery_start - start_time > MAX_TEST_DURATION_MS) {
            std.debug.panic("Recovery setup timeout: {}ms", .{recovery_start - start_time});
        }

        try storage_engine.recover_from_wal();

        const recovery_end = std.time.milliTimestamp();
        if (recovery_end - start_time > MAX_TEST_DURATION_MS * 2) {
            std.debug.panic("WAL recovery timeout: {}ms", .{recovery_end - start_time});
        }

        // Verify recovery worked
        try testing.expectEqual(@as(u32, 1), storage_engine.block_count());

        const recovered = (try storage_engine.find_block(try BlockId.from_hex("00000000000000000000000000000001"))).?;
        try testing.expectEqualStrings("debug test content", recovered.content);
    }

    const total_time = std.time.milliTimestamp() - start_time;
    std.debug.print("Total test time: {}ms\n", .{total_time});
}

test "wal hang debug: empty directory iteration" {
    const allocator = testing.allocator;

    var sim = try Simulation.init(allocator, 11111);
    defer sim.deinit();

    const node = try sim.add_node();
    const node_ptr = sim.find_node(node);
    const sim_vfs = node_ptr.filesystem_interface();

    // Create empty directory
    const empty_dir = "debug_empty_wal";
    try sim_vfs.mkdir_all(empty_dir);

    // Test iteration over empty directory using arena
    var iter_arena = std.heap.ArenaAllocator.init(allocator);
    defer iter_arena.deinit();
    const iter_allocator = iter_arena.allocator();

    var dir_iterator = try sim_vfs.iterate_directory(empty_dir, iter_allocator);

    var iteration_count: u32 = 0;
    const start_time = std.time.milliTimestamp();

    while (dir_iterator.next()) |entry| {
        _ = entry;
        iteration_count += 1;

        const current_time = std.time.milliTimestamp();
        if (current_time - start_time > 1000) {
            std.debug.panic("Empty directory iteration timeout", .{});
        }

        if (iteration_count > 10) {
            std.debug.panic("Empty directory returned entries: {}", .{iteration_count});
        }
    }

    // Empty directory should have no entries
    try testing.expectEqual(@as(u32, 0), iteration_count);
}

test "wal hang debug: wal recovery with large blocks" {
    // Use GPA with safety for memory corruption detection
    var gpa = std.heap.GeneralPurposeAllocator(.{ .safety = true }){};
    defer {
        const deinit_status = gpa.deinit();
        if (deinit_status == .leak) @panic("Memory leak detected in WAL recovery test");
    }
    const allocator = gpa.allocator();

    const start_time = std.time.milliTimestamp();

    var sim = try Simulation.init(allocator, 54321);
    defer sim.deinit();

    const node = try sim.add_node();
    const node_ptr = sim.find_node(node);
    var node_vfs = node_ptr.filesystem_interface();

    const data_dir = "wal_hang_large_blocks";

    // Write phase - create scenario similar to hanging test
    {
        var engine = try StorageEngine.init_default(allocator, node_vfs, data_dir);
        defer engine.deinit();

        try engine.initialize_storage();

        const checkpoint_1 = std.time.milliTimestamp();
        if (checkpoint_1 - start_time > MAX_TEST_DURATION_MS / 4) {
            std.debug.panic("Storage init timeout: {}ms", .{checkpoint_1 - start_time});
        }

        // Create large block similar to hanging test but smaller for debug
        const large_content = try allocator.alloc(u8, 512 * 1024); // 512KB instead of 2MB
        defer allocator.free(large_content);
        @memset(large_content, 'X');

        // Write fewer blocks to avoid timeout but still trigger segmentation
        var i: u32 = 0;
        while (i < 5) : (i += 1) {
            const checkpoint_write = std.time.milliTimestamp();
            if (checkpoint_write - start_time > MAX_TEST_DURATION_MS / 2) {
                std.debug.panic("Write loop timeout at block {}: {}ms", .{ i, checkpoint_write - start_time });
            }

            var id_bytes: [16]u8 = undefined;
            std.mem.writeInt(u128, &id_bytes, i, .big);

            const block = ContextBlock{
                .id = BlockId{ .bytes = id_bytes },
                .version = 1,
                .source_uri = "test://debug_large.file",
                .metadata_json = "{}",
                .content = large_content,
            };

            try engine.put_block(block);
        }

        const checkpoint_2 = std.time.milliTimestamp();
        if (checkpoint_2 - start_time > MAX_TEST_DURATION_MS * 3 / 4) {
            std.debug.panic("Write phase timeout: {}ms", .{checkpoint_2 - start_time});
        }

        try engine.flush_wal();

        const checkpoint_3 = std.time.milliTimestamp();
        if (checkpoint_3 - start_time > MAX_TEST_DURATION_MS) {
            std.debug.panic("WAL flush timeout: {}ms", .{checkpoint_3 - start_time});
        }
    }

    // Verify WAL files were created
    const wal_dir = try std.fmt.allocPrint(allocator, "{s}/wal", .{data_dir});
    defer allocator.free(wal_dir);

    // Use arena for directory iteration to prevent memory leaks
    var iter_arena = std.heap.ArenaAllocator.init(allocator);
    defer iter_arena.deinit();
    const iter_allocator = iter_arena.allocator();

    var dir_iterator = try node_vfs.iterate_directory(wal_dir, iter_allocator);
    var wal_file_count: u32 = 0;
    var iteration_count: u32 = 0;

    while (dir_iterator.next()) |entry| {
        iteration_count += 1;

        const checkpoint_iter = std.time.milliTimestamp();
        if (checkpoint_iter - start_time > MAX_TEST_DURATION_MS) {
            std.debug.panic("Directory iteration timeout: {}ms, {} iterations", .{ checkpoint_iter - start_time, iteration_count });
        }

        if (iteration_count > MAX_ITERATIONS) {
            std.debug.panic("Directory iteration exceeded limit: {} iterations", .{iteration_count});
        }

        if (std.mem.startsWith(u8, entry.name, "wal_") and
            std.mem.endsWith(u8, entry.name, ".log"))
        {
            wal_file_count += 1;
        }
    }

    try testing.expect(wal_file_count >= 1);

    // Recovery phase - this is where hangs typically occur
    {
        var engine2 = try StorageEngine.init_default(allocator, node_vfs, data_dir);
        defer engine2.deinit();

        try engine2.initialize_storage();

        const recovery_start = std.time.milliTimestamp();
        if (recovery_start - start_time > MAX_TEST_DURATION_MS) {
            std.debug.panic("Recovery init timeout: {}ms", .{recovery_start - start_time});
        }

        // This is the critical call that hangs in the original tests
        try engine2.recover_from_wal();

        const recovery_end = std.time.milliTimestamp();
        if (recovery_end - start_time > MAX_TEST_DURATION_MS * 2) {
            std.debug.panic("WAL recovery completed but took too long: {}ms", .{recovery_end - start_time});
        }

        // Verify recovery worked
        try testing.expectEqual(@as(u32, 5), engine2.block_count());
    }

    const total_time = std.time.milliTimestamp() - start_time;
    std.debug.print("WAL recovery debug test completed in: {}ms\n", .{total_time});
}

test "wal hang debug: minimal recovery simulation" {
    const allocator = testing.allocator;

    const start_time = std.time.milliTimestamp();

    var sim = try Simulation.init(allocator, 99999);
    defer sim.deinit();

    const node = try sim.add_node();
    const node_ptr = sim.find_node(node);
    const node_vfs = node_ptr.filesystem_interface();

    const data_dir = "minimal_recovery_debug";

    // Create storage and write minimal data
    {
        var engine = try StorageEngine.init_default(allocator, node_vfs, data_dir);
        defer engine.deinit();

        try engine.initialize_storage();

        // Single small block - minimal case
        const small_block = ContextBlock{
            .id = try BlockId.from_hex("00000000000000000000000000000001"),
            .version = 1,
            .source_uri = "test://minimal.zig",
            .metadata_json = "{}",
            .content = "minimal test content",
        };

        try engine.put_block(small_block);
        try engine.flush_wal();

        const write_time = std.time.milliTimestamp();
        if (write_time - start_time > 1000) {
            std.debug.panic("Minimal write timeout: {}ms", .{write_time - start_time});
        }
    }

    // Recovery with timeout monitoring
    {
        var engine2 = try StorageEngine.init_default(allocator, node_vfs, data_dir);
        defer engine2.deinit();

        try engine2.initialize_storage();

        const init_time = std.time.milliTimestamp();
        if (init_time - start_time > 2000) {
            std.debug.panic("Recovery init timeout: {}ms", .{init_time - start_time});
        }

        // Critical recovery call with timeout
        try engine2.recover_from_wal();

        const recovery_time = std.time.milliTimestamp();
        if (recovery_time - start_time > 5000) {
            std.debug.panic("Minimal recovery timeout: {}ms", .{recovery_time - start_time});
        }

        try testing.expectEqual(@as(u32, 1), engine2.block_count());
    }

    const total_time = std.time.milliTimestamp() - start_time;
    std.debug.print("Minimal recovery completed in: {}ms\n", .{total_time});
}

test "wal hang debug: empty WAL file recovery" {
    const allocator = testing.allocator;

    var sim = try Simulation.init(allocator, 77777);
    defer sim.deinit();

    const node = try sim.add_node();
    const node_ptr = sim.find_node(node);
    const node_vfs = node_ptr.filesystem_interface();

    const data_dir = "empty_wal_debug";

    // Create storage but don't write anything
    var engine = try StorageEngine.init_default(allocator, node_vfs, data_dir);
    defer engine.deinit();

    try engine.initialize_storage();

    // Try to recover from empty/uninitialized WAL - this should not hang
    const start_time = std.time.milliTimestamp();

    try engine.recover_from_wal();

    const recovery_time = std.time.milliTimestamp() - start_time;
    if (recovery_time > 1000) {
        std.debug.panic("Empty WAL recovery took too long: {}ms", .{recovery_time});
    }

    // Should have no blocks since nothing was written
    try testing.expectEqual(@as(u32, 0), engine.block_count());

    std.debug.print("Empty WAL recovery completed in: {}ms\n", .{recovery_time});
}

test "wal hang debug: WAL file initialization check" {
    const allocator = testing.allocator;

    var sim = try Simulation.init(allocator, 88888);
    defer sim.deinit();

    const node = try sim.add_node();
    const node_ptr = sim.find_node(node);
    const node_vfs = node_ptr.filesystem_interface();

    const data_dir = "wal_init_debug";

    // Initialize storage and check WAL directory state
    var engine = try StorageEngine.init_default(allocator, node_vfs, data_dir);
    defer engine.deinit();

    try engine.initialize_storage();

    // Check if WAL directory was created properly
    const wal_dir = try std.fmt.allocPrint(allocator, "{s}/wal", .{data_dir});
    defer allocator.free(wal_dir);

    const wal_exists = node_vfs.exists(wal_dir);
    try testing.expect(wal_exists);

    // Check initial WAL file state using arena
    var iter_arena = std.heap.ArenaAllocator.init(allocator);
    defer iter_arena.deinit();
    const iter_allocator = iter_arena.allocator();

    var dir_iterator = try node_vfs.iterate_directory(wal_dir, iter_allocator);
    var wal_file_count: u32 = 0;
    var first_wal_file: ?[]const u8 = null;

    while (dir_iterator.next()) |entry| {
        if (std.mem.startsWith(u8, entry.name, "wal_") and
            std.mem.endsWith(u8, entry.name, ".log"))
        {
            wal_file_count += 1;
            if (first_wal_file == null) {
                first_wal_file = try allocator.dupe(u8, entry.name);
            }
        }
    }

    // Should have exactly one initial WAL file
    try testing.expectEqual(@as(u32, 1), wal_file_count);
    try testing.expect(first_wal_file != null);

    // Check the size of the initial WAL file
    if (first_wal_file) |filename| {
        defer allocator.free(filename);
        const wal_path = try std.fmt.allocPrint(allocator, "{s}/{s}", .{ wal_dir, filename });
        defer allocator.free(wal_path);

        const stat = try node_vfs.stat(wal_path);
        std.debug.print("Initial WAL file size: {} bytes\n", .{stat.size});

        // An empty WAL file should have minimal size
        try testing.expect(stat.size < 100); // Should be very small or empty
    }
}

test "wal hang debug: investigate data corruption source" {
    // Use GPA with safety for memory corruption detection
    var gpa = std.heap.GeneralPurposeAllocator(.{ .safety = true }){};
    defer {
        const deinit_status = gpa.deinit();
        if (deinit_status == .leak) @panic("Memory leak detected in corruption debug test");
    }
    const allocator = gpa.allocator();

    var sim = try Simulation.init(allocator, 12345);
    defer sim.deinit();

    const node = try sim.add_node();
    const node_ptr = sim.find_node(node);
    const sim_vfs = node_ptr.filesystem_interface();

    const data_dir = "corruption_debug";

    // Step 1: Create a simple, well-defined ContextBlock
    const test_block = ContextBlock{
        .id = try BlockId.from_hex("00000000000000000000000000000001"),
        .version = 1,
        .source_uri = "test://debug.zig",
        .metadata_json = "{}",
        .content = "simple test content",
    };

    std.debug.print("Original block data:\n", .{});
    std.debug.print("  ID: {}\n", .{test_block.id});
    std.debug.print("  Version: {}\n", .{test_block.version});
    std.debug.print("  Source URI: '{s}'\n", .{test_block.source_uri});
    std.debug.print("  Metadata: '{s}'\n", .{test_block.metadata_json});
    std.debug.print("  Content: '{s}'\n", .{test_block.content});

    // Step 2: Test ContextBlock serialization directly
    const expected_size = test_block.serialized_size();
    std.debug.print("Expected serialized size: {} bytes\n", .{expected_size});

    const block_buffer = try allocator.alloc(u8, expected_size);
    defer allocator.free(block_buffer);

    const block_bytes_written = try test_block.serialize(block_buffer);
    std.debug.print("Block serialized {} bytes\n", .{block_bytes_written});
    try testing.expectEqual(expected_size, block_bytes_written);

    // Print first 64 bytes of serialized block for inspection
    std.debug.print("First 64 bytes of serialized block: ", .{});
    for (block_buffer[0..@min(64, block_buffer.len)]) |byte| {
        std.debug.print("{x:0>2} ", .{byte});
    }
    std.debug.print("\n", .{});

    // Step 3: Create WAL entry manually and inspect it
    const wal_entry = try storage.WALEntry.create_put_block(test_block, allocator);
    defer wal_entry.deinit(allocator);

    std.debug.print("WAL entry data:\n", .{});
    std.debug.print("  Checksum: 0x{X:0>16}\n", .{wal_entry.checksum});
    std.debug.print("  Entry type: {}\n", .{wal_entry.entry_type});
    std.debug.print("  Payload size: {}\n", .{wal_entry.payload_size});
    std.debug.print("  Payload length: {}\n", .{wal_entry.payload.len});

    // Step 4: Serialize WAL entry and inspect
    const wal_serialized_size = storage.WALEntry.HEADER_SIZE + wal_entry.payload.len;
    const wal_buffer = try allocator.alloc(u8, wal_serialized_size);
    defer allocator.free(wal_buffer);

    const wal_bytes_written = try wal_entry.serialize(wal_buffer);
    std.debug.print("WAL entry serialized {} bytes\n", .{wal_bytes_written});

    // Print first 32 bytes of serialized WAL entry
    std.debug.print("First 32 bytes of serialized WAL entry: ", .{});
    for (wal_buffer[0..@min(32, wal_buffer.len)]) |byte| {
        std.debug.print("{x:0>2} ", .{byte});
    }
    std.debug.print("\n", .{});

    // Step 5: Write using storage engine and check what actually gets written
    var storage_engine = try StorageEngine.init_default(allocator, sim_vfs, data_dir);
    defer storage_engine.deinit();

    try storage_engine.initialize_storage();

    std.debug.print("Writing block to storage...\n", .{});
    try storage_engine.put_block(test_block);
    try storage_engine.flush_wal();

    // Step 6: Read the WAL file directly and inspect its contents
    const wal_dir = try std.fmt.allocPrint(allocator, "{s}/wal", .{data_dir});
    defer allocator.free(wal_dir);

    // Use arena for directory iteration to prevent memory leaks
    var iter_arena = std.heap.ArenaAllocator.init(allocator);
    defer iter_arena.deinit();
    const iter_allocator = iter_arena.allocator();

    var dir_iterator = try sim_vfs.iterate_directory(wal_dir, iter_allocator);
    var wal_filename: ?[]const u8 = null;

    while (dir_iterator.next()) |entry| {
        if (std.mem.startsWith(u8, entry.name, "wal_") and
            std.mem.endsWith(u8, entry.name, ".log"))
        {
            wal_filename = try allocator.dupe(u8, entry.name);
            break;
        }
    }

    if (wal_filename) |filename| {
        defer allocator.free(filename);
        const wal_path = try std.fmt.allocPrint(allocator, "{s}/{s}", .{ wal_dir, filename });
        defer allocator.free(wal_path);

        var wal_file = try sim_vfs.open(wal_path, .read);
        defer wal_file.close();

        const file_size = try wal_file.file_size();
        std.debug.print("WAL file size: {} bytes\n", .{file_size});

        const file_contents = try allocator.alloc(u8, file_size);
        defer allocator.free(file_contents);

        _ = try wal_file.read(file_contents);

        std.debug.print("First 64 bytes of WAL file: ", .{});
        for (file_contents[0..@min(64, file_contents.len)]) |byte| {
            std.debug.print("{x:0>2} ", .{byte});
        }
        std.debug.print("\n", .{});

        // Compare expected vs actual
        if (file_contents.len >= wal_buffer.len) {
            var mismatch_found = false;
            for (wal_buffer, 0..) |expected_byte, i| {
                if (file_contents[i] != expected_byte) {
                    if (!mismatch_found) {
                        std.debug.print("Data mismatch found!\n", .{});
                        mismatch_found = true;
                    }
                    std.debug.print("  Offset {}: expected 0x{x:0>2}, got 0x{x:0>2}\n", .{ i, expected_byte, file_contents[i] });
                    if (i > 20) break; // Limit output
                }
            }
            if (!mismatch_found) {
                std.debug.print("WAL file contents match expected data!\n", .{});
            }
        } else {
            std.debug.print("WAL file too small: {} < {} bytes\n", .{ file_contents.len, wal_buffer.len });
        }
    } else {
        std.debug.panic("No WAL file found", .{});
    }
}

test "vfs debug: file expansion zero initialization" {
    const allocator = testing.allocator;

    var sim = try simulation.Simulation.init(allocator, 12345);
    defer sim.deinit();

    const node1 = try sim.add_node();
    const node1_ptr = sim.find_node(node1);
    const node1_vfs = node1_ptr.filesystem_interface();

    // Create a test file
    var file = try node1_vfs.create("test_expansion.dat");
    defer file.close();

    // Write initial data at position 0
    const initial_data = "HELLO";
    const bytes_written1 = try file.write(initial_data);
    try testing.expectEqual(initial_data.len, bytes_written1);

    // Seek to position 100 (beyond current file size of 5)
    _ = try file.seek(100, .start);

    // Write more data - this should expand the file and zero-fill the gap
    const second_data = "WORLD";
    const bytes_written2 = try file.write(second_data);
    try testing.expectEqual(second_data.len, bytes_written2);

    // Reset file position to read entire file
    _ = try file.seek(0, .start);

    // Read entire file content
    const expected_size = 100 + second_data.len;
    var buffer = try allocator.alloc(u8, expected_size);
    defer allocator.free(buffer);
    const bytes_read = try file.read(buffer);
    try testing.expectEqual(expected_size, bytes_read);

    std.debug.print("File expansion test - buffer contents:\n", .{});
    for (buffer[0..@min(32, buffer.len)], 0..) |byte, i| {
        if (i % 16 == 0) std.debug.print("{x:0>4}: ", .{i});
        std.debug.print("{x:0>2} ", .{byte});
        if ((i + 1) % 16 == 0) std.debug.print("\n", .{});
    }
    std.debug.print("\n", .{});

    // Verify initial data
    try testing.expectEqualSlices(u8, initial_data, buffer[0..5]);

    // Verify gap is zero-filled (positions 5-99)
    var non_zero_count: u32 = 0;
    for (buffer[5..100], 5..) |byte, i| {
        if (byte != 0) {
            std.debug.print("Non-zero byte at position {}: 0x{x:0>2}\n", .{ i, byte });
            non_zero_count += 1;
            if (non_zero_count > 10) break; // Limit output
        }
    }

    // Verify second data
    try testing.expectEqualSlices(u8, second_data, buffer[100..105]);

    if (non_zero_count == 0) {
        std.debug.print("SUCCESS: File expansion properly zero-initialized gap\n", .{});
    } else {
        std.debug.print("FAILURE: Found {} non-zero bytes in gap\n", .{non_zero_count});
        return error.FileExpansionNotZeroInitialized;
    }
}

test "wal corruption debug: isolated single block write-read" {
    const allocator = testing.allocator;

    var sim = try simulation.Simulation.init(allocator, 99999);
    defer sim.deinit();

    const node1 = try sim.add_node();
    const node1_ptr = sim.find_node(node1);
    const node1_vfs = node1_ptr.filesystem_interface();

    // Create simple test block with known, clean content
    const test_block = context_block.ContextBlock{
        .id = try context_block.BlockId.from_hex("deadbeefdeadbeefdeadbeefdeadbeef"),
        .version = 42,
        .source_uri = "test://isolated.zig",
        .metadata_json = "{\"test\":\"isolated\"}",
        .content = "Simple clean content without patterns",
    };

    // Initialize storage with isolated allocator
    const data_dir = try allocator.dupe(u8, "wal_corruption_debug");
    defer allocator.free(data_dir);
    var storage_engine = try storage.StorageEngine.init_default(
        allocator,
        node1_vfs,
        data_dir,
    );
    defer storage_engine.deinit();

    try storage_engine.initialize_storage();

    std.debug.print("=== WAL Corruption Debug Test ===\n", .{});

    // Step 1: Write single block
    std.debug.print("Writing test block...\n", .{});
    try storage_engine.put_block(test_block);
    try storage_engine.flush_wal();

    // Step 2: Inspect WAL file contents immediately
    const wal_dir = try std.fmt.allocPrint(allocator, "{s}/wal", .{data_dir});
    defer allocator.free(wal_dir);

    // Use arena for directory iteration to prevent memory leaks
    var iter_arena = std.heap.ArenaAllocator.init(allocator);
    defer iter_arena.deinit();
    const iter_allocator = iter_arena.allocator();

    var dir_iterator = try node1_vfs.iterate_directory(wal_dir, iter_allocator);

    var wal_file_found = false;
    while (dir_iterator.next()) |entry| {
        if (std.mem.endsWith(u8, entry.name, ".log")) {
            wal_file_found = true;

            const wal_file_path = try std.fmt.allocPrint(allocator, "{s}/{s}", .{ wal_dir, entry.name });
            defer allocator.free(wal_file_path);

            std.debug.print("Found WAL file: {s}\n", .{entry.name});

            // Read entire WAL file
            var wal_file = try node1_vfs.open(wal_file_path, .read);
            defer wal_file.close();

            const file_size = try wal_file.file_size();
            std.debug.print("WAL file size: {} bytes\n", .{file_size});

            const wal_contents = try allocator.alloc(u8, file_size);
            defer allocator.free(wal_contents);

            const bytes_read = try wal_file.read(wal_contents);
            try testing.expectEqual(file_size, bytes_read);

            // Inspect first 64 bytes for corruption patterns
            std.debug.print("First 64 bytes of WAL file:\n", .{});
            for (wal_contents[0..@min(64, wal_contents.len)], 0..) |byte, i| {
                if (i % 16 == 0) std.debug.print("{x:0>4}: ", .{i});
                std.debug.print("{x:0>2} ", .{byte});
                if ((i + 1) % 16 == 0) std.debug.print("\n", .{});
            }
            if (wal_contents.len % 16 != 0) std.debug.print("\n", .{});

            // Check for corruption patterns
            var corruption_count: u32 = 0;
            for (wal_contents, 0..) |byte, i| {
                // Check for common corruption patterns
                if (byte == 'X' or byte == 'x' or byte == 'U' or byte == 0xAA or byte == 0xFF) {
                    // Only report if this appears to be in header area (not content)
                    if (i < 100) { // Rough estimate of header area
                        std.debug.print("Potential corruption at byte {}: 0x{x:0>2} ('{c}')\n", .{ i, byte, if (std.ascii.isPrint(byte)) byte else '?' });
                        corruption_count += 1;
                    }
                }
            }

            if (corruption_count > 0) {
                std.debug.print("WARNING: Found {} potential corruption bytes in header area\n", .{corruption_count});
            } else {
                std.debug.print("No obvious corruption patterns detected in header area\n", .{});
            }

            break;
        }
    }

    try testing.expect(wal_file_found);

    // Step 3: Try recovery
    std.debug.print("Attempting WAL recovery...\n", .{});
    var recovery_engine = try storage.StorageEngine.init_default(
        allocator,
        node1_vfs,
        data_dir,
    );
    defer recovery_engine.deinit();

    try recovery_engine.initialize_storage();
    try recovery_engine.recover_from_wal();

    const recovered_count = recovery_engine.block_count();
    std.debug.print("Recovered {} blocks\n", .{recovered_count});

    if (recovered_count == 1) {
        std.debug.print("SUCCESS: Block recovery successful\n", .{});

        // Verify the recovered block
        const recovered_block = (try recovery_engine.find_block(test_block.id)).?;
        try testing.expect(test_block.id.eql(recovered_block.id));
        try testing.expectEqual(test_block.version, recovered_block.version);
        try testing.expectEqualStrings(test_block.source_uri, recovered_block.source_uri);
        try testing.expectEqualStrings(test_block.metadata_json, recovered_block.metadata_json);
        try testing.expectEqualStrings(test_block.content, recovered_block.content);

        std.debug.print("Block content verification passed\n", .{});
    } else {
        std.debug.print("FAILURE: Expected 1 block, got {}\n", .{recovered_count});
        return error.RecoveryFailed;
    }
}
