//! WAL Hang Debug Tests
//!
//! Minimal debug tests to isolate hanging WAL operations.
//! Uses defensive programming patterns with timeouts and iteration limits.

const std = @import("std");
const testing = std.testing;
const assert = std.debug.assert;

const simulation = @import("simulation");
const context_block = @import("context_block");
const storage = @import("storage");
const vfs = @import("vfs");

const Simulation = simulation.Simulation;
const ContextBlock = context_block.ContextBlock;
const BlockId = context_block.BlockId;
const StorageEngine = storage.StorageEngine;

// Defensive limits to prevent infinite loops
const MAX_TEST_DURATION_MS = 5000;
const MAX_DIRECTORY_ENTRIES = 1000;
const MAX_ITERATIONS = 100;

test "wal hang debug: minimal directory iteration" {
    // Arena-per-test for perfect isolation
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const allocator = arena.allocator();

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
    try sim_vfs.mkdir_all(wal_dir);

    // Create a few test WAL files to iterate over
    const test_files = [_][]const u8{
        "wal_0000.log",
        "wal_0001.log",
        "wal_0002.log",
    };

    for (test_files) |filename| {
        const filepath = try std.fmt.allocPrint(allocator, "{s}/{s}", .{ wal_dir, filename });
        var file = try sim_vfs.create(filepath);
        defer file.close();
        _ = try file.write("test content");
        try file.flush();
    }

    // Test directory iteration with defensive patterns
    var dir_iterator = try sim_vfs.iterate_directory(wal_dir, allocator);

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
    // Arena-per-test for perfect isolation
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const allocator = arena.allocator();

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

        const recovered = try storage_engine.find_block_by_id(try BlockId.from_hex("00000000000000000000000000000001"));
        try testing.expectEqualStrings("debug test content", recovered.content);
    }

    const total_time = std.time.milliTimestamp() - start_time;
    std.debug.print("Total test time: {}ms\n", .{total_time});
}

test "wal hang debug: empty directory iteration" {
    // Arena-per-test for perfect isolation
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const allocator = arena.allocator();

    var sim = try Simulation.init(allocator, 11111);
    defer sim.deinit();

    const node = try sim.add_node();
    const node_ptr = sim.find_node(node);
    const sim_vfs = node_ptr.filesystem_interface();

    // Create empty directory
    const empty_dir = "debug_empty_wal";
    try sim_vfs.mkdir_all(empty_dir);

    // Test iteration over empty directory
    var dir_iterator = try sim_vfs.iterate_directory(empty_dir, allocator);

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

    var dir_iterator = try node_vfs.iterate_directory(wal_dir, allocator);
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
    // Arena-per-test for perfect isolation
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const allocator = arena.allocator();

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
    // Arena-per-test for perfect isolation
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const allocator = arena.allocator();

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
    // Arena-per-test for perfect isolation
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const allocator = arena.allocator();

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

    // Check initial WAL file state
    var dir_iterator = try node_vfs.iterate_directory(wal_dir, allocator);
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
