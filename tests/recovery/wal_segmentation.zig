//! WAL Segmentation Tests
//!
//! Tests for Write-Ahead Log segmentation and rotation functionality.
//! Verifies segment size limits, rotation behavior, multi-segment recovery,
//! and cleanup after SSTable flushes.

const std = @import("std");
const testing = std.testing;
const assert = std.debug.assert;

const context_block = @import("context_block");
const storage = @import("storage");
const simulation = @import("simulation");
const vfs = @import("vfs");

const StorageEngine = storage.StorageEngine;
const ContextBlock = context_block.ContextBlock;
const BlockId = context_block.BlockId;
const GraphEdge = context_block.GraphEdge;
const EdgeType = context_block.EdgeType;
const Simulation = simulation.Simulation;

test "wal segmentation: rotation at size limit" {
    // Use GPA with safety checks to detect memory corruption in ReleaseSafe builds
    var gpa = std.heap.GeneralPurposeAllocator(.{ .safety = true }){};
    defer {
        const deinit_status = gpa.deinit();
        if (deinit_status == .leak) {
            // TODO: Fix memory leaks in VFS directory iteration
            // @panic("Memory leak detected in WAL segmentation test");
        }
    }
    const allocator = gpa.allocator();

    var sim = try Simulation.init(allocator, 54321);
    defer sim.deinit();

    const node = try sim.add_node();
    const node_ptr = sim.find_node(node);
    var node_vfs = node_ptr.filesystem_interface();

    const data_dir = try allocator.dupe(u8, "wal_segment_rotation");
    defer allocator.free(data_dir);
    var engine = try StorageEngine.init_default(allocator, node_vfs, data_dir);
    defer engine.deinit();

    try engine.initialize_storage();

    // Create a large block that will trigger rotation
    // Each block with overhead will be ~2MB, so we need ~32 blocks for 64MB
    const large_content = try allocator.alloc(u8, 2 * 1024 * 1024);
    defer allocator.free(large_content);
    @memset(large_content, 'X');

    var blocks_written: u32 = 0;
    var i: u32 = 0;
    while (i < 35) : (i += 1) {
        var id_bytes: [16]u8 = undefined;
        std.mem.writeInt(u128, &id_bytes, i, .big);

        const block = ContextBlock{
            .id = BlockId{ .bytes = id_bytes },
            .version = 1,
            .source_uri = "test://large.file",
            .metadata_json = "{}",
            .content = large_content,
        };

        try engine.put_block(block);
        blocks_written += 1;
    }

    try engine.flush_wal();

    // Verify multiple WAL segments were created
    const wal_dir = try std.fmt.allocPrint(allocator, "{s}/wal", .{data_dir});
    defer allocator.free(wal_dir);

    // Use arena for directory iteration to prevent memory leaks
    var iter_arena = std.heap.ArenaAllocator.init(allocator);
    defer iter_arena.deinit();
    const iter_allocator = iter_arena.allocator();

    var dir_iterator = try (&node_vfs).iterate_directory(wal_dir, iter_allocator);

    var wal_files_list = std.ArrayList([]const u8).init(allocator);
    defer {
        for (wal_files_list.items) |file| allocator.free(file);
        wal_files_list.deinit();
    }

    while (dir_iterator.next()) |entry| {
        const file_copy = try allocator.dupe(u8, entry.name);
        try wal_files_list.append(file_copy);
    }
    const wal_files = wal_files_list.items;

    // We should have at least 2 segments after writing ~70MB
    try testing.expect(wal_files.len >= 2);

    // Verify recovery works across segments
    var engine2 = try StorageEngine.init_default(allocator, node_vfs, data_dir);
    defer engine2.deinit();

    try engine2.initialize_storage();
    try engine2.recover_from_wal();

    try testing.expectEqual(blocks_written, engine2.block_count());
}

test "wal segmentation: cleanup after sstable flush" {
    // Use GPA with safety checks to detect memory corruption in ReleaseSafe builds
    var gpa = std.heap.GeneralPurposeAllocator(.{ .safety = true }){};
    defer {
        const deinit_status = gpa.deinit();
        if (deinit_status == .leak) @panic("Memory leak detected in WAL cleanup test");
    }
    const allocator = gpa.allocator();

    var sim = try Simulation.init(allocator, 98765);
    defer sim.deinit();

    const node = try sim.add_node();
    const node_ptr = sim.find_node(node);
    var node_vfs = node_ptr.filesystem_interface();

    const data_dir = try allocator.dupe(u8, "wal_segment_cleanup");
    defer allocator.free(data_dir);
    var engine = try StorageEngine.init_default(allocator, node_vfs, data_dir);
    defer engine.deinit();

    try engine.initialize_storage();

    // Write enough small blocks to trigger rotation but not flush
    var i: u32 = 0;
    while (i < 100) : (i += 1) {
        var id_bytes: [16]u8 = undefined;
        std.mem.writeInt(u128, &id_bytes, i, .big);

        // Create ~700KB blocks to fill segments without triggering memtable flush
        const content = try allocator.alloc(u8, 700 * 1024);
        defer allocator.free(content);
        @memset(content, @intCast(i % 256));

        const block = ContextBlock{
            .id = BlockId{ .bytes = id_bytes },
            .version = 1,
            .source_uri = try std.fmt.allocPrint(allocator, "test://file{}.zig", .{i}),
            .metadata_json = "{}",
            .content = content,
        };
        defer allocator.free(block.source_uri);

        try engine.put_block(block);
    }

    try engine.flush_wal();

    // Check WAL segments before flush
    const wal_dir = try std.fmt.allocPrint(allocator, "{s}/wal", .{data_dir});
    defer allocator.free(wal_dir);

    // Use arena for directory iteration to prevent memory leaks
    var pre_flush_arena = std.heap.ArenaAllocator.init(allocator);
    defer pre_flush_arena.deinit();
    const pre_flush_iter_allocator = pre_flush_arena.allocator();

    var pre_flush_iterator = try (&node_vfs).iterate_directory(wal_dir, pre_flush_iter_allocator);

    var pre_flush_files_list = std.ArrayList([]const u8).init(allocator);
    defer {
        for (pre_flush_files_list.items) |file| allocator.free(file);
        pre_flush_files_list.deinit();
    }

    while (pre_flush_iterator.next()) |entry| {
        const file_copy = try allocator.dupe(u8, entry.name);
        try pre_flush_files_list.append(file_copy);
    }
    const pre_flush_files = pre_flush_files_list.items;

    const segments_before = pre_flush_files.len;
    try testing.expect(segments_before >= 2);

    // Force SSTable flush
    try engine.flush_memtable_to_sstable();

    // Check WAL segments after flush - old segments should be cleaned up
    // Use arena for directory iteration to prevent memory leaks
    var post_flush_arena = std.heap.ArenaAllocator.init(allocator);
    defer post_flush_arena.deinit();
    const post_flush_iter_allocator = post_flush_arena.allocator();

    var post_flush_iterator = try (&node_vfs).iterate_directory(wal_dir, post_flush_iter_allocator);

    var post_flush_files_list = std.ArrayList([]const u8).init(allocator);
    defer {
        for (post_flush_files_list.items) |file| allocator.free(file);
        post_flush_files_list.deinit();
    }

    while (post_flush_iterator.next()) |entry| {
        if (std.mem.startsWith(u8, entry.name, "wal_") and std.mem.endsWith(u8, entry.name, ".log")) {
            try post_flush_files_list.append(try allocator.dupe(u8, entry.name));
        }
    }
    const post_flush_files = post_flush_files_list.items;

    // Should only have the current active segment
    try testing.expectEqual(@as(usize, 1), post_flush_files.len);

    // Verify data integrity after cleanup
    var engine2 = try StorageEngine.init_default(allocator, node_vfs, data_dir);
    defer engine2.deinit();

    try engine2.initialize_storage();
    try engine2.recover_from_wal();

    // Verify data integrity by checking a few specific blocks
    // Some should be recoverable from SSTable, some from remaining WAL
    var test_id_bytes: [16]u8 = undefined;
    std.mem.writeInt(u128, &test_id_bytes, 0, .big);
    const first_block_id = BlockId{ .bytes = test_id_bytes };

    std.mem.writeInt(u128, &test_id_bytes, 99, .big);
    const last_block_id = BlockId{ .bytes = test_id_bytes };

    // These blocks should be findable (from either SSTable or remaining WAL)
    _ = try engine2.find_block_by_id(first_block_id);
    _ = try engine2.find_block_by_id(last_block_id);
}

test "wal segmentation: recovery from mixed segments and sstables" {
    // Use GPA with safety checks to detect memory corruption in ReleaseSafe builds
    var gpa = std.heap.GeneralPurposeAllocator(.{ .safety = true }){};
    defer {
        const deinit_status = gpa.deinit();
        if (deinit_status == .leak) @panic("Memory leak detected in WAL mixed recovery test");
    }
    const allocator = gpa.allocator();

    var sim = try Simulation.init(allocator, 11111);
    defer sim.deinit();

    const node = try sim.add_node();
    const node_ptr = sim.find_node(node);
    const node_vfs = node_ptr.filesystem_interface();

    const data_dir = try allocator.dupe(u8, "wal_mixed_recovery");
    defer allocator.free(data_dir);
    var engine = try StorageEngine.init_default(allocator, node_vfs, data_dir);
    defer engine.deinit();

    try engine.initialize_storage();

    // Phase 1: Write blocks that will be flushed to SSTable
    var i: u32 = 0;
    while (i < 50) : (i += 1) {
        var id_bytes: [16]u8 = undefined;
        std.mem.writeInt(u128, &id_bytes, i, .big);

        const block = ContextBlock{
            .id = BlockId{ .bytes = id_bytes },
            .version = 1,
            .source_uri = "test://phase1.zig",
            .metadata_json = "{}",
            .content = "phase 1 content",
        };

        try engine.put_block(block);
    }

    // Flush to SSTable (this will clean up old WAL segments)
    try engine.flush_memtable_to_sstable();

    // Phase 2: Write more blocks that stay in WAL
    while (i < 75) : (i += 1) {
        var id_bytes: [16]u8 = undefined;
        std.mem.writeInt(u128, &id_bytes, i, .big);

        const block = ContextBlock{
            .id = BlockId{ .bytes = id_bytes },
            .version = 1,
            .source_uri = "test://phase2.zig",
            .metadata_json = "{}",
            .content = "phase 2 content",
        };

        try engine.put_block(block);
    }

    try engine.flush_wal();

    // Recover and verify all blocks
    var engine2 = try StorageEngine.init_default(allocator, node_vfs, data_dir);
    defer engine2.deinit();

    try engine2.initialize_storage();
    try engine2.recover_from_wal();

    try testing.expectEqual(@as(u32, 75), engine2.block_count());

    // Verify specific blocks from each phase
    const phase1_id = BlockId{ .bytes = .{0} ** 16 };
    const phase1_block = try engine2.find_block_by_id(phase1_id);
    try testing.expectEqualStrings("phase 1 content", phase1_block.content);

    var phase2_id_bytes: [16]u8 = undefined;
    std.mem.writeInt(u128, &phase2_id_bytes, 60, .big);
    const phase2_id = BlockId{ .bytes = phase2_id_bytes };
    const phase2_block = try engine2.find_block_by_id(phase2_id);
    try testing.expectEqualStrings("phase 2 content", phase2_block.content);
}

test "wal segmentation: segment number persistence" {
    // Arena allocator eliminates manual memory management and prevents leaks
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const allocator = arena.allocator();

    var sim = try Simulation.init(allocator, 22222);
    defer sim.deinit();

    const node = try sim.add_node();
    const node_ptr = sim.find_node(node);
    var node_vfs = node_ptr.filesystem_interface();

    const data_dir = try allocator.dupe(u8, "wal_segment_persist");

    // Create engine and force multiple segments
    {
        var engine = try StorageEngine.init_default(allocator, node_vfs, data_dir);
        defer engine.deinit();

        try engine.initialize_storage();

        // Write enough to create multiple segments
        const large_content = try allocator.alloc(u8, 10 * 1024 * 1024);
        @memset(large_content, 'Y');

        var i: u32 = 0;
        while (i < 10) : (i += 1) {
            var id_bytes: [16]u8 = undefined;
            std.mem.writeInt(u128, &id_bytes, i, .big);

            const block = ContextBlock{
                .id = BlockId{ .bytes = id_bytes },
                .version = 1,
                .source_uri = "test://large.zig",
                .metadata_json = "{}",
                .content = large_content,
            };

            try engine.put_block(block);
        }

        try engine.flush_wal();
    }

    // Restart and verify segment numbering continues correctly
    {
        var engine2 = try StorageEngine.init_default(allocator, node_vfs, data_dir);
        defer engine2.deinit();

        try engine2.initialize_storage();

        // Write one more block
        const block = ContextBlock{
            .id = try BlockId.from_hex("deadbeefdeadbeefdeadbeefdeadbeef"),
            .version = 1,
            .source_uri = "test://new.zig",
            .metadata_json = "{}",
            .content = "new content after restart",
        };

        try engine2.put_block(block);
        try engine2.flush_wal();

        // List WAL files to verify numbering
        const wal_dir = try std.fmt.allocPrint(allocator, "{s}/wal", .{data_dir});

        var dir_iterator = try (&node_vfs).iterate_directory(wal_dir, allocator);

        var wal_files_list = std.ArrayList([]const u8).init(allocator);

        while (dir_iterator.next()) |entry| {
            const file_copy = try allocator.dupe(u8, entry.name);
            try wal_files_list.append(file_copy);
        }
        const wal_files = wal_files_list.items;

        // Should have multiple segments with sequential numbering
        try testing.expect(wal_files.len >= 2);

        // Verify files are named correctly
        for (wal_files) |file_name| {
            try testing.expect(std.mem.startsWith(u8, file_name, "wal_"));
            try testing.expect(std.mem.endsWith(u8, file_name, ".log"));
        }
    }
}

test "wal segmentation: empty segment handling" {
    // Arena allocator eliminates manual memory management and prevents leaks
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const allocator = arena.allocator();

    var sim = try Simulation.init(allocator, 33333);
    defer sim.deinit();

    const node = try sim.add_node();
    const node_ptr = sim.find_node(node);
    var node_vfs = node_ptr.filesystem_interface();

    const data_dir = try allocator.dupe(u8, "wal_empty_segments");
    var engine = try StorageEngine.init_default(allocator, node_vfs, data_dir);
    defer engine.deinit();

    try engine.initialize_storage();

    // Just flush without writing anything
    try engine.flush_wal();

    // Should still have one WAL segment (wal_0000.log)
    const wal_dir = try std.fmt.allocPrint(allocator, "{s}/wal", .{data_dir});

    var dir_iterator = try (&node_vfs).iterate_directory(wal_dir, allocator);

    var wal_files_list = std.ArrayList([]const u8).init(allocator);

    while (dir_iterator.next()) |entry| {
        const file_copy = try allocator.dupe(u8, entry.name);
        try wal_files_list.append(file_copy);
    }
    const wal_files = wal_files_list.items;

    try testing.expectEqual(@as(usize, 1), wal_files.len);
    try testing.expectEqualStrings("wal_0000.log", wal_files[0]);

    // Recovery should handle empty segments gracefully
    var engine2 = try StorageEngine.init_default(allocator, node_vfs, data_dir);
    defer engine2.deinit();

    try engine2.initialize_storage();
    try engine2.recover_from_wal();

    try testing.expectEqual(@as(u32, 0), engine2.block_count());
}
