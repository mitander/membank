//! Isolated unit tests for SSTableManager functionality.

const cortexdb = @import("cortexdb");
const std = @import("std");
const testing = std.testing;

const simulation_vfs = cortexdb.simulation_vfs;
const context_block = cortexdb.types;
const storage = cortexdb.storage;

const SimulationVFS = simulation_vfs.SimulationVFS;
const ContextBlock = context_block.ContextBlock;
const BlockId = context_block.BlockId;
const SSTableManager = storage.SSTableManager;

fn create_test_block(id: BlockId, content: []const u8) ContextBlock {
    return ContextBlock{
        .id = id,
        .version = 1,
        .source_uri = "test://source.zig",
        .metadata_json = "{}",
        .content = content,
    };
}

test "SSTableManager initialization" {
    const allocator = testing.allocator;

    var sim_vfs = try SimulationVFS.init(allocator);
    defer sim_vfs.deinit();

    var manager = SSTableManager.init(allocator, sim_vfs.vfs(), "/test/data");
    defer manager.deinit();

    try manager.startup();
    try testing.expect(sim_vfs.vfs().exists("/test/data/sst"));
}

test "SSTableManager create and read SSTable" {
    const allocator = testing.allocator;

    var sim_vfs = try SimulationVFS.init(allocator);
    defer sim_vfs.deinit();

    var manager = SSTableManager.init(allocator, sim_vfs.vfs(), "/test/data");
    defer manager.deinit();

    try manager.startup();

    const block_id = try BlockId.from_hex("00000000000000000000000000000001");
    const test_block = create_test_block(block_id, "sstable content");

    const blocks = [_]ContextBlock{test_block};
    try manager.create_new_sstable(blocks[0..]);

    var query_arena = std.heap.ArenaAllocator.init(allocator);
    defer query_arena.deinit();

    const found_block = try manager.find_block_in_sstables(block_id, query_arena.allocator());
    try testing.expect(found_block != null);
    try testing.expectEqualStrings("sstable content", found_block.?.content);
}

test "SSTableManager LSM-tree read semantics" {
    const allocator = testing.allocator;

    var sim_vfs = try SimulationVFS.init(allocator);
    defer sim_vfs.deinit();

    var manager = SSTableManager.init(allocator, sim_vfs.vfs(), "/test/data");
    defer manager.deinit();

    try manager.startup();

    const block_id = try BlockId.from_hex("00000000000000000000000000000001");

    // Create first SSTable with original version
    const old_block = create_test_block(block_id, "old version");
    const blocks1 = [_]ContextBlock{old_block};
    try manager.create_new_sstable(blocks1[0..]);

    // Create second SSTable with updated version (same block ID)
    const new_block = create_test_block(block_id, "new version");
    const blocks2 = [_]ContextBlock{new_block};
    try manager.create_new_sstable(blocks2[0..]);

    var query_arena = std.heap.ArenaAllocator.init(allocator);
    defer query_arena.deinit();

    // Should return newest version (LSM-tree semantics)
    const found_block = try manager.find_block_in_sstables(block_id, query_arena.allocator());
    try testing.expect(found_block != null);
    try testing.expectEqualStrings("new version", found_block.?.content);
}

test "SSTableManager non-existent block" {
    const allocator = testing.allocator;

    var sim_vfs = try SimulationVFS.init(allocator);
    defer sim_vfs.deinit();

    var manager = SSTableManager.init(allocator, sim_vfs.vfs(), "/test/data");
    defer manager.deinit();

    try manager.startup();

    var query_arena = std.heap.ArenaAllocator.init(allocator);
    defer query_arena.deinit();

    const non_existent_id = try BlockId.from_hex("00000000000000000000000000000063");
    const not_found = try manager.find_block_in_sstables(non_existent_id, query_arena.allocator());
    try testing.expect(not_found == null);
}
