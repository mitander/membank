const std = @import("std");
const testing = std.testing;

const context_block = @import("src/context_block.zig");
const storage = @import("src/storage.zig");
const simulation_vfs = @import("src/simulation_vfs.zig");

const ContextBlock = context_block.ContextBlock;
const BlockId = context_block.BlockId;
const StorageEngine = storage.StorageEngine;

test "minimal HashMap corruption reproduction" {
    const allocator = testing.allocator;

    var sim_vfs = simulation_vfs.SimulationVFS.init(allocator);
    defer sim_vfs.deinit();

    const vfs_interface = sim_vfs.vfs();
    const data_dir = try allocator.dupe(u8, "debug_test");

    var storage_engine = try StorageEngine.init(allocator, vfs_interface, data_dir);
    defer storage_engine.deinit(); // This should trigger the HashMap corruption

    try storage_engine.startup();

    // Create a simple test block
    const test_id = try BlockId.from_hex("1111111111111111111111111111111");
    const test_block = ContextBlock{
        .id = test_id,
        .version = 1,
        .source_uri = "test://debug.zig",
        .metadata_json = "{}",
        .content = "debug test content",
    };

    // Put the block to populate the BlockIndex HashMap
    try storage_engine.put_block(test_block);

    // Verify it was stored
    const retrieved = try storage_engine.find_block_by_id(test_id);
    try testing.expect(retrieved.id.eql(test_id));

    // The deinit in defer should now trigger the HashMap corruption issue
}

test "minimal BlockIndex test" {
    const allocator = testing.allocator;

    var index = storage.BlockIndex.init(allocator);
    defer index.deinit(); // Direct test of BlockIndex deinit

    const test_id = try BlockId.from_hex("2222222222222222222222222222222");
    const test_block = ContextBlock{
        .id = test_id,
        .version = 1,
        .source_uri = "test://index.zig",
        .metadata_json = "{}",
        .content = "index test content",
    };

    try index.put_block(test_block);
    try testing.expectEqual(@as(u32, 1), index.block_count());

    // The deinit should happen here and might show the corruption
}
