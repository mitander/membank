//! MemtableManager unit tests
//!
//! Tests individual MemtableManager functionality in isolation.
//! Focus: basic operations, memory management, block lifecycle.

const kausaldb = @import("kausaldb");
const std = @import("std");
const testing = std.testing;

const simulation_vfs = kausaldb.simulation_vfs;
const storage = kausaldb.storage;
const types = kausaldb.types;

const ContextBlock = types.ContextBlock;
const BlockId = types.BlockId;
const MemtableManager = storage.MemtableManager;
const SimulationVFS = simulation_vfs.SimulationVFS;

test "put block basic" {
    const allocator = testing.allocator;

    var sim_vfs = try SimulationVFS.init(allocator);
    defer sim_vfs.deinit();

    var manager = try MemtableManager.init(allocator, sim_vfs.vfs(), "/test/data", 1024 * 1024);
    defer manager.deinit();
    try manager.startup();

    // Direct block creation - no helpers
    const test_block = ContextBlock{
        .id = BlockId{ .bytes = [_]u8{1} ** 16 },
        .version = 1,
        .source_uri = "test://memtable.zig",
        .metadata_json = "{}",
        .content = "test content",
    };

    // Test specific operation
    try manager.put_block(test_block);
    const retrieved = manager.find_block_in_memtable(test_block.id);

    // Simple, direct assertions
    try testing.expect(retrieved != null);
    try testing.expectEqualStrings(test_block.content, retrieved.?.content);
}

test "find block missing" {
    const allocator = testing.allocator;

    var sim_vfs = try SimulationVFS.init(allocator);
    defer sim_vfs.deinit();

    var manager = try MemtableManager.init(allocator, sim_vfs.vfs(), "/test/data", 1024 * 1024);
    defer manager.deinit();
    try manager.startup();

    // Test finding non-existent block
    const missing_id = BlockId{ .bytes = [_]u8{99} ** 16 };
    const result = manager.find_block_in_memtable(missing_id);

    try testing.expect(result == null);
}

test "put block overwrite" {
    const allocator = testing.allocator;

    var sim_vfs = try SimulationVFS.init(allocator);
    defer sim_vfs.deinit();

    var manager = try MemtableManager.init(allocator, sim_vfs.vfs(), "/test/data", 1024 * 1024);
    defer manager.deinit();
    try manager.startup();

    const block_id = BlockId{ .bytes = [_]u8{1} ** 16 };

    // Put first version
    const block_v1 = ContextBlock{
        .id = block_id,
        .version = 1,
        .source_uri = "test://version1.zig",
        .metadata_json = "{}",
        .content = "version 1 content",
    };
    try manager.put_block(block_v1);

    // Put second version (overwrite)
    const block_v2 = ContextBlock{
        .id = block_id,
        .version = 2,
        .source_uri = "test://version2.zig",
        .metadata_json = "{}",
        .content = "version 2 content",
    };
    try manager.put_block(block_v2);

    // Verify latest version is retrieved
    const retrieved = manager.find_block_in_memtable(block_id).?;
    try testing.expectEqual(@as(u64, 2), retrieved.version);
    try testing.expectEqualStrings("version 2 content", retrieved.content);
}
