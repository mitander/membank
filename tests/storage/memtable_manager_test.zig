//! Isolated unit tests for MemtableManager functionality.

const cortexdb = @import("cortexdb");
const std = @import("std");
const testing = std.testing;

const simulation_vfs = cortexdb.simulation_vfs;
const context_block = cortexdb.types;
const storage = cortexdb.storage;

const SimulationVFS = simulation_vfs.SimulationVFS;
const ContextBlock = context_block.ContextBlock;
const BlockId = context_block.BlockId;
const GraphEdge = context_block.GraphEdge;
const EdgeType = context_block.EdgeType;
const MemtableManager = storage.MemtableManager;


fn create_test_block(id: BlockId, content: []const u8) ContextBlock {
    return ContextBlock{
        .id = id,
        .version = 1,
        .source_uri = "test://source.zig",
        .metadata_json = "{}",
        .content = content,
    };
}

fn create_test_edge(source: BlockId, target: BlockId, edge_type: EdgeType) GraphEdge {
    return GraphEdge{
        .source_id = source,
        .target_id = target,
        .edge_type = edge_type,
    };
}

test "MemtableManager basic lifecycle" {
    const allocator = testing.allocator;

    var sim_vfs = try SimulationVFS.init(allocator);
    defer sim_vfs.deinit();

    var manager = try MemtableManager.init(allocator, sim_vfs.vfs(), "/test/data", 1024 * 1024);
    defer manager.deinit();

    try testing.expectEqual(@as(u32, 0), manager.block_count());
    try testing.expectEqual(@as(u32, 0), manager.edge_count());
    try testing.expectEqual(@as(u64, 0), manager.memory_usage());
}

test "MemtableManager with WAL operations" {
    const allocator = testing.allocator;

    var sim_vfs = try SimulationVFS.init(allocator);
    defer sim_vfs.deinit();

    var manager = try MemtableManager.init(allocator, sim_vfs.vfs(), "/test/data", 1024 * 1024);
    defer manager.deinit();

    try manager.startup();

    const block_id = try BlockId.from_hex("00000000000000000000000000000001");
    const test_block = create_test_block(block_id, "test content");

    try manager.put_block_durable(test_block);
    try testing.expectEqual(@as(u32, 1), manager.block_count());

    const found_block = manager.find_block_in_memtable(block_id);
    try testing.expect(found_block != null);
    try testing.expectEqualStrings("test content", found_block.?.content);
}

test "MemtableManager multiple blocks" {
    const allocator = testing.allocator;

    var sim_vfs = try SimulationVFS.init(allocator);
    defer sim_vfs.deinit();

    var manager = try MemtableManager.init(allocator, sim_vfs.vfs(), "/test/data", 1024 * 1024);
    defer manager.deinit();

    try manager.startup();

    const block1_id = try BlockId.from_hex("00000000000000000000000000000001");
    const block2_id = try BlockId.from_hex("00000000000000000000000000000002");
    const block1 = create_test_block(block1_id, "content 1");
    const block2 = create_test_block(block2_id, "content 2");

    try manager.put_block_durable(block1);
    try manager.put_block_durable(block2);

    try testing.expectEqual(@as(u32, 2), manager.block_count());
    try testing.expect(manager.find_block_in_memtable(block1_id) != null);
    try testing.expect(manager.find_block_in_memtable(block2_id) != null);
}

test "MemtableManager edge operations" {
    const allocator = testing.allocator;

    var sim_vfs = try SimulationVFS.init(allocator);
    defer sim_vfs.deinit();

    var manager = try MemtableManager.init(allocator, sim_vfs.vfs(), "/test/data", 1024 * 1024);
    defer manager.deinit();

    try manager.startup();

    const source_id = try BlockId.from_hex("00000000000000000000000000000001");
    const target_id = try BlockId.from_hex("00000000000000000000000000000002");
    const test_edge = create_test_edge(source_id, target_id, .imports);

    try manager.put_edge_durable(test_edge);
    try testing.expectEqual(@as(u32, 1), manager.edge_count());

    const outgoing = manager.find_outgoing_edges(source_id);
    try testing.expectEqual(@as(usize, 1), outgoing.len);
    try testing.expectEqual(target_id, outgoing[0].target_id);
}

test "MemtableManager clear operation" {
    const allocator = testing.allocator;

    var sim_vfs = try SimulationVFS.init(allocator);
    defer sim_vfs.deinit();

    var manager = try MemtableManager.init(allocator, sim_vfs.vfs(), "/test/data", 1024 * 1024);
    defer manager.deinit();

    try manager.startup();

    const block_id = try BlockId.from_hex("00000000000000000000000000000001");
    const test_block = create_test_block(block_id, "clear test");
    try manager.put_block_durable(test_block);

    try testing.expectEqual(@as(u32, 1), manager.block_count());

    // O(1) arena cleanup
    manager.clear();

    try testing.expectEqual(@as(u32, 0), manager.block_count());
    try testing.expectEqual(@as(u64, 0), manager.memory_usage());
}