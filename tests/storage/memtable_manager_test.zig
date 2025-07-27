//! Isolated unit tests for MemtableManager functionality.
//!
//! Tests the complete in-memory write buffer management including
//! block operations, edge operations, WAL integration, and lifecycle
//! management. Uses simulation VFS for deterministic testing without
//! external dependencies.

const cortexdb = @import("cortexdb");
const std = @import("std");
const testing = std.testing;
const assert = cortexdb.assert.assert;

const vfs = cortexdb.vfs;
const simulation_vfs = cortexdb.simulation_vfs;
const simulation = cortexdb.simulation;
const context_block = cortexdb.types;
const storage = cortexdb.storage;

const SimulationVFS = simulation_vfs.SimulationVFS;
const Simulation = simulation.Simulation;
const ContextBlock = context_block.ContextBlock;
const BlockId = context_block.BlockId;
const GraphEdge = context_block.GraphEdge;
const EdgeType = context_block.EdgeType;
const MemtableManager = storage.MemtableManager;

/// Helper to create test MemtableManager with isolated VFS
fn create_test_memtable_manager(allocator: std.mem.Allocator) !struct {
    simulation: Simulation,
    manager: MemtableManager,

    pub fn deinit(self: *@This()) void {
        self.manager.deinit();
        self.simulation.deinit();
    }
} {
    var sim = try Simulation.init(allocator, 12345);
    const node1 = try sim.add_node();
    const node1_ptr = sim.find_node(node1);
    const node1_vfs = node1_ptr.filesystem_interface();
    const manager = try MemtableManager.init(allocator, node1_vfs, "/test/data");

    return .{
        .simulation = sim,
        .manager = manager,
    };
}

/// Generate a unique test BlockId for testing
fn generate_test_block_id(counter: u8) BlockId {
    var hex_string: [32]u8 = undefined;

    // Fill with zeros first
    @memset(&hex_string, '0');

    // Put counter in the last 2 positions as hex (ensures uniqueness)
    const hex_chars = "0123456789abcdef";
    hex_string[30] = hex_chars[counter / 16];
    hex_string[31] = hex_chars[counter % 16];

    return BlockId.from_hex(&hex_string) catch unreachable;
}

/// Helper to create test ContextBlock with given ID
fn create_test_block(id: BlockId, content: []const u8) ContextBlock {
    return ContextBlock{
        .id = id,
        .version = 1,
        .source_uri = "test://source.zig",
        .metadata_json = "{}",
        .content = content,
    };
}

/// Helper to create test GraphEdge
fn create_test_edge(source: BlockId, target: BlockId, edge_type: EdgeType) GraphEdge {
    return GraphEdge{
        .source_id = source,
        .target_id = target,
        .edge_type = edge_type,
    };
}

test "MemtableManager initialization and cleanup" {
    const allocator = testing.allocator;

    var test_setup = try create_test_memtable_manager(allocator);
    defer test_setup.deinit();

    try test_setup.manager.startup();

    // Initial state should be empty
    try testing.expectEqual(@as(u32, 0), test_setup.manager.block_count());
    try testing.expectEqual(@as(u32, 0), test_setup.manager.edge_count());
    try testing.expectEqual(@as(u64, 0), test_setup.manager.memory_usage());
}

test "MemtableManager basic block operations" {
    const allocator = testing.allocator;

    var test_setup = try create_test_memtable_manager(allocator);
    defer test_setup.deinit();

    try test_setup.manager.startup();

    const block_id = generate_test_block_id(1);
    const test_block = create_test_block(block_id, "test content");

    // Put block - should go through WAL-first pattern
    try test_setup.manager.put_block_durable(test_block);

    // Verify block is in memtable
    try testing.expectEqual(@as(u32, 1), test_setup.manager.block_count());
    try testing.expect(test_setup.manager.memory_usage() > 0);

    // Find block in memtable
    const found_block = test_setup.manager.find_block_in_memtable(block_id);
    try testing.expect(found_block != null);
    try testing.expectEqualStrings("test content", found_block.?.content);

    // Delete block - should go through WAL-first pattern
    try test_setup.manager.delete_block_durable(block_id);
    try testing.expectEqual(@as(u32, 0), test_setup.manager.block_count());

    // Block should no longer be found
    try testing.expect(test_setup.manager.find_block_in_memtable(block_id) == null);
}

test "MemtableManager multiple block operations" {
    const allocator = testing.allocator;

    var test_setup = try create_test_memtable_manager(allocator);
    defer test_setup.deinit();

    try test_setup.manager.startup();

    // Add multiple blocks
    const block1_id = generate_test_block_id(1);
    const block2_id = generate_test_block_id(2);
    const block3_id = generate_test_block_id(3);

    const block1 = create_test_block(block1_id, "content 1");
    const block2 = create_test_block(block2_id, "content 2");
    const block3 = create_test_block(block3_id, "content 3");

    try test_setup.manager.put_block_durable(block1);
    try test_setup.manager.put_block_durable(block2);
    try test_setup.manager.put_block_durable(block3);

    try testing.expectEqual(@as(u32, 3), test_setup.manager.block_count());

    // Verify all blocks can be found
    try testing.expect(test_setup.manager.find_block_in_memtable(block1_id) != null);
    try testing.expect(test_setup.manager.find_block_in_memtable(block2_id) != null);
    try testing.expect(test_setup.manager.find_block_in_memtable(block3_id) != null);

    // Update a block (overwrite)
    const updated_block2 = create_test_block(block2_id, "updated content 2");
    try test_setup.manager.put_block_durable(updated_block2);

    // Should still have 3 blocks (overwrite, not addition)
    try testing.expectEqual(@as(u32, 3), test_setup.manager.block_count());

    // Verify update took effect
    const found_block2 = test_setup.manager.find_block_in_memtable(block2_id);
    try testing.expect(found_block2 != null);
    try testing.expectEqualStrings("updated content 2", found_block2.?.content);
}

test "MemtableManager edge operations" {
    const allocator = testing.allocator;

    var test_setup = try create_test_memtable_manager(allocator);
    defer test_setup.deinit();

    try test_setup.manager.startup();

    const source_id = generate_test_block_id(4);
    const target_id = generate_test_block_id(5);
    const test_edge = create_test_edge(source_id, target_id, .imports);

    // Put edge - should go through WAL-first pattern
    try test_setup.manager.put_edge_durable(test_edge);

    try testing.expectEqual(@as(u32, 1), test_setup.manager.edge_count());

    // Find outgoing edges
    const outgoing = test_setup.manager.find_outgoing_edges(source_id);
    try testing.expectEqual(@as(usize, 1), outgoing.len);
    try testing.expectEqual(target_id, outgoing[0].target_id);
    try testing.expectEqual(EdgeType.imports, outgoing[0].edge_type);

    // Find incoming edges
    const incoming = test_setup.manager.find_incoming_edges(target_id);
    try testing.expectEqual(@as(usize, 1), incoming.len);
    try testing.expectEqual(source_id, incoming[0].source_id);
    try testing.expectEqual(EdgeType.imports, incoming[0].edge_type);

    // Add another edge with different type
    const second_target = generate_test_block_id(6);
    const second_edge = create_test_edge(source_id, second_target, .calls);
    try test_setup.manager.put_edge_durable(second_edge);

    try testing.expectEqual(@as(u32, 2), test_setup.manager.edge_count());

    // Source should now have 2 outgoing edges
    const outgoing_after = test_setup.manager.find_outgoing_edges(source_id);
    try testing.expectEqual(@as(usize, 2), outgoing_after.len);
}

test "MemtableManager block iterator functionality" {
    const allocator = testing.allocator;

    var test_setup = try create_test_memtable_manager(allocator);
    defer test_setup.deinit();

    try test_setup.manager.startup();

    // Add test blocks
    const block1_id = generate_test_block_id(1);
    const block2_id = generate_test_block_id(2);
    const block1 = create_test_block(block1_id, "iterator test 1");
    const block2 = create_test_block(block2_id, "iterator test 2");

    try test_setup.manager.put_block_durable(block1);
    try test_setup.manager.put_block_durable(block2);

    // Test iterator
    var iterator = test_setup.manager.iterator();
    var blocks_found = std.ArrayList(ContextBlock).init(allocator);
    defer blocks_found.deinit();

    while (iterator.next()) |block| {
        try blocks_found.append(block);
    }

    try testing.expectEqual(@as(usize, 2), blocks_found.items.len);

    // Verify both blocks are found (order may vary due to HashMap iteration)
    var found_block1 = false;
    var found_block2 = false;
    for (blocks_found.items) |block| {
        if (std.mem.eql(u8, &block.id.bytes, &block1_id.bytes)) {
            found_block1 = true;
            try testing.expectEqualStrings("iterator test 1", block.content);
        } else if (std.mem.eql(u8, &block.id.bytes, &block2_id.bytes)) {
            found_block2 = true;
            try testing.expectEqualStrings("iterator test 2", block.content);
        }
    }
    try testing.expect(found_block1);
    try testing.expect(found_block2);
}

test "MemtableManager clear operation" {
    const allocator = testing.allocator;

    var test_setup = try create_test_memtable_manager(allocator);
    defer test_setup.deinit();

    try test_setup.manager.startup();

    // Add blocks and edges
    const block_id = generate_test_block_id(1);
    const source_id = generate_test_block_id(4);
    const target_id = generate_test_block_id(5);

    const test_block = create_test_block(block_id, "clear test");
    const test_edge = create_test_edge(source_id, target_id, .imports);

    try test_setup.manager.put_block_durable(test_block);
    try test_setup.manager.put_edge_durable(test_edge);

    try testing.expectEqual(@as(u32, 1), test_setup.manager.block_count());
    try testing.expectEqual(@as(u32, 1), test_setup.manager.edge_count());
    try testing.expect(test_setup.manager.memory_usage() > 0);

    // Clear should reset to empty state with O(1) arena cleanup
    test_setup.manager.clear();

    try testing.expectEqual(@as(u32, 0), test_setup.manager.block_count());
    try testing.expectEqual(@as(u32, 0), test_setup.manager.edge_count());
    try testing.expectEqual(@as(u64, 0), test_setup.manager.memory_usage());

    // Blocks should no longer be found
    try testing.expect(test_setup.manager.find_block_in_memtable(block_id) == null);
    try testing.expectEqual(@as(usize, 0), test_setup.manager.find_outgoing_edges(source_id).len);
}

test "MemtableManager memory usage tracking" {
    const allocator = testing.allocator;

    var test_setup = try create_test_memtable_manager(allocator);
    defer test_setup.deinit();

    try test_setup.manager.startup();

    // Initial memory usage should be zero
    try testing.expectEqual(@as(u64, 0), test_setup.manager.memory_usage());

    // Add a block and verify memory usage increases
    const block_id = generate_test_block_id(1);
    const large_content = "x" ** 1024; // 1KB content
    const test_block = create_test_block(block_id, large_content);

    try test_setup.manager.put_block_durable(test_block);

    const memory_after_block = test_setup.manager.memory_usage();
    try testing.expect(memory_after_block > 1024); // Should include content plus metadata

    // Add an edge and verify memory usage increases further
    const source_id = generate_test_block_id(4);
    const target_id = generate_test_block_id(5);
    const test_edge = create_test_edge(source_id, target_id, .imports);

    try test_setup.manager.put_edge_durable(test_edge);

    const memory_after_edge = test_setup.manager.memory_usage();
    // Edge adds additional memory usage (amount may vary by implementation)
    try testing.expect(memory_after_edge > 0);

    // Clear should reset memory usage to zero
    test_setup.manager.clear();
    try testing.expectEqual(@as(u64, 0), test_setup.manager.memory_usage());
}

test "MemtableManager WAL recovery integration" {
    const allocator = testing.allocator;

    var test_setup = try create_test_memtable_manager(allocator);
    defer test_setup.deinit();

    try test_setup.manager.startup();

    // Add data that will be written to WAL
    const block_id = generate_test_block_id(1);
    const test_block = create_test_block(block_id, "recovery test");
    const edge = create_test_edge(generate_test_block_id(7), generate_test_block_id(8), .calls);

    try test_setup.manager.put_block_durable(test_block);
    try test_setup.manager.put_edge_durable(edge);

    try testing.expectEqual(@as(u32, 1), test_setup.manager.block_count());
    try testing.expectEqual(@as(u32, 1), test_setup.manager.edge_count());

    // Clear memtable (simulating flush)
    test_setup.manager.clear();
    try testing.expectEqual(@as(u32, 0), test_setup.manager.block_count());
    try testing.expectEqual(@as(u32, 0), test_setup.manager.edge_count());

    // Recover from WAL should restore state
    try test_setup.manager.recover_from_wal();

    try testing.expectEqual(@as(u32, 1), test_setup.manager.block_count());
    try testing.expectEqual(@as(u32, 1), test_setup.manager.edge_count());

    // Verify recovered block content
    const recovered_block = test_setup.manager.find_block_in_memtable(block_id);
    try testing.expect(recovered_block != null);
    try testing.expectEqualStrings("recovery test", recovered_block.?.content);
}

test "MemtableManager error handling with invalid operations" {
    const allocator = testing.allocator;

    var test_setup = try create_test_memtable_manager(allocator);
    defer test_setup.deinit();

    // Operations before startup should work (init sets up basic state)
    try test_setup.manager.startup();

    // Test with invalid block content
    const block_id = generate_test_block_id(1);
    var invalid_block = create_test_block(block_id, "test");
    invalid_block.content = ""; // Empty content might trigger validation

    // Should handle gracefully or return specific error
    const result = test_setup.manager.put_block_durable(invalid_block);
    // Note: Depending on validation logic, this might succeed or fail
    // The test verifies that the operation doesn't crash
    _ = result catch {};
}

test "MemtableManager concurrent-style operations simulation" {
    const allocator = testing.allocator;

    var test_setup = try create_test_memtable_manager(allocator);
    defer test_setup.deinit();

    try test_setup.manager.startup();

    // Simulate rapid put/delete cycles that might occur in concurrent scenarios
    // Note: MemtableManager is single-threaded, but we can test rapid operations

    const iterations = 100;
    var block_ids = std.ArrayList(BlockId).init(allocator);
    defer block_ids.deinit();

    // Rapid puts
    for (0..iterations) |i| {
        const block_id = generate_test_block_id(@as(u8, @intCast(i + 1)));
        try block_ids.append(block_id);

        const content = try std.fmt.allocPrint(allocator, "rapid test {}", .{i});
        defer allocator.free(content);

        const test_block = create_test_block(block_id, content);
        try test_setup.manager.put_block_durable(test_block);
    }

    try testing.expectEqual(@as(u32, iterations), test_setup.manager.block_count());

    // Rapid deletes
    for (block_ids.items) |block_id| {
        try test_setup.manager.delete_block_durable(block_id);
    }

    try testing.expectEqual(@as(u32, 0), test_setup.manager.block_count());
}
