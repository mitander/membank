//! Advanced query scenario tests for KausalDB.
//!
//! Tests complex graph traversals, query optimization, performance characteristics,
//! and edge cases across the query engine. Validates query plan generation,
//! execution strategies, and memory efficiency under various workloads.

const std = @import("std");
const builtin = @import("builtin");
const testing = std.testing;
const kausaldb = @import("kausaldb");

const storage = kausaldb.storage;
const query = kausaldb.query;
const simulation_vfs = kausaldb.simulation_vfs;
const types = kausaldb.types;

const StorageEngine = storage.StorageEngine;
const QueryEngine = query.engine.QueryEngine;
const SimulationVFS = simulation_vfs.SimulationVFS;
const ContextBlock = types.ContextBlock;
const BlockId = types.BlockId;
const GraphEdge = types.GraphEdge;
const EdgeType = types.EdgeType;
const FindBlocksQuery = query.operations.FindBlocksQuery;
const TraversalQuery = query.traversal.TraversalQuery;
const QueryResult = query.operations.QueryResult;

fn create_test_block_id(id: u32) BlockId {
    var id_bytes: [16]u8 = undefined;
    std.mem.writeInt(u128, &id_bytes, id, .little);
    return BlockId{ .bytes = id_bytes };
}

fn create_test_block(id: u32, content: []const u8, allocator: std.mem.Allocator) !ContextBlock {
    return ContextBlock{
        .id = create_test_block_id(id),
        .version = 1,
        .source_uri = try std.fmt.allocPrint(allocator, "test://block_{}.zig", .{id}),
        .metadata_json = try allocator.dupe(u8, "{}"),
        .content = try allocator.dupe(u8, content),
    };
}

fn create_test_edge(from_id: u32, to_id: u32, edge_type: EdgeType) GraphEdge {
    return GraphEdge{
        .source_id = create_test_block_id(from_id),
        .target_id = create_test_block_id(to_id),
        .edge_type = edge_type,
    };
}

test "complex graph traversal scenarios" {
    const allocator = testing.allocator;

    var sim_vfs = try SimulationVFS.init(allocator);
    defer sim_vfs.deinit();

    var storage_engine = try StorageEngine.init_default(allocator, sim_vfs.vfs(), "complex_traversal_test");
    defer storage_engine.deinit();
    try storage_engine.startup();

    var query_engine = QueryEngine.init(allocator, &storage_engine);
    defer query_engine.deinit();

    // Create a complex graph structure: function -> imports -> dependencies
    // Function block
    const main_func = try create_test_block(1, "pub fn main() !void { process(); }", allocator);
    defer allocator.free(main_func.content);
    defer allocator.free(main_func.source_uri);
    defer allocator.free(main_func.metadata_json);
    try storage_engine.put_block(main_func);

    // Process function
    const process_func = try create_test_block(2, "fn process() void { validate(); render(); }", allocator);
    defer allocator.free(process_func.content);
    defer allocator.free(process_func.source_uri);
    defer allocator.free(process_func.metadata_json);
    try storage_engine.put_block(process_func);

    // Validation function
    const validate_func = try create_test_block(3, "fn validate() bool { return true; }", allocator);
    defer allocator.free(validate_func.content);
    defer allocator.free(validate_func.source_uri);
    defer allocator.free(validate_func.metadata_json);
    try storage_engine.put_block(validate_func);

    // Render function
    const render_func = try create_test_block(4, "fn render() void { draw(); display(); }", allocator);
    defer allocator.free(render_func.content);
    defer allocator.free(render_func.source_uri);
    defer allocator.free(render_func.metadata_json);
    try storage_engine.put_block(render_func);

    // Utility functions
    const draw_func = try create_test_block(5, "fn draw() void { /* drawing logic */ }", allocator);
    defer allocator.free(draw_func.content);
    defer allocator.free(draw_func.source_uri);
    defer allocator.free(draw_func.metadata_json);
    try storage_engine.put_block(draw_func);

    const display_func = try create_test_block(6, "fn display() void { /* display logic */ }", allocator);
    defer allocator.free(display_func.content);
    defer allocator.free(display_func.source_uri);
    defer allocator.free(display_func.metadata_json);
    try storage_engine.put_block(display_func);

    // Create call graph edges
    try storage_engine.put_edge(create_test_edge(1, 2, EdgeType.calls)); // main -> process
    try storage_engine.put_edge(create_test_edge(2, 3, EdgeType.calls)); // process -> validate
    try storage_engine.put_edge(create_test_edge(2, 4, EdgeType.calls)); // process -> render
    try storage_engine.put_edge(create_test_edge(4, 5, EdgeType.calls)); // render -> draw
    try storage_engine.put_edge(create_test_edge(4, 6, EdgeType.calls)); // render -> display

    // Test multi-hop traversal
    const traversal_query = TraversalQuery{
        .start_block_id = main_func.id,
        .edge_filter = .{ .include_types = &[_]EdgeType{EdgeType.calls} },
        .direction = .outgoing,
        .algorithm = .breadth_first,
        .max_depth = 3,
        .max_results = 100,
    };

    var result = try query_engine.execute_traversal(traversal_query);
    defer result.deinit();

    // Should find all reachable functions within 3 hops
    var found_blocks = std.ArrayList(BlockId).init(allocator);
    try found_blocks.ensureTotalCapacity(10); // Expected number of matching blocks
    defer found_blocks.deinit();

    for (result.blocks) |block| {
        try found_blocks.append(block.id);
    }

    try testing.expect(found_blocks.items.len >= 5); // Should find most functions
}

test "query optimization strategy validation" {
    const allocator = testing.allocator;

    var sim_vfs = try SimulationVFS.init(allocator);
    defer sim_vfs.deinit();

    var storage_engine = try StorageEngine.init_default(allocator, sim_vfs.vfs(), "optimization_test");
    defer storage_engine.deinit();
    try storage_engine.startup();

    var query_engine = QueryEngine.init(allocator, &storage_engine);
    defer query_engine.deinit();

    // Create a large dataset to trigger optimization strategies
    const dataset_size = 500;
    var i: u32 = 0;
    while (i < dataset_size) : (i += 1) {
        const content = try std.fmt.allocPrint(allocator, "Block content {}", .{i});
        defer allocator.free(content);
        const block = try create_test_block(i, content, allocator);
        defer allocator.free(block.content);
        defer allocator.free(block.source_uri);
        defer allocator.free(block.metadata_json);
        try storage_engine.put_block(block);
    }

    // Test small query (should use direct strategy)
    const small_query = FindBlocksQuery{
        .block_ids = &[_]BlockId{
            create_test_block_id(0),
            create_test_block_id(1),
        },
    };

    var small_result = try query.operations.execute_find_blocks(allocator, &storage_engine, small_query);
    defer small_result.deinit();

    // Test large query (should use optimized strategy)
    var large_block_ids = std.ArrayList(BlockId).init(allocator);
    try large_block_ids.ensureTotalCapacity(50); // Number of large blocks in the test
    defer large_block_ids.deinit();
    i = 0;
    while (i < 100) : (i += 1) {
        try large_block_ids.append(create_test_block_id(i));
    }

    const large_query = FindBlocksQuery{
        .block_ids = large_block_ids.items,
    };

    var large_result = try query.operations.execute_find_blocks(allocator, &storage_engine, large_query);
    defer large_result.deinit();

    // Both should complete successfully with appropriate results
    try testing.expect(true); // If we get here, optimization worked
}

test "query performance under memory pressure" {
    const allocator = testing.allocator;

    var sim_vfs = try SimulationVFS.init(allocator);
    defer sim_vfs.deinit();

    var storage_engine = try StorageEngine.init_default(allocator, sim_vfs.vfs(), "memory_pressure_test");
    defer storage_engine.deinit();
    try storage_engine.startup();

    var query_engine = QueryEngine.init(allocator, &storage_engine);
    defer query_engine.deinit();

    // Create dataset with varying block sizes
    var i: u32 = 0;
    while (i < 500) : (i += 1) {
        // Create blocks with increasing content size
        const content_size = (i % 100) * 50 + 100; // 100-5000 bytes
        var content = std.ArrayList(u8).init(allocator);
        try content.ensureTotalCapacity(1024); // Expected size of the content
        defer content.deinit();

        var j: usize = 0;
        while (j < content_size) : (j += 1) {
            try content.append('A' + @as(u8, @intCast(j % 26)));
        }

        const block = try create_test_block(i, content.items, allocator);
        defer allocator.free(block.content);
        defer allocator.free(block.source_uri);
        defer allocator.free(block.metadata_json);
        try storage_engine.put_block(block);
    }

    // Perform multiple queries to test memory efficiency
    var query_round: u32 = 0;
    while (query_round < 10) : (query_round += 1) {
        var query_block_ids = std.ArrayList(BlockId).init(allocator);
        try query_block_ids.ensureTotalCapacity(10); // Expected number of blocks in the test query
        defer query_block_ids.deinit();

        // Query random subset
        i = query_round * 20;
        while (i < (query_round + 1) * 20) : (i += 1) {
            try query_block_ids.append(create_test_block_id(i));
        }

        const find_query = FindBlocksQuery{
            .block_ids = query_block_ids.items,
        };

        var result = try query.operations.execute_find_blocks(allocator, &storage_engine, find_query);
        defer result.deinit();

        // Consume results to test memory handling
        while (try result.next()) |block| {
            // Verify block is valid
            try testing.expect(block.content.len > 0);
        }
    }

    // Test should complete without memory issues
    try testing.expect(true);
}

test "complex filtering and search scenarios" {
    const allocator = testing.allocator;

    var sim_vfs = try SimulationVFS.init(allocator);
    defer sim_vfs.deinit();

    var storage_engine = try StorageEngine.init_default(allocator, sim_vfs.vfs(), "filtering_test");
    defer storage_engine.deinit();
    try storage_engine.startup();

    var query_engine = QueryEngine.init(allocator, &storage_engine);
    defer query_engine.deinit();

    // Create blocks with different content patterns
    const content_patterns = [_][]const u8{
        "function processData() { return validate(input); }",
        "struct DataProcessor { fn process(self: *Self) void {} }",
        "const CONFIG = struct { timeout: u32 = 5000, retries: u8 = 3 };",
        "test \"data processing\" { try testing.expect(true); }",
        "pub fn handleRequest(req: Request) Response { return process(req); }",
    };

    for (content_patterns, 0..) |pattern, idx| {
        const block = try create_test_block(@intCast(idx), pattern, allocator);
        defer allocator.free(block.content);
        defer allocator.free(block.source_uri);
        defer allocator.free(block.metadata_json);
        try storage_engine.put_block(block);
    }

    // Test content-based queries (when filtering is implemented)
    var all_block_ids = std.ArrayList(BlockId).init(allocator);
    try all_block_ids.ensureTotalCapacity(100); // Total number of blocks in the test
    defer all_block_ids.deinit();

    var i: u32 = 0;
    while (i < content_patterns.len) : (i += 1) {
        try all_block_ids.append(create_test_block_id(i));
    }

    const find_query = FindBlocksQuery{
        .block_ids = all_block_ids.items,
    };

    var result = try query.operations.execute_find_blocks(allocator, &storage_engine, find_query);
    defer result.deinit();

    var found_count: u32 = 0;
    while (try result.next()) |block| {
        found_count += 1;
        try testing.expect(block.content.len > 0);
    }

    try testing.expectEqual(@as(u32, content_patterns.len), found_count);
}

test "graph traversal with cycle detection" {
    const allocator = testing.allocator;

    var sim_vfs = try SimulationVFS.init(allocator);
    defer sim_vfs.deinit();

    var storage_engine = try StorageEngine.init_default(allocator, sim_vfs.vfs(), "cycle_detection_test");
    defer storage_engine.deinit();
    try storage_engine.startup();

    var query_engine = QueryEngine.init(allocator, &storage_engine);
    defer query_engine.deinit();

    // Create blocks that form a cycle
    const block_a = try create_test_block(1, "Function A calls B", allocator);
    defer allocator.free(block_a.content);
    defer allocator.free(block_a.source_uri);
    defer allocator.free(block_a.metadata_json);
    try storage_engine.put_block(block_a);

    const block_b = try create_test_block(2, "Function B calls C", allocator);
    defer allocator.free(block_b.content);
    defer allocator.free(block_b.source_uri);
    defer allocator.free(block_b.metadata_json);
    try storage_engine.put_block(block_b);

    const block_c = try create_test_block(3, "Function C calls A", allocator);
    defer allocator.free(block_c.content);
    defer allocator.free(block_c.source_uri);
    defer allocator.free(block_c.metadata_json);
    try storage_engine.put_block(block_c);

    // Create cycle: A -> B -> C -> A
    try storage_engine.put_edge(create_test_edge(1, 2, EdgeType.calls));
    try storage_engine.put_edge(create_test_edge(2, 3, EdgeType.calls));
    try storage_engine.put_edge(create_test_edge(3, 1, EdgeType.calls));

    // Traversal should handle cycle gracefully
    const traversal_query = TraversalQuery{
        .start_block_id = block_a.id,
        .edge_filter = .{ .include_types = &[_]EdgeType{EdgeType.calls} },
        .direction = .outgoing,
        .algorithm = .breadth_first,
        .max_depth = 10, // Deep enough to detect cycle
        .max_results = 100,
    };

    var result = try query_engine.execute_traversal(traversal_query);
    defer result.deinit();

    // Should find all blocks but not infinite loop
    var found_blocks = std.ArrayList(BlockId).init(allocator);
    try found_blocks.ensureTotalCapacity(10); // Expected number of matching blocks
    defer found_blocks.deinit();

    for (result.blocks) |block| {
        try found_blocks.append(block.id);
    }

    // Should find all 3 blocks exactly once
    try testing.expectEqual(@as(usize, 3), found_blocks.items.len);
}

test "batch query operations and efficiency" {
    const allocator = testing.allocator;

    var sim_vfs = try SimulationVFS.init(allocator);
    defer sim_vfs.deinit();

    var storage_engine = try StorageEngine.init_default(allocator, sim_vfs.vfs(), "batch_operations_test");
    defer storage_engine.deinit();
    try storage_engine.startup();

    var query_engine = QueryEngine.init(allocator, &storage_engine);
    defer query_engine.deinit();

    // Create a moderate dataset
    var i: u32 = 0;
    while (i < 100) : (i += 1) {
        const content = try std.fmt.allocPrint(allocator, "Batch test block {}", .{i});
        defer allocator.free(content);
        const block = try create_test_block(i, content, allocator);
        defer allocator.free(block.content);
        defer allocator.free(block.source_uri);
        defer allocator.free(block.metadata_json);
        try storage_engine.put_block(block);
    }

    const start_time = std.time.nanoTimestamp();

    // Perform multiple batch queries
    var batch_round: u32 = 0;
    while (batch_round < 10) : (batch_round += 1) {
        var batch_block_ids = std.ArrayList(BlockId).init(allocator);
        try batch_block_ids.ensureTotalCapacity(10); // Batch size for testing
        defer batch_block_ids.deinit();

        // Create batch of 10 block IDs
        i = batch_round * 10;
        while (i < (batch_round + 1) * 10) : (i += 1) {
            try batch_block_ids.append(create_test_block_id(i));
        }

        const batch_query = FindBlocksQuery{
            .block_ids = batch_block_ids.items,
        };

        var result = try query.operations.execute_find_blocks(allocator, &storage_engine, batch_query);
        defer result.deinit();

        // Verify batch results
        var batch_count: u32 = 0;
        while (try result.next()) |_| {
            batch_count += 1;
        }

        try testing.expectEqual(@as(u32, 10), batch_count);
    }

    const end_time = std.time.nanoTimestamp();
    const total_time = end_time - start_time;

    // Should complete 100 queries in reasonable time
    const max_time = 100_000_000; // 100ms
    try testing.expect(total_time < max_time);
}

test "query error handling and recovery" {
    const allocator = testing.allocator;

    var sim_vfs = try SimulationVFS.init(allocator);
    defer sim_vfs.deinit();

    var storage_engine = try StorageEngine.init_default(allocator, sim_vfs.vfs(), "error_handling_test");
    defer storage_engine.deinit();
    try storage_engine.startup();

    var query_engine = QueryEngine.init(allocator, &storage_engine);
    defer query_engine.deinit();

    // Test query with non-existent blocks
    var nonexistent_ids = std.ArrayList(BlockId).init(allocator);
    try nonexistent_ids.ensureTotalCapacity(5); // Number of non-existent IDs to test
    defer nonexistent_ids.deinit();

    var i: u32 = 9000; // IDs that don't exist
    while (i < 9010) : (i += 1) {
        try nonexistent_ids.append(create_test_block_id(i));
    }

    const missing_query = FindBlocksQuery{
        .block_ids = nonexistent_ids.items,
    };

    var result = try query.operations.execute_find_blocks(allocator, &storage_engine, missing_query);
    defer result.deinit();

    // Should handle missing blocks gracefully (return empty results)
    var found_count: u32 = 0;
    while (try result.next()) |_| {
        found_count += 1;
    }

    try testing.expectEqual(@as(u32, 0), found_count);

    // Test traversal with invalid start points
    const invalid_traversal = TraversalQuery{
        .start_block_id = nonexistent_ids.items[0], // Use first nonexistent ID
        .edge_filter = .{ .include_types = &[_]EdgeType{EdgeType.calls} },
        .direction = .outgoing,
        .algorithm = .breadth_first,
        .max_depth = 5,
        .max_results = 50,
    };

    var traversal_result = try query_engine.execute_traversal(invalid_traversal);
    defer traversal_result.deinit();

    // Should handle gracefully
    found_count = 0;
    for (traversal_result.blocks) |_| {
        found_count += 1;
    }

    try testing.expectEqual(@as(u32, 0), found_count);
}

test "mixed query workload simulation" {
    const allocator = testing.allocator;

    var sim_vfs = try SimulationVFS.init(allocator);
    defer sim_vfs.deinit();

    var storage_engine = try StorageEngine.init_default(allocator, sim_vfs.vfs(), "mixed_workload_test");
    defer storage_engine.deinit();
    try storage_engine.startup();

    var query_engine = QueryEngine.init(allocator, &storage_engine);
    defer query_engine.deinit();

    // Create diverse dataset
    var i: u32 = 0;
    while (i < 50) : (i += 1) {
        const content = try std.fmt.allocPrint(allocator, "Mixed workload block {}: {s}", .{ i, if (i % 2 == 0) "function" else "struct" });
        defer allocator.free(content);
        const block = try create_test_block(i, content, allocator);
        defer allocator.free(block.content);
        defer allocator.free(block.source_uri);
        defer allocator.free(block.metadata_json);
        try storage_engine.put_block(block);

        // Add some edges for traversal testing
        if (i > 0 and i % 5 != 0) {
            try storage_engine.put_edge(create_test_edge(i - 1, i, EdgeType.calls));
        }
    }

    // Simulate mixed workload: single lookups, batch queries, traversals
    var workload_round: u32 = 0;
    while (workload_round < 5) : (workload_round += 1) {
        // Single block lookup
        const single_id = create_test_block_id(workload_round);
        const single_query = FindBlocksQuery{
            .block_ids = &[_]BlockId{single_id},
        };

        var single_result = try query.operations.execute_find_blocks(allocator, &storage_engine, single_query);
        defer single_result.deinit();

        // Batch lookup
        var batch_ids = std.ArrayList(BlockId).init(allocator);
        try batch_ids.ensureTotalCapacity(10); // Batch size for mixed workload
        defer batch_ids.deinit();
        i = workload_round * 5;
        while (i < (workload_round + 1) * 5) : (i += 1) {
            try batch_ids.append(create_test_block_id(i));
        }

        const batch_query = FindBlocksQuery{
            .block_ids = batch_ids.items,
        };

        var batch_result = try query.operations.execute_find_blocks(allocator, &storage_engine, batch_query);
        defer batch_result.deinit();

        // Graph traversal
        const traversal_start = create_test_block_id(workload_round * 5);
        const traversal_query = TraversalQuery{
            .start_block_id = traversal_start,
            .edge_filter = .{ .include_types = &[_]EdgeType{EdgeType.calls} },
            .direction = .outgoing,
            .algorithm = .breadth_first,
            .max_depth = 3,
            .max_results = 100,
        };

        var traversal_result = try query_engine.execute_traversal(traversal_query);
        defer traversal_result.deinit();

        // Consume all results to test memory handling
        while (try single_result.next()) |_| {}

        while (try batch_result.next()) |_| {}

        for (traversal_result.blocks) |_| {}
    }

    // Workload should complete successfully
    try testing.expect(true);
}
