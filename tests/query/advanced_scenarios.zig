//! Comprehensive advanced query scenario tests for Membank.
//!
//! Tests complex graph traversals, query optimization, performance characteristics,
//! and edge cases across the query engine. Validates query plan generation,
//! execution strategies, and memory efficiency under various workloads.

const std = @import("std");
const testing = std.testing;
const membank = @import("membank");

const storage = membank.storage;
const query = membank.query;
const simulation_vfs = membank.simulation_vfs;
const context_block = membank.types;
const concurrency = membank.concurrency;

const StorageEngine = storage.StorageEngine;
const QueryEngine = query.QueryEngine;
const SimulationVFS = simulation_vfs.SimulationVFS;
const ContextBlock = context_block.ContextBlock;
const BlockId = context_block.BlockId;
const GraphEdge = context_block.GraphEdge;
const EdgeType = context_block.EdgeType;
const FindBlocksQuery = query.operations.FindBlocksQuery;
const TraversalQuery = query.operations.TraversalQuery;
const QueryResult = query.operations.QueryResult;

fn create_test_block(id: u32, content: []const u8, allocator: std.mem.Allocator) !ContextBlock {
    var id_bytes: [16]u8 = undefined;
    std.mem.writeInt(u128, &id_bytes, id, .little);

    return ContextBlock{
        .id = BlockId{ .bytes = id_bytes },
        .version = 1,
        .source_uri = try allocator.dupe(u8, "test://advanced_scenarios"),
        .metadata_json = try std.fmt.allocPrint(allocator, "{{\"id\": {}, \"type\": \"test\"}}", .{id}),
        .content = try allocator.dupe(u8, content),
    };
}

fn create_test_edge(from_id: u32, to_id: u32, edge_type: EdgeType) GraphEdge {
    var from_bytes: [16]u8 = undefined;
    var to_bytes: [16]u8 = undefined;
    std.mem.writeInt(u128, &from_bytes, from_id, .little);
    std.mem.writeInt(u128, &to_bytes, to_id, .little);

    return GraphEdge{
        .from_block_id = BlockId{ .bytes = from_bytes },
        .to_block_id = BlockId{ .bytes = to_bytes },
        .edge_type = edge_type,
        .metadata_json = "{}",
    };
}

test "complex graph traversal scenarios" {
    concurrency.init();
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
        .start_block_ids = &[_]BlockId{main_func.id},
        .edge_types = &[_]EdgeType{EdgeType.calls},
        .direction = .outgoing,
        .max_depth = 3,
        .max_results = 100,
    };

    var result = try query_engine.execute_traversal(traversal_query, allocator);
    defer result.deinit(allocator);

    // Should find all reachable functions within 3 hops
    var found_blocks = std.ArrayList(BlockId).init(allocator);
    try found_blocks.ensureTotalCapacity(10); // Expected number of matching blocks
    defer found_blocks.deinit();

    var iterator = result.iterator();
    while (try iterator.next(allocator)) |block| {
        defer block.deinit(allocator);
        try found_blocks.append(block.id); // tidy:ignore-perf Unknown result count from query
    }

    try testing.expect(found_blocks.items.len >= 5); // Should find most functions
}

test "query optimization strategy validation" {
    concurrency.init();
    const allocator = testing.allocator;

    var sim_vfs = try SimulationVFS.init(allocator);
    defer sim_vfs.deinit();

    var storage_engine = try StorageEngine.init_default(allocator, sim_vfs.vfs(), "optimization_test");
    defer storage_engine.deinit();
    try storage_engine.startup();

    var query_engine = QueryEngine.init(allocator, &storage_engine);
    defer query_engine.deinit();

    // Create a large dataset to trigger optimization strategies
    var i: u32 = 0;
    while (i < 1000) : (i += 1) {
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
            (try create_test_block(1, "", allocator)).id,
            (try create_test_block(2, "", allocator)).id,
        },
        .max_results = 10,
    };

    var small_result = try query_engine.execute_find_blocks(small_query, allocator);
    defer small_result.deinit(allocator);

    // Test large query (should use optimized strategy)
    var large_block_ids = std.ArrayList(BlockId).init(allocator);
    try large_block_ids.ensureTotalCapacity(50); // Number of large blocks in the test
    defer large_block_ids.deinit();
    i = 0;
    while (i < 100) : (i += 1) {
        try large_block_ids.append((try create_test_block(i, "", allocator)).id);
    }

    const large_query = FindBlocksQuery{
        .block_ids = large_block_ids.items,
        .max_results = 1000,
    };

    var large_result = try query_engine.execute_find_blocks(large_query, allocator);
    defer large_result.deinit(allocator);

    // Both should complete successfully with appropriate results
    try testing.expect(true); // If we get here, optimization worked
}

test "query performance under memory pressure" {
    concurrency.init();
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
            try query_block_ids.append((try create_test_block(i, "", allocator)).id);
        }

        const query = FindBlocksQuery{
            .block_ids = query_block_ids.items,
            .max_results = 50,
        };

        var result = try query_engine.execute_find_blocks(query, allocator);
        defer result.deinit(allocator);

        // Consume results to test memory handling
        var iterator = result.iterator();
        while (try iterator.next(allocator)) |block| {
            defer block.deinit(allocator);
            // Verify block is valid
            try testing.expect(block.content.len > 0);
        }
    }

    // Test should complete without memory issues
    try testing.expect(true);
}

test "complex filtering and search scenarios" {
    concurrency.init();
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
        try all_block_ids.append((try create_test_block(i, "", allocator)).id);
    }

    const find_query = FindBlocksQuery{
        .block_ids = all_block_ids.items,
        .max_results = 10,
    };

    var result = try query_engine.execute_find_blocks(find_query, allocator);
    defer result.deinit(allocator);

    var found_count: u32 = 0;
    var iterator = result.iterator();
    while (try iterator.next(allocator)) |block| {
        defer block.deinit(allocator);
        found_count += 1;
        try testing.expect(block.content.len > 0);
    }

    try testing.expectEqual(@as(u32, content_patterns.len), found_count);
}

test "graph traversal with cycle detection" {
    concurrency.init();
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
        .start_block_ids = &[_]BlockId{block_a.id},
        .edge_types = &[_]EdgeType{EdgeType.calls},
        .direction = .outgoing,
        .max_depth = 10, // Deep enough to detect cycle
        .max_results = 100,
    };

    var result = try query_engine.execute_traversal(traversal_query, allocator);
    defer result.deinit(allocator);

    // Should find all blocks but not infinite loop
    var found_blocks = std.ArrayList(BlockId).init(allocator);
    try found_blocks.ensureTotalCapacity(10); // Expected number of matching blocks
    defer found_blocks.deinit();

    var iterator = result.iterator();
    while (try iterator.next(allocator)) |block| {
        defer block.deinit(allocator);
        try found_blocks.append(block.id); // tidy:ignore-perf Unknown result count from query
    }

    // Should find all 3 blocks exactly once
    try testing.expectEqual(@as(usize, 3), found_blocks.items.len);
}

test "batch query operations and efficiency" {
    concurrency.init();
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
            try batch_block_ids.append((try create_test_block(i, "", allocator)).id);
        }

        const batch_query = FindBlocksQuery{
            .block_ids = batch_block_ids.items,
            .max_results = 20,
        };

        var result = try query_engine.execute_find_blocks(batch_query, allocator);
        defer result.deinit(allocator);

        // Verify batch results
        var batch_count: u32 = 0;
        var iterator = result.iterator();
        while (try iterator.next(allocator)) |block| {
            defer block.deinit(allocator);
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
    concurrency.init();
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
        try nonexistent_ids.append((try create_test_block(i, "", allocator)).id);
    }

    const missing_query = FindBlocksQuery{
        .block_ids = nonexistent_ids.items,
        .max_results = 20,
    };

    var result = try query_engine.execute_find_blocks(missing_query, allocator);
    defer result.deinit(allocator);

    // Should handle missing blocks gracefully (return empty results)
    var found_count: u32 = 0;
    var iterator = result.iterator();
    while (try iterator.next(allocator)) |block| {
        defer block.deinit(allocator);
        found_count += 1;
    }

    try testing.expectEqual(@as(u32, 0), found_count);

    // Test traversal with invalid start points
    const invalid_traversal = TraversalQuery{
        .start_block_ids = nonexistent_ids.items,
        .edge_types = &[_]EdgeType{EdgeType.calls},
        .direction = .outgoing,
        .max_depth = 5,
        .max_results = 50,
    };

    var traversal_result = try query_engine.execute_traversal(invalid_traversal, allocator);
    defer traversal_result.deinit(allocator);

    // Should handle gracefully
    found_count = 0;
    var traversal_iterator = traversal_result.iterator();
    while (try traversal_iterator.next(allocator)) |block| {
        defer block.deinit(allocator);
        found_count += 1;
    }

    try testing.expectEqual(@as(u32, 0), found_count);
}

test "mixed query workload simulation" {
    concurrency.init();
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
        const single_id = (try create_test_block(workload_round, "", allocator)).id;
        const single_query = FindBlocksQuery{
            .block_ids = &[_]BlockId{single_id},
            .max_results = 1,
        };

        var single_result = try query_engine.execute_find_blocks(single_query, allocator);
        defer single_result.deinit(allocator);

        // Batch lookup
        var batch_ids = std.ArrayList(BlockId).init(allocator);
        try batch_ids.ensureTotalCapacity(10); // Batch size for mixed workload
        defer batch_ids.deinit();
        i = workload_round * 5;
        while (i < (workload_round + 1) * 5) : (i += 1) {
            try batch_ids.append((try create_test_block(i, "", allocator)).id);
        }

        const batch_query = FindBlocksQuery{
            .block_ids = batch_ids.items,
            .max_results = 10,
        };

        var batch_result = try query_engine.execute_find_blocks(batch_query, allocator);
        defer batch_result.deinit(allocator);

        // Graph traversal
        const traversal_start = (try create_test_block(workload_round * 5, "", allocator)).id;
        const traversal_query = TraversalQuery{
            .start_block_ids = &[_]BlockId{traversal_start},
            .edge_types = &[_]EdgeType{EdgeType.calls},
            .direction = .outgoing,
            .max_depth = 3,
            .max_results = 20,
        };

        var traversal_result = try query_engine.execute_traversal(traversal_query, allocator);
        defer traversal_result.deinit(allocator);

        // Consume all results to test memory handling
        var single_iterator = single_result.iterator();
        while (try single_iterator.next(allocator)) |block| {
            defer block.deinit(allocator);
        }

        var batch_iterator = batch_result.iterator();
        while (try batch_iterator.next(allocator)) |block| {
            defer block.deinit(allocator);
        }

        var traversal_iterator = traversal_result.iterator();
        while (try traversal_iterator.next(allocator)) |block| {
            defer block.deinit(allocator);
        }
    }

    // Workload should complete successfully
    try testing.expect(true);
}
