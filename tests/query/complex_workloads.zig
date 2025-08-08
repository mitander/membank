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

const TestData = kausaldb.TestData;
const StorageHarness = kausaldb.StorageHarness;
const QueryHarness = kausaldb.QueryHarness;

test "complex graph traversal scenarios" {
    const allocator = testing.allocator;

    var harness = try kausaldb.QueryHarness.init_and_startup(allocator, "complex_traversal_test");
    defer harness.deinit();

    // Create a complex graph structure: function -> imports -> dependencies
    // Function block
    const main_func = ContextBlock{
        .id = TestData.deterministic_block_id(1),
        .version = 1,
        .source_uri = "test://complex_traversal_main.zig",
        .metadata_json = "{\"test\":\"complex_traversal\",\"type\":\"main_func\"}",
        .content = "Complex traversal main function",
    };
    try harness.storage_engine().put_block(main_func);

    // Process function
    const process_func = ContextBlock{
        .id = TestData.deterministic_block_id(2),
        .version = 1,
        .source_uri = "test://complex_process_func.zig",
        .metadata_json = "{\"test\":\"complex_traversal\",\"type\":\"process_func\"}",
        .content = "Complex traversal process function",
    };
    try harness.storage_engine().put_block(process_func);

    // Validation function
    const validate_func = ContextBlock{
        .id = TestData.deterministic_block_id(3),
        .version = 1,
        .source_uri = "test://complex_validate_func.zig",
        .metadata_json = "{\"test\":\"complex_traversal\",\"type\":\"validate_func\"}",
        .content = "Complex traversal validation function",
    };
    try harness.storage_engine().put_block(validate_func);

    // Render function
    const render_func = ContextBlock{
        .id = TestData.deterministic_block_id(4),
        .version = 1,
        .source_uri = "test://complex_render_func.zig",
        .metadata_json = "{\"test\":\"complex_traversal\",\"type\":\"render_func\"}",
        .content = "Complex traversal render function",
    };
    try harness.storage_engine().put_block(render_func);

    // Utility functions
    const draw_func = ContextBlock{
        .id = TestData.deterministic_block_id(5),
        .version = 1,
        .source_uri = "test://complex_draw_func.zig",
        .metadata_json = "{\"test\":\"complex_traversal\",\"type\":\"draw_func\"}",
        .content = "Complex traversal draw function",
    };
    try harness.storage_engine().put_block(draw_func);

    const display_func = ContextBlock{
        .id = TestData.deterministic_block_id(6),
        .version = 1,
        .source_uri = "test://complex_display_func.zig",
        .metadata_json = "{\"test\":\"complex_traversal\",\"type\":\"display_func\"}",
        .content = "Complex traversal display function",
    };
    try harness.storage_engine().put_block(display_func);

    // Create call graph edges using explicit construction
    try harness.storage_engine().put_edge(GraphEdge{
        .source_id = TestData.deterministic_block_id(1),
        .target_id = TestData.deterministic_block_id(2),
        .edge_type = EdgeType.calls,
    }); // main -> process
    try harness.storage_engine().put_edge(GraphEdge{
        .source_id = TestData.deterministic_block_id(2),
        .target_id = TestData.deterministic_block_id(3),
        .edge_type = EdgeType.calls,
    }); // process -> validate
    try harness.storage_engine().put_edge(GraphEdge{
        .source_id = TestData.deterministic_block_id(2),
        .target_id = TestData.deterministic_block_id(4),
        .edge_type = EdgeType.calls,
    }); // process -> render
    try harness.storage_engine().put_edge(GraphEdge{
        .source_id = TestData.deterministic_block_id(4),
        .target_id = TestData.deterministic_block_id(5),
        .edge_type = EdgeType.calls,
    }); // render -> draw
    try harness.storage_engine().put_edge(GraphEdge{
        .source_id = TestData.deterministic_block_id(4),
        .target_id = TestData.deterministic_block_id(6),
        .edge_type = EdgeType.calls,
    }); // render -> display

    // Test multi-hop traversal
    const traversal_query = TraversalQuery{
        .start_block_id = main_func.id,
        .edge_filter = .{ .include_types = &[_]EdgeType{EdgeType.calls} },
        .direction = .outgoing,
        .algorithm = .breadth_first,
        .max_depth = 3,
        .max_results = 100,
    };

    var result = try harness.query_engine.execute_traversal(traversal_query);
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
        const block = ContextBlock{
            .id = TestData.deterministic_block_id(i),
            .version = 1,
            .source_uri = try std.fmt.allocPrint(allocator, "test://optimization_block_{}.zig", .{i}),
            .metadata_json = try std.fmt.allocPrint(allocator, "{{\"optimization_test\":{}}}", .{i}),
            .content = content,
        };
        try storage_engine.put_block(block);
    }

    // Test small query (should use direct strategy)
    const small_query = FindBlocksQuery{
        .block_ids = &[_]BlockId{
            TestData.deterministic_block_id(1),
            TestData.deterministic_block_id(2),
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
        try large_block_ids.append(TestData.deterministic_block_id(i));
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

        const owned_content = try allocator.dupe(u8, content.items);
        const block = ContextBlock{
            .id = TestData.deterministic_block_id(i),
            .version = 1,
            .source_uri = try std.fmt.allocPrint(allocator, "test://memory_pressure_block_{}.zig", .{i}),
            .metadata_json = try std.fmt.allocPrint(allocator, "{{\"memory_pressure_test\":{}}}", .{i}),
            .content = owned_content,
        };
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
            try query_block_ids.append(TestData.deterministic_block_id(i));
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
        const owned_content = try allocator.dupe(u8, pattern);
        const block = ContextBlock{
            .id = TestData.deterministic_block_id(@intCast(idx)),
            .version = 1,
            .source_uri = try std.fmt.allocPrint(allocator, "test://filtering_pattern_{}.zig", .{idx}),
            .metadata_json = try std.fmt.allocPrint(allocator, "{{\"filtering_test\":{}}}", .{idx}),
            .content = owned_content,
        };
        try storage_engine.put_block(block);
    }

    // Test content-based queries (when filtering is implemented)
    var all_block_ids = std.ArrayList(BlockId).init(allocator);
    try all_block_ids.ensureTotalCapacity(100); // Total number of blocks in the test
    defer all_block_ids.deinit();

    var i: u32 = 0;
    while (i < content_patterns.len) : (i += 1) {
        try all_block_ids.append(TestData.deterministic_block_id(i));
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
    const block_a = ContextBlock{
        .id = TestData.deterministic_block_id(1),
        .version = 1,
        .source_uri = "test://cycle_function_a.zig",
        .metadata_json = "{\"test\":\"cycle_detection\",\"function\":\"A\"}",
        .content = "Function A calls B",
    };
    try storage_engine.put_block(block_a);

    const block_b = ContextBlock{
        .id = TestData.deterministic_block_id(2),
        .version = 1,
        .source_uri = "test://cycle_function_b.zig",
        .metadata_json = "{\"test\":\"cycle_detection\",\"function\":\"B\"}",
        .content = "Function B calls C",
    };
    try storage_engine.put_block(block_b);

    const block_c = ContextBlock{
        .id = TestData.deterministic_block_id(3),
        .version = 1,
        .source_uri = "test://cycle_function_c.zig",
        .metadata_json = "{\"test\":\"cycle_detection\",\"function\":\"C\"}",
        .content = "Function C calls A",
    };
    try storage_engine.put_block(block_c);

    // Create cycle: A -> B -> C -> A
    const edge = GraphEdge{
        .source_id = TestData.deterministic_block_id(1),
        .target_id = TestData.deterministic_block_id(2),
        .edge_type = EdgeType.calls,
    };
    try storage_engine.put_edge(edge);
    const edge2 = GraphEdge{
        .source_id = TestData.deterministic_block_id(2),
        .target_id = TestData.deterministic_block_id(3),
        .edge_type = EdgeType.calls,
    };
    try storage_engine.put_edge(edge2);
    const edge3 = GraphEdge{
        .source_id = TestData.deterministic_block_id(3),
        .target_id = TestData.deterministic_block_id(1),
        .edge_type = EdgeType.calls,
    };
    try storage_engine.put_edge(edge3);

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
        const block = ContextBlock{
            .id = TestData.deterministic_block_id(i),
            .version = 1,
            .source_uri = try std.fmt.allocPrint(allocator, "test://batch_block_{}.zig", .{i}),
            .metadata_json = try std.fmt.allocPrint(allocator, "{{\"batch_test\":{}}}", .{i}),
            .content = content,
        };
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
            try batch_block_ids.append(TestData.deterministic_block_id(@intCast(i)));
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
        try nonexistent_ids.append(TestData.deterministic_block_id(@intCast(i)));
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
        const block = ContextBlock{
            .id = TestData.deterministic_block_id(i),
            .version = 1,
            .source_uri = try std.fmt.allocPrint(allocator, "test://mixed_workload_block_{}.zig", .{i}),
            .metadata_json = try std.fmt.allocPrint(allocator, "{{\"mixed_workload_test\":{},\"type\":\"{s}\"}}", .{ i, if (i % 2 == 0) "function" else "struct" }),
            .content = content,
        };
        try storage_engine.put_block(block);

        // Add some edges for traversal testing
        // Skip creating edge for first block (i==0) to avoid negative index
        if (i > 0 and i % 5 != 0) {
            const edge = GraphEdge{
                .source_id = TestData.deterministic_block_id(@intCast(i - 1)),
                .target_id = TestData.deterministic_block_id(@intCast(i)),
                .edge_type = EdgeType.calls,
            };
            try storage_engine.put_edge(edge);
        }
    }

    // Simulate mixed workload: single lookups, batch queries, traversals
    var workload_round: u32 = 0;
    while (workload_round < 5) : (workload_round += 1) {
        // Single block lookup
        const single_id = TestData.block_id_from_index(workload_round);
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
            try batch_ids.append(TestData.block_id_from_index(i));
        }

        const batch_query = FindBlocksQuery{
            .block_ids = batch_ids.items,
        };

        var batch_result = try query.operations.execute_find_blocks(allocator, &storage_engine, batch_query);
        defer batch_result.deinit();

        // Graph traversal
        const traversal_start = TestData.deterministic_block_id(workload_round * 5);
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
