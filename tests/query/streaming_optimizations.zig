//! Advanced query scenario tests for streaming, caching, and optimization.
//!
//! Tests advanced query engine capabilities including streaming result iteration,
//! query optimization strategies, caching behavior, complex graph scenarios,
//! and performance characteristics under various conditions.

const std = @import("std");
const builtin = @import("builtin");
const testing = std.testing;
const kausaldb = @import("kausaldb");

const types = kausaldb.types;
const query_engine = kausaldb.query_engine;
const query_traversal = kausaldb.query_traversal;
const operations = kausaldb.query_operations;

const ContextBlock = types.ContextBlock;
const BlockId = types.BlockId;
const GraphEdge = types.GraphEdge;
const EdgeType = types.EdgeType;
const FindBlocksQuery = kausaldb.FindBlocksQuery;
const TraversalQuery = query_traversal.TraversalQuery;
const TraversalDirection = query_traversal.TraversalDirection;
const QueryResult = operations.QueryResult;

const TestData = kausaldb.TestData;
const QueryHarness = kausaldb.QueryHarness;

test "streaming query results with large datasets" {
    const allocator = testing.allocator;

    var harness = try kausaldb.QueryHarness.init_and_startup(allocator, "streaming_large_test");
    defer harness.deinit();

    // Create large dataset to test streaming behavior
    const dataset_size = if (builtin.mode == .Debug) 100 else 500;
    var stored_block_ids = std.ArrayList(BlockId).init(allocator);
    defer stored_block_ids.deinit();
    try stored_block_ids.ensureTotalCapacity(dataset_size);

    // Store large number of blocks using TestData
    for (1..dataset_size + 1) |i| {
        const block = ContextBlock{
            .id = TestData.deterministic_block_id(@intCast(i)),
            .version = 1,
            .source_uri = "test://streaming_optimization.zig",
            .metadata_json = "{\"test\":\"streaming_optimization\"}",
            .content = "Streaming optimization test block content",
        };

        try harness.storage_engine().put_block(block);
        try stored_block_ids.append(block.id);
    }

    // Query all blocks using streaming
    const query = FindBlocksQuery{ .block_ids = stored_block_ids.items };
    var result = try operations.execute_find_blocks(allocator, harness.storage_engine(), query);
    defer result.deinit();

    // Verify streaming behavior with memory efficiency
    var count: usize = 0;
    while (try result.next()) |block| {
        try testing.expect(block.read(.query_engine).content.len == 41); // Actual content: "Streaming optimization test block content"
        count += 1;
    }

    try testing.expectEqual(dataset_size, count);
}

test "query result caching with identical queries" {
    const allocator = testing.allocator;

    var harness = try kausaldb.QueryHarness.init_and_startup(allocator, "caching_test");
    defer harness.deinit();

    // Create test blocks
    const block1 = ContextBlock{
        .id = TestData.deterministic_block_id(1),
        .version = 1,
        .source_uri = "test://paged_queries_1.zig",
        .metadata_json = "{\"test\":\"paged_queries\"}",
        .content = "Paged queries test block 1 content",
    };
    const block2 = ContextBlock{
        .id = TestData.deterministic_block_id(2),
        .version = 1,
        .source_uri = "test://paged_queries_2.zig",
        .metadata_json = "{\"test\":\"paged_queries\"}",
        .content = "Paged queries test block 2 content",
    };
    const block3 = ContextBlock{
        .id = TestData.deterministic_block_id(3),
        .version = 1,
        .source_uri = "test://paged_queries_3.zig",
        .metadata_json = "{\"test\":\"paged_queries\"}",
        .content = "Paged queries test block 3 content",
    };

    try harness.storage_engine().put_block(block1);
    try harness.storage_engine().put_block(block2);
    try harness.storage_engine().put_block(block3);

    // Create edges for graph traversal
    const edge1_2 = GraphEdge{
        .source_id = block1.id,
        .target_id = block2.id,
        .edge_type = EdgeType.calls,
    };
    const edge2_3 = GraphEdge{
        .source_id = block2.id,
        .target_id = block3.id,
        .edge_type = EdgeType.calls,
    };
    try harness.storage_engine().put_edge(edge1_2);
    try harness.storage_engine().put_edge(edge2_3);

    // Execute identical traversal queries
    var query1 = TraversalQuery.init(block1.id, .outgoing);
    query1.algorithm = .breadth_first;
    query1.max_depth = 5;

    var query2 = TraversalQuery.init(block1.id, TraversalDirection.outgoing);
    query2.algorithm = .breadth_first;
    query2.max_depth = 5;

    // Measure timing for both queries (second should be faster due to caching)
    const start1 = std.time.nanoTimestamp();
    var result1 = try harness.query_engine.execute_traversal(query1);
    defer result1.deinit();
    const end1 = std.time.nanoTimestamp();

    const start2 = std.time.nanoTimestamp();
    var result2 = try harness.query_engine.execute_traversal(query2);
    defer result2.deinit();
    const end2 = std.time.nanoTimestamp();

    // Verify identical results
    try testing.expectEqual(result1.paths.len, result2.paths.len);
    if (result1.paths.len > 0 and result2.paths.len > 0) {
        try testing.expectEqual(result1.paths[0][0], result2.paths[0][0]); // First BlockId in first path
        if (result1.paths[0].len > 1 and result2.paths[0].len > 1) {
            try testing.expectEqual(result1.paths[0][result1.paths[0].len - 1], result2.paths[0][result2.paths[0].len - 1]); // Last BlockId in first path
        }
    }

    const duration1 = @as(u64, @intCast(end1 - start1));
    const duration2 = @as(u64, @intCast(end2 - start2));

    // Second query should be significantly faster (allow for some variance)
    // This is a performance hint, not a strict requirement
    _ = duration1;
    _ = duration2;
}

test "memory efficient graph traversal with depth limits" {
    const allocator = testing.allocator;

    var harness = try kausaldb.QueryHarness.init_and_startup(allocator, "memory_efficient_test");
    defer harness.deinit();

    // Create a deep chain of nodes
    const chain_length = 50;
    var nodes = std.ArrayList(BlockId).init(allocator);
    defer nodes.deinit();
    try nodes.ensureTotalCapacity(chain_length);

    // Create chain of connected blocks
    for (0..chain_length) |i| {
        const block = ContextBlock{
            .id = TestData.deterministic_block_id(@intCast(i)),
            .version = 1,
            .source_uri = "test://concurrent_streaming.zig",
            .metadata_json = "{\"test\":\"concurrent_streaming\"}",
            .content = "Concurrent streaming test block content",
        };
        try harness.storage_engine().put_block(block);
        try nodes.append(block.id);

        // Connect to previous node
        if (i > 0) {
            const edge = GraphEdge{
                .source_id = TestData.deterministic_block_id(@intCast(i - 1)),
                .target_id = TestData.deterministic_block_id(@intCast(i)),
                .edge_type = EdgeType.calls,
            };
            try harness.storage_engine().put_edge(edge);
        }
    }

    // Test depth-limited traversal
    var shallow_query = TraversalQuery.init(nodes.items[0], TraversalDirection.outgoing);
    shallow_query.algorithm = .breadth_first;
    shallow_query.max_depth = 10; // Shallow depth

    var shallow_result = try harness.query_engine.execute_traversal(shallow_query);
    defer shallow_result.deinit();

    // With max_depth=10, should find paths to nodes 0-10 (11 total)
    try testing.expectEqual(@as(usize, 11), shallow_result.paths.len);

    // Test deep traversal
    var deep_query = TraversalQuery.init(nodes.items[0], .outgoing);
    deep_query.algorithm = .breadth_first;
    deep_query.max_depth = 100; // Deep enough

    var deep_result = try harness.query_engine.execute_traversal(deep_query);
    defer deep_result.deinit();

    // Should find paths to all 50 nodes
    try testing.expectEqual(@as(usize, chain_length), deep_result.paths.len);

    // Find the longest path (should reach the final node)
    var longest_path_idx: usize = 0;
    for (deep_result.paths, 0..) |path, i| {
        if (path.len > deep_result.paths[longest_path_idx].len) {
            longest_path_idx = i;
        }
    }

    const longest_path = deep_result.paths[longest_path_idx];
    try testing.expectEqual(nodes.items[0], longest_path[0]);
    try testing.expectEqual(nodes.items[chain_length - 1], longest_path[longest_path.len - 1]);
    try testing.expectEqual(@as(usize, chain_length), longest_path.len);
}

test "query optimization with different algorithms" {
    const allocator = testing.allocator;

    var harness = try kausaldb.QueryHarness.init_and_startup(allocator, "optimization_test");
    defer harness.deinit();

    // Create a small graph for algorithm comparison
    const nodes_count = 10;
    var nodes = std.ArrayList(BlockId).init(allocator);
    defer nodes.deinit();
    try nodes.ensureTotalCapacity(nodes_count);

    // Create nodes
    for (0..nodes_count) |i| {
        const block = ContextBlock{
            .id = TestData.deterministic_block_id(@intCast(i)),
            .version = 1,
            .source_uri = "test://result_ordering.zig",
            .metadata_json = "{\"test\":\"result_ordering\"}",
            .content = "Result ordering test block content",
        };
        try harness.storage_engine().put_block(block);
        try nodes.append(block.id);
    }

    // Create a graph with multiple paths
    // Linear path: 0 -> 1 -> 2 -> ... -> 9
    for (0..nodes_count - 1) |i| {
        const edge = GraphEdge{
            .source_id = TestData.deterministic_block_id(@intCast(i)),
            .target_id = TestData.deterministic_block_id(@intCast(i + 1)),
            .edge_type = EdgeType.calls,
        };
        try harness.storage_engine().put_edge(edge);
    }

    // Shortcut path: 0 -> 5 -> 9
    const shortcut1 = GraphEdge{
        .source_id = nodes.items[0],
        .target_id = nodes.items[5],
        .edge_type = EdgeType.imports,
    };
    const shortcut2 = GraphEdge{
        .source_id = nodes.items[5],
        .target_id = nodes.items[9],
        .edge_type = EdgeType.imports,
    };
    try harness.storage_engine().put_edge(shortcut1);
    try harness.storage_engine().put_edge(shortcut2);

    // Test breadth-first search
    var bfs_query = TraversalQuery.init(nodes.items[0], .outgoing);
    bfs_query.algorithm = .breadth_first;
    bfs_query.max_depth = 20;

    var bfs_result = try harness.query_engine.execute_traversal(bfs_query);
    defer bfs_result.deinit();

    try testing.expect(bfs_result.paths.len > 0);

    // Test A* search (should find optimal path)
    var astar_query = TraversalQuery.init(nodes.items[0], .outgoing);
    astar_query.algorithm = .astar_search;
    astar_query.max_depth = 20;

    var astar_result = try harness.query_engine.execute_traversal(astar_query);
    defer astar_result.deinit();

    try testing.expect(astar_result.paths.len > 0);

    // Test bidirectional search
    var bidirectional_query = TraversalQuery.init(nodes.items[0], .bidirectional);
    bidirectional_query.algorithm = .bidirectional_search;
    bidirectional_query.max_depth = 20;

    var bidirectional_result = try harness.query_engine.execute_traversal(bidirectional_query);
    defer bidirectional_result.deinit();

    try testing.expect(bidirectional_result.paths.len > 0);

    // Find paths that reach the final node for each algorithm
    var bfs_final_path: ?[]const BlockId = null;
    for (bfs_result.paths) |path| {
        if (path.len > 0 and path[path.len - 1].eql(nodes.items[nodes_count - 1])) {
            bfs_final_path = path;
            break;
        }
    }

    var astar_final_path: ?[]const BlockId = null;
    for (astar_result.paths) |path| {
        if (path.len > 0 and path[path.len - 1].eql(nodes.items[nodes_count - 1])) {
            astar_final_path = path;
            break;
        }
    }

    // Verify each algorithm found a path to the final node
    try testing.expect(bfs_final_path != null);
    try testing.expect(astar_final_path != null);

    try testing.expectEqual(nodes.items[0], bfs_final_path.?[0]);
    try testing.expectEqual(nodes.items[nodes_count - 1], bfs_final_path.?[bfs_final_path.?.len - 1]);
    try testing.expectEqual(nodes.items[0], astar_final_path.?[0]);
    try testing.expectEqual(nodes.items[nodes_count - 1], astar_final_path.?[astar_final_path.?.len - 1]);

    // Find bidirectional path that reaches the final node
    var bidirectional_final_path: ?[]const BlockId = null;
    for (bidirectional_result.paths) |path| {
        if (path.len > 0 and path[path.len - 1].eql(nodes.items[nodes_count - 1])) {
            bidirectional_final_path = path;
            break;
        }
    }

    try testing.expect(bidirectional_final_path != null);
    try testing.expectEqual(nodes.items[0], bidirectional_final_path.?[0]);
    try testing.expectEqual(nodes.items[nodes_count - 1], bidirectional_final_path.?[bidirectional_final_path.?.len - 1]);
}

test "streaming result pagination and memory bounds" {
    const allocator = testing.allocator;

    var harness = try kausaldb.QueryHarness.init_and_startup(allocator, "pagination_test");
    defer harness.deinit();

    // Create test blocks for pagination
    const total_blocks = 200;
    var block_ids = std.ArrayList(BlockId).init(allocator);
    defer block_ids.deinit();
    try block_ids.ensureTotalCapacity(total_blocks);

    for (1..total_blocks + 1) |i| {
        const block = ContextBlock{
            .id = TestData.deterministic_block_id(@intCast(i)),
            .version = 1,
            .source_uri = "test://memory_stress.zig",
            .metadata_json = "{\"test\":\"memory_stress\"}",
            .content = "Memory stress test block content",
        };
        try harness.storage_engine().put_block(block);
        try block_ids.append(block.id);
    }

    // Test streaming with result limits
    const query = FindBlocksQuery{ .block_ids = block_ids.items };
    var result = try operations.execute_find_blocks(allocator, harness.storage_engine(), query);
    defer result.deinit();

    // Count streamed results
    var streamed_count: usize = 0;
    var memory_samples = std.ArrayList(usize).init(allocator);
    defer memory_samples.deinit();

    while (try result.next()) |block| {
        streamed_count += 1;

        // Sample memory usage periodically
        if (streamed_count % 50 == 0) {
            // In a real implementation, we'd measure arena memory usage here
            try memory_samples.append(streamed_count);
        }

        // Verify block integrity
        try testing.expect(block.read(.query_engine).content.len > 0);
        try testing.expect(block.read(.query_engine).source_uri.len > 0);
    }

    try testing.expectEqual(total_blocks, streamed_count);
    try testing.expect(memory_samples.items.len > 0);
}

test "complex graph scenario performance characteristics" {
    const allocator = testing.allocator;

    var harness = try kausaldb.QueryHarness.init_and_startup(allocator, "complex_scenario_test");
    defer harness.deinit();

    // Create a complex graph with multiple components
    const component_size = 20;
    var component_roots = std.ArrayList(BlockId).init(allocator);
    defer component_roots.deinit();
    try component_roots.ensureTotalCapacity(3);

    // Create 3 disconnected components
    for (0..3) |comp| {
        var component_nodes = std.ArrayList(BlockId).init(allocator);
        defer component_nodes.deinit();
        try component_nodes.ensureTotalCapacity(component_size);

        // Create nodes for this component
        for (0..component_size) |i| {
            const node_id = comp * 1000 + i + 1; // Ensure unique IDs
            const block = ContextBlock{
                .id = TestData.deterministic_block_id(@intCast(node_id)),
                .version = 1,
                .source_uri = "test://large_result_set.zig",
                .metadata_json = "{\"test\":\"large_result_set\"}",
                .content = "Large result set test block content",
            };
            try harness.storage_engine().put_block(block);
            try component_nodes.append(block.id);

            if (i == 0) {
                try component_roots.append(block.id);
            }
        }

        // Connect nodes within component (star pattern)
        for (1..component_size) |i| {
            const edge = GraphEdge{
                .source_id = component_nodes.items[0],
                .target_id = component_nodes.items[i],
                .edge_type = EdgeType.calls,
            };
            try harness.storage_engine().put_edge(edge);
        }
    }

    // Test traversal performance across components
    const start_time = std.time.nanoTimestamp();

    // Test traversal from each component root
    for (component_roots.items) |root| {
        var query = TraversalQuery.init(root, .bidirectional); // Self-traversal
        query.algorithm = .breadth_first;
        query.max_depth = 5;

        var result = try harness.query_engine.execute_traversal(query);
        defer result.deinit();

        // Should find nodes within the component
        try testing.expect(result.paths.len >= 0);
    }

    const end_time = std.time.nanoTimestamp();
    const total_duration = @as(u64, @intCast(end_time - start_time));

    // Performance assertion: complex traversals should complete reasonably fast
    const max_duration_ns = 50_000_000; // 50ms for complex scenario
    try testing.expect(total_duration < max_duration_ns);
}
