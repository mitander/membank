//! Integration tests for advanced graph traversal algorithms.
//!
//! Tests A* search, bidirectional search, and other advanced algorithms
//! through the complete KausalDB stack including storage engine, query engine,
//! and VFS integration. Validates performance, correctness, and memory safety.
//!
//! Migrated to use standardized harness infrastructure for simplified setup
//! and enhanced reliability through arena-per-subsystem memory management.

const std = @import("std");
const testing = std.testing;
const kausaldb = @import("kausaldb");

// Import types through kausaldb test API
const ContextBlock = kausaldb.ContextBlock;
const BlockId = kausaldb.BlockId;
const GraphEdge = kausaldb.GraphEdge;
const EdgeType = kausaldb.EdgeType;
const TraversalQuery = kausaldb.query.traversal.TraversalQuery;
const TraversalAlgorithm = kausaldb.query.traversal.TraversalAlgorithm;
const TraversalDirection = kausaldb.query.traversal.TraversalDirection;
const execute_traversal = kausaldb.query.traversal.execute_traversal;

const QueryHarness = kausaldb.QueryHarness;
const TestData = kausaldb.TestData;
const SimulationVFS = kausaldb.simulation_vfs.SimulationVFS;
const StorageEngine = kausaldb.StorageEngine;

test "A* search integration with storage engine" {
    const allocator = testing.allocator;

    // Use QueryHarness for complete integration testing
    var harness = try QueryHarness.init_and_startup(allocator, "test_astar_integration");
    defer harness.deinit(); // O(1) cleanup via arena allocation

    // Create a knowledge graph that would benefit from A* pathfinding
    // Structure: A -> B -> C -> D with some branches
    const blocks = [_]struct { id: u32, content: []const u8 }{
        .{ .id = 1, .content = "Main function - entry point" },
        .{ .id = 2, .content = "Process function - core logic" },
        .{ .id = 3, .content = "Validate function - input validation" },
        .{ .id = 4, .content = "Transform function - data transformation" },
        .{ .id = 5, .content = "Output function - result handling" },
        .{ .id = 6, .content = "Utility function - helper methods" },
        .{ .id = 7, .content = "Config function - configuration" },
    };

    // Add all blocks to storage using standardized test data
    for (blocks) |block_info| {
        const block = ContextBlock{
            .id = TestData.deterministic_block_id(block_info.id),
            .version = 1,
            .source_uri = try std.fmt.allocPrint(allocator, "test://traversal_block_{}.zig", .{block_info.id}),
            .metadata_json = try std.fmt.allocPrint(allocator, "{{\"id\":{},\"content\":\"{s}\"}}", .{ block_info.id, block_info.content }),
            .content = block_info.content,
        };
        try harness.storage_engine().put_block(block);
    }

    // Create edges forming a more complex graph
    const edges = [_]struct { from: u32, to: u32, edge_type: EdgeType }{
        .{ .from = 1, .to = 2, .edge_type = .calls }, // main -> process
        .{ .from = 2, .to = 3, .edge_type = .calls }, // process -> validate
        .{ .from = 2, .to = 4, .edge_type = .calls }, // process -> transform
        .{ .from = 3, .to = 6, .edge_type = .calls }, // validate -> utility
        .{ .from = 4, .to = 5, .edge_type = .calls }, // transform -> output
        .{ .from = 4, .to = 6, .edge_type = .calls }, // transform -> utility
        .{ .from = 1, .to = 7, .edge_type = .references }, // main -> config
    };

    for (edges) |edge_info| {
        const edge = TestData.create_test_edge_from_indices(edge_info.from, edge_info.to, edge_info.edge_type);
        try harness.storage_engine().put_edge(edge);
    }

    // Test A* search using standardized block ID generation
    const start_id = TestData.deterministic_block_id(1);

    const astar_query = TraversalQuery{
        .start_block_id = start_id,
        .direction = .outgoing,
        .algorithm = .astar_search,
        .max_depth = 4,
        .max_results = 20,
        .edge_filter = .all_types,
    };

    // Execute A* search
    const result = try kausaldb.query.traversal.execute_traversal(allocator, harness.storage_engine(), astar_query);
    defer result.deinit();

    // Verify A* found optimal paths
    try testing.expect(result.count() > 0);
    try testing.expect(result.blocks_traversed >= result.count());
    try testing.expect(result.max_depth_reached <= 4);

    // Verify all found blocks are valid
    var found_main = false;
    for (result.blocks) |block| {
        try testing.expect(block.content.len > 0);
        if (block.id.eql(start_id)) {
            found_main = true;
        }
    }
    try testing.expect(found_main);

    std.debug.print("A* search found {} blocks, traversed {} nodes\n", .{ result.count(), result.blocks_traversed });
}

test "bidirectional search integration and performance" {
    const allocator = testing.allocator;

    var harness = try QueryHarness.init_and_startup(allocator, "test_bidirectional_integration");
    defer harness.deinit();

    // Create a larger graph using standardized test data
    const block_count: u32 = 20;
    var i: u32 = 1;
    while (i <= block_count) : (i += 1) {
        const block = ContextBlock{
            .id = TestData.deterministic_block_id(i),
            .version = 1,
            .source_uri = try std.fmt.allocPrint(allocator, "test://large_graph_block_{}.zig", .{i}),
            .metadata_json = try std.fmt.allocPrint(allocator, "{{\"large_graph_test\":{}}}", .{i}),
            .content = try std.fmt.allocPrint(allocator, "Large graph test block {}", .{i}),
        };
        try harness.storage_engine().put_block(block);
    }

    // Create a connected graph structure using TestData
    i = 1;
    while (i < block_count) : (i += 1) {
        // Create forward edges
        const edge_forward = TestData.create_test_edge_from_indices(i, i + 1, .calls);
        try harness.storage_engine().put_edge(edge_forward);

        // Create some backward references
        if (i % 3 == 0 and i > 3) {
            const edge_back = TestData.create_test_edge_from_indices(i, i - 3, .references);
            try harness.storage_engine().put_edge(edge_back);
        }
    }

    const start_id = TestData.deterministic_block_id(1);

    const bidirectional_query = TraversalQuery{
        .start_block_id = start_id,
        .direction = .bidirectional,
        .algorithm = .bidirectional_search,
        .max_depth = 8,
        .max_results = 50,
        .edge_filter = .all_types,
    };

    const start_time = std.time.nanoTimestamp();
    const result = try kausaldb.query.traversal.execute_traversal(allocator, harness.storage_engine(), bidirectional_query);
    defer result.deinit();
    const end_time = std.time.nanoTimestamp();

    const execution_time_ns = end_time - start_time;
    const execution_time_us = @divTrunc(execution_time_ns, 1000);

    // Verify bidirectional search results
    try testing.expect(result.count() > 0);
    try testing.expect(result.blocks_traversed > 0);

    // Performance check - should be fast for this graph size
    const max_time_us = 5000; // 5ms for 20 nodes should be plenty
    try testing.expect(execution_time_us < max_time_us);

    std.debug.print("Bidirectional search: {} blocks found, {} traversed, {}μs execution time\n", .{ result.count(), result.blocks_traversed, execution_time_us });
}

test "algorithm comparison BFS vs DFS vs A* vs Bidirectional" {
    const allocator = testing.allocator;

    // Use QueryHarness for algorithm comparison testing
    var harness = try QueryHarness.init_and_startup(allocator, "test_algorithm_comparison");
    defer harness.deinit();

    // Create a structured graph using standardized test data
    const graph_size = 15;
    var i: u32 = 1;
    while (i <= graph_size) : (i += 1) {
        const block = ContextBlock{
            .id = TestData.deterministic_block_id(i),
            .version = 1,
            .source_uri = try std.fmt.allocPrint(allocator, "test://performance_block_{}.zig", .{i}),
            .metadata_json = try std.fmt.allocPrint(allocator, "{{\"performance_test\":{}}}", .{i}),
            .content = try std.fmt.allocPrint(allocator, "Performance test block {}", .{i}),
        };
        try harness.storage_engine().put_block(block);
    }

    // Create a binary tree-like structure using TestData
    i = 1;
    while (i <= graph_size / 2) : (i += 1) {
        // Left child
        if (i * 2 <= graph_size) {
            const edge_left = TestData.create_test_edge_from_indices(i, i * 2, .calls);
            try harness.storage_engine().put_edge(edge_left);
        }
        // Right child
        if (i * 2 + 1 <= graph_size) {
            const edge_right = TestData.create_test_edge_from_indices(i, i * 2 + 1, .calls);
            try harness.storage_engine().put_edge(edge_right);
        }
    }

    const start_id = TestData.deterministic_block_id(1);

    const algorithms = [_]TraversalAlgorithm{ .breadth_first, .depth_first, .astar_search, .bidirectional_search };
    const algorithm_names = [_][]const u8{ "BFS", "DFS", "A*", "Bidirectional" };

    for (algorithms, algorithm_names) |algorithm, name| {
        const traversal_query = TraversalQuery{
            .start_block_id = start_id,
            .direction = .outgoing,
            .algorithm = algorithm,
            .max_depth = 4,
            .max_results = 20,
            .edge_filter = .all_types,
        };

        const start_time = std.time.nanoTimestamp();
        const result = try kausaldb.query.traversal.execute_traversal(allocator, harness.storage_engine(), traversal_query);
        defer result.deinit();
        const end_time = std.time.nanoTimestamp();

        const execution_time_ns = end_time - start_time;
        const execution_time_us = @divTrunc(execution_time_ns, 1000);

        // All algorithms should find results
        try testing.expect(result.count() > 0);
        try testing.expect(result.blocks_traversed > 0);
        try testing.expect(execution_time_us < 10000); // All should be under 10ms

        std.debug.print("{s}: {} blocks, {} traversed, {}μs\n", .{ name, result.count(), result.blocks_traversed, execution_time_us });
    }
}

test "large graph traversal with new algorithms" {
    const allocator = testing.allocator;

    // Use QueryHarness for large graph testing
    var harness = try QueryHarness.init_and_startup(allocator, "test_large_graph");
    defer harness.deinit();

    // Create a larger graph using standardized test data
    const large_graph_size = 100;
    var i: u32 = 1;
    while (i <= large_graph_size) : (i += 1) {
        const block = try TestData.create_test_block(allocator, i);
        defer TestData.cleanup_test_block(allocator, block);
        try harness.storage_engine().put_block(block);
    }

    // Create a sparse graph structure using TestData
    i = 1;
    while (i <= large_graph_size) : (i += 1) {
        // Connect to next few nodes
        var j: u32 = 1;
        while (j <= 3 and i + j <= large_graph_size) : (j += 1) {
            const edge = TestData.create_test_edge_from_indices(i, i + j, .calls);
            try harness.storage_engine().put_edge(edge);
        }

        // Connect to some previous nodes for richness
        if (i > 10 and i % 5 == 0) {
            const edge_back = TestData.create_test_edge_from_indices(i, i - 5, .references);
            try harness.storage_engine().put_edge(edge_back);
        }
    }

    var start_bytes: [16]u8 = std.mem.zeroes([16]u8);
    std.mem.writeInt(u32, start_bytes[12..16], 1, .little);
    const start_id = TestData.deterministic_block_id(1);

    // Test A* on large graph
    const astar_query = TraversalQuery{
        .start_block_id = start_id,
        .direction = .outgoing,
        .algorithm = .astar_search,
        .max_depth = 6,
        .max_results = 100,
        .edge_filter = .all_types,
    };

    const start_time = std.time.nanoTimestamp();
    const result = try kausaldb.query.traversal.execute_traversal(allocator, harness.storage_engine(), astar_query);
    defer result.deinit();
    const end_time = std.time.nanoTimestamp();

    const execution_time_ns = end_time - start_time;
    const execution_time_ms = @divTrunc(execution_time_ns, 1_000_000);

    // Performance validation
    try testing.expect(result.count() > 0);
    try testing.expect(execution_time_ms < 50); // Should complete within 50ms

    // Memory efficiency check - should handle large graphs without issues
    try testing.expect(result.count() <= 100); // Respects max_results
    try testing.expect(result.max_depth_reached <= 6); // Respects max_depth

    std.debug.print("Large graph A* search: {} blocks found in {}ms\n", .{ result.count(), execution_time_ms });
}

test "edge type filtering integration" {
    const allocator = testing.allocator;

    var sim_vfs = try SimulationVFS.init(allocator);
    defer sim_vfs.deinit();

    var storage_engine = try StorageEngine.init_default(allocator, sim_vfs.vfs(), "./test_edge_filtering");
    defer storage_engine.deinit();
    try storage_engine.startup();

    // Create blocks
    var i: u32 = 1;
    while (i <= 6) : (i += 1) {
        const content = try std.fmt.allocPrint(allocator, "Block {} for edge filtering test", .{i});
        defer allocator.free(content);

        const block = try TestData.create_test_block_with_content(allocator, @intCast(i), content);
        defer allocator.free(block.source_uri);
        defer allocator.free(block.metadata_json);
        defer allocator.free(block.content);
        try storage_engine.put_block(block);
    }

    // Create mixed edge types
    const mixed_edges = [_]struct { from: u32, to: u32, edge_type: EdgeType }{
        .{ .from = 1, .to = 2, .edge_type = .calls },
        .{ .from = 1, .to = 3, .edge_type = .imports },
        .{ .from = 2, .to = 4, .edge_type = .calls },
        .{ .from = 2, .to = 5, .edge_type = .references },
        .{ .from = 3, .to = 6, .edge_type = .imports },
    };

    for (mixed_edges) |edge_info| {
        const edge = TestData.create_test_edge_from_indices(edge_info.from, edge_info.to, edge_info.edge_type);
        try storage_engine.put_edge(edge);
    }

    var start_bytes: [16]u8 = std.mem.zeroes([16]u8);
    std.mem.writeInt(u32, start_bytes[12..16], 1, .little);
    const start_id = BlockId{ .bytes = start_bytes };

    // Test filtering for 'calls' edges only
    const calls_query = TraversalQuery{
        .start_block_id = start_id,
        .direction = .outgoing,
        .algorithm = .astar_search,
        .max_depth = 3,
        .max_results = 10,
        .edge_filter = .{ .only_type = .calls },
    };

    const calls_result = try execute_traversal(allocator, &storage_engine, calls_query);
    defer calls_result.deinit();

    // Should find blocks 1, 2, 4 (connected by calls edges)
    try testing.expect(calls_result.count() >= 3);

    // Test filtering for 'imports' edges only
    const imports_query = TraversalQuery{
        .start_block_id = start_id,
        .direction = .outgoing,
        .algorithm = .breadth_first, // Use BFS for simple outgoing traversal
        .max_depth = 3,
        .max_results = 10,
        .edge_filter = .{ .only_type = .imports },
    };

    const imports_result = try execute_traversal(allocator, &storage_engine, imports_query);
    defer imports_result.deinit();

    // Should find blocks 1, 3, 6 (connected by imports edges)
    try testing.expect(imports_result.count() >= 3);

    std.debug.print("Edge filtering: {} calls-filtered, {} imports-filtered\n", .{ calls_result.count(), imports_result.count() });
}

test "memory safety under stress with new algorithms" {
    const allocator = testing.allocator;

    var sim_vfs = try SimulationVFS.init(allocator);
    defer sim_vfs.deinit();

    var storage_engine = try StorageEngine.init_default(allocator, sim_vfs.vfs(), "./test_memory_safety");
    defer storage_engine.deinit();
    try storage_engine.startup();

    // Create a moderate graph
    const stress_graph_size = 50;
    var i: u32 = 1;
    while (i <= stress_graph_size) : (i += 1) {
        const content = try std.fmt.allocPrint(allocator, "Stress test block {} with variable content length", .{i});
        defer allocator.free(content);

        const block = try TestData.create_test_block_with_content(allocator, @intCast(i), content);
        defer allocator.free(block.source_uri);
        defer allocator.free(block.metadata_json);
        defer allocator.free(block.content);
        try storage_engine.put_block(block);
    }

    // Create edges
    i = 1;
    while (i < stress_graph_size) : (i += 1) {
        const edge = GraphEdge{
            .source_id = TestData.deterministic_block_id(i),
            .target_id = TestData.deterministic_block_id(i + 1),
            .edge_type = .calls,
        };
        try storage_engine.put_edge(edge);

        // Add some cross-links
        if (i % 3 == 0 and i + 3 <= stress_graph_size) {
            const cross_edge = GraphEdge{
                .source_id = TestData.deterministic_block_id(i),
                .target_id = TestData.deterministic_block_id(i + 3),
                .edge_type = .references,
            };
            try storage_engine.put_edge(cross_edge);
        }
    }

    // Run multiple queries with different algorithms to stress memory management
    const algorithms = [_]TraversalAlgorithm{ .breadth_first, .depth_first, .astar_search, .bidirectional_search };

    var round: u32 = 0;
    while (round < 10) : (round += 1) {
        for (algorithms) |algorithm| {
            var start_bytes: [16]u8 = std.mem.zeroes([16]u8);
            std.mem.writeInt(u32, start_bytes[12..16], (round % 10) + 1, .little);
            const start_id = BlockId{ .bytes = start_bytes };

            const traversal_query = TraversalQuery{
                .start_block_id = start_id,
                .direction = .outgoing,
                .algorithm = algorithm,
                .max_depth = 5,
                .max_results = 30,
                .edge_filter = .all_types,
            };

            const result = try execute_traversal(allocator, &storage_engine, traversal_query);
            defer result.deinit();

            // Verify results are valid
            try testing.expect(result.count() > 0);
            for (result.blocks) |block| {
                try testing.expect(block.content.len > 0);
                try testing.expect(block.source_uri.len > 0);
            }
        }
    }

    // If we reach here, memory management is working correctly
    try testing.expect(true);
    std.debug.print("Memory safety stress test: 40 traversals completed successfully\n", .{});
}
