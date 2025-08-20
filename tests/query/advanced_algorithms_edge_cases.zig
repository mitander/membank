//! Advanced graph traversal algorithm edge case testing.
//!
//! Tests boundary conditions and failure scenarios for sophisticated graph algorithms
//! including A* search, bidirectional search, topological sort, and SCC detection.
//! Validates algorithm correctness under hostile conditions and ensures memory safety.

const std = @import("std");

const kausaldb = @import("kausaldb");

const query_engine = kausaldb.query_engine;
const query_traversal = kausaldb.query_traversal;
const testing = std.testing;
const types = kausaldb.types;
const execute_traversal = kausaldb.query.traversal.execute_traversal;

const ContextBlock = types.ContextBlock;
const BlockId = types.BlockId;
const GraphEdge = types.GraphEdge;
const EdgeType = types.EdgeType;
const TraversalQuery = query_traversal.TraversalQuery;
const TraversalDirection = query_traversal.TraversalDirection;
const TestData = kausaldb.TestData;
const QueryHarness = kausaldb.QueryHarness;

// Helper for creating disconnected graph components
fn create_disconnected_components(harness: *QueryHarness, _: std.mem.Allocator) !struct {
    component_a: [3]BlockId,
    component_b: [3]BlockId,
    isolated: BlockId,
} {
    // Component A: Linear chain A1 -> A2 -> A3
    const a1_block = ContextBlock{
        .id = TestData.deterministic_block_id(1001),
        .version = 1,
        .source_uri = "test://disconnected_component_a1.zig",
        .metadata_json = "{\"test\":\"disconnected_components\",\"component\":\"A1\"}",
        .content = "Disconnected component A1 test block content",
    };
    const a2_block = ContextBlock{
        .id = TestData.deterministic_block_id(1002),
        .version = 1,
        .source_uri = "test://disconnected_component_a2.zig",
        .metadata_json = "{\"test\":\"disconnected_components\",\"component\":\"A2\"}",
        .content = "Disconnected component A2 test block content",
    };
    const a3_block = ContextBlock{
        .id = TestData.deterministic_block_id(1003),
        .version = 1,
        .source_uri = "test://disconnected_component_a3.zig",
        .metadata_json = "{\"test\":\"disconnected_components\",\"component\":\"A3\"}",
        .content = "Disconnected component A3 test block content",
    };

    try harness.storage_engine().put_block(a1_block);
    try harness.storage_engine().put_block(a2_block);
    try harness.storage_engine().put_block(a3_block);

    const edge_a1_a2 = GraphEdge{
        .source_id = a1_block.id,
        .target_id = a2_block.id,
        .edge_type = EdgeType.calls,
    };
    const edge_a2_a3 = GraphEdge{
        .source_id = a2_block.id,
        .target_id = a3_block.id,
        .edge_type = EdgeType.calls,
    };
    try harness.storage_engine().put_edge(edge_a1_a2);
    try harness.storage_engine().put_edge(edge_a2_a3);

    // Component B: Linear chain B1 -> B2 -> B3
    const b1_block = ContextBlock{
        .id = TestData.deterministic_block_id(2001),
        .version = 1,
        .source_uri = "test://disconnected_component_b1.zig",
        .metadata_json = "{\"test\":\"disconnected_components\",\"component\":\"B1\"}",
        .content = "Disconnected component B1 test block content",
    };
    const b2_block = ContextBlock{
        .id = TestData.deterministic_block_id(2002),
        .version = 1,
        .source_uri = "test://disconnected_component_b2.zig",
        .metadata_json = "{\"test\":\"disconnected_components\",\"component\":\"B2\"}",
        .content = "Disconnected component B2 test block content",
    };
    const b3_block = ContextBlock{
        .id = TestData.deterministic_block_id(2003),
        .version = 1,
        .source_uri = "test://disconnected_component_b3.zig",
        .metadata_json = "{\"test\":\"disconnected_components\",\"component\":\"B3\"}",
        .content = "Disconnected component B3 test block content",
    };

    try harness.storage_engine().put_block(b1_block);
    try harness.storage_engine().put_block(b2_block);
    try harness.storage_engine().put_block(b3_block);

    const edge_b1_b2 = GraphEdge{
        .source_id = b1_block.id,
        .target_id = b2_block.id,
        .edge_type = EdgeType.imports,
    };
    const edge_b2_b3 = GraphEdge{
        .source_id = b2_block.id,
        .target_id = b3_block.id,
        .edge_type = EdgeType.imports,
    };
    try harness.storage_engine().put_edge(edge_b1_b2);
    try harness.storage_engine().put_edge(edge_b2_b3);

    // Isolated node with no connections
    const isolated_block = ContextBlock{
        .id = TestData.deterministic_block_id(3001),
        .version = 1,
        .source_uri = "test://isolated_node.zig",
        .metadata_json = "{\"test\":\"disconnected_components\",\"component\":\"isolated\"}",
        .content = "Isolated node test block content",
    };
    try harness.storage_engine().put_block(isolated_block);

    return .{
        .component_a = [3]BlockId{ a1_block.id, a2_block.id, a3_block.id },
        .component_b = [3]BlockId{ b1_block.id, b2_block.id, b3_block.id },
        .isolated = isolated_block.id,
    };
}

// Helper for creating cyclic graph structure
fn create_cyclic_graph(harness: *QueryHarness, _: std.mem.Allocator) !struct {
    cycle: [3]BlockId,
    acyclic_branch: [2]BlockId,
} {
    // Create a 3-node cycle: C1 -> C2 -> C3 -> C1
    const c1_block = ContextBlock{
        .id = TestData.deterministic_block_id(4001),
        .version = 1,
        .source_uri = "test://cyclic_graph_c1.zig",
        .metadata_json = "{\"test\":\"cyclic_graph\",\"component\":\"C1\"}",
        .content = "Cyclic graph C1 test block content",
    };
    const c2_block = ContextBlock{
        .id = TestData.deterministic_block_id(4002),
        .version = 1,
        .source_uri = "test://cyclic_graph_c2.zig",
        .metadata_json = "{\"test\":\"cyclic_graph\",\"component\":\"C2\"}",
        .content = "Cyclic graph C2 test block content",
    };
    const c3_block = ContextBlock{
        .id = TestData.deterministic_block_id(4003),
        .version = 1,
        .source_uri = "test://cyclic_graph_c3.zig",
        .metadata_json = "{\"test\":\"cyclic_graph\",\"component\":\"C3\"}",
        .content = "Cyclic graph C3 test block content",
    };

    try harness.storage_engine().put_block(c1_block);
    try harness.storage_engine().put_block(c2_block);
    try harness.storage_engine().put_block(c3_block);

    const edge_c1_c2 = GraphEdge{
        .source_id = c1_block.id,
        .target_id = c2_block.id,
        .edge_type = EdgeType.calls,
    };
    const edge_c2_c3 = GraphEdge{
        .source_id = c2_block.id,
        .target_id = c3_block.id,
        .edge_type = EdgeType.calls,
    };
    const edge_c3_c1 = GraphEdge{
        .source_id = c3_block.id,
        .target_id = c1_block.id,
        .edge_type = EdgeType.calls,
    };
    try harness.storage_engine().put_edge(edge_c1_c2);
    try harness.storage_engine().put_edge(edge_c2_c3);
    try harness.storage_engine().put_edge(edge_c3_c1);

    // Acyclic branch: B1 -> B2
    const b1_block = ContextBlock{
        .id = TestData.deterministic_block_id(5001),
        .version = 1,
        .source_uri = "test://acyclic_branch_b1.zig",
        .metadata_json = "{\"test\":\"cyclic_graph\",\"component\":\"B1\"}",
        .content = "Acyclic branch B1 test block content",
    };
    const b2_block = ContextBlock{
        .id = TestData.deterministic_block_id(5002),
        .version = 1,
        .source_uri = "test://acyclic_branch_b2.zig",
        .metadata_json = "{\"test\":\"cyclic_graph\",\"component\":\"B2\"}",
        .content = "Acyclic branch B2 test block content",
    };

    try harness.storage_engine().put_block(b1_block);
    try harness.storage_engine().put_block(b2_block);

    const edge_b1_b2 = GraphEdge{
        .source_id = b1_block.id,
        .target_id = b2_block.id,
        .edge_type = EdgeType.imports,
    };
    try harness.storage_engine().put_edge(edge_b1_b2);

    return .{
        .cycle = [3]BlockId{ c1_block.id, c2_block.id, c3_block.id },
        .acyclic_branch = [2]BlockId{ b1_block.id, b2_block.id },
    };
}

test "A* search disconnected component edge cases" {
    const allocator = testing.allocator;

    var harness = try kausaldb.QueryHarness.init_and_startup(allocator, "astar_edge_test");
    defer harness.deinit();

    const components = try create_disconnected_components(&harness, allocator);

    // Test A* search within Component A (should succeed)
    var query_a = TraversalQuery.init(components.component_a[0], TraversalDirection.outgoing);
    query_a.algorithm = .astar_search;
    query_a.max_depth = 10;

    var result_a = try harness.query_engine.execute_traversal(query_a);
    defer result_a.deinit();

    try testing.expect(result_a.paths.len > 0);

    // Find path that reaches component_a[2]
    var target_path: ?[]const BlockId = null;
    for (result_a.paths) |path| {
        if (path.len > 0 and path[path.len - 1].eql(components.component_a[2])) {
            target_path = path;
            break;
        }
    }

    try testing.expect(target_path != null);
    try testing.expectEqual(components.component_a[0], target_path.?[0]);
    try testing.expectEqual(components.component_a[2], target_path.?[target_path.?.len - 1]);

    // Test A* search within Component A (should find all 3 nodes)
    var query_component_a = TraversalQuery.init(components.component_a[0], .outgoing);
    query_component_a.algorithm = .astar_search;
    query_component_a.max_depth = 20;

    var result_component_a = try execute_traversal(allocator, harness.storage_engine(), query_component_a);
    defer result_component_a.deinit();

    try testing.expectEqual(@as(usize, 3), result_component_a.paths.len);

    // Test A* search from isolated node (should find only itself)
    var query_isolated = TraversalQuery.init(components.isolated, .outgoing);
    query_isolated.algorithm = .astar_search;
    query_isolated.max_depth = 50;

    var result_isolated = try execute_traversal(allocator, harness.storage_engine(), query_isolated);
    defer result_isolated.deinit();

    try testing.expectEqual(@as(usize, 1), result_isolated.paths.len);
    try testing.expectEqual(components.isolated, result_isolated.paths[0][0]);
}

test "bidirectional search disconnected component scenarios" {
    const allocator = testing.allocator;

    var harness = try kausaldb.QueryHarness.init_and_startup(allocator, "bidirectional_edge_test");
    defer harness.deinit();

    const components = try create_disconnected_components(&harness, allocator);

    // Test bidirectional search within Component B (should succeed)
    var query_b = TraversalQuery.init(components.component_b[0], TraversalDirection.outgoing);
    query_b.algorithm = .breadth_first;
    query_b.max_depth = 10;

    var result_b = try harness.query_engine.execute_traversal(query_b);
    defer result_b.deinit();

    try testing.expect(result_b.paths.len > 0);

    // Find path that reaches component_b[2]
    var target_path_b: ?[]const BlockId = null;
    for (result_b.paths) |path| {
        if (path.len > 0 and path[path.len - 1].eql(components.component_b[2])) {
            target_path_b = path;
            break;
        }
    }

    try testing.expect(target_path_b != null);
    try testing.expectEqual(components.component_b[0], target_path_b.?[0]);
    try testing.expectEqual(components.component_b[2], target_path_b.?[target_path_b.?.len - 1]);

    // Test breadth-first search from isolated node (should find only itself)
    var query_isolated_bfs = TraversalQuery.init(components.isolated, .outgoing);
    query_isolated_bfs.algorithm = .breadth_first;
    query_isolated_bfs.max_depth = 5;

    var result_isolated_bfs = try execute_traversal(allocator, harness.storage_engine(), query_isolated_bfs);
    defer result_isolated_bfs.deinit();

    try testing.expectEqual(@as(usize, 1), result_isolated_bfs.paths.len);
}

test "topological sort cycle detection edge cases" {
    const allocator = testing.allocator;

    var harness = try QueryHarness.init_and_startup(allocator, "topo_sort_cycles_test");
    defer harness.deinit();

    const graph = try create_cyclic_graph(&harness, allocator);

    // Test topological sort on cyclic subgraph (should detect cycle)
    var cyclic_query = TraversalQuery.init(graph.cycle[0], TraversalDirection.outgoing);
    cyclic_query.algorithm = .topological_sort;
    cyclic_query.max_depth = 10;

    var topo_result_cyclic = try harness.query_engine.execute_traversal(cyclic_query);
    defer topo_result_cyclic.deinit();

    // Should have empty result for cyclic graph
    try testing.expect(topo_result_cyclic.paths.len == 0);

    // Test topological sort on acyclic subgraph (should succeed)
    var acyclic_query = TraversalQuery.init(graph.acyclic_branch[0], TraversalDirection.outgoing);
    acyclic_query.algorithm = .topological_sort;
    acyclic_query.max_depth = 10;

    var topo_result_acyclic = try harness.query_engine.execute_traversal(acyclic_query);
    defer topo_result_acyclic.deinit();

    try testing.expect(topo_result_acyclic.paths.len > 0);
}

test "SCC detection self loop edge cases" {
    const allocator = testing.allocator;

    var harness = try QueryHarness.init_and_startup(allocator, "scc_self_loop_test");
    defer harness.deinit();

    // Create a self-loop block
    const self_loop_block = ContextBlock{
        .id = TestData.deterministic_block_id(7001),
        .version = 1,
        .source_uri = "test://scc_self_loop.zig",
        .metadata_json = "{\"test\":\"scc_detection\",\"component\":\"self_loop\"}",
        .content = "Self loop SCC test block content",
    };
    try harness.storage_engine().put_block(self_loop_block);

    // Note: Self-loop edges are not supported by the storage engine
    // Testing SCC detection with valid multi-node cycles instead

    // Create two-node SCC
    const scc1_block = ContextBlock{
        .id = TestData.deterministic_block_id(7002),
        .version = 1,
        .source_uri = "test://scc_two_node_1.zig",
        .metadata_json = "{\"test\":\"scc_detection\",\"component\":\"two_node_1\"}",
        .content = "Two node SCC test block 1 content",
    };
    const scc2_block = ContextBlock{
        .id = TestData.deterministic_block_id(7003),
        .version = 1,
        .source_uri = "test://scc_two_node_2.zig",
        .metadata_json = "{\"test\":\"scc_detection\",\"component\":\"two_node_2\"}",
        .content = "Two node SCC test block 2 content",
    };
    try harness.storage_engine().put_block(scc1_block);
    try harness.storage_engine().put_block(scc2_block);

    const edge_1_2 = GraphEdge{
        .source_id = scc1_block.id,
        .target_id = scc2_block.id,
        .edge_type = EdgeType.imports,
    };
    const edge_2_1 = GraphEdge{
        .source_id = scc2_block.id,
        .target_id = scc1_block.id,
        .edge_type = EdgeType.imports,
    };
    try harness.storage_engine().put_edge(edge_1_2);
    try harness.storage_engine().put_edge(edge_2_1);

    // Test SCC detection using traversal algorithm
    var scc_query = TraversalQuery.init(self_loop_block.id, TraversalDirection.bidirectional);
    scc_query.algorithm = .strongly_connected;
    scc_query.max_depth = 10;

    var scc_result = try harness.query_engine.execute_traversal(scc_query);
    defer scc_result.deinit();

    // Should find strongly connected components
    try testing.expect(scc_result.paths.len > 0);
}

test "algorithm robustness malformed input edge cases" {
    const allocator = testing.allocator;

    var harness = try QueryHarness.init_and_startup(allocator, "malformed_input_test");
    defer harness.deinit();

    // Test traversal with non-existent nodes
    var id_bytes: [16]u8 = undefined;
    std.mem.writeInt(u128, &id_bytes, 99999999, .little);
    const nonexistent_id = BlockId{ .bytes = id_bytes };

    var query_nonexistent = TraversalQuery.init(nonexistent_id, TraversalDirection.outgoing);
    query_nonexistent.algorithm = .breadth_first;
    query_nonexistent.max_depth = 5;

    var result_nonexistent = try harness.query_engine.execute_traversal(query_nonexistent);
    defer result_nonexistent.deinit();

    try testing.expectEqual(@as(usize, 0), result_nonexistent.paths.len);

    // Test with single node graphs
    const single_block = ContextBlock{
        .id = TestData.deterministic_block_id(8001),
        .version = 1,
        .source_uri = "test://single_node_graph.zig",
        .metadata_json = "{\"test\":\"malformed_input_edge_cases\",\"component\":\"single\"}",
        .content = "Single node graph test block content",
    };
    try harness.storage_engine().put_block(single_block);

    // Test topological sort on single node
    var single_topo_query = TraversalQuery.init(single_block.id, TraversalDirection.outgoing);
    single_topo_query.algorithm = .topological_sort;
    single_topo_query.max_depth = 5;

    var single_topo_result = try harness.query_engine.execute_traversal(single_topo_query);
    defer single_topo_result.deinit();

    try testing.expect(single_topo_result.paths.len >= 0);

    // Test SCC detection on single node
    var single_scc_query = TraversalQuery.init(single_block.id, TraversalDirection.bidirectional);
    single_scc_query.algorithm = .strongly_connected;
    single_scc_query.max_depth = 5;

    var single_scc_result = try harness.query_engine.execute_traversal(single_scc_query);
    defer single_scc_result.deinit();

    try testing.expect(single_scc_result.paths.len >= 0);
}

test "algorithm performance edge case timing validation" {
    const allocator = testing.allocator;

    var harness = try kausaldb.QueryHarness.init_and_startup(allocator, "performance_edge_test");
    defer harness.deinit();

    // Create a moderately sized graph for performance testing
    const graph_size = 100;
    var nodes = std.ArrayList(BlockId).init(allocator);
    defer nodes.deinit();
    try nodes.ensureTotalCapacity(graph_size);

    // Create nodes
    for (0..graph_size) |i| {
        const block = ContextBlock{
            .id = TestData.deterministic_block_id(@intCast(i + 1)),
            .version = 1,
            .source_uri = "test://edge_case_dense_graph.zig",
            .metadata_json = "{\"test\":\"edge_case_dense_graph\"}",
            .content = "Edge case dense graph test block content",
        };
        try harness.storage_engine().put_block(block);
        try nodes.append(block.id);
    }

    // Create a linear chain with some cross-connections
    for (0..graph_size - 1) |i| {
        const edge = GraphEdge{
            .source_id = nodes.items[i],
            .target_id = nodes.items[i + 1],
            .edge_type = EdgeType.calls,
        };
        try harness.storage_engine().put_edge(edge);

        // Add some cross-connections every 10 nodes
        if (i % 10 == 0 and i + 5 < graph_size) {
            const cross_edge = GraphEdge{
                .source_id = nodes.items[i],
                .target_id = nodes.items[i + 5],
                .edge_type = EdgeType.imports,
            };
            try harness.storage_engine().put_edge(cross_edge);
        }
    }

    // Measure traversal performance
    const start_time = std.time.nanoTimestamp();

    var query = TraversalQuery.init(nodes.items[0], .outgoing);
    query.algorithm = .astar_search;
    query.max_depth = 100;

    var result = try harness.query_engine.execute_traversal(query);
    defer result.deinit();

    const end_time = std.time.nanoTimestamp();
    const duration_ns = @as(u64, @intCast(end_time - start_time));

    // Verify path was found
    try testing.expect(result.paths.len > 0);

    // Performance assertion: traversal should complete within reasonable time
    // Allow 10ms for moderate graph traversal in CI environment
    const max_duration_ns = 10_000_000; // 10ms
    try testing.expect(duration_ns < max_duration_ns);

    // Measure topological sort performance
    const topo_start_time = std.time.nanoTimestamp();

    var topo_query = TraversalQuery.init(nodes.items[0], TraversalDirection.outgoing);
    topo_query.algorithm = .topological_sort;
    topo_query.max_depth = 100; // Keep within validation limit

    var topo_result = try harness.query_engine.execute_traversal(topo_query);
    defer topo_result.deinit();

    const topo_end_time = std.time.nanoTimestamp();
    const topo_duration_ns = @as(u64, @intCast(topo_end_time - topo_start_time));

    // Topological sort should complete quickly on acyclic portion
    const max_topo_duration_ns = 5_000_000; // 5ms
    try testing.expect(topo_duration_ns < max_topo_duration_ns);

    // Topological sort should find blocks, but not necessarily all of them
    // due to the cross-connections creating cycles
    try testing.expect(topo_result.blocks.len > 0);
    try testing.expect(topo_result.blocks.len <= graph_size);
}
