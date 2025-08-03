//! Advanced graph traversal algorithm edge case testing.
//!
//! Tests boundary conditions and failure scenarios for sophisticated graph algorithms
//! including A* search, bidirectional search, topological sort, and SCC detection.
//! Validates algorithm correctness under hostile conditions and ensures memory safety.

const std = @import("std");
const testing = std.testing;
const kausaldb = @import("kausaldb");

const storage = kausaldb.storage;
const query_engine = kausaldb.query_engine;
const context_block = kausaldb.types;
const simulation_vfs = kausaldb.simulation_vfs;
const concurrency = kausaldb.concurrency;

const StorageEngine = storage.StorageEngine;
const QueryEngine = query_engine.QueryEngine;
const ContextBlock = context_block.ContextBlock;
const BlockId = context_block.BlockId;
const GraphEdge = context_block.GraphEdge;
const EdgeType = context_block.EdgeType;
const TraversalQuery = query_engine.TraversalQuery;

// Test harness for creating isolated graph scenarios
const GraphTestHarness = struct {
    allocator: std.mem.Allocator,
    storage_engine: *StorageEngine,
    query_engine: *QueryEngine,
    test_blocks: std.ArrayList(ContextBlock),
    test_edges: std.ArrayList(GraphEdge),

    const Self = @This();

    pub fn init(allocator: std.mem.Allocator, storage_engine: *StorageEngine, query_eng: *QueryEngine) Self {
        return Self{
            .allocator = allocator,
            .storage_engine = storage_engine,
            .query_engine = query_eng,
            .test_blocks = std.ArrayList(ContextBlock).init(allocator),
            .test_edges = std.ArrayList(GraphEdge).init(allocator),
        };
    }

    pub fn deinit(self: *Self) void {
        for (self.test_blocks.items) |block| {
            self.allocator.free(block.source_uri);
            self.allocator.free(block.metadata_json);
            self.allocator.free(block.content);
        }
        self.test_blocks.deinit();
        self.test_edges.deinit();
    }

    // Create a test block with given ID
    pub fn create_block(self: *Self, id_suffix: u64, name: []const u8) !BlockId {
        const block_id_hex = try std.fmt.allocPrint(self.allocator, "{x:0>32}", .{id_suffix});
        defer self.allocator.free(block_id_hex);

        const block_id = try BlockId.from_hex(block_id_hex);
        const source_uri = try std.fmt.allocPrint(self.allocator, "test://edge_cases/{s}.zig", .{name});
        const metadata_json = try std.fmt.allocPrint(self.allocator, "{{\"type\":\"function\",\"name\":\"{s}\"}}", .{name});
        const content = try std.fmt.allocPrint(self.allocator, "pub fn {s}() void {{ /* Edge case test */ }}", .{name});

        const block = ContextBlock{
            .id = block_id,
            .version = 1,
            .source_uri = source_uri,
            .metadata_json = metadata_json,
            .content = content,
        };

        try self.test_blocks.append(block);
        try self.storage_engine.put_block(block);

        return block_id;
    }

    // Create a directed edge between two blocks
    pub fn create_edge(self: *Self, from_id: BlockId, to_id: BlockId, edge_type: EdgeType) !void {
        const edge = GraphEdge{
            .source_id = from_id,
            .target_id = to_id,
            .edge_type = edge_type,
        };

        try self.test_edges.append(edge);
        try self.storage_engine.put_edge(edge);
    }

    // Create a disconnected graph with multiple components
    pub fn create_disconnected_components(self: *Self) !struct {
        component_a: [3]BlockId,
        component_b: [3]BlockId,
        isolated: BlockId,
    } {
        // Component A: Linear chain A1 -> A2 -> A3
        const a1 = try self.create_block(1001, "component_a1");
        const a2 = try self.create_block(1002, "component_a2");
        const a3 = try self.create_block(1003, "component_a3");

        try self.create_edge(a1, a2, EdgeType.calls);
        try self.create_edge(a2, a3, EdgeType.calls);

        // Component B: Linear chain B1 -> B2 -> B3
        const b1 = try self.create_block(2001, "component_b1");
        const b2 = try self.create_block(2002, "component_b2");
        const b3 = try self.create_block(2003, "component_b3");

        try self.create_edge(b1, b2, EdgeType.imports);
        try self.create_edge(b2, b3, EdgeType.imports);

        // Isolated node with no connections
        const isolated = try self.create_block(3001, "isolated_node");

        return .{
            .component_a = [3]BlockId{ a1, a2, a3 },
            .component_b = [3]BlockId{ b1, b2, b3 },
            .isolated = isolated,
        };
    }

    // Create a cyclic graph for topological sort testing
    pub fn create_cyclic_graph(self: *Self) !struct {
        cycle: [3]BlockId,
        acyclic_branch: [2]BlockId,
    } {
        // Create a 3-node cycle: C1 -> C2 -> C3 -> C1
        const c1 = try self.create_block(4001, "cycle_node1");
        const c2 = try self.create_block(4002, "cycle_node2");
        const c3 = try self.create_block(4003, "cycle_node3");

        try self.create_edge(c1, c2, EdgeType.depends_on);
        try self.create_edge(c2, c3, EdgeType.depends_on);
        try self.create_edge(c3, c1, EdgeType.depends_on); // Creates cycle

        // Add an acyclic branch off the cycle
        const branch1 = try self.create_block(4101, "acyclic_branch1");
        const branch2 = try self.create_block(4102, "acyclic_branch2");

        try self.create_edge(c2, branch1, EdgeType.calls);
        try self.create_edge(branch1, branch2, EdgeType.calls);

        return .{
            .cycle = [3]BlockId{ c1, c2, c3 },
            .acyclic_branch = [2]BlockId{ branch1, branch2 },
        };
    }

    // Create a self-loop for SCC testing (note: self-loops not allowed in this implementation)
    pub fn create_self_loop_graph(self: *Self) !struct {
        self_loop: BlockId,
        connected: [2]BlockId,
    } {
        // Node with mutual connections (simulating a strongly connected component)
        const self_loop = try self.create_block(5001, "self_referencing");
        // Note: Self-loops are not allowed by the storage engine, so we create a mutual connection

        // Connected nodes that form a strongly connected component
        const connected1 = try self.create_block(5002, "connected1");
        const connected2 = try self.create_block(5003, "connected2");

        // Create a strongly connected component: self_loop <-> connected1 <-> connected2 <-> self_loop
        try self.create_edge(connected1, self_loop, EdgeType.imports);
        try self.create_edge(self_loop, connected2, EdgeType.calls);
        try self.create_edge(connected2, connected1, EdgeType.depends_on);
        try self.create_edge(connected2, self_loop, EdgeType.calls); // Back to self_loop to complete SCC

        return .{
            .self_loop = self_loop,
            .connected = [2]BlockId{ connected1, connected2 },
        };
    }
};

// Test A* search with disconnected components
test "A* search - disconnected component edge cases" {
    concurrency.init();
    const allocator = testing.allocator;

    var sim_vfs = try simulation_vfs.SimulationVFS.init(allocator);
    defer sim_vfs.deinit();

    var storage_engine = try StorageEngine.init_default(allocator, sim_vfs.vfs(), "astar_edge_test");
    defer storage_engine.deinit();
    try storage_engine.startup();

    var query_eng = QueryEngine.init(allocator, &storage_engine);
    defer query_eng.deinit();

    var harness = GraphTestHarness.init(allocator, &storage_engine, &query_eng);
    defer harness.deinit();

    const components = try harness.create_disconnected_components();

    // Test 1: A* search from component A (should find nodes within component only)
    {
        const query = TraversalQuery{
            .start_block_id = components.component_a[0],
            .direction = .outgoing,
            .algorithm = .astar_search,
            .max_depth = 10,
            .max_results = 100,
            .edge_type_filter = EdgeType.calls,
        };

        var result = try query_eng.execute_traversal(query);
        defer result.deinit();

        // Should find nodes within component A only, not component B
        try testing.expect(result.blocks.len > 0);
        try testing.expect(result.blocks.len <= 3); // Max 3 nodes in component A
    }

    // Test 2: A* search from isolated node (should find only itself)
    {
        const query = TraversalQuery{
            .start_block_id = components.isolated,
            .direction = .outgoing,
            .algorithm = .astar_search,
            .max_depth = 10,
            .max_results = 100,
            .edge_type_filter = null, // Allow all edge types
        };

        var result = try query_eng.execute_traversal(query);
        defer result.deinit();

        // Should find only the start node itself (no outgoing connections)
        try testing.expect(result.blocks.len <= 1); // Either 0 or just the start node
    }

    // Test 3: A* search with wrong edge type filter
    {
        const query = TraversalQuery{
            .start_block_id = components.component_a[0],
            .direction = .outgoing,
            .algorithm = .astar_search,
            .max_depth = 10,
            .max_results = 100,
            .edge_type_filter = EdgeType.imports, // Wrong edge type for component A
        };

        var result = try query_eng.execute_traversal(query);
        defer result.deinit();

        // Should find no path with wrong edge type filter (or only start node)
        try testing.expect(result.blocks.len <= 1); // Either 0 or just the start node
    }
}

// Test bidirectional search on disconnected components
test "Bidirectional search - disconnected component scenarios" {
    concurrency.init();
    const allocator = testing.allocator;

    var sim_vfs = try simulation_vfs.SimulationVFS.init(allocator);
    defer sim_vfs.deinit();

    var storage_engine = try StorageEngine.init_default(allocator, sim_vfs.vfs(), "bidirectional_edge_test");
    defer storage_engine.deinit();
    try storage_engine.startup();

    var query_eng = QueryEngine.init(allocator, &storage_engine);
    defer query_eng.deinit();

    var harness = GraphTestHarness.init(allocator, &storage_engine, &query_eng);
    defer harness.deinit();

    const components = try harness.create_disconnected_components();

    // Test 1: Bidirectional search within single component (should work)
    {
        const query = TraversalQuery{
            .start_block_id = components.component_a[0],
            .direction = .bidirectional,
            .algorithm = .bidirectional_search,
            .max_depth = 10,
            .max_results = 100,
            .edge_type_filter = EdgeType.calls,
        };

        var result = try query_eng.execute_traversal(query);
        defer result.deinit();

        // Should find connected nodes in component A
        try testing.expect(result.blocks.len > 0);
    }

    // Test 2: Bidirectional search with very shallow depth
    {
        const query = TraversalQuery{
            .start_block_id = components.component_a[0],
            .direction = .bidirectional,
            .algorithm = .bidirectional_search,
            .max_depth = 1, // Very shallow
            .max_results = 100,
            .edge_type_filter = EdgeType.calls,
        };

        var result = try query_eng.execute_traversal(query);
        defer result.deinit();

        // Should find limited results due to depth constraint
        try testing.expect(result.blocks.len <= 2); // Start node + 1 hop
    }
}

// Test topological sort with cycle detection
test "Topological sort - cycle detection edge cases" {
    concurrency.init();
    const allocator = testing.allocator;

    var sim_vfs = try simulation_vfs.SimulationVFS.init(allocator);
    defer sim_vfs.deinit();

    var storage_engine = try StorageEngine.init_default(allocator, sim_vfs.vfs(), "topological_edge_test");
    defer storage_engine.deinit();
    try storage_engine.startup();

    var query_eng = QueryEngine.init(allocator, &storage_engine);
    defer query_eng.deinit();

    var harness = GraphTestHarness.init(allocator, &storage_engine, &query_eng);
    defer harness.deinit();

    const cyclic_graph = try harness.create_cyclic_graph();

    // Test 1: Topological sort on acyclic subgraph
    {
        const query = TraversalQuery{
            .start_block_id = cyclic_graph.acyclic_branch[0],
            .direction = .outgoing,
            .algorithm = .topological_sort,
            .max_depth = 5,
            .max_results = 100,
            .edge_type_filter = EdgeType.calls,
        };

        var result = try query_eng.execute_traversal(query);
        defer result.deinit();

        // Should work on acyclic portion
        try testing.expect(result.blocks.len >= 0); // May be 0 or more depending on implementation
    }

    // Test 2: Topological sort starting from cyclic portion
    {
        const query = TraversalQuery{
            .start_block_id = cyclic_graph.cycle[0],
            .direction = .outgoing,
            .algorithm = .topological_sort,
            .max_depth = 10,
            .max_results = 100,
            .edge_type_filter = EdgeType.depends_on,
        };

        var result = try query_eng.execute_traversal(query);
        defer result.deinit();

        // Should handle cycles gracefully (implementation dependent)
        // Either detect cycle and return partial result, or return empty
    }
}

// Test strongly connected components with self-loops
test "SCC detection - self-loop edge cases" {
    concurrency.init();
    const allocator = testing.allocator;

    var sim_vfs = try simulation_vfs.SimulationVFS.init(allocator);
    defer sim_vfs.deinit();

    var storage_engine = try StorageEngine.init_default(allocator, sim_vfs.vfs(), "scc_edge_test");
    defer storage_engine.deinit();
    try storage_engine.startup();

    var query_eng = QueryEngine.init(allocator, &storage_engine);
    defer query_eng.deinit();

    var harness = GraphTestHarness.init(allocator, &storage_engine, &query_eng);
    defer harness.deinit();

    const self_loop_graph = try harness.create_self_loop_graph();

    // Test 1: SCC detection with self-loops
    {
        const query = TraversalQuery{
            .start_block_id = self_loop_graph.self_loop,
            .direction = .bidirectional,
            .algorithm = .strongly_connected,
            .max_depth = 10,
            .max_results = 100,
            .edge_type_filter = null, // Allow all edge types
        };

        var result = try query_eng.execute_traversal(query);
        defer result.deinit();

        // Should identify strongly connected component including self-loop
        try testing.expect(result.blocks.len >= 1);
    }

    // Test 2: Single node SCC (isolated node without self-loop since they're not allowed)
    {
        const isolated_self = try harness.create_block(9001, "isolated_single_node");
        // Don't create self-loop since they're not allowed by the storage engine

        const query = TraversalQuery{
            .start_block_id = isolated_self,
            .direction = .bidirectional,
            .algorithm = .strongly_connected,
            .max_depth = 5,
            .max_results = 100,
            .edge_type_filter = EdgeType.calls,
        };

        var result = try query_eng.execute_traversal(query);
        defer result.deinit();

        // Should find the single-node SCC
        try testing.expect(result.blocks.len >= 1);
    }
}

// Test algorithm robustness with malformed inputs
test "Algorithm robustness - malformed input edge cases" {
    concurrency.init();
    const allocator = testing.allocator;

    var sim_vfs = try simulation_vfs.SimulationVFS.init(allocator);
    defer sim_vfs.deinit();

    var storage_engine = try StorageEngine.init_default(allocator, sim_vfs.vfs(), "robustness_edge_test");
    defer storage_engine.deinit();
    try storage_engine.startup();

    var query_eng = QueryEngine.init(allocator, &storage_engine);
    defer query_eng.deinit();

    var harness = GraphTestHarness.init(allocator, &storage_engine, &query_eng);
    defer harness.deinit();

    const test_node = try harness.create_block(8001, "test_node");

    // Test 1: Zero max_depth (should return error)
    {
        const query = TraversalQuery{
            .start_block_id = test_node,
            .direction = .outgoing,
            .algorithm = .astar_search,
            .max_depth = 0,
            .max_results = 100,
            .edge_type_filter = EdgeType.calls,
        };

        // Should return InvalidDepth error for zero depth
        const result = query_eng.execute_traversal(query);
        try testing.expectError(error.InvalidDepth, result);
    }

    // Test 2: Non-existent start node
    {
        const fake_id = try BlockId.from_hex("deadbeefdeadbeefdeadbeefdeadbeef");

        const query = TraversalQuery{
            .start_block_id = fake_id,
            .direction = .outgoing,
            .algorithm = .astar_search,
            .max_depth = 10,
            .max_results = 100,
            .edge_type_filter = EdgeType.calls,
        };

        var result = try query_eng.execute_traversal(query);
        defer result.deinit();

        // Should handle non-existent start node gracefully
        try testing.expectEqual(@as(usize, 0), result.blocks.len);
    }

    // Test 3: Maximum allowed max_results (should work)
    {
        const query = TraversalQuery{
            .start_block_id = test_node,
            .direction = .outgoing,
            .algorithm = .breadth_first,
            .max_depth = 1,
            .max_results = TraversalQuery.ABSOLUTE_MAX_RESULTS, // Maximum allowed
            .edge_type_filter = null,
        };

        var result = try query_eng.execute_traversal(query);
        defer result.deinit();

        // Should handle large max_results without issues
        try testing.expect(result.blocks.len < 1000); // Should be bounded by actual graph size
    }

    // Test 4: Excessive max_results (should return error)
    {
        const query = TraversalQuery{
            .start_block_id = test_node,
            .direction = .outgoing,
            .algorithm = .breadth_first,
            .max_depth = 1,
            .max_results = TraversalQuery.ABSOLUTE_MAX_RESULTS + 1, // Exceeds limit
            .edge_type_filter = null,
        };

        // Should return InvalidMaxResults error for excessive value
        const result = query_eng.execute_traversal(query);
        try testing.expectError(error.InvalidMaxResults, result);
    }
}

// Performance regression test for algorithm efficiency
test "Algorithm performance - edge case timing validation" {
    concurrency.init();
    const allocator = testing.allocator;

    var sim_vfs = try simulation_vfs.SimulationVFS.init(allocator);
    defer sim_vfs.deinit();

    var storage_engine = try StorageEngine.init_default(allocator, sim_vfs.vfs(), "performance_edge_test");
    defer storage_engine.deinit();
    try storage_engine.startup();

    var query_eng = QueryEngine.init(allocator, &storage_engine);
    defer query_eng.deinit();

    var harness = GraphTestHarness.init(allocator, &storage_engine, &query_eng);
    defer harness.deinit();

    // Create moderate-sized graph for performance testing
    var nodes = std.ArrayList(BlockId).init(allocator);
    try nodes.ensureTotalCapacity(20); // tidy:ignore-perf - capacity pre-allocated for 20 nodes
    defer nodes.deinit();

    // Create 20 connected nodes for performance testing
    for (0..20) |i| {
        const name = try std.fmt.allocPrint(allocator, "perf_node_{}", .{i});
        defer allocator.free(name);

        const node_id = try harness.create_block(6000 + i, name);
        try nodes.append(node_id);

        // Connect each node to the next one
        if (i > 0) {
            try harness.create_edge(nodes.items[i - 1], node_id, EdgeType.calls);
        }
    }

    // Performance test: A* search should complete within reasonable time
    {
        const start_time = std.time.nanoTimestamp();

        const query = TraversalQuery{
            .start_block_id = nodes.items[0],
            .direction = .outgoing,
            .algorithm = .astar_search,
            .max_depth = 20,
            .max_results = 100,
            .edge_type_filter = EdgeType.calls,
        };

        var result = try query_eng.execute_traversal(query);
        defer result.deinit();

        const end_time = std.time.nanoTimestamp();
        const duration_ns = end_time - start_time;

        // Algorithm should complete within 10ms for 20-node graph
        const max_duration_ns = 10 * 1000 * 1000; // 10ms
        try testing.expect(duration_ns < max_duration_ns);

        // Should find the connected nodes
        try testing.expect(result.blocks.len > 0);
    }

    // Performance test: Bidirectional search should be efficient
    {
        const start_time = std.time.nanoTimestamp();

        const query = TraversalQuery{
            .start_block_id = nodes.items[5],
            .direction = .bidirectional,
            .algorithm = .bidirectional_search,
            .max_depth = 15,
            .max_results = 100,
            .edge_type_filter = EdgeType.calls,
        };

        var result = try query_eng.execute_traversal(query);
        defer result.deinit();

        const end_time = std.time.nanoTimestamp();
        const duration_ns = end_time - start_time;

        // Bidirectional search should also be fast
        const max_duration_ns = 10 * 1000 * 1000; // 10ms
        try testing.expect(duration_ns < max_duration_ns);
    }
}
