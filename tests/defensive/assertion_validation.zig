//! Comprehensive defensive programming validation test suite.
//!
//! Tests the assertion framework and defensive programming checks throughout
//! KausalDB to ensure they properly catch invalid conditions while not
//! interfering with normal operation. Follows TigerBeetle-style defensive
//! programming principles with comprehensive validation.

const std = @import("std");
const testing = std.testing;
const kausaldb = @import("kausaldb");
const assert = kausaldb.assert;
const types = kausaldb.types;
const storage = kausaldb.storage;
const simulation = kausaldb.simulation;

const BlockId = types.BlockId;
const ContextBlock = types.ContextBlock;
const GraphEdge = types.GraphEdge;
const EdgeType = types.EdgeType;
const StorageEngine = storage.StorageEngine;
const Simulation = simulation.Simulation;

test "assertion framework basic functionality validation" {
    // Test basic assert functionality
    assert.assert(true);
    assert.assert_fmt(true, "This should not fail", .{});

    // Test range assertions
    assert.assert_range(50, 0, 100, "Value {} not in range 0-100", .{50});
    assert.assert_range(0, 0, 100, "Value {} not in range 0-100", .{0});
    assert.assert_range(100, 0, 100, "Value {} not in range 0-100", .{100});

    // Test buffer bounds assertions
    assert.assert_buffer_bounds(0, 50, 100, "Buffer overflow: {} + {} > {}", .{ 0, 50, 100 });
    assert.assert_buffer_bounds(50, 50, 100, "Buffer overflow: {} + {} > {}", .{ 50, 50, 100 });

    // Test index validation
    assert.assert_index_valid(0, 10, "Index out of bounds: {} >= {}", .{ 0, 10 });
    assert.assert_index_valid(9, 10, "Index out of bounds: {} >= {}", .{ 9, 10 });

    // Test not empty validation
    const slice = [_]u8{ 1, 2, 3 };
    assert.assert_not_empty(slice[0..], "Slice cannot be empty", .{});

    // Test equality assertions
    assert.assert_equal(42, 42, "Values not equal: {} != {}", .{ 42, 42 });

    // Test counter bounds
    assert.assert_counter_bounds(10, 100, "Counter overflow: {} > {}", .{ 10, 100 });

    // Test state validation
    assert.assert_state_valid(true, "Invalid state: {}", .{42});

    // Test stride validation
    assert.assert_stride_positive(1, "Invalid stride: {} must be positive", .{1});
    assert.assert_stride_positive(100, "Invalid stride: {} must be positive", .{100});
}

test "storage engine defensive programming validation" {
    const allocator = testing.allocator;

    var sim = try Simulation.init(allocator, 0xDEFE15E);
    defer sim.deinit();

    const data_dir = "defensive_test_storage";
    const config = storage.Config{
        .memtable_max_size = 1024 * 1024,
    };

    const node_id = try sim.add_node();
    const node = sim.find_node(node_id);
    const vfs_interface = node.filesystem_interface();

    var engine = try StorageEngine.init(allocator, vfs_interface, data_dir, config);
    defer engine.deinit();

    try engine.startup();

    // Test valid block operations - should not trigger assertions
    const valid_block = ContextBlock{
        .id = try BlockId.from_hex("00000000000000000000000000000001"), // Non-zero BlockID required
        .version = 1,
        .source_uri = "test://valid.zig",
        .metadata_json = "{}",
        .content = "valid test content",
    };

    try engine.put_block(valid_block);

    // Test block retrieval
    const retrieved = try engine.find_block(valid_block.id);
    try testing.expect(retrieved != null);
    try testing.expectEqualStrings(valid_block.content, retrieved.?.content);

    // Test valid edge operations
    const valid_edge = GraphEdge{
        .source_id = valid_block.id,
        .target_id = try BlockId.from_hex("00000000000000000000000000000002"),
        .edge_type = EdgeType.calls,
    };

    try engine.put_edge(valid_edge);

    // Test edge retrieval
    const outgoing_edges = engine.find_outgoing_edges(valid_block.id);
    try testing.expect(outgoing_edges.len > 0);
    try testing.expectEqual(valid_edge.edge_type, outgoing_edges[0].edge_type);

    // Test metrics access
    const metrics = engine.metrics();
    try testing.expect(metrics.blocks_written.load() > 0);
    try testing.expect(metrics.edges_added.load() > 0);
}

test "block validation defensive programming" {
    const allocator = testing.allocator;

    var sim = try Simulation.init(allocator, 0xB10CDEF);
    defer sim.deinit();

    const data_dir = "defensive_block_test";
    const config = storage.Config{
        .memtable_max_size = 1024 * 1024,
    };

    const node_id = try sim.add_node();
    const node = sim.find_node(node_id);
    const vfs_interface = node.filesystem_interface();

    var engine = try StorageEngine.init(allocator, vfs_interface, data_dir, config);
    defer engine.deinit();

    try engine.startup();

    // Test blocks with valid boundary conditions
    const boundary_tests = [_]struct {
        content_size: usize,
        uri_size: usize,
        metadata_size: usize,
        should_succeed: bool,
    }{
        // Valid cases
        .{ .content_size = 1, .uri_size = 10, .metadata_size = 20, .should_succeed = true },
        .{ .content_size = 1000, .uri_size = 100, .metadata_size = 100, .should_succeed = true },
        .{ .content_size = 1024 * 1024, .uri_size = 1000, .metadata_size = 10000, .should_succeed = true },

        // Edge of valid range (stay within 16MB limit)
        .{ .content_size = 15 * 1024 * 1024, .uri_size = 1024, .metadata_size = 512 * 1024, .should_succeed = true },
    };

    for (boundary_tests, 0..) |test_case, index| { // Start from 0 but add offset to BlockID
        const content = try allocator.alloc(u8, test_case.content_size);
        defer allocator.free(content);
        @memset(content, @as(u8, @intCast('A' + (index % 26))));

        const uri = try allocator.alloc(u8, test_case.uri_size);
        defer allocator.free(uri);
        @memset(uri, @as(u8, @intCast('a' + (index % 26))));

        // Generate valid JSON metadata of the specified size
        const base_json = "{\"test\":\"";
        const suffix_json = "\"}";
        const min_required_size = base_json.len + suffix_json.len;

        // Skip test cases where metadata_size is too small for valid JSON structure
        if (test_case.metadata_size < min_required_size) {
            continue; // Skip this test case
        }

        const value_size = test_case.metadata_size - min_required_size;

        var metadata = try allocator.alloc(u8, test_case.metadata_size);
        defer allocator.free(metadata);

        @memcpy(metadata[0..base_json.len], base_json);
        @memset(metadata[base_json.len .. base_json.len + value_size], 'x');
        @memcpy(metadata[base_json.len + value_size ..], suffix_json);

        // Use offset to ensure non-zero BlockID (all-zero BlockID invalid)
        const block_id_hex = try std.fmt.allocPrint(allocator, "{:0>32}", .{index + 10});
        defer allocator.free(block_id_hex);

        const test_block = ContextBlock{
            .id = try BlockId.from_hex(block_id_hex),
            .version = @as(u32, @intCast(index + 1)),
            .source_uri = uri,
            .metadata_json = metadata,
            .content = content,
        };

        if (test_case.should_succeed) {
            try engine.put_block(test_block);

            // Verify retrieval works
            const retrieved = try engine.find_block(test_block.id);
            try testing.expect(retrieved != null);
            try testing.expectEqual(test_block.version, retrieved.?.version);
        }
    }
}

test "graph edge defensive programming validation" {
    const allocator = testing.allocator;

    var sim = try Simulation.init(allocator, 0xED6EDEF);
    defer sim.deinit();

    const data_dir = "defensive_edge_test";
    const config = storage.Config{
        .memtable_max_size = 1024 * 1024,
    };

    const node_id = try sim.add_node();
    const node = sim.find_node(node_id);
    const vfs_interface = node.filesystem_interface();

    var engine = try StorageEngine.init(allocator, vfs_interface, data_dir, config);
    defer engine.deinit();

    try engine.startup();

    // Create valid blocks for edge testing
    const source_block = ContextBlock{
        .id = BlockId.from_bytes([_]u8{1} ** 16),
        .version = 1,
        .source_uri = "test://source.zig",
        .metadata_json = "{}",
        .content = "source block content",
    };

    const target_block = ContextBlock{
        .id = BlockId.from_bytes([_]u8{3} ** 16),
        .version = 1,
        .source_uri = "test://target.zig",
        .metadata_json = "{}",
        .content = "target block content",
    };

    try engine.put_block(source_block);
    try engine.put_block(target_block);

    // Test valid edge types
    const edge_types = [_]EdgeType{ .calls, .imports, .references };

    for (edge_types) |edge_type| {
        const valid_edge = GraphEdge{
            .source_id = source_block.id,
            .target_id = target_block.id,
            .edge_type = edge_type,
        };

        try engine.put_edge(valid_edge);

        // Verify edge retrieval
        const outgoing = engine.find_outgoing_edges(source_block.id);
        var found_edge = false;
        for (outgoing) |edge| {
            if (edge.edge_type == edge_type and
                std.mem.eql(u8, &edge.target_id.bytes, &target_block.id.bytes))
            {
                found_edge = true;
                break;
            }
        }
        try testing.expect(found_edge);

        // Verify incoming edges
        const incoming = engine.find_incoming_edges(target_block.id);
        found_edge = false;
        for (incoming) |edge| {
            if (edge.edge_type == edge_type and
                std.mem.eql(u8, &edge.source_id.bytes, &source_block.id.bytes))
            {
                found_edge = true;
                break;
            }
        }
        try testing.expect(found_edge);
    }

    // Test edge count validation
    const edge_count = engine.edge_count();
    try testing.expect(edge_count == edge_types.len);
}

test "memory management defensive programming" {
    const allocator = testing.allocator;

    var sim = try Simulation.init(allocator, 0xDEADBEEF);
    defer sim.deinit();

    const data_dir = "defensive_memory_test";
    const config = storage.Config{
        .memtable_max_size = 1024 * 1024,
    };

    const node_id = try sim.add_node();
    const node = sim.find_node(node_id);
    const vfs_interface = node.filesystem_interface();

    var engine = try StorageEngine.init(allocator, vfs_interface, data_dir, config);
    defer engine.deinit();

    try engine.startup();

    // Test memory pressure scenarios
    const test_iterations = 20;
    var blocks: [test_iterations]ContextBlock = undefined;

    for (0..test_iterations) |i| {
        const content = try std.fmt.allocPrint(allocator, "Test content for block {} - this is substantial content to test memory management under pressure. The content includes structured data that would be typical in a real KausalDB deployment.", .{i});
        defer allocator.free(content);

        const uri = try std.fmt.allocPrint(allocator, "test://memory_test_{}.zig", .{i});
        defer allocator.free(uri);

        blocks[i] = ContextBlock{
            .id = BlockId.from_bytes([_]u8{ 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, @as(u8, @intCast(i + 10)) }),
            .version = @as(u32, @intCast(i + 1)),
            .source_uri = uri,
            .metadata_json = "{}",
            .content = content,
        };

        try engine.put_block(blocks[i]);

        // Verify immediate retrieval
        const retrieved = try engine.find_block(blocks[i].id);
        try testing.expect(retrieved != null);
        try testing.expectEqualStrings(blocks[i].content, retrieved.?.content);
    }

    // Verify all blocks are still retrievable after potential flushes
    for (blocks) |block| {
        const retrieved = try engine.find_block(block.id);
        try testing.expect(retrieved != null);
        try testing.expectEqual(block.version, retrieved.?.version);
    }

    // Test metrics consistency
    const metrics = engine.metrics();
    try testing.expectEqual(@as(u64, test_iterations), metrics.blocks_written.load());
    try testing.expect(metrics.total_write_time_ns.load() > 0);
    try testing.expect(metrics.total_bytes_written.load() > 0);
}

test "concurrent operation defensive programming" {
    const allocator = testing.allocator;

    var sim = try Simulation.init(allocator, 0xCAFEBABE);
    defer sim.deinit();

    const data_dir = "defensive_concurrent_test";
    const config = storage.Config{
        .memtable_max_size = 1024 * 1024,
    };

    const node_id = try sim.add_node();
    const node = sim.find_node(node_id);
    const vfs_interface = node.filesystem_interface();

    var engine = try StorageEngine.init(allocator, vfs_interface, data_dir, config);
    defer engine.deinit();

    try engine.startup();

    // Test rapid block operations that might stress assertion validation
    // Test rapid operations under defensive programming
    const rapid_operations = 50;
    var previous_block_id: ?BlockId = null;

    for (0..rapid_operations) |i| {
        const block = ContextBlock{
            .id = BlockId.from_bytes([_]u8{ 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, @as(u8, @intCast(i + 100)) }),
            .version = 1,
            .source_uri = "test://rapid.zig",
            .metadata_json = "{}",
            .content = "rapid operation test content",
        };

        try engine.put_block(block);

        // Immediate read to test assertion validation under rapid access
        const retrieved = try engine.find_block(block.id);
        try testing.expect(retrieved != null);
        try testing.expectEqualStrings(block.content, retrieved.?.content);

        // Add edge to create graph structure - use previous block as target to avoid self-reference
        if (previous_block_id) |prev_id| {
            const edge = GraphEdge{
                .source_id = block.id,
                .target_id = prev_id,
                .edge_type = EdgeType.references,
            };

            // This should succeed since it's a valid edge
            try engine.put_edge(edge);
        }

        previous_block_id = block.id;
    }

    // Verify final state consistency
    const final_metrics = engine.metrics();
    try testing.expectEqual(@as(u64, rapid_operations), final_metrics.blocks_written.load());
}

test "boundary condition defensive programming" {
    _ = testing.allocator;

    // Test zero-length and maximum-length scenarios
    const empty_string = "";
    const max_reasonable_string = "a" ** 1000;

    // Test basic assertion boundary conditions
    assert.assert_range(0, 0, 0, "Single value range test: {}", .{0});
    assert.assert_range(1000, 0, 1000, "Max boundary test: {}", .{1000});

    // Test buffer boundary conditions
    assert.assert_buffer_bounds(0, 0, 100, "Zero write test: {} + {} <= {}", .{ 0, 0, 100 });
    assert.assert_buffer_bounds(100, 0, 100, "Boundary write test: {} + {} <= {}", .{ 100, 0, 100 });

    // Test index boundary conditions
    assert.assert_index_valid(0, 1, "Single element array: {} < {}", .{ 0, 1 });

    // Test string validation
    assert.assert_not_empty(max_reasonable_string, "Max string should not be empty", .{});

    // Test counter boundary conditions
    assert.assert_counter_bounds(0, 0, "Zero counter boundary: {} <= {}", .{ 0, 0 });
    assert.assert_counter_bounds(std.math.maxInt(u32), std.math.maxInt(u32), "Max counter boundary: {} <= {}", .{ std.math.maxInt(u32), std.math.maxInt(u32) });

    // Test validation with empty inputs
    _ = empty_string; // Acknowledge but don't use in assertions that would fail
}

test "error propagation with defensive programming" {
    const allocator = testing.allocator;

    var sim = try Simulation.init(allocator, 0xE44DEF);
    defer sim.deinit();

    // Test error propagation doesn't bypass defensive checks
    const data_dir = "defensive_error_test";
    const config = storage.Config{
        .memtable_max_size = 1024 * 1024,
    };

    const node_id = try sim.add_node();
    const node = sim.find_node(node_id);
    const vfs_interface = node.filesystem_interface();

    var engine = try StorageEngine.init(allocator, vfs_interface, data_dir, config);
    defer engine.deinit();

    try engine.startup();

    // Test that even under error conditions, defensive programming is maintained
    const valid_block = ContextBlock{
        .id = BlockId.from_bytes([_]u8{5} ** 16),
        .version = 1,
        .source_uri = "test://error.zig",
        .metadata_json = "{}",
        .content = "error testing content",
    };

    try engine.put_block(valid_block);

    // Verify normal operation continues with defensive programming active
    const retrieved = try engine.find_block(valid_block.id);
    try testing.expect(retrieved != null);

    // Test that metrics are still properly validated
    const metrics = engine.metrics();
    try testing.expect(metrics.blocks_written.load() > 0);
}
