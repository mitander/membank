//! Graph traversal operations for Membank knowledge graph.
//!
//! Provides breadth-first and depth-first search algorithms for exploring
//! relationships between context blocks. Handles cycle detection, path tracking,
//! and result formatting with comprehensive traversal statistics.

const std = @import("std");
const assert = @import("../core/assert.zig").assert;
const storage = @import("../storage/engine.zig");
const context_block = @import("../core/types.zig");
const simulation_vfs = @import("../sim/simulation_vfs.zig");
const testing = std.testing;

const StorageEngine = storage.StorageEngine;
const ContextBlock = context_block.ContextBlock;
const GraphEdge = context_block.GraphEdge;
const BlockId = context_block.BlockId;
const EdgeType = context_block.EdgeType;
const SimulationVFS = simulation_vfs.SimulationVFS;

/// Hash context for BlockId HashMap operations
const BlockIdHashContext = struct {
    pub fn hash(ctx: @This(), key: BlockId) u64 {
        _ = ctx;
        return std.hash_map.hashString(&key.bytes);
    }
    pub fn eql(ctx: @This(), a: BlockId, b: BlockId) bool {
        _ = ctx;
        return a.eql(b);
    }
};

/// HashMap type for BlockId visited tracking during traversal
const BlockIdHashMap = std.HashMap(
    BlockId,
    void,
    BlockIdHashContext,
    std.hash_map.default_max_load_percentage,
);

/// Traversal operation errors
pub const TraversalError = error{
    /// Block not found in storage
    BlockNotFound,
    /// Empty query (invalid parameters)
    EmptyQuery,
    /// Too many results requested
    TooManyResults,
    /// Query engine not initialized
    NotInitialized,
} || std.mem.Allocator.Error || storage.StorageError;

/// Traversal direction for graph queries
pub const TraversalDirection = enum(u8) {
    /// Follow outgoing edges (from source to targets)
    outgoing = 0x01,
    /// Follow incoming edges (from targets to sources)
    incoming = 0x02,
    /// Follow both outgoing and incoming edges
    bidirectional = 0x03,

    pub fn from_u8(value: u8) !TraversalDirection {
        return std.meta.intToEnum(TraversalDirection, value) catch TraversalError.EmptyQuery;
    }
};

/// Traversal algorithm type
pub const TraversalAlgorithm = enum(u8) {
    /// Breadth-first search (explores by depth level)
    breadth_first = 0x01,
    /// Depth-first search (explores deeply before backtracking)
    depth_first = 0x02,

    pub fn from_u8(value: u8) !TraversalAlgorithm {
        return std.meta.intToEnum(TraversalAlgorithm, value) catch TraversalError.EmptyQuery;
    }
};

/// Query for traversing the knowledge graph
pub const TraversalQuery = struct {
    /// Starting block ID for traversal
    start_block_id: BlockId,
    /// Direction to traverse (outgoing, incoming, or bidirectional)
    direction: TraversalDirection,
    /// Algorithm to use (BFS or DFS)
    algorithm: TraversalAlgorithm,
    /// Maximum depth to traverse (0 = no limit)
    max_depth: u32,
    /// Maximum number of blocks to return
    max_results: u32,
    /// Optional edge type filter (null = all types)
    edge_type_filter: ?EdgeType,

    /// Default maximum depth for traversal
    pub const DEFAULT_MAX_DEPTH = 10;
    /// Default maximum results
    pub const DEFAULT_MAX_RESULTS = 1000;
    /// Absolute maximum results to prevent memory exhaustion
    pub const ABSOLUTE_MAX_RESULTS = 10000;

    /// Create a basic traversal query with defaults
    pub fn init(start_block_id: BlockId, direction: TraversalDirection) TraversalQuery {
        return TraversalQuery{
            .start_block_id = start_block_id,
            .direction = direction,
            .algorithm = .breadth_first,
            .max_depth = DEFAULT_MAX_DEPTH,
            .max_results = DEFAULT_MAX_RESULTS,
            .edge_type_filter = null,
        };
    }

    /// Validate traversal query parameters
    pub fn validate(self: TraversalQuery) !void {
        if (self.max_results == 0) return TraversalError.EmptyQuery;
        if (self.max_results > ABSOLUTE_MAX_RESULTS) return TraversalError.TooManyResults;
    }
};

/// Result from graph traversal containing blocks and path information
pub const TraversalResult = struct {
    /// Retrieved context blocks in traversal order
    blocks: []const ContextBlock,
    /// Paths from start block to each result block
    paths: []const []const BlockId,
    /// Depths of each block from start block
    depths: []const u32,
    /// Total number of blocks traversed
    blocks_traversed: u32,
    /// Maximum depth reached during traversal
    max_depth_reached: u32,
    /// Allocator used for result memory
    allocator: std.mem.Allocator,

    /// Create traversal result
    pub fn init(
        allocator: std.mem.Allocator,
        blocks: []const ContextBlock,
        paths: []const []const BlockId,
        depths: []const u32,
        blocks_traversed: u32,
        max_depth_reached: u32,
    ) TraversalResult {
        return TraversalResult{
            .blocks = blocks,
            .paths = paths,
            .depths = depths,
            .blocks_traversed = blocks_traversed,
            .max_depth_reached = max_depth_reached,
            .allocator = allocator,
        };
    }

    /// Free allocated memory for traversal results
    pub fn deinit(self: TraversalResult) void {
        for (self.blocks) |block| {
            self.allocator.free(block.source_uri);
            self.allocator.free(block.metadata_json);
            self.allocator.free(block.content);
        }
        self.allocator.free(self.blocks);

        for (self.paths) |path| {
            self.allocator.free(path);
        }
        self.allocator.free(self.paths);

        self.allocator.free(self.depths);
    }

    /// Get number of blocks in traversal result
    pub fn count(self: TraversalResult) usize {
        return self.blocks.len;
    }

    /// Check if traversal result is empty
    pub fn is_empty(self: TraversalResult) bool {
        return self.blocks.len == 0;
    }
};

/// Execute a graph traversal query
/// Time complexity: O(V + E) where V is vertices and E is edges traversed
/// Space complexity: O(V) for visited tracking and result storage
pub fn execute_traversal(
    allocator: std.mem.Allocator,
    storage_engine: *StorageEngine,
    query: TraversalQuery,
) !TraversalResult {
    try query.validate();

    switch (query.algorithm) {
        .breadth_first => return traverse_breadth_first(allocator, storage_engine, query),
        .depth_first => return traverse_depth_first(allocator, storage_engine, query),
    }
}

/// Perform breadth-first traversal of the knowledge graph
fn traverse_breadth_first(
    allocator: std.mem.Allocator,
    storage_engine: *StorageEngine,
    query: TraversalQuery,
) !TraversalResult {
    var visited = BlockIdHashMap.init(allocator);
    defer visited.deinit();

    var result_blocks = std.ArrayList(ContextBlock).init(allocator);
    defer result_blocks.deinit();

    var result_paths = std.ArrayList([]BlockId).init(allocator);
    defer result_paths.deinit();

    var result_depths = std.ArrayList(u32).init(allocator);
    defer result_depths.deinit();

    const QueueItem = struct {
        block_id: BlockId,
        depth: u32,
        path: []BlockId,
    };

    var queue = std.ArrayList(QueueItem).init(allocator);
    defer {
        for (queue.items) |item| {
            allocator.free(item.path);
        }
        queue.deinit();
    }

    const start_path = try allocator.alloc(BlockId, 1);
    start_path[0] = query.start_block_id;

    try queue.append(QueueItem{
        .block_id = query.start_block_id,
        .depth = 0,
        .path = start_path,
    });

    try visited.put(query.start_block_id, {});

    var blocks_traversed: u32 = 0;
    var max_depth_reached: u32 = 0;

    while (queue.items.len > 0 and result_blocks.items.len < query.max_results) {
        const current = queue.orderedRemove(0);
        defer allocator.free(current.path);

        blocks_traversed += 1;
        max_depth_reached = @max(max_depth_reached, current.depth);

        const current_block = (try storage_engine.find_block(
            current.block_id,
        )) orelse continue;

        const cloned_block = try clone_block(allocator, current_block);
        try result_blocks.append(cloned_block);

        const cloned_path = try allocator.dupe(BlockId, current.path);
        try result_paths.append(cloned_path);

        try result_depths.append(current.depth);

        if (query.max_depth > 0 and current.depth >= query.max_depth) {
            continue;
        }

        try add_neighbors_to_queue(
            allocator,
            storage_engine,
            &queue,
            &visited,
            current.block_id,
            current.depth + 1,
            current.path,
            query,
        );
    }

    const owned_blocks = try result_blocks.toOwnedSlice();
    const owned_paths = try result_paths.toOwnedSlice();
    const owned_depths = try result_depths.toOwnedSlice();

    return TraversalResult.init(
        allocator,
        owned_blocks,
        owned_paths,
        owned_depths,
        blocks_traversed,
        max_depth_reached,
    );
}

/// Perform depth-first traversal of the knowledge graph
fn traverse_depth_first(
    allocator: std.mem.Allocator,
    storage_engine: *StorageEngine,
    query: TraversalQuery,
) !TraversalResult {
    var visited = BlockIdHashMap.init(allocator);
    defer visited.deinit();

    var result_blocks = std.ArrayList(ContextBlock).init(allocator);
    defer result_blocks.deinit();

    var result_paths = std.ArrayList([]BlockId).init(allocator);
    defer result_paths.deinit();

    var result_depths = std.ArrayList(u32).init(allocator);
    defer result_depths.deinit();

    const StackItem = struct {
        block_id: BlockId,
        depth: u32,
        path: []BlockId,
    };

    var stack = std.ArrayList(StackItem).init(allocator);
    defer {
        for (stack.items) |item| {
            allocator.free(item.path);
        }
        stack.deinit();
    }

    const start_path = try allocator.alloc(BlockId, 1);
    start_path[0] = query.start_block_id;

    try stack.append(StackItem{
        .block_id = query.start_block_id,
        .depth = 0,
        .path = start_path,
    });

    var blocks_traversed: u32 = 0;
    var max_depth_reached: u32 = 0;

    while (stack.items.len > 0 and result_blocks.items.len < query.max_results) {
        const current = stack.items[stack.items.len - 1];
        _ = stack.pop();
        defer allocator.free(current.path);

        if (visited.contains(current.block_id)) {
            continue;
        }

        try visited.put(current.block_id, {});
        blocks_traversed += 1;
        max_depth_reached = @max(max_depth_reached, current.depth);

        const current_block = (try storage_engine.find_block(
            current.block_id,
        )) orelse continue;

        const cloned_block = try clone_block(allocator, current_block);
        try result_blocks.append(cloned_block);

        const cloned_path = try allocator.dupe(BlockId, current.path);
        try result_paths.append(cloned_path);

        try result_depths.append(current.depth);

        if (query.max_depth > 0 and current.depth >= query.max_depth) {
            continue;
        }

        try add_neighbors_to_stack(
            allocator,
            storage_engine,
            &stack,
            &visited,
            current.block_id,
            current.depth + 1,
            current.path,
            query,
        );
    }

    const owned_blocks = try result_blocks.toOwnedSlice();
    const owned_paths = try result_paths.toOwnedSlice();
    const owned_depths = try result_depths.toOwnedSlice();

    return TraversalResult.init(
        allocator,
        owned_blocks,
        owned_paths,
        owned_depths,
        blocks_traversed,
        max_depth_reached,
    );
}

/// Clone a block for query results with owned memory
fn clone_block(allocator: std.mem.Allocator, block: ContextBlock) !ContextBlock {
    return ContextBlock{
        .id = block.id,
        .version = block.version,
        .source_uri = try allocator.dupe(u8, block.source_uri),
        .metadata_json = try allocator.dupe(u8, block.metadata_json),
        .content = try allocator.dupe(u8, block.content),
    };
}

/// Add neighbors to BFS queue
fn add_neighbors_to_queue(
    allocator: std.mem.Allocator,
    storage_engine: *StorageEngine,
    queue: anytype,
    visited: *BlockIdHashMap,
    current_id: BlockId,
    next_depth: u32,
    current_path: []const BlockId,
    query: TraversalQuery,
) !void {
    const QueueItem = @TypeOf(queue.items[0]);

    if (query.direction == .outgoing or query.direction == .bidirectional) {
        const edges = storage_engine.find_outgoing_edges(current_id);
        if (edges.len > 0) {
            for (edges) |edge| {
                if (query.edge_type_filter) |filter| {
                    if (edge.edge_type != filter) continue;
                }

                if (!visited.contains(edge.target_id)) {
                    const new_path = try allocator.alloc(BlockId, current_path.len + 1);
                    @memcpy(new_path[0..current_path.len], current_path);
                    new_path[current_path.len] = edge.target_id;

                    try queue.append(QueueItem{
                        .block_id = edge.target_id,
                        .depth = next_depth,
                        .path = new_path,
                    });
                }
            }
        }
    }

    if (query.direction == .incoming or query.direction == .bidirectional) {
        const edges = storage_engine.find_incoming_edges(current_id);
        if (edges.len > 0) {
            for (edges) |edge| {
                if (query.edge_type_filter) |filter| {
                    if (edge.edge_type != filter) continue;
                }

                if (!visited.contains(edge.source_id)) {
                    const new_path = try allocator.alloc(BlockId, current_path.len + 1);
                    @memcpy(new_path[0..current_path.len], current_path);
                    new_path[current_path.len] = edge.source_id;

                    try queue.append(QueueItem{
                        .block_id = edge.source_id,
                        .depth = next_depth,
                        .path = new_path,
                    });
                }
            }
        }
    }
}

/// Add neighbors to DFS stack
fn add_neighbors_to_stack(
    allocator: std.mem.Allocator,
    storage_engine: *StorageEngine,
    stack: anytype,
    visited: *BlockIdHashMap,
    current_id: BlockId,
    next_depth: u32,
    current_path: []const BlockId,
    query: TraversalQuery,
) !void {
    const StackItem = @TypeOf(stack.items[0]);

    if (query.direction == .outgoing or query.direction == .bidirectional) {
        const edges = storage_engine.find_outgoing_edges(current_id);
        if (edges.len > 0) {
            for (edges) |edge| {
                if (query.edge_type_filter) |filter| {
                    if (edge.edge_type != filter) continue;
                }

                if (!visited.contains(edge.target_id)) {
                    const new_path = try allocator.alloc(BlockId, current_path.len + 1);
                    @memcpy(new_path[0..current_path.len], current_path);
                    new_path[current_path.len] = edge.target_id;

                    try stack.append(StackItem{
                        .block_id = edge.target_id,
                        .depth = next_depth,
                        .path = new_path,
                    });
                }
            }
        }
    }

    if (query.direction == .incoming or query.direction == .bidirectional) {
        const edges = storage_engine.find_incoming_edges(current_id);
        if (edges.len > 0) {
            for (edges) |edge| {
                if (query.edge_type_filter) |filter| {
                    if (edge.edge_type != filter) continue;
                }

                if (!visited.contains(edge.source_id)) {
                    const new_path = try allocator.alloc(BlockId, current_path.len + 1);
                    @memcpy(new_path[0..current_path.len], current_path);
                    new_path[current_path.len] = edge.source_id;

                    try stack.append(StackItem{
                        .block_id = edge.source_id,
                        .depth = next_depth,
                        .path = new_path,
                    });
                }
            }
        }
    }
}

/// Convenience function for outgoing traversal
pub fn traverse_outgoing(
    allocator: std.mem.Allocator,
    storage_engine: *StorageEngine,
    start_id: BlockId,
    max_depth: u32,
) !TraversalResult {
    const query = TraversalQuery{
        .start_block_id = start_id,
        .direction = .outgoing,
        .algorithm = .breadth_first,
        .max_depth = max_depth,
        .max_results = TraversalQuery.DEFAULT_MAX_RESULTS,
        .edge_type_filter = null,
    };
    return execute_traversal(allocator, storage_engine, query);
}

/// Convenience function for incoming traversal
pub fn traverse_incoming(
    allocator: std.mem.Allocator,
    storage_engine: *StorageEngine,
    start_id: BlockId,
    max_depth: u32,
) !TraversalResult {
    const query = TraversalQuery{
        .start_block_id = start_id,
        .direction = .incoming,
        .algorithm = .breadth_first,
        .max_depth = max_depth,
        .max_results = TraversalQuery.DEFAULT_MAX_RESULTS,
        .edge_type_filter = null,
    };
    return execute_traversal(allocator, storage_engine, query);
}

/// Convenience function for bidirectional traversal
pub fn traverse_bidirectional(
    allocator: std.mem.Allocator,
    storage_engine: *StorageEngine,
    start_id: BlockId,
    max_depth: u32,
) !TraversalResult {
    const query = TraversalQuery{
        .start_block_id = start_id,
        .direction = .bidirectional,
        .algorithm = .breadth_first,
        .max_depth = max_depth,
        .max_results = TraversalQuery.DEFAULT_MAX_RESULTS,
        .edge_type_filter = null,
    };
    return execute_traversal(allocator, storage_engine, query);
}

fn create_test_storage_engine(allocator: std.mem.Allocator) !StorageEngine {
    var sim_vfs = try SimulationVFS.init(allocator);
    var storage_engine = try StorageEngine.init(allocator, sim_vfs.vfs(), "./test_traversal");
    try storage_engine.startup();
    return storage_engine;
}

fn create_test_block(id: BlockId, content: []const u8) ContextBlock {
    return ContextBlock{
        .id = id,
        .version = 1,
        .source_uri = "test://traversal.zig",
        .metadata_json = "{}",
        .content = content,
    };
}

fn create_test_edge(from_id: BlockId, to_id: BlockId, edge_type: EdgeType) GraphEdge {
    return GraphEdge{
        .from_block_id = from_id,
        .to_block_id = to_id,
        .edge_type = edge_type,
        .metadata_json = "{}",
    };
}

test "traversal query validation" {
    const start_id = try BlockId.from_hex("1111111111111111111111111111111111111111");

    // Valid query
    const valid_query = TraversalQuery.init(start_id, .outgoing, .breadth_first);
    try valid_query.validate();

    // Invalid - zero max depth
    var invalid_query = TraversalQuery.init(start_id, .outgoing, .breadth_first);
    invalid_query.max_depth = 0;
    try testing.expectError(TraversalError.InvalidDepth, invalid_query.validate());

    // Invalid - excessive max depth
    invalid_query.max_depth = 101; // > 100
    try testing.expectError(TraversalError.InvalidDepth, invalid_query.validate());

    // Invalid - zero max results
    invalid_query.max_depth = TraversalQuery.DEFAULT_MAX_DEPTH;
    invalid_query.max_results = 0;
    try testing.expectError(TraversalError.InvalidMaxResults, invalid_query.validate());

    // Invalid - excessive max results
    invalid_query.max_results = 20000; // > ABSOLUTE_MAX_RESULTS
    try testing.expectError(TraversalError.InvalidMaxResults, invalid_query.validate());
}

test "traversal direction enum parsing" {
    try testing.expectEqual(TraversalDirection.outgoing, try TraversalDirection.from_u8(0x01));
    try testing.expectEqual(TraversalDirection.incoming, try TraversalDirection.from_u8(0x02));
    try testing.expectEqual(TraversalDirection.bidirectional, try TraversalDirection.from_u8(0x03));

    // Invalid direction should return error
    try testing.expectError(TraversalError.InvalidDirection, TraversalDirection.from_u8(0xFF));
}

test "traversal algorithm enum parsing" {
    try testing.expectEqual(TraversalAlgorithm.breadth_first, try TraversalAlgorithm.from_u8(0x01));
    try testing.expectEqual(TraversalAlgorithm.depth_first, try TraversalAlgorithm.from_u8(0x02));

    // Invalid algorithm should return error
    try testing.expectError(TraversalError.InvalidAlgorithm, TraversalAlgorithm.from_u8(0xFF));
}

test "traversal result memory management" {
    const allocator = testing.allocator;

    const test_block = create_test_block(try BlockId.from_hex("1111111111111111111111111111111111111111"), "test content");

    var result = try TraversalResult.init(allocator, &[_]ContextBlock{test_block}, 1, 1);
    defer result.deinit();

    try testing.expectEqual(@as(usize, 1), result.count());
    try testing.expect(!result.is_empty());
    try testing.expectEqual(@as(u32, 1), result.blocks_traversed);
    try testing.expectEqual(@as(u32, 1), result.max_depth_reached);
}

test "basic traversal with empty graph" {
    const allocator = testing.allocator;

    var storage_engine = try create_test_storage_engine(allocator);
    defer storage_engine.deinit();

    const start_id = try BlockId.from_hex("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa");
    const start_block = create_test_block(start_id, "isolated block");
    try storage_engine.put_block(start_block);

    // Execute traversal
    const query = TraversalQuery.init(start_id, .outgoing, .breadth_first);
    const result = try execute_traversal(allocator, &storage_engine, query);
    defer result.deinit();

    // Should only find the starting block
    try testing.expectEqual(@as(usize, 1), result.count());
    try testing.expect(result.blocks[0].id.eql(start_id));
    try testing.expectEqual(@as(u32, 0), result.max_depth_reached);
}

test "outgoing traversal with linear chain" {
    const allocator = testing.allocator;

    var storage_engine = try create_test_storage_engine(allocator);
    defer storage_engine.deinit();

    const id_a = try BlockId.from_hex("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa");
    const id_b = try BlockId.from_hex("bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb");
    const id_c = try BlockId.from_hex("cccccccccccccccccccccccccccccccccccccccc");

    const block_a = create_test_block(id_a, "block A");
    const block_b = create_test_block(id_b, "block B");
    const block_c = create_test_block(id_c, "block C");

    try storage_engine.put_block(block_a);
    try storage_engine.put_block(block_b);
    try storage_engine.put_block(block_c);

    const edge_ab = create_test_edge(id_a, id_b, .calls);
    const edge_bc = create_test_edge(id_b, id_c, .calls);
    try storage_engine.put_edge(edge_ab);
    try storage_engine.put_edge(edge_bc);

    // Traverse outgoing from A with max depth 2
    const query = TraversalQuery{
        .start_block_id = id_a,
        .direction = .outgoing,
        .algorithm = .breadth_first,
        .max_depth = 2,
        .max_results = 10,
        .edge_type_filter = null,
    };

    const result = try execute_traversal(allocator, &storage_engine, query);
    defer result.deinit();

    // Should find A, B, C (depth 0, 1, 2)
    try testing.expectEqual(@as(usize, 3), result.count());
    try testing.expectEqual(@as(u32, 2), result.max_depth_reached);

    // Verify blocks are in the result
    var found_a = false;
    var found_b = false;
    var found_c = false;
    for (result.blocks) |block| {
        if (block.id.eql(id_a)) found_a = true;
        if (block.id.eql(id_b)) found_b = true;
        if (block.id.eql(id_c)) found_c = true;
    }
    try testing.expect(found_a and found_b and found_c);
}

test "incoming traversal" {
    const allocator = testing.allocator;

    var storage_engine = try create_test_storage_engine(allocator);
    defer storage_engine.deinit();

    const id_a = try BlockId.from_hex("1111111111111111111111111111111111111111");
    const id_b = try BlockId.from_hex("2222222222222222222222222222222222222222");
    const id_c = try BlockId.from_hex("3333333333333333333333333333333333333333");

    const block_a = create_test_block(id_a, "block A");
    const block_b = create_test_block(id_b, "block B");
    const block_c = create_test_block(id_c, "block C");

    try storage_engine.put_block(block_a);
    try storage_engine.put_block(block_b);
    try storage_engine.put_block(block_c);

    const edge_ab = create_test_edge(id_a, id_b, .imports);
    const edge_bc = create_test_edge(id_b, id_c, .imports);
    try storage_engine.put_edge(edge_ab);
    try storage_engine.put_edge(edge_bc);

    // Traverse incoming from C
    const result = try traverse_incoming(allocator, &storage_engine, id_c, 2);
    defer result.deinit();

    // Should find C, B, A (following edges backwards)
    try testing.expectEqual(@as(usize, 3), result.count());

    var found_a = false;
    var found_b = false;
    var found_c = false;
    for (result.blocks) |block| {
        if (block.id.eql(id_a)) found_a = true;
        if (block.id.eql(id_b)) found_b = true;
        if (block.id.eql(id_c)) found_c = true;
    }
    try testing.expect(found_a and found_b and found_c);
}

test "bidirectional traversal" {
    const allocator = testing.allocator;

    var storage_engine = try create_test_storage_engine(allocator);
    defer storage_engine.deinit();

    const id_a = try BlockId.from_hex("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa");
    const id_b = try BlockId.from_hex("bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb");
    const id_c = try BlockId.from_hex("cccccccccccccccccccccccccccccccccccccccc");

    const block_a = create_test_block(id_a, "block A");
    const block_b = create_test_block(id_b, "block B");
    const block_c = create_test_block(id_c, "block C");

    try storage_engine.put_block(block_a);
    try storage_engine.put_block(block_b);
    try storage_engine.put_block(block_c);

    const edge_ab = create_test_edge(id_a, id_b, .extends);
    const edge_cb = create_test_edge(id_c, id_b, .references);
    try storage_engine.put_edge(edge_ab);
    try storage_engine.put_edge(edge_cb);

    // Traverse bidirectional from B
    const result = try traverse_bidirectional(allocator, &storage_engine, id_b, 1);
    defer result.deinit();

    // Should find A, B, C
    try testing.expectEqual(@as(usize, 3), result.count());

    var found_a = false;
    var found_b = false;
    var found_c = false;
    for (result.blocks) |block| {
        if (block.id.eql(id_a)) found_a = true;
        if (block.id.eql(id_b)) found_b = true;
        if (block.id.eql(id_c)) found_c = true;
    }
    try testing.expect(found_a and found_b and found_c);
}

test "edge type filtering" {
    const allocator = testing.allocator;

    var storage_engine = try create_test_storage_engine(allocator);
    defer storage_engine.deinit();

    const id_a = try BlockId.from_hex("1111111111111111111111111111111111111111");
    const id_b = try BlockId.from_hex("2222222222222222222222222222222222222222");
    const id_c = try BlockId.from_hex("3333333333333333333333333333333333333333");

    const block_a = create_test_block(id_a, "block A");
    const block_b = create_test_block(id_b, "block B");
    const block_c = create_test_block(id_c, "block C");

    try storage_engine.put_block(block_a);
    try storage_engine.put_block(block_b);
    try storage_engine.put_block(block_c);

    // A -> B (calls), A -> C (imports)
    const edge_ab_calls = create_test_edge(id_a, id_b, .calls);
    const edge_ac_imports = create_test_edge(id_a, id_c, .imports);
    try storage_engine.put_edge(edge_ab_calls);
    try storage_engine.put_edge(edge_ac_imports);

    // Traverse outgoing from A, filtering only 'calls' edges
    const query_calls = TraversalQuery{
        .start_block_id = id_a,
        .direction = .outgoing,
        .algorithm = .breadth_first,
        .max_depth = 1,
        .max_results = 10,
        .edge_type_filter = .calls,
    };

    const result_calls = try execute_traversal(allocator, &storage_engine, query_calls);
    defer result_calls.deinit();

    // Should find A and B only (not C)
    try testing.expectEqual(@as(usize, 2), result_calls.count());

    var found_a = false;
    var found_b = false;
    var found_c = false;
    for (result_calls.blocks) |block| {
        if (block.id.eql(id_a)) found_a = true;
        if (block.id.eql(id_b)) found_b = true;
        if (block.id.eql(id_c)) found_c = true;
    }
    try testing.expect(found_a and found_b and !found_c);
}

test "max depth limit enforcement" {
    const allocator = testing.allocator;

    var storage_engine = try create_test_storage_engine(allocator);
    defer storage_engine.deinit();

    const id_a = try BlockId.from_hex("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa");
    const id_b = try BlockId.from_hex("bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb");
    const id_c = try BlockId.from_hex("cccccccccccccccccccccccccccccccccccccccc");
    const id_d = try BlockId.from_hex("dddddddddddddddddddddddddddddddddddddddd");

    const blocks = [_]ContextBlock{
        create_test_block(id_a, "block A"),
        create_test_block(id_b, "block B"),
        create_test_block(id_c, "block C"),
        create_test_block(id_d, "block D"),
    };

    for (blocks) |block| {
        try storage_engine.put_block(block);
    }

    const edges = [_]GraphEdge{
        create_test_edge(id_a, id_b, .calls),
        create_test_edge(id_b, id_c, .calls),
        create_test_edge(id_c, id_d, .calls),
    };

    for (edges) |edge| {
        try storage_engine.put_edge(edge);
    }

    // Traverse with max depth 2
    const query = TraversalQuery{
        .start_block_id = id_a,
        .direction = .outgoing,
        .algorithm = .breadth_first,
        .max_depth = 2,
        .max_results = 10,
        .edge_type_filter = null,
    };

    const result = try execute_traversal(allocator, &storage_engine, query);
    defer result.deinit();

    // Should find A, B, C but not D (depth 0, 1, 2 but not 3)
    try testing.expectEqual(@as(usize, 3), result.count());
    try testing.expectEqual(@as(u32, 2), result.max_depth_reached);

    var found_d = false;
    for (result.blocks) |block| {
        if (block.id.eql(id_d)) found_d = true;
    }
    try testing.expect(!found_d);
}

test "max results limit enforcement" {
    const allocator = testing.allocator;

    var storage_engine = try create_test_storage_engine(allocator);
    defer storage_engine.deinit();

    const id_a = try BlockId.from_hex("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa");
    const id_b = try BlockId.from_hex("bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb");
    const id_c = try BlockId.from_hex("cccccccccccccccccccccccccccccccccccccccc");
    const id_d = try BlockId.from_hex("dddddddddddddddddddddddddddddddddddddddd");
    const id_e = try BlockId.from_hex("eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee");

    const blocks = [_]ContextBlock{
        create_test_block(id_a, "block A"),
        create_test_block(id_b, "block B"),
        create_test_block(id_c, "block C"),
        create_test_block(id_d, "block D"),
        create_test_block(id_e, "block E"),
    };

    for (blocks) |block| {
        try storage_engine.put_block(block);
    }

    const edges = [_]GraphEdge{
        create_test_edge(id_a, id_b, .calls),
        create_test_edge(id_a, id_c, .calls),
        create_test_edge(id_a, id_d, .calls),
        create_test_edge(id_a, id_e, .calls),
    };

    for (edges) |edge| {
        try storage_engine.put_edge(edge);
    }

    // Traverse with max results 3
    const query = TraversalQuery{
        .start_block_id = id_a,
        .direction = .outgoing,
        .algorithm = .breadth_first,
        .max_depth = 2,
        .max_results = 3,
        .edge_type_filter = null,
    };

    const result = try execute_traversal(allocator, &storage_engine, query);
    defer result.deinit();

    // Should find exactly 3 blocks (limited by max_results)
    try testing.expectEqual(@as(usize, 3), result.count());
}

test "breadth-first vs depth-first traversal ordering" {
    const allocator = testing.allocator;

    var storage_engine = try create_test_storage_engine(allocator);
    defer storage_engine.deinit();

    //     A
    //   /   \
    //  B     C
    //  |     |
    //  D     E
    const id_a = try BlockId.from_hex("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa");
    const id_b = try BlockId.from_hex("bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb");
    const id_c = try BlockId.from_hex("cccccccccccccccccccccccccccccccccccccccc");
    const id_d = try BlockId.from_hex("dddddddddddddddddddddddddddddddddddddddd");
    const id_e = try BlockId.from_hex("eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee");

    const blocks = [_]ContextBlock{
        create_test_block(id_a, "block A"),
        create_test_block(id_b, "block B"),
        create_test_block(id_c, "block C"),
        create_test_block(id_d, "block D"),
        create_test_block(id_e, "block E"),
    };

    for (blocks) |block| {
        try storage_engine.put_block(block);
    }

    const edges = [_]GraphEdge{
        create_test_edge(id_a, id_b, .calls),
        create_test_edge(id_a, id_c, .calls),
        create_test_edge(id_b, id_d, .calls),
        create_test_edge(id_c, id_e, .calls),
    };

    for (edges) |edge| {
        try storage_engine.put_edge(edge);
    }

    // Test breadth-first
    const bfs_query = TraversalQuery{
        .start_block_id = id_a,
        .direction = .outgoing,
        .algorithm = .breadth_first,
        .max_depth = 2,
        .max_results = 10,
        .edge_type_filter = null,
    };

    const bfs_result = try execute_traversal(allocator, &storage_engine, bfs_query);
    defer bfs_result.deinit();

    // Test depth-first
    const dfs_query = TraversalQuery{
        .start_block_id = id_a,
        .direction = .outgoing,
        .algorithm = .depth_first,
        .max_depth = 2,
        .max_results = 10,
        .edge_type_filter = null,
    };

    const dfs_result = try execute_traversal(allocator, &storage_engine, dfs_query);
    defer dfs_result.deinit();

    // Both should find all 5 blocks
    try testing.expectEqual(@as(usize, 5), bfs_result.count());
    try testing.expectEqual(@as(usize, 5), dfs_result.count());

    // Verify both approaches visit the same blocks
    var bfs_ids = std.ArrayList(BlockId).init(allocator);
    defer bfs_ids.deinit();
    var dfs_ids = std.ArrayList(BlockId).init(allocator);
    defer dfs_ids.deinit();

    for (bfs_result.blocks) |block| {
        try bfs_ids.append(block.id);
    }
    for (dfs_result.blocks) |block| {
        try dfs_ids.append(block.id);
    }

    // Both should have same set of IDs (though possibly different order)
    try testing.expectEqual(bfs_ids.items.len, dfs_ids.items.len);
}

test "traversal error handling" {
    const allocator = testing.allocator;

    var storage_engine = try create_test_storage_engine(allocator);
    defer storage_engine.deinit();

    // Test traversal with non-existent start block
    const missing_id = try BlockId.from_hex("0000000000000000000000000000000000000000");

    const query = TraversalQuery.init(missing_id, .outgoing, .breadth_first);
    const result = try execute_traversal(allocator, &storage_engine, query);
    defer result.deinit();

    // Should return empty result for missing start block
    try testing.expect(result.is_empty());
    try testing.expectEqual(@as(usize, 0), result.count());
}

test "convenience traversal functions" {
    const allocator = testing.allocator;

    var storage_engine = try create_test_storage_engine(allocator);
    defer storage_engine.deinit();

    const start_id = try BlockId.from_hex("1111111111111111111111111111111111111111");
    const test_block = create_test_block(start_id, "convenience test");
    try storage_engine.put_block(test_block);

    // Test convenience functions
    const outgoing_result = try traverse_outgoing(allocator, &storage_engine, start_id, 2);
    defer outgoing_result.deinit();

    const incoming_result = try traverse_incoming(allocator, &storage_engine, start_id, 2);
    defer incoming_result.deinit();

    const bidirectional_result = try traverse_bidirectional(allocator, &storage_engine, start_id, 2);
    defer bidirectional_result.deinit();

    // All should find at least the starting block
    try testing.expect(!outgoing_result.is_empty());
    try testing.expect(!incoming_result.is_empty());
    try testing.expect(!bidirectional_result.is_empty());

    try testing.expectEqual(@as(usize, 1), outgoing_result.count());
    try testing.expectEqual(@as(usize, 1), incoming_result.count());
    try testing.expectEqual(@as(usize, 1), bidirectional_result.count());
}
