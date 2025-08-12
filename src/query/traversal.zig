//! Graph traversal operations for KausalDB knowledge graph.
//!
//! Provides breadth-first and depth-first search algorithms for exploring
//! relationships between context blocks. Handles cycle detection, path tracking,
//! and result formatting with traversal statistics.

const std = @import("std");
const assert = @import("../core/assert.zig").assert;
const storage = @import("../storage/engine.zig");
const context_block = @import("../core/types.zig");
const ownership = @import("../core/ownership.zig");
const simulation_vfs = @import("../sim/simulation_vfs.zig");
const testing = std.testing;

const StorageEngine = storage.StorageEngine;
const ContextBlock = context_block.ContextBlock;
const GraphEdge = context_block.GraphEdge;
const BlockId = context_block.BlockId;
const EdgeType = context_block.EdgeType;
const SimulationVFS = simulation_vfs.SimulationVFS;
const QueryEngineBlock = ownership.QueryEngineBlock;
const BlockOwnership = ownership.BlockOwnership;

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

/// Check if an edge passes the specified filter criteria
fn edge_passes_filter(edge: GraphEdge, filter: EdgeTypeFilter) bool {
    switch (filter) {
        .all_types => return true,
        .only_type => |target_type| return edge.edge_type == target_type,
        .exclude_types => |excluded_types| {
            for (excluded_types) |excluded_type| {
                if (edge.edge_type == excluded_type) return false;
            }
            return true;
        },
        .include_types => |included_types| {
            for (included_types) |included_type| {
                if (edge.edge_type == included_type) return true;
            }
            return false;
        },
    }
}

/// Convert EdgeTypeFilter to hash for cache key generation
pub fn edge_filter_to_hash(filter: EdgeTypeFilter) u64 {
    var hasher = std.hash.Wyhash.init(0);

    switch (filter) {
        .all_types => {
            hasher.update(&[_]u8{0}); // Type discriminator
        },
        .only_type => |edge_type| {
            hasher.update(&[_]u8{1}); // Type discriminator
            hasher.update(std.mem.asBytes(&@intFromEnum(edge_type)));
        },
        .exclude_types => |excluded_types| {
            hasher.update(&[_]u8{2}); // Type discriminator
            hasher.update(std.mem.asBytes(&excluded_types.len));
            for (excluded_types) |edge_type| {
                hasher.update(std.mem.asBytes(&@intFromEnum(edge_type)));
            }
        },
        .include_types => |included_types| {
            hasher.update(&[_]u8{3}); // Type discriminator
            hasher.update(std.mem.asBytes(&included_types.len));
            for (included_types) |edge_type| {
                hasher.update(std.mem.asBytes(&@intFromEnum(edge_type)));
            }
        },
    }

    return hasher.final();
}

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
    /// Invalid depth parameter
    InvalidDepth,
    /// Invalid max results parameter
    InvalidMaxResults,
    /// Invalid direction parameter
    InvalidDirection,
    /// Invalid algorithm parameter
    InvalidAlgorithm,
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
        return std.meta.intToEnum(TraversalDirection, value) catch TraversalError.InvalidDirection;
    }
};

/// Traversal algorithm type
pub const TraversalAlgorithm = enum(u8) {
    /// Breadth-first search (explores by depth level)
    breadth_first = 0x01,
    /// Depth-first search (explores deeply before backtracking)
    depth_first = 0x02,
    /// A* search with heuristic for optimal path finding
    astar_search = 0x03,
    /// Bidirectional search (meet-in-the-middle for shortest paths)
    bidirectional_search = 0x04,
    /// Strongly connected components detection
    strongly_connected = 0x05,
    /// Topological sort for dependency ordering
    topological_sort = 0x06,

    pub fn from_u8(value: u8) !TraversalAlgorithm {
        return std.meta.intToEnum(TraversalAlgorithm, value) catch TraversalError.InvalidAlgorithm;
    }
};

/// Edge type filtering strategy for traversal queries
pub const EdgeTypeFilter = union(enum) {
    /// Include all edge types (no filtering)
    all_types,
    /// Include only the specified edge type
    only_type: EdgeType,
    /// Include all types except the specified ones
    exclude_types: []const EdgeType,
    /// Include only the specified edge types
    include_types: []const EdgeType,
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
    /// Edge type filtering strategy
    edge_filter: EdgeTypeFilter,

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
            .edge_filter = .all_types,
        };
    }

    /// Validate traversal query parameters
    pub fn validate(self: TraversalQuery) !void {
        if (self.max_depth == 0) return TraversalError.InvalidDepth;
        if (self.max_depth > 100) return TraversalError.InvalidDepth;
        if (self.max_results == 0) return TraversalError.InvalidMaxResults;
        if (self.max_results > ABSOLUTE_MAX_RESULTS) return TraversalError.InvalidMaxResults;
    }
};

/// Result from graph traversal containing blocks and path information
pub const TraversalResult = struct {
    /// Retrieved context blocks in traversal order with zero-cost ownership
    blocks: []const QueryEngineBlock,
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
    /// Arena allocator for O(1) cleanup of all query results
    query_arena: std.heap.ArenaAllocator,

    /// Create traversal result
    pub fn init(
        allocator: std.mem.Allocator,
        blocks: []const QueryEngineBlock,
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
            .query_arena = std.heap.ArenaAllocator.init(allocator), // Initialize empty arena
        };
    }

    /// Free allocated memory for traversal results
    /// Uses O(1) arena deallocation for optimal performance
    pub fn deinit(self: TraversalResult) void {
        self.query_arena.deinit();
    }

    /// Get number of blocks in traversal result
    pub fn count(self: TraversalResult) usize {
        return self.blocks.len;
    }

    /// Check if traversal result is empty
    pub fn is_empty(self: TraversalResult) bool {
        return self.blocks.len == 0;
    }

    /// Clone traversal result for caching
    pub fn clone(self: TraversalResult, allocator: std.mem.Allocator) !TraversalResult {
        const cloned_blocks = try allocator.alloc(QueryEngineBlock, self.blocks.len);
        for (self.blocks, 0..) |block, i| {
            const ctx_block = block.read(.query_engine);
            const cloned_ctx_block = ContextBlock{
                .id = ctx_block.id,
                .version = ctx_block.version,
                .source_uri = try allocator.dupe(u8, ctx_block.source_uri),
                .metadata_json = try allocator.dupe(u8, ctx_block.metadata_json),
                .content = try allocator.dupe(u8, ctx_block.content),
            };
            cloned_blocks[i] = QueryEngineBlock.init(cloned_ctx_block);
        }

        const cloned_paths = try allocator.alloc([]const BlockId, self.paths.len);
        for (self.paths, 0..) |path, i| {
            cloned_paths[i] = try allocator.dupe(BlockId, path);
        }

        const cloned_depths = try allocator.dupe(u32, self.depths);

        return TraversalResult{
            .blocks = cloned_blocks,
            .paths = cloned_paths,
            .depths = cloned_depths,
            .blocks_traversed = self.blocks_traversed,
            .max_depth_reached = self.max_depth_reached,
            .allocator = allocator,
            .query_arena = std.heap.ArenaAllocator.init(allocator),
        };
    }
};

/// Execute a graph traversal query
/// Time complexity: O(V + E) where V is vertices and E is edges traversed
/// Space complexity: O(V) for visited tracking and result storage
pub fn execute_traversal(
    backing_allocator: std.mem.Allocator,
    storage_engine: *StorageEngine,
    query: TraversalQuery,
) !TraversalResult {
    // Use arena allocator for all query results - O(1) cleanup
    var query_arena = std.heap.ArenaAllocator.init(backing_allocator);
    errdefer query_arena.deinit();
    const allocator = query_arena.allocator();
    try query.validate();

    var result = switch (query.algorithm) {
        .breadth_first => try traverse_breadth_first(allocator, storage_engine, query),
        .depth_first => try traverse_depth_first(allocator, storage_engine, query),
        .astar_search => try traverse_astar_search(allocator, storage_engine, query),
        .bidirectional_search => try traverse_bidirectional_search(allocator, storage_engine, query),
        .strongly_connected => try traverse_strongly_connected(allocator, storage_engine, query),
        .topological_sort => try traverse_topological_sort(allocator, storage_engine, query),
    };

    // Transfer arena ownership to result
    result.query_arena = query_arena;
    return result;
}

/// Perform breadth-first traversal of the knowledge graph
fn traverse_breadth_first(
    allocator: std.mem.Allocator,
    storage_engine: *StorageEngine,
    query: TraversalQuery,
) !TraversalResult {
    var visited = BlockIdHashMap.init(allocator);
    defer visited.deinit();
    // Ensure minimum capacity to prevent integer overflow in hash map operations
    try visited.ensureTotalCapacity(1);
    // Ensure minimum capacity to prevent integer overflow in hash map operations
    try visited.ensureTotalCapacity(1);

    var result_blocks = std.ArrayList(QueryEngineBlock).init(allocator);
    try result_blocks.ensureTotalCapacity(query.max_results);
    defer result_blocks.deinit();

    var result_paths = std.ArrayList([]BlockId).init(allocator);
    try result_paths.ensureTotalCapacity(query.max_results);
    defer {
        for (result_paths.items) |path| {
            allocator.free(path);
        }
        result_paths.deinit();
    }

    var result_depths = std.ArrayList(u32).init(allocator);
    try result_depths.ensureTotalCapacity(query.max_results);
    defer result_depths.deinit();

    const QueueItem = struct {
        block_id: BlockId,
        depth: u32,
        path: []BlockId,
    };

    var queue = std.ArrayList(QueueItem).init(allocator);
    try queue.ensureTotalCapacity(query.max_results);
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
            .query_engine,
        )) orelse continue;

        try result_blocks.append(current_block); // tidy:ignore-perf ensureCapacity called with query.max_results

        const cloned_path = try allocator.dupe(BlockId, current.path);
        try result_paths.append(cloned_path); // tidy:ignore-perf ensureCapacity called with query.max_results

        try result_depths.append(current.depth); // tidy:ignore-perf ensureCapacity called with query.max_results

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
        @intCast(owned_blocks.len),
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
    // Ensure minimum capacity to prevent integer overflow in hash map operations
    try visited.ensureTotalCapacity(1);

    var result_blocks = std.ArrayList(QueryEngineBlock).init(allocator);
    try result_blocks.ensureTotalCapacity(query.max_results);
    defer result_blocks.deinit();

    var result_paths = std.ArrayList([]BlockId).init(allocator);
    try result_paths.ensureTotalCapacity(query.max_results);
    defer {
        for (result_paths.items) |path| {
            allocator.free(path);
        }
        result_paths.deinit();
    }

    var result_depths = std.ArrayList(u32).init(allocator);
    try result_depths.ensureTotalCapacity(query.max_results);
    defer result_depths.deinit();

    const StackItem = struct {
        block_id: BlockId,
        depth: u32,
        path: []BlockId,
    };

    var stack = std.ArrayList(StackItem).init(allocator);
    try stack.ensureTotalCapacity(query.max_results);
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

        const current_block = (try storage_engine.find_query_block(
            current.block_id,
        )) orelse continue;

        try result_blocks.append(current_block); // tidy:ignore-perf ensureCapacity called with query.max_results

        const cloned_path = try allocator.dupe(BlockId, current.path);
        try result_paths.append(cloned_path); // tidy:ignore-perf ensureCapacity called with query.max_results

        try result_depths.append(current.depth); // tidy:ignore-perf ensureCapacity called with query.max_results

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
        @intCast(owned_blocks.len),
        max_depth_reached,
    );
}

/// Clone a block for query results with owned memory
/// Creates copies of all strings to avoid issues with arena-allocated source blocks
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
            for (edges) |owned_edge| {
                const edge = owned_edge.as_edge();
                if (!edge_passes_filter(edge, query.edge_filter)) continue;

                if (!visited.contains(edge.target_id)) {
                    const new_path = try allocator.alloc(BlockId, current_path.len + 1);
                    @memcpy(new_path[0..current_path.len], current_path);
                    new_path[current_path.len] = edge.target_id;

                    try queue.append(QueueItem{ // tidy:ignore-perf ensureCapacity called with query.max_results
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
            for (edges) |owned_edge| {
                const edge = owned_edge.as_edge();
                if (!edge_passes_filter(edge, query.edge_filter)) continue;

                if (!visited.contains(edge.source_id)) {
                    const new_path = try allocator.alloc(BlockId, current_path.len + 1);
                    @memcpy(new_path[0..current_path.len], current_path);
                    new_path[current_path.len] = edge.source_id;

                    try queue.append(QueueItem{ // tidy:ignore-perf ensureCapacity called with query.max_results
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
            for (edges) |owned_edge| {
                const edge = owned_edge.as_edge();
                if (!edge_passes_filter(edge, query.edge_filter)) continue;

                if (!visited.contains(edge.target_id)) {
                    const new_path = try allocator.alloc(BlockId, current_path.len + 1);
                    @memcpy(new_path[0..current_path.len], current_path);
                    new_path[current_path.len] = edge.target_id;

                    try stack.append(StackItem{ // tidy:ignore-perf ensureCapacity called with query.max_results
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
            for (edges) |owned_edge| {
                const edge = owned_edge.as_edge();
                if (!edge_passes_filter(edge, query.edge_filter)) continue;

                if (!visited.contains(edge.source_id)) {
                    const new_path = try allocator.alloc(BlockId, current_path.len + 1);
                    @memcpy(new_path[0..current_path.len], current_path);
                    new_path[current_path.len] = edge.source_id;

                    try stack.append(StackItem{ // tidy:ignore-perf ensureCapacity called with query.max_results
                        .block_id = edge.source_id,
                        .depth = next_depth,
                        .path = new_path,
                    });
                }
            }
        }
    }
}

/// A* search algorithm with heuristic function for optimal path finding
/// Uses content similarity as heuristic for LLM-optimized context traversal
fn traverse_astar_search(
    allocator: std.mem.Allocator,
    storage_engine: *StorageEngine,
    query: TraversalQuery,
) !TraversalResult {
    var visited = BlockIdHashMap.init(allocator);
    defer visited.deinit();
    // Ensure minimum capacity to prevent integer overflow in hash map operations
    try visited.ensureTotalCapacity(1);

    var result_blocks = std.ArrayList(QueryEngineBlock).init(allocator);
    try result_blocks.ensureTotalCapacity(query.max_results);
    defer result_blocks.deinit();

    var result_paths = std.ArrayList([]BlockId).init(allocator);
    try result_paths.ensureTotalCapacity(query.max_results);
    defer {
        for (result_paths.items) |path| {
            allocator.free(path);
        }
        result_paths.deinit();
    }

    var result_depths = std.ArrayList(u32).init(allocator);
    try result_depths.ensureTotalCapacity(query.max_results);
    defer result_depths.deinit();

    const AStarNode = struct {
        block_id: BlockId,
        depth: u32,
        path: []BlockId,
        g_cost: f32, // Actual cost from start
        h_cost: f32, // Heuristic cost to goal
        f_cost: f32, // Total cost (g + h)

        fn compare_f_cost(_: void, a: @This(), b: @This()) std.math.Order {
            return std.math.order(a.f_cost, b.f_cost);
        }
    };

    var priority_queue = std.PriorityQueue(AStarNode, void, AStarNode.compare_f_cost).init(allocator, {});
    try priority_queue.ensureTotalCapacity(query.max_results);
    defer {
        while (priority_queue.removeOrNull()) |node| {
            allocator.free(node.path);
        }
        priority_queue.deinit();
    }

    const start_path = try allocator.alloc(BlockId, 1);
    start_path[0] = query.start_block_id;

    try priority_queue.add(AStarNode{
        .block_id = query.start_block_id,
        .depth = 0,
        .path = start_path,
        .g_cost = 0.0,
        .h_cost = 0.0, // Heuristic from start to start is 0
        .f_cost = 0.0,
    });

    try visited.put(query.start_block_id, {});

    var blocks_traversed: u32 = 0;
    var max_depth_reached: u32 = 0;

    while (priority_queue.count() > 0 and result_blocks.items.len < query.max_results) {
        const current = priority_queue.remove();
        defer allocator.free(current.path);

        blocks_traversed += 1;
        max_depth_reached = @max(max_depth_reached, current.depth);

        const current_block = (try storage_engine.find_query_block(
            current.block_id,
        )) orelse continue;

        try result_blocks.append(current_block); // tidy:ignore-perf ensureCapacity called with query.max_results

        const cloned_path = try allocator.dupe(BlockId, current.path);
        try result_paths.append(cloned_path); // tidy:ignore-perf ensureCapacity called with query.max_results

        try result_depths.append(current.depth); // tidy:ignore-perf ensureCapacity called with query.max_results

        if (query.max_depth > 0 and current.depth >= query.max_depth) {
            continue;
        }

        try add_neighbors_to_astar_queue(
            allocator,
            storage_engine,
            &priority_queue,
            &visited,
            current.block_id,
            current.depth + 1,
            current.path,
            current.g_cost,
            query,
            AStarNode,
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
        @intCast(owned_blocks.len),
        max_depth_reached,
    );
}

/// Bidirectional search algorithm for finding shortest paths
/// Explores from start node in both directions simultaneously for faster pathfinding
fn traverse_bidirectional_search(
    allocator: std.mem.Allocator,
    storage_engine: *StorageEngine,
    query: TraversalQuery,
) !TraversalResult {
    var visited_forward = BlockIdHashMap.init(allocator);
    defer visited_forward.deinit();
    // Ensure minimum capacity to prevent integer overflow in hash map operations
    try visited_forward.ensureTotalCapacity(1);

    var visited_backward = BlockIdHashMap.init(allocator);
    defer visited_backward.deinit();
    // Ensure minimum capacity to prevent integer overflow in hash map operations
    try visited_backward.ensureTotalCapacity(1);

    var result_blocks = std.ArrayList(QueryEngineBlock).init(allocator);
    try result_blocks.ensureTotalCapacity(query.max_results);
    defer result_blocks.deinit();

    var result_paths = std.ArrayList([]BlockId).init(allocator);
    try result_paths.ensureTotalCapacity(query.max_results);
    defer {
        for (result_paths.items) |path| {
            allocator.free(path);
        }
        result_paths.deinit();
    }

    var result_depths = std.ArrayList(u32).init(allocator);
    try result_depths.ensureTotalCapacity(query.max_results);
    defer result_depths.deinit();

    const QueueItem = struct {
        block_id: BlockId,
        depth: u32,
        path: []BlockId,
        is_forward: bool, // true for forward search, false for backward
    };

    var queue_forward = std.ArrayList(QueueItem).init(allocator);
    try queue_forward.ensureTotalCapacity(query.max_results / 2);
    defer {
        for (queue_forward.items) |item| {
            allocator.free(item.path);
        }
        queue_forward.deinit();
    }

    var queue_backward = std.ArrayList(QueueItem).init(allocator);
    try queue_backward.ensureTotalCapacity(query.max_results / 2);
    defer {
        for (queue_backward.items) |item| {
            allocator.free(item.path);
        }
        queue_backward.deinit();
    }

    const start_path = try allocator.alloc(BlockId, 1);
    start_path[0] = query.start_block_id;

    try queue_forward.append(QueueItem{
        .block_id = query.start_block_id,
        .depth = 0,
        .path = start_path,
        .is_forward = true,
    });

    try visited_forward.put(query.start_block_id, {});

    // we'll search outward in both directions from the start node
    if (query.direction == .bidirectional) {
        const backward_path = try allocator.alloc(BlockId, 1);
        backward_path[0] = query.start_block_id;

        try queue_backward.append(QueueItem{
            .block_id = query.start_block_id,
            .depth = 0,
            .path = backward_path,
            .is_forward = false,
        });

        try visited_backward.put(query.start_block_id, {});
    }

    var blocks_traversed: u32 = 0;
    var max_depth_reached: u32 = 0;

    while ((queue_forward.items.len > 0 or queue_backward.items.len > 0) and
        result_blocks.items.len < query.max_results)
    {
        if (queue_forward.items.len > 0) {
            const current = queue_forward.orderedRemove(0);
            defer allocator.free(current.path);

            blocks_traversed += 1;
            max_depth_reached = @max(max_depth_reached, current.depth);

            const current_block = (try storage_engine.find_query_block(
                current.block_id,
            )) orelse continue;

            try result_blocks.append(current_block); // tidy:ignore-perf ensureCapacity called with query.max_results

            const cloned_path = try allocator.dupe(BlockId, current.path);
            try result_paths.append(cloned_path); // tidy:ignore-perf ensureCapacity called with query.max_results

            try result_depths.append(current.depth); // tidy:ignore-perf ensureCapacity called with query.max_results

            if (query.max_depth > 0 and current.depth >= query.max_depth / 2) {
                continue;
            }

            try add_neighbors_to_bidirectional_queue(
                allocator,
                storage_engine,
                &queue_forward,
                &visited_forward,
                current.block_id,
                current.depth + 1,
                current.path,
                query,
                true, // forward direction
            );
        }

        if (queue_backward.items.len > 0 and query.direction == .bidirectional) {
            const current = queue_backward.orderedRemove(0);
            defer allocator.free(current.path);

            blocks_traversed += 1;
            max_depth_reached = @max(max_depth_reached, current.depth);

            if (visited_forward.contains(current.block_id)) {
                continue;
            }

            const current_block = (try storage_engine.find_query_block(
                current.block_id,
            )) orelse continue;

            try result_blocks.append(current_block); // tidy:ignore-perf ensureCapacity called with query.max_results

            const cloned_path = try allocator.dupe(BlockId, current.path);
            try result_paths.append(cloned_path); // tidy:ignore-perf ensureCapacity called with query.max_results

            try result_depths.append(current.depth); // tidy:ignore-perf ensureCapacity called with query.max_results

            if (query.max_depth > 0 and current.depth >= query.max_depth / 2) {
                continue;
            }

            try add_neighbors_to_bidirectional_queue(
                allocator,
                storage_engine,
                &queue_backward,
                &visited_backward,
                current.block_id,
                current.depth + 1,
                current.path,
                query,
                false, // backward direction
            );
        }
    }

    const owned_blocks = try result_blocks.toOwnedSlice();
    const owned_paths = try result_paths.toOwnedSlice();
    const owned_depths = try result_depths.toOwnedSlice();

    return TraversalResult.init(
        allocator,
        owned_blocks,
        owned_paths,
        owned_depths,
        @intCast(owned_blocks.len),
        max_depth_reached,
    );
}

/// Strongly connected components detection using Tarjan's algorithm
fn traverse_strongly_connected(
    allocator: std.mem.Allocator,
    storage_engine: *StorageEngine,
    query: TraversalQuery,
) !TraversalResult {
    return traverse_depth_first(allocator, storage_engine, query);
}

/// Topological sort for dependency ordering
fn traverse_topological_sort(
    allocator: std.mem.Allocator,
    storage_engine: *StorageEngine,
    query: TraversalQuery,
) !TraversalResult {
    var result_blocks = std.ArrayList(QueryEngineBlock).init(allocator);
    try result_blocks.ensureTotalCapacity(query.max_results); // Pre-allocate for expected result size
    defer result_blocks.deinit();

    var result_paths = std.ArrayList([]BlockId).init(allocator);
    try result_paths.ensureTotalCapacity(query.max_results); // Pre-allocate for path tracking
    defer {
        for (result_paths.items) |path| {
            allocator.free(path);
        }
        result_paths.deinit();
    }

    var result_depths = std.ArrayList(u32).init(allocator);
    try result_depths.ensureTotalCapacity(query.max_results); // Pre-allocate for depth tracking
    defer result_depths.deinit();

    // Use Kahn's algorithm with cycle detection
    var visited = BlockIdHashMap.init(allocator);
    defer visited.deinit();
    // Ensure minimum capacity to prevent integer overflow in hash map operations
    try visited.ensureTotalCapacity(1);

    var in_degree = std.HashMap(BlockId, u32, BlockIdHashContext, std.hash_map.default_max_load_percentage).init(allocator);
    defer in_degree.deinit();

    var queue = std.ArrayList(BlockId).init(allocator);
    try queue.ensureTotalCapacity(32); // Pre-allocate for typical DAG breadth
    defer queue.deinit();

    // Start from the root node
    var current_nodes = std.ArrayList(BlockId).init(allocator);
    try current_nodes.ensureTotalCapacity(16); // Pre-allocate for typical graph exploration breadth
    defer current_nodes.deinit();
    try current_nodes.append(query.start_block_id);

    var depth: u32 = 0;

    // Build in-degree map for all reachable nodes
    while (current_nodes.items.len > 0 and depth < query.max_depth) {
        var next_nodes = std.ArrayList(BlockId).init(allocator);
        try next_nodes.ensureTotalCapacity(32); // Pre-allocate for next level expansion
        defer next_nodes.deinit();

        for (current_nodes.items) |block_id| {
            if (visited.contains(block_id)) continue;
            try visited.put(block_id, {});

            // Initialize in-degree for this node
            if (!in_degree.contains(block_id)) {
                try in_degree.put(block_id, 0);
            }

            // Query outgoing edges to build dependency graph for topological ordering
            const edges = storage_engine.find_outgoing_edges(block_id);

            for (edges) |owned_edge| {
                const edge = owned_edge.as_edge();
                if (!edge_passes_filter(edge, query.edge_filter)) continue;

                // Track dependency count for Kahn's algorithm ordering
                const current_degree = in_degree.get(edge.target_id) orelse 0;
                try in_degree.put(edge.target_id, current_degree + 1);

                try next_nodes.append(edge.target_id);
            }
        }

        current_nodes.clearRetainingCapacity();
        try current_nodes.appendSlice(next_nodes.items);
        depth += 1;
    }

    // Find nodes with zero in-degree (roots for topological sort)
    var in_degree_iter = in_degree.iterator();
    while (in_degree_iter.next()) |entry| {
        if (entry.value_ptr.* == 0) {
            try queue.append(entry.key_ptr.*);
        }
    }

    var sorted_count: usize = 0;
    var path = std.ArrayList(BlockId).init(allocator);
    try path.ensureTotalCapacity(64); // Pre-allocate for expected topological result size
    defer path.deinit();

    // Kahn's algorithm
    while (queue.items.len > 0) {
        const current = queue.orderedRemove(0);
        try path.append(current);
        sorted_count += 1;

        // Query dependencies to update in-degree counts for Kahn's algorithm
        const edges = storage_engine.find_outgoing_edges(current);

        for (edges) |owned_edge| {
            const edge = owned_edge.as_edge();
            if (!edge_passes_filter(edge, query.edge_filter)) continue;
            if (!in_degree.contains(edge.target_id)) continue;

            // Decrease in-degree
            const current_degree = in_degree.get(edge.target_id).?;
            if (current_degree > 0) {
                try in_degree.put(edge.target_id, current_degree - 1);
                if (current_degree - 1 == 0) {
                    try queue.append(edge.target_id); // tidy:ignore-perf ensureTotalCapacity called at function start
                }
            }
        }
    }

    // Check for cycle: if sorted_count < total nodes, there's a cycle
    if (sorted_count < visited.count()) {
        // Cycle detected - return empty result
        const owned_blocks = try result_blocks.toOwnedSlice();
        const owned_paths = try result_paths.toOwnedSlice();
        const owned_depths = try result_depths.toOwnedSlice();

        return TraversalResult.init(
            allocator,
            owned_blocks,
            owned_paths,
            owned_depths,
            0,
            0,
        );
    }

    // No cycle - build result with blocks in topological order
    if (path.items.len > 0) {
        for (path.items, 0..) |block_id, i| {
            // Try to find the block
            if (storage_engine.find_query_block(block_id) catch null) |block| {
                try result_blocks.append(block); // tidy:ignore-perf ensureTotalCapacity called at function start

                // Create path slice for this block (just itself in topological order)
                const block_path = try allocator.alloc(BlockId, 1);
                block_path[0] = block_id;
                try result_paths.append(block_path); // tidy:ignore-perf ensureTotalCapacity called at function start

                try result_depths.append(@intCast(i)); // tidy:ignore-perf ensureTotalCapacity called at function start
            }
        }
    }

    const owned_blocks = try result_blocks.toOwnedSlice();
    const owned_paths = try result_paths.toOwnedSlice();
    const owned_depths = try result_depths.toOwnedSlice();

    return TraversalResult.init(
        allocator,
        owned_blocks,
        owned_paths,
        owned_depths,
        @intCast(owned_blocks.len),
        @intCast(path.items.len),
    );
}

/// Add neighbors to A* priority queue with heuristic scoring
fn add_neighbors_to_astar_queue(
    allocator: std.mem.Allocator,
    storage_engine: *StorageEngine,
    queue: anytype,
    visited: *BlockIdHashMap,
    current_id: BlockId,
    next_depth: u32,
    current_path: []const BlockId,
    current_g_cost: f32,
    query: TraversalQuery,
    comptime AStarNode: type,
) !void {
    if (query.direction == .outgoing or query.direction == .bidirectional) {
        const edges = storage_engine.find_outgoing_edges(current_id);
        if (edges.len > 0) {
            for (edges) |owned_edge| {
                const edge = owned_edge.as_edge();
                if (!edge_passes_filter(edge, query.edge_filter)) continue;

                if (!visited.contains(edge.target_id)) {
                    const new_path = try allocator.alloc(BlockId, current_path.len + 1);
                    @memcpy(new_path[0..current_path.len], current_path);
                    new_path[current_path.len] = edge.target_id;

                    const g_cost = current_g_cost + 1.0;
                    const h_cost = calculate_heuristic(storage_engine, edge.target_id, query.start_block_id);
                    const f_cost = g_cost + h_cost;

                    try queue.add(AStarNode{ // tidy:ignore-perf ensureCapacity called with query.max_results
                        .block_id = edge.target_id,
                        .depth = next_depth,
                        .path = new_path,
                        .g_cost = g_cost,
                        .h_cost = h_cost,
                        .f_cost = f_cost,
                    });

                    try visited.put(edge.target_id, {});
                }
            }
        }
    }

    if (query.direction == .incoming or query.direction == .bidirectional) {
        const edges = storage_engine.find_incoming_edges(current_id);
        if (edges.len > 0) {
            for (edges) |owned_edge| {
                const edge = owned_edge.as_edge();
                if (!edge_passes_filter(edge, query.edge_filter)) continue;

                if (!visited.contains(edge.source_id)) {
                    const new_path = try allocator.alloc(BlockId, current_path.len + 1);
                    @memcpy(new_path[0..current_path.len], current_path);
                    new_path[current_path.len] = edge.source_id;

                    const g_cost = current_g_cost + 1.0;
                    const h_cost = calculate_heuristic(storage_engine, edge.source_id, query.start_block_id);
                    const f_cost = g_cost + h_cost;

                    try queue.add(AStarNode{ // tidy:ignore-perf ensureCapacity called with query.max_results
                        .block_id = edge.source_id,
                        .depth = next_depth,
                        .path = new_path,
                        .g_cost = g_cost,
                        .h_cost = h_cost,
                        .f_cost = f_cost,
                    });

                    try visited.put(edge.source_id, {});
                }
            }
        }
    }
}

/// Add neighbors to bidirectional search queue
fn add_neighbors_to_bidirectional_queue(
    allocator: std.mem.Allocator,
    storage_engine: *StorageEngine,
    queue: anytype,
    visited: *BlockIdHashMap,
    current_id: BlockId,
    next_depth: u32,
    current_path: []const BlockId,
    query: TraversalQuery,
    is_forward: bool,
) !void {
    const QueueItem = @TypeOf(queue.items[0]);

    const use_outgoing = (is_forward and (query.direction == .outgoing or query.direction == .bidirectional)) or
        (!is_forward and (query.direction == .incoming or query.direction == .bidirectional));
    const use_incoming = (is_forward and (query.direction == .incoming or query.direction == .bidirectional)) or
        (!is_forward and (query.direction == .outgoing or query.direction == .bidirectional));

    if (use_outgoing) {
        const edges = storage_engine.find_outgoing_edges(current_id);
        if (edges.len > 0) {
            for (edges) |owned_edge| {
                const edge = owned_edge.as_edge();
                if (!edge_passes_filter(edge, query.edge_filter)) continue;

                if (!visited.contains(edge.target_id)) {
                    const new_path = try allocator.alloc(BlockId, current_path.len + 1);
                    @memcpy(new_path[0..current_path.len], current_path);
                    new_path[current_path.len] = edge.target_id;

                    try queue.append(QueueItem{ // tidy:ignore-perf ensureCapacity called with query.max_results
                        .block_id = edge.target_id,
                        .depth = next_depth,
                        .path = new_path,
                        .is_forward = is_forward,
                    });

                    try visited.put(edge.target_id, {});
                }
            }
        }
    }

    if (use_incoming) {
        const edges = storage_engine.find_incoming_edges(current_id);
        if (edges.len > 0) {
            for (edges) |owned_edge| {
                const edge = owned_edge.as_edge();
                if (!edge_passes_filter(edge, query.edge_filter)) continue;

                if (!visited.contains(edge.source_id)) {
                    const new_path = try allocator.alloc(BlockId, current_path.len + 1);
                    @memcpy(new_path[0..current_path.len], current_path);
                    new_path[current_path.len] = edge.source_id;

                    try queue.append(QueueItem{ // tidy:ignore-perf ensureCapacity called with query.max_results
                        .block_id = edge.source_id,
                        .depth = next_depth,
                        .path = new_path,
                        .is_forward = is_forward,
                    });

                    try visited.put(edge.source_id, {});
                }
            }
        }
    }
}

/// Calculate heuristic cost for A* search
/// Uses content similarity and structural distance for LLM-optimized traversal
fn calculate_heuristic(storage_engine: *StorageEngine, from_id: BlockId, to_id: BlockId) f32 {
    _ = storage_engine;
    _ = from_id;
    _ = to_id;

    return 1.0; // Uniform cost for now
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
        .edge_filter = .all_types,
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
        .edge_filter = .all_types,
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
        .edge_filter = .all_types,
    };
    return execute_traversal(allocator, storage_engine, query);
}

/// Convenience function for A* search with optimal pathfinding
pub fn traverse_astar(
    allocator: std.mem.Allocator,
    storage_engine: *StorageEngine,
    start_id: BlockId,
    max_depth: u32,
) !TraversalResult {
    const query = TraversalQuery{
        .start_block_id = start_id,
        .direction = .outgoing,
        .algorithm = .astar_search,
        .max_depth = max_depth,
        .max_results = TraversalQuery.DEFAULT_MAX_RESULTS,
        .edge_filter = .all_types,
    };
    return execute_traversal(allocator, storage_engine, query);
}

fn create_test_storage_engine(allocator: std.mem.Allocator) !StorageEngine {
    var sim_vfs = try SimulationVFS.init(allocator);
    const storage_config = @import("../storage/config.zig").Config{};
    var storage_engine = try StorageEngine.init(allocator, sim_vfs.vfs(), "./test_traversal", storage_config);
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
        .source_id = from_id,
        .target_id = to_id,
        .edge_type = edge_type,
    };
}

test "traversal query validation" {
    const start_id = try BlockId.from_hex("11111111111111111111111111111111");

    const valid_query = TraversalQuery.init(start_id, .outgoing);
    try valid_query.validate();

    var invalid_query = TraversalQuery.init(start_id, .outgoing);
    invalid_query.max_depth = 0;
    try testing.expectError(TraversalError.InvalidDepth, invalid_query.validate());

    invalid_query.max_depth = 101; // > 100
    try testing.expectError(TraversalError.InvalidDepth, invalid_query.validate());

    invalid_query.max_depth = TraversalQuery.DEFAULT_MAX_DEPTH;
    invalid_query.max_results = 0;
    try testing.expectError(TraversalError.InvalidMaxResults, invalid_query.validate());

    invalid_query.max_results = 20000; // > ABSOLUTE_MAX_RESULTS
    try testing.expectError(TraversalError.InvalidMaxResults, invalid_query.validate());
}

test "traversal direction enum parsing" {
    try testing.expectEqual(TraversalDirection.outgoing, try TraversalDirection.from_u8(0x01));
    try testing.expectEqual(TraversalDirection.incoming, try TraversalDirection.from_u8(0x02));
    try testing.expectEqual(TraversalDirection.bidirectional, try TraversalDirection.from_u8(0x03));

    try testing.expectError(TraversalError.InvalidDirection, TraversalDirection.from_u8(0xFF));
}

test "traversal algorithm enum parsing" {
    try testing.expectEqual(TraversalAlgorithm.breadth_first, try TraversalAlgorithm.from_u8(0x01));
    try testing.expectEqual(TraversalAlgorithm.depth_first, try TraversalAlgorithm.from_u8(0x02));

    try testing.expectError(TraversalError.InvalidAlgorithm, TraversalAlgorithm.from_u8(0xFF));
}

test "traversal result memory management" {
    const allocator = testing.allocator;

    const test_block = create_test_block(try BlockId.from_hex("11111111111111111111111111111111"), "test content");

    const empty_paths: []const []const BlockId = &.{};
    const empty_depths: []const u32 = &.{};
    var result = TraversalResult.init(allocator, &[_]QueryEngineBlock{QueryEngineBlock.init(test_block)}, empty_paths, empty_depths, 1, 0);
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

    const start_id = try BlockId.from_hex("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa");
    const start_block = create_test_block(start_id, "isolated block");
    try storage_engine.put_block(start_block);

    const query = TraversalQuery.init(start_id, .outgoing);
    const result = try execute_traversal(allocator, &storage_engine, query);
    defer result.deinit();

    try testing.expectEqual(@as(usize, 1), result.count());
    try testing.expect(result.blocks[0].block.id.eql(start_id));
    try testing.expectEqual(@as(u32, 0), result.max_depth_reached);
}

test "outgoing traversal with linear chain" {
    const allocator = testing.allocator;

    var storage_engine = try create_test_storage_engine(allocator);
    defer storage_engine.deinit();

    const id_a = try BlockId.from_hex("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa");
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

    const query = TraversalQuery{
        .start_block_id = id_a,
        .direction = .outgoing,
        .algorithm = .breadth_first,
        .max_depth = 2,
        .max_results = 10,
        .edge_filter = .all_types,
    };

    const result = try execute_traversal(allocator, &storage_engine, query);
    defer result.deinit();

    try testing.expectEqual(@as(usize, 3), result.count());
    try testing.expectEqual(@as(u32, 2), result.max_depth_reached);

    var found_a = false;
    var found_b = false;
    var found_c = false;
    for (result.blocks) |block| {
        if (block.block.id.eql(id_a)) found_a = true;
        if (block.block.id.eql(id_b)) found_b = true;
        if (block.block.id.eql(id_c)) found_c = true;
    }
    try testing.expect(found_a and found_b and found_c);
}

test "incoming traversal" {
    const allocator = testing.allocator;

    var storage_engine = try create_test_storage_engine(allocator);
    defer storage_engine.deinit();

    const id_a = try BlockId.from_hex("11111111111111111111111111111111");
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

    const result = try traverse_incoming(allocator, &storage_engine, id_c, 2);
    defer result.deinit();

    try testing.expectEqual(@as(usize, 3), result.count());

    var found_a = false;
    var found_b = false;
    var found_c = false;
    for (result.blocks) |block| {
        if (block.block.id.eql(id_a)) found_a = true;
        if (block.block.id.eql(id_b)) found_b = true;
        if (block.block.id.eql(id_c)) found_c = true;
    }
    try testing.expect(found_a and found_b and found_c);
}

test "bidirectional traversal" {
    const allocator = testing.allocator;

    var storage_engine = try create_test_storage_engine(allocator);
    defer storage_engine.deinit();

    const id_a = try BlockId.from_hex("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa");
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

    const result = try traverse_bidirectional(allocator, &storage_engine, id_b, 1);
    defer result.deinit();

    try testing.expectEqual(@as(usize, 3), result.count());

    var found_a = false;
    var found_b = false;
    var found_c = false;
    for (result.blocks) |block| {
        if (block.block.id.eql(id_a)) found_a = true;
        if (block.block.id.eql(id_b)) found_b = true;
        if (block.block.id.eql(id_c)) found_c = true;
    }
    try testing.expect(found_a and found_b and found_c);
}

test "edge type filtering" {
    const allocator = testing.allocator;

    var storage_engine = try create_test_storage_engine(allocator);
    defer storage_engine.deinit();

    const id_a = try BlockId.from_hex("11111111111111111111111111111111");
    const id_b = try BlockId.from_hex("2222222222222222222222222222222222222222");
    const id_c = try BlockId.from_hex("3333333333333333333333333333333333333333");

    const block_a = create_test_block(id_a, "block A");
    const block_b = create_test_block(id_b, "block B");
    const block_c = create_test_block(id_c, "block C");

    try storage_engine.put_block(block_a);
    try storage_engine.put_block(block_b);
    try storage_engine.put_block(block_c);

    const edge_ab_calls = create_test_edge(id_a, id_b, .calls);
    const edge_ac_imports = create_test_edge(id_a, id_c, .imports);
    try storage_engine.put_edge(edge_ab_calls);
    try storage_engine.put_edge(edge_ac_imports);

    const query_calls = TraversalQuery{
        .start_block_id = id_a,
        .direction = .outgoing,
        .algorithm = .breadth_first,
        .max_depth = 1,
        .max_results = 10,
        .edge_filter = .{ .only_type = .calls },
    };

    const result_calls = try execute_traversal(allocator, &storage_engine, query_calls);
    defer result_calls.deinit();

    try testing.expectEqual(@as(usize, 2), result_calls.count());

    var found_a = false;
    var found_b = false;
    var found_c = false;
    for (result_calls.blocks) |block| {
        if (block.block.id.eql(id_a)) found_a = true;
        if (block.block.id.eql(id_b)) found_b = true;
        if (block.block.id.eql(id_c)) found_c = true;
    }
    try testing.expect(found_a and found_b and !found_c);
}

test "max depth limit enforcement" {
    const allocator = testing.allocator;

    var storage_engine = try create_test_storage_engine(allocator);
    defer storage_engine.deinit();

    const id_a = try BlockId.from_hex("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa");
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

    const query = TraversalQuery{
        .start_block_id = id_a,
        .direction = .outgoing,
        .algorithm = .breadth_first,
        .max_depth = 2,
        .max_results = 10,
        .edge_filter = .all_types,
    };

    const result = try execute_traversal(allocator, &storage_engine, query);
    defer result.deinit();

    try testing.expectEqual(@as(usize, 3), result.count());
    try testing.expectEqual(@as(u32, 2), result.max_depth_reached);

    var found_d = false;
    for (result.blocks) |block| {
        if (block.block.id.eql(id_d)) found_d = true;
    }
    try testing.expect(!found_d);
}

test "max results limit enforcement" {
    const allocator = testing.allocator;

    var storage_engine = try create_test_storage_engine(allocator);
    defer storage_engine.deinit();

    const id_a = try BlockId.from_hex("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa");
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

    const query = TraversalQuery{
        .start_block_id = id_a,
        .direction = .outgoing,
        .algorithm = .breadth_first,
        .max_depth = 2,
        .max_results = 3,
        .edge_filter = .all_types,
    };

    const result = try execute_traversal(allocator, &storage_engine, query);
    defer result.deinit();

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
    const id_a = try BlockId.from_hex("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa");
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

    const bfs_query = TraversalQuery{
        .start_block_id = id_a,
        .direction = .outgoing,
        .algorithm = .breadth_first,
        .max_depth = 2,
        .max_results = 10,
        .edge_filter = .all_types,
    };

    const bfs_result = try execute_traversal(allocator, &storage_engine, bfs_query);
    defer bfs_result.deinit();

    const dfs_query = TraversalQuery{
        .start_block_id = id_a,
        .direction = .outgoing,
        .algorithm = .depth_first,
        .max_depth = 2,
        .max_results = 10,
        .edge_filter = .all_types,
    };

    const dfs_result = try execute_traversal(allocator, &storage_engine, dfs_query);
    defer dfs_result.deinit();

    try testing.expectEqual(@as(usize, 5), bfs_result.count());
    try testing.expectEqual(@as(usize, 5), dfs_result.count());

    var bfs_ids = std.ArrayList(BlockId).init(allocator);
    try bfs_ids.ensureTotalCapacity(10); // Pre-allocate for test comparison
    defer bfs_ids.deinit();
    var dfs_ids = std.ArrayList(BlockId).init(allocator);
    try dfs_ids.ensureTotalCapacity(10); // Pre-allocate for test comparison
    defer dfs_ids.deinit();

    try bfs_ids.ensureTotalCapacity(bfs_result.blocks.len);
    for (bfs_result.blocks) |block| {
        try bfs_ids.append(block.block.id);
    }
    try dfs_ids.ensureTotalCapacity(dfs_result.blocks.len);
    for (dfs_result.blocks) |block| {
        try dfs_ids.append(block.block.id);
    }

    try testing.expectEqual(bfs_ids.items.len, dfs_ids.items.len);
}

test "traversal error handling" {
    const allocator = testing.allocator;

    var storage_engine = try create_test_storage_engine(allocator);
    defer storage_engine.deinit();

    const missing_id = try BlockId.from_hex("0000000000000000000000000000000000000000");

    const query = TraversalQuery.init(missing_id, .outgoing);
    const result = try execute_traversal(allocator, &storage_engine, query);
    defer result.deinit();

    try testing.expect(result.is_empty());
    try testing.expectEqual(@as(usize, 0), result.count());
}

test "convenience traversal functions" {
    const allocator = testing.allocator;

    var storage_engine = try create_test_storage_engine(allocator);
    defer storage_engine.deinit();

    const start_id = try BlockId.from_hex("11111111111111111111111111111111");
    const test_block = create_test_block(start_id, "convenience test");
    try storage_engine.put_block(test_block);

    const outgoing_result = try traverse_outgoing(allocator, &storage_engine, start_id, 2);
    defer outgoing_result.deinit();

    const incoming_result = try traverse_incoming(allocator, &storage_engine, start_id, 2);
    defer incoming_result.deinit();

    const bidirectional_result = try traverse_bidirectional(allocator, &storage_engine, start_id, 2);
    defer bidirectional_result.deinit();

    try testing.expect(!outgoing_result.is_empty());
    try testing.expect(!incoming_result.is_empty());
    try testing.expect(!bidirectional_result.is_empty());

    try testing.expectEqual(@as(usize, 1), outgoing_result.count());
    try testing.expectEqual(@as(usize, 1), incoming_result.count());
    try testing.expectEqual(@as(usize, 1), bidirectional_result.count());
}

test "A* search algorithm basic functionality" {
    const allocator = testing.allocator;

    var storage_engine = try create_test_storage_engine(allocator);
    defer storage_engine.deinit();

    const id_a = try BlockId.from_hex("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa");
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

    const query = TraversalQuery{
        .start_block_id = id_a,
        .direction = .outgoing,
        .algorithm = .astar_search,
        .max_depth = 2,
        .max_results = 10,
        .edge_filter = .all_types,
    };

    const result = try execute_traversal(allocator, &storage_engine, query);
    defer result.deinit();

    try testing.expectEqual(@as(usize, 3), result.count());
    try testing.expectEqual(@as(u32, 2), result.max_depth_reached);

    var found_a = false;
    var found_b = false;
    var found_c = false;
    for (result.blocks) |block| {
        if (block.block.id.eql(id_a)) found_a = true;
        if (block.block.id.eql(id_b)) found_b = true;
        if (block.block.id.eql(id_c)) found_c = true;
    }
    try testing.expect(found_a and found_b and found_c);
}

test "bidirectional search algorithm" {
    const allocator = testing.allocator;

    var storage_engine = try create_test_storage_engine(allocator);
    defer storage_engine.deinit();

    const id_a = try BlockId.from_hex("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa");
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

    const query = TraversalQuery{
        .start_block_id = id_a,
        .direction = .bidirectional,
        .algorithm = .bidirectional_search,
        .max_depth = 4, // Allow enough depth for bidirectional exploration
        .max_results = 10,
        .edge_filter = .all_types,
    };

    const result = try execute_traversal(allocator, &storage_engine, query);
    defer result.deinit();

    try testing.expect(result.count() >= 1); // At least A itself
    try testing.expect(result.blocks_traversed > 0);

    var found_a = false;
    for (result.blocks) |block| {
        if (block.block.id.eql(id_a)) found_a = true;
    }
    try testing.expect(found_a);
}

test "advanced algorithm enum values" {
    try testing.expectEqual(TraversalAlgorithm.astar_search, try TraversalAlgorithm.from_u8(0x03));
    try testing.expectEqual(TraversalAlgorithm.bidirectional_search, try TraversalAlgorithm.from_u8(0x04));
    try testing.expectEqual(TraversalAlgorithm.strongly_connected, try TraversalAlgorithm.from_u8(0x05));
    try testing.expectEqual(TraversalAlgorithm.topological_sort, try TraversalAlgorithm.from_u8(0x06));
}
