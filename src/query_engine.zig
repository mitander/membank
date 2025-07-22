//! Query engine for CortexDB context retrieval.
//!
//! Provides semantic and structural query processing with deterministic
//! behavior for testing and production environments. Supports direct block
//! retrieval by ID with future support for graph traversal and metadata filtering.

const std = @import("std");
const assert = std.debug.assert;
const storage = @import("storage");
const context_block = @import("context_block");

const StorageEngine = storage.StorageEngine;
const ContextBlock = context_block.ContextBlock;
const GraphEdge = context_block.GraphEdge;
const BlockId = context_block.BlockId;

/// String-to-frequency mapping for metadata analysis
const MetadataAnalysisMap = std.StringHashMap(u32);

/// Query engine errors.
const QueryError = error{
    /// Invalid query command
    InvalidCommand,
    /// Block not found
    BlockNotFound,
    /// Empty query
    EmptyQuery,
    /// Too many results requested
    TooManyResults,
    /// Query engine not initialized
    NotInitialized,
} || std.mem.Allocator.Error || storage.StorageError;

/// Query command types.
const QueryCommand = enum(u8) {
    get_blocks = 0x01,
    traverse = 0x02,
    filter = 0x03,

    pub fn from_u8(value: u8) !QueryCommand {
        return std.meta.intToEnum(QueryCommand, value) catch error.InvalidCommand;
    }
};

/// Query for retrieving blocks by their IDs.
pub const GetBlocksQuery = struct {
    /// List of block IDs to retrieve
    block_ids: []const BlockId,

    /// Maximum number of blocks to return
    const MAX_BLOCKS = 1000;

    pub fn validate(self: GetBlocksQuery) !void {
        if (self.block_ids.len == 0) return QueryError.EmptyQuery;
        if (self.block_ids.len > MAX_BLOCKS) return QueryError.TooManyResults;
    }
};

/// Enhanced filtering condition for Query Engine V2.
pub const FilterCondition = union(enum) {
    /// Match blocks where metadata field equals value
    metadata_equals: struct {
        field: []const u8,
        value: []const u8,
    },
    /// Match blocks where metadata field contains substring
    metadata_contains: struct {
        field: []const u8,
        substring: []const u8,
    },
    /// Match blocks where content contains substring (case insensitive)
    content_contains: struct {
        substring: []const u8,
    },
    /// Match blocks from specific source URI pattern
    source_matches: struct {
        pattern: []const u8,
    },
    /// Match blocks with version in range
    version_range: struct {
        min_version: u64,
        max_version: u64,
    },
    /// Logical AND of multiple conditions
    and_conditions: struct {
        conditions: []const FilterCondition,
    },
    /// Logical OR of multiple conditions
    or_conditions: struct {
        conditions: []const FilterCondition,
    },
    /// Logical NOT of a condition
    not_condition: struct {
        condition: *const FilterCondition,
    },

    /// Check if a context block matches this filter condition.
    pub fn matches(self: FilterCondition, block: ContextBlock, allocator: std.mem.Allocator) !bool {
        return switch (self) {
            .metadata_equals => |cond| {
                return try match_metadata_equals(
                    block.metadata_json,
                    cond.field,
                    cond.value,
                    allocator,
                );
            },
            .metadata_contains => |cond| {
                return try match_metadata_contains(
                    block.metadata_json,
                    cond.field,
                    cond.substring,
                    allocator,
                );
            },
            .content_contains => |cond| {
                return match_content_contains(block.content, cond.substring);
            },
            .source_matches => |cond| {
                return match_source_pattern(block.source_uri, cond.pattern);
            },
            .version_range => |cond| {
                return block.version >= cond.min_version and block.version <= cond.max_version;
            },
            .and_conditions => |cond| {
                for (cond.conditions) |sub_condition| {
                    if (!try sub_condition.matches(block, allocator)) return false;
                }
                return true;
            },
            .or_conditions => |cond| {
                for (cond.conditions) |sub_condition| {
                    if (try sub_condition.matches(block, allocator)) return true;
                }
                return false;
            },
            .not_condition => |cond| {
                return !try cond.condition.matches(block, allocator);
            },
        };
    }
};

/// Enhanced query for filtering blocks by complex criteria.
pub const FilteredQuery = struct {
    /// Filter condition to apply
    filter: FilterCondition,
    /// Maximum number of results to return
    max_results: u32,
    /// Sort results by version (ascending/descending)
    sort_by_version: ?enum { ascending, descending },
    /// Include metadata in results for analysis
    include_metadata_analysis: bool,

    /// Default maximum results
    const DEFAULT_MAX_RESULTS = 1000;
    /// Absolute maximum results to prevent memory exhaustion
    const ABSOLUTE_MAX_RESULTS = 10000;

    /// Create a basic filtered query.
    pub fn init(filter: FilterCondition) FilteredQuery {
        return FilteredQuery{
            .filter = filter,
            .max_results = DEFAULT_MAX_RESULTS,
            .sort_by_version = null,
            .include_metadata_analysis = false,
        };
    }

    /// Validate filtered query parameters.
    pub fn validate(self: FilteredQuery) !void {
        if (self.max_results == 0) return QueryError.EmptyQuery;
        if (self.max_results > ABSOLUTE_MAX_RESULTS) return QueryError.TooManyResults;
    }
};

/// Semantic search interface for future external embedding integration.
pub const SemanticQuery = struct {
    /// Natural language query string
    query_text: []const u8,
    /// Similarity threshold for results (0.0 to 1.0)
    similarity_threshold: f32,
    /// Maximum number of results
    max_results: u32,
    /// Optional metadata filter to apply before semantic search
    metadata_filter: ?FilterCondition,

    /// Default similarity threshold
    const DEFAULT_THRESHOLD: f32 = 0.7;
    /// Default maximum results
    const DEFAULT_MAX_RESULTS = 100;

    /// Create a semantic query.
    pub fn init(query_text: []const u8) SemanticQuery {
        return SemanticQuery{
            .query_text = query_text,
            .similarity_threshold = DEFAULT_THRESHOLD,
            .max_results = DEFAULT_MAX_RESULTS,
            .metadata_filter = null,
        };
    }

    /// Validate semantic query parameters.
    pub fn validate(self: SemanticQuery) !void {
        if (self.query_text.len == 0) return QueryError.EmptyQuery;
        if (self.similarity_threshold < 0.0 or self.similarity_threshold > 1.0) {
            return QueryError.InvalidCommand;
        }
        if (self.max_results == 0) return QueryError.EmptyQuery;
        if (self.max_results > FilteredQuery.ABSOLUTE_MAX_RESULTS) return QueryError.TooManyResults;
    }
};

/// Traversal direction for graph queries.
pub const TraversalDirection = enum(u8) {
    /// Follow outgoing edges (from source to targets)
    outgoing = 0x01,
    /// Follow incoming edges (from targets to sources)
    incoming = 0x02,
    /// Follow both outgoing and incoming edges
    bidirectional = 0x03,

    pub fn from_u8(value: u8) !TraversalDirection {
        return std.meta.intToEnum(TraversalDirection, value) catch error.InvalidCommand;
    }
};

/// Traversal algorithm type.
pub const TraversalAlgorithm = enum(u8) {
    /// Breadth-first search (explores by depth level)
    breadth_first = 0x01,
    /// Depth-first search (explores deeply before backtracking)
    depth_first = 0x02,

    pub fn from_u8(value: u8) !TraversalAlgorithm {
        return std.meta.intToEnum(TraversalAlgorithm, value) catch error.InvalidCommand;
    }
};

/// Query for traversing the knowledge graph.
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
    edge_type_filter: ?context_block.EdgeType,

    /// Default maximum depth for traversal
    const DEFAULT_MAX_DEPTH = 10;
    /// Default maximum results
    const DEFAULT_MAX_RESULTS = 1000;
    /// Absolute maximum results to prevent memory exhaustion
    const ABSOLUTE_MAX_RESULTS = 10000;

    /// Create a basic traversal query with defaults.
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

    /// Validate traversal query parameters.
    pub fn validate(self: TraversalQuery) !void {
        if (self.max_results == 0) return QueryError.EmptyQuery;
        if (self.max_results > ABSOLUTE_MAX_RESULTS) return QueryError.TooManyResults;
    }
};

/// Result from graph traversal containing blocks and path information.
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

    /// Create traversal result.
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

    /// Free allocated memory for traversal results.
    pub fn deinit(self: TraversalResult) void {
        // Free blocks
        for (self.blocks) |block| {
            block.deinit(self.allocator);
        }
        self.allocator.free(self.blocks);

        // Free paths
        for (self.paths) |path| {
            self.allocator.free(path);
        }
        self.allocator.free(self.paths);

        // Free depths array
        self.allocator.free(self.depths);
    }

    /// Format traversal result for LLM consumption with path information.
    pub fn format_for_llm(self: TraversalResult, allocator: std.mem.Allocator) ![]u8 {
        var result = std.ArrayList(u8).init(allocator);
        defer result.deinit();

        try result.writer().print("=== GRAPH TRAVERSAL RESULT ===\n");
        try result.writer().print("Blocks found: {}\n", .{self.blocks.len});
        try result.writer().print("Blocks traversed: {}\n", .{self.blocks_traversed});
        try result.writer().print("Max depth reached: {}\n\n", .{self.max_depth_reached});

        for (self.blocks, 0..) |block, i| {
            try result.appendSlice("--- BEGIN TRAVERSAL BLOCK ---\n");

            // Write block ID as hex
            const id_hex = try block.id.to_hex(allocator);
            defer allocator.free(id_hex);
            try result.writer().print("ID: {s}\n", .{id_hex});

            // Write depth and path
            try result.writer().print("Depth: {}\n", .{self.depths[i]});

            try result.appendSlice("Path: ");
            for (self.paths[i], 0..) |path_block_id, j| {
                const path_id_hex = try path_block_id.to_hex(allocator);
                defer allocator.free(path_id_hex);
                if (j > 0) try result.appendSlice(" -> ");
                try result.appendSlice(path_id_hex);
            }
            try result.appendSlice("\n");

            // Write source URI
            try result.writer().print("Source: {s}\n", .{block.source_uri});

            // Write version
            try result.writer().print("Version: {}\n", .{block.version});

            // Write metadata
            try result.writer().print("Metadata: {s}\n", .{block.metadata_json});

            // Write content
            try result.appendSlice(block.content);
            try result.appendSlice("\n--- END TRAVERSAL BLOCK ---\n\n");
        }

        return result.toOwnedSlice();
    }
};

/// Query result containing retrieved blocks.
pub const QueryResult = struct {
    /// Retrieved context blocks
    blocks: []const ContextBlock,
    /// Total number of blocks found
    count: u32,
    /// Allocator used for result memory
    allocator: std.mem.Allocator,

    /// Create query result from blocks.
    pub fn init(allocator: std.mem.Allocator, blocks: []const ContextBlock) QueryResult {
        return QueryResult{
            .blocks = blocks,
            .count = @intCast(blocks.len),
            .allocator = allocator,
        };
    }

    /// Free allocated memory for query results.
    pub fn deinit(self: QueryResult) void {
        for (self.blocks) |block| {
            block.deinit(self.allocator);
        }
        self.allocator.free(self.blocks);
    }

    /// Format result as structured text payload for LLM consumption.
    /// Uses clear separators that an LLM can parse to understand block boundaries.
    pub fn format_for_llm(self: QueryResult, allocator: std.mem.Allocator) ![]u8 {
        var result = std.ArrayList(u8).init(allocator);
        defer result.deinit();

        for (self.blocks) |block| {
            try result.appendSlice("--- BEGIN CONTEXT BLOCK ---\n");

            // Write block ID as hex
            const id_hex = try block.id.to_hex(allocator);
            defer allocator.free(id_hex);
            try result.writer().print("ID: {s}\n", .{id_hex});

            // Write source URI
            try result.writer().print("Source: {s}\n", .{block.source_uri});

            // Write version
            try result.writer().print("Version: {}\n", .{block.version});

            // Write metadata
            try result.writer().print("Metadata: {s}\n", .{block.metadata_json});

            // Write content
            try result.appendSlice(block.content);
            try result.appendSlice("\n--- END CONTEXT BLOCK ---\n\n");
        }

        return result.toOwnedSlice();
    }
};

/// Enhanced query result with metadata analysis for Query Engine V2.
pub const FilteredQueryResult = struct {
    /// Retrieved context blocks
    blocks: []const ContextBlock,
    /// Total number of blocks found
    count: u32,
    /// Metadata field analysis (field -> frequency)
    metadata_analysis: ?MetadataAnalysisMap,
    /// Query execution statistics
    blocks_scanned: u32,
    blocks_matched: u32,
    execution_time_ns: u64,
    /// Allocator used for result memory
    allocator: std.mem.Allocator,

    /// Create filtered query result.
    pub fn init(
        allocator: std.mem.Allocator,
        blocks: []const ContextBlock,
        metadata_analysis: ?MetadataAnalysisMap,
        blocks_scanned: u32,
        blocks_matched: u32,
        execution_time_ns: u64,
    ) FilteredQueryResult {
        return FilteredQueryResult{
            .blocks = blocks,
            .count = @intCast(blocks.len),
            .metadata_analysis = metadata_analysis,
            .blocks_scanned = blocks_scanned,
            .blocks_matched = blocks_matched,
            .execution_time_ns = execution_time_ns,
            .allocator = allocator,
        };
    }

    /// Free allocated memory for filtered query results.
    pub fn deinit(self: *FilteredQueryResult) void {
        for (self.blocks) |block| {
            block.deinit(self.allocator);
        }
        self.allocator.free(self.blocks);

        if (self.metadata_analysis) |*analysis| {
            var iterator = analysis.iterator();
            while (iterator.next()) |entry| {
                self.allocator.free(entry.key_ptr.*);
            }
            analysis.deinit();
        }
    }

    /// Format enhanced result with metadata analysis for LLM consumption.
    pub fn format_for_llm(self: FilteredQueryResult, allocator: std.mem.Allocator) ![]u8 {
        var result = std.ArrayList(u8).init(allocator);
        defer result.deinit();

        try result.writer().print("=== FILTERED QUERY RESULT ===\n");
        try result.writer().print(
            "Blocks matched: {} / {} scanned\n",
            .{ self.blocks_matched, self.blocks_scanned },
        );
        try result.writer().print(
            "Execution time: {} ns ({d:.2} ms)\n",
            .{
                self.execution_time_ns,
                @as(f64, @floatFromInt(self.execution_time_ns)) / 1_000_000.0,
            },
        );

        // Include metadata analysis if available
        if (self.metadata_analysis) |analysis| {
            try result.appendSlice("\n--- METADATA ANALYSIS ---\n");
            var iterator = analysis.iterator();
            while (iterator.next()) |entry| {
                try result.writer().print(
                    "  {s}: {} occurrences\n",
                    .{ entry.key_ptr.*, entry.value_ptr.* },
                );
            }
        }

        try result.appendSlice("\n");

        for (self.blocks) |block| {
            try result.appendSlice("--- BEGIN FILTERED BLOCK ---\n");

            // Write block ID as hex
            const id_hex = try block.id.to_hex(allocator);
            defer allocator.free(id_hex);
            try result.writer().print("ID: {s}\n", .{id_hex});

            // Write source URI
            try result.writer().print("Source: {s}\n", .{block.source_uri});

            // Write version
            try result.writer().print("Version: {}\n", .{block.version});

            // Write metadata with enhanced formatting
            try result.writer().print("Metadata: {s}\n", .{block.metadata_json});

            // Write content
            try result.appendSlice(block.content);
            try result.appendSlice("\n--- END FILTERED BLOCK ---\n\n");
        }

        return result.toOwnedSlice();
    }
};

/// Semantic search result with similarity scores.
pub const SemanticQueryResult = struct {
    /// Retrieved context blocks with similarity scores
    results: []const struct {
        block: ContextBlock,
        similarity_score: f32,
    },
    /// Total number of blocks found
    count: u32,
    /// Query execution statistics
    blocks_scanned: u32,
    execution_time_ns: u64,
    /// Average similarity score
    avg_similarity: f32,
    /// Allocator used for result memory
    allocator: std.mem.Allocator,

    /// Create semantic query result.
    pub fn init(
        allocator: std.mem.Allocator,
        results: []const struct { block: ContextBlock, similarity_score: f32 },
        blocks_scanned: u32,
        execution_time_ns: u64,
    ) SemanticQueryResult {
        var total_similarity: f32 = 0;
        for (results) |result| {
            total_similarity += result.similarity_score;
        }
        const avg_similarity = if (results.len > 0)
            total_similarity / @as(f32, @floatFromInt(results.len))
        else
            0;

        return SemanticQueryResult{
            .results = results,
            .count = @intCast(results.len),
            .blocks_scanned = blocks_scanned,
            .execution_time_ns = execution_time_ns,
            .avg_similarity = avg_similarity,
            .allocator = allocator,
        };
    }

    /// Free allocated memory for semantic query results.
    pub fn deinit(self: SemanticQueryResult) void {
        for (self.results) |result| {
            result.block.deinit(self.allocator);
        }
        self.allocator.free(self.results);
    }

    /// Format semantic result with similarity scores for LLM consumption.
    pub fn format_for_llm(self: SemanticQueryResult, allocator: std.mem.Allocator) ![]u8 {
        var result = std.ArrayList(u8).init(allocator);
        defer result.deinit();

        try result.writer().print("=== SEMANTIC SEARCH RESULT ===\n");
        try result.writer().print("Blocks found: {} / {} scanned\n", .{ self.count, self.blocks_scanned });
        try result.writer().print("Execution time: {} ns ({d:.2} ms)\n", .{ self.execution_time_ns, @as(f64, @floatFromInt(self.execution_time_ns)) / 1_000_000.0 });
        try result.writer().print("Average similarity: {d:.3}\n\n", .{self.avg_similarity});

        for (self.results) |semantic_result| {
            try result.appendSlice("--- BEGIN SEMANTIC BLOCK ---\n");

            // Write similarity score
            try result.writer().print("Similarity: {d:.3}\n", .{semantic_result.similarity_score});

            const block = semantic_result.block;

            // Write block ID as hex
            const id_hex = try block.id.to_hex(allocator);
            defer allocator.free(id_hex);
            try result.writer().print("ID: {s}\n", .{id_hex});

            // Write source URI
            try result.writer().print("Source: {s}\n", .{block.source_uri});

            // Write version
            try result.writer().print("Version: {}\n", .{block.version});

            // Write metadata
            try result.writer().print("Metadata: {s}\n", .{block.metadata_json});

            // Write content
            try result.appendSlice(block.content);
            try result.appendSlice("\n--- END SEMANTIC BLOCK ---\n\n");
        }

        return result.toOwnedSlice();
    }
};

/// Query execution engine.
pub const QueryEngine = struct {
    allocator: std.mem.Allocator,
    storage_engine: *StorageEngine,
    initialized: bool,
    // Query metrics (atomic for thread safety)
    queries_executed: std.atomic.Value(u64),
    get_blocks_queries: std.atomic.Value(u64),
    traversal_queries: std.atomic.Value(u64),
    filtered_queries: std.atomic.Value(u64),
    semantic_queries: std.atomic.Value(u64),
    total_query_time_ns: std.atomic.Value(u64),

    /// Initialize query engine with storage backend.
    pub fn init(allocator: std.mem.Allocator, storage_engine: *StorageEngine) QueryEngine {
        return QueryEngine{
            .allocator = allocator,
            .storage_engine = storage_engine,
            .initialized = true,
            .queries_executed = std.atomic.Value(u64).init(0),
            .get_blocks_queries = std.atomic.Value(u64).init(0),
            .traversal_queries = std.atomic.Value(u64).init(0),
            .filtered_queries = std.atomic.Value(u64).init(0),
            .semantic_queries = std.atomic.Value(u64).init(0),
            .total_query_time_ns = std.atomic.Value(u64).init(0),
        };
    }

    /// Clean up query engine resources.
    pub fn deinit(self: *QueryEngine) void {
        self.initialized = false;
    }

    /// Execute a GetBlocks query to retrieve blocks by ID.
    /// Time complexity: O(n) where n is the number of requested blocks.
    /// Space complexity: O(m) where m is the total size of retrieved blocks.
    pub fn execute_get_blocks(self: *QueryEngine, query: GetBlocksQuery) !QueryResult {
        const start_time = std.time.nanoTimestamp();
        defer {
            const duration = @as(u64, @intCast(std.time.nanoTimestamp() - start_time));
            _ = self.queries_executed.fetchAdd(1, .monotonic);
            _ = self.get_blocks_queries.fetchAdd(1, .monotonic);
            _ = self.total_query_time_ns.fetchAdd(duration, .monotonic);
        }

        assert(self.initialized);
        if (!self.initialized) return QueryError.NotInitialized;

        try query.validate();

        var results = std.ArrayList(ContextBlock).init(self.allocator);
        defer results.deinit();

        // Retrieve each requested block
        for (query.block_ids) |block_id| {
            const maybe_block = self.storage_engine.find_block_by_id(block_id) catch |err|
                switch (err) {
                    storage.StorageError.BlockNotFound => continue, // Skip missing blocks
                    else => return err,
                };

            const cloned_block = ContextBlock{
                .id = maybe_block.id,
                .version = maybe_block.version,
                .source_uri = try self.allocator.dupe(u8, maybe_block.source_uri),
                .metadata_json = try self.allocator.dupe(u8, maybe_block.metadata_json),
                .content = try self.allocator.dupe(u8, maybe_block.content),
            };

            try results.append(cloned_block);
        }

        const owned_blocks = try results.toOwnedSlice();
        return QueryResult.init(self.allocator, owned_blocks);
    }

    /// Find a single block by ID. Convenience method for single block queries.
    pub fn find_block_by_id(self: *QueryEngine, block_id: BlockId) !QueryResult {
        const query = GetBlocksQuery{
            .block_ids = &[_]BlockId{block_id},
        };
        return self.execute_get_blocks(query);
    }

    /// Execute a graph traversal query.
    /// Time complexity: O(V + E) where V is vertices and E is edges traversed.
    /// Space complexity: O(V) for visited tracking and result storage.
    pub fn execute_traversal(self: *QueryEngine, query: TraversalQuery) !TraversalResult {
        const start_time = std.time.nanoTimestamp();
        defer {
            const duration = @as(u64, @intCast(std.time.nanoTimestamp() - start_time));
            _ = self.queries_executed.fetchAdd(1, .monotonic);
            _ = self.traversal_queries.fetchAdd(1, .monotonic);
            _ = self.total_query_time_ns.fetchAdd(duration, .monotonic);
        }

        assert(self.initialized);
        if (!self.initialized) return QueryError.NotInitialized;

        try query.validate();

        const start_block_ptr = self.storage_engine.find_block_by_id(
            query.start_block_id,
        ) catch |err| switch (err) {
            storage.StorageError.BlockNotFound => return QueryError.BlockNotFound,
            else => return err,
        };
        const start_block = start_block_ptr.*;

        switch (query.algorithm) {
            .breadth_first => return self.traverse_breadth_first(query, start_block),
            .depth_first => return self.traverse_depth_first(query, start_block),
        }
    }

    /// Perform breadth-first traversal of the knowledge graph.
    fn traverse_breadth_first(
        self: *QueryEngine,
        query: TraversalQuery,
        start_block: ContextBlock,
    ) !TraversalResult {
        _ = start_block;
        var visited = std.HashMap(
            BlockId,
            void,
            struct {
                pub fn hash(ctx: @This(), key: BlockId) u64 {
                    _ = ctx;
                    return std.hash_map.hashString(&key.bytes);
                }
                pub fn eql(ctx: @This(), a: BlockId, b: BlockId) bool {
                    _ = ctx;
                    return a.eql(b);
                }
            },
            std.hash_map.default_max_load_percentage,
        ).init(self.allocator);
        defer visited.deinit();

        var result_blocks = std.ArrayList(ContextBlock).init(self.allocator);
        defer result_blocks.deinit();

        var result_paths = std.ArrayList([]BlockId).init(self.allocator);
        defer result_paths.deinit();

        var result_depths = std.ArrayList(u32).init(self.allocator);
        defer result_depths.deinit();

        // Queue for BFS: (block_id, depth, path_to_block)
        const QueueItem = struct {
            block_id: BlockId,
            depth: u32,
            path: []BlockId,
        };

        var queue = std.ArrayList(QueueItem).init(self.allocator);
        defer {
            for (queue.items) |item| {
                self.allocator.free(item.path);
            }
            queue.deinit();
        }

        const start_path = try self.allocator.alloc(BlockId, 1);
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
            defer self.allocator.free(current.path);

            blocks_traversed += 1;
            max_depth_reached = @max(max_depth_reached, current.depth);

            const current_block = self.storage_engine.find_block_by_id(
                current.block_id,
            ) catch continue;

            const cloned_block = try self.clone_block(current_block);
            try result_blocks.append(cloned_block);

            const cloned_path = try self.allocator.dupe(BlockId, current.path);
            try result_paths.append(cloned_path);

            try result_depths.append(current.depth);

            if (query.max_depth > 0 and current.depth >= query.max_depth) {
                continue;
            }

            try self.add_neighbors_to_queue(
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
            self.allocator,
            owned_blocks,
            owned_paths,
            owned_depths,
            blocks_traversed,
            max_depth_reached,
        );
    }

    /// Perform depth-first traversal of the knowledge graph.
    fn traverse_depth_first(
        self: *QueryEngine,
        query: TraversalQuery,
        start_block: ContextBlock,
    ) !TraversalResult {
        _ = start_block;
        var visited = std.HashMap(
            BlockId,
            void,
            struct {
                pub fn hash(ctx: @This(), key: BlockId) u64 {
                    _ = ctx;
                    return std.hash_map.hashString(&key.bytes);
                }
                pub fn eql(ctx: @This(), a: BlockId, b: BlockId) bool {
                    _ = ctx;
                    return a.eql(b);
                }
            },
            std.hash_map.default_max_load_percentage,
        ).init(self.allocator);
        defer visited.deinit();

        var result_blocks = std.ArrayList(ContextBlock).init(self.allocator);
        defer result_blocks.deinit();

        var result_paths = std.ArrayList([]BlockId).init(self.allocator);
        defer result_paths.deinit();

        var result_depths = std.ArrayList(u32).init(self.allocator);
        defer result_depths.deinit();

        // Stack for DFS: (block_id, depth, path_to_block)
        const StackItem = struct {
            block_id: BlockId,
            depth: u32,
            path: []BlockId,
        };

        var stack = std.ArrayList(StackItem).init(self.allocator);
        defer {
            for (stack.items) |item| {
                self.allocator.free(item.path);
            }
            stack.deinit();
        }

        const start_path = try self.allocator.alloc(BlockId, 1);
        start_path[0] = query.start_block_id;

        try stack.append(StackItem{
            .block_id = query.start_block_id,
            .depth = 0,
            .path = start_path,
        });

        var blocks_traversed: u32 = 0;
        var max_depth_reached: u32 = 0;

        while (stack.items.len > 0 and result_blocks.items.len < query.max_results) {
            const current = stack.pop();
            defer self.allocator.free(current.path);

            if (visited.contains(current.block_id)) {
                continue;
            }

            try visited.put(current.block_id, {});
            blocks_traversed += 1;
            max_depth_reached = @max(max_depth_reached, current.depth);

            const current_block = self.storage_engine.find_block_by_id(
                current.block_id,
            ) catch continue;

            const cloned_block = try self.clone_block(current_block);
            try result_blocks.append(cloned_block);

            const cloned_path = try self.allocator.dupe(BlockId, current.path);
            try result_paths.append(cloned_path);

            try result_depths.append(current.depth);

            if (query.max_depth > 0 and current.depth >= query.max_depth) {
                continue;
            }

            try self.add_neighbors_to_stack(
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
            self.allocator,
            owned_blocks,
            owned_paths,
            owned_depths,
            blocks_traversed,
            max_depth_reached,
        );
    }

    /// Helper to clone a block for query results.
    fn clone_block(self: *QueryEngine, block: ContextBlock) !ContextBlock {
        return ContextBlock{
            .id = block.id,
            .version = block.version,
            .source_uri = try self.allocator.dupe(u8, block.source_uri),
            .metadata_json = try self.allocator.dupe(u8, block.metadata_json),
            .content = try self.allocator.dupe(u8, block.content),
        };
    }

    /// Add neighbors to BFS queue.
    fn add_neighbors_to_queue(
        self: *QueryEngine,
        queue: anytype,
        visited: *std.HashMap(
            BlockId,
            void,
            struct {
                pub fn hash(ctx: @This(), key: BlockId) u64 {
                    _ = ctx;
                    return std.hash_map.hashString(&key.bytes);
                }
                pub fn eql(ctx: @This(), a: BlockId, b: BlockId) bool {
                    _ = ctx;
                    return a.eql(b);
                }
            },
            std.hash_map.default_max_load_percentage,
        ),
        current_id: BlockId,
        next_depth: u32,
        current_path: []const BlockId,
        query: TraversalQuery,
    ) !void {
        const QueueItem = @TypeOf(queue.items[0]);

        if (query.direction == .outgoing or query.direction == .bidirectional) {
            if (self.storage_engine.graph_index.find_outgoing_edges(current_id)) |edges| {
                for (edges) |edge| {
                    if (query.edge_type_filter) |filter| {
                        if (edge.edge_type != filter) continue;
                    }

                    if (!visited.contains(edge.target_id)) {
                        const new_path = try self.allocator.alloc(BlockId, current_path.len + 1);
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
            if (self.storage_engine.graph_index.find_incoming_edges(current_id)) |edges| {
                for (edges) |edge| {
                    if (query.edge_type_filter) |filter| {
                        if (edge.edge_type != filter) continue;
                    }

                    if (!visited.contains(edge.source_id)) {
                        const new_path = try self.allocator.alloc(BlockId, current_path.len + 1);
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

    /// Add neighbors to DFS stack.
    fn add_neighbors_to_stack(
        self: *QueryEngine,
        stack: anytype,
        visited: *std.HashMap(
            BlockId,
            void,
            struct {
                pub fn hash(ctx: @This(), key: BlockId) u64 {
                    _ = ctx;
                    return std.hash_map.hashString(&key.bytes);
                }
                pub fn eql(ctx: @This(), a: BlockId, b: BlockId) bool {
                    _ = ctx;
                    return a.eql(b);
                }
            },
            std.hash_map.default_max_load_percentage,
        ),
        current_id: BlockId,
        next_depth: u32,
        current_path: []const BlockId,
        query: TraversalQuery,
    ) !void {
        const StackItem = @TypeOf(stack.items[0]);

        if (query.direction == .outgoing or query.direction == .bidirectional) {
            if (self.storage_engine.graph_index.find_outgoing_edges(current_id)) |edges| {
                for (edges) |edge| {
                    if (query.edge_type_filter) |filter| {
                        if (edge.edge_type != filter) continue;
                    }

                    if (!visited.contains(edge.target_id)) {
                        const new_path = try self.allocator.alloc(BlockId, current_path.len + 1);
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
            if (self.storage_engine.graph_index.find_incoming_edges(current_id)) |edges| {
                for (edges) |edge| {
                    if (query.edge_type_filter) |filter| {
                        if (edge.edge_type != filter) continue;
                    }

                    if (!visited.contains(edge.source_id)) {
                        const new_path = try self.allocator.alloc(BlockId, current_path.len + 1);
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

    /// Convenience method for outgoing traversal.
    pub fn traverse_outgoing(
        self: *QueryEngine,
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
        return self.execute_traversal(query);
    }

    /// Convenience method for incoming traversal.
    pub fn traverse_incoming(
        self: *QueryEngine,
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
        return self.execute_traversal(query);
    }

    /// Convenience method for bidirectional traversal.
    pub fn traverse_bidirectional(
        self: *QueryEngine,
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
        return self.execute_traversal(query);
    }

    /// Execute a filtered query with enhanced filtering capabilities.
    /// Time complexity: O(n) where n is the total number of blocks in storage.
    /// Space complexity: O(m) where m is the number of matching blocks.
    pub fn execute_filtered_query(self: *QueryEngine, query: FilteredQuery) !FilteredQueryResult {
        const start_time = std.time.nanoTimestamp();
        defer {
            const duration = @as(u64, @intCast(std.time.nanoTimestamp() - start_time));
            _ = self.queries_executed.fetchAdd(1, .monotonic);
            _ = self.filtered_queries.fetchAdd(1, .monotonic);
            _ = self.total_query_time_ns.fetchAdd(duration, .monotonic);
        }

        assert(self.initialized);
        if (!self.initialized) return QueryError.NotInitialized;

        try query.validate();

        var matched_blocks = std.ArrayList(ContextBlock).init(self.allocator);
        defer matched_blocks.deinit();

        var metadata_analysis: ?MetadataAnalysisMap = null;
        if (query.include_metadata_analysis) {
            metadata_analysis = MetadataAnalysisMap.init(self.allocator);
        }

        var blocks_scanned: u32 = 0;
        var blocks_matched: u32 = 0;

        // Iterate through all blocks in storage
        var block_iterator = self.storage_engine.iterate_all_blocks();
        while (try block_iterator.next()) |block| {
            blocks_scanned += 1;

            // Apply filter condition
            if (try query.filter.matches(block, self.allocator)) {
                blocks_matched += 1;

                const cloned_block = try self.clone_block(block);
                try matched_blocks.append(cloned_block);

                // Update metadata analysis if requested
                if (metadata_analysis) |*analysis| {
                    try self.update_metadata_analysis(analysis, block.metadata_json);
                }

                if (matched_blocks.items.len >= query.max_results) {
                    break;
                }
            }
        }

        // Sort results if requested
        if (query.sort_by_version) |sort_order| {
            switch (sort_order) {
                .ascending => {
                    std.mem.sort(ContextBlock, matched_blocks.items, {}, struct {
                        fn less_than(context: void, a: ContextBlock, b: ContextBlock) bool {
                            _ = context;
                            return a.version < b.version;
                        }
                    }.less_than);
                },
                .descending => {
                    std.mem.sort(ContextBlock, matched_blocks.items, {}, struct {
                        fn less_than(context: void, a: ContextBlock, b: ContextBlock) bool {
                            _ = context;
                            return a.version > b.version;
                        }
                    }.less_than);
                },
            }
        }

        const execution_time = @as(u64, @intCast(std.time.nanoTimestamp() - start_time));
        const owned_blocks = try matched_blocks.toOwnedSlice();

        return FilteredQueryResult.init(
            self.allocator,
            owned_blocks,
            metadata_analysis,
            blocks_scanned,
            blocks_matched,
            execution_time,
        );
    }

    /// Execute a semantic search query (placeholder for external embedding integration).
    /// Currently returns an error indicating semantic search is not yet implemented.
    pub fn execute_semantic_query(self: *QueryEngine, query: SemanticQuery) !SemanticQueryResult {
        const start_time = std.time.nanoTimestamp();
        defer {
            const duration = @as(u64, @intCast(std.time.nanoTimestamp() - start_time));
            _ = self.queries_executed.fetchAdd(1, .monotonic);
            _ = self.semantic_queries.fetchAdd(1, .monotonic);
            _ = self.total_query_time_ns.fetchAdd(duration, .monotonic);
        }

        assert(self.initialized);
        if (!self.initialized) return QueryError.NotInitialized;

        try query.validate();

        // Semantic search not yet implemented - returns empty results
        // Will be expanded when embedding model integration is added

        // For now, return an empty result to maintain interface compatibility
        const empty_results = try self.allocator.alloc(struct { block: ContextBlock, similarity_score: f32 }, 0);
        const execution_time = @as(u64, @intCast(std.time.nanoTimestamp() - start_time));

        return SemanticQueryResult.init(
            self.allocator,
            empty_results,
            0, // blocks_scanned
            execution_time,
        );
    }

    /// Helper method to update metadata analysis with field frequencies.
    fn update_metadata_analysis(
        self: *QueryEngine,
        analysis: *MetadataAnalysisMap,
        metadata_json: []const u8,
    ) !void {
        _ = self;
        // Simple JSON field extraction - parse fields from metadata
        // This is a simplified implementation that looks for quoted field names
        var i: usize = 0;
        while (i < metadata_json.len) {
            if (metadata_json[i] == '"') {
                const start = i + 1;
                i += 1;
                // Find the end of the field name
                while (i < metadata_json.len and metadata_json[i] != '"') {
                    if (metadata_json[i] == '\\') i += 1; // Skip escaped characters
                    i += 1;
                }
                if (i < metadata_json.len) {
                    const field_name = metadata_json[start..i];
                    // Skip to the colon and value
                    while (i < metadata_json.len and metadata_json[i] != ':') i += 1;

                    if (field_name.len > 0) {
                        const owned_field = try analysis.allocator.dupe(u8, field_name);
                        const result = try analysis.getOrPut(owned_field);
                        if (!result.found_existing) {
                            result.value_ptr.* = 1;
                        } else {
                            // Free the duplicate key since we already have it
                            analysis.allocator.free(owned_field);
                            result.value_ptr.* += 1;
                        }
                    }
                }
            }
            i += 1;
        }
    }

    /// Get query engine statistics.
    pub fn statistics(self: *QueryEngine) QueryStatistics {
        return QueryStatistics{
            .total_blocks_stored = self.storage_engine.block_count(),
            .queries_executed = self.queries_executed.load(.monotonic),
            .get_blocks_queries = self.get_blocks_queries.load(.monotonic),
            .traversal_queries = self.traversal_queries.load(.monotonic),
            .filtered_queries = self.filtered_queries.load(.monotonic),
            .semantic_queries = self.semantic_queries.load(.monotonic),
            .total_query_time_ns = self.total_query_time_ns.load(.monotonic),
        };
    }
};

/// Query engine performance statistics.
pub const QueryStatistics = struct {
    /// Total number of blocks in storage
    total_blocks_stored: u32,
    /// Total number of queries executed
    queries_executed: u64,
    /// Number of get_blocks queries executed
    get_blocks_queries: u64,
    /// Number of traversal queries executed
    traversal_queries: u64,
    /// Number of filtered queries executed
    filtered_queries: u64,
    /// Number of semantic queries executed
    semantic_queries: u64,
    /// Total query execution time in nanoseconds
    total_query_time_ns: u64,
    /// Average query latency in nanoseconds
    pub fn average_query_latency_ns(self: *const QueryStatistics) u64 {
        if (self.queries_executed == 0) return 0;
        return self.total_query_time_ns / self.queries_executed;
    }
    /// Query throughput in queries per second (based on total execution time)
    pub fn queries_per_second(self: *const QueryStatistics) f64 {
        if (self.total_query_time_ns == 0) return 0.0;
        const seconds = @as(f64, @floatFromInt(self.total_query_time_ns)) / 1_000_000_000.0;
        return @as(f64, @floatFromInt(self.queries_executed)) / seconds;
    }

    /// Format query statistics as human-readable text.
    pub fn format_human_readable(self: *const QueryStatistics, writer: anytype) !void {
        try writer.writeAll("=== Query Engine Metrics ===\n");
        try writer.print("Storage: {} blocks available\n", .{self.total_blocks_stored});
        try writer.print("Queries: {} total ({} get_blocks, {} traversal, {} filtered, {} semantic)\n", .{
            self.queries_executed,
            self.get_blocks_queries,
            self.traversal_queries,
            self.filtered_queries,
            self.semantic_queries,
        });
        try writer.print("Performance: {} ns avg latency, {d:.2} queries/sec\n", .{
            self.average_query_latency_ns(),
            self.queries_per_second(),
        });
    }

    /// Format query statistics as JSON.
    pub fn format_json(self: *const QueryStatistics, writer: anytype) !void {
        try writer.writeAll("{\n");
        try writer.print("  \"total_blocks_stored\": {},\n", .{self.total_blocks_stored});
        try writer.print("  \"queries_executed\": {},\n", .{self.queries_executed});
        try writer.print("  \"get_blocks_queries\": {},\n", .{self.get_blocks_queries});
        try writer.print("  \"traversal_queries\": {},\n", .{self.traversal_queries});
        try writer.print("  \"filtered_queries\": {},\n", .{self.filtered_queries});
        try writer.print("  \"semantic_queries\": {},\n", .{self.semantic_queries});
        try writer.print("  \"total_query_time_ns\": {},\n", .{self.total_query_time_ns});
        try writer.print(
            "  \"average_query_latency_ns\": {},\n",
            .{self.average_query_latency_ns()},
        );
        try writer.print("  \"queries_per_second\": {d:.2}\n", .{self.queries_per_second()});
        try writer.writeAll("}\n");
    }
};

// Helper functions for metadata filtering

/// Check if metadata field equals specific value.
fn match_metadata_equals(metadata_json: []const u8, field: []const u8, value: []const u8, allocator: std.mem.Allocator) !bool {

    // Look for the field in JSON: "field":"value" or "field": "value"
    const search_pattern = try std.fmt.allocPrint(allocator, "\"{s}\"", .{field});
    defer allocator.free(search_pattern);

    if (std.mem.indexOf(u8, metadata_json, search_pattern)) |field_pos| {
        // Find the colon after the field name
        var pos = field_pos + search_pattern.len;
        while (pos < metadata_json.len and metadata_json[pos] != ':') pos += 1;
        if (pos < metadata_json.len) {
            pos += 1; // Skip the colon
            // Skip whitespace
            while (pos < metadata_json.len and (metadata_json[pos] == ' ' or metadata_json[pos] == '\t')) pos += 1;

            // Check if the value matches (with quotes)
            if (pos < metadata_json.len and metadata_json[pos] == '"') {
                pos += 1; // Skip opening quote
                const value_start = pos;
                // Find the closing quote
                while (pos < metadata_json.len and metadata_json[pos] != '"') {
                    if (metadata_json[pos] == '\\') pos += 1; // Skip escaped characters
                    pos += 1;
                }
                if (pos < metadata_json.len) {
                    const found_value = metadata_json[value_start..pos];
                    return std.mem.eql(u8, found_value, value);
                }
            }
        }
    }
    return false;
}

/// Check if metadata field contains substring.
fn match_metadata_contains(metadata_json: []const u8, field: []const u8, substring: []const u8, allocator: std.mem.Allocator) !bool {

    // Look for the field in JSON
    const search_pattern = try std.fmt.allocPrint(allocator, "\"{s}\"", .{field});
    defer allocator.free(search_pattern);

    if (std.mem.indexOf(u8, metadata_json, search_pattern)) |field_pos| {
        // Find the colon after the field name
        var pos = field_pos + search_pattern.len;
        while (pos < metadata_json.len and metadata_json[pos] != ':') pos += 1;
        if (pos < metadata_json.len) {
            pos += 1; // Skip the colon
            // Skip whitespace
            while (pos < metadata_json.len and (metadata_json[pos] == ' ' or metadata_json[pos] == '\t')) pos += 1;

            // Check if the value contains the substring
            if (pos < metadata_json.len and metadata_json[pos] == '"') {
                pos += 1; // Skip opening quote
                const value_start = pos;
                // Find the closing quote
                while (pos < metadata_json.len and metadata_json[pos] != '"') {
                    if (metadata_json[pos] == '\\') pos += 1; // Skip escaped characters
                    pos += 1;
                }
                if (pos < metadata_json.len) {
                    const found_value = metadata_json[value_start..pos];
                    return std.mem.indexOf(u8, found_value, substring) != null;
                }
            }
        }
    }
    return false;
}

/// Check if content contains substring (case insensitive).
fn match_content_contains(content: []const u8, substring: []const u8) bool {
    // Simple implementation - just check if substring exists (case sensitive for now)
    return std.mem.indexOf(u8, content, substring) != null;
}

/// Check if source URI matches pattern.
fn match_source_pattern(source_uri: []const u8, pattern: []const u8) bool {
    // Simple pattern matching - check if URI contains the pattern
    return std.mem.indexOf(u8, source_uri, pattern) != null;
}

// Tests

test "GetBlocksQuery validation" {
    const allocator = std.testing.allocator;

    // Valid query
    const test_id = try BlockId.from_hex("0123456789abcdeffedcba9876543210");
    const valid_query = GetBlocksQuery{
        .block_ids = &[_]BlockId{test_id},
    };
    try valid_query.validate();

    // Empty query should fail
    const empty_query = GetBlocksQuery{
        .block_ids = &[_]BlockId{},
    };
    try std.testing.expectError(QueryError.EmptyQuery, empty_query.validate());

    // Too many blocks should fail
    const too_many_ids = try allocator.alloc(BlockId, GetBlocksQuery.MAX_BLOCKS + 1);
    defer allocator.free(too_many_ids);
    for (too_many_ids) |*id| {
        id.* = test_id;
    }

    const oversized_query = GetBlocksQuery{
        .block_ids = too_many_ids,
    };
    try std.testing.expectError(QueryError.TooManyResults, oversized_query.validate());
}

test "QueryResult formatting" {
    const allocator = std.testing.allocator;

    const test_id = try BlockId.from_hex("0123456789abcdeffedcba9876543210");
    const test_block = ContextBlock{
        .id = test_id,
        .version = 42,
        .source_uri = "git://example.com/repo.git/file.zig#L123",
        .metadata_json = "{\"type\":\"function\",\"language\":\"zig\"}",
        .content = "pub fn test_function() void { return; }",
    };

    const cloned_block = ContextBlock{
        .id = test_block.id,
        .version = test_block.version,
        .source_uri = try allocator.dupe(u8, test_block.source_uri),
        .metadata_json = try allocator.dupe(u8, test_block.metadata_json),
        .content = try allocator.dupe(u8, test_block.content),
    };

    const blocks = try allocator.alloc(ContextBlock, 1);
    blocks[0] = cloned_block;

    const result = QueryResult.init(allocator, blocks);
    defer result.deinit();

    // Test LLM formatting
    const formatted = try result.format_for_llm(allocator);
    defer allocator.free(formatted);

    try std.testing.expect(std.mem.indexOf(u8, formatted, "--- BEGIN CONTEXT BLOCK ---") != null);
    try std.testing.expect(std.mem.indexOf(u8, formatted, "--- END CONTEXT BLOCK ---") != null);
    const expected_id = "ID: 0123456789abcdeffedcba9876543210";
    try std.testing.expect(std.mem.indexOf(u8, formatted, expected_id) != null);
    try std.testing.expect(std.mem.indexOf(u8, formatted, "Version: 42") != null);
    try std.testing.expect(std.mem.indexOf(u8, formatted, test_block.content) != null);
}

test "QueryEngine basic operations" {
    const allocator = std.testing.allocator;
    const simulation_vfs = @import("simulation_vfs");

    // Setup storage engine
    var sim_vfs = simulation_vfs.SimulationVFS.init(allocator);
    defer sim_vfs.deinit();

    const vfs_interface = sim_vfs.vfs();
    const data_dir = try allocator.dupe(u8, "test_data");

    var storage_engine = try StorageEngine.init(allocator, vfs_interface, data_dir);
    defer storage_engine.deinit();

    try storage_engine.initialize_storage();

    // Setup query engine
    var query_engine = QueryEngine.init(allocator, &storage_engine);
    defer query_engine.deinit();

    const test_id = try BlockId.from_hex("0123456789abcdeffedcba9876543210");
    const test_block = ContextBlock{
        .id = test_id,
        .version = 1,
        .source_uri = "test://uri",
        .metadata_json = "{\"test\":true}",
        .content = "test content",
    };

    try storage_engine.put_block(test_block);

    // Test single block query
    const result = try query_engine.find_block_by_id(test_id);
    defer result.deinit();

    try std.testing.expectEqual(@as(u32, 1), result.count);
    try std.testing.expect(result.blocks[0].id.eql(test_id));
    try std.testing.expectEqualStrings("test content", result.blocks[0].content);

    // Test missing block
    const missing_id = try BlockId.from_hex("fedcba9876543210123456789abcdef0");
    const missing_result = try query_engine.find_block_by_id(missing_id);
    defer missing_result.deinit();

    try std.testing.expectEqual(@as(u32, 0), missing_result.count);
}

test "QueryEngine multiple blocks" {
    const allocator = std.testing.allocator;
    const simulation_vfs = @import("simulation_vfs");

    // Setup storage engine
    var sim_vfs = simulation_vfs.SimulationVFS.init(allocator);
    defer sim_vfs.deinit();

    const vfs_interface = sim_vfs.vfs();
    const data_dir = try allocator.dupe(u8, "test_data");

    var storage_engine = try StorageEngine.init(allocator, vfs_interface, data_dir);
    defer storage_engine.deinit();

    try storage_engine.initialize_storage();

    // Setup query engine
    var query_engine = QueryEngine.init(allocator, &storage_engine);
    defer query_engine.deinit();

    const block1_id = try BlockId.from_hex("0123456789abcdeffedcba9876543210");
    const block2_id = try BlockId.from_hex("fedcba9876543210123456789abcdef0");
    const block3_id = try BlockId.from_hex("1111111111111111222222222222222");

    const block1 = ContextBlock{
        .id = block1_id,
        .version = 1,
        .source_uri = "test://block1",
        .metadata_json = "{\"name\":\"block1\"}",
        .content = "block 1 content",
    };

    const block2 = ContextBlock{
        .id = block2_id,
        .version = 2,
        .source_uri = "test://block2",
        .metadata_json = "{\"name\":\"block2\"}",
        .content = "block 2 content",
    };

    try storage_engine.put_block(block1);
    try storage_engine.put_block(block2);

    // Test multiple block query (including one missing block)
    const query = GetBlocksQuery{
        .block_ids = &[_]BlockId{ block1_id, block2_id, block3_id },
    };

    const result = try query_engine.execute_get_blocks(query);
    defer result.deinit();

    // Should find 2 blocks (block3 is missing)
    try std.testing.expectEqual(@as(u32, 2), result.count);

    // Verify blocks are correctly retrieved
    var found_block1 = false;
    var found_block2 = false;

    for (result.blocks) |block| {
        if (block.id.eql(block1_id)) {
            found_block1 = true;
            try std.testing.expectEqualStrings("block 1 content", block.content);
        } else if (block.id.eql(block2_id)) {
            found_block2 = true;
            try std.testing.expectEqualStrings("block 2 content", block.content);
        }
    }

    try std.testing.expect(found_block1);
    try std.testing.expect(found_block2);
}

test "QueryStatistics formatting methods" {
    const stats = QueryStatistics{
        .total_blocks_stored = 42,
        .queries_executed = 10,
        .get_blocks_queries = 6,
        .traversal_queries = 4,
        .filtered_queries = 2,
        .semantic_queries = 1,
        .total_query_time_ns = 1000000, // 1ms total
    };

    // Test human-readable formatting
    var buffer: [1024]u8 = undefined;
    var stream = std.io.fixedBufferStream(&buffer);
    try stats.format_human_readable(stream.writer());

    const output = stream.getWritten();
    try std.testing.expect(std.mem.indexOf(u8, output, "Query Engine Metrics") != null);
    try std.testing.expect(std.mem.indexOf(u8, output, "42 blocks") != null);
    try std.testing.expect(std.mem.indexOf(u8, output, "10 total") != null);

    // Test JSON formatting
    stream.reset();
    try stats.format_json(stream.writer());

    const json_output = stream.getWritten();
    try std.testing.expect(std.mem.indexOf(u8, json_output, "\"queries_executed\": 10") != null);
    try std.testing.expect(std.mem.indexOf(u8, json_output, "\"total_blocks_stored\": 42") != null);
}

test "QueryEngine statistics" {
    const allocator = std.testing.allocator;
    const simulation_vfs = @import("simulation_vfs");

    // Setup storage engine
    var sim_vfs = simulation_vfs.SimulationVFS.init(allocator);
    defer sim_vfs.deinit();

    const vfs_interface = sim_vfs.vfs();
    const data_dir = try allocator.dupe(u8, "test_data");

    var storage_engine = try StorageEngine.init(allocator, vfs_interface, data_dir);
    defer storage_engine.deinit();

    try storage_engine.initialize_storage();

    // Setup query engine
    var query_engine = QueryEngine.init(allocator, &storage_engine);
    defer query_engine.deinit();

    // Test initial statistics
    var stats = query_engine.statistics();
    try std.testing.expectEqual(@as(u32, 0), stats.total_blocks_stored);

    const test_id = try BlockId.from_hex("0123456789abcdeffedcba9876543210");
    const test_block = ContextBlock{
        .id = test_id,
        .version = 1,
        .source_uri = "test://uri",
        .metadata_json = "{\"test\":true}",
        .content = "test content",
    };

    try storage_engine.put_block(test_block);

    stats = query_engine.statistics();
    try std.testing.expectEqual(@as(u32, 1), stats.total_blocks_stored);
}

test "TraversalQuery validation" {
    const test_id = try BlockId.from_hex("0123456789abcdeffedcba9876543210");

    // Valid query
    const valid_query = TraversalQuery.init(test_id, .outgoing);
    try valid_query.validate();

    // Empty results should fail
    var empty_query = TraversalQuery.init(test_id, .outgoing);
    empty_query.max_results = 0;
    try std.testing.expectError(QueryError.EmptyQuery, empty_query.validate());

    // Too many results should fail
    var oversized_query = TraversalQuery.init(test_id, .outgoing);
    oversized_query.max_results = TraversalQuery.ABSOLUTE_MAX_RESULTS + 1;
    try std.testing.expectError(QueryError.TooManyResults, oversized_query.validate());
}

test "QueryEngine breadth-first traversal" {
    const allocator = std.testing.allocator;
    const simulation_vfs = @import("simulation_vfs");

    // Setup storage engine
    var sim_vfs = simulation_vfs.SimulationVFS.init(allocator);
    defer sim_vfs.deinit();

    const vfs_interface = sim_vfs.vfs();
    const data_dir = try allocator.dupe(u8, "test_traversal");

    var storage_engine = try StorageEngine.init(allocator, vfs_interface, data_dir);
    defer storage_engine.deinit();

    try storage_engine.initialize_storage();

    // Setup query engine
    var query_engine = QueryEngine.init(allocator, &storage_engine);
    defer query_engine.deinit();

    const block_a_id = try BlockId.from_hex("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa0");
    const block_b_id = try BlockId.from_hex("bbbbbbbbbbbbbbbbbbbbbbbbbbbbbb0");
    const block_c_id = try BlockId.from_hex("cccccccccccccccccccccccccccccc0");
    const block_d_id = try BlockId.from_hex("dddddddddddddddddddddddddddddd0");

    const block_a = ContextBlock{
        .id = block_a_id,
        .version = 1,
        .source_uri = "test://block_a",
        .metadata_json = "{\"name\":\"A\"}",
        .content = "Block A content",
    };

    const block_b = ContextBlock{
        .id = block_b_id,
        .version = 1,
        .source_uri = "test://block_b",
        .metadata_json = "{\"name\":\"B\"}",
        .content = "Block B content",
    };

    const block_c = ContextBlock{
        .id = block_c_id,
        .version = 1,
        .source_uri = "test://block_c",
        .metadata_json = "{\"name\":\"C\"}",
        .content = "Block C content",
    };

    const block_d = ContextBlock{
        .id = block_d_id,
        .version = 1,
        .source_uri = "test://block_d",
        .metadata_json = "{\"name\":\"D\"}",
        .content = "Block D content",
    };

    try storage_engine.put_block(block_a);
    try storage_engine.put_block(block_b);
    try storage_engine.put_block(block_c);
    try storage_engine.put_block(block_d);

    // Create edges: A -> B, B -> C, A -> D
    const edge_a_to_b = GraphEdge{
        .source_id = block_a_id,
        .target_id = block_b_id,
        .edge_type = .references,
    };

    const edge_b_to_c = GraphEdge{
        .source_id = block_b_id,
        .target_id = block_c_id,
        .edge_type = .references,
    };

    const edge_a_to_d = GraphEdge{
        .source_id = block_a_id,
        .target_id = block_d_id,
        .edge_type = .imports,
    };

    try storage_engine.put_edge(edge_a_to_b);
    try storage_engine.put_edge(edge_b_to_c);
    try storage_engine.put_edge(edge_a_to_d);

    // Test BFS traversal from A
    const traversal_query = TraversalQuery{
        .start_block_id = block_a_id,
        .direction = .outgoing,
        .algorithm = .breadth_first,
        .max_depth = 3,
        .max_results = 100,
        .edge_type_filter = null,
    };

    const result = try query_engine.execute_traversal(traversal_query);
    defer result.deinit();

    // Should find all 4 blocks (A, B, C, D)
    try std.testing.expectEqual(@as(usize, 4), result.blocks.len);
    try std.testing.expectEqual(@as(u32, 4), result.blocks_traversed);
    try std.testing.expectEqual(@as(u32, 2), result.max_depth_reached);

    // Verify BFS order: A (depth 0), B and D (depth 1), C (depth 2)
    try std.testing.expect(result.blocks[0].id.eql(block_a_id));
    try std.testing.expectEqual(@as(u32, 0), result.depths[0]);

    // B and D should be at depth 1 (can be in either order due to HashMap iteration)
    var found_b = false;
    var found_d = false;
    for (1..3) |i| {
        try std.testing.expectEqual(@as(u32, 1), result.depths[i]);
        if (result.blocks[i].id.eql(block_b_id)) found_b = true;
        if (result.blocks[i].id.eql(block_d_id)) found_d = true;
    }
    try std.testing.expect(found_b);
    try std.testing.expect(found_d);

    // C should be at depth 2
    try std.testing.expect(result.blocks[3].id.eql(block_c_id));
    try std.testing.expectEqual(@as(u32, 2), result.depths[3]);
}

test "QueryEngine depth-first traversal" {
    const allocator = std.testing.allocator;
    const simulation_vfs = @import("simulation_vfs");

    // Setup storage engine
    var sim_vfs = simulation_vfs.SimulationVFS.init(allocator);
    defer sim_vfs.deinit();

    const vfs_interface = sim_vfs.vfs();
    const data_dir = try allocator.dupe(u8, "test_dfs");

    var storage_engine = try StorageEngine.init(allocator, vfs_interface, data_dir);
    defer storage_engine.deinit();

    try storage_engine.initialize_storage();

    // Setup query engine
    var query_engine = QueryEngine.init(allocator, &storage_engine);
    defer query_engine.deinit();

    // Create linear chain: A -> B -> C
    const block_a_id = try BlockId.from_hex("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa1");
    const block_b_id = try BlockId.from_hex("bbbbbbbbbbbbbbbbbbbbbbbbbbbbbb1");
    const block_c_id = try BlockId.from_hex("cccccccccccccccccccccccccccccc1");

    const block_a = ContextBlock{
        .id = block_a_id,
        .version = 1,
        .source_uri = "test://dfs_a",
        .metadata_json = "{\"type\":\"dfs\"}",
        .content = "DFS Block A",
    };

    const block_b = ContextBlock{
        .id = block_b_id,
        .version = 1,
        .source_uri = "test://dfs_b",
        .metadata_json = "{\"type\":\"dfs\"}",
        .content = "DFS Block B",
    };

    const block_c = ContextBlock{
        .id = block_c_id,
        .version = 1,
        .source_uri = "test://dfs_c",
        .metadata_json = "{\"type\":\"dfs\"}",
        .content = "DFS Block C",
    };

    try storage_engine.put_block(block_a);
    try storage_engine.put_block(block_b);
    try storage_engine.put_block(block_c);

    // Create edges: A -> B -> C
    const edge_a_to_b = GraphEdge{
        .source_id = block_a_id,
        .target_id = block_b_id,
        .edge_type = .references,
    };

    const edge_b_to_c = GraphEdge{
        .source_id = block_b_id,
        .target_id = block_c_id,
        .edge_type = .references,
    };

    try storage_engine.put_edge(edge_a_to_b);
    try storage_engine.put_edge(edge_b_to_c);

    // Test DFS traversal
    const traversal_query = TraversalQuery{
        .start_block_id = block_a_id,
        .direction = .outgoing,
        .algorithm = .depth_first,
        .max_depth = 3,
        .max_results = 100,
        .edge_type_filter = null,
    };

    const result = try query_engine.execute_traversal(traversal_query);
    defer result.deinit();

    // Should find all 3 blocks
    try std.testing.expectEqual(@as(usize, 3), result.blocks.len);
    try std.testing.expectEqual(@as(u32, 3), result.blocks_traversed);
    try std.testing.expectEqual(@as(u32, 2), result.max_depth_reached);

    // Verify DFS order: A, B, C (should visit deeply before backtracking)
    try std.testing.expect(result.blocks[0].id.eql(block_a_id));
    try std.testing.expect(result.blocks[1].id.eql(block_b_id));
    try std.testing.expect(result.blocks[2].id.eql(block_c_id));
}

test "QueryEngine traversal directions" {
    const allocator = std.testing.allocator;
    const simulation_vfs = @import("simulation_vfs");

    // Setup storage engine
    var sim_vfs = simulation_vfs.SimulationVFS.init(allocator);
    defer sim_vfs.deinit();

    const vfs_interface = sim_vfs.vfs();
    const data_dir = try allocator.dupe(u8, "test_directions");

    var storage_engine = try StorageEngine.init(allocator, vfs_interface, data_dir);
    defer storage_engine.deinit();

    try storage_engine.initialize_storage();

    // Setup query engine
    var query_engine = QueryEngine.init(allocator, &storage_engine);
    defer query_engine.deinit();

    // Create blocks: A -> B <- C
    const block_a_id = try BlockId.from_hex("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa2");
    const block_b_id = try BlockId.from_hex("bbbbbbbbbbbbbbbbbbbbbbbbbbbbbb2");
    const block_c_id = try BlockId.from_hex("cccccccccccccccccccccccccccccc2");

    const block_a = ContextBlock{
        .id = block_a_id,
        .version = 1,
        .source_uri = "test://dir_a",
        .metadata_json = "{}",
        .content = "Direction test A",
    };

    const block_b = ContextBlock{
        .id = block_b_id,
        .version = 1,
        .source_uri = "test://dir_b",
        .metadata_json = "{}",
        .content = "Direction test B",
    };

    const block_c = ContextBlock{
        .id = block_c_id,
        .version = 1,
        .source_uri = "test://dir_c",
        .metadata_json = "{}",
        .content = "Direction test C",
    };

    try storage_engine.put_block(block_a);
    try storage_engine.put_block(block_b);
    try storage_engine.put_block(block_c);

    // Create edges: A -> B, C -> B
    const edge_a_to_b = GraphEdge{
        .source_id = block_a_id,
        .target_id = block_b_id,
        .edge_type = .references,
    };

    const edge_c_to_b = GraphEdge{
        .source_id = block_c_id,
        .target_id = block_b_id,
        .edge_type = .references,
    };

    try storage_engine.put_edge(edge_a_to_b);
    try storage_engine.put_edge(edge_c_to_b);

    // Test outgoing traversal from A (should find A -> B)
    const outgoing_result = try query_engine.traverse_outgoing(block_a_id, 2);
    defer outgoing_result.deinit();

    try std.testing.expectEqual(@as(usize, 2), outgoing_result.blocks.len);
    try std.testing.expect(outgoing_result.blocks[0].id.eql(block_a_id));
    try std.testing.expect(outgoing_result.blocks[1].id.eql(block_b_id));

    // Test incoming traversal from B (should find A -> B and C -> B)
    const incoming_result = try query_engine.traverse_incoming(block_b_id, 2);
    defer incoming_result.deinit();

    try std.testing.expectEqual(@as(usize, 3), incoming_result.blocks.len);
    try std.testing.expect(incoming_result.blocks[0].id.eql(block_b_id));

    // Should find both A and C as incoming to B
    var found_a = false;
    var found_c = false;
    for (1..3) |i| {
        if (incoming_result.blocks[i].id.eql(block_a_id)) found_a = true;
        if (incoming_result.blocks[i].id.eql(block_c_id)) found_c = true;
    }
    try std.testing.expect(found_a);
    try std.testing.expect(found_c);

    // Test bidirectional traversal from B
    const bidirectional_result = try query_engine.traverse_bidirectional(block_b_id, 1);
    defer bidirectional_result.deinit();

    try std.testing.expectEqual(@as(usize, 3), bidirectional_result.blocks.len);
}

test "QueryEngine traversal with edge type filter" {
    const allocator = std.testing.allocator;
    const simulation_vfs = @import("simulation_vfs");

    // Setup storage engine
    var sim_vfs = simulation_vfs.SimulationVFS.init(allocator);
    defer sim_vfs.deinit();

    const vfs_interface = sim_vfs.vfs();
    const data_dir = try allocator.dupe(u8, "test_filter");

    var storage_engine = try StorageEngine.init(allocator, vfs_interface, data_dir);
    defer storage_engine.deinit();

    try storage_engine.initialize_storage();

    // Setup query engine
    var query_engine = QueryEngine.init(allocator, &storage_engine);
    defer query_engine.deinit();

    // Create blocks with different edge types
    const block_a_id = try BlockId.from_hex("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa3");
    const block_b_id = try BlockId.from_hex("bbbbbbbbbbbbbbbbbbbbbbbbbbbbbb3");
    const block_c_id = try BlockId.from_hex("cccccccccccccccccccccccccccccc3");

    const block_a = ContextBlock{
        .id = block_a_id,
        .version = 1,
        .source_uri = "test://filter_a",
        .metadata_json = "{}",
        .content = "Filter test A",
    };

    const block_b = ContextBlock{
        .id = block_b_id,
        .version = 1,
        .source_uri = "test://filter_b",
        .metadata_json = "{}",
        .content = "Filter test B",
    };

    const block_c = ContextBlock{
        .id = block_c_id,
        .version = 1,
        .source_uri = "test://filter_c",
        .metadata_json = "{}",
        .content = "Filter test C",
    };

    try storage_engine.put_block(block_a);
    try storage_engine.put_block(block_b);
    try storage_engine.put_block(block_c);

    // Create different edge types: A -references-> B, A -imports-> C
    const edge_a_to_b = GraphEdge{
        .source_id = block_a_id,
        .target_id = block_b_id,
        .edge_type = .references,
    };

    const edge_a_to_c = GraphEdge{
        .source_id = block_a_id,
        .target_id = block_c_id,
        .edge_type = .imports,
    };

    try storage_engine.put_edge(edge_a_to_b);
    try storage_engine.put_edge(edge_a_to_c);

    // Test traversal with references filter (should only find A -> B)
    const references_query = TraversalQuery{
        .start_block_id = block_a_id,
        .direction = .outgoing,
        .algorithm = .breadth_first,
        .max_depth = 2,
        .max_results = 100,
        .edge_type_filter = .references,
    };

    const references_result = try query_engine.execute_traversal(references_query);
    defer references_result.deinit();

    try std.testing.expectEqual(@as(usize, 2), references_result.blocks.len);
    try std.testing.expect(references_result.blocks[0].id.eql(block_a_id));
    try std.testing.expect(references_result.blocks[1].id.eql(block_b_id));

    // Test traversal with imports filter (should only find A -> C)
    const imports_query = TraversalQuery{
        .start_block_id = block_a_id,
        .direction = .outgoing,
        .algorithm = .breadth_first,
        .max_depth = 2,
        .max_results = 100,
        .edge_type_filter = .imports,
    };

    const imports_result = try query_engine.execute_traversal(imports_query);
    defer imports_result.deinit();

    try std.testing.expectEqual(@as(usize, 2), imports_result.blocks.len);
    try std.testing.expect(imports_result.blocks[0].id.eql(block_a_id));
    try std.testing.expect(imports_result.blocks[1].id.eql(block_c_id));
}

test "QueryEngine traversal depth limits" {
    const allocator = std.testing.allocator;
    const simulation_vfs = @import("simulation_vfs");

    // Setup storage engine
    var sim_vfs = simulation_vfs.SimulationVFS.init(allocator);
    defer sim_vfs.deinit();

    const vfs_interface = sim_vfs.vfs();
    const data_dir = try allocator.dupe(u8, "test_depth");

    var storage_engine = try StorageEngine.init(allocator, vfs_interface, data_dir);
    defer storage_engine.deinit();

    try storage_engine.initialize_storage();

    // Setup query engine
    var query_engine = QueryEngine.init(allocator, &storage_engine);
    defer query_engine.deinit();

    // Create a longer chain: A -> B -> C -> D -> E
    const block_ids = [_]BlockId{
        try BlockId.from_hex("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa4"),
        try BlockId.from_hex("bbbbbbbbbbbbbbbbbbbbbbbbbbbbbb4"),
        try BlockId.from_hex("cccccccccccccccccccccccccccccc4"),
        try BlockId.from_hex("dddddddddddddddddddddddddddddd4"),
        try BlockId.from_hex("eeeeeeeeeeeeeeeeeeeeeeeeeeeeee4"),
    };

    // Create and store blocks
    for (block_ids, 0..) |block_id, i| {
        const block = ContextBlock{
            .id = block_id,
            .version = 1,
            .source_uri = try std.fmt.allocPrint(allocator, "test://depth_{}", .{i}),
            .metadata_json = "{}",
            .content = try std.fmt.allocPrint(allocator, "Depth test block {}", .{i}),
        };
        defer allocator.free(block.source_uri);
        defer allocator.free(block.content);

        try storage_engine.put_block(block);
    }

    // Create chain edges: A -> B -> C -> D -> E
    for (0..block_ids.len - 1) |i| {
        const edge = GraphEdge{
            .source_id = block_ids[i],
            .target_id = block_ids[i + 1],
            .edge_type = .references,
        };
        try storage_engine.put_edge(edge);
    }

    // Test with depth limit 2 (should find A, B, C only)
    const limited_result = try query_engine.traverse_outgoing(block_ids[0], 2);
    defer limited_result.deinit();

    try std.testing.expectEqual(@as(usize, 3), limited_result.blocks.len);
    try std.testing.expectEqual(@as(u32, 2), limited_result.max_depth_reached);

    // Test with no depth limit (should find all blocks)
    const unlimited_result = try query_engine.traverse_outgoing(block_ids[0], 0);
    defer unlimited_result.deinit();

    try std.testing.expectEqual(@as(usize, 5), unlimited_result.blocks.len);
    try std.testing.expectEqual(@as(u32, 4), unlimited_result.max_depth_reached);
}

test "TraversalResult formatting" {
    const allocator = std.testing.allocator;

    const block_a_id = try BlockId.from_hex("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa5");
    const block_b_id = try BlockId.from_hex("bbbbbbbbbbbbbbbbbbbbbbbbbbbbbb5");

    const block_a = ContextBlock{
        .id = block_a_id,
        .version = 1,
        .source_uri = try allocator.dupe(u8, "test://format_a"),
        .metadata_json = try allocator.dupe(u8, "{\"test\":\"format\"}"),
        .content = try allocator.dupe(u8, "Format test content A"),
    };

    const block_b = ContextBlock{
        .id = block_b_id,
        .version = 1,
        .source_uri = try allocator.dupe(u8, "test://format_b"),
        .metadata_json = try allocator.dupe(u8, "{\"test\":\"format\"}"),
        .content = try allocator.dupe(u8, "Format test content B"),
    };

    const blocks = try allocator.alloc(ContextBlock, 2);
    blocks[0] = block_a;
    blocks[1] = block_b;

    const path_a = try allocator.alloc(BlockId, 1);
    path_a[0] = block_a_id;

    const path_b = try allocator.alloc(BlockId, 2);
    path_b[0] = block_a_id;
    path_b[1] = block_b_id;

    const paths = try allocator.alloc([]BlockId, 2);
    paths[0] = path_a;
    paths[1] = path_b;

    const depths = try allocator.alloc(u32, 2);
    depths[0] = 0;
    depths[1] = 1;

    const result = TraversalResult.init(allocator, blocks, paths, depths, 2, 1);
    defer result.deinit();

    // Test formatting
    const formatted = try result.format_for_llm(allocator);
    defer allocator.free(formatted);

    try std.testing.expect(std.mem.indexOf(
        u8,
        formatted,
        "=== GRAPH TRAVERSAL RESULT ===",
    ) != null);
    try std.testing.expect(std.mem.indexOf(u8, formatted, "Blocks found: 2") != null);
    try std.testing.expect(std.mem.indexOf(u8, formatted, "--- BEGIN TRAVERSAL BLOCK ---") != null);
    try std.testing.expect(std.mem.indexOf(u8, formatted, "--- END TRAVERSAL BLOCK ---") != null);
    try std.testing.expect(std.mem.indexOf(u8, formatted, "Depth: 0") != null);
    try std.testing.expect(std.mem.indexOf(u8, formatted, "Depth: 1") != null);
    try std.testing.expect(std.mem.indexOf(u8, formatted, "Path: ") != null);
}

// Query Engine V2 Tests

test "FilterCondition metadata_equals matching" {
    const allocator = std.testing.allocator;

    const condition = FilterCondition{
        .metadata_equals = .{
            .field = "type",
            .value = "function",
        },
    };

    const test_block = ContextBlock{
        .id = try BlockId.from_hex("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa6"),
        .version = 1,
        .source_uri = "test://uri",
        .metadata_json = "{\"type\":\"function\",\"language\":\"zig\"}",
        .content = "test content",
    };

    // Should match
    try std.testing.expect(try condition.matches(test_block, allocator));

    const wrong_block = ContextBlock{
        .id = try BlockId.from_hex("bbbbbbbbbbbbbbbbbbbbbbbbbbbbbb6"),
        .version = 1,
        .source_uri = "test://uri",
        .metadata_json = "{\"type\":\"struct\",\"language\":\"zig\"}",
        .content = "test content",
    };

    // Should not match
    try std.testing.expect(!try condition.matches(wrong_block, allocator));
}

test "FilterCondition content_contains matching" {
    const allocator = std.testing.allocator;

    const condition = FilterCondition{
        .content_contains = .{
            .substring = "hello world",
        },
    };

    const test_block = ContextBlock{
        .id = try BlockId.from_hex("cccccccccccccccccccccccccccccc6"),
        .version = 1,
        .source_uri = "test://uri",
        .metadata_json = "{}",
        .content = "This is a hello world example",
    };

    try std.testing.expect(try condition.matches(test_block, allocator));

    const wrong_block = ContextBlock{
        .id = try BlockId.from_hex("dddddddddddddddddddddddddddddd6"),
        .version = 1,
        .source_uri = "test://uri",
        .metadata_json = "{}",
        .content = "This does not contain the phrase",
    };

    try std.testing.expect(!try condition.matches(wrong_block, allocator));
}

test "FilterCondition version_range matching" {
    const allocator = std.testing.allocator;

    const condition = FilterCondition{
        .version_range = .{
            .min_version = 5,
            .max_version = 10,
        },
    };

    const valid_block = ContextBlock{
        .id = try BlockId.from_hex("eeeeeeeeeeeeeeeeeeeeeeeeeeeeee6"),
        .version = 7,
        .source_uri = "test://uri",
        .metadata_json = "{}",
        .content = "test content",
    };

    try std.testing.expect(try condition.matches(valid_block, allocator));

    const old_block = ContextBlock{
        .id = try BlockId.from_hex("ffffffffffffffffffffffffffffff6"),
        .version = 3,
        .source_uri = "test://uri",
        .metadata_json = "{}",
        .content = "test content",
    };

    try std.testing.expect(!try condition.matches(old_block, allocator));
}

test "FilterCondition and_conditions matching" {
    const allocator = std.testing.allocator;

    const condition1 = FilterCondition{
        .metadata_equals = .{
            .field = "language",
            .value = "zig",
        },
    };

    const condition2 = FilterCondition{
        .content_contains = .{
            .substring = "function",
        },
    };

    const conditions = [_]FilterCondition{ condition1, condition2 };
    const and_condition = FilterCondition{
        .and_conditions = .{
            .conditions = &conditions,
        },
    };

    const matching_block = ContextBlock{
        .id = try BlockId.from_hex("1111111111111111111111111111116"),
        .version = 1,
        .source_uri = "test://uri",
        .metadata_json = "{\"language\":\"zig\",\"type\":\"code\"}",
        .content = "pub fn my_function() void {}",
    };

    try std.testing.expect(try and_condition.matches(matching_block, allocator));

    const non_matching_block = ContextBlock{
        .id = try BlockId.from_hex("2222222222222222222222222222226"),
        .version = 1,
        .source_uri = "test://uri",
        .metadata_json = "{\"language\":\"rust\",\"type\":\"code\"}",
        .content = "pub fn my_function() void {}",
    };

    // Should fail because language is not "zig"
    try std.testing.expect(!try and_condition.matches(non_matching_block, allocator));
}

test "FilteredQuery validation" {
    const condition = FilterCondition{
        .metadata_equals = .{
            .field = "test",
            .value = "value",
        },
    };

    // Valid query
    const valid_query = FilteredQuery.init(condition);
    try valid_query.validate();

    // Empty results query should fail
    var empty_query = FilteredQuery.init(condition);
    empty_query.max_results = 0;
    try std.testing.expectError(QueryError.EmptyQuery, empty_query.validate());

    // Too many results should fail
    var oversized_query = FilteredQuery.init(condition);
    oversized_query.max_results = FilteredQuery.ABSOLUTE_MAX_RESULTS + 1;
    try std.testing.expectError(QueryError.TooManyResults, oversized_query.validate());
}

test "SemanticQuery validation" {
    // Valid query
    const valid_query = SemanticQuery.init("test query");
    try valid_query.validate();

    // Empty query should fail
    const empty_query = SemanticQuery.init("");
    try std.testing.expectError(QueryError.EmptyQuery, empty_query.validate());

    // Invalid similarity threshold should fail
    var invalid_threshold_query = SemanticQuery.init("test");
    invalid_threshold_query.similarity_threshold = 1.5;
    try std.testing.expectError(QueryError.InvalidCommand, invalid_threshold_query.validate());
}

test "QueryEngine enhanced statistics" {
    const allocator = std.testing.allocator;
    const simulation_vfs = @import("simulation_vfs");

    // Setup storage engine
    var sim_vfs = simulation_vfs.SimulationVFS.init(allocator);
    defer sim_vfs.deinit();

    const vfs_interface = sim_vfs.vfs();
    const data_dir = try allocator.dupe(u8, "test_stats_v2");

    var storage_engine = try StorageEngine.init(allocator, vfs_interface, data_dir);
    defer storage_engine.deinit();

    try storage_engine.initialize_storage();

    // Setup query engine
    var query_engine = QueryEngine.init(allocator, &storage_engine);
    defer query_engine.deinit();

    // Test initial enhanced statistics
    var stats = query_engine.statistics();
    try std.testing.expectEqual(@as(u64, 0), stats.filtered_queries);
    try std.testing.expectEqual(@as(u64, 0), stats.semantic_queries);

    const test_id = try BlockId.from_hex("3333333333333333333333333333336");
    const test_block = ContextBlock{
        .id = test_id,
        .version = 1,
        .source_uri = "test://uri",
        .metadata_json = "{\"type\":\"test\"}",
        .content = "test content",
    };
    try storage_engine.put_block(test_block);

    // Execute a semantic query (placeholder implementation)
    const semantic_query = SemanticQuery.init("test query");
    const semantic_result = try query_engine.execute_semantic_query(semantic_query);
    defer semantic_result.deinit();

    // Check that statistics were updated
    stats = query_engine.statistics();
    try std.testing.expectEqual(@as(u64, 1), stats.queries_executed);
    try std.testing.expectEqual(@as(u64, 1), stats.semantic_queries);
    try std.testing.expect(stats.total_query_time_ns > 0);
}
