//! Query operations for block lookup and context retrieval.
//!
//! Provides direct ID-based lookups, batch operations, and semantic search
//! with ownership-safe memory management and structured error handling.
//! Coordinates with storage engine through ownership system for safe access.
//!
//! Design rationale: Centralized query operations ensure consistent error
//! handling and memory management across query patterns. Ownership integration
//! prevents cross-subsystem memory access violations during result processing.

const builtin = @import("builtin");
const std = @import("std");

const context_block = @import("../core/types.zig");
const error_context = @import("../core/error_context.zig");
const ownership = @import("../core/ownership.zig");
const storage = @import("../storage/engine.zig");
const assert_mod = @import("../core/assert.zig");

const assert = assert_mod.assert;

const BlockId = context_block.BlockId;
const BlockOwnership = ownership.BlockOwnership;
const ContextBlock = context_block.ContextBlock;
const OwnedQueryEngineBlock = ownership.OwnedQueryEngineBlock;
const StorageEngine = storage.StorageEngine;
const TemporaryBlock = ownership.TemporaryBlock;

/// Basic query operation errors
pub const QueryError = error{
    /// Block not found in storage
    BlockNotFound,
    /// Empty query (no block IDs provided)
    EmptyQuery,
    /// Too many results requested
    TooManyResults,
    /// Query engine not initialized
    NotInitialized,
    /// Invalid semantic query
    InvalidSemanticQuery,
    /// Semantic search not available
    SemanticSearchUnavailable,
} || std.mem.Allocator.Error || storage.StorageError;

/// Query for retrieving blocks by their IDs
pub const FindBlocksQuery = struct {
    /// List of block IDs to retrieve
    block_ids: []const BlockId,

    /// Maximum number of blocks to return in a single query
    pub const MAX_BLOCKS = 1000;

    /// Validate query parameters before execution
    pub fn validate(self: FindBlocksQuery) QueryError!void {
        if (self.block_ids.len == 0) {
            const ctx = error_context.ServerContext{
                .operation = "find_blocks_query_validation",
                .message_size = 0,
            };
            error_context.log_server_error(QueryError.EmptyQuery, ctx);
            return QueryError.EmptyQuery;
        }
        if (self.block_ids.len > MAX_BLOCKS) {
            const ctx = error_context.ServerContext{
                .operation = "find_blocks_query_validation",
                .message_size = self.block_ids.len,
            };
            error_context.log_server_error(QueryError.TooManyResults, ctx);
            return QueryError.TooManyResults;
        }
    }
};

/// Streaming result container for basic block retrieval operations.
/// Uses zero-allocation iterator pattern with arena-managed temporary references.
/// Prevents unbounded memory usage while eliminating per-result allocation overhead.
pub const QueryResult = struct {
    /// Allocator for query infrastructure (not per-result allocation)
    allocator: std.mem.Allocator,
    /// Storage engine iterator for block streaming
    iterator: QueryBlockIterator,
    /// Metadata about the query for consumer information
    total_found: u32,
    consumed_count: u32,
    /// Block IDs slice that needs cleanup (may be null for direct queries)
    owned_block_ids: ?[]BlockId,
    /// Arena for temporary block references - reset periodically
    temp_arena: std.heap.ArenaAllocator,

    /// Create streaming query result from block IDs
    pub fn init(
        allocator: std.mem.Allocator,
        storage_engine: *StorageEngine,
        block_ids: []const BlockId,
    ) QueryResult {
        return QueryResult{
            .allocator = allocator,
            .iterator = QueryBlockIterator.init(storage_engine, block_ids),
            .total_found = @intCast(block_ids.len),
            .consumed_count = 0,
            .owned_block_ids = null, // Caller owns block_ids
            .temp_arena = std.heap.ArenaAllocator.init(allocator),
        };
    }

    /// Create streaming query result with owned block IDs that need cleanup
    pub fn init_with_owned_ids(
        allocator: std.mem.Allocator,
        storage_engine: *StorageEngine,
        owned_block_ids: []BlockId,
    ) QueryResult {
        return QueryResult{
            .allocator = allocator,
            .iterator = QueryBlockIterator.init(storage_engine, owned_block_ids),
            .total_found = @intCast(owned_block_ids.len),
            .consumed_count = 0,
            .owned_block_ids = owned_block_ids,
            .temp_arena = std.heap.ArenaAllocator.init(allocator),
        };
    }

    /// Get next block from the result set. Returns null when exhausted.
    /// Returns zero-cost owned block - no cleanup required.
    /// Block reference valid until next call to next() or deinit().
    pub fn next(self: *QueryResult) QueryError!?OwnedQueryEngineBlock {
        // Reset arena every 100 iterations to prevent unbounded growth
        if (self.consumed_count % 100 == 0 and self.consumed_count > 0) {
            _ = self.temp_arena.reset(.retain_capacity);
        }

        if (try self.iterator.next()) |owned_block| {
            self.consumed_count += 1;
            return owned_block;
        }
        return null;
    }

    /// Check if result set is exhausted
    pub fn is_empty(self: *QueryResult) bool {
        return self.consumed_count >= self.total_found;
    }

    /// Clean up iterator resources, temp arena, and owned block IDs
    pub fn deinit(self: QueryResult) void {
        if (self.owned_block_ids) |block_ids| {
            self.allocator.free(block_ids);
        }
        self.temp_arena.deinit();
        self.iterator.deinit();
    }

    /// Reset iterator to beginning for re-consumption (needed for serialization)
    pub fn reset(self: *QueryResult) void {
        self.iterator.current_index = 0;
        self.consumed_count = 0;
        _ = self.temp_arena.reset(.retain_capacity);
    }

    /// Format results for LLM consumption, streaming blocks as they're consumed
    pub fn format_for_llm(self: *QueryResult, writer: std.io.AnyWriter) anyerror!void {
        try writer.print("Retrieved {} blocks:\n\n", .{self.total_found});

        var block_index: u32 = 1;
        while (try self.next()) |block_ptr| {
            // No defer needed - arena-managed

            try writer.writeAll("--- BEGIN CONTEXT BLOCK ---\n");
            const ctx_block = block_ptr.read(.query_engine);
            try writer.print("Block {} (ID: {}):\n", .{ block_index, ctx_block.id });
            try writer.print("Source: {s}\n", .{ctx_block.source_uri});
            try writer.print("Version: {}\n", .{ctx_block.version});
            try writer.print("Metadata: {s}\n", .{ctx_block.metadata_json});
            try writer.print("Content: {s}\n", .{ctx_block.content});
            try writer.writeAll("--- END CONTEXT BLOCK ---\n\n");

            block_index += 1;
        }
    }
};

/// Iterator for specific block IDs - used by QueryResult for streaming
const QueryBlockIterator = struct {
    storage_engine: *StorageEngine,
    block_ids: []const BlockId,
    current_index: usize,

    fn init(storage_engine: *StorageEngine, block_ids: []const BlockId) QueryBlockIterator {
        return QueryBlockIterator{
            .storage_engine = storage_engine,
            .block_ids = block_ids,
            .current_index = 0,
        };
    }

    fn next(self: *QueryBlockIterator) QueryError!?OwnedQueryEngineBlock {
        while (self.current_index < self.block_ids.len) {
            const block_id = self.block_ids[self.current_index];
            self.current_index += 1;

            if (self.storage_engine.find_block(block_id, .query_engine) catch null) |owned_block| {
                return owned_block;
            }
        }
        return null;
    }

    fn deinit(_: QueryBlockIterator) void {}
};

/// Query for semantic search based on natural language
pub const SemanticQuery = struct {
    query_text: []const u8,
    max_results: u32 = 100,
    similarity_threshold: f32 = 0.7,

    /// Maximum results for semantic queries
    pub const MAX_RESULTS = 500;

    pub fn init(query_text: []const u8) SemanticQuery {
        return SemanticQuery{ .query_text = query_text };
    }

    /// Validate semantic query parameters for correctness and bounds checking
    ///
    /// Checks that query text is non-empty, result limits are reasonable, and similarity thresholds are valid.
    /// Prevents crashes from invalid query parameters.
    pub fn validate(self: SemanticQuery) QueryError!void {
        if (self.query_text.len == 0) {
            const ctx = error_context.ServerContext{
                .operation = "semantic_query_validation",
                .message_size = 0,
            };
            error_context.log_server_error(QueryError.InvalidSemanticQuery, ctx);
            return QueryError.InvalidSemanticQuery;
        }
        if (self.max_results == 0) {
            const ctx = error_context.ServerContext{
                .operation = "semantic_query_validation",
                .message_size = 0,
            };
            error_context.log_server_error(QueryError.EmptyQuery, ctx);
            return QueryError.EmptyQuery;
        }
        if (self.max_results > MAX_RESULTS) {
            const ctx = error_context.ServerContext{
                .operation = "semantic_query_validation",
                .message_size = self.max_results,
            };
            error_context.log_server_error(QueryError.TooManyResults, ctx);
            return QueryError.TooManyResults;
        }
        if (self.similarity_threshold < 0.0 or self.similarity_threshold > 1.0) {
            const ctx = error_context.ServerContext{
                .operation = "semantic_query_validation",
            };
            error_context.log_server_error(QueryError.InvalidSemanticQuery, ctx);
            return QueryError.InvalidSemanticQuery;
        }
    }
};

/// Single result from semantic search with similarity score
pub const SemanticResult = struct {
    block: OwnedQueryEngineBlock,
    similarity_score: f32,
};

/// Result container for semantic search operations
pub const SemanticQueryResult = struct {
    allocator: std.mem.Allocator,
    results: []SemanticResult,
    total_matches: u32,

    pub fn init(
        allocator: std.mem.Allocator,
        results: []const SemanticResult,
        total_matches: u32,
    ) QueryError!SemanticQueryResult {
        const owned_results = try allocator.alloc(SemanticResult, results.len);
        for (results, 0..) |result, i| {
            owned_results[i] = SemanticResult{
                .block = OwnedQueryEngineBlock.init(try clone_block(allocator, result.block.block)),
                .similarity_score = result.similarity_score,
            };
        }

        return SemanticQueryResult{
            .allocator = allocator,
            .results = owned_results,
            .total_matches = total_matches,
        };
    }

    pub fn deinit(self: SemanticQueryResult) void {
        for (self.results) |result| {
            self.allocator.free(result.block.block.source_uri);
            self.allocator.free(result.block.block.metadata_json);
            self.allocator.free(result.block.block.content);
        }
        self.allocator.free(self.results);
    }

    /// Format semantic results for LLM consumption with similarity scores, streaming to writer
    pub fn format_for_llm(self: SemanticQueryResult, writer: std.io.AnyWriter) anyerror!void {
        try writer.print("Found {} semantically similar blocks:\n\n", .{self.total_matches});

        for (self.results, 0..) |search_result, i| {
            try writer.writeAll("--- BEGIN CONTEXT BLOCK ---\n");
            try writer.print("Block {} (ID: {}, Similarity: {d:.3}):\n", .{
                i + 1,
                search_result.block.id,
                search_result.similarity_score,
            });
            try writer.print("Source: {s}\n", .{search_result.block.source_uri});
            try writer.print("Version: {}\n", .{search_result.block.version});
            try writer.print("Metadata: {s}\n", .{search_result.block.metadata_json});
            try writer.print("Content: {s}\n", .{search_result.block.content});
            try writer.writeAll("--- END CONTEXT BLOCK ---\n\n");
        }
    }

    /// Get blocks sorted by similarity score (highest first)
    pub fn sorted_blocks(self: SemanticQueryResult, allocator: std.mem.Allocator) QueryError![]ContextBlock {
        var blocks = try allocator.alloc(ContextBlock, self.results.len);

        const sorted_results = try allocator.dupe(SemanticResult, self.results);
        defer allocator.free(sorted_results);

        std.sort.heap(SemanticResult, sorted_results, {}, semantic_result_less_than);

        for (sorted_results, 0..) |search_result, i| {
            blocks[i] = try clone_block(allocator, search_result.block.block);
        }

        return blocks;
    }
};

/// Execute a basic find_blocks query against storage with streaming results.
/// Returns iterator-based result to prevent unbounded memory usage.
pub fn execute_find_blocks(
    allocator: std.mem.Allocator,
    storage_engine: *StorageEngine,
    query: FindBlocksQuery,
) !QueryResult {
    try query.validate();

    return QueryResult.init(allocator, storage_engine, query.block_ids);
}

/// Execute keyword search query with basic text matching
/// NOTE: This is NOT semantic search - it's simple keyword matching
pub fn execute_keyword_query(
    allocator: std.mem.Allocator,
    storage_engine: *StorageEngine,
    query: SemanticQuery,
) !SemanticQueryResult {
    try query.validate();

    var results = std.array_list.Managed(SemanticResult).init(allocator);
    defer results.deinit();
    try results.ensureTotalCapacity(query.max_results);

    var iterator = storage_engine.iterate_all_blocks();

    var matches_found: u32 = 0;
    while (try iterator.next()) |block| {
        const similarity = calculate_keyword_similarity(block.content, query.query_text);

        if (similarity >= query.similarity_threshold) {
            matches_found += 1;

            if (results.items.len < query.max_results) {
                try results.append(SemanticResult{
                    .block = OwnedQueryEngineBlock.init(block),
                    .similarity_score = similarity,
                });
            }
        }
    }

    // Sort results by similarity score (highest first)
    std.sort.heap(SemanticResult, results.items, {}, semantic_result_less_than);

    return SemanticQueryResult.init(allocator, results.items, matches_found);
}

/// Check if a block exists in storage
pub fn has_block(storage_engine: *StorageEngine, block_id: BlockId) bool {
    const result = storage_engine.find_block(block_id, .query_engine) catch return false;
    return result != null;
}

/// Find a single block by ID - convenience method for single block queries
pub fn find_block(
    allocator: std.mem.Allocator,
    storage_engine: *StorageEngine,
    block_id: BlockId,
) !QueryResult {
    // Create owned copy to prevent use-after-free from temporary array
    const owned_block_ids = try allocator.dupe(BlockId, &[_]BlockId{block_id});
    return QueryResult.init_with_owned_ids(allocator, storage_engine, owned_block_ids);
}

/// Get count of blocks that exist from a list of IDs
pub fn count_existing_blocks(
    storage_engine: *StorageEngine,
    block_ids: []const BlockId,
) usize {
    var count: usize = 0;
    for (block_ids) |block_id| {
        if (has_block(storage_engine, block_id)) {
            count += 1;
        }
    }
    return count;
}

/// Simple keyword similarity calculation using word matching
/// Returns ratio of query words found in content (0.0-1.0)
fn calculate_keyword_similarity(content: []const u8, query: []const u8) f32 {
    if (content.len == 0 or query.len == 0) return 0.0;

    var query_words = std.mem.splitSequence(u8, query, " ");
    var total_query_words: f32 = 0;
    var matching_words: f32 = 0;

    while (query_words.next()) |word| {
        total_query_words += 1;
        if (std.mem.indexOf(u8, content, word) != null) {
            matching_words += 1;
        }
    }

    if (total_query_words == 0) return 0.0;
    return matching_words / total_query_words;
}

/// Comparison function for sorting semantic results by similarity score
fn semantic_result_less_than(context: void, a: SemanticResult, b: SemanticResult) bool {
    _ = context;
    return a.similarity_score > b.similarity_score; // Descending order
}

/// Create a deep copy of a context block for SemanticQuery results
/// QueryResult no longer uses this due to zero-allocation optimization
fn clone_block(allocator: std.mem.Allocator, block: ContextBlock) !ContextBlock {
    return ContextBlock{
        .id = block.id,
        .version = block.version,
        .source_uri = try allocator.dupe(u8, block.source_uri),
        .metadata_json = try allocator.dupe(u8, block.metadata_json),
        .content = try allocator.dupe(u8, block.content),
    };
}

fn free_cloned_block(allocator: std.mem.Allocator, block: ContextBlock) void {
    allocator.free(block.source_uri);
    allocator.free(block.metadata_json);
    allocator.free(block.content);
}
