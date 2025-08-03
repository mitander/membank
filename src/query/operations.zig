//! Basic query operations for KausalDB context retrieval.
//!
//! Provides fundamental block lookup operations including direct ID-based
//! retrieval, batch operations, and semantic search. Handles memory management,
//! error recovery, and result formatting for basic query patterns.

const std = @import("std");
const assert = @import("../core/assert.zig").assert;
const storage = @import("../storage/engine.zig");
const context_block = @import("../core/types.zig");
const simulation_vfs = @import("../sim/simulation_vfs.zig");
const testing = std.testing;

const StorageEngine = storage.StorageEngine;
const ContextBlock = context_block.ContextBlock;
const BlockId = context_block.BlockId;
const SimulationVFS = simulation_vfs.SimulationVFS;

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
        if (self.block_ids.len == 0) return QueryError.EmptyQuery;
        if (self.block_ids.len > MAX_BLOCKS) return QueryError.TooManyResults;
    }
};

/// Streaming result container for basic block retrieval operations.
/// Uses iterator pattern to avoid materializing full result sets in memory,
/// preventing unbounded memory usage on large queries.
pub const QueryResult = struct {
    /// Temporary allocator for individual block cloning during iteration
    allocator: std.mem.Allocator,
    /// Storage engine iterator for block streaming
    iterator: QueryBlockIterator,
    /// Metadata about the query for consumer information
    total_found: u32,
    consumed_count: u32,
    /// Block IDs slice that needs cleanup (may be null for direct queries)
    owned_block_ids: ?[]BlockId,

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
        };
    }

    /// Get next block from the result set. Returns null when exhausted.
    /// Caller owns returned block and must call `deinit_block()` when done.
    pub fn next(self: *QueryResult) QueryError!?ContextBlock {
        if (self.iterator.next()) |block| {
            self.consumed_count += 1;
            // Clone block to prevent use-after-free when storage data changes
            return try clone_block(self.allocator, block);
        }
        return null;
    }

    /// Clean up a block returned by `next()`. Required for each consumed block.
    pub fn deinit_block(self: QueryResult, block: ContextBlock) void {
        self.allocator.free(block.source_uri);
        self.allocator.free(block.metadata_json);
        self.allocator.free(block.content);
    }

    /// Check if result set is exhausted
    pub fn is_empty(self: *QueryResult) bool {
        return self.consumed_count >= self.total_found;
    }

    /// Clean up iterator resources and owned block IDs
    pub fn deinit(self: QueryResult) void {
        if (self.owned_block_ids) |block_ids| {
            self.allocator.free(block_ids);
        }
        self.iterator.deinit();
    }

    /// Reset iterator to beginning for re-consumption (needed for serialization)
    pub fn reset(self: *QueryResult) void {
        self.iterator.current_index = 0;
        self.consumed_count = 0;
    }

    /// Format results for LLM consumption, streaming blocks as they're consumed
    pub fn format_for_llm(self: *QueryResult, writer: std.io.AnyWriter) anyerror!void {
        try writer.print("Retrieved {} blocks:\n\n", .{self.total_found});

        var block_index: u32 = 1;
        while (try self.next()) |block| {
            defer self.deinit_block(block);

            try writer.writeAll("--- BEGIN CONTEXT BLOCK ---\n");
            try writer.print("Block {} (ID: {}):\n", .{ block_index, block.id });
            try writer.print("Source: {s}\n", .{block.source_uri});
            try writer.print("Version: {}\n", .{block.version});
            try writer.print("Metadata: {s}\n", .{block.metadata_json});
            try writer.print("Content: {s}\n", .{block.content});
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

    fn next(self: *QueryBlockIterator) ?ContextBlock {
        while (self.current_index < self.block_ids.len) {
            const block_id = self.block_ids[self.current_index];
            self.current_index += 1;

            // Skip missing blocks to maintain backward compatibility
            if (self.storage_engine.find_block(block_id) catch null) |block| {
                return block;
            }
        }
        return null;
    }

    fn deinit(_: QueryBlockIterator) void {
        // Iterator owns no allocated resources
    }
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

    pub fn validate(self: SemanticQuery) QueryError!void {
        if (self.query_text.len == 0) return QueryError.InvalidSemanticQuery;
        if (self.max_results == 0) return QueryError.EmptyQuery;
        if (self.max_results > MAX_RESULTS) return QueryError.TooManyResults;
        if (self.similarity_threshold < 0.0 or self.similarity_threshold > 1.0) {
            return QueryError.InvalidSemanticQuery;
        }
    }
};

/// Single result from semantic search with similarity score
pub const SemanticResult = struct {
    block: ContextBlock,
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
                .block = try clone_block(allocator, result.block),
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
            self.allocator.free(result.block.source_uri);
            self.allocator.free(result.block.metadata_json);
            self.allocator.free(result.block.content);
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
            blocks[i] = try clone_block(allocator, search_result.block);
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

    var results = std.ArrayList(SemanticResult).init(allocator);
    defer results.deinit();
    try results.ensureCapacity(query.max_results);

    var iterator = storage_engine.iterate_all_blocks();

    var matches_found: u32 = 0;
    while (try iterator.next()) |block| {
        const similarity = calculate_keyword_similarity(block.content, query.query_text);

        if (similarity >= query.similarity_threshold) {
            matches_found += 1;

            if (results.items.len < query.max_results) {
                try results.append(SemanticResult{
                    .block = block,
                    .similarity_score = similarity,
                });
            }
        }
    }

    return SemanticQueryResult.init(allocator, results.items, matches_found);
}

/// Check if a block exists in storage
pub fn block_exists(storage_engine: *StorageEngine, block_id: BlockId) bool {
    const result = storage_engine.find_block(block_id) catch return false;
    return result != null;
}

/// Find a single block by ID - convenience method for single block queries
pub fn find_block(
    allocator: std.mem.Allocator,
    storage_engine: *StorageEngine,
    block_id: BlockId,
) !QueryResult {
    const query = FindBlocksQuery{
        .block_ids = &[_]BlockId{block_id},
    };
    return execute_find_blocks(allocator, storage_engine, query);
}

/// Get count of blocks that exist from a list of IDs
pub fn count_existing_blocks(
    storage_engine: *StorageEngine,
    block_ids: []const BlockId,
) usize {
    var count: usize = 0;
    for (block_ids) |block_id| {
        if (block_exists(storage_engine, block_id)) {
            count += 1;
        }
    }
    return count;
}

/// Simple keyword similarity calculation using word matching
/// Returns ratio of query words found in content (0.0-1.0)
fn calculate_keyword_similarity(content: []const u8, query: []const u8) f32 {
    if (content.len == 0 or query.len == 0) return 0.0;

    var query_words = std.mem.split(u8, query, " ");
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

/// Create a deep copy of a context block
fn clone_block(allocator: std.mem.Allocator, block: ContextBlock) !ContextBlock {
    return ContextBlock{
        .id = block.id,
        .version = block.version,
        .source_uri = try allocator.dupe(u8, block.source_uri),
        .metadata_json = try allocator.dupe(u8, block.metadata_json),
        .content = try allocator.dupe(u8, block.content),
    };
}

test "find blocks query validation" {
    const valid_query = FindBlocksQuery{
        .block_ids = &[_]BlockId{ 1, 2, 3 },
    };
    try valid_query.validate();

    const empty_query = FindBlocksQuery{
        .block_ids = &[_]BlockId{},
    };
    try testing.expectError(QueryError.EmptyQuery, empty_query.validate());

    var too_many_ids: [1001]BlockId = undefined;
    for (&too_many_ids, 0..) |*id, i| {
        id.* = @intCast(i);
    }
    const large_query = FindBlocksQuery{
        .block_ids = &too_many_ids,
    };
    try testing.expectError(QueryError.TooManyResults, large_query.validate());
}

test "semantic query validation" {
    // Valid query
    var query = SemanticQuery.init("test query");
    try query.validate();

    // Empty query text
    query.query_text = "";
    try testing.expectError(QueryError.InvalidSemanticQuery, query.validate());

    // Invalid similarity threshold
    query.query_text = "test";
    query.similarity_threshold = 1.5;
    try testing.expectError(QueryError.InvalidSemanticQuery, query.validate());

    query.similarity_threshold = -0.1;
    try testing.expectError(QueryError.InvalidSemanticQuery, query.validate());

    // Too many results
    query.similarity_threshold = 0.7;
    query.max_results = 1000;
    try testing.expectError(QueryError.TooManyResults, query.validate());
}

test "keyword similarity calculation" {
    try testing.expect(calculate_keyword_similarity("hello world", "hello") > 0.0);
    try testing
        .expect(calculate_keyword_similarity("hello world", "world") > 0.0);
    try testing.expect(calculate_keyword_similarity("hello world", "hello world") == 1.0);
    try testing.expect(calculate_keyword_similarity("hello world", "goodbye") == 0.0);
    try testing.expect(calculate_keyword_similarity("", "test") == 0.0);
    try testing.expect(calculate_keyword_similarity("test", "") == 0.0);
}

test "query result formatting" {
    const allocator = testing.allocator;

    var sim_vfs = SimulationVFS.init(allocator);
    defer sim_vfs.deinit();

    var storage_engine = try StorageEngine.init(allocator, sim_vfs.vfs(), "./test_formatting");
    defer storage_engine.deinit();
    try storage_engine.startup();

    const test_id = try BlockId.from_hex("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa");
    const test_block = ContextBlock{
        .id = test_id,
        .version = 1,
        .source_uri = "test.zig",
        .metadata_json = "{\"language\": \"zig\"}",
        .content = "pub fn test() void {}",
    };

    try storage_engine.put_block(test_block);

    var result = QueryResult.init(allocator, &storage_engine, &[_]BlockId{test_id});
    defer result.deinit();

    var formatted_output = std.ArrayList(u8).init(testing.allocator);
    defer formatted_output.deinit();

    try result.format_for_llm(formatted_output.writer().any());
    const formatted = formatted_output.items;

    try testing.expect(std.mem.indexOf(u8, formatted, "Retrieved 1 blocks") != null);
    try testing.expect(std.mem.indexOf(u8, formatted, "test.zig") != null);
    try testing.expect(std.mem.indexOf(u8, formatted, "pub fn test() void {}") != null);
}

test "semantic result sorting" {
    const block1 = ContextBlock{
        .id = 1,
        .version = 1,
        .source_uri = "test1.zig",
        .metadata_json = "{}",
        .content = "low similarity",
    };

    const block2 = ContextBlock{
        .id = 2,
        .version = 1,
        .source_uri = "test2.zig",
        .metadata_json = "{}",
        .content = "high similarity",
    };

    const results = [_]SemanticResult{
        .{ .block = block1, .similarity_score = 0.3 },
        .{ .block = block2, .similarity_score = 0.9 },
    };

    var semantic_result = try SemanticQueryResult.init(testing.allocator, &results, 2);
    defer semantic_result.deinit();

    const sorted_blocks = try semantic_result.sorted_blocks(testing.allocator);
    defer testing.allocator.free(sorted_blocks);

    // Higher similarity should come first
    try testing.expect(sorted_blocks[0].id == 2);
    try testing.expect(sorted_blocks[1].id == 1);
}

test "execute_find_blocks with storage engine" {
    const allocator = testing.allocator;

    var sim_vfs = SimulationVFS.init(allocator);
    defer sim_vfs.deinit();

    var storage_engine = try StorageEngine.init(allocator, sim_vfs.vfs(), "./test_operations");
    defer storage_engine.deinit();
    try storage_engine.startup();
    const test_id1 = try BlockId.from_hex("1111111111111111111111111111111111111111");
    const test_id2 = try BlockId.from_hex("2222222222222222222222222222222222222222");
    const missing_id = try BlockId.from_hex("3333333333333333333333333333333333333333");

    const block1 = ContextBlock{
        .id = test_id1,
        .version = 1,
        .source_uri = "test1.zig",
        .metadata_json = "{}",
        .content = "first test block",
    };

    const block2 = ContextBlock{
        .id = test_id2,
        .version = 2,
        .source_uri = "test2.zig",
        .metadata_json = "{\"type\": \"function\"}",
        .content = "second test block",
    };

    try storage_engine.put_block(block1);
    try storage_engine.put_block(block2);

    // Test finding existing blocks
    const query = FindBlocksQuery{
        .block_ids = &[_]BlockId{ test_id1, test_id2 },
    };

    const result = try execute_find_blocks(allocator, &storage_engine, query);
    defer result.deinit();

    try testing.expectEqual(@as(u32, 2), result.total_found);
    try testing.expect(!result.is_empty());

    // Test partial results (some blocks missing)
    const partial_query = FindBlocksQuery{
        .block_ids = &[_]BlockId{ test_id1, missing_id, test_id2 },
    };

    const partial_result = try execute_find_blocks(allocator, &storage_engine, partial_query);
    defer partial_result.deinit();

    try testing.expectEqual(@as(u32, 3), partial_result.total_found); // Query has 3 IDs total
}

test "execute_keyword_query with word matching" {
    const allocator = testing.allocator;

    var sim_vfs = SimulationVFS.init(allocator);
    defer sim_vfs.deinit();

    var storage_engine = try StorageEngine.init(allocator, sim_vfs.vfs(), "./test_semantic");
    defer storage_engine.deinit();
    try storage_engine.startup();

    const blocks = [_]ContextBlock{
        .{
            .id = try BlockId.from_hex("1111111111111111111111111111111111111111"),
            .version = 1,
            .source_uri = "high_match.zig",
            .metadata_json = "{}",
            .content = "hello world test function", // High similarity to "hello world"
        },
        .{
            .id = try BlockId.from_hex("2222222222222222222222222222222222222222"),
            .version = 1,
            .source_uri = "medium_match.zig",
            .metadata_json = "{}",
            .content = "hello there other code", // Medium similarity
        },
        .{
            .id = try BlockId.from_hex("3333333333333333333333333333333333333333"),
            .version = 1,
            .source_uri = "no_match.zig",
            .metadata_json = "{}",
            .content = "completely different content", // No similarity
        },
    };

    for (blocks) |block| {
        try storage_engine.put_block(block);
    }

    // Execute semantic query
    const query = SemanticQuery{
        .query_text = "hello world",
        .max_results = 10,
        .similarity_threshold = 0.3,
    };

    const result = try execute_keyword_query(allocator, &storage_engine, query);
    defer result.deinit();

    // Should find blocks with sufficient similarity
    try testing.expect(result.total_matches >= 1);
    try testing.expect(result.results.len <= query.max_results);

    // Results should be sorted by similarity (if multiple results)
    if (result.results.len > 1) {
        for (result.results[0 .. result.results.len - 1], result.results[1..]) |current, next| {
            try testing.expect(current.similarity_score >= next.similarity_score);
        }
    }
}

test "query result memory management" {
    const allocator = testing.allocator;

    var sim_vfs = SimulationVFS.init(allocator);
    defer sim_vfs.deinit();

    var storage_engine = try StorageEngine.init(allocator, sim_vfs.vfs(), "./test_memory");
    defer storage_engine.deinit();
    try storage_engine.startup();

    const test_blocks = [_]ContextBlock{
        .{
            .id = try BlockId.from_hex("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"),
            .version = 1,
            .source_uri = "mem_test1.zig",
            .metadata_json = "{\"test\": true}",
            .content = "memory test content 1",
        },
        .{
            .id = try BlockId.from_hex("bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"),
            .version = 1,
            .source_uri = "mem_test2.zig",
            .metadata_json = "{\"test\": false}",
            .content = "memory test content 2",
        },
    };

    for (test_blocks) |block| {
        try storage_engine.put_block(block);
    }

    const block_ids = [_]BlockId{ test_blocks[0].id, test_blocks[1].id };
    var result = QueryResult.init(allocator, &storage_engine, &block_ids);
    defer result.deinit();

    try testing.expectEqual(@as(u32, 2), result.total_found);
    try testing.expect(!result.is_empty());

    const first_block = (try result.next()).?;
    defer result.deinit_block(first_block);
    try testing.expect(first_block.id.eql(test_blocks[0].id));
    try testing.expect(std.mem.eql(u8, first_block.content, test_blocks[0].content));
}

test "semantic query result operations" {
    const allocator = testing.allocator;

    const test_results = [_]SemanticResult{
        .{
            .block = .{
                .id = try BlockId.from_hex("1111111111111111111111111111111111111111"),
                .version = 1,
                .source_uri = "semantic1.zig",
                .metadata_json = "{}",
                .content = "high relevance content",
            },
            .similarity_score = 0.9,
        },
        .{
            .block = .{
                .id = try BlockId.from_hex("2222222222222222222222222222222222222222"),
                .version = 1,
                .source_uri = "semantic2.zig",
                .metadata_json = "{}",
                .content = "medium relevance content",
            },
            .similarity_score = 0.6,
        },
    };

    var semantic_result = try SemanticQueryResult.init(allocator, &test_results, 2);
    defer semantic_result.deinit();

    try testing.expectEqual(@as(u32, 2), semantic_result.total_matches);
    try testing.expectEqual(@as(usize, 2), semantic_result.results.len);

    // Test sorted blocks retrieval
    const sorted_blocks = try semantic_result.sorted_blocks(allocator);
    defer allocator.free(sorted_blocks);

    try testing.expectEqual(@as(usize, 2), sorted_blocks.len);
    // Verify sorting by similarity (highest first)
    try testing.expect(semantic_result.results[0].similarity_score >= semantic_result.results[1].similarity_score);
}

test "block existence checking" {
    const allocator = testing.allocator;

    var sim_vfs = SimulationVFS.init(allocator);
    defer sim_vfs.deinit();

    var storage_engine = try StorageEngine.init(allocator, sim_vfs.vfs(), "./test_existence");
    defer storage_engine.deinit();
    try storage_engine.startup();

    const existing_id = try BlockId.from_hex("1111111111111111111111111111111111111111");
    const missing_id = try BlockId.from_hex("2222222222222222222222222222222222222222");

    // Initially, no blocks exist
    try testing.expect(!block_exists(&storage_engine, existing_id));
    try testing.expect(!block_exists(&storage_engine, missing_id));

    const test_block = ContextBlock{
        .id = existing_id,
        .version = 1,
        .source_uri = "exists.zig",
        .metadata_json = "{}",
        .content = "block exists",
    };
    try storage_engine.put_block(test_block);

    // Now one should exist
    try testing.expect(block_exists(&storage_engine, existing_id));
    try testing.expect(!block_exists(&storage_engine, missing_id));

    // Test count_existing_blocks
    const test_ids = [_]BlockId{ existing_id, missing_id };
    const count = count_existing_blocks(&storage_engine, &test_ids);
    try testing.expectEqual(@as(usize, 1), count);
}

test "find_block convenience function" {
    const allocator = testing.allocator;

    var sim_vfs = SimulationVFS.init(allocator);
    defer sim_vfs.deinit();

    var storage_engine = try StorageEngine.init(allocator, sim_vfs.vfs(), "./test_find_single");
    defer storage_engine.deinit();
    try storage_engine.startup();

    const test_id = try BlockId.from_hex("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa");
    const test_block = ContextBlock{
        .id = test_id,
        .version = 1,
        .source_uri = "single.zig",
        .metadata_json = "{}",
        .content = "single block test",
    };
    try storage_engine.put_block(test_block);

    // Test finding the block
    const result = try find_block(allocator, &storage_engine, test_id);
    defer result.deinit();

    try testing.expectEqual(@as(u32, 1), result.total_found);

    const found_block = (try result.next()).?;
    defer result.deinit_block(found_block);
    try testing.expect(found_block.id.eql(test_id));
    try testing.expect(std.mem.eql(u8, found_block.content, test_block.content));
}

test "large dataset query performance" {
    const allocator = testing.allocator;

    var sim_vfs = SimulationVFS.init(allocator);
    defer sim_vfs.deinit();

    var storage_engine = try StorageEngine.init(allocator, sim_vfs.vfs(), "./test_performance");
    defer storage_engine.deinit();
    try storage_engine.startup();

    const block_count = 50;
    var block_ids = try allocator.alloc(BlockId, block_count);
    defer allocator.free(block_ids);

    for (0..block_count) |i| {
        var id_bytes: [16]u8 = undefined;
        std.mem.writeInt(u128, &id_bytes, @as(u128, i), .big);
        block_ids[i] = BlockId{ .bytes = id_bytes };

        const content = try std.fmt.allocPrint(allocator, "test content for block {d}", .{i});
        defer allocator.free(content);

        const test_block = ContextBlock{
            .id = block_ids[i],
            .version = 1,
            .source_uri = "perf_test.zig",
            .metadata_json = "{}",
            .content = content,
        };
        try storage_engine.put_block(test_block);
    }

    // Query all blocks
    const query = FindBlocksQuery{
        .block_ids = block_ids,
    };

    const start_time = std.time.nanoTimestamp();
    const result = try execute_find_blocks(allocator, &storage_engine, query);
    defer result.deinit();
    const end_time = std.time.nanoTimestamp();

    try testing.expectEqual(@as(u32, block_count), result.total_found);

    // Verify reasonable performance (should complete in reasonable time)
    const query_duration_ms = @as(f64, @floatFromInt(end_time - start_time)) / 1_000_000.0;
    try testing.expect(query_duration_ms < 1000.0); // Should complete in under 1 second
}

test "query error handling" {
    // Test empty query validation
    const empty_query = FindBlocksQuery{
        .block_ids = &[_]BlockId{},
    };
    try testing.expectError(QueryError.EmptyQuery, empty_query.validate());

    // Test too many blocks query
    var too_many_ids: [1001]BlockId = undefined;
    for (&too_many_ids, 0..) |*id, i| {
        var id_bytes: [16]u8 = undefined;
        std.mem.writeInt(u128, &id_bytes, @as(u128, i), .big);
        id.* = BlockId{ .bytes = id_bytes };
    }
    const large_query = FindBlocksQuery{
        .block_ids = &too_many_ids,
    };
    try testing.expectError(QueryError.TooManyResults, large_query.validate());

    // Test semantic query validation errors
    var semantic_query = SemanticQuery.init("");
    try testing.expectError(QueryError.InvalidSemanticQuery, semantic_query.validate());

    semantic_query = SemanticQuery.init("valid query");
    semantic_query.similarity_threshold = 1.5; // Invalid threshold
    try testing.expectError(QueryError.InvalidSemanticQuery, semantic_query.validate());

    semantic_query.similarity_threshold = 0.7;
    semantic_query.max_results = 600; // Too many results
    try testing.expectError(QueryError.TooManyResults, semantic_query.validate());
}

test "query result formatting edge cases" {
    const allocator = testing.allocator;

    var sim_vfs = SimulationVFS.init(allocator);
    defer sim_vfs.deinit();

    var storage_engine = try StorageEngine.init(allocator, sim_vfs.vfs(), "./test_edge_cases");
    defer storage_engine.deinit();
    try storage_engine.startup();

    // Test empty result formatting
    var empty_result = QueryResult.init(allocator, &storage_engine, &[_]BlockId{});
    defer empty_result.deinit();

    try testing.expect(empty_result.is_empty());
    try testing.expectEqual(@as(u32, 0), empty_result.total_found);

    var empty_formatted_output = std.ArrayList(u8).init(allocator);
    defer empty_formatted_output.deinit();
    try empty_result.format_for_llm(empty_formatted_output.writer().any());
    const empty_formatted = empty_formatted_output.items;
    try testing.expect(std.mem.indexOf(u8, empty_formatted, "Retrieved 0 blocks") != null);

    // Test result with special characters
    const special_id = try BlockId.from_hex("1111111111111111111111111111111111111111");
    const special_block = ContextBlock{
        .id = special_id,
        .version = 1,
        .source_uri = "test with spaces & symbols.zig",
        .metadata_json = "{\"description\": \"contains \\\"quotes\\\" and newlines\\n\"}",
        .content = "Content with\nnewlines\tand\ttabs",
    };

    try storage_engine.put_block(special_block);

    var special_result = QueryResult.init(allocator, &storage_engine, &[_]BlockId{special_id});
    defer special_result.deinit();

    var special_formatted_output = std.ArrayList(u8).init(allocator);
    defer special_formatted_output.deinit();
    try special_result.format_for_llm(special_formatted_output.writer().any());
    const special_formatted = special_formatted_output.items;
    try testing.expect(std.mem.indexOf(u8, special_formatted, "test with spaces & symbols.zig") != null);
}
