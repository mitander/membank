//! Basic query operations for CortexDB context retrieval.
//!
//! Provides fundamental block lookup operations including direct ID-based
//! retrieval, batch operations, and semantic search. Handles memory management,
//! error recovery, and result formatting for basic query patterns.

const std = @import("std");
const assert = @import("../core/assert.zig").assert;
const storage = @import("../storage/engine.zig");
const context_block = @import("../core/types.zig");

const StorageEngine = storage.StorageEngine;
const ContextBlock = context_block.ContextBlock;
const BlockId = context_block.BlockId;

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
    pub fn validate(self: FindBlocksQuery) !void {
        if (self.block_ids.len == 0) return QueryError.EmptyQuery;
        if (self.block_ids.len > MAX_BLOCKS) return QueryError.TooManyResults;
    }
};

/// Result container for basic block retrieval operations
pub const QueryResult = struct {
    allocator: std.mem.Allocator,
    blocks: []ContextBlock,
    count: u32,

    /// Initialize query result with owned block data
    pub fn init(allocator: std.mem.Allocator, blocks: []const ContextBlock) !QueryResult {
        const owned_blocks = try allocator.alloc(ContextBlock, blocks.len);
        for (blocks, 0..) |block, i| {
            owned_blocks[i] = try clone_block(allocator, block);
        }

        return QueryResult{
            .allocator = allocator,
            .blocks = owned_blocks,
            .count = @intCast(blocks.len),
        };
    }

    /// Clean up allocated block data
    pub fn deinit(self: QueryResult) void {
        for (self.blocks) |block| {
            self.allocator.free(block.source_uri);
            self.allocator.free(block.metadata_json);
            self.allocator.free(block.content);
        }
        self.allocator.free(self.blocks);
    }

    /// Check if result set is empty
    pub fn is_empty(self: QueryResult) bool {
        return self.blocks.len == 0;
    }

    /// Format results for LLM consumption with block count
    pub fn format_for_llm(self: QueryResult, allocator: std.mem.Allocator) ![]const u8 {
        var result = std.ArrayList(u8).init(allocator);
        defer result.deinit();

        const writer = result.writer();
        try writer.print("Retrieved {} blocks:\n\n", .{self.count});

        for (self.blocks, 0..) |block, i| {
            try writer.writeAll("--- BEGIN CONTEXT BLOCK ---\n");
            try writer.print("Block {} (ID: {}):\n", .{ i + 1, block.id });
            try writer.print("Source: {s}\n", .{block.source_uri});
            try writer.print("Version: {}\n", .{block.version});
            try writer.print("Metadata: {s}\n", .{block.metadata_json});
            try writer.print("Content: {s}\n", .{block.content});
            try writer.writeAll("--- END CONTEXT BLOCK ---\n\n");
        }

        return result.toOwnedSlice();
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

    pub fn validate(self: SemanticQuery) !void {
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

    pub fn init(allocator: std.mem.Allocator, results: []const SemanticResult, total_matches: u32) !SemanticQueryResult {
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

    /// Format semantic results for LLM consumption with similarity scores
    pub fn format_for_llm(self: SemanticQueryResult, allocator: std.mem.Allocator) ![]const u8 {
        var result = std.ArrayList(u8).init(allocator);
        defer result.deinit();

        const writer = result.writer();
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

        return result.toOwnedSlice();
    }

    /// Get blocks sorted by similarity score (highest first)
    pub fn sorted_blocks(self: SemanticQueryResult, allocator: std.mem.Allocator) ![]ContextBlock {
        var blocks = try allocator.alloc(ContextBlock, self.results.len);

        // Sort results by similarity score in descending order
        const sorted_results = try allocator.dupe(SemanticResult, self.results);
        defer allocator.free(sorted_results);

        std.sort.heap(SemanticResult, sorted_results, {}, semantic_result_less_than);

        for (sorted_results, 0..) |search_result, i| {
            blocks[i] = try clone_block(allocator, search_result.block);
        }

        return blocks;
    }
};

/// Execute a basic find_blocks query against storage
pub fn execute_find_blocks(
    allocator: std.mem.Allocator,
    storage_engine: *StorageEngine,
    query: FindBlocksQuery,
) !QueryResult {
    try query.validate();

    var found_blocks = std.ArrayList(ContextBlock).init(allocator);
    defer found_blocks.deinit();

    for (query.block_ids) |block_id| {
        if (try storage_engine.find_block(block_id)) |block| {
            try found_blocks.append(block);
        }
        // If null, block not found - continue to next block
    }

    return QueryResult.init(allocator, found_blocks.items);
}

/// Execute semantic search query (placeholder implementation)
pub fn execute_semantic_query(
    allocator: std.mem.Allocator,
    storage_engine: *StorageEngine,
    query: SemanticQuery,
) !SemanticQueryResult {
    try query.validate();

    // TODO: Implement actual semantic search with embeddings
    // For now, return basic text matching with mock similarity scores
    var results = std.ArrayList(SemanticResult).init(allocator);
    defer results.deinit();

    var iterator = storage_engine.iterate_all_blocks();

    var matches_found: u32 = 0;
    while (try iterator.next()) |block| {
        const similarity = calculate_text_similarity(block.content, query.query_text);

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

/// Simple text similarity calculation (placeholder for proper semantic search)
fn calculate_text_similarity(content: []const u8, query: []const u8) f32 {
    if (content.len == 0 or query.len == 0) return 0.0;

    // Simple word overlap ratio as placeholder for embeddings
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

// Tests
const testing = std.testing;

test "find blocks query validation" {
    // Valid query
    const valid_query = FindBlocksQuery{
        .block_ids = &[_]BlockId{ 1, 2, 3 },
    };
    try valid_query.validate();

    // Empty query
    const empty_query = FindBlocksQuery{
        .block_ids = &[_]BlockId{},
    };
    try testing.expectError(QueryError.EmptyQuery, empty_query.validate());

    // Too many blocks
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

test "text similarity calculation" {
    try testing.expect(calculate_text_similarity("hello world", "hello") > 0.0);
    try testing
        .expect(calculate_text_similarity("hello world", "world") > 0.0);
    try testing.expect(calculate_text_similarity("hello world", "hello world") == 1.0);
    try testing.expect(calculate_text_similarity("hello world", "goodbye") == 0.0);
    try testing.expect(calculate_text_similarity("", "test") == 0.0);
    try testing.expect(calculate_text_similarity("test", "") == 0.0);
}

test "query result formatting" {
    const allocator = testing.allocator;

    const test_block = ContextBlock{
        .id = 42,
        .version = 1,
        .source_uri = "test.zig",
        .metadata_json = "{\"language\": \"zig\"}",
        .content = "pub fn test() void {}",
    };

    var result = try QueryResult.init(allocator, &[_]ContextBlock{test_block});
    defer result.deinit();

    const formatted = try result.format_for_llm(allocator);
    defer allocator.free(formatted);

    try testing.expect(std.mem.indexOf(u8, formatted, "Retrieved 1 blocks") != null);
    try testing.expect(std.mem.indexOf(u8, formatted, "test.zig") != null);
    try testing.expect(std.mem.indexOf(u8, formatted, "pub fn test() void {}") != null);
}

test "semantic result sorting" {
    const allocator = testing.allocator;

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

    var semantic_result = try SemanticQueryResult.init(allocator, &results, 2);
    defer semantic_result.deinit();

    const sorted_blocks = try semantic_result.sorted_blocks(allocator);
    defer allocator.free(sorted_blocks);

    // Higher similarity should come first
    try testing.expect(sorted_blocks[0].id == 2);
    try testing.expect(sorted_blocks[1].id == 1);
}

test "execute_find_blocks with storage engine" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const allocator = arena.allocator();

    // Create test storage engine
    const simulation_vfs = @import("../sim/simulation_vfs.zig");
    var sim_vfs = simulation_vfs.SimulationVFS.init(allocator);
    defer sim_vfs.deinit();

    var storage_engine = try storage.StorageEngine.init(allocator, sim_vfs.vfs(), "./test_operations");
    defer storage_engine.deinit();
    try storage_engine.initialize_storage();

    // Add test blocks
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

    try testing.expectEqual(@as(u32, 2), result.count);
    try testing.expect(!result.is_empty());

    // Test partial results (some blocks missing)
    const partial_query = FindBlocksQuery{
        .block_ids = &[_]BlockId{ test_id1, missing_id, test_id2 },
    };

    const partial_result = try execute_find_blocks(allocator, &storage_engine, partial_query);
    defer partial_result.deinit();

    try testing.expectEqual(@as(u32, 2), partial_result.count); // Only found blocks returned
}

test "execute_semantic_query with mock similarity" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const allocator = arena.allocator();

    // Create test storage engine
    const simulation_vfs = @import("../sim/simulation_vfs.zig");
    var sim_vfs = simulation_vfs.SimulationVFS.init(allocator);
    defer sim_vfs.deinit();

    var storage_engine = try storage.StorageEngine.init(allocator, sim_vfs.vfs(), "./test_semantic");
    defer storage_engine.deinit();
    try storage_engine.initialize_storage();

    // Add test blocks with varying similarity to query
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

    const result = try execute_semantic_query(allocator, &storage_engine, query);
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
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const allocator = arena.allocator();

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

    // Test QueryResult creation and cleanup
    var result = try QueryResult.init(allocator, &test_blocks);
    defer result.deinit();

    try testing.expectEqual(@as(u32, 2), result.count);
    try testing.expect(!result.is_empty());

    // Verify blocks were properly cloned
    try testing.expect(result.blocks[0].id.eql(test_blocks[0].id));
    try testing.expect(std.mem.eql(u8, result.blocks[0].content, test_blocks[0].content));
}

test "semantic query result operations" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const allocator = arena.allocator();

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
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const allocator = arena.allocator();

    // Create test storage engine
    const simulation_vfs = @import("../sim/simulation_vfs.zig");
    var sim_vfs = simulation_vfs.SimulationVFS.init(allocator);
    defer sim_vfs.deinit();

    var storage_engine = try storage.StorageEngine.init(allocator, sim_vfs.vfs(), "./test_existence");
    defer storage_engine.deinit();
    try storage_engine.initialize_storage();

    const existing_id = try BlockId.from_hex("1111111111111111111111111111111111111111");
    const missing_id = try BlockId.from_hex("2222222222222222222222222222222222222222");

    // Initially, no blocks exist
    try testing.expect(!block_exists(&storage_engine, existing_id));
    try testing.expect(!block_exists(&storage_engine, missing_id));

    // Add a block
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
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const allocator = arena.allocator();

    // Create test storage engine
    const simulation_vfs = @import("../sim/simulation_vfs.zig");
    var sim_vfs = simulation_vfs.SimulationVFS.init(allocator);
    defer sim_vfs.deinit();

    var storage_engine = try storage.StorageEngine.init(allocator, sim_vfs.vfs(), "./test_find_single");
    defer storage_engine.deinit();
    try storage_engine.initialize_storage();

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

    try testing.expectEqual(@as(u32, 1), result.count);
    try testing.expect(result.blocks[0].id.eql(test_id));
    try testing.expect(std.mem.eql(u8, result.blocks[0].content, test_block.content));
}

test "large dataset query performance" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const allocator = arena.allocator();

    // Create test storage engine
    const simulation_vfs = @import("../sim/simulation_vfs.zig");
    var sim_vfs = simulation_vfs.SimulationVFS.init(allocator);
    defer sim_vfs.deinit();

    var storage_engine = try storage.StorageEngine.init(allocator, sim_vfs.vfs(), "./test_performance");
    defer storage_engine.deinit();
    try storage_engine.initialize_storage();

    // Add multiple blocks
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

    try testing.expectEqual(@as(u32, block_count), result.count);

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
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const allocator = arena.allocator();

    // Test empty result formatting
    const empty_result = try QueryResult.init(allocator, &[_]ContextBlock{});
    defer empty_result.deinit();

    try testing.expect(empty_result.is_empty());
    try testing.expectEqual(@as(u32, 0), empty_result.count);

    const empty_formatted = try empty_result.format_for_llm(allocator);
    defer allocator.free(empty_formatted);
    try testing.expect(std.mem.indexOf(u8, empty_formatted, "Retrieved 0 blocks") != null);

    // Test result with special characters
    const special_block = ContextBlock{
        .id = try BlockId.from_hex("1111111111111111111111111111111111111111"),
        .version = 1,
        .source_uri = "test with spaces & symbols.zig",
        .metadata_json = "{\"description\": \"contains \\\"quotes\\\" and newlines\\n\"}",
        .content = "Content with\nnewlines\tand\ttabs",
    };

    const special_result = try QueryResult.init(allocator, &[_]ContextBlock{special_block});
    defer special_result.deinit();

    const special_formatted = try special_result.format_for_llm(allocator);
    defer allocator.free(special_formatted);
    try testing.expect(std.mem.indexOf(u8, special_formatted, "test with spaces & symbols.zig") != null);
}
