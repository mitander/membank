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

/// Query execution engine.
pub const QueryEngine = struct {
    allocator: std.mem.Allocator,
    storage_engine: *StorageEngine,
    initialized: bool,

    /// Initialize query engine with storage backend.
    pub fn init(allocator: std.mem.Allocator, storage_engine: *StorageEngine) QueryEngine {
        return QueryEngine{
            .allocator = allocator,
            .storage_engine = storage_engine,
            .initialized = true,
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
        assert(self.initialized);
        if (!self.initialized) return QueryError.NotInitialized;

        try query.validate();

        var results = std.ArrayList(ContextBlock).init(self.allocator);
        defer results.deinit();

        // Retrieve each requested block
        for (query.block_ids) |block_id| {
            const maybe_block = self.storage_engine.get_block_by_id(block_id) catch |err|
                switch (err) {
                    storage.StorageError.BlockNotFound => continue, // Skip missing blocks
                    else => return err,
                };

            // Clone the block to ensure result ownership
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

    /// Get a single block by ID. Convenience method for single block queries.
    pub fn get_block_by_id(self: *QueryEngine, block_id: BlockId) !QueryResult {
        const query = GetBlocksQuery{
            .block_ids = &[_]BlockId{block_id},
        };
        return self.execute_get_blocks(query);
    }

    /// Get query engine statistics.
    pub fn get_statistics(self: *QueryEngine) QueryStatistics {
        return QueryStatistics{
            .total_blocks_stored = self.storage_engine.block_count(),
            .queries_executed = 0, // TODO Add query counting
        };
    }
};

/// Query engine performance statistics.
pub const QueryStatistics = struct {
    /// Total number of blocks in storage
    total_blocks_stored: u32,
    /// Total number of queries executed
    queries_executed: u64,
};

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

    // Create test block
    const test_id = try BlockId.from_hex("0123456789abcdeffedcba9876543210");
    const test_block = ContextBlock{
        .id = test_id,
        .version = 42,
        .source_uri = "git://example.com/repo.git/file.zig#L123",
        .metadata_json = "{\"type\":\"function\",\"language\":\"zig\"}",
        .content = "pub fn test_function() void { return; }",
    };

    // Clone block for result (since QueryResult takes ownership)
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

    var storage_engine = StorageEngine.init(allocator, vfs_interface, data_dir);
    defer storage_engine.deinit();

    try storage_engine.initialize_storage();

    // Setup query engine
    var query_engine = QueryEngine.init(allocator, &storage_engine);
    defer query_engine.deinit();

    // Create and store test block
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
    const result = try query_engine.get_block_by_id(test_id);
    defer result.deinit();

    try std.testing.expectEqual(@as(u32, 1), result.count);
    try std.testing.expect(result.blocks[0].id.eql(test_id));
    try std.testing.expectEqualStrings("test content", result.blocks[0].content);

    // Test missing block
    const missing_id = try BlockId.from_hex("fedcba9876543210123456789abcdef0");
    const missing_result = try query_engine.get_block_by_id(missing_id);
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

    var storage_engine = StorageEngine.init(allocator, vfs_interface, data_dir);
    defer storage_engine.deinit();

    try storage_engine.initialize_storage();

    // Setup query engine
    var query_engine = QueryEngine.init(allocator, &storage_engine);
    defer query_engine.deinit();

    // Create and store multiple test blocks
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

test "QueryEngine statistics" {
    const allocator = std.testing.allocator;
    const simulation_vfs = @import("simulation_vfs");

    // Setup storage engine
    var sim_vfs = simulation_vfs.SimulationVFS.init(allocator);
    defer sim_vfs.deinit();

    const vfs_interface = sim_vfs.vfs();
    const data_dir = try allocator.dupe(u8, "test_data");

    var storage_engine = StorageEngine.init(allocator, vfs_interface, data_dir);
    defer storage_engine.deinit();

    try storage_engine.initialize_storage();

    // Setup query engine
    var query_engine = QueryEngine.init(allocator, &storage_engine);
    defer query_engine.deinit();

    // Test initial statistics
    var stats = query_engine.get_statistics();
    try std.testing.expectEqual(@as(u32, 0), stats.total_blocks_stored);

    // Add a block and verify statistics
    const test_id = try BlockId.from_hex("0123456789abcdeffedcba9876543210");
    const test_block = ContextBlock{
        .id = test_id,
        .version = 1,
        .source_uri = "test://uri",
        .metadata_json = "{\"test\":true}",
        .content = "test content",
    };

    try storage_engine.put_block(test_block);

    stats = query_engine.get_statistics();
    try std.testing.expectEqual(@as(u32, 1), stats.total_blocks_stored);
}
