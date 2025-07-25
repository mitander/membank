//! Query engine coordination and public interface for CortexDB.
//!
//! Provides the main QueryEngine struct that coordinates between different
//! query operation modules. Handles metrics, statistics, and provides a
//! unified public API for all query types.

const std = @import("std");
const assert = @import("../core/assert.zig").assert;
const storage = @import("../storage/storage.zig");
const context_block = @import("../core/types.zig");
const operations = @import("operations.zig");
const traversal = @import("traversal.zig");
const filtering = @import("filtering.zig");

const StorageEngine = storage.StorageEngine;
const ContextBlock = context_block.ContextBlock;
const BlockId = context_block.BlockId;

// Re-export basic operations
pub const QueryError = operations.QueryError;
pub const FindBlocksQuery = operations.FindBlocksQuery;
pub const QueryResult = operations.QueryResult;

// Re-export traversal types
pub const TraversalQuery = traversal.TraversalQuery;
pub const TraversalResult = traversal.TraversalResult;
pub const TraversalDirection = traversal.TraversalDirection;
pub const TraversalAlgorithm = traversal.TraversalAlgorithm;

// Re-export semantic search types
pub const SemanticQuery = operations.SemanticQuery;
pub const SemanticQueryResult = operations.SemanticQueryResult;
pub const SemanticResult = operations.SemanticResult;

// Re-export filtering types
pub const FilteredQuery = filtering.FilteredQuery;
pub const FilteredQueryResult = filtering.FilteredQueryResult;
pub const FilterCondition = filtering.FilterCondition;
pub const FilterExpression = filtering.FilterExpression;
pub const FilterOperator = filtering.FilterOperator;
pub const FilterTarget = filtering.FilterTarget;

/// Query engine errors
const EngineError = error{
    /// Query engine not initialized
    NotInitialized,
} || operations.QueryError || filtering.FilterError;

/// Query execution statistics
pub const QueryStatistics = struct {
    total_blocks_stored: u32,
    queries_executed: u64,
    find_blocks_queries: u64,
    traversal_queries: u64,
    filtered_queries: u64,
    semantic_queries: u64,
    total_query_time_ns: u64,

    /// Initialize empty statistics
    pub fn init() QueryStatistics {
        return QueryStatistics{
            .total_blocks_stored = 0,
            .queries_executed = 0,
            .find_blocks_queries = 0,
            .traversal_queries = 0,
            .filtered_queries = 0,
            .semantic_queries = 0,
            .total_query_time_ns = 0,
        };
    }

    /// Calculate average query latency in nanoseconds
    pub fn average_query_latency_ns(self: *const QueryStatistics) u64 {
        if (self.queries_executed == 0) return 0;
        return self.total_query_time_ns / self.queries_executed;
    }

    /// Calculate queries per second based on total execution time
    pub fn queries_per_second(self: *const QueryStatistics) f64 {
        if (self.total_query_time_ns == 0) return 0.0;
        const seconds = @as(f64, @floatFromInt(self.total_query_time_ns)) / 1_000_000_000.0;
        return @as(f64, @floatFromInt(self.queries_executed)) / seconds;
    }
};

/// Main query engine that coordinates all query operations
pub const QueryEngine = struct {
    allocator: std.mem.Allocator,
    storage_engine: *StorageEngine,
    initialized: bool,

    // Thread-safe metrics using atomic operations
    queries_executed: std.atomic.Value(u64),
    find_blocks_queries: std.atomic.Value(u64),
    traversal_queries: std.atomic.Value(u64),
    filtered_queries: std.atomic.Value(u64),
    semantic_queries: std.atomic.Value(u64),
    total_query_time_ns: std.atomic.Value(u64),

    /// Initialize query engine with storage backend
    pub fn init(allocator: std.mem.Allocator, storage_engine: *StorageEngine) QueryEngine {
        return QueryEngine{
            .allocator = allocator,
            .storage_engine = storage_engine,
            .initialized = true,
            .queries_executed = std.atomic.Value(u64).init(0),
            .find_blocks_queries = std.atomic.Value(u64).init(0),
            .traversal_queries = std.atomic.Value(u64).init(0),
            .filtered_queries = std.atomic.Value(u64).init(0),
            .semantic_queries = std.atomic.Value(u64).init(0),
            .total_query_time_ns = std.atomic.Value(u64).init(0),
        };
    }

    /// Clean up query engine resources
    pub fn deinit(self: *QueryEngine) void {
        self.initialized = false;
    }

    /// Execute a FindBlocks query to retrieve blocks by ID
    pub fn execute_find_blocks(self: *QueryEngine, query: FindBlocksQuery) !QueryResult {
        const start_time = std.time.nanoTimestamp();
        defer self.record_find_blocks_query(start_time);

        assert(self.initialized);
        if (!self.initialized) return EngineError.NotInitialized;

        return operations.execute_find_blocks(
            self.allocator,
            self.storage_engine,
            query,
        );
    }

    /// Find a single block by ID - convenience method for single block queries
    pub fn find_block_by_id(self: *QueryEngine, block_id: BlockId) !QueryResult {
        const query = FindBlocksQuery{
            .block_ids = &[_]BlockId{block_id},
        };
        return self.execute_find_blocks(query);
    }

    /// Check if a block exists without retrieving its content
    pub fn block_exists(self: *QueryEngine, block_id: BlockId) bool {
        assert(self.initialized);
        return operations.block_exists(self.storage_engine, block_id);
    }

    /// Execute a graph traversal query
    pub fn execute_traversal(self: *QueryEngine, query: TraversalQuery) !TraversalResult {
        const start_time = std.time.nanoTimestamp();
        defer self.record_traversal_query(start_time);

        assert(self.initialized);
        if (!self.initialized) return EngineError.NotInitialized;

        return traversal.execute_traversal(
            self.allocator,
            self.storage_engine,
            query,
        );
    }

    /// Convenience method for outgoing traversal
    pub fn traverse_outgoing(self: *QueryEngine, start_id: BlockId, max_depth: u32) !TraversalResult {
        return traversal.traverse_outgoing(
            self.allocator,
            self.storage_engine,
            start_id,
            max_depth,
        );
    }

    /// Convenience method for incoming traversal
    pub fn traverse_incoming(self: *QueryEngine, start_id: BlockId, max_depth: u32) !TraversalResult {
        return traversal.traverse_incoming(
            self.allocator,
            self.storage_engine,
            start_id,
            max_depth,
        );
    }

    /// Convenience method for bidirectional traversal
    pub fn traverse_bidirectional(self: *QueryEngine, start_id: BlockId, max_depth: u32) !TraversalResult {
        return traversal.traverse_bidirectional(
            self.allocator,
            self.storage_engine,
            start_id,
            max_depth,
        );
    }

    /// Get current query execution statistics
    pub fn statistics(self: *const QueryEngine) QueryStatistics {
        return QueryStatistics{
            .total_blocks_stored = self.storage_engine.block_count(),
            .queries_executed = self.queries_executed.load(.monotonic),
            .find_blocks_queries = self.find_blocks_queries.load(.monotonic),
            .traversal_queries = self.traversal_queries.load(.monotonic),
            .filtered_queries = self.filtered_queries.load(.monotonic),
            .semantic_queries = self.semantic_queries.load(.monotonic),
            .total_query_time_ns = self.total_query_time_ns.load(.monotonic),
        };
    }

    /// Reset all query statistics to zero
    pub fn reset_statistics(self: *QueryEngine) void {
        self.queries_executed.store(0, .monotonic);
        self.find_blocks_queries.store(0, .monotonic);
        self.traversal_queries.store(0, .monotonic);
        self.filtered_queries.store(0, .monotonic);
        self.semantic_queries.store(0, .monotonic);
        self.total_query_time_ns.store(0, .monotonic);
    }

    /// Record metrics for a find_blocks query execution
    fn record_find_blocks_query(self: *QueryEngine, start_time: i128) void {
        const duration = @as(u64, @intCast(std.time.nanoTimestamp() - start_time));
        _ = self.queries_executed.fetchAdd(1, .monotonic);
        _ = self.find_blocks_queries.fetchAdd(1, .monotonic);
        _ = self.total_query_time_ns.fetchAdd(duration, .monotonic);
    }

    /// Record metrics for a traversal query execution
    fn record_traversal_query(self: *QueryEngine, start_time: i128) void {
        const duration = @as(u64, @intCast(std.time.nanoTimestamp() - start_time));
        _ = self.queries_executed.fetchAdd(1, .monotonic);
        _ = self.traversal_queries.fetchAdd(1, .monotonic);
        _ = self.total_query_time_ns.fetchAdd(duration, .monotonic);
    }

    /// Record metrics for a filtered query execution
    fn record_filtered_query(self: *QueryEngine, start_time: i128) void {
        const duration = @as(u64, @intCast(std.time.nanoTimestamp() - start_time));
        _ = self.queries_executed.fetchAdd(1, .monotonic);
        _ = self.filtered_queries.fetchAdd(1, .monotonic);
        _ = self.total_query_time_ns.fetchAdd(duration, .monotonic);
    }

    /// Record metrics for a semantic query execution
    fn record_semantic_query(self: *QueryEngine, start_time: i128) void {
        const duration = @as(u64, @intCast(std.time.nanoTimestamp() - start_time));
        _ = self.queries_executed.fetchAdd(1, .monotonic);
        _ = self.semantic_queries.fetchAdd(1, .monotonic);
        _ = self.total_query_time_ns.fetchAdd(duration, .monotonic);
    }

    /// Execute a semantic search query
    pub fn execute_semantic_query(self: *QueryEngine, query: SemanticQuery) !SemanticQueryResult {
        const start_time = std.time.nanoTimestamp();
        defer self.record_semantic_query(start_time);

        assert(self.initialized);
        if (!self.initialized) return EngineError.NotInitialized;

        return operations.execute_semantic_query(
            self.allocator,
            self.storage_engine,
            query,
        );
    }

    /// Execute a filtered query
    pub fn execute_filtered_query(self: *QueryEngine, query: FilteredQuery) !FilteredQueryResult {
        const start_time = std.time.nanoTimestamp();
        defer self.record_filtered_query(start_time);

        assert(self.initialized);
        if (!self.initialized) return EngineError.NotInitialized;

        return filtering.execute_filtered_query(
            self.allocator,
            self.storage_engine,
            query,
        );
    }
};

/// Convenience function to create a FindBlocksQuery for a single block
pub fn single_block_query(block_id: BlockId) FindBlocksQuery {
    return FindBlocksQuery{
        .block_ids = &[_]BlockId{block_id},
    };
}

/// Convenience function to create a FindBlocksQuery for multiple blocks
pub fn multi_block_query(block_ids: []const BlockId) FindBlocksQuery {
    return FindBlocksQuery{
        .block_ids = block_ids,
    };
}

/// Command types for protocol compatibility
pub const QueryCommand = enum(u8) {
    find_blocks = 0x01,
    traverse = 0x02,
    filter = 0x03,
    semantic = 0x04,

    pub fn from_u8(value: u8) !QueryCommand {
        return std.meta.intToEnum(QueryCommand, value) catch EngineError.NotInitialized;
    }
};

// Tests
const testing = std.testing;
const simulation_vfs = @import("../sim/simulation_vfs.zig");

fn create_test_storage_engine(allocator: std.mem.Allocator) !StorageEngine {
    var sim_vfs = simulation_vfs.SimulationVFS.init(allocator);
    var storage_engine = try StorageEngine.init(allocator, sim_vfs.vfs(), "./test_query_engine");
    try storage_engine.initialize_storage();
    return storage_engine;
}

fn create_test_block(id: BlockId, content: []const u8) ContextBlock {
    return ContextBlock{
        .id = id,
        .version = 1,
        .source_uri = "test://query_engine.zig",
        .metadata_json = "{}",
        .content = content,
    };
}

test "query engine initialization and deinitialization" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const allocator = arena.allocator();

    var storage_engine = try create_test_storage_engine(allocator);
    defer storage_engine.deinit();

    var query_engine = QueryEngine.init(allocator, &storage_engine);
    defer query_engine.deinit();

    try testing.expect(query_engine.initialized);
    try testing.expect(query_engine.queries_executed.load(.monotonic) == 0);
    try testing.expect(query_engine.find_blocks_queries.load(.monotonic) == 0);
}

test "query engine statistics tracking" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const allocator = arena.allocator();

    var storage_engine = try create_test_storage_engine(allocator);
    defer storage_engine.deinit();

    var query_engine = QueryEngine.init(allocator, &storage_engine);
    defer query_engine.deinit();

    // Initial statistics should be zero
    const initial_stats = query_engine.statistics();
    try testing.expectEqual(@as(u64, 0), initial_stats.queries_executed);
    try testing.expectEqual(@as(u64, 0), initial_stats.find_blocks_queries);
    try testing.expectEqual(@as(u64, 0), initial_stats.traversal_queries);

    // Add test block
    const test_id = try BlockId.from_hex("1234567890123456789012345678901234567890");
    const test_block = create_test_block(test_id, "test content");
    try storage_engine.put_block(test_block);

    // Execute a query
    const query = FindBlocksQuery{ .block_ids = &[_]BlockId{test_id} };
    const result = try query_engine.execute_find_blocks(query);
    defer result.deinit();

    // Statistics should be updated
    const updated_stats = query_engine.statistics();
    try testing.expectEqual(@as(u64, 1), updated_stats.queries_executed);
    try testing.expectEqual(@as(u64, 1), updated_stats.find_blocks_queries);
    try testing.expectEqual(@as(u64, 0), updated_stats.traversal_queries);
    try testing.expect(updated_stats.total_query_time_ns > 0);
}

test "query engine statistics calculations" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const allocator = arena.allocator();

    var storage_engine = try create_test_storage_engine(allocator);
    defer storage_engine.deinit();

    var query_engine = QueryEngine.init(allocator, &storage_engine);
    defer query_engine.deinit();

    // Test average latency with zero queries
    const empty_stats = query_engine.statistics();
    try testing.expectEqual(@as(u64, 0), empty_stats.average_query_latency_ns());
    try testing.expectEqual(@as(f64, 0.0), empty_stats.queries_per_second());

    // Manually set some statistics for calculation testing
    query_engine.queries_executed.store(10, .monotonic);
    query_engine.total_query_time_ns.store(1_000_000_000, .monotonic); // 1 second

    const stats_with_data = query_engine.statistics();
    try testing.expectEqual(@as(u64, 100_000_000), stats_with_data.average_query_latency_ns()); // 100ms average
    try testing.expectEqual(@as(f64, 10.0), stats_with_data.queries_per_second());
}

test "query engine statistics reset" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const allocator = arena.allocator();

    var storage_engine = try create_test_storage_engine(allocator);
    defer storage_engine.deinit();

    var query_engine = QueryEngine.init(allocator, &storage_engine);
    defer query_engine.deinit();

    // Set some statistics
    query_engine.queries_executed.store(5, .monotonic);
    query_engine.find_blocks_queries.store(3, .monotonic);
    query_engine.total_query_time_ns.store(1000000, .monotonic);

    // Reset and verify
    query_engine.reset_statistics();

    const reset_stats = query_engine.statistics();
    try testing.expectEqual(@as(u64, 0), reset_stats.queries_executed);
    try testing.expectEqual(@as(u64, 0), reset_stats.find_blocks_queries);
    try testing.expectEqual(@as(u64, 0), reset_stats.traversal_queries);
    try testing.expectEqual(@as(u64, 0), reset_stats.total_query_time_ns);
}

test "query engine find_blocks execution" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const allocator = arena.allocator();

    var storage_engine = try create_test_storage_engine(allocator);
    defer storage_engine.deinit();

    var query_engine = QueryEngine.init(allocator, &storage_engine);
    defer query_engine.deinit();

    // Add test blocks
    const test_id1 = try BlockId.from_hex("1111111111111111111111111111111111111111");
    const test_id2 = try BlockId.from_hex("2222222222222222222222222222222222222222");
    const test_block1 = create_test_block(test_id1, "content 1");
    const test_block2 = create_test_block(test_id2, "content 2");

    try storage_engine.put_block(test_block1);
    try storage_engine.put_block(test_block2);

    // Execute query for both blocks
    const query = FindBlocksQuery{ .block_ids = &[_]BlockId{ test_id1, test_id2 } };
    const result = try query_engine.execute_find_blocks(query);
    defer result.deinit();

    try testing.expectEqual(@as(u32, 2), result.count);
    try testing.expect(!result.is_empty());
}

test "query engine find_block_by_id convenience method" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const allocator = arena.allocator();

    var storage_engine = try create_test_storage_engine(allocator);
    defer storage_engine.deinit();

    var query_engine = QueryEngine.init(allocator, &storage_engine);
    defer query_engine.deinit();

    // Add test block
    const test_id = try BlockId.from_hex("3333333333333333333333333333333333333333");
    const test_block = create_test_block(test_id, "single block content");
    try storage_engine.put_block(test_block);

    // Use convenience method
    const result = try query_engine.find_block_by_id(test_id);
    defer result.deinit();

    try testing.expectEqual(@as(u32, 1), result.count);
    try testing.expect(result.blocks[0].id.eql(test_id));
}

test "query engine block_exists check" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const allocator = arena.allocator();

    var storage_engine = try create_test_storage_engine(allocator);
    defer storage_engine.deinit();

    var query_engine = QueryEngine.init(allocator, &storage_engine);
    defer query_engine.deinit();

    const existing_id = try BlockId.from_hex("4444444444444444444444444444444444444444");
    const missing_id = try BlockId.from_hex("5555555555555555555555555555555555555555");

    // Initially, block shouldn't exist
    try testing.expect(!query_engine.block_exists(existing_id));
    try testing.expect(!query_engine.block_exists(missing_id));

    // Add block and test again
    const test_block = create_test_block(existing_id, "exists test");
    try storage_engine.put_block(test_block);

    try testing.expect(query_engine.block_exists(existing_id));
    try testing.expect(!query_engine.block_exists(missing_id));
}

test "query engine uninitialized error handling" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const allocator = arena.allocator();

    var storage_engine = try create_test_storage_engine(allocator);
    defer storage_engine.deinit();

    var query_engine = QueryEngine.init(allocator, &storage_engine);

    // Mark as uninitialized
    query_engine.initialized = false;

    const test_id = try BlockId.from_hex("6666666666666666666666666666666666666666");
    const query = FindBlocksQuery{ .block_ids = &[_]BlockId{test_id} };

    // Should return NotInitialized error
    try testing.expectError(EngineError.NotInitialized, query_engine.execute_find_blocks(query));
}

test "convenience query functions" {
    const test_id = try BlockId.from_hex("7777777777777777777777777777777777777777");
    const test_ids = [_]BlockId{ test_id, try BlockId.from_hex("8888888888888888888888888888888888888888") };

    // Test single block query
    const single_query = single_block_query(test_id);
    try testing.expectEqual(@as(usize, 1), single_query.block_ids.len);
    try testing.expect(single_query.block_ids[0].eql(test_id));

    // Test multi block query
    const multi_query = multi_block_query(&test_ids);
    try testing.expectEqual(@as(usize, 2), multi_query.block_ids.len);
    try testing.expect(multi_query.block_ids[0].eql(test_ids[0]));
    try testing.expect(multi_query.block_ids[1].eql(test_ids[1]));
}

test "query command enum parsing" {
    try testing.expectEqual(QueryCommand.find_blocks, try QueryCommand.from_u8(0x01));
    try testing.expectEqual(QueryCommand.traverse, try QueryCommand.from_u8(0x02));
    try testing.expectEqual(QueryCommand.filter, try QueryCommand.from_u8(0x03));
    try testing.expectEqual(QueryCommand.semantic, try QueryCommand.from_u8(0x04));

    // Invalid command should return error
    try testing.expectError(EngineError.NotInitialized, QueryCommand.from_u8(0xFF));
}

test "query engine traversal integration" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const allocator = arena.allocator();

    var storage_engine = try create_test_storage_engine(allocator);
    defer storage_engine.deinit();

    var query_engine = QueryEngine.init(allocator, &storage_engine);
    defer query_engine.deinit();

    // Add test block for traversal
    const start_id = try BlockId.from_hex("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa");
    const test_block = create_test_block(start_id, "traversal start");
    try storage_engine.put_block(test_block);

    // Test traversal convenience methods
    const outgoing_result = try query_engine.traverse_outgoing(start_id, 2);
    defer outgoing_result.deinit();

    const incoming_result = try query_engine.traverse_incoming(start_id, 2);
    defer incoming_result.deinit();

    const bidirectional_result = try query_engine.traverse_bidirectional(start_id, 2);
    defer bidirectional_result.deinit();

    // Verify statistics were updated
    const stats = query_engine.statistics();
    try testing.expectEqual(@as(u64, 3), stats.traversal_queries);
    try testing.expectEqual(@as(u64, 3), stats.queries_executed);
}
