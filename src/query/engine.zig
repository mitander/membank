//! Query engine coordination and public interface for Membank.
//!
//! Provides the main QueryEngine struct that coordinates between different
//! query operation modules. Handles metrics, statistics, and provides a
//! unified public API for all query types.

const std = @import("std");
const assert = @import("../core/assert.zig").assert;
const stdx = @import("../core/stdx.zig");
const storage = @import("../storage/engine.zig");
const context_block = @import("../core/types.zig");
const error_context = @import("../core/error_context.zig");
const operations = @import("operations.zig");
const traversal = @import("traversal.zig");
const filtering = @import("filtering.zig");
const simulation_vfs = @import("../sim/simulation_vfs.zig");
const testing = std.testing;

const StorageEngine = storage.StorageEngine;
const ContextBlock = context_block.ContextBlock;
const BlockId = context_block.BlockId;
const SimulationVFS = simulation_vfs.SimulationVFS;

pub const QueryError = operations.QueryError;
pub const FindBlocksQuery = operations.FindBlocksQuery;
pub const QueryResult = operations.QueryResult;

pub const TraversalQuery = traversal.TraversalQuery;
pub const TraversalResult = traversal.TraversalResult;
pub const TraversalDirection = traversal.TraversalDirection;
pub const TraversalAlgorithm = traversal.TraversalAlgorithm;

pub const SemanticQuery = operations.SemanticQuery;
pub const SemanticQueryResult = operations.SemanticQueryResult;
pub const SemanticResult = operations.SemanticResult;

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

/// Query planning and optimization framework for future extensibility
pub const QueryPlan = struct {
    query_type: QueryType,
    estimated_cost: u64,
    estimated_result_count: u32,
    optimization_hints: OptimizationHints,
    cache_eligible: bool,
    execution_strategy: ExecutionStrategy,

    pub const QueryType = enum {
        find_blocks,
        traversal,
        semantic,
        filtered,
    };

    pub const OptimizationHints = struct {
        use_index: bool = false,
        parallel_execution: bool = false,
        result_limit: ?u32 = null,
        early_termination: bool = false,
        prefer_memtable: bool = false,
        batch_size: ?u32 = null,
        enable_prefetch: bool = false,
    };

    pub const ExecutionStrategy = enum {
        direct_storage,
        cached_result,
        index_lookup,
        hybrid_approach,
        streaming_scan,
        optimized_traversal,
    };

    /// Create a basic query plan for immediate execution
    pub fn create_basic(query_type: QueryType) QueryPlan {
        return QueryPlan{
            .query_type = query_type,
            .estimated_cost = 1000, // Default cost estimate
            .estimated_result_count = 10, // Conservative estimate
            .optimization_hints = .{},
            .cache_eligible = false, // Conservative default
            .execution_strategy = .direct_storage,
        };
    }

    /// Analyze query complexity for optimization decisions
    pub fn analyze_complexity(self: *QueryPlan, block_count: u32, edge_count: u32) void {
        const complexity_factor = block_count + (edge_count / 2);
        self.estimated_cost = complexity_factor * 10;

        // Multi-tier optimization strategy
        if (complexity_factor > 10000) {
            self.optimization_hints.use_index = true;
            self.optimization_hints.enable_prefetch = true;
            self.execution_strategy = .hybrid_approach;
            self.cache_eligible = true;
        } else if (complexity_factor > 1000) {
            self.optimization_hints.use_index = true;
            self.cache_eligible = true;
            if (self.query_type == .traversal) {
                self.execution_strategy = .optimized_traversal;
            }
        } else if (complexity_factor < 100) {
            self.optimization_hints.prefer_memtable = true;
            self.execution_strategy = .direct_storage;
        }

        // Set optimal batch sizes based on query type and complexity
        switch (self.query_type) {
            .find_blocks => {
                if (complexity_factor > 500) {
                    self.optimization_hints.batch_size = 64;
                } else {
                    self.optimization_hints.batch_size = 16;
                }
            },
            .traversal => {
                self.optimization_hints.batch_size = if (complexity_factor > 1000) 32 else 8;
            },
            .semantic, .filtered => {
                self.optimization_hints.batch_size = if (complexity_factor > 200) 48 else 12;
            },
        }
    }
};

/// Query execution context with metrics and caching hooks
pub const QueryContext = struct {
    query_id: u64,
    start_time_ns: i64,
    plan: QueryPlan,
    metrics: QueryMetrics,
    cache_key: ?[]const u8 = null,

    pub const QueryMetrics = struct {
        blocks_scanned: u32 = 0,
        edges_traversed: u32 = 0,
        cache_hits: u32 = 0,
        cache_misses: u32 = 0,
        optimization_applied: bool = false,
        memtable_hits: u32 = 0,
        sstable_reads: u32 = 0,
        index_lookups: u32 = 0,
        prefetch_efficiency: f32 = 0.0,
    };

    /// Create execution context for query
    pub fn create(query_id: u64, plan: QueryPlan) QueryContext {
        return QueryContext{
            .query_id = query_id,
            .start_time_ns = @intCast(std.time.nanoTimestamp()),
            .plan = plan,
            .metrics = .{},
        };
    }

    /// Calculate execution duration in nanoseconds
    pub fn execution_duration_ns(self: *const QueryContext) u64 {
        const current_time = std.time.nanoTimestamp();
        return @as(u64, @intCast(current_time - self.start_time_ns));
    }
};

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

    // Query planning and optimization
    next_query_id: stdx.MetricsCounter,
    planning_enabled: bool,

    // Thread-safe metrics using coordination primitives
    queries_executed: stdx.MetricsCounter,
    find_blocks_queries: stdx.MetricsCounter,
    traversal_queries: stdx.MetricsCounter,
    filtered_queries: stdx.MetricsCounter,
    semantic_queries: stdx.MetricsCounter,
    total_query_time_ns: stdx.MetricsCounter,

    /// Initialize query engine with storage backend
    pub fn init(allocator: std.mem.Allocator, storage_engine: *StorageEngine) QueryEngine {
        return QueryEngine{
            .allocator = allocator,
            .storage_engine = storage_engine,
            .initialized = true,
            .next_query_id = stdx.MetricsCounter.init(1),
            .planning_enabled = true, // Enable planning by default for future extensibility
            .queries_executed = stdx.MetricsCounter.init(0),
            .find_blocks_queries = stdx.MetricsCounter.init(0),
            .traversal_queries = stdx.MetricsCounter.init(0),
            .filtered_queries = stdx.MetricsCounter.init(0),
            .semantic_queries = stdx.MetricsCounter.init(0),
            .total_query_time_ns = stdx.MetricsCounter.init(0),
        };
    }

    /// Clean up query engine resources
    pub fn deinit(self: *QueryEngine) void {
        self.initialized = false;
    }

    /// Generate a new unique query ID in a thread-safe manner
    pub fn generate_query_id(self: *QueryEngine) u64 {
        self.next_query_id.incr();
        return self.next_query_id.load();
    }

    /// Create query plan for optimization and metrics
    fn create_query_plan(
        self: *QueryEngine,
        query_type: QueryPlan.QueryType,
        estimated_results: u32,
    ) QueryPlan {
        var plan = QueryPlan.create_basic(query_type);

        if (self.planning_enabled) {
            const storage_metrics = self.storage_engine.metrics();
            plan.analyze_complexity(
                @intCast(storage_metrics.blocks_written.load()),
                @intCast(storage_metrics.edges_added.load()),
            );

            // Apply workload-adaptive optimizations
            self.apply_workload_optimizations(&plan, estimated_results);

            // Enable caching based on cost and query patterns
            if (plan.estimated_cost > 5000 or self.should_cache_query(query_type)) {
                plan.cache_eligible = true;
            }

            // Generate cache key for eligible queries
            if (plan.cache_eligible) {
                // Cache key generation will be implemented with actual caching system
                // For now, we prepare the infrastructure
            }
        }

        return plan;
    }

    /// Apply workload-specific optimizations based on query characteristics
    fn apply_workload_optimizations(self: *QueryEngine, plan: *QueryPlan, estimated_results: u32) void {
        const recent_query_ratio = self.calculate_recent_query_ratio(plan.query_type);

        switch (plan.query_type) {
            .find_blocks => {
                if (estimated_results > 100) {
                    plan.optimization_hints.early_termination = true;
                    plan.optimization_hints.batch_size = 32;
                }

                // Prefer memtable for small queries on recent data
                if (estimated_results <= 10 and recent_query_ratio > 0.7) {
                    plan.optimization_hints.prefer_memtable = true;
                }
            },
            .traversal => {
                plan.optimization_hints.result_limit = estimated_results;

                // Enable prefetching for complex traversals
                if (estimated_results > 50) {
                    plan.optimization_hints.enable_prefetch = true;
                    plan.execution_strategy = .optimized_traversal;
                }
            },
            .semantic, .filtered => {
                if (estimated_results > 50) {
                    plan.optimization_hints.use_index = true;
                    plan.execution_strategy = .index_lookup;
                }

                // Use streaming for large result sets
                if (estimated_results > 1000) {
                    plan.execution_strategy = .streaming_scan;
                }
            },
        }
    }

    /// Determine if query should be cached based on patterns and cost
    fn should_cache_query(self: *QueryEngine, query_type: QueryPlan.QueryType) bool {
        _ = self; // Reserved for future cache hit rate analysis

        // Cache expensive query types by default
        return switch (query_type) {
            .semantic, .filtered => true, // Complex queries benefit from caching
            .traversal => true, // Graph traversals often repeat
            .find_blocks => false, // Simple lookups rarely repeat exactly
        };
    }

    /// Calculate ratio of recent queries of this type (for optimization hints)
    fn calculate_recent_query_ratio(self: *QueryEngine, query_type: QueryPlan.QueryType) f32 {
        const total_recent = self.queries_executed.load();
        if (total_recent == 0) return 0.0;

        const query_count = switch (query_type) {
            .find_blocks => self.find_blocks_queries.load(),
            .traversal => self.traversal_queries.load(),
            .filtered => self.filtered_queries.load(),
            .semantic => self.semantic_queries.load(),
        };

        return @as(f32, @floatFromInt(query_count)) / @as(f32, @floatFromInt(total_recent));
    }

    /// Record query execution metrics for analysis
    fn record_query_execution(self: *QueryEngine, context: *const QueryContext) void {
        const duration_ns = context.execution_duration_ns();
        self.total_query_time_ns.add(duration_ns);

        // Record optimization effectiveness
        if (context.plan.optimization_hints.use_index and context.metrics.index_lookups > 0) {
            // Index optimization was beneficial
        }

        if (context.plan.optimization_hints.prefer_memtable and context.metrics.memtable_hits > 0) {
            // Memtable preference was effective
        }

        // Track cache effectiveness for future planning
        if (context.plan.cache_eligible) {
            const cache_hit_rate = if (context.metrics.cache_hits + context.metrics.cache_misses > 0)
                @as(f32, @floatFromInt(context.metrics.cache_hits)) /
                    @as(f32, @floatFromInt(context.metrics.cache_hits + context.metrics.cache_misses))
            else
                0.0;

            // Future: Adjust caching strategies based on hit rates
            _ = cache_hit_rate;
        }

        // Future: Machine learning hooks for query pattern recognition
        // This data will feed into sophisticated optimization algorithms
    }

    /// Execute a FindBlocks query to retrieve blocks by ID
    pub fn execute_find_blocks(
        self: *QueryEngine,
        query: FindBlocksQuery,
    ) !QueryResult {
        const start_time = std.time.nanoTimestamp();
        defer self.record_find_blocks_query(start_time);

        assert(self.initialized);
        if (!self.initialized) return EngineError.NotInitialized;

        // Create query plan and execution context
        const query_id = self.generate_query_id();
        const plan = self.create_query_plan(.find_blocks, @intCast(query.block_ids.len));
        var context = QueryContext.create(query_id, plan);

        // Apply optimization hints from query plan
        const optimized_query = query;
        if (plan.optimization_hints.batch_size) |batch_size| {
            // Future: Implement batched execution for large queries
            _ = batch_size;
        }

        const result = if (plan.optimization_hints.prefer_memtable)
            // Optimize for recent data by checking memtable first
            self.execute_find_blocks_optimized(optimized_query, &context)
        else
            operations.execute_find_blocks(
                self.allocator,
                self.storage_engine,
                optimized_query,
            ) catch |err| {
                error_context.log_storage_error(err, error_context.StorageContext{
                    .operation = "execute_find_blocks",
                    .size = query.block_ids.len,
                });
                return err;
            };

        // Record execution metrics for future optimization
        context.metrics.blocks_scanned = @intCast(query.block_ids.len);
        context.metrics.optimization_applied = plan.optimization_hints.prefer_memtable or
            plan.optimization_hints.batch_size != null;
        self.record_query_execution(&context);

        return result;
    }

    /// Find a single block by ID - convenience method for single block queries
    pub fn find_block(self: *QueryEngine, block_id: BlockId) !QueryResult {
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
        ) catch |err| {
            error_context.log_storage_error(err, error_context.StorageContext{
                .operation = "execute_traversal",
                .block_id = query.start_block_id,
                .size = query.max_results,
            });
            return err;
        };
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
        return .{
            .queries_executed = self.queries_executed.load(),
            .find_blocks_queries = self.find_blocks_queries.load(),
            .traversal_queries = self.traversal_queries.load(),
            .filtered_queries = self.filtered_queries.load(),
            .semantic_queries = self.semantic_queries.load(),
            .total_query_time_ns = self.total_query_time_ns.load(),
            .total_blocks_stored = @intCast(self.storage_engine.metrics().blocks_written.load()),
        };
    }

    /// Reset all query statistics to zero
    pub fn reset_statistics(self: *QueryEngine) void {
        self.queries_executed.reset();
        self.find_blocks_queries.reset();
        self.traversal_queries.reset();
        self.filtered_queries.reset();
        self.semantic_queries.reset();
        self.total_query_time_ns.reset();
    }

    /// Record metrics for a find_blocks query execution
    fn record_find_blocks_query(self: *QueryEngine, start_time: i128) void {
        const duration = @as(u64, @intCast(std.time.nanoTimestamp() - start_time));
        self.queries_executed.incr();
        self.find_blocks_queries.incr();
        self.total_query_time_ns.add(duration);
    }

    /// Record metrics for a traversal query execution
    fn record_traversal_query(self: *QueryEngine, start_time: i128) void {
        const duration = @as(u64, @intCast(std.time.nanoTimestamp() - start_time));
        self.queries_executed.incr();
        self.traversal_queries.incr();
        self.total_query_time_ns.add(duration);
    }

    /// Record metrics for a filtered query execution
    fn record_filtered_query(self: *QueryEngine, start_time: i128) void {
        const duration = @as(u64, @intCast(std.time.nanoTimestamp() - start_time));
        self.queries_executed.incr();
        self.filtered_queries.incr();
        self.total_query_time_ns.add(duration);
    }

    /// Record metrics for a semantic query execution
    fn record_semantic_query(self: *QueryEngine, start_time: i128) void {
        const duration = @as(u64, @intCast(std.time.nanoTimestamp() - start_time));
        self.queries_executed.incr();
        self.semantic_queries.incr();
        self.total_query_time_ns.add(duration);
    }

    /// Execute a semantic search query
    /// Execute a semantic query to find related blocks
    pub fn execute_semantic_query(
        self: *QueryEngine,
        query: SemanticQuery,
    ) !SemanticQueryResult {
        const start_time = std.time.nanoTimestamp();
        defer self.record_semantic_query(start_time);

        assert(self.initialized);
        if (!self.initialized) return EngineError.NotInitialized;

        // Create optimized execution plan for semantic queries
        const query_id = self.generate_query_id();
        const plan = self.create_query_plan(.semantic, query.max_results orelse 100);
        var context = QueryContext.create(query_id, plan);

        const result = switch (plan.execution_strategy) {
            .index_lookup => if (plan.optimization_hints.use_index)
                self.execute_semantic_query_indexed(query, &context)
            else
                operations.execute_keyword_query(self.allocator, self.storage_engine, query),
            .streaming_scan => self.execute_semantic_query_streaming(query, &context),
            else => operations.execute_keyword_query(self.allocator, self.storage_engine, query),
        } catch |err| {
            error_context.log_storage_error(err, error_context.StorageContext{
                .operation = "execute_semantic_query",
            });
            return err;
        };

        // Record semantic query metrics
        context.metrics.optimization_applied = plan.execution_strategy != .direct_storage;
        if (plan.optimization_hints.use_index) {
            context.metrics.index_lookups = 1; // Simplified metric
        }
        self.record_query_execution(&context);

        return result;
    }

    /// Optimized find blocks execution for memtable preference
    fn execute_find_blocks_optimized(
        self: *QueryEngine,
        query: FindBlocksQuery,
        context: *QueryContext,
    ) !QueryResult {
        var result_blocks = std.ArrayList(ContextBlock).init(self.allocator);
        defer result_blocks.deinit();

        // Pre-allocate the ArrayList with the maximum possible capacity we might need
        try result_blocks.ensureTotalCapacity(@as(u32, @intCast(query.block_ids.len)));

        var blocks_found: u32 = 0;
        var memtable_hits: u32 = 0;
        var sstable_reads: u32 = 0;

        const initial_blocks_read = self.storage_engine.storage_metrics.blocks_read.load();
        const initial_sstable_reads = self.storage_engine.storage_metrics.sstable_reads.load();

        for (query.block_ids) |block_id| {
            if (try self.storage_engine.find_block(block_id)) |block| {
                try result_blocks.append(block);
                blocks_found += 1;
            }
        }

        const final_blocks_read = self.storage_engine.storage_metrics.blocks_read.load();
        const final_sstable_reads = self.storage_engine.storage_metrics.sstable_reads.load();

        sstable_reads = @intCast(final_sstable_reads - initial_sstable_reads);
        const total_reads = @as(u32, @intCast(final_blocks_read - initial_blocks_read));
        memtable_hits = total_reads - sstable_reads;

        context.metrics.memtable_hits = memtable_hits;
        context.metrics.sstable_reads = sstable_reads;
        context.metrics.optimization_applied = true;

        // Bridge optimized materialization to streaming interface
        const block_ids = try self.allocator.alloc(BlockId, result_blocks.items.len);
        for (result_blocks.items, 0..) |block, i| {
            block_ids[i] = block.id;
        }

        return operations.QueryResult.init_with_owned_ids(self.allocator, self.storage_engine, block_ids);
    }

    /// Execute semantic query using index optimization
    fn execute_semantic_query_indexed(
        self: *QueryEngine,
        query: SemanticQuery,
        context: *QueryContext,
    ) !SemanticQueryResult {
        // Index optimization: Currently no secondary indices implemented
        // This provides a clean interface for future index integration
        context.metrics.index_lookups += 1;
        context.metrics.optimization_applied = true;

        return operations.execute_keyword_query(
            self.allocator,
            self.storage_engine,
            query,
        );
    }

    /// Execute filtered query using streaming scan
    fn execute_filtered_query_streaming(
        self: *QueryEngine,
        query: FilteredQuery,
        context: *QueryContext,
    ) !FilteredQueryResult {
        const FILTER_STREAMING_CHUNK_SIZE = 50;
        var blocks_evaluated: u32 = 0;

        const full_result = try filtering.execute_filtered_query(
            self.allocator,
            self.storage_engine,
            query,
        );
        defer full_result.deinit();

        // Streaming optimization: process filtered results in memory-efficient chunks
        var streaming_blocks = std.ArrayList(ContextBlock).init(self.allocator);
        defer streaming_blocks.deinit();

        // Pre-allocate with the chunk size to avoid reallocations
        try streaming_blocks.ensureTotalCapacity(@min(FILTER_STREAMING_CHUNK_SIZE, full_result.blocks.len));

        var chunk_start: usize = 0;
        while (chunk_start < full_result.blocks.len) {
            const chunk_end = @min(chunk_start + FILTER_STREAMING_CHUNK_SIZE, full_result.blocks.len);

            for (full_result.blocks[chunk_start..chunk_end]) |block| {
                try streaming_blocks.append(block);
                blocks_evaluated += 1;

                // Early termination for very large result sets
                if (streaming_blocks.items.len >= 1000) break;
            }

            chunk_start = chunk_end;
            if (streaming_blocks.items.len >= 1000) break;
        }

        context.metrics.optimization_applied = true;
        context.metrics.blocks_scanned = blocks_evaluated;

        return filtering.FilteredQueryResult.init(self.allocator, streaming_blocks.items, @intCast(streaming_blocks.items.len), false);
    }

    /// Execute filtered query using index optimization
    fn execute_filtered_query_indexed(
        self: *QueryEngine,
        query: FilteredQuery,
        context: *QueryContext,
    ) !FilteredQueryResult {
        // Index optimization: Currently no secondary indices for filtering
        // This provides a clean interface for future index-based filtering
        context.metrics.index_lookups += 1;
        context.metrics.optimization_applied = true;

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

fn create_test_storage_engine(allocator: std.mem.Allocator) !StorageEngine {
    var sim_vfs = try SimulationVFS.init(allocator);
    var storage_engine = try StorageEngine.init(allocator, sim_vfs.vfs(), "./test_query_engine");
    try storage_engine.startup();
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
    const allocator = testing.allocator;

    var storage_engine = try create_test_storage_engine(allocator);
    defer storage_engine.deinit();

    var query_engine = QueryEngine.init(allocator, &storage_engine);
    defer query_engine.deinit();

    try testing.expect(query_engine.initialized);
    try testing.expect(query_engine.queries_executed.load() == 0);
    try testing.expect(query_engine.find_blocks_queries.load() == 0);
}

test "query engine statistics tracking" {
    const allocator = testing.allocator;

    var storage_engine = try create_test_storage_engine(allocator);
    defer storage_engine.deinit();

    var query_engine = QueryEngine.init(allocator, &storage_engine);
    defer query_engine.deinit();

    // Initial statistics should be zero
    const initial_stats = query_engine.statistics();
    try testing.expectEqual(@as(u64, 0), initial_stats.queries_executed);
    try testing.expectEqual(@as(u64, 0), initial_stats.find_blocks_queries);
    try testing.expectEqual(@as(u64, 0), initial_stats.traversal_queries);

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
    const allocator = testing.allocator;

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
    const allocator = testing.allocator;

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
    const allocator = testing.allocator;

    var storage_engine = try create_test_storage_engine(allocator);
    defer storage_engine.deinit();

    var query_engine = QueryEngine.init(allocator, &storage_engine);
    defer query_engine.deinit();

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

    try testing.expectEqual(@as(u32, 2), result.total_found);
    try testing.expect(!result.is_empty());
}

test "query engine find_block convenience method" {
    const allocator = testing.allocator;

    var storage_engine = try create_test_storage_engine(allocator);
    defer storage_engine.deinit();

    var query_engine = QueryEngine.init(allocator, &storage_engine);
    defer query_engine.deinit();

    const test_id = try BlockId.from_hex("3333333333333333333333333333333333333333");
    const test_block = create_test_block(test_id, "single block content");
    try storage_engine.put_block(test_block);

    // Use convenience method
    const result = try query_engine.find_block(test_id);
    defer result.deinit();

    try testing.expectEqual(@as(u32, 1), result.total_found);

    const found_block = (try result.next()).?;
    defer result.deinit_block(found_block);
    try testing.expect(found_block.id.eql(test_id));
}

test "query engine block_exists check" {
    const allocator = testing.allocator;

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
    const allocator = testing.allocator;

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
    const allocator = testing.allocator;

    var storage_engine = try create_test_storage_engine(allocator);
    defer storage_engine.deinit();

    var query_engine = QueryEngine.init(allocator, &storage_engine);
    defer query_engine.deinit();

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
