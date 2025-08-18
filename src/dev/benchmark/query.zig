//! Query engine performance benchmarks.
//!
//! Tests single queries, batch operations, and graph traversal performance.
//! Focuses on query execution time and memory efficiency during result processing.

const std = @import("std");
const builtin = @import("builtin");
const kausaldb = @import("kausaldb");
const coordinator = @import("../benchmark.zig");

const storage = kausaldb.storage;
const query_engine = kausaldb.query_engine;
const context_block = kausaldb.types;
const production_vfs = kausaldb.production_vfs;
const operations = kausaldb.query_operations;

const StorageEngine = storage.StorageEngine;
const QueryEngine = query_engine.QueryEngine;
const ContextBlock = context_block.ContextBlock;
const BlockId = context_block.BlockId;
const GraphEdge = context_block.GraphEdge;
const EdgeType = context_block.EdgeType;
const FindBlocksQuery = operations.FindBlocksQuery;

const SINGLE_QUERY_THRESHOLD_NS = 300; // direct storage access ~0.12µs → 300ns (2.5x margin)
const BATCH_QUERY_THRESHOLD_NS = 3_000; // 10 blocks × 300ns = 3µs (simple loop)

const MAX_PEAK_MEMORY_BYTES = 100 * 1024 * 1024;
const MAX_MEMORY_GROWTH_PER_OP = 1024;

const ITERATIONS = 1000;
const WARMUP_ITERATIONS = 50;
const BATCH_SIZE = 10;

const BenchmarkResult = coordinator.BenchmarkResult;

pub fn run_all(allocator: std.mem.Allocator) !std.ArrayList(BenchmarkResult) {
    var results = std.ArrayList(BenchmarkResult).init(allocator);
    try results.append(try run_single_queries(allocator));
    try results.append(try run_batch_queries(allocator));
    return results;
}

/// Benchmark single block query performance with individual lookup timing
///
/// Tests the performance of finding individual blocks by ID in the query engine.
/// Used for understanding single query response characteristics.
/// Benchmark single-block query operations for fast lookups
pub fn run_single_queries(allocator: std.mem.Allocator) !BenchmarkResult {
    var prod_vfs = try allocator.create(production_vfs.ProductionVFS);
    defer allocator.destroy(prod_vfs);
    prod_vfs.* = production_vfs.ProductionVFS.init(allocator);
    defer prod_vfs.deinit();

    var storage_engine = try StorageEngine.init_default(allocator, prod_vfs.vfs(), "/tmp/kausaldb-tests/benchmark_single_queries");
    defer storage_engine.deinit();
    try storage_engine.startup();

    // Disable immediate sync for performance testing
    // WARNING: This reduces durability guarantees but allows measuring optimal performance
    storage_engine.configure_wal_immediate_sync(false);

    var query_eng = QueryEngine.init(allocator, &storage_engine);
    defer query_eng.deinit();
    query_eng.startup();

    return benchmark_single_block_queries(&query_eng, allocator);
}

/// Benchmark batch query performance with multi-block lookup tracking
///
/// Tests the performance of executing multiple queries together as a batch.
/// Used for understanding batch processing optimization benefits.
/// Benchmark batch query operations for efficient bulk access
pub fn run_batch_queries(allocator: std.mem.Allocator) !BenchmarkResult {
    var prod_vfs = try allocator.create(production_vfs.ProductionVFS);
    defer allocator.destroy(prod_vfs);
    prod_vfs.* = production_vfs.ProductionVFS.init(allocator);
    defer prod_vfs.deinit();

    var storage_engine = try StorageEngine.init_default(allocator, prod_vfs.vfs(), "/tmp/kausaldb-tests/benchmark_batch_queries");
    defer storage_engine.deinit();
    try storage_engine.startup();

    // Disable immediate sync for performance testing
    // WARNING: This reduces durability guarantees but allows measuring optimal performance
    storage_engine.configure_wal_immediate_sync(false);

    var query_eng = QueryEngine.init(allocator, &storage_engine);
    defer query_eng.deinit();
    query_eng.startup();

    return benchmark_batch_queries_impl(&query_eng, allocator);
}

fn benchmark_single_block_queries(query_eng: *QueryEngine, allocator: std.mem.Allocator) !BenchmarkResult {
    try setup_query_test_data(query_eng.storage_engine);
    const test_block_ids = try create_query_test_block_ids(allocator);
    defer allocator.free(test_block_ids);

    const initial_memory = kausaldb.profiler.query_current_rss_memory();
    var timings = try allocator.alloc(u64, ITERATIONS);
    defer allocator.free(timings);

    for (0..WARMUP_ITERATIONS) |i| {
        const block_id = test_block_ids[i % test_block_ids.len];
        _ = try query_eng.find_block(block_id);
    }

    var found_count: u32 = 0;
    for (0..ITERATIONS) |i| {
        const block_id = test_block_ids[i % test_block_ids.len];

        const start_time = std.time.nanoTimestamp();
        const maybe_block = try query_eng.find_block(block_id);
        const end_time = std.time.nanoTimestamp();

        if (maybe_block != null) {
            found_count += 1;
        }

        timings[i] = @intCast(end_time - start_time);
    }

    const peak_memory = kausaldb.profiler.query_current_rss_memory();
    const memory_growth = peak_memory - initial_memory;

    const stats = analyze_timings(timings);
    const throughput = calculate_safe_throughput(ITERATIONS, stats.total_time_ns);

    const result = BenchmarkResult{
        .operation_name = "Single Query",
        .iterations = ITERATIONS,
        .total_time_ns = stats.total_time_ns,
        .min_ns = stats.min,
        .max_ns = stats.max,
        .mean_ns = stats.mean,
        .median_ns = stats.median,
        .stddev_ns = stats.stddev,
        .throughput_ops_per_sec = throughput,
        .passed_threshold = stats.mean <= SINGLE_QUERY_THRESHOLD_NS,
        .threshold_ns = SINGLE_QUERY_THRESHOLD_NS,
        .peak_memory_bytes = peak_memory,
        .memory_growth_bytes = memory_growth,
        .memory_efficient = peak_memory <= MAX_PEAK_MEMORY_BYTES and memory_growth <= (MAX_MEMORY_GROWTH_PER_OP * ITERATIONS),
    };

    return result;
}

fn benchmark_batch_queries_impl(query_eng: *QueryEngine, allocator: std.mem.Allocator) !BenchmarkResult {
    try setup_query_test_data(query_eng.storage_engine);
    const test_block_ids = try create_query_test_block_ids(allocator);
    defer allocator.free(test_block_ids);

    const initial_memory = kausaldb.profiler.query_current_rss_memory();
    var timings = try allocator.alloc(u64, ITERATIONS);
    defer allocator.free(timings);

    for (0..WARMUP_ITERATIONS) |i| {
        const start_idx = (i * BATCH_SIZE) % test_block_ids.len;
        const end_idx = @min(start_idx + BATCH_SIZE, test_block_ids.len);
        const batch_ids = test_block_ids[start_idx..end_idx];

        // Use proper batch API for warmup
        const warmup_query = FindBlocksQuery{ .block_ids = batch_ids };
        var warmup_result = try operations.execute_find_blocks(allocator, query_eng.storage_engine, warmup_query);
        defer warmup_result.deinit();

        // Consume results to ensure proper warmup
        while (try warmup_result.next()) |_| {}
    }

    for (0..ITERATIONS) |i| {
        const start_idx = (i * BATCH_SIZE) % test_block_ids.len;
        const end_idx = @min(start_idx + BATCH_SIZE, test_block_ids.len);
        const batch_ids = test_block_ids[start_idx..end_idx];

        const start_time = std.time.nanoTimestamp();

        // Use proper batch API instead of individual loops
        const batch_query = FindBlocksQuery{ .block_ids = batch_ids };
        var batch_result = try operations.execute_find_blocks(allocator, query_eng.storage_engine, batch_query);
        defer batch_result.deinit();

        // Consume all results to ensure complete execution
        var result_count: u32 = 0;
        while (try batch_result.next()) |_| {
            result_count += 1;
        }

        const end_time = std.time.nanoTimestamp();
        timings[i] = @intCast(end_time - start_time);
    }

    const peak_memory = kausaldb.profiler.query_current_rss_memory();
    const memory_growth = peak_memory - initial_memory;

    const stats = analyze_timings(timings);
    const throughput = calculate_safe_throughput(ITERATIONS, stats.total_time_ns);

    const result = BenchmarkResult{
        .operation_name = "Batch Query",
        .iterations = ITERATIONS,
        .total_time_ns = stats.total_time_ns,
        .min_ns = stats.min,
        .max_ns = stats.max,
        .mean_ns = stats.mean,
        .median_ns = stats.median,
        .stddev_ns = stats.stddev,
        .throughput_ops_per_sec = throughput,
        .passed_threshold = stats.mean <= BATCH_QUERY_THRESHOLD_NS,
        .threshold_ns = BATCH_QUERY_THRESHOLD_NS,
        .peak_memory_bytes = peak_memory,
        .memory_growth_bytes = memory_growth,
        .memory_efficient = peak_memory <= MAX_PEAK_MEMORY_BYTES and memory_growth <= (MAX_MEMORY_GROWTH_PER_OP * ITERATIONS),
    };

    return result;
}

fn setup_query_test_data(storage_engine: *StorageEngine) !void {
    const allocator = std.heap.page_allocator; // Temporary for setup

    for (0..100) |i| {
        const block = try create_query_test_block(allocator, i);
        defer free_query_test_block(allocator, block);
        _ = try storage_engine.put_block(block);
    }

    for (0..50) |i| {
        const source_id_hex = try std.fmt.allocPrint(allocator, "{x:0>32}", .{i + 1});
        defer allocator.free(source_id_hex);
        const target_id_hex = try std.fmt.allocPrint(allocator, "{x:0>32}", .{i + 2});
        defer allocator.free(target_id_hex);

        const source_id = try BlockId.from_hex(source_id_hex);
        const target_id = try BlockId.from_hex(target_id_hex);

        const edge = GraphEdge{
            .source_id = source_id,
            .target_id = target_id,
            .edge_type = EdgeType.calls,
        };

        try storage_engine.put_edge(edge);
    }
}

fn create_query_test_block_ids(allocator: std.mem.Allocator) ![]BlockId {
    var block_ids = try allocator.alloc(BlockId, 100);

    for (0..100) |i| {
        const id_hex = try std.fmt.allocPrint(allocator, "{x:0>32}", .{i + 1});
        defer allocator.free(id_hex);
        block_ids[i] = try BlockId.from_hex(id_hex);
    }

    return block_ids;
}

fn create_query_test_block(allocator: std.mem.Allocator, index: usize) !ContextBlock {
    const block_id_hex = try std.fmt.allocPrint(allocator, "{x:0>32}", .{index + 1});
    defer allocator.free(block_id_hex);

    const block_id = try BlockId.from_hex(block_id_hex);
    const source_uri = try std.fmt.allocPrint(allocator, "query://test_block_{}.zig", .{index});
    const metadata_json = try std.fmt.allocPrint(allocator, "{{\"type\":\"query_test\",\"index\":{}}}", .{index});
    const content = try std.fmt.allocPrint(allocator, "Query test block content {}", .{index});

    return ContextBlock{
        .id = block_id,
        .version = 1,
        .source_uri = source_uri,
        .metadata_json = metadata_json,
        .content = content,
    };
}

fn free_query_test_block(allocator: std.mem.Allocator, block: ContextBlock) void {
    allocator.free(block.source_uri);
    allocator.free(block.metadata_json);
    allocator.free(block.content);
}

fn calculate_safe_throughput(iterations: u64, total_time_ns: u64) f64 {
    if (total_time_ns == 0) {
        // When timing resolution is insufficient, report based on minimum measurable time (1ns)
        return @as(f64, @floatFromInt(iterations)) / (1.0 / 1_000_000_000.0);
    }
    return @as(f64, @floatFromInt(iterations)) / (@as(f64, @floatFromInt(total_time_ns)) / 1_000_000_000.0);
}

fn analyze_timings(timings: []u64) struct {
    total_time_ns: u64,
    min: u64,
    max: u64,
    mean: u64,
    median: u64,
    stddev: u64,
} {
    if (timings.len == 0) return .{ .total_time_ns = 0, .min = 0, .max = 0, .mean = 0, .median = 0, .stddev = 0 };

    std.mem.sort(u64, timings, {}, std.sort.asc(u64));

    const min = timings[0];
    const max = timings[timings.len - 1];
    const median = timings[timings.len / 2];

    var total_time_ns: u64 = 0;
    for (timings) |time| total_time_ns += time;
    const mean = total_time_ns / timings.len;

    var variance_sum: u64 = 0;
    for (timings) |time| {
        const diff = if (time > mean) time - mean else mean - time;
        variance_sum += diff * diff;
    }
    const variance = variance_sum / timings.len;
    const stddev = std.math.sqrt(variance);

    return .{
        .total_time_ns = total_time_ns,
        .min = min,
        .max = max,
        .mean = mean,
        .median = median,
        .stddev = stddev,
    };
}
