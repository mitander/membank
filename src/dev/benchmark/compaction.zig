//! Compaction performance benchmarks.
//!
//! Tests SSTable compaction operations, merge performance, and memory efficiency
//! during compaction cycles. Focuses on background operation impact on system performance.

const builtin = @import("builtin");
const std = @import("std");

const kausaldb = @import("kausaldb");

const coordinator = @import("../benchmark.zig");

const assert = kausaldb.assert;
const context_block = kausaldb.types;
const production_vfs = kausaldb.production_vfs;
const storage = kausaldb.storage;

const BenchmarkResult = coordinator.BenchmarkResult;
const StorageEngine = storage.StorageEngine;
const ContextBlock = context_block.ContextBlock;
const BlockId = context_block.BlockId;

const COMPACTION_THRESHOLD_NS = 50_000_000; // 50ms for compaction operations
const MERGE_THRESHOLD_NS = 10_000_000; // 10ms for merge operations
const MAX_PEAK_MEMORY_BYTES = 200 * 1024 * 1024; // 200MB during compaction
const MAX_MEMORY_GROWTH_PER_OP = 50 * 1024; // 50KB per compaction (actual storage usage)
const COMPACTION_ITERATIONS = 10;
const WARMUP_ITERATIONS = 2;

pub fn run_all(allocator: std.mem.Allocator) !std.array_list.Managed(BenchmarkResult) {
    var results = std.array_list.Managed(BenchmarkResult).init(allocator);
    try results.append(try run_compaction_benchmark(allocator));
    return results;
}

/// Benchmark SSTable compaction performance
///
/// Tests how long it takes to merge and clean up SSTables.
/// Helps understand how compaction affects overall performance.
pub fn run_compaction_benchmark(allocator: std.mem.Allocator) !BenchmarkResult {
    var prod_vfs = try allocator.create(production_vfs.ProductionVFS);
    defer allocator.destroy(prod_vfs);
    prod_vfs.* = production_vfs.ProductionVFS.init(allocator);
    defer prod_vfs.deinit();

    var storage_engine = try StorageEngine.init_default(allocator, prod_vfs.vfs(), "/tmp/kausaldb-tests/benchmark_compaction");
    defer storage_engine.deinit();

    try storage_engine.startup();

    return benchmark_compaction_operations(&storage_engine, allocator);
}

fn benchmark_compaction_operations(
    storage_engine: *StorageEngine,
    allocator: std.mem.Allocator,
) !BenchmarkResult {
    try setup_compaction_test_data(storage_engine, allocator);

    const initial_memory = storage_engine.memory_usage().total_bytes;
    var timings = try allocator.alloc(u64, COMPACTION_ITERATIONS);
    defer allocator.free(timings);

    for (0..WARMUP_ITERATIONS) |_| {
        try storage_engine.flush_memtable_to_sstable();
    }

    for (0..COMPACTION_ITERATIONS) |i| {
        const base_offset = 10000 + (i * 100);

        // Add test data to memtable
        for (0..50) |j| {
            const block = try create_compaction_test_block(allocator, base_offset + j);
            defer free_compaction_test_block(allocator, block);
            _ = try storage_engine.put_block(block);
        }

        // Time the actual compaction operation
        const start_time = std.time.nanoTimestamp();
        try storage_engine.flush_memtable_to_sstable();
        const end_time = std.time.nanoTimestamp();

        timings[i] = @intCast(end_time - start_time);
    }

    const final_memory = storage_engine.memory_usage().total_bytes;
    const memory_growth = if (final_memory >= initial_memory) final_memory - initial_memory else 0;

    const stats = analyze_timings(timings);
    const throughput = calculate_safe_throughput(COMPACTION_ITERATIONS, stats.total_time_ns);

    const result = BenchmarkResult{
        .operation_name = "Compaction",
        .iterations = COMPACTION_ITERATIONS,
        .total_time_ns = stats.total_time_ns,
        .min_ns = stats.min,
        .max_ns = stats.max,
        .mean_ns = stats.mean,
        .median_ns = stats.median,
        .stddev_ns = stats.stddev,
        .throughput_ops_per_sec = throughput,
        .passed_threshold = stats.mean <= COMPACTION_THRESHOLD_NS,
        .threshold_ns = COMPACTION_THRESHOLD_NS,
        .peak_memory_bytes = final_memory,
        .memory_growth_bytes = memory_growth,
        .memory_efficient = final_memory <= MAX_PEAK_MEMORY_BYTES and memory_growth <= (MAX_MEMORY_GROWTH_PER_OP * COMPACTION_ITERATIONS),
    };

    return result;
}

fn setup_compaction_test_data(storage_engine: *StorageEngine, allocator: std.mem.Allocator) !void {
    const num_blocks = 1000;

    for (0..num_blocks) |i| {
        const block = try create_compaction_test_block(allocator, i);
        defer free_compaction_test_block(allocator, block);
        _ = try storage_engine.put_block(block);

        if (i > 0 and i % 100 == 0) {
            try storage_engine.flush_memtable_to_sstable();
        }
    }
}

fn create_compaction_test_block(allocator: std.mem.Allocator, index: usize) !ContextBlock {
    const block_id_hex = try std.fmt.allocPrint(allocator, "{x:0>32}", .{index + 1});
    defer allocator.free(block_id_hex);

    const block_id = try BlockId.from_hex(block_id_hex);
    const source_uri = try std.fmt.allocPrint(allocator, "compaction://test_block_{}.zig", .{index});
    const metadata_json = try std.fmt.allocPrint(allocator, "{{\"type\":\"compaction_test\",\"index\":{}}}", .{index});

    const content = try std.fmt.allocPrint(allocator,
        \\pub fn compaction_test_function_{}() void {{
        \\    // This is test content for compaction benchmarking
        \\    // Index: {}
        \\    // Content is intentionally verbose to simulate real blocks
        \\    var data: [100]u8 = undefined;
        \\    for (data) |*byte, i| {{
        \\        byte.* = @intCast(u8, i % 256);
        \\    }}
        \\    const result = data[0] + data[99];
        \\    assert(result >= 0);
        \\}}
    , .{ index, index });

    return ContextBlock{
        .id = block_id,
        .version = 1,
        .source_uri = source_uri,
        .metadata_json = metadata_json,
        .content = content,
    };
}

fn free_compaction_test_block(allocator: std.mem.Allocator, block: ContextBlock) void {
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
