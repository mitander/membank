//! Storage engine performance benchmarks.
//!
//! Tests block operations, WAL performance, and memory efficiency.
//! Thresholds calibrated based on measured performance with safety margins
//! for reliable CI regression detection.

const std = @import("std");
const builtin = @import("builtin");
const kausaldb = @import("kausaldb");
const coordinator = @import("../benchmark.zig");

const storage = kausaldb.storage;
const context_block = kausaldb.types;
const simulation_vfs = kausaldb.simulation_vfs;
const ownership = kausaldb.ownership;

const StorageEngine = storage.StorageEngine;
const ContextBlock = context_block.ContextBlock;
const BlockId = context_block.BlockId;
const StorageEngineBlock = ownership.StorageEngineBlock;
const OwnedBlock = ownership.OwnedBlock;

const BLOCK_WRITE_THRESHOLD_NS = 100_000; // measured 21µs → 100µs (4.7x margin)
const BLOCK_READ_THRESHOLD_NS = 1_000; // measured 0.06µs → 1µs (17x margin)
const BLOCK_UPDATE_THRESHOLD_NS = 50_000; // measured 10.2µs → 50µs (4.9x margin)
const BLOCK_DELETE_THRESHOLD_NS = 15_000; // measured 2.9µs → 15µs (5.2x margin)
const WAL_FLUSH_THRESHOLD_NS = 10_000; // conservative for no-op operation

const MAX_PEAK_MEMORY_BYTES = 100 * 1024 * 1024; // 100MB for 10K operations
const MAX_MEMORY_GROWTH_PER_OP = 12 * 1024; // 12KB per operation (measured up to 9.6KB)

const ITERATIONS = 10;
const WARMUP_ITERATIONS = 5;
const LARGE_ITERATIONS = 50;
const HIGH_PRECISION_ITERATIONS = 10000; // For sub-microsecond operations
const STATISTICAL_SAMPLES = 5;

const BenchmarkResult = coordinator.BenchmarkResult;

/// Run all storage benchmark tests with performance measurement
///
/// Runs benchmarks for writes, reads, updates, deletes, and WAL flush operations.
/// Tests all main storage engine operations.
pub fn run_all(allocator: std.mem.Allocator) !std.ArrayList(BenchmarkResult) {
    var results = std.ArrayList(BenchmarkResult).init(allocator);

    try results.append(try run_block_writes(allocator));
    try results.append(try run_block_reads(allocator));
    try results.append(try run_block_updates(allocator));
    try results.append(try run_block_deletes(allocator));
    try results.append(try run_wal_flush(allocator));
    try results.append(try run_zero_cost_ownership(allocator));
    return results;
}

/// Benchmark block write operations with performance and memory tracking
///
/// Creates test blocks and measures time to write them to storage engine.
/// Used for understanding ingestion pipeline performance.
pub fn run_block_writes(allocator: std.mem.Allocator) !BenchmarkResult {
    var sim_vfs = try simulation_vfs.SimulationVFS.heap_init(allocator);
    defer {
        sim_vfs.deinit();
        allocator.destroy(sim_vfs);
    }

    var storage_engine = try StorageEngine.init_default(allocator, sim_vfs.vfs(), "benchmark_writes");
    defer storage_engine.deinit();
    try storage_engine.startup();

    return benchmark_block_writes(&storage_engine, allocator);
}

/// Benchmark block read operations with lookup performance measurement
///
/// Pre-populates storage with test blocks then measures retrieval time.
/// Used for understanding query response characteristics.
pub fn run_block_reads(allocator: std.mem.Allocator) !BenchmarkResult {
    var sim_vfs = try simulation_vfs.SimulationVFS.heap_init(allocator);
    defer {
        sim_vfs.deinit();
        allocator.destroy(sim_vfs);
    }

    var storage_engine = try StorageEngine.init_default(allocator, sim_vfs.vfs(), "benchmark_reads");
    defer storage_engine.deinit();
    try storage_engine.startup();

    return benchmark_block_reads(&storage_engine, allocator);
}

/// Benchmark block update operations with modification performance tracking
///
/// Updates existing blocks with new versions and measures performance.
/// Used for understanding version management overhead.
pub fn run_block_updates(allocator: std.mem.Allocator) !BenchmarkResult {
    var sim_vfs = try simulation_vfs.SimulationVFS.heap_init(allocator);
    defer {
        sim_vfs.deinit();
        allocator.destroy(sim_vfs);
    }

    var storage_engine = try StorageEngine.init_default(allocator, sim_vfs.vfs(), "benchmark_updates");
    defer storage_engine.deinit();
    try storage_engine.startup();

    return benchmark_block_updates(&storage_engine, allocator);
}

/// Benchmark block delete operations with removal performance tracking
///
/// Creates blocks to delete and measures time to remove them from storage.
/// Includes tombstone handling and compaction effects.
pub fn run_block_deletes(allocator: std.mem.Allocator) !BenchmarkResult {
    var sim_vfs = try simulation_vfs.SimulationVFS.heap_init(allocator);
    defer {
        sim_vfs.deinit();
        allocator.destroy(sim_vfs);
    }

    var storage_engine = try StorageEngine.init_default(allocator, sim_vfs.vfs(), "benchmark_deletes");
    defer storage_engine.deinit();
    try storage_engine.startup();

    return benchmark_block_deletes(&storage_engine, allocator);
}

/// Benchmark WAL flush operations with durability performance tracking
///
/// Measures time to flush Write-Ahead Log to persistent storage.
/// Used for understanding commit latency characteristics.
pub fn run_wal_flush(allocator: std.mem.Allocator) !BenchmarkResult {
    var sim_vfs = try simulation_vfs.SimulationVFS.heap_init(allocator);
    defer {
        sim_vfs.deinit();
        allocator.destroy(sim_vfs);
    }

    var storage_engine = try StorageEngine.init_default(allocator, sim_vfs.vfs(), "benchmark_wal");
    defer storage_engine.deinit();
    try storage_engine.startup();

    return benchmark_wal_flush(&storage_engine, allocator);
}

/// Run zero-cost ownership benchmark comparing compile-time vs runtime validation
pub fn run_zero_cost_ownership(allocator: std.mem.Allocator) !BenchmarkResult {
    var sim_vfs = try simulation_vfs.SimulationVFS.heap_init(allocator);
    defer {
        sim_vfs.deinit();
        allocator.destroy(sim_vfs);
    }

    var storage_engine = try StorageEngine.init_default(allocator, sim_vfs.vfs(), "benchmark_ownership");
    defer storage_engine.deinit();
    try storage_engine.startup();

    return benchmark_zero_cost_ownership(&storage_engine, allocator);
}

fn benchmark_block_writes(storage_engine: *StorageEngine, allocator: std.mem.Allocator) !BenchmarkResult {
    const initial_memory = kausaldb.profiler.query_current_rss_memory();
    var timings = try allocator.alloc(u64, ITERATIONS);
    defer allocator.free(timings);

    // Pre-allocate test blocks to eliminate allocation overhead from timing measurements
    var warmup_blocks = try allocator.alloc(ContextBlock, WARMUP_ITERATIONS);
    defer {
        for (warmup_blocks) |block| {
            free_test_block(allocator, block);
        }
        allocator.free(warmup_blocks);
    }

    var test_blocks = try allocator.alloc(ContextBlock, ITERATIONS);
    defer {
        for (test_blocks) |block| {
            free_test_block(allocator, block);
        }
        allocator.free(test_blocks);
    }

    for (0..WARMUP_ITERATIONS) |i| {
        warmup_blocks[i] = try create_test_block(allocator, i);
    }

    for (0..ITERATIONS) |i| {
        test_blocks[i] = try create_test_block(allocator, WARMUP_ITERATIONS + i);
    }

    // Warmup phase - no allocation overhead
    for (0..WARMUP_ITERATIONS) |i| {
        _ = try storage_engine.put_block(warmup_blocks[i]);
    }

    // Benchmark phase - pure storage engine performance measurement
    for (0..ITERATIONS) |i| {
        const start_time = std.time.nanoTimestamp();
        _ = try storage_engine.put_block(test_blocks[i]);
        const end_time = std.time.nanoTimestamp();

        timings[i] = @intCast(end_time - start_time);
    }

    const peak_memory = kausaldb.profiler.query_current_rss_memory();
    const memory_growth = peak_memory - initial_memory;

    const stats = analyze_timings(timings);
    const throughput = calculate_safe_throughput(ITERATIONS, stats.total_time_ns);

    const result = BenchmarkResult{
        .operation_name = "Block Write",
        .iterations = ITERATIONS,
        .total_time_ns = stats.total_time_ns,
        .min_ns = stats.min,
        .max_ns = stats.max,
        .mean_ns = stats.mean,
        .median_ns = stats.median,
        .stddev_ns = stats.stddev,
        .throughput_ops_per_sec = throughput,
        .passed_threshold = stats.mean <= BLOCK_WRITE_THRESHOLD_NS,
        .threshold_ns = BLOCK_WRITE_THRESHOLD_NS,
        .peak_memory_bytes = peak_memory,
        .memory_growth_bytes = memory_growth,
        .memory_efficient = peak_memory <= MAX_PEAK_MEMORY_BYTES and memory_growth <= (MAX_MEMORY_GROWTH_PER_OP * ITERATIONS),
    };

    return result;
}

fn benchmark_block_reads(storage_engine: *StorageEngine, allocator: std.mem.Allocator) !BenchmarkResult {
    const block_ids = try setup_read_test_blocks(storage_engine, allocator);
    defer allocator.free(block_ids);

    const initial_memory = kausaldb.profiler.query_current_rss_memory();
    var timings = try allocator.alloc(u64, HIGH_PRECISION_ITERATIONS);
    defer allocator.free(timings);

    for (0..WARMUP_ITERATIONS) |i| {
        const block_id = block_ids[i % block_ids.len];
        _ = try storage_engine.find_block(block_id, .temporary);
    }

    for (0..HIGH_PRECISION_ITERATIONS) |i| {
        const block_id = block_ids[i % block_ids.len];

        const start_time = std.time.nanoTimestamp();
        _ = try storage_engine.find_block(block_id, .temporary);
        const end_time = std.time.nanoTimestamp();

        timings[i] = @intCast(end_time - start_time);
    }

    const peak_memory = kausaldb.profiler.query_current_rss_memory();
    const memory_growth = peak_memory - initial_memory;

    const stats = analyze_timings(timings);
    const throughput = calculate_safe_throughput(HIGH_PRECISION_ITERATIONS, stats.total_time_ns);

    const result = BenchmarkResult{
        .operation_name = "Block Read",
        .iterations = HIGH_PRECISION_ITERATIONS,
        .total_time_ns = stats.total_time_ns,
        .min_ns = stats.min,
        .max_ns = stats.max,
        .mean_ns = stats.mean,
        .median_ns = stats.median,
        .stddev_ns = stats.stddev,
        .throughput_ops_per_sec = throughput,
        .passed_threshold = stats.mean <= BLOCK_READ_THRESHOLD_NS,
        .threshold_ns = BLOCK_READ_THRESHOLD_NS,
        .peak_memory_bytes = peak_memory,
        .memory_growth_bytes = memory_growth,
        .memory_efficient = peak_memory <= MAX_PEAK_MEMORY_BYTES and memory_growth <= (MAX_MEMORY_GROWTH_PER_OP * HIGH_PRECISION_ITERATIONS),
    };

    return result;
}

fn benchmark_block_updates(storage_engine: *StorageEngine, allocator: std.mem.Allocator) !BenchmarkResult {
    const block_ids = try setup_read_test_blocks(storage_engine, allocator);
    defer allocator.free(block_ids);

    const initial_memory = kausaldb.profiler.query_current_rss_memory();
    var timings = try allocator.alloc(u64, ITERATIONS);
    defer allocator.free(timings);

    for (0..WARMUP_ITERATIONS) |i| {
        const block_id = block_ids[i % block_ids.len];
        const updated_block = try create_updated_test_block(allocator, block_id, i);
        defer free_test_block(allocator, updated_block);
        _ = try storage_engine.put_block(updated_block);
    }

    for (0..ITERATIONS) |i| {
        const block_id = block_ids[i % block_ids.len];
        const updated_block = try create_updated_test_block(allocator, block_id, WARMUP_ITERATIONS + i);
        defer free_test_block(allocator, updated_block);

        const start_time = std.time.nanoTimestamp();
        _ = try storage_engine.put_block(updated_block);
        const end_time = std.time.nanoTimestamp();

        timings[i] = @intCast(end_time - start_time);
    }

    const peak_memory = kausaldb.profiler.query_current_rss_memory();
    const memory_growth = peak_memory - initial_memory;

    const stats = analyze_timings(timings);
    const throughput = calculate_safe_throughput(ITERATIONS, stats.total_time_ns);

    const result = BenchmarkResult{
        .operation_name = "Block Update",
        .iterations = ITERATIONS,
        .total_time_ns = stats.total_time_ns,
        .min_ns = stats.min,
        .max_ns = stats.max,
        .mean_ns = stats.mean,
        .median_ns = stats.median,
        .stddev_ns = stats.stddev,
        .throughput_ops_per_sec = throughput,
        .passed_threshold = stats.mean <= BLOCK_UPDATE_THRESHOLD_NS,
        .threshold_ns = BLOCK_UPDATE_THRESHOLD_NS,
        .peak_memory_bytes = peak_memory,
        .memory_growth_bytes = memory_growth,
        .memory_efficient = peak_memory <= MAX_PEAK_MEMORY_BYTES and memory_growth <= (MAX_MEMORY_GROWTH_PER_OP * ITERATIONS),
    };

    return result;
}

fn benchmark_block_deletes(storage_engine: *StorageEngine, allocator: std.mem.Allocator) !BenchmarkResult {
    const block_ids = try setup_delete_test_blocks(storage_engine, allocator);
    defer allocator.free(block_ids);

    const initial_memory = kausaldb.profiler.query_current_rss_memory();
    var timings = try allocator.alloc(u64, ITERATIONS);
    defer allocator.free(timings);

    for (0..WARMUP_ITERATIONS) |i| {
        const block_id = block_ids[i % block_ids.len];
        const start_time = std.time.nanoTimestamp();
        _ = try storage_engine.delete_block(block_id);
        const end_time = std.time.nanoTimestamp();
        timings[i] = @intCast(end_time - start_time);

        const replacement_block = try create_test_block(allocator, i + 10000);
        defer free_test_block(allocator, replacement_block);
        _ = try storage_engine.put_block(replacement_block);
    }

    for (0..ITERATIONS) |i| {
        const block_id = if (i < block_ids.len) block_ids[i] else block_ids[i % block_ids.len];

        const start_time = std.time.nanoTimestamp();
        _ = try storage_engine.delete_block(block_id);
        const end_time = std.time.nanoTimestamp();

        timings[i] = @intCast(end_time - start_time);
    }

    const peak_memory = kausaldb.profiler.query_current_rss_memory();
    const memory_growth = peak_memory - initial_memory;

    const stats = analyze_timings(timings);
    const throughput = calculate_safe_throughput(ITERATIONS, stats.total_time_ns);

    const result = BenchmarkResult{
        .operation_name = "Block Delete",
        .iterations = ITERATIONS,
        .total_time_ns = stats.total_time_ns,
        .min_ns = stats.min,
        .max_ns = stats.max,
        .mean_ns = stats.mean,
        .median_ns = stats.median,
        .stddev_ns = stats.stddev,
        .throughput_ops_per_sec = throughput,
        .passed_threshold = stats.mean <= BLOCK_DELETE_THRESHOLD_NS,
        .threshold_ns = BLOCK_DELETE_THRESHOLD_NS,
        .peak_memory_bytes = peak_memory,
        .memory_growth_bytes = memory_growth,
        .memory_efficient = peak_memory <= MAX_PEAK_MEMORY_BYTES and memory_growth <= (MAX_MEMORY_GROWTH_PER_OP * ITERATIONS),
    };

    return result;
}

fn benchmark_wal_flush(storage_engine: *StorageEngine, allocator: std.mem.Allocator) !BenchmarkResult {
    const initial_memory = kausaldb.profiler.query_current_rss_memory();
    var timings = try allocator.alloc(u64, HIGH_PRECISION_ITERATIONS);
    defer allocator.free(timings);

    for (0..HIGH_PRECISION_ITERATIONS) |i| {
        const start_time = std.time.nanoTimestamp();
        try storage_engine.flush_wal();
        const end_time = std.time.nanoTimestamp();

        timings[i] = @intCast(end_time - start_time);
    }

    const peak_memory = kausaldb.profiler.query_current_rss_memory();
    const memory_growth = peak_memory - initial_memory;

    const stats = analyze_timings(timings);
    const throughput = calculate_safe_throughput(HIGH_PRECISION_ITERATIONS, stats.total_time_ns);

    const result = BenchmarkResult{
        .operation_name = "WAL Flush",
        .iterations = HIGH_PRECISION_ITERATIONS,
        .total_time_ns = stats.total_time_ns,
        .min_ns = stats.min,
        .max_ns = stats.max,
        .mean_ns = stats.mean,
        .median_ns = stats.median,
        .stddev_ns = stats.stddev,
        .throughput_ops_per_sec = throughput,
        .passed_threshold = stats.mean <= WAL_FLUSH_THRESHOLD_NS,
        .threshold_ns = WAL_FLUSH_THRESHOLD_NS,
        .peak_memory_bytes = peak_memory,
        .memory_growth_bytes = memory_growth,
        .memory_efficient = peak_memory <= MAX_PEAK_MEMORY_BYTES and memory_growth <= (MAX_MEMORY_GROWTH_PER_OP * HIGH_PRECISION_ITERATIONS),
    };

    return result;
}

fn benchmark_zero_cost_ownership(storage_engine: *StorageEngine, allocator: std.mem.Allocator) !BenchmarkResult {
    // Setup test data - populate storage with blocks for reading
    const block_ids = try setup_read_test_blocks(storage_engine, allocator);
    defer allocator.free(block_ids);

    const initial_memory = kausaldb.profiler.query_current_rss_memory();
    var zero_cost_timings = try allocator.alloc(u64, HIGH_PRECISION_ITERATIONS);
    var runtime_timings = try allocator.alloc(u64, HIGH_PRECISION_ITERATIONS);
    defer allocator.free(zero_cost_timings);
    defer allocator.free(runtime_timings);

    // Warmup for zero-cost approach
    for (0..WARMUP_ITERATIONS) |i| {
        const block_id = block_ids[i % block_ids.len];
        _ = try storage_engine.find_storage_block(block_id);
    }

    // Benchmark zero-cost compile-time ownership
    for (0..HIGH_PRECISION_ITERATIONS) |i| {
        const block_id = block_ids[i % block_ids.len];
        const start_time = std.time.nanoTimestamp();
        _ = try storage_engine.find_storage_block(block_id);
        const end_time = std.time.nanoTimestamp();
        zero_cost_timings[i] = @intCast(end_time - start_time);
    }

    // Warmup for runtime approach
    for (0..WARMUP_ITERATIONS) |i| {
        const block_id = block_ids[i % block_ids.len];
        _ = try storage_engine.find_block_with_ownership(block_id, .storage_engine);
    }

    // Benchmark runtime ownership validation
    for (0..HIGH_PRECISION_ITERATIONS) |i| {
        const block_id = block_ids[i % block_ids.len];
        const start_time = std.time.nanoTimestamp();
        _ = try storage_engine.find_block_with_ownership(block_id, .storage_engine);
        const end_time = std.time.nanoTimestamp();
        runtime_timings[i] = @intCast(end_time - start_time);
    }

    const peak_memory = kausaldb.profiler.query_current_rss_memory();
    const memory_growth = peak_memory - initial_memory;

    const zero_cost_stats = analyze_timings(zero_cost_timings);
    const runtime_stats = analyze_timings(runtime_timings);

    // Calculate performance improvement
    const improvement_ratio = @as(f64, @floatFromInt(runtime_stats.mean)) / @as(f64, @floatFromInt(zero_cost_stats.mean));
    const throughput = calculate_safe_throughput(HIGH_PRECISION_ITERATIONS, zero_cost_stats.total_time_ns);

    // Verify zero-cost approach has no significant regression (should be within 10% of runtime)
    const performance_improved = zero_cost_stats.mean < runtime_stats.mean;
    const runtime_threshold = @as(u64, @intFromFloat(@as(f64, @floatFromInt(runtime_stats.mean)) * 1.1));
    const no_regression = zero_cost_stats.mean <= runtime_threshold;

    // Log comparison results
    if (builtin.mode == .Debug) {
        std.log.info("Zero-cost ownership benchmark:", .{});
        std.log.info("  Zero-cost mean: {}ns", .{zero_cost_stats.mean});
        std.log.info("  Runtime mean: {}ns", .{runtime_stats.mean});
        std.log.info("  Improvement ratio: {d:.2}x", .{improvement_ratio});
        std.log.info("  Performance improved: {}", .{performance_improved});
    }

    const result = BenchmarkResult{
        .operation_name = "Zero-Cost Ownership Read",
        .iterations = HIGH_PRECISION_ITERATIONS,
        .total_time_ns = zero_cost_stats.total_time_ns,
        .min_ns = zero_cost_stats.min,
        .max_ns = zero_cost_stats.max,
        .mean_ns = zero_cost_stats.mean,
        .median_ns = zero_cost_stats.median,
        .stddev_ns = zero_cost_stats.stddev,
        .throughput_ops_per_sec = throughput,
        .passed_threshold = zero_cost_stats.mean <= BLOCK_READ_THRESHOLD_NS and no_regression,
        .threshold_ns = BLOCK_READ_THRESHOLD_NS,
        .peak_memory_bytes = peak_memory,
        .memory_growth_bytes = memory_growth,
        .memory_efficient = peak_memory <= MAX_PEAK_MEMORY_BYTES and memory_growth <= (MAX_MEMORY_GROWTH_PER_OP * HIGH_PRECISION_ITERATIONS * 2),
    };

    return result;
}

fn create_test_block(allocator: std.mem.Allocator, index: usize) !ContextBlock {
    const block_id_hex = try std.fmt.allocPrint(allocator, "{x:0>32}", .{index});
    defer allocator.free(block_id_hex);

    const block_id = try BlockId.from_hex(block_id_hex);
    const source_uri = try std.fmt.allocPrint(allocator, "benchmark://test_block_{}.zig", .{index});
    const metadata_json = try std.fmt.allocPrint(allocator, "{{\"type\":\"test\",\"index\":{}}}", .{index});
    const content = try std.fmt.allocPrint(allocator, "Test block content for benchmark iteration {}", .{index});

    return ContextBlock{
        .id = block_id,
        .version = 1,
        .source_uri = source_uri,
        .metadata_json = metadata_json,
        .content = content,
    };
}

fn free_test_block(allocator: std.mem.Allocator, block: ContextBlock) void {
    allocator.free(block.source_uri);
    allocator.free(block.metadata_json);
    allocator.free(block.content);
}

fn create_updated_test_block(allocator: std.mem.Allocator, block_id: BlockId, version: usize) !ContextBlock {
    const source_uri = try std.fmt.allocPrint(allocator, "benchmark://updated_block_{}.zig", .{version});
    const metadata_json = try std.fmt.allocPrint(allocator, "{{\"type\":\"updated\",\"version\":{}}}", .{version});
    const content = try std.fmt.allocPrint(allocator, "Updated test block content version {}", .{version});

    return ContextBlock{
        .id = block_id,
        .version = @intCast(version + 1),
        .source_uri = source_uri,
        .metadata_json = metadata_json,
        .content = content,
    };
}

fn setup_read_test_blocks(storage_engine: *StorageEngine, allocator: std.mem.Allocator) ![]BlockId {
    const block_count = 100;
    var block_ids = try allocator.alloc(BlockId, block_count);

    for (0..block_count) |i| {
        const block = try create_test_block(allocator, i + 50000);
        defer free_test_block(allocator, block);

        block_ids[i] = block.id;
        _ = try storage_engine.put_block(block);
    }

    return block_ids;
}

fn setup_delete_test_blocks(storage_engine: *StorageEngine, allocator: std.mem.Allocator) ![]BlockId {
    const block_count = ITERATIONS + WARMUP_ITERATIONS;
    var block_ids = try allocator.alloc(BlockId, block_count);

    for (0..block_count) |i| {
        const block = try create_test_block(allocator, i + 60000);
        defer free_test_block(allocator, block);

        block_ids[i] = block.id;
        _ = try storage_engine.put_block(block);
    }

    return block_ids;
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

    // Filter outliers using IQR method
    const q1_idx = timings.len / 4;
    const q3_idx = (3 * timings.len) / 4;
    const q1 = timings[q1_idx];
    const q3 = timings[q3_idx];
    const iqr = q3 - q1;

    // Only filter if we have enough data and significant outliers
    const outlier_threshold = if (iqr > 1000) iqr * 3 / 2 else std.math.maxInt(u64); // 1.5 * IQR, only if IQR > 1µs
    const lower_bound = if (q1 > outlier_threshold) q1 - outlier_threshold else 0;
    const upper_bound = q3 + outlier_threshold;

    // Count filtered values
    var filtered_count: usize = 0;
    var filtered_total: u64 = 0;
    var filtered_min: u64 = std.math.maxInt(u64);
    var filtered_max: u64 = 0;

    for (timings) |time| {
        if (time >= lower_bound and time <= upper_bound) {
            filtered_total += time;
            filtered_count += 1;
            filtered_min = @min(filtered_min, time);
            filtered_max = @max(filtered_max, time);
        }
    }

    // Use filtered data if we removed outliers, otherwise use all data
    if (filtered_count < timings.len and filtered_count > timings.len / 2) {
        const median = timings[timings.len / 2];
        const mean = filtered_total / filtered_count;

        var variance_sum: u64 = 0;
        var variance_count: usize = 0;
        for (timings) |time| {
            if (time >= lower_bound and time <= upper_bound) {
                const diff = if (time > mean) time - mean else mean - time;
                variance_sum += diff * diff;
                variance_count += 1;
            }
        }
        const variance = if (variance_count > 0) variance_sum / variance_count else 0;
        const stddev = std.math.sqrt(variance);

        return .{
            .total_time_ns = filtered_total,
            .min = filtered_min,
            .max = filtered_max,
            .mean = mean,
            .median = median,
            .stddev = stddev,
        };
    } else {
        // No significant outliers or too few samples, use all data
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
}
