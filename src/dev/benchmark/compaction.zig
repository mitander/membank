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

    // Use unique directory to avoid conflicts with previous runs
    const timestamp = std.time.microTimestamp();
    const unique_dir = try std.fmt.allocPrint(allocator, "/tmp/kausaldb-tests/benchmark_compaction_{d}", .{timestamp});
    defer allocator.free(unique_dir);

    // Ensure parent directory exists
    try std.fs.cwd().makePath("/tmp/kausaldb-tests");

    var storage_engine = try StorageEngine.init_default(allocator, prod_vfs.vfs(), unique_dir);
    defer storage_engine.deinit();

    try storage_engine.startup();

    const result = benchmark_compaction_operations(&storage_engine, allocator);

    // Validate SSTable integrity before cleanup
    validate_sstable_integrity(allocator, unique_dir) catch |err| {
        std.debug.print("ERROR: SSTable integrity validation failed: {any}\n", .{err});
        // Don't fail the benchmark, but log the issue for investigation
    };

    // Clean up test directory (best effort)
    std.fs.cwd().deleteTree(unique_dir) catch |err| {
        std.debug.print("Warning: Failed to clean up compaction benchmark directory {s}: {any}\n", .{ unique_dir, err });
    };

    return result;
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
    const source_uri = try std.fmt.allocPrint(allocator, "compaction://test_block_{d}.zig", .{index});
    const metadata_json = try std.fmt.allocPrint(allocator, "{{\"type\":\"compaction_test\",\"index\":{d}}}", .{index});

    const content = try std.fmt.allocPrint(allocator,
        \\pub fn compaction_test_function_{d}() void {{
        \\    // This is test content for compaction benchmarking
        \\    // Index: {d}
        \\    // Content is intentionally verbose to simulate real blocks
        \\    var data: [100]u8 = undefined;
        \\    for (data, 0..) |*byte, i| {{
        \\        byte.* = @as(u8, @intCast(i % 256));
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

/// Validate integrity of all SSTable files in the directory
///
/// This function prevents silent SSTable corruption by validating headers
/// and basic structure. Follows KausalDB principle: "Correctness is Not Negotiable"
fn validate_sstable_integrity(allocator: std.mem.Allocator, test_dir: []const u8) !void {
    var dir = std.fs.cwd().openDir(test_dir, .{ .iterate = true }) catch |err| {
        // Directory might not exist if no SSTables were created
        if (err == error.FileNotFound) return;
        return err;
    };
    defer dir.close();

    var sstable_count: u32 = 0;
    var iterator = dir.iterate();
    while (try iterator.next()) |entry| {
        if (std.mem.endsWith(u8, entry.name, ".sstable")) {
            sstable_count += 1;

            const sstable_path = try std.fmt.allocPrint(allocator, "{s}/{s}", .{ test_dir, entry.name });
            defer allocator.free(sstable_path);

            // Validate SSTable header using the same logic as SSTableManager.read_sstable_block_count
            try validate_single_sstable_header(sstable_path);
        }
    }

    if (sstable_count == 0) {
        std.debug.print("Warning: No SSTables found in {s} - this may indicate a flush issue\n", .{test_dir});
    }
}

/// Validate a single SSTable header for corruption
///
/// Replicates the exact validation logic from SSTableManager.read_sstable_block_count
/// to catch the same errors that would cause "SSTable missing" warnings.
fn validate_single_sstable_header(sstable_path: []const u8) !void {
    var file = try std.fs.cwd().openFile(sstable_path, .{});
    defer file.close();

    // Read header exactly like SSTableManager.read_sstable_block_count
    var header_buffer: [64]u8 = undefined; // SSTable.HEADER_SIZE = 64
    const bytes_read = try file.readAll(&header_buffer);
    if (bytes_read < 64) {
        std.debug.print("CORRUPTION: SSTable {s} header too small: {d} < 64 bytes\n", .{ sstable_path, bytes_read });
        return error.InvalidSSTableHeader;
    }

    // Validate magic number and version (same as SSTable.Header.deserialize)
    var offset: usize = 0;

    // Check magic number
    const magic = header_buffer[offset .. offset + 4];
    if (!std.mem.eql(u8, magic, "SSTB")) {
        std.debug.print("CORRUPTION: SSTable {s} has invalid magic: expected 'SSTB', got '{s}'\n", .{ sstable_path, magic });
        return error.InvalidMagic;
    }
    offset += 4;

    // Check version compatibility
    const format_version = std.mem.readInt(u16, header_buffer[offset..][0..2], .little);
    if (format_version > 1) {
        std.debug.print("ERROR: SSTable {s} unsupported version: {d} > 1\n", .{ sstable_path, format_version });
        return error.UnsupportedVersion;
    }
    offset += 2;

    // Skip flags (2 bytes) and index_offset (8 bytes)
    offset += 10;

    // Block count is at offset 16 for O(1) statistics without index loading
    const block_count = std.mem.readInt(u32, header_buffer[offset..][0..4], .little);
    if (block_count == 0) {
        std.debug.print("WARNING: SSTable {s} has 0 blocks - this may indicate an empty flush\n", .{sstable_path});
    }
}
