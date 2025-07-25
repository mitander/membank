//! Comprehensive performance benchmarking framework for CortexDB.
//!
//! Provides automated regression testing to ensure performance doesn't degrade
//! as features are added. Benchmarks core operations with statistical analysis.

const std = @import("std");
const assert = std.debug.assert;
const log = std.log.scoped(.benchmark);

const storage = @import("../storage/storage.zig");
const query_engine = @import("../query/query_engine.zig");
const context_block = @import("../core/types.zig");
const simulation_vfs = @import("../sim/simulation_vfs.zig");

const StorageEngine = storage.StorageEngine;
const QueryEngine = query_engine.QueryEngine;
const ContextBlock = context_block.ContextBlock;
const BlockId = context_block.BlockId;
const GraphEdge = context_block.GraphEdge;
const EdgeType = context_block.EdgeType;

// Benchmark configuration
const BENCHMARK_ITERATIONS = 1000;
const WARMUP_ITERATIONS = 100;
const LARGE_BENCHMARK_ITERATIONS = 100;
const STATISTICAL_SAMPLES = 10;

// Performance thresholds (nanoseconds)
const BLOCK_WRITE_THRESHOLD_NS = 50_000; // 50µs
const BLOCK_READ_THRESHOLD_NS = 10_000; // 10µs
const QUERY_BATCH_THRESHOLD_NS = 100_000; // 100µs
const WAL_FLUSH_THRESHOLD_NS = 1_000_000; // 1ms

/// Benchmark results with statistical analysis
const BenchmarkResult = struct {
    operation_name: []const u8,
    iterations: u64,
    total_time_ns: u64,
    min_ns: u64,
    max_ns: u64,
    mean_ns: u64,
    median_ns: u64,
    stddev_ns: u64,
    throughput_ops_per_sec: f64,
    passed_threshold: bool,
    threshold_ns: u64,

    pub fn print_results(self: BenchmarkResult) void {
        const status = if (self.passed_threshold) "PASS" else "FAIL";
        const status_color = if (self.passed_threshold) "\x1b[32m" else "\x1b[31m";

        std.debug.print("\n=== {s} Benchmark ===\n", .{self.operation_name});
        std.debug.print("Iterations: {}\n", .{self.iterations});
        const total_time_ms = @as(f64, @floatFromInt(self.total_time_ns)) / 1_000_000.0;
        std.debug.print("Total time: {d:.2}ms\n", .{total_time_ms});
        const mean_us = @as(f64, @floatFromInt(self.mean_ns)) / 1000.0;
        const median_us = @as(f64, @floatFromInt(self.median_ns)) / 1000.0;
        const min_us = @as(f64, @floatFromInt(self.min_ns)) / 1000.0;
        const max_us = @as(f64, @floatFromInt(self.max_ns)) / 1000.0;
        const stddev_us = @as(f64, @floatFromInt(self.stddev_ns)) / 1000.0;
        std.debug.print("Mean:       {}ns ({d:.2}µs)\n", .{ self.mean_ns, mean_us });
        std.debug.print("Median:     {}ns ({d:.2}µs)\n", .{ self.median_ns, median_us });
        std.debug.print("Min:        {}ns ({d:.2}µs)\n", .{ self.min_ns, min_us });
        std.debug.print("Max:        {}ns ({d:.2}µs)\n", .{ self.max_ns, max_us });
        std.debug.print("Std dev:    {}ns ({d:.2}µs)\n", .{ self.stddev_ns, stddev_us });
        std.debug.print("Throughput: {d:.1} ops/sec\n", .{self.throughput_ops_per_sec});
        const threshold_us = @as(f64, @floatFromInt(self.threshold_ns)) / 1000.0;
        std.debug.print("Threshold:  {}ns ({d:.2}µs)\n", .{ self.threshold_ns, threshold_us });
        std.debug.print("Status:     {s}[{s}]\x1b[0m\n", .{ status_color, status });
    }

    pub fn print_json(self: BenchmarkResult) void {
        std.debug.print("{{", .{});
        std.debug.print("\"operation_name\":\"{s}\",", .{self.operation_name});
        std.debug.print("\"iterations\":{},", .{self.iterations});
        std.debug.print("\"total_time_ns\":{},", .{self.total_time_ns});
        std.debug.print("\"min_ns\":{},", .{self.min_ns});
        std.debug.print("\"max_ns\":{},", .{self.max_ns});
        std.debug.print("\"mean_ns\":{},", .{self.mean_ns});
        std.debug.print("\"median_ns\":{},", .{self.median_ns});
        std.debug.print("\"stddev_ns\":{},", .{self.stddev_ns});
        std.debug.print("\"throughput_ops_per_sec\":{d:.1},", .{self.throughput_ops_per_sec});
        std.debug.print("\"passed_threshold\":{},", .{self.passed_threshold});
        std.debug.print("\"threshold_ns\":{}", .{self.threshold_ns});
        std.debug.print("}}", .{});
    }
};

/// Statistical analyzer for benchmark timing data
const StatisticalAnalyzer = struct {
    samples: std.ArrayList(u64),
    allocator: std.mem.Allocator,

    pub fn init(allocator: std.mem.Allocator) StatisticalAnalyzer {
        return StatisticalAnalyzer{
            .samples = std.ArrayList(u64).init(allocator),
            .allocator = allocator,
        };
    }

    pub fn deinit(self: *StatisticalAnalyzer) void {
        self.samples.deinit();
    }

    pub fn add_sample(self: *StatisticalAnalyzer, sample_ns: u64) !void {
        try self.samples.append(sample_ns);
    }

    pub fn analyze(
        self: *StatisticalAnalyzer,
        operation_name: []const u8,
        threshold_ns: u64,
    ) BenchmarkResult {
        assert(self.samples.items.len > 0);

        // Sort for median calculation
        std.sort.pdq(u64, self.samples.items, {}, std.sort.asc(u64));

        const min_ns = self.samples.items[0];
        const max_ns = self.samples.items[self.samples.items.len - 1];

        // Calculate mean
        var sum: u64 = 0;
        for (self.samples.items) |sample| {
            sum += sample;
        }
        const mean_ns = sum / self.samples.items.len;

        // Calculate median
        const median_ns = if (self.samples.items.len % 2 == 0) blk: {
            const mid = self.samples.items.len / 2;
            break :blk (self.samples.items[mid - 1] + self.samples.items[mid]) / 2;
        } else blk: {
            break :blk self.samples.items[self.samples.items.len / 2];
        };

        // Calculate standard deviation
        var variance_sum: u64 = 0;
        for (self.samples.items) |sample| {
            const diff = if (sample > mean_ns) sample - mean_ns else mean_ns - sample;
            variance_sum += diff * diff;
        }
        const variance = variance_sum / self.samples.items.len;
        const stddev_ns = @as(u64, @intFromFloat(@sqrt(@as(f64, @floatFromInt(variance)))));

        const total_time_ns = sum;
        const iterations = self.samples.items.len;
        const iterations_f = @as(f64, @floatFromInt(iterations));
        const total_time_f = @as(f64, @floatFromInt(total_time_ns));
        const throughput_ops_per_sec = (iterations_f * 1_000_000_000.0) / total_time_f;
        const passed_threshold = mean_ns <= threshold_ns;

        return BenchmarkResult{
            .operation_name = operation_name,
            .iterations = @as(u64, @intCast(iterations)),
            .total_time_ns = total_time_ns,
            .min_ns = min_ns,
            .max_ns = max_ns,
            .mean_ns = mean_ns,
            .median_ns = median_ns,
            .stddev_ns = stddev_ns,
            .throughput_ops_per_sec = throughput_ops_per_sec,
            .passed_threshold = passed_threshold,
            .threshold_ns = threshold_ns,
        };
    }
};

// Global state for output format
var json_output = false;
var all_results = std.ArrayList(BenchmarkResult).init(std.heap.page_allocator);

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    const args = try std.process.argsAlloc(allocator);
    defer std.process.argsFree(allocator, args);

    if (args.len < 2) {
        try print_usage();
        return;
    }

    // Parse command line options
    var benchmark_name: []const u8 = "";
    var i: usize = 1;
    while (i < args.len) : (i += 1) {
        const arg = args[i];
        if (std.mem.eql(u8, arg, "--json")) {
            json_output = true;
        } else if (benchmark_name.len == 0) {
            benchmark_name = arg;
        }
    }

    if (benchmark_name.len == 0) {
        try print_usage();
        return;
    }

    if (!json_output) {
        std.debug.print("\nCortexDB Performance Benchmark Suite\n", .{});
        std.debug.print("=========================================\n", .{});
    }

    if (std.mem.eql(u8, benchmark_name, "storage")) {
        try run_storage_benchmarks(allocator);
    } else if (std.mem.eql(u8, benchmark_name, "query")) {
        try run_query_benchmarks(allocator);
    } else if (std.mem.eql(u8, benchmark_name, "compaction")) {
        try run_compaction_benchmarks(allocator);
    } else if (std.mem.eql(u8, benchmark_name, "all")) {
        try run_all_benchmarks(allocator);
    } else {
        std.debug.print("Unknown benchmark: {s}\n", .{benchmark_name});
        try print_usage();
        std.process.exit(1);
    }

    if (json_output) {
        std.debug.print("{{\n", .{});
        std.debug.print("  \"benchmark_suite\": \"CortexDB Performance\",\n", .{});
        std.debug.print("  \"timestamp\": {},\n", .{std.time.timestamp()});
        std.debug.print("  \"results\": [\n", .{});
        for (all_results.items, 0..) |result, idx| {
            std.debug.print("    ", .{});
            result.print_json();
            if (idx < all_results.items.len - 1) {
                std.debug.print(",", .{});
            }
            std.debug.print("\n", .{});
        }
        std.debug.print("  ]\n", .{});
        std.debug.print("}}\n", .{});
    } else {
        std.debug.print("\nBenchmark suite completed successfully!\n", .{});
    }
}

fn print_usage() !void {
    std.debug.print(
        \\CortexDB Performance Benchmark Suite
        \\
        \\Usage:
        \\  benchmark <category> [--json]
        \\
        \\Categories:
        \\  storage      Storage engine operations (read/write/delete)
        \\  query        Query engine operations (batch queries, single queries)
        \\  compaction   Compaction operations (WAL flush, SSTable operations)
        \\  all          Run all benchmark categories
        \\
        \\Options:
        \\  --json       Output results in JSON format for CI/regression testing
        \\
        \\Examples:
        \\  benchmark storage
        \\  benchmark all --json
        \\
    , .{});
}

fn store_and_print_result(result: BenchmarkResult) !void {
    try all_results.append(result);
    if (!json_output) {
        try store_and_print_result(result);
    }
}

fn run_all_benchmarks(allocator: std.mem.Allocator) !void {
    if (!json_output) {
        std.debug.print("Running comprehensive benchmark suite...\n", .{});
    }

    try run_storage_benchmarks(allocator);
    try run_query_benchmarks(allocator);
    try run_compaction_benchmarks(allocator);
}

fn run_storage_benchmarks(allocator: std.mem.Allocator) !void {
    if (!json_output) {
        std.debug.print("\nStorage Engine Benchmarks\n", .{});
        std.debug.print("============================\n", .{});
    }

    // Initialize storage engine
    var sim_vfs = try simulation_vfs.SimulationVFS.init(allocator);
    defer sim_vfs.deinit();

    var storage_engine = try StorageEngine.init_default(allocator, sim_vfs.vfs(), "benchmark_data");
    defer storage_engine.deinit();

    try storage_engine.startup();

    // Block write benchmark
    try benchmark_block_writes(&storage_engine, allocator);

    // Block read benchmark
    try benchmark_block_reads(&storage_engine, allocator);

    // Block update benchmark
    try benchmark_block_updates(&storage_engine, allocator);

    // Block delete benchmark
    try benchmark_block_deletes(&storage_engine, allocator);
}

fn run_query_benchmarks(allocator: std.mem.Allocator) !void {
    if (!json_output) {
        std.debug.print("\nQuery Engine Benchmarks\n", .{});
        std.debug.print("==========================\n", .{});
    }

    // Initialize engines
    var sim_vfs = try simulation_vfs.SimulationVFS.init(allocator);
    defer sim_vfs.deinit();

    var storage_engine = try StorageEngine.init_default(allocator, sim_vfs.vfs(), "query_benchmark_data");
    defer storage_engine.deinit();

    try storage_engine.startup();

    var query_eng = QueryEngine.init(allocator, &storage_engine);
    defer query_eng.deinit();

    // Setup test data
    try setup_query_test_data(&storage_engine);

    // Single block query benchmark
    try benchmark_single_block_queries(&query_eng, allocator);

    // Batch query benchmark
    try benchmark_batch_queries(&query_eng, allocator);
}

fn run_compaction_benchmarks(allocator: std.mem.Allocator) !void {
    if (!json_output) {
        std.debug.print("\nCompaction Operations Benchmarks\n", .{});
        std.debug.print("===================================\n", .{});
    }

    // Initialize storage engine
    var sim_vfs = try simulation_vfs.SimulationVFS.init(allocator);
    defer sim_vfs.deinit();

    const data_dir = "compaction_benchmark_data";
    var storage_engine = try StorageEngine.init_default(allocator, sim_vfs.vfs(), data_dir);
    defer storage_engine.deinit();

    try storage_engine.startup();

    // Setup compaction test data
    try setup_compaction_test_data(&storage_engine);

    // WAL flush benchmark
    try benchmark_wal_flush(&storage_engine, allocator);
}

// Storage benchmarks implementation

fn benchmark_block_writes(storage_engine: *StorageEngine, allocator: std.mem.Allocator) !void {
    var analyzer = StatisticalAnalyzer.init(allocator);
    defer analyzer.deinit();

    // Warmup
    for (0..WARMUP_ITERATIONS) |i| {
        const block = try create_test_block(allocator, i);
        defer free_test_block(allocator, block);
        try storage_engine.put_block(block);
    }

    // Benchmark
    for (0..STATISTICAL_SAMPLES) |sample| {
        const start_time = std.time.nanoTimestamp();

        for (0..LARGE_BENCHMARK_ITERATIONS) |i| {
            const offset = sample * LARGE_BENCHMARK_ITERATIONS + i;
            const block = try create_test_block(allocator, offset + 10000);
            defer free_test_block(allocator, block);
            try storage_engine.put_block(block);
        }

        const end_time = std.time.nanoTimestamp();
        const batch_time = @as(u64, @intCast(end_time - start_time));
        const per_op_time = batch_time / LARGE_BENCHMARK_ITERATIONS;

        try analyzer.add_sample(per_op_time);
    }

    const result = analyzer.analyze("Block Write", BLOCK_WRITE_THRESHOLD_NS);
    try store_and_print_result(result);
}

fn benchmark_block_reads(storage_engine: *StorageEngine, allocator: std.mem.Allocator) !void {
    var analyzer = StatisticalAnalyzer.init(allocator);
    defer analyzer.deinit();

    // Setup test blocks
    const test_ids = try setup_read_test_blocks(storage_engine, allocator);
    defer allocator.free(test_ids);

    // Warmup
    for (0..WARMUP_ITERATIONS) |i| {
        const block_id = test_ids[i % test_ids.len];
        _ = try storage_engine.find_block_by_id(block_id);
    }

    // Benchmark
    for (0..STATISTICAL_SAMPLES) |_| {
        const start_time = std.time.nanoTimestamp();

        for (0..LARGE_BENCHMARK_ITERATIONS) |i| {
            const block_id = test_ids[i % test_ids.len];
            _ = try storage_engine.find_block_by_id(block_id);
        }

        const end_time = std.time.nanoTimestamp();
        const batch_time = @as(u64, @intCast(end_time - start_time));
        const per_op_time = batch_time / LARGE_BENCHMARK_ITERATIONS;

        try analyzer.add_sample(per_op_time);
    }

    const result = analyzer.analyze("Block Read", BLOCK_READ_THRESHOLD_NS);
    try store_and_print_result(result);
}

fn benchmark_block_updates(storage_engine: *StorageEngine, allocator: std.mem.Allocator) !void {
    var analyzer = StatisticalAnalyzer.init(allocator);
    defer analyzer.deinit();

    // Setup initial blocks
    const test_ids = try setup_read_test_blocks(storage_engine, allocator);
    defer allocator.free(test_ids);

    // Warmup
    for (0..WARMUP_ITERATIONS) |i| {
        const block_id = test_ids[i % test_ids.len];
        const updated_block = try create_updated_test_block(allocator, block_id, i);
        defer free_test_block(allocator, updated_block);
        try storage_engine.put_block(updated_block);
    }

    // Benchmark
    for (0..STATISTICAL_SAMPLES) |sample| {
        const start_time = std.time.nanoTimestamp();

        for (0..LARGE_BENCHMARK_ITERATIONS) |i| {
            const block_id = test_ids[i % test_ids.len];
            const version_offset = sample * LARGE_BENCHMARK_ITERATIONS + i;
            const updated_block = try create_updated_test_block(
                allocator,
                block_id,
                version_offset,
            );
            defer free_test_block(allocator, updated_block);
            try storage_engine.put_block(updated_block);
        }

        const end_time = std.time.nanoTimestamp();
        const batch_time = @as(u64, @intCast(end_time - start_time));
        const per_op_time = batch_time / LARGE_BENCHMARK_ITERATIONS;

        try analyzer.add_sample(per_op_time);
    }

    const result = analyzer.analyze("Block Update", BLOCK_WRITE_THRESHOLD_NS);
    try store_and_print_result(result);
}

fn benchmark_block_deletes(storage_engine: *StorageEngine, allocator: std.mem.Allocator) !void {
    var analyzer = StatisticalAnalyzer.init(allocator);
    defer analyzer.deinit();

    // Benchmark
    for (0..STATISTICAL_SAMPLES) |sample| {
        // Setup blocks for deletion
        const delete_ids = try setup_delete_test_blocks(storage_engine, allocator, sample);
        defer allocator.free(delete_ids);

        const start_time = std.time.nanoTimestamp();

        for (delete_ids) |block_id| {
            try storage_engine.delete_block(block_id);
        }

        const end_time = std.time.nanoTimestamp();
        const batch_time = @as(u64, @intCast(end_time - start_time));
        const per_op_time = batch_time / delete_ids.len;

        try analyzer.add_sample(per_op_time);
    }

    const result = analyzer.analyze("Block Delete", BLOCK_WRITE_THRESHOLD_NS);
    try store_and_print_result(result);
}

// Query benchmarks implementation

fn benchmark_single_block_queries(query_eng: *QueryEngine, allocator: std.mem.Allocator) !void {
    var analyzer = StatisticalAnalyzer.init(allocator);
    defer analyzer.deinit();

    const test_ids = try create_query_test_block_ids(allocator);
    defer allocator.free(test_ids);

    // Warmup
    for (0..WARMUP_ITERATIONS) |i| {
        const block_id = test_ids[i % test_ids.len];
        const result = try query_eng.find_block_by_id(block_id);
        defer result.deinit();
    }

    // Benchmark
    for (0..STATISTICAL_SAMPLES) |_| {
        const start_time = std.time.nanoTimestamp();

        for (0..LARGE_BENCHMARK_ITERATIONS) |i| {
            const block_id = test_ids[i % test_ids.len];
            const result = try query_eng.find_block_by_id(block_id);
            defer result.deinit();
        }

        const end_time = std.time.nanoTimestamp();
        const batch_time = @as(u64, @intCast(end_time - start_time));
        const per_op_time = batch_time / LARGE_BENCHMARK_ITERATIONS;

        try analyzer.add_sample(per_op_time);
    }

    const result = analyzer.analyze("Single Block Query", BLOCK_READ_THRESHOLD_NS);
    try store_and_print_result(result);
}

fn benchmark_batch_queries(query_eng: *QueryEngine, allocator: std.mem.Allocator) !void {
    var analyzer = StatisticalAnalyzer.init(allocator);
    defer analyzer.deinit();

    const test_ids = try create_query_test_block_ids(allocator);
    defer allocator.free(test_ids);

    const batch_size = 10;
    const batch_query = query_engine.GetBlocksQuery{
        .block_ids = test_ids[0..batch_size],
    };

    // Warmup
    for (0..WARMUP_ITERATIONS) |_| {
        const result = try query_eng.execute_get_blocks(batch_query);
        defer result.deinit();
    }

    // Benchmark
    for (0..STATISTICAL_SAMPLES) |_| {
        const start_time = std.time.nanoTimestamp();

        for (0..LARGE_BENCHMARK_ITERATIONS) |_| {
            const result = try query_eng.execute_get_blocks(batch_query);
            defer result.deinit();
        }

        const end_time = std.time.nanoTimestamp();
        const batch_time = @as(u64, @intCast(end_time - start_time));
        const per_op_time = batch_time / LARGE_BENCHMARK_ITERATIONS;

        try analyzer.add_sample(per_op_time);
    }

    const result = analyzer.analyze("Batch Query (10 blocks)", QUERY_BATCH_THRESHOLD_NS);
    try store_and_print_result(result);
}

// Compaction benchmarks implementation

fn benchmark_wal_flush(storage_engine: *StorageEngine, allocator: std.mem.Allocator) !void {
    var analyzer = StatisticalAnalyzer.init(allocator);
    defer analyzer.deinit();

    // Benchmark
    for (0..STATISTICAL_SAMPLES) |_| {
        // Add some data to flush
        for (0..10) |i| {
            const block = try create_test_block(allocator, i + 50000);
            defer free_test_block(allocator, block);
            try storage_engine.put_block(block);
        }

        const start_time = std.time.nanoTimestamp();
        try storage_engine.flush_wal();
        const end_time = std.time.nanoTimestamp();

        const operation_time = @as(u64, @intCast(end_time - start_time));
        try analyzer.add_sample(operation_time);
    }

    const result = analyzer.analyze("WAL Flush", WAL_FLUSH_THRESHOLD_NS);
    try store_and_print_result(result);
}

// Helper functions for test data setup

fn create_test_block(allocator: std.mem.Allocator, index: usize) !ContextBlock {
    const block_id_hex = try std.fmt.allocPrint(allocator, "b{x:0>31}", .{index});
    const source_uri = try std.fmt.allocPrint(allocator, "benchmark://test/block_{}.zig", .{index});
    const metadata_json = try std.fmt.allocPrint(
        allocator,
        "{{\"benchmark\":true,\"index\":{}}}",
        .{index},
    );
    const content = try std.fmt.allocPrint(
        allocator,
        "pub fn benchmark_function_{}() void {{ /* Block {} content */ }}",
        .{ index, index },
    );

    return ContextBlock{
        .id = try BlockId.from_hex(block_id_hex),
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

fn create_updated_test_block(
    allocator: std.mem.Allocator,
    block_id: BlockId,
    version: usize,
) !ContextBlock {
    const source_uri = try std.fmt.allocPrint(allocator, "benchmark://test/updated_block.zig", .{});
    const metadata_json = try std.fmt.allocPrint(
        allocator,
        "{{\"benchmark\":true,\"updated\":true,\"version\":{}}}",
        .{version},
    );
    const content = try std.fmt.allocPrint(
        allocator,
        "pub fn updated_benchmark_function_{}() void {{ /* Updated content */ }}",
        .{version},
    );

    return ContextBlock{
        .id = block_id,
        .version = @as(u64, @intCast(version + 2)),
        .source_uri = source_uri,
        .metadata_json = metadata_json,
        .content = content,
    };
}

fn setup_read_test_blocks(storage_engine: *StorageEngine, allocator: std.mem.Allocator) ![]BlockId {
    const num_blocks = 100;
    var block_ids = try allocator.alloc(BlockId, num_blocks);

    for (0..num_blocks) |i| {
        const block = try create_test_block(allocator, i + 1000);
        defer free_test_block(allocator, block);

        block_ids[i] = block.id;
        try storage_engine.put_block(block);
    }

    return block_ids;
}

fn setup_delete_test_blocks(
    storage_engine: *StorageEngine,
    allocator: std.mem.Allocator,
    batch: usize,
) ![]BlockId {
    const num_blocks = 50;
    var block_ids = try allocator.alloc(BlockId, num_blocks);

    for (0..num_blocks) |i| {
        const index = batch * 1000 + i + 2000;
        const block = try create_test_block(allocator, index);
        defer free_test_block(allocator, block);

        block_ids[i] = block.id;
        try storage_engine.put_block(block);
    }

    return block_ids;
}

fn setup_query_test_data(storage_engine: *StorageEngine) !void {
    // Query benchmarks will use blocks already created by storage benchmarks
    _ = storage_engine;
}

fn create_query_test_block_ids(allocator: std.mem.Allocator) ![]BlockId {
    const num_blocks = 50;
    var block_ids = try allocator.alloc(BlockId, num_blocks);

    for (0..num_blocks) |i| {
        const block_id_hex = try std.fmt.allocPrint(allocator, "b{x:0>31}", .{i + 1000});
        defer allocator.free(block_id_hex);
        block_ids[i] = try BlockId.from_hex(block_id_hex);
    }

    return block_ids;
}

fn setup_compaction_test_data(storage_engine: *StorageEngine) !void {
    // Compaction benchmarks will add their own test data
    _ = storage_engine;
}

test "benchmark framework tests" {
    // Test statistical analyzer
    var analyzer = StatisticalAnalyzer.init(std.testing.allocator);
    defer analyzer.deinit();

    try analyzer.add_sample(1000);
    try analyzer.add_sample(2000);
    try analyzer.add_sample(1500);

    const result = analyzer.analyze("Test Operation", 2500);
    try std.testing.expect(result.passed_threshold);
    try std.testing.expectEqual(@as(u64, 1500), result.mean_ns);
    try std.testing.expectEqual(@as(u64, 1500), result.median_ns);
}
