//! Query engine performance benchmarks.
//!
//! Tests single queries, batch operations, and graph traversal performance.
//! Focuses on query execution time and memory efficiency during result processing.

const std = @import("std");
const builtin = @import("builtin");
const kausaldb = @import("kausaldb");

const storage = kausaldb.storage;
const query_engine = kausaldb.query_engine;
const context_block = kausaldb.types;
const simulation_vfs = kausaldb.simulation_vfs;

const StorageEngine = storage.StorageEngine;
const QueryEngine = query_engine.QueryEngine;
const ContextBlock = context_block.ContextBlock;
const BlockId = context_block.BlockId;
const GraphEdge = context_block.GraphEdge;
const EdgeType = context_block.EdgeType;

const SINGLE_QUERY_THRESHOLD_NS = 300; // direct storage access ~0.12µs → 300ns (2.5x margin)
const BATCH_QUERY_THRESHOLD_NS = 3_000; // 10 blocks × 300ns = 3µs (simple loop)

const MAX_PEAK_MEMORY_BYTES = 100 * 1024 * 1024;
const MAX_MEMORY_GROWTH_PER_OP = 1024;

const ITERATIONS = 1000;
const WARMUP_ITERATIONS = 50;
const BATCH_SIZE = 10;

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
    peak_memory_bytes: u64,
    memory_growth_bytes: u64,
    memory_efficient: bool,

    fn print_results(self: BenchmarkResult) void {
        const status = if (self.passed_threshold) "PASS" else "FAIL";
        const status_color = if (self.passed_threshold) "\x1b[32m" else "\x1b[31m";
        const memory_status = if (self.memory_efficient) "PASS" else "FAIL";
        const memory_color = if (self.memory_efficient) "\x1b[32m" else "\x1b[31m";

        std.debug.print("\n=== {s} Benchmark ===\n", .{self.operation_name});
        std.debug.print("Iterations: {}\n", .{self.iterations});

        const total_time_ms = @as(f64, @floatFromInt(self.total_time_ns)) / 1_000_000.0;
        std.debug.print("Total time: {d:.2}ms\n", .{total_time_ms});

        const mean_us = @as(f64, @floatFromInt(self.mean_ns)) / 1_000.0;
        const threshold_us = @as(f64, @floatFromInt(self.threshold_ns)) / 1_000.0;
        std.debug.print("Mean time: {d:.2}µs (threshold: {d:.0}µs)\n", .{ mean_us, threshold_us });

        const min_us = @as(f64, @floatFromInt(self.min_ns)) / 1_000.0;
        const max_us = @as(f64, @floatFromInt(self.max_ns)) / 1_000.0;
        std.debug.print("Range: {d:.2}µs - {d:.2}µs\n", .{ min_us, max_us });

        std.debug.print("Throughput: {d:.0} ops/sec\n", .{self.throughput_ops_per_sec});
        std.debug.print("Performance: {s}{s}\x1b[0m\n", .{ status_color, status });

        const peak_mb = @as(f64, @floatFromInt(self.peak_memory_bytes)) / (1024.0 * 1024.0);
        const growth_kb = @as(f64, @floatFromInt(self.memory_growth_bytes)) / 1024.0;
        std.debug.print("Peak memory: {d:.1}MB, Growth: {d:.1}KB\n", .{ peak_mb, growth_kb });
        std.debug.print("Memory efficiency: {s}{s}\x1b[0m\n", .{ memory_color, memory_status });
    }

    fn print_json(self: BenchmarkResult) void {
        std.debug.print("{{\n", .{});
        std.debug.print("\"operation\":\"{s}\",", .{self.operation_name});
        std.debug.print("\"iterations\":{},", .{self.iterations});
        std.debug.print("\"total_time_ns\":{},", .{self.total_time_ns});
        std.debug.print("\"mean_ns\":{},", .{self.mean_ns});
        std.debug.print("\"min_ns\":{},", .{self.min_ns});
        std.debug.print("\"max_ns\":{},", .{self.max_ns});
        std.debug.print("\"stddev_ns\":{},", .{self.stddev_ns});
        std.debug.print("\"throughput_ops_per_sec\":{d:.2},", .{self.throughput_ops_per_sec});
        std.debug.print("\"passed_threshold\":{},", .{self.passed_threshold});
        std.debug.print("\"threshold_ns\":{},", .{self.threshold_ns});
        std.debug.print("\"peak_memory_bytes\":{},", .{self.peak_memory_bytes});
        std.debug.print("\"memory_growth_bytes\":{},", .{self.memory_growth_bytes});
        std.debug.print("\"memory_efficient\":{}", .{self.memory_efficient});
        std.debug.print("}}\n", .{});
    }
};

pub fn run_all(allocator: std.mem.Allocator, json_output: bool) !void {
    try run_single_queries(allocator, json_output);
    try run_batch_queries(allocator, json_output);
}

/// Benchmark single block query performance with individual lookup timing
///
/// Tests the performance of finding individual blocks by ID in the query engine.
/// Used for understanding single query response characteristics.
pub fn run_single_queries(allocator: std.mem.Allocator, json_output: bool) !void {
    var sim_vfs = try simulation_vfs.SimulationVFS.init(allocator);
    defer sim_vfs.deinit();

    var storage_engine = try StorageEngine.init_default(allocator, sim_vfs.vfs(), "benchmark_single_queries");
    defer storage_engine.deinit();
    try storage_engine.startup();

    var query_eng = QueryEngine.init(allocator, &storage_engine);
    defer query_eng.deinit();

    try benchmark_single_block_queries(&query_eng, allocator, json_output);
}

/// Benchmark batch query performance with multi-block lookup tracking
///
/// Tests the performance of executing multiple queries together as a batch.
/// Used for understanding batch processing optimization benefits.
pub fn run_batch_queries(allocator: std.mem.Allocator, json_output: bool) !void {
    var sim_vfs = try simulation_vfs.SimulationVFS.init(allocator);
    defer sim_vfs.deinit();

    var storage_engine = try StorageEngine.init_default(allocator, sim_vfs.vfs(), "benchmark_batch_queries");
    defer storage_engine.deinit();
    try storage_engine.startup();

    var query_eng = QueryEngine.init(allocator, &storage_engine);
    defer query_eng.deinit();

    try benchmark_batch_queries_impl(&query_eng, allocator, json_output);
}

fn benchmark_single_block_queries(query_eng: *QueryEngine, allocator: std.mem.Allocator, json_output: bool) !void {
    try setup_query_test_data(query_eng.storage_engine);
    const test_block_ids = try create_query_test_block_ids(allocator);
    defer allocator.free(test_block_ids);

    const initial_memory = query_current_rss_memory();
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

    const peak_memory = query_current_rss_memory();
    const memory_growth = peak_memory - initial_memory;

    const stats = analyze_timings(timings);
    const throughput = @as(f64, @floatFromInt(ITERATIONS)) / (@as(f64, @floatFromInt(stats.total_time_ns)) / 1_000_000_000.0);

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

    if (json_output) {
        result.print_json();
    } else {
        result.print_results();
    }
}

fn benchmark_batch_queries_impl(query_eng: *QueryEngine, allocator: std.mem.Allocator, json_output: bool) !void {
    try setup_query_test_data(query_eng.storage_engine);
    const test_block_ids = try create_query_test_block_ids(allocator);
    defer allocator.free(test_block_ids);

    const initial_memory = query_current_rss_memory();
    var timings = try allocator.alloc(u64, ITERATIONS);
    defer allocator.free(timings);

    for (0..WARMUP_ITERATIONS) |i| {
        const start_idx = (i * BATCH_SIZE) % test_block_ids.len;
        const end_idx = @min(start_idx + BATCH_SIZE, test_block_ids.len);
        const batch_ids = test_block_ids[start_idx..end_idx];

        for (batch_ids) |block_id| {
            _ = try query_eng.find_block(block_id);
        }
    }

    for (0..ITERATIONS) |i| {
        const start_idx = (i * BATCH_SIZE) % test_block_ids.len;
        const end_idx = @min(start_idx + BATCH_SIZE, test_block_ids.len);
        const batch_ids = test_block_ids[start_idx..end_idx];

        const start_time = std.time.nanoTimestamp();
        for (batch_ids) |block_id| {
            _ = try query_eng.find_block(block_id);
        }
        const end_time = std.time.nanoTimestamp();

        timings[i] = @intCast(end_time - start_time);
    }

    const peak_memory = query_current_rss_memory();
    const memory_growth = peak_memory - initial_memory;

    const stats = analyze_timings(timings);
    const throughput = @as(f64, @floatFromInt(ITERATIONS)) / (@as(f64, @floatFromInt(stats.total_time_ns)) / 1_000_000_000.0);

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

    if (json_output) {
        result.print_json();
    } else {
        result.print_results();
    }
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

fn query_current_rss_memory() u64 {
    return switch (builtin.os.tag) {
        .linux => query_rss_linux() catch 0,
        .macos => query_rss_macos() catch 0,
        .windows => query_rss_windows() catch 0,
        else => 0,
    };
}

fn query_rss_linux() !u64 {
    const file = std.fs.cwd().openFile("/proc/self/status", .{}) catch return error.FileNotFound;
    defer file.close();

    var buf: [4096]u8 = undefined;
    const bytes_read = try file.readAll(&buf);
    const content = buf[0..bytes_read];

    if (std.mem.indexOf(u8, content, "VmRSS:")) |start| {
        const line_start = start;
        const line_end = std.mem.indexOfScalarPos(u8, content, line_start, '\n') orelse content.len;
        const line = content[line_start..line_end];

        if (std.mem.indexOf(u8, line, "\t")) |tab_pos| {
            if (std.mem.indexOf(u8, line, " kB")) |kb_pos| {
                const value_str = std.mem.trim(u8, line[tab_pos + 1 .. kb_pos], " \t");
                const kb_value = std.fmt.parseInt(u64, value_str, 10) catch return error.ParseError;
                return kb_value * 1024;
            }
        }
    }
    return error.ParseError;
}

fn query_rss_macos() !u64 {
    const c = @cImport({
        @cInclude("mach/mach.h");
        @cInclude("mach/task.h");
        @cInclude("mach/mach_init.h");
    });

    var info: c.mach_task_basic_info_data_t = undefined;
    var count: c.mach_msg_type_number_t = c.MACH_TASK_BASIC_INFO_COUNT;

    const kr = c.task_info(c.mach_task_self(), c.MACH_TASK_BASIC_INFO, @as(c.task_info_t, @ptrCast(&info)), &count);

    if (kr != c.KERN_SUCCESS) return error.TaskInfoFailed;
    return info.resident_size;
}

fn query_rss_windows() !u64 {
    const windows = std.os.windows;
    const PROCESS_MEMORY_COUNTERS = extern struct {
        cb: windows.DWORD,
        PageFaultCount: windows.DWORD,
        PeakWorkingSetSize: windows.SIZE_T,
        WorkingSetSize: windows.SIZE_T,
        QuotaPeakPagedPoolUsage: windows.SIZE_T,
        QuotaPagedPoolUsage: windows.SIZE_T,
        QuotaPeakNonPagedPoolUsage: windows.SIZE_T,
        QuotaNonPagedPoolUsage: windows.SIZE_T,
        PagefileUsage: windows.SIZE_T,
        PeakPagefileUsage: windows.SIZE_T,
    };

    const psapi = struct {
        extern "psapi" fn GetProcessMemoryInfo(
            hProcess: windows.HANDLE,
            ppsmemCounters: *PROCESS_MEMORY_COUNTERS,
            cb: windows.DWORD,
        ) callconv(windows.WINAPI) windows.BOOL;
    };

    var pmc: PROCESS_MEMORY_COUNTERS = undefined;
    pmc.cb = @sizeOf(PROCESS_MEMORY_COUNTERS);

    const success = psapi.GetProcessMemoryInfo(windows.kernel32.GetCurrentProcess(), &pmc, @sizeOf(PROCESS_MEMORY_COUNTERS));

    if (success == 0) return error.GetProcessMemoryInfoFailed;
    return pmc.WorkingSetSize;
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
