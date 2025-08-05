//! Storage engine performance benchmarks.
//!
//! Tests block operations, WAL performance, and memory efficiency.
//! Thresholds calibrated based on measured performance with safety margins
//! for reliable CI regression detection.

const std = @import("std");
const builtin = @import("builtin");
const kausaldb = @import("kausaldb");

const storage = kausaldb.storage;
const context_block = kausaldb.types;
const simulation_vfs = kausaldb.simulation_vfs;

const StorageEngine = storage.StorageEngine;
const ContextBlock = context_block.ContextBlock;
const BlockId = context_block.BlockId;

const BLOCK_WRITE_THRESHOLD_NS = 100_000; // measured 21µs → 100µs (4.7x margin)
const BLOCK_READ_THRESHOLD_NS = 1_000; // measured 0.06µs → 1µs (17x margin)
const BLOCK_UPDATE_THRESHOLD_NS = 50_000; // measured 10.2µs → 50µs (4.9x margin)
const BLOCK_DELETE_THRESHOLD_NS = 15_000; // measured 2.9µs → 15µs (5.2x margin)
const WAL_FLUSH_THRESHOLD_NS = 10_000; // conservative for no-op operation

const MAX_PEAK_MEMORY_BYTES = 100 * 1024 * 1024; // 100MB for 10K operations
const MAX_MEMORY_GROWTH_PER_OP = 2048; // 2KB average per operation (measured 1.6KB)

const ITERATIONS = 1000;
const WARMUP_ITERATIONS = 50;
const LARGE_ITERATIONS = 50;
const STATISTICAL_SAMPLES = 5;

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

/// Run all storage benchmark tests with performance measurement
///
/// Runs benchmarks for writes, reads, updates, deletes, and WAL flush operations.
/// Tests all main storage engine operations.
pub fn run_all(allocator: std.mem.Allocator, json_output: bool) !void {
    try run_block_writes(allocator, json_output);
    try run_block_reads(allocator, json_output);
    try run_block_updates(allocator, json_output);
    try run_block_deletes(allocator, json_output);
    try run_wal_flush(allocator, json_output);
}

/// Benchmark block write operations with performance and memory tracking
///
/// Creates test blocks and measures time to write them to storage engine.
/// Used for understanding ingestion pipeline performance.
pub fn run_block_writes(allocator: std.mem.Allocator, json_output: bool) !void {
    var sim_vfs = try simulation_vfs.SimulationVFS.init(allocator);
    defer sim_vfs.deinit();

    var storage_engine = try StorageEngine.init_default(allocator, sim_vfs.vfs(), "benchmark_writes");
    defer storage_engine.deinit();
    try storage_engine.startup();

    try benchmark_block_writes(&storage_engine, allocator, json_output);
}

/// Benchmark block read operations with lookup performance measurement
///
/// Pre-populates storage with test blocks then measures retrieval time.
/// Used for understanding query response characteristics.
pub fn run_block_reads(allocator: std.mem.Allocator, json_output: bool) !void {
    var sim_vfs = try simulation_vfs.SimulationVFS.init(allocator);
    defer sim_vfs.deinit();

    var storage_engine = try StorageEngine.init_default(allocator, sim_vfs.vfs(), "benchmark_reads");
    defer storage_engine.deinit();
    try storage_engine.startup();

    try benchmark_block_reads(&storage_engine, allocator, json_output);
}

/// Benchmark block update operations with modification performance tracking
///
/// Updates existing blocks with new versions and measures performance.
/// Used for understanding version management overhead.
pub fn run_block_updates(allocator: std.mem.Allocator, json_output: bool) !void {
    var sim_vfs = try simulation_vfs.SimulationVFS.init(allocator);
    defer sim_vfs.deinit();

    var storage_engine = try StorageEngine.init_default(allocator, sim_vfs.vfs(), "benchmark_updates");
    defer storage_engine.deinit();
    try storage_engine.startup();

    try benchmark_block_updates(&storage_engine, allocator, json_output);
}

/// Benchmark block delete operations with removal performance tracking
///
/// Creates blocks to delete and measures time to remove them from storage.
/// Includes tombstone handling and compaction effects.
pub fn run_block_deletes(allocator: std.mem.Allocator, json_output: bool) !void {
    var sim_vfs = try simulation_vfs.SimulationVFS.init(allocator);
    defer sim_vfs.deinit();

    var storage_engine = try StorageEngine.init_default(allocator, sim_vfs.vfs(), "benchmark_deletes");
    defer storage_engine.deinit();
    try storage_engine.startup();

    try benchmark_block_deletes(&storage_engine, allocator, json_output);
}

/// Benchmark WAL flush operations with durability performance tracking
///
/// Measures time to flush Write-Ahead Log to persistent storage.
/// Used for understanding commit latency characteristics.
pub fn run_wal_flush(allocator: std.mem.Allocator, json_output: bool) !void {
    var sim_vfs = try simulation_vfs.SimulationVFS.init(allocator);
    defer sim_vfs.deinit();

    var storage_engine = try StorageEngine.init_default(allocator, sim_vfs.vfs(), "benchmark_wal");
    defer storage_engine.deinit();
    try storage_engine.startup();

    try benchmark_wal_flush(&storage_engine, allocator, json_output);
}

fn benchmark_block_writes(storage_engine: *StorageEngine, allocator: std.mem.Allocator, json_output: bool) !void {
    const initial_memory = query_current_rss_memory();
    var timings = try allocator.alloc(u64, ITERATIONS);
    defer allocator.free(timings);

    for (0..WARMUP_ITERATIONS) |i| {
        const block = try create_test_block(allocator, i);
        defer free_test_block(allocator, block);
        _ = try storage_engine.put_block(block);
    }

    for (0..ITERATIONS) |i| {
        const block = try create_test_block(allocator, WARMUP_ITERATIONS + i);
        defer free_test_block(allocator, block);

        const start_time = std.time.nanoTimestamp();
        _ = try storage_engine.put_block(block);
        const end_time = std.time.nanoTimestamp();

        timings[i] = @intCast(end_time - start_time);
    }

    const peak_memory = query_current_rss_memory();
    const memory_growth = peak_memory - initial_memory;

    const stats = analyze_timings(timings);
    const throughput = @as(f64, @floatFromInt(ITERATIONS)) / (@as(f64, @floatFromInt(stats.total_time_ns)) / 1_000_000_000.0);

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

    if (json_output) {
        result.print_json();
    } else {
        result.print_results();
    }
}

fn benchmark_block_reads(storage_engine: *StorageEngine, allocator: std.mem.Allocator, json_output: bool) !void {
    const block_ids = try setup_read_test_blocks(storage_engine, allocator);
    defer allocator.free(block_ids);

    const initial_memory = query_current_rss_memory();
    var timings = try allocator.alloc(u64, ITERATIONS);
    defer allocator.free(timings);

    for (0..WARMUP_ITERATIONS) |i| {
        const block_id = block_ids[i % block_ids.len];
        _ = try storage_engine.find_block(block_id);
    }

    for (0..ITERATIONS) |i| {
        const block_id = block_ids[i % block_ids.len];

        const start_time = std.time.nanoTimestamp();
        _ = try storage_engine.find_block(block_id);
        const end_time = std.time.nanoTimestamp();

        timings[i] = @intCast(end_time - start_time);
    }

    const peak_memory = query_current_rss_memory();
    const memory_growth = peak_memory - initial_memory;

    const stats = analyze_timings(timings);
    const throughput = @as(f64, @floatFromInt(ITERATIONS)) / (@as(f64, @floatFromInt(stats.total_time_ns)) / 1_000_000_000.0);

    const result = BenchmarkResult{
        .operation_name = "Block Read",
        .iterations = ITERATIONS,
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
        .memory_efficient = peak_memory <= MAX_PEAK_MEMORY_BYTES and memory_growth <= (MAX_MEMORY_GROWTH_PER_OP * ITERATIONS),
    };

    if (json_output) {
        result.print_json();
    } else {
        result.print_results();
    }
}

fn benchmark_block_updates(storage_engine: *StorageEngine, allocator: std.mem.Allocator, json_output: bool) !void {
    const block_ids = try setup_read_test_blocks(storage_engine, allocator);
    defer allocator.free(block_ids);

    const initial_memory = query_current_rss_memory();
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

    const peak_memory = query_current_rss_memory();
    const memory_growth = peak_memory - initial_memory;

    const stats = analyze_timings(timings);
    const throughput = @as(f64, @floatFromInt(ITERATIONS)) / (@as(f64, @floatFromInt(stats.total_time_ns)) / 1_000_000_000.0);

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

    if (json_output) {
        result.print_json();
    } else {
        result.print_results();
    }
}

fn benchmark_block_deletes(storage_engine: *StorageEngine, allocator: std.mem.Allocator, json_output: bool) !void {
    const block_ids = try setup_delete_test_blocks(storage_engine, allocator);
    defer allocator.free(block_ids);

    const initial_memory = query_current_rss_memory();
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

    const peak_memory = query_current_rss_memory();
    const memory_growth = peak_memory - initial_memory;

    const stats = analyze_timings(timings);
    const throughput = @as(f64, @floatFromInt(ITERATIONS)) / (@as(f64, @floatFromInt(stats.total_time_ns)) / 1_000_000_000.0);

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

    if (json_output) {
        result.print_json();
    } else {
        result.print_results();
    }
}

fn benchmark_wal_flush(storage_engine: *StorageEngine, allocator: std.mem.Allocator, json_output: bool) !void {
    const initial_memory = query_current_rss_memory();
    var timings = try allocator.alloc(u64, ITERATIONS);
    defer allocator.free(timings);

    for (0..ITERATIONS) |i| {
        const start_time = std.time.nanoTimestamp();
        try storage_engine.flush_wal();
        const end_time = std.time.nanoTimestamp();

        timings[i] = @intCast(end_time - start_time);
    }

    const peak_memory = query_current_rss_memory();
    const memory_growth = peak_memory - initial_memory;

    const stats = analyze_timings(timings);
    const throughput = @as(f64, @floatFromInt(ITERATIONS)) / (@as(f64, @floatFromInt(stats.total_time_ns)) / 1_000_000_000.0);

    const result = BenchmarkResult{
        .operation_name = "WAL Flush",
        .iterations = ITERATIONS,
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
        .memory_efficient = peak_memory <= MAX_PEAK_MEMORY_BYTES and memory_growth <= (MAX_MEMORY_GROWTH_PER_OP * ITERATIONS),
    };

    if (json_output) {
        result.print_json();
    } else {
        result.print_results();
    }
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
