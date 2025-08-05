//! Compaction performance benchmarks.
//!
//! Tests SSTable compaction operations, merge performance, and memory efficiency
//! during compaction cycles. Focuses on background operation impact on system performance.

const std = @import("std");
const builtin = @import("builtin");
const kausaldb = @import("kausaldb");
const assert = kausaldb.assert;

const storage = kausaldb.storage;
const context_block = kausaldb.types;
const simulation_vfs = kausaldb.simulation_vfs;

const StorageEngine = storage.StorageEngine;
const ContextBlock = context_block.ContextBlock;
const BlockId = context_block.BlockId;

const COMPACTION_THRESHOLD_NS = 50_000_000; // 50ms for compaction operations
const MERGE_THRESHOLD_NS = 10_000_000; // 10ms for merge operations

const MAX_PEAK_MEMORY_BYTES = 200 * 1024 * 1024; // 200MB during compaction
const MAX_MEMORY_GROWTH_PER_OP = 10 * 1024; // 10KB per compaction

const COMPACTION_ITERATIONS = 10;
const WARMUP_ITERATIONS = 2;

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

        const mean_ms = @as(f64, @floatFromInt(self.mean_ns)) / 1_000_000.0;
        const threshold_ms = @as(f64, @floatFromInt(self.threshold_ns)) / 1_000_000.0;
        std.debug.print("Mean time: {d:.2}ms (threshold: {d:.0}ms)\n", .{ mean_ms, threshold_ms });

        const min_ms = @as(f64, @floatFromInt(self.min_ns)) / 1_000_000.0;
        const max_ms = @as(f64, @floatFromInt(self.max_ns)) / 1_000_000.0;
        std.debug.print("Range: {d:.2}ms - {d:.2}ms\n", .{ min_ms, max_ms });

        std.debug.print("Throughput: {d:.2} ops/sec\n", .{self.throughput_ops_per_sec});
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
    try run_compaction_benchmark(allocator, json_output);
}

/// Benchmark SSTable compaction performance
///
/// Tests how long it takes to merge and clean up SSTables.
/// Helps understand how compaction affects overall performance.
pub fn run_compaction_benchmark(allocator: std.mem.Allocator, json_output: bool) !void {
    var sim_vfs = try simulation_vfs.SimulationVFS.init(allocator);
    defer sim_vfs.deinit();

    var storage_engine = try StorageEngine.init_default(allocator, sim_vfs.vfs(), "benchmark_compaction");
    defer storage_engine.deinit();

    storage_engine.startup() catch |err| {
        if (!json_output) {
            std.debug.print("Compaction benchmark startup error: {}\n", .{err});
        }
        return err;
    };

    try benchmark_compaction_operations(&storage_engine, allocator, json_output);
}

fn benchmark_compaction_operations(
    storage_engine: *StorageEngine,
    allocator: std.mem.Allocator,
    json_output: bool,
) !void {
    try setup_compaction_test_data(storage_engine, allocator);

    const initial_memory = query_current_rss_memory();
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

    const peak_memory = query_current_rss_memory();
    const memory_growth = peak_memory - initial_memory;

    const stats = analyze_timings(timings);
    const throughput = @as(f64, @floatFromInt(COMPACTION_ITERATIONS)) / (@as(f64, @floatFromInt(stats.total_time_ns)) / 1_000_000_000.0);

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
        .peak_memory_bytes = peak_memory,
        .memory_growth_bytes = memory_growth,
        .memory_efficient = peak_memory <= MAX_PEAK_MEMORY_BYTES and memory_growth <= (MAX_MEMORY_GROWTH_PER_OP * COMPACTION_ITERATIONS),
    };

    if (json_output) {
        result.print_json();
    } else {
        result.print_results();
    }
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
    const block_id_hex = try std.fmt.allocPrint(allocator, "{x:0>32}", .{index});
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
