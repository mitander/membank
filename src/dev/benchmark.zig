//! Benchmark coordinator for KausalDB performance testing.
//!
//! Dispatches to specialized benchmark modules based on command arguments.
//! Each module returns structured results which are formatted by this coordinator.
//! Follows separation of concerns - benchmarks produce data, main handles presentation.

const std = @import("std");

const compaction_benchmarks = @import("benchmark/compaction.zig");
const query_benchmarks = @import("benchmark/query.zig");
const storage_benchmarks = @import("benchmark/storage.zig");

pub const BenchmarkResult = struct {
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

    /// Formats benchmark results for console output with color-coded pass/fail status.
    ///
    /// Displays performance metrics, memory usage, and threshold compliance
    /// with ANSI color codes for immediate visual feedback on CI/development.
    pub fn print_results(self: BenchmarkResult) void {
        const status = if (self.passed_threshold) "PASS" else "FAIL";
        const status_color = if (self.passed_threshold) "\x1b[32m" else "\x1b[31m";
        const memory_status = if (self.memory_efficient) "PASS" else "FAIL";
        const memory_color = if (self.memory_efficient) "\x1b[32m" else "\x1b[31m";

        std.debug.print("\n=== {s} Benchmark ===\n", .{self.operation_name});
        std.debug.print("Iterations: {}\n", .{self.iterations});

        // Display total time with appropriate precision
        if (self.total_time_ns >= 1_000_000) {
            const total_time_ms = @as(f64, @floatFromInt(self.total_time_ns)) / 1_000_000.0;
            std.debug.print("Total time: {d:.2}ms\n", .{total_time_ms});
        } else if (self.total_time_ns >= 1_000) {
            const total_time_us = @as(f64, @floatFromInt(self.total_time_ns)) / 1_000.0;
            std.debug.print("Total time: {d:.2}µs\n", .{total_time_us});
        } else {
            std.debug.print("Total time: {}ns\n", .{self.total_time_ns});
        }

        // Display mean time with appropriate precision
        if (self.mean_ns >= 1_000) {
            const mean_us = @as(f64, @floatFromInt(self.mean_ns)) / 1_000.0;
            const threshold_us = @as(f64, @floatFromInt(self.threshold_ns)) / 1_000.0;
            std.debug.print("Mean time: {d:.2}µs (threshold: {d:.0}µs)\n", .{ mean_us, threshold_us });
        } else {
            const threshold_us = @as(f64, @floatFromInt(self.threshold_ns)) / 1_000.0;
            std.debug.print("Mean time: {}ns (threshold: {d:.0}µs)\n", .{ self.mean_ns, threshold_us });
        }

        // Display range with appropriate precision
        if (self.min_ns >= 1_000 and self.max_ns >= 1_000) {
            const min_us = @as(f64, @floatFromInt(self.min_ns)) / 1_000.0;
            const max_us = @as(f64, @floatFromInt(self.max_ns)) / 1_000.0;
            std.debug.print("Range: {d:.2}µs - {d:.2}µs\n", .{ min_us, max_us });
        } else {
            std.debug.print("Range: {}ns - {}ns\n", .{ self.min_ns, self.max_ns });
        }

        // Handle infinite throughput display gracefully
        if (std.math.isInf(self.throughput_ops_per_sec)) {
            std.debug.print("Throughput: >1B ops/sec (sub-nanosecond)\n", .{});
        } else {
            std.debug.print("Throughput: {d:.0} ops/sec\n", .{self.throughput_ops_per_sec});
        }
        std.debug.print("Performance: {s}{s}\x1b[0m\n", .{ status_color, status });

        const peak_mb = @as(f64, @floatFromInt(self.peak_memory_bytes)) / (1024.0 * 1024.0);
        const growth_kb = @as(f64, @floatFromInt(self.memory_growth_bytes)) / 1024.0;
        std.debug.print("Peak memory: {d:.1}MB, Growth: {d:.1}KB\n", .{ peak_mb, growth_kb });
        std.debug.print("Memory efficiency: {s}{s}\x1b[0m\n", .{ memory_color, memory_status });
    }

    /// Formats single benchmark result as JSON object with proper indentation.
    ///
    /// Used by format_json_output to create individual array elements in the final
    /// JSON output. Includes all performance metrics for CI/monitoring systems.
    pub fn format_json(self: BenchmarkResult) void {
        std.debug.print("  {{\n", .{});
        std.debug.print("    \"operation\": \"{s}\",\n", .{self.operation_name});
        std.debug.print("    \"iterations\": {},\n", .{self.iterations});
        std.debug.print("    \"total_time_ns\": {},\n", .{self.total_time_ns});
        std.debug.print("    \"mean_ns\": {},\n", .{self.mean_ns});
        std.debug.print("    \"min_ns\": {},\n", .{self.min_ns});
        std.debug.print("    \"max_ns\": {},\n", .{self.max_ns});
        std.debug.print("    \"median_ns\": {},\n", .{self.median_ns});
        std.debug.print("    \"stddev_ns\": {},\n", .{self.stddev_ns});
        std.debug.print("    \"throughput_ops_per_sec\": {d:.2},\n", .{self.throughput_ops_per_sec});
        std.debug.print("    \"passed_threshold\": {},\n", .{self.passed_threshold});
        std.debug.print("    \"threshold_ns\": {},\n", .{self.threshold_ns});
        std.debug.print("    \"peak_memory_bytes\": {},\n", .{self.peak_memory_bytes});
        std.debug.print("    \"memory_growth_bytes\": {},\n", .{self.memory_growth_bytes});
        std.debug.print("    \"memory_efficient\": {}\n", .{self.memory_efficient});
        std.debug.print("  }}", .{});
    }
};

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    const args = try std.process.argsAlloc(allocator);
    defer std.process.argsFree(allocator, args);

    if (args.len < 2) {
        print_usage();
        return;
    }

    var benchmark_name: []const u8 = "";
    var json_output = false;
    var i: usize = 1;
    while (i < args.len) : (i += 1) {
        const arg = args[i];
        if (std.mem.eql(u8, arg, "--json")) {
            json_output = true;
        } else if (benchmark_name.len == 0) {
            benchmark_name = arg;
        } else {
            std.debug.print("Error: Unknown argument '{s}'\n", .{arg});
            print_usage();
            return;
        }
    }

    var results = std.ArrayList(BenchmarkResult).init(allocator);
    defer results.deinit();

    if (std.mem.eql(u8, benchmark_name, "all")) {
        try run_all_benchmarks(allocator, &results);
    } else if (std.mem.eql(u8, benchmark_name, "storage")) {
        var storage_results = try storage_benchmarks.run_all(allocator);
        defer storage_results.deinit();
        try results.appendSlice(storage_results.items);
    } else if (std.mem.eql(u8, benchmark_name, "query")) {
        var query_results = try query_benchmarks.run_all(allocator);
        defer query_results.deinit();
        try results.appendSlice(query_results.items);
    } else if (std.mem.eql(u8, benchmark_name, "compaction")) {
        var compaction_results = try compaction_benchmarks.run_all(allocator);
        defer compaction_results.deinit();
        try results.appendSlice(compaction_results.items);
    } else if (std.mem.eql(u8, benchmark_name, "block-write")) {
        const result = try storage_benchmarks.run_block_writes(allocator);
        try results.append(result);
    } else if (std.mem.eql(u8, benchmark_name, "block-read")) {
        const result = try storage_benchmarks.run_block_reads(allocator);
        try results.append(result);
    } else if (std.mem.eql(u8, benchmark_name, "block-update")) {
        const result = try storage_benchmarks.run_block_updates(allocator);
        try results.append(result);
    } else if (std.mem.eql(u8, benchmark_name, "block-delete")) {
        const result = try storage_benchmarks.run_block_deletes(allocator);
        try results.append(result);
    } else if (std.mem.eql(u8, benchmark_name, "single-query")) {
        const result = try query_benchmarks.run_single_queries(allocator);
        try results.append(result);
    } else if (std.mem.eql(u8, benchmark_name, "batch-query")) {
        const result = try query_benchmarks.run_batch_queries(allocator);
        try results.append(result);
    } else if (std.mem.eql(u8, benchmark_name, "wal-flush")) {
        const result = try storage_benchmarks.run_wal_flush(allocator);
        try results.append(result);
    } else {
        std.debug.print("Error: Unknown benchmark '{s}'\n", .{benchmark_name});
        print_usage();
        return;
    }

    if (json_output) {
        format_json_output(results.items);
    } else {
        format_console_output(results.items);
    }
}

fn run_all_benchmarks(allocator: std.mem.Allocator, results: *std.ArrayList(BenchmarkResult)) !void {
    var storage_results = try storage_benchmarks.run_all(allocator);
    defer storage_results.deinit();
    try results.appendSlice(storage_results.items);

    var query_results = try query_benchmarks.run_all(allocator);
    defer query_results.deinit();
    try results.appendSlice(query_results.items);

    var compaction_results = try compaction_benchmarks.run_all(allocator);
    defer compaction_results.deinit();
    try results.appendSlice(compaction_results.items);
}

fn format_console_output(results: []const BenchmarkResult) void {
    std.debug.print("Running KausalDB benchmarks...\n", .{});

    for (results) |result| {
        result.print_results();
    }
}

fn format_json_output(results: []const BenchmarkResult) void {
    std.debug.print("[\n", .{});
    for (results, 0..) |result, i| {
        result.format_json();
        if (i < results.len - 1) {
            std.debug.print(",\n", .{});
        } else {
            std.debug.print("\n", .{});
        }
    }
    std.debug.print("]\n", .{});
}

fn print_usage() void {
    const usage =
        \\KausalDB Performance Benchmark Tool
        \\
        \\Usage: benchmark [--json] <benchmark_type>
        \\
        \\Benchmark Types:
        \\  all            Run all benchmarks
        \\  storage        All storage engine benchmarks
        \\  query          All query engine benchmarks
        \\  compaction     All compaction benchmarks
        \\  block-write    Single block write operations
        \\  block-read     Single block read operations
        \\  block-update   Single block update operations
        \\  block-delete   Single block delete operations
        \\  single-query   Individual query operations
        \\  batch-query    Batch query operations
        \\  wal-flush      Write-ahead log flush operations
        \\
        \\Options:
        \\  --json         Output results in JSON format
        \\
        \\Examples:
        \\  benchmark all                  Run benchmark suite
        \\  benchmark storage --json       Run storage benchmarks with JSON output
        \\  benchmark block-write          Benchmark block write performance
        \\
    ;
    std.debug.print("{s}", .{usage});
}
