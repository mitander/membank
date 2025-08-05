//! Benchmark coordinator for KausalDB performance testing.
//!
//! Dispatches to specialized benchmark modules based on command arguments.
//! Each module owns its performance thresholds and statistical analysis
//! to maintain clear separation of concerns.

const std = @import("std");
const storage_benchmarks = @import("storage.zig");
const query_benchmarks = @import("query.zig");
const compaction_benchmarks = @import("compaction.zig");

/// Global output format selection
var json_output = false;

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

    // Parse command line arguments
    var benchmark_name: []const u8 = "";
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

    // Dispatch to appropriate benchmark module
    if (std.mem.eql(u8, benchmark_name, "all")) {
        try run_all_benchmarks(allocator);
    } else if (std.mem.eql(u8, benchmark_name, "storage")) {
        try storage_benchmarks.run_all(allocator, json_output);
    } else if (std.mem.eql(u8, benchmark_name, "query")) {
        try query_benchmarks.run_all(allocator, json_output);
    } else if (std.mem.eql(u8, benchmark_name, "compaction")) {
        try compaction_benchmarks.run_all(allocator, json_output);
    } else if (std.mem.eql(u8, benchmark_name, "block-write")) {
        try storage_benchmarks.run_block_writes(allocator, json_output);
    } else if (std.mem.eql(u8, benchmark_name, "block-read")) {
        try storage_benchmarks.run_block_reads(allocator, json_output);
    } else if (std.mem.eql(u8, benchmark_name, "block-update")) {
        try storage_benchmarks.run_block_updates(allocator, json_output);
    } else if (std.mem.eql(u8, benchmark_name, "block-delete")) {
        try storage_benchmarks.run_block_deletes(allocator, json_output);
    } else if (std.mem.eql(u8, benchmark_name, "single-query")) {
        try query_benchmarks.run_single_queries(allocator, json_output);
    } else if (std.mem.eql(u8, benchmark_name, "batch-query")) {
        try query_benchmarks.run_batch_queries(allocator, json_output);
    } else if (std.mem.eql(u8, benchmark_name, "wal-flush")) {
        try storage_benchmarks.run_wal_flush(allocator, json_output);
    } else {
        std.debug.print("Error: Unknown benchmark '{s}'\n", .{benchmark_name});
        print_usage();
        return;
    }
}

fn run_all_benchmarks(allocator: std.mem.Allocator) !void {
    if (!json_output) {
        std.debug.print("Running all KausalDB benchmarks...\n", .{});
    }

    try storage_benchmarks.run_all(allocator, json_output);
    try query_benchmarks.run_all(allocator, json_output);
    try compaction_benchmarks.run_all(allocator, json_output);
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
