//! Benchmark suite for CortexDB performance testing.

const std = @import("std");
const assert = std.debug.assert;

const BENCHMARK_ITERATIONS = 1000;
const WARMUP_ITERATIONS = 100;

pub fn main() !void {
    var general_purpose_allocator = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = general_purpose_allocator.deinit();
    const allocator = general_purpose_allocator.allocator();

    const args = try std.process.argsAlloc(allocator);
    defer std.process.argsFree(allocator, args);

    if (args.len < 2) {
        try print_usage();
        return;
    }

    const benchmark_name = args[1];

    if (std.mem.eql(u8, benchmark_name, "block_validation")) {
        try run_block_validation_benchmark(allocator);
    } else if (std.mem.eql(u8, benchmark_name, "query_processing")) {
        try run_query_processing_benchmark(allocator);
    } else if (std.mem.eql(u8, benchmark_name, "all")) {
        try run_all_benchmarks(allocator);
    } else {
        std.debug.print("Unknown benchmark: {s}\n", .{benchmark_name});
        try print_usage();
        std.process.exit(1);
    }
}

fn print_usage() !void {
    std.debug.print(
        \\CortexDB Benchmark Suite
        \\
        \\Usage:
        \\  benchmark <name>
        \\
        \\Benchmarks:
        \\  block_validation    Block validation performance
        \\  query_processing    Query processing performance
        \\  all                 Run all benchmarks
        \\
        \\Examples:
        \\  benchmark block_validation
        \\  benchmark all
        \\
    , .{});
}

fn run_all_benchmarks(allocator: std.mem.Allocator) !void {
    std.debug.print("Running all benchmarks...\n", .{});
    try run_block_validation_benchmark(allocator);
    try run_query_processing_benchmark(allocator);
}

fn run_block_validation_benchmark(allocator: std.mem.Allocator) !void {
    _ = allocator;
    std.debug.print("Block validation benchmark:\n", .{});

    // Warmup
    for (0..WARMUP_ITERATIONS) |_| {
        benchmark_validate_block();
    }

    const start_time = std.time.nanoTimestamp();

    for (0..BENCHMARK_ITERATIONS) |_| {
        benchmark_validate_block();
    }

    const end_time = std.time.nanoTimestamp();
    const elapsed_ns = @as(u64, @intCast(end_time - start_time));
    const ns_per_op = elapsed_ns / BENCHMARK_ITERATIONS;

    std.debug.print("  {} iterations in {} ns\n", .{ BENCHMARK_ITERATIONS, elapsed_ns });
    std.debug.print("  {} ns per operation\n", .{ns_per_op});
    std.debug.print("  {d:.2} ops/sec\n", .{1_000_000_000.0 / @as(f64, @floatFromInt(ns_per_op))});
}

fn run_query_processing_benchmark(allocator: std.mem.Allocator) !void {
    _ = allocator;
    std.debug.print("Query processing benchmark:\n", .{});

    // Warmup
    for (0..WARMUP_ITERATIONS) |_| {
        benchmark_process_query();
    }

    const start_time = std.time.nanoTimestamp();

    for (0..BENCHMARK_ITERATIONS) |_| {
        benchmark_process_query();
    }

    const end_time = std.time.nanoTimestamp();
    const elapsed_ns = @as(u64, @intCast(end_time - start_time));
    const ns_per_op = elapsed_ns / BENCHMARK_ITERATIONS;

    std.debug.print("  {} iterations in {} ns\n", .{ BENCHMARK_ITERATIONS, elapsed_ns });
    std.debug.print("  {} ns per operation\n", .{ns_per_op});
    std.debug.print("  {d:.2} ops/sec\n", .{1_000_000_000.0 / @as(f64, @floatFromInt(ns_per_op))});
}

/// Placeholder block validation benchmark.
fn benchmark_validate_block() void {
    // Simulate block validation work
    var hash: u64 = 0x123456789ABCDEF0;
    for (0..100) |i| {
        hash ^= @as(u64, @intCast(i));
        hash = hash *% 0x9E3779B97F4A7C15;
    }
    std.mem.doNotOptimizeAway(hash);
}

/// Placeholder query processing benchmark.
fn benchmark_process_query() void {
    // Simulate query processing work
    var result: u32 = 0;
    for (0..50) |i| {
        result +%= @as(u32, @intCast(i * i));
    }
    std.mem.doNotOptimizeAway(result);
}

test "benchmark module tests" {
    // Test benchmark functions don't crash
    benchmark_validate_block();
    benchmark_process_query();
}
