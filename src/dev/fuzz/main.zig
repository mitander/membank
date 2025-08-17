//! KausalDB fuzzing coordination and entry point.
//!
//! Orchestrates different fuzzing modules and provides unified command-line interface.
//! This module handles argument parsing, target selection, and crash reporting coordination.

const std = @import("std");
const builtin = @import("builtin");
const kausaldb = @import("kausaldb");

const stdx = kausaldb.stdx;

const storage_fuzz = @import("storage.zig");
const query_fuzz = @import("query.zig");
const parser_fuzz = @import("parser.zig");
const serialization_fuzz = @import("serialization.zig");
const network_fuzz = @import("network.zig");
const compaction_fuzz = @import("compaction.zig");
const common = @import("common.zig");

const FUZZ_ITERATIONS_DEFAULT = 100_000;
const FUZZ_ITERATIONS_CONTINUOUS = std.math.maxInt(u64);
const FUZZ_SEED_DEFAULT = 42;

var global_verbose_mode = stdx.ProtectedType(bool).init(false);
var global_validation_errors = stdx.MetricsCounter.init(0);
var global_shutdown_requested = stdx.ProtectedType(bool).init(false);

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{ .safety = true }){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    kausaldb.concurrency.init();

    const args = try std.process.argsAlloc(allocator);
    defer std.process.argsFree(allocator, args);

    if (args.len < 2) {
        try print_usage();
        return;
    }

    var target: []const u8 = undefined;
    var iterations: u64 = FUZZ_ITERATIONS_DEFAULT;
    var seed: u64 = FUZZ_SEED_DEFAULT;
    var arg_index: u32 = 1;

    if (arg_index < args.len and (std.mem.eql(u8, args[arg_index], "--verbose") or std.mem.eql(u8, args[arg_index], "-v"))) {
        _ = global_verbose_mode.with(
            fn (*bool, void) void,
            {},
            struct {
                fn f(verbose: *bool, ctx: void) void {
                    _ = ctx;
                    verbose.* = true;
                }
            }.f,
        );
        arg_index += 1;
    }

    if (arg_index >= args.len) {
        try print_usage();
        return;
    }
    target = args[arg_index];
    arg_index += 1;

    if (arg_index < args.len) {
        if (std.mem.eql(u8, args[arg_index], "continuous")) {
            iterations = FUZZ_ITERATIONS_CONTINUOUS;
        } else {
            iterations = try std.fmt.parseInt(u64, args[arg_index], 10);
        }
        arg_index += 1;
    }

    if (arg_index < args.len) {
        seed = try std.fmt.parseInt(u64, args[arg_index], 10);
    }

    try common.setup_crash_reporting(allocator);

    if (std.mem.eql(u8, target, "storage")) {
        try storage_fuzz.run(allocator, iterations, seed, &global_verbose_mode, &global_validation_errors);
    } else if (std.mem.eql(u8, target, "query")) {
        try query_fuzz.run(allocator, iterations, seed, &global_verbose_mode, &global_validation_errors);
    } else if (std.mem.eql(u8, target, "parser")) {
        try parser_fuzz.run(allocator, iterations, seed, &global_verbose_mode);
    } else if (std.mem.eql(u8, target, "serialization")) {
        try serialization_fuzz.run(allocator, iterations, seed, &global_verbose_mode);
    } else if (std.mem.eql(u8, target, "network")) {
        try network_fuzz.run(allocator, iterations, seed, &global_verbose_mode, &global_validation_errors);
    } else if (std.mem.eql(u8, target, "compaction")) {
        try compaction_fuzz.run(allocator, iterations, seed, &global_verbose_mode, &global_validation_errors);
    } else if (std.mem.eql(u8, target, "all")) {
        try run_all_targets(allocator, iterations, seed);
    } else {
        std.debug.print("Unknown fuzz target: {s}\n", .{target});
        try print_usage();
        std.process.exit(1);
    }
}

fn print_usage() !void {
    std.debug.print(
        \\KausalDB Production Fuzzer
        \\
        \\Usage:
        \\  fuzz [--verbose|-v] <target> [iterations|continuous] [seed]
        \\
        \\Flags:
        \\  --verbose, -v   Show detailed progress with timing, rates, validation errors
        \\                  (default: summary mode shows only iteration counts and crashes)
        \\
        \\Targets:
        \\  storage         Fuzz storage engine (WAL, SSTables, BlockIndex)
        \\  query           Fuzz query engine processing
        \\  parser          Fuzz Zig source code parser
        \\  serialization   Fuzz block serialization/deserialization
        \\  network         Fuzz network protocol and connection handling
        \\  compaction      Fuzz SSTable compaction under stress conditions
        \\  all             Fuzz all targets
        \\
        \\Iterations:
        \\  <number>        Run specified number of iterations (default: 100,000)
        \\  continuous      Run until manually stopped (24/7 fuzzing)
        \\
        \\Examples:
        \\  fuzz storage 1000000 42          # 1M iterations with seed 42 (summary mode)
        \\  fuzz --verbose storage 50000     # Detailed timing and error metrics
        \\  fuzz -v query continuous         # Verbose continuous with full stats
        \\  fuzz query continuous            # Clean output, crashes only
        \\  fuzz all continuous 12345        # Continuous fuzzing of all targets
        \\
        \\Output Modes:
        \\  Summary (default): Clean progress reports showing iteration counts and crashes
        \\  Verbose (--verbose): Detailed timing, rates, validation errors, and full stats
        \\
        \\Crash reports are saved to: {s}/
        \\
    , .{common.CRASH_REPORT_DIR});
}

fn run_all_targets(allocator: std.mem.Allocator, iterations: u64, seed: u64) !void {
    if (iterations == FUZZ_ITERATIONS_CONTINUOUS) {
        std.debug.print("Fuzzing all targets continuously, starting seed {}\n", .{seed});

        var current_seed = seed;
        while (true) {
            const batch_size = 10_000;
            try storage_fuzz.run(allocator, batch_size, current_seed, &global_verbose_mode, &global_validation_errors);
            try query_fuzz.run(allocator, batch_size, current_seed + 1, &global_verbose_mode, &global_validation_errors);
            try parser_fuzz.run(allocator, batch_size, current_seed + 2, &global_verbose_mode);
            try serialization_fuzz.run(allocator, batch_size, current_seed + 3, &global_verbose_mode);
            try network_fuzz.run(allocator, batch_size, current_seed + 4, &global_verbose_mode, &global_validation_errors);
            try compaction_fuzz.run(allocator, batch_size, current_seed + 5, &global_verbose_mode, &global_validation_errors);
            current_seed += 6;

            std.Thread.sleep(100_000_000); // 100ms // tidy:ignore-arch - controlled coordination for system stability
        }
    } else {
        std.debug.print("Fuzzing all targets with {} iterations, seed {}\n", .{ iterations, seed });
        const per_target = iterations / 6;
        try storage_fuzz.run(allocator, per_target, seed, &global_verbose_mode, &global_validation_errors);
        try query_fuzz.run(allocator, per_target, seed + 1, &global_verbose_mode, &global_validation_errors);
        try parser_fuzz.run(allocator, per_target, seed + 2, &global_verbose_mode);
        try serialization_fuzz.run(allocator, per_target, seed + 3, &global_verbose_mode);
        try network_fuzz.run(allocator, per_target, seed + 4, &global_verbose_mode, &global_validation_errors);
        try compaction_fuzz.run(allocator, per_target, seed + 5, &global_verbose_mode, &global_validation_errors);
    }
}

/// Check if a shutdown has been requested via signal handler
///
/// Thread-safe check of the global shutdown flag set by signal handlers.
/// Essential for graceful fuzzing termination without data corruption.
pub fn check_shutdown_request() bool {
    if (global_shutdown_requested.with(
        fn (*const bool, void) bool,
        {},
        struct {
            fn f(shutdown: *const bool, ctx: void) bool {
                _ = ctx;
                return shutdown.*;
            }
        }.f,
    )) {
        return true;
    }

    std.fs.cwd().access(".kausaldb_stop", .{}) catch {
        return false; // File doesn't exist, continue
    };

    _ = global_shutdown_requested.with(
        fn (*bool, bool) void,
        true,
        struct {
            fn f(shutdown: *bool, value: bool) void {
                shutdown.* = value;
            }
        }.f,
    );
    std.debug.print("Shutdown requested via .kausaldb_stop file\n", .{});

    std.fs.cwd().deleteFile(".kausaldb_stop") catch {};
    return true;
}
