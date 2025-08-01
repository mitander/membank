//! Zig parser fuzzing module.
//!
//! Tests the Zig source code parser with malformed input, edge cases,
//! and corrupted syntax to validate robustness and error handling.

const std = @import("std");
const membank = @import("membank");
const common = @import("common.zig");

const stdx = membank.stdx;

const ZigParser = membank.ZigParser;
const ZigParserConfig = membank.ZigParserConfig;
const SourceContent = membank.SourceContent;

const FUZZ_ITERATIONS_CONTINUOUS = std.math.maxInt(u64);

/// Run Zig parser fuzzing with specified parameters
pub fn run(
    allocator: std.mem.Allocator,
    iterations: u64,
    seed: u64,
    verbose_mode: *stdx.ProtectedType(bool),
) !void {
    const is_continuous = (iterations == FUZZ_ITERATIONS_CONTINUOUS);
    if (is_continuous) {
        std.debug.print("Fuzzing Zig parser continuously, starting seed {}\n", .{seed});
    } else {
        std.debug.print("Fuzzing Zig parser: {} iterations, seed {}\n", .{ iterations, seed });
    }

    var prng = std.Random.DefaultPrng.init(seed);
    const random = prng.random();
    var failures: u64 = 0;
    var total_completed: u64 = 0;

    const max_iter = if (is_continuous) std.math.maxInt(u64) else iterations;
    var i: u64 = 0;
    while (i < max_iter) : (i += 1) {
        var arena = std.heap.ArenaAllocator.init(allocator);
        defer arena.deinit();
        const test_allocator = arena.allocator();

        const result = run_single_iteration(test_allocator, random) catch |err| blk: {
            try common.report_crash(allocator, "parser", i, seed, err);
            break :blk common.FuzzResult.crash;
        };

        total_completed += 1;
        if (result == .crash) {
            failures += 1;
            std.debug.print("  Iteration {}: CRASH detected - report saved\n", .{i});
        }

        if (is_continuous) {
            if (i > 0 and i % 10_000 == 0) {
                if (verbose_mode.with(
                    fn (*const bool, void) bool,
                    {},
                    struct {
                        fn f(verbose: *const bool, ctx: void) bool {
                            _ = ctx;
                            return verbose.*;
                        }
                    }.f,
                )) {
                    std.debug.print("  Progress: {} iterations completed, {} failures\n", .{ i + 1, failures });
                } else {
                    std.debug.print("  Parser: {} iters, {} crashes\n", .{ i + 1, failures });
                }
            }
        } else if (i > 0 and i % 1_000 == 0) {
            if (verbose_mode.with(
                fn (*const bool, void) bool,
                {},
                struct {
                    fn f(verbose: *const bool, ctx: void) bool {
                        _ = ctx;
                        return verbose.*;
                    }
                }.f,
            )) {
                std.debug.print("  Progress: {}/{} ({d:.1}%)\n", .{ i + 1, iterations, @as(f64, @floatFromInt(i + 1)) / @as(f64, @floatFromInt(iterations)) * 100.0 });
            } else {
                std.debug.print("  Parser: {}/{} ({d:.0}%)\n", .{ i + 1, iterations, @as(f64, @floatFromInt(i + 1)) / @as(f64, @floatFromInt(iterations)) * 100.0 });
            }
        }
    }

    if (verbose_mode.with(
        fn (*const bool, void) bool,
        {},
        struct {
            fn f(verbose: *const bool, ctx: void) bool {
                _ = ctx;
                return verbose.*;
            }
        }.f,
    )) {
        if (is_continuous) {
            std.debug.print("  Continuous fuzzing stopped after {} iterations: {} failures\n", .{ total_completed, failures });
        } else {
            std.debug.print("  Completed: {} failures out of {} iterations\n", .{ failures, iterations });
        }
    } else {
        if (is_continuous) {
            std.debug.print("  Parser fuzzing stopped: {} iterations, {} crashes\n", .{ total_completed, failures });
        } else {
            std.debug.print("  Parser completed: {} iterations, {} crashes\n", .{ iterations, failures });
        }
    }
}

/// Execute a single fuzzing iteration against Zig parser
fn run_single_iteration(allocator: std.mem.Allocator, random: std.Random) !common.FuzzResult {
    const config = ZigParserConfig{
        .include_function_bodies = random.boolean(),
        .include_private = random.boolean(),
        .include_inline_comments = random.boolean(),
        .include_tests = random.boolean(),
        .max_unit_size = random.intRangeAtMost(u32, 100, 10000),
    };

    var parser = ZigParser.init(allocator, config);
    defer parser.deinit();

    // Generate malformed Zig source code
    const source_code = try common.generate_malformed_zig_source(allocator, random);
    defer allocator.free(source_code);

    // Create source content
    var metadata = std.StringHashMap([]const u8).init(allocator);
    try metadata.put("file_path", "fuzz_test.zig");

    const source_content = SourceContent{
        .data = source_code,
        .content_type = "text/zig",
        .metadata = metadata,
        .timestamp_ns = @intCast(std.time.nanoTimestamp()),
    };

    // Try to parse - this should handle malformed input gracefully
    _ = parser.parser().parse(allocator, source_content) catch {
        // Parsing errors are expected with malformed input
        return common.FuzzResult.expected_error;
    };

    return common.FuzzResult.success;
}
