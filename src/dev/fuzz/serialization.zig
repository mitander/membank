//! Block serialization fuzzing module.
//!
//! Tests ContextBlock serialization and deserialization with corrupted data,
//! edge cases, and malformed input to validate data integrity and error handling.

const std = @import("std");
const membank = @import("membank");
const common = @import("common.zig");

const stdx = membank.stdx;

const ContextBlock = membank.ContextBlock;

const FUZZ_ITERATIONS_CONTINUOUS = std.math.maxInt(u64);

/// Run serialization fuzzing with specified parameters
pub fn run(
    allocator: std.mem.Allocator,
    iterations: u64,
    seed: u64,
    verbose_mode: *stdx.ProtectedType(bool),
) !void {
    const is_continuous = (iterations == FUZZ_ITERATIONS_CONTINUOUS);
    if (is_continuous) {
        std.debug.print("Fuzzing serialization continuously, starting seed {}\n", .{seed});
    } else {
        std.debug.print("Fuzzing serialization: {} iterations, seed {}\n", .{ iterations, seed });
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
            try common.report_crash(allocator, "serialization", i, seed, err);
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
                    std.debug.print("  Serialization: {} iters, {} crashes\n", .{ i + 1, failures });
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
                std.debug.print("  Serialization: {}/{} ({d:.0}%)\n", .{ i + 1, iterations, @as(f64, @floatFromInt(i + 1)) / @as(f64, @floatFromInt(iterations)) * 100.0 });
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
            std.debug.print("  Serialization fuzzing stopped: {} iterations, {} crashes\n", .{ total_completed, failures });
        } else {
            std.debug.print("  Serialization completed: {} iterations, {} crashes\n", .{ iterations, failures });
        }
    }
}

/// Execute a single fuzzing iteration against serialization system
fn run_single_iteration(allocator: std.mem.Allocator, random: std.Random) !common.FuzzResult {
    // Test ContextBlock serialization with random/corrupted data
    const original_block = try common.generate_random_block(allocator, random);

    // Serialize the block
    const buffer_size = original_block.serialized_size();
    const buffer = try allocator.alloc(u8, buffer_size);
    defer allocator.free(buffer);

    _ = original_block.serialize(buffer) catch {
        return common.FuzzResult.expected_error;
    };

    // Test different corruption strategies
    const corruption_type = random.intRangeAtMost(u32, 0, 3);
    switch (corruption_type) {
        0 => try test_random_corruption(allocator, buffer, random),
        1 => try test_truncation(allocator, buffer, random),
        2 => try test_header_corruption(allocator, buffer, random),
        3 => try test_boundary_corruption(allocator, buffer, random),
        else => unreachable,
    }

    return common.FuzzResult.success;
}

/// Test random byte corruption throughout the buffer
fn test_random_corruption(allocator: std.mem.Allocator, buffer: []const u8, random: std.Random) !void {
    var corrupted = try allocator.dupe(u8, buffer);
    defer allocator.free(corrupted);

    const corruption_count = random.intRangeAtMost(usize, 0, @min(corrupted.len / 10, 50));
    for (0..corruption_count) |_| {
        const pos = random.intRangeAtMost(usize, 0, corrupted.len - 1);
        corrupted[pos] = random.int(u8);
    }

    // Try to deserialize corrupted data
    _ = ContextBlock.deserialize(corrupted, allocator) catch {
        // Deserialization errors are expected with corrupted data
        return;
    };
}

/// Test truncation at various points
fn test_truncation(allocator: std.mem.Allocator, buffer: []const u8, random: std.Random) !void {
    if (buffer.len == 0) return;

    const truncate_at = random.intRangeAtMost(usize, 0, buffer.len - 1);
    const truncated = buffer[0..truncate_at];

    // Try to deserialize truncated data
    _ = ContextBlock.deserialize(truncated, allocator) catch {
        // Deserialization errors are expected with truncated data
        return;
    };
}

/// Test corruption of header/magic bytes
fn test_header_corruption(allocator: std.mem.Allocator, buffer: []const u8, random: std.Random) !void {
    var corrupted = try allocator.dupe(u8, buffer);
    defer allocator.free(corrupted);

    // Corrupt first few bytes (likely header/magic)
    const header_corruption_count = @min(16, corrupted.len);
    for (0..header_corruption_count) |i| {
        if (random.boolean()) {
            corrupted[i] = random.int(u8);
        }
    }

    // Try to deserialize header-corrupted data
    _ = ContextBlock.deserialize(corrupted, allocator) catch {
        // Deserialization errors are expected with header corruption
        return;
    };
}

/// Test corruption at buffer boundaries
fn test_boundary_corruption(allocator: std.mem.Allocator, buffer: []const u8, random: std.Random) !void {
    var corrupted = try allocator.dupe(u8, buffer);
    defer allocator.free(corrupted);

    // Corrupt last few bytes (likely checksum/footer)
    if (corrupted.len > 0) {
        const boundary_corruption_count = @min(8, corrupted.len);
        const start_pos = corrupted.len - boundary_corruption_count;
        for (start_pos..corrupted.len) |i| {
            if (random.boolean()) {
                corrupted[i] = random.int(u8);
            }
        }
    }

    // Try to deserialize boundary-corrupted data
    _ = ContextBlock.deserialize(corrupted, allocator) catch {
        // Deserialization errors are expected with boundary corruption
        return;
    };
}
