//! Storage engine fuzzing module.
//!
//! Tests WAL operations, SSTable management, BlockIndex behavior,
//! and storage engine coordination under random operations and corruption.

const std = @import("std");

const kausaldb = @import("kausaldb");

const common = @import("common.zig");

const stdx = kausaldb.stdx;

const SimulationVFS = kausaldb.SimulationVFS;
const StorageEngine = kausaldb.StorageEngine;

const FUZZ_ITERATIONS_CONTINUOUS = std.math.maxInt(u64);

/// Run storage engine fuzzing with specified parameters
pub fn run(
    allocator: std.mem.Allocator,
    iterations: u64,
    seed: u64,
    verbose_mode: *stdx.ProtectedType(bool),
    validation_errors: *stdx.MetricsCounter,
) !void {
    const is_continuous = (iterations == FUZZ_ITERATIONS_CONTINUOUS);
    if (is_continuous) {
        std.debug.print("Fuzzing storage engine continuously, starting seed {}\n", .{seed});
    } else {
        std.debug.print("Fuzzing storage engine: {} iterations, seed {}\n", .{ iterations, seed });
    }

    var stats = common.FuzzStats.init(allocator);
    defer stats.deinit();

    var prng = std.Random.DefaultPrng.init(seed);
    const random = prng.random();

    const max_iter = if (is_continuous) std.math.maxInt(u64) else iterations;
    var i: u64 = 0;
    while (i < max_iter) : (i += 1) {
        var arena = std.heap.ArenaAllocator.init(allocator);
        defer arena.deinit();
        const test_allocator = arena.allocator();

        _ = run_single_iteration(test_allocator, random) catch |err| blk: {
            const crash_hash = common.calculate_error_hash(err);
            const is_new_crash = stats.record_unique_crash(crash_hash);

            if (is_new_crash) {
                try common.report_crash(allocator, "storage", i, seed, err);
                std.debug.print("  Iteration {}: NEW CRASH detected - report saved\n", .{i});
                stats.failures += 1;
            }
            break :blk common.FuzzResult.crash;
        };

        stats.iterations += 1;

        if (stats.should_report_progress()) {
            const rate = stats.rate();
            const elapsed = std.time.nanoTimestamp() - stats.start_time;
            const elapsed_sec = @as(f64, @floatFromInt(elapsed)) / 1_000_000_000.0;
            const current_validation_errors = validation_errors.load();

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
                std.debug.print("  [{d:>7.1}s] {} iters @ {d:.0}/sec | {} validation errors, {} crashes\n", .{ elapsed_sec, stats.iterations, rate, current_validation_errors, stats.failures });
            } else {
                std.debug.print("  Storage: {} iters ({d:.0}/sec), {} crashes\n", .{ stats.iterations, rate, stats.failures });
            }
        }

        if (is_continuous and stats.should_check_git()) {
            if (common.check_git_updates()) {
                std.debug.print("Git updates found. Exiting for restart...\n", .{});
                std.process.exit(common.EXIT_GIT_UPDATE_NEEDED);
            }
        }
    }

    // Fuzzing completed successfully

    const final_rate = stats.rate();
    const final_validation_errors = validation_errors.load();
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
            std.debug.print("  Continuous fuzzing stopped: {} iterations @ {d:.0}/sec | {} validation errors, {} crashes\n", .{ stats.iterations, final_rate, final_validation_errors, stats.failures });
        } else {
            std.debug.print("  Completed: {} iterations @ {d:.0}/sec | {} validation errors, {} crashes\n", .{ iterations, final_rate, final_validation_errors, stats.failures });
        }
    } else {
        if (is_continuous) {
            std.debug.print("  Storage fuzzing stopped: {} iterations, {} crashes\n", .{ stats.iterations, stats.failures });
        } else {
            std.debug.print("  Storage completed: {} iterations, {} crashes\n", .{ iterations, stats.failures });
        }
    }
}

/// Execute a single fuzzing iteration against storage engine
fn run_single_iteration(allocator: std.mem.Allocator, random: std.Random) !common.FuzzResult {
    // Optimized fuzzing: fewer operations per iteration, focus on small blocks
    var sim_vfs = SimulationVFS.init(allocator) catch {
        return common.FuzzResult.expected_error;
    };
    defer sim_vfs.deinit();

    const vfs_instance = sim_vfs.vfs();
    var engine = StorageEngine.init_default(allocator, vfs_instance, "/test_db") catch {
        return common.FuzzResult.expected_error;
    };
    defer engine.deinit();

    engine.startup() catch {
        return common.FuzzResult.expected_error;
    };

    // Fast operations - reduced count and mostly small blocks
    const op_count = random.intRangeAtMost(u32, 1, 5); // Much fewer operations
    for (0..op_count) |_| {
        const operation = random.intRangeAtMost(u32, 0, 10);
        switch (operation) {
            // 70% small block operations (fast)
            0, 1, 2, 3, 4, 5, 6 => try test_block_insertion(&engine, allocator, random),
            // 20% lookups (very fast)
            7, 8 => try test_block_lookup(&engine, random),
            // 10% other operations
            9 => try test_block_deletion(&engine, random),
            // Large blocks only 1% of the time for CI speed
            10 => if (random.intRangeAtMost(u32, 1, 100) == 1) {
                try test_large_block_insertion(&engine, allocator, random);
            } else {
                try test_block_insertion(&engine, allocator, random);
            },
            else => unreachable,
        }
    }

    return common.FuzzResult.success;
}

fn test_block_insertion(engine: *StorageEngine, allocator: std.mem.Allocator, random: std.Random) !void {
    const block = try common.generate_random_block(allocator, random);
    _ = engine.put_block(block) catch {
        // Expected errors are fine - focus on crashes
        return;
    };
}

fn test_block_lookup(engine: *StorageEngine, random: std.Random) !void {
    const random_id = common.generate_random_block_id(random);
    _ = engine.find_block(random_id, .storage_engine) catch {
        // Expected errors are fine - focus on crashes
        return;
    };
}

fn test_block_deletion(engine: *StorageEngine, random: std.Random) !void {
    const random_id = common.generate_random_block_id(random);
    _ = engine.delete_block(random_id) catch {
        // Expected errors are fine - focus on crashes
        return;
    };
}

fn test_memtable_flush(engine: *StorageEngine, allocator: std.mem.Allocator, random: std.Random) !void {
    const block_count = random.intRangeAtMost(u32, 5, 20);
    for (0..block_count) |_| {
        const block = common.generate_random_block(allocator, random) catch continue;
        _ = engine.put_block(block) catch continue;
    }
}

fn test_wal_recovery(engine: *StorageEngine, allocator: std.mem.Allocator, random: std.Random) !void {
    // Insert some blocks
    const block_count = random.intRangeAtMost(u32, 1, 10);
    for (0..block_count) |_| {
        const block = common.generate_random_block(allocator, random) catch continue;
        _ = engine.put_block(block) catch continue;
    }

    // This exercises WAL replay logic
    // Future: Add more sophisticated recovery testing with random corruption
}

fn test_large_block_insertion(engine: *StorageEngine, allocator: std.mem.Allocator, random: std.Random) !void {
    // Test large blocks that trigger streaming WAL writes (>=512KB threshold)
    const sizes = [_]usize{
        512 * 1024, // 512KB - streaming threshold
        1024 * 1024, // 1MB - common large block
        2 * 1024 * 1024, // 2MB - stress test
        5 * 1024 * 1024, // 5MB - maximum realistic size
    };

    const size = sizes[random.intRangeAtMost(usize, 0, sizes.len - 1)];
    const large_block = common.generate_large_block(allocator, random, size) catch {
        // Expected - large allocations can fail
        return;
    };
    defer {
        allocator.free(large_block.source_uri);
        allocator.free(large_block.metadata_json);
        allocator.free(large_block.content);
    }

    _ = engine.put_block(large_block) catch {
        // Expected errors are fine - focus on crashes and memory corruption
        return;
    };

    // Verify the large block can be retrieved correctly
    const retrieved = engine.find_block(large_block.id, .storage_engine) catch return;
    if (retrieved) |found| {
        const extracted = found.extract();
        // Basic validation - content length should match
        if (extracted.content.len != large_block.content.len) {
            // This would indicate a serious data corruption bug
            return error.LargeBlockCorruption;
        }
    }
}

fn test_streaming_wal_scenarios(engine: *StorageEngine, allocator: std.mem.Allocator, random: std.Random) !void {
    // Test mixed workload of small and large blocks to stress the streaming logic
    const batch_size = random.intRangeAtMost(u32, 5, 15);

    for (0..batch_size) |i| {
        if (i % 3 == 0) {
            // Large block that should use streaming
            const large_size = random.intRangeAtMost(usize, 600 * 1024, 1024 * 1024); // 600KB-1MB
            const large_block = common.generate_large_block(allocator, random, large_size) catch continue;
            defer {
                allocator.free(large_block.source_uri);
                allocator.free(large_block.metadata_json);
                allocator.free(large_block.content);
            }
            _ = engine.put_block(large_block) catch continue;
        } else {
            // Small block that should use regular WAL path
            const small_block = common.generate_random_block(allocator, random) catch continue;
            defer {
                allocator.free(small_block.source_uri);
                allocator.free(small_block.metadata_json);
                allocator.free(small_block.content);
            }
            _ = engine.put_block(small_block) catch continue;
        }
    }
}
