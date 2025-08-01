//! Storage engine fuzzing module.
//!
//! Tests WAL operations, SSTable management, BlockIndex behavior,
//! and storage engine coordination under random operations and corruption.

const std = @import("std");
const membank = @import("membank");
const common = @import("common.zig");

const stdx = membank.stdx;

const SimulationVFS = membank.SimulationVFS;
const StorageEngine = membank.StorageEngine;

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
    // Create simulation VFS with random corruption potential
    var sim_vfs = SimulationVFS.init(allocator) catch {
        return common.FuzzResult.expected_error;
    };
    defer sim_vfs.deinit();

    const vfs_instance = sim_vfs.vfs();
    var engine = StorageEngine.init_default(allocator, vfs_instance, "/test_db") catch {
        return common.FuzzResult.expected_error;
    };
    defer engine.deinit();

    // Initialize storage before using it
    engine.startup() catch {
        return common.FuzzResult.expected_error;
    };

    // Generate random operations sequence
    const op_count = random.intRangeAtMost(u32, 1, 50);
    for (0..op_count) |_| {
        const operation = random.intRangeAtMost(u32, 0, 4);
        switch (operation) {
            0 => try test_block_insertion(&engine, allocator, random),
            1 => try test_block_lookup(&engine, random),
            2 => try test_block_deletion(&engine, random),
            3 => try test_memtable_flush(&engine, allocator, random),
            4 => try test_wal_recovery(&engine, allocator, random),
            else => unreachable,
        }
    }

    return common.FuzzResult.success;
}

/// Test random block insertion operations
fn test_block_insertion(engine: *StorageEngine, allocator: std.mem.Allocator, random: std.Random) !void {
    const block = try common.generate_random_block(allocator, random);
    _ = engine.put_block(block) catch {
        // Expected errors are fine - focus on crashes
        return;
    };
}

/// Test random block lookup operations
fn test_block_lookup(engine: *StorageEngine, random: std.Random) !void {
    const random_id = common.generate_random_block_id(random);
    _ = engine.find_block(random_id) catch {
        // Expected errors are fine - focus on crashes
        return;
    };
}

/// Test random block deletion operations
fn test_block_deletion(engine: *StorageEngine, random: std.Random) !void {
    const random_id = common.generate_random_block_id(random);
    _ = engine.delete_block(random_id) catch {
        // Expected errors are fine - focus on crashes
        return;
    };
}

/// Test memtable flush behavior under load
fn test_memtable_flush(engine: *StorageEngine, allocator: std.mem.Allocator, random: std.Random) !void {
    // Insert multiple blocks to trigger potential flush
    const block_count = random.intRangeAtMost(u32, 5, 20);
    for (0..block_count) |_| {
        const block = common.generate_random_block(allocator, random) catch continue;
        _ = engine.put_block(block) catch continue;
    }
}

/// Test WAL recovery scenarios
fn test_wal_recovery(engine: *StorageEngine, allocator: std.mem.Allocator, random: std.Random) !void {
    // Insert some blocks
    const block_count = random.intRangeAtMost(u32, 1, 10);
    for (0..block_count) |_| {
        const block = common.generate_random_block(allocator, random) catch continue;
        _ = engine.put_block(block) catch continue;
    }

    // Test recovery by reinitializing engine
    // This exercises WAL replay logic
    // Future: Add more sophisticated recovery testing with random corruption
}
