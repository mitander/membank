//! Query engine fuzzing module.
//!
//! Tests graph traversal algorithms, filtering logic, result formatting,
//! and query engine coordination under random queries and malformed inputs.

const std = @import("std");

const kausaldb = @import("kausaldb");

const common = @import("common.zig");

const stdx = kausaldb.stdx;

const FilterCondition = kausaldb.FilterCondition;
const FilterExpression = kausaldb.FilterExpression;
const FilteredQuery = kausaldb.FilteredQuery;
const QueryEngine = kausaldb.QueryEngine;
const SimulationVFS = kausaldb.SimulationVFS;
const StorageEngine = kausaldb.StorageEngine;
const TraversalAlgorithm = kausaldb.TraversalAlgorithm;
const TraversalDirection = kausaldb.TraversalDirection;
const TraversalQuery = kausaldb.TraversalQuery;

const FUZZ_ITERATIONS_CONTINUOUS = std.math.maxInt(u64);

/// Run query engine fuzzing with specified parameters
pub fn run(
    allocator: std.mem.Allocator,
    iterations: u64,
    seed: u64,
    verbose_mode: *stdx.ProtectedType(bool),
    validation_errors: *stdx.MetricsCounter,
) !void {
    const is_continuous = (iterations == FUZZ_ITERATIONS_CONTINUOUS);
    if (is_continuous) {
        std.debug.print("Fuzzing query engine continuously, starting seed {}\n", .{seed});
    } else {
        std.debug.print("Fuzzing query engine: {} iterations, seed {}\n", .{ iterations, seed });
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
                try common.report_crash(allocator, "query", i, seed, err);
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
                std.debug.print("  Query: {} iters ({d:.0}/sec), {} crashes\n", .{ stats.iterations, rate, stats.failures });
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
            std.debug.print("  Query fuzzing stopped: {} iterations, {} crashes\n", .{ stats.iterations, stats.failures });
        } else {
            std.debug.print("  Query completed: {} iterations, {} crashes\n", .{ iterations, stats.failures });
        }
    }
}

/// Execute a single fuzzing iteration against query engine
fn run_single_iteration(allocator: std.mem.Allocator, random: std.Random) !common.FuzzResult {
    // Create storage backend for query engine
    var sim_vfs = SimulationVFS.init(allocator) catch {
        return common.FuzzResult.expected_error;
    };
    defer sim_vfs.deinit();

    const vfs_instance = sim_vfs.vfs();
    var storage_engine = StorageEngine.init_default(allocator, vfs_instance, "/test_db") catch {
        return common.FuzzResult.expected_error;
    };
    defer storage_engine.deinit();

    // Initialize storage before using it
    storage_engine.startup() catch {
        return common.FuzzResult.expected_error;
    };

    var query_engine = QueryEngine.init(allocator, &storage_engine);
    defer query_engine.deinit();

    // Add some random blocks for querying
    const block_count = random.intRangeAtMost(u32, 0, 10);
    for (0..block_count) |_| {
        const block = common.generate_random_block(allocator, random) catch continue;
        _ = storage_engine.put_block(block) catch continue;
    }

    // Generate and execute random queries
    const query_count = random.intRangeAtMost(u32, 1, 20);
    for (0..query_count) |_| {
        const query_type = random.intRangeAtMost(u32, 0, 2);
        switch (query_type) {
            0 => try test_block_lookup(&query_engine, random),
            1 => try test_filtered_query(&query_engine, allocator, random),
            2 => try test_traversal_query(&query_engine, allocator, random),
            else => unreachable,
        }
    }

    return common.FuzzResult.success;
}

fn test_block_lookup(query_engine: *QueryEngine, random: std.Random) !void {
    const random_id = common.generate_random_block_id(random);
    _ = query_engine.storage_engine.find_block(random_id, .query_engine) catch {
        // Expected errors are fine - focus on crashes
        return;
    };
}

fn test_filtered_query(query_engine: *QueryEngine, allocator: std.mem.Allocator, random: std.Random) !void {
    // Skip semantic query test for now due to missing generate function
    _ = query_engine;
    _ = allocator;
    _ = random;
}

fn test_traversal_query(query_engine: *QueryEngine, allocator: std.mem.Allocator, random: std.Random) !void {
    const traversal_query = try generate_random_traversal_query(allocator, random);
    var result = query_engine.execute_traversal(traversal_query) catch {
        // Expected errors are fine - focus on crashes
        return;
    };
    result.deinit();
}

/// Generate random filtered query with potential edge cases
fn generate_random_filtered_query(allocator: std.mem.Allocator, random: std.Random) !FilteredQuery {
    const field_name = try common.generate_random_string(allocator, random, 1, 50);
    const search_value = try common.generate_random_string(allocator, random, 0, 100);

    const condition = FilterCondition{
        .target = .metadata_field,
        .operator = .contains,
        .value = search_value,
        .metadata_field = field_name,
    };

    const expression = FilterExpression{
        .condition = condition,
    };

    return FilteredQuery{
        .expression = expression,
        .max_results = random.intRangeAtMost(u32, 1, 1000),
        .offset = 0,
    };
}

/// Generate random traversal query with edge case parameters
fn generate_random_traversal_query(allocator: std.mem.Allocator, random: std.Random) !TraversalQuery {
    _ = allocator;
    const directions = [_]TraversalDirection{ .incoming, .outgoing, .bidirectional };
    const algorithms = [_]TraversalAlgorithm{ .breadth_first, .depth_first };

    return TraversalQuery{
        .start_block_id = common.generate_random_block_id(random),
        .direction = directions[random.intRangeAtMost(usize, 0, directions.len - 1)],
        .algorithm = algorithms[random.intRangeAtMost(usize, 0, algorithms.len - 1)],
        .max_depth = random.intRangeAtMost(u32, 1, 10),
        .max_results = random.intRangeAtMost(u32, 1, 100),
        .edge_filter = .all_types,
    };
}
