//! Real fuzz testing for CortexDB components.
//!
//! This fuzzer tests actual CortexDB components with malformed, corrupted,
//! and edge-case inputs to discover memory safety issues, assertion failures,
//! and other robustness problems.

const std = @import("std");
const cortexdb = @import("cortexdb");

const SimulationVFS = cortexdb.SimulationVFS;
const BlockId = cortexdb.BlockId;
const ContextBlock = cortexdb.ContextBlock;
const StorageEngine = cortexdb.StorageEngine;
const QueryEngine = cortexdb.QueryEngine;
const TraversalQuery = cortexdb.TraversalQuery;
const FilteredQuery = cortexdb.FilteredQuery;
const TraversalDirection = cortexdb.TraversalDirection;
const TraversalAlgorithm = cortexdb.TraversalAlgorithm;
const FilterCondition = cortexdb.FilterCondition;
const FilterExpression = cortexdb.FilterExpression;
const ZigParser = cortexdb.ZigParser;
const ZigParserConfig = cortexdb.ZigParserConfig;
const SourceContent = cortexdb.SourceContent;

const FUZZ_ITERATIONS_DEFAULT = 100_000;
const FUZZ_ITERATIONS_CONTINUOUS = std.math.maxInt(u64);
const FUZZ_SEED_DEFAULT = 42;
const CRASH_REPORT_DIR = "fuzz_reports";
const PROGRESS_INTERVAL_SEC = 60;
const GIT_CHECK_INTERVAL_SEC = 600; // 10 minutes

// Exit codes for coordination with shell script
const EXIT_GIT_UPDATE_NEEDED = 0; // Shell should rebuild and restart
const EXIT_NORMAL = 0;
const EXIT_ERROR = 1;

var global_shutdown_requested: std.atomic.Value(bool) = std.atomic.Value(bool).init(false);

// Check for shutdown request by polling a file or signal
fn check_shutdown_request() bool {
    // Check if shutdown was requested through signal or file
    if (global_shutdown_requested.load(.seq_cst)) {
        return true;
    }

    // Check for shutdown file (lightweight alternative to complex signal handling)
    std.fs.cwd().access(".cortexdb_stop", .{}) catch {
        return false; // File doesn't exist, continue
    };

    // Shutdown file exists - request shutdown
    global_shutdown_requested.store(true, .seq_cst);
    std.debug.print("Shutdown requested via .cortexdb_stop file\n", .{});

    // Clean up the file
    std.fs.cwd().deleteFile(".cortexdb_stop") catch {};
    return true;
}

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{ .safety = true }){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    // Initialize concurrency model
    cortexdb.concurrency.init();

    const args = try std.process.argsAlloc(allocator);
    defer std.process.argsFree(allocator, args);

    if (args.len < 2) {
        try print_usage();
        return;
    }

    const target = args[1];
    const iterations = if (args.len > 2) blk: {
        if (std.mem.eql(u8, args[2], "continuous")) {
            break :blk FUZZ_ITERATIONS_CONTINUOUS;
        }
        break :blk try std.fmt.parseInt(u64, args[2], 10);
    } else FUZZ_ITERATIONS_DEFAULT;

    const seed = if (args.len > 3)
        try std.fmt.parseInt(u64, args[3], 10)
    else
        FUZZ_SEED_DEFAULT;

    // Setup crash reporting
    try setup_crash_reporting(allocator);

    if (std.mem.eql(u8, target, "storage")) {
        try fuzz_storage_engine(allocator, iterations, seed);
    } else if (std.mem.eql(u8, target, "query")) {
        try fuzz_query_engine(allocator, iterations, seed);
    } else if (std.mem.eql(u8, target, "parser")) {
        try fuzz_zig_parser(allocator, iterations, seed);
    } else if (std.mem.eql(u8, target, "serialization")) {
        try fuzz_serialization(allocator, iterations, seed);
    } else if (std.mem.eql(u8, target, "all")) {
        try fuzz_all_targets(allocator, iterations, seed);
    } else {
        std.debug.print("Unknown fuzz target: {s}\n", .{target});
        try print_usage();
        std.process.exit(1);
    }
}

fn print_usage() !void {
    std.debug.print(
        \\CortexDB Production Fuzzer
        \\
        \\Usage:
        \\  fuzz <target> [iterations|continuous] [seed]
        \\
        \\Targets:
        \\  storage         Fuzz storage engine (WAL, SSTables, BlockIndex)
        \\  query           Fuzz query engine processing
        \\  parser          Fuzz Zig source code parser
        \\  serialization   Fuzz block serialization/deserialization
        \\  all             Fuzz all targets
        \\
        \\Iterations:
        \\  <number>        Run specified number of iterations (default: 100,000)
        \\  continuous      Run until manually stopped (24/7 fuzzing)
        \\
        \\Examples:
        \\  fuzz storage 1000000 42          # 1M iterations with seed 42
        \\  fuzz query continuous            # Run forever with random seeds
        \\  fuzz all continuous 12345        # Continuous fuzzing of all targets
        \\
        \\Crash reports are saved to: {s}/
        \\
    , .{CRASH_REPORT_DIR});
}

fn fuzz_all_targets(allocator: std.mem.Allocator, iterations: u64, seed: u64) !void {
    if (iterations == FUZZ_ITERATIONS_CONTINUOUS) {
        std.debug.print("Fuzzing all targets continuously, starting seed {}\n", .{seed});

        var current_seed = seed;
        while (true) {
            const batch_size = 10_000;
            try fuzz_storage_engine(allocator, batch_size, current_seed);
            try fuzz_query_engine(allocator, batch_size, current_seed + 1);
            try fuzz_zig_parser(allocator, batch_size, current_seed + 2);
            try fuzz_serialization(allocator, batch_size, current_seed + 3);
            current_seed += 4;

            // Brief pause to prevent system overload
            std.Thread.sleep(100_000_000); // 100ms
        }
    } else {
        std.debug.print("Fuzzing all targets with {} iterations, seed {}\n", .{ iterations, seed });
        const per_target = iterations / 4;
        try fuzz_storage_engine(allocator, per_target, seed);
        try fuzz_query_engine(allocator, per_target, seed + 1);
        try fuzz_zig_parser(allocator, per_target, seed + 2);
        try fuzz_serialization(allocator, per_target, seed + 3);
    }
}

const FuzzResult = enum {
    success,
    expected_error,
    crash,
};

const FuzzStats = struct {
    iterations: u64 = 0,
    failures: u64 = 0,
    start_time: i128,
    last_progress_time: i128,
    last_git_check: i128,
    seen_crashes: std.ArrayList(u64),

    pub fn init(allocator: std.mem.Allocator) FuzzStats {
        const now = std.time.nanoTimestamp();
        return FuzzStats{
            .start_time = now,
            .last_progress_time = now,
            .last_git_check = now,
            .seen_crashes = std.ArrayList(u64).init(allocator),
        };
    }

    pub fn deinit(self: *FuzzStats) void {
        self.seen_crashes.deinit();
    }

    pub fn should_report_progress(self: *FuzzStats) bool {
        const now = std.time.nanoTimestamp();
        const elapsed = now - self.last_progress_time;
        if (elapsed >= std.time.ns_per_s * PROGRESS_INTERVAL_SEC) {
            self.last_progress_time = now;
            return true;
        }
        return false;
    }

    pub fn should_check_git(self: *FuzzStats) bool {
        const now = std.time.nanoTimestamp();
        const elapsed = now - self.last_git_check;
        if (elapsed >= std.time.ns_per_s * GIT_CHECK_INTERVAL_SEC) {
            self.last_git_check = now;
            return true;
        }
        return false;
    }

    pub fn rate(self: *const FuzzStats) f64 {
        const now = std.time.nanoTimestamp();
        const elapsed_ns = now - self.start_time;
        const elapsed_sec = @as(f64, @floatFromInt(elapsed_ns)) / 1_000_000_000.0;
        if (elapsed_sec > 0.0) {
            return @as(f64, @floatFromInt(self.iterations)) / elapsed_sec;
        }
        return 0.0;
    }

    pub fn record_unique_crash(self: *FuzzStats, crash_hash: u64) bool {
        // Simple linear search for uniqueness - fine for small crash counts
        for (self.seen_crashes.items) |seen_hash| {
            if (seen_hash == crash_hash) {
                return false; // Already seen
            }
        }
        self.seen_crashes.append(crash_hash) catch return false;
        return true; // New crash
    }
};

fn fuzz_storage_engine(allocator: std.mem.Allocator, iterations: u64, seed: u64) !void {
    const is_continuous = (iterations == FUZZ_ITERATIONS_CONTINUOUS);
    if (is_continuous) {
        std.debug.print("Fuzzing storage engine continuously, starting seed {}\n", .{seed});
    } else {
        std.debug.print("Fuzzing storage engine: {} iterations, seed {}\n", .{ iterations, seed });
    }

    var stats = FuzzStats.init(allocator);
    defer stats.deinit();

    var prng = std.Random.DefaultPrng.init(seed);
    const random = prng.random();

    const max_iter = if (is_continuous) std.math.maxInt(u64) else iterations;
    var i: u64 = 0;
    while (i < max_iter) : (i += 1) {
        var arena = std.heap.ArenaAllocator.init(allocator);
        defer arena.deinit();
        const test_allocator = arena.allocator();

        _ = fuzz_storage_iteration(test_allocator, random) catch |err| blk: {
            const crash_hash = calculate_error_hash(err);
            const is_new_crash = stats.record_unique_crash(crash_hash);

            if (is_new_crash) {
                try report_crash(allocator, "storage", i, seed, err);
                std.debug.print("  Iteration {}: NEW CRASH detected - report saved\n", .{i});
                stats.failures += 1;
            }
            break :blk FuzzResult.crash;
        };

        stats.iterations += 1;

        if (stats.should_report_progress()) {
            const rate = stats.rate();
            const elapsed = std.time.nanoTimestamp() - stats.start_time;
            const elapsed_sec = @as(f64, @floatFromInt(elapsed)) / 1_000_000_000.0;

            std.debug.print("  [{d:>7.1}s] {} iters @ {d:.0}/sec | {} unique crashes\n", .{ elapsed_sec, stats.iterations, rate, stats.failures });
        }

        if (is_continuous and stats.should_check_git()) {
            if (check_git_updates()) {
                std.debug.print("Git updates found. Exiting for restart...\n", .{});
                std.process.exit(EXIT_GIT_UPDATE_NEEDED);
            }
        }
    }

    const final_rate = stats.rate();
    if (is_continuous) {
        std.debug.print("  Continuous fuzzing stopped: {} iterations @ {d:.0}/sec, {} unique crashes\n", .{ stats.iterations, final_rate, stats.failures });
    } else {
        std.debug.print("  Completed: {} unique crashes out of {} iterations @ {d:.0}/sec\n", .{ stats.failures, iterations, final_rate });
    }
}

fn fuzz_storage_iteration(allocator: std.mem.Allocator, random: std.Random) !FuzzResult {
    // Create simulation VFS with random corruption potential
    var sim_vfs = SimulationVFS.init(allocator) catch {
        return FuzzResult.expected_error;
    };
    defer sim_vfs.deinit();

    const vfs_instance = sim_vfs.vfs();
    var engine = StorageEngine.init_default(allocator, vfs_instance, "/test_db") catch {
        return FuzzResult.expected_error;
    };
    defer engine.deinit();

    // Initialize storage before using it
    engine.startup() catch {
        return FuzzResult.expected_error;
    };

    // Generate random operations
    const op_count = random.intRangeAtMost(u32, 1, 50);
    for (0..op_count) |_| {
        const operation = random.intRangeAtMost(u32, 0, 3);
        switch (operation) {
            0 => {
                // Random block insertion
                const block = try generate_random_block(allocator, random);
                _ = engine.put_block(block) catch {
                    // Expected errors are fine
                    continue;
                };
            },
            1 => {
                // Random block lookup
                const random_id = generate_random_block_id(random);
                _ = engine.find_block(random_id) catch {
                    // Expected errors are fine
                    continue;
                };
            },
            2 => {
                // Random block deletion
                const random_id = generate_random_block_id(random);
                _ = engine.delete_block(random_id) catch {
                    // Expected errors are fine
                    continue;
                };
            },
            3 => {
                // Try to flush memtable to trigger potential issues
                _ = engine.put_block(try generate_random_block(allocator, random)) catch {
                    // Expected errors are fine
                    continue;
                };
            },
            else => unreachable,
        }
    }

    return FuzzResult.success;
}

fn fuzz_query_engine(allocator: std.mem.Allocator, iterations: u64, seed: u64) !void {
    const is_continuous = (iterations == FUZZ_ITERATIONS_CONTINUOUS);
    if (is_continuous) {
        std.debug.print("Fuzzing query engine continuously, starting seed {}\n", .{seed});
    } else {
        std.debug.print("Fuzzing query engine: {} iterations, seed {}\n", .{ iterations, seed });
    }

    var stats = FuzzStats.init(allocator);
    defer stats.deinit();

    var prng = std.Random.DefaultPrng.init(seed);
    const random = prng.random();

    const max_iter = if (is_continuous) std.math.maxInt(u64) else iterations;
    var i: u64 = 0;
    while (i < max_iter) : (i += 1) {
        var arena = std.heap.ArenaAllocator.init(allocator);
        defer arena.deinit();
        const test_allocator = arena.allocator();

        _ = fuzz_query_iteration(test_allocator, random) catch |err| blk: {
            const crash_hash = calculate_error_hash(err);
            const is_new_crash = stats.record_unique_crash(crash_hash);

            if (is_new_crash) {
                try report_crash(allocator, "query", i, seed, err);
                std.debug.print("  Iteration {}: NEW CRASH detected - report saved\n", .{i});
                stats.failures += 1;
            }
            break :blk FuzzResult.crash;
        };

        stats.iterations += 1;

        if (stats.should_report_progress()) {
            const rate = stats.rate();
            const elapsed = std.time.nanoTimestamp() - stats.start_time;
            const elapsed_sec = @as(f64, @floatFromInt(elapsed)) / 1_000_000_000.0;

            std.debug.print("  [{d:>7.1}s] {} iters @ {d:.0}/sec | {} unique crashes\n", .{ elapsed_sec, stats.iterations, rate, stats.failures });
        }

        if (is_continuous and stats.should_check_git()) {
            if (check_git_updates()) {
                std.debug.print("Git updates found. Exiting for restart...\n", .{});
                std.process.exit(EXIT_GIT_UPDATE_NEEDED);
            }
        }
    }

    const final_rate = stats.rate();
    if (is_continuous) {
        std.debug.print("  Continuous fuzzing stopped: {} iterations @ {d:.0}/sec, {} unique crashes\n", .{ stats.iterations, final_rate, stats.failures });
    } else {
        std.debug.print("  Completed: {} unique crashes out of {} iterations @ {d:.0}/sec\n", .{ stats.failures, iterations, final_rate });
    }
}

fn fuzz_query_iteration(allocator: std.mem.Allocator, random: std.Random) !FuzzResult {
    // Create storage backend for query engine
    var sim_vfs = SimulationVFS.init(allocator) catch {
        return FuzzResult.expected_error;
    };
    defer sim_vfs.deinit();

    const vfs_instance = sim_vfs.vfs();
    var storage_engine = StorageEngine.init_default(allocator, vfs_instance, "/test_db") catch {
        return FuzzResult.expected_error;
    };
    defer storage_engine.deinit();

    // Initialize storage before using it
    storage_engine.startup() catch {
        return FuzzResult.expected_error;
    };

    var query_eng = QueryEngine.init(allocator, &storage_engine);
    defer query_eng.deinit();

    // Add some random blocks first
    const block_count = random.intRangeAtMost(u32, 0, 10);
    for (0..block_count) |_| {
        const block = try generate_random_block(allocator, random);
        _ = storage_engine.put_block(block) catch continue;
    }

    // Generate and execute random queries
    const query_count = random.intRangeAtMost(u32, 1, 20);
    for (0..query_count) |_| {
        const query_type = random.intRangeAtMost(u32, 0, 2);
        switch (query_type) {
            0 => {
                // Random block ID query
                const random_id = generate_random_block_id(random);
                _ = query_eng.storage_engine.find_block(random_id) catch continue;
            },
            1 => {
                // Random filtered query
                const filter_query = try generate_random_filtered_query(allocator, random);
                var result = query_eng.execute_filtered_query(filter_query) catch continue;
                result.deinit();
            },
            2 => {
                // Random traversal query
                const traversal_query = try generate_random_traversal_query(allocator, random);
                var result = query_eng.execute_traversal(traversal_query) catch continue;
                result.deinit();
            },
            else => unreachable,
        }
    }

    return FuzzResult.success;
}

fn fuzz_zig_parser(allocator: std.mem.Allocator, iterations: u64, seed: u64) !void {
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

        const result = fuzz_parser_iteration(test_allocator, random) catch |err| blk: {
            try report_crash(allocator, "parser", i, seed, err);
            break :blk FuzzResult.crash;
        };

        total_completed += 1;
        if (result == .crash) {
            failures += 1;
            std.debug.print("  Iteration {}: CRASH detected - report saved\n", .{i});
        }

        if (is_continuous) {
            if (i > 0 and i % 10_000 == 0) {
                std.debug.print("  Progress: {} iterations completed, {} failures\n", .{ i + 1, failures });
            }
        } else if (i > 0 and i % 1_000 == 0) {
            std.debug.print("  Progress: {}/{} ({d:.1}%)\n", .{ i + 1, iterations, @as(f64, @floatFromInt(i + 1)) / @as(f64, @floatFromInt(iterations)) * 100.0 });
        }
    }

    if (is_continuous) {
        std.debug.print("  Continuous fuzzing stopped after {} iterations: {} failures\n", .{ total_completed, failures });
    } else {
        std.debug.print("  Completed: {} failures out of {} iterations\n", .{ failures, iterations });
    }
}

fn fuzz_parser_iteration(allocator: std.mem.Allocator, random: std.Random) !FuzzResult {
    const config = ZigParserConfig{
        .include_function_bodies = random.boolean(),
        .include_private = random.boolean(),
        .include_inline_comments = random.boolean(),
        .include_tests = random.boolean(),
        .max_unit_size = random.intRangeAtMost(u32, 100, 10000),
    };

    var parser = ZigParser.init(allocator, config);
    defer parser.deinit(allocator);

    // Generate malformed Zig source code
    const source_code = try generate_malformed_zig_source(allocator, random);
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
        return FuzzResult.expected_error;
    };

    return FuzzResult.success;
}

fn fuzz_serialization(allocator: std.mem.Allocator, iterations: u64, seed: u64) !void {
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

        const result = fuzz_serialization_iteration(test_allocator, random) catch |err| blk: {
            try report_crash(allocator, "serialization", i, seed, err);
            break :blk FuzzResult.crash;
        };

        total_completed += 1;
        if (result == .crash) {
            failures += 1;
            std.debug.print("  Iteration {}: CRASH detected - report saved\n", .{i});
        }

        if (is_continuous) {
            if (i > 0 and i % 10_000 == 0) {
                std.debug.print("  Progress: {} iterations completed, {} failures\n", .{ i + 1, failures });
            }
        } else if (i > 0 and i % 1_000 == 0) {
            std.debug.print("  Progress: {}/{} ({d:.1}%)\n", .{ i + 1, iterations, @as(f64, @floatFromInt(i + 1)) / @as(f64, @floatFromInt(iterations)) * 100.0 });
        }
    }

    if (is_continuous) {
        std.debug.print("  Continuous fuzzing stopped after {} iterations: {} failures\n", .{ total_completed, failures });
    } else {
        std.debug.print("  Completed: {} failures out of {} iterations\n", .{ failures, iterations });
    }
}

fn fuzz_serialization_iteration(allocator: std.mem.Allocator, random: std.Random) !FuzzResult {
    // Test ContextBlock serialization with random/corrupted data
    const original_block = try generate_random_block(allocator, random);

    // Serialize
    const buffer_size = original_block.serialized_size();
    const buffer = try allocator.alloc(u8, buffer_size);
    defer allocator.free(buffer);

    _ = original_block.serialize(buffer) catch {
        return FuzzResult.expected_error;
    };

    // Corrupt some bytes randomly
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
        return FuzzResult.expected_error;
    };

    return FuzzResult.success;
}

// Crash reporting infrastructure

fn setup_crash_reporting(allocator: std.mem.Allocator) !void {
    // Ensure crash report directory exists
    std.fs.cwd().makeDir(CRASH_REPORT_DIR) catch |err| switch (err) {
        error.PathAlreadyExists => {},
        else => return err,
    };

    // Log startup
    const timestamp = std.time.timestamp();
    std.debug.print("Fuzzing session started at {}\n", .{timestamp});
    std.debug.print("Crash reports will be saved to: {s}/\n", .{CRASH_REPORT_DIR});

    _ = allocator;
}

fn report_crash(allocator: std.mem.Allocator, target: []const u8, iteration: u64, seed: u64, err: anyerror) !void {
    const timestamp = std.time.timestamp();
    const report_filename = try std.fmt.allocPrint(allocator, "{s}/crash_{s}_iter{}_seed{}_{}.txt", .{ CRASH_REPORT_DIR, target, iteration, seed, timestamp });
    defer allocator.free(report_filename);

    const report_file = std.fs.cwd().createFile(report_filename, .{}) catch |create_err| {
        std.debug.print("Failed to create crash report {s}: {}\n", .{ report_filename, create_err });
        return;
    };
    defer report_file.close();

    const report_content = try std.fmt.allocPrint(allocator,
        \\CortexDB Fuzzer Crash Report
        \\============================
        \\
        \\Timestamp: {}
        \\Target: {s}
        \\Iteration: {}
        \\Seed: {}
        \\Error: {}
        \\
        \\Stack Trace:
        \\(Stack traces require debug builds)
        \\
        \\Reproduction Command:
        \\./zig-out/bin/fuzz-debug {s} 1 {}
        \\
        \\System Information:
        \\Zig Version: {s}
        \\Build Mode: {s}
        \\
    , .{ timestamp, target, iteration, seed, err, target, seed, @import("builtin").zig_version_string, @tagName(@import("builtin").mode) });
    defer allocator.free(report_content);

    try report_file.writeAll(report_content);

    std.debug.print("Crash report saved: {s}\n", .{report_filename});
}

fn calculate_error_hash(err: anyerror) u64 {
    var hasher = std.hash.Wyhash.init(0);
    const error_name = @errorName(err);
    hasher.update(error_name);
    return hasher.final();
}

fn check_git_updates() bool {
    // Simple git check - return true if updates are available
    const result = std.process.Child.run(.{
        .allocator = std.heap.page_allocator,
        .argv = &[_][]const u8{ "git", "fetch", "--dry-run" },
        .max_output_bytes = 1024,
    }) catch {
        return false; // Error checking git, assume no updates
    };
    defer std.heap.page_allocator.free(result.stdout);
    defer std.heap.page_allocator.free(result.stderr);

    // If fetch has output, there are updates
    return result.stderr.len > 0;
}

// Helper functions for generating random test data

fn generate_random_block_id(random: std.Random) BlockId {
    var bytes: [16]u8 = undefined;
    random.bytes(&bytes);
    return BlockId{ .bytes = bytes };
}

fn generate_random_block(allocator: std.mem.Allocator, random: std.Random) !ContextBlock {
    const id = generate_random_block_id(random);
    const version = random.int(u64);

    // Generate random strings with potential edge cases
    const source_uri = try generate_random_string(allocator, random, 1, 200);
    const metadata_json = try generate_random_json_like_string(allocator, random);
    const content = try generate_random_string(allocator, random, 0, 1000);

    return ContextBlock{
        .id = id,
        .version = version,
        .source_uri = source_uri,
        .metadata_json = metadata_json,
        .content = content,
    };
}

fn generate_random_string(allocator: std.mem.Allocator, random: std.Random, min_len: usize, max_len: usize) ![]u8 {
    const len = random.intRangeAtMost(usize, min_len, max_len);
    const str = try allocator.alloc(u8, len);

    for (str) |*byte| {
        if (random.boolean()) {
            // ASCII printable characters
            byte.* = random.intRangeAtMost(u8, 32, 126);
        } else {
            // Random bytes including nulls and control characters
            byte.* = random.int(u8);
        }
    }

    return str;
}

fn generate_random_json_like_string(allocator: std.mem.Allocator, random: std.Random) ![]u8 {
    const templates = [_][]const u8{
        "{}",
        "{\"key\":\"value\"}",
        "{\"number\":42}",
        "{\"bool\":true}",
        "{\"nested\":{\"inner\":\"value\"}}",
        "[1,2,3]",
        "null",
        "\"string\"",
        // Malformed JSON
        "{",
        "}",
        "{\"unclosed\":\"",
        "{\"key\":}",
        "{\"key\"::\"value\"}",
        "",
    };

    const template = templates[random.intRangeAtMost(usize, 0, templates.len - 1)];
    var result = try allocator.dupe(u8, template);

    // Randomly corrupt some characters
    if (result.len > 0 and random.boolean()) {
        const pos = random.intRangeAtMost(usize, 0, result.len - 1);
        result[pos] = random.int(u8);
    }

    return result;
}

fn generate_malformed_zig_source(allocator: std.mem.Allocator, random: std.Random) ![]u8 {
    const templates = [_][]const u8{
        "const std = @import(\"std\");",
        "pub fn main() void {}",
        "const VALUE = 42;",
        "pub const Struct = struct { field: u32 };",
        "test \"example\" { try std.testing.expect(true); }",
        // Malformed Zig code
        "const = ;",
        "fn () { return; }",
        "struct { pub fn }",
        "{{{{{",
        "}}}}}",
        "const std = @import(",
        "pub fn main() void {",
        "",
        "\x00\x01\x02\x03",
    };

    var result = std.ArrayList(u8).init(allocator);
    defer result.deinit();

    const line_count = random.intRangeAtMost(usize, 1, 20);
    for (0..line_count) |i| {
        if (i > 0) try result.append('\n');

        const template = templates[random.intRangeAtMost(usize, 0, templates.len - 1)];
        try result.appendSlice(template);

        // Occasionally inject random bytes
        if (random.boolean()) {
            const random_bytes = random.intRangeAtMost(usize, 1, 10);
            for (0..random_bytes) |_| {
                try result.append(random.int(u8));
            }
        }
    }

    return result.toOwnedSlice();
}

fn generate_random_filtered_query(allocator: std.mem.Allocator, random: std.Random) !FilteredQuery {
    const field_name = try generate_random_string(allocator, random, 1, 50);
    const search_value = try generate_random_string(allocator, random, 0, 100);

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

fn generate_random_traversal_query(allocator: std.mem.Allocator, random: std.Random) !TraversalQuery {
    _ = allocator;
    const directions = [_]TraversalDirection{ .incoming, .outgoing, .bidirectional };
    const algorithms = [_]TraversalAlgorithm{ .breadth_first, .depth_first };

    return TraversalQuery{
        .start_block_id = generate_random_block_id(random),
        .direction = directions[random.intRangeAtMost(usize, 0, directions.len - 1)],
        .algorithm = algorithms[random.intRangeAtMost(usize, 0, algorithms.len - 1)],
        .max_depth = random.intRangeAtMost(u32, 1, 10),
        .max_results = random.intRangeAtMost(u32, 1, 100),
        .edge_type_filter = null,
    };
}

// Tests

test "fuzz: basic functionality" {
    cortexdb.concurrency.init();

    var prng = std.Random.DefaultPrng.init(12345);
    const random = prng.random();

    // Test helper functions don't crash
    const block = try generate_random_block(std.testing.allocator, random);
    defer {
        std.testing.allocator.free(block.source_uri);
        std.testing.allocator.free(block.metadata_json);
        std.testing.allocator.free(block.content);
    }

    const query = try generate_random_filtered_query(std.testing.allocator, random);
    defer {
        std.testing.allocator.free(query.filter.metadata_contains.field);
        std.testing.allocator.free(query.filter.metadata_contains.substring);
    }

    _ = generate_random_block_id(random);
}

test "fuzz: deterministic data generation" {
    // Same seed should produce same data
    var prng1 = std.Random.DefaultPrng.init(54321);
    var prng2 = std.Random.DefaultPrng.init(54321);

    const id1 = generate_random_block_id(prng1.random());
    const id2 = generate_random_block_id(prng2.random());

    try std.testing.expectEqualSlices(u8, &id1.bytes, &id2.bytes);
}
