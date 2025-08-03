//! Common fuzzing utilities and infrastructure.
//!
//! Provides crash reporting, statistics tracking, data generation helpers,
//! and other shared functionality across all fuzzing modules.

const std = @import("std");
const builtin = @import("builtin");
const kausaldb = @import("kausaldb");

const BlockId = kausaldb.BlockId;
const ContextBlock = kausaldb.ContextBlock;

pub const CRASH_REPORT_DIR = "fuzz_reports";
pub const PROGRESS_INTERVAL_SEC = 60;
pub const GIT_CHECK_INTERVAL_SEC = 600; // 10 minutes

// Exit codes for coordination with shell script
pub const EXIT_GIT_UPDATE_NEEDED = 0; // Shell should rebuild and restart
pub const EXIT_NORMAL = 0;
pub const EXIT_ERROR = 1;

pub const FuzzResult = enum {
    success,
    expected_error,
    crash,
};

pub const FuzzStats = struct {
    iterations: u64 = 0,
    failures: u64 = 0,
    validation_errors: u64 = 0,
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

/// Setup crash reporting infrastructure
pub fn setup_crash_reporting(allocator: std.mem.Allocator) !void {
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

/// Generate detailed crash report for debugging
pub fn report_crash(
    allocator: std.mem.Allocator,
    target: []const u8,
    iteration: u64,
    seed: u64,
    err: anyerror,
) !void {
    const timestamp = std.time.timestamp();
    const report_filename = try std.fmt.allocPrint(
        allocator,
        "{s}/crash_{s}_iter{}_seed{}_{}.txt",
        .{ CRASH_REPORT_DIR, target, iteration, seed, timestamp },
    );
    defer allocator.free(report_filename);

    const report_file = std.fs.cwd().createFile(report_filename, .{}) catch |create_err| {
        std.debug.print("Failed to create crash report {s}: {}\n", .{ report_filename, create_err });
        return;
    };
    defer report_file.close();

    const report_content = try std.fmt.allocPrint(allocator,
        \\KausalDB Fuzzer Crash Report
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

/// Calculate unique hash for error deduplication
pub fn calculate_error_hash(err: anyerror) u64 {
    var hasher = std.hash.Wyhash.init(0);
    const error_name = @errorName(err);
    hasher.update(error_name);
    return hasher.final();
}

/// Check for git updates during continuous fuzzing
pub fn check_git_updates() bool {
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

// Data generation helpers

/// Generate random block ID for testing
pub fn generate_random_block_id(random: std.Random) BlockId {
    var bytes: [16]u8 = undefined;
    random.bytes(&bytes);
    return BlockId{ .bytes = bytes };
}

/// Generate random context block with potential edge cases
pub fn generate_random_block(allocator: std.mem.Allocator, random: std.Random) !ContextBlock {
    const id = generate_random_block_id(random);
    const version = random.int(u64);

    // Generate random strings with potential edge cases
    const source_uri = try generate_random_string(allocator, random, 1, 200);
    const metadata_json = try generate_random_json_like_string(allocator, random);
    const content = try generate_random_string(allocator, random, 1, 1000);

    return ContextBlock{
        .id = id,
        .version = version,
        .source_uri = source_uri,
        .metadata_json = metadata_json,
        .content = content,
    };
}

/// Generate random string with edge cases (nulls, control chars, unicode)
pub fn generate_random_string(
    allocator: std.mem.Allocator,
    random: std.Random,
    min_len: usize,
    max_len: usize,
) ![]u8 {
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

/// Generate JSON-like strings with intentional malformation
pub fn generate_random_json_like_string(allocator: std.mem.Allocator, random: std.Random) ![]u8 {
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

/// Generate malformed Zig source code for parser testing
pub fn generate_malformed_zig_source(allocator: std.mem.Allocator, random: std.Random) ![]u8 {
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

    // Pre-allocate with estimated capacity
    const line_count = random.intRangeAtMost(usize, 1, 20);
    const estimated_capacity = 100 * line_count; // tidy:ignore-perf - capacity managed explicitly

    var result = std.ArrayList(u8).init(allocator);
    try result.ensureTotalCapacity(estimated_capacity);
    defer result.deinit();

    for (0..line_count) |i| {
        if (i > 0) try result.append('\n');

        const template = templates[random.intRangeAtMost(usize, 0, templates.len - 1)];
        try result.appendSlice(template);

        // Occasionally inject random bytes
        if (random.boolean()) {
            const random_bytes = random.intRangeAtMost(usize, 1, 10);
            try result.ensureUnusedCapacity(random_bytes);
            for (0..random_bytes) |_| {
                result.appendAssumeCapacity(random.int(u8));
            }
        }
    }

    return result.toOwnedSlice();
}

// Tests

test "fuzz: basic data generation functionality" {
    const kausaldb_import = @import("kausaldb"); // tidy:ignore-arch - test requires kausaldb import for concurrency init
    kausaldb_import.concurrency.init();

    var prng = std.Random.DefaultPrng.init(12345);
    const random = prng.random();

    // Test helper functions don't crash
    const block = try generate_random_block(std.testing.allocator, random);
    defer {
        std.testing.allocator.free(block.source_uri);
        std.testing.allocator.free(block.metadata_json);
        std.testing.allocator.free(block.content);
    }

    _ = generate_random_block_id(random);

    // Test string generation
    const test_string = try generate_random_string(std.testing.allocator, random, 5, 10);
    defer std.testing.allocator.free(test_string);

    try std.testing.expect(test_string.len >= 5 and test_string.len <= 10);
}

test "fuzz: deterministic data generation" {
    // Same seed should produce same data
    var prng1 = std.Random.DefaultPrng.init(54321);
    var prng2 = std.Random.DefaultPrng.init(54321);

    const id1 = generate_random_block_id(prng1.random());
    const id2 = generate_random_block_id(prng2.random());

    try std.testing.expectEqualSlices(u8, &id1.bytes, &id2.bytes);
}
