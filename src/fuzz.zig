//! Fuzz testing for CortexDB components and data structures.

const std = @import("std");
const assert = std.debug.assert;

const FUZZ_ITERATIONS_DEFAULT = 10000;
const FUZZ_SEED_DEFAULT = 42;

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    const args = try std.process.argsAlloc(allocator);
    defer std.process.argsFree(allocator, args);

    if (args.len < 2) {
        try print_usage();
        return;
    }

    const target = args[1];
    const iterations = if (args.len > 2)
        try std.fmt.parseInt(u32, args[2], 10)
    else
        FUZZ_ITERATIONS_DEFAULT;

    const seed = if (args.len > 3)
        try std.fmt.parseInt(u64, args[3], 10)
    else
        FUZZ_SEED_DEFAULT;

    if (std.mem.eql(u8, target, "block_parser")) {
        try fuzz_block_parser(allocator, iterations, seed);
    } else if (std.mem.eql(u8, target, "query_engine")) {
        try fuzz_query_engine(allocator, iterations, seed);
    } else if (std.mem.eql(u8, target, "storage")) {
        try fuzz_storage(allocator, iterations, seed);
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
        \\CortexDB Fuzz Tester
        \\
        \\Usage:
        \\  fuzz <target> [iterations] [seed]
        \\
        \\Targets:
        \\  block_parser     Fuzz block parsing logic
        \\  query_engine     Fuzz query processing
        \\  storage          Fuzz storage operations
        \\  all              Fuzz all targets
        \\
        \\Examples:
        \\  fuzz block_parser 1000 42
        \\  fuzz all
        \\
    , .{});
}

fn fuzz_all_targets(allocator: std.mem.Allocator, iterations: u32, seed: u64) !void {
    std.debug.print("Fuzzing all targets with {} iterations, seed {}\n", .{ iterations, seed });

    try fuzz_block_parser(allocator, iterations, seed);
    try fuzz_query_engine(allocator, iterations, seed + 1);
    try fuzz_storage(allocator, iterations, seed + 2);
}

fn fuzz_block_parser(allocator: std.mem.Allocator, iterations: u32, seed: u64) !void {
    std.debug.print("Fuzzing block parser: {} iterations, seed {}\n", .{ iterations, seed });

    var prng = std.Random.DefaultPrng.init(seed);
    const random = prng.random();

    var failures: u32 = 0;

    for (0..iterations) |i| {
        const fuzz_data = try generate_block_data(allocator, random);
        defer allocator.free(fuzz_data);

        const result = fuzz_parse_block(fuzz_data);

        if (result == .crash) {
            failures += 1;
            std.debug.print("  Iteration {}: CRASH detected\n", .{i});
        }

        if (i > 0 and i % 1000 == 0) {
            std.debug.print("  Progress: {}/{}\n", .{ i, iterations });
        }
    }

    std.debug.print("  Completed: {} failures out of {} iterations\n", .{ failures, iterations });
}

fn fuzz_query_engine(allocator: std.mem.Allocator, iterations: u32, seed: u64) !void {
    std.debug.print("Fuzzing query engine: {} iterations, seed {}\n", .{ iterations, seed });

    var prng = std.Random.DefaultPrng.init(seed);
    const random = prng.random();

    var failures: u32 = 0;

    for (0..iterations) |i| {
        const query_data = try generate_query_data(allocator, random);
        defer allocator.free(query_data);

        const result = fuzz_process_query(query_data);

        if (result == .crash) {
            failures += 1;
            std.debug.print("  Iteration {}: CRASH detected\n", .{i});
        }

        if (i > 0 and i % 1000 == 0) {
            std.debug.print("  Progress: {}/{}\n", .{ i, iterations });
        }
    }

    std.debug.print("  Completed: {} failures out of {} iterations\n", .{ failures, iterations });
}

fn fuzz_storage(allocator: std.mem.Allocator, iterations: u32, seed: u64) !void {
    _ = allocator;
    std.debug.print("Fuzzing storage: {} iterations, seed {}\n", .{ iterations, seed });

    var prng = std.Random.DefaultPrng.init(seed);
    const random = prng.random();

    var failures: u32 = 0;

    for (0..iterations) |i| {
        const storage_op = generate_storage_operation(random);

        const result = fuzz_storage_operation(storage_op);

        if (result == .crash) {
            failures += 1;
            std.debug.print("  Iteration {}: CRASH detected\n", .{i});
        }

        if (i > 0 and i % 1000 == 0) {
            std.debug.print("  Progress: {}/{}\n", .{ i, iterations });
        }
    }

    std.debug.print("  Completed: {} failures out of {} iterations\n", .{ failures, iterations });
}

fn generate_block_data(allocator: std.mem.Allocator, random: std.Random) ![]u8 {
    const size = random.intRangeAtMost(usize, 0, 1024);
    const data = try allocator.alloc(u8, size);

    random.bytes(data);

    // Occasionally inject valid-looking headers
    if (size >= 8 and random.boolean()) {
        std.mem.writeInt(u32, data[0..4], 0xDEADBEEF, .little);
        std.mem.writeInt(u32, data[4..8], @intCast(size - 8), .little);
    }

    return data;
}

fn generate_query_data(allocator: std.mem.Allocator, random: std.Random) ![]u8 {
    const size = random.intRangeAtMost(usize, 0, 256);
    const data = try allocator.alloc(u8, size);

    // Generate mostly ASCII with some random bytes
    for (data) |*byte| {
        if (random.boolean()) {
            byte.* = random.intRangeAtMost(u8, 32, 126);
        } else {
            byte.* = random.int(u8);
        }
    }

    return data;
}

const StorageOperation = enum {
    read,
    write,
    delete,
    corrupt,
};

fn generate_storage_operation(random: std.Random) StorageOperation {
    const operations = [_]StorageOperation{ .read, .write, .delete, .corrupt };
    return operations[random.intRangeAtMost(usize, 0, operations.len - 1)];
}

const FuzzResult = enum {
    success,
    failure,
    crash,
};

/// Fuzz block parsing with generated data.
fn fuzz_parse_block(data: []const u8) FuzzResult {
    // Placeholder block parsing logic
    if (data.len == 0) return .failure;

    // Simulate parsing that might crash on malformed input
    if (data.len >= 4) {
        const magic = std.mem.readInt(u32, data[0..4], .little);
        if (magic == 0xDEADBEEF and data.len >= 8) {
            const size = std.mem.readInt(u32, data[4..8], .little);
            if (size > data.len - 8) {
                // This would cause a crash in real parsing
                return .crash;
            }
        }
    }

    return .success;
}

/// Fuzz query processing with generated data.
fn fuzz_process_query(query: []const u8) FuzzResult {
    // Placeholder query processing logic
    if (query.len == 0) return .success; // Empty query is valid

    // Look for patterns that might cause issues
    var null_count: u32 = 0;
    for (query) |byte| {
        if (byte == 0) null_count += 1;
    }

    // Too many null bytes might indicate corruption
    if (null_count > query.len / 2) {
        return .crash;
    }

    return .success;
}

/// Fuzz storage operations.
fn fuzz_storage_operation(op: StorageOperation) FuzzResult {
    // Placeholder storage operation logic
    switch (op) {
        .read => {
            // Reading might fail but shouldn't crash
            return .success;
        },
        .write => {
            // Writing might fail due to disk full, etc.
            return .success;
        },
        .delete => {
            // Deletion might fail if file doesn't exist
            return .success;
        },
        .corrupt => {
            // Corruption simulation - this might expose bugs
            return .crash;
        },
    }
}

test "fuzz: basic functionality" {
    const allocator = std.testing.allocator;

    // Test data generation
    var prng = std.Random.DefaultPrng.init(12345);
    const random = prng.random();

    const block_data = try generate_block_data(allocator, random);
    defer allocator.free(block_data);

    const query_data = try generate_query_data(allocator, random);
    defer allocator.free(query_data);

    // Test fuzz functions don't crash
    _ = fuzz_parse_block(block_data);
    _ = fuzz_process_query(query_data);
    _ = fuzz_storage_operation(.read);
}

test "fuzz: deterministic behavior" {
    const allocator = std.testing.allocator;

    // Same seed should produce same data
    var prng1 = std.Random.DefaultPrng.init(54321);
    var prng2 = std.Random.DefaultPrng.init(54321);

    const data1 = try generate_block_data(allocator, prng1.random());
    defer allocator.free(data1);

    const data2 = try generate_block_data(allocator, prng2.random());
    defer allocator.free(data2);

    try std.testing.expectEqualSlices(u8, data1, data2);
}
