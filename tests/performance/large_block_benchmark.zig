//! Focused benchmark for large block optimizations.
//!
//! Simple benchmark to validate that our streaming serialization and memory
//! pool optimizations provide measurable performance improvements for large blocks.

const std = @import("std");
const testing = std.testing;
const kausaldb = @import("kausaldb");

const types = kausaldb.types;
const TestData = kausaldb.test_harness.TestData;
const SimulationHarness = kausaldb.test_harness.SimulationHarness;
const ContextBlock = types.ContextBlock;

/// Create a test block with specified size
fn create_test_block(allocator: std.mem.Allocator, size: usize) !ContextBlock {
    const content = try allocator.alloc(u8, size);

    // Fill with a pattern for validation
    for (content, 0..) |*byte, i| {
        byte.* = @intCast(i % 256);
    }

    return ContextBlock{
        .id = types.BlockId.generate(),
        .version = 1,
        .source_uri = "test://benchmark.zig",
        .metadata_json = "{\"benchmark\":true}",
        .content = content,
    };
}

test "large block serialization performance baseline" {
    const allocator = testing.allocator;

    std.debug.print("\n=== Large Block Serialization Performance Baseline ===\n", .{});

    // Test with different block sizes
    const sizes = [_]usize{ 1024 * 1024, 2 * 1024 * 1024, 5 * 1024 * 1024 }; // 1MB, 2MB, 5MB

    for (sizes) |size| {
        const test_block = try create_test_block(allocator, size);
        defer allocator.free(test_block.content);

        const size_mb = @as(f64, @floatFromInt(size)) / (1024.0 * 1024.0);

        // Serialization benchmark
        const iterations = 5;
        var total_ns: u64 = 0;

        for (0..iterations) |_| {
            const start = std.time.nanoTimestamp();

            const serialized_size = test_block.serialized_size();
            const buffer = try allocator.alloc(u8, serialized_size);
            defer allocator.free(buffer);

            const bytes_written = try test_block.serialize(buffer);
            try testing.expectEqual(serialized_size, bytes_written);

            const end = std.time.nanoTimestamp();
            total_ns += @intCast(end - start);
        }

        const avg_us = @as(f64, @floatFromInt(total_ns)) / (1000.0 * iterations);
        const throughput_mbps = size_mb / (avg_us / 1_000_000.0);

        std.debug.print("{d:.1}MB block: {d:.1}µs serialization, {d:.1} MB/s throughput\n", .{ size_mb, avg_us, throughput_mbps });
    }
}

test "large block storage engine performance" {
    const allocator = testing.allocator;

    std.debug.print("\n=== Large Block Storage Engine Performance ===\n", .{});

    // Test with 1MB and 2MB blocks
    const sizes = [_]usize{ 1024 * 1024, 2 * 1024 * 1024 }; // 1MB, 2MB

    for (sizes) |size| {
        const test_block = try create_test_block(allocator, size);
        defer allocator.free(test_block.content);

        const size_mb = @as(f64, @floatFromInt(size)) / (1024.0 * 1024.0);

        // Storage engine benchmark
        const iterations = 3; // Fewer iterations for storage tests
        var total_write_ns: u64 = 0;
        var total_read_ns: u64 = 0;

        for (0..iterations) |i| {
            // Create unique DB name for each iteration
            const db_name = try std.fmt.allocPrint(allocator, "large_perf_{}", .{i});
            defer allocator.free(db_name);

            var harness = try SimulationHarness.init_and_startup(allocator, 0xBEEF1234 + @as(u64, i), db_name);
            defer harness.deinit();

            // Disable immediate sync for performance testing
            // WARNING: This reduces durability guarantees but allows measuring optimal performance
            harness.storage_engine.configure_wal_immediate_sync(false);

            // Profile WAL entry creation separately
            const wal_start = std.time.nanoTimestamp();
            const wal_entry = try kausaldb.storage.WALEntry.create_put_block(allocator, test_block);
            const wal_end = std.time.nanoTimestamp();
            defer wal_entry.deinit(allocator);

            // Measure full write time
            const write_start = std.time.nanoTimestamp();
            try harness.storage_engine.put_block(test_block);
            const write_end = std.time.nanoTimestamp();

            // Calculate breakdown
            const wal_create_us = @as(f64, @floatFromInt(wal_end - wal_start)) / 1000.0;
            const total_write_us = @as(f64, @floatFromInt(write_end - write_start)) / 1000.0;
            const non_wal_us = total_write_us - wal_create_us;

            if (i == 0) { // Only print for first iteration to avoid spam
                std.debug.print("  WAL create: {d:.1}µs, Storage pipeline: {d:.1}µs, Total: {d:.1}µs\n", .{ wal_create_us, non_wal_us, total_write_us });
            }

            // Measure read time
            const read_start = std.time.nanoTimestamp();
            const retrieved = try harness.storage_engine.find_block(test_block.id, .query_engine);
            const read_end = std.time.nanoTimestamp();

            try testing.expect(retrieved != null);
            try testing.expectEqual(size, retrieved.?.extract().content.len);

            total_write_ns += @intCast(write_end - write_start);
            total_read_ns += @intCast(read_end - read_start);
        }

        const avg_write_us = @as(f64, @floatFromInt(total_write_ns)) / (1000.0 * iterations);
        const avg_read_us = @as(f64, @floatFromInt(total_read_ns)) / (1000.0 * iterations);
        const write_throughput_mbps = size_mb / (avg_write_us / 1_000_000.0);

        std.debug.print("{d:.1}MB block: Write={d:.1}µs, Read={d:.1}µs, Throughput={d:.1}MB/s\n", .{ size_mb, avg_write_us, avg_read_us, write_throughput_mbps });

        // Performance target validation (from TODO.md: <100µs for 1MB+ blocks)
        const target_us = 100.0;
        if (avg_write_us <= target_us) {
            std.debug.print("PASS: Write time within target\n", .{});
        } else {
            std.debug.print("NOTICE: Write time {d:.1}µs exceeds target {d:.1}µs\n", .{ avg_write_us, target_us });
        }
    }
}
