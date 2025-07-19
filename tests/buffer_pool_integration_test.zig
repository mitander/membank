//! Buffer Pool Integration Tests
//!
//! Tests the buffer pool integration with storage engine to verify that
//! the memory corruption issue has been resolved. This test reproduces
//! the original failure pattern of ~15 tests causing HashMap corruption.

const std = @import("std");
const testing = std.testing;
const assert = std.debug.assert;

const simulation = @import("simulation");
const storage = @import("storage");
const context_block = @import("context_block");
// const buffer_pool = @import("buffer_pool"); // TODO Add buffer_pool to test imports in build.zig

const Simulation = simulation.Simulation;
const StorageEngine = storage.StorageEngine;
const ContextBlock = context_block.ContextBlock;
const BlockId = context_block.BlockId;

test "buffer pool integration: memory safety under repeated operations" {
    const allocator = testing.allocator;

    // Run multiple test cycles that previously caused HashMap corruption
    const test_cycles = 20; // Exceeds the ~15 cycle corruption threshold

    for (0..test_cycles) |cycle| {
        var sim = try Simulation.init(allocator, @intCast(0xBEEF0000 + cycle));
        defer sim.deinit();

        const node = try sim.add_node();
        const node_ptr = sim.find_node(node);
        const vfs_interface = node_ptr.filesystem_interface();

        const data_dir = try allocator.dupe(u8, "buffer_pool_test");
        defer allocator.free(data_dir);

        var engine = try StorageEngine.init(allocator, vfs_interface, data_dir);
        defer engine.deinit();

        try engine.initialize_storage();

        // Perform operations that stress both buffer pool and HashMap
        try perform_storage_operations(&engine, cycle);

        // Verify storage engine state remains consistent
        try verify_storage_integrity(&engine, cycle);
    }
}

test "buffer pool integration: allocation path verification" {
    const allocator = testing.allocator;

    var pool = try buffer_pool.BufferPool.init(allocator);
    defer pool.deinit();

    var pooled_allocator = buffer_pool.PooledAllocator.init(&pool, allocator);
    const alloc = (&pooled_allocator).allocator();

    // Test small allocations (should use pool)
    var small_allocations: [16][]u8 = undefined;
    for (&small_allocations, 0..) |*allocation, i| {
        allocation.* = try alloc.alloc(u8, 100 + i * 10);

        // Write unique pattern to verify no corruption
        for (allocation.*, 0..) |*byte, j| {
            byte.* = @intCast((i + j) % 256);
        }
    }

    // Test large allocations (should use heap fallback)
    var large_allocations: [8][]u8 = undefined;
    for (&large_allocations, 0..) |*allocation, i| {
        allocation.* = try alloc.alloc(u8, buffer_pool.MAX_BUFFER_SIZE + 1000 + i * 100);

        // Write unique pattern
        for (allocation.*, 0..) |*byte, j| {
            byte.* = @intCast((i + j + 128) % 256);
        }
    }

    // Verify all patterns are intact
    for (small_allocations, 0..) |allocation, i| {
        for (allocation, 0..) |byte, j| {
            try testing.expectEqual(@as(u8, @intCast((i + j) % 256)), byte);
        }
    }

    for (large_allocations, 0..) |allocation, i| {
        for (allocation, 0..) |byte, j| {
            try testing.expectEqual(@as(u8, @intCast((i + j + 128) % 256)), byte);
        }
    }

    // Free all allocations (mix of pool and heap)
    for (small_allocations) |allocation| {
        alloc.free(allocation);
    }

    for (large_allocations) |allocation| {
        alloc.free(allocation);
    }

    // Verify pool statistics make sense
    const stats = pool.statistics();
    try testing.expect(stats.allocations > 0);
    try testing.expect(stats.deallocations > 0);
}

test "buffer pool integration: HashMap stability stress test" {
    const allocator = testing.allocator;

    var sim = try Simulation.init(allocator, 0xDEADBEEF);
    defer sim.deinit();

    const node = try sim.add_node();
    const node_ptr = sim.find_node(node);
    const vfs_interface = node_ptr.filesystem_interface();

    const data_dir = try allocator.dupe(u8, "hashmap_stress_test");
    defer allocator.free(data_dir);

    var engine = try StorageEngine.init(allocator, vfs_interface, data_dir);
    defer engine.deinit();

    try engine.initialize_storage();

    // Stress test HashMap operations with buffer pool allocation patterns
    const operations = 100;
    var block_ids: [operations]BlockId = undefined;

    // Create many blocks with varying sizes
    for (0..operations) |i| {
        const block_id_str = try std.fmt.allocPrint(allocator, "{:0>31}{}", .{ 0, i });
        defer allocator.free(block_id_str);

        const block_id = try BlockId.from_hex(block_id_str);
        block_ids[i] = block_id;

        // Vary content size to exercise different allocation paths
        const content_size = if (i % 3 == 0)
            256 // Small - should use buffer pool
        else if (i % 3 == 1)
            4096 // Medium - should use buffer pool
        else
            1024 * 1024 + 1000; // Large - heap (MAX_BUFFER_SIZE)

        const content = try allocator.alloc(u8, content_size);
        defer allocator.free(content);

        // Fill with recognizable pattern
        for (content, 0..) |*byte, j| {
            byte.* = @intCast((i + j) % 256);
        }

        const metadata = std.StringHashMap([]const u8).init(allocator);
        const block = ContextBlock{
            .id = block_id,
            .version = 1,
            .source_uri = "test://buffer_pool_stress",
            .content = content,
            .metadata = metadata,
            .edges = std.ArrayList(context_block.GraphEdge).init(allocator),
        };

        try engine.put_block(block);
    }

    // Verify all blocks can be retrieved correctly (HashMap operations work)
    for (block_ids, 0..) |block_id, i| {
        const retrieved = try engine.get_block(block_id);
        try testing.expect(retrieved != null);

        const block = retrieved.?;

        // Verify content integrity
        const expected_size = if (i % 3 == 0)
            256
        else if (i % 3 == 1)
            4096
        else
            buffer_pool.MAX_BUFFER_SIZE + 1000;

        try testing.expectEqual(expected_size, block.content.len);

        // Verify pattern is intact
        for (block.content, 0..) |byte, j| {
            try testing.expectEqual(@as(u8, @intCast((i + j) % 256)), byte);
        }
    }

    // Perform updates to stress WAL + buffer pool interaction
    for (block_ids) |block_id| {
        var existing = (try engine.get_block(block_id)).?;

        // Modify content
        for (existing.content) |*byte| {
            byte.* = byte.* ^ 0xFF; // Flip all bits
        }

        existing.version += 1;
        try engine.put_block(existing);
    }

    // Verify updates worked correctly
    for (block_ids, 0..) |block_id, i| {
        const updated = try engine.get_block(block_id);
        try testing.expect(updated != null);

        const block = updated.?;
        try testing.expectEqual(@as(u32, 2), block.version);

        // Verify flipped pattern
        for (block.content, 0..) |byte, j| {
            const expected = @as(u8, @intCast((i + j) % 256)) ^ 0xFF;
            try testing.expectEqual(expected, byte);
        }
    }
}

test "buffer pool integration: WAL recovery with mixed allocations" {
    const allocator = testing.allocator;

    var sim = try Simulation.init(allocator, 0xCAFEBABE);
    defer sim.deinit();

    const node = try sim.add_node();
    const node_ptr = sim.find_node(node);
    const vfs_interface = node_ptr.filesystem_interface();

    const data_dir = try allocator.dupe(u8, "wal_buffer_pool_test");
    defer allocator.free(data_dir);

    // Create storage engine and add blocks
    {
        var engine = try StorageEngine.init(allocator, vfs_interface, data_dir);
        defer engine.deinit();

        try engine.initialize_storage();

        // Add blocks with different sizes to exercise both allocation paths
        const test_cases = [_]struct { size: usize, id_suffix: u8 }{
            .{ .size = 128, .id_suffix = 1 }, // Small - buffer pool
            .{ .size = 8192, .id_suffix = 2 }, // Medium - buffer pool
            .{ .size = 1024 * 1024 + 2000, .id_suffix = 3 }, // Large - heap
        };

        for (test_cases) |test_case| {
            const block_id_str = try std.fmt.allocPrint(
                allocator,
                "{:0>31}{}",
                .{ 0, test_case.id_suffix },
            );
            defer allocator.free(block_id_str);

            const content = try allocator.alloc(u8, test_case.size);
            defer allocator.free(content);

            // Fill with pattern based on size
            std.mem.set(u8, content, test_case.id_suffix);

            const metadata = std.StringHashMap([]const u8).init(allocator);
            const block = ContextBlock{
                .id = try BlockId.parse(block_id_str),
                .version = 1,
                .source_uri = "test://wal_recovery",
                .content = content,
                .metadata = metadata,
                .edges = std.ArrayList(context_block.GraphEdge).init(allocator),
            };

            try engine.put_block(block);
        }
    }

    // Create new storage engine and recover from WAL
    {
        var engine2 = try StorageEngine.init(allocator, vfs_interface, data_dir);
        defer engine2.deinit();

        try engine2.recover_from_wal();

        // Verify all blocks were recovered correctly
        const test_cases = [_]struct { size: usize, id_suffix: u8 }{
            .{ .size = 128, .id_suffix = 1 },
            .{ .size = 8192, .id_suffix = 2 },
            .{ .size = 1024 * 1024 + 2000, .id_suffix = 3 }, // Large - heap
        };

        for (test_cases) |test_case| {
            const block_id_str = try std.fmt.allocPrint(
                allocator,
                "{:0>31}{}",
                .{ 0, test_case.id_suffix },
            );
            defer allocator.free(block_id_str);

            const block_id = try BlockId.from_hex(block_id_str);
            const recovered = try engine2.get_block(block_id);

            try testing.expect(recovered != null);
            const block = recovered.?;

            try testing.expectEqual(test_case.size, block.content.len);
            try testing.expectEqual(@as(u32, 1), block.version);

            // Verify content pattern
            for (block.content) |byte| {
                try testing.expectEqual(test_case.id_suffix, byte);
            }
        }
    }
}

fn perform_storage_operations(engine: *StorageEngine, cycle: usize) !void {
    const allocator = testing.allocator;

    // Create blocks with mixed allocation patterns
    const block_count = 10;
    for (0..block_count) |i| {
        const block_id_str = try std.fmt.allocPrint(allocator, "{:0>30}{}{}", .{ 0, cycle, i });
        defer allocator.free(block_id_str);

        const block_id = try BlockId.from_hex(block_id_str);

        // Vary sizes to stress different allocation paths
        const size = switch (i % 4) {
            0 => 256, // Small - pool
            1 => 2048, // Medium - pool
            2 => 32768, // Large - pool
            3 => 1024 * 1024 + 500, // Huge - heap (MAX_BUFFER_SIZE)
            else => unreachable,
        };

        const content = try allocator.alloc(u8, size);
        defer allocator.free(content);

        // Create recognizable pattern
        for (content, 0..) |*byte, j| {
            byte.* = @intCast((cycle + i + j) % 256);
        }

        const metadata = std.StringHashMap([]const u8).init(allocator);
        const block = ContextBlock{
            .id = block_id,
            .version = 1,
            .source_uri = "test://memory_safety",
            .content = content,
            .metadata = metadata,
            .edges = std.ArrayList(context_block.GraphEdge).init(allocator),
        };

        try engine.put_block(block);
    }
}

fn verify_storage_integrity(engine: *StorageEngine, cycle: usize) !void {
    const allocator = testing.allocator;

    // Verify all blocks from this cycle can be retrieved correctly
    const block_count = 10;
    for (0..block_count) |i| {
        const block_id_str = try std.fmt.allocPrint(allocator, "{:0>30}{}{}", .{ 0, cycle, i });
        defer allocator.free(block_id_str);

        const block_id = try BlockId.from_hex(block_id_str);
        const retrieved = try engine.get_block(block_id);

        try testing.expect(retrieved != null);
        const block = retrieved.?;

        // Verify content pattern is intact
        for (block.content, 0..) |byte, j| {
            const expected = @as(u8, @intCast((cycle + i + j) % 256));
            try testing.expectEqual(expected, byte);
        }
    }
}
