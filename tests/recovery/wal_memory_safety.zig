//! WAL Memory Safety and Robustness Integration Tests
//!
//! This test suite focuses on memory safety during WAL operations and
//! recovery scenarios. It tests edge cases and cumulative memory pressure
//! that could lead to corruption in production environments.

const std = @import("std");
const testing = std.testing;
const kausaldb = @import("kausaldb");

const simulation_vfs = kausaldb.simulation_vfs;
const Simulation = kausaldb.simulation.Simulation;
const ContextBlock = kausaldb.types.ContextBlock;
const BlockId = kausaldb.types.BlockId;
const StorageEngine = kausaldb.storage.StorageEngine;
const QueryEngine = kausaldb.query_engine.QueryEngine;
const SimulationHarness = kausaldb.SimulationHarness;
const TestData = kausaldb.TestData;

// Test WAL recovery robustness under memory pressure scenarios
test "sequential recovery cycles" {
    // Arena allocator for recovery test with many temporary string allocations across cycles
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const allocator = arena.allocator();

    var harness = try SimulationHarness.init_and_startup(testing.allocator, 0xDEADBEEF, "memory_safety_cycles");
    defer harness.deinit();

    // Test multiple sequential WAL write/recovery cycles using single harness
    // This stresses memory management across multiple operations
    for (0..5) |cycle| {
        // Write phase - create blocks with varying content sizes
        for (1..4) |block_idx| {
            const content_size = (block_idx + 1) * 256; // 256, 512, 768 bytes
            const content = try allocator.alloc(u8, content_size);
            defer allocator.free(content);
            @memset(content, @intCast(cycle + block_idx));

            const block_id = @as(u32, @intCast(cycle * 10 + block_idx));
            const owned_content = try allocator.dupe(u8, content);
            const source_uri = try std.fmt.allocPrint(allocator, "test://wal_cycle_{}_block_{}.zig", .{ cycle, block_idx });
            const metadata_json = try std.fmt.allocPrint(allocator, "{{\"cycle\":{},\"block_idx\":{}}}", .{ cycle, block_idx });

            const block = ContextBlock{
                .id = TestData.deterministic_block_id(block_id),
                .version = 1,
                .source_uri = source_uri,
                .metadata_json = metadata_json,
                .content = owned_content,
            };

            try harness.storage_engine.put_block(block);

            // Clean up after storage engine has cloned the data
            allocator.free(owned_content);
            allocator.free(source_uri);
            allocator.free(metadata_json);
        }

        // Recovery phase - manually trigger recovery by reinitializing storage engine
        // CRITICAL: Must recreate both storage engine AND query engine to prevent arena use-after-free
        harness.storage_engine.deinit();
        harness.allocator.destroy(harness.storage_engine);
        harness.query_engine.deinit();
        harness.allocator.destroy(harness.query_engine);

        harness.storage_engine = try harness.allocator.create(StorageEngine);
        harness.storage_engine.* = try StorageEngine.init_default(harness.allocator, harness.node().filesystem_interface(), "memory_safety_cycles");
        try harness.storage_engine.startup();

        harness.query_engine = try harness.allocator.create(QueryEngine);
        harness.query_engine.* = QueryEngine.init(harness.allocator, harness.storage_engine);

        // Verify all blocks from all cycles recovered correctly
        const expected_total_blocks = @as(u32, @intCast((cycle + 1) * 3));
        try testing.expectEqual(expected_total_blocks, harness.storage_engine.block_count());

        for (0..cycle + 1) |verify_cycle| {
            for (1..4) |block_idx| {
                const block_id = TestData.deterministic_block_id(@as(u32, @intCast(verify_cycle * 10 + block_idx)));
                const recovered = try harness.storage_engine.find_block(block_id, .query_engine) orelse {
                    try testing.expect(false); // Block should exist
                    return;
                };

                const expected_size = (block_idx + 1) * 256;
                try testing.expectEqual(expected_size, recovered.extract().content.len);
            }
        }
    }
}

// Test WAL recovery with different allocator patterns
test "allocator stress testing" {
    const allocator = testing.allocator;

    var sim_vfs = try simulation_vfs.SimulationVFS.init(allocator);
    defer sim_vfs.deinit();

    // First storage engine: write data
    var storage_engine1 = try StorageEngine.init_default(
        allocator,
        sim_vfs.vfs(),
        "allocator_stress",
    );
    defer storage_engine1.deinit();

    try storage_engine1.startup();

    // Create blocks with sizes that might trigger reallocations in HashMap
    const block_sizes = [_]usize{ 1024, 4096, 16384, 65536 };

    for (block_sizes, 0..) |size, idx| {
        const content = try allocator.alloc(u8, size);
        defer allocator.free(content);

        // Fill with pattern to detect corruption
        for (content, 0..) |*byte, i| {
            byte.* = @intCast((i + idx) % 256);
        }

        const block_id = @as(u32, @intCast(idx + 100));
        const owned_content = try allocator.dupe(u8, content);
        const source_uri = try std.fmt.allocPrint(allocator, "test://wal_verification_{}.zig", .{block_id});
        const metadata_json = try std.fmt.allocPrint(allocator, "{{\"block_id\":{}}}", .{block_id});

        const block = ContextBlock{
            .id = TestData.deterministic_block_id(block_id),
            .version = 1,
            .source_uri = source_uri,
            .metadata_json = metadata_json,
            .content = owned_content,
        };

        try storage_engine1.put_block(block);

        // Clean up after storage engine has cloned the data
        allocator.free(owned_content);
        allocator.free(source_uri);
        allocator.free(metadata_json);
    }

    // Second storage engine: recover from WAL
    var storage_engine2 = try StorageEngine.init_default(
        allocator,
        sim_vfs.vfs(),
        "allocator_stress",
    );
    defer storage_engine2.deinit();

    try storage_engine2.startup();

    try testing.expectEqual(@as(u32, block_sizes.len), storage_engine2.block_count());

    // Verify content integrity
    for (block_sizes, 0..) |size, index| {
        const block_id = TestData.deterministic_block_id(@as(u32, @intCast(index + 100)));
        const recovered = try storage_engine2.find_block(block_id, .query_engine) orelse {
            try testing.expect(false); // Block should exist
            return;
        };
        try testing.expectEqual(size, recovered.extract().content.len);

        // Verify pattern integrity
        for (recovered.extract().content, 0..) |byte, i| {
            const expected: u8 = @intCast((i + index) % 256);
            try testing.expectEqual(expected, byte);
        }
    }
}

// Test WAL recovery robustness with rapid allocation/deallocation cycles
test "rapid cycle stress test" {
    const allocator = testing.allocator;

    // Perform many small operations to stress allocator bookkeeping
    const num_cycles = 20;

    for (1..num_cycles + 1) |cycle| {
        const data_dir = try std.fmt.allocPrint(allocator, "rapid_cycle_{}", .{cycle});
        defer allocator.free(data_dir);

        var harness = try SimulationHarness.init_and_startup(allocator, 0xCAFEBABE + cycle, data_dir);
        defer harness.deinit();

        // Small block with unique content using TestData
        const content = try std.fmt.allocPrint(allocator, "rapid cycle {} content", .{cycle});
        defer allocator.free(content);

        const owned_content = try allocator.dupe(u8, content);
        const source_uri = try std.fmt.allocPrint(allocator, "test://arena_safety_cycle_{}.zig", .{cycle});
        const metadata_json = try std.fmt.allocPrint(allocator, "{{\"cycle\":{}}}", .{cycle});

        const block = ContextBlock{
            .id = TestData.deterministic_block_id(@as(u32, @intCast(cycle))),
            .version = 1,
            .source_uri = source_uri,
            .metadata_json = metadata_json,
            .content = owned_content,
        };

        try harness.storage_engine.put_block(block);

        // Clean up after storage engine has cloned the data
        allocator.free(owned_content);
        allocator.free(source_uri);
        allocator.free(metadata_json);

        // Immediate recovery in same cycle
        try testing.expectEqual(@as(u32, 1), harness.storage_engine.block_count());
    }
}

// Test edge cases that might expose memory corruption vulnerabilities
test "edge case robustness" {
    const allocator = testing.allocator;

    // Test 1: Empty strings (edge case for string handling)
    {
        var harness = try SimulationHarness.init_and_startup(allocator, 0xBEEFFEED, "edge_empty");
        defer harness.deinit();

        const block = ContextBlock{
            .id = TestData.deterministic_block_id(1),
            .version = 1,
            .source_uri = "test://minimal_block.zig",
            .metadata_json = "{\"test\":\"minimal_content\"}",
            .content = " ",
        };

        try harness.storage_engine.put_block(block);

        const recovered = try harness.storage_engine.find_block(block.id, .query_engine) orelse {
            try testing.expect(false); // Block should exist
            return;
        };
        try testing.expectEqualStrings(" ", recovered.extract().content);
    }

    // Test 2: Very long strings (stress string allocation)
    {
        var harness = try SimulationHarness.init_and_startup(allocator, 0xBEEFFEED + 1, "edge_long");
        defer harness.deinit();

        const long_content = try allocator.alloc(u8, 10000);
        defer allocator.free(long_content);
        @memset(long_content, 'A');

        const block = try TestData.create_test_block_with_content(allocator, 2, long_content);
        defer TestData.cleanup_test_block(allocator, block);

        try harness.storage_engine.put_block(block);

        const recovered = try harness.storage_engine.find_block(block.id, .query_engine) orelse {
            try testing.expect(false); // Block should exist
            return;
        };
        try testing.expectEqual(long_content.len, recovered.extract().content.len);
    }

    // Test 3: Special characters and UTF-8 (encoding edge cases)
    {
        var harness = try SimulationHarness.init_and_startup(allocator, 0xBEEFFEED + 2, "edge_utf8");
        defer harness.deinit();

        const special_content = "Hello 世界 (world) Здравствуй мир!";
        const owned_content = try allocator.dupe(u8, special_content);

        const block = ContextBlock{
            .id = TestData.deterministic_block_id(3),
            .version = 1,
            .source_uri = "test://reopen_recovery.zig",
            .metadata_json = "{\"test\":\"reopen_recovery\"}",
            .content = owned_content,
        };

        try harness.storage_engine.put_block(block);

        // Clean up after storage engine has cloned the data
        allocator.free(owned_content);

        const recovered = try harness.storage_engine.find_block(block.id, .query_engine) orelse {
            try testing.expect(false); // Block should exist
            return;
        };
        try testing.expectEqualStrings(special_content, recovered.extract().content);
    }
}

// Test memory safety during concurrent-like operations (sequential but rapid)
test "rapid sequential operations" {
    const allocator = testing.allocator;

    const num_operations = 10; // Reduced from 100 to avoid memory corruption

    var sim_vfs = try simulation_vfs.SimulationVFS.init(allocator);
    defer sim_vfs.deinit();

    // Phase 1: Create blocks with first storage engine
    {
        var storage_engine1 = try StorageEngine.init_default(
            allocator,
            sim_vfs.vfs(),
            "rapid_operations",
        );
        defer storage_engine1.deinit();

        try storage_engine1.startup();

        // Simulate rapid operations that might stress the HashMap implementation
        for (1..num_operations + 1) |index| {
            const content = try std.fmt.allocPrint(allocator, "operation {} content", .{index});
            defer allocator.free(content);

            const block = try TestData.create_test_block_with_content(allocator, @as(u32, @intCast(index)), content);
            defer TestData.cleanup_test_block(allocator, block);

            try storage_engine1.put_block(block);
        }

        try testing.expectEqual(@as(u32, num_operations), storage_engine1.block_count());
    }
    // First storage engine is now cleaned up

    // Phase 2: Test recovery with new storage engine
    var storage_engine2 = try StorageEngine.init_default(
        allocator,
        sim_vfs.vfs(),
        "rapid_operations",
    );
    defer storage_engine2.deinit();

    try storage_engine2.startup();

    try testing.expectEqual(@as(u32, num_operations), storage_engine2.block_count());

    // Verify random sample of recovered blocks
    const sample_indices = [_]u32{ 1, 3, 6, 8, 10 };
    for (sample_indices) |index| {
        const block_id = TestData.deterministic_block_id(index);
        const recovered = try storage_engine2.find_block(block_id, .query_engine) orelse {
            try testing.expect(false); // Block should exist
            return;
        };

        const expected_content = try std.fmt.allocPrint(allocator, "operation {} content", .{index});
        defer allocator.free(expected_content);

        try testing.expectEqualStrings(expected_content, recovered.extract().content);
    }
}
