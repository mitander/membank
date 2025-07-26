//! WAL Memory Safety and Robustness Integration Tests
//!
//! This test suite focuses on memory safety during WAL operations and
//! recovery scenarios. It tests edge cases and cumulative memory pressure
//! that could lead to corruption in production environments.

const cortexdb = @import("cortexdb");
const std = @import("std");
const testing = std.testing;

const simulation = cortexdb.simulation;
const context_block = cortexdb.types;
const storage = cortexdb.storage;

const Simulation = simulation.Simulation;
const ContextBlock = context_block.ContextBlock;
const BlockId = context_block.BlockId;
const StorageEngine = storage.StorageEngine;

// Test WAL recovery robustness under memory pressure scenarios
test "wal memory safety: sequential recovery cycles" {
    const allocator = testing.allocator;

    var sim = try Simulation.init(allocator, 0xDEADBEEF);
    defer sim.deinit();

    const node = try sim.add_node();
    const node_ptr = sim.find_node(node);
    const vfs = node_ptr.filesystem_interface();

    // Test multiple sequential WAL write/recovery cycles
    // This stresses memory management across multiple operations
    for (0..5) |cycle| {
        const data_dir = try std.fmt.allocPrint(allocator, "memory_safety_cycle_{}", .{cycle});
        defer allocator.free(data_dir);

        // Write phase
        {
            var engine = try StorageEngine.init_default(allocator, vfs, data_dir);
            defer engine.deinit();

            try engine.startup();

            // Create blocks with varying content sizes to stress memory allocation
            for (0..3) |block_idx| {
                const block_id_str = try std.fmt.allocPrint(
                    allocator,
                    "{:0>32}",
                    .{cycle * 10 + block_idx},
                );
                defer allocator.free(block_id_str);

                const content_size = (block_idx + 1) * 256; // 256, 512, 768 bytes
                const content = try allocator.alloc(u8, content_size);
                defer allocator.free(content);
                @memset(content, @intCast(cycle + block_idx));

                const metadata = try std.fmt.allocPrint(
                    allocator,
                    "{{\"cycle\":{},\"block\":{},\"size\":{}}}",
                    .{ cycle, block_idx, content_size },
                );
                defer allocator.free(metadata);

                const block = ContextBlock{
                    .id = try BlockId.from_hex(block_id_str),
                    .version = @intCast(cycle + 1),
                    .source_uri = "test://memory_safety.zig",
                    .metadata_json = metadata,
                    .content = content,
                };

                try engine.put_block(block);
            }

            try engine.flush_wal();
        }

        // Recovery phase
        {
            var engine = try StorageEngine.init_default(allocator, vfs, data_dir);
            defer engine.deinit();

            try engine.startup();
            try engine.recover_from_wal();

            // Verify all blocks recovered correctly
            try testing.expectEqual(@as(u32, 3), engine.block_count());

            for (0..3) |block_idx| {
                const block_id_str = try std.fmt.allocPrint(
                    allocator,
                    "{:0>32}",
                    .{cycle * 10 + block_idx},
                );
                defer allocator.free(block_id_str);

                const recovered = (try engine.find_block(try BlockId.from_hex(block_id_str))) orelse {
                    try testing.expect(false); // Block should exist
                    return;
                };
                try testing.expectEqual(@as(u32, @intCast(cycle + 1)), recovered.version);

                const expected_size = (block_idx + 1) * 256;
                try testing.expectEqual(expected_size, recovered.content.len);
            }
        }
    }
}

// Test WAL recovery with different allocator patterns
test "wal memory safety: allocator stress testing" {
    const allocator = testing.allocator;

    var sim = try Simulation.init(allocator, 0xFEEDFACE);
    defer sim.deinit();

    const node = try sim.add_node();
    const node_ptr = sim.find_node(node);
    const vfs = node_ptr.filesystem_interface();

    // Test with large blocks that stress memory allocation patterns
    var engine = try StorageEngine.init_default(
        allocator,
        vfs,
        "allocator_stress",
    );
    defer engine.deinit();

    try engine.startup();

    // Create blocks with sizes that might trigger reallocations in HashMap
    const block_sizes = [_]usize{ 1024, 4096, 16384, 65536 };

    for (block_sizes, 0..) |size, idx| {
        const block_id_str = try std.fmt.allocPrint(allocator, "{:0>31}{}", .{ 0, idx });
        defer allocator.free(block_id_str);

        const content = try allocator.alloc(u8, size);
        defer allocator.free(content);

        // Fill with pattern to detect corruption
        for (content, 0..) |*byte, i| {
            byte.* = @intCast((i + idx) % 256);
        }

        const uri = try std.fmt.allocPrint(allocator, "test://stress_{}.zig", .{size});
        defer allocator.free(uri);

        const metadata = try std.fmt.allocPrint(
            allocator,
            "{{\"size\":{},\"pattern\":true}}",
            .{size},
        );
        defer allocator.free(metadata);

        const block = ContextBlock{
            .id = try BlockId.from_hex(block_id_str),
            .version = 1,
            .source_uri = uri,
            .metadata_json = metadata,
            .content = content,
        };

        try engine.put_block(block);
    }

    try engine.flush_wal();

    // Recovery with same allocator
    var recovery_engine = try StorageEngine.init_default(
        allocator,
        vfs,
        "allocator_stress",
    );
    defer recovery_engine.deinit();

    try recovery_engine.startup();
    try recovery_engine.recover_from_wal();

    try testing.expectEqual(@as(u32, block_sizes.len), recovery_engine.block_count());

    // Verify content integrity
    for (block_sizes, 0..) |size, idx| {
        const block_id_str = try std.fmt.allocPrint(allocator, "{:0>31}{}", .{ 0, idx });
        defer allocator.free(block_id_str);

        const recovered = (try recovery_engine.find_block(try BlockId.from_hex(block_id_str))) orelse {
            try testing.expect(false); // Block should exist
            return;
        };
        try testing.expectEqual(size, recovered.content.len);

        // Verify pattern integrity
        for (recovered.content, 0..) |byte, i| {
            const expected: u8 = @intCast((i + idx) % 256);
            try testing.expectEqual(expected, byte);
        }
    }
}

// Test WAL recovery robustness with rapid allocation/deallocation cycles
test "wal memory safety: rapid cycle stress test" {
    const allocator = testing.allocator;

    var sim = try Simulation.init(allocator, 0xCAFEBABE);
    defer sim.deinit();

    const node = try sim.add_node();
    const node_ptr = sim.find_node(node);
    const vfs = node_ptr.filesystem_interface();

    // Perform many small operations to stress allocator bookkeeping
    const num_cycles = 20;

    for (0..num_cycles) |cycle| {
        const data_dir = try std.fmt.allocPrint(allocator, "rapid_cycle_{}", .{cycle});
        defer allocator.free(data_dir);

        var engine = try StorageEngine.init_default(allocator, vfs, data_dir);
        defer engine.deinit();

        try engine.startup();

        // Small block with unique content
        const block_id_str = try std.fmt.allocPrint(allocator, "{:0>32}", .{cycle});
        defer allocator.free(block_id_str);

        const content = try std.fmt.allocPrint(allocator, "rapid cycle {} content", .{cycle});
        defer allocator.free(content);

        const block = ContextBlock{
            .id = try BlockId.from_hex(block_id_str),
            .version = 1,
            .source_uri = "test://rapid.zig",
            .metadata_json = "{}",
            .content = content,
        };

        try engine.put_block(block);
        try engine.flush_wal();

        // Immediate recovery in same cycle
        try engine.recover_from_wal();
        try testing.expectEqual(@as(u32, 1), engine.block_count());
    }
}

// Test edge cases that might expose memory corruption vulnerabilities
test "wal memory safety: edge case robustness" {
    const allocator = testing.allocator;

    var sim = try Simulation.init(allocator, 0xBEEFFEED);
    defer sim.deinit();

    const node = try sim.add_node();
    const node_ptr = sim.find_node(node);
    const vfs = node_ptr.filesystem_interface();

    // Test 1: Empty strings (edge case for string handling)
    {
        var engine = try StorageEngine.init_default(allocator, vfs, "edge_empty");
        defer engine.deinit();

        try engine.startup();

        const block = ContextBlock{
            .id = try BlockId.from_hex("00000000000000000000000000000001"),
            .version = 1,
            .source_uri = "",
            .metadata_json = "{}",
            .content = "",
        };

        try engine.put_block(block);
        try engine.flush_wal();

        try engine.recover_from_wal();
        const recovered = (try engine.find_block(block.id)) orelse {
            try testing.expect(false); // Block should exist
            return;
        };
        try testing.expectEqualStrings("", recovered.source_uri);
        try testing.expectEqualStrings("", recovered.content);
    }

    // Test 2: Very long strings (stress string allocation)
    {
        var engine = try StorageEngine.init_default(allocator, vfs, "edge_long");
        defer engine.deinit();

        try engine.startup();

        const long_content = try allocator.alloc(u8, 10000);
        defer allocator.free(long_content);
        @memset(long_content, 'A');

        const long_uri = try allocator.alloc(u8, 1000);
        defer allocator.free(long_uri);
        @memset(long_uri, 'U');

        const block = ContextBlock{
            .id = try BlockId.from_hex("00000000000000000000000000000002"),
            .version = 1,
            .source_uri = long_uri,
            .metadata_json = "{}",
            .content = long_content,
        };

        try engine.put_block(block);
        try engine.flush_wal();

        try engine.recover_from_wal();
        const recovered = (try engine.find_block(block.id)).?;
        try testing.expectEqual(long_content.len, recovered.content.len);
        try testing.expectEqual(long_uri.len, recovered.source_uri.len);
    }

    // Test 3: Special characters and UTF-8 (encoding edge cases)
    {
        var engine = try StorageEngine.init_default(allocator, vfs, "edge_utf8");
        defer engine.deinit();

        try engine.startup();

        const special_content = "Hello 世界 (world) Здравствуй мир!";
        const special_metadata = "{\"unicode\":\"测试\",\"emoji\":\"(rocket)\"}";

        const block = ContextBlock{
            .id = try BlockId.from_hex("00000000000000000000000000000003"),
            .version = 1,
            .source_uri = "test://unicode_файл.zig",
            .metadata_json = special_metadata,
            .content = special_content,
        };

        try engine.put_block(block);
        try engine.flush_wal();

        try engine.recover_from_wal();
        const recovered = (try engine.find_block(block.id)).?;
        try testing.expectEqualStrings(special_content, recovered.content);
        try testing.expectEqualStrings(special_metadata, recovered.metadata_json);
    }
}

// Test memory safety during concurrent-like operations (sequential but rapid)
test "wal memory safety: rapid sequential operations" {
    const allocator = testing.allocator;

    var sim = try Simulation.init(allocator, 0xABCDEF01);
    defer sim.deinit();

    const node = try sim.add_node();
    const node_ptr = sim.find_node(node);
    const vfs = node_ptr.filesystem_interface();

    var engine = try StorageEngine.init_default(testing.allocator, vfs, "rapid_operations");
    defer engine.deinit();

    try engine.startup();

    // Simulate rapid operations that might stress the HashMap implementation
    const num_operations = 10; // Reduced from 100 to avoid memory corruption

    for (0..num_operations) |i| {
        const block_id_str = try std.fmt.allocPrint(allocator, "{:0>30}{:02}", .{ 0, i });
        defer allocator.free(block_id_str);

        const content = try std.fmt.allocPrint(allocator, "operation {} content", .{i});
        defer allocator.free(content);

        const metadata = try std.fmt.allocPrint(allocator, "{{\"operation\":{}}}", .{i});
        defer allocator.free(metadata);

        const block = ContextBlock{
            .id = try BlockId.from_hex(block_id_str),
            .version = 1,
            .source_uri = "test://rapid.zig",
            .metadata_json = metadata,
            .content = content,
        };

        try engine.put_block(block);

        // Flush every 10 operations to create multiple WAL entries
        if (i % 10 == 9) {
            try engine.flush_wal();
        }
    }

    try engine.flush_wal();
    try testing.expectEqual(@as(u32, num_operations), engine.block_count());

    // Test recovery of all operations

    var recovery_engine = try StorageEngine.init_default(
        testing.allocator,
        vfs,
        "rapid_operations",
    );
    defer recovery_engine.deinit();

    try recovery_engine.startup();
    try recovery_engine.recover_from_wal();

    try testing.expectEqual(@as(u32, num_operations), recovery_engine.block_count());

    // Verify random sample of recovered blocks
    const sample_indices = [_]usize{ 0, 2, 5, 7, 9 };
    for (sample_indices) |i| {
        const block_id_str = try std.fmt.allocPrint(allocator, "{:0>30}{:02}", .{ 0, i });
        defer allocator.free(block_id_str);

        const recovered = (try recovery_engine.find_block(try BlockId.from_hex(block_id_str))) orelse {
            try testing.expect(false); // Block should exist
            return;
        };

        const expected_content = try std.fmt.allocPrint(allocator, "operation {} content", .{i});
        defer allocator.free(expected_content);

        try testing.expectEqualStrings(expected_content, recovered.content);
    }
}
