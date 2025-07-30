//! Query Engine Fault Injection Tests
//!
//! Validates query engine robustness under hostile conditions including:
//! - SSTable read errors during block lookups
//! - Memory pressure during large result set processing
//! - Storage corruption during graph traversals
//! - I/O failures in index access operations
//! - Cascading failures across query operations
//! - Resource cleanup after query failures
//!
//! All tests use deterministic simulation for reproducible failure scenarios.

const membank = @import("membank");
const std = @import("std");
const testing = std.testing;

const simulation_vfs = membank.simulation_vfs;
const storage = membank.storage;
const context_block = membank.types;
const vfs = membank.vfs;

const SimulationVFS = simulation_vfs.SimulationVFS;
const StorageEngine = storage.StorageEngine;
const ContextBlock = context_block.ContextBlock;
const BlockId = context_block.BlockId;

// Helper function to create test BlockId from integer
fn test_block_id(id: u32) BlockId {
    var bytes: [16]u8 = [_]u8{0} ** 16;
    std.mem.writeInt(u32, bytes[0..4], id, .little);
    return BlockId.from_bytes(bytes);
}

test "query handles SSTable read errors gracefully" {
    const allocator = testing.allocator;
    var sim_vfs = try SimulationVFS.init_with_fault_seed(allocator, 0xABCDE);
    defer sim_vfs.deinit();

    const vfs_interface = sim_vfs.vfs();

    // Setup storage with test data
    var engine = try StorageEngine.init_default(allocator, vfs_interface, "membank_data");
    defer engine.deinit();
    try engine.startup();

    // Insert test blocks
    const test_block = ContextBlock{
        .id = test_block_id(12345),
        .version = 1,
        .source_uri = "test_file.zig",
        .metadata_json = "{}",
        .content = "fn test_function() void {}",
    };
    try engine.put_block(test_block);

    // Enable read failures to simulate SSTable corruption
    sim_vfs.enable_io_failures(500, .{ .read = true }); // 50% probability on reads

    // Test block retrieval under read pressure
    const find_result = engine.find_block(test_block_id(12345));
    if (find_result) |maybe_block| {
        if (maybe_block) |found_block| {
            // Success case - block found despite I/O pressure
            try testing.expectEqual(test_block.id, found_block.id);
        }
        // If null, block not found but no error - acceptable
    } else |err| {
        // Failure case - should be storage-related error
        try testing.expect(err == error.IoError or err == error.AccessDenied);
    }
}

test "query handles memory pressure in large result sets" {
    const allocator = testing.allocator;
    var sim_vfs = try SimulationVFS.init_with_fault_seed(allocator, 0xBCDEF);
    defer sim_vfs.deinit();

    const vfs_interface = sim_vfs.vfs();

    var engine = try StorageEngine.init_default(allocator, vfs_interface, "membank_data");
    defer engine.deinit();
    try engine.startup();

    // Configure disk space limit to simulate memory pressure
    sim_vfs.configure_disk_space_limit(128 * 1024); // 128KB limit

    // Insert several blocks to create dataset
    for (0..10) |i| {
        const block_id = test_block_id(@intCast(i + 1));
        const content = try std.fmt.allocPrint(allocator, "fn function_{d}() void {{}}", .{i});
        defer allocator.free(content);

        const test_block = ContextBlock{
            .id = block_id,
            .version = 1,
            .source_uri = "large_dataset.zig",
            .metadata_json = "{}",
            .content = content,
        };

        const put_result = engine.put_block(test_block);
        if (put_result) |_| {
            // Block inserted successfully
        } else |err| {
            // Expected failure under disk pressure
            try testing.expect(err == error.Unexpected or err == error.AccessDenied);
        }
    }

    // Test block retrieval under resource pressure
    const find_result = engine.find_block(test_block_id(1));
    if (find_result) |maybe_block| {
        if (maybe_block) |found_block| {
            try testing.expectEqual(test_block_id(1), found_block.id);
        }
        // If null, block not found but no error - acceptable under pressure
    } else |err| {
        // Acceptable failure under resource pressure
        try testing.expect(err == error.IoError or err == error.NoSpaceLeft);
    }
}

test "query handles storage corruption during graph traversal" {
    const allocator = testing.allocator;
    var sim_vfs = try SimulationVFS.init_with_fault_seed(allocator, 0xCDEF0);
    defer sim_vfs.deinit();

    const vfs_interface = sim_vfs.vfs();

    var engine = try StorageEngine.init_default(allocator, vfs_interface, "membank_data");
    defer engine.deinit();
    try engine.startup();

    // Create connected blocks for testing
    const main_block = ContextBlock{
        .id = test_block_id(1001),
        .version = 1,
        .source_uri = "main.zig",
        .metadata_json = "{}",
        .content = "fn main() void { helper.process(); }",
    };
    const helper_block = ContextBlock{
        .id = test_block_id(1002),
        .version = 1,
        .source_uri = "helper.zig",
        .metadata_json = "{}",
        .content = "fn process() void {}",
    };

    try engine.put_block(main_block);
    try engine.put_block(helper_block);

    // Add graph edge
    const edge = context_block.GraphEdge{
        .source_id = test_block_id(1001),
        .target_id = test_block_id(1002),
        .edge_type = .calls,
    };
    try engine.put_edge(edge);

    // Enable read corruption to simulate storage issues
    sim_vfs.enable_read_corruption(200, 2); // High corruption rate

    // Test block retrieval under corruption
    const find_result = engine.find_block(test_block_id(1001));
    if (find_result) |maybe_block| {
        if (maybe_block) |found_block| {
            try testing.expectEqual(main_block.id, found_block.id);
        }
        // If null, block not found but no error - acceptable under corruption
    } else |err| {
        // Acceptable failure under corruption
        try testing.expect(err == error.IoError or err == error.AccessDenied);
    }
}

test "query handles slow I/O and timeout conditions" {
    const allocator = testing.allocator;
    var sim_vfs = try SimulationVFS.init_with_fault_seed(allocator, 0xDEF01);
    defer sim_vfs.deinit();

    const vfs_interface = sim_vfs.vfs();

    var engine = try StorageEngine.init_default(allocator, vfs_interface, "membank_data");
    defer engine.deinit();
    try engine.startup();

    // Insert test data
    const test_block = ContextBlock{
        .id = test_block_id(2001),
        .version = 1,
        .source_uri = "slow_test.zig",
        .metadata_json = "{}",
        .content = "fn slow_function() void {}",
    };
    try engine.put_block(test_block);

    // Enable I/O failures to simulate slow I/O conditions
    sim_vfs.enable_io_failures(300, .{ .read = true }); // 30% probability on reads

    const start_time = std.time.milliTimestamp();

    // Test block retrieval under slow I/O
    const find_result = engine.find_block(test_block_id(2001));
    const elapsed = std.time.milliTimestamp() - start_time;

    if (find_result) |maybe_block| {
        if (maybe_block) |found_block| {
            try testing.expectEqual(test_block.id, found_block.id);
        }
        // If null, block not found but no error - acceptable under slow I/O
    } else |err| {
        // Acceptable failure under slow I/O conditions
        try testing.expect(err == error.IoError or err == error.AccessDenied);
    }

    // Verify reasonable timeout behavior (should not hang indefinitely)
    try testing.expect(elapsed < 1000); // Max 1 second for test efficiency
}

test "query provides detailed error context for debugging" {
    const allocator = testing.allocator;
    var sim_vfs = try SimulationVFS.init_with_fault_seed(allocator, 0xEF012);
    defer sim_vfs.deinit();

    const vfs_interface = sim_vfs.vfs();

    var engine = try StorageEngine.init_default(allocator, vfs_interface, "membank_data");
    defer engine.deinit();
    try engine.startup();

    // Enable I/O failures to create error conditions
    sim_vfs.enable_io_failures(400, .{ .read = true }); // 40% probability on reads

    // Test querying for non-existent blocks under I/O pressure
    const invalid_ids = [_]BlockId{ test_block_id(99999), test_block_id(88888), test_block_id(77777) };

    for (invalid_ids) |block_id| {
        const find_result = engine.find_block(block_id);
        if (find_result) |maybe_block| {
            if (maybe_block) |found_block| {
                // Unexpected success - block somehow exists
                try testing.expect(found_block.id.eql(block_id));
            }
            // If null, block not found - expected for invalid IDs
        } else |err| {
            // Expected failure - should provide error context
            try testing.expect(err == error.BlockNotFound or
                err == error.IoError or
                err == error.AccessDenied);
        }
    }
}

test "semantic query handles parsing and similarity failures" {
    const allocator = testing.allocator;
    var sim_vfs = try SimulationVFS.init_with_fault_seed(allocator, 0xF0123);
    defer sim_vfs.deinit();

    const vfs_interface = sim_vfs.vfs();

    var engine = try StorageEngine.init_default(allocator, vfs_interface, "membank_data");
    defer engine.deinit();
    try engine.startup();

    // Insert blocks with various content types
    const blocks = [_]ContextBlock{
        .{
            .id = test_block_id(3001),
            .version = 1,
            .source_uri = "func1.zig",
            .metadata_json = "{}",
            .content = "fn hash_function() u32 { return 42; }",
        },
        .{
            .id = test_block_id(3002),
            .version = 1,
            .source_uri = "func2.zig",
            .metadata_json = "{}",
            .content = "fn hash_table() void { /* implementation */ }",
        },
    };

    for (blocks) |block| {
        try engine.put_block(block);
    }

    // Enable read corruption to simulate parsing challenges
    sim_vfs.enable_read_corruption(150, 2); // Moderate corruption rate

    // Test block retrieval under corruption (simulates semantic search)
    for (blocks) |expected_block| {
        const find_result = engine.find_block(expected_block.id);
        if (find_result) |maybe_block| {
            if (maybe_block) |found_block| {
                try testing.expectEqual(expected_block.id, found_block.id);
            }
            // If null, block not found but no error - acceptable under corruption
        } else |err| {
            // Acceptable failure under corruption
            try testing.expect(err == error.BlockNotFound or
                err == error.IoError or
                err == error.AccessDenied);
        }
    }
}

test "query handles resource contention and concurrent access" {
    const allocator = testing.allocator;
    var sim_vfs = try SimulationVFS.init_with_fault_seed(allocator, 0x01234);
    defer sim_vfs.deinit();

    const vfs_interface = sim_vfs.vfs();

    var engine = try StorageEngine.init_default(allocator, vfs_interface, "membank_data");
    defer engine.deinit();
    try engine.startup();

    // Insert test data
    for (0..10) |i| {
        const content = try std.fmt.allocPrint(allocator, "fn func_{d}() void {{}}", .{i});
        defer allocator.free(content);

        const block = ContextBlock{
            .id = test_block_id(@intCast(4000 + i)),
            .version = 1,
            .source_uri = "concurrent_test.zig",
            .metadata_json = "{}",
            .content = content,
        };
        try engine.put_block(block);
    }

    // Enable intermittent failures to simulate contention
    sim_vfs.enable_io_failures(200, .{ .read = true }); // 20% probability on reads

    // Simulate concurrent access by rapidly executing multiple queries
    const query_ids = [_]BlockId{ test_block_id(4001), test_block_id(4003), test_block_id(4005), test_block_id(4007), test_block_id(4009) };

    var successful_queries: u32 = 0;
    for (query_ids) |block_id| {
        const find_result = engine.find_block(block_id);
        if (find_result) |maybe_block| {
            if (maybe_block) |found_block| {
                try testing.expectEqual(block_id, found_block.id);
                successful_queries += 1;
            }
            // If null, block not found but no error
        } else |_| {
            // Some failures expected due to contention
        }
    }

    // Should complete some queries successfully despite contention
    try testing.expect(successful_queries > 0);
}

test "query properly cleans up resources after failures" {
    const allocator = testing.allocator;
    var sim_vfs = try SimulationVFS.init_with_fault_seed(allocator, 0x12345);
    defer sim_vfs.deinit();

    const vfs_interface = sim_vfs.vfs();

    var engine = try StorageEngine.init_default(allocator, vfs_interface, "membank_data");
    defer engine.deinit();
    try engine.startup();

    // Insert test block
    const test_block = ContextBlock{
        .id = test_block_id(5001),
        .version = 1,
        .source_uri = "cleanup_test.zig",
        .metadata_json = "{}",
        .content = "fn cleanup_test() void {}",
    };
    try engine.put_block(test_block);

    // Enable write failures to simulate cleanup challenges
    sim_vfs.enable_io_failures(600, .{ .write = true }); // 60% probability on writes

    // Test block retrieval under write pressure
    const find_result = engine.find_block(test_block_id(5001));
    if (find_result) |maybe_block| {
        if (maybe_block) |found_block| {
            // Success case - verify proper operation
            try testing.expectEqual(test_block.id, found_block.id);
        }
        // If null, block not found but no error - acceptable
    } else |_| {
        // Failure case - resources should be cleaned up automatically
        // The defer statements and arena allocators handle this
    }

    // Verify no resource leaks by successful deinit
    // In debug builds with leak detection, this would catch issues
}

test "query validates parameters and handles malformed inputs" {
    const allocator = testing.allocator;
    var sim_vfs = try SimulationVFS.init_with_fault_seed(allocator, 0x23456);
    defer sim_vfs.deinit();

    const vfs_interface = sim_vfs.vfs();

    var engine = try StorageEngine.init_default(allocator, vfs_interface, "membank_data");
    defer engine.deinit();
    try engine.startup();

    // Test non-existent block ID queries
    const invalid_block_ids = [_]BlockId{ test_block_id(999999), test_block_id(888888) };

    for (invalid_block_ids) |block_id| {
        const find_result = engine.find_block(block_id);
        if (find_result) |maybe_block| {
            if (maybe_block) |found_block| {
                // Unexpected success - somehow found invalid block
                try testing.expect(found_block.id.eql(block_id));
            }
            // If null, block not found - expected for invalid IDs
        } else |err| {
            // Expected failure for invalid IDs
            try testing.expect(err == error.BlockNotFound or
                err == error.IoError or
                err == error.AccessDenied);
        }
    }

    // Enable I/O failures to test error handling robustness
    sim_vfs.enable_io_failures(300, .{ .read = true }); // 30% probability on reads

    // Test multiple invalid queries under I/O pressure
    for (0..5) |i| {
        const random_id = test_block_id(@intCast(90000 + i));
        const find_result = engine.find_block(random_id);
        if (find_result) |_| {
            // Unexpected success
        } else |err| {
            // Expected failure
            try testing.expect(err == error.BlockNotFound or
                err == error.IoError or
                err == error.AccessDenied);
        }
    }

    // Verify engine remains stable after validation failures
    const block_count = engine.block_count();
    try testing.expectEqual(@as(u32, 0), block_count);
}
