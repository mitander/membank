//! Storage Engine Fault Injection Tests
//!
//! Tests storage engine behavior under various fault conditions including:
//! - Mid-compaction crashes
//! - WAL corruption during write
//! - SSTable corruption during read
//! - Disk full during compaction

const std = @import("std");
const testing = std.testing;
const kausaldb = @import("kausaldb");

const ContextBlock = kausaldb.types.ContextBlock;
const BlockId = kausaldb.types.BlockId;
const SimulationVFS = kausaldb.simulation_vfs.SimulationVFS;
const StorageEngine = kausaldb.storage.StorageEngine;
const TestData = kausaldb.TestData;
const FaultInjectionHarness = kausaldb.FaultInjectionHarness;
const FaultInjectionConfig = kausaldb.FaultInjectionConfig;

test "fault injection simulation vfs infrastructure" {
    // Test basic fault injection infrastructure without full storage engine
    const allocator = testing.allocator;

    var sim_vfs = try SimulationVFS.init_with_fault_seed(allocator, 12345);
    defer sim_vfs.deinit();

    // Test torn write functionality
    sim_vfs.enable_torn_writes(1000, 1, 50); // 100% probability, 50% completion

    var vfs_interface = sim_vfs.vfs();

    // Create a test file and write to it
    var file = try vfs_interface.create("test_torn_write.txt");
    defer file.close();

    const test_data = "This is a test message that should be partially written";
    const written = try file.write(test_data);
    file.close();

    // Verify torn write occurred
    try testing.expect(written < test_data.len);
    try testing.expect(written > 0);

    // Test disk space limits
    sim_vfs.configure_disk_space_limit(10);

    var file2 = try vfs_interface.create("test_disk_full.txt");
    defer file2.close();

    const large_data = "This data exceeds the disk limit";
    const result = file2.write(large_data);
    try testing.expectError(error.NoSpaceLeft, result);
}

test "fault injection disk full during compaction" {
    const allocator = testing.allocator;

    // Configure fault injection for disk space exhaustion
    var fault_config = FaultInjectionConfig{};
    fault_config.io_failures.enabled = true;
    fault_config.io_failures.failure_rate_per_thousand = 200; // 20% failure rate
    fault_config.io_failures.operations.write = true;

    var harness = try FaultInjectionHarness.init_with_faults(allocator, 0xDEAD, "test_db", fault_config);
    defer harness.deinit();
    try harness.startup();

    // Fill up storage with blocks to trigger compaction using standardized test data
    var blocks_written: u32 = 0;
    for (1..101) |i| {
        const block = ContextBlock{
            .id = TestData.deterministic_block_id(@intCast(i)),
            .version = 1,
            .source_uri = "test://storage_fault_test.zig",
            .metadata_json = "{\"test\":\"storage_fault\"}",
            .content = "Storage fault test block content",
        };

        harness.storage_engine().put_block(block) catch {
            // Any error during storage indicates resource exhaustion
            break;
        };
        blocks_written += 1;

        // Advance simulation time to trigger fault injection
        if (i % 10 == 0) {
            harness.tick(1);
        }
    }

    // Storage engine should handle faults gracefully
    // Some blocks should be stored despite I/O failures
    try testing.expect(blocks_written > 0);
}

test "fault injection read corruption during query" {
    const allocator = testing.allocator;

    // Configure fault injection for read operations
    var fault_config = FaultInjectionConfig{};
    fault_config.io_failures.enabled = true;
    fault_config.io_failures.failure_rate_per_thousand = 100; // 10% failure rate
    fault_config.io_failures.operations.read = true;

    var harness = try FaultInjectionHarness.init_with_faults(allocator, 0xDEADBEEF, "test_db", fault_config);
    defer harness.deinit();
    try harness.startup();

    // Create and store a test block using standardized test data
    const test_block = ContextBlock{
        .id = TestData.deterministic_block_id(1),
        .version = 1,
        .source_uri = "test://read_corruption_test.zig",
        .metadata_json = "{\"test\":\"read_corruption\"}",
        .content = "Read corruption test block content",
    };

    try harness.storage_engine().put_block(test_block);

    // Force flush to SSTable to ensure data goes to disk
    try harness.storage_engine().flush_memtable_to_sstable();

    // Advance simulation time to trigger fault injection
    harness.tick(5);

    // Try to read the block - may succeed or fail due to fault injection
    const found_block = harness.storage_engine().find_block(test_block.id, .query_engine) catch |err| {
        // I/O failures during read are expected with fault injection
        try testing.expect(err == error.IOFailure or err == error.Corruption);
        return;
    };

    if (found_block) |block| {
        try testing.expect(std.mem.eql(u8, block.extract().content, test_block.content));
    }
    // Block not found is also acceptable under fault injection
}

// Helper function for low-level disk usage calculation
// This accesses SimulationVFS internals for testing purposes
fn calculate_disk_usage(sim_vfs: *SimulationVFS) u64 {
    var total_usage: u64 = 0;
    for (sim_vfs.file_storage.items) |file_entry| {
        if (file_entry.active and !file_entry.data.is_directory) {
            total_usage += file_entry.data.content.items.len;
        }
    }
    return total_usage;
}
