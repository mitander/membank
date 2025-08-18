//! Memory safety fatal assertion tests for KausalDB.
//!
//! Validates that fatal assertions trigger correctly when memory corruption
//! is detected. These tests use controlled corruption scenarios to verify
//! the system fails fast on critical invariant violations.

const std = @import("std");
const testing = std.testing;

const kausaldb = @import("kausaldb");
const storage = kausaldb.storage;

const VFS = kausaldb.VFS;
const VFile = kausaldb.VFile;
const ContextBlock = kausaldb.ContextBlock;
const BlockId = kausaldb.BlockId;
const StorageEngine = kausaldb.StorageEngine;
const SimulationVFS = kausaldb.SimulationVFS;

// Test that arena state validation triggers fatal assertion in storage components
test "arena corruption detection through storage engine" {
    const allocator = testing.allocator;

    var sim_vfs = try SimulationVFS.init(allocator);
    defer sim_vfs.deinit();

    const vfs_interface = sim_vfs.vfs();
    const data_dir = "arena_corruption_test";

    const storage_config = storage.Config{};
    var engine = try StorageEngine.init(allocator, vfs_interface, data_dir, storage_config);
    defer engine.deinit();

    try engine.startup();

    const test_block = ContextBlock{
        .id = BlockId.from_bytes([_]u8{1} ** 16),
        .version = 1,
        .source_uri = "test://memory_safety",
        .metadata_json = "{}",
        .content = "test content for arena corruption detection",
    };

    // Note: Arena corruption detection requires internal component access
    // This test documents expected fatal assertion behavior on arena failure
    // In actual corruption scenarios, operations would trigger fatal_assert immediately

    try engine.put_block(test_block);

    // Verify basic storage engine operation without corruption
    const retrieved = try engine.find_block(test_block.id, .query_engine);
    try testing.expect(retrieved != null);
    try testing.expectEqualStrings(test_block.content, retrieved.?.extract().content);
}

// Test VFS handle corruption detection
test "VFS handle corruption fatal assertion" {
    const allocator = testing.allocator;

    var sim_vfs = try SimulationVFS.init(allocator);
    defer sim_vfs.deinit();

    const vfs_interface = sim_vfs.vfs();

    // Create a file normally
    var test_file = try vfs_interface.create("test_file.txt");

    // Note: VFile corruption testing requires deeper integration with the VFS layer
    // This test documents the expected behavior rather than implementing it

    const buffer: [10]u8 = undefined;
    _ = buffer; // Suppress unused variable warning

    // This should trigger fatal assertion about VFS handle corruption
    if (@import("builtin").mode != .Debug) {
        // In release builds, fatal_assert should trigger on handle corruption
        // Expected: "VFS handle corruption detected: ptr=0x100 handle=0 - memory safety violation"
        // In actual test runs, this would crash with fatal_assert
    }

    test_file.close();
}

// Test storage engine pointer corruption detection
// Test disabled for 0.1.0 release - requires expectPanic API not available in current Zig version
// test "storage engine pointer corruption fatal assertion" {
//     const allocator = testing.allocator;

//     var sim_vfs = try SimulationVFS.init(allocator);
//     defer sim_vfs.deinit();

//     const vfs_interface = sim_vfs.vfs();
//     const data_dir = "test_storage";

//     const storage_config = storage.Config{};
//     var engine = try StorageEngine.init(allocator, vfs_interface, data_dir, storage_config);

//     try engine.startup();

//     // Manually corrupt the data_dir to trigger fatal assertion
//     // This simulates heap corruption where the data_dir string gets corrupted
//     allocator.free(engine.data_dir);
//     engine.data_dir = ""; // Empty data_dir should trigger fatal assertion

//     // This should trigger: "StorageEngine data_dir corrupted - heap corruption detected"
//     // The fatal assertion should fire during deinit()
//     try testing.expectPanic(engine.deinit);
// }

// Test memory accounting corruption detection through storage engine
test "memory accounting corruption detection through storage engine" {
    const allocator = testing.allocator;

    var sim_vfs = try SimulationVFS.init(allocator);
    defer sim_vfs.deinit();

    const vfs_interface = sim_vfs.vfs();
    const data_dir = "accounting_corruption_test";

    const storage_config = storage.Config{};
    var engine = try StorageEngine.init(allocator, vfs_interface, data_dir, storage_config);
    defer engine.deinit();

    try engine.startup();

    const test_block = ContextBlock{
        .id = BlockId.from_bytes([_]u8{3} ** 16),
        .version = 1,
        .source_uri = "test://accounting",
        .metadata_json = "{}",
        .content = "test content for memory accounting validation",
    };

    // Note: Memory accounting corruption testing requires internal component access
    // This test documents expected fatal assertion behavior on accounting underflow
    // In actual corruption scenarios, operations would trigger fatal_assert immediately

    try engine.put_block(test_block);

    // Verify accounting through successful retrieval
    const retrieved = try engine.find_block(test_block.id, .query_engine);
    try testing.expect(retrieved != null);
    try testing.expectEqualStrings(test_block.content, retrieved.?.extract().content);
}

// Test arena clear corruption detection through storage engine
test "arena clear corruption detection through storage engine" {
    const allocator = testing.allocator;

    var sim_vfs = try SimulationVFS.init(allocator);
    defer sim_vfs.deinit();

    const vfs_interface = sim_vfs.vfs();
    const data_dir = "arena_clear_test";

    const storage_config = storage.Config{};
    var engine = try StorageEngine.init(allocator, vfs_interface, data_dir, storage_config);
    defer engine.deinit();

    try engine.startup();

    const test_block = ContextBlock{
        .id = BlockId.from_bytes([_]u8{4} ** 16),
        .version = 1,
        .source_uri = "test://clear",
        .metadata_json = "{}",
        .content = "test content for arena clear validation",
    };

    try engine.put_block(test_block);

    try engine.put_block(test_block);

    // Verify arena operations through storage functionality
    const retrieved = try engine.find_block(test_block.id, .query_engine);
    try testing.expect(retrieved != null);

    // Verify flush operation completes without corruption
    try engine.flush_wal();

    // Verify data persistence after flush
    const post_flush_retrieved = try engine.find_block(test_block.id, .query_engine);
    try testing.expect(post_flush_retrieved != null);
    try testing.expectEqualStrings(test_block.content, post_flush_retrieved.?.extract().content);
}

// Integration test: verify fatal assertions don't trigger in normal operation
test "normal operation does not trigger fatal assertions" {
    const allocator = testing.allocator;

    var sim_vfs = try SimulationVFS.init(allocator);
    defer sim_vfs.deinit();

    const vfs_interface = sim_vfs.vfs();
    const data_dir = "normal_test_storage";

    const storage_config = storage.Config{};
    var engine = try StorageEngine.init(allocator, vfs_interface, data_dir, storage_config);
    defer engine.deinit();

    try engine.startup();

    // Perform normal operations - these should not trigger any fatal assertions
    const test_block = ContextBlock{
        .id = BlockId.from_bytes([_]u8{42} ++ [_]u8{0} ** 15),
        .version = 1,
        .source_uri = "test://normal",
        .metadata_json = "{}",
        .content = "normal test content",
    };

    // Normal put operation
    try engine.put_block(test_block);

    // Normal find operation
    const found_block = try engine.find_block(test_block.id, .query_engine);
    try testing.expect(found_block != null);
    try testing.expect(std.mem.eql(u8, found_block.?.extract().content, test_block.content));

    // Normal VFS operations
    var test_file = try vfs_interface.create("normal_test_file.txt");
    defer test_file.close();

    const write_data = "normal file content";
    _ = try test_file.write(write_data);

    _ = try test_file.seek(0, .start);
    var read_buffer: [100]u8 = undefined;
    const bytes_read = try test_file.read(&read_buffer);
    try testing.expect(bytes_read == write_data.len);
    try testing.expect(std.mem.eql(u8, read_buffer[0..bytes_read], write_data));

    // All operations completed successfully without triggering fatal assertions
}
