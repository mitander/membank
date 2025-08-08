//! MemtableManager unit tests
//!
//! Tests individual MemtableManager functionality in isolation.
//! Focus: basic operations, memory management, block lifecycle.

const kausaldb = @import("kausaldb");
const std = @import("std");
const testing = std.testing;

const simulation_vfs = kausaldb.simulation_vfs;
const storage = kausaldb.storage;
const types = kausaldb.types;

const ContextBlock = types.ContextBlock;
const BlockId = types.BlockId;
const MemtableManager = storage.MemtableManager;
const SimulationVFS = simulation_vfs.SimulationVFS;
const TestData = kausaldb.TestData;

test "put block basic" {
    const allocator = testing.allocator;

    // Use StorageHarness for simplified setup while testing MemtableManager specifically
    var harness = try kausaldb.StorageHarness.init_and_startup(allocator, "memtable_basic");
    defer harness.deinit();

    // Access the underlying memtable manager for direct testing
    const manager = &harness.storage_engine.memtable_manager;

    // Use standardized test data for consistent block creation
    const test_block = try TestData.create_test_block(allocator, 1);
    defer TestData.cleanup_test_block(allocator, test_block);

    // Test specific memtable operation
    try manager.put_block(test_block);
    const retrieved = manager.find_block_in_memtable(test_block.id);

    // Simple, direct assertions
    try testing.expect(retrieved != null);
    try testing.expectEqualStrings(test_block.content, retrieved.?.content);
}

test "find block missing" {
    const allocator = testing.allocator;

    var harness = try kausaldb.StorageHarness.init_and_startup(allocator, "memtable_missing");
    defer harness.deinit();

    const manager = &harness.storage_engine.memtable_manager;

    // Test finding non-existent block using deterministic ID
    const missing_id = TestData.deterministic_block_id(99);
    const result = manager.find_block_in_memtable(missing_id);

    try testing.expect(result == null);
}

test "put block overwrite" {
    const allocator = testing.allocator;

    var harness = try kausaldb.StorageHarness.init_and_startup(allocator, "memtable_overwrite");
    defer harness.deinit();

    const manager = &harness.storage_engine.memtable_manager;

    // Create blocks with same ID but different versions using TestData base
    const base_block = try TestData.create_test_block(allocator, 1);
    defer TestData.cleanup_test_block(allocator, base_block);

    const block_id = base_block.id;

    // Put first version
    const block_v1 = ContextBlock{
        .id = block_id,
        .version = 1,
        .source_uri = try allocator.dupe(u8, "test://version1.zig"),
        .metadata_json = try allocator.dupe(u8, "{}"),
        .content = try allocator.dupe(u8, "version 1 content"),
    };
    defer {
        allocator.free(block_v1.source_uri);
        allocator.free(block_v1.metadata_json);
        allocator.free(block_v1.content);
    }
    try manager.put_block(block_v1);

    // Put second version (overwrite)
    const block_v2 = ContextBlock{
        .id = block_id,
        .version = 2,
        .source_uri = try allocator.dupe(u8, "test://version2.zig"),
        .metadata_json = try allocator.dupe(u8, "{}"),
        .content = try allocator.dupe(u8, "version 2 content"),
    };
    defer {
        allocator.free(block_v2.source_uri);
        allocator.free(block_v2.metadata_json);
        allocator.free(block_v2.content);
    }
    try manager.put_block(block_v2);

    // Verify latest version is retrieved
    const retrieved = manager.find_block_in_memtable(block_id).?;
    try testing.expectEqual(@as(u64, 2), retrieved.version);
    try testing.expectEqualStrings("version 2 content", retrieved.content);
}
