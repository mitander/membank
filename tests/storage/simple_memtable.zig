//! Simple test to debug MemtableManager segfault

const membank = @import("membank");
const std = @import("std");
const testing = std.testing;

const simulation_vfs = membank.simulation_vfs;
const storage = membank.storage;

const SimulationVFS = simulation_vfs.SimulationVFS;
const MemtableManager = storage.MemtableManager;

test "SimulationVFS file operations" {
    const allocator = testing.allocator;

    var sim_vfs = try SimulationVFS.init(allocator);
    defer sim_vfs.deinit();

    const vfs = sim_vfs.vfs();

    // Verify VFS can create and write to files
    var test_file = try vfs.create("test.txt");
    defer test_file.close();

    const test_data = "VFS verification data";
    try test_file.write_all(test_data);
    try test_file.sync();
}

test "MemtableManager basic operations" {
    const allocator = testing.allocator;

    var sim_vfs = try SimulationVFS.init(allocator);
    defer sim_vfs.deinit();

    var manager = try MemtableManager.init(allocator, sim_vfs.vfs(), "/test/data", 1024 * 1024);
    defer manager.deinit();

    try manager.startup();

    // Verify manager can store and retrieve a block
    const test_block = ContextBlock{
        .id = BlockId.from_bytes([_]u8{1} ** 16),
        .version = 1,
        .source_uri = "test://memtable",
        .metadata_json = "{}",
        .content = "test content",
    };

    try manager.put_block(test_block);
    const retrieved = manager.find_block(test_block.id);
    try testing.expect(retrieved != null);
    try testing.expectEqualStrings(test_block.content, retrieved.?.content);
}
