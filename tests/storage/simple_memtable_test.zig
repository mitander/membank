//! Simple test to debug MemtableManager segfault

const cortexdb = @import("cortexdb");
const std = @import("std");
const testing = std.testing;

const simulation_vfs = cortexdb.simulation_vfs;
const storage = cortexdb.storage;

const SimulationVFS = simulation_vfs.SimulationVFS;
const MemtableManager = storage.MemtableManager;

test "simple SimulationVFS test" {
    const allocator = testing.allocator;

    var sim_vfs = try SimulationVFS.init(allocator);
    defer sim_vfs.deinit();

    // Just verify VFS works
    try testing.expect(true);
}

test "simple MemtableManager creation" {
    const allocator = testing.allocator;

    var sim_vfs = try SimulationVFS.init(allocator);
    defer sim_vfs.deinit();

    var manager = try MemtableManager.init(allocator, sim_vfs.vfs(), "/test/data", 1024 * 1024);
    defer manager.deinit();

    // Just verify creation works
    try testing.expect(true);
}
