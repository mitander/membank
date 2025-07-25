const std = @import("std");
const testing = std.testing;
const StorageEngine = @import("src/storage/storage.zig").StorageEngine;
const ContextBlock = @import("src/core/types.zig").ContextBlock;
const BlockId = @import("src/core/types.zig").BlockId;
const Simulation = @import("src/sim/simulation.zig").Simulation;

test "minimal WAL corruption reproduction" {
    var gpa = std.heap.GeneralPurposeAllocator(.{ .safety = true }){};
    defer {
        const deinit_status = gpa.deinit();
        if (deinit_status == .leak) @panic("Memory leak detected");
    }
    const allocator = gpa.allocator();

    var sim = try Simulation.init(allocator, 42);
    defer sim.deinit();

    const node = try sim.add_node();
    const node_ptr = sim.find_node(node);
    const vfs = node_ptr.filesystem_interface();

    std.debug.print("=== Creating minimal corruption test ===\n", .{});

    // Create a small block with pattern data that triggers corruption
    const content = try allocator.alloc(u8, 1024);
    defer allocator.free(content);
    @memset(content, 'x'); // This pattern causes corruption

    const block = ContextBlock{
        .id = try BlockId.from_hex("1111111111111111222222222222222"),
        .version = 1,
        .source_uri = "test://debug.txt",
        .metadata_json = "{}",
        .content = content,
    };

    std.debug.print("Block content pattern: first 16 bytes = ", .{});
    for (content[0..16]) |byte| {
        std.debug.print("{c}", .{byte});
    }
    std.debug.print("\n", .{});

    // Write the block
    var storage = try StorageEngine.init_default(allocator, vfs, "debug_test");
    defer storage.deinit();

    try storage.initialize_storage();
    std.debug.print("Storage initialized\n", .{});

    try storage.put_block(block);
    std.debug.print("Block written to storage\n", .{});

    try storage.flush_wal();
    std.debug.print("WAL flushed\n", .{});

    // Now try to recover - this should fail
    var recovery_storage = try StorageEngine.init_default(allocator, vfs, "debug_test");
    defer recovery_storage.deinit();

    try recovery_storage.initialize_storage();
    std.debug.print("Recovery storage initialized\n", .{});

    std.debug.print("Attempting WAL recovery...\n", .{});
    recovery_storage.recover_from_wal() catch |err| {
        std.debug.print("Recovery failed with error: {}\n", .{err});
        return; // This is expected
    };

    std.debug.print("Recovery succeeded unexpectedly\n", .{});
}
