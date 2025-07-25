//! VFS Debug Tests
//!
//! Focused tests to debug VFS file content initialization issues.
//! Tests the specific case where file content expansion should be zero-initialized.

const cortexdb = @import("cortexdb");
const std = @import("std");
const testing = std.testing;
const assert = std.debug.assert;

const vfs = cortexdb.vfs;
const simulation = cortexdb.simulation;

const VFS = vfs.VFS;
const VFile = vfs.VFile;
const Simulation = simulation.Simulation;

test "vfs debug: file content expansion zero initialization" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const allocator = arena.allocator();

    var sim = try Simulation.init(allocator, 12345);
    defer sim.deinit();

    const node1 = try sim.add_node();
    const node1_ptr = sim.find_node(node1);
    const node1_vfs = node1_ptr.filesystem_interface();

    // Create a test file
    var file = try node1_vfs.create("test_expansion.dat");
    defer file.close();

    // Write initial data at position 0
    const initial_data = "HELLO";
    const bytes_written1 = try file.write(initial_data);
    try testing.expectEqual(initial_data.len, bytes_written1);

    // Seek to position 100 (beyond current file size of 5)
    _ = try file.seek(100, .start);

    // Write more data - this should expand the file and zero-fill the gap
    const second_data = "WORLD";
    const bytes_written2 = try file.write(second_data);
    try testing.expectEqual(second_data.len, bytes_written2);

    // Reset file position to read entire file
    _ = try file.seek(0, .start);

    // Read entire file content
    const expected_size = 100 + second_data.len;
    var buffer = try allocator.alloc(u8, expected_size);
    const bytes_read = try file.read(buffer);
    try testing.expectEqual(expected_size, bytes_read);

    // Verify initial data
    try testing.expectEqualSlices(u8, initial_data, buffer[0..5]);

    // Verify gap is zero-filled (positions 5-99)
    for (buffer[5..100]) |byte| {
        try testing.expectEqual(@as(u8, 0), byte);
    }

    // Verify second data
    try testing.expectEqualSlices(u8, second_data, buffer[100..105]);

    std.debug.print("File expansion test passed - gap properly zero-initialized\n", .{});
}

test "vfs debug: write past end without seek" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const allocator = arena.allocator();

    var sim = try Simulation.init(allocator, 54321);
    defer sim.deinit();

    const node1 = try sim.add_node();
    const node1_ptr = sim.find_node(node1);
    const node1_vfs = node1_ptr.filesystem_interface();

    // Create a test file
    var file = try node1_vfs.create("test_append.dat");
    defer file.close();

    // Write data multiple times in sequence
    const data1 = "AAAA";
    const data2 = "BBBB";
    const data3 = "CCCC";

    _ = try file.write(data1);
    _ = try file.write(data2);
    _ = try file.write(data3);

    // Read back and verify
    _ = try file.seek(0, .start);
    var buffer = try allocator.alloc(u8, 12);
    const bytes_read = try file.read(buffer);
    try testing.expectEqual(@as(usize, 12), bytes_read);

    try testing.expectEqualSlices(u8, "AAAABBBBCCCC", buffer);

    std.debug.print("Sequential write test passed - no corruption\n", .{});
}

test "vfs debug: multiple resize operations" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const allocator = arena.allocator();

    var sim = try Simulation.init(allocator, 98765);
    defer sim.deinit();

    const node1 = try sim.add_node();
    const node1_ptr = sim.find_node(node1);
    const node1_vfs = node1_ptr.filesystem_interface();

    // Create a test file
    var file = try node1_vfs.create("test_multi_resize.dat");
    defer file.close();

    // Write at position 0
    _ = try file.write("START");

    // Write at position 50
    _ = try file.seek(50, .start);
    _ = try file.write("MID");

    // Write at position 200
    _ = try file.seek(200, .start);
    _ = try file.write("END");

    // Read entire file and verify zero-fill regions
    _ = try file.seek(0, .start);
    var buffer = try allocator.alloc(u8, 203);
    const bytes_read = try file.read(buffer);
    try testing.expectEqual(@as(usize, 203), bytes_read);

    // Verify content
    try testing.expectEqualSlices(u8, "START", buffer[0..5]);
    try testing.expectEqualSlices(u8, "MID", buffer[50..53]);
    try testing.expectEqualSlices(u8, "END", buffer[200..203]);

    // Verify zero-fill regions
    for (buffer[5..50]) |byte| {
        if (byte != 0) {
            std.debug.print("Non-zero byte found at position {}: 0x{x:0>2}\n", .{ @as(usize, @intFromPtr(&byte) - @intFromPtr(buffer.ptr)), byte });
            return error.NonZeroFillDetected;
        }
    }

    for (buffer[53..200]) |byte| {
        if (byte != 0) {
            std.debug.print("Non-zero byte found at position {}: 0x{x:0>2}\n", .{ @as(usize, @intFromPtr(&byte) - @intFromPtr(buffer.ptr)), byte });
            return error.NonZeroFillDetected;
        }
    }

    std.debug.print("Multiple resize test passed - all gaps properly zero-initialized\n", .{});
}

test "vfs debug: raw buffer inspection" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const allocator = arena.allocator();

    var sim = try Simulation.init(allocator, 11111);
    defer sim.deinit();

    const node1 = try sim.add_node();
    const node1_ptr = sim.find_node(node1);
    const node1_vfs = node1_ptr.filesystem_interface();

    // Create a test file
    var file = try node1_vfs.create("test_inspect.dat");
    defer file.close();

    // Write pattern that would be easy to spot corruption
    const pattern = "0123456789ABCDEF";
    _ = try file.write(pattern);

    // Seek forward and write more
    _ = try file.seek(32, .start);
    _ = try file.write(pattern);

    // Read entire file
    _ = try file.seek(0, .start);
    var buffer = try allocator.alloc(u8, 48);
    const bytes_read = try file.read(buffer);
    try testing.expectEqual(@as(usize, 48), bytes_read);

    // Print raw buffer for inspection
    std.debug.print("Raw file buffer (48 bytes):\n", .{});
    for (buffer, 0..) |byte, i| {
        if (i % 16 == 0) {
            std.debug.print("{x:0>4}: ", .{i});
        }
        std.debug.print("{x:0>2} ", .{byte});
        if ((i + 1) % 16 == 0) {
            std.debug.print("\n", .{});
        }
    }
    if (buffer.len % 16 != 0) {
        std.debug.print("\n", .{});
    }

    // Verify specific bytes
    try testing.expectEqualSlices(u8, pattern, buffer[0..16]);
    try testing.expectEqualSlices(u8, pattern, buffer[32..48]);

    // Check gap is zero
    for (buffer[16..32]) |byte| {
        try testing.expectEqual(@as(u8, 0), byte);
    }

    std.debug.print("Raw buffer inspection passed\n", .{});
}
