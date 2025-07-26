//! VFS Memory Safety Unit Tests
//!
//! Comprehensive test coverage for VFS memory safety and corruption detection.
//! These tests isolate VFS operations from the broader system to catch corruption
//! at the source and provide clear failure diagnostics.

const cortexdb = @import("cortexdb");
const std = @import("std");
const testing = std.testing;
const assert = cortexdb.assert.assert;

const vfs = cortexdb.vfs;
const simulation_vfs = cortexdb.simulation_vfs;

const VFS = vfs.VFS;
const VFile = vfs.VFile;
const SimulationVFS = simulation_vfs.SimulationVFS;

test "vfs memory safety: arraylist expansion corruption detection" {
    const allocator = testing.allocator;

    var sim_vfs = try SimulationVFS.init(allocator);
    defer sim_vfs.deinit();
    const test_vfs = sim_vfs.vfs();

    // Create file and write small amount first
    var file = try test_vfs.create("test_expansion.log");
    defer file.close();

    // Small write to establish initial allocation
    const small_data = "HEADER01";
    const written_small = try file.write(small_data);
    try testing.expectEqual(small_data.len, written_small);

    // Immediate read-back verification
    _ = try file.seek(0, .start);
    var read_buffer: [8]u8 = undefined;
    const read_small = try file.read(&read_buffer);
    try testing.expectEqual(small_data.len, read_small);
    try testing.expect(std.mem.eql(u8, small_data, read_buffer[0..read_small]));

    // Force ArrayList expansion with large write
    var large_buffer: [65536]u8 = undefined;
    @memset(&large_buffer, 0xAB);

    // Write pattern that should trigger reallocation
    _ = try file.seek(8, .start);
    const written_large = try file.write(&large_buffer);
    try testing.expectEqual(large_buffer.len, written_large);

    // Critical: verify original data is still intact after expansion
    _ = try file.seek(0, .start);
    var verify_buffer: [8]u8 = undefined;
    const read_verify = try file.read(&verify_buffer);
    try testing.expectEqual(8, read_verify);

    if (!std.mem.eql(u8, small_data, verify_buffer[0..8])) {
        std.debug.print("CORRUPTION DETECTED: Expected vs actual data mismatch\n", .{});
        return error.MemoryCorruption;
    }

    // Verify large data integrity
    _ = try file.seek(8, .start);
    var large_verify: [1024]u8 = undefined;
    const read_large = try file.read(&large_verify);
    try testing.expectEqual(1024, read_large);

    for (large_verify) |byte| {
        try testing.expectEqual(@as(u8, 0xAB), byte);
    }
}

test "vfs memory safety: multiple file creation stress test" {
    const allocator = testing.allocator;

    var sim_vfs = try SimulationVFS.init(allocator);
    defer sim_vfs.deinit();
    const test_vfs = sim_vfs.vfs();

    const num_files = 100;
    var files: [num_files]VFile = undefined;
    var file_data: [num_files][16]u8 = undefined;

    // Create many files to trigger file_storage reallocation
    for (0..num_files) |i| {
        const file_name = try std.fmt.allocPrint(allocator, "test_file_{}.log", .{i});
        defer allocator.free(file_name);
        files[i] = try test_vfs.create(file_name);

        // Write unique pattern to each file
        std.mem.writeInt(u64, file_data[i][0..8], @as(u64, i), .little);
        std.mem.writeInt(u64, file_data[i][8..16], @as(u64, i) ^ 0xDEADBEEF, .little);

        const written = try files[i].write(&file_data[i]);
        try testing.expectEqual(16, written);
    }

    // Verify all files retain correct data after potential reallocation
    for (0..num_files) |i| {
        _ = try files[i].seek(0, .start);
        var read_data: [16]u8 = undefined;
        const read_bytes = try files[i].read(&read_data);
        try testing.expectEqual(16, read_bytes);

        if (!std.mem.eql(u8, &file_data[i], &read_data)) {
            std.debug.print("File {} corrupted: expected 0x{X}, got 0x{X}\n", .{
                i,
                std.mem.readInt(u64, file_data[i][0..8], .little),
                std.mem.readInt(u64, read_data[0..8], .little),
            });
            return error.FileCorruption;
        }

        files[i].close();
    }
}

test "vfs memory safety: incremental growth corruption detection" {
    const allocator = testing.allocator;

    var sim_vfs = try SimulationVFS.init(allocator);
    defer sim_vfs.deinit();
    const test_vfs = sim_vfs.vfs();

    var file = try test_vfs.create("incremental_test.log");
    defer file.close();

    const write_sizes = [_]usize{ 13, 256, 1024, 4096, 16384, 65536 };
    var total_written: usize = 0;
    var checksum: u32 = 0;

    // Write in increasing chunk sizes
    for (write_sizes) |size| {
        const write_buffer = try allocator.alloc(u8, size);
        defer allocator.free(write_buffer);

        // Fill with deterministic pattern
        for (write_buffer, 0..) |*byte, i| {
            byte.* = @intCast((total_written + i) & 0xFF);
        }

        // Calculate checksum for verification
        for (write_buffer, 0..) |byte, i| {
            checksum = checksum ^ (@as(u32, byte) << @intCast((total_written + i) & 7));
        }

        const written = try file.write(write_buffer);
        try testing.expectEqual(size, written);
        total_written += written;

        // Immediate verification: read back what we just wrote
        const current_pos = try file.tell();
        _ = try file.seek(current_pos - size, .start);

        const verify_buffer = try allocator.alloc(u8, size);
        defer allocator.free(verify_buffer);
        const read_bytes = try file.read(verify_buffer);
        try testing.expectEqual(size, read_bytes);

        if (!std.mem.eql(u8, write_buffer, verify_buffer)) {
            return error.ImmediateCorruption;
        }

        // Return to end for next write
        _ = try file.seek(current_pos, .start);
    }

    // Final verification: read entire file and verify integrity
    _ = try file.seek(0, .start);
    const file_size = try file.file_size();
    try testing.expectEqual(total_written, file_size);

    const full_read_buffer = try allocator.alloc(u8, total_written);
    defer allocator.free(full_read_buffer);
    const full_read = try file.read(full_read_buffer);
    try testing.expectEqual(total_written, full_read);

    // Verify content matches expected pattern
    var verify_checksum: u32 = 0;
    for (full_read_buffer, 0..) |byte, i| {
        const expected = @as(u8, @intCast(i & 0xFF));
        if (byte != expected) {
            std.debug.print("Content corruption at offset {}: expected 0x{X}, got 0x{X}\n", .{ i, expected, byte });
            return error.ContentCorruption;
        }
        verify_checksum = verify_checksum ^ (@as(u32, byte) << @intCast(i & 7));
    }

    try testing.expectEqual(checksum, verify_checksum);
}

test "vfs memory safety: sparse file gap handling" {
    const allocator = testing.allocator;

    var sim_vfs = try SimulationVFS.init(allocator);
    defer sim_vfs.deinit();
    const test_vfs = sim_vfs.vfs();

    var file = try test_vfs.create("sparse_test.log");
    defer file.close();

    // Write at position 0
    const header = "HEADER01";
    const written_header = try file.write(header);
    try testing.expectEqual(header.len, written_header);

    // Seek far ahead and write (creates gap)
    _ = try file.seek(4096, .start);
    const footer = "FOOTER01";
    const written_footer = try file.write(footer);
    try testing.expectEqual(footer.len, written_footer);

    // Verify file size
    const file_size = try file.file_size();
    try testing.expectEqual(4096 + footer.len, file_size);

    // Verify header is intact
    _ = try file.seek(0, .start);
    var header_buffer: [8]u8 = undefined;
    const read_header = try file.read(&header_buffer);
    try testing.expectEqual(header.len, read_header);
    try testing.expect(std.mem.eql(u8, header, header_buffer[0..read_header]));

    // Verify gap is zeroed
    _ = try file.seek(header.len, .start);
    var gap_buffer: [100]u8 = undefined;
    const read_gap = try file.read(&gap_buffer);
    try testing.expectEqual(100, read_gap);

    for (gap_buffer) |byte| {
        try testing.expectEqual(@as(u8, 0), byte);
    }

    // Verify footer is intact
    _ = try file.seek(4096, .start);
    var footer_buffer: [8]u8 = undefined;
    const read_footer = try file.read(&footer_buffer);
    try testing.expectEqual(footer.len, read_footer);
    try testing.expect(std.mem.eql(u8, footer, footer_buffer[0..read_footer]));
}

test "vfs memory safety: capacity boundary write" {
    const allocator = testing.allocator;

    var sim_vfs = try SimulationVFS.init(allocator);
    defer sim_vfs.deinit();
    const test_vfs = sim_vfs.vfs();

    var file = try test_vfs.create("boundary_test.log");
    defer file.close();

    // Write data that will likely trigger capacity growth
    const sizes = [_]usize{ 127, 128, 129, 255, 256, 257, 511, 512, 513, 1023, 1024, 1025 };

    for (sizes) |size| {
        const write_buffer = try allocator.alloc(u8, size);
        defer allocator.free(write_buffer);
        @memset(write_buffer, @intCast(size & 0xFF));

        const written = try file.write(write_buffer);
        try testing.expectEqual(size, written);

        // Immediate read-back verification
        const pos = try file.tell();
        _ = try file.seek(pos - size, .start);

        const read_buffer = try allocator.alloc(u8, size);
        defer allocator.free(read_buffer);
        const read_bytes = try file.read(read_buffer);
        try testing.expectEqual(size, read_bytes);

        if (!std.mem.eql(u8, write_buffer, read_buffer)) {
            std.debug.print("Boundary corruption at size {}\n", .{size});
            return error.BoundaryCorruption;
        }

        _ = try file.seek(pos, .start);
    }
}

test "vfs memory safety: handle stability during expansion" {
    const allocator = testing.allocator;

    var sim_vfs = try SimulationVFS.init(allocator);
    defer sim_vfs.deinit();
    const test_vfs = sim_vfs.vfs();

    // Create multiple files and keep handles open
    var file1 = try test_vfs.create("handle_test_1.log");
    defer file1.close();

    var file2 = try test_vfs.create("handle_test_2.log");
    defer file2.close();

    // Write initial data
    const data1 = "FILE1DATA";
    const data2 = "FILE2DATA";

    _ = try file1.write(data1);
    _ = try file2.write(data2);

    // Create many more files to potentially trigger file_storage reallocation
    var temp_files: [50]VFile = undefined;
    for (0..50) |i| {
        const name = try std.fmt.allocPrint(allocator, "temp_{}.log", .{i});
        defer allocator.free(name);
        temp_files[i] = try test_vfs.create(name);
        _ = try temp_files[i].write("TEMP");
    }

    // Verify original files are still accessible with correct data
    _ = try file1.seek(0, .start);
    var buffer1: [9]u8 = undefined;
    const read1 = try file1.read(&buffer1);
    try testing.expectEqual(data1.len, read1);
    try testing.expect(std.mem.eql(u8, data1, buffer1[0..read1]));

    _ = try file2.seek(0, .start);
    var buffer2: [9]u8 = undefined;
    const read2 = try file2.read(&buffer2);
    try testing.expectEqual(data2.len, read2);
    try testing.expect(std.mem.eql(u8, data2, buffer2[0..read2]));

    // Clean up temp files
    for (&temp_files) |*temp_file| {
        temp_file.close();
    }
}

test "vfs memory safety: comprehensive checksum validation" {
    const allocator = testing.allocator;

    var sim_vfs = try SimulationVFS.init(allocator);
    defer sim_vfs.deinit();
    const test_vfs = sim_vfs.vfs();

    var file = try test_vfs.create("checksum_test.log");
    defer file.close();

    const test_data = "CortexDB deterministic checksum test data pattern 123456789";
    const expected_checksum = blk: {
        var hasher = std.hash.Crc32.init();
        hasher.update(test_data);
        break :blk hasher.final();
    };

    // Write test data
    const written = try file.write(test_data);
    try testing.expectEqual(test_data.len, written);

    // Multiple read-back verifications with checksum
    for (0..10) |iteration| {
        _ = try file.seek(0, .start);

        var read_buffer: [test_data.len]u8 = undefined;
        const read_bytes = try file.read(&read_buffer);
        try testing.expectEqual(test_data.len, read_bytes);

        // Verify content
        if (!std.mem.eql(u8, test_data, &read_buffer)) {
            std.debug.print("Content mismatch on iteration {}\n", .{iteration});
            return error.ContentMismatch;
        }

        // Verify checksum
        var hasher = std.hash.Crc32.init();
        hasher.update(&read_buffer);
        const actual_checksum = hasher.final();

        if (expected_checksum != actual_checksum) {
            std.debug.print("Checksum mismatch on iteration {}: expected 0x{X}, got 0x{X}\n", .{ iteration, expected_checksum, actual_checksum });
            return error.ChecksumMismatch;
        }
    }
}
