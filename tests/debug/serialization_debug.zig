//! Minimal serialization debugging test to isolate corruption sources.
//!
//! This test isolates ContextBlock serialization/deserialization from the
//! broader storage engine to identify if corruption occurs at the data
//! structure level or in higher-level systems.

const cortexdb = @import("cortexdb");
const std = @import("std");
const testing = std.testing;
const assert = cortexdb.assert.assert;

const context_block = cortexdb.types;
const ContextBlock = context_block.ContextBlock;
const BlockId = context_block.BlockId;

test "serialization debug: minimal block roundtrip" {
    // Use GPA with safety to detect corruption
    var gpa = std.heap.GeneralPurposeAllocator(.{ .safety = true }){};
    defer {
        const deinit_status = gpa.deinit();
        if (deinit_status == .leak) @panic("Memory leak detected in serialization test");
    }
    const allocator = gpa.allocator();

    std.debug.print("=== Serialization Debug Test ===\n", .{});

    // Create minimal test block with clean, known content
    const test_block = ContextBlock{
        .id = try BlockId.from_hex("deadbeefcafebabe1234567890abcdef"),
        .version = 42,
        .source_uri = "test://clean.zig",
        .metadata_json = "{\"test\":\"clean\"}",
        .content = "Clean test content without corruption patterns",
    };

    std.debug.print("Original block:\n", .{});
    std.debug.print("  ID: deadbeefcafebabe1234567890abcdef\n", .{});
    std.debug.print("  Version: {}\n", .{test_block.version});
    std.debug.print("  Source URI: {s}\n", .{test_block.source_uri});
    std.debug.print("  Metadata: {s}\n", .{test_block.metadata_json});
    std.debug.print("  Content: {s}\n", .{test_block.content});

    // Calculate required buffer size
    const required_size = test_block.serialized_size();
    std.debug.print("Required serialization buffer size: {} bytes\n", .{required_size});

    // Allocate buffer and verify it's clean
    const buffer = try allocator.alloc(u8, required_size);
    defer allocator.free(buffer);

    // Zero-initialize buffer to ensure clean start
    @memset(buffer, 0);

    // Verify buffer is actually zero-initialized
    for (buffer, 0..) |byte, i| {
        if (byte != 0) {
            std.debug.print("ERROR: Buffer not zero-initialized at offset {}: 0x{x:0>2}\n", .{ i, byte });
            return error.BufferNotZeroInitialized;
        }
    }
    std.debug.print("Buffer zero-initialization verified\n", .{});

    // Serialize the block
    const bytes_written = try test_block.serialize(buffer);
    std.debug.print("Bytes written during serialization: {}\n", .{bytes_written});

    if (bytes_written != required_size) {
        std.debug.print("ERROR: Size mismatch - expected {}, got {}\n", .{ required_size, bytes_written });
        return error.SerializationSizeMismatch;
    }

    // Dump the serialized buffer for analysis
    std.debug.print("Serialized buffer (first 128 bytes):\n", .{});
    const dump_size = @min(128, buffer.len);
    for (buffer[0..dump_size], 0..) |byte, i| {
        if (i % 16 == 0) {
            std.debug.print("{x:0>4}: ", .{i});
        }
        std.debug.print("{x:0>2} ", .{byte});
        if ((i + 1) % 16 == 0) {
            std.debug.print("| ");
            // Print ASCII representation
            for (buffer[i - 15 .. i + 1]) |ascii_byte| {
                const c = if (ascii_byte >= 32 and ascii_byte <= 126) ascii_byte else '.';
                std.debug.print("{c}", .{c});
            }
            std.debug.print("\n", .{});
        }
    }
    if (dump_size % 16 != 0) {
        std.debug.print("\n", .{});
    }

    // Check for corruption patterns in the buffer
    var corruption_detected = false;
    const corruption_patterns = [_]u8{ 'X', 'x', 'U', 'u', 0xAA, 0xBB, 0xCC, 0xFF };

    for (buffer, 0..) |byte, i| {
        for (corruption_patterns) |pattern| {
            if (byte == pattern) {
                // Only report as corruption if it's in the header area
                // Content area may legitimately contain these bytes
                if (i < ContextBlock.BlockHeader.SIZE) {
                    std.debug.print("WARNING: Potential corruption pattern 0x{x:0>2} at header offset {}\n", .{ pattern, i });
                    corruption_detected = true;
                }
            }
        }
    }

    if (!corruption_detected) {
        std.debug.print("No corruption patterns detected in header\n", .{});
    }

    // Attempt deserialization
    std.debug.print("Attempting deserialization...\n", .{});
    const deserialized = try ContextBlock.deserialize(buffer, allocator);
    defer deserialized.deinit(allocator);

    std.debug.print("Deserialization successful\n", .{});

    // Verify roundtrip integrity
    try testing.expect(test_block.id.eql(deserialized.id));
    try testing.expectEqual(test_block.version, deserialized.version);
    try testing.expectEqualStrings(test_block.source_uri, deserialized.source_uri);
    try testing.expectEqualStrings(test_block.metadata_json, deserialized.metadata_json);
    try testing.expectEqualStrings(test_block.content, deserialized.content);

    std.debug.print("Roundtrip verification passed\n", .{});
    std.debug.print("=== Serialization Debug Test PASSED ===\n", .{});
}

test "serialization debug: buffer boundary analysis" {
    var gpa = std.heap.GeneralPurposeAllocator(.{ .safety = true }){};
    defer {
        const deinit_status = gpa.deinit();
        if (deinit_status == .leak) @panic("Memory leak detected in boundary test");
    }
    const allocator = gpa.allocator();

    std.debug.print("=== Buffer Boundary Analysis ===\n", .{});

    // Create a block with known content sizes
    const test_block = ContextBlock{
        .id = try BlockId.from_hex("1111111111111111222222222222222"),
        .version = 1,
        .source_uri = "12345", // 5 bytes
        .metadata_json = "{}", // 2 bytes
        .content = "ABCDE", // 5 bytes
    };

    const required_size = test_block.serialized_size();
    std.debug.print("Block component sizes:\n", .{});
    std.debug.print("  Header: {} bytes\n", .{ContextBlock.BlockHeader.SIZE});
    std.debug.print("  Source URI: {} bytes\n", .{test_block.source_uri.len});
    std.debug.print("  Metadata: {} bytes\n", .{test_block.metadata_json.len});
    std.debug.print("  Content: {} bytes\n", .{test_block.content.len});
    std.debug.print("  Total required: {} bytes\n", .{required_size});

    // Allocate buffer with extra space to detect overruns
    const buffer_size = required_size + 64;
    const buffer = try allocator.alloc(u8, buffer_size);
    defer allocator.free(buffer);

    // Fill entire buffer with canary pattern
    @memset(buffer, 0xCA);

    // Serialize into beginning of buffer
    const bytes_written = try test_block.serialize(buffer[0..required_size]);
    std.debug.print("Serialization wrote {} bytes\n", .{bytes_written});

    // Check for buffer overruns
    var overrun_detected = false;
    for (buffer[required_size..], required_size..) |byte, i| {
        if (byte != 0xCA) {
            std.debug.print("ERROR: Buffer overrun detected at offset {}: expected 0xCA, got 0x{x:0>2}\n", .{ i, byte });
            overrun_detected = true;
        }
    }

    if (!overrun_detected) {
        std.debug.print("No buffer overrun detected\n", .{});
    }

    // Verify exact content boundaries
    std.debug.print("Boundary verification:\n", .{});

    // Header should be exactly 64 bytes
    if (bytes_written < ContextBlock.BlockHeader.SIZE) {
        std.debug.print("ERROR: Serialization too short for header\n", .{});
        return error.SerializationTooShort;
    }

    // Check that we can deserialize successfully
    const deserialized = try ContextBlock.deserialize(buffer[0..required_size], allocator);
    defer deserialized.deinit(allocator);

    try testing.expectEqualStrings(test_block.source_uri, deserialized.source_uri);
    try testing.expectEqualStrings(test_block.metadata_json, deserialized.metadata_json);
    try testing.expectEqualStrings(test_block.content, deserialized.content);

    std.debug.print("Boundary analysis passed\n", .{});
}

test "serialization debug: corrupted buffer detection" {
    var gpa = std.heap.GeneralPurposeAllocator(.{ .safety = true }){};
    defer {
        const deinit_status = gpa.deinit();
        if (deinit_status == .leak) @panic("Memory leak detected in corruption test");
    }
    const allocator = gpa.allocator();

    std.debug.print("=== Corrupted Buffer Detection ===\n", .{});

    const test_block = ContextBlock{
        .id = try BlockId.from_hex("ccccccccccccccccdddddddddddddddd"),
        .version = 999,
        .source_uri = "corruption_test",
        .metadata_json = "{\"corrupted\":false}",
        .content = "Original clean content",
    };

    const required_size = test_block.serialized_size();
    const clean_buffer = try allocator.alloc(u8, required_size);
    defer allocator.free(clean_buffer);

    // Serialize to clean buffer
    _ = try test_block.serialize(clean_buffer);

    // Create corrupted version
    const corrupted_buffer = try allocator.dupe(u8, clean_buffer);
    defer allocator.free(corrupted_buffer);

    // Introduce corruption patterns at various offsets
    corrupted_buffer[10] = 'X'; // Header corruption
    corrupted_buffer[20] = 'X';
    corrupted_buffer[30] = 'X';
    corrupted_buffer[40] = 'X';

    std.debug.print("Introduced X corruption at offsets 10, 20, 30, 40\n", .{});

    // Try to deserialize corrupted buffer
    std.debug.print("Attempting to deserialize corrupted buffer...\n", .{});

    const result = ContextBlock.deserialize(corrupted_buffer, allocator);
    if (result) |deserialized| {
        defer deserialized.deinit(allocator);
        std.debug.print("WARNING: Corruption not detected during deserialization\n", .{});

        // Print what we got back
        std.debug.print("Deserialized content: '{s}'\n", .{deserialized.content});
    } else |err| {
        std.debug.print("Corruption correctly detected: {}\n", .{err});
    }

    std.debug.print("Corruption detection test completed\n", .{});
}
