//! Deserialization Fault Injection Tests
//!
//! Testing of deserialization robustness under various
//! corruption scenarios. Validates that corrupted data fails gracefully
//! without memory corruption or crashes.

const kausaldb = @import("kausaldb");
const std = @import("std");
const testing = std.testing;

const context_block = kausaldb.types;
const simulation_vfs = kausaldb.simulation_vfs;

const ContextBlock = context_block.ContextBlock;
const BlockId = context_block.BlockId;
const GraphEdge = context_block.GraphEdge;
const EdgeType = context_block.EdgeType;
const SimulationVFS = simulation_vfs.SimulationVFS;

// Helper to create valid serialized ContextBlock for corruption testing
fn create_valid_serialized_block(allocator: std.mem.Allocator) ![]u8 {
    const test_block = ContextBlock{
        .id = BlockId.from_string("test-block-12345"),
        .version = 1,
        .source_uri = "test://source.zig",
        .metadata_json = "{\"type\":\"function\"}",
        .content = "pub fn test() void {}",
    };

    const size = test_block.serialized_size();
    const buffer = try allocator.alloc(u8, size);
    _ = try test_block.serialize(buffer);
    return buffer;
}

// Helper to create valid serialized GraphEdge for corruption testing
fn create_valid_serialized_edge() ![GraphEdge.SERIALIZED_SIZE]u8 {
    const test_edge = GraphEdge{
        .source_id = BlockId.from_string("source-block-123"),
        .target_id = BlockId.from_string("target-block-456"),
        .edge_type = EdgeType.imports,
    };

    var buffer: [GraphEdge.SERIALIZED_SIZE]u8 = undefined;
    _ = try test_edge.serialize(&buffer);
    return buffer;
}

test "fault injection - contextblock header magic corruption" {
    const allocator = testing.allocator;

    const buffer = try create_valid_serialized_block(allocator);
    defer allocator.free(buffer);

    // Corrupt magic number in header
    var corrupted_buffer = try allocator.dupe(u8, buffer);
    defer allocator.free(corrupted_buffer);

    std.mem.writeInt(u32, corrupted_buffer[0..4], 0xDEADBEEF, .little);

    // Deserialization should fail gracefully with InvalidMagic
    const result = ContextBlock.deserialize(corrupted_buffer, allocator);
    try testing.expectError(error.InvalidMagic, result);
}

test "fault injection - contextblock version corruption" {
    const allocator = testing.allocator;

    const buffer = try create_valid_serialized_block(allocator);
    defer allocator.free(buffer);

    var corrupted_buffer = try allocator.dupe(u8, buffer);
    defer allocator.free(corrupted_buffer);

    // Corrupt version field to unsupported value
    std.mem.writeInt(u16, corrupted_buffer[4..6], 9999, .little);

    const result = ContextBlock.deserialize(corrupted_buffer, allocator);
    try testing.expectError(error.UnsupportedVersion, result);
}

test "fault injection - contextblock length field overflow" {
    const allocator = testing.allocator;

    const buffer = try create_valid_serialized_block(allocator);
    defer allocator.free(buffer);

    var corrupted_buffer = try allocator.dupe(u8, buffer);
    defer allocator.free(corrupted_buffer);

    // Corrupt content_len to cause integer overflow
    const content_len_offset = 32; // After magic, version, flags, id, block_version, source_uri_len, metadata_json_len
    std.mem.writeInt(u64, corrupted_buffer[content_len_offset .. content_len_offset + 8], std.math.maxInt(u64), .little);

    const result = ContextBlock.deserialize(corrupted_buffer, allocator);
    try testing.expectError(error.IncompleteData, result);
}

test "fault injection - contextblock checksum validation" {
    const allocator = testing.allocator;

    const buffer = try create_valid_serialized_block(allocator);
    defer allocator.free(buffer);

    var corrupted_buffer = try allocator.dupe(u8, buffer);
    defer allocator.free(corrupted_buffer);

    // Corrupt checksum field
    const checksum_offset = 40; // Near end of header
    std.mem.writeInt(u32, corrupted_buffer[checksum_offset .. checksum_offset + 4], 0x12345678, .little);

    const result = ContextBlock.deserialize(corrupted_buffer, allocator);
    try testing.expectError(error.InvalidChecksum, result);
}

test "fault injection - contextblock truncated buffer" {
    const allocator = testing.allocator;

    const buffer = try create_valid_serialized_block(allocator);
    defer allocator.free(buffer);

    // Test various truncation points
    const truncation_points = [_]usize{ 0, 10, 30, buffer.len / 2 };

    for (truncation_points) |truncate_at| {
        if (truncate_at >= buffer.len) continue;

        const truncated = buffer[0..truncate_at];
        const result = ContextBlock.deserialize(truncated, allocator);

        // Should fail with either BufferTooSmall or IncompleteData
        const is_expected_error =
            std.mem.eql(u8, @errorName(result), "BufferTooSmall") or
            std.mem.eql(u8, @errorName(result), "IncompleteData");
        try testing.expect(is_expected_error);
    }
}

test "fault injection - graphedge corrupted data" {
    var buffer = try create_valid_serialized_edge();

    // Corrupt source_id bytes
    @memset(buffer[0..16], 0xFF);

    // Deserialization should still work (BlockId accepts any 16 bytes)
    // but let's corrupt the edge type to invalid value
    std.mem.writeInt(u16, buffer[32..34], 9999, .little);

    const result = GraphEdge.deserialize(&buffer);
    try testing.expectError(error.InvalidEdgeType, result);
}

test "fault injection - graphedge truncated buffer" {
    const buffer = try create_valid_serialized_edge();

    // Test truncated buffers at various points
    const truncation_points = [_]usize{ 0, 5, 10, 15, 20, 30 };

    for (truncation_points) |truncate_at| {
        const truncated = buffer[0..truncate_at];
        const result = GraphEdge.deserialize(truncated);
        try testing.expectError(error.BufferTooSmall, result);
    }
}

test "fault injection - random bit flips in contextblock" {
    const allocator = testing.allocator;

    const buffer = try create_valid_serialized_block(allocator);
    defer allocator.free(buffer);

    var sim_vfs = SimulationVFS.init_with_fault_seed(allocator, 12345);
    defer sim_vfs.deinit();

    // Enable read corruption to simulate bit flips
    sim_vfs.enable_read_corruption(100, 3); // High probability, up to 3 bits

    // Test multiple rounds of random corruption
    var successful_corruptions: u32 = 0;
    const max_attempts = 50;

    for (0..max_attempts) |_| {
        const corrupted_buffer = try allocator.dupe(u8, buffer);
        defer allocator.free(corrupted_buffer);

        // Apply random corruption similar to SimulationVFS
        sim_vfs.fault_injection.apply_read_corruption(corrupted_buffer);

        // Attempt deserialization - should either succeed or fail gracefully
        if (ContextBlock.deserialize(corrupted_buffer, allocator)) |deserialized| {
            deserialized.deinit(allocator);
            // Successful parse despite corruption is OK (corruption might not affect critical fields)
        } else |err| {
            // Expected errors from corruption
            const expected_errors = [_]anyerror{
                error.InvalidMagic,
                error.UnsupportedVersion,
                error.InvalidChecksum,
                error.BufferTooSmall,
                error.IncompleteData,
                error.InvalidReservedBytes,
                error.OutOfMemory,
            };

            var is_expected = false;
            for (expected_errors) |expected| {
                if (err == expected) {
                    is_expected = true;
                    successful_corruptions += 1;
                    break;
                }
            }

            if (!is_expected) {
                std.debug.print("Unexpected error from corruption: {}\n", .{err});
                return err;
            }
        }
    }

    // We should have successfully corrupted and detected some blocks
    try testing.expect(successful_corruptions > 0);
}

test "fault injection - extreme length values" {
    const allocator = testing.allocator;

    // Create minimal valid header with extreme length values
    var header_buffer: [ContextBlock.BlockHeader.SIZE]u8 = undefined;
    @memset(&header_buffer, 0);

    // Write valid magic and version
    std.mem.writeInt(u32, header_buffer[0..4], ContextBlock.MAGIC, .little);
    std.mem.writeInt(u16, header_buffer[4..6], ContextBlock.FORMAT_VERSION, .little);

    // Test extreme values that should cause overflow detection
    const extreme_values = [_]u64{
        std.math.maxInt(u32),
        std.math.maxInt(u64),
        std.math.maxInt(u64) - 1,
    };

    for (extreme_values) |extreme_len| {
        var test_buffer = header_buffer;

        // Set content_len to extreme value
        const content_len_offset = 32;
        std.mem.writeInt(u64, test_buffer[content_len_offset .. content_len_offset + 8], extreme_len, .little);

        const result = ContextBlock.deserialize(&test_buffer, allocator);
        try testing.expectError(error.IncompleteData, result);
    }
}

test "fault injection - concurrent corruption scenarios" {
    const allocator = testing.allocator;

    const buffer = try create_valid_serialized_block(allocator);
    defer allocator.free(buffer);

    // Simulate concurrent corruption scenarios that might occur
    // during power loss or disk corruption
    var prng = std.Random.DefaultPrng.init(99999);
    const random = prng.random();

    const corruption_scenarios = 20;
    var graceful_failures: u32 = 0;

    for (0..corruption_scenarios) |_| {
        var corrupted_buffer = try allocator.dupe(u8, buffer);
        defer allocator.free(corrupted_buffer);

        // Apply multiple types of corruption simultaneously
        const corruption_count = random.intRangeAtMost(u8, 1, 5);
        for (0..corruption_count) |_| {
            const corruption_type = random.intRangeAtMost(u8, 0, 3);
            switch (corruption_type) {
                0 => {
                    // Random byte corruption
                    const byte_index = random.intRangeAtMost(usize, 0, corrupted_buffer.len - 1);
                    corrupted_buffer[byte_index] = random.int(u8);
                },
                1 => {
                    // Zero out section
                    const start = random.intRangeAtMost(usize, 0, corrupted_buffer.len - 1);
                    const len = random.intRangeAtMost(usize, 1, @min(8, corrupted_buffer.len - start));
                    @memset(corrupted_buffer[start .. start + len], 0);
                },
                2 => {
                    // Fill section with 0xFF
                    const start = random.intRangeAtMost(usize, 0, corrupted_buffer.len - 1);
                    const len = random.intRangeAtMost(usize, 1, @min(8, corrupted_buffer.len - start));
                    @memset(corrupted_buffer[start .. start + len], 0xFF);
                },
                3 => {
                    // Bit flip
                    const byte_index = random.intRangeAtMost(usize, 0, corrupted_buffer.len - 1);
                    const bit_pos = random.intRangeAtMost(u3, 0, 7);
                    corrupted_buffer[byte_index] ^= (@as(u8, 1) << bit_pos);
                },
            }
        }

        // Deserialization must not crash or corrupt memory
        if (ContextBlock.deserialize(corrupted_buffer, allocator)) |deserialized| {
            deserialized.deinit(allocator);
        } else |_| {
            graceful_failures += 1;
        }
    }

    // Most corruption should result in graceful failures
    try testing.expect(graceful_failures > corruption_scenarios / 2);
}
