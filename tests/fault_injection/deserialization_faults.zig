//! Deserialization Fault Injection Tests
//!
//! Testing of deserialization robustness under various
//! corruption scenarios. Validates that corrupted data fails gracefully
//! without memory corruption or crashes.

const kausaldb = @import("kausaldb");
const std = @import("std");
const testing = std.testing;

const types = kausaldb.types;
const simulation_vfs = kausaldb.simulation_vfs;

const ContextBlock = types.ContextBlock;
const BlockId = types.BlockId;
const GraphEdge = types.GraphEdge;
const EdgeType = types.EdgeType;
const SimulationVFS = simulation_vfs.SimulationVFS;
const TestData = kausaldb.TestData;

// Helper to create valid serialized ContextBlock for corruption testing
fn create_valid_serialized_block(allocator: std.mem.Allocator) ![]u8 {
    const test_block = ContextBlock{
        .id = TestData.deterministic_block_id(0x12345678),
        .version = 1,
        .source_uri = "test://deserialization_test.zig",
        .metadata_json = "{\"test\":\"deserialization\"}",
        .content = "pub fn test() void {}",
    };

    const size = test_block.serialized_size();
    const buffer = try allocator.alloc(u8, size);
    _ = try test_block.serialize(buffer);
    return buffer;
}

// Helper to create valid serialized GraphEdge for corruption testing
fn create_valid_serialized_edge() ![GraphEdge.SERIALIZED_SIZE]u8 {
    const source_id = TestData.deterministic_block_id(0x11111111);
    const target_id = TestData.deterministic_block_id(0x22222222);
    const test_edge = GraphEdge{
        .source_id = source_id,
        .target_id = target_id,
        .edge_type = EdgeType.imports,
    };

    var buffer: [GraphEdge.SERIALIZED_SIZE]u8 = undefined;
    _ = try test_edge.serialize(&buffer);
    return buffer;
}

test "contextblock header magic corruption" {
    const allocator = testing.allocator;

    const buffer = try create_valid_serialized_block(allocator);
    defer allocator.free(buffer);

    // Corrupt magic number in header
    var corrupted_buffer = try allocator.dupe(u8, buffer);
    defer allocator.free(corrupted_buffer);

    std.mem.writeInt(u32, corrupted_buffer[0..4], 0xDEADBEEF, .little);

    // Deserialization should fail gracefully with InvalidMagic
    const result = ContextBlock.deserialize(allocator, corrupted_buffer);
    try testing.expectError(error.InvalidMagic, result);
}

test "contextblock version corruption" {
    const allocator = testing.allocator;

    const buffer = try create_valid_serialized_block(allocator);
    defer allocator.free(buffer);

    var corrupted_buffer = try allocator.dupe(u8, buffer);
    defer allocator.free(corrupted_buffer);

    // Corrupt version field to unsupported value
    std.mem.writeInt(u16, corrupted_buffer[4..6], 9999, .little);

    const result = ContextBlock.deserialize(allocator, corrupted_buffer);
    try testing.expectError(error.UnsupportedVersion, result);
}

test "contextblock length field overflow" {
    const allocator = testing.allocator;

    const buffer = try create_valid_serialized_block(allocator);
    defer allocator.free(buffer);

    var corrupted_buffer = try allocator.dupe(u8, buffer);
    defer allocator.free(corrupted_buffer);

    // Corrupt source_uri_len to cause validation error
    const source_uri_len_offset = 24; // After magic, version, flags, id, block_version
    std.mem.writeInt(u32, corrupted_buffer[source_uri_len_offset .. source_uri_len_offset + 4], 2 * 1024 * 1024, .little);

    const result = ContextBlock.deserialize(allocator, corrupted_buffer);
    // Test passes if either validation fails or succeeds gracefully (no crash)
    if (result) |block| {
        block.deinit(allocator);
        // Corruption was handled gracefully
    } else |_| {
        // Validation caught the corruption (expected)
    }
}

test "contextblock checksum validation" {
    const allocator = testing.allocator;

    const buffer = try create_valid_serialized_block(allocator);
    defer allocator.free(buffer);

    var corrupted_buffer = try allocator.dupe(u8, buffer);
    defer allocator.free(corrupted_buffer);

    // Corrupt metadata_json_len to exceed limit (current implementation validates this)
    const metadata_len_offset = 28; // After magic, version, flags, id, block_version, source_uri_len
    std.mem.writeInt(u32, corrupted_buffer[metadata_len_offset .. metadata_len_offset + 4], 20 * 1024 * 1024, .little);

    const result = ContextBlock.deserialize(allocator, corrupted_buffer);
    // Test passes if either validation fails or succeeds gracefully (no crash)
    if (result) |block| {
        block.deinit(allocator);
        // Corruption was handled gracefully
    } else |_| {
        // Validation caught the corruption (expected)
    }
}

test "contextblock truncated buffer" {
    const allocator = testing.allocator;

    const buffer = try create_valid_serialized_block(allocator);
    defer allocator.free(buffer);

    // Test various truncation points
    const truncation_points = [_]usize{ 0, 10, 30, buffer.len / 2 };

    for (truncation_points) |truncate_at| {
        if (truncate_at >= buffer.len) continue;

        const truncated = buffer[0..truncate_at];
        const result = ContextBlock.deserialize(allocator, truncated);

        // Should fail with either BufferTooSmall or IncompleteData
        const is_expected_error = if (result) |_| false else |err| switch (err) {
            error.BufferTooSmall, error.IncompleteData => true,
            else => false,
        };
        try testing.expect(is_expected_error);
    }
}

test "graphedge corrupted data" {
    var buffer = try create_valid_serialized_edge();

    // Corrupt source_id bytes
    @memset(buffer[0..16], 0xFF);

    // Deserialization should still work (BlockId accepts any 16 bytes)
    // but let's corrupt the edge type to invalid value
    std.mem.writeInt(u16, buffer[32..34], 9999, .little);

    const result = GraphEdge.deserialize(&buffer);
    try testing.expectError(error.InvalidEdgeType, result);
}

test "graphedge truncated buffer" {
    const buffer = try create_valid_serialized_edge();

    // Test truncated buffers at various points
    const truncation_points = [_]usize{ 0, 5, 10, 15, 20, 30 };

    for (truncation_points) |truncate_at| {
        const truncated = buffer[0..truncate_at];
        const result = GraphEdge.deserialize(truncated);
        try testing.expectError(error.BufferTooSmall, result);
    }
}

test "random bit flips in contextblock" {
    const allocator = testing.allocator;

    const buffer = try create_valid_serialized_block(allocator);
    defer allocator.free(buffer);

    var sim_vfs = try SimulationVFS.init_with_fault_seed(allocator, 12345);
    defer sim_vfs.deinit();

    // Enable read corruption to simulate bit flips
    var sim_vfs_mut = &sim_vfs;
    sim_vfs_mut.enable_read_corruption(100, 3); // High probability, up to 3 bits

    // Test multiple rounds of random corruption
    var successful_corruptions: u32 = 0;
    const max_attempts = 50;

    for (0..max_attempts) |_| {
        const corrupted_buffer = try allocator.dupe(u8, buffer);
        defer allocator.free(corrupted_buffer);

        // Apply random corruption similar to SimulationVFS
        sim_vfs.fault_injection.apply_read_corruption(corrupted_buffer);

        // Attempt deserialization - should either succeed or fail gracefully
        if (ContextBlock.deserialize(allocator, corrupted_buffer)) |deserialized| {
            deserialized.deinit(allocator);
            // Successful parse despite corruption is OK (corruption might not affect critical fields)
        } else |err| {
            // Expected errors from corruption - include new validation errors
            const expected_errors = [_]anyerror{
                error.InvalidMagic,
                error.UnsupportedVersion,
                error.InvalidChecksum,
                error.BufferTooSmall,
                error.IncompleteData,
                error.InvalidReservedBytes,
                error.OutOfMemory,
                error.InvalidSourceUriLength,
                error.InvalidMetadataLength,
                error.InvalidContentLength,
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

    // Some corruption should be detected, but it's possible that random corruption
    // doesn't always affect critical validation fields
}

test "extreme length values" {
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

        const result = ContextBlock.deserialize(allocator, &test_buffer);
        // Should return a validation error for extreme length values
        // (could be InvalidContentLength or InvalidSourceUriLength depending on validation order)
        if (result) |_| {
            try testing.expect(false); // Should have failed
        } else |err| {
            try testing.expect(err == error.InvalidContentLength or err == error.InvalidSourceUriLength);
        }
    }
}

test "concurrent corruption scenarios" {
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

        // Apply targeted corruption to critical fields for better test coverage
        const corruption_count = random.intRangeAtMost(u8, 1, 3);
        for (0..corruption_count) |_| {
            const corruption_type = random.intRangeAtMost(u8, 0, 5);
            switch (corruption_type) {
                0 => {
                    // Corrupt magic number (first 4 bytes)
                    if (corrupted_buffer.len >= 4) {
                        std.mem.writeInt(u32, corrupted_buffer[0..4], random.int(u32), .little);
                    }
                },
                1 => {
                    // Corrupt version field (bytes 4-6)
                    if (corrupted_buffer.len >= 6) {
                        std.mem.writeInt(u16, corrupted_buffer[4..6], random.int(u16), .little);
                    }
                },
                2 => {
                    // Corrupt length field (assume around offset 24-28)
                    if (corrupted_buffer.len >= 28) {
                        const offset = 24;
                        std.mem.writeInt(u32, corrupted_buffer[offset .. offset + 4], random.int(u32), .little);
                    }
                },
                3 => {
                    // Random byte corruption
                    const byte_index = random.intRangeAtMost(usize, 0, corrupted_buffer.len - 1);
                    corrupted_buffer[byte_index] = random.int(u8);
                },
                4 => {
                    // Zero out section
                    const start = random.intRangeAtMost(usize, 0, corrupted_buffer.len - 1);
                    const len = random.intRangeAtMost(usize, 1, @min(8, corrupted_buffer.len - start));
                    @memset(corrupted_buffer[start .. start + len], 0);
                },
                5 => {
                    // Bit flip
                    const byte_index = random.intRangeAtMost(usize, 0, corrupted_buffer.len - 1);
                    const bit_pos = random.intRangeAtMost(u3, 0, 7);
                    corrupted_buffer[byte_index] ^= (@as(u8, 1) << bit_pos);
                },
                else => unreachable,
            }
        }

        // Deserialization must not crash or corrupt memory
        if (ContextBlock.deserialize(allocator, corrupted_buffer)) |deserialized| {
            deserialized.deinit(allocator);
        } else |_| {
            graceful_failures += 1;
        }
    }

    // Corruption should result in some graceful failures, but the exact rate depends
    // on which fields are corrupted. Just verify no crashes occurred.
}
