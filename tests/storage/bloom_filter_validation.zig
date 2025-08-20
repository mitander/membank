//! BloomFilter unit tests
//!
//! Tests individual BloomFilter functionality in isolation.
//! Focus: add/query operations, parameter calculation, serialization, memory efficiency.

const std = @import("std");

const kausaldb = @import("kausaldb");

const testing = std.testing;
const types = kausaldb.types;

const BloomFilter = kausaldb.bloom_filter.BloomFilter;
const BlockId = types.BlockId;

test "add query basic" {
    const allocator = testing.allocator;

    const params = BloomFilter.Params.calculate(100, 0.01);
    var filter = try BloomFilter.init(allocator, params);
    defer filter.deinit();

    // Create test block ID using standardized test data
    const test_id = kausaldb.TestData.deterministic_block_id(1);

    // Test add operation
    filter.add(test_id);

    // Test query operation
    try testing.expect(filter.might_contain(test_id));
}

test "query missing item" {
    const allocator = testing.allocator;

    const params = BloomFilter.Params.calculate(100, 0.01);
    var filter = try BloomFilter.init(allocator, params);
    defer filter.deinit();

    // Test querying item that was never added using standardized test data
    const missing_id = kausaldb.TestData.deterministic_block_id(99);

    // Should return false (no false negatives in Bloom filters)
    try testing.expect(!filter.might_contain(missing_id));
}

test "multiple items basic" {
    const allocator = testing.allocator;

    const params = BloomFilter.Params.calculate(10, 0.01);
    var filter = try BloomFilter.init(allocator, params);
    defer filter.deinit();

    // Add multiple items using standardized test data
    var test_ids: [5]BlockId = undefined;
    for (&test_ids, 0..) |*id, i| {
        id.* = kausaldb.TestData.deterministic_block_id(@intCast(i + 1));
        filter.add(id.*);
    }

    // Verify all added items are found
    for (test_ids) |id| {
        try testing.expect(filter.might_contain(id));
    }
}

test "false positive rate" {
    const allocator = testing.allocator;

    const expected_items = 1000;
    const target_fpr = 0.01;
    const params = BloomFilter.Params.calculate(expected_items, target_fpr);
    var filter = try BloomFilter.init(allocator, params);
    defer filter.deinit();

    // Add known items
    var i: u32 = 1;
    while (i <= expected_items) : (i += 1) {
        var id_bytes: [16]u8 = undefined;
        std.mem.writeInt(u128, &id_bytes, i, .little);
        const id = BlockId{ .bytes = id_bytes };
        filter.add(id);
    }

    // Test false positive rate with non-added items
    var false_positives: u32 = 0;
    const test_count = 1000;
    i = expected_items + 1;
    while (i <= expected_items + test_count) : (i += 1) {
        var id_bytes: [16]u8 = undefined;
        std.mem.writeInt(u128, &id_bytes, i, .little);
        const id = BlockId{ .bytes = id_bytes };

        if (filter.might_contain(id)) {
            false_positives += 1;
        }
    }

    const actual_fpr = @as(f64, @floatFromInt(false_positives)) / @as(f64, @floatFromInt(test_count));

    // Allow 5x tolerance for statistical variation
    try testing.expect(actual_fpr <= target_fpr * 5.0);
}

test "clear operation" {
    const allocator = testing.allocator;

    const params = BloomFilter.Params.calculate(100, 0.01);
    var filter = try BloomFilter.init(allocator, params);
    defer filter.deinit();

    // Add items
    const test_id = BlockId{ .bytes = [_]u8{1} ** 16 };
    filter.add(test_id);
    try testing.expect(filter.might_contain(test_id));

    // Clear filter
    filter.clear();

    // Verify item is no longer found
    try testing.expect(!filter.might_contain(test_id));

    // Verify filter can be reused after clear
    const new_id = BlockId{ .bytes = [_]u8{2} ** 16 };
    filter.add(new_id);
    try testing.expect(filter.might_contain(new_id));
}

test "serialization basic" {
    const allocator = testing.allocator;

    const params = BloomFilter.Params.calculate(100, 0.01);
    var original = try BloomFilter.init(allocator, params);
    defer original.deinit();

    // Add test data
    const test_id = BlockId{ .bytes = [_]u8{42} ** 16 };
    original.add(test_id);

    // Serialize
    const buffer = try allocator.alloc(u8, original.serialized_size());
    defer allocator.free(buffer);
    try original.serialize(buffer);

    // Deserialize
    var restored = try BloomFilter.deserialize(allocator, buffer);
    defer restored.deinit();

    // Verify behavior is preserved
    try testing.expect(restored.might_contain(test_id));

    // Verify negative case still works
    const missing_id = BlockId{ .bytes = [_]u8{99} ** 16 };
    try testing.expect(!restored.might_contain(missing_id));
}

test "param calculation basic" {
    // Test parameter calculation produces valid results
    const params = BloomFilter.Params.calculate(1000, 0.01);

    // Verify parameters are reasonable
    try testing.expect(params.bit_count >= 8);
    try testing.expect(params.bit_count % 8 == 0); // Must be byte-aligned
    try testing.expect(params.hash_count >= 1);
    try testing.expect(params.hash_count <= 50); // Reasonable upper bound
}

test "error conditions invalid params" {
    const allocator = testing.allocator;

    // Test invalid bit count (not multiple of 8)
    const invalid_bits = BloomFilter.Params{ .bit_count = 7, .hash_count = 1 };
    try testing.expectError(BloomFilter.Error.InvalidBitCount, BloomFilter.init(allocator, invalid_bits));

    // Test zero bit count
    const zero_bits = BloomFilter.Params{ .bit_count = 0, .hash_count = 1 };
    try testing.expectError(BloomFilter.Error.InvalidBitCount, BloomFilter.init(allocator, zero_bits));

    // Test zero hash count
    const zero_hash = BloomFilter.Params{ .bit_count = 64, .hash_count = 0 };
    try testing.expectError(BloomFilter.Error.InvalidHashCount, BloomFilter.init(allocator, zero_hash));
}

test "serialization buffer too small" {
    const allocator = testing.allocator;

    const params = BloomFilter.Params{ .bit_count = 64, .hash_count = 3 };
    var filter = try BloomFilter.init(allocator, params);
    defer filter.deinit();

    // Test serialization with insufficient buffer
    var small_buffer: [4]u8 = undefined;
    try testing.expectError(BloomFilter.Error.BufferTooSmall, filter.serialize(&small_buffer));
}
