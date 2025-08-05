//! Bloom filter validation tests for storage integration.
//!
//! Tests Bloom filter behavior in realistic storage scenarios including
//! SSTable integration, performance characteristics, memory efficiency,
//! and robustness under various conditions.

const std = @import("std");
const testing = std.testing;
const kausaldb = @import("kausaldb");

const storage = kausaldb.storage;
const simulation_vfs = kausaldb.simulation_vfs;
const context_block = kausaldb.types;
const concurrency = kausaldb.concurrency;

const BloomFilter = kausaldb.bloom_filter.BloomFilter;
const StorageEngine = storage.StorageEngine;
const SimulationVFS = simulation_vfs.SimulationVFS;
const ContextBlock = context_block.ContextBlock;
const BlockId = context_block.BlockId;

fn create_test_block(id: u32, content: []const u8, allocator: std.mem.Allocator) !ContextBlock {
    var id_bytes: [16]u8 = undefined;
    std.mem.writeInt(u128, &id_bytes, id, .little);

    return ContextBlock{
        .id = BlockId{ .bytes = id_bytes },
        .version = 1,
        .source_uri = try allocator.dupe(u8, "test://bloom_validation"),
        .metadata_json = try allocator.dupe(u8, "{\"test\": \"bloom_filter\"}"),
        .content = try allocator.dupe(u8, content),
    };
}

test "bloom filter integration with storage engine" {
    concurrency.init();
    const allocator = testing.allocator;

    var sim_vfs = try SimulationVFS.init(allocator);
    defer sim_vfs.deinit();

    var storage_engine = try StorageEngine.init_default(allocator, sim_vfs.vfs(), "bloom_integration_test");
    defer storage_engine.deinit();
    try storage_engine.startup();

    // Create test blocks for storage
    const test_blocks = [_]struct { id: u32, content: []const u8 }{
        .{ .id = 1, .content = "block one content" },
        .{ .id = 2, .content = "block two content" },
        .{ .id = 3, .content = "block three content" },
        .{ .id = 4, .content = "block four content" },
        .{ .id = 5, .content = "block five content" },
    };

    var stored_blocks = std.ArrayList(ContextBlock).init(allocator);
    defer {
        for (stored_blocks.items) |block| {
            allocator.free(block.content);
            allocator.free(block.source_uri);
            allocator.free(block.metadata_json);
        }
        stored_blocks.deinit();
    }

    // Store blocks in storage engine
    for (test_blocks) |test_block| {
        const block = try create_test_block(test_block.id, test_block.content, allocator);
        try storage_engine.put_block(block);
        try stored_blocks.append(block);
    }

    // Create Bloom filter with appropriate size for test data
    const params = BloomFilter.Params.calculate(test_blocks.len, 0.01);
    var bloom_filter = try BloomFilter.init(allocator, params);
    defer bloom_filter.deinit();

    // Add stored block IDs to Bloom filter
    for (stored_blocks.items) |block| {
        bloom_filter.add(block.id);
    }

    // Test positive cases - all stored blocks should be found
    for (stored_blocks.items) |block| {
        try testing.expect(bloom_filter.might_contain(block.id));
    }

    // Test negative cases - non-existent blocks should mostly not be found
    var false_positives: u32 = 0;
    const test_count = 1000;
    var i: u32 = test_blocks.len + 1;
    while (i < test_blocks.len + 1 + test_count) : (i += 1) {
        var id_bytes: [16]u8 = undefined;
        std.mem.writeInt(u128, &id_bytes, i, .little);
        const non_existent_id = BlockId{ .bytes = id_bytes };

        if (bloom_filter.might_contain(non_existent_id)) {
            false_positives += 1;
        }
    }

    // False positive rate should be reasonable
    const actual_fpr = @as(f64, @floatFromInt(false_positives)) / @as(f64, @floatFromInt(test_count));
    try testing.expect(actual_fpr <= 0.05); // Allow up to 5% false positive rate
}

test "bloom filter performance with large datasets" {
    concurrency.init();
    const allocator = testing.allocator;

    // Test with realistic dataset size
    const dataset_size = 50000;
    const target_fpr = 0.001;

    const params = BloomFilter.Params.calculate(dataset_size, target_fpr);
    var bloom_filter = try BloomFilter.init(allocator, params);
    defer bloom_filter.deinit();

    // Measure insertion performance
    const insert_start = std.time.nanoTimestamp();

    var i: u32 = 0;
    while (i < dataset_size) : (i += 1) {
        var id_bytes: [16]u8 = undefined;
        std.mem.writeInt(u128, &id_bytes, i, .little);
        const block_id = BlockId{ .bytes = id_bytes };
        bloom_filter.add(block_id);
    }

    const insert_end = std.time.nanoTimestamp();
    const insert_duration = insert_end - insert_start;

    // Measure lookup performance
    const lookup_start = std.time.nanoTimestamp();

    i = 0;
    var found_count: u32 = 0;
    while (i < dataset_size) : (i += 1) {
        var id_bytes: [16]u8 = undefined;
        std.mem.writeInt(u128, &id_bytes, i, .little);
        const block_id = BlockId{ .bytes = id_bytes };

        if (bloom_filter.might_contain(block_id)) {
            found_count += 1;
        }
    }

    const lookup_end = std.time.nanoTimestamp();
    const lookup_duration = lookup_end - lookup_start;

    // All inserted items should be found
    try testing.expectEqual(dataset_size, found_count);

    // Performance should be reasonable (more generous for CI environments)
    const max_insert_time = 1_000_000_000; // 1s for 50K insertions
    const max_lookup_time = 500_000_000; // 500ms for 50K lookups

    try testing.expect(insert_duration < max_insert_time);
    try testing.expect(lookup_duration < max_lookup_time);

    // Test false positive rate on large scale
    var false_positives: u32 = 0;
    const test_count = 10000;
    i = dataset_size;
    while (i < dataset_size + test_count) : (i += 1) {
        var id_bytes: [16]u8 = undefined;
        std.mem.writeInt(u128, &id_bytes, i, .little);
        const block_id = BlockId{ .bytes = id_bytes };

        if (bloom_filter.might_contain(block_id)) {
            false_positives += 1;
        }
    }

    const actual_fpr = @as(f64, @floatFromInt(false_positives)) / @as(f64, @floatFromInt(test_count));
    const max_acceptable_fpr = target_fpr * 5.0; // Allow 5x tolerance for statistical variation
    try testing.expect(actual_fpr <= max_acceptable_fpr);
}

test "bloom filter memory efficiency validation" {
    concurrency.init();
    const allocator = testing.allocator;

    // Test different filter sizes and validate memory usage
    const test_cases = [_]struct {
        expected_items: u32,
        target_fpr: f64,
        max_memory_kb: u32,
    }{
        .{ .expected_items = 1000, .target_fpr = 0.01, .max_memory_kb = 5 },
        .{ .expected_items = 10000, .target_fpr = 0.001, .max_memory_kb = 50 },
        .{ .expected_items = 100000, .target_fpr = 0.0001, .max_memory_kb = 500 },
    };

    for (test_cases) |test_case| {
        const params = BloomFilter.Params.calculate(test_case.expected_items, test_case.target_fpr);
        var bloom_filter = try BloomFilter.init(allocator, params);
        defer bloom_filter.deinit();

        const actual_memory_bytes = bloom_filter.bits.len;
        const actual_memory_kb = actual_memory_bytes / 1024;

        // Memory usage should be within expected bounds
        try testing.expect(actual_memory_kb <= test_case.max_memory_kb);

        // Verify filter still works correctly with memory constraints
        var i: u32 = 0;
        while (i < test_case.expected_items / 10) : (i += 1) {
            var id_bytes: [16]u8 = undefined;
            std.mem.writeInt(u128, &id_bytes, i, .little);
            const block_id = BlockId{ .bytes = id_bytes };
            bloom_filter.add(block_id);
        }

        // Test that added items are found
        i = 0;
        while (i < test_case.expected_items / 10) : (i += 1) {
            var id_bytes: [16]u8 = undefined;
            std.mem.writeInt(u128, &id_bytes, i, .little);
            const block_id = BlockId{ .bytes = id_bytes };
            try testing.expect(bloom_filter.might_contain(block_id));
        }
    }
}

test "bloom filter serialization robustness" {
    concurrency.init();
    const allocator = testing.allocator;

    // Test serialization with various filter states
    const params = BloomFilter.Params.calculate(5000, 0.01);
    var original = try BloomFilter.init(allocator, params);
    defer original.deinit();

    // Test empty filter serialization
    const buffer = try allocator.alloc(u8, original.serialized_size());
    defer allocator.free(buffer);

    try original.serialize(buffer);
    var empty_deserialized = try BloomFilter.deserialize(allocator, buffer);
    defer empty_deserialized.deinit();

    // Test partially filled filter
    var i: u32 = 0;
    while (i < 1000) : (i += 1) {
        var id_bytes: [16]u8 = undefined;
        std.mem.writeInt(u128, &id_bytes, i, .little);
        const block_id = BlockId{ .bytes = id_bytes };
        original.add(block_id);
    }

    try original.serialize(buffer);
    var partial_deserialized = try BloomFilter.deserialize(allocator, buffer);
    defer partial_deserialized.deinit();

    // Verify behavior is preserved
    i = 0;
    while (i < 1000) : (i += 1) {
        var id_bytes: [16]u8 = undefined;
        std.mem.writeInt(u128, &id_bytes, i, .little);
        const block_id = BlockId{ .bytes = id_bytes };
        try testing.expect(partial_deserialized.might_contain(block_id));
    }

    // Test fully saturated filter
    i = 1000;
    while (i < 5000) : (i += 1) {
        var id_bytes: [16]u8 = undefined;
        std.mem.writeInt(u128, &id_bytes, i, .little);
        const block_id = BlockId{ .bytes = id_bytes };
        original.add(block_id);
    }

    try original.serialize(buffer);
    var full_deserialized = try BloomFilter.deserialize(allocator, buffer);
    defer full_deserialized.deinit();

    // Verify all items are still found
    i = 0;
    while (i < 5000) : (i += 1) {
        var id_bytes: [16]u8 = undefined;
        std.mem.writeInt(u128, &id_bytes, i, .little);
        const block_id = BlockId{ .bytes = id_bytes };
        try testing.expect(full_deserialized.might_contain(block_id));
    }
}

test "bloom filter error conditions and edge cases" {
    concurrency.init();
    const allocator = testing.allocator;

    // Test invalid parameters
    const invalid_params = BloomFilter.Params{ .bit_count = 7, .hash_count = 1 }; // Not multiple of 8
    try testing.expectError(BloomFilter.Error.InvalidBitCount, BloomFilter.init(allocator, invalid_params));

    const zero_bits = BloomFilter.Params{ .bit_count = 0, .hash_count = 1 };
    try testing.expectError(BloomFilter.Error.InvalidBitCount, BloomFilter.init(allocator, zero_bits));

    const zero_hash = BloomFilter.Params{ .bit_count = 64, .hash_count = 0 };
    try testing.expectError(BloomFilter.Error.InvalidHashCount, BloomFilter.init(allocator, zero_hash));

    // Test serialization with insufficient buffer
    const valid_params = BloomFilter.Params{ .bit_count = 64, .hash_count = 3 };
    var filter = try BloomFilter.init(allocator, valid_params);
    defer filter.deinit();

    var small_buffer: [4]u8 = undefined;
    try testing.expectError(BloomFilter.Error.BufferTooSmall, filter.serialize(&small_buffer));

    // Test deserialization with corrupted data
    var corrupt_buffer: [16]u8 = undefined;
    @memset(&corrupt_buffer, 0xFF);
    try testing.expectError(BloomFilter.Error.BufferTooSmall, BloomFilter.deserialize(allocator, &corrupt_buffer));

    // Test minimum and maximum parameters
    const min_params = BloomFilter.Params{ .bit_count = 8, .hash_count = 1 };
    var min_filter = try BloomFilter.init(allocator, min_params);
    defer min_filter.deinit();

    // Should work with minimum parameters
    var id_bytes: [16]u8 = undefined;
    std.mem.writeInt(u128, &id_bytes, 42, .little);
    const test_id = BlockId{ .bytes = id_bytes };

    min_filter.add(test_id);
    try testing.expect(min_filter.might_contain(test_id));
}

test "bloom filter with realistic block ID patterns" {
    concurrency.init();
    const allocator = testing.allocator;

    const params = BloomFilter.Params.calculate(10000, 0.001);
    var filter = try BloomFilter.init(allocator, params);
    defer filter.deinit();

    // Test with realistic BlockId patterns (UUIDs, content hashes, etc.)
    const realistic_ids = [_][]const u8{
        "0123456789abcdeffedcba9876543210",
        "fedcba9876543210123456789abcdef0",
        "aaaaaaaaaaaaaaaabbbbbbbbbbbbbbbb",
        "ccccccccccccccccdddddddddddddddd",
        "11111111111111112222222222222222",
        "99999999999999998888888888888888",
        "abcdefabcdefabcdefabcdefabcdefab",
        "deadbeefdeadbeefdeadbeefdeadbeef",
        "cafebabecafebabecafebabecafebabe",
        "00000000000000010000000000000002",
    };

    var test_block_ids = std.ArrayList(BlockId).init(allocator);
    defer test_block_ids.deinit();

    // Add realistic block IDs
    for (realistic_ids) |id_hex| {
        const block_id = try BlockId.from_hex(id_hex);
        filter.add(block_id);
        try test_block_ids.append(block_id);
    }

    // Verify all added IDs are found
    for (test_block_ids.items) |block_id| {
        try testing.expect(filter.might_contain(block_id));
    }

    // Test with similar but different IDs (should mostly not be found)
    var similar_false_positives: u32 = 0;
    for (realistic_ids) |id_hex| {
        // Create similar ID by flipping one byte
        var modified_hex = try allocator.alloc(u8, id_hex.len);
        defer allocator.free(modified_hex);
        @memcpy(modified_hex, id_hex);

        // Flip a character to create similar but different ID
        if (modified_hex[0] == '0') {
            modified_hex[0] = '1';
        } else {
            modified_hex[0] = '0';
        }

        const similar_id = try BlockId.from_hex(modified_hex);
        if (filter.might_contain(similar_id)) {
            similar_false_positives += 1;
        }
    }

    // Should have low false positive rate even for similar IDs
    const similar_fpr = @as(f64, @floatFromInt(similar_false_positives)) / @as(f64, @floatFromInt(realistic_ids.len));
    try testing.expect(similar_fpr <= 0.3); // Allow some false positives for similar IDs
}

test "bloom filter clear operation validation" {
    concurrency.init();
    const allocator = testing.allocator;

    const params = BloomFilter.Params.calculate(1000, 0.01);
    var filter = try BloomFilter.init(allocator, params);
    defer filter.deinit();

    // Add items to filter
    var i: u32 = 0;
    while (i < 500) : (i += 1) {
        var id_bytes: [16]u8 = undefined;
        std.mem.writeInt(u128, &id_bytes, i, .little);
        const block_id = BlockId{ .bytes = id_bytes };
        filter.add(block_id);
    }

    // Verify items are found
    i = 0;
    while (i < 500) : (i += 1) {
        var id_bytes: [16]u8 = undefined;
        std.mem.writeInt(u128, &id_bytes, i, .little);
        const block_id = BlockId{ .bytes = id_bytes };
        try testing.expect(filter.might_contain(block_id));
    }

    // Clear the filter
    filter.clear();

    // Verify no items are found after clear
    i = 0;
    while (i < 500) : (i += 1) {
        var id_bytes: [16]u8 = undefined;
        std.mem.writeInt(u128, &id_bytes, i, .little);
        const block_id = BlockId{ .bytes = id_bytes };
        try testing.expect(!filter.might_contain(block_id));
    }

    // Verify filter can be reused after clear
    const new_id_bytes: [16]u8 = [_]u8{0xFF} ** 16;
    const new_block_id = BlockId{ .bytes = new_id_bytes };

    filter.add(new_block_id);
    try testing.expect(filter.might_contain(new_block_id));
}

test "bloom filter parameter calculation validation" {
    concurrency.init();

    // Test parameter calculation for various scenarios
    const test_cases = [_]struct {
        expected_items: u32,
        target_fpr: f64,
        min_bits: u32,
        max_bits: u32,
        min_hash: u8,
        max_hash: u8,
    }{
        .{ .expected_items = 100, .target_fpr = 0.1, .min_bits = 64, .max_bits = 1024, .min_hash = 1, .max_hash = 10 },
        .{ .expected_items = 1000, .target_fpr = 0.01, .min_bits = 512, .max_bits = 16384, .min_hash = 3, .max_hash = 15 },
        .{ .expected_items = 10000, .target_fpr = 0.001, .min_bits = 4096, .max_bits = 262144, .min_hash = 5, .max_hash = 20 },
        .{ .expected_items = 100000, .target_fpr = 0.0001, .min_bits = 32768, .max_bits = 2097152, .min_hash = 8, .max_hash = 25 },
    };

    for (test_cases) |test_case| {
        const params = BloomFilter.Params.calculate(test_case.expected_items, test_case.target_fpr);

        // Verify bit count is reasonable and aligned
        try testing.expect(params.bit_count >= test_case.min_bits);
        try testing.expect(params.bit_count <= test_case.max_bits);
        try testing.expect(params.bit_count % 8 == 0);

        // Verify hash count is reasonable
        try testing.expect(params.hash_count >= test_case.min_hash);
        try testing.expect(params.hash_count <= test_case.max_hash);

        // Verify parameters make sense relative to each other
        // More stringent false positive rates should require more bits and/or hash functions
        if (test_case.target_fpr < 0.01) {
            try testing.expect(params.bit_count > 1000 or params.hash_count > 5);
        }
    }

    // Test edge case parameters
    const tiny_params = BloomFilter.Params.calculate(1, 0.5);
    try testing.expect(tiny_params.bit_count >= 8);
    try testing.expect(tiny_params.hash_count >= 1);

    const large_params = BloomFilter.Params.calculate(1000000, 0.00001);
    try testing.expect(large_params.bit_count > 1000000);
    try testing.expect(large_params.hash_count > 10);
}
