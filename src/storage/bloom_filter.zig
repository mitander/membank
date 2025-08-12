const std = @import("std");
const custom_assert = @import("../core/assert.zig");
const assert = custom_assert.assert;
const context_block = @import("../core/types.zig");
const BlockId = context_block.BlockId;

/// Bloom filter optimized for BlockId lookups in SSTables.
/// Uses double hashing with Wyhash for speed and quality distribution.
pub const BloomFilter = struct {
    /// Bit array backing storage - must be heap allocated for larger filters
    bits: []u8,
    /// Number of hash functions to use (k parameter)
    hash_count: u8,
    /// Size in bits (m parameter) - always multiple of 8 for byte alignment
    bit_count: u32,
    /// Allocator used for bits array
    allocator: std.mem.Allocator,

    /// Recommended parameters for different expected item counts and false positive rates
    pub const Params = struct {
        bit_count: u32,
        hash_count: u8,

        /// For small SSTables (< 1000 blocks): ~1% false positive rate
        pub const small = Params{ .bit_count = 8192, .hash_count = 7 };
        /// For medium SSTables (< 10000 blocks): ~1% false positive rate
        pub const medium = Params{ .bit_count = 65536, .hash_count = 7 };
        /// For large SSTables (< 100000 blocks): ~1% false positive rate
        pub const large = Params{ .bit_count = 524288, .hash_count = 7 };

        /// Calculate optimal parameters for given expected items and desired false positive rate
        pub fn calculate(expected_items: u32, false_positive_rate: f64) Params {
            assert(expected_items > 0);
            assert(false_positive_rate > 0.0 and false_positive_rate < 1.0);

            // m = -n * ln(p) / (ln(2)^2)
            const items_f = @as(f64, @floatFromInt(expected_items));
            const ln2_squared = 0.4804530139182014; // (ln(2))^2
            const bit_count_f = -items_f * @log(false_positive_rate) / ln2_squared;

            const bit_count = @as(u32, @intFromFloat(@ceil(bit_count_f / 8.0) * 8.0));

            // k = (m/n) * ln(2)
            const hash_count_f = (@as(f64, @floatFromInt(bit_count)) / items_f) * @log(2.0);
            const hash_count = @as(u8, @intFromFloat(@round(hash_count_f)));

            return Params{
                .bit_count = @max(64, bit_count), // Minimum 64 bits
                .hash_count = @max(1, @min(16, hash_count)), // Clamp to reasonable range
            };
        }
    };

    pub const Error = error{
        OutOfMemory,
        InvalidBitCount,
        InvalidHashCount,
        BufferTooSmall,
    };

    /// Create new Bloom filter with specified parameters
    pub fn init(allocator: std.mem.Allocator, params: Params) Error!BloomFilter {
        if (params.bit_count == 0 or params.bit_count % 8 != 0) {
            return Error.InvalidBitCount;
        }
        if (params.hash_count == 0) {
            return Error.InvalidHashCount;
        }

        const byte_count = params.bit_count / 8;
        const bits = allocator.alloc(u8, byte_count) catch return Error.OutOfMemory;
        @memset(bits, 0);

        return BloomFilter{
            .bits = bits,
            .hash_count = params.hash_count,
            .bit_count = params.bit_count,
            .allocator = allocator,
        };
    }

    pub fn deinit(self: *BloomFilter) void {
        self.allocator.free(self.bits);
    }

    /// Add a BlockId to the filter
    pub fn add(self: *BloomFilter, block_id: BlockId) void {
        var i: u8 = 0;
        while (i < self.hash_count) : (i += 1) {
            const bit_index = self.hash_block_id(block_id, i);
            self.enable_bit(bit_index);
        }
    }

    /// Check if BlockId might exist in the filter
    /// Returns true if item MIGHT be in set (could be false positive)
    /// Returns false if item is DEFINITELY NOT in set (no false negatives)
    pub fn might_contain(self: *const BloomFilter, block_id: BlockId) bool {
        var i: u8 = 0;
        while (i < self.hash_count) : (i += 1) {
            const bit_index = self.hash_block_id(block_id, i);
            if (!self.test_bit(bit_index)) {
                return false; // Definitely not in set
            }
        }
        return true; // Might be in set
    }

    /// Clear all bits in the filter
    pub fn clear(self: *BloomFilter) void {
        @memset(self.bits, 0);
    }

    /// Calculate serialized size needed for this filter
    pub fn serialized_size(self: *const BloomFilter) u32 {
        // 4 bytes bit_count + 1 byte hash_count + 3 bytes padding + bits
        return 8 + @as(u32, @intCast(self.bits.len));
    }

    /// Serialize filter to buffer
    pub fn serialize(self: *const BloomFilter, buffer: []u8) Error!void {
        const required_size = self.serialized_size();
        if (buffer.len < required_size) {
            return Error.BufferTooSmall;
        }

        var offset: usize = 0;

        std.mem.writeInt(u32, buffer[offset..][0..4], self.bit_count, .little);
        offset += 4;

        buffer[offset] = self.hash_count;
        offset += 1;

        @memset(buffer[offset .. offset + 3], 0);
        offset += 3;

        @memcpy(buffer[offset..][0..self.bits.len], self.bits);
    }

    /// Deserialize filter from buffer
    pub fn deserialize(allocator: std.mem.Allocator, buffer: []const u8) Error!BloomFilter {
        if (buffer.len < 8) {
            return Error.BufferTooSmall;
        }

        var offset: usize = 0;

        const bit_count = std.mem.readInt(u32, buffer[offset..][0..4], .little);
        offset += 4;

        const hash_count = buffer[offset];
        offset += 1;

        offset += 3;

        const byte_count = bit_count / 8;
        if (buffer.len < 8 + byte_count) {
            return Error.BufferTooSmall;
        }

        const bits = allocator.alloc(u8, byte_count) catch return Error.OutOfMemory;
        @memcpy(bits, buffer[offset..][0..byte_count]);

        return BloomFilter{
            .bits = bits,
            .hash_count = hash_count,
            .bit_count = bit_count,
            .allocator = allocator,
        };
    }

    /// Double hashing using Wyhash for speed and quality
    /// Uses h1(x) + i * h2(x) pattern to generate hash_count different hash values
    fn hash_block_id(self: *const BloomFilter, block_id: BlockId, index: u8) u32 {
        var hasher1 = std.hash.Wyhash.init(0);
        hasher1.update(&block_id.bytes);
        const h1 = hasher1.final();

        var hasher2 = std.hash.Wyhash.init(0x9e3779b9);
        hasher2.update(&block_id.bytes);
        const h2 = hasher2.final();

        const combined = h1 +% (@as(u64, index) *% h2);
        return @as(u32, @truncate(combined)) % self.bit_count;
    }

    fn enable_bit(self: *BloomFilter, bit_index: u32) void {
        assert(bit_index < self.bit_count);
        const byte_index = bit_index / 8;
        const bit_offset = @as(u3, @truncate(bit_index % 8));
        self.bits[byte_index] |= (@as(u8, 1) << bit_offset);
    }

    fn test_bit(self: *const BloomFilter, bit_index: u32) bool {
        assert(bit_index < self.bit_count);
        const byte_index = bit_index / 8;
        const bit_offset = @as(u3, @truncate(bit_index % 8));
        return (self.bits[byte_index] & (@as(u8, 1) << bit_offset)) != 0;
    }
};

// ===== TESTS =====

test "Bloom filter basic operations" {
    const allocator = std.testing.allocator;

    var filter = try BloomFilter.init(allocator, BloomFilter.Params.small);
    defer filter.deinit();

    const block1 = try BlockId.from_hex("0123456789abcdeffedcba9876543210");
    const block2 = try BlockId.from_hex("fedcba9876543210123456789abcdef0");
    const block3 = try BlockId.from_hex("11111111111111111111111111111111");

    try std.testing.expect(!filter.might_contain(block1));
    try std.testing.expect(!filter.might_contain(block2));
    try std.testing.expect(!filter.might_contain(block3));

    filter.add(block1);
    try std.testing.expect(filter.might_contain(block1));
    try std.testing.expect(!filter.might_contain(block2));
    try std.testing.expect(!filter.might_contain(block3));

    filter.add(block2);
    try std.testing.expect(filter.might_contain(block1));
    try std.testing.expect(filter.might_contain(block2));
    try std.testing.expect(!filter.might_contain(block3));
}

test "Bloom filter serialization" {
    const allocator = std.testing.allocator;

    var original = try BloomFilter.init(allocator, BloomFilter.Params.small);
    defer original.deinit();

    const block1 = try BlockId.from_hex("0123456789abcdeffedcba9876543210");
    const block2 = try BlockId.from_hex("fedcba9876543210123456789abcdef0");

    original.add(block1);
    original.add(block2);

    const buffer = try allocator.alloc(u8, original.serialized_size());
    defer allocator.free(buffer);

    try original.serialize(buffer);

    var deserialized = try BloomFilter.deserialize(allocator, buffer);
    defer deserialized.deinit();

    try std.testing.expect(deserialized.might_contain(block1));
    try std.testing.expect(deserialized.might_contain(block2));

    const block3 = try BlockId.from_hex("11111111111111111111111111111111");
    try std.testing.expect(!deserialized.might_contain(block3));
}

test "Bloom filter parameter calculation" {
    const small_params = BloomFilter.Params.calculate(1000, 0.01);
    try std.testing.expect(small_params.bit_count > 0);
    try std.testing.expect(small_params.bit_count % 8 == 0);
    try std.testing.expect(small_params.hash_count > 0);

    const large_params = BloomFilter.Params.calculate(100000, 0.001);
    try std.testing.expect(large_params.bit_count > small_params.bit_count);
}

test "Bloom filter false positive behavior" {
    const allocator = std.testing.allocator;

    const params = BloomFilter.Params{ .bit_count = 64, .hash_count = 3 };
    var filter = try BloomFilter.init(allocator, params);
    defer filter.deinit();

    var added_blocks = std.ArrayList(BlockId).init(allocator);
    defer added_blocks.deinit();

    var i: u8 = 0;
    while (i < 10) : (i += 1) {
        var id_bytes: [16]u8 = undefined;
        @memset(&id_bytes, i);
        const block_id = BlockId{ .bytes = id_bytes };
        filter.add(block_id);
        try added_blocks.append(block_id);
    }

    for (added_blocks.items) |block_id| {
        try std.testing.expect(filter.might_contain(block_id));
    }

    filter.clear();
    for (added_blocks.items) |block_id| {
        try std.testing.expect(!filter.might_contain(block_id));
    }
}

test "Bloom filter edge cases" {
    const allocator = std.testing.allocator;

    const min_params = BloomFilter.Params{ .bit_count = 64, .hash_count = 1 };
    var filter = try BloomFilter.init(allocator, min_params);
    defer filter.deinit();

    const block_id = try BlockId.from_hex("0123456789abcdeffedcba9876543210");

    filter.add(block_id);
    try std.testing.expect(filter.might_contain(block_id));
}

test "Bloom filter false positive rate validation" {
    const allocator = std.testing.allocator;

    const test_cases = [_]struct {
        expected_items: u32,
        target_fpr: f64,
        test_items: u32,
    }{
        .{ .expected_items = 1000, .target_fpr = 0.01, .test_items = 10000 },
        .{ .expected_items = 5000, .target_fpr = 0.001, .test_items = 50000 },
        .{ .expected_items = 100, .target_fpr = 0.1, .test_items = 1000 },
    };

    for (test_cases) |test_case| {
        const params = BloomFilter.Params.calculate(test_case.expected_items, test_case.target_fpr);
        var filter = try BloomFilter.init(allocator, params);
        defer filter.deinit();

        var i: u32 = 0;
        while (i < test_case.expected_items) : (i += 1) {
            var id_bytes: [16]u8 = undefined;
            std.mem.writeInt(u128, &id_bytes, i, .little);
            const block_id = BlockId{ .bytes = id_bytes };
            filter.add(block_id);
        }

        var false_positives: u32 = 0;
        i = test_case.expected_items;
        while (i < test_case.expected_items + test_case.test_items) : (i += 1) {
            var id_bytes: [16]u8 = undefined;
            std.mem.writeInt(u128, &id_bytes, i, .little);
            const block_id = BlockId{ .bytes = id_bytes };

            if (filter.might_contain(block_id)) {
                false_positives += 1;
            }
        }

        const actual_fpr = @as(f64, @floatFromInt(false_positives)) / @as(f64, @floatFromInt(test_case.test_items));

        const max_acceptable_fpr = test_case.target_fpr * 3.0;
        try std.testing.expect(actual_fpr <= max_acceptable_fpr);
    }
}

test "Bloom filter performance characteristics" {
    const allocator = std.testing.allocator;

    const params = BloomFilter.Params.calculate(10000, 0.01);
    var filter = try BloomFilter.init(allocator, params);
    defer filter.deinit();

    const start_time = std.time.nanoTimestamp();

    var i: u32 = 0;
    while (i < 10000) : (i += 1) {
        var id_bytes: [16]u8 = undefined;
        std.mem.writeInt(u128, &id_bytes, i, .little);
        const block_id = BlockId{ .bytes = id_bytes };
        filter.add(block_id);
    }

    const add_time = std.time.nanoTimestamp();

    i = 0;
    var found_count: u32 = 0;
    while (i < 10000) : (i += 1) {
        var id_bytes: [16]u8 = undefined;
        std.mem.writeInt(u128, &id_bytes, i, .little);
        const block_id = BlockId{ .bytes = id_bytes };

        if (filter.might_contain(block_id)) {
            found_count += 1;
        }
    }

    const lookup_time = std.time.nanoTimestamp();

    try std.testing.expectEqual(@as(u32, 10000), found_count);

    const add_duration = add_time - start_time;
    const lookup_duration = lookup_time - add_time;

    const max_add_time = 100_000_000; // 100ms for 10k adds (more lenient)
    const max_lookup_time = 100_000_000; // 100ms for 10k lookups (more lenient)

    try std.testing.expect(add_duration < max_add_time);
    try std.testing.expect(lookup_duration < max_lookup_time);
}

test "Bloom filter hash distribution quality" {
    const allocator = std.testing.allocator;

    const params = BloomFilter.Params{ .bit_count = 1024, .hash_count = 4 };
    var filter = try BloomFilter.init(allocator, params);
    defer filter.deinit();

    var i: u32 = 0;
    while (i < 200) : (i += 1) {
        var id_bytes: [16]u8 = undefined;
        std.mem.writeInt(u128, &id_bytes, i, .little);
        const block_id = BlockId{ .bytes = id_bytes };
        filter.add(block_id);
    }

    var set_bits: u32 = 0;
    for (filter.bits) |byte| {
        set_bits += @popCount(byte);
    }

    const bit_density = @as(f64, @floatFromInt(set_bits)) / @as(f64, @floatFromInt(filter.bit_count));
    try std.testing.expect(bit_density > 0.1);
    try std.testing.expect(bit_density < 0.9); // More lenient threshold
}

test "Bloom filter memory usage validation" {
    const allocator = std.testing.allocator;

    const sizes = [_]u32{ 64, 512, 4096, 32768 };

    for (sizes) |bit_count| {
        const params = BloomFilter.Params{ .bit_count = bit_count, .hash_count = 3 };
        var filter = try BloomFilter.init(allocator, params);
        defer filter.deinit();

        const expected_bytes = bit_count / 8;
        try std.testing.expectEqual(expected_bytes, @as(u32, @intCast(filter.bits.len)));

        const serialized_size = filter.serialized_size();
        try std.testing.expectEqual(expected_bytes + 8, serialized_size);
    }
}

test "Bloom filter parameter optimization" {
    const test_cases = [_]struct {
        items: u32,
        fpr: f64,
        expected_min_bits: u32,
        expected_max_hash: u8,
    }{
        .{ .items = 100, .fpr = 0.01, .expected_min_bits = 800, .expected_max_hash = 10 },
        .{ .items = 1000, .fpr = 0.001, .expected_min_bits = 10000, .expected_max_hash = 15 },
        .{ .items = 10000, .fpr = 0.1, .expected_min_bits = 40000, .expected_max_hash = 5 },
    };

    for (test_cases) |test_case| {
        const params = BloomFilter.Params.calculate(test_case.items, test_case.fpr);

        try std.testing.expect(params.bit_count >= test_case.expected_min_bits);
        try std.testing.expect(params.bit_count % 8 == 0); // Byte aligned

        try std.testing.expect(params.hash_count > 0);
        try std.testing.expect(params.hash_count <= test_case.expected_max_hash);
    }
}

test "Bloom filter error handling" {
    const allocator = std.testing.allocator;

    // Test bit_count validation
    const invalid_bit_params = [_]BloomFilter.Params{
        .{ .bit_count = 0, .hash_count = 3 }, // Zero bits
        .{ .bit_count = 15, .hash_count = 3 }, // Non-byte-aligned
    };

    for (invalid_bit_params) |params| {
        const result = BloomFilter.init(allocator, params);
        try std.testing.expectError(BloomFilter.Error.InvalidBitCount, result);
    }

    // Test hash_count validation
    const invalid_hash_params = [_]BloomFilter.Params{
        .{ .bit_count = 64, .hash_count = 0 }, // Zero hash functions
    };

    for (invalid_hash_params) |params| {
        const result = BloomFilter.init(allocator, params);
        try std.testing.expectError(BloomFilter.Error.InvalidHashCount, result);
    }

    var filter = try BloomFilter.init(allocator, BloomFilter.Params.small);
    defer filter.deinit();

    var small_buffer: [4]u8 = undefined;
    const serialize_result = filter.serialize(&small_buffer);
    try std.testing.expectError(BloomFilter.Error.BufferTooSmall, serialize_result);

    const deserialize_result = BloomFilter.deserialize(allocator, &small_buffer);
    try std.testing.expectError(BloomFilter.Error.BufferTooSmall, deserialize_result);
}

test "Bloom filter SSTable integration scenarios" {
    const allocator = std.testing.allocator;

    var filter = try BloomFilter.init(allocator, BloomFilter.Params.medium);
    defer filter.deinit();

    var sstable_blocks = std.ArrayList(BlockId).init(allocator);
    defer sstable_blocks.deinit();

    var i: u32 = 0;
    while (i < 1000) : (i += 1) {
        var id_bytes: [16]u8 = undefined;
        std.mem.writeInt(u128, &id_bytes, i * 17, .little); // Non-sequential pattern
        const block_id = BlockId{ .bytes = id_bytes };

        filter.add(block_id);
        try sstable_blocks.append(block_id);
    }

    for (sstable_blocks.items) |block_id| {
        try std.testing.expect(filter.might_contain(block_id));
    }

    var false_positives: u32 = 0;
    i = 0;
    while (i < 10000) : (i += 1) {
        var id_bytes: [16]u8 = undefined;
        std.mem.writeInt(u128, &id_bytes, i * 19 + 1, .little); // Different pattern
        const block_id = BlockId{ .bytes = id_bytes };

        var is_in_sstable = false;
        for (sstable_blocks.items) |sstable_block| {
            if (std.mem.eql(u8, &block_id.bytes, &sstable_block.bytes)) {
                is_in_sstable = true;
                break;
            }
        }

        if (!is_in_sstable and filter.might_contain(block_id)) {
            false_positives += 1;
        }
    }

    const fpr = @as(f64, @floatFromInt(false_positives)) / 10000.0;
    try std.testing.expect(fpr < 0.05); // Less than 5% false positive rate
}
