//! Core Data Types for CortexDB
//!
//! This module defines the fundamental data structures used throughout the system:
//! - ContextBlock: The atomic unit of knowledge with metadata and relationships
//! - BlockId: 128-bit ULID identifiers for blocks
//! - GraphEdge: Typed relationships between blocks
//! - EdgeType: Enumeration of relationship types (imports, references, etc.)
//!
//! These types form the foundation of CortexDB's knowledge representation model.

const std = @import("std");
const custom_assert = @import("assert");
const assert = custom_assert.assert;
const assert_not_null = custom_assert.assert_not_null;
const assert_not_empty = custom_assert.assert_not_empty;
const assert_range = custom_assert.assert_range;
const assert_buffer_bounds = custom_assert.assert_buffer_bounds;

/// 128-bit ULID identifier for Context Blocks.
/// Time-ordered and unique, providing both uniqueness and natural ordering.
pub const BlockId = struct {
    bytes: [16]u8,

    /// Create a new BlockId from raw bytes.
    pub fn from_bytes(bytes: [16]u8) BlockId {
        return BlockId{ .bytes = bytes };
    }

    /// Create a new BlockId from a hex string.
    pub fn from_hex(hex: []const u8) !BlockId {
        if (hex.len != 32) return error.InvalidHexLength;

        var bytes: [16]u8 = undefined;
        for (0..16) |i| {
            bytes[i] = try std.fmt.parseInt(u8, hex[i * 2 .. i * 2 + 2], 16);
        }
        return BlockId{ .bytes = bytes };
    }

    /// Convert BlockId to hex string representation.
    pub fn to_hex(self: BlockId, allocator: std.mem.Allocator) ![]u8 {
        var result = try allocator.alloc(u8, self.bytes.len * 2);
        for (self.bytes, 0..) |byte, i| {
            _ = std.fmt.bufPrint(result[i * 2 .. i * 2 + 2], "{x:0>2}", .{byte}) catch unreachable;
        }
        return result;
    }

    /// Compare two BlockIds for equality.
    pub fn eql(self: BlockId, other: BlockId) bool {
        return std.mem.eql(u8, &self.bytes, &other.bytes);
    }
};

/// Edge type enum representing relationships between Context Blocks.
pub const EdgeType = enum(u16) {
    imports = 1,
    defined_in = 2,
    references = 3,
    contains = 4,
    extends = 5,
    implements = 6,
    calls = 7,
    depends_on = 8,

    /// Convert EdgeType to its numeric representation.
    pub fn to_u16(self: EdgeType) u16 {
        return @intFromEnum(self);
    }

    /// Create EdgeType from numeric representation.
    pub fn from_u16(value: u16) !EdgeType {
        return std.meta.intToEnum(EdgeType, value) catch error.InvalidEdgeType;
    }
};

/// Context Block - the atomic unit of knowledge in CortexDB.
/// Represents any logical chunk of information with structured metadata
/// and relationships to other blocks through graph edges.
pub const ContextBlock = struct {
    /// 128-bit ULID identifier, time-ordered and unique
    id: BlockId,

    /// Monotonically increasing version number for this block
    version: u64,

    /// URI of the block's origin (e.g., "git://repo.git/file.zig#L123")
    source_uri: []const u8,

    /// JSON string containing flexible key-value metadata
    /// Examples: {"type": "function", "language": "zig", "start_line": 123}
    metadata_json: []const u8,

    /// Raw content of the block
    content: []const u8,

    /// File format header for versioning and future-proofing
    pub const BlockHeader = struct {
        magic: u32 = ContextBlock.MAGIC, // "CXDB" magic number
        format_version: u16 = ContextBlock.FORMAT_VERSION, // Major.minor versioning
        flags: u16 = 0, // Feature flags for extensions
        id: [16]u8, // Block identifier
        block_version: u64, // Monotonic version for this block
        source_uri_len: u32, // Length of source URI
        metadata_json_len: u32, // Length of metadata JSON
        content_len: u32, // Length of content
        checksum: u32 = 0, // CRC32 of variable data (computed on write)
        reserved: [12]u8 = [_]u8{0} ** 12, // Reserved for future use

        pub const SIZE = 64; // Cache-aligned header size

        pub fn serialize(self: BlockHeader, buffer: []u8) !void {
            assert(buffer.len >= SIZE, "Buffer too small for BlockHeader: {} < {}", .{ buffer.len, SIZE });
            if (buffer.len < SIZE) return error.BufferTooSmall;

            var offset: usize = 0;

            std.mem.writeInt(u32, buffer[offset..][0..4], self.magic, .little);
            offset += 4;

            std.mem.writeInt(u16, buffer[offset..][0..2], self.format_version, .little);
            offset += 2;

            std.mem.writeInt(u16, buffer[offset..][0..2], self.flags, .little);
            offset += 2;

            @memcpy(buffer[offset .. offset + 16], &self.id);
            offset += 16;

            std.mem.writeInt(u64, buffer[offset..][0..8], self.block_version, .little);
            offset += 8;

            std.mem.writeInt(u32, buffer[offset..][0..4], self.source_uri_len, .little);
            offset += 4;
            std.mem.writeInt(u32, buffer[offset..][0..4], self.metadata_json_len, .little);
            offset += 4;
            std.mem.writeInt(u32, buffer[offset..][0..4], self.content_len, .little);
            offset += 4;

            std.mem.writeInt(u32, buffer[offset..][0..4], self.checksum, .little);
            offset += 4;

            // Reserved bytes must be zero for forward compatibility
            @memset(buffer[offset .. offset + 12], 0);
        }

        pub fn deserialize(buffer: []const u8) !BlockHeader {
            assert(buffer.len >= SIZE, "Buffer too small for BlockHeader deserialization: {} < {}", .{ buffer.len, SIZE });
            if (buffer.len < SIZE) return error.BufferTooSmall;

            var offset: usize = 0;

            const magic = std.mem.readInt(u32, buffer[offset..][0..4], .little);
            if (magic != ContextBlock.MAGIC) return error.InvalidMagic;
            offset += 4;

            const format_version = std.mem.readInt(u16, buffer[offset..][0..2], .little);
            if (format_version > ContextBlock.FORMAT_VERSION) return error.UnsupportedVersion;
            offset += 2;

            const flags = std.mem.readInt(u16, buffer[offset..][0..2], .little);
            offset += 2;

            var id: [16]u8 = undefined;
            @memcpy(&id, buffer[offset .. offset + 16]);
            offset += 16;

            const block_version = std.mem.readInt(u64, buffer[offset..][0..8], .little);
            offset += 8;

            const source_uri_len = std.mem.readInt(u32, buffer[offset..][0..4], .little);
            offset += 4;
            const metadata_json_len = std.mem.readInt(u32, buffer[offset..][0..4], .little);
            offset += 4;
            const content_len = std.mem.readInt(u32, buffer[offset..][0..4], .little);
            offset += 4;

            const checksum = std.mem.readInt(u32, buffer[offset..][0..4], .little);
            offset += 4;

            // Reserved bytes must be zero for forward compatibility
            const reserved_bytes = buffer[offset .. offset + 12];
            for (reserved_bytes) |byte| {
                if (byte != 0) return error.InvalidReservedBytes;
            }

            return BlockHeader{
                .magic = magic,
                .format_version = format_version,
                .flags = flags,
                .id = id,
                .block_version = block_version,
                .source_uri_len = source_uri_len,
                .metadata_json_len = metadata_json_len,
                .content_len = content_len,
                .checksum = checksum,
                .reserved = [_]u8{0} ** 12,
            };
        }
    };

    /// File format constants
    pub const MAGIC = 0x42444358; // "CXDB" in little-endian
    pub const FORMAT_VERSION = 1;

    /// Minimum serialized size (header + no variable-length data)
    pub const MIN_SERIALIZED_SIZE = BlockHeader.SIZE;

    /// Calculate the serialized size in bytes.
    pub fn serialized_size(self: ContextBlock) usize {
        return BlockHeader.SIZE + // 64-byte header
            self.source_uri.len +
            self.metadata_json.len +
            self.content.len;
    }

    /// Compute serialized size from buffer header without full deserialization.
    /// Used during WAL recovery to determine entry boundaries.
    pub fn compute_serialized_size_from_buffer(buffer: []const u8) !usize {
        if (buffer.len < MIN_SERIALIZED_SIZE) return error.BufferTooSmall;

        // Deserialize header to get length fields
        const header = try BlockHeader.deserialize(buffer);

        // Calculate total size with overflow protection
        const variable_data_size = std.math.add(u64, header.source_uri_len, header.metadata_json_len) catch return error.InvalidHeader;
        const total_variable_size = std.math.add(u64, variable_data_size, header.content_len) catch return error.InvalidHeader;
        const total_size = std.math.add(u64, MIN_SERIALIZED_SIZE, total_variable_size) catch return error.InvalidHeader;

        if (total_size > std.math.maxInt(usize)) return error.InvalidHeader;
        return @intCast(total_size);
    }

    /// Serialize the Context Block to bytes using versioned header format.
    /// Layout: | BlockHeader (64) | source_uri (...) | metadata_json (...) | content (...) |
    pub fn serialize(self: ContextBlock, buffer: []u8) !usize {
        const required_size = self.serialized_size();
        assert(buffer.len >= required_size, "Buffer too small for ContextBlock serialization: {} < {}", .{ buffer.len, required_size });
        if (buffer.len < required_size) return error.BufferTooSmall;

        // Calculate checksum of variable data
        var hasher = std.hash.Wyhash.init(0);
        hasher.update(self.source_uri);
        hasher.update(self.metadata_json);
        hasher.update(self.content);
        const data_checksum: u32 = @truncate(hasher.final());

        // Create header
        const header = BlockHeader{
            .magic = ContextBlock.MAGIC,
            .format_version = ContextBlock.FORMAT_VERSION,
            .flags = 0,
            .id = self.id.bytes,
            .block_version = self.version,
            .source_uri_len = @intCast(self.source_uri.len),
            .metadata_json_len = @intCast(self.metadata_json.len),
            .content_len = @intCast(self.content.len),
            .checksum = data_checksum,
            .reserved = [_]u8{0} ** 12,
        };

        // Write header
        try header.serialize(buffer[0..BlockHeader.SIZE]);
        var offset: usize = BlockHeader.SIZE;

        // Write variable-length data
        @memcpy(buffer[offset .. offset + self.source_uri.len], self.source_uri);
        offset += self.source_uri.len;
        @memcpy(buffer[offset .. offset + self.metadata_json.len], self.metadata_json);
        offset += self.metadata_json.len;
        @memcpy(buffer[offset .. offset + self.content.len], self.content);
        offset += self.content.len;

        return offset;
    }

    /// Deserialize a Context Block from bytes with versioned header support.
    /// The returned block owns its data through the provided allocator.
    pub fn deserialize(buffer: []const u8, allocator: std.mem.Allocator) !ContextBlock {
        assert_not_empty(buffer, "Deserialize buffer cannot be empty", .{});
        assert(@intFromPtr(allocator.ptr) != 0, "Allocator cannot be null", .{});
        assert(buffer.len >= MIN_SERIALIZED_SIZE, "Buffer too small: {} < {}", .{ buffer.len, MIN_SERIALIZED_SIZE });

        if (buffer.len == 0) return error.EmptyBuffer;
        if (buffer.len < MIN_SERIALIZED_SIZE) return error.BufferTooSmall;

        assert_buffer_bounds(0, BlockHeader.SIZE, buffer.len, "Header bounds check failed: {} + {} > {}", .{ 0, BlockHeader.SIZE, buffer.len });
        const header = try BlockHeader.deserialize(buffer[0..BlockHeader.SIZE]);

        assert(header.magic == ContextBlock.MAGIC, "Invalid magic number: 0x{X} != 0x{X}", .{ header.magic, ContextBlock.MAGIC });
        assert(header.format_version <= ContextBlock.FORMAT_VERSION, "Unsupported format version: {} > {}", .{ header.format_version, ContextBlock.FORMAT_VERSION });

        const variable_data_size = std.math.add(u64, header.source_uri_len, header.metadata_json_len) catch return error.InvalidHeader;
        const total_variable_size = std.math.add(u64, variable_data_size, header.content_len) catch return error.InvalidHeader;
        const total_size = std.math.add(u64, BlockHeader.SIZE, total_variable_size) catch return error.InvalidHeader;

        if (total_size > buffer.len) return error.BufferTooSmall;
        if (total_size > std.math.maxInt(usize)) return error.InvalidHeader;

        var offset: usize = BlockHeader.SIZE;

        // Extract variable-length data with bounds checking
        assert_buffer_bounds(offset, header.source_uri_len, buffer.len, "Source URI bounds check failed: {} + {} > {}", .{ offset, header.source_uri_len, buffer.len });
        const source_uri_data = buffer[offset .. offset + header.source_uri_len];
        offset += header.source_uri_len;

        assert_buffer_bounds(offset, header.metadata_json_len, buffer.len, "Metadata JSON bounds check failed: {} + {} > {}", .{ offset, header.metadata_json_len, buffer.len });
        const metadata_json_data = buffer[offset .. offset + header.metadata_json_len];
        offset += header.metadata_json_len;

        assert_buffer_bounds(offset, header.content_len, buffer.len, "Content bounds check failed: {} + {} > {}", .{ offset, header.content_len, buffer.len });
        const content_data = buffer[offset .. offset + header.content_len];

        // Verify checksum
        var hasher = std.hash.Wyhash.init(0);
        hasher.update(source_uri_data);
        hasher.update(metadata_json_data);
        hasher.update(content_data);
        const computed_checksum: u32 = @truncate(hasher.final());
        if (computed_checksum != header.checksum) {
            return error.InvalidChecksum;
        }

        // Copy variable-length data to ensure ownership
        const source_uri = try allocator.dupe(u8, source_uri_data);
        const metadata_json = try allocator.dupe(u8, metadata_json_data);
        const content = try allocator.dupe(u8, content_data);

        assert(source_uri.len == header.source_uri_len, "Source URI allocation length mismatch: {} != {}", .{ source_uri.len, header.source_uri_len });
        assert(metadata_json.len == header.metadata_json_len, "Metadata JSON allocation length mismatch: {} != {}", .{ metadata_json.len, header.metadata_json_len });
        assert(content.len == header.content_len, "Content allocation length mismatch: {} != {}", .{ content.len, header.content_len });

        const result = ContextBlock{
            .id = BlockId.from_bytes(header.id),
            .version = header.block_version,
            .source_uri = source_uri,
            .metadata_json = metadata_json,
            .content = content,
        };

        // Exit condition assertions
        assert(result.version == header.block_version, "Result version mismatch: {} != {}", .{ result.version, header.block_version });
        assert(std.mem.eql(u8, &result.id.bytes, &header.id), "Result ID mismatch", .{});

        return result;
    }

    /// Free allocated memory for the Context Block.
    pub fn deinit(self: ContextBlock, allocator: std.mem.Allocator) void {
        allocator.free(self.source_uri);
        allocator.free(self.metadata_json);
        allocator.free(self.content);
    }

    /// Validation strategy for ContextBlock data integrity.
    ///
    /// This function enforces data format correctness but deliberately does NOT
    /// enforce business logic constraints like non-empty fields. The rationale:
    ///
    /// 1. **Storage layer agnostic**: Empty fields are valid at the storage level
    /// 2. **Test flexibility**: Edge case tests legitimately use empty data
    /// 3. **Recovery robustness**: WAL recovery must handle any serialized data
    /// 4. **Domain separation**: Business rules belong in ingestion/application layers
    ///
    /// Validation covers:
    /// - JSON format validity in metadata_json
    /// - UTF-8 encoding in all text fields
    /// - Basic structural integrity
    ///
    /// For business logic validation (non-empty constraints, semantic rules),
    /// use validate_for_ingestion() or implement domain-specific checks.
    pub fn validate(self: ContextBlock, allocator: std.mem.Allocator) !void {
        // JSON format validation - metadata must be parseable
        var parsed = std.json.parseFromSlice(
            std.json.Value,
            allocator,
            self.metadata_json,
            .{},
        ) catch {
            return error.InvalidMetadataJson;
        };
        defer parsed.deinit();

        // UTF-8 encoding validation for all text fields
        if (!std.unicode.utf8ValidateSlice(self.source_uri)) {
            return error.InvalidSourceUriEncoding;
        }
        if (!std.unicode.utf8ValidateSlice(self.metadata_json)) {
            return error.InvalidMetadataEncoding;
        }

        // Version must be positive (zero indicates uninitialized state)
        if (self.version == 0) {
            return error.InvalidVersion;
        }
    }

    /// Strict validation for ingestion contexts where business rules apply.
    ///
    /// This enforces constraints appropriate for data coming from external sources:
    /// - Non-empty source_uri (must identify origin)
    /// - Non-empty content (must have meaningful data)
    /// - Valid JSON metadata with required fields
    ///
    /// Use this at ingestion boundaries, not for internal storage operations.
    pub fn validate_for_ingestion(self: ContextBlock, allocator: std.mem.Allocator) !void {
        // First run basic structural validation
        try self.validate(allocator);

        // Ingestion-specific business rules
        if (self.source_uri.len == 0) {
            return error.MissingSourceUri;
        }
        if (self.content.len == 0) {
            return error.MissingContent;
        }

        // Content should be valid UTF-8 for ingested data
        if (!std.unicode.utf8ValidateSlice(self.content)) {
            return error.InvalidContentEncoding;
        }

        // Metadata should contain at minimum a type field for ingested content
        var parsed = try std.json.parseFromSlice(
            std.json.Value,
            allocator,
            self.metadata_json,
            .{},
        );
        defer parsed.deinit();

        // Require type field in ingested metadata
        if (parsed.value.object.get("type") == null) {
            return error.MissingMetadataType;
        }
    }
};

/// Graph Edge representing a directed relationship between Context Blocks.
pub const GraphEdge = struct {
    /// Source block ID (where the edge starts)
    source_id: BlockId,

    /// Target block ID (where the edge ends)
    target_id: BlockId,

    /// Type of relationship
    edge_type: EdgeType,

    const SERIALIZED_SIZE = 40; // 16 + 16 + 2 + 6

    /// Serialize the Graph Edge to bytes according to the specification.
    /// Layout: | source_id (16) | target_id (16) | type_id (2) | reserved (6) |
    pub fn serialize(self: GraphEdge, buffer: []u8) !usize {
        if (buffer.len < SERIALIZED_SIZE) return error.BufferTooSmall;

        var offset: usize = 0;

        // Write source ID (16 bytes)
        @memcpy(buffer[offset .. offset + 16], &self.source_id.bytes);
        offset += 16;

        // Write target ID (16 bytes)
        @memcpy(buffer[offset .. offset + 16], &self.target_id.bytes);
        offset += 16;

        // Write edge type (2 bytes, little-endian)
        std.mem.writeInt(u16, buffer[offset..][0..2], self.edge_type.to_u16(), .little);
        offset += 2;

        // Write reserved bytes (6 bytes, must be zero)
        @memset(buffer[offset .. offset + 6], 0);
        offset += 6;

        return offset;
    }

    /// Deserialize a Graph Edge from bytes.
    pub fn deserialize(buffer: []const u8) !GraphEdge {
        if (buffer.len < SERIALIZED_SIZE) return error.BufferTooSmall;

        var offset: usize = 0;

        // Read source ID (16 bytes)
        var source_bytes: [16]u8 = undefined;
        @memcpy(&source_bytes, buffer[offset .. offset + 16]);
        const source_id = BlockId.from_bytes(source_bytes);
        offset += 16;

        // Read target ID (16 bytes)
        var target_bytes: [16]u8 = undefined;
        @memcpy(&target_bytes, buffer[offset .. offset + 16]);
        const target_id = BlockId.from_bytes(target_bytes);
        offset += 16;

        // Read edge type (2 bytes, little-endian)
        const type_id = std.mem.readInt(u16, buffer[offset..][0..2], .little);
        const edge_type = try EdgeType.from_u16(type_id);
        offset += 2;

        for (buffer[offset .. offset + 6]) |byte| {
            if (byte != 0) return error.InvalidReservedBytes;
        }

        return GraphEdge{
            .source_id = source_id,
            .target_id = target_id,
            .edge_type = edge_type,
        };
    }
};

// Tests

test "BlockId basic operations" {
    const allocator = std.testing.allocator;

    // Test from_bytes and to_hex
    const test_bytes = [16]u8{
        0x01, 0x23, 0x45, 0x67, 0x89, 0xAB, 0xCD, 0xEF,
        0xFE, 0xDC, 0xBA, 0x98, 0x76, 0x54, 0x32, 0x10,
    };
    const id = BlockId.from_bytes(test_bytes);

    const hex = try id.to_hex(allocator);
    defer allocator.free(hex);
    try std.testing.expectEqualStrings("0123456789abcdeffedcba9876543210", hex);

    // Test from_hex roundtrip
    const id2 = try BlockId.from_hex("0123456789abcdeffedcba9876543210");
    try std.testing.expect(id.eql(id2));
}

test "ContextBlock serialization roundtrip" {
    const allocator = std.testing.allocator;

    // Create test block
    const test_id = try BlockId.from_hex("0123456789abcdeffedcba9876543210");
    const original = ContextBlock{
        .id = test_id,
        .version = 42,
        .source_uri = "git://example.com/repo.git/file.zig#L123",
        .metadata_json = "{\"type\":\"function\",\"language\":\"zig\"}",
        .content = "pub fn test_function() void { return; }",
    };

    // Test serialization
    const size = original.serialized_size();
    const buffer = try allocator.alloc(u8, size);
    defer allocator.free(buffer);

    const written = try original.serialize(buffer);
    try std.testing.expectEqual(size, written);

    // Test deserialization
    const deserialized = try ContextBlock.deserialize(buffer, allocator);
    defer deserialized.deinit(allocator);

    // Verify all fields match
    try std.testing.expect(original.id.eql(deserialized.id));
    try std.testing.expectEqual(original.version, deserialized.version);
    try std.testing.expectEqualStrings(original.source_uri, deserialized.source_uri);
    try std.testing.expectEqualStrings(original.metadata_json, deserialized.metadata_json);
    try std.testing.expectEqualStrings(original.content, deserialized.content);
}

test "GraphEdge serialization roundtrip" {
    const allocator = std.testing.allocator;

    // Create test edge
    const source_id = try BlockId.from_hex("0123456789abcdeffedcba9876543210");
    const target_id = try BlockId.from_hex("fedcba9876543210123456789abcdef0");
    const original = GraphEdge{
        .source_id = source_id,
        .target_id = target_id,
        .edge_type = .imports,
    };

    // Test serialization
    var buffer: [40]u8 = undefined;
    const written = try original.serialize(&buffer);
    try std.testing.expectEqual(40, written);

    // Test deserialization
    const deserialized = try GraphEdge.deserialize(&buffer);

    // Verify all fields match
    try std.testing.expect(original.source_id.eql(deserialized.source_id));
    try std.testing.expect(original.target_id.eql(deserialized.target_id));
    try std.testing.expectEqual(original.edge_type, deserialized.edge_type);

    _ = allocator; // Suppress unused variable warning
}

test "ContextBlock validation" {
    const allocator = std.testing.allocator;

    // Valid block
    const valid_block = ContextBlock{
        .id = try BlockId.from_hex("0123456789abcdeffedcba9876543210"),
        .version = 1,
        .source_uri = "git://example.com/repo.git",
        .metadata_json = "{\"type\":\"function\"}",
        .content = "pub fn example() void {}",
    };

    try valid_block.validate(allocator);

    // Invalid JSON metadata
    const invalid_json_block = ContextBlock{
        .id = try BlockId.from_hex("0123456789abcdeffedcba9876543210"),
        .version = 1,
        .source_uri = "git://example.com/repo.git",
        .metadata_json = "{invalid json",
        .content = "pub fn example() void {}",
    };

    try std.testing.expectError(error.InvalidMetadataJson, invalid_json_block.validate(allocator));
}

test "BlockHeader versioned format" {
    const allocator = std.testing.allocator;

    // Test header serialization and deserialization
    const original_header = ContextBlock.BlockHeader{
        .magic = ContextBlock.MAGIC,
        .format_version = ContextBlock.FORMAT_VERSION,
        .flags = 0x1234,
        .id = [_]u8{ 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16 },
        .block_version = 42,
        .source_uri_len = 100,
        .metadata_json_len = 200,
        .content_len = 300,
        .checksum = 0xDEADBEEF,
        .reserved = [_]u8{0} ** 12,
    };

    var buffer: [ContextBlock.BlockHeader.SIZE]u8 = undefined;
    try original_header.serialize(&buffer);

    const deserialized_header = try ContextBlock.BlockHeader.deserialize(&buffer);

    // Verify all fields match
    try std.testing.expectEqual(original_header.magic, deserialized_header.magic);
    try std.testing.expectEqual(original_header.format_version, deserialized_header.format_version);
    try std.testing.expectEqual(original_header.flags, deserialized_header.flags);
    try std.testing.expectEqualSlices(u8, &original_header.id, &deserialized_header.id);
    try std.testing.expectEqual(original_header.block_version, deserialized_header.block_version);
    try std.testing.expectEqual(original_header.source_uri_len, deserialized_header.source_uri_len);
    try std.testing.expectEqual(
        original_header.metadata_json_len,
        deserialized_header.metadata_json_len,
    );
    try std.testing.expectEqual(original_header.content_len, deserialized_header.content_len);
    try std.testing.expectEqual(original_header.checksum, deserialized_header.checksum);

    _ = allocator; // Suppress unused variable warning
}

test "BlockHeader invalid magic" {
    var buffer: [ContextBlock.BlockHeader.SIZE]u8 = undefined;
    @memset(&buffer, 0);

    // Write invalid magic
    std.mem.writeInt(u32, buffer[0..4], 0x12345678, .little);

    try std.testing.expectError(error.InvalidMagic, ContextBlock.BlockHeader.deserialize(&buffer));
}

test "BlockHeader unsupported version" {
    var buffer: [ContextBlock.BlockHeader.SIZE]u8 = undefined;
    @memset(&buffer, 0);

    // Write valid magic
    std.mem.writeInt(u32, buffer[0..4], ContextBlock.MAGIC, .little);
    // Write unsupported version (higher than current)
    std.mem.writeInt(u16, buffer[4..6], ContextBlock.FORMAT_VERSION + 1, .little);

    try std.testing.expectError(
        error.UnsupportedVersion,
        ContextBlock.BlockHeader.deserialize(&buffer),
    );
}

test "BlockHeader reserved bytes validation" {
    var buffer: [ContextBlock.BlockHeader.SIZE]u8 = undefined;
    @memset(&buffer, 0);

    // Set up valid header
    std.mem.writeInt(u32, buffer[0..4], ContextBlock.MAGIC, .little);
    std.mem.writeInt(u16, buffer[4..6], ContextBlock.FORMAT_VERSION, .little);

    // Corrupt reserved bytes
    buffer[ContextBlock.BlockHeader.SIZE - 1] = 0xFF;

    try std.testing.expectError(
        error.InvalidReservedBytes,
        ContextBlock.BlockHeader.deserialize(&buffer),
    );
}

test "ContextBlock versioned serialization" {
    const allocator = std.testing.allocator;

    const original = ContextBlock{
        .id = try BlockId.from_hex("0123456789abcdeffedcba9876543210"),
        .version = 123,
        .source_uri = "test://example.com/file.zig",
        .metadata_json = "{\"test\":true,\"line\":456}",
        .content = "pub fn test_function() void { return; }",
    };

    // Serialize with versioned format
    const buffer_size = original.serialized_size();
    const buffer = try allocator.alloc(u8, buffer_size);
    defer allocator.free(buffer);

    const written = try original.serialize(buffer);
    try std.testing.expectEqual(buffer_size, written);

    // Verify header is present
    try std.testing.expectEqual(ContextBlock.MAGIC, std.mem.readInt(u32, buffer[0..4], .little));
    try std.testing.expectEqual(
        ContextBlock.FORMAT_VERSION,
        std.mem.readInt(u16, buffer[4..6], .little),
    );

    // Deserialize and verify
    const deserialized = try ContextBlock.deserialize(buffer, allocator);
    defer deserialized.deinit(allocator);

    try std.testing.expect(original.id.eql(deserialized.id));
    try std.testing.expectEqual(original.version, deserialized.version);
    try std.testing.expectEqualStrings(original.source_uri, deserialized.source_uri);
    try std.testing.expectEqualStrings(original.metadata_json, deserialized.metadata_json);
    try std.testing.expectEqualStrings(original.content, deserialized.content);
}

test "ContextBlock checksum validation" {
    const allocator = std.testing.allocator;

    const original = ContextBlock{
        .id = try BlockId.from_hex("fedcba9876543210123456789abcdef0"),
        .version = 1,
        .source_uri = "test://checksum.zig",
        .metadata_json = "{}",
        .content = "test content for checksum",
    };

    const buffer_size = original.serialized_size();
    const buffer = try allocator.alloc(u8, buffer_size);
    defer allocator.free(buffer);

    _ = try original.serialize(buffer);

    // Corrupt the content
    buffer[buffer.len - 1] ^= 0xFF;

    // Should fail checksum validation
    try std.testing.expectError(error.InvalidChecksum, ContextBlock.deserialize(buffer, allocator));
}

test "ContextBlock size computation from buffer" {
    const allocator = std.testing.allocator;

    const test_block = ContextBlock{
        .id = try BlockId.from_hex("1111111111111111111111111111111"),
        .version = 1,
        .source_uri = "test://size.zig",
        .metadata_json = "{\"size_test\":true}",
        .content = "content for size testing",
    };

    const buffer_size = test_block.serialized_size();
    const buffer = try allocator.alloc(u8, buffer_size);
    defer allocator.free(buffer);

    _ = try test_block.serialize(buffer);

    // Test size computation from buffer
    const computed_size = try ContextBlock.compute_serialized_size_from_buffer(buffer);
    try std.testing.expectEqual(buffer_size, computed_size);
}
