//! Context Block data structure and serialization.
//!
//! Context Blocks are the atomic unit of knowledge in CortexDB. They represent
//! any logical chunk of information (functions, classes, document paragraphs, etc.)
//! with structured metadata and relationships to other blocks.

const std = @import("std");
const assert = std.debug.assert;

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
        return std.fmt.allocPrint(allocator, "{}", .{std.fmt.fmtSliceHexLower(&self.bytes)});
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

    /// Calculate the serialized size in bytes.
    pub fn serialized_size(self: ContextBlock) usize {
        return 16 + // id
            8 + // version
            4 + // source_uri_len
            4 + // metadata_json_len
            4 + // content_len
            self.source_uri.len +
            self.metadata_json.len +
            self.content.len;
    }

    /// Serialize the Context Block to bytes according to the specification.
    /// Layout: | id (16) | ver (8) | uri_len (4) | meta_len (4) | content_len (4) |
    ///         | uri (...) | metadata (...) | content (...) |
    pub fn serialize(self: ContextBlock, buffer: []u8) !usize {
        const required_size = self.serialized_size();
        assert(buffer.len >= required_size);
        if (buffer.len < required_size) return error.BufferTooSmall;

        var offset: usize = 0;

        // Write ID (16 bytes)
        @memcpy(buffer[offset .. offset + 16], &self.id.bytes);
        offset += 16;

        // Write version (8 bytes, little-endian)
        std.mem.writeInt(u64, buffer[offset..][0..8], self.version, .little);
        offset += 8;

        // Write lengths (4 bytes each, little-endian)
        std.mem.writeInt(u32, buffer[offset..][0..4], @intCast(self.source_uri.len), .little);
        offset += 4;
        std.mem.writeInt(u32, buffer[offset..][0..4], @intCast(self.metadata_json.len), .little);
        offset += 4;
        std.mem.writeInt(u32, buffer[offset..][0..4], @intCast(self.content.len), .little);
        offset += 4;

        // Write variable-length data
        @memcpy(buffer[offset .. offset + self.source_uri.len], self.source_uri);
        offset += self.source_uri.len;
        @memcpy(buffer[offset .. offset + self.metadata_json.len], self.metadata_json);
        offset += self.metadata_json.len;
        @memcpy(buffer[offset .. offset + self.content.len], self.content);
        offset += self.content.len;

        return offset;
    }

    /// Deserialize a Context Block from bytes.
    /// The returned block references slices into the input buffer,
    /// so the buffer must remain valid for the lifetime of the block.
    pub fn deserialize(buffer: []const u8, allocator: std.mem.Allocator) !ContextBlock {
        assert(buffer.len > 0);
        assert(buffer.len >= 36);
        if (buffer.len == 0) return error.EmptyBuffer;
        if (buffer.len < 36) return error.BufferTooSmall;

        var offset: usize = 0;

        // Read ID (16 bytes)
        var id_bytes: [16]u8 = undefined;
        @memcpy(&id_bytes, buffer[offset .. offset + 16]);
        const id = BlockId.from_bytes(id_bytes);
        offset += 16;

        // Read version (8 bytes, little-endian)
        const version = std.mem.readInt(u64, buffer[offset..][0..8], .little);
        offset += 8;

        // Read lengths (4 bytes each, little-endian)
        const source_uri_len = std.mem.readInt(u32, buffer[offset..][0..4], .little);
        offset += 4;
        const metadata_json_len = std.mem.readInt(u32, buffer[offset..][0..4], .little);
        offset += 4;
        const content_len = std.mem.readInt(u32, buffer[offset..][0..4], .little);
        offset += 4;

        // Validate total size
        const total_size = offset + source_uri_len + metadata_json_len + content_len;
        if (buffer.len < total_size) return error.BufferTooSmall;

        // Copy variable-length data to ensure ownership
        const source_uri = try allocator.dupe(u8, buffer[offset .. offset + source_uri_len]);
        offset += source_uri_len;
        const metadata_json = try allocator.dupe(u8, buffer[offset .. offset + metadata_json_len]);
        offset += metadata_json_len;
        const content = try allocator.dupe(u8, buffer[offset .. offset + content_len]);

        return ContextBlock{
            .id = id,
            .version = version,
            .source_uri = source_uri,
            .metadata_json = metadata_json,
            .content = content,
        };
    }

    /// Free allocated memory for the Context Block.
    pub fn deinit(self: ContextBlock, allocator: std.mem.Allocator) void {
        allocator.free(self.source_uri);
        allocator.free(self.metadata_json);
        allocator.free(self.content);
    }

    /// Validate that the Context Block has valid structure.
    pub fn validate(self: ContextBlock, allocator: std.mem.Allocator) !void {
        // Validate JSON metadata by attempting to parse it
        var parsed = std.json.parseFromSlice(
            std.json.Value,
            allocator,
            self.metadata_json,
            .{},
        ) catch {
            return error.InvalidMetadataJson;
        };
        defer parsed.deinit();

        // Validate UTF-8 encoding
        if (!std.unicode.utf8ValidateSlice(self.source_uri)) {
            return error.InvalidSourceUriEncoding;
        }
        if (!std.unicode.utf8ValidateSlice(self.metadata_json)) {
            return error.InvalidMetadataEncoding;
        }
        if (!std.unicode.utf8ValidateSlice(self.content)) {
            return error.InvalidContentEncoding;
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

        // Validate reserved bytes are zero
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
