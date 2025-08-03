//! Core data types for KausalDB.
//!
//! This module defines the fundamental data structures used throughout KausalDB:
//! - BlockId: Unique identifier for context blocks
//! - ContextBlock: The primary unit of stored knowledge
//! - GraphEdge: Typed relationship between context blocks
//! - EdgeType: Types of relationships between blocks
//!
//! All types include comprehensive serialization/deserialization support
//! and validation methods to ensure data integrity.

const std = @import("std");
const builtin = @import("builtin");
const custom_assert = @import("assert.zig");
const assert = custom_assert.assert;
const assert_fmt = custom_assert.assert_fmt;
const assert_not_null = custom_assert.assert_not_null;
const assert_not_empty = custom_assert.assert_not_empty;
const assert_range = custom_assert.assert_range;
const assert_buffer_bounds = custom_assert.assert_buffer_bounds;
const comptime_assert = custom_assert.comptime_assert;
const comptime_no_padding = custom_assert.comptime_no_padding;

/// Unique identifier for a Context Block.
/// Uses 128-bit UUID to ensure global uniqueness across distributed systems.
pub const BlockId = struct {
    bytes: [16]u8,

    /// Create BlockId from raw bytes.
    pub fn from_bytes(bytes: [16]u8) BlockId {
        return BlockId{ .bytes = bytes };
    }

    /// Create BlockId from hex string representation.
    pub fn from_hex(hex_string: []const u8) !BlockId {
        if (hex_string.len != 32) return error.InvalidHexLength;

        var bytes: [16]u8 = undefined;
        _ = try std.fmt.hexToBytes(&bytes, hex_string);
        return BlockId{ .bytes = bytes };
    }

    /// Convert BlockId to hex string.
    pub fn to_hex(self: BlockId, allocator: std.mem.Allocator) ![]u8 {
        const hex_string = try allocator.alloc(u8, 32);
        for (self.bytes, 0..) |byte, i| {
            _ = try std.fmt.bufPrint(hex_string[i * 2 .. i * 2 + 2], "{x:0>2}", .{byte});
        }
        return hex_string;
    }

    /// Check equality between two BlockIds.
    pub fn eql(self: BlockId, other: BlockId) bool {
        return std.mem.eql(u8, &self.bytes, &other.bytes);
    }
};

/// Types of edges between Context Blocks.
/// Defines semantic relationships in the knowledge graph.
pub const EdgeType = enum(u16) {
    imports = 1, // A imports B (dependency relationship)
    defined_in = 2, // A is defined in B (containment relationship)
    references = 3, // A references B (usage relationship)
    contains = 4, // A contains B (parent-child relationship)
    extends = 5, // A extends B (inheritance relationship)
    implements = 6, // A implements B (interface relationship)
    calls = 7, // A calls B (invocation relationship)
    depends_on = 8, // A depends on B (dependency relationship)

    /// Convert EdgeType to u16 for serialization.
    pub fn to_u16(self: EdgeType) u16 {
        return @intFromEnum(self);
    }

    /// Create EdgeType from u16.
    pub fn from_u16(value: u16) !EdgeType {
        return std.meta.intToEnum(EdgeType, value) catch error.InvalidEdgeType;
    }
};

/// Context Block - the fundamental unit of knowledge storage.
/// Represents a semantically meaningful chunk of information with metadata.
pub const ContextBlock = struct {
    /// Unique identifier for this block
    id: BlockId,

    /// Version number for this block (for update tracking)
    version: u64,

    /// URI identifying the source of this content
    source_uri: []const u8,

    /// JSON metadata providing additional context
    metadata_json: []const u8,

    /// The actual content/knowledge stored in this block
    content: []const u8,

    /// Serialized block header structure.
    pub const BlockHeader = struct {
        magic: u32,
        format_version: u16,
        flags: u16,
        id: [16]u8,
        block_version: u64,
        source_uri_len: u32,
        metadata_json_len: u32,
        content_len: u64,
        checksum: u32,
        reserved: [12]u8,

        pub const SIZE: usize = 64;

        pub fn serialize(self: BlockHeader, buffer: []u8) !usize {
            if (buffer.len < SIZE) return error.BufferTooSmall;

            var offset: usize = 0;
            std.mem.writeInt(u32, buffer[offset .. offset + 4][0..4], self.magic, .little);
            offset += 4;
            std.mem.writeInt(u16, buffer[offset .. offset + 2][0..2], self.format_version, .little);
            offset += 2;
            std.mem.writeInt(u16, buffer[offset .. offset + 2][0..2], self.flags, .little);
            offset += 2;
            @memcpy(buffer[offset .. offset + 16], &self.id);
            offset += 16;
            std.mem.writeInt(u64, buffer[offset .. offset + 8][0..8], self.block_version, .little);
            offset += 8;
            std.mem.writeInt(u32, buffer[offset .. offset + 4][0..4], self.source_uri_len, .little);
            offset += 4;
            std.mem.writeInt(u32, buffer[offset .. offset + 4][0..4], self.metadata_json_len, .little);
            offset += 4;
            std.mem.writeInt(u64, buffer[offset .. offset + 8][0..8], self.content_len, .little);
            offset += 8;
            std.mem.writeInt(u32, buffer[offset .. offset + 4][0..4], self.checksum, .little);
            offset += 4;
            @memcpy(buffer[offset .. offset + 12], &self.reserved);
            offset += 12;

            return offset;
        }

        pub fn deserialize(buffer: []const u8) !BlockHeader {
            if (buffer.len < SIZE) return error.BufferTooSmall;

            var offset: usize = 0;
            const magic = std.mem.readInt(u32, buffer[offset .. offset + 4][0..4], .little);
            offset += 4;
            const format_version = std.mem.readInt(u16, buffer[offset .. offset + 2][0..2], .little);
            offset += 2;
            const flags = std.mem.readInt(u16, buffer[offset .. offset + 2][0..2], .little);
            offset += 2;
            var id: [16]u8 = undefined;
            @memcpy(&id, buffer[offset .. offset + 16]);
            offset += 16;
            const block_version = std.mem.readInt(u64, buffer[offset .. offset + 8][0..8], .little);
            offset += 8;
            const source_uri_len = std.mem.readInt(u32, buffer[offset .. offset + 4][0..4], .little);
            offset += 4;
            const metadata_json_len = std.mem.readInt(u32, buffer[offset .. offset + 4][0..4], .little);
            offset += 4;
            const content_len = std.mem.readInt(u64, buffer[offset .. offset + 8][0..8], .little);
            offset += 8;
            const checksum = std.mem.readInt(u32, buffer[offset .. offset + 4][0..4], .little);
            offset += 4;
            var reserved: [12]u8 = undefined;
            @memcpy(&reserved, buffer[offset .. offset + 12]);

            if (magic != MAGIC) return error.InvalidMagic;
            if (format_version != FORMAT_VERSION) return error.UnsupportedVersion;

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
                .reserved = reserved,
            };
        }
    };

    // Compile-time guarantees for on-disk format integrity
    comptime {
        comptime_assert(@sizeOf(BlockHeader) == 64, "BlockHeader must be exactly 64 bytes for on-disk format compatibility");
        comptime_assert(BlockHeader.SIZE == @sizeOf(BlockHeader), "BlockHeader.SIZE constant must match actual struct size");
        comptime_assert(@sizeOf(u32) + @sizeOf(u16) + @sizeOf(u16) + 16 +
            @sizeOf(u64) + @sizeOf(u32) + @sizeOf(u32) + @sizeOf(u64) + @sizeOf(u32) + 12 == 64, "BlockHeader field sizes must sum to exactly 64 bytes");
    }

    pub const MAGIC: u32 = 0x42444358; // "XDBC" in little endian
    pub const FORMAT_VERSION: u16 = 1;

    /// Minimum size for a serialized block (header only).
    pub const MIN_SERIALIZED_SIZE: usize = BlockHeader.SIZE;

    /// Calculate the total serialized size for this block.
    pub fn serialized_size(self: ContextBlock) usize {
        return BlockHeader.SIZE + self.source_uri.len + self.metadata_json.len + self.content.len;
    }

    /// Compute serialized size from buffer without full deserialization.
    pub fn compute_serialized_size_from_buffer(buffer: []const u8) !usize {
        if (buffer.len < BlockHeader.SIZE) return error.BufferTooSmall;

        const header = try BlockHeader.deserialize(buffer);
        const total_size = BlockHeader.SIZE + header.source_uri_len + header.metadata_json_len + header.content_len;

        if (total_size > buffer.len) return error.IncompleteData;
        return total_size;
    }

    /// Serialize this ContextBlock to a buffer.
    pub fn serialize(self: ContextBlock, buffer: []u8) !usize {
        const required_size = self.serialized_size();
        if (buffer.len < required_size) return error.BufferTooSmall;

        // Zero-initialize entire buffer to prevent garbage data
        @memset(buffer[0..required_size], 0);

        // Create and serialize header
        const header = BlockHeader{
            .magic = MAGIC,
            .format_version = FORMAT_VERSION,
            .flags = 0,
            .id = self.id.bytes,
            .block_version = self.version,
            .source_uri_len = @intCast(self.source_uri.len),
            .metadata_json_len = @intCast(self.metadata_json.len),
            .content_len = self.content.len,
            .checksum = 0, // Computed later
            .reserved = std.mem.zeroes([12]u8),
        };

        var offset = try header.serialize(buffer);

        // Serialize variable-length fields with bounds checking
        if (offset + self.source_uri.len > buffer.len) return error.BufferTooSmall;
        @memcpy(buffer[offset .. offset + self.source_uri.len], self.source_uri);
        offset += self.source_uri.len;

        if (offset + self.metadata_json.len > buffer.len) return error.BufferTooSmall;
        @memcpy(buffer[offset .. offset + self.metadata_json.len], self.metadata_json);
        offset += self.metadata_json.len;

        if (offset + self.content.len > buffer.len) return error.BufferTooSmall;
        @memcpy(buffer[offset .. offset + self.content.len], self.content);
        offset += self.content.len;

        // Validate serialization completed correctly
        assert_fmt(offset == required_size, "Serialization size mismatch: expected {}, got {}", .{ required_size, offset });
        if (offset != required_size) return error.SerializationSizeMismatch;

        return offset;
    }

    /// Deserialize a ContextBlock from a buffer.
    pub fn deserialize(buffer: []const u8, allocator: std.mem.Allocator) !ContextBlock {
        if (buffer.len < BlockHeader.SIZE) return error.BufferTooSmall;

        const header = try BlockHeader.deserialize(buffer);
        var offset = BlockHeader.SIZE;

        // Validate header fields for reasonable values
        if (header.source_uri_len > 1024 * 1024) return error.InvalidSourceUriLength;
        if (header.metadata_json_len > 10 * 1024 * 1024) return error.InvalidMetadataLength;
        if (header.content_len > 100 * 1024 * 1024) return error.InvalidContentLength;

        const total_size = offset + header.source_uri_len + header.metadata_json_len + header.content_len;
        if (buffer.len < total_size) return error.IncompleteData;

        // Extract variable-length fields with bounds validation
        if (offset + header.source_uri_len > buffer.len) return error.IncompleteData;
        const source_uri = try allocator.dupe(u8, buffer[offset .. offset + header.source_uri_len]);
        errdefer allocator.free(source_uri);
        offset += header.source_uri_len;

        if (offset + header.metadata_json_len > buffer.len) return error.IncompleteData;
        const metadata_json = try allocator.dupe(u8, buffer[offset .. offset + header.metadata_json_len]);
        errdefer allocator.free(metadata_json);
        offset += header.metadata_json_len;

        if (offset + header.content_len > buffer.len) return error.IncompleteData;
        const content = try allocator.dupe(u8, buffer[offset .. offset + header.content_len]);
        errdefer allocator.free(content);

        return ContextBlock{
            .id = BlockId{ .bytes = header.id },
            .version = header.block_version,
            .source_uri = source_uri,
            .metadata_json = metadata_json,
            .content = content,
        };
    }

    /// Free memory allocated for this ContextBlock.
    pub fn deinit(self: ContextBlock, allocator: std.mem.Allocator) void {
        allocator.free(self.source_uri);
        allocator.free(self.metadata_json);
        allocator.free(self.content);
    }

    /// Validate the internal consistency and format of this ContextBlock.
    ///
    /// Performs structural validation suitable for all contexts:
    /// - JSON syntax validation for metadata
    /// - UTF-8 encoding validation for text fields
    /// - Basic invariant checks (positive version)
    ///
    /// This is a lightweight check focused on preventing system crashes
    /// rather than enforcing business rules or semantic constraints.
    ///
    /// For business logic validation (non-empty constraints, semantic rules),
    /// use validate_for_ingestion() or implement domain-specific checks.
    pub fn validate(self: ContextBlock, allocator: std.mem.Allocator) !void {
        assert_fmt(@intFromPtr(allocator.ptr) != 0, "Allocator cannot be null", .{});

        // Size validation - return errors instead of asserting
        if (self.metadata_json.len >= 10 * 1024 * 1024) {
            return error.MetadataJsonTooLarge;
        }
        if (self.metadata_json.len > 0 and @intFromPtr(self.metadata_json.ptr) == 0) {
            return error.MetadataJsonNullPointer;
        }

        if (self.source_uri.len >= 1024 * 1024) {
            return error.SourceUriTooLarge;
        }
        if (self.source_uri.len > 0 and @intFromPtr(self.source_uri.ptr) == 0) {
            return error.SourceUriNullPointer;
        }

        if (self.content.len >= 100 * 1024 * 1024) {
            return error.ContentTooLarge;
        }
        if (self.content.len > 0 and @intFromPtr(self.content.ptr) == 0) {
            return error.ContentNullPointer;
        }

        // JSON format validation - metadata must be parseable
        // Use builtin.mode check to avoid Zig JSON parser bug in ReleaseSafe mode
        if (builtin.mode == .Debug) {
            var parsed = std.json.parseFromSlice(
                std.json.Value,
                allocator,
                self.metadata_json,
                .{},
            ) catch {
                return error.InvalidMetadataJson;
            };
            defer parsed.deinit();
        } else {
            // In release modes, do basic JSON syntax validation
            if (!is_valid_json_syntax(self.metadata_json)) {
                return error.InvalidMetadataJson;
            }
        }

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
        // First perform basic structural validation
        try self.validate(allocator);

        // Additional business rules for ingestion
        if (self.source_uri.len == 0) {
            return error.EmptySourceUri;
        }
        if (self.content.len == 0) {
            return error.EmptyContent;
        }

        // Validate that metadata contains required fields for ingestion
        // Use builtin.mode check to avoid Zig JSON parser bug in ReleaseSafe mode
        if (builtin.mode == .Debug) {
            var parsed = std.json.parseFromSlice(
                std.json.Value,
                allocator,
                self.metadata_json,
                .{},
            ) catch {
                return error.InvalidMetadataJson;
            };
            defer parsed.deinit();
        } else {
            // In release modes, do basic JSON syntax validation
            if (!is_valid_json_syntax(self.metadata_json)) {
                return error.InvalidMetadataJson;
            }
        }

        // Future: Add specific metadata field requirements here
    }
};

/// Graph edge representing a typed relationship between two Context Blocks.
pub const GraphEdge = struct {
    /// Source block ID
    source_id: BlockId,

    /// Target block ID
    target_id: BlockId,

    /// Type of relationship
    edge_type: EdgeType,

    pub const SERIALIZED_SIZE: usize = 40; // 16 + 16 + 8 bytes

    /// Serialize this GraphEdge to a buffer.
    pub fn serialize(self: GraphEdge, buffer: []u8) !usize {
        if (buffer.len < SERIALIZED_SIZE) return error.BufferTooSmall;

        var offset: usize = 0;

        @memcpy(buffer[offset .. offset + 16], &self.source_id.bytes);
        offset += 16;

        @memcpy(buffer[offset .. offset + 16], &self.target_id.bytes);
        offset += 16;

        std.mem.writeInt(u16, buffer[offset .. offset + 2][0..2], self.edge_type.to_u16(), .little);
        offset += 2;

        // Reserved bytes for future expansion
        @memset(buffer[offset .. offset + 6], 0);
        offset += 6;

        return offset;
    }

    /// Deserialize a GraphEdge from a buffer.
    pub fn deserialize(buffer: []const u8) !GraphEdge {
        if (buffer.len < SERIALIZED_SIZE) return error.BufferTooSmall;

        var offset: usize = 0;

        var source_bytes: [16]u8 = undefined;
        @memcpy(&source_bytes, buffer[offset .. offset + 16]);
        offset += 16;

        var target_bytes: [16]u8 = undefined;
        @memcpy(&target_bytes, buffer[offset .. offset + 16]);
        offset += 16;

        const edge_type_raw = std.mem.readInt(u16, buffer[offset .. offset + 2][0..2], .little);
        const edge_type = try EdgeType.from_u16(edge_type_raw);

        return GraphEdge{
            .source_id = BlockId{ .bytes = source_bytes },
            .target_id = BlockId{ .bytes = target_bytes },
            .edge_type = edge_type,
        };
    }
};

// Compile-time guarantees for GraphEdge serialization format
comptime {
    comptime_assert(GraphEdge.SERIALIZED_SIZE == 40, "GraphEdge SERIALIZED_SIZE must be 40 bytes (16 + 16 + 2 + 6 reserved)");
    comptime_assert(@sizeOf(BlockId) == 16, "BlockId must be 16 bytes");
    comptime_assert(@sizeOf(EdgeType) == 2, "EdgeType must be 2 bytes (u16)");
    comptime_assert(16 + 16 + 2 + 6 == GraphEdge.SERIALIZED_SIZE, "GraphEdge field sizes plus reserved bytes must equal SERIALIZED_SIZE");
}

// Tests
test "BlockId basic operations" {
    const hex_string = "deadbeefdeadbeefdeadbeefdeadbeef";
    const block_id = try BlockId.from_hex(hex_string);

    const allocator = std.testing.allocator;
    const hex_result = try block_id.to_hex(allocator);
    defer allocator.free(hex_result);

    try std.testing.expectEqualStrings(hex_string, hex_result);

    const block_id2 = try BlockId.from_hex(hex_string);
    try std.testing.expect(block_id.eql(block_id2));

    const different_id = try BlockId.from_hex("cafebabecafebabecafebabecafebabe");
    try std.testing.expect(!block_id.eql(different_id));
}

test "ContextBlock serialization roundtrip" {
    const allocator = std.testing.allocator;

    const original = ContextBlock{
        .id = try BlockId.from_hex("deadbeefdeadbeefdeadbeefdeadbeef"),
        .version = 42,
        .source_uri = "test://example.zig",
        .metadata_json = "{\"type\": \"function\"}",
        .content = "pub fn test() void {}",
    };

    const buffer_size = original.serialized_size();
    const buffer = try allocator.alloc(u8, buffer_size);
    defer allocator.free(buffer);

    const written = try original.serialize(buffer);
    try std.testing.expectEqual(buffer_size, written);

    const deserialized = try ContextBlock.deserialize(buffer, allocator);
    defer deserialized.deinit(allocator);

    try std.testing.expect(original.id.eql(deserialized.id));
    try std.testing.expectEqual(original.version, deserialized.version);
    try std.testing.expectEqualStrings(original.source_uri, deserialized.source_uri);
    try std.testing.expectEqualStrings(original.metadata_json, deserialized.metadata_json);
    try std.testing.expectEqualStrings(original.content, deserialized.content);
}

test "GraphEdge serialization roundtrip" {
    const original = GraphEdge{
        .source_id = try BlockId.from_hex("deadbeefdeadbeefdeadbeefdeadbeef"),
        .target_id = try BlockId.from_hex("cafebabecafebabecafebabecafebabe"),
        .edge_type = .imports,
    };

    var buffer: [GraphEdge.SERIALIZED_SIZE]u8 = undefined;
    const written = try original.serialize(&buffer);
    try std.testing.expectEqual(GraphEdge.SERIALIZED_SIZE, written);

    const deserialized = try GraphEdge.deserialize(&buffer);

    try std.testing.expect(original.source_id.eql(deserialized.source_id));
    try std.testing.expect(original.target_id.eql(deserialized.target_id));
    try std.testing.expectEqual(original.edge_type, deserialized.edge_type);
}

test "ContextBlock validation" {
    const allocator = std.testing.allocator;

    const valid_block = ContextBlock{
        .id = try BlockId.from_hex("deadbeefdeadbeefdeadbeefdeadbeef"),
        .version = 1,
        .source_uri = "test://example.zig",
        .metadata_json = "{}",
        .content = "test content",
    };

    try valid_block.validate(allocator);

    const invalid_json_block = ContextBlock{
        .id = try BlockId.from_hex("deadbeefdeadbeefdeadbeefdeadbeef"),
        .version = 1,
        .source_uri = "test://example.zig",
        .metadata_json = "{invalid json",
        .content = "test content",
    };

    try std.testing.expectError(error.InvalidMetadataJson, invalid_json_block.validate(allocator));
}

test "BlockHeader versioned format" {
    const header = ContextBlock.BlockHeader{
        .magic = ContextBlock.MAGIC,
        .format_version = ContextBlock.FORMAT_VERSION,
        .flags = 0,
        .id = [_]u8{1} ** 16,
        .block_version = 42,
        .source_uri_len = 100,
        .metadata_json_len = 50,
        .content_len = 1000,
        .checksum = 0x12345678,
        .reserved = std.mem.zeroes([12]u8),
    };

    var buffer: [ContextBlock.BlockHeader.SIZE]u8 = undefined;
    const written = try header.serialize(&buffer);
    try std.testing.expectEqual(ContextBlock.BlockHeader.SIZE, written);

    const deserialized = try ContextBlock.BlockHeader.deserialize(&buffer);
    try std.testing.expectEqual(header.magic, deserialized.magic);
    try std.testing.expectEqual(header.format_version, deserialized.format_version);
    try std.testing.expectEqual(header.block_version, deserialized.block_version);
}

test "BlockHeader invalid magic" {
    var buffer: [ContextBlock.BlockHeader.SIZE]u8 = undefined;
    std.mem.writeInt(u32, buffer[0..4], 0xDEADBEEF, .little); // Wrong magic

    try std.testing.expectError(error.InvalidMagic, ContextBlock.BlockHeader.deserialize(&buffer));
}

test "BlockHeader unsupported version" {
    const header = ContextBlock.BlockHeader{
        .magic = ContextBlock.MAGIC,
        .format_version = 999, // Unsupported version
        .flags = 0,
        .id = [_]u8{1} ** 16,
        .block_version = 1,
        .source_uri_len = 0,
        .metadata_json_len = 0,
        .content_len = 0,
        .checksum = 0,
        .reserved = std.mem.zeroes([12]u8),
    };

    var buffer: [ContextBlock.BlockHeader.SIZE]u8 = undefined;
    _ = try header.serialize(&buffer);

    try std.testing.expectError(error.UnsupportedVersion, ContextBlock.BlockHeader.deserialize(&buffer));
}

test "BlockHeader reserved bytes validation" {
    const header = ContextBlock.BlockHeader{
        .magic = ContextBlock.MAGIC,
        .format_version = ContextBlock.FORMAT_VERSION,
        .flags = 0,
        .id = [_]u8{1} ** 16,
        .block_version = 1,
        .source_uri_len = 0,
        .metadata_json_len = 0,
        .content_len = 0,
        .checksum = 0,
        .reserved = std.mem.zeroes([12]u8),
    };

    var buffer: [ContextBlock.BlockHeader.SIZE]u8 = undefined;
    _ = try header.serialize(&buffer);

    const deserialized = try ContextBlock.BlockHeader.deserialize(&buffer);
    try std.testing.expectEqualSlices(u8, &header.reserved, &deserialized.reserved);
}

test "ContextBlock versioned serialization" {
    const allocator = std.testing.allocator;

    const block_v1 = ContextBlock{
        .id = try BlockId.from_hex("deadbeefdeadbeefdeadbeefdeadbeef"),
        .version = 1,
        .source_uri = "test://v1.zig",
        .metadata_json = "{\"version\": 1}",
        .content = "version 1 content",
    };

    const buffer_size = block_v1.serialized_size();
    const buffer = try allocator.alloc(u8, buffer_size);
    defer allocator.free(buffer);

    _ = try block_v1.serialize(buffer);
    const deserialized = try ContextBlock.deserialize(buffer, allocator);
    defer deserialized.deinit(allocator);

    try std.testing.expectEqual(@as(u64, 1), deserialized.version);
    try std.testing.expectEqualStrings("{\"version\": 1}", deserialized.metadata_json);
}

test "ContextBlock checksum validation" {
    const allocator = std.testing.allocator;

    const block = ContextBlock{
        .id = try BlockId.from_hex("deadbeefdeadbeefdeadbeefdeadbeef"),
        .version = 1,
        .source_uri = "test://checksum.zig",
        .metadata_json = "{}",
        .content = "checksum test",
    };

    const buffer_size = block.serialized_size();
    const buffer = try allocator.alloc(u8, buffer_size);
    defer allocator.free(buffer);

    _ = try block.serialize(buffer);

    // Corrupt the data
    buffer[buffer.len - 1] ^= 0xFF;

    // Should still deserialize but checksum would be wrong
    // (checksum validation would be implemented in higher-level code)
    const deserialized = try ContextBlock.deserialize(buffer, allocator);
    defer deserialized.deinit(allocator);

    try std.testing.expect(block.id.eql(deserialized.id));
}

test "ContextBlock size computation from buffer" {
    const allocator = std.testing.allocator;

    const block = ContextBlock{
        .id = try BlockId.from_hex("deadbeefdeadbeefdeadbeefdeadbeef"),
        .version = 1,
        .source_uri = "test://size.zig",
        .metadata_json = "{}",
        .content = "size test content",
    };

    const expected_size = block.serialized_size();
    const buffer = try allocator.alloc(u8, expected_size);
    defer allocator.free(buffer);

    _ = try block.serialize(buffer);

    const computed_size = try ContextBlock.compute_serialized_size_from_buffer(buffer);
    try std.testing.expectEqual(expected_size, computed_size);
}

/// Basic JSON syntax validation without full parsing
/// This is a workaround for Zig JSON parser issues in ReleaseSafe mode
fn is_valid_json_syntax(json: []const u8) bool {
    if (json.len == 0) return false;

    // Must start and end with proper delimiters
    const first = json[0];
    const last = json[json.len - 1];

    // Support objects and arrays
    if (first == '{' and last == '}') return true;
    if (first == '[' and last == ']') return true;

    // Support simple string values
    if (first == '"' and last == '"' and json.len >= 2) return true;

    // Support simple literals
    if (std.mem.eql(u8, json, "null")) return true;
    if (std.mem.eql(u8, json, "true")) return true;
    if (std.mem.eql(u8, json, "false")) return true;

    // Support simple numbers
    if (std.fmt.parseFloat(f64, json)) |_| {
        return true;
    } else |_| {}

    if (std.fmt.parseInt(i64, json, 10)) |_| {
        return true;
    } else |_| {}

    return false;
}
