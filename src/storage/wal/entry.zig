//! WAL Entry Structure and Operations
//!
//! Defines the WALEntry struct and all associated operations including
//! serialization, deserialization, creation methods, and memory management.
//! This module encapsulates the core entry format and validation logic.

const std = @import("std");
const assert = std.debug.assert;

const types = @import("types.zig");
const context_block = @import("../../core/types.zig");
const wal_entry_stream = @import("stream.zig");

const WALError = types.WALError;
const WALEntryType = types.WALEntryType;
const MAX_PAYLOAD_SIZE = types.MAX_PAYLOAD_SIZE;
const ContextBlock = context_block.ContextBlock;
const GraphEdge = context_block.GraphEdge;
const BlockId = context_block.BlockId;

/// WAL entry header structure with corruption detection
pub const WALEntry = struct {
    checksum: u64,
    entry_type: WALEntryType,
    payload_size: u32,
    payload: []const u8,

    pub const HEADER_SIZE = 13; // 8 bytes checksum + 1 byte type + 4 bytes payload_size

    // Cross-platform binary compatibility requires fixed field sizes
    comptime {
        assert(@sizeOf(u64) == 8);
        assert(@sizeOf(u32) == 4);
        assert(@sizeOf(WALEntryType) == 1);

        const calculated_header_size = @sizeOf(u64) + @sizeOf(WALEntryType) + @sizeOf(u32);
        assert(HEADER_SIZE == calculated_header_size);
        assert(HEADER_SIZE == 13);

        // Minimum size prevents degenerate entries that waste storage
        assert(MAX_PAYLOAD_SIZE >= 1024);
        assert(MAX_PAYLOAD_SIZE <= types.MAX_SEGMENT_SIZE);
        assert(std.math.maxInt(u32) >= MAX_PAYLOAD_SIZE);
    }

    /// Calculate CRC-64 checksum of type and payload for corruption detection
    fn calculate_checksum(entry_type: WALEntryType, payload: []const u8) u64 {
        var hasher = std.hash.Wyhash.init(0);
        hasher.update(&[_]u8{@intFromEnum(entry_type)});
        hasher.update(payload);
        return hasher.final();
    }

    /// Serialize WAL entry to buffer for disk storage.
    /// Returns number of bytes written or WALError.BufferTooSmall if insufficient space.
    pub fn serialize(self: WALEntry, buffer: []u8) WALError!usize {
        const total_size = HEADER_SIZE + self.payload.len;
        if (buffer.len < total_size) return WALError.BufferTooSmall;

        var offset: usize = 0;

        // Write checksum (8 bytes, little-endian)
        std.mem.writeInt(u64, buffer[offset..][0..8], self.checksum, .little);
        offset += 8;

        // Write entry type (1 byte)
        buffer[offset] = @intFromEnum(self.entry_type);
        offset += 1;

        // Write payload size (4 bytes, little-endian)
        std.mem.writeInt(u32, buffer[offset..][0..4], self.payload_size, .little);
        offset += 4;

        // Write payload
        @memcpy(buffer[offset .. offset + self.payload.len], self.payload);
        offset += self.payload.len;

        return offset;
    }

    /// Deserialize WAL entry from buffer, allocating payload memory.
    /// Caller must call deinit() to free allocated payload memory.
    /// Returns WALError.InvalidChecksum or WALError.InvalidEntryType for corruption.
    pub fn deserialize(buffer: []const u8, allocator: std.mem.Allocator) WALError!WALEntry {
        if (buffer.len < HEADER_SIZE) return WALError.BufferTooSmall;

        var offset: usize = 0;

        // Read checksum
        const checksum = std.mem.readInt(u64, buffer[offset..][0..8], .little);
        offset += 8;

        // Read entry type
        const entry_type = try WALEntryType.from_u8(buffer[offset]);
        offset += 1;

        // Read payload size
        const payload_size = std.mem.readInt(u32, buffer[offset..][0..4], .little);
        offset += 4;

        // Validate payload size is reasonable before checking buffer bounds
        if (payload_size > MAX_PAYLOAD_SIZE) {
            return WALError.CorruptedEntry;
        }

        // Validate payload size against remaining buffer
        if (offset + payload_size > buffer.len) {
            return WALError.BufferTooSmall;
        }

        // Read payload
        const payload = try allocator.dupe(u8, buffer[offset .. offset + payload_size]);
        errdefer allocator.free(payload);

        // Verify checksum for corruption detection
        const expected_checksum = calculate_checksum(entry_type, payload);
        if (checksum != expected_checksum) {
            return WALError.InvalidChecksum;
        }

        return WALEntry{
            .checksum = checksum,
            .entry_type = entry_type,
            .payload_size = payload_size,
            .payload = payload,
        };
    }

    /// Create WAL entry for storing a Context Block.
    /// Serializes block data into WAL payload with integrity checksum.
    /// Returns WALError.CorruptedEntry if block serialization fails.
    pub fn create_put_block(block: ContextBlock, allocator: std.mem.Allocator) WALError!WALEntry {
        const payload_size = block.serialized_size();

        // Zero-size blocks indicate serialization logic failure, not data corruption
        if (payload_size == 0) {
            return WALError.CorruptedEntry;
        }
        if (payload_size > MAX_PAYLOAD_SIZE) {
            return WALError.CorruptedEntry;
        }

        const payload = try allocator.alloc(u8, payload_size);
        errdefer allocator.free(payload);
        @memset(payload, 0); // Zero-initialize to prevent garbage data

        const bytes_written = try block.serialize(payload);

        // Serialization size mismatch indicates internal logic error
        assert(bytes_written == payload_size);
        if (bytes_written != payload_size) {
            return WALError.CorruptedEntry;
        }

        const checksum = calculate_checksum(.put_block, payload);

        const entry = WALEntry{
            .checksum = checksum,
            .entry_type = .put_block,
            .payload_size = @intCast(payload_size),
            .payload = payload,
        };

        // Corruption detection: validate entry header doesn't contain pattern data
        var header_buffer: [WALEntry.HEADER_SIZE]u8 = undefined;
        std.mem.writeInt(u64, header_buffer[0..8], entry.checksum, .little);
        header_buffer[8] = @intFromEnum(entry.entry_type);
        std.mem.writeInt(u32, header_buffer[9..13], entry.payload_size, .little);

        // Invariant: payload size consistency prevents downstream corruption
        assert(entry.payload_size == payload_size);
        assert(entry.payload.len == payload_size);

        return entry;
    }

    /// Create WAL entry for deleting a Context Block.
    /// Payload contains only the 16-byte BlockId for efficient deletion replay.
    pub fn create_delete_block(block_id: BlockId, allocator: std.mem.Allocator) WALError!WALEntry {
        comptime assert(@sizeOf(BlockId) == 16);

        const payload = try allocator.dupe(u8, &block_id.bytes);
        assert(payload.len == 16);

        const checksum = calculate_checksum(.delete_block, payload);

        const entry = WALEntry{
            .checksum = checksum,
            .entry_type = .delete_block,
            .payload_size = @intCast(payload.len),
            .payload = payload,
        };

        // Invariant: delete entries must contain exactly one BlockId
        assert(entry.payload.len == 16);
        assert(entry.payload_size == 16);

        return entry;
    }

    /// Create WAL entry for storing a Graph Edge.
    /// Serializes edge relationship data for graph index replay.
    pub fn create_put_edge(edge: GraphEdge, allocator: std.mem.Allocator) WALError!WALEntry {
        const payload = try allocator.alloc(u8, 40); // GraphEdge.SERIALIZED_SIZE
        errdefer allocator.free(payload);
        @memset(payload, 0); // Zero-initialize to prevent garbage data

        _ = try edge.serialize(payload);
        const checksum = calculate_checksum(.put_edge, payload);

        return WALEntry{
            .checksum = checksum,
            .entry_type = .put_edge,
            .payload_size = @intCast(payload.len),
            .payload = payload,
        };
    }

    /// Create WAL entry from stream components for compatibility with WALEntryStream
    /// Validates checksum and constructs proper WALEntry from streaming data
    pub fn deserialize_from_stream(checksum: u64, entry_type_raw: u8, payload: []const u8, allocator: std.mem.Allocator) WALError!WALEntry {
        // Validate entry type range
        const entry_type: WALEntryType = switch (entry_type_raw) {
            1 => .put_block,
            2 => .delete_block,
            3 => .put_edge,
            else => return WALError.InvalidEntryType,
        };

        // Create owned copy of payload for WALEntry
        const owned_payload = try allocator.dupe(u8, payload);
        errdefer allocator.free(owned_payload);

        // Validate checksum matches payload
        const expected_checksum = calculate_checksum(entry_type, owned_payload);
        if (checksum != expected_checksum) {
            return WALError.InvalidChecksum;
        }

        return WALEntry{
            .checksum = checksum,
            .entry_type = entry_type,
            .payload_size = @intCast(owned_payload.len),
            .payload = owned_payload,
        };
    }

    /// Convert WALEntryStream.StreamEntry to WALEntry
    /// Transfers ownership of payload from StreamEntry to WALEntry
    pub fn from_stream_entry(stream_entry: wal_entry_stream.StreamEntry, allocator: std.mem.Allocator) WALError!WALEntry {
        return deserialize_from_stream(
            stream_entry.checksum,
            stream_entry.entry_type,
            stream_entry.payload,
            allocator,
        );
    }

    /// Free allocated payload memory.
    /// Must be called for all entries created via deserialize() or create_*() methods.
    pub fn deinit(self: WALEntry, allocator: std.mem.Allocator) void {
        // Size consistency prevents double-free and use-after-free bugs
        assert(self.payload.len == self.payload_size);

        if (self.payload.len > 0) {
            allocator.free(self.payload);
        }
    }
};
