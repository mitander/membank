//! SSTable (Sorted String Table) implementation for CortexDB LSM-Tree.
//!
//! SSTables are immutable, sorted files that store Context Blocks and Graph Edges.
//! They support efficient range queries and are the foundation of the LSM-Tree storage engine.

const std = @import("std");
const assert = std.debug.assert;
const context_block = @import("context_block");
const vfs = @import("vfs");

const ContextBlock = context_block.ContextBlock;
const BlockId = context_block.BlockId;
const GraphEdge = context_block.GraphEdge;
const VFS = vfs.VFS;

/// SSTable file format:
/// | Magic (4) | Version (4) | Index Offset (8) | Block Count (4) | Reserved (12) |
/// | Block Data ... |
/// | Index Entries ... |
/// | Footer Checksum (8) |
pub const SSTable = struct {
    allocator: std.mem.Allocator,
    filesystem: VFS,
    file_path: []const u8,
    block_count: u32,
    index: std.ArrayList(IndexEntry),

    const MAGIC = [4]u8{ 'S', 'S', 'T', 'B' }; // "SSTB" for SSTable Blocks
    const VERSION = 1;
    const HEADER_SIZE = 64; // Cache-aligned header size for performance
    const FOOTER_SIZE = 8; // Checksum

    /// Index entry pointing to a block within the SSTable
    const IndexEntry = struct {
        block_id: BlockId,
        offset: u64,
        size: u32,

        const SERIALIZED_SIZE = 16 + 8 + 4; // BlockId + offset + size

        pub fn serialize(self: IndexEntry, buffer: []u8) !void {
            assert(buffer.len >= SERIALIZED_SIZE);

            var offset: usize = 0;

            // Write block ID
            @memcpy(buffer[offset .. offset + 16], &self.block_id.bytes);
            offset += 16;

            // Write offset
            std.mem.writeInt(u64, buffer[offset..][0..8], self.offset, .little);
            offset += 8;

            // Write size
            std.mem.writeInt(u32, buffer[offset..][0..4], self.size, .little);
        }

        pub fn deserialize(buffer: []const u8) !IndexEntry {
            assert(buffer.len >= SERIALIZED_SIZE);
            if (buffer.len < SERIALIZED_SIZE) return error.BufferTooSmall;

            var offset: usize = 0;

            // Read block ID
            var id_bytes: [16]u8 = undefined;
            @memcpy(&id_bytes, buffer[offset .. offset + 16]);
            const block_id = BlockId.from_bytes(id_bytes);
            offset += 16;

            // Read offset
            const block_offset = std.mem.readInt(u64, buffer[offset..][0..8], .little);
            offset += 8;

            // Read size
            const size = std.mem.readInt(u32, buffer[offset..][0..4], .little);

            return IndexEntry{
                .block_id = block_id,
                .offset = block_offset,
                .size = size,
            };
        }
    };

    /// File header structure (64-byte aligned)
    const Header = struct {
        magic: [4]u8, // 4 bytes: "SSTB"
        format_version: u16, // 2 bytes: Major.minor versioning
        flags: u16, // 2 bytes: Feature flags
        index_offset: u64, // 8 bytes: Offset to index section
        block_count: u32, // 4 bytes: Number of blocks
        file_checksum: u32, // 4 bytes: CRC32 of entire file
        created_timestamp: u64, // 8 bytes: Unix timestamp
        reserved: [32]u8, // 32 bytes: Reserved for future use

        pub fn serialize(self: Header, buffer: []u8) !void {
            assert(buffer.len >= HEADER_SIZE);

            var offset: usize = 0;

            // Write magic
            @memcpy(buffer[offset .. offset + 4], &self.magic);
            offset += 4;

            // Write format version
            std.mem.writeInt(u16, buffer[offset..][0..2], self.format_version, .little);
            offset += 2;

            // Write flags
            std.mem.writeInt(u16, buffer[offset..][0..2], self.flags, .little);
            offset += 2;

            // Write index offset
            std.mem.writeInt(u64, buffer[offset..][0..8], self.index_offset, .little);
            offset += 8;

            // Write block count
            std.mem.writeInt(u32, buffer[offset..][0..4], self.block_count, .little);
            offset += 4;

            // Write file checksum
            std.mem.writeInt(u32, buffer[offset..][0..4], self.file_checksum, .little);
            offset += 4;

            // Write created timestamp
            std.mem.writeInt(u64, buffer[offset..][0..8], self.created_timestamp, .little);
            offset += 8;

            // Write reserved bytes (must be zero)
            @memset(buffer[offset .. offset + 32], 0);
        }

        pub fn deserialize(buffer: []const u8) !Header {
            assert(buffer.len >= HEADER_SIZE);
            if (buffer.len < HEADER_SIZE) return error.BufferTooSmall;

            var offset: usize = 0;

            // Read and validate magic
            const magic = buffer[offset .. offset + 4];
            if (!std.mem.eql(u8, magic, &MAGIC)) return error.InvalidMagic;
            offset += 4;

            // Read format version
            const format_version = std.mem.readInt(u16, buffer[offset..][0..2], .little);
            if (format_version > VERSION) return error.UnsupportedVersion;
            offset += 2;

            // Read flags
            const flags = std.mem.readInt(u16, buffer[offset..][0..2], .little);
            offset += 2;

            // Read index offset
            const index_offset = std.mem.readInt(u64, buffer[offset..][0..8], .little);
            offset += 8;

            // Read block count
            const block_count = std.mem.readInt(u32, buffer[offset..][0..4], .little);
            offset += 4;

            // Read file checksum
            const file_checksum = std.mem.readInt(u32, buffer[offset..][0..4], .little);
            offset += 4;

            // Read created timestamp
            const created_timestamp = std.mem.readInt(u64, buffer[offset..][0..8], .little);
            offset += 8;

            // Validate reserved bytes are zero
            const reserved = buffer[offset .. offset + 32];
            for (reserved) |byte| {
                if (byte != 0) return error.InvalidReservedBytes;
            }

            return Header{
                .magic = MAGIC,
                .format_version = format_version,
                .flags = flags,
                .index_offset = index_offset,
                .block_count = block_count,
                .file_checksum = file_checksum,
                .created_timestamp = created_timestamp,
                .reserved = [_]u8{0} ** 32,
            };
        }
    };

    pub fn init(allocator: std.mem.Allocator, filesystem: VFS, file_path: []const u8) SSTable {
        return SSTable{
            .allocator = allocator,
            .filesystem = filesystem,
            .file_path = file_path,
            .block_count = 0,
            .index = std.ArrayList(IndexEntry).init(allocator),
        };
    }

    pub fn deinit(self: *SSTable) void {
        self.index.deinit();
        self.allocator.free(self.file_path);
    }

    /// Write blocks to SSTable file in sorted order
    pub fn write_blocks(self: *SSTable, blocks: []const ContextBlock) !void {
        assert(blocks.len > 0);

        // Sort blocks by ID for efficient range queries
        const sorted_blocks = try self.allocator.dupe(ContextBlock, blocks);
        defer self.allocator.free(sorted_blocks);

        std.mem.sort(ContextBlock, sorted_blocks, {}, struct {
            fn less_than(context: void, a: ContextBlock, b: ContextBlock) bool {
                _ = context;
                return std.mem.order(u8, &a.id.bytes, &b.id.bytes) == .lt;
            }
        }.less_than);

        // Create the SSTable file
        var file = try self.filesystem.create(self.file_path);
        defer file.close() catch {};

        // Reserve space for header
        var header_buffer: [HEADER_SIZE]u8 = undefined;
        @memset(&header_buffer, 0);
        _ = try file.write(&header_buffer);

        // Write blocks and build index
        var current_offset: u64 = HEADER_SIZE;

        for (sorted_blocks) |block| {
            const block_size = block.serialized_size();
            const buffer = try self.allocator.alloc(u8, block_size);
            defer self.allocator.free(buffer);

            const written = try block.serialize(buffer);
            assert(written == block_size);

            // Write block data
            _ = try file.write(buffer);

            // Add to index
            try self.index.append(IndexEntry{
                .block_id = block.id,
                .offset = current_offset,
                .size = @intCast(block_size),
            });

            current_offset += block_size;
        }

        // Write index
        const index_offset = current_offset;
        for (self.index.items) |entry| {
            var entry_buffer: [IndexEntry.SERIALIZED_SIZE]u8 = undefined;
            try entry.serialize(&entry_buffer);
            _ = try file.write(&entry_buffer);
        }

        // Calculate file checksum over all content (excluding header checksum field)
        const file_checksum = try self.calculate_file_checksum(&file, index_offset);

        // Calculate footer checksum
        const file_size = try file.file_size();
        const content_size = file_size - FOOTER_SIZE;

        var hasher = std.hash.Wyhash.init(0);
        hasher.update(std.mem.asBytes(&content_size));
        const footer_checksum = hasher.final();

        // Write footer
        var footer_buffer: [FOOTER_SIZE]u8 = undefined;
        std.mem.writeInt(u64, &footer_buffer, footer_checksum, .little);
        _ = try file.write(&footer_buffer);

        // Go back and write header with calculated checksum
        _ = try file.seek(0, .start);
        const header = Header{
            .magic = MAGIC,
            .format_version = VERSION,
            .flags = 0,
            .index_offset = index_offset,
            .block_count = @intCast(sorted_blocks.len),
            .file_checksum = file_checksum,
            .created_timestamp = @intCast(std.time.timestamp()),
            .reserved = [_]u8{0} ** 32,
        };

        try header.serialize(&header_buffer);
        _ = try file.write(&header_buffer);

        try file.flush();
        self.block_count = @intCast(sorted_blocks.len);
    }

    /// Calculate CRC32 checksum over file content (excluding header checksum field)
    fn calculate_file_checksum(self: *SSTable, file: *vfs.VFile, index_offset: u64) !u32 {
        _ = self;

        // Calculate checksum over blocks and index sections
        var crc = std.hash.Crc32.init();

        // Read block data section (from end of header to start of index)
        _ = try file.seek(HEADER_SIZE, .start);
        const block_data_size = index_offset - HEADER_SIZE;

        var buffer: [4096]u8 = undefined;
        var remaining = block_data_size;

        while (remaining > 0) {
            const chunk_size = @min(remaining, buffer.len);
            const bytes_read = try file.read(buffer[0..chunk_size]);
            if (bytes_read == 0) break;

            crc.update(buffer[0..bytes_read]);
            remaining -= bytes_read;
        }

        // Read index section
        _ = try file.seek(@intCast(index_offset), .start);
        const file_size = try file.file_size();
        const index_size = file_size - index_offset - FOOTER_SIZE;

        remaining = index_size;
        while (remaining > 0) {
            const chunk_size = @min(remaining, buffer.len);
            const bytes_read = try file.read(buffer[0..chunk_size]);
            if (bytes_read == 0) break;

            crc.update(buffer[0..bytes_read]);
            remaining -= bytes_read;
        }

        return crc.final();
    }

    /// Read SSTable and load index
    pub fn read_index(self: *SSTable) !void {
        var file = try self.filesystem.open(self.file_path, .read);
        defer file.close() catch {};

        // Read and validate header
        var header_buffer: [HEADER_SIZE]u8 = undefined;
        _ = try file.read(&header_buffer);

        const header = try Header.deserialize(&header_buffer);
        self.block_count = header.block_count;

        // Verify file checksum if present
        if (header.file_checksum != 0) {
            const calculated_checksum = try self.calculate_file_checksum(
                &file,
                header.index_offset,
            );
            if (calculated_checksum != header.file_checksum) {
                return error.ChecksumMismatch;
            }
        }

        // Read index entries
        _ = try file.seek(@intCast(header.index_offset), .start);

        for (0..header.block_count) |_| {
            var entry_buffer: [IndexEntry.SERIALIZED_SIZE]u8 = undefined;
            _ = try file.read(&entry_buffer);

            const entry = try IndexEntry.deserialize(&entry_buffer);
            try self.index.append(entry);
        }

        // Sort index by block ID for binary search
        std.mem.sort(IndexEntry, self.index.items, {}, struct {
            fn less_than(context: void, a: IndexEntry, b: IndexEntry) bool {
                _ = context;
                return std.mem.order(u8, &a.block_id.bytes, &b.block_id.bytes) == .lt;
            }
        }.less_than);
    }

    /// Find a block by ID from this SSTable
    pub fn find_block(self: *SSTable, block_id: BlockId) !?ContextBlock {
        // Linear search through sorted index (simple implementation)
        var entry: ?IndexEntry = null;
        for (self.index.items) |idx_entry| {
            if (idx_entry.block_id.eql(block_id)) {
                entry = idx_entry;
                break;
            }
        }

        const found_entry = entry orelse return null;

        // Read block from file
        var file = try self.filesystem.open(self.file_path, .read);
        defer file.close() catch {};

        _ = try file.seek(@intCast(found_entry.offset), .start);

        const buffer = try self.allocator.alloc(u8, found_entry.size);
        defer self.allocator.free(buffer);

        _ = try file.read(buffer);

        return try ContextBlock.deserialize(buffer, self.allocator);
    }

    /// Get iterator for all blocks in sorted order
    pub fn iterator(self: *SSTable) SSTableIterator {
        return SSTableIterator.init(self);
    }
};

/// Iterator for reading all blocks from an SSTable in sorted order
pub const SSTableIterator = struct {
    sstable: *SSTable,
    current_index: usize,
    file: ?vfs.VFile,

    pub fn init(sstable: *SSTable) SSTableIterator {
        return SSTableIterator{
            .sstable = sstable,
            .current_index = 0,
            .file = null,
        };
    }

    pub fn deinit(self: *SSTableIterator) void {
        if (self.file) |*file| {
            file.close() catch {};
        }
    }

    pub fn next(self: *SSTableIterator) !?ContextBlock {
        if (self.current_index >= self.sstable.index.items.len) {
            return null;
        }

        // Open file if not already open
        if (self.file == null) {
            self.file = try self.sstable.filesystem.open(self.sstable.file_path, .read);
        }

        const entry = self.sstable.index.items[self.current_index];
        self.current_index += 1;

        // Read block from file
        _ = try self.file.?.seek(@intCast(entry.offset), .start);

        const buffer = try self.sstable.allocator.alloc(u8, entry.size);
        defer self.sstable.allocator.free(buffer);

        _ = try self.file.?.read(buffer);

        return try ContextBlock.deserialize(buffer, self.sstable.allocator);
    }
};

/// SSTable compaction manager
pub const Compactor = struct {
    allocator: std.mem.Allocator,
    filesystem: VFS,
    data_dir: []const u8,

    pub fn init(allocator: std.mem.Allocator, filesystem: VFS, data_dir: []const u8) Compactor {
        return Compactor{
            .allocator = allocator,
            .filesystem = filesystem,
            .data_dir = data_dir,
        };
    }

    /// Compact multiple SSTables into a single larger SSTable
    pub fn compact_sstables(
        self: *Compactor,
        input_paths: []const []const u8,
        output_path: []const u8,
    ) !void {
        assert(input_paths.len > 1);

        // Open all input SSTables
        var input_tables = try self.allocator.alloc(SSTable, input_paths.len);
        defer {
            for (input_tables) |*table| {
                table.deinit();
            }
            self.allocator.free(input_tables);
        }

        for (input_paths, 0..) |path, i| {
            const path_copy = try self.allocator.dupe(u8, path);
            input_tables[i] = SSTable.init(self.allocator, self.filesystem, path_copy);
            try input_tables[i].read_index();
        }

        // Create output SSTable
        const output_path_copy = try self.allocator.dupe(u8, output_path);
        var output_table = SSTable.init(self.allocator, self.filesystem, output_path_copy);
        defer output_table.deinit();

        // Merge sort all blocks from input tables
        var all_blocks = std.ArrayList(ContextBlock).init(self.allocator);
        defer {
            for (all_blocks.items) |block| {
                block.deinit(self.allocator);
            }
            all_blocks.deinit();
        }

        // Collect all blocks
        for (input_tables) |*table| {
            var iter = table.iterator();
            defer iter.deinit();

            while (try iter.next()) |block| {
                try all_blocks.append(block);
            }
        }

        // Remove duplicates (keep latest version)
        const unique_blocks = try self.dedup_blocks(all_blocks.items);
        defer {
            for (unique_blocks) |block| {
                block.deinit(self.allocator);
            }
            self.allocator.free(unique_blocks);
        }

        // Write compacted SSTable
        try output_table.write_blocks(unique_blocks);

        // Remove input files after successful compaction
        for (input_paths) |path| {
            self.filesystem.remove(path) catch |err| {
                std.log.warn("Failed to remove input SSTable {s}: {}", .{ path, err });
            };
        }
    }

    /// Remove duplicate blocks, keeping the one with highest version
    fn dedup_blocks(self: *Compactor, blocks: []ContextBlock) ![]ContextBlock {
        if (blocks.len == 0) return try self.allocator.alloc(ContextBlock, 0);

        // Sort by ID first
        const sorted = try self.allocator.dupe(ContextBlock, blocks);
        std.mem.sort(ContextBlock, sorted, {}, struct {
            fn less_than(context: void, a: ContextBlock, b: ContextBlock) bool {
                _ = context;
                const order = std.mem.order(u8, &a.id.bytes, &b.id.bytes);
                if (order == .eq) {
                    // Same ID - sort by version descending (highest first)
                    return a.version > b.version;
                }
                return order == .lt;
            }
        }.less_than);

        // Keep only the highest version of each block
        var unique = std.ArrayList(ContextBlock).init(self.allocator);
        defer unique.deinit();

        var prev_id: ?BlockId = null;
        for (sorted) |block| {
            if (prev_id == null or !block.id.eql(prev_id.?)) {
                // Clone the block for ownership
                const cloned = ContextBlock{
                    .id = block.id,
                    .version = block.version,
                    .source_uri = try self.allocator.dupe(u8, block.source_uri),
                    .metadata_json = try self.allocator.dupe(u8, block.metadata_json),
                    .content = try self.allocator.dupe(u8, block.content),
                };
                try unique.append(cloned);
                prev_id = block.id;
            }
            // Skip duplicates (lower versions)
        }

        self.allocator.free(sorted);
        return try unique.toOwnedSlice();
    }
};

// Tests

test "SSTable write and read" {
    const allocator = std.testing.allocator;
    const simulation_vfs = @import("simulation_vfs");

    var sim_vfs = simulation_vfs.SimulationVFS.init(allocator);
    defer sim_vfs.deinit();

    var sstable = SSTable.init(allocator, sim_vfs.vfs(), try allocator.dupe(u8, "test.sst"));
    defer sstable.deinit();

    // Create test blocks
    const block1 = ContextBlock{
        .id = try BlockId.from_hex("0123456789abcdeffedcba9876543210"),
        .version = 1,
        .source_uri = "test://block1",
        .metadata_json = "{\"type\":\"test\"}",
        .content = "test content 1",
    };

    const block2 = ContextBlock{
        .id = try BlockId.from_hex("fedcba9876543210123456789abcdef0"),
        .version = 1,
        .source_uri = "test://block2",
        .metadata_json = "{\"type\":\"test\"}",
        .content = "test content 2",
    };

    const blocks = [_]ContextBlock{ block1, block2 };

    // Write blocks to SSTable
    try sstable.write_blocks(&blocks);

    // Read index
    try sstable.read_index();
    try std.testing.expectEqual(@as(u32, 2), sstable.block_count);

    // Test find_block
    const retrieved1 = try sstable.find_block(block1.id);
    try std.testing.expect(retrieved1 != null);
    try std.testing.expect(retrieved1.?.id.eql(block1.id));
    try std.testing.expectEqualStrings("test://block1", retrieved1.?.source_uri);

    if (retrieved1) |block| {
        block.deinit(allocator);
    }

    // Test non-existent block
    const non_existent_id = try BlockId.from_hex("1111111111111111111111111111111");
    const not_found = try sstable.find_block(non_existent_id);
    try std.testing.expect(not_found == null);
}

test "SSTable iterator" {
    const allocator = std.testing.allocator;
    const simulation_vfs = @import("simulation_vfs");

    var sim_vfs = simulation_vfs.SimulationVFS.init(allocator);
    defer sim_vfs.deinit();

    var sstable = SSTable.init(allocator, sim_vfs.vfs(), try allocator.dupe(u8, "test_iter.sst"));
    defer sstable.deinit();

    // Create test blocks
    const blocks = [_]ContextBlock{
        ContextBlock{
            .id = try BlockId.from_hex("3333333333333333333333333333333"),
            .version = 1,
            .source_uri = "test://block3",
            .metadata_json = "{}",
            .content = "content 3",
        },
        ContextBlock{
            .id = try BlockId.from_hex("1111111111111111111111111111111"),
            .version = 1,
            .source_uri = "test://block1",
            .metadata_json = "{}",
            .content = "content 1",
        },
        ContextBlock{
            .id = try BlockId.from_hex("2222222222222222222222222222222"),
            .version = 1,
            .source_uri = "test://block2",
            .metadata_json = "{}",
            .content = "content 2",
        },
    };

    try sstable.write_blocks(&blocks);
    try sstable.read_index();

    // Test iterator returns blocks in sorted order
    var iter = sstable.iterator();
    defer iter.deinit();

    const first = try iter.next();
    try std.testing.expect(first != null);
    try std.testing.expectEqualStrings("content 1", first.?.content);
    first.?.deinit(allocator);

    const second = try iter.next();
    try std.testing.expect(second != null);
    try std.testing.expectEqualStrings("content 2", second.?.content);
    second.?.deinit(allocator);

    const third = try iter.next();
    try std.testing.expect(third != null);
    try std.testing.expectEqualStrings("content 3", third.?.content);
    third.?.deinit(allocator);

    const end = try iter.next();
    try std.testing.expect(end == null);
}

test "SSTable compaction" {
    const allocator = std.testing.allocator;
    const simulation_vfs = @import("simulation_vfs");

    var sim_vfs = simulation_vfs.SimulationVFS.init(allocator);
    defer sim_vfs.deinit();

    // Create first SSTable
    var sstable1 = SSTable.init(allocator, sim_vfs.vfs(), try allocator.dupe(u8, "table1.sst"));
    defer sstable1.deinit();

    const blocks1 = [_]ContextBlock{
        ContextBlock{
            .id = try BlockId.from_hex("1111111111111111111111111111111"),
            .version = 1,
            .source_uri = "test://v1",
            .metadata_json = "{}",
            .content = "version 1",
        },
        ContextBlock{
            .id = try BlockId.from_hex("3333333333333333333333333333333"),
            .version = 1,
            .source_uri = "test://block3",
            .metadata_json = "{}",
            .content = "content 3",
        },
    };

    try sstable1.write_blocks(&blocks1);

    // Create second SSTable with overlapping data
    var sstable2 = SSTable.init(allocator, sim_vfs.vfs(), try allocator.dupe(u8, "table2.sst"));
    defer sstable2.deinit();

    const blocks2 = [_]ContextBlock{
        ContextBlock{
            .id = try BlockId.from_hex("1111111111111111111111111111111"),
            .version = 2, // Higher version
            .source_uri = "test://v2",
            .metadata_json = "{}",
            .content = "version 2",
        },
        ContextBlock{
            .id = try BlockId.from_hex("2222222222222222222222222222222"),
            .version = 1,
            .source_uri = "test://block2",
            .metadata_json = "{}",
            .content = "content 2",
        },
    };

    try sstable2.write_blocks(&blocks2);

    // Compact tables
    var compactor = Compactor.init(allocator, sim_vfs.vfs(), "test_data");
    const input_paths = [_][]const u8{ "table1.sst", "table2.sst" };
    try compactor.compact_sstables(&input_paths, "compacted.sst");

    // Verify compacted result
    var compacted = SSTable.init(allocator, sim_vfs.vfs(), try allocator.dupe(u8, "compacted.sst"));
    defer compacted.deinit();

    try compacted.read_index();
    try std.testing.expectEqual(@as(u32, 3), compacted.block_count);

    // Should have version 2 of the duplicate block
    const test_id = try BlockId.from_hex("1111111111111111111111111111111");
    const retrieved = try compacted.find_block(test_id);
    try std.testing.expect(retrieved != null);
    try std.testing.expectEqual(@as(u64, 2), retrieved.?.version);
    try std.testing.expectEqualStrings("version 2", retrieved.?.content);

    if (retrieved) |block| {
        block.deinit(allocator);
    }
}

test "SSTable checksum validation" {
    const allocator = std.testing.allocator;
    const simulation_vfs = @import("simulation_vfs");

    var sim_vfs = simulation_vfs.SimulationVFS.init(allocator);
    defer sim_vfs.deinit();

    const path = try allocator.dupe(u8, "checksum_test.sst");
    var sstable = SSTable.init(allocator, sim_vfs.vfs(), path);
    defer sstable.deinit();

    // Create test block
    const block = ContextBlock{
        .id = try BlockId.from_hex("1234567890abcdef1234567890abcdef"),
        .version = 1,
        .source_uri = "test://checksum",
        .metadata_json = "{\"type\":\"checksum_test\"}",
        .content = "checksum test content",
    };

    const blocks = [_]ContextBlock{block};

    // Write blocks (this calculates and stores checksum)
    try sstable.write_blocks(&blocks);

    // Reading should succeed with valid checksum
    try sstable.read_index();
    try std.testing.expectEqual(@as(u32, 1), sstable.block_count);

    // Corrupt the file by modifying a byte in the block data
    var file = try sim_vfs.vfs().open("checksum_test.sst", .write);
    defer file.close() catch {};

    // Modify a byte in the block data section (after header)
    _ = try file.seek(SSTable.HEADER_SIZE + 10, .start);
    const corrupt_byte = [1]u8{0xFF};
    _ = try file.write(&corrupt_byte);
    try file.close();

    // Reset SSTable for fresh read
    sstable.index.clearAndFree();
    sstable.block_count = 0;

    // Reading should now fail with checksum mismatch
    try std.testing.expectError(error.ChecksumMismatch, sstable.read_index());
}
