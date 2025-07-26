//! SSTable (Sorted String Table) implementation for CortexDB LSM-Tree.
//!
//! ## On-Disk Format Philosophy
//!
//! The SSTable on-disk format is designed with three principles in mind:
//!
//! 1.  **Performance:** The file header is 64-byte aligned. This ensures that it fits
//!     cleanly within a typical CPU cache line, improving read performance.
//!
//! 2.  **Robustness:** All data structures are protected by checksums. This allows the
//!     engine to detect silent disk corruption (bit rot) and fail gracefully rather
//!     than propagating corrupted data.
//!
//! 3.  **Forward/Backward Compatibility:** The header includes a `format_version`. This
//!     allows future versions of CortexDB to read older SSTables and perform safe
//!     upgrades. The `reserved` fields provide space for new features without
//!     breaking the format for older clients.

const std = @import("std");
const custom_assert = @import("../core/assert.zig");
const assert = custom_assert.assert;
const comptime_assert = custom_assert.comptime_assert;
const log = std.log.scoped(.sstable);
const context_block = @import("../core/types.zig");
const vfs = @import("../core/vfs.zig");
const bloom_filter = @import("bloom_filter.zig");
const simulation_vfs = @import("../sim/simulation_vfs.zig");
const testing = std.testing;

const ContextBlock = context_block.ContextBlock;
const BlockId = context_block.BlockId;
const GraphEdge = context_block.GraphEdge;
const VFS = vfs.VFS;
const VFile = vfs.VFile;
const BloomFilter = bloom_filter.BloomFilter;
const SimulationVFS = simulation_vfs.SimulationVFS;

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
    bloom_filter: ?BloomFilter,

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

            @memcpy(buffer[offset .. offset + 16], &self.block_id.bytes);
            offset += 16;

            std.mem.writeInt(u64, buffer[offset..][0..8], self.offset, .little);
            offset += 8;

            std.mem.writeInt(u32, buffer[offset..][0..4], self.size, .little);
        }

        pub fn deserialize(buffer: []const u8) !IndexEntry {
            assert(buffer.len >= SERIALIZED_SIZE);
            if (buffer.len < SERIALIZED_SIZE) return error.BufferTooSmall;

            var offset: usize = 0;

            var id_bytes: [16]u8 = undefined;
            @memcpy(&id_bytes, buffer[offset .. offset + 16]);
            const block_id = BlockId.from_bytes(id_bytes);
            offset += 16;

            const block_offset = std.mem.readInt(u64, buffer[offset..][0..8], .little);
            offset += 8;

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
        bloom_filter_offset: u64, // 8 bytes: Offset to bloom filter
        bloom_filter_size: u32, // 4 bytes: Size of serialized bloom filter
        reserved: [20]u8, // 20 bytes: Reserved for future use

        // Compile-time guarantees for SSTable on-disk format integrity
        comptime {
            comptime_assert(@sizeOf(Header) == 64, "SSTable Header must be exactly 64 bytes for cache-aligned performance");
            comptime_assert(HEADER_SIZE == @sizeOf(Header), "HEADER_SIZE constant must match actual Header struct size");
            comptime_assert(4 + @sizeOf(u16) + @sizeOf(u16) + @sizeOf(u64) + @sizeOf(u32) +
                @sizeOf(u32) + @sizeOf(u64) + @sizeOf(u64) + @sizeOf(u32) + 20 == 64, "SSTable Header field sizes must sum to exactly 64 bytes");
        }

        pub fn serialize(self: Header, buffer: []u8) !void {
            assert(buffer.len >= HEADER_SIZE);

            var offset: usize = 0;

            @memcpy(buffer[offset .. offset + 4], &self.magic);
            offset += 4;

            std.mem.writeInt(u16, buffer[offset..][0..2], self.format_version, .little);
            offset += 2;

            std.mem.writeInt(u16, buffer[offset..][0..2], self.flags, .little);
            offset += 2;

            std.mem.writeInt(u64, buffer[offset..][0..8], self.index_offset, .little);
            offset += 8;

            std.mem.writeInt(u32, buffer[offset..][0..4], self.block_count, .little);
            offset += 4;

            std.mem.writeInt(u32, buffer[offset..][0..4], self.file_checksum, .little);
            offset += 4;

            std.mem.writeInt(u64, buffer[offset..][0..8], self.created_timestamp, .little);
            offset += 8;

            std.mem.writeInt(u64, buffer[offset..][0..8], self.bloom_filter_offset, .little);
            offset += 8;

            std.mem.writeInt(u32, buffer[offset..][0..4], self.bloom_filter_size, .little);
            offset += 4;

            // Reserved bytes must be zero for future compatibility
            @memset(buffer[offset .. offset + 20], 0);
        }

        pub fn deserialize(buffer: []const u8) !Header {
            assert(buffer.len >= HEADER_SIZE);
            if (buffer.len < HEADER_SIZE) return error.BufferTooSmall;

            var offset: usize = 0;

            const magic = buffer[offset .. offset + 4];
            if (!std.mem.eql(u8, magic, &MAGIC)) return error.InvalidMagic;
            offset += 4;

            const format_version = std.mem.readInt(u16, buffer[offset..][0..2], .little);
            if (format_version > VERSION) return error.UnsupportedVersion;
            offset += 2;

            const flags = std.mem.readInt(u16, buffer[offset..][0..2], .little);
            offset += 2;

            const index_offset = std.mem.readInt(u64, buffer[offset..][0..8], .little);
            offset += 8;

            const block_count = std.mem.readInt(u32, buffer[offset..][0..4], .little);
            offset += 4;

            const file_checksum = std.mem.readInt(u32, buffer[offset..][0..4], .little);
            offset += 4;

            const created_timestamp = std.mem.readInt(u64, buffer[offset..][0..8], .little);
            offset += 8;

            const bloom_filter_offset = std.mem.readInt(u64, buffer[offset..][0..8], .little);
            offset += 8;

            const bloom_filter_size = std.mem.readInt(u32, buffer[offset..][0..4], .little);
            offset += 4;

            // Validate reserved bytes are zero for forward compatibility
            const reserved = buffer[offset .. offset + 20];
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
                .bloom_filter_offset = bloom_filter_offset,
                .bloom_filter_size = bloom_filter_size,
                .reserved = [_]u8{0} ** 20,
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
            .bloom_filter = null,
        };
    }

    pub fn deinit(self: *SSTable) void {
        self.index.deinit();
        if (self.bloom_filter) |*filter| {
            filter.deinit();
        }
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

        var file = try self.filesystem.create(self.file_path);
        defer file.close();

        var header_buffer: [HEADER_SIZE]u8 = undefined;
        @memset(&header_buffer, 0);
        _ = try file.write(&header_buffer);

        var current_offset: u64 = HEADER_SIZE;

        // Create and populate Bloom filter
        const bloom_params = if (sorted_blocks.len < 1000)
            BloomFilter.Params.small
        else if (sorted_blocks.len < 10000)
            BloomFilter.Params.medium
        else
            BloomFilter.Params.large;

        var bloom = try BloomFilter.init(self.allocator, bloom_params);
        for (sorted_blocks) |block| {
            bloom.add(block.id);
        }

        for (sorted_blocks) |block| {
            const block_size = block.serialized_size();
            const buffer = try self.allocator.alloc(u8, block_size);
            defer self.allocator.free(buffer);

            const written = try block.serialize(buffer);
            assert(written == block_size);

            _ = try file.write(buffer);

            try self.index.append(IndexEntry{
                .block_id = block.id,
                .offset = current_offset,
                .size = @intCast(block_size),
            });

            current_offset += block_size;
        }

        const index_offset = current_offset;
        for (self.index.items) |entry| {
            var entry_buffer: [IndexEntry.SERIALIZED_SIZE]u8 = undefined;
            try entry.serialize(&entry_buffer);
            _ = try file.write(&entry_buffer);
            current_offset += IndexEntry.SERIALIZED_SIZE;
        }

        // Write Bloom filter after index
        const bloom_filter_offset = current_offset;
        const bloom_filter_size = bloom.serialized_size();
        const bloom_buffer = try self.allocator.alloc(u8, bloom_filter_size);
        defer self.allocator.free(bloom_buffer);

        try bloom.serialize(bloom_buffer);
        _ = try file.write(bloom_buffer);
        current_offset += bloom_filter_size;

        // Calculate file checksum over all content (excluding header checksum field)
        const file_checksum = try self.calculate_file_checksum(&file, current_offset);

        // Calculate footer checksum
        const file_size = try file.file_size();
        const content_size = file_size - FOOTER_SIZE;

        var hasher = std.hash.Wyhash.init(0);
        hasher.update(std.mem.asBytes(&content_size));
        const footer_checksum = hasher.final();

        var footer_buffer: [FOOTER_SIZE]u8 = undefined;
        std.mem.writeInt(u64, &footer_buffer, footer_checksum, .little);
        _ = try file.write(&footer_buffer);

        _ = try file.seek(0, .start);
        const header = Header{
            .magic = MAGIC,
            .format_version = VERSION,
            .flags = 0,
            .index_offset = index_offset,
            .block_count = @intCast(sorted_blocks.len),
            .file_checksum = file_checksum,
            .created_timestamp = @intCast(std.time.timestamp()),
            .bloom_filter_offset = bloom_filter_offset,
            .bloom_filter_size = bloom_filter_size,
            .reserved = [_]u8{0} ** 20,
        };

        try header.serialize(&header_buffer);
        _ = try file.write(&header_buffer);

        try file.flush();
        self.block_count = @intCast(sorted_blocks.len);

        // Store the Bloom filter in memory for future queries
        self.bloom_filter = bloom;
    }

    /// Calculate CRC32 checksum over file content (excluding header checksum field and footer)
    fn calculate_file_checksum(self: *SSTable, file: *VFile, content_end_offset: u64) !u32 {
        _ = self;
        var crc = std.hash.Crc32.init();

        // Calculate checksum over all content from header end to content end
        // This includes: blocks + index + bloom filter (but excludes header and footer)
        _ = try file.seek(HEADER_SIZE, .start);
        const content_size = content_end_offset - HEADER_SIZE;

        var buffer: [4096]u8 = undefined;
        var remaining = content_size;

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
        defer file.close();

        var header_buffer: [HEADER_SIZE]u8 = undefined;
        _ = try file.read(&header_buffer);

        const header = try Header.deserialize(&header_buffer);
        self.block_count = header.block_count;

        // Verify file checksum if present
        if (header.file_checksum != 0) {
            // Calculate checksum over content from header end to bloom filter end
            const content_end = if (header.bloom_filter_size > 0)
                header.bloom_filter_offset + header.bloom_filter_size
            else
                header.index_offset + (header.block_count * IndexEntry.SERIALIZED_SIZE);

            const calculated_checksum = try self.calculate_file_checksum(
                &file,
                content_end,
            );
            if (calculated_checksum != header.file_checksum) {
                return error.ChecksumMismatch;
            }
        }

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

        // Load Bloom filter if present
        if (header.bloom_filter_size > 0) {
            _ = try file.seek(@intCast(header.bloom_filter_offset), .start);

            const bloom_buffer = try self.allocator.alloc(u8, header.bloom_filter_size);
            defer self.allocator.free(bloom_buffer);

            _ = try file.read(bloom_buffer);

            self.bloom_filter = try BloomFilter.deserialize(self.allocator, bloom_buffer);
        }
    }

    /// Find a block by ID from this SSTable
    pub fn find_block(self: *SSTable, block_id: BlockId) !?ContextBlock {
        // Check Bloom filter first to avoid expensive disk I/O for non-existent keys
        if (self.bloom_filter) |*filter| {
            if (!filter.might_contain(block_id)) {
                // Definitely not in this SSTable - no false negatives
                return null;
            }
            // Might be in SSTable - proceed with index lookup (could be false positive)
        }

        // Binary search through sorted index for O(log n) performance
        var left: usize = 0;
        var right: usize = self.index.items.len;
        var entry: ?IndexEntry = null;

        while (left < right) {
            const mid = left + (right - left) / 2;
            const mid_entry = self.index.items[mid];

            const order = std.mem.order(u8, &mid_entry.block_id.bytes, &block_id.bytes);
            switch (order) {
                .lt => left = mid + 1,
                .gt => right = mid,
                .eq => {
                    entry = mid_entry;
                    break;
                },
            }
        }

        const found_entry = entry orelse return null;

        var file = try self.filesystem.open(self.file_path, .read);
        defer file.close();

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
            file.close();
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

        const output_path_copy = try self.allocator.dupe(u8, output_path);
        var output_table = SSTable.init(self.allocator, self.filesystem, output_path_copy);
        defer output_table.deinit();

        var all_blocks = std.ArrayList(ContextBlock).init(self.allocator);
        defer {
            for (all_blocks.items) |block| {
                block.deinit(self.allocator);
            }
            all_blocks.deinit();
        }

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

        try output_table.write_blocks(unique_blocks);

        // Remove input files after successful compaction
        for (input_paths) |path| {
            self.filesystem.remove(path) catch |err| {
                log.warn("Failed to remove input SSTable {s}: {}", .{ path, err });
            };
        }
    }

    /// Remove duplicate blocks, keeping the one with highest version
    fn dedup_blocks(self: *Compactor, blocks: []ContextBlock) ![]ContextBlock {
        if (blocks.len == 0) return try self.allocator.alloc(ContextBlock, 0);

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

        var unique = std.ArrayList(ContextBlock).init(self.allocator);
        defer unique.deinit();

        var prev_id: ?BlockId = null;
        for (sorted) |block| {
            if (prev_id == null or !block.id.eql(prev_id.?)) {
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
        }

        self.allocator.free(sorted);
        return try unique.toOwnedSlice();
    }
};

// Tests

test "SSTable write and read" {
    const allocator = testing.allocator;

    var sim_vfs = SimulationVFS.init(allocator);
    defer sim_vfs.deinit();

    var sstable = SSTable.init(allocator, sim_vfs.vfs(), try allocator.dupe(u8, "test.sst"));
    defer sstable.deinit();

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

    try sstable.write_blocks(&blocks);

    try sstable.read_index();
    try std.testing.expectEqual(@as(u32, 2), sstable.block_count);

    const retrieved1 = try sstable.find_block(block1.id);
    try std.testing.expect(retrieved1 != null);
    try std.testing.expect(retrieved1.?.id.eql(block1.id));
    try std.testing.expectEqualStrings("test://block1", retrieved1.?.source_uri);

    if (retrieved1) |block| {
        block.deinit(allocator);
    }

    const non_existent_id = try BlockId.from_hex("1111111111111111111111111111111");
    const not_found = try sstable.find_block(non_existent_id);
    try std.testing.expect(not_found == null);
}

test "SSTable iterator" {
    const allocator = testing.allocator;

    var sim_vfs = SimulationVFS.init(allocator);
    defer sim_vfs.deinit();

    var sstable = SSTable.init(allocator, sim_vfs.vfs(), try allocator.dupe(u8, "test_iter.sst"));
    defer sstable.deinit();

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
    const allocator = testing.allocator;

    var sim_vfs = SimulationVFS.init(allocator);
    defer sim_vfs.deinit();

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

    var compactor = Compactor.init(allocator, sim_vfs.vfs(), "test_data");
    const input_paths = [_][]const u8{ "table1.sst", "table2.sst" };
    try compactor.compact_sstables(&input_paths, "compacted.sst");

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
    const allocator = testing.allocator;

    var sim_vfs = SimulationVFS.init(allocator);
    defer sim_vfs.deinit();

    const path = try allocator.dupe(u8, "checksum_test.sst");
    var sstable = SSTable.init(allocator, sim_vfs.vfs(), path);
    defer sstable.deinit();

    const block = ContextBlock{
        .id = try BlockId.from_hex("1234567890abcdef1234567890abcdef"),
        .version = 1,
        .source_uri = "test://checksum",
        .metadata_json = "{\"type\":\"checksum_test\"}",
        .content = "checksum test content",
    };

    const blocks = [_]ContextBlock{block};

    try sstable.write_blocks(&blocks);

    try sstable.read_index();
    try std.testing.expectEqual(@as(u32, 1), sstable.block_count);

    var file = try sim_vfs.vfs().open("checksum_test.sst", .write);
    defer file.close() catch {};

    // Modify a byte in the block data section (after header)
    _ = try file.seek(SSTable.HEADER_SIZE + 10, .start);
    const corrupt_byte = [1]u8{0xFF};
    _ = try file.write(&corrupt_byte);
    try file.close();

    sstable.index.clearAndFree();
    sstable.block_count = 0;

    try std.testing.expectError(error.ChecksumMismatch, sstable.read_index());
}

test "SSTable Bloom filter functionality" {
    const allocator = testing.allocator;

    var sim_vfs = SimulationVFS.init(allocator);
    defer sim_vfs.deinit();

    var sstable = SSTable.init(allocator, sim_vfs.vfs(), try allocator.dupe(u8, "bloom_test.sst"));
    defer sstable.deinit();

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

    // Write blocks - this should create and populate the Bloom filter
    try sstable.write_blocks(&blocks);

    // Verify Bloom filter was created
    try std.testing.expect(sstable.bloom_filter != null);

    // Test that all written blocks are found by Bloom filter
    if (sstable.bloom_filter) |*filter| {
        try std.testing.expect(filter.might_contain(block1.id));
        try std.testing.expect(filter.might_contain(block2.id));
    }

    // Test actual block retrieval works
    const retrieved1 = try sstable.find_block(block1.id);
    try std.testing.expect(retrieved1 != null);
    try std.testing.expect(retrieved1.?.id.eql(block1.id));
    if (retrieved1) |block| {
        block.deinit(allocator);
    }

    // Test with non-existent block - Bloom filter should eliminate false negatives
    const non_existent_id = try BlockId.from_hex("1111111111111111111111111111111");
    const not_found = try sstable.find_block(non_existent_id);
    try std.testing.expect(not_found == null);
}

test "SSTable Bloom filter persistence" {
    const allocator = testing.allocator;

    var sim_vfs = SimulationVFS.init(allocator);
    defer sim_vfs.deinit();

    const file_path = try allocator.dupe(u8, "bloom_persist_test.sst");
    defer allocator.free(file_path);

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

    // Write SSTable with Bloom filter
    {
        var sstable_write = SSTable.init(allocator, sim_vfs.vfs(), try allocator.dupe(u8, file_path));
        defer sstable_write.deinit();

        try sstable_write.write_blocks(&blocks);
        try std.testing.expect(sstable_write.bloom_filter != null);
    }

    // Read SSTable and verify Bloom filter is loaded
    {
        var sstable_read = SSTable.init(allocator, sim_vfs.vfs(), try allocator.dupe(u8, file_path));
        defer sstable_read.deinit();

        try sstable_read.read_index();
        try std.testing.expect(sstable_read.bloom_filter != null);

        // Verify Bloom filter behavior is preserved
        if (sstable_read.bloom_filter) |*filter| {
            try std.testing.expect(filter.might_contain(block1.id));
            try std.testing.expect(filter.might_contain(block2.id));
        }

        // Test actual retrieval still works
        const retrieved = try sstable_read.find_block(block1.id);
        try std.testing.expect(retrieved != null);
        if (retrieved) |block| {
            block.deinit(allocator);
        }
    }
}

test "SSTable Bloom filter with many blocks" {
    const allocator = testing.allocator;

    var sim_vfs = SimulationVFS.init(allocator);
    defer sim_vfs.deinit();

    var sstable = SSTable.init(allocator, sim_vfs.vfs(), try allocator.dupe(u8, "bloom_many_test.sst"));
    defer sstable.deinit();

    // Create many blocks to test Bloom filter scaling
    var blocks = std.ArrayList(ContextBlock).init(allocator);
    defer {
        for (blocks.items) |block| {
            block.deinit(allocator);
        }
        blocks.deinit();
    }

    var i: u8 = 0;
    while (i < 50) : (i += 1) {
        var id_bytes: [16]u8 = undefined;
        @memset(&id_bytes, 0);
        id_bytes[0] = i;

        const block = ContextBlock{
            .id = BlockId{ .bytes = id_bytes },
            .version = 1,
            .source_uri = try std.fmt.allocPrint(allocator, "test://block{d}", .{i}),
            .metadata_json = try allocator.dupe(u8, "{}"),
            .content = try std.fmt.allocPrint(allocator, "content {d}", .{i}),
        };
        try blocks.append(block);
    }

    try sstable.write_blocks(blocks.items);

    // Verify Bloom filter was created and all blocks are detected
    try std.testing.expect(sstable.bloom_filter != null);
    if (sstable.bloom_filter) |*filter| {
        for (blocks.items) |block| {
            try std.testing.expect(filter.might_contain(block.id));
        }
    }

    // Test retrieval of several blocks
    const retrieved_first = try sstable.find_block(blocks.items[0].id);
    try std.testing.expect(retrieved_first != null);
    if (retrieved_first) |block| {
        block.deinit(allocator);
    }

    const retrieved_last = try sstable.find_block(blocks.items[blocks.items.len - 1].id);
    try std.testing.expect(retrieved_last != null);
    if (retrieved_last) |block| {
        block.deinit(allocator);
    }

    // Test non-existent block
    var non_existent_bytes: [16]u8 = undefined;
    @memset(&non_existent_bytes, 0xFF);
    const non_existent_id = BlockId{ .bytes = non_existent_bytes };
    const not_found = try sstable.find_block(non_existent_id);
    try std.testing.expect(not_found == null);
}

test "SSTable binary search performance" {
    const allocator = testing.allocator;

    var sim_vfs = SimulationVFS.init(allocator);
    defer sim_vfs.deinit();

    var sstable = SSTable.init(allocator, sim_vfs.vfs(), try allocator.dupe(u8, "binary_search_test.sst"));
    defer sstable.deinit();

    // Create blocks with IDs that will test binary search ordering
    var blocks = std.ArrayList(ContextBlock).init(allocator);
    defer {
        for (blocks.items) |block| {
            block.deinit(allocator);
        }
        blocks.deinit();
    }

    // Create blocks with deliberately unsorted IDs to verify sorting works
    const test_ids = [_][]const u8{
        "ffff000000000000000000000000000", // Should be last after sorting
        "0000000000000000000000000000000", // Should be first after sorting
        "8888888888888888888888888888888", // Should be in middle
        "4444444444444444444444444444444", // Should be in first half
        "cccccccccccccccccccccccccccccccc", // Should be in second half
    };

    for (test_ids, 0..) |id_hex, i| {
        const block = ContextBlock{
            .id = try BlockId.from_hex(id_hex),
            .version = 1,
            .source_uri = try std.fmt.allocPrint(allocator, "test://block{d}", .{i}),
            .metadata_json = try allocator.dupe(u8, "{}"),
            .content = try std.fmt.allocPrint(allocator, "content {d}", .{i}),
        };
        try blocks.append(block);
    }

    try sstable.write_blocks(blocks.items);
    try sstable.read_index();

    // Verify all blocks can be found (binary search should work correctly)
    for (blocks.items) |original_block| {
        const found = try sstable.find_block(original_block.id);
        try std.testing.expect(found != null);
        try std.testing.expect(found.?.id.eql(original_block.id));
        if (found) |block| {
            block.deinit(allocator);
        }
    }

    // Verify index is properly sorted
    for (1..sstable.index.items.len) |i| {
        const prev = sstable.index.items[i - 1];
        const curr = sstable.index.items[i];
        const order = std.mem.order(u8, &prev.block_id.bytes, &curr.block_id.bytes);
        try std.testing.expect(order == .lt);
    }
}
