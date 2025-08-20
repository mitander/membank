//! Secondary metadata index for fast query filtering and optimization.
//!
//! Provides O(log n) lookup performance for filtered queries on metadata fields
//! like unit_type, language, and semantic categories. Enables query plan optimization
//! by pre-filtering result sets before expensive traversal operations.
//!
//! Design rationale: Secondary indexes avoid full storage scans for common
//! filter patterns while maintaining manageable memory overhead. Arena allocation
//! enables efficient index rebuilds during compaction without fragmentation concerns.

const std = @import("std");

const context_block = @import("../core/types.zig");
const memory = @import("../core/memory.zig");
const assert_mod = @import("../core/assert.zig");

const assert = assert_mod.assert;

const ArenaCoordinator = memory.ArenaCoordinator;
const BlockId = context_block.BlockId;
const ContextBlock = context_block.ContextBlock;

/// Secondary index entry mapping metadata value to block IDs
const IndexEntry = struct {
    value: []const u8, // Metadata field value (e.g., "function", "struct")
    block_ids: std.ArrayList(BlockId), // All blocks with this metadata value

    /// Initialize new index entry with allocated value storage
    pub fn init(allocator: std.mem.Allocator, value: []const u8) IndexEntry {
        return IndexEntry{
            .value = value,
            .block_ids = std.ArrayList(BlockId).init(allocator),
        };
    }

    /// Clean up allocated memory for this index entry
    pub fn deinit(self: *IndexEntry, allocator: std.mem.Allocator) void {
        allocator.free(self.value);
        self.block_ids.deinit();
    }

    /// Add a block ID to this index entry, avoiding duplicates
    pub fn add_block(self: *IndexEntry, block_id: BlockId) !void {
        // Avoid duplicates - common in update scenarios
        for (self.block_ids.items) |existing_id| {
            if (existing_id.eql(block_id)) return;
        }
        try self.block_ids.append(block_id);
    }

    pub fn remove_block(self: *IndexEntry, block_id: BlockId) void {
        for (self.block_ids.items, 0..) |existing_id, i| {
            if (existing_id.eql(block_id)) {
                _ = self.block_ids.swapRemove(i);
                return;
            }
        }
    }
};

/// Secondary index for a specific metadata field
pub const MetadataFieldIndex = struct {
    field_name: []const u8, // e.g., "unit_type"
    entries: std.HashMap(
        []const u8,
        IndexEntry,
        StringContext,
        std.hash_map.default_max_load_percentage,
    ),
    arena_coordinator: *const ArenaCoordinator,
    backing_allocator: std.mem.Allocator,

    const StringContext = struct {
        pub fn hash(self: @This(), s: []const u8) u64 {
            _ = self;
            return std.hash_map.hashString(s);
        }

        pub fn eql(self: @This(), a: []const u8, b: []const u8) bool {
            _ = self;
            return std.mem.eql(u8, a, b);
        }
    };

    pub fn init(
        allocator: std.mem.Allocator,
        arena_coordinator: *const ArenaCoordinator,
        field_name: []const u8,
    ) !MetadataFieldIndex {
        return MetadataFieldIndex{
            .field_name = try allocator.dupe(u8, field_name),
            .entries = std.HashMap(
                []const u8,
                IndexEntry,
                StringContext,
                std.hash_map.default_max_load_percentage,
            ).init(allocator),
            .arena_coordinator = arena_coordinator,
            .backing_allocator = allocator,
        };
    }

    pub fn deinit(self: *MetadataFieldIndex) void {
        var iterator = self.entries.iterator();
        while (iterator.next()) |entry| {
            entry.value_ptr.deinit(self.backing_allocator);
        }
        self.entries.deinit();
        self.backing_allocator.free(self.field_name);
    }

    /// Add or update a block in the index
    pub fn put_block(self: *MetadataFieldIndex, block: ContextBlock) !void {
        const metadata_value = try self.extract_metadata_field(block.metadata_json);
        defer if (metadata_value.len > 0) self.arena_coordinator.allocator().free(metadata_value);

        if (metadata_value.len == 0) return; // Field not present in this block

        const result = try self.entries.getOrPut(metadata_value);
        if (!result.found_existing) {
            const owned_value = try self.backing_allocator.dupe(u8, metadata_value);
            result.key_ptr.* = owned_value;
            result.value_ptr.* = IndexEntry.init(self.backing_allocator, owned_value);
        }

        try result.value_ptr.add_block(block.id);
    }

    /// Remove a block from the index
    pub fn remove_block(self: *MetadataFieldIndex, block: ContextBlock) !void {
        const metadata_value = try self.extract_metadata_field(block.metadata_json);
        defer if (metadata_value.len > 0) self.arena_coordinator.allocator().free(metadata_value);

        if (metadata_value.len == 0) return;

        if (self.entries.getPtr(metadata_value)) |entry| {
            entry.remove_block(block.id);

            // Clean up empty entries to prevent memory leaks
            if (entry.block_ids.items.len == 0) {
                var owned_entry = self.entries.fetchRemove(metadata_value).?;
                owned_entry.value.deinit(self.backing_allocator);
            }
        }
    }

    /// Lookup all blocks with a specific metadata value
    pub fn find_blocks_by_value(self: *const MetadataFieldIndex, value: []const u8) ?[]const BlockId {
        if (self.entries.get(value)) |entry| {
            return entry.block_ids.items;
        }
        return null;
    }

    /// Calculate all unique values for this metadata field
    pub fn calculate_all_values(self: *const MetadataFieldIndex, allocator: std.mem.Allocator) ![][]const u8 {
        var values = std.ArrayList([]const u8).init(allocator);
        errdefer values.deinit();

        var iterator = self.entries.iterator();
        while (iterator.next()) |entry| {
            try values.append(entry.key_ptr.*);
        }

        return values.toOwnedSlice();
    }

    /// Get statistics about this index
    pub fn statistics(self: *const MetadataFieldIndex) IndexStatistics {
        var total_blocks: u32 = 0;
        var max_blocks_per_value: u32 = 0;

        var iterator = self.entries.iterator();
        while (iterator.next()) |entry| {
            const count = @as(u32, @intCast(entry.value_ptr.block_ids.items.len));
            total_blocks += count;
            max_blocks_per_value = @max(max_blocks_per_value, count);
        }

        return IndexStatistics{
            .field_name = self.field_name,
            .unique_values = @intCast(self.entries.count()),
            .total_blocks = total_blocks,
            .max_blocks_per_value = max_blocks_per_value,
        };
    }

    /// Extract metadata field value from JSON metadata
    fn extract_metadata_field(self: *const MetadataFieldIndex, metadata_json: []const u8) ![]const u8 {
        const field_pattern = try std.fmt.allocPrint(
            self.arena_coordinator.allocator(),
            "\"{s}\":",
            .{self.field_name},
        );
        defer self.arena_coordinator.allocator().free(field_pattern);

        const start_pos = std.mem.indexOf(u8, metadata_json, field_pattern) orelse return "";
        const value_start = start_pos + field_pattern.len;

        var pos = value_start;
        while (pos < metadata_json.len and (metadata_json[pos] == ' ' or metadata_json[pos] == '"')) {
            pos += 1;
        }

        var end_pos = pos;
        while (end_pos < metadata_json.len and metadata_json[end_pos] != '"' and metadata_json[end_pos] != ',') {
            end_pos += 1;
        }

        if (pos >= end_pos) return "";

        const value = metadata_json[pos..end_pos];
        return try self.arena_coordinator.allocator().dupe(u8, value);
    }
};

/// Statistics about a metadata field index
pub const IndexStatistics = struct {
    field_name: []const u8,
    unique_values: u32,
    total_blocks: u32,
    max_blocks_per_value: u32,

    /// Calculate selectivity (lower is more selective)
    pub fn selectivity(self: IndexStatistics) f32 {
        if (self.total_blocks == 0) return 1.0;
        return @as(f32, @floatFromInt(self.unique_values)) / @as(f32, @floatFromInt(self.total_blocks));
    }

    /// Check if this index is worth using for optimization
    pub fn is_selective(self: IndexStatistics) bool {
        return self.selectivity() < 0.5 and self.max_blocks_per_value < self.total_blocks / 2;
    }
};

/// Multi-field metadata index manager
pub const MetadataIndexManager = struct {
    indexes: std.ArrayList(MetadataFieldIndex),
    arena_coordinator: *const ArenaCoordinator,
    backing_allocator: std.mem.Allocator,

    pub fn init(
        allocator: std.mem.Allocator,
        arena_coordinator: *const ArenaCoordinator,
    ) MetadataIndexManager {
        return MetadataIndexManager{
            .indexes = std.ArrayList(MetadataFieldIndex).init(allocator),
            .arena_coordinator = arena_coordinator,
            .backing_allocator = allocator,
        };
    }

    pub fn deinit(self: *MetadataIndexManager) void {
        for (self.indexes.items) |*index| {
            index.deinit();
        }
        self.indexes.deinit();
    }

    /// Add an index for a specific metadata field
    pub fn add_field_index(self: *MetadataIndexManager, field_name: []const u8) !void {
        const index = try MetadataFieldIndex.init(self.backing_allocator, self.arena_coordinator, field_name);
        try self.indexes.append(index);
    }

    /// Update all indexes when a block is added
    pub fn put_block(self: *MetadataIndexManager, block: ContextBlock) !void {
        for (self.indexes.items) |*index| {
            try index.put_block(block);
        }
    }

    /// Update all indexes when a block is removed
    pub fn remove_block(self: *MetadataIndexManager, block: ContextBlock) !void {
        for (self.indexes.items) |*index| {
            try index.remove_block(block);
        }
    }

    /// Find index for a specific field
    pub fn find_index(self: *const MetadataIndexManager, field_name: []const u8) ?*const MetadataFieldIndex {
        for (self.indexes.items) |*index| {
            if (std.mem.eql(u8, index.field_name, field_name)) {
                return index;
            }
        }
        return null;
    }

    /// Clear all indexes (used during memtable flush)
    pub fn clear(self: *MetadataIndexManager) void {
        for (self.indexes.items) |*index| {
            var iterator = index.entries.iterator();
            while (iterator.next()) |entry| {
                entry.value_ptr.deinit(self.backing_allocator);
            }
            index.entries.clearRetainingCapacity();
        }
    }
};

test "metadata field index basic operations" {
    const testing = std.testing;
    const allocator = testing.allocator;

    var arena = std.heap.ArenaAllocator.init(allocator);
    defer arena.deinit();
    const arena_coordinator = ArenaCoordinator.init(&arena);

    var index = try MetadataFieldIndex.init(allocator, &arena_coordinator, "unit_type");
    defer index.deinit();

    const block1 = ContextBlock{
        .id = try BlockId.from_hex("11111111111111111111111111111111"),
        .version = 1,
        .source_uri = "test1.zig",
        .metadata_json = "{\"unit_type\": \"function\", \"other\": \"value\"}",
        .content = "pub fn test1() void {}",
    };

    const block2 = ContextBlock{
        .id = try BlockId.from_hex("22222222222222222222222222222222"),
        .version = 1,
        .source_uri = "test2.zig",
        .metadata_json = "{\"unit_type\": \"function\", \"other\": \"value\"}",
        .content = "pub fn test2() void {}",
    };

    const block3 = ContextBlock{
        .id = try BlockId.from_hex("33333333333333333333333333333333"),
        .version = 1,
        .source_uri = "test3.zig",
        .metadata_json = "{\"unit_type\": \"struct\", \"other\": \"value\"}",
        .content = "const Test = struct {};",
    };

    // Add blocks to index
    try index.put_block(block1);
    try index.put_block(block2);
    try index.put_block(block3);

    // Test lookup
    const function_blocks = index.find_blocks_by_value("function");
    try testing.expect(function_blocks != null);
    try testing.expectEqual(@as(usize, 2), function_blocks.?.len);

    const struct_blocks = index.find_blocks_by_value("struct");
    try testing.expect(struct_blocks != null);
    try testing.expectEqual(@as(usize, 1), struct_blocks.?.len);

    const missing_blocks = index.find_blocks_by_value("enum");
    try testing.expect(missing_blocks == null);

    // Test statistics
    const stats = index.statistics();
    try testing.expectEqual(@as(u32, 2), stats.unique_values); // "function", "struct"
    try testing.expectEqual(@as(u32, 3), stats.total_blocks);
    try testing.expectEqual(@as(u32, 2), stats.max_blocks_per_value); // 2 function blocks
}

test "metadata index manager integration" {
    const testing = std.testing;
    const allocator = testing.allocator;

    var arena = std.heap.ArenaAllocator.init(allocator);
    defer arena.deinit();
    const arena_coordinator = ArenaCoordinator.init(&arena);

    var manager = MetadataIndexManager.init(allocator, &arena_coordinator);
    defer manager.deinit();

    // Add indexes for common fields
    try manager.add_field_index("unit_type");

    const test_block = ContextBlock{
        .id = try BlockId.from_hex("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"),
        .version = 1,
        .source_uri = "manager_test.zig",
        .metadata_json = "{\"unit_type\": \"test\", \"complexity\": \"low\"}",
        .content = "test \"manager integration\" {}",
    };

    try manager.put_block(test_block);

    const unit_type_index = manager.find_index("unit_type");
    try testing.expect(unit_type_index != null);

    const test_blocks = unit_type_index.?.find_blocks_by_value("test");
    try testing.expect(test_blocks != null);
    try testing.expectEqual(@as(usize, 1), test_blocks.?.len);

    // Test removal
    try manager.remove_block(test_block);
    const after_removal = unit_type_index.?.find_blocks_by_value("test");
    try testing.expect(after_removal == null);
}
