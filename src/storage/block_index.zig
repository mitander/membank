//! In-memory block index (memtable) with arena-per-subsystem memory management.
//!
//! Implements the write-optimized memtable component of the LSM-tree storage engine.
//! Uses ArenaAllocator for O(1) bulk deallocation when flushing to SSTables,
//! eliminating per-block memory management overhead and preventing memory leaks.
//! Memory accounting tracks string content only (not HashMap overhead) to enable
//! accurate flush thresholds based on actual data size.

const std = @import("std");
const assert = @import("../core/assert.zig").assert;
const assert_fmt = @import("../core/assert.zig").assert_fmt;
const fatal_assert = @import("../core/assert.zig").fatal_assert;
const context_block = @import("../core/types.zig");
const arena = @import("../core/arena.zig");
const ownership = @import("../core/ownership.zig");
const ContextBlock = context_block.ContextBlock;
const BlockId = context_block.BlockId;
const TypedArenaType = arena.TypedArenaType;
const ArenaOwnership = arena.ArenaOwnership;
const OwnedBlock = ownership.OwnedBlock;
const BlockOwnership = ownership.BlockOwnership;

/// In-memory block index using typed arena allocation for efficient bulk operations.
/// Provides fast writes and reads while maintaining O(1) memory cleanup through
/// arena reset when flushing to SSTables. Uses type-safe OwnedBlock system
/// to prevent cross-subsystem memory access violations.
pub const BlockIndex = struct {
    blocks: std.HashMap(
        BlockId,
        OwnedBlock,
        BlockIdContext,
        std.hash_map.default_max_load_percentage,
    ),
    block_arena: TypedArenaType(ContextBlock, BlockIndex),
    backing_allocator: std.mem.Allocator,
    /// Track total memory used by block content strings in arena.
    /// Excludes HashMap overhead to provide clean flush threshold calculations.
    memory_used: u64,

    /// Hash context for BlockId keys in HashMap.
    /// Uses Wyhash for performance with cryptographically strong distribution.
    pub const BlockIdContext = struct {
        pub fn hash(self: @This(), block_id: BlockId) u64 {
            _ = self;
            var hasher = std.hash.Wyhash.init(0);
            hasher.update(&block_id.bytes);
            return hasher.final();
        }

        pub fn eql(self: @This(), a: BlockId, b: BlockId) bool {
            _ = self;
            return a.eql(b);
        }
    };

    /// Initialize empty block index with typed arena for string storage.
    /// HashMap uses stable backing allocator while block content uses typed arena
    /// to enable O(1) bulk deallocation on flush with ownership tracking.
    pub fn init(allocator: std.mem.Allocator) BlockIndex {
        return BlockIndex{
            .blocks = std.HashMap(
                BlockId,
                OwnedBlock,
                BlockIdContext,
                std.hash_map.default_max_load_percentage,
            ).init(allocator), // HashMap uses stable backing allocator
            .block_arena = TypedArenaType(ContextBlock, BlockIndex).init(allocator, .memtable_manager),
            .backing_allocator = allocator,
            .memory_used = 0,
        };
    }

    /// Clean up all resources including HashMap and typed arena.
    /// HashMap must be cleared before arena deallocation to prevent use-after-free
    /// of arena-allocated strings referenced by HashMap entries.
    pub fn deinit(self: *BlockIndex) void {
        self.blocks.clearAndFree();
        self.block_arena.deinit();
    }

    /// Insert or update a block in the index with typed arena-allocated string storage.
    /// Clones all string content into the typed arena to ensure memory safety and
    /// enable O(1) bulk deallocation. Creates OwnedBlock for ownership tracking.
    pub fn put_block(self: *BlockIndex, block: ContextBlock) !void {
        assert_fmt(@intFromPtr(self) != 0, "BlockIndex self pointer cannot be null", .{});
        assert_fmt(@intFromPtr(&self.block_arena) != 0, "BlockIndex arena pointer cannot be null", .{});

        const arena_allocator = self.block_arena.allocator();

        // Validate string lengths to prevent allocation of corrupted sizes
        assert_fmt(block.source_uri.len < 1024 * 1024, "source_uri too large: {} bytes", .{block.source_uri.len});
        assert_fmt(block.metadata_json.len < 1024 * 1024, "metadata_json too large: {} bytes", .{block.metadata_json.len});
        assert_fmt(block.content.len < 100 * 1024 * 1024, "content too large: {} bytes", .{block.content.len});

        // Catch null pointers masquerading as slices
        if (block.source_uri.len > 0) {
            assert_fmt(@intFromPtr(block.source_uri.ptr) != 0, "source_uri has null pointer with non-zero length", .{});
        }
        if (block.metadata_json.len > 0) {
            assert_fmt(@intFromPtr(block.metadata_json.ptr) != 0, "metadata_json has null pointer with non-zero length", .{});
        }
        if (block.content.len > 0) {
            assert_fmt(@intFromPtr(block.content.ptr) != 0, "content has null pointer with non-zero length", .{});
        }

        const cloned_block = ContextBlock{
            .id = block.id,
            .version = block.version,
            .source_uri = try arena_allocator.dupe(u8, block.source_uri),
            .metadata_json = try arena_allocator.dupe(u8, block.metadata_json),
            .content = try arena_allocator.dupe(u8, block.content),
        };

        // CRITICAL: Verify that the arena allocator *actually* cloned the strings and
        // did not return an aliased pointer. An alias here would be a catastrophic
        // memory safety violation, leading to a use-after-free when this arena is reset
        // on memtable flush. This fatal assertion makes our safety contract explicit.
        if (block.source_uri.len > 0) {
            fatal_assert(@intFromPtr(cloned_block.source_uri.ptr) != @intFromPtr(block.source_uri.ptr), "Arena failed to clone source_uri - returned original pointer, heap corruption detected", .{});
        }
        if (block.metadata_json.len > 0) {
            fatal_assert(@intFromPtr(cloned_block.metadata_json.ptr) != @intFromPtr(block.metadata_json.ptr), "Arena failed to clone metadata_json - returned original pointer, heap corruption detected", .{});
        }
        if (block.content.len > 0) {
            fatal_assert(@intFromPtr(cloned_block.content.ptr) != @intFromPtr(block.content.ptr), "Arena failed to clone content - returned original pointer, heap corruption detected", .{});
        }

        // Create owned block with memtable ownership
        const owned_block = OwnedBlock.init(cloned_block, .memtable_manager, null);

        // Adjust memory accounting for replacement case
        if (self.blocks.get(block.id)) |existing_owned_block| {
            const existing_block = existing_owned_block.read_immutable();
            const old_memory = existing_block.source_uri.len + existing_block.metadata_json.len + existing_block.content.len;
            fatal_assert(self.memory_used >= old_memory, "Memory accounting underflow: tracked={} removing={} - indicates heap corruption", .{ self.memory_used, old_memory });
            self.memory_used -= old_memory;
        }

        const new_memory = block.source_uri.len + block.metadata_json.len + block.content.len;
        self.memory_used += new_memory;

        try self.blocks.put(block.id, owned_block);
    }

    /// Find a block by ID with ownership validation.
    /// Returns pointer to the owned block for safe access with ownership tracking.
    pub fn find_block(self: *const BlockIndex, block_id: BlockId, accessor: BlockOwnership) ?*const ContextBlock {
        if (self.blocks.get(block_id)) |owned_block| {
            return owned_block.read(accessor);
        }
        return null;
    }

    /// Remove a block from the index and update memory accounting.
    /// Memory is not immediately freed (arena handles bulk deallocation),
    /// but accounting is updated for accurate memory usage tracking.
    pub fn remove_block(self: *BlockIndex, block_id: BlockId) void {
        if (self.blocks.get(block_id)) |existing_owned_block| {
            const existing_block = existing_owned_block.read_immutable();
            const old_memory = existing_block.source_uri.len + existing_block.metadata_json.len + existing_block.content.len;
            fatal_assert(self.memory_used >= old_memory, "Memory accounting underflow during removal: tracked={} removing={} - indicates heap corruption", .{ self.memory_used, old_memory });
            self.memory_used -= old_memory;
        }
        _ = self.blocks.remove(block_id);
    }

    /// Get current number of blocks in the index.
    pub fn block_count(self: *const BlockIndex) u32 {
        return @intCast(self.blocks.count());
    }

    /// Get current memory usage of block content strings in bytes.
    /// Excludes HashMap overhead to provide clean measurement for flush thresholds.
    pub fn memory_usage(self: *const BlockIndex) u64 {
        return self.memory_used;
    }

    /// Clear all blocks and reset typed arena for O(1) bulk deallocation.
    /// Retains HashMap and arena capacity for efficient reuse after flush.
    /// This is the key operation that makes typed arena-per-subsystem efficient.
    pub fn clear(self: *BlockIndex) void {
        // CRITICAL: Validate structure integrity before performing the O(1) arena reset.
        // This operation frees all allocated memory at once. If these pointers are corrupted,
        // the arena reset could cause a double-free or use-after-free, corrupting the heap.
        // These assertions ensure the typed arena-per-subsystem pattern operates safely.
        fatal_assert(@intFromPtr(self) != 0, "BlockIndex self pointer is null - memory corruption detected", .{});
        fatal_assert(@intFromPtr(&self.block_arena) != 0, "BlockIndex arena pointer is null - memory corruption detected", .{});

        self.blocks.clearRetainingCapacity();
        self.block_arena.reset();
        self.memory_used = 0;

        fatal_assert(self.blocks.count() == 0, "HashMap clear failed - data structure corruption detected", .{});
    }
};

const testing = std.testing;

test "block index initialization creates empty index" {
    var index = BlockIndex.init(testing.allocator);
    defer index.deinit();

    try testing.expectEqual(@as(u32, 0), index.block_count());
    try testing.expectEqual(@as(u64, 0), index.memory_usage());
}

test "put and find block operations work correctly" {
    const allocator = testing.allocator;

    var index = BlockIndex.init(allocator);
    defer index.deinit();

    const block_id = BlockId.generate();
    const block = ContextBlock{
        .id = block_id,
        .version = 1,
        .source_uri = "file://test.zig",
        .metadata_json = "{}",
        .content = "test content",
    };

    try index.put_block(block);

    try testing.expectEqual(@as(u32, 1), index.block_count());
    try testing.expect(index.memory_usage() > 0);

    const found_block = index.find_block(block_id, .simulation_test);
    try testing.expect(found_block != null);
    try testing.expect(found_block.?.id.eql(block_id));
    try testing.expectEqualStrings("test content", found_block.?.content);
}

test "put block clones strings into arena" {
    const allocator = testing.allocator;

    var index = BlockIndex.init(allocator);
    defer index.deinit();

    const original_content = try allocator.dupe(u8, "original content");
    defer allocator.free(original_content);

    const block = ContextBlock{
        .id = BlockId.generate(),
        .version = 1,
        .source_uri = "file://test.zig",
        .metadata_json = "{}",
        .content = original_content,
    };

    try index.put_block(block);

    const found_block = index.find_block(block.id, .simulation_test).?;

    // Verify content is equal but memory addresses are different (cloned)
    try testing.expectEqualStrings(original_content, found_block.content);
    try testing.expect(@intFromPtr(original_content.ptr) != @intFromPtr(found_block.content.ptr));
}

test "remove block updates count and memory accounting" {
    var index = BlockIndex.init(testing.allocator);
    defer index.deinit();

    const block_id = BlockId.generate();
    const block = ContextBlock{
        .id = block_id,
        .version = 1,
        .source_uri = "file://test.zig",
        .metadata_json = "{}",
        .content = "test content",
    };

    try index.put_block(block);
    const memory_after_put = index.memory_usage();
    try testing.expect(memory_after_put > 0);

    index.remove_block(block_id);

    try testing.expectEqual(@as(u32, 0), index.block_count());
    try testing.expectEqual(@as(u64, 0), index.memory_usage());
    try testing.expect(index.find_block(block_id, .simulation_test) == null);
}

test "block replacement updates memory accounting correctly" {
    var index = BlockIndex.init(testing.allocator);
    defer index.deinit();

    const block_id = BlockId.generate();

    // Insert initial block
    const block_v1 = ContextBlock{
        .id = block_id,
        .version = 1,
        .source_uri = "file://test.zig",
        .metadata_json = "{}",
        .content = "short",
    };

    try index.put_block(block_v1);
    const memory_v1 = index.memory_usage();

    // Replace with larger block
    const block_v2 = ContextBlock{
        .id = block_id,
        .version = 2,
        .source_uri = "file://test.zig",
        .metadata_json = "{}",
        .content = "much longer content here",
    };

    try index.put_block(block_v2);
    const memory_v2 = index.memory_usage();

    try testing.expectEqual(@as(u32, 1), index.block_count());
    try testing.expect(memory_v2 > memory_v1);

    const found = index.find_block(block_id, .simulation_test).?;
    try testing.expectEqual(@as(u32, 2), found.version);
}

test "clear operation resets index to empty state efficiently" {
    var index = BlockIndex.init(testing.allocator);
    defer index.deinit();

    for (0..10) |i| {
        const block = ContextBlock{
            .id = BlockId.generate(),
            .version = 1,
            .source_uri = "file://test.zig",
            .metadata_json = "{}",
            .content = try std.fmt.allocPrint(testing.allocator, "content {}", .{i}),
        };
        defer testing.allocator.free(block.content);

        try index.put_block(block);
    }

    try testing.expectEqual(@as(u32, 10), index.block_count());
    try testing.expect(index.memory_usage() > 0);

    index.clear();

    try testing.expectEqual(@as(u32, 0), index.block_count());
    try testing.expectEqual(@as(u64, 0), index.memory_usage());
}

test "memory accounting tracks string content accurately" {
    var index = BlockIndex.init(testing.allocator);
    defer index.deinit();

    const source_uri = "file://example.zig";
    const metadata_json = "{\"type\":\"function\"}";
    const content = "fn example() void {}";

    const expected_memory = source_uri.len + metadata_json.len + content.len;

    const block = ContextBlock{
        .id = BlockId.generate(),
        .version = 1,
        .source_uri = source_uri,
        .metadata_json = metadata_json,
        .content = content,
    };

    try index.put_block(block);

    try testing.expectEqual(expected_memory, index.memory_usage());
}

test "block id hash context provides good distribution" {
    const ctx = BlockIndex.BlockIdContext{};

    // Generate different block IDs and verify they hash to different values
    const id1 = BlockId.generate();
    const id2 = BlockId.generate();

    const hash1 = ctx.hash(id1);
    const hash2 = ctx.hash(id2);

    try testing.expect(hash1 != hash2);

    // Same ID should hash to same value
    try testing.expectEqual(hash1, ctx.hash(id1));

    try testing.expect(ctx.eql(id1, id1));
    try testing.expect(!ctx.eql(id1, id2));
}

test "large block content handling" {
    const allocator = testing.allocator;

    var index = BlockIndex.init(allocator);
    defer index.deinit();

    const large_content = try allocator.alloc(u8, 1024 * 1024);
    defer allocator.free(large_content);

    // Fill with recognizable pattern
    for (large_content, 0..) |*byte, i| {
        byte.* = @intCast(i % 256);
    }

    const block = ContextBlock{
        .id = BlockId.generate(),
        .version = 1,
        .source_uri = "file://large.zig",
        .metadata_json = "{}",
        .content = large_content,
    };

    try index.put_block(block);

    const found = index.find_block(block.id, .simulation_test).?;
    try testing.expectEqual(large_content.len, found.content.len);
    try testing.expectEqual(@as(u8, 255), found.content[255]); // Check pattern

    // Memory usage should account for the large content
    try testing.expect(index.memory_usage() >= 1024 * 1024);
}
