//! In-memory block index for the KausalDB memtable.
//!
//! This module provides fast insertion, lookup, and deletion of blocks using a HashMap
//! backed by arena allocation for content storage. Follows the arena refresh pattern
//! to eliminate dangling allocator references and enable O(1) bulk memory cleanup.

const std = @import("std");
const assert = @import("../core/assert.zig").assert;
const assert_fmt = @import("../core/assert.zig").assert_fmt;
const fatal_assert = @import("../core/assert.zig").fatal_assert;
const context_block = @import("../core/types.zig");
const ownership = @import("../core/ownership.zig");

const ContextBlock = context_block.ContextBlock;
const BlockId = context_block.BlockId;
const OwnedBlock = ownership.OwnedBlock;
const BlockOwnership = ownership.BlockOwnership;

/// In-memory block index using arena refresh pattern for efficient bulk operations.
/// Provides fast writes and reads while maintaining O(1) memory cleanup through
/// storage engine arena reset. Uses type-safe OwnedBlock system to prevent dangling
/// allocator references.
///
/// Arena refresh pattern: BlockIndex uses direct StorageEngine pointer for all content
/// allocation, eliminating temporal coupling with arena resets. HashMap uses stable
/// backing allocator while content uses StorageEngine's current arena state.
pub const BlockIndex = struct {
    blocks: std.HashMap(
        BlockId,
        OwnedBlock,
        BlockIdContext,
        std.hash_map.default_max_load_percentage,
    ),
    /// Direct pointer to StorageEngine for safe arena allocation (avoids circular dependency)
    storage_engine_ptr: *anyopaque,
    /// Stable backing allocator for HashMap structure
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

    /// Initialize empty block index following arena refresh pattern.
    /// HashMap uses stable backing allocator while content uses StorageEngine reference
    /// to prevent dangling allocator references after arena resets.
    pub fn init(storage_engine_ptr: *anyopaque, backing: std.mem.Allocator) BlockIndex {
        var blocks = std.HashMap(
            BlockId,
            OwnedBlock,
            BlockIdContext,
            std.hash_map.default_max_load_percentage,
        ).init(backing);

        // Ensure minimum capacity to prevent integer overflow in hash map operations
        blocks.ensureTotalCapacity(1) catch |err| {
            // If we can't allocate even 1 entry, we're in serious trouble
            std.debug.panic("Failed to allocate minimum HashMap capacity: {}", .{err});
        };

        return BlockIndex{
            .blocks = blocks, // HashMap uses stable backing allocator
            .storage_engine_ptr = storage_engine_ptr, // Direct reference to StorageEngine
            .backing_allocator = backing,
            .memory_used = 0,
        };
    }

    /// Clean up BlockIndex resources following arena refresh pattern.
    /// Only clears HashMap since arena memory is managed by StorageEngine.
    /// Content memory cleanup happens when StorageEngine resets its storage arena.
    pub fn deinit(self: *BlockIndex) void {
        self.blocks.clearAndFree();
        // Arena memory is owned by StorageEngine - no local cleanup needed
    }

    /// Insert or update a block in the index using StorageEngine's arena for content storage.
    /// Clones all string content through StorageEngine methods to ensure memory safety and
    /// eliminate dangling allocator references after arena resets.
    pub fn put_block(self: *BlockIndex, block: ContextBlock) !void {
        assert_fmt(@intFromPtr(self) != 0, "BlockIndex self pointer cannot be null", .{});

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

        // Clone strings using StorageEngine's arena allocation methods
        // The StorageEngine interface is: pub fn duplicate_storage(self: *StorageEngine, comptime T: type, slice: []const T) ![]T
        const StorageEngineType = @import("engine.zig").StorageEngine;
        const storage_engine: *StorageEngineType = @ptrCast(@alignCast(self.storage_engine_ptr));

        const cloned_block = ContextBlock{
            .id = block.id,
            .version = block.version,
            .source_uri = try storage_engine.duplicate_storage(u8, block.source_uri),
            .metadata_json = try storage_engine.duplicate_storage(u8, block.metadata_json),
            .content = try storage_engine.duplicate_storage(u8, block.content),
        };

        // Debug-time validation that StorageEngine correctly clones strings.
        // These checks ensure memory safety during development but compile to no-ops
        // in release builds for zero-overhead production performance.
        if (block.source_uri.len > 0) {
            assert_fmt(@intFromPtr(cloned_block.source_uri.ptr) != @intFromPtr(block.source_uri.ptr), "StorageEngine failed to clone source_uri - returned original pointer", .{});
        }
        if (block.metadata_json.len > 0) {
            assert_fmt(@intFromPtr(cloned_block.metadata_json.ptr) != @intFromPtr(block.metadata_json.ptr), "StorageEngine failed to clone metadata_json - returned original pointer", .{});
        }
        if (block.content.len > 0) {
            assert_fmt(@intFromPtr(cloned_block.content.ptr) != @intFromPtr(block.content.ptr), "StorageEngine failed to clone content - returned original pointer", .{});
        }

        // Adjust memory accounting for replacement case
        // Calculate memory changes but don't update accounting until after HashMap operation succeeds
        var old_memory: usize = 0;
        if (self.blocks.get(cloned_block.id)) |existing_block| {
            old_memory = existing_block.block.source_uri.len + existing_block.block.metadata_json.len + existing_block.block.content.len;
            fatal_assert(self.memory_used >= old_memory, "Memory accounting underflow: tracked={} removing={} - indicates heap corruption", .{ self.memory_used, old_memory });
        }

        const new_memory = block.source_uri.len + block.metadata_json.len + block.content.len;
        // Arena ownership tracking is handled at StorageEngine level
        const owned_block = OwnedBlock.init(cloned_block, .memtable_manager, null);

        // Critical: Update HashMap first, then memory accounting to prevent corruption on allocation failure
        try self.blocks.put(cloned_block.id, owned_block);

        // Update memory accounting only after successful HashMap operation
        self.memory_used = self.memory_used - old_memory + new_memory;
    }

    /// Find a block by ID with ownership validation.
    /// Returns pointer to the block if found and accessor has valid ownership.
    pub fn find_block(self: *const BlockIndex, block_id: BlockId, accessor: BlockOwnership) ?*const ContextBlock {
        if (self.blocks.getPtr(block_id)) |owned_block_ptr| {
            // Validate ownership access through OwnedBlock
            return owned_block_ptr.read_runtime(accessor);
        }
        return null;
    }

    /// Find a block by ID and return the OwnedBlock for ownership operations.
    /// Returns pointer to the OwnedBlock if found, allowing access to ownership metadata.
    /// Used during transition from runtime to compile-time ownership validation.
    pub fn find_block_runtime(self: *const BlockIndex, block_id: BlockId, accessor: BlockOwnership) ?*const OwnedBlock {
        // Safety check: ensure HashMap has capacity to prevent integer overflow
        // This can happen if HashMap was cleared without being re-initialized
        if (self.blocks.capacity() == 0) {
            return null;
        }

        if (self.blocks.getPtr(block_id)) |owned_block_ptr| {
            // Validate ownership access but return the full OwnedBlock
            _ = owned_block_ptr.read_runtime(accessor);
            return owned_block_ptr;
        }
        return null;
    }

    /// Remove a block from the index and update memory accounting.
    /// Memory is not immediately freed (arena handles bulk deallocation),
    /// but accounting is updated for accurate memory usage tracking.
    pub fn remove_block(self: *BlockIndex, block_id: BlockId) void {
        if (self.blocks.get(block_id)) |existing_block| {
            const old_memory = existing_block.block.source_uri.len + existing_block.block.metadata_json.len + existing_block.block.content.len;
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

    /// Clear all blocks in preparation for StorageEngine arena reset.
    /// Retains HashMap capacity for efficient reuse after StorageEngine resets arena.
    /// This is the key operation that enables O(1) bulk deallocation through StorageEngine.
    /// Called during memtable flush before StorageEngine reclaims all arena memory at once.
    pub fn clear(self: *BlockIndex) void {
        // Arena refresh pattern: Arena reset happens at StorageEngine level
        // BlockIndex only clears its HashMap structure
        fatal_assert(@intFromPtr(self) != 0, "BlockIndex self pointer is null - memory corruption detected", .{});

        self.blocks.clearRetainingCapacity();
        // Arena memory reset handled by StorageEngine - no local reset needed
        self.memory_used = 0;
    }
};

const testing = std.testing;

// Test helper: Mock StorageEngine for unit tests
const MockStorageEngine = struct {
    allocator: std.mem.Allocator,

    pub fn duplicate_storage(self: *MockStorageEngine, comptime T: type, slice: []const T) ![]T {
        return self.allocator.dupe(T, slice);
    }
};

fn create_mock_storage_engine(allocator: std.mem.Allocator) *MockStorageEngine {
    // Safety: Test allocator allocation only fails in OOM, which aborts tests appropriately
    const mock = testing.allocator.create(MockStorageEngine) catch unreachable;
    mock.* = MockStorageEngine{ .allocator = allocator };
    return mock;
}

test "block index initialization creates empty index" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const mock_engine = create_mock_storage_engine(arena.allocator());
    defer testing.allocator.destroy(mock_engine);
    var index = BlockIndex.init(mock_engine, testing.allocator);
    defer index.deinit();

    try testing.expectEqual(@as(u32, 0), index.block_count());
    try testing.expectEqual(@as(u64, 0), index.memory_usage());
}

test "put and find block operations work correctly" {
    const allocator = testing.allocator;

    var arena = std.heap.ArenaAllocator.init(allocator);
    defer arena.deinit();
    const mock_engine = create_mock_storage_engine(arena.allocator());
    defer allocator.destroy(mock_engine);
    var index = BlockIndex.init(mock_engine, allocator);
    defer index.deinit();

    const block_id = BlockId.generate();
    const test_block = ContextBlock{
        .id = block_id,
        .version = 1,
        .source_uri = "test://example.zig",
        .metadata_json = "{}",
        .content = "test content",
    };

    try index.put_block(test_block);
    try testing.expectEqual(@as(u32, 1), index.block_count());

    const found_block = index.find_block(block_id, .memtable_manager);
    try testing.expect(found_block != null);
    try testing.expect(found_block.?.id.eql(block_id));
    try testing.expectEqualStrings("test content", found_block.?.content);
}

test "put block clones strings into arena" {
    const allocator = testing.allocator;

    var arena = std.heap.ArenaAllocator.init(allocator);
    defer arena.deinit();
    const mock_engine = create_mock_storage_engine(arena.allocator());
    defer allocator.destroy(mock_engine);
    var index = BlockIndex.init(mock_engine, allocator);
    defer index.deinit();

    const original_content = try allocator.dupe(u8, "original content");
    defer allocator.free(original_content);

    const block_id = BlockId.generate();
    const test_block = ContextBlock{
        .id = block_id,
        .version = 1,
        .source_uri = "test://example.zig",
        .metadata_json = "{}",
        .content = original_content,
    };

    try index.put_block(test_block);

    const found_block = index.find_block(block_id, .memtable_manager);
    try testing.expect(found_block != null);
    try testing.expectEqualStrings("original content", found_block.?.content);
    // Verify it's a different pointer (cloned, not original)
    try testing.expect(@intFromPtr(found_block.?.content.ptr) != @intFromPtr(original_content.ptr));
}

test "remove block updates count and memory accounting" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const mock_engine = create_mock_storage_engine(arena.allocator());
    defer testing.allocator.destroy(mock_engine);
    var index = BlockIndex.init(mock_engine, testing.allocator);
    defer index.deinit();

    const block_id = BlockId.generate();
    const test_block = ContextBlock{
        .id = block_id,
        .version = 1,
        .source_uri = "test://example.zig",
        .metadata_json = "{}",
        .content = "test content",
    };

    try index.put_block(test_block);
    const memory_before = index.memory_usage();
    try testing.expect(memory_before > 0);

    index.remove_block(block_id);
    try testing.expectEqual(@as(u32, 0), index.block_count());
    try testing.expectEqual(@as(u64, 0), index.memory_usage());
}

test "block replacement updates memory accounting correctly" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const mock_engine = create_mock_storage_engine(arena.allocator());
    defer testing.allocator.destroy(mock_engine);
    var index = BlockIndex.init(mock_engine, testing.allocator);
    defer index.deinit();

    const block_id = BlockId.generate();
    const original_block = ContextBlock{
        .id = block_id,
        .version = 1,
        .source_uri = "test://example.zig",
        .metadata_json = "{}",
        .content = "short",
    };

    try index.put_block(original_block);
    const memory_after_first = index.memory_usage();

    const replacement_block = ContextBlock{
        .id = block_id,
        .version = 2,
        .source_uri = "test://example.zig",
        .metadata_json = "{}",
        .content = "much longer content than before",
    };

    try index.put_block(replacement_block);
    const memory_after_second = index.memory_usage();

    // Should still have 1 block
    try testing.expectEqual(@as(u32, 1), index.block_count());

    // Memory should have increased due to longer content
    try testing.expect(memory_after_second > memory_after_first);

    const found_block = index.find_block(block_id, .memtable_manager);
    try testing.expect(found_block != null);
    try testing.expectEqual(@as(u32, 2), found_block.?.version);
    try testing.expectEqualStrings("much longer content than before", found_block.?.content);
}

test "clear operation resets index to empty state efficiently" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const mock_engine = create_mock_storage_engine(arena.allocator());
    defer testing.allocator.destroy(mock_engine);
    var index = BlockIndex.init(mock_engine, testing.allocator);
    defer index.deinit();

    for (0..10) |i| {
        const block_id = BlockId.generate();
        const test_block = ContextBlock{
            .id = block_id,
            .version = 1,
            .source_uri = "test://example.zig",
            .metadata_json = "{}",
            .content = try std.fmt.allocPrint(testing.allocator, "content {}", .{i}),
        };
        defer testing.allocator.free(test_block.content);

        try index.put_block(test_block);
    }

    try testing.expectEqual(@as(u32, 10), index.block_count());
    try testing.expect(index.memory_usage() > 0);

    index.clear();
    try testing.expectEqual(@as(u32, 0), index.block_count());
    try testing.expectEqual(@as(u64, 0), index.memory_usage());
}

test "memory accounting tracks string content accurately" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const mock_engine = create_mock_storage_engine(arena.allocator());
    defer testing.allocator.destroy(mock_engine);
    var index = BlockIndex.init(mock_engine, testing.allocator);
    defer index.deinit();

    const source_uri = "file://example.zig";
    const metadata_json = "{}";
    const content = "test content here";

    const block_id = BlockId.generate();
    const test_block = ContextBlock{
        .id = block_id,
        .version = 1,
        .source_uri = source_uri,
        .metadata_json = metadata_json,
        .content = content,
    };

    try index.put_block(test_block);

    const expected_memory = source_uri.len + metadata_json.len + content.len;
    try testing.expectEqual(@as(u64, expected_memory), index.memory_usage());
}

test "large block content handling" {
    const allocator = testing.allocator;

    var arena = std.heap.ArenaAllocator.init(allocator);
    defer arena.deinit();
    const mock_engine = create_mock_storage_engine(arena.allocator());
    defer allocator.destroy(mock_engine);
    var index = BlockIndex.init(mock_engine, allocator);
    defer index.deinit();

    const large_content = try allocator.alloc(u8, 1024 * 1024);
    defer allocator.free(large_content);
    @memset(large_content, 'X');

    const block_id = BlockId.generate();
    const test_block = ContextBlock{
        .id = block_id,
        .version = 1,
        .source_uri = "test://large.zig",
        .metadata_json = "{}",
        .content = large_content,
    };

    try index.put_block(test_block);

    const found_block = index.find_block(block_id, .memtable_manager);
    try testing.expect(found_block != null);
    try testing.expectEqual(@as(usize, 1024 * 1024), found_block.?.content.len);
    try testing.expectEqual(@as(u8, 'X'), found_block.?.content[0]);
    try testing.expectEqual(@as(u8, 'X'), found_block.?.content[1024 * 1024 - 1]);
}
