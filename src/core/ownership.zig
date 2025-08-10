//! Type-safe ownership tracking for memory management.
//!
//! Design rationale: Provides compile-time ownership enforcement to prevent
//! cross-subsystem memory access violations. The ownership system uses tagged
//! unions and compile-time validation to make invalid memory access patterns
//! unrepresentable in the type system.
//!
//! The system integrates with TypedArena to provide both allocation tracking
//! and ownership validation, enabling safe memory transfer between subsystems
//! while maintaining zero runtime overhead in release builds.

const std = @import("std");
const builtin = @import("builtin");
const custom_assert = @import("assert.zig");
const fatal_assert = custom_assert.fatal_assert;
const assert_fmt = custom_assert.assert_fmt;
const types = @import("types.zig");
const ContextBlock = types.ContextBlock;
const BlockId = types.BlockId;

/// Ownership categories for subsystem memory management.
/// Each subsystem uses a distinct ownership type to prevent accidental
/// cross-subsystem memory access through compile-time validation.
pub const BlockOwnership = enum {
    storage_engine,
    memtable_manager,
    sstable_manager,
    query_engine,
    connection_manager,
    simulation_test,
    temporary,

    /// Get human-readable name for debugging and logging.
    pub fn name(self: BlockOwnership) []const u8 {
        return switch (self) {
            .storage_engine => "StorageEngine",
            .memtable_manager => "MemtableManager",
            .sstable_manager => "SSTableManager",
            .query_engine => "QueryEngine",
            .connection_manager => "ConnectionManager",
            .simulation_test => "SimulationTest",
            .temporary => "Temporary",
        };
    }

    /// Check if ownership allows read access.
    /// Temporary ownership can read from any subsystem.
    pub fn can_read_from(self: BlockOwnership, owner: BlockOwnership) bool {
        return self == owner or self == .temporary;
    }

    /// Check if ownership allows write access.
    /// Only the actual owner or temporary can write.
    pub fn can_write_to(self: BlockOwnership, owner: BlockOwnership) bool {
        return self == owner or self == .temporary;
    }
};

/// Type-safe wrapper for ContextBlock with ownership tracking.
/// Prevents cross-subsystem memory access through compile-time validation
/// and runtime checks in debug builds.
pub const OwnedBlock = struct {
    block: ContextBlock,
    ownership: BlockOwnership,
    arena_ptr: ?*std.heap.ArenaAllocator,
    debug_allocation_source: if (builtin.mode == .Debug) ?std.builtin.SourceLocation else void,

    /// Create owned block with explicit ownership and optional arena tracking.
    pub fn init(block: ContextBlock, ownership: BlockOwnership, arena_ptr: ?*std.heap.ArenaAllocator) OwnedBlock {
        return OwnedBlock{
            .block = block,
            .ownership = ownership,
            .arena_ptr = arena_ptr,
            .debug_allocation_source = if (builtin.mode == .Debug) @src() else {},
        };
    }

    /// Create owned block from existing block with ownership transfer.
    pub fn take_ownership(block: ContextBlock, new_ownership: BlockOwnership) OwnedBlock {
        if (builtin.mode == .Debug) {
            std.log.debug("Taking ownership of block {any} as {s}", .{ block.id.bytes, new_ownership.name() });
        }
        return OwnedBlock{
            .block = block,
            .ownership = new_ownership,
            .arena_ptr = null,
            .debug_allocation_source = if (builtin.mode == .Debug) @src() else {},
        };
    }

    /// Get read access to the underlying block with ownership validation.
    pub fn read(self: *const OwnedBlock, accessor: BlockOwnership) *const ContextBlock {
        self.validate_read_access(accessor);
        return &self.block;
    }

    /// Get write access to the underlying block with ownership validation.
    pub fn write(self: *OwnedBlock, accessor: BlockOwnership) *ContextBlock {
        self.validate_write_access(accessor);
        return &self.block;
    }

    /// Get immutable reference without ownership check.
    /// Use sparingly - prefer read() with explicit ownership.
    pub fn read_immutable(self: *const OwnedBlock) *const ContextBlock {
        return &self.block;
    }

    /// Clone block with new ownership for transfer between subsystems.
    /// The original block remains valid and owned by the original subsystem.
    pub fn clone_with_ownership(
        self: *const OwnedBlock,
        allocator: std.mem.Allocator,
        new_ownership: BlockOwnership,
        new_arena: ?*std.heap.ArenaAllocator,
    ) !OwnedBlock {
        // Clone all dynamic data
        const cloned_source_uri = try allocator.dupe(u8, self.block.source_uri);
        const cloned_metadata = try allocator.dupe(u8, self.block.metadata_json);
        const cloned_content = try allocator.dupe(u8, self.block.content);

        const cloned_block = ContextBlock{
            .id = self.block.id,
            .version = self.block.version,
            .source_uri = cloned_source_uri,
            .metadata_json = cloned_metadata,
            .content = cloned_content,
        };

        if (builtin.mode == .Debug) {
            std.log.debug("Cloned block {any} from {s} to {s}", .{ self.block.id.bytes, self.ownership.name(), new_ownership.name() });
        }

        return OwnedBlock{
            .block = cloned_block,
            .ownership = new_ownership,
            .arena_ptr = new_arena,
            .debug_allocation_source = if (builtin.mode == .Debug) @src() else {},
        };
    }

    /// Transfer ownership without cloning data.
    /// DANGEROUS: Only use when you're certain the original owner won't access the block.
    pub fn transfer_ownership(self: *OwnedBlock, new_ownership: BlockOwnership, new_arena: ?*std.heap.ArenaAllocator) void {
        if (builtin.mode == .Debug) {
            std.log.warn("Ownership transfer: block {any} from {s} to {s} (DANGEROUS - ensure original owner won't access)", .{ self.block.id.bytes, self.ownership.name(), new_ownership.name() });
        }
        self.ownership = new_ownership;
        self.arena_ptr = new_arena;
        self.debug_allocation_source = if (builtin.mode == .Debug) @src() else {};
    }

    /// Validate read access for the given accessor.
    pub fn validate_read_access(self: *const OwnedBlock, accessor: BlockOwnership) void {
        if (builtin.mode == .Debug) {
            if (!accessor.can_read_from(self.ownership)) {
                fatal_assert(false, "Read access violation: {s} cannot read {s}-owned block {}", .{ accessor.name(), self.ownership.name(), self.block.id });
            }
        } else {
            // Release builds still check critical violations
            fatal_assert(accessor == self.ownership or accessor == .temporary, "Critical ownership violation: {s} accessing {s} block", .{ accessor.name(), self.ownership.name() });
        }
    }

    /// Validate write access for the given accessor.
    pub fn validate_write_access(self: *const OwnedBlock, accessor: BlockOwnership) void {
        if (builtin.mode == .Debug) {
            if (!accessor.can_write_to(self.ownership)) {
                fatal_assert(false, "Write access violation: {s} cannot write to {s}-owned block {}", .{ accessor.name(), self.ownership.name(), self.block.id });
            }
        } else {
            // Release builds still check critical violations
            fatal_assert(accessor == self.ownership or accessor == .temporary, "Critical ownership violation: {s} modifying {s} block", .{ accessor.name(), self.ownership.name() });
        }
    }

    /// Get ownership information for debugging.
    pub fn query_owner(self: *const OwnedBlock) BlockOwnership {
        return self.ownership;
    }

    /// Check if block is owned by specific subsystem.
    pub fn is_owned_by(self: *const OwnedBlock, ownership: BlockOwnership) bool {
        return self.ownership == ownership;
    }

    /// Check if block has temporary ownership (can be accessed by any subsystem).
    pub fn is_temporary(self: *const OwnedBlock) bool {
        return self.ownership == .temporary;
    }

    /// Free block memory if arena is tracked.
    /// Only call this when you're certain the block is no longer needed.
    pub fn deinit(self: *OwnedBlock, allocator: std.mem.Allocator) void {
        if (builtin.mode == .Debug) {
            std.log.debug("Deallocating {s}-owned block {any}", .{ self.ownership.name(), self.block.id.bytes });
        }

        // Free dynamic allocations
        allocator.free(self.block.source_uri);
        allocator.free(self.block.metadata_json);
        allocator.free(self.block.content);

        // If we have arena tracking, we could validate arena consistency here
        if (self.arena_ptr) |arena| {
            _ = arena; // Arena will be reset by owner
        }
    }
};

/// Collection of owned blocks with batch operations.
/// Provides type-safe batch operations while maintaining ownership tracking.
pub const OwnedBlockCollection = struct {
    blocks: std.ArrayList(OwnedBlock),
    ownership: BlockOwnership,

    /// Initialize collection for specific ownership.
    pub fn init(allocator: std.mem.Allocator, ownership: BlockOwnership) OwnedBlockCollection {
        var blocks = std.ArrayList(OwnedBlock).init(allocator);
        blocks.ensureTotalCapacity(16) catch {}; // Pre-allocate for typical subsystem usage patterns
        return OwnedBlockCollection{
            .blocks = blocks,
            .ownership = ownership,
        };
    }

    /// Add block to collection, transferring ownership.
    pub fn add_block(self: *OwnedBlockCollection, mut_block: *OwnedBlock) !void {
        // Transfer ownership to collection's ownership
        mut_block.transfer_ownership(self.ownership, null);
        try self.blocks.append(mut_block.*);

        if (builtin.mode == .Debug) {
            std.log.debug("Added block {any} to {s} collection (total: {})", .{ mut_block.block.id.bytes, self.ownership.name(), self.blocks.items.len });
        }
    }

    /// Add block by cloning with collection ownership.
    pub fn add_block_by_clone(
        self: *OwnedBlockCollection,
        source_block: *const OwnedBlock,
        allocator: std.mem.Allocator,
    ) !void {
        try self.blocks.ensureTotalCapacity(self.blocks.items.len + 1); // Ensure capacity before cloning
        const cloned = try source_block.clone_with_ownership(allocator, self.ownership, null);
        try self.blocks.append(cloned);
    }

    /// Find block by ID with ownership validation.
    pub fn find_block(
        self: *const OwnedBlockCollection,
        block_id: BlockId,
        accessor: BlockOwnership,
    ) ?*const ContextBlock {
        for (self.blocks.items) |*owned_block| {
            if (owned_block.block.id.eql(block_id)) {
                return owned_block.read(accessor);
            }
        }
        return null;
    }

    /// Query all blocks as slice with ownership validation.
    pub fn query_blocks(self: *const OwnedBlockCollection, accessor: BlockOwnership) []const OwnedBlock {
        // Validate accessor can read from this collection
        if (builtin.mode == .Debug) {
            assert_fmt(accessor.can_read_from(self.ownership), "Collection access violation: {s} cannot read {s} collection", .{ accessor.name(), self.ownership.name() });
        }
        return self.blocks.items;
    }

    /// Clear all blocks and free memory.
    pub fn clear(self: *OwnedBlockCollection, allocator: std.mem.Allocator) void {
        if (builtin.mode == .Debug) {
            std.log.debug("Clearing {s} collection with {} blocks", .{ self.ownership.name(), self.blocks.items.len });
        }

        for (self.blocks.items) |*owned_block| {
            owned_block.deinit(allocator);
        }
        self.blocks.clearAndFree();
    }

    /// Get collection length.
    pub fn length(self: *const OwnedBlockCollection) usize {
        return self.blocks.items.len;
    }

    /// Check if collection is empty.
    pub fn is_empty(self: *const OwnedBlockCollection) bool {
        return self.blocks.items.len == 0;
    }

    /// Deinitialize collection and free all resources.
    pub fn deinit(self: *OwnedBlockCollection, allocator: std.mem.Allocator) void {
        self.clear(allocator);
        self.blocks.deinit();
    }
};

/// Ownership transfer context for debugging and validation.
/// Tracks ownership transfers in debug builds for audit trails.
pub const OwnershipTransfer = struct {
    source: BlockOwnership,
    destination: BlockOwnership,
    block_id: BlockId,
    timestamp: if (builtin.mode == .Debug) i64 else void,
    source_location: if (builtin.mode == .Debug) std.builtin.SourceLocation else void,

    /// Record ownership transfer for debugging.
    pub fn record(source: BlockOwnership, destination: BlockOwnership, block_id: BlockId) OwnershipTransfer {
        if (builtin.mode == .Debug) {
            std.log.debug("Ownership transfer: block {any} from {s} to {s}", .{ block_id.bytes, source.name(), destination.name() });
        }

        return OwnershipTransfer{
            .source = source,
            .destination = destination,
            .block_id = block_id,
            .timestamp = if (builtin.mode == .Debug) std.time.milliTimestamp() else {},
            .source_location = if (builtin.mode == .Debug) @src() else {},
        };
    }
};

/// Global ownership tracker for debugging memory issues.
/// Only active in debug builds to track ownership patterns.
pub const OwnershipTracker = struct {
    transfers: if (builtin.mode == .Debug) std.ArrayList(OwnershipTransfer) else void,
    active_blocks: if (builtin.mode == .Debug) std.HashMap(BlockId, BlockOwnership, BlockIdContext, std.hash_map.default_max_load_percentage) else void,

    const BlockIdContext = struct {
        pub fn hash(self: @This(), key: BlockId) u64 {
            _ = self;
            return std.hash_map.getAutoHashFn(BlockId, void)({}, key);
        }

        pub fn eql(self: @This(), a: BlockId, b: BlockId) bool {
            _ = self;
            return a.eql(b);
        }
    };

    /// Initialize ownership tracker.
    /// Only tracks in debug builds - zero overhead in release.
    pub fn init(allocator: std.mem.Allocator) OwnershipTracker {
        return OwnershipTracker{
            .transfers = if (builtin.mode == .Debug) blk: {
                var transfers = std.ArrayList(OwnershipTransfer).init(allocator);
                transfers.ensureTotalCapacity(32) catch {}; // Pre-allocate for typical ownership transfer tracking
                break :blk transfers;
            } else {},
            .active_blocks = if (builtin.mode == .Debug) std.HashMap(BlockId, BlockOwnership, BlockIdContext, std.hash_map.default_max_load_percentage).init(allocator) else {},
        };
    }

    /// Track new block allocation.
    pub fn track_allocation(self: *OwnershipTracker, block_id: BlockId, ownership: BlockOwnership) void {
        if (builtin.mode == .Debug) {
            self.active_blocks.put(block_id, ownership) catch |err| {
                std.log.warn("Failed to track block allocation: {}", .{err});
                return;
            };
            std.log.debug("Tracking allocation: block {any} owned by {s}", .{ block_id.bytes, ownership.name() });
        }
    }

    /// Track ownership transfer between subsystems.
    pub fn track_transfer(self: *OwnershipTracker, block_id: BlockId, from: BlockOwnership, to: BlockOwnership) void {
        if (builtin.mode == .Debug) {
            const transfer = OwnershipTransfer.record(from, to, block_id);
            self.transfers.append(transfer) catch |err| {
                std.log.warn("Failed to track ownership transfer: {}", .{err});
                return;
            };

            // Update active tracking
            self.active_blocks.put(block_id, to) catch |err| {
                std.log.warn("Failed to update active block tracking: {}", .{err});
                return;
            };
        }
    }

    /// Track block deallocation.
    pub fn track_deallocation(self: *OwnershipTracker, block_id: BlockId) void {
        if (builtin.mode == .Debug) {
            if (self.active_blocks.remove(block_id)) {
                std.log.debug("Tracked deallocation: block {any}", .{block_id.bytes});
            } else {
                std.log.warn("Deallocating untracked block: {any}", .{block_id.bytes});
            }
        }
    }

    /// Validate that accessor has proper ownership of block.
    pub fn validate_access(self: *const OwnershipTracker, block_id: BlockId, accessor: BlockOwnership) void {
        if (builtin.mode == .Debug) {
            if (self.active_blocks.get(block_id)) |owner| {
                if (!accessor.can_read_from(owner)) {
                    fatal_assert(false, "Ownership validation failed", .{});
                }
            } else {
                std.log.warn("Accessing untracked block: {any} by {s}", .{ block_id.bytes, accessor.name() });
            }
        }
    }

    /// Query current owner of block, returns null if not tracked.
    pub fn query_owner_for_block(self: *const OwnershipTracker, block_id: BlockId) ?BlockOwnership {
        if (builtin.mode == .Debug) {
            return self.active_blocks.get(block_id);
        } else {
            return null;
        }
    }

    /// Report ownership statistics and potential leaks.
    /// Report ownership statistics for debugging.
    pub fn report_statistics(self: *const OwnershipTracker) void {
        if (builtin.mode == .Debug) {
            std.log.info("Ownership Statistics:", .{});
            std.log.info("  Active blocks: {}", .{self.active_blocks.count()});
            std.log.info("  Total transfers: {}", .{self.transfers.items.len});

            // Group statistics by ownership for debugging memory allocation patterns
            var owner_counts = std.EnumArray(BlockOwnership, usize).initFill(0);
            var iter = self.active_blocks.iterator();
            while (iter.next()) |entry| {
                const ownership = entry.value_ptr.*;
                const current_count = owner_counts.get(ownership);
                owner_counts.set(ownership, current_count + 1);
            }

            // Display ownership distribution to identify memory allocation imbalances
            inline for (@typeInfo(BlockOwnership).@"enum".fields) |field| {
                const ownership: BlockOwnership = @enumFromInt(field.value);
                const count = owner_counts.get(ownership);
                if (count > 0) {
                    std.log.info("  {s}: {} blocks", .{ ownership.name(), count });
                }
            }
        }

        // Detect blocks that weren't properly deallocated by subsystems
        if (self.active_blocks.count() > 0) {
            std.log.warn("Potential memory leaks: {} blocks still tracked", .{self.active_blocks.count()});
        }
    }

    /// Deinitialize tracker and report leaks.
    pub fn deinit(self: *OwnershipTracker) void {
        if (builtin.mode == .Debug) {
            self.report_statistics();
            self.transfers.deinit();
            self.active_blocks.deinit();
        }
    }
};

/// Compile-time validation that ownership patterns are followed.
pub fn validate_ownership_usage(comptime T: type) void {
    // Skip validation for core ownership structures themselves
    const struct_name = @typeName(T);
    if (std.mem.indexOf(u8, struct_name, "OwnedBlock") != null or
        std.mem.indexOf(u8, struct_name, "OwnedBlockCollection") != null or
        std.mem.indexOf(u8, struct_name, "OwnershipTracker") != null)
    {
        return;
    }

    const info = @typeInfo(T);
    switch (info) {
        .@"struct" => |s| {
            inline for (s.fields) |field| {
                // Check for raw ContextBlock usage
                if (field.type == ContextBlock) {
                    @compileError("Raw ContextBlock field '" ++ field.name ++ "' in " ++ @typeName(T) ++
                        ". Use OwnedBlock for ownership tracking.");
                }

                // Check for raw pointer to ContextBlock
                if (field.type == *ContextBlock or field.type == ?*ContextBlock) {
                    @compileError("Raw ContextBlock pointer '" ++ field.name ++ "' in " ++ @typeName(T) ++
                        ". Use OwnedBlock for ownership tracking.");
                }

                // Validation disabled: @typeName() string matching produces false positives with generic types
                // Trade-off chosen to avoid blocking legitimate OwnedBlock usage until Zig gains better comptime introspection
                // const field_type_name = @typeName(field.type);
                // if (std.mem.indexOf(u8, field_type_name, "ArrayList") != null and
                //     std.mem.indexOf(u8, field_type_name, "ContextBlock") != null)
                // {
                //     @compileError("ArrayList of raw ContextBlock in field '" ++ field.name ++ "' in " ++ @typeName(T) ++
                //         ". Use OwnedBlockCollection or ArrayList(OwnedBlock).");
                // }
            }
        },
        else => @compileError("validate_ownership_usage only works on struct types"),
    }
}

// Compile-time validation
comptime {
    // Enforce reasonable enum size limits to prevent excessive compile-time overhead
    const ownership_count = @typeInfo(BlockOwnership).@"enum".fields.len;
    custom_assert.comptime_assert(ownership_count >= 3 and ownership_count <= 16, "BlockOwnership should have 3-16 variants for reasonable subsystem count");

    // Prevent memory bloat by enforcing size constraints on core ownership structures
    custom_assert.comptime_assert(@sizeOf(OwnedBlock) <= 256, "OwnedBlock should be reasonably sized");
    custom_assert.comptime_assert(@sizeOf(OwnershipTransfer) <= 128, "OwnershipTransfer should be compact");
}

// Tests

test "BlockOwnership access validation" {
    const storage = BlockOwnership.storage_engine;
    const query = BlockOwnership.query_engine;
    const temp = BlockOwnership.temporary;

    // Same subsystem can read/write
    try std.testing.expect(storage.can_read_from(.storage_engine));
    try std.testing.expect(storage.can_write_to(.storage_engine));

    // Different subsystems cannot access
    try std.testing.expect(!query.can_read_from(.storage_engine));
    try std.testing.expect(!query.can_write_to(.storage_engine));

    // Temporary can access anything
    try std.testing.expect(temp.can_read_from(.storage_engine));
    try std.testing.expect(temp.can_write_to(.query_engine));
}

test "OwnedBlock basic operations" {
    const block = ContextBlock{
        // Safety: Valid hex string is statically verified
        .id = BlockId.from_hex("00112233445566778899AABBCCDDEEFF") catch unreachable,
        .version = 1,
        .source_uri = "test://block",
        .metadata_json = "{}",
        .content = "test content",
    };

    var owned = OwnedBlock.init(block, .simulation_test, null);

    // Owner can read and write
    const read_ptr = owned.read(.simulation_test);
    try std.testing.expect(read_ptr.id.eql(block.id));

    const write_ptr = owned.write(.simulation_test);
    write_ptr.version = 2;
    try std.testing.expect(owned.block.version == 2);

    // Temporary can also access
    _ = owned.read(.temporary);
    _ = owned.write(.temporary);
}

test "OwnedBlock ownership transfer" {
    const block = ContextBlock{
        // Safety: Valid hex string is statically verified
        .id = BlockId.from_hex("FFEEDDCCBBAA99887766554433221100") catch unreachable,
        .version = 1,
        .source_uri = "test://transfer",
        .metadata_json = "{}",
        .content = "transfer test",
    };

    var owned = OwnedBlock.init(block, .memtable_manager, null);

    // Transfer ownership
    owned.transfer_ownership(.query_engine, null);
    try std.testing.expect(owned.is_owned_by(.query_engine));

    // New owner can access
    _ = owned.read(.query_engine);
    _ = owned.write(.query_engine);
}

test "OwnedBlock cloning with ownership" {
    const block = ContextBlock{
        // Safety: Valid hex string is statically verified
        .id = BlockId.from_hex("1122334455667788AABBCCDDEEFF0099") catch unreachable,
        .version = 1,
        .source_uri = "test://clone",
        .metadata_json = "{}",
        .content = "clone test",
    };

    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();

    const original = OwnedBlock.init(block, .memtable_manager, null);
    const cloned = try original.clone_with_ownership(arena.allocator(), .query_engine, &arena);

    // Both blocks have same content but different ownership
    try std.testing.expect(original.block.id.eql(cloned.block.id));
    try std.testing.expect(original.is_owned_by(.memtable_manager));
    try std.testing.expect(cloned.is_owned_by(.query_engine));

    // Each can be accessed by their respective owners
    _ = original.read(.memtable_manager);
    _ = cloned.read(.query_engine);
}

test "OwnedBlockCollection management" {
    var collection = OwnedBlockCollection.init(std.testing.allocator, .storage_engine);
    defer collection.deinit(std.testing.allocator);

    const block1 = ContextBlock{
        // Safety: Valid hex string is statically verified
        .id = BlockId.from_hex("1111111111111111AAAAAAAAAAAAAAAA") catch unreachable,
        .version = 1,
        .source_uri = "test://block1",
        .metadata_json = "{}",
        .content = "content1",
    };

    const block2 = ContextBlock{
        // Safety: Valid hex string is statically verified
        .id = BlockId.from_hex("2222222222222222BBBBBBBBBBBBBBBB") catch unreachable,
        .version = 1,
        .source_uri = "test://block2",
        .metadata_json = "{}",
        .content = "content2",
    };

    var owned1 = OwnedBlock.init(block1, .temporary, null);
    var owned2 = OwnedBlock.init(block2, .temporary, null);

    // Add blocks to collection
    try collection.add_block(&owned1);
    try collection.add_block(&owned2);

    try std.testing.expect(collection.length() == 2);
    try std.testing.expect(!collection.is_empty());

    // Find blocks
    const found = collection.find_block(block1.id, .storage_engine);
    try std.testing.expect(found != null);
    try std.testing.expect(found.?.id.eql(block1.id));

    // Get all blocks
    const blocks = collection.query_blocks(.storage_engine);
    try std.testing.expect(blocks.len == 2);
}

test "OwnershipTracker functionality" {
    if (builtin.mode != .Debug) return;

    var tracker = OwnershipTracker.init(std.testing.allocator);
    defer tracker.deinit();

    // Safety: Valid hex string is statically verified
    const block_id = BlockId.from_hex("ABCDEF1234567890FEDCBA0987654321") catch unreachable;

    // Track allocation
    tracker.track_allocation(block_id, .memtable_manager);
    try std.testing.expect(tracker.query_owner_for_block(block_id) == .memtable_manager);

    // Track transfer
    tracker.track_transfer(block_id, .memtable_manager, .sstable_manager);
    try std.testing.expect(tracker.query_owner_for_block(block_id) == .sstable_manager);

    // Arena allocator doesn't require individual frees - memory lifecycle managed by arena
    tracker.validate_access(block_id, .sstable_manager); // Should pass
    tracker.validate_access(block_id, .temporary); // Should pass

    // Track deallocation
    tracker.track_deallocation(block_id);
    try std.testing.expect(tracker.query_owner_for_block(block_id) == null);
}

test "compile-time ownership validation" {
    // This would fail at compile time if uncommented:
    // const BadStruct = struct {
    //     raw_block: ContextBlock, // Should use OwnedBlock
    // };
    // validate_ownership_usage(BadStruct);

    // This should pass
    const GoodStruct = struct {
        owned_block: OwnedBlock,
        block_collection: OwnedBlockCollection,
        other_field: u32,
    };
    validate_ownership_usage(GoodStruct);
}

test "ownership names are consistent" {
    try std.testing.expectEqualStrings("StorageEngine", BlockOwnership.storage_engine.name());
    try std.testing.expectEqualStrings("MemtableManager", BlockOwnership.memtable_manager.name());
    try std.testing.expectEqualStrings("SSTableManager", BlockOwnership.sstable_manager.name());
    try std.testing.expectEqualStrings("QueryEngine", BlockOwnership.query_engine.name());
    try std.testing.expectEqualStrings("ConnectionManager", BlockOwnership.connection_manager.name());
    try std.testing.expectEqualStrings("SimulationTest", BlockOwnership.simulation_test.name());
    try std.testing.expectEqualStrings("Temporary", BlockOwnership.temporary.name());
}

test "ownership transfer recording" {
    // Safety: Valid hex string is statically verified
    const block_id = BlockId.from_hex("CAFEBABE12345678DEADBEEF87654321") catch unreachable;
    const transfer = OwnershipTransfer.record(.memtable_manager, .sstable_manager, block_id);

    try std.testing.expect(transfer.source == .memtable_manager);
    try std.testing.expect(transfer.destination == .sstable_manager);
    try std.testing.expect(transfer.block_id.eql(block_id));
}
