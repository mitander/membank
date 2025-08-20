//! Comprehensive ownership safety integration tests
//!
//! Tests the complete type-safe ownership system across multiple subsystems
//! to verify memory safety, ownership transfer, and cross-arena protection.

const builtin = @import("builtin");
const std = @import("std");

const kausaldb = @import("kausaldb");

const testing = std.testing;

const ArenaOwnership = kausaldb.arena.ArenaOwnership;
const BlockId = kausaldb.core_types.BlockId;
const BlockOwnership = kausaldb.ownership.BlockOwnership;
const ContextBlock = kausaldb.core_types.ContextBlock;
const OwnedBlock = kausaldb.ownership.OwnedBlock;
const OwnedBlockCollection = kausaldb.ownership.OwnedBlockCollection;
const TypedArenaType = kausaldb.arena.TypedArenaType;

// Test subsystem simulators
const MemtableSubsystem = struct {
    arena: TypedArenaType(ContextBlock, @This()),
    blocks: OwnedBlockCollection,

    const Self = @This();

    pub fn init(allocator: std.mem.Allocator) Self {
        return Self{
            .arena = TypedArenaType(ContextBlock, Self).init(allocator, .memtable_manager),
            .blocks = OwnedBlockCollection.init(allocator, .memtable_manager),
        };
    }

    pub fn deinit(self: *Self) void {
        self.blocks.blocks.deinit();
        self.arena.deinit();
    }

    pub fn add_block(self: *Self, block: ContextBlock) !void {
        const owned_block = try self.arena.alloc();
        owned_block.* = block;

        var wrapped = OwnedBlock.init(owned_block.*, .memtable_manager, &self.arena.arena);
        try self.blocks.add_block(&wrapped);
    }

    pub fn transfer_to_storage(self: *Self, storage: *StorageSubsystem) !void {
        for (self.blocks.blocks.items) |*owned_block| {
            const cloned = try owned_block.clone_with_ownership(storage.arena.allocator(), .storage_engine, &storage.arena.arena);
            try storage.blocks.add_block_by_clone(&cloned, storage.arena.allocator());
        }
        // Clear memtable after transfer - arena reset handles memory cleanup
        self.blocks.blocks.clearRetainingCapacity();
        self.arena.reset();
    }
};

const StorageSubsystem = struct {
    arena: TypedArenaType(ContextBlock, @This()),
    blocks: OwnedBlockCollection,

    const Self = @This();

    pub fn init(allocator: std.mem.Allocator) Self {
        return Self{
            .arena = TypedArenaType(ContextBlock, Self).init(allocator, .storage_engine),
            .blocks = OwnedBlockCollection.init(allocator, .storage_engine),
        };
    }

    pub fn deinit(self: *Self) void {
        self.blocks.blocks.deinit();
        self.arena.deinit();
    }

    pub fn find_block(self: *Self, block_id: BlockId) ?*const ContextBlock {
        for (self.blocks.blocks.items) |*owned_block| {
            if (owned_block.block.id.eql(block_id)) {
                return owned_block.read(.storage_engine);
            }
        }
        return null;
    }
};

const QuerySubsystem = struct {
    arena: TypedArenaType(u8, @This()),
    temp_blocks: std.array_list.Managed(OwnedBlock),

    const Self = @This();

    pub fn init(allocator: std.mem.Allocator) Self {
        return Self{
            .arena = TypedArenaType(u8, Self).init(allocator, .query_engine),
            .temp_blocks = std.array_list.Managed(OwnedBlock).init(allocator),
        };
    }

    pub fn deinit(self: *Self) void {
        // Arena cleanup handles memory for cloned blocks
        self.temp_blocks.deinit();
        self.arena.deinit();
    }

    pub fn create_query_result(self: *Self, source_block: *const ContextBlock) !void {
        // Clone block for query processing with temporary ownership
        const cloned = try OwnedBlock.take_ownership(source_block.*, .temporary)
            .clone_with_ownership(self.arena.allocator(), .query_engine, &self.arena.arena);
        try self.temp_blocks.append(cloned);
    }
};

test "cross-subsystem ownership safety" {
    var memtable = MemtableSubsystem.init(testing.allocator);
    defer memtable.deinit();

    var storage = StorageSubsystem.init(testing.allocator);
    defer storage.deinit();

    var query = QuerySubsystem.init(testing.allocator);
    defer query.deinit();

    // Verify different arenas have different ownership
    try testing.expect(memtable.arena.ownership != storage.arena.ownership);
    try testing.expect(storage.arena.ownership != query.arena.ownership);
    try testing.expect(memtable.arena.ownership != query.arena.ownership);

    // Each arena should validate its own ownership
    memtable.arena.validate_ownership_access(.memtable_manager);
    storage.arena.validate_ownership_access(.storage_engine);
    query.arena.validate_ownership_access(.query_engine);

    // All should accept temporary access
    memtable.arena.validate_ownership_access(.temporary);
    storage.arena.validate_ownership_access(.temporary);
    query.arena.validate_ownership_access(.temporary);
}

test "complete block lifecycle with ownership transfers" {
    var memtable = MemtableSubsystem.init(testing.allocator);
    defer memtable.deinit();

    var storage = StorageSubsystem.init(testing.allocator);
    defer storage.deinit();

    var query = QuerySubsystem.init(testing.allocator);
    defer query.deinit();

    // Create test blocks
    const block1 = ContextBlock{
        .id = BlockId.from_hex("1111111111111111AAAAAAAAAAAAAAAA") catch unreachable,
        .version = 1,
        .source_uri = "test://lifecycle1",
        .metadata_json = "{}",
        .content = "lifecycle test content 1",
    };

    const block2 = ContextBlock{
        .id = BlockId.from_hex("2222222222222222BBBBBBBBBBBBBBBB") catch unreachable,
        .version = 1,
        .source_uri = "test://lifecycle2",
        .metadata_json = "{}",
        .content = "lifecycle test content 2",
    };

    // Step 1: Add blocks to memtable
    try memtable.add_block(block1);
    try memtable.add_block(block2);
    try testing.expect(memtable.blocks.blocks.items.len == 2);

    // Step 2: Transfer blocks to storage (memtable flush simulation)
    try memtable.transfer_to_storage(&storage);
    try testing.expect(memtable.blocks.blocks.items.len == 0); // Memtable cleared
    try testing.expect(storage.blocks.blocks.items.len == 2); // Storage has blocks

    // Step 3: Query can read from storage
    const found_block = storage.find_block(block1.id);
    try testing.expect(found_block != null);
    try testing.expect(found_block.?.id.eql(block1.id));

    // Step 4: Query creates temporary copies for processing
    try query.create_query_result(found_block.?);
    try testing.expect(query.temp_blocks.items.len == 1);

    // Step 5: Verify ownership isolation
    // Each subsystem owns its blocks independently
    for (storage.blocks.blocks.items) |*storage_block| {
        try testing.expect(storage_block.is_owned_by(.storage_engine));
    }

    for (query.temp_blocks.items) |*query_block| {
        try testing.expect(query_block.is_owned_by(.query_engine));
    }
}

test "memory safety with arena reset and reuse" {
    var memtable = MemtableSubsystem.init(testing.allocator);
    defer memtable.deinit();

    // Add blocks and reset multiple times to test safety
    for (0..5) |cycle| {
        const block_id_hex = switch (cycle) {
            0 => "0000000000000000AAAAAAAAAAAAAAAA",
            1 => "1111111111111111AAAAAAAAAAAAAAAA",
            2 => "2222222222222222AAAAAAAAAAAAAAAA",
            3 => "3333333333333333AAAAAAAAAAAAAAAA",
            4 => "4444444444444444AAAAAAAAAAAAAAAA",
            else => unreachable,
        };

        const block = ContextBlock{
            .id = BlockId.from_hex(block_id_hex) catch unreachable,
            .version = @intCast(cycle + 1),
            .source_uri = "test://safety",
            .metadata_json = "{}",
            .content = "safety test content",
        };

        try memtable.add_block(block);
        try testing.expect(memtable.blocks.blocks.items.len == 1);

        // Clear and reset for next cycle - arena reset handles memory cleanup
        memtable.blocks.blocks.clearRetainingCapacity();
        memtable.arena.reset();
    }
}

test "ownership validation with fatal assertions" {
    var memtable = MemtableSubsystem.init(testing.allocator);
    defer memtable.deinit();

    const block = ContextBlock{
        .id = BlockId.from_hex("AAAAAAAAAAAAAAAA1111111111111111") catch unreachable,
        .version = 1,
        .source_uri = "test://validation",
        .metadata_json = "{}",
        .content = "validation test",
    };

    // Create owned block
    var owned = OwnedBlock.init(block, .memtable_manager, null);

    // Valid access patterns
    _ = owned.read(.memtable_manager); // Owner can read
    _ = owned.read(.temporary); // Temporary can read
    _ = owned.write(.memtable_manager); // Owner can write
    _ = owned.write(.temporary); // Temporary can write

    // Test ownership queries
    try testing.expect(owned.is_owned_by(.memtable_manager));
    try testing.expect(!owned.is_owned_by(.storage_engine));
    try testing.expect(!owned.is_owned_by(.query_engine));

    // Test ownership transfer
    owned.transfer_ownership(.storage_engine, null);
    try testing.expect(owned.is_owned_by(.storage_engine));
    try testing.expect(!owned.is_owned_by(.memtable_manager));

    // After transfer, new owner can access
    _ = owned.read(.storage_engine);
    _ = owned.write(.storage_engine);
}

test "large scale ownership operations" {
    var memtable = MemtableSubsystem.init(testing.allocator);
    defer memtable.deinit();

    var storage = StorageSubsystem.init(testing.allocator);
    defer storage.deinit();

    // Create many blocks to test scalability
    const num_blocks = 100;

    for (0..num_blocks) |i| {
        var block_id_bytes: [16]u8 = undefined;
        // Create unique block IDs
        std.mem.writeInt(u64, block_id_bytes[0..8], i, .little);
        std.mem.writeInt(u64, block_id_bytes[8..16], i + 1000, .little);

        const block = ContextBlock{
            .id = BlockId{ .bytes = block_id_bytes },
            .version = @intCast(i + 1),
            .source_uri = "test://scale",
            .metadata_json = "{}",
            .content = "scale test content",
        };

        try memtable.add_block(block);
    }

    try testing.expect(memtable.blocks.blocks.items.len == num_blocks);

    // Transfer all blocks
    try memtable.transfer_to_storage(&storage);
    try testing.expect(memtable.blocks.blocks.items.len == 0);
    try testing.expect(storage.blocks.blocks.items.len == num_blocks);

    // Verify all blocks are properly owned by storage
    for (storage.blocks.blocks.items) |*owned_block| {
        try testing.expect(owned_block.is_owned_by(.storage_engine));
    }
}

test "memory accounting accuracy" {
    if (builtin.mode != .Debug) return; // Debug info only available in debug mode

    var arena = TypedArenaType(ContextBlock, MemtableSubsystem).init(testing.allocator, .memtable_manager);
    defer arena.deinit();

    // Check initial state
    var info = arena.debug_info();
    try testing.expect(info.allocation_count == 0);
    try testing.expect(info.total_bytes == 0);

    // Allocate some blocks
    _ = try arena.alloc(); // 1 block
    _ = try arena.alloc_slice(3); // 3 blocks
    _ = try arena.alloc(); // 1 more block

    // Check accounting
    info = arena.debug_info();
    try testing.expect(info.allocation_count == 5); // 1 + 3 + 1
    try testing.expect(info.total_bytes == 5 * @sizeOf(ContextBlock));

    // Reset and verify cleanup
    arena.reset();
    info = arena.debug_info();
    try testing.expect(info.allocation_count == 0);
    try testing.expect(info.total_bytes == 0);
}
