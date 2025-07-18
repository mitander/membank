//! Storage engine for CortexDB Context Block persistence.
//!
//! Implements a Log-Structured Merge-Tree (LSMT) with Write-Ahead Logging
//! for durable, high-performance storage of Context Blocks and Graph Edges.
//! All I/O operations go through the VFS interface for deterministic testing.

const std = @import("std");
const assert = std.debug.assert;
const vfs = @import("vfs");
const context_block = @import("context_block");

const VFS = vfs.VFS;
const ContextBlock = context_block.ContextBlock;
const GraphEdge = context_block.GraphEdge;
const BlockId = context_block.BlockId;

/// Storage engine errors.
pub const StorageError = error{
    /// Block not found in storage
    BlockNotFound,
    /// Corrupted WAL entry
    CorruptedWALEntry,
    /// Invalid checksum
    InvalidChecksum,
    /// Storage already initialized
    AlreadyInitialized,
    /// Storage not initialized
    NotInitialized,
    /// WAL file corrupted
    WALCorrupted,
} || std.mem.Allocator.Error || anyerror;

/// WAL entry types as defined in the data model specification.
const WALEntryType = enum(u8) {
    put_block = 0x01,
    delete_block = 0x02,
    put_edge = 0x03,

    pub fn from_u8(value: u8) !WALEntryType {
        return std.meta.intToEnum(WALEntryType, value) catch error.InvalidWALEntryType;
    }
};

/// WAL entry header structure.
const WALEntry = struct {
    checksum: u64,
    entry_type: WALEntryType,
    payload: []const u8,

    const HEADER_SIZE = 9; // 8 bytes checksum + 1 byte type

    /// Calculate CRC-64 checksum of type and payload.
    fn calculate_checksum(entry_type: WALEntryType, payload: []const u8) u64 {
        var hasher = std.hash.Wyhash.init(0);
        hasher.update(&[_]u8{@intFromEnum(entry_type)});
        hasher.update(payload);
        return hasher.final();
    }

    /// Serialize WAL entry to buffer.
    pub fn serialize(self: WALEntry, buffer: []u8) !usize {
        const total_size = HEADER_SIZE + self.payload.len;
        if (buffer.len < total_size) return error.BufferTooSmall;

        var offset: usize = 0;

        // Write checksum (8 bytes, little-endian)
        std.mem.writeInt(u64, buffer[offset..][0..8], self.checksum, .little);
        offset += 8;

        // Write entry type (1 byte)
        buffer[offset] = @intFromEnum(self.entry_type);
        offset += 1;

        // Write payload
        @memcpy(buffer[offset .. offset + self.payload.len], self.payload);
        offset += self.payload.len;

        return offset;
    }

    /// Deserialize WAL entry from buffer.
    pub fn deserialize(buffer: []const u8, allocator: std.mem.Allocator) !WALEntry {
        if (buffer.len < HEADER_SIZE) return error.BufferTooSmall;

        var offset: usize = 0;

        // Read checksum
        const checksum = std.mem.readInt(u64, buffer[offset..][0..8], .little);
        offset += 8;

        // Read entry type
        const entry_type = try WALEntryType.from_u8(buffer[offset]);
        offset += 1;

        // Read payload
        const payload = try allocator.dupe(u8, buffer[offset..]);

        // Verify checksum
        const expected_checksum = calculate_checksum(entry_type, payload);
        if (checksum != expected_checksum) {
            allocator.free(payload);
            return StorageError.InvalidChecksum;
        }

        return WALEntry{
            .checksum = checksum,
            .entry_type = entry_type,
            .payload = payload,
        };
    }

    /// Create WAL entry for putting a Context Block.
    pub fn create_put_block(block: ContextBlock, allocator: std.mem.Allocator) !WALEntry {
        const payload_size = block.serialized_size();
        const payload = try allocator.alloc(u8, payload_size);
        _ = try block.serialize(payload);

        const checksum = calculate_checksum(.put_block, payload);

        return WALEntry{
            .checksum = checksum,
            .entry_type = .put_block,
            .payload = payload,
        };
    }

    /// Create WAL entry for deleting a Context Block.
    pub fn create_delete_block(block_id: BlockId, allocator: std.mem.Allocator) !WALEntry {
        const payload = try allocator.dupe(u8, &block_id.bytes);
        const checksum = calculate_checksum(.delete_block, payload);

        return WALEntry{
            .checksum = checksum,
            .entry_type = .delete_block,
            .payload = payload,
        };
    }

    /// Create WAL entry for putting a Graph Edge.
    pub fn create_put_edge(edge: GraphEdge, allocator: std.mem.Allocator) !WALEntry {
        const payload = try allocator.alloc(u8, 40); // GraphEdge.SERIALIZED_SIZE
        _ = try edge.serialize(payload);

        const checksum = calculate_checksum(.put_edge, payload);

        return WALEntry{
            .checksum = checksum,
            .entry_type = .put_edge,
            .payload = payload,
        };
    }

    /// Free allocated payload memory.
    pub fn deinit(self: WALEntry, allocator: std.mem.Allocator) void {
        allocator.free(self.payload);
    }
};

/// In-memory block index for fast lookups.
const BlockIndex = struct {
    blocks: std.HashMap(BlockId, ContextBlock, BlockIdContext, std.hash_map.default_max_load_percentage),
    allocator: std.mem.Allocator,

    const BlockIdContext = struct {
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

    pub fn init(allocator: std.mem.Allocator) BlockIndex {
        return BlockIndex{
            .blocks = std.HashMap(BlockId, ContextBlock, BlockIdContext, std.hash_map.default_max_load_percentage).init(allocator),
            .allocator = allocator,
        };
    }

    pub fn deinit(self: *BlockIndex) void {
        var iterator = self.blocks.iterator();
        while (iterator.next()) |entry| {
            entry.value_ptr.deinit(self.allocator);
        }
        self.blocks.deinit();
    }

    pub fn put_block(self: *BlockIndex, block: ContextBlock) !void {
        // Check if block already exists and free it first
        if (self.blocks.fetchRemove(block.id)) |kv| {
            kv.value.deinit(self.allocator);
        }

        const cloned_block = ContextBlock{
            .id = block.id,
            .version = block.version,
            .source_uri = try self.allocator.dupe(u8, block.source_uri),
            .metadata_json = try self.allocator.dupe(u8, block.metadata_json),
            .content = try self.allocator.dupe(u8, block.content),
        };
        try self.blocks.put(block.id, cloned_block);
    }

    pub fn get_block(self: *BlockIndex, block_id: BlockId) ?*const ContextBlock {
        return self.blocks.getPtr(block_id);
    }

    pub fn remove_block(self: *BlockIndex, block_id: BlockId) void {
        if (self.blocks.fetchRemove(block_id)) |kv| {
            kv.value.deinit(self.allocator);
        }
    }

    pub fn block_count(self: *const BlockIndex) u32 {
        return @intCast(self.blocks.count());
    }
};

/// Storage engine state.
pub const StorageEngine = struct {
    allocator: std.mem.Allocator,
    vfs: VFS,
    data_dir: []const u8,
    index: BlockIndex,
    wal_file: ?vfs.VFile,
    initialized: bool,

    /// Initialize a new storage engine instance.
    pub fn init(allocator: std.mem.Allocator, filesystem: VFS, data_dir: []const u8) StorageEngine {
        return StorageEngine{
            .allocator = allocator,
            .vfs = filesystem,
            .data_dir = data_dir,
            .index = BlockIndex.init(allocator),
            .wal_file = null,
            .initialized = false,
        };
    }

    /// Clean up storage engine resources.
    pub fn deinit(self: *StorageEngine) void {
        if (self.wal_file) |*file| {
            file.close() catch {};
        }
        self.index.deinit();
        self.allocator.free(self.data_dir);
    }

    /// Initialize storage engine by creating necessary directories and files.
    pub fn initialize_storage(self: *StorageEngine) !void {
        assert(!self.initialized);
        if (self.initialized) return StorageError.AlreadyInitialized;

        // Create data directory structure
        if (!self.vfs.exists(self.data_dir)) {
            try self.vfs.mkdir(self.data_dir);
        }

        const wal_dir = try std.fmt.allocPrint(self.allocator, "{s}/wal", .{self.data_dir});
        defer self.allocator.free(wal_dir);
        if (!self.vfs.exists(wal_dir)) {
            try self.vfs.mkdir(wal_dir);
        }

        const sst_dir = try std.fmt.allocPrint(self.allocator, "{s}/sst", .{self.data_dir});
        defer self.allocator.free(sst_dir);
        if (!self.vfs.exists(sst_dir)) {
            try self.vfs.mkdir(sst_dir);
        }

        // Create initial WAL file
        const wal_path = try std.fmt.allocPrint(self.allocator, "{s}/wal/wal_0000.log", .{self.data_dir});
        defer self.allocator.free(wal_path);

        self.wal_file = try self.vfs.create(wal_path);

        // Create LOCK file to prevent multiple instances
        const lock_path = try std.fmt.allocPrint(self.allocator, "{s}/LOCK", .{self.data_dir});
        defer self.allocator.free(lock_path);

        var lock_file = try self.vfs.create(lock_path);
        defer lock_file.close() catch {};

        const process_id = 12345; // Placeholder PID for simulation
        const lock_content = try std.fmt.allocPrint(self.allocator, "PID:{}\n", .{process_id});
        defer self.allocator.free(lock_content);

        _ = try lock_file.write(lock_content);
        try lock_file.close();

        self.initialized = true;
    }

    /// Put a Context Block into storage.
    pub fn put_block(self: *StorageEngine, block: ContextBlock) !void {
        assert(self.initialized);
        if (!self.initialized) return StorageError.NotInitialized;

        // Validate block before storing
        try block.validate(self.allocator);

        // Create WAL entry
        const wal_entry = try WALEntry.create_put_block(block, self.allocator);
        defer wal_entry.deinit(self.allocator);

        // Write to WAL for durability
        try self.write_wal_entry(wal_entry);

        // Update in-memory index
        try self.index.put_block(block);
    }

    /// Get a Context Block by ID.
    pub fn get_block_by_id(self: *StorageEngine, block_id: BlockId) StorageError!*const ContextBlock {
        assert(self.initialized);
        if (!self.initialized) return StorageError.NotInitialized;

        return self.index.get_block(block_id) orelse StorageError.BlockNotFound;
    }

    /// Delete a Context Block by ID.
    pub fn delete_block(self: *StorageEngine, block_id: BlockId) !void {
        assert(self.initialized);
        if (!self.initialized) return StorageError.NotInitialized;

        // Create WAL entry
        const wal_entry = try WALEntry.create_delete_block(block_id, self.allocator);
        defer wal_entry.deinit(self.allocator);

        // Write to WAL for durability
        try self.write_wal_entry(wal_entry);

        // Remove from in-memory index
        self.index.remove_block(block_id);
    }

    /// Put a Graph Edge into storage.
    pub fn put_edge(self: *StorageEngine, edge: GraphEdge) !void {
        assert(self.initialized);
        if (!self.initialized) return StorageError.NotInitialized;

        // Create WAL entry
        const wal_entry = try WALEntry.create_put_edge(edge, self.allocator);
        defer wal_entry.deinit(self.allocator);

        // Write to WAL for durability
        try self.write_wal_entry(wal_entry);

        // TODO: Add edge to graph index when implemented
    }

    /// Get the current number of blocks in storage.
    pub fn block_count(self: *const StorageEngine) u32 {
        return self.index.block_count();
    }

    /// Flush WAL to disk.
    pub fn flush_wal(self: *StorageEngine) !void {
        if (self.wal_file) |*file| {
            try file.flush();
        }
    }

    /// Write a WAL entry to the current WAL file.
    fn write_wal_entry(self: *StorageEngine, entry: WALEntry) !void {
        assert(self.wal_file != null);
        if (self.wal_file == null) return StorageError.NotInitialized;

        const serialized_size = WALEntry.HEADER_SIZE + entry.payload.len;
        const buffer = try self.allocator.alloc(u8, serialized_size);
        defer self.allocator.free(buffer);

        const written = try entry.serialize(buffer);
        assert(written == serialized_size);

        _ = try self.wal_file.?.write(buffer);
    }

    /// Recover storage state from WAL files.
    pub fn recover_from_wal(self: *StorageEngine) !void {
        assert(self.initialized);
        if (!self.initialized) return StorageError.NotInitialized;

        const wal_dir = try std.fmt.allocPrint(self.allocator, "{s}/wal", .{self.data_dir});
        defer self.allocator.free(wal_dir);

        // TODO: Implement WAL recovery by reading all WAL files
        // For now, this is a placeholder that would:
        // 1. List all WAL files in chronological order
        // 2. Read each file entry by entry
        // 3. Apply each entry to rebuild the in-memory index
        // 4. Handle corruption by truncating at the first bad entry
    }
};

// Tests

test "WALEntry serialization roundtrip" {
    const allocator = std.testing.allocator;

    // Create test block
    const test_id = try BlockId.from_hex("0123456789abcdeffedcba9876543210");
    const test_block = ContextBlock{
        .id = test_id,
        .version = 1,
        .source_uri = "test://uri",
        .metadata_json = "{\"test\":true}",
        .content = "test content",
    };

    // Create WAL entry
    const wal_entry = try WALEntry.create_put_block(test_block, allocator);
    defer wal_entry.deinit(allocator);

    // Serialize
    const serialized_size = WALEntry.HEADER_SIZE + wal_entry.payload.len;
    const buffer = try allocator.alloc(u8, serialized_size);
    defer allocator.free(buffer);

    const written = try wal_entry.serialize(buffer);
    try std.testing.expectEqual(serialized_size, written);

    // Deserialize
    const deserialized = try WALEntry.deserialize(buffer, allocator);
    defer deserialized.deinit(allocator);

    // Verify
    try std.testing.expectEqual(wal_entry.checksum, deserialized.checksum);
    try std.testing.expectEqual(wal_entry.entry_type, deserialized.entry_type);
    try std.testing.expectEqualSlices(u8, wal_entry.payload, deserialized.payload);
}

test "BlockIndex basic operations" {
    const allocator = std.testing.allocator;

    var index = BlockIndex.init(allocator);
    defer index.deinit();

    // Test empty index
    try std.testing.expectEqual(@as(u32, 0), index.block_count());

    // Create test block
    const test_id = try BlockId.from_hex("0123456789abcdeffedcba9876543210");
    const test_block = ContextBlock{
        .id = test_id,
        .version = 1,
        .source_uri = "test://uri",
        .metadata_json = "{\"test\":true}",
        .content = "test content",
    };

    // Test put and get
    try index.put_block(test_block);
    try std.testing.expectEqual(@as(u32, 1), index.block_count());

    const retrieved = index.get_block(test_id);
    try std.testing.expect(retrieved != null);
    try std.testing.expect(retrieved.?.id.eql(test_id));
    try std.testing.expectEqual(@as(u64, 1), retrieved.?.version);
    try std.testing.expectEqualStrings("test://uri", retrieved.?.source_uri);

    // Test remove
    index.remove_block(test_id);
    try std.testing.expectEqual(@as(u32, 0), index.block_count());
    try std.testing.expect(index.get_block(test_id) == null);
}

test "StorageEngine basic operations" {
    const allocator = std.testing.allocator;
    const simulation_vfs = @import("simulation_vfs");

    // Create simulation VFS
    var sim_vfs = simulation_vfs.SimulationVFS.init(allocator);
    defer sim_vfs.deinit();

    const vfs_interface = sim_vfs.vfs();
    const data_dir = try allocator.dupe(u8, "test_data");

    var storage = StorageEngine.init(allocator, vfs_interface, data_dir);
    defer storage.deinit();

    // Initialize storage
    try storage.initialize_storage();

    // Create test block
    const test_id = try BlockId.from_hex("0123456789abcdeffedcba9876543210");
    const test_block = ContextBlock{
        .id = test_id,
        .version = 1,
        .source_uri = "test://uri",
        .metadata_json = "{\"test\":true}",
        .content = "test content",
    };

    // Test put and get
    try storage.put_block(test_block);
    try std.testing.expectEqual(@as(u32, 1), storage.block_count());

    const retrieved = try storage.get_block_by_id(test_id);
    try std.testing.expect(retrieved.id.eql(test_id));
    try std.testing.expectEqual(@as(u64, 1), retrieved.version);

    // Test delete
    try storage.delete_block(test_id);
    try std.testing.expectEqual(@as(u32, 0), storage.block_count());
    try std.testing.expectError(StorageError.BlockNotFound, storage.get_block_by_id(test_id));
}

test "StorageEngine graph edge operations" {
    const allocator = std.testing.allocator;
    const simulation_vfs = @import("simulation_vfs");

    var sim_vfs = simulation_vfs.SimulationVFS.init(allocator);
    defer sim_vfs.deinit();

    const vfs_interface = sim_vfs.vfs();
    const data_dir = try allocator.dupe(u8, "test_data");

    var storage = StorageEngine.init(allocator, vfs_interface, data_dir);
    defer storage.deinit();

    try storage.initialize_storage();

    // Create test edge
    const source_id = try BlockId.from_hex("0123456789abcdeffedcba9876543210");
    const target_id = try BlockId.from_hex("fedcba9876543210123456789abcdef0");
    const test_edge = GraphEdge{
        .source_id = source_id,
        .target_id = target_id,
        .edge_type = .imports,
    };

    // Test put edge (should not error)
    try storage.put_edge(test_edge);
}
