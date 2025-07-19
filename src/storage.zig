//! Storage engine for CortexDB Context Block persistence.
//!
//! Implements a Log-Structured Merge-Tree (LSMT) with Write-Ahead Logging
//! for durable, high-performance storage of Context Blocks and Graph Edges.
//! All I/O operations go through the VFS interface for deterministic testing.

const std = @import("std");
const assert = std.debug.assert;
const vfs = @import("vfs");
const context_block = @import("context_block");
const sstable = @import("sstable");
const buffer_pool = @import("buffer_pool");
const error_context = @import("error_context");
const concurrency = @import("concurrency");

const VFS = vfs.VFS;
const ContextBlock = context_block.ContextBlock;
const GraphEdge = context_block.GraphEdge;
const BlockId = context_block.BlockId;
const SSTable = sstable.SSTable;
const Compactor = sstable.Compactor;

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
pub const WALEntry = struct {
    checksum: u64,
    entry_type: WALEntryType,
    payload: []const u8,

    pub const HEADER_SIZE = 9; // 8 bytes checksum + 1 byte type

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
            return error_context.wal_error(
                StorageError.InvalidChecksum,
                error_context.WALContext{
                    .operation = "deserialize_wal_entry",
                    .checksum_expected = expected_checksum,
                    .checksum_actual = checksum,
                    .entry_type = @intFromEnum(entry_type),
                },
            );
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
    blocks: std.HashMap(
        BlockId,
        ContextBlock,
        BlockIdContext,
        std.hash_map.default_max_load_percentage,
    ),
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
            .blocks = std.HashMap(
                BlockId,
                ContextBlock,
                BlockIdContext,
                std.hash_map.default_max_load_percentage,
            ).init(allocator),
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

        // Clone block data for long-term storage in index
        // Note: Index storage uses heap allocation since it's long-lived
        // Buffer pools are for temporary hot-path allocations only
        const cloned_block = ContextBlock{
            .id = block.id,
            .version = block.version,
            .source_uri = try self.allocator.dupe(u8, block.source_uri),
            .metadata_json = try self.allocator.dupe(u8, block.metadata_json),
            .content = try self.allocator.dupe(u8, block.content),
        };
        try self.blocks.put(block.id, cloned_block);
    }

    pub fn find_block(self: *BlockIndex, block_id: BlockId) ?*const ContextBlock {
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
    sstables: std.ArrayList([]const u8), // Paths to SSTable files
    compactor: Compactor,
    next_sstable_id: u32,
    initialized: bool,
    buffer_pool: buffer_pool.BufferPool,

    /// Initialize a new storage engine instance.
    pub fn init(allocator: std.mem.Allocator, filesystem: VFS, data_dir: []const u8) StorageEngine {
        return StorageEngine{
            .allocator = allocator,
            .vfs = filesystem,
            .data_dir = data_dir,
            .index = BlockIndex.init(allocator),
            .wal_file = null,
            .sstables = std.ArrayList([]const u8).init(allocator),
            .compactor = Compactor.init(allocator, filesystem, data_dir),
            .next_sstable_id = 0,
            .initialized = false,
            .buffer_pool = buffer_pool.BufferPool.init(),
        };
    }

    /// Clean up storage engine resources.
    pub fn deinit(self: *StorageEngine) void {
        if (self.wal_file) |*file| {
            file.close() catch {};
        }
        self.index.deinit();

        // Free SSTable paths
        for (self.sstables.items) |path| {
            self.allocator.free(path);
        }
        self.sstables.deinit();

        self.allocator.free(self.data_dir);
    }

    /// Initialize storage engine by creating necessary directories and files.
    pub fn initialize_storage(self: *StorageEngine) !void {
        concurrency.assert_main_thread();
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

        // Create initial WAL file (will be wal_0000.log)
        const wal_path = try std.fmt.allocPrint(
            self.allocator,
            "{s}/wal_0000.log",
            .{wal_dir},
        );
        defer self.allocator.free(wal_path);

        // For initialization, always create a new WAL file
        // Existing WAL files will be recovered separately
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
        concurrency.assert_main_thread();
        assert(self.initialized);
        if (!self.initialized) return StorageError.NotInitialized;

        // Validate block before storing
        try block.validate(self.allocator);

        // Create WAL entry using buffer pool when possible for temporary serialization
        const wal_entry = try WALEntry.create_put_block(block, self.allocator);
        defer wal_entry.deinit(self.allocator);

        // Write to WAL for durability
        try self.write_wal_entry(wal_entry);

        // Update in-memory index
        try self.index.put_block(block);

        // Check if we need to flush MemTable to SSTable
        if (self.index.block_count() >= 1000) { // Flush when we have 1000+ blocks
            try self.flush_memtable_to_sstable();
        }
    }

    /// Find a Context Block by ID.
    pub fn find_block_by_id(
        self: *StorageEngine,
        block_id: BlockId,
    ) StorageError!*const ContextBlock {
        concurrency.assert_main_thread();
        assert(self.initialized);
        if (!self.initialized) return StorageError.NotInitialized;

        // First check in-memory index (most recent data)
        if (self.index.find_block(block_id)) |block| {
            return block;
        }

        // Search SSTables (older data)
        for (self.sstables.items) |sstable_path| {
            const path_copy = try self.allocator.dupe(u8, sstable_path);
            var table = SSTable.init(self.allocator, self.vfs, path_copy);
            defer table.deinit();

            table.read_index() catch continue; // Skip corrupted SSTables

            if (table.find_block(block_id) catch null) |block| {
                // Transfer ownership to index for future fast access
                self.index.put_block(block) catch {}; // Ignore errors, it's an optimization
                return self.index.find_block(block_id) orelse return error_context.storage_error(
                    StorageError.BlockNotFound,
                    error_context.block_context("find_block_after_sstable_transfer", block_id),
                );
            }
        }

        return error_context.storage_error(
            StorageError.BlockNotFound,
            error_context.block_context("find_block_exhaustive_search", block_id),
        );
    }

    /// Delete a Context Block by ID.
    pub fn delete_block(self: *StorageEngine, block_id: BlockId) !void {
        concurrency.assert_main_thread();
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
        concurrency.assert_main_thread();
        assert(self.initialized);
        if (!self.initialized) return StorageError.NotInitialized;

        // Create WAL entry
        const wal_entry = try WALEntry.create_put_edge(edge, self.allocator);
        defer wal_entry.deinit(self.allocator);

        // Write to WAL for durability
        try self.write_wal_entry(wal_entry);

        // TODO Add edge to graph index when implemented
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

    /// Flush in-memory blocks to an SSTable.
    pub fn flush_memtable_to_sstable(self: *StorageEngine) !void {
        concurrency.assert_main_thread();
        assert(self.initialized);
        if (!self.initialized) return StorageError.NotInitialized;

        if (self.index.block_count() == 0) {
            return; // Nothing to flush
        }

        // Create SSTable file path
        const sstable_path = try std.fmt.allocPrint(
            self.allocator,
            "{s}/sst/sstable_{d:0>4}.sst",
            .{ self.data_dir, self.next_sstable_id },
        );

        // Collect all blocks from in-memory index
        var blocks_to_flush = std.ArrayList(ContextBlock).init(self.allocator);
        defer blocks_to_flush.deinit();

        var iterator = self.index.blocks.iterator();
        while (iterator.next()) |entry| {
            try blocks_to_flush.append(entry.value_ptr.*);
        }

        if (blocks_to_flush.items.len == 0) {
            self.allocator.free(sstable_path);
            return;
        }

        // Create and write SSTable
        var new_sstable = SSTable.init(self.allocator, self.vfs, sstable_path);
        defer new_sstable.deinit();

        try new_sstable.write_blocks(blocks_to_flush.items);

        // Add to SSTables list
        try self.sstables.append(try self.allocator.dupe(u8, sstable_path));
        self.next_sstable_id += 1;

        // Clear the in-memory index (blocks are now persisted in SSTable)
        self.index.deinit();
        self.index = BlockIndex.init(self.allocator);

        std.log.info(
            "Flushed {} blocks to SSTable: {s}",
            .{ blocks_to_flush.items.len, sstable_path },
        );

        // Trigger compaction if we have too many SSTables
        if (self.sstables.items.len >= 4) {
            try self.compact_sstables();
        }
    }

    /// Compact multiple SSTables into fewer, larger SSTables.
    fn compact_sstables(self: *StorageEngine) !void {
        if (self.sstables.items.len < 2) {
            return; // Need at least 2 SSTables to compact
        }

        std.log.info("Starting SSTable compaction of {} tables", .{self.sstables.items.len});

        // Simple compaction strategy: compact all SSTables into one
        const output_path = try std.fmt.allocPrint(
            self.allocator,
            "{s}/sst/compacted_{d:0>4}.sst",
            .{ self.data_dir, self.next_sstable_id },
        );

        try self.compactor.compact_sstables(self.sstables.items, output_path);

        // Update SSTables list (remove old, add new)
        for (self.sstables.items) |path| {
            self.allocator.free(path);
        }
        self.sstables.clearAndFree();

        try self.sstables.append(try self.allocator.dupe(u8, output_path));
        self.next_sstable_id += 1;

        std.log.info("Compaction completed: {s}", .{output_path});
    }

    /// Write a WAL entry to the current WAL file.
    fn write_wal_entry(self: *StorageEngine, entry: WALEntry) !void {
        assert(self.wal_file != null);
        if (self.wal_file == null) return StorageError.NotInitialized;

        const serialized_size = WALEntry.HEADER_SIZE + entry.payload.len;

        // Use buffer pool for temporary serialization buffer (hot path optimization)
        if (self.buffer_pool.acquire_buffer(serialized_size)) |pooled_buffer| {
            defer pooled_buffer.release();
            const buffer_slice = pooled_buffer.slice(serialized_size);
            const written = try entry.serialize(buffer_slice);
            assert(written == serialized_size);

            _ = try self.wal_file.?.write(buffer_slice);
        } else {
            // Fall back to heap allocation for very large entries
            const buffer = try self.allocator.alloc(u8, serialized_size);
            defer self.allocator.free(buffer);

            const written = try entry.serialize(buffer);
            assert(written == serialized_size);

            _ = try self.wal_file.?.write(buffer);
        }
    }

    /// Recover storage state from WAL files.
    pub fn recover_from_wal(self: *StorageEngine) !void {
        concurrency.assert_main_thread();
        assert(self.initialized);
        if (!self.initialized) return StorageError.NotInitialized;

        const wal_dir = try std.fmt.allocPrint(self.allocator, "{s}/wal", .{self.data_dir});
        defer self.allocator.free(wal_dir);

        // Check if WAL directory exists
        if (!self.vfs.exists(wal_dir)) {
            return; // No WAL files to recover
        }

        // Get list of WAL files
        const wal_files = try self.list_wal_files(wal_dir);
        defer {
            for (wal_files) |file_name| {
                self.allocator.free(file_name);
            }
            self.allocator.free(wal_files);
        }

        var entries_recovered: u32 = 0;
        var files_processed: u32 = 0;

        // Process each WAL file in chronological order
        for (wal_files) |file_name| {
            const file_path = try std.fmt.allocPrint(
                self.allocator,
                "{s}/{s}",
                .{ wal_dir, file_name },
            );
            defer self.allocator.free(file_path);

            const file_entries = self.recover_from_wal_file(file_path) catch |err| switch (err) {
                StorageError.InvalidChecksum, StorageError.InvalidWALEntryType => {
                    // Corruption detected - stop recovery at this point
                    std.log.warn("WAL corruption detected in {s}, stopping recovery", .{file_path});
                    break;
                },
                else => return err,
            };

            entries_recovered += file_entries;
            files_processed += 1;
        }

        std.log.info(
            "WAL recovery completed: {} files processed, {} entries recovered",
            .{ files_processed, entries_recovered },
        );
    }

    /// List WAL files in chronological order.
    fn list_wal_files(self: *StorageEngine, wal_dir: []const u8) ![][]const u8 {
        // Use VFS list_dir to get all files in the WAL directory
        const all_files = self.vfs.list_dir(wal_dir, self.allocator) catch |err| switch (err) {
            error.FileNotFound => {
                // Directory doesn't exist, return empty list
                return try self.allocator.alloc([]const u8, 0);
            },
            else => return err,
        };
        defer {
            for (all_files) |file_name| {
                self.allocator.free(file_name);
            }
            self.allocator.free(all_files);
        }

        var wal_files = std.ArrayList([]const u8).init(self.allocator);
        defer wal_files.deinit();

        // Filter for WAL files (wal_XXXX.log pattern)
        for (all_files) |file_name| {
            if (std.mem.startsWith(u8, file_name, "wal_") and
                std.mem.endsWith(u8, file_name, ".log"))
            {
                try wal_files.append(try self.allocator.dupe(u8, file_name));
            }
        }

        // Sort WAL files in chronological order (by filename)
        const wal_file_slice = try wal_files.toOwnedSlice();
        std.mem.sort([]const u8, wal_file_slice, {}, struct {
            fn less_than(context: void, a: []const u8, b: []const u8) bool {
                _ = context;
                return std.mem.order(u8, a, b) == .lt;
            }
        }.less_than);

        return wal_file_slice;
    }

    /// Recover from a single WAL file.
    fn recover_from_wal_file(self: *StorageEngine, file_path: []const u8) !u32 {
        var file = self.vfs.open(file_path, .read) catch |err| switch (err) {
            error.FileNotFound => return 0, // File doesn't exist, nothing to recover
            else => return err,
        };
        defer file.close() catch {};

        const file_size = try file.file_size();
        if (file_size == 0) return 0; // Empty file

        // Use buffer pool for file reading when possible (hot path during recovery)
        const file_content = blk: {
            if (self.buffer_pool.acquire_buffer(file_size)) |pooled_buffer| {
                break :blk pooled_buffer.slice(file_size);
            } else {
                break :blk self.allocator.alloc(u8, file_size) catch |err| {
                    return error_context.storage_error(
                        err,
                        error_context.file_context("allocate_wal_recovery_buffer", file_path),
                    );
                };
            }
        };
        defer {
            // Only free if it came from allocator, not pool
            if (file_size > buffer_pool.MAX_BUFFER_SIZE) {
                self.allocator.free(file_content);
            }
        }

        _ = try file.read(file_content);

        var offset: usize = 0;
        var entries_recovered: u32 = 0;

        // Create arena for WAL entry processing to avoid memory leaks
        var arena = std.heap.ArenaAllocator.init(self.allocator);
        defer arena.deinit();
        const temp_allocator = arena.allocator();

        while (offset < file_content.len) {
            // Check if we have enough bytes for a header
            if (offset + WALEntry.HEADER_SIZE > file_content.len) {
                // Incomplete header - likely corruption or partial write
                break;
            }

            // Parse entry header to determine payload size
            const entry_type_byte = file_content[offset + 8];
            const entry_type = WALEntryType.from_u8(entry_type_byte) catch {
                break;
            };

            // Determine payload size based on entry type
            const payload_size = switch (entry_type) {
                .delete_block => 16, // BlockId size
                .put_edge => 40, // GraphEdge.SERIALIZED_SIZE
                .put_block => blk: {
                    // For put_block, we need to read the block header to get size
                    const payload_start = offset + WALEntry.HEADER_SIZE;
                    if (payload_start + ContextBlock.MIN_SERIALIZED_SIZE > file_content.len) {
                        break :blk 0; // Will cause break below
                    }

                    const block_content = file_content[payload_start..];
                    const computed_size = ContextBlock.compute_serialized_size_from_buffer(
                        block_content,
                    ) catch {
                        break :blk 0;
                    };
                    break :blk computed_size;
                },
            };

            if (payload_size == 0) break; // Error occurred above

            const total_entry_size = WALEntry.HEADER_SIZE + payload_size;
            if (offset + total_entry_size > file_content.len) {
                break;
            }

            // Deserialize and apply the entry using temporary allocator
            const entry_buffer = file_content[offset .. offset + total_entry_size];
            const entry = WALEntry.deserialize(entry_buffer, temp_allocator) catch |err|
                switch (err) {
                    StorageError.InvalidChecksum => break,
                    else => return err,
                };

            // Apply the entry to rebuild state
            try self.apply_wal_entry(entry);

            offset += total_entry_size;
            entries_recovered += 1;
        }

        return entries_recovered;
    }

    /// Apply a single WAL entry to rebuild storage state.
    fn apply_wal_entry(self: *StorageEngine, entry: WALEntry) !void {
        switch (entry.entry_type) {
            .put_block => {
                // Create temporary arena for this operation
                var arena = std.heap.ArenaAllocator.init(self.allocator);
                defer arena.deinit();
                const temp_allocator = arena.allocator();

                const block = try ContextBlock.deserialize(entry.payload, temp_allocator);

                // Add block to in-memory index (put_block will clone the strings)
                try self.index.put_block(block);
                // Arena will automatically free the temporary block
            },
            .delete_block => {
                if (entry.payload.len != 16) return error.InvalidPayloadSize;

                const block_id = BlockId{ .bytes = entry.payload[0..16].* };
                self.index.remove_block(block_id);
            },
            .put_edge => {
                if (entry.payload.len != 40) return error.InvalidPayloadSize;

                const edge = try GraphEdge.deserialize(entry.payload);
                // Note: Graph edge storage would be implemented when graph indexing is added
                _ = edge; // Suppress unused variable warning for now
            },
        }
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

    const retrieved = index.find_block(test_id);
    try std.testing.expect(retrieved != null);
    try std.testing.expect(retrieved.?.id.eql(test_id));
    try std.testing.expectEqual(@as(u64, 1), retrieved.?.version);

    // Test remove
    index.remove_block(test_id);
    try std.testing.expectEqual(@as(u32, 0), index.block_count());
    try std.testing.expect(index.find_block(test_id) == null);
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

    const retrieved = try storage.find_block_by_id(test_id);
    try std.testing.expect(retrieved.id.eql(test_id));
    try std.testing.expectEqual(@as(u64, 1), retrieved.version);

    // Test delete
    try storage.delete_block(test_id);
    try std.testing.expectEqual(@as(u32, 0), storage.block_count());
    try std.testing.expectError(StorageError.BlockNotFound, storage.find_block_by_id(test_id));
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

test "StorageEngine buffer pool integration" {
    const allocator = std.testing.allocator;
    const simulation_vfs = @import("simulation_vfs");

    var sim_vfs = simulation_vfs.SimulationVFS.init(allocator);
    defer sim_vfs.deinit();

    const vfs_interface = sim_vfs.vfs();
    const data_dir = try allocator.dupe(u8, "buffer_pool_test");

    var storage = StorageEngine.init(allocator, vfs_interface, data_dir);
    defer storage.deinit();

    try storage.initialize_storage();

    // Get initial buffer pool statistics
    const initial_stats = storage.buffer_pool.statistics();
    try std.testing.expectEqual(@as(u64, 0), initial_stats.allocations);

    // Create test blocks to trigger buffer pool usage
    const test_blocks = [_]ContextBlock{
        ContextBlock{
            .id = try BlockId.from_hex("1111111111111111111111111111111"),
            .version = 1,
            .source_uri = "test://small_block",
            .metadata_json = "{\"type\":\"small\"}",
            .content = "small content", // Small enough for buffer pool
        },
        ContextBlock{
            .id = try BlockId.from_hex("2222222222222222222222222222222"),
            .version = 1,
            .source_uri = "test://medium_block",
            .metadata_json = "{\"type\":\"medium\",\"size\":\"larger\"}",
            .content = "medium content that is somewhat larger",
        },
    };

    // Put blocks - this should use buffer pool for WAL serialization
    for (test_blocks) |block| {
        try storage.put_block(block);
    }

    // Verify buffer pool was used
    const after_put_stats = storage.buffer_pool.statistics();
    try std.testing.expect(after_put_stats.allocations > initial_stats.allocations);
    try std.testing.expect(after_put_stats.pool_hits > 0);

    // Create edges to test edge operations with buffer pool
    const test_edge = GraphEdge{
        .source_id = test_blocks[0].id,
        .target_id = test_blocks[1].id,
        .edge_type = .references,
    };

    try storage.put_edge(test_edge);

    // Verify more buffer pool usage
    const after_edge_stats = storage.buffer_pool.statistics();
    try std.testing.expect(after_edge_stats.allocations > after_put_stats.allocations);

    // Test block retrieval
    const retrieved = try storage.find_block_by_id(test_blocks[0].id);
    try std.testing.expect(retrieved.id.eql(test_blocks[0].id));
    try std.testing.expectEqualStrings("small content", retrieved.content);

    // Test deletion with buffer pool
    try storage.delete_block(test_blocks[1].id);

    // Verify buffer pool statistics show usage but no excessive misses
    const final_stats = storage.buffer_pool.statistics();
    try std.testing.expect(final_stats.hit_rate > 0.5); // At least 50% hit rate
    try std.testing.expect(final_stats.deallocations > 0);

    std.log.info("Buffer pool stats: allocations={}, hits={}, misses={}, hit_rate={d:.2}", .{
        final_stats.allocations,
        final_stats.pool_hits,
        final_stats.pool_misses,
        final_stats.hit_rate,
    });
}

test "StorageEngine buffer pool fallback behavior" {
    const allocator = std.testing.allocator;
    const simulation_vfs = @import("simulation_vfs");

    var sim_vfs = simulation_vfs.SimulationVFS.init(allocator);
    defer sim_vfs.deinit();

    const vfs_interface = sim_vfs.vfs();
    const data_dir = try allocator.dupe(u8, "fallback_test");

    var storage = StorageEngine.init(allocator, vfs_interface, data_dir);
    defer storage.deinit();

    try storage.initialize_storage();

    // Create a block larger than buffer pool can handle
    const large_content = try allocator.alloc(u8, buffer_pool.MAX_BUFFER_SIZE + 1000);
    defer allocator.free(large_content);
    @memset(large_content, 'X'); // Fill with test data

    const large_block = ContextBlock{
        .id = try BlockId.from_hex("9999999999999999999999999999999"),
        .version = 1,
        .source_uri = "test://large_block",
        .metadata_json = "{\"type\":\"oversized\"}",
        .content = large_content,
    };

    // This should still work, falling back to heap allocation
    try storage.put_block(large_block);

    // Verify the block was stored correctly
    const retrieved = try storage.find_block_by_id(large_block.id);
    try std.testing.expect(retrieved.id.eql(large_block.id));
    try std.testing.expectEqual(large_content.len, retrieved.content.len);

    // Check that fallback allocations were used
    const stats = storage.buffer_pool.statistics();
    try std.testing.expect(stats.fallback_allocations > 0 or stats.pool_misses > 0);

    std.log.info("Fallback test stats: fallback_allocations={}, pool_misses={}", .{
        stats.fallback_allocations,
        stats.pool_misses,
    });
}
