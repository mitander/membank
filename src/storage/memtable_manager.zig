//! Manages the complete in-memory write buffer (memtable) for blocks and edges.
//!
//! Encapsulates both BlockIndex and GraphEdgeIndex to provide a single,
//! cohesive interface for all in-memory state management. Follows the
//! arena-per-subsystem pattern for O(1) bulk cleanup during memtable flushes.
//! This is a state-oriented subsystem that owns the complete in-memory view
//! of the database, as opposed to scattered coordination logic.
//!
//! **Architectural Responsibility**: Owns WAL for durability guarantees.
//! All mutations go through WAL-first pattern before updating in-memory state.

const std = @import("std");
const assert = @import("../core/assert.zig").assert;
const assert_fmt = @import("../core/assert.zig").assert_fmt;
const context_block = @import("../core/types.zig");
const concurrency = @import("../core/concurrency.zig");
const vfs = @import("../core/vfs.zig");

const BlockIndex = @import("block_index.zig").BlockIndex;
const GraphEdgeIndex = @import("graph_edge_index.zig").GraphEdgeIndex;
const wal = @import("wal.zig");

const ContextBlock = context_block.ContextBlock;
const GraphEdge = context_block.GraphEdge;
const BlockId = context_block.BlockId;
const VFS = vfs.VFS;
const WAL = wal.WAL;
const WALEntry = wal.WALEntry;

/// Iterator for all blocks in the memtable, used during SSTable flush operations.
/// Provides ordered iteration over all blocks to enable deterministic SSTable creation.
pub const BlockIterator = struct {
    block_index: *const BlockIndex,
    hash_map_iterator: std.HashMap(BlockId, ContextBlock, BlockIndex.BlockIdContext, std.hash_map.default_max_load_percentage).Iterator,

    pub fn next(self: *BlockIterator) ?ContextBlock {
        if (self.hash_map_iterator.next()) |entry| {
            return entry.value_ptr.*;
        }
        return null;
    }
};

/// Manages the complete in-memory write buffer (memtable) state.
/// Encapsulates both block and edge indexes to provide single ownership
/// boundary for all in-memory data. Uses coordinated arena management
/// for atomic O(1) cleanup during memtable flushes.
/// **Owns WAL for durability**: All mutations go WAL-first before memtable update.
pub const MemtableManager = struct {
    backing_allocator: std.mem.Allocator,
    vfs: VFS,
    data_dir: []const u8,
    block_index: BlockIndex,
    graph_index: GraphEdgeIndex,
    wal: WAL,
    memtable_max_size: u64,

    /// Phase 1: Create the memtable manager without I/O operations.
    /// Initializes both block and edge indexes with their dedicated arenas.
    /// Creates WAL instance but does not perform I/O until startup() is called.
    /// Follows CortexDB two-phase initialization pattern for testability.
    pub fn init(
        allocator: std.mem.Allocator,
        filesystem: VFS,
        data_dir: []const u8,
        memtable_max_size: u64,
    ) !MemtableManager {
        const owned_data_dir = try allocator.dupe(u8, data_dir);

        const wal_dir = try std.fmt.allocPrint(allocator, "{s}/wal", .{owned_data_dir});
        defer allocator.free(wal_dir);

        return MemtableManager{
            .backing_allocator = allocator,
            .vfs = filesystem,
            .data_dir = owned_data_dir,
            .block_index = BlockIndex.init(allocator),
            .graph_index = GraphEdgeIndex.init(allocator),
            .wal = try WAL.init(allocator, filesystem, wal_dir),
            .memtable_max_size = memtable_max_size,
        };
    }

    /// Phase 2: Perform I/O operations to start up WAL for durability.
    /// Creates WAL directory structure and prepares for write operations.
    /// Must be called after init() and before any write operations.
    pub fn startup(self: *MemtableManager) !void {
        concurrency.assert_main_thread();

        try self.wal.startup();
    }

    /// Clean up all memtable resources including arena-allocated memory.
    /// Must be called to prevent memory leaks. Coordinates cleanup of
    /// both block and edge indexes atomically plus WAL cleanup.
    pub fn deinit(self: *MemtableManager) void {
        concurrency.assert_main_thread();

        self.wal.deinit();
        self.block_index.deinit();
        self.graph_index.deinit();
        self.backing_allocator.free(self.data_dir);
    }

    /// Add a context block to the in-memory memtable with full durability guarantees.
    /// WAL-first design ensures durability before in-memory state update.
    /// This is the primary method for durable block storage operations.
    pub fn put_block_durable(self: *MemtableManager, block: ContextBlock) !void {
        concurrency.assert_main_thread();

        const wal_entry = try WALEntry.create_put_block(block, self.backing_allocator);
        defer wal_entry.deinit(self.backing_allocator);
        try self.wal.write_entry(wal_entry);

        try self.block_index.put_block(block);
    }

    /// Add a context block to the in-memory memtable without WAL durability.
    /// Used for WAL recovery operations where durability is already guaranteed.
    /// For regular operations, use put_block_durable() instead.
    pub fn put_block(self: *MemtableManager, block: ContextBlock) !void {
        concurrency.assert_main_thread();

        try self.block_index.put_block(block);
    }

    /// Remove a block from the memtable with full durability guarantees.
    /// WAL-first design ensures delete operation is recorded before state update.
    /// This is the primary method for durable block deletion operations.
    pub fn delete_block_durable(self: *MemtableManager, block_id: BlockId) !void {
        concurrency.assert_main_thread();

        const wal_entry = try WALEntry.create_delete_block(block_id, self.backing_allocator);
        defer wal_entry.deinit(self.backing_allocator);
        try self.wal.write_entry(wal_entry);

        self.block_index.remove_block(block_id);
        self.graph_index.remove_block_edges(block_id);
    }

    /// Remove a block from the memtable by ID without WAL durability.
    /// Also removes all associated graph edges to maintain consistency.
    /// Used for WAL recovery operations where durability is already guaranteed.
    /// For regular operations, use delete_block_durable() instead.
    pub fn delete_block(self: *MemtableManager, block_id: BlockId) void {
        concurrency.assert_main_thread();

        self.block_index.remove_block(block_id);
        self.graph_index.remove_block_edges(block_id);
    }

    /// Add a graph edge with full durability guarantees.
    /// WAL-first design ensures edge operation is recorded before state update.
    /// This is the primary method for durable edge storage operations.
    pub fn put_edge_durable(self: *MemtableManager, edge: GraphEdge) !void {
        concurrency.assert_main_thread();

        const wal_entry = try WALEntry.create_put_edge(edge, self.backing_allocator);
        defer wal_entry.deinit(self.backing_allocator);
        try self.wal.write_entry(wal_entry);

        try self.graph_index.put_edge(edge);
    }

    /// Add a graph edge to the in-memory edge index without WAL durability.
    /// Maintains bidirectional indexes for efficient traversal in both directions.
    /// Used for WAL recovery operations where durability is already guaranteed.
    /// For regular operations, use put_edge_durable() instead.
    pub fn put_edge(self: *MemtableManager, edge: GraphEdge) !void {
        concurrency.assert_main_thread();

        try self.graph_index.put_edge(edge);
    }

    /// Find a block in the in-memory memtable by ID.
    /// Returns a pointer to the block if found, null otherwise.
    /// Used by the storage engine for LSM-tree read path (memtable first).
    pub fn find_block_in_memtable(self: *const MemtableManager, id: BlockId) ?*const ContextBlock {
        return self.block_index.find_block(id);
    }

    /// Get total memory usage of all data stored in the memtable.
    /// Used by storage engine to determine when memtable flush is needed.
    /// Accounts for string content only, not HashMap overhead.
    pub fn memory_usage(self: *const MemtableManager) u64 {
        return self.block_index.memory_usage();
    }

    /// Encapsulates flush decision within memtable ownership boundary.
    /// Prevents StorageEngine from needing knowledge of internal memory thresholds,
    /// maintaining clear separation between coordination and state management.
    pub fn should_flush(self: *const MemtableManager) bool {
        return self.memory_usage() >= self.memtable_max_size;
    }

    /// Atomically clear all in-memory data with O(1) arena reset.
    /// Used after successful memtable flush to SSTable. Resets both
    /// block and edge indexes while retaining HashMap capacity for performance.
    pub fn clear(self: *MemtableManager) void {
        concurrency.assert_main_thread();

        self.block_index.clear();
        self.graph_index.clear();
    }

    /// Create an iterator over all blocks for SSTable flush operations.
    /// Provides deterministic iteration order for consistent SSTable creation.
    /// Iterator remains valid until the next mutation operation.
    pub fn iterator(self: *const MemtableManager) BlockIterator {
        return BlockIterator{
            .block_index = &self.block_index,
            .hash_map_iterator = self.block_index.blocks.iterator(),
        };
    }

    /// Get raw HashMap iterator for backward compatibility with storage engine.
    /// Used by StorageEngine.iterate_all_blocks for mixed memtable+SSTable iteration.
    pub fn raw_iterator(self: *const MemtableManager) std.HashMap(BlockId, ContextBlock, BlockIndex.BlockIdContext, std.hash_map.default_max_load_percentage).Iterator {
        return self.block_index.blocks.iterator();
    }

    /// Get count of blocks currently in the memtable.
    /// Used for metrics and debugging. O(1) operation.
    pub fn block_count(self: *const MemtableManager) u32 {
        return @intCast(self.block_index.blocks.count());
    }

    /// Find all outgoing edges from a source block.
    /// Returns empty slice if no edges found. Used for graph traversal queries.
    pub fn find_outgoing_edges(self: *const MemtableManager, source_id: BlockId) []const GraphEdge {
        return self.graph_index.find_outgoing_edges(source_id) orelse &[_]GraphEdge{};
    }

    /// Find all incoming edges to a target block.
    /// Returns empty slice if no edges found. Used for reverse graph traversal.
    pub fn find_incoming_edges(self: *const MemtableManager, target_id: BlockId) []const GraphEdge {
        return self.graph_index.find_incoming_edges(target_id) orelse &[_]GraphEdge{};
    }

    /// Get edge count in graph index for metrics.
    /// Exposes graph index count through the memtable manager interface.
    pub fn edge_count(self: *const MemtableManager) u32 {
        return self.graph_index.edge_count();
    }

    /// Recover memtable state from WAL files.
    /// Replays all committed operations to reconstruct consistent state.
    /// Uses WAL module's streaming recovery for memory efficiency.
    pub fn recover_from_wal(self: *MemtableManager) !void {
        concurrency.assert_main_thread();

        const RecoveryContext = struct {
            memtable: *MemtableManager,
        };

        const recovery_callback = struct {
            fn apply(entry: WALEntry, context: *anyopaque) !void {
                const ctx: *RecoveryContext = @ptrCast(@alignCast(context));
                try ctx.memtable.apply_wal_entry(entry);
            }
        }.apply;

        var recovery_context = RecoveryContext{ .memtable = self };

        self.wal.recover_entries(recovery_callback, &recovery_context) catch |err| switch (err) {
            wal.WALError.FileNotFound => {
                return;
            },
            else => return err,
        };
    }

    /// Clean up old WAL segments after successful memtable flush.
    /// Delegates to WAL module for actual cleanup operations.
    pub fn cleanup_old_wal_segments(self: *MemtableManager) !void {
        concurrency.assert_main_thread();

        try self.wal.cleanup_old_segments();
    }

    /// Orchestrates atomic transition from write-optimized to read-optimized storage.
    /// Maintains LSM-tree performance characteristics by ensuring memtable state
    /// remains consistent throughout the flush operation. Prevents partial flushes
    /// that could compromise durability guarantees or create inconsistent views.
    pub fn flush_to_sstable(self: *MemtableManager, sstable_manager: anytype) !void {
        concurrency.assert_main_thread();

        if (self.block_count() == 0) return;

        // Snapshot current state to ensure atomic flush semantics
        var blocks = std.ArrayList(ContextBlock).init(self.backing_allocator);
        defer blocks.deinit();

        var block_iterator = self.iterator();
        while (block_iterator.next()) |block| {
            try blocks.append(block);
        }

        // Maintain responsibility boundaries - SSTable creation is not memtable concern
        try sstable_manager.create_new_sstable(blocks.items);

        // Only clear after successful persistence to maintain durability guarantees
        self.clear();

        // WAL entries now redundant since data persisted to durable SSTable storage
        try self.cleanup_old_wal_segments();
    }

    /// Apply a WAL entry during recovery to rebuild memtable state.
    /// Uses non-durable methods since WAL durability is already guaranteed.
    fn apply_wal_entry(self: *MemtableManager, entry: WALEntry) !void {
        switch (entry.entry_type) {
            .put_block => {
                var temp_arena = std.heap.ArenaAllocator.init(self.backing_allocator);
                defer temp_arena.deinit();
                const temp_allocator = temp_arena.allocator();

                const block = try entry.extract_block(temp_allocator);
                try self.put_block(block); // Non-durable version for recovery
            },
            .delete_block => {
                const block_id = try entry.extract_block_id();
                self.delete_block(block_id); // Non-durable version for recovery
            },
            .put_edge => {
                const edge = try entry.extract_edge();
                try self.put_edge(edge); // Non-durable version for recovery
            },
        }
    }
};
