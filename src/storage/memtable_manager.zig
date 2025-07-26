//! Manages the complete in-memory write buffer (memtable) for blocks and edges.
//!
//! Encapsulates both BlockIndex and GraphEdgeIndex to provide a single,
//! cohesive interface for all in-memory state management. Follows the
//! arena-per-subsystem pattern for O(1) bulk cleanup during memtable flushes.
//! This is a state-oriented subsystem that owns the complete in-memory view
//! of the database, as opposed to scattered coordination logic.

const std = @import("std");
const assert = @import("../core/assert.zig").assert;
const assert_fmt = @import("../core/assert.zig").assert_fmt;
const context_block = @import("../core/types.zig");
const concurrency = @import("../core/concurrency.zig");

const BlockIndex = @import("block_index.zig").BlockIndex;
const GraphEdgeIndex = @import("graph_edge_index.zig").GraphEdgeIndex;

const ContextBlock = context_block.ContextBlock;
const GraphEdge = context_block.GraphEdge;
const BlockId = context_block.BlockId;

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
pub const MemtableManager = struct {
    backing_allocator: std.mem.Allocator,
    block_index: BlockIndex,
    graph_index: GraphEdgeIndex,

    /// Phase 1: Create the memtable manager without I/O operations.
    /// Initializes both block and edge indexes with their dedicated arenas.
    /// Follows CortexDB two-phase initialization pattern for testability.
    pub fn init(allocator: std.mem.Allocator) MemtableManager {
        return MemtableManager{
            .backing_allocator = allocator,
            .block_index = BlockIndex.init(allocator),
            .graph_index = GraphEdgeIndex.init(allocator),
        };
    }

    /// Clean up all memtable resources including arena-allocated memory.
    /// Must be called to prevent memory leaks. Coordinates cleanup of
    /// both block and edge indexes atomically.
    pub fn deinit(self: *MemtableManager) void {
        concurrency.assert_main_thread();

        self.block_index.deinit();
        self.graph_index.deinit();
    }

    /// Add a context block to the in-memory memtable.
    /// Clones all string data into the arena for ownership isolation.
    /// Updates memory accounting for accurate flush threshold calculations.
    pub fn put_block(self: *MemtableManager, block: ContextBlock) !void {
        concurrency.assert_main_thread();

        try self.block_index.put_block(block);
    }

    /// Remove a block from the memtable by ID.
    /// Also removes all associated graph edges to maintain consistency.
    /// Used for delete operations and during WAL recovery.
    pub fn delete_block(self: *MemtableManager, block_id: BlockId) void {
        concurrency.assert_main_thread();

        self.block_index.remove_block(block_id);
        self.graph_index.remove_block_edges(block_id);
    }

    /// Add a graph edge to the in-memory edge index.
    /// Maintains bidirectional indexes for efficient traversal in both directions.
    /// Clones edge data into the arena for ownership isolation.
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
};
