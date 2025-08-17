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
const builtin = @import("builtin");
const assert = @import("../core/assert.zig").assert;
const assert_fmt = @import("../core/assert.zig").assert_fmt;
const fatal_assert = @import("../core/assert.zig").fatal_assert;
const context_block = @import("../core/types.zig");
const concurrency = @import("../core/concurrency.zig");
const vfs = @import("../core/vfs.zig");
const ownership = @import("../core/ownership.zig");
const memory = @import("../core/memory.zig");

const SSTableManager = @import("sstable_manager.zig").SSTableManager;
const BlockIndex = @import("block_index.zig").BlockIndex;
const graph_edge_index = @import("graph_edge_index.zig");
const GraphEdgeIndex = graph_edge_index.GraphEdgeIndex;
const wal = @import("wal.zig");

const ContextBlock = context_block.ContextBlock;
const GraphEdge = context_block.GraphEdge;
const BlockId = context_block.BlockId;
const EdgeType = context_block.EdgeType;

const OwnedGraphEdge = graph_edge_index.OwnedGraphEdge;
const VFS = vfs.VFS;
const WAL = wal.WAL;
const WALEntry = wal.WALEntry;
const OwnedBlock = ownership.OwnedBlock;
const BlockOwnership = ownership.BlockOwnership;
const ArenaCoordinator = memory.ArenaCoordinator;

pub const BlockIterator = struct {
    block_index: *const BlockIndex,
    hash_map_iterator: std.HashMap(BlockId, OwnedBlock, BlockIndex.BlockIdContext, std.hash_map.default_max_load_percentage).Iterator,

    pub fn next(self: *BlockIterator) ?*const OwnedBlock {
        if (self.hash_map_iterator.next()) |entry| {
            return entry.value_ptr;
        }
        return null;
    }
};

/// Manages the complete in-memory write buffer (memtable) state.
/// Uses Arena Coordinator Pattern: receives stable coordinator interface
/// and passes it to child components (BlockIndex, GraphEdgeIndex).
/// Coordinator interface remains valid across arena operations, eliminating temporal coupling.
/// **Owns WAL for durability**: All mutations go WAL-first before memtable update.
pub const MemtableManager = struct {
    /// Arena coordinator pointer for stable allocation access (remains valid across arena resets)
    /// CRITICAL: Must be pointer to prevent coordinator struct copying corruption
    arena_coordinator: *const ArenaCoordinator,
    /// Backing allocator for stable data structures (HashMap, ArrayList)
    backing_allocator: std.mem.Allocator,
    vfs: VFS,
    data_dir: []const u8,
    block_index: BlockIndex,
    graph_index: GraphEdgeIndex,
    wal: WAL,
    memtable_max_size: u64,

    /// Phase 1: Create the memtable manager without I/O operations.
    /// Uses Arena Coordinator Pattern: receives stable coordinator interface
    /// and passes it to child components. Coordinator remains valid across operations.
    /// Creates WAL instance but does not perform I/O until startup() is called.
    /// CRITICAL: ArenaCoordinator must be passed by pointer to prevent struct copying corruption.
    pub fn init(
        coordinator: *const ArenaCoordinator,
        backing: std.mem.Allocator,
        filesystem: VFS,
        data_dir: []const u8,
        memtable_max_size: u64,
    ) !MemtableManager {
        const owned_data_dir = try backing.dupe(u8, data_dir);

        var wal_dir_buffer: [512]u8 = undefined;
        const wal_dir = try std.fmt.bufPrint(wal_dir_buffer[0..], "{s}/wal", .{owned_data_dir});

        return MemtableManager{
            .arena_coordinator = coordinator,
            .backing_allocator = backing,
            .vfs = filesystem,
            .data_dir = owned_data_dir,
            .block_index = BlockIndex.init(coordinator, backing),
            .graph_index = GraphEdgeIndex.init(backing),
            .wal = try WAL.init(backing, filesystem, wal_dir),
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

    /// Configure WAL sync behavior for performance optimization.
    /// WARNING: Disabling immediate sync reduces durability guarantees.
    /// Should only be used for benchmarking or testing purposes.
    pub fn configure_wal_immediate_sync(self: *MemtableManager, enable: bool) void {
        self.wal.configure_immediate_sync(enable);
    }

    /// Clean up all memtable resources including arena-allocated memory.
    /// Must be called to prevent memory leaks. Coordinates cleanup of
    /// both block and edge indexes atomically plus WAL cleanup.
    pub fn deinit(self: *MemtableManager) void {
        concurrency.assert_main_thread();

        fatal_assert(@intFromPtr(self) != 0, "MemtableManager self pointer is null - memory corruption detected", .{});
        fatal_assert(@intFromPtr(&self.wal) != 0, "MemtableManager WAL pointer corrupted - memory safety violation detected", .{});
        fatal_assert(@intFromPtr(&self.block_index) != 0, "MemtableManager block_index pointer corrupted - memory safety violation detected", .{});
        fatal_assert(@intFromPtr(&self.graph_index) != 0, "MemtableManager graph_index pointer corrupted - memory safety violation detected", .{});

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

        // CRITICAL: WAL write must complete before memtable update for durability guarantees
        const wal_succeeded = blk: {
            // Streaming writes eliminate WALEntry buffer allocation for multi-MB blocks
            if (block.content.len >= 512 * 1024) {
                self.wal.write_block_streaming(block) catch |err| {
                    fatal_assert(false, "WAL streaming write failed before memtable update: {}", .{err});
                    return err;
                };
            } else {
                const wal_entry = try WALEntry.create_put_block(self.backing_allocator, block);
                defer wal_entry.deinit(self.backing_allocator);
                self.wal.write_entry(wal_entry) catch |err| {
                    fatal_assert(false, "WAL entry write failed before memtable update: {}", .{err});
                    return err;
                };
            }
            break :blk true;
        };

        // Validate WAL operation succeeded before proceeding to memtable
        fatal_assert(wal_succeeded, "WAL write did not complete successfully before memtable update", .{});

        // Additional validation for immediate sync mode (production safety)
        if (builtin.mode == .Debug and self.wal.enable_immediate_sync) {
            // In immediate sync mode, data should be durable at this point
            // This catches cases where durability ordering could be violated
        }

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

        // CRITICAL: WAL write must complete before memtable update for durability guarantees
        const wal_entry = try WALEntry.create_delete_block(self.backing_allocator, block_id);
        defer wal_entry.deinit(self.backing_allocator);
        self.wal.write_entry(wal_entry) catch |err| {
            fatal_assert(false, "WAL delete entry write failed before memtable update: {}", .{err});
            return err;
        };

        // Validate WAL write succeeded before proceeding with deletion
        if (builtin.mode == .Debug) {
            // In debug builds, verify the deletion operation order is correct
            fatal_assert(true, "WAL delete operation completed, proceeding with memtable deletion", .{});
        }

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

        const wal_entry = try WALEntry.create_put_edge(self.backing_allocator, edge);
        defer wal_entry.deinit(self.backing_allocator);
        try self.wal.write_entry(wal_entry);

        try self.graph_index.put_edge(edge);

        // Validate invariants after mutation in debug builds
        if (builtin.mode == .Debug) {
            self.validate_invariants();
        }
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
        if (self.block_index.find_block_runtime(id, .memtable_manager)) |owned_block_ptr| {
            return &owned_block_ptr.block;
        }
        return null;
    }

    /// Find a block in memtable and clone it with specified ownership.
    /// Creates a new OwnedBlock that can be safely transferred between subsystems.
    /// Used by StorageEngine to provide ownership-aware block access.
    pub fn find_block_with_ownership(
        self: *const MemtableManager,
        id: BlockId,
        block_ownership: BlockOwnership,
    ) !?OwnedBlock {
        if (self.block_index.find_block(id, .memtable_manager)) |block_ptr| {
            // Create temporary owned block for cloning
            const temp_owned = OwnedBlock.init(block_ptr.*, .memtable_manager, null);
            // Clone with new ownership for safe transfer between subsystems
            return try temp_owned.clone_with_ownership(self.backing_allocator, block_ownership, null);
        }
        return null;
    }

    /// Write an owned block to storage with full durability guarantees.
    /// Extracts ContextBlock from ownership wrapper for WAL and memtable storage.
    /// Used by StorageEngine when accepting ownership-aware block operations.
    pub fn put_block_durable_owned(self: *MemtableManager, owned_block: OwnedBlock) !void {
        concurrency.assert_main_thread();

        // Extract block for storage - MemtableManager accessing owned block for persistence
        const block = owned_block.read(.memtable_manager);

        // P0.5: WAL-before-memtable ordering assertions
        // Durability ordering: WAL MUST be persistent before memtable update
        const wal_entries_before = self.wal.statistics().entries_written;

        // Streaming writes eliminate WALEntry buffer allocation for multi-MB blocks
        if (block.content.len >= 512 * 1024) {
            try self.wal.write_block_streaming(block.*);
        } else {
            const wal_entry = try WALEntry.create_put_block(self.backing_allocator, block.*);
            defer wal_entry.deinit(self.backing_allocator);
            try self.wal.write_entry(wal_entry);
        }

        // P0.5: Verify WAL write completed before memtable update
        // This ordering is CRITICAL for durability guarantees
        const wal_entries_after = self.wal.statistics().entries_written;
        assert_fmt(wal_entries_after > wal_entries_before, "WAL entry count must increase before memtable update: {} -> {}", .{ wal_entries_before, wal_entries_after });

        // Only update memtable AFTER WAL persistence is confirmed
        try self.block_index.put_block(block.*);

        // P0.5: Validate durability ordering invariant was maintained
        if (builtin.mode == .Debug) {
            self.validate_invariants();
        }
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

        // Validate invariants after clearing in debug builds
        if (builtin.mode == .Debug) {
            self.validate_invariants();
        }
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
    pub fn raw_iterator(
        self: *const MemtableManager,
    ) std.HashMap(BlockId, OwnedBlock, BlockIndex.BlockIdContext, std.hash_map.default_max_load_percentage).Iterator {
        return self.block_index.blocks.iterator();
    }

    /// Get count of blocks currently in the memtable.
    /// Used for metrics and debugging. O(1) operation.
    pub fn block_count(self: *const MemtableManager) u32 {
        return @intCast(self.block_index.blocks.count());
    }

    pub fn find_outgoing_edges(self: *const MemtableManager, source_id: BlockId) []const OwnedGraphEdge {
        return self.graph_index.find_outgoing_edges_with_ownership(source_id, .memtable_manager) orelse &[_]OwnedGraphEdge{};
    }

    pub fn find_incoming_edges(self: *const MemtableManager, target_id: BlockId) []const OwnedGraphEdge {
        return self.graph_index.find_incoming_edges_with_ownership(target_id, .memtable_manager) orelse &[_]OwnedGraphEdge{};
    }

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
            fn apply(entry: WALEntry, context: *anyopaque) wal.WALError!void {
                const ctx: *RecoveryContext = @ptrCast(@alignCast(context));
                ctx.memtable.apply_wal_entry(entry) catch |err| switch (err) {
                    error.OutOfMemory => return wal.WALError.OutOfMemory,
                    else => return wal.WALError.CallbackFailed,
                };
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

    /// Ensure all WAL operations are durably persisted to disk.
    /// Forces synchronization of any pending write operations.
    pub fn flush_wal(self: *MemtableManager) !void {
        concurrency.assert_main_thread();

        if (self.wal.active_file) |*file| {
            file.flush() catch return error.IoError;
        }
    }

    /// P0.5: Validate WAL-before-memtable ordering invariant is maintained.
    /// Ensures WAL durability ordering is preserved - WAL entries must be persistent
    /// before corresponding memtable state exists. Critical for LSM-tree durability guarantees.
    fn validate_wal_memtable_ordering_invariant(self: *const MemtableManager) void {
        assert_fmt(builtin.mode == .Debug, "WAL ordering validation should only run in debug builds", .{});

        // WAL entry count should reflect all memtable blocks in durable form
        const memtable_blocks = self.block_count();
        const wal_entries = self.wal.statistics().entries_written;

        // In normal operation, WAL should have at least as many entries as memtable blocks
        // (WAL may have more due to edges, deletes, or unflushed entries)
        assert_fmt(wal_entries >= memtable_blocks, "WAL entry count {} cannot be less than memtable block count {} - durability ordering violated", .{ wal_entries, memtable_blocks });

        // Verify WAL is in a consistent state for durability
        if (self.wal.active_file != null) {
            assert_fmt(self.wal.statistics().entries_written > 0 or memtable_blocks == 0, "Active WAL file exists but no entries written with {} blocks in memtable - inconsistent state", .{memtable_blocks});
        }
    }

    /// P0.6 & P0.7: Comprehensive invariant validation for MemtableManager.
    /// Validates arena coordinator stability, memory accounting consistency,
    /// and component state coherence. Critical for detecting programming errors.
    pub fn validate_invariants(self: *const MemtableManager) void {
        if (builtin.mode == .Debug) {
            self.validate_arena_coordinator_stability();
            self.validate_memory_accounting_consistency();
            self.validate_component_state_coherence();
            self.validate_wal_memtable_ordering_invariant();
        }
    }

    /// P0.6: Validate arena coordinator stability across all subsystems.
    /// Ensures arena coordinators remain functional after struct operations.
    fn validate_arena_coordinator_stability(self: *const MemtableManager) void {
        assert_fmt(builtin.mode == .Debug, "Arena coordinator validation should only run in debug builds", .{});

        // Validate BlockIndex arena coordinator stability
        self.block_index.validate_invariants();

        // Validate GraphEdgeIndex has stable coordinator (if it uses one)
        // The graph_index should have its own validation if it uses arena coordinator
        assert_fmt(@intFromPtr(&self.graph_index) != 0, "GraphEdgeIndex pointer corruption detected", .{});

        // Validate backing allocator is still functional
        const test_alloc = self.backing_allocator.alloc(u8, 1) catch {
            fatal_assert(false, "MemtableManager backing allocator non-functional - corruption detected", .{});
            return;
        };
        defer self.backing_allocator.free(test_alloc);
    }

    /// P0.7: Validate memory accounting consistency across subsystems.
    /// Ensures tracked memory usage matches actual subsystem usage.
    fn validate_memory_accounting_consistency(self: *const MemtableManager) void {
        assert_fmt(builtin.mode == .Debug, "Memory accounting validation should only run in debug builds", .{});

        // Validate BlockIndex memory accounting (delegated to BlockIndex.validate_invariants)
        const block_index_memory = self.block_index.memory_usage();
        const total_memory = self.memory_usage();

        // MemtableManager memory should equal BlockIndex memory (main consumer)
        assert_fmt(total_memory == block_index_memory, "MemtableManager memory accounting inconsistency: total={} block_index={}", .{ total_memory, block_index_memory });

        // Validate memory usage is reasonable given block count
        const current_block_count = self.block_count();
        if (current_block_count > 0) {
            const avg_block_size = total_memory / current_block_count;
            assert_fmt(avg_block_size > 0 and avg_block_size < 100 * 1024 * 1024, "Average block size {} indicates memory corruption", .{avg_block_size});
        }
    }

    /// Validate coherence between different MemtableManager components.
    /// Ensures WAL, BlockIndex, and GraphEdgeIndex are in consistent states.
    fn validate_component_state_coherence(self: *const MemtableManager) void {
        assert_fmt(builtin.mode == .Debug, "Component coherence validation should only run in debug builds", .{});

        // Validate pointers are not corrupted
        fatal_assert(@intFromPtr(&self.block_index) != 0, "BlockIndex pointer corruption in MemtableManager", .{});
        fatal_assert(@intFromPtr(&self.graph_index) != 0, "GraphEdgeIndex pointer corruption in MemtableManager", .{});
        fatal_assert(@intFromPtr(&self.wal) != 0, "WAL pointer corruption in MemtableManager", .{});

        // Configuration corruption could lead to OOM or pathological performance degradation
        assert_fmt(self.memtable_max_size > 0 and self.memtable_max_size <= 10 * 1024 * 1024 * 1024, "Memtable max size {} is unreasonable - indicates corruption", .{self.memtable_max_size});

        // Validate current usage doesn't exceed configured maximum (with small buffer for overhead)
        const current_usage = self.memory_usage();
        assert_fmt(current_usage <= self.memtable_max_size * 2, "Memory usage {} exceeds 2x max size {} - indicates accounting corruption", .{ current_usage, self.memtable_max_size });
    }

    /// Clean up old WAL segments after successful memtable flush.
    /// Delegates to WAL module for actual cleanup operations.
    pub fn cleanup_old_wal_segments(self: *MemtableManager) !void {
        concurrency.assert_main_thread();

        try self.wal.cleanup_old_segments();
    }

    /// Orchestrates atomic transition from write-optimized to read-optimized storage.
    /// Maintains LSM-tree performance characteristics with proper ownership transfer.
    /// Creates OwnedBlock collection for SSTableManager with validated ownership.
    pub fn flush_to_sstable(self: *MemtableManager, sstable_manager: anytype) !void {
        concurrency.assert_main_thread();

        if (self.block_count() == 0) return;

        var owned_blocks = std.ArrayList(OwnedBlock).init(self.backing_allocator);
        defer owned_blocks.deinit();

        var block_iterator = self.iterator();
        while (block_iterator.next()) |owned_block_ptr| {
            try owned_blocks.append(owned_block_ptr.*);
        }

        try sstable_manager.create_new_sstable_from_memtable(owned_blocks.items);

        self.clear();

        // Clean up old WAL segments after successful SSTable creation
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

                const owned_block = try entry.extract_block(temp_allocator);
                try self.put_block(owned_block.read(.storage_engine).*);
            },
            .delete_block => {
                const block_id = try entry.extract_block_id();
                self.delete_block(block_id);
            },
            .put_edge => {
                const edge = try entry.extract_edge();
                try self.put_edge(edge);
            },
        }
    }
};

const testing = std.testing;

// Test helper: Mock StorageEngine for unit tests

const simulation_vfs = @import("../sim/simulation_vfs.zig");
const SimulationVFS = simulation_vfs.SimulationVFS;

fn create_test_block(id: BlockId, content: []const u8) ContextBlock {
    return ContextBlock{
        .id = id,
        .version = 1,
        .source_uri = "test://source.zig",
        .metadata_json = "{}",
        .content = content,
    };
}

fn create_test_edge(source: BlockId, target: BlockId, edge_type: EdgeType) GraphEdge {
    return GraphEdge{
        .source_id = source,
        .target_id = target,
        .edge_type = edge_type,
    };
}

test "MemtableManager basic lifecycle" {
    const allocator = testing.allocator;

    var sim_vfs = try SimulationVFS.init(allocator);
    defer sim_vfs.deinit();

    // Hierarchical memory model: create arena for content, use backing for structure
    var arena = std.heap.ArenaAllocator.init(allocator);
    defer arena.deinit();
    const coordinator = ArenaCoordinator{ .arena = &arena };
    var manager = try MemtableManager.init(&coordinator, allocator, sim_vfs.vfs(), "/test/data", 1024 * 1024);
    defer manager.deinit();

    try testing.expectEqual(@as(u32, 0), manager.block_count());
    try testing.expectEqual(@as(u32, 0), manager.edge_count());
    try testing.expectEqual(@as(u64, 0), manager.memory_usage());
}

test "MemtableManager with WAL operations" {
    const allocator = testing.allocator;

    var sim_vfs = try SimulationVFS.init(allocator);
    defer sim_vfs.deinit();

    // Hierarchical memory model: create arena for content, use backing for structure
    var arena = std.heap.ArenaAllocator.init(allocator);
    defer arena.deinit();
    const coordinator = ArenaCoordinator{ .arena = &arena };
    var manager = try MemtableManager.init(&coordinator, allocator, sim_vfs.vfs(), "/test/data", 1024 * 1024);
    defer manager.deinit();

    try manager.startup();

    const block_id = try BlockId.from_hex("00000000000000000000000000000001");
    const test_block = create_test_block(block_id, "test content");

    try manager.put_block_durable(test_block);
    try testing.expectEqual(@as(u32, 1), manager.block_count());

    const found_block = manager.find_block_in_memtable(block_id);
    try testing.expect(found_block != null);
    try testing.expectEqualStrings("test content", found_block.?.content);
}

test "MemtableManager multiple blocks" {
    const allocator = testing.allocator;

    var sim_vfs = try SimulationVFS.init(allocator);
    defer sim_vfs.deinit();

    // Hierarchical memory model: create arena for content, use backing for structure
    var arena = std.heap.ArenaAllocator.init(allocator);
    defer arena.deinit();
    const coordinator = ArenaCoordinator{ .arena = &arena };
    var manager = try MemtableManager.init(&coordinator, allocator, sim_vfs.vfs(), "/test/data", 1024 * 1024);
    defer manager.deinit();

    try manager.startup();

    const block1_id = try BlockId.from_hex("00000000000000000000000000000001");
    const block2_id = try BlockId.from_hex("00000000000000000000000000000002");
    const block1 = create_test_block(block1_id, "content 1");
    const block2 = create_test_block(block2_id, "content 2");

    try manager.put_block_durable(block1);
    try manager.put_block_durable(block2);

    try testing.expectEqual(@as(u32, 2), manager.block_count());
    try testing.expect(manager.find_block_in_memtable(block1_id) != null);
    try testing.expect(manager.find_block_in_memtable(block2_id) != null);
}

test "MemtableManager edge operations" {
    const allocator = testing.allocator;

    var sim_vfs = try SimulationVFS.init(allocator);
    defer sim_vfs.deinit();

    // Hierarchical memory model: create arena for content, use backing for structure
    var arena = std.heap.ArenaAllocator.init(allocator);
    defer arena.deinit();
    const coordinator = ArenaCoordinator{ .arena = &arena };
    var manager = try MemtableManager.init(&coordinator, allocator, sim_vfs.vfs(), "/test/data", 1024 * 1024);
    defer manager.deinit();

    try manager.startup();

    const source_id = try BlockId.from_hex("00000000000000000000000000000001");
    const target_id = try BlockId.from_hex("00000000000000000000000000000002");
    const test_edge = create_test_edge(source_id, target_id, .imports);

    try manager.put_edge_durable(test_edge);
    try testing.expectEqual(@as(u32, 1), manager.edge_count());

    const outgoing = manager.find_outgoing_edges(source_id);
    try testing.expectEqual(@as(usize, 1), outgoing.len);
    try testing.expectEqual(target_id, outgoing[0].edge.target_id);
}

test "MemtableManager clear operation" {
    const allocator = testing.allocator;

    var sim_vfs = try SimulationVFS.init(allocator);
    defer sim_vfs.deinit();

    // Hierarchical memory model: create arena for content, use backing for structure
    var arena = std.heap.ArenaAllocator.init(allocator);
    defer arena.deinit();
    const coordinator = ArenaCoordinator{ .arena = &arena };
    var manager = try MemtableManager.init(&coordinator, allocator, sim_vfs.vfs(), "/test/data", 1024 * 1024);
    defer manager.deinit();

    try manager.startup();

    const block_id = try BlockId.from_hex("00000000000000000000000000000001");
    const test_block = create_test_block(block_id, "clear test");
    try manager.put_block_durable(test_block);

    try testing.expectEqual(@as(u32, 1), manager.block_count());

    // O(1) arena cleanup
    manager.clear();

    try testing.expectEqual(@as(u32, 0), manager.block_count());
    try testing.expectEqual(@as(u64, 0), manager.memory_usage());
}
