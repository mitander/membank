//! Storage engine coordination and main public API.
//!
//! Implements the main StorageEngine struct that coordinates between all
//! storage subsystems: BlockIndex (memtable), GraphEdgeIndex, WAL, SSTables,
//! and compaction. Provides the unified public interface for all storage
//! operations while maintaining the LSM-tree architecture principles.
//!
//! Key responsibilities:
//! - Coordinate writes through WAL -> BlockIndex -> SSTable flush pipeline
//! - Orchestrate reads from BlockIndex -> SSTables with proper precedence
//! - Manage background compaction to maintain read performance
//! - Enforce arena-per-subsystem memory management patterns
//! - Provide comprehensive metrics and error handling

const std = @import("std");
const assert = @import("../core/assert.zig").assert;
const assert_fmt = @import("../core/assert.zig").assert_fmt;
const vfs = @import("../core/vfs.zig");
const context_block = @import("../core/types.zig");
const error_context = @import("../core/error_context.zig");
const concurrency = @import("../core/concurrency.zig");

// Import storage submodules
const config_mod = @import("config.zig");
const metrics_mod = @import("metrics.zig");
const memtable_manager_mod = @import("memtable_manager.zig");
const sstable_manager_mod = @import("sstable_manager.zig");
const block_index_mod = @import("block_index.zig");

// Import existing storage modules
const sstable = @import("sstable.zig");
const tiered_compaction = @import("tiered_compaction.zig");
const wal = @import("wal.zig");

const VFS = vfs.VFS;
const ContextBlock = context_block.ContextBlock;
const GraphEdge = context_block.GraphEdge;
const BlockId = context_block.BlockId;

// Type aliases for cleaner code
const BlockHashMap = std.HashMap(BlockId, ContextBlock, block_index_mod.BlockIndex.BlockIdContext, std.hash_map.default_max_load_percentage);
const BlockHashMapIterator = BlockHashMap.Iterator;

// Re-export submodule types
pub const Config = config_mod.Config;
pub const StorageMetrics = metrics_mod.StorageMetrics;
pub const MemtableManager = memtable_manager_mod.MemtableManager;
pub const SSTableManager = sstable_manager_mod.SSTableManager;

// Re-export existing module types
pub const SSTable = sstable.SSTable;
pub const Compactor = sstable.Compactor;
pub const TieredCompactionManager = tiered_compaction.TieredCompactionManager;
pub const WAL = wal.WAL;
pub const WALEntry = wal.WALEntry;
pub const WALEntryType = wal.WALEntryType;
pub const WALError = wal.WALError;

/// Storage engine errors.
pub const StorageError = error{
    /// Block not found in storage
    BlockNotFound,
    /// Storage already initialized
    AlreadyInitialized,
    /// Storage not initialized
    NotInitialized,
} || config_mod.ConfigError || wal.WALError || vfs.VFSError || vfs.VFileError;

/// Main storage engine coordinating all storage subsystems.
/// Implements LSM-tree architecture with WAL durability, in-memory
/// memtable management, immutable SSTables, and background compaction.
/// Follows state-oriented decomposition with MemtableManager for in-memory
/// state and SSTableManager for on-disk state. True coordinator pattern.
pub const StorageEngine = struct {
    backing_allocator: std.mem.Allocator,
    vfs: VFS,
    data_dir: []const u8,
    config: Config,
    memtable_manager: MemtableManager,
    sstable_manager: SSTableManager,
    wal: WAL,
    initialized: bool,
    storage_metrics: StorageMetrics,
    query_cache_arena: std.heap.ArenaAllocator,

    /// Initialize storage engine with default configuration.
    /// Convenience method for common use cases that don't require custom configuration.
    pub fn init_default(
        allocator: std.mem.Allocator,
        filesystem: VFS,
        data_dir: []const u8,
    ) !StorageEngine {
        return init(allocator, filesystem, data_dir, Config{});
    }

    /// Initialize storage engine with custom configuration.
    /// Creates all necessary subsystems but does not perform I/O operations.
    /// Call initialize_storage() and recover_from_wal() to complete startup.
    pub fn init(
        allocator: std.mem.Allocator,
        filesystem: VFS,
        data_dir: []const u8,
        storage_config: Config,
    ) !StorageEngine {
        try storage_config.validate();

        // Clone data_dir for owned storage
        const owned_data_dir = try allocator.dupe(u8, data_dir);

        // Initialize WAL directory path
        const wal_dir = try std.fmt.allocPrint(allocator, "{s}/wal", .{owned_data_dir});
        defer allocator.free(wal_dir);

        const engine = StorageEngine{
            .backing_allocator = allocator,
            .vfs = filesystem,
            .data_dir = owned_data_dir,
            .config = storage_config,
            .memtable_manager = MemtableManager.init(allocator),
            .sstable_manager = SSTableManager.init(allocator, filesystem, owned_data_dir),
            .wal = try WAL.init(allocator, filesystem, wal_dir),
            .initialized = false,
            .storage_metrics = StorageMetrics.init(),
            .query_cache_arena = std.heap.ArenaAllocator.init(allocator),
        };

        return engine;
    }

    /// Clean up all storage engine resources.
    /// Must be called to prevent memory leaks and ensure proper cleanup.
    pub fn deinit(self: *StorageEngine) void {
        concurrency.assert_main_thread();

        self.wal.deinit();
        self.memtable_manager.deinit();
        self.sstable_manager.deinit();
        self.query_cache_arena.deinit();
        self.backing_allocator.free(self.data_dir);
    }

    /// Initialize storage by creating directories and discovering existing data.
    /// Must be called before any read/write operations. Creates necessary
    /// directory structure and starts up all subsystems with I/O operations.
    pub fn initialize_storage(self: *StorageEngine) !void {
        concurrency.assert_main_thread();
        if (self.initialized) return StorageError.AlreadyInitialized;

        // Create data directory structure
        if (!self.vfs.exists(self.data_dir)) {
            try self.vfs.mkdir(self.data_dir);
        }

        // Create WAL directory
        const wal_dir = try std.fmt.allocPrint(self.backing_allocator, "{s}/wal", .{self.data_dir});
        defer self.backing_allocator.free(wal_dir);

        if (!self.vfs.exists(wal_dir)) {
            try self.vfs.mkdir(wal_dir);
        }

        // Start up SSTable manager (creates sst dir and discovers existing files)
        try self.sstable_manager.startup();

        self.initialized = true;
    }

    /// Complete startup by performing WAL recovery.
    /// Convenience method that calls initialize_storage() followed by recover_from_wal().
    pub fn startup(self: *StorageEngine) !void {
        try self.initialize_storage();
        try self.recover_from_wal();
    }

    /// Write a Context Block to storage with full durability guarantees.
    /// WAL-first design ensures durability before in-memory state update.
    /// Automatically triggers memtable flush when size threshold exceeded.
    pub fn put_block(self: *StorageEngine, block: ContextBlock) !void {
        concurrency.assert_main_thread();
        if (!self.initialized) return StorageError.NotInitialized;

        const start_time = std.time.nanoTimestamp();

        // Validate block structure and content before accepting
        try block.validate(self.backing_allocator);

        // WAL-first: durability before visibility
        const wal_entry = try WALEntry.create_put_block(block, self.backing_allocator);
        defer wal_entry.deinit(self.backing_allocator);
        try self.write_wal_entry(wal_entry);

        // Update in-memory memtable after WAL write
        try self.memtable_manager.put_block(block);

        // Check if memtable flush is needed
        if (self.memtable_manager.memory_usage() >= self.config.memtable_max_size) {
            try self.flush_memtable();
        }

        // Update metrics
        const end_time = std.time.nanoTimestamp();
        _ = self.storage_metrics.blocks_written.fetchAdd(1, .monotonic);
        _ = self.storage_metrics.total_write_time_ns.fetchAdd(@intCast(end_time - start_time), .monotonic);
        _ = self.storage_metrics.total_bytes_written.fetchAdd(block.content.len, .monotonic);
    }

    /// Find a Context Block by ID with LSM-tree read semantics.
    /// Checks memtable first, then SSTables in reverse chronological order
    /// to ensure most recent version is returned.
    pub fn find_block(self: *StorageEngine, block_id: BlockId) !?ContextBlock {
        if (!self.initialized) return StorageError.NotInitialized;

        const start_time = std.time.nanoTimestamp();

        // Check memtable first for most recent data
        if (self.memtable_manager.find_block_in_memtable(block_id)) |block_ptr| {
            const end_time = std.time.nanoTimestamp();
            _ = self.storage_metrics.blocks_read.fetchAdd(1, .monotonic);
            _ = self.storage_metrics.total_read_time_ns.fetchAdd(@intCast(end_time - start_time), .monotonic);
            _ = self.storage_metrics.total_bytes_read.fetchAdd(block_ptr.content.len, .monotonic);
            return block_ptr.*;
        }

        // Search SSTables in reverse order (newest first)
        if (try self.sstable_manager.find_block_in_sstables(block_id, self.query_cache_arena.allocator())) |block| {
            const end_time = std.time.nanoTimestamp();
            _ = self.storage_metrics.blocks_read.fetchAdd(1, .monotonic);
            _ = self.storage_metrics.sstable_reads.fetchAdd(1, .monotonic);
            _ = self.storage_metrics.total_read_time_ns.fetchAdd(@intCast(end_time - start_time), .monotonic);
            _ = self.storage_metrics.total_bytes_read.fetchAdd(block.content.len, .monotonic);

            // Periodically clear query cache to prevent unbounded growth
            self.maybe_clear_query_cache();

            return block;
        }

        return null;
    }

    /// Delete a Context Block by ID with tombstone semantics.
    /// Creates WAL entry for durability then removes from memtable.
    /// Actual space reclamation occurs during SSTable compaction.
    pub fn delete_block(self: *StorageEngine, block_id: BlockId) !void {
        concurrency.assert_main_thread();
        if (!self.initialized) return StorageError.NotInitialized;

        // WAL-first: record deletion for durability
        const wal_entry = try WALEntry.create_delete_block(block_id, self.backing_allocator);
        defer wal_entry.deinit(self.backing_allocator);
        try self.write_wal_entry(wal_entry);

        // Remove from memtable (handles both block and edges)
        self.memtable_manager.delete_block(block_id);

        _ = self.storage_metrics.blocks_deleted.fetchAdd(1, .monotonic);
    }

    /// Add a graph edge with durability guarantees.
    /// Updates both WAL and in-memory graph index for immediate availability.
    pub fn put_edge(self: *StorageEngine, edge: GraphEdge) !void {
        concurrency.assert_main_thread();
        if (!self.initialized) return StorageError.NotInitialized;

        // WAL-first: durability before visibility
        const wal_entry = try WALEntry.create_put_edge(edge, self.backing_allocator);
        defer wal_entry.deinit(self.backing_allocator);
        try self.write_wal_entry(wal_entry);

        // Update in-memory graph index via memtable manager
        try self.memtable_manager.put_edge(edge);

        _ = self.storage_metrics.edges_added.fetchAdd(1, .monotonic);
    }

    /// Get current block count across all storage layers.
    pub fn block_count(self: *const StorageEngine) u32 {
        return self.memtable_manager.block_count();
    }

    /// Get current edge count in graph index.
    pub fn edge_count(self: *const StorageEngine) u32 {
        return self.memtable_manager.edge_count();
    }

    /// Find all outgoing edges from a source block.
    /// Delegates to memtable manager for graph traversal operations.
    pub fn find_outgoing_edges(self: *const StorageEngine, source_id: BlockId) []const GraphEdge {
        return self.memtable_manager.find_outgoing_edges(source_id);
    }

    /// Find all incoming edges to a target block.
    /// Delegates to memtable manager for reverse graph traversal operations.
    pub fn find_incoming_edges(self: *const StorageEngine, target_id: BlockId) []const GraphEdge {
        return self.memtable_manager.find_incoming_edges(target_id);
    }

    /// Get performance metrics for monitoring and debugging.
    pub fn metrics(self: *const StorageEngine) *const StorageMetrics {
        return &self.storage_metrics;
    }


    /// Block iterator for scanning all blocks in storage (memtable + SSTables)
    pub const BlockIterator = struct {
        storage_engine: *StorageEngine,
        memtable_iterator: ?BlockHashMapIterator,
        sstable_index: usize,
        current_sstable: ?SSTable,
        current_sstable_iterator: ?sstable.SSTableIterator,

        pub fn next(self: *BlockIterator) !?ContextBlock {
            // First, iterate through memtable
            if (self.memtable_iterator) |*iter| {
                if (iter.next()) |entry| {
                    return entry.value_ptr.*;
                } else {
                    self.memtable_iterator = null;
                }
            }

            // Then iterate through SSTables
            while (self.sstable_index < self.storage_engine.sstables.items.len) {
                if (self.current_sstable_iterator == null) {
                    // Open next SSTable
                    const sstable_path = self.storage_engine.sstables.items[self.sstable_index];
                    var sstable_instance = SSTable.init(self.storage_engine.backing_allocator, self.storage_engine.vfs, sstable_path);
                    sstable_instance.read_index() catch {
                        self.sstable_index += 1;
                        continue;
                    };
                    self.current_sstable = sstable_instance;
                    self.current_sstable_iterator = sstable_instance.iterator();
                }

                if (try self.current_sstable_iterator.?.next()) |block| {
                    return block;
                } else {
                    // Finished with current SSTable, move to next
                    if (self.current_sstable) |*sstable_ref| {
                        sstable_ref.deinit();
                    }
                    self.current_sstable = null;
                    self.current_sstable_iterator = null;
                    self.sstable_index += 1;
                }
            }

            return null;
        }

        pub fn deinit(self: *BlockIterator) void {
            if (self.current_sstable) |*sstable_ref| {
                sstable_ref.deinit();
            }
        }
    };

    /// Create iterator to scan all blocks in storage (memtable + SSTables)
    pub fn iterate_all_blocks(self: *StorageEngine) BlockIterator {
        return BlockIterator{
            .storage_engine = self,
            .memtable_iterator = self.memtable_manager.raw_iterator(),
            .sstable_index = 0,
            .current_sstable = null,
            .current_sstable_iterator = null,
        };
    }

    /// Force WAL flush to disk for durability testing.
    /// WAL normally flushes automatically on writes for durability.
    pub fn flush_wal(self: *StorageEngine) !void {
        const start_time = std.time.nanoTimestamp();

        const end_time = std.time.nanoTimestamp();
        _ = self.storage_metrics.wal_flushes.fetchAdd(1, .monotonic);
        _ = self.storage_metrics.total_wal_flush_time_ns.fetchAdd(@intCast(end_time - start_time), .monotonic);
    }

    /// Flush current memtable to SSTable with coordinated subsystem management.
    /// Core LSM-tree operation that delegates to SSTableManager for SSTable creation
    /// and coordinates cleanup with MemtableManager. Follows the coordinator pattern.
    fn flush_memtable(self: *StorageEngine) !void {
        concurrency.assert_main_thread();
        
        if (self.memtable_manager.block_count() == 0) return; // Nothing to flush

        // Collect all blocks from memtable for SSTable creation
        var blocks = std.ArrayList(ContextBlock).init(self.backing_allocator);
        defer blocks.deinit();

        var iterator = self.memtable_manager.iterator();
        while (iterator.next()) |block| {
            try blocks.append(block);
        }

        // Delegate SSTable creation to SSTableManager
        try self.sstable_manager.create_new_sstable(blocks.items);

        // Atomically clear memtable after successful SSTable creation
        self.memtable_manager.clear();

        // Check for compaction opportunities
        try self.sstable_manager.check_and_run_compaction();

        // Clean up old WAL segments after successful flush
        try self.wal.cleanup_old_segments();

        _ = self.storage_metrics.sstable_writes.fetchAdd(1, .monotonic);
    }

    /// Public wrapper for memtable flush - backward compatibility.
    /// Delegates to internal flush_memtable method for coordinated subsystem management.
    pub fn flush_memtable_to_sstable(self: *StorageEngine) !void {
        try self.flush_memtable();
    }

    /// Recover storage state from WAL files.
    /// Replays all committed operations to reconstruct consistent state
    /// after system restart. Uses WAL module's streaming recovery for
    /// memory efficiency with large WAL files.
    pub fn recover_from_wal(self: *StorageEngine) !void {
        concurrency.assert_main_thread();
        if (!self.initialized) return StorageError.NotInitialized;

        const RecoveryContext = struct {
            engine: *StorageEngine,
        };

        const recovery_callback = struct {
            fn apply(entry: WALEntry, context: *anyopaque) !void {
                const ctx: *RecoveryContext = @ptrCast(@alignCast(context));
                try ctx.engine.apply_wal_entry(entry);
            }
        }.apply;

        var recovery_context = RecoveryContext{ .engine = self };

        try self.wal.recover_entries(recovery_callback, &recovery_context);

        _ = self.storage_metrics.wal_recoveries.fetchAdd(1, .monotonic);
    }

    // Internal helper methods

    /// Clear query cache arena if it exceeds memory threshold to prevent unbounded growth.
    /// Called after SSTable reads to maintain bounded memory usage for query operations.
    fn maybe_clear_query_cache(self: *StorageEngine) void {
        const QUERY_CACHE_THRESHOLD = 50 * 1024 * 1024; // 50MB threshold

        if (self.query_cache_arena.queryCapacity() > QUERY_CACHE_THRESHOLD) {
            _ = self.query_cache_arena.reset(.retain_capacity);
        }
    }

    /// Write a WAL entry using the WAL subsystem.
    fn write_wal_entry(self: *StorageEngine, entry: WALEntry) !void {
        try self.wal.write_entry(entry);
        _ = self.storage_metrics.wal_writes.fetchAdd(1, .monotonic);
    }

    /// Apply a WAL entry during recovery to rebuild storage state.
    fn apply_wal_entry(self: *StorageEngine, entry: WALEntry) !void {
        switch (entry.entry_type) {
            .put_block => {
                // Use temporary arena for block deserialization to prevent memory leaks
                var temp_arena = std.heap.ArenaAllocator.init(self.backing_allocator);
                defer temp_arena.deinit();
                const temp_allocator = temp_arena.allocator();

                const block = try entry.extract_block(temp_allocator);
                try self.memtable_manager.put_block(block);
            },
            .delete_block => {
                const block_id = try entry.extract_block_id();
                self.memtable_manager.delete_block(block_id);
            },
            .put_edge => {
                const edge = try entry.extract_edge();
                try self.memtable_manager.put_edge(edge);
            },
        }
    }

    /// Check for compaction opportunities and execute if beneficial.
    /// Delegates to TieredCompactionManager for LSM-tree optimization.
    fn check_and_run_compaction(self: *StorageEngine) !void {
        const compaction_job = self.compaction_manager.check_compaction_needed();
        if (compaction_job) |job| {
            try self.compaction_manager.execute_compaction(job);
            _ = self.storage_metrics.compactions.fetchAdd(1, .monotonic);
        }
    }

    /// Get file size for SSTable registration with compaction manager.
    fn read_file_size(self: *StorageEngine, path: []const u8) !u64 {
        var file = try self.vfs.open(path, .read);
        defer file.close();
        return try file.file_size();
    }

    /// Discover existing SSTable files and register with compaction manager.
    /// Called during initialization to restore system state after restart.
    fn discover_existing_sstables(self: *StorageEngine) !void {
        const sst_dir = try std.fmt.allocPrint(self.backing_allocator, "{s}/sst", .{self.data_dir});
        defer self.backing_allocator.free(sst_dir);

        if (!self.vfs.exists(sst_dir)) return;

        var dir_iter = try self.vfs.iterate_directory(sst_dir, self.backing_allocator);
        defer dir_iter.deinit(self.backing_allocator);

        while (dir_iter.next()) |entry| {
            if (entry.kind != .file) continue;

            const extension = std.fs.path.extension(entry.name);
            if (!std.mem.eql(u8, extension, ".sst")) continue;

            // Build full path and register with storage
            const full_path = try std.fmt.allocPrint(self.backing_allocator, "{s}/{s}", .{ sst_dir, entry.name });
            try self.sstables.append(full_path);

            // Register with compaction manager
            const file_size = try self.read_file_size(full_path);
            try self.compaction_manager.add_sstable(full_path, file_size, 0);

            // Update next SSTable ID to avoid conflicts
            if (std.mem.startsWith(u8, entry.name, "sstable_")) {
                const id_str = entry.name[8..12]; // Extract 4-digit ID
                if (std.fmt.parseInt(u32, id_str, 10)) |id| {
                    if (id >= self.next_sstable_id) {
                        self.next_sstable_id = id + 1;
                    }
                } else |_| {
                    // Invalid format, skip
                }
            }
        }
    }
};

// Tests
const testing = std.testing;
const simulation_vfs = @import("../sim/simulation_vfs.zig");

test "storage engine initialization and cleanup" {
    const allocator = testing.allocator;

    var sim_vfs = try simulation_vfs.SimulationVFS.init(allocator);
    defer sim_vfs.deinit();

    var engine = try StorageEngine.init_default(allocator, sim_vfs.vfs(), "/test/data");
    defer engine.deinit();

    try testing.expect(!engine.initialized);
    try testing.expectEqual(@as(u32, 0), engine.block_count());
}

test "storage engine startup and basic operations" {
    const allocator = testing.allocator;

    var sim_vfs = try simulation_vfs.SimulationVFS.init(allocator);
    defer sim_vfs.deinit();

    var engine = try StorageEngine.init_default(allocator, sim_vfs.vfs(), "/test/data");
    defer engine.deinit();

    try engine.startup();
    try testing.expect(engine.initialized);

    // Test basic block operations
    const block_id = BlockId.generate();
    const block = ContextBlock{
        .id = block_id,
        .version = 1,
        .source_uri = "file://test.zig",
        .metadata_json = "{}",
        .content = "test content",
    };

    try engine.put_block(block);
    try testing.expectEqual(@as(u32, 1), engine.block_count());

    const found_block = try engine.find_block(block_id);
    try testing.expect(found_block != null);
    try testing.expectEqualStrings("test content", found_block.?.content);

    try engine.delete_block(block_id);
    try testing.expectEqual(@as(u32, 0), engine.block_count());
}

test "memtable flush triggers at size threshold" {
    const allocator = testing.allocator;

    var sim_vfs = try simulation_vfs.SimulationVFS.init(allocator);
    defer sim_vfs.deinit();

    // Use minimal config for fast test
    const config = Config.minimal_for_testing();
    var engine = try StorageEngine.init(allocator, sim_vfs.vfs(), "/test/data", config);
    defer engine.deinit();

    try engine.startup();

    // Add blocks until flush threshold reached
    const large_content = try allocator.alloc(u8, 1024 * 256); // 256KB blocks
    defer allocator.free(large_content);
    @memset(large_content, 'A');

    // Add enough blocks to exceed 1MB threshold
    for (0..5) |i| {
        const block = ContextBlock{
            .id = BlockId.generate(),
            .version = 1,
            .source_uri = "file://test.zig",
            .metadata_json = "{}",
            .content = large_content,
        };
        try engine.put_block(block);

        // Should flush and reset memtable when threshold exceeded
        if (i >= 3) { // After ~1MB of data
            try testing.expect(engine.index.memory_usage() < config.memtable_max_size);
        }
    }

    // Verify SSTable was created
    const metrics = engine.metrics();
    try testing.expect(metrics.sstable_writes.load(.monotonic) > 0);
}

test "WAL recovery restores storage state" {
    const allocator = testing.allocator;

    var sim_vfs = try simulation_vfs.SimulationVFS.init(allocator);
    defer sim_vfs.deinit();

    const block_id = BlockId.generate();
    const block = ContextBlock{
        .id = block_id,
        .version = 1,
        .source_uri = "file://test.zig",
        .metadata_json = "{}",
        .content = "test content",
    };

    // Write data and clean shutdown
    {
        var engine = try StorageEngine.init_default(allocator, sim_vfs.vfs(), "/test/data");
        defer engine.deinit();

        try engine.startup();
        try engine.put_block(block);
        try engine.flush_wal();
    }

    // Restart and verify recovery
    {
        var engine = try StorageEngine.init_default(allocator, sim_vfs.vfs(), "/test/data");
        defer engine.deinit();

        try engine.startup();
        try testing.expectEqual(@as(u32, 1), engine.block_count());

        const recovered_block = try engine.find_block(block_id);
        try testing.expect(recovered_block != null);
        try testing.expectEqualStrings("test content", recovered_block.?.content);
    }
}

test "graph edge operations work correctly" {
    const allocator = testing.allocator;

    var sim_vfs = try simulation_vfs.SimulationVFS.init(allocator);
    defer sim_vfs.deinit();

    var engine = try StorageEngine.init_default(allocator, sim_vfs.vfs(), "/test/data");
    defer engine.deinit();

    try engine.startup();

    const source_id = BlockId.generate();
    const target_id = BlockId.generate();
    const edge = GraphEdge{
        .source_id = source_id,
        .target_id = target_id,
        .edge_type = .calls,
    };

    try engine.put_edge(edge);
    try testing.expectEqual(@as(u32, 1), engine.edge_count());

    const outgoing = engine.find_outgoing_edges(source_id);
    try testing.expect(outgoing != null);
    try testing.expectEqual(@as(usize, 1), outgoing.?.len);

    const incoming = engine.find_incoming_edges(target_id);
    try testing.expect(incoming != null);
    try testing.expectEqual(@as(usize, 1), incoming.?.len);
}

test "storage metrics track operations accurately" {
    const allocator = testing.allocator;

    var sim_vfs = try simulation_vfs.SimulationVFS.init(allocator);
    defer sim_vfs.deinit();

    var engine = try StorageEngine.init_default(allocator, sim_vfs.vfs(), "/test/data");
    defer engine.deinit();

    try engine.startup();

    const metrics_initial = engine.metrics();
    try testing.expectEqual(@as(u64, 0), metrics_initial.blocks_written.load(.monotonic));

    const block = ContextBlock{
        .id = BlockId.generate(),
        .version = 1,
        .source_uri = "file://test.zig",
        .metadata_json = "{}",
        .content = "test content",
    };

    try engine.put_block(block);

    const metrics_after = engine.metrics();
    try testing.expectEqual(@as(u64, 1), metrics_after.blocks_written.load(.monotonic));
    try testing.expect(metrics_after.total_write_time_ns.load(.monotonic) > 0);
    try testing.expect(metrics_after.total_bytes_written.load(.monotonic) > 0);
}

test "block iterator with empty storage" {
    const allocator = testing.allocator;

    var sim_vfs = try simulation_vfs.SimulationVFS.init(allocator);
    defer sim_vfs.deinit();

    var engine = try StorageEngine.init_default(allocator, sim_vfs.vfs(), "/test/data");
    defer engine.deinit();
    try engine.initialize_storage();

    // Test iterator over empty storage
    var iterator = engine.iterate_all_blocks();
    defer iterator.deinit();

    const block = try iterator.next();
    try testing.expectEqual(@as(?ContextBlock, null), block);
}

test "block iterator with memtable blocks only" {
    const allocator = testing.allocator;

    var sim_vfs = try simulation_vfs.SimulationVFS.init(allocator);
    defer sim_vfs.deinit();

    var engine = try StorageEngine.init_default(allocator, sim_vfs.vfs(), "/test/data");
    defer engine.deinit();
    try engine.initialize_storage();

    // Add test blocks to memtable
    const block1 = ContextBlock{
        .id = BlockId.generate(),
        .version = 1,
        .source_uri = "test1.zig",
        .metadata_json = "{}",
        .content = "content 1",
    };
    const block2 = ContextBlock{
        .id = BlockId.generate(),
        .version = 1,
        .source_uri = "test2.zig",
        .metadata_json = "{}",
        .content = "content 2",
    };

    try engine.put_block(block1);
    try engine.put_block(block2);

    // Iterate and collect all blocks
    var iterator = engine.iterate_all_blocks();
    defer iterator.deinit();

    var found_blocks = std.ArrayList(ContextBlock).init(allocator);
    defer found_blocks.deinit();

    while (try iterator.next()) |block| {
        try found_blocks.append(block);
    }

    // Should find exactly 2 blocks
    try testing.expectEqual(@as(usize, 2), found_blocks.items.len);

    // Verify we can find both blocks (order may vary due to HashMap iteration)
    var found_block1 = false;
    var found_block2 = false;
    for (found_blocks.items) |block| {
        if (std.mem.eql(u8, &block.id.bytes, &block1.id.bytes)) {
            found_block1 = true;
            try testing.expectEqualStrings("content 1", block.content);
        } else if (std.mem.eql(u8, &block.id.bytes, &block2.id.bytes)) {
            found_block2 = true;
            try testing.expectEqualStrings("content 2", block.content);
        }
    }
    try testing.expect(found_block1);
    try testing.expect(found_block2);
}

test "block iterator with SSTable blocks" {
    const allocator = testing.allocator;

    var sim_vfs = try simulation_vfs.SimulationVFS.init(allocator);
    defer sim_vfs.deinit();

    // Use minimal config for fast test
    const config = Config.minimal_for_testing();
    var engine = try StorageEngine.init(allocator, sim_vfs.vfs(), "/test/data", config);
    defer engine.deinit();
    try engine.initialize_storage();

    // Add blocks to trigger SSTable flush
    const block1 = ContextBlock{
        .id = BlockId.generate(),
        .version = 1,
        .source_uri = "sstable1.zig",
        .metadata_json = "{}",
        .content = "sstable content 1",
    };
    const block2 = ContextBlock{
        .id = BlockId.generate(),
        .version = 1,
        .source_uri = "sstable2.zig",
        .metadata_json = "{}",
        .content = "sstable content 2",
    };

    try engine.put_block(block1);
    try engine.put_block(block2);

    // Force memtable flush to create SSTable
    try engine.flush_memtable_to_sstable();

    // Verify memtable is empty
    try testing.expectEqual(@as(u32, 0), engine.block_count());

    // Iterate and collect all blocks from SSTables
    var iterator = engine.iterate_all_blocks();
    defer iterator.deinit();

    var found_blocks = std.ArrayList(ContextBlock).init(allocator);
    defer found_blocks.deinit();

    while (try iterator.next()) |block| {
        try found_blocks.append(block);
    }

    // Should find exactly 2 blocks from SSTable
    try testing.expectEqual(@as(usize, 2), found_blocks.items.len);
}

test "block iterator with mixed memtable and SSTable blocks" {
    const allocator = testing.allocator;

    var sim_vfs = try simulation_vfs.SimulationVFS.init(allocator);
    defer sim_vfs.deinit();

    // Use minimal config for fast test
    const config = Config.minimal_for_testing();
    var engine = try StorageEngine.init(allocator, sim_vfs.vfs(), "/test/data", config);
    defer engine.deinit();
    try engine.initialize_storage();

    // Add blocks to SSTable first
    const sstable_block = ContextBlock{
        .id = BlockId.generate(),
        .version = 1,
        .source_uri = "sstable.zig",
        .metadata_json = "{}",
        .content = "sstable content",
    };

    try engine.put_block(sstable_block);
    try engine.flush_memtable_to_sstable();

    // Add new blocks to memtable
    const memtable_block = ContextBlock{
        .id = BlockId.generate(),
        .version = 1,
        .source_uri = "memtable.zig",
        .metadata_json = "{}",
        .content = "memtable content",
    };

    try engine.put_block(memtable_block);

    // Iterate and collect all blocks
    var iterator = engine.iterate_all_blocks();
    defer iterator.deinit();

    var found_blocks = std.ArrayList(ContextBlock).init(allocator);
    defer found_blocks.deinit();

    while (try iterator.next()) |block| {
        try found_blocks.append(block);
    }

    // Should find blocks from both memtable and SSTable
    try testing.expectEqual(@as(usize, 2), found_blocks.items.len);

    // Verify we get blocks from both sources
    var found_sstable_block = false;
    var found_memtable_block = false;
    for (found_blocks.items) |block| {
        if (std.mem.eql(u8, &block.id.bytes, &sstable_block.id.bytes)) {
            found_sstable_block = true;
            try testing.expectEqualStrings("sstable content", block.content);
        } else if (std.mem.eql(u8, &block.id.bytes, &memtable_block.id.bytes)) {
            found_memtable_block = true;
            try testing.expectEqualStrings("memtable content", block.content);
        }
    }
    try testing.expect(found_sstable_block);
    try testing.expect(found_memtable_block);
}

test "block iterator handles multiple calls to next after exhaustion" {
    const allocator = testing.allocator;

    var sim_vfs = try simulation_vfs.SimulationVFS.init(allocator);
    defer sim_vfs.deinit();

    var engine = try StorageEngine.init_default(allocator, sim_vfs.vfs(), "/test/data");
    defer engine.deinit();
    try engine.initialize_storage();

    // Add single test block
    const block = ContextBlock{
        .id = BlockId.generate(),
        .version = 1,
        .source_uri = "test.zig",
        .metadata_json = "{}",
        .content = "test content",
    };
    try engine.put_block(block);

    var iterator = engine.iterate_all_blocks();
    defer iterator.deinit();

    // First call should return the block
    const first = try iterator.next();
    try testing.expect(first != null);

    // Subsequent calls should return null
    const second = try iterator.next();
    try testing.expectEqual(@as(?ContextBlock, null), second);

    const third = try iterator.next();
    try testing.expectEqual(@as(?ContextBlock, null), third);
}
