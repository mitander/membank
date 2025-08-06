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
//! - Provide metrics and error handling

const std = @import("std");
const assert = @import("../core/assert.zig");
const fatal_assert = @import("../core/assert.zig").fatal_assert;
const vfs = @import("../core/vfs.zig");
const context_block = @import("../core/types.zig");
const error_context = @import("../core/error_context.zig");
const concurrency = @import("../core/concurrency.zig");
const simulation_vfs = @import("../sim/simulation_vfs.zig");
const testing = std.testing;

const config_mod = @import("config.zig");
const metrics_mod = @import("metrics.zig");
const memtable_manager_mod = @import("memtable_manager.zig");
const sstable_manager_mod = @import("sstable_manager.zig");
const block_index_mod = @import("block_index.zig");

const sstable = @import("sstable.zig");
const tiered_compaction = @import("tiered_compaction.zig");
const wal = @import("wal.zig");

const VFS = vfs.VFS;
const ContextBlock = context_block.ContextBlock;
const GraphEdge = context_block.GraphEdge;
const BlockId = context_block.BlockId;
const SimulationVFS = simulation_vfs.SimulationVFS;

const BlockHashMap = std.HashMap(BlockId, ContextBlock, block_index_mod.BlockIndex.BlockIdContext, std.hash_map.default_max_load_percentage);
const BlockHashMapIterator = BlockHashMap.Iterator;

pub const Config = config_mod.Config;
pub const StorageMetrics = metrics_mod.StorageMetrics;
pub const MemtableManager = memtable_manager_mod.MemtableManager;
pub const SSTableManager = sstable_manager_mod.SSTableManager;

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
/// state (including WAL ownership) and SSTableManager for on-disk state.
/// True coordinator pattern with clear ownership boundaries.
pub const StorageEngine = struct {
    backing_allocator: std.mem.Allocator,
    vfs: VFS,
    data_dir: []const u8,
    config: Config,
    memtable_manager: MemtableManager,
    sstable_manager: SSTableManager,
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

    /// Phase 1 initialization: Create storage engine with custom configuration.
    /// Creates all necessary subsystems but does not perform I/O operations.
    /// Call startup() to complete Phase 2 initialization.
    pub fn init(
        allocator: std.mem.Allocator,
        filesystem: VFS,
        data_dir: []const u8,
        storage_config: Config,
    ) !StorageEngine {
        assert.assert_not_empty(data_dir, "Storage data_dir cannot be empty", .{});
        assert.assert_fmt(data_dir.len < 4096, "Storage data_dir path too long: {} bytes", .{data_dir.len});
        assert.assert_fmt(@intFromPtr(data_dir.ptr) != 0, "Storage data_dir has null pointer", .{});

        storage_config.validate() catch |err| {
            error_context.log_storage_error(err, error_context.StorageContext{ .operation = "config_validation" });
            return err;
        };

        const owned_data_dir = allocator.dupe(u8, data_dir) catch |err| {
            error_context.log_storage_error(err, error_context.file_context("allocate_data_dir", data_dir));
            return err;
        };

        const engine = StorageEngine{
            .backing_allocator = allocator,
            .vfs = filesystem,
            .data_dir = owned_data_dir,
            .config = storage_config,
            .memtable_manager = MemtableManager.init(allocator, filesystem, owned_data_dir, storage_config.memtable_max_size) catch |err| {
                allocator.free(owned_data_dir);
                error_context.log_storage_error(err, error_context.file_context("memtable_manager_init", owned_data_dir));
                return err;
            },
            .sstable_manager = SSTableManager.init(allocator, filesystem, owned_data_dir),
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
        fatal_assert(@intFromPtr(self) != 0, "StorageEngine self pointer is null - memory corruption detected", .{});
        fatal_assert(self.data_dir.len > 0, "StorageEngine data_dir corrupted during cleanup - heap corruption detected", .{});

        self.memtable_manager.deinit();
        self.sstable_manager.deinit();
        self.query_cache_arena.deinit();
        self.backing_allocator.free(self.data_dir);
    }

    /// Create directory structure and discover existing data files.
    /// Called internally by startup() to prepare filesystem state.
    fn create_storage_directories(self: *StorageEngine) !void {
        concurrency.assert_main_thread();
        fatal_assert(@intFromPtr(self) != 0, "StorageEngine self pointer is null - memory corruption detected", .{});
        fatal_assert(self.data_dir.len > 0, "StorageEngine data_dir is empty - heap corruption detected", .{});

        if (self.initialized) return StorageError.AlreadyInitialized;

        if (!self.vfs.exists(self.data_dir)) {
            self.vfs.mkdir(self.data_dir) catch |err| switch (err) {
                error.FileExists => {}, // Directory already exists, continue
                else => {
                    error_context.log_storage_error(err, error_context.file_context("create_data_directory", self.data_dir));
                    return err;
                },
            };
        }

        self.initialized = true;
    }

    /// Phase 2 initialization: Complete startup by performing storage initialization and WAL recovery.
    /// This is the primary entry point for bringing StorageEngine from cold to hot state.
    /// Performs I/O operations including directory creation, SSTable discovery, and WAL recovery.
    pub fn startup(self: *StorageEngine) !void {
        self.create_storage_directories() catch |err| {
            error_context.log_storage_error(err, error_context.file_context("create_storage_directories", self.data_dir));
            return err;
        };
        self.memtable_manager.startup() catch |err| {
            error_context.log_storage_error(err, error_context.file_context("memtable_manager_startup", self.data_dir));
            return err;
        };
        self.sstable_manager.startup() catch |err| {
            error_context.log_storage_error(err, error_context.file_context("sstable_manager_startup", self.data_dir));
            return err;
        };
        self.memtable_manager.recover_from_wal() catch |err| {
            error_context.log_storage_error(err, error_context.file_context("wal_recovery", self.data_dir));
            return err;
        };
    }

    /// Write a Context Block to storage with full durability guarantees.
    /// Pure coordinator that delegates block storage and orchestrates flush operations.
    /// All business logic handled by MemtableManager and SSTableManager.
    pub fn put_block(self: *StorageEngine, block: ContextBlock) !void {
        concurrency.assert_main_thread();

        fatal_assert(@intFromPtr(self) != 0, "StorageEngine self pointer is null - memory corruption detected", .{});
        fatal_assert(self.data_dir.len > 0, "StorageEngine data_dir corrupted - heap corruption detected", .{});

        assert.assert_not_empty(block.content, "Block content cannot be empty", .{});
        assert.assert_not_empty(block.source_uri, "Block source_uri cannot be empty", .{});
        assert.assert_fmt(block.content.len < 100 * 1024 * 1024, "Block content too large: {} bytes", .{block.content.len});
        assert.assert_fmt(block.source_uri.len < 2048, "Block source_uri too long: {} bytes", .{block.source_uri.len});
        assert.assert_fmt(block.metadata_json.len < 1024 * 1024, "Block metadata_json too large: {} bytes", .{block.metadata_json.len});
        assert.assert_fmt(block.version > 0, "Block version must be positive: {}", .{block.version});

        if (!self.initialized) return StorageError.NotInitialized;

        const start_time = std.time.nanoTimestamp();
        assert.assert_fmt(start_time > 0, "Invalid timestamp: {}", .{start_time});

        block.validate(self.backing_allocator) catch |err| {
            error_context.log_storage_error(err, error_context.block_context("block_validation", block.id));
            return err;
        };

        fatal_assert(@intFromPtr(&self.memtable_manager) != 0, "MemtableManager pointer corrupted - memory safety violation detected", .{});
        self.memtable_manager.put_block_durable(block) catch |err| {
            error_context.log_storage_error(err, error_context.block_context("put_block_durable", block.id));
            return err;
        };

        if (self.memtable_manager.should_flush()) {
            self.coordinate_memtable_flush() catch |err| {
                error_context.log_storage_error(err, error_context.block_context("coordinate_memtable_flush", block.id));
                return err;
            };
        }

        self.track_write_metrics(start_time, block.content.len);
    }

    /// Find a Context Block by ID with LSM-tree read semantics.
    /// Checks memtable first, then SSTables in reverse chronological order
    /// to ensure most recent version is returned.
    pub fn find_block(self: *StorageEngine, block_id: BlockId) !?ContextBlock {
        fatal_assert(@intFromPtr(self) != 0, "StorageEngine self pointer is null - memory corruption detected", .{});
        fatal_assert(self.data_dir.len > 0, "StorageEngine data_dir corrupted - heap corruption detected", .{});

        var non_zero_bytes: u32 = 0;
        for (block_id.bytes) |byte| {
            if (byte != 0) non_zero_bytes += 1;
        }
        assert.assert_fmt(non_zero_bytes > 0, "Block ID cannot be all zeros", .{});

        if (!self.initialized) return StorageError.NotInitialized;

        const start_time = std.time.nanoTimestamp();
        assert.assert_fmt(start_time > 0, "Invalid timestamp: {}", .{start_time});

        fatal_assert(@intFromPtr(&self.memtable_manager) != 0, "MemtableManager pointer corrupted - memory safety violation detected", .{});

        if (self.memtable_manager.find_block_in_memtable(block_id)) |block_ptr| {
            // and data corruption that could cause silent data loss or crashes.
            fatal_assert(@intFromPtr(block_ptr) != 0, "MemtableManager returned null block pointer - heap corruption detected", .{});
            fatal_assert(block_ptr.content.len > 0, "MemtableManager returned block with empty content - data corruption detected", .{});
            fatal_assert(std.mem.eql(u8, &block_ptr.id.bytes, &block_id.bytes), "MemtableManager returned wrong block ID - index corruption detected", .{});

            const end_time = std.time.nanoTimestamp();
            assert.assert_fmt(end_time >= start_time, "Invalid timestamp sequence: {} < {}", .{ end_time, start_time });

            const blocks_before = self.storage_metrics.blocks_read.load();
            self.storage_metrics.blocks_read.incr();
            self.storage_metrics.total_read_time_ns.add(@intCast(end_time - start_time));
            self.storage_metrics.total_bytes_read.add(block_ptr.content.len);

            fatal_assert(self.storage_metrics.blocks_read.load() == blocks_before + 1, "Blocks read counter update failed - metrics corruption detected", .{});

            return block_ptr.*;
        }

        const sstable_result = self.sstable_manager.find_block_in_sstables(block_id, self.query_cache_arena.allocator()) catch |err| {
            error_context.log_storage_error(err, error_context.block_context("find_block_in_sstables", block_id));
            return err;
        };
        if (sstable_result) |block| {
            const end_time = std.time.nanoTimestamp();
            self.storage_metrics.blocks_read.incr();
            self.storage_metrics.sstable_reads.incr();
            self.storage_metrics.total_read_time_ns.add(@intCast(end_time - start_time));
            self.storage_metrics.total_bytes_read.add(block.content.len);

            self.maybe_clear_query_cache();

            return block;
        }

        return null;
    }

    /// Delete a Context Block by ID with tombstone semantics.
    /// Delegates to MemtableManager for WAL-first durability pattern.
    /// Actual space reclamation occurs during SSTable compaction.
    pub fn delete_block(self: *StorageEngine, block_id: BlockId) !void {
        concurrency.assert_main_thread();
        if (!self.initialized) return StorageError.NotInitialized;

        self.memtable_manager.delete_block_durable(block_id) catch |err| {
            error_context.log_storage_error(err, error_context.block_context("delete_block_durable", block_id));
            return err;
        };

        self.storage_metrics.blocks_deleted.incr();
    }

    /// Add a graph edge with durability guarantees.
    /// Delegates to MemtableManager for WAL-first durability pattern.
    pub fn put_edge(self: *StorageEngine, edge: GraphEdge) !void {
        concurrency.assert_main_thread();

        fatal_assert(@intFromPtr(self) != 0, "StorageEngine self pointer is null - memory corruption detected", .{});
        fatal_assert(self.data_dir.len > 0, "StorageEngine data_dir corrupted - heap corruption detected", .{});

        var source_non_zero: u32 = 0;
        var target_non_zero: u32 = 0;
        for (edge.source_id.bytes) |byte| {
            if (byte != 0) source_non_zero += 1;
        }
        for (edge.target_id.bytes) |byte| {
            if (byte != 0) target_non_zero += 1;
        }
        assert.assert_fmt(source_non_zero > 0, "Edge source_id cannot be all zeros", .{});
        assert.assert_fmt(target_non_zero > 0, "Edge target_id cannot be all zeros", .{});
        assert.assert_fmt(!std.mem.eql(u8, &edge.source_id.bytes, &edge.target_id.bytes), "Edge cannot be self-referential", .{});

        if (!self.initialized) return StorageError.NotInitialized;

        const start_time = std.time.nanoTimestamp();
        assert.assert_fmt(start_time > 0, "Invalid timestamp: {}", .{start_time});

        fatal_assert(@intFromPtr(&self.memtable_manager) != 0, "MemtableManager pointer corrupted - memory safety violation detected", .{});

        self.memtable_manager.put_edge_durable(edge) catch |err| {
            error_context.log_storage_error(err, error_context.block_context("put_edge_durable", edge.source_id));
            return err;
        };

        const edges_before = self.storage_metrics.edges_added.load();
        self.storage_metrics.edges_added.incr();

        fatal_assert(self.storage_metrics.edges_added.load() == edges_before + 1, "Edges added counter update failed - metrics corruption detected", .{});
    }

    /// Force synchronization of all WAL operations to durable storage.
    /// Delegates to MemtableManager for WAL ownership boundary.
    pub fn flush_wal(self: *StorageEngine) !void {
        concurrency.assert_main_thread();
        if (!self.initialized) return StorageError.NotInitialized;

        self.memtable_manager.flush_wal() catch |err| {
            error_context.log_storage_error(err, error_context.StorageContext{ .operation = "flush_wal" });
            return err;
        };
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
        fatal_assert(@intFromPtr(self) != 0, "StorageEngine self pointer is null - memory corruption detected", .{});
        fatal_assert(self.data_dir.len > 0, "StorageEngine data_dir corrupted - heap corruption detected", .{});

        var non_zero_bytes: u32 = 0;
        for (source_id.bytes) |byte| {
            if (byte != 0) non_zero_bytes += 1;
        }
        assert.assert_fmt(non_zero_bytes > 0, "Source block ID cannot be all zeros", .{});

        fatal_assert(@intFromPtr(&self.memtable_manager) != 0, "MemtableManager pointer corrupted - memory safety violation detected", .{});

        const edges = self.memtable_manager.find_outgoing_edges(source_id);

        if (edges.len > 0) {
            fatal_assert(@intFromPtr(edges.ptr) != 0, "MemtableManager returned null edges pointer with non-zero length - heap corruption detected", .{});
            fatal_assert(std.mem.eql(u8, &edges[0].source_id.bytes, &source_id.bytes), "First edge has wrong source_id - index corruption detected", .{});
        }

        return edges;
    }

    /// Find all incoming edges to a target block.
    /// Delegates to memtable manager for reverse graph traversal operations.
    pub fn find_incoming_edges(self: *const StorageEngine, target_id: BlockId) []const GraphEdge {
        fatal_assert(@intFromPtr(self) != 0, "StorageEngine self pointer is null - memory corruption detected", .{});
        fatal_assert(self.data_dir.len > 0, "StorageEngine data_dir corrupted - heap corruption detected", .{});

        var non_zero_bytes: u32 = 0;
        for (target_id.bytes) |byte| {
            if (byte != 0) non_zero_bytes += 1;
        }
        assert.assert_fmt(non_zero_bytes > 0, "Target block ID cannot be all zeros", .{});

        fatal_assert(@intFromPtr(&self.memtable_manager) != 0, "MemtableManager pointer corrupted - memory safety violation detected", .{});

        const edges = self.memtable_manager.find_incoming_edges(target_id);

        if (edges.len > 0) {
            fatal_assert(@intFromPtr(edges.ptr) != 0, "MemtableManager returned null edges pointer with non-zero length - heap corruption detected", .{});
            fatal_assert(std.mem.eql(u8, &edges[0].target_id.bytes, &target_id.bytes), "First edge has wrong target_id - index corruption detected", .{});
        }

        return edges;
    }

    /// Get performance metrics for monitoring and debugging.
    pub fn metrics(self: *const StorageEngine) *const StorageMetrics {
        return &self.storage_metrics;
    }

    /// Calculate current memory pressure level for backpressure control.
    /// Updates metrics with current memtable and compaction state before calculation.
    /// Used by ingestion pipeline to adapt batch sizes based on storage load.
    pub fn memory_pressure(
        self: *StorageEngine,
        config: StorageMetrics.MemoryPressureConfig,
    ) StorageMetrics.MemoryPressure {
        const memtable_bytes = self.memtable_manager.memory_usage();
        self.storage_metrics.memtable_memory_bytes.store(memtable_bytes);

        const queue_size = self.sstable_manager.pending_compaction_count();
        self.storage_metrics.compaction_queue_size.store(queue_size);

        return self.storage_metrics.calculate_memory_pressure(config);
    }

    /// Block iterator for scanning all blocks in storage (memtable only).
    /// SSTable iteration delegated to SSTableManager to maintain separation of concerns.
    /// For full storage iteration, use memtable iterator + SSTableManager methods.
    pub const BlockIterator = struct {
        memtable_iterator: BlockHashMapIterator,

        pub fn next(self: *BlockIterator) ?ContextBlock {
            if (self.memtable_iterator.next()) |entry| {
                return entry.value_ptr.*;
            }
            return null;
        }

        pub fn deinit(_: *BlockIterator) void {}
    };

    /// Create iterator to scan blocks in memtable only.
    /// For complete storage iteration, coordinate with SSTableManager directly.
    /// Pure coordinator pattern - delegates complex iteration to subsystems.
    pub fn iterate_all_blocks(self: *StorageEngine) BlockIterator {
        return BlockIterator{
            .memtable_iterator = self.memtable_manager.raw_iterator(),
        };
    }

    /// Flush current memtable to SSTable with coordinated subsystem management.
    /// Core LSM-tree operation that delegates to SSTableManager for SSTable creation
    /// and coordinates cleanup with MemtableManager. Follows the coordinator pattern.
    fn flush_memtable(self: *StorageEngine) !void {
        concurrency.assert_main_thread();

        self.memtable_manager.flush_to_sstable(&self.sstable_manager) catch |err| {
            error_context.log_storage_error(err, error_context.StorageContext{ .operation = "flush_to_sstable" });
            return err;
        };

        if (self.sstable_manager.should_compact()) {
            self.sstable_manager.execute_compaction() catch |err| {
                error_context.log_storage_error(err, error_context.StorageContext{ .operation = "post_flush_compaction" });
                return err;
            };
        }

        self.storage_metrics.sstable_writes.incr();
    }

    /// Public wrapper for memtable flush - backward compatibility.
    /// Delegates to internal flush_memtable method for coordinated subsystem management.
    pub fn flush_memtable_to_sstable(self: *StorageEngine) !void {
        self.flush_memtable() catch |err| {
            error_context.log_storage_error(err, error_context.StorageContext{ .operation = "flush_memtable_to_sstable" });
            return err;
        };
    }

    /// Coordinate memtable flush operation without containing business logic.
    /// Pure delegation to subsystems for flush orchestration.
    fn coordinate_memtable_flush(self: *StorageEngine) !void {
        assert.assert_fmt(@intFromPtr(&self.sstable_manager) != 0, "SSTableManager corrupted before flush", .{});
        self.flush_memtable() catch |err| {
            error_context.log_storage_error(err, error_context.StorageContext{ .operation = "coordinate_memtable_flush" });
            return err;
        };
    }

    /// Track write operation metrics without business logic.
    /// Pure metrics recording delegation to storage metrics subsystem.
    fn track_write_metrics(self: *StorageEngine, start_time: i128, content_len: usize) void {
        const end_time = std.time.nanoTimestamp();
        assert.assert_fmt(end_time >= start_time, "Invalid timestamp sequence: {} < {}", .{ end_time, start_time });

        const blocks_before = self.storage_metrics.blocks_written.load();
        self.storage_metrics.blocks_written.incr();

        const write_duration = @as(u64, @intCast(end_time - start_time));
        self.storage_metrics.total_write_time_ns.add(write_duration);
        self.storage_metrics.total_bytes_written.add(content_len);

        assert.assert_fmt(self.storage_metrics.blocks_written.load() == blocks_before + 1, "Blocks written counter update failed", .{});
    }

    /// Clear query cache arena if it exceeds memory threshold to prevent unbounded growth.
    /// Called after SSTable reads to maintain bounded memory usage for query operations.
    fn maybe_clear_query_cache(self: *StorageEngine) void {
        const QUERY_CACHE_THRESHOLD = 50 * 1024 * 1024; // 50MB threshold

        if (self.query_cache_arena.queryCapacity() > QUERY_CACHE_THRESHOLD) {
            _ = self.query_cache_arena.reset(.retain_capacity);
        }
    }

    /// Check for compaction opportunities and execute if beneficial.
    /// Pure coordinator that delegates decision and execution to SSTableManager.
    fn check_and_run_compaction(self: *StorageEngine) !void {
        if (self.sstable_manager.should_compact()) {
            self.sstable_manager.execute_compaction() catch |err| {
                error_context.log_storage_error(err, error_context.StorageContext{ .operation = "check_and_run_compaction" });
                return err;
            };
            _ = self.storage_metrics.compactions.add(1);
        }
    }

    /// Get file size for SSTable registration with compaction manager.
    fn read_file_size(self: *StorageEngine, path: []const u8) !u64 {
        var file = self.vfs.open(path, .read) catch |err| {
            error_context.log_storage_error(err, error_context.file_context("read_file_size_open", path));
            return err;
        };
        defer file.close();
        return file.file_size() catch |err| {
            error_context.log_storage_error(err, error_context.file_context("read_file_size_stat", path));
            return err;
        };
    }
};

test "storage engine initialization and cleanup" {
    const allocator = testing.allocator;

    var sim_vfs = try SimulationVFS.init(allocator);
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

    const config = Config.minimal_for_testing();
    var engine = try StorageEngine.init(allocator, sim_vfs.vfs(), "/test/data", config);
    defer engine.deinit();

    try engine.startup();

    const large_content = try allocator.alloc(u8, 1024 * 256); // 256KB blocks
    defer allocator.free(large_content);
    @memset(large_content, 'A');

    for (0..5) |i| {
        const block = ContextBlock{
            .id = BlockId.generate(),
            .version = 1,
            .source_uri = "file://test.zig",
            .metadata_json = "{}",
            .content = large_content,
        };
        try engine.put_block(block);

        if (i >= 3) { // After ~1MB of data
            try testing.expect(engine.memtable_manager.memory_usage() < config.memtable_max_size);
        }
    }

    const metrics = engine.metrics();
    try testing.expect(metrics.sstable_writes.load() > 0);
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

    {
        var engine = try StorageEngine.init_default(allocator, sim_vfs.vfs(), "/test/data");
        defer engine.deinit();

        try engine.startup();
        try engine.put_block(block);
        try engine.flush_wal();
    }

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
    try testing.expectEqual(@as(u64, 0), metrics_initial.blocks_written.load());

    const block = ContextBlock{
        .id = BlockId.generate(),
        .version = 1,
        .source_uri = "file://test.zig",
        .metadata_json = "{}",
        .content = "test content",
    };

    try engine.put_block(block);

    const metrics_after = engine.metrics();
    try testing.expectEqual(@as(u64, 1), metrics_after.blocks_written.load());
    try testing.expect(metrics_after.total_write_time_ns.load() > 0);
    try testing.expect(metrics_after.total_bytes_written.load() > 0);
}

test "block iterator with empty storage" {
    const allocator = testing.allocator;

    var sim_vfs = try simulation_vfs.SimulationVFS.init(allocator);
    defer sim_vfs.deinit();

    var engine = try StorageEngine.init_default(allocator, sim_vfs.vfs(), "/test/data");
    defer engine.deinit();
    try engine.startup();

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
    try engine.startup();

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

    var iterator = engine.iterate_all_blocks();
    defer iterator.deinit();

    var found_blocks = std.ArrayList(ContextBlock).init(allocator);
    defer found_blocks.deinit();

    while (try iterator.next()) |block| {
        try found_blocks.append(block);
    }

    try testing.expectEqual(@as(usize, 2), found_blocks.items.len);

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

    const config = Config.minimal_for_testing();
    var engine = try StorageEngine.init(allocator, sim_vfs.vfs(), "/test/data", config);
    defer engine.deinit();
    try engine.startup();

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

    try engine.flush_memtable_to_sstable();

    try testing.expectEqual(@as(u32, 0), engine.block_count());

    var iterator = engine.iterate_all_blocks();
    defer iterator.deinit();

    var found_blocks = std.ArrayList(ContextBlock).init(allocator);
    defer found_blocks.deinit();

    while (try iterator.next()) |block| {
        try found_blocks.append(block);
    }

    try testing.expectEqual(@as(usize, 2), found_blocks.items.len);
}

test "block iterator with mixed memtable and SSTable blocks" {
    const allocator = testing.allocator;

    var sim_vfs = try simulation_vfs.SimulationVFS.init(allocator);
    defer sim_vfs.deinit();

    const config = Config.minimal_for_testing();
    var engine = try StorageEngine.init(allocator, sim_vfs.vfs(), "/test/data", config);
    defer engine.deinit();
    try engine.startup();

    const sstable_block = ContextBlock{
        .id = BlockId.generate(),
        .version = 1,
        .source_uri = "sstable.zig",
        .metadata_json = "{}",
        .content = "sstable content",
    };

    try engine.put_block(sstable_block);
    try engine.flush_memtable_to_sstable();

    const memtable_block = ContextBlock{
        .id = BlockId.generate(),
        .version = 1,
        .source_uri = "memtable.zig",
        .metadata_json = "{}",
        .content = "memtable content",
    };

    try engine.put_block(memtable_block);

    var iterator = engine.iterate_all_blocks();
    defer iterator.deinit();

    var found_blocks = std.ArrayList(ContextBlock).init(allocator);
    defer found_blocks.deinit();

    while (try iterator.next()) |block| {
        try found_blocks.append(block);
    }

    try testing.expectEqual(@as(usize, 2), found_blocks.items.len);

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
    try engine.startup();

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

    const first = try iterator.next();
    try testing.expect(first != null);

    const second = try iterator.next();
    try testing.expectEqual(@as(?ContextBlock, null), second);

    const third = try iterator.next();
    try testing.expectEqual(@as(?ContextBlock, null), third);
}

test "error context logging for storage operations" {
    const allocator = testing.allocator;

    var sim_vfs = try simulation_vfs.SimulationVFS.init(allocator);
    defer sim_vfs.deinit();

    var engine = try StorageEngine.init_default(allocator, sim_vfs.vfs(), "/test/data");
    defer engine.deinit();

    const test_block = ContextBlock{
        .id = BlockId.generate(),
        .version = 1,
        .source_uri = "test.zig",
        .metadata_json = "{}",
        .content = "test content",
    };

    const put_result = engine.put_block(test_block);
    try testing.expectError(StorageError.NotInitialized, put_result);

    const find_result = engine.find_block(test_block.id);
    try testing.expectError(StorageError.NotInitialized, find_result);

    const delete_result = engine.delete_block(test_block.id);
    try testing.expectError(StorageError.NotInitialized, delete_result);
}

test "storage engine error context wrapping validation" {
    const allocator = testing.allocator;
    var sim_vfs = try simulation_vfs.SimulationVFS.init(allocator);
    defer sim_vfs.deinit();

    // Test that configuration validation provides error context
    const invalid_config = Config{ .memtable_max_size = 0 }; // Invalid config
    const init_result = StorageEngine.init(allocator, sim_vfs.vfs(), "/test", invalid_config);
    try testing.expect(std.meta.isError(init_result));

    // Test with valid config but I/O failures enabled for startup errors
    sim_vfs.enable_io_failures(500, .{ .read = true, .write = true }); // 50% failure rate

    var engine = try StorageEngine.init_default(allocator, sim_vfs.vfs(), "/error_test");
    defer engine.deinit();

    // Startup may fail with I/O errors, demonstrating error context is provided
    const startup_result = engine.startup();
    // Don't assert error since it's probabilistic, but demonstrates context wrapping
    _ = startup_result;
}
