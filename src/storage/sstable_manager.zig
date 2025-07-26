//! Manages the collection of on-disk SSTable files for the LSM-tree.
//!
//! Provides a single ownership boundary for all on-disk storage state including
//! SSTable discovery, read coordination, and compaction management. Follows
//! two-phase initialization pattern with I/O operations separated from object
//! creation for testability. Coordinates with TieredCompactionManager to
//! maintain optimal read performance through background compaction.

const std = @import("std");
const assert = @import("../core/assert.zig").assert;
const assert_fmt = @import("../core/assert.zig").assert_fmt;
const vfs = @import("../core/vfs.zig");
const context_block = @import("../core/types.zig");
const concurrency = @import("../core/concurrency.zig");

const sstable = @import("sstable.zig");
const tiered_compaction = @import("tiered_compaction.zig");

const VFS = vfs.VFS;
const ContextBlock = context_block.ContextBlock;
const BlockId = context_block.BlockId;
const SSTable = sstable.SSTable;
const TieredCompactionManager = tiered_compaction.TieredCompactionManager;

/// Manages the complete collection of on-disk SSTable files.
/// Provides single ownership boundary for all persistent storage state
/// including discovery, read coordination, and compaction management.
/// Uses two-phase initialization to separate object creation from I/O.
pub const SSTableManager = struct {
    backing_allocator: std.mem.Allocator,
    vfs: VFS,
    data_dir: []const u8,
    sstable_paths: std.ArrayList([]const u8),
    next_sstable_id: u32,
    compaction_manager: TieredCompactionManager,

    /// Phase 1: Create SSTable manager without I/O operations.
    /// Initializes data structures and prepares for startup phase.
    /// Follows CortexDB two-phase initialization pattern for testability.
    pub fn init(
        allocator: std.mem.Allocator,
        filesystem: VFS,
        data_dir: []const u8,
    ) SSTableManager {
        return SSTableManager{
            .backing_allocator = allocator,
            .vfs = filesystem,
            .data_dir = data_dir,
            .sstable_paths = std.ArrayList([]const u8).init(allocator),
            .next_sstable_id = 0,
            .compaction_manager = TieredCompactionManager.init(
                allocator,
                filesystem,
                data_dir,
            ),
        };
    }

    /// Clean up all SSTable manager resources including path strings.
    /// Must be called to prevent memory leaks. Coordinates cleanup of
    /// compaction manager and owned SSTable path strings.
    pub fn deinit(self: *SSTableManager) void {
        concurrency.assert_main_thread();

        // Clean up SSTable path strings
        for (self.sstable_paths.items) |sstable_path| {
            self.backing_allocator.free(sstable_path);
        }
        self.sstable_paths.deinit();

        self.compaction_manager.deinit();
    }

    /// Phase 2: Perform I/O operations to discover existing SSTables.
    /// Scans the SSTable directory and registers all existing files with
    /// the compaction manager. Creates directory structure if needed.
    pub fn startup(self: *SSTableManager) !void {
        concurrency.assert_main_thread();

        // Create SSTable directory if needed
        const sst_dir = try std.fmt.allocPrint(self.backing_allocator, "{s}/sst", .{self.data_dir});
        defer self.backing_allocator.free(sst_dir);

        if (!self.vfs.exists(sst_dir)) {
            try self.vfs.mkdir(sst_dir);
        }

        // Discover existing SSTables
        try self.discover_existing_sstables();
    }

    /// Find a block by searching all managed SSTables from newest to oldest.
    /// Implements LSM-tree read semantics by checking SSTables in reverse
    /// chronological order to ensure most recent version is returned.
    /// Uses query cache allocator for temporary SSTable operations.
    pub fn find_block_in_sstables(
        self: *SSTableManager,
        block_id: BlockId,
        query_cache: std.mem.Allocator,
    ) !?ContextBlock {
        // Search SSTables in reverse order (newest first)
        var i: usize = self.sstable_paths.items.len;
        while (i > 0) {
            i -= 1;
            const sstable_path = self.sstable_paths.items[i];

            var sstable_file = SSTable.init(query_cache, self.vfs, sstable_path);
            sstable_file.read_index() catch continue; // Skip corrupted SSTables
            defer sstable_file.deinit();

            if (try sstable_file.find_block(block_id)) |block| {
                return block;
            }
        }

        return null;
    }

    /// Create a new SSTable from a collection of blocks (memtable flush).
    /// Sorts blocks by ID for optimal SSTable layout and registers the new
    /// file with the compaction manager. Generates unique SSTable filename.
    pub fn create_new_sstable(self: *SSTableManager, blocks: []const ContextBlock) !void {
        concurrency.assert_main_thread();

        if (blocks.len == 0) return; // Nothing to flush

        // Generate unique SSTable filename
        const sstable_filename = try std.fmt.allocPrint(
            self.backing_allocator,
            "{s}/sst/sstable_{:04}.sst",
            .{ self.data_dir, self.next_sstable_id },
        );
        self.next_sstable_id += 1;

        // Create sorted copy for optimal SSTable layout
        const sorted_blocks = try self.backing_allocator.alloc(ContextBlock, blocks.len);
        defer self.backing_allocator.free(sorted_blocks);
        @memcpy(sorted_blocks, blocks);

        // Sort blocks by ID for efficient SSTable layout
        std.sort.pdq(ContextBlock, sorted_blocks, {}, struct {
            fn less_than(_: void, a: ContextBlock, b: ContextBlock) bool {
                return std.mem.lessThan(u8, &a.id.bytes, &b.id.bytes);
            }
        }.less_than);

        // Write SSTable atomically
        var new_sstable = SSTable.init(self.backing_allocator, self.vfs, sstable_filename);
        defer {
            // Manual cleanup to avoid double-free of file_path (owned by SSTableManager)
            new_sstable.index.deinit();
            if (new_sstable.bloom_filter) |*filter| {
                filter.deinit();
            }
        }
        try new_sstable.write_blocks(sorted_blocks);

        // Register new SSTable with manager and compaction
        try self.sstable_paths.append(sstable_filename);
        try self.compaction_manager.add_sstable(
            sstable_filename,
            try self.read_file_size(sstable_filename),
            0, // Level 0 for new SSTables
        );
    }

    /// Check if compaction is needed and run it if beneficial.
    /// Delegates to TieredCompactionManager for actual compaction logic.
    /// Called after new SSTable creation to maintain read performance.
    pub fn check_and_run_compaction(self: *SSTableManager) !void {
        concurrency.assert_main_thread();

        const compaction_job = self.compaction_manager.check_compaction_needed();
        if (compaction_job) |job| {
            try self.compaction_manager.execute_compaction(job);
        }
    }

    /// Get the total number of SSTables currently managed.
    /// Used for metrics and debugging. O(1) operation.
    pub fn sstable_count(self: *const SSTableManager) u32 {
        return @intCast(self.sstable_paths.items.len);
    }

    /// Get the next SSTable ID that will be assigned.
    /// Used for testing and debugging SSTable creation sequence.
    pub fn next_id(self: *const SSTableManager) u32 {
        return self.next_sstable_id;
    }

    // Private helper methods

    /// Discover existing SSTable files and register with compaction manager.
    /// Called during startup to restore system state after restart.
    /// Scans SSTable directory for .sst files and registers them in order.
    fn discover_existing_sstables(self: *SSTableManager) !void {
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
            const full_path = try std.fmt.allocPrint(
                self.backing_allocator,
                "{s}/{s}",
                .{ sst_dir, entry.name },
            );
            try self.sstable_paths.append(full_path);

            // Register with compaction manager
            const file_size = try self.read_file_size(full_path);
            try self.compaction_manager.add_sstable(full_path, file_size, 0);

            // Update next_sstable_id to avoid conflicts
            if (self.parse_sstable_id_from_path(entry.name)) |id| {
                if (id >= self.next_sstable_id) {
                    self.next_sstable_id = id + 1;
                }
            }
        }
    }

    /// Read file size for compaction manager registration.
    /// Uses VFS abstraction for testability with SimulationVFS.
    fn read_file_size(self: *SSTableManager, file_path: []const u8) !u64 {
        var file = try self.vfs.open(file_path, .read);
        defer file.close();
        return try file.file_size();
    }

    /// Extract SSTable ID from filename for conflict avoidance.
    /// Parses "sstable_NNNN.sst" format to determine next ID.
    /// Returns null if filename doesn't match expected pattern.
    fn parse_sstable_id_from_path(self: *SSTableManager, filename: []const u8) ?u32 {
        _ = self;

        // Expected format: "sstable_NNNN.sst"
        if (!std.mem.startsWith(u8, filename, "sstable_")) return null;
        if (!std.mem.endsWith(u8, filename, ".sst")) return null;

        const id_str = filename[8 .. filename.len - 4]; // Remove "sstable_" and ".sst"
        return std.fmt.parseInt(u32, id_str, 10) catch null;
    }
};

// Tests

const testing = std.testing;
const simulation_vfs = @import("../sim/simulation_vfs.zig");

test "SSTableManager two-phase initialization" {
    const allocator = testing.allocator;

    var sim_vfs = try simulation_vfs.SimulationVFS.init(allocator);
    defer sim_vfs.deinit();

    var manager = SSTableManager.init(allocator, sim_vfs.vfs(), "/test/data");
    defer manager.deinit();

    try testing.expectEqual(@as(u32, 0), manager.sstable_count());
    try testing.expectEqual(@as(u32, 0), manager.get_next_sstable_id());

    try manager.startup();
    try testing.expectEqual(@as(u32, 0), manager.sstable_count());
}

test "SSTableManager creates new SSTable from blocks" {
    const allocator = testing.allocator;

    var sim_vfs = try simulation_vfs.SimulationVFS.init(allocator);
    defer sim_vfs.deinit();

    var manager = SSTableManager.init(allocator, sim_vfs.vfs(), "/test/data");
    defer manager.deinit();

    try manager.startup();

    // Create test blocks
    const block1 = ContextBlock{
        .id = BlockId.generate(),
        .version = 1,
        .source_uri = "file://test1.zig",
        .metadata_json = "{}",
        .content = "test content 1",
    };

    const blocks = [_]ContextBlock{block1};
    try manager.create_new_sstable(&blocks);

    try testing.expectEqual(@as(u32, 1), manager.sstable_count());
    try testing.expectEqual(@as(u32, 1), manager.get_next_sstable_id());
}

test "SSTableManager finds blocks in SSTables" {
    const allocator = testing.allocator;

    var sim_vfs = try simulation_vfs.SimulationVFS.init(allocator);
    defer sim_vfs.deinit();

    var manager = SSTableManager.init(allocator, sim_vfs.vfs(), "/test/data");
    defer manager.deinit();

    try manager.startup();

    // Create and write test block
    const block_id = BlockId.generate();
    const block = ContextBlock{
        .id = block_id,
        .version = 1,
        .source_uri = "file://test.zig",
        .metadata_json = "{}",
        .content = "test content",
    };

    const blocks = [_]ContextBlock{block};
    try manager.create_new_sstable(&blocks);

    // Query cache for temporary allocations
    var query_arena = std.heap.ArenaAllocator.init(allocator);
    defer query_arena.deinit();

    // Find the block
    const found_block = try manager.find_block_in_sstables(block_id, query_arena.allocator());
    try testing.expect(found_block != null);
    try testing.expectEqualStrings("test content", found_block.?.content);

    // Try to find non-existent block
    const missing_block = try manager.find_block_in_sstables(BlockId.generate(), query_arena.allocator());
    try testing.expect(missing_block == null);
}
