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
const fatal_assert = @import("../core/assert.zig").fatal_assert;
const vfs = @import("../core/vfs.zig");
const context_block = @import("../core/types.zig");
const concurrency = @import("../core/concurrency.zig");
const simulation_vfs = @import("../sim/simulation_vfs.zig");
const testing = std.testing;

const sstable = @import("sstable.zig");
const tiered_compaction = @import("tiered_compaction.zig");

const VFS = vfs.VFS;
const ContextBlock = context_block.ContextBlock;
const BlockId = context_block.BlockId;
const SSTable = sstable.SSTable;
const TieredCompactionManager = tiered_compaction.TieredCompactionManager;
const SimulationVFS = simulation_vfs.SimulationVFS;

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
    /// Follows KausalDB two-phase initialization pattern for testability.
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

        var sst_dir_buffer: [512]u8 = undefined;
        const sst_dir = try std.fmt.bufPrint(sst_dir_buffer[0..], "{s}/sst", .{self.data_dir});

        if (!self.vfs.exists(sst_dir)) {
            self.vfs.mkdir(sst_dir) catch |err| switch (err) {
                error.FileExists => {}, // Directory already exists, continue
                else => return err,
            };
        }

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
        // Comprehensive corruption detection for SSTable paths array
        fatal_assert(@intFromPtr(&self.sstable_paths) != 0, "SSTable paths ArrayList structure corrupted - null pointer", .{});
        fatal_assert(@intFromPtr(self.sstable_paths.items.ptr) != 0 or self.sstable_paths.items.len == 0, "SSTable paths array has null pointer with non-zero length: {} - heap corruption detected", .{self.sstable_paths.items.len});
        fatal_assert(self.sstable_paths.capacity >= self.sstable_paths.items.len, "SSTable paths capacity {} < length {} - ArrayList corruption", .{ self.sstable_paths.capacity, self.sstable_paths.items.len });

        var i: usize = self.sstable_paths.items.len;
        while (i > 0) {
            i -= 1;

            // Validate array bounds and pointer consistency before each access
            fatal_assert(i < self.sstable_paths.items.len, "SSTable index out of bounds: {} >= {} - memory corruption detected", .{ i, self.sstable_paths.items.len });
            fatal_assert(@intFromPtr(self.sstable_paths.items.ptr) != 0, "SSTable paths array pointer became null during iteration", .{});

            const sstable_path = self.sstable_paths.items[i];

            // Validate the path string itself
            fatal_assert(@intFromPtr(sstable_path.ptr) != 0 or sstable_path.len == 0, "SSTable path[{}] has null pointer with length {} - string corruption", .{ i, sstable_path.len });
            fatal_assert(sstable_path.len < 4096, "SSTable path[{}] has suspicious length {} - possible corruption", .{ i, sstable_path.len });

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

        const sstable_filename = try std.fmt.allocPrint(
            self.backing_allocator,
            "{s}/sst/sstable_{:04}.sst",
            .{ self.data_dir, self.next_sstable_id },
        );
        self.next_sstable_id += 1;

        const sorted_blocks = try self.backing_allocator.alloc(ContextBlock, blocks.len);
        defer self.backing_allocator.free(sorted_blocks);
        @memcpy(sorted_blocks, blocks);

        std.sort.pdq(ContextBlock, sorted_blocks, {}, struct {
            fn less_than(_: void, a: ContextBlock, b: ContextBlock) bool {
                return std.mem.lessThan(u8, &a.id.bytes, &b.id.bytes);
            }
        }.less_than);

        var new_sstable = SSTable.init(self.backing_allocator, self.vfs, sstable_filename);
        defer {
            new_sstable.index.deinit();
            if (new_sstable.bloom_filter) |*filter| {
                filter.deinit();
            }
        }
        try new_sstable.write_blocks(sorted_blocks);

        try self.sstable_paths.append(sstable_filename);
        try self.compaction_manager.add_sstable(
            sstable_filename,
            try self.read_file_size(sstable_filename),
            0, // Level 0 for new SSTables
        );
    }

    /// Check if compaction is beneficial based on SSTable collection state.
    /// Delegates decision logic to TieredCompactionManager without executing.
    /// Pure decision method for coordinator pattern compliance.
    pub fn should_compact(self: *SSTableManager) bool {
        if (self.compaction_manager.check_compaction_needed() catch null) |job| {
            var mutable_job = job;
            defer mutable_job.deinit();
            return true;
        }
        return false;
    }

    /// Execute compaction operation without decision logic.
    /// Delegates to TieredCompactionManager for actual compaction execution.
    /// Called by coordinator after should_compact() returns true.
    pub fn execute_compaction(self: *SSTableManager) !void {
        concurrency.assert_main_thread();

        const compaction_job = try self.compaction_manager.check_compaction_needed();
        if (compaction_job) |job| {
            try self.compaction_manager.execute_compaction(job);
        }
    }

    /// Check if compaction is needed and run it if beneficial.
    /// Delegates to TieredCompactionManager for actual compaction logic.
    /// Called after new SSTable creation to maintain read performance.
    pub fn check_and_run_compaction(self: *SSTableManager) !void {
        concurrency.assert_main_thread();

        if (self.should_compact()) {
            try self.execute_compaction();
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

    /// Get the number of SSTables pending compaction.
    /// Used for memory pressure calculation in backpressure control.
    /// Returns the current SSTable count as a proxy for compaction pressure.
    pub fn pending_compaction_count(self: *const SSTableManager) u64 {
        // For now, use total SSTable count as compaction pressure indicator
        // More sophisticated compaction management would track actual pending jobs
        return @intCast(self.sstable_paths.items.len);
    }

    /// Discover existing SSTable files and register with compaction manager.
    /// Called during startup to restore system state after restart.
    /// Scans SSTable directory for .sst files and registers them in order.
    fn discover_existing_sstables(self: *SSTableManager) !void {
        var sst_dir_buffer: [512]u8 = undefined;
        const sst_dir = try std.fmt.bufPrint(sst_dir_buffer[0..], "{s}/sst", .{self.data_dir});

        if (!self.vfs.exists(sst_dir)) {
            return;
        }

        var dir_iter = try self.vfs.iterate_directory(sst_dir, self.backing_allocator);
        defer dir_iter.deinit(self.backing_allocator);

        while (dir_iter.next()) |entry| {
            if (entry.kind != .file) continue;

            const extension = std.fs.path.extension(entry.name);
            if (!std.mem.eql(u8, extension, ".sst")) continue;

            const full_path = try std.fmt.allocPrint(
                self.backing_allocator,
                "{s}/{s}",
                .{ sst_dir, entry.name },
            );
            try self.sstable_paths.append(full_path);

            const file_size = try self.read_file_size(full_path);
            try self.compaction_manager.add_sstable(full_path, file_size, 0);
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

        if (!std.mem.startsWith(u8, filename, "sstable_")) return null;
        if (!std.mem.endsWith(u8, filename, ".sst")) return null;

        const id_str = filename[8 .. filename.len - 4];
        return std.fmt.parseInt(u32, id_str, 10) catch null;
    }
};

test "SSTableManager two-phase initialization" {
    const allocator = testing.allocator;

    var sim_vfs = try SimulationVFS.init(allocator);
    defer sim_vfs.deinit();

    var manager = SSTableManager.init(allocator, sim_vfs.vfs(), "/test/data");
    defer manager.deinit();

    try testing.expectEqual(@as(u32, 0), manager.sstable_count());
    try testing.expectEqual(@as(u32, 0), manager.next_id());

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
    try testing.expectEqual(@as(u32, 1), manager.next_id());
}

test "SSTableManager finds blocks in SSTables" {
    const allocator = testing.allocator;

    var sim_vfs = try simulation_vfs.SimulationVFS.init(allocator);
    defer sim_vfs.deinit();

    var manager = SSTableManager.init(allocator, sim_vfs.vfs(), "/test/data");
    defer manager.deinit();

    try manager.startup();

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

    var query_arena = std.heap.ArenaAllocator.init(allocator);
    defer query_arena.deinit();

    const found_block = try manager.find_block_in_sstables(block_id, query_arena.allocator());
    try testing.expect(found_block != null);
    try testing.expectEqualStrings("test content", found_block.?.content);
    const missing_block = try manager.find_block_in_sstables(BlockId.generate(), query_arena.allocator());
    try testing.expect(missing_block == null);
}
