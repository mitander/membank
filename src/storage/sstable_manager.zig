//! Manages the collection of on-disk SSTable files for the LSM-tree.
//!
//! Provides a single ownership boundary for all on-disk storage state including
//! SSTable discovery, read coordination, and compaction management. Follows
//! two-phase initialization pattern with I/O operations separated from object
//! creation for testability. Coordinates with TieredCompactionManager to
//! maintain optimal read performance through background compaction.

const std = @import("std");
const builtin = @import("builtin");
const log = std.log.scoped(.sstable_manager);
const assert = @import("../core/assert.zig").assert;
const assert_fmt = @import("../core/assert.zig").assert_fmt;
const fatal_assert = @import("../core/assert.zig").fatal_assert;
const vfs = @import("../core/vfs.zig");
const context_block = @import("../core/types.zig");
const concurrency = @import("../core/concurrency.zig");
const memory = @import("../core/memory.zig");

const ownership = @import("../core/ownership.zig");
const simulation_vfs = @import("../sim/simulation_vfs.zig");
const testing = std.testing;

// Test helper: Mock StorageEngine for unit tests

const sstable = @import("sstable.zig");
const tiered_compaction = @import("tiered_compaction.zig");

const VFS = vfs.VFS;
const ContextBlock = context_block.ContextBlock;
const BlockId = context_block.BlockId;
const SSTable = sstable.SSTable;
const TieredCompactionManager = tiered_compaction.TieredCompactionManager;
const SimulationVFS = simulation_vfs.SimulationVFS;

const OwnedBlock = ownership.OwnedBlock;
const BlockOwnership = ownership.BlockOwnership;
const ArenaCoordinator = memory.ArenaCoordinator;

/// Manages the complete collection of on-disk SSTable files with type-safe ownership.
/// Provides single ownership boundary for all persistent storage state
/// including discovery, read coordination, and compaction management.
/// Uses TypedArena for safe memory management and OwnedBlock for access control.
///
/// Arena Coordinator Pattern: SSTableManager receives stable coordinator interface
/// and uses it for ALL SSTable content allocation. Stable backing allocator
/// used for path strings and structures while content uses coordinator's arena.
pub const SSTableManager = struct {
    /// Arena coordinator pointer for stable allocation access (remains valid across arena resets)
    /// CRITICAL: Must be pointer to prevent coordinator struct copying corruption
    arena_coordinator: *const ArenaCoordinator,
    /// Stable backing allocator for path strings and data structures
    backing_allocator: std.mem.Allocator,
    vfs: VFS,
    data_dir: []const u8,
    sstable_paths: std.ArrayList([]const u8),
    next_sstable_id: u32,
    compaction_manager: TieredCompactionManager,

    /// Initialize SSTable manager following hierarchical memory model.
    /// Path ArrayList uses stable backing allocator while content uses parent's arena reference
    /// to prevent corruption from allocator conflicts during fault injection.
    /// CRITICAL: ArenaCoordinator must be passed by pointer to prevent struct copying corruption.
    pub fn init(
        coordinator: *const ArenaCoordinator,
        backing: std.mem.Allocator,
        filesystem: VFS,
        data_dir: []const u8,
    ) SSTableManager {
        return SSTableManager{
            .arena_coordinator = coordinator,
            .backing_allocator = backing,
            .vfs = filesystem,
            .data_dir = data_dir,
            .sstable_paths = std.ArrayList([]const u8).init(backing),
            .next_sstable_id = 0,
            .compaction_manager = TieredCompactionManager.init(
                coordinator,
                backing,
                filesystem,
                data_dir,
            ),
        };
    }

    /// Clean up SSTableManager resources following hierarchical model.
    /// Frees path strings and structures using backing allocator. Arena memory
    /// cleanup happens when StorageEngine resets its storage_arena.
    pub fn deinit(self: *SSTableManager) void {
        concurrency.assert_main_thread();

        // Free all our copied paths before deinitializing the ArrayList
        for (self.sstable_paths.items) |path| {
            self.backing_allocator.free(path);
        }
        self.sstable_paths.deinit();
        self.compaction_manager.deinit();
        // Arena memory is owned by coordinator - no local cleanup needed
    }

    /// Find path by index for external components (read-only access).
    /// This provides safe access to path strings without transferring ownership.
    /// Used by TieredCompactionManager and other components that need path references.
    pub fn find_path_by_index(self: *const SSTableManager, index: u32) ?[]const u8 {
        if (index >= self.sstable_paths.items.len) {
            return null;
        }
        return self.sstable_paths.items[index];
    }

    /// Register an SSTable path and return its index for reference management.
    /// This is the ONLY way to add paths to the system, ensuring single ownership.
    /// Returns the index that other components can use to reference this path.
    fn register_sstable_path(self: *SSTableManager, path: []const u8) !u32 {
        // Pre-allocate capacity for performance
        try self.sstable_paths.ensureTotalCapacity(self.sstable_paths.items.len + 1);
        // Create our own copy to ensure single ownership
        const path_copy = try self.backing_allocator.dupe(u8, path);
        try self.sstable_paths.append(path_copy);
        return @intCast(self.sstable_paths.items.len - 1);
    }

    /// Discover existing SSTables and register with compaction manager.
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

    /// Find a block by searching all managed SSTables with ownership validation.
    pub fn find_block_in_sstables(
        self: *SSTableManager,
        block_id: BlockId,
        accessor: BlockOwnership,
        query_cache: std.mem.Allocator,
    ) !?OwnedBlock {
        // Detect SSTable paths corruption and handle gracefully in fault injection tests
        if (self.sstable_paths.items.len > 1000000) {
            return error.CorruptedSSTablePaths;
        }

        // Comprehensive corruption detection for SSTable paths array
        fatal_assert(@intFromPtr(&self.sstable_paths) != 0, "SSTable paths ArrayList structure corrupted - null pointer", .{});

        if (@intFromPtr(self.sstable_paths.items.ptr) == 0 and self.sstable_paths.items.len > 0) {
            // SSTable paths array corruption detected - handle gracefully in fault injection scenarios
            return error.CorruptedSSTablePaths;
        }
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

            const sstable_path_copy = try query_cache.dupe(u8, sstable_path);
            var sstable_file = SSTable.init(self.arena_coordinator, query_cache, self.vfs, sstable_path_copy);

            // CRITICAL: Track SSTable index loading failures,
            sstable_file.read_index() catch |err| {
                log.warn("SSTable index loading failed for '{s}': {any} - block lookup will fail for this SSTable", .{ sstable_path, err });
                continue; // Skip this SSTable but log the failure
            };
            defer sstable_file.deinit();

            if (try sstable_file.find_block(block_id)) |block| {
                // Clone strings using coordinator's arena allocation methods
                const cloned_block = ContextBlock{
                    .id = block.block.id,
                    .version = block.block.version,
                    .source_uri = try self.arena_coordinator.duplicate_slice(u8, block.block.source_uri),
                    .metadata_json = try self.arena_coordinator.duplicate_slice(u8, block.block.metadata_json),
                    .content = try self.arena_coordinator.duplicate_slice(u8, block.block.content),
                };
                // Note: OwnedBlock.init still expects arena pointer for debug tracking
                // In hierarchical model, this is now coordinator's arena
                const owned_block = OwnedBlock.init(cloned_block, accessor, null);
                return owned_block;
            }
        }

        return null;
    }

    /// Create a new SSTable from memtable flush with implicit ownership validation.
    pub fn create_new_sstable_from_memtable(self: *SSTableManager, owned_blocks: []const OwnedBlock) !void {
        concurrency.assert_main_thread();

        if (owned_blocks.len == 0) return; // Nothing to flush

        // Validate that blocks can be read for SSTable creation from memtable
        for (owned_blocks) |*owned_block| {
            _ = owned_block.read(.memtable_manager);
        }

        return self.create_new_sstable_internal(owned_blocks, .memtable_manager);
    }

    /// Create a new SSTable from owned blocks with explicit access validation.
    pub fn create_new_sstable(
        self: *SSTableManager,
        owned_blocks: []const OwnedBlock,
        comptime accessor: BlockOwnership,
    ) !void {
        concurrency.assert_main_thread();

        if (owned_blocks.len == 0) return; // Nothing to flush
        for (owned_blocks) |*owned_block| {
            _ = owned_block.read(accessor); // Validates access permission
        }

        return self.create_new_sstable_internal(owned_blocks, accessor);
    }

    fn create_new_sstable_internal(
        self: *SSTableManager,
        owned_blocks: []const OwnedBlock,
        comptime accessor: BlockOwnership,
    ) !void {
        const sstable_filename = try std.fmt.allocPrint(
            self.backing_allocator,
            "{s}/sst/sstable_{:04}.sst",
            .{ self.data_dir, self.next_sstable_id },
        );
        // Free after all consumers have made their own copies
        defer self.backing_allocator.free(sstable_filename);
        self.next_sstable_id += 1;

        const sorted_blocks = try self.backing_allocator.alloc(ContextBlock, owned_blocks.len);
        defer self.backing_allocator.free(sorted_blocks);
        for (owned_blocks, 0..) |*owned_block, i| {
            sorted_blocks[i] = owned_block.read(accessor).*;
        }

        std.sort.pdq(ContextBlock, sorted_blocks, {}, struct {
            fn less_than(_: void, a: ContextBlock, b: ContextBlock) bool {
                return std.mem.lessThan(u8, &a.id.bytes, &b.id.bytes);
            }
        }.less_than);

        var new_sstable = SSTable.init(self.arena_coordinator, self.backing_allocator, self.vfs, sstable_filename);
        defer {
            new_sstable.index.deinit();
            if (new_sstable.bloom_filter) |*filter| {
                filter.deinit();
            }
        }
        try new_sstable.write_blocks(sorted_blocks);

        // Register path in single ownership system and get reference
        const path_index = try self.register_sstable_path(sstable_filename);
        const owned_path = self.sstable_paths.items[path_index];
        const file_size = try self.read_file_size(sstable_filename);

        // Pass owned path reference to compaction manager (no copying)
        // TieredCompactionManager will store reference, not copy
        try self.compaction_manager.add_sstable(
            owned_path,
            file_size,
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

    /// Get the total number of blocks across all SSTables.
    /// Used for system-wide statistics and validation.
    /// Iterates through all managed SSTables and sums their block counts.
    pub fn total_block_count(self: *const SSTableManager) u64 {
        var total: u64 = 0;

        for (self.sstable_paths.items) |sstable_path| {
            // Read block count directly from SSTable header without creating full SSTable instance
            const block_count = self.read_sstable_block_count(sstable_path) catch |err| {
                log.warn("Failed to read block count from '{s}': {any} - excluding from total", .{ sstable_path, err });
                continue;
            };
            total += block_count;
        }

        return total;
    }

    /// Read block count directly from SSTable header without full initialization.
    /// More efficient than creating a full SSTable instance for read-only access.
    fn read_sstable_block_count(self: *const SSTableManager, file_path: []const u8) !u32 {
        var file = try self.vfs.open(file_path, .read);
        defer file.close();

        // Read and validate the header to get block count safely
        var header_buffer: [64]u8 = undefined; // SSTable.HEADER_SIZE = 64
        const bytes_read = try file.read(&header_buffer);
        if (bytes_read < 64) return error.InvalidSSTableHeader;

        // Validate magic number and version (same as SSTable.Header.deserialize)
        var offset: usize = 0;

        // Check magic number to ensure this is a valid SSTable
        const magic = header_buffer[offset .. offset + 4];
        if (!std.mem.eql(u8, magic, "SSTB")) return error.InvalidMagic;
        offset += 4;

        // Check version compatibility
        const format_version = std.mem.readInt(u16, header_buffer[offset..][0..2], .little);
        if (format_version > 1) return error.UnsupportedVersion;
        offset += 2;

        // Skip flags (2 bytes) and index_offset (8 bytes)
        offset += 10;

        // Block count is at offset 16 in header for O(1) statistics without index loading
        const block_count = std.mem.readInt(u32, header_buffer[offset..][0..4], .little);

        // Validate block count is reasonable to catch corruption
        if (block_count > 100_000_000) return error.CorruptedBlockCount;

        return block_count;
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

            // Corruption tracking: Validate path before append
            if (builtin.mode == .Debug) {
                fatal_assert(full_path.len > 0 and full_path.len < 4096, "Invalid path length: {}, path: '{s}'", .{ full_path.len, full_path });
                fatal_assert(@intFromPtr(full_path.ptr) != 0, "Path pointer is null", .{});
            }

            // Corruption tracking: Validate ArrayList before append
            self.validate_sstable_paths_integrity("before append") catch |err| {
                log.err("CORRUPTION: ArrayList corrupted before appending path '{s}': {any}", .{ full_path, err });
                self.backing_allocator.free(full_path);
                return err;
            };

            // Register path in single ownership system and get index for reference
            const path_index = try self.register_sstable_path(full_path);

            // Free the temporary path since register_sstable_path made its own copy
            self.backing_allocator.free(full_path);

            // Get our owned copy for file operations
            const path_copy = self.sstable_paths.items[path_index];
            const file_size = try self.read_file_size(path_copy);

            // Pass owned path reference to compaction manager (no copying)
            // TieredCompactionManager will store reference, not copy
            try self.compaction_manager.add_sstable(path_copy, file_size, 0);

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

    /// Comprehensive ArrayList corruption detection and validation.
    /// Returns error if corruption is detected, logs detailed diagnostics.
    fn validate_sstable_paths_integrity(self: *const SSTableManager, context: []const u8) !void {
        // Basic structure validation
        if (self.sstable_paths.items.len > 1000000) {
            log.err("CORRUPTION DETECTED [{s}]: ArrayList len corrupted: {}", .{ context, self.sstable_paths.items.len });
            return error.CorruptedSSTablePaths;
        }

        if (@intFromPtr(&self.sstable_paths) == 0) {
            log.err("CORRUPTION DETECTED [{s}]: ArrayList structure pointer is null", .{context});
            return error.CorruptedSSTablePaths;
        }

        if (self.sstable_paths.items.len > 0) {
            if (@intFromPtr(self.sstable_paths.items.ptr) == 0) {
                log.err("CORRUPTION DETECTED [{s}]: ArrayList items pointer null with len={}", .{ context, self.sstable_paths.items.len });
                return error.CorruptedSSTablePaths;
            }

            if (self.sstable_paths.capacity < self.sstable_paths.items.len) {
                log.err("CORRUPTION DETECTED [{s}]: ArrayList capacity {} < len {}", .{ context, self.sstable_paths.capacity, self.sstable_paths.items.len });
                return error.CorruptedSSTablePaths;
            }
        }

        // Validate individual path strings
        for (self.sstable_paths.items, 0..) |path, i| {
            // Check for null or invalid length paths
            if (path.len == 0) {
                log.warn("CORRUPTION WARNING [{s}]: Empty path at index {}", .{ context, i });
                continue;
            }

            if (path.len > 4096) {
                log.err("CORRUPTION DETECTED [{s}]: Path length {} too large at index {}", .{ context, path.len, i });
                return error.CorruptedSSTablePaths;
            }

            if (@intFromPtr(path.ptr) == 0) {
                log.err("CORRUPTION DETECTED [{s}]: Null path pointer at index {}, len={}", .{ context, i, path.len });
                return error.CorruptedSSTablePaths;
            }

            // Check for obviously corrupted path content (non-printable chars)
            var printable_chars: u32 = 0;
            var total_chars: u32 = 0;
            for (path[0..@min(path.len, 64)]) |char| {
                total_chars += 1;
                if (char >= 32 and char <= 126) {
                    printable_chars += 1;
                }
            }

            if (total_chars > 0 and printable_chars * 100 / total_chars < 50) {
                log.err("CORRUPTION DETECTED [{s}]: Path at index {} has {}% printable chars, likely corrupted: '{any}'", .{ context, i, printable_chars * 100 / total_chars, path[0..@min(path.len, 32)] });
                return error.CorruptedSSTablePaths;
            }
        }

        // Log successful validation in debug mode
        if (builtin.mode == .Debug and self.sstable_paths.items.len > 0) {
            log.debug("ArrayList integrity OK [{s}]: len={}, cap={}, ptr={}, first_path='{s}'", .{ context, self.sstable_paths.items.len, self.sstable_paths.capacity, @intFromPtr(self.sstable_paths.items.ptr), if (self.sstable_paths.items.len > 0) self.sstable_paths.items[0] else "none" });
        }
    }

    /// Enhanced periodic validation that logs detailed memory state for corruption debugging.
    /// Call this from various points to track when corruption occurs.
    pub fn debug_validate_with_timing(
        self: *const SSTableManager,
        context: []const u8,
        operation_details: []const u8,
    ) void {
        if (builtin.mode != .Debug) return;

        const list_addr = @intFromPtr(&self.sstable_paths);
        const items_ptr = if (self.sstable_paths.items.len > 0) @intFromPtr(self.sstable_paths.items.ptr) else 0;

        log.debug("TIMING CHECK [{s}] {s}: ArrayList@0x{X} len={} cap={} items@0x{X}", .{ context, operation_details, list_addr, self.sstable_paths.items.len, self.sstable_paths.capacity, items_ptr });

        // Quick corruption check without error propagation
        if (self.sstable_paths.items.len > 1000000) {
            log.err("CORRUPTION TIMING [{s}]: ArrayList len corrupted to {} during {s}", .{ context, self.sstable_paths.items.len, operation_details });
            return;
        }

        // Sample first few paths for corruption
        const sample_count = @min(self.sstable_paths.items.len, 3);
        for (self.sstable_paths.items[0..sample_count], 0..) |path, i| {
            if (path.len > 4096) {
                log.err("CORRUPTION TIMING [{s}]: Path {} corrupted (len={}) during {s}", .{ context, i, path.len, operation_details });
                return;
            }

            if (path.len > 0) {
                var printable: u32 = 0;
                const check_len = @min(path.len, 16);
                for (path[0..check_len]) |c| {
                    if (c >= 32 and c <= 126) printable += 1;
                }
                if (printable * 100 / check_len < 50) {
                    log.err("CORRUPTION TIMING [{s}]: Path {} has garbled content during {s}: {any}", .{ context, i, operation_details, path[0..check_len] });
                    return;
                }
            }
        }
    }

    /// P0.6 & P0.7: Comprehensive invariant validation for SSTableManager.
    /// Validates arena coordinator stability, path integrity, and compaction state.
    pub fn validate_invariants(self: *const SSTableManager) void {
        if (builtin.mode == .Debug) {
            self.validate_sstable_paths_integrity("invariant_validation") catch |err| {
                fatal_assert(false, "SSTableManager path integrity validation failed: {}", .{err});
            };
            self.validate_arena_coordinator_stability();
            self.validate_compaction_manager_coherence();
        }
    }

    /// P0.6: Validate arena coordinator stability.
    fn validate_arena_coordinator_stability(self: *const SSTableManager) void {
        assert_fmt(builtin.mode == .Debug, "Arena coordinator validation should only run in debug builds", .{});

        // Arena coordinator corruption indicates struct copying which breaks allocation interface
        fatal_assert(@intFromPtr(self.arena_coordinator) != 0, "Arena coordinator pointer is null - struct copying corruption", .{});

        // Minimal allocation test verifies coordinator hasn't been corrupted by struct copying
        const test_alloc = self.arena_coordinator.alloc(u8, 1) catch {
            fatal_assert(false, "SSTableManager arena coordinator non-functional - corruption detected", .{});
            return;
        };
        _ = test_alloc; // Arena will clean up during next reset

        // Validate backing allocator is functional
        const test_backing_alloc = self.backing_allocator.alloc(u8, 1) catch {
            fatal_assert(false, "SSTableManager backing allocator non-functional", .{});
            return;
        };
        defer self.backing_allocator.free(test_backing_alloc);
    }

    /// Validate compaction manager state coherence.
    fn validate_compaction_manager_coherence(self: *const SSTableManager) void {
        assert_fmt(builtin.mode == .Debug, "Compaction manager validation should only run in debug builds", .{});

        // Compaction manager corruption would break background maintenance operations
        fatal_assert(@intFromPtr(&self.compaction_manager) != 0, "CompactionManager pointer corruption", .{});

        // Excessive SSTable count indicates counter corruption or runaway file creation
        const current_sstable_count = self.sstable_count();
        assert_fmt(current_sstable_count < 10000, "SSTable count {} is unreasonable - indicates corruption", .{current_sstable_count});

        // ID overflow or corruption could cause file naming conflicts and data loss
        assert_fmt(self.next_sstable_id < 1000000, "Next SSTable ID {} is unreasonable - indicates corruption", .{self.next_sstable_id});

        // Validate data directory is set and reasonable
        if (self.data_dir.len > 0) {
            assert_fmt(self.data_dir.len < 4096, "Data directory path too long: {} bytes", .{self.data_dir.len});
            assert_fmt(@intFromPtr(self.data_dir.ptr) != 0, "Data directory pointer is null with length {}", .{self.data_dir.len});
        }
    }
};

test "SSTableManager two-phase initialization" {
    const allocator = testing.allocator;

    var sim_vfs = try SimulationVFS.init(allocator);
    defer sim_vfs.deinit();

    // Hierarchical memory model: create arena for content, use backing for structure
    var arena = std.heap.ArenaAllocator.init(allocator);
    defer arena.deinit();
    const coordinator = ArenaCoordinator.init(&arena);
    var manager = SSTableManager.init(&coordinator, allocator, sim_vfs.vfs(), "/test/data");
    defer manager.deinit();

    try testing.expectEqual(@as(u32, 0), manager.sstable_count());
    try testing.expectEqual(@as(u32, 0), manager.next_id());

    try manager.startup();
    try testing.expectEqual(@as(u32, 0), manager.sstable_count());
}

test "SSTableManager creates new SSTable from owned blocks" {
    const allocator = testing.allocator;

    var sim_vfs = try simulation_vfs.SimulationVFS.init(allocator);
    defer sim_vfs.deinit();

    // Hierarchical memory model: create arena for content, use backing for structure
    var arena = std.heap.ArenaAllocator.init(allocator);
    defer arena.deinit();
    const coordinator = ArenaCoordinator.init(&arena);
    var manager = SSTableManager.init(&coordinator, allocator, sim_vfs.vfs(), "/test/data");
    defer manager.deinit();

    try manager.startup();

    const block1 = ContextBlock{
        .id = BlockId.generate(),
        .version = 1,
        .source_uri = "file://test1.zig",
        .metadata_json = "{}",
        .content = "test content 1",
    };

    const owned_block1 = OwnedBlock.init(block1, .simulation_test, null);
    const owned_blocks = [_]OwnedBlock{owned_block1};
    try manager.create_new_sstable(&owned_blocks, .simulation_test);

    try testing.expectEqual(@as(u32, 1), manager.sstable_count());
    try testing.expectEqual(@as(u32, 1), manager.next_id());
}

test "SSTableManager finds owned blocks in SSTables" {
    const allocator = testing.allocator;

    var sim_vfs = try simulation_vfs.SimulationVFS.init(allocator);
    defer sim_vfs.deinit();

    // Hierarchical memory model: create arena for content, use backing for structure
    var arena = std.heap.ArenaAllocator.init(allocator);
    defer arena.deinit();
    const coordinator = ArenaCoordinator.init(&arena);
    var manager = SSTableManager.init(&coordinator, allocator, sim_vfs.vfs(), "/test/data");
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

    const owned_block = OwnedBlock.init(block, .simulation_test, null);
    const owned_blocks = [_]OwnedBlock{owned_block};
    try manager.create_new_sstable(&owned_blocks, .simulation_test);

    var query_arena = std.heap.ArenaAllocator.init(allocator);
    defer query_arena.deinit();

    const found_owned_block = try manager.find_block_in_sstables(block_id, .simulation_test, query_arena.allocator());
    try testing.expect(found_owned_block != null);
    const found_block = found_owned_block.?.read(.simulation_test);
    try testing.expectEqualStrings("test content", found_block.content);

    const missing_block = try manager.find_block_in_sstables(BlockId.generate(), .simulation_test, query_arena.allocator());
    try testing.expect(missing_block == null);
}

test "SSTableManager total_block_count aggregates across SSTables" {
    const allocator = testing.allocator;

    var sim_vfs = try simulation_vfs.SimulationVFS.init(allocator);
    defer sim_vfs.deinit();

    var arena = std.heap.ArenaAllocator.init(allocator);
    defer arena.deinit();
    const coordinator = ArenaCoordinator.init(&arena);
    var manager = SSTableManager.init(&coordinator, allocator, sim_vfs.vfs(), "/test/data");
    defer manager.deinit();

    try manager.startup();

    // Initially no blocks
    try testing.expectEqual(@as(u64, 0), manager.total_block_count());

    // Create first SSTable with 2 blocks
    const block1 = ContextBlock{
        .id = BlockId.generate(),
        .version = 1,
        .source_uri = "file://test1.zig",
        .metadata_json = "{}",
        .content = "test content 1",
    };
    const block2 = ContextBlock{
        .id = BlockId.generate(),
        .version = 1,
        .source_uri = "file://test2.zig",
        .metadata_json = "{}",
        .content = "test content 2",
    };

    const owned_block1 = OwnedBlock.init(block1, .simulation_test, null);
    const owned_block2 = OwnedBlock.init(block2, .simulation_test, null);
    const first_sstable_blocks = [_]OwnedBlock{ owned_block1, owned_block2 };
    try manager.create_new_sstable(&first_sstable_blocks, .simulation_test);

    // Should have 2 blocks total
    try testing.expectEqual(@as(u64, 2), manager.total_block_count());

    // Create second SSTable with 1 block
    const block3 = ContextBlock{
        .id = BlockId.generate(),
        .version = 1,
        .source_uri = "file://test3.zig",
        .metadata_json = "{}",
        .content = "test content 3",
    };

    const owned_block3 = OwnedBlock.init(block3, .simulation_test, null);
    const second_sstable_blocks = [_]OwnedBlock{owned_block3};
    try manager.create_new_sstable(&second_sstable_blocks, .simulation_test);

    // Should have 3 blocks total across both SSTables
    try testing.expectEqual(@as(u64, 3), manager.total_block_count());
}
