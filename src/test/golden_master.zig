//! Golden Master Testing Framework
//!
//! Provides deterministic reference output validation for recovery tests.
//! Captures database state as canonical snapshots that can be compared
//! across test runs to ensure recovery behavior is consistent and regression-free.

const std = @import("std");
const testing = std.testing;

// Direct imports since we're inside the kausaldb module
const storage = @import("../storage/engine.zig");
const types = @import("../core/types.zig");
const vfs = @import("../core/vfs.zig");

const StorageEngine = storage.StorageEngine;
const ContextBlock = types.ContextBlock;
const BlockId = types.BlockId;
const GraphEdge = types.GraphEdge;

/// Golden master snapshot of storage engine state
/// Captures all recoverable data in a canonical, deterministic format
const StorageSnapshot = struct {
    /// Total number of blocks recovered
    block_count: u32,
    /// Sorted list of all recovered blocks (by ID for determinism)
    blocks: []BlockSnapshot,
    /// Sorted list of all recovered edges (by source/target ID for determinism) 
    edges: []EdgeSnapshot,
    /// Storage metrics at time of snapshot
    metrics: MetricsSnapshot,

    const BlockSnapshot = struct {
        id: [32]u8, // Hex representation for readability
        version: u32,
        source_uri: []const u8,
        metadata_json: []const u8,
        content_hash: u64, // Hash of content for compact comparison
        content_length: usize,
    };

    const EdgeSnapshot = struct {
        source_id: [32]u8,
        target_id: [32]u8,
        edge_type: u16,
    };

    const MetricsSnapshot = struct {
        blocks_written: u64,
        blocks_read: u64,
        blocks_deleted: u64,
        wal_writes: u64,
        wal_flushes: u64,
        wal_recoveries: u64,
    };
};

/// Golden master configuration and state
pub const GoldenMaster = struct {
    allocator: std.mem.Allocator,
    test_name: []const u8,
    golden_dir: []const u8,

    const Self = @This();

    pub fn init(allocator: std.mem.Allocator, test_name: []const u8) !Self {
        // Create golden masters directory in test root
        const golden_dir = try std.fs.path.join(allocator, &[_][]const u8{ "tests", "golden_masters" });
        
        return Self{
            .allocator = allocator,
            .test_name = test_name,
            .golden_dir = golden_dir,
        };
    }

    pub fn deinit(self: *Self) void {
        self.allocator.free(self.golden_dir);
    }

    /// Capture current storage engine state as a snapshot
    pub fn capture_snapshot(self: Self, storage_engine: *const StorageEngine) !StorageSnapshot {
        var blocks = std.ArrayList(StorageSnapshot.BlockSnapshot).init(self.allocator);
        defer blocks.deinit();

        // TODO: Need to iterate through all blocks in storage engine
        // For now, this would require extending the StorageEngine API
        // to provide an iterator over all blocks
        
        const metrics = storage_engine.metrics();
        const metrics_snapshot = StorageSnapshot.MetricsSnapshot{
            .blocks_written = metrics.blocks_written.load(),
            .blocks_read = metrics.blocks_read.load(), 
            .blocks_deleted = metrics.blocks_deleted.load(),
            .wal_writes = metrics.wal_writes.load(),
            .wal_flushes = metrics.wal_flushes.load(),
            .wal_recoveries = metrics.wal_recoveries.load(),
        };

        return StorageSnapshot{
            .block_count = storage_engine.block_count(),
            .blocks = try blocks.toOwnedSlice(),
            .edges = &[_]StorageSnapshot.EdgeSnapshot{}, // TODO: Implement edge iteration
            .metrics = metrics_snapshot,
        };
    }

    /// Compare a snapshot against the stored golden master
    pub fn verify_snapshot(self: Self, snapshot: StorageSnapshot) !void {
        const golden_path = try self.get_golden_master_path();
        defer self.allocator.free(golden_path);

        // Try to read existing golden master
        const file_content = std.fs.cwd().readFileAlloc(
            self.allocator,
            golden_path,
            1024 * 1024, // 1MB max
        ) catch |err| switch (err) {
            error.FileNotFound => {
                // Create new golden master
                try self.save_snapshot(snapshot);
                std.debug.print(
                    "Created new golden master: {s}\n" ++
                    "This is the first run - future runs will validate against this snapshot.\n",
                    .{golden_path}
                );
                return;
            },
            else => return err,
        };
        defer self.allocator.free(file_content);

        // Parse and compare existing golden master
        const golden_snapshot = try self.parse_snapshot(file_content);
        try self.compare_snapshots(golden_snapshot, snapshot);
    }

    /// Save a snapshot as the golden master
    fn save_snapshot(self: Self, snapshot: StorageSnapshot) !void {
        const golden_path = try self.get_golden_master_path();
        defer self.allocator.free(golden_path);

        // Ensure golden masters directory exists
        const golden_dir = std.fs.path.dirname(golden_path) orelse return error.InvalidPath;
        try std.fs.cwd().makePath(golden_dir);

        // Serialize snapshot to canonical JSON format
        const json_content = try self.serialize_snapshot(snapshot);
        defer self.allocator.free(json_content);

        try std.fs.cwd().writeFile(.{
            .sub_path = golden_path,
            .data = json_content,
        });
    }

    /// Get path to golden master file for this test
    fn get_golden_master_path(self: Self) ![]u8 {
        return std.fmt.allocPrint(
            self.allocator,
            "{s}/{s}.golden.json",
            .{ self.golden_dir, self.test_name }
        );
    }

    /// Serialize snapshot to canonical JSON format
    fn serialize_snapshot(self: Self, snapshot: StorageSnapshot) ![]u8 {
        // Create a canonical JSON representation
        // Format is stable and deterministic for comparison
        var json_buf = std.ArrayList(u8).init(self.allocator);
        defer json_buf.deinit();

        const writer = json_buf.writer();
        try writer.print(
            \\{{
            \\  "block_count": {},
            \\  "metrics": {{
            \\    "blocks_written": {},
            \\    "blocks_read": {},
            \\    "blocks_deleted": {},
            \\    "wal_writes": {},
            \\    "wal_flushes": {},
            \\    "wal_recoveries": {}
            \\  }},
            \\  "blocks": [],
            \\  "edges": []
            \\}}
        , .{
            snapshot.block_count,
            snapshot.metrics.blocks_written,
            snapshot.metrics.blocks_read,
            snapshot.metrics.blocks_deleted,
            snapshot.metrics.wal_writes,
            snapshot.metrics.wal_flushes,
            snapshot.metrics.wal_recoveries,
        });

        return json_buf.toOwnedSlice();
    }

    /// Parse snapshot from JSON content  
    fn parse_snapshot(self: Self, json_content: []const u8) !StorageSnapshot {
        _ = self;
        
        // Simple JSON parsing - extract values using string search
        // For production, would use proper JSON parser
        
        const block_count = blk: {
            if (std.mem.indexOf(u8, json_content, "\"block_count\": ")) |start| {
                const value_start = start + "\"block_count\": ".len;
                if (std.mem.indexOfScalar(u8, json_content[value_start..], ',')) |comma_offset| {
                    const value_str = json_content[value_start..value_start + comma_offset];
                    break :blk std.fmt.parseInt(u32, value_str, 10) catch 0;
                }
            }
            break :blk 0;
        };
        
        const blocks_read = blk: {
            if (std.mem.indexOf(u8, json_content, "\"blocks_read\": ")) |start| {
                const value_start = start + "\"blocks_read\": ".len;
                if (std.mem.indexOfScalar(u8, json_content[value_start..], ',')) |comma_offset| {
                    const value_str = json_content[value_start..value_start + comma_offset];
                    break :blk std.fmt.parseInt(u64, value_str, 10) catch 0;
                }
            }
            break :blk 0;
        };
        
        return StorageSnapshot{
            .block_count = block_count,
            .blocks = &[_]StorageSnapshot.BlockSnapshot{},
            .edges = &[_]StorageSnapshot.EdgeSnapshot{},
            .metrics = .{
                .blocks_written = 0,
                .blocks_read = blocks_read,
                .blocks_deleted = 0,
                .wal_writes = 0,
                .wal_flushes = 0,
                .wal_recoveries = 0,
            },
        };
    }

    /// Compare two snapshots for equality
    fn compare_snapshots(self: Self, golden: StorageSnapshot, actual: StorageSnapshot) !void {
        _ = self;

        // Compare basic metrics
        if (golden.block_count != actual.block_count) {
            std.debug.print(
                "Block count mismatch: golden={}, actual={}\n",
                .{ golden.block_count, actual.block_count }
            );
            return error.GoldenMasterMismatch;
        }

        if (golden.metrics.wal_recoveries != actual.metrics.wal_recoveries) {
            std.debug.print(
                "WAL recoveries mismatch: golden={}, actual={}\n", 
                .{ golden.metrics.wal_recoveries, actual.metrics.wal_recoveries }
            );
            return error.GoldenMasterMismatch;
        }

        // TODO: Compare blocks and edges arrays
    }
};

/// Convenience function for recovery test golden master validation
pub fn verify_recovery_golden_master(
    allocator: std.mem.Allocator,
    test_name: []const u8,
    storage_engine: *const StorageEngine,
) !void {
    var golden_master = try GoldenMaster.init(allocator, test_name);
    defer golden_master.deinit();

    const snapshot = try golden_master.capture_snapshot(storage_engine);
    defer allocator.free(snapshot.blocks);

    try golden_master.verify_snapshot(snapshot);
}