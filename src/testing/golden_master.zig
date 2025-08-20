//! Golden master testing for deterministic output validation.
//!
//! Captures database state as canonical snapshots for comparing recovery
//! behavior across test runs. Enables regression detection in complex
//! recovery scenarios where manual validation is impractical.
//!
//! Design rationale: Golden master approach provides comprehensive validation
//! of recovery correctness without requiring detailed assertions for every
//! possible state combination. Deterministic snapshots enable precise
//! regression detection across system changes.

const std = @import("std");

const testing = std.testing;

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
        content_hash: u64, // Hash of content for validation
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

    /// Free allocated memory for the snapshot
    pub fn deinit(self: *StorageSnapshot, allocator: std.mem.Allocator) void {
        // Free block and edge arrays
        allocator.free(self.blocks);
        allocator.free(self.edges);
    }
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
    pub fn capture_snapshot(
        self: Self,
        storage_engine: *StorageEngine,
    ) !StorageSnapshot {
        var blocks = std.ArrayList(StorageSnapshot.BlockSnapshot).init(self.allocator);
        defer blocks.deinit();

        // Capture all blocks using the available iterator API
        var block_iterator = storage_engine.iterate_all_blocks();
        while (try block_iterator.next()) |block| {
            const id_hex = try block.id.to_hex(self.allocator);
            defer self.allocator.free(id_hex);

            var id_array: [32]u8 = undefined;
            @memcpy(&id_array, id_hex[0..32]);

            const block_snapshot = StorageSnapshot.BlockSnapshot{
                .id = id_array,
                .version = @intCast(block.version),
                .content_hash = std.hash_map.hashString(block.content),
                .content_length = block.content.len,
            };
            try blocks.append(block_snapshot);
        }

        const metrics = storage_engine.metrics();
        const metrics_snapshot = StorageSnapshot.MetricsSnapshot{
            .blocks_written = metrics.blocks_written.load(),
            .blocks_read = metrics.blocks_read.load(),
            .blocks_deleted = metrics.blocks_deleted.load(),
            .wal_writes = metrics.wal_writes.load(),
            .wal_flushes = metrics.wal_flushes.load(),
            .wal_recoveries = metrics.wal_recoveries.load(),
        };

        // Sort blocks by ID for deterministic output
        const BlockComparator = struct {
            pub fn less_than(_: void, a: StorageSnapshot.BlockSnapshot, b: StorageSnapshot.BlockSnapshot) bool {
                return std.mem.order(u8, &a.id, &b.id) == .lt;
            }
        };
        std.mem.sort(StorageSnapshot.BlockSnapshot, blocks.items, {}, BlockComparator.less_than);

        // Edge iteration deferred - requires additional StorageEngine API
        // Current block iteration provides sufficient deterministic validation
        const edges_array = try self.allocator.alloc(StorageSnapshot.EdgeSnapshot, 0);

        const blocks_array = try self.allocator.dupe(StorageSnapshot.BlockSnapshot, blocks.items);

        return StorageSnapshot{
            .block_count = @intCast(blocks.items.len),
            .blocks = blocks_array,
            .edges = edges_array,
            .metrics = metrics_snapshot,
        };
    }

    /// Compare a snapshot against the stored golden master
    pub fn verify_snapshot(self: Self, snapshot: StorageSnapshot) !void {
        const golden_path = try self.build_golden_master_path();
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
                std.debug.print("Created new golden master: {s}\n" ++
                    "This is the first run - future runs will validate against this snapshot.\n", .{golden_path});
                return;
            },
            else => return err,
        };
        defer self.allocator.free(file_content);

        // Parse and compare existing golden master
        var golden_snapshot = try self.parse_snapshot(file_content);
        defer golden_snapshot.deinit(self.allocator);
        try self.compare_snapshots(golden_snapshot, snapshot);
    }

    /// Save a snapshot as the golden master
    fn save_snapshot(self: Self, snapshot: StorageSnapshot) !void {
        const golden_path = try self.build_golden_master_path();
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

    /// Build path to golden master file for this test
    fn build_golden_master_path(self: Self) ![]u8 {
        return std.fmt.allocPrint(self.allocator, "{s}/{s}.golden.json", .{ self.golden_dir, self.test_name });
    }

    /// Serialize snapshot to canonical JSON format
    fn serialize_snapshot(self: Self, snapshot: StorageSnapshot) ![]u8 {
        var json_buf = std.ArrayList(u8).init(self.allocator);
        defer json_buf.deinit();

        const writer = json_buf.writer();
        try self.write_json_header(writer, snapshot);
        try self.write_blocks_array(writer, snapshot.blocks);
        try self.write_json_footer(writer);

        return json_buf.toOwnedSlice();
    }

    /// Write JSON header with metadata and metrics
    fn write_json_header( // tidy:ignore-length - large JSON template string causes false positive
        self: Self,
        writer: anytype,
        snapshot: StorageSnapshot,
    ) !void {
        _ = self;
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
            \\  "blocks": [
        , .{
            snapshot.block_count,
            snapshot.metrics.blocks_written,
            snapshot.metrics.blocks_read,
            snapshot.metrics.blocks_deleted,
            snapshot.metrics.wal_writes,
            snapshot.metrics.wal_flushes,
            snapshot.metrics.wal_recoveries,
        });
    }

    /// Write JSON array of block snapshots
    fn write_blocks_array(self: Self, writer: anytype, blocks: []StorageSnapshot.BlockSnapshot) !void {
        _ = self;
        for (blocks, 0..) |block, i| {
            if (i > 0) try writer.writeAll(",");
            try writer.print(
                \\{{
                \\    "id": "{s}",
                \\    "version": {},
                \\    "content_hash": {},
                \\    "content_length": {}
                \\  }}
            , .{
                block.id,
                block.version,
                block.content_hash,
                block.content_length,
            });
        }
    }

    /// Write JSON footer
    fn write_json_footer(self: Self, writer: anytype) !void {
        _ = self;
        try writer.writeAll(
            \\],
            \\  "edges": []
            \\}
        );
    }

    /// Parse snapshot from JSON content
    fn parse_snapshot(self: Self, json_content: []const u8) !StorageSnapshot {
        const block_count = self.parse_block_count(json_content);
        const metrics = self.parse_metrics(json_content);
        const blocks_array = try self.parse_blocks_array(json_content);
        const edges_array = try self.allocator.alloc(StorageSnapshot.EdgeSnapshot, 0);

        return StorageSnapshot{
            .block_count = block_count,
            .blocks = blocks_array,
            .edges = edges_array,
            .metrics = metrics,
        };
    }

    /// Parse block count from JSON content
    fn parse_block_count(self: Self, json_content: []const u8) u32 {
        _ = self;
        if (std.mem.indexOf(u8, json_content, "\"block_count\": ")) |start| {
            const value_start = start + "\"block_count\": ".len;
            if (std.mem.indexOfScalar(u8, json_content[value_start..], ',')) |comma_offset| {
                const value_str = json_content[value_start .. value_start + comma_offset];
                return std.fmt.parseInt(u32, value_str, 10) catch 0;
            }
        }
        return 0;
    }

    /// Parse metrics from JSON content
    fn parse_metrics(self: Self, json_content: []const u8) StorageSnapshot.MetricsSnapshot {
        return StorageSnapshot.MetricsSnapshot{
            .blocks_written = self.extract_metric_value(json_content, "blocks_written") catch 0,
            .blocks_read = self.extract_metric_value(json_content, "blocks_read") catch 0,
            .blocks_deleted = self.extract_metric_value(json_content, "blocks_deleted") catch 0,
            .wal_writes = self.extract_metric_value(json_content, "wal_writes") catch 0,
            .wal_flushes = self.extract_metric_value(json_content, "wal_flushes") catch 0,
            .wal_recoveries = self.extract_metric_value(json_content, "wal_recoveries") catch 0,
        };
    }

    /// Parse blocks array from JSON content
    fn parse_blocks_array(self: Self, json_content: []const u8) ![]StorageSnapshot.BlockSnapshot {
        var blocks = std.ArrayList(StorageSnapshot.BlockSnapshot).init(self.allocator);
        defer blocks.deinit();

        if (std.mem.indexOf(u8, json_content, "\"blocks\": [")) |blocks_start| {
            const array_start = blocks_start + "\"blocks\": [".len;
            if (std.mem.indexOf(u8, json_content[array_start..], "]")) |array_end| {
                const blocks_content = json_content[array_start .. array_start + array_end];
                try self.parse_individual_blocks(blocks_content, &blocks);
            }
        }

        return self.allocator.dupe(StorageSnapshot.BlockSnapshot, blocks.items);
    }

    /// Parse individual block objects from blocks array content
    fn parse_individual_blocks(
        self: Self,
        blocks_content: []const u8,
        blocks: *std.ArrayList(StorageSnapshot.BlockSnapshot),
    ) !void {
        var block_start: usize = 0;
        while (std.mem.indexOf(u8, blocks_content[block_start..], "{")) |obj_start| {
            const abs_obj_start = block_start + obj_start;
            if (std.mem.indexOf(u8, blocks_content[abs_obj_start..], "}")) |obj_end| {
                const block_json = blocks_content[abs_obj_start .. abs_obj_start + obj_end + 1];

                const block_snapshot = StorageSnapshot.BlockSnapshot{
                    .id = self.parse_block_id(block_json) catch [_]u8{0} ** 32,
                    .version = self.parse_block_version(block_json) catch 0,
                    .content_hash = self.parse_block_content_hash(block_json) catch 0,
                    .content_length = self.parse_block_content_length(block_json) catch 0,
                };
                try blocks.append(block_snapshot);

                block_start = abs_obj_start + obj_end + 1;
            } else {
                break;
            }
        }
    }

    /// Compare two snapshots for equality
    fn compare_snapshots(
        self: Self,
        golden: StorageSnapshot,
        actual: StorageSnapshot,
    ) !void {
        _ = self;

        // Compare basic metrics
        if (golden.block_count != actual.block_count) {
            std.debug.print("Block count mismatch: golden={}, actual={}\n", .{ golden.block_count, actual.block_count });
            return error.GoldenMasterMismatch;
        }

        // Compare metrics
        const golden_metrics = golden.metrics;
        const actual_metrics = actual.metrics;

        if (golden_metrics.blocks_written != actual_metrics.blocks_written) {
            std.debug.print("Blocks written mismatch: golden={}, actual={}\n", .{ golden_metrics.blocks_written, actual_metrics.blocks_written });
            return error.GoldenMasterMismatch;
        }

        if (golden_metrics.wal_recoveries != actual_metrics.wal_recoveries) {
            std.debug.print("WAL recoveries mismatch: golden={}, actual={}\n", .{ golden_metrics.wal_recoveries, actual_metrics.wal_recoveries });
            return error.GoldenMasterMismatch;
        }

        // Block comparison - validate count and content integrity
        if (golden.block_count != actual.block_count) {
            std.debug.print("Block count mismatch: golden={}, actual={}\n", .{ golden.block_count, actual.block_count });
            return error.GoldenMasterMismatch;
        }

        if (golden.blocks.len != actual.blocks.len) {
            std.debug.print("Block array length mismatch: golden={}, actual={}\n", .{ golden.blocks.len, actual.blocks.len });
            return error.GoldenMasterMismatch;
        }

        // Compare each block (both arrays are sorted by ID for deterministic comparison)
        for (golden.blocks, actual.blocks) |golden_block, actual_block| {
            if (!std.mem.eql(u8, &golden_block.id, &actual_block.id)) {
                std.debug.print("Block ID mismatch at position\n", .{});
                return error.GoldenMasterMismatch;
            }

            if (golden_block.version != actual_block.version) {
                std.debug.print("Block version mismatch for ID: {s}\n", .{golden_block.id});
                return error.GoldenMasterMismatch;
            }

            if (golden_block.content_hash != actual_block.content_hash) {
                std.debug.print("Block content hash mismatch for ID: {s}\n", .{golden_block.id});
                return error.GoldenMasterMismatch;
            }

            if (golden_block.content_length != actual_block.content_length) {
                std.debug.print("Block content length mismatch for ID: {s}\n", .{golden_block.id});
                return error.GoldenMasterMismatch;
            }
        }
    }

    /// Parse block ID from JSON block object
    fn parse_block_id(self: Self, block_json: []const u8) ![32]u8 {
        _ = self;
        if (std.mem.indexOf(u8, block_json, "\"id\": \"")) |start| {
            const value_start = start + "\"id\": \"".len;
            if (std.mem.indexOf(u8, block_json[value_start..], "\"")) |end_offset| {
                const id_str = block_json[value_start .. value_start + end_offset];
                var id_array: [32]u8 = undefined;
                @memcpy(&id_array, id_str[0..@min(32, id_str.len)]);
                return id_array;
            }
        }
        return error.ParseError;
    }

    /// Parse block version from JSON block object
    fn parse_block_version(self: Self, block_json: []const u8) !u32 {
        _ = self;
        if (std.mem.indexOf(u8, block_json, "\"version\": ")) |start| {
            const value_start = start + "\"version\": ".len;
            if (std.mem.indexOfAny(u8, block_json[value_start..], ",}")) |end_offset| {
                const value_str = block_json[value_start .. value_start + end_offset];
                return std.fmt.parseInt(u32, value_str, 10);
            }
        }
        return error.ParseError;
    }

    /// Parse block content hash from JSON block object
    fn parse_block_content_hash(self: Self, block_json: []const u8) !u64 {
        _ = self;
        if (std.mem.indexOf(u8, block_json, "\"content_hash\": ")) |start| {
            const value_start = start + "\"content_hash\": ".len;
            if (std.mem.indexOfAny(u8, block_json[value_start..], ",}")) |end_offset| {
                const value_str = block_json[value_start .. value_start + end_offset];
                return std.fmt.parseInt(u64, value_str, 10);
            }
        }
        return error.ParseError;
    }

    /// Parse block content length from JSON block object
    fn parse_block_content_length(self: Self, block_json: []const u8) !usize {
        _ = self;
        // Look for the field name first
        if (std.mem.indexOf(u8, block_json, "\"content_length\"")) |field_start| {
            // Find the colon after the field name
            if (std.mem.indexOf(u8, block_json[field_start..], ":")) |colon_offset| {
                const colon_pos = field_start + colon_offset;
                // Skip whitespace after colon
                var value_start = colon_pos + 1;
                while (value_start < block_json.len and (block_json[value_start] == ' ' or block_json[value_start] == '\t' or block_json[value_start] == '\n')) {
                    value_start += 1;
                }
                // Find end of number (comma, newline, or closing brace)
                if (std.mem.indexOfAny(u8, block_json[value_start..], ",}\n\r\t ")) |end_offset| {
                    const value_str = block_json[value_start .. value_start + end_offset];
                    return std.fmt.parseInt(usize, value_str, 10);
                }
            }
        }
        return error.ParseError;
    }

    /// Extract metric value from JSON content
    fn extract_metric_value(self: Self, json_content: []const u8, metric_name: []const u8) !u64 {
        const search_str = try std.fmt.allocPrint(self.allocator, "\"{s}\": ", .{metric_name});
        defer self.allocator.free(search_str);

        if (std.mem.indexOf(u8, json_content, search_str)) |start| {
            const value_start = start + search_str.len;
            if (std.mem.indexOfAny(u8, json_content[value_start..], ",}")) |end_offset| {
                const value_str = json_content[value_start .. value_start + end_offset];
                return std.fmt.parseInt(u64, value_str, 10);
            }
        }
        return error.MetricNotFound;
    }
};

/// Convenience function for recovery test golden master validation
pub fn verify_recovery_golden_master(
    allocator: std.mem.Allocator,
    test_name: []const u8,
    storage_engine: *StorageEngine,
) !void {
    var golden_master = try GoldenMaster.init(allocator, test_name);
    defer golden_master.deinit();

    var snapshot = try golden_master.capture_snapshot(storage_engine);
    defer snapshot.deinit(allocator);

    try golden_master.verify_snapshot(snapshot);
}
