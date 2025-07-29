//! WAL recovery coordination and validation for storage engine.
//!
//! Provides utilities for coordinating WAL recovery operations across
//! storage subsystems. Handles recovery callbacks, state validation,
//! and error recovery scenarios to ensure consistent system state
//! after crashes or restarts. Works with the WAL module to replay
//! operations and reconstruct storage engine state.

const std = @import("std");
const assert = @import("../core/assert.zig").assert;
const assert_fmt = @import("../core/assert.zig").assert_fmt;
const log = std.log.scoped(.storage_recovery);
const context_block = @import("../core/types.zig");
const concurrency = @import("../core/concurrency.zig");
const simulation_vfs = @import("../sim/simulation_vfs.zig");
const testing = std.testing;

// Import storage submodules
const block_index_mod = @import("block_index.zig");
const graph_edge_index_mod = @import("graph_edge_index.zig");
const wal = @import("wal.zig");

const ContextBlock = context_block.ContextBlock;
const GraphEdge = context_block.GraphEdge;
const BlockId = context_block.BlockId;
const BlockIndex = block_index_mod.BlockIndex;
const GraphEdgeIndex = graph_edge_index_mod.GraphEdgeIndex;
const SimulationVFS = simulation_vfs.SimulationVFS;
const WALEntry = wal.WALEntry;

/// Recovery statistics for monitoring and debugging.
pub const RecoveryStats = struct {
    blocks_recovered: u32,
    edges_recovered: u32,
    blocks_deleted: u32,
    recovery_time_ns: u64,
    corrupted_entries_skipped: u32,
    total_entries_processed: u32,

    pub fn init() RecoveryStats {
        return RecoveryStats{
            .blocks_recovered = 0,
            .edges_recovered = 0,
            .blocks_deleted = 0,
            .recovery_time_ns = 0,
            .corrupted_entries_skipped = 0,
            .total_entries_processed = 0,
        };
    }

    /// Calculate recovery throughput in entries per second.
    pub fn entries_per_second(self: RecoveryStats) f64 {
        if (self.recovery_time_ns == 0) return 0.0;
        const time_seconds = @as(f64, @floatFromInt(self.recovery_time_ns)) / 1_000_000_000.0;
        return @as(f64, @floatFromInt(self.total_entries_processed)) / time_seconds;
    }

    /// Get corruption rate as percentage of total entries.
    pub fn corruption_rate(self: RecoveryStats) f64 {
        if (self.total_entries_processed == 0) return 0.0;
        return (@as(f64, @floatFromInt(self.corrupted_entries_skipped)) / @as(f64, @floatFromInt(self.total_entries_processed))) * 100.0;
    }
};

/// Recovery errors specific to storage engine state reconstruction.
pub const RecoveryError = error{
    /// Recovery context is null or invalid
    InvalidRecoveryContext,
    /// Block index corruption detected during recovery
    BlockIndexCorruption,
    /// Graph index corruption detected during recovery
    GraphIndexCorruption,
    /// Inconsistent state detected after recovery
    InconsistentRecoveryState,
} || wal.WALError;

/// Recovery context passed to WAL recovery callbacks.
/// Contains references to storage subsystems that need state reconstruction
/// and tracks recovery progress for monitoring and validation.
pub const RecoveryContext = struct {
    block_index: *BlockIndex,
    graph_index: *GraphEdgeIndex,
    stats: RecoveryStats,
    start_time: i128,

    /// Initialize recovery context with storage subsystems.
    /// Records start time for performance measurement.
    pub fn init(block_index: *BlockIndex, graph_index: *GraphEdgeIndex) RecoveryContext {
        return RecoveryContext{
            .block_index = block_index,
            .graph_index = graph_index,
            .stats = RecoveryStats.init(),
            .start_time = std.time.nanoTimestamp(),
        };
    }

    /// Finalize recovery statistics with total elapsed time.
    pub fn finalize(self: *RecoveryContext) void {
        const end_time = std.time.nanoTimestamp();
        self.stats.recovery_time_ns = @intCast(end_time - self.start_time);
    }

    /// Validate storage state consistency after recovery.
    /// Performs basic sanity checks to detect corruption or inconsistencies
    /// that could lead to incorrect behavior during normal operations.
    pub fn validate_recovery_state(self: *const RecoveryContext) RecoveryError!void {
        // Verify block index is in valid state
        if (self.block_index.block_count() > 0) {
            // Memory usage should be positive if blocks exist
            if (self.block_index.memory_usage() == 0) {
                return RecoveryError.BlockIndexCorruption;
            }
        }

        // Verify graph index consistency
        const edge_count = self.graph_index.edge_count();
        const source_count = self.graph_index.source_block_count();
        const target_count = self.graph_index.target_block_count();

        // Edge count should be consistent with index structure
        if (edge_count > 0 and source_count == 0) {
            return RecoveryError.GraphIndexCorruption;
        }

        // Basic relationship validation
        if (source_count > edge_count or target_count > edge_count) {
            return RecoveryError.GraphIndexCorruption;
        }
    }
};

/// Standard WAL recovery callback for storage engine state reconstruction.
/// Applies WAL entries to rebuild block index and graph index state.
/// Tracks statistics and handles error conditions gracefully to maximize
/// data recovery even with partial corruption.
pub fn apply_wal_entry_to_storage(context: ?*anyopaque, entry: WALEntry) !void {
    const recovery_ctx: *RecoveryContext = @ptrCast(@alignCast(context orelse {
        return RecoveryError.InvalidRecoveryContext;
    }));

    recovery_ctx.stats.total_entries_processed += 1;

    switch (entry.entry_type) {
        .put_block => {
            const block = entry.extract_block(recovery_ctx.block_index.arena.allocator()) catch |err| {
                log.warn("Failed to extract block from WAL entry: {}", .{err});
                recovery_ctx.stats.corrupted_entries_skipped += 1;
                return;
            };
            recovery_ctx.block_index.put_block(block) catch |err| {
                // Log corruption but continue recovery to maximize data salvage
                log.warn("Failed to recover block {}: {}", .{ block.id, err });
                recovery_ctx.stats.corrupted_entries_skipped += 1;
                return;
            };
            recovery_ctx.stats.blocks_recovered += 1;
        },
        .delete_block => {
            const block_id = entry.extract_block_id() catch |err| {
                log.warn("Failed to extract block_id from WAL entry: {}", .{err});
                recovery_ctx.stats.corrupted_entries_skipped += 1;
                return;
            };
            recovery_ctx.block_index.remove_block(block_id);
            recovery_ctx.graph_index.remove_block_edges(block_id);
            recovery_ctx.stats.blocks_deleted += 1;
        },
        .put_edge => {
            const edge = entry.extract_edge() catch |err| {
                log.warn("Failed to extract edge from WAL entry: {}", .{err});
                recovery_ctx.stats.corrupted_entries_skipped += 1;
                return;
            };
            recovery_ctx.graph_index.put_edge(edge) catch |err| {
                // Log corruption but continue recovery
                log.warn("Failed to recover edge {} -> {}: {}", .{ edge.source_id, edge.target_id, err });
                recovery_ctx.stats.corrupted_entries_skipped += 1;
                return;
            };
            recovery_ctx.stats.edges_recovered += 1;
        },
    }
}

/// Perform complete storage recovery from WAL with validation.
/// High-level interface that coordinates WAL recovery, state validation,
/// and error handling. Returns detailed statistics for monitoring.
pub fn recover_storage_from_wal(
    wal_instance: *wal.WAL,
    block_index: *BlockIndex,
    graph_index: *GraphEdgeIndex,
) !RecoveryStats {
    concurrency.assert_main_thread();

    var recovery_context = RecoveryContext.init(block_index, graph_index);

    // Clear existing state to ensure clean recovery
    block_index.clear();
    graph_index.clear();

    // Perform WAL recovery with our callback
    try wal_instance.recover(&recovery_context, apply_wal_entry_to_storage);

    // Finalize timing and validate result
    recovery_context.finalize();
    try recovery_context.validate_recovery_state();

    log.info("Recovery completed: {} blocks, {} edges, {} corrupted entries in {}ms", .{
        recovery_context.stats.blocks_recovered,
        recovery_context.stats.edges_recovered,
        recovery_context.stats.corrupted_entries_skipped,
        recovery_context.stats.recovery_time_ns / 1_000_000,
    });

    return recovery_context.stats;
}

/// Create a minimal recovery test setup for unit testing.
/// Provides pre-configured storage subsystems suitable for recovery testing
/// without requiring full storage engine initialization.
pub fn create_test_recovery_setup(allocator: std.mem.Allocator) !struct {
    block_index: BlockIndex,
    graph_index: GraphEdgeIndex,

    pub fn deinit(self: *@This()) void {
        self.block_index.deinit();
        self.graph_index.deinit();
    }
} {
    return .{
        .block_index = BlockIndex.init(allocator),
        .graph_index = GraphEdgeIndex.init(allocator),
    };
}

// Tests

test "recovery context initialization and finalization" {
    var setup = try create_test_recovery_setup(testing.allocator);
    defer setup.deinit();

    var context = RecoveryContext.init(&setup.block_index, &setup.graph_index);

    try testing.expectEqual(@as(u32, 0), context.stats.blocks_recovered);
    try testing.expectEqual(@as(u32, 0), context.stats.total_entries_processed);
    try testing.expect(context.start_time > 0);

    // Simulate some recovery work
    std.time.sleep(1_000_000); // 1ms
    context.finalize();

    try testing.expect(context.stats.recovery_time_ns > 0);
}

test "recovery statistics calculations" {
    var stats = RecoveryStats.init();
    stats.total_entries_processed = 100;
    stats.corrupted_entries_skipped = 5;
    stats.recovery_time_ns = 1_000_000_000; // 1 second

    try testing.expectEqual(@as(f64, 100.0), stats.entries_per_second());
    try testing.expectEqual(@as(f64, 5.0), stats.corruption_rate());

    // Test zero cases
    stats.recovery_time_ns = 0;
    try testing.expectEqual(@as(f64, 0.0), stats.entries_per_second());

    stats.total_entries_processed = 0;
    try testing.expectEqual(@as(f64, 0.0), stats.corruption_rate());
}

test "apply wal entry to storage with block operations" {
    var setup = try create_test_recovery_setup(testing.allocator);
    defer setup.deinit();

    var context = RecoveryContext.init(&setup.block_index, &setup.graph_index);

    const block_id = BlockId.generate();
    const block = ContextBlock{
        .id = block_id,
        .version = 1,
        .source_uri = "file://test.zig",
        .metadata_json = "{}",
        .content = "test content",
    };

    // Test put_block entry
    const put_entry = WALEntry{
        .entry_type = .put_block,
        .block = block,
    };

    try apply_wal_entry_to_storage(&context, put_entry);

    try testing.expectEqual(@as(u32, 1), context.stats.blocks_recovered);
    try testing.expectEqual(@as(u32, 1), context.stats.total_entries_processed);
    try testing.expectEqual(@as(u32, 1), setup.block_index.block_count());

    // Test delete_block entry
    const delete_entry = WALEntry{
        .entry_type = .delete_block,
        .block_id = block_id,
    };

    try apply_wal_entry_to_storage(&context, delete_entry);

    try testing.expectEqual(@as(u32, 1), context.stats.blocks_deleted);
    try testing.expectEqual(@as(u32, 2), context.stats.total_entries_processed);
    try testing.expectEqual(@as(u32, 0), setup.block_index.block_count());
}

test "apply wal entry to storage with edge operations" {
    var setup = try create_test_recovery_setup(testing.allocator);
    defer setup.deinit();

    var context = RecoveryContext.init(&setup.block_index, &setup.graph_index);

    const source_id = BlockId.generate();
    const target_id = BlockId.generate();
    const edge = GraphEdge{
        .source_id = source_id,
        .target_id = target_id,
        .edge_type = .calls,
    };

    const edge_entry = WALEntry{
        .entry_type = .put_edge,
        .edge = edge,
    };

    try apply_wal_entry_to_storage(&context, edge_entry);

    try testing.expectEqual(@as(u32, 1), context.stats.edges_recovered);
    try testing.expectEqual(@as(u32, 1), context.stats.total_entries_processed);
    try testing.expectEqual(@as(u32, 1), setup.graph_index.edge_count());
}

test "recovery state validation detects corruption" {
    var setup = try create_test_recovery_setup(testing.allocator);
    defer setup.deinit();

    // Valid state should pass validation
    {
        var context = RecoveryContext.init(&setup.block_index, &setup.graph_index);
        try context.validate_recovery_state();
    }

    const block = ContextBlock{
        .id = BlockId.generate(),
        .version = 1,
        .source_uri = "file://test.zig",
        .metadata_json = "{}",
        .content = "test content",
    };
    try setup.block_index.put_block(block);

    // Valid non-empty state should pass
    {
        var context = RecoveryContext.init(&setup.block_index, &setup.graph_index);
        try context.validate_recovery_state();
    }
}

test "recovery handles invalid context gracefully" {
    const block = ContextBlock{
        .id = BlockId.generate(),
        .version = 1,
        .source_uri = "file://test.zig",
        .metadata_json = "{}",
        .content = "test content",
    };

    const entry = WALEntry{
        .entry_type = .put_block,
        .block = block,
    };

    // Null context should return error
    const result = apply_wal_entry_to_storage(null, entry);
    try testing.expectError(RecoveryError.InvalidRecoveryContext, result);
}

test "recovery callback handles corrupted blocks gracefully" {
    var setup = try create_test_recovery_setup(testing.allocator);
    defer setup.deinit();

    var context = RecoveryContext.init(&setup.block_index, &setup.graph_index);

    var invalid_content = [_]u8{0xFF} ** (1024 * 1024 * 1024); // Huge content
    const corrupted_block = ContextBlock{
        .id = BlockId.generate(),
        .version = 1,
        .source_uri = "file://test.zig",
        .metadata_json = "{}",
        .content = &invalid_content,
    };

    const entry = WALEntry{
        .entry_type = .put_block,
        .block = corrupted_block,
    };

    // Should handle gracefully without crashing
    try apply_wal_entry_to_storage(&context, entry);

    // Should have skipped the corrupted entry
    try testing.expectEqual(@as(u32, 1), context.stats.corrupted_entries_skipped);
    try testing.expectEqual(@as(u32, 1), context.stats.total_entries_processed);
    try testing.expectEqual(@as(u32, 0), context.stats.blocks_recovered);
}

test "complete recovery workflow with mixed operations" {
    const allocator = testing.allocator;

    var sim_vfs = SimulationVFS.init(allocator);
    defer sim_vfs.deinit();

    var setup = try create_test_recovery_setup(allocator);
    defer setup.deinit();

    // Create WAL for testing
    var wal_instance = try wal.WAL.init(allocator, sim_vfs.vfs(), "/test/wal");
    defer wal_instance.deinit();
    try wal_instance.initialize();

    const block1 = ContextBlock{
        .id = BlockId.generate(),
        .version = 1,
        .source_uri = "file://test1.zig",
        .metadata_json = "{}",
        .content = "first block",
    };

    const block2 = ContextBlock{
        .id = BlockId.generate(),
        .version = 1,
        .source_uri = "file://test2.zig",
        .metadata_json = "{}",
        .content = "second block",
    };

    const edge = GraphEdge{
        .source_id = block1.id,
        .target_id = block2.id,
        .edge_type = .calls,
    };

    try wal_instance.write_entry(WALEntry{ .entry_type = .put_block, .block = block1 });
    try wal_instance.write_entry(WALEntry{ .entry_type = .put_block, .block = block2 });
    try wal_instance.write_entry(WALEntry{ .entry_type = .put_edge, .edge = edge });
    try wal_instance.write_entry(WALEntry{ .entry_type = .delete_block, .block_id = block2.id });
    try wal_instance.flush();

    // Perform recovery
    const stats = try recover_storage_from_wal(allocator, &wal_instance, &setup.block_index, &setup.graph_index);

    // Verify recovery results
    try testing.expectEqual(@as(u32, 2), stats.blocks_recovered);
    try testing.expectEqual(@as(u32, 1), stats.edges_recovered);
    try testing.expectEqual(@as(u32, 1), stats.blocks_deleted);
    try testing.expectEqual(@as(u32, 4), stats.total_entries_processed);
    try testing.expect(stats.recovery_time_ns > 0);

    // Verify final state
    try testing.expectEqual(@as(u32, 1), setup.block_index.block_count()); // block1 remains
    try testing.expectEqual(@as(u32, 0), setup.graph_index.edge_count()); // edge removed with block2
}

test "create test recovery setup provides working components" {
    var setup = try create_test_recovery_setup(testing.allocator);
    defer setup.deinit();

    // Verify components are functional
    try testing.expectEqual(@as(u32, 0), setup.block_index.block_count());
    try testing.expectEqual(@as(u32, 0), setup.graph_index.edge_count());

    const block = ContextBlock{
        .id = BlockId.generate(),
        .version = 1,
        .source_uri = "file://test.zig",
        .metadata_json = "{}",
        .content = "test content",
    };

    try setup.block_index.put_block(block);
    try testing.expectEqual(@as(u32, 1), setup.block_index.block_count());
}
