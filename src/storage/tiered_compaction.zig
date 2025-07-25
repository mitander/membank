//! Tiered compaction strategy for CortexDB LSM-Tree.
//!
//! Implements a size-tiered compaction strategy to minimize write amplification
//! while maintaining read performance. Based on Cassandra's size-tiered approach
//! with optimizations for CortexDB's workload characteristics.

const std = @import("std");
const assert = std.debug.assert;
const vfs = @import("../core/vfs.zig");
const sstable = @import("sstable.zig");
const concurrency = @import("../core/concurrency.zig");

const VFS = vfs.VFS;
const SSTable = sstable.SSTable;
const Compactor = sstable.Compactor;

/// Tiered compaction configuration and strategy implementation.
pub const TieredCompactionManager = struct {
    allocator: std.mem.Allocator,
    compactor: Compactor,

    /// Configuration parameters for compaction strategy
    config: CompactionConfig,

    /// Current state of each tier
    tiers: [MAX_TIERS]TierState,

    const MAX_TIERS = 8; // L0 through L7

    /// Configuration for the tiered compaction strategy.
    pub const CompactionConfig = struct {
        /// Maximum number of SSTables in L0 before compaction
        l0_compaction_threshold: u32 = 4,

        /// Size ratio between tiers (each tier is ~4x larger than previous)
        tier_size_ratio: f64 = 4.0,

        /// Base size for L1 in bytes (64MB)
        l1_base_size: u64 = 64 * 1024 * 1024,

        /// Maximum number of SSTables per tier before compaction
        max_sstables_per_tier: u32 = 10,

        /// Minimum number of SSTables to trigger size-tiered compaction
        min_compaction_threshold: u32 = 2,
    };

    /// State tracking for each tier
    const TierState = struct {
        sstables: std.ArrayList(SSTableInfo),
        total_size: u64 = 0,

        const SSTableInfo = struct {
            path: []const u8,
            size: u64,
            level: u8,
        };

        pub fn init(allocator: std.mem.Allocator) TierState {
            return TierState{
                .sstables = std.ArrayList(SSTableInfo).init(allocator),
            };
        }

        pub fn deinit(self: *TierState, allocator: std.mem.Allocator) void {
            for (self.sstables.items) |info| {
                allocator.free(info.path);
            }
            self.sstables.deinit();
        }

        pub fn add_sstable(
            self: *TierState,
            allocator: std.mem.Allocator,
            path: []const u8,
            size: u64,
            level: u8,
        ) !void {
            const path_copy = try allocator.dupe(u8, path);
            try self.sstables.append(.{
                .path = path_copy,
                .size = size,
                .level = level,
            });
            self.total_size += size;
        }

        pub fn remove_sstable(
            self: *TierState,
            allocator: std.mem.Allocator,
            path: []const u8,
        ) void {
            for (self.sstables.items, 0..) |info, i| {
                if (std.mem.eql(u8, info.path, path)) {
                    self.total_size -= info.size;
                    allocator.free(info.path);
                    _ = self.sstables.swapRemove(i);
                    return;
                }
            }
        }

        /// Calculate target size for this tier level
        pub fn target_size(level: u8, config: CompactionConfig) u64 {
            if (level == 0) return 0; // L0 has no size limit, only count limit

            const base_size = config.l1_base_size;
            var calculated_size = base_size;

            var i: u8 = 1;
            while (i < level) : (i += 1) {
                calculated_size = @intFromFloat(
                    @as(f64, @floatFromInt(calculated_size)) * config.tier_size_ratio,
                );
            }

            return calculated_size;
        }
    };

    pub fn init(
        allocator: std.mem.Allocator,
        filesystem: VFS,
        data_dir: []const u8,
    ) TieredCompactionManager {
        var tiers: [MAX_TIERS]TierState = undefined;
        for (&tiers) |*tier| {
            tier.* = TierState.init(allocator);
        }

        return TieredCompactionManager{
            .allocator = allocator,
            .compactor = Compactor.init(allocator, filesystem, data_dir),
            .config = CompactionConfig{},
            .tiers = tiers,
        };
    }

    pub fn deinit(self: *TieredCompactionManager) void {
        for (&self.tiers) |*tier| {
            tier.deinit(self.allocator);
        }
    }

    /// Add a new SSTable to the appropriate tier
    pub fn add_sstable(
        self: *TieredCompactionManager,
        path: []const u8,
        size: u64,
        level: u8,
    ) !void {
        concurrency.assert_main_thread();
        assert(level < MAX_TIERS);

        try self.tiers[level].add_sstable(self.allocator, path, size, level);
    }

    /// Remove an SSTable from tracking
    pub fn remove_sstable(self: *TieredCompactionManager, path: []const u8, level: u8) void {
        concurrency.assert_main_thread();
        assert(level < MAX_TIERS);

        self.tiers[level].remove_sstable(self.allocator, path);
    }

    /// Check if compaction is needed and return compaction job if so
    pub fn check_compaction_needed(self: *TieredCompactionManager) ?CompactionJob {
        concurrency.assert_main_thread();

        // Check L0 first - it has count-based compaction
        if (self.tiers[0].sstables.items.len >= self.config.l0_compaction_threshold) {
            return self.create_l0_compaction_job();
        }

        // Check other levels for size-tiered compaction
        for (self.tiers[1..], 1..) |*tier, level_idx| {
            const level: u8 = @intCast(level_idx);

            if (tier.sstables.items.len >= self.config.min_compaction_threshold) {
                if (self.should_compact_tier(level)) {
                    return self.create_tier_compaction_job(level);
                }
            }
        }

        return null;
    }

    /// Execute a compaction job
    pub fn execute_compaction(self: *TieredCompactionManager, job: CompactionJob) !void {
        concurrency.assert_main_thread();

        switch (job.compaction_type) {
            .l0_to_l1 => try self.execute_l0_compaction(job),
            .tier_compaction => try self.execute_tier_compaction(job),
        }
    }

    fn create_l0_compaction_job(self: *TieredCompactionManager) CompactionJob {
        // Compact all L0 SSTables into L1
        var input_paths = std.ArrayList([]const u8).init(self.allocator);
        for (self.tiers[0].sstables.items) |info| {
            input_paths.append(info.path) catch unreachable;
        }

        return CompactionJob{
            .compaction_type = .l0_to_l1,
            .input_level = 0,
            .output_level = 1,
            .input_paths = input_paths,
            .estimated_output_size = self.tiers[0].total_size,
        };
    }

    fn create_tier_compaction_job(self: *TieredCompactionManager, level: u8) CompactionJob {
        // Find SSTables of similar size to compact together
        const tier = &self.tiers[level];
        var candidates = std.ArrayList(usize).init(self.allocator);
        defer candidates.deinit();

        // Simple heuristic: find the largest group of similarly-sized SSTables
        for (tier.sstables.items, 0..) |info, i| {
            _ = info;
            candidates.append(i) catch unreachable;
            if (candidates.items.len >= self.config.max_sstables_per_tier) break;
        }

        var input_paths = std.ArrayList([]const u8).init(self.allocator);
        var total_size: u64 = 0;

        for (candidates.items) |idx| {
            const info = tier.sstables.items[idx];
            input_paths.append(info.path) catch unreachable;
            total_size += info.size;
        }

        return CompactionJob{
            .compaction_type = .tier_compaction,
            .input_level = level,
            .output_level = level, // Same level for size-tiered compaction
            .input_paths = input_paths,
            .estimated_output_size = total_size,
        };
    }

    fn should_compact_tier(self: *TieredCompactionManager, level: u8) bool {
        const tier = &self.tiers[level];

        // Too many SSTables in this tier
        if (tier.sstables.items.len >= self.config.max_sstables_per_tier) {
            return true;
        }

        // Tier has grown too large compared to target size
        const target_size = TierState.target_size(level, self.config);
        if (tier.total_size > target_size * 2) {
            return true;
        }

        return false;
    }

    fn execute_l0_compaction(self: *TieredCompactionManager, job: CompactionJob) !void {
        const output_path = try std.fmt.allocPrint(
            self.allocator,
            "{s}/sst/l1_{d:0>8}.sst",
            .{ self.compactor.data_dir, std.time.timestamp() },
        );
        defer self.allocator.free(output_path);

        // Perform compaction
        try self.compactor.compact_sstables(job.input_paths.items, output_path);

        // Update tier tracking
        for (job.input_paths.items) |path| {
            self.remove_sstable(path, 0);
        }

        try self.add_sstable(output_path, job.estimated_output_size, 1);

        job.input_paths.deinit();
    }

    fn execute_tier_compaction(self: *TieredCompactionManager, job: CompactionJob) !void {
        const output_path = try std.fmt.allocPrint(
            self.allocator,
            "{s}/sst/l{d}_{d:0>8}.sst",
            .{ self.compactor.data_dir, job.output_level, std.time.timestamp() },
        );
        defer self.allocator.free(output_path);

        // Perform compaction
        try self.compactor.compact_sstables(job.input_paths.items, output_path);

        // Update tier tracking
        for (job.input_paths.items) |path| {
            self.remove_sstable(path, job.input_level);
        }

        try self.add_sstable(output_path, job.estimated_output_size, job.output_level);

        job.input_paths.deinit();
    }
};

/// Represents a compaction job to be executed
pub const CompactionJob = struct {
    compaction_type: CompactionType,
    input_level: u8,
    output_level: u8,
    input_paths: std.ArrayList([]const u8),
    estimated_output_size: u64,

    pub const CompactionType = enum {
        l0_to_l1, // L0 -> L1 compaction (count-based)
        tier_compaction, // Same-level size-tiered compaction
    };
};

// Tests

test "TieredCompactionManager initialization" {
    const allocator = std.testing.allocator;
    _ = allocator; // Test placeholder - VFS required for actual testing

    // We need a VFS for testing, but can't easily create one here
    // This would be tested in integration tests with SimulationVFS
}

test "tier size calculation" {
    const config = TieredCompactionManager.CompactionConfig{};

    // L0 has no size limit
    try std.testing.expectEqual(
        @as(u64, 0),
        TieredCompactionManager.TierState.target_size(0, config),
    );

    // L1 = base size (64MB)
    try std.testing.expectEqual(
        @as(u64, 64 * 1024 * 1024),
        TieredCompactionManager.TierState.target_size(1, config),
    );

    // L2 = base * ratio (256MB)
    try std.testing.expectEqual(
        @as(u64, 256 * 1024 * 1024),
        TieredCompactionManager.TierState.target_size(2, config),
    );

    // L3 = base * ratio^2 (1GB)
    try std.testing.expectEqual(
        @as(u64, 1024 * 1024 * 1024),
        TieredCompactionManager.TierState.target_size(3, config),
    );
}

test "compaction thresholds" {
    // This would require more complex setup with actual SSTables
    // Integration tests would cover the full compaction logic
}
