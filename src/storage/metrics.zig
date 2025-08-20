//! Storage engine performance metrics and observability.
//!
//! Provides atomic counters and analytics for storage operations to enable
//! performance monitoring, capacity planning, and operational debugging.
//! All counters use thread-safe coordination primitives to support safe
//! concurrent access from monitoring threads without impacting storage performance.

const std = @import("std");

const stdx = @import("../core/stdx.zig");
const assert_mod = @import("../core/assert.zig");

const assert = assert_mod.assert;

/// Performance metrics for storage engine observability.
/// Atomic counters enable safe concurrent access from monitoring threads
/// without requiring locks that would impact storage engine performance.
pub const StorageMetrics = struct {
    // Block operations - core storage workload metrics
    blocks_written: stdx.MetricsCounter,
    blocks_read: stdx.MetricsCounter,
    blocks_deleted: stdx.MetricsCounter,

    // WAL operations - durability and recovery metrics
    wal_writes: stdx.MetricsCounter,
    wal_flushes: stdx.MetricsCounter,
    wal_recoveries: stdx.MetricsCounter,

    // SSTable operations - compaction and read path metrics
    sstable_reads: stdx.MetricsCounter,
    sstable_writes: stdx.MetricsCounter,
    compactions: stdx.MetricsCounter,

    // Edge operations - graph relationship metrics
    edges_added: stdx.MetricsCounter,
    edges_removed: stdx.MetricsCounter,

    // Performance timings in nanoseconds for latency analysis
    total_write_time_ns: stdx.MetricsCounter,
    total_read_time_ns: stdx.MetricsCounter,
    total_wal_flush_time_ns: stdx.MetricsCounter,

    // Error counts for reliability monitoring
    write_errors: stdx.MetricsCounter,
    read_errors: stdx.MetricsCounter,
    wal_errors: stdx.MetricsCounter,

    // Storage utilization metrics for capacity planning
    total_bytes_written: stdx.MetricsCounter,
    total_bytes_read: stdx.MetricsCounter,
    wal_bytes_written: stdx.MetricsCounter,
    sstable_bytes_written: stdx.MetricsCounter,

    // Memory pressure metrics for backpressure control
    memtable_memory_bytes: stdx.MetricsCounter,
    compaction_queue_size: stdx.MetricsCounter,

    /// Initialize all metrics to zero.
    /// Initialize a new StorageMetrics instance with all counters set to zero.
    /// Uses thread-safe coordination primitives for all counter operations.
    pub fn init() StorageMetrics {
        return .{
            .blocks_written = stdx.MetricsCounter.init(0),
            .blocks_read = stdx.MetricsCounter.init(0),
            .blocks_deleted = stdx.MetricsCounter.init(0),
            .wal_writes = stdx.MetricsCounter.init(0),
            .wal_flushes = stdx.MetricsCounter.init(0),
            .wal_recoveries = stdx.MetricsCounter.init(0),
            .sstable_reads = stdx.MetricsCounter.init(0),
            .sstable_writes = stdx.MetricsCounter.init(0),
            .compactions = stdx.MetricsCounter.init(0),
            .edges_added = stdx.MetricsCounter.init(0),
            .edges_removed = stdx.MetricsCounter.init(0),
            .total_write_time_ns = stdx.MetricsCounter.init(0),
            .total_read_time_ns = stdx.MetricsCounter.init(0),
            .total_wal_flush_time_ns = stdx.MetricsCounter.init(0),
            .write_errors = stdx.MetricsCounter.init(0),
            .read_errors = stdx.MetricsCounter.init(0),
            .wal_errors = stdx.MetricsCounter.init(0),
            .total_bytes_written = stdx.MetricsCounter.init(0),
            .total_bytes_read = stdx.MetricsCounter.init(0),
            .wal_bytes_written = stdx.MetricsCounter.init(0),
            .sstable_bytes_written = stdx.MetricsCounter.init(0),
            .memtable_memory_bytes = stdx.MetricsCounter.init(0),
            .compaction_queue_size = stdx.MetricsCounter.init(0),
        };
    }

    /// Storage memory pressure levels for backpressure control.
    /// Used by ingestion pipeline to adapt batch sizes based on storage load.
    pub const MemoryPressure = enum {
        /// Memory usage below 50% of target - normal operation
        low,
        /// Memory usage 50-80% of target - reduce batch sizes
        medium,
        /// Memory usage above 80% of target - single-file processing
        high,
    };

    /// Calculate current memory pressure based on memtable usage and compaction queue.
    /// Uses configurable thresholds to determine if ingestion should apply backpressure.
    pub fn calculate_memory_pressure(self: *const StorageMetrics, config: MemoryPressureConfig) MemoryPressure {
        const mem_usage = self.memtable_memory_bytes.load();
        const queue_size = self.compaction_queue_size.load();

        // Memory pressure increases with both memtable size and compaction backlog
        const memtable_ratio = @as(f64, @floatFromInt(mem_usage)) / @as(f64, @floatFromInt(config.memtable_target_bytes));
        const queue_ratio = @as(f64, @floatFromInt(queue_size)) / @as(f64, @floatFromInt(config.max_compaction_queue_size));

        // Use the higher of the two pressure indicators
        const pressure_ratio = @max(memtable_ratio, queue_ratio);

        if (pressure_ratio > config.high_pressure_threshold) return .high;
        if (pressure_ratio > config.medium_pressure_threshold) return .medium;
        return .low;
    }

    /// Configuration for memory pressure thresholds.
    /// Allows tuning backpressure behavior for different deployment scenarios.
    pub const MemoryPressureConfig = struct {
        /// Target memtable size in bytes before pressure increases
        memtable_target_bytes: u64 = 64 * 1024 * 1024, // 64MB default
        /// Maximum pending compaction queue size before high pressure
        max_compaction_queue_size: u64 = 10,
        /// Medium pressure threshold (50% of capacity)
        medium_pressure_threshold: f64 = 0.5,
        /// High pressure threshold (80% of capacity)
        high_pressure_threshold: f64 = 0.8,
    };

    /// Calculate average write latency in nanoseconds.
    /// Returns 0 if no writes have occurred to avoid division by zero.
    pub fn average_write_latency_ns(self: *const StorageMetrics) u64 {
        const writes = self.blocks_written.load();
        if (writes == 0) return 0;
        return self.total_write_time_ns.load() / writes;
    }

    /// Calculate average read latency in nanoseconds.
    /// Returns 0 if no reads have occurred to avoid division by zero.
    pub fn average_read_latency_ns(self: *const StorageMetrics) u64 {
        const reads = self.blocks_read.load();
        if (reads == 0) return 0;
        return self.total_read_time_ns.load() / reads;
    }

    /// Calculate average WAL flush latency in nanoseconds.
    /// Returns 0 if no flushes have occurred to avoid division by zero.
    pub fn average_wal_flush_latency_ns(self: *const StorageMetrics) u64 {
        const flushes = self.wal_flushes.load();
        if (flushes == 0) return 0;
        return self.total_wal_flush_time_ns.load() / flushes;
    }

    /// Calculate average bytes per block written.
    /// Returns 0 if no blocks have been written to avoid division by zero.
    pub fn average_block_size_bytes(self: *const StorageMetrics) u64 {
        const blocks = self.blocks_written.load();
        if (blocks == 0) return 0;
        return self.total_bytes_written.load() / blocks;
    }

    /// Calculate storage write throughput in bytes per second.
    /// Returns 0.0 if no time has elapsed to avoid division by zero.
    pub fn write_throughput_bps(self: *const StorageMetrics) f64 {
        const total_time_ns = self.total_write_time_ns.load();
        const total_time_seconds = @as(f64, @floatFromInt(total_time_ns)) / 1_000_000_000.0;
        if (total_time_seconds == 0.0) return 0.0;
        const total_bytes = self.total_bytes_written.load();
        return @as(f64, @floatFromInt(total_bytes)) / total_time_seconds;
    }

    /// Calculate storage read throughput in bytes per second.
    /// Returns 0.0 if no time has elapsed to avoid division by zero.
    pub fn read_throughput_bps(self: *const StorageMetrics) f64 {
        const total_time_ns = self.total_read_time_ns.load();
        const total_time_seconds = @as(f64, @floatFromInt(total_time_ns)) / 1_000_000_000.0;
        if (total_time_seconds == 0.0) return 0.0;
        const total_bytes = self.total_bytes_read.load();
        return @as(f64, @floatFromInt(total_bytes)) / total_time_seconds;
    }

    /// Format metrics as human-readable text for operational dashboards.
    /// Provides key performance indicators in an easily scannable format.
    pub fn format_human_readable(self: *const StorageMetrics, writer: anytype) !void {
        try writer.writeAll("=== Storage Metrics ===\n");
        try writer.print("Blocks: {} written, {} read, {} deleted\n", .{
            self.blocks_written.load(),
            self.blocks_read.load(),
            self.blocks_deleted.load(),
        });
        try writer.print("WAL: {} writes, {} flushes, {} recoveries\n", .{
            self.wal_writes.load(),
            self.wal_flushes.load(),
            self.wal_recoveries.load(),
        });
        try writer.print("SSTable: {} reads, {} writes, {} compactions\n", .{
            self.sstable_reads.load(),
            self.sstable_writes.load(),
            self.compactions.load(),
        });
        try writer.print("Edges: {} added, {} removed\n", .{
            self.edges_added.load(),
            self.edges_removed.load(),
        });
        try writer.print("Latency: {} ns write, {} ns read, {} ns WAL flush\n", .{
            self.average_write_latency_ns(),
            self.average_read_latency_ns(),
            self.average_wal_flush_latency_ns(),
        });
        try writer.print("Throughput: {d:.2} MB/s write, {d:.2} MB/s read\n", .{
            self.write_throughput_bps() / (1024.0 * 1024.0),
            self.read_throughput_bps() / (1024.0 * 1024.0),
        });
        try writer.print("Data: {d:.2} MB written, {d:.2} MB read, avg block {d:.2} KB\n", .{
            @as(f64, @floatFromInt(self.total_bytes_written.load())) / (1024.0 * 1024.0),
            @as(f64, @floatFromInt(self.total_bytes_read.load())) / (1024.0 * 1024.0),
            @as(f64, @floatFromInt(self.average_block_size_bytes())) / 1024.0,
        });
        try writer.print("Errors: {} write, {} read, {} WAL\n", .{
            self.write_errors.load(),
            self.read_errors.load(),
            self.wal_errors.load(),
        });
    }

    /// Format metrics as JSON for programmatic consumption by monitoring systems.
    /// Provides structured data format suitable for time-series databases and alerts.
    pub fn format_json(self: *const StorageMetrics, writer: anytype) !void {
        try writer.writeAll("{\n");
        try writer.print("  \"blocks_written\": {},\n", .{self.blocks_written.load()});
        try writer.print("  \"blocks_read\": {},\n", .{self.blocks_read.load()});
        try writer.print("  \"blocks_deleted\": {},\n", .{self.blocks_deleted.load()});
        try writer.print("  \"wal_writes\": {},\n", .{self.wal_writes.load()});
        try writer.print("  \"wal_flushes\": {},\n", .{self.wal_flushes.load()});
        try writer.print("  \"wal_recoveries\": {},\n", .{self.wal_recoveries.load()});
        try writer.print("  \"sstable_reads\": {},\n", .{self.sstable_reads.load()});
        try writer.print("  \"sstable_writes\": {},\n", .{self.sstable_writes.load()});
        try writer.print("  \"compactions\": {},\n", .{self.compactions.load()});
        try writer.print("  \"edges_added\": {},\n", .{self.edges_added.load()});
        try writer.print("  \"edges_removed\": {},\n", .{self.edges_removed.load()});
        try writer.print("  \"total_bytes_written\": {},\n", .{self.total_bytes_written.load()});
        try writer.print("  \"total_bytes_read\": {},\n", .{self.total_bytes_read.load()});
        try writer.print("  \"wal_bytes_written\": {},\n", .{self.wal_bytes_written.load()});
        try writer.print("  \"sstable_bytes_written\": {},\n", .{self.sstable_bytes_written.load()});
        try writer.print("  \"average_write_latency_ns\": {},\n", .{self.average_write_latency_ns()});
        try writer.print("  \"average_read_latency_ns\": {},\n", .{self.average_read_latency_ns()});
        try writer.print("  \"average_wal_flush_latency_ns\": {},\n", .{self.average_wal_flush_latency_ns()});
        try writer.print("  \"average_block_size_bytes\": {},\n", .{self.average_block_size_bytes()});
        try writer.print("  \"write_throughput_bps\": {d:.2},\n", .{self.write_throughput_bps()});
        try writer.print("  \"read_throughput_bps\": {d:.2},\n", .{self.read_throughput_bps()});
        try writer.print("  \"write_errors\": {},\n", .{self.write_errors.load()});
        try writer.print("  \"read_errors\": {},\n", .{self.read_errors.load()});
        try writer.print("  \"wal_errors\": {}\n", .{self.wal_errors.load()});
        try writer.writeAll("}\n");
    }
};

const testing = std.testing;

test "metrics initialization sets all counters to zero" {
    const metrics = StorageMetrics.init();

    try testing.expectEqual(@as(u64, 0), metrics.blocks_written.load());
    try testing.expectEqual(@as(u64, 0), metrics.blocks_read.load());
    try testing.expectEqual(@as(u64, 0), metrics.total_write_time_ns.load());
    try testing.expectEqual(@as(u64, 0), metrics.write_errors.load());
}

test "average calculations handle zero operations gracefully" {
    const metrics = StorageMetrics.init();

    try testing.expectEqual(@as(u64, 0), metrics.average_write_latency_ns());
    try testing.expectEqual(@as(u64, 0), metrics.average_read_latency_ns());
    try testing.expectEqual(@as(u64, 0), metrics.average_wal_flush_latency_ns());
    try testing.expectEqual(@as(u64, 0), metrics.average_block_size_bytes());
    try testing.expectEqual(@as(f64, 0.0), metrics.write_throughput_bps());
    try testing.expectEqual(@as(f64, 0.0), metrics.read_throughput_bps());
}

test "average write latency calculation" {
    var metrics = StorageMetrics.init();

    // Simulate 2 writes taking 1000ns and 2000ns respectively
    metrics.blocks_written.add(2);
    metrics.total_write_time_ns.add(3000);

    try testing.expectEqual(@as(u64, 1500), metrics.average_write_latency_ns());
}

test "write throughput calculation" {
    var metrics = StorageMetrics.init();

    // Simulate 1MB written in 1 second (1_000_000_000 ns)
    metrics.total_bytes_written.add(1024 * 1024);
    metrics.total_write_time_ns.add(1_000_000_000);

    const throughput = metrics.write_throughput_bps();
    try testing.expectEqual(@as(f64, 1024.0 * 1024.0), throughput);
}

test "average block size calculation" {
    var metrics = StorageMetrics.init();

    // Simulate 4 blocks totaling 8KB
    metrics.blocks_written.add(4);
    metrics.total_bytes_written.add(8192);

    try testing.expectEqual(@as(u64, 2048), metrics.average_block_size_bytes());
}

test "human readable format contains key metrics" {
    const allocator = testing.allocator;

    var metrics = StorageMetrics.init();
    metrics.blocks_written.add(100);
    metrics.blocks_read.add(200);

    var buffer = std.ArrayList(u8).init(allocator);
    defer buffer.deinit();

    try metrics.format_human_readable(buffer.writer());
    const output = buffer.items;

    try testing.expect(std.mem.indexOf(u8, output, "100 written") != null);
    try testing.expect(std.mem.indexOf(u8, output, "200 read") != null);
    try testing.expect(std.mem.indexOf(u8, output, "Storage Metrics") != null);
}

test "json format produces valid json structure" {
    const allocator = testing.allocator;

    var metrics = StorageMetrics.init();
    metrics.blocks_written.add(50);
    metrics.wal_writes.add(75);

    var buffer = std.ArrayList(u8).init(allocator);
    defer buffer.deinit();

    try metrics.format_json(buffer.writer());
    const output = buffer.items;

    try testing.expect(std.mem.indexOf(u8, output, "\"blocks_written\": 50") != null);
    try testing.expect(std.mem.startsWith(u8, output, "{"));
    try testing.expect(std.mem.endsWith(u8, output, "}\n"));
}
