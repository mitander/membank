//! Storage engine performance metrics and observability.
//!
//! Provides atomic counters and analytics for storage operations to enable
//! performance monitoring, capacity planning, and operational debugging.
//! All counters use atomic operations to support safe concurrent access
//! from monitoring threads without impacting storage performance.

const std = @import("std");
const assert = @import("../core/assert.zig").assert;

/// Performance metrics for storage engine observability.
/// Atomic counters enable safe concurrent access from monitoring threads
/// without requiring locks that would impact storage engine performance.
pub const StorageMetrics = struct {
    // Block operations - core storage workload metrics
    blocks_written: std.atomic.Value(u64),
    blocks_read: std.atomic.Value(u64),
    blocks_deleted: std.atomic.Value(u64),

    // WAL operations - durability and recovery metrics
    wal_writes: std.atomic.Value(u64),
    wal_flushes: std.atomic.Value(u64),
    wal_recoveries: std.atomic.Value(u64),

    // SSTable operations - compaction and read path metrics
    sstable_reads: std.atomic.Value(u64),
    sstable_writes: std.atomic.Value(u64),
    compactions: std.atomic.Value(u64),

    // Edge operations - graph relationship metrics
    edges_added: std.atomic.Value(u64),
    edges_removed: std.atomic.Value(u64),

    // Performance timings in nanoseconds for latency analysis
    total_write_time_ns: std.atomic.Value(u64),
    total_read_time_ns: std.atomic.Value(u64),
    total_wal_flush_time_ns: std.atomic.Value(u64),

    // Error counts for reliability monitoring
    write_errors: std.atomic.Value(u64),
    read_errors: std.atomic.Value(u64),
    wal_errors: std.atomic.Value(u64),

    // Storage utilization metrics for capacity planning
    total_bytes_written: std.atomic.Value(u64),
    total_bytes_read: std.atomic.Value(u64),
    wal_bytes_written: std.atomic.Value(u64),
    sstable_bytes_written: std.atomic.Value(u64),

    /// Initialize all metrics to zero.
    /// Atomic values must be explicitly initialized to ensure deterministic behavior.
    pub fn init() StorageMetrics {
        return StorageMetrics{
            .blocks_written = std.atomic.Value(u64).init(0),
            .blocks_read = std.atomic.Value(u64).init(0),
            .blocks_deleted = std.atomic.Value(u64).init(0),
            .wal_writes = std.atomic.Value(u64).init(0),
            .wal_flushes = std.atomic.Value(u64).init(0),
            .wal_recoveries = std.atomic.Value(u64).init(0),
            .sstable_reads = std.atomic.Value(u64).init(0),
            .sstable_writes = std.atomic.Value(u64).init(0),
            .compactions = std.atomic.Value(u64).init(0),
            .edges_added = std.atomic.Value(u64).init(0),
            .edges_removed = std.atomic.Value(u64).init(0),
            .total_write_time_ns = std.atomic.Value(u64).init(0),
            .total_read_time_ns = std.atomic.Value(u64).init(0),
            .total_wal_flush_time_ns = std.atomic.Value(u64).init(0),
            .write_errors = std.atomic.Value(u64).init(0),
            .read_errors = std.atomic.Value(u64).init(0),
            .wal_errors = std.atomic.Value(u64).init(0),
            .total_bytes_written = std.atomic.Value(u64).init(0),
            .total_bytes_read = std.atomic.Value(u64).init(0),
            .wal_bytes_written = std.atomic.Value(u64).init(0),
            .sstable_bytes_written = std.atomic.Value(u64).init(0),
        };
    }

    /// Calculate average write latency in nanoseconds.
    /// Returns 0 if no writes have occurred to avoid division by zero.
    pub fn average_write_latency_ns(self: *const StorageMetrics) u64 {
        const writes = self.blocks_written.load(.monotonic);
        if (writes == 0) return 0;
        return self.total_write_time_ns.load(.monotonic) / writes;
    }

    /// Calculate average read latency in nanoseconds.
    /// Returns 0 if no reads have occurred to avoid division by zero.
    pub fn average_read_latency_ns(self: *const StorageMetrics) u64 {
        const reads = self.blocks_read.load(.monotonic);
        if (reads == 0) return 0;
        return self.total_read_time_ns.load(.monotonic) / reads;
    }

    /// Calculate average WAL flush latency in nanoseconds.
    /// Returns 0 if no flushes have occurred to avoid division by zero.
    pub fn average_wal_flush_latency_ns(self: *const StorageMetrics) u64 {
        const flushes = self.wal_flushes.load(.monotonic);
        if (flushes == 0) return 0;
        return self.total_wal_flush_time_ns.load(.monotonic) / flushes;
    }

    /// Calculate average bytes per block written.
    /// Returns 0 if no blocks have been written to avoid division by zero.
    pub fn average_block_size_bytes(self: *const StorageMetrics) u64 {
        const blocks = self.blocks_written.load(.monotonic);
        if (blocks == 0) return 0;
        return self.total_bytes_written.load(.monotonic) / blocks;
    }

    /// Calculate storage write throughput in bytes per second.
    /// Returns 0.0 if no time has elapsed to avoid division by zero.
    pub fn write_throughput_bps(self: *const StorageMetrics) f64 {
        const total_time_ns = self.total_write_time_ns.load(.monotonic);
        const total_time_seconds = @as(f64, @floatFromInt(total_time_ns)) / 1_000_000_000.0;
        if (total_time_seconds == 0.0) return 0.0;
        const total_bytes = self.total_bytes_written.load(.monotonic);
        return @as(f64, @floatFromInt(total_bytes)) / total_time_seconds;
    }

    /// Calculate storage read throughput in bytes per second.
    /// Returns 0.0 if no time has elapsed to avoid division by zero.
    pub fn read_throughput_bps(self: *const StorageMetrics) f64 {
        const total_time_ns = self.total_read_time_ns.load(.monotonic);
        const total_time_seconds = @as(f64, @floatFromInt(total_time_ns)) / 1_000_000_000.0;
        if (total_time_seconds == 0.0) return 0.0;
        const total_bytes = self.total_bytes_read.load(.monotonic);
        return @as(f64, @floatFromInt(total_bytes)) / total_time_seconds;
    }

    /// Format metrics as human-readable text for operational dashboards.
    /// Provides key performance indicators in an easily scannable format.
    pub fn format_human_readable(self: *const StorageMetrics, writer: anytype) !void {
        try writer.writeAll("=== Storage Metrics ===\n");
        try writer.print("Blocks: {} written, {} read, {} deleted\n", .{
            self.blocks_written.load(.monotonic),
            self.blocks_read.load(.monotonic),
            self.blocks_deleted.load(.monotonic),
        });
        try writer.print("WAL: {} writes, {} flushes, {} recoveries\n", .{
            self.wal_writes.load(.monotonic),
            self.wal_flushes.load(.monotonic),
            self.wal_recoveries.load(.monotonic),
        });
        try writer.print("SSTable: {} reads, {} writes, {} compactions\n", .{
            self.sstable_reads.load(.monotonic),
            self.sstable_writes.load(.monotonic),
            self.compactions.load(.monotonic),
        });
        try writer.print("Edges: {} added, {} removed\n", .{
            self.edges_added.load(.monotonic),
            self.edges_removed.load(.monotonic),
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
            @as(f64, @floatFromInt(self.total_bytes_written.load(.monotonic))) / (1024.0 * 1024.0),
            @as(f64, @floatFromInt(self.total_bytes_read.load(.monotonic))) / (1024.0 * 1024.0),
            @as(f64, @floatFromInt(self.average_block_size_bytes())) / 1024.0,
        });
        try writer.print("Errors: {} write, {} read, {} WAL\n", .{
            self.write_errors.load(.monotonic),
            self.read_errors.load(.monotonic),
            self.wal_errors.load(.monotonic),
        });
    }

    /// Format metrics as JSON for programmatic consumption by monitoring systems.
    /// Provides structured data format suitable for time-series databases and alerts.
    pub fn format_json(self: *const StorageMetrics, writer: anytype) !void {
        try writer.writeAll("{\n");
        try writer.print("  \"blocks_written\": {},\n", .{self.blocks_written.load(.monotonic)});
        try writer.print("  \"blocks_read\": {},\n", .{self.blocks_read.load(.monotonic)});
        try writer.print("  \"blocks_deleted\": {},\n", .{self.blocks_deleted.load(.monotonic)});
        try writer.print("  \"wal_writes\": {},\n", .{self.wal_writes.load(.monotonic)});
        try writer.print("  \"wal_flushes\": {},\n", .{self.wal_flushes.load(.monotonic)});
        try writer.print("  \"wal_recoveries\": {},\n", .{self.wal_recoveries.load(.monotonic)});
        try writer.print("  \"sstable_reads\": {},\n", .{self.sstable_reads.load(.monotonic)});
        try writer.print("  \"sstable_writes\": {},\n", .{self.sstable_writes.load(.monotonic)});
        try writer.print("  \"compactions\": {},\n", .{self.compactions.load(.monotonic)});
        try writer.print("  \"edges_added\": {},\n", .{self.edges_added.load(.monotonic)});
        try writer.print("  \"edges_removed\": {},\n", .{self.edges_removed.load(.monotonic)});
        try writer.print("  \"total_bytes_written\": {},\n", .{self.total_bytes_written.load(.monotonic)});
        try writer.print("  \"total_bytes_read\": {},\n", .{self.total_bytes_read.load(.monotonic)});
        try writer.print("  \"wal_bytes_written\": {},\n", .{self.wal_bytes_written.load(.monotonic)});
        try writer.print("  \"sstable_bytes_written\": {},\n", .{self.sstable_bytes_written.load(.monotonic)});
        try writer.print("  \"average_write_latency_ns\": {},\n", .{self.average_write_latency_ns()});
        try writer.print("  \"average_read_latency_ns\": {},\n", .{self.average_read_latency_ns()});
        try writer.print("  \"average_wal_flush_latency_ns\": {},\n", .{self.average_wal_flush_latency_ns()});
        try writer.print("  \"average_block_size_bytes\": {},\n", .{self.average_block_size_bytes()});
        try writer.print("  \"write_throughput_bps\": {d:.2},\n", .{self.write_throughput_bps()});
        try writer.print("  \"read_throughput_bps\": {d:.2},\n", .{self.read_throughput_bps()});
        try writer.print("  \"write_errors\": {},\n", .{self.write_errors.load(.monotonic)});
        try writer.print("  \"read_errors\": {},\n", .{self.read_errors.load(.monotonic)});
        try writer.print("  \"wal_errors\": {}\n", .{self.wal_errors.load(.monotonic)});
        try writer.writeAll("}\n");
    }
};

// Tests
const testing = std.testing;

test "metrics initialization sets all counters to zero" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();

    const metrics = StorageMetrics.init();

    try testing.expectEqual(@as(u64, 0), metrics.blocks_written.load(.monotonic));
    try testing.expectEqual(@as(u64, 0), metrics.blocks_read.load(.monotonic));
    try testing.expectEqual(@as(u64, 0), metrics.total_write_time_ns.load(.monotonic));
    try testing.expectEqual(@as(u64, 0), metrics.write_errors.load(.monotonic));
}

test "average calculations handle zero operations gracefully" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();

    const metrics = StorageMetrics.init();

    try testing.expectEqual(@as(u64, 0), metrics.average_write_latency_ns());
    try testing.expectEqual(@as(u64, 0), metrics.average_read_latency_ns());
    try testing.expectEqual(@as(u64, 0), metrics.average_wal_flush_latency_ns());
    try testing.expectEqual(@as(u64, 0), metrics.average_block_size_bytes());
    try testing.expectEqual(@as(f64, 0.0), metrics.write_throughput_bps());
    try testing.expectEqual(@as(f64, 0.0), metrics.read_throughput_bps());
}

test "average write latency calculation" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();

    var metrics = StorageMetrics.init();

    // Simulate 2 writes taking 1000ns and 2000ns respectively
    metrics.blocks_written.store(2, .monotonic);
    metrics.total_write_time_ns.store(3000, .monotonic);

    try testing.expectEqual(@as(u64, 1500), metrics.average_write_latency_ns());
}

test "write throughput calculation" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();

    var metrics = StorageMetrics.init();

    // Simulate 1MB written in 1 second (1_000_000_000 ns)
    metrics.total_bytes_written.store(1024 * 1024, .monotonic);
    metrics.total_write_time_ns.store(1_000_000_000, .monotonic);

    const throughput = metrics.write_throughput_bps();
    try testing.expectEqual(@as(f64, 1024.0 * 1024.0), throughput);
}

test "average block size calculation" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();

    var metrics = StorageMetrics.init();

    // Simulate 4 blocks totaling 8KB
    metrics.blocks_written.store(4, .monotonic);
    metrics.total_bytes_written.store(8192, .monotonic);

    try testing.expectEqual(@as(u64, 2048), metrics.average_block_size_bytes());
}

test "human readable format contains key metrics" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const allocator = arena.allocator();

    var metrics = StorageMetrics.init();
    metrics.blocks_written.store(100, .monotonic);
    metrics.blocks_read.store(200, .monotonic);

    var buffer = std.ArrayList(u8).init(allocator);
    defer buffer.deinit();

    try metrics.format_human_readable(buffer.writer());
    const output = buffer.items;

    try testing.expect(std.mem.indexOf(u8, output, "100 written") != null);
    try testing.expect(std.mem.indexOf(u8, output, "200 read") != null);
    try testing.expect(std.mem.indexOf(u8, output, "Storage Metrics") != null);
}

test "json format produces valid json structure" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const allocator = arena.allocator();

    var metrics = StorageMetrics.init();
    metrics.blocks_written.store(50, .monotonic);
    metrics.wal_writes.store(75, .monotonic);

    var buffer = std.ArrayList(u8).init(allocator);
    defer buffer.deinit();

    try metrics.format_json(buffer.writer());
    const output = buffer.items;

    try testing.expect(std.mem.indexOf(u8, output, "\"blocks_written\": 50") != null);
    try testing.expect(std.mem.indexOf(u8, output, "\"wal_writes\": 75") != null);
    try testing.expect(std.mem.startsWith(u8, output, "{"));
    try testing.expect(std.mem.endsWith(u8, output, "}\n"));
}

test "atomic operations work correctly under concurrent access simulation" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();

    var metrics = StorageMetrics.init();

    // Simulate concurrent increments
    for (0..100) |_| {
        _ = metrics.blocks_written.fetchAdd(1, .monotonic);
        _ = metrics.total_bytes_written.fetchAdd(1024, .monotonic);
    }

    try testing.expectEqual(@as(u64, 100), metrics.blocks_written.load(.monotonic));
    try testing.expectEqual(@as(u64, 102400), metrics.total_bytes_written.load(.monotonic));
    try testing.expectEqual(@as(u64, 1024), metrics.average_block_size_bytes());
}
