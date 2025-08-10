//! Performance and memory efficiency tests for KausalDB operations.
//!
//! Tests streaming query formatting, storage engine throughput, memory management
//! efficiency, and performance regression detection across all major subsystems.

const kausaldb = @import("kausaldb");
const std = @import("std");
const testing = std.testing;

const log = std.log.scoped(.streaming_memory_benchmark);

// Import tiered performance assertions
const PerformanceAssertion = kausaldb.PerformanceAssertion;
const BatchPerformanceMeasurement = kausaldb.BatchPerformanceMeasurement;

const types = kausaldb.types;
const assert = kausaldb.assert.assert;

const ContextBlock = types.ContextBlock;
const BlockId = types.BlockId;
const GraphEdge = types.GraphEdge;
const EdgeType = types.EdgeType;
const TestData = kausaldb.TestData;

// Query operations and types
const operations = kausaldb.query_operations;
const FindBlocksQuery = kausaldb.FindBlocksQuery;

// Performance targets from PERFORMANCE.md
const TARGET_BLOCK_WRITE_LATENCY_NS = 50_000; // 50µs
const TARGET_BLOCK_READ_LATENCY_NS = 10_000; // 10µs
const TARGET_QUERY_LATENCY_NS = 10_000; // 10µs
const TARGET_BATCH_QUERY_LATENCY_NS = 100_000; // 100µs

// Test limits for performance validation
const MAX_BENCHMARK_DURATION_MS = 30_000;
const MAX_MEMORY_GROWTH_FACTOR = 10.0;
const MAX_STREAMING_OVERHEAD_PERCENT = 20.0;

test "memory efficiency during large dataset operations" {
    const allocator = testing.allocator;

    var harness = try kausaldb.QueryHarness.init_and_startup(allocator, "memory_efficiency_test");
    defer harness.deinit();

    // Phase 1: Memory baseline establishment - add a small block to establish non-zero baseline
    const baseline_block = ContextBlock{
        .id = TestData.deterministic_block_id(9999),
        .version = 1,
        .source_uri = "test://baseline_memory_measurement.zig",
        .metadata_json = "{\"test\":\"baseline_memory_measurement\"}",
        .content = "Baseline memory measurement block",
    };
    try harness.storage_engine().put_block(baseline_block);

    // Phase 2: Streaming query formation with varying result sizes
    const result_sizes = [_]usize{ 10, 50, 100, 500, 1000 };
    const baseline_memory = harness.storage_engine().memtable_manager.memory_usage();
    var peak_memory: u64 = baseline_memory;
    var total_streaming_time: i64 = 0;
    var total_streamed_count: usize = 0;

    for (result_sizes) |result_size| {
        // Prepare test blocks using TestData
        var block_ids = std.ArrayList(BlockId).init(allocator);
        defer block_ids.deinit();
        try block_ids.ensureTotalCapacity(result_size);

        for (0..result_size) |i| {
            const content = try allocator.alloc(u8, 256);
            defer allocator.free(content);
            @memset(content, @as(u8, @intCast('A' + (i % 26))));
            const block = ContextBlock{
                .id = TestData.deterministic_block_id(@intCast(i + 1)),
                .version = 1,
                .source_uri = "test://streaming_memory_benchmark.zig",
                .metadata_json = "{\"test\":\"streaming_memory_benchmark\"}",
                .content = content,
            };

            try harness.storage_engine().put_block(block);
            try block_ids.append(block.id);
        }

        // Measure streaming performance
        const start_time = std.time.nanoTimestamp();

        const query = FindBlocksQuery{ .block_ids = block_ids.items };
        var result = try operations.execute_find_blocks(allocator, harness.storage_engine(), query);

        // Stream through results
        var streamed_count: usize = 0;
        while (try result.next()) |block| {
            streamed_count += 1;

            // Track peak memory during streaming
            const current_memory = harness.storage_engine().memtable_manager.memory_usage();
            peak_memory = @max(peak_memory, current_memory);

            // Verify block integrity
            try testing.expect(block.read(.query_engine).content.len > 0);
        }

        const end_time = std.time.nanoTimestamp();
        total_streaming_time += @intCast(end_time - start_time);
        total_streamed_count += streamed_count;

        try testing.expectEqual(result_size, streamed_count);

        // Close result before flushing to avoid memory corruption
        result.deinit();

        // Flush memtable to prevent memory accumulation across iterations
        try harness.storage_engine().flush_memtable_to_sstable();
    }

    // Phase 3: Memory efficiency validation
    // For small baselines (< 1KB), use absolute limits instead of growth factors
    const min_baseline_for_growth_factor = 1024; // 1KB minimum

    if (baseline_memory < min_baseline_for_growth_factor) {
        const absolute_memory_limit_kb = 500; // 500KB absolute limit for small baseline
        const peak_memory_kb = peak_memory / 1024;

        std.debug.print("Memory efficiency (absolute): baseline={}B, peak={}KB (limit={}KB)\n", .{
            baseline_memory,
            peak_memory_kb,
            absolute_memory_limit_kb,
        });

        try testing.expect(peak_memory_kb < absolute_memory_limit_kb);
    } else {
        const memory_growth_factor = @as(f64, @floatFromInt(peak_memory)) / @as(f64, @floatFromInt(baseline_memory));

        std.debug.print("Memory efficiency: baseline={}KB, peak={}KB, growth_factor={d:.2} (limit={d:.2})\n", .{
            baseline_memory / 1024,
            peak_memory / 1024,
            memory_growth_factor,
            MAX_MEMORY_GROWTH_FACTOR,
        });

        try testing.expect(memory_growth_factor < MAX_MEMORY_GROWTH_FACTOR);
    }

    // Phase 4: Streaming performance validation
    const avg_streaming_time_per_result = @divTrunc(total_streaming_time, @as(i64, @intCast(result_sizes.len)));
    try testing.expect(avg_streaming_time_per_result < 10_000_000); // 10ms per result set
}

test "query engine performance benchmark" {
    const allocator = testing.allocator;

    var harness = try kausaldb.QueryHarness.init_and_startup(allocator, "query_perf_test");
    defer harness.deinit();

    // Phase 1: Setup test data with relationships
    const block_count = 500;
    var block_ids = std.ArrayList(BlockId).init(allocator);
    defer block_ids.deinit();
    try block_ids.ensureTotalCapacity(block_count);

    // Create blocks using TestData
    for (0..block_count) |i| {
        const block = ContextBlock{
            .id = TestData.deterministic_block_id(@intCast(i)),
            .version = 1,
            .source_uri = "test://memory_efficiency_benchmark.zig",
            .metadata_json = "{\"test\":\"memory_efficiency_benchmark\"}",
            .content = "Memory efficiency benchmark test block content",
        };

        try harness.storage_engine().put_block(block);
        try block_ids.append(block.id);
    }

    // Create edges for graph traversal testing
    for (0..block_count - 1) |i| {
        const edge = GraphEdge{
            .source_id = TestData.deterministic_block_id(@intCast(i)),
            .target_id = TestData.deterministic_block_id(@intCast(i + 1)),
            .edge_type = EdgeType.calls,
        };
        try harness.storage_engine().put_edge(edge);
    }

    // Phase 3: Single block query performance
    var single_measurement = BatchPerformanceMeasurement.init(allocator);
    defer single_measurement.deinit();

    for (0..100) |_| {
        const random_index = std.crypto.random.intRangeLessThan(usize, 0, block_count);
        const target_id = block_ids.items[random_index];

        const start_time = std.time.nanoTimestamp();
        const found_block = try harness.storage_engine().find_block(target_id);
        const end_time = std.time.nanoTimestamp();

        try testing.expect(found_block != null);
        try single_measurement.add_measurement(@intCast(end_time - start_time));
    }

    try single_measurement.assert_statistics("query_engine_single", TARGET_QUERY_LATENCY_NS, "single block query");

    // Phase 3: Batch query performance
    var batch_measurement = BatchPerformanceMeasurement.init(allocator);
    defer batch_measurement.deinit();

    const batch_sizes = [_]usize{ 5, 10, 25, 50 };
    for (batch_sizes) |batch_size| {
        var batch_ids = try allocator.alloc(BlockId, batch_size);
        defer allocator.free(batch_ids);

        for (0..batch_size) |i| {
            const random_index = std.crypto.random.intRangeLessThan(usize, 0, block_count);
            batch_ids[i] = block_ids.items[random_index];
        }

        const start_time = std.time.nanoTimestamp();
        const query = FindBlocksQuery{ .block_ids = batch_ids };
        var result = try operations.execute_find_blocks(allocator, harness.storage_engine(), query);
        defer result.deinit();

        var found_count: usize = 0;
        while (try result.next()) |_| {
            found_count += 1;
        }
        const end_time = std.time.nanoTimestamp();

        try testing.expect(found_count > 0);
        try batch_measurement.add_measurement(@intCast(end_time - start_time));
    }

    try batch_measurement.assert_statistics("query_engine_batch", TARGET_BATCH_QUERY_LATENCY_NS, "batch block query");

    log.info("Query performance completed successfully", .{});
}

test "storage engine write throughput measurement" {
    const allocator = testing.allocator;

    var harness = try kausaldb.StorageHarness.init_and_startup(allocator, "write_throughput_test");
    defer harness.deinit();

    // Initialize performance assertion framework
    const perf = PerformanceAssertion.init("storage_write_throughput");

    // Phase 1: Single block write performance
    var write_measurement = BatchPerformanceMeasurement.init(allocator);
    defer write_measurement.deinit();

    const write_iterations = 1000;
    for (0..write_iterations) |i| {
        const block = try TestData.create_test_block(allocator, @intCast(i + 1));
        defer TestData.cleanup_test_block(allocator, block);

        const start_time = std.time.nanoTimestamp();
        try harness.storage_engine.put_block(block);
        const end_time = std.time.nanoTimestamp();

        try write_measurement.add_measurement(@intCast(end_time - start_time));
    }

    try write_measurement.assert_statistics("storage_write_throughput", TARGET_BLOCK_WRITE_LATENCY_NS, "block write operation");

    // Apply tiered performance assertions for write latency
    const write_stats = write_measurement.calculate_statistics();
    try perf.assert_mean_latency_within_target(
        write_stats.mean_latency_ns,
        TARGET_BLOCK_WRITE_LATENCY_NS,
    );

    try perf.assert_p99_latency_acceptable(
        write_stats.p99_latency_ns,
        TARGET_BLOCK_WRITE_LATENCY_NS * 5, // 5x allowance for P99
    );

    // Phase 2: Read performance validation
    var read_measurement = BatchPerformanceMeasurement.init(allocator);
    defer read_measurement.deinit();

    for (0..100) |i| {
        const target_block = try TestData.create_test_block(allocator, @intCast(i + 2000));
        defer TestData.cleanup_test_block(allocator, target_block);
        try harness.storage_engine.put_block(target_block);

        const start_time = std.time.nanoTimestamp();
        const found_block = try harness.storage_engine.find_block(target_block.id);
        const end_time = std.time.nanoTimestamp();

        try testing.expect(found_block != null);
        try read_measurement.record_latency_ns(@intCast(end_time - start_time));
    }

    const read_stats = read_measurement.calculate_statistics();

    try perf.assert_mean_latency_within_target(
        read_stats.mean_latency_ns,
        TARGET_BLOCK_READ_LATENCY_NS,
    );

    log.info("Storage performance: write_avg={d}µs, read_avg={d}µs", .{
        write_stats.mean_latency_ns / 1000,
        read_stats.mean_latency_ns / 1000,
    });
}

test "streaming query result formatting performance" {
    const allocator = testing.allocator;

    var harness = try kausaldb.QueryHarness.init_and_startup(allocator, "streaming_benchmark_test");
    defer harness.deinit();

    // Phase 1: Setup diverse dataset
    const dataset_size = 200;
    var block_ids = std.ArrayList(BlockId).init(allocator);
    defer block_ids.deinit();
    try block_ids.ensureTotalCapacity(dataset_size);

    for (0..dataset_size) |i| {
        const content_size = 256 + @as(u32, @intCast(i % 512));
        const content = try allocator.alloc(u8, content_size);
        defer allocator.free(content);
        @memset(content, @as(u8, @intCast('A' + (i % 26))));
        const block = ContextBlock{
            .id = TestData.deterministic_block_id(@intCast(i + 1)),
            .version = 1,
            .source_uri = "test://streaming_result_formatting.zig",
            .metadata_json = "{\"test\":\"streaming_result_formatting\"}",
            .content = content,
        };

        try harness.storage_engine().put_block(block);
        try block_ids.append(block.id);
    }

    // Phase 2: Streaming performance measurement
    var streaming_measurement = BatchPerformanceMeasurement.init(allocator);
    defer streaming_measurement.deinit();

    const chunk_sizes = [_]usize{ 10, 25, 50, 100 };
    for (chunk_sizes) |chunk_size| {
        const chunk_end = @min(chunk_size, block_ids.items.len);
        const chunk_ids = block_ids.items[0..chunk_end];

        const start_time = std.time.nanoTimestamp();

        const query = FindBlocksQuery{ .block_ids = chunk_ids };
        var result = try operations.execute_find_blocks(allocator, harness.storage_engine(), query);
        defer result.deinit();

        var formatted_count: usize = 0;
        while (try result.next()) |block| {
            // Simulate LLM formatting overhead
            const formatted = try std.fmt.allocPrint(allocator, "Formatted: {s}", .{block.read(.query_engine).content});
            defer allocator.free(formatted);

            try testing.expect(formatted.len > 0);
            formatted_count += 1;
        }

        const end_time = std.time.nanoTimestamp();

        try testing.expectEqual(chunk_end, formatted_count);
        try streaming_measurement.record_latency_ns(@intCast(end_time - start_time));
    }

    const streaming_stats = streaming_measurement.calculate_statistics();

    // Streaming should not have excessive overhead
    const per_block_latency = streaming_stats.mean_latency_ns / (dataset_size / chunk_sizes.len);
    try testing.expect(per_block_latency < 1_000_000); // 1ms per block max

    log.info("Streaming performance: avg_per_block={d}µs", .{per_block_latency / 1000});
}

test "memory usage growth patterns under load" {
    const allocator = testing.allocator;

    var harness = try kausaldb.QueryHarness.init_and_startup(allocator, "memory_growth_test");
    defer harness.deinit();

    const baseline_memory = harness.storage_engine().memtable_manager.memory_usage();
    var memory_samples = std.ArrayList(u64).init(allocator);
    defer memory_samples.deinit();
    try memory_samples.ensureTotalCapacity(5);

    // Phase 1: Gradual load increase
    const load_phases = [_]usize{ 50, 100, 200, 400, 800 };
    for (load_phases) |phase_size| {
        for (0..phase_size) |i| {
            const block = ContextBlock{
                .id = TestData.deterministic_block_id(@intCast(i + phase_size * 1000)),
                .version = 1,
                .source_uri = "test://fragmentation_benchmark.zig",
                .metadata_json = "{\"test\":\"fragmentation_benchmark\"}",
                .content = "Fragmentation benchmark test block content",
            };
            try harness.storage_engine().put_block(block);
        }

        const current_memory = harness.storage_engine().memtable_manager.memory_usage();
        try memory_samples.append(current_memory);

        // Flush memtable after each phase to isolate memory growth patterns
        try harness.storage_engine().flush_memtable_to_sstable();
    }

    // Phase 2: Memory growth analysis
    var max_growth_rate: f64 = 0.0;
    for (1..memory_samples.items.len) |i| {
        const prev_memory = memory_samples.items[i - 1];
        const curr_memory = memory_samples.items[i];

        if (prev_memory > 0) {
            const growth_rate = @as(f64, @floatFromInt(curr_memory)) / @as(f64, @floatFromInt(prev_memory));
            max_growth_rate = @max(max_growth_rate, growth_rate);
        }
    }

    // Memory growth should be roughly linear, not exponential
    try testing.expect(max_growth_rate < 3.0); // No more than 3x growth between phases

    const final_memory = memory_samples.items[memory_samples.items.len - 1];

    // Handle small baseline with absolute limits instead of growth factors
    const min_baseline_for_growth_factor = 1024; // 1KB minimum

    if (baseline_memory < min_baseline_for_growth_factor) {
        const absolute_memory_limit_kb = 1000; // 1MB absolute limit for small baseline
        const final_memory_kb = final_memory / 1024;

        log.info("Memory growth (absolute): baseline={}B, final={}KB (limit={}KB)", .{
            baseline_memory,
            final_memory_kb,
            absolute_memory_limit_kb,
        });

        try testing.expect(final_memory_kb < absolute_memory_limit_kb);
    } else {
        const total_growth_factor = @as(f64, @floatFromInt(final_memory)) / @as(f64, @floatFromInt(baseline_memory));

        log.info("Memory growth: baseline={}KB, final={}KB, factor={d:.2}", .{
            baseline_memory / 1024,
            final_memory / 1024,
            total_growth_factor,
        });

        try testing.expect(total_growth_factor < 20.0); // Less than 20x growth
    }
}
