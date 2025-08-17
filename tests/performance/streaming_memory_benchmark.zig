//! Performance and memory efficiency tests for KausalDB operations.
//!
//! Tests streaming query formatting, storage engine throughput, memory management
//! efficiency, and performance regression detection across all major subsystems.

const kausaldb = @import("kausaldb");
const std = @import("std");
const testing = std.testing;

// Configure test output - use debug_print for performance debugging
const test_config = kausaldb.test_config;

const log = std.log.scoped(.streaming_memory_benchmark);

// Import tiered performance assertions
const PerformanceAssertion = kausaldb.PerformanceAssertion;
const PerformanceThresholds = kausaldb.PerformanceThresholds;
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

// Performance targets aligned with ProductionVFS benchmark results
// Base performance targets (local development, optimal conditions)
const BASE_BLOCK_WRITE_LATENCY_NS = 25_000; // 25µs base target (benchmark shows 19µs, without allocation overhead)
const BASE_BLOCK_READ_LATENCY_NS = 100; // 100ns base target (benchmark shows 34ns - excellent)
const BASE_QUERY_LATENCY_NS = 500; // 500ns base target (benchmark shows 76ns single, tests show 1000ns)
const BASE_BATCH_QUERY_LATENCY_NS = 2_000; // 2µs base target (benchmark shows 624ns)

// Test limits for performance validation
const MAX_BENCHMARK_DURATION_MS = 30_000;
const MAX_MEMORY_GROWTH_FACTOR = 10.0;
const MAX_STREAMING_OVERHEAD_PERCENT = 20.0;

test "memory efficiency during large dataset operations" {
    const allocator = testing.allocator;

    var perf_assertion = PerformanceAssertion.init("memory_efficiency");

    // Create unique database name with timestamp to ensure test isolation
    const timestamp = std.time.nanoTimestamp();
    const db_name = try std.fmt.allocPrint(allocator, "memory_efficiency_test_{}", .{timestamp});
    defer allocator.free(db_name);
    var harness = try kausaldb.ProductionHarness.init_and_startup(allocator, db_name);
    defer harness.deinit();

    // Disable immediate sync for performance testing
    harness.storage_engine().configure_wal_immediate_sync(false);

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
    const baseline_memory = harness.storage_engine().memory_usage().total_bytes;
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
            const current_memory = harness.storage_engine().memory_usage().total_bytes;
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

    // Use tier-based performance assertion for streaming latency
    // Memory efficiency streaming test - more realistic threshold for large dataset operations
    try perf_assertion.assert_latency(@intCast(avg_streaming_time_per_result), BASE_QUERY_LATENCY_NS * 10000, "streaming query result formatting");
}

test "query engine performance benchmark" {
    const allocator = testing.allocator;

    test_config.debug_print("DEBUG: Starting query engine performance benchmark\n", .{});

    // Create unique database name with timestamp to ensure test isolation
    const timestamp = std.time.nanoTimestamp();
    const db_name = try std.fmt.allocPrint(allocator, "query_perf_test_{}", .{timestamp});
    defer allocator.free(db_name);
    var harness = try kausaldb.ProductionHarness.init_and_startup(allocator, db_name);
    defer harness.deinit();

    // Disable immediate sync for performance testing
    harness.storage_engine().configure_wal_immediate_sync(false);

    test_config.debug_print("DEBUG: Harness initialized successfully\n", .{});

    // Phase 1: Setup test data with relationships
    const block_count = 500;
    var block_ids = std.ArrayList(BlockId).init(allocator);
    defer block_ids.deinit();
    try block_ids.ensureTotalCapacity(block_count);

    test_config.debug_print("DEBUG: Starting to create {} blocks\n", .{block_count});

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

        if (i % 100 == 0) {
            test_config.debug_print("DEBUG: Created {} blocks so far\n", .{i + 1});
        }
    }

    test_config.debug_print("DEBUG: Finished creating {} blocks\n", .{block_count});

    test_config.debug_print("DEBUG: Starting to create {} edges\n", .{block_count - 1});

    // Create edges for graph traversal testing
    for (0..block_count - 1) |i| {
        const edge = GraphEdge{
            .source_id = TestData.deterministic_block_id(@intCast(i)),
            .target_id = TestData.deterministic_block_id(@intCast(i + 1)),
            .edge_type = EdgeType.calls,
        };
        try harness.storage_engine().put_edge(edge);

        if (i % 100 == 0) {
            test_config.debug_print("DEBUG: Created {} edges so far\n", .{i + 1});
        }
    }

    test_config.debug_print("DEBUG: Finished creating edges, starting single query tests\n", .{});

    // Phase 3: Single block query performance
    var single_measurement = BatchPerformanceMeasurement.init(allocator);
    defer single_measurement.deinit();

    for (0..100) |_| {
        const random_index = std.crypto.random.intRangeLessThan(usize, 0, block_count);
        const target_id = block_ids.items[random_index];

        const start_time = std.time.nanoTimestamp();
        const found_block = try harness.storage_engine().find_block(target_id, .query_engine);
        const end_time = std.time.nanoTimestamp();

        try testing.expect(found_block != null);
        try single_measurement.add_measurement(@intCast(end_time - start_time));
    }

    // Pass base requirement directly to assert_statistics - it will apply tier multiplier internally
    try single_measurement.assert_statistics("query_engine_single", BASE_QUERY_LATENCY_NS, "single block query");

    // Phase 3: Batch query performance
    var batch_measurement = BatchPerformanceMeasurement.init(allocator);
    defer batch_measurement.deinit();

    // Debug: Print database state for performance analysis
    const db_stats = harness.storage_engine().memory_usage();
    std.debug.print("DB_STATE: blocks={}, edges={}, total_bytes={}\n", .{ db_stats.block_count, db_stats.edge_count, db_stats.total_bytes });

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

        const duration_ns = end_time - start_time;
        const per_block_ns = @divTrunc(duration_ns, found_count);

        // Always print performance data for debugging platform differences
        std.debug.print("BATCH_PERF: size={}, found={}, total={}ns ({}µs), per_block={}ns ({}µs)\n", .{ batch_size, found_count, duration_ns, @divTrunc(duration_ns, 1000), per_block_ns, @divTrunc(per_block_ns, 1000) });

        try testing.expect(found_count > 0);
        try batch_measurement.add_measurement(@intCast(duration_ns));
    }

    // Pass base requirement directly to assert_statistics - it will apply tier multiplier internally
    test_config.debug_print("DEBUG: About to call assert_statistics with target={}ns (base: {}ns)\n", .{BASE_BATCH_QUERY_LATENCY_NS * 2, BASE_BATCH_QUERY_LATENCY_NS});

    try batch_measurement.assert_statistics("query_engine_batch", BASE_BATCH_QUERY_LATENCY_NS, "batch block query");

    test_config.debug_print("DEBUG: Successfully completed batch query assertions\n", .{});

    log.info("Query performance completed successfully", .{});
}

test "storage engine write throughput measurement" {
    const allocator = testing.allocator;

    var perf_assertion = PerformanceAssertion.init("write_throughput");

    // Create unique database name with timestamp to ensure test isolation
    const timestamp = std.time.nanoTimestamp();
    const db_name = try std.fmt.allocPrint(allocator, "write_throughput_test_{}", .{timestamp});
    defer allocator.free(db_name);
    var harness = try kausaldb.ProductionHarness.init_and_startup(allocator, db_name);
    defer harness.deinit();

    // Disable immediate sync for performance testing
    // WARNING: This reduces durability guarantees but allows measuring optimal performance
    harness.storage_engine().configure_wal_immediate_sync(false);

    // Initialize performance assertion framework

    // Phase 1: Single block write performance
    var write_measurement = BatchPerformanceMeasurement.init(allocator);
    defer write_measurement.deinit();

    // PRE-ALLOCATE test blocks to eliminate allocation overhead from timing measurements
    const write_iterations = 100; // Reduced iterations, focus on storage performance
    var test_blocks = try allocator.alloc(ContextBlock, write_iterations);
    defer {
        for (test_blocks) |block| {
            TestData.cleanup_test_block(allocator, block);
        }
        allocator.free(test_blocks);
    }
    
    // Create all test blocks upfront
    for (0..write_iterations) |i| {
        test_blocks[i] = try TestData.create_test_block(allocator, @intCast(i + 1));
    }

    // Pure storage engine performance measurement
    for (0..write_iterations) |i| {
        const start_time = std.time.nanoTimestamp();
        try harness.storage_engine().put_block(test_blocks[i]);
        const end_time = std.time.nanoTimestamp();

        try write_measurement.add_measurement(@intCast(end_time - start_time));
    }

    // Pass base requirement directly to assert_statistics - it will apply tier multiplier internally
    try write_measurement.assert_statistics("storage_write_throughput", BASE_BLOCK_WRITE_LATENCY_NS, "block write operation");

    // Apply tiered performance assertions for write latency
    const write_stats = write_measurement.calculate_statistics();
    try perf_assertion.assert_latency(
        write_stats.mean_latency_ns,
        BASE_BLOCK_WRITE_LATENCY_NS,
        "mean block write latency",
    );

    try perf_assertion.assert_latency(
        write_stats.p99_latency_ns,
        BASE_BLOCK_WRITE_LATENCY_NS * 5, // 5x allowance for P99
        "P99 block write latency",
    );

    // Phase 2: Read performance validation
    var read_measurement = BatchPerformanceMeasurement.init(allocator);
    defer read_measurement.deinit();

    for (0..100) |i| {
        const target_block = try TestData.create_test_block(allocator, @intCast(i + 2000));
        defer TestData.cleanup_test_block(allocator, target_block);
        try harness.storage_engine().put_block(target_block);

        const start_time = std.time.nanoTimestamp();
        const found_block = try harness.storage_engine().find_block(target_block.id, .query_engine);
        const end_time = std.time.nanoTimestamp();

        try testing.expect(found_block != null);
        try read_measurement.add_measurement(@intCast(end_time - start_time));
    }

    const read_stats = read_measurement.calculate_statistics();

    try perf_assertion.assert_latency(
        read_stats.mean_latency_ns,
        BASE_BLOCK_READ_LATENCY_NS,
        "mean block read latency",
    );

    log.info("Storage performance: write_avg={d}µs, read_avg={d}µs", .{
        write_stats.mean_latency_ns / 1000,
        read_stats.mean_latency_ns / 1000,
    });
}

test "streaming query result formatting performance" {
    const allocator = testing.allocator;

    var perf_assertion = PerformanceAssertion.init("streaming_format");

    // Create unique database name with timestamp to ensure test isolation
    const timestamp = std.time.nanoTimestamp();
    const db_name = try std.fmt.allocPrint(allocator, "streaming_format_test_{}", .{timestamp});
    defer allocator.free(db_name);
    var harness = try kausaldb.ProductionHarness.init_and_startup(allocator, db_name);
    defer harness.deinit();

    // Disable immediate sync for performance testing
    harness.storage_engine().configure_wal_immediate_sync(false);

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
        try streaming_measurement.add_measurement(@intCast(end_time - start_time));
    }

    const streaming_stats = streaming_measurement.calculate_statistics();

    // Use tier-based performance assertion for streaming latency
    // Streaming includes query + format allocation overhead - much higher than query alone
    try perf_assertion.assert_latency(
        streaming_stats.mean_latency_ns,
        BASE_QUERY_LATENCY_NS * 3000, // 3000x base for streaming formatting overhead (1.2ms base)
        "streaming query result formatting",
    );

    // Streaming should not have excessive overhead
    const per_block_latency = streaming_stats.mean_latency_ns / (dataset_size / chunk_sizes.len);
    try testing.expect(per_block_latency < 1_000_000); // 1ms per block max

    log.info("Streaming performance: avg_per_block={d}µs", .{per_block_latency / 1000});
}

test "memory usage growth patterns under load" {
    const allocator = testing.allocator;

    const perf_assertion = PerformanceAssertion.init("memory_growth");

    // Create unique database name with timestamp to ensure test isolation
    const timestamp = std.time.nanoTimestamp();
    const db_name = try std.fmt.allocPrint(allocator, "memory_growth_test_{}", .{timestamp});
    defer allocator.free(db_name);
    var harness = try kausaldb.ProductionHarness.init_and_startup(allocator, db_name);
    defer harness.deinit();

    // Disable immediate sync for performance testing
    harness.storage_engine().configure_wal_immediate_sync(false);

    // Prevent unused variable warning until memory assertions are implemented
    _ = perf_assertion;

    const baseline_memory = harness.storage_engine().memory_usage().total_bytes;
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

        const current_memory = harness.storage_engine().memory_usage().total_bytes;
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
