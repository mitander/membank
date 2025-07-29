//! Comprehensive performance and memory efficiency tests for CortexDB operations.
//!
//! Tests streaming query formatting, storage engine throughput, memory management
//! efficiency, and performance regression detection across all major subsystems.

const cortexdb = @import("cortexdb");
const std = @import("std");
const testing = std.testing;

const storage = cortexdb.storage;
const query = cortexdb.query;
const simulation_vfs = cortexdb.simulation_vfs;
const context_block = cortexdb.types;
const concurrency = cortexdb.concurrency;
const assert = cortexdb.assert.assert;

const StorageEngine = storage.StorageEngine;
const QueryEngine = query.QueryEngine;
const SimulationVFS = simulation_vfs.SimulationVFS;
const ContextBlock = context_block.ContextBlock;
const BlockId = context_block.BlockId;
const GraphEdge = context_block.GraphEdge;
const EdgeType = context_block.EdgeType;

// Performance targets from PERFORMANCE.md
const TARGET_BLOCK_WRITE_LATENCY_NS = 50_000; // 50µs
const TARGET_BLOCK_READ_LATENCY_NS = 10_000; // 10µs
const TARGET_QUERY_LATENCY_NS = 10_000; // 10µs
const TARGET_BATCH_QUERY_LATENCY_NS = 100_000; // 100µs

// Test limits for performance validation
const MAX_BENCHMARK_DURATION_MS = 30_000;
const MIN_OPERATIONS_FOR_STATS = 100;
const PERFORMANCE_SAMPLES = 50;
const WARMUP_OPERATIONS = 25;

fn create_test_block(id: BlockId, size: usize, allocator: std.mem.Allocator) !ContextBlock {
    const content = try allocator.alloc(u8, size);
    @memset(content, @intCast(id & 0xFF));

    return ContextBlock{
        .id = id,
        .version = 1,
        .source_uri = "test://performance_benchmark.zig",
        .metadata_json = "{}",
        .content = content,
    };
}

fn measure_operation_latency(comptime operation_fn: anytype, args: anytype) i64 {
    const start = std.time.nanoTimestamp();
    _ = operation_fn(args) catch unreachable;
    return std.time.nanoTimestamp() - start;
}

test "streaming_memory_efficiency_benchmark" {
    const allocator = testing.allocator;

    var sim_vfs = try SimulationVFS.init(allocator);
    defer sim_vfs.deinit();

    var storage_engine = try StorageEngine.init_default(allocator, sim_vfs.vfs(), "streaming_perf_test");
    defer storage_engine.deinit();

    try storage_engine.startup();

    var query_engine = try QueryEngine.init(allocator, &storage_engine);
    defer query_engine.deinit();

    // Phase 1: Memory baseline measurement
    const baseline_memory = query_engine.memory_usage();

    // Phase 2: Streaming query formation with varying result sizes
    const result_sizes = [_]usize{ 10, 50, 100, 500, 1000 };
    var peak_memory: u64 = baseline_memory;
    var total_streaming_time: i64 = 0;

    for (result_sizes) |result_size| {
        // Prepare test blocks
        for (0..result_size) |i| {
            const block = try create_test_block(@intCast(i + 1), 256, allocator);
            defer allocator.free(block.content);
            try storage_engine.put_block(block);
        }

        // Measure streaming query performance
        const stream_start = std.time.nanoTimestamp();

        var result_buffer = std.ArrayList(ContextBlock).init(allocator);
        defer {
            for (result_buffer.items) |block| {
                allocator.free(block.content);
            }
            result_buffer.deinit();
        }

        // Stream results to simulate real query formatting
        for (1..result_size + 1) |i| {
            if (try storage_engine.find_block(@intCast(i))) |block| {
                const owned_content = try allocator.dupe(u8, block.content);
                const owned_block = ContextBlock{
                    .id = block.id,
                    .version = block.version,
                    .source_uri = block.source_uri,
                    .metadata_json = block.metadata_json,
                    .content = owned_content,
                };
                try result_buffer.append(owned_block);
            }
        }

        const stream_time = std.time.nanoTimestamp() - stream_start;
        total_streaming_time += stream_time;

        // Track peak memory usage during streaming
        const current_memory = query_engine.memory_usage();
        if (current_memory > peak_memory) {
            peak_memory = current_memory;
        }

        // Verify streaming maintained reasonable memory usage
        const memory_growth = current_memory - baseline_memory;
        const expected_growth = result_size * 256; // Approximate per-block overhead
        try testing.expect(memory_growth < expected_growth * 2); // 2x tolerance
    }

    // Phase 3: Memory efficiency validation
    const average_streaming_latency = @divTrunc(total_streaming_time, result_sizes.len);
    const memory_overhead = peak_memory - baseline_memory;

    // Memory overhead should be reasonable for streaming operations
    try testing.expect(memory_overhead < 10 * 1024 * 1024); // <10MB overhead

    // Streaming should complete within reasonable time
    try testing.expect(average_streaming_latency < 1_000_000); // <1ms average
}

test "storage_engine_throughput_benchmark" {
    const allocator = testing.allocator;

    var sim_vfs = try SimulationVFS.init(allocator);
    defer sim_vfs.deinit();

    var engine = try StorageEngine.init_default(allocator, sim_vfs.vfs(), "throughput_test");
    defer engine.deinit();

    try engine.startup();

    // Phase 1: Write throughput measurement
    const write_operations = 1000;
    const write_start = std.time.nanoTimestamp();

    for (1..write_operations + 1) |i| {
        const block = try create_test_block(@intCast(i), 512, allocator);
        defer allocator.free(block.content);
        try engine.put_block(block);
    }

    const write_time = std.time.nanoTimestamp() - write_start;
    const write_throughput = @divTrunc(write_operations * 1_000_000_000, write_time);
    const avg_write_latency = @divTrunc(write_time, write_operations);

    // Validate write performance targets
    try testing.expect(avg_write_latency < TARGET_BLOCK_WRITE_LATENCY_NS);
    try testing.expect(write_throughput > 10_000); // >10K ops/sec

    // Phase 2: Read throughput measurement
    const read_start = std.time.nanoTimestamp();

    for (1..write_operations + 1) |i| {
        const block = try engine.find_block(@intCast(i));
        try testing.expect(block != null);
    }

    const read_time = std.time.nanoTimestamp() - read_start;
    const read_throughput = @divTrunc(write_operations * 1_000_000_000, read_time);
    const avg_read_latency = @divTrunc(read_time, write_operations);

    // Validate read performance targets
    try testing.expect(avg_read_latency < TARGET_BLOCK_READ_LATENCY_NS);
    try testing.expect(read_throughput > 100_000); // >100K ops/sec

    // Phase 3: Mixed workload performance
    const mixed_start = std.time.nanoTimestamp();
    var mixed_operations: u32 = 0;

    while (mixed_operations < 500) {
        // 70% reads, 30% writes
        if (mixed_operations % 10 < 7) {
            const id = (mixed_operations % write_operations) + 1;
            const block = try engine.find_block(@intCast(id));
            try testing.expect(block != null);
        } else {
            const new_id = write_operations + mixed_operations + 1;
            const block = try create_test_block(@intCast(new_id), 512, allocator);
            defer allocator.free(block.content);
            try engine.put_block(block);
        }
        mixed_operations += 1;
    }

    const mixed_time = std.time.nanoTimestamp() - mixed_start;
    const mixed_throughput = @divTrunc(mixed_operations * 1_000_000_000, mixed_time);

    // Mixed workload should maintain reasonable throughput
    try testing.expect(mixed_throughput > 20_000); // >20K ops/sec
}

test "query_engine_performance_benchmark" {
    const allocator = testing.allocator;

    var sim_vfs = try SimulationVFS.init(allocator);
    defer sim_vfs.deinit();

    var storage_engine = try StorageEngine.init_default(allocator, sim_vfs.vfs(), "query_perf_test");
    defer storage_engine.deinit();

    try storage_engine.startup();

    var query_engine = try QueryEngine.init(allocator, &storage_engine);
    defer query_engine.deinit();

    // Phase 1: Setup test data with relationships
    const block_count = 500;
    for (1..block_count + 1) |i| {
        const block = try create_test_block(@intCast(i), 256, allocator);
        defer allocator.free(block.content);
        try storage_engine.put_block(block);

        // Create edges for graph traversal testing
        if (i > 1) {
            const edge = GraphEdge{
                .source_id = @intCast(i - 1),
                .target_id = @intCast(i),
                .edge_type = .CALLS,
            };
            try storage_engine.put_edge(edge);
        }
    }

    // Phase 2: Single block query performance
    var single_query_times = std.ArrayList(i64).init(allocator);
    defer single_query_times.deinit();

    for (0..PERFORMANCE_SAMPLES) |_| {
        const query_id = (@mod(std.crypto.random.int(u32), block_count)) + 1;

        const start = std.time.nanoTimestamp();
        const result = try query_engine.find_blocks(&[_]BlockId{@intCast(query_id)}, allocator);
        const end = std.time.nanoTimestamp();

        defer allocator.free(result);
        try single_query_times.append(end - start);
    }

    const avg_single_query = calculate_average(single_query_times.items);
    try testing.expect(avg_single_query < TARGET_QUERY_LATENCY_NS);

    // Phase 3: Batch query performance
    var batch_query_times = std.ArrayList(i64).init(allocator);
    defer batch_query_times.deinit();

    for (0..PERFORMANCE_SAMPLES) |_| {
        var query_ids: [10]BlockId = undefined;
        for (&query_ids, 0..) |*id, j| {
            id.* = @intCast((@mod(std.crypto.random.int(u32), block_count)) + 1);
            _ = j;
        }

        const start = std.time.nanoTimestamp();
        const result = try query_engine.find_blocks(&query_ids, allocator);
        const end = std.time.nanoTimestamp();

        defer allocator.free(result);
        try batch_query_times.append(end - start);
    }

    const avg_batch_query = calculate_average(batch_query_times.items);
    try testing.expect(avg_batch_query < TARGET_BATCH_QUERY_LATENCY_NS);

    // Phase 4: Graph traversal performance
    var traversal_times = std.ArrayList(i64).init(allocator);
    defer traversal_times.deinit();

    for (0..PERFORMANCE_SAMPLES) |_| {
        const start_id = (@mod(std.crypto.random.int(u32), block_count - 10)) + 1;

        const start = std.time.nanoTimestamp();
        const edges = try storage_engine.find_outgoing_edges(@intCast(start_id), allocator);
        const end = std.time.nanoTimestamp();

        defer allocator.free(edges);
        try traversal_times.append(end - start);
    }

    const avg_traversal = calculate_average(traversal_times.items);
    try testing.expect(avg_traversal < TARGET_QUERY_LATENCY_NS * 5); // 5x tolerance for traversal
}

test "memory_management_efficiency_benchmark" {
    const allocator = testing.allocator;

    // Phase 1: Arena allocation performance
    var arena_times = std.ArrayList(i64).init(allocator);
    defer arena_times.deinit();

    for (0..PERFORMANCE_SAMPLES) |_| {
        var arena = std.heap.ArenaAllocator.init(allocator);
        defer arena.deinit();
        const arena_allocator = arena.allocator();

        const start = std.time.nanoTimestamp();

        // Allocate many small blocks (simulating MemtableManager pattern)
        for (0..100) |_| {
            const mem = try arena_allocator.alloc(u8, 256);
            @memset(mem, 0xAB);
        }

        const end = std.time.nanoTimestamp();
        try arena_times.append(end - start);
    }

    const avg_arena_time = calculate_average(arena_times.items);

    // Phase 2: Arena cleanup performance
    var cleanup_times = std.ArrayList(i64).init(allocator);
    defer cleanup_times.deinit();

    for (0..PERFORMANCE_SAMPLES) |_| {
        var arena = std.heap.ArenaAllocator.init(allocator);
        const arena_allocator = arena.allocator();

        // Pre-allocate significant data
        for (0..1000) |_| {
            const mem = try arena_allocator.alloc(u8, 512);
            @memset(mem, 0xCD);
        }

        const start = std.time.nanoTimestamp();
        arena.deinit();
        const end = std.time.nanoTimestamp();

        try cleanup_times.append(end - start);
    }

    const avg_cleanup_time = calculate_average(cleanup_times.items);

    // Arena operations should be very fast
    try testing.expect(avg_arena_time < 100_000); // <100µs for 100 allocations
    try testing.expect(avg_cleanup_time < 10_000); // <10µs for cleanup

    // Phase 3: Memory pressure simulation
    var sim_vfs = try SimulationVFS.init(allocator);
    defer sim_vfs.deinit();

    var engine = try StorageEngine.init_default(allocator, sim_vfs.vfs(), "memory_pressure_test");
    defer engine.deinit();

    try engine.startup();

    const initial_memory = engine.total_memory_usage();

    // Allocate substantial data
    for (1..1001) |i| {
        const block = try create_test_block(@intCast(i), 1024, allocator);
        defer allocator.free(block.content);
        try engine.put_block(block);
    }

    const peak_memory = engine.total_memory_usage();
    const memory_growth = peak_memory - initial_memory;

    // Memory growth should be proportional to data size
    const expected_minimum = 1000 * 1024; // At least 1MB for 1000 1KB blocks
    try testing.expect(memory_growth >= expected_minimum);
    try testing.expect(memory_growth < expected_minimum * 3); // <3x overhead
}

test "performance_regression_detection" {
    const allocator = testing.allocator;

    var sim_vfs = try SimulationVFS.init(allocator);
    defer sim_vfs.deinit();

    var engine = try StorageEngine.init_default(allocator, sim_vfs.vfs(), "regression_test");
    defer engine.deinit();

    try engine.startup();

    // Baseline performance measurement
    const baseline_start = std.time.nanoTimestamp();

    for (1..101) |i| {
        const block = try create_test_block(@intCast(i), 512, allocator);
        defer allocator.free(block.content);
        try engine.put_block(block);
    }

    const baseline_time = std.time.nanoTimestamp() - baseline_start;
    const baseline_per_op = @divTrunc(baseline_time, 100);

    // Stress test with larger dataset
    const stress_start = std.time.nanoTimestamp();

    for (101..1001) |i| {
        const block = try create_test_block(@intCast(i), 512, allocator);
        defer allocator.free(block.content);
        try engine.put_block(block);
    }

    const stress_time = std.time.nanoTimestamp() - stress_start;
    const stress_per_op = @divTrunc(stress_time, 900);

    // Performance should not degrade significantly with scale
    try testing.expect(stress_per_op < baseline_per_op * 3); // <3x degradation

    // Read performance should remain consistent
    const read_start = std.time.nanoTimestamp();

    for (1..101) |i| {
        const block = try engine.find_block(@intCast(i));
        try testing.expect(block != null);
    }

    const read_time = std.time.nanoTimestamp() - read_start;
    const read_per_op = @divTrunc(read_time, 100);

    // Read performance should meet targets even with larger dataset
    try testing.expect(read_per_op < TARGET_BLOCK_READ_LATENCY_NS);
}

test "concurrent_safety_performance_validation" {
    const allocator = testing.allocator;

    var sim_vfs = try SimulationVFS.init(allocator);
    defer sim_vfs.deinit();

    var engine = try StorageEngine.init_default(allocator, sim_vfs.vfs(), "concurrent_safety_test");
    defer engine.deinit();

    try engine.startup();

    // Verify single-threaded design performance
    concurrency.assert_main_thread();

    const rapid_ops_start = std.time.nanoTimestamp();

    // Rapid sequential operations to test thread safety assertions overhead
    for (1..501) |i| {
        concurrency.assert_main_thread();

        const block = try create_test_block(@intCast(i), 256, allocator);
        defer allocator.free(block.content);
        try engine.put_block(block);

        concurrency.assert_main_thread();

        const retrieved = try engine.find_block(@intCast(i));
        try testing.expect(retrieved != null);

        concurrency.assert_main_thread();
    }

    const rapid_ops_time = std.time.nanoTimestamp() - rapid_ops_start;
    const rapid_ops_per_op = @divTrunc(rapid_ops_time, 500 * 2); // 2 ops per iteration

    // Thread safety assertions should not significantly impact performance
    try testing.expect(rapid_ops_per_op < TARGET_BLOCK_WRITE_LATENCY_NS * 2);
}

fn calculate_average(times: []const i64) i64 {
    if (times.len == 0) return 0;

    var sum: i64 = 0;
    for (times) |time| {
        sum += time;
    }
    return @divTrunc(sum, @as(i64, @intCast(times.len)));
}

fn calculate_percentile(times: []i64, percentile: f64) i64 {
    if (times.len == 0) return 0;

    std.sort.heap(i64, times, {}, std.sort.asc(i64));
    const index = @as(usize, @intFromFloat(@as(f64, @floatFromInt(times.len)) * percentile / 100.0));
    return times[@min(index, times.len - 1)];
}
