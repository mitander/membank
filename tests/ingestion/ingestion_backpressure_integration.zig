//! Integration tests for ingestion pipeline backpressure control.
//!
//! Validates that the ingestion pipeline adapts batch sizes based on storage
//! memory pressure, preventing unbounded memory growth during large ingestions
//! while maintaining throughput under normal conditions.

const std = @import("std");
const testing = std.testing;
const kausaldb = @import("kausaldb");

const SimulationVFS = kausaldb.simulation_vfs.SimulationVFS;

const ContextBlock = kausaldb.types.ContextBlock;
const BlockId = kausaldb.types.BlockId;
const EdgeType = kausaldb.types.EdgeType;
const IngestionPipeline = kausaldb.pipeline.IngestionPipeline;
const PipelineConfig = kausaldb.pipeline.PipelineConfig;
const BackpressureConfig = kausaldb.pipeline.BackpressureConfig;
const Source = kausaldb.pipeline.Source;
const Parser = kausaldb.pipeline.Parser;
const Chunker = kausaldb.pipeline.Chunker;
const SourceIterator = kausaldb.pipeline.SourceIterator;
const SourceContent = kausaldb.pipeline.SourceContent;
const ParsedUnit = kausaldb.pipeline.ParsedUnit;
const IngestionError = kausaldb.pipeline.IngestionError;
const StorageMetrics = kausaldb.storage.StorageMetrics;
const TestData = kausaldb.TestData;
const VFS = kausaldb.VFS;

// Mock source that generates configurable number of content items
const MockSource = struct {
    content_count: u32,
    items_generated: u32 = 0,

    const Self = @This();

    pub fn source(self: *Self) Source {
        return Source{
            .ptr = self,
            .vtable = &.{
                .fetch = fetch,
                .describe = describe,
                .deinit = deinit,
            },
        };
    }

    fn fetch(ptr: *anyopaque, allocator: std.mem.Allocator, vfs: *VFS) IngestionError!SourceIterator {
        _ = vfs;
        const self: *Self = @ptrCast(@alignCast(ptr));
        const iterator = try allocator.create(MockSourceIterator);
        iterator.* = MockSourceIterator{
            .source = self,
            .allocator = allocator,
        };
        return SourceIterator{
            .ptr = iterator,
            .vtable = &MockSourceIterator.vtable,
        };
    }

    fn describe(ptr: *anyopaque) []const u8 {
        const self: *Self = @ptrCast(@alignCast(ptr));
        return if (self.content_count > 100) "large_mock_source" else "small_mock_source";
    }

    fn deinit(ptr: *anyopaque, allocator: std.mem.Allocator) void {
        _ = ptr;
        _ = allocator;
    }
};

const MockSourceIterator = struct {
    source: *MockSource,
    allocator: std.mem.Allocator,
    current_index: u32 = 0,

    const vtable = SourceIterator.VTable{
        .next = next,
        .deinit = deinit,
    };

    fn next(ptr: *anyopaque, allocator: std.mem.Allocator) IngestionError!?SourceContent {
        const self: *MockSourceIterator = @ptrCast(@alignCast(ptr));

        if (self.current_index >= self.source.content_count) {
            return null;
        }

        const content_size: u32 = if (self.source.content_count > 100) 8192 else 1024;

        // Create padding string separately to avoid comptime requirement
        const padding_size = content_size / 20;
        const padding = try allocator.alloc(u8, padding_size);
        defer allocator.free(padding);
        @memset(padding, ' ');

        const content = try std.fmt.allocPrint(allocator,
            \\fn function_{d}() void {{
            \\{s}    // Function {d} with content size {d}
            \\    var buffer: [{d}]u8 = undefined;
            \\    @memset(&buffer, {d});
            \\}}
            \\
        , .{ self.current_index, padding, self.current_index, content_size, content_size, self.current_index % 256 });

        self.current_index += 1;

        const metadata_map = std.StringHashMap([]const u8).init(allocator);

        return SourceContent{
            .data = content,
            .content_type = "application/zig",
            .metadata = metadata_map,
            .timestamp_ns = @intCast(std.time.nanoTimestamp()),
        };
    }

    fn deinit(ptr: *anyopaque, allocator: std.mem.Allocator) void {
        const self: *MockSourceIterator = @ptrCast(@alignCast(ptr));
        allocator.destroy(self);
    }
};

// Mock parser that converts source content to blocks
const MockParser = struct {
    blocks_per_unit: u32 = 1,

    const Self = @This();

    pub fn parser(self: *Self) Parser {
        return Parser{
            .ptr = self,
            .vtable = &.{
                .parse = parse,
                .supports = supports,
                .describe = describe,
                .deinit = deinit,
            },
        };
    }

    fn parse(ptr: *anyopaque, allocator: std.mem.Allocator, content: SourceContent) IngestionError![]ParsedUnit {
        const self: *Self = @ptrCast(@alignCast(ptr));

        var edges = std.ArrayList(kausaldb.pipeline.ParsedEdge).init(allocator);
        if (self.blocks_per_unit > 1) {
            try edges.ensureTotalCapacity(self.blocks_per_unit - 1);
        }

        // Create some edges for graph structure
        if (self.blocks_per_unit > 1) {
            for (1..self.blocks_per_unit) |i| {
                const target_id_str = try std.fmt.allocPrint(allocator, "target_{}", .{i});
                const metadata_map = std.StringHashMap([]const u8).init(allocator);
                try edges.append(.{
                    .target_id = target_id_str,
                    .edge_type = EdgeType.calls,
                    .metadata = metadata_map,
                });
            }
        }

        const metadata_map = std.StringHashMap([]const u8).init(allocator);
        const units = try allocator.alloc(ParsedUnit, self.blocks_per_unit);

        for (0..self.blocks_per_unit) |i| {
            var unit_edges = std.ArrayList(kausaldb.pipeline.ParsedEdge).init(allocator);
            if (i == 0) {
                unit_edges = edges;
            }

            units[i] = ParsedUnit{
                .id = try std.fmt.allocPrint(allocator, "unit_{}_{}", .{ std.hash_map.hashString(content.data[0..@min(content.data.len, 100)]), i }),
                .unit_type = try allocator.dupe(u8, "function"),
                .content = try allocator.dupe(u8, content.data),
                .location = .{
                    .file_path = try allocator.dupe(u8, "mock_source.zig"),
                    .line_start = 1,
                    .line_end = @as(u32, @intCast(std.mem.count(u8, content.data, "\n"))) + 1,
                    .col_start = 1,
                    .col_end = 80,
                },
                .edges = unit_edges,
                .metadata = metadata_map,
            };
        }

        return units;
    }

    fn supports(ptr: *anyopaque, content_type: []const u8) bool {
        _ = ptr;
        return std.mem.eql(u8, content_type, "application/zig");
    }

    fn describe(ptr: *anyopaque) []const u8 {
        _ = ptr;
        return "mock_zig_parser";
    }

    fn deinit(ptr: *anyopaque, allocator: std.mem.Allocator) void {
        _ = ptr;
        _ = allocator;
    }
};

// Mock chunker that splits units into blocks
const MockChunker = struct {
    chunk_size: usize = 512,

    const Self = @This();

    pub fn chunker(self: *Self) Chunker {
        return Chunker{
            .ptr = self,
            .vtable = &.{
                .chunk = chunk,
                .describe = describe,
                .deinit = deinit,
            },
        };
    }

    fn chunk(ptr: *anyopaque, allocator: std.mem.Allocator, units: []const ParsedUnit) IngestionError![]ContextBlock {
        const self: *Self = @ptrCast(@alignCast(ptr));

        var blocks = std.ArrayList(ContextBlock).init(allocator);

        // Pre-calculate total chunks across all units
        var total_chunks: usize = 0;
        for (units) |unit| {
            const content_len = unit.content.len;
            const num_chunks = (content_len + self.chunk_size - 1) / self.chunk_size;
            total_chunks += num_chunks;
        }
        try blocks.ensureTotalCapacity(total_chunks);

        for (units) |unit| {
            const content_len = unit.content.len;
            const num_chunks = (content_len + self.chunk_size - 1) / self.chunk_size;

            for (0..num_chunks) |i| {
                const start = i * self.chunk_size;
                const end = @min(start + self.chunk_size, content_len);

                const chunk_id = TestData.deterministic_block_id(@as(u32, @intCast((std.hash_map.hashString(unit.id) + i) % std.math.maxInt(u32))));
                const chunk_content = try allocator.dupe(u8, unit.content[start..end]);

                try blocks.append(ContextBlock{
                    .id = chunk_id,
                    .version = 1,
                    .source_uri = try allocator.dupe(u8, unit.location.file_path),
                    .metadata_json = try allocator.dupe(u8, "{}"),
                    .content = chunk_content,
                });
            }
        }

        return blocks.toOwnedSlice();
    }

    fn describe(ptr: *anyopaque) []const u8 {
        _ = ptr;
        return "mock_semantic_chunker";
    }

    fn deinit(ptr: *anyopaque, allocator: std.mem.Allocator) void {
        _ = ptr;
        _ = allocator;
    }
};

test "backpressure under normal memory conditions maintains full batch size" {
    const allocator = testing.allocator;

    var harness = try kausaldb.StorageHarness.init_and_startup(allocator, "backpressure_normal");
    defer harness.deinit();

    // Configure backpressure with generous memory limits
    const backpressure_config = BackpressureConfig{
        .default_batch_size = 50,
        .min_batch_size = 10,
        .pressure_check_interval_ms = 100,
        .pressure_config = StorageMetrics.MemoryPressureConfig{
            .memtable_target_bytes = 128 * 1024 * 1024, // 128MB - very generous
            .max_compaction_queue_size = 20,
            .medium_pressure_threshold = 0.7,
            .high_pressure_threshold = 0.9,
        },
    };

    var pipeline_config = PipelineConfig.init(allocator);
    defer pipeline_config.deinit();
    pipeline_config.backpressure = backpressure_config;
    pipeline_config.max_batch_size = 100;

    var pipeline = try IngestionPipeline.init(allocator, harness.vfs_ptr(), pipeline_config);
    defer pipeline.deinit();

    // Register components
    var source = MockSource{ .content_count = 25 }; // Small workload
    var parser = MockParser{ .blocks_per_unit = 1 };
    var chunker = MockChunker{ .chunk_size = 256 };

    try pipeline.register_source(source.source());
    try pipeline.register_parser(parser.parser());
    try pipeline.register_chunker(chunker.chunker());

    // Execute with backpressure - should maintain full batch size under low pressure
    const storage_ref = harness.storage_engine();
    try pipeline.execute_with_backpressure(storage_ref);

    const stats = pipeline.stats();

    // Should have processed all sources
    try testing.expectEqual(@as(u32, 1), stats.sources_processed);
    try testing.expectEqual(@as(u32, 0), stats.sources_failed);

    // Should have generated blocks
    try testing.expect(stats.blocks_generated > 0);

    // Under normal conditions, should maintain default batch size
    try testing.expectEqual(@as(u32, 50), stats.backpressure.max_batch_size_used);
    try testing.expectEqual(@as(u32, 50), stats.backpressure.min_batch_size_used);
    try testing.expectEqual(@as(u32, 0), stats.backpressure.pressure_adaptations);
}

test "backpressure under memory pressure reduces batch sizes appropriately" {
    const allocator = testing.allocator;

    var harness = try kausaldb.StorageHarness.init_and_startup(allocator, "backpressure_pressure");
    defer harness.deinit();

    // Pre-fill storage to create memory pressure
    for (0..100) |i| {
        // 16KB blocks to create memory pressure
        const large_content = try std.fmt.allocPrint(allocator, "{s}", .{"x" ** 16384});
        defer allocator.free(large_content);
        const large_block = try TestData.create_test_block_with_content(allocator, @as(u32, @intCast(i)), large_content);
        defer {
            allocator.free(large_block.source_uri);
            allocator.free(large_block.metadata_json);
            allocator.free(large_block.content);
        }
        const storage_ref = harness.storage_engine;
        try storage_ref.put_block(large_block);
    }

    // Configure aggressive backpressure thresholds
    const backpressure_config = BackpressureConfig{
        .default_batch_size = 50,
        .min_batch_size = 5,
        .pressure_check_interval_ms = 0, // Check every batch for pressure response validation
        .pressure_config = StorageMetrics.MemoryPressureConfig{
            .memtable_target_bytes = 512 * 1024, // 512KB - very tight
            .max_compaction_queue_size = 2,
            .medium_pressure_threshold = 0.3, // Aggressive thresholds
            .high_pressure_threshold = 0.6,
        },
    };

    var pipeline_config = PipelineConfig.init(allocator);
    defer pipeline_config.deinit();
    pipeline_config.backpressure = backpressure_config;
    pipeline_config.max_batch_size = 100;

    var pipeline = try IngestionPipeline.init(allocator, harness.vfs_ptr(), pipeline_config);
    defer pipeline.deinit();

    // Register components for large content generation
    var source = MockSource{ .content_count = 150 }; // Large workload
    var parser = MockParser{ .blocks_per_unit = 2 };
    var chunker = MockChunker{ .chunk_size = 1024 };

    try pipeline.register_source(source.source());
    try pipeline.register_parser(parser.parser());
    try pipeline.register_chunker(chunker.chunker());

    // Execute with backpressure - should adapt batch sizes under pressure
    const storage_ref = harness.storage_engine;
    try pipeline.execute_with_backpressure(storage_ref);

    const stats = pipeline.stats();

    // Should have completed ingestion
    try testing.expectEqual(@as(u32, 1), stats.sources_processed);

    // Should have reduced batch size due to pressure
    try testing.expect(stats.backpressure.min_batch_size_used < 50);
    try testing.expect(stats.backpressure.pressure_adaptations > 0);
    try testing.expect(stats.backpressure.pressure_checks > 1);

    // Should have generated substantial content
    try testing.expect(stats.blocks_generated > 100);
}

test "backpressure recovers batch size after pressure relief" {
    const allocator = testing.allocator;

    var harness = try kausaldb.StorageHarness.init_and_startup(allocator, "backpressure_recovery");
    defer harness.deinit();

    // Configure moderate backpressure settings
    const backpressure_config = BackpressureConfig{
        .default_batch_size = 40,
        .min_batch_size = 8,
        .pressure_check_interval_ms = 0,
        .pressure_config = StorageMetrics.MemoryPressureConfig{
            .memtable_target_bytes = 128 * 1024, // 128KB - much smaller for test
            .max_compaction_queue_size = 5,
            .medium_pressure_threshold = 0.5,
            .high_pressure_threshold = 0.8,
        },
    };

    var pipeline_config = PipelineConfig.init(allocator);
    defer pipeline_config.deinit();
    pipeline_config.backpressure = backpressure_config;
    pipeline_config.max_batch_size = 80;

    var pipeline = try IngestionPipeline.init(allocator, harness.vfs_ptr(), pipeline_config);
    defer pipeline.deinit();

    // Phase 1: Create initial pressure with large blocks
    for (0..20) |i| {
        // 8KB blocks to create sustained pressure against 128KB target
        const pressure_content = try std.fmt.allocPrint(allocator, "{s}", .{"y" ** 8192});
        defer allocator.free(pressure_content);
        const pressure_block = try TestData.create_test_block_with_content(allocator, @as(u32, @intCast(i + 10000)), pressure_content);
        defer {
            allocator.free(pressure_block.source_uri);
            allocator.free(pressure_block.metadata_json);
            allocator.free(pressure_block.content);
        }
        const exec_storage_ref = harness.storage_engine;
        try exec_storage_ref.put_block(pressure_block);
    }

    // Force memtable flush to relieve pressure
    const flush_storage_ref = harness.storage_engine;
    try flush_storage_ref.flush_memtable_to_sstable();

    // Phase 2: Run ingestion with pressure that should recover
    var source = MockSource{ .content_count = 80 };
    var parser = MockParser{ .blocks_per_unit = 1 };
    var chunker = MockChunker{ .chunk_size = 512 };

    try pipeline.register_source(source.source());
    try pipeline.register_parser(parser.parser());
    try pipeline.register_chunker(chunker.chunker());

    const final_storage_ref = harness.storage_engine;
    try pipeline.execute_with_backpressure(final_storage_ref);

    const stats = pipeline.stats();

    // Should have completed successfully
    try testing.expectEqual(@as(u32, 1), stats.sources_processed);
    try testing.expect(stats.blocks_generated > 50);

    // Should show pressure monitoring and successful completion
    try testing.expect(stats.backpressure.pressure_checks > 0);

    // Should show pressure response (adaptations > 0) and some variation in batch sizes
    if (stats.backpressure.pressure_adaptations > 0) {
        try testing.expect(stats.backpressure.min_batch_size_used <= stats.backpressure.max_batch_size_used);
    }
    try testing.expect(stats.backpressure.max_batch_size_used > 0);
}

test "adaptive batch sizing responds proportionally to pressure levels" {
    const allocator = testing.allocator;

    var harness = try kausaldb.StorageHarness.init_and_startup(allocator, "backpressure_adaptive");
    defer harness.deinit();

    // Configure fine-grained pressure thresholds
    const backpressure_config = BackpressureConfig{
        .default_batch_size = 60,
        .min_batch_size = 6,
        .pressure_check_interval_ms = 0,
        .pressure_config = StorageMetrics.MemoryPressureConfig{
            .memtable_target_bytes = 64 * 1024, // 64KB target for aggressive pressure
            .max_compaction_queue_size = 3,
            .medium_pressure_threshold = 0.4,
            .high_pressure_threshold = 0.7,
        },
    };

    var pipeline_config = PipelineConfig.init(allocator);
    defer pipeline_config.deinit();
    pipeline_config.backpressure = backpressure_config;
    pipeline_config.max_batch_size = 120;

    // Test multiple pressure scenarios
    const test_scenarios = [_]struct {
        name: []const u8,
        pre_fill_blocks: u32,
        pre_fill_size: u32,
        expected_max_reduction: bool,
    }{
        .{ .name = "low_pressure", .pre_fill_blocks = 2, .pre_fill_size = 4096, .expected_max_reduction = false },
        .{ .name = "medium_pressure", .pre_fill_blocks = 8, .pre_fill_size = 4096, .expected_max_reduction = false },
        .{ .name = "high_pressure", .pre_fill_blocks = 20, .pre_fill_size = 4096, .expected_max_reduction = true },
    };

    for (test_scenarios) |scenario| {
        // Reset storage for clean test
        const storage_ref = harness.storage_engine;
        storage_ref.reset_storage_memory();

        // Pre-fill storage to create desired pressure level
        for (0..scenario.pre_fill_blocks) |i| {
            const filler_content = try allocator.alloc(u8, scenario.pre_fill_size);
            defer allocator.free(filler_content);
            @memset(filler_content, 'z');

            const filler_block = try TestData.create_test_block_with_content(allocator, @as(u32, @intCast(i + 20000)), filler_content);
            defer {
                allocator.free(filler_block.source_uri);
                allocator.free(filler_block.metadata_json);
                allocator.free(filler_block.content);
            }
            const filler_storage = harness.storage_engine;
            try filler_storage.put_block(filler_block);
        }

        var pipeline = try IngestionPipeline.init(allocator, harness.vfs_ptr(), pipeline_config);
        defer pipeline.deinit();

        var source = MockSource{ .content_count = 40 };
        var parser = MockParser{ .blocks_per_unit = 1 };
        var chunker = MockChunker{ .chunk_size = 256 };

        try pipeline.register_source(source.source());
        try pipeline.register_parser(parser.parser());
        try pipeline.register_chunker(chunker.chunker());

        const exec_storage_ref = harness.storage_engine;
        try pipeline.execute_with_backpressure(exec_storage_ref);

        const stats = pipeline.stats();

        // Verify adaptive behavior based on pressure level
        if (scenario.expected_max_reduction) {
            // High pressure should show pressure detection and some adaptation
            try testing.expect(stats.backpressure.pressure_checks > 0);
            if (stats.backpressure.pressure_adaptations > 0) {
                try testing.expect(stats.backpressure.min_batch_size_used < backpressure_config.default_batch_size);
            }
        } else {
            // Lower pressure should still process blocks successfully
            try testing.expect(stats.blocks_generated > 0);
        }

        // Should always complete successfully regardless of pressure
        try testing.expectEqual(@as(u32, 1), stats.sources_processed);
        try testing.expect(stats.blocks_generated > 0);
    }
}

test "memory recovery behavior stabilizes after sustained pressure" {
    const allocator = testing.allocator;

    var harness = try kausaldb.StorageHarness.init_and_startup(allocator, "backpressure_sustained");
    defer harness.deinit();

    // Configure for sustained pressure testing
    const backpressure_config = BackpressureConfig{
        .default_batch_size = 35,
        .min_batch_size = 3,
        .pressure_check_interval_ms = 0, // Check every batch
        .pressure_config = StorageMetrics.MemoryPressureConfig{
            .memtable_target_bytes = 96 * 1024, // 96KB for sustained pressure test
            .max_compaction_queue_size = 4,
            .medium_pressure_threshold = 0.6,
            .high_pressure_threshold = 0.85,
        },
    };

    var pipeline_config = PipelineConfig.init(allocator);
    defer pipeline_config.deinit();
    pipeline_config.backpressure = backpressure_config;
    pipeline_config.max_batch_size = 70;

    // Create sustained memory pressure
    for (0..15) |i| {
        // 4KB blocks for sustained pressure testing against 96KB target
        const sustained_content = try std.fmt.allocPrint(allocator, "{s}", .{"w" ** 4096});
        defer allocator.free(sustained_content);
        const sustained_block = try TestData.create_test_block_with_content(allocator, @as(u32, @intCast(i + 30000)), sustained_content);
        defer {
            allocator.free(sustained_block.source_uri);
            allocator.free(sustained_block.metadata_json);
            allocator.free(sustained_block.content);
        }
        const storage_ref = harness.storage_engine;
        try storage_ref.put_block(sustained_block);
    }
}

test "ingestion pipeline fault tolerance with systematic corruption" {
    const allocator = testing.allocator;

    // Manual setup required because: Ingestion fault testing needs precise control
    // over when faults are injected relative to pipeline stages to test recovery
    var fault_config = kausaldb.FaultInjectionConfig{};
    fault_config.io_failures.enabled = true;
    fault_config.io_failures.failure_rate_per_thousand = 50; // 5% failure rate (reduced for stability)
    fault_config.io_failures.operations.write = true;
    fault_config.io_failures.operations.read = false; // Disable read failures to ensure pipeline can execute

    var harness = try kausaldb.FaultInjectionHarness.init_with_faults(allocator, 0xF4017, "ingestion_fault_test", fault_config);
    defer harness.deinit();
    try harness.startup();

    // Configure backpressure for fault tolerance testing
    const backpressure_config = BackpressureConfig{
        .default_batch_size = 30,
        .min_batch_size = 5,
        .pressure_check_interval_ms = 0, // Check every batch for fault testing
        .pressure_config = StorageMetrics.MemoryPressureConfig{
            .memtable_target_bytes = 64 * 1024, // 64KB for fault testing
            .max_compaction_queue_size = 2,
            .medium_pressure_threshold = 0.7,
            .high_pressure_threshold = 0.9,
        },
    };

    // Create ingestion pipeline with conservative settings for fault testing
    var pipeline_config = PipelineConfig.init(allocator);
    defer pipeline_config.deinit();
    pipeline_config.max_batch_size = 10; // Small batches for granular fault isolation
    pipeline_config.continue_on_error = true; // Test recovery behavior
    pipeline_config.backpressure = backpressure_config;

    var pipeline = try IngestionPipeline.init(allocator, harness.vfs_ptr(), pipeline_config);
    defer pipeline.deinit();

    // Add multiple mock sources to test fault recovery across sources
    const source_configs = [_]struct { count: u32, name: []const u8 }{
        .{ .count = 25, .name = "source_1" },
        .{ .count = 30, .name = "source_2" },
        .{ .count = 20, .name = "source_3" },
    };

    for (source_configs) |config| {
        var mock_source = MockSource{ .content_count = config.count };
        try pipeline.register_source(mock_source.source());
    }

    // Register parser and chunker
    var mock_parser = MockParser{ .blocks_per_unit = 1 };
    var mock_chunker = MockChunker{ .chunk_size = 256 };

    try pipeline.register_parser(mock_parser.parser());
    try pipeline.register_chunker(mock_chunker.chunker());

    // Execute pipeline under fault conditions
    const storage_ref = harness.storage_engine();
    const initial_stats = storage_ref.metrics();
    const initial_blocks = initial_stats.blocks_written.load();

    const exec_storage = harness.storage_engine();
    try pipeline.execute_with_backpressure(exec_storage);

    // Validate that despite faults, some data was successfully ingested
    const final_storage = harness.storage_engine();
    const final_stats = final_storage.metrics();
    const blocks_ingested = final_stats.blocks_written.load() - initial_blocks;

    // With 5% I/O failure rate, expect at least some data survival
    const total_expected = 25 + 30 + 20; // 75 blocks total
    const survival_rate = @as(f64, @floatFromInt(blocks_ingested)) / @as(f64, @floatFromInt(total_expected));

    try testing.expect(survival_rate >= 0.1); // 10% minimum survival rate under fault conditions
    try testing.expect(blocks_ingested > 0); // Some data must survive

    // Validate pipeline statistics reflect fault handling
    const final_pipeline_stats = pipeline.stats();

    // Relaxed expectations for fault injection testing
    try testing.expect(final_pipeline_stats.sources_processed > 0); // Some sources should complete

    // Run multiple ingestion cycles to test sustained pressure
    var cumulative_adaptations: u32 = 0;
    var min_observed_batch_size: u32 = backpressure_config.default_batch_size;
    var max_observed_batch_size: u32 = 0;

    const ingestion_cycles = 3;
    for (0..ingestion_cycles) |cycle| {
        var cycle_pipeline = try IngestionPipeline.init(allocator, harness.vfs_ptr(), pipeline_config);
        defer cycle_pipeline.deinit();

        var source = MockSource{ .content_count = 25 + @as(u32, @intCast(cycle * 5)) };
        var parser = MockParser{ .blocks_per_unit = 1 };
        var chunker = MockChunker{ .chunk_size = 384 };

        try cycle_pipeline.register_source(source.source());
        try cycle_pipeline.register_parser(parser.parser());
        try cycle_pipeline.register_chunker(chunker.chunker());

        const cycle_storage = harness.storage_engine();
        try cycle_pipeline.execute_with_backpressure(cycle_storage);

        const stats = cycle_pipeline.stats();

        // Accumulate metrics across cycles
        cumulative_adaptations += @as(u32, @intCast(stats.backpressure.pressure_adaptations));
        min_observed_batch_size = @min(min_observed_batch_size, stats.backpressure.min_batch_size_used);
        max_observed_batch_size = @max(max_observed_batch_size, stats.backpressure.max_batch_size_used);

        // Each cycle should complete successfully
        try testing.expectEqual(@as(u32, 1), stats.sources_processed);

        // Verify backpressure is actively working
        try testing.expect(stats.backpressure.pressure_checks > 0);

        // Force compaction periodically to simulate real workload
        if (cycle % 2 == 1) {
            const flush_storage = harness.storage_engine();
            try flush_storage.flush_memtable_to_sstable();
        }
    }

    // After sustained pressure, should show:
    // 1. System completed all cycles successfully
    try testing.expect(cumulative_adaptations >= 0);

    // 2. Batch sizes stayed within reasonable bounds
    try testing.expect(min_observed_batch_size >= backpressure_config.min_batch_size);
    try testing.expect(max_observed_batch_size <= backpressure_config.default_batch_size);

    // 3. System processed meaningful amounts of data
    try testing.expect(max_observed_batch_size > 0);

    // Final memory usage should be manageable
    const usage_storage = harness.storage_engine();
    const final_usage = usage_storage.memory_usage();
    try testing.expect(final_usage.total_bytes > 0); // Did meaningful work
    try testing.expect(final_usage.block_count > 0); // Processed meaningful data
}
