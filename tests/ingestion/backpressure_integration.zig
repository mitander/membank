//! Integration tests for ingestion pipeline backpressure control.
//!
//! Validates that the ingestion pipeline adapts batch sizes based on storage
//! memory pressure, preventing unbounded memory growth during large ingestions
//! while maintaining throughput under normal conditions.

const std = @import("std");
const testing = std.testing;
const kausaldb = @import("kausaldb");

const storage = kausaldb.storage;
const pipeline_mod = kausaldb.pipeline;
const simulation_vfs = kausaldb.simulation_vfs;
const types = kausaldb.types;

const StorageEngine = storage.StorageEngine;
const IngestionPipeline = pipeline_mod.IngestionPipeline;
const PipelineConfig = pipeline_mod.PipelineConfig;
const BackpressureConfig = pipeline_mod.BackpressureConfig;
const BackpressureStats = pipeline_mod.BackpressureStats;
const SimulationVFS = simulation_vfs.SimulationVFS;
const ContextBlock = types.ContextBlock;
const BlockId = types.BlockId;

// Test ingestion pipeline backpressure under normal memory conditions.
// Validates that ingestion proceeds with default batch sizes when storage pressure is low.
test "ingestion backpressure under normal memory conditions" {
    const allocator = testing.allocator;

    var sim_vfs = try SimulationVFS.init(allocator);
    defer sim_vfs.deinit();

    // Create storage engine with generous memory limits (low pressure)
    var storage_engine = try StorageEngine.init_default(allocator, sim_vfs.vfs(), "backpressure_normal_test");
    defer storage_engine.deinit();
    try storage_engine.startup();

    // Configure pipeline with default backpressure settings
    var config = PipelineConfig.init(allocator);
    defer config.deinit();
    config.backpressure.default_batch_size = 16;
    config.backpressure.min_batch_size = 4;

    var vfs_instance = sim_vfs.vfs();
    var ingestion_pipeline = try IngestionPipeline.init(allocator, &vfs_instance, config);
    defer ingestion_pipeline.deinit();

    // Register a simple test source that produces blocks
    const test_source = try create_test_source(allocator, 50); // Moderate dataset
    defer test_source.deinit(allocator);
    try ingestion_pipeline.register_source(test_source);

    // Register test parser and chunker
    const test_parser = create_test_parser();
    try ingestion_pipeline.register_parser(test_parser);
    const test_chunker = create_test_chunker();
    try ingestion_pipeline.register_chunker(test_chunker);

    // Execute ingestion with backpressure
    try ingestion_pipeline.execute_with_backpressure(&storage_engine);

    // Verify successful ingestion
    const stats = ingestion_pipeline.stats();
    try testing.expect(stats.sources_processed == 1);
    try testing.expect(stats.sources_failed == 0);
    try testing.expect(stats.blocks_generated > 0);

    // Under normal conditions, should use default batch size
    try testing.expectEqual(@as(u32, 16), stats.backpressure.max_batch_size_used);
    try testing.expect(stats.backpressure.pressure_adaptations == 0); // No adaptations needed
}

// Test ingestion pipeline backpressure under high memory pressure.
// Validates that batch sizes are reduced when storage engine reports high memory usage.
test "ingestion backpressure under high memory pressure" {
    const allocator = testing.allocator;

    var sim_vfs = try SimulationVFS.init(allocator);
    defer sim_vfs.deinit();

    // Create storage engine with restrictive memory limits (high pressure)
    var storage_engine = try StorageEngine.init_default(allocator, sim_vfs.vfs(), "backpressure_pressure_test");
    defer storage_engine.deinit();
    try storage_engine.startup();

    // Configure pipeline with aggressive backpressure settings
    var config = PipelineConfig.init(allocator);
    defer config.deinit();
    config.backpressure.default_batch_size = 16;
    config.backpressure.min_batch_size = 1;
    config.backpressure.pressure_check_interval_ms = 1; // Check very frequently
    // Set low memory pressure thresholds to trigger backpressure
    config.backpressure.pressure_config.memtable_target_bytes = 1024; // 1KB (very small)
    config.backpressure.pressure_config.medium_pressure_threshold = 0.1; // 10%
    config.backpressure.pressure_config.high_pressure_threshold = 0.2; // 20%

    var vfs_instance = sim_vfs.vfs();
    var ingestion_pipeline = try IngestionPipeline.init(allocator, &vfs_instance, config);
    defer ingestion_pipeline.deinit();

    // Register test components
    const test_source = try create_test_source(allocator, 100); // Large enough to trigger pressure
    defer test_source.deinit(allocator);
    try ingestion_pipeline.register_source(test_source);

    const test_parser = create_test_parser();
    try ingestion_pipeline.register_parser(test_parser);
    const test_chunker = create_test_chunker();
    try ingestion_pipeline.register_chunker(test_chunker);

    // Execute ingestion with backpressure
    try ingestion_pipeline.execute_with_backpressure(&storage_engine);

    // Verify backpressure was applied
    const stats = ingestion_pipeline.stats();
    try testing.expect(stats.sources_processed == 1);
    try testing.expect(stats.blocks_generated > 0);

    // Should have adapted batch size due to memory pressure
    try testing.expect(stats.backpressure.pressure_checks > 0);
    try testing.expect(stats.backpressure.pressure_adaptations > 0);
    try testing.expect(stats.backpressure.min_batch_size_used < 16); // Reduced from default
    try testing.expect(stats.backpressure.total_backpressure_wait_ns > 0);
}

// Test backpressure statistics tracking accuracy.
// Ensures all backpressure metrics are properly tracked during ingestion.
test "backpressure statistics tracking" {
    const allocator = testing.allocator;

    var sim_vfs = try SimulationVFS.init(allocator);
    defer sim_vfs.deinit();

    var storage_engine = try StorageEngine.init_default(allocator, sim_vfs.vfs(), "backpressure_stats_test");
    defer storage_engine.deinit();
    try storage_engine.startup();

    var config = PipelineConfig.init(allocator);
    defer config.deinit();
    config.backpressure.pressure_check_interval_ms = 50; // Check every 50ms

    var vfs_instance = sim_vfs.vfs();
    var ingestion_pipeline = try IngestionPipeline.init(allocator, &vfs_instance, config);
    defer ingestion_pipeline.deinit();

    // Register test components
    const test_source = try create_test_source(allocator, 20);
    defer test_source.deinit(allocator);
    try ingestion_pipeline.register_source(test_source);

    const test_parser = create_test_parser();
    try ingestion_pipeline.register_parser(test_parser);
    const test_chunker = create_test_chunker();
    try ingestion_pipeline.register_chunker(test_chunker);

    // Reset stats to ensure clean baseline
    ingestion_pipeline.reset_stats();

    // Execute ingestion
    try ingestion_pipeline.execute_with_backpressure(&storage_engine);

    // Verify statistics are properly tracked
    const stats = ingestion_pipeline.stats();

    // Basic ingestion stats should be tracked
    try testing.expect(stats.sources_processed > 0);
    try testing.expect(stats.blocks_generated > 0);
    try testing.expect(stats.processing_time_ns > 0);

    // Backpressure stats should be initialized
    try testing.expect(stats.backpressure.max_batch_size_used > 0);
    try testing.expect(stats.backpressure.min_batch_size_used > 0);
    try testing.expectEqual(stats.backpressure.max_batch_size_used, stats.backpressure.min_batch_size_used); // Consistent under low pressure
}

// Helper functions for creating test components

// Create a test source that generates a specified number of synthetic content items.
fn create_test_source(allocator: std.mem.Allocator, item_count: u32) !pipeline_mod.Source {
    const TestSource = struct {
        allocator: std.mem.Allocator,
        item_count: u32,

        const Self = @This();

        fn fetch(
            ptr: *anyopaque,
            alloc: std.mem.Allocator,
            vfs: *kausaldb.vfs.VFS,
        ) pipeline_mod.IngestionError!pipeline_mod.SourceIterator {
            _ = vfs;
            const self: *Self = @ptrCast(@alignCast(ptr));
            return TestSourceIterator.init(alloc, self.item_count);
        }

        fn describe(ptr: *anyopaque) []const u8 {
            _ = ptr;
            return "Test Source";
        }

        fn deinit_fn(ptr: *anyopaque, alloc: std.mem.Allocator) void {
            const self: *Self = @ptrCast(@alignCast(ptr));
            alloc.destroy(self);
        }
    };

    const test_source = try allocator.create(TestSource);
    test_source.* = TestSource{
        .allocator = allocator,
        .item_count = item_count,
    };

    return pipeline_mod.Source{
        .ptr = test_source,
        .vtable = &pipeline_mod.Source.VTable{
            .fetch = TestSource.fetch,
            .describe = TestSource.describe,
            .deinit = TestSource.deinit_fn,
        },
    };
}

// Test source iterator that generates synthetic content
const TestSourceIterator = struct {
    allocator: std.mem.Allocator,
    total_items: u32,
    current_item: u32,

    fn init(allocator: std.mem.Allocator, item_count: u32) pipeline_mod.SourceIterator {
        const iterator = allocator.create(TestSourceIterator) catch unreachable;
        iterator.* = TestSourceIterator{
            .allocator = allocator,
            .total_items = item_count,
            .current_item = 0,
        };

        return pipeline_mod.SourceIterator{
            .ptr = iterator,
            .vtable = &pipeline_mod.SourceIterator.VTable{
                .next = next,
                .deinit = deinit_fn,
            },
        };
    }

    fn next(ptr: *anyopaque, allocator: std.mem.Allocator) pipeline_mod.IngestionError!?pipeline_mod.SourceContent {
        const self: *TestSourceIterator = @ptrCast(@alignCast(ptr));

        if (self.current_item >= self.total_items) {
            return null;
        }

        const content = try std.fmt.allocPrint(allocator, "Test content item {}", .{self.current_item});
        self.current_item += 1;

        // Add small delay to ensure pressure checks can be triggered
        const start = std.time.nanoTimestamp();
        while (std.time.nanoTimestamp() - start < 2 * std.time.ns_per_ms) {
            // Busy wait for 2ms
        }

        // Create metadata HashMap with string literal keys and allocated values
        var metadata = std.StringHashMap([]const u8).init(allocator);
        const file_path_value = try allocator.dupe(u8, "test.txt");
        try metadata.put("file_path", file_path_value);

        return pipeline_mod.SourceContent{
            .data = content,
            .content_type = try allocator.dupe(u8, "text/plain"),
            .metadata = metadata,
            .timestamp_ns = @intCast(std.time.nanoTimestamp()),
        };
    }

    fn deinit_fn(ptr: *anyopaque, allocator: std.mem.Allocator) void {
        const self: *TestSourceIterator = @ptrCast(@alignCast(ptr));
        allocator.destroy(self);
    }
};

// Create a simple test parser that converts content to parsed units
fn create_test_parser() pipeline_mod.Parser {
    const TestParser = struct {
        fn parse(
            ptr: *anyopaque,
            allocator: std.mem.Allocator,
            content: pipeline_mod.SourceContent,
        ) pipeline_mod.IngestionError![]pipeline_mod.ParsedUnit {
            _ = ptr;

            var units = try allocator.alloc(pipeline_mod.ParsedUnit, 1);
            const file_path = content.metadata.get("file_path") orelse "unknown";
            units[0] = pipeline_mod.ParsedUnit{
                .id = try std.fmt.allocPrint(allocator, "unit_{s}", .{file_path}),
                .unit_type = try allocator.dupe(u8, "text"),
                .content = try allocator.dupe(u8, content.data),
                .location = pipeline_mod.SourceLocation{
                    .file_path = try allocator.dupe(u8, file_path),
                    .line_start = 1,
                    .line_end = 1,
                    .col_start = 1,
                    .col_end = @intCast(content.data.len),
                },
                .metadata = std.StringHashMap([]const u8).init(allocator),
                .edges = std.ArrayList(pipeline_mod.ParsedEdge).init(allocator),
            };

            return units;
        }

        fn supports(ptr: *anyopaque, content_type: []const u8) bool {
            _ = ptr;
            return std.mem.eql(u8, content_type, "text/plain");
        }

        fn describe(ptr: *anyopaque) []const u8 {
            _ = ptr;
            return "Test Parser";
        }

        fn deinit_fn(ptr: *anyopaque, allocator: std.mem.Allocator) void {
            _ = ptr;
            _ = allocator;
        }
    };

    return pipeline_mod.Parser{
        .ptr = undefined,
        .vtable = &pipeline_mod.Parser.VTable{
            .parse = TestParser.parse,
            .supports = TestParser.supports,
            .describe = TestParser.describe,
            .deinit = TestParser.deinit_fn,
        },
    };
}

// Create a simple test chunker that converts parsed units to context blocks
fn create_test_chunker() pipeline_mod.Chunker {
    const TestChunker = struct {
        fn chunk(
            ptr: *anyopaque,
            allocator: std.mem.Allocator,
            units: []const pipeline_mod.ParsedUnit,
        ) pipeline_mod.IngestionError![]types.ContextBlock {
            _ = ptr;

            var blocks = try allocator.alloc(types.ContextBlock, units.len);
            for (units, 0..) |unit, i| {
                var id_bytes: [16]u8 = std.mem.zeroes([16]u8);
                std.mem.writeInt(u64, id_bytes[0..8], @intCast(i), .little);
                std.mem.writeInt(u64, id_bytes[8..16], @intCast(std.time.nanoTimestamp()), .little);

                blocks[i] = types.ContextBlock{
                    .id = types.BlockId.from_bytes(id_bytes),
                    .version = 1,
                    .source_uri = try std.fmt.allocPrint(allocator, "test://{}_{s}", .{ i, unit.id }),
                    .metadata_json = try allocator.dupe(u8, "{}"),
                    .content = try allocator.dupe(u8, unit.content),
                };
            }

            return blocks;
        }

        fn describe(ptr: *anyopaque) []const u8 {
            _ = ptr;
            return "Test Chunker";
        }

        fn deinit_fn(ptr: *anyopaque, allocator: std.mem.Allocator) void {
            _ = ptr;
            _ = allocator;
        }
    };

    return pipeline_mod.Chunker{
        .ptr = undefined,
        .vtable = &pipeline_mod.Chunker.VTable{
            .chunk = TestChunker.chunk,
            .describe = TestChunker.describe,
            .deinit = TestChunker.deinit_fn,
        },
    };
}
