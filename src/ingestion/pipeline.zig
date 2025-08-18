//! KausalDB Ingestion Pipeline
//!
//! Provides a flexible framework for automatically populating the database
//! from various sources. The pipeline consists of three main components:
//!
//! 1. **Sources**: Fetch raw data from external systems (Git repos, files, APIs)
//! 2. **Parsers**: Extract semantic structure from raw data (functions, types, etc.)
//! 3. **Chunkers**: Break large content into meaningful ContextBlocks
//!
//! Design Principles:
//! - Uses VFS abstraction for simulation testing
//! - Per-file arena memory management for bounded usage
//! - Explicit error handling with context
//! - Single-threaded execution model

const std = @import("std");
const testing = std.testing;

const context_block = @import("../core/types.zig");
const vfs = @import("../core/vfs.zig");
const assert = @import("../core/assert.zig");
const error_context = @import("../core/error_context.zig");
const concurrency = @import("../core/concurrency.zig");
const simulation_vfs = @import("../sim/simulation_vfs.zig");
const storage_metrics = @import("../storage/metrics.zig");

const StorageMetrics = storage_metrics.StorageMetrics;
const ContextBlock = context_block.ContextBlock;
const BlockId = context_block.BlockId;
const GraphEdge = context_block.GraphEdge;
const EdgeType = context_block.EdgeType;
const VFS = vfs.VFS;
const SimulationVFS = simulation_vfs.SimulationVFS;

/// Errors that can occur during ingestion pipeline operations
pub const IngestionError = error{
    /// Source data could not be fetched
    SourceFetchFailed,
    /// Raw data could not be parsed
    ParseFailed,
    /// Content could not be chunked into blocks
    ChunkingFailed,
    /// Invalid source configuration
    InvalidSourceConfig,
    /// Unsupported content type
    UnsupportedContentType,
} || std.mem.Allocator.Error || std.fs.File.ReadError || std.fs.File.WriteError;

/// Iterator over source content items (one per file)
pub const SourceIterator = struct {
    ptr: *anyopaque,
    vtable: *const VTable,

    pub const VTable = struct {
        /// Get the next content item, or null if finished
        next: *const fn (ptr: *anyopaque, allocator: std.mem.Allocator) IngestionError!?SourceContent,
        /// Clean up iterator resources
        deinit: *const fn (ptr: *anyopaque, allocator: std.mem.Allocator) void,
    };

    /// Get the next content item from the iterator
    pub fn next(self: *SourceIterator, allocator: std.mem.Allocator) IngestionError!?SourceContent {
        return self.vtable.next(self.ptr, allocator);
    }

    /// Clean up iterator resources
    pub fn deinit(self: *SourceIterator, allocator: std.mem.Allocator) void {
        self.vtable.deinit(self.ptr, allocator);
    }
};

/// Metadata about fetched source content
pub const SourceContent = struct {
    /// Raw content bytes
    data: []const u8,
    /// Content type (e.g., "text/zig", "text/markdown", "application/json")
    content_type: []const u8,
    /// Source-specific metadata (file path, commit hash, etc.)
    metadata: std.StringHashMap([]const u8),
    /// When this content was fetched
    timestamp_ns: u64,

    pub fn deinit(self: *SourceContent, allocator: std.mem.Allocator) void {
        // Note: HashMap keys are string literals and should not be freed
        var iterator = self.metadata.iterator();
        while (iterator.next()) |entry| {
            allocator.free(entry.value_ptr.*);
        }
        self.metadata.deinit();
        allocator.free(self.data);
        allocator.free(self.content_type);
    }
};

/// Semantic unit extracted by a parser
pub const ParsedUnit = struct {
    /// Unique identifier for this unit within the source
    id: []const u8,
    /// Type of semantic unit (e.g., "function", "struct", "comment", "section")
    unit_type: []const u8,
    /// The actual content
    content: []const u8,
    /// Source location information
    location: SourceLocation,
    /// Relationships to other units
    edges: std.ArrayList(ParsedEdge),
    /// Unit-specific metadata
    metadata: std.StringHashMap([]const u8),

    pub fn deinit(self: *ParsedUnit, allocator: std.mem.Allocator) void {
        self.edges.deinit();

        // Note: HashMap keys are string literals and should not be freed
        var iterator = self.metadata.iterator();
        while (iterator.next()) |entry| {
            allocator.free(entry.value_ptr.*);
        }
        self.metadata.deinit();

        allocator.free(self.id);
        allocator.free(self.unit_type);
        allocator.free(self.content);
    }
};

/// Location information for parsed content
pub const SourceLocation = struct {
    /// Source file path
    file_path: []const u8,
    /// Starting line number (1-based)
    line_start: u32,
    /// Ending line number (1-based, inclusive)
    line_end: u32,
    /// Starting column (1-based)
    col_start: u32,
    /// Ending column (1-based, inclusive)
    col_end: u32,
};

/// Relationship between parsed units
pub const ParsedEdge = struct {
    /// Target unit ID
    target_id: []const u8,
    /// Type of relationship
    edge_type: EdgeType,
    /// Edge-specific metadata
    metadata: std.StringHashMap([]const u8),

    pub fn deinit(self: *ParsedEdge, allocator: std.mem.Allocator) void {
        self.metadata.deinit();
        allocator.free(self.target_id);
    }
};

/// Abstract interface for data sources
pub const Source = struct {
    ptr: *anyopaque,
    vtable: *const VTable,

    pub const VTable = struct {
        /// Fetch content iterator from the source
        fetch: *const fn (ptr: *anyopaque, allocator: std.mem.Allocator, file_system: *VFS) IngestionError!SourceIterator,
        /// Get human-readable description of this source
        describe: *const fn (ptr: *anyopaque) []const u8,
        /// Clean up source resources
        deinit: *const fn (ptr: *anyopaque, allocator: std.mem.Allocator) void,
    };

    /// Fetch content iterator from this source
    pub fn fetch(self: Source, allocator: std.mem.Allocator, file_system: *VFS) IngestionError!SourceIterator {
        return self.vtable.fetch(self.ptr, allocator, file_system);
    }

    /// Get description of this source
    pub fn describe(self: Source) []const u8 {
        return self.vtable.describe(self.ptr);
    }

    /// Clean up source resources
    pub fn deinit(self: Source, allocator: std.mem.Allocator) void {
        self.vtable.deinit(self.ptr, allocator);
    }
};

/// Abstract interface for content parsers
pub const Parser = struct {
    ptr: *anyopaque,
    vtable: *const VTable,

    pub const VTable = struct {
        /// Parse content into semantic units
        parse: *const fn (ptr: *anyopaque, allocator: std.mem.Allocator, content: SourceContent) IngestionError![]ParsedUnit,
        /// Check if this parser supports the given content type
        supports: *const fn (ptr: *anyopaque, content_type: []const u8) bool,
        /// Get human-readable description of this parser
        describe: *const fn (ptr: *anyopaque) []const u8,
        /// Clean up parser resources
        deinit: *const fn (ptr: *anyopaque, allocator: std.mem.Allocator) void,
    };

    /// Parse source content into semantic units
    pub fn parse(self: Parser, allocator: std.mem.Allocator, content: SourceContent) IngestionError![]ParsedUnit {
        return self.vtable.parse(self.ptr, allocator, content);
    }

    /// Check if this parser supports the content type
    pub fn supports(self: Parser, content_type: []const u8) bool {
        return self.vtable.supports(self.ptr, content_type);
    }

    /// Get description of this parser
    pub fn describe(self: Parser) []const u8 {
        return self.vtable.describe(self.ptr);
    }

    /// Clean up parser resources
    pub fn deinit(self: Parser, allocator: std.mem.Allocator) void {
        self.vtable.deinit(self.ptr, allocator);
    }
};

/// Abstract interface for content chunkers
pub const Chunker = struct {
    ptr: *anyopaque,
    vtable: *const VTable,

    pub const VTable = struct {
        /// Convert parsed units into ContextBlocks
        chunk: *const fn (ptr: *anyopaque, allocator: std.mem.Allocator, units: []const ParsedUnit) IngestionError![]ContextBlock,
        /// Get human-readable description of this chunker
        describe: *const fn (ptr: *anyopaque) []const u8,
        /// Clean up chunker resources
        deinit: *const fn (ptr: *anyopaque, allocator: std.mem.Allocator) void,
    };

    /// Convert parsed units into ContextBlocks
    pub fn chunk(self: Chunker, allocator: std.mem.Allocator, units: []const ParsedUnit) IngestionError![]ContextBlock {
        return self.vtable.chunk(self.ptr, allocator, units);
    }

    /// Get description of this chunker
    pub fn describe(self: Chunker) []const u8 {
        return self.vtable.describe(self.ptr);
    }

    /// Clean up chunker resources
    pub fn deinit(self: Chunker, allocator: std.mem.Allocator) void {
        self.vtable.deinit(self.ptr, allocator);
    }
};

/// Configuration for ingestion pipeline backpressure control.
/// Allows tuning batch sizes and memory pressure thresholds for different scenarios.
pub const BackpressureConfig = struct {
    /// Default batch size for normal memory pressure conditions
    default_batch_size: u32 = 32,
    /// Minimum batch size under high memory pressure (always >= 1)
    min_batch_size: u32 = 1,
    /// How often to check storage memory pressure (in milliseconds)
    pressure_check_interval_ms: u64 = 100,
    /// Memory pressure thresholds for storage engine
    pressure_config: StorageMetrics.MemoryPressureConfig = StorageMetrics.MemoryPressureConfig{},
};

/// Statistics for backpressure adaptations during ingestion.
/// Tracks how ingestion pipeline adapts to storage memory pressure.
pub const BackpressureStats = struct {
    /// Number of times batch size was reduced due to memory pressure
    pressure_adaptations: u64 = 0,
    /// Total number of pressure checks performed
    pressure_checks: u64 = 0,
    /// Maximum batch size used during ingestion
    max_batch_size_used: u32 = 0,
    /// Minimum batch size used during ingestion
    min_batch_size_used: u32 = 0,
    /// Time spent waiting for storage pressure to decrease (nanoseconds)
    total_backpressure_wait_ns: u64 = 0,
};

/// Configuration for the ingestion pipeline
pub const PipelineConfig = struct {
    /// Maximum number of blocks to process in a single batch
    max_batch_size: u32 = 1000,
    /// Whether to continue processing if individual items fail
    continue_on_error: bool = true,
    /// Custom metadata to attach to all generated blocks
    global_metadata: std.StringHashMap([]const u8),
    /// Backpressure configuration for storage memory pressure adaptation
    backpressure: BackpressureConfig = BackpressureConfig{},

    pub fn init(allocator: std.mem.Allocator) PipelineConfig {
        return PipelineConfig{
            .global_metadata = std.StringHashMap([]const u8).init(allocator),
        };
    }

    pub fn deinit(self: *PipelineConfig) void {
        self.global_metadata.deinit();
    }
};

/// Statistics from pipeline execution
pub const PipelineStats = struct {
    /// Number of sources processed
    sources_processed: u32 = 0,
    /// Number of sources that failed
    sources_failed: u32 = 0,
    /// Number of parsed units extracted
    units_parsed: u32 = 0,
    /// Number of context blocks generated
    blocks_generated: u32 = 0,
    /// Total processing time in nanoseconds
    processing_time_ns: u64 = 0,
    /// Backpressure adaptation statistics
    backpressure: BackpressureStats = BackpressureStats{},
    /// Memory peak usage in bytes
    peak_memory_bytes: u64 = 0,
};

/// Main ingestion pipeline that orchestrates sources, parsers, and chunkers
pub const IngestionPipeline = struct {
    /// Base allocator for component lists
    allocator: std.mem.Allocator,
    /// Arena for all pipeline memory allocations
    arena: std.heap.ArenaAllocator,
    /// Configuration
    config: PipelineConfig,
    /// Registered sources
    sources: std.ArrayList(Source),
    /// Registered parsers
    parsers: std.ArrayList(Parser),
    /// Registered chunkers
    chunkers: std.ArrayList(Chunker),
    /// Virtual file system for testing
    file_system: *VFS,
    /// Execution statistics
    current_stats: PipelineStats,
    /// Backpressure state for adaptive batch sizing
    last_pressure_check: i128,
    current_batch_size: u32,

    /// Initialize a new ingestion pipeline
    pub fn init(allocator: std.mem.Allocator, file_system: *VFS, config: PipelineConfig) !IngestionPipeline {
        const arena = std.heap.ArenaAllocator.init(allocator);

        return IngestionPipeline{
            .allocator = allocator,
            .arena = arena,
            .config = config,
            // Use the external allocator for component lists to avoid cross-allocator references
            .sources = std.ArrayList(Source).init(allocator),
            .parsers = std.ArrayList(Parser).init(allocator),
            .chunkers = std.ArrayList(Chunker).init(allocator),
            .file_system = file_system,
            .current_stats = PipelineStats{},
            .last_pressure_check = std.time.nanoTimestamp(),
            .current_batch_size = config.backpressure.default_batch_size,
        };
    }

    /// Clean up pipeline resources
    pub fn deinit(self: *IngestionPipeline) void {
        // Note: Components are cleaned up by their owners, not by the pipeline
        // The pipeline only holds interface references, not ownership
        self.sources.deinit();
        self.parsers.deinit();
        self.chunkers.deinit();
        self.config.deinit();
        self.arena.deinit();
    }

    /// Register a new source with the pipeline
    pub fn register_source(self: *IngestionPipeline, source: Source) !void {
        try self.sources.append(source);
    }

    /// Register a new parser with the pipeline
    pub fn register_parser(self: *IngestionPipeline, parser: Parser) !void {
        try self.parsers.append(parser);
    }

    /// Register a new chunker with the pipeline
    pub fn register_chunker(self: *IngestionPipeline, chunker: Chunker) !void {
        try self.chunkers.append(chunker);
    }

    /// Execute the full ingestion pipeline
    pub fn execute(self: *IngestionPipeline) IngestionError![]ContextBlock {
        concurrency.assert_main_thread();

        const start_time = std.time.nanoTimestamp();
        defer self.current_stats.processing_time_ns = @intCast(std.time.nanoTimestamp() - start_time);

        const allocator = self.arena.allocator();
        var all_blocks = std.ArrayList(ContextBlock).init(allocator);

        for (self.sources.items) |source| {
            self.process_source(source, &all_blocks) catch |err| {
                self.current_stats.sources_failed += 1;
                const ctx = error_context.IngestionContext{
                    .operation = "process_source",
                    .repository_path = source.describe(),
                    .content_type = "source",
                };
                error_context.log_ingestion_error(err, ctx);
                if (!self.config.continue_on_error) {
                    return err;
                }
                continue;
            };
            self.current_stats.sources_processed += 1;
        }

        self.current_stats.blocks_generated = @intCast(all_blocks.items.len);
        return all_blocks.toOwnedSlice();
    }

    /// Execute ingestion with backpressure control integrated with storage engine.
    /// Processes sources in adaptive batches based on storage memory pressure,
    /// directly storing blocks to avoid unbounded memory growth during large ingestions.
    pub fn execute_with_backpressure(self: *IngestionPipeline, storage_engine: anytype) IngestionError!void {
        concurrency.assert_main_thread();

        const start_time = std.time.nanoTimestamp();
        defer self.current_stats.processing_time_ns = @intCast(std.time.nanoTimestamp() - start_time);

        self.current_stats.backpressure.max_batch_size_used = self.current_batch_size;
        self.current_stats.backpressure.min_batch_size_used = self.current_batch_size;
        for (self.sources.items) |source| {
            self.process_source_with_backpressure(source, storage_engine) catch |err| {
                self.current_stats.sources_failed += 1;
                const ctx = error_context.IngestionContext{
                    .operation = "process_source_with_backpressure",
                    .repository_path = source.describe(),
                    .content_type = "source",
                };
                error_context.log_ingestion_error(err, ctx);
                if (!self.config.continue_on_error) {
                    return err;
                }
                continue;
            };
            self.current_stats.sources_processed += 1;
        }
    }

    /// Process a single source through the pipeline
    fn process_source(
        self: *IngestionPipeline,
        source: Source,
        blocks: *std.ArrayList(ContextBlock),
    ) IngestionError!void {
        const main_allocator = self.arena.allocator();

        var content_iterator = try source.fetch(main_allocator, self.file_system);
        defer content_iterator.deinit(main_allocator);

        while (try content_iterator.next(main_allocator)) |content| {
            // Memory optimization: Use temporary arena per file to bound memory usage.
            // Without this, the main arena would accumulate memory for ALL files in the
            // ingestion run, causing unbounded growth for large repositories.
            // With per-file arenas, memory usage is bounded by the largest individual file.
            // Final ContextBlocks are copied to main arena for persistence after temp cleanup.
            var temp_arena = std.heap.ArenaAllocator.init(self.allocator);
            defer temp_arena.deinit();
            const temp_allocator = temp_arena.allocator();

            var mutable_content = content;
            defer mutable_content.deinit(main_allocator);

            // Silently skip unsupported content types to avoid failing entire ingestion
            const parser = self.find_parser(mutable_content.content_type) orelse {
                continue;
            };

            // Transform raw text into structured elements for context building using temporary allocator
            const units = try parser.parse(temp_allocator, mutable_content);
            defer {
                for (units) |*unit| {
                    unit.deinit(temp_allocator);
                }
                temp_allocator.free(units);
            }
            self.current_stats.units_parsed += @intCast(units.len);

            for (self.chunkers.items) |chunker| {
                const temp_blocks = try chunker.chunk(temp_allocator, units);

                // Copy blocks from temporary arena to main arena for persistence
                for (temp_blocks) |temp_block| {
                    const persistent_block = try self.copy_block_to_main_arena(temp_block, main_allocator);
                    try blocks.append(persistent_block);
                }
                break;
            } else {
                // Skip if no compatible chunker found
                continue;
            }
        }
    }

    /// Copy a ContextBlock from temporary arena to main arena for persistence
    fn copy_block_to_main_arena(
        _: *IngestionPipeline,
        temp_block: ContextBlock,
        main_allocator: std.mem.Allocator,
    ) !ContextBlock {
        return ContextBlock{
            .id = temp_block.id, // BlockId is copy-by-value
            .version = temp_block.version,
            .source_uri = try main_allocator.dupe(u8, temp_block.source_uri),
            .metadata_json = try main_allocator.dupe(u8, temp_block.metadata_json),
            .content = try main_allocator.dupe(u8, temp_block.content),
        };
    }

    /// Find a parser that supports the given content type
    fn find_parser(self: *IngestionPipeline, content_type: []const u8) ?Parser {
        for (self.parsers.items) |parser| {
            if (parser.supports(content_type)) {
                return parser;
            }
        }
        return null;
    }

    /// Process a single source with backpressure control and direct storage integration.
    /// Uses adaptive batching to maintain bounded memory usage during large ingestions.
    fn process_source_with_backpressure(
        self: *IngestionPipeline,
        source: Source,
        storage_engine: anytype,
    ) IngestionError!void {
        var source_arena = std.heap.ArenaAllocator.init(self.allocator);
        defer source_arena.deinit(); // O(1) cleanup when source completes
        const temp_allocator = source_arena.allocator();

        var content_iterator = try source.fetch(temp_allocator, self.file_system);
        defer content_iterator.deinit(temp_allocator);

        // Use main allocator for batch buffer to survive arena resets
        var batch_buffer = std.ArrayList(ContextBlock).init(self.allocator);
        defer {
            for (batch_buffer.items) |*block| {
                self.allocator.free(block.source_uri);
                self.allocator.free(block.metadata_json);
                self.allocator.free(block.content);
            }
            batch_buffer.deinit();
        }

        while (try content_iterator.next(temp_allocator)) |content| {
            try self.process_content_to_batch_with_copying(content, temp_allocator, &batch_buffer);

            // Flush batch when full or under memory pressure
            if (batch_buffer.items.len >= self.current_batch_size) {
                if (self.should_check_storage_pressure()) {
                    try self.adapt_batch_size_to_pressure(storage_engine);
                }

                try self.flush_batch_to_storage(storage_engine, &batch_buffer);

                for (batch_buffer.items) |*block| {
                    self.allocator.free(block.source_uri);
                    self.allocator.free(block.metadata_json);
                    self.allocator.free(block.content);
                }
                batch_buffer.clearRetainingCapacity(); // Reuse allocation
                _ = source_arena.reset(.retain_capacity); // O(1) memory reclaim
            }
        }

        if (batch_buffer.items.len > 0) {
            // Check pressure for final batch to ensure stats are recorded
            if (self.should_check_storage_pressure()) {
                try self.adapt_batch_size_to_pressure(storage_engine);
            }
            try self.flush_batch_to_storage(storage_engine, &batch_buffer);
        }
    }

    /// Check if it's time to evaluate storage memory pressure.
    /// Avoids expensive pressure calculations on every block by using time-based sampling.
    fn should_check_storage_pressure(self: *IngestionPipeline) bool {
        // Special case: interval_ms = 0 means check on every batch for testing
        if (self.config.backpressure.pressure_check_interval_ms == 0) {
            return true;
        }

        const now = std.time.nanoTimestamp();
        const elapsed_ms = @divTrunc(now - self.last_pressure_check, std.time.ns_per_ms);
        return elapsed_ms >= self.config.backpressure.pressure_check_interval_ms;
    }

    /// Adapt current batch size based on storage engine memory pressure.
    /// Reduces batch size under pressure to provide backpressure control.
    fn adapt_batch_size_to_pressure(self: *IngestionPipeline, storage_engine: anytype) !void {
        self.last_pressure_check = std.time.nanoTimestamp();
        self.current_stats.backpressure.pressure_checks += 1;

        const pressure = storage_engine.memory_pressure(self.config.backpressure.pressure_config);
        const previous_batch_size = self.current_batch_size;

        // Adapt batch size based on memory pressure level
        self.current_batch_size = switch (pressure) {
            .low => self.config.backpressure.default_batch_size,
            .medium => @max(self.config.backpressure.min_batch_size, self.config.backpressure.default_batch_size / 2),
            .high => self.config.backpressure.min_batch_size,
        };

        // Track batch size statistics
        self.current_stats.backpressure.max_batch_size_used = @max(self.current_stats.backpressure.max_batch_size_used, self.current_batch_size);
        self.current_stats.backpressure.min_batch_size_used = @min(self.current_stats.backpressure.min_batch_size_used, self.current_batch_size);

        // Count adaptations when batch size is reduced due to pressure
        if (self.current_batch_size < previous_batch_size) {
            self.current_stats.backpressure.pressure_adaptations += 1;
        }
    }

    /// Process content through parsing and chunking pipeline into batch buffer.
    /// Uses temporary allocator for intermediate processing to maintain memory bounds.
    fn process_content_to_batch(
        self: *IngestionPipeline,
        content: SourceContent,
        temp_allocator: std.mem.Allocator,
        batch_buffer: *std.ArrayList(ContextBlock),
    ) !void {
        const parser = self.find_parser(content.content_type) orelse return;

        const parsed_units = try parser.parse(temp_allocator, content);
        defer {
            for (parsed_units) |*unit| {
                unit.deinit(temp_allocator);
            }
            temp_allocator.free(parsed_units);
        }
        self.current_stats.units_parsed += @intCast(parsed_units.len);

        for (self.chunkers.items) |chunker| {
            const chunked_blocks = try chunker.chunk(temp_allocator, parsed_units);
            defer temp_allocator.free(chunked_blocks);

            try batch_buffer.appendSlice(chunked_blocks);
        }
    }

    /// Process content through parsing and chunking pipeline with proper memory copying.
    /// Copies blocks from temporary allocator to main allocator to survive arena resets.
    fn process_content_to_batch_with_copying(
        self: *IngestionPipeline,
        content: SourceContent,
        temp_allocator: std.mem.Allocator,
        batch_buffer: *std.ArrayList(ContextBlock),
    ) !void {
        const parser = self.find_parser(content.content_type) orelse return;

        const parsed_units = try parser.parse(temp_allocator, content);
        defer {
            for (parsed_units) |*unit| {
                unit.deinit(temp_allocator);
            }
            temp_allocator.free(parsed_units);
        }
        self.current_stats.units_parsed += @intCast(parsed_units.len);

        for (self.chunkers.items) |chunker| {
            const chunked_blocks = try chunker.chunk(temp_allocator, parsed_units);
            defer temp_allocator.free(chunked_blocks);

            // Copy blocks from temp allocator to main allocator for persistence
            for (chunked_blocks) |temp_block| {
                const persistent_block = ContextBlock{
                    .id = temp_block.id, // BlockId is copy-by-value
                    .version = temp_block.version,
                    .source_uri = try self.allocator.dupe(u8, temp_block.source_uri),
                    .metadata_json = try self.allocator.dupe(u8, temp_block.metadata_json),
                    .content = try self.allocator.dupe(u8, temp_block.content),
                };
                try batch_buffer.append(persistent_block);
            }
        }
    }

    /// Flush batch of blocks directly to storage engine.
    /// Provides durability without requiring intermediate memory allocation.
    fn flush_batch_to_storage(
        self: *IngestionPipeline,
        storage_engine: anytype,
        batch_buffer: *std.ArrayList(ContextBlock),
    ) IngestionError!void {
        const flush_start = std.time.nanoTimestamp();
        defer {
            const flush_time = std.time.nanoTimestamp() - flush_start;
            // Track time waiting for storage (backpressure effect)
            self.current_stats.backpressure.total_backpressure_wait_ns += @intCast(flush_time);
        }

        for (batch_buffer.items) |block| {
            storage_engine.put_block(block) catch |err| switch (err) {
                error.OutOfMemory => return error.OutOfMemory,
                else => {
                    const ctx = error_context.IngestionContext{
                        .operation = "flush_batch_to_storage",
                        .content_type = "context_block",
                        .unit_count = batch_buffer.items.len,
                    };
                    error_context.log_ingestion_error(err, ctx);
                    return error.SourceFetchFailed; // Generic storage failure
                },
            };
            self.current_stats.blocks_generated += 1;
        }
    }

    /// Current pipeline statistics
    pub fn stats(self: *const IngestionPipeline) PipelineStats {
        return self.current_stats;
    }

    /// Reset pipeline statistics
    pub fn reset_stats(self: *IngestionPipeline) void {
        self.current_stats = PipelineStats{};
    }
};

test "pipeline creation and cleanup" {
    const allocator = testing.allocator;

    // Use simulation VFS for testing
    var sim_vfs = try SimulationVFS.init(allocator);
    defer sim_vfs.deinit();

    var config = PipelineConfig.init(allocator);
    defer config.deinit();

    var vfs_instance = sim_vfs.vfs();
    var pipeline = try IngestionPipeline.init(allocator, &vfs_instance, config);
    defer pipeline.deinit();

    try testing.expectEqual(@as(usize, 0), pipeline.sources.items.len);
    try testing.expectEqual(@as(usize, 0), pipeline.parsers.items.len);
    try testing.expectEqual(@as(usize, 0), pipeline.chunkers.items.len);
}

test "source content lifecycle" {
    const allocator = testing.allocator;

    var metadata = std.StringHashMap([]const u8).init(allocator);

    try metadata.put("file_path", try allocator.dupe(u8, "/test/example.zig"));

    var content = SourceContent{
        .data = try allocator.dupe(u8, "test content"),
        .content_type = try allocator.dupe(u8, "text/zig"),
        .metadata = metadata,
        .timestamp_ns = 1234567890,
    };
    defer content.deinit(allocator);

    try testing.expectEqualStrings("test content", content.data);
    try testing.expectEqualStrings("text/zig", content.content_type);
    try testing.expectEqual(@as(u64, 1234567890), content.timestamp_ns);
}
