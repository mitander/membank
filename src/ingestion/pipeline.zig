//! Membank Ingestion Pipeline
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
const context_block = @import("../core/types.zig");
const vfs = @import("../core/vfs.zig");
const assert = @import("../core/assert.zig");
const concurrency = @import("../core/concurrency.zig");
const simulation_vfs = @import("../sim/simulation_vfs.zig");
const testing = std.testing;

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

    const VTable = struct {
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

        // Free all values in metadata HashMap
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

    const VTable = struct {
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

    const VTable = struct {
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

    const VTable = struct {
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

/// Configuration for the ingestion pipeline
pub const PipelineConfig = struct {
    /// Maximum number of blocks to process in a single batch
    max_batch_size: u32 = 1000,
    /// Whether to continue processing if individual items fail
    continue_on_error: bool = true,
    /// Custom metadata to attach to all generated blocks
    global_metadata: std.StringHashMap([]const u8),

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

        // Process each source
        for (self.sources.items) |source| {
            self.process_source(source, &all_blocks) catch |err| {
                self.current_stats.sources_failed += 1;
                if (!self.config.continue_on_error) {
                    return err;
                }
                // Log error and continue with next source
                continue;
            };
            self.current_stats.sources_processed += 1;
        }

        self.current_stats.blocks_generated = @intCast(all_blocks.items.len);
        return all_blocks.toOwnedSlice();
    }

    /// Process a single source through the pipeline
    fn process_source(self: *IngestionPipeline, source: Source, blocks: *std.ArrayList(ContextBlock)) IngestionError!void {
        const main_allocator = self.arena.allocator();

        // Fetch content iterator from source
        var content_iterator = try source.fetch(main_allocator, self.file_system);
        defer content_iterator.deinit(main_allocator);

        // Process each content item from the source
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

            // Find compatible parser
            const parser = self.find_parser(mutable_content.content_type) orelse {
                // Skip unsupported content types instead of failing
                continue;
            };

            // Parse content into semantic units using temporary allocator
            const units = try parser.parse(temp_allocator, mutable_content);
            defer {
                for (units) |*unit| {
                    unit.deinit(temp_allocator);
                }
                temp_allocator.free(units);
            }
            self.current_stats.units_parsed += @intCast(units.len);

            // Find compatible chunker and convert to blocks
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
    fn copy_block_to_main_arena(_: *IngestionPipeline, temp_block: ContextBlock, main_allocator: std.mem.Allocator) !ContextBlock {
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

    var pipeline = try IngestionPipeline.init(allocator, &sim_vfs.vfs, config);
    defer pipeline.deinit();

    // Verify initial state
    try testing.expectEqual(@as(usize, 0), pipeline.sources.items.len);
    try testing.expectEqual(@as(usize, 0), pipeline.parsers.items.len);
    try testing.expectEqual(@as(usize, 0), pipeline.chunkers.items.len);
}

test "source content lifecycle" {
    const allocator = testing.allocator;

    var metadata = std.StringHashMap([]const u8).init(allocator);
    defer metadata.deinit();

    try metadata.put("file_path", "/test/example.zig");

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
