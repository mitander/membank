//! Semantic Chunker
//!
//! Implements the Chunker interface for converting parsed semantic units
//! into ContextBlocks. This chunker preserves the semantic structure
//! extracted by parsers and creates meaningful context blocks with
//! proper metadata and relationships.
//!
//! Design Notes:
//! - Converts ParsedUnit structures to ContextBlock structures
//! - Preserves all semantic metadata as JSON
//! - Creates deterministic block IDs based on content and location
//! - Arena-based memory management for lifecycle safety
//! - Single-threaded execution model

const std = @import("std");
const ingestion = @import("pipeline.zig");
const context_block = @import("../core/types.zig");
const assert = @import("../core/assert.zig");
const concurrency = @import("../core/concurrency.zig");
const error_context = @import("../core/error_context.zig");

const IngestionError = ingestion.IngestionError;
const ParsedUnit = ingestion.ParsedUnit;
const ParsedEdge = ingestion.ParsedEdge;
const SourceLocation = ingestion.SourceLocation;
const Chunker = ingestion.Chunker;
const ContextBlock = context_block.ContextBlock;
const BlockId = context_block.BlockId;
const GraphEdge = context_block.GraphEdge;
const EdgeType = context_block.EdgeType;

/// Configuration for semantic chunker
pub const SemanticChunkerConfig = struct {
    /// Maximum size of a single chunk in bytes
    max_chunk_size: usize = 32 * 1024, // 32KB default
    /// Whether to include source location in metadata
    include_source_location: bool = true,
    /// Whether to preserve original unit metadata
    preserve_unit_metadata: bool = true,
    /// Prefix for generated block IDs
    id_prefix: []const u8 = "semantic",
    /// Version to assign to all generated blocks
    block_version: u64 = 1,
};

/// Semantic chunker implementation
pub const SemanticChunker = struct {
    /// Chunker configuration
    config: SemanticChunkerConfig,
    /// Arena for all allocations
    arena: std.heap.ArenaAllocator,

    /// Initialize semantic chunker with configuration
    pub fn init(allocator: std.mem.Allocator, config: SemanticChunkerConfig) SemanticChunker {
        return SemanticChunker{
            .config = config,
            .arena = std.heap.ArenaAllocator.init(allocator),
        };
    }

    /// Clean up chunker resources
    pub fn deinit(self: *SemanticChunker, allocator: std.mem.Allocator) void {
        _ = allocator; // unused in this implementation
        self.arena.deinit();
    }

    /// Create Chunker interface wrapper
    pub fn chunker(self: *SemanticChunker) Chunker {
        return Chunker{
            .ptr = self,
            .vtable = &.{
                .chunk = chunk_impl,
                .describe = describe_impl,
                .deinit = deinit_impl,
            },
        };
    }

    /// Convert parsed units into ContextBlocks
    fn chunk_content(
        self: *SemanticChunker,
        allocator: std.mem.Allocator,
        units: []const ParsedUnit,
    ) IngestionError![]ContextBlock {
        concurrency.assert_main_thread();

        var blocks = std.ArrayList(ContextBlock).init(allocator);

        for (units) |unit| {
            if (unit.content.len > self.config.max_chunk_size) {
                continue;
            }

            const block = self.convert_unit_to_block(allocator, unit) catch |err| {
                error_context.log_ingestion_error(err, error_context.chunking_context(
                    "convert_unit_to_block",
                    unit.unit_type,
                    units.len,
                ));
                return err;
            };
            blocks.append(block) catch |err| {
                error_context.log_ingestion_error(err, error_context.chunking_context(
                    "append_converted_block",
                    unit.unit_type,
                    blocks.items.len,
                ));
                return err;
            };
        }

        return blocks.toOwnedSlice();
    }

    /// Convert a single ParsedUnit to a ContextBlock
    fn convert_unit_to_block(self: *SemanticChunker, allocator: std.mem.Allocator, unit: ParsedUnit) !ContextBlock {
        // Content-based hashing ensures identical units produce same block IDs
        const block_id = self.generate_block_id(allocator, unit) catch |err| {
            error_context.log_ingestion_error(err, error_context.chunking_context(
                "generate_block_id",
                unit.unit_type,
                1,
            ));
            return err;
        };

        const source_uri = self.create_source_uri(allocator, unit.location) catch |err| {
            error_context.log_ingestion_error(err, error_context.chunking_context(
                "create_source_uri",
                unit.unit_type,
                1,
            ));
            return err;
        };

        const metadata_json = self.serialize_metadata(allocator, unit) catch |err| {
            error_context.log_ingestion_error(err, error_context.chunking_context(
                "serialize_metadata",
                unit.unit_type,
                1,
            ));
            return err;
        };

        const content = allocator.dupe(u8, unit.content) catch |err| {
            error_context.log_ingestion_error(err, error_context.chunking_context(
                "duplicate_content",
                unit.unit_type,
                unit.content.len,
            ));
            return err;
        };

        return ContextBlock{
            .id = block_id,
            .version = self.config.block_version,
            .source_uri = source_uri,
            .metadata_json = metadata_json,
            .content = content,
        };
    }

    /// Generate a deterministic block ID for a parsed unit
    fn generate_block_id(self: *SemanticChunker, allocator: std.mem.Allocator, unit: ParsedUnit) !BlockId {
        const id_string = try std.fmt.allocPrint(
            allocator,
            "{s}_{s}_{s}_{d}_{d}",
            .{
                self.config.id_prefix,
                unit.unit_type,
                unit.id,
                unit.location.line_start,
                unit.location.col_start,
            },
        );
        defer allocator.free(id_string);

        // SHA-1 provides 160-bit collision resistance for block deduplication
        var hasher = std.crypto.hash.Blake3.init(.{});
        hasher.update(id_string);
        var hash: [32]u8 = undefined;
        hasher.final(&hash);

        var block_id: BlockId = undefined;
        @memcpy(block_id.bytes[0..16], hash[0..16]);

        return block_id;
    }

    /// Create source URI from location information
    fn create_source_uri(self: *SemanticChunker, allocator: std.mem.Allocator, location: SourceLocation) ![]const u8 {
        _ = self;
        return std.fmt.allocPrint(
            allocator,
            "file://{s}#L{d}-{d}",
            .{ location.file_path, location.line_start, location.line_end },
        );
    }

    /// Serialize unit metadata to JSON using simple string building
    fn serialize_metadata(self: *SemanticChunker, allocator: std.mem.Allocator, unit: ParsedUnit) ![]const u8 {
        var json = std.ArrayList(u8).init(allocator);
        defer json.deinit();

        try json.appendSlice("{");

        try json.writer().print("\"unit_type\":\"{s}\",\"unit_id\":\"{s}\"", .{ unit.unit_type, unit.id });

        if (self.config.include_source_location) {
            try json.writer().print(",\"location\":{{\"file_path\":\"{s}\",\"line_start\":{d},\"line_end\":{d},\"col_start\":{d},\"col_end\":{d}}}", .{
                unit.location.file_path,
                unit.location.line_start,
                unit.location.line_end,
                unit.location.col_start,
                unit.location.col_end,
            });
        }

        if (self.config.preserve_unit_metadata and unit.metadata.count() > 0) {
            try json.appendSlice(",\"original_metadata\":{");
            var first = true;
            var iter = unit.metadata.iterator();
            while (iter.next()) |entry| {
                if (!first) try json.appendSlice(",");
                try json.writer().print("\"{s}\":\"{s}\"", .{ entry.key_ptr.*, entry.value_ptr.* });
                first = false;
            }
            try json.appendSlice("}");
        }

        if (unit.edges.items.len > 0) {
            try json.appendSlice(",\"edges\":[");
            for (unit.edges.items, 0..) |edge, i| {
                if (i > 0) try json.appendSlice(",");
                try json.writer().print("{{\"target_id\":\"{s}\",\"edge_type\":\"{s}\"", .{ edge.target_id, @tagName(edge.edge_type) });

                if (edge.metadata.count() > 0) {
                    try json.appendSlice(",\"metadata\":{");
                    var first = true;
                    var edge_iter = edge.metadata.iterator();
                    while (edge_iter.next()) |entry| {
                        if (!first) try json.appendSlice(",");
                        try json.writer().print("\"{s}\":\"{s}\"", .{ entry.key_ptr.*, entry.value_ptr.* });
                        first = false;
                    }
                    try json.appendSlice("}");
                }
                try json.appendSlice("}");
            }
            try json.appendSlice("]");
        }

        try json.appendSlice("}");
        return json.toOwnedSlice();
    }

    fn chunk_impl(
        ptr: *anyopaque,
        allocator: std.mem.Allocator,
        units: []const ParsedUnit,
    ) IngestionError![]ContextBlock {
        const self: *SemanticChunker = @ptrCast(@alignCast(ptr));
        return self.chunk_content(allocator, units);
    }

    fn describe_impl(ptr: *anyopaque) []const u8 {
        _ = ptr;
        return "Semantic Unit Chunker";
    }

    fn deinit_impl(ptr: *anyopaque, allocator: std.mem.Allocator) void {
        const self: *SemanticChunker = @ptrCast(@alignCast(ptr));
        self.deinit(allocator);
    }
};

test "semantic chunker creation and cleanup" {
    const testing = std.testing;
    const allocator = testing.allocator;

    const config = SemanticChunkerConfig{};
    var semantic_chunker = SemanticChunker.init(allocator, config);
    defer semantic_chunker.deinit(allocator);

    const chunker_interface = semantic_chunker.chunker();
    try testing.expectEqualStrings("Semantic Unit Chunker", chunker_interface.describe());
}

test "convert unit to block" {
    const testing = std.testing;
    const allocator = testing.allocator;

    const config = SemanticChunkerConfig{};
    var semantic_chunker = SemanticChunker.init(allocator, config);
    defer semantic_chunker.deinit(allocator);

    var metadata = std.StringHashMap([]const u8).init(allocator);
    defer metadata.deinit();
    try metadata.put("function_name", "test_func");
    try metadata.put("is_public", "true");

    const unit_id = try allocator.dupe(u8, "test_function");
    defer allocator.free(unit_id);
    const unit_type = try allocator.dupe(u8, "function");
    defer allocator.free(unit_type);
    const unit_content = try allocator.dupe(u8, "pub fn test_func() void {}");
    defer allocator.free(unit_content);

    var unit_edges = std.ArrayList(ParsedEdge).init(allocator);
    defer unit_edges.deinit();

    const unit = ParsedUnit{
        .id = unit_id,
        .unit_type = unit_type,
        .content = unit_content,
        .location = SourceLocation{
            .file_path = "test.zig",
            .line_start = 10,
            .line_end = 12,
            .col_start = 1,
            .col_end = 25,
        },
        .edges = unit_edges,
        .metadata = metadata,
    };

    const block = try semantic_chunker.convert_unit_to_block(allocator, unit);
    defer block.deinit(allocator);

    try testing.expectEqualStrings("pub fn test_func() void {}", block.content);
    try testing.expectEqualStrings("file://test.zig#L10-12", block.source_uri);
    try testing.expectEqual(@as(u64, 1), block.version);

    try testing.expect(std.mem.indexOf(u8, block.metadata_json, "unit_type") != null);
    try testing.expect(std.mem.indexOf(u8, block.metadata_json, "function") != null);
    try testing.expect(std.mem.indexOf(u8, block.metadata_json, "location") != null);
}

test "generate deterministic block ID" {
    const testing = std.testing;
    const allocator = testing.allocator;

    const config = SemanticChunkerConfig{};
    var semantic_chunker = SemanticChunker.init(allocator, config);
    defer semantic_chunker.deinit(allocator);

    var unit_metadata = std.StringHashMap([]const u8).init(allocator);
    defer unit_metadata.deinit();

    var unit_edges = std.ArrayList(ParsedEdge).init(allocator);
    defer unit_edges.deinit();

    const unit_id = try allocator.dupe(u8, "test_function");
    defer allocator.free(unit_id);
    const unit_type = try allocator.dupe(u8, "function");
    defer allocator.free(unit_type);
    const unit_content = try allocator.dupe(u8, "content");
    defer allocator.free(unit_content);

    const unit = ParsedUnit{
        .id = unit_id,
        .unit_type = unit_type,
        .content = unit_content,
        .location = SourceLocation{
            .file_path = "test.zig",
            .line_start = 10,
            .line_end = 12,
            .col_start = 1,
            .col_end = 25,
        },
        .edges = unit_edges,
        .metadata = unit_metadata,
    };

    const id1 = try semantic_chunker.generate_block_id(allocator, unit);
    const id2 = try semantic_chunker.generate_block_id(allocator, unit);

    try testing.expect(std.mem.eql(u8, &id1.bytes, &id2.bytes));
}

test "chunk multiple units" {
    const testing = std.testing;
    const allocator = testing.allocator;

    const config = SemanticChunkerConfig{};
    var semantic_chunker = SemanticChunker.init(allocator, config);
    defer semantic_chunker.deinit(allocator);

    var units = std.ArrayList(ParsedUnit).init(allocator);
    defer units.deinit();

    var metadata1 = std.StringHashMap([]const u8).init(allocator);
    defer metadata1.deinit();
    try metadata1.put("function_name", "func1");

    const unit1_id = try allocator.dupe(u8, "func1");
    defer allocator.free(unit1_id);
    const unit1_type = try allocator.dupe(u8, "function");
    defer allocator.free(unit1_type);
    const unit1_content = try allocator.dupe(u8, "fn func1() void {}");
    defer allocator.free(unit1_content);
    var unit1_edges = std.ArrayList(ParsedEdge).init(allocator);
    defer unit1_edges.deinit();

    const unit1 = ParsedUnit{
        .id = unit1_id,
        .unit_type = unit1_type,
        .content = unit1_content,
        .location = SourceLocation{
            .file_path = "test.zig",
            .line_start = 1,
            .line_end = 1,
            .col_start = 1,
            .col_end = 18,
        },
        .edges = unit1_edges,
        .metadata = metadata1,
    };
    try units.append(unit1);

    var metadata2 = std.StringHashMap([]const u8).init(allocator);
    defer metadata2.deinit();
    try metadata2.put("constant_name", "VERSION");

    const unit2_id = try allocator.dupe(u8, "VERSION");
    defer allocator.free(unit2_id);
    const unit2_type = try allocator.dupe(u8, "constant");
    defer allocator.free(unit2_type);
    const unit2_content = try allocator.dupe(u8, "const VERSION = \"1.0.0\";");
    defer allocator.free(unit2_content);
    var unit2_edges = std.ArrayList(ParsedEdge).init(allocator);
    defer unit2_edges.deinit();

    const unit2 = ParsedUnit{
        .id = unit2_id,
        .unit_type = unit2_type,
        .content = unit2_content,
        .location = SourceLocation{
            .file_path = "test.zig",
            .line_start = 3,
            .line_end = 3,
            .col_start = 1,
            .col_end = 24,
        },
        .edges = unit2_edges,
        .metadata = metadata2,
    };
    try units.append(unit2);

    const chunker_interface = semantic_chunker.chunker();
    const blocks = try chunker_interface.chunk(allocator, units.items);
    defer {
        for (blocks) |block| {
            block.deinit(allocator);
        }
        allocator.free(blocks);
    }

    try testing.expectEqual(@as(usize, 2), blocks.len);
    try testing.expectEqualStrings("fn func1() void {}", blocks[0].content);
    try testing.expectEqualStrings("const VERSION = \"1.0.0\";", blocks[1].content);
}
