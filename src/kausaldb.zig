//! KausalDB public API for application integration.
//!
//! Provides clean, minimal interface for knowledge graph database operations
//! including context block storage, graph traversal, and semantic queries.
//! Exports core types, storage engine, query engine, and server components.
//!
//! Design rationale: Single entry point prevents API sprawl and enables
//! controlled evolution of public interfaces. Separation from testing_api.zig
//! maintains clean boundaries between production and development usage.

const builtin = @import("builtin");
const std = @import("std");

pub const types = @import("core/types.zig");
pub const ContextBlock = types.ContextBlock;
pub const BlockId = types.BlockId;
pub const GraphEdge = types.GraphEdge;
pub const EdgeType = types.EdgeType;

pub const storage = @import("storage/engine.zig");
pub const Database = storage.StorageEngine;
pub const StorageEngine = storage.StorageEngine;
pub const Config = storage.Config;
pub const Error = storage.StorageError;

pub const query_engine = @import("query/engine.zig");
pub const QueryEngine = query_engine.QueryEngine;
pub const QueryResult = query_engine.QueryResult;
pub const TraversalDirection = query_engine.TraversalDirection;
pub const TraversalResult = query_engine.TraversalResult;
pub const TraversalQuery = query_engine.TraversalQuery;
pub const TraversalAlgorithm = query_engine.TraversalAlgorithm;
pub const EdgeTypeFilter = query_engine.EdgeTypeFilter;
pub const SemanticQuery = query_engine.SemanticQuery;
pub const SemanticQueryResult = query_engine.SemanticQueryResult;
pub const SemanticResult = query_engine.SemanticResult;
pub const FilteredQuery = query_engine.FilteredQuery;
pub const FilteredQueryResult = query_engine.FilteredQueryResult;
pub const FilterCondition = query_engine.FilterCondition;
pub const FilterExpression = query_engine.FilterExpression;
pub const FilterOperator = query_engine.FilterOperator;
pub const FilterTarget = query_engine.FilterTarget;

pub const pipeline = @import("ingestion/pipeline.zig");
pub const IngestionPipeline = pipeline.IngestionPipeline;
pub const PipelineConfig = pipeline.PipelineConfig;
pub const BackpressureConfig = pipeline.BackpressureConfig;
pub const BackpressureStats = pipeline.BackpressureStats;
pub const Source = pipeline.Source;
pub const Parser = pipeline.Parser;
pub const Chunker = pipeline.Chunker;
pub const SourceContent = pipeline.SourceContent;
pub const ParsedUnit = pipeline.ParsedUnit;

pub const git_source = @import("ingestion/git_source.zig");
pub const GitSource = git_source.GitSource;
pub const GitSourceConfig = git_source.GitSourceConfig;

pub const zig_parser = @import("ingestion/zig_parser.zig");
pub const ZigParser = zig_parser.ZigParser;
pub const ZigParserConfig = zig_parser.ZigParserConfig;

pub const semantic_chunker = @import("ingestion/semantic_chunker.zig");
pub const SemanticChunker = semantic_chunker.SemanticChunker;
pub const SemanticChunkerConfig = semantic_chunker.SemanticChunkerConfig;

pub const handler = @import("server/handler.zig");
pub const Server = handler.Server;

pub const Allocator = std.mem.Allocator;

pub const version = .{
    .major = 0,
    .minor = 1,
    .patch = 0,
};

/// Initialize a new database instance
///
/// Creates the storage engine with the provided configuration.
/// The database handles knowledge graph storage and querying.
/// Uses production VFS and default data directory.
pub fn init(allocator: Allocator, config: Config) !Database {
    const production_vfs = @import("core/production_vfs.zig");
    var vfs_instance = production_vfs.ProductionVFS.init(allocator);
    return Database.init(allocator, vfs_instance.vfs(), "data", config);
}

comptime {
    if (builtin.zig_version.major != 0 or builtin.zig_version.minor < 13) {
        @compileError("Requires Zig 0.13.0 or later");
    }
}

test "library exports" {
    const testing = std.testing;

    const block_id = try BlockId.from_hex("0123456789abcdeffedcba9876543210");
    try testing.expect(block_id.bytes.len == 16);

    const edge = EdgeType.imports;
    try testing.expectEqual(@as(u16, 1), edge.to_u16());
}
