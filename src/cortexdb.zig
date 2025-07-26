//! CortexDB Core Library
//!
//! This module exports the public API for the CortexDB engine.
//! All internal implementation details are encapsulated and not exposed here.
//!
//! CortexDB is a high-performance, mission-critical database built on principles of:
//! - Simplicity is the Prerequisite for Reliability
//! - Correctness is Not Negotiable
//! - Explicitness Over Magic
//! - Determinism & Testability

const std = @import("std");
const builtin = @import("builtin");

// Simulation Framework (Public for Testing)
pub const simulation = @import("sim/simulation.zig");
pub const simulation_vfs = @import("sim/simulation_vfs.zig");

// Convenient re-exports
pub const Simulation = simulation.Simulation;
pub const SimulationVFS = simulation_vfs.SimulationVFS;

// Virtual File System
pub const VFS = @import("core/vfs.zig").VFS;
pub const VFile = @import("core/vfs.zig").VFile;

// Core Utilities
pub const assert = @import("core/assert.zig");
pub const concurrency = @import("core/concurrency.zig");
pub const error_context = @import("core/error_context.zig");
pub const stdx = @import("core/stdx.zig");
pub const production_vfs = @import("core/production_vfs.zig");

// Storage Layer Components
pub const bloom_filter = @import("storage/bloom_filter.zig");
pub const sstable = @import("storage/sstable.zig");
pub const tiered_compaction = @import("storage/tiered_compaction.zig");
pub const storage = @import("storage/engine.zig");

// Query Layer
pub const query_engine = @import("query/engine.zig");
pub const query_operations = @import("query/operations.zig");
pub const query_traversal = @import("query/traversal.zig");
pub const query_filtering = @import("query/filtering.zig");

// Ingestion Components
pub const pipeline = @import("ingestion/pipeline.zig");
pub const git_source = @import("ingestion/git_source.zig");
pub const zig_parser = @import("ingestion/zig_parser.zig");
pub const semantic_chunker = @import("ingestion/semantic_chunker.zig");

// Development Tools
pub const debug_allocator = @import("dev/debug_allocator.zig");

// Server Components
pub const handler = @import("server/handler.zig");

// Core type aliases for backward compatibility
pub const types = @import("core/types.zig");
pub const vfs = @import("core/vfs.zig");

// Core Data Structures
pub const ContextBlock = types.ContextBlock;
pub const BlockId = types.BlockId;
pub const GraphEdge = types.GraphEdge;
pub const EdgeType = types.EdgeType;

// Core Engines
pub const StorageEngine = storage.StorageEngine;
pub const QueryEngine = query_engine.QueryEngine;
pub const QueryResult = query_engine.QueryResult;
pub const FindBlocksQuery = query_engine.FindBlocksQuery;

// Query Engine (Public Interfaces)
pub const TraversalQuery = query_engine.TraversalQuery;
pub const TraversalResult = query_engine.TraversalResult;
pub const TraversalDirection = query_engine.TraversalDirection;
pub const TraversalAlgorithm = query_engine.TraversalAlgorithm;
pub const SemanticQuery = query_engine.SemanticQuery;
pub const SemanticQueryResult = query_engine.SemanticQueryResult;
pub const SemanticResult = query_engine.SemanticResult;
pub const FilteredQuery = query_engine.FilteredQuery;
pub const FilteredQueryResult = query_engine.FilteredQueryResult;
pub const FilterCondition = query_engine.FilterCondition;
pub const FilterExpression = query_engine.FilterExpression;
pub const FilterOperator = query_engine.FilterOperator;
pub const FilterTarget = query_engine.FilterTarget;

// Ingestion Framework (Public Interfaces)
pub const IngestionPipeline = pipeline.IngestionPipeline;
pub const PipelineConfig = pipeline.PipelineConfig;
pub const Source = pipeline.Source;
pub const Parser = pipeline.Parser;
pub const Chunker = pipeline.Chunker;
pub const SourceContent = pipeline.SourceContent;
pub const ParsedUnit = pipeline.ParsedUnit;

// Source Connectors
pub const GitSource = git_source.GitSource;
pub const GitSourceConfig = git_source.GitSourceConfig;

// Content Parsers
pub const ZigParser = zig_parser.ZigParser;
pub const ZigParserConfig = zig_parser.ZigParserConfig;

// Content Chunkers
pub const SemanticChunker = semantic_chunker.SemanticChunker;
pub const SemanticChunkerConfig = semantic_chunker.SemanticChunkerConfig;

// Server Components
pub const Server = handler.Server;

// Convenient type aliases
pub const Allocator = std.mem.Allocator;
pub const Database = StorageEngine;

// Version information
pub const version = .{
    .major = 0,
    .minor = 1,
    .patch = 0,
};

// Configuration types
pub const Config = StorageEngine.Config;
pub const Error = StorageEngine.StorageError;

/// Initialize CortexDB with the given configuration
pub fn init(allocator: Allocator, config: Config) !Database {
    return Database.init(allocator, config);
}

comptime {
    // Ensure we're using a supported Zig version
    if (builtin.zig_version.major != 0 or builtin.zig_version.minor < 13) {
        @compileError("CortexDB requires Zig 0.13.0 or later");
    }
}

test "library exports" {
    // Verify all core types are accessible
    const testing = std.testing;

    // Test that we can create basic types
    const block_id = try BlockId.from_hex("0123456789abcdeffedcba9876543210");
    try testing.expect(block_id.bytes.len == 16);

    // Test edge type
    const edge = EdgeType.imports;
    try testing.expectEqual(@as(u16, 1), edge.to_u16());
}
