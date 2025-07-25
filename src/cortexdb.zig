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

// Core Data Structures
pub const ContextBlock = @import("core/types.zig").ContextBlock;
pub const BlockId = @import("core/types.zig").BlockId;
pub const GraphEdge = @import("core/types.zig").GraphEdge;
pub const EdgeType = @import("core/types.zig").EdgeType;

// Core Engines
pub const StorageEngine = @import("storage/storage.zig").StorageEngine;
pub const QueryEngine = @import("query/engine.zig").QueryEngine;
pub const QueryResult = @import("query/engine.zig").QueryResult;
pub const FindBlocksQuery = @import("query/engine.zig").FindBlocksQuery;

// Ingestion Framework (Public Interfaces)
pub const IngestionPipeline = @import("ingestion/pipeline.zig").IngestionPipeline;
pub const PipelineConfig = @import("ingestion/pipeline.zig").PipelineConfig;
pub const Source = @import("ingestion/pipeline.zig").Source;
pub const Parser = @import("ingestion/pipeline.zig").Parser;
pub const Chunker = @import("ingestion/pipeline.zig").Chunker;
pub const SourceContent = @import("ingestion/pipeline.zig").SourceContent;
pub const ParsedUnit = @import("ingestion/pipeline.zig").ParsedUnit;

// Source Connectors
pub const GitSource = @import("ingestion/git_source.zig").GitSource;
pub const GitSourceConfig = @import("ingestion/git_source.zig").GitSourceConfig;

// Content Parsers
pub const ZigParser = @import("ingestion/zig_parser.zig").ZigParser;
pub const ZigParserConfig = @import("ingestion/zig_parser.zig").ZigParserConfig;

// Content Chunkers
pub const SemanticChunker = @import("ingestion/semantic_chunker.zig").SemanticChunker;
pub const SemanticChunkerConfig = @import("ingestion/semantic_chunker.zig").SemanticChunkerConfig;

// Server Components
pub const Server = @import("server/handler.zig").Server;

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
pub const storage = @import("storage/storage.zig");

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
pub const server = @import("server/handler.zig");

// Core type aliases for backward compatibility
pub const types = @import("core/types.zig");
pub const vfs = @import("core/vfs.zig");

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
