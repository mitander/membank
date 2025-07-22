//! CortexDB library root - exports public API for CortexDB components.
//!
//! This module provides access to the core CortexDB functionality including
//! storage engines, query processing, and context block management.

const std = @import("std");

// Core data structures
pub const context_block = @import("context_block.zig");
pub const ContextBlock = context_block.ContextBlock;
pub const BlockId = context_block.BlockId;
pub const GraphEdge = context_block.GraphEdge;
pub const EdgeType = context_block.EdgeType;

// Storage engine
pub const storage = @import("storage.zig");
pub const StorageEngine = storage.StorageEngine;

// Query engine
pub const query_engine = @import("query_engine.zig");
pub const QueryEngine = query_engine.QueryEngine;
pub const QueryResult = query_engine.QueryResult;
pub const GetBlocksQuery = query_engine.GetBlocksQuery;

// Ingestion Pipeline
pub const ingestion = @import("ingestion.zig");
pub const IngestionPipeline = ingestion.IngestionPipeline;
pub const PipelineConfig = ingestion.PipelineConfig;
pub const Source = ingestion.Source;
pub const Parser = ingestion.Parser;
pub const Chunker = ingestion.Chunker;
pub const SourceContent = ingestion.SourceContent;
pub const ParsedUnit = ingestion.ParsedUnit;

// Source Connectors
pub const git_source = @import("git_source.zig");
pub const GitSource = git_source.GitSource;
pub const GitSourceConfig = git_source.GitSourceConfig;

// Content Parsers
pub const zig_parser = @import("zig_parser.zig");
pub const ZigParser = zig_parser.ZigParser;
pub const ZigParserConfig = zig_parser.ZigParserConfig;

// Content Chunkers
pub const semantic_chunker = @import("semantic_chunker.zig");
pub const SemanticChunker = semantic_chunker.SemanticChunker;
pub const SemanticChunkerConfig = semantic_chunker.SemanticChunkerConfig;

// Virtual File System
pub const vfs = @import("vfs.zig");
pub const VFS = vfs.VFS;
pub const VFile = vfs.VFile;

// Simulation framework
pub const simulation = @import("simulation.zig");
pub const Simulation = simulation.Simulation;
pub const SimulationVFS = simulation.SimulationVFS;

// Utilities
pub const assert = @import("assert.zig");

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
