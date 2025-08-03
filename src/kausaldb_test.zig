//! Internal Testing API
//!
//! Drop-in replacement for the public API with additional internal components
//! for comprehensive testing, debugging, and development.
//!
//! Import as @import("kausaldb") in test files.

const std = @import("std");

// Re-export all public API components (drop-in replacement)
const kausaldb_public = @import("kausaldb.zig");

// Core knowledge graph data structures
pub const types = kausaldb_public.types;
pub const ContextBlock = kausaldb_public.ContextBlock;
pub const BlockId = kausaldb_public.BlockId;
pub const GraphEdge = kausaldb_public.GraphEdge;
pub const EdgeType = kausaldb_public.EdgeType;

// Database storage and configuration
pub const storage = kausaldb_public.storage;
pub const Database = kausaldb_public.Database;
pub const StorageEngine = kausaldb_public.StorageEngine;
pub const Config = kausaldb_public.Config;
pub const Error = kausaldb_public.Error;

// Query engine for knowledge graph traversal
pub const query_engine = kausaldb_public.query_engine;
pub const QueryEngine = kausaldb_public.QueryEngine;
pub const QueryResult = kausaldb_public.QueryResult;
pub const TraversalDirection = kausaldb_public.TraversalDirection;
pub const TraversalResult = kausaldb_public.TraversalResult;
pub const FindBlocksQuery = kausaldb_public.FindBlocksQuery;
pub const TraversalQuery = kausaldb_public.TraversalQuery;
pub const TraversalAlgorithm = kausaldb_public.TraversalAlgorithm;
pub const SemanticQuery = kausaldb_public.SemanticQuery;
pub const SemanticQueryResult = kausaldb_public.SemanticQueryResult;
pub const SemanticResult = kausaldb_public.SemanticResult;
pub const FilteredQuery = kausaldb_public.FilteredQuery;
pub const FilteredQueryResult = kausaldb_public.FilteredQueryResult;
pub const FilterCondition = kausaldb_public.FilterCondition;
pub const FilterExpression = kausaldb_public.FilterExpression;
pub const FilterOperator = kausaldb_public.FilterOperator;
pub const FilterTarget = kausaldb_public.FilterTarget;

// Content ingestion and parsing pipeline
pub const pipeline = kausaldb_public.pipeline;
pub const IngestionPipeline = kausaldb_public.IngestionPipeline;
pub const PipelineConfig = kausaldb_public.PipelineConfig;
pub const Source = kausaldb_public.Source;
pub const Parser = kausaldb_public.Parser;
pub const Chunker = kausaldb_public.Chunker;
pub const SourceContent = kausaldb_public.SourceContent;
pub const ParsedUnit = kausaldb_public.ParsedUnit;

pub const git_source = kausaldb_public.git_source;
pub const GitSource = kausaldb_public.GitSource;
pub const GitSourceConfig = kausaldb_public.GitSourceConfig;

pub const zig_parser = kausaldb_public.zig_parser;
pub const ZigParser = kausaldb_public.ZigParser;
pub const ZigParserConfig = kausaldb_public.ZigParserConfig;

pub const semantic_chunker = kausaldb_public.semantic_chunker;
pub const SemanticChunker = kausaldb_public.SemanticChunker;
pub const SemanticChunkerConfig = kausaldb_public.SemanticChunkerConfig;

// TCP server for database access
pub const handler = kausaldb_public.handler;
pub const Server = kausaldb_public.Server;

// Standard allocator type alias
pub const Allocator = kausaldb_public.Allocator;

// Version information
pub const version = kausaldb_public.version;

// Initialization function
pub const init = kausaldb_public.init;

// === INTERNAL TESTING COMPONENTS ===

// Simulation and deterministic testing framework
pub const simulation = @import("sim/simulation.zig");
pub const simulation_vfs = @import("sim/simulation_vfs.zig");
pub const Simulation = simulation.Simulation;
pub const SimulationVFS = simulation_vfs.SimulationVFS;

// VFS abstractions for testing different storage backends
pub const VFS = @import("core/vfs.zig").VFS;
pub const VFile = @import("core/vfs.zig").VFile;
pub const vfs = @import("core/vfs.zig");
pub const production_vfs = @import("core/production_vfs.zig");

// Core utilities for internal testing and assertions
pub const assert = @import("core/assert.zig");
pub const concurrency = @import("core/concurrency.zig");
pub const error_context = @import("core/error_context.zig");
pub const stdx = @import("core/stdx.zig");

// Storage engine internals for testing storage behavior
pub const bloom_filter = @import("storage/bloom_filter.zig");
pub const sstable = @import("storage/sstable.zig");
pub const tiered_compaction = @import("storage/tiered_compaction.zig");

// WAL subsystem components
pub const wal = struct {
    pub const corruption_tracker = @import("storage/wal/corruption_tracker.zig");
    pub const entry = @import("storage/wal/entry.zig");
    pub const recovery = @import("storage/wal/recovery.zig");
    pub const types = @import("storage/wal/types.zig");
};

// Query engine internals for testing query behavior and optimization
pub const query_operations = @import("query/operations.zig");
pub const query_traversal = @import("query/traversal.zig");
pub const query_filtering = @import("query/filtering.zig");

// Query module structure for backward compatibility
pub const query = struct {
    pub const engine = @import("query/engine.zig");
    pub const operations = @import("query/operations.zig");
    pub const traversal = @import("query/traversal.zig");
    pub const filtering = @import("query/filtering.zig");
    pub const cache = @import("query/cache.zig");
};

// Development and debugging tools
pub const debug_allocator = @import("dev/debug_allocator.zig");

// Main CLI module for testing CLI interface
pub const main = @import("main.zig");
