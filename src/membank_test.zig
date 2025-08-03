//! Membank Internal Testing API
//!
//! Drop-in replacement for the public API with additional internal components
//! for comprehensive testing, debugging, and development.
//!
//! Import as @import("membank") in test files.

const std = @import("std");

// Re-export all public API components (drop-in replacement)
const membank_public = @import("membank.zig");

// Core knowledge graph data structures
pub const types = membank_public.types;
pub const ContextBlock = membank_public.ContextBlock;
pub const BlockId = membank_public.BlockId;
pub const GraphEdge = membank_public.GraphEdge;
pub const EdgeType = membank_public.EdgeType;

// Database storage and configuration
pub const storage = membank_public.storage;
pub const Database = membank_public.Database;
pub const StorageEngine = membank_public.StorageEngine;
pub const Config = membank_public.Config;
pub const Error = membank_public.Error;

// Query engine for knowledge graph traversal
pub const query_engine = membank_public.query_engine;
pub const QueryEngine = membank_public.QueryEngine;
pub const QueryResult = membank_public.QueryResult;
pub const TraversalDirection = membank_public.TraversalDirection;
pub const TraversalResult = membank_public.TraversalResult;
pub const FindBlocksQuery = membank_public.FindBlocksQuery;
pub const TraversalQuery = membank_public.TraversalQuery;
pub const TraversalAlgorithm = membank_public.TraversalAlgorithm;
pub const SemanticQuery = membank_public.SemanticQuery;
pub const SemanticQueryResult = membank_public.SemanticQueryResult;
pub const SemanticResult = membank_public.SemanticResult;
pub const FilteredQuery = membank_public.FilteredQuery;
pub const FilteredQueryResult = membank_public.FilteredQueryResult;
pub const FilterCondition = membank_public.FilterCondition;
pub const FilterExpression = membank_public.FilterExpression;
pub const FilterOperator = membank_public.FilterOperator;
pub const FilterTarget = membank_public.FilterTarget;

// Content ingestion and parsing pipeline
pub const pipeline = membank_public.pipeline;
pub const IngestionPipeline = membank_public.IngestionPipeline;
pub const PipelineConfig = membank_public.PipelineConfig;
pub const Source = membank_public.Source;
pub const Parser = membank_public.Parser;
pub const Chunker = membank_public.Chunker;
pub const SourceContent = membank_public.SourceContent;
pub const ParsedUnit = membank_public.ParsedUnit;

pub const git_source = membank_public.git_source;
pub const GitSource = membank_public.GitSource;
pub const GitSourceConfig = membank_public.GitSourceConfig;

pub const zig_parser = membank_public.zig_parser;
pub const ZigParser = membank_public.ZigParser;
pub const ZigParserConfig = membank_public.ZigParserConfig;

pub const semantic_chunker = membank_public.semantic_chunker;
pub const SemanticChunker = membank_public.SemanticChunker;
pub const SemanticChunkerConfig = membank_public.SemanticChunkerConfig;

// TCP server for database access
pub const handler = membank_public.handler;
pub const Server = membank_public.Server;

// Standard allocator type alias
pub const Allocator = membank_public.Allocator;

// Version information
pub const version = membank_public.version;

// Initialization function
pub const init = membank_public.init;

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
