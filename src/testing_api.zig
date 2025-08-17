//! Internal Testing API
//!
//! Drop-in replacement for the public API with additional internal components
//! for testing, debugging, and development.
//!
//! Import as @import("kausaldb") in test files.

const std = @import("std");

// Configure quieter logging for tests by default
// Note: Test configuration needs to be called at runtime, not comptime

const kausaldb_public = @import("kausaldb.zig");

pub const types = kausaldb_public.types;
pub const ContextBlock = kausaldb_public.ContextBlock;
pub const BlockId = kausaldb_public.BlockId;
pub const GraphEdge = kausaldb_public.GraphEdge;
pub const EdgeType = kausaldb_public.EdgeType;

pub const storage = kausaldb_public.storage;
pub const Database = kausaldb_public.Database;
pub const StorageEngine = kausaldb_public.StorageEngine;
pub const Config = kausaldb_public.Config;
pub const Error = kausaldb_public.Error;

pub const query_engine = kausaldb_public.query_engine;
pub const QueryEngine = kausaldb_public.QueryEngine;
pub const QueryResult = kausaldb_public.QueryResult;
pub const TraversalDirection = kausaldb_public.TraversalDirection;
pub const TraversalResult = kausaldb_public.TraversalResult;
pub const TraversalQuery = kausaldb_public.TraversalQuery;
pub const TraversalAlgorithm = kausaldb_public.TraversalAlgorithm;
pub const FindBlocksQuery = @import("query/operations.zig").FindBlocksQuery;
pub const SemanticQuery = kausaldb_public.SemanticQuery;
pub const SemanticQueryResult = kausaldb_public.SemanticQueryResult;
pub const SemanticResult = kausaldb_public.SemanticResult;
pub const FilteredQuery = kausaldb_public.FilteredQuery;
pub const FilteredQueryResult = kausaldb_public.FilteredQueryResult;
pub const FilterCondition = kausaldb_public.FilterCondition;
pub const FilterExpression = kausaldb_public.FilterExpression;
pub const FilterOperator = kausaldb_public.FilterOperator;
pub const FilterTarget = kausaldb_public.FilterTarget;

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

pub const handler = kausaldb_public.handler;
pub const Server = kausaldb_public.Server;
pub const connection_manager = @import("server/connection_manager.zig");

pub const Allocator = kausaldb_public.Allocator;

pub const version = kausaldb_public.version;

pub const init = kausaldb_public.init;

pub const simulation = @import("sim/simulation.zig");
pub const simulation_vfs = @import("sim/simulation_vfs.zig");
pub const Simulation = simulation.Simulation;
pub const SimulationVFS = simulation_vfs.SimulationVFS;

pub const VFS = @import("core/vfs.zig").VFS;
pub const VFile = @import("core/vfs.zig").VFile;
pub const vfs = @import("core/vfs.zig");
pub const production_vfs = @import("core/production_vfs.zig");

pub const assert = @import("core/assert.zig");
pub const concurrency = @import("core/concurrency.zig");
pub const error_context = @import("core/error_context.zig");
pub const stdx = @import("core/stdx.zig");
pub const memory = @import("core/memory.zig");

// Core type safety modules
pub const arena = @import("core/arena.zig");
pub const ownership = @import("core/ownership.zig");
pub const state_machines = @import("core/state_machines.zig");
pub const file_handle = @import("core/file_handle.zig");
pub const bounded = @import("core/bounded.zig");
pub const core_types = @import("core/types.zig");

// Test utilities and frameworks
pub const golden_master = @import("testing/golden_master.zig");
pub const performance_assertions = @import("testing/performance_assertions.zig");
pub const PerformanceAssertion = performance_assertions.PerformanceAssertion;
// Simple test configuration - use build_options.debug_tests for conditional output
pub const PerformanceTier = performance_assertions.PerformanceTier;
pub const BatchPerformanceMeasurement = performance_assertions.BatchPerformanceMeasurement;

pub const fatal_assertions = @import("testing/fatal_assertions.zig");
pub const FatalCategory = fatal_assertions.FatalCategory;
pub const FatalContext = fatal_assertions.FatalContext;

// Test harnesses for standardized setup patterns
pub const test_harness = @import("testing/harness.zig");
pub const TestData = test_harness.TestData;
pub const StorageHarness = test_harness.StorageHarness;
pub const QueryHarness = test_harness.QueryHarness;
pub const SimulationHarness = test_harness.SimulationHarness;
pub const FaultInjectionHarness = test_harness.FaultInjectionHarness;
pub const FaultInjectionConfig = test_harness.FaultInjectionConfig;

// Scenario-driven fault injection testing framework
pub const scenarios = @import("testing/scenarios.zig");
pub const FaultScenario = scenarios.FaultScenario;
pub const ScenarioExecutor = scenarios.ScenarioExecutor;
pub const WalDurabilityScenario = scenarios.WalDurabilityScenario;
pub const CompactionCrashScenario = scenarios.CompactionCrashScenario;

pub const bloom_filter = @import("storage/bloom_filter.zig");
pub const sstable = @import("storage/sstable.zig");
pub const tiered_compaction = @import("storage/tiered_compaction.zig");

pub const wal = struct {
    pub const WAL = @import("storage/wal/core.zig").WAL;
    pub const WALEntry = @import("storage/wal/entry.zig").WALEntry;
    pub const WALError = @import("storage/wal/types.zig").WALError;
    pub const WALEntryType = @import("storage/wal/types.zig").WALEntryType;
    pub const corruption_tracker = @import("storage/wal/corruption_tracker.zig");
    pub const entry = @import("storage/wal/entry.zig");
    pub const recovery = @import("storage/wal/recovery.zig");
    pub const types = @import("storage/wal/types.zig");
    pub const stream = @import("storage/wal/stream.zig");
};

pub const query_operations = @import("query/operations.zig");
pub const query_traversal = @import("query/traversal.zig");
pub const query_filtering = @import("query/filtering.zig");

pub const query = struct {
    pub const engine = @import("query/engine.zig");
    pub const operations = @import("query/operations.zig");
    pub const traversal = @import("query/traversal.zig");
    pub const filtering = @import("query/filtering.zig");
    pub const cache = @import("query/cache.zig");
    pub const FindBlocksQuery = @import("query/operations.zig").FindBlocksQuery;
};

pub const debug_allocator = @import("dev/debug_allocator.zig");
pub const profiler = @import("dev/profiler.zig");

pub const main = @import("main.zig");
