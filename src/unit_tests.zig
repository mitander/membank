//! Comprehensive unit test runner for KausalDB components
//! Incrementally enabling modules to systematically fix issues

// Import modules in dependency order, enabling more as issues are resolved
comptime {
    // Phase 1: Core foundation modules (most basic)
    _ = @import("core/concurrency.zig");
    _ = @import("core/error_context.zig");
    _ = @import("core/types.zig");
    _ = @import("core/assert.zig");
    _ = @import("core/bounded.zig");
    _ = @import("core/memory.zig");
    _ = @import("core/ownership.zig");
    _ = @import("core/arena.zig");

    // Phase 2: Core with dependencies
    _ = @import("sim/simulation_vfs.zig");
    _ = @import("core/production_vfs.zig");
    _ = @import("core/file_handle.zig");
    _ = @import("core/state_machines.zig");
    _ = @import("core/vfs.zig");

    // Phase 3: Storage fundamentals
    _ = @import("storage/config.zig");
    _ = @import("storage/wal/types.zig");
    _ = @import("storage/wal/entry.zig");
    _ = @import("storage/wal/core.zig");
    _ = @import("storage/bloom_filter.zig");
    _ = @import("storage/metrics.zig");

    // Phase 4: WAL modules (working)
    _ = @import("storage/wal/stream.zig");
    _ = @import("storage/wal/recovery.zig");
    _ = @import("storage/wal/corruption_tracker.zig");

    // Phase 5: Storage indexes (next to enable)
    _ = @import("storage/graph_edge_index.zig");
    _ = @import("storage/block_index.zig");

    // TODO: Enable more modules incrementally as issues are fixed
    // _ = @import("storage/recovery.zig");
    // _ = @import("storage/sstable.zig");
    // _ = @import("storage/sstable_manager.zig");
    // _ = @import("storage/memtable_manager.zig");
    // _ = @import("storage/wal.zig");
    // _ = @import("storage/engine.zig");
    // _ = @import("storage/tiered_compaction.zig");

    // _ = @import("query/cache.zig");
    // _ = @import("query/operations.zig");
    // _ = @import("query/filtering.zig");
    // _ = @import("query/traversal.zig");
    // _ = @import("query/engine.zig");

    // _ = @import("ingestion/glob_matcher.zig");
    // _ = @import("ingestion/semantic_chunker.zig");
    // _ = @import("ingestion/zig_parser.zig");
    // _ = @import("ingestion/git_source.zig");
    // _ = @import("ingestion/pipeline.zig");

    // _ = @import("server/connection_manager.zig");
    // _ = @import("server/handler.zig");

    // _ = @import("sim/simulation.zig");

    // _ = @import("dev/shell.zig");
    // _ = @import("dev/tidy/rules.zig");
    // _ = @import("dev/tidy/patterns.zig");
    // _ = @import("dev/commit_msg_validator.zig");
    // _ = @import("dev/tidy.zig");
    // _ = @import("dev/allocator_torture_test.zig");
    // _ = @import("dev/debug_allocator.zig");
    // _ = @import("dev/fuzz/common.zig");
}
