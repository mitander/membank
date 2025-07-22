//! CortexDB - High-Performance Deterministic Database
//!
//! CortexDB is a high-performance, mission-critical database built on principles of:
//! - Simplicity is the Prerequisite for Reliability
//! - Correctness is Not Negotiable
//! - Explicitness Over Magic
//! - Determinism & Testability
//!
//! This module serves as the root API for the CortexDB database system.

const std = @import("std");
const builtin = @import("builtin");

// Core infrastructure
pub const assert = @import("assert.zig");
pub const vfs = @import("vfs.zig");
pub const context_block = @import("context_block.zig");
pub const error_context = @import("error_context.zig");
pub const concurrency = @import("concurrency.zig");

// Simulation framework for deterministic testing
pub const simulation = .{
    .core = @import("simulation.zig"),
    .vfs = @import("simulation_vfs.zig"),
};

// Storage layer components
pub const storage = .{
    .engine = @import("storage.zig"),
    .sstable = @import("sstable.zig"),
    .compaction = @import("tiered_compaction.zig"),
};

// Query processing
pub const query = @import("query_engine.zig");

// Memory debugging and testing infrastructure
pub const testing = .{
    .debug_allocator = @import("debug_allocator.zig"),
    .allocator_torture = @import("allocator_torture_test.zig"),
};

// Version information
pub const version = .{
    .major = 0,
    .minor = 1,
    .patch = 0,
};

/// Initialize a CortexDB instance with the given configuration
pub const Database = storage.engine.StorageEngine;

/// Configuration for CortexDB instance
pub const Config = storage.engine.Config;

/// Core types re-exported for convenience
pub const ContextBlock = context_block.ContextBlock;
pub const BlockId = context_block.BlockId;
pub const VFS = vfs.VFS;
pub const Allocator = std.mem.Allocator;

/// Error types that can be returned by CortexDB operations
pub const Error = storage.engine.StorageError;

/// Initialize CortexDB with recommended defaults for production use
pub fn init(allocator: Allocator, config: Config) !Database {
    return Database.init(allocator, config);
}

/// Initialize CortexDB with enhanced debugging enabled
pub fn init_with_debug_allocator(allocator: Allocator, config: Config) !struct {
    database: Database,
    debug_allocator: testing.debug_allocator.DebugAllocator,
} {
    var debug_alloc = testing.debug_allocator.DebugAllocator.init(allocator);
    const debug_allocator_instance = debug_alloc.allocator();

    const database = try Database.init(debug_allocator_instance, config);

    return .{
        .database = database,
        .debug_allocator = debug_alloc,
    };
}

/// Run comprehensive memory safety validation
pub fn validate_memory_safety(allocator: Allocator, config: testing.allocator_torture.TortureTestConfig) !testing.allocator_torture.TortureTestStats {
    return testing.allocator_torture.run_allocator_torture_test(allocator, config);
}

comptime {
    // Ensure we're using a supported Zig version
    if (builtin.zig_version.major != 0 or builtin.zig_version.minor < 13) {
        @compileError("CortexDB requires Zig 0.13.0 or later");
    }
}
