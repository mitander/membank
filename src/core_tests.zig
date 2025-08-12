//! Core modules test runner
//! Tests just the core/ modules with all necessary dependencies

// Import all core modules and their dependencies
comptime {
    // Import core modules with their dependencies
    _ = @import("core/concurrency.zig");
    _ = @import("core/production_vfs.zig");
    _ = @import("core/error_context.zig");
    _ = @import("core/types.zig");
    _ = @import("core/file_handle.zig");
    _ = @import("core/state_machines.zig");
    _ = @import("core/assert.zig");
    _ = @import("core/bounded.zig");
    _ = @import("core/vfs.zig");
    _ = @import("core/memory.zig");
    _ = @import("core/ownership.zig");
    _ = @import("core/arena.zig");
    
    // Import dependencies needed by core tests
    _ = @import("sim/simulation_vfs.zig");
}