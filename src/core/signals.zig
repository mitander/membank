//! Signal handling for graceful shutdown
//!
//! Provides cross-platform signal handling for SIGINT and SIGTERM to enable
//! graceful shutdown of the database server. Follows KausalDB principles:
//! - Single-threaded with global signal state
//! - Explicit error handling without hidden allocations
//! - Production-safe with proper cleanup coordination

const std = @import("std");

const assert_mod = @import("assert.zig");

const assert = assert_mod.assert;
const log = std.log.scoped(.signals);

/// Global shutdown signal state
var shutdown_requested: bool = false;

/// Check if graceful shutdown has been requested
pub fn should_shutdown() bool {
    return shutdown_requested;
}

/// Request graceful shutdown (for testing or programmatic shutdown)
pub fn request_shutdown() void {
    shutdown_requested = true;
    log.info("Graceful shutdown requested programmatically", .{});
}

/// Signal handler for SIGINT and SIGTERM
fn signal_handler(sig: c_int) callconv(.c) void {
    switch (sig) {
        std.posix.SIG.INT => {
            shutdown_requested = true;
            log.info("Received SIGINT, initiating graceful shutdown", .{});
        },
        std.posix.SIG.TERM => {
            shutdown_requested = true;
            log.info("Received SIGTERM, initiating graceful shutdown", .{});
        },
        else => {
            log.warn("Received unexpected signal: {d}", .{sig});
        },
    }
}

/// Setup signal handlers for graceful shutdown
pub fn setup_signal_handlers() !void {
    const action = std.posix.Sigaction{
        .handler = .{ .handler = signal_handler },
        .mask = std.mem.zeroes(std.posix.sigset_t),
        .flags = 0,
    };

    _ = std.posix.sigaction(std.posix.SIG.INT, &action, null);
    _ = std.posix.sigaction(std.posix.SIG.TERM, &action, null);

    log.info("Signal handlers installed for SIGINT and SIGTERM", .{});
}

/// Reset shutdown state (for testing)
pub fn reset_shutdown_state() void {
    shutdown_requested = false;
}

test "signal handling state management" {
    const testing = std.testing;

    // Initial state should be false
    try testing.expect(!should_shutdown());

    // Request shutdown programmatically
    request_shutdown();
    try testing.expect(should_shutdown());

    // Reset state
    reset_shutdown_state();
    try testing.expect(!should_shutdown());
}
