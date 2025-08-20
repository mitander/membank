//! Deterministic logger for the simulation environment.
//!
//! This logger uses the simulation's logical clock (tick count) instead of
//! real-world timestamps to ensure log outputs are reproducible across runs.
//! Enabled only in debug builds to maintain zero overhead in release builds.

const builtin = @import("builtin");
const std = @import("std");

const simulation_mod = @import("simulation.zig");

const Simulation = simulation_mod.Simulation;

pub const DeterministicLogger = struct {
    sim: *const Simulation,

    /// Log message with deterministic timestamp using simulation tick count
    /// Provides reproducible log output for simulation tests by using logical time
    /// instead of wall clock timestamps, ensuring identical logs across test runs
    pub fn log(
        self: @This(),
        comptime level: std.log.Level,
        comptime scope: @TypeOf(.EnumLiteral),
        comptime format: []const u8,
        args: anytype,
    ) void {
        if (builtin.mode != .Debug) return;

        // Use a fixed buffer to avoid allocation during logging
        var buf: [1024]u8 = undefined;
        const msg = std.fmt.bufPrint(&buf, format, args) catch "(log format error)";

        const level_name = @tagName(level);
        const scope_name = @tagName(scope);

        std.debug.print("[tick:{d}] [{s}] {s}: {s}\n", .{
            self.sim.tick_count,
            scope_name,
            level_name,
            msg,
        });
    }
};
