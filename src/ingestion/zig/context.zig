//! Context types for Zig parser
//!
//! Minimal types for parser configuration and batch operations.
//! No compiler dependencies for stability across Zig versions.

const std = @import("std");
const pipeline = @import("../pipeline.zig");

const ParsedUnit = pipeline.ParsedUnit;

// Re-export parser config from main parser module
pub const ZigParserConfig = @import("parser.zig").ZigParserConfig;

pub const BatchParseResult = struct {
    units: []ParsedUnit,
    allocator: std.mem.Allocator,

    pub fn init(allocator: std.mem.Allocator, units: []ParsedUnit) BatchParseResult {
        return .{
            .units = units,
            .allocator = allocator,
        };
    }

    pub fn deinit(self: *BatchParseResult) void {
        for (self.units) |*unit| {
            unit.deinit(self.allocator);
        }
        self.allocator.free(self.units);
    }
};
