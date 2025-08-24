//! Cross-file dependency resolver for Zig projects.
//!
//! Tracks import relationships and symbol references across multiple Zig
//! source files to build accurate dependency graphs. Resolves @import()
//! statements, extern declarations, and cross-file function calls.
//!
//! Key capabilities:
//! - Import path resolution and normalization
//! - Symbol export/import tracking across file boundaries
//! - Unresolved reference collection for batch resolution
//! - Dependency cycle detection and reporting
//! - Module-level relationship graph construction
//!
//! Design rationale: Maintains global symbol tables to resolve cross-file
//! references that cannot be determined from single-file parsing. Uses
//! deferred resolution to handle circular dependencies and forward references
//! common in large Zig projects.

const std = @import("std");
const context_types = @import("context.zig");
const context_block = @import("../../core/types.zig");
const ingestion = @import("../pipeline.zig");

const CrossFileSymbol = context_types.CrossFileSymbol;
const EdgeType = context_block.EdgeType;
const ParsedEdge = ingestion.ParsedEdge;
const ParsedUnit = ingestion.ParsedUnit;
const UnresolvedCall = context_types.UnresolvedCall;

pub const CrossFileResolver = struct {
    allocator: std.mem.Allocator,
    symbols: std.StringHashMap(CrossFileSymbol),
    unresolved_calls: std.array_list.Managed(UnresolvedCall),

    pub fn init(allocator: std.mem.Allocator) CrossFileResolver {
        return CrossFileResolver{
            .allocator = allocator,
            .symbols = std.StringHashMap(CrossFileSymbol).init(allocator),
            .unresolved_calls = std.array_list.Managed(UnresolvedCall).init(allocator),
        };
    }

    pub fn deinit(self: *CrossFileResolver) void {
        var symbol_iterator = self.symbols.iterator();
        while (symbol_iterator.next()) |entry| {
            self.allocator.free(entry.key_ptr.*);
            self.allocator.free(entry.value_ptr.unit_id);
            self.allocator.free(entry.value_ptr.symbol_name);
            self.allocator.free(entry.value_ptr.file_path);
            if (entry.value_ptr.container_name) |name| {
                self.allocator.free(name);
            }
        }

        for (self.unresolved_calls.items) |call| {
            self.allocator.free(call.caller_unit_id);
            self.allocator.free(call.call_name);
            self.allocator.free(call.file_path);
        }

        self.symbols.deinit();
        self.unresolved_calls.deinit();
    }

    /// Register a symbol for cross-file resolution
    /// Enables later resolution of unresolved calls to this symbol
    pub fn register_symbol(
        self: *CrossFileResolver,
        symbol_name: []const u8,
        unit_id: []const u8,
        file_path: []const u8,
        is_method: bool,
        container_name: ?[]const u8,
    ) !void {
        if (self.symbols.getPtr(symbol_name)) |existing_symbol| {
            self.allocator.free(existing_symbol.unit_id);
            self.allocator.free(existing_symbol.symbol_name);
            self.allocator.free(existing_symbol.file_path);
            if (existing_symbol.container_name) |name| {
                self.allocator.free(name);
            }

            existing_symbol.unit_id = try self.allocator.dupe(u8, unit_id);
            existing_symbol.symbol_name = try self.allocator.dupe(u8, symbol_name);
            existing_symbol.file_path = try self.allocator.dupe(u8, file_path);
            existing_symbol.is_method = is_method;
            existing_symbol.container_name = if (container_name) |name| try self.allocator.dupe(u8, name) else null;
        } else {
            const symbol = CrossFileSymbol{
                .unit_id = try self.allocator.dupe(u8, unit_id),
                .symbol_name = try self.allocator.dupe(u8, symbol_name),
                .file_path = try self.allocator.dupe(u8, file_path),
                .is_method = is_method,
                .container_name = if (container_name) |name| try self.allocator.dupe(u8, name) else null,
            };

            const key = try self.allocator.dupe(u8, symbol_name);
            try self.symbols.put(key, symbol);
        }
    }

    pub fn register_unresolved_call(
        self: *CrossFileResolver,
        caller_unit_id: []const u8,
        call_name: []const u8,
        is_method_call: bool,
        file_path: []const u8,
    ) !void {
        const unresolved_call = UnresolvedCall{
            .caller_unit_id = try self.allocator.dupe(u8, caller_unit_id),
            .call_name = try self.allocator.dupe(u8, call_name),
            .is_method_call = is_method_call,
            .file_path = try self.allocator.dupe(u8, file_path),
        };

        try self.unresolved_calls.append(unresolved_call);
    }

    /// Resolve cross-file calls by matching unresolved calls with registered symbols
    /// Creates edges between calling units and target symbols across file boundaries
    pub fn resolve_cross_file_calls(
        self: *CrossFileResolver,
        allocator: std.mem.Allocator,
        units: []ParsedUnit,
    ) !void {
        var unit_map = std.StringHashMap(*ParsedUnit).init(allocator);
        defer unit_map.deinit();

        for (units) |*unit| {
            try unit_map.put(unit.id, unit);
        }

        for (self.unresolved_calls.items) |unresolved_call| {
            if (self.symbols.get(unresolved_call.call_name)) |symbol| {
                if (unresolved_call.is_method_call == symbol.is_method) {
                    try self.create_cross_file_edge(allocator, &unit_map, unresolved_call, symbol);
                }
            }
        }

        std.log.debug("DEBUG resolve_cross_file_calls: {} symbols, {} unresolved calls", .{ self.symbols.count(), self.unresolved_calls.items.len });
    }

    fn create_cross_file_edge(
        self: *CrossFileResolver,
        allocator: std.mem.Allocator,
        unit_map: *std.StringHashMap(*ParsedUnit),
        unresolved_call: UnresolvedCall,
        symbol: CrossFileSymbol,
    ) !void {
        _ = self;

        if (unit_map.get(unresolved_call.caller_unit_id)) |caller_unit| {
            const edge_type = if (symbol.is_method)
                EdgeType.calls_method
            else
                EdgeType.calls_function;

            var edge_metadata = std.StringHashMap([]const u8).init(allocator);
            try edge_metadata.put("target_file", try allocator.dupe(u8, symbol.file_path));
            try edge_metadata.put("caller_file", try allocator.dupe(u8, unresolved_call.file_path));

            if (symbol.container_name) |container| {
                try edge_metadata.put("target_container", try allocator.dupe(u8, container));
            }

            const edge = ParsedEdge{
                .target_id = try allocator.dupe(u8, symbol.unit_id),
                .edge_type = edge_type,
                .metadata = edge_metadata,
            };

            try caller_unit.edges.append(edge);
        }
    }
};
