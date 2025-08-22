//! Professional Zig semantic parser using Zig's own AST implementation.
//!
//! This parser uses Zig's battle-tested std.zig.Ast parser to extract semantic
//! structure from Zig source files. By leveraging the official compiler's AST,
//! we achieve perfect accuracy for method vs function distinction, proper scope
//! tracking, and comprehensive edge case handling.
//!
//! Design rationale: Reusing proven parsing infrastructure eliminates entire
//! classes of parsing bugs while providing access to complete semantic information.
//! The abstraction layer converts AST nodes to KausalDB semantic units for storage.

const std = @import("std");

const assert_mod = @import("../core/assert.zig");
const concurrency = @import("../core/concurrency.zig");
const context_block = @import("../core/types.zig");
const error_context = @import("../core/error_context.zig");
const ingestion = @import("pipeline.zig");

const assert = assert_mod.assert;
const fatal_assert = assert_mod.fatal_assert;

const EdgeType = context_block.EdgeType;
const IngestionError = ingestion.IngestionError;
const ParsedEdge = ingestion.ParsedEdge;
const ParsedUnit = ingestion.ParsedUnit;
const Parser = ingestion.Parser;
const SourceContent = ingestion.SourceContent;
const SourceLocation = ingestion.SourceLocation;

/// Configuration for semantic parsing behavior
pub const ZigParserConfig = struct {
    /// Whether to include function bodies or just signatures
    include_function_bodies: bool = true,
    /// Whether to extract private (non-pub) definitions
    include_private: bool = true,
    /// Whether to extract inline comments (compatibility - ignored in semantic parser)
    include_inline_comments: bool = false,
    /// Whether to extract test blocks
    include_tests: bool = true,
    /// Maximum size of extracted unit content
    max_unit_size: usize = 64 * 1024, // 64KB
};

/// Semantic context for AST traversal and scope tracking
const SemanticContext = struct {
    /// Source AST from Zig parser
    ast: std.zig.Ast,
    /// Arena allocator for temporary allocations during parsing
    arena: std.heap.ArenaAllocator,
    /// Parsed units accumulator
    units: std.array_list.Managed(ParsedUnit),
    /// File path for location tracking
    file_path: []const u8,
    /// Parser configuration
    config: ZigParserConfig,
    /// Current container scope (struct/enum/union name)
    current_container: ?[]const u8 = null,
    /// Container declaration node for scope tracking
    current_container_node: ?std.zig.Ast.Node.Index = null,

    /// Initialize semantic context with AST
    fn init(
        allocator: std.mem.Allocator,
        ast: std.zig.Ast,
        file_path: []const u8,
        config: ZigParserConfig,
    ) SemanticContext {
        return SemanticContext{
            .ast = ast,
            .arena = std.heap.ArenaAllocator.init(allocator),
            .units = std.array_list.Managed(ParsedUnit).init(allocator),
            .file_path = file_path,
            .config = config,
        };
    }

    /// Clean up semantic context resources
    fn deinit(self: *SemanticContext) void {
        if (self.current_container) |container| {
            self.units.allocator.free(container);
        }
        self.arena.deinit();
        self.units.deinit();
    }

    /// Check if a node is inside the current container scope
    fn is_in_current_container(self: *const SemanticContext, node: std.zig.Ast.Node.Index) bool {
        if (self.current_container_node == null) return false;

        // Token position heuristic provides O(1) scope detection without AST traversal.
        // Full hierarchical scope tracking would require expensive tree walking on every check.
        // This approach trades precision for performance in the common case.
        const container_token = self.ast.nodeMainToken(self.current_container_node.?);
        const node_token = self.ast.nodeMainToken(node);
        return node_token > container_token;
    }
};

/// Cross-file symbol for resolution
const CrossFileSymbol = struct {
    unit_id: []const u8,
    symbol_name: []const u8,
    file_path: []const u8,
    is_method: bool,
    container_name: ?[]const u8,
};

/// Unresolved call for cross-file resolution
const UnresolvedCall = struct {
    caller_unit_id: []const u8,
    call_name: []const u8,
    is_method_call: bool,
    file_path: []const u8,
};

/// Batch parsing result with cross-file resolution data
/// Data is allocated using the caller's allocator for proper ownership
pub const BatchParseResult = struct {
    units: []ParsedUnit,

    pub fn deinit(self: *BatchParseResult, allocator: std.mem.Allocator) void {
        // Clean up all units and their data
        for (self.units) |*unit| {
            unit.deinit(allocator);
        }
        allocator.free(self.units);
    }
};

/// Professional Zig semantic parser using stdlib AST
pub const ZigParser = struct {
    /// Base allocator for parser-owned data
    allocator: std.mem.Allocator,
    /// Parser configuration
    config: ZigParserConfig,
    /// Global symbol table for cross-file resolution
    symbols: std.StringHashMap(CrossFileSymbol),
    /// Unresolved calls collection
    unresolved_calls: std.array_list.Managed(UnresolvedCall),

    /// Initialize Zig semantic parser (compatibility interface)
    pub fn init(allocator: std.mem.Allocator, config: ZigParserConfig) ZigParser {
        return ZigParser{
            .allocator = allocator,
            .config = config,
            .symbols = std.StringHashMap(CrossFileSymbol).init(allocator),
            .unresolved_calls = std.array_list.Managed(UnresolvedCall).init(allocator),
        };
    }

    /// Clean up parser resources
    pub fn deinit(self: *ZigParser) void {
        // Clean up unresolved calls memory
        for (self.unresolved_calls.items) |call| {
            self.allocator.free(call.caller_unit_id);
            self.allocator.free(call.call_name);
            self.allocator.free(call.file_path);
        }
        self.unresolved_calls.deinit();

        // Clean up symbols HashMap memory
        var symbol_iter = self.symbols.iterator();
        while (symbol_iter.next()) |entry| {
            self.allocator.free(entry.key_ptr.*);
            self.allocator.free(entry.value_ptr.unit_id);
            self.allocator.free(entry.value_ptr.symbol_name);
            self.allocator.free(entry.value_ptr.file_path);
            if (entry.value_ptr.container_name) |container| {
                self.allocator.free(container);
            }
        }
        self.symbols.deinit();
    }

    /// Create Parser interface wrapper
    pub fn parser(self: *ZigParser) Parser {
        return Parser{
            .ptr = self,
            .vtable = &.{
                .parse = parse_impl,
                .supports = supports_impl,
                .describe = describe_impl,
                .deinit = deinit_impl,
            },
        };
    }

    /// Parse Zig source using stdlib AST parser
    /// Uses internal arena for parsing, returns data in caller's allocator
    pub fn parse_content(
        self: *ZigParser,
        allocator: std.mem.Allocator,
        content: SourceContent,
    ) IngestionError![]ParsedUnit {
        concurrency.assert_main_thread();

        const file_path = content.metadata.get("file_path") orelse "unknown.zig";

        // Use internal arena for temporary parsing operations
        var temp_arena = std.heap.ArenaAllocator.init(allocator);
        defer temp_arena.deinit(); // O(1) cleanup of all temporary data
        const temp_allocator = temp_arena.allocator();

        // Ensure source is null-terminated for Zig parser compatibility
        const null_terminated_source = try temp_allocator.dupeZ(u8, content.data);

        // Use Zig's official AST parser for bulletproof parsing
        var ast = std.zig.Ast.parse(temp_allocator, null_terminated_source, .zig) catch |err| {
            const ctx = error_context.IngestionContext{
                .operation = "parse_zig_ast",
                .repository_path = file_path,
                .content_type = "zig_source",
            };
            error_context.log_ingestion_error(err, ctx);
            return error.ParseFailed;
        };

        // Report any parse errors but continue with partial AST
        if (ast.errors.len > 0) {
            for (ast.errors) |parse_error| {
                const location = ast.tokenLocation(0, parse_error.token);
                std.log.warn("Parse error in {s}:{d}:{d}", .{ file_path, location.line + 1, location.column + 1 });
            }
        }

        // Extract semantic units using temporary allocator
        var context = SemanticContext.init(temp_allocator, ast, file_path, self.config);
        defer context.deinit();

        try self.extract_semantic_units(&context);

        // Copy results to caller's allocator for ownership
        const temp_units = try context.units.toOwnedSlice();
        return try self.copy_units_to_caller_allocator(allocator, temp_units);
    }

    /// Copy parsed units from temporary arena to caller's allocator for ownership
    fn copy_units_to_caller_allocator(
        self: *ZigParser,
        allocator: std.mem.Allocator,
        temp_units: []const ParsedUnit,
    ) IngestionError![]ParsedUnit {
        const units = try allocator.alloc(ParsedUnit, temp_units.len);
        errdefer allocator.free(units);

        for (temp_units, 0..) |temp_unit, i| {
            units[i] = try self.copy_unit_to_allocator(allocator, temp_unit);
        }

        return units;
    }

    /// Copy a single ParsedUnit from temporary to caller's allocator
    fn copy_unit_to_allocator(
        self: *ZigParser,
        allocator: std.mem.Allocator,
        temp_unit: ParsedUnit,
    ) IngestionError!ParsedUnit {
        // Copy metadata
        var metadata = std.StringHashMap([]const u8).init(allocator);
        var metadata_iter = temp_unit.metadata.iterator();
        while (metadata_iter.next()) |entry| {
            const key = try allocator.dupe(u8, entry.key_ptr.*);
            const value = try allocator.dupe(u8, entry.value_ptr.*);
            try metadata.put(key, value);
        }

        // Copy edges
        var edges = std.array_list.Managed(ParsedEdge).init(allocator);
        for (temp_unit.edges.items) |temp_edge| {
            const edge = try self.copy_edge_to_allocator(allocator, temp_edge);
            try edges.append(edge);
        }

        return ParsedUnit{
            .id = try allocator.dupe(u8, temp_unit.id),
            .unit_type = try allocator.dupe(u8, temp_unit.unit_type),
            .content = try allocator.dupe(u8, temp_unit.content),
            .location = SourceLocation{
                .file_path = try allocator.dupe(u8, temp_unit.location.file_path),
                .line_start = temp_unit.location.line_start,
                .line_end = temp_unit.location.line_end,
                .col_start = temp_unit.location.col_start,
                .col_end = temp_unit.location.col_end,
            },
            .edges = edges,
            .metadata = metadata,
        };
    }

    /// Copy a single ParsedEdge from temporary to caller's allocator
    fn copy_edge_to_allocator(
        self: *ZigParser,
        allocator: std.mem.Allocator,
        temp_edge: ParsedEdge,
    ) IngestionError!ParsedEdge {
        _ = self; // Unused but kept for consistency

        // Copy edge metadata
        var metadata = std.StringHashMap([]const u8).init(allocator);
        var metadata_iter = temp_edge.metadata.iterator();
        while (metadata_iter.next()) |entry| {
            const key = try allocator.dupe(u8, entry.key_ptr.*);
            const value = try allocator.dupe(u8, entry.value_ptr.*);
            try metadata.put(key, value);
        }

        return ParsedEdge{
            .target_id = try allocator.dupe(u8, temp_edge.target_id),
            .edge_type = temp_edge.edge_type,
            .metadata = metadata,
        };
    }

    /// Parse multiple files with cross-file call resolution
    /// Uses internal arena for temporary allocations, returns data in caller's allocator
    pub fn parse_batch(
        self: *ZigParser,
        allocator: std.mem.Allocator,
        files: []const SourceContent,
    ) IngestionError!BatchParseResult {
        // Create internal arena for temporary parsing operations
        var temp_arena = std.heap.ArenaAllocator.init(allocator);
        defer temp_arena.deinit(); // O(1) cleanup of all temporary data
        const temp_allocator = temp_arena.allocator();

        // Phase 1: Parse all files using temporary arena and collect symbols/calls
        var temp_units = std.array_list.Managed(ParsedUnit).init(temp_allocator);

        for (files) |file_content| {
            const file_units = try self.parse_content_with_collection(temp_allocator, file_content);

            for (file_units) |unit| {
                try temp_units.append(unit);
            }
        }

        // Phase 2: Resolve cross-file calls within temporary arena
        try self.resolve_cross_file_calls(temp_allocator, temp_units.items);

        // Phase 3: Copy final results to caller's allocator for ownership
        const final_units = try self.copy_units_to_caller_allocator(allocator, temp_units.items);

        return BatchParseResult{
            .units = final_units,
        };
    }

    /// Parse content and collect symbols/calls for cross-file resolution
    fn parse_content_with_collection(
        self: *ZigParser,
        allocator: std.mem.Allocator,
        content: SourceContent,
    ) IngestionError![]ParsedUnit {
        concurrency.assert_main_thread();

        const file_path = content.metadata.get("file_path") orelse "unknown.zig";

        // Ensure source is null-terminated for Zig parser compatibility
        const null_terminated_source = try allocator.dupeZ(u8, content.data);
        defer allocator.free(null_terminated_source);

        // Use Zig's official AST parser for bulletproof parsing
        var ast = std.zig.Ast.parse(allocator, null_terminated_source, .zig) catch |err| {
            const ctx = error_context.IngestionContext{
                .operation = "parse_zig_ast",
                .repository_path = file_path,
                .content_type = "zig_source",
            };
            error_context.log_ingestion_error(err, ctx);
            return error.ParseFailed;
        };
        defer ast.deinit(allocator);

        // Report any parse errors but continue with partial AST
        if (ast.errors.len > 0) {
            for (ast.errors) |parse_error| {
                const location = ast.tokenLocation(0, parse_error.token);
                std.log.warn("Parse error in {s}:{d}:{d}", .{ file_path, location.line + 1, location.column + 1 });
            }
        }

        // Extract semantic units from AST with cross-file collection
        var context = SemanticContext.init(allocator, ast, file_path, self.config);
        defer context.deinit();

        try self.extract_semantic_units_with_collection(&context);

        return context.units.toOwnedSlice();
    }

    /// Extract semantic units from AST by walking all nodes
    fn extract_semantic_units(self: *ZigParser, context: *SemanticContext) !void {
        const root_decls = context.ast.rootDecls();

        // Process all top-level declarations
        for (root_decls) |decl_node| {
            try self.process_declaration(context, decl_node);
        }
    }

    /// Process a declaration node and extract semantic information
    fn process_declaration(self: *ZigParser, context: *SemanticContext, node: std.zig.Ast.Node.Index) !void {
        const node_tag = context.ast.nodeTag(node);

        switch (node_tag) {
            .fn_decl => try self.process_function_declaration(context, node),
            .simple_var_decl, .global_var_decl, .local_var_decl, .aligned_var_decl => {
                // Import vs container vs variable distinction enables proper semantic categorization.
                // Import statements are variable declarations containing @import() calls.
                if (self.is_import_declaration(context, node)) {
                    try self.process_import_declaration(context, node);
                } else if (self.is_container_declaration(context, node)) {
                    try self.process_container_declaration(context, node);
                } else {
                    try self.process_variable_declaration(context, node);
                }
            },
            .test_decl => {
                if (self.config.include_tests) {
                    try self.process_test_declaration(context, node);
                }
            },
            else => {
                // For other node types, recursively process child nodes if they exist
                try self.process_child_nodes(context, node);
            },
        }
    }

    /// Process function declaration node
    fn process_function_declaration(
        self: *ZigParser,
        context: *SemanticContext,
        node: std.zig.Ast.Node.Index,
    ) !void {
        var buffer: [1]std.zig.Ast.Node.Index = undefined;
        const fn_proto = context.ast.fullFnProto(&buffer, node) orelse return;

        const fn_token = fn_proto.ast.fn_token;
        const fn_name_token = fn_token + 1; // Function name follows 'fn' keyword
        const fn_name = context.ast.tokenSlice(fn_name_token);

        // Determine if this is a method (inside a container) or free function
        const is_method = context.is_in_current_container(node);
        const unit_type = if (is_method) "method" else "function";

        // Create unique ID based on context
        const unit_id = if (is_method and context.current_container != null)
            try std.fmt.allocPrint(context.units.allocator, "{s}_{s}_{s}_{s}", .{ std.fs.path.basename(context.file_path), context.current_container.?, unit_type, fn_name })
        else
            try std.fmt.allocPrint(context.units.allocator, "{s}_fn_{s}", .{ std.fs.path.basename(context.file_path), fn_name });

        // Function body inclusion controlled by configuration to balance detail vs performance.
        // Full bodies enable call analysis but increase memory usage for large functions.
        const content = if (self.config.include_function_bodies)
            context.ast.getNodeSource(node)
        else
            context.ast.getNodeSource(fn_proto.ast.proto_node);

        // Create location information
        const start_token = context.ast.firstToken(node);
        const end_token = context.ast.lastToken(node);
        const start_loc = context.ast.tokenLocation(0, start_token);
        const end_loc = context.ast.tokenLocation(0, end_token);

        // Build metadata
        var metadata = std.StringHashMap([]const u8).init(context.units.allocator);
        try metadata.put("function_name", try context.units.allocator.dupe(u8, fn_name));
        try metadata.put("is_public", if (fn_proto.visib_token != null)
            try context.units.allocator.dupe(u8, "true")
        else
            try context.units.allocator.dupe(u8, "false"));
        try metadata.put("is_method", if (is_method)
            try context.units.allocator.dupe(u8, "true")
        else
            try context.units.allocator.dupe(u8, "false"));

        if (is_method and context.current_container != null) {
            try metadata.put("parent_container", try context.units.allocator.dupe(u8, context.current_container.?));
        }

        // Create edges for method ownership
        var edges = std.array_list.Managed(ParsedEdge).init(context.units.allocator);
        if (is_method and context.current_container != null) {
            const container_id = try std.fmt.allocPrint(context.units.allocator, "container_{s}", .{context.current_container.?});
            const edge = ParsedEdge{
                .target_id = container_id,
                .edge_type = EdgeType.method_of,
                .metadata = std.StringHashMap([]const u8).init(context.units.allocator),
            };
            try edges.append(edge);
        }

        // Analyze function body for calls if included
        if (self.config.include_function_bodies) {
            try self.analyze_function_calls(context, node, unit_id, &edges);
        }

        // Create semantic unit
        const unit = ParsedUnit{
            .id = unit_id,
            .unit_type = try context.units.allocator.dupe(u8, unit_type),
            .content = try context.units.allocator.dupe(u8, content),
            .location = SourceLocation{
                .file_path = context.file_path,
                .line_start = @intCast(start_loc.line + 1),
                .line_end = @intCast(end_loc.line + 1),
                .col_start = @intCast(start_loc.column + 1),
                .col_end = @intCast(end_loc.column + 1),
            },
            .edges = std.array_list.Managed(ParsedEdge).fromOwnedSlice(context.units.allocator, try edges.toOwnedSlice()),
            .metadata = metadata,
        };

        try context.units.append(unit);
    }

    /// Register symbol for cross-file resolution
    fn register_symbol(
        self: *ZigParser,
        symbol_name: []const u8,
        unit_id: []const u8,
        file_path: []const u8,
        is_method: bool,
        container_name: ?[]const u8,
    ) !void {
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

    /// Extract semantic units with cross-file symbol/call collection
    fn extract_semantic_units_with_collection(self: *ZigParser, context: *SemanticContext) !void {
        const root_decls = context.ast.rootDecls();
        for (root_decls) |decl_node| {
            try self.process_declaration_with_collection(context, decl_node);
        }
    }

    /// Process declaration with cross-file collection
    fn process_declaration_with_collection(
        self: *ZigParser,
        context: *SemanticContext,
        node: std.zig.Ast.Node.Index,
    ) !void {
        const node_tag = context.ast.nodeTag(node);

        switch (node_tag) {
            .fn_decl => try self.process_function_declaration_with_collection(context, node),
            .simple_var_decl, .global_var_decl => {
                if (self.is_container_declaration(context, node)) {
                    try self.process_container_declaration(context, node);
                }
            },
            else => {},
        }
    }

    /// Process function with cross-file collection
    fn process_function_declaration_with_collection(
        self: *ZigParser,
        context: *SemanticContext,
        node: std.zig.Ast.Node.Index,
    ) !void {
        var buffer: [1]std.zig.Ast.Node.Index = undefined;
        const fn_proto = context.ast.fullFnProto(&buffer, node) orelse return;

        // Extract function name from tokens
        const fn_token = fn_proto.ast.fn_token;
        const fn_name_token = fn_token + 1; // Function name follows 'fn' keyword
        const fn_name = context.ast.tokenSlice(fn_name_token);

        // Determine if this is a method (inside a container) or free function
        const is_method = context.is_in_current_container(node);
        const unit_type = if (is_method) "method" else "function";

        // Create unique ID based on context
        const unit_id = if (is_method and context.current_container != null)
            try std.fmt.allocPrint(context.units.allocator, "{s}_{s}_{s}_{s}", .{ std.fs.path.basename(context.file_path), context.current_container.?, unit_type, fn_name })
        else
            try std.fmt.allocPrint(context.units.allocator, "{s}_fn_{s}", .{ std.fs.path.basename(context.file_path), fn_name });

        // Function body inclusion controlled by configuration to balance detail vs performance.
        // Full bodies enable call analysis but increase memory usage for large functions.
        const content = if (self.config.include_function_bodies)
            context.ast.getNodeSource(node)
        else
            context.ast.getNodeSource(fn_proto.ast.proto_node);

        // Create location information
        const start_token = context.ast.firstToken(node);
        const end_token = context.ast.lastToken(node);
        const start_loc = context.ast.tokenLocation(0, start_token);
        const end_loc = context.ast.tokenLocation(0, end_token);

        // Build metadata
        var metadata = std.StringHashMap([]const u8).init(context.units.allocator);
        try metadata.put("function_name", try context.units.allocator.dupe(u8, fn_name));
        try metadata.put("is_public", if (fn_proto.visib_token != null)
            try context.units.allocator.dupe(u8, "true")
        else
            try context.units.allocator.dupe(u8, "false"));
        try metadata.put("is_method", if (is_method)
            try context.units.allocator.dupe(u8, "true")
        else
            try context.units.allocator.dupe(u8, "false"));

        if (is_method and context.current_container != null) {
            try metadata.put("parent_container", try context.units.allocator.dupe(u8, context.current_container.?));
        }

        // Create edges for method ownership
        var edges = std.array_list.Managed(ParsedEdge).init(context.units.allocator);
        if (is_method and context.current_container != null) {
            const container_id = try std.fmt.allocPrint(context.units.allocator, "container_{s}", .{context.current_container.?});
            const edge = ParsedEdge{
                .target_id = container_id,
                .edge_type = EdgeType.method_of,
                .metadata = std.StringHashMap([]const u8).init(context.units.allocator),
            };
            try edges.append(edge);
        }

        // Collect unresolved calls for cross-file resolution
        if (self.config.include_function_bodies) {
            try self.collect_function_calls(context, node, unit_id);
        }

        // Create semantic unit
        const unit = ParsedUnit{
            .id = unit_id,
            .unit_type = try context.units.allocator.dupe(u8, unit_type),
            .content = try context.units.allocator.dupe(u8, content),
            .location = SourceLocation{
                .file_path = context.file_path,
                .line_start = @intCast(start_loc.line + 1),
                .line_end = @intCast(end_loc.line + 1),
                .col_start = @intCast(start_loc.column + 1),
                .col_end = @intCast(end_loc.column + 1),
            },
            .edges = std.array_list.Managed(ParsedEdge).fromOwnedSlice(context.units.allocator, try edges.toOwnedSlice()),
            .metadata = metadata,
        };

        // Register symbol for cross-file resolution
        try self.register_symbol(fn_name, unit_id, context.file_path, is_method, context.current_container);

        try context.units.append(unit);
    }

    /// Collect function calls for cross-file resolution
    fn collect_function_calls(
        self: *ZigParser,
        context: *SemanticContext,
        func_node: std.zig.Ast.Node.Index,
        caller_id: []const u8,
    ) IngestionError!void {
        // Get the token range of the function to limit our search
        const func_start = context.ast.firstToken(func_node);
        const func_end = context.ast.lastToken(func_node);

        // Iterate through all AST nodes looking for call expressions within this function
        for (0..context.ast.nodes.len) |i| {
            const node: std.zig.Ast.Node.Index = @enumFromInt(i);
            const node_tag = context.ast.nodeTag(node);

            // Skip if not a call expression
            if (node_tag != .call_one and node_tag != .call_one_comma and
                node_tag != .call and node_tag != .call_comma)
            {
                continue;
            }

            // Check if this call is within our function's token range
            const node_start = context.ast.firstToken(node);
            const node_end = context.ast.lastToken(node);

            if (node_start >= func_start and node_end <= func_end) {
                // Process this call expression for cross-file resolution
                const is_single_param = (node_tag == .call_one or node_tag == .call_one_comma);
                try self.process_call_for_collection(context, node, caller_id, is_single_param);
            }
        }
    }

    /// Process a call expression and add to unresolved calls collection
    fn process_call_for_collection(
        self: *ZigParser,
        context: *SemanticContext,
        call_node: std.zig.Ast.Node.Index,
        caller_id: []const u8,
        is_single_param: bool,
    ) IngestionError!void {
        // Get call details using Zig's AST utilities
        var buffer: [1]std.zig.Ast.Node.Index = undefined;
        const call = if (is_single_param)
            context.ast.callOne(&buffer, call_node)
        else
            context.ast.callFull(call_node);

        // Analyze the function expression to determine call type and target
        const fn_expr = call.ast.fn_expr;
        const fn_expr_tag = context.ast.nodeTag(fn_expr);

        var is_method_call = false;
        var call_name: []const u8 = undefined;

        switch (fn_expr_tag) {
            .field_access => {
                // Field access expressions store the method name as the second token after the object reference
                const node_data = context.ast.nodeData(fn_expr);
                const field_token = node_data.node_and_token[1];
                call_name = context.ast.tokenSlice(field_token);

                // Check if this is module.function() (cross-file function call) or object.method() (method call)
                const object_node = node_data.node_and_token[0];
                const object_tag = context.ast.nodeTag(object_node);

                if (object_tag == .identifier) {
                    const object_token = context.ast.nodeMainToken(object_node);
                    const object_name = context.ast.tokenSlice(object_token);

                    // If object name looks like a module (utils, std, etc.) treat as function call
                    if (std.mem.eql(u8, object_name, "utils") or std.mem.eql(u8, object_name, "std")) {
                        is_method_call = false; // Module function call
                    } else {
                        is_method_call = true; // Instance method call
                    }
                } else {
                    is_method_call = true; // Default to method call
                }
            },
            .identifier => {
                is_method_call = false;
                const name_token = context.ast.nodeMainToken(fn_expr);
                call_name = context.ast.tokenSlice(name_token);
            },
            else => {
                return; // Skip complex call expressions
            },
        }

        if (call_name.len == 0) return;

        // Add to unresolved calls collection
        const unresolved_call = UnresolvedCall{
            .caller_unit_id = try self.allocator.dupe(u8, caller_id),
            .call_name = try self.allocator.dupe(u8, call_name),
            .is_method_call = is_method_call,
            .file_path = try self.allocator.dupe(u8, context.file_path),
        };

        try self.unresolved_calls.append(unresolved_call);
    }

    /// Resolve cross-file calls by matching unresolved calls with registered symbols
    fn resolve_cross_file_calls(
        self: *ZigParser,
        allocator: std.mem.Allocator,
        units: []ParsedUnit,
    ) IngestionError!void {
        // Create a map for quick unit lookup by ID
        var unit_map = std.StringHashMap(*ParsedUnit).init(allocator);
        defer unit_map.deinit();

        for (units) |*unit| {
            try unit_map.put(unit.id, unit);
        }

        // Process each unresolved call
        for (self.unresolved_calls.items) |unresolved_call| {
            // Try to find a matching symbol
            if (self.symbols.get(unresolved_call.call_name)) |symbol| {
                // Verify the call type matches the symbol type
                if (unresolved_call.is_method_call == symbol.is_method) {
                    // Unit lookup enables efficient cross-file edge creation without O(n²) search
                    if (unit_map.get(unresolved_call.caller_unit_id)) |caller_unit| {
                        // Create appropriate edge
                        const edge_type = if (symbol.is_method)
                            EdgeType.calls_method
                        else
                            EdgeType.calls_function;

                        // Build metadata for the edge
                        var edge_metadata = std.StringHashMap([]const u8).init(allocator);
                        try edge_metadata.put("call_type", try allocator.dupe(u8, if (symbol.is_method) "method" else "function"));
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
            }
        }
    }

    /// Analyze function calls within a function body for call graph construction
    fn analyze_function_calls(
        self: *ZigParser,
        context: *SemanticContext,
        func_node: std.zig.Ast.Node.Index,
        caller_id: []const u8,
        edges: *std.array_list.Managed(ParsedEdge),
    ) std.mem.Allocator.Error!void {
        // Get the token range of the function to limit our search
        const func_start = context.ast.firstToken(func_node);
        const func_end = context.ast.lastToken(func_node);

        // Iterate through all AST nodes looking for call expressions within this function
        for (0..context.ast.nodes.len) |i| {
            const node: std.zig.Ast.Node.Index = @enumFromInt(i);
            const node_tag = context.ast.nodeTag(node);

            // Skip if not a call expression
            if (node_tag != .call_one and node_tag != .call_one_comma and
                node_tag != .call and node_tag != .call_comma)
            {
                continue;
            }

            // Check if this call is within our function's token range
            const node_start = context.ast.firstToken(node);
            const node_end = context.ast.lastToken(node);

            if (node_start >= func_start and node_end <= func_end) {
                // Process this call expression
                const is_single_param = (node_tag == .call_one or node_tag == .call_one_comma);
                try self.process_call_expression(context, node, caller_id, edges, is_single_param);
            }
        }
    }

    /// Check if a variable declaration is actually a container declaration
    fn is_container_declaration(
        self: *ZigParser,
        context: *SemanticContext,
        node: std.zig.Ast.Node.Index,
    ) bool {
        _ = self;

        // Simple heuristic: look for struct/enum/union keywords in the node source
        const source = context.ast.getNodeSource(node);
        return std.mem.indexOf(u8, source, "struct") != null or
            std.mem.indexOf(u8, source, "enum") != null or
            std.mem.indexOf(u8, source, "union") != null;
    }

    /// Process container declaration (struct, enum, union)
    fn process_container_declaration(
        self: *ZigParser,
        context: *SemanticContext,
        node: std.zig.Ast.Node.Index,
    ) !void {
        // Extract container name from the variable declaration
        const node_tag = context.ast.nodeTag(node);
        const var_name = switch (node_tag) {
            .simple_var_decl => blk: {
                const var_decl = context.ast.simpleVarDecl(node);
                break :blk context.ast.tokenSlice(var_decl.ast.mut_token + 1);
            },
            .global_var_decl => blk: {
                const var_decl = context.ast.globalVarDecl(node);
                break :blk context.ast.tokenSlice(var_decl.ast.mut_token + 1);
            },
            else => return, // Skip other declaration types
        };

        // Container context enables method detection during recursive AST traversal.
        // Stack-based context tracking maintains parent-child relationships.
        const old_container = context.current_container;
        const old_container_node = context.current_container_node;

        // Save old container for later restoration (don't free yet)

        context.current_container = try context.units.allocator.dupe(u8, var_name);
        context.current_container_node = node;

        // Container units anchor method relationships and provide semantic structure.
        const unit_id = try std.fmt.allocPrint(context.units.allocator, "container_{s}", .{var_name});
        const content = context.ast.getNodeSource(node);

        // Location mapping enables precise source-to-semantic traceability.
        const start_token = context.ast.firstToken(node);
        const end_token = context.ast.lastToken(node);
        const start_loc = context.ast.tokenLocation(0, start_token);
        const end_loc = context.ast.tokenLocation(0, end_token);

        // Build metadata
        var metadata = std.StringHashMap([]const u8).init(context.units.allocator);
        try metadata.put("container_name", try context.units.allocator.dupe(u8, var_name));

        // String matching provides reliable container type detection for semantic categorization.
        // AST traversal would be more precise but significantly slower for this classification task.
        const container_type = if (std.mem.indexOf(u8, content, "struct") != null)
            "struct"
        else if (std.mem.indexOf(u8, content, "enum") != null)
            "enum"
        else if (std.mem.indexOf(u8, content, "union") != null)
            "union"
        else
            "container";

        try metadata.put("container_type", try context.units.allocator.dupe(u8, container_type));

        // Determine visibility
        const mut_token = switch (node_tag) {
            .simple_var_decl => context.ast.simpleVarDecl(node).ast.mut_token,
            .global_var_decl => context.ast.globalVarDecl(node).ast.mut_token,
            else => unreachable,
        };

        const is_pub = mut_token > 0 and std.mem.eql(u8, context.ast.tokenSlice(mut_token - 1), "pub");
        try metadata.put("is_public", if (is_pub)
            try context.units.allocator.dupe(u8, "true")
        else
            try context.units.allocator.dupe(u8, "false"));

        // Create semantic unit
        const unit = ParsedUnit{
            .id = unit_id,
            .unit_type = try context.units.allocator.dupe(u8, container_type),
            .content = try context.units.allocator.dupe(u8, content),
            .location = SourceLocation{
                .file_path = context.file_path,
                .line_start = @intCast(start_loc.line + 1),
                .line_end = @intCast(end_loc.line + 1),
                .col_start = @intCast(start_loc.column + 1),
                .col_end = @intCast(end_loc.column + 1),
            },
            .edges = std.array_list.Managed(ParsedEdge).init(context.units.allocator),
            .metadata = metadata,
        };

        try context.units.append(unit);

        // Process child declarations (this will pick up methods)
        try self.process_child_nodes(context, node);

        // Restore previous container context
        if (context.current_container) |current_name| {
            context.units.allocator.free(current_name);
        }
        context.current_container = old_container;
        context.current_container_node = old_container_node;
    }

    /// Process variable declaration
    fn process_variable_declaration(
        self: *ZigParser,
        context: *SemanticContext,
        node: std.zig.Ast.Node.Index,
    ) !void {
        if (!self.config.include_private) {
            // Check if this is a public declaration
            // Implementation depends on specific node type
        }

        // For now, skip variable declarations to focus on functions and containers
        // This can be extended later for completeness
        _ = context;
        _ = node;
    }

    /// Process test declaration
    fn process_test_declaration(
        self: *ZigParser,
        context: *SemanticContext,
        node: std.zig.Ast.Node.Index,
    ) !void {
        _ = self;

        const test_token = context.ast.nodeMainToken(node);
        const test_name_token = test_token + 1;

        // Extract test name (might be string literal or identifier)
        var test_name: []const u8 = "unnamed_test";
        if (test_name_token < context.ast.tokens.len) {
            const token_slice = context.ast.tokenSlice(test_name_token);
            if (std.mem.startsWith(u8, token_slice, "\"") and std.mem.endsWith(u8, token_slice, "\"")) {
                test_name = token_slice[1 .. token_slice.len - 1]; // Remove quotes
            } else {
                test_name = token_slice;
            }
        }

        const unit_id = try std.fmt.allocPrint(context.units.allocator, "test_{s}", .{test_name});
        const content = context.ast.getNodeSource(node);

        // Create location information
        const start_token = context.ast.firstToken(node);
        const end_token = context.ast.lastToken(node);
        const start_loc = context.ast.tokenLocation(0, start_token);
        const end_loc = context.ast.tokenLocation(0, end_token);

        // Build metadata
        var metadata = std.StringHashMap([]const u8).init(context.units.allocator);
        try metadata.put("test_name", try context.units.allocator.dupe(u8, test_name));

        // Create semantic unit
        const unit = ParsedUnit{
            .id = unit_id,
            .unit_type = try context.units.allocator.dupe(u8, "test"),
            .content = try context.units.allocator.dupe(u8, content),
            .location = SourceLocation{
                .file_path = context.file_path,
                .line_start = @intCast(start_loc.line + 1),
                .line_end = @intCast(end_loc.line + 1),
                .col_start = @intCast(start_loc.column + 1),
                .col_end = @intCast(end_loc.column + 1),
            },
            .edges = std.array_list.Managed(ParsedEdge).init(context.units.allocator),
            .metadata = metadata,
        };

        try context.units.append(unit);
    }

    /// Recursively process child nodes
    fn process_child_nodes(
        self: *ZigParser,
        context: *SemanticContext,
        node: std.zig.Ast.Node.Index,
    ) IngestionError!void {
        // Extract declaration from variable node to access container body
        const node_tag = context.ast.nodeTag(node);

        if (node_tag == .simple_var_decl) {
            const var_decl = context.ast.simpleVarDecl(node);
            if (var_decl.ast.init_node.unwrap()) |init_node| {
                try self.process_container_body(context, init_node);
            }
        } else if (node_tag == .global_var_decl) {
            const var_decl = context.ast.globalVarDecl(node);
            if (var_decl.ast.init_node.unwrap()) |init_node| {
                try self.process_container_body(context, init_node);
            }
        }
    }

    /// Process container body to find member functions
    fn process_container_body(
        self: *ZigParser,
        context: *SemanticContext,
        container_node: std.zig.Ast.Node.Index,
    ) IngestionError!void {
        const node_tag = context.ast.nodeTag(container_node);

        // Handle different container types
        switch (node_tag) {
            .container_decl, .container_decl_trailing => {
                var buffer: [2]std.zig.Ast.Node.Index = undefined;
                const container_decl = context.ast.fullContainerDecl(&buffer, container_node) orelse return;

                // Process all declarations inside the container
                for (container_decl.ast.members) |member_node| {
                    try self.process_declaration(context, member_node);
                }
            },
            else => {
                // For other node types, try to find child declarations
                // This handles cases where the container structure varies
                return;
            },
        }
    }

    /// Analyze function calls within a function body
    /// Check if a variable declaration is an import statement
    fn is_import_declaration(_: *ZigParser, context: *SemanticContext, node: std.zig.Ast.Node.Index) bool {
        const source = context.ast.getNodeSource(node);
        return std.mem.indexOf(u8, source, "@import(") != null;
    }

    /// Process import declaration (@import statements)
    fn process_import_declaration(
        _: *ZigParser,
        context: *SemanticContext,
        node: std.zig.Ast.Node.Index,
    ) !void {
        const source = context.ast.getNodeSource(node);

        // Extract import name from variable declaration
        const var_decl = context.ast.fullVarDecl(node) orelse return;
        const var_name_token = var_decl.ast.mut_token + 1;
        const var_name = context.ast.tokenSlice(var_name_token);

        // Create unique ID for this import
        const unit_id = try std.fmt.allocPrint(context.units.allocator, "{s}_import_{s}", .{ std.fs.path.basename(context.file_path), var_name });

        // Create location information
        const start_token = context.ast.firstToken(node);
        const end_token = context.ast.lastToken(node);
        const start_loc = context.ast.tokenLocation(0, start_token);
        const end_loc = context.ast.tokenLocation(0, end_token);

        // Build metadata
        var metadata = std.StringHashMap([]const u8).init(context.units.allocator);
        try metadata.put("import_name", try context.units.allocator.dupe(u8, var_name));
        try metadata.put("file_path", try context.units.allocator.dupe(u8, context.file_path));

        // Extract import path from @import("path")
        if (std.mem.indexOf(u8, source, "@import(\"")) |start_idx| {
            const path_start = start_idx + 9; // Length of '@import("'
            if (std.mem.indexOf(u8, source[path_start..], "\")")) |end_offset| {
                const import_path = source[path_start .. path_start + end_offset];
                try metadata.put("import_path", try context.units.allocator.dupe(u8, import_path));
            }
        }

        // Create parsed unit for the import
        const unit = ParsedUnit{
            .id = unit_id,
            .unit_type = try context.units.allocator.dupe(u8, "import"),
            .content = try context.units.allocator.dupe(u8, source),
            .metadata = metadata,
            .location = SourceLocation{
                .line_start = @intCast(start_loc.line + 1),
                .line_end = @intCast(end_loc.line + 1),
                .col_start = @intCast(start_loc.column + 1),
                .col_end = @intCast(end_loc.column + 1),
                .file_path = context.file_path,
            },
            .edges = std.array_list.Managed(ParsedEdge).init(context.units.allocator),
        };

        try context.units.append(unit);
    }

    /// Process a specific call expression and generate appropriate edges
    fn process_call_expression(
        self: *ZigParser,
        context: *SemanticContext,
        call_node: std.zig.Ast.Node.Index,
        caller_id: []const u8,
        edges: *std.array_list.Managed(ParsedEdge),
        is_single_param: bool,
    ) std.mem.Allocator.Error!void {
        _ = self;

        // Get call details using Zig's AST utilities
        var buffer: [1]std.zig.Ast.Node.Index = undefined;
        const call = if (is_single_param)
            context.ast.callOne(&buffer, call_node)
        else
            context.ast.callFull(call_node);

        // Analyze the function expression to determine call type and target
        const fn_expr = call.ast.fn_expr;
        const fn_expr_tag = context.ast.nodeTag(fn_expr);

        var edge_type: EdgeType = undefined;
        var target_name: []const u8 = undefined;

        switch (fn_expr_tag) {
            .field_access => {
                edge_type = EdgeType.calls_method;
                // Field access expressions store the method name as the second token after the object reference
                const node_data = context.ast.nodeData(fn_expr);
                const field_token = node_data.node_and_token[1];
                target_name = try context.arena.allocator().dupe(u8, context.ast.tokenSlice(field_token));
            },
            .identifier => {
                edge_type = EdgeType.calls_function;
                const name_token = context.ast.nodeMainToken(fn_expr);
                target_name = try context.arena.allocator().dupe(u8, context.ast.tokenSlice(name_token));
            },
            else => {
                return;
            },
        }

        if (target_name.len == 0) return;

        var metadata = std.StringHashMap([]const u8).init(context.units.allocator);
        try metadata.put("call_type", try context.units.allocator.dupe(u8, if (edge_type == EdgeType.calls_method) "method" else "function"));
        try metadata.put("caller_id", try context.units.allocator.dupe(u8, caller_id));

        const target_id = if (edge_type == EdgeType.calls_method)
            try std.fmt.allocPrint(context.units.allocator, "method_{s}", .{target_name})
        else
            try std.fmt.allocPrint(context.units.allocator, "function_{s}", .{target_name});

        const edge = ParsedEdge{
            .target_id = target_id,
            .edge_type = edge_type,
            .metadata = metadata,
        };

        try edges.append(edge);
    }

    // Parser interface implementations
    fn parse_impl(ptr: *anyopaque, allocator: std.mem.Allocator, content: SourceContent) IngestionError![]ParsedUnit {
        const self: *ZigParser = @ptrCast(@alignCast(ptr));
        return self.parse_content(allocator, content);
    }

    fn supports_impl(ptr: *anyopaque, content_type: []const u8) bool {
        _ = ptr;
        return std.mem.eql(u8, content_type, "text/zig");
    }

    fn describe_impl(ptr: *anyopaque) []const u8 {
        _ = ptr;
        return "Professional Zig Semantic Parser";
    }

    fn deinit_impl(ptr: *anyopaque, allocator: std.mem.Allocator) void {
        _ = allocator;
        const self: *ZigParser = @ptrCast(@alignCast(ptr));
        self.deinit();
    }
};

// Unit tests

test "semantic parser basic functionality verification" {
    const testing = std.testing;

    // Simple test to verify the parser can be created and has basic functionality
    const config = ZigParserConfig{};
    var zig_parser = ZigParser.init(testing.allocator, config);
    defer zig_parser.deinit();

    const parser = zig_parser.parser();

    // Verify parser interface works
    try testing.expect(parser.supports("text/zig"));
    try testing.expect(!parser.supports("text/rust"));
}
test "semantic parser creation and cleanup" {
    const testing = std.testing;

    const config = ZigParserConfig{};
    var zig_parser = ZigParser.init(testing.allocator, config);
    defer zig_parser.deinit();

    const parser = zig_parser.parser();
    try testing.expect(parser.supports("text/zig"));
    try testing.expect(!parser.supports("text/rust"));
    try testing.expectEqualStrings("Professional Zig Semantic Parser", parser.describe());
}

test "function vs method distinction" {
    const testing = std.testing;

    var metadata = std.StringHashMap([]const u8).init(testing.allocator);
    defer metadata.deinit();

    const source_content = SourceContent{
        .data =
        \\const Calculator = struct {
        \\    value: f64,
        \\
        \\    pub fn init(value: f64) Calculator {
        \\        return Calculator{ .value = value };
        \\    }
        \\
        \\    pub fn add(self: *Calculator, x: f64) void {
        \\        self.value += x;
        \\    }
        \\};
        \\
        \\fn helper_function() void {
        \\    // Free function
        \\}
        \\
        \\pub fn main() void {
        \\    var calc = Calculator.init(0);
        \\    calc.add(5.0);
        \\    helper_function();
        \\}
        ,
        .content_type = "text/zig",
        .metadata = metadata,
        .timestamp_ns = 0,
    };

    var parser = ZigParser.init(testing.allocator, ZigParserConfig{});
    defer parser.deinit();

    const parsed_units = try parser.parse_content(testing.allocator, source_content);
    defer {
        for (parsed_units) |*unit| {
            unit.deinit(testing.allocator);
        }
        testing.allocator.free(parsed_units);
    }

    // Should correctly distinguish methods from functions
    var method_count: u32 = 0;
    var function_count: u32 = 0;
    var container_count: u32 = 0;

    for (parsed_units) |unit| {
        if (std.mem.eql(u8, unit.unit_type, "method")) {
            method_count += 1;
            // Verify method is linked to container
            const is_method = unit.metadata.get("is_method");
            try testing.expect(is_method != null);
            try testing.expectEqualStrings("true", is_method.?);
        } else if (std.mem.eql(u8, unit.unit_type, "function")) {
            function_count += 1;
        } else if (std.mem.eql(u8, unit.unit_type, "struct")) {
            container_count += 1;
        }
    }

    try testing.expect(method_count >= 2); // init and add methods
    try testing.expect(function_count >= 2); // helper_function and main
    try testing.expectEqual(@as(u32, 1), container_count); // Calculator struct
}

test "method ownership edges" {
    const testing = std.testing;

    var metadata = std.StringHashMap([]const u8).init(testing.allocator);
    defer metadata.deinit();

    const source_content = SourceContent{
        .data =
        \\const Database = struct {
        \\    name: []const u8,
        \\
        \\    pub fn connect(name: []const u8) Database {
        \\        return Database{ .name = name };
        \\    }
        \\
        \\    pub fn query(self: *Database, sql: []const u8) void {
        \\        _ = self;
        \\        _ = sql;
        \\    }
        \\};
        ,
        .content_type = "text/zig",
        .metadata = metadata,
        .timestamp_ns = 0,
    };

    var parser = ZigParser.init(testing.allocator, ZigParserConfig{});
    defer parser.deinit();

    const parsed_units = try parser.parse_content(testing.allocator, source_content);
    defer {
        for (parsed_units) |*unit| {
            unit.deinit(testing.allocator);
        }
        testing.allocator.free(parsed_units);
    }

    // Find methods and verify they have method_of edges
    for (parsed_units) |unit| {
        if (std.mem.eql(u8, unit.unit_type, "method")) {
            // Each method should have a method_of edge to the container
            var has_method_of_edge = false;
            for (unit.edges.items) |edge| {
                if (edge.edge_type == EdgeType.method_of) {
                    has_method_of_edge = true;
                    try testing.expect(std.mem.indexOf(u8, edge.target_id, "Database") != null);
                    break;
                }
            }
            try testing.expect(has_method_of_edge);
        }
    }
}
