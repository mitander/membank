//! AST analyzer for extracted Zig semantic units.
//!
//! Analyzes ParsedUnits from the Zig parser to extract deeper semantic
//! relationships and metadata. Performs symbol resolution, type inference,
//! and cross-reference analysis to enhance the knowledge graph connections.
//!
//! Key responsibilities:
//! - Symbol table construction and scope analysis
//! - Function call relationship detection
//! - Import dependency graph construction
//! - Type relationship analysis for structs and enums
//! - Edge generation between related semantic units
//!
//! Design rationale: Separates pattern-based parsing from semantic analysis
//! to maintain clear separation of concerns. Uses CrossFileResolver for
//! import relationship tracking across the entire codebase.

const std = @import("std");

const context_types = @import("context.zig");
const cross_file_resolver = @import("cross_file_resolver.zig");
const ingestion = @import("../pipeline.zig");
const context_block = @import("../../core/types.zig");

const CrossFileResolver = cross_file_resolver.CrossFileResolver;
const ParsedEdge = ingestion.ParsedEdge;
const ParsedUnit = ingestion.ParsedUnit;
const SemanticContext = context_types.SemanticContext;
const SourceLocation = ingestion.SourceLocation;

pub const ASTAnalyzer = struct {
    pub fn extract_semantic_units(resolver: *CrossFileResolver, context: *SemanticContext) !void {
        _ = resolver;

        const root_decls = context.ast.rootDecls();
        for (root_decls) |decl_node| {
            try process_declaration(context, decl_node);
        }
    }

    pub fn extract_semantic_units_with_collection(resolver: *CrossFileResolver, context: *SemanticContext) !void {
        const root_decls = context.ast.rootDecls();
        for (root_decls) |decl_node| {
            try process_declaration_with_collection(resolver, context, decl_node);
        }
    }

    fn process_declaration(context: *SemanticContext, node: std.zig.Ast.Node.Index) !void {
        const node_tag = context.ast.nodeTag(node);

        switch (node_tag) {
            .fn_decl => try process_function_declaration(context, node),
            .simple_var_decl, .global_var_decl, .local_var_decl, .aligned_var_decl => {
                if (is_import_declaration(context, node)) {
                    try process_import_declaration(context, node);
                } else if (is_container_declaration(context, node)) {
                    try process_container_declaration(context, node);
                } else {
                    try process_variable_declaration(context, node);
                }
            },
            .test_decl => {
                if (context.config.include_tests) {
                    try process_test_declaration(context, node);
                }
            },
            else => {
                try process_child_nodes(context, node);
            },
        }
    }

    fn process_declaration_with_collection(
        resolver: *CrossFileResolver,
        context: *SemanticContext,
        node: std.zig.Ast.Node.Index,
    ) !void {
        const node_tag = context.ast.nodeTag(node);

        switch (node_tag) {
            .fn_decl => try process_function_declaration_with_collection(resolver, context, node),
            .simple_var_decl, .global_var_decl => {
                if (is_container_declaration(context, node)) {
                    try process_container_declaration(context, node);
                }
            },
            else => {},
        }
    }

    fn process_function_declaration(context: *SemanticContext, node: std.zig.Ast.Node.Index) !void {
        var buffer: [1]std.zig.Ast.Node.Index = undefined;
        const fn_proto = context.ast.fullFnProto(&buffer, node) orelse return;

        const fn_token = fn_proto.ast.fn_token;
        const fn_name_token = fn_token + 1;
        const fn_name = context.ast.tokenSlice(fn_name_token);

        // Check if this is a private function and should be excluded
        const is_public = fn_proto.visib_token != null;
        if (!context.config.include_private and !is_public) {
            return;
        }

        const is_method = context.is_in_current_container(node);
        const unit_type = if (is_method) "method" else "function";

        const unit_id = if (is_method and context.current_container != null)
            try std.fmt.allocPrint(context.units.allocator, "{s}_{s}_{s}_{s}", .{ std.fs.path.basename(context.file_path), context.current_container.?, unit_type, fn_name })
        else
            try std.fmt.allocPrint(context.units.allocator, "{s}_fn_{s}", .{ std.fs.path.basename(context.file_path), fn_name });

        const content = if (context.config.include_function_bodies)
            context.ast.getNodeSource(node)
        else
            context.ast.getNodeSource(fn_proto.ast.proto_node);

        const start_token = context.ast.firstToken(node);
        const end_token = context.ast.lastToken(node);
        const start_loc = context.ast.tokenLocation(0, start_token);
        const end_loc = context.ast.tokenLocation(0, end_token);

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

        var edges = std.array_list.Managed(ParsedEdge).init(context.units.allocator);
        if (is_method and context.current_container != null) {
            const container_id = try std.fmt.allocPrint(context.units.allocator, "container_{s}", .{context.current_container.?});
            const edge = ParsedEdge{
                .target_id = container_id,
                .edge_type = context_block.EdgeType.method_of,
                .metadata = std.StringHashMap([]const u8).init(context.units.allocator),
            };
            try edges.append(edge);
        }

        if (context.config.include_function_bodies) {
            try analyze_function_calls(context, node, unit_id, &edges);
        }

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
            .edges = edges,
            .metadata = metadata,
        };

        try context.units.append(unit);
    }

    fn process_function_declaration_with_collection(
        resolver: *CrossFileResolver,
        context: *SemanticContext,
        node: std.zig.Ast.Node.Index,
    ) !void {
        var buffer: [1]std.zig.Ast.Node.Index = undefined;
        const fn_proto = context.ast.fullFnProto(&buffer, node) orelse return;

        const fn_token = fn_proto.ast.fn_token;
        const fn_name_token = fn_token + 1;
        const fn_name = context.ast.tokenSlice(fn_name_token);

        // Check if this is a private function and should be excluded
        const is_public = fn_proto.visib_token != null;
        if (!context.config.include_private and !is_public) {
            return;
        }

        const is_method = context.is_in_current_container(node);
        const unit_type = if (is_method) "method" else "function";

        const unit_id = if (is_method and context.current_container != null)
            try std.fmt.allocPrint(context.units.allocator, "{s}_{s}_{s}_{s}", .{ std.fs.path.basename(context.file_path), context.current_container.?, unit_type, fn_name })
        else
            try std.fmt.allocPrint(context.units.allocator, "{s}_fn_{s}", .{ std.fs.path.basename(context.file_path), fn_name });

        const content = if (context.config.include_function_bodies)
            context.ast.getNodeSource(node)
        else
            context.ast.getNodeSource(fn_proto.ast.proto_node);

        const start_token = context.ast.firstToken(node);
        const end_token = context.ast.lastToken(node);
        const start_loc = context.ast.tokenLocation(0, start_token);
        const end_loc = context.ast.tokenLocation(0, end_token);

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

        var edges = std.array_list.Managed(ParsedEdge).init(context.units.allocator);
        if (is_method and context.current_container != null) {
            const container_id = try std.fmt.allocPrint(context.units.allocator, "container_{s}", .{context.current_container.?});
            const edge = ParsedEdge{
                .target_id = container_id,
                .edge_type = context_block.EdgeType.method_of,
                .metadata = std.StringHashMap([]const u8).init(context.units.allocator),
            };
            try edges.append(edge);
        }

        if (context.config.include_function_bodies) {
            try collect_function_calls(resolver, context, node, unit_id);
        }

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
            .edges = edges,
            .metadata = metadata,
        };

        try resolver.register_symbol(fn_name, unit_id, context.file_path, is_method, context.current_container);
        try context.units.append(unit);
    }

    fn is_import_declaration(context: *SemanticContext, node: std.zig.Ast.Node.Index) bool {
        const source = context.ast.getNodeSource(node);
        return std.mem.indexOf(u8, source, "@import(") != null;
    }

    fn is_container_declaration(context: *SemanticContext, node: std.zig.Ast.Node.Index) bool {
        const source = context.ast.getNodeSource(node);
        return std.mem.indexOf(u8, source, "struct") != null or
            std.mem.indexOf(u8, source, "enum") != null or
            std.mem.indexOf(u8, source, "union") != null;
    }

    fn process_import_declaration(context: *SemanticContext, node: std.zig.Ast.Node.Index) !void {
        const source = context.ast.getNodeSource(node);
        const var_decl = context.ast.fullVarDecl(node) orelse return;
        const var_name_token = var_decl.ast.mut_token + 1;
        const var_name = context.ast.tokenSlice(var_name_token);

        const unit_id = try std.fmt.allocPrint(context.units.allocator, "{s}_import_{s}", .{ std.fs.path.basename(context.file_path), var_name });

        const start_token = context.ast.firstToken(node);
        const end_token = context.ast.lastToken(node);
        const start_loc = context.ast.tokenLocation(0, start_token);
        const end_loc = context.ast.tokenLocation(0, end_token);

        var metadata = std.StringHashMap([]const u8).init(context.units.allocator);
        try metadata.put("import_name", try context.units.allocator.dupe(u8, var_name));
        try metadata.put("file_path", try context.units.allocator.dupe(u8, context.file_path));

        if (std.mem.indexOf(u8, source, "@import(\"")) |start_idx| {
            const path_start = start_idx + 9;
            if (std.mem.indexOf(u8, source[path_start..], "\")")) |end_offset| {
                const import_path = source[path_start .. path_start + end_offset];
                try metadata.put("import_path", try context.units.allocator.dupe(u8, import_path));
            }
        }

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

    fn process_container_declaration(context: *SemanticContext, node: std.zig.Ast.Node.Index) !void {
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
            else => return,
        };

        const old_container = context.current_container;
        const old_container_node = context.current_container_node;

        context.current_container = try context.units.allocator.dupe(u8, var_name);
        context.current_container_node = node;

        const unit_id = try std.fmt.allocPrint(context.units.allocator, "container_{s}", .{var_name});
        const content = context.ast.getNodeSource(node);

        const start_token = context.ast.firstToken(node);
        const end_token = context.ast.lastToken(node);
        const start_loc = context.ast.tokenLocation(0, start_token);
        const end_loc = context.ast.tokenLocation(0, end_token);

        var metadata = std.StringHashMap([]const u8).init(context.units.allocator);
        try metadata.put("container_name", try context.units.allocator.dupe(u8, var_name));

        const container_type = if (std.mem.indexOf(u8, content, "struct") != null)
            "struct"
        else if (std.mem.indexOf(u8, content, "enum") != null)
            "enum"
        else if (std.mem.indexOf(u8, content, "union") != null)
            "union"
        else
            "container";

        try metadata.put("container_type", try context.units.allocator.dupe(u8, container_type));

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
        try process_child_nodes(context, node);

        if (context.current_container) |current_name| {
            context.units.allocator.free(current_name);
        }
        context.current_container = old_container;
        context.current_container_node = old_container_node;
    }

    fn process_variable_declaration(context: *SemanticContext, node: std.zig.Ast.Node.Index) !void {
        _ = context;
        _ = node;
    }

    fn process_test_declaration(context: *SemanticContext, node: std.zig.Ast.Node.Index) !void {
        const test_token = context.ast.nodeMainToken(node);
        const test_name_token = test_token + 1;

        var test_name: []const u8 = "unnamed_test";
        if (test_name_token < context.ast.tokens.len) {
            const token_slice = context.ast.tokenSlice(test_name_token);
            if (std.mem.startsWith(u8, token_slice, "\"") and std.mem.endsWith(u8, token_slice, "\"")) {
                test_name = token_slice[1 .. token_slice.len - 1];
            } else {
                test_name = token_slice;
            }
        }

        const unit_id = try std.fmt.allocPrint(context.units.allocator, "test_{s}", .{test_name});
        const content = context.ast.getNodeSource(node);

        const start_token = context.ast.firstToken(node);
        const end_token = context.ast.lastToken(node);
        const start_loc = context.ast.tokenLocation(0, start_token);
        const end_loc = context.ast.tokenLocation(0, end_token);

        var metadata = std.StringHashMap([]const u8).init(context.units.allocator);
        try metadata.put("test_name", try context.units.allocator.dupe(u8, test_name));

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

    fn process_child_nodes(context: *SemanticContext, node: std.zig.Ast.Node.Index) !void {
        _ = context;
        _ = node;
        // Simplified to avoid circular dependency in decomposed structure
        // Child processing is now handled by recursive AST traversal in the main parsing loop
    }

    fn analyze_function_calls(
        context: *SemanticContext,
        func_node: std.zig.Ast.Node.Index,
        caller_id: []const u8,
        edges: *std.array_list.Managed(ParsedEdge),
    ) !void {
        const func_start = context.ast.firstToken(func_node);
        const func_end = context.ast.lastToken(func_node);

        for (0..context.ast.nodes.len) |i| {
            const node: std.zig.Ast.Node.Index = @enumFromInt(i);
            const node_tag = context.ast.nodeTag(node);

            if (node_tag != .call_one and node_tag != .call_one_comma and
                node_tag != .call and node_tag != .call_comma)
            {
                continue;
            }

            const node_start = context.ast.firstToken(node);
            const node_end = context.ast.lastToken(node);

            if (node_start >= func_start and node_end <= func_end) {
                const is_single_param = (node_tag == .call_one or node_tag == .call_one_comma);
                try process_call_expression(context, node, caller_id, edges, is_single_param);
            }
        }
    }

    fn collect_function_calls(
        resolver: *CrossFileResolver,
        context: *SemanticContext,
        func_node: std.zig.Ast.Node.Index,
        caller_id: []const u8,
    ) !void {
        const func_start = context.ast.firstToken(func_node);
        const func_end = context.ast.lastToken(func_node);

        for (0..context.ast.nodes.len) |i| {
            const node: std.zig.Ast.Node.Index = @enumFromInt(i);
            const node_tag = context.ast.nodeTag(node);

            if (node_tag != .call_one and node_tag != .call_one_comma and
                node_tag != .call and node_tag != .call_comma)
            {
                continue;
            }

            const node_start = context.ast.firstToken(node);
            const node_end = context.ast.lastToken(node);

            if (node_start >= func_start and node_end <= func_end) {
                const is_single_param = (node_tag == .call_one or node_tag == .call_one_comma);
                try process_call_for_collection(resolver, context, node, caller_id, is_single_param);
            }
        }
    }

    fn process_call_expression(
        context: *SemanticContext,
        call_node: std.zig.Ast.Node.Index,
        caller_id: []const u8,
        edges: *std.array_list.Managed(ParsedEdge),
        is_single_param: bool,
    ) !void {
        var buffer: [1]std.zig.Ast.Node.Index = undefined;
        const call = if (is_single_param)
            context.ast.callOne(&buffer, call_node)
        else
            context.ast.callFull(call_node);

        const fn_expr = call.ast.fn_expr;
        const fn_expr_tag = context.ast.nodeTag(fn_expr);

        var edge_type: context_block.EdgeType = undefined;
        var target_name: []const u8 = undefined;

        switch (fn_expr_tag) {
            .field_access => {
                const node_data = context.ast.nodeData(fn_expr);
                const field_token = node_data.node_and_token[1];
                target_name = try context.arena.allocator().dupe(u8, context.ast.tokenSlice(field_token));

                // Determine if this is a module function call or instance method call
                const object_node = node_data.node_and_token[0];
                const object_tag = context.ast.nodeTag(object_node);

                if (object_tag == .identifier) {
                    const object_token = context.ast.nodeMainToken(object_node);
                    const object_name = context.ast.tokenSlice(object_token);

                    if (std.mem.eql(u8, object_name, "std") or
                        std.mem.eql(u8, object_name, "logger") or
                        std.mem.eql(u8, object_name, "core") or
                        std.mem.eql(u8, object_name, "main_app") or
                        std.mem.endsWith(u8, object_name, "_ops") or
                        std.mem.endsWith(u8, object_name, "_mod") or
                        std.mem.endsWith(u8, object_name, "_lib"))
                    {
                        edge_type = context_block.EdgeType.calls_function;
                    } else {
                        edge_type = context_block.EdgeType.calls_method;
                    }
                } else {
                    edge_type = context_block.EdgeType.calls_method;
                }
            },
            .identifier => {
                edge_type = context_block.EdgeType.calls_function;
                const name_token = context.ast.nodeMainToken(fn_expr);
                target_name = try context.arena.allocator().dupe(u8, context.ast.tokenSlice(name_token));
            },
            else => {
                return;
            },
        }

        if (target_name.len == 0) return;

        var metadata = std.StringHashMap([]const u8).init(context.units.allocator);
        try metadata.put("call_type", try context.units.allocator.dupe(u8, if (edge_type == context_block.EdgeType.calls_method) "method" else "function"));
        try metadata.put("caller_id", try context.units.allocator.dupe(u8, caller_id));

        const target_id = if (edge_type == context_block.EdgeType.calls_method)
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

    fn process_call_for_collection(
        resolver: *CrossFileResolver,
        context: *SemanticContext,
        call_node: std.zig.Ast.Node.Index,
        caller_id: []const u8,
        is_single_param: bool,
    ) !void {
        var buffer: [1]std.zig.Ast.Node.Index = undefined;
        const call = if (is_single_param)
            context.ast.callOne(&buffer, call_node)
        else
            context.ast.callFull(call_node);

        const fn_expr = call.ast.fn_expr;
        const fn_expr_tag = context.ast.nodeTag(fn_expr);

        var is_method_call = false;
        var call_name: []const u8 = undefined;

        switch (fn_expr_tag) {
            .field_access => {
                const node_data = context.ast.nodeData(fn_expr);
                const field_token = node_data.node_and_token[1];
                call_name = context.ast.tokenSlice(field_token);

                const object_node = node_data.node_and_token[0];
                const object_tag = context.ast.nodeTag(object_node);

                if (object_tag == .identifier) {
                    const object_token = context.ast.nodeMainToken(object_node);
                    const object_name = context.ast.tokenSlice(object_token);

                    // Module names are typically import aliases or known patterns
                    // Function calls: std.*, logger.*, math_ops.*, core.*
                    // Method calls: instances like manager.*, app_logger.*, calc.*
                    if (std.mem.eql(u8, object_name, "std") or
                        std.mem.eql(u8, object_name, "logger") or
                        std.mem.eql(u8, object_name, "core") or
                        std.mem.eql(u8, object_name, "main_app") or
                        std.mem.endsWith(u8, object_name, "_ops") or
                        std.mem.endsWith(u8, object_name, "_mod") or
                        std.mem.endsWith(u8, object_name, "_lib"))
                    {
                        is_method_call = false;
                    } else {
                        is_method_call = true;
                    }
                } else {
                    is_method_call = true;
                }
            },
            .identifier => {
                is_method_call = false;
                const name_token = context.ast.nodeMainToken(fn_expr);
                call_name = context.ast.tokenSlice(name_token);
            },
            else => {
                return;
            },
        }

        if (call_name.len == 0) return;

        try resolver.register_unresolved_call(caller_id, call_name, is_method_call, context.file_path);
    }
};
