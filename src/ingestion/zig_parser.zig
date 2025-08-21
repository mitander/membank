//! Zig source code parser for semantic structure extraction.
//!
//! Extracts functions, structs, enums, constants, and their relationships
//! from Zig source files using tokenization-based parsing. Produces
//! structured context blocks suitable for LLM knowledge ingestion.
//!
//! Design rationale: Simple tokenization avoids full AST complexity while
//! capturing semantic structure needed for code understanding. Arena allocation
//! enables efficient batch processing of large codebases without manual
//! memory management overhead.

const std = @import("std");

const assert_mod = @import("../core/assert.zig");
const concurrency = @import("../core/concurrency.zig");
const context_block = @import("../core/types.zig");
const error_context = @import("../core/error_context.zig");
const ingestion = @import("pipeline.zig");

const EdgeType = context_block.EdgeType;
const IngestionError = ingestion.IngestionError;
const ParsedEdge = ingestion.ParsedEdge;
const ParsedUnit = ingestion.ParsedUnit;
const Parser = ingestion.Parser;
const SourceContent = ingestion.SourceContent;
const SourceLocation = ingestion.SourceLocation;

/// Types of semantic units we can extract from Zig code
pub const ZigUnitType = enum {
    function,
    struct_def,
    enum_def,
    constant,
    variable,
    import,
    comment_block,
    test_block,
    type_alias,

    /// Convert a ZigUnitType enum to its string representation
    ///
    /// Returns the canonical string name for each unit type, used for
    /// metadata and debugging output in the ingestion pipeline.
    pub fn to_string(self: ZigUnitType) []const u8 {
        return switch (self) {
            .function => "function",
            .struct_def => "struct",
            .enum_def => "enum",
            .constant => "constant",
            .variable => "variable",
            .import => "import",
            .comment_block => "comment",
            .test_block => "test",
            .type_alias => "type_alias",
        };
    }
};

/// Configuration for Zig parser
pub const ZigParserConfig = struct {
    /// Whether to extract function bodies or just signatures
    include_function_bodies: bool = true,
    /// Whether to extract private (non-pub) definitions
    include_private: bool = true,
    /// Whether to extract inline comments
    include_inline_comments: bool = false,
    /// Whether to extract test blocks
    include_tests: bool = true,
    /// Maximum size of extracted unit content
    max_unit_size: usize = 64 * 1024, // 64KB
};

/// Parsing context to track state
const ParseContext = struct {
    /// Source file content
    source: []const u8,
    /// Current line number (1-based)
    current_line: u32 = 1,
    /// Current position in source
    position: usize = 0,
    /// File path for location tracking
    file_path: []const u8,
    /// Allocator for temporary allocations
    allocator: std.mem.Allocator,
    /// Parsed units accumulator
    units: std.array_list.Managed(ParsedUnit),
    /// Configuration
    config: ZigParserConfig,

    fn is_at_end(self: *const ParseContext) bool {
        return self.position >= self.source.len;
    }

    fn current_char(self: *const ParseContext) ?u8 {
        if (self.is_at_end()) return null;
        return self.source[self.position];
    }

    fn advance(self: *ParseContext) ?u8 {
        if (self.is_at_end()) return null;
        const char = self.source[self.position];
        self.position += 1;
        if (char == '\n') {
            self.current_line += 1;
        }
        return char;
    }

    fn skip_whitespace(self: *ParseContext) void {
        while (self.current_char()) |char| {
            if (char == ' ' or char == '\t' or char == '\r') {
                _ = self.advance();
            } else {
                break;
            }
        }
    }

    fn read_line(self: *ParseContext) []const u8 {
        const start = self.position;
        while (self.current_char()) |char| {
            if (char == '\n') {
                const line = self.source[start..self.position];
                _ = self.advance(); // consume newline
                return line;
            }
            _ = self.advance();
        }
        return self.source[start..self.position];
    }

    fn peek_line(self: *const ParseContext) []const u8 {
        const start = self.position;
        var pos = self.position;
        while (pos < self.source.len) {
            if (self.source[pos] == '\n') {
                return self.source[start..pos];
            }
            pos += 1;
        }
        return self.source[start..];
    }
};

/// Zig source code parser
pub const ZigParser = struct {
    /// Parser configuration
    config: ZigParserConfig,
    /// Arena for all allocations
    arena: std.heap.ArenaAllocator,

    /// Initialize Zig parser with configuration
    pub fn init(allocator: std.mem.Allocator, config: ZigParserConfig) ZigParser {
        return ZigParser{
            .config = config,
            .arena = std.heap.ArenaAllocator.init(allocator),
        };
    }

    /// Clean up parser resources
    pub fn deinit(self: *ZigParser) void {
        self.arena.deinit();
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

    /// Parse Zig source content into semantic units
    fn parse_content(
        self: *ZigParser,
        allocator: std.mem.Allocator,
        content: SourceContent,
    ) IngestionError![]ParsedUnit {
        concurrency.assert_main_thread();

        const file_path = content.metadata.get("file_path") orelse "unknown.zig";

        var context = ParseContext{
            .source = content.data,
            .file_path = file_path,
            .allocator = allocator,
            .units = std.array_list.Managed(ParsedUnit).init(allocator),
            .config = self.config,
        };

        try self.parse_source(&context);
        return context.units.toOwnedSlice();
    }

    /// Main parsing loop
    fn parse_source(self: *ZigParser, context: *ParseContext) !void {
        while (!context.is_at_end()) {
            context.skip_whitespace();

            if (context.is_at_end()) break;

            const line = context.peek_line();
            const trimmed = std.mem.trim(u8, line, " \t");

            if (trimmed.len == 0) {
                _ = context.advance();
                continue;
            }

            if (std.mem.startsWith(u8, trimmed, "//")) {
                try self.parse_comment_block(context);
            } else if (std.mem.startsWith(u8, trimmed, "const ") and std.mem.indexOf(u8, trimmed, "@import") != null) {
                try self.parse_import(context);
            } else if (std.mem.startsWith(u8, trimmed, "pub fn ") or std.mem.startsWith(u8, trimmed, "fn ")) {
                try self.parse_function(context);
            } else if (contains_keyword(trimmed, "struct") or contains_keyword(trimmed, "packed struct") or contains_keyword(trimmed, "extern struct")) {
                try self.parse_struct(context);
            } else if (std.mem.startsWith(u8, trimmed, "pub const ") or std.mem.startsWith(u8, trimmed, "const ")) {
                try self.parse_constant(context);
            } else if (std.mem.startsWith(u8, trimmed, "pub var ") or std.mem.startsWith(u8, trimmed, "var ")) {
                try self.parse_variable(context);
            } else if (contains_keyword(trimmed, "enum")) {
                try self.parse_enum(context);
            } else if (std.mem.startsWith(u8, trimmed, "test ")) {
                if (self.config.include_tests) {
                    try self.parse_test(context);
                } else {
                    _ = context.read_line();
                }
            } else {
                _ = context.read_line();
            }
        }
    }

    /// Parse comment blocks (/// documentation or // regular comments)
    fn parse_comment_block(self: *ZigParser, context: *ParseContext) !void {
        _ = self;
        const start_line = context.current_line;
        var content_builder = std.array_list.Managed(u8).init(context.allocator);
        defer content_builder.deinit();

        while (!context.is_at_end()) {
            const line = context.peek_line();
            const trimmed = std.mem.trim(u8, line, " \t");

            if (!std.mem.startsWith(u8, trimmed, "//")) {
                break;
            }

            try content_builder.appendSlice(trimmed);
            try content_builder.append('\n');
            _ = context.read_line();
        }

        if (content_builder.items.len > 0) {
            const file_basename = std.fs.path.basename(context.file_path);
            const unit_id = try std.fmt.allocPrint(context.allocator, "{s}_comment_{d}", .{ file_basename, start_line });
            const content = try content_builder.toOwnedSlice();

            const unit = ParsedUnit{
                .id = unit_id,
                .unit_type = try context.allocator.dupe(u8, ZigUnitType.comment_block.to_string()),
                .content = content,
                .location = SourceLocation{
                    .file_path = context.file_path,
                    .line_start = start_line,
                    .line_end = context.current_line - 1,
                    .col_start = 1,
                    .col_end = 1,
                },
                .edges = std.array_list.Managed(ParsedEdge).init(context.allocator),
                .metadata = std.StringHashMap([]const u8).init(context.allocator),
            };

            try context.units.append(unit);
        }
    }

    /// Parse import statements
    fn parse_import(self: *ZigParser, context: *ParseContext) !void {
        _ = self;
        const start_line = context.current_line;
        const line = context.read_line();

        var import_path: ?[]const u8 = null;
        var var_name: ?[]const u8 = null;

        if (std.mem.indexOf(u8, line, "const ")) |const_pos| {
            const after_const = line[const_pos + 6 ..];
            if (std.mem.indexOf(u8, after_const, " =")) |eq_pos| {
                var_name = std.mem.trim(u8, after_const[0..eq_pos], " \t");
            }
        }

        if (std.mem.indexOf(u8, line, "@import(\"")) |import_pos| {
            const start_quote = import_pos + 9;
            if (std.mem.indexOf(u8, line[start_quote..], "\"")) |end_quote| {
                import_path = line[start_quote .. start_quote + end_quote];
            }
        }

        if (var_name != null and import_path != null) {
            const file_basename = std.fs.path.basename(context.file_path);
            const unit_id = try std.fmt.allocPrint(context.allocator, "{s}_import_{s}", .{ file_basename, var_name.? });
            const content = try context.allocator.dupe(u8, std.mem.trim(u8, line, " \t"));

            var metadata = std.StringHashMap([]const u8).init(context.allocator);
            try metadata.put("import_path", try context.allocator.dupe(u8, import_path.?));
            try metadata.put("variable_name", try context.allocator.dupe(u8, var_name.?));

            const unit = ParsedUnit{
                .id = unit_id,
                .unit_type = try context.allocator.dupe(u8, ZigUnitType.import.to_string()),
                .content = content,
                .location = SourceLocation{
                    .file_path = context.file_path,
                    .line_start = start_line,
                    .line_end = start_line,
                    .col_start = 1,
                    .col_end = @intCast(line.len),
                },
                .edges = std.array_list.Managed(ParsedEdge).init(context.allocator),
                .metadata = metadata,
            };

            try context.units.append(unit);
        }
    }

    /// Parse function definitions
    fn parse_function(self: *ZigParser, context: *ParseContext) !void {
        const start_line = context.current_line;
        const signature_line = context.read_line();

        const fn_name = extract_function_name(signature_line) orelse return;

        var content_builder = std.array_list.Managed(u8).init(context.allocator);
        defer content_builder.deinit();

        try content_builder.appendSlice(std.mem.trim(u8, signature_line, " \t"));

        if (self.config.include_function_bodies) {
            var brace_count: i32 = 0;
            var found_opening_brace = false;

            while (!context.is_at_end()) {
                const line = context.peek_line();

                for (line) |char| {
                    if (char == '{') {
                        brace_count += 1;
                        found_opening_brace = true;
                    } else if (char == '}') {
                        brace_count -= 1;
                    }
                }

                const actual_line = context.read_line();
                try content_builder.append('\n');
                try content_builder.appendSlice(actual_line);

                if (found_opening_brace and brace_count <= 0) {
                    break;
                }
            }
        }

        const file_basename = std.fs.path.basename(context.file_path);
        const unit_id = try std.fmt.allocPrint(context.allocator, "{s}_fn_{s}", .{ file_basename, fn_name });
        const content = try content_builder.toOwnedSlice();

        var metadata = std.StringHashMap([]const u8).init(context.allocator);
        try metadata.put("function_name", try context.allocator.dupe(u8, fn_name));
        try metadata.put("is_public", if (std.mem.startsWith(u8, signature_line, "pub ")) try context.allocator.dupe(u8, "true") else try context.allocator.dupe(u8, "false"));

        const unit = ParsedUnit{
            .id = unit_id,
            .unit_type = try context.allocator.dupe(u8, ZigUnitType.function.to_string()),
            .content = content,
            .location = SourceLocation{
                .file_path = context.file_path,
                .line_start = start_line,
                .line_end = context.current_line - 1,
                .col_start = 1,
                .col_end = 1,
            },
            .edges = std.array_list.Managed(ParsedEdge).init(context.allocator),
            .metadata = metadata,
        };

        try context.units.append(unit);
    }

    /// Parse constant definitions
    fn parse_constant(self: *ZigParser, context: *ParseContext) !void {
        _ = self;
        const start_line = context.current_line;
        const line = context.read_line();

        const const_name = extract_constant_name(line) orelse return;

        const unit_id = try std.fmt.allocPrint(context.allocator, "const_{s}", .{const_name});
        const content = try context.allocator.dupe(u8, std.mem.trim(u8, line, " \t"));

        var metadata = std.StringHashMap([]const u8).init(context.allocator);
        try metadata.put("constant_name", try context.allocator.dupe(u8, const_name));
        try metadata.put("is_public", if (std.mem.startsWith(u8, line, "pub ")) try context.allocator.dupe(u8, "true") else try context.allocator.dupe(u8, "false"));

        const unit = ParsedUnit{
            .id = unit_id,
            .unit_type = try context.allocator.dupe(u8, ZigUnitType.constant.to_string()),
            .content = content,
            .location = SourceLocation{
                .file_path = context.file_path,
                .line_start = start_line,
                .line_end = start_line,
                .col_start = 1,
                .col_end = @intCast(line.len),
            },
            .edges = std.array_list.Managed(ParsedEdge).init(context.allocator),
            .metadata = metadata,
        };

        try context.units.append(unit);
    }

    /// Parse variable definitions
    fn parse_variable(self: *ZigParser, context: *ParseContext) !void {
        _ = self;
        const start_line = context.current_line;
        const line = context.read_line();

        const var_name = extract_variable_name(line) orelse return;

        const unit_id = try std.fmt.allocPrint(context.allocator, "var_{s}", .{var_name});
        const content = try context.allocator.dupe(u8, std.mem.trim(u8, line, " \t"));

        var metadata = std.StringHashMap([]const u8).init(context.allocator);
        try metadata.put("variable_name", try context.allocator.dupe(u8, var_name));
        try metadata.put("is_public", if (std.mem.startsWith(u8, line, "pub ")) try context.allocator.dupe(u8, "true") else try context.allocator.dupe(u8, "false"));

        const unit = ParsedUnit{
            .id = unit_id,
            .unit_type = try context.allocator.dupe(u8, ZigUnitType.variable.to_string()),
            .content = content,
            .location = SourceLocation{
                .file_path = context.file_path,
                .line_start = start_line,
                .line_end = start_line,
                .col_start = 1,
                .col_end = @intCast(line.len),
            },
            .edges = std.array_list.Managed(ParsedEdge).init(context.allocator),
            .metadata = metadata,
        };

        try context.units.append(unit);
    }

    /// Parse struct definitions
    fn parse_struct(self: *ZigParser, context: *ParseContext) !void {
        _ = self;
        const start_line = context.current_line;
        const signature_line = context.read_line();

        const struct_name = extract_struct_name(signature_line) orelse return;

        var content_builder = std.array_list.Managed(u8).init(context.allocator);
        defer content_builder.deinit();

        try content_builder.appendSlice(std.mem.trim(u8, signature_line, " \t"));

        var brace_count: i32 = 0;
        var found_opening_brace = false;

        for (signature_line) |char| {
            if (char == '{') {
                brace_count += 1;
                found_opening_brace = true;
            } else if (char == '}') {
                brace_count -= 1;
            }
        }

        while (!context.is_at_end() and (brace_count > 0 or !found_opening_brace)) {
            const line = context.peek_line();

            for (line) |char| {
                if (char == '{') {
                    brace_count += 1;
                    found_opening_brace = true;
                } else if (char == '}') {
                    brace_count -= 1;
                }
            }

            const actual_line = context.read_line();
            try content_builder.append('\n');
            try content_builder.appendSlice(actual_line);

            if (found_opening_brace and brace_count <= 0) {
                break;
            }
        }

        const unit_id = try std.fmt.allocPrint(context.allocator, "struct_{s}", .{struct_name});
        const content = try content_builder.toOwnedSlice();

        var metadata = std.StringHashMap([]const u8).init(context.allocator);
        try metadata.put("struct_name", try context.allocator.dupe(u8, struct_name));
        try metadata.put("is_public", if (std.mem.startsWith(u8, signature_line, "pub ")) try context.allocator.dupe(u8, "true") else try context.allocator.dupe(u8, "false"));

        const unit = ParsedUnit{
            .id = unit_id,
            .unit_type = try context.allocator.dupe(u8, ZigUnitType.struct_def.to_string()),
            .content = content,
            .location = SourceLocation{
                .file_path = context.file_path,
                .line_start = start_line,
                .line_end = context.current_line - 1,
                .col_start = 1,
                .col_end = 1,
            },
            .edges = std.array_list.Managed(ParsedEdge).init(context.allocator),
            .metadata = metadata,
        };

        try context.units.append(unit);
    }

    /// Parse enum definitions
    fn parse_enum(self: *ZigParser, context: *ParseContext) !void {
        _ = self;
        const start_line = context.current_line;
        const line = context.read_line();

        const enum_name = extract_enum_name(line) orelse return;

        const unit_id = try std.fmt.allocPrint(context.allocator, "enum_{s}", .{enum_name});
        const content = try context.allocator.dupe(u8, std.mem.trim(u8, line, " \t"));

        var metadata = std.StringHashMap([]const u8).init(context.allocator);
        try metadata.put("enum_name", try context.allocator.dupe(u8, enum_name));
        try metadata.put("is_public", if (std.mem.startsWith(u8, line, "pub ")) try context.allocator.dupe(u8, "true") else try context.allocator.dupe(u8, "false"));

        const unit = ParsedUnit{
            .id = unit_id,
            .unit_type = try context.allocator.dupe(u8, ZigUnitType.enum_def.to_string()),
            .content = content,
            .location = SourceLocation{
                .file_path = context.file_path,
                .line_start = start_line,
                .line_end = start_line,
                .col_start = 1,
                .col_end = @intCast(line.len),
            },
            .edges = std.array_list.Managed(ParsedEdge).init(context.allocator),
            .metadata = metadata,
        };

        try context.units.append(unit);
    }

    /// Parse test blocks
    fn parse_test(self: *ZigParser, context: *ParseContext) !void {
        const start_line = context.current_line;
        const signature_line = context.read_line();

        const test_name = extract_test_name(signature_line) orelse "unnamed_test";

        var content_builder = std.array_list.Managed(u8).init(context.allocator);
        defer content_builder.deinit();

        try content_builder.appendSlice(std.mem.trim(u8, signature_line, " \t"));

        if (self.config.include_function_bodies) {
            var brace_count: i32 = 0;
            var found_opening_brace = false;

            while (!context.is_at_end()) {
                const line = context.peek_line();

                for (line) |char| {
                    if (char == '{') {
                        brace_count += 1;
                        found_opening_brace = true;
                    } else if (char == '}') {
                        brace_count -= 1;
                    }
                }

                const actual_line = context.read_line();
                try content_builder.append('\n');
                try content_builder.appendSlice(actual_line);

                if (found_opening_brace and brace_count <= 0) {
                    break;
                }
            }
        }

        const unit_id = try std.fmt.allocPrint(context.allocator, "test_{s}_{d}", .{ test_name, start_line });
        const content = try content_builder.toOwnedSlice();

        var metadata = std.StringHashMap([]const u8).init(context.allocator);
        try metadata.put("test_name", try context.allocator.dupe(u8, test_name));

        const unit = ParsedUnit{
            .id = unit_id,
            .unit_type = try context.allocator.dupe(u8, ZigUnitType.test_block.to_string()),
            .content = content,
            .location = SourceLocation{
                .file_path = context.file_path,
                .line_start = start_line,
                .line_end = context.current_line - 1,
                .col_start = 1,
                .col_end = 1,
            },
            .edges = std.array_list.Managed(ParsedEdge).init(context.allocator),
            .metadata = metadata,
        };

        try context.units.append(unit);
    }

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
        return "Zig Source Code Parser";
    }

    fn deinit_impl(ptr: *anyopaque, allocator: std.mem.Allocator) void {
        _ = allocator;
        const self: *ZigParser = @ptrCast(@alignCast(ptr));
        self.deinit();
    }
};

fn contains_keyword(line: []const u8, keyword: []const u8) bool {
    return std.mem.indexOf(u8, line, keyword) != null;
}

fn extract_function_name(line: []const u8) ?[]const u8 {
    const fn_pos = std.mem.indexOf(u8, line, "fn ") orelse return null;
    const after_fn = line[fn_pos + 3 ..];

    var start: usize = 0;
    while (start < after_fn.len and (after_fn[start] == ' ' or after_fn[start] == '\t')) {
        start += 1;
    }

    var end = start;
    while (end < after_fn.len and after_fn[end] != '(' and after_fn[end] != ' ' and after_fn[end] != '\t') {
        end += 1;
    }

    if (end > start) {
        return after_fn[start..end];
    }
    return null;
}

fn extract_constant_name(line: []const u8) ?[]const u8 {
    const const_pos = std.mem.indexOf(u8, line, "const ") orelse return null;
    const after_const = line[const_pos + 6 ..];

    var start: usize = 0;
    while (start < after_const.len and (after_const[start] == ' ' or after_const[start] == '\t')) {
        start += 1;
    }

    var end = start;
    while (end < after_const.len and after_const[end] != '=' and after_const[end] != ':' and after_const[end] != ' ' and after_const[end] != '\t') {
        end += 1;
    }

    if (end > start) {
        return after_const[start..end];
    }
    return null;
}

fn extract_variable_name(line: []const u8) ?[]const u8 {
    const var_pos = std.mem.indexOf(u8, line, "var ") orelse return null;
    const after_var = line[var_pos + 4 ..];

    var start: usize = 0;
    while (start < after_var.len and (after_var[start] == ' ' or after_var[start] == '\t')) {
        start += 1;
    }

    var end = start;
    while (end < after_var.len and after_var[end] != '=' and after_var[end] != ':' and after_var[end] != ' ' and after_var[end] != '\t') {
        end += 1;
    }

    if (end > start) {
        return after_var[start..end];
    }
    return null;
}

fn extract_struct_name(line: []const u8) ?[]const u8 {
    if (std.mem.indexOf(u8, line, "const ")) |const_pos| {
        const after_const = line[const_pos + 6 ..];

        var start: usize = 0;
        while (start < after_const.len and (after_const[start] == ' ' or after_const[start] == '\t')) {
            start += 1;
        }

        var end = start;
        while (end < after_const.len and after_const[end] != '=' and after_const[end] != ' ' and after_const[end] != '\t') {
            end += 1;
        }

        if (end > start) {
            return after_const[start..end];
        }
    }

    return null;
}

fn extract_enum_name(line: []const u8) ?[]const u8 {
    const const_pos = std.mem.indexOf(u8, line, "const ") orelse return null;
    const after_const = line[const_pos + 6 ..];

    var start: usize = 0;
    while (start < after_const.len and (after_const[start] == ' ' or after_const[start] == '\t')) {
        start += 1;
    }

    var end = start;
    while (end < after_const.len and after_const[end] != '=' and after_const[end] != ' ' and after_const[end] != '\t') {
        end += 1;
    }

    if (end > start) {
        return after_const[start..end];
    }
    return null;
}

fn extract_test_name(line: []const u8) ?[]const u8 {
    const test_pos = std.mem.indexOf(u8, line, "test ") orelse return null;
    const after_test = line[test_pos + 5 ..];

    var start: usize = 0;
    while (start < after_test.len and (after_test[start] == ' ' or after_test[start] == '\t')) {
        start += 1;
    }

    if (start >= after_test.len) return null;

    if (after_test[start] == '"') {
        start += 1; // skip opening quote
        var end = start;
        while (end < after_test.len and after_test[end] != '"') {
            end += 1;
        }
        if (end > start) {
            return after_test[start..end];
        }
    } else {
        var end = start;
        while (end < after_test.len and after_test[end] != ' ' and after_test[end] != '\t' and after_test[end] != '{') {
            end += 1;
        }
        if (end > start) {
            return after_test[start..end];
        }
    }

    return null;
}

test "zig parser creation and cleanup" {
    const testing = std.testing;
    const allocator = testing.allocator;

    const config = ZigParserConfig{};
    var zig_parser = ZigParser.init(allocator, config);
    defer zig_parser.deinit();

    const parser = zig_parser.parser();
    try testing.expect(parser.supports("text/zig"));
    try testing.expect(!parser.supports("text/rust"));
    try testing.expectEqualStrings("Zig Source Code Parser", parser.describe());
}

test "extract function names" {
    const testing = std.testing;

    try testing.expectEqualStrings("main", extract_function_name("pub fn main() void {").?);
    try testing.expectEqualStrings("test_func", extract_function_name("fn test_func(param: u32) !void {").?);
    try testing.expectEqualStrings("init", extract_function_name("    pub fn init(allocator: std.mem.Allocator) Self {").?);
}

test "extract constant names" {
    const testing = std.testing;

    try testing.expectEqualStrings("VERSION", extract_constant_name("pub const VERSION = \"1.0.0\";").?);
    try testing.expectEqualStrings("MyStruct", extract_constant_name("const MyStruct = struct {").?);
    try testing.expectEqualStrings("BUFFER_SIZE", extract_constant_name("    const BUFFER_SIZE: usize = 1024;").?);
}

test "extract test names" {
    const testing = std.testing;

    try testing.expectEqualStrings("basic functionality", extract_test_name("test \"basic functionality\" {").?);
    try testing.expectEqualStrings("memory management", extract_test_name("    test \"memory management\" {").?);
}
