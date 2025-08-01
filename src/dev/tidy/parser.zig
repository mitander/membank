//! Simple semantic parser for tidy rule evaluation.
//!
//! Extracts structural information from Zig source code to enable
//! rule-based quality checks. Focuses on architectural patterns
//! rather than complete syntax analysis.

const std = @import("std");
const rules = @import("rules.zig");

const RuleContext = rules.RuleContext;
const FunctionInfo = RuleContext.FunctionInfo;
const VariableInfo = RuleContext.VariableInfo;
const ImportInfo = RuleContext.ImportInfo;

/// Parse source code into semantic context for rule evaluation
pub fn parse_source(allocator: std.mem.Allocator, file_path: []const u8, source: []const u8) !RuleContext {
    var functions = std.ArrayList(FunctionInfo).init(allocator);
    var variables = std.ArrayList(VariableInfo).init(allocator);
    var imports = std.ArrayList(ImportInfo).init(allocator);

    var lines = std.mem.splitScalar(u8, source, '\n');
    var line_num: u32 = 0;

    while (lines.next()) |line| {
        line_num += 1;
        const trimmed = std.mem.trim(u8, line, " \t");

        if (trimmed.len == 0 or std.mem.startsWith(u8, trimmed, "//")) {
            continue;
        }

        // Parse function definitions
        if (parse_function(trimmed, line_num)) |func| {
            try functions.append(func);
        }

        // Parse variable/constant declarations
        if (parse_variable(trimmed, line_num)) |variable| {
            try variables.append(variable);
        }

        // Parse imports
        if (parse_import(trimmed, line_num)) |import| {
            try imports.append(import);
        }
    }

    return RuleContext{
        .file_path = file_path,
        .source = source,
        .allocator = allocator,
        .functions = try functions.toOwnedSlice(),
        .variables = try variables.toOwnedSlice(),
        .imports = try imports.toOwnedSlice(),
    };
}

/// Extract function information from a line
fn parse_function(line: []const u8, line_num: u32) ?FunctionInfo {
    const is_public = std.mem.startsWith(u8, line, "pub fn ");
    const fn_start: usize = if (is_public) 7 else if (std.mem.startsWith(u8, line, "fn ")) 3 else return null;

    const remaining = line[fn_start..];
    const paren_pos = std.mem.indexOf(u8, remaining, "(") orelse return null;
    const func_name = std.mem.trim(u8, remaining[0..paren_pos], " \t");

    if (func_name.len == 0) return null;

    // Count parameters (simple heuristic)
    const params_end = std.mem.indexOf(u8, remaining[paren_pos..], ")") orelse return null;
    const params = remaining[paren_pos + 1 .. paren_pos + params_end];
    const param_count = if (std.mem.trim(u8, params, " \t").len == 0) 0 else count_commas(params) + 1;

    return FunctionInfo{
        .name = func_name,
        .line = line_num,
        .is_public = is_public,
        .parameter_count = @intCast(param_count),
        .line_count = 0, // TODO: Calculate by finding function end
        .calls = &[_][]const u8{}, // TODO: Extract function calls
    };
}

/// Extract variable/constant information from a line
fn parse_variable(line: []const u8, line_num: u32) ?VariableInfo {
    var is_const = false;
    var var_start: usize = 0;

    if (std.mem.startsWith(u8, line, "const ")) {
        is_const = true;
        var_start = 6;
    } else if (std.mem.startsWith(u8, line, "var ")) {
        var_start = 4;
    } else {
        return null;
    }

    const remaining = line[var_start..];
    const colon_pos = std.mem.indexOf(u8, remaining, ":");
    const equals_pos = std.mem.indexOf(u8, remaining, "=");

    // Variable name ends at first colon or equals
    const name_end = if (colon_pos != null and equals_pos != null)
        @min(colon_pos.?, equals_pos.?)
    else
        colon_pos orelse equals_pos orelse remaining.len;

    const var_name = std.mem.trim(u8, remaining[0..name_end], " \t");
    if (var_name.len == 0) return null;

    // Extract type hint if present
    var type_hint: ?[]const u8 = null;
    if (colon_pos != null and equals_pos != null) {
        const type_end = if (equals_pos.? > colon_pos.?) equals_pos.? else remaining.len;
        type_hint = std.mem.trim(u8, remaining[colon_pos.? + 1 .. type_end], " \t");
    }

    return VariableInfo{
        .name = var_name,
        .line = line_num,
        .is_const = is_const,
        .type_hint = type_hint,
    };
}

/// Extract import information from a line
fn parse_import(line: []const u8, line_num: u32) ?ImportInfo {
    if (!std.mem.startsWith(u8, line, "const ")) return null;

    const equals_pos = std.mem.indexOf(u8, line, "= @import(") orelse return null;
    const quote_start = equals_pos + 10;
    const quote_end = std.mem.indexOf(u8, line[quote_start..], "\"") orelse return null;

    const module_name = line[quote_start .. quote_start + quote_end];

    return ImportInfo{
        .module = module_name,
        .line = line_num,
    };
}

/// Count commas in parameter list (simple parameter counting)
fn count_commas(params: []const u8) u32 {
    var count: u32 = 0;
    var paren_depth: u32 = 0;

    for (params) |char| {
        switch (char) {
            '(' => paren_depth += 1,
            ')' => paren_depth = if (paren_depth > 0) paren_depth - 1 else 0,
            ',' => {
                if (paren_depth == 0) count += 1;
            },
            else => {},
        }
    }

    return count;
}
