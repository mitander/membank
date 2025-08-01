//! Rule-based code quality enforcement for Membank.
//!
//! Defines architectural constraints as composable rules rather than
//! hardcoded pattern matching. Rules encode the "why" of code quality
//! requirements and can be systematically validated and evolved.

const std = @import("std");
const violation = @import("violation.zig");

const Violation = violation.Violation;
const ViolationType = violation.ViolationType;

/// A rule defines a constraint and how to check it
pub const Rule = struct {
    name: []const u8,
    description: []const u8,
    violation_type: ViolationType,
    check_fn: *const fn (context: *RuleContext) []const RuleViolation,
    
    pub const RuleViolation = struct {
        line: u32,
        message: []const u8,
        context: ?[]const u8 = null,
        suggested_fix: ?[]const u8 = null,
    };
};

pub const RuleContext = struct {
    file_path: []const u8,
    source: []const u8,
    allocator: std.mem.Allocator,
    
    // Parsed context for semantic understanding
    functions: []const FunctionInfo,
    variables: []const VariableInfo,
    imports: []const ImportInfo,
    
    pub const FunctionInfo = struct {
        name: []const u8,
        line: u32,
        is_public: bool,
        parameter_count: u32,
        line_count: u32,
        calls: []const []const u8, // Functions this function calls
    };
    
    pub const VariableInfo = struct {
        name: []const u8,
        line: u32,
        is_const: bool,
        type_hint: ?[]const u8,
    };
    
    pub const ImportInfo = struct {
        module: []const u8,
        line: u32,
    };
    
    /// Check if this file is in a specific architectural layer
    pub fn is_in_layer(self: *const RuleContext, layer: []const u8) bool {
        return std.mem.indexOf(u8, self.file_path, layer) != null;
    }
    
    /// Check if a function uses arena allocation patterns
    pub fn function_uses_arena(self: *const RuleContext, func_name: []const u8) bool {
        // Look for arena-related patterns near function
        const func_start = self.find_function_start(func_name) orelse return false;
        const func_end = self.find_function_end(func_start);
        const func_body = self.source[func_start..func_end];
        
        return std.mem.indexOf(u8, func_body, "arena") != null or
               std.mem.indexOf(u8, func_body, "Arena") != null;
    }
    
    fn find_function_start(self: *const RuleContext, func_name: []const u8) ?usize {
        const pattern = std.fmt.allocPrint(self.allocator, "fn {s}(", .{func_name}) catch return null;
        defer self.allocator.free(pattern);
        return std.mem.indexOf(u8, self.source, pattern);
    }
    
    fn find_function_end(self: *const RuleContext, start: usize) usize {
        var brace_count: i32 = 0;
        var in_function = false;
        
        for (self.source[start..], start..) |char, i| {
            if (char == '{') {
                brace_count += 1;
                in_function = true;
            } else if (char == '}') {
                brace_count -= 1;
                if (in_function and brace_count == 0) {
                    return i + 1;
                }
            }
        }
        
        return self.source.len;
    }
};

/// Membank architectural rules
pub const MEMBANK_RULES = [_]Rule{
    .{
        .name = "naming_conventions",
        .description = "Enforce Membank naming conventions: snake_case functions, reject get_/set_ prefixes",
        .violation_type = .naming_convention,
        .check_fn = check_naming_conventions,
    },
    .{
        .name = "control_characters",
        .description = "Reject carriage returns and tabs in source files",
        .violation_type = .control_character,
        .check_fn = check_control_characters,
    },
    .{
        .name = "unicode_emojis",
        .description = "Ban Unicode emojis in source code",
        .violation_type = .unicode_emoji,
        .check_fn = check_unicode_emojis,
    },
    .{
        .name = "documentation_standards",
        .description = "Enforce documentation standards for public functions",
        .violation_type = .documentation_standard,
        .check_fn = check_documentation_standards,
    },
    .{
        .name = "function_length",
        .description = "Prevent functions from exceeding maximum line count",
        .violation_type = .function_length,
        .check_fn = check_function_length,
    },
    .{
        .name = "generic_functions",
        .description = "Ensure generic functions end with 'Type' suffix",
        .violation_type = .generic_function,
        .check_fn = check_generic_functions,
    },
    .{
        .name = "banned_patterns",
        .description = "Prevent usage of banned code patterns and enforce stdx usage",
        .violation_type = .banned_pattern,
        .check_fn = check_banned_patterns,
    },
    .{
        .name = "single_threaded_architecture",
        .description = "Enforce single-threaded design by preventing raw threading primitives",
        .violation_type = .architecture,
        .check_fn = check_single_threaded,
    },
    .{
        .name = "arena_per_subsystem",
        .description = "Ensure subsystems use arena allocation for bulk operations",
        .violation_type = .memory_management,
        .check_fn = check_arena_usage,
    },
    .{
        .name = "explicit_error_handling", 
        .description = "Prevent implicit error handling that masks failures",
        .violation_type = .error_handling,
        .check_fn = check_error_handling,
    },
    .{
        .name = "performance_conscious_allocation",
        .description = "Prevent allocation patterns that cause performance issues",
        .violation_type = .performance,
        .check_fn = check_allocation_patterns,
    },
    .{
        .name = "architectural_boundaries",
        .description = "Enforce clean boundaries between architectural layers",
        .violation_type = .architecture,
        .check_fn = check_layer_boundaries,
    },
    .{
        .name = "function_declaration_length",
        .description = "Enforce function declaration line length limits with multiline formatting",
        .violation_type = .function_length,
        .check_fn = check_function_declaration_length,
    },
};

/// Rule: Enforce Membank naming conventions
fn check_naming_conventions(context: *RuleContext) []const Rule.RuleViolation {
    var violations = std.ArrayList(Rule.RuleViolation).init(context.allocator);
    
    // Skip tidy files - they contain pattern examples
    if (std.mem.indexOf(u8, context.file_path, "tidy/rules.zig") != null) {
        return violations.toOwnedSlice() catch &[_]Rule.RuleViolation{};
    }
    
    // Look for actual function definitions with banned prefixes
    const banned_prefixes = [_][]const u8{ "get_", "set_" };
    
    var line_start: usize = 0;
    while (std.mem.indexOfScalarPos(u8, context.source, line_start, '\n')) |line_end| {
        const line = context.source[line_start..line_end];
        const line_num = find_line_with_pos(context.source, line_start) orelse 1;
        
        // Skip comments and string literals
        if (is_comment_or_string_line(line)) {
            line_start = line_end + 1;
            continue;
        }
        
        // Look for function definitions: "fn name(" or "pub fn name("
        if (find_function_definition(line)) |func_name| {
            for (banned_prefixes) |prefix| {
                if (std.mem.startsWith(u8, func_name, prefix)) {
                    const message = if (std.mem.eql(u8, prefix, "get_")) 
                        "Function prefix 'get_' violates naming conventions - use descriptive verbs"
                    else 
                        "Function prefix 'set_' violates naming conventions - use descriptive verbs";
                        
                    violations.append(.{
                        .line = line_num,
                        .message = message,
                        .suggested_fix = "Use descriptive verbs instead: query_*, find_*, create_*, etc.",
                    }) catch continue;
                    break;
                }
            }
        }
        
        line_start = line_end + 1;
        
        // Also check for camelCase while we're parsing function definitions
        if (find_function_definition(line)) |func_name| {
            if (has_camel_case_name(func_name)) {
                violations.append(.{
                    .line = line_num,
                    .message = "Function names must use snake_case, not camelCase",
                    .suggested_fix = "Convert to snake_case: myFunction -> my_function",
                }) catch continue;
            }
        }
        
        line_start = line_end + 1;
    }
    
    return violations.toOwnedSlice() catch &[_]Rule.RuleViolation{};
}

/// Check if a line is primarily a comment or string literal
fn is_comment_or_string_line(line: []const u8) bool {
    const trimmed = std.mem.trim(u8, line, " \t");
    if (trimmed.len == 0) return true;
    
    // Skip comment lines
    if (std.mem.startsWith(u8, trimmed, "//")) return true;
    
    // Skip lines that are mostly string literals (heuristic)
    const quote_count = std.mem.count(u8, line, "\"");
    return quote_count >= 2; // Likely contains string literals
}

/// Extract function name from a function definition line
fn find_function_definition(line: []const u8) ?[]const u8 {
    const trimmed = std.mem.trim(u8, line, " \t");
    
    // Look for function declarations with all possible prefixes:
    // pub fn, export fn, const fn, fn, pub export fn, etc.
    const fn_pos = std.mem.indexOf(u8, trimmed, "fn ") orelse return null;
    
    // Make sure "fn " is preceded by valid keywords or nothing
    if (fn_pos > 0) {
        const before_fn = trimmed[0..fn_pos];
        const valid_prefixes = [_][]const u8{ "pub ", "export ", "const ", "pub export ", "pub const ", "export const " };
        
        var valid_prefix = false;
        for (valid_prefixes) |prefix| {
            if (std.mem.endsWith(u8, before_fn, prefix)) {
                valid_prefix = true;
                break;
            }
        }
        
        // Also allow exact matches
        for (valid_prefixes) |prefix| {
            if (std.mem.eql(u8, before_fn, std.mem.trim(u8, prefix, " "))) {
                valid_prefix = true;
                break;
            }
        }
        
        if (!valid_prefix) return null;
    }
    
    const fn_start = fn_pos + 3; // Skip "fn "
    if (fn_start >= trimmed.len) return null;
    
    // Find the function name (up to opening parenthesis)
    const remaining = trimmed[fn_start..];
    const paren_pos = std.mem.indexOfScalar(u8, remaining, '(') orelse return null;
    
    if (paren_pos == 0) return null;
    const func_name = std.mem.trim(u8, remaining[0..paren_pos], " \t");
    
    return if (func_name.len > 0) func_name else null;
}

/// Check if a function name contains camelCase pattern
fn has_camel_case_name(name: []const u8) bool {
    // Skip common exceptions - standard functions
    if (std.mem.eql(u8, name, "main") or 
        std.mem.eql(u8, name, "init") or
        std.mem.eql(u8, name, "deinit") or
        std.mem.startsWith(u8, name, "test")) {
        return false;
    }
    
    // Skip type constructor functions that should use PascalCase
    // These return 'type' and should start with uppercase
    if (name.len > 0 and name[0] >= 'A' and name[0] <= 'Z') {
        // Check if it ends with "Type" suffix (conventional pattern)
        if (std.mem.endsWith(u8, name, "Type")) {
            return false;
        }
        // Check if function returns 'type' keyword pattern (simplified heuristic)
        // This is a type constructor function using PascalCase convention
        return false;
    }
    
    // Check for camelCase pattern (contains uppercase after first character)
    // Since we're only checking functions WE declare, this is simple
    for (name[1..]) |char| {
        if (char >= 'A' and char <= 'Z') {
            return true;
        }
    }
    
    return false;
}

/// Check if a line contains a camelCase function definition
fn has_camel_case_function(line: []const u8) bool {
    // Find "fn " pattern
    const fn_pos = std.mem.indexOf(u8, line, "fn ") orelse return false;
    const after_fn = line[fn_pos + 3..];
    
    // Find function name (up to opening paren or space)
    var name_end: usize = 0;
    for (after_fn, 0..) |char, i| {
        if (char == '(' or char == ' ' or char == '\t') {
            name_end = i;
            break;
        }
    }
    
    if (name_end == 0) return false;
    const fn_name = after_fn[0..name_end];
    
    // Skip common exceptions
    if (std.mem.eql(u8, fn_name, "main") or 
        std.mem.eql(u8, fn_name, "init") or
        std.mem.eql(u8, fn_name, "deinit") or
        std.mem.startsWith(u8, fn_name, "test")) {
        return false;
    }
    
    // Check for camelCase pattern (contains uppercase after first character)
    for (fn_name[1..]) |char| {
        if (char >= 'A' and char <= 'Z') {
            return true;
        }
    }
    
    return false;
}

/// Rule: Prevent threading primitives in single-threaded architecture
fn check_single_threaded(context: *RuleContext) []const Rule.RuleViolation {
    var violations = std.ArrayList(Rule.RuleViolation).init(context.allocator);
    
    // Skip tidy files - they contain pattern examples
    if (std.mem.indexOf(u8, context.file_path, "tidy/rules.zig") != null) {
        return violations.toOwnedSlice() catch &[_]Rule.RuleViolation{};
    }
    
    // Threading primitives violate single-threaded design
    const banned_threading = [_][]const u8{
        "std.Thread.spawn",
        "std.Thread.Mutex",
        "std.atomic",
        "std.Condvar",
        "std.Once",
    };
    
    for (banned_threading) |pattern| {
        if (find_pattern_violations(context.source, pattern, "Use coordination patterns instead of raw threading")) |viols| {
            violations.appendSlice(viols) catch continue;
        }
    }
    
    return violations.toOwnedSlice() catch &[_]Rule.RuleViolation{};
}

/// Rule: Suggest arena allocation for appropriate bulk operations
fn check_arena_usage(context: *RuleContext) []const Rule.RuleViolation {
    var violations = std.ArrayList(Rule.RuleViolation).init(context.allocator);
    
    // Skip test files - they should use testing.allocator for leak detection
    if (std.mem.indexOf(u8, context.file_path, "test") != null or
        std.mem.indexOf(u8, context.source, "testing.allocator") != null) {
        return violations.toOwnedSlice() catch &[_]Rule.RuleViolation{};
    }
    
    // Only suggest arena for production code in storage/query with large-scale operations
    if (context.is_in_layer("storage") or context.is_in_layer("query")) {
        var search_pos: usize = 0;
        while (std.mem.indexOfPos(u8, context.source, search_pos, "ArrayList")) |pos| {
            const line_num = find_line_with_pos(context.source, pos) orelse 1;
            
            const context_start = if (pos > 1200) pos - 1200 else 0;
            const local_context = context.source[context_start..@min(pos + 300, context.source.len)];
            
            // Look for large-scale bulk processing patterns
            const is_large_bulk_operation = 
                // Processing many blocks/entries with clear lifecycle
                (std.mem.indexOf(u8, local_context, "blocks") != null and
                 std.mem.indexOf(u8, local_context, "for (") != null) or
                (std.mem.indexOf(u8, local_context, "entries") != null and
                 std.mem.indexOf(u8, local_context, "while (") != null) or
                // Recovery/compaction operations (clear start/end boundaries)
                std.mem.indexOf(u8, local_context, "recovery") != null or
                std.mem.indexOf(u8, local_context, "compaction") != null or
                // Operations with thousands of items
                std.mem.indexOf(u8, local_context, "1000") != null or
                std.mem.indexOf(u8, local_context, "max_results") != null;
            
            const uses_arena = std.mem.indexOf(u8, local_context, "arena") != null or
                              std.mem.indexOf(u8, local_context, "Arena") != null;
            
            // Skip small collections and simple patterns
            const is_small_collection = 
                std.mem.indexOf(u8, local_context, ".init(allocator)") != null and
                std.mem.indexOf(u8, local_context, "for (") == null and
                std.mem.indexOf(u8, local_context, "while (") == null;
            
            if (is_large_bulk_operation and !uses_arena and !is_small_collection) {
                violations.append(.{
                    .line = line_num,
                    .message = "Large-scale bulk operations benefit from arena allocation",
                    .suggested_fix = "Consider ArenaAllocator for bulk data with clear lifecycle (init->process->deinit)",
                }) catch continue;
            }
            
            search_pos = pos + 1;
        }
    }
    
    return violations.toOwnedSlice() catch &[_]Rule.RuleViolation{};
}

/// Rule: Explicit error handling prevents silent failures
fn check_error_handling(context: *RuleContext) []const Rule.RuleViolation {
    var violations = std.ArrayList(Rule.RuleViolation).init(context.allocator);
    
    // Skip tidy files and test files - they contain pattern examples and test failures are not production segfaults
    if (std.mem.indexOf(u8, context.file_path, "tidy/rules.zig") != null or
        std.mem.indexOf(u8, context.file_path, "test") != null or
        std.mem.endsWith(u8, context.file_path, "_test.zig")) {
        return violations.toOwnedSlice() catch &[_]Rule.RuleViolation{};
    }
    
    // Check for error masking patterns that need safety comments
    const error_patterns = [_][]const u8{ "catch unreachable", "orelse unreachable", "try unreachable" };
    
    for (error_patterns) |pattern| {
        var search_pos: usize = 0;
        while (std.mem.indexOfPos(u8, context.source, search_pos, pattern)) |pos| {
            const line_num = find_line_with_pos(context.source, pos) orelse 1;
            const line_start = find_line_start(context.source, pos);
            const line_end = std.mem.indexOfScalarPos(u8, context.source, line_start, '\n') orelse context.source.len;
            
            // Check if there's a "Safety:" comment on the same line or in preceding lines
            const has_safety_comment = has_safety_comment_nearby(context.source, line_start, line_end);
            
            if (!has_safety_comment) {
                const message = if (std.mem.eql(u8, pattern, "catch unreachable"))
                    "Production 'catch unreachable' requires 'Safety:' comment explaining why unreachable is guaranteed"
                else if (std.mem.eql(u8, pattern, "try unreachable"))
                    "Handle the error case explicitly or add 'Safety:' comment"
                else
                    "Use proper error propagation or add 'Safety:' comment";
                
                violations.append(.{
                    .line = line_num,
                    .message = message,
                    .suggested_fix = "Add 'Safety:' comment on same line or preceding lines explaining why this is safe",
                }) catch {};
            }
            
            search_pos = pos + 1;
        }
    }
    
    return violations.toOwnedSlice() catch &[_]Rule.RuleViolation{};
}

/// Rule: Prevent allocation patterns that hurt performance
fn check_allocation_patterns(context: *RuleContext) []const Rule.RuleViolation {
    var violations = std.ArrayList(Rule.RuleViolation).init(context.allocator);
    
    // Find specific problematic patterns - bounded collections that should pre-allocate
    var search_start: usize = 0;
    while (std.mem.indexOfPos(u8, context.source, search_start, ".append(")) |append_pos| {
        const line_num = find_line_with_pos(context.source, append_pos) orelse 1;
        
        // Look for bounded collection patterns where we know the final size
        const search_start_pos = if (append_pos > 800) append_pos - 800 else 0;
        const local_context = context.source[search_start_pos..append_pos];
        
        // Only flag patterns where size is knowable in advance
        const is_bounded_collection = 
            // Query results with max_results parameter
            (std.mem.indexOf(u8, local_context, "max_results") != null and
             std.mem.indexOf(u8, local_context, "query") != null) or
            // Fixed iteration counts
            std.mem.indexOf(u8, local_context, "items.len") != null or
            // Range-based loops with known bounds
            (std.mem.indexOf(u8, local_context, "for (") != null and
             std.mem.indexOf(u8, local_context, "..") != null);
                        
        const has_capacity_management = std.mem.indexOf(u8, local_context, "ensureCapacity") != null or
                                       std.mem.indexOf(u8, local_context, "ensureTotalCapacity") != null;
        
        // Skip I/O parsing patterns (dynamic growth is appropriate)
        const is_parsing_pattern = 
            std.mem.indexOf(u8, local_context, "read_line") != null or
            std.mem.indexOf(u8, local_context, "parse") != null or
            std.mem.indexOf(u8, local_context, "peek_line") != null;
        
        // Check for tidy suppression comment on the line
        const line_start = find_line_start(context.source, append_pos);
        const line_end = std.mem.indexOfScalarPos(u8, context.source, append_pos, '\n') orelse context.source.len;
        const line_content = context.source[line_start..line_end];
        const has_suppression = std.mem.indexOf(u8, line_content, "// tidy:ignore-perf") != null;
        
        if (is_bounded_collection and !has_capacity_management and !is_parsing_pattern and !has_suppression) {
            violations.append(.{
                .line = line_num,
                .message = "ArrayList with known bounds should pre-allocate capacity for performance",
                .suggested_fix = "Use ensureCapacity() when collection size is predictable",
            }) catch {};
        }
        
        search_start = append_pos + 1;
    }
    
    return violations.toOwnedSlice() catch &[_]Rule.RuleViolation{};
}

/// Rule: Maintain clean architectural boundaries
fn check_layer_boundaries(context: *RuleContext) []const Rule.RuleViolation {
    var violations = std.ArrayList(Rule.RuleViolation).init(context.allocator);
    
    // Storage layer shouldn't import query layer
    if (context.is_in_layer("storage")) {
        for (context.imports) |import| {
            if (std.mem.indexOf(u8, import.module, "query") != null) {
                violations.append(.{
                    .line = import.line,
                    .message = "Storage layer cannot depend on query layer - violates layered architecture",
                    .suggested_fix = "Move shared types to core/ or restructure dependency",
                }) catch continue;
            }
        }
    }
    
    return violations.toOwnedSlice() catch &[_]Rule.RuleViolation{};
}

/// Utility: Find violations of a specific pattern
fn find_pattern_violations(source: []const u8, pattern: []const u8, fix: []const u8) ?[]const Rule.RuleViolation {
    if (std.mem.indexOf(u8, source, pattern)) |pattern_pos| {
        // Check for architecture suppression on the same line
        const line_num = find_line_with_pos(source, pattern_pos) orelse 1;
        const line_start = find_line_start(source, pattern_pos);
        const line_end = find_line_end(source, line_start);
        const line_content = source[line_start..line_end];
        
        if (std.mem.indexOf(u8, line_content, "// tidy:ignore-arch") != null) {
            return null; // Suppressed
        }
        
        const violations = std.heap.page_allocator.alloc(Rule.RuleViolation, 1) catch return null;
        violations[0] = .{
            .line = line_num,
            .message = std.fmt.allocPrint(std.heap.page_allocator, "Pattern '{s}' violates architecture", .{pattern}) catch "Pattern violation",
            .suggested_fix = fix,
        };
        return violations;
    }
    return null;
}

/// Utility: Find line number containing pattern
fn find_line_with_pattern(source: []const u8, pattern: []const u8) ?u32 {
    const pattern_pos = std.mem.indexOf(u8, source, pattern) orelse return null;
    return find_line_with_pos(source, pattern_pos);
}

/// Utility: Find line number for a given position in source
fn find_line_with_pos(source: []const u8, pos: usize) ?u32 {
    if (pos >= source.len) return null;
    
    var line: u32 = 1;
    for (source[0..pos]) |char| {
        if (char == '\n') line += 1;
    }
    
    return line;
}

/// Utility: Find the start of the line containing the given position
fn find_line_start(source: []const u8, pos: usize) usize {
    if (pos == 0) return 0;
    
    var i = pos;
    while (i > 0) {
        i -= 1;
        if (source[i] == '\n') {
            return i + 1;
        }
    }
    return 0;
}

/// Utility: Find the start of the previous line
fn find_previous_line_start(source: []const u8, current_line_start: usize) usize {
    if (current_line_start == 0) return 0;
    
    // Find the end of the previous line (newline before current line)
    var i = current_line_start;
    while (i > 0) {
        i -= 1;
        if (source[i] == '\n') {
            // Found the newline, now find the start of that previous line
            return find_line_start(source, i);
        }
    }
    return 0;
}

/// Check if there's a "Safety:" comment on the same line or in the preceding lines (up to 3 lines back)
fn has_safety_comment_nearby(source: []const u8, line_start: usize, line_end: usize) bool {
    // Check the current line first
    const current_line = source[line_start..line_end];
    if (std.mem.indexOf(u8, current_line, "Safety:") != null) {
        return true;
    }
    
    // Check up to 3 preceding lines for Safety: comment
    var check_line_start = line_start;
    var lines_checked: u32 = 0;
    
    while (lines_checked < 3 and check_line_start > 0) {
        // Find the previous line
        check_line_start = find_previous_line_start(source, check_line_start);
        const prev_line_end = find_next_newline_or_end(source, check_line_start);
        
        if (prev_line_end > check_line_start) {
            const prev_line = source[check_line_start..prev_line_end];
            if (std.mem.indexOf(u8, prev_line, "Safety:") != null) {
                return true;
            }
        }
        
        lines_checked += 1;
    }
    
    return false;
}

/// Utility: Find the end of the line starting at the given position
fn find_line_end(source: []const u8, line_start: usize) usize {
    for (source[line_start..], line_start..) |char, i| {
        if (char == '\n') {
            return i;
        }
    }
    return source.len;
}

/// Find the next newline or end of source
fn find_next_newline_or_end(source: []const u8, start: usize) usize {
    for (source[start..], start..) |char, i| {
        if (char == '\n') {
            return i;
        }
    }
    return source.len;
}

/// Rule: Check for control characters (carriage returns and tabs)
fn check_control_characters(context: *RuleContext) []const Rule.RuleViolation {
    var violations = std.ArrayList(Rule.RuleViolation).init(context.allocator);
    
    // Skip binary files  
    const binary_extensions = [_][]const u8{ ".ico", ".png", ".webp", ".svg" };
    for (binary_extensions) |ext| {
        if (std.mem.endsWith(u8, context.file_path, ext)) {
            return violations.toOwnedSlice() catch &[_]Rule.RuleViolation{};
        }
    }
    
    // Check for carriage returns
    if (std.mem.indexOfScalar(u8, context.source, '\r')) |pos| {
        if (!std.mem.endsWith(u8, context.file_path, ".bat")) {
            const line_num = find_line_with_pos(context.source, pos) orelse 1;
            violations.append(.{
                .line = line_num,
                .message = "Carriage return characters (\\r) are not allowed",
                .suggested_fix = "Use Unix line endings (LF only)",
            }) catch {};
        }
    }
    
    // Check for tabs (except in specific file types)
    if (std.mem.indexOfScalar(u8, context.source, '\t')) |pos| {
        const allowed_tab_files = std.mem.endsWith(u8, context.file_path, ".sln") or
                                  std.mem.endsWith(u8, context.file_path, ".go") or
                                  (std.mem.endsWith(u8, context.file_path, ".md") and 
                                   std.mem.indexOf(u8, context.source, "```go") != null);
        
        if (!allowed_tab_files) {
            const line_num = find_line_with_pos(context.source, pos) orelse 1;
            violations.append(.{
                .line = line_num,
                .message = "Tab characters are not allowed - use spaces for indentation",
                .suggested_fix = "Replace tabs with 4 spaces",
            }) catch {};
        }
    }
    
    return violations.toOwnedSlice() catch &[_]Rule.RuleViolation{};
}

/// Rule: Check for Unicode emojis 
fn check_unicode_emojis(context: *RuleContext) []const Rule.RuleViolation {
    var violations = std.ArrayList(Rule.RuleViolation).init(context.allocator);
    violations.ensureTotalCapacity(4) catch {}; // Emojis are rare in code
    
    for (context.source, 0..) |byte, i| {
        // Check for 4-byte UTF-8 sequences (most emojis)
        if (byte >= 0xF0 and i + 3 < context.source.len) {
            const bytes = context.source[i..i + 4];
            if (bytes[0] == 0xF0 and bytes[1] == 0x9F) {
                if ((bytes[2] >= 0x80 and bytes[2] <= 0xBF) or
                    (bytes[2] >= 0x98 and bytes[2] <= 0x9F) or
                    (bytes[2] >= 0xA4 and bytes[2] <= 0xAF)) {
                    const line_num = find_line_with_pos(context.source, i) orelse 1;
                    violations.append(.{
                        .line = line_num,
                        .message = "Unicode emojis are banned in code",
                        .suggested_fix = "Use ASCII alternatives like checkmarks: ✓",
                    }) catch {};
                }
            }
        }
        // Check for 3-byte sequences with emoji-like symbols
        else if (byte == 0xE2 and i + 2 < context.source.len) {
            const bytes = context.source[i..i + 3];
            // Allow checkmark ✓ (E2 9C 93) but ban other emoji-like symbols
            if (bytes[1] == 0x9C and bytes[2] == 0x93) continue;
            
            if ((bytes[1] == 0x9C and bytes[2] == 0x94) or
                (bytes[1] == 0x9D and bytes[2] == 0xA4) or
                (bytes[1] == 0x9A and bytes[2] >= 0x80 and bytes[2] <= 0xBF)) {
                const line_num = find_line_with_pos(context.source, i) orelse 1;
                violations.append(.{
                    .line = line_num,
                    .message = "Unicode emojis are banned in code",
                    .suggested_fix = "Use ASCII alternatives",
                }) catch {};
            }
        }
    }
    
    return violations.toOwnedSlice() catch &[_]Rule.RuleViolation{};
}

/// Rule: Check documentation standards (simplified)
fn check_documentation_standards(context: *RuleContext) []const Rule.RuleViolation {
    var violations = std.ArrayList(Rule.RuleViolation).init(context.allocator);
    
    // Skip tidy files - they contain pattern examples
    if (std.mem.indexOf(u8, context.file_path, "tidy/rules.zig") != null) {
        return violations.toOwnedSlice() catch &[_]Rule.RuleViolation{};
    }
    
    // Simple check for /// without space
    var search_pos: usize = 0;
    while (std.mem.indexOfPos(u8, context.source, search_pos, "///")) |pos| {
        if (pos + 3 < context.source.len and context.source[pos + 3] != ' ' and context.source[pos + 3] != '\n') {
            const line_num = find_line_with_pos(context.source, pos) orelse 1;
            violations.append(.{
                .line = line_num,
                .message = "Documentation comments should have space after ///",
                .suggested_fix = "Use '/// ' instead of '///'",
            }) catch {};
        }
        search_pos = pos + 1;
        if (search_pos >= context.source.len) break;
    }
    
    return violations.toOwnedSlice() catch &[_]Rule.RuleViolation{};
}

/// Rule: Check function length limits using smart pattern matching
fn check_function_length( // tidy:ignore-length - analysis function may legitimately be long for comprehensiveness
    context: *RuleContext,
) []const Rule.RuleViolation {
    var violations = std.ArrayList(Rule.RuleViolation).init(context.allocator);
    
    var line_start: usize = 0;
    var line_num: u32 = 1;
    
    while (line_start < context.source.len) {
        const line_end = std.mem.indexOfScalarPos(u8, context.source, line_start, '\n') orelse context.source.len;
        const line = context.source[line_start..line_end];
        
        // Skip comments and string literals
        if (is_comment_or_string_line(line)) {
            line_start = line_end + 1;
            line_num += 1;
            continue;
        }
        
        // Use smart function definition detection
        if (find_function_definition(line)) |func_name| {
            // Calculate function length by finding matching brace
            const fn_start_pos = line_start;
            const opening_brace = std.mem.indexOfScalarPos(u8, context.source, fn_start_pos, '{') orelse {
                line_start = line_end + 1;
                line_num += 1;
                continue;
            };
            
            // Find matching closing brace (simple brace counting)
            var brace_count: i32 = 1;
            var pos = opening_brace + 1;
            var function_lines: u32 = 0;
            
            while (pos < context.source.len and brace_count > 0) {
                if (context.source[pos] == '{') {
                    brace_count += 1;
                } else if (context.source[pos] == '}') {
                    brace_count -= 1;
                } else if (context.source[pos] == '\n') {
                    function_lines += 1;
                }
                pos += 1;
            }
            
            // Flag functions longer than 200 lines
            if (function_lines > 200) {
                // Check for length suppression on the function definition line
                if (std.mem.indexOf(u8, line, "// tidy:ignore-length") == null) {
                    violations.append(.{
                        .line = line_num,
                        .message = "Function is too long (>200 lines) - consider breaking into smaller functions",
                        .suggested_fix = "Extract logical units into separate functions for better maintainability",
                    }) catch {};
                }
            }
            
            _ = func_name; // Function name used for validation
        }
        
        line_start = line_end + 1;
        line_num += 1;
    }
    
    return violations.toOwnedSlice() catch &[_]Rule.RuleViolation{};
}

/// Rule: Check generic function naming using smart pattern matching
fn check_generic_functions(context: *RuleContext) []const Rule.RuleViolation {
    var violations = std.ArrayList(Rule.RuleViolation).init(context.allocator);
    
    var line_start: usize = 0;
    var line_num: u32 = 1;
    
    while (line_start < context.source.len) {
        const line_end = std.mem.indexOfScalarPos(u8, context.source, line_start, '\n') orelse context.source.len;
        const line = context.source[line_start..line_end];
        
        // Skip comments and string literals
        if (is_comment_or_string_line(line)) {
            line_start = line_end + 1;
            line_num += 1;
            continue;
        }
        
        // Use smart function definition detection - only check functions WE declare
        if (find_function_definition(line)) |func_name| {
            // Check for PascalCase functions that should be generic types
            // Only flag functions WE declare that start with uppercase
            if (func_name.len > 0 and func_name[0] >= 'A' and func_name[0] <= 'Z') {
                // Skip system functions and well-known exceptions
                if (std.mem.startsWith(u8, func_name, "JNI_") or
                    std.mem.eql(u8, func_name, "TestError") or
                    std.mem.eql(u8, func_name, "TestContext")) {
                    line_start = line_end + 1;
                    line_num += 1;
                    continue;
                }
                
                // PascalCase function should end with 'Type' suffix for generic functions
                if (!std.mem.endsWith(u8, func_name, "Type")) {
                    violations.append(.{
                        .line = line_num,
                        .message = "PascalCase function names should end with 'Type' suffix for generic functions",
                        .suggested_fix = "Add 'Type' suffix or convert to snake_case for regular functions",
                    }) catch {};
                }
            }
        }
        
        line_start = line_end + 1;
        line_num += 1;
    }
    
    return violations.toOwnedSlice() catch &[_]Rule.RuleViolation{};
}

/// Rule: Check for banned patterns and enforce stdx usage
fn check_banned_patterns(context: *RuleContext) []const Rule.RuleViolation {
    var violations = std.ArrayList(Rule.RuleViolation).init(context.allocator);
    
    // stdx.zig is allowed to use std functions, and tidy rules need these patterns for detection  
    if (std.mem.endsWith(u8, context.file_path, "src/core/stdx.zig") or
        std.mem.indexOf(u8, context.file_path, "tidy/rules.zig") != null) {
        return violations.toOwnedSlice() catch &[_]Rule.RuleViolation{};
    }
    
    const banned_patterns = [_]struct { pattern: []const u8, message: []const u8 }{
        .{ .pattern = "std.BoundedArray", .message = "use stdx.BoundedArrayType instead of std version" },
        .{ .pattern = "StaticBitSet", .message = "use stdx.bit_set_type instead of std version" },
        .{ .pattern = "std.time.Duration", .message = "use stdx.Duration instead of std version" },
        .{ .pattern = "std.time.Instant", .message = "use stdx.Instant instead of std version" },
        .{ .pattern = "trait.hasUniqueRepresentation", .message = "use stdx.has_unique_representation instead of std version" },
        .{ .pattern = "mem.copy(", .message = "use stdx.copy_disjoint instead of std version" },
        .{ .pattern = "mem.copyForwards(", .message = "use stdx.copy_left instead of std version" },
        .{ .pattern = "mem.copyBackwards(", .message = "use stdx.copy_right instead of std version" },
        .{ .pattern = "posix.unexpectedErrno(", .message = "use stdx.unexpected_errno instead of std version" },
        .{ .pattern = "FIXME", .message = "FIXME comments must be addressed before merging" },
        .{ .pattern = "TODO ", .message = "TODO comments must be resolved and deleted before merging" },
        .{ .pattern = "!comptime", .message = "use ! inside comptime blocks" },
    };
    
    // Skip tidy files for std.debug.assert pattern - they need these patterns for detection
    if (!std.mem.endsWith(u8, context.file_path, "src/dev/tidy.zig") and
        std.mem.indexOf(u8, context.file_path, "tidy/rules.zig") == null) {
        if (std.mem.indexOf(u8, context.source, "std.debug.assert")) |pos| {
            const line_num = find_line_with_pos(context.source, pos) orelse 1;
            violations.append(.{
                .line = line_num,
                .message = "use custom assert module instead of std.debug.assert",
                .suggested_fix = "Import and use membank assert module",
            }) catch {};
        }
    }
    
    // Skip tidy files for debug.assert pattern
    if (std.mem.indexOf(u8, context.file_path, "tidy/rules.zig") == null) {
        if (std.mem.indexOf(u8, context.source, "debug.assert(")) |pos| {
        const line_num = find_line_with_pos(context.source, pos) orelse 1;
            violations.append(.{
                .line = line_num,
                .message = "use unqualified assert() calls",
                .suggested_fix = "Use assert() instead of debug.assert()",
            }) catch {};
        }
    }
    
    // Check for dbg() calls (but not function definitions)
    if (std.mem.indexOf(u8, context.source, "dbg(")) |pos| {
        if (std.mem.indexOf(u8, context.source, "fn dbg(") == null) {
            const line_num = find_line_with_pos(context.source, pos) orelse 1;
            violations.append(.{
                .line = line_num,
                .message = "dbg() calls must be removed before merging",
                .suggested_fix = "Remove debug print statement",
            }) catch {};
        }
    }
    
    for (banned_patterns) |ban| {
        if (std.mem.indexOf(u8, context.source, ban.pattern)) |pos| {
            const line_num = find_line_with_pos(context.source, pos) orelse 1;
            violations.append(.{
                .line = line_num,
                .message = ban.message,
                .suggested_fix = "Use approved alternative",
            }) catch {};
        }
    }
    
    return violations.toOwnedSlice() catch &[_]Rule.RuleViolation{};
}

/// Rule: Check function declaration line length and suggest multiline formatting
fn check_function_declaration_length(context: *RuleContext) []const Rule.RuleViolation {
    var violations = std.ArrayList(Rule.RuleViolation).init(context.allocator);
    
    const MAX_DECLARATION_LENGTH = 120; // Maximum line length for function declarations
    
    var line_start: usize = 0;
    var line_num: u32 = 1;
    
    while (line_start < context.source.len) {
        const line_end = std.mem.indexOfScalarPos(u8, context.source, line_start, '\n') orelse context.source.len;
        const line = context.source[line_start..line_end];
        
        // Skip comments and string literals  
        if (is_comment_or_string_line(line)) {
            line_start = line_end + 1;
            line_num += 1;
            continue;
        }
        
        // Use smart function definition detection
        if (find_function_definition(line)) |func_name| {
            // Simple check: if this line is too long, suggest multiline formatting
            if (line.len > MAX_DECLARATION_LENGTH) {
                violations.append(.{
                    .line = line_num,
                    .message = "Function declaration line too long - format multiline with trailing comma",
                    .suggested_fix = "Put each parameter on separate line with trailing comma",
                }) catch {};
            }
            
            _ = func_name; // Used for detection
        }
        
        line_start = line_end + 1;
        line_num += 1;
    }
    
    return violations.toOwnedSlice() catch &[_]Rule.RuleViolation{};
}

