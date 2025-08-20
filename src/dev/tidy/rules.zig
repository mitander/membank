//! Rule-based code quality enforcement for KausalDB.
//!
//! Defines architectural constraints as composable rules rather than
//! hardcoded pattern matching. Rules encode the "why" of code quality
//! requirements and can be systematically validated and evolved.

const std = @import("std");
const mem = std.mem;

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
        parameters: []const ParameterInfo, // Function parameters for detailed analysis
    };

    pub const ParameterInfo = struct {
        name: []const u8,
        type_name: ?[]const u8,
        position: u32, // 0-indexed parameter position
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

/// KausalDB architectural rules
pub const KAUSALDB_RULES = [_]Rule{
    .{
        .name = "naming_conventions",
        .description = "Enforce KausalDB naming conventions: snake_case functions, reject get_/set_ prefixes",
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
        .name = "public_function_documentation",
        .description = "Require all public functions to have documentation",
        .violation_type = .documentation_standard,
        .check_fn = check_public_function_documentation,
    },
    .{
        .name = "intelligent_comment_analysis",
        .description = "Detect 'WHAT' comments that restate obvious code instead of explaining 'WHY'",
        .violation_type = .comment_quality,
        .check_fn = check_comment_quality,
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
    .{
        .name = "import_field_access",
        .description = "Prevent direct field access in import statements, require two-phase pattern",
        .violation_type = .import_pattern,
        .check_fn = check_import_field_access,
    },
    .{
        .name = "two_phase_initialization",
        .description = "Enforce two-phase initialization pattern (init + startup)",
        .violation_type = .architecture,
        .check_fn = check_two_phase_initialization,
    },
    .{
        .name = "lifecycle_naming",
        .description = "Enforce precise lifecycle naming (startup vs start)",
        .violation_type = .naming_convention,
        .check_fn = check_lifecycle_naming,
    },
    .{
        .name = "simulation_first_testing",
        .description = "Ensure tests use SimulationVFS instead of direct filesystem access",
        .violation_type = .architecture,
        .check_fn = check_simulation_first_testing,
    },
    .{
        .name = "test_harness_usage",
        .description = "Encourage standardized test harness usage over manual VFS/StorageEngine setup",
        .violation_type = .architecture,
        .check_fn = check_test_harness_usage,
    },
    .{
        .name = "allocator_first_parameter",
        .description = "Enforce that std.mem.Allocator should always be the first parameter in functions",
        .violation_type = .naming_convention,
        .check_fn = check_allocator_first_parameter,
    },
};

/// Rule: Enforce KausalDB naming conventions
fn check_naming_conventions(context: *RuleContext) []const Rule.RuleViolation {
    var violations = std.array_list.Managed(Rule.RuleViolation).init(context.allocator);
    violations.ensureTotalCapacity(10) catch {};

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
            if (is_camel_case(func_name)) {
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

/// Extract function name from a source line containing function definition.
/// Handles all Zig function prefixes (pub, export, const, etc.).
/// Returns function name or null if line doesn't contain a function definition.
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

/// Check if an identifier follows camelCase pattern.
/// Takes a bare identifier name (not source code).
fn is_camel_case(name: []const u8) bool {
    // Skip common exceptions - standard functions
    if (std.mem.eql(u8, name, "main") or
        std.mem.eql(u8, name, "init") or
        std.mem.eql(u8, name, "deinit") or
        std.mem.startsWith(u8, name, "test"))
    {
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

/// Rule: Prevent threading primitives in single-threaded architecture
fn check_single_threaded(context: *RuleContext) []const Rule.RuleViolation {
    var violations = std.array_list.Managed(Rule.RuleViolation).init(context.allocator);
    violations.ensureTotalCapacity(5) catch {};

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
    var violations = std.array_list.Managed(Rule.RuleViolation).init(context.allocator);
    violations.ensureTotalCapacity(10) catch {};

    // Skip test files - they should use testing.allocator for leak detection
    if (std.mem.indexOf(u8, context.file_path, "test") != null or
        std.mem.indexOf(u8, context.source, "testing.allocator") != null)
    {
        return violations.toOwnedSlice() catch &[_]Rule.RuleViolation{};
    }

    // Only suggest arena for production code in storage/query with large-scale operations
    if (context.is_in_layer("storage") or context.is_in_layer("query")) {
        var search_pos: usize = 0;
        while (std.mem.indexOfPos(u8, context.source, search_pos, "ArrayList")) |pos| {
            const line_num = find_line_with_pos(context.source, pos) orelse 1;

            const context_start = if (pos > 1200) pos - 1200 else 0;
            const local_context = context.source[context_start..@min(pos + 300, context.source.len)];

            // Look for truly large-scale bulk processing patterns that benefit from arena
            const is_large_bulk_operation =
                // Recovery operations processing entire WAL or SSTable files
                (std.mem.indexOf(u8, local_context, "recovery") != null and
                    (std.mem.indexOf(u8, local_context, "wal") != null or
                        std.mem.indexOf(u8, local_context, "sstable") != null)) or
                // Compaction operations processing multiple SSTables
                (std.mem.indexOf(u8, local_context, "compaction") != null and
                    std.mem.indexOf(u8, local_context, "sstable") != null) or
                // Import/export operations processing entire datasets
                std.mem.indexOf(u8, local_context, "import_data") != null or
                std.mem.indexOf(u8, local_context, "export_data") != null or
                // Operations explicitly marked as bulk with very large numbers (100k+)
                std.mem.indexOf(u8, local_context, "100000") != null or
                std.mem.indexOf(u8, local_context, "1000000") != null;

            const uses_arena = std.mem.indexOf(u8, local_context, "arena") != null or
                std.mem.indexOf(u8, local_context, "Arena") != null;

            // Skip bounded query operations and algorithm collections
            const is_bounded_operation =
                // Query operations with max_results are inherently bounded
                std.mem.indexOf(u8, local_context, "max_results") != null or
                // Traversal algorithms with depth limits
                std.mem.indexOf(u8, local_context, "max_depth") != null or
                // Test collections (typically small and bounded)
                std.mem.indexOf(u8, local_context, "test") != null or
                // Collections with capacity management
                std.mem.indexOf(u8, local_context, "ensureCapacity") != null or
                // Simple collections without bulk processing
                (std.mem.indexOf(u8, local_context, ".init(allocator)") != null and
                    std.mem.indexOf(u8, local_context, "for (") == null and
                    std.mem.indexOf(u8, local_context, "while (") == null);

            if (is_large_bulk_operation and !uses_arena and !is_bounded_operation) {
                // Check for tidy ignore comment
                const line_start = find_line_start(context.source, pos);
                const line_end = std.mem.indexOfScalarPos(u8, context.source, pos, '\n') orelse context.source.len;
                const line_content = context.source[line_start..line_end];
                const has_suppression = std.mem.indexOf(u8, line_content, "// tidy:ignore-memory") != null;

                if (!has_suppression) {
                    violations.append(.{
                        .line = line_num,
                        .message = "Large-scale bulk operations benefit from arena allocation",
                        .suggested_fix = "Consider ArenaAllocator for bulk data with clear lifecycle (init->process->deinit)",
                    }) catch continue;
                }
            }

            search_pos = pos + 1;
        }
    }

    return violations.toOwnedSlice() catch &[_]Rule.RuleViolation{};
}

/// Rule: Explicit error handling prevents silent failures
fn check_error_handling(context: *RuleContext) []const Rule.RuleViolation {
    var violations = std.array_list.Managed(Rule.RuleViolation).init(context.allocator);
    violations.ensureTotalCapacity(20) catch {};

    // Skip tidy files and test files - they contain pattern examples and test failures are not production segfaults
    if (std.mem.indexOf(u8, context.file_path, "tidy/rules.zig") != null or
        std.mem.indexOf(u8, context.file_path, "test") != null or
        std.mem.endsWith(u8, context.file_path, "_test.zig"))
    {
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
    var violations = std.array_list.Managed(Rule.RuleViolation).init(context.allocator);
    violations.ensureTotalCapacity(15) catch {};

    // Find ArrayList.init patterns that clearly need capacity management
    var search_start: usize = 0;
    while (std.mem.indexOfPos(u8, context.source, search_start, "ArrayList")) |arraylist_pos| {
        const line_num = find_line_with_pos(context.source, arraylist_pos) orelse 1;

        // Look for .init pattern
        const init_end = @min(arraylist_pos + 50, context.source.len);
        const init_context = context.source[arraylist_pos..init_end];
        if (std.mem.indexOf(u8, init_context, ".init(") == null) {
            search_start = arraylist_pos + 1;
            continue;
        }

        // Check for capacity management within 300 characters after ArrayList.init
        const capacity_check_end = @min(arraylist_pos + 300, context.source.len);
        const capacity_context = context.source[arraylist_pos..capacity_check_end];
        const has_capacity_management =
            std.mem.indexOf(u8, capacity_context, "ensureCapacity(") != null or
            std.mem.indexOf(u8, capacity_context, "ensureTotalCapacity(") != null;

        // Skip if capacity is already managed
        if (has_capacity_management) {
            search_start = arraylist_pos + 1;
            continue;
        }

        // Check surrounding context for patterns
        const context_start = if (arraylist_pos > 1000) arraylist_pos - 1000 else 0;
        const context_end = @min(arraylist_pos + 500, context.source.len);
        const wider_context = context.source[context_start..context_end];

        // Skip dynamic patterns where size is unpredictable
        const is_dynamic_pattern =
            // Struct field initialization (like in IndexEntry.init)
            std.mem.indexOf(u8, wider_context, "return") != null and
            std.mem.indexOf(u8, wider_context, "{") != null or
            // HashMap operations
            std.mem.indexOf(u8, wider_context, "getOrPut") != null or
            // Iterator patterns
            std.mem.indexOf(u8, wider_context, "iterator") != null or
            std.mem.indexOf(u8, wider_context, "iterate_all") != null or
            std.mem.indexOf(u8, wider_context, "while (") != null or
            // I/O operations
            std.mem.indexOf(u8, wider_context, "read") != null or
            std.mem.indexOf(u8, wider_context, "parse") != null or
            std.mem.indexOf(u8, wider_context, "stream") != null or
            // Graph operations
            std.mem.indexOf(u8, wider_context, "edge") != null or
            std.mem.indexOf(u8, wider_context, "graph") != null or
            // Function/method definitions
            std.mem.indexOf(u8, wider_context, "pub fn") != null or
            std.mem.indexOf(u8, wider_context, "fn ") != null;

        // Only flag clear bounded patterns
        const is_clearly_bounded =
            // Explicit range loops with ".." pattern
            (std.mem.indexOf(u8, wider_context, "for (") != null and
                std.mem.indexOf(u8, wider_context, "..") != null) or
            // Processing items.len (known size)
            std.mem.indexOf(u8, wider_context, "items.len") != null or
            // Large numbers suggesting bulk operations
            std.mem.indexOf(u8, wider_context, "1000") != null or
            std.mem.indexOf(u8, wider_context, "10000") != null;

        // Check for suppression
        const line_start = find_line_start(context.source, arraylist_pos);
        const line_end = std.mem.indexOfScalarPos(u8, context.source, arraylist_pos, '\n') orelse context.source.len;
        const line_content = context.source[line_start..line_end];
        const has_suppression = std.mem.indexOf(u8, line_content, "// tidy:ignore-perf") != null or
            std.mem.indexOf(u8, line_content, "// tidy:ignore-memory") != null;

        if (is_clearly_bounded and !is_dynamic_pattern and !has_suppression) {
            violations.append(.{
                .line = line_num,
                .message = "ArrayList with known bounds should pre-allocate capacity for performance",
                .suggested_fix = "Use ensureCapacity() when collection size is predictable",
            }) catch {};
        }

        search_start = arraylist_pos + 1;
    }

    return violations.toOwnedSlice() catch &[_]Rule.RuleViolation{};
}

/// Rule: Maintain clean architectural boundaries
fn check_layer_boundaries(context: *RuleContext) []const Rule.RuleViolation {
    var violations = std.array_list.Managed(Rule.RuleViolation).init(context.allocator);
    violations.ensureTotalCapacity(5) catch {};

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
    var violations = std.array_list.Managed(Rule.RuleViolation).init(context.allocator);
    violations.ensureTotalCapacity(3) catch {};

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
    var violations = std.array_list.Managed(Rule.RuleViolation).init(context.allocator);
    violations.ensureTotalCapacity(4) catch {}; // Emojis are rare in code

    for (context.source, 0..) |byte, i| {
        // Check for 4-byte UTF-8 sequences (most emojis)
        if (byte >= 0xF0 and i + 3 < context.source.len) {
            const bytes = context.source[i .. i + 4];
            if (bytes[0] == 0xF0 and bytes[1] == 0x9F) {
                if ((bytes[2] >= 0x80 and bytes[2] <= 0xBF) or
                    (bytes[2] >= 0x98 and bytes[2] <= 0x9F) or
                    (bytes[2] >= 0xA4 and bytes[2] <= 0xAF))
                {
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
            const bytes = context.source[i .. i + 3];
            // Allow checkmark ✓ (E2 9C 93) but ban other emoji-like symbols
            if (bytes[1] == 0x9C and bytes[2] == 0x93) continue;

            if ((bytes[1] == 0x9C and bytes[2] == 0x94) or
                (bytes[1] == 0x9D and bytes[2] == 0xA4) or
                (bytes[1] == 0x9A and bytes[2] >= 0x80 and bytes[2] <= 0xBF))
            {
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
    var violations = std.array_list.Managed(Rule.RuleViolation).init(context.allocator);
    violations.ensureTotalCapacity(10) catch {};

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
fn check_function_length(
    context: *RuleContext,
) []const Rule.RuleViolation {
    var violations = std.array_list.Managed(Rule.RuleViolation).init(context.allocator);
    violations.ensureTotalCapacity(10) catch {};

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
            if (function_lines > 300) {
                // Check for length suppression on the function definition line
                // Also skip generated or analysis functions that are inherently long
                const is_analysis_function = std.mem.indexOf(u8, line, "check_") != null or
                    std.mem.indexOf(u8, line, "analyze_") != null or
                    std.mem.indexOf(u8, line, "validate_") != null;

                if (std.mem.indexOf(u8, line, "// tidy:ignore-length") == null and !is_analysis_function) {
                    violations.append(.{
                        .line = line_num,
                        .message = "Function is too long (>300 lines) - consider breaking into smaller functions",
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
    var violations = std.array_list.Managed(Rule.RuleViolation).init(context.allocator);
    violations.ensureTotalCapacity(5) catch {};

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
                    std.mem.eql(u8, func_name, "TestContext"))
                {
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
    var violations = std.array_list.Managed(Rule.RuleViolation).init(context.allocator);
    violations.ensureTotalCapacity(15) catch {};

    // stdx.zig is allowed to use std functions, and tidy rules need these patterns for detection
    if (std.mem.endsWith(u8, context.file_path, "src/core/stdx.zig") or
        std.mem.indexOf(u8, context.file_path, "tidy/rules.zig") != null)
    {
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
        std.mem.indexOf(u8, context.file_path, "tidy/rules.zig") == null)
    {
        if (std.mem.indexOf(u8, context.source, "std.debug.assert")) |pos| {
            const line_num = find_line_with_pos(context.source, pos) orelse 1;
            violations.append(.{
                .line = line_num,
                .message = "use custom assert module instead of std.debug.assert",
                .suggested_fix = "Import and use kausaldb assert module",
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
    var violations = std.array_list.Managed(Rule.RuleViolation).init(context.allocator);
    violations.ensureTotalCapacity(10) catch {};

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

/// Rule: Require all public functions to have documentation
fn check_public_function_documentation(context: *RuleContext) []const Rule.RuleViolation {
    var violations = std.array_list.Managed(Rule.RuleViolation).init(context.allocator);
    violations.ensureTotalCapacity(20) catch {};

    // Skip test files - test functions don't need full documentation
    if (std.mem.indexOf(u8, context.file_path, "test") != null or
        std.mem.endsWith(u8, context.file_path, "_test.zig") or
        std.mem.indexOf(u8, context.file_path, "tidy/rules.zig") != null)
    {
        return violations.toOwnedSlice() catch &[_]Rule.RuleViolation{};
    }

    var line_start: usize = 0;
    var line_num: u32 = 1;

    while (line_start < context.source.len) {
        const line_end = std.mem.indexOfScalarPos(u8, context.source, line_start, '\n') orelse context.source.len;
        const line = context.source[line_start..line_end];

        // Look for public function declarations
        if (std.mem.indexOf(u8, line, "pub fn ") != null) {
            if (find_function_definition(line)) |func_name| {
                // Skip functions that don't need documentation:
                // - Standard lifecycle functions
                // - Simple getters/setters (single return statement)
                // - Standard interface implementations (alloc, free, etc.)
                // - Test functions
                if (std.mem.eql(u8, func_name, "main") or
                    std.mem.eql(u8, func_name, "init") or
                    std.mem.eql(u8, func_name, "deinit") or
                    std.mem.startsWith(u8, func_name, "test") or
                    // Standard allocator interface
                    std.mem.eql(u8, func_name, "alloc") or
                    std.mem.eql(u8, func_name, "free") or
                    std.mem.eql(u8, func_name, "dupe") or
                    std.mem.eql(u8, func_name, "realloc") or
                    // Simple getters (likely trivial)
                    is_simple_getter_or_setter(context.source, line_start, func_name))
                {
                    line_start = line_end + 1;
                    line_num += 1;
                    continue;
                }

                // Check if there's a /// comment preceding this function
                const has_doc_comment = has_documentation_comment_before(context.source, line_start);

                if (!has_doc_comment) {
                    violations.append(.{
                        .line = line_num,
                        .message = "Public function must have documentation comment (///) explaining purpose, parameters, and errors",
                        .suggested_fix = "Add /// comment block before function explaining what it does and why",
                    }) catch {};
                }
            }
        }

        line_start = line_end + 1;
        line_num += 1;
    }

    return violations.toOwnedSlice() catch &[_]Rule.RuleViolation{};
}

/// Rule: Intelligent detection of "WHAT" comments that should explain "WHY"
fn check_comment_quality(context: *RuleContext) []const Rule.RuleViolation {
    var violations = std.array_list.Managed(Rule.RuleViolation).init(context.allocator);
    violations.ensureTotalCapacity(25) catch {};

    // Skip tidy files - they contain pattern examples
    if (std.mem.indexOf(u8, context.file_path, "tidy/rules.zig") != null) {
        return violations.toOwnedSlice() catch &[_]Rule.RuleViolation{};
    }

    // Skip test files - they use flow comments to explain test scenarios
    if (std.mem.endsWith(u8, context.file_path, "_test.zig") or
        std.mem.indexOf(u8, context.file_path, "/tests/") != null or
        std.mem.indexOf(u8, context.file_path, "test ") != null)
    {
        return violations.toOwnedSlice() catch &[_]Rule.RuleViolation{};
    }

    var line_start: usize = 0;
    var line_num: u32 = 1;
    var in_test_function = false;
    var brace_depth: u32 = 0;

    while (line_start < context.source.len) {
        const line_end = std.mem.indexOfScalarPos(u8, context.source, line_start, '\n') orelse context.source.len;
        const line = context.source[line_start..line_end];
        const trimmed_line = std.mem.trim(u8, line, " \t");

        // Track test function boundaries
        if (std.mem.startsWith(u8, trimmed_line, "test ")) {
            in_test_function = true;
            brace_depth = 0;
        }

        // Track brace depth to know when test function ends
        for (line) |char| {
            if (char == '{') brace_depth += 1;
            if (char == '}') {
                if (brace_depth > 0) brace_depth -= 1;
                if (in_test_function and brace_depth == 0) {
                    in_test_function = false;
                }
            }
        }

        // Look for single-line comments (not doc comments)
        if (std.mem.indexOf(u8, line, "//") != null and std.mem.indexOf(u8, line, "///") == null) {
            // Skip comments inside test functions - they explain test flow
            if (in_test_function) {
                line_start = line_end + 1;
                line_num += 1;
                continue;
            }
            const comment_start = std.mem.indexOf(u8, line, "//").?;
            const comment_text = std.mem.trim(u8, line[comment_start + 2 ..], " \t");

            // Skip valid comments that explain WHY, constraints, or rationale
            if (comment_text.len == 0 or
                std.mem.startsWith(u8, comment_text, "tidy:") or
                std.mem.startsWith(u8, comment_text, "Safety:") or
                std.mem.startsWith(u8, comment_text, "NOTE:") or
                std.mem.startsWith(u8, comment_text, "TODO:") or
                std.mem.startsWith(u8, comment_text, "FIXME:") or
                std.mem.startsWith(u8, comment_text, "BUG:") or
                std.mem.startsWith(u8, comment_text, "HACK:") or
                std.mem.startsWith(u8, comment_text, "Per ") or // "Per RFC specification"
                std.mem.startsWith(u8, comment_text, "RFC ") or // "RFC 3095 requires"
                // Comments that explain WHY/rationale (good comments)
                std.mem.indexOf(u8, comment_text, "because") != null or
                std.mem.indexOf(u8, comment_text, "since") != null or
                std.mem.indexOf(u8, comment_text, "to prevent") != null or
                std.mem.indexOf(u8, comment_text, "to avoid") != null or
                std.mem.indexOf(u8, comment_text, "for performance") != null or
                std.mem.indexOf(u8, comment_text, "instead of") != null or
                std.mem.indexOf(u8, comment_text, "rather than") != null or
                std.mem.indexOf(u8, comment_text, "ensures") != null or
                std.mem.indexOf(u8, comment_text, "guarantees") != null)
            {
                line_start = line_end + 1;
                line_num += 1;
                continue;
            }

            // Check for "WHAT" comment patterns that restate obvious code
            const next_line_start = line_end + 1;
            if (next_line_start < context.source.len) {
                const next_line_end = std.mem.indexOfScalarPos(u8, context.source, next_line_start, '\n') orelse context.source.len;
                const next_line = std.mem.trim(u8, context.source[next_line_start..next_line_end], " \t");

                if (is_obvious_what_comment(context, comment_text, next_line)) {
                    violations.append(.{
                        .line = line_num,
                        .message = "Comment restates obvious code - explain WHY, not WHAT",
                        .suggested_fix = "Replace with rationale: why this approach was chosen, performance trade-offs, or constraints",
                    }) catch {};
                }
            }
        }

        line_start = line_end + 1;
        line_num += 1;
    }

    return violations.toOwnedSlice() catch &[_]Rule.RuleViolation{};
}

/// Check if there's a documentation comment (///) before the given position
fn has_documentation_comment_before(source: []const u8, line_start: usize) bool {
    // Look for /// comment in the few lines before this function
    var check_pos = line_start;
    var lines_back: u32 = 0;

    while (lines_back < 5 and check_pos > 0) {
        // Find start of previous line
        while (check_pos > 0 and source[check_pos - 1] != '\n') {
            check_pos -= 1;
        }
        if (check_pos > 0) check_pos -= 1; // Skip the newline

        // Find start of this line
        const prev_line_start = blk: {
            var pos = check_pos;
            while (pos > 0 and source[pos - 1] != '\n') {
                pos -= 1;
            }
            break :blk pos;
        };

        const prev_line_end = check_pos + 1;
        const prev_line = source[prev_line_start..prev_line_end];

        // Check if this line contains /// comment
        if (std.mem.indexOf(u8, prev_line, "///") != null) {
            return true;
        }

        // If we hit a non-comment, non-empty line, stop looking
        const trimmed = std.mem.trim(u8, prev_line, " \t\n");
        if (trimmed.len > 0 and !std.mem.startsWith(u8, trimmed, "//")) {
            break;
        }

        check_pos = prev_line_start;
        lines_back += 1;
    }

    return false;
}

/// Check if function name indicates it's simple and doesn't need documentation
fn is_simple_by_name(func_name: []const u8) bool {
    // Most getters/setters are trivial wrappers that don't justify documentation overhead
    if (std.mem.startsWith(u8, func_name, "get") and func_name.len > 3) {
        return true;
    }

    // Setters rarely have complex business logic worth documenting
    if (std.mem.startsWith(u8, func_name, "set") and func_name.len > 3) {
        return true;
    }

    // Boolean checks are typically self-explanatory from function name
    if (std.mem.startsWith(u8, func_name, "is") or
        std.mem.startsWith(u8, func_name, "has") or
        std.mem.startsWith(u8, func_name, "can"))
    {
        return true;
    }

    return false;
}

/// Extract function body text for complexity analysis
fn find_function_body(
    source: []const u8,
    line_start: usize,
    func_name: []const u8,
) ?[]const u8 {
    const func_pattern = std.fmt.allocPrint(std.heap.page_allocator, "fn {s}(", .{func_name}) catch return null;
    defer std.heap.page_allocator.free(func_pattern);

    const func_start = std.mem.indexOf(u8, source[line_start..], func_pattern) orelse return null;
    const func_body_start = std.mem.indexOf(u8, source[line_start + func_start ..], "{") orelse return null;
    const func_body_end = blk: {
        var brace_count: i32 = 0;
        var pos = line_start + func_start + func_body_start;

        while (pos < source.len) {
            if (source[pos] == '{') {
                brace_count += 1;
            } else if (source[pos] == '}') {
                brace_count -= 1;
                if (brace_count == 0) {
                    break :blk pos;
                }
            }
            pos += 1;
        }
        break :blk source.len;
    };

    return source[line_start + func_start + func_body_start .. func_body_end];
}

/// Count meaningful lines in function body (exclude trivial lines)
fn count_meaningful_lines(func_body: []const u8) u32 {
    var meaningful_lines: u32 = 0;
    var body_line_it = std.mem.splitSequence(u8, func_body, "\n");
    while (body_line_it.next()) |line| {
        const trimmed = std.mem.trim(u8, line, " \t");
        if (trimmed.len == 0 or
            std.mem.eql(u8, trimmed, "{") or
            std.mem.eql(u8, trimmed, "}") or
            std.mem.startsWith(u8, trimmed, "return") or
            std.mem.indexOf(u8, trimmed, " = ") != null)
        {
            continue;
        }
        meaningful_lines += 1;
    }
    return meaningful_lines;
}

/// Check if function is a simple getter or setter that doesn't need documentation
fn is_simple_getter_or_setter(source: []const u8, line_start: usize, func_name: []const u8) bool {
    // Check simple naming patterns first
    if (is_simple_by_name(func_name)) {
        return true;
    }

    // Analyze function body complexity
    const func_body = find_function_body(source, line_start, func_name) orelse return false;
    const meaningful_lines = count_meaningful_lines(func_body);

    // Simple functions have <= 2 meaningful lines
    return meaningful_lines <= 2;
}

/// Intelligent detection of "WHAT" comments that restate obvious code
fn is_obvious_what_comment(context: *RuleContext, comment_text: []const u8, next_line: []const u8) bool {
    // Skip very short comments or code lines (not enough data to analyze)
    if (comment_text.len < 10 or next_line.len < 10) return false;

    // Context-aware exemptions: Skip files/contexts where descriptive comments are valuable
    if (is_test_context(context.file_path)) return false;
    if (is_documentation_context(comment_text)) return false;
    if (is_why_explanation(comment_text)) return false;

    // Convert both to lowercase for comparison
    const lower_comment = std.ascii.allocLowerString(context.allocator, comment_text) catch return false;
    defer context.allocator.free(lower_comment);

    const lower_code = std.ascii.allocLowerString(context.allocator, next_line) catch return false;
    defer context.allocator.free(lower_code);

    // Extract meaningful words (>3 chars) from comment
    var comment_words = std.array_list.Managed([]const u8).init(context.allocator);
    defer comment_words.deinit();
    comment_words.ensureTotalCapacity(10) catch {};

    var comment_word_it = std.mem.tokenizeAny(u8, lower_comment, " \t(){}[].,;:");
    while (comment_word_it.next()) |word| {
        if (word.len > 3 and !is_common_word(word)) {
            comment_words.append(word) catch continue;
        }
    }

    if (comment_words.items.len == 0) return false;

    // Count how many comment words appear in the code line
    var matching_words: u32 = 0;
    for (comment_words.items) |word| {
        if (std.mem.indexOf(u8, lower_code, word) != null) {
            matching_words += 1;
        }
    }

    // If >60% of meaningful words in comment appear in code, likely a "WHAT" comment
    const overlap_ratio = (matching_words * 100) / @as(u32, @intCast(comment_words.items.len));
    return overlap_ratio > 60;
}

/// Check if word is too common to be meaningful for overlap analysis
fn is_common_word(word: []const u8) bool {
    const common_words = [_][]const u8{
        "the",  "and",  "for",  "with", "this", "that", "from", "into", "over", "than",
        "when", "will", "have", "been", "they", "were", "what", "some", "time",
    };

    for (common_words) |common| {
        if (std.mem.eql(u8, word, common)) return true;
    }
    return false;
}

/// Context-aware refinement: Check if file is in test context where descriptive comments are valuable
fn is_test_context(file_path: []const u8) bool {
    return std.mem.indexOf(u8, file_path, "test") != null or
        std.mem.indexOf(u8, file_path, "spec") != null or
        std.mem.endsWith(u8, file_path, "_test.zig") or
        std.mem.endsWith(u8, file_path, "test.zig");
}

/// Context-aware refinement: Check if comment is documentation context (examples, API usage)
fn is_documentation_context(comment_text: []const u8) bool {
    const doc_indicators = [_][]const u8{
        "example", "usage", "note:",    "important:", "warning:", "todo:",
        "fixme:",  "hack:", "see also", "reference",  "spec",     "rfc",
    };

    const lower_comment = std.ascii.allocLowerString(std.heap.page_allocator, comment_text) catch return false;
    defer std.heap.page_allocator.free(lower_comment);

    for (doc_indicators) |indicator| {
        if (std.mem.indexOf(u8, lower_comment, indicator) != null) return true;
    }
    return false;
}

/// Context-aware refinement: Check if comment explains WHY (rationale, design decisions)
fn is_why_explanation(comment_text: []const u8) bool {
    // WHY indicators: words that typically introduce rationale or design reasoning
    const why_indicators = [_][]const u8{
        "enables",         "allows",        "provides",        "ensures",        "guarantees",   "prevents",
        "avoids",          "optimizes",     "improves",        "reduces",        "increases",    "maintains",
        "because",         "since",         "due to",          "in order to",    "so that",      "to avoid",
        "for performance", "for safety",    "for correctness", "for robustness", "without this", "otherwise",
        "alternatively",   "instead of",    "trade-off",       "tradeoff",       "compromise",   "design decision",
        "rationale",       "requirement",   "constraint",      "limitation",     "assumption",   "invariant",
        "protocol",        "specification", "standard",        "convention",     "pattern",
    };

    const lower_comment = std.ascii.allocLowerString(std.heap.page_allocator, comment_text) catch return false;
    defer std.heap.page_allocator.free(lower_comment);

    for (why_indicators) |indicator| {
        if (std.mem.indexOf(u8, lower_comment, indicator) != null) return true;
    }

    // Additional heuristic: Comments with justification structure ("X because Y", "X to Y")
    if (std.mem.indexOf(u8, lower_comment, " because ") != null or
        std.mem.indexOf(u8, lower_comment, " to ") != null or
        std.mem.indexOf(u8, lower_comment, " for ") != null)
    {
        return true;
    }

    return false;
}

/// Rule: Enforce two-phase initialization pattern (init + startup)
/// Major services should separate cold initialization from hot I/O operations
fn check_two_phase_initialization(context: *RuleContext) []const Rule.RuleViolation {
    var violations = std.array_list.Managed(Rule.RuleViolation).init(context.allocator);
    violations.ensureTotalCapacity(10) catch {};

    // Only apply to major service components
    if (!context.is_in_layer("storage") and
        !context.is_in_layer("server") and
        !context.is_in_layer("ingestion"))
    {
        return violations.toOwnedSlice() catch &[_]Rule.RuleViolation{};
    }

    // Look for init functions that perform I/O operations
    for (context.functions) |func| {
        if (std.mem.eql(u8, func.name, "init") or std.mem.startsWith(u8, func.name, "init_")) {
            const func_start = context.find_function_start(func.name) orelse continue;
            const func_end = context.find_function_end(func_start);
            const func_body = context.source[func_start..func_end];

            // Check for I/O operations in init functions
            const io_patterns = [_][]const u8{
                "fs.openFile",
                "std.fs.cwd().openFile",
                "createFile",
                "mkdir",
                "writeFile",
                "readFile",
                "listen(",
                "bind(",
                "connect(",
            };

            for (io_patterns) |pattern| {
                if (std.mem.indexOf(u8, func_body, pattern) != null) {
                    violations.append(.{
                        .line = func.line,
                        .message = "init() function performs I/O - violates two-phase initialization pattern",
                        .context = "Found I/O operation in init() function",
                        .suggested_fix = "Move I/O operations to startup() function, keep init() memory-only",
                    }) catch continue;
                }
            }
        }
    }

    return violations.toOwnedSlice() catch &[_]Rule.RuleViolation{};
}

/// Rule: Enforce precise lifecycle naming (startup vs start)
/// startup() is for the second initialization phase, start() is deprecated
fn check_lifecycle_naming(context: *RuleContext) []const Rule.RuleViolation {
    var violations = std.array_list.Managed(Rule.RuleViolation).init(context.allocator);
    violations.ensureTotalCapacity(10) catch {};

    // Check for deprecated lifecycle method names
    const deprecated_names = [_][]const u8{
        "start",
        "initialize_storage",
        "initialize_server",
        "init_storage",
        "init_server",
    };

    for (context.functions) |func| {
        for (deprecated_names) |deprecated| {
            if (std.mem.eql(u8, func.name, deprecated)) {
                violations.append(.{
                    .line = func.line,
                    .message = "Function uses deprecated lifecycle naming",
                    .context = "KausalDB uses precise lifecycle verbs for clarity",
                    .suggested_fix = "Rename to 'startup'",
                }) catch continue;
            }
        }
    }

    return violations.toOwnedSlice() catch &[_]Rule.RuleViolation{};
}

/// Rule: Enforce simulation-first testing via VFS abstraction
/// Tests should use SimulationVFS instead of direct file system access
fn check_simulation_first_testing(context: *RuleContext) []const Rule.RuleViolation {
    var violations = std.array_list.Managed(Rule.RuleViolation).init(context.allocator);
    violations.ensureTotalCapacity(20) catch {};

    // Only apply to test files
    if (!std.mem.endsWith(u8, context.file_path, "_test.zig") and
        std.mem.indexOf(u8, context.file_path, "tests/") == null)
    {
        return violations.toOwnedSlice() catch &[_]Rule.RuleViolation{};
    }

    // Check for direct file system usage in tests
    const direct_fs_patterns = [_][]const u8{
        "std.fs.cwd()",
        "fs.cwd()",
        "std.fs.openFileAbsolute",
        "std.fs.createFileAbsolute",
        "std.testing.tmpDir",
    };

    var line_start: usize = 0;
    while (std.mem.indexOfScalarPos(u8, context.source, line_start, '\n')) |line_end| {
        const line = context.source[line_start..line_end];
        const line_num = find_line_with_pos(context.source, line_start) orelse 1;

        for (direct_fs_patterns) |pattern| {
            if (std.mem.indexOf(u8, line, pattern) != null) {
                // Allow if there's an explicit suppression comment or legitimate usage
                const has_suppression = std.mem.indexOf(u8, line, "// tidy:ignore-simulation") != null;
                const is_cleanup = std.mem.indexOf(u8, line, "cleanup") != null or
                    std.mem.indexOf(u8, line, "deleteTree") != null or
                    std.mem.indexOf(u8, line, "Best effort") != null or
                    std.mem.indexOf(u8, line, "// CLEANUP:") != null;

                if (!has_suppression and !is_cleanup) {
                    violations.append(.{
                        .line = line_num,
                        .message = "Test uses direct file system access instead of SimulationVFS",
                        .context = "Direct filesystem access prevents deterministic testing",
                        .suggested_fix = "Use SimulationVFS for deterministic, simulation-first testing",
                    }) catch continue;
                }
            }
        }

        line_start = line_end + 1;
        if (line_start >= context.source.len) break;
    }

    return violations.toOwnedSlice() catch &[_]Rule.RuleViolation{};
}

fn check_test_harness_usage(context: *RuleContext) []const Rule.RuleViolation {
    var violations = std.array_list.Managed(Rule.RuleViolation).init(context.allocator);
    violations.ensureTotalCapacity(10) catch {};

    // Only apply to test files
    if (!std.mem.endsWith(u8, context.file_path, "_test.zig") and
        std.mem.indexOf(u8, context.file_path, "tests/") == null)
    {
        return violations.toOwnedSlice() catch &[_]Rule.RuleViolation{};
    }

    // Skip files that are legitimately manual (benchmark, memory safety, etc.)
    if (std.mem.indexOf(u8, context.file_path, "defensive/") != null or
        std.mem.indexOf(u8, context.file_path, "benchmark/") != null or
        std.mem.indexOf(u8, context.file_path, "recovery/") != null)
    {
        return violations.toOwnedSlice() catch &[_]Rule.RuleViolation{};
    }

    // Look for manual VFS + StorageEngine pattern
    var line_start: usize = 0;
    var found_sim_vfs = false;
    var found_storage_engine = false;
    var sim_vfs_line: u32 = 0;
    var has_justification = false;

    while (std.mem.indexOfScalarPos(u8, context.source, line_start, '\n')) |line_end| {
        const line = context.source[line_start..line_end];
        const line_num = find_line_with_pos(context.source, line_start) orelse 1;

        // Check for justification comment (flexible pattern matching)
        if (std.mem.indexOf(u8, line, "// Manual setup required") != null) {
            has_justification = true;
        }

        // Look for SimulationVFS.init pattern
        if (std.mem.indexOf(u8, line, "SimulationVFS.init(") != null) {
            found_sim_vfs = true;
            sim_vfs_line = line_num;
        }

        // Look for StorageEngine.init_default pattern
        if (std.mem.indexOf(u8, line, "StorageEngine.init_default(") != null and found_sim_vfs) {
            found_storage_engine = true;

            // If we found both patterns without justification, suggest harness
            if (!has_justification) {
                violations.append(.{
                    .line = sim_vfs_line,
                    .message = "Consider using StorageHarness instead of manual VFS/StorageEngine setup",
                    .context = "Manual SimulationVFS + StorageEngine pattern detected",
                    .suggested_fix = "Use StorageHarness.init_and_startup() or add justification comment",
                }) catch continue;
            }

            // Reset for next potential pattern
            found_sim_vfs = false;
            found_storage_engine = false;
            has_justification = false;
        }

        line_start = line_end + 1;
        if (line_start >= context.source.len) break;
    }

    return violations.toOwnedSlice() catch &[_]Rule.RuleViolation{};
}

/// Rule: Enforce that std.mem.Allocator should always be the first parameter
/// Following Zig standard library conventions for consistent API design
fn check_allocator_first_parameter(context: *RuleContext) []const Rule.RuleViolation {
    var violations = std.array_list.Managed(Rule.RuleViolation).init(context.allocator);
    // Pre-allocate based on function count - typically much fewer violations than functions
    violations.ensureTotalCapacity(@min(context.functions.len, 50)) catch {};

    // Skip test files - they often use testing.allocator and have different patterns
    if (std.mem.indexOf(u8, context.file_path, "test") != null or
        std.mem.endsWith(u8, context.file_path, "_test.zig") or
        std.mem.indexOf(u8, context.file_path, "tidy/rules.zig") != null)
    {
        return violations.toOwnedSlice() catch &[_]Rule.RuleViolation{};
    }

    for (context.functions) |func| {
        // Skip functions with less than 2 parameters (allocator + at least one other)
        if (func.parameters.len < 2) continue;

        // Skip special lifecycle functions that don't typically take allocators first
        if (std.mem.eql(u8, func.name, "main") or
            std.mem.eql(u8, func.name, "init") or
            std.mem.eql(u8, func.name, "deinit") or
            std.mem.eql(u8, func.name, "startup") or
            std.mem.eql(u8, func.name, "shutdown") or
            std.mem.startsWith(u8, func.name, "test") or
            // Skip interface implementation functions (ptr comes first by convention)
            std.mem.endsWith(u8, func.name, "_impl") or
            // Skip methods (first param is usually self or pointer types)
            (func.parameters.len > 0 and (std.mem.eql(u8, func.parameters[0].name, "self") or
                (func.parameters[0].type_name != null and
                    (std.mem.startsWith(u8, func.parameters[0].type_name.?, "*") or
                        std.mem.startsWith(u8, func.parameters[0].type_name.?, "?*"))))))
        {
            continue;
        }

        // Look for allocator parameters
        var allocator_param_pos: ?u32 = null;
        for (func.parameters, 0..) |param, i| {
            if (param.type_name) |type_name| {
                if (is_allocator_type(type_name)) {
                    allocator_param_pos = @intCast(i);
                    break;
                }
            }
        }

        // If function has an allocator parameter and it's not first, flag it
        if (allocator_param_pos) |pos| {
            if (pos != 0) {
                violations.append(.{
                    .line = func.line,
                    .message = "Allocator parameter should be first, following Zig standard library conventions",
                    .context = "Found allocator not in first position",
                    .suggested_fix = "Move allocator parameter to first position for consistent API design",
                }) catch continue;
            }
        }
    }

    return violations.toOwnedSlice() catch &[_]Rule.RuleViolation{};
}

/// Check if a type string represents an allocator type
fn is_allocator_type(type_name: []const u8) bool {
    return std.mem.eql(u8, type_name, "std.mem.Allocator") or
        std.mem.eql(u8, type_name, "Allocator") or
        std.mem.eql(u8, type_name, "std.mem.ArenaAllocator") or
        std.mem.eql(u8, type_name, "ArenaAllocator") or
        std.mem.endsWith(u8, type_name, "Allocator") or
        std.mem.indexOf(u8, type_name, "arena") != null or
        std.mem.indexOf(u8, type_name, "Arena") != null;
}

/// Check for direct field access in import statements.
/// Enforces two-phase pattern: import module, then extract fields separately.
fn check_import_field_access(context: *RuleContext) []const Rule.RuleViolation {
    var violations = std.array_list.Managed(Rule.RuleViolation).init(context.allocator);

    var lines = mem.splitScalar(u8, context.source, '\n');
    var line_number: u32 = 1;

    while (lines.next()) |line| {
        defer line_number += 1;

        const trimmed = mem.trim(u8, line, " \t");
        if (is_import_field_access_pattern(trimmed) and !is_in_string_literal(trimmed)) {
            const rule_violation = Rule.RuleViolation{
                .line = line_number,
                .message = "Direct field access in import statement violates two-phase pattern",
                .context = context.allocator.dupe(u8, trimmed) catch trimmed,
                .suggested_fix = "Split into: const module_name = @import(\"...\"); const field = module_name.field;",
            };
            violations.append(rule_violation) catch break;
        }
    }

    return violations.toOwnedSlice() catch &[_]Rule.RuleViolation{};
}

/// Detect pattern: const name = @import("path").field;
fn is_import_field_access_pattern(line: []const u8) bool {
    if (!mem.startsWith(u8, line, "const ") or
        mem.indexOf(u8, line, "= @import(") == null)
    {
        return false;
    }

    // Look for pattern: @import("...").field_name;
    const import_end = mem.indexOf(u8, line, "\").") orelse return false;
    const after_dot = line[import_end + 3 ..];

    // Check if there's a valid field access followed by semicolon
    const semicolon = mem.indexOf(u8, after_dot, ";") orelse return false;
    const field_part = mem.trim(u8, after_dot[0..semicolon], " \t");

    // Must be a simple field access (no spaces, parentheses, etc.)
    return field_part.len > 0 and
        mem.indexOf(u8, field_part, " ") == null and
        mem.indexOf(u8, field_part, "(") == null and
        mem.indexOf(u8, field_part, "[") == null;
}

/// Check if the import pattern appears inside a string literal
fn is_in_string_literal(line: []const u8) bool {
    // Look for the import pattern within quotes
    if (mem.indexOf(u8, line, "\"") == null) return false;

    const import_pos = mem.indexOf(u8, line, "@import(") orelse return false;

    // Find all quote positions before the import
    var quote_count: u32 = 0;
    var i: usize = 0;
    while (i < import_pos) {
        if (line[i] == '"' and (i == 0 or line[i - 1] != '\\')) {
            quote_count += 1;
        }
        i += 1;
    }

    // If odd number of quotes before @import, we're inside a string literal
    return quote_count % 2 == 1;
}
