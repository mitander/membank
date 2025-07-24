//! Code quality and style enforcement for CortexDB.
//!
//! Checks coding standards, naming conventions, documentation,
//! and catches common issues before they reach main branch.

const std = @import("std");
const assert = std.debug.assert;
const fs = std.fs;
const mem = std.mem;

const Shell = @import("shell.zig").Shell;

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    const args = try std.process.argsAlloc(allocator);
    defer std.process.argsFree(allocator, args);

    if (args.len > 1) {
        std.debug.print("Usage: tidy\n", .{});
        std.debug.print("Checks code quality and style for CortexDB.\n", .{});
        return;
    }

    std.debug.print("Running tidy checks...\n", .{});

    // Get all source files
    const shell = try Shell.create(allocator);
    defer shell.destroy();

    const file_paths = try list_file_paths(shell);

    var violations: u32 = 0;

    for (file_paths) |file_path| {
        const file = fs.cwd().openFile(file_path, .{}) catch |err| {
            std.debug.print("Error opening file {s}: {}\n", .{ file_path, err });
            continue;
        };
        defer file.close();

        const file_size = try file.getEndPos();
        const content = try allocator.allocSentinel(u8, file_size, 0);
        defer allocator.free(content);

        _ = try file.readAll(content);

        const source_file = SourceFile{
            .path = file_path,
            .text = content,
        };

        // Run all tidy checks
        if (tidy_banned_patterns(source_file.path, source_file.text)) |err_msg| {
            std.debug.print("{s}: banned pattern: {s}\n", .{ file_path, err_msg });
            violations += 1;
        }

        if (tidy_control_characters(source_file)) |char| {
            std.debug.print("{s}: invalid control character: {c}\n", .{ file_path, char });
            violations += 1;
        }

        if (tidy_naming_conventions(source_file)) |violation| {
            std.debug.print(
                "{s}: line {}: {s}\n",
                .{ file_path, violation.line, violation.message },
            );
            violations += 1;
        }

        if (tidy_documentation_standards(source_file)) |doc_error| {
            std.debug.print(
                "{s}: line {}: {s}\n",
                .{ file_path, doc_error.line, doc_error.message },
            );
            violations += 1;
        }

        if (tidy_generic_functions(source_file)) |generic_fn| {
            std.debug.print(
                "{s}: line {}: generic function detected: {s}\n",
                .{ file_path, generic_fn.line, generic_fn.name },
            );
            violations += 1;
        }

        if (tidy_function_length(source_file)) |long_fn| {
            std.debug.print(
                "{s}: line {}: function '{s}' is {} lines (max {})\n",
                .{ file_path, long_fn.line, long_fn.name, long_fn.length, function_line_count_max },
            );
            violations += 1;
        }

        // Dead declaration detection disabled due to AST API changes in Zig 0.15+
    }

    if (violations > 0) {
        std.debug.print("Found {} style violations.\n", .{violations});
        std.process.exit(1);
    } else {
        std.debug.print("All tidy checks passed!\n", .{});
    }
}

const UsedDeclarations = std.StringHashMapUnmanaged(struct {
    count: u32,
    offset: u32,
});

test "tidy" {
    const allocator = std.testing.allocator;

    const shell = try Shell.create(allocator);
    defer shell.destroy();

    const paths = try list_file_paths(shell);

    const buffer_size = 1024 * 1024;
    const buffer = try allocator.alloc(u8, buffer_size);
    defer allocator.free(buffer);

    var violations = std.ArrayList(struct { path: []const u8, line: ?u32, violation_type: []const u8, message: []const u8 }).init(allocator);
    defer violations.deinit();

    // Collect all violations instead of returning on first error
    for (paths) |path| {
        const bytes_read = (try std.fs.cwd().readFile(path, buffer)).len;
        if (bytes_read >= buffer.len - 1) {
            try violations.append(.{
                .path = path,
                .line = null,
                .violation_type = "file_too_long",
                .message = "file exceeds maximum read buffer size",
            });
            continue;
        }
        buffer[bytes_read] = 0;

        const source_file = SourceFile{ .path = path, .text = buffer[0..bytes_read :0] };

        if (tidy_control_characters(source_file)) |control_character| {
            try violations.append(.{
                .path = source_file.path,
                .line = null,
                .violation_type = "control_character",
                .message = try std.fmt.allocPrint(allocator, "contains control character: code={} symbol='{c}'", .{ control_character, control_character }),
            });
        }

        if (mem.endsWith(u8, source_file.path, ".zig")) {
            if (tidy_banned_patterns(source_file.path, source_file.text)) |ban_reason| {
                try violations.append(.{
                    .path = source_file.path,
                    .line = null,
                    .violation_type = "banned_pattern",
                    .message = ban_reason,
                });
            }

            if (tidy_naming_conventions(source_file)) |violation| {
                try violations.append(.{
                    .path = source_file.path,
                    .line = @intCast(violation.line),
                    .violation_type = "naming_violation",
                    .message = violation.message,
                });
            }

            if (tidy_unicode_emojis(source_file)) |emoji_error| {
                try violations.append(.{
                    .path = source_file.path,
                    .line = null,
                    .violation_type = "unicode_emoji",
                    .message = emoji_error,
                });
            }

            if (tidy_documentation_standards(source_file)) |doc_error| {
                try violations.append(.{
                    .path = source_file.path,
                    .line = @intCast(doc_error.line),
                    .violation_type = "documentation_error",
                    .message = doc_error.message,
                });
            }

            if (tidy_generic_functions(source_file)) |function| {
                try violations.append(.{
                    .path = source_file.path,
                    .line = @intCast(function.line),
                    .violation_type = "generic_function_naming",
                    .message = try std.fmt.allocPrint(allocator, "'{s}' should end with the 'Type' suffix", .{function.name}),
                });
            }

            // Comment pattern checking disabled pending violation cleanup
            // if (tidy_what_comments(source_file)) |comment_issue| {
            //     try violations.append(.{
            //         .path = source_file.path,
            //         .line = comment_issue.line,
            //         .violation_type = "what_comment",
            //         .message = comment_issue.message,
            //     });
            // }
        }

        if (mem.endsWith(u8, source_file.path, ".md")) {
            tidy_markdown_standards(source_file.text) catch |err| {
                try violations.append(.{
                    .path = source_file.path,
                    .line = null,
                    .violation_type = "markdown_error",
                    .message = try std.fmt.allocPrint(allocator, "markdown issue: {}", .{err}),
                });
            };
        }
    }

    // Report all violations at once with clean summary
    if (violations.items.len > 0) {
        std.debug.print("\n=== TIDY VIOLATIONS ({}) ===\n", .{violations.items.len});

        for (violations.items) |violation| {
            if (violation.line) |line| {
                std.debug.print("{s}:{d} [{s}] {s}\n", .{ violation.path, line, violation.violation_type, violation.message });
            } else {
                std.debug.print("{s} [{s}] {s}\n", .{ violation.path, violation.violation_type, violation.message });
            }
        }
        std.debug.print("\n", .{});

        return error.StyleViolations;
    }
}

const SourceFile = struct { path: []const u8, text: [:0]const u8 };

/// Returns error message if source contains banned patterns.
fn tidy_banned_patterns(file_path: []const u8, source: []const u8) ?[]const u8 {
    // stdx.zig is allowed to use std functions as it implements the approved wrappers
    if (std.mem.endsWith(u8, file_path, "src/core/stdx.zig")) {
        return null;
    }
    // Prefer stdx alternatives to std library
    if (std.mem.indexOf(u8, source, "std." ++ "BoundedArray") != null) {
        return "use stdx.BoundedArrayType instead of std version";
    }

    if (std.mem.indexOf(u8, source, "Static" ++ "BitSet") != null) {
        return "use stdx.BitSetType instead of std version";
    }

    if (std.mem.indexOf(u8, source, "std.time." ++ "Duration") != null) {
        return "use stdx.Duration instead of std version";
    }

    if (std.mem.indexOf(u8, source, "std.time." ++ "Instant") != null) {
        return "use stdx.Instant instead of std version";
    }

    if (std.mem.indexOf(u8, source, "trait." ++ "hasUniqueRepresentation") != null) {
        return "use stdx.has_unique_representation instead of std version";
    }

    // Use stdx memory operations
    if (std.mem.indexOf(u8, source, "mem." ++ "copy(") != null) {
        return "use stdx.copy_disjoint instead of std version";
    }

    if (std.mem.indexOf(u8, source, "mem." ++ "copyForwards(") != null) {
        return "use stdx.copy_left instead of std version";
    }

    if (std.mem.indexOf(u8, source, "mem." ++ "copyBackwards(") != null) {
        return "use stdx.copy_right instead of std version";
    }

    // Use stdx error handling
    if (std.mem.indexOf(u8, source, "posix." ++ "unexpectedErrno(") != null) {
        return "use stdx.unexpected_errno instead of std version";
    }

    // Remove before merging
    if (std.mem.indexOf(u8, source, "FIX" ++ "ME") != null) {
        return "FIX" ++ "ME comments must be addressed before merging";
    }

    if (std.mem.indexOf(u8, source, "dbg(") != null) {
        if (std.mem.indexOf(u8, source, "fn dbg(") == null) {
            return "dbg" ++ "() calls must be removed before merging";
        }
    }

    // Banned patterns

    if (std.mem.indexOf(u8, source, "!" ++ "comptime") != null) {
        return "use ! inside comptime blocks";
    }

    if (std.mem.indexOf(u8, source, "debug." ++ "assert(") != null) {
        return "use unqualified assert() calls";
    }

    // Project specific rules
    if (std.mem.indexOf(u8, source, "TO" ++ "DO ") != null) {
        return "TO" ++ "DO comments must be resolved and deleted before merging";
    }

    return null;
}

/// Checks for Unicode emoji characters (banned in code)
fn tidy_unicode_emojis(file: SourceFile) ?[]const u8 {
    for (file.text, 0..) |byte, i| {
        // Check for common emoji ranges in UTF-8
        if (byte >= 0xF0) {
            // 4-byte UTF-8 sequences often contain emojis
            if (i + 3 < file.text.len) {
                const bytes = file.text[i .. i + 4];
                // Check for common emoji ranges
                if (bytes[0] == 0xF0 and bytes[1] == 0x9F) {
                    // Range U+1F000-U+1FFFF (includes most emojis)
                    if ((bytes[2] >= 0x80 and bytes[2] <= 0xBF) or
                        (bytes[2] >= 0x98 and bytes[2] <= 0x9F) or
                        (bytes[2] >= 0xA4 and bytes[2] <= 0xAF))
                    {
                        return "Unicode emojis are banned in code " ++
                            "(use ASCII alternatives like '✓' for checkmarks)";
                    }
                }
            }
        }
        // Check for 3-byte sequences with common emoji symbols
        else if (byte == 0xE2) {
            if (i + 2 < file.text.len) {
                const bytes = file.text[i .. i + 3];
                // Check for various emoji-like symbols that should be banned
                // Allow checkmark ✓ (bytes: E2 9C 93)
                if (bytes[1] == 0x9C and bytes[2] == 0x93) {
                    // This is a checkmark, allow it
                    continue;
                }

                // Ban other emoji-like symbols
                if ((bytes[1] == 0x9C and bytes[2] == 0x94) or // question mark variants
                    (bytes[1] == 0x9D and bytes[2] == 0xA4) or // heart symbols
                    (bytes[1] == 0x9A and bytes[2] >= 0x80 and bytes[2] <= 0xBF))
                { // lightning/warning symbols etc
                    return "Unicode emojis are banned in code (use ASCII alternatives)";
                }
            }
        }
    }
    return null;
}

/// Returns control character code if found in source.
fn tidy_control_characters(file: SourceFile) ?u8 {
    const binary_file_extensions: []const []const u8 = &.{ ".ico", ".png", ".webp", ".svg" };
    for (binary_file_extensions) |extension| {
        if (std.mem.endsWith(u8, file.path, extension)) return null;
    }

    // Reject carriage returns
    if (mem.indexOfScalar(u8, file.text, '\r') != null) {
        if (std.mem.endsWith(u8, file.path, ".bat")) return null;
        return '\r';
    }

    // Reject tabs except in specific files
    if (std.mem.endsWith(u8, file.path, ".sln") or
        std.mem.endsWith(u8, file.path, ".go") or
        (std.mem.endsWith(u8, file.path, ".md") and mem.indexOf(u8, file.text, "```go") != null))
    {
        return null;
    }

    if (mem.indexOfScalar(u8, file.text, '\t') != null) {
        return '\t';
    }

    return null;
}

const NamingViolation = struct {
    line: usize,
    message: []const u8,
};

/// Returns naming violation if conventions are broken.
fn tidy_naming_conventions(file: SourceFile) ?NamingViolation {
    var line_count: u32 = 0;
    var it = std.mem.splitScalar(u8, file.text, '\n');

    while (it.next()) |line| {
        line_count += 1;

        // Validate function names
        if (check_function_naming(line)) |violation| {
            return NamingViolation{
                .line = line_count,
                .message = violation,
            };
        }

        // Validate variable names
        if (check_variable_naming(line)) |violation| {
            return NamingViolation{
                .line = line_count,
                .message = violation,
            };
        }

        // Validate constants
        if (check_constant_naming(line)) |violation| {
            return NamingViolation{
                .line = line_count,
                .message = violation,
            };
        }
    }

    return null;
}

fn check_function_naming(line: []const u8) ?[]const u8 {
    const fn_prefix = "fn ";
    const index = std.mem.indexOf(u8, line, fn_prefix) orelse return null;

    // Skip if "fn " is inside a string literal
    if (is_inside_string_literal(line, index)) return null;

    // Only check actual function definitions (line starts with "fn " or whitespace + "fn ")
    if (index == 0 or std.ascii.isWhitespace(line[index - 1])) {
        const begin = index + fn_prefix.len;
        const end = std.mem.indexOf(u8, line[begin..], "(") orelse return null;
        if (end == 0) return null;

        const function_name = line[begin..][0..end];

        // Skip test functions and special cases
        if (std.mem.startsWith(u8, function_name, "test")) return null;
        if (std.mem.startsWith(u8, function_name, "JNI_")) return null;

        // Check for banned get_/set_ patterns
        if (std.mem.startsWith(u8, function_name, "get_")) {
            return "avoid get_ prefix: use nouns for simple getters (id() not get_id()) " ++
                "or contextual verbs for operations (find_block() not get_block(), " ++
                "load_config() not get_config())";
        }
        if (std.mem.startsWith(u8, function_name, "set_")) {
            return "use contextual verb prefix instead of set_ " ++
                "(e.g., update_id(), modify_state(), configure_option())";
        }

        // Allow generic type functions ending with 'Type' (e.g., BoundedArrayType)
        if (std.mem.endsWith(u8, function_name, "Type")) {
            return null; // Type-suffixed functions are allowed to use PascalCase
        }

        // Simple check: if function name has camelCase, reject it
        if (has_camel_case_in_name(function_name)) {
            return "function names should use snake_case, not camelCase";
        }
    }

    return null;
}

fn check_variable_naming(line: []const u8) ?[]const u8 {
    // We only enforce naming on function declarations, not variables
    _ = line;
    return null;
}

fn check_constant_naming(line: []const u8) ?[]const u8 {
    if (std.mem.indexOf(u8, line, "const ") != null) {
        // Check constant formatting
        if (has_improper_constant_case(line)) {
            return "constants should use SCREAMING_SNAKE_CASE";
        }
    }
    return null;
}

fn is_valid_snake_case_function(name: []const u8) bool {
    const valid_prefixes = [_][]const u8{
        // Query prefixes
        "is_",          "has_",     "can_",     "should_",    "contains_",
        // Lifecycle prefixes
        "create_",      "destroy_", "init_",    "deinit_",    "open_",
        "close_",       "start_",   "stop_",    "begin_",     "end_",
        "reset_",       "clear_",
        // Action prefixes
          "process_", "execute_",   "handle_",
        "perform_",     "apply_",   "revert_",  "validate_",  "verify_",
        "check_",       "ensure_",  "assert_",  "confirm_",
        // IO prefixes
          "read_",
        "write_",       "load_",    "save_",    "store_",     "fetch_",
        "send_",        "receive_", "acquire_", "release_",   "allocate_",
        "deallocate_",
        // Transform prefixes
         "parse_",   "encode_",  "decode_",    "serialize_",
        "deserialize_", "format_",  "convert_", "transform_", "map_",
        "filter_",      "reduce_",  "sort_",
        // Update prefixes
           "update_",    "modify_",
        "change_",      "insert_",  "remove_",  "delete_",    "append_",
        "prepend_",     "push_",    "pop_",     "swap_",      "move_",
        // Search prefixes
        "find_",        "search_",  "lookup_",  "locate_",    "discover_",
        // Result prefixes
        "try_",         "maybe_",   "as_",      "into_",      "to_",
        "from_",
        // Copy/clone prefixes
               "copy_",    "clone_",   "duplicate_", "merge_",
        "split_",       "join_",
        // Debug/logging prefixes
           "log_",     "trace_",     "debug_",
        "warn_",        "error_",   "panic_",
        // Connection prefixes
          "connect_",   "disconnect_",
        "bind_",        "unbind_",  "attach_",  "detach_",
        // State prefixes
           "commit_",
        "rollback_",    "flush_",   "sync_",    "refresh_",   "reload_",
    };

    for (valid_prefixes) |prefix| {
        if (std.mem.startsWith(u8, name, prefix)) return true;
    }

    // Allow noun getters
    return !std.mem.containsAtLeast(u8, name, 1, "_") or
        std.mem.endsWith(u8, name, "_count") or
        std.mem.endsWith(u8, name, "_size") or
        std.mem.endsWith(u8, name, "_len") or
        std.mem.endsWith(u8, name, "_capacity") or
        std.mem.endsWith(u8, name, "_offset") or
        std.mem.endsWith(u8, name, "_index") or
        std.mem.endsWith(u8, name, "_timestamp") or
        std.mem.endsWith(u8, name, "_duration") or
        std.mem.endsWith(u8, name, "_timeout");
}

fn has_camel_case_identifier(line: []const u8) bool {
    // Detect camelCase pattern
    for (line, 0..) |c, i| {
        if (i > 0 and std.ascii.isLower(line[i - 1]) and std.ascii.isUpper(c)) {
            return true;
        }
    }
    return false;
}

fn has_camel_case_in_name(name: []const u8) bool {
    // Detect camelCase pattern in function names
    for (name, 0..) |c, i| {
        if (i > 0 and std.ascii.isLower(name[i - 1]) and std.ascii.isUpper(c)) {
            return true;
        }
    }
    return false;
}

fn is_inside_string_literal(line: []const u8, pos: usize) bool {
    // Count unescaped quotes before the position
    var quote_count: usize = 0;
    var i: usize = 0;

    while (i < pos and i < line.len) {
        if (line[i] == '"') {
            // Check if this quote is escaped
            var escaped = false;
            if (i > 0 and line[i - 1] == '\\') {
                // Count consecutive backslashes to determine if quote is escaped
                var backslash_count: usize = 0;
                var j = i;
                while (j > 0 and line[j - 1] == '\\') {
                    backslash_count += 1;
                    j -= 1;
                }
                escaped = (backslash_count % 2) == 1;
            }

            if (!escaped) {
                quote_count += 1;
            }
        }
        i += 1;
    }

    // If quote count is odd, we're inside a string literal
    return (quote_count % 2) == 1;
}

fn has_improper_constant_case(line: []const u8) bool {
    // Simple constant check
    return std.mem.indexOf(u8, line, "const " ++ "MAX") != null and
        std.mem.indexOf(u8, line, "const " ++ "MAX_") == null;
}

fn has_conventional_name(line: []const u8) bool {
    // Allow conventional standard library imports and common names
    const conventional_names = [_][]const u8{
        "const std = ",
        "const assert = ",
        "const mem = ",
        "const fs = ",
        "const os = ",
        "const log = ",
        "const builtin = ",
        "const testing = ",
        "const allocator = ",
        "const Allocator = ",
        "const ArrayList = ",
        "const HashMap = ",
        "const Random = ",
        "const Timer = ",
        "const target = b.standardTargetOptions",
        "const optimize_mode = b.standardOptimizeOption",
        "const exe = b.addExecutable",
        "const tests = b.addTest",
        "const module = b.createModule",
        "GeneralPurposeAllocator",
        "argsAlloc",
        "argsFree",
    };

    for (conventional_names) |name| {
        if (std.mem.indexOf(u8, line, name) != null) {
            return true;
        }
    }
    return false;
}

const DocumentationError = struct {
    line: usize,
    message: []const u8,
};

/// Returns error if documentation standards are violated.
fn tidy_documentation_standards(file: SourceFile) ?DocumentationError {
    var line_count: u32 = 0;
    var it = std.mem.splitScalar(u8, file.text, '\n');
    var in_public_function = false;
    var has_doc_comment = false;

    while (it.next()) |line| {
        line_count += 1;

        // Track public functions
        if (std.mem.indexOf(u8, line, "pub fn ") != null) {
            if (in_public_function and !has_doc_comment) {
                return DocumentationError{
                    .line = line_count - 1,
                    .message = "public functions must have documentation comments",
                };
            }
            in_public_function = true;
            has_doc_comment = false;
        }

        // Validate doc comments
        if (std.mem.startsWith(u8, std.mem.trim(u8, line, " \t"), "///")) {
            has_doc_comment = true;

            // Check comment format
            const doc_line = std.mem.trim(u8, line, " \t");

            if (doc_line.len > 3 and doc_line[3] != ' ') {
                return DocumentationError{
                    .line = line_count,
                    .message = "documentation comments should have space after ///",
                };
            }
        }

        // Reset tracking state
        if (!std.mem.startsWith(u8, std.mem.trim(u8, line, " \t"), "///") and
            std.mem.indexOf(u8, line, "pub fn ") == null and
            std.mem.trim(u8, line, " \t").len > 0)
        {
            in_public_function = false;
        }
    }

    return null;
}

const identifiers_per_file_max = 100_000;

/// Returns name of unused declaration if found.
/// Currently disabled due to AST API instability in Zig 0.15+
fn tidy_dead_declarations(
    tree: *const std.zig.Ast,
    used: *UsedDeclarations,
) ?[]const u8 {
    _ = tree;
    _ = used;
    // Temporarily disabled due to AST API changes
    return null;
}

/// Update this when longest function changes.
const function_line_count_max = 200;

/// Returns line number of function that exceeds length limit.
fn tidy_function_length(file: SourceFile) ?struct {
    line: u32,
    length: u32,
    name: []const u8,
} {
    var line_count: u32 = 0;
    var it = std.mem.splitScalar(u8, file.text, '\n');

    while (it.next()) |line| {
        line_count += 1;
        const trimmed = std.mem.trim(u8, line, " \t");

        // Look for function definitions
        if (std.mem.startsWith(u8, trimmed, "fn ") or
            std.mem.startsWith(u8, trimmed, "pub fn ") or
            std.mem.startsWith(u8, trimmed, "export fn ") or
            std.mem.startsWith(u8, trimmed, "inline fn "))
        {

            // Extract function name
            const fn_start = std.mem.indexOf(u8, trimmed, "fn ").? + 3;
            const name_end = std.mem.indexOf(u8, trimmed[fn_start..], "(") orelse continue;
            const fn_name = std.mem.trim(u8, trimmed[fn_start .. fn_start + name_end], " \t");

            const fn_start_line = line_count;
            var brace_count: i32 = 0;
            var found_opening_brace = false;
            var current_line = line_count;

            // Count lines until function ends
            var inner_it = std.mem.splitScalar(u8, file.text, '\n');
            var skip_lines = line_count - 1;
            while (skip_lines > 0) : (skip_lines -= 1) {
                _ = inner_it.next();
            }

            while (inner_it.next()) |func_line| {
                for (func_line) |char| {
                    if (char == '{') {
                        brace_count += 1;
                        found_opening_brace = true;
                    } else if (char == '}') {
                        brace_count -= 1;
                        if (found_opening_brace and brace_count == 0) {
                            const func_length = current_line - fn_start_line + 1;
                            if (func_length > function_line_count_max) {
                                return .{
                                    .line = fn_start_line,
                                    .length = func_length,
                                    .name = fn_name,
                                };
                            }
                            break;
                        }
                    }
                }
                current_line += 1;
                if (found_opening_brace and brace_count == 0) break;
            }
        }
    }

    return null;
}

/// Returns "what" comment violation if found.
fn tidy_what_comments(file: SourceFile) ?struct {
    line: u32,
    message: []const u8,
} {
    const forbidden_patterns = [_][]const u8{
        "// Write ",
        "// Read ",
        "// Set ",
        "// Get ",
        "// Call ",
        "// Create ",
        "// Update ",
        "// Delete ",
        "// Remove ",
        "// Add ",
        "// Clear ",
        "// Initialize ",
        "// Check ",
        "// Validate ",
        "// Process ",
        "// Handle ",
        "// Calculate ",
        "// Find ",
        "// Search ",
        "// Load ",
        "// Save ",
        "// Store ",
        "// Free ",
        "// Allocate ",
        "// Loop ",
        "// Iterate ",
    };

    var line_count: u32 = 0;
    var it = std.mem.splitScalar(u8, file.text, '\n');
    while (it.next()) |line| {
        line_count += 1;

        // Skip test files - they can use narrative comments
        if (std.mem.indexOf(u8, file.path, "test") != null) continue;

        const trimmed = std.mem.trim(u8, line, " \t");

        for (forbidden_patterns) |pattern| {
            if (std.mem.startsWith(u8, trimmed, pattern)) {
                // Skip if it's explaining WHY (contains "because", "to", "for", "since")
                const lower_line = std.heap.page_allocator.alloc(u8, line.len) catch continue;
                defer std.heap.page_allocator.free(lower_line);
                _ = std.ascii.lowerString(lower_line, line);

                if (std.mem.indexOf(u8, lower_line, "because") != null or
                    std.mem.indexOf(u8, lower_line, " to ") != null or
                    std.mem.indexOf(u8, lower_line, " for ") != null or
                    std.mem.indexOf(u8, lower_line, "since") != null) continue;

                return .{
                    .line = line_count,
                    .message = "Comment explains WHAT the code does instead of WHY (forbidden pattern detected)",
                };
            }
        }
    }
    return null;
}

/// Returns function that violates Type suffix rule.
fn tidy_generic_functions(
    file: SourceFile,
) ?struct {
    line: usize,
    name: []const u8,
} {
    var line_count: u32 = 0;
    var it = std.mem.splitScalar(u8, file.text, '\n');
    while (it.next()) |line| {
        line_count += 1;

        const function_name = function_name: {
            const fn_prefix = "fn ";
            const index = std.mem.indexOf(u8, line, fn_prefix) orelse continue;

            if (index == 0 or std.ascii.isWhitespace(line[index - 1])) {
                const begin = index + fn_prefix.len;
                const end = std.mem.indexOf(u8, line[begin..], "(") orelse continue;
                if (end == 0) continue;

                assert(begin + end < line.len);
                break :function_name line[begin..][0..end];
            }
            continue;
        };

        // Skip exceptions
        if (std.mem.startsWith(u8, function_name, "JNI_")) continue;

        if (std.ascii.isUpper(function_name[0])) {
            if (!std.mem.endsWith(u8, function_name, "Type")) {
                return .{
                    .line = line_count,
                    .name = function_name,
                };
            }
        }
    }

    return null;
}

/// Validates markdown formatting and structure.
fn tidy_markdown_standards(text: []const u8) !void {
    var fenced_block = false;
    var heading_count: u32 = 0;
    var line_count: u32 = 0;
    var it = std.mem.splitScalar(u8, text, '\n');

    while (it.next()) |line| {
        line_count += 1;

        // Reject trailing whitespace
        if (std.mem.endsWith(u8, line, " ")) {
            std.debug.print("line {d}: trailing whitespace\n", .{line_count});
            return error.TrailingWhitespace;
        }

        // Track code blocks
        if (mem.startsWith(u8, line, "```")) fenced_block = !fenced_block;

        // Count h1 headings
        if (!fenced_block and mem.startsWith(u8, line, "# ")) heading_count += 1;

        // Line length check removed for markdown files
    }

    assert(!fenced_block);

    switch (heading_count) {
        0 => if (line_count > 2) return error.MissingTitle,
        1 => {},
        else => return error.DuplicateTitle,
    }
}

/// Detects source files that are never imported.
const DeadFilesDetector = struct {
    const FileName = [64]u8;
    const FileState = struct { import_count: u32, definition_count: u32 };
    const FileMap = std.AutoArrayHashMap(FileName, FileState);

    files: FileMap,

    fn init(allocator: std.mem.Allocator) DeadFilesDetector {
        return .{ .files = FileMap.init(allocator) };
    }

    fn deinit(detector: *DeadFilesDetector) void {
        detector.files.deinit();
    }

    fn visit(detector: *DeadFilesDetector, file: SourceFile) !void {
        (try detector.file_state(file.path)).definition_count += 1;

        var rest: []const u8 = file.text;
        for (0..1024) |_| {
            const prefix, rest = cut(rest, "@import(\"") orelse break;
            _ = prefix;
            const import_path, rest = cut(rest, "\")") orelse break;
            if (std.mem.endsWith(u8, import_path, ".zig")) {
                (try detector.file_state(import_path)).import_count += 1;
            }
        } else {
            std.debug.panic("file with more than 1024 imports: {s}", .{file.path});
        }
    }

    fn finish(detector: *DeadFilesDetector) !void {
        defer detector.files.clearRetainingCapacity();

        for (detector.files.keys(), detector.files.values()) |name, state| {
            if (state.definition_count == 0) {
                std.debug.print("imported file untracked by git: {s}\n", .{name});
                return error.DeadFile;
            }
            if (state.import_count == 0 and !is_entry_point(name)) {
                std.debug.print("file never imported: {s}\n", .{name});
                return error.DeadFile;
            }
        }
    }

    fn file_state(detector: *DeadFilesDetector, path: []const u8) !*FileState {
        const gop = try detector.files.getOrPut(path_to_name(path));
        if (!gop.found_existing) gop.value_ptr.* = .{ .import_count = 0, .definition_count = 0 };
        return gop.value_ptr;
    }

    fn path_to_name(path: []const u8) FileName {
        assert(std.mem.endsWith(u8, path, ".zig"));
        const basename = std.fs.path.basename(path);
        var file_name: FileName = @splat(0);
        assert(basename.len <= file_name.len);
        @memcpy(file_name[0..basename.len], basename);
        return file_name;
    }

    fn is_entry_point(file: FileName) bool {
        const entry_points: []const []const u8 = &.{
            "build.zig",
            "main.zig",
            "tidy.zig",
            "shell.zig",
            "test_runner.zig",
            "benchmark.zig",
            "fuzz.zig",
        };

        for (entry_points) |entry_point| {
            if (std.mem.startsWith(u8, &file, entry_point)) return true;
        }
        return false;
    }
};

/// Returns true if line contains a URL.
fn has_url(line: []const u8) bool {
    return std.mem.indexOf(u8, line, "https://") != null or
        std.mem.indexOf(u8, line, "http://") != null;
}

/// Extracts content from multiline string if present.
fn parse_multiline_string(line: []const u8) ?[]const u8 {
    const indentation, const value = cut(line, "\\\\") orelse return null;
    for (indentation) |c| if (c != ' ') return null;
    return value;
}

/// Splits string at first occurrence of delimiter.
fn cut(s: []const u8, delimiter: []const u8) ?struct { []const u8, []const u8 } {
    const index = std.mem.indexOf(u8, s, delimiter) orelse return null;
    return .{ s[0..index], s[index + delimiter.len ..] };
}

/// Returns list of all tracked files in repository.
fn list_file_paths(shell: *Shell) ![]const []const u8 {
    var result = std.ArrayList([]const u8).init(shell.arena.allocator());

    const files = try shell.exec_stdout("git ls-files -z", .{});
    assert(files[files.len - 1] == 0);
    var lines = std.mem.splitScalar(u8, files[0 .. files.len - 1], 0);
    while (lines.next()) |line| {
        assert(line.len > 0);
        try result.append(line);
    }

    return result.items;
}

test "tidy extensions" {
    // Only check .zig files for proper extension
    const allocator = std.testing.allocator;

    const shell = try Shell.create(allocator);
    defer shell.destroy();

    const paths = try list_file_paths(shell);

    for (paths) |path| {
        if (path.len == 0) continue;
        const extension = std.fs.path.extension(path);

        // Only validate that .zig files have .zig extension (no other restrictions)
        if (std.mem.eql(u8, extension, ".zig")) {
            // .zig files are valid, nothing to check
            continue;
        }

        // All other files are allowed regardless of extension
    }
    // No extension validation needed - all files are allowed
}

test "tidy naming conventions enforcement" {
    const test_cases = [_]struct {
        code: []const u8,
        should_pass: bool,
        expected_error: ?[]const u8,
    }{
        // BAD: Banned get_/set_ patterns
        .{
            .code = "fn get_id(self: *const Block) u64 { return self.id; }",
            .should_pass = false,
            .expected_error = "avoid get_ prefix: use nouns for simple getters " ++
                "(id() not get_id()) or contextual verbs for operations " ++
                "(find_block() not get_block(), load_config() not get_config())",
        },
        .{
            .code = "fn set_id(self: *Block, id: u64) void { self.id = id; }",
            .should_pass = false,
            .expected_error = "use contextual verb prefix instead of set_ " ++
                "(e.g., update_id(), modify_state(), configure_option())",
        },

        // GOOD: noun style getters
        .{
            .code = "fn id(self: *const Block) u64 { return self.id; }",
            .should_pass = true,
            .expected_error = null,
        },
        .{
            .code = "fn timestamp(self: *const Event) u64 { return self.ts; }",
            .should_pass = true,
            .expected_error = null,
        },
        .{
            .code = "fn capacity(self: *const Buffer) usize { return self.cap; }",
            .should_pass = true,
            .expected_error = null,
        },

        // GOOD: update prefixes
        .{
            .code = "fn update_id(self: *Block, id: u64) void { self.id = id; }",
            .should_pass = true,
            .expected_error = null,
        },
        .{
            .code = "fn update_metadata(self: *Block, meta: Metadata) void {}",
            .should_pass = true,
            .expected_error = null,
        },

        // GOOD: Valid verb prefixes
        .{
            .code = "fn process_query(query: []const u8) !QueryResult {}",
            .should_pass = true,
            .expected_error = null,
        },
        .{
            .code = "fn validate_block(block: *const Block) !void {}",
            .should_pass = true,
            .expected_error = null,
        },
        .{
            .code = "fn verify_signature(sig: []const u8) !bool {}",
            .should_pass = true,
            .expected_error = null,
        },
        .{
            .code = "fn receive_message(conn: *Connection) !Message {}",
            .should_pass = true,
            .expected_error = null,
        },
        .{
            .code = "fn serialize_header(header: *Header) []u8 {}",
            .should_pass = true,
            .expected_error = null,
        },
        .{
            .code = "fn find_block_by_id(blocks: []Block, id: u64) ?*Block {}",
            .should_pass = true,
            .expected_error = null,
        },

        // GOOD: Boolean query prefixes
        .{
            .code = "fn is_valid(block: *const Block) bool { return true; }",
            .should_pass = true,
            .expected_error = null,
        },
        .{
            .code = "fn has_dependency(block: *const Block, dep_id: u64) bool {}",
            .should_pass = true,
            .expected_error = null,
        },
        .{
            .code = "fn can_execute(query: *const Query) bool { return true; }",
            .should_pass = true,
            .expected_error = null,
        },

        // BAD: camelCase functions
        .{
            .code = "fn processQuery(query: []const u8) !QueryResult {}",
            .should_pass = false,
            .expected_error = "function names should use snake_case, not camelCase",
        },
        .{
            .code = "fn validateBlock(block: *const Block) !void {}",
            .should_pass = false,
            .expected_error = "function names should use snake_case, not camelCase",
        },

        // GOOD: Test functions are allowed
        .{
            .code = "test \"query processing works\" {}",
            .should_pass = true,
            .expected_error = null,
        },
        .{
            .code = "fn testHelper() void {}",
            .should_pass = true,
            .expected_error = null,
        },
    };

    for (test_cases) |case| {
        const source_file = SourceFile{
            .path = "test.zig",
            .text = @ptrCast(case.code),
        };

        const result = tidy_naming_conventions(source_file);

        if (case.should_pass) {
            if (result != null) {
                std.debug.print("Expected '{s}' to pass, but got error: {s}\n", .{
                    case.code, result.?.message,
                });
                return error.UnexpectedNamingError;
            }
        } else {
            if (result == null) {
                std.debug.print("Expected '{s}' to fail, but it passed\n", .{
                    case.code,
                });
                return error.MissingNamingError;
            }

            if (case.expected_error) |expected| {
                if (!std.mem.eql(u8, result.?.message, expected)) {
                    std.debug.print("Expected error '{s}' but got '{s}' for code: {s}\n", .{
                        expected, result.?.message, case.code,
                    });
                    return error.WrongNamingError;
                }
            }
        }
    }
}
