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
        if (tidy_banned_patterns(source_file.text)) |err_msg| {
            std.debug.print("{s}: banned pattern: {s}\n", .{ file_path, err_msg });
            violations += 1;
        }

        if (try tidy_line_length(source_file)) |line_num| {
            std.debug.print("{s}: line {} exceeds length limit\n", .{ file_path, line_num });
            violations += 1;
        }

        if (tidy_control_characters(source_file)) |char| {
            std.debug.print("{s}: invalid control character: {c}\n", .{ file_path, char });
            violations += 1;
        }

        if (tidy_naming_conventions(source_file)) |violation| {
            std.debug.print("{s}: line {}: {s}\n", .{ file_path, violation.line, violation.message });
            violations += 1;
        }

        if (tidy_documentation_standards(source_file)) |doc_error| {
            std.debug.print("{s}: line {}: {s}\n", .{ file_path, doc_error.line, doc_error.message });
            violations += 1;
        }

        if (tidy_generic_functions(source_file)) |generic_fn| {
            std.debug.print("{s}: line {}: generic function detected: {s}\n", .{ file_path, generic_fn.line, generic_fn.name });
            violations += 1;
        }

        if (tidy_function_length(source_file)) |long_fn| {
            std.debug.print("{s}: line {}: function '{s}' is {} lines (max {})\n", .{ file_path, long_fn.line, long_fn.name, long_fn.length, function_line_count_max });
            violations += 1;
        }

        // Dead declaration detection temporarily disabled due to AST API changes
        // TODO: Restore when AST parsing is more reliable
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

    // TODO: Re-enable dead files detection when AST API is stable
    // var dead_files_detector = DeadFilesDetector.init(allocator);
    // defer dead_files_detector.deinit();

    var dead_declarations: UsedDeclarations = .{};
    defer dead_declarations.deinit(allocator);

    try dead_declarations.ensureTotalCapacity(allocator, identifiers_per_file_max);

    // TODO: Re-enable when AST-based function length checking is restored
    // var function_line_count_longest: usize = 0;

    // Stream through files once to perform all checks
    for (paths) |path| {
        const bytes_read = (try std.fs.cwd().readFile(path, buffer)).len;
        if (bytes_read >= buffer.len - 1) return error.FileTooLong;
        buffer[bytes_read] = 0;

        const source_file = SourceFile{ .path = path, .text = buffer[0..bytes_read :0] };

        if (tidy_control_characters(source_file)) |control_character| {
            std.debug.print(
                "{s} error: contains control character: code={} symbol='{c}'\n",
                .{ source_file.path, control_character, control_character },
            );
            return error.BannedControlCharacter;
        }

        if (mem.endsWith(u8, source_file.path, ".zig")) {
            if (tidy_banned_patterns(source_file.text)) |ban_reason| {
                std.debug.print(
                    "{s}: error: banned pattern, {s}\n",
                    .{ source_file.path, ban_reason },
                );
                return error.BannedPattern;
            }

            if (try tidy_line_length(source_file)) |line_index| {
                std.debug.print(
                    "{s}:{d} error: line exceeds 100 columns\n",
                    .{ source_file.path, line_index + 1 },
                );
                return error.LineTooLong;
            }

            if (tidy_naming_conventions(source_file)) |violation| {
                std.debug.print(
                    "{s}:{d} error: naming violation: {s}\n",
                    .{ source_file.path, violation.line, violation.message },
                );
                return error.NamingViolation;
            }

            // TODO: Re-enable dead declaration detection when AST API is stable
            _ = &dead_declarations;

            if (tidy_documentation_standards(source_file)) |doc_error| {
                std.debug.print(
                    "{s}:{d} error: documentation issue: {s}\n",
                    .{ source_file.path, doc_error.line, doc_error.message },
                );
                return error.DocumentationError;
            }

            // TODO: Re-enable function length checking when AST API is stable
            // function_line_count_longest = @max(function_line_count_longest, 0);

            if (tidy_generic_functions(source_file)) |function| {
                std.debug.print(
                    "{s}:{d} error: '{s}' should end with the 'Type' suffix\n",
                    .{
                        source_file.path,
                        function.line,
                        function.name,
                    },
                );
                return error.GenericFunctionWithoutType;
            }

            // TODO: Re-enable dead files detection when AST API is stable
        }

        if (mem.endsWith(u8, source_file.path, ".md")) {
            tidy_markdown_standards(source_file.text) catch |err| {
                std.debug.print(
                    "{s} error: markdown issue, {}\n",
                    .{ source_file.path, err },
                );
                return err;
            };
        }
    }

    // TODO: Re-enable when dead files detection is restored
    // try dead_files_detector.finish();

    // TODO: Re-enable when AST-based function length checking is restored
    // if (function_line_count_longest < function_line_count_max) {
    //     std.debug.print("error: `function_line_count_max` must be updated to {d}\n", .{
    //         function_line_count_longest,
    //     });
    //     return error.LineCountOutdated;
    // }
}

const SourceFile = struct { path: []const u8, text: [:0]const u8 };

/// Returns error message if source contains banned patterns.
fn tidy_banned_patterns(source: []const u8) ?[]const u8 {
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

    if (std.mem.indexOf(u8, source, "@memcpy(") != null) {
        if (std.mem.indexOf(u8, source, "// Bypass tidy's ban, for stdx.") == null) {
            return "use stdx.copy_disjoint instead of @memcpy";
        }
    }

    // Use stdx error handling
    if (std.mem.indexOf(u8, source, "posix." ++ "unexpectedErrno(") != null) {
        return "use stdx.unexpected_errno instead of std version";
    }

    // Use stdx PRNG
    if (std.mem.indexOf(u8, source, "uint" ++ "LessThan") != null or
        std.mem.indexOf(u8, source, "int" ++ "RangeLessThan") != null or
        std.mem.indexOf(u8, source, "int" ++ "RangeAtMost") != null or
        std.mem.indexOf(u8, source, "int" ++ "RangeAtMostBiased") != null)
    {
        return "use stdx.PRNG instead of std.Random";
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
    if (std.mem.indexOf(u8, source, "Self = " ++ "@This()") != null) {
        return "use explicit type names instead of Self";
    }

    if (std.mem.indexOf(u8, source, "!" ++ "comptime") != null) {
        return "use ! inside comptime blocks";
    }

    if (std.mem.indexOf(u8, source, "debug." ++ "assert(") != null) {
        return "use unqualified assert() calls";
    }

    // Project specific rules
    if (std.mem.indexOf(u8, source, "TODO:") != null) {
        // Allow TODO comments but not TODO: with colon (suggests urgency)
        return "use TODO without colon for general reminders";
    }

    return null;
}

/// Returns line index if any line exceeds 100 characters.
fn tidy_line_length(file: SourceFile) !?u32 {
    // Skip generated files
    if (std.mem.endsWith(u8, file.path, "test_vectors.zig")) return null;

    var line_iterator = mem.splitScalar(u8, file.text, '\n');
    var line_index: u32 = 0;
    while (line_iterator.next()) |line| : (line_index += 1) {
        const line_length = try std.unicode.utf8CountCodepoints(line);
        if (line_length > 100) {
            if (has_url(line)) continue;

            // Allow long test cases
            if (std.mem.indexOf(u8, line, "TestCase.init(") != null) continue;

            // Check multiline strings
            if (parse_multiline_string(line)) |string_value| {
                const string_value_length = try std.unicode.utf8CountCodepoints(string_value);
                if (string_value_length <= 100) continue;

                // Allow test data
                if (std.mem.endsWith(u8, file.path, "test_data.zig") and
                    (std.mem.startsWith(u8, string_value, " block B") or
                        std.mem.startsWith(u8, string_value, " query Q") or
                        std.mem.startsWith(u8, string_value, " result   ")))
                {
                    continue;
                }

                // Allow JSON snapshots
                if (std.mem.startsWith(u8, string_value, "{\"id\":") or
                    std.mem.startsWith(u8, string_value, "[{\"type\":"))
                {
                    continue;
                }
            }

            return line_index;
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

    if (index == 0 or std.ascii.isWhitespace(line[index - 1])) {
        const begin = index + fn_prefix.len;
        const end = std.mem.indexOf(u8, line[begin..], "(") orelse return null;
        if (end == 0) return null;

        const function_name = line[begin..][0..end];

        // Skip exceptions
        if (std.mem.startsWith(u8, function_name, "JNI_")) return null;
        if (std.mem.startsWith(u8, function_name, "test_")) return null;

        // PascalCase must end with Type
        if (std.ascii.isUpper(function_name[0])) {
            if (!std.mem.endsWith(u8, function_name, "Type")) {
                return "PascalCase functions must end with 'Type' suffix";
            }
        } else {
            // snake_case should follow verb_noun pattern
            if (!is_valid_snake_case_function(function_name)) {
                return "function names should use snake_case with verb_noun pattern";
            }
        }
    }

    return null;
}

fn check_variable_naming(line: []const u8) ?[]const u8 {
    // Reject camelCase variables
    if (std.mem.indexOf(u8, line, "const ") != null or std.mem.indexOf(u8, line, "var ") != null) {
        // Allow conventional std library names
        if (has_conventional_name(line)) {
            return null;
        }
        // Simple camelCase detection
        if (has_camel_case_identifier(line)) {
            return "variable names should use snake_case, not camelCase";
        }
    }
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
    // Allow common verb prefixes
    const valid_prefixes = [_][]const u8{
        "get_",    "set_",     "is_",    "has_",    "can_",      "should_",
        "create_", "destroy_", "init_",  "deinit_", "validate_", "process_",
        "handle_", "execute_", "parse_", "encode_", "decode_",   "serialize_",
        "try_",    "maybe_",   "as_",    "into_",   "to_",
    };

    for (valid_prefixes) |prefix| {
        if (std.mem.startsWith(u8, name, prefix)) return true;
    }

    // Allow noun getters
    return !std.mem.containsAtLeast(u8, name, 1, "_") or
        std.mem.endsWith(u8, name, "_count") or
        std.mem.endsWith(u8, name, "_size") or
        std.mem.endsWith(u8, name, "_len");
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

fn has_improper_constant_case(line: []const u8) bool {
    // Simple constant check
    return std.mem.indexOf(u8, line, "const MAX") != null and
        std.mem.indexOf(u8, line, "const MAX_") == null;
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
            if (doc_line.len == 3) { // Just "///"
                return DocumentationError{
                    .line = line_count,
                    .message = "documentation comments should not be empty",
                };
            }

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
/// TODO: Restore when AST API is stable in Zig 0.15+
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

        // Check line length
        const line_length = std.unicode.utf8CountCodepoints(line) catch line.len;
        if (line_length > 100 and !has_url(line)) {
            std.debug.print("line {d}: exceeds 100 columns\n", .{line_count});
            return error.LineTooLong;
        }
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

test "tidy changelog" {
    const allocator = std.testing.allocator;

    const changelog_size_max = 1024 * 1024;
    const changelog = try fs.cwd().readFileAlloc(allocator, "CHANGELOG.md", changelog_size_max);
    defer allocator.free(changelog);

    var line_iterator = mem.splitScalar(u8, changelog, '\n');
    var line_index: usize = 0;
    while (line_iterator.next()) |line| : (line_index += 1) {
        if (std.mem.endsWith(u8, line, " ")) {
            std.debug.print("CHANGELOG.md:{d} trailing whitespace", .{line_index + 1});
            return error.TrailingWhitespace;
        }
        const line_length = try std.unicode.utf8CountCodepoints(line);
        if (line_length > 100 and !has_url(line)) {
            std.debug.print("CHANGELOG.md:{d} line exceeds 100 columns\n", .{line_index + 1});
            return error.LineTooLong;
        }
    }
}

test "tidy extensions" {
    const allowed_extensions = std.StaticStringMap(void).initComptime(.{
        .{".c"},  .{".css"},  .{".go"},  .{".h"},   .{".html"}, .{".java"},
        .{".js"}, .{".json"}, .{".md"},  .{".py"},  .{".rs"},   .{".toml"},
        .{".ts"}, .{".txt"},  .{".xml"}, .{".yml"}, .{".zig"},  .{".zon"},
    });

    const exceptions = std.StaticStringMap(void).initComptime(.{
        .{".editorconfig"},
        .{".gitignore"},
        .{"LICENSE"},
        .{"README.md"},
        .{"CLAUDE.md"},
        .{"TODO.md"},
    });

    const allocator = std.testing.allocator;
    const shell = try Shell.create(allocator);
    defer shell.destroy();

    const paths = try list_file_paths(shell);

    var bad_extension = false;
    for (paths) |path| {
        if (path.len == 0) continue;
        const extension = std.fs.path.extension(path);
        if (!allowed_extensions.has(extension)) {
            const basename = std.fs.path.basename(path);
            if (!exceptions.has(basename) and !exceptions.has(path)) {
                std.debug.print("bad extension: {s}\n", .{path});
                bad_extension = true;
            }
        }
    }
    if (bad_extension) return error.BadExtension;
}
