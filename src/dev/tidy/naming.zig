//! Naming convention validation for Membank codebase.
//!
//! Enforces consistent function, variable, and constant naming patterns
//! following the established Membank style guide conventions.

const std = @import("std");
const mem = std.mem;

pub const NamingViolation = struct {
    line: u32,
    message: []const u8,
};

pub const SourceFile = struct { path: []const u8, text: [:0]const u8 };

/// Check naming conventions across an entire source file.
/// Returns first violation found, or null if all naming is correct.
pub fn check_naming_conventions(file: SourceFile) ?NamingViolation {
    var line_num: u32 = 1;
    var lines = mem.split(u8, file.text, "\n");

    while (lines.next()) |line| {
        defer line_num += 1;

        // Skip empty lines and comments
        const trimmed = mem.trim(u8, line, " \t");
        if (trimmed.len == 0 or mem.startsWith(u8, trimmed, "//")) {
            continue;
        }

        // Check function naming
        if (check_function_naming(line)) |violation| {
            return NamingViolation{
                .line = line_num,
                .message = violation,
            };
        }

        // Check variable naming
        if (check_variable_naming(line)) |violation| {
            return NamingViolation{
                .line = line_num,
                .message = violation,
            };
        }

        // Check constant naming
        if (check_constant_naming(line)) |violation| {
            return NamingViolation{
                .line = line_num,
                .message = violation,
            };
        }
    }

    return null;
}

/// Check function naming conventions.
/// Functions should use snake_case, with specific patterns for different types.
fn check_function_naming(line: []const u8) ?[]const u8 {
    const trimmed = mem.trim(u8, line, " \t");

    // Look for function definitions
    if (mem.startsWith(u8, trimmed, "pub fn ") or mem.startsWith(u8, trimmed, "fn ")) {
        const fn_start = if (mem.startsWith(u8, trimmed, "pub fn ")) 7 else 3;
        const remaining = trimmed[fn_start..];

        // Find function name (up to opening parenthesis)
        if (mem.indexOf(u8, remaining, "(")) |paren_pos| {
            const fn_name = mem.trim(u8, remaining[0..paren_pos], " \t");

            if (!is_valid_snake_case_function(fn_name)) {
                return "function names must use snake_case";
            }
        }
    }

    return null;
}

/// Check variable naming conventions.
/// Variables should use snake_case.
fn check_variable_naming(line: []const u8) ?[]const u8 {
    // Check for obvious camelCase patterns in variable contexts
    if (has_camel_case_identifier(line)) {
        return "variable names should use snake_case, not camelCase";
    }

    return null;
}

/// Check constant naming conventions.
/// Constants should use SCREAMING_SNAKE_CASE.
fn check_constant_naming(line: []const u8) ?[]const u8 {
    if (has_improper_constant_case(line)) {
        return "constants should use SCREAMING_SNAKE_CASE";
    }

    return null;
}

/// Validate snake_case function names.
/// Allows specific patterns like test names and common conventions.
fn is_valid_snake_case_function(name: []const u8) bool {
    if (name.len == 0) return false;

    // Special cases that are allowed
    if (mem.eql(u8, name, "main") or
        mem.eql(u8, name, "init") or
        mem.eql(u8, name, "deinit") or
        mem.startsWith(u8, name, "test_"))
    {
        return true;
    }

    // Check for conventional patterns that override snake_case
    if (has_conventional_name(name)) {
        return true;
    }

    // Standard snake_case validation
    var has_lower = false;
    var prev_was_underscore = false;

    for (name, 0..) |char, i| {
        if (i == 0 and char == '_') return false; // No leading underscore
        if (char == '_') {
            if (prev_was_underscore) return false; // No double underscores
            prev_was_underscore = true;
            continue;
        }
        prev_was_underscore = false;

        if (char >= 'A' and char <= 'Z') return false; // No uppercase
        if (char >= 'a' and char <= 'z') has_lower = true;
        if (!(char >= 'a' and char <= 'z') and !(char >= '0' and char <= '9')) {
            return false; // Only letters, numbers, underscores
        }
    }

    return has_lower; // Must have at least one lowercase letter
}

/// Check for camelCase identifiers in variable contexts.
/// Looks for patterns like someVariable but excludes string literals.
fn has_camel_case_identifier(line: []const u8) bool {
    var i: usize = 0;
    while (i < line.len) {
        if (is_inside_string_literal(line, i)) {
            i += 1;
            continue;
        }

        // Look for identifier start
        if ((line[i] >= 'a' and line[i] <= 'z') or line[i] == '_') {
            const start = i;
            while (i < line.len and ((line[i] >= 'a' and line[i] <= 'z') or
                (line[i] >= 'A' and line[i] <= 'Z') or
                (line[i] >= '0' and line[i] <= '9') or
                line[i] == '_'))
            {
                i += 1;
            }

            const identifier = line[start..i];
            if (has_camel_case_in_name(identifier)) {
                return true;
            }
        } else {
            i += 1;
        }
    }
    return false;
}

/// Check if a name contains camelCase pattern.
fn has_camel_case_in_name(name: []const u8) bool {
    if (name.len < 2) return false;

    var has_lower = false;
    for (name) |char| {
        if (char >= 'a' and char <= 'z') {
            has_lower = true;
        } else if (char >= 'A' and char <= 'Z' and has_lower) {
            return true; // Found uppercase after lowercase = camelCase
        }
    }
    return false;
}

/// Check if position is inside a string literal.
fn is_inside_string_literal(line: []const u8, pos: usize) bool {
    var in_string = false;
    var in_char = false;
    var escaped = false;

    for (line[0..pos]) |char| {
        if (escaped) {
            escaped = false;
            continue;
        }

        switch (char) {
            '\\' => escaped = true,
            '"' => if (!in_char) in_string = !in_string,
            '\'' => if (!in_string) in_char = !in_char,
            else => {},
        }
    }

    return in_string or in_char;
}

/// Check for improper constant case patterns.
fn has_improper_constant_case(line: []const u8) bool {
    // Look for const declarations
    if (mem.indexOf(u8, line, "const ")) |const_pos| {
        const after_const = line[const_pos + 6 ..];
        if (mem.indexOf(u8, after_const, " =")) |eq_pos| {
            const const_name = mem.trim(u8, after_const[0..eq_pos], " \t");

            // Skip if it's a type or function
            if (const_name.len == 0 or
                (const_name[0] >= 'A' and const_name[0] <= 'Z' and
                    !mem.containsAtLeast(u8, const_name, 1, "_")))
            {
                return false; // Likely a type name
            }

            // Check if it's a literal constant that should be SCREAMING_SNAKE_CASE
            if (has_lowercase_in_constant(const_name)) {
                return true;
            }
        }
    }

    return false;
}

/// Check if constant name has lowercase letters (should be all caps).
fn has_lowercase_in_constant(name: []const u8) bool {
    for (name) |char| {
        if (char >= 'a' and char <= 'z') {
            return true;
        }
    }
    return false;
}

/// Check for conventional names that override standard snake_case rules.
fn has_conventional_name(line: []const u8) bool {
    const conventional_patterns = [_][]const u8{
        "eql",          "hash",      "format",  "parse",                  "clone", "dupe",
        "toOwnedSlice", "ArrayList", "HashMap", "clearRetainingCapacity",
    };

    for (conventional_patterns) |pattern| {
        if (mem.indexOf(u8, line, pattern) != null) {
            return true;
        }
    }

    return false;
}
