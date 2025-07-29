//! Glob pattern matcher for file path filtering
//!
//! Supports common glob patterns for include/exclude file matching
//! with performance optimized for typical repository scanning patterns.

const std = @import("std");
const assert = @import("../core/assert.zig").assert;

const GlobError = error{
    InvalidPattern,
    OutOfMemory,
};

/// Glob pattern matcher supporting *, **, ?, and basic character classes
pub const GlobMatcher = struct {
    pattern: []const u8,
    allocator: std.mem.Allocator,

    pub fn init(allocator: std.mem.Allocator, pattern: []const u8) GlobError!GlobMatcher {
        return GlobMatcher{
            .pattern = try allocator.dupe(u8, pattern),
            .allocator = allocator,
        };
    }

    pub fn deinit(self: *GlobMatcher) void {
        self.allocator.free(self.pattern);
    }

    /// Check if path matches the glob pattern
    pub fn matches(self: *const GlobMatcher, path: []const u8) bool {
        return match_recursive(self.pattern, path);
    }
};

/// Create glob matcher and test path in one operation for simple cases
pub fn matches_pattern(pattern: []const u8, path: []const u8) bool {
    return match_recursive(pattern, path);
}

/// Recursive glob matching implementation
/// Uses recursive descent for readable pattern decomposition
fn match_recursive(pattern: []const u8, text: []const u8) bool {
    if (pattern.len == 0) return text.len == 0;
    if (text.len == 0) return pattern_matches_empty(pattern);

    switch (pattern[0]) {
        '*' => {
            if (pattern.len >= 2 and pattern[1] == '*') {
                // Handle ** (recursive wildcard)
                return match_double_star(pattern, text);
            } else {
                // Handle * (single-level wildcard)
                return match_single_star(pattern, text);
            }
        },
        '?' => {
            // Single character wildcard
            return match_recursive(pattern[1..], text[1..]);
        },
        '[' => {
            // Character class matching
            if (find_closing_bracket(pattern)) |close_pos| {
                const char_class = pattern[1..close_pos];
                if (matches_char_class(text[0], char_class)) {
                    return match_recursive(pattern[close_pos + 1 ..], text[1..]);
                }
                return false;
            } else {
                // Invalid pattern - treat [ as literal
                return pattern[0] == text[0] and match_recursive(pattern[1..], text[1..]);
            }
        },
        else => {
            // Literal character matching
            return pattern[0] == text[0] and match_recursive(pattern[1..], text[1..]);
        },
    }
}

/// Handle ** recursive wildcard matching
/// ** matches zero or more path components including separators
fn match_double_star(pattern: []const u8, text: []const u8) bool {
    assert(pattern.len >= 2 and pattern[0] == '*' and pattern[1] == '*');

    // Skip the **
    var remaining_pattern = pattern[2..];

    // If ** is followed by /, skip the / too
    if (remaining_pattern.len > 0 and remaining_pattern[0] == '/') {
        remaining_pattern = remaining_pattern[1..];
    }

    // ** at end of pattern matches everything
    if (remaining_pattern.len == 0) return true;

    // Try matching at each position in text
    for (0..text.len + 1) |i| {
        if (match_recursive(remaining_pattern, text[i..])) {
            return true;
        }
    }

    return false;
}

/// Handle * single-level wildcard matching
/// * matches zero or more characters but stops at path separators
fn match_single_star(pattern: []const u8, text: []const u8) bool {
    assert(pattern.len >= 1 and pattern[0] == '*');

    const remaining_pattern = pattern[1..];

    // * at end of pattern matches rest of current path component
    if (remaining_pattern.len == 0) {
        return std.mem.indexOf(u8, text, "/") == null;
    }

    // Try matching * with 0 to N characters (but not across path separators)
    var i: usize = 0;
    while (i <= text.len) : (i += 1) {
        if (match_recursive(remaining_pattern, text[i..])) {
            return true;
        }

        // Stop at path separator - * cannot match across path components
        if (i < text.len and text[i] == '/') break;
    }

    return false;
}

/// Check if pattern can match empty string
/// Used for handling end-of-text conditions
fn pattern_matches_empty(pattern: []const u8) bool {
    if (pattern.len == 0) return true;

    switch (pattern[0]) {
        '*' => {
            if (pattern.len >= 2 and pattern[1] == '*') {
                // ** can match empty
                var remaining = pattern[2..];
                if (remaining.len > 0 and remaining[0] == '/') {
                    remaining = remaining[1..];
                }
                return pattern_matches_empty(remaining);
            } else {
                // * can match empty
                return pattern_matches_empty(pattern[1..]);
            }
        },
        else => return false,
    }
}

/// Find closing bracket for character class
fn find_closing_bracket(pattern: []const u8) ?usize {
    assert(pattern.len > 0 and pattern[0] == '[');

    var i: usize = 1;
    var first = true;

    while (i < pattern.len) : (i += 1) {
        if (pattern[i] == ']' and !first) {
            return i;
        }
        first = false;
    }

    return null;
}

/// Check if character matches character class pattern
/// Supports ranges (a-z), negation (!abc or ^abc), and literal chars
fn matches_char_class(char: u8, char_class: []const u8) bool {
    if (char_class.len == 0) return false;

    var negated = false;
    var class_chars = char_class;

    // Handle negation
    if (class_chars[0] == '!' or class_chars[0] == '^') {
        negated = true;
        class_chars = class_chars[1..];
    }

    var matched = false;
    var i: usize = 0;

    while (i < class_chars.len) {
        if (i + 2 < class_chars.len and class_chars[i + 1] == '-') {
            // Range matching (a-z)
            const start = class_chars[i];
            const end = class_chars[i + 2];
            if (char >= start and char <= end) {
                matched = true;
                break;
            }
            i += 3;
        } else {
            // Literal character matching
            if (char == class_chars[i]) {
                matched = true;
                break;
            }
            i += 1;
        }
    }

    return if (negated) !matched else matched;
}

test "basic wildcard patterns" {
    const testing = std.testing;

    // Basic * patterns
    try testing.expect(matches_pattern("*.zig", "main.zig"));
    try testing.expect(matches_pattern("*.zig", "test.zig"));
    try testing.expect(!matches_pattern("*.zig", "main.rs"));
    try testing.expect(!matches_pattern("*.zig", "subdir/main.zig"));

    // Basic ** patterns
    try testing.expect(matches_pattern("**/*.zig", "main.zig"));
    try testing.expect(matches_pattern("**/*.zig", "src/main.zig"));
    try testing.expect(matches_pattern("**/*.zig", "src/parser/lexer.zig"));
    try testing.expect(!matches_pattern("**/*.zig", "main.rs"));

    // ? wildcard
    try testing.expect(matches_pattern("test?.zig", "test1.zig"));
    try testing.expect(matches_pattern("test?.zig", "testa.zig"));
    try testing.expect(!matches_pattern("test?.zig", "test.zig"));
    try testing.expect(!matches_pattern("test?.zig", "test12.zig"));
}

test "character class patterns" {
    const testing = std.testing;

    // Basic character classes
    try testing.expect(matches_pattern("test[123].zig", "test1.zig"));
    try testing.expect(matches_pattern("test[123].zig", "test2.zig"));
    try testing.expect(!matches_pattern("test[123].zig", "test4.zig"));

    // Character ranges
    try testing.expect(matches_pattern("test[a-z].zig", "testa.zig"));
    try testing.expect(matches_pattern("test[a-z].zig", "testz.zig"));
    try testing.expect(!matches_pattern("test[a-z].zig", "testA.zig"));
    try testing.expect(!matches_pattern("test[a-z].zig", "test1.zig"));

    // Negated character classes
    try testing.expect(matches_pattern("test[!0-9].zig", "testa.zig"));
    try testing.expect(!matches_pattern("test[!0-9].zig", "test1.zig"));

    try testing.expect(matches_pattern("test[^abc].zig", "testd.zig"));
    try testing.expect(!matches_pattern("test[^abc].zig", "testa.zig"));
}

test "complex patterns" {
    const testing = std.testing;

    // Combining patterns
    try testing.expect(matches_pattern("src/**/*.zig", "src/main.zig"));
    try testing.expect(matches_pattern("src/**/*.zig", "src/parser/lexer.zig"));
    try testing.expect(matches_pattern("src/**/*.zig", "src/deep/nested/path/file.zig"));
    try testing.expect(!matches_pattern("src/**/*.zig", "tests/main.zig"));

    // Multiple wildcards
    try testing.expect(matches_pattern("**/test*.zig", "tests/test_main.zig"));
    try testing.expect(matches_pattern("**/test*.zig", "src/deep/test_parser.zig"));

    // Path component boundaries
    try testing.expect(matches_pattern("src/*/main.zig", "src/core/main.zig"));
    try testing.expect(!matches_pattern("src/*/main.zig", "src/core/parser/main.zig"));
}

test "edge cases" {
    const testing = std.testing;

    // Empty patterns and paths
    try testing.expect(matches_pattern("", ""));
    try testing.expect(!matches_pattern("", "something"));
    try testing.expect(!matches_pattern("something", ""));

    // Only wildcards
    try testing.expect(matches_pattern("*", "anything"));
    try testing.expect(!matches_pattern("*", "path/with/slashes"));
    try testing.expect(matches_pattern("**", "anything/including/slashes"));

    // Patterns ending with wildcards
    try testing.expect(matches_pattern("prefix*", "prefix"));
    try testing.expect(matches_pattern("prefix*", "prefixsuffix"));
    try testing.expect(matches_pattern("prefix**", "prefix/deep/path"));

    // Invalid character class (treat as literal)
    try testing.expect(matches_pattern("test[.zig", "test[.zig"));
    try testing.expect(!matches_pattern("test[.zig", "test1.zig"));
}

test "glob matcher lifecycle" {
    const testing = std.testing;
    const allocator = testing.allocator;

    var matcher = try GlobMatcher.init(allocator, "src/**/*.zig");
    defer matcher.deinit();

    try testing.expect(matcher.matches("src/main.zig"));
    try testing.expect(matcher.matches("src/parser/lexer.zig"));
    try testing.expect(!matcher.matches("tests/main.zig"));
}
