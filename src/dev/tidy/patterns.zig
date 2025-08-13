//! Banned pattern detection for KausalDB codebase.
//!
//! Enforces architectural constraints by detecting patterns that violate
//! single-threaded design, arena-per-subsystem memory model, and explicit
//! error handling principles. Prevents subtle bugs and maintains performance.
//!
//! Safety: This module performs static analysis and must be careful about
//! false positives. All pattern matches should be precise and documented.

const std = @import("std");
const mem = std.mem;
const kausaldb = @import("kausaldb");

const stdx = kausaldb.stdx;

/// Check source code for patterns that violate KausalDB's architectural principles.
/// Returns violation message or null if code adheres to standards.
pub fn check_banned_patterns(file_path: []const u8, source: []const u8) ?[]const u8 {
    if (mem.endsWith(u8, file_path, "test.zig") or
        mem.endsWith(u8, file_path, "_test.zig") or
        mem.indexOf(u8, file_path, "/tests/") != null)
    {
        return check_test_patterns(source);
    }

    return check_banned_patterns_production(source);
}

/// Check banned patterns in production code (strict).
/// Check if there's proper capacity management near an ArrayList append operation
/// This helps reduce false positives for performance warnings
fn has_capacity_management_nearby(source: []const u8) bool {
    const capacity_patterns = [_][]const u8{
        "ensureTotalCapacity",
        "ensureUnusedCapacity",
        "ensureTotalCapacityPrecise",
        "resize",
        "// CAPACITY_MANAGED:",
    };

    for (capacity_patterns) |pattern| {
        if (mem.indexOf(u8, source, pattern) != null) {
            return true;
        }
    }

    if (mem.indexOf(u8, source, "test ") != null or
        mem.indexOf(u8, source, "test_") != null)
    {
        return true;
    }

    return false;
}

/// Check banned patterns in production code (strict).
fn check_banned_patterns_production(source: []const u8) ?[]const u8 {
    if (mem.indexOf(u8, source, "malloc") != null) {
        return "use Zig allocators instead of malloc";
    }
    if (mem.indexOf(u8, source, "free(") != null) {
        return "use Zig allocators instead of free()";
    }
    if (mem.indexOf(u8, source, "realloc") != null) {
        return "use Zig allocators instead of realloc";
    }

    if (mem.indexOf(u8, source, ".unwrap()") != null) {
        return "use proper error handling instead of .unwrap()";
    }
    if (mem.indexOf(u8, source, "try " ++ "unreachable") != null) {
        return "use proper error handling instead of 'try' + 'unreachable'";
    }
    if (mem.indexOf(u8, source, "catch " ++ "unreachable") != null and
        mem.indexOf(u8, source, "// Safety:") == null)
    {
        return "use 'catch unreachable' only with safety comment explaining why it's safe";
    }

    if ((mem.indexOf(u8, source, "@ptrCast") != null or
        mem.indexOf(u8, source, "@intToPtr") != null or
        mem.indexOf(u8, source, "@alignCast") != null) and
        mem.indexOf(u8, source, "// Safety:") == null)
    {
        return "unsafe operations require safety comments explaining invariants";
    }

    if (mem.indexOf(u8, source, "std.Thread.spawn") != null and // tidy:ignore-arch - pattern detection for architecture compliance
        mem.indexOf(u8, source, "// ALLOW: direct thread spawn") == null)
    {
        return "use stdx.ThreadPool or other coordinated concurrency patterns instead of raw thread spawning";
    }

    if (mem.indexOf(u8, source, "std.atomic") != null and // tidy:ignore-arch - pattern detection for architecture compliance
        mem.indexOf(u8, source, "// ALLOW: direct atomic") == null and
        mem.indexOf(u8, source, "single_threaded") == null and
        mem.indexOf(u8, source, "stdx.MetricsCounter") == null and
        mem.indexOf(u8, source, "stdx.Protected") == null)
    {
        return "use stdx coordination primitives (MetricsCounter, Protected) instead of direct atomics";
    }

    if (mem.indexOf(u8, source, "std.Thread.Mutex") != null and // tidy:ignore-arch - pattern detection for architecture compliance
        mem.indexOf(u8, source, "// ALLOW: direct mutex") == null and
        mem.indexOf(u8, source, "stdx.Protected") == null)
    {
        return "use stdx.Protected wrapper instead of raw std.Thread.Mutex for better safety";
    }

    if (mem.indexOf(u8, source, "std.ArrayList.append") != null and
        mem.indexOf(u8, source, "ensureCapacity") == null and
        mem.indexOf(u8, source, "// ALLOW: append without ensureCapacity") == null)
    {
        if (!has_capacity_management_nearby(source)) {
            return "use ensureCapacity before append in hot paths or add ALLOW comment";
        }
    }

    if (mem.indexOf(u8, source, "std.HashMap") != null and
        mem.indexOf(u8, source, "arena") == null and
        mem.indexOf(u8, source, "Arena") == null)
    {
        return check_hashmap_usage(source);
    }

    if (mem.indexOf(u8, source, "std.debug.print") != null) {
        return "use proper logging instead of std.debug.print in production code";
    }
    if (mem.indexOf(u8, source, "std.process.exit") != null) {
        return "use proper error propagation instead of std.process.exit";
    }

    if (mem.indexOf(u8, source, "*anyopaque") != null and
        mem.indexOf(u8, source, "vtable") == null)
    {
        return "avoid type erasure without clear vtable pattern";
    }

    if (mem.indexOf(u8, source, "std.fmt.allocPrint") != null and
        mem.indexOf(u8, source, "defer") == null)
    {
        return check_allocprint_usage(source);
    }

    return null;
}

/// Check for unsafe patterns with insufficient safety comments
/// Returns an error message if unsafe operations are found without proper safety documentation
fn check_unsafe_patterns(source: []const u8) ?[]const u8 {
    const unsafe_patterns = [_]struct {
        pattern: []const u8,
        description: []const u8,
    }{
        .{ .pattern = "@ptrCast", .description = "@ptrCast requires a safety comment explaining type safety" },
        .{ .pattern = "@intToPtr", .description = "@intToPtr requires a safety comment explaining pointer validity" },
        .{ .pattern = "@alignCast", .description = "@alignCast requires a safety comment explaining alignment guarantees" },
        .{ .pattern = "@as", .description = "@as with pointer types requires a safety comment explaining type safety" },
    };

    for (unsafe_patterns) |pattern| {
        if (mem.indexOf(u8, source, pattern.pattern) != null and
            mem.indexOf(u8, source, "// Safety:") == null and
            mem.indexOf(u8, source, "// ALLOW: ") == null)
        {
            return pattern.description;
        }
    }

    return null;
}

/// Check banned patterns in test code (more lenient).
/// Check for patterns that should be avoided in tests
/// Tests have more relaxed rules but still need to follow some best practices
fn check_test_patterns(source: []const u8) ?[]const u8 {
    if (mem.indexOf(u8, source, "std.fmt.allocPrint") != null and
        mem.indexOf(u8, source, "defer") == null and
        mem.indexOf(u8, source, "testing.allocator") != null)
    {
        return check_allocprint_usage(source);
    }

    if (mem.indexOf(u8, source, "while (true)") != null and
        mem.indexOf(u8, source, "break") == null)
    {
        return "infinite loops in tests must have break conditions";
    }

    return null;
}

/// Check ArrayList usage for capacity management.
fn check_arraylist_usage(source: []const u8) ?[]const u8 {
    var lines = mem.split(u8, source, "\n");
    var line_num: u32 = 0;
    var has_capacity_management = false;

    while (lines.next()) |line| {
        line_num += 1;

        if (mem.indexOf(u8, line, "ensureCapacity") != null or
            mem.indexOf(u8, line, "ensureTotalCapacity") != null or
            mem.indexOf(u8, line, "ArrayList.initCapacity") != null)
        {
            has_capacity_management = true;
        }

        if (mem.indexOf(u8, line, "while") != null or
            mem.indexOf(u8, line, "for") != null)
        {
            if (mem.indexOf(u8, line, ".append(") != null and !has_capacity_management) {
                return "use ensureCapacity before ArrayList.append in loops";
            }
        }
    }

    return null;
}

/// Check HashMap usage for arena allocation patterns.
fn check_hashmap_usage(source: []const u8) ?[]const u8 {
    if (mem.indexOf(u8, source, "HashMap.init(allocator)") != null) {
        if (mem.indexOf(u8, source, "memtable") != null or
            mem.indexOf(u8, source, "index") != null or
            mem.indexOf(u8, source, "cache") != null)
        {
            return "use arena allocator for HashMap in memtable/index/cache contexts";
        }
    }

    return null;
}

/// Check allocPrint usage for proper cleanup.
fn check_allocprint_usage(source: []const u8) ?[]const u8 {
    var lines = mem.split(u8, source, "\n");
    var allocprint_line: ?u32 = null;
    var has_defer = false;
    var line_num: u32 = 0;

    while (lines.next()) |line| {
        line_num += 1;

        if (mem.indexOf(u8, line, "std.fmt.allocPrint") != null) {
            allocprint_line = line_num;
            has_defer = false; // Reset for this allocPrint
        }

        if (allocprint_line != null and
            mem.indexOf(u8, line, "defer") != null and
            (mem.indexOf(u8, line, ".free(") != null or mem.indexOf(u8, line, "allocator.free") != null))
        {
            has_defer = true;
        }

        if (allocprint_line != null and line_num > allocprint_line + 5 and !has_defer) {
            return "std.fmt.allocPrint must be followed by defer allocator.free()";
        }
    }

    if (allocprint_line != null and !has_defer) {
        return "std.fmt.allocPrint must be followed by defer allocator.free()";
    }

    return null;
}

/// Check for Unicode emoji usage (should be ASCII-only).
pub fn check_unicode_emojis(source: []const u8) ?[]const u8 {
    for (source, 0..) |byte, i| {
        if (byte >= 0x80) { // Non-ASCII
            if (i + 2 < source.len) {
                const next_bytes = source[i .. i + 3];
                if (mem.startsWith(u8, next_bytes, "\xF0\x9F")) {
                    return "avoid Unicode emojis in source code - use ASCII only";
                }
            }
        }
    }
    return null;
}

/// Check for control characters that could cause issues.
pub fn check_control_characters(source: []const u8) ?u8 {
    for (source) |char| {
        if (char == '\n' or char == '\r' or char == '\t' or char == ' ') {
            continue;
        }

        if (char < 32 or char == 127) {
            return char;
        }
    }
    return null;
}
