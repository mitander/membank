//! Code quality and style enforcement for KausalDB.
//!
//! Rule-based checker with violation reporting
//! and systematic architectural constraint enforcement.

const tidy_main = @import("tidy/main.zig");
const violation = @import("tidy/violation.zig");
const rules = @import("tidy/rules.zig");
const parser = @import("tidy/parser.zig");

pub const main = tidy_main.main;

// Re-export test for build system compatibility
test "tidy" {
    // Run the new systematic tidy analysis in test mode
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    var summary = violation.ViolationSummary.init(allocator);
    defer summary.deinit();

    const file_paths = try discover_source_files(allocator);
    defer {
        for (file_paths) |path| allocator.free(path);
        allocator.free(file_paths);
    }

    var files_processed: u32 = 0;
    for (file_paths) |file_path| {
        if (!std.mem.endsWith(u8, file_path, ".zig")) continue;

        const source = read_file_content(allocator, file_path) catch |err| {
            std.debug.print("WARNING: Error reading {s}: {}\n", .{ file_path, err });
            continue;
        };
        defer allocator.free(source);

        try analyze_file(&summary, allocator, file_path, source);
        files_processed += 1;
    }

    if (summary.total_violations > 0) {
        summary.print_summary();
        summary.print_detailed_violations();
        return error.StyleViolations;
    }
}

const std = @import("std");
const fs = std.fs;
const mem = std.mem;

/// Analyze a single file against all KausalDB rules
fn analyze_file(
    summary: *violation.ViolationSummary,
    allocator: std.mem.Allocator,
    file_path: []const u8,
    source: []const u8,
) !void {
    // Parse source for semantic analysis
    const context = parser.parse_source(allocator, file_path, source) catch |err| {
        std.debug.print("WARNING: Parse error in {s}: {}\n", .{ file_path, err });
        return;
    };
    defer {
        allocator.free(context.functions);
        allocator.free(context.variables);
        allocator.free(context.imports);
    }

    // Apply all architectural rules
    for (rules.KAUSALDB_RULES) |rule| {
        const violations = rule.check_fn(@constCast(&context));
        defer allocator.free(violations);

        for (violations) |rule_violation| {
            try summary.add_violation(violation.Violation{
                .file_path = file_path,
                .line = rule_violation.line,
                .violation_type = rule.violation_type,
                .message = rule_violation.message,
                .context = rule_violation.context,
                .suggested_fix = rule_violation.suggested_fix,
            });
        }
    }
}

/// Discover all Zig source files in the project
fn discover_source_files(allocator: std.mem.Allocator) ![][]const u8 {
    var file_paths = std.ArrayList([]const u8).init(allocator);

    // Search key directories
    const search_dirs = [_][]const u8{ "src", "tests" };

    for (search_dirs) |dir| {
        discover_files_recursive(allocator, &file_paths, dir) catch |err| {
            if (err == error.FileNotFound) continue; // Directory might not exist
            return err;
        };
    }

    return file_paths.toOwnedSlice();
}

/// Recursively find all .zig files in a directory
fn discover_files_recursive(
    allocator: std.mem.Allocator,
    file_paths: *std.ArrayList([]const u8),
    dir_path: []const u8,
) !void {
    var dir = fs.cwd().openDir(dir_path, .{ .iterate = true }) catch return;
    defer dir.close();

    var iterator = dir.iterate();
    while (try iterator.next()) |entry| {
        if (mem.eql(u8, entry.name, ".") or mem.eql(u8, entry.name, "..")) continue;

        const full_path = try fs.path.join(allocator, &[_][]const u8{ dir_path, entry.name });

        switch (entry.kind) {
            .file => {
                if (mem.endsWith(u8, entry.name, ".zig")) {
                    try file_paths.append(full_path);
                } else {
                    allocator.free(full_path);
                }
            },
            .directory => {
                // Skip certain directories
                if (mem.eql(u8, entry.name, "zig-cache") or
                    mem.eql(u8, entry.name, "zig-out") or
                    mem.eql(u8, entry.name, ".git"))
                {
                    allocator.free(full_path);
                    continue;
                }

                discover_files_recursive(allocator, file_paths, full_path) catch {};
                allocator.free(full_path);
            },
            else => {
                allocator.free(full_path);
            },
        }
    }
}

/// Read entire file content into memory
fn read_file_content(allocator: std.mem.Allocator, file_path: []const u8) ![]u8 {
    const file = try fs.cwd().openFile(file_path, .{});
    defer file.close();

    const file_size = try file.getEndPos();
    const content = try allocator.alloc(u8, file_size);
    _ = try file.readAll(content);

    return content;
}
