//! Violation tracking and reporting for tidy checks.
//!
//! Collects all code quality violations and presents
//! summaries to enable fixing multiple issues rather
//! than the traditional stop-at-first-error approach.

const std = @import("std");

pub const ViolationType = enum {
    naming_convention,
    banned_pattern,
    documentation_standard,
    function_length,
    generic_function,
    control_character,
    unicode_emoji,
    memory_management,
    error_handling,
    architecture,
    performance,
    comment_quality,
    import_pattern,
};

pub const Violation = struct {
    file_path: []const u8,
    line: u32,
    column: ?u32 = null,
    violation_type: ViolationType,
    message: []const u8,
    context: ?[]const u8 = null, // Code snippet showing the issue
    suggested_fix: ?[]const u8 = null,
};

pub const ViolationSummary = struct {
    total_violations: u32,
    violations_by_type: std.EnumMap(ViolationType, u32),
    violations_by_file: std.StringHashMap(u32),
    all_violations: std.array_list.Managed(Violation),

    pub fn init(allocator: std.mem.Allocator) ViolationSummary {
        return ViolationSummary{
            .total_violations = 0,
            .violations_by_type = std.EnumMap(ViolationType, u32).init(.{}),
            .violations_by_file = std.StringHashMap(u32).init(allocator),
            .all_violations = std.array_list.Managed(Violation).init(allocator),
        };
    }

    pub fn deinit(self: *ViolationSummary) void {
        self.violations_by_file.deinit();
        self.all_violations.deinit();
    }

    /// Add a violation to this summary and update all tracking counters
    ///
    /// Updates the total count, violations by type, and violations by file.
    /// Used for building tidy reports.
    pub fn add_violation(self: *ViolationSummary, violation: Violation) !void {
        try self.all_violations.append(violation);
        self.total_violations += 1;

        const current_type_count = self.violations_by_type.get(violation.violation_type) orelse 0;
        self.violations_by_type.put(violation.violation_type, current_type_count + 1);

        const current_file_count = self.violations_by_file.get(violation.file_path) orelse 0;
        try self.violations_by_file.put(violation.file_path, current_file_count + 1);
    }

    /// Print a formatted summary of all violations to stdout
    ///
    /// Displays violation counts by type and by file with clear formatting.
    /// Provides the main tidy output that developers see.
    pub fn print_summary(self: *const ViolationSummary) void {
        if (self.total_violations == 0) {
            std.debug.print("No tidy violations found!\n\n", .{});
            return;
        }

        std.debug.print("TIDY VIOLATION SUMMARY\n", .{});
        std.debug.print("=======================\n", .{});
        std.debug.print("Total violations: {d}\n\n", .{self.total_violations});

        std.debug.print("BY VIOLATION TYPE:\n", .{});

        const all_types = [_]ViolationType{
            .naming_convention,
            .banned_pattern,
            .documentation_standard,
            .function_length,
            .generic_function,
            .control_character,
            .unicode_emoji,
            .memory_management,
            .error_handling,
            .architecture,
            .performance,
        };

        for (all_types) |violation_type| {
            const count = self.violations_by_type.get(violation_type) orelse 0;
            if (count > 0) {
                std.debug.print("  {s}: {d}\n", .{ format_violation_type(violation_type), count });
            }
        }

        std.debug.print("\nBY FILE (most problematic first):\n", .{});
        var file_list = std.array_list.Managed(struct { []const u8, u32 }).init(std.heap.page_allocator);
        defer file_list.deinit();

        var file_iter = self.violations_by_file.iterator();
        while (file_iter.next()) |entry| {
            file_list.append(.{ entry.key_ptr.*, entry.value_ptr.* }) catch continue;
        }

        std.sort.insertion(@TypeOf(file_list.items[0]), file_list.items, {}, struct {
            fn less_than(_: void, lhs: @TypeOf(file_list.items[0]), rhs: @TypeOf(file_list.items[0])) bool {
                return lhs[1] > rhs[1]; // Descending order
            }
        }.less_than);

        const max_files_shown = @min(10, file_list.items.len);
        for (file_list.items[0..max_files_shown]) |file_entry| {
            std.debug.print("  {s}: {d} violations\n", .{ file_entry[0], file_entry[1] });
        }

        std.debug.print("\n", .{});
    }

    /// Print detailed information about each violation found
    ///
    /// Shows specific line numbers, messages, and suggested fixes for each violation.
    /// Essential for developers to understand and fix code quality issues.
    pub fn print_detailed_violations(self: *const ViolationSummary) void {
        if (self.total_violations == 0) return;

        std.debug.print("DETAILED VIOLATIONS:\n", .{});
        std.debug.print("====================\n\n", .{});

        var current_file: ?[]const u8 = null;

        for (self.all_violations.items) |violation| {
            if (current_file == null or !std.mem.eql(u8, current_file.?, violation.file_path)) {
                if (current_file != null) std.debug.print("\n", .{});
                std.debug.print("FILE: {s}\n", .{violation.file_path});
                std.debug.print("-----------------------------\n", .{});
                current_file = violation.file_path;
            }

            std.debug.print("  Line {d}: [{s}] {s}\n", .{
                violation.line,
                format_violation_type(violation.violation_type),
                violation.message,
            });

            if (violation.context) |context| {
                std.debug.print("    Context: {s}\n", .{context});
            }

            if (violation.suggested_fix) |fix| {
                std.debug.print("    Suggested fix: {s}\n", .{fix});
            }

            std.debug.print("\n", .{});
        }
    }
};

fn format_violation_type(violation_type: ViolationType) []const u8 {
    return switch (violation_type) {
        .naming_convention => "NAMING",
        .banned_pattern => "PATTERN",
        .documentation_standard => "DOCS",
        .function_length => "LENGTH",
        .generic_function => "GENERIC",
        .control_character => "CONTROL",
        .unicode_emoji => "UNICODE",
        .memory_management => "MEMORY",
        .error_handling => "ERROR",
        .architecture => "ARCH",
        .performance => "PERF",
        .comment_quality => "COMMENT",
        .import_pattern => "IMPORT",
    };
}
