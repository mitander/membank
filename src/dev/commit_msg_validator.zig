//! Commit message validator for Conventional Commits format
//!
//! Replaces shell script validation for cross-platform consistency
//! and better maintainability within the Zig ecosystem.

const std = @import("std");

const ValidationError = error{
    InvalidFormat,
    EmptySubject,
    SubjectTooLong,
    InvalidType,
    FileNotFound,
    NoArguments,
} || std.fs.File.OpenError || std.fs.File.ReadError || std.mem.Allocator.Error;

const VALID_TYPES = [_][]const u8{ "feat", "fix", "build", "chore", "ci", "docs", "perf", "refactor", "revert", "style", "test", "wip" };

const MAX_SUBJECT_LENGTH = 100;

const CommitMessage = struct {
    type: []const u8,
    scope: ?[]const u8,
    breaking: bool,
    subject: []const u8,

    fn deinit(self: *CommitMessage, allocator: std.mem.Allocator) void {
        allocator.free(self.type);
        if (self.scope) |scope| allocator.free(scope);
        allocator.free(self.subject);
    }
};

fn parse_commit_message(allocator: std.mem.Allocator, header: []const u8) ValidationError!?CommitMessage {
    const trimmed = std.mem.trim(u8, header, " \t\r\n");
    if (trimmed.len == 0) return null;

    // Conventional format: type(scope)!: subject
    const colon_pos = std.mem.indexOf(u8, trimmed, ": ") orelse return null;
    const prefix = trimmed[0..colon_pos];
    const subject = trimmed[colon_pos + 2 ..];

    // Subject must start with alphanumeric and be within length limit
    if (subject.len == 0 or !std.ascii.isAlphanumeric(subject[0])) return null;
    if (subject.len > MAX_SUBJECT_LENGTH) return null;

    // Parse type(scope)! prefix
    var type_end: usize = 0;
    while (type_end < prefix.len and prefix[type_end] != '(' and prefix[type_end] != '!') {
        type_end += 1;
    }

    if (type_end == 0) return null;
    const commit_type = prefix[0..type_end];

    // Validate type against allowed list
    for (VALID_TYPES) |valid_type| {
        if (std.mem.eql(u8, commit_type, valid_type)) break;
    } else return null;

    var pos = type_end;
    var scope: ?[]const u8 = null;
    var breaking = false;

    // Parse optional scope
    if (pos < prefix.len and prefix[pos] == '(') {
        pos += 1;
        const scope_start = pos;

        while (pos < prefix.len and prefix[pos] != ')') pos += 1;
        if (pos >= prefix.len) return null;

        scope = prefix[scope_start..pos];
        if (scope.?.len == 0) return null;
        pos += 1;
    }

    // Parse optional breaking change indicator
    if (pos < prefix.len and prefix[pos] == '!') {
        breaking = true;
        pos += 1;
    }

    if (pos != prefix.len) return null;

    return CommitMessage{
        .type = try allocator.dupe(u8, commit_type),
        .scope = if (scope) |s| try allocator.dupe(u8, s) else null,
        .breaking = breaking,
        .subject = try allocator.dupe(u8, subject),
    };
}

fn read_first_line(allocator: std.mem.Allocator, file_path: []const u8) ValidationError![]const u8 {
    const file = try std.fs.cwd().openFile(file_path, .{});
    defer file.close();

    // Read commit message file content - allow larger files for rebase operations
    // During rebasing, commit message files can be quite large
    const content = file.readToEndAlloc(allocator, 64 * 1024) catch |err| switch (err) {
        error.FileTooBig => {
            // For very large commit messages (e.g., during complex rebases),
            // just read the first part and extract the first line
            var buffer: [1024]u8 = undefined;
            const bytes_read = try file.read(&buffer);
            const partial_content = buffer[0..bytes_read];

            return if (std.mem.indexOf(u8, partial_content, "\n")) |newline_pos|
                allocator.dupe(u8, partial_content[0..newline_pos])
            else
                allocator.dupe(u8, partial_content);
        },
        else => return err,
    };
    defer allocator.free(content);

    // Extract first line only
    return if (std.mem.indexOf(u8, content, "\n")) |newline_pos|
        try allocator.dupe(u8, content[0..newline_pos])
    else
        try allocator.dupe(u8, content);
}

fn print_error_message(header: []const u8) void {
    std.debug.print(
        \\-------------------------------------------------------------------
        \\ERROR: Invalid commit message format.
        \\-------------------------------------------------------------------
        \\Your commit message header:
        \\    "{s}"
        \\
        \\Aborting commit.
        \\Please follow the Conventional Commits format: https://www.conventionalcommits.org/
        \\Format: <type>(optional scope)!: <description>
        \\Example: feat(parser): add ability to parse arrays
        \\Example: fix!: correct handling of unicode characters (BREAKING CHANGE)
        \\Allowed types: feat, fix, build, chore, ci, docs, perf, refactor, revert, style, test, wip
        \\The description must start with a letter or digit after ': '.
        \\The description must be 1-{} characters long.
        \\-------------------------------------------------------------------
        \\
    , .{ header, MAX_SUBJECT_LENGTH });
}

fn validate_commit_message(allocator: std.mem.Allocator, file_path: []const u8) ValidationError!void {
    const header = read_first_line(allocator, file_path) catch |err| {
        std.debug.print("Error: Could not read commit message file '{s}': {}\n", .{ file_path, err });
        return err;
    };
    defer allocator.free(header);

    const commit_msg = parse_commit_message(allocator, header) catch |err| {
        std.debug.print("Error: Failed to parse commit message: {}\n", .{err});
        return err;
    };

    if (commit_msg == null) {
        print_error_message(header);
        return ValidationError.InvalidFormat;
    }

    var parsed = commit_msg.?;
    defer parsed.deinit(allocator);

    if (parsed.subject.len == 0) {
        std.debug.print("ERROR: Commit message subject must not be empty.\n", .{});
        return ValidationError.EmptySubject;
    }

    if (parsed.subject.len > MAX_SUBJECT_LENGTH) {
        std.debug.print("ERROR: Commit message subject is too long (Max {} characters).\n", .{MAX_SUBJECT_LENGTH});
        std.debug.print("Your subject: \"{s}\" (Length: {})\n", .{ parsed.subject, parsed.subject.len });
        return ValidationError.SubjectTooLong;
    }

    std.debug.print("Commit message format OK.\n", .{});
}

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    const args = try std.process.argsAlloc(allocator);
    defer std.process.argsFree(allocator, args);

    if (args.len < 2) {
        std.debug.print("Usage: {s} <commit-message-file>\n", .{args[0]});
        return ValidationError.NoArguments;
    }

    validate_commit_message(allocator, args[1]) catch {
        std.process.exit(1);
    };
}

test "parse_commit_message - valid formats" {
    const allocator = std.testing.allocator;

    // Basic format
    {
        var msg = (try parse_commit_message(allocator, "feat: add new feature")).?;
        defer msg.deinit(allocator);

        try std.testing.expectEqualStrings("feat", msg.type);
        try std.testing.expect(msg.scope == null);
        try std.testing.expect(!msg.breaking);
        try std.testing.expectEqualStrings("add new feature", msg.subject);
    }

    // With scope
    {
        var msg = (try parse_commit_message(allocator, "fix(parser): resolve edge case")).?;
        defer msg.deinit(allocator);

        try std.testing.expectEqualStrings("fix", msg.type);
        try std.testing.expectEqualStrings("parser", msg.scope.?);
        try std.testing.expect(!msg.breaking);
        try std.testing.expectEqualStrings("resolve edge case", msg.subject);
    }

    // Breaking change
    {
        var msg = (try parse_commit_message(allocator, "feat!: introduce breaking API change")).?;
        defer msg.deinit(allocator);

        try std.testing.expectEqualStrings("feat", msg.type);
        try std.testing.expect(msg.scope == null);
        try std.testing.expect(msg.breaking);
        try std.testing.expectEqualStrings("introduce breaking API change", msg.subject);
    }

    // Scope and breaking change
    {
        var msg = (try parse_commit_message(allocator, "refactor(core)!: restructure API")).?;
        defer msg.deinit(allocator);

        try std.testing.expectEqualStrings("refactor", msg.type);
        try std.testing.expectEqualStrings("core", msg.scope.?);
        try std.testing.expect(msg.breaking);
        try std.testing.expectEqualStrings("restructure API", msg.subject);
    }
}

test "parse_commit_message - invalid formats" {
    const allocator = std.testing.allocator;

    // Missing colon
    try std.testing.expect((try parse_commit_message(allocator, "feat add feature")) == null);

    // No space after colon
    try std.testing.expect((try parse_commit_message(allocator, "feat:add feature")) == null);

    // Empty subject
    try std.testing.expect((try parse_commit_message(allocator, "feat: ")) == null);

    // Subject starts with non-alphanumeric
    try std.testing.expect((try parse_commit_message(allocator, "feat: !invalid")) == null);

    // Invalid type
    try std.testing.expect((try parse_commit_message(allocator, "invalid: subject")) == null);

    // Empty scope
    try std.testing.expect((try parse_commit_message(allocator, "feat(): subject")) == null);

    // Unclosed scope
    try std.testing.expect((try parse_commit_message(allocator, "feat(scope: subject")) == null);
}

test "parse_commit_message - length validation" {
    const allocator = std.testing.allocator;

    // Exactly max length should work
    const max_subject = "a" ** MAX_SUBJECT_LENGTH;
    const max_msg = try std.fmt.allocPrint(allocator, "feat: {s}", .{max_subject});
    defer allocator.free(max_msg);

    var msg = (try parse_commit_message(allocator, max_msg)).?;
    defer msg.deinit(allocator);
    try std.testing.expectEqual(@as(usize, MAX_SUBJECT_LENGTH), msg.subject.len);

    // Over max length should fail
    const over_max_subject = "a" ** (MAX_SUBJECT_LENGTH + 1);
    const over_max_msg = try std.fmt.allocPrint(allocator, "feat: {s}", .{over_max_subject});
    defer allocator.free(over_max_msg);

    try std.testing.expect((try parse_commit_message(allocator, over_max_msg)) == null);
}
