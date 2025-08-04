//! Shell command execution for git operations and tidy checks.

const std = @import("std");
const kausaldb = @import("kausaldb");
const assert = kausaldb.assert.assert;
const Allocator = std.mem.Allocator;
const ArrayList = std.ArrayList;

pub const Shell = struct {
    arena: std.heap.ArenaAllocator,
    allocator: Allocator,

    pub fn create(allocator: Allocator) !*Shell {
        const shell = try allocator.create(Shell);
        shell.* = .{
            .arena = std.heap.ArenaAllocator.init(allocator),
            .allocator = allocator,
        };
        return shell;
    }

    pub fn destroy(self: *Shell) void {
        self.arena.deinit();
        self.allocator.destroy(self);
    }

    /// Execute command and return stdout.
    pub fn exec_stdout(self: *Shell, comptime cmd: []const u8, args: anytype) ![]const u8 {
        return self.exec_stdout_options(.{}, cmd, args);
    }

    /// Execute command with options and return stdout.
    pub fn exec_stdout_options(
        self: *Shell,
        options: ExecOptions,
        comptime cmd: []const u8,
        args: anytype,
    ) ![]const u8 {
        const arena_allocator = self.arena.allocator();

        const full_cmd = try std.fmt.allocPrint(arena_allocator, cmd, args);

        // Split command into argv
        var argv = ArrayList([]const u8).init(arena_allocator);
        var arg_iter = std.mem.tokenizeAny(u8, full_cmd, " \t");
        while (arg_iter.next()) |arg| {
            try argv.append(arg);
        }

        if (argv.items.len == 0) return error.EmptyCommand;

        var process = std.process.Child.init(argv.items, arena_allocator);
        process.stdout_behavior = .Pipe;
        process.stderr_behavior = .Pipe;

        if (options.stdin_slice) |_| {
            process.stdin_behavior = .Pipe;
        }

        try process.spawn();

        if (options.stdin_slice) |stdin_data| {
            if (process.stdin) |stdin| {
                try stdin.writeAll(stdin_data);
                stdin.close();
                process.stdin = null;
            }
        }

        const stdout = if (process.stdout) |stdout_pipe|
            try stdout_pipe.readToEndAlloc(arena_allocator, std.math.maxInt(usize))
        else
            "";

        const stderr = if (process.stderr) |stderr_pipe|
            try stderr_pipe.readToEndAlloc(arena_allocator, std.math.maxInt(usize))
        else
            "";

        const result = try process.wait();

        switch (result) {
            .Exited => |code| {
                if (code != 0) {
                    std.debug.print("Command failed: {s}\n", .{full_cmd});
                    std.debug.print("Exit code: {d}\n", .{code});
                    if (stderr.len > 0) {
                        std.debug.print("Stderr: {s}\n", .{stderr});
                    }
                    return error.CommandFailed;
                }
            },
            else => return error.CommandFailed,
        }

        return stdout;
    }
};

const ExecOptions = struct {
    stdin_slice: ?[]const u8 = null,
};

test "shell basic execution" {
    const allocator = std.testing.allocator;

    const shell = try Shell.create(allocator);
    defer shell.destroy();

    const result = try shell.exec_stdout("echo hello", .{});
    try std.testing.expect(std.mem.startsWith(u8, result, "hello"));
}

test "shell with arguments" {
    const allocator = std.testing.allocator;

    const shell = try Shell.create(allocator);
    defer shell.destroy();

    const result = try shell.exec_stdout("echo {s}", .{"test_argument"});
    try std.testing.expect(std.mem.startsWith(u8, result, "test_argument"));
}

test "shell git command" {
    const allocator = std.testing.allocator;

    const shell = try Shell.create(allocator);
    defer shell.destroy();

    const result = shell.exec_stdout("git --version", .{}) catch |err| switch (err) {
        error.CommandFailed => return, // Git not available or not in a git repo
        else => return err,
    };

    try std.testing.expect(std.mem.startsWith(u8, result, "git version"));
}
