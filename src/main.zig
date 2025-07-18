//! CortexDB main entry point and CLI interface.

const std = @import("std");
const assert = std.debug.assert;

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    const args = try std.process.argsAlloc(allocator);
    defer std.process.argsFree(allocator, args);

    if (args.len < 2) {
        try print_usage();
        return;
    }

    const command = args[1];

    if (std.mem.eql(u8, command, "version")) {
        try print_version();
    } else if (std.mem.eql(u8, command, "help")) {
        try print_usage();
    } else if (std.mem.eql(u8, command, "server")) {
        try run_server(allocator, args[2..]);
    } else {
        std.debug.print("Unknown command: {s}\n", .{command});
        try print_usage();
        std.process.exit(1);
    }
}

fn print_version() !void {
    std.debug.print("CortexDB v0.1.0\n", .{});
}

fn print_usage() !void {
    std.debug.print(
        \\CortexDB - High-performance context database
        \\
        \\Usage:
        \\  cortexdb <command> [options]
        \\
        \\Commands:
        \\  version    Show version information
        \\  help       Show this help message
        \\  server     Start the database server
        \\
        \\Examples:
        \\  cortexdb server --port 8080
        \\  cortexdb version
        \\
    , .{});
}

fn run_server(allocator: std.mem.Allocator, args: [][:0]u8) !void {
    _ = allocator;
    _ = args;

    std.debug.print("CortexDB server starting...\n", .{});
    std.debug.print("Server implementation coming soon!\n", .{});
}

test "main module tests" {
    // Tests for main module
}
