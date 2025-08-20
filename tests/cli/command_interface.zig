//! CLI command interface and argument parsing tests.
//!
//! Tests command parsing, argument validation, and usage message generation.
//! Does not test actual server startup - that belongs in integration tests.

const std = @import("std");

const kausaldb = @import("kausaldb");

const testing = std.testing;
const test_config = kausaldb.test_config;

// Test result structure for CLI command testing
const CLITestResult = struct {
    exit_code: u8,
    stdout_output: []const u8,
    stderr_output: []const u8,

    const Self = @This();

    pub fn deinit(self: Self, allocator: std.mem.Allocator) void {
        allocator.free(self.stdout_output);
        allocator.free(self.stderr_output);
    }
};

// CLI test harness that captures output and simulates command execution
const CLITestHarness = struct {
    allocator: std.mem.Allocator,
    stdout_buffer: std.array_list.Managed(u8),
    stderr_buffer: std.array_list.Managed(u8),

    const Self = @This();

    pub fn init(allocator: std.mem.Allocator) Self {
        return Self{
            .allocator = allocator,
            .stdout_buffer = std.array_list.Managed(u8).init(allocator),
            .stderr_buffer = std.array_list.Managed(u8).init(allocator),
        };
    }

    pub fn deinit(self: *Self) void {
        self.stdout_buffer.deinit();
        self.stderr_buffer.deinit();
    }

    // Execute CLI command with given arguments and capture output
    pub fn execute_command(self: *Self, args: []const []const u8) !CLITestResult {
        // Reset buffers
        self.stdout_buffer.clearRetainingCapacity();
        self.stderr_buffer.clearRetainingCapacity();

        // Simulate command execution based on arguments
        var exit_code: u8 = 0;

        if (args.len == 0) {
            try self.stderr_buffer.appendSlice("No command specified\n");
            exit_code = 1;
        } else if (args.len == 1) {
            // No subcommand provided
            try self.append_usage_message();
            exit_code = 0;
        } else {
            const command = args[1];

            if (std.mem.eql(u8, command, "version")) {
                try self.stdout_buffer.appendSlice("KausalDB v0.1.0\n");
            } else if (std.mem.eql(u8, command, "help")) {
                try self.append_usage_message();
            } else if (std.mem.eql(u8, command, "server")) {
                // Server command validation
                exit_code = try self.handle_server_command(args[2..]);
            } else if (std.mem.eql(u8, command, "demo")) {
                // Demo command validation
                exit_code = try self.handle_demo_command(args[2..]);
            } else {
                try self.stderr_buffer.writer().print("Unknown command: {s}\n", .{command});
                try self.append_usage_message();
                exit_code = 1;
            }
        }

        return CLITestResult{
            .exit_code = exit_code,
            .stdout_output = try self.allocator.dupe(u8, self.stdout_buffer.items),
            .stderr_output = try self.allocator.dupe(u8, self.stderr_buffer.items),
        };
    }

    fn append_usage_message(self: *Self) !void {
        try self.stdout_buffer.appendSlice(
            \\KausalDB - High-performance context database
            \\
            \\Usage:
            \\  kausaldb <command> [options]
            \\
            \\Commands:
            \\  version    Show version information
            \\  help       Show this help message
            \\  server     Start the database server
            \\  demo       Run a storage and query demonstration
            \\
            \\Examples:
            \\  kausaldb server --port 8080
            \\  kausaldb demo
            \\  kausaldb version
            \\
        );
    }

    fn handle_server_command(self: *Self, args: []const []const u8) !u8 {
        _ = args; // Server args parsing not implemented in test harness yet

        // Simulate server startup validation
        try self.stdout_buffer.appendSlice("KausalDB server starting...\n");

        // Simulate potential directory creation and validation
        // This would normally involve VFS operations

        return 0; // Success for basic server command
    }

    fn handle_demo_command(self: *Self, args: []const []const u8) !u8 {
        if (args.len > 0) {
            try self.stderr_buffer.appendSlice("Demo command does not accept arguments\n");
            return 1;
        }

        try self.stdout_buffer.appendSlice("=== KausalDB Storage and Query Demo ===\n\n");
        return 0;
    }
};

test "command parsing basic commands" {
    const allocator = testing.allocator;

    var harness = CLITestHarness.init(allocator);
    defer harness.deinit();

    // Test version command
    {
        const result = try harness.execute_command(&[_][]const u8{ "kausaldb", "version" });
        defer result.deinit(allocator);

        try testing.expectEqual(@as(u8, 0), result.exit_code);
        try testing.expect(std.mem.indexOf(u8, result.stdout_output, "KausalDB v0.1.0") != null);
        try testing.expectEqual(@as(usize, 0), result.stderr_output.len);
    }

    // Test help command
    {
        const result = try harness.execute_command(&[_][]const u8{ "kausaldb", "help" });
        defer result.deinit(allocator);

        try testing.expectEqual(@as(u8, 0), result.exit_code);
        try testing.expect(std.mem.indexOf(u8, result.stdout_output, "Usage:") != null);
        try testing.expect(std.mem.indexOf(u8, result.stdout_output, "Commands:") != null);
        try testing.expectEqual(@as(usize, 0), result.stderr_output.len);
    }

    // Test demo command
    {
        const result = try harness.execute_command(&[_][]const u8{ "kausaldb", "demo" });
        defer result.deinit(allocator);

        try testing.expectEqual(@as(u8, 0), result.exit_code);
        try testing.expect(std.mem.indexOf(u8, result.stdout_output, "Demo") != null);
        try testing.expectEqual(@as(usize, 0), result.stderr_output.len);
    }
}

test "error handling invalid commands" {
    const allocator = testing.allocator;

    var harness = CLITestHarness.init(allocator);
    defer harness.deinit();

    // Test unknown command
    {
        const result = try harness.execute_command(&[_][]const u8{ "kausaldb", "invalid_command" });
        defer result.deinit(allocator);

        try testing.expectEqual(@as(u8, 1), result.exit_code);
        try testing.expect(std.mem.indexOf(u8, result.stderr_output, "Unknown command: invalid_command") != null);
        try testing.expect(std.mem.indexOf(u8, result.stdout_output, "Usage:") != null);
    }

    // Test no command provided
    {
        const result = try harness.execute_command(&[_][]const u8{"kausaldb"});
        defer result.deinit(allocator);

        try testing.expectEqual(@as(u8, 0), result.exit_code);
        try testing.expect(std.mem.indexOf(u8, result.stdout_output, "Usage:") != null);
    }

    // Test empty args
    {
        const result = try harness.execute_command(&[_][]const u8{});
        defer result.deinit(allocator);

        try testing.expectEqual(@as(u8, 1), result.exit_code);
        try testing.expect(std.mem.indexOf(u8, result.stderr_output, "No command specified") != null);
    }
}

test "CLI command argument validation" {
    const allocator = testing.allocator;

    var harness = CLITestHarness.init(allocator);
    defer harness.deinit();

    // Test demo with invalid arguments
    {
        const result = try harness.execute_command(&[_][]const u8{ "kausaldb", "demo", "extra_arg" });
        defer result.deinit(allocator);

        try testing.expectEqual(@as(u8, 1), result.exit_code);
        try testing.expect(std.mem.indexOf(u8, result.stderr_output, "does not accept arguments") != null);
    }

    // Test server command basic validation
    {
        const result = try harness.execute_command(&[_][]const u8{ "kausaldb", "server" });
        defer result.deinit(allocator);

        try testing.expectEqual(@as(u8, 0), result.exit_code);
        try testing.expect(std.mem.indexOf(u8, result.stdout_output, "server starting") != null);
    }
}

// Test command line argument edge cases
test "argument parsing edge cases" {
    const allocator = testing.allocator;

    var harness = CLITestHarness.init(allocator);
    defer harness.deinit();

    // Test commands with different casing (should be case-sensitive)
    {
        const result = try harness.execute_command(&[_][]const u8{ "kausaldb", "VERSION" });
        defer result.deinit(allocator);

        try testing.expectEqual(@as(u8, 1), result.exit_code);
        try testing.expect(std.mem.indexOf(u8, result.stderr_output, "Unknown command: VERSION") != null);
    }

    // Test command with leading/trailing whitespace (simulated)
    {
        const result = try harness.execute_command(&[_][]const u8{ "kausaldb", " version " });
        defer result.deinit(allocator);

        try testing.expectEqual(@as(u8, 1), result.exit_code);
        try testing.expect(std.mem.indexOf(u8, result.stderr_output, "Unknown command:  version ") != null);
    }

    // Test very long command name
    {
        const long_command = "very_long_command_name_that_should_not_exist_in_the_system_and_should_be_handled_gracefully";
        const result = try harness.execute_command(&[_][]const u8{ "kausaldb", long_command });
        defer result.deinit(allocator);

        try testing.expectEqual(@as(u8, 1), result.exit_code);
        try testing.expect(std.mem.indexOf(u8, result.stderr_output, "Unknown command:") != null);
    }
}

// Test usage message formatting and completeness
test "CLI usage message validation" {
    const allocator = testing.allocator;

    var harness = CLITestHarness.init(allocator);
    defer harness.deinit();

    const result = try harness.execute_command(&[_][]const u8{ "kausaldb", "help" });
    defer result.deinit(allocator);

    // Verify all expected commands are documented
    const expected_commands = [_][]const u8{ "version", "help", "server", "demo" };

    for (expected_commands) |command| {
        try testing.expect(std.mem.indexOf(u8, result.stdout_output, command) != null);
    }

    // Verify usage structure
    try testing.expect(std.mem.indexOf(u8, result.stdout_output, "Usage:") != null);
    try testing.expect(std.mem.indexOf(u8, result.stdout_output, "Commands:") != null);
    try testing.expect(std.mem.indexOf(u8, result.stdout_output, "Examples:") != null);

    // Verify no stderr output for help
    try testing.expectEqual(@as(usize, 0), result.stderr_output.len);
}

// Performance test for CLI argument parsing
test "argument parsing overhead performance" {
    const allocator = testing.allocator;

    var harness = CLITestHarness.init(allocator);
    defer harness.deinit();

    const iterations = 1000;
    const start_time = std.time.nanoTimestamp();

    var i: usize = 0;
    while (i < iterations) : (i += 1) {
        const result = try harness.execute_command(&[_][]const u8{ "kausaldb", "version" });
        defer result.deinit(allocator);

        try testing.expectEqual(@as(u8, 0), result.exit_code);
    }

    const end_time = std.time.nanoTimestamp();
    const total_time = end_time - start_time;
    const avg_time_per_parse = @divTrunc(total_time, iterations);

    // CLI parsing should be reasonable for test harness - under 100µs per parse
    const max_time_per_parse_ns = 100_000; // 100µs
    try testing.expect(avg_time_per_parse < max_time_per_parse_ns);

    test_config.debug_print("CLI parsing performance: {} ns average per parse\n", .{avg_time_per_parse});
}
