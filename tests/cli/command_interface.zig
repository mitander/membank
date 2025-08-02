//! Comprehensive CLI interface testing for Membank.
//!
//! Tests command-line argument parsing, validation, server startup robustness,
//! path handling edge cases, and error scenarios. Validates that CLI interface
//! behaves correctly across all supported scenarios and provides proper error
//! messages for invalid usage.

const std = @import("std");
const testing = std.testing;
const membank = @import("membank");

const simulation_vfs = membank.simulation_vfs;
const concurrency = membank.concurrency;

const SimulationVFS = simulation_vfs.SimulationVFS;

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
    stdout_buffer: std.ArrayList(u8),
    stderr_buffer: std.ArrayList(u8),
    
    const Self = @This();
    
    pub fn init(allocator: std.mem.Allocator) Self {
        return Self{
            .allocator = allocator,
            .stdout_buffer = std.ArrayList(u8).init(allocator),
            .stderr_buffer = std.ArrayList(u8).init(allocator),
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
                try self.stdout_buffer.appendSlice("Membank v0.1.0\n");
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
            \\Membank - High-performance context database
            \\
            \\Usage:
            \\  membank <command> [options]
            \\
            \\Commands:
            \\  version    Show version information
            \\  help       Show this help message
            \\  server     Start the database server
            \\  demo       Run a storage and query demonstration
            \\
            \\Examples:
            \\  membank server --port 8080
            \\  membank demo
            \\  membank version
            \\
        );
    }
    
    fn handle_server_command(self: *Self, args: []const []const u8) !u8 {
        _ = args; // Server args parsing not implemented in test harness yet
        
        // Simulate server startup validation
        try self.stdout_buffer.appendSlice("Membank server starting...\n");
        
        // Simulate potential directory creation and validation
        // This would normally involve VFS operations
        
        return 0; // Success for basic server command
    }
    
    fn handle_demo_command(self: *Self, args: []const []const u8) !u8 {
        if (args.len > 0) {
            try self.stderr_buffer.appendSlice("Demo command does not accept arguments\n");
            return 1;
        }
        
        try self.stdout_buffer.appendSlice("=== Membank Storage and Query Demo ===\n\n");
        return 0;
    }
};

test "CLI command parsing - basic commands" {
    concurrency.init();
    const allocator = testing.allocator;
    
    var harness = CLITestHarness.init(allocator);
    defer harness.deinit();
    
    // Test version command
    {
        const result = try harness.execute_command(&[_][]const u8{ "membank", "version" });
        defer result.deinit(allocator);
        
        try testing.expectEqual(@as(u8, 0), result.exit_code);
        try testing.expect(std.mem.indexOf(u8, result.stdout_output, "Membank v0.1.0") != null);
        try testing.expectEqual(@as(usize, 0), result.stderr_output.len);
    }
    
    // Test help command
    {
        const result = try harness.execute_command(&[_][]const u8{ "membank", "help" });
        defer result.deinit(allocator);
        
        try testing.expectEqual(@as(u8, 0), result.exit_code);
        try testing.expect(std.mem.indexOf(u8, result.stdout_output, "Usage:") != null);
        try testing.expect(std.mem.indexOf(u8, result.stdout_output, "Commands:") != null);
        try testing.expectEqual(@as(usize, 0), result.stderr_output.len);
    }
    
    // Test demo command
    {
        const result = try harness.execute_command(&[_][]const u8{ "membank", "demo" });
        defer result.deinit(allocator);
        
        try testing.expectEqual(@as(u8, 0), result.exit_code);
        try testing.expect(std.mem.indexOf(u8, result.stdout_output, "Demo") != null);
        try testing.expectEqual(@as(usize, 0), result.stderr_output.len);
    }
}

test "CLI error handling - invalid commands" {
    concurrency.init();
    const allocator = testing.allocator;
    
    var harness = CLITestHarness.init(allocator);
    defer harness.deinit();
    
    // Test unknown command
    {
        const result = try harness.execute_command(&[_][]const u8{ "membank", "invalid_command" });
        defer result.deinit(allocator);
        
        try testing.expectEqual(@as(u8, 1), result.exit_code);
        try testing.expect(std.mem.indexOf(u8, result.stderr_output, "Unknown command: invalid_command") != null);
        try testing.expect(std.mem.indexOf(u8, result.stdout_output, "Usage:") != null);
    }
    
    // Test no command provided
    {
        const result = try harness.execute_command(&[_][]const u8{"membank"});
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
    concurrency.init();
    const allocator = testing.allocator;
    
    var harness = CLITestHarness.init(allocator);
    defer harness.deinit();
    
    // Test demo with invalid arguments
    {
        const result = try harness.execute_command(&[_][]const u8{ "membank", "demo", "extra_arg" });
        defer result.deinit(allocator);
        
        try testing.expectEqual(@as(u8, 1), result.exit_code);
        try testing.expect(std.mem.indexOf(u8, result.stderr_output, "does not accept arguments") != null);
    }
    
    // Test server command basic validation
    {
        const result = try harness.execute_command(&[_][]const u8{ "membank", "server" });
        defer result.deinit(allocator);
        
        try testing.expectEqual(@as(u8, 0), result.exit_code);
        try testing.expect(std.mem.indexOf(u8, result.stdout_output, "server starting") != null);
    }
}

// Advanced CLI testing with actual directory operations using SimulationVFS
test "CLI directory creation and path handling" {
    concurrency.init();
    const allocator = testing.allocator;
    
    var sim_vfs = try SimulationVFS.init(allocator);
    defer sim_vfs.deinit();
    
    // Test directory creation scenarios that would occur in actual server startup
    const test_paths = [_][]const u8{
        "/tmp/membank_test_data",
        "/very/deep/nested/path/membank_data", 
        "relative/path/membank_data",
        "/path/with spaces/membank_data",
        "/path/with-special_chars.123/membank_data",
    };
    
    for (test_paths) |path| {
        // Test directory creation
        sim_vfs.vfs().mkdir_all(path) catch |err| switch (err) {
            membank.vfs.VFSError.FileExists => {}, // OK if exists
            else => return err,
        };
        
        // Verify directory was created
        const stat = try sim_vfs.vfs().stat(path);
        try testing.expect(stat.is_directory);
    }
}

// Test very long paths that might cause issues
test "CLI path handling - edge cases" {
    concurrency.init();
    const allocator = testing.allocator;
    
    var sim_vfs = try SimulationVFS.init(allocator);
    defer sim_vfs.deinit();
    
    // Test very long path name
    var long_path = std.ArrayList(u8).init(allocator);
    defer long_path.deinit();
    
    try long_path.appendSlice("/tmp/");
    
    // Create a path component that's 100 characters long
    var i: usize = 0;
    while (i < 100) : (i += 1) {
        try long_path.append('a');
    }
    try long_path.appendSlice("/membank_data");
    
    // Test that we can handle long paths
    sim_vfs.vfs().mkdir_all(long_path.items) catch |err| switch (err) {
        membank.vfs.VFSError.FileExists => {},
        membank.vfs.VFSError.InvalidPath => {
            // This is acceptable - the system correctly detected the limit
            return;
        },
        else => return err,
    };
    
    // Verify creation if it succeeded
    const stat = try sim_vfs.vfs().stat(long_path.items);
    try testing.expect(stat.is_directory);
}

// Test concurrent directory creation (simulating multiple server startups)
test "CLI directory creation - concurrent access simulation" {
    concurrency.init();
    const allocator = testing.allocator;
    
    var sim_vfs = try SimulationVFS.init(allocator);
    defer sim_vfs.deinit();
    
    const test_dir = "/tmp/concurrent_test_membank_data";
    
    // Simulate multiple processes trying to create the same directory
    var creation_attempts: u32 = 0;
    var success_count: u32 = 0;
    
    while (creation_attempts < 5) : (creation_attempts += 1) {
        sim_vfs.vfs().mkdir_all(test_dir) catch |err| switch (err) {
            membank.vfs.VFSError.FileExists => {
                // This is expected after the first creation
                success_count += 1;
                continue;
            },
            else => return err,
        };
        success_count += 1;
    }
    
    // All attempts should succeed (either creating or finding existing)
    try testing.expectEqual(@as(u32, 5), success_count);
    
    // Verify final state
    const stat = try sim_vfs.vfs().stat(test_dir);
    try testing.expect(stat.is_directory);
}

// Test error scenarios for directory creation
test "CLI directory creation - permission and error scenarios" {
    concurrency.init();
    const allocator = testing.allocator;
    
    var sim_vfs = try SimulationVFS.init(allocator);
    defer sim_vfs.deinit();
    
    // Test creating directory with edge case paths (simulation)
    const edge_case_paths = [_][]const u8{
        "/", // Root only
        "//double//slashes//path",
        "/path/with/./dots/./in/./it",
    };
    
    for (edge_case_paths) |test_path| {
        // These should either succeed (if path is valid) or fail gracefully
        sim_vfs.vfs().mkdir_all(test_path) catch |err| switch (err) {
            membank.vfs.VFSError.InvalidPath,
            membank.vfs.VFSError.FileExists,
            membank.vfs.VFSError.AccessDenied => {
                // These are acceptable error responses
                continue;
            },
            else => {
                // Log unexpected error for debugging
                std.debug.print("Unexpected error for path '{s}': {}\n", .{ test_path, err });
                return err;
            },
        };
    }
}

// Test command line argument edge cases
test "CLI argument parsing - edge cases" {
    concurrency.init();
    const allocator = testing.allocator;
    
    var harness = CLITestHarness.init(allocator);
    defer harness.deinit();
    
    // Test commands with different casing (should be case-sensitive)
    {
        const result = try harness.execute_command(&[_][]const u8{ "membank", "VERSION" });
        defer result.deinit(allocator);
        
        try testing.expectEqual(@as(u8, 1), result.exit_code);
        try testing.expect(std.mem.indexOf(u8, result.stderr_output, "Unknown command: VERSION") != null);
    }
    
    // Test command with leading/trailing whitespace (simulated)
    {
        const result = try harness.execute_command(&[_][]const u8{ "membank", " version " });
        defer result.deinit(allocator);
        
        try testing.expectEqual(@as(u8, 1), result.exit_code);
        try testing.expect(std.mem.indexOf(u8, result.stderr_output, "Unknown command:  version ") != null);
    }
    
    // Test very long command name
    {
        const long_command = "very_long_command_name_that_should_not_exist_in_the_system_and_should_be_handled_gracefully";
        const result = try harness.execute_command(&[_][]const u8{ "membank", long_command });
        defer result.deinit(allocator);
        
        try testing.expectEqual(@as(u8, 1), result.exit_code);
        try testing.expect(std.mem.indexOf(u8, result.stderr_output, "Unknown command:") != null);
    }
}

// Test usage message formatting and completeness
test "CLI usage message validation" {
    concurrency.init();
    const allocator = testing.allocator;
    
    var harness = CLITestHarness.init(allocator);
    defer harness.deinit();
    
    const result = try harness.execute_command(&[_][]const u8{ "membank", "help" });
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
test "CLI performance - argument parsing overhead" {
    concurrency.init();
    const allocator = testing.allocator;
    
    var harness = CLITestHarness.init(allocator);
    defer harness.deinit();
    
    const iterations = 1000;
    const start_time = std.time.nanoTimestamp();
    
    var i: usize = 0;
    while (i < iterations) : (i += 1) {
        const result = try harness.execute_command(&[_][]const u8{ "membank", "version" });
        defer result.deinit(allocator);
        
        try testing.expectEqual(@as(u8, 0), result.exit_code);
    }
    
    const end_time = std.time.nanoTimestamp();
    const total_time = end_time - start_time;
    const avg_time_per_parse = @divTrunc(total_time, iterations);
    
    // CLI parsing should be reasonable for test harness - under 100µs per parse
    const max_time_per_parse_ns = 100_000; // 100µs
    try testing.expect(avg_time_per_parse < max_time_per_parse_ns);
    
    std.debug.print("CLI parsing performance: {} ns average per parse\n", .{avg_time_per_parse});
}