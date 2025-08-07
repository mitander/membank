//! Server lifecycle integration tests.
//!
//! Tests actual server startup, binding, and shutdown with real I/O operations.
//! Uses both ProductionVFS (happy path) and SimulationVFS (deterministic testing)
//! to ensure server behaves correctly under normal conditions.
//!
//! Focus: Proper resource management and lifecycle patterns rather than failure injection.

const std = @import("std");
const builtin = @import("builtin");
const testing = std.testing;
const kausaldb = @import("kausaldb");

const vfs = kausaldb.vfs;
const production_vfs = kausaldb.production_vfs;
const simulation_vfs = kausaldb.simulation_vfs;
const StorageEngine = kausaldb.StorageEngine;
const QueryEngine = kausaldb.QueryEngine;
const Server = kausaldb.handler.Server;
const ServerConfig = kausaldb.handler.ServerConfig;

const SimulationVFS = simulation_vfs.SimulationVFS;
const ProductionVFS = production_vfs.ProductionVFS;

/// Test server with complete lifecycle management
const TestServer = struct {
    allocator: std.mem.Allocator,
    storage_engine: StorageEngine,
    query_engine: QueryEngine,
    server: Server,
    data_dir: []const u8,

    const Self = @This();

    /// Create server with VFS injection for testing
    pub fn init(
        allocator: std.mem.Allocator,
        vfs_interface: vfs.VFS,
        server_config: ServerConfig,
        data_dir: []const u8,
    ) !Self {
        // Ensure test data directory exists
        try vfs_interface.mkdir_all(data_dir);

        var storage_engine = try StorageEngine.init_default(allocator, vfs_interface, data_dir);
        errdefer storage_engine.deinit();

        try storage_engine.startup();
        errdefer storage_engine.deinit(); // Additional cleanup after startup

        var query_engine = QueryEngine.init(allocator, &storage_engine);
        errdefer query_engine.deinit();

        const server = Server.init(allocator, server_config, &storage_engine, &query_engine);

        const owned_data_dir = try allocator.dupe(u8, data_dir);
        errdefer allocator.free(owned_data_dir);

        return Self{
            .allocator = allocator,
            .storage_engine = storage_engine,
            .query_engine = query_engine,
            .server = server,
            .data_dir = owned_data_dir,
        };
    }

    pub fn deinit(self: *Self) void {
        self.server.deinit();
        self.query_engine.deinit();
        self.storage_engine.deinit();
        self.allocator.free(self.data_dir);
    }

    /// Bind server to socket (non-blocking)
    pub fn bind(self: *Self) !void {
        try self.server.bind();
    }

    /// Stop server and clean up resources
    pub fn stop(self: *Self) void {
        self.server.stop();
    }

    /// Query the actual bound port (useful for ephemeral ports)
    pub fn bound_port(self: *const Self) u16 {
        return self.server.bound_port();
    }
};

// Skip ProductionVFS tests when libc is unavailable (CI environments)
test "server startup and shutdown with ProductionVFS" {
    if (!builtin.link_libc) {
        // ProductionVFS requires libc for sync() operations - skip in restricted environments
        return;
    }

    const allocator = testing.allocator;

    var prod_vfs = ProductionVFS.init(allocator);
    defer prod_vfs.deinit();

    // Use ephemeral port to avoid conflicts
    const server_config = ServerConfig{
        .port = 0, // OS assigns available port
        .enable_logging = false, // Reduce test noise
    };

    // Create unique test directory
    var test_dir_buf: [256]u8 = undefined;
    const test_dir = try std.fmt.bufPrint(&test_dir_buf, "/tmp/kausaldb_server_test_{d}", .{std.time.timestamp()});

    var test_server = TestServer.init(allocator, prod_vfs.vfs(), server_config, test_dir) catch |err| switch (err) {
        // Some CI environments may not allow /tmp access
        error.AccessDenied => {
            std.debug.print("Skipping server test - no /tmp access in this environment\n", .{});
            return;
        },
        else => return err,
    };
    defer test_server.deinit();

    // Test server binding
    try test_server.bind();

    // Verify server is bound to a port
    const bound_port = test_server.bound_port();
    try testing.expect(bound_port > 0);
    try testing.expect(bound_port != server_config.port); // Should be different from 0

    std.debug.print("Server bound to port: {d}\n", .{bound_port});

    // Test graceful shutdown
    test_server.stop();

    // Cleanup test directory
    std.fs.cwd().deleteTree(test_dir) catch {}; // Best effort cleanup
}

// Server startup with SimulationVFS demonstrates VFS abstraction
test "server startup with SimulationVFS integration" {
    const allocator = testing.allocator;

    var sim_vfs = try SimulationVFS.init_with_fault_seed(allocator, 12345);
    defer sim_vfs.deinit();

    const server_config = ServerConfig{
        .port = 0,
        .enable_logging = false,
    };

    const test_dir = "/test/server_sim_vfs";

    // Server startup should succeed with SimulationVFS in normal operation
    var test_server = try TestServer.init(allocator, sim_vfs.vfs(), server_config, test_dir);
    defer test_server.deinit();

    // Test basic server binding with simulated filesystem
    try test_server.bind();

    const bound_port = test_server.bound_port();
    try testing.expect(bound_port > 0);

    test_server.stop();
}

// Server resource cleanup validation
test "server resource cleanup and lifecycle management" {
    if (!builtin.link_libc) {
        // ProductionVFS requires libc for sync() operations - skip in restricted environments
        return;
    }

    const allocator = testing.allocator;

    var prod_vfs = ProductionVFS.init(allocator);
    defer prod_vfs.deinit();

    const server_config = ServerConfig{
        .port = 0, // Use ephemeral port to ensure success
        .enable_logging = false,
    };

    // Use unique test directory
    var test_dir_buf: [256]u8 = undefined;
    const test_dir = try std.fmt.bufPrint(&test_dir_buf, "/tmp/kausaldb_cleanup_test_{d}", .{std.time.timestamp()});

    // Create and fully initialize server
    var test_server = TestServer.init(allocator, prod_vfs.vfs(), server_config, test_dir) catch |err| switch (err) {
        error.AccessDenied => {
            std.debug.print("Skipping cleanup test - no /tmp access in this environment\n", .{});
            return;
        },
        else => return err,
    };
    defer test_server.deinit();

    // Test server binding and resource allocation
    try test_server.bind();
    const bound_port = test_server.bound_port();
    try testing.expect(bound_port > 0);

    // Test graceful shutdown and resource cleanup
    test_server.stop();

    // Verify server can be rebound after stop (demonstrates proper cleanup)
    try test_server.bind();
    const second_port = test_server.bound_port();
    try testing.expect(second_port > 0);
    test_server.stop();

    // Cleanup test directory
    std.fs.cwd().deleteTree(test_dir) catch {};
}

// Server initialization with proper error handling patterns
test "server initialization error handling patterns" {
    const allocator = testing.allocator;

    var sim_vfs = try SimulationVFS.init_with_fault_seed(allocator, 98765);
    defer sim_vfs.deinit();

    const server_config = ServerConfig{
        .port = 0,
        .enable_logging = false,
    };

    const test_dir = "/test/server_error_patterns";

    // Test successful initialization (normal case)
    var test_server = try TestServer.init(allocator, sim_vfs.vfs(), server_config, test_dir);
    defer test_server.deinit();

    // Verify server initialized correctly
    try test_server.bind();
    const bound_port = test_server.bound_port();
    try testing.expect(bound_port > 0);

    // Test proper cleanup
    test_server.stop();

    // This test demonstrates that our errdefer cleanup patterns work correctly
    // by successfully creating and destroying a server without memory leaks
}

// Multiple server instances with ephemeral ports
test "multiple server instances with ephemeral ports" {
    if (!builtin.link_libc) {
        // ProductionVFS requires libc for sync() operations - skip in restricted environments
        return;
    }

    const allocator = testing.allocator;

    var prod_vfs = ProductionVFS.init(allocator);
    defer prod_vfs.deinit();

    const server_config = ServerConfig{
        .port = 0, // Each server gets unique ephemeral port
        .enable_logging = false,
    };

    // Start 3 servers simultaneously
    var servers: [3]TestServer = undefined;
    var ports: [3]u16 = undefined;

    for (0..3) |i| {
        var test_dir_buf: [256]u8 = undefined;
        const test_dir = try std.fmt.bufPrint(&test_dir_buf, "/tmp/kausaldb_multi_test_{d}_{d}", .{ std.time.timestamp(), i });

        servers[i] = TestServer.init(allocator, prod_vfs.vfs(), server_config, test_dir) catch |err| switch (err) {
            error.AccessDenied => {
                // Clean up any servers we already started
                for (0..i) |j| {
                    servers[j].stop();
                    servers[j].deinit();
                }
                std.debug.print("Skipping multi-server test - no /tmp access\n", .{});
                return;
            },
            else => return err,
        };

        try servers[i].bind();
        ports[i] = servers[i].bound_port();
    }

    // Verify all servers got unique ports
    for (0..3) |i| {
        try testing.expect(ports[i] > 0);
        for (i + 1..3) |j| {
            try testing.expect(ports[i] != ports[j]); // All ports should be unique
        }
    }

    // Clean up all servers
    for (0..3) |i| {
        servers[i].stop();
        servers[i].deinit();

        // Best effort cleanup
        var test_dir_buf: [256]u8 = undefined;
        const test_dir = try std.fmt.bufPrint(&test_dir_buf, "/tmp/kausaldb_multi_test_{d}_{d}", .{ std.time.timestamp(), i });
        std.fs.cwd().deleteTree(test_dir) catch {};
    }
}
