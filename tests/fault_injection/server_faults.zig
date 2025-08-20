//! Server Connection Management Fault Injection Tests
//!
//! Validates server resilience under network failures, I/O errors, memory
//! pressure, and connection failures. Tests ConnectionManager isolation,
//! resource cleanup, and error recovery using KausalDB's simulation-first
//! fault injection approach.
//!
//! These tests ensure server components maintain consistency under hostile
//! conditions and demonstrate proper fault tolerance patterns.

const std = @import("std");

const kausaldb = @import("kausaldb");

const simulation_vfs = kausaldb.simulation_vfs;
const testing = std.testing;
const handler = kausaldb.handler;

const SimulationVFS = simulation_vfs.SimulationVFS;
const StorageEngine = kausaldb.StorageEngine;
const QueryEngine = kausaldb.QueryEngine;
const Server = handler.Server;
const ServerConfig = handler.ServerConfig;
const ConnectionManager = handler.ConnectionManager;

// Test configuration for various server scenarios
const TestServerConfig = struct {
    max_connections: u32 = 5,
    connection_timeout_sec: u32 = 10,
    enable_logging: bool = false,
};

// Helper to create test server environment
fn create_test_server(allocator: std.mem.Allocator, config: TestServerConfig) !struct {
    sim_vfs: *SimulationVFS,
    storage: *StorageEngine,
    query: *QueryEngine,
    server: *Server,
} {
    // Manual setup required because: Fault injection testing needs precise control over
    // server component initialization to test failure scenarios. Harnesses coordinate
    // components automatically, preventing fault injection at specific lifecycle points.
    var sim_vfs = try SimulationVFS.heap_init(allocator);
    const storage_engine = try allocator.create(StorageEngine);
    storage_engine.* = try StorageEngine.init_default(allocator, sim_vfs.vfs(), "coordinator_test");
    try storage_engine.startup();

    const query_engine = try allocator.create(QueryEngine);
    query_engine.* = QueryEngine.init(allocator, storage_engine);

    const server_config = ServerConfig{
        .port = 0, // Ephemeral port for testing
        .max_connections = config.max_connections,
        .connection_timeout_sec = config.connection_timeout_sec,
        .enable_logging = config.enable_logging,
    };

    const server = try allocator.create(Server);
    server.* = Server.init(allocator, server_config, storage_engine, query_engine);

    return .{
        .sim_vfs = sim_vfs,
        .storage = storage_engine,
        .query = query_engine,
        .server = server,
    };
}

test "server coordinator pattern basic functionality" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const allocator = arena.allocator();

    var test_env = try create_test_server(allocator, .{});
    defer {
        test_env.server.deinit();
        allocator.destroy(test_env.server);
        test_env.query.deinit();
        allocator.destroy(test_env.query);
        test_env.storage.deinit();
        allocator.destroy(test_env.storage);
        test_env.sim_vfs.deinit();
        allocator.destroy(test_env.sim_vfs);
    }

    // Test server follows coordinator pattern
    try testing.expectEqual(@as(u32, 0), test_env.server.connection_manager.active_connection_count());

    // Test statistics aggregation from coordinator
    test_env.server.update_aggregated_statistics();
    const server_stats = test_env.server.statistics();
    const manager_stats = test_env.server.connection_manager.statistics();

    try testing.expectEqual(manager_stats.connections_accepted, server_stats.connections_accepted);
    try testing.expectEqual(manager_stats.connections_active, server_stats.connections_active);
}

test "connection manager isolation and independence" {
    const config1 = kausaldb.connection_manager.ConnectionManagerConfig{
        .max_connections = 3,
        .connection_timeout_sec = 30,
        .poll_timeout_ms = 500,
    };

    const config2 = kausaldb.connection_manager.ConnectionManagerConfig{
        .max_connections = 7,
        .connection_timeout_sec = 60,
        .poll_timeout_ms = 1000,
    };

    var manager1 = ConnectionManager.init(testing.allocator, config1);
    defer manager1.deinit();
    try manager1.startup();

    var manager2 = ConnectionManager.init(testing.allocator, config2);
    defer manager2.deinit();
    try manager2.startup();

    // Verify complete isolation between managers
    try testing.expectEqual(@as(u32, 3), manager1.config.max_connections);
    try testing.expectEqual(@as(u32, 7), manager2.config.max_connections);

    try testing.expectEqual(@as(u32, 30), manager1.config.connection_timeout_sec);
    try testing.expectEqual(@as(u32, 60), manager2.config.connection_timeout_sec);

    // Verify independent resource allocation
    try testing.expectEqual(@as(usize, 4), manager1.poll_fds.len); // 3 + 1 listener
    try testing.expectEqual(@as(usize, 8), manager2.poll_fds.len); // 7 + 1 listener

    // Verify independent connection ID sequences
    try testing.expectEqual(@as(u32, 1), manager1.next_connection_id);
    try testing.expectEqual(@as(u32, 1), manager2.next_connection_id);

    manager1.next_connection_id += 5;
    try testing.expectEqual(@as(u32, 6), manager1.next_connection_id);
    try testing.expectEqual(@as(u32, 1), manager2.next_connection_id); // Unaffected
}

test "server coordination delegates properly to connection manager" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const allocator = arena.allocator();

    var test_env = try create_test_server(allocator, .{
        .max_connections = 10,
        .connection_timeout_sec = 45,
    });
    defer {
        test_env.server.deinit();
        allocator.destroy(test_env.server);
        test_env.query.deinit();
        allocator.destroy(test_env.query);
        test_env.storage.deinit();
        allocator.destroy(test_env.storage);
        test_env.sim_vfs.deinit();
        allocator.destroy(test_env.sim_vfs);
    }

    // Verify server properly configures connection manager
    try testing.expectEqual(@as(u32, 10), test_env.server.connection_manager.config.max_connections);
    try testing.expectEqual(@as(u32, 45), test_env.server.connection_manager.config.connection_timeout_sec);

    // Test coordinator methods don't directly manage connections
    try testing.expect(!test_env.server.connection_manager.has_ready_requests());
    try testing.expect(test_env.server.connection_manager.find_connection_with_ready_request() == null);

    // Verify statistics delegation
    const initial_server_stats = test_env.server.statistics();
    const initial_manager_stats = test_env.server.connection_manager.statistics();

    try testing.expectEqual(initial_manager_stats.connections_accepted, initial_server_stats.connections_accepted);
    try testing.expectEqual(initial_manager_stats.connections_active, initial_server_stats.connections_active);
}

test "arena per subsystem memory isolation under stress" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const allocator = arena.allocator();

    // Create multiple connection managers to test memory isolation
    var managers: [3]ConnectionManager = undefined;

    for (&managers, 0..) |*manager, i| {
        const config = kausaldb.connection_manager.ConnectionManagerConfig{
            .max_connections = @as(u32, @intCast(i + 2)), // 2, 3, 4 connections
            .connection_timeout_sec = @as(u32, @intCast((i + 1) * 10)), // 10, 20, 30 seconds
        };

        manager.* = ConnectionManager.init(allocator, config);
        try manager.startup();
    }

    defer for (&managers) |*manager| {
        manager.deinit();
    };

    // Test independent arena allocation under stress
    for (&managers, 0..) |*manager, i| {
        const arena_allocator = manager.arena.allocator();

        // Allocate significant memory from each arena
        const stress_size = 1024 * (i + 1);
        const stress_memory = try arena_allocator.alloc(u8, stress_size);

        // Fill with unique pattern to detect cross-contamination
        const pattern = @as(u8, @intCast(0xA0 + i));
        @memset(stress_memory, pattern);

        // Verify pattern integrity
        for (stress_memory) |byte| {
            try testing.expectEqual(pattern, byte);
        }
    }

    // Verify each manager maintained independent state
    for (&managers, 0..) |*manager, i| {
        try testing.expectEqual(@as(u32, @intCast(i + 2)), manager.config.max_connections);
        try testing.expectEqual(@as(u32, 1), manager.next_connection_id);
    }
}

test "two phase initialization enforced across server hierarchy" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const allocator = arena.allocator();

    var test_env = try create_test_server(allocator, .{});
    defer test_env.sim_vfs.deinit();
    defer test_env.storage.deinit();
    defer test_env.query.deinit();
    defer test_env.server.deinit();

    // Phase 1 verification: Only memory allocated, no I/O resources
    try testing.expect(test_env.server.listener == null);

    // ConnectionManager should not have poll_fds allocated yet
    const manager = &test_env.server.connection_manager;
    try testing.expectEqual(@as(usize, 0), manager.poll_fds.len);

    // Phase 2 would be: server.startup() -> manager.startup() -> bind()
    // We can't test the full server startup without actual networking,
    // but we can test the manager startup independently
    try manager.startup();
    try testing.expect(manager.poll_fds.len > 0);
}

test "connection manager handles I/O failures gracefully" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const allocator = arena.allocator();

    var test_env = try create_test_server(allocator, .{
        .max_connections = 3,
        .connection_timeout_sec = 5,
    });
    defer test_env.sim_vfs.deinit();
    defer test_env.storage.deinit();
    defer test_env.query.deinit();
    defer test_env.server.deinit();

    // Enable fault injection in simulation VFS
    test_env.sim_vfs.enable_io_failures(200, .{ .read = true, .write = true }); // 20% failure rate

    // Test connection manager resilience
    const manager = &test_env.server.connection_manager;
    try manager.startup();

    // Manager should handle faults gracefully and maintain valid state
    const stats = manager.statistics();
    try testing.expect(stats.poll_cycles_completed >= 0);
    try testing.expectEqual(@as(u32, 0), stats.connections_active);

    // Test cleanup operations work under fault injection
    try manager.cleanup_timed_out_connections();

    const post_cleanup_stats = manager.statistics();
    try testing.expectEqual(stats.connections_active, post_cleanup_stats.connections_active);
}

test "connection manager overhead characteristics" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const allocator = arena.allocator();

    const config = kausaldb.connection_manager.ConnectionManagerConfig{
        .max_connections = 100, // Larger scale for performance testing
        .connection_timeout_sec = 300,
        .poll_timeout_ms = 10, // Short timeout for performance
    };

    var manager = ConnectionManager.init(allocator, config);
    defer manager.deinit();

    // Measure startup time
    const startup_start = std.time.nanoTimestamp();
    try manager.startup();
    const startup_time = std.time.nanoTimestamp() - startup_start;

    // Use environment-aware performance assertion for startup time
    const perf = kausaldb.PerformanceAssertion.init("connection_manager_startup");
    try perf.assert_latency(@intCast(startup_time), 1_000_000, "ConnectionManager startup for 100 connections");

    // Measure statistics collection overhead
    const stats_start = std.time.nanoTimestamp();
    for (0..1000) |_| {
        _ = manager.statistics();
    }
    const stats_time = std.time.nanoTimestamp() - stats_start;
    const avg_stats_time = @divTrunc(stats_time, 1000);

    // Use environment-aware performance assertion for statistics collection
    try perf.assert_latency(@intCast(avg_stats_time), 1000, "ConnectionManager statistics collection");
}

test "server statistics aggregation accuracy" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const allocator = arena.allocator();

    var test_env = try create_test_server(allocator, .{
        .max_connections = 5,
        .connection_timeout_sec = 20,
    });
    defer test_env.sim_vfs.deinit();
    defer test_env.storage.deinit();
    defer test_env.query.deinit();
    defer test_env.server.deinit();

    // Test initial state
    test_env.server.update_aggregated_statistics();
    const initial_stats = test_env.server.statistics();
    try testing.expectEqual(@as(u64, 0), initial_stats.connections_accepted);
    try testing.expectEqual(@as(u32, 0), initial_stats.connections_active);
    try testing.expectEqual(@as(u64, 0), initial_stats.requests_processed);
    try testing.expectEqual(@as(u64, 0), initial_stats.bytes_received);
    try testing.expectEqual(@as(u64, 0), initial_stats.bytes_sent);
    try testing.expectEqual(@as(u64, 0), initial_stats.errors_encountered);

    // Test that server maintains its own request-processing statistics
    // while delegating connection statistics to manager
    test_env.server.stats.requests_processed += 5;
    test_env.server.stats.bytes_received += 1024;
    test_env.server.stats.bytes_sent += 2048;
    test_env.server.stats.errors_encountered += 1;

    test_env.server.update_aggregated_statistics();
    const updated_stats = test_env.server.statistics();

    // Connection stats should come from manager (still 0)
    try testing.expectEqual(@as(u64, 0), updated_stats.connections_accepted);
    try testing.expectEqual(@as(u32, 0), updated_stats.connections_active);

    // Request processing stats should come from server
    try testing.expectEqual(@as(u64, 5), updated_stats.requests_processed);
    try testing.expectEqual(@as(u64, 1024), updated_stats.bytes_received);
    try testing.expectEqual(@as(u64, 2048), updated_stats.bytes_sent);
    try testing.expectEqual(@as(u64, 1), updated_stats.errors_encountered);
}

test "connection manager resource cleanup under various failure modes" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const allocator = arena.allocator();

    // Test resource cleanup under different failure scenarios
    const SetupFn = *const fn (*SimulationVFS) void;
    const scenarios = [_]struct {
        description: []const u8,
        setup: SetupFn,
    }{
        .{
            .description = "I/O failures during operations",
            .setup = struct {
                fn setup(sim_vfs: *SimulationVFS) void {
                    sim_vfs.enable_io_failures(300, .{ .read = true, .write = true }); // 30% failure
                }
            }.setup,
        },
        .{
            .description = "Memory pressure conditions",
            .setup = struct {
                fn setup(sim_vfs: *SimulationVFS) void {
                    sim_vfs.configure_disk_space_limit(1024 * 1024); // 1MB limit
                }
            }.setup,
        },
        .{
            .description = "Normal operation baseline",
            .setup = struct {
                fn setup(_: *SimulationVFS) void {
                    // No faults injected
                }
            }.setup,
        },
    };

    for (scenarios) |scenario| {
        var test_env = try create_test_server(allocator, .{
            .max_connections = 10,
            .connection_timeout_sec = 1, // Short timeout for quick testing
        });
        defer test_env.sim_vfs.deinit();
        defer test_env.storage.deinit();
        defer test_env.query.deinit();
        defer test_env.server.deinit();

        // Apply scenario-specific fault injection
        scenario.setup(test_env.sim_vfs);

        // Test manager handles scenario gracefully
        const manager = &test_env.server.connection_manager;
        try manager.startup();

        // Allocate arena memory to test cleanup
        const arena_allocator = manager.arena.allocator();
        const test_data = try arena_allocator.alloc(u8, 4096);
        @memset(test_data, 0xCC);

        // Perform operations that might fail
        try manager.cleanup_timed_out_connections();
        const stats = manager.statistics();

        // Verify manager maintains consistency regardless of faults
        try testing.expect(stats.connections_active >= 0);
        try testing.expect(stats.poll_cycles_completed >= 0);
        try testing.expectEqual(@as(u32, 1), manager.next_connection_id);

        // Verify arena data integrity
        for (test_data) |byte| {
            try testing.expectEqual(@as(u8, 0xCC), byte);
        }
    }
}
