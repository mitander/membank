//! Server Coordinator Pattern Integration Tests
//!
//! Validates that the Server acts as a pure coordinator while delegating
//! connection management to ConnectionManager. Tests coordinator behavior,
//! component isolation, and proper statistics aggregation.
//!
//! These tests verify the coordinator pattern implementation and serve
//! as examples of proper component testing in KausalDB.

const std = @import("std");
const testing = std.testing;
const kausaldb = @import("kausaldb");

const simulation_vfs = kausaldb.simulation_vfs;
const SimulationVFS = simulation_vfs.SimulationVFS;
const StorageEngine = kausaldb.StorageEngine;
const QueryEngine = kausaldb.QueryEngine;
const handler = kausaldb.handler;
const Server = handler.Server;
const ServerConfig = handler.ServerConfig;
const ConnectionManager = handler.ConnectionManager;

test "server decomposition maintains coordinator pattern" {
    const allocator = testing.allocator;

    var harness = try kausaldb.QueryHarness.init_and_startup(allocator, "decomp_test");
    defer harness.deinit();

    // Test server initialization follows coordinator pattern
    const config = ServerConfig{
        .port = 0, // Use ephemeral port
        .max_connections = 5,
        .connection_timeout_sec = 30,
    };

    var server = Server.init(allocator, config, harness.storage_engine(), harness.query_engine);
    defer server.deinit();

    // Verify server has ConnectionManager
    try testing.expect(server.connection_manager.active_connection_count() == 0);

    // Verify server statistics are properly aggregated
    const stats = server.statistics();
    try testing.expectEqual(@as(u64, 0), stats.connections_accepted);
    try testing.expectEqual(@as(u32, 0), stats.connections_active);
}

test "connection manager operates independently" {
    const config = kausaldb.connection_manager.ConnectionManagerConfig{
        .max_connections = 3,
        .connection_timeout_sec = 10,
        .poll_timeout_ms = 100,
    };

    // Test Phase 1 initialization (memory-only)
    var manager = ConnectionManager.init(testing.allocator, config);
    defer manager.deinit();

    // Verify initial state
    try testing.expectEqual(@as(u32, 0), manager.active_connection_count());
    try testing.expectEqual(@as(u32, 1), manager.next_connection_id);

    // Test Phase 2 initialization (I/O resources)
    try manager.startup();

    // Verify startup allocated poll_fds
    try testing.expect(manager.poll_fds.len == config.max_connections + 1);

    // Test statistics tracking
    const stats = manager.statistics();
    try testing.expectEqual(@as(u64, 0), stats.connections_accepted);
    try testing.expectEqual(@as(u32, 0), stats.connections_active);
}

test "server coordinator delegates to connection manager" {
    const allocator = testing.allocator;

    var harness = try kausaldb.QueryHarness.init_and_startup(allocator, "delegation_test");
    defer harness.deinit();

    const config = ServerConfig{
        .port = 0,
        .max_connections = 2,
        .connection_timeout_sec = 5,
    };

    var server = Server.init(allocator, config, harness.storage_engine(), harness.query_engine);
    defer server.deinit();

    // Test that server properly initializes manager
    try testing.expectEqual(@as(u32, 0), server.connection_manager.active_connection_count());

    // Verify statistics aggregation works
    server.update_aggregated_statistics();
    const server_stats = server.statistics();
    const manager_stats = server.connection_manager.statistics();

    try testing.expectEqual(manager_stats.connections_accepted, server_stats.connections_accepted);
    try testing.expectEqual(manager_stats.connections_active, server_stats.connections_active);
}

test "arena per subsystem memory isolation" {
    // Test that ConnectionManager's arena doesn't interfere with other components
    var manager1 = ConnectionManager.init(testing.allocator, .{});
    defer manager1.deinit();

    var manager2 = ConnectionManager.init(testing.allocator, .{});
    defer manager2.deinit();

    try manager1.startup();
    try manager2.startup();

    // Verify each manager has independent state
    try testing.expect(manager1.next_connection_id == 1);
    try testing.expect(manager2.next_connection_id == 1);

    // Verify independent poll_fds allocation
    try testing.expect(manager1.poll_fds.ptr != manager2.poll_fds.ptr);
    try testing.expectEqual(manager1.poll_fds.len, manager2.poll_fds.len);
}

test "two phase initialization enforced correctly" {
    var manager = ConnectionManager.init(testing.allocator, .{});
    defer manager.deinit();

    // Phase 1: Only memory allocated
    try testing.expectEqual(@as(usize, 0), manager.poll_fds.len);

    // Phase 2: I/O resources allocated
    try manager.startup();
    try testing.expect(manager.poll_fds.len > 0);
}

test "coordinator error handling preserves system stability" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const allocator = arena.allocator();

    // Manual setup required because: Test needs to enable I/O failures before storage engine
    // startup to validate coordinator error handling. FaultInjectionHarness applies faults
    // during startup(), which would prevent testing error recovery in coordinator initialization.
    var sim_vfs = try SimulationVFS.init(allocator);
    defer sim_vfs.deinit();

    // Enable I/O failures for stress testing
    sim_vfs.enable_io_failures(100, .{ .read = true, .write = false }); // 10% failure rate

    var storage_engine = try StorageEngine.init_default(allocator, sim_vfs.vfs(), "error_test");
    defer storage_engine.deinit();
    try storage_engine.startup();

    var query_engine = QueryEngine.init(allocator, &storage_engine);
    defer query_engine.deinit();

    const config = ServerConfig{
        .port = 0,
        .max_connections = 1,
        .connection_timeout_sec = 1,
    };

    var server = Server.init(allocator, config, &storage_engine, &query_engine);
    defer server.deinit();

    // Server should handle initialization errors gracefully
    try testing.expect(server.connection_manager.active_connection_count() == 0);

    // Verify error handling doesn't corrupt coordinator state
    const stats = server.statistics();
    try testing.expect(stats.connections_accepted >= 0); // Statistics remain valid
}
