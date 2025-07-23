//! CortexDB TCP Server and Protocol Tests
//!
//! Simplified testing of the TCP server implementation and binary protocol.
//! Tests cover basic functionality without complex networking scenarios.

const std = @import("std");
const testing = std.testing;
const net = std.net;

const vfs = @import("vfs");
const simulation_vfs = @import("simulation_vfs");
const storage = @import("storage");
const query_engine = @import("query_engine");
const context_block = @import("context_block");
const server_handler = @import("server");
const concurrency = @import("concurrency");

const StorageEngine = storage.StorageEngine;
const QueryEngine = query_engine.QueryEngine;
const ContextBlock = context_block.ContextBlock;
const BlockId = context_block.BlockId;
const SimulationVFS = simulation_vfs.SimulationVFS;
const CortexServer = server_handler.CortexServer;
const ServerConfig = server_handler.ServerConfig;
const MessageHeader = server_handler.MessageHeader;
const MessageType = server_handler.MessageType;
const ClientConnection = server_handler.ClientConnection;

// Helper to create test storage engine
fn create_test_storage(allocator: std.mem.Allocator, vfs_interface: vfs.VFS) !StorageEngine {
    var storage_engine = try StorageEngine.init(allocator, vfs_interface, "test_server_db");
    try storage_engine.initialize_storage();
    return storage_engine;
}

// Helper to create test query engine
fn create_test_query_engine(allocator: std.mem.Allocator, storage_engine: *StorageEngine) !QueryEngine {
    return QueryEngine.init(allocator, storage_engine);
}

test "server config - default values" {
    const config = ServerConfig{};

    try testing.expectEqual(@as(u16, 8080), config.port);
    try testing.expectEqual(@as(u32, 100), config.max_connections);
    try testing.expectEqual(@as(u32, 300), config.connection_timeout_sec);
    try testing.expectEqual(@as(u32, 64 * 1024), config.max_request_size);
    try testing.expectEqual(@as(u32, 16 * 1024 * 1024), config.max_response_size);
    try testing.expectEqual(false, config.enable_logging);
}

test "server config - custom values" {
    const config = ServerConfig{
        .port = 9090,
        .max_connections = 50,
        .connection_timeout_sec = 600,
        .max_request_size = 128 * 1024,
        .max_response_size = 32 * 1024 * 1024,
        .enable_logging = true,
    };

    try testing.expectEqual(@as(u16, 9090), config.port);
    try testing.expectEqual(@as(u32, 50), config.max_connections);
    try testing.expectEqual(@as(u32, 600), config.connection_timeout_sec);
    try testing.expectEqual(@as(u32, 128 * 1024), config.max_request_size);
    try testing.expectEqual(@as(u32, 32 * 1024 * 1024), config.max_response_size);
    try testing.expectEqual(true, config.enable_logging);
}

test "message header - encode and decode" {
    const original = MessageHeader{
        .msg_type = MessageType.ping,
        .version = 1,
        .reserved = 0,
        .payload_length = 1024,
    };

    var buffer: [8]u8 = undefined;
    original.encode(&buffer);

    const decoded = try MessageHeader.decode(&buffer);

    try testing.expectEqual(original.msg_type, decoded.msg_type);
    try testing.expectEqual(original.version, decoded.version);
    try testing.expectEqual(original.reserved, decoded.reserved);
    try testing.expectEqual(original.payload_length, decoded.payload_length);
}

test "message header - invalid message type" {
    var buffer: [8]u8 = undefined;

    // Create header with invalid message type (99)
    buffer[0] = 99; // Invalid message type
    buffer[1] = 1; // Valid version
    std.mem.writeInt(u16, buffer[2..4], 0, .little); // Reserved
    std.mem.writeInt(u32, buffer[4..8], 100, .little); // Payload length

    const result = MessageHeader.decode(&buffer);
    try testing.expectError(error.InvalidRequest, result);
}

test "message header - buffer too small" {
    var buffer: [4]u8 = undefined; // Too small

    const result = MessageHeader.decode(&buffer);
    try testing.expectError(error.InvalidRequest, result);
}

test "server initialization" {
    concurrency.init();

    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const allocator = arena.allocator();

    var sim_vfs = SimulationVFS.init(allocator);
    defer sim_vfs.deinit();

    var storage_engine = try create_test_storage(allocator, sim_vfs.vfs());
    defer storage_engine.deinit();

    var test_query_engine = try create_test_query_engine(allocator, &storage_engine);
    defer test_query_engine.deinit();

    const config = ServerConfig{
        .port = 0, // Let OS choose available port
        .max_connections = 10,
    };

    var server = CortexServer.init(allocator, config, &storage_engine, &test_query_engine);
    defer server.deinit();

    // Verify initial state
    try testing.expectEqual(@as(u64, 0), server.stats.connections_accepted);
    try testing.expectEqual(@as(u32, 0), server.stats.connections_active);
    try testing.expectEqual(@as(u64, 0), server.stats.requests_processed);
    try testing.expectEqual(@as(u64, 0), server.stats.bytes_received);
    try testing.expectEqual(@as(u64, 0), server.stats.bytes_sent);
    try testing.expectEqual(@as(u64, 0), server.stats.errors_encountered);
}

test "client connection - initialization" {
    concurrency.init();

    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const allocator = arena.allocator();

    // Create a mock stream
    const mock_stream = std.net.Stream{
        .handle = @as(std.posix.fd_t, -1),
    };

    // Test connection creation
    const connection_id: u32 = 1;

    const connection = ClientConnection.init(allocator, mock_stream, connection_id);
    // Note: Not calling deinit() because mock stream has invalid handle

    try testing.expectEqual(connection_id, connection.connection_id);
    try testing.expect(connection.established_time > 0);
}

test "protocol - ping message" {
    const ping_header = MessageHeader{
        .msg_type = MessageType.ping,
        .version = 1,
        .reserved = 0,
        .payload_length = 0,
    };

    var buffer: [8]u8 = undefined;
    ping_header.encode(&buffer);

    const decoded = try MessageHeader.decode(&buffer);

    try testing.expectEqual(MessageType.ping, decoded.msg_type);
    try testing.expectEqual(@as(u32, 0), decoded.payload_length);
}

test "protocol - get_blocks message" {
    const get_blocks_header = MessageHeader{
        .msg_type = MessageType.get_blocks,
        .version = 1,
        .reserved = 0,
        .payload_length = 16, // Size of BlockId
    };

    var buffer: [8]u8 = undefined;
    get_blocks_header.encode(&buffer);

    const decoded = try MessageHeader.decode(&buffer);

    try testing.expectEqual(MessageType.get_blocks, decoded.msg_type);
    try testing.expectEqual(@as(u32, 16), decoded.payload_length);
}

test "server - connection limit configuration" {
    concurrency.init();

    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const allocator = arena.allocator();

    var sim_vfs = SimulationVFS.init(allocator);
    defer sim_vfs.deinit();

    var storage_engine = try create_test_storage(allocator, sim_vfs.vfs());
    defer storage_engine.deinit();

    var test_query_engine = try create_test_query_engine(allocator, &storage_engine);
    defer test_query_engine.deinit();

    const config = ServerConfig{
        .port = 0,
        .max_connections = 2, // Very low limit for testing
    };

    var server = CortexServer.init(allocator, config, &storage_engine, &test_query_engine);
    defer server.deinit();

    // Verify configuration
    try testing.expectEqual(@as(u32, 2), server.config.max_connections);
    try testing.expectEqual(@as(u32, 0), server.stats.connections_active);
}

test "server stats - initial values" {
    concurrency.init();

    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const allocator = arena.allocator();

    var sim_vfs = SimulationVFS.init(allocator);
    defer sim_vfs.deinit();

    var storage_engine = try create_test_storage(allocator, sim_vfs.vfs());
    defer storage_engine.deinit();

    var test_query_engine = try create_test_query_engine(allocator, &storage_engine);
    defer test_query_engine.deinit();

    const config = ServerConfig{};

    var server = CortexServer.init(allocator, config, &storage_engine, &test_query_engine);
    defer server.deinit();

    // Verify initial stats
    try testing.expectEqual(@as(u64, 0), server.stats.connections_accepted);
    try testing.expectEqual(@as(u32, 0), server.stats.connections_active);
    try testing.expectEqual(@as(u64, 0), server.stats.requests_processed);
    try testing.expectEqual(@as(u64, 0), server.stats.bytes_sent);
    try testing.expectEqual(@as(u64, 0), server.stats.bytes_received);
    try testing.expectEqual(@as(u64, 0), server.stats.errors_encountered);
}

test "protocol - all message types" {
    // Verify all expected message types exist and have correct values
    try testing.expectEqual(@as(u8, 0x01), @intFromEnum(MessageType.ping));
    try testing.expectEqual(@as(u8, 0x02), @intFromEnum(MessageType.get_blocks));
    try testing.expectEqual(@as(u8, 0x03), @intFromEnum(MessageType.filtered_query));
    try testing.expectEqual(@as(u8, 0x04), @intFromEnum(MessageType.traversal_query));

    // Response types
    try testing.expectEqual(@as(u8, 0x81), @intFromEnum(MessageType.pong));
    try testing.expectEqual(@as(u8, 0x82), @intFromEnum(MessageType.blocks_response));
    try testing.expectEqual(@as(u8, 0x83), @intFromEnum(MessageType.filtered_response));
    try testing.expectEqual(@as(u8, 0x84), @intFromEnum(MessageType.traversal_response));
    try testing.expectEqual(@as(u8, 0xFF), @intFromEnum(MessageType.error_response));
}

test "protocol - corrupted header recovery" {
    const valid_header = MessageHeader{
        .msg_type = MessageType.ping,
        .version = 1,
        .reserved = 0,
        .payload_length = 0,
    };

    var buffer: [8]u8 = undefined;
    valid_header.encode(&buffer);

    // Corrupt the buffer at various positions
    const corruption_positions = [_]usize{ 0, 1, 3, 7 };

    for (corruption_positions) |pos| {
        var corrupted_buffer = buffer;
        corrupted_buffer[pos] ^= 0xFF; // Flip all bits at position

        const result = MessageHeader.decode(&corrupted_buffer);

        // Should fail gracefully - most corruption will result in InvalidRequest
        if (result) |_| {
            // Some corruption might still result in valid (but incorrect) headers
            // This is acceptable as long as no crashes occur
        } else |err| {
            try testing.expect(err == error.InvalidRequest);
        }
    }
}

test "server - engine references" {
    concurrency.init();

    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const allocator = arena.allocator();

    var sim_vfs = SimulationVFS.init(allocator);
    defer sim_vfs.deinit();

    var storage_engine = try create_test_storage(allocator, sim_vfs.vfs());
    defer storage_engine.deinit();

    var test_query_engine = try create_test_query_engine(allocator, &storage_engine);
    defer test_query_engine.deinit();

    const config = ServerConfig{};

    var server = CortexServer.init(allocator, config, &storage_engine, &test_query_engine);
    defer server.deinit();

    // Test that server maintains valid references
    try testing.expect(@intFromPtr(server.storage_engine) == @intFromPtr(&storage_engine));
    try testing.expect(@intFromPtr(server.query_engine) == @intFromPtr(&test_query_engine));

    // Verify configuration is properly copied
    try testing.expectEqual(config.port, server.config.port);
    try testing.expectEqual(config.max_connections, server.config.max_connections);
}
