//! KausalDB TCP Server and Protocol Tests
//!
//! Simplified testing of the TCP server implementation and binary protocol.
//! Tests cover basic functionality without complex networking scenarios.

const kausaldb = @import("kausaldb");
const std = @import("std");
const testing = std.testing;
const net = std.net;

const vfs = kausaldb.vfs;
const simulation_vfs = kausaldb.simulation_vfs;
const storage = kausaldb.storage;
const query_engine = kausaldb.query_engine;
const context_block = kausaldb.types;
const server_handler = kausaldb.handler;
const concurrency = kausaldb.concurrency;

const StorageEngine = storage.StorageEngine;
const QueryEngine = query_engine.QueryEngine;
const ContextBlock = context_block.ContextBlock;
const BlockId = context_block.BlockId;
const SimulationVFS = simulation_vfs.SimulationVFS;
const KausalDBServer = server_handler.KausalDBServer;
const ServerConfig = server_handler.ServerConfig;
const MessageHeader = server_handler.MessageHeader;
const MessageType = server_handler.MessageType;
const ClientConnection = server_handler.ClientConnection;
const ConnectionState = server_handler.ConnectionState;

// Helper to create test storage engine
fn create_test_storage(allocator: std.mem.Allocator, vfs_interface: vfs.VFS) !StorageEngine {
    var storage_engine = try StorageEngine.init_default(allocator, vfs_interface, "test_server_db");
    try storage_engine.startup();
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

    const allocator = testing.allocator;

    var sim_vfs = try SimulationVFS.init(allocator);
    defer sim_vfs.deinit();

    var storage_engine = try create_test_storage(allocator, sim_vfs.vfs());
    defer storage_engine.deinit();

    var test_query_engine = try create_test_query_engine(allocator, &storage_engine);
    defer test_query_engine.deinit();

    const config = ServerConfig{
        .port = 0, // Let OS choose available port
        .max_connections = 10,
    };

    var server = KausalDBServer.init(allocator, config, &storage_engine, &test_query_engine);
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

    const allocator = testing.allocator;

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

test "protocol - find_blocks message" {
    const find_blocks_header = MessageHeader{
        .msg_type = MessageType.find_blocks,
        .version = 1,
        .reserved = 0,
        .payload_length = 16, // Size of BlockId
    };

    var buffer: [8]u8 = undefined;
    find_blocks_header.encode(&buffer);

    const decoded = try MessageHeader.decode(&buffer);

    try testing.expectEqual(MessageType.find_blocks, decoded.msg_type);
    try testing.expectEqual(@as(u32, 16), decoded.payload_length);
}

test "server - connection limit configuration" {
    concurrency.init();

    const allocator = testing.allocator;

    var sim_vfs = try SimulationVFS.init(allocator);
    defer sim_vfs.deinit();

    var storage_engine = try create_test_storage(allocator, sim_vfs.vfs());
    defer storage_engine.deinit();

    var test_query_engine = try create_test_query_engine(allocator, &storage_engine);
    defer test_query_engine.deinit();

    const config = ServerConfig{
        .port = 0,
        .max_connections = 2, // Very low limit for testing
    };

    var server = KausalDBServer.init(allocator, config, &storage_engine, &test_query_engine);
    defer server.deinit();

    // Verify configuration
    try testing.expectEqual(@as(u32, 2), server.config.max_connections);
    try testing.expectEqual(@as(u32, 0), server.stats.connections_active);
}

test "server stats - initial values" {
    concurrency.init();

    const allocator = testing.allocator;

    var sim_vfs = try SimulationVFS.init(allocator);
    defer sim_vfs.deinit();

    var storage_engine = try create_test_storage(allocator, sim_vfs.vfs());
    defer storage_engine.deinit();

    var test_query_engine = try create_test_query_engine(allocator, &storage_engine);
    defer test_query_engine.deinit();

    const config = ServerConfig{};

    var server = KausalDBServer.init(allocator, config, &storage_engine, &test_query_engine);
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
    try testing.expectEqual(@as(u8, 0x02), @intFromEnum(MessageType.find_blocks));
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

    const allocator = testing.allocator;

    var sim_vfs = try SimulationVFS.init(allocator);
    defer sim_vfs.deinit();

    var storage_engine = try create_test_storage(allocator, sim_vfs.vfs());
    defer storage_engine.deinit();

    var test_query_engine = try create_test_query_engine(allocator, &storage_engine);
    defer test_query_engine.deinit();

    const config = ServerConfig{};

    var server = KausalDBServer.init(allocator, config, &storage_engine, &test_query_engine);
    defer server.deinit();

    // Test that server maintains valid references
    try testing.expect(@intFromPtr(server.storage_engine) == @intFromPtr(&storage_engine));
    try testing.expect(@intFromPtr(server.query_engine) == @intFromPtr(&test_query_engine));

    // Verify configuration is properly copied
    try testing.expectEqual(config.port, server.config.port);
    try testing.expectEqual(config.max_connections, server.config.max_connections);
}

// Integration tests for async connection state machine
test "connection state machine - header reading with partial I/O" {
    concurrency.init();

    const allocator = testing.allocator;

    const pipe_result = try std.posix.pipe();
    const read_fd = pipe_result[0];
    const write_fd = pipe_result[1];
    defer std.posix.close(read_fd);
    defer std.posix.close(write_fd);

    // Set both ends to non-blocking
    const read_flags = try std.posix.fcntl(read_fd, std.posix.F.GETFL, 0);
    const write_flags = try std.posix.fcntl(write_fd, std.posix.F.GETFL, 0);
    const nonblock_flag = 1 << @bitOffsetOf(std.posix.O, "NONBLOCK");
    _ = try std.posix.fcntl(read_fd, std.posix.F.SETFL, read_flags | nonblock_flag);
    _ = try std.posix.fcntl(write_fd, std.posix.F.SETFL, write_flags | nonblock_flag);

    const mock_stream = std.net.Stream{ .handle = read_fd };
    var connection = ClientConnection.init(allocator, mock_stream, 1);
    defer connection.arena.deinit(); // Only deinit the arena, not the stream

    const server_config = server_handler.ServerConfig{};
    const config = server_config.to_connection_config();

    // Test partial header reads
    try testing.expectEqual(ConnectionState.reading_header, connection.state);

    // Write partial header (4 bytes out of 8)
    const partial_header = [_]u8{ 0x01, 0x01, 0x00, 0x00 }; // ping, version 1, reserved 0
    _ = try std.posix.write(write_fd, &partial_header);

    // Process I/O - should remain in reading_header state
    const keep_alive1 = try connection.process_io(config);
    try testing.expect(keep_alive1);
    try testing.expectEqual(ConnectionState.reading_header, connection.state);
    try testing.expectEqual(@as(usize, 4), connection.header_bytes_read);
    try testing.expect(connection.current_header == null);

    // Write remaining header bytes
    const remaining_header = [_]u8{ 0x00, 0x00, 0x00, 0x00 }; // payload_length = 0
    _ = try std.posix.write(write_fd, &remaining_header);

    // Process I/O - should transition to processing state
    const keep_alive2 = try connection.process_io(config);
    try testing.expect(keep_alive2);
    try testing.expectEqual(ConnectionState.processing, connection.state);
    try testing.expect(connection.current_header != null);
    try testing.expectEqual(MessageType.ping, connection.current_header.?.msg_type);
    try testing.expectEqual(@as(u32, 0), connection.current_header.?.payload_length);
}

test "connection state machine - payload reading with flow control" {
    concurrency.init();

    const allocator = testing.allocator;

    const pipe_result = try std.posix.pipe();
    const read_fd = pipe_result[0];
    const write_fd = pipe_result[1];
    defer std.posix.close(read_fd);
    defer std.posix.close(write_fd);

    // Set both ends to non-blocking
    const read_flags = try std.posix.fcntl(read_fd, std.posix.F.GETFL, 0);
    const write_flags = try std.posix.fcntl(write_fd, std.posix.F.GETFL, 0);
    const nonblock_flag = 1 << @bitOffsetOf(std.posix.O, "NONBLOCK");
    _ = try std.posix.fcntl(read_fd, std.posix.F.SETFL, read_flags | nonblock_flag);
    _ = try std.posix.fcntl(write_fd, std.posix.F.SETFL, write_flags | nonblock_flag);

    const mock_stream = std.net.Stream{ .handle = read_fd };
    var connection = ClientConnection.init(allocator, mock_stream, 2);
    defer connection.arena.deinit(); // Only deinit the arena, not the stream

    const server_config = server_handler.ServerConfig{};
    const config = server_config.to_connection_config();

    // Write complete header for get_blocks with 20-byte payload
    const header = MessageHeader{
        .msg_type = MessageType.find_blocks,
        .version = 1,
        .reserved = 0,
        .payload_length = 20,
    };
    var header_bytes: [MessageHeader.HEADER_SIZE]u8 = undefined;
    header.encode(&header_bytes);
    _ = try std.posix.write(write_fd, &header_bytes);

    // Process header
    const keep_alive1 = try connection.process_io(config);
    try testing.expect(keep_alive1);
    try testing.expectEqual(ConnectionState.reading_payload, connection.state);
    try testing.expect(connection.current_payload != null);
    try testing.expectEqual(@as(usize, 20), connection.current_payload.?.len);

    // Write partial payload (12 bytes out of 20)
    const partial_payload = [_]u8{ 0x01, 0x00, 0x00, 0x00 } ++ // block count = 1
        [_]u8{ 0x00, 0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77 }; // partial block ID
    _ = try std.posix.write(write_fd, &partial_payload);

    // Process partial payload
    const keep_alive2 = try connection.process_io(config);
    try testing.expect(keep_alive2);
    try testing.expectEqual(ConnectionState.reading_payload, connection.state);
    try testing.expectEqual(@as(usize, 12), connection.payload_bytes_read);

    // Write remaining payload
    const remaining_payload = [_]u8{ 0x88, 0x99, 0xAA, 0xBB, 0xCC, 0xDD, 0xEE, 0xFF };
    _ = try std.posix.write(write_fd, &remaining_payload);

    // Process complete payload
    const keep_alive3 = try connection.process_io(config);
    try testing.expect(keep_alive3);
    try testing.expectEqual(ConnectionState.processing, connection.state);
    try testing.expectEqual(@as(usize, 20), connection.payload_bytes_read);
    try testing.expect(connection.has_complete_request());

    const payload = connection.request_payload().?;
    try testing.expectEqual(@as(usize, 20), payload.len);
    try testing.expectEqual(@as(u32, 1), std.mem.readInt(u32, payload[0..4], .little));
}

test "connection state machine - response state transitions" {
    concurrency.init();

    const allocator = testing.allocator;

    const pipe_result = try std.posix.pipe();
    const read_fd = pipe_result[0];
    const write_fd = pipe_result[1];
    defer std.posix.close(read_fd);
    defer std.posix.close(write_fd);

    // Set write end to non-blocking
    const write_flags = try std.posix.fcntl(write_fd, std.posix.F.GETFL, 0);
    const nonblock_flag = 1 << @bitOffsetOf(std.posix.O, "NONBLOCK");
    _ = try std.posix.fcntl(write_fd, std.posix.F.SETFL, write_flags | nonblock_flag);

    const mock_stream = std.net.Stream{ .handle = write_fd };
    var connection = ClientConnection.init(allocator, mock_stream, 3);
    defer connection.arena.deinit(); // Only deinit the arena, not the stream

    // Simulate connection in processing state with a response ready
    connection.state = .processing;
    connection.current_header = MessageHeader{
        .msg_type = MessageType.ping,
        .version = 1,
        .reserved = 0,
        .payload_length = 0,
    };

    // Send response - should transition to writing_response state
    const response_data = "KausalDB server v0.1.0";
    connection.send_response(response_data);
    try testing.expectEqual(ConnectionState.writing_response, connection.state);
    try testing.expect(connection.current_response != null);
    try testing.expectEqual(@as(usize, 0), connection.response_bytes_written);

    // Verify the response data is set correctly
    try testing.expectEqual(response_data.len, connection.current_response.?.len);
    try testing.expectEqualSlices(u8, response_data, connection.current_response.?);
}

test "connection state machine - request size limits" {
    concurrency.init();

    const allocator = testing.allocator;

    const pipe_result = try std.posix.pipe();
    const read_fd = pipe_result[0];
    const write_fd = pipe_result[1];
    defer std.posix.close(read_fd);
    defer std.posix.close(write_fd);

    // Set both ends to non-blocking
    const read_flags = try std.posix.fcntl(read_fd, std.posix.F.GETFL, 0);
    const nonblock_flag = 1 << @bitOffsetOf(std.posix.O, "NONBLOCK");
    _ = try std.posix.fcntl(read_fd, std.posix.F.SETFL, read_flags | nonblock_flag);

    const mock_stream = std.net.Stream{ .handle = read_fd };
    var connection = ClientConnection.init(allocator, mock_stream, 4);
    defer connection.arena.deinit(); // Only deinit the arena, not the stream

    // Use a restrictive config with small max request size
    const server_config = server_handler.ServerConfig{
        .max_request_size = 100, // Very small limit
    };
    const config = server_config.to_connection_config();

    // Write header with oversized payload
    const header = MessageHeader{
        .msg_type = MessageType.find_blocks,
        .version = 1,
        .reserved = 0,
        .payload_length = 1000, // Exceeds limit
    };
    var header_bytes: [MessageHeader.HEADER_SIZE]u8 = undefined;
    header.encode(&header_bytes);
    _ = try std.posix.write(write_fd, &header_bytes);

    // Process header - should reject due to size limit and transition to reading_payload first
    const keep_alive = try connection.process_io(config);
    try testing.expect(!keep_alive); // Connection should be closed due to oversized request
}

test "connection state machine - client disconnection handling" {
    concurrency.init();

    const allocator = testing.allocator;

    const pipe_result = try std.posix.pipe();
    const read_fd = pipe_result[0];
    const write_fd = pipe_result[1];
    defer std.posix.close(read_fd);
    // Close write end immediately to simulate client disconnection
    std.posix.close(write_fd);

    // Set read end to non-blocking
    const read_flags = try std.posix.fcntl(read_fd, std.posix.F.GETFL, 0);
    const nonblock_flag = 1 << @bitOffsetOf(std.posix.O, "NONBLOCK");
    _ = try std.posix.fcntl(read_fd, std.posix.F.SETFL, read_flags | nonblock_flag);

    const mock_stream = std.net.Stream{ .handle = read_fd };
    var connection = ClientConnection.init(allocator, mock_stream, 5);
    defer connection.arena.deinit(); // Only deinit the arena, not the stream

    const server_config = server_handler.ServerConfig{};
    const config = server_config.to_connection_config();

    // Try to read from closed socket
    const keep_alive = try connection.process_io(config);
    try testing.expect(!keep_alive); // Connection should be closed due to EOF
}

test "connection state machine - arena memory isolation" {
    concurrency.init();

    const allocator = testing.allocator;

    const pipe_result = try std.posix.pipe();
    const read_fd = pipe_result[0];
    const write_fd = pipe_result[1];
    defer std.posix.close(read_fd);
    defer std.posix.close(write_fd);

    // Set both ends to non-blocking
    const read_flags = try std.posix.fcntl(read_fd, std.posix.F.GETFL, 0);
    const nonblock_flag = 1 << @bitOffsetOf(std.posix.O, "NONBLOCK");
    _ = try std.posix.fcntl(read_fd, std.posix.F.SETFL, read_flags | nonblock_flag);

    const mock_stream = std.net.Stream{ .handle = read_fd };
    var connection = ClientConnection.init(allocator, mock_stream, 6);
    defer connection.arena.deinit(); // Only deinit the arena, not the stream

    const server_config = server_handler.ServerConfig{};
    const config = server_config.to_connection_config();

    // Process a complete request with payload
    const header = MessageHeader{
        .msg_type = MessageType.find_blocks,
        .version = 1,
        .reserved = 0,
        .payload_length = 20,
    };
    var header_bytes: [MessageHeader.HEADER_SIZE]u8 = undefined;
    header.encode(&header_bytes);
    _ = try std.posix.write(write_fd, &header_bytes);

    const payload_data = [_]u8{ 0x01, 0x00, 0x00, 0x00 } ++ // block count = 1
        [_]u8{ 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0A, 0x0B, 0x0C, 0x0D, 0x0E, 0x0F, 0x10 }; // block ID
    _ = try std.posix.write(write_fd, &payload_data);

    // Process header and payload
    _ = try connection.process_io(config);
    _ = try connection.process_io(config);
    try testing.expectEqual(ConnectionState.processing, connection.state);

    // Verify payload is allocated in connection's arena
    const payload = connection.request_payload().?;
    try testing.expectEqual(@as(usize, 20), payload.len);

    // Simulate response and reset
    const response_data = "test response";
    connection.send_response(response_data);
    try testing.expectEqual(ConnectionState.writing_response, connection.state);

    // Complete the response write (simulate successful write)
    connection.response_bytes_written = response_data.len;
    _ = try connection.process_io(config);

    // Verify connection reset to initial state
    try testing.expectEqual(ConnectionState.reading_header, connection.state);
    try testing.expectEqual(@as(usize, 0), connection.header_bytes_read);
    try testing.expect(connection.current_header == null);
    try testing.expect(connection.current_payload == null);
    try testing.expectEqual(@as(usize, 0), connection.payload_bytes_read);
    try testing.expect(connection.current_response == null);
    try testing.expectEqual(@as(usize, 0), connection.response_bytes_written);
}
