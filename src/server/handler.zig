//! CortexDB TCP Server Implementation
//!
//! Provides a simple, high-performance TCP server for client-server communication.
//! Follows CortexDB architectural principles:
//! - Single-threaded with async I/O
//! - Arena-per-connection memory management
//! - Explicit allocator parameters
//! - Simple binary protocol
//! - Deterministic testing support via abstracted networking

const std = @import("std");
const assert = std.debug.assert;
const log = std.log.scoped(.server);
const concurrency = @import("concurrency");
const storage = @import("storage");
const query_engine = @import("query_engine");
const context_block = @import("context_block");
const custom_assert = @import("assert");
const comptime_assert = custom_assert.comptime_assert;
const comptime_no_padding = custom_assert.comptime_no_padding;

const StorageEngine = storage.StorageEngine;
const QueryEngine = query_engine.QueryEngine;
const ContextBlock = context_block.ContextBlock;
const BlockId = context_block.BlockId;

/// Server configuration
pub const ServerConfig = struct {
    /// Port to listen on
    port: u16 = 8080,
    /// Maximum number of concurrent client connections
    max_connections: u32 = 100,
    /// Connection timeout in seconds
    connection_timeout_sec: u32 = 300,
    /// Maximum request size in bytes
    max_request_size: u32 = 64 * 1024, // 64KB
    /// Maximum response size in bytes
    max_response_size: u32 = 16 * 1024 * 1024, // 16MB
    /// Enable request/response logging
    enable_logging: bool = false,
};

/// Server error types
pub const ServerError = error{
    /// Address already in use
    AddressInUse,
    /// Too many connections
    TooManyConnections,
    /// Connection timeout
    ConnectionTimeout,
    /// Invalid request format
    InvalidRequest,
    /// Request too large
    RequestTooLarge,
    /// Response too large
    ResponseTooLarge,
    /// Client disconnected unexpectedly
    ClientDisconnected,
    /// End of stream
    EndOfStream,
} || std.mem.Allocator.Error || std.net.Stream.ReadError || std.net.Stream.WriteError;

/// Binary protocol message types
pub const MessageType = enum(u8) {
    // Client requests
    ping = 0x01,
    get_blocks = 0x02,
    filtered_query = 0x03,
    traversal_query = 0x04,

    // Server responses
    pong = 0x81,
    blocks_response = 0x82,
    filtered_response = 0x83,
    traversal_response = 0x84,
    error_response = 0xFF,

    pub fn from_u8(value: u8) !MessageType {
        return std.meta.intToEnum(MessageType, value) catch ServerError.InvalidRequest;
    }
};

/// Binary protocol header (8 bytes, aligned)
pub const MessageHeader = packed struct {
    /// Message type
    msg_type: MessageType,
    /// Protocol version (currently 1)
    version: u8 = 1,
    /// Reserved for future use
    reserved: u16 = 0,
    /// Payload length in bytes
    payload_length: u32,

    const HEADER_SIZE = 8;

    pub fn encode(self: MessageHeader, buffer: []u8) void {
        assert(buffer.len >= HEADER_SIZE);
        buffer[0] = @intFromEnum(self.msg_type);
        buffer[1] = self.version;
        std.mem.writeInt(u16, buffer[2..4], self.reserved, .little);
        std.mem.writeInt(u32, buffer[4..8], self.payload_length, .little);
    }

    pub fn decode(buffer: []const u8) !MessageHeader {
        if (buffer.len < HEADER_SIZE) return ServerError.InvalidRequest;

        return MessageHeader{
            .msg_type = try MessageType.from_u8(buffer[0]),
            .version = buffer[1],
            .reserved = std.mem.readInt(u16, buffer[2..4], .little),
            .payload_length = std.mem.readInt(u32, buffer[4..8], .little),
        };
    }
};

// Compile-time guarantees for network protocol stability
comptime {
    comptime_assert(@sizeOf(MessageHeader) == 8, "MessageHeader must be exactly 8 bytes for network protocol compatibility");
    comptime_assert(MessageHeader.HEADER_SIZE == @sizeOf(MessageHeader), "MessageHeader.HEADER_SIZE constant must match actual struct size");
    comptime_no_padding(MessageHeader);
    comptime_assert(@sizeOf(MessageType) == 1, "MessageType must be 1 byte");
    comptime_assert(@sizeOf(u8) == 1, "version field must be 1 byte");
    comptime_assert(@sizeOf(u16) == 2, "reserved field must be 2 bytes");
    comptime_assert(@sizeOf(u32) == 4, "payload_length field must be 4 bytes");
}

/// Client connection state
pub const ClientConnection = struct {
    /// TCP stream for this connection
    stream: std.net.Stream,
    /// Arena allocator for this connection's memory
    arena: std.heap.ArenaAllocator,
    /// Connection ID for logging
    connection_id: u32,
    /// When this connection was established
    established_time: i64,
    /// Buffer for reading requests
    read_buffer: [4096]u8,
    /// Buffer for writing responses
    write_buffer: [4096]u8,

    pub fn init(allocator: std.mem.Allocator, stream: std.net.Stream, connection_id: u32) ClientConnection {
        return ClientConnection{
            .stream = stream,
            .arena = std.heap.ArenaAllocator.init(allocator),
            .connection_id = connection_id,
            .established_time = std.time.timestamp(),
            .read_buffer = std.mem.zeroes([4096]u8),
            .write_buffer = std.mem.zeroes([4096]u8),
        };
    }

    pub fn deinit(self: *ClientConnection) void {
        self.stream.close();
        self.arena.deinit();
    }

    /// Read a complete message from the client
    pub fn read_message(self: *ClientConnection, config: ServerConfig) ![]u8 {
        const allocator = self.arena.allocator();

        // Read message header first
        var header_bytes: [MessageHeader.HEADER_SIZE]u8 = undefined;
        var bytes_read: usize = 0;
        while (bytes_read < header_bytes.len) {
            const n = try self.stream.read(header_bytes[bytes_read..]);
            if (n == 0) return ServerError.ClientDisconnected;
            bytes_read += n;
        }

        const header = try MessageHeader.decode(&header_bytes);

        // Validate payload size
        if (header.payload_length > config.max_request_size) {
            return ServerError.RequestTooLarge;
        }

        // Read payload if present
        if (header.payload_length == 0) {
            return &[_]u8{}; // Empty payload
        }

        const payload = try allocator.alloc(u8, header.payload_length);
        bytes_read = 0;
        while (bytes_read < payload.len) {
            const n = try self.stream.read(payload[bytes_read..]);
            if (n == 0) return ServerError.ClientDisconnected;
            bytes_read += n;
        }

        return payload;
    }

    /// Write a message to the client
    pub fn write_message(self: *ClientConnection, msg_type: MessageType, payload: []const u8, config: ServerConfig) !void {
        if (payload.len > config.max_response_size) {
            return ServerError.ResponseTooLarge;
        }

        const header = MessageHeader{
            .msg_type = msg_type,
            .payload_length = @intCast(payload.len),
        };

        // Write header
        var header_bytes: [MessageHeader.HEADER_SIZE]u8 = undefined;
        header.encode(&header_bytes);
        _ = try self.stream.writeAll(&header_bytes);

        // Write payload if present
        if (payload.len > 0) {
            _ = try self.stream.writeAll(payload);
        }
    }
};

/// Main TCP server
pub const CortexServer = struct {
    /// Base allocator for server infrastructure
    allocator: std.mem.Allocator,
    /// Server configuration
    config: ServerConfig,
    /// Storage engine reference
    storage_engine: *StorageEngine,
    /// Query engine reference
    query_engine: *QueryEngine,
    /// TCP listener
    listener: ?std.net.Server = null,
    /// Active client connections
    connections: std.ArrayList(*ClientConnection),
    /// Next connection ID
    next_connection_id: u32,
    /// Server statistics
    stats: ServerStats,

    pub const ServerStats = struct {
        connections_accepted: u64 = 0,
        connections_active: u32 = 0,
        requests_processed: u64 = 0,
        bytes_received: u64 = 0,
        bytes_sent: u64 = 0,
        errors_encountered: u64 = 0,

        pub fn format_human_readable(self: ServerStats, writer: anytype) !void {
            try writer.print("Server Statistics:\n");
            try writer.print("  Connections: {} accepted, {} active\n", .{ self.connections_accepted, self.connections_active });
            try writer.print("  Requests: {} processed\n", .{self.requests_processed});
            try writer.print("  Traffic: {} bytes received, {} bytes sent\n", .{ self.bytes_received, self.bytes_sent });
            try writer.print("  Errors: {} encountered\n", .{self.errors_encountered});
        }
    };

    /// Initialize server with storage and query engines
    pub fn init(
        allocator: std.mem.Allocator,
        config: ServerConfig,
        storage_engine: *StorageEngine,
        query_eng: *QueryEngine,
    ) CortexServer {
        return CortexServer{
            .allocator = allocator,
            .config = config,
            .storage_engine = storage_engine,
            .query_engine = query_eng,
            .connections = std.ArrayList(*ClientConnection).init(allocator),
            .next_connection_id = 1,
            .stats = ServerStats{},
        };
    }

    /// Clean up server resources
    pub fn deinit(self: *CortexServer) void {
        self.stop();

        // Clean up active connections
        for (self.connections.items) |connection| {
            connection.deinit();
            self.allocator.destroy(connection);
        }
        self.connections.deinit();
    }

    /// Start the server and listen for connections
    pub fn start(self: *CortexServer) !void {
        concurrency.assert_main_thread();

        const address = std.net.Address.initIp4(.{ 127, 0, 0, 1 }, self.config.port);
        self.listener = try address.listen(.{ .reuse_address = true });

        log.info("CortexDB server started on port {d}", .{self.config.port});
        log.info("Server config: max_connections={d}, timeout={d}s", .{ self.config.max_connections, self.config.connection_timeout_sec });

        // Main server loop
        while (true) {
            const connection = self.listener.?.accept() catch |err| switch (err) {
                error.ConnectionAborted => continue,
                else => return err,
            };

            try self.handle_new_connection(connection);
        }
    }

    /// Stop the server
    pub fn stop(self: *CortexServer) void {
        if (self.listener) |*listener| {
            listener.deinit();
            self.listener = null;
        }
        log.info("CortexDB server stopped", .{});
    }

    /// Handle a new client connection
    fn handle_new_connection(self: *CortexServer, tcp_connection: std.net.Server.Connection) !void {
        // Check connection limit
        if (self.connections.items.len >= self.config.max_connections) {
            tcp_connection.stream.close();
            self.stats.errors_encountered += 1;
            log.warn("Connection rejected: max connections ({d}) exceeded", .{self.config.max_connections});
            return;
        }

        // Create client connection
        const connection = try self.allocator.create(ClientConnection);
        connection.* = ClientConnection.init(self.allocator, tcp_connection.stream, self.next_connection_id);
        self.next_connection_id += 1;

        try self.connections.append(connection);
        self.stats.connections_accepted += 1;
        self.stats.connections_active += 1;

        log.info("New connection {d} from {any}", .{ connection.connection_id, tcp_connection.address });

        // Handle connection in place (single-threaded)
        self.handle_connection(connection) catch |err| {
            log.err("Connection {d} error: {any}", .{ connection.connection_id, err });
            self.stats.errors_encountered += 1;
        };

        // Clean up connection
        self.cleanup_connection(connection);
    }

    /// Handle requests from a client connection
    fn handle_connection(self: *CortexServer, connection: *ClientConnection) !void {
        defer log.info("Connection {d} closed", .{connection.connection_id});

        while (true) {
            // Read request message
            const payload = connection.read_message(self.config) catch |err| switch (err) {
                error.EndOfStream => break, // Client disconnected
                error.ConnectionResetByPeer => break,
                else => return err,
            };
            defer connection.arena.allocator().free(payload);

            self.stats.requests_processed += 1;
            self.stats.bytes_received += payload.len + MessageHeader.HEADER_SIZE;

            if (self.config.enable_logging) {
                log.debug("Connection {d}: received {d} bytes", .{ connection.connection_id, payload.len });
            }

            // Process the request (implementation continues below)
            try self.process_request(connection, payload);
        }
    }

    /// Process a client request and send response
    fn process_request(self: *CortexServer, connection: *ClientConnection, payload: []const u8) !void {
        const allocator = connection.arena.allocator();

        // Read the message header from the beginning of the payload
        if (payload.len < MessageHeader.HEADER_SIZE) {
            try self.send_error_response(connection, "Invalid request: header too small");
            return;
        }

        const header = MessageHeader.decode(payload[0..MessageHeader.HEADER_SIZE]) catch {
            try self.send_error_response(connection, "Invalid request: malformed header");
            return;
        };

        const request_payload = if (header.payload_length > 0) payload[MessageHeader.HEADER_SIZE..] else &[_]u8{};

        switch (header.msg_type) {
            .ping => {
                try connection.write_message(.pong, "CortexDB server v0.1.0", self.config);
                self.stats.bytes_sent += "CortexDB server v0.1.0".len + MessageHeader.HEADER_SIZE;

                if (self.config.enable_logging) {
                    log.debug("Connection {d}: handled ping request", .{connection.connection_id});
                }
            },

            .get_blocks => {
                try self.handle_get_blocks_request(connection, allocator, request_payload);
            },

            .filtered_query => {
                try self.handle_filtered_query_request(connection, allocator, request_payload);
            },

            .traversal_query => {
                try self.handle_traversal_query_request(connection, allocator, request_payload);
            },

            // Server response types should not be sent by clients
            .pong, .blocks_response, .filtered_response, .traversal_response, .error_response => {
                try self.send_error_response(connection, "Invalid request: client sent server response type");
            },
        }
    }

    /// Send an error response to the client
    fn send_error_response(self: *CortexServer, connection: *ClientConnection, error_msg: []const u8) !void {
        try connection.write_message(.error_response, error_msg, self.config);
        self.stats.bytes_sent += error_msg.len + MessageHeader.HEADER_SIZE;
        self.stats.errors_encountered += 1;

        if (self.config.enable_logging) {
            log.warn("Connection {d}: sent error response: {s}", .{ connection.connection_id, error_msg });
        }
    }

    /// Handle get_blocks request - retrieve specific blocks by ID
    fn handle_get_blocks_request(self: *CortexServer, connection: *ClientConnection, allocator: std.mem.Allocator, payload: []const u8) !void {
        // Parse block IDs from payload (simple format: count + list of 16-byte block IDs)
        if (payload.len < 4) {
            try self.send_error_response(connection, "Invalid get_blocks request: missing block count");
            return;
        }

        const block_count = std.mem.readInt(u32, payload[0..4], .little);
        if (block_count == 0) {
            try self.send_error_response(connection, "Invalid get_blocks request: zero blocks requested");
            return;
        }

        if (payload.len < 4 + (block_count * 16)) {
            try self.send_error_response(connection, "Invalid get_blocks request: insufficient payload for block IDs");
            return;
        }

        // Extract block IDs
        var block_ids = try allocator.alloc(BlockId, block_count);
        for (0..block_count) |i| {
            const id_start = 4 + (i * 16);
            const id_bytes = payload[id_start .. id_start + 16];
            block_ids[i] = BlockId{ .bytes = id_bytes[0..16].* };
        }

        // Build query and execute
        const query = query_engine.GetBlocksQuery{ .block_ids = block_ids };
        const result = self.query_engine.execute_get_blocks(query) catch |err| {
            const error_msg = try std.fmt.allocPrint(allocator, "Query execution failed: {any}", .{err});
            try self.send_error_response(connection, error_msg);
            return;
        };
        defer result.deinit();

        // Serialize response
        const response_data = try self.serialize_blocks_response(allocator, result);
        try connection.write_message(.blocks_response, response_data, self.config);
        self.stats.bytes_sent += response_data.len + MessageHeader.HEADER_SIZE;

        if (self.config.enable_logging) {
            log.debug("Connection {d}: handled get_blocks request, returned {d} blocks", .{ connection.connection_id, result.count });
        }
    }

    /// Handle filtered query request - retrieve blocks matching metadata criteria
    fn handle_filtered_query_request(self: *CortexServer, connection: *ClientConnection, allocator: std.mem.Allocator, payload: []const u8) !void {
        _ = payload;

        const empty_blocks: []const ContextBlock = &[_]ContextBlock{};
        const common_result = query_engine.QueryResult.init(allocator, empty_blocks);
        const response_data = try self.serialize_blocks_response(allocator, common_result);
        try connection.write_message(.filtered_response, response_data, self.config);
        self.stats.bytes_sent += response_data.len + MessageHeader.HEADER_SIZE;

        if (self.config.enable_logging) {
            log.debug("Connection {d}: handled filtered query (placeholder), returned {d} blocks", .{ connection.connection_id, common_result.count });
        }
    }

    /// Handle traversal query request - perform graph traversal from starting blocks
    fn handle_traversal_query_request(self: *CortexServer, connection: *ClientConnection, allocator: std.mem.Allocator, payload: []const u8) !void {
        // Parse traversal parameters from payload
        if (payload.len < 20) { // minimum: 4 bytes count + 16 bytes start ID
            try self.send_error_response(connection, "Invalid traversal request: insufficient payload");
            return;
        }

        const start_count = std.mem.readInt(u32, payload[0..4], .little);
        if (start_count == 0) {
            try self.send_error_response(connection, "Invalid traversal request: no starting blocks");
            return;
        }

        const start_id_bytes = payload[4..20];
        const start_id = BlockId{ .bytes = start_id_bytes[0..16].* };

        var result_blocks = std.ArrayList(ContextBlock).init(allocator);
        defer result_blocks.deinit();

        const start_block = self.storage_engine.find_block_by_id(start_id) catch |err| {
            const error_msg = try std.fmt.allocPrint(allocator, "Starting block not found: {any}", .{err});
            try self.send_error_response(connection, error_msg);
            return;
        };

        try result_blocks.append(start_block.*);
        const common_result = query_engine.QueryResult.init(allocator, result_blocks.items);
        const response_data = try self.serialize_blocks_response(allocator, common_result);
        try connection.write_message(.traversal_response, response_data, self.config);
        self.stats.bytes_sent += response_data.len + MessageHeader.HEADER_SIZE;

        if (self.config.enable_logging) {
            log.debug("Connection {d}: handled traversal query, returned {d} blocks", .{ connection.connection_id, common_result.count });
        }
    }

    /// Serialize query results into binary format for network transmission
    fn serialize_blocks_response(self: *CortexServer, allocator: std.mem.Allocator, result: query_engine.QueryResult) ![]u8 {
        _ = self; // Suppress unused parameter warning

        // Calculate total size needed
        var total_size: usize = 4; // 4 bytes for block count
        for (0..result.count) |i| {
            const block = result.blocks[i];
            total_size += 16; // Block ID
            total_size += 4 + block.source_uri.len; // URI length + URI
            total_size += 4 + block.metadata_json.len; // Metadata length + metadata
            total_size += 4 + block.content.len; // Content length + content
        }

        // Allocate and serialize
        const buffer = try allocator.alloc(u8, total_size);
        var offset: usize = 0;

        // Write block count
        std.mem.writeInt(u32, buffer[offset .. offset + 4][0..4], @intCast(result.count), .little);
        offset += 4;

        // Write each block
        for (0..result.count) |i| {
            const block = result.blocks[i];

            // Block ID (16 bytes)
            @memcpy(buffer[offset .. offset + 16], &block.id.bytes);
            offset += 16;

            // Source URI
            std.mem.writeInt(u32, buffer[offset .. offset + 4][0..4], @intCast(block.source_uri.len), .little);
            offset += 4;
            @memcpy(buffer[offset .. offset + block.source_uri.len], block.source_uri);
            offset += block.source_uri.len;

            // Metadata JSON
            std.mem.writeInt(u32, buffer[offset .. offset + 4][0..4], @intCast(block.metadata_json.len), .little);
            offset += 4;
            @memcpy(buffer[offset .. offset + block.metadata_json.len], block.metadata_json);
            offset += block.metadata_json.len;

            // Content
            std.mem.writeInt(u32, buffer[offset .. offset + 4][0..4], @intCast(block.content.len), .little);
            offset += 4;
            @memcpy(buffer[offset .. offset + block.content.len], block.content);
            offset += block.content.len;
        }

        assert(offset == total_size);
        return buffer;
    }

    /// Remove a connection from the active list
    fn cleanup_connection(self: *CortexServer, connection: *ClientConnection) void {
        // Find and remove connection from list
        for (self.connections.items, 0..) |conn, i| {
            if (conn == connection) {
                _ = self.connections.swapRemove(i);
                break;
            }
        }

        connection.deinit();
        self.allocator.destroy(connection);
        self.stats.connections_active -= 1;
    }

    /// Get current server statistics
    pub fn statistics(self: *const CortexServer) ServerStats {
        return self.stats;
    }
};

test "message header encode/decode" {
    const testing = std.testing;

    const original = MessageHeader{
        .msg_type = .ping,
        .payload_length = 1024,
    };

    var buffer: [8]u8 = undefined;
    original.encode(&buffer);

    const decoded = try MessageHeader.decode(&buffer);
    try testing.expectEqual(original.msg_type, decoded.msg_type);
    try testing.expectEqual(original.version, decoded.version);
    try testing.expectEqual(original.payload_length, decoded.payload_length);
}

test "server initialization" {
    const testing = std.testing;
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    // Mock storage and query engines for testing
    // In real usage these would be properly initialized
    var mock_storage: StorageEngine = undefined;
    var mock_query: QueryEngine = undefined;

    const config = ServerConfig{ .port = 0 }; // Use ephemeral port for testing
    var server = CortexServer.init(allocator, config, &mock_storage, &mock_query);
    defer server.deinit();

    try testing.expectEqual(@as(u32, 0), server.statistics().connections_active);
    try testing.expectEqual(@as(u64, 0), server.statistics().requests_processed);
}
