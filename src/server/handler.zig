//! KausalDB TCP Server Implementation
//!
//! Provides a simple, high-performance TCP server for client-server communication.
//! Orchestrates the async I/O event loop and manages multiple client connections.
//! Follows KausalDB architectural principles:
//! - Single-threaded with async I/O event loop
//! - Connection state machines for non-blocking I/O
//! - Arena-per-connection memory management
//! - Explicit allocator parameters
//! - Deterministic testing support via abstracted networking

const std = @import("std");
const custom_assert = @import("../core/assert.zig");
const assert = custom_assert.assert;
const comptime_assert = custom_assert.comptime_assert;
const comptime_no_padding = custom_assert.comptime_no_padding;
const log = std.log.scoped(.server);
const concurrency = @import("../core/concurrency.zig");
const storage = @import("../storage/engine.zig");
const query_engine = @import("../query/engine.zig");
const ctx_block = @import("../core/types.zig");
const ownership = @import("../core/ownership.zig");
const error_context = @import("../core/error_context.zig");

const conn = @import("connection.zig");
const connection_manager = @import("connection_manager.zig");
pub const ClientConnection = conn.ClientConnection;
pub const MessageType = conn.MessageType;
pub const MessageHeader = conn.MessageHeader;
pub const ConnectionState = conn.ConnectionState;
pub const ConnectionManager = connection_manager.ConnectionManager;

const StorageEngine = storage.StorageEngine;
const QueryResult = query_engine.QueryResult;
const QueryEngine = query_engine.QueryEngine;
const ContextBlock = ctx_block.ContextBlock;
const BlockId = ctx_block.BlockId;

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

    /// Convert to connection-level configuration
    pub fn to_connection_config(self: ServerConfig) conn.ServerConfig {
        return conn.ServerConfig{
            .max_request_size = self.max_request_size,
            .max_response_size = self.max_response_size,
            .enable_logging = self.enable_logging,
        };
    }
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

/// Main TCP server coordinator.
/// Delegates connection management to ConnectionManager and focuses on
/// request processing coordination. Follows pure coordinator pattern:
/// owns no state, orchestrates operations between managers.
pub const Server = struct {
    /// Base allocator for server infrastructure
    allocator: std.mem.Allocator,
    /// Server configuration
    config: ServerConfig,
    /// Storage engine reference for request processing
    storage_engine: *StorageEngine,
    /// Query engine reference for request processing
    query_engine: *QueryEngine,
    /// TCP listener socket
    listener: ?std.net.Server = null,
    /// Connection state manager
    connection_manager: ConnectionManager,
    /// Server-level statistics (aggregated from managers)
    stats: ServerStats,

    pub const ServerStats = struct {
        connections_accepted: u64 = 0,
        connections_active: u32 = 0,
        requests_processed: u64 = 0,
        bytes_received: u64 = 0,
        bytes_sent: u64 = 0,
        errors_encountered: u64 = 0,

        /// Format server statistics in human-readable text format
        ///
        /// Prints server metrics including connection counts, traffic, and errors.
        /// Used for server monitoring and debugging.
        pub fn format_human_readable(self: ServerStats, writer: anytype) !void {
            try writer.print("Server Statistics:\n");
            try writer.print("  Connections: {} accepted, {} active\n", .{ self.connections_accepted, self.connections_active });
            try writer.print("  Requests: {} processed\n", .{self.requests_processed});
            try writer.print("  Traffic: {} bytes received, {} bytes sent\n", .{ self.bytes_received, self.bytes_sent });
            try writer.print("  Errors: {} encountered\n", .{self.errors_encountered});
        }
    };

    /// Phase 1 initialization: Memory-only setup following coordinator pattern.
    /// Initializes ConnectionManager and sets up coordination structures.
    pub fn init(
        allocator: std.mem.Allocator,
        config: ServerConfig,
        storage_engine: *StorageEngine,
        query_eng: *QueryEngine,
    ) Server {
        const conn_config = connection_manager.ConnectionManagerConfig{
            .max_connections = config.max_connections,
            .connection_timeout_sec = config.connection_timeout_sec,
            .poll_timeout_ms = 1000, // Standard poll timeout
        };

        return Server{
            .allocator = allocator,
            .config = config,
            .storage_engine = storage_engine,
            .query_engine = query_eng,
            .listener = null,
            .connection_manager = ConnectionManager.init(allocator, conn_config),
            .stats = ServerStats{},
        };
    }

    /// Clean up all server resources including managers
    pub fn deinit(self: *Server) void {
        self.stop();

        self.connection_manager.deinit();
    }

    /// Phase 2 initialization: Start managers and bind listener socket.
    /// Delegates to ConnectionManager startup and performs network binding.
    pub fn startup(self: *Server) !void {
        // Manager must be started before binding to allocate poll_fds
        try self.connection_manager.startup();

        try self.bind();
        try self.run();
    }

    /// Bind to socket and prepare for connections (non-blocking)
    pub fn bind(self: *Server) !void {
        concurrency.assert_main_thread();

        const address = std.net.Address.initIp4(.{ 127, 0, 0, 1 }, self.config.port);
        self.listener = try address.listen(.{ .reuse_address = true });

        const flags = try std.posix.fcntl(self.listener.?.stream.handle, std.posix.F.GETFL, 0);
        const nonblock_flag = 1 << @bitOffsetOf(std.posix.O, "NONBLOCK");
        _ = try std.posix.fcntl(self.listener.?.stream.handle, std.posix.F.SETFL, flags | nonblock_flag);

        log.info("KausalDB server bound to port {d}", .{self.bound_port()});
        log.info("Server config: max_connections={d}, timeout={d}s", .{ self.config.max_connections, self.config.connection_timeout_sec });
    }

    /// Query the actual port the server is bound to (useful for ephemeral ports)
    pub fn bound_port(self: *const Server) u16 {
        if (self.listener) |listener| {
            return listener.listen_address.getPort();
        }
        return self.config.port;
    }

    /// Run the coordination loop delegating to ConnectionManager for I/O.
    /// Pure coordinator: polls for ready connections, processes requests.
    pub fn run(self: *Server) !void {
        concurrency.assert_main_thread();
        try self.run_coordination_loop();
    }

    /// Main coordination loop: delegate I/O to ConnectionManager, handle requests.
    /// Demonstrates coordinator pattern - no direct I/O, only orchestration.
    fn run_coordination_loop(self: *Server) !void {
        const listener = &self.listener.?;

        while (true) {
            const ready_connections = self.connection_manager.poll_for_ready_connections(listener) catch |err| {
                const ctx = error_context.ServerContext{ .operation = "poll_connections" };
                error_context.log_server_error(err, ctx);
                continue; // Server must remain available despite I/O errors
            };

            for (ready_connections) |connection| {
                const keep_alive = self.connection_manager.process_connection_io(connection, self.config.to_connection_config()) catch |err| {
                    const ctx = error_context.connection_context("process_io", connection.connection_id);
                    error_context.log_server_error(err, ctx);
                    continue; // Individual connection errors don't stop server
                };
                _ = keep_alive;
            }

            while (self.connection_manager.find_connection_with_ready_request()) |connection| {
                try self.process_connection_request(connection);
            }

            self.update_aggregated_statistics();
        }
    }

    /// Update server statistics by aggregating from ConnectionManager.
    /// Server owns no connection state, delegates to manager for statistics.
    pub fn update_aggregated_statistics(self: *Server) void {
        const conn_stats = self.connection_manager.statistics();

        self.stats.connections_accepted = conn_stats.connections_accepted;
        self.stats.connections_active = conn_stats.connections_active;
    }

    /// Process a complete request from a connection
    fn process_connection_request(self: *Server, connection: *ClientConnection) !void {
        const payload = connection.request_payload() orelse return;
        const header = connection.current_header orelse return;

        switch (header.msg_type) {
            .ping => {
                const response = "KausalDB server v0.1.0";
                connection.send_response(response);
                self.stats.bytes_sent += response.len + MessageHeader.HEADER_SIZE;

                if (self.config.enable_logging) {
                    log.debug("Connection {d}: handled ping request", .{connection.connection_id});
                }
            },

            .find_blocks => {
                try self.handle_find_blocks_request_async(connection, payload);
            },

            .filtered_query => {
                try self.handle_filtered_query_request_async(connection, payload);
            },

            .traversal_query => {
                try self.handle_traversal_query_request_async(connection, payload);
            },

            .pong, .blocks_response, .filtered_response, .traversal_response, .error_response => {
                const error_msg = "Invalid request: client sent server response type";
                connection.send_response(error_msg);
                self.stats.errors_encountered += 1;
            },
        }

        self.stats.requests_processed += 1;
        self.stats.bytes_received += payload.len + MessageHeader.HEADER_SIZE;
    }

    /// Stop the server
    pub fn stop(self: *Server) void {
        if (self.listener) |*listener| {
            listener.deinit();
            self.listener = null;
        }
        log.info("KausalDB server stopped", .{});
    }

    /// Handle find_blocks request asynchronously
    fn handle_find_blocks_request_async(
        self: *Server,
        connection: *ClientConnection,
        payload: []const u8,
    ) !void {
        const allocator = connection.arena.allocator();

        if (payload.len < 4) {
            const error_msg = "Invalid find_blocks request: missing block count";
            connection.send_response(error_msg);
            self.stats.errors_encountered += 1;
            return;
        }

        const block_count = std.mem.readInt(u32, payload[0..4], .little);
        if (block_count == 0) {
            const error_msg = "Invalid find_blocks request: zero blocks requested";
            connection.send_response(error_msg);
            self.stats.errors_encountered += 1;
            return;
        }

        if (payload.len < 4 + (block_count * 16)) {
            const error_msg = "Invalid find_blocks request: insufficient payload for block IDs";
            connection.send_response(error_msg);
            self.stats.errors_encountered += 1;
            return;
        }

        var found_blocks = std.ArrayList(ownership.QueryEngineBlock).init(allocator);
        defer found_blocks.deinit();

        for (0..block_count) |i| {
            const id_start = 4 + (i * 16);
            const id_bytes = payload[id_start .. id_start + 16];
            const block_id = ctx_block.BlockId{ .bytes = id_bytes[0..16].* };

            const maybe_block = self.query_engine.find_block(block_id) catch |err| {
                const ctx = error_context.ServerContext{
                    .operation = "find_block",
                    .connection_id = connection.connection_id,
                    .message_size = block_count,
                };
                error_context.log_server_error(err, ctx);
                const error_msg = try std.fmt.allocPrint(allocator, "Query execution failed: {any}", .{err});
                connection.send_response(error_msg);
                self.stats.errors_encountered += 1;
                return;
            };

            if (maybe_block) |block| {
                try found_blocks.append(block);
            }
        }

        const response_data = try self.serialize_blocks_array(allocator, found_blocks.items);
        connection.send_response(response_data);
        self.stats.bytes_sent += response_data.len + MessageHeader.HEADER_SIZE;

        if (self.config.enable_logging) {
            log.debug("Connection {d}: handled find_blocks request, returned {d} blocks", .{ connection.connection_id, found_blocks.items.len });
        }
    }

    /// Handle filtered query request asynchronously
    fn handle_filtered_query_request_async(
        self: *Server,
        connection: *ClientConnection,
        payload: []const u8,
    ) !void {
        _ = payload;
        const allocator = connection.arena.allocator();

        const empty_block_ids: []const BlockId = &[_]BlockId{};
        const common_result = QueryResult.init(allocator, self.storage_engine, empty_block_ids);
        var mutable_result = common_result;
        const response_data = try self.serialize_blocks_response(allocator, &mutable_result);
        connection.send_response(response_data);
        self.stats.bytes_sent += response_data.len + MessageHeader.HEADER_SIZE;

        if (self.config.enable_logging) {
            log.debug("Connection {d}: handled filtered query (placeholder), returned {d} blocks", .{ connection.connection_id, common_result.total_found });
        }
    }

    /// Handle traversal query request asynchronously
    fn handle_traversal_query_request_async(
        self: *Server,
        connection: *ClientConnection,
        payload: []const u8,
    ) !void {
        const allocator = connection.arena.allocator();

        if (payload.len < 20) { // minimum: 4 bytes count + 16 bytes start ID
            const error_msg = "Invalid traversal request: insufficient payload";
            connection.send_response(error_msg);
            self.stats.errors_encountered += 1;
            return;
        }

        const start_count = std.mem.readInt(u32, payload[0..4], .little);
        if (start_count == 0) {
            const error_msg = "Invalid traversal request: no starting blocks";
            connection.send_response(error_msg);
            self.stats.errors_encountered += 1;
            return;
        }

        const start_id_bytes = payload[4..20];
        const start_id = ctx_block.BlockId{ .bytes = start_id_bytes[0..16].* };

        var result_blocks = std.ArrayList(ownership.QueryEngineBlock).init(allocator);
        defer result_blocks.deinit();

        const start_block = (try self.storage_engine.find_block(start_id)) orelse {
            const error_msg = try std.fmt.allocPrint(allocator, "Starting block not found", .{});
            connection.send_response(error_msg);
            self.stats.errors_encountered += 1;
            return;
        };

        try result_blocks.append(start_block);
        const block_ids = try allocator.alloc(BlockId, result_blocks.items.len);
        for (result_blocks.items, 0..) |block, i| {
            const block_data = block.read(.query_engine);
            block_ids[i] = block_data.id;
        }
        const common_result = QueryResult.init_with_owned_ids(allocator, self.storage_engine, block_ids);
        var mutable_result = common_result;
        const response_data = try self.serialize_blocks_response(allocator, &mutable_result);
        connection.send_response(response_data);
        self.stats.bytes_sent += response_data.len + MessageHeader.HEADER_SIZE;

        if (self.config.enable_logging) {
            log.debug("Connection {d}: handled traversal query, returned {d} blocks", .{ connection.connection_id, common_result.total_found });
        }
    }

    /// Serialize query results into binary format for network transmission using streaming
    fn serialize_blocks_response(_: *Server, allocator: std.mem.Allocator, result_ptr: *QueryResult) ![]u8 {
        var total_size: usize = 4; // 4 bytes for block count
        var block_count: u32 = 0;

        while (try result_ptr.next()) |block| {
            block_count += 1;
            total_size += 16;
            const context_block = block.read(.query_engine);
            total_size += 4 + context_block.source_uri.len;
            total_size += 4 + context_block.metadata_json.len;
            total_size += 4 + context_block.content.len;
        }

        const buffer = try allocator.alloc(u8, total_size);
        var offset: usize = 0;

        std.mem.writeInt(u32, buffer[offset .. offset + 4][0..4], block_count, .little);
        offset += 4;

        result_ptr.reset();

        while (try result_ptr.next()) |block| {
            const context_block = block.read(.query_engine);
            @memcpy(buffer[offset .. offset + 16], &context_block.id.bytes);
            offset += 16;

            std.mem.writeInt(u32, buffer[offset .. offset + 4][0..4], @intCast(context_block.source_uri.len), .little);
            offset += 4;
            @memcpy(buffer[offset .. offset + context_block.source_uri.len], context_block.source_uri);
            offset += context_block.source_uri.len;

            std.mem.writeInt(u32, buffer[offset .. offset + 4][0..4], @intCast(context_block.metadata_json.len), .little);
            offset += 4;
            @memcpy(buffer[offset .. offset + context_block.metadata_json.len], context_block.metadata_json);
            offset += context_block.metadata_json.len;

            std.mem.writeInt(u32, buffer[offset .. offset + 4][0..4], @intCast(context_block.content.len), .little);
            offset += 4;
            @memcpy(buffer[offset .. offset + context_block.content.len], context_block.content);
            offset += context_block.content.len;
        }

        assert(offset == total_size);
        return buffer;
    }

    /// Serialize array of blocks into binary format for network transmission
    fn serialize_blocks_array(
        _: *Server,
        allocator: std.mem.Allocator,
        blocks: []const ownership.QueryEngineBlock,
    ) ![]u8 {
        var total_size: usize = 4; // 4 bytes for block count

        for (blocks) |block| {
            const block_data = block.read(.query_engine);
            total_size += 16; // Block ID
            total_size += 4 + block_data.source_uri.len;
            total_size += 4 + block_data.metadata_json.len;
            total_size += 4 + block_data.content.len;
        }

        const buffer = try allocator.alloc(u8, total_size);
        var offset: usize = 0;

        std.mem.writeInt(u32, buffer[offset .. offset + 4][0..4], @intCast(blocks.len), .little);
        offset += 4;

        for (blocks) |block| {
            const block_data = block.read(.query_engine);
            @memcpy(buffer[offset .. offset + 16], &block_data.id.bytes);
            offset += 16;

            std.mem.writeInt(u32, buffer[offset .. offset + 4][0..4], @intCast(block_data.source_uri.len), .little);
            offset += 4;
            @memcpy(buffer[offset .. offset + block_data.source_uri.len], block_data.source_uri);
            offset += block_data.source_uri.len;

            std.mem.writeInt(u32, buffer[offset .. offset + 4][0..4], @intCast(block_data.metadata_json.len), .little);
            offset += 4;
            @memcpy(buffer[offset .. offset + block_data.metadata_json.len], block_data.metadata_json);
            offset += block_data.metadata_json.len;

            std.mem.writeInt(u32, buffer[offset .. offset + 4][0..4], @intCast(block_data.content.len), .little);
            offset += 4;
            @memcpy(buffer[offset .. offset + block_data.content.len], block_data.content);
            offset += block_data.content.len;
        }

        assert(offset == total_size);
        return buffer;
    }

    /// Get current server statistics
    pub fn statistics(self: *const Server) ServerStats {
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

    var mock_storage: StorageEngine = undefined;
    var mock_query: QueryEngine = undefined;

    const config = ServerConfig{ .port = 0 }; // Use ephemeral port for testing
    var server = Server.init(allocator, config, &mock_storage, &mock_query);
    defer server.deinit();

    try testing.expectEqual(@as(u32, 0), server.statistics().connections_active);
    try testing.expectEqual(@as(u64, 0), server.statistics().requests_processed);
}
