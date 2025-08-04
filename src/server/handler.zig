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
const error_context = @import("../core/error_context.zig");

const conn = @import("connection.zig");
pub const ClientConnection = conn.ClientConnection;
pub const MessageType = conn.MessageType;
pub const MessageHeader = conn.MessageHeader;
pub const ConnectionState = conn.ConnectionState;

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

/// Main TCP server
pub const Server = struct {
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
    ) Server {
        return Server{
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
    pub fn deinit(self: *Server) void {
        self.stop();

        for (self.connections.items) |connection| {
            connection.deinit();
            self.allocator.destroy(connection);
        }
        self.connections.deinit();
    }

    /// Phase 2 initialization: Start the server and listen for connections.
    /// Performs I/O operations including socket binding and network setup.
    pub fn startup(self: *Server) !void {
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

    /// Run the blocking event loop
    pub fn run(self: *Server) !void {
        concurrency.assert_main_thread();
        try self.run_event_loop();
    }

    /// Main async event loop - polls all sockets and processes I/O events
    fn run_event_loop(self: *Server) !void {
        var poll_fds = try self.allocator.alloc(std.posix.pollfd, self.config.max_connections + 1);
        defer self.allocator.free(poll_fds);

        while (true) {
            poll_fds[0] = std.posix.pollfd{
                .fd = self.listener.?.stream.handle,
                .events = std.posix.POLL.IN,
                .revents = 0,
            };

            var poll_count: usize = 1;

            for (self.connections.items) |connection| {
                if (poll_count >= poll_fds.len) break; // Safety check

                var events: i16 = 0;

                switch (connection.state) {
                    .reading_header, .reading_payload => events |= std.posix.POLL.IN,
                    .writing_response => events |= std.posix.POLL.OUT,
                    .processing => {},
                    .closing, .closed => continue,
                }

                if (events != 0) {
                    poll_fds[poll_count] = std.posix.pollfd{
                        .fd = connection.stream.handle,
                        .events = events,
                        .revents = 0,
                    };
                    poll_count += 1;
                }
            }

            const ready_count = std.posix.poll(poll_fds[0..poll_count], 1000) catch |err| switch (err) {
                error.Unexpected => continue, // Interrupted by signal, retry
                else => return err,
            };

            if (ready_count == 0) {
                try self.cleanup_timed_out_connections();
                continue;
            }

            try self.process_poll_events(poll_fds[0..poll_count]);
        }
    }

    /// Process events from poll() results
    fn process_poll_events(self: *Server, poll_fds: []std.posix.pollfd) !void {
        if (poll_fds[0].revents & std.posix.POLL.IN != 0) {
            try self.accept_new_connections();
        }

        var i: usize = 1;
        var conn_index: usize = 0;

        while (i < poll_fds.len and conn_index < self.connections.items.len) {
            const poll_fd = poll_fds[i];
            const connection = self.connections.items[conn_index];

            if (poll_fd.fd != connection.stream.handle) {
                conn_index += 1;
                continue;
            }

            if (poll_fd.revents & (std.posix.POLL.ERR | std.posix.POLL.HUP | std.posix.POLL.NVAL) != 0) {
                log.info("Connection {d}: socket error, closing", .{connection.connection_id});
                self.close_connection(conn_index);
                i += 1;
                continue;
            }

            const keep_alive = connection.process_io(self.config.to_connection_config()) catch |err| blk: {
                const ctx = error_context.connection_context("process_io", connection.connection_id);
                error_context.log_server_error(err, ctx);
                log.err("Connection {d}: I/O error: {any}", .{ connection.connection_id, err });
                break :blk false;
            };

            if (!keep_alive) {
                self.close_connection(conn_index);
            } else {
                if (connection.has_complete_request()) {
                    try self.process_connection_request(connection);
                }
                conn_index += 1;
            }

            i += 1;
        }
    }

    /// Accept new connections non-blockingly
    fn accept_new_connections(self: *Server) !void {
        while (true) {
            const tcp_connection = self.listener.?.accept() catch |err| switch (err) {
                error.WouldBlock => break, // No more connections to accept
                error.ConnectionAborted => continue, // Client canceled, try next
                else => {
                    const ctx = error_context.ServerContext{ .operation = "accept_connection" };
                    error_context.log_server_error(err, ctx);
                    return err;
                },
            };

            if (self.connections.items.len >= self.config.max_connections) {
                tcp_connection.stream.close();
                self.stats.errors_encountered += 1;
                log.warn("Connection rejected: max connections ({d}) exceeded", .{self.config.max_connections});
                continue;
            }

            const connection = try self.allocator.create(ClientConnection);
            connection.* = ClientConnection.init(self.allocator, tcp_connection.stream, self.next_connection_id);
            const conn_flags = try std.posix.fcntl(connection.stream.handle, std.posix.F.GETFL, 0);
            const nonblock_flag = 1 << @bitOffsetOf(std.posix.O, "NONBLOCK");
            _ = try std.posix.fcntl(connection.stream.handle, std.posix.F.SETFL, conn_flags | nonblock_flag);
            self.next_connection_id += 1;

            try self.connections.append(connection);
            self.stats.connections_accepted += 1;
            self.stats.connections_active += 1;

            log.info("New connection {d} from {any}", .{ connection.connection_id, tcp_connection.address });
        }
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

    /// Close a connection and remove it from the connections list
    fn close_connection(self: *Server, index: usize) void {
        assert(index < self.connections.items.len);

        const connection = self.connections.items[index];
        log.info("Connection {d} closed", .{connection.connection_id});

        connection.deinit();
        self.allocator.destroy(connection);
        _ = self.connections.swapRemove(index);
        self.stats.connections_active -= 1;
    }

    /// Clean up connections that have timed out
    fn cleanup_timed_out_connections(self: *Server) !void {
        const current_time = std.time.timestamp();
        const timeout_seconds: i64 = @intCast(self.config.connection_timeout_sec);

        var i: usize = 0;
        while (i < self.connections.items.len) {
            const connection = self.connections.items[i];
            const connection_age = current_time - connection.established_time;

            if (connection_age > timeout_seconds) {
                log.info("Connection {d}: timeout after {d}s", .{ connection.connection_id, connection_age });
                self.close_connection(i);
            } else {
                i += 1;
            }
        }
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

        var block_ids = try allocator.alloc(ctx_block.BlockId, block_count);
        for (0..block_count) |i| {
            const id_start = 4 + (i * 16);
            const id_bytes = payload[id_start .. id_start + 16];
            block_ids[i] = ctx_block.BlockId{ .bytes = id_bytes[0..16].* };
        }

        const query = query_engine.FindBlocksQuery{ .block_ids = block_ids };
        const result = self.query_engine.execute_find_blocks(query) catch |err| {
            const ctx = error_context.ServerContext{
                .operation = "execute_find_blocks",
                .connection_id = connection.connection_id,
                .message_size = block_count,
            };
            error_context.log_server_error(err, ctx);
            const error_msg = try std.fmt.allocPrint(allocator, "Query execution failed: {any}", .{err});
            connection.send_response(error_msg);
            self.stats.errors_encountered += 1;
            return;
        };
        defer result.deinit();

        var mutable_result = result;
        const response_data = try self.serialize_blocks_response(allocator, &mutable_result);
        connection.send_response(response_data);
        self.stats.bytes_sent += response_data.len + MessageHeader.HEADER_SIZE;

        if (self.config.enable_logging) {
            log.debug("Connection {d}: handled find_blocks request, returned {d} blocks", .{ connection.connection_id, result.total_found });
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

        var result_blocks = std.ArrayList(ContextBlock).init(allocator);
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
            block_ids[i] = block.id;
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
    fn serialize_blocks_response(self: *Server, allocator: std.mem.Allocator, result_ptr: *QueryResult) ![]u8 {
        _ = self; // Suppress unused parameter warning

        var total_size: usize = 4; // 4 bytes for block count
        var block_count: u32 = 0;

        while (try result_ptr.next()) |block| {
            defer result_ptr.deinit_block(block);
            block_count += 1;
            total_size += 16;
            total_size += 4 + block.source_uri.len;
            total_size += 4 + block.metadata_json.len;
            total_size += 4 + block.content.len;
        }

        const buffer = try allocator.alloc(u8, total_size);
        var offset: usize = 0;

        std.mem.writeInt(u32, buffer[offset .. offset + 4][0..4], block_count, .little);
        offset += 4;

        result_ptr.reset();

        while (try result_ptr.next()) |block| {
            defer result_ptr.deinit_block(block);

            @memcpy(buffer[offset .. offset + 16], &block.id.bytes);
            offset += 16;

            std.mem.writeInt(u32, buffer[offset .. offset + 4][0..4], @intCast(block.source_uri.len), .little);
            offset += 4;
            @memcpy(buffer[offset .. offset + block.source_uri.len], block.source_uri);
            offset += block.source_uri.len;

            std.mem.writeInt(u32, buffer[offset .. offset + 4][0..4], @intCast(block.metadata_json.len), .little);
            offset += 4;
            @memcpy(buffer[offset .. offset + block.metadata_json.len], block.metadata_json);
            offset += block.metadata_json.len;

            std.mem.writeInt(u32, buffer[offset .. offset + 4][0..4], @intCast(block.content.len), .little);
            offset += 4;
            @memcpy(buffer[offset .. offset + block.content.len], block.content);
            offset += block.content.len;
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
