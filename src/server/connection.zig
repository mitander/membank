//! Client Connection State Machine for Async I/O
//!
//! Manages individual client connections as non-blocking state machines.
//! Handles the complete request/response lifecycle:
//! - Header parsing and validation
//! - Payload reading with partial I/O support
//! - Request processing coordination
//! - Response writing with flow control
//!
//! Follows CortexDB architectural principles:
//! - Arena-per-connection memory management
//! - Explicit error handling with context
//! - Non-blocking I/O with proper state transitions

const std = @import("std");
const assert = std.debug.assert;
const log = std.log.scoped(.connection);
const context_block = @import("context_block");
const query_engine = @import("query_engine");
const custom_assert = @import("assert");
const comptime_assert = custom_assert.comptime_assert;
const comptime_no_padding = custom_assert.comptime_no_padding;

const ContextBlock = context_block.ContextBlock;
const BlockId = context_block.BlockId;

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
        return std.meta.intToEnum(MessageType, value) catch return error.InvalidRequest;
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

    pub const HEADER_SIZE = 8;

    pub fn encode(self: MessageHeader, buffer: []u8) void {
        assert(buffer.len >= HEADER_SIZE);
        buffer[0] = @intFromEnum(self.msg_type);
        buffer[1] = self.version;
        std.mem.writeInt(u16, buffer[2..4], self.reserved, .little);
        std.mem.writeInt(u32, buffer[4..8], self.payload_length, .little);
    }

    pub fn decode(buffer: []const u8) !MessageHeader {
        if (buffer.len < HEADER_SIZE) return error.InvalidRequest;

        return MessageHeader{
            .msg_type = try MessageType.from_u8(buffer[0]),
            .version = buffer[1],
            .reserved = std.mem.readInt(u16, buffer[2..4], .little),
            .payload_length = std.mem.readInt(u32, buffer[4..8], .little),
        };
    }
};

/// Connection state for async I/O state machine
pub const ConnectionState = enum {
    /// Waiting to read message header
    reading_header,
    /// Waiting to read message payload
    reading_payload,
    /// Processing request (synchronous operation)
    processing,
    /// Waiting to write response
    writing_response,
    /// Connection should be closed
    closing,
    /// Connection is closed
    closed,
};

/// Server configuration needed by connections
pub const ServerConfig = struct {
    /// Maximum request size in bytes
    max_request_size: u32 = 64 * 1024, // 64KB
    /// Maximum response size in bytes
    max_response_size: u32 = 16 * 1024 * 1024, // 16MB
    /// Enable request/response logging
    enable_logging: bool = false,
};

/// Connection errors
pub const ConnectionError = error{
    /// Invalid request format
    InvalidRequest,
    /// Request too large
    RequestTooLarge,
    /// Response too large
    ResponseTooLarge,
    /// Client disconnected unexpectedly
    ClientDisconnected,
} || std.mem.Allocator.Error || std.net.Stream.ReadError || std.net.Stream.WriteError;

/// Client connection state machine for async I/O
pub const ClientConnection = struct {
    /// TCP stream for this connection
    stream: std.net.Stream,
    /// Arena allocator for this connection's memory
    arena: std.heap.ArenaAllocator,
    /// Connection ID for logging
    connection_id: u32,
    /// When this connection was established
    established_time: i64,
    /// Current state in the I/O state machine
    state: ConnectionState,
    /// Buffer for reading requests
    read_buffer: [4096]u8,
    /// Buffer for writing responses
    write_buffer: [4096]u8,
    /// Current position in header read
    header_bytes_read: usize,
    /// Current message header being read
    current_header: ?MessageHeader,
    /// Current payload buffer being read
    current_payload: ?[]u8,
    /// Current position in payload read
    payload_bytes_read: usize,
    /// Current response buffer being written
    current_response: ?[]const u8,
    /// Current position in response write
    response_bytes_written: usize,

    pub fn init(allocator: std.mem.Allocator, stream: std.net.Stream, connection_id: u32) ClientConnection {
        return ClientConnection{
            .stream = stream,
            .arena = std.heap.ArenaAllocator.init(allocator),
            .connection_id = connection_id,
            .established_time = std.time.timestamp(),
            .state = .reading_header,
            .read_buffer = std.mem.zeroes([4096]u8),
            .write_buffer = std.mem.zeroes([4096]u8),
            .header_bytes_read = 0,
            .current_header = null,
            .current_payload = null,
            .payload_bytes_read = 0,
            .current_response = null,
            .response_bytes_written = 0,
        };
    }

    pub fn deinit(self: *ClientConnection) void {
        self.stream.close();
        self.arena.deinit();
    }

    /// Process non-blocking I/O for this connection
    /// Returns true if the connection should remain active, false if it should be closed
    pub fn process_io(self: *ClientConnection, config: ServerConfig) !bool {
        switch (self.state) {
            .reading_header => return self.try_read_header(config),
            .reading_payload => return self.try_read_payload(config),
            .processing => {
                // This should not happen in async model - processing is synchronous
                return false;
            },
            .writing_response => return self.try_write_response(),
            .closing, .closed => return false,
        }
    }

    /// Attempt to read message header non-blockingly
    fn try_read_header(self: *ClientConnection, config: ServerConfig) !bool {
        // Try to read remaining header bytes
        const header_remaining = MessageHeader.HEADER_SIZE - self.header_bytes_read;
        const n = self.stream.read(self.read_buffer[self.header_bytes_read .. self.header_bytes_read + header_remaining]) catch |err| switch (err) {
            error.WouldBlock => return true, // No data available, try again later
            error.ConnectionResetByPeer, error.BrokenPipe => return false, // Client disconnected
            else => return err,
        };

        if (n == 0) return false; // Client disconnected

        self.header_bytes_read += n;

        // Check if we have the complete header
        if (self.header_bytes_read >= MessageHeader.HEADER_SIZE) {
            self.current_header = MessageHeader.decode(self.read_buffer[0..MessageHeader.HEADER_SIZE]) catch {
                log.warn("Connection {d}: Invalid message header", .{self.connection_id});
                return false;
            };

            // Transition to reading payload or processing
            if (self.current_header.?.payload_length == 0) {
                self.state = .processing;
            } else {
                if (self.current_header.?.payload_length > config.max_request_size) {
                    log.warn("Connection {d}: Request too large: {d} > {d}", .{ self.connection_id, self.current_header.?.payload_length, config.max_request_size });
                    return false;
                }

                // Allocate payload buffer
                self.current_payload = self.arena.allocator().alloc(u8, self.current_header.?.payload_length) catch {
                    log.warn("Connection {d}: Failed to allocate payload buffer", .{self.connection_id});
                    return false;
                };
                self.payload_bytes_read = 0;
                self.state = .reading_payload;
            }
        }

        return true;
    }

    /// Attempt to read message payload non-blockingly
    fn try_read_payload(self: *ClientConnection, config: ServerConfig) !bool {
        _ = config;

        const payload = self.current_payload orelse return false;

        const n = self.stream.read(payload[self.payload_bytes_read..]) catch |err| switch (err) {
            error.WouldBlock => return true, // No data available, try again later
            error.ConnectionResetByPeer, error.BrokenPipe => return false, // Client disconnected
            else => return err,
        };

        if (n == 0) return false; // Client disconnected

        self.payload_bytes_read += n;

        // Check if we have the complete payload
        if (self.payload_bytes_read >= payload.len) {
            self.state = .processing;
        }

        return true;
    }

    /// Attempt to write response non-blockingly
    fn try_write_response(self: *ClientConnection) !bool {
        const response = self.current_response orelse return false;

        const n = self.stream.write(response[self.response_bytes_written..]) catch |err| switch (err) {
            error.WouldBlock => return true, // Socket buffer full, try again later
            error.ConnectionResetByPeer, error.BrokenPipe => return false, // Client disconnected
            else => return err,
        };

        self.response_bytes_written += n;

        // Check if we've written the complete response
        if (self.response_bytes_written >= response.len) {
            // Reset for next request
            self.header_bytes_read = 0;
            self.current_header = null;
            self.current_payload = null;
            self.payload_bytes_read = 0;
            self.current_response = null;
            self.response_bytes_written = 0;
            self.state = .reading_header;

            // Reset arena to free memory from this request
            _ = self.arena.reset(.retain_capacity);
        }

        return true;
    }

    /// Check if connection is ready for processing a complete request
    pub fn has_complete_request(self: *const ClientConnection) bool {
        return self.state == .processing;
    }

    /// Access the complete request payload for processing
    pub fn request_payload(self: *const ClientConnection) ?[]const u8 {
        if (self.state != .processing) return null;
        return self.current_payload orelse &[_]u8{}; // Empty payload if no payload expected
    }

    /// Begin response transmission and transition to writing state
    pub fn send_response(self: *ClientConnection, response_data: []const u8) void {
        assert(self.state == .processing);
        self.current_response = response_data;
        self.response_bytes_written = 0;
        self.state = .writing_response;
    }

    /// Write a message to the client (legacy blocking method - kept for compatibility)
    pub fn write_message(self: *ClientConnection, msg_type: MessageType, payload: []const u8, config: ServerConfig) !void {
        if (payload.len > config.max_response_size) {
            return ConnectionError.ResponseTooLarge;
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
