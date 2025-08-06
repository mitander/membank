# Server Architecture

## Overview

The server follows a coordinator pattern with clean separation between I/O management and request processing. The architecture eliminates the "god module" anti-pattern by delegating connection lifecycle management to a dedicated subsystem.

## Core Components

### Server (`src/server/handler.zig`)

The main server coordinator that handles:

- **Request routing**: Dispatches incoming requests to appropriate handlers
- **Response serialization**: Converts query results to binary protocol format
- **Statistics tracking**: Monitors server operations and performance
- **Configuration management**: Handles server-wide settings

### ConnectionManager (`src/server/connection_manager.zig`)

Dedicated connection lifecycle manager following coordinator pattern:

- **Connection pooling**: Manages active client connections with configurable limits
- **I/O polling**: Non-blocking event loop using poll() for scalable I/O
- **Timeout handling**: Automatic cleanup of idle connections
- **Resource management**: Arena-per-subsystem memory model for O(1) cleanup

### ClientConnection (`src/server/connection.zig`)

Individual connection state machine:

- **Protocol handling**: Request parsing and response buffering
- **State transitions**: Reading → Processing → Writing → Closing
- **Memory management**: Connection-scoped allocations with automatic cleanup

## Architecture Principles

### Coordinator Pattern

The Server acts as a pure coordinator - it contains no I/O logic or connection state:

```zig
// Server delegates I/O concerns to ConnectionManager
const ready_connections = try self.connection_manager.poll_for_ready_connections(listener);

// Server focuses on request processing
for (ready_connections) |connection| {
    _ = try self.connection_manager.process_connection_io(connection, self.config);
}
```

### Two-Phase Initialization

All components follow the init() → startup() pattern:

- **`init()`**: Memory-only setup, no I/O operations
- **`startup()`**: I/O resource allocation and system preparation

```zig
// Phase 1: Memory allocation only
var connection_manager = ConnectionManager.init(allocator, config);

// Phase 2: Allocate poll_fds, configure sockets
try connection_manager.startup();
```

### Arena-Per-Subsystem Memory Model

ConnectionManager uses arena allocation for automatic resource cleanup:

```zig
pub const ConnectionManager = struct {
    arena: std.heap.ArenaAllocator,           // For connection objects
    backing_allocator: std.mem.Allocator,    // For stable structures
    connections: std.ArrayList(*ClientConnection),
    poll_fds: []std.posix.pollfd,           // Allocated with backing allocator
};
```

Benefits:
- **O(1) cleanup**: All connection memory freed in single arena reset
- **Leak prevention**: Complex connection graphs can't leak memory
- **Performance**: Bulk deallocation eliminates individual free() calls

### Non-Blocking I/O Model

The server uses poll()-based event detection for scalable I/O:

1. **Build poll_fds array**: Based on connection states (reading/writing)
2. **Poll for events**: Non-blocking check for ready file descriptors
3. **Process ready connections**: Handle I/O for connections with events
4. **Timeout maintenance**: Clean up idle connections during poll timeouts

## Connection Lifecycle

### State Machine

Each connection progresses through defined states:

```
┌─────────────┐    ┌──────────────┐    ┌─────────────┐    ┌─────────────┐
│ ReadingHdr  │───→│ ReadingPayld │───→│ Processing  │───→│ WritingResp │
└─────────────┘    └──────────────┘    └─────────────┘    └─────────────┘
       ↑                                                         │
       │                                                         ↓
┌─────────────┐                                           ┌─────────────┐
│   Closing   │←──────────────────────────────────────────│   Closed    │
└─────────────┘                                           └─────────────┘
```

### Resource Management

Connection resources are automatically managed:

- **Creation**: Connections allocated from ConnectionManager arena
- **I/O buffers**: Request/response buffers managed per-connection
- **Cleanup**: Arena reset handles all connection memory in O(1) operation

### Error Handling

Connection errors are handled gracefully:

- **I/O errors**: Connection marked for cleanup, resources deallocated
- **Protocol errors**: Error response sent, connection closed
- **Timeout errors**: Idle connections automatically cleaned up

## Configuration

### ConnectionManagerConfig

Controls connection behavior:

```zig
pub const ConnectionManagerConfig = struct {
    max_connections: u32 = 100,        // Concurrent connection limit
    connection_timeout_sec: u32 = 300, // Idle timeout
    poll_timeout_ms: i32 = 1000,       // Poll blocking timeout
};
```

### ServerConfig

Server-wide settings:
- **Port binding**: TCP port configuration
- **Request limits**: Maximum request size, timeout values
- **Logging**: Debug output and performance tracking
- **Protocol**: Binary response format options

## Performance Characteristics

### Memory Usage

- **Bounded**: Arena allocation prevents unbounded growth
- **Predictable**: O(1) cleanup operations for connection management
- **Efficient**: Bulk allocation/deallocation reduces fragmentation

### Scalability

- **Non-blocking I/O**: Scales to hundreds of concurrent connections
- **Connection limits**: Prevents resource exhaustion attacks
- **Event-driven**: CPU usage scales with active I/O, not connection count

### Reliability

- **Resource cleanup**: Arena pattern prevents connection memory leaks
- **Timeout handling**: Automatic cleanup prevents resource accumulation
- **Error isolation**: Connection failures don't affect other connections
