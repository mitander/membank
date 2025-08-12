//! Type-safe file handle system for VFS operations.
//!
//! Design rationale: Replaces raw file handles with type-safe abstractions that
//! track file state, ownership, and operation validity through the type system.
//! Prevents invalid file operations by encoding file lifecycle in types.
//!
//! The handle system integrates with enum state machines to validate operations
//! and provides zero-cost abstractions in release builds while maintaining
//! comprehensive validation in debug builds.

const std = @import("std");
const builtin = @import("builtin");
const custom_assert = @import("assert.zig");
const fatal_assert = custom_assert.fatal_assert;
const assert_fmt = custom_assert.assert_fmt;
const state_machines = @import("state_machines.zig");
const FileState = state_machines.FileState;

/// Unique identifier for file handles with type safety.
/// Prevents mixing handles from different VFS implementations.
pub const FileHandleId = struct {
    id: u32,
    generation: u32,

    /// Create new file handle ID with generation counter.
    pub fn init(id: u32, generation: u32) FileHandleId {
        fatal_assert(id != std.math.maxInt(u32), "Invalid file handle ID: {}", .{id});
        return FileHandleId{ .id = id, .generation = generation };
    }

    /// Check if handle IDs are equal.
    pub fn eql(self: FileHandleId, other: FileHandleId) bool {
        return self.id == other.id and self.generation == other.generation;
    }

    /// Get unique hash for handle.
    pub fn hash(self: FileHandleId) u64 {
        return (@as(u64, self.generation) << 32) | self.id;
    }

    /// Check if handle is valid (not default/null).
    pub fn is_valid(self: FileHandleId) bool {
        return self.id != 0 or self.generation != 0;
    }

    /// Create invalid handle for initialization.
    pub fn invalid() FileHandleId {
        return FileHandleId{ .id = 0, .generation = 0 };
    }
};

/// File access mode for type-safe operation validation.
pub const FileAccessMode = enum {
    read_only,
    write_only,
    read_write,

    /// Check if mode allows read operations.
    pub fn can_read(self: FileAccessMode) bool {
        return switch (self) {
            .read_only, .read_write => true,
            .write_only => false,
        };
    }

    /// Check if mode allows write operations.
    pub fn can_write(self: FileAccessMode) bool {
        return switch (self) {
            .write_only, .read_write => true,
            .read_only => false,
        };
    }

    /// Convert to FileState for state machine integration.
    pub fn to_file_state(self: FileAccessMode) FileState {
        return switch (self) {
            .read_only => .open_read,
            .write_only => .open_write,
            .read_write => .open_read_write,
        };
    }
};

/// Type-safe file handle with state tracking and operation validation.
/// Replaces raw file descriptors with type-safe abstractions.
pub const TypedFileHandle = struct {
    id: FileHandleId,
    path: []const u8,
    access_mode: FileAccessMode,
    state: FileState,
    file_size: u64,
    position: u64,
    debug_open_location: if (builtin.mode == .Debug) ?std.builtin.SourceLocation else void,

    /// Create new typed file handle.
    pub fn init(handle_id: FileHandleId, path: []const u8, access_mode: FileAccessMode) TypedFileHandle {
        fatal_assert(handle_id.is_valid(), "Invalid file handle ID", .{});
        fatal_assert(path.len > 0, "Empty file path", .{});
        fatal_assert(path.len <= 4096, "File path too long: {}", .{path.len});

        return TypedFileHandle{
            .id = handle_id,
            .path = path,
            .access_mode = access_mode,
            .state = access_mode.to_file_state(),
            .file_size = 0,
            .position = 0,
            .debug_open_location = if (builtin.mode == .Debug) @src() else {},
        };
    }

    /// Read data from file with state and access validation.
    pub fn read(self: *TypedFileHandle, buffer: []u8) !usize {
        self.state.assert_can_read();
        fatal_assert(self.access_mode.can_read(), "File opened for write-only access", .{});

        if (builtin.mode == .Debug) {
            assert_fmt(self.position <= self.file_size, "File position beyond EOF: {} > {}", .{ self.position, self.file_size });
        }

        const available = if (self.position >= self.file_size) 0 else self.file_size - self.position;
        const to_read = @min(buffer.len, available);

        if (builtin.mode == .Debug) {
            std.log.debug("Reading {} bytes from {s} at position {} (available: {})", .{ to_read, self.path, self.position, available });
        }

        self.position += to_read;
        return to_read;
    }

    /// Write data to file with state and access validation.
    pub fn write(self: *TypedFileHandle, data: []const u8) !void {
        self.state.assert_can_write();
        fatal_assert(self.access_mode.can_write(), "File opened for read-only access", .{});
        fatal_assert(data.len > 0, "Cannot write empty data", .{});

        if (builtin.mode == .Debug) {
            std.log.debug("Writing {} bytes to {s} at position {}", .{ data.len, self.path, self.position });
        }

        self.position += data.len;
        if (self.position > self.file_size) {
            self.file_size = self.position;
        }
    }

    /// Seek to position in file with bounds validation.
    pub fn seek(self: *TypedFileHandle, offset: u64) !void {
        fatal_assert(self.state.is_open(), "Cannot seek on closed file", .{});

        if (builtin.mode == .Debug) {
            assert_fmt(offset <= self.file_size, "Seek beyond EOF: {} > {}", .{ offset, self.file_size });
        }

        self.position = offset;

        if (builtin.mode == .Debug) {
            std.log.debug("Seeked to position {} in {s}", .{ offset, self.path });
        }
    }

    /// Sync file data to storage with state validation.
    pub fn sync(self: *TypedFileHandle) !void {
        fatal_assert(self.state.is_open(), "Cannot sync closed file", .{});
        fatal_assert(self.access_mode.can_write(), "Cannot sync read-only file", .{});

        if (builtin.mode == .Debug) {
            std.log.debug("Syncing file {s} (size: {})", .{ self.path, self.file_size });
        }
    }

    /// Close file and transition to closed state.
    pub fn close(self: *TypedFileHandle) void {
        if (builtin.mode == .Debug) {
            std.log.debug("Closing file {s} (final size: {})", .{ self.path, self.file_size });
        }

        self.state.transition(.closed);
        self.position = 0;
    }

    /// Query current file position.
    pub fn query_position(self: *const TypedFileHandle) u64 {
        return self.position;
    }

    /// Query file size.
    pub fn query_size(self: *const TypedFileHandle) u64 {
        return self.file_size;
    }

    /// Query file path.
    pub fn query_path(self: *const TypedFileHandle) []const u8 {
        return self.path;
    }

    /// Check if file is open for reading.
    pub fn can_read_now(self: *const TypedFileHandle) bool {
        return self.state.can_read() and self.access_mode.can_read();
    }

    /// Check if file is open for writing.
    pub fn can_write_now(self: *const TypedFileHandle) bool {
        return self.state.can_write() and self.access_mode.can_write();
    }

    /// Check if handle is valid and file is open.
    pub fn is_open(self: *const TypedFileHandle) bool {
        return self.id.is_valid() and self.state.is_open();
    }

    /// Force update file size (for VFS implementations).
    pub fn update_size(self: *TypedFileHandle, new_size: u64) void {
        if (builtin.mode == .Debug) {
            std.log.debug("Updating file size: {} -> {} for {s}", .{ self.file_size, new_size, self.path });
        }
        self.file_size = new_size;
    }

    /// Validate file handle consistency in debug builds.
    pub fn validate_consistency(self: *const TypedFileHandle) void {
        if (builtin.mode == .Debug) {
            assert_fmt(self.id.is_valid(), "Invalid file handle for {s}", .{self.path});
            assert_fmt(self.position <= self.file_size, "Position beyond EOF: {} > {} for {s}", .{ self.position, self.file_size, self.path });
            assert_fmt(self.path.len > 0, "Empty file path", .{});

            // Validate state machine consistency
            switch (self.state) {
                .open_read => assert_fmt(self.access_mode == .read_only, "State/mode mismatch: read state with {} mode", .{self.access_mode}),
                .open_write => assert_fmt(self.access_mode == .write_only, "State/mode mismatch: write state with {} mode", .{self.access_mode}),
                .open_read_write => assert_fmt(self.access_mode == .read_write, "State/mode mismatch: read_write state with {} mode", .{self.access_mode}),
                .closed, .deleted => {}, // No mode restrictions when closed
            }
        }
    }
};

/// File handle registry for managing multiple open files.
/// Provides type-safe lookup and lifecycle management.
pub const FileHandleRegistry = struct {
    handles: std.HashMap(FileHandleId, TypedFileHandle, FileHandleContext, std.hash_map.default_max_load_percentage),
    next_id: u32,
    generation: u32,
    allocator: std.mem.Allocator,

    const FileHandleContext = struct {
        pub fn hash(self: @This(), key: FileHandleId) u64 {
            _ = self;
            return key.hash();
        }

        pub fn eql(self: @This(), a: FileHandleId, b: FileHandleId) bool {
            _ = self;
            return a.eql(b);
        }
    };

    /// Initialize file handle registry.
    pub fn init(allocator: std.mem.Allocator) FileHandleRegistry {
        return FileHandleRegistry{
            .handles = std.HashMap(FileHandleId, TypedFileHandle, FileHandleContext, std.hash_map.default_max_load_percentage).init(allocator),
            .next_id = 1, // Start at 1, reserve 0 for invalid
            .generation = 1,
            .allocator = allocator,
        };
    }

    /// Register new file handle and return handle ID.
    pub fn register_handle(self: *FileHandleRegistry, path: []const u8, access_mode: FileAccessMode) !FileHandleId {
        const handle_id = FileHandleId.init(self.next_id, self.generation);

        // Clone path for storage
        const owned_path = try self.allocator.dupe(u8, path);
        const handle = TypedFileHandle.init(handle_id, owned_path, access_mode);

        try self.handles.put(handle_id, handle);

        self.next_id += 1;
        if (self.next_id == 0) {
            // Wrap around, increment generation
            self.next_id = 1;
            self.generation += 1;
        }

        if (builtin.mode == .Debug) {
            std.log.debug("Registered file handle {} for {s} (mode: {any})", .{ handle_id.id, path, access_mode });
        }

        return handle_id;
    }

    /// Query mutable file handle by ID.
    pub fn query_handle(self: *FileHandleRegistry, handle_id: FileHandleId) ?*TypedFileHandle {
        return self.handles.getPtr(handle_id);
    }

    /// Query immutable file handle by ID.
    pub fn query_handle_const(self: *const FileHandleRegistry, handle_id: FileHandleId) ?*const TypedFileHandle {
        return self.handles.getPtr(handle_id);
    }

    /// Remove and close file handle.
    pub fn close_handle(self: *FileHandleRegistry, handle_id: FileHandleId) bool {
        if (self.handles.getPtr(handle_id)) |handle| {
            handle.close();

            self.allocator.free(handle.path);

            _ = self.handles.remove(handle_id);

            if (builtin.mode == .Debug) {
                std.log.debug("Closed and removed file handle {}", .{handle_id.id});
            }

            return true;
        }
        return false;
    }

    /// Close all handles and clear registry.
    pub fn close_all(self: *FileHandleRegistry) void {
        var iter = self.handles.iterator();
        while (iter.next()) |entry| {
            entry.value_ptr.close();
            self.allocator.free(entry.value_ptr.path);
        }
        self.handles.clearAndFree();

        if (builtin.mode == .Debug) {
            std.log.debug("Closed all file handles", .{});
        }
    }

    /// Get count of open handles.
    pub fn handle_count(self: *const FileHandleRegistry) u32 {
        return @intCast(self.handles.count());
    }

    /// Check if registry has handles open.
    pub fn has_open_handles(self: *const FileHandleRegistry) bool {
        return self.handles.count() > 0;
    }

    /// Validate all handles for consistency in debug builds.
    pub fn validate_all_handles(self: *const FileHandleRegistry) void {
        if (builtin.mode == .Debug) {
            var iter = self.handles.iterator();
            while (iter.next()) |entry| {
                entry.value_ptr.validate_consistency();
            }
        }
    }

    /// Deinitialize registry and close all handles.
    pub fn deinit(self: *FileHandleRegistry) void {
        self.close_all();
        self.handles.deinit();
    }
};

/// File operation result with type-safe error handling.
/// Provides detailed context for file operation failures.
pub const FileOperationResult = union(enum) {
    success: struct { bytes_transferred: usize },
    error_invalid_handle: void,
    error_invalid_state: FileState,
    error_access_denied: FileAccessMode,
    error_io_failure: []const u8,
    error_out_of_bounds: struct { requested: u64, available: u64 },

    /// Check if operation succeeded.
    pub fn is_success(self: FileOperationResult) bool {
        return switch (self) {
            .success => true,
            else => false,
        };
    }

    /// Query bytes transferred if operation succeeded.
    pub fn query_bytes(self: FileOperationResult) ?usize {
        return switch (self) {
            .success => |s| s.bytes_transferred,
            else => null,
        };
    }

    /// Convert to standard error type for compatibility.
    pub fn to_error(self: FileOperationResult) !usize {
        return switch (self) {
            .success => |s| s.bytes_transferred,
            .error_invalid_handle => error.InvalidHandle,
            .error_invalid_state => error.InvalidState,
            .error_access_denied => error.AccessDenied,
            .error_io_failure => error.IoError,
            .error_out_of_bounds => error.OutOfBounds,
        };
    }
};

/// Type-safe file operations wrapper.
/// Provides validated operations over TypedFileHandle with error context.
pub const FileOperations = struct {
    registry: *FileHandleRegistry,

    /// Initialize file operations with handle registry.
    pub fn init(registry: *FileHandleRegistry) FileOperations {
        return FileOperations{ .registry = registry };
    }

    /// Open file with type-safe access mode.
    pub fn open_file(self: *FileOperations, path: []const u8, access_mode: FileAccessMode) !FileHandleId {
        fatal_assert(path.len > 0, "Cannot open file with empty path", .{});

        const handle_id = try self.registry.register_handle(path, access_mode);

        if (builtin.mode == .Debug) {
            std.log.debug("Opened file {s} with mode {any} (handle: {})", .{ path, access_mode, handle_id.id });
        }

        return handle_id;
    }

    /// Read from file with comprehensive validation.
    pub fn read_from_file(self: *FileOperations, handle_id: FileHandleId, buffer: []u8) FileOperationResult {
        const handle = self.registry.query_handle(handle_id) orelse {
            return FileOperationResult{ .error_invalid_handle = {} };
        };

        if (!handle.can_read_now()) {
            if (!handle.state.can_read()) {
                return FileOperationResult{ .error_invalid_state = handle.state };
            } else {
                return FileOperationResult{ .error_access_denied = handle.access_mode };
            }
        }

        const bytes_read = handle.read(buffer) catch |err| switch (err) {
            error.OutOfBounds => return FileOperationResult{ .error_out_of_bounds = .{ .requested = buffer.len, .available = handle.file_size - handle.position } },
            else => return FileOperationResult{ .error_io_failure = "Read operation failed" },
        };

        return FileOperationResult{ .success = .{ .bytes_transferred = bytes_read } };
    }

    /// Write to file with comprehensive validation.
    pub fn write_to_file(self: *FileOperations, handle_id: FileHandleId, data: []const u8) FileOperationResult {
        const handle = self.registry.query_handle(handle_id) orelse {
            return FileOperationResult{ .error_invalid_handle = {} };
        };

        if (!handle.can_write_now()) {
            if (!handle.state.can_write()) {
                return FileOperationResult{ .error_invalid_state = handle.state };
            } else {
                return FileOperationResult{ .error_access_denied = handle.access_mode };
            }
        }

        handle.write(data) catch |err| switch (err) {
            else => return FileOperationResult{ .error_io_failure = "Write operation failed" },
        };

        return FileOperationResult{ .success = .{ .bytes_transferred = data.len } };
    }

    /// Seek in file with validation.
    pub fn seek_in_file(self: *FileOperations, handle_id: FileHandleId, offset: u64) FileOperationResult {
        const handle = self.registry.query_handle(handle_id) orelse {
            return FileOperationResult{ .error_invalid_handle = {} };
        };

        handle.seek(offset) catch |err| switch (err) {
            error.OutOfBounds => return FileOperationResult{ .error_out_of_bounds = .{ .requested = offset, .available = handle.file_size } },
            else => return FileOperationResult{ .error_io_failure = "Seek operation failed" },
        };

        return FileOperationResult{ .success = .{ .bytes_transferred = 0 } };
    }

    /// Close file and remove from registry.
    pub fn close_file(self: *FileOperations, handle_id: FileHandleId) bool {
        return self.registry.close_handle(handle_id);
    }

    /// Query file information without modifying state.
    pub fn query_file_info(self: *const FileOperations, handle_id: FileHandleId) ?FileInfo {
        const handle = self.registry.query_handle_const(handle_id) orelse return null;

        return FileInfo{
            .path = handle.path,
            .size = handle.file_size,
            .position = handle.position,
            .access_mode = handle.access_mode,
            .state = handle.state,
        };
    }

    /// Validate all open files for consistency.
    pub fn validate_all_files(self: *const FileOperations) void {
        self.registry.validate_all_handles();
    }
};

/// File information structure for queries.
pub const FileInfo = struct {
    path: []const u8,
    size: u64,
    position: u64,
    access_mode: FileAccessMode,
    state: FileState,

    /// Check if file is readable.
    pub fn is_readable(self: FileInfo) bool {
        return self.state.can_read() and self.access_mode.can_read();
    }

    /// Check if file is writable.
    pub fn is_writable(self: FileInfo) bool {
        return self.state.can_write() and self.access_mode.can_write();
    }

    /// Get remaining bytes to read.
    pub fn remaining_bytes(self: FileInfo) u64 {
        if (self.position >= self.size) return 0;
        return self.size - self.position;
    }
};

// Compile-time validation
comptime {
    custom_assert.comptime_assert(@sizeOf(FileHandleId) <= 16, "FileHandleId should be compact");
    custom_assert.comptime_assert(@sizeOf(TypedFileHandle) <= 128, "TypedFileHandle should be reasonably sized");

    // Validate that file access modes cover all cases
    const mode_count = @typeInfo(FileAccessMode).@"enum".fields.len;
    custom_assert.comptime_assert(mode_count == 3, "FileAccessMode should have exactly 3 variants");
}

// Tests

test "FileHandleId basic operations" {
    const handle1 = FileHandleId.init(1, 1);
    const handle2 = FileHandleId.init(1, 1);
    const handle3 = FileHandleId.init(2, 1);

    try std.testing.expect(handle1.eql(handle2));
    try std.testing.expect(!handle1.eql(handle3));
    try std.testing.expect(handle1.is_valid());

    const invalid = FileHandleId.invalid();
    try std.testing.expect(!invalid.is_valid());

    try std.testing.expect(handle1.hash() != handle3.hash());
}

test "FileAccessMode validation" {
    const read_only = FileAccessMode.read_only;
    const write_only = FileAccessMode.write_only;
    const read_write = FileAccessMode.read_write;

    try std.testing.expect(read_only.can_read());
    try std.testing.expect(!read_only.can_write());

    try std.testing.expect(!write_only.can_read());
    try std.testing.expect(write_only.can_write());

    try std.testing.expect(read_write.can_read());
    try std.testing.expect(read_write.can_write());

    // State conversion
    try std.testing.expect(read_only.to_file_state() == .open_read);
    try std.testing.expect(write_only.to_file_state() == .open_write);
    try std.testing.expect(read_write.to_file_state() == .open_read_write);
}

test "TypedFileHandle lifecycle" {
    const handle_id = FileHandleId.init(1, 1);
    var handle = TypedFileHandle.init(handle_id, "/test/file.txt", .read_write);

    try std.testing.expect(handle.is_open());
    try std.testing.expect(handle.can_read_now());
    try std.testing.expect(handle.can_write_now());
    try std.testing.expect(handle.query_position() == 0);
    try std.testing.expect(handle.query_size() == 0);

    // Write data
    try handle.write("hello");
    try std.testing.expect(handle.query_position() == 5);
    try std.testing.expect(handle.query_size() == 5);

    // Seek and read
    try handle.seek(0);
    try std.testing.expect(handle.query_position() == 0);

    var buffer: [10]u8 = undefined;
    const bytes_read = try handle.read(&buffer);
    try std.testing.expect(bytes_read == 5);
    try std.testing.expect(handle.query_position() == 5);

    // Close
    handle.close();
    try std.testing.expect(!handle.is_open());
}

test "FileHandleRegistry management" {
    var registry = FileHandleRegistry.init(std.testing.allocator);
    defer registry.deinit();

    // Register handles
    const handle1 = try registry.register_handle("/test1.txt", .read_only);
    const handle2 = try registry.register_handle("/test2.txt", .write_only);

    try std.testing.expect(registry.handle_count() == 2);
    try std.testing.expect(registry.has_open_handles());

    // Access handles
    const h1 = registry.query_handle(handle1);
    try std.testing.expect(h1 != null);
    try std.testing.expect(h1.?.can_read_now());
    try std.testing.expect(!h1.?.can_write_now());

    const h2 = registry.query_handle(handle2);
    try std.testing.expect(h2 != null);
    try std.testing.expect(!h2.?.can_read_now());
    try std.testing.expect(h2.?.can_write_now());

    // Close specific handle
    try std.testing.expect(registry.close_handle(handle1));
    try std.testing.expect(registry.handle_count() == 1);

    // Invalid handle
    const invalid = FileHandleId.init(999, 999);
    try std.testing.expect(registry.query_handle(invalid) == null);
}

test "FileOperations comprehensive workflow" {
    var registry = FileHandleRegistry.init(std.testing.allocator);
    defer registry.deinit();

    var ops = FileOperations.init(&registry);

    // Open file
    const handle = try ops.open_file("/test.txt", .read_write);

    // Write data
    const write_result = ops.write_to_file(handle, "test data");
    try std.testing.expect(write_result.is_success());
    try std.testing.expect(write_result.query_bytes().? == 9);

    // Seek to beginning
    const seek_result = ops.seek_in_file(handle, 0);
    try std.testing.expect(seek_result.is_success());

    // Read data back
    var buffer: [20]u8 = undefined;
    const read_result = ops.read_from_file(handle, &buffer);
    try std.testing.expect(read_result.is_success());
    try std.testing.expect(read_result.query_bytes().? == 9);

    // Get file info
    const info = ops.query_file_info(handle);
    try std.testing.expect(info != null);
    try std.testing.expect(info.?.size == 9);
    try std.testing.expect(info.?.is_readable());
    try std.testing.expect(info.?.is_writable());

    // Close file
    try std.testing.expect(ops.close_file(handle));
}

test "FileOperationResult error handling" {
    var registry = FileHandleRegistry.init(std.testing.allocator);
    defer registry.deinit();

    var ops = FileOperations.init(&registry);

    // Invalid handle operations
    const invalid_handle = FileHandleId.init(999, 999);
    var buffer: [10]u8 = undefined;

    const read_result = ops.read_from_file(invalid_handle, &buffer);
    try std.testing.expect(!read_result.is_success());
    try std.testing.expect(read_result.query_bytes() == null);

    // Convert to error
    _ = read_result.to_error() catch |err| {
        try std.testing.expect(err == error.InvalidHandle);
    };
}

test "file access mode restrictions" {
    var registry = FileHandleRegistry.init(std.testing.allocator);
    defer registry.deinit();

    var ops = FileOperations.init(&registry);

    // Open read-only file
    const read_handle = try ops.open_file("/readonly.txt", .read_only);

    // Writing should fail
    const write_result = ops.write_to_file(read_handle, "data");
    try std.testing.expect(!write_result.is_success());

    // Open write-only file
    const write_handle = try ops.open_file("/writeonly.txt", .write_only);

    // Reading should fail
    var buffer: [10]u8 = undefined;
    const read_result = ops.read_from_file(write_handle, &buffer);
    try std.testing.expect(!read_result.is_success());
}

test "handle generation prevents reuse" {
    var registry = FileHandleRegistry.init(std.testing.allocator);
    defer registry.deinit();

    // Register and close handle
    const handle1 = try registry.register_handle("/test.txt", .read_only);
    try std.testing.expect(registry.close_handle(handle1));

    // New handle should have different generation or ID
    const handle2 = try registry.register_handle("/test.txt", .read_only);
    try std.testing.expect(!handle1.eql(handle2));

    // Old handle should be invalid
    try std.testing.expect(registry.query_handle(handle1) == null);
    try std.testing.expect(registry.query_handle(handle2) != null);
}
