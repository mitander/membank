//! Virtual File System (VFS) abstraction for KausalDB storage operations.
//!
//! Design rationale: The VFS abstraction enables deterministic testing by allowing
//! identical production code to run against both real filesystems and simulated
//! in-memory filesystems. This eliminates the need for mocking while providing
//! failure scenario testing capabilities.
//!
//! Directory iteration uses caller-provided arena allocators to avoid manual
//! cleanup patterns that violate the arena-per-subsystem memory management model.
//! All string memory is owned by the caller's arena and freed atomically.

const std = @import("std");

const assert_mod = @import("assert.zig");
const file_handle = @import("file_handle.zig");

const assert = assert_mod.assert;
const fatal_assert = assert_mod.fatal_assert;
const testing = std.testing;

/// Maximum path length for defensive validation across platforms
const MAX_PATH_LENGTH = 4096;

/// File magic number for production file validation
const PRODUCTION_FILE_MAGIC = 0xDEADBEEF_CAFEBABE;

/// Maximum reasonable file size to prevent memory exhaustion attacks
const MAX_REASONABLE_FILE_SIZE = 1024 * 1024 * 1024; // 1GB

comptime {
    assert(MAX_PATH_LENGTH > 0);
    assert(MAX_PATH_LENGTH <= 8192);
    assert(MAX_REASONABLE_FILE_SIZE > 0);
    assert(MAX_REASONABLE_FILE_SIZE < std.math.maxInt(u64) / 2);
}

/// VFS-specific errors distinct from generic I/O failures
pub const VFSError = error{
    FileNotFound,
    AccessDenied,
    IsDirectory,
    NotDirectory,
    FileExists,
    DirectoryNotEmpty,
    InvalidPath,
    OutOfMemory,
    IoError,
    Unsupported,
    NoSpaceLeft,
};

/// VFile-specific errors for file operations
pub const VFileError = error{
    InvalidSeek,
    ReadError,
    WriteError,
    FileClosed,
    ReadOnlyFile,
    WriteOnlyFile,
    EmptyData,
    InvalidFileState,
    IoError,
    NoSpaceLeft,
} || std.mem.Allocator.Error;

/// Directory entry with type information for efficient filtering
pub const DirectoryEntry = struct {
    name: []const u8,
    kind: Kind,

    pub const Kind = enum(u8) {
        file = 0x01,
        directory = 0x02,
        symlink = 0x03,
        unknown = 0xFF,

        /// Convert from platform-specific file type to our abstraction
        pub fn from_file_type(file_type: std.fs.File.Kind) Kind {
            return switch (file_type) {
                .file => .file,
                .directory => .directory,
                .sym_link => .symlink,
                else => .unknown,
            };
        }
    };
};

/// Directory iterator using caller-provided arena for memory management.
/// Eliminates manual cleanup patterns by using arena-per-subsystem model.
pub const DirectoryIterator = struct {
    entries: []DirectoryEntry,
    index: usize,

    comptime {
        assert(@sizeOf(usize) >= 4); // Minimum 32-bit addressing
    }

    /// Get next directory entry or null if iteration complete.
    /// Entries are returned in filesystem order (typically sorted).
    pub fn next(self: *DirectoryIterator) ?DirectoryEntry {
        if (self.index >= self.entries.len) return null;

        const entry = self.entries[self.index];
        self.index += 1;
        return entry;
    }

    /// Clean up allocated memory for directory entries.
    /// Must be called with the same allocator used for iterate_directory().
    pub fn deinit(self: *DirectoryIterator, allocator: std.mem.Allocator) void {
        for (self.entries) |entry| {
            allocator.free(entry.name);
        }
        allocator.free(self.entries);
    }

    /// Reset iterator to beginning for reuse within same arena scope
    pub fn reset(self: *DirectoryIterator) void {
        self.index = 0;
    }

    /// Get remaining entry count for memory planning
    pub fn remaining(self: *const DirectoryIterator) usize {
        return if (self.index < self.entries.len)
            self.entries.len - self.index
        else
            0;
    }
};

/// Virtual File System interface providing platform abstraction
pub const VFS = struct {
    ptr: *anyopaque,
    vtable: *const VTable,

    pub const VTable = struct {
        open: *const fn (ptr: *anyopaque, path: []const u8, mode: OpenMode) VFSError!VFile,
        create: *const fn (ptr: *anyopaque, path: []const u8) VFSError!VFile,
        remove: *const fn (ptr: *anyopaque, path: []const u8) VFSError!void,
        exists: *const fn (ptr: *anyopaque, path: []const u8) bool,

        mkdir: *const fn (ptr: *anyopaque, path: []const u8) VFSError!void,
        mkdir_all: *const fn (ptr: *anyopaque, path: []const u8) VFSError!void,
        rmdir: *const fn (ptr: *anyopaque, path: []const u8) VFSError!void,
        iterate_directory: *const fn (ptr: *anyopaque, path: []const u8, allocator: std.mem.Allocator) VFSError!DirectoryIterator,

        rename: *const fn (ptr: *anyopaque, old_path: []const u8, new_path: []const u8) VFSError!void,
        stat: *const fn (ptr: *anyopaque, path: []const u8) VFSError!FileStat,

        sync: *const fn (ptr: *anyopaque) VFSError!void,
        deinit: *const fn (ptr: *anyopaque, allocator: std.mem.Allocator) void,
    };

    pub const OpenMode = enum(u8) {
        read = 0x01,
        write = 0x02,
        read_write = 0x03,

        /// Check if mode allows reading operations
        pub fn can_read(self: OpenMode) bool {
            return self == .read or self == .read_write;
        }

        /// Check if mode allows writing operations
        pub fn can_write(self: OpenMode) bool {
            return self == .write or self == .read_write;
        }
    };

    pub const FileStat = struct {
        size: u64,
        created_time: i64,
        modified_time: i64,
        is_directory: bool,

        /// Validate stat result for consistency
        pub fn is_valid(self: FileStat) bool {
            return self.size <= MAX_REASONABLE_FILE_SIZE and
                self.created_time >= 0 and
                self.modified_time >= 0 and
                self.modified_time >= self.created_time;
        }
    };

    pub fn open(self: VFS, path: []const u8, mode: OpenMode) VFSError!VFile {
        return self.vtable.open(self.ptr, path, mode);
    }

    pub fn create(self: VFS, path: []const u8) VFSError!VFile {
        return self.vtable.create(self.ptr, path);
    }

    pub fn remove(self: VFS, path: []const u8) VFSError!void {
        return self.vtable.remove(self.ptr, path);
    }

    pub fn exists(self: VFS, path: []const u8) bool {
        return self.vtable.exists(self.ptr, path);
    }

    pub fn mkdir(self: VFS, path: []const u8) VFSError!void {
        return self.vtable.mkdir(self.ptr, path);
    }

    pub fn mkdir_all(self: VFS, path: []const u8) VFSError!void {
        return self.vtable.mkdir_all(self.ptr, path);
    }

    pub fn rmdir(self: VFS, path: []const u8) VFSError!void {
        return self.vtable.rmdir(self.ptr, path);
    }

    /// Iterate directory entries using caller-provided arena allocator.
    /// All entry names are allocated in the provided arena and freed
    /// atomically when the arena is reset.
    pub fn iterate_directory(self: VFS, path: []const u8, allocator: std.mem.Allocator) VFSError!DirectoryIterator {
        return self.vtable.iterate_directory(self.ptr, path, allocator);
    }

    pub fn rename(self: VFS, old_path: []const u8, new_path: []const u8) VFSError!void {
        return self.vtable.rename(self.ptr, old_path, new_path);
    }

    pub fn stat(self: VFS, path: []const u8) VFSError!FileStat {
        return self.vtable.stat(self.ptr, path);
    }

    pub fn sync(self: VFS) VFSError!void {
        return self.vtable.sync(self.ptr);
    }

    pub fn deinit(self: VFS, allocator: std.mem.Allocator) void {
        self.vtable.deinit(self.ptr, allocator);
    }

    /// Read entire file into caller-provided arena allocator.
    /// Memory is owned by the arena and freed atomically on arena reset.
    pub fn read_file_alloc(
        self: VFS,
        allocator: std.mem.Allocator,
        path: []const u8,
        max_size: usize,
    ) (VFSError || VFileError)![]u8 {
        var file = try self.open(path, .read);
        defer file.close();

        const file_size = try file.file_size();
        if (file_size > max_size) return VFSError.IoError;

        const content = try allocator.alloc(u8, file_size);
        const bytes_read = file.read(content) catch |err| switch (err) {
            error.OutOfMemory => return error.OutOfMemory,
            else => return VFSError.IoError,
        };

        if (bytes_read < content.len) {
            return allocator.realloc(content, bytes_read);
        }

        return content;
    }
};

/// Virtual File interface providing platform-abstracted file operations.
/// VFile is a value type that manages its own resources internally,
/// following the arena-per-subsystem memory management pattern.
pub const VFile = struct {
    impl: union(enum) {
        production: ProductionFileImpl,
        simulation: SimulationFileImpl,
    },

    const ProductionFileImpl = struct {
        file: std.fs.File,
        closed: bool,
    };

    const SimulationFileImpl = struct {
        vfs_ptr: *anyopaque,
        handle_id: file_handle.FileHandleId,
        position: u64,
        mode: VFS.OpenMode,
        closed: bool,
        file_data_fn: *const fn (*anyopaque, file_handle.FileHandleId) ?*SimulationFileData,
        current_time_fn: *const fn (*anyopaque) i64,
        fault_injection_fn: *const fn (*anyopaque, usize) VFileError!usize,
        read_corruption_fn: *const fn (*anyopaque, []u8) void,
    };

    pub const SeekFrom = enum(u8) {
        start = 0x01,
        current = 0x02,
        end = 0x03,
    };

    /// Read data from the file into the provided buffer
    ///
    /// Returns the number of bytes read, or 0 at end-of-file.
    /// Works with both production filesystem and simulation VFS.
    pub fn read(self: *VFile, buffer: []u8) VFileError!usize {
        return switch (self.impl) {
            .production => |*prod| blk: {
                if (prod.closed) return VFileError.FileClosed;
                break :blk prod.file.read(buffer) catch |err| switch (err) {
                    error.AccessDenied => VFileError.ReadError,
                    error.Unexpected => VFileError.IoError,
                    else => VFileError.IoError,
                };
            },
            .simulation => |*sim| blk: {
                if (sim.closed) return VFileError.FileClosed;
                if (!sim.mode.can_read()) return VFileError.ReadError;

                fatal_assert(@intFromPtr(sim.vfs_ptr) >= 0x1000 and sim.handle_id.is_valid(), "VFS handle corruption detected: ptr=0x{X} handle={} - memory safety violation", .{ @intFromPtr(sim.vfs_ptr), sim.handle_id.id });
                fatal_assert(!sim.closed, "VFS file handle used after close - use-after-free detected", .{});

                const data = sim.file_data_fn(sim.vfs_ptr, sim.handle_id) orelse return VFileError.FileClosed;

                const available = @min(buffer.len, data.content.items.len - sim.position);
                if (available == 0) break :blk 0;

                @memcpy(buffer[0..available], data.content.items[sim.position .. sim.position + available]);

                // Apply read corruption fault injection
                sim.read_corruption_fn(sim.vfs_ptr, buffer[0..available]);

                sim.position += available;
                break :blk available;
            },
        };
    }

    /// Write data to the file from the provided buffer
    ///
    /// Returns the number of bytes written. All bytes are written or an error occurs.
    /// Works with both production filesystem and simulation VFS for deterministic testing.
    pub fn write(self: *VFile, data: []const u8) VFileError!usize {
        return switch (self.impl) {
            .production => |*prod| blk: {
                if (prod.closed) return VFileError.FileClosed;
                break :blk prod.file.write(data) catch |err| switch (err) {
                    error.AccessDenied => VFileError.WriteError,
                    error.NoSpaceLeft => VFileError.WriteError,
                    error.Unexpected => VFileError.IoError,
                    else => VFileError.IoError,
                };
            },
            .simulation => |*sim| blk: {
                if (sim.closed) return VFileError.FileClosed;
                if (!sim.mode.can_write()) return VFileError.WriteError;

                fatal_assert(@intFromPtr(sim.vfs_ptr) >= 0x1000 and sim.handle_id.is_valid(), "VFS handle corruption detected in write: ptr=0x{X} handle={} - memory safety violation", .{ @intFromPtr(sim.vfs_ptr), sim.handle_id.id });
                assert(data.len > 0);

                const actual_write_size = sim.fault_injection_fn(sim.vfs_ptr, data.len) catch |err| {
                    return err;
                };

                {
                    const file_data = sim.file_data_fn(sim.vfs_ptr, sim.handle_id) orelse return VFileError.FileClosed;
                    if (sim.position + actual_write_size > file_data.content.items.len) {
                        const old_len = file_data.content.items.len;
                        const new_len = sim.position + actual_write_size;

                        // ensureTotalCapacity preserves existing data automatically
                        try file_data.content.ensureTotalCapacity(new_len);

                        const fresh_file_data = sim.file_data_fn(sim.vfs_ptr, sim.handle_id) orelse return VFileError.FileClosed;

                        fresh_file_data.content.items.len = new_len;

                        @memset(fresh_file_data.content.items[old_len..new_len], 0);

                        const allocated_slice = fresh_file_data.content.allocatedSlice();
                        if (allocated_slice.len > new_len) {
                            @memset(allocated_slice[new_len..], 0);
                        }

                        if (sim.position > old_len) {
                            @memset(fresh_file_data.content.items[old_len..sim.position], 0);
                        }
                    }
                }

                const file_data = sim.file_data_fn(sim.vfs_ptr, sim.handle_id) orelse return VFileError.FileClosed;
                @memcpy(file_data.content.items[sim.position .. sim.position + actual_write_size], data[0..actual_write_size]);
                sim.position += actual_write_size;

                const write_start_pos = sim.position - actual_write_size;
                const verify_file_data = sim.file_data_fn(sim.vfs_ptr, sim.handle_id) orelse return VFileError.FileClosed;
                const written_slice = verify_file_data.content.items[write_start_pos..sim.position];

                if (false) {
                    std.debug.print("=== VFS WRITE VERIFICATION: pos={}, size={} ===\n", .{ write_start_pos, actual_write_size });
                    std.debug.print("Expected first 20 bytes: ", .{});
                    for (data[0..@min(20, data.len)], 0..) |byte, i| {
                        std.debug.print("{}:0x{X} ", .{ i, byte });
                    }
                    std.debug.print("\n", .{});
                    std.debug.print("Actual first 20 bytes:   ", .{});
                    for (written_slice[0..@min(20, written_slice.len)], 0..) |byte, i| {
                        std.debug.print("{}:0x{X} ", .{ i, byte });
                    }
                    std.debug.print("\n", .{});

                    const debug_file_data = sim.file_data_fn(sim.vfs_ptr, sim.handle_id) orelse return VFileError.FileClosed;
                    std.debug.print("=== FULL FILE CONTENT (first 64 bytes) ===\n", .{});
                    for (debug_file_data.content.items[0..@min(64, debug_file_data.content.items.len)], 0..) |byte, i| {
                        std.debug.print("{}:0x{X} ", .{ i, byte });
                        if (i > 0 and (i + 1) % 16 == 0) std.debug.print("\n", .{});
                    }
                    if (debug_file_data.content.items.len % 16 != 0) std.debug.print("\n", .{});
                }

                if (!std.mem.eql(u8, written_slice, data[0..actual_write_size])) {
                    std.debug.print("VFS write corruption detected: written data mismatch at pos {}\n", .{write_start_pos});
                    if (actual_write_size >= 8) {
                        const expected = std.mem.readInt(u64, data[0..8], .little);
                        const actual = std.mem.readInt(u64, written_slice[0..8], .little);
                        std.debug.print("VFS corruption: expected 0x{X}, got 0x{X}\n", .{ expected, actual });
                    }
                    return VFileError.IoError;
                }

                const time_update_file_data = sim.file_data_fn(sim.vfs_ptr, sim.handle_id) orelse return VFileError.FileClosed;
                time_update_file_data.modified_time = sim.current_time_fn(sim.vfs_ptr);
                break :blk actual_write_size;
            },
        };
    }

    /// Write data to a specific offset in the file without changing the current position
    ///
    /// Preserves the original file position after the write operation completes.
    /// Useful for efficient random access writes in storage systems.
    pub fn write_at(self: *VFile, offset: u64, data: []const u8) VFileError!usize {
        return switch (self.impl) {
            .production => |*prod| blk: {
                if (prod.closed) return VFileError.FileClosed;

                const current_pos = prod.file.getPos() catch return VFileError.IoError;
                prod.file.seekTo(offset) catch return VFileError.IoError;

                const bytes_written = prod.file.write(data) catch |err| switch (err) {
                    error.AccessDenied => return VFileError.WriteError,
                    error.NoSpaceLeft => return VFileError.WriteError,
                    error.Unexpected => return VFileError.IoError,
                    else => return VFileError.IoError,
                };

                prod.file.seekTo(current_pos) catch return VFileError.IoError;

                break :blk bytes_written;
            },
            .simulation => |*sim| blk: {
                if (sim.closed) return VFileError.FileClosed;
                if (!sim.mode.can_write()) return VFileError.WriteError;

                fatal_assert(@intFromPtr(sim.vfs_ptr) >= 0x1000 and sim.handle_id.is_valid(), "VFS handle corruption detected in write_at: ptr=0x{X} handle={} - memory safety violation", .{ @intFromPtr(sim.vfs_ptr), sim.handle_id.id });
                assert(data.len > 0);

                const actual_write_size = sim.fault_injection_fn(sim.vfs_ptr, data.len) catch |err| {
                    return err;
                };

                const file_data = sim.file_data_fn(sim.vfs_ptr, sim.handle_id) orelse return VFileError.IoError;
                const required_size = offset + actual_write_size;
                if (file_data.content.items.len < required_size) {
                    file_data.content.resize(required_size) catch return VFileError.WriteError;
                }

                @memcpy(file_data.content.items[offset .. offset + actual_write_size], data[0..actual_write_size]);
                break :blk actual_write_size;
            },
        };
    }

    /// Change the file position to the specified location
    ///
    /// Returns the new absolute position in the file.
    /// Essential for random access file operations in the storage engine.
    pub fn seek(self: *VFile, pos: u64, whence: SeekFrom) VFileError!u64 {
        return switch (self.impl) {
            .production => |*prod| blk: {
                if (prod.closed) return VFileError.FileClosed;
                const target_pos = switch (whence) {
                    .start => pos,
                    .current => blk2: {
                        const current_pos = prod.file.getPos() catch return VFileError.IoError;
                        break :blk2 current_pos + pos;
                    },
                    .end => blk2: {
                        const file_end = prod.file.getEndPos() catch return VFileError.IoError;
                        break :blk2 file_end + pos;
                    },
                };

                prod.file.seekTo(target_pos) catch |err| {
                    return switch (err) {
                        error.Unseekable => VFileError.InvalidSeek,
                        else => VFileError.IoError,
                    };
                };

                break :blk prod.file.getPos() catch VFileError.IoError;
            },
            .simulation => |*sim| blk: {
                if (sim.closed) return VFileError.FileClosed;

                fatal_assert(@intFromPtr(sim.vfs_ptr) >= 0x1000 and sim.handle_id.is_valid(), "VFS handle corruption detected in seek: ptr=0x{X} handle={} - memory safety violation", .{ @intFromPtr(sim.vfs_ptr), sim.handle_id.id });
                fatal_assert(!sim.closed, "VFS file handle used after close in seek - use-after-free detected", .{});

                const file_data = sim.file_data_fn(sim.vfs_ptr, sim.handle_id) orelse return VFileError.FileClosed;

                const target_pos = switch (whence) {
                    .start => pos,
                    .current => sim.position + pos,
                    .end => file_data.content.items.len + pos,
                };

                sim.position = target_pos;
                break :blk target_pos;
            },
        };
    }

    /// Get the current file position
    ///
    /// Returns the absolute byte offset from the beginning of the file.
    /// Useful for tracking position during sequential operations.
    pub fn tell(self: *VFile) VFileError!u64 {
        return switch (self.impl) {
            .production => |*prod| blk: {
                if (prod.closed) return VFileError.FileClosed;
                break :blk prod.file.getPos() catch VFileError.IoError;
            },
            .simulation => |*sim| blk: {
                if (sim.closed) return VFileError.FileClosed;
                break :blk sim.position;
            },
        };
    }

    /// Force all buffered writes to be written to the underlying storage
    ///
    /// Ensures data durability by synchronizing with the storage device.
    /// Critical for WAL operations and database consistency.
    pub fn flush(self: *VFile) VFileError!void {
        return switch (self.impl) {
            .production => |*prod| blk: {
                if (prod.closed) return VFileError.FileClosed;
                prod.file.sync() catch |err| {
                    break :blk switch (err) {
                        error.AccessDenied => VFileError.WriteError,
                        else => VFileError.IoError,
                    };
                };
            },
            .simulation => |*sim| blk: {
                if (sim.closed) return VFileError.FileClosed;
                break :blk;
            },
        };
    }

    /// Close the file and release associated resources
    ///
    /// After calling close(), all other operations on this file will fail.
    /// Safe to call multiple times - subsequent calls are no-ops.
    pub fn close(self: *VFile) void {
        switch (self.impl) {
            .production => |*prod| {
                if (!prod.closed) {
                    prod.file.close();
                    prod.closed = true;
                }
            },
            .simulation => |*sim| {
                sim.closed = true;
            },
        }
    }

    /// Get the total size of the file in bytes
    ///
    /// Returns the current file size regardless of the current position.
    /// Works with both production files and simulation VFS.
    pub fn file_size(self: *VFile) VFileError!u64 {
        return switch (self.impl) {
            .production => |*prod| blk: {
                if (prod.closed) return VFileError.FileClosed;
                const size = prod.file.getEndPos() catch |err| {
                    return switch (err) {
                        error.AccessDenied => VFileError.ReadError,
                        else => VFileError.IoError,
                    };
                };

                if (size > MAX_REASONABLE_FILE_SIZE) {
                    return VFileError.IoError;
                }

                break :blk size;
            },
            .simulation => |*sim| blk: {
                if (sim.closed) return VFileError.FileClosed;

                fatal_assert(@intFromPtr(sim.vfs_ptr) >= 0x1000 and sim.handle_id.is_valid(), "VFS handle corruption detected in file_size: ptr=0x{X} handle={} - memory safety violation", .{ @intFromPtr(sim.vfs_ptr), sim.handle_id.id });
                fatal_assert(!sim.closed, "VFS file handle used after close in file_size - use-after-free detected", .{});

                const file_data = sim.file_data_fn(sim.vfs_ptr, sim.handle_id) orelse return VFileError.FileClosed;
                break :blk file_data.content.items.len;
            },
        };
    }
};

/// Simulation file data structure used by VFile.
/// This must match the structure used by SimulationVFS implementations.
pub const SimulationFileData = struct {
    content: std.array_list.Managed(u8),
    created_time: i64,
    modified_time: i64,
    is_directory: bool,
};

const simulation_vfs = @import("../sim/simulation_vfs.zig");
const SimulationVFS = simulation_vfs.SimulationVFS;

test "vfs memory expansion safety" {
    const allocator = testing.allocator;

    var sim_vfs = try SimulationVFS.init(allocator);
    defer sim_vfs.deinit();
    const test_vfs = sim_vfs.vfs();

    var file = try test_vfs.create("expansion_test.log");
    defer file.close();

    const header = "HEADER01";
    const written_header = try file.write(header);
    try testing.expectEqual(header.len, written_header);

    var large_buffer: [32768]u8 = undefined;
    @memset(&large_buffer, 0xAB);

    _ = try file.seek(header.len, .start);
    const written_large = try file.write(&large_buffer);
    try testing.expectEqual(large_buffer.len, written_large);

    _ = try file.seek(0, .start);
    var header_verify: [8]u8 = undefined;
    const read_header = try file.read(&header_verify);
    try testing.expectEqual(header.len, read_header);
    try testing.expect(std.mem.eql(u8, header, header_verify[0..read_header]));

    _ = try file.seek(header.len, .start);
    var large_verify: [1024]u8 = undefined;
    const read_large = try file.read(&large_verify);
    try testing.expectEqual(1024, read_large);

    for (large_verify) |byte| {
        try testing.expectEqual(@as(u8, 0xAB), byte);
    }
}

test "vfs multiple file handle stability" {
    const allocator = testing.allocator;

    var sim_vfs = try SimulationVFS.init(allocator);
    defer sim_vfs.deinit();
    const test_vfs = sim_vfs.vfs();

    const num_files = 50;
    var files: [num_files]VFile = undefined;
    var file_data: [num_files][16]u8 = undefined;

    for (0..num_files) |i| {
        const file_name = try std.fmt.allocPrint(allocator, "stress_file_{}.log", .{i});
        defer allocator.free(file_name);
        files[i] = try test_vfs.create(file_name);

        std.mem.writeInt(u64, file_data[i][0..8], @as(u64, i), .little);
        std.mem.writeInt(u64, file_data[i][8..16], @as(u64, i) ^ 0xDEADBEEF, .little);

        const written = try files[i].write(&file_data[i]);
        try testing.expectEqual(16, written);
    }

    for (0..num_files) |i| {
        _ = try files[i].seek(0, .start);
        var read_data: [16]u8 = undefined;
        const read_bytes = try files[i].read(&read_data);
        try testing.expectEqual(16, read_bytes);
        try testing.expect(std.mem.eql(u8, &file_data[i], &read_data));
        files[i].close();
    }
}

test "vfs sparse file handling" {
    const allocator = testing.allocator;

    var sim_vfs = try SimulationVFS.init(allocator);
    defer sim_vfs.deinit();
    const test_vfs = sim_vfs.vfs();

    var file = try test_vfs.create("sparse_test.log");
    defer file.close();

    const header = "HEADER01";
    _ = try file.write(header);

    _ = try file.seek(4096, .start);
    const footer = "FOOTER01";
    _ = try file.write(footer);

    const file_size = try file.file_size();
    try testing.expectEqual(4096 + footer.len, file_size);

    _ = try file.seek(0, .start);
    var header_buffer: [8]u8 = undefined;
    const read_header = try file.read(&header_buffer);
    try testing.expectEqual(header.len, read_header);
    try testing.expect(std.mem.eql(u8, header, header_buffer[0..read_header]));

    _ = try file.seek(header.len, .start);
    var gap_buffer: [100]u8 = undefined;
    const read_gap = try file.read(&gap_buffer);
    try testing.expectEqual(100, read_gap);

    for (gap_buffer) |byte| {
        try testing.expectEqual(@as(u8, 0), byte);
    }

    _ = try file.seek(4096, .start);
    var footer_buffer: [8]u8 = undefined;
    const read_footer = try file.read(&footer_buffer);
    try testing.expectEqual(footer.len, read_footer);
    try testing.expect(std.mem.eql(u8, footer, footer_buffer[0..read_footer]));
}

test "vfs capacity boundary conditions" {
    const allocator = testing.allocator;

    var sim_vfs = try SimulationVFS.init(allocator);
    defer sim_vfs.deinit();
    const test_vfs = sim_vfs.vfs();

    var file = try test_vfs.create("boundary_test.log");
    defer file.close();

    const boundary_sizes = [_]usize{ 127, 128, 129, 255, 256, 257, 511, 512, 513, 1023, 1024, 1025 };

    for (boundary_sizes) |size| {
        const write_buffer = try allocator.alloc(u8, size);
        defer allocator.free(write_buffer);
        @memset(write_buffer, @intCast(size & 0xFF));

        const written = try file.write(write_buffer);
        try testing.expectEqual(size, written);

        const pos = try file.tell();
        _ = try file.seek(pos - size, .start);

        const read_buffer = try allocator.alloc(u8, size);
        defer allocator.free(read_buffer);
        const read_bytes = try file.read(read_buffer);
        try testing.expectEqual(size, read_bytes);
        try testing.expect(std.mem.eql(u8, write_buffer, read_buffer));

        _ = try file.seek(pos, .start);
    }
}

test "vfs data integrity with checksums" {
    const allocator = testing.allocator;

    var sim_vfs = try SimulationVFS.init(allocator);
    defer sim_vfs.deinit();
    const test_vfs = sim_vfs.vfs();

    var file = try test_vfs.create("checksum_test.log");
    defer file.close();

    const test_data = "KausalDB deterministic checksum validation pattern";
    const expected_checksum = blk: {
        var hasher = std.hash.Crc32.init();
        hasher.update(test_data);
        break :blk hasher.final();
    };

    _ = try file.write(test_data);

    for (0..5) |_| {
        _ = try file.seek(0, .start);

        var read_buffer: [test_data.len]u8 = undefined;
        const read_bytes = try file.read(&read_buffer);
        try testing.expectEqual(test_data.len, read_bytes);
        try testing.expect(std.mem.eql(u8, test_data, &read_buffer));

        var hasher = std.hash.Crc32.init();
        hasher.update(&read_buffer);
        const actual_checksum = hasher.final();
        try testing.expectEqual(expected_checksum, actual_checksum);
    }
}

test "vfs directory operations" {
    const allocator = testing.allocator;

    var sim_vfs = try SimulationVFS.init(allocator);
    defer sim_vfs.deinit();
    const test_vfs = sim_vfs.vfs();

    try test_vfs.mkdir("test_dir");
    try testing.expect(test_vfs.exists("test_dir"));

    try test_vfs.mkdir_all("nested/deep/structure");
    try testing.expect(test_vfs.exists("nested"));
    try testing.expect(test_vfs.exists("nested/deep"));
    try testing.expect(test_vfs.exists("nested/deep/structure"));

    var nested_file = try test_vfs.create("nested/deep/test_file.log");
    defer nested_file.close();

    const nested_data = "Nested file data";
    _ = try nested_file.write(nested_data);

    try testing.expect(test_vfs.exists("nested/deep/test_file.log"));

    _ = try nested_file.seek(0, .start);
    var verify_buffer: [16]u8 = undefined;
    const read_bytes = try nested_file.read(&verify_buffer);
    try testing.expectEqual(nested_data.len, read_bytes);
    try testing.expect(std.mem.eql(u8, nested_data, verify_buffer[0..read_bytes]));
}

test "vfs error handling patterns" {
    const allocator = testing.allocator;

    var sim_vfs = try SimulationVFS.init(allocator);
    defer sim_vfs.deinit();
    const test_vfs = sim_vfs.vfs();

    const open_result = test_vfs.open("non_existent.log", .read);
    try testing.expectError(error.FileNotFound, open_result);

    var file = try test_vfs.create("close_test.log");
    _ = try file.write("test");
    file.close();

    try testing.expect(test_vfs.exists("close_test.log"));
}
