//! Virtual File System (VFS) abstraction for CortexDB storage operations.
//!
//! Design rationale: The VFS abstraction enables deterministic testing by allowing
//! identical production code to run against both real filesystems and simulated
//! in-memory filesystems. This eliminates the need for mocking while providing
//! comprehensive failure scenario testing capabilities.
//!
//! Directory iteration uses caller-provided arena allocators to avoid manual
//! cleanup patterns that violate the arena-per-subsystem memory management model.
//! All string memory is owned by the caller's arena and freed atomically.

const std = @import("std");
const assert = std.debug.assert;
const testing = std.testing;

/// Maximum path length for defensive validation across platforms
const MAX_PATH_LENGTH = 4096;

/// File magic number for production file validation
const PRODUCTION_FILE_MAGIC = 0xDEADBEEF_CAFEBABE;

/// Maximum reasonable file size to prevent memory exhaustion attacks
const MAX_REASONABLE_FILE_SIZE = 1024 * 1024 * 1024; // 1GB

// Cross-platform compatibility and security validation
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
};

/// VFile-specific errors for file operations
pub const VFileError = error{
    InvalidSeek,
    ReadError,
    WriteError,
    FileClosed,
    IoError,
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
        // File operations
        open: *const fn (ptr: *anyopaque, path: []const u8, mode: OpenMode) VFSError!VFile,
        create: *const fn (ptr: *anyopaque, path: []const u8) VFSError!VFile,
        remove: *const fn (ptr: *anyopaque, path: []const u8) VFSError!void,
        exists: *const fn (ptr: *anyopaque, path: []const u8) bool,

        // Directory operations
        mkdir: *const fn (ptr: *anyopaque, path: []const u8) VFSError!void,
        mkdir_all: *const fn (ptr: *anyopaque, path: []const u8) VFSError!void,
        rmdir: *const fn (ptr: *anyopaque, path: []const u8) VFSError!void,
        iterate_directory: *const fn (ptr: *anyopaque, path: []const u8, allocator: std.mem.Allocator) VFSError!DirectoryIterator,

        // Metadata operations
        rename: *const fn (ptr: *anyopaque, old_path: []const u8, new_path: []const u8) VFSError!void,
        stat: *const fn (ptr: *anyopaque, path: []const u8) VFSError!FileStat,

        // System operations
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

    // Delegation methods for type-safe interface

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
    pub fn read_file_alloc(self: VFS, allocator: std.mem.Allocator, path: []const u8, max_size: usize) VFSError![]u8 {
        var file = try self.open(path, .read);
        defer file.close();

        const file_size = try file.file_size();
        if (file_size > max_size) return VFSError.IoError;

        const content = try allocator.alloc(u8, file_size);
        const bytes_read = file.read(content) catch |err| switch (err) {
            error.OutOfMemory => return error.OutOfMemory,
            else => return VFSError.IoError,
        };

        // Truncate if file was smaller than reported size
        if (bytes_read < content.len) {
            return allocator.realloc(content, bytes_read);
        }

        return content;
    }
};

/// Virtual File interface providing platform-abstracted file operations
pub const VFile = struct {
    file_impl: *anyopaque,
    vtable: *const VTable,
    allocator: std.mem.Allocator,

    pub const VTable = struct {
        read: *const fn (ptr: *anyopaque, buffer: []u8) VFileError!usize,
        write: *const fn (ptr: *anyopaque, data: []const u8) VFileError!usize,
        seek: *const fn (ptr: *anyopaque, pos: u64, whence: SeekFrom) VFileError!u64,
        tell: *const fn (ptr: *anyopaque) VFileError!u64,
        flush: *const fn (ptr: *anyopaque) VFileError!void,
        close: *const fn (ptr: *anyopaque) void,
        file_size: *const fn (ptr: *anyopaque) VFileError!u64,
        destroy: *const fn (ptr: *anyopaque, allocator: std.mem.Allocator) void,
    };

    pub const SeekFrom = enum(u8) {
        start = 0x01,
        current = 0x02,
        end = 0x03,
    };

    // Delegation methods for type-safe interface

    pub fn read(self: VFile, buffer: []u8) VFileError!usize {
        return self.vtable.read(self.file_impl, buffer);
    }

    pub fn write(self: VFile, data: []const u8) VFileError!usize {
        return self.vtable.write(self.file_impl, data);
    }

    pub fn seek(self: VFile, pos: u64, whence: SeekFrom) VFileError!u64 {
        return self.vtable.seek(self.file_impl, pos, whence);
    }

    pub fn tell(self: VFile) VFileError!u64 {
        return self.vtable.tell(self.file_impl);
    }

    pub fn flush(self: VFile) VFileError!void {
        return self.vtable.flush(self.file_impl);
    }

    pub fn close(self: VFile) void {
        self.vtable.close(self.file_impl);
    }

    pub fn file_size(self: VFile) VFileError!u64 {
        return self.vtable.file_size(self.file_impl);
    }

    /// Release file resources. Safe to call after close().
    pub fn deinit(self: VFile) void {
        self.vtable.destroy(self.file_impl, self.allocator);
    }
};
