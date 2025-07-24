//! Virtual File System (VFS) interface for CortexDB.
//!
//! ## The Cornerstone of Determinism
//!
//! The VFS is the single most important abstraction for ensuring the correctness and
//! reliability of CortexDB. It is not merely an OS abstraction layer; it is the
//! foundation of our "Simulation First" testing philosophy.
//!
//! Every single file I/O operation in the engine MUST go through this interface.
//! This allows the production code to be tested, byte-for-byte, within a
//! deterministic `SimulationVFS` that can simulate disk corruption, I/O errors,
//! and other catastrophic failures in a perfectly reproducible manner.
//!
//! **Developer Consideration:** Bypassing the VFS (e.g., by using `std.fs` directly
//! in the storage engine) is considered a critical architectural violation, as it
//! undermines the system's guarantee of testability and correctness.

const std = @import("std");
const assert = std.debug.assert;

// Magic number for ProductionFile corruption detection
const PRODUCTION_FILE_MAGIC: u64 = 0xDEADBEEF_CAFEBABE;

/// Directory iterator for VFS
pub const DirectoryIterator = struct {
    entries: [][]const u8,
    index: usize,
    allocator: std.mem.Allocator,
    vfs: *VFS,
    base_path: []const u8,

    pub const Entry = struct {
        name: []const u8,
        kind: Kind,

        pub const Kind = enum {
            file,
            directory,
            symlink,
            unknown,
        };
    };

    pub fn next(self: *DirectoryIterator) ?Entry {
        if (self.index >= self.entries.len) {
            return null;
        }

        const name = self.entries[self.index];
        self.index += 1;

        // Build full path to stat the entry
        const full_path = std.fs.path.join(self.allocator, &.{ self.base_path, name }) catch {
            return Entry{
                .name = name,
                .kind = .unknown,
            };
        };
        defer self.allocator.free(full_path);

        // Determine the actual file type
        const kind = if (self.vfs.stat(full_path)) |stat|
            if (stat.is_directory) Entry.Kind.directory else Entry.Kind.file
        else |_|
            Entry.Kind.unknown;

        return Entry{
            .name = name,
            .kind = kind,
        };
    }

    pub fn deinit(self: *DirectoryIterator) void {
        for (self.entries) |entry| {
            self.allocator.free(entry);
        }
        self.allocator.free(self.entries);
    }
};

/// Virtual File System interface.
/// All file I/O operations go through this interface.
pub const VFS = struct {
    ptr: *anyopaque,
    vtable: *const VTable,

    pub const VTable = struct {
        open: *const fn (ptr: *anyopaque, path: []const u8, mode: OpenMode) anyerror!*VFileImpl,
        create: *const fn (ptr: *anyopaque, path: []const u8) anyerror!*VFileImpl,
        remove: *const fn (ptr: *anyopaque, path: []const u8) anyerror!void,
        exists: *const fn (ptr: *anyopaque, path: []const u8) bool,
        mkdir: *const fn (ptr: *anyopaque, path: []const u8) anyerror!void,
        rmdir: *const fn (ptr: *anyopaque, path: []const u8) anyerror!void,
        list_dir: *const fn (
            ptr: *anyopaque,
            path: []const u8,
            allocator: std.mem.Allocator,
        ) anyerror![][]const u8,
        rename: *const fn (
            ptr: *anyopaque,
            old_path: []const u8,
            new_path: []const u8,
        ) anyerror!void,
        stat: *const fn (ptr: *anyopaque, path: []const u8) anyerror!FileStat,
        sync: *const fn (ptr: *anyopaque) anyerror!void,
        deinit: *const fn (ptr: *anyopaque) void,
    };

    pub const OpenMode = enum {
        read,
        write,
        read_write,
    };

    pub const FileStat = struct {
        size: u64,
        created_time: u64,
        modified_time: u64,
        is_directory: bool,
    };

    pub fn open(self: *VFS, path: []const u8, mode: OpenMode) anyerror!VFile {
        const impl = try self.vtable.open(self.ptr, path, mode);
        return VFile.init(impl);
    }

    pub fn create(self: *VFS, path: []const u8) anyerror!VFile {
        const impl = try self.vtable.create(self.ptr, path);
        return VFile.init(impl);
    }

    pub fn remove(self: *VFS, path: []const u8) anyerror!void {
        return self.vtable.remove(self.ptr, path);
    }

    pub fn exists(self: *VFS, path: []const u8) bool {
        return self.vtable.exists(self.ptr, path);
    }

    pub fn mkdir(self: *VFS, path: []const u8) anyerror!void {
        return self.vtable.mkdir(self.ptr, path);
    }

    pub fn rmdir(self: *VFS, path: []const u8) anyerror!void {
        return self.vtable.rmdir(self.ptr, path);
    }

    pub fn list_dir(
        self: *VFS,
        path: []const u8,
        allocator: std.mem.Allocator,
    ) anyerror![][]const u8 {
        return self.vtable.list_dir(self.ptr, path, allocator);
    }

    pub fn rename(self: *VFS, old_path: []const u8, new_path: []const u8) anyerror!void {
        return self.vtable.rename(self.ptr, old_path, new_path);
    }

    pub fn stat(self: *VFS, path: []const u8) anyerror!FileStat {
        return self.vtable.stat(self.ptr, path);
    }

    pub fn sync(self: *VFS) anyerror!void {
        return self.vtable.sync(self.ptr);
    }

    pub fn deinit(self: *VFS) void {
        self.vtable.deinit(self.ptr);
    }

    /// Read entire file content into allocated memory
    pub fn read_file_alloc(self: *VFS, allocator: std.mem.Allocator, path: []const u8, max_size: usize) ![]u8 {
        var file = try self.open(path, .read);
        defer file.force_close();

        const file_size = try file.file_size();
        if (file_size > max_size) {
            return error.FileTooLarge;
        }

        const content = try allocator.alloc(u8, file_size);
        const bytes_read = try file.read(content);
        if (bytes_read != file_size) {
            allocator.free(content);
            return error.IncompleteRead;
        }

        return content;
    }

    /// Create directory iterator
    pub fn iterate_directory(self: *VFS, path: []const u8) !DirectoryIterator {
        const entries = try self.list_dir(path, std.heap.page_allocator);
        return DirectoryIterator{
            .entries = entries,
            .index = 0,
            .allocator = std.heap.page_allocator,
            .vfs = self,
            .base_path = path,
        };
    }
};

/// Virtual File handle.
/// Internal VFile implementation with vtable interface.
/// Users should not interact with this directly - use VFile instead.
pub const VFileImpl = struct {
    ptr: *anyopaque,
    vtable: *const VTable,

    pub const VTable = struct {
        read: *const fn (ptr: *anyopaque, buffer: []u8) anyerror!usize,
        write: *const fn (ptr: *anyopaque, data: []const u8) anyerror!usize,
        seek: *const fn (ptr: *anyopaque, offset: i64, whence: SeekFrom) anyerror!u64,
        tell: *const fn (ptr: *anyopaque) anyerror!u64,
        flush: *const fn (ptr: *anyopaque) anyerror!void,
        close: *const fn (ptr: *anyopaque) anyerror!void,
        file_size: *const fn (ptr: *anyopaque) anyerror!u64,
    };

    pub const SeekFrom = enum {
        start,
        current,
        end,
    };

    pub fn read(self: *VFileImpl, buffer: []u8) anyerror!usize {
        return self.vtable.read(self.ptr, buffer);
    }

    pub fn write(self: *VFileImpl, data: []const u8) anyerror!usize {
        return self.vtable.write(self.ptr, data);
    }

    pub fn seek(self: *VFileImpl, offset: i64, whence: SeekFrom) anyerror!u64 {
        return self.vtable.seek(self.ptr, offset, whence);
    }

    pub fn tell(self: *VFileImpl) anyerror!u64 {
        return self.vtable.tell(self.ptr);
    }

    pub fn flush(self: *VFileImpl) anyerror!void {
        return self.vtable.flush(self.ptr);
    }

    pub fn close_impl(self: *VFileImpl) anyerror!void {
        return self.vtable.close(self.ptr);
    }

    pub fn file_size(self: *VFileImpl) anyerror!u64 {
        return self.vtable.file_size(self.ptr);
    }
};

/// Ergonomic file handle with RAII resource management.
/// This is the public API that users should interact with.
pub const VFile = struct {
    impl: *VFileImpl,
    closed: bool,

    pub const SeekFrom = VFileImpl.SeekFrom;

    /// Create VFile from internal implementation.
    /// Asserts file pointer is valid following defensive programming principles.
    pub fn init(impl: *VFileImpl) VFile {
        assert(@intFromPtr(impl) >= 0x1000); // Basic pointer sanity check
        return VFile{
            .impl = impl,
            .closed = false,
        };
    }

    /// Explicit close with proper error handling.
    /// Unlike defer patterns, this allows caller to handle close errors appropriately.
    pub fn close(self: *VFile) !void {
        if (self.closed) return;

        try self.impl.close_impl();
        self.closed = true;
    }

    /// Force close for cleanup scenarios where error handling isn't possible.
    /// Logs errors instead of silently swallowing them.
    pub fn force_close(self: *VFile) void {
        if (self.closed) return;

        self.impl.close_impl() catch |err| {
            std.log.err("Failed to close file: {any}", .{err});
        };
        self.closed = true;
    }

    /// Ergonomic wrapper methods with built-in safety checks
    pub fn read(self: *VFile, buffer: []u8) !usize {
        assert(!self.closed);
        return self.impl.read(buffer);
    }

    pub fn write(self: *VFile, data: []const u8) !usize {
        assert(!self.closed);
        return self.impl.write(data);
    }

    pub fn seek(self: *VFile, offset: i64, whence: SeekFrom) !u64 {
        assert(!self.closed);
        return self.impl.seek(offset, whence);
    }

    pub fn tell(self: *VFile) !u64 {
        assert(!self.closed);
        return self.impl.tell();
    }

    pub fn flush(self: *VFile) !void {
        assert(!self.closed);
        return self.impl.flush();
    }

    pub fn file_size(self: *VFile) !u64 {
        assert(!self.closed);
        return self.impl.file_size();
    }
};

/// Production VFS implementation that uses real OS file system calls.
pub const ProductionVFS = struct {
    allocator: std.mem.Allocator,

    const Self = @This();

    pub fn init(allocator: std.mem.Allocator) Self {
        return Self{
            .allocator = allocator,
        };
    }

    pub fn vfs(self: *Self) VFS {
        return VFS{
            .ptr = self,
            .vtable = &vtable,
        };
    }

    const vtable = VFS.VTable{
        .open = open,
        .create = create,
        .remove = remove,
        .exists = exists,
        .mkdir = mkdir,
        .rmdir = rmdir,
        .list_dir = list_dir,
        .rename = rename,
        .stat = stat,
        .sync = sync,
        .deinit = deinit,
    };

    fn open(ptr: *anyopaque, path: []const u8, mode: VFS.OpenMode) anyerror!*VFileImpl {
        const self: *Self = @ptrCast(@alignCast(ptr));
        const flags = switch (mode) {
            .read => std.fs.File.OpenFlags{ .mode = .read_only },
            .write => std.fs.File.OpenFlags{ .mode = .write_only },
            .read_write => std.fs.File.OpenFlags{ .mode = .read_write },
        };

        const file = try std.fs.cwd().openFile(path, flags);
        const file_wrapper = try self.allocator.create(ProductionFile);
        file_wrapper.* = ProductionFile{
            .magic = PRODUCTION_FILE_MAGIC,
            .file = file,
            .allocator = self.allocator,
            .closed = false,
        };

        // Validate allocation immediately
        assert(file_wrapper.magic == PRODUCTION_FILE_MAGIC);

        const vfile = try self.allocator.create(VFileImpl);
        vfile.* = VFileImpl{
            .ptr = file_wrapper,
            .vtable = &ProductionFile.vtable,
        };
        return vfile;
    }

    fn create(ptr: *anyopaque, path: []const u8) anyerror!*VFileImpl {
        const self: *Self = @ptrCast(@alignCast(ptr));
        const file = try std.fs.cwd().createFile(path, .{});
        const file_wrapper = try self.allocator.create(ProductionFile);
        file_wrapper.* = ProductionFile{
            .magic = PRODUCTION_FILE_MAGIC,
            .file = file,
            .allocator = self.allocator,
            .closed = false,
        };

        // Validate allocation immediately
        assert(file_wrapper.magic == PRODUCTION_FILE_MAGIC);

        const vfile = try self.allocator.create(VFileImpl);
        vfile.* = VFileImpl{
            .ptr = file_wrapper,
            .vtable = &ProductionFile.vtable,
        };
        return vfile;
    }

    fn remove(ptr: *anyopaque, path: []const u8) anyerror!void {
        _ = ptr;
        try std.fs.cwd().deleteFile(path);
    }

    fn exists(ptr: *anyopaque, path: []const u8) bool {
        _ = ptr;
        std.fs.cwd().access(path, .{}) catch return false;
        return true;
    }

    fn mkdir(ptr: *anyopaque, path: []const u8) anyerror!void {
        _ = ptr;
        try std.fs.cwd().makeDir(path);
    }

    fn rmdir(ptr: *anyopaque, path: []const u8) anyerror!void {
        _ = ptr;
        try std.fs.cwd().deleteDir(path);
    }

    fn list_dir(
        ptr: *anyopaque,
        path: []const u8,
        allocator: std.mem.Allocator,
    ) anyerror![][]const u8 {
        _ = ptr;
        var dir = try std.fs.cwd().openDir(path, .{ .iterate = true });
        defer dir.close();

        var entries = std.ArrayList([]const u8).init(allocator);
        defer entries.deinit();

        var iterator = dir.iterate();
        while (try iterator.next()) |entry| {
            const name = try allocator.dupe(u8, entry.name);
            try entries.append(name);
        }

        return entries.toOwnedSlice();
    }

    fn rename(ptr: *anyopaque, old_path: []const u8, new_path: []const u8) anyerror!void {
        _ = ptr;
        try std.fs.cwd().rename(old_path, new_path);
    }

    fn stat(ptr: *anyopaque, path: []const u8) anyerror!VFS.FileStat {
        _ = ptr;
        const file_stat = try std.fs.cwd().statFile(path);
        return VFS.FileStat{
            .size = file_stat.size,
            .created_time = @intCast(file_stat.ctime),
            .modified_time = @intCast(file_stat.mtime),
            .is_directory = file_stat.kind == .directory,
        };
    }

    fn sync(ptr: *anyopaque) anyerror!void {
        _ = ptr;
        // For production VFS, sync() is a no-op as individual files handle their own syncing
    }

    fn deinit(ptr: *anyopaque) void {
        _ = ptr;
        // Nothing to clean up for production VFS
    }
};

/// Production file wrapper.
const ProductionFile = struct {
    magic: u64,
    file: std.fs.File,
    allocator: std.mem.Allocator,
    closed: bool,

    const Self = @This();

    const vtable = VFileImpl.VTable{
        .read = read,
        .write = write,
        .seek = seek,
        .tell = tell,
        .flush = flush,
        .close = close,
        .file_size = file_size,
    };

    fn read(ptr: *anyopaque, buffer: []u8) anyerror!usize {
        assert(@intFromPtr(ptr) >= 0x1000); // Basic pointer sanity check
        const self: *Self = @ptrCast(@alignCast(ptr));
        assert(self.magic == PRODUCTION_FILE_MAGIC); // Corruption check
        assert(!self.closed); // Double-use check
        return self.file.read(buffer);
    }

    fn write(ptr: *anyopaque, data: []const u8) anyerror!usize {
        assert(@intFromPtr(ptr) >= 0x1000); // Basic pointer sanity check
        const self: *Self = @ptrCast(@alignCast(ptr));
        assert(self.magic == PRODUCTION_FILE_MAGIC); // Corruption check
        assert(!self.closed); // Double-use check
        return self.file.write(data);
    }

    fn seek(ptr: *anyopaque, offset: i64, whence: VFile.SeekFrom) anyerror!u64 {
        assert(@intFromPtr(ptr) >= 0x1000); // Basic pointer sanity check
        const self: *Self = @ptrCast(@alignCast(ptr));
        assert(self.magic == PRODUCTION_FILE_MAGIC); // Corruption check
        assert(!self.closed); // Double-use check
        const new_pos = switch (whence) {
            .start => @as(u64, @intCast(offset)),
            .current => try self.file.getPos() + @as(u64, @intCast(offset)),
            .end => try self.file.getEndPos() + @as(u64, @intCast(offset)),
        };
        try self.file.seekTo(new_pos);
        return new_pos;
    }

    fn tell(ptr: *anyopaque) anyerror!u64 {
        assert(@intFromPtr(ptr) >= 0x1000); // Basic pointer sanity check
        const self: *Self = @ptrCast(@alignCast(ptr));
        assert(self.magic == PRODUCTION_FILE_MAGIC); // Corruption check
        assert(!self.closed); // Double-use check
        return self.file.getPos();
    }

    fn flush(ptr: *anyopaque) anyerror!void {
        assert(@intFromPtr(ptr) >= 0x1000); // Basic pointer sanity check
        const self: *Self = @ptrCast(@alignCast(ptr));
        assert(self.magic == PRODUCTION_FILE_MAGIC); // Corruption check
        assert(!self.closed); // Double-use check
        return self.file.sync();
    }

    fn close(ptr: *anyopaque) anyerror!void {
        assert(@intFromPtr(ptr) >= 0x1000); // Basic pointer sanity check
        const self: *Self = @ptrCast(@alignCast(ptr));
        assert(self.magic == PRODUCTION_FILE_MAGIC); // Corruption check

        // Prevent double-close
        if (self.closed) {
            return;
        }

        self.closed = true;
        self.magic = 0xDEADDEAD; // Poison the magic number
        self.file.close();
        self.allocator.destroy(self);
    }

    fn file_size(ptr: *anyopaque) anyerror!u64 {
        assert(@intFromPtr(ptr) >= 0x1000); // Basic pointer sanity check
        const self: *Self = @ptrCast(@alignCast(ptr));
        assert(self.magic == PRODUCTION_FILE_MAGIC); // Corruption check
        assert(!self.closed); // Double-use check
        const stat = try self.file.stat();
        return stat.size;
    }
};

test "production vfs basic operations" {
    const allocator = std.testing.allocator;

    var prod_vfs = ProductionVFS.init(allocator);
    var vfs_interface = prod_vfs.vfs();

    const test_file = "/tmp/cortexdb_test_file";
    const test_data = "Hello, CortexDB!";

    // Clean up any existing test file
    if (vfs_interface.exists(test_file)) {
        try vfs_interface.remove(test_file);
    }

    // Create and write to file
    var file = try vfs_interface.create(test_file);
    defer file.force_close();

    const written = try file.write(test_data);
    try std.testing.expect(written == test_data.len);

    try file.flush();
    // Remove explicit close - defer will handle it

    // Verify file exists
    try std.testing.expect(vfs_interface.exists(test_file));

    // Read back the data
    var read_file = try vfs_interface.open(test_file, .read);
    defer read_file.force_close();

    var buffer: [100]u8 = undefined;
    const read_bytes = try read_file.read(&buffer);
    try std.testing.expect(read_bytes == test_data.len);
    try std.testing.expect(std.mem.eql(u8, buffer[0..read_bytes], test_data));

    // Clean up
    try vfs_interface.remove(test_file);
    try std.testing.expect(!vfs_interface.exists(test_file));
}
