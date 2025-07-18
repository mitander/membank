//! Virtual File System interface for deterministic testing.
//!
//! Provides an abstraction over file system operations that allows
//! the same code to run in production (using real OS calls) and
//! simulation (using in-memory data structures).

const std = @import("std");
const assert = std.debug.assert;

/// Virtual File System interface.
/// All file I/O operations go through this interface.
pub const VFS = struct {
    ptr: *anyopaque,
    vtable: *const VTable,

    pub const VTable = struct {
        open: *const fn (ptr: *anyopaque, path: []const u8, mode: OpenMode) anyerror!VFile,
        create: *const fn (ptr: *anyopaque, path: []const u8) anyerror!VFile,
        remove: *const fn (ptr: *anyopaque, path: []const u8) anyerror!void,
        exists: *const fn (ptr: *anyopaque, path: []const u8) bool,
        mkdir: *const fn (ptr: *anyopaque, path: []const u8) anyerror!void,
        rmdir: *const fn (ptr: *anyopaque, path: []const u8) anyerror!void,
        list_dir: *const fn (ptr: *anyopaque, path: []const u8, allocator: std.mem.Allocator) anyerror![][]const u8,
        rename: *const fn (ptr: *anyopaque, old_path: []const u8, new_path: []const u8) anyerror!void,
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
        return self.vtable.open(self.ptr, path, mode);
    }

    pub fn create(self: *VFS, path: []const u8) anyerror!VFile {
        return self.vtable.create(self.ptr, path);
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

    pub fn list_dir(self: *VFS, path: []const u8, allocator: std.mem.Allocator) anyerror![][]const u8 {
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
};

/// Virtual File handle.
pub const VFile = struct {
    ptr: *anyopaque,
    vtable: *const VTable,

    pub const VTable = struct {
        read: *const fn (ptr: *anyopaque, buffer: []u8) anyerror!usize,
        write: *const fn (ptr: *anyopaque, data: []const u8) anyerror!usize,
        seek: *const fn (ptr: *anyopaque, offset: i64, whence: SeekFrom) anyerror!u64,
        tell: *const fn (ptr: *anyopaque) anyerror!u64,
        flush: *const fn (ptr: *anyopaque) anyerror!void,
        close: *const fn (ptr: *anyopaque) anyerror!void,
        get_size: *const fn (ptr: *anyopaque) anyerror!u64,
    };

    pub const SeekFrom = enum {
        start,
        current,
        end,
    };

    pub fn read(self: *VFile, buffer: []u8) anyerror!usize {
        return self.vtable.read(self.ptr, buffer);
    }

    pub fn write(self: *VFile, data: []const u8) anyerror!usize {
        return self.vtable.write(self.ptr, data);
    }

    pub fn seek(self: *VFile, offset: i64, whence: SeekFrom) anyerror!u64 {
        return self.vtable.seek(self.ptr, offset, whence);
    }

    pub fn tell(self: *VFile) anyerror!u64 {
        return self.vtable.tell(self.ptr);
    }

    pub fn flush(self: *VFile) anyerror!void {
        return self.vtable.flush(self.ptr);
    }

    pub fn close(self: *VFile) anyerror!void {
        return self.vtable.close(self.ptr);
    }

    pub fn get_size(self: *VFile) anyerror!u64 {
        return self.vtable.get_size(self.ptr);
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

    fn open(ptr: *anyopaque, path: []const u8, mode: VFS.OpenMode) anyerror!VFile {
        const self: *Self = @ptrCast(@alignCast(ptr));
        const flags = switch (mode) {
            .read => std.fs.File.OpenFlags{ .mode = .read_only },
            .write => std.fs.File.OpenFlags{ .mode = .write_only },
            .read_write => std.fs.File.OpenFlags{ .mode = .read_write },
        };

        const file = try std.fs.cwd().openFile(path, flags);
        const file_wrapper = try self.allocator.create(ProductionFile);
        file_wrapper.* = ProductionFile{
            .file = file,
            .allocator = self.allocator,
        };

        return VFile{
            .ptr = file_wrapper,
            .vtable = &ProductionFile.vtable,
        };
    }

    fn create(ptr: *anyopaque, path: []const u8) anyerror!VFile {
        const self: *Self = @ptrCast(@alignCast(ptr));
        const file = try std.fs.cwd().createFile(path, .{});
        const file_wrapper = try self.allocator.create(ProductionFile);
        file_wrapper.* = ProductionFile{
            .file = file,
            .allocator = self.allocator,
        };

        return VFile{
            .ptr = file_wrapper,
            .vtable = &ProductionFile.vtable,
        };
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

    fn list_dir(ptr: *anyopaque, path: []const u8, allocator: std.mem.Allocator) anyerror![][]const u8 {
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
    file: std.fs.File,
    allocator: std.mem.Allocator,

    const Self = @This();

    const vtable = VFile.VTable{
        .read = read,
        .write = write,
        .seek = seek,
        .tell = tell,
        .flush = flush,
        .close = close,
        .get_size = get_size,
    };

    fn read(ptr: *anyopaque, buffer: []u8) anyerror!usize {
        const self: *Self = @ptrCast(@alignCast(ptr));
        return self.file.read(buffer);
    }

    fn write(ptr: *anyopaque, data: []const u8) anyerror!usize {
        const self: *Self = @ptrCast(@alignCast(ptr));
        return self.file.write(data);
    }

    fn seek(ptr: *anyopaque, offset: i64, whence: VFile.SeekFrom) anyerror!u64 {
        const self: *Self = @ptrCast(@alignCast(ptr));
        const new_pos = switch (whence) {
            .start => @as(u64, @intCast(offset)),
            .current => try self.file.getPos() + @as(u64, @intCast(offset)),
            .end => try self.file.getEndPos() + @as(u64, @intCast(offset)),
        };
        try self.file.seekTo(new_pos);
        return new_pos;
    }

    fn tell(ptr: *anyopaque) anyerror!u64 {
        const self: *Self = @ptrCast(@alignCast(ptr));
        return self.file.getPos();
    }

    fn flush(ptr: *anyopaque) anyerror!void {
        const self: *Self = @ptrCast(@alignCast(ptr));
        return self.file.sync();
    }

    fn close(ptr: *anyopaque) anyerror!void {
        const self: *Self = @ptrCast(@alignCast(ptr));
        self.file.close();
        self.allocator.destroy(self);
    }

    fn get_size(ptr: *anyopaque) anyerror!u64 {
        const self: *Self = @ptrCast(@alignCast(ptr));
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
    defer file.close() catch {};

    const written = try file.write(test_data);
    try std.testing.expect(written == test_data.len);

    try file.flush();
    try file.close();

    // Verify file exists
    try std.testing.expect(vfs_interface.exists(test_file));

    // Read back the data
    var read_file = try vfs_interface.open(test_file, .read);
    defer read_file.close() catch {};

    var buffer: [100]u8 = undefined;
    const read_bytes = try read_file.read(&buffer);
    try std.testing.expect(read_bytes == test_data.len);
    try std.testing.expect(std.mem.eql(u8, buffer[0..read_bytes], test_data));

    try read_file.close();

    // Clean up
    try vfs_interface.remove(test_file);
    try std.testing.expect(!vfs_interface.exists(test_file));
}
