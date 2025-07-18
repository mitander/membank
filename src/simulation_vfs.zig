//! Simulation VFS implementation for deterministic testing.
//!
//! Provides an in-memory file system that behaves identically
//! across test runs when given the same seed.

const std = @import("std");
const assert = std.debug.assert;
const vfs = @import("vfs.zig");
const VFS = vfs.VFS;
const VFile = vfs.VFile;

/// Simulation VFS that stores files in memory.
pub const SimulationVFS = struct {
    allocator: std.mem.Allocator,
    files: std.StringHashMap(FileData),
    directories: std.StringHashMap(void),
    next_file_id: u64,
    current_time: u64,

    const Self = @This();

    const FileData = struct {
        content: std.ArrayList(u8),
        created_time: u64,
        modified_time: u64,
        is_directory: bool,
    };

    pub fn init(allocator: std.mem.Allocator) Self {
        return Self{
            .allocator = allocator,
            .files = std.StringHashMap(FileData).init(allocator),
            .directories = std.StringHashMap(void).init(allocator),
            .next_file_id = 1,
            .current_time = 1640995200, // 2022-01-01 00:00:00 UTC
        };
    }

    pub fn deinit(self: *Self) void {
        var iterator = self.files.iterator();
        while (iterator.next()) |entry| {
            entry.value_ptr.content.deinit();
            self.allocator.free(entry.key_ptr.*);
        }
        self.files.deinit();

        var dir_iterator = self.directories.iterator();
        while (dir_iterator.next()) |entry| {
            self.allocator.free(entry.key_ptr.*);
        }
        self.directories.deinit();
    }

    pub fn vfs(self: *Self) VFS {
        return VFS{
            .ptr = self,
            .vtable = &vtable,
        };
    }

    /// Advance the simulation time by the given number of seconds.
    pub fn advance_time(self: *Self, seconds: u64) void {
        self.current_time += seconds;
    }

    /// Get the current state of the filesystem as a sorted list of paths and their contents.
    /// Useful for comparing filesystem states in tests.
    pub fn get_state(self: *Self, allocator: std.mem.Allocator) ![]FileState {
        var states = std.ArrayList(FileState).init(allocator);
        defer states.deinit();

        var iterator = self.files.iterator();
        while (iterator.next()) |entry| {
            const content = if (entry.value_ptr.is_directory)
                null
            else
                try allocator.dupe(u8, entry.value_ptr.content.items);

            try states.append(FileState{
                .path = try allocator.dupe(u8, entry.key_ptr.*),
                .content = content,
                .is_directory = entry.value_ptr.is_directory,
                .size = if (entry.value_ptr.is_directory) 0 else entry.value_ptr.content.items.len,
                .created_time = entry.value_ptr.created_time,
                .modified_time = entry.value_ptr.modified_time,
            });
        }

        const result = try states.toOwnedSlice();
        std.sort.heap(FileState, result, {}, compare_file_states);
        return result;
    }

    pub const FileState = struct {
        path: []const u8,
        content: ?[]const u8,
        is_directory: bool,
        size: u64,
        created_time: u64,
        modified_time: u64,
    };

    fn compare_file_states(context: void, a: FileState, b: FileState) bool {
        _ = context;
        return std.mem.order(u8, a.path, b.path) == .lt;
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
        .deinit = deinit_vfs,
    };

    fn open(ptr: *anyopaque, path: []const u8, mode: VFS.OpenMode) anyerror!VFile {
        const self: *Self = @ptrCast(@alignCast(ptr));

        if (!self.files.contains(path)) {
            return error.FileNotFound;
        }

        const file_data = self.files.getPtr(path).?;
        if (file_data.is_directory) {
            return error.IsDir;
        }

        const file_wrapper = try self.allocator.create(SimulationFile);
        file_wrapper.* = SimulationFile{
            .data = file_data,
            .position = 0,
            .mode = mode,
            .allocator = self.allocator,
        };

        return VFile{
            .ptr = file_wrapper,
            .vtable = &SimulationFile.vtable,
        };
    }

    fn create(ptr: *anyopaque, path: []const u8) anyerror!VFile {
        const self: *Self = @ptrCast(@alignCast(ptr));

        // Check if parent directory exists
        if (std.fs.path.dirname(path)) |parent| {
            if (!self.directories.contains(parent) and !std.mem.eql(u8, parent, ".")) {
                return error.ParentNotFound;
            }
        }

        const path_copy = try self.allocator.dupe(u8, path);
        const file_data = FileData{
            .content = std.ArrayList(u8).init(self.allocator),
            .created_time = self.current_time,
            .modified_time = self.current_time,
            .is_directory = false,
        };

        try self.files.put(path_copy, file_data);

        const file_wrapper = try self.allocator.create(SimulationFile);
        file_wrapper.* = SimulationFile{
            .data = self.files.getPtr(path_copy).?,
            .position = 0,
            .mode = .read_write,
            .allocator = self.allocator,
        };

        return VFile{
            .ptr = file_wrapper,
            .vtable = &SimulationFile.vtable,
        };
    }

    fn remove(ptr: *anyopaque, path: []const u8) anyerror!void {
        const self: *Self = @ptrCast(@alignCast(ptr));

        if (self.files.fetchRemove(path)) |kv| {
            kv.value.content.deinit();
            self.allocator.free(kv.key);
        } else {
            return error.FileNotFound;
        }
    }

    fn exists(ptr: *anyopaque, path: []const u8) bool {
        const self: *Self = @ptrCast(@alignCast(ptr));
        return self.files.contains(path) or self.directories.contains(path);
    }

    fn mkdir(ptr: *anyopaque, path: []const u8) anyerror!void {
        const self: *Self = @ptrCast(@alignCast(ptr));

        if (self.files.contains(path) or self.directories.contains(path)) {
            return error.PathAlreadyExists;
        }

        const path_copy = try self.allocator.dupe(u8, path);
        try self.directories.put(path_copy, {});

        // Also add to files map as a directory
        const file_data = FileData{
            .content = std.ArrayList(u8).init(self.allocator),
            .created_time = self.current_time,
            .modified_time = self.current_time,
            .is_directory = true,
        };
        try self.files.put(path_copy, file_data);
    }

    fn rmdir(ptr: *anyopaque, path: []const u8) anyerror!void {
        const self: *Self = @ptrCast(@alignCast(ptr));

        if (!self.directories.contains(path)) {
            return error.DirNotFound;
        }

        // Check if directory is empty
        var iterator = self.files.iterator();
        while (iterator.next()) |entry| {
            if (std.mem.startsWith(u8, entry.key_ptr.*, path) and
                entry.key_ptr.*.len > path.len)
            {
                return error.DirNotEmpty;
            }
        }

        if (self.directories.fetchRemove(path)) |kv| {
            self.allocator.free(kv.key);
        }

        if (self.files.fetchRemove(path)) |kv| {
            kv.value.content.deinit();
            self.allocator.free(kv.key);
        }
    }

    fn list_dir(ptr: *anyopaque, path: []const u8, allocator: std.mem.Allocator) anyerror![][]const u8 {
        const self: *Self = @ptrCast(@alignCast(ptr));

        if (!self.directories.contains(path)) {
            return error.NotDir;
        }

        var entries = std.ArrayList([]const u8).init(allocator);
        defer entries.deinit();

        var iterator = self.files.iterator();
        while (iterator.next()) |entry| {
            const file_path = entry.key_ptr.*;
            if (std.mem.startsWith(u8, file_path, path) and
                file_path.len > path.len)
            {
                const relative_path = file_path[path.len + 1 ..];
                if (std.mem.indexOf(u8, relative_path, "/") == null) {
                    // This is a direct child
                    const name = try allocator.dupe(u8, relative_path);
                    try entries.append(name);
                }
            }
        }

        return entries.toOwnedSlice();
    }

    fn rename(ptr: *anyopaque, old_path: []const u8, new_path: []const u8) anyerror!void {
        const self: *Self = @ptrCast(@alignCast(ptr));

        if (!self.files.contains(old_path)) {
            return error.FileNotFound;
        }

        if (self.files.contains(new_path)) {
            return error.PathAlreadyExists;
        }

        const file_data = self.files.get(old_path).?;
        const new_path_copy = try self.allocator.dupe(u8, new_path);

        try self.files.put(new_path_copy, file_data);

        if (self.files.fetchRemove(old_path)) |kv| {
            self.allocator.free(kv.key);
        }
    }

    fn stat(ptr: *anyopaque, path: []const u8) anyerror!VFS.FileStat {
        const self: *Self = @ptrCast(@alignCast(ptr));

        if (self.files.get(path)) |file_data| {
            return VFS.FileStat{
                .size = if (file_data.is_directory) 0 else file_data.content.items.len,
                .created_time = file_data.created_time,
                .modified_time = file_data.modified_time,
                .is_directory = file_data.is_directory,
            };
        }

        return error.FileNotFound;
    }

    fn sync(ptr: *anyopaque) anyerror!void {
        _ = ptr;
        // For simulation VFS, sync is a no-op as everything is in memory
    }

    fn deinit_vfs(ptr: *anyopaque) void {
        const self: *Self = @ptrCast(@alignCast(ptr));
        self.deinit();
    }
};

/// Simulation file handle.
const SimulationFile = struct {
    data: *SimulationVFS.FileData,
    position: usize,
    mode: VFS.OpenMode,
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

        if (self.mode == .write) {
            return error.BadFileDescriptor;
        }

        const content = self.data.content.items;
        const remaining = if (self.position < content.len)
            content.len - self.position
        else
            0;

        const to_read = @min(buffer.len, remaining);
        if (to_read > 0) {
            @memcpy(buffer[0..to_read], content[self.position .. self.position + to_read]);
            self.position += to_read;
        }

        return to_read;
    }

    fn write(ptr: *anyopaque, data: []const u8) anyerror!usize {
        const self: *Self = @ptrCast(@alignCast(ptr));

        if (self.mode == .read) {
            return error.BadFileDescriptor;
        }

        // Extend content if necessary
        const needed_size = self.position + data.len;
        if (needed_size > self.data.content.items.len) {
            try self.data.content.resize(needed_size);
        }

        @memcpy(self.data.content.items[self.position .. self.position + data.len], data);
        self.position += data.len;

        return data.len;
    }

    fn seek(ptr: *anyopaque, offset: i64, whence: VFile.SeekFrom) anyerror!u64 {
        const self: *Self = @ptrCast(@alignCast(ptr));

        const new_position = switch (whence) {
            .start => offset,
            .current => @as(i64, @intCast(self.position)) + offset,
            .end => @as(i64, @intCast(self.data.content.items.len)) + offset,
        };

        if (new_position < 0) {
            return error.InvalidSeek;
        }

        self.position = @intCast(new_position);
        return @intCast(new_position);
    }

    fn tell(ptr: *anyopaque) anyerror!u64 {
        const self: *Self = @ptrCast(@alignCast(ptr));
        return @intCast(self.position);
    }

    fn flush(ptr: *anyopaque) anyerror!void {
        _ = ptr;
        // For simulation files, flush is a no-op as everything is in memory
    }

    fn close(ptr: *anyopaque) anyerror!void {
        const self: *Self = @ptrCast(@alignCast(ptr));
        self.allocator.destroy(self);
    }

    fn get_size(ptr: *anyopaque) anyerror!u64 {
        const self: *Self = @ptrCast(@alignCast(ptr));
        return @intCast(self.data.content.items.len);
    }
};

test "simulation vfs basic operations" {
    const allocator = std.testing.allocator;

    var sim_vfs = SimulationVFS.init(allocator);
    defer sim_vfs.deinit();

    var vfs_interface = sim_vfs.vfs();

    const test_file = "test_file.txt";
    const test_data = "Hello, Simulation!";

    // Create and write to file
    var file = try vfs_interface.create(test_file);
    defer file.close() catch {};

    const written = try file.write(test_data);
    try std.testing.expect(written == test_data.len);

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

    // Test file stat
    const stat = try vfs_interface.stat(test_file);
    try std.testing.expect(stat.size == test_data.len);
    try std.testing.expect(!stat.is_directory);

    // Test directory operations
    const test_dir = "test_dir";
    try vfs_interface.mkdir(test_dir);
    try std.testing.expect(vfs_interface.exists(test_dir));

    const dir_stat = try vfs_interface.stat(test_dir);
    try std.testing.expect(dir_stat.is_directory);

    // Clean up
    try vfs_interface.remove(test_file);
    try vfs_interface.rmdir(test_dir);

    try std.testing.expect(!vfs_interface.exists(test_file));
    try std.testing.expect(!vfs_interface.exists(test_dir));
}

test "simulation vfs deterministic state" {
    const allocator = std.testing.allocator;

    var sim_vfs = SimulationVFS.init(allocator);
    defer sim_vfs.deinit();

    var vfs_interface = sim_vfs.vfs();

    // Create some files and directories
    try vfs_interface.mkdir("data");

    var file1 = try vfs_interface.create("data/file1.txt");
    _ = try file1.write("content1");
    try file1.close();

    var file2 = try vfs_interface.create("data/file2.txt");
    _ = try file2.write("content2");
    try file2.close();

    // Get the filesystem state
    const state = try sim_vfs.get_state(allocator);
    defer {
        for (state) |file_state| {
            allocator.free(file_state.path);
            if (file_state.content) |content| {
                allocator.free(content);
            }
        }
        allocator.free(state);
    }

    // Verify state is sorted and complete
    try std.testing.expect(state.len == 3); // data dir + 2 files
    try std.testing.expect(std.mem.eql(u8, state[0].path, "data"));
    try std.testing.expect(state[0].is_directory);
    try std.testing.expect(std.mem.eql(u8, state[1].path, "data/file1.txt"));
    try std.testing.expect(std.mem.eql(u8, state[1].content.?, "content1"));
    try std.testing.expect(std.mem.eql(u8, state[2].path, "data/file2.txt"));
    try std.testing.expect(std.mem.eql(u8, state[2].content.?, "content2"));
}
