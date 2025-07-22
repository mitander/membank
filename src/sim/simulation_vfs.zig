//! Simulation VFS implementation for deterministic testing.
//!
//! Provides an in-memory file system that behaves identically
//! across test runs when given the same seed.

const std = @import("std");
const assert = std.debug.assert;
const vfs = @import("vfs");
const VFS = vfs.VFS;
const VFile = vfs.VFile;

/// Simulation VFS that stores files in memory.
pub const SimulationVFS = struct {
    allocator: std.mem.Allocator,
    files: std.StringHashMap(FileData),
    file_handle_pool: std.ArrayList(*SimulationFile),
    next_file_id: u64,
    current_time: u64,
    fault_injection: FaultInjectionState,

    const Self = @This();

    /// Fault injection configuration for deterministic failure simulation
    pub const FaultInjectionState = struct {
        /// Random number generator seeded for deterministic behavior
        prng: std.Random.DefaultPrng,
        /// Global operation counter for deterministic fault timing
        operation_count: u64 = 0,
        /// Maximum available disk space (bytes) - simulates disk full
        max_disk_space: ?u64 = null,
        /// Current used disk space (bytes)
        used_disk_space: u64 = 0,
        /// Torn write configuration - writes only partial data
        torn_write_config: TornWriteConfig = .{},
        /// Read corruption configuration - introduces bit flips
        read_corruption_config: ReadCorruptionConfig = .{},
        /// General I/O failure configuration
        io_failure_config: IoFailureConfig = .{},

        pub const TornWriteConfig = struct {
            enabled: bool = false,
            /// Probability (0-1000) of torn write occurring (per thousand)
            probability: u32 = 0,
            /// Minimum bytes that must be written before tearing
            min_partial_bytes: u32 = 1,
            /// Maximum fraction of write that completes (0-1000 per thousand)
            max_completion_fraction: u32 = 500, // 50% by default
        };

        pub const ReadCorruptionConfig = struct {
            enabled: bool = false,
            /// Probability (0-1000) of bit corruption per 1KB read
            bit_flip_probability_per_kb: u32 = 0,
            /// Maximum number of bits to flip in a single corruption event
            max_bits_per_corruption: u32 = 1,
        };

        pub const IoFailureConfig = struct {
            enabled: bool = false,
            /// Probability (0-1000) of I/O operation failing
            failure_probability: u32 = 0,
            /// Specific operations to fail (bitmask)
            target_operations: OperationType = OperationType{},

            pub const OperationType = packed struct {
                read: bool = false,
                write: bool = false,
                create: bool = false,
                remove: bool = false,
                mkdir: bool = false,
                sync: bool = false,
                _padding: u2 = 0,
            };
        };

        pub fn init(seed: u64) FaultInjectionState {
            return FaultInjectionState{
                .prng = std.Random.DefaultPrng.init(seed),
            };
        }

        /// Check if an operation should fail based on configured probability
        pub fn should_fail_operation(self: *FaultInjectionState, op_type: IoFailureConfig.OperationType) bool {
            if (!self.io_failure_config.enabled) return false;

            const target = self.io_failure_config.target_operations;
            const matches_target = (op_type.read and target.read) or
                (op_type.write and target.write) or
                (op_type.create and target.create) or
                (op_type.remove and target.remove) or
                (op_type.mkdir and target.mkdir) or
                (op_type.sync and target.sync);

            if (!matches_target) return false;

            self.operation_count += 1;
            const roll = self.prng.random().uintLessThan(u32, 1000);
            return roll < self.io_failure_config.failure_probability;
        }

        /// Check if a write should be torn (partial)
        pub fn should_torn_write(self: *FaultInjectionState, write_size: usize) ?usize {
            if (!self.torn_write_config.enabled or write_size == 0) return null;

            self.operation_count += 1;
            const roll = self.prng.random().uintLessThan(u32, 1000);
            if (roll >= self.torn_write_config.probability) return null;

            const min_bytes = @min(self.torn_write_config.min_partial_bytes, write_size);
            const max_completion = (write_size * self.torn_write_config.max_completion_fraction) / 1000;
            const actual_completion = @max(min_bytes, max_completion);

            return @min(actual_completion, write_size);
        }

        /// Corrupt read data by flipping random bits
        pub fn apply_read_corruption(self: *FaultInjectionState, data: []u8) void {
            if (!self.read_corruption_config.enabled or data.len == 0) return;

            const kb_count = (data.len + 1023) / 1024; // Round up to KB
            for (0..kb_count) |_| {
                self.operation_count += 1;
                const roll = self.prng.random().uintLessThan(u32, 1000);
                if (roll < self.read_corruption_config.bit_flip_probability_per_kb) {
                    const bits_to_flip = self.prng.random().uintLessThan(u32, self.read_corruption_config.max_bits_per_corruption) + 1;

                    for (0..bits_to_flip) |_| {
                        const byte_idx = self.prng.random().uintLessThan(usize, data.len);
                        const bit_idx = self.prng.random().uintLessThan(u8, 8);
                        data[byte_idx] ^= (@as(u8, 1) << @as(u3, @intCast(bit_idx)));
                    }
                }
            }
        }

        /// Check if disk space is available for write
        pub fn check_disk_space(self: *FaultInjectionState, additional_bytes: u64) bool {
            if (self.max_disk_space == null) return true;
            return (self.used_disk_space + additional_bytes) <= self.max_disk_space.?;
        }

        /// Update used disk space
        pub fn update_disk_usage(self: *FaultInjectionState, delta: i64) void {
            if (delta >= 0) {
                self.used_disk_space += @intCast(delta);
            } else {
                const reduction: u64 = @intCast(-delta);
                self.used_disk_space = if (self.used_disk_space >= reduction)
                    self.used_disk_space - reduction
                else
                    0;
            }
        }
    };

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
            .file_handle_pool = std.ArrayList(*SimulationFile).init(allocator),
            .next_file_id = 1,
            .current_time = 1640995200, // 2022-01-01 00:00:00 UTC
            .fault_injection = FaultInjectionState.init(0xDEADBEEF),
        };
    }

    pub fn init_with_fault_seed(allocator: std.mem.Allocator, fault_seed: u64) Self {
        return Self{
            .allocator = allocator,
            .files = std.StringHashMap(FileData).init(allocator),
            .file_handle_pool = std.ArrayList(*SimulationFile).init(allocator),
            .next_file_id = 1,
            .current_time = 1640995200,
            .fault_injection = FaultInjectionState.init(fault_seed),
        };
    }

    /// Configure torn write simulation
    pub fn enable_torn_writes(self: *Self, probability: u32, min_partial: u32, max_completion_pct: u32) void {
        self.fault_injection.torn_write_config = .{
            .enabled = true,
            .probability = probability,
            .min_partial_bytes = min_partial,
            .max_completion_fraction = max_completion_pct * 10, // Convert percentage to per-thousand
        };
    }

    /// Configure read corruption simulation
    pub fn enable_read_corruption(self: *Self, bit_flip_prob_per_kb: u32, max_bits: u32) void {
        self.fault_injection.read_corruption_config = .{
            .enabled = true,
            .bit_flip_probability_per_kb = bit_flip_prob_per_kb,
            .max_bits_per_corruption = max_bits,
        };
    }

    /// Configure I/O failure simulation
    pub fn enable_io_failures(self: *Self, probability: u32, operations: FaultInjectionState.IoFailureConfig.OperationType) void {
        self.fault_injection.io_failure_config = .{
            .enabled = true,
            .failure_probability = probability,
            .target_operations = operations,
        };
    }

    /// Configure disk space limits
    pub fn configure_disk_space_limit(self: *Self, max_bytes: u64) void {
        self.fault_injection.max_disk_space = max_bytes;
        // Calculate current usage
        var total_usage: u64 = 0;
        var iterator = self.files.iterator();
        while (iterator.next()) |entry| {
            if (!entry.value_ptr.is_directory) {
                total_usage += entry.value_ptr.content.items.len;
            }
        }
        self.fault_injection.used_disk_space = total_usage;
    }

    pub fn deinit(self: *Self) void {
        var iterator = self.files.iterator();
        while (iterator.next()) |entry| {
            entry.value_ptr.content.deinit();
            self.allocator.free(entry.key_ptr.*);
        }
        self.files.deinit();

        // Clean up file handle pool
        for (self.file_handle_pool.items) |file_handle| {
            self.allocator.destroy(file_handle);
        }
        self.file_handle_pool.deinit();
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
    pub fn state(self: *Self, allocator: std.mem.Allocator) ![]FileState {
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
            .vfs = self,
            .closed = false,
        };
        try self.file_handle_pool.append(file_wrapper);

        return VFile{
            .ptr = file_wrapper,
            .vtable = &SimulationFile.vtable,
        };
    }

    fn create(ptr: *anyopaque, path: []const u8) anyerror!VFile {
        const self: *Self = @ptrCast(@alignCast(ptr));

        // Check for I/O failure injection
        if (self.fault_injection.should_fail_operation(.{ .create = true })) {
            return error.InputOutput;
        }

        // Check if parent directory exists
        if (std.fs.path.dirname(path)) |parent| {
            if (!std.mem.eql(u8, parent, ".")) {
                if (self.files.get(parent)) |parent_data| {
                    if (!parent_data.is_directory) {
                        return error.ParentNotFound;
                    }
                } else {
                    return error.ParentNotFound;
                }
            }
        }

        // Check if file already exists and clean it up first
        var old_size: u64 = 0;
        if (self.files.fetchRemove(path)) |existing| {
            old_size = existing.value.content.items.len;
            existing.value.content.deinit();
            self.allocator.free(existing.key);
            // Update disk usage to reflect file removal
            self.fault_injection.update_disk_usage(-@as(i64, @intCast(old_size)));
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
            .vfs = self,
            .closed = false,
        };
        try self.file_handle_pool.append(file_wrapper);

        return VFile{
            .ptr = file_wrapper,
            .vtable = &SimulationFile.vtable,
        };
    }

    fn remove(ptr: *anyopaque, path: []const u8) anyerror!void {
        const self: *Self = @ptrCast(@alignCast(ptr));

        // Check for I/O failure injection
        if (self.fault_injection.should_fail_operation(.{ .remove = true })) {
            return error.InputOutput;
        }

        if (self.files.fetchRemove(path)) |kv| {
            const old_size = kv.value.content.items.len;
            kv.value.content.deinit();
            self.allocator.free(kv.key);
            // Update disk usage to reflect file removal
            self.fault_injection.update_disk_usage(-@as(i64, @intCast(old_size)));
        } else {
            return error.FileNotFound;
        }
    }

    fn exists(ptr: *anyopaque, path: []const u8) bool {
        const self: *Self = @ptrCast(@alignCast(ptr));
        return self.files.contains(path);
    }

    fn mkdir(ptr: *anyopaque, path: []const u8) anyerror!void {
        const self: *Self = @ptrCast(@alignCast(ptr));

        // Check for I/O failure injection
        if (self.fault_injection.should_fail_operation(.{ .mkdir = true })) {
            return error.InputOutput;
        }

        if (self.files.contains(path)) {
            return error.PathAlreadyExists;
        }

        const path_copy = try self.allocator.dupe(u8, path);

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

        if (self.files.get(path)) |file_data| {
            if (!file_data.is_directory) {
                return error.NotDir;
            }
        } else {
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

        if (self.files.fetchRemove(path)) |kv| {
            kv.value.content.deinit();
            self.allocator.free(kv.key);
        }
    }

    fn list_dir(
        ptr: *anyopaque,
        path: []const u8,
        allocator: std.mem.Allocator,
    ) anyerror![][]const u8 {
        const self: *Self = @ptrCast(@alignCast(ptr));

        if (self.files.get(path)) |file_data| {
            if (!file_data.is_directory) {
                return error.NotDir;
            }
        } else {
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
        const self: *Self = @ptrCast(@alignCast(ptr));

        // Check for I/O failure injection
        if (self.fault_injection.should_fail_operation(.{ .sync = true })) {
            return error.InputOutput;
        }

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
    vfs: *SimulationVFS,
    closed: bool,

    const Self = @This();

    const vtable = VFile.VTable{
        .read = read,
        .write = write,
        .seek = seek,
        .tell = tell,
        .flush = flush,
        .close = close,
        .file_size = file_size,
    };

    fn read(ptr: *anyopaque, buffer: []u8) anyerror!usize {
        const self: *Self = @ptrCast(@alignCast(ptr));

        if (self.mode == .write) {
            return error.BadFileDescriptor;
        }

        // Check for I/O failure injection
        if (self.vfs.fault_injection.should_fail_operation(.{ .read = true })) {
            return error.InputOutput;
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

            // Apply read corruption after copying data
            self.vfs.fault_injection.apply_read_corruption(buffer[0..to_read]);
        }

        return to_read;
    }

    fn write(ptr: *anyopaque, data: []const u8) anyerror!usize {
        const self: *Self = @ptrCast(@alignCast(ptr));

        if (self.closed) {
            return error.FileClosed;
        }

        if (self.mode == .read) {
            return error.BadFileDescriptor;
        }

        // Check for I/O failure injection
        if (self.vfs.fault_injection.should_fail_operation(.{ .write = true })) {
            return error.InputOutput;
        }

        // Check for torn write injection
        var write_size = data.len;
        if (self.vfs.fault_injection.should_torn_write(data.len)) |partial_size| {
            write_size = partial_size;
        }

        // Check disk space limits
        const additional_bytes = if (self.position + write_size > self.data.content.items.len)
            (self.position + write_size) - self.data.content.items.len
        else
            0;

        if (!self.vfs.fault_injection.check_disk_space(additional_bytes)) {
            return error.NoSpaceLeft;
        }

        // Extend content if necessary
        const needed_size = self.position + write_size;
        if (needed_size > self.data.content.items.len) {
            const old_size = self.data.content.items.len;
            try self.data.content.resize(needed_size);
            // Update disk usage
            self.vfs.fault_injection.update_disk_usage(@as(i64, @intCast(needed_size - old_size)));
        }

        @memcpy(self.data.content.items[self.position .. self.position + write_size], data[0..write_size]);
        self.position += write_size;

        return write_size;
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

        if (!self.closed) {
            self.closed = true;
            // Don't destroy immediately - let VFS handle cleanup to prevent double-close segfaults
        }
    }

    fn file_size(ptr: *anyopaque) anyerror!u64 {
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
    const state = try sim_vfs.state(allocator);
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

test "fault injection - torn writes" {
    const allocator = std.testing.allocator;

    var sim_vfs = SimulationVFS.init_with_fault_seed(allocator, 12345);
    defer sim_vfs.deinit();

    // Enable torn writes with 100% probability, 50% completion
    sim_vfs.enable_torn_writes(1000, 1, 50);

    var vfs_interface = sim_vfs.vfs();

    const test_data = "Hello, this is a test message for torn writes!";

    var file = try vfs_interface.create("test.txt");
    defer file.close() catch {};

    // Write should be torn (only partial data written)
    const written = try file.write(test_data);
    try file.close();

    // Verify that less data was written than requested
    try std.testing.expect(written < test_data.len);
    try std.testing.expect(written > 0); // At least some data was written

    // Read back and verify partial content
    var read_file = try vfs_interface.open("test.txt", .read);
    defer read_file.close() catch {};

    var buffer: [100]u8 = undefined;
    const read_bytes = try read_file.read(&buffer);

    try std.testing.expect(read_bytes == written);
    try std.testing.expect(std.mem.eql(u8, buffer[0..read_bytes], test_data[0..written]));
}

test "fault injection - read corruption" {
    const allocator = std.testing.allocator;

    var sim_vfs = SimulationVFS.init_with_fault_seed(allocator, 54321);
    defer sim_vfs.deinit();

    // Enable read corruption with high probability
    sim_vfs.enable_read_corruption(1000, 3); // 100% chance per KB, up to 3 bits

    var vfs_interface = sim_vfs.vfs();

    const test_data = "A" ** 1024; // 1KB of 'A's (0x41)

    var file = try vfs_interface.create("test.txt");
    _ = try file.write(test_data);
    try file.close();

    // Read back - should have corruption
    var read_file = try vfs_interface.open("test.txt", .read);
    defer read_file.close() catch {};

    var buffer: [1024]u8 = undefined;
    const read_bytes = try read_file.read(&buffer);

    try std.testing.expect(read_bytes == test_data.len);

    // Should have some corruption (not all bytes should match)
    var corruption_detected = false;
    for (0..read_bytes) |i| {
        if (buffer[i] != test_data[i]) {
            corruption_detected = true;
            break;
        }
    }
    try std.testing.expect(corruption_detected);
}

test "fault injection - disk full" {
    const allocator = std.testing.allocator;

    var sim_vfs = SimulationVFS.init(allocator);
    defer sim_vfs.deinit();

    // Set a very small disk limit
    sim_vfs.set_disk_space_limit(10);

    var vfs_interface = sim_vfs.vfs();

    const test_data = "This message is longer than 10 bytes";

    var file = try vfs_interface.create("test.txt");
    defer file.close() catch {};

    // Write should fail due to disk space limit
    const result = file.write(test_data);
    try std.testing.expectError(error.NoSpaceLeft, result);
}

test "fault injection - io failures" {
    const allocator = std.testing.allocator;

    var sim_vfs = SimulationVFS.init_with_fault_seed(allocator, 99999);
    defer sim_vfs.deinit();

    // Enable 100% failure rate for create operations
    sim_vfs.enable_io_failures(1000, .{ .create = true });

    var vfs_interface = sim_vfs.vfs();

    // Create should fail
    const result = vfs_interface.create("test.txt");
    try std.testing.expectError(error.InputOutput, result);
}

test "fault injection deterministic behavior" {
    const allocator = std.testing.allocator;

    // Same seed should produce identical behavior
    const seed: u64 = 12345;

    var results: [2]usize = undefined;

    for (0..2) |iteration| {
        var sim_vfs = SimulationVFS.init_with_fault_seed(allocator, seed);
        defer sim_vfs.deinit();

        sim_vfs.enable_torn_writes(500, 1, 75); // 50% chance, 75% completion

        var vfs_interface = sim_vfs.vfs();

        const test_data = "Deterministic test data for fault injection";

        var file = try vfs_interface.create("test.txt");
        defer file.close() catch {};

        results[iteration] = try file.write(test_data);
        try file.close();
    }

    // Both runs should produce identical results
    try std.testing.expect(results[0] == results[1]);
}
