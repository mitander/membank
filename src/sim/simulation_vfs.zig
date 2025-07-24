//! Simulation VFS implementation for deterministic testing.
//!
//! Design rationale: Provides an in-memory filesystem that enables deterministic
//! testing by controlling all sources of non-determinism including time, I/O
//! failures, and corruption scenarios. The simulation maintains identical behavior
//! across test runs when given the same seed, crucial for reproducible testing.
//!
//! Fault injection capabilities include torn writes, read corruption, disk space
//! limits, and I/O failures with configurable probabilities. All failures are
//! deterministic based on operation count and seeded PRNG state.

const std = @import("std");
const assert = std.debug.assert;
const testing = std.testing;
const vfs_mod = @import("vfs");

const VFS = vfs_mod.VFS;
const VFile = vfs_mod.VFile;
const VFSError = vfs_mod.VFSError;
const VFileError = vfs_mod.VFileError;
const DirectoryIterator = vfs_mod.DirectoryIterator;
const DirectoryEntry = vfs_mod.DirectoryEntry;

/// Maximum path length for defensive validation across platforms
const MAX_PATH_LENGTH = 4096;

/// Simulation file magic number for corruption detection in debug builds
const SIMULATION_FILE_MAGIC: u64 = 0xFEEDFACE_DEADBEEF;

/// Maximum reasonable file size to prevent memory exhaustion attacks
const MAX_REASONABLE_FILE_SIZE: u64 = 1024 * 1024 * 1024; // 1GB

/// Default maximum disk space for simulation (unlimited if not set)
const DEFAULT_MAX_DISK_SPACE: u64 = 1024 * 1024 * 1024 * 1024; // 1TB

// Cross-platform compatibility and security validation
comptime {
    assert(MAX_PATH_LENGTH > 0);
    assert(MAX_PATH_LENGTH <= 8192);
    assert(MAX_REASONABLE_FILE_SIZE > 0);
    assert(MAX_REASONABLE_FILE_SIZE < std.math.maxInt(u64) / 2);
    assert(SIMULATION_FILE_MAGIC != 0);
    assert(SIMULATION_FILE_MAGIC != std.math.maxInt(u64));
}

/// Stable file handle for lifetime management
const FileHandle = u32;

/// File storage with stable references
const FileStorage = struct {
    data: vfs_mod.SimulationFileData,
    path: []const u8,
    handle: FileHandle,
    active: bool,
};

/// Simulation VFS providing deterministic in-memory filesystem operations
pub const SimulationVFS = struct {
    allocator: std.mem.Allocator,
    arena: std.heap.ArenaAllocator,
    files: std.StringHashMap(FileHandle),
    file_storage: std.ArrayList(FileStorage),
    next_file_id: u64,
    next_handle: FileHandle,
    current_time_ns: i64,
    fault_injection: FaultInjectionState,

    const Self = @This();

    /// File metadata and content storage for in-memory filesystem
    const FileData = vfs_mod.SimulationFileData;


    /// Fault injection configuration for deterministic failure simulation
    pub const FaultInjectionState = struct {
        prng: std.Random.DefaultPrng,
        operation_count: u64,
        max_disk_space: u64,
        used_disk_space: u64,
        torn_write_config: TornWriteConfig,
        read_corruption_config: ReadCorruptionConfig,
        io_failure_config: IoFailureConfig,

        pub const TornWriteConfig = struct {
            enabled: bool = false,
            probability_per_thousand: u32 = 0,
            min_partial_bytes: u32 = 1,
            max_completion_fraction_per_thousand: u32 = 500,
        };

        pub const ReadCorruptionConfig = struct {
            enabled: bool = false,
            bit_flip_probability_per_kb: u32 = 0,
            max_bits_per_corruption: u32 = 1,
        };

        pub const IoFailureConfig = struct {
            enabled: bool = false,
            failure_probability_per_thousand: u32 = 0,
            target_operations: OperationType = .{},

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
                .operation_count = 0,
                .max_disk_space = DEFAULT_MAX_DISK_SPACE,
                .used_disk_space = 0,
                .torn_write_config = .{},
                .read_corruption_config = .{},
                .io_failure_config = .{},
            };
        }

        /// Check if operation should fail based on current configuration
        pub fn should_fail_operation(self: *FaultInjectionState, operation_type: IoFailureConfig.OperationType) bool {
            if (!self.io_failure_config.enabled) return false;

            const target_ops = self.io_failure_config.target_operations;
            const should_target = (operation_type.read and target_ops.read) or
                (operation_type.write and target_ops.write) or
                (operation_type.create and target_ops.create) or
                (operation_type.remove and target_ops.remove) or
                (operation_type.mkdir and target_ops.mkdir) or
                (operation_type.sync and target_ops.sync);

            if (!should_target) return false;

            self.operation_count += 1;
            const random_value = self.prng.random().uintLessThan(u32, 1000);
            return random_value < self.io_failure_config.failure_probability_per_thousand;
        }

        /// Check if write should be torn (partial) based on configuration
        pub fn should_torn_write(self: *FaultInjectionState, write_size: usize) ?usize {
            if (!self.torn_write_config.enabled) return null;

            self.operation_count += 1;
            const random_value = self.prng.random().uintLessThan(u32, 1000);
            if (random_value >= self.torn_write_config.probability_per_thousand) return null;

            const min_bytes = @min(write_size, self.torn_write_config.min_partial_bytes);
            const completion_fraction = self.prng.random().uintLessThan(u32, self.torn_write_config.max_completion_fraction_per_thousand);
            const partial_size = @max(min_bytes, (write_size * completion_fraction) / 1000);
            return @min(partial_size, write_size);
        }

        /// Apply read corruption to buffer based on configuration
        pub fn apply_read_corruption(self: *FaultInjectionState, buffer: []u8) void {
            if (!self.read_corruption_config.enabled or buffer.len == 0) return;

            const kb_count = @max(1, buffer.len / 1024);
            for (0..kb_count) |_| {
                const should_corrupt = self.prng.random().uintLessThan(u32, 1000);
                if (should_corrupt < self.read_corruption_config.bit_flip_probability_per_kb) {
                    const bits_to_flip = self.prng.random().uintLessThan(u32, self.read_corruption_config.max_bits_per_corruption) + 1;
                    for (0..bits_to_flip) |_| {
                        const byte_index = self.prng.random().uintLessThan(usize, buffer.len);
                        const bit_index = @as(u3, @intCast(self.prng.random().uintLessThan(u4, 8)));
                        buffer[byte_index] ^= (@as(u8, 1) << bit_index);
                    }
                }
            }
        }

        /// Check if write would exceed disk space limit
        pub fn check_disk_space(self: *const FaultInjectionState, additional_bytes: usize) bool {
            return self.used_disk_space + additional_bytes <= self.max_disk_space;
        }

        /// Update disk usage tracking
        pub fn update_disk_usage(self: *FaultInjectionState, old_size: usize, new_size: usize) void {
            self.used_disk_space = self.used_disk_space - old_size + new_size;
        }
    };

    pub fn init(allocator: std.mem.Allocator) !SimulationVFS {
        return init_with_fault_seed(allocator, 0);
    }

    pub fn init_with_fault_seed(allocator: std.mem.Allocator, seed: u64) !SimulationVFS {
        const arena = std.heap.ArenaAllocator.init(allocator);
        return SimulationVFS{
            .allocator = allocator,
            .arena = arena,
            .files = std.StringHashMap(FileHandle).init(allocator),
            .file_storage = std.ArrayList(FileStorage).init(allocator),
            .next_file_id = 1,
            .next_handle = 1,
            .current_time_ns = 1_700_000_000_000_000_000, // Fixed epoch for determinism
            .fault_injection = FaultInjectionState.init(seed),
        };
    }

    /// Enable torn write simulation with specified parameters
    pub fn enable_torn_writes(self: *SimulationVFS, probability_per_thousand: u32, min_partial_bytes: u32, max_completion_fraction_per_thousand: u32) void {
        assert(probability_per_thousand <= 1000);
        assert(max_completion_fraction_per_thousand <= 1000);
        assert(min_partial_bytes > 0);

        self.fault_injection.torn_write_config = .{
            .enabled = true,
            .probability_per_thousand = probability_per_thousand,
            .min_partial_bytes = min_partial_bytes,
            .max_completion_fraction_per_thousand = max_completion_fraction_per_thousand,
        };
    }

    /// Enable read corruption simulation with specified parameters
    pub fn enable_read_corruption(self: *SimulationVFS, bit_flip_probability_per_kb: u32, max_bits_per_corruption: u32) void {
        assert(bit_flip_probability_per_kb <= 1000);
        assert(max_bits_per_corruption > 0 and max_bits_per_corruption <= 8);

        self.fault_injection.read_corruption_config = .{
            .enabled = true,
            .bit_flip_probability_per_kb = bit_flip_probability_per_kb,
            .max_bits_per_corruption = max_bits_per_corruption,
        };
    }

    /// Enable I/O failure simulation with specified parameters
    pub fn enable_io_failures(self: *SimulationVFS, failure_probability_per_thousand: u32, target_operations: FaultInjectionState.IoFailureConfig.OperationType) void {
        assert(failure_probability_per_thousand <= 1000);

        self.fault_injection.io_failure_config = .{
            .enabled = true,
            .failure_probability_per_thousand = failure_probability_per_thousand,
            .target_operations = target_operations,
        };
    }

    /// Configure disk space limits for testing disk full scenarios
    pub fn configure_disk_space_limit(self: *SimulationVFS, max_bytes: u64) void {
        assert(max_bytes > 0);
        assert(max_bytes <= DEFAULT_MAX_DISK_SPACE);

        self.fault_injection.max_disk_space = max_bytes;
        // Recalculate current usage
        self.fault_injection.used_disk_space = 0;
        for (self.file_storage.items) |storage| {
            if (storage.active) {
                self.fault_injection.used_disk_space += storage.data.content.items.len;
            }
        }
    }

    /// Get stable pointer to file data by handle
    fn get_file_data(self: *SimulationVFS, handle: FileHandle) ?*vfs_mod.SimulationFileData {
        for (self.file_storage.items) |*storage| {
            if (storage.handle == handle and storage.active) {
                return &storage.data;
            }
        }
        return null;
    }

    /// Function pointer wrapper for VFile
    fn get_file_data_fn(vfs_ptr: *anyopaque, handle: u32) ?*vfs_mod.SimulationFileData {
        const self: *SimulationVFS = @ptrCast(@alignCast(vfs_ptr));
        return self.get_file_data(handle);
    }

    /// Create new file storage entry
    fn create_file_storage(self: *SimulationVFS, path: []const u8, data: vfs_mod.SimulationFileData) !FileHandle {
        const handle = self.next_handle;
        self.next_handle += 1;
        
        const path_copy = try self.allocator.dupe(u8, path);
        
        try self.file_storage.append(FileStorage{
            .data = data,
            .path = path_copy,
            .handle = handle,
            .active = true,
        });
        
        return handle;
    }

    /// Remove file storage entry
    fn remove_file_storage(self: *SimulationVFS, handle: FileHandle) void {
        for (self.file_storage.items) |*storage| {
            if (storage.handle == handle and storage.active) {
                self.allocator.free(storage.path);
                storage.active = false;
                break;
            }
        }
    }

    pub fn deinit(self: *SimulationVFS) void {
        // Free path keys and file storage
        var file_iter = self.files.iterator();
        while (file_iter.next()) |entry| {
            self.allocator.free(entry.key_ptr.*);
        }
        self.files.deinit();
        
        // Free file storage paths
        for (self.file_storage.items) |storage| {
            if (storage.active) {
                self.allocator.free(storage.path);
            }
        }
        self.file_storage.deinit();

        // Arena handles all file content cleanup
        self.arena.deinit();
    }

    /// Get VFS interface for this implementation
    pub fn vfs(self: *Self) VFS {
        return VFS{
            .ptr = self,
            .vtable = &vtable_impl,
        };
    }

    /// Advance simulated time for testing time-based operations
    pub fn advance_time(self: *SimulationVFS, delta_ns: i64) void {
        assert(delta_ns >= 0);
        self.current_time_ns += delta_ns;
    }

    /// Export filesystem state for deterministic testing verification
    pub fn state(self: *SimulationVFS, allocator: std.mem.Allocator) ![]FileState {
        var states = std.ArrayList(FileState).init(allocator);
        errdefer {
            for (states.items) |file_state| {
                allocator.free(file_state.path);
                if (file_state.content) |content| {
                    allocator.free(content);
                }
            }
            states.deinit();
        }

        var iterator = self.files.iterator();
        while (iterator.next()) |entry| {
            const handle = entry.value_ptr.*;
            const file_data = self.get_file_data(handle) orelse continue;
            
            const path = try allocator.dupe(u8, entry.key_ptr.*);
            const content = if (!file_data.is_directory)
                try allocator.dupe(u8, file_data.content.items)
            else
                null;

            try states.append(FileState{
                .path = path,
                .content = content,
                .is_directory = file_data.is_directory,
                .size = file_data.content.items.len,
                .created_time = file_data.created_time,
                .modified_time = file_data.modified_time,
            });
        }

        const result = try states.toOwnedSlice();
        std.mem.sort(FileState, result, {}, compare_file_states);
        return result;
    }

    pub const FileState = struct {
        path: []const u8,
        content: ?[]const u8,
        is_directory: bool,
        size: usize,
        created_time: i64,
        modified_time: i64,
    };

    fn compare_file_states(context: void, a: FileState, b: FileState) bool {
        _ = context;
        return std.mem.lessThan(u8, a.path, b.path);
    }

    const vtable_impl = VFS.VTable{
        .open = open,
        .create = create,
        .remove = remove,
        .exists = exists,
        .mkdir = mkdir,
        .mkdir_all = mkdir_all,
        .rmdir = rmdir,
        .iterate_directory = iterate_directory,
        .rename = rename,
        .stat = stat,
        .sync = sync,
        .deinit = deinit_vfs,
    };

    fn open(ptr: *anyopaque, path: []const u8, mode: VFS.OpenMode) VFSError!VFile {
        const self: *Self = @ptrCast(@alignCast(ptr));
        assert(path.len > 0 and path.len < MAX_PATH_LENGTH);

        if (self.fault_injection.should_fail_operation(.{ .read = mode.can_read() })) {
            return VFSError.IoError;
        }

        const handle = self.files.get(path) orelse return VFSError.FileNotFound;
        const file_data = self.get_file_data(handle) orelse return VFSError.FileNotFound;
        
        if (file_data.is_directory) {
            return VFSError.IsDirectory;
        }

        return VFile{
            .impl = .{ .simulation = .{
                .vfs_ptr = ptr,
                .handle = handle,
                .position = 0,
                .mode = mode,
                .closed = false,
                .get_data_fn = get_file_data_fn,
            }},
        };
    }

    fn create(ptr: *anyopaque, path: []const u8) VFSError!VFile {
        const self: *Self = @ptrCast(@alignCast(ptr));
        assert(path.len > 0 and path.len < MAX_PATH_LENGTH);

        if (self.fault_injection.should_fail_operation(.{ .create = true })) {
            return VFSError.IoError;
        }

        if (self.files.contains(path)) {
            return VFSError.FileExists;
        }

        // Check disk space for new file (estimate initial overhead)
        if (!self.fault_injection.check_disk_space(1024)) {
            return VFSError.IoError; // Disk full
        }

        const file_data = FileData{
            .content = std.ArrayList(u8).init(self.arena.allocator()),
            .created_time = self.current_time_ns,
            .modified_time = self.current_time_ns,
            .is_directory = false,
        };
        
        const handle = try self.create_file_storage(path, file_data);
        
        const path_copy = try self.allocator.dupe(u8, path);
        errdefer self.allocator.free(path_copy);
        
        try self.files.put(path_copy, handle);

        return VFile{
            .impl = .{ .simulation = .{
                .vfs_ptr = ptr,
                .handle = handle,
                .position = 0,
                .mode = .write,
                .closed = false,
                .get_data_fn = get_file_data_fn,
            }},
        };
    }

    fn remove(ptr: *anyopaque, path: []const u8) VFSError!void {
        const self: *Self = @ptrCast(@alignCast(ptr));
        assert(path.len > 0 and path.len < MAX_PATH_LENGTH);

        if (self.fault_injection.should_fail_operation(.{ .remove = true })) {
            return VFSError.IoError;
        }

        const handle = self.files.get(path) orelse return VFSError.FileNotFound;
        const file_data = self.get_file_data(handle) orelse return VFSError.FileNotFound;
        
        if (file_data.is_directory) {
            return VFSError.IsDirectory;
        }

        // Update disk usage
        self.fault_injection.update_disk_usage(file_data.content.items.len, 0);

        // Remove the file (arena owns file content, we only free the path key)
        const removed_entry = self.files.fetchRemove(path).?;
        self.allocator.free(removed_entry.key);
        
        // Mark file storage as inactive
        self.remove_file_storage(handle);
    }

    fn exists(ptr: *anyopaque, path: []const u8) bool {
        const self: *Self = @ptrCast(@alignCast(ptr));
        assert(path.len > 0 and path.len < MAX_PATH_LENGTH);
        return self.files.contains(path);
    }

    fn mkdir(ptr: *anyopaque, path: []const u8) VFSError!void {
        const self: *Self = @ptrCast(@alignCast(ptr));
        assert(path.len > 0 and path.len < MAX_PATH_LENGTH);

        if (self.fault_injection.should_fail_operation(.{ .mkdir = true })) {
            return VFSError.IoError;
        }

        if (self.files.contains(path)) {
            return VFSError.FileExists;
        }

        const dir_data = FileData{
            .content = std.ArrayList(u8).init(self.arena.allocator()),
            .created_time = self.current_time_ns,
            .modified_time = self.current_time_ns,
            .is_directory = true,
        };
        
        const handle = try self.create_file_storage(path, dir_data);
        
        const path_copy = try self.allocator.dupe(u8, path);
        errdefer self.allocator.free(path_copy);
        
        try self.files.put(path_copy, handle);
    }

    fn mkdir_all(ptr: *anyopaque, path: []const u8) VFSError!void {
        const self: *Self = @ptrCast(@alignCast(ptr));
        assert(path.len > 0 and path.len < MAX_PATH_LENGTH);

        if (self.files.contains(path)) {
            const handle = self.files.get(path).?;
            const file_data = self.get_file_data(handle) orelse return VFSError.FileNotFound;
            if (file_data.is_directory) {
                return; // Success - directory already exists
            } else {
                return VFSError.FileExists;
            }
        }

        // For simulation, mkdir_all behaves the same as mkdir
        // Real implementation would create parent directories
        return mkdir(ptr, path);
    }

    fn rmdir(ptr: *anyopaque, path: []const u8) VFSError!void {
        const self: *Self = @ptrCast(@alignCast(ptr));
        assert(path.len > 0 and path.len < MAX_PATH_LENGTH);

        const handle = self.files.get(path) orelse return VFSError.FileNotFound;
        const file_data = self.get_file_data(handle) orelse return VFSError.FileNotFound;
        
        if (!file_data.is_directory) {
            return VFSError.NotDirectory;
        }

        // Check if directory is empty by looking for child entries
        var iterator = self.files.iterator();
        while (iterator.next()) |entry| {
            const file_path = entry.key_ptr.*;
            if (std.mem.startsWith(u8, file_path, path) and
                file_path.len > path.len and
                file_path[path.len] == '/')
            {
                return VFSError.DirectoryNotEmpty;
            }
        }

        // Remove the directory (arena owns file content, we only free the path key)
        const removed_entry = self.files.fetchRemove(path).?;
        self.allocator.free(removed_entry.key);
        
        // Mark file storage as inactive
        self.remove_file_storage(handle);
    }

    /// Iterate directory entries using caller-provided arena allocator.
    /// All entry names and metadata are allocated in the provided arena,
    /// enabling O(1) cleanup when the arena is reset or destroyed.
    fn iterate_directory(ptr: *anyopaque, path: []const u8, allocator: std.mem.Allocator) VFSError!DirectoryIterator {
        const self: *Self = @ptrCast(@alignCast(ptr));
        assert(path.len > 0 and path.len < MAX_PATH_LENGTH);

        const handle = self.files.get(path) orelse return VFSError.FileNotFound;
        const file_data = self.get_file_data(handle) orelse return VFSError.FileNotFound;
        
        if (!file_data.is_directory) {
            return VFSError.NotDirectory;
        }

        var entries = std.ArrayList(DirectoryEntry).init(allocator);
        errdefer entries.deinit();

        var iterator = self.files.iterator();
        while (iterator.next()) |entry| {
            const file_path = entry.key_ptr.*;
            if (std.mem.startsWith(u8, file_path, path) and
                file_path.len > path.len and
                file_path[path.len] == '/')
            {
                const relative_path = file_path[path.len + 1 ..];
                if (std.mem.indexOf(u8, relative_path, "/") == null) {
                    const child_handle = entry.value_ptr.*;
                    const child_data = self.get_file_data(child_handle) orelse continue;
                    
                    // This is a direct child - allocate name in caller's arena
                    const name = try allocator.dupe(u8, relative_path);
                    const kind = if (child_data.is_directory)
                        DirectoryEntry.Kind.directory
                    else
                        DirectoryEntry.Kind.file;

                    try entries.append(DirectoryEntry{
                        .name = name,
                        .kind = kind,
                    });
                }
            }
        }

        return DirectoryIterator{
            .entries = try entries.toOwnedSlice(),
            .index = 0,
        };
    }

    fn rename(ptr: *anyopaque, old_path: []const u8, new_path: []const u8) VFSError!void {
        const self: *Self = @ptrCast(@alignCast(ptr));
        assert(old_path.len > 0 and old_path.len < MAX_PATH_LENGTH);
        assert(new_path.len > 0 and new_path.len < MAX_PATH_LENGTH);

        if (!self.files.contains(old_path)) {
            return VFSError.FileNotFound;
        }

        if (self.files.contains(new_path)) {
            return VFSError.FileExists;
        }

        const new_path_copy = try self.allocator.dupe(u8, new_path);
        errdefer self.allocator.free(new_path_copy);

        // Move the file handle
        const removed_entry = self.files.fetchRemove(old_path).?;
        self.allocator.free(removed_entry.key);
        try self.files.put(new_path_copy, removed_entry.value);
    }

    fn stat(ptr: *anyopaque, path: []const u8) VFSError!VFS.FileStat {
        const self: *Self = @ptrCast(@alignCast(ptr));
        assert(path.len > 0 and path.len < MAX_PATH_LENGTH);

        const handle = self.files.get(path) orelse return VFSError.FileNotFound;
        const file_data = self.get_file_data(handle) orelse return VFSError.FileNotFound;

        return VFS.FileStat{
            .size = file_data.content.items.len,
            .created_time = file_data.created_time,
            .modified_time = file_data.modified_time,
            .is_directory = file_data.is_directory,
        };
    }

    fn sync(ptr: *anyopaque) VFSError!void {
        const self: *Self = @ptrCast(@alignCast(ptr));

        if (self.fault_injection.should_fail_operation(.{ .sync = true })) {
            return VFSError.IoError;
        }

        // Simulation VFS sync is a no-op since everything is already in memory
    }

    fn deinit_vfs(ptr: *anyopaque, allocator: std.mem.Allocator) void {
        const self: *Self = @ptrCast(@alignCast(ptr));
        _ = allocator;
        _ = self;
        // SimulationVFS resources are cleaned up by explicit deinit() call
    }
};

test "SimulationVFS basic file operations" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const allocator = arena.allocator();

    var sim_vfs = try SimulationVFS.init(allocator);
    defer sim_vfs.deinit();

    const vfs_interface = sim_vfs.vfs();

    // Test file creation and basic I/O
    const test_path = "test_file.txt";
    var file = try vfs_interface.create(test_path);
    defer {
        file.close();
        file.deinit();
        vfs_interface.remove(test_path) catch {};
    }

    const test_data = "Hello, SimulationVFS!";
    const bytes_written = try file.write(test_data);
    try testing.expectEqual(test_data.len, bytes_written);

    try file.flush();
    _ = try file.seek(0, .start);

    var read_buffer: [256]u8 = undefined;
    const bytes_read = try file.read(&read_buffer);
    try testing.expectEqual(test_data.len, bytes_read);
    try testing.expectEqualStrings(test_data, read_buffer[0..bytes_read]);
}

test "SimulationVFS directory operations" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const allocator = arena.allocator();

    var sim_vfs = try SimulationVFS.init(allocator);
    defer sim_vfs.deinit();

    const vfs_interface = sim_vfs.vfs();

    const test_dir = "test_dir";
    try vfs_interface.mkdir(test_dir);
    defer vfs_interface.rmdir(test_dir) catch {};

    // Test directory existence
    try testing.expect(vfs_interface.exists(test_dir));

    // Test directory iteration with arena allocator
    var iter_arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer iter_arena.deinit();
    const iter_allocator = iter_arena.allocator();

    var iterator = try vfs_interface.iterate_directory(test_dir, iter_allocator);

    // Empty directory should have no entries
    try testing.expectEqual(@as(usize, 0), iterator.remaining());
    try testing.expect(iterator.next() == null);
}

test "SimulationVFS fault injection - torn writes" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const allocator = arena.allocator();

    var sim_vfs = try SimulationVFS.init_with_fault_seed(allocator, 12345);
    defer sim_vfs.deinit();

    // Enable torn writes with 100% probability, 50% completion
    sim_vfs.enable_torn_writes(1000, 1, 500);

    const vfs_interface = sim_vfs.vfs();

    const test_data = "Hello, this is a test message for torn writes!";

    var file = try vfs_interface.create("test.txt");
    defer {
        file.close();
        file.deinit();
        vfs_interface.remove("test.txt") catch {};
    }

    // Write should be torn (only partial data written)
    const written = try file.write(test_data);

    // Verify that less data was written than requested
    try testing.expect(written < test_data.len);
    try testing.expect(written > 0); // At least some data was written

    // Read back and verify partial content
    _ = try file.seek(0, .start);
    var buffer: [100]u8 = undefined;
    const read_bytes = try file.read(&buffer);

    try testing.expectEqual(written, read_bytes);
    try testing.expectEqualStrings(test_data[0..written], buffer[0..read_bytes]);
}

test "SimulationVFS fault injection - read corruption" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const allocator = arena.allocator();

    var sim_vfs = try SimulationVFS.init_with_fault_seed(allocator, 54321);
    defer sim_vfs.deinit();

    // Enable read corruption with high probability
    sim_vfs.enable_read_corruption(1000, 3); // 100% chance per KB, up to 3 bits

    const vfs_interface = sim_vfs.vfs();

    const test_data = "A" ** 1024; // 1KB of 'A's (0x41)

    var file = try vfs_interface.create("test.txt");
    defer {
        file.close();
        file.deinit();
        vfs_interface.remove("test.txt") catch {};
    }

    _ = try file.write(test_data);
    _ = try file.seek(0, .start);

    // Read back - should have corruption
    var buffer: [1024]u8 = undefined;
    const read_bytes = try file.read(&buffer);

    try testing.expectEqual(test_data.len, read_bytes);

    // Should have some corruption (not all bytes should match)
    var corruption_detected = false;
    for (0..read_bytes) |i| {
        if (buffer[i] != test_data[i]) {
            corruption_detected = true;
            break;
        }
    }
    try testing.expect(corruption_detected);
}

test "SimulationVFS fault injection - disk full" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const allocator = arena.allocator();

    var sim_vfs = try SimulationVFS.init(allocator);
    defer sim_vfs.deinit();

    // Set a very small disk limit
    sim_vfs.configure_disk_space_limit(10);

    const vfs_interface = sim_vfs.vfs();

    const test_data = "This message is longer than 10 bytes";

    var file = try vfs_interface.create("test.txt");
    defer {
        file.close();
        file.deinit();
        vfs_interface.remove("test.txt") catch {};
    }

    // Write should fail due to disk space limit
    const result = file.write(test_data);
    try testing.expectError(VFileError.WriteError, result);
}

test "SimulationVFS fault injection - io failures" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const allocator = arena.allocator();

    var sim_vfs = try SimulationVFS.init_with_fault_seed(allocator, 99999);
    defer sim_vfs.deinit();

    // Enable 100% failure rate for create operations
    sim_vfs.enable_io_failures(1000, .{ .create = true });

    const vfs_interface = sim_vfs.vfs();

    // Create should fail
    const result = vfs_interface.create("test.txt");
    try testing.expectError(VFSError.IoError, result);
}

test "SimulationVFS deterministic behavior" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const allocator = arena.allocator();

    // Same seed should produce identical behavior
    const seed: u64 = 12345;
    var results: [2]usize = undefined;

    for (0..2) |iteration| {
        var sim_vfs = try SimulationVFS.init_with_fault_seed(allocator, seed);
        defer sim_vfs.deinit();

        sim_vfs.enable_torn_writes(500, 1, 750); // 50% chance, 75% completion

        const vfs_interface = sim_vfs.vfs();

        const test_data = "Deterministic test data for fault injection";

        var file = try vfs_interface.create("test.txt");
        defer {
            file.close();
            file.deinit();
            vfs_interface.remove("test.txt") catch {};
        }

        results[iteration] = try file.write(test_data);
    }

    // Both runs should produce identical results
    try testing.expectEqual(results[0], results[1]);
}
