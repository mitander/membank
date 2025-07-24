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
const vfs = @import("vfs");

const VFS = vfs.VFS;
const VFile = vfs.VFile;
const VFSError = vfs.VFSError;
const VFileError = vfs.VFileError;
const DirectoryIterator = vfs.DirectoryIterator;
const DirectoryEntry = vfs.DirectoryEntry;

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

/// Simulation VFS providing deterministic in-memory filesystem operations
pub const SimulationVFS = struct {
    allocator: std.mem.Allocator,
    arena: std.heap.ArenaAllocator,
    files: std.StringHashMap(FileData),
    next_file_id: u64,
    current_time_ns: i64,
    fault_injection: FaultInjectionState,

    const Self = @This();

    /// File metadata and content storage for in-memory filesystem
    const FileData = struct {
        content: std.ArrayList(u8),
        created_time: i64,
        modified_time: i64,
        is_directory: bool,

        fn init(allocator: std.mem.Allocator, is_directory: bool, current_time: i64) FileData {
            return FileData{
                .content = std.ArrayList(u8).init(allocator),
                .created_time = current_time,
                .modified_time = current_time,
                .is_directory = is_directory,
            };
        }
    };

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
            .files = std.StringHashMap(FileData).init(allocator),
            .next_file_id = 1,
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
        var iterator = self.files.iterator();
        while (iterator.next()) |entry| {
            self.fault_injection.used_disk_space += entry.value_ptr.content.items.len;
        }
    }

    pub fn deinit(self: *SimulationVFS) void {
        // Free path keys (HashMap structure uses regular allocator)
        var iterator = self.files.iterator();
        while (iterator.next()) |entry| {
            self.allocator.free(entry.key_ptr.*);
        }
        self.files.deinit();

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
    pub fn state(self: *const SimulationVFS, allocator: std.mem.Allocator) ![]FileState {
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
            const path = try allocator.dupe(u8, entry.key_ptr.*);
            const content = if (!entry.value_ptr.is_directory)
                try allocator.dupe(u8, entry.value_ptr.content.items)
            else
                null;

            try states.append(FileState{
                .path = path,
                .content = content,
                .is_directory = entry.value_ptr.is_directory,
                .size = entry.value_ptr.content.items.len,
                .created_time = entry.value_ptr.created_time,
                .modified_time = entry.value_ptr.modified_time,
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

        if (!self.files.contains(path)) {
            return VFSError.FileNotFound;
        }

        const file_data = self.files.getPtr(path).?;
        if (file_data.is_directory) {
            return VFSError.IsDirectory;
        }

        const sim_file = try self.allocator.create(SimulationFile);
        sim_file.* = SimulationFile{
            .magic = SIMULATION_FILE_MAGIC,
            .data = file_data,
            .position = 0,
            .mode = mode,
            .allocator = self.allocator,
            .vfs = self,
            .closed = false,
        };

        return VFile{
            .file_impl = sim_file,
            .vtable = &SimulationFile.vtable_impl,
            .allocator = self.allocator,
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

        const path_copy = try self.allocator.dupe(u8, path);
        errdefer self.allocator.free(path_copy);

        const file_data = FileData.init(self.arena.allocator(), false, self.current_time_ns);
        try self.files.put(path_copy, file_data);

        const sim_file = try self.allocator.create(SimulationFile);
        sim_file.* = SimulationFile{
            .magic = SIMULATION_FILE_MAGIC,
            .data = self.files.getPtr(path_copy).?,
            .position = 0,
            .mode = .write,
            .allocator = self.allocator,
            .vfs = self,
            .closed = false,
        };

        return VFile{
            .file_impl = sim_file,
            .vtable = &SimulationFile.vtable_impl,
            .allocator = self.allocator,
        };
    }

    fn remove(ptr: *anyopaque, path: []const u8) VFSError!void {
        const self: *Self = @ptrCast(@alignCast(ptr));
        assert(path.len > 0 and path.len < MAX_PATH_LENGTH);

        if (self.fault_injection.should_fail_operation(.{ .remove = true })) {
            return VFSError.IoError;
        }

        if (!self.files.contains(path)) {
            return VFSError.FileNotFound;
        }

        const file_data = self.files.get(path).?;
        if (file_data.is_directory) {
            return VFSError.IsDirectory;
        }

        // Update disk usage
        self.fault_injection.update_disk_usage(file_data.content.items.len, 0);

        // Remove the file (arena owns file content, we only free the path key)
        const removed_entry = self.files.fetchRemove(path).?;
        self.allocator.free(removed_entry.key);
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

        const path_copy = try self.allocator.dupe(u8, path);
        errdefer self.allocator.free(path_copy);

        const dir_data = FileData.init(self.arena.allocator(), true, self.current_time_ns);
        try self.files.put(path_copy, dir_data);
    }

    fn mkdir_all(ptr: *anyopaque, path: []const u8) VFSError!void {
        const self: *Self = @ptrCast(@alignCast(ptr));
        assert(path.len > 0 and path.len < MAX_PATH_LENGTH);

        if (self.files.contains(path)) {
            const file_data = self.files.get(path).?;
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

        if (!self.files.contains(path)) {
            return VFSError.FileNotFound;
        }

        const file_data = self.files.get(path).?;
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
    }

    /// Iterate directory entries using caller-provided arena allocator.
    /// All entry names and metadata are allocated in the provided arena,
    /// enabling O(1) cleanup when the arena is reset or destroyed.
    fn iterate_directory(ptr: *anyopaque, path: []const u8, allocator: std.mem.Allocator) VFSError!DirectoryIterator {
        const self: *Self = @ptrCast(@alignCast(ptr));
        assert(path.len > 0 and path.len < MAX_PATH_LENGTH);

        if (!self.files.contains(path)) {
            return VFSError.FileNotFound;
        }

        const file_data = self.files.get(path).?;
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
                    // This is a direct child - allocate name in caller's arena
                    const name = try allocator.dupe(u8, relative_path);
                    const kind = if (entry.value_ptr.is_directory)
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

        // Move the file data
        const removed_entry = self.files.fetchRemove(old_path).?;
        self.allocator.free(removed_entry.key);
        try self.files.put(new_path_copy, removed_entry.value);
    }

    fn stat(ptr: *anyopaque, path: []const u8) VFSError!VFS.FileStat {
        const self: *Self = @ptrCast(@alignCast(ptr));
        assert(path.len > 0 and path.len < MAX_PATH_LENGTH);

        if (!self.files.contains(path)) {
            return VFSError.FileNotFound;
        }

        const file_data = self.files.get(path).?;
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

/// Simulation file implementation wrapping in-memory content
const SimulationFile = struct {
    magic: u64,
    data: *SimulationVFS.FileData,
    position: u64,
    mode: VFS.OpenMode,
    allocator: std.mem.Allocator,
    vfs: *SimulationVFS,
    closed: bool,

    const Self = @This();

    const vtable_impl = VFile.VTable{
        .read = read,
        .write = write,
        .seek = seek,
        .tell = tell,
        .flush = flush,
        .close = close,
        .file_size = file_size,
        .destroy = destroy,
    };

    fn read(ptr: *anyopaque, buffer: []u8) VFileError!usize {
        const self: *Self = @ptrCast(@alignCast(ptr));
        assert(self.magic == SIMULATION_FILE_MAGIC);
        if (self.closed) return VFileError.FileClosed;
        if (!self.mode.can_read()) return VFileError.ReadError;

        if (self.vfs.fault_injection.should_fail_operation(.{ .read = true })) {
            return VFileError.IoError;
        }

        const content = self.data.content.items;
        if (self.position >= content.len) {
            return 0; // End of file
        }

        const available = @min(buffer.len, content.len - self.position);
        @memcpy(buffer[0..available], content[self.position .. self.position + available]);

        // Apply read corruption simulation
        self.vfs.fault_injection.apply_read_corruption(buffer[0..available]);

        self.position += available;
        return available;
    }

    fn write(ptr: *anyopaque, data: []const u8) VFileError!usize {
        const self: *Self = @ptrCast(@alignCast(ptr));
        assert(self.magic == SIMULATION_FILE_MAGIC);
        if (self.closed) return VFileError.FileClosed;
        if (!self.mode.can_write()) return VFileError.WriteError;

        if (self.vfs.fault_injection.should_fail_operation(.{ .write = true })) {
            return VFileError.IoError;
        }

        // Check for torn write simulation
        const write_size = if (self.vfs.fault_injection.should_torn_write(data.len)) |torn_size|
            torn_size
        else
            data.len;

        // Check disk space before writing
        const old_size = self.data.content.items.len;
        const new_size = @max(old_size, self.position + write_size);
        const size_increase = if (new_size > old_size) new_size - old_size else 0;

        if (!self.vfs.fault_injection.check_disk_space(size_increase)) {
            return VFileError.WriteError; // Disk full
        }

        // Extend content if writing past end
        if (self.position + write_size > self.data.content.items.len) {
            try self.data.content.resize(self.position + write_size);
        }

        // Copy data to buffer
        @memcpy(self.data.content.items[self.position .. self.position + write_size], data[0..write_size]);
        self.position += write_size;

        // Update disk usage and modification time
        self.vfs.fault_injection.update_disk_usage(old_size, self.data.content.items.len);
        self.data.modified_time = self.vfs.current_time_ns;

        return write_size;
    }

    fn seek(ptr: *anyopaque, pos: u64, whence: VFile.SeekFrom) VFileError!u64 {
        const self: *Self = @ptrCast(@alignCast(ptr));
        assert(self.magic == SIMULATION_FILE_MAGIC);
        if (self.closed) return VFileError.FileClosed;

        const new_position = switch (whence) {
            .start => pos,
            .current => self.position + pos,
            .end => self.data.content.items.len + pos,
        };

        self.position = new_position;
        return new_position;
    }

    fn tell(ptr: *anyopaque) VFileError!u64 {
        const self: *Self = @ptrCast(@alignCast(ptr));
        assert(self.magic == SIMULATION_FILE_MAGIC);
        if (self.closed) return VFileError.FileClosed;

        return self.position;
    }

    fn flush(ptr: *anyopaque) VFileError!void {
        const self: *Self = @ptrCast(@alignCast(ptr));
        assert(self.magic == SIMULATION_FILE_MAGIC);
        if (self.closed) return VFileError.FileClosed;

        // Simulation VFS flush is a no-op since everything is already in memory
    }

    fn close(ptr: *anyopaque) void {
        const self: *Self = @ptrCast(@alignCast(ptr));
        assert(self.magic == SIMULATION_FILE_MAGIC);

        if (!self.closed) {
            self.closed = true;
        }
    }

    fn file_size(ptr: *anyopaque) VFileError!u64 {
        const self: *Self = @ptrCast(@alignCast(ptr));
        assert(self.magic == SIMULATION_FILE_MAGIC);
        if (self.closed) return VFileError.FileClosed;

        const size = self.data.content.items.len;
        // Defensive validation of file size
        if (size > MAX_REASONABLE_FILE_SIZE) {
            return VFileError.IoError;
        }

        return size;
    }

    fn destroy(ptr: *anyopaque, allocator: std.mem.Allocator) void {
        const self: *Self = @ptrCast(@alignCast(ptr));
        assert(self.magic == SIMULATION_FILE_MAGIC);

        // Ensure file is closed before destroying
        if (!self.closed) {
            close(self);
        }

        // Poison the magic number to catch use-after-free in debug builds
        self.magic = 0xDEADDEAD;
        allocator.destroy(self);
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
