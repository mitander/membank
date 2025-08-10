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
const custom_assert = @import("../core/assert.zig");
const assert = custom_assert.assert;
const fatal_assert = custom_assert.fatal_assert;
const testing = std.testing;
const vfs_types = @import("../core/vfs.zig");
const state_machines = @import("../core/state_machines.zig");
const file_handle = @import("../core/file_handle.zig");
const arena = @import("../core/arena.zig");

const VFS = vfs_types.VFS;
const VFile = vfs_types.VFile;
const VFSError = vfs_types.VFSError;
const VFileError = vfs_types.VFileError;
const SimulationFileData = vfs_types.SimulationFileData;
const MachineFileState = state_machines.FileState;
const TypedFileHandle = file_handle.TypedFileHandle;
const FileHandleId = file_handle.FileHandleId;
const FileAccessMode = file_handle.FileAccessMode;
const FileOperations = file_handle.FileOperations;
const FileHandleRegistry = file_handle.FileHandleRegistry;
const TypedArenaType = arena.TypedArenaType;

/// Type alias for VFile write operations with fault injection
const VFileWriteError = VFileError;
const DirectoryIterator = vfs_types.DirectoryIterator;
const DirectoryEntry = vfs_types.DirectoryEntry;

/// Maximum path length for defensive validation across platforms
const MAX_PATH_LENGTH = 4096;

/// Simulation file magic number for corruption detection in debug builds
const SIMULATION_FILE_MAGIC: u64 = 0xFEEDFACE_DEADBEEF;

/// Maximum reasonable file size to prevent memory exhaustion attacks
const MAX_REASONABLE_FILE_SIZE: u64 = 1024 * 1024 * 1024; // 1GB

/// Default maximum disk space for simulation (unlimited if not set)
const DEFAULT_MAX_DISK_SPACE: u64 = 100 * 1024 * 1024; // 100MB - reasonable for in-memory simulation

comptime {
    assert(MAX_PATH_LENGTH > 0);
    assert(MAX_PATH_LENGTH <= 8192);
    assert(MAX_REASONABLE_FILE_SIZE > 0);
    assert(MAX_REASONABLE_FILE_SIZE < std.math.maxInt(u64) / 2);
    assert(SIMULATION_FILE_MAGIC != 0);
    assert(SIMULATION_FILE_MAGIC != std.math.maxInt(u64));
}

/// Type-safe file storage with state machine validation using TypedFileHandle.
const FileStorage = struct {
    data: SimulationFileData,
    handle: TypedFileHandle,
    active: bool,

    const Self = @This();

    /// Create new file storage with TypedFileHandle integration.
    pub fn init(data: SimulationFileData, path: []const u8, handle_id: FileHandleId, access_mode: FileAccessMode) Self {
        var handle = TypedFileHandle.init(handle_id, path, access_mode);
        handle.file_size = data.content.items.len;

        return Self{
            .data = data,
            .handle = handle,
            .active = true,
        };
    }

    /// Validate file can perform read operation using TypedFileHandle.
    pub fn validate_read(self: *const Self) void {
        self.handle.state.assert_can_read();
        fatal_assert(self.handle.access_mode.can_read(), "File not opened for reading", .{});
    }

    /// Validate file can perform write operation using TypedFileHandle.
    pub fn validate_write(self: *const Self) void {
        self.handle.state.assert_can_write();
        fatal_assert(self.handle.access_mode.can_write(), "File not opened for writing", .{});
    }

    /// Close file and transition to closed state.
    pub fn close(self: *Self) void {
        self.handle.state.transition(.closed);
        self.handle.position = 0;
    }

    /// Check if file is active (not closed or deleted).
    pub fn is_active(self: *const Self) bool {
        return self.handle.state != .closed and self.handle.state != .deleted and self.active;
    }
};

/// Simulation VFS providing deterministic in-memory filesystem operations
pub const SimulationVFS = struct {
    magic: u64,
    allocator: std.mem.Allocator,
    file_arena: TypedArenaType(FileStorage, SimulationVFS),
    files: std.StringHashMap(FileHandleId),
    file_storage: std.ArrayList(FileStorage),
    handle_registry: FileHandleRegistry,
    current_time_ns: i64,
    fault_injection: FaultInjectionState,

    const MAGIC_NUMBER: u64 = 0xDEADBEEFCAFEBABE;

    comptime {
        // Validate struct follows type safety guidelines
        arena.validate_arena_naming(@This());
    }

    const Self = @This();

    /// File metadata and content storage for in-memory filesystem
    const FileData = SimulationFileData;

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
            // Prevent underflow when old_size > used_disk_space
            if (old_size > self.used_disk_space) {
                self.used_disk_space = new_size;
            } else {
                self.used_disk_space = self.used_disk_space - old_size + new_size;
            }
        }

        /// Disable all fault injection for clean recovery testing
        pub fn disable_all_faults(self: *FaultInjectionState) void {
            self.io_failure_config.enabled = false;
            self.torn_write_config.enabled = false;
            self.read_corruption_config.enabled = false;
        }
    };

    pub fn init(allocator: std.mem.Allocator) !SimulationVFS {
        return init_with_fault_seed(allocator, 0);
    }

    /// Initialize simulation VFS with deterministic fault injection using provided seed
    ///
    /// Creates a simulation filesystem with reproducible I/O failures.
    /// Useful for testing how the system handles file errors.
    pub fn init_with_fault_seed(allocator: std.mem.Allocator, seed: u64) !SimulationVFS {
        var file_arena = TypedArenaType(FileStorage, SimulationVFS).init(allocator, .simulation_test);
        errdefer file_arena.deinit();

        const files = std.StringHashMap(FileHandleId).init(allocator);
        var file_storage = std.ArrayList(FileStorage).init(allocator);
        file_storage.ensureTotalCapacity(512) catch unreachable; // Safety: generous initial capacity for testing

        return SimulationVFS{
            .magic = MAGIC_NUMBER,
            .allocator = allocator,
            .file_arena = file_arena,
            .files = files,
            .file_storage = file_storage,
            .handle_registry = FileHandleRegistry.init(allocator),
            .current_time_ns = 1_700_000_000_000_000_000, // Fixed epoch for determinism
            .fault_injection = FaultInjectionState.init(seed),
        };
    }

    /// Enable torn write simulation with specified parameters
    pub fn enable_torn_writes(
        self: *SimulationVFS,
        probability_per_thousand: u32,
        min_partial_bytes: u32,
        max_completion_fraction_per_thousand: u32,
    ) void {
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
    pub fn enable_read_corruption(
        self: *SimulationVFS,
        bit_flip_probability_per_kb: u32,
        max_bits_per_corruption: u32,
    ) void {
        assert(bit_flip_probability_per_kb <= 1000);
        assert(max_bits_per_corruption > 0 and max_bits_per_corruption <= 8);

        self.fault_injection.read_corruption_config = .{
            .enabled = true,
            .bit_flip_probability_per_kb = bit_flip_probability_per_kb,
            .max_bits_per_corruption = max_bits_per_corruption,
        };
    }

    /// Enable I/O failure simulation with specified parameters
    pub fn enable_io_failures(
        self: *SimulationVFS,
        failure_probability_per_thousand: u32,
        target_operations: FaultInjectionState.IoFailureConfig.OperationType,
    ) void {
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
        self.fault_injection.used_disk_space = 0;
        for (self.file_storage.items) |storage| {
            if (storage.active) {
                self.fault_injection.used_disk_space += storage.data.content.items.len;
            }
        }
    }

    /// Recalculate total disk usage from all active files
    fn recalculate_disk_usage(self: *SimulationVFS) void {
        self.fault_injection.used_disk_space = 0;
        for (self.file_storage.items) |storage| {
            if (storage.active) {
                self.fault_injection.used_disk_space += storage.data.content.items.len;
            }
        }
    }

    /// Retrieve stable pointer to file data by handle
    fn file_data_by_handle(self: *SimulationVFS, handle: FileHandleId) ?*SimulationFileData {
        for (self.file_storage.items) |*storage| {
            if (storage.handle.id.eql(handle) and storage.active) {
                return &storage.data;
            }
        }
        return null;
    }

    /// Retrieve FileStorage with state validation for operations
    fn file_storage_by_handle(self: *SimulationVFS, handle: FileHandleId) ?*FileStorage {
        for (self.file_storage.items) |*storage| {
            if (storage.handle.id.eql(handle) and storage.active) {
                return storage;
            }
        }
        return null;
    }

    /// Retrieve file data by handle for VFile read operations with state validation
    /// Returns null if handle is invalid, file was deleted, or state is invalid for reading
    fn file_data_fn(vfs_ptr: *anyopaque, handle_id: FileHandleId) ?*SimulationFileData {
        assert(@intFromPtr(vfs_ptr) >= 0x1000);
        assert(handle_id.is_valid());

        const self: *SimulationVFS = @ptrCast(@alignCast(vfs_ptr));

        // Get FileStorage with state validation
        const storage = self.file_storage_by_handle(handle_id) orelse return null;

        // State machine validation for read operations - must be active and readable
        if (!storage.is_active()) return null;
        storage.validate_read(); // This will assert if state doesn't allow reading

        return &storage.data;
    }

    /// Retrieve file storage for write operations with comprehensive state validation
    fn file_storage_for_write(vfs_ptr: *anyopaque, handle_id: FileHandleId) ?*FileStorage {
        assert(@intFromPtr(vfs_ptr) >= 0x1000);
        assert(handle_id.is_valid());

        const self: *SimulationVFS = @ptrCast(@alignCast(vfs_ptr));

        // Get FileStorage with state validation
        const storage = self.file_storage_by_handle(handle_id) orelse return null;

        // State machine validation for write operations - must be active and writable
        if (!storage.is_active()) return null;
        storage.validate_write(); // This will assert if state doesn't allow writing

        return storage;
    }

    /// Retrieve current deterministic time for VFile timestamp operations
    fn current_time_fn(vfs_ptr: *anyopaque) i64 {
        assert(@intFromPtr(vfs_ptr) >= 0x1000);

        const self: *SimulationVFS = @ptrCast(@alignCast(vfs_ptr));
        return self.current_time_ns;
    }

    /// Apply fault injection logic for VFile write operations
    /// Returns actual bytes to write (may be less than requested for torn writes)
    fn fault_injection_fn(vfs_ptr: *anyopaque, write_size: usize) VFileWriteError!usize {
        assert(@intFromPtr(vfs_ptr) >= 0x1000);
        assert(write_size > 0);
        assert(write_size <= MAX_REASONABLE_FILE_SIZE);

        const self: *SimulationVFS = @ptrCast(@alignCast(vfs_ptr));

        // Conservative disk space check - may reject writes that would fit due to overwrites
        // but prevents violations by always assuming additional space usage
        if (self.fault_injection.used_disk_space + write_size > self.fault_injection.max_disk_space) {
            return VFileWriteError.NoSpaceLeft;
        }

        if (self.fault_injection.should_torn_write(write_size)) |partial_size| {
            assert(partial_size <= write_size); // Torn writes never exceed original
            return partial_size;
        }

        return write_size;
    }

    /// Create new file storage entry using proper handle registry
    fn create_file_storage(
        self: *SimulationVFS,
        path: []const u8,
        data: SimulationFileData,
        access_mode: FileAccessMode,
    ) !FileHandleId {
        // Register the handle through the proper registry
        const handle_id = try self.handle_registry.register_handle(path, access_mode);

        // Create file storage using the registered handle
        const path_copy = try self.file_arena.allocator().dupe(u8, path);
        const file_storage = FileStorage.init(data, path_copy, handle_id, access_mode);
        try self.file_storage.append(file_storage);

        return handle_id;
    }

    /// Remove file storage entry and close handle through registry
    fn remove_file_storage(self: *SimulationVFS, handle: FileHandleId) void {
        // Remove from file storage list
        for (self.file_storage.items) |*storage| {
            if (storage.handle.id.eql(handle) and storage.active) {
                storage.active = false;
                break;
            }
        }

        // Close handle through registry for proper lifecycle management
        _ = self.handle_registry.close_handle(handle);
    }

    pub fn deinit(self: *SimulationVFS) void {
        var file_iter = self.files.iterator();
        while (file_iter.next()) |entry| {
            self.allocator.free(entry.key_ptr.*);
        }
        self.files.deinit();

        // Arena-managed file storage requires explicit content cleanup to prevent leaks
        for (self.file_storage.items) |*storage| {
            storage.data.content.deinit();
        }

        self.file_storage.deinit();
        self.handle_registry.deinit();
        self.file_arena.deinit();
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
    pub fn state(self: *SimulationVFS, allocator: std.mem.Allocator) ![]SimulationFileState {
        var states = std.ArrayList(SimulationFileState).init(allocator);
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
            const file_data = self.file_data_by_handle(handle) orelse continue;

            const path = try allocator.dupe(u8, entry.key_ptr.*);
            const content = if (!file_data.is_directory)
                try allocator.dupe(u8, file_data.content.items)
            else
                null;

            try states.append(SimulationFileState{
                .path = path,
                .content = content,
                .is_directory = file_data.is_directory,
                .size = file_data.content.items.len,
                .created_time = file_data.created_time,
                .modified_time = file_data.modified_time,
            });
        }

        const result = try states.toOwnedSlice();
        std.mem.sort(SimulationFileState, result, {}, compare_file_states);
        return result;
    }

    pub const SimulationFileState = struct {
        path: []const u8,
        content: ?[]const u8,
        is_directory: bool,
        size: usize,
        created_time: i64,
        modified_time: i64,
    };

    fn compare_file_states(context: void, a: SimulationFileState, b: SimulationFileState) bool {
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
        const file_data = self.file_data_by_handle(handle) orelse return VFSError.FileNotFound;

        if (file_data.is_directory) {
            return VFSError.IsDirectory;
        }

        return VFile{
            .impl = .{ .simulation = .{
                .vfs_ptr = ptr,
                .handle_id = handle,
                .position = 0,
                .mode = mode,
                .closed = false,
                .file_data_fn = file_data_fn,
                .current_time_fn = current_time_fn,
                .fault_injection_fn = fault_injection_fn,
            } },
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
        assert(path.len > 0 and path.len < MAX_PATH_LENGTH);

        const file_data = FileData{
            .content = std.ArrayList(u8).init(self.file_arena.allocator()),
            .created_time = self.current_time_ns,
            .modified_time = self.current_time_ns,
            .is_directory = false,
        };

        const handle = try self.create_file_storage(path, file_data, .read_write);

        const path_copy = try self.allocator.dupe(u8, path);

        try self.files.put(path_copy, handle);

        return VFile{
            .impl = .{ .simulation = .{
                .vfs_ptr = ptr,
                .handle_id = handle,
                .position = 0,
                .mode = .read_write,
                .closed = false,
                .file_data_fn = file_data_fn,
                .current_time_fn = current_time_fn,
                .fault_injection_fn = fault_injection_fn,
            } },
        };
    }

    fn remove(ptr: *anyopaque, path: []const u8) VFSError!void {
        const self: *Self = @ptrCast(@alignCast(ptr));
        assert(path.len > 0 and path.len < MAX_PATH_LENGTH);

        if (self.fault_injection.should_fail_operation(.{ .remove = true })) {
            return VFSError.IoError;
        }

        const handle = self.files.get(path) orelse return VFSError.FileNotFound;
        const file_data = self.file_data_by_handle(handle) orelse return VFSError.FileNotFound;

        if (file_data.is_directory) {
            return VFSError.IsDirectory;
        }

        // Recalculate disk usage after file removal
        self.recalculate_disk_usage();

        const removed_entry = self.files.fetchRemove(path).?;
        self.allocator.free(removed_entry.key);

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
            .content = std.ArrayList(u8).init(self.file_arena.allocator()),
            .created_time = self.current_time_ns,
            .modified_time = self.current_time_ns,
            .is_directory = true,
        };

        const handle = try self.create_file_storage(path, dir_data, .read_write);

        const path_copy = try self.allocator.dupe(u8, path);

        try self.files.put(path_copy, handle);
    }

    fn mkdir_all(ptr: *anyopaque, path: []const u8) VFSError!void {
        const self: *Self = @ptrCast(@alignCast(ptr));
        assert(path.len > 0 and path.len < MAX_PATH_LENGTH);

        if (self.files.contains(path)) {
            const handle = self.files.get(path).?;
            const file_data = self.file_data_by_handle(handle) orelse return VFSError.FileNotFound;
            if (file_data.is_directory) {
                return; // Success - directory already exists
            } else {
                return VFSError.FileExists;
            }
        }

        return mkdir(ptr, path);
    }

    fn rmdir(ptr: *anyopaque, path: []const u8) VFSError!void {
        const self: *Self = @ptrCast(@alignCast(ptr));
        assert(path.len > 0 and path.len < MAX_PATH_LENGTH);

        const handle = self.files.get(path) orelse return VFSError.FileNotFound;
        const file_data = self.file_data_by_handle(handle) orelse return VFSError.FileNotFound;

        if (!file_data.is_directory) {
            return VFSError.NotDirectory;
        }

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

        const removed_entry = self.files.fetchRemove(path).?;
        self.allocator.free(removed_entry.key);

        self.remove_file_storage(handle);
    }

    /// Iterate directory entries using caller-provided arena allocator.
    /// All entry names and metadata are allocated in the provided arena,
    /// enabling O(1) cleanup when the arena is reset or destroyed.
    fn iterate_directory(ptr: *anyopaque, path: []const u8, allocator: std.mem.Allocator) VFSError!DirectoryIterator {
        const self: *Self = @ptrCast(@alignCast(ptr));
        assert(path.len > 0 and path.len < MAX_PATH_LENGTH);

        const handle = self.files.get(path) orelse return VFSError.FileNotFound;
        const file_data = self.file_data_by_handle(handle) orelse return VFSError.FileNotFound;

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
                    const child_data = self.file_data_by_handle(child_handle) orelse continue;

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

        const entries_slice = try entries.toOwnedSlice();
        std.sort.block(DirectoryEntry, entries_slice, {}, struct {
            fn less_than(context: void, lhs: DirectoryEntry, rhs: DirectoryEntry) bool {
                _ = context;
                return std.mem.order(u8, lhs.name, rhs.name) == .lt;
            }
        }.less_than);

        return DirectoryIterator{
            .entries = entries_slice,
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

        const removed_entry = self.files.fetchRemove(old_path).?;
        self.allocator.free(removed_entry.key);
        try self.files.put(new_path_copy, removed_entry.value);
    }

    fn stat(ptr: *anyopaque, path: []const u8) VFSError!VFS.FileStat {
        const self: *Self = @ptrCast(@alignCast(ptr));
        assert(path.len > 0 and path.len < MAX_PATH_LENGTH);

        const handle = self.files.get(path) orelse return VFSError.FileNotFound;
        const file_data = self.file_data_by_handle(handle) orelse return VFSError.FileNotFound;

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
    }

    fn deinit_vfs(ptr: *anyopaque, allocator: std.mem.Allocator) void {
        const self: *Self = @ptrCast(@alignCast(ptr));
        _ = allocator;
        _ = self;
    }

    /// Disable all fault injection for clean recovery testing
    pub fn disable_all_fault_injection(self: *Self) void {
        self.fault_injection.disable_all_faults();
    }
};

test "SimulationVFS basic file operations" {
    const allocator = testing.allocator;

    var sim_vfs = try SimulationVFS.init(allocator);
    defer sim_vfs.deinit();

    const vfs_interface = sim_vfs.vfs();

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
    const allocator = testing.allocator;

    var sim_vfs = try SimulationVFS.init(allocator);
    defer sim_vfs.deinit();

    const vfs_interface = sim_vfs.vfs();

    const test_dir = "test_dir";
    try vfs_interface.mkdir(test_dir);
    defer vfs_interface.rmdir(test_dir) catch {};

    try testing.expect(vfs_interface.exists(test_dir));

    var iter_arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer iter_arena.deinit();
    const iter_allocator = iter_arena.allocator();

    var iterator = try vfs_interface.iterate_directory(test_dir, iter_allocator);

    try testing.expectEqual(@as(usize, 0), iterator.remaining());
    try testing.expect(iterator.next() == null);
}

test "SimulationVFS fault injection - torn writes" {
    const allocator = testing.allocator;

    var sim_vfs = try SimulationVFS.init_with_fault_seed(allocator, 12345);
    defer sim_vfs.deinit();

    sim_vfs.enable_torn_writes(1000, 1, 500);

    const vfs_interface = sim_vfs.vfs();

    const test_data = "Hello, this is a test message for torn writes!";

    var file = try vfs_interface.create("test.txt");
    defer {
        file.close();
        file.deinit();
        vfs_interface.remove("test.txt") catch {};
    }

    const written = try file.write(test_data);

    try testing.expect(written < test_data.len);
    try testing.expect(written > 0); // At least some data was written

    _ = try file.seek(0, .start);
    var buffer: [100]u8 = undefined;
    const read_bytes = try file.read(&buffer);

    try testing.expectEqual(written, read_bytes);
    try testing.expectEqualStrings(test_data[0..written], buffer[0..read_bytes]);
}

test "SimulationVFS fault injection - read corruption" {
    const allocator = testing.allocator;

    var sim_vfs = try SimulationVFS.init_with_fault_seed(allocator, 54321);
    defer sim_vfs.deinit();

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

    var buffer: [1024]u8 = undefined;
    const read_bytes = try file.read(&buffer);

    try testing.expectEqual(test_data.len, read_bytes);

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
    const allocator = testing.allocator;

    var sim_vfs = try SimulationVFS.init(allocator);
    defer sim_vfs.deinit();

    sim_vfs.configure_disk_space_limit(10);

    const vfs_interface = sim_vfs.vfs();

    const test_data = "This message is longer than 10 bytes";

    var file = try vfs_interface.create("test.txt");
    defer {
        file.close();
        file.deinit();
        vfs_interface.remove("test.txt") catch {};
    }

    const result = file.write(test_data);
    try testing.expectError(VFileError.WriteError, result);
}

test "SimulationVFS fault injection - io failures" {
    const allocator = testing.allocator;

    var sim_vfs = try SimulationVFS.init_with_fault_seed(allocator, 99999);
    defer sim_vfs.deinit();

    sim_vfs.enable_io_failures(1000, .{ .create = true });

    const vfs_interface = sim_vfs.vfs();

    const result = vfs_interface.create("test.txt");
    try testing.expectError(VFSError.IoError, result);
}

test "SimulationVFS deterministic behavior" {
    const allocator = testing.allocator;

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

    try testing.expectEqual(results[0], results[1]);
}

test "simulation_vfs_memory_safety_arraylist_expansion_resilience" {
    const allocator = testing.allocator;

    var sim_vfs = SimulationVFS.init(allocator);
    defer sim_vfs.deinit();
    const vfs_interface = sim_vfs.vfs();

    var file = try vfs_interface.create("expansion_test.log");
    defer {
        file.close();
        file.deinit();
        vfs_interface.remove("expansion_test.log") catch {};
    }

    const header_data = "HDR12345";
    const written_header = try file.write(header_data);
    try testing.expectEqual(header_data.len, written_header);

    _ = try file.seek(0, .start);
    var verify_header: [8]u8 = undefined;
    const read_header = try file.read(&verify_header);
    try testing.expectEqual(header_data.len, read_header);
    try testing.expect(std.mem.eql(u8, header_data, verify_header[0..read_header]));

    var large_buffer: [32768]u8 = undefined; // 32KB to trigger multiple reallocations
    @memset(&large_buffer, 0xCC);

    _ = try file.seek(8, .start);
    const written_large = try file.write(&large_buffer);
    try testing.expectEqual(large_buffer.len, written_large);

    _ = try file.seek(0, .start);
    var verify_after_expansion: [8]u8 = undefined;
    const read_after = try file.read(&verify_after_expansion);
    try testing.expectEqual(8, read_after);

    if (!std.mem.eql(u8, header_data, verify_after_expansion)) {
        return VFileError.CorruptedData;
    }

    _ = try file.seek(8, .start);
    var sample_verify: [256]u8 = undefined;
    const read_sample = try file.read(&sample_verify);
    try testing.expectEqual(256, read_sample);

    for (sample_verify) |byte| {
        try testing.expectEqual(@as(u8, 0xCC), byte);
    }
}

test "simulation_vfs_memory_safety_concurrent_file_creation_stress" {
    const allocator = testing.allocator;

    var sim_vfs = SimulationVFS.init(allocator);
    defer sim_vfs.deinit();
    const vfs_interface = sim_vfs.vfs();

    const num_files = 64; // Chosen to trigger HashMap expansion
    var file_patterns: [num_files][24]u8 = undefined;

    for (0..num_files) |i| {
        const file_name = try std.fmt.allocPrint(allocator, "stress_file_{}.dat", .{i});
        defer allocator.free(file_name);

        var file = try vfs_interface.create(file_name);
        defer {
            file.close();
            file.deinit();
            vfs_interface.remove(file_name) catch {};
        }

        const pattern_base = @as(u64, i) ^ 0xFEEDFACE;
        std.mem.writeInt(u64, file_patterns[i][0..8], pattern_base, .little);
        std.mem.writeInt(u64, file_patterns[i][8..16], pattern_base ^ 0xDEADBEEF, .little);
        std.mem.writeInt(u64, file_patterns[i][16..24], pattern_base ^ 0xCAFEBABE, .little);

        const written = try file.write(&file_patterns[i]);
        try testing.expectEqual(24, written);
    }

    for (0..num_files) |i| {
        const file_name = try std.fmt.allocPrint(allocator, "stress_file_{}.dat", .{i});
        defer allocator.free(file_name);

        var file = try vfs_interface.open(file_name, .read);
        defer {
            file.close();
            file.deinit();
        }

        var read_pattern: [24]u8 = undefined;
        const bytes_read = try file.read(&read_pattern);
        try testing.expectEqual(24, bytes_read);

        if (!std.mem.eql(u8, &file_patterns[i], &read_pattern)) {
            return VFileError.CorruptedData;
        }

        const read_base = std.mem.readInt(u64, read_pattern[0..8], .little);
        const expected_base = @as(u64, i) ^ 0xFEEDFACE;
        try testing.expectEqual(expected_base, read_base);
    }
}

test "simulation_vfs_memory_safety_zero_initialization_gap_filling" {
    const allocator = testing.allocator;

    var sim_vfs = SimulationVFS.init(allocator);
    defer sim_vfs.deinit();
    const vfs_interface = sim_vfs.vfs();

    var file = try vfs_interface.create("gap_fill_test.dat");
    defer {
        file.close();
        file.deinit();
        vfs_interface.remove("gap_fill_test.dat") catch {};
    }

    const initial_data = "INITIAL_DATA";
    const written_initial = try file.write(initial_data);
    try testing.expectEqual(initial_data.len, written_initial);

    const gap_start = 1000;
    _ = try file.seek(gap_start, .start);

    const final_data = "FINAL_DATA";
    const written_final = try file.write(final_data);
    try testing.expectEqual(final_data.len, written_final);

    _ = try file.seek(0, .start);
    const total_size = initial_data.len + (gap_start - initial_data.len) + final_data.len;
    var full_buffer = try allocator.alloc(u8, total_size);
    defer allocator.free(full_buffer);

    const total_read = try file.read(full_buffer);
    try testing.expectEqual(total_size, total_read);

    try testing.expect(std.mem.eql(u8, initial_data, full_buffer[0..initial_data.len]));

    var gap_corruption_count: u32 = 0;
    for (full_buffer[initial_data.len..gap_start]) |byte| {
        if (byte != 0) {
            gap_corruption_count += 1;
        }
    }

    if (gap_corruption_count > 0) {
        return VFileError.CorruptedData;
    }

    const final_start = gap_start;
    const final_end = final_start + final_data.len;
    try testing.expect(std.mem.eql(u8, final_data, full_buffer[final_start..final_end]));
}
