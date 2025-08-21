//! Mock VFS patterns and helpers for isolated storage testing.
//!
//! Provides reusable patterns for creating controlled filesystem
//! environments for testing storage components. Wraps SimulationVFS
//! with testing-specific convenience methods and failure injection
//! capabilities.

const std = @import("std");

const kausaldb = @import("kausaldb");

const simulation_vfs = kausaldb.simulation_vfs;
const testing = std.testing;
const vfs = kausaldb.vfs;

const SimulationVFS = simulation_vfs.SimulationVFS;
const VFS = vfs.VFS;

/// Mock VFS wrapper with testing conveniences and failure injection
pub const MockVFS = struct {
    sim_vfs: SimulationVFS,
    fail_next_create: bool = false,
    fail_next_write: bool = false,
    fail_next_read: bool = false,
    disk_full: bool = false,

    pub fn init(allocator: std.mem.Allocator) !MockVFS {
        return MockVFS{
            .sim_vfs = try SimulationVFS.init(allocator),
        };
    }

    pub fn deinit(self: *MockVFS) void {
        self.sim_vfs.deinit();
    }

    pub fn vfs(self: *MockVFS) VFS {
        return self.sim_vfs.vfs();
    }

    /// Create a pre-populated filesystem for testing
    pub fn setup_test_filesystem(self: *MockVFS) !void {
        // Create standard directory structure
        try self.vfs().mkdir("/test");
        try self.vfs().mkdir("/test/data");
        try self.vfs().mkdir("/test/data/wal");
        try self.vfs().mkdir("/test/data/sst");

        // Create some test files for discovery tests
        var file = try self.vfs().create("/test/data/sst/test_001.sst", .write);
        defer file.close();

        const test_content = "test sstable content";
        _ = try file.write(test_content);
        try file.flush();
    }

    /// Verify filesystem state matches expectations
    pub fn verify_directory_structure(self: *MockVFS, allocator: std.mem.Allocator) !void {
        try testing.expect(self.vfs().exists("/test/data"));
        try testing.expect(self.vfs().exists("/test/data/wal"));
        try testing.expect(self.vfs().exists("/test/data/sst"));

        // Verify we can list directories
        var arena = std.heap.ArenaAllocator.init(allocator);
        defer arena.deinit();

        const entries = try self.vfs().list_directory("/test/data", arena.allocator());
        try testing.expect(entries.len >= 2); // Should have wal and sst subdirs
    }

    /// Get list of files in SSTable directory
    pub fn list_sstables(self: *MockVFS, allocator: std.mem.Allocator) ![][]const u8 {
        return self.vfs().list_directory("/test/data/sst", allocator);
    }

    /// Get list of files in WAL directory
    pub fn list_wal_files(self: *MockVFS, allocator: std.mem.Allocator) ![][]const u8 {
        return self.vfs().list_directory("/test/data/wal", allocator);
    }

    /// Create a file with specific content for testing
    pub fn create_test_file(self: *MockVFS, path: []const u8, content: []const u8) !void {
        var file = try self.vfs().create(path, .write);
        defer file.close();

        _ = try file.write(content);
        try file.flush();
    }

    /// Read entire file content for verification
    pub fn read_test_file(self: *MockVFS, allocator: std.mem.Allocator, path: []const u8) ![]u8 {
        var file = try self.vfs().open(path, .read);
        defer file.close();

        const size = try file.file_size();
        const content = try allocator.alloc(u8, size);
        _ = try file.readAll(content);
        return content;
    }

    /// Simulate disk space exhaustion
    pub fn exhaust_disk_space(self: *MockVFS) void {
        self.sim_vfs.set_disk_limit(0);
    }

    /// Restore normal disk space
    pub fn restore_disk_space(self: *MockVFS) void {
        self.sim_vfs.set_disk_limit(1024 * 1024 * 1024); // 1GB limit
    }
};

/// Helper to create isolated test environment for storage components
pub fn create_isolated_test_env(allocator: std.mem.Allocator) !MockVFS {
    var mock_vfs = try MockVFS.init(allocator);
    try mock_vfs.setup_test_filesystem(allocator);
    return mock_vfs;
}

// Tests for the mock VFS helper itself

test "MockVFS basic functionality" {
    const allocator = testing.allocator;

    var mock_vfs = try MockVFS.init(allocator);
    defer mock_vfs.deinit();

    try mock_vfs.setup_test_filesystem(allocator);
    try mock_vfs.verify_directory_structure(allocator);
}

test "MockVFS failure injection" {
    const allocator = testing.allocator;

    var mock_vfs = try MockVFS.init(allocator);
    defer mock_vfs.deinit();

    // Test disk space exhaustion
    mock_vfs.exhaust_disk_space();

    // Should fail to create files when disk is full
    const result = mock_vfs.vfs().create("/test/should_fail.txt", .write);
    try testing.expectError(error.NoSpaceLeft, result);

    // Restore and verify normal operation
    mock_vfs.restore_disk_space();
    var file = try mock_vfs.vfs().create("/test/should_succeed.txt", .write);
    file.close();
}

test "MockVFS test file operations" {
    const allocator = testing.allocator;

    var mock_vfs = try MockVFS.init(allocator);
    defer mock_vfs.deinit();

    const test_content = "Hello, MockVFS!";
    const test_path = "/test/content.txt";

    // Create and write file
    try mock_vfs.create_test_file(test_path, test_content);

    // Read and verify content
    const read_content = try mock_vfs.read_test_file(allocator, test_path);
    defer allocator.free(read_content);

    try testing.expectEqualStrings(test_content, read_content);
}

test "MockVFS isolated environment creation" {
    const allocator = testing.allocator;

    var env = try create_isolated_test_env(allocator);
    defer env.deinit();

    try env.verify_directory_structure(allocator);

    var arena = std.heap.ArenaAllocator.init(allocator);
    defer arena.deinit();

    const sstables = try env.list_sstables(arena.allocator());
    // Should have the test SSTable created by setup
    try testing.expect(sstables.len > 0);
}
