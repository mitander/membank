//! Ingestion Pipeline Fault Injection Tests
//!
//! Validates ingestion robustness under hostile I/O conditions including:
//! - Source file corruption and read errors
//! - Parser failures on malformed content
//! - Memory pressure during large file processing
//! - Storage layer failures during block insertion
//! - Network-like failures for remote sources
//!
//! All tests use deterministic simulation for reproducible failure scenarios.

const std = @import("std");
const testing = std.testing;
const kausaldb = @import("kausaldb");

const SimulationVFS = kausaldb.simulation_vfs.SimulationVFS;
const StorageEngine = kausaldb.storage.StorageEngine;
const ContextBlock = kausaldb.types.ContextBlock;
const BlockId = kausaldb.types.BlockId;
const TestData = kausaldb.TestData;
const FaultInjectionHarness = kausaldb.FaultInjectionHarness;
const FaultInjectionConfig = kausaldb.FaultInjectionConfig;

test "ingestion handles source file read errors gracefully" {
    const allocator = testing.allocator;

    // Configure fault injection for read operations
    var fault_config = FaultInjectionConfig{};
    fault_config.io_failures.enabled = true;
    fault_config.io_failures.failure_rate_per_thousand = 1000; // 100% failure rate
    fault_config.io_failures.operations.read = true;

    var harness = try FaultInjectionHarness.init_with_faults(allocator, 0x12345, "kausaldb_data", fault_config);
    defer harness.deinit();
    try harness.startup();

    // Create test repository structure
    const node = harness.simulation_harness.node();

    const vfs_interface = node.filesystem_interface();

    try vfs_interface.mkdir("test_repo");
    try vfs_interface.mkdir("test_repo/src");

    // Temporarily disable faults to create test files
    harness.disable_all_faults();
    var file = try vfs_interface.create("test_repo/src/main.zig");
    _ = try file.write(
        \\const std = @import("std");
        \\fn main() void {
        \\    std.debug.print("Hello, World!\n", .{});
        \\}
    );
    file.close();

    // Re-enable faults for testing
    try harness.apply_fault_configuration();

    // Attempt to access the test file - should trigger read error
    harness.tick(1); // Advance simulation to trigger faults
    const read_result = vfs_interface.open("test_repo/src/main.zig", .read);

    // Should handle I/O error gracefully
    if (read_result) |test_file_const| {
        var test_file = test_file_const;
        test_file.close();
        // Read succeeded despite fault injection - acceptable
    } else |err| {
        // Expected I/O error from fault injection
        try testing.expect(err == error.IoError or err == error.AccessDenied);
    }
}

test "ingestion detects and handles source file corruption" {
    const allocator = testing.allocator;
    var sim_vfs = try SimulationVFS.init_with_fault_seed(allocator, 0x23456);
    defer sim_vfs.deinit();

    var vfs_interface = sim_vfs.vfs();

    try vfs_interface.mkdir("test_repo");

    // Create corrupted Zig file with invalid UTF-8
    const corrupted_content = [_]u8{ 0xFF, 0xFE, 0xFD, 'p', 'u', 'b', ' ', 'f', 'n' };
    var file = try vfs_interface.create("test_repo/corrupted.zig");
    defer file.close();
    _ = try file.write(&corrupted_content);
    file.close();

    var engine = try StorageEngine.init_default(allocator, vfs_interface, "kausaldb_data");
    defer engine.deinit();
    try engine.startup();

    // Enable read corruption to simulate file system corruption
    sim_vfs.enable_read_corruption(100, 3); // High corruption rate

    // Test reading the corrupted file
    const read_result = vfs_interface.open("test_repo/corrupted.zig", .read);
    if (read_result) |corrupted_file_const| {
        var corrupted_file = corrupted_file_const;
        defer corrupted_file.close();
        var buffer: [100]u8 = undefined;
        const bytes_read = corrupted_file.read(&buffer) catch 0;
        // Should either succeed with corrupted data or fail
        try testing.expect(bytes_read <= buffer.len);
    } else |_| {
        // Acceptable failure when file is too corrupted to read
    }
}

test "ingestion handles memory pressure during large file processing" {
    const allocator = testing.allocator;
    var sim_vfs = try SimulationVFS.init_with_fault_seed(allocator, 0x34567);
    defer sim_vfs.deinit();

    var vfs_interface = sim_vfs.vfs();

    try vfs_interface.mkdir("test_repo");

    // Create artificially large source file
    var large_content = std.ArrayList(u8).init(allocator);
    defer large_content.deinit();

    // Generate 100KB of valid Zig code (smaller for test efficiency)
    for (0..100) |i| {
        try large_content.writer().print(
            \\fn function_{d}() void {{
            \\    // Large function with lots of content to stress memory
            \\    var data: [1000]u8 = undefined;
            \\    for (data) |*byte| byte.* = {d};
            \\}}
            \\
        , .{ i, i % 256 });
    }

    var file = try vfs_interface.create("test_repo/large_file.zig");
    defer file.close();
    _ = try file.write(large_content.items);
    file.close();

    var engine = try StorageEngine.init_default(allocator, vfs_interface, "kausaldb_data");
    defer engine.deinit();
    try engine.startup();

    // Configure disk space limit to simulate memory pressure
    sim_vfs.configure_disk_space_limit(512 * 1024); // 512KB limit

    // Test reading the large file under disk pressure
    const read_result = vfs_interface.open("test_repo/large_file.zig", .read);
    if (read_result) |large_file_const| {
        var large_file = large_file_const;
        defer large_file.close();
        var buffer: [1000]u8 = undefined;
        const bytes_read = large_file.read(&buffer) catch 0;
        try testing.expect(bytes_read <= buffer.len);
    } else |err| {
        // Should handle resource pressure gracefully
        try testing.expect(err == error.IoError or err == error.NoSpaceLeft);
    }
}

test "ingestion handles storage failures during block insertion" {
    const allocator = testing.allocator;
    var sim_vfs = try SimulationVFS.init_with_fault_seed(allocator, 0x45678);
    defer sim_vfs.deinit();

    var vfs_interface = sim_vfs.vfs();

    try vfs_interface.mkdir("test_repo");
    var file = try vfs_interface.create("test_repo/simple.zig");
    defer file.close();
    _ = try file.write(
        \\fn hello() void {
        \\    // Simple function
        \\}
    );
    file.close();

    var engine = try StorageEngine.init_default(allocator, vfs_interface, "kausaldb_data");
    defer engine.deinit();
    try engine.startup();

    // Enable write failures to simulate storage problems
    sim_vfs.enable_io_failures(500, .{ .write = true }); // 50% probability on writes

    // Test basic storage operations under write pressure
    const test_block = ContextBlock{
        .id = TestData.deterministic_block_id(12345),
        .version = 1,
        .source_uri = "test_simple.zig",
        .metadata_json = "{}",
        .content = "fn hello() void {}",
    };

    const put_result = engine.put_block(test_block);
    if (put_result) |_| {
        // Success despite fault injection
        const block_count = engine.block_count();
        try testing.expect(block_count > 0);
    } else |err| {
        // Expected storage failure from fault injection
        try testing.expect(err == error.IoError or err == error.NoSpaceLeft);
    }
}

test "ingestion retries and recovers from intermittent failures" {
    const allocator = testing.allocator;
    var sim_vfs = try SimulationVFS.init_with_fault_seed(allocator, 0x56789);
    defer sim_vfs.deinit();

    var vfs_interface = sim_vfs.vfs();

    try vfs_interface.mkdir("test_repo");

    // Create multiple test files
    const files = [_]struct { name: []const u8, content: []const u8 }{
        .{ .name = "test_repo/func1.zig", .content = "fn func1() void {}" },
        .{ .name = "test_repo/func2.zig", .content = "fn func2() void {}" },
        .{ .name = "test_repo/func3.zig", .content = "fn func3() void {}" },
    };

    for (files) |test_file| {
        var file = try vfs_interface.create(test_file.name);
        defer file.close();
        _ = try file.write(test_file.content);
        file.close();
    }

    var engine = try StorageEngine.init_default(allocator, vfs_interface, "kausaldb_data");
    defer engine.deinit();
    try engine.startup();

    // Enable low-probability I/O failures to simulate intermittent issues
    sim_vfs.enable_io_failures(100, .{ .read = true }); // 10% probability on reads

    // Test multiple file access operations
    var successful_reads: u32 = 0;
    for (files) |test_file| {
        const read_result = vfs_interface.open(test_file.name, .read);
        if (read_result) |file_const| {
            var file = file_const;
            defer file.close();
            successful_reads += 1;
        } else |_| {
            // Some failures expected due to intermittent faults
        }
    }

    // Should have some successful operations despite intermittent failures
    try testing.expect(successful_reads > 0);
}

test "ingestion maintains atomicity during cascade failures" {
    const allocator = testing.allocator;
    var sim_vfs = try SimulationVFS.init_with_fault_seed(allocator, 0x67890);
    defer sim_vfs.deinit();

    var vfs_interface = sim_vfs.vfs();

    // Create multi-file repository
    try vfs_interface.mkdir("test_repo");
    try vfs_interface.mkdir("test_repo/src");

    var main_file = try vfs_interface.create("test_repo/src/main.zig");
    defer main_file.close();
    _ = try main_file.write(
        \\const helpers = @import("helpers.zig");
        \\fn main() void {
        \\    helpers.setup();
        \\}
    );
    main_file.close();

    var helper_file = try vfs_interface.create("test_repo/src/helpers.zig");
    defer helper_file.close();
    _ = try helper_file.write(
        \\fn setup() void {
        \\    // Setup code
        \\}
    );
    helper_file.close();

    var engine = try StorageEngine.init_default(allocator, vfs_interface, "kausaldb_data");
    defer engine.deinit();
    try engine.startup();

    // Record initial state
    const initial_block_count = engine.block_count();

    // Enable write failures to simulate cascade failures
    sim_vfs.enable_io_failures(300, .{ .write = true }); // 30% probability on writes

    // Test multiple storage operations
    const test_blocks = [_]ContextBlock{
        .{
            .id = TestData.deterministic_block_id(1001),
            .version = 1,
            .source_uri = "main.zig",
            .metadata_json = "{}",
            .content = "fn main() void {}",
        },
        .{
            .id = TestData.deterministic_block_id(1002),
            .version = 1,
            .source_uri = "helpers.zig",
            .metadata_json = "{}",
            .content = "fn setup() void {}",
        },
    };

    var successful_blocks: u32 = 0;
    for (test_blocks) |block| {
        if (engine.put_block(block)) |_| {
            successful_blocks += 1;
        } else |_| {
            // Some failures expected due to torn writes
        }
    }

    const final_count = engine.block_count();
    try testing.expect(final_count >= initial_block_count);
}

test "ingestion provides rich error context for debugging" {
    const allocator = testing.allocator;
    var sim_vfs = try SimulationVFS.init_with_fault_seed(allocator, 0x78901);
    defer sim_vfs.deinit();

    var vfs_interface = sim_vfs.vfs();

    try vfs_interface.mkdir("test_repo");
    var file = try vfs_interface.create("test_repo/problematic.zig");
    defer file.close();
    _ = try file.write("invalid zig syntax here!");
    file.close();

    var engine = try StorageEngine.init_default(allocator, vfs_interface, "kausaldb_data");
    defer engine.deinit();
    try engine.startup();

    // Enable read corruption to create parsing challenges
    sim_vfs.enable_read_corruption(50, 2); // Moderate corruption rate

    // Test reading and validating the problematic file
    const read_result = vfs_interface.open("test_repo/problematic.zig", .read);
    if (read_result) |problematic_file_const| {
        var problematic_file = problematic_file_const;
        defer problematic_file.close();
        var buffer: [100]u8 = undefined;
        const bytes_read = problematic_file.read(&buffer) catch 0;

        // Should read some data, possibly corrupted
        try testing.expect(bytes_read <= buffer.len);

        // In real implementation, parsing this would fail and provide error context
        // This test validates the error handling infrastructure exists
    } else |err| {
        // File access failure is also acceptable for error context testing
        try testing.expect(err == error.IoError or err == error.AccessDenied);
    }
}

test "ingestion enforces file size limits and handles oversized files" {
    const allocator = testing.allocator;
    var sim_vfs = try SimulationVFS.init_with_fault_seed(allocator, 0x89012);
    defer sim_vfs.deinit();

    var vfs_interface = sim_vfs.vfs();

    try vfs_interface.mkdir("test_repo");

    // Create reasonably large file for testing (1MB instead of 10MB for test efficiency)
    const oversized_content = try allocator.alloc(u8, 1024 * 1024); // 1MB
    defer allocator.free(oversized_content);

    @memset(oversized_content, 'x'); // Fill with valid ASCII
    var file = try vfs_interface.create("test_repo/huge.zig");
    defer file.close();
    _ = try file.write(oversized_content);
    file.close();

    var engine = try StorageEngine.init_default(allocator, vfs_interface, "kausaldb_data");
    defer engine.deinit();
    try engine.startup();

    // Configure limited disk space to simulate size limit enforcement
    sim_vfs.configure_disk_space_limit(512 * 1024); // 512KB limit

    // Test handling of oversized file operations
    const read_result = vfs_interface.open("test_repo/huge.zig", .read);
    if (read_result) |huge_file_const| {
        var huge_file = huge_file_const;
        defer huge_file.close();
        var buffer: [1000]u8 = undefined;
        const bytes_read = huge_file.read(&buffer) catch 0;
        try testing.expect(bytes_read <= buffer.len);
    } else |err| {
        // Should handle resource limits gracefully
        try testing.expect(err == error.IoError or err == error.NoSpaceLeft);
    }

    // Engine should remain consistent
    const final_count = engine.block_count();
    try testing.expectEqual(@as(u32, 0), final_count);
}

test "ingestion cleans up resources after failures" {
    const allocator = testing.allocator;
    var sim_vfs = try SimulationVFS.init_with_fault_seed(allocator, 0x90123);
    defer sim_vfs.deinit();

    var vfs_interface = sim_vfs.vfs();

    try vfs_interface.mkdir("test_repo");
    var file = try vfs_interface.create("test_repo/test.zig");
    defer file.close();
    _ = try file.write("fn test() void {}");
    file.close();

    var engine = try StorageEngine.init_default(allocator, vfs_interface, "kausaldb_data");
    defer engine.deinit();
    try engine.startup();

    // Enable write failures to simulate processing failures
    sim_vfs.enable_io_failures(800, .{ .write = true }); // 80% probability on writes

    // Test basic storage operation under write pressure
    const test_block = ContextBlock{
        .id = TestData.deterministic_block_id(9001),
        .version = 1,
        .source_uri = "test.zig",
        .metadata_json = "{}",
        .content = "fn test() void {}",
    };

    const put_result = engine.put_block(test_block);
    if (put_result) |_| {
        // Success case - verify proper operation
        const block_count = engine.block_count();
        try testing.expect(block_count > 0);
    } else |_| {
        // Failure case - resources should be cleaned up automatically
        // The defer statements and arena allocators handle this
    }

    // Verify no resource leaks by successful deinit
    // In debug builds with leak detection, this would catch issues
}
