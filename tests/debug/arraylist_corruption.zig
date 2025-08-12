//! ArrayList corruption debugging test focusing on path discovery mechanism
//!
//! This test exercises the SSTableManager's path discovery process which adds
//! paths to the ArrayList during startup(). The goal is to isolate the buffer
//! overflow that corrupts path strings, manifesting as Unicode replacement
//! characters (���...) when memory gets overwritten.

const std = @import("std");
const testing = std.testing;
const kausaldb = @import("kausaldb");

const assert = kausaldb.assert.assert;
const fatal_assert = kausaldb.assert.fatal_assert;
const SSTableManager = kausaldb.storage.SSTableManager;
const SimulationVFS = kausaldb.simulation_vfs.SimulationVFS;
const ContextBlock = kausaldb.types.ContextBlock;
const BlockId = kausaldb.types.BlockId;
const TestData = kausaldb.TestData;
const ArenaCoordinator = kausaldb.memory.ArenaCoordinator;

/// Validate ArrayList integrity and detect string corruption patterns
fn validate_path_integrity(manager: *SSTableManager, context: []const u8) !void {
    for (manager.sstable_paths.items, 0..) |path, i| {
        // Check for null pointer corruption
        if (@intFromPtr(path.ptr) == 0 and path.len > 0) {
            std.log.err("CORRUPTION in {s}: Path[{}] null pointer, len={}", .{ context, i, path.len });
            return error.PathCorrupted;
        }

        // Check for suspicious length values
        if (path.len > 4096) {
            std.log.err("CORRUPTION in {s}: Path[{}] suspicious length {}", .{ context, i, path.len });
            return error.PathCorrupted;
        }

        // Check for Unicode replacement characters (buffer overflow indicator)
        if (std.mem.indexOf(u8, path, "���") != null) {
            std.log.err("CORRUPTION in {s}: Path[{}] has replacement chars: '{s}'", .{ context, i, path });
            return error.PathCorrupted;
        }

        // Check for 0xAA poison bytes (memory corruption pattern)
        for (path) |byte| {
            if (byte == 0xAA or byte == 0xBB or byte == 0xCC) {
                std.log.err("CORRUPTION in {s}: Path[{}] poison byte 0x{X}: '{s}'", .{ context, i, byte, path });
                return error.PathCorrupted;
            }
        }

        std.log.debug("Path[{}] OK: '{s}' (len={})", .{ i, path, path.len });
    }
}

test "sstable discovery arraylist corruption detection" {
    // Manual setup required because: Need GPA safety features to detect
    // buffer overflow that corrupts ArrayList path strings during discovery
    var gpa = std.heap.GeneralPurposeAllocator(.{ .safety = true }){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    var sim_vfs = try SimulationVFS.init(allocator);
    defer sim_vfs.deinit();

    // Create SSTable files that discovery will find and add to ArrayList
    const sst_dir = "test_db/sst";
    try sim_vfs.vfs().mkdir("test_db");
    try sim_vfs.vfs().mkdir(sst_dir);

    // Create multiple .sst files to trigger ArrayList operations
    var i: u32 = 0;
    while (i < 10) : (i += 1) {
        const filename = try std.fmt.allocPrint(allocator, "{s}/table_{:06}.sst", .{ sst_dir, i });
        defer allocator.free(filename);

        // Create a minimal valid SSTable file
        var file = try sim_vfs.vfs().create(filename);
        defer file.close();

        // Write SSTable header with magic number
        const header_data = [_]u8{
            0x53, 0x53, 0x54, 0x42, // SSTB magic
            0x01, 0x00, 0x00, 0x00, // version = 1
            0x00, 0x00, 0x00, 0x00, // block_count = 0
            0x40, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // index_offset = 64
        };
        _ = try file.write(header_data[0..]);

        // Pad to 64 bytes
        const padding = [_]u8{0} ** (64 - header_data.len);
        _ = try file.write(padding[0..]);

        std.log.debug("Created SSTable file: {s}", .{filename});
    }

    std.log.info("=== TESTING ARRAYLIST CORRUPTION DURING DISCOVERY ===", .{});

    var test_arena = std.heap.ArenaAllocator.init(allocator);
    defer test_arena.deinit();
    const coordinator = ArenaCoordinator.init(&test_arena);
    var manager = SSTableManager.init(&coordinator, allocator, sim_vfs.vfs(), "test_db");
    defer manager.deinit();

    // Validate initial state
    try validate_path_integrity(&manager, "initial state");
    try testing.expectEqual(@as(usize, 0), manager.sstable_paths.items.len);

    std.log.info("Starting discovery process that will populate ArrayList...", .{});

    // This triggers discover_existing_sstables() which adds paths to ArrayList
    try manager.startup();

    std.log.info("Discovery completed, validating path integrity...", .{});

    // Validate that all discovered paths are uncorrupted
    try validate_path_integrity(&manager, "after discovery");

    // Log the results
    std.log.info("Discovered {} SSTable paths", .{manager.sstable_paths.items.len});
    for (manager.sstable_paths.items, 0..) |path, idx| {
        std.log.info("Path[{}]: '{s}'", .{ idx, path });
    }

    try testing.expectEqual(@as(usize, 10), manager.sstable_paths.items.len);

    std.log.info("=== DISCOVERY ARRAYLIST CORRUPTION TEST PASSED ===", .{});
}

test "sstable discovery with large path names" {
    // Manual setup required because: Need GPA to detect buffer overflow
    // in path string allocation during discovery of files with long names
    var gpa = std.heap.GeneralPurposeAllocator(.{ .safety = true }){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    var sim_vfs = try SimulationVFS.init(allocator);
    defer sim_vfs.deinit();

    const sst_dir = "test_db/sst";
    try sim_vfs.vfs().mkdir("test_db");
    try sim_vfs.vfs().mkdir(sst_dir);

    // Create files with increasingly long names to stress path allocation
    var i: u32 = 0;
    while (i < 8) : (i += 1) {
        // Create progressively longer filenames with dynamic padding
        const padding_size = 50 + i * 10; // 50, 60, 70, etc. characters
        const name_padding = try allocator.alloc(u8, padding_size);
        defer allocator.free(name_padding);
        @memset(name_padding, 'X');
        const filename = try std.fmt.allocPrint(allocator, "{s}/very_long_sstable_name_{s}_{:06}.sst", .{ sst_dir, name_padding, i });
        defer allocator.free(filename);

        var file = try sim_vfs.vfs().create(filename);
        defer file.close();

        // Write minimal SSTable header
        const header_data = [_]u8{
            0x53, 0x53, 0x54, 0x42, // SSTB magic
            0x01, 0x00, 0x00, 0x00, // version = 1
            0x00, 0x00, 0x00, 0x00, // block_count = 0
            0x40, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // index_offset = 64
        };
        _ = try file.write(header_data[0..]);

        // Pad to 64 bytes
        const padding = [_]u8{0} ** (64 - header_data.len);
        _ = try file.write(padding[0..]);

        std.log.debug("Created long-named SSTable: {s} (len={})", .{ filename, filename.len });
    }

    std.log.info("=== TESTING LARGE PATH ALLOCATION CORRUPTION ===", .{});

    var test_arena = std.heap.ArenaAllocator.init(allocator);
    defer test_arena.deinit();
    const coordinator = ArenaCoordinator.init(&test_arena);
    var manager = SSTableManager.init(&coordinator, allocator, sim_vfs.vfs(), "test_db");
    defer manager.deinit();

    // Monitor memory state before discovery
    std.log.info("Pre-discovery ArrayList: capacity={}, len={}", .{ manager.sstable_paths.capacity, manager.sstable_paths.items.len });

    try manager.startup();

    // Validate all paths after discovery with large names
    try validate_path_integrity(&manager, "after large path discovery");

    std.log.info("Post-discovery ArrayList: capacity={}, len={}", .{ manager.sstable_paths.capacity, manager.sstable_paths.items.len });

    // Verify all paths are correctly stored
    for (manager.sstable_paths.items, 0..) |path, idx| {
        try testing.expect(path.len > 0);
        try testing.expect(std.mem.endsWith(u8, path, ".sst"));
        std.log.debug("Large path[{}]: '{s}' (len={})", .{ idx, path, path.len });
    }

    try testing.expectEqual(@as(usize, 8), manager.sstable_paths.items.len);

    std.log.info("=== LARGE PATH ALLOCATION TEST PASSED ===", .{});
}

test "sstable discovery stress test with many files" {
    // Manual setup required because: Need GPA to detect memory corruption
    // during ArrayList reallocation with many SSTable files
    var gpa = std.heap.GeneralPurposeAllocator(.{ .safety = true }){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    var sim_vfs = try SimulationVFS.init(allocator);
    defer sim_vfs.deinit();

    const sst_dir = "test_db/sst";
    try sim_vfs.vfs().mkdir("test_db");
    try sim_vfs.vfs().mkdir(sst_dir);

    // Create many SSTable files to force ArrayList reallocation
    const file_count = 100;
    var i: u32 = 0;
    while (i < file_count) : (i += 1) {
        const filename = try std.fmt.allocPrint(allocator, "{s}/stress_table_{:06}.sst", .{ sst_dir, i });
        defer allocator.free(filename);

        var file = try sim_vfs.vfs().create(filename);
        defer file.close();

        // Write minimal SSTable header
        const header_data = [_]u8{
            0x53, 0x53, 0x54, 0x42, // SSTB magic
            0x01, 0x00, 0x00, 0x00, // version = 1
            0x00, 0x00, 0x00, 0x00, // block_count = 0
            0x40, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // index_offset = 64
        };
        _ = try file.write(header_data[0..]);

        // Pad to 64 bytes
        const padding = [_]u8{0} ** (64 - header_data.len);
        _ = try file.write(padding[0..]);
    }

    std.log.info("=== STRESS TESTING ARRAYLIST WITH {} FILES ===", .{file_count});

    var test_arena = std.heap.ArenaAllocator.init(allocator);
    defer test_arena.deinit();
    const coordinator = ArenaCoordinator.init(&test_arena);
    var manager = SSTableManager.init(&coordinator, allocator, sim_vfs.vfs(), "test_db");
    defer manager.deinit();

    // Monitor initial state
    std.log.info("Pre-startup: capacity={}, len={}", .{ manager.sstable_paths.capacity, manager.sstable_paths.items.len });

    // This should trigger multiple ArrayList reallocations
    try manager.startup();

    // Validate integrity after potential reallocation stress
    try validate_path_integrity(&manager, "after stress discovery");

    std.log.info("Post-startup: capacity={}, len={}", .{ manager.sstable_paths.capacity, manager.sstable_paths.items.len });

    // Verify we found all files
    try testing.expectEqual(@as(usize, file_count), manager.sstable_paths.items.len);

    // Additional corruption checks on random samples
    const sample_indices = [_]usize{ 4, 25, 50, 75, 99 }; // Index 4 is where corruption typically occurs
    for (sample_indices) |idx| {
        if (idx < manager.sstable_paths.items.len) {
            const path = manager.sstable_paths.items[idx];
            try testing.expect(path.len > 0);
            try testing.expect(std.mem.endsWith(u8, path, ".sst"));

            // Check that path can be read as valid UTF-8
            if (!std.unicode.utf8ValidateSlice(path)) {
                std.log.err("CORRUPTION: Path[{}] invalid UTF-8: '{s}'", .{ idx, path });
                return error.InvalidUTF8;
            }

            std.log.debug("Sample path[{}]: '{s}' ✓", .{ idx, path });
        }
    }

    std.log.info("=== STRESS TEST COMPLETED - {} PATHS VALIDATED ===", .{file_count});
}
