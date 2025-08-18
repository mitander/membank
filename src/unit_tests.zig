//! Comprehensive unit test runner for KausalDB components
//! Automatically discovers and includes all .zig files with tests

comptime {
    _ = @import("core/arena.zig");
    _ = @import("core/assert.zig");
    _ = @import("core/bounded.zig");
    _ = @import("core/concurrency.zig");
    _ = @import("core/error_context.zig");
    _ = @import("core/file_handle.zig");
    _ = @import("core/memory.zig");
    _ = @import("core/memory_integration.zig");
    _ = @import("core/ownership.zig");
    _ = @import("core/pools.zig");
    _ = @import("core/production_vfs.zig");
    _ = @import("core/signals.zig");
    _ = @import("core/state_machines.zig");
    _ = @import("core/types.zig");
    _ = @import("core/vfs.zig");
    _ = @import("dev/commit_msg_validator.zig");
    _ = @import("dev/debug_allocator.zig");
    _ = @import("dev/shell.zig");
    _ = @import("dev/tidy.zig");
    _ = @import("ingestion/git_source.zig");
    _ = @import("ingestion/glob_matcher.zig");
    _ = @import("ingestion/pipeline.zig");
    _ = @import("ingestion/semantic_chunker.zig");
    _ = @import("ingestion/zig_parser.zig");
    _ = @import("kausaldb.zig");
    _ = @import("query/cache.zig");
    _ = @import("query/engine.zig");
    _ = @import("query/filtering.zig");
    _ = @import("query/zero_copy.zig");
    _ = @import("server/connection_manager.zig");
    _ = @import("server/handler.zig");
    _ = @import("sim/simulation.zig");
    _ = @import("sim/simulation_vfs.zig");
    _ = @import("storage/block_index.zig");
    _ = @import("storage/bloom_filter.zig");
    _ = @import("storage/config.zig");
    _ = @import("storage/engine.zig");
    _ = @import("storage/graph_edge_index.zig");
    _ = @import("storage/memtable_manager.zig");
    _ = @import("storage/metadata_index.zig");
    _ = @import("storage/metrics.zig");
    _ = @import("storage/recovery.zig");
    _ = @import("storage/sstable.zig");
    _ = @import("storage/sstable_manager.zig");
    _ = @import("storage/tiered_compaction.zig");
    _ = @import("storage/wal.zig");
    _ = @import("storage/wal/core.zig");
    _ = @import("storage/wal/corruption_tracker.zig");
    _ = @import("storage/wal/entry.zig");
    _ = @import("storage/wal/recovery.zig");
    _ = @import("storage/wal/stream.zig");
    _ = @import("storage/wal/types.zig");
}

const std = @import("std");
const builtin = @import("builtin");
const assert = @import("core/assert.zig").assert;
const stdx = @import("core/stdx.zig");

test {
    var arena_instance = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena_instance.deinit();
    const arena = arena_instance.allocator();

    // build.zig runs this in the root dir.
    var src_dir = try std.fs.cwd().openDir("src", .{
        .access_sub_paths = true,
        .iterate = true,
    });

    var unit_tests_contents = std.ArrayList(u8).init(arena);
    try unit_tests_contents.writer().writeAll("//! Comprehensive unit test runner for KausalDB components\n//! Automatically discovers and includes all .zig files with tests\n\ncomptime {\n");

    for (try unit_test_files(arena, src_dir)) |unit_test_file| {
        try unit_tests_contents.writer().print("    _ = @import(\"{s}\");\n", .{unit_test_file});
    }

    try unit_tests_contents.writer().writeAll("}\n");

    assert(std.mem.eql(u8, @src().file, "unit_tests.zig"));
    const unit_tests_contents_disk = try src_dir.readFileAlloc(
        arena,
        @src().file,
        1024 * 1024, // 1MB should be sufficient
    );
    assert(std.mem.startsWith(u8, unit_tests_contents_disk, "//! Comprehensive unit test runner"));
    assert(std.mem.endsWith(u8, unit_tests_contents.items, "}\n"));

    const unit_tests_needs_update = !std.mem.startsWith(
        u8,
        unit_tests_contents_disk,
        unit_tests_contents.items,
    );

    if (unit_tests_needs_update) {
        if (std.process.hasEnvVarConstant("SNAP_UPDATE")) {
            const includes_end = std.mem.indexOf(u8, unit_tests_contents_disk, "}\n").? + 2;

            // Add the rest of the real file on disk to the generated in-memory file.
            try unit_tests_contents.writer().writeAll(unit_tests_contents_disk[includes_end..]);
            try src_dir.writeFile(.{
                .sub_path = "unit_tests.zig",
                .data = unit_tests_contents.items,
                .flags = .{ .exclusive = false, .truncate = true },
            });
        } else {
            std.debug.print("unit_tests.zig needs updating.\n", .{});
            std.debug.print(
                "Rerun with SNAP_UPDATE=1 environmental variable to update the contents.\n",
                .{},
            );
            assert(false);
        }
    }
}

fn unit_test_files(arena: std.mem.Allocator, src_dir: std.fs.Dir) ![]const []const u8 {
    // Different platforms can walk the directory in different orders. Store the paths and sort them
    // to ensure consistency.
    var result = std.ArrayList([]const u8).init(arena);

    var src_walker = try src_dir.walk(arena);
    defer src_walker.deinit();

    while (try src_walker.next()) |entry| {
        if (entry.kind != .file) continue;

        const entry_path = try arena.dupe(u8, entry.path);

        // Replace the path separator to be Unix-style, for consistency on Windows.
        // Don't use entry.path directly!
        if (builtin.os.tag == .windows) {
            std.mem.replaceScalar(u8, entry_path, '\\', '/');
        }

        if (!std.mem.endsWith(u8, entry_path, ".zig")) continue;

        if (std.mem.eql(u8, entry_path, "unit_tests.zig")) continue;
        if (std.mem.eql(u8, entry_path, "integration_tests.zig")) continue;
        if (std.mem.eql(u8, entry_path, "main.zig")) continue;

        if (std.mem.startsWith(u8, entry_path, "dev/fuzz/")) continue;

        const contents = try src_dir.readFileAlloc(arena, entry_path, 1024 * 1024); // 1MB max
        var line_iterator = std.mem.splitScalar(u8, contents, '\n');
        while (line_iterator.next()) |line| {
            const line_trimmed = std.mem.trimLeft(u8, line, " ");
            if (std.mem.startsWith(u8, line_trimmed, "test ")) {
                try result.append(entry_path);
                break;
            }
        }
    }

    std.mem.sort(
        []const u8,
        result.items,
        {},
        struct {
            fn less_than_fn(_: void, a: []const u8, b: []const u8) bool {
                return std.mem.order(u8, a, b) == .lt;
            }
        }.less_than_fn,
    );

    return result.items;
}
