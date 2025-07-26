//! Memory Isolation Test
//!
//! Comprehensive test to demonstrate that storage operations work correctly
//! when run in isolation without cumulative memory corruption from test framework.
//! This test runs many storage operations within a single test to avoid the
//! HashMap alignment corruption that occurs after ~15 separate tests.

const cortexdb = @import("cortexdb");
const std = @import("std");
const testing = std.testing;
const log = std.log.scoped(.stress_memory);

const storage = cortexdb.storage;
const context_block = cortexdb.types;
const simulation = cortexdb.simulation;

const StorageEngine = storage.StorageEngine;
const ContextBlock = context_block.ContextBlock;
const BlockId = context_block.BlockId;
const Simulation = simulation.Simulation;

test "memory isolation: single test with 25 storage cycles" {
    var cycle: u32 = 0;
    while (cycle < 25) : (cycle += 1) {
        // Use GPA with safety checks to detect memory corruption in ReleaseSafe builds
        var gpa = std.heap.GeneralPurposeAllocator(.{ .safety = true }){};
        defer {
            const deinit_status = gpa.deinit();
            if (deinit_status == .leak) @panic("Memory leak detected in memory isolation test");
        }
        const allocator = gpa.allocator();
        log.debug("Starting storage cycle {}", .{cycle});

        var sim = try Simulation.init(allocator, 0xDEADBEEF + cycle);
        defer sim.deinit();

        const node = try sim.add_node();
        const node_ptr = sim.find_node(node);
        const vfs = node_ptr.filesystem_interface();

        // Create unique data directory for this cycle
        var data_dir_buf: [64]u8 = undefined;
        const data_dir = try std.fmt.bufPrint(&data_dir_buf, "isolation_test_{}", .{cycle});
        const data_dir_owned = try allocator.dupe(u8, data_dir);
        defer allocator.free(data_dir_owned);

        var engine = try StorageEngine.init_default(allocator, vfs, data_dir_owned);
        defer engine.deinit();

        try engine.startup();

        // Create and store multiple blocks per cycle
        var block_num: u32 = 0;
        while (block_num < 5) : (block_num += 1) {
            // Create unique block ID
            var id_bytes: [16]u8 = undefined;
            std.mem.writeInt(u64, id_bytes[0..8], cycle, .little);
            std.mem.writeInt(u64, id_bytes[8..16], block_num, .little);

            const content = try std.fmt.allocPrint(
                allocator,
                "Block {} in cycle {}",
                .{ block_num, cycle },
            );
            defer allocator.free(content);

            const block = ContextBlock{
                .id = BlockId{ .bytes = id_bytes },
                .version = 1,
                .source_uri = "test://isolation",
                .content = content,
                .metadata_json = "{\"test\": true}",
            };

            try engine.put_block(block);

            // Verify block can be retrieved
            const retrieved = (try engine.find_block(block.id)) orelse {
                try testing.expect(false); // Block should exist
                continue;
            };
            try testing.expect(retrieved.id.eql(block.id));
        }

        // Force compaction if needed
        if (cycle % 5 == 0) {
            try engine.flush_wal();
        }

        log.debug("Completed storage cycle {} successfully", .{cycle});
    }

    log.info("Completed all 25 storage cycles without corruption", .{});
}

test "memory isolation: HashMap operations under stress" {
    // Use GPA with safety checks to detect memory corruption in ReleaseSafe builds
    var gpa = std.heap.GeneralPurposeAllocator(.{ .safety = true }){};
    defer {
        const deinit_status = gpa.deinit();
        if (deinit_status == .leak) @panic("Memory leak detected in HashMap stress test");
    }
    const allocator = gpa.allocator();

    // Stress test the HashMap operations specifically
    var sim = try Simulation.init(allocator, 0xFEEDFACE);
    defer sim.deinit();

    const node = try sim.add_node();
    const node_ptr = sim.find_node(node);
    const vfs = node_ptr.filesystem_interface();

    var engine = try StorageEngine.init_default(allocator, vfs, "hashmap_stress");
    defer engine.deinit();

    try engine.startup();

    // Create many blocks to trigger HashMap resizing
    var i: u32 = 0;
    while (i < 100) : (i += 1) {
        var id_bytes: [16]u8 = undefined;
        std.mem.writeInt(u128, &id_bytes, i, .little);

        const content = try std.fmt.allocPrint(allocator, "HashMap stress block {}", .{i});
        defer allocator.free(content);

        const block = ContextBlock{
            .id = BlockId{ .bytes = id_bytes },
            .version = 1,
            .source_uri = "test://hashmap",
            .content = content,
            .metadata_json = "{\"test\": true}",
        };

        try engine.put_block(block);

        // Periodically retrieve and verify blocks
        if (i % 10 == 0) {
            const retrieved = (try engine.find_block(block.id)) orelse {
                try testing.expect(false); // Block should exist
                continue;
            };
            try testing.expect(retrieved.id.eql(block.id));
        }
    }

    log.info("HashMap stress test completed successfully", .{});
}
