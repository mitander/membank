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

        // Create and store multiple blocks per cycle (start from 1, all-zero BlockID invalid)
        var block_index: u32 = 1;
        while (block_index <= 5) : (block_index += 1) {
            // Create unique block ID
            const combined_id = cycle * 100 + block_index;
            const block_id_hex = try std.fmt.allocPrint(allocator, "{:0>32}", .{combined_id});
            defer allocator.free(block_id_hex);

            const content = try std.fmt.allocPrint(
                allocator,
                "Block {} in cycle {}",
                .{ block_index, cycle },
            );
            defer allocator.free(content);

            const block = ContextBlock{
                .id = try BlockId.from_hex(block_id_hex),
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

    // Create many blocks to trigger HashMap resizing (start from 1, all-zero BlockID invalid)
    var index: u32 = 1;
    while (index <= 100) : (index += 1) {
        const block_id_hex = try std.fmt.allocPrint(allocator, "{:0>32}", .{index});
        defer allocator.free(block_id_hex);

        const content = try std.fmt.allocPrint(allocator, "HashMap stress block {}", .{index});
        defer allocator.free(content);

        const block = ContextBlock{
            .id = try BlockId.from_hex(block_id_hex),
            .version = 1,
            .source_uri = "test://hashmap",
            .content = content,
            .metadata_json = "{\"test\": true}",
        };

        try engine.put_block(block);

        // Periodically retrieve and verify blocks
        if (index % 10 == 0) {
            const retrieved = (try engine.find_block(block.id)) orelse {
                try testing.expect(false); // Block should exist
                continue;
            };
            try testing.expect(retrieved.id.eql(block.id));
        }
    }

    log.info("HashMap stress test completed successfully", .{});
}
