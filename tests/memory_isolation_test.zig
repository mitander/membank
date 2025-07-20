//! Memory Isolation Test
//!
//! Comprehensive test to demonstrate that storage operations work correctly
//! when run in isolation without cumulative memory corruption from test framework.
//! This test runs many storage operations within a single test to avoid the
//! HashMap alignment corruption that occurs after ~15 separate tests.

const std = @import("std");
const testing = std.testing;

const storage = @import("storage");
const context_block = @import("context_block");
const simulation = @import("simulation");

const StorageEngine = storage.StorageEngine;
const ContextBlock = context_block.ContextBlock;
const BlockId = context_block.BlockId;
const Simulation = simulation.Simulation;

test "memory isolation: single test with 25 storage cycles" {
    // Use GeneralPurposeAllocator to avoid test framework corruption
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    // Run 25 complete storage cycles within a single test
    // This exceeds the ~15 test threshold where corruption occurs
    var cycle: u32 = 0;
    while (cycle < 25) : (cycle += 1) {
        std.log.debug("Starting storage cycle {}", .{cycle});

        var sim = try Simulation.init(allocator, 0xDEADBEEF + cycle);
        defer sim.deinit();

        const node = try sim.add_node();
        const node_ptr = sim.find_node(node);
        const vfs = node_ptr.filesystem_interface();

        // Create unique data directory for this cycle
        var data_dir_buf: [64]u8 = undefined;
        const data_dir = try std.fmt.bufPrint(&data_dir_buf, "isolation_test_{}", .{cycle});
        const data_dir_owned = try allocator.dupe(u8, data_dir);
        // Note: StorageEngine will take ownership and free this in deinit()

        var engine = try StorageEngine.init(allocator, vfs, data_dir_owned);
        defer engine.deinit();

        try engine.initialize_storage();

        // Create and store multiple blocks per cycle
        var block_num: u32 = 0;
        while (block_num < 5) : (block_num += 1) {
            // Create unique block ID
            var id_bytes: [16]u8 = undefined;
            std.mem.writeInt(u64, id_bytes[0..8], cycle, .little);
            std.mem.writeInt(u64, id_bytes[8..16], block_num, .little);

            const block = ContextBlock{
                .id = BlockId{ .bytes = id_bytes },
                .version = 1,
                .source_uri = try allocator.dupe(u8, "test://isolation"),
                .content = try std.fmt.allocPrint(
                    allocator,
                    "Block {} in cycle {}",
                    .{ block_num, cycle },
                ),
                .metadata_json = try allocator.dupe(u8, "{\"test\": true}"),
            };

            try engine.put_block(block);

            // Verify block can be retrieved
            const retrieved = try engine.find_block_by_id(block.id);
            try testing.expect(retrieved.id.eql(block.id));
        }

        // Force compaction if needed
        if (cycle % 5 == 0) {
            try engine.flush_wal();
        }

        std.log.debug("Completed storage cycle {} successfully", .{cycle});
    }

    std.log.info("Completed all 25 storage cycles without corruption", .{});
}

test "memory isolation: HashMap operations under stress" {
    // Use GeneralPurposeAllocator to avoid test framework corruption
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    // Stress test the HashMap operations specifically
    var sim = try Simulation.init(allocator, 0xFEEDFACE);
    defer sim.deinit();

    const node = try sim.add_node();
    const node_ptr = sim.find_node(node);
    const vfs = node_ptr.filesystem_interface();

    const data_dir = try allocator.dupe(u8, "hashmap_stress");
    // Note: StorageEngine will take ownership and free this in deinit()

    var engine = try StorageEngine.init(allocator, vfs, data_dir);
    defer engine.deinit();

    try engine.initialize_storage();

    // Create many blocks to trigger HashMap resizing
    var i: u32 = 0;
    while (i < 100) : (i += 1) {
        var id_bytes: [16]u8 = undefined;
        std.mem.writeInt(u128, &id_bytes, i, .little);

        const block = ContextBlock{
            .id = BlockId{ .bytes = id_bytes },
            .version = 1,
            .source_uri = try allocator.dupe(u8, "test://hashmap"),
            .content = try std.fmt.allocPrint(allocator, "HashMap stress block {}", .{i}),
            .metadata_json = try allocator.dupe(u8, "{\"test\": true}"),
        };

        try engine.put_block(block);

        // Periodically retrieve and verify blocks
        if (i % 10 == 0) {
            const retrieved = try engine.find_block_by_id(block.id);
            try testing.expect(retrieved.id.eql(block.id));
        }
    }

    std.log.info("HashMap stress test completed successfully", .{});
}
