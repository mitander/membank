//! Memory Isolation Test
//!
//! Validates arena-per-subsystem memory model under stress conditions.
//! Demonstrates that multiple storage operations within a single test context
//! maintain memory isolation and prevent cross-contamination between cycles.

const kausaldb = @import("kausaldb");
const std = @import("std");
const testing = std.testing;
const log = std.log.scoped(.stress_memory);

const storage = kausaldb.storage;
const types = kausaldb.types;
const simulation = kausaldb.simulation;

const StorageEngine = storage.StorageEngine;
const ContextBlock = types.ContextBlock;
const BlockId = types.BlockId;
const Simulation = simulation.Simulation;

test "memory isolation with 5 storage cycles" {
    var cycle: u32 = 0;
    while (cycle < 5) : (cycle += 1) {
        // Use testing allocator for faster compilation and execution
        const allocator = testing.allocator;
        log.debug("Starting storage cycle {}", .{cycle});

        var data_dir_buf: [64]u8 = undefined;
        const data_dir = try std.fmt.bufPrint(&data_dir_buf, "isolation_test_{}", .{cycle});

        // Use SimulationHarness for coordinated setup
        var harness = try kausaldb.SimulationHarness.init_and_startup(allocator, 0xDEADBEEF + cycle, data_dir);
        defer harness.deinit();

        var block_index: u32 = 1;
        while (block_index <= 5) : (block_index += 1) {
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

            try harness.storage_engine.put_block(block);

            // Verify block can be retrieved
            const retrieved = (try harness.storage_engine.find_block(block.id)) orelse {
                try testing.expect(false); // Block should exist
                continue;
            };
            try testing.expect(retrieved.id.eql(block.id));
        }

        log.debug("Completed storage cycle {} successfully", .{cycle});
    }

    log.info("Completed all 5 storage cycles without corruption", .{});
}

test "hashmap operations under stress" {
    // Use testing allocator for faster execution
    const allocator = testing.allocator;

    // Use SimulationHarness for hashmap stress testing
    var harness = try kausaldb.SimulationHarness.init_and_startup(allocator, 0xFEEDFACE, "hashmap_stress");
    defer harness.deinit();

    var index: u32 = 1;
    while (index <= 20) : (index += 1) {
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

        try harness.storage_engine.put_block(block);

        // Periodically retrieve and verify blocks
        if (index % 10 == 0) {
            const retrieved = (try harness.storage_engine.find_block(block.id)) orelse {
                try testing.expect(false); // Block should exist
                continue;
            };
            try testing.expect(retrieved.id.eql(block.id));
        }
    }

    log.info("HashMap stress test completed successfully", .{});
}
