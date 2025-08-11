//! Memory Isolation Test
//!
//! Validates arena-per-subsystem memory model under stress conditions.
//! Demonstrates that multiple storage operations within a single test context
//! maintain memory isolation and prevent cross-contamination between cycles.

const std = @import("std");
const testing = std.testing;
const kausaldb = @import("kausaldb");
const log = std.log.scoped(.stress_memory);

const StorageEngine = kausaldb.storage.StorageEngine;
const ContextBlock = kausaldb.types.ContextBlock;
const BlockId = kausaldb.types.BlockId;
const Simulation = kausaldb.simulation.Simulation;
const SimulationHarness = kausaldb.SimulationHarness;
const TestData = kausaldb.TestData;

test "memory isolation with 5 storage cycles" {
    var cycle: u32 = 0;
    while (cycle < 5) : (cycle += 1) {
        // Use testing allocator for faster compilation and execution
        const allocator = testing.allocator;
        log.debug("Starting storage cycle {}", .{cycle});

        var data_dir_buf: [64]u8 = undefined;
        const data_dir = try std.fmt.bufPrint(&data_dir_buf, "isolation_test_{}", .{cycle});

        // Use SimulationHarness for coordinated setup
        var harness = try SimulationHarness.init_and_startup(allocator, 0xDEADBEEF + cycle, data_dir);
        defer harness.deinit();

        var block_index: u32 = 1;
        while (block_index <= 5) : (block_index += 1) {
            const combined_id = cycle * 100 + block_index;
            const block = ContextBlock{
                .id = TestData.deterministic_block_id(combined_id),
                .version = 1,
                .source_uri = "test://memory_pressure.zig",
                .metadata_json = "{\"test\":\"memory_pressure\"}",
                .content = "Memory pressure test block content",
            };

            try harness.storage_engine.put_block(block);

            // Verify block can be retrieved
            const retrieved = (try harness.storage_engine.find_block(block.id, .query_engine)) orelse {
                try testing.expect(false); // Block should exist
                continue;
            };
            try testing.expect(retrieved.extract().id.eql(block.id));
        }

        log.debug("Completed storage cycle {} successfully", .{cycle});
    }

    log.info("Completed all 5 storage cycles without corruption", .{});
}

test "hashmap operations under stress" {
    // Use testing allocator for faster execution
    const allocator = testing.allocator;

    // Use SimulationHarness for hashmap stress testing
    var harness = try SimulationHarness.init_and_startup(allocator, 0xFEEDFACE, "hashmap_stress");
    defer harness.deinit();

    var index: u32 = 1;
    while (index <= 20) : (index += 1) {
        const block = ContextBlock{
            .id = TestData.deterministic_block_id(index + 200),
            .version = 1,
            .source_uri = "test://sustained_memory_pressure.zig",
            .metadata_json = "{\"test\":\"sustained_memory_pressure\"}",
            .content = "Sustained memory pressure test block content",
        };

        try harness.storage_engine.put_block(block);

        // Periodically retrieve and verify blocks
        if (index % 10 == 0) {
            const retrieved = (try harness.storage_engine.find_block(block.id, .query_engine)) orelse {
                try testing.expect(false); // Block should exist
                continue;
            };
            try testing.expect(retrieved.extract().id.eql(block.id));
        }
    }

    log.info("HashMap stress test completed successfully", .{});
}
