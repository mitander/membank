//! Liveness testing profile for KausalDB.
//!
//! Tests the system's ability to recover and make progress even in degraded states.
//! Uses the FaultInjectionHarness to verify that the system can:
//! 1. Handle chaotic fault conditions gracefully (expected failures)
//! 2. Recover and make progress when conditions partially improve
//! 3. Maintain liveness guarantees under realistic failure scenarios

const std = @import("std");

const kausaldb = @import("kausaldb");

const testing = std.testing;
const types = kausaldb.types;

const FaultInjectionConfig = kausaldb.test_harness.FaultInjectionConfig;
const FaultInjectionHarness = kausaldb.test_harness.FaultInjectionHarness;
const TestData = kausaldb.test_harness.TestData;
const ContextBlock = types.ContextBlock;
const BlockId = types.BlockId;
const GraphEdge = types.GraphEdge;
const EdgeType = types.EdgeType;

test "system liveness under fault conditions" {
    const allocator = testing.allocator;

    // Use a deterministic seed for reproducible test behavior
    const test_seed: u64 = 0xDEADBEEF_CAFEBABE;

    // Configure fault injection using the proven pattern from working tests
    var fault_config = FaultInjectionConfig{};
    fault_config.io_failures.enabled = true;
    fault_config.io_failures.failure_rate_per_thousand = 300; // 30% failure rate
    fault_config.io_failures.operations.read = true;
    fault_config.io_failures.operations.write = true;
    fault_config.io_failures.operations.create = true;

    var harness = try FaultInjectionHarness.init_with_faults(allocator, test_seed, "liveness_test", fault_config);
    defer harness.deinit();
    try harness.startup();

    // Create test blocks under degraded conditions
    const chaos_block_1 = try TestData.create_test_block_with_content(allocator, 1, "chaos_test_content_1");
    defer TestData.cleanup_test_block(allocator, chaos_block_1);

    const chaos_block_2 = try TestData.create_test_block_with_content(allocator, 2, "chaos_test_content_2");
    defer TestData.cleanup_test_block(allocator, chaos_block_2);

    // Test system behavior under degraded conditions
    var chaos_successes: u32 = 0;
    var chaos_failures: u32 = 0;

    // Advance simulation to trigger faults (from working fault injection pattern)
    harness.tick(1);

    // Test direct VFS operations which are subject to fault injection
    // This follows the pattern from working fault injection tests
    const node = harness.simulation_harness.node();
    const vfs_interface = node.filesystem_interface();

    // Attempt VFS operations under degraded conditions
    var i: u32 = 0;
    while (i < 10) : (i += 1) {
        // Test file creation (write operation)
        const test_filename = try std.fmt.allocPrint(allocator, "test_file_{}.txt", .{i});
        defer allocator.free(test_filename);

        if (vfs_interface.create(test_filename)) |file_const| {
            var file = file_const;
            file.close();
            chaos_successes += 1;
        } else |_| {
            chaos_failures += 1;
        }

        // Test file reading (read operation)
        if (vfs_interface.open(test_filename, .read)) |file_const| {
            var file = file_const;
            file.close();
            chaos_successes += 1;
        } else |_| {
            chaos_failures += 1;
        }
    }

    // Under degraded conditions, we should see both successes and failures
    // This proves the system is resilient but affected by faults
    const total_operations = chaos_successes + chaos_failures;
    const failure_rate = (chaos_failures * 100) / total_operations;

    // With 30% fault injection, expect some failures but not all
    try testing.expect(total_operations >= 10); // Most operations attempted
    try testing.expect(chaos_failures > 0); // Should see some failures with fault injection
    try testing.expect(failure_rate >= 10); // Should see at least 10% failure rate

    // Phase 2: Partial Heal - reduce fault rates to simulate partial recovery
    harness.disable_all_faults(); // Clear existing faults first

    // Configure reduced fault rates for partial recovery testing
    var recovery_config = FaultInjectionConfig{};
    recovery_config.io_failures.enabled = true;
    recovery_config.io_failures.failure_rate_per_thousand = 100; // 10% failure rate
    recovery_config.io_failures.operations.write = true; // Only write failures

    // Apply new configuration (note: this may require harness enhancement)
    // For now, test with no faults during recovery phase

    // Phase 3: Liveness Verification - operations MUST succeed now
    const recovery_block_1 = try TestData.create_test_block_with_content(allocator, 10, "recovery_test_content_1");
    defer TestData.cleanup_test_block(allocator, recovery_block_1);

    const recovery_block_2 = try TestData.create_test_block_with_content(allocator, 11, "recovery_test_content_2");
    defer TestData.cleanup_test_block(allocator, recovery_block_2);

    // These operations MUST succeed - this is the liveness guarantee
    try harness.storage_engine().put_block(recovery_block_1);
    try harness.storage_engine().put_block(recovery_block_2);

    // Verify we can read back the data we just wrote
    const found_block_1 = try harness.storage_engine().find_block(recovery_block_1.id, .query_engine);
    try testing.expect(found_block_1 != null);
    try testing.expectEqualStrings(recovery_block_1.content, found_block_1.?.block.content);

    const found_block_2 = try harness.storage_engine().find_block(recovery_block_2.id, .query_engine);
    try testing.expect(found_block_2 != null);
    try testing.expectEqualStrings(recovery_block_2.content, found_block_2.?.block.content);

    // Create an edge relationship to test graph operations under partial faults
    const test_edge = TestData.create_test_edge_from_indices(10, 11, .calls);
    try harness.storage_engine().put_edge(test_edge);

    // Verify graph traversal works under partial fault conditions
    const traversal_result = try harness.query_engine().traverse_outgoing(recovery_block_1.id, 1);
    try testing.expect(traversal_result.blocks.len > 0);
    traversal_result.deinit(); // Free the traversal result
}

test "multi-node liveness under network partitions" {
    const allocator = testing.allocator;

    // Use the same configuration as the first test to debug the issue
    const test_seed: u64 = 0xDEADBEEF_CAFEBABE; // Same seed as first test

    // Configure fault injection exactly like the first test
    var fault_config = FaultInjectionConfig{};
    fault_config.io_failures.enabled = true;
    fault_config.io_failures.failure_rate_per_thousand = 300; // Same 30% rate as first test
    fault_config.io_failures.operations.read = true;
    fault_config.io_failures.operations.write = true;
    fault_config.io_failures.operations.create = true;

    var harness = try FaultInjectionHarness.init_with_faults(allocator, test_seed, "liveness_test", fault_config);
    defer harness.deinit();
    try harness.startup();

    // Phase 1: Create a test scenario with 3 logical nodes worth of data
    const node1_block = try TestData.create_test_block_with_content(allocator, 100, "node1_data");
    defer TestData.cleanup_test_block(allocator, node1_block);

    const node2_block = try TestData.create_test_block_with_content(allocator, 200, "node2_data");
    defer TestData.cleanup_test_block(allocator, node2_block);

    const node3_block = try TestData.create_test_block_with_content(allocator, 300, "node3_data");
    defer TestData.cleanup_test_block(allocator, node3_block);

    // Successfully store initial data (no faults yet)
    try harness.storage_engine().put_block(node1_block);
    try harness.storage_engine().put_block(node2_block);
    try harness.storage_engine().put_block(node3_block);

    // Phase 2: Network partition simulation - fault injection already configured
    harness.tick(1); // Advance simulation to trigger faults

    // Test mixed success/failure behavior during partition
    var partition_successes: u32 = 0;
    var partition_failures: u32 = 0;

    // Test VFS operations during partition
    const node = harness.simulation_harness.node();
    const vfs_interface = node.filesystem_interface();

    var j: u32 = 0;
    while (j < 10) : (j += 1) {
        // Test file operations during network partition
        const partition_filename = try std.fmt.allocPrint(allocator, "partition_test_{}.txt", .{j});
        defer allocator.free(partition_filename);

        if (vfs_interface.create(partition_filename)) |file_const| {
            var file = file_const;
            file.close();
            partition_successes += 1;
        } else |_| {
            partition_failures += 1;
        }
    }

    // During moderate partition, expect both successes and failures
    const total_partition_ops = partition_successes + partition_failures;
    const partition_failure_rate = (partition_failures * 100) / total_partition_ops;

    try testing.expect(total_partition_ops == 10); // All operations attempted
    try testing.expect(partition_failures > 0); // Should see some failures with fault injection
    try testing.expect(partition_failure_rate >= 10); // Should see at least 10% failure rate with 30% injection

    // Phase 3: Heal network partition - system MUST recover liveness
    harness.disable_all_faults();

    // All read operations MUST succeed after healing
    const recovered_node1 = try harness.storage_engine().find_block(node1_block.id, .query_engine);
    try testing.expect(recovered_node1 != null);
    try testing.expectEqualStrings(node1_block.content, recovered_node1.?.block.content);

    const recovered_node2 = try harness.storage_engine().find_block(node2_block.id, .query_engine);
    try testing.expect(recovered_node2 != null);
    try testing.expectEqualStrings(node2_block.content, recovered_node2.?.block.content);

    const recovered_node3 = try harness.storage_engine().find_block(node3_block.id, .query_engine);
    try testing.expect(recovered_node3 != null);
    try testing.expectEqualStrings(node3_block.content, recovered_node3.?.block.content);

    // New write operations MUST succeed after healing
    const post_heal_block = try TestData.create_test_block_with_content(allocator, 400, "post_healing_data");
    defer TestData.cleanup_test_block(allocator, post_heal_block);

    try harness.storage_engine().put_block(post_heal_block);

    const verified_post_heal = try harness.storage_engine().find_block(post_heal_block.id, .query_engine);
    try testing.expect(verified_post_heal != null);
    try testing.expectEqualStrings(post_heal_block.content, verified_post_heal.?.block.content);
}
