//! Example migration showing how to refactor fault injection tests
//! to use the new standardized harness and scenario framework.
//!
//! This demonstrates the transformation from manual setup to declarative scenarios
//! following KausalDB's explicitness-over-magic philosophy.

const std = @import("std");
const testing = std.testing;
const kausaldb = @import("kausaldb");

// ========== OLD PATTERN (before refactoring) ==========

// test "old style manual WAL durability fault injection" {
//     const allocator = testing.allocator;
//
//     // Manual VFS setup with boilerplate
//     var sim_vfs = try kausaldb.SimulationVFS.init_with_fault_seed(allocator, 12345);
//     defer sim_vfs.deinit();
//
//     const vfs_interface = sim_vfs.vfs();
//
//     // Manual storage engine setup
//     var storage_engine = try kausaldb.StorageEngine.init_default(allocator, vfs_interface, "wal_flush_test");
//     defer storage_engine.deinit();
//
//     try storage_engine.startup();
//
//     // Manual test data creation with custom helper functions
//     const initial_blocks = 50;
//     for (0..initial_blocks) |i| {
//         const block = try create_test_block(@intCast(i), allocator); // Custom helper
//         defer cleanup_test_block(block, allocator); // Custom cleanup
//         try storage_engine.put_block(block);
//     }
//
//     // Manual fault injection configuration
//     sim_vfs.enable_io_failures(300, .{ .sync = true, .write = true });
//
//     // Manual fault injection operations with repetitive error handling
//     const fault_injection_blocks = 30;
//     var successful_writes: u32 = 0;
//     for (initial_blocks..initial_blocks + fault_injection_blocks) |i| {
//         const block = try create_test_block(@intCast(i), allocator);
//         defer cleanup_test_block(block, allocator);
//
//         if (storage_engine.put_block(block)) |_| {
//             successful_writes += 1;
//         } else |err| {
//             try testing.expect(err == kausaldb.vfs.VFSError.IoError);
//         }
//     }
//
//     // Manual recovery setup
//     sim_vfs.enable_io_failures(0, .{ .sync = true, .write = true });
//     var recovered_engine = try kausaldb.StorageEngine.init_default(allocator, vfs_interface, "wal_flush_test");
//     defer recovered_engine.deinit();
//     try recovered_engine.startup();
//
//     // Manual validation with custom logic
//     var recovered_blocks: u32 = 0;
//     for (0..initial_blocks) |i| {
//         const id = create_deterministic_id(@intCast(i)); // Another custom helper
//         const result = recovered_engine.find_block(id);
//         if (result) |maybe_block| {
//             if (maybe_block != null) recovered_blocks += 1;
//         } else |_| {}
//     }
//
//     // Manual survival rate calculation and validation
//     const survival_rate = @as(f32, @floatFromInt(recovered_blocks)) / @as(f32, @floatFromInt(initial_blocks));
//     try testing.expect(survival_rate >= 0.8);
// }

// ========== NEW PATTERN (after refactoring) ==========

/// Modern approach using declarative scenario framework
test "systematic WAL durability fault injection using scenarios" {
    const allocator = testing.allocator;
    
    // Execute predefined scenario - all parameters explicit and discoverable
    try kausaldb.scenarios.execute_wal_durability_scenario(allocator, 0); // "I/O failure during flush operations"
}

/// Individual scenario execution for targeted testing
test "specific torn write scenario for WAL operations" {
    const allocator = testing.allocator;
    
    // Execute specific scenario by index - clear and explicit
    try kausaldb.scenarios.execute_wal_durability_scenario(allocator, 2); // "Torn writes during WAL entry serialization"  
}

/// Custom scenario execution for specialized testing
test "custom scenario with specific parameters" {
    const allocator = testing.allocator;
    
    // Create custom scenario with explicit parameters
    const custom_scenario = kausaldb.FaultScenario{
        .description = "High-intensity write failures for stress testing",
        .seed = 0x12345678,
        .fault_type = .write_failures,
        .initial_blocks = 500, // More blocks for stress testing
        .fault_operations = 200,
        .expected_recovery_success = true,
        .expected_min_survival_rate = 0.75, // Slightly lower expectation for high-intensity test
        .golden_master_file = null,
    };
    
    const executor = kausaldb.ScenarioExecutor.init(allocator, custom_scenario);
    try executor.execute();
}

/// Comprehensive testing using scenario batches
test "comprehensive WAL durability validation" {
    const allocator = testing.allocator;
    
    // Execute all predefined WAL scenarios for comprehensive testing
    try kausaldb.scenarios.execute_all_wal_scenarios(allocator);
}

// ========== HARNESS USAGE EXAMPLES ==========

/// Example using storage harness for basic operations
test "storage harness usage example" {
    const allocator = testing.allocator;
    
    // Simple storage testing with standardized setup
    var harness = try kausaldb.StorageHarness.init_and_startup(allocator, "example_storage_test");
    defer harness.deinit(); // O(1) cleanup via arena
    
    // Use standardized test data creation
    const block = try kausaldb.TestData.create_test_block(allocator, 1);
    defer kausaldb.TestData.cleanup_test_block(allocator, block);
    
    try harness.storage_engine.put_block(block);
    const retrieved = try harness.storage_engine.find_block(block.id);
    try testing.expect(retrieved != null);
}

/// Example using query harness for graph operations
test "query harness usage example" {
    const allocator = testing.allocator;
    
    // Query testing with storage + query engine coordination
    var harness = try kausaldb.QueryHarness.init_and_startup(allocator, "example_query_test");
    defer harness.deinit(); // O(1) cleanup via arena
    
    // Create test data with relationships
    const block1 = try kausaldb.TestData.create_test_block(allocator, 1);
    defer kausaldb.TestData.cleanup_test_block(allocator, block1);
    const block2 = try kausaldb.TestData.create_test_block(allocator, 2);
    defer kausaldb.TestData.cleanup_test_block(allocator, block2);
    
    try harness.storage_engine().put_block(block1);
    try harness.storage_engine().put_block(block2);
    
    const edge = kausaldb.TestData.create_test_edge(1, 2, .calls);
    try harness.storage_engine().put_edge(edge);
    
    // Query using standardized harness
    const found_block = try harness.query_engine.find_block(block1.id);
    try testing.expect(found_block != null);
}

/// Example using simulation harness for integration testing
test "simulation harness usage example" {
    const allocator = testing.allocator;
    
    // Full simulation testing with deterministic seed
    var harness = try kausaldb.SimulationHarness.init_and_startup(allocator, 0xDEADBEEF, "example_simulation_test");
    defer harness.deinit(); // O(1) cleanup via arena
    
    // Create test data
    const block = try kausaldb.TestData.create_test_block(allocator, 42);
    defer kausaldb.TestData.cleanup_test_block(allocator, block);
    
    try harness.storage_engine.put_block(block);
    
    // Advance simulation time for deterministic testing
    harness.tick(10);
    
    const retrieved = try harness.query_engine.find_block(block.id);
    try testing.expect(retrieved != null);
}

// ========== KEY IMPROVEMENTS DEMONSTRATED ==========

// 1. **Explicitness Over Magic**: All parameters visible in scenario definitions
// 2. **Reusable Infrastructure**: Standardized harnesses eliminate setup boilerplate  
// 3. **Deterministic Testing**: Seeds ensure reproducible test behavior
// 4. **Arena-per-Subsystem**: O(1) cleanup via harness.deinit()
// 5. **Pure Coordinator Pattern**: Harnesses orchestrate without owning state
// 6. **Two-Phase Initialization**: Clear init() then startup() separation
// 7. **Systematic Fault Injection**: Declarative scenarios vs manual setup
// 8. **Golden Master Integration**: Optional state verification capabilities
// 9. **Comprehensive Coverage**: Batch execution of related scenarios
// 10. **Performance**: Standardized test data creation and cleanup patterns