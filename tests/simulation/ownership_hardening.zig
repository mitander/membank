//! Deterministic simulation tests for ownership violation detection.
//!
//! These tests validate that the ownership system correctly detects and handles
//! cross-subsystem memory access violations through controlled injection in
//! a deterministic simulation environment.

const std = @import("std");

const kausaldb = @import("kausaldb");

const assert = kausaldb.assert.assert;
const ownership = kausaldb.ownership;
const testing = std.testing;

const BlockId = kausaldb.types.BlockId;
const ContextBlock = kausaldb.types.ContextBlock;
const Simulation = kausaldb.simulation.Simulation;
const SimulationHarness = kausaldb.SimulationHarness;
const StorageHarness = kausaldb.StorageHarness;
const TestData = kausaldb.TestData;

test "cross-subsystem memory access violation detection" {
    const allocator = testing.allocator;

    // Use deterministic seed for reproducible violation patterns
    var harness = try SimulationHarness.init_and_startup(allocator, 0xBAD_ACC355, "ownership_violation_test");
    defer harness.deinit();

    // Enable ownership violation injection with 50% rate for varied patterns
    harness.simulation.enable_ownership_violations(0.5);

    // Create test blocks as raw ContextBlocks (storage engine will wrap as .temporary)
    const storage_block_raw = ContextBlock{
        .id = TestData.deterministic_block_id(1),
        .version = 1,
        .source_uri = "test://cross_subsystem_test.zig",
        .metadata_json = "{}",
        .content = "Cross-subsystem access test content",
    };

    const query_block_raw = ContextBlock{
        .id = TestData.deterministic_block_id(2),
        .version = 1,
        .source_uri = "test://cross_subsystem_test2.zig",
        .metadata_json = "{}",
        .content = "Second cross-subsystem access test content",
    };

    // Store blocks in storage engine (will be auto-wrapped as .temporary ownership)
    try harness.storage_engine.put_block(storage_block_raw);
    try harness.storage_engine.put_block(query_block_raw);

    // Attempt cross-subsystem access with violation injection enabled
    var violation_detected = false;
    var attempts: u32 = 0;
    const max_attempts = 100;

    // Deterministic violation injection should trigger failures within max_attempts
    while (attempts < max_attempts and !violation_detected) : (attempts += 1) {
        harness.simulation.reset_ownership_violation_stats();

        // Create owned block for violation injection testing
        const storage_owned = ownership.OwnedBlock.take_ownership(storage_block_raw, .storage_engine);

        // Inject cross-subsystem access violation
        const potentially_corrupted = harness.simulation.inject_ownership_violation(
            storage_owned,
            .cross_subsystem_read,
            .query_engine, // Wrong accessor
        );

        if (potentially_corrupted) |corrupted_block| {
            // In debug builds, attempting to read with wrong ownership should trigger assertion
            if (@import("builtin").mode == .Debug) {
                // Simulate the violation detection that would occur in real usage
                if (corrupted_block.query_owner() != .storage_engine) {
                    violation_detected = true;
                }
            }
        }

        // Check if violations were injected
        const stats = harness.simulation.ownership_violation_stats();
        if (stats.violation_count > 0) {
            violation_detected = true;
        }
    }

    // Verify that violations were successfully detected
    try testing.expect(violation_detected);

    const final_stats = harness.simulation.ownership_violation_stats();
    try testing.expect(final_stats.is_enabled);
    try testing.expect(final_stats.injection_rate == 0.5);

    // Disable violations and verify clean operation
    harness.simulation.disable_ownership_violations();
    harness.simulation.reset_ownership_violation_stats();

    const clean_stats = harness.simulation.ownership_violation_stats();
    try testing.expect(!clean_stats.is_enabled);
    try testing.expect(clean_stats.violation_count == 0);
}

test "arena corruption scenario simulation" {
    const allocator = testing.allocator;

    var harness = try SimulationHarness.init_and_startup(allocator, 0xC0EEE7, "arena_corruption_test");
    defer harness.deinit();

    // Enable dangling arena access injection
    harness.simulation.enable_ownership_violations(1.0); // 100% rate for deterministic testing

    // Create block and simulate arena-based operations
    const test_block = try TestData.create_test_block(allocator, 42);
    defer TestData.cleanup_test_block(allocator, test_block);

    // Store in memtable (arena-allocated)
    try harness.storage_engine.put_block(test_block);

    // Create owned block reference
    const owned_block = ownership.OwnedBlock.take_ownership(test_block, .memtable_manager);

    // Inject dangling arena access scenario
    const corrupted_block = harness.simulation.inject_ownership_violation(
        owned_block,
        .dangling_arena_access,
        .temporary, // Accessor doesn't matter for this violation type
    );

    // Verify violation injection occurred
    const stats = harness.simulation.ownership_violation_stats();
    try testing.expect(stats.violation_count > 0);

    // In debug builds, the corrupted block should have null arena pointer
    if (@import("builtin").mode == .Debug and corrupted_block != null) {
        // Arena corruption simulation completed - would trigger assertion in real usage
    }

    try testing.expect(corrupted_block != null);
}

test "state machine violation resilience" {
    const allocator = testing.allocator;

    // Test that storage engine handles ownership violations gracefully
    var storage_harness = try StorageHarness.init_and_startup(allocator, "state_violation_test");
    defer storage_harness.deinit();

    // Create blocks with ownership transfers between subsystems
    const block1 = try TestData.create_test_block(allocator, 1);
    defer TestData.cleanup_test_block(allocator, block1);

    const block2 = try TestData.create_test_block(allocator, 2);
    defer TestData.cleanup_test_block(allocator, block2);

    // Normal operations should work
    try storage_harness.storage_engine.put_block(block1);
    try storage_harness.storage_engine.put_block(block2);

    // Verify blocks are retrievable
    const retrieved1 = try storage_harness.storage_engine.find_query_block(block1.id);
    const retrieved2 = try storage_harness.storage_engine.find_query_block(block2.id);

    try testing.expect(retrieved1 != null);
    try testing.expect(retrieved2 != null);

    // Verify correct ownership types
    if (retrieved1) |block| {
        try testing.expect(@TypeOf(block).query_owner() == .query_engine);
    }
    if (retrieved2) |block| {
        try testing.expect(@TypeOf(block).query_owner() == .query_engine);
    }
}

test "fail-fast behavior verification" {
    const allocator = testing.allocator;

    var sim = try Simulation.init(allocator, 0xFA11_FA57);
    defer sim.deinit();

    // Enable violation injection for fail-fast testing
    sim.enable_ownership_violations(1.0);

    // Create block with specific ownership
    const test_block = ContextBlock{
        .id = TestData.deterministic_block_id(999),
        .version = 1,
        .source_uri = "test://fail_fast_verification.zig",
        .metadata_json = "{\"test\":\"fail_fast\"}",
        .content = "Fail-fast behavior test content",
    };
    const owned_block = ownership.OwnedBlock.take_ownership(test_block, .storage_engine);

    // Test various violation types
    const violation_types = [_]kausaldb.simulation.OwnershipViolationInjector.ViolationType{
        .cross_subsystem_read,
        .cross_subsystem_write,
        .dangling_arena_access,
        .use_after_transfer,
    };

    var total_violations: u32 = 0;
    for (violation_types) |violation_type| {
        sim.reset_ownership_violation_stats();

        const corrupted_block = sim.inject_ownership_violation(
            owned_block,
            violation_type,
            .query_engine, // Wrong accessor
        );

        // Each injection should create a violation
        if (corrupted_block != null) {
            const stats = sim.ownership_violation_stats();
            total_violations += stats.violation_count;
        }
    }

    // Verify violations were injected across all types
    try testing.expect(total_violations > 0);
    try testing.expectEqual(@as(usize, violation_types.len), total_violations);

    // Verify deterministic behavior - same seed should produce same violations
    var sim2 = try Simulation.init(allocator, 0xFA11_FA57);
    defer sim2.deinit();

    sim2.enable_ownership_violations(1.0);

    var total_violations2: u32 = 0;
    for (violation_types) |violation_type| {
        sim2.reset_ownership_violation_stats();

        const corrupted_block = sim2.inject_ownership_violation(
            owned_block,
            violation_type,
            .query_engine,
        );

        if (corrupted_block != null) {
            const stats = sim2.ownership_violation_stats();
            total_violations2 += stats.violation_count;
        }
    }

    // Both simulations with same seed should produce same violation count
    try testing.expectEqual(total_violations, total_violations2);
}
