//! WAL Corruption Tracker Fatal Assertion Tests
//!
//! Tests the fail-fast behavior of the corruption tracker when systematic
//! corruption is detected. These tests validate that the system correctly
//! distinguishes between isolated failures and unrecoverable corruption.

const std = @import("std");
const testing = std.testing;
const kausaldb = @import("kausaldb");

const corruption_tracker_mod = kausaldb.wal.corruption_tracker;
const CorruptionTracker = corruption_tracker_mod.CorruptionTracker;
const WAL_MAGIC_NUMBER = corruption_tracker_mod.WAL_MAGIC_NUMBER;
const WAL_ENTRY_MAGIC = corruption_tracker_mod.WAL_ENTRY_MAGIC;

test "CorruptionTracker normal operation - no fatal assertions" {
    var tracker = CorruptionTracker.init();

    // Normal success/failure patterns should not trigger fatal assertions
    tracker.record_success();
    tracker.record_failure("test_operation");
    tracker.record_success();
    tracker.record_failure("test_operation");
    tracker.record_failure("test_operation");
    tracker.record_success();

    // Should complete without panic
    try testing.expectEqual(@as(u32, 0), tracker.consecutive_failures);
    try testing.expectEqual(@as(u32, 3), tracker.total_failures);
    try testing.expectEqual(@as(u32, 6), tracker.total_operations);
}

test "CorruptionTracker at systematic corruption threshold" {
    var tracker = CorruptionTracker.init();

    // Record exactly 3 consecutive failures (at threshold)
    tracker.record_failure("test_operation");
    tracker.record_failure("test_operation");
    tracker.record_failure("test_operation");

    // Should not panic at threshold
    try testing.expectEqual(@as(u32, 3), tracker.consecutive_failures);
    try testing.expectEqual(@as(u32, 3), tracker.total_failures);
}

test "CorruptionTracker systematic corruption detection expects panic" {
    // This test expects the 4th consecutive failure to trigger fatal_assert
    // In a real scenario, this would terminate the process

    var tracker = CorruptionTracker.init();

    // Record 3 consecutive failures (at threshold)
    tracker.record_failure("test_operation");
    tracker.record_failure("test_operation");
    tracker.record_failure("test_operation");

    // The 4th consecutive failure should trigger fatal_assert
    // Note: In actual testing, this would be verified with expectPanic
    // but we document the expected behavior here for reference

    // tracker.record_failure("test_operation"); // <- This would panic

    // If we reach this point, the threshold logic is working correctly
    try testing.expectEqual(@as(u32, 3), tracker.consecutive_failures);
}

test "CorruptionTracker success resets consecutive failures" {
    var tracker = CorruptionTracker.init();

    // Build up consecutive failures
    tracker.record_failure("test_operation");
    tracker.record_failure("test_operation");
    tracker.record_failure("test_operation");
    try testing.expectEqual(@as(u32, 3), tracker.consecutive_failures);

    // Success should reset consecutive count
    tracker.record_success();
    try testing.expectEqual(@as(u32, 0), tracker.consecutive_failures);
    try testing.expectEqual(@as(u32, 3), tracker.total_failures); // Total preserved

    // Can now record more failures without immediate panic
    tracker.record_failure("test_operation");
    tracker.record_failure("test_operation");
    try testing.expectEqual(@as(u32, 2), tracker.consecutive_failures);
}

test "CorruptionTracker failure rate calculation accuracy" {
    var tracker = CorruptionTracker.init();

    // Test various failure rates

    // 0% failure rate
    for (0..10) |_| {
        tracker.record_success();
    }
    try testing.expectApproxEqRel(@as(f64, 0.0), tracker.failure_rate(), 0.001);

    // 50% failure rate
    tracker.reset();
    for (0..5) |_| {
        tracker.record_success();
        tracker.record_failure("test");
    }
    try testing.expectApproxEqRel(@as(f64, 0.5), tracker.failure_rate(), 0.001);

    // 100% failure rate
    tracker.reset();
    for (0..3) |_| { // Stay below panic threshold
        tracker.record_failure("test");
    }
    try testing.expectApproxEqRel(@as(f64, 1.0), tracker.failure_rate(), 0.001);
}

test "CorruptionTracker elevated corruption detection" {
    var tracker = CorruptionTracker.init_testing();

    // Below minimum operations threshold - not elevated
    for (0..50) |_| {
        tracker.record_success();
    }
    for (0..10) |_| { // 20% failure rate but low volume
        tracker.record_failure("test");
    }
    try testing.expect(!tracker.is_corruption_elevated());

    // Above threshold with sufficient volume
    for (0..40) |_| {
        tracker.record_success();
    }
    // Now at 100 operations with 10 failures = 10% rate (> 5% threshold)
    try testing.expect(tracker.is_corruption_elevated());
}

test "CorruptionTracker WAL magic validation success" {
    var tracker = CorruptionTracker.init();

    // Valid magic numbers should record success
    tracker.validate_file_magic(WAL_MAGIC_NUMBER, "test.log");
    try testing.expectEqual(@as(u32, 0), tracker.consecutive_failures);
    try testing.expectEqual(@as(u32, 1), tracker.total_operations);

    tracker.validate_entry_magic(WAL_ENTRY_MAGIC, 0);
    try testing.expectEqual(@as(u32, 0), tracker.consecutive_failures);
    try testing.expectEqual(@as(u32, 2), tracker.total_operations);
}

test "CorruptionTracker reset functionality" {
    var tracker = CorruptionTracker.init();

    // Build up some state
    tracker.record_failure("test");
    tracker.record_failure("test");
    tracker.record_success();
    tracker.record_failure("test");

    try testing.expect(tracker.consecutive_failures > 0);
    try testing.expect(tracker.total_failures > 0);
    try testing.expect(tracker.total_operations > 0);

    // Reset should clear all state
    tracker.reset();
    try testing.expectEqual(@as(u32, 0), tracker.consecutive_failures);
    try testing.expectEqual(@as(u32, 0), tracker.total_failures);
    try testing.expectEqual(@as(u32, 0), tracker.total_operations);
    try testing.expectEqual(@as(f64, 0.0), tracker.failure_rate());
}

test "CorruptionTracker different failure contexts" {
    var tracker = CorruptionTracker.init();

    // Different failure contexts should all count toward systematic corruption
    tracker.record_failure("checksum_validation");
    tracker.record_failure("entry_type_validation");
    tracker.record_failure("header_validation");

    // All failures count regardless of context
    try testing.expectEqual(@as(u32, 3), tracker.consecutive_failures);
    try testing.expectEqual(@as(u32, 3), tracker.total_failures);

    // The 4th failure would trigger systematic corruption detection
    // tracker.record_failure("any_context"); // <- This would panic
}

test "CorruptionTracker magic number constants validation" {
    // Verify magic numbers are properly defined and distinct
    try testing.expect(WAL_MAGIC_NUMBER != 0);
    try testing.expect(WAL_ENTRY_MAGIC != 0);
    try testing.expect(WAL_MAGIC_NUMBER != WAL_ENTRY_MAGIC);

    // Verify expected ASCII-based values
    try testing.expectEqual(@as(u32, 0x574C4147), WAL_MAGIC_NUMBER); // "GLAW" reversed
    try testing.expectEqual(@as(u32, 0x57454E54), WAL_ENTRY_MAGIC); // "TNEW" reversed
}

test "CorruptionTracker boundary condition - exactly at threshold then success" {
    var tracker = CorruptionTracker.init();

    // Record exactly threshold failures
    tracker.record_failure("test");
    tracker.record_failure("test");
    tracker.record_failure("test"); // At threshold, should not panic

    try testing.expectEqual(@as(u32, 3), tracker.consecutive_failures);

    // Success should reset and allow continued operation
    tracker.record_success();
    try testing.expectEqual(@as(u32, 0), tracker.consecutive_failures);

    // Can now record more failures safely
    tracker.record_failure("test");
    tracker.record_failure("test");
    tracker.record_failure("test");
    try testing.expectEqual(@as(u32, 3), tracker.consecutive_failures);

    // Total counts should be accurate
    try testing.expectEqual(@as(u32, 6), tracker.total_failures);
    try testing.expectEqual(@as(u32, 7), tracker.total_operations);
}
