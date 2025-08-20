//! Mid-Compaction Crash Recovery Tests
//!
//! Tests storage engine recovery from crashes that occur during compaction.
//! Validates that the system can recover gracefully from intermediate states
//! and maintain data integrity across crash boundaries.

const std = @import("std");

const kausaldb = @import("kausaldb");

const testing = std.testing;

test "recovery from partial sstable write" {
    const allocator = testing.allocator;

    // Use standardized scenario framework for partial SSTable write testing
    try kausaldb.scenarios.run_compaction_crash_scenario(allocator, .partial_sstable_write);
}

test "recovery with orphaned files" {
    const allocator = testing.allocator;

    // Use standardized scenario framework for orphaned files testing
    try kausaldb.scenarios.run_compaction_crash_scenario(allocator, .orphaned_files);
}

test "multiple sequential crash recovery" {
    const allocator = testing.allocator;

    // Use standardized scenario framework for sequential crashes testing
    try kausaldb.scenarios.run_compaction_crash_scenario(allocator, .sequential_crashes);
}

test "torn write recovery" {
    const allocator = testing.allocator;

    // Use standardized scenario framework for torn write testing
    try kausaldb.scenarios.run_compaction_crash_scenario(allocator, .torn_write_header);
}

test "systematic compaction crash validation" {
    const allocator = testing.allocator;

    // Run all predefined compaction crash scenarios for comprehensive testing
    try kausaldb.scenarios.run_all_compaction_scenarios(allocator);
}
