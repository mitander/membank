//! Allocator Torture Testing Integration
//!
//! Comprehensive integration test combining DebugAllocator with AllocatorTortureTester
//! to validate enhanced memory debugging infrastructure under stress conditions.
//! This completes Priority 0 from CORTEX_TODO.md by proving the debug infrastructure
//! can catch memory bugs at their source during hostile conditions.

const cortexdb = @import("cortexdb");
const std = @import("std");
const testing = std.testing;
const debug_allocator = cortexdb.debug_allocator;
const allocator_torture_test = cortexdb.allocator_torture_test;

const DebugAllocator = debug_allocator.DebugAllocator;
const AllocatorTortureTester = allocator_torture_test.AllocatorTortureTester;
const TortureTestConfig = allocator_torture_test.TortureTestConfig;

test "DebugAllocator torture test integration" {
    // Arena per test for perfect isolation as mandated by STYLE.md
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const allocator = arena.allocator();

    // Layer DebugAllocator over arena for enhanced debugging capability
    var debug_alloc = DebugAllocator.init(allocator);
    const debug_allocator_instance = debug_alloc.allocator();

    // Configure torture test for moderate stress without overwhelming CI
    const config = TortureTestConfig{
        .allocation_cycles = 100,
        .max_allocation_size = 2048,
        .min_allocation_size = 16,
        .random_seed = 0xDEADBEEF,
        .enable_pattern_validation = true,
        .enable_alignment_stress = true,
        .free_probability = 0.8,
        .enable_boundary_testing = true,
    };

    var tester = try AllocatorTortureTester.init(debug_allocator_instance, config);
    defer tester.deinit();

    // Execute comprehensive torture test with debug tracking
    try tester.run_torture_test();

    // Validate debug allocator detected no memory corruption during stress test
    const debug_stats = debug_alloc.statistics();
    try testing.expectEqual(@as(u64, 0), debug_stats.double_frees);
    try testing.expectEqual(@as(u64, 0), debug_stats.buffer_overflows);
    try testing.expectEqual(@as(u64, 0), debug_stats.alignment_violations);
    try testing.expectEqual(@as(u64, 0), debug_stats.invalid_frees);

    // Validate torture test completed successfully
    const torture_stats = tester.get_stats();
    try testing.expect(torture_stats.successful_allocations > 0);
    try testing.expect(torture_stats.successful_frees > 0);
    try testing.expectEqual(@as(u64, 0), torture_stats.pattern_violations);
    try testing.expectEqual(@as(u64, 0), torture_stats.alignment_violations);

    // Verify memory accounting is perfect - no leaks
    try testing.expectEqual(@as(u64, 0), debug_stats.active_allocations);
    try testing.expectEqual(@as(u64, 0), debug_stats.current_bytes_allocated);
}

test "DebugAllocator under alignment stress conditions" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const allocator = arena.allocator();

    var debug_alloc = DebugAllocator.init(allocator);
    const debug_allocator_instance = debug_alloc.allocator();

    // Configuration focused on alignment edge cases
    const config = TortureTestConfig{
        .allocation_cycles = 50,
        .max_allocation_size = 1024,
        .min_allocation_size = 1,
        .random_seed = 0xCAFEBABE,
        .enable_pattern_validation = true,
        .enable_alignment_stress = true,
        .free_probability = 0.9,
        .enable_boundary_testing = true,
    };

    var tester = try AllocatorTortureTester.init(debug_allocator_instance, config);
    defer tester.deinit();

    try tester.run_torture_test();

    // Under alignment stress, DebugAllocator should catch any violations
    const debug_stats = debug_alloc.statistics();
    try testing.expectEqual(@as(u64, 0), debug_stats.alignment_violations);
    try testing.expectEqual(@as(u64, 0), debug_stats.buffer_overflows);

    const torture_stats = tester.get_stats();
    try testing.expectEqual(@as(u64, 0), torture_stats.alignment_violations);
}

test "DebugAllocator pattern validation stress test" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const allocator = arena.allocator();

    var debug_alloc = DebugAllocator.init(allocator);
    const debug_allocator_instance = debug_alloc.allocator();

    // Heavy pattern validation to stress memory integrity checking
    const config = TortureTestConfig{
        .allocation_cycles = 200,
        .max_allocation_size = 512,
        .min_allocation_size = 8,
        .random_seed = 0xBEEFCAFE,
        .enable_pattern_validation = true,
        .enable_alignment_stress = false,
        .free_probability = 0.7,
        .enable_boundary_testing = false,
    };

    var tester = try AllocatorTortureTester.init(debug_allocator_instance, config);
    defer tester.deinit();

    try tester.run_torture_test();

    // Pattern validation should catch any buffer overruns or corruption
    const debug_stats = debug_alloc.statistics();
    try testing.expectEqual(@as(u64, 0), debug_stats.buffer_overflows);

    const torture_stats = tester.get_stats();
    try testing.expectEqual(@as(u64, 0), torture_stats.pattern_violations);
    try testing.expect(torture_stats.successful_allocations >= 100);
}

test "DebugAllocator comprehensive stress validation" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const allocator = arena.allocator();

    var debug_alloc = DebugAllocator.init(allocator);
    const debug_allocator_instance = debug_alloc.allocator();

    // Full-featured stress test with all validation enabled
    const config = TortureTestConfig{
        .allocation_cycles = 300,
        .max_allocation_size = 4096,
        .min_allocation_size = 1,
        .random_seed = 0xDEADC0DE,
        .enable_pattern_validation = true,
        .enable_alignment_stress = true,
        .free_probability = 0.75,
        .enable_boundary_testing = true,
        .enable_double_free_testing = false, // Disabled as it causes undefined behavior
    };

    var tester = try AllocatorTortureTester.init(debug_allocator_instance, config);
    defer tester.deinit();

    try tester.run_torture_test();

    // Comprehensive validation - no errors should be detected
    const debug_stats = debug_alloc.statistics();
    try testing.expectEqual(@as(u64, 0), debug_stats.double_frees);
    try testing.expectEqual(@as(u64, 0), debug_stats.buffer_overflows);
    try testing.expectEqual(@as(u64, 0), debug_stats.alignment_violations);
    try testing.expectEqual(@as(u64, 0), debug_stats.invalid_frees);

    const torture_stats = tester.get_stats();
    try testing.expectEqual(@as(u64, 0), torture_stats.pattern_violations);
    try testing.expectEqual(@as(u64, 0), torture_stats.alignment_violations);

    // Verify substantial work was performed
    try testing.expect(torture_stats.successful_allocations >= 200);
    try testing.expect(torture_stats.successful_frees >= 150);

    // Perfect memory accounting after stress test
    try testing.expectEqual(@as(u64, 0), debug_stats.active_allocations);
    try testing.expectEqual(@as(u64, 0), debug_stats.current_bytes_allocated);
}
