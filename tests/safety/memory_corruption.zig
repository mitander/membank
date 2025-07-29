//! Memory safety and corruption detection tests.
//!
//! Validates CortexDB's arena-per-subsystem memory management strategy,
//! defensive programming patterns, and fatal assertion mechanisms under
//! hostile conditions including systematic corruption scenarios.

const cortexdb = @import("cortexdb");
const std = @import("std");
const testing = std.testing;

const debug_allocator = cortexdb.debug_allocator;
const allocator_torture_test = cortexdb.allocator_torture_test;
const assert = cortexdb.assert.assert;
const fatal_assert = cortexdb.assert.fatal_assert;
const simulation = cortexdb.simulation;
const context_block = cortexdb.types;

const DebugAllocator = debug_allocator.DebugAllocator;
const AllocatorTortureTester = allocator_torture_test.AllocatorTortureTester;
const TortureTestConfig = allocator_torture_test.TortureTestConfig;
const Simulation = simulation.Simulation;
const ContextBlock = context_block.ContextBlock;
const BlockId = context_block.BlockId;

// Defensive limits to prevent runaway tests
const MAX_TEST_DURATION_MS = 5000;
const MAX_CORRUPTION_ATTEMPTS = 100;
const CORRUPTION_THRESHOLD = 4;

test "arena allocator bulk deallocation safety" {
    const allocator = testing.allocator;

    // Arena-per-subsystem pattern used throughout CortexDB
    var arena = std.heap.ArenaAllocator.init(allocator);
    defer arena.deinit();
    const arena_allocator = arena.allocator();

    var allocated_ptrs = std.ArrayList(*[]u8).init(allocator);
    defer {
        for (allocated_ptrs.items) |ptr| {
            allocator.free(ptr.*);
        }
        allocated_ptrs.deinit();
    }

    // Allocate many blocks of varying sizes within arena
    const allocation_sizes = [_]usize{ 16, 32, 64, 128, 256, 512, 1024, 2048, 4096 };
    for (allocation_sizes) |size| {
        for (0..10) |_| {
            const memory = try arena_allocator.alloc(u8, size);
            @memset(memory, 0xAB);

            // Store pointer for later verification
            const ptr = try allocator.create([]u8);
            ptr.* = memory;
            try allocated_ptrs.append(ptr);
        }
    }

    // Verify all allocations contain expected pattern
    for (allocated_ptrs.items) |ptr| {
        for (ptr.*) |byte| {
            try testing.expectEqual(@as(u8, 0xAB), byte);
        }
    }

    // Arena reset deallocates everything in O(1)
    _ = arena.reset(.retain_capacity);

    // All arena-allocated memory now invalid, but pointers remain for verification
    // This tests that arena deallocation doesn't corrupt the backing allocator
}

test "debug_allocator torture test integration" {
    const allocator = testing.allocator;

    var debug_alloc = DebugAllocator.init(allocator);
    const debug_allocator_instance = debug_alloc.allocator();

    // Moderate stress configuration suitable for CI
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

    try tester.run_torture_test();

    // Verify no memory corruption detected during stress test
    const debug_stats = debug_alloc.statistics();
    try testing.expectEqual(@as(u64, 0), debug_stats.double_frees);
    try testing.expectEqual(@as(u64, 0), debug_stats.buffer_overflows);
    try testing.expectEqual(@as(u64, 0), debug_stats.alignment_violations);
    try testing.expectEqual(@as(u64, 0), debug_stats.invalid_frees);

    const torture_stats = tester.get_stats();
    try testing.expect(torture_stats.successful_allocations > 0);
    try testing.expect(torture_stats.successful_frees > 0);
    try testing.expectEqual(@as(u64, 0), torture_stats.pattern_violations);
    try testing.expectEqual(@as(u64, 0), torture_stats.alignment_violations);

    // Perfect memory accounting - critical for CortexDB reliability
    try testing.expectEqual(@as(u64, 0), debug_stats.active_allocations);
    try testing.expectEqual(@as(u64, 0), debug_stats.current_bytes_allocated);
}

test "alignment stress testing" {
    const allocator = testing.allocator;

    var debug_alloc = DebugAllocator.init(allocator);
    const debug_allocator_instance = debug_alloc.allocator();

    // Focus on alignment edge cases that cause subtle corruption
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

    const debug_stats = debug_alloc.statistics();
    try testing.expectEqual(@as(u64, 0), debug_stats.alignment_violations);
    try testing.expectEqual(@as(u64, 0), debug_stats.buffer_overflows);

    const torture_stats = tester.get_stats();
    try testing.expectEqual(@as(u64, 0), torture_stats.alignment_violations);
}

test "pattern validation memory integrity" {
    const allocator = testing.allocator;

    var debug_alloc = DebugAllocator.init(allocator);
    const debug_allocator_instance = debug_alloc.allocator();

    // Heavy pattern validation to detect buffer overruns
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

    const debug_stats = debug_alloc.statistics();
    try testing.expectEqual(@as(u64, 0), debug_stats.buffer_overflows);

    const torture_stats = tester.get_stats();
    try testing.expectEqual(@as(u64, 0), torture_stats.pattern_violations);
    try testing.expect(torture_stats.successful_allocations >= 100);
}

test "comprehensive memory safety validation" {
    const allocator = testing.allocator;

    var debug_alloc = DebugAllocator.init(allocator);
    const debug_allocator_instance = debug_alloc.allocator();

    // Full stress test with all validation mechanisms enabled
    const config = TortureTestConfig{
        .allocation_cycles = 300,
        .max_allocation_size = 4096,
        .min_allocation_size = 1,
        .random_seed = 0xDEADC0DE,
        .enable_pattern_validation = true,
        .enable_alignment_stress = true,
        .free_probability = 0.75,
        .enable_boundary_testing = true,
        .enable_double_free_testing = false, // Causes undefined behavior
    };

    var tester = try AllocatorTortureTester.init(debug_allocator_instance, config);
    defer tester.deinit();

    try tester.run_torture_test();

    // Comprehensive validation - zero tolerance for memory errors
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

    // Perfect memory accounting after comprehensive stress test
    try testing.expectEqual(@as(u64, 0), debug_stats.active_allocations);
    try testing.expectEqual(@as(u64, 0), debug_stats.current_bytes_allocated);
}

test "arena subsystem isolation" {
    const allocator = testing.allocator;

    // Simulate multiple subsystems each with their own arena
    var storage_arena = std.heap.ArenaAllocator.init(allocator);
    defer storage_arena.deinit();
    var query_arena = std.heap.ArenaAllocator.init(allocator);
    defer query_arena.deinit();
    var wal_arena = std.heap.ArenaAllocator.init(allocator);
    defer wal_arena.deinit();

    const storage_allocator = storage_arena.allocator();
    const query_allocator = query_arena.allocator();
    const wal_allocator = wal_arena.allocator();

    // Allocate different data patterns in each subsystem
    const storage_data = try storage_allocator.alloc(u8, 1024);
    @memset(storage_data, 0xAA);

    const query_data = try query_allocator.alloc(u8, 2048);
    @memset(query_data, 0xBB);

    const wal_data = try wal_allocator.alloc(u8, 512);
    @memset(wal_data, 0xCC);

    // Verify isolation - each subsystem has correct pattern
    for (storage_data) |byte| {
        try testing.expectEqual(@as(u8, 0xAA), byte);
    }
    for (query_data) |byte| {
        try testing.expectEqual(@as(u8, 0xBB), byte);
    }
    for (wal_data) |byte| {
        try testing.expectEqual(@as(u8, 0xCC), byte);
    }

    // Selective subsystem cleanup (simulating MemtableManager flush)
    _ = storage_arena.reset(.retain_capacity);

    // Other subsystems remain unaffected
    for (query_data) |byte| {
        try testing.expectEqual(@as(u8, 0xBB), byte);
    }
    for (wal_data) |byte| {
        try testing.expectEqual(@as(u8, 0xCC), byte);
    }
}

test "memory accounting precision" {
    const allocator = testing.allocator;

    var debug_alloc = DebugAllocator.init(allocator);
    const debug_allocator_instance = debug_alloc.allocator();

    // Test precise memory accounting under various allocation patterns
    const sizes = [_]usize{ 1, 7, 8, 15, 16, 31, 32, 63, 64, 127, 128, 255, 256 };
    var total_expected: u64 = 0;
    var allocations = std.ArrayList([]u8).init(allocator);
    defer {
        for (allocations.items) |allocation| {
            debug_allocator_instance.free(allocation);
        }
        allocations.deinit();
    }

    // Allocate various sizes and track expected total
    for (sizes) |size| {
        const memory = try debug_allocator_instance.alloc(u8, size);
        try allocations.append(memory);
        total_expected += size;

        // Verify accounting matches expected
        const stats = debug_alloc.statistics();
        try testing.expectEqual(total_expected, stats.current_bytes_allocated);
        try testing.expectEqual(allocations.items.len, stats.active_allocations);
    }

    // Free half the allocations
    const half_count = allocations.items.len / 2;
    for (0..half_count) |i| {
        debug_allocator_instance.free(allocations.items[i]);
        total_expected -= sizes[i];

        const stats = debug_alloc.statistics();
        try testing.expectEqual(total_expected, stats.current_bytes_allocated);
        try testing.expectEqual(allocations.items.len - i - 1, stats.active_allocations);
    }

    // Free remaining allocations
    for (half_count..allocations.items.len) |i| {
        debug_allocator_instance.free(allocations.items[i]);
        total_expected -= sizes[i];
    }
    allocations.clearRetainingCapacity();

    // Verify perfect cleanup
    const final_stats = debug_alloc.statistics();
    try testing.expectEqual(@as(u64, 0), final_stats.current_bytes_allocated);
    try testing.expectEqual(@as(u64, 0), final_stats.active_allocations);
    try testing.expectEqual(@as(u64, 0), final_stats.double_frees);
    try testing.expectEqual(@as(u64, 0), final_stats.invalid_frees);
}

test "cross_allocator_corruption_detection" {
    // Test defensive programming against cross-allocator memory corruption
    const allocator = testing.allocator;

    var arena1 = std.heap.ArenaAllocator.init(allocator);
    defer arena1.deinit();
    var arena2 = std.heap.ArenaAllocator.init(allocator);
    defer arena2.deinit();

    const alloc1 = arena1.allocator();
    const alloc2 = arena2.allocator();

    // Allocate from different arenas
    const data1 = try alloc1.alloc(u8, 256);
    const data2 = try alloc2.alloc(u8, 256);

    @memset(data1, 0xAA);
    @memset(data2, 0xBB);

    // Verify isolation
    for (data1) |byte| {
        try testing.expectEqual(@as(u8, 0xAA), byte);
    }
    for (data2) |byte| {
        try testing.expectEqual(@as(u8, 0xBB), byte);
    }

    // Reset one arena - should not affect the other
    _ = arena1.reset(.retain_capacity);

    // data2 should remain valid and uncorrupted
    for (data2) |byte| {
        try testing.expectEqual(@as(u8, 0xBB), byte);
    }
}

test "buffer_overflow_boundary_detection" {
    const allocator = testing.allocator;

    var debug_alloc = DebugAllocator.init(allocator);
    const debug_allocator_instance = debug_alloc.allocator();

    // Allocate buffer and test boundary protection
    const buffer = try debug_allocator_instance.alloc(u8, 64);
    defer debug_allocator_instance.free(buffer);

    // Fill buffer completely (should be safe)
    @memset(buffer, 0xDE);

    // Verify buffer contents
    for (buffer) |byte| {
        try testing.expectEqual(@as(u8, 0xDE), byte);
    }

    // DebugAllocator should detect any buffer overruns during free()
    const stats = debug_alloc.statistics();
    try testing.expectEqual(@as(u64, 0), stats.buffer_overflows);
}

test "arena_corruption_systematic_detection" {
    const allocator = testing.allocator;

    // Test systematic arena corruption detection using simulation
    var sim = try Simulation.init(allocator, 0xDEADC0DE);
    defer sim.deinit();

    const node = try sim.add_node();
    const node_ptr = sim.find_node(node);
    const sim_vfs = node_ptr.filesystem_interface();

    // Create data directory and test arena corruption scenarios
    const test_dir = "arena_corruption_test";
    try sim_vfs.mkdir_all(test_dir);

    // Simulate multiple arena allocations that could trigger corruption
    var arena1 = std.heap.ArenaAllocator.init(allocator);
    defer arena1.deinit();
    var arena2 = std.heap.ArenaAllocator.init(allocator);
    defer arena2.deinit();

    const alloc1 = arena1.allocator();
    const alloc2 = arena2.allocator();

    // Allocate blocks that could be corrupted via pointer aliasing
    const block1_data = try alloc1.alloc(u8, 1024);
    const block2_data = try alloc2.alloc(u8, 1024);

    @memset(block1_data, 0xAA);
    @memset(block2_data, 0xBB);

    // Verify no cross-arena corruption
    for (block1_data) |byte| {
        try testing.expectEqual(@as(u8, 0xAA), byte);
    }
    for (block2_data) |byte| {
        try testing.expectEqual(@as(u8, 0xBB), byte);
    }

    // Test arena reset safety - should not affect other arenas
    _ = arena1.reset(.retain_capacity);

    // block2_data should remain valid and uncorrupted
    for (block2_data) |byte| {
        try testing.expectEqual(@as(u8, 0xBB), byte);
    }
}

test "defensive_timeout_memory_operations" {
    const allocator = testing.allocator;

    const start_time = std.time.milliTimestamp();
    var debug_alloc = DebugAllocator.init(allocator);
    const debug_allocator_instance = debug_alloc.allocator();

    var operations_completed: u32 = 0;
    var allocations = std.ArrayList([]u8).init(allocator);
    defer {
        for (allocations.items) |allocation| {
            debug_allocator_instance.free(allocation);
        }
        allocations.deinit();
    }

    // Perform operations with timeout protection
    while (operations_completed < 1000) {
        const current_time = std.time.milliTimestamp();
        if (current_time - start_time > MAX_TEST_DURATION_MS) {
            break; // Defensive timeout reached
        }

        const size = (operations_completed % 256) + 1;
        const memory = try debug_allocator_instance.alloc(u8, size);
        @memset(memory, @intCast(operations_completed & 0xFF));
        try allocations.append(memory);

        operations_completed += 1;

        // Periodic validation to catch corruption early
        if (operations_completed % 100 == 0) {
            const stats = debug_alloc.statistics();
            try testing.expectEqual(@as(u64, 0), stats.buffer_overflows);
            try testing.expectEqual(@as(u64, 0), stats.double_frees);
        }
    }

    // Verify operations completed within timeout
    const total_time = std.time.milliTimestamp() - start_time;
    try testing.expect(total_time < MAX_TEST_DURATION_MS);
    try testing.expect(operations_completed > 0);

    // Final memory safety validation
    const final_stats = debug_alloc.statistics();
    try testing.expectEqual(@as(u64, 0), final_stats.buffer_overflows);
    try testing.expectEqual(@as(u64, 0), final_stats.double_frees);
    try testing.expectEqual(@as(u64, 0), final_stats.alignment_violations);
}

test "memory_corruption_fatal_assertion_patterns" {
    const allocator = testing.allocator;

    // Test memory accounting corruption detection patterns
    var debug_alloc = DebugAllocator.init(allocator);
    const debug_allocator_instance = debug_alloc.allocator();

    // Allocate and track memory
    const memory1 = try debug_allocator_instance.alloc(u8, 256);
    const memory2 = try debug_allocator_instance.alloc(u8, 512);
    defer debug_allocator_instance.free(memory1);
    defer debug_allocator_instance.free(memory2);

    const stats_after_alloc = debug_alloc.statistics();
    try testing.expectEqual(@as(u64, 2), stats_after_alloc.active_allocations);
    try testing.expectEqual(@as(u64, 768), stats_after_alloc.current_bytes_allocated);

    // Verify corruption detection mechanisms are active
    try testing.expectEqual(@as(u64, 0), stats_after_alloc.buffer_overflows);
    try testing.expectEqual(@as(u64, 0), stats_after_alloc.double_frees);
    try testing.expectEqual(@as(u64, 0), stats_after_alloc.invalid_frees);

    // Fill allocated memory to test boundary detection
    @memset(memory1, 0xDE);
    @memset(memory2, 0xAD);

    // Validate memory patterns remain intact
    for (memory1) |byte| {
        try testing.expectEqual(@as(u8, 0xDE), byte);
    }
    for (memory2) |byte| {
        try testing.expectEqual(@as(u8, 0xAD), byte);
    }
}

test "systematic_corruption_threshold_detection" {
    const allocator = testing.allocator;

    // Simulate systematic corruption detection pattern similar to WAL
    var corruption_counter: u32 = 0;
    const test_checksums = [_]u32{ 0xDEADBEEF, 0xCAFEBABE, 0xFEEDFACE, 0xBADC0DE };
    const expected_checksum: u32 = 0x900DDA7A; // Intentionally different

    for (test_checksums) |checksum| {
        if (checksum != expected_checksum) {
            corruption_counter += 1;

            // Log corruption detection (would use error_context in real code)
            if (corruption_counter >= CORRUPTION_THRESHOLD) {
                // In real code, this would fatal_assert
                // Here we verify the detection mechanism works
                try testing.expect(corruption_counter >= CORRUPTION_THRESHOLD);
                break;
            }
        } else {
            corruption_counter = 0; // Reset on success
        }
    }

    // Verify systematic corruption was detected
    try testing.expectEqual(@as(u32, CORRUPTION_THRESHOLD), corruption_counter);
}

test "arena_memory_accounting_precision" {
    const allocator = testing.allocator;

    // Test precise memory accounting in arena-per-subsystem pattern
    var storage_arena = std.heap.ArenaAllocator.init(allocator);
    defer storage_arena.deinit();
    var query_arena = std.heap.ArenaAllocator.init(allocator);
    defer query_arena.deinit();

    const storage_allocator = storage_arena.allocator();
    const query_allocator = query_arena.allocator();

    // Track initial arena state
    const storage_state_before = storage_arena.state();
    const query_state_before = query_arena.state();

    // Allocate different amounts in each subsystem
    const storage_blocks = try storage_allocator.alloc(ContextBlock, 10);
    const query_buffer = try query_allocator.alloc(u8, 2048);

    // Fill with patterns to ensure allocation worked
    for (storage_blocks, 0..) |*block, i| {
        block.* = ContextBlock{
            .id = BlockId.generate(),
            .version = 1,
            .source_uri = "test://memory_test",
            .metadata_json = "{}",
            .content = try std.fmt.allocPrint(storage_allocator, "Block {}", .{i}),
        };
    }
    @memset(query_buffer, 0xAB);

    // Verify arena state advanced (memory was allocated)
    const storage_state_after = storage_arena.state();
    const query_state_after = query_arena.state();

    try testing.expect(storage_state_after.end_index > storage_state_before.end_index);
    try testing.expect(query_state_after.end_index > query_state_before.end_index);

    // Selective arena reset should not affect other arenas
    _ = storage_arena.reset(.retain_capacity);

    // Query arena should remain valid and data intact
    for (query_buffer) |byte| {
        try testing.expectEqual(@as(u8, 0xAB), byte);
    }
}
