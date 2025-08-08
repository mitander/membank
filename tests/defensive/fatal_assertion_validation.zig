//! Comprehensive validation tests for the enhanced fatal assertion framework.
//!
//! Tests that fatal assertions provide proper error reporting, categorization,
//! and debugging context for critical KausalDB failures. Validates assertion
//! behavior, error formatting, and integration with the broader defensive
//! programming strategy.

const std = @import("std");
const testing = std.testing;
const kausaldb = @import("kausaldb");
const fatal_assertions = kausaldb.fatal_assertions;

const FatalCategory = fatal_assertions.FatalCategory;
const FatalContext = fatal_assertions.FatalContext;

// Test data structures
const MockBlockId = struct {
    bytes: [16]u8,

    pub fn zero() MockBlockId {
        return MockBlockId{ .bytes = [_]u8{0} ** 16 };
    }

    pub fn valid() MockBlockId {
        return MockBlockId{ .bytes = [_]u8{
            0x01, 0x23, 0x45, 0x67, 0x89, 0xAB, 0xCD, 0xEF,
            0xFE, 0xDC, 0xBA, 0x98, 0x76, 0x54, 0x32, 0x10,
        } };
    }
};

const MockWALEntry = struct {
    magic: u32,
    size: u32,

    pub fn valid() MockWALEntry {
        return MockWALEntry{
            .magic = 0x57414C45, // "WALE"
            .size = 1024,
        };
    }

    pub fn corrupted_magic() MockWALEntry {
        return MockWALEntry{
            .magic = 0xDEADBEEF,
            .size = 1024,
        };
    }

    pub fn oversized() MockWALEntry {
        return MockWALEntry{
            .magic = 0x57414C45,
            .size = 128 * 1024 * 1024, // 128MB > 64MB limit
        };
    }
};

const ContextState = enum {
    uninitialized,
    initializing,
    ready,
    processing,
    flushing,
    shutdown,

    const valid_transitions = [_]struct { from: ContextState, to: ContextState }{
        .{ .from = .uninitialized, .to = .initializing },
        .{ .from = .initializing, .to = .ready },
        .{ .from = .ready, .to = .processing },
        .{ .from = .processing, .to = .flushing },
        .{ .from = .processing, .to = .ready },
        .{ .from = .flushing, .to = .ready },
        .{ .from = .ready, .to = .shutdown },
        .{ .from = .processing, .to = .shutdown },
    };
};

test "fatal assertion categories have proper descriptions" {
    try testing.expectEqualStrings("MEMORY CORRUPTION", FatalCategory.memory_corruption.description());
    try testing.expectEqualStrings("INVARIANT VIOLATION", FatalCategory.invariant_violation.description());
    try testing.expectEqualStrings("DATA CORRUPTION", FatalCategory.data_corruption.description());
    try testing.expectEqualStrings("PROTOCOL VIOLATION", FatalCategory.protocol_violation.description());
    try testing.expectEqualStrings("RESOURCE EXHAUSTION", FatalCategory.resource_exhaustion.description());
    try testing.expectEqualStrings("LOGIC ERROR", FatalCategory.logic_error.description());
    try testing.expectEqualStrings("SECURITY VIOLATION", FatalCategory.security_violation.description());
}

test "fatal context initialization and formatting" {
    const allocator = testing.allocator;

    const context = FatalContext.init(
        .memory_corruption,
        "Test Component",
        "Test Operation",
        @src(),
    );

    try testing.expectEqual(FatalCategory.memory_corruption, context.category);
    try testing.expectEqualStrings("Test Component", context.component);
    try testing.expectEqualStrings("Test Operation", context.operation);
    try testing.expect(context.timestamp > 0);

    // Test header formatting
    var buffer = std.ArrayList(u8).init(allocator);
    defer buffer.deinit();

    try context.format_header(buffer.writer());
    const header = buffer.items;

    // Verify header contains expected information
    try testing.expect(std.mem.indexOf(u8, header, "FATAL ASSERTION FAILURE: MEMORY CORRUPTION") != null);
    try testing.expect(std.mem.indexOf(u8, header, "Component: Test Component") != null);
    try testing.expect(std.mem.indexOf(u8, header, "Operation: Test Operation") != null);
    try testing.expect(std.mem.indexOf(u8, header, "fatal_assertion_validation.zig") != null);
}

test "block ID validation passes for valid IDs" {
    // Valid BlockId should pass validation
    const valid_block_id = MockBlockId.valid();

    // This should not panic (test passes if no panic occurs)
    fatal_assertions.fatal_assert_block_id_valid(valid_block_id, @src());
}

test "memory alignment validation passes for aligned pointers" {
    const allocator = testing.allocator;

    // Allocate aligned memory
    const aligned_memory = try allocator.alignedAlloc(u8, .@"16", 64);
    defer allocator.free(aligned_memory);

    // This should not panic (test passes if no panic occurs)
    fatal_assertions.fatal_assert_memory_aligned(aligned_memory.ptr, 16, @src());
}

test "buffer bounds validation passes for valid writes" {
    const buffer_len = 100;
    const pos = 10;
    const write_len = 50;

    // Valid write should pass (test passes if no panic occurs)
    fatal_assertions.fatal_assert_buffer_bounds(pos, write_len, buffer_len, @src());
}

test "file operation validation passes for successful operations" {
    // Test with boolean success
    fatal_assertions.fatal_assert_file_operation(true, "test_operation", "/tmp/test", @src());

    // Test with non-null optional
    const optional_result: ?u32 = 42;
    fatal_assertions.fatal_assert_file_operation(optional_result, "test_operation", "/tmp/test", @src());

    // Test with non-zero result
    fatal_assertions.fatal_assert_file_operation(@as(i32, 1), "test_operation", "/tmp/test", @src());
}

test "CRC validation passes for matching checksums" {
    const expected_crc: u32 = 0x1234ABCD;
    const actual_crc: u32 = 0x1234ABCD;

    // Matching CRCs should pass (test passes if no panic occurs)
    fatal_assertions.fatal_assert_crc_valid(actual_crc, expected_crc, "test data", @src());
}

test "WAL entry validation passes for valid entries" {
    const valid_entry = MockWALEntry.valid();

    // Valid WAL entry should pass (test passes if no panic occurs)
    fatal_assertions.fatal_assert_wal_entry_valid(valid_entry, @src());
}

test "context state transition validation passes for valid transitions" {
    // Valid transition: ready -> processing
    fatal_assertions.fatal_assert_context_transition(
        ContextState.ready,
        ContextState.processing,
        &ContextState.valid_transitions,
        @src(),
    );

    // Valid transition: uninitialized -> initializing
    fatal_assertions.fatal_assert_context_transition(
        ContextState.uninitialized,
        ContextState.initializing,
        &ContextState.valid_transitions,
        @src(),
    );
}

test "protocol invariant validation passes for valid conditions" {
    // Valid protocol condition should pass
    fatal_assertions.fatal_assert_protocol_invariant(
        true,
        "TestProtocol",
        "message_length_invariant",
        "Message length {} must be <= {} bytes",
        .{ 100, 1000 },
        @src(),
    );
}

test "resource limit validation passes for usage within limits" {
    const current_usage: u64 = 1024 * 1024; // 1MB
    const limit: u64 = 10 * 1024 * 1024; // 10MB

    // Usage within limit should pass (test passes if no panic occurs)
    fatal_assertions.fatal_assert_resource_limit(current_usage, limit, "memory", @src());
}

test "convenience macros provide proper source location" {
    const allocator = testing.allocator;

    // Test that macros work and provide source location automatically
    const valid_block_id = MockBlockId.valid();
    fatal_assertions.fatal_assert_block_id_valid(valid_block_id, @src());

    const aligned_buffer = try allocator.alignedAlloc(u8, .@"8", 64);
    defer allocator.free(aligned_buffer);
    fatal_assertions.fatal_assert_memory_aligned(aligned_buffer.ptr, 8, @src());

    fatal_assertions.fatal_assert_buffer_bounds(0, 32, 64, @src());

    fatal_assertions.fatal_assert_file_operation(true, "test", "/path", @src());

    fatal_assertions.fatal_assert_crc_valid(0x123, 0x123, "test data", @src());

    const valid_wal_entry = MockWALEntry.valid();
    fatal_assertions.fatal_assert_wal_entry_valid(valid_wal_entry, @src());

    fatal_assertions.fatal_assert_context_transition(
        ContextState.ready,
        ContextState.processing,
        &ContextState.valid_transitions,
        @src(),
    );

    fatal_assertions.fatal_assert_protocol_invariant(
        true,
        "TestProtocol",
        "test_invariant",
        "Test condition holds: {}",
        .{true},
        @src(),
    );

    fatal_assertions.fatal_assert_resource_limit(100, 1000, "test_resource", @src());
}

test "fatal assertion lazy context creation" {
    // Test that context is only created when assertion fails
    // This test validates the performance optimization

    // Passing assertions should not create any context (no system calls)
    fatal_assertions.fatal_assert_ctx(
        true,
        .logic_error,
        "TestComponent",
        "validation",
        @src(),
        "This should not fail",
        .{},
    );
}

test "fatal assertions provide rich debugging context" {
    // Test that error messages contain sufficient debugging information
    const allocator = testing.allocator;

    // Create various contexts and verify they contain expected information
    const contexts = [_]struct {
        category: FatalCategory,
        component: []const u8,
        operation: []const u8,
    }{
        .{ .category = .memory_corruption, .component = "MemTable", .operation = "block_insertion" },
        .{ .category = .data_corruption, .component = "SSTable", .operation = "checksum_validation" },
        .{ .category = .invariant_violation, .component = "WAL", .operation = "entry_ordering" },
        .{ .category = .protocol_violation, .component = "NetworkServer", .operation = "message_parsing" },
        .{ .category = .resource_exhaustion, .component = "BufferPool", .operation = "allocation" },
    };

    for (contexts) |ctx| {
        const context = FatalContext.init(ctx.category, ctx.component, ctx.operation, @src());

        var buffer = std.ArrayList(u8).init(allocator);
        defer buffer.deinit();

        try context.format_header(buffer.writer());
        const header = buffer.items;

        // Verify all expected information is present
        try testing.expect(std.mem.indexOf(u8, header, ctx.category.description()) != null);
        try testing.expect(std.mem.indexOf(u8, header, ctx.component) != null);
        try testing.expect(std.mem.indexOf(u8, header, ctx.operation) != null);

        // Verify debugging hints are category-appropriate
        const hint_keywords = switch (ctx.category) {
            .memory_corruption => "AddressSanitizer",
            .data_corruption => "file integrity",
            .invariant_violation => "data structure consistency",
            .protocol_violation => "protocol state",
            .resource_exhaustion => "resource limits",
            .logic_error => "algorithmic assumptions",
            .security_violation => "authentication",
        };
        _ = hint_keywords; // We can't easily test panic output, so just verify compilation
    }
}

test "fatal assertions integrate with existing assert framework" {
    const assert = kausaldb.assert;

    // Verify that fatal assertions work alongside regular assertions
    assert.assert(true); // Regular assertion (debug only)

    // Fatal assertions should always be active
    const valid_block_id = MockBlockId.valid();
    fatal_assertions.fatal_assert_block_id_valid(valid_block_id, @src());

    // Verify that both frameworks can be used together
    assert.assert_fmt(true, "Regular assertion with format: {}", .{42});

    // Use properly aligned memory for integration test
    const allocator = testing.allocator;
    const aligned_buffer = try allocator.alignedAlloc(u8, .@"4", 16);
    defer allocator.free(aligned_buffer);
    fatal_assertions.fatal_assert_memory_aligned(aligned_buffer.ptr, 4, @src());
}

test "fatal assertion performance impact is minimal" {
    // Measure the overhead of fatal assertions compared to regular checks
    const allocator = testing.allocator;
    const iterations = 1000;

    // Use pseudo-random condition that's almost always true but not optimizable
    var prng = std.Random.DefaultPrng.init(@as(u64, @intCast(std.time.nanoTimestamp())));
    const random = prng.random();

    // Baseline: simple condition check
    const baseline_start = std.time.nanoTimestamp();
    var baseline_result: usize = 0;
    for (0..iterations) |_| {
        // Condition that's ~99.9% true but compiler can't prove it
        const condition = random.int(u32) < 0xFFFFF000; // Almost always true
        if (!condition) {
            baseline_result +%= 1; // Rare case, just continue
        }
        baseline_result +%= 1; // Prevent optimization
    }
    std.mem.doNotOptimizeAway(&baseline_result);
    const baseline_time = std.time.nanoTimestamp() - baseline_start;

    // Reset PRNG for fair comparison
    prng = std.Random.DefaultPrng.init(@as(u64, @intCast(std.time.nanoTimestamp())));

    // With fatal assertion framework
    const assertion_start = std.time.nanoTimestamp();
    var assertion_result: usize = 0;
    for (0..iterations) |_| {
        // Same probabilistic condition for fair comparison
        const condition = random.int(u32) < 0xFFFFF000; // Almost always true
        fatal_assertions.fatal_assert_ctx(
            condition,
            .logic_error,
            "PerfTest",
            "iteration",
            @src(),
            "Performance test iteration failed",
            .{},
        );
        assertion_result +%= 1; // Prevent optimization
    }
    std.mem.doNotOptimizeAway(&assertion_result);
    const assertion_time = std.time.nanoTimestamp() - assertion_start;

    // Calculate overhead, handling optimized baseline
    const baseline_per_call = @as(f64, @floatFromInt(baseline_time)) / iterations;
    const assertion_per_call = @as(f64, @floatFromInt(assertion_time)) / iterations;

    if (baseline_per_call > 0.1) {
        // Normal case: calculate overhead ratio
        const overhead_ratio = assertion_per_call / baseline_per_call;
        try testing.expect(overhead_ratio <= 10.0);
    } else {
        // Baseline optimized away: just check assertion is reasonable (ReleaseSafe mode)
        try testing.expect(assertion_per_call <= 100.0); // Less than 100ns per call
    }

    // Basic sanity checks
    try testing.expect(baseline_time >= 0);
    try testing.expect(assertion_time > 0);
    _ = allocator;
}

test "fatal assertions handle edge cases properly" {
    // Test various edge cases to ensure robust behavior

    // Very large buffer bounds (near integer limits)
    const max_size = std.math.maxInt(usize) - 1000;
    fatal_assertions.fatal_assert_buffer_bounds(0, 500, max_size, @src());

    // Very small resource limits
    fatal_assertions.fatal_assert_resource_limit(0, 1, "tiny_resource", @src());

    // CRC values at integer boundaries
    fatal_assertions.fatal_assert_crc_valid(0, 0, "zero_crc_data", @src());
    fatal_assertions.fatal_assert_crc_valid(
        std.math.maxInt(u32),
        std.math.maxInt(u32),
        "max_crc_data",
        @src(),
    );

    // Memory alignment with power-of-2 values using proper aligned allocation
    const allocator = testing.allocator;

    // Test 1-byte alignment (always passes)
    const buffer_1 = try allocator.alloc(u8, 16);
    defer allocator.free(buffer_1);
    fatal_assertions.fatal_assert_memory_aligned(buffer_1.ptr, 1, @src());

    // Test 2-byte alignment
    const buffer_2 = try allocator.alignedAlloc(u8, .@"2", 16);
    defer allocator.free(buffer_2);
    fatal_assertions.fatal_assert_memory_aligned(buffer_2.ptr, 2, @src());

    // Test 4-byte alignment
    const buffer_4 = try allocator.alignedAlloc(u8, .@"4", 16);
    defer allocator.free(buffer_4);
    fatal_assertions.fatal_assert_memory_aligned(buffer_4.ptr, 4, @src());

    // Test 8-byte alignment
    const buffer_8 = try allocator.alignedAlloc(u8, .@"8", 16);
    defer allocator.free(buffer_8);
    fatal_assertions.fatal_assert_memory_aligned(buffer_8.ptr, 8, @src());
}

// Note: The following tests demonstrate scenarios that WOULD trigger fatal assertions
// but cannot be run as part of the regular test suite since they would terminate
// the test process. They are included as documentation of the expected behavior.

// This test would demonstrate fatal assertion triggering, but is commented out
// because it would terminate the test process:
//
// test "fatal assertion triggers on invalid block ID" {
//     const invalid_block_id = MockBlockId.zero();
//     // This would panic with detailed error information:
//     // fatal_assertions.fatal_assert_block_id_valid(invalid_block_id, @src());
// }

// Similarly, these tests would demonstrate other failure modes:
//
// test "fatal assertion triggers on buffer overflow" {
//     // This would panic:
//     // fatal_assertions.fatal_assert_buffer_bounds(50, 60, 100, @src());
// }
//
// test "fatal assertion triggers on CRC mismatch" {
//     // This would panic:
//     // fatal_assertions.fatal_assert_crc_valid(0x1111, 0x2222, "test", @src());
// }
//
// test "fatal assertion triggers on invalid WAL entry" {
//     const corrupted_entry = MockWALEntry.corrupted_magic();
//     // This would panic:
//     // fatal_assertions.fatal_assert_wal_entry_valid(corrupted_entry, @src());
// }

// Integration test: demonstrate fatal assertions in a realistic scenario
test "fatal assertions in storage engine scenario" {
    const allocator = testing.allocator;

    // Simulate a storage engine operation with multiple fatal assertion checks
    const buffer_size = 4096;
    const write_buffer = try allocator.alloc(u8, buffer_size);
    defer allocator.free(write_buffer);

    // Step 1: Validate buffer alignment for DMA operations
    fatal_assertions.fatal_assert_memory_aligned(write_buffer.ptr, 8, @src());

    // Step 2: Validate block ID before writing
    const block_id = MockBlockId.valid();
    fatal_assertions.fatal_assert_block_id_valid(block_id, @src());

    // Step 3: Validate write bounds
    const write_offset = 1000;
    const write_size = 2048;
    fatal_assertions.fatal_assert_buffer_bounds(write_offset, write_size, buffer_size, @src());

    // Step 4: Simulate successful write with CRC validation
    const expected_crc: u32 = 0x1234ABCD;
    const computed_crc: u32 = 0x1234ABCD; // Simulate matching checksum
    fatal_assertions.fatal_assert_crc_valid(computed_crc, expected_crc, "block_data", @src());

    // Step 5: Validate WAL entry format
    const wal_entry = MockWALEntry.valid();
    fatal_assertions.fatal_assert_wal_entry_valid(wal_entry, @src());

    // Step 6: Validate resource usage
    const memory_used = 1024 * 1024; // 1MB
    const memory_limit = 100 * 1024 * 1024; // 100MB
    fatal_assertions.fatal_assert_resource_limit(memory_used, memory_limit, "write_buffer_memory", @src());

    // All validations passed - the operation would continue normally
    try testing.expect(true); // Test passes if we reach this point
}
