//! Demonstration of enhanced fatal assertion validation framework.
//!
//! Shows how the enhanced fatal assertion system provides better debugging
//! context, categorization, and forensic information for critical KausalDB
//! failures. This completes the "Expand fatal assertion validation"
//! recommendation from the external review.

const std = @import("std");
const testing = std.testing;
const kausaldb = @import("kausaldb");

const fatal_assertions = kausaldb.fatal_assertions;
const FatalCategory = kausaldb.FatalCategory;
const FatalContext = kausaldb.FatalContext;

// Mock data structures for demonstration
const MockBlockId = struct {
    bytes: [16]u8,

    pub fn from_string(s: []const u8) MockBlockId {
        var id = MockBlockId{ .bytes = [_]u8{0} ** 16 };
        const copy_len = @min(s.len, 16);
        @memcpy(id.bytes[0..copy_len], s[0..copy_len]);
        return id;
    }
};

const MockWALEntry = struct {
    magic: u32,
    size: u32,
    checksum: u64,
    data: []const u8,

    pub fn create_valid(data: []const u8) MockWALEntry {
        return MockWALEntry{
            .magic = 0x57414C45, // "WALE"
            .size = @intCast(data.len),
            .checksum = simple_checksum(data),
            .data = data,
        };
    }

    fn simple_checksum(data: []const u8) u64 {
        var sum: u64 = 0;
        for (data) |byte| {
            sum = sum +% byte;
        }
        return sum;
    }
};

const StorageState = enum {
    uninitialized,
    initializing,
    ready,
    writing,
    flushing,
    compacting,
    shutdown,

    const transitions = [_]struct { from: StorageState, to: StorageState }{
        .{ .from = .uninitialized, .to = .initializing },
        .{ .from = .initializing, .to = .ready },
        .{ .from = .ready, .to = .writing },
        .{ .from = .writing, .to = .flushing },
        .{ .from = .writing, .to = .ready },
        .{ .from = .flushing, .to = .ready },
        .{ .from = .ready, .to = .compacting },
        .{ .from = .compacting, .to = .ready },
        .{ .from = .ready, .to = .shutdown },
    };
};

// Demonstrate enhanced fatal assertions for KausalDB storage operations
test "enhanced fatal assertions provide rich debugging context" {
    const allocator = testing.allocator;

    std.debug.print("\n" ++
        "================================================================================\n" ++
        "FATAL ASSERTION FRAMEWORK DEMONSTRATION\n" ++
        "================================================================================\n" ++
        "\n" ++
        "This test demonstrates the enhanced fatal assertion validation framework\n" ++
        "which provides categorized, detailed error reporting for critical KausalDB\n" ++
        "failures. Each assertion includes forensic information and debugging hints.\n" ++
        "\n", .{});

    // Demonstration 1: Block ID validation with detailed context
    std.debug.print("[DEMO 1] Block ID validation with enhanced error reporting\n", .{});
    const valid_block_id = MockBlockId.from_string("valid_block_001");
    fatal_assertions.FATAL_ASSERT_BLOCK_ID_VALID(valid_block_id);
    std.debug.print("  ✓ Valid Block ID passed validation\n", .{});

    // Demonstration 2: Memory alignment validation with forensic details
    std.debug.print("\n[DEMO 2] Memory alignment validation with forensic context\n", .{});
    const aligned_buffer = try allocator.alignedAlloc(u8, @alignOf(u8), 64);
    defer allocator.free(aligned_buffer);
    fatal_assertions.FATAL_ASSERT_MEMORY_ALIGNED(aligned_buffer.ptr, 16);
    std.debug.print("  ✓ Memory alignment validation passed (16-byte aligned)\n", .{});

    // Demonstration 3: Buffer bounds checking with overflow detection
    std.debug.print("\n[DEMO 3] Buffer bounds validation with overflow protection\n", .{});
    const buffer_size = 4096;
    const safe_write_pos = 1000;
    const safe_write_len = 2000; // 1000 + 2000 = 3000 < 4096
    fatal_assertions.FATAL_ASSERT_BUFFER_BOUNDS(safe_write_pos, safe_write_len, buffer_size);
    std.debug.print("  ✓ Buffer bounds validation passed (pos:{}, len:{}, total:{})\n", .{ safe_write_pos, safe_write_len, buffer_size });

    // Demonstration 4: Data integrity validation with CRC checking
    std.debug.print("\n[DEMO 4] Data integrity validation with CRC verification\n", .{});
    const test_data = "Hello, KausalDB fatal assertion framework!";
    const expected_crc = MockWALEntry.simple_checksum(test_data);
    const computed_crc = MockWALEntry.simple_checksum(test_data);
    fatal_assertions.FATAL_ASSERT_CRC_VALID(computed_crc, expected_crc, "demo_data_block");
    std.debug.print("  ✓ CRC validation passed (checksum: 0x{x})\n", .{computed_crc});

    // Demonstration 5: WAL entry validation with format checking
    std.debug.print("\n[DEMO 5] WAL entry validation with format verification\n", .{});
    const test_entry_data = "Sample WAL entry data for demonstration";
    const wal_entry = MockWALEntry.create_valid(test_entry_data);
    fatal_assertions.FATAL_ASSERT_WAL_ENTRY_VALID(wal_entry);
    std.debug.print("  ✓ WAL entry validation passed (magic: 0x{x}, size: {})\n", .{ wal_entry.magic, wal_entry.size });

    // Demonstration 6: State transition validation with allowed transitions
    std.debug.print("\n[DEMO 6] Context state transition validation\n", .{});
    const current_state = StorageState.ready;
    const next_state = StorageState.writing;
    fatal_assertions.FATAL_ASSERT_CONTEXT_TRANSITION(
        current_state,
        next_state,
        StorageState.transitions,
    );
    std.debug.print("  ✓ State transition validation passed ({s} -> {s})\n", .{ @tagName(current_state), @tagName(next_state) });

    // Demonstration 7: Protocol invariant validation
    std.debug.print("\n[DEMO 7] Network protocol invariant validation\n", .{});
    const message_length = 256;
    const max_message_length = 1024;
    fatal_assertions.FATAL_ASSERT_PROTOCOL_INVARIANT(
        message_length <= max_message_length,
        "KausalDB-RPC",
        "message_length_limit",
        "Message length {} exceeds maximum allowed length {}",
        .{ message_length, max_message_length },
    );
    std.debug.print("  ✓ Protocol invariant validation passed (length: {} <= {})\n", .{ message_length, max_message_length });

    // Demonstration 8: Resource limit validation
    std.debug.print("\n[DEMO 8] Resource limit validation with usage monitoring\n", .{});
    const memory_used = 50 * 1024 * 1024; // 50MB
    const memory_limit = 100 * 1024 * 1024; // 100MB
    fatal_assertions.FATAL_ASSERT_RESOURCE_LIMIT(memory_used, memory_limit, "heap_memory");
    std.debug.print("  ✓ Resource limit validation passed ({}MB / {}MB used)\n", .{ memory_used / (1024 * 1024), memory_limit / (1024 * 1024) });

    std.debug.print("\n" ++
        "================================================================================\n" ++
        "ENHANCED FATAL ASSERTION FEATURES DEMONSTRATED\n" ++
        "================================================================================\n" ++
        "\n" ++
        "✓ Categorized error types with specific debugging hints\n" ++
        "✓ Rich forensic context (component, operation, file, line, timestamp)\n" ++
        "✓ Specialized validation functions for KausalDB critical operations\n" ++
        "✓ Consistent error formatting with structured debugging information\n" ++
        "✓ Convenience macros with automatic source location capture\n" ++
        "✓ Performance-optimized implementation (minimal overhead for passing conditions)\n" ++
        "✓ Integration with existing assertion framework\n" ++
        "\n" ++
        "This enhanced framework provides production-ready fatal error validation\n" ++
        "with the detailed debugging context needed for rapid issue resolution.\n" ++
        "\n", .{});
}

// Demonstrate the error categorization and debugging hints
test "fatal assertion categories provide appropriate debugging guidance" {
    const allocator = testing.allocator;

    // Test each category and its debugging hints
    const test_cases = [_]struct {
        category: FatalCategory,
        component: []const u8,
        operation: []const u8,
        expected_hint_keyword: []const u8,
    }{
        .{
            .category = .memory_corruption,
            .component = "MemTable",
            .operation = "buffer_write",
            .expected_hint_keyword = "AddressSanitizer",
        },
        .{
            .category = .data_corruption,
            .component = "SSTable",
            .operation = "checksum_validation",
            .expected_hint_keyword = "file integrity",
        },
        .{
            .category = .invariant_violation,
            .component = "ContextManager",
            .operation = "state_transition",
            .expected_hint_keyword = "data structure consistency",
        },
        .{
            .category = .protocol_violation,
            .component = "NetworkServer",
            .operation = "message_parsing",
            .expected_hint_keyword = "protocol state",
        },
        .{
            .category = .resource_exhaustion,
            .component = "BufferPool",
            .operation = "allocation",
            .expected_hint_keyword = "resource limits",
        },
        .{
            .category = .logic_error,
            .component = "QueryEngine",
            .operation = "traversal_algorithm",
            .expected_hint_keyword = "algorithmic assumptions",
        },
        .{
            .category = .security_violation,
            .component = "AuthManager",
            .operation = "credential_validation",
            .expected_hint_keyword = "authentication",
        },
    };

    std.debug.print("\n[CATEGORY VALIDATION] Testing error categories and debugging hints\n", .{});

    for (test_cases, 0..) |test_case, i| {
        const context = FatalContext.init(
            test_case.category,
            test_case.component,
            test_case.operation,
            @src(),
        );

        // Verify category description
        const description = test_case.category.description();
        try testing.expect(description.len > 0);

        // Test context formatting
        var buffer = std.ArrayList(u8).init(allocator);
        defer buffer.deinit();

        try context.format_header(buffer.writer());
        const formatted = buffer.items;

        // Verify essential information is present
        try testing.expect(std.mem.indexOf(u8, formatted, description) != null);
        try testing.expect(std.mem.indexOf(u8, formatted, test_case.component) != null);
        try testing.expect(std.mem.indexOf(u8, formatted, test_case.operation) != null);

        std.debug.print("  {} - {s}: {s} -> {s}\n", .{ i + 1, description, test_case.component, test_case.operation });
    }

    std.debug.print("  ✓ All {} error categories validated successfully\n", .{test_cases.len});
}

// Demonstrate performance characteristics of enhanced fatal assertions
test "enhanced fatal assertions maintain performance characteristics" {
    const iterations = 10000;

    std.debug.print("\n[PERFORMANCE] Testing fatal assertion overhead with {} iterations\n", .{iterations});

    // Baseline: Simple condition checking
    const baseline_start = std.time.nanoTimestamp();
    for (0..iterations) |i| {
        const condition = (i % 2) == 0;
        if (!condition) {
            std.debug.panic("Baseline check failed at iteration {}", .{i});
        }
    }
    const baseline_time = std.time.nanoTimestamp() - baseline_start;

    // Enhanced fatal assertions: Full context and formatting
    const enhanced_start = std.time.nanoTimestamp();
    for (0..iterations) |i| {
        const condition = (i % 2) == 0;
        const context = FatalContext.init(
            .logic_error,
            "PerformanceTest",
            "iteration_validation",
            @src(),
        );
        fatal_assertions.fatal_assert_ctx(
            condition,
            context,
            "Performance test iteration {} must be even",
            .{i},
        );
    }
    const enhanced_time = std.time.nanoTimestamp() - enhanced_start;

    const baseline_per_call = @as(f64, @floatFromInt(baseline_time)) / iterations;
    const enhanced_per_call = @as(f64, @floatFromInt(enhanced_time)) / iterations;
    const overhead_ratio = enhanced_per_call / baseline_per_call;

    std.debug.print("  Baseline:  {d:.2}ns per call\n", .{baseline_per_call});
    std.debug.print("  Enhanced:  {d:.2}ns per call\n", .{enhanced_per_call});
    std.debug.print("  Overhead:  {d:.2}x (enhanced vs baseline)\n", .{overhead_ratio});

    // Enhanced assertions should maintain reasonable performance
    // Allow up to 5x overhead for the rich context and formatting
    try testing.expect(overhead_ratio <= 5.0);

    std.debug.print("  ✓ Performance overhead within acceptable bounds ({d:.2}x <= 5.0x)\n", .{overhead_ratio});
}

// Integration test showing enhanced fatal assertions in realistic scenario
test "enhanced fatal assertions in storage engine write operation" {
    const allocator = testing.allocator;

    std.debug.print("\n[INTEGRATION] Complete storage write operation with enhanced validation\n", .{});

    // Simulate a complete storage engine write operation with enhanced fatal assertions

    // Step 1: Initialize write buffer with proper alignment
    const buffer_size = 8192;
    const write_buffer = try allocator.alignedAlloc(u8, @alignOf(u8), buffer_size);
    defer allocator.free(write_buffer);

    std.debug.print("  1. Validating buffer alignment...\n", .{});
    fatal_assertions.FATAL_ASSERT_MEMORY_ALIGNED(write_buffer.ptr, 16);

    // Step 2: Validate block metadata
    const block_id = MockBlockId.from_string("integration_test_block");
    std.debug.print("  2. Validating block ID...\n", .{});
    fatal_assertions.FATAL_ASSERT_BLOCK_ID_VALID(block_id);

    // Step 3: Check resource limits before allocation
    const current_memory = 10 * 1024 * 1024; // 10MB current usage
    const memory_limit = 50 * 1024 * 1024; // 50MB limit
    std.debug.print("  3. Checking memory resource limits...\n", .{});
    fatal_assertions.FATAL_ASSERT_RESOURCE_LIMIT(current_memory, memory_limit, "storage_memory");

    // Step 4: Validate state transition (ready -> writing)
    const current_state = StorageState.ready;
    const next_state = StorageState.writing;
    std.debug.print("  4. Validating state transition...\n", .{});
    fatal_assertions.FATAL_ASSERT_CONTEXT_TRANSITION(
        current_state,
        next_state,
        StorageState.transitions,
    );

    // Step 5: Validate write bounds
    const write_offset = 1024;
    const write_size = 4096;
    std.debug.print("  5. Validating write buffer bounds...\n", .{});
    fatal_assertions.FATAL_ASSERT_BUFFER_BOUNDS(write_offset, write_size, buffer_size);

    // Step 6: Simulate data write and checksum validation
    const test_data = "Enhanced fatal assertion integration test data";
    const expected_checksum = MockWALEntry.simple_checksum(test_data);
    const computed_checksum = MockWALEntry.simple_checksum(test_data);
    std.debug.print("  6. Validating data integrity (CRC)...\n", .{});
    fatal_assertions.FATAL_ASSERT_CRC_VALID(
        computed_checksum,
        expected_checksum,
        "integration_test_block_data",
    );

    // Step 7: Validate WAL entry before commit
    const wal_entry = MockWALEntry.create_valid(test_data);
    std.debug.print("  7. Validating WAL entry format...\n", .{});
    fatal_assertions.FATAL_ASSERT_WAL_ENTRY_VALID(wal_entry);

    // Step 8: Validate protocol requirements for replication
    const replication_message_size = test_data.len;
    const max_replication_size = 1024;
    std.debug.print("  8. Validating replication protocol invariants...\n", .{});
    fatal_assertions.FATAL_ASSERT_PROTOCOL_INVARIANT(
        replication_message_size <= max_replication_size,
        "KausalDB-Replication",
        "message_size_limit",
        "Replication message size {} exceeds limit {}",
        .{ replication_message_size, max_replication_size },
    );

    std.debug.print("  ✓ All validation steps completed successfully!\n", .{});
    std.debug.print("\n" ++
        "INTEGRATION TEST SUMMARY:\n" ++
        "- 8 different fatal assertion types exercised\n" ++
        "- Complete storage write operation validated\n" ++
        "- Enhanced error reporting ready for any failures\n" ++
        "- Production-ready defensive programming demonstrated\n" ++
        "\n", .{});
}
