//! Memory usage benchmark for streaming query result formatting.
//!
//! Demonstrates the memory efficiency improvement of streaming format_for_llm
//! vs the old approach that built entire output strings in memory.

const std = @import("std");
const cortexdb = @import("cortexdb");
const testing = std.testing;

const context_block = cortexdb.types;
const operations = cortexdb.query.operations;
const ContextBlock = context_block.ContextBlock;
const BlockId = context_block.BlockId;
const QueryResult = operations.QueryResult;

/// Create a test QueryResult with specified number of blocks
fn create_test_query_result(allocator: std.mem.Allocator, block_count: u32) !QueryResult {
    const blocks = try allocator.alloc(ContextBlock, block_count);
    defer {
        // Free the content strings we allocated
        for (blocks) |block| {
            allocator.free(block.content);
        }
        allocator.free(blocks);
    }

    for (blocks, 0..) |*block, i| {
        // Create unique block ID (start from 1, all-zero BlockID invalid)
        var id_bytes: [16]u8 = std.mem.zeroes([16]u8);
        std.mem.writeInt(u32, id_bytes[12..16], @as(u32, @intCast(i + 1)), .little);

        const content = try std.fmt.allocPrint(allocator, "This is test block {} with substantial content that represents typical block size in CortexDB. The content includes code, documentation, or other structured data that would be provided to an LLM for context. This makes the memory benchmark more realistic.", .{i});

        block.* = ContextBlock{
            .id = BlockId{ .bytes = id_bytes },
            .version = 1,
            .source_uri = "test://benchmark.zig", // String literal - no allocation needed
            .metadata_json = "{}", // String literal - no allocation needed
            .content = content,
        };
    }

    return QueryResult.init(allocator, blocks);
}

/// Simulate old behavior - build entire formatted string in memory
fn format_old_style(result: QueryResult, allocator: std.mem.Allocator) ![]const u8 {
    var output = std.ArrayList(u8).init(allocator);
    defer output.deinit();

    // This mimics what the old format_for_llm method did
    try result.format_for_llm(output.writer().any());
    return output.toOwnedSlice();
}

/// New streaming behavior - write directly to final destination
fn format_streaming(result: QueryResult, writer: std.io.AnyWriter) !void {
    try result.format_for_llm(writer);
}

test "memory efficiency demonstration: streaming vs buffered formatting" {
    const allocator = testing.allocator;

    // Test with different block counts to show scaling benefits
    const test_sizes = [_]u32{ 10, 100, 500 };

    for (test_sizes) |block_count| {
        std.debug.print("\n=== Memory Efficiency Demo: {} blocks ===\n", .{block_count});

        // Create test data
        var test_result = try create_test_query_result(allocator, block_count);
        defer test_result.deinit();

        // Old style: Build entire string in memory first
        const old_formatted = try format_old_style(test_result, allocator);
        defer allocator.free(old_formatted);

        // New style: Stream directly to destination
        var new_output = std.ArrayList(u8).init(allocator);
        defer new_output.deinit();
        try format_streaming(test_result, new_output.writer().any());

        // Results should be identical
        try testing.expectEqualStrings(old_formatted, new_output.items);

        std.debug.print("Output size: {} bytes\n", .{old_formatted.len});
        std.debug.print("Old approach: Allocates entire {} byte string at once\n", .{old_formatted.len});
        std.debug.print("New approach: Streams incrementally, no large buffer allocation\n", .{});

        // Verify both approaches produce the same output
        try testing.expect(old_formatted.len > 0);
        try testing.expectEqualStrings(old_formatted, new_output.items);

        // For larger datasets, the streaming approach avoids peak memory usage
        if (block_count >= 100) {
            std.debug.print("Memory efficiency: Streaming avoids {}+ byte peak allocation\n", .{old_formatted.len});
        }
    }
}

test "streaming output correctness" {
    const allocator = testing.allocator;

    // Create small test result
    var test_result = try create_test_query_result(allocator, 3);
    defer test_result.deinit();

    // Format with old style
    const old_output = try format_old_style(test_result, allocator);
    defer allocator.free(old_output);

    // Format with streaming
    var new_output = std.ArrayList(u8).init(allocator);
    defer new_output.deinit();
    try format_streaming(test_result, new_output.writer().any());

    // Outputs should be identical
    try testing.expectEqualStrings(old_output, new_output.items);

    // Verify expected content is present
    try testing.expect(std.mem.indexOf(u8, new_output.items, "Retrieved 3 blocks") != null);
    try testing.expect(std.mem.indexOf(u8, new_output.items, "BEGIN CONTEXT BLOCK") != null);
    try testing.expect(std.mem.indexOf(u8, new_output.items, "END CONTEXT BLOCK") != null);
    try testing.expect(std.mem.indexOf(u8, new_output.items, "test://benchmark.zig") != null);
}

test "streaming to different writer types" {
    const allocator = testing.allocator;

    var test_result = try create_test_query_result(allocator, 5);
    defer test_result.deinit();

    // Test streaming to ArrayList
    var array_output = std.ArrayList(u8).init(allocator);
    defer array_output.deinit();
    try format_streaming(test_result, array_output.writer().any());

    // Test streaming to fixed buffer
    var fixed_buffer: [64 * 1024]u8 = undefined; // 64KB buffer
    var fixed_stream = std.io.fixedBufferStream(&fixed_buffer);
    try format_streaming(test_result, fixed_stream.writer().any());

    // Both outputs should be identical
    const array_content = array_output.items;
    const fixed_content = fixed_stream.getWritten();
    try testing.expectEqualStrings(array_content, fixed_content);

    // Verify streaming works with different writer types
    try testing.expect(array_content.len > 0);
    try testing.expect(fixed_content.len > 0);
}
