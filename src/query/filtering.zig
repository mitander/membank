//! Advanced filtering system for CortexDB query operations.
//!
//! Provides configurable filter conditions for block metadata, content,
//! and graph structure. Supports complex boolean logic and efficient
//! execution against the storage layer.

const std = @import("std");
const assert = @import("../core/assert.zig").assert;
const storage = @import("../storage/engine.zig");
const context_block = @import("../core/types.zig");
const simulation_vfs = @import("../sim/simulation_vfs.zig");
const testing = std.testing;

const StorageEngine = storage.StorageEngine;
const ContextBlock = context_block.ContextBlock;
const BlockId = context_block.BlockId;
const SimulationVFS = simulation_vfs.SimulationVFS;

/// Filtering operation errors
pub const FilterError = error{
    /// Invalid filter expression
    InvalidFilter,
    /// Empty filter query
    EmptyQuery,
    /// Maximum results exceeded
    ResultsLimitExceeded,
    /// Metadata parsing failed
    MetadataParseError,
} || std.mem.Allocator.Error;

/// Filter condition operators for numeric and string comparisons
pub const FilterOperator = enum(u8) {
    equal = 0x01,
    not_equal = 0x02,
    greater_than = 0x03,
    greater_equal = 0x04,
    less_than = 0x05,
    less_equal = 0x06,
    contains = 0x07,
    starts_with = 0x08,
    ends_with = 0x09,
    regex_match = 0x0A,

    pub fn from_u8(value: u8) !FilterOperator {
        return std.meta.intToEnum(FilterOperator, value) catch FilterError.InvalidFilter;
    }
};

/// Boolean logic operators for combining filter conditions
pub const LogicalOperator = enum(u8) {
    and_op = 0x01,
    or_op = 0x02,
    not_op = 0x03,

    pub fn from_u8(value: u8) !LogicalOperator {
        return std.meta.intToEnum(LogicalOperator, value) catch FilterError.InvalidFilter;
    }
};

/// Filter target specifies which part of the block to filter on
pub const FilterTarget = enum(u8) {
    content = 0x01,
    source_uri = 0x02,
    metadata_field = 0x03,
    version = 0x04,
    content_length = 0x05,

    pub fn from_u8(value: u8) !FilterTarget {
        return std.meta.intToEnum(FilterTarget, value) catch FilterError.InvalidFilter;
    }
};

/// Individual filter condition with target, operator, and value
pub const FilterCondition = struct {
    target: FilterTarget,
    operator: FilterOperator,
    value: []const u8,
    metadata_field: ?[]const u8, // Used when target is metadata_field

    pub fn init(target: FilterTarget, operator: FilterOperator, value: []const u8) FilterCondition {
        return FilterCondition{
            .target = target,
            .operator = operator,
            .value = value,
            .metadata_field = null,
        };
    }

    pub fn init_metadata(field_name: []const u8, operator: FilterOperator, value: []const u8) FilterCondition {
        return FilterCondition{
            .target = .metadata_field,
            .operator = operator,
            .value = value,
            .metadata_field = field_name,
        };
    }

    /// Evaluate this condition against a block
    pub fn matches(self: FilterCondition, block: ContextBlock, allocator: std.mem.Allocator) !bool {
        const target_value = try self.extract_target_value(block, allocator);
        defer if (self.target == .metadata_field) allocator.free(target_value);

        return self.evaluate_comparison(target_value);
    }

    /// Extract the value from the block based on the filter target
    fn extract_target_value(self: FilterCondition, block: ContextBlock, allocator: std.mem.Allocator) ![]const u8 {
        return switch (self.target) {
            .content => block.content,
            .source_uri => block.source_uri,
            .version => blk: {
                const buffer = try allocator.alloc(u8, 32);
                _ = std.fmt.bufPrint(buffer, "{d}", .{block.version}) catch unreachable;
                break :blk buffer;
            },
            .content_length => blk: {
                const buffer = try allocator.alloc(u8, 16);
                _ = std.fmt.bufPrint(buffer, "{d}", .{block.content.len}) catch unreachable;
                break :blk buffer;
            },
            .metadata_field => blk: {
                if (self.metadata_field) |field_name| {
                    break :blk try extract_metadata_field(block.metadata_json, field_name, allocator);
                }
                return FilterError.InvalidFilter;
            },
        };
    }

    /// Evaluate the comparison operation between target and filter values
    fn evaluate_comparison(self: FilterCondition, target_value: []const u8) bool {
        return switch (self.operator) {
            .equal => std.mem.eql(u8, target_value, self.value),
            .not_equal => !std.mem.eql(u8, target_value, self.value),
            .greater_than => compare_numeric_or_lexical(target_value, self.value) > 0,
            .greater_equal => compare_numeric_or_lexical(target_value, self.value) >= 0,
            .less_than => compare_numeric_or_lexical(target_value, self.value) < 0,
            .less_equal => compare_numeric_or_lexical(target_value, self.value) <= 0,
            .contains => std.mem.indexOf(u8, target_value, self.value) != null,
            .starts_with => std.mem.startsWith(u8, target_value, self.value),
            .ends_with => std.mem.endsWith(u8, target_value, self.value),
            .regex_match => false, // TODO: Implement regex matching when needed
        };
    }
};

/// Complex filter expression supporting boolean logic
pub const FilterExpression = union(enum) {
    condition: FilterCondition,
    logical: struct {
        operator: LogicalOperator,
        left: *FilterExpression,
        right: ?*FilterExpression, // null for NOT operator
    },

    /// Evaluate this expression against a block
    pub fn matches(self: FilterExpression, block: ContextBlock, allocator: std.mem.Allocator) !bool {
        return switch (self) {
            .condition => |cond| try cond.matches(block, allocator),
            .logical => |logical| switch (logical.operator) {
                .and_op => {
                    const left_result = try logical.left.matches(block, allocator);
                    if (!left_result) return false;
                    if (logical.right) |right| {
                        return try right.matches(block, allocator);
                    }
                    return left_result;
                },
                .or_op => {
                    const left_result = try logical.left.matches(block, allocator);
                    if (left_result) return true;
                    if (logical.right) |right| {
                        return try right.matches(block, allocator);
                    }
                    return left_result;
                },
                .not_op => {
                    return !(try logical.left.matches(block, allocator));
                },
            },
        };
    }
};

/// Query configuration for filtered block retrieval
pub const FilteredQuery = struct {
    expression: FilterExpression,
    max_results: u32 = 1000,
    offset: u32 = 0,

    pub fn init(expression: FilterExpression) FilteredQuery {
        return FilteredQuery{ .expression = expression };
    }

    pub fn validate(self: FilteredQuery) !void {
        if (self.max_results == 0) return FilterError.EmptyQuery;
        if (self.max_results > 10000) return FilterError.ResultsLimitExceeded;
    }
};

/// Result set from a filtered query operation
pub const FilteredQueryResult = struct {
    blocks: []ContextBlock,
    total_matches: u32,
    has_more: bool,
    allocator: std.mem.Allocator,

    pub fn init(allocator: std.mem.Allocator, blocks: []ContextBlock, total_matches: u32, has_more: bool) FilteredQueryResult {
        return FilteredQueryResult{
            .allocator = allocator,
            .blocks = blocks,
            .total_matches = total_matches,
            .has_more = has_more,
        };
    }

    pub fn deinit(self: FilteredQueryResult) void {
        for (self.blocks) |block| {
            self.allocator.free(block.source_uri);
            self.allocator.free(block.metadata_json);
            self.allocator.free(block.content);
        }
        self.allocator.free(self.blocks);
    }

    /// Format results for LLM consumption with metadata about filtering
    pub fn format_for_llm(self: FilteredQueryResult, allocator: std.mem.Allocator) ![]const u8 {
        var result = std.ArrayList(u8).init(allocator);
        defer result.deinit();

        const writer = result.writer();
        try writer.print("Found {} matching blocks", .{self.total_matches});
        if (self.has_more) {
            try writer.print(" (showing first {})", .{self.blocks.len});
        }
        try writer.print(":\n\n");

        for (self.blocks, 0..) |block, i| {
            try writer.print("Block {} (ID: {}):\n", .{ i + 1, block.id });
            try writer.print("Source: {s}\n", .{block.source_uri});
            try writer.print("Type: {s}\n", .{@tagName(block.block_type)});
            try writer.print("Content: {s}\n\n", .{block.content});
        }

        return result.toOwnedSlice();
    }
};

/// Execute a filtered query against the storage engine
pub fn execute_filtered_query(
    allocator: std.mem.Allocator,
    storage_engine: *StorageEngine,
    query: FilteredQuery,
) !FilteredQueryResult {
    try query.validate();

    var matched_blocks = std.ArrayList(ContextBlock).init(allocator);
    defer matched_blocks.deinit();

    var total_matches: u32 = 0;
    var blocks_collected: u32 = 0;
    const max_to_collect = query.max_results;

    // Scan through all blocks and apply filter
    // TODO: Optimize with indices for common filter patterns
    var iterator = storage_engine.iterate_all_blocks();

    while (try iterator.next()) |block| {
        if (try query.expression.matches(block, allocator)) {
            total_matches += 1;

            // Skip blocks before offset
            if (total_matches <= query.offset) continue;

            // Collect blocks up to max_results
            if (blocks_collected < max_to_collect) {
                try matched_blocks.append(try clone_block(allocator, block));
                blocks_collected += 1;
            }
        }
    }

    const has_more = total_matches > query.offset + blocks_collected;
    return FilteredQueryResult.init(
        allocator,
        try matched_blocks.toOwnedSlice(),
        total_matches,
        has_more,
    );
}

/// Create simple filter condition for common use cases
pub fn filter_by_content_contains(text: []const u8) FilterCondition {
    return FilterCondition.init(.content, .contains, text);
}

pub fn filter_by_source_uri(uri: []const u8) FilterCondition {
    return FilterCondition.init(.source_uri, .equal, uri);
}

pub fn filter_by_metadata_field(field_name: []const u8, value: []const u8) FilterCondition {
    return FilterCondition.init_metadata(field_name, .equal, value);
}

/// Helper function to extract a field from JSON metadata
fn extract_metadata_field(metadata_json: []const u8, field_name: []const u8, allocator: std.mem.Allocator) ![]const u8 {
    // Simple JSON field extraction - replace with proper JSON parser if needed
    const field_pattern = try std.fmt.allocPrint(allocator, "\"{s}\":", .{field_name});
    defer allocator.free(field_pattern);

    const start_pos = std.mem.indexOf(u8, metadata_json, field_pattern) orelse return "";
    const value_start = start_pos + field_pattern.len;

    // Skip whitespace and quotes
    var pos = value_start;
    while (pos < metadata_json.len and (metadata_json[pos] == ' ' or metadata_json[pos] == '"')) {
        pos += 1;
    }

    // Find end of value
    var end_pos = pos;
    while (end_pos < metadata_json.len and metadata_json[end_pos] != '"' and metadata_json[end_pos] != ',') {
        end_pos += 1;
    }

    if (pos >= end_pos) return "";

    const value = metadata_json[pos..end_pos];
    return try allocator.dupe(u8, value);
}

/// Compare values numerically if possible, otherwise lexically
fn compare_numeric_or_lexical(a: []const u8, b: []const u8) i8 {
    // Try numeric comparison first
    const a_num = std.fmt.parseFloat(f64, a) catch null;
    const b_num = std.fmt.parseFloat(f64, b) catch null;

    if (a_num != null and b_num != null) {
        if (a_num.? < b_num.?) return -1;
        if (a_num.? > b_num.?) return 1;
        return 0;
    }

    // Fall back to lexical comparison
    return switch (std.mem.order(u8, a, b)) {
        .lt => -1,
        .gt => 1,
        .eq => 0,
    };
}

/// Create a deep copy of a context block for result storage
fn clone_block(allocator: std.mem.Allocator, block: ContextBlock) !ContextBlock {
    return ContextBlock{
        .id = block.id,
        .version = block.version,
        .source_uri = try allocator.dupe(u8, block.source_uri),
        .metadata_json = try allocator.dupe(u8, block.metadata_json),
        .content = try allocator.dupe(u8, block.content),
    };
}

// Tests

test "filter condition - content contains" {
    const allocator = testing.allocator;

    const block = ContextBlock{
        .id = 1,
        .version = 1,
        .source_uri = "test.zig",
        .metadata_json = "{}",
        .content = "pub fn hello() void {}",
    };

    const condition = filter_by_content_contains("hello");
    try testing.expect(try condition.matches(block, allocator));

    const no_match_condition = filter_by_content_contains("goodbye");
    try testing.expect(!try no_match_condition.matches(block, allocator));
}

test "filter condition - metadata field extraction" {
    const allocator = testing.allocator;

    const block = ContextBlock{
        .id = 1,
        .version = 1,
        .source_uri = "test.zig",
        .metadata_json = "{\"language\": \"zig\", \"complexity\": \"low\"}",
        .content = "content",
    };

    const condition = filter_by_metadata_field("language", "zig");
    try testing.expect(try condition.matches(block, allocator));

    const no_match_condition = filter_by_metadata_field("language", "rust");
    try testing.expect(!try no_match_condition.matches(block, allocator));
}

test "filter expression - logical AND" {
    const allocator = testing.allocator;

    const block = ContextBlock{
        .id = 1,
        .version = 1,
        .source_uri = "main.zig",
        .metadata_json = "{}",
        .content = "pub fn main() void {}",
    };

    const cond1 = FilterExpression{ .condition = filter_by_content_contains("main") };
    const cond2 = FilterExpression{ .condition = filter_by_source_uri("main.zig") };

    const and_expr = FilterExpression{
        .logical = .{
            .operator = .and_op,
            .left = @constCast(&cond1),
            .right = @constCast(&cond2),
        },
    };

    try testing.expect(try and_expr.matches(block, allocator));
}

test "filtered query validation" {
    const condition = FilterExpression{ .condition = filter_by_content_contains("test") };

    // Valid query
    var query = FilteredQuery.init(condition);
    try query.validate();

    // Invalid - zero max results
    query.max_results = 0;
    try testing.expectError(FilterError.EmptyQuery, query.validate());

    // Invalid - too many results
    query.max_results = 20000;
    try testing.expectError(FilterError.ResultsLimitExceeded, query.validate());
}

test "filter operators - comparison operations" {
    const allocator = testing.allocator;

    const block_v1 = ContextBlock{
        .id = 1,
        .version = 1,
        .source_uri = "test_v1.zig",
        .metadata_json = "{}",
        .content = "version 1 content",
    };

    const block_v2 = ContextBlock{
        .id = 2,
        .version = 2,
        .source_uri = "test_v2.zig",
        .metadata_json = "{}",
        .content = "version 2 content",
    };

    // Test version greater than
    const gt_condition = FilterCondition.init(.version, .greater_than, "1");
    try testing.expect(!try gt_condition.matches(block_v1, allocator));
    try testing.expect(try gt_condition.matches(block_v2, allocator));

    // Test version less than or equal
    const lte_condition = FilterCondition.init(.version, .less_equal, "2");
    try testing.expect(try lte_condition.matches(block_v1, allocator));
    try testing.expect(try lte_condition.matches(block_v2, allocator));

    // Test not equal
    const ne_condition = FilterCondition.init(.version, .not_equal, "1");
    try testing.expect(!try ne_condition.matches(block_v1, allocator));
    try testing.expect(try ne_condition.matches(block_v2, allocator));
}

test "filter operators - string operations" {
    const allocator = testing.allocator;

    const block = ContextBlock{
        .id = 1,
        .version = 1,
        .source_uri = "hello_world.zig",
        .metadata_json = "{}",
        .content = "function hello_world() { return 'Hello, World!'; }",
    };

    // Test starts_with
    const starts_condition = FilterCondition.init(.source_uri, .starts_with, "hello");
    try testing.expect(try starts_condition.matches(block, allocator));

    const no_start_condition = FilterCondition.init(.source_uri, .starts_with, "goodbye");
    try testing.expect(!try no_start_condition.matches(block, allocator));

    // Test ends_with
    const ends_condition = FilterCondition.init(.source_uri, .ends_with, ".zig");
    try testing.expect(try ends_condition.matches(block, allocator));

    const no_end_condition = FilterCondition.init(.source_uri, .ends_with, ".rs");
    try testing.expect(!try no_end_condition.matches(block, allocator));

    // Test contains in content
    const contains_condition = FilterCondition.init(.content, .contains, "Hello, World!");
    try testing.expect(try contains_condition.matches(block, allocator));

    const no_contains_condition = FilterCondition.init(.content, .contains, "Goodbye");
    try testing.expect(!try no_contains_condition.matches(block, allocator));
}

test "filter expression - logical OR operation" {
    const allocator = testing.allocator;

    const block = ContextBlock{
        .id = 1,
        .version = 1,
        .source_uri = "main.zig",
        .metadata_json = "{}",
        .content = "pub fn main() void {}",
    };

    const cond1 = FilterExpression{ .condition = filter_by_content_contains("nonexistent") };
    const cond2 = FilterExpression{ .condition = filter_by_source_uri("main.zig") };

    const or_expr = FilterExpression{
        .logical = .{
            .operator = .or_op,
            .left = @constCast(&cond1),
            .right = @constCast(&cond2),
        },
    };

    // Should match because second condition is true
    try testing.expect(try or_expr.matches(block, allocator));

    // Test with both conditions false
    const false_cond1 = FilterExpression{ .condition = filter_by_content_contains("nonexistent") };
    const false_cond2 = FilterExpression{ .condition = filter_by_source_uri("missing.zig") };

    const false_or_expr = FilterExpression{
        .logical = .{
            .operator = .or_op,
            .left = @constCast(&false_cond1),
            .right = @constCast(&false_cond2),
        },
    };

    try testing.expect(!try false_or_expr.matches(block, allocator));
}

test "filter expression - logical NOT operation" {
    const allocator = testing.allocator;

    const block = ContextBlock{
        .id = 1,
        .version = 1,
        .source_uri = "test.zig",
        .metadata_json = "{}",
        .content = "test content",
    };

    const true_condition = FilterExpression{ .condition = filter_by_content_contains("test") };
    const not_expr = FilterExpression{
        .logical = .{
            .operator = .not_op,
            .left = @constCast(&true_condition),
            .right = null,
        },
    };

    // NOT true should be false
    try testing.expect(!try not_expr.matches(block, allocator));

    const false_condition = FilterExpression{ .condition = filter_by_content_contains("missing") };
    const not_false_expr = FilterExpression{
        .logical = .{
            .operator = .not_op,
            .left = @constCast(&false_condition),
            .right = null,
        },
    };

    // NOT false should be true
    try testing.expect(try not_false_expr.matches(block, allocator));
}

test "filter target - content length filtering" {
    const allocator = testing.allocator;

    const short_block = ContextBlock{
        .id = 1,
        .version = 1,
        .source_uri = "short.zig",
        .metadata_json = "{}",
        .content = "short", // 5 characters
    };

    const long_block = ContextBlock{
        .id = 2,
        .version = 1,
        .source_uri = "long.zig",
        .metadata_json = "{}",
        .content = "this is much longer content", // 27 characters
    };

    // Test content length greater than 10
    const length_condition = FilterCondition.init(.content_length, .greater_than, "10");
    try testing.expect(!try length_condition.matches(short_block, allocator));
    try testing.expect(try length_condition.matches(long_block, allocator));

    // Test content length equal to exact value
    const exact_length_condition = FilterCondition.init(.content_length, .equal, "5");
    try testing.expect(try exact_length_condition.matches(short_block, allocator));
    try testing.expect(!try exact_length_condition.matches(long_block, allocator));
}

test "complex nested filter expressions" {
    const allocator = testing.allocator;

    const block = ContextBlock{
        .id = 1,
        .version = 2,
        .source_uri = "complex.zig",
        .metadata_json = "{\"language\": \"zig\", \"complexity\": \"high\"}",
        .content = "complex function implementation",
    };

    // Create complex expression: (content contains "function" AND version > 1) OR metadata.language = "zig"
    const content_condition = FilterExpression{ .condition = filter_by_content_contains("function") };
    const version_condition = FilterExpression{ .condition = FilterCondition.init(.version, .greater_than, "1") };
    const metadata_condition = FilterExpression{ .condition = filter_by_metadata_field("language", "zig") };

    const and_expr = FilterExpression{
        .logical = .{
            .operator = .and_op,
            .left = @constCast(&content_condition),
            .right = @constCast(&version_condition),
        },
    };

    const complex_expr = FilterExpression{
        .logical = .{
            .operator = .or_op,
            .left = @constCast(&and_expr),
            .right = @constCast(&metadata_condition),
        },
    };

    try testing.expect(try complex_expr.matches(block, allocator));
}

test "filter operator enum parsing" {
    try testing.expectEqual(FilterOperator.equal, try FilterOperator.from_u8(0x01));
    try testing.expectEqual(FilterOperator.not_equal, try FilterOperator.from_u8(0x02));
    try testing.expectEqual(FilterOperator.greater_than, try FilterOperator.from_u8(0x03));
    try testing.expectEqual(FilterOperator.contains, try FilterOperator.from_u8(0x07));

    // Invalid operator should return error
    try testing.expectError(FilterError.InvalidFilter, FilterOperator.from_u8(0xFF));
}

test "logical operator enum parsing" {
    try testing.expectEqual(LogicalOperator.and_op, try LogicalOperator.from_u8(0x01));
    try testing.expectEqual(LogicalOperator.or_op, try LogicalOperator.from_u8(0x02));
    try testing.expectEqual(LogicalOperator.not_op, try LogicalOperator.from_u8(0x03));

    // Invalid operator should return error
    try testing.expectError(FilterError.InvalidFilter, LogicalOperator.from_u8(0xFF));
}

test "filter target enum parsing" {
    try testing.expectEqual(FilterTarget.content, try FilterTarget.from_u8(0x01));
    try testing.expectEqual(FilterTarget.source_uri, try FilterTarget.from_u8(0x02));
    try testing.expectEqual(FilterTarget.metadata_field, try FilterTarget.from_u8(0x03));
    try testing.expectEqual(FilterTarget.version, try FilterTarget.from_u8(0x04));
    try testing.expectEqual(FilterTarget.content_length, try FilterTarget.from_u8(0x05));

    // Invalid target should return error
    try testing.expectError(FilterError.InvalidFilter, FilterTarget.from_u8(0xFF));
}

test "filtered query result operations" {
    const allocator = testing.allocator;

    const test_blocks = [_]ContextBlock{
        .{
            .id = try BlockId.from_hex("1111111111111111111111111111111111111111"),
            .version = 1,
            .source_uri = "result1.zig",
            .metadata_json = "{}",
            .content = "first result",
        },
        .{
            .id = try BlockId.from_hex("2222222222222222222222222222222222222222"),
            .version = 1,
            .source_uri = "result2.zig",
            .metadata_json = "{}",
            .content = "second result",
        },
    };

    var result = FilteredQueryResult.init(allocator, try allocator.dupe(ContextBlock, &test_blocks), 5, true);
    defer result.deinit();

    try testing.expectEqual(@as(u32, 5), result.total_matches);
    try testing.expectEqual(@as(usize, 2), result.blocks.len);
    try testing.expect(result.has_more);

    // Test formatting
    const formatted = try result.format_for_llm(allocator);
    defer allocator.free(formatted);

    try testing.expect(std.mem.indexOf(u8, formatted, "Found 5 matching blocks") != null);
    try testing.expect(std.mem.indexOf(u8, formatted, "showing first 2") != null);
    try testing.expect(std.mem.indexOf(u8, formatted, "result1.zig") != null);
    try testing.expect(std.mem.indexOf(u8, formatted, "result2.zig") != null);
}

test "execute filtered query with storage engine" {
    const allocator = testing.allocator;

    // Create test storage engine
    var sim_vfs = SimulationVFS.init(allocator);
    defer sim_vfs.deinit();

    var storage_engine = try StorageEngine.init(allocator, sim_vfs.vfs(), "./test_filtering");
    defer storage_engine.deinit();
    try storage_engine.startup();

    // Add test blocks with varying content
    const blocks = [_]ContextBlock{
        .{
            .id = try BlockId.from_hex("1111111111111111111111111111111111111111"),
            .version = 1,
            .source_uri = "match1.zig",
            .metadata_json = "{\"type\": \"function\"}",
            .content = "function test_match() {}", // Contains "test"
        },
        .{
            .id = try BlockId.from_hex("2222222222222222222222222222222222222222"),
            .version = 1,
            .source_uri = "match2.zig",
            .metadata_json = "{\"type\": \"variable\"}",
            .content = "var test_variable = 42;", // Contains "test"
        },
        .{
            .id = try BlockId.from_hex("3333333333333333333333333333333333333333"),
            .version = 1,
            .source_uri = "nomatch.zig",
            .metadata_json = "{\"type\": \"struct\"}",
            .content = "struct Example {}", // No "test"
        },
    };

    for (blocks) |block| {
        try storage_engine.put_block(block);
    }

    // Execute filtered query for content containing "test"
    const condition = FilterExpression{ .condition = filter_by_content_contains("test") };
    const query = FilteredQuery.init(condition);

    const result = try execute_filtered_query(allocator, &storage_engine, query);
    defer result.deinit();

    // Should find 2 matching blocks
    try testing.expectEqual(@as(u32, 2), result.total_matches);
    try testing.expectEqual(@as(usize, 2), result.blocks.len);
    try testing.expect(!result.has_more);

    // Verify correct blocks were found
    var found_function = false;
    var found_variable = false;
    var found_struct = false;

    for (result.blocks) |block| {
        if (std.mem.indexOf(u8, block.content, "function") != null) found_function = true;
        if (std.mem.indexOf(u8, block.content, "var") != null) found_variable = true;
        if (std.mem.indexOf(u8, block.content, "struct") != null) found_struct = true;
    }

    try testing.expect(found_function and found_variable and !found_struct);
}

test "filtered query with pagination" {
    const allocator = testing.allocator;

    // Create test storage engine
    var sim_vfs = SimulationVFS.init(allocator);
    defer sim_vfs.deinit();

    var storage_engine = try StorageEngine.init(allocator, sim_vfs.vfs(), "./test_pagination");
    defer storage_engine.deinit();
    try storage_engine.startup();

    // Add multiple matching blocks
    for (0..10) |i| {
        var id_bytes: [16]u8 = undefined;
        std.mem.writeInt(u128, &id_bytes, @as(u128, i), .big);
        const block_id = BlockId{ .bytes = id_bytes };

        const content = try std.fmt.allocPrint(allocator, "matching content {d}", .{i});
        defer allocator.free(content);

        const block = ContextBlock{
            .id = block_id,
            .version = 1,
            .source_uri = "paginated.zig",
            .metadata_json = "{}",
            .content = content,
        };
        try storage_engine.put_block(block);
    }

    // Test first page
    const condition = FilterExpression{ .condition = filter_by_content_contains("matching") };
    var query = FilteredQuery{
        .expression = condition,
        .max_results = 5,
        .offset = 0,
    };

    const first_page = try execute_filtered_query(allocator, &storage_engine, query);
    defer first_page.deinit();

    try testing.expectEqual(@as(u32, 10), first_page.total_matches);
    try testing.expectEqual(@as(usize, 5), first_page.blocks.len);
    try testing.expect(first_page.has_more);

    // Test second page
    query.offset = 5;
    const second_page = try execute_filtered_query(allocator, &storage_engine, query);
    defer second_page.deinit();

    try testing.expectEqual(@as(u32, 10), second_page.total_matches);
    try testing.expectEqual(@as(usize, 5), second_page.blocks.len);
    try testing.expect(!second_page.has_more);
}

test "metadata field extraction edge cases" {
    const allocator = testing.allocator;

    // Test malformed JSON
    const malformed_block = ContextBlock{
        .id = 1,
        .version = 1,
        .source_uri = "malformed.zig",
        .metadata_json = "{\"incomplete\":", // Invalid JSON
        .content = "content",
    };

    const condition = filter_by_metadata_field("incomplete", "value");
    try testing.expect(!try condition.matches(malformed_block, allocator));

    // Test empty metadata
    const empty_block = ContextBlock{
        .id = 2,
        .version = 1,
        .source_uri = "empty.zig",
        .metadata_json = "{}",
        .content = "content",
    };

    const empty_condition = filter_by_metadata_field("missing", "value");
    try testing.expect(!try empty_condition.matches(empty_block, allocator));

    // Test nested JSON extraction (simple case)
    const nested_block = ContextBlock{
        .id = 3,
        .version = 1,
        .source_uri = "nested.zig",
        .metadata_json = "{\"config\": {\"debug\": \"true\"}, \"level\": \"info\"}",
        .content = "content",
    };

    const level_condition = filter_by_metadata_field("level", "info");
    try testing.expect(try level_condition.matches(nested_block, allocator));
}

test "numeric comparison edge cases" {
    const allocator = testing.allocator;

    const block = ContextBlock{
        .id = 1,
        .version = 42,
        .source_uri = "numeric.zig",
        .metadata_json = "{}",
        .content = "test content",
    };

    // Test floating point comparison
    const float_condition = FilterCondition.init(.version, .equal, "42.0");
    try testing.expect(try float_condition.matches(block, allocator));

    // Test lexical vs numeric comparison
    const lexical_condition = FilterCondition.init(.source_uri, .greater_than, "a");
    try testing.expect(try lexical_condition.matches(block, allocator)); // "numeric.zig" > "a"

    // Test invalid numeric comparison falls back to lexical
    const mixed_condition = FilterCondition.init(.source_uri, .greater_than, "100");
    try testing.expect(try mixed_condition.matches(block, allocator)); // "numeric.zig" > "100" lexically
}
