//! Filtering system for KausalDB query operations.
//!
//! Provides configurable filter conditions for block metadata, content,
//! and graph structure. Supports complex boolean logic and
//! execution against the storage layer. Includes secondary index support
//! for optimized metadata field queries.

const std = @import("std");

const context_block = @import("../core/types.zig");
const metadata_index = @import("../storage/metadata_index.zig");
const ownership = @import("../core/ownership.zig");
const simulation_vfs = @import("../sim/simulation_vfs.zig");
const storage = @import("../storage/engine.zig");
const assert_mod = @import("../core/assert.zig");
const storage_config_mod = @import("../storage/config.zig");

const assert = assert_mod.assert;
const testing = std.testing;

const BlockId = context_block.BlockId;
const BlockOwnership = ownership.BlockOwnership;
const Config = storage_config_mod.Config;
const ContextBlock = context_block.ContextBlock;
const OwnedQueryEngineBlock = ownership.OwnedQueryEngineBlock;
const SimulationVFS = simulation_vfs.SimulationVFS;
const StorageEngine = storage.StorageEngine;

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
        defer {
            // Free allocated buffers for version, content_length, and metadata_field
            switch (self.target) {
                .version, .content_length, .metadata_field => allocator.free(target_value),
                .content, .source_uri => {}, // These are borrowed, don't free
            }
        }

        return self.evaluate_comparison(target_value);
    }

    /// Extract the value from the block based on the filter target
    fn extract_target_value(self: FilterCondition, block: ContextBlock, allocator: std.mem.Allocator) ![]const u8 {
        return switch (self.target) {
            .content => block.content,
            .source_uri => block.source_uri,
            .version => blk: {
                var stack_buffer: [32]u8 = undefined;
                // Safety: u32 max value is 4,294,967,295 (10 digits), buffer is 32 bytes - guaranteed fit
                const written_slice = std.fmt.bufPrint(&stack_buffer, "{d}", .{block.version}) catch unreachable;
                break :blk try allocator.dupe(u8, written_slice);
            },
            .content_length => blk: {
                var stack_buffer: [16]u8 = undefined;
                // Safety: Content length fits in 16 bytes - max usize is at most 20 digits on 64-bit
                const written_slice = std.fmt.bufPrint(&stack_buffer, "{d}", .{block.content.len}) catch unreachable;
                break :blk try allocator.dupe(u8, written_slice);
            },
            .metadata_field => blk: {
                if (self.metadata_field) |field_name| {
                    break :blk try extract_metadata_field(allocator, block.metadata_json, field_name);
                }
                return FilterError.InvalidFilter;
            },
        };
    }

    /// Evaluate the comparison operation between target and filter values
    fn evaluate_comparison(self: FilterCondition, target_value: []const u8) bool {
        return switch (self.operator) {
            .equal => compare_numeric_or_lexical(target_value, self.value) == 0,
            .not_equal => compare_numeric_or_lexical(target_value, self.value) != 0,
            .greater_than => compare_numeric_or_lexical(target_value, self.value) > 0,
            .greater_equal => compare_numeric_or_lexical(target_value, self.value) >= 0,
            .less_than => compare_numeric_or_lexical(target_value, self.value) < 0,
            .less_equal => compare_numeric_or_lexical(target_value, self.value) <= 0,
            .contains => std.mem.indexOf(u8, target_value, self.value) != null,
            .starts_with => std.mem.startsWith(u8, target_value, self.value),
            .ends_with => std.mem.endsWith(u8, target_value, self.value),
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

/// Streaming iterator for filtered query results - prevents memory exhaustion on large result sets
pub const FilteredQueryIterator = struct {
    allocator: std.mem.Allocator,
    storage_iterator: StorageEngine.BlockIterator,
    query: FilteredQuery,
    total_scanned: u32,
    total_matches: u32,
    results_returned: u32,
    finished: bool,

    pub fn init(
        allocator: std.mem.Allocator,
        storage_engine: *StorageEngine,
        query: FilteredQuery,
    ) FilteredQueryIterator {
        return FilteredQueryIterator{
            .allocator = allocator,
            .storage_iterator = storage_engine.iterate_all_blocks(),
            .query = query,
            .total_scanned = 0,
            .total_matches = 0,
            .results_returned = 0,
            .finished = false,
        };
    }

    /// Get the next matching block, or null if no more results
    pub fn next(self: *FilteredQueryIterator) !?OwnedQueryEngineBlock {
        if (self.finished) return null;

        while (try self.storage_iterator.next()) |block| {
            self.total_scanned += 1;

            if (try self.query.expression.matches(block, self.allocator)) {
                self.total_matches += 1;

                // Pagination requires skipping initial matches to implement offset functionality
                if (self.total_matches <= self.query.offset) continue;

                // Prevent unbounded memory usage by enforcing result set size limits
                if (self.results_returned >= self.query.max_results) {
                    self.finished = true;
                    return null;
                }

                // Clone block to ensure iterator owns the memory
                const cloned_block = try clone_block(self.allocator, block);
                self.results_returned += 1;
                return OwnedQueryEngineBlock.init(cloned_block);
            }
        }

        self.finished = true;
        return null;
    }

    /// Calculate statistics about the filtering operation
    pub fn calculate_stats(self: FilteredQueryIterator) FilteringStats {
        return FilteringStats{
            .total_scanned = self.total_scanned,
            .total_matches = self.total_matches,
            .results_returned = self.results_returned,
            .finished = self.finished,
        };
    }

    pub fn deinit(self: *FilteredQueryIterator) void {
        // Storage iterator is owned by StorageEngine, no cleanup needed
        _ = self;
    }
};

/// Statistics for filtered query operations
pub const FilteringStats = struct {
    total_scanned: u32,
    total_matches: u32,
    results_returned: u32,
    finished: bool,

    pub fn has_more_results(self: FilteringStats) bool {
        return !self.finished or self.results_returned < self.total_matches;
    }
};

/// Legacy result set from a filtered query operation (deprecated - use FilteredQueryIterator)
pub const FilteredQueryResult = struct {
    blocks: []OwnedQueryEngineBlock,
    total_matches: u32,
    has_more: bool,
    allocator: std.mem.Allocator,

    pub fn init(
        allocator: std.mem.Allocator,
        blocks: []OwnedQueryEngineBlock,
        total_matches: u32,
        has_more: bool,
    ) FilteredQueryResult {
        return FilteredQueryResult{
            .allocator = allocator,
            .blocks = blocks,
            .total_matches = total_matches,
            .has_more = has_more,
        };
    }

    pub fn deinit(self: FilteredQueryResult) void {
        // FilteredQueryResult always owns block strings - free them unconditionally
        for (self.blocks) |block| {
            const ctx_block = block.read(.query_engine);
            self.allocator.free(ctx_block.source_uri);
            self.allocator.free(ctx_block.metadata_json);
            self.allocator.free(ctx_block.content);
        }
        self.allocator.free(self.blocks);
    }

    /// Format results for LLM consumption with metadata about filtering, streaming to writer
    pub fn format_for_llm(self: FilteredQueryResult, writer: std.io.AnyWriter) anyerror!void {
        try writer.print("Found {} matching blocks", .{self.total_matches});
        if (self.has_more) {
            try writer.print(" (showing first {})", .{self.blocks.len});
        }
        try writer.print(":\n\n", .{});

        for (self.blocks, 0..) |block, i| {
            const ctx_block = block.read(.query_engine);
            try writer.print("Block {} (ID: {}):\n", .{ i + 1, ctx_block.id });
            try writer.print("Source: {s}\n", .{ctx_block.source_uri});
            try writer.print("Version: {}\n", .{ctx_block.version});
            try writer.print("Content: {s}\n\n", .{ctx_block.content});
        }
    }
};

/// Execute a streaming filtered query against the storage engine (recommended)
/// Returns an iterator that yields results one by one, preventing memory exhaustion
pub fn execute_filtered_query_streaming(
    allocator: std.mem.Allocator,
    storage_engine: *StorageEngine,
    query: FilteredQuery,
) !FilteredQueryIterator {
    try query.validate();
    return FilteredQueryIterator.init(allocator, storage_engine, query);
}

/// Execute filtered query with optional secondary index optimization
/// Automatically detects simple metadata field queries and uses indexes when available
pub fn execute_filtered_query_optimized(
    allocator: std.mem.Allocator,
    storage_engine: *StorageEngine,
    query: FilteredQuery,
    metadata_indexes: ?*const metadata_index.MetadataIndexManager,
) !FilteredQueryResult {
    try query.validate();

    // Analyze query for index optimization opportunities
    if (metadata_indexes) |indexes| {
        if (try analyze_for_index_optimization(allocator, query.expression, indexes)) |optimized_block_ids| {
            defer allocator.free(optimized_block_ids);
            return execute_indexed_query(allocator, storage_engine, query, optimized_block_ids);
        }
    }

    // Fall back to full scan
    return execute_filtered_query(allocator, storage_engine, query);
}

/// Analyze filter expression to determine if it can use secondary indexes
/// Returns block IDs from index if optimization is possible, null otherwise
fn analyze_for_index_optimization(
    allocator: std.mem.Allocator,
    expression: FilterExpression,
    indexes: *const metadata_index.MetadataIndexManager,
) !?[]BlockId {
    switch (expression) {
        .condition => |condition| {
            // Only optimize simple equality conditions on metadata fields
            if (condition.target == .metadata_field and
                condition.operator == .equal and
                condition.metadata_field != null)
            {
                const field_name = condition.metadata_field.?;
                if (indexes.find_index(field_name)) |index| {
                    if (index.find_blocks_by_value(condition.value)) |block_ids| {
                        // Only use index if it's selective (reduces search space significantly)
                        const stats = index.statistics();
                        if (stats.is_selective()) {
                            return try allocator.dupe(BlockId, block_ids);
                        }
                    }
                }
            }
        },
        .logical => {
            // For now, only optimize simple conditions
            // Future enhancement: support AND/OR operations with multiple indexed fields
            return null;
        },
    }
    return null;
}

/// Execute query using pre-filtered block IDs from secondary index
fn execute_indexed_query(
    allocator: std.mem.Allocator,
    storage_engine: *StorageEngine,
    query: FilteredQuery,
    candidate_block_ids: []const BlockId,
) !FilteredQueryResult {
    var matched_blocks = std.array_list.Managed(OwnedQueryEngineBlock).init(allocator);
    errdefer {
        for (matched_blocks.items) |block| {
            const ctx_block = block.read(.query_engine);
            allocator.free(ctx_block.source_uri);
            allocator.free(ctx_block.metadata_json);
            allocator.free(ctx_block.content);
        }
        matched_blocks.deinit();
    }

    var total_matches: u32 = 0;
    var blocks_collected: u32 = 0;
    const max_to_collect = query.max_results;

    try matched_blocks.ensureTotalCapacity(@min(max_to_collect, candidate_block_ids.len));

    // Only check blocks that match the index
    for (candidate_block_ids) |block_id| {
        if (try storage_engine.find_block(block_id, .query_engine)) |found_block| {
            const ctx_block = found_block.read(.query_engine);

            // Apply the full filter expression to ensure correctness
            if (try query.expression.matches(ctx_block.*, allocator)) {
                total_matches += 1;

                if (total_matches <= query.offset) continue;

                if (blocks_collected < max_to_collect) {
                    // Clone block for ownership transfer
                    const cloned_ctx_block = try clone_block(allocator, ctx_block.*);
                    try matched_blocks.append(OwnedQueryEngineBlock.init(cloned_ctx_block));
                    blocks_collected += 1;
                }
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

/// Execute a filtered query against the storage engine (legacy - collects all results in memory)
/// WARNING: This can cause memory exhaustion on large result sets. Use execute_filtered_query_streaming instead.
pub fn execute_filtered_query(
    allocator: std.mem.Allocator,
    storage_engine: *StorageEngine,
    query: FilteredQuery,
) !FilteredQueryResult {
    try query.validate();

    var total_matches: u32 = 0;
    var blocks_collected: u32 = 0;
    const max_to_collect = query.max_results;

    var matched_blocks = std.array_list.Managed(OwnedQueryEngineBlock).init(allocator);
    errdefer {
        for (matched_blocks.items) |block| {
            const ctx_block = block.read(.query_engine);
            allocator.free(ctx_block.source_uri);
            allocator.free(ctx_block.metadata_json);
            allocator.free(ctx_block.content);
        }
        matched_blocks.deinit();
    }
    try matched_blocks.ensureTotalCapacity(max_to_collect);

    var iterator = storage_engine.iterate_all_blocks();

    while (try iterator.next()) |block| {
        const ctx_block = block;
        if (try query.expression.matches(ctx_block, allocator)) {
            total_matches += 1;

            if (total_matches <= query.offset) continue;

            if (blocks_collected < max_to_collect) {
                // Always clone blocks to unify ownership - FilteredQueryResult owns all strings
                const cloned_ctx_block = try clone_block(allocator, ctx_block);
                try matched_blocks.append(OwnedQueryEngineBlock.init(cloned_ctx_block));
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
fn extract_metadata_field(allocator: std.mem.Allocator, metadata_json: []const u8, field_name: []const u8) ![]const u8 {
    const field_pattern = try std.fmt.allocPrint(allocator, "\"{s}\":", .{field_name});
    defer allocator.free(field_pattern);

    const start_pos = std.mem.indexOf(u8, metadata_json, field_pattern) orelse return "";
    const value_start = start_pos + field_pattern.len;

    var pos = value_start;
    while (pos < metadata_json.len and (metadata_json[pos] == ' ' or metadata_json[pos] == '"')) {
        pos += 1;
    }

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
    const a_num = std.fmt.parseFloat(f64, a) catch null;
    const b_num = std.fmt.parseFloat(f64, b) catch null;

    if (a_num != null and b_num != null) {
        if (a_num.? < b_num.?) return -1;
        if (a_num.? > b_num.?) return 1;
        return 0;
    }

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

test "filter condition - content contains" {
    const allocator = testing.allocator;

    const block = ContextBlock{
        .id = BlockId.from_hex("11111111111111111111111111111111") catch unreachable, // Safety: hardcoded valid hex string
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
        // Safety: Hardcoded hex string is guaranteed to be valid 32-character hex
        .id = BlockId.from_hex("11111111111111111111111111111111") catch unreachable,
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
        // Safety: Hardcoded hex string is guaranteed to be valid 32-character hex
        .id = BlockId.from_hex("11111111111111111111111111111111") catch unreachable,
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

    var query = FilteredQuery.init(condition);
    try query.validate();

    query.max_results = 0;
    try testing.expectError(FilterError.EmptyQuery, query.validate());

    query.max_results = 20000;
    try testing.expectError(FilterError.ResultsLimitExceeded, query.validate());
}

test "filter operators - comparison operations" {
    const allocator = testing.allocator;

    const block_v1 = ContextBlock{
        .id = BlockId.from_hex("11111111111111111111111111111111") catch unreachable, // Safety: hardcoded valid hex string
        .version = 1,
        .source_uri = "test_v1.zig",
        .metadata_json = "{}",
        .content = "version 1 content",
    };

    const block_v2 = ContextBlock{
        .id = BlockId.from_hex("22222222222222222222222222222222") catch unreachable, // Safety: hardcoded valid hex string
        .version = 2,
        .source_uri = "test_v2.zig",
        .metadata_json = "{}",
        .content = "version 2 content",
    };

    const gt_condition = FilterCondition.init(.version, .greater_than, "1");
    try testing.expect(!try gt_condition.matches(block_v1, allocator));
    try testing.expect(try gt_condition.matches(block_v2, allocator));

    const lte_condition = FilterCondition.init(.version, .less_equal, "2");
    try testing.expect(try lte_condition.matches(block_v1, allocator));
    try testing.expect(try lte_condition.matches(block_v2, allocator));

    const ne_condition = FilterCondition.init(.version, .not_equal, "1");
    try testing.expect(!try ne_condition.matches(block_v1, allocator));
    try testing.expect(try ne_condition.matches(block_v2, allocator));
}

test "filter operators - string operations" {
    const allocator = testing.allocator;

    const block = ContextBlock{
        // Safety: Hardcoded hex string is guaranteed to be valid 32-character hex
        .id = BlockId.from_hex("11111111111111111111111111111111") catch unreachable,
        .version = 1,
        .source_uri = "hello_world.zig",
        .metadata_json = "{}",
        .content = "function hello_world() { return 'Hello, World!'; }",
    };

    const starts_condition = FilterCondition.init(.source_uri, .starts_with, "hello");
    try testing.expect(try starts_condition.matches(block, allocator));

    const no_start_condition = FilterCondition.init(.source_uri, .starts_with, "goodbye");
    try testing.expect(!try no_start_condition.matches(block, allocator));

    const ends_condition = FilterCondition.init(.source_uri, .ends_with, ".zig");
    try testing.expect(try ends_condition.matches(block, allocator));

    const no_end_condition = FilterCondition.init(.source_uri, .ends_with, ".rs");
    try testing.expect(!try no_end_condition.matches(block, allocator));

    const contains_condition = FilterCondition.init(.content, .contains, "Hello, World!");
    try testing.expect(try contains_condition.matches(block, allocator));

    const no_contains_condition = FilterCondition.init(.content, .contains, "Goodbye");
    try testing.expect(!try no_contains_condition.matches(block, allocator));
}

test "filter expression - logical OR operation" {
    const allocator = testing.allocator;

    const block = ContextBlock{
        .id = BlockId.from_hex("11111111111111111111111111111111") catch unreachable, // Safety: hardcoded valid hex string
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

    try testing.expect(try or_expr.matches(block, allocator));

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
        .id = BlockId.from_hex("11111111111111111111111111111111") catch unreachable, // Safety: hardcoded valid hex string
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

    try testing.expect(!try not_expr.matches(block, allocator));

    const false_condition = FilterExpression{ .condition = filter_by_content_contains("missing") };
    const not_false_expr = FilterExpression{
        .logical = .{
            .operator = .not_op,
            .left = @constCast(&false_condition),
            .right = null,
        },
    };

    try testing.expect(try not_false_expr.matches(block, allocator));
}

test "filter target - content length filtering" {
    const allocator = testing.allocator;

    const short_block = ContextBlock{
        // Safety: Hardcoded hex string is guaranteed to be valid 32-character hex
        .id = BlockId.from_hex("11111111111111111111111111111111") catch unreachable,
        .version = 1,
        .source_uri = "short.zig",
        .metadata_json = "{}",
        .content = "short", // 5 characters
    };

    const long_block = ContextBlock{
        .id = BlockId.from_hex("22222222222222222222222222222222") catch unreachable, // Safety: hardcoded valid hex string
        .version = 1,
        .source_uri = "long.zig",
        .metadata_json = "{}",
        .content = "this is much longer content", // 27 characters
    };

    const length_condition = FilterCondition.init(.content_length, .greater_than, "10");
    try testing.expect(!try length_condition.matches(short_block, allocator));
    try testing.expect(try length_condition.matches(long_block, allocator));

    const exact_length_condition = FilterCondition.init(.content_length, .equal, "5");
    try testing.expect(try exact_length_condition.matches(short_block, allocator));
    try testing.expect(!try exact_length_condition.matches(long_block, allocator));
}

test "complex nested filter expressions" {
    const allocator = testing.allocator;

    const block = ContextBlock{
        // Safety: Hardcoded hex string is guaranteed to be valid 32-character hex
        .id = BlockId.from_hex("11111111111111111111111111111111") catch unreachable,
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

    const test_ctx_blocks = [_]ContextBlock{
        .{
            .id = try BlockId.from_hex("11111111111111111111111111111111"),
            .version = 1,
            .source_uri = "result1.zig",
            .metadata_json = "{}",
            .content = "first result",
        },
        .{
            .id = try BlockId.from_hex("22222222222222222222222222222222"),
            .version = 1,
            .source_uri = "result2.zig",
            .metadata_json = "{}",
            .content = "second result",
        },
    };

    var test_blocks = try allocator.alloc(OwnedQueryEngineBlock, test_ctx_blocks.len);
    defer allocator.free(test_blocks);
    for (test_ctx_blocks, 0..) |ctx_block, i| {
        test_blocks[i] = OwnedQueryEngineBlock.init(ctx_block);
    }

    var owned_blocks = try allocator.alloc(OwnedQueryEngineBlock, test_blocks.len);
    for (test_blocks, 0..) |test_block, i| {
        const cloned_block = try clone_block(allocator, test_block.read(.query_engine).*);
        owned_blocks[i] = OwnedQueryEngineBlock.init(cloned_block);
    }

    var result = FilteredQueryResult.init(allocator, owned_blocks, 5, true);
    defer result.deinit();

    try testing.expectEqual(@as(u32, 5), result.total_matches);
    try testing.expectEqual(@as(usize, 2), result.blocks.len);
    try testing.expect(result.has_more);

    var formatted_output = std.array_list.Managed(u8).init(allocator);
    defer formatted_output.deinit();

    try result.format_for_llm(formatted_output.writer().any());
    const formatted = formatted_output.items;

    try testing.expect(std.mem.indexOf(u8, formatted, "Found 5 matching blocks") != null);
    try testing.expect(std.mem.indexOf(u8, formatted, "showing first 2") != null);
    try testing.expect(std.mem.indexOf(u8, formatted, "result1.zig") != null);
    try testing.expect(std.mem.indexOf(u8, formatted, "result2.zig") != null);
}

test "execute filtered query with storage engine" {
    const allocator = testing.allocator;

    var sim_vfs = try SimulationVFS.init(allocator);
    defer sim_vfs.deinit();

    var storage_engine = try StorageEngine.init(allocator, sim_vfs.vfs(), "./test_filtering", Config{});
    defer storage_engine.deinit();
    try storage_engine.startup();

    const blocks = [_]ContextBlock{
        .{
            .id = try BlockId.from_hex("11111111111111111111111111111111"),
            .version = 1,
            .source_uri = "match1.zig",
            .metadata_json = "{\"type\": \"function\"}",
            .content = "function test_match() {}", // Contains "test"
        },
        .{
            .id = try BlockId.from_hex("22222222222222222222222222222222"),
            .version = 1,
            .source_uri = "match2.zig",
            .metadata_json = "{\"type\": \"variable\"}",
            .content = "var test_variable = 42;", // Contains "test"
        },
        .{
            .id = try BlockId.from_hex("33333333333333333333333333333333"),
            .version = 1,
            .source_uri = "nomatch.zig",
            .metadata_json = "{\"type\": \"struct\"}",
            .content = "struct Example {}", // No "test"
        },
    };

    for (blocks) |block| {
        try storage_engine.put_block(block);
    }

    const condition = FilterExpression{ .condition = filter_by_content_contains("test") };
    const query = FilteredQuery.init(condition);

    const result = try execute_filtered_query(allocator, &storage_engine, query);
    defer result.deinit();

    try testing.expectEqual(@as(u32, 2), result.total_matches);
    try testing.expectEqual(@as(usize, 2), result.blocks.len);
    try testing.expect(!result.has_more);

    var found_function = false;
    var found_variable = false;
    var found_struct = false;

    for (result.blocks) |block| {
        const ctx_block = block.read(.query_engine);
        if (std.mem.indexOf(u8, ctx_block.content, "function") != null) found_function = true;
        if (std.mem.indexOf(u8, ctx_block.content, "var") != null) found_variable = true;
        if (std.mem.indexOf(u8, ctx_block.content, "struct") != null) found_struct = true;
    }

    try testing.expect(found_function and found_variable and !found_struct);
}

test "filtered query with pagination" {
    const allocator = testing.allocator;

    var sim_vfs = try SimulationVFS.init(allocator);
    defer sim_vfs.deinit();

    var storage_engine = try StorageEngine.init(allocator, sim_vfs.vfs(), "./test_pagination", Config{});
    defer storage_engine.deinit();
    try storage_engine.startup();

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

    query.offset = 5;
    const second_page = try execute_filtered_query(allocator, &storage_engine, query);
    defer second_page.deinit();

    try testing.expectEqual(@as(u32, 10), second_page.total_matches);
    try testing.expectEqual(@as(usize, 5), second_page.blocks.len);
    try testing.expect(!second_page.has_more);
}

test "metadata field extraction edge cases" {
    const allocator = testing.allocator;

    const malformed_block = ContextBlock{
        .id = BlockId.from_hex("11111111111111111111111111111111") catch unreachable, // Safety: hardcoded valid hex string
        .version = 1,
        .source_uri = "malformed.zig",
        .metadata_json = "{\"incomplete\":", // Invalid JSON
        .content = "content",
    };

    const condition = filter_by_metadata_field("incomplete", "value");
    try testing.expect(!try condition.matches(malformed_block, allocator));

    const empty_block = ContextBlock{
        .id = BlockId.from_hex("22222222222222222222222222222222") catch unreachable, // Safety: hardcoded valid hex string
        .version = 1,
        .source_uri = "empty.zig",
        .metadata_json = "{}",
        .content = "content",
    };

    const empty_condition = filter_by_metadata_field("missing", "value");
    try testing.expect(!try empty_condition.matches(empty_block, allocator));

    const nested_block = ContextBlock{
        .id = BlockId.from_hex("33333333333333333333333333333333") catch unreachable, // Safety: hardcoded valid hex string
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
        // Safety: Hardcoded hex string is guaranteed to be valid 32-character hex
        .id = BlockId.from_hex("11111111111111111111111111111111") catch unreachable,
        .version = 42,
        .source_uri = "numeric.zig",
        .metadata_json = "{}",
        .content = "test content",
    };

    const float_condition = FilterCondition.init(.version, .equal, "42.0");
    try testing.expect(try float_condition.matches(block, allocator));

    const lexical_condition = FilterCondition.init(.source_uri, .greater_than, "a");
    try testing.expect(try lexical_condition.matches(block, allocator)); // "numeric.zig" > "a"

    const mixed_condition = FilterCondition.init(.source_uri, .greater_than, "100");
    try testing.expect(try mixed_condition.matches(block, allocator)); // "numeric.zig" > "100" lexically
}

test "streaming filtered query prevents memory exhaustion" {
    const allocator = testing.allocator;

    var sim_vfs = try SimulationVFS.init(allocator);
    defer sim_vfs.deinit();

    var storage_engine = try StorageEngine.init(allocator, sim_vfs.vfs(), "./test_streaming", Config{});
    defer storage_engine.deinit();
    try storage_engine.startup();

    for (0..1000) |i| {
        var id_bytes: [16]u8 = undefined;
        std.mem.writeInt(u128, &id_bytes, @as(u128, i), .big);
        const block_id = BlockId{ .bytes = id_bytes };

        const content = try std.fmt.allocPrint(allocator, "streaming test content {d}", .{i});
        defer allocator.free(content);

        const block = ContextBlock{
            .id = block_id,
            .version = 1,
            .source_uri = "streaming.zig",
            .metadata_json = "{}",
            .content = content,
        };
        try storage_engine.put_block(block);
    }

    const condition = FilterExpression{ .condition = filter_by_content_contains("streaming") };
    const query = FilteredQuery{
        .expression = condition,
        .max_results = 10, // Small limit to test streaming behavior
        .offset = 0,
    };

    var iterator = try execute_filtered_query_streaming(allocator, &storage_engine, query);
    defer iterator.deinit();

    var results_count: u32 = 0;
    while (try iterator.next()) |block| {
        results_count += 1;
        // Clean up the block memory immediately - simulates low-memory processing
        const ctx_block = block.read(.query_engine);
        allocator.free(ctx_block.source_uri);
        allocator.free(ctx_block.metadata_json);
        allocator.free(ctx_block.content);
    }

    try testing.expectEqual(@as(u32, 10), results_count);

    const stats = iterator.calculate_stats();
    try testing.expect(stats.total_matches >= 10); // Should have found at least 10 matching blocks
    try testing.expectEqual(@as(u32, 10), stats.results_returned);
    try testing.expect(stats.finished);

    try testing.expect(stats.results_returned == 10); // Only processed the limited results, not all matches
}
