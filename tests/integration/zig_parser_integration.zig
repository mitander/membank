//! Zig Parser Integration Tests
//!
//! Tests core parser functionality through the Parser interface.
//! Validates semantic extraction, error resilience, and edge creation
//! without depending on implementation-specific methods.

const std = @import("std");
const kausaldb = @import("kausaldb");
const testing = std.testing;

const IngestionPipeline = kausaldb.pipeline.IngestionPipeline;
const Parser = kausaldb.pipeline.Parser;
const ParsedUnit = kausaldb.pipeline.ParsedUnit;
const SourceContent = kausaldb.pipeline.SourceContent;
const ZigParser = kausaldb.zig_parser.ZigParser;
const ZigParserConfig = kausaldb.zig_parser.ZigParserConfig;
const EdgeType = kausaldb.types.EdgeType;

test "parser extracts functions and creates call edges" {
    const allocator = testing.allocator;

    const zig_source =
        \\const std = @import("std");
        \\
        \\pub fn initialize() void {
        \\    setup_logging();
        \\    validate_config();
        \\}
        \\
        \\fn setup_logging() void {
        \\    std.debug.print("Logging initialized\n", .{});
        \\}
        \\
        \\fn validate_config() void {
        \\    setup_logging();
        \\}
    ;

    const config = ZigParserConfig{
        .include_function_bodies = true,
        .include_tests = false,
    };
    var parser = ZigParser.init(allocator, config);
    defer parser.deinit();

    var metadata = std.StringHashMap([]const u8).init(allocator);
    defer {
        var iter = metadata.iterator();
        while (iter.next()) |entry| {
            allocator.free(entry.value_ptr.*);
        }
        metadata.deinit();
    }
    try metadata.put("path", try allocator.dupe(u8, "test.zig"));

    const content = SourceContent{
        .data = zig_source,
        .content_type = "text/zig",
        .metadata = metadata,
        .timestamp_ns = 0,
    };

    const units = try parser.parser().parse(allocator, content);
    defer {
        for (units) |*unit| {
            unit.deinit(allocator);
        }
        allocator.free(units);
    }

    // Validate we extracted semantic units
    try testing.expect(units.len >= 3); // Should have at least 3 functions

    var found_functions: u32 = 0;
    var found_import: bool = false;
    var found_calls: u32 = 0;

    for (units) |unit| {
        if (std.mem.eql(u8, unit.unit_type, "function")) {
            found_functions += 1;
        } else if (std.mem.eql(u8, unit.unit_type, "import")) {
            found_import = true;
        }

        // Count call edges
        for (unit.edges.items) |edge| {
            if (edge.edge_type == .calls) {
                found_calls += 1;
            }
        }
    }

    try testing.expect(found_functions >= 3);
    try testing.expect(found_import);
    try testing.expect(found_calls >= 2); // initialize calls setup_logging and validate_config
}

test "parser handles malformed code gracefully" {
    const allocator = testing.allocator;

    // Intentionally broken Zig code
    const malformed_source =
        \\const std = @import("std");
        \\
        \\pub fn broken_syntax( {
        \\    // Missing closing paren and return type
        \\    var x =
        \\    // Missing value
        \\}
        \\
        \\// This function should still be parsed
        \\pub fn valid_function() void {
        \\    return;
        \\}
        \\
        \\struct BrokenStruct {
        \\    // Missing field type
        \\    field: ,
        \\}
    ;

    const config = ZigParserConfig{
        .include_function_bodies = true,
    };
    var parser = ZigParser.init(allocator, config);
    defer parser.deinit();

    var metadata = std.StringHashMap([]const u8).init(allocator);
    defer {
        var iter = metadata.iterator();
        while (iter.next()) |entry| {
            allocator.free(entry.value_ptr.*);
        }
        metadata.deinit();
    }
    try metadata.put("path", try allocator.dupe(u8, "broken.zig"));

    const content = SourceContent{
        .data = malformed_source,
        .content_type = "text/zig",
        .metadata = metadata,
        .timestamp_ns = 0,
    };

    // Parser should not crash and should extract what it can
    const units = parser.parser().parse(allocator, content) catch |err| switch (err) {
        error.ParseFailed => {
            // Acceptable failure mode
            return;
        },
        else => return err,
    };

    defer {
        for (units) |*unit| {
            unit.deinit(allocator);
        }
        allocator.free(units);
    }

    // Should extract at least the import and valid function
    try testing.expect(units.len >= 1);

    var found_valid_function = false;
    for (units) |unit| {
        if (std.mem.containsAtLeast(u8, unit.id, 1, "valid_function")) {
            found_valid_function = true;
        }
    }
    try testing.expect(found_valid_function);
}

test "parser extracts struct definitions and metadata" {
    const allocator = testing.allocator;

    const struct_source =
        \\const std = @import("std");
        \\
        \\pub const Config = struct {
        \\    timeout: u64,
        \\    enabled: bool,
        \\
        \\    pub fn init() Config {
        \\        return Config{
        \\            .timeout = 1000,
        \\            .enabled = true,
        \\        };
        \\    }
        \\
        \\    pub fn is_valid(self: *const Config) bool {
        \\        return self.enabled and self.timeout > 0;
        \\    }
        \\};
        \\
        \\const PrivateStruct = struct {
        \\    value: i32,
        \\};
    ;

    const config = ZigParserConfig{
        .include_function_bodies = true,
        .include_private = true,
    };
    var parser = ZigParser.init(allocator, config);
    defer parser.deinit();

    var metadata = std.StringHashMap([]const u8).init(allocator);
    defer {
        var iter = metadata.iterator();
        while (iter.next()) |entry| {
            allocator.free(entry.value_ptr.*);
        }
        metadata.deinit();
    }
    try metadata.put("path", try allocator.dupe(u8, "structs.zig"));

    const content = SourceContent{
        .data = struct_source,
        .content_type = "text/zig",
        .metadata = metadata,
        .timestamp_ns = 0,
    };

    std.debug.print("DEBUG HANG: About to call parser.parse() in 'extracts struct definitions'\n", .{});
    const units = try parser.parser().parse(allocator, content);
    std.debug.print("DEBUG HANG: parser.parse() completed in 'extracts struct definitions', got {} units\n", .{units.len});
    defer {
        for (units) |*unit| {
            unit.deinit(allocator);
        }
        allocator.free(units);
    }

    var found_public_struct = false;
    var found_private_struct = false;
    var found_methods: u32 = 0;

    for (units) |unit| {
        if (std.mem.eql(u8, unit.unit_type, "type")) {
            const visibility = unit.metadata.get("visibility") orelse "unknown";
            if (std.mem.containsAtLeast(u8, unit.id, 1, "Config")) {
                try testing.expectEqualStrings("public", visibility);
                found_public_struct = true;
            } else if (std.mem.containsAtLeast(u8, unit.id, 1, "PrivateStruct")) {
                found_private_struct = true;
            }
        } else if (std.mem.eql(u8, unit.unit_type, "function")) {
            const is_method = unit.metadata.get("is_method");
            if (is_method != null and std.mem.eql(u8, is_method.?, "true")) {
                found_methods += 1;
            }
        }
    }

    try testing.expect(found_public_struct);
    try testing.expect(found_private_struct);
    try testing.expect(found_methods >= 1); // is_valid method has self parameter
}

test "parser content type support" {
    const allocator = testing.allocator;

    const config = ZigParserConfig{};
    var parser = ZigParser.init(allocator, config);
    defer parser.deinit();

    const parser_interface = parser.parser();

    // Should support Zig content types
    try testing.expect(parser_interface.supports("text/zig"));
    try testing.expect(parser_interface.supports("text/x-zig"));
    try testing.expect(parser_interface.supports("application/x-zig"));

    // Should not support other types
    try testing.expect(!parser_interface.supports("text/javascript"));
    try testing.expect(!parser_interface.supports("text/plain"));
    try testing.expect(!parser_interface.supports("application/json"));

    // Description should be informative
    const description = parser_interface.describe();
    try testing.expect(std.mem.containsAtLeast(u8, description, 1, "Zig"));
}

test "parser preserves source location information" {
    const allocator = testing.allocator;

    const multi_line_source =
        \\const std = @import("std");
        \\
        \\/// First function
        \\pub fn first() void {
        \\    return;
        \\}
        \\
        \\/// Second function
        \\pub fn second() void {
        \\    first();
        \\    return;
        \\}
    ;

    const config = ZigParserConfig{
        .include_function_bodies = true,
    };
    var parser = ZigParser.init(allocator, config);
    defer parser.deinit();

    var metadata = std.StringHashMap([]const u8).init(allocator);
    defer {
        var iter = metadata.iterator();
        while (iter.next()) |entry| {
            allocator.free(entry.value_ptr.*);
        }
        metadata.deinit();
    }
    try metadata.put("path", try allocator.dupe(u8, "multiline.zig"));

    const content = SourceContent{
        .data = multi_line_source,
        .content_type = "text/zig",
        .metadata = metadata,
        .timestamp_ns = 0,
    };

    std.debug.print("DEBUG HANG: About to call parser.parse() in 'preserves source location'\n", .{});
    const units = try parser.parser().parse(allocator, content);
    std.debug.print("DEBUG HANG: parser.parse() completed in 'preserves source location', got {} units\n", .{units.len});
    defer {
        for (units) |*unit| {
            unit.deinit(allocator);
        }
        allocator.free(units);
    }

    // Verify source location tracking
    for (units) |unit| {
        try testing.expect(unit.location.line_start >= 1);
        try testing.expect(unit.location.line_end >= unit.location.line_start);
        try testing.expect(unit.location.col_start >= 1);
        try testing.expectEqualStrings("multiline.zig", unit.location.file_path);
    }
}

test "parser handles test declarations" {
    const allocator = testing.allocator;

    const test_source =
        \\const std = @import("std");
        \\const testing = std.testing;
        \\
        \\pub fn add(a: i32, b: i32) i32 {
        \\    return a + b;
        \\}
        \\
        \\test "add function works correctly" {
        \\    try testing.expect(add(2, 3) == 5);
        \\}
        \\
        \\test "add handles negatives" {
        \\    try testing.expect(add(-1, 1) == 0);
        \\}
    ;

    // Test with include_tests = true
    var config = ZigParserConfig{
        .include_function_bodies = true,
        .include_tests = true,
    };
    var parser = ZigParser.init(allocator, config);
    defer parser.deinit();

    var metadata = std.StringHashMap([]const u8).init(allocator);
    defer {
        var iter = metadata.iterator();
        while (iter.next()) |entry| {
            allocator.free(entry.value_ptr.*);
        }
        metadata.deinit();
    }
    try metadata.put("path", try allocator.dupe(u8, "with_tests.zig"));

    const content = SourceContent{
        .data = test_source,
        .content_type = "text/zig",
        .metadata = metadata,
        .timestamp_ns = 0,
    };

    const units = try parser.parser().parse(allocator, content);
    defer {
        for (units) |*unit| {
            unit.deinit(allocator);
        }
        allocator.free(units);
    }

    var found_tests: u32 = 0;
    var found_functions: u32 = 0;

    for (units) |unit| {
        if (std.mem.eql(u8, unit.unit_type, "test")) {
            found_tests += 1;
            const test_type = unit.metadata.get("test_type");
            try testing.expectEqualStrings("unit", test_type.?);
        } else if (std.mem.eql(u8, unit.unit_type, "function")) {
            found_functions += 1;
        }
    }

    try testing.expect(found_tests >= 2);
    try testing.expect(found_functions >= 1);

    // Test with include_tests = false
    config.include_tests = false;
    var parser_no_tests = ZigParser.init(allocator, config);
    defer parser_no_tests.deinit();

    const units_no_tests = try parser_no_tests.parser().parse(allocator, content);
    defer {
        for (units_no_tests) |*unit| {
            unit.deinit(allocator);
        }
        allocator.free(units_no_tests);
    }

    var found_tests_filtered: u32 = 0;
    for (units_no_tests) |unit| {
        if (std.mem.eql(u8, unit.unit_type, "test")) {
            found_tests_filtered += 1;
        }
    }

    try testing.expect(found_tests_filtered == 0);
}
