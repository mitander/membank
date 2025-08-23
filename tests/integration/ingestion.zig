//! Integration Tests for Ingestion Pipeline
//!
//! Tests the core ingestion functionality with focus on parser integration
//! and basic pipeline functionality. Avoids complex cross-file resolution
//! that depends on implementation details.

const std = @import("std");
const kausaldb = @import("kausaldb");
const testing = std.testing;

const ZigParser = kausaldb.zig_parser.ZigParser;
const ZigParserConfig = kausaldb.zig_parser.ZigParserConfig;
const SourceContent = kausaldb.pipeline.SourceContent;
const EdgeType = kausaldb.types.EdgeType;

test "parser integration through pipeline interface" {
    const allocator = testing.allocator;

    const test_source =
        \\const std = @import("std");
        \\const testing = std.testing;
        \\
        \\pub const VERSION = "1.0.0";
        \\
        \\pub fn initialize() void {
        \\    setup_logging();
        \\}
        \\
        \\fn setup_logging() void {
        \\    std.debug.print("Logging setup\n", .{});
        \\}
        \\
        \\pub const Config = struct {
        \\    timeout: u64,
        \\
        \\    pub fn init() Config {
        \\        return Config{ .timeout = 5000 };
        \\    }
        \\
        \\    pub fn validate(self: *const Config) bool {
        \\        return self.timeout > 0;
        \\    }
        \\};
        \\
        \\test "basic functionality" {
        \\    const config = Config.init();
        \\    try testing.expect(config.validate());
        \\}
    ;

    const config = ZigParserConfig{
        .include_function_bodies = true,
        .include_tests = true,
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
    try metadata.put("path", try allocator.dupe(u8, "integration_test.zig"));

    const content = SourceContent{
        .data = test_source,
        .content_type = "text/zig",
        .metadata = metadata,
        .timestamp_ns = @intCast(std.time.nanoTimestamp()),
    };

    // Test parser through Pipeline interface
    const parser_interface = parser.parser();

    // Verify content type support
    try testing.expect(parser_interface.supports("text/zig"));
    try testing.expect(!parser_interface.supports("text/javascript"));

    // Test parsing
    const units = try parser_interface.parse(allocator, content);
    defer {
        for (units) |*unit| {
            unit.deinit(allocator);
        }
        allocator.free(units);
    }

    // Validate extraction results
    try testing.expect(units.len >= 4); // Should extract functions, constants, types, tests

    var found_functions: u32 = 0;
    var found_constants: u32 = 0;
    var found_types: u32 = 0;
    var found_tests: u32 = 0;
    var found_imports: u32 = 0;

    for (units) |unit| {
        if (std.mem.eql(u8, unit.unit_type, "function")) found_functions += 1;
        if (std.mem.eql(u8, unit.unit_type, "constant")) found_constants += 1;
        if (std.mem.eql(u8, unit.unit_type, "type")) found_types += 1;
        if (std.mem.eql(u8, unit.unit_type, "test")) found_tests += 1;
        if (std.mem.eql(u8, unit.unit_type, "import")) found_imports += 1;

        // Validate each unit has required fields
        try testing.expect(unit.id.len > 0);
        try testing.expect(unit.unit_type.len > 0);
        try testing.expectEqualStrings("integration_test.zig", unit.location.file_path);
        try testing.expect(unit.location.line_start >= 1);
        try testing.expect(unit.location.line_end >= unit.location.line_start);
    }

    // Validate expected semantic extraction
    try testing.expect(found_functions >= 3); // initialize, setup_logging, init, validate
    try testing.expect(found_constants >= 1); // VERSION
    try testing.expect(found_types >= 1); // Config struct
    try testing.expect(found_tests >= 1); // test block
    try testing.expect(found_imports >= 1); // std import
}

test "parser error handling and resilience" {
    const allocator = testing.allocator;

    const malformed_source =
        \\const std = @import("std");
        \\
        \\pub fn broken_syntax( {
        \\    // Missing closing paren, return type
        \\    var x =
        \\    // Missing assignment value
        \\}
        \\
        \\
        // This should still be parseable
        \\pub fn working_function() void {
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
    try metadata.put("path", try allocator.dupe(u8, "malformed.zig"));

    const content = SourceContent{
        .data = malformed_source,
        .content_type = "text/zig",
        .metadata = metadata,
        .timestamp_ns = @intCast(std.time.nanoTimestamp()),
    };

    // Parser should handle malformed input gracefully
    const units = parser.parser().parse(allocator, content) catch |err| {
        // Acceptable failure modes
        switch (err) {
            error.ParseFailed => return, // Parser rejected malformed input
            else => return err, // Unexpected error
        }
    };

    defer {
        for (units) |*unit| {
            unit.deinit(allocator);
        }
        allocator.free(units);
    }

    // Should extract what it can
    try testing.expect(units.len >= 1);

    // Should find the working function and import
    var found_import = false;
    var found_working_function = false;

    for (units) |unit| {
        if (std.mem.eql(u8, unit.unit_type, "import")) {
            found_import = true;
        }
        if (std.mem.containsAtLeast(u8, unit.id, 1, "working_function")) {
            found_working_function = true;
        }
    }

    try testing.expect(found_import);
    try testing.expect(found_working_function);
}

test "parser metadata extraction and visibility" {
    const allocator = testing.allocator;

    const visibility_source =
        \\const std = @import("std");
        \\
        \\pub const PUBLIC_CONST = 42;
        \\const PRIVATE_CONST = 24;
        \\
        \\pub fn public_function() void {}
        \\fn private_function() void {}
        \\
        \\pub const PublicStruct = struct {
        \\    pub fn method(self: *PublicStruct) void {
        \\        _ = self;
        \\    }
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
    try metadata.put("path", try allocator.dupe(u8, "visibility.zig"));

    const content = SourceContent{
        .data = visibility_source,
        .content_type = "text/zig",
        .metadata = metadata,
        .timestamp_ns = @intCast(std.time.nanoTimestamp()),
    };

    const units = try parser.parser().parse(allocator, content);
    defer {
        for (units) |*unit| {
            unit.deinit(allocator);
        }
        allocator.free(units);
    }

    var public_items: u32 = 0;
    var private_items: u32 = 0;
    var methods: u32 = 0;

    for (units) |unit| {
        const visibility = unit.metadata.get("visibility");
        if (visibility) |vis| {
            if (std.mem.eql(u8, vis, "public")) {
                public_items += 1;
            } else if (std.mem.eql(u8, vis, "private")) {
                private_items += 1;
            }
        }

        const is_method = unit.metadata.get("is_method");
        if (is_method != null and std.mem.eql(u8, is_method.?, "true")) {
            methods += 1;
        }
    }

    // Should detect visibility correctly
    try testing.expect(public_items >= 3); // PUBLIC_CONST, public_function, PublicStruct
    try testing.expect(private_items >= 2); // PRIVATE_CONST, private_function
    try testing.expect(methods >= 1); // method function with self parameter
}

test "parser configuration options work correctly" {
    const allocator = testing.allocator;

    const source_with_tests =
        \\const std = @import("std");
        \\
        \\pub fn main_function() void {}
        \\fn private_function() void {}
        \\
        \\test "sample test" {
        \\    // Test content
        \\}
    ;

    var metadata = std.StringHashMap([]const u8).init(allocator);
    defer {
        var iter = metadata.iterator();
        while (iter.next()) |entry| {
            allocator.free(entry.value_ptr.*);
        }
        metadata.deinit();
    }
    try metadata.put("path", try allocator.dupe(u8, "config_test.zig"));

    const content = SourceContent{
        .data = source_with_tests,
        .content_type = "text/zig",
        .metadata = metadata,
        .timestamp_ns = @intCast(std.time.nanoTimestamp()),
    };

    // Test with tests enabled
    {
        const config_with_tests = ZigParserConfig{
            .include_tests = true,
            .include_private = true,
        };

        var parser = ZigParser.init(allocator, config_with_tests);
        defer parser.deinit();

        const units = try parser.parser().parse(allocator, content);
        defer {
            for (units) |*unit| {
                unit.deinit(allocator);
            }
            allocator.free(units);
        }

        var found_tests: u32 = 0;
        for (units) |unit| {
            if (std.mem.eql(u8, unit.unit_type, "test")) {
                found_tests += 1;
            }
        }

        try testing.expect(found_tests >= 1);
    }

    // Test with tests disabled
    {
        const config_no_tests = ZigParserConfig{
            .include_tests = false,
            .include_private = true,
        };

        var parser = ZigParser.init(allocator, config_no_tests);
        defer parser.deinit();

        const units = try parser.parser().parse(allocator, content);
        defer {
            for (units) |*unit| {
                unit.deinit(allocator);
            }
            allocator.free(units);
        }

        var found_tests: u32 = 0;
        for (units) |unit| {
            if (std.mem.eql(u8, unit.unit_type, "test")) {
                found_tests += 1;
            }
        }

        try testing.expect(found_tests == 0);
    }
}

test "parser handles edge cases gracefully" {
    const allocator = testing.allocator;

    const edge_cases = [_]struct { source: []const u8, description: []const u8 }{
        .{ .source = "", .description = "empty file" },
        .{ .source = "   \n  \n  ", .description = "whitespace only" },
        .{ .source = "// Just a comment\n", .description = "comment only" },
        .{ .source = "const x = 42;", .description = "single constant" },
    };

    const config = ZigParserConfig{};
    var parser = ZigParser.init(allocator, config);
    defer parser.deinit();

    for (edge_cases) |test_case| {
        var metadata = std.StringHashMap([]const u8).init(allocator);
        defer {
            var iter = metadata.iterator();
            while (iter.next()) |entry| {
                allocator.free(entry.value_ptr.*);
            }
            metadata.deinit();
        }

        const filename = try std.fmt.allocPrint(allocator, "{s}.zig", .{test_case.description});
        defer allocator.free(filename);

        try metadata.put("path", try allocator.dupe(u8, filename));

        const content = SourceContent{
            .data = test_case.source,
            .content_type = "text/zig",
            .metadata = metadata,
            .timestamp_ns = @intCast(std.time.nanoTimestamp()),
        };

        // Parser should not crash on any input
        const units = parser
            .parser().parse(allocator, content) catch |err| {
            // Acceptable failures for edge cases
            switch (err) {
                error.ParseFailed => continue,
                else => return err,
            }
        };

        defer {
            for (units) |*unit| {
                unit.deinit(allocator);
            }
            allocator.free(units);
        }

        // Validate any extracted units are well-formed
        for (units) |unit| {
            try testing.expect(unit.id.len > 0);
            try testing.expect(unit.unit_type.len > 0);
            try testing.expect(unit.location.line_start >= 1);
            try testing.expect(unit.location.line_end >= unit.location.line_start);
        }
    }
}
