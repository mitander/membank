//! Simplified Zig Parser Validation Test
//!
//! Validates core parser functionality without complex cross-file resolution.
//! Tests semantic extraction, error handling, and basic edge creation.

const std = @import("std");
const kausaldb = @import("kausaldb");
const testing = std.testing;

const ZigParser = kausaldb.zig_parser.ZigParser;
const ZigParserConfig = kausaldb.zig_parser.ZigParserConfig;
const SourceContent = kausaldb.pipeline.SourceContent;
const EdgeType = kausaldb.types.EdgeType;

test "parser handles realistic zig file structure" {
    const allocator = testing.allocator;

    const realistic_source =
        \\const std = @import("std");
        \\const testing = std.testing;
        \\
        \\/// Application configuration
        \\pub const Config = struct {
        \\    timeout: u64,
        \\    debug: bool,
        \\
        \\    pub fn init() Config {
        \\        return Config{
        \\            .timeout = 5000,
        \\            .debug = false,
        \\        };
        \\    }
        \\
        \\    pub fn validate(self: *const Config) bool {
        \\        return self.timeout > 0;
        \\    }
        \\};
        \\
        \\/// Initialize application with configuration
        \\pub fn initialize(config: Config) !void {
        \\    if (!config.validate()) {
        \\        return error.InvalidConfig;
        \\    }
        \\    setup_logging(config.debug);
        \\}
        \\
        \\fn setup_logging(debug: bool) void {
        \\    if (debug) {
        \\        std.debug.print("Debug logging enabled\n", .{});
        \\    }
        \\}
        \\
        \\test "config validation works" {
        \\    var config = Config.init();
        \\    try testing.expect(config.validate());
        \\
        \\    config.timeout = 0;
        \\    try testing.expect(!config.validate());
        \\}
        \\
        \\test "initialization handles invalid config" {
        \\    const invalid_config = Config{ .timeout = 0, .debug = false };
        \\    try testing.expectError(error.InvalidConfig, initialize(invalid_config));
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
    try metadata.put("path", try allocator.dupe(u8, "main.zig"));

    const content = SourceContent{
        .data = realistic_source,
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

    // Validate semantic extraction
    var found_imports: u32 = 0;
    var found_functions: u32 = 0;
    var found_types: u32 = 0;
    var found_tests: u32 = 0;
    var total_edges: u32 = 0;

    for (units) |unit| {
        if (std.mem.eql(u8, unit.unit_type, "import")) found_imports += 1;
        if (std.mem.eql(u8, unit.unit_type, "function")) found_functions += 1;
        if (std.mem.eql(u8, unit.unit_type, "type")) found_types += 1;
        if (std.mem.eql(u8, unit.unit_type, "test")) found_tests += 1;
        total_edges += @intCast(unit.edges.items.len);
    }

    // Validate expected extraction
    try testing.expect(found_imports >= 1); // std import
    try testing.expect(found_functions >= 3); // init, validate, initialize, setup_logging
    try testing.expect(found_types >= 1); // Config struct
    try testing.expect(found_tests >= 2); // Two test blocks

    // Should have extracted some relationships
    try testing.expect(total_edges >= 1);
}

test "parser supports content type detection" {
    const allocator = testing.allocator;

    const config = ZigParserConfig{};
    var parser = ZigParser.init(allocator, config);
    defer parser.deinit();

    const parser_interface = parser.parser();

    // Should support standard Zig content types
    try testing.expect(parser_interface.supports("text/zig"));
    try testing.expect(parser_interface.supports("text/x-zig"));

    // Should reject non-Zig types
    try testing.expect(!parser_interface.supports("text/javascript"));
    try testing.expect(!parser_interface.supports("application/json"));
    try testing.expect(!parser_interface.supports("text/plain"));
}

test "parser handles empty and minimal files" {
    const allocator = testing.allocator;

    const empty_source = "";
    const minimal_source = "const x = 42;";

    const config = ZigParserConfig{};
    var parser = ZigParser.init(allocator, config);
    defer parser.deinit();

    // Test empty file
    {
        var metadata = std.StringHashMap([]const u8).init(allocator);
        defer {
            var iter = metadata.iterator();
            while (iter.next()) |entry| {
                allocator.free(entry.value_ptr.*);
            }
            metadata.deinit();
        }
        try metadata.put("path", try allocator.dupe(u8, "empty.zig"));

        const content = SourceContent{
            .data = empty_source,
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

        // Empty file should produce no units
        try testing.expect(units.len == 0);
    }

    // Test minimal file
    {
        var metadata = std.StringHashMap([]const u8).init(allocator);
        defer {
            var iter = metadata.iterator();
            while (iter.next()) |entry| {
                allocator.free(entry.value_ptr.*);
            }
            metadata.deinit();
        }
        try metadata.put("path", try allocator.dupe(u8, "minimal.zig"));

        const content = SourceContent{
            .data = minimal_source,
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

        // Should extract the constant
        try testing.expect(units.len >= 1);

        var found_constant = false;
        for (units) |unit| {
            if (std.mem.eql(u8, unit.unit_type, "constant")) {
                found_constant = true;
            }
        }
        try testing.expect(found_constant);
    }
}

test "parser extracts metadata correctly" {
    const allocator = testing.allocator;

    const metadata_source =
        \\const std = @import("std");
        \\
        \\pub const PUBLIC_CONST = "visible";
        \\const PRIVATE_CONST = "hidden";
        \\
        \\pub fn public_function() void {}
        \\fn private_function() void {}
        \\
        \\pub const PublicStruct = struct {};
        \\const PrivateStruct = struct {};
    ;

    const config = ZigParserConfig{
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
        .data = metadata_source,
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

    // Validate visibility metadata is captured
    var public_items: u32 = 0;
    var private_items: u32 = 0;

    for (units) |unit| {
        const visibility = unit.metadata.get("visibility");
        if (visibility) |vis| {
            if (std.mem.eql(u8, vis, "public")) {
                public_items += 1;
            } else if (std.mem.eql(u8, vis, "private")) {
                private_items += 1;
            }
        }
    }

    try testing.expect(public_items >= 3); // PUBLIC_CONST, public_function, PublicStruct
    try testing.expect(private_items >= 2); // PRIVATE_CONST, private_function, PrivateStruct
}
