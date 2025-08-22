//! Cross-file call resolution test for ZigParser
//!
//! Creates multiple files with complex back-and-forth call patterns to thoroughly
//! test cross-file resolution capabilities. Tests both function calls and method calls
//! across different file boundaries with proper metadata tracking.

const std = @import("std");
const kausaldb = @import("kausaldb");

// Test files simulating a realistic multi-file Zig project
const TestFile = struct {
    path: []const u8,
    content: []const u8,
};

const test_files = [_]TestFile{
    // File 1: Core utilities with logging and math functions
    .{
        .path = "utils/core.zig",
        .content =
        \\const std = @import("std");
        \\const logger = @import("logger.zig");
        \\const math_ops = @import("../math/operations.zig");
        \\
        \\pub fn initialize_system() void {
        \\    logger.log_info("System initializing");
        \\    const result = math_ops.add(10, 20);
        \\    logger.log_debug("Init result: {}", .{result});
        \\}
        \\
        \\pub fn shutdown_system() void {
        \\    logger.log_info("System shutting down");
        \\    const cleanup_code = math_ops.subtract(100, 1);
        \\    logger.log_debug("Cleanup code: {}", .{cleanup_code});
        \\}
        \\
        \\pub const SystemManager = struct {
        \\    status: bool,
        \\
        \\    pub fn init() SystemManager {
        \\        return SystemManager{ .status = false };
        \\    }
        \\
        \\    pub fn start(self: *SystemManager) void {
        \\        self.status = true;
        \\        initialize_system();
        \\    }
        \\
        \\    pub fn stop(self: *SystemManager) void {
        \\        self.status = false;
        \\        shutdown_system();
        \\    }
        \\
        \\    pub fn check_status(self: *const SystemManager) bool {
        \\        logger.log_debug("Checking status");
        \\        return self.status;
        \\    }
        \\};
        ,
    },

    // File 2: Logger module that calls back to math operations
    .{
        .path = "utils/logger.zig",
        .content =
        \\const std = @import("std");
        \\const math_ops = @import("../math/operations.zig");
        \\const core = @import("core.zig");
        \\
        \\pub fn log_info(message: []const u8) void {
        \\    const timestamp = math_ops.current_timestamp();
        \\    std.debug.print("[INFO {}] {s}\n", .{ timestamp, message });
        \\}
        \\
        \\pub fn log_debug(comptime fmt: []const u8, args: anytype) void {
        \\    const level = math_ops.multiply(2, 3); // Debug level 6
        \\    std.debug.print("[DEBUG {}] ", .{level});
        \\    std.debug.print(fmt, args);
        \\    std.debug.print("\n", .{});
        \\}
        \\
        \\pub fn log_error(message: []const u8) void {
        \\    const error_code = math_ops.add(500, 1);
        \\    std.debug.print("[ERROR {}] {s}\n", .{ error_code, message });
        \\    // Call back to core for system health check
        \\    var manager = core.SystemManager.init();
        \\    const is_healthy = manager.check_status();
        \\    if (!is_healthy) {
        \\        std.debug.print("System not healthy!\n", .{});
        \\    }
        \\}
        \\
        \\pub const Logger = struct {
        \\    level: u8,
        \\    prefix: []const u8,
        \\
        \\    pub fn init(prefix: []const u8) Logger {
        \\        return Logger{ 
        \\            .level = @intCast(math_ops.add(1, 2)), // Level 3
        \\            .prefix = prefix 
        \\        };
        \\    }
        \\
        \\    pub fn info(self: *const Logger, message: []const u8) void {
        \\        if (self.level >= 3) {
        \\            log_info(message);
        \\        }
        \\    }
        \\
        \\    pub fn debug(self: *const Logger, comptime fmt: []const u8, args: anytype) void {
        \\        if (self.level >= 4) {
        \\            log_debug(fmt, args);
        \\        }
        \\    }
        \\};
        ,
    },

    // File 3: Math operations that occasionally call logger
    .{
        .path = "math/operations.zig",
        .content =
        \\const std = @import("std");
        \\
        \\pub fn add(a: i32, b: i32) i32 {
        \\    return a + b;
        \\}
        \\
        \\pub fn subtract(a: i32, b: i32) i32 {
        \\    return a - b;
        \\}
        \\
        \\pub fn multiply(a: i32, b: i32) i32 {
        \\    const result = a * b;
        \\    // Note: would call logger but avoiding circular import
        \\    return result;
        \\}
        \\
        \\pub fn divide(a: i32, b: i32) i32 {
        \\    if (b == 0) {
        \\        // Would normally call logger.log_error but avoiding circular import
        \\        return 0;
        \\    }
        \\    return @divTrunc(a, b);
        \\}
        \\
        \\pub fn current_timestamp() i64 {
        \\    return std.time.timestamp();
        \\}
        \\
        \\pub const Calculator = struct {
        \\    precision: u8,
        \\
        \\    pub fn init(precision: u8) Calculator {
        \\        return Calculator{ .precision = precision };
        \\    }
        \\
        \\    pub fn compute(self: *const Calculator, a: i32, b: i32) f64 {
        \\        const sum = add(a, b);
        \\        const product = multiply(sum, 2);
        \\        return @floatFromInt(product) / @as(f64, @floatFromInt(self.precision));
        \\    }
        \\
        \\    pub fn advanced_compute(self: *const Calculator, values: []const i32) f64 {
        \\        var sum: i32 = 0;
        \\        for (values) |value| {
        \\            sum = add(sum, value);
        \\        }
        \\        const multiplied = multiply(sum, @intCast(values.len));
        \\        return @floatFromInt(divide(multiplied, @intCast(self.precision)));
        \\    }
        \\};
        ,
    },

    // File 4: Main application that uses everything
    .{
        .path = "main.zig",
        .content =
        \\const std = @import("std");
        \\const core = @import("utils/core.zig");
        \\const logger = @import("utils/logger.zig");
        \\const math_ops = @import("math/operations.zig");
        \\
        \\pub fn main() void {
        \\    // Initialize system
        \\    core.initialize_system();
        \\    
        \\    // Create logger
        \\    var app_logger = logger.Logger.init("APP");
        \\    app_logger.info("Application starting");
        \\    
        \\    // Create system manager
        \\    var manager = core.SystemManager.init();
        \\    manager.start();
        \\    
        \\    // Do some math
        \\    const result1 = math_ops.add(10, 20);
        \\    const result2 = math_ops.multiply(result1, 2);
        \\    app_logger.debug("Math result: {}", .{result2});
        \\    
        \\    // Use calculator
        \\    var calc = math_ops.Calculator.init(4);
        \\    const computed = calc.compute(100, 200);
        \\    app_logger.debug("Computed: {d}", .{computed});
        \\    
        \\    // Advanced computation
        \\    const values = [_]i32{ 1, 2, 3, 4, 5 };
        \\    const advanced = calc.advanced_compute(&values);
        \\    app_logger.info("Advanced result");
        \\    
        \\    // Check system status
        \\    if (manager.check_status()) {
        \\        logger.log_info("System running properly");
        \\    } else {
        \\        logger.log_error("System failure detected");
        \\    }
        \\    
        \\    // Shutdown
        \\    manager.stop();
        \\    core.shutdown_system();
        \\    app_logger.info("Application finished");
        \\}
        \\
        \\pub fn run_tests() void {
        \\    var test_logger = logger.Logger.init("TEST");
        \\    test_logger.info("Running tests");
        \\    
        \\    // Test math operations
        \\    const add_result = math_ops.add(5, 3);
        \\    const sub_result = math_ops.subtract(10, 4);
        \\    const mul_result = math_ops.multiply(add_result, sub_result);
        \\    
        \\    test_logger.debug("Test results: {} {} {}", .{ add_result, sub_result, mul_result });
        \\    
        \\    // Test calculator
        \\    var test_calc = math_ops.Calculator.init(2);
        \\    const calc_result = test_calc.compute(mul_result, 10);
        \\    
        \\    if (calc_result > 0) {
        \\        test_logger.info("All tests passed");
        \\    } else {
        \\        logger.log_error("Tests failed");
        \\    }
        \\}
        ,
    },

    // File 5: Configuration module that ties everything together
    .{
        .path = "config.zig",
        .content =
        \\const std = @import("std");
        \\const core = @import("utils/core.zig");
        \\const logger = @import("utils/logger.zig");
        \\const math_ops = @import("math/operations.zig");
        \\const main_app = @import("main.zig");
        \\
        \\pub const AppConfig = struct {
        \\    log_level: u8,
        \\    math_precision: u8,
        \\    system_timeout: i32,
        \\    
        \\    pub fn default() AppConfig {
        \\        return AppConfig{
        \\            .log_level = @intCast(math_ops.add(2, 2)),
        \\            .math_precision = @intCast(math_ops.subtract(10, 6)),
        \\            .system_timeout = math_ops.multiply(30, 1000),
        \\        };
        \\    }
        \\    
        \\    pub fn validate(self: *const AppConfig) bool {
        \\        var config_logger = logger.Logger.init("CONFIG");
        \\        config_logger.debug("Validating config", .{});
        \\        
        \\        if (self.log_level == 0) {
        \\            logger.log_error("Invalid log level");
        \\            return false;
        \\        }
        \\        
        \\        if (self.math_precision == 0) {
        \\            logger.log_error("Invalid math precision");
        \\            return false;
        \\        }
        \\        
        \\        const timeout_check = math_ops.divide(self.system_timeout, 1000);
        \\        if (timeout_check <= 0) {
        \\            logger.log_error("Invalid timeout");
        \\            return false;
        \\        }
        \\        
        \\        config_logger.info("Config validation passed");
        \\        return true;
        \\    }
        \\    
        \\    pub fn run_with_config(self: *const AppConfig) void {
        \\        if (!self.validate()) {
        \\            logger.log_error("Configuration invalid, cannot run");
        \\            return;
        \\        }
        \\        
        \\        var system_manager = core.SystemManager.init();
        \\        system_manager.start();
        \\        
        \\        // Run main application
        \\        main_app.main();
        \\        
        \\        // Run tests
        \\        main_app.run_tests();
        \\        
        \\        system_manager.stop();
        \\        logger.log_info("Application run completed");
        \\    }
        \\};
        \\
        \\pub fn create_optimized_config() AppConfig {
        \\    var config = AppConfig.default();
        \\    
        \\    // Optimize based on system capabilities
        \\    const base_precision = math_ops.add(4, 4);
        \\    config.math_precision = @intCast(base_precision);
        \\    
        \\    const optimal_timeout = math_ops.multiply(60, 1000);
        \\    config.system_timeout = optimal_timeout;
        \\    
        \\    var optimizer_logger = logger.Logger.init("OPTIMIZER");
        \\    optimizer_logger.info("Created optimized configuration");
        \\    
        \\    return config;
        \\}
        ,
    },
};

test "cross-file call resolution with multi-file project simulation" {
    const testing = std.testing;

    // Convert test files to SourceContent
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const arena_allocator = arena.allocator();

    var source_files = try arena_allocator.alloc(kausaldb.SourceContent, test_files.len);

    for (test_files, 0..) |test_file, i| {
        var metadata = std.StringHashMap([]const u8).init(arena_allocator);
        try metadata.put("file_path", test_file.path);

        source_files[i] = kausaldb.SourceContent{
            .data = test_file.content,
            .content_type = "text/zig",
            .metadata = metadata,
            .timestamp_ns = 0,
        };
    }

    // Parse with cross-file resolution
    var parser = kausaldb.ZigParser.init(testing.allocator, .{
        .include_function_bodies = true,
        .include_private = false,
        .include_tests = false,
    });
    defer parser.deinit();

    var batch_result = parser.parse_batch(testing.allocator, source_files) catch |err| {
        std.debug.print("Parse batch failed: {}\n", .{err});
        return err;
    };
    defer batch_result.deinit(testing.allocator);

    std.debug.print("Successfully parsed {} units from {} files\n", .{ batch_result.units.len, test_files.len });

    // Analyze cross-file relationships
    var cross_file_edges: u32 = 0;
    var function_calls: u32 = 0;
    var method_calls: u32 = 0;
    var files_with_edges: u32 = 0;

    var file_edge_map = std.StringHashMap(u32).init(testing.allocator);
    defer file_edge_map.deinit();

    for (batch_result.units) |unit| {
        if (unit.edges.items.len > 0) {
            // Count edges per file
            const file_path = unit.location.file_path;
            const current_count = file_edge_map.get(file_path) orelse 0;
            try file_edge_map.put(file_path, current_count + @as(u32, @intCast(unit.edges.items.len)));

            for (unit.edges.items) |edge| {
                if (edge.metadata.get("target_file")) |target_file| {
                    if (!std.mem.eql(u8, target_file, unit.location.file_path)) {
                        cross_file_edges += 1;

                        switch (edge.edge_type) {
                            .calls_function => function_calls += 1,
                            .calls_method => method_calls += 1,
                            else => {},
                        }
                    }
                }
            }
        }
    }

    // Count files that have outgoing edges
    var file_iter = file_edge_map.iterator();
    while (file_iter.next()) |entry| {
        if (entry.value_ptr.* > 0) {
            files_with_edges += 1;
        }
    }

    // Verify comprehensive cross-file resolution
    std.debug.print("Cross-file analysis results:\n", .{});
    std.debug.print("  Total cross-file edges: {}\n", .{cross_file_edges});
    std.debug.print("  Function calls: {}\n", .{function_calls});
    std.debug.print("  Method calls: {}\n", .{method_calls});
    std.debug.print("  Files with outgoing edges: {}\n", .{files_with_edges});

    // Expectations for comprehensive test
    try testing.expect(cross_file_edges >= 10); // Should have many cross-file calls
    try testing.expect(function_calls >= 5); // Should detect function calls
    try testing.expect(method_calls >= 3); // Should detect method calls
    try testing.expect(files_with_edges >= 3); // Multiple files should have edges

    // Verify specific call patterns exist
    var found_main_to_core = false;
    var found_logger_to_math = false;
    var found_config_to_main = false;

    for (batch_result.units) |unit| {
        if (std.mem.indexOf(u8, unit.location.file_path, "main.zig") != null and
            std.mem.indexOf(u8, unit.id, "main") != null)
        {
            // Check if main function calls core functions
            for (unit.edges.items) |edge| {
                if (edge.metadata.get("target_file")) |target_file| {
                    if (std.mem.indexOf(u8, target_file, "core.zig") != null) {
                        found_main_to_core = true;
                    }
                }
            }
        }

        if (std.mem.indexOf(u8, unit.location.file_path, "logger.zig") != null) {
            for (unit.edges.items) |edge| {
                if (edge.metadata.get("target_file")) |target_file| {
                    if (std.mem.indexOf(u8, target_file, "operations.zig") != null) {
                        found_logger_to_math = true;
                    }
                }
            }
        }

        if (std.mem.indexOf(u8, unit.location.file_path, "config.zig") != null) {
            for (unit.edges.items) |edge| {
                if (edge.metadata.get("target_file")) |target_file| {
                    if (std.mem.indexOf(u8, target_file, "main.zig") != null) {
                        found_config_to_main = true;
                    }
                }
            }
        }
    }

    std.debug.print("Specific call pattern verification:\n", .{});
    std.debug.print("  main.zig -> core.zig: {}\n", .{found_main_to_core});
    std.debug.print("  logger.zig -> operations.zig: {}\n", .{found_logger_to_math});
    std.debug.print("  config.zig -> main.zig: {}\n", .{found_config_to_main});

    // Verify specific patterns were detected
    try testing.expect(found_main_to_core);
    try testing.expect(found_logger_to_math);
    try testing.expect(found_config_to_main);

    std.debug.print("Cross-file call resolution test PASSED\n", .{});
}
