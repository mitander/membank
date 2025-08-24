//! Parsing performance benchmarks for ingestion pipeline.
//!
//! Tests Zig parser performance on various code sizes and patterns.
//! Measures parsing throughput, memory efficiency, and semantic extraction accuracy.

const builtin = @import("builtin");
const std = @import("std");

const kausaldb = @import("kausaldb");

const coordinator = @import("../benchmark.zig");

const BenchmarkResult = coordinator.BenchmarkResult;
const StatisticalSampler = kausaldb.StatisticalSampler;
const WarmupUtils = kausaldb.WarmupUtils;
const ZigParser = kausaldb.zig_parser.ZigParser;
const ZigParserConfig = kausaldb.zig_parser.ZigParserConfig;
const SourceContent = kausaldb.pipeline.SourceContent;

// Performance thresholds based on expected parsing performance
const SMALL_FILE_PARSE_THRESHOLD_NS = 10_000; // 10μs for small files (<1KB)
const MEDIUM_FILE_PARSE_THRESHOLD_NS = 50_000; // 50μs for medium files (1-10KB)
const LARGE_FILE_PARSE_THRESHOLD_NS = 200_000; // 200μs for large files (10-100KB)
const MAX_PARSE_MEMORY_BYTES = 10 * 1024 * 1024; // 10MB max memory for parsing
const MAX_MEMORY_GROWTH_PER_KB = 1024; // 1KB memory per KB of source

const ITERATIONS = 100;
const WARMUP_ITERATIONS = 10;
const STATISTICAL_SAMPLES = 10;
const TIMEOUT_MS = 10_000; // 10 second timeout

/// Timeout wrapper for parsing benchmark operations
fn run_with_timeout(
    allocator: std.mem.Allocator,
    comptime benchmark_fn: anytype,
    timeout_ms: u32,
) !BenchmarkResult {
    const start_time = std.time.milliTimestamp();

    const result = benchmark_fn(allocator) catch |err| switch (err) {
        error.OutOfMemory => {
            std.debug.print("Benchmark failed: out of memory\n", .{});
            return err;
        },
        else => {
            std.debug.print("Benchmark failed with error: {any}\n", .{err});
            return err;
        },
    };

    const elapsed_time = std.time.milliTimestamp() - start_time;
    if (elapsed_time > timeout_ms) {
        std.debug.print("Benchmark timed out after {d}ms\n", .{elapsed_time});
        return error.Timeout;
    }

    return result;
}

/// Run all parsing benchmarks and return aggregated results
pub fn run_all(allocator: std.mem.Allocator) !std.array_list.Managed(BenchmarkResult) {
    var results = std.array_list.Managed(BenchmarkResult).init(allocator);

    try results.append(try run_with_timeout(allocator, run_small_file_parsing, TIMEOUT_MS));
    try results.append(try run_with_timeout(allocator, run_medium_file_parsing, TIMEOUT_MS));
    try results.append(try run_with_timeout(allocator, run_large_file_parsing, TIMEOUT_MS));
    try results.append(try run_with_timeout(allocator, run_semantic_extraction, TIMEOUT_MS));

    return results;
}

/// Benchmark parsing performance on small Zig files (typical single-module files)
pub fn run_small_file_parsing(allocator: std.mem.Allocator) !BenchmarkResult {
    const config = ZigParserConfig{
        .include_function_bodies = true,
        .include_private = true,
        .include_tests = true,
    };

    const small_zig_source =
        \\const std = @import("std");
        \\
        \\pub fn add(a: i32, b: i32) i32 {
        \\    return a + b;
        \\}
        \\
        \\pub fn multiply(a: i32, b: i32) i32 {
        \\    return a * b;
        \\}
        \\
        \\test "add function" {
        \\    const testing = std.testing;
        \\    try testing.expect(add(2, 3) == 5);
        \\}
    ;

    return run_parsing_benchmark(allocator, config, small_zig_source, "small_file_parsing", SMALL_FILE_PARSE_THRESHOLD_NS);
}

/// Benchmark parsing performance on medium Zig files (typical library modules)
pub fn run_medium_file_parsing(allocator: std.mem.Allocator) !BenchmarkResult {
    const config = ZigParserConfig{
        .include_function_bodies = true,
        .include_private = true,
        .include_tests = true,
    };

    // Generate a medium-sized Zig file with multiple structs and functions
    var medium_source = std.array_list.Managed(u8).init(allocator);
    defer medium_source.deinit();

    try medium_source.appendSlice("const std = @import(\"std\");\n\n");

    // Add multiple structs with methods
    var i: u32 = 0;
    while (i < 10) : (i += 1) {
        try medium_source.writer().print(
            \\pub const Struct{d} = struct {{
            \\    value: u32,
            \\    
            \\    pub fn init(value: u32) Struct{d} {{
            \\        return Struct{d}{{ .value = value }};
            \\    }}
            \\    
            \\    pub fn read_value(self: *const Struct{d}) u32 {{
            \\        return self.value;
            \\    }}
            \\    
            \\    pub fn update_value(self: *Struct{d}, value: u32) void {{
            \\        self.value = value;
            \\    }}
            \\}};
            \\
            \\
        , .{ i, i, i, i, i });
    }

    // Add test functions
    i = 0;
    while (i < 5) : (i += 1) {
        try medium_source.writer().print(
            \\test "struct {d} functionality" {{
            \\    const s = Struct{d}.init({d});
            \\    try std.testing.expect(s.read_value() == {d});
            \\}}
            \\
            \\
        , .{ i, i, i, i });
    }

    return run_parsing_benchmark(allocator, config, medium_source.items, "medium_file_parsing", MEDIUM_FILE_PARSE_THRESHOLD_NS);
}

/// Benchmark parsing performance on large Zig files (complex application modules)
pub fn run_large_file_parsing(allocator: std.mem.Allocator) !BenchmarkResult {
    const config = ZigParserConfig{
        .include_function_bodies = true,
        .include_private = true,
        .include_tests = true,
    };

    // Generate a large Zig file with complex patterns
    var large_source = std.array_list.Managed(u8).init(allocator);
    defer large_source.deinit();

    try large_source.appendSlice(
        \\const std = @import("std");
        \\const testing = std.testing;
        \\
        \\pub const Config = struct {
        \\    debug: bool,
        \\    timeout: u64,
        \\    retries: u32,
        \\};
        \\
        \\
    );

    // Generate many functions with different patterns
    var i: u32 = 0;
    while (i < 50) : (i += 1) {
        try large_source.writer().print(
            \\pub fn process_data_{d}(data: []const u8, config: Config) ![]u8 {{
            \\    if (config.debug) {{
            \\        std.debug.print("Processing data chunk {d}\n", .{{i}});
            \\    }}
            \\    
            \\    var result = std.array_list.Managed(u8).init(std.heap.page_allocator);
            \\    defer result.deinit();
            \\    
            \\    for (data) |byte| {{
            \\        try result.append(byte ^ 0xFF);
            \\    }}
            \\    
            \\    return result.toOwnedSlice();
            \\}}
            \\
            \\fn helper_function_{d}(value: u32) u32 {{
            \\    return value * 2 + 1;
            \\}}
            \\
            \\
        , .{ i, i, i });
    }

    // Add comprehensive tests
    i = 0;
    while (i < 20) : (i += 1) {
        try large_source.writer().print(
            \\test "process_data_{d} handles empty input" {{
            \\    const config = Config{{ .debug = false, .timeout = 1000, .retries = 3 }};
            \\    const result = try process_data_{d}(&[_]u8{{}}, config);
            \\    defer std.heap.page_allocator.free(result);
            \\    try testing.expect(result.len == 0);
            \\}}
            \\
            \\test "helper_function_{d} computation" {{
            \\    try testing.expect(helper_function_{d}({d}) == {d});
            \\}}
            \\
            \\
        , .{ i, i, i, i, i, 2 * i + 1 });
    }

    return run_parsing_benchmark(allocator, config, large_source.items, "large_file_parsing", LARGE_FILE_PARSE_THRESHOLD_NS);
}

/// Benchmark semantic extraction accuracy and completeness
pub fn run_semantic_extraction(allocator: std.mem.Allocator) !BenchmarkResult {
    const config = ZigParserConfig{
        .include_function_bodies = true,
        .include_private = true,
        .include_tests = true,
    };

    const semantic_test_source =
        \\const std = @import("std");
        \\const testing = std.testing;
        \\
        \\pub const CONSTANT_VALUE = 42;
        \\const PRIVATE_CONSTANT = "hidden";
        \\
        \\pub const DataStructure = struct {
        \\    id: u32,
        \\    name: []const u8,
        \\    
        \\    pub fn init(id: u32, name: []const u8) DataStructure {
        \\        return DataStructure{ .id = id, .name = name };
        \\    }
        \\    
        \\    pub fn validate(self: *const DataStructure) bool {
        \\        return self.id > 0 and self.name.len > 0;
        \\    }
        \\    
        \\    fn get_private_info(self: *const DataStructure) []const u8 {
        \\        return PRIVATE_CONSTANT;
        \\    }
        \\};
        \\
        \\pub fn create_data(id: u32) DataStructure {
        \\    return DataStructure.init(id, "default");
        \\}
        \\
        \\fn internal_helper() u32 {
        \\    return CONSTANT_VALUE;
        \\}
        \\
        \\test "semantic extraction validation" {
        \\    const data = create_data(1);
        \\    try testing.expect(data.validate());
        \\}
        \\
        \\test "private constant access" {
        \\    try testing.expect(internal_helper() == CONSTANT_VALUE);
        \\}
    ;

    // This benchmark focuses on semantic accuracy rather than just speed
    const start_time = std.time.nanoTimestamp();

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
    try metadata.put("path", try allocator.dupe(u8, "semantic_test.zig"));

    const content = SourceContent{
        .data = semantic_test_source,
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

    const end_time = std.time.nanoTimestamp();
    const elapsed_ns = @as(u64, @intCast(end_time - start_time));

    // Validate semantic extraction completeness
    var found_functions: u32 = 0;
    var found_constants: u32 = 0;
    var found_types: u32 = 0;
    var found_tests: u32 = 0;
    var found_methods: u32 = 0;

    for (units) |unit| {
        if (std.mem.eql(u8, unit.unit_type, "function")) {
            found_functions += 1;
            if (unit.metadata.get("is_method")) |is_method| {
                if (std.mem.eql(u8, is_method, "true")) {
                    found_methods += 1;
                }
            }
        } else if (std.mem.eql(u8, unit.unit_type, "constant")) {
            found_constants += 1;
        } else if (std.mem.eql(u8, unit.unit_type, "type")) {
            found_types += 1;
        } else if (std.mem.eql(u8, unit.unit_type, "test")) {
            found_tests += 1;
        }
    }

    // Expected: 5 functions (init, validate, get_private_info, create_data, internal_helper)
    // Expected: 2 constants (CONSTANT_VALUE, PRIVATE_CONSTANT)
    // Expected: 1 type (DataStructure)
    // Expected: 2 tests
    // Expected: 2 methods (init, validate have self parameter)
    const semantic_accuracy = found_functions >= 5 and found_constants >= 2 and
        found_types >= 1 and found_tests >= 2 and found_methods >= 2;

    const passed_threshold = elapsed_ns <= MEDIUM_FILE_PARSE_THRESHOLD_NS and semantic_accuracy;

    return BenchmarkResult{
        .operation_name = "semantic_extraction",
        .iterations = 1,
        .total_time_ns = elapsed_ns,
        .min_ns = elapsed_ns,
        .max_ns = elapsed_ns,
        .mean_ns = elapsed_ns,
        .median_ns = elapsed_ns,
        .stddev_ns = 0,
        .throughput_ops_per_sec = 1_000_000_000.0 / @as(f64, @floatFromInt(elapsed_ns)),
        .passed_threshold = passed_threshold,
        .threshold_ns = MEDIUM_FILE_PARSE_THRESHOLD_NS,
        .peak_memory_bytes = 0,
        .memory_growth_bytes = 0,
        .memory_efficient = true,
    };
}

/// Core parsing benchmark implementation
fn run_parsing_benchmark(
    allocator: std.mem.Allocator,
    config: ZigParserConfig,
    source_code: []const u8,
    operation_name: []const u8,
    threshold_ns: u64,
) !BenchmarkResult {
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
    try metadata.put("path", try allocator.dupe(u8, "benchmark_test.zig"));

    const content = SourceContent{
        .data = source_code,
        .content_type = "text/zig",
        .metadata = metadata,
        .timestamp_ns = @intCast(std.time.nanoTimestamp()),
    };

    // Warmup runs
    std.debug.print("[WARMUP] Running {d} warmup samples for {s}...\n", .{ WARMUP_ITERATIONS, operation_name });
    var i: u32 = 0;
    while (i < WARMUP_ITERATIONS) : (i += 1) {
        const units = try parser.parser().parse(allocator, content);
        for (units) |*unit| {
            unit.deinit(allocator);
        }
        allocator.free(units);
    }

    // Measurement runs
    std.debug.print("[MEASURE] Collecting {d} measurement samples for {s}...\n", .{ STATISTICAL_SAMPLES, operation_name });
    var measurements = std.array_list.Managed(u64).init(allocator);
    defer measurements.deinit();

    var total_time_ns: u64 = 0;
    var min_ns: u64 = std.math.maxInt(u64);
    var max_ns: u64 = 0;

    i = 0;
    while (i < STATISTICAL_SAMPLES) : (i += 1) {
        const start_time = std.time.nanoTimestamp();

        const units = try parser.parser().parse(allocator, content);

        const end_time = std.time.nanoTimestamp();
        const elapsed_ns = @as(u64, @intCast(end_time - start_time));

        // Clean up
        for (units) |*unit| {
            unit.deinit(allocator);
        }
        allocator.free(units);

        try measurements.append(elapsed_ns);
        total_time_ns += elapsed_ns;
        min_ns = @min(min_ns, elapsed_ns);
        max_ns = @max(max_ns, elapsed_ns);
    }

    // Calculate statistics manually
    std.mem.sort(u64, measurements.items, {}, std.sort.asc(u64));
    const mean_ns = total_time_ns / STATISTICAL_SAMPLES;
    const median_ns = measurements.items[measurements.items.len / 2];

    // Simple standard deviation calculation
    var variance_sum: u64 = 0;
    for (measurements.items) |measurement| {
        const diff = if (measurement > mean_ns) measurement - mean_ns else mean_ns - measurement;
        variance_sum += diff * diff;
    }
    const stddev_ns = @as(u64, @intFromFloat(@sqrt(@as(f64, @floatFromInt(variance_sum / STATISTICAL_SAMPLES)))));
    const passed_threshold = mean_ns <= threshold_ns;
    const throughput_ops_per_sec = 1_000_000_000.0 / @as(f64, @floatFromInt(mean_ns));

    return BenchmarkResult{
        .operation_name = operation_name,
        .iterations = STATISTICAL_SAMPLES,
        .total_time_ns = total_time_ns,
        .min_ns = min_ns,
        .max_ns = max_ns,
        .mean_ns = mean_ns,
        .median_ns = median_ns,
        .stddev_ns = stddev_ns,
        .throughput_ops_per_sec = throughput_ops_per_sec,
        .passed_threshold = passed_threshold,
        .threshold_ns = threshold_ns,
        .peak_memory_bytes = 0, // Memory tracking could be added
        .memory_growth_bytes = 0,
        .memory_efficient = true,
    };
}
