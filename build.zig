const std = @import("std");

const TestCategory = enum {
    unit,
    integration,
    ingestion,
    simulation,
    stress,
    performance,
    fault_injection,
    recovery,
    safety,
    defensive,

    fn description(self: TestCategory) []const u8 {
        return switch (self) {
            .unit => "unit tests from src/ modules",
            .integration => "integration and server tests",
            .ingestion => "ingestion pipeline and backpressure tests",
            .simulation => "deterministic simulation tests",
            .stress => "stress and memory pressure tests",
            .performance => "performance benchmarks and validations",
            .fault_injection => "fault injection and error handling tests",
            .recovery => "WAL recovery and corruption tests",
            .safety => "memory safety and ownership tests",
            .defensive => "defensive programming validation tests",
        };
    }
};

const TestGroup = struct {
    category: TestCategory,
    patterns: []const []const u8,
};

const test_groups = [_]TestGroup{
    .{ .category = .integration, .patterns = &.{ "integration_", "server_", "cli_" } },
    .{ .category = .ingestion, .patterns = &.{"ingestion_"} },
    .{ .category = .simulation, .patterns = &.{"simulation_"} },
    .{ .category = .stress, .patterns = &.{"stress_"} },
    .{ .category = .performance, .patterns = &.{"performance_"} },
    .{ .category = .fault_injection, .patterns = &.{"fault_injection_"} },
    .{ .category = .recovery, .patterns = &.{"recovery_"} },
    .{ .category = .safety, .patterns = &.{"safety_"} },
    .{ .category = .defensive, .patterns = &.{"defensive_"} },
};

fn categorize_test(test_name: []const u8) TestCategory {
    for (test_groups) |group| {
        for (group.patterns) |pattern| {
            if (std.mem.startsWith(u8, test_name, pattern)) {
                return group.category;
            }
        }
    }
    return .integration;
}

fn component_needs_libc(name: []const u8) bool {
    const libc_components = [_][]const u8{
        "unit",
        "server_lifecycle",
        "integration_lifecycle",
        "integration_server_coordinator",
        "performance",
        "benchmark",
    };

    for (libc_components) |component| {
        if (std.mem.eql(u8, name, component)) return true;
    }
    return false;
}

const BuildModules = struct {
    kausaldb: *std.Build.Module,
    kausaldb_test: *std.Build.Module,
    build_options: *std.Build.Module,
    enable_thread_sanitizer: bool,
    enable_ubsan: bool,
    debug_tests: bool,
};

fn create_build_modules(
    b: *std.Build,
    target: std.Build.ResolvedTarget,
    optimize: std.builtin.OptimizeMode,
) BuildModules {
    const enable_statistics = b.option(bool, "enable-stats", "Enable runtime statistics collection") orelse false;
    const enable_detailed_logging = b.option(bool, "enable-detailed-logs", "Enable detailed debug logging") orelse false;
    const enable_fault_injection = b.option(bool, "enable-fault-injection", "Enable fault injection testing") orelse false;
    const enable_thread_sanitizer = b.option(bool, "enable-thread-sanitizer", "Enable Thread Sanitizer") orelse false;
    const enable_ubsan = b.option(bool, "enable-ubsan", "Enable Undefined Behavior Sanitizer") orelse false;
    const debug_tests = b.option(bool, "debug", "Enable debug test output (verbose logging and demo content)") orelse false;

    const sanitizers_active = enable_thread_sanitizer or enable_ubsan;

    const build_options = b.addOptions();
    build_options.addOption(bool, "enable_statistics", enable_statistics);
    build_options.addOption(bool, "enable_detailed_logging", enable_detailed_logging);
    build_options.addOption(bool, "enable_fault_injection", enable_fault_injection);
    build_options.addOption(bool, "enable_thread_sanitizer", enable_thread_sanitizer);
    build_options.addOption(bool, "enable_ubsan", enable_ubsan);
    build_options.addOption(bool, "sanitizers_active", sanitizers_active);
    build_options.addOption(bool, "is_debug_build", optimize == .Debug);
    build_options.addOption(bool, "debug_tests", debug_tests);

    const kausaldb_module = b.createModule(.{
        .root_source_file = b.path("src/kausaldb.zig"),
        .target = target,
        .optimize = optimize,
    });

    // Apply sanitizer flags to main module
    if (enable_thread_sanitizer) {
        kausaldb_module.sanitize_thread = true;
    }
    if (enable_ubsan) {
        kausaldb_module.sanitize_c = .full;
    }

    kausaldb_module.addImport("build_options", build_options.createModule());

    const kausaldb_test_module = b.createModule(.{
        .root_source_file = b.path("src/testing_api.zig"),
        .target = target,
        .optimize = optimize,
    });

    // Apply sanitizer flags to test module
    if (enable_thread_sanitizer) {
        kausaldb_test_module.sanitize_thread = true;
    }
    if (enable_ubsan) {
        kausaldb_test_module.sanitize_c = .full;
    }

    kausaldb_test_module.addImport("build_options", build_options.createModule());

    return BuildModules{
        .kausaldb = kausaldb_module,
        .kausaldb_test = kausaldb_test_module,
        .build_options = build_options.createModule(),
        .enable_thread_sanitizer = enable_thread_sanitizer,
        .enable_ubsan = enable_ubsan,
        .debug_tests = debug_tests,
    };
}

const TestFile = struct {
    name: []const u8,
    path: []const u8,
    category: TestCategory,
};

fn discover_test_files(allocator: std.mem.Allocator) !std.ArrayList(TestFile) {
    var discovered_tests = std.ArrayList(TestFile).init(allocator);

    var tests_dir = std.fs.cwd().openDir("tests", .{ .iterate = true }) catch |err| switch (err) {
        error.FileNotFound => return discovered_tests,
        else => return err,
    };
    defer tests_dir.close();

    var walker = try tests_dir.walk(allocator);
    defer walker.deinit();

    while (try walker.next()) |entry| {
        if (entry.kind != .file or !std.mem.endsWith(u8, entry.path, ".zig")) continue;
        if (std.mem.endsWith(u8, entry.path, "_helper.zig") or
            std.mem.endsWith(u8, entry.path, "helper.zig")) continue;

        const step_name = try generate_step_name(allocator, entry.path);
        const full_path = try std.fmt.allocPrint(allocator, "tests/{s}", .{entry.path});
        const category = categorize_test(step_name);

        try discovered_tests.append(.{
            .name = step_name,
            .path = full_path,
            .category = category,
        });
    }

    return discovered_tests;
}

fn generate_step_name(allocator: std.mem.Allocator, path: []const u8) ![]const u8 {
    var name_buf: [256]u8 = undefined;
    var name_len: usize = 0;

    var path_parts = std.mem.splitSequence(u8, path, "/");
    var is_first = true;
    while (path_parts.next()) |part| {
        if (std.mem.endsWith(u8, part, ".zig")) {
            const basename = part[0 .. part.len - 4];
            if (!is_first) {
                name_buf[name_len] = '_';
                name_len += 1;
            }
            @memcpy(name_buf[name_len .. name_len + basename.len], basename);
            name_len += basename.len;
        } else {
            if (!is_first) {
                name_buf[name_len] = '_';
                name_len += 1;
            }
            @memcpy(name_buf[name_len .. name_len + part.len], part);
            name_len += part.len;
        }
        is_first = false;
    }

    return try allocator.dupe(u8, name_buf[0..name_len]);
}

fn create_test_executable(
    b: *std.Build,
    test_file: TestFile,
    modules: BuildModules,
    target: std.Build.ResolvedTarget,
    optimize: std.builtin.OptimizeMode,
) *std.Build.Step.Compile {
    // Performance tests should run in release mode for accurate measurements
    const test_optimize = if (test_file.category == .performance) .ReleaseFast else optimize;

    const test_exe = b.addTest(.{
        .root_module = b.createModule(.{
            .root_source_file = b.path(test_file.path),
            .target = target,
            .optimize = test_optimize,
        }),
    });

    if (component_needs_libc(test_file.name)) {
        test_exe.linkLibC();
    }

    // For performance tests, create release-mode kausaldb module
    const kausaldb_module = if (test_file.category == .performance) blk: {
        const perf_kausaldb_module = b.createModule(.{
            .root_source_file = b.path("src/testing_api.zig"),
            .target = target,
            .optimize = .ReleaseFast,
        });
        perf_kausaldb_module.addImport("build_options", modules.build_options);
        break :blk perf_kausaldb_module;
    } else modules.kausaldb_test;

    // Apply sanitizer flags if enabled (skip for performance tests to avoid overhead)
    if (test_file.category != .performance) {
        if (modules.enable_thread_sanitizer) {
            test_exe.root_module.sanitize_thread = true;
        }
        if (modules.enable_ubsan) {
            test_exe.root_module.sanitize_c = .full;
        }
    }

    test_exe.root_module.addImport("kausaldb", kausaldb_module);
    test_exe.root_module.addImport("build_options", modules.build_options);

    // Create test-specific build options with log level
    const test_options = b.addOptions();
    test_options.addOption(bool, "debug_tests", modules.debug_tests);

    // Set appropriate log level for test category unless --debug flag is used
    const log_level: std.log.Level = if (modules.debug_tests) .debug else switch (test_file.category) {
        .performance, .stress => .err, // Performance tests: errors only
        .fault_injection, .defensive => .err, // Fault injection: expected failures are noise
        .integration, .simulation => .warn, // Integration: warnings and errors
        .unit, .ingestion, .recovery, .safety => .warn, // Standard: warnings and errors
    };
    test_options.addOption(std.log.Level, "test_log_level", log_level);
    test_exe.root_module.addImport("test_build_options", test_options.createModule());

    return test_exe;
}

const CategorizedTests = struct {
    unit: std.ArrayList(*std.Build.Step.Run),
    integration: std.ArrayList(*std.Build.Step.Run),
    ingestion: std.ArrayList(*std.Build.Step.Run),
    simulation: std.ArrayList(*std.Build.Step.Run),
    stress: std.ArrayList(*std.Build.Step.Run),
    performance: std.ArrayList(*std.Build.Step.Run),
    fault_injection: std.ArrayList(*std.Build.Step.Run),
    recovery: std.ArrayList(*std.Build.Step.Run),
    safety: std.ArrayList(*std.Build.Step.Run),
    defensive: std.ArrayList(*std.Build.Step.Run),

    fn get_category_tests(self: *CategorizedTests, category: TestCategory) *std.ArrayList(*std.Build.Step.Run) {
        return switch (category) {
            .unit => &self.unit,
            .integration => &self.integration,
            .ingestion => &self.ingestion,
            .simulation => &self.simulation,
            .stress => &self.stress,
            .performance => &self.performance,
            .fault_injection => &self.fault_injection,
            .recovery => &self.recovery,
            .safety => &self.safety,
            .defensive => &self.defensive,
        };
    }

    fn init(allocator: std.mem.Allocator) CategorizedTests {
        return CategorizedTests{
            .unit = std.ArrayList(*std.Build.Step.Run).init(allocator),
            .integration = std.ArrayList(*std.Build.Step.Run).init(allocator),
            .ingestion = std.ArrayList(*std.Build.Step.Run).init(allocator),
            .simulation = std.ArrayList(*std.Build.Step.Run).init(allocator),
            .stress = std.ArrayList(*std.Build.Step.Run).init(allocator),
            .performance = std.ArrayList(*std.Build.Step.Run).init(allocator),
            .fault_injection = std.ArrayList(*std.Build.Step.Run).init(allocator),
            .recovery = std.ArrayList(*std.Build.Step.Run).init(allocator),
            .safety = std.ArrayList(*std.Build.Step.Run).init(allocator),
            .defensive = std.ArrayList(*std.Build.Step.Run).init(allocator),
        };
    }
};

fn create_categorized_tests(
    b: *std.Build,
    test_files: []TestFile,
    modules: BuildModules,
    target: std.Build.ResolvedTarget,
    optimize: std.builtin.OptimizeMode,
    allocator: std.mem.Allocator,
) !CategorizedTests {
    var categorized = CategorizedTests.init(allocator);

    // Add unit test runner
    const unit_test_exe = b.addTest(.{
        .root_module = b.createModule(.{
            .root_source_file = b.path("src/unit_tests.zig"),
            .target = target,
            .optimize = optimize,
        }),
    });
    unit_test_exe.linkLibC();

    // Apply sanitizer flags if enabled
    if (modules.enable_thread_sanitizer) {
        unit_test_exe.root_module.sanitize_thread = true;
    }
    if (modules.enable_ubsan) {
        unit_test_exe.root_module.sanitize_c = .full;
    }

    unit_test_exe.root_module.addImport("build_options", modules.build_options);

    const unit_test_run = b.addRunArtifact(unit_test_exe);
    try categorized.unit.append(unit_test_run);

    // Process discovered integration tests
    for (test_files) |test_file| {
        const test_exe = create_test_executable(b, test_file, modules, target, optimize);
        const test_run = b.addRunArtifact(test_exe);

        const category_tests = categorized.get_category_tests(test_file.category);
        try category_tests.append(test_run);
    }

    return categorized;
}

fn create_development_tools(
    b: *std.Build,
    modules: BuildModules,
    target: std.Build.ResolvedTarget,
) void {
    const benchmark_exe = b.addExecutable(.{
        .name = "benchmark",
        .root_module = b.createModule(.{
            .root_source_file = b.path("src/dev/benchmark.zig"),
            .target = target,
            .optimize = .ReleaseFast,
        }),
    });
    benchmark_exe.linkLibC();
    benchmark_exe.root_module.addImport("kausaldb", modules.kausaldb_test);
    const install_benchmark = b.addInstallArtifact(benchmark_exe, .{});
    const benchmark_step = b.step("benchmark", "Build and install benchmark");
    benchmark_step.dependOn(&install_benchmark.step);

    const fuzz_exe = b.addExecutable(.{
        .name = "fuzz",
        .root_module = b.createModule(.{
            .root_source_file = b.path("src/dev/fuzz.zig"),
            .target = target,
            .optimize = .ReleaseFast,
        }),
    });
    fuzz_exe.root_module.addImport("kausaldb", modules.kausaldb_test);
    const install_fuzz = b.addInstallArtifact(fuzz_exe, .{});
    const fuzz_step = b.step("fuzz", "Build and install fuzz tester");
    fuzz_step.dependOn(&install_fuzz.step);

    const commit_validator_exe = b.addExecutable(.{
        .name = "commit-msg-validator",
        .root_module = b.createModule(.{
            .root_source_file = b.path("src/dev/commit_msg_validator.zig"),
            .target = target,
            .optimize = .ReleaseFast,
        }),
    });
    const install_commit_validator = b.addInstallArtifact(commit_validator_exe, .{});
    const commit_validator_step = b.step("commit-msg-validator", "Build and install commit message validator");
    commit_validator_step.dependOn(&install_commit_validator.step);
}

fn create_workflow_steps(
    b: *std.Build,
    categorized: CategorizedTests,
) void {
    // Primary test categories
    const test_step = b.step("test", "Run unit tests");
    for (categorized.unit.items) |test_run| {
        test_step.dependOn(&test_run.step);
    }

    const test_integration_step = b.step("test-integration", "Run integration tests");
    for (categorized.integration.items) |test_run| {
        test_integration_step.dependOn(&test_run.step);
    }

    const test_ingestion_step = b.step("test-ingestion", "Run ingestion tests");
    for (categorized.ingestion.items) |test_run| {
        test_ingestion_step.dependOn(&test_run.step);
    }

    const test_simulation_step = b.step("test-simulation", "Run simulation tests");
    for (categorized.simulation.items) |test_run| {
        test_simulation_step.dependOn(&test_run.step);
    }

    const test_stress_step = b.step("test-stress", "Run stress tests");
    for (categorized.stress.items) |test_run| {
        test_stress_step.dependOn(&test_run.step);
    }

    // Secondary test categories (less common)
    const test_performance_step = b.step("test-performance", "Run performance tests");
    for (categorized.performance.items) |test_run| {
        test_performance_step.dependOn(&test_run.step);
    }

    const test_fault_injection_step = b.step("test-fault-injection", "Run fault injection tests");
    for (categorized.fault_injection.items) |test_run| {
        test_fault_injection_step.dependOn(&test_run.step);
    }

    const test_recovery_step = b.step("test-recovery", "Run recovery tests");
    for (categorized.recovery.items) |test_run| {
        test_recovery_step.dependOn(&test_run.step);
    }

    const test_safety_step = b.step("test-safety", "Run safety tests");
    for (categorized.safety.items) |test_run| {
        test_safety_step.dependOn(&test_run.step);
    }

    const test_defensive_step = b.step("test-defensive", "Run defensive programming tests");
    for (categorized.defensive.items) |test_run| {
        test_defensive_step.dependOn(&test_run.step);
    }

    // Aggregate workflows
    const test_fast_step = b.step("test-fast", "Run core tests (unit + integration)");
    test_fast_step.dependOn(test_step);
    test_fast_step.dependOn(test_integration_step);

    const test_all_step = b.step("test-all", "Run all tests including stress tests");
    test_all_step.dependOn(test_fast_step);
    test_all_step.dependOn(test_ingestion_step);
    test_all_step.dependOn(test_simulation_step);
    test_all_step.dependOn(test_stress_step);
    test_all_step.dependOn(test_performance_step);
    test_all_step.dependOn(test_fault_injection_step);
    test_all_step.dependOn(test_recovery_step);
    test_all_step.dependOn(test_safety_step);
    test_all_step.dependOn(test_defensive_step);

    // Code quality steps
    const tidy_step = b.step("tidy", "Run code quality checks");

    const fmt_check = b.addFmt(.{
        .paths = &.{ "src", "tests", "build.zig" },
        .check = true,
    });
    const fmt_step = b.step("fmt", "Check code formatting");
    fmt_step.dependOn(&fmt_check.step);

    const fmt_fix = b.addFmt(.{
        .paths = &.{ "src", "tests", "build.zig" },
        .check = false,
    });
    const fmt_fix_step = b.step("fmt-fix", "Fix code formatting");
    fmt_fix_step.dependOn(&fmt_fix.step);

    // CI step
    const ci_step = b.step("ci", "Run all CI checks (tests, tidy, format)");
    ci_step.dependOn(test_fast_step);
    ci_step.dependOn(tidy_step);
    ci_step.dependOn(&fmt_check.step);

    // Test listing step
    const test_list_step = b.step("test-list", "List all available individual tests");
    test_list_step.makeFn = struct {
        fn make(step: *std.Build.Step, options: std.Build.Step.MakeOptions) anyerror!void {
            _ = options;
            _ = step;

            std.debug.print("\nAvailable test targets:\n\n", .{});
            std.debug.print("Core test categories:\n", .{});
            std.debug.print("  test                 - Run unit tests\n", .{});
            std.debug.print("  test-integration     - Run integration tests\n", .{});
            std.debug.print("  test-ingestion       - Run ingestion tests\n", .{});
            std.debug.print("  test-simulation      - Run simulation tests\n", .{});
            std.debug.print("  test-stress          - Run stress tests\n", .{});
            std.debug.print("  test-performance     - Run performance tests\n", .{});
            std.debug.print("  test-fault-injection - Run fault injection tests\n", .{});
            std.debug.print("  test-recovery        - Run recovery tests\n", .{});
            std.debug.print("  test-safety          - Run safety tests\n", .{});
            std.debug.print("  test-defensive       - Run defensive programming tests\n", .{});

            std.debug.print("\nWorkflow commands:\n", .{});
            std.debug.print("  test-fast            - Run core tests (unit + integration)\n", .{});
            std.debug.print("  test-all             - Run all tests including stress tests\n", .{});

            std.debug.print("\nUse the category commands above to run groups of tests.\n", .{});
            std.debug.print("Use './zig/zig build --list-steps' to see all available build targets.\n\n", .{});
        }
    }.make;
}

pub fn build(b: *std.Build) void {
    const target = b.standardTargetOptions(.{});
    const optimize = b.standardOptimizeOption(.{});

    // Create core modules
    const modules = create_build_modules(b, target, optimize);

    // Create main executable
    const exe = b.addExecutable(.{
        .name = "kausaldb",
        .root_module = b.createModule(.{
            .root_source_file = b.path("src/main.zig"),
            .target = target,
            .optimize = optimize,
        }),
    });

    // Apply sanitizer flags to main executable
    if (modules.enable_thread_sanitizer) {
        exe.root_module.sanitize_thread = true;
    }
    if (modules.enable_ubsan) {
        exe.root_module.sanitize_c = .full;
    }

    exe.linkLibC();
    exe.root_module.addImport("kausaldb", modules.kausaldb);
    b.installArtifact(exe);

    const run_cmd = b.addRunArtifact(exe);
    run_cmd.step.dependOn(b.getInstallStep());
    if (b.args) |args| {
        run_cmd.addArgs(args);
    }
    const run_step = b.step("run", "Run KausalDB");
    run_step.dependOn(&run_cmd.step);

    // Discover and organize tests
    var arena = std.heap.ArenaAllocator.init(b.allocator);
    defer arena.deinit();
    const arena_allocator = arena.allocator();

    const test_files = discover_test_files(arena_allocator) catch |err| {
        std.debug.print("Failed to discover test files: {}\n", .{err});
        return;
    };

    const categorized = create_categorized_tests(b, test_files.items, modules, target, optimize, arena_allocator) catch |err| {
        std.debug.print("Failed to create categorized tests: {}\n", .{err});
        return;
    };

    // Create development tools and workflow steps
    create_development_tools(b, modules, target);
    create_workflow_steps(b, categorized);
}
