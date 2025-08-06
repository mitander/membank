const std = @import("std");

// Test discovery and configuration
const TestConfig = struct {
    name: []const u8,
    source_file: []const u8,
    description: []const u8,
};

// Dynamic test discovery function
fn discover_test_files(allocator: std.mem.Allocator, tests_dir_path: []const u8) !std.ArrayList(TestConfig) {
    var discovered_tests = std.ArrayList(TestConfig).init(allocator);

    // Add the unit test (special case - embedded in src/main.zig)
    try discovered_tests.append(.{
        .name = "unit",
        .source_file = "src/main.zig",
        .description = "core component unit tests",
    });

    var tests_dir = std.fs.cwd().openDir(tests_dir_path, .{ .iterate = true }) catch |err| switch (err) {
        error.FileNotFound => return discovered_tests, // No tests directory, return unit tests only
        else => return err,
    };
    defer tests_dir.close();

    var walker = try tests_dir.walk(allocator);
    defer walker.deinit();

    while (try walker.next()) |entry| {
        if (entry.kind != .file or !std.mem.endsWith(u8, entry.path, ".zig")) continue;

        // Skip helper files that aren't test files
        if (std.mem.endsWith(u8, entry.path, "_helper.zig") or
            std.mem.endsWith(u8, entry.path, "helper.zig")) continue;

        // Generate step name from path: tests/category/file.zig -> category_file
        const step_name = blk: {
            var name_buf: [256]u8 = undefined;
            var name_len: usize = 0;

            // Split path and build name
            var path_parts = std.mem.splitSequence(u8, entry.path, "/");
            var is_first = true;
            while (path_parts.next()) |part| {
                if (std.mem.endsWith(u8, part, ".zig")) {
                    // Remove .zig extension
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

            break :blk try allocator.dupe(u8, name_buf[0..name_len]);
        };

        // Generate description from category and filename
        const description = blk: {
            const category = std.fs.path.dirname(entry.path) orelse "general";
            const basename = std.fs.path.stem(entry.path);
            break :blk try std.fmt.allocPrint(allocator, "{s} {s} tests", .{ category, basename });
        };

        const full_path = try std.fmt.allocPrint(allocator, "tests/{s}", .{entry.path});

        try discovered_tests.append(.{
            .name = step_name,
            .source_file = full_path,
            .description = description,
        });
    }

    return discovered_tests;
}

pub fn build(b: *std.Build) void {
    // Build configuration options
    const enable_statistics = b.option(bool, "enable-stats", "Enable runtime statistics collection (debug builds only)") orelse false;
    const enable_detailed_logging = b.option(bool, "enable-detailed-logs", "Enable detailed debug logging") orelse false;
    const enable_fault_injection = b.option(bool, "enable-fault-injection", "Enable fault injection testing") orelse false;
    const target = b.standardTargetOptions(.{});
    const optimize = b.standardOptimizeOption(.{});

    // Create build options module
    const build_options = b.addOptions();
    build_options.addOption(bool, "enable_statistics", enable_statistics);
    build_options.addOption(bool, "enable_detailed_logging", enable_detailed_logging);
    build_options.addOption(bool, "enable_fault_injection", enable_fault_injection);
    build_options.addOption(bool, "is_debug_build", optimize == .Debug);

    const kausaldb_module = b.createModule(.{
        .root_source_file = b.path("src/kausaldb.zig"),
        .target = target,
        .optimize = optimize,
    });

    // Testing module with internal APIs for test files
    const kausaldb_test_module = b.createModule(.{
        .root_source_file = b.path("src/kausaldb_test.zig"),
        .target = target,
        .optimize = optimize,
    });

    // Add build options to core module for configuration access
    kausaldb_module.addImport("build_options", build_options.createModule());
    kausaldb_test_module.addImport("build_options", build_options.createModule());

    const exe = b.addExecutable(.{
        .name = "kausaldb",
        .root_module = b.createModule(.{
            .root_source_file = b.path("src/main.zig"),
            .target = target,
            .optimize = optimize,
        }),
    });
    exe.linkLibC(); // Required for production VFS sync() calls
    exe.root_module.addImport("kausaldb", kausaldb_module);
    b.installArtifact(exe);

    // Run command for main executable
    const run_cmd = b.addRunArtifact(exe);
    run_cmd.step.dependOn(b.getInstallStep());
    if (b.args) |args| {
        run_cmd.addArgs(args);
    }

    const run_step = b.step("run", "Run KausalDB");
    run_step.dependOn(&run_cmd.step);

    // Discover test files dynamically
    var arena = std.heap.ArenaAllocator.init(b.allocator);
    defer arena.deinit();
    const arena_allocator = arena.allocator();

    const test_configs = discover_test_files(arena_allocator, "tests") catch |err| {
        std.debug.print("Failed to discover test files: {}\n", .{err});
        // Fallback to minimal unit test only
        return;
    };

    var test_steps = std.ArrayList(*std.Build.Step.Run).init(arena_allocator);
    var test_install_steps = std.ArrayList(*std.Build.Step.InstallArtifact).init(arena_allocator);

    // Create all test executables
    for (test_configs.items) |config| {
        const test_exe = b.addTest(.{
            .root_module = b.createModule(.{
                .root_source_file = b.path(config.source_file),
                .target = target,
                .optimize = optimize,
            }),
        });
        // Provide both public API and internal testing APIs
        test_exe.root_module.addImport("kausaldb", kausaldb_test_module);

        const install_step = b.addInstallArtifact(test_exe, .{});
        test_install_steps.append(install_step) catch @panic("Failed to append test install step");

        const test_run_step = b.addRunArtifact(test_exe);
        test_steps.append(test_run_step) catch @panic("Failed to append test run step");
    }

    for (test_configs.items, test_steps.items) |config, test_run| {
        const step_name = if (std.mem.eql(u8, config.name, "unit")) "unit-test" else config.name;
        const individual_step = b.step(step_name, b.fmt("Run {s}", .{config.description}));
        individual_step.dependOn(&test_run.step);
    }

    // Tidy tests for code quality
    const tidy_tests = b.addTest(.{
        .root_module = b.createModule(.{
            .root_source_file = b.path("src/dev/tidy.zig"),
            .target = target,
            .optimize = optimize,
        }),
    });
    tidy_tests.root_module.addImport("kausaldb", kausaldb_test_module);
    tidy_tests.root_module.addImport("kausaldb_test", kausaldb_test_module);
    const run_tidy_tests = b.addRunArtifact(tidy_tests);

    // Fast developer workflow: core functionality validation only
    const unit_test_run_step = blk: {
        for (test_configs.items, test_steps.items) |config, test_run| {
            if (std.mem.eql(u8, config.name, "unit")) break :blk test_run;
        }
        @panic("unit test config not found");
    };
    const test_step = b.step("test", "Run fast unit tests (developer default)");
    test_step.dependOn(&unit_test_run_step.step);

    // CI validation: full coverage with reasonable runtime
    const test_fast_step = b.step("test-fast", "Run tests (fast CI validation)");
    for (test_steps.items) |test_run| {
        test_fast_step.dependOn(&test_run.step);
    }

    // Complete validation: includes memory stress and edge cases
    const test_all_step = b.step("test-all", "Run all tests including problematic ones");
    for (test_steps.items) |test_run| {
        test_all_step.dependOn(&test_run.step);
    }
    // Enable memory debugging with external tools like Valgrind
    for (test_install_steps.items) |install_step| {
        test_all_step.dependOn(&install_step.step);
    }

    const tidy_step = b.step("tidy", "Run code quality checks");
    tidy_step.dependOn(&run_tidy_tests.step);

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

    const check_step = b.step("check", "Run tests and code quality checks");
    check_step.dependOn(test_step);
    check_step.dependOn(&run_tidy_tests.step);

    const ci_step = b.step("ci", "Run all CI checks (tests, tidy, format)");
    ci_step.dependOn(test_fast_step);
    ci_step.dependOn(&run_tidy_tests.step);
    ci_step.dependOn(&fmt_check.step);

    const benchmark = b.addExecutable(.{
        .name = "benchmark",
        .root_module = b.createModule(.{
            .root_source_file = b.path("src/dev/benchmark.zig"),
            .target = target,
            .optimize = .ReleaseFast,
        }),
    });
    benchmark.root_module.addImport("kausaldb", kausaldb_test_module);

    const install_benchmark = b.addInstallArtifact(benchmark, .{});
    const benchmark_step = b.step("benchmark", "Build and install benchmark");
    benchmark_step.dependOn(&install_benchmark.step);

    const allocator_torture = b.addExecutable(.{
        .name = "allocator_torture",
        .root_module = b.createModule(.{
            .root_source_file = b.path("src/dev/allocator_torture_test.zig"),
            .target = target,
            .optimize = .ReleaseSafe,
        }),
    });
    allocator_torture.root_module.addImport("kausaldb", kausaldb_test_module);

    const install_allocator_torture = b.addInstallArtifact(allocator_torture, .{});
    const allocator_torture_step = b.step("allocator_torture", "Build and install allocator torture tester");
    allocator_torture_step.dependOn(&install_allocator_torture.step);

    const fuzz = b.addExecutable(.{
        .name = "fuzz",
        .root_module = b.createModule(.{
            .root_source_file = b.path("src/dev/fuzz.zig"),
            .target = target,
            .optimize = .ReleaseFast,
        }),
    });
    fuzz.root_module.addImport("kausaldb", kausaldb_test_module);

    const install_fuzz = b.addInstallArtifact(fuzz, .{});
    const fuzz_step = b.step("fuzz", "Build and install fuzz tester");
    fuzz_step.dependOn(&install_fuzz.step);

    const fuzz_debug = b.addExecutable(.{
        .name = "fuzz-debug",
        .root_module = b.createModule(.{
            .root_source_file = b.path("src/dev/fuzz.zig"),
            .target = target,
            .optimize = .Debug,
        }),
    });
    fuzz_debug.root_module.addImport("kausaldb", kausaldb_test_module);

    const install_fuzz_debug = b.addInstallArtifact(fuzz_debug, .{});
    const fuzz_debug_step = b.step("fuzz-debug", "Build and install debug fuzz tester with enhanced debugging");
    fuzz_debug_step.dependOn(&install_fuzz_debug.step);

    const commit_msg_validator = b.addExecutable(.{
        .name = "commit-msg-validator",
        .root_module = b.createModule(.{
            .root_source_file = b.path("src/dev/commit_msg_validator.zig"),
            .target = target,
            .optimize = .ReleaseFast,
        }),
    });

    const install_commit_validator = b.addInstallArtifact(commit_msg_validator, .{});
    const commit_validator_step = b.step("commit-msg-validator", "Build and install Zig-based commit message validator");
    commit_validator_step.dependOn(&install_commit_validator.step);

    // Sanitizer test target for enhanced memory safety
    const sanitizer_tests = b.addTest(.{
        .root_module = b.createModule(.{
            .root_source_file = b.path("src/kausaldb.zig"),
            .target = target,
            .optimize = .ReleaseSafe,
        }),
    });
    sanitizer_tests.root_module.addImport("kausaldb", kausaldb_module);

    // Detect concurrency issues in single-threaded architecture validation
    sanitizer_tests.root_module.sanitize_thread = true;
    // Catch undefined behavior in foreign function interfaces
    sanitizer_tests.root_module.sanitize_c = .full;

    const run_sanitizer_tests = b.addRunArtifact(sanitizer_tests);
    const sanitizer_test_step = b.step("test-sanitizer", "Run tests with Thread Sanitizer and C UBSan");
    sanitizer_test_step.dependOn(&run_sanitizer_tests.step);

    const memory_stress = b.addTest(.{
        .root_module = b.createModule(.{
            .root_source_file = b.path("tests/stress/memory.zig"),
            .target = target,
            .optimize = .ReleaseSafe,
        }),
    });
    memory_stress.root_module.addImport("kausaldb", kausaldb_module);

    const run_memory_stress = b.addRunArtifact(memory_stress);
    const memory_stress_step = b.step("test-memory-stress", "Run memory safety stress tests");
    memory_stress_step.dependOn(&run_memory_stress.step);
}
