const std = @import("std");

pub fn build(b: *std.Build) void {
    const target = b.standardTargetOptions(.{});
    const optimize_mode = b.standardOptimizeOption(.{});

    // Main executable
    const exe = b.addExecutable(.{
        .name = "cortexdb",
        .root_module = b.createModule(.{
            .root_source_file = b.path("src/main.zig"),
            .target = target,
            .optimize = optimize_mode,
        }),
    });

    // Install the executable
    b.installArtifact(exe);

    // Run command for the main executable
    const run_cmd = b.addRunArtifact(exe);
    run_cmd.step.dependOn(b.getInstallStep());
    if (b.args) |args| {
        run_cmd.addArgs(args);
    }

    const run_step = b.step("run", "Run CortexDB");
    run_step.dependOn(&run_cmd.step);

    // Unit tests
    const unit_tests = b.addTest(.{
        .root_module = b.createModule(.{
            .root_source_file = b.path("src/main.zig"),
            .target = target,
            .optimize = optimize_mode,
        }),
    });

    const run_unit_tests = b.addRunArtifact(unit_tests);

    // Tidy tests for code quality checking
    const tidy_tests = b.addTest(.{
        .root_module = b.createModule(.{
            .root_source_file = b.path("src/tidy.zig"),
            .target = target,
            .optimize = optimize_mode,
        }),
    });

    const run_tidy_tests = b.addRunArtifact(tidy_tests);

    // Simulation tests
    const simulation_tests = b.addTest(.{
        .root_module = b.createModule(.{
            .root_source_file = b.path("tests/simulation_test.zig"),
            .target = target,
            .optimize = optimize_mode,
        }),
    });

    // Add source files as dependencies for simulation tests
    const assert_module = b.createModule(.{
        .root_source_file = b.path("src/assert.zig"),
    });
    const vfs_module = b.createModule(.{
        .root_source_file = b.path("src/vfs.zig"),
    });
    const simulation_vfs_module = b.createModule(.{
        .root_source_file = b.path("src/simulation_vfs.zig"),
    });
    simulation_vfs_module.addImport("vfs", vfs_module);
    simulation_vfs_module.addImport("assert", assert_module);

    const simulation_module = b.createModule(.{
        .root_source_file = b.path("src/simulation.zig"),
    });
    simulation_module.addImport("assert", assert_module);
    simulation_module.addImport("vfs", vfs_module);
    simulation_module.addImport("simulation_vfs", simulation_vfs_module);

    simulation_tests.root_module.addImport("simulation", simulation_module);
    simulation_tests.root_module.addImport("vfs", vfs_module);
    simulation_tests.root_module.addImport("assert", assert_module);

    const run_simulation_tests = b.addRunArtifact(simulation_tests);

    // Test step that runs all tests
    const test_step = b.step("test", "Run all tests");
    test_step.dependOn(&run_unit_tests.step);
    test_step.dependOn(&run_simulation_tests.step);

    // Separate tidy step for code quality checks
    const tidy_step = b.step("tidy", "Run code quality checks");
    tidy_step.dependOn(&run_tidy_tests.step);

    // Full check step that runs tests + tidy
    const check_step = b.step("check", "Run tests and code quality checks");
    check_step.dependOn(&run_unit_tests.step);
    check_step.dependOn(&run_simulation_tests.step);
    check_step.dependOn(&run_tidy_tests.step);

    // Format check
    const fmt_check = b.addFmt(.{
        .paths = &.{ "src", "build.zig" },
        .check = true,
    });

    const fmt_step = b.step("fmt", "Check code formatting");
    fmt_step.dependOn(&fmt_check.step);

    // Format fix
    const fmt_fix = b.addFmt(.{
        .paths = &.{ "src", "build.zig" },
        .check = false,
    });

    const fmt_fix_step = b.step("fmt-fix", "Fix code formatting");
    fmt_fix_step.dependOn(&fmt_fix.step);

    // CI step that runs everything
    const ci_step = b.step("ci", "Run all CI checks (tests, tidy, format)");
    ci_step.dependOn(&run_unit_tests.step);
    ci_step.dependOn(&run_simulation_tests.step);
    ci_step.dependOn(&run_tidy_tests.step);
    ci_step.dependOn(&fmt_check.step);

    // Benchmark executable (for future use)
    const benchmark = b.addExecutable(.{
        .name = "benchmark",
        .root_module = b.createModule(.{
            .root_source_file = b.path("src/benchmark.zig"),
            .target = target,
            .optimize = .ReleaseFast,
        }),
    });

    const install_benchmark = b.addInstallArtifact(benchmark, .{});
    const benchmark_step = b.step("benchmark", "Build and install benchmark");
    benchmark_step.dependOn(&install_benchmark.step);

    // Fuzz testing executable (for future use)
    const fuzz = b.addExecutable(.{
        .name = "fuzz",
        .root_module = b.createModule(.{
            .root_source_file = b.path("src/fuzz.zig"),
            .target = target,
            .optimize = .ReleaseFast,
        }),
    });

    const install_fuzz = b.addInstallArtifact(fuzz, .{});
    const fuzz_step = b.step("fuzz", "Build and install fuzz tester");
    fuzz_step.dependOn(&install_fuzz.step);
}
