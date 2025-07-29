const std = @import("std");

pub fn build(b: *std.Build) void {
    const target = b.standardTargetOptions(.{});
    const optimize_mode = b.standardOptimizeOption(.{});

    // Create the cortexdb library module
    const cortexdb_module = b.createModule(.{
        .root_source_file = b.path("src/cortexdb.zig"),
    });

    // Main executable
    const exe = b.addExecutable(.{
        .name = "cortexdb",
        .root_module = b.createModule(.{
            .root_source_file = b.path("src/main.zig"),
            .target = target,
            .optimize = optimize_mode,
        }),
    });
    b.installArtifact(exe);

    // Run command for main executable
    const run_cmd = b.addRunArtifact(exe);
    run_cmd.step.dependOn(b.getInstallStep());
    if (b.args) |args| {
        run_cmd.addArgs(args);
    }

    const run_step = b.step("run", "Run CortexDB");
    run_step.dependOn(&run_cmd.step);

    // Test configuration
    const TestConfig = struct {
        name: []const u8,
        source_file: []const u8,
        description: []const u8,
    };

    const test_configs = [_]TestConfig{
        // Core unit tests - run tests embedded in implementation files
        .{ .name = "unit", .source_file = "src/main.zig", .description = "core component unit tests" },

        // Integration tests - cross-component behavior validation
        .{
            .name = "integration_lifecycle",
            .source_file = "tests/integration/lifecycle.zig",
            .description = "storage engine lifecycle integration tests",
        },
        .{
            .name = "integration_ingestion",
            .source_file = "tests/integration/ingestion.zig",
            .description = "ingestion pipeline integration tests",
        },

        // Simulation tests - deterministic failure scenario validation
        .{
            .name = "simulation_network",
            .source_file = "tests/simulation/network.zig",
            .description = "network partition simulation tests",
        },

        // Stress tests - high-load and resource exhaustion validation
        .{
            .name = "stress_storage",
            .source_file = "tests/stress/storage.zig",
            .description = "storage engine stress tests",
        },
        .{
            .name = "stress_memory",
            .source_file = "tests/stress/memory.zig",
            .description = "memory allocation stress tests",
        },

        // Recovery tests - WAL recovery and corruption handling
        .{
            .name = "recovery_wal",
            .source_file = "tests/recovery/wal.zig",
            .description = "WAL recovery tests",
        },
        .{
            .name = "recovery_streaming_wal",
            .source_file = "tests/recovery/streaming_wal_recovery.zig",
            .description = "streaming WAL recovery tests",
        },
        .{
            .name = "recovery_wal_segmentation",
            .source_file = "tests/recovery/wal_segmentation.zig",
            .description = "WAL segmentation tests",
        },
        .{
            .name = "recovery_wal_memory_safety",
            .source_file = "tests/recovery/wal_memory_safety.zig",
            .description = "WAL memory safety tests",
        },
        .{
            .name = "recovery_wal_corruption",
            .source_file = "tests/recovery/wal_corruption.zig",
            .description = "WAL corruption detection and recovery tests",
        },
        .{
            .name = "recovery_wal_corruption_fatal",
            .source_file = "tests/recovery/wal_corruption_fatal.zig",
            .description = "WAL corruption fatal assertion tests",
        },

        // Safety tests - memory corruption and safety validation
        .{
            .name = "safety_memory_corruption",
            .source_file = "tests/safety/memory_corruption.zig",
            .description = "memory safety and corruption detection tests",
        },
        .{
            .name = "safety_memory_fatal",
            .source_file = "tests/safety/memory_safety_fatal.zig",
            .description = "memory safety fatal assertion tests",
        },

        // Fault injection tests - targeted failure injection validation
        .{
            .name = "fault_injection_storage",
            .source_file = "tests/fault_injection/storage_faults.zig",
            .description = "storage fault injection tests",
        },
        .{
            .name = "fault_injection_wal_cleanup",
            .source_file = "tests/fault_injection/wal_cleanup_faults.zig",
            .description = "WAL cleanup fault injection tests",
        },

        // Performance tests - regression detection and benchmarking
        .{
            .name = "performance_streaming",
            .source_file = "tests/performance/streaming_memory_benchmark.zig",
            .description = "streaming memory efficiency benchmarks",
        },

        // Defensive programming tests - assertion and safety validation
        .{
            .name = "defensive_assertions",
            .source_file = "tests/defensive/assertion_validation.zig",
            .description = "defensive assertion validation tests",
        },
        .{
            .name = "defensive_corruption",
            .source_file = "tests/defensive/corruption_injection.zig",
            .description = "corruption detection validation tests",
        },
        .{
            .name = "defensive_performance",
            .source_file = "tests/defensive/performance_impact.zig",
            .description = "defensive programming performance impact tests",
        },

        // Server tests - network protocol and API validation
        .{
            .name = "server_protocol",
            .source_file = "tests/server/protocol.zig",
            .description = "TCP server protocol tests",
        },
    };

    var test_steps: [test_configs.len]*std.Build.Step.Run = undefined;

    // Create all test executables
    for (test_configs, 0..) |config, i| {
        const test_exe = b.addTest(.{
            .root_module = b.createModule(.{
                .root_source_file = b.path(config.source_file),
                .target = target,
                .optimize = optimize_mode,
            }),
        });
        // Add cortexdb module to all tests
        test_exe.root_module.addImport("cortexdb", cortexdb_module);
        test_steps[i] = b.addRunArtifact(test_exe);
    }

    // Create individual test steps
    for (test_configs, test_steps) |config, run_test| {
        const step_name = if (std.mem.eql(u8, config.name, "unit")) "unit-test" else config.name;
        const individual_step = b.step(step_name, b.fmt("Run {s}", .{config.description}));
        individual_step.dependOn(&run_test.step);
    }

    // Tidy tests for code quality
    const tidy_tests = b.addTest(.{
        .root_module = b.createModule(.{
            .root_source_file = b.path("src/dev/tidy.zig"),
            .target = target,
            .optimize = optimize_mode,
        }),
    });
    tidy_tests.root_module.addImport("cortexdb", cortexdb_module);
    const run_tidy_tests = b.addRunArtifact(tidy_tests);

    // Test step that runs all tests
    const test_step = b.step("test", "Run all tests");
    for (test_steps) |run_test| {
        test_step.dependOn(&run_test.step);
    }

    // Code quality steps
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

    // Combined steps
    const check_step = b.step("check", "Run tests and code quality checks");
    check_step.dependOn(test_step);
    check_step.dependOn(&run_tidy_tests.step);

    const ci_step = b.step("ci", "Run all CI checks (tests, tidy, format)");
    ci_step.dependOn(test_step);
    ci_step.dependOn(&run_tidy_tests.step);
    ci_step.dependOn(&fmt_check.step);

    // Development tools
    const benchmark = b.addExecutable(.{
        .name = "benchmark",
        .root_module = b.createModule(.{
            .root_source_file = b.path("src/dev/benchmark.zig"),
            .target = target,
            .optimize = .ReleaseFast,
        }),
    });
    benchmark.root_module.addImport("cortexdb", cortexdb_module);

    const install_benchmark = b.addInstallArtifact(benchmark, .{});
    const benchmark_step = b.step("benchmark", "Build and install benchmark");
    benchmark_step.dependOn(&install_benchmark.step);

    const fuzz = b.addExecutable(.{
        .name = "fuzz",
        .root_module = b.createModule(.{
            .root_source_file = b.path("src/dev/fuzz.zig"),
            .target = target,
            .optimize = .ReleaseFast,
        }),
    });
    fuzz.root_module.addImport("cortexdb", cortexdb_module);

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
    fuzz_debug.root_module.addImport("cortexdb", cortexdb_module);

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
            .root_source_file = b.path("src/cortexdb.zig"),
            .target = target,
            .optimize = .ReleaseSafe,
        }),
    });
    sanitizer_tests.root_module.addImport("cortexdb", cortexdb_module);

    // Enable Thread Sanitizer for race condition detection
    sanitizer_tests.root_module.sanitize_thread = true;
    // Enable C undefined behavior detection
    sanitizer_tests.root_module.sanitize_c = .full;

    const run_sanitizer_tests = b.addRunArtifact(sanitizer_tests);
    const sanitizer_test_step = b.step("test-sanitizer", "Run tests with Thread Sanitizer and C UBSan");
    sanitizer_test_step.dependOn(&run_sanitizer_tests.step);

    // Memory stress test target
    const memory_stress = b.addTest(.{
        .root_module = b.createModule(.{
            .root_source_file = b.path("tests/stress/memory.zig"),
            .target = target,
            .optimize = .ReleaseSafe,
        }),
    });
    memory_stress.root_module.addImport("cortexdb", cortexdb_module);

    const run_memory_stress = b.addRunArtifact(memory_stress);
    const memory_stress_step = b.step("test-memory-stress", "Run comprehensive memory safety stress tests");
    memory_stress_step.dependOn(&run_memory_stress.step);

    // Note: Sanitizer and stress tests available as separate targets:
    // ./zig/zig build test-sanitizer
    // ./zig/zig build test-memory-stress
}
