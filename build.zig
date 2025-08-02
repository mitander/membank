const std = @import("std");

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

    const membank_module = b.createModule(.{
        .root_source_file = b.path("src/membank.zig"),
        .target = target,
        .optimize = optimize,
    });

    // Testing module with internal APIs for test files
    const membank_test_module = b.createModule(.{
        .root_source_file = b.path("src/membank_test.zig"),
        .target = target,
        .optimize = optimize,
    });

    // Add build options to core module for configuration access
    membank_module.addImport("build_options", build_options.createModule());
    membank_test_module.addImport("build_options", build_options.createModule());

    const exe = b.addExecutable(.{
        .name = "membank",
        .root_module = b.createModule(.{
            .root_source_file = b.path("src/main.zig"),
            .target = target,
            .optimize = optimize,
        }),
    });
    exe.root_module.addImport("membank", membank_module);
    b.installArtifact(exe);

    // Run command for main executable
    const run_cmd = b.addRunArtifact(exe);
    run_cmd.step.dependOn(b.getInstallStep());
    if (b.args) |args| {
        run_cmd.addArgs(args);
    }

    const run_step = b.step("run", "Run Membank");
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
        .{
            .name = "fault_injection_ingestion",
            .source_file = "tests/fault_injection/ingestion_faults.zig",
            .description = "ingestion pipeline fault injection tests",
        },
        .{
            .name = "fault_injection_query",
            .source_file = "tests/fault_injection/query_faults.zig",
            .description = "query engine fault injection tests",
        },
        .{
            .name = "fault_injection_network",
            .source_file = "tests/fault_injection/network_faults.zig",
            .description = "network layer fault injection tests",
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

        // Network protocol and API validation
        .{
            .name = "server_protocol",
            .source_file = "tests/server/protocol.zig",
            .description = "TCP server protocol tests",
        },

        // Bloom filter validation
        .{
            .name = "bloom_filter_validation",
            .source_file = "tests/storage/bloom_filter_validation.zig",
            .description = "comprehensive Bloom filter integration and validation tests",
        },

        // Advanced query scenarios
        .{
            .name = "streaming_and_optimization",
            .source_file = "tests/query/streaming_and_optimization.zig",
            .description = "streaming query results and optimization strategy tests",
        },

        // Compaction strategies
        .{
            .name = "enhanced_compaction_strategies",
            .source_file = "tests/storage/enhanced_compaction_strategies.zig",
            .description = "enhanced tiered compaction strategy and edge case tests",
        },

        // Ingestion Pipeline Backpressure - Integration tests
        .{
            .name = "backpressure_integration",
            .source_file = "tests/ingestion/backpressure_integration.zig",
            .description = "ingestion pipeline backpressure control and memory pressure adaptation tests",
        },
    };

    var test_steps: [test_configs.len]*std.Build.Step.Run = undefined;
    var test_install_steps: [test_configs.len]*std.Build.Step.InstallArtifact = undefined;

    // Create all test executables
    for (test_configs, 0..) |config, i| {
        const test_exe = b.addTest(.{
            .root_module = b.createModule(.{
                .root_source_file = b.path(config.source_file),
                .target = target,
                .optimize = optimize,
            }),
        });
        // Provide both public API and internal testing APIs
        test_exe.root_module.addImport("membank", membank_test_module);

        test_install_steps[i] = b.addInstallArtifact(test_exe, .{});

        test_steps[i] = b.addRunArtifact(test_exe);
    }

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
            .optimize = optimize,
        }),
    });
    tidy_tests.root_module.addImport("membank", membank_test_module);
    tidy_tests.root_module.addImport("membank_test", membank_test_module);
    const run_tidy_tests = b.addRunArtifact(tidy_tests);

    // Fast developer workflow: core functionality validation only
    const unit_test_index = blk: {
        for (test_configs, 0..) |config, i| {
            if (std.mem.eql(u8, config.name, "unit")) break :blk i;
        }
        @panic("unit test config not found");
    };
    const test_step = b.step("test", "Run fast unit tests (developer default)");
    test_step.dependOn(&test_steps[unit_test_index].step);

    // CI validation: comprehensive coverage with reasonable runtime
    const test_fast_step = b.step("test-fast", "Run comprehensive tests (fast CI validation)");
    for (test_steps) |run_test| {
        test_fast_step.dependOn(&run_test.step);
    }

    // Complete validation: includes memory stress and edge cases
    const test_all_step = b.step("test-all", "Run all tests including problematic ones");
    for (test_steps) |run_test| {
        test_all_step.dependOn(&run_test.step);
    }
    // Enable memory debugging with external tools like Valgrind
    for (test_install_steps) |install_step| {
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
    benchmark.root_module.addImport("membank", membank_test_module);

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
    allocator_torture.root_module.addImport("membank", membank_test_module);

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
    fuzz.root_module.addImport("membank", membank_test_module);

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
    fuzz_debug.root_module.addImport("membank", membank_test_module);

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
            .root_source_file = b.path("src/membank.zig"),
            .target = target,
            .optimize = .ReleaseSafe,
        }),
    });
    sanitizer_tests.root_module.addImport("membank", membank_module);

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
    memory_stress.root_module.addImport("membank", membank_module);

    const run_memory_stress = b.addRunArtifact(memory_stress);
    const memory_stress_step = b.step("test-memory-stress", "Run comprehensive memory safety stress tests");
    memory_stress_step.dependOn(&run_memory_stress.step);
}
