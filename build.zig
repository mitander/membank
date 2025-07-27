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
        .{ .name = "unit", .source_file = "src/main.zig", .description = "unit tests" },
        .{
            .name = "simulation",
            .source_file = "tests/simulation/network.zig",
            .description = "network simulation tests",
        },
        .{
            .name = "storage_stress",
            .source_file = "tests/stress/storage.zig",
            .description = "storage stress tests",
        },
        .{
            .name = "wal_recovery",
            .source_file = "tests/recovery/wal.zig",
            .description = "WAL recovery tests",
        },
        .{
            .name = "streaming_wal_recovery",
            .source_file = "tests/recovery/streaming_wal_recovery.zig",
            .description = "Streaming WAL recovery tests",
        },
        .{
            .name = "wal_segmentation",
            .source_file = "tests/recovery/wal_segmentation.zig",
            .description = "WAL segmentation tests",
        },
        .{
            .name = "wal_memory_safety",
            .source_file = "tests/recovery/wal_memory_safety.zig",
            .description = "WAL memory safety tests",
        },
        .{
            .name = "memory_stress",
            .source_file = "tests/stress/memory.zig",
            .description = "Memory stress tests - single test with multiple cycles",
        },
        .{
            .name = "integration",
            .source_file = "tests/integration/lifecycle.zig",
            .description = "integration lifecycle tests",
        },
        .{
            .name = "debug_allocator",
            .source_file = "tests/debug/debug_allocator.zig",
            .description = "debug allocator tests",
        },
        .{
            .name = "wal_hang_debug",
            .source_file = "tests/debug/wal_hang_debug.zig",
            .description = "WAL hang debug tests",
        },
        .{
            .name = "wal_corruption_debug",
            .source_file = "tests/debug/wal_corruption_debug.zig",
            .description = "WAL corruption debug tests",
        },
        .{
            .name = "vfs_direct_debug",
            .source_file = "tests/debug/vfs_direct_debug.zig",
            .description = "Direct VFS corruption debug tests",
        },
        .{
            .name = "wal_write_read_debug",
            .source_file = "tests/debug/wal_write_read_debug.zig",
            .description = "WAL write then direct read debug tests",
        },
        .{
            .name = "vfs_read_boundary_debug",
            .source_file = "tests/debug/vfs_read_boundary_debug.zig",
            .description = "VFS read boundary condition debug tests",
        },

        .{
            .name = "vfs_memory_safety",
            .source_file = "tests/debug/vfs_memory_safety.zig",
            .description = "VFS memory safety unit tests",
        },
        .{
            .name = "allocator_torture",
            .source_file = "src/dev/allocator_torture_test.zig",
            .description = "allocator torture tests",
        },
        .{
            .name = "ingestion",
            .source_file = "tests/integration/ingestion.zig",
            .description = "ingestion pipeline integration tests",
        },
        .{
            .name = "fault_injection",
            .source_file = "tests/fault_injection/storage_faults.zig",
            .description = "fault injection and storage resilience tests",
        },
        .{
            .name = "server_protocol",
            .source_file = "tests/server/protocol_tests.zig",
            .description = "TCP server and binary protocol tests",
        },
        .{
            .name = "memtable_manager",
            .source_file = "tests/storage/memtable_manager_test.zig",
            .description = "isolated MemtableManager component tests",
        },
        .{
            .name = "sstable_manager",
            .source_file = "tests/storage/sstable_manager_test.zig",
            .description = "isolated SSTableManager component tests",
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
    for (test_configs, test_steps) |config, run_test| {
        // Skip debug_allocator in Debug mode due to slow linking performance
        if (std.mem.eql(u8, config.name, "debug_allocator") and optimize_mode == .Debug) {
            continue;
        }
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
