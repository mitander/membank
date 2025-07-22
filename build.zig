const std = @import("std");

// Module collection for easy management
const CoreModules = struct {
    assert: *std.Build.Module,
    vfs: *std.Build.Module,
    context_block: *std.Build.Module,
    error_context: *std.Build.Module,
    concurrency: *std.Build.Module,
    bloom_filter: *std.Build.Module,
    simulation_vfs: *std.Build.Module,
    simulation: *std.Build.Module,
    sstable: *std.Build.Module,
    tiered_compaction: *std.Build.Module,
    storage: *std.Build.Module,
    query_engine: *std.Build.Module,
    ingestion: *std.Build.Module,
    git_source: *std.Build.Module,
    zig_parser: *std.Build.Module,
    semantic_chunker: *std.Build.Module,
    debug_allocator: *std.Build.Module,
    allocator_torture_test: *std.Build.Module,
    server: *std.Build.Module,
};

fn create_core_modules(b: *std.Build) CoreModules {
    // Foundation modules (no dependencies)
    const assert_module = b.createModule(.{
        .root_source_file = b.path("src/assert.zig"),
    });

    const vfs_module = b.createModule(.{
        .root_source_file = b.path("src/vfs.zig"),
    });

    const context_block_module = b.createModule(.{
        .root_source_file = b.path("src/context_block.zig"),
    });

    const concurrency_module = b.createModule(.{
        .root_source_file = b.path("src/concurrency.zig"),
    });

    const bloom_filter_module = b.createModule(.{
        .root_source_file = b.path("src/bloom_filter.zig"),
    });
    bloom_filter_module.addImport("context_block", context_block_module);

    // Modules with single dependencies
    const error_context_module = b.createModule(.{
        .root_source_file = b.path("src/error_context.zig"),
    });
    error_context_module.addImport("context_block", context_block_module);

    const simulation_vfs_module = b.createModule(.{
        .root_source_file = b.path("src/simulation_vfs.zig"),
    });
    simulation_vfs_module.addImport("vfs", vfs_module);
    simulation_vfs_module.addImport("assert", assert_module);

    // Simulation framework
    const simulation_module = b.createModule(.{
        .root_source_file = b.path("src/simulation.zig"),
    });
    simulation_module.addImport("assert", assert_module);
    simulation_module.addImport("vfs", vfs_module);
    simulation_module.addImport("simulation_vfs", simulation_vfs_module);

    // Storage layer modules
    const sstable_module = b.createModule(.{
        .root_source_file = b.path("src/sstable.zig"),
    });
    sstable_module.addImport("context_block", context_block_module);
    sstable_module.addImport("vfs", vfs_module);
    sstable_module.addImport("bloom_filter", bloom_filter_module);
    sstable_module.addImport("simulation_vfs", simulation_vfs_module);
    sstable_module.addImport("error_context", error_context_module);

    const tiered_compaction_module = b.createModule(.{
        .root_source_file = b.path("src/tiered_compaction.zig"),
    });
    tiered_compaction_module.addImport("vfs", vfs_module);
    tiered_compaction_module.addImport("sstable", sstable_module);
    tiered_compaction_module.addImport("concurrency", concurrency_module);

    const storage_module = b.createModule(.{
        .root_source_file = b.path("src/storage.zig"),
    });
    storage_module.addImport("vfs", vfs_module);
    storage_module.addImport("context_block", context_block_module);
    storage_module.addImport("sstable", sstable_module);
    storage_module.addImport("error_context", error_context_module);
    storage_module.addImport("concurrency", concurrency_module);
    storage_module.addImport("tiered_compaction", tiered_compaction_module);

    // Query engine (top level)
    const query_engine_module = b.createModule(.{
        .root_source_file = b.path("src/query_engine.zig"),
    });
    query_engine_module.addImport("storage", storage_module);
    query_engine_module.addImport("context_block", context_block_module);

    // Ingestion pipeline
    const ingestion_module = b.createModule(.{
        .root_source_file = b.path("src/ingestion.zig"),
    });
    ingestion_module.addImport("context_block", context_block_module);
    ingestion_module.addImport("vfs", vfs_module);
    ingestion_module.addImport("assert", assert_module);
    ingestion_module.addImport("concurrency", concurrency_module);

    // Git source connector
    const git_source_module = b.createModule(.{
        .root_source_file = b.path("src/git_source.zig"),
    });
    git_source_module.addImport("ingestion", ingestion_module);
    git_source_module.addImport("vfs", vfs_module);
    git_source_module.addImport("assert", assert_module);
    git_source_module.addImport("concurrency", concurrency_module);

    // Zig parser
    const zig_parser_module = b.createModule(.{
        .root_source_file = b.path("src/zig_parser.zig"),
    });
    zig_parser_module.addImport("ingestion", ingestion_module);
    zig_parser_module.addImport("assert", assert_module);
    zig_parser_module.addImport("concurrency", concurrency_module);

    // Semantic chunker
    const semantic_chunker_module = b.createModule(.{
        .root_source_file = b.path("src/semantic_chunker.zig"),
    });
    semantic_chunker_module.addImport("ingestion", ingestion_module);
    semantic_chunker_module.addImport("context_block", context_block_module);
    semantic_chunker_module.addImport("assert", assert_module);
    semantic_chunker_module.addImport("concurrency", concurrency_module);

    // Debug and testing modules
    const debug_allocator_module = b.createModule(.{
        .root_source_file = b.path("src/debug_allocator.zig"),
    });
    debug_allocator_module.addImport("assert", assert_module);

    const allocator_torture_test_module = b.createModule(.{
        .root_source_file = b.path("src/allocator_torture_test.zig"),
    });
    allocator_torture_test_module.addImport("assert", assert_module);

    const server_module = b.createModule(.{
        .root_source_file = b.path("src/server.zig"),
    });
    server_module.addImport("concurrency", concurrency_module);
    server_module.addImport("storage", storage_module);
    server_module.addImport("query_engine", query_engine_module);
    server_module.addImport("context_block", context_block_module);

    return CoreModules{
        .assert = assert_module,
        .vfs = vfs_module,
        .context_block = context_block_module,
        .error_context = error_context_module,
        .concurrency = concurrency_module,
        .bloom_filter = bloom_filter_module,
        .simulation_vfs = simulation_vfs_module,
        .simulation = simulation_module,
        .sstable = sstable_module,
        .tiered_compaction = tiered_compaction_module,
        .storage = storage_module,
        .query_engine = query_engine_module,
        .ingestion = ingestion_module,
        .git_source = git_source_module,
        .zig_parser = zig_parser_module,
        .semantic_chunker = semantic_chunker_module,
        .debug_allocator = debug_allocator_module,
        .allocator_torture_test = allocator_torture_test_module,
        .server = server_module,
    };
}

fn add_all_imports(module: *std.Build.Module, core_modules: CoreModules) void {
    module.addImport("assert", core_modules.assert);
    module.addImport("vfs", core_modules.vfs);
    module.addImport("context_block", core_modules.context_block);
    module.addImport("error_context", core_modules.error_context);
    module.addImport("concurrency", core_modules.concurrency);
    module.addImport("bloom_filter", core_modules.bloom_filter);
    module.addImport("simulation_vfs", core_modules.simulation_vfs);
    module.addImport("simulation", core_modules.simulation);
    module.addImport("sstable", core_modules.sstable);
    module.addImport("tiered_compaction", core_modules.tiered_compaction);
    module.addImport("storage", core_modules.storage);
    module.addImport("query_engine", core_modules.query_engine);
    module.addImport("ingestion", core_modules.ingestion);
    module.addImport("git_source", core_modules.git_source);
    module.addImport("zig_parser", core_modules.zig_parser);
    module.addImport("semantic_chunker", core_modules.semantic_chunker);
    module.addImport("debug_allocator", core_modules.debug_allocator);
    module.addImport("allocator_torture_test", core_modules.allocator_torture_test);
    module.addImport("server", core_modules.server);
}

fn add_test_imports(module: *std.Build.Module, core_modules: CoreModules) void {
    // Common imports for test files
    module.addImport("assert", core_modules.assert);
    module.addImport("vfs", core_modules.vfs);
    module.addImport("context_block", core_modules.context_block);
    module.addImport("concurrency", core_modules.concurrency);
    module.addImport("bloom_filter", core_modules.bloom_filter);
    module.addImport("simulation", core_modules.simulation);
    module.addImport("simulation_vfs", core_modules.simulation_vfs);
    module.addImport("sstable", core_modules.sstable);
    module.addImport("storage", core_modules.storage);
    module.addImport("query_engine", core_modules.query_engine);
    module.addImport("ingestion", core_modules.ingestion);
    module.addImport("git_source", core_modules.git_source);
    module.addImport("zig_parser", core_modules.zig_parser);
    module.addImport("semantic_chunker", core_modules.semantic_chunker);
    module.addImport("tiered_compaction", core_modules.tiered_compaction);
    module.addImport("debug_allocator", core_modules.debug_allocator);
    module.addImport("allocator_torture_test", core_modules.allocator_torture_test);
    module.addImport("server", core_modules.server);
}

pub fn build(b: *std.Build) void {
    const target = b.standardTargetOptions(.{});
    const optimize_mode = b.standardOptimizeOption(.{});

    // Create all core modules once
    const core_modules = create_core_modules(b);

    // Main executable
    const exe = b.addExecutable(.{
        .name = "cortexdb",
        .root_module = b.createModule(.{
            .root_source_file = b.path("src/main.zig"),
            .target = target,
            .optimize = optimize_mode,
        }),
    });
    add_all_imports(exe.root_module, core_modules);
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
            .source_file = "tests/simulation_test.zig",
            .description = "simulation tests",
        },
        .{
            .name = "storage_simulation",
            .source_file = "tests/storage_simulation_test.zig",
            .description = "storage simulation tests",
        },
        .{
            .name = "wal_recovery",
            .source_file = "tests/wal_recovery_test.zig",
            .description = "WAL recovery tests",
        },
        .{
            .name = "wal_segmentation",
            .source_file = "tests/wal_segmentation_test.zig",
            .description = "WAL segmentation tests",
        },
        .{
            .name = "wal_memory_safety",
            .source_file = "tests/wal_memory_safety_test.zig",
            .description = "WAL memory safety tests",
        },
        .{
            .name = "memory_isolation",
            .source_file = "tests/memory_isolation_test.zig",
            .description = "Memory isolation tests - single test with multiple cycles",
        },
        .{
            .name = "integration",
            .source_file = "tests/integration_test.zig",
            .description = "integration tests",
        },
        .{
            .name = "debug_allocator",
            .source_file = "tests/debug_allocator_test.zig",
            .description = "debug allocator tests",
        },
        .{
            .name = "allocator_torture",
            .source_file = "src/allocator_torture_test.zig",
            .description = "allocator torture tests",
        },
        .{
            .name = "ingestion",
            .source_file = "tests/ingestion_test.zig",
            .description = "ingestion pipeline tests",
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
        add_test_imports(test_exe.root_module, core_modules);
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
            .root_source_file = b.path("src/tidy.zig"),
            .target = target,
            .optimize = optimize_mode,
        }),
    });
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
            .root_source_file = b.path("src/benchmark.zig"),
            .target = target,
            .optimize = .ReleaseFast,
        }),
    });
    add_all_imports(benchmark.root_module, core_modules);

    const install_benchmark = b.addInstallArtifact(benchmark, .{});
    const benchmark_step = b.step("benchmark", "Build and install benchmark");
    benchmark_step.dependOn(&install_benchmark.step);

    const fuzz = b.addExecutable(.{
        .name = "fuzz",
        .root_module = b.createModule(.{
            .root_source_file = b.path("src/fuzz.zig"),
            .target = target,
            .optimize = .ReleaseFast,
        }),
    });
    add_all_imports(fuzz.root_module, core_modules);

    const install_fuzz = b.addInstallArtifact(fuzz, .{});
    const fuzz_step = b.step("fuzz", "Build and install fuzz tester");
    fuzz_step.dependOn(&install_fuzz.step);

    const fuzz_debug = b.addExecutable(.{
        .name = "fuzz-debug",
        .root_module = b.createModule(.{
            .root_source_file = b.path("src/fuzz.zig"),
            .target = target,
            .optimize = .Debug,
        }),
    });
    add_all_imports(fuzz_debug.root_module, core_modules);

    const install_fuzz_debug = b.addInstallArtifact(fuzz_debug, .{});
    const fuzz_debug_step = b.step("fuzz-debug", "Build and install debug fuzz tester with AddressSanitizer");
    fuzz_debug_step.dependOn(&install_fuzz_debug.step);
}
