//! Scenario-driven fault injection testing framework
//!
//! Provides systematic fault injection following explicitness-over-magic philosophy.
//! All scenario parameters declared upfront for reproducible hostile condition testing.

const std = @import("std");
const testing = std.testing;

// Import from kausaldb_test for consistency
const kausaldb = @import("../kausaldb_test.zig");

const storage = kausaldb.storage;
const simulation_vfs = kausaldb.simulation_vfs;
const types = kausaldb.types;
const golden_master = kausaldb.golden_master;
const TestData = kausaldb.TestData;

const StorageEngine = storage.StorageEngine;
const SimulationVFS = simulation_vfs.SimulationVFS;
const ContextBlock = types.ContextBlock;
const BlockId = types.BlockId;

/// Fault injection scenario configuration with explicit parameters for reproducibility
pub const FaultScenario = struct {
    /// Scenario description for test output and debugging
    description: []const u8,
    /// Deterministic seed ensuring reproducible fault injection
    seed: u64,
    /// Fault type determining injection strategy
    fault_type: FaultType,
    /// Initial data population before fault injection begins
    initial_blocks: u32,
    /// Operations attempted under hostile conditions
    fault_operations: u32,
    /// Expected recovery outcome for validation
    expected_recovery_success: bool,
    /// Minimum data survival rate threshold for validation
    expected_min_survival_rate: f32,
    /// Optional golden master for post-recovery state verification
    golden_master_file: ?[]const u8,

    pub const FaultType = enum {
        /// Write operation failures
        write_failures,
        /// Flush and sync operation failures
        sync_failures,
        /// File removal and cleanup failures
        cleanup_failures,
        /// Power loss simulation via incomplete writes
        torn_writes,
        /// Multiple sequential fault types for compound failures
        sequential_faults,
        /// Storage capacity exhaustion
        disk_space_exhaustion,
        /// Post-write data corruption
        read_corruption,
    };
};

/// WAL durability scenario types for explicit scenario selection
pub const WalDurabilityScenario = enum {
    io_flush_failures,
    disk_space_exhaustion,
    torn_writes,
    sequential_faults,

    /// Get scenario configuration for this fault type
    pub fn config(self: @This()) FaultScenario {
        return switch (self) {
            .io_flush_failures => .{
                .description = "I/O failure during flush operations",
                .seed = 0xDEAD1111,
                .fault_type = .sync_failures,
                .initial_blocks = 100,
                .fault_operations = 50,
                .expected_recovery_success = true,
                .expected_min_survival_rate = 0.8,
                .golden_master_file = null,
            },
            .disk_space_exhaustion => .{
                .description = "Disk space exhaustion handling",
                .seed = 0xBEEF2222,
                .fault_type = .disk_space_exhaustion,
                .initial_blocks = 150,
                .fault_operations = 75,
                .expected_recovery_success = true,
                .expected_min_survival_rate = 0.9,
                .golden_master_file = null,
            },
            .torn_writes => .{
                .description = "Torn writes during WAL entry serialization",
                .seed = 0xCAFE3333,
                .fault_type = .torn_writes,
                .initial_blocks = 75,
                .fault_operations = 25,
                .expected_recovery_success = true,
                .expected_min_survival_rate = 0.7,
                .golden_master_file = null,
            },
            .sequential_faults => .{
                .description = "Multiple sequential crashes with different fault types",
                .seed = 0xFEED4444,
                .fault_type = .sequential_faults,
                .initial_blocks = 200,
                .fault_operations = 100,
                .expected_recovery_success = true,
                .expected_min_survival_rate = 0.6,
                .golden_master_file = null,
            },
        };
    }
};

/// Compaction crash scenario types for explicit scenario selection
pub const CompactionCrashScenario = enum {
    partial_sstable_write,
    orphaned_files,
    torn_write_header,
    sequential_crashes,

    /// Get scenario configuration for this fault type
    pub fn config(self: @This()) FaultScenario {
        return switch (self) {
            .partial_sstable_write => .{
                .description = "Partial SSTable write during compaction",
                .seed = 0xDEADBEEF,
                .fault_type = .write_failures,
                .initial_blocks = 150,
                .fault_operations = 50,
                .expected_recovery_success = true,
                .expected_min_survival_rate = 0.8,
                .golden_master_file = "partial_sstable_write_recovery.golden.json",
            },
            .orphaned_files => .{
                .description = "Orphaned files after compaction failure",
                .seed = 0xCAFEBABE,
                .fault_type = .cleanup_failures,
                .initial_blocks = 200,
                .fault_operations = 30,
                .expected_recovery_success = true,
                .expected_min_survival_rate = 0.9,
                .golden_master_file = null,
            },
            .torn_write_header => .{
                .description = "Torn write in SSTable header",
                .seed = 0xBEEFCAFE,
                .fault_type = .torn_writes,
                .initial_blocks = 100,
                .fault_operations = 20,
                .expected_recovery_success = true,
                .expected_min_survival_rate = 0.7,
                .golden_master_file = null,
            },
            .sequential_crashes => .{
                .description = "Multiple sequential crashes",
                .seed = 0xFEEDFACE,
                .fault_type = .sequential_faults,
                .initial_blocks = 180,
                .fault_operations = 40,
                .expected_recovery_success = true,
                .expected_min_survival_rate = 0.6,
                .golden_master_file = null,
            },
        };
    }
};

/// Executes fault scenarios with validation and recovery testing
pub const ScenarioExecutor = struct {
    allocator: std.mem.Allocator,
    scenario: FaultScenario,

    const Self = @This();

    pub fn init(allocator: std.mem.Allocator, scenario: FaultScenario) Self {
        return Self{
            .allocator = allocator,
            .scenario = scenario,
        };
    }

    /// Run fault scenario with systematic validation and recovery testing
    pub fn run(self: Self) !void {
        std.debug.print("Executing scenario: {s}\n", .{self.scenario.description});

        // Deterministic VFS ensures reproducible fault behavior
        var sim_vfs = try SimulationVFS.init_with_fault_seed(self.allocator, self.scenario.seed);
        defer sim_vfs.deinit();

        // Unique directory prevents test interference
        const dir_name = try std.fmt.allocPrint(self.allocator, "scenario_{x}", .{
            @as(u64, @intCast(@intFromPtr(self.scenario.description.ptr))) ^ self.scenario.seed,
        });
        defer self.allocator.free(dir_name);

        var storage_engine = try StorageEngine.init_default(self.allocator, sim_vfs.vfs(), dir_name);
        try storage_engine.startup();

        // Baseline data population establishes pre-fault state
        try self.populate_initial_data(&storage_engine);
        const initial_block_count = storage_engine.block_count();

        // Optional golden master captures expected state before faults
        if (self.scenario.golden_master_file) |golden_file| {
            // This would record the state before fault injection
            // Commented out as it would only be run once to generate the golden file
            // try golden_master.record_vfs_state(&sim_vfs, golden_file);
            _ = golden_file;
        }

        // Fault injection configuration matches scenario specification
        try self.configure_fault_injection(&sim_vfs);

        // Operations under hostile conditions test system resilience
        try self.execute_fault_operations(&storage_engine);

        // Clean shutdown simulates system crash for recovery testing
        storage_engine.deinit();

        // Fresh engine instance validates recovery from persisted state
        var recovered_engine = try StorageEngine.init_default(self.allocator, sim_vfs.vfs(), dir_name);
        defer recovered_engine.deinit();

        // Clean environment required for accurate recovery validation
        self.disable_fault_injection(&sim_vfs);

        // Recovery attempt determines system resilience
        const recovery_result = recovered_engine.startup();

        // Recovery validation against scenario success criteria
        if (self.scenario.expected_recovery_success) {
            try recovery_result;

            const recovered_block_count = recovered_engine.block_count();
            const survival_rate = @as(f32, @floatFromInt(recovered_block_count)) /
                @as(f32, @floatFromInt(initial_block_count));

            std.debug.print("Recovery stats: {d}/{d} blocks survived ({:.1}% survival rate)\n", .{
                recovered_block_count,
                initial_block_count,
                survival_rate * 100.0,
            });

            // Verify survival rate meets expectations
            if (survival_rate < self.scenario.expected_min_survival_rate) {
                std.debug.print("FAILURE: Survival rate {d:.1}% below expected {d:.1}%\n", .{
                    survival_rate * 100.0,
                    self.scenario.expected_min_survival_rate * 100.0,
                });
                return error.InsufficientDataSurvival;
            }

            // Post-recovery functionality validation ensures complete restoration
            try self.verify_post_recovery_functionality(&recovered_engine);

            // Golden master validation ensures exact state recovery
            if (self.scenario.golden_master_file) |golden_file| {
                try golden_master.verify_recovery_golden_master(
                    self.allocator,
                    golden_file,
                    &recovered_engine,
                );
            }
        } else {
            // Graceful failure scenarios validate error handling paths
            try testing.expectError(error.CorruptionDetected, recovery_result);
        }

        std.debug.print("Scenario completed successfully: {s}\n", .{self.scenario.description});
    }

    /// Create baseline data set for fault injection testing
    fn populate_initial_data(self: Self, storage_engine: *StorageEngine) !void {
        for (0..self.scenario.initial_blocks) |i| {
            const block = ContextBlock{
                .id = TestData.deterministic_block_id(@intCast(i)),
                .version = 1,
                .source_uri = "test://fault_injection_block.zig",
                .metadata_json = "{\"fault_injection_test\":true}",
                .content = "pub fn fault_injection_test_block() void { return; }",
            };
            try storage_engine.put_block(block);
        }
    }

    /// Apply scenario-specific fault configuration to simulation VFS
    fn configure_fault_injection(self: Self, sim_vfs: *SimulationVFS) !void {
        switch (self.scenario.fault_type) {
            .write_failures => {
                sim_vfs.enable_io_failures(500, .{ .write = true }); // 50% write failure rate
            },
            .sync_failures => {
                sim_vfs.enable_io_failures(300, .{ .sync = true }); // 30% sync failure rate
            },
            .cleanup_failures => {
                sim_vfs.enable_io_failures(400, .{ .remove = true }); // 40% remove failure rate
            },
            .torn_writes => {
                sim_vfs.enable_torn_writes(800, 1, 70); // 80% probability, 70% completion max
            },
            .sequential_faults => {
                // Multiple fault types simulate complex real-world failures
                sim_vfs.enable_io_failures(300, .{ .write = true, .sync = true });
                sim_vfs.enable_torn_writes(200, 1, 50); // Lower probability for combined faults
            },
            .disk_space_exhaustion => {
                sim_vfs.configure_disk_space_limit(1024 * 1024); // 1MB limit to trigger exhaustion
            },
            .read_corruption => {
                sim_vfs.enable_read_corruption(100, 1); // 0.1% corruption rate, 1 bit per corruption
            },
        }
    }

    /// Perform operations under hostile conditions to test resilience
    fn execute_fault_operations(self: Self, storage_engine: *StorageEngine) !void {
        var successful_operations: u32 = 0;

        for (0..self.scenario.fault_operations) |i| {
            const operation_index = self.scenario.initial_blocks + @as(u32, @intCast(i));
            const block = ContextBlock{
                .id = TestData.deterministic_block_id(operation_index),
                .version = 1,
                .source_uri = "test://fault_operation.zig",
                .metadata_json = "{\"fault_operation\":true}",
                .content = "Fault operation test block content",
            };

            // Expected failures under fault injection validate error handling
            if (storage_engine.put_block(block)) |_| {
                successful_operations += 1;
            } else |err| switch (err) {
                error.IoError, error.NoSpaceLeft => {
                    // These errors indicate proper fault injection behavior
                },
                else => return err, // Unexpected errors indicate test failures
            }
        }

        std.debug.print("Fault phase: {d}/{d} operations succeeded under hostile conditions\n", .{
            successful_operations,
            self.scenario.fault_operations,
        });
    }

    /// Clear fault injection state for accurate recovery validation
    fn disable_fault_injection(self: Self, sim_vfs: *SimulationVFS) void {
        _ = self;
        sim_vfs.fault_injection.io_failure_config.enabled = false;
        sim_vfs.fault_injection.torn_write_config.enabled = false;
        sim_vfs.fault_injection.read_corruption_config.enabled = false;
        // Restore normal disk capacity for recovery operations
        sim_vfs.fault_injection.max_disk_space = std.math.maxInt(u64);
    }

    /// Validate complete functionality restoration after recovery
    fn verify_post_recovery_functionality(self: Self, storage_engine: *StorageEngine) !void {
        // New operations validate complete recovery and functionality
        const recovery_test_index = self.scenario.initial_blocks + self.scenario.fault_operations + 1000;
        const recovery_block = ContextBlock{
            .id = TestData.deterministic_block_id(recovery_test_index),
            .version = 1,
            .source_uri = "test://recovery_validation.zig",
            .metadata_json = "{\"test\":\"recovery_validation\"}",
            .content = "Recovery validation test block content",
        };

        try storage_engine.put_block(recovery_block);
        const retrieved = (try storage_engine.find_block(recovery_block.id, .storage_engine)).?;
        try testing.expect(retrieved.extract().id.eql(recovery_block.id));
    }
};

/// Run predefined scenario sets for systematic testing
pub fn run_wal_durability_scenario(allocator: std.mem.Allocator, scenario: WalDurabilityScenario) !void {
    const executor = ScenarioExecutor.init(allocator, scenario.config());
    try executor.run();
}

pub fn run_compaction_crash_scenario(allocator: std.mem.Allocator, scenario: CompactionCrashScenario) !void {
    const executor = ScenarioExecutor.init(allocator, scenario.config());
    try executor.run();
}

/// Batch execution of related scenarios for comprehensive validation
pub fn run_all_wal_scenarios(allocator: std.mem.Allocator) !void {
    const scenario_count = @typeInfo(WalDurabilityScenario).@"enum".fields.len;
    std.debug.print("Running all WAL durability scenarios ({d} total)...\n", .{scenario_count});
    inline for (@typeInfo(WalDurabilityScenario).@"enum".fields) |field| {
        const scenario = @as(WalDurabilityScenario, @enumFromInt(field.value));
        try run_wal_durability_scenario(allocator, scenario);
    }
}

pub fn run_all_compaction_scenarios(allocator: std.mem.Allocator) !void {
    const scenario_count = @typeInfo(CompactionCrashScenario).@"enum".fields.len;
    std.debug.print("Running all compaction crash scenarios ({d} total)...\n", .{scenario_count});
    inline for (@typeInfo(CompactionCrashScenario).@"enum".fields) |field| {
        const scenario = @as(CompactionCrashScenario, @enumFromInt(field.value));
        try run_compaction_crash_scenario(allocator, scenario);
    }
}
