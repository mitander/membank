//! Standardized Test Harness Framework
//!
//! Provides reusable test setup patterns following KausalDB's architecture principles.
//! Each harness follows the pure coordinator pattern and two-phase initialization.
//! All harnesses use arena allocation for O(1) cleanup and explicit lifecycle management.

const std = @import("std");

const kausaldb = @import("../testing_api.zig");

const production_vfs = @import("../core/production_vfs.zig");

const assert = kausaldb.assert;
const query_engine = kausaldb.query_engine;
const simulation = kausaldb.simulation;
const simulation_vfs = kausaldb.simulation_vfs;
const storage = kausaldb.storage;
const testing = std.testing;
const types = kausaldb.types;

const ProductionVFS = production_vfs.ProductionVFS;
const StorageEngine = storage.StorageEngine;
const NodeId = simulation.NodeId;
const Node = simulation.Node;
const QueryEngine = query_engine.QueryEngine;
const Simulation = simulation.Simulation;
const SimulationVFS = simulation_vfs.SimulationVFS;
const VFS = kausaldb.VFS;
const ContextBlock = types.ContextBlock;
const BlockId = types.BlockId;
const GraphEdge = types.GraphEdge;
const EdgeType = types.EdgeType;

/// Test data utilities following deterministic patterns for reproducible testing
pub const TestData = struct {
    /// Generate deterministic BlockId from seed ensuring non-zero result
    /// All-zero BlockId is invalid per storage engine requirements
    pub fn deterministic_block_id(seed: u32) BlockId {
        var bytes: [16]u8 = undefined;
        // Use seed + 1 to ensure non-zero BlockId
        std.mem.writeInt(u128, &bytes, seed + 1, .little);
        return BlockId.from_bytes(bytes);
    }

    /// Create test block with deterministic content for reproducible testing
    pub fn create_test_block(allocator: std.mem.Allocator, index: u32) !ContextBlock {
        const block_id = deterministic_block_id(index);
        const source_uri = try std.fmt.allocPrint(allocator, "test://block_{}.zig", .{index});
        const metadata_json = try std.fmt.allocPrint(allocator, "{{\"type\":\"test\",\"index\":{}}}", .{index});
        const content = try std.fmt.allocPrint(allocator, "Test block content for index {}", .{index});

        return ContextBlock{
            .id = block_id,
            .version = 1,
            .source_uri = source_uri,
            .metadata_json = metadata_json,
            .content = content,
        };
    }

    /// Create test edge between blocks using provided block IDs
    pub fn create_test_edge(source_id: BlockId, target_id: BlockId, edge_type: EdgeType) GraphEdge {
        return GraphEdge{
            .source_id = source_id,
            .target_id = target_id,
            .edge_type = edge_type,
        };
    }

    /// Create test edge from indices using deterministic ID generation
    /// Convenience function for tests that work with sequential indices
    pub fn create_test_edge_from_indices(source_index: u32, target_index: u32, edge_type: EdgeType) GraphEdge {
        return create_test_edge(deterministic_block_id(source_index), deterministic_block_id(target_index), edge_type);
    }

    /// Create test block with custom content for specific test scenarios
    pub fn create_test_block_with_content(allocator: std.mem.Allocator, index: u32, content: []const u8) !ContextBlock {
        const block_id = deterministic_block_id(index);
        const source_uri = try std.fmt.allocPrint(allocator, "test://block_{}.zig", .{index});
        const metadata_json = try std.fmt.allocPrint(allocator, "{{\"type\":\"test\",\"index\":{}}}", .{index});
        const content_copy = try allocator.dupe(u8, content);

        return ContextBlock{
            .id = block_id,
            .version = 1,
            .source_uri = source_uri,
            .metadata_json = metadata_json,
            .content = content_copy,
        };
    }

    /// Create BlockId from index for use in queries and edge creation
    pub fn create_block_id(index: u32) BlockId {
        return deterministic_block_id(index);
    }

    /// Clean up test block allocated strings
    pub fn cleanup_test_block(allocator: std.mem.Allocator, block: ContextBlock) void {
        allocator.free(block.source_uri);
        allocator.free(block.metadata_json);
        allocator.free(block.content);
    }
};

/// Storage test harness providing standardized setup for storage engine testing
/// Follows two-phase initialization and arena-per-subsystem memory patterns
pub const StorageHarness = struct {
    allocator: std.mem.Allocator,
    sim_vfs: *SimulationVFS,
    vfs_instance: VFS,
    storage_engine: *StorageEngine,

    const Self = @This();

    /// Get VFS interface for components that need direct VFS access
    pub fn vfs(self: *Self) *SimulationVFS {
        return self.sim_vfs;
    }

    /// Get VFS pointer for components that need *VFS parameter
    pub fn vfs_ptr(self: *Self) *VFS {
        return &self.vfs_instance;
    }

    /// Phase 1 initialization: memory allocation only, no I/O operations
    pub fn init(allocator: std.mem.Allocator, db_name: []const u8) !Self {
        // Use backing allocator consistently - components manage their own arenas

        // SimulationVFS manages its own internal arena
        var sim_vfs = try SimulationVFS.heap_init(allocator);

        // Storage engine uses heap allocation to prevent struct copying corruption
        const vfs_instance = sim_vfs.vfs();
        const storage_engine = try allocator.create(StorageEngine);
        storage_engine.* = try StorageEngine.init_default(allocator, vfs_instance, db_name);

        return Self{
            .allocator = allocator,
            .sim_vfs = sim_vfs,
            .vfs_instance = vfs_instance,
            .storage_engine = storage_engine,
        };
    }

    /// Phase 2 initialization: perform all I/O operations to complete startup
    pub fn startup(self: *Self) !void {
        try self.storage_engine.startup();
    }

    /// Clean up all harness resources
    pub fn deinit(self: *Self) void {
        self.storage_engine.deinit();
        self.allocator.destroy(self.storage_engine);
        self.sim_vfs.deinit();
        self.allocator.destroy(self.sim_vfs);
    }

    /// Convenience method combining init and startup phases
    pub fn init_and_startup(allocator: std.mem.Allocator, db_name: []const u8) !Self {
        var harness = try Self.init(allocator, db_name);
        try harness.startup();
        return harness;
    }
};

/// Query test harness extending storage harness with query engine coordination
/// Pure coordinator pattern orchestrating storage and query components
pub const QueryHarness = struct {
    storage_harness: StorageHarness,
    query_engine: *QueryEngine,

    const Self = @This();

    /// Phase 1 initialization: memory allocation only, no I/O operations
    pub fn init(allocator: std.mem.Allocator, db_name: []const u8) !Self {
        var storage_harness = try StorageHarness.init(allocator, db_name);

        // Query engine uses backing allocator for consistency
        const query_engine_ptr = try storage_harness.allocator.create(QueryEngine);
        query_engine_ptr.* = QueryEngine.init(storage_harness.allocator, storage_harness.storage_engine);

        // Disable caching in tests to prevent memory leaks from arena allocator
        query_engine_ptr.caching_enabled = false;

        return Self{
            .storage_harness = storage_harness,
            .query_engine = query_engine_ptr,
        };
    }

    /// Phase 2 initialization: perform I/O operations to complete startup
    pub fn startup(self: *Self) !void {
        try self.storage_harness.startup();
        self.query_engine.startup();
    }

    /// Clean up all harness resources
    pub fn deinit(self: *Self) void {
        self.storage_harness.allocator.destroy(self.query_engine);
        self.storage_harness.deinit();
    }

    /// Convenience method combining init and startup phases
    pub fn init_and_startup(allocator: std.mem.Allocator, db_name: []const u8) !Self {
        var harness = try Self.init(allocator, db_name);
        try harness.startup();
        return harness;
    }

    /// Access storage engine through harness coordinator
    pub fn storage(self: *Self) *StorageEngine {
        return self.storage_harness.storage_engine;
    }

    /// Access simulation VFS for fault injection testing
    pub fn sim_vfs(self: *Self) *SimulationVFS {
        return self.storage_harness.sim_vfs;
    }
};

/// Production harness for performance testing with real filesystem
/// Uses ProductionVFS for minimal overhead and realistic I/O performance
pub const ProductionHarness = struct {
    storage_harness: ProductionStorageHarness,
    query_engine: *QueryEngine,

    const Self = @This();

    /// Phase 1 initialization: memory allocation only, no I/O operations
    pub fn init(allocator: std.mem.Allocator, db_name: []const u8) !Self {
        var storage_harness = try ProductionStorageHarness.init(allocator, db_name);

        // Query engine uses backing allocator for consistency
        const query_engine_ptr = try storage_harness.allocator.create(QueryEngine);
        query_engine_ptr.* = QueryEngine.init(storage_harness.allocator, storage_harness.storage_engine);

        // Disable caching in tests to prevent memory leaks from arena allocator
        query_engine_ptr.enable_caching(false);

        return Self{
            .storage_harness = storage_harness,
            .query_engine = query_engine_ptr,
        };
    }

    /// Phase 2 startup: hot path initialization with I/O operations
    pub fn startup(self: *Self) !void {
        try self.storage_harness.startup();
    }

    /// Convenience method combining init and startup phases
    pub fn init_and_startup(allocator: std.mem.Allocator, db_name: []const u8) !Self {
        var harness = try Self.init(allocator, db_name);
        try harness.startup();
        return harness;
    }

    /// Access storage engine through harness coordinator
    pub fn storage(self: *Self) *StorageEngine {
        return self.storage_harness.storage_engine;
    }

    pub fn deinit(self: *Self) void {
        self.storage_harness.allocator.destroy(self.query_engine);
        self.storage_harness.deinit();
    }
};

/// Production storage harness for performance testing with real filesystem
pub const ProductionStorageHarness = struct {
    allocator: std.mem.Allocator,
    prod_vfs: *ProductionVFS,
    vfs_instance: VFS,
    storage_engine: *StorageEngine,
    db_path: []const u8, // Store database path for cleanup

    const Self = @This();

    /// Phase 1 initialization: memory allocation only, no I/O operations
    pub fn init(allocator: std.mem.Allocator, db_name: []const u8) !Self {
        // Use backing allocator consistently - components manage their own arenas

        // ProductionVFS manages its own internal arena
        var prod_vfs = try allocator.create(ProductionVFS);
        prod_vfs.* = ProductionVFS.init(allocator);

        // Storage engine uses heap allocation to prevent struct copying corruption
        const vfs_instance = prod_vfs.vfs();
        const storage_engine = try allocator.create(StorageEngine);
        storage_engine.* = try StorageEngine.init_default(allocator, vfs_instance, db_name);

        // Store database path for cleanup
        const db_path = try allocator.dupe(u8, db_name);

        return Self{
            .allocator = allocator,
            .prod_vfs = prod_vfs,
            .vfs_instance = vfs_instance,
            .storage_engine = storage_engine,
            .db_path = db_path,
        };
    }

    /// Phase 2 startup: hot path initialization with I/O operations
    pub fn startup(self: *Self) !void {
        try self.storage_engine.startup();
    }

    /// Convenience method combining init and startup phases
    pub fn init_and_startup(allocator: std.mem.Allocator, db_name: []const u8) !Self {
        var harness = try Self.init(allocator, db_name);
        try harness.startup();
        return harness;
    }

    pub fn deinit(self: *Self) void {
        // Shutdown storage engine first
        self.storage_engine.deinit();
        self.allocator.destroy(self.storage_engine);

        // Clean up VFS
        self.prod_vfs.deinit();
        self.allocator.destroy(self.prod_vfs);

        // Clean up database directory to prevent filesystem pollution
        const cwd = std.fs.cwd();
        cwd.deleteTree(self.db_path) catch |err| {
            // Don't fail the test if directory cleanup fails, just log warning
            std.log.warn("Failed to clean up test database directory '{s}': {}", .{ self.db_path, err });
        };

        // Free the database path string
        self.allocator.free(self.db_path);
    }
};

/// Simulation harness for integration testing with deterministic time control
/// Coordinates simulation, storage, and query components
pub const SimulationHarness = struct {
    allocator: std.mem.Allocator,
    simulation: *Simulation,
    storage_engine: *StorageEngine,
    query_engine: *QueryEngine,
    node_id: NodeId,
    seed: u64,
    vfs_instance: VFS,

    const Self = @This();

    /// Phase 1 initialization with deterministic seed for reproducible behavior
    pub fn init(allocator: std.mem.Allocator, seed: u64, db_name: []const u8) !Self {
        // Use backing allocator consistently for all components

        // Deterministic simulation ensures reproducible test behavior
        var simulation_ptr = try allocator.create(Simulation);
        simulation_ptr.* = try Simulation.init(allocator, seed);

        // Single simulation node hosts storage engine
        const node_id = try simulation_ptr.add_node();

        // Stabilization period ensures consistent simulation state
        simulation_ptr.tick_multiple(5);

        const node_ptr = simulation_ptr.find_node(node_id);
        const node_vfs = node_ptr.filesystem_interface();

        // Storage engine uses simulation VFS for deterministic I/O
        const storage_engine = try allocator.create(StorageEngine);
        storage_engine.* = try StorageEngine.init_default(allocator, node_vfs, db_name);

        // Query engine coordinates with storage engine
        const query_engine_ptr = try allocator.create(QueryEngine);
        query_engine_ptr.* = QueryEngine.init(allocator, storage_engine);

        return Self{
            .allocator = allocator,
            .simulation = simulation_ptr,
            .storage_engine = storage_engine,
            .query_engine = query_engine_ptr,
            .node_id = node_id,
            .seed = seed,
            .vfs_instance = node_vfs,
        };
    }

    /// Phase 2 initialization: perform I/O operations to complete startup
    pub fn startup(self: *Self) !void {
        try self.storage_engine.startup();
        self.query_engine.startup();
    }

    /// Clean up all harness resources
    pub fn deinit(self: *Self) void {
        self.query_engine.deinit();
        self.allocator.destroy(self.query_engine);
        self.storage_engine.deinit();
        self.allocator.destroy(self.storage_engine);
        self.simulation.deinit();
        self.allocator.destroy(self.simulation);
    }

    /// Convenience method combining init and startup phases
    pub fn init_and_startup(allocator: std.mem.Allocator, seed: u64, db_name: []const u8) !Self {
        var harness = try Self.init(allocator, seed, db_name);
        try harness.startup();
        return harness;
    }

    /// Advance simulation time by specified tick count
    pub fn tick(self: *Self, count: u32) void {
        self.simulation.tick_multiple(count);
    }

    /// Access simulation node for network and filesystem operations
    pub fn node(self: *Self) *Node {
        return self.simulation.find_node(self.node_id);
    }
};

/// Fault injection configuration for systematic hostile condition testing
pub const FaultInjectionConfig = struct {
    /// I/O operation failure configuration
    io_failures: struct {
        enabled: bool = false,
        failure_rate_per_thousand: u32 = 100, // 10% default failure rate
        operations: struct {
            read: bool = false,
            write: bool = false,
            create: bool = false,
            remove: bool = false,
            mkdir: bool = false,
            sync: bool = false,
        } = .{},
    } = .{},

    /// Torn write configuration simulating power loss during writes
    torn_writes: struct {
        enabled: bool = false,
        probability_per_thousand: u32 = 500, // 50% default probability
        completion_threshold_percent: u8 = 70, // 70% completion before interruption
        min_interruption_bytes: u32 = 1,
    } = .{},

    /// Data corruption injection configuration
    data_corruption: struct {
        enabled: bool = false,
        corruption_rate_per_million: u32 = 100, // 0.01% default corruption rate
        patterns: struct {
            bit_flip: bool = true,
            zero_bytes: bool = false,
            random_bytes: bool = false,
        } = .{},
    } = .{},
};

/// Fault injection harness extending simulation harness with systematic fault injection
/// Provides hostile condition testing capabilities
pub const FaultInjectionHarness = struct {
    simulation_harness: SimulationHarness,
    fault_config: FaultInjectionConfig,
    seed: u64,

    const Self = @This();

    /// Initialize harness with fault injection configuration
    pub fn init_with_faults(
        allocator: std.mem.Allocator,
        seed: u64,
        db_name: []const u8,
        fault_config: FaultInjectionConfig,
    ) !Self {
        const simulation_harness = try SimulationHarness.init(allocator, seed, db_name);
        return Self{
            .simulation_harness = simulation_harness,
            .fault_config = fault_config,
            .seed = seed,
        };
    }

    pub fn startup(self: *Self) !void {
        try self.simulation_harness.startup();
        try self.apply_fault_configuration();
    }

    pub fn deinit(self: *Self) void {
        self.simulation_harness.deinit();
    }

    /// Convenience method combining init_with_faults and startup phases
    pub fn init_and_startup(
        allocator: std.mem.Allocator,
        seed: u64,
        db_name: []const u8,
    ) !Self {
        const default_fault_config = FaultInjectionConfig{};
        var harness = try Self.init_with_faults(allocator, seed, db_name, default_fault_config);
        try harness.startup();
        return harness;
    }

    /// Configure fault injection parameters in simulation VFS
    pub fn apply_fault_configuration(self: *Self) !void {
        const node = self.simulation_harness.node();
        const node_vfs = node.filesystem;

        // I/O failure configuration based on fault settings
        if (self.fault_config.io_failures.enabled) {
            const operations = SimulationVFS.FaultInjectionState.IoFailureConfig.OperationType{
                .read = self.fault_config.io_failures.operations.read,
                .write = self.fault_config.io_failures.operations.write,
                .create = self.fault_config.io_failures.operations.create,
                .remove = self.fault_config.io_failures.operations.remove,
                .mkdir = self.fault_config.io_failures.operations.mkdir,
                .sync = self.fault_config.io_failures.operations.sync,
            };
            node_vfs.enable_io_failures(
                self.fault_config.io_failures.failure_rate_per_thousand,
                operations,
            );
        }

        // Torn write configuration for power loss simulation
        if (self.fault_config.torn_writes.enabled) {
            node_vfs.enable_torn_writes(
                self.fault_config.torn_writes.probability_per_thousand,
                self.fault_config.torn_writes.min_interruption_bytes,
                self.fault_config.torn_writes.completion_threshold_percent,
            );
        }

        // Additional fault types configured based on requirements
    }

    /// Access storage engine through harness
    pub fn storage(self: *Self) *StorageEngine {
        return self.simulation_harness.storage_engine;
    }

    /// Access query engine through harness
    pub fn query_engine(self: *Self) *QueryEngine {
        return self.simulation_harness.query_engine;
    }

    /// Advance simulation time by specified tick count
    pub fn tick(self: *Self, count: u32) void {
        self.simulation_harness.tick(count);
    }

    /// Disable all fault injection to enable clean recovery testing
    pub fn disable_all_faults(self: *Self) void {
        const node = self.simulation_harness.node();
        const node_vfs = node.filesystem;
        node_vfs.disable_all_fault_injection();
    }

    /// Disable torn writes while keeping other fault types active
    pub fn disable_torn_writes(self: *Self) void {
        const node = self.simulation_harness.node();
        const node_vfs = node.filesystem;
        node_vfs.disable_torn_writes();
    }

    /// Access VFS for pipeline initialization compatibility
    pub fn vfs_ptr(self: *Self) *VFS {
        return &self.simulation_harness.vfs_instance;
    }

    /// Access simulation VFS for direct fault injection control
    pub fn sim_vfs(self: *Self) *SimulationVFS {
        const node = self.simulation_harness.node();
        return node.filesystem;
    }

    /// Enable I/O failures with specified rate and operation types
    pub fn enable_io_failures(
        self: *Self,
        rate_per_thousand: u32,
        operations: struct {
            read: bool = false,
            write: bool = false,
            create: bool = false,
            remove: bool = false,
            mkdir: bool = false,
            sync: bool = false,
        },
    ) void {
        const node = self.simulation_harness.node();
        const node_vfs = node.filesystem;

        const vfs_operations = SimulationVFS.FaultInjectionState.IoFailureConfig.OperationType{
            .read = operations.read,
            .write = operations.write,
            .create = operations.create,
            .remove = operations.remove,
            .mkdir = operations.mkdir,
            .sync = operations.sync,
        };

        node_vfs.enable_io_failures(rate_per_thousand, vfs_operations);
    }
};
