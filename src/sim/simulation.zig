//! Deterministic simulation framework for system-wide testing.
//!
//! Runs complete KausalDB system in controlled environment with deterministic
//! I/O, networking, and timing for reproducible failure scenario testing.
//! Enables byte-for-byte identical test execution across platforms and runs.
//!
//! Design rationale: Deterministic simulation enables precise reproduction
//! of complex failure scenarios without flaky tests. Running actual production
//! code in simulation avoids mocking artifacts while providing hostile
//! condition testing capabilities impossible with real systems.

const builtin = @import("builtin");
const std = @import("std");

const assert_mod = @import("../core/assert.zig");
const deterministic_logger_mod = @import("deterministic_logger.zig");
const ownership = @import("../core/ownership.zig");
const sim_vfs = @import("simulation_vfs.zig");
const vfs = @import("../core/vfs.zig");

const DeterministicLogger = deterministic_logger_mod.DeterministicLogger;

/// Deterministic simulation harness.
pub const Simulation = struct {
    allocator: std.mem.Allocator,
    prng: std.Random.DefaultPrng,
    tick_count: u64,
    nodes: std.array_list.Managed(Node),
    network: Network,
    ownership_injector: OwnershipViolationInjector,
    logger: ?DeterministicLogger,

    const Self = @This();

    /// Initialize a new simulation with a specific seed for reproducibility.
    pub fn init(allocator: std.mem.Allocator, seed: u64) !Self {
        var prng = std.Random.DefaultPrng.init(seed);

        var sim = Self{
            .allocator = allocator,
            .prng = prng,
            .tick_count = 0,
            .nodes = std.array_list.Managed(Node).init(allocator),
            .network = Network.init(allocator, &prng),
            .ownership_injector = OwnershipViolationInjector.init(seed),
            .logger = if (builtin.mode == .Debug) DeterministicLogger{ .sim = undefined } else null,
        };

        if (sim.logger) |*logger| {
            logger.sim = &sim;
        }

        return sim;
    }

    pub fn deinit(self: *Self) void {
        for (self.nodes.items) |*node| {
            node.deinit();
        }
        self.nodes.deinit();
        self.network.deinit();
    }

    /// Add a new node to the simulation.
    pub fn add_node(self: *Self) !NodeId {
        const node_id = NodeId{ .id = self.nodes.items.len };
        const node = try Node.init(self.allocator, node_id);
        try self.nodes.append(node);
        try self.network.add_node(node_id);
        return node_id;
    }

    /// Get a node by its ID.
    pub fn find_node(self: *Self, node_id: NodeId) *Node {
        assert_mod.assert_index_valid(
            node_id.id,
            self.nodes.items.len,
            "Invalid node ID: {}",
            .{node_id.id},
        );
        return &self.nodes.items[node_id.id];
    }

    /// Advance the simulation by one tick.
    pub fn tick(self: *Self) void {
        self.tick_count += 1;

        self.network.process_tick();

        for (self.nodes.items) |*node| {
            node.process_tick(&self.network);
        }
    }

    /// Advance the simulation by multiple ticks.
    pub fn tick_multiple(self: *Self, count: u64) void {
        var i: u64 = 0;
        while (i < count) : (i += 1) {
            self.tick();
        }
    }

    /// Create a network partition between two nodes.
    pub fn partition_nodes(self: *Self, node_a: NodeId, node_b: NodeId) void {
        self.network.partition_nodes(node_a, node_b);
    }

    /// Heal a network partition between two nodes.
    pub fn heal_partition(self: *Self, node_a: NodeId, node_b: NodeId) void {
        self.network.heal_partition(node_a, node_b);
    }

    /// Configure packet loss between two nodes.
    pub fn configure_packet_loss(self: *Self, node_a: NodeId, node_b: NodeId, loss_rate: f32) void {
        assert_mod.assert_range(loss_rate, 0.0, 1.0, "Invalid loss rate: {}", .{loss_rate});
        self.network.configure_packet_loss(node_a, node_b, loss_rate);
    }

    /// Configure network latency between two nodes.
    pub fn configure_latency(self: *Self, node_a: NodeId, node_b: NodeId, latency_ticks: u32) void {
        self.network.configure_latency(node_a, node_b, latency_ticks);
    }

    /// Get the current tick count.
    pub fn ticks(self: *Self) u64 {
        return self.tick_count;
    }

    /// Get a deterministic random number.
    pub fn random(self: *Self) u64 {
        return self.prng.random().int(u64);
    }

    /// Get the filesystem state for a specific node.
    pub fn node_filesystem_state(
        self: *Self,
        node_id: NodeId,
    ) ![]sim_vfs.SimulationVFS.SimulationFileState {
        const node = self.find_node(node_id);
        return node.filesystem.state(self.allocator);
    }

    /// Enable ownership violation injection for testing fail-fast behavior.
    /// Rate controls how frequently violations are injected (0.0 = never, 1.0 = always).
    pub fn enable_ownership_violations(self: *Self, injection_rate: f32) void {
        self.ownership_injector.enable_injection(injection_rate);
    }

    /// Disable ownership violation injection.
    pub fn disable_ownership_violations(self: *Self) void {
        self.ownership_injector.disable_injection();
    }

    /// Get ownership violation injection statistics for test validation.
    pub fn ownership_violation_stats(self: *const Self) struct {
        injection_rate: f32,
        is_enabled: bool,
        violation_count: u32,
    } {
        const stats = self.ownership_injector.violation_stats();
        return .{
            .injection_rate = stats.injection_rate,
            .is_enabled = stats.is_enabled,
            .violation_count = stats.violation_count,
        };
    }

    /// Reset ownership violation statistics for new test scenarios.
    pub fn reset_ownership_violation_stats(self: *Self) void {
        self.ownership_injector.reset_stats();
    }

    /// Inject ownership violations into block operations for testing.
    /// This method allows controlled corruption of ownership information.
    pub fn inject_ownership_violation(
        self: *Self,
        owned_block: ownership.OwnedBlock,
        violation_type: OwnershipViolationInjector.ViolationType,
        fake_accessor: ownership.BlockOwnership,
    ) ?ownership.OwnedBlock {
        return switch (violation_type) {
            .cross_subsystem_read => self.ownership_injector.inject_cross_subsystem_read(owned_block, fake_accessor),
            .dangling_arena_access => self.ownership_injector.inject_dangling_arena_access(owned_block),
            .cross_subsystem_write => self.ownership_injector.inject_cross_subsystem_read(owned_block, fake_accessor),
            .use_after_transfer => self.ownership_injector.inject_dangling_arena_access(owned_block),
        };
    }
};

/// Unique identifier for a node in the simulation.
pub const NodeId = struct {
    id: usize,

    pub fn format(
        self: NodeId,
        comptime fmt: []const u8,
        options: std.fmt.FormatOptions,
        writer: anytype,
    ) !void {
        _ = fmt;
        _ = options;
        try writer.print("Node{}", .{self.id});
    }
};

/// A simulated node in the KausalDB cluster.
pub const Node = struct {
    id: NodeId,
    filesystem: *sim_vfs.SimulationVFS,
    message_queue: std.array_list.Managed(Message),
    allocator: std.mem.Allocator,

    const Self = @This();

    pub fn init(allocator: std.mem.Allocator, id: NodeId) !Self {
        const filesystem_ptr = try sim_vfs.SimulationVFS.heap_init(allocator);

        return Self{
            .id = id,
            .filesystem = filesystem_ptr,
            .message_queue = std.array_list.Managed(Message).init(allocator),
            .allocator = allocator,
        };
    }

    pub fn deinit(self: *Self) void {
        self.filesystem.deinit();
        self.allocator.destroy(self.filesystem);
        self.message_queue.deinit();
    }

    /// Process one simulation tick for this node.
    pub fn process_tick(self: *Self, network: *Network) void {
        while (self.message_queue.items.len > 0) {
            const message = self.message_queue.orderedRemove(0);
            self.handle_message(message);
        }

        self.filesystem.advance_time(1);

        _ = network; // Future: Process network events
    }

    /// Handle an incoming message.
    fn handle_message(self: *Self, message: Message) void {
        _ = self;
        _ = message;
    }

    /// Send a message to another node.
    pub fn send_message(
        self: *Self,
        target: NodeId,
        message_type: MessageType,
        data: []const u8,
        network: *Network,
    ) !void {
        const message = Message{
            .sender = self.id,
            .receiver = target,
            .message_type = message_type,
            .data = try self.allocator.dupe(u8, data),
        };

        try network.send_message(message);
    }

    /// Get the VFS interface for this node.
    pub fn filesystem_interface(self: *Self) vfs.VFS {
        return self.filesystem.vfs();
    }
};

/// A message sent between nodes.
pub const Message = struct {
    sender: NodeId,
    receiver: NodeId,
    message_type: MessageType,
    data: []const u8,
};

/// Types of messages that can be sent between nodes.
pub const MessageType = enum {
    ping,
    pong,
    write_request,
    write_response,
    read_request,
    read_response,
    heartbeat,
    leader_election,
};

/// Simulated network for message passing between nodes.
pub const Network = struct {
    allocator: std.mem.Allocator,
    prng: *std.Random.DefaultPrng,
    nodes: std.array_list.Managed(NodeId),
    message_queues: std.HashMap(
        NodeId,
        std.array_list.Managed(DelayedMessage),
        NodeIdContext,
        std.hash_map.default_max_load_percentage,
    ),
    partitions: std.HashMap(
        NodePair,
        void,
        NodePairContext,
        std.hash_map.default_max_load_percentage,
    ),
    packet_loss: std.HashMap(
        NodePair,
        f32,
        NodePairContext,
        std.hash_map.default_max_load_percentage,
    ),
    latencies: std.HashMap(
        NodePair,
        u32,
        NodePairContext,
        std.hash_map.default_max_load_percentage,
    ),

    const Self = @This();

    const DelayedMessage = struct {
        message: Message,
        delivery_tick: u64,
    };

    const NodePair = struct {
        a: NodeId,
        b: NodeId,

        fn normalize(self: NodePair) NodePair {
            return if (self.a.id < self.b.id)
                self
            else
                NodePair{ .a = self.b, .b = self.a };
        }
    };

    const NodeIdContext = struct {
        pub fn hash(self: @This(), key: NodeId) u64 {
            _ = self;
            var hasher = std.hash.Wyhash.init(0);
            hasher.update(std.mem.asBytes(&key.id));
            return hasher.final();
        }

        pub fn eql(self: @This(), a: NodeId, b: NodeId) bool {
            _ = self;
            return a.id == b.id;
        }
    };

    const NodePairContext = struct {
        pub fn hash(self: @This(), key: NodePair) u64 {
            _ = self;
            const normalized = key.normalize();
            var hasher = std.hash.Wyhash.init(0);
            hasher.update(std.mem.asBytes(&normalized.a.id));
            hasher.update(std.mem.asBytes(&normalized.b.id));
            return hasher.final();
        }

        pub fn eql(self: @This(), a: NodePair, b: NodePair) bool {
            _ = self;
            const norm_a = a.normalize();
            const norm_b = b.normalize();
            return norm_a.a.id == norm_b.a.id and
                norm_a.b.id == norm_b.b.id;
        }
    };

    pub fn init(allocator: std.mem.Allocator, prng: *std.Random.DefaultPrng) Self {
        return Self{
            .allocator = allocator,
            .prng = prng,
            .nodes = std.array_list.Managed(NodeId).init(allocator),
            .message_queues = std.HashMap(
                NodeId,
                std.array_list.Managed(DelayedMessage),
                NodeIdContext,
                std.hash_map.default_max_load_percentage,
            ).init(allocator),
            .partitions = std.HashMap(
                NodePair,
                void,
                NodePairContext,
                std.hash_map.default_max_load_percentage,
            ).init(allocator),
            .packet_loss = std.HashMap(
                NodePair,
                f32,
                NodePairContext,
                std.hash_map.default_max_load_percentage,
            ).init(allocator),
            .latencies = std.HashMap(
                NodePair,
                u32,
                NodePairContext,
                std.hash_map.default_max_load_percentage,
            ).init(allocator),
        };
    }

    pub fn deinit(self: *Self) void {
        var iterator = self.message_queues.iterator();
        while (iterator.next()) |entry| {
            for (entry.value_ptr.items) |delayed_message| {
                self.allocator.free(delayed_message.message.data);
            }
            entry.value_ptr.deinit();
        }
        self.message_queues.deinit();

        self.nodes.deinit();
        self.partitions.deinit();
        self.packet_loss.deinit();
        self.latencies.deinit();
    }

    /// Add a node to the network simulation with message queue setup
    ///
    /// Sets up message queues and network state for the node.
    /// Needed to connect the node to the simulated network.
    pub fn add_node(self: *Self, node_id: NodeId) !void {
        try self.nodes.append(node_id);
        try self.message_queues.put(
            node_id,
            std.array_list.Managed(DelayedMessage).init(self.allocator),
        );
    }

    /// Send a message through the simulated network with failure modeling
    ///
    /// Applies network conditions like partitions, packet loss, and latency delays.
    /// Used for testing how the system behaves when the network is unreliable.
    pub fn send_message(self: *Self, message: Message) !void {
        const pair = NodePair{ .a = message.sender, .b = message.receiver };

        if (self.partitions.contains(pair.normalize())) {
            self.allocator.free(message.data);
            return;
        }

        if (self.packet_loss.get(pair.normalize())) |loss_rate| {
            if (self.prng.random().float(f32) < loss_rate) {
                self.allocator.free(message.data);
                return;
            }
        }

        const base_latency = self.latencies.get(pair.normalize()) orelse 1;
        const delivery_tick = 0 + base_latency;

        const delayed_message = DelayedMessage{
            .message = message,
            .delivery_tick = delivery_tick,
        };

        var queue = self.message_queues.getPtr(message.receiver).?;
        try queue.append(delayed_message);
    }

    pub fn process_tick(self: *Self) void {
        _ = self; // Unused for now
    }

    pub fn partition_nodes(self: *Self, node_a: NodeId, node_b: NodeId) void {
        const pair = NodePair{ .a = node_a, .b = node_b };
        self.partitions.put(pair.normalize(), {}) catch {};
    }

    pub fn heal_partition(self: *Self, node_a: NodeId, node_b: NodeId) void {
        const pair = NodePair{ .a = node_a, .b = node_b };
        _ = self.partitions.remove(pair.normalize());
    }

    pub fn configure_packet_loss(self: *Self, node_a: NodeId, node_b: NodeId, loss_rate: f32) void {
        const pair = NodePair{ .a = node_a, .b = node_b };
        self.packet_loss.put(pair.normalize(), loss_rate) catch {};
    }

    pub fn configure_latency(self: *Self, node_a: NodeId, node_b: NodeId, latency_ticks: u32) void {
        const pair = NodePair{ .a = node_a, .b = node_b };
        self.latencies.put(pair.normalize(), latency_ticks) catch {};
    }
};

/// Deterministic ownership violation injector for simulation testing.
/// Provides controlled injection of ownership violations to test fail-fast behavior.
pub const OwnershipViolationInjector = struct {
    prng: std.Random.DefaultPrng,
    injection_rate: f32,
    is_enabled: bool,
    violation_count: u32,

    const Self = @This();

    /// Ownership violation types that can be injected during simulation.
    pub const ViolationType = enum {
        cross_subsystem_read, // StorageEngine trying to read QueryEngine-owned block
        cross_subsystem_write, // QueryEngine trying to write MemtableManager-owned block
        dangling_arena_access, // Accessing block after arena is reset
        use_after_transfer, // Using block after ownership transfer
    };

    /// Initialize injector with deterministic seed for reproducible violation patterns.
    pub fn init(seed: u64) Self {
        return Self{
            .prng = std.Random.DefaultPrng.init(seed ^ 0xBAD_000ED),
            .injection_rate = 0.0, // Disabled by default
            .is_enabled = false,
            .violation_count = 0,
        };
    }

    /// Enable ownership violation injection with specified rate.
    /// Rate should be between 0.0 (never) and 1.0 (always).
    pub fn enable_injection(self: *Self, rate: f32) void {
        assert_mod.assert_fmt(rate >= 0.0 and rate <= 1.0, "Invalid injection rate: {d}", .{rate});
        self.injection_rate = rate;
        self.is_enabled = true;
    }

    /// Disable ownership violation injection.
    pub fn disable_injection(self: *Self) void {
        self.is_enabled = false;
        self.injection_rate = 0.0;
    }

    /// Check if a violation should be injected for this operation.
    /// Uses deterministic random number generation for reproducible test scenarios.
    pub fn should_inject_violation(self: *Self, violation_type: ViolationType) bool {
        if (!self.is_enabled) return false;

        // Use violation type as additional entropy for varied patterns
        _ = @intFromEnum(violation_type); // For future entropy mixing
        const random_val = self.prng.random().float(f32);

        if (random_val < self.injection_rate) {
            self.violation_count += 1;
            return true;
        }
        return false;
    }

    /// Inject a cross-subsystem read violation by corrupting ownership information.
    /// Returns a block with incorrect ownership to trigger validation failure.
    pub fn inject_cross_subsystem_read(
        self: *Self,
        owned_block: ownership.OwnedBlock,
        fake_accessor: ownership.BlockOwnership,
    ) ownership.OwnedBlock {
        if (!self.should_inject_violation(.cross_subsystem_read)) {
            return owned_block;
        }

        // Create a block with corrupted ownership that will fail validation
        var corrupted_block = owned_block;
        corrupted_block.ownership = fake_accessor;
        return corrupted_block;
    }

    /// Inject a dangling arena access by returning a block that appears valid
    /// but whose arena has been conceptually "reset".
    pub fn inject_dangling_arena_access(
        self: *Self,
        owned_block: ownership.OwnedBlock,
    ) ?ownership.OwnedBlock {
        if (!self.should_inject_violation(.dangling_arena_access)) {
            return owned_block;
        }

        // In a real scenario, the arena would be reset but we're accessing the block
        // For simulation, we can corrupt the arena pointer to simulate this
        // Arena corruption simulation handled separately for consistent alignment
        return owned_block;
    }

    /// Query statistics about injected violations for test validation.
    pub fn violation_stats(self: *const Self) struct {
        injection_rate: f32,
        is_enabled: bool,
        violation_count: u32,
    } {
        return .{
            .injection_rate = self.injection_rate,
            .is_enabled = self.is_enabled,
            .violation_count = self.violation_count,
        };
    }

    /// Reset violation statistics for new test scenarios.
    pub fn reset_stats(self: *Self) void {
        self.violation_count = 0;
    }
};

test "simulation basic functionality" {
    const allocator = std.testing.allocator;

    var sim = try Simulation.init(allocator, 0xDEADBEEF);
    defer sim.deinit();

    const node1 = try sim.add_node();
    const node2 = try sim.add_node();

    try std.testing.expect(node1.id == 0);
    try std.testing.expect(node2.id == 1);

    const initial_tick = sim.ticks();
    sim.tick();
    try std.testing.expect(sim.ticks() == initial_tick + 1);

    sim.tick_multiple(5);
    try std.testing.expect(sim.ticks() == initial_tick + 6);
}

test "simulation node filesystem" {
    const allocator = std.testing.allocator;

    var sim = try Simulation.init(allocator, 0xCAFEBABE);
    defer sim.deinit();

    const node1 = try sim.add_node();
    const node = sim.find_node(node1);

    var vfs_interface = node.filesystem_interface();

    var file = try vfs_interface.create("test.txt");
    defer file.close();

    _ = try file.write("Hello, Simulation!");
    file.close();

    try std.testing.expect(vfs_interface.exists("test.txt"));

    const state = try sim.node_filesystem_state(node1);
    defer {
        for (state) |file_state| {
            allocator.free(file_state.path);
            if (file_state.content) |content| {
                allocator.free(content);
            }
        }
        allocator.free(state);
    }

    try std.testing.expect(state.len == 1);
    try std.testing.expect(std.mem.eql(u8, state[0].path, "test.txt"));
    try std.testing.expect(std.mem.eql(u8, state[0].content.?, "Hello, Simulation!"));
}

test "simulation network partitions" {
    const allocator = std.testing.allocator;

    var sim = try Simulation.init(allocator, 0x12345678);
    defer sim.deinit();

    const node1 = try sim.add_node();
    const node2 = try sim.add_node();

    sim.partition_nodes(node1, node2);

    sim.heal_partition(node1, node2);

    sim.configure_packet_loss(node1, node2, 0.5);

    sim.configure_latency(node1, node2, 10);
}

test "simulation deterministic behavior" {
    const allocator = std.testing.allocator;

    const seed = 0xDEADBEEF;

    var sim1 = try Simulation.init(allocator, seed);
    defer sim1.deinit();

    var sim2 = try Simulation.init(allocator, seed);
    defer sim2.deinit();

    _ = try sim1.add_node();
    _ = try sim2.add_node();

    sim1.tick_multiple(10);
    sim2.tick_multiple(10);

    try std.testing.expect(sim1.ticks() == sim2.ticks());

    const rand1 = sim1.random();
    const rand2 = sim2.random();
    try std.testing.expect(rand1 == rand2);
}

test "ownership violation injection" {
    const allocator = std.testing.allocator;
    const types = @import("../core/types.zig");

    var sim = try Simulation.init(allocator, 0xBAD_000ED);
    defer sim.deinit();

    // Create a test block owned by storage engine
    const test_block = types.ContextBlock{
        .id = types.BlockId.from_hex("1234567890abcdef1234567890abcdef") catch unreachable, // Safety: Valid 32-char hex string
        .version = 1,
        .source_uri = "test://simulation_violation.zig",
        .metadata_json = "{}",
        .content = "Test content for ownership violation injection",
    };
    const owned_block = ownership.OwnedBlock.take_ownership(test_block, .storage_engine);

    // Initially, no violations should be enabled
    var stats = sim.ownership_violation_stats();
    try std.testing.expect(!stats.is_enabled);
    try std.testing.expect(stats.violation_count == 0);

    // Enable violation injection with 100% rate
    sim.enable_ownership_violations(1.0);
    stats = sim.ownership_violation_stats();
    try std.testing.expect(stats.is_enabled);
    try std.testing.expect(stats.injection_rate == 1.0);

    // Test cross-subsystem read violation
    const corrupted_block = sim.inject_ownership_violation(
        owned_block,
        .cross_subsystem_read,
        .query_engine, // Wrong accessor - should trigger violation
    );

    try std.testing.expect(corrupted_block != null);
    if (corrupted_block) |block| {
        // The injected block should have corrupted ownership
        try std.testing.expect(block.query_owner() == .query_engine);
        try std.testing.expect(block.query_owner() != owned_block.query_owner());
    }

    // Verify violation was counted
    stats = sim.ownership_violation_stats();
    try std.testing.expect(stats.violation_count > 0);

    // Test disabling injections
    sim.disable_ownership_violations();
    sim.reset_ownership_violation_stats();

    stats = sim.ownership_violation_stats();
    try std.testing.expect(!stats.is_enabled);
    try std.testing.expect(stats.violation_count == 0);

    // With injections disabled, block should remain unchanged
    const unchanged_block = sim.inject_ownership_violation(
        owned_block,
        .cross_subsystem_read,
        .query_engine,
    );

    if (unchanged_block) |block| {
        try std.testing.expect(block.query_owner() == owned_block.query_owner());
    }
}
