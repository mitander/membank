//! Deterministic simulation harness for Membank testing.
//!
//! Provides a framework for running the entire Membank system
//! in a controlled, deterministic environment for testing.

const std = @import("std");
const assert = @import("../core/assert.zig");
const vfs = @import("../core/vfs.zig");
const sim_vfs = @import("simulation_vfs.zig");

/// Deterministic simulation harness.
pub const Simulation = struct {
    allocator: std.mem.Allocator,
    prng: std.Random.DefaultPrng,
    tick_count: u64,
    nodes: std.ArrayList(Node),
    network: Network,

    const Self = @This();

    /// Initialize a new simulation with a specific seed for reproducibility.
    pub fn init(allocator: std.mem.Allocator, seed: u64) !Self {
        var prng = std.Random.DefaultPrng.init(seed);

        return Self{
            .allocator = allocator,
            .prng = prng,
            .tick_count = 0,
            .nodes = std.ArrayList(Node).init(allocator),
            .network = Network.init(allocator, &prng),
        };
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
        try self.nodes.append(node); // tidy:ignore-perf Dynamic node addition without known count
        try self.network.add_node(node_id);
        return node_id;
    }

    /// Get a node by its ID.
    pub fn find_node(self: *Self, node_id: NodeId) *Node {
        assert.assert_index_valid(
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
        assert.assert_range(loss_rate, 0.0, 1.0, "Invalid loss rate: {}", .{loss_rate});
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
    ) ![]sim_vfs.SimulationVFS.FileState {
        const node = self.find_node(node_id);
        return node.filesystem.state(self.allocator);
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

/// A simulated node in the Membank cluster.
pub const Node = struct {
    id: NodeId,
    filesystem: sim_vfs.SimulationVFS,
    message_queue: std.ArrayList(Message),
    allocator: std.mem.Allocator,

    const Self = @This();

    pub fn init(allocator: std.mem.Allocator, id: NodeId) !Self {
        return Self{
            .id = id,
            .filesystem = try sim_vfs.SimulationVFS.init(allocator),
            .message_queue = std.ArrayList(Message).init(allocator),
            .allocator = allocator,
        };
    }

    pub fn deinit(self: *Self) void {
        self.filesystem.deinit();
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
        // Future: Handle different message types
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
    nodes: std.ArrayList(NodeId),
    message_queues: std.HashMap(
        NodeId,
        std.ArrayList(DelayedMessage),
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
            .nodes = std.ArrayList(NodeId).init(allocator),
            .message_queues = std.HashMap(
                NodeId,
                std.ArrayList(DelayedMessage),
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

    pub fn add_node(self: *Self, node_id: NodeId) !void {
        try self.nodes.append(node_id);
        try self.message_queues.put(
            node_id,
            std.ArrayList(DelayedMessage).init(self.allocator),
        );
    }

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
        // For now, this is a placeholder
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

test "simulation basic functionality" {
    const allocator = std.testing.allocator;

    var sim = try Simulation.init(allocator, 0xDEADBEEF);
    defer sim.deinit();

    // Add some nodes
    const node1 = try sim.add_node();
    const node2 = try sim.add_node();

    try std.testing.expect(node1.id == 0);
    try std.testing.expect(node2.id == 1);

    // Test basic tick functionality
    const initial_tick = sim.ticks();
    sim.tick();
    try std.testing.expect(sim.ticks() == initial_tick + 1);

    // Test multiple ticks
    sim.tick_multiple(5);
    try std.testing.expect(sim.ticks() == initial_tick + 6);
}

test "simulation node filesystem" {
    const allocator = std.testing.allocator;

    var sim = try Simulation.init(allocator, 0xCAFEBABE);
    defer sim.deinit();

    const node1 = try sim.add_node();
    const node = sim.find_node(node1);

    // Test filesystem operations
    var vfs_interface = node.filesystem_interface();

    var file = try vfs_interface.create("test.txt");
    defer file.close() catch {};

    _ = try file.write("Hello, Simulation!");
    try file.close();

    try std.testing.expect(vfs_interface.exists("test.txt"));

    // Get filesystem state
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

    // Create partition
    sim.partition_nodes(node1, node2);

    // Test that partition exists (we'd need to extend the API to check this)
    // For now, we just test that the functions don't crash

    // Heal partition
    sim.heal_partition(node1, node2);

    // Test packet loss
    sim.configure_packet_loss(node1, node2, 0.5);

    // Test latency
    sim.configure_latency(node1, node2, 10);
}

test "simulation deterministic behavior" {
    const allocator = std.testing.allocator;

    // Run the same simulation twice with the same seed
    const seed = 0xDEADBEEF;

    var sim1 = try Simulation.init(allocator, seed);
    defer sim1.deinit();

    var sim2 = try Simulation.init(allocator, seed);
    defer sim2.deinit();

    // Perform the same operations on both
    _ = try sim1.add_node();
    _ = try sim2.add_node();

    sim1.tick_multiple(10);
    sim2.tick_multiple(10);

    // Both should have the same tick count
    try std.testing.expect(sim1.ticks() == sim2.ticks());

    // Both should generate the same random numbers
    const rand1 = sim1.random();
    const rand2 = sim2.random();
    try std.testing.expect(rand1 == rand2);
}
