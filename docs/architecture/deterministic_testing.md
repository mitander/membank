# Deterministic Testing Specification

**Philosophy:** Determinism is the cornerstone of CortexDB's reliability. There are no flaky tests. A test either passes or fails, and its result is perfectly reproducible given the same seed. This is achieved by abstracting all external I/O and controlling it from within a simulation harness.

## The Simulation Harness

The test runner (`zig build test`) compiles the entire CortexDB system with simulation-backed interfaces instead of production ones. The simulation is driven by a single script for each test case and a seeded Pseudo-Random Number Generator (PRNG) for all randomized decisions.

### Virtual File System (VFS)

*   **Interface:** The storage engine is not coded to `std.fs` but to a `VFS` interface defined in `src/vfs.zig`.
    ```zig
    pub const VFS = struct {
        ptr: *anyopaque,
        vtable: *const VTable,
        
        pub const VTable = struct {
            open: *const fn (ptr: *anyopaque, path: []const u8, mode: OpenMode) anyerror!VFile,
            create: *const fn (ptr: *anyopaque, path: []const u8) anyerror!VFile,
            remove: *const fn (ptr: *anyopaque, path: []const u8) anyerror!void,
            exists: *const fn (ptr: *anyopaque, path: []const u8) bool,
            // ... etc
        };
    };
    ```
*   **Production Implementation (`ProductionVFS`):** Maps the interface functions directly to `std.fs` calls through the vtable pattern.
*   **Simulation Implementation (`SimulationVFS`):** Maps to a `std.StringHashMap(FileData, ...)`. This in-memory file system can be inspected at the end of a test to assert its state. It can also be instrumented to simulate faults, control time progression, and provide deterministic filesystem state comparisons.

### Virtual Network (VNet)

*   **Interface:** Client and Server communication is written against a `Network` interface defined in `src/simulation.zig`.
*   **Production Implementation:** Will map to `std.net` TCP sockets (future implementation).
*   **Simulation Implementation:** An in-memory message bus implemented as hash maps of `node_id -> message_queue`. The simulation harness controls the "tick" of this network, allowing it to introduce:
    *   **Latency:** Delaying message delivery by N ticks using `set_latency()`.
    *   **Partitions:** Dropping all messages between specified nodes using `partition_nodes()`.
    *   **Message Loss:** Dropping packets based on configurable loss rates using `set_packet_loss()`.

## Writing a Simulation Test Case

A typical test file in `tests/` follows this structure:

```zig
const std = @import("std");
const simulation = @import("simulation");
const vfs = @import("vfs");
const assert = @import("assert");

const Simulation = simulation.Simulation;
const NodeId = simulation.NodeId;

test "network partition: write succeeds after partition heals" {
    const allocator = std.testing.allocator;
    
    // 1. Initialize the simulation with a static seed for reproducibility.
    var sim = try Simulation.init(allocator, 0xCAFE_BABE);
    defer sim.deinit();
    
    // 2. Configure the cluster.
    const node1 = try sim.add_node();
    const node2 = try sim.add_node();
    const node3 = try sim.add_node();
    
    // Allow cluster to stabilize
    sim.tick_multiple(10);
    
    // 3. Script the scenario.
    // Write some initial data to node1
    const node1_ptr = sim.get_node(node1);
    var node1_vfs = node1_ptr.get_vfs();
    
    var file = try node1_vfs.create("data/block_001.db");
    defer file.close() catch {};
    
    const initial_data = "Initial block data";
    _ = try file.write(initial_data);
    try file.close();
    
    // Create network partition isolating node3
    sim.partition_nodes(node1, node3);
    sim.partition_nodes(node2, node3);
    
    // Run simulation for 50 ticks with partition active
    sim.tick_multiple(50);
    
    // Heal the partition
    sim.heal_partition(node1, node3);
    sim.heal_partition(node2, node3);
    
    // Allow time for cluster to recover and synchronize
    sim.tick_multiple(30);
    
    // 4. Assert the final state.
    const final_state = try sim.get_node_filesystem_state(node1);
    defer {
        for (final_state) |file_state| {
            allocator.free(file_state.path);
            if (file_state.content) |content| {
                allocator.free(content);
            }
        }
        allocator.free(final_state);
    }
    
    // Verify the expected files are present
    try std.testing.expect(final_state.len >= 1);
    try std.testing.expect(std.mem.eql(u8, final_state[0].content.?, initial_data));
}
```

This approach allows us to test complex failure scenarios (leader failure during a write, disk corruption, network partitions) with absolute, byte-for-byte precision, providing a level of confidence that is impossible to achieve with traditional integration testing.
