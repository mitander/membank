# Deterministic Testing Specification

**Philosophy:** Determinism is the cornerstone of CortexDB's reliability. There are no flaky tests. A test either passes or fails, and its result is perfectly reproducible given the same seed. This is achieved by abstracting all external I/O and controlling it from within a simulation harness.

## The Simulation Harness

The test runner (`zig build test`) compiles the entire CortexDB system with simulation-backed interfaces instead of production ones. The simulation is driven by a single script for each test case and a seeded Pseudo-Random Number Generator (PRNG) for all randomized decisions.

### Virtual File System (VFS)

*   **Interface:** The storage engine is not coded to `std.fs` but to a `VFS` interface defined in `src/vfs.zig`.
    ```zig
    pub const VFS = struct {
        open: *const fn (path: []const u8) anyerror!File,
        read: *const fn (file: File, buffer: []u8) anyerror!usize,
        // ... etc
    };
    ```
*   **Production Implementation:** Maps the interface functions directly to `std.fs` calls.
*   **Simulation Implementation:** Maps to a `std.AutoHashMap(string, ArrayList(u8))`. This in-memory file system can be inspected at the end of a test to assert its state. It can also be instrumented to simulate faults, e.g., a `write` call can be programmed to return `error.DiskFull` after N bytes.

### Virtual Network (VNet)

*   **Interface:** Client and Server communication is written against a `VNet` interface (`send`, `recv`).
*   **Production Implementation:** Maps to `std.net` TCP sockets.
*   **Simulation Implementation:** An in-memory message bus. It's a global hash map of `node_id -> message_queue`. The simulation harness controls the "tick" of this network, allowing it to introduce:
    *   **Latency:** Delaying message delivery by N ticks.
    *   **Partitions:** Dropping all messages between specified nodes for a duration.
    *   **Message Loss:** Dropping packets based on the PRNG.

## Writing a Simulation Test Case

A typical test file in `tests/` follows this structure:

```zig
const std = @import("std");
const sim = @import("cortex-sim");
const expect = std.testing.expect;

test "a client write succeeds during a brief network partition" {
    // 1. Initialize the simulation with a static seed for reproducibility.
    var harness = try sim.Harness.init(0xCAFE_BABE);
    defer harness.deinit();

    // 2. Configure the cluster.
    const node1 = harness.addNode();
    const node2 = harness.addNode();
    harness.startCluster();

    // 3. Script the scenario tick by tick.
    harness.tick(); // Allow leader election
    const client = harness.addClient(node1);
    client.write("blockA", "content");

    harness.tick();
    harness.partition(node1, node2); // Create a network partition
    harness.tick(5); // Run for 5 ticks with the partition active
    harness.heal(node1, node2); // Heal the partition
    harness.tick(10); // Allow the system to recover

    // 4. Assert the final state.
    // The state of the VFS on both nodes must be identical.
    const state1 = harness.vfsState(node1);
    const state2 = harness.vfsState(node2);
    try expect(std.mem.eql(u8, state1, state2));

    // And it must match the pre-approved "golden" output for this test.
    const golden = @embedFile("golden/test_partition_write.bin");
    try expect(std.mem.eql(u8, state1, golden));
}
```

This approach allows us to test complex failure scenarios (leader failure during a write, disk corruption, network partitions) with absolute, byte-for-byte precision, providing a level of confidence that is impossible to achieve with traditional integration testing.
